//! caller が使用する high-level client API と、その handle ownership を定義する。
//!
//! `Client` は単一 app-server process を共有する cloneable handle である。最後の handle が drop された場合は
//! async cleanup を待てないため process group を fail-safe に強制終了する。正常終了が必要な caller は明示的に
//! [`Client::shutdown`] を await する。

use crate::event::{EventReceiver, ServerRequest};
use crate::process::{ProcessCommand, spawn_process};
use crate::protocol::{
    ClientInfo, InitializeParams, InitializeRuntimeResponse, RpcError, ServerRequestId,
    ServerRequestResult, ThreadId, ThreadStartOutcome, ThreadStartParams, ThreadStartResponse,
    ThreadTurnsListParams, ThreadTurnsListResponse, TurnId, TurnOutcome, TurnStartParams,
    TurnStartResponse, WireObject,
};
use crate::runtime::{RequestPermission, Shared};
use crate::shutdown;
use crate::state::{ClientState, ShutdownReport};
use crate::{ClientConfig, ClientError};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{Instant, timeout_at};

/// 単一の `codex app-server` child process を操作する cloneable handle。
///
/// clone 間では process、runtime state、request correlation、event stream を共有する。
/// 同一 client で同時に実行できる active turn は一つだけである。
pub struct Client {
    /// process と background task を所有する shared runtime。
    inner: Arc<Shared>,
    /// 最後の public handle の drop を検出するための明示的な handle 数。
    ///
    /// `Arc<Shared>` の strong count には background task も含まれるため、public handle 数の代用にはできない。
    handles: Arc<AtomicUsize>,

    /// `codex app-server` 起動時の設定。
    config: Arc<ClientConfig>,
    /// initialize responseでapp-serverが返した実効`CODEX_HOME`。
    codex_home: Arc<str>,
}

/// response 送信前に local response claim を取得した raw / untyped ServerRequest。
///
/// [`Client::claim_server_request`] が成功した場合だけ生成される。response 送信はこの値を消費するため、
/// 同じ claim から複数回送信することはできない。
pub struct ClaimedServerRequest {
    client: Client,
    request_id: ServerRequestId,
}

impl ClaimedServerRequest {
    /// claim 済みServerRequestへraw success responseを送信する。
    ///
    /// stdin write が失敗した場合は既存の local claim rollback を行う。
    pub async fn respond(self, result: Value) -> Result<(), ClientError> {
        self.client
            .inner
            .send_claimed_server_request(self.request_id, json!({"id": null, "result": result}))
            .await
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        self.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            handles: Arc::clone(&self.handles),
            config: Arc::clone(&self.config),
            codex_home: Arc::clone(&self.codex_home),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Drop では async shutdown を await できない。stdin writer を停止し、process supervisor へ
            // kill を通知して child process group が orphan になることを防ぐ。
            let _ = self.inner.writer_stop().send(true);
            let _ = self.inner.process_sender().try_send(ProcessCommand::Kill);
        }
    }
}

impl Client {
    /// 設定を検証し、app-server process の起動と initialize handshake を完了する。
    ///
    /// `initialize` response の受信だけでは ready とせず、`initialized` notification の stdin write 完了までを
    /// handshake に含める。途中で失敗した場合は child process と background task を cleanup してから error を返す。
    pub async fn spawn(config: ClientConfig) -> Result<Self, ClientError> {
        // 副作用を開始する前に path、timeout、capacity、worst-case memory を確定させる。
        config.validate()?;
        let process = spawn_process(&config)?;
        let (outgoing_sender, outgoing_receiver) = mpsc::channel(config.outgoing_queue_capacity);
        let (event_sender, _) = broadcast::channel(config.event_queue_capacity);
        let (process_sender, process_receiver) = mpsc::channel(4);
        let (writer_stop, writer_stop_receiver) = watch::channel(false);
        let inner = Arc::new(Shared::new(
            outgoing_sender,
            event_sender,
            process_sender,
            writer_stop,
            config.shutdown_timeout,
        ));
        let mut client = Self {
            inner: Arc::clone(&inner),
            handles: Arc::new(AtomicUsize::new(1)),
            config: Arc::new(config),
            codex_home: Arc::from(""),
        };

        inner.lock_state().begin_initialization();
        inner.spawn_tasks(
            process,
            outgoing_receiver,
            process_receiver,
            writer_stop_receiver,
            client.config.max_message_bytes,
        );

        let initialize = InitializeParams {
            client_info: ClientInfo {
                name: "codex-app-server-client",
                version: env!("CARGO_PKG_VERSION"),
            },
            capabilities: client.config.capabilities.clone(),
        };
        let initialize_deadline = Instant::now() + client.config.initialize_timeout;
        let initialize_result = timeout_at(initialize_deadline, async {
            let WireObject(response): WireObject<InitializeRuntimeResponse> = inner
                .send_request("initialize", &initialize, RequestPermission::Initializing)
                .await?;
            // app-server が client request を受理できるのは initialized notification の write 成功後とする。
            inner
                .send_value(json!({"method": "initialized"}), true)
                .await?;
            Ok::<String, ClientError>(response.codex_home)
        })
        .await;

        match initialize_result {
            Ok(Ok(codex_home)) => client.codex_home = Arc::from(codex_home),
            Ok(Err(error)) => {
                let error = match error {
                    ClientError::RpcFailed { .. } | ClientError::StdinWriteFailed { .. } => {
                        ClientError::InitializeFailed {
                            reason: error.to_string(),
                        }
                    }
                    other => other,
                };
                inner.fail(error.clone());
                shutdown::cleanup_failed_spawn(&inner).await;
                return Err(error);
            }
            Err(_) => {
                let error = ClientError::InitializeTimeout;
                inner.fail(error.clone());
                shutdown::cleanup_failed_spawn(&inner).await;
                return Err(error);
            }
        }

        inner.lock_state().finish_initialization()?;
        Ok(client)
    }

    /// 現在の client lifecycle state を取得する。
    ///
    /// snapshot であり、この呼び出し直後に background task によって state が変化する可能性がある。
    pub fn state(&self) -> ClientState {
        self.inner.state()
    }

    /// event stream の新しい subscriber を作成する。
    ///
    /// subscription 前に配信済みの event は取得できない。consumer が queue capacity より遅れた場合は
    /// `RecvError::Lagged` が返るため、必要に応じて current state と pending request を再取得する。
    pub fn subscribe(&self) -> EventReceiver {
        self.inner.subscribe()
    }

    /// caller の応答を待っている ServerRequest の snapshot を返す。
    ///
    /// event receiver の lag から復旧する場合や、UI を再構築する場合に使用する。
    pub fn pending_server_requests(&self) -> Vec<ServerRequest> {
        self.inner.pending_server_requests()
    }

    /// 新しい app-server thread を開始し、初期実効設定を返す。
    pub async fn start_thread(
        &self,
        params: ThreadStartParams,
    ) -> Result<ThreadStartOutcome, ClientError> {
        let WireObject(response): WireObject<ThreadStartResponse> = self
            .inner
            .send_request("thread/start", &params, RequestPermission::Ready)
            .await?;
        Ok(ThreadStartOutcome {
            thread_id: response.thread.id,
            model: response.model,
            reasoning_effort: response.reasoning_effort,
            approval_policy: response.approval_policy,
            approvals_reviewer: response.approvals_reviewer,
            sandbox: response.sandbox,
            cwd: response.cwd,
            model_provider: response.model_provider,
        })
    }

    /// 既存の app-server thread を再開し、再開時の実効設定を返す。
    ///
    /// 履歴はレスポンスから省略される。再開した thread の会話コンテキストは
    /// 返された thread ID を指定する後続の [`Client::run_turn`] で引き継がれる。
    pub async fn resume_thread(
        &self,
        thread_id: ThreadId,
    ) -> Result<ThreadStartOutcome, ClientError> {
        let WireObject(response): WireObject<ThreadStartResponse> = self
            .inner
            .send_request(
                "thread/resume",
                &json!({"threadId": thread_id, "excludeTurns": true}),
                RequestPermission::Ready,
            )
            .await?;
        Ok(ThreadStartOutcome {
            thread_id: response.thread.id,
            model: response.model,
            reasoning_effort: response.reasoning_effort,
            approval_policy: response.approval_policy,
            approvals_reviewer: response.approvals_reviewer,
            sandbox: response.sandbox,
            cwd: response.cwd,
            model_provider: response.model_provider,
        })
    }

    /// スレッドの turn 履歴を指定された条件で 1 ページ取得する。
    ///
    /// 次のページを取得する場合は、返された `next_cursor` を次回の `params.cursor` に指定する。
    pub async fn list_thread_turns(
        &self,
        params: ThreadTurnsListParams,
    ) -> Result<ThreadTurnsListResponse, ClientError> {
        let WireObject(response) = self
            .inner
            .send_request("thread/turns/list", &params, RequestPermission::Ready)
            .await?;
        Ok(response)
    }

    /// turn を開始し、対応する terminal `turn/completed` notification まで待機する。
    ///
    /// app-server は `turn/start` response より先に terminal notification を送る可能性があるため、runtime state が
    /// 両者の到着順を吸収する。同一 client 上で別の active turn が存在する場合は開始しない。
    pub async fn run_turn(&self, params: TurnStartParams) -> Result<TurnOutcome, ClientError> {
        // request を送る前に active turn slot を確保し、早着した completion を取りこぼさない。
        let waiter_receiver = self.inner.begin_turn(params.thread_id.clone())?;
        let response = self
            .inner
            .send_request::<_, WireObject<TurnStartResponse>>(
                "turn/start",
                &params,
                RequestPermission::Ready,
            )
            .await;
        if let Err(error) = response {
            self.inner.clear_active_turn_with_error(error.clone());
            return Err(error);
        }

        waiter_receiver.await.map_err(|_| {
            self.inner
                .primary_failure()
                .unwrap_or(ClientError::ClientFailed {
                    cause: "turn waiter closed".to_owned(),
                })
        })?
    }

    /// 現在 active な turn へ interrupt request を送信する。
    ///
    /// stale ID や別 thread の turn を誤って interrupt しないよう、request 送信前に local active turn と照合する。
    /// terminal outcome は引き続き [`Client::run_turn`] の waiter から返る。
    pub async fn interrupt_turn(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
    ) -> Result<(), ClientError> {
        self.inner.validate_active_turn(thread_id, turn_id)?;
        let _: EmptyResponse = self
            .inner
            .send_request(
                "turn/interrupt",
                &json!({"threadId": thread_id, "turnId": turn_id}),
                RequestPermission::Ready,
            )
            .await?;
        Ok(())
    }

    /// pending ServerRequest を成功 response で一度だけ解決する。
    ///
    /// typed method には対応する typed result が必要である。stdin write が失敗した場合は local claim を rollback するが、
    /// transport failure によって client が `Failed` へ遷移した場合は再応答できない。
    pub async fn respond_server_request(
        &self,
        request_id: ServerRequestId,
        result: ServerRequestResult,
    ) -> Result<(), ClientError> {
        self.inner
            .claim_server_request(&request_id, Some(&result))?;
        self.inner
            .send_claimed_server_request(
                request_id,
                json!({"id": null, "result": result.into_value()}),
            )
            .await
    }

    /// raw result を使用できる pending ServerRequest の local response claim を送信前に一度だけ取得する。
    ///
    /// typed approval request と user-input request はこの操作では claim できない。
    pub fn claim_server_request(
        &self,
        request_id: ServerRequestId,
    ) -> Result<ClaimedServerRequest, ClientError> {
        let result = ServerRequestResult::new(Value::Null);
        self.inner
            .claim_server_request(&request_id, Some(&result))?;
        Ok(ClaimedServerRequest {
            client: self.clone(),
            request_id,
        })
    }

    /// pending ServerRequest を JSON-RPC error response で一度だけ拒否する。
    ///
    /// stdin write が失敗した場合は local claim を rollback するが、transport failure による lifecycle transition を
    /// 超えて再応答可能であることは保証しない。
    pub async fn reject_server_request(
        &self,
        request_id: ServerRequestId,
        error: RpcError,
    ) -> Result<(), ClientError> {
        self.inner.claim_server_request(&request_id, None)?;
        self.inner
            .send_claimed_server_request(request_id, json!({"id": null, "error": error}))
            .await
    }

    /// active operation を取消し、stdin close、child process 回収、background task join を実行する。
    ///
    /// 複数 clone から同時に呼び出しても shutdown sequence は一度だけ実行され、以後は同じ result を返す。
    pub async fn shutdown(&self) -> Result<ShutdownReport, ClientError> {
        shutdown::shutdown(&self.inner).await
    }

    /// `codex app-server` 起動時の設定を取得する。
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// initialize responseでapp-serverが返した実効`CODEX_HOME`を返す。
    pub fn codex_home(&self) -> &str {
        &self.codex_home
    }
}

/// payload を持たない JSON-RPC success response を deserialize するための型。
#[derive(serde::Deserialize)]
struct EmptyResponse {}

#[cfg(test)]
mod tests {
    use crate::{
        ApprovalsReviewer, AskForApproval, SandboxPolicy, SimpleApprovalPolicy, TurnStatus,
        UserInput, event::ServerRequestKind, protocol::RequestId, transport::Outgoing,
    };

    use super::*;

    fn ready_client() -> (Client, mpsc::Receiver<Outgoing>, watch::Receiver<bool>) {
        let (outgoing_sender, outgoing_receiver) = mpsc::channel(1);
        let (event_sender, _) = broadcast::channel(1);
        let (process_sender, _) = mpsc::channel(1);
        let (writer_stop, writer_stop_receiver) = watch::channel(false);
        let inner = Arc::new(Shared::new(
            outgoing_sender,
            event_sender,
            process_sender,
            writer_stop,
            std::time::Duration::from_secs(1),
        ));
        {
            let mut state = inner.lock_state();
            state.begin_initialization();
            state.finish_initialization().unwrap();
        }
        (
            Client {
                inner,
                handles: Arc::new(AtomicUsize::new(1)),
                config: Arc::new(ClientConfig::new(".", ".")),
                codex_home: Arc::from(""),
            },
            outgoing_receiver,
            writer_stop_receiver,
        )
    }

    fn register_raw_request(client: &Client, request_id: &RequestId) {
        client
            .inner
            .lock_state()
            .register_server_request(
                ServerRequest {
                    id: request_id.clone(),
                    method: "future/request".to_owned(),
                    kind: ServerRequestKind::Unsupported,
                    params: Value::Null,
                    raw: Value::Null,
                },
                None,
                None,
                None,
            )
            .unwrap();
    }

    async fn acknowledge_next_message(receiver: &mut mpsc::Receiver<Outgoing>) -> Value {
        let Outgoing::Message { value, ack } = receiver.recv().await.unwrap() else {
            panic!("expected response message");
        };
        ack.unwrap().send(Ok(())).unwrap();
        value
    }

    async fn list_thread_turns_with_response(
        client: &Client,
        outgoing: &mut mpsc::Receiver<Outgoing>,
        params: ThreadTurnsListParams,
        expected_params: Value,
        mut response: Value,
    ) -> Result<ThreadTurnsListResponse, ClientError> {
        let (result, ()) = tokio::join!(client.list_thread_turns(params), async {
            let Some(Outgoing::Message { value, ack: None }) = outgoing.recv().await else {
                panic!("expected thread/turns/list request");
            };
            assert_eq!(value["method"], "thread/turns/list");
            assert_eq!(value["params"], expected_params);
            response["id"] = value["id"].clone();
            client.inner.dispatch(response).await.unwrap();
        });
        result
    }

    #[tokio::test]
    async fn initialize_response_requires_only_string_codex_home() {
        for (result, accepted) in [
            (json!({"codexHome": "/codex"}), true),
            (
                json!({"codexHome": "/codex", "platformOs": 42, "userAgent": false}),
                true,
            ),
            (json!({}), false),
            (json!({"codexHome": 42}), false),
            (json!(["/codex"]), false),
        ] {
            let (client, mut outgoing, _) = ready_client();
            client.inner.lock_state().begin_initialization();
            let (response, ()) = tokio::join!(
                client
                    .inner
                    .send_request::<_, WireObject<InitializeRuntimeResponse>>(
                        "initialize",
                        &(),
                        RequestPermission::Initializing
                    ),
                async {
                    let Some(Outgoing::Message { value, ack: None }) = outgoing.recv().await else {
                        panic!("expected initialize request");
                    };
                    assert_eq!(value["method"], "initialize");
                    client
                        .inner
                        .dispatch(json!({"id": value["id"], "result": result}))
                        .await
                        .unwrap();
                }
            );
            if accepted {
                assert_eq!(response.unwrap().0.codex_home, "/codex");
            } else {
                assert!(matches!(
                    response,
                    Err(ClientError::ProtocolViolation { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn start_thread_only_requires_the_id_and_effective_settings() {
        let valid_result = json!({
            "thread": {"id": "thread"},
            "model": "model",
            "modelProvider": "provider",
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandbox": {"type": "readOnly"},
            "cwd": "/workspace"
        });
        let mut extended_result = valid_result.clone();
        extended_result["thread"]["turns"] = 42.into();
        let mut invalid_id = valid_result.clone();
        invalid_id["thread"]["id"] = 42.into();
        let mut invalid_settings = valid_result.clone();
        invalid_settings["sandbox"]["networkAccess"] = "true".into();
        let mut positional_sandbox = valid_result.clone();
        positional_sandbox["sandbox"] = json!(["dangerFullAccess"]);
        let mut missing_id = valid_result.clone();
        missing_id["thread"] = json!({});
        for (result, accepted) in [
            (valid_result, true),
            (extended_result, true),
            (invalid_id, false),
            (invalid_settings, false),
            (positional_sandbox, false),
            (missing_id, false),
        ] {
            let (client, mut outgoing, _) = ready_client();
            let (outcome, ()) =
                tokio::join!(client.start_thread(ThreadStartParams::default()), async {
                    let Some(Outgoing::Message { value, ack: None }) = outgoing.recv().await else {
                        panic!("expected thread/start request");
                    };
                    assert_eq!(value["method"], "thread/start");
                    client
                        .inner
                        .dispatch(json!({"id": value["id"], "result": result}))
                        .await
                        .unwrap();
                });
            if accepted {
                let outcome = outcome.unwrap();
                assert_eq!(outcome.thread_id.as_str(), "thread");
                assert_eq!(outcome.model, "model");
                assert_eq!(outcome.model_provider, "provider");
                assert_eq!(outcome.cwd, "/workspace");
            } else {
                assert!(matches!(
                    outcome,
                    Err(ClientError::ProtocolViolation { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn resume_thread_returns_effective_settings_and_can_run_a_turn() {
        let (client, mut outgoing, _) = ready_client();
        let (thread, ()) = tokio::join!(client.resume_thread(ThreadId::new("existing")), async {
            let Some(Outgoing::Message { value, ack: None }) = outgoing.recv().await else {
                panic!("expected thread/resume request");
            };
            assert_eq!(value["method"], "thread/resume");
            assert_eq!(
                value["params"],
                json!({"threadId": "existing", "excludeTurns": true})
            );
            let result = json!({
                "thread": {"id": "existing", "turns": []},
                "model": "resumed-model",
                "reasoningEffort": "high",
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandbox": {"type": "readOnly", "networkAccess": false},
                "cwd": "/resumed-workspace",
                "modelProvider": "resumed-provider"
            });
            client
                .inner
                .dispatch(json!({"id": value["id"], "result": result}))
                .await
                .unwrap();
        });
        let thread = thread.unwrap();
        assert_eq!(thread.thread_id.as_str(), "existing");
        assert_eq!(thread.model, "resumed-model");
        assert_eq!(thread.reasoning_effort.unwrap().as_str(), "high");
        assert!(matches!(
            thread.approval_policy,
            AskForApproval::Simple(SimpleApprovalPolicy::OnRequest)
        ));
        assert!(matches!(thread.approvals_reviewer, ApprovalsReviewer::User));
        assert!(matches!(
            thread.sandbox,
            SandboxPolicy::ReadOnly {
                network_access: Some(false)
            }
        ));
        assert_eq!(thread.cwd, "/resumed-workspace");
        assert_eq!(thread.model_provider, "resumed-provider");

        let turn = TurnStartParams::new(
            thread.thread_id,
            vec![UserInput::Text {
                text: "Continue the conversation.".to_owned(),
                text_elements: Vec::new(),
            }],
        );
        let (outcome, ()) = tokio::join!(client.run_turn(turn), async {
            let Some(Outgoing::Message { value, ack: None }) = outgoing.recv().await else {
                panic!("expected turn/start request");
            };
            assert_eq!(value["method"], "turn/start");
            assert_eq!(value["params"]["threadId"], "existing");
            client
                .inner
                .dispatch(json!({"id": value["id"], "result": {"turn": {"id": "turn"}}}))
                .await
                .unwrap();
            client
                .inner
                .dispatch(json!({"method": "turn/completed", "params": {
                    "threadId": "existing",
                    "turn": {"id": "turn", "status": "completed", "items": []}
                }}))
                .await
                .unwrap();
        });
        let outcome = outcome.unwrap();
        assert_eq!(outcome.thread_id.as_str(), "existing");
        assert_eq!(outcome.turn_id.as_str(), "turn");
        assert_eq!(outcome.turn.status, TurnStatus::Completed);
    }

    #[tokio::test]
    async fn resume_thread_propagates_rpc_errors_without_sending_another_request() {
        let (client, mut outgoing, _) = ready_client();
        let mut resume = std::pin::pin!(client.resume_thread(ThreadId::new("missing")));
        let request = tokio::select! {
            message = outgoing.recv() => message,
            result = &mut resume => panic!("resume returned before sending a request: {result:?}"),
        };
        let Some(Outgoing::Message { value, ack: None }) = request else {
            panic!("expected thread/resume request");
        };
        assert_eq!(value["method"], "thread/resume");
        client
            .inner
            .dispatch(json!({
                "id": value["id"],
                "error": {"code": -32602, "message": "thread not found"}
            }))
            .await
            .unwrap();

        let error = tokio::select! {
            result = &mut resume => result.unwrap_err(),
            _ = outgoing.recv() => panic!("resume sent another request after the RPC error"),
        };
        let ClientError::RpcFailed {
            method,
            request_id,
            code,
            message,
        } = error
        else {
            panic!("expected a JSON-RPC failure: {error:?}");
        };
        assert_eq!(method, "thread/resume");
        assert_eq!(request_id, value["id"].as_i64().unwrap());
        assert_eq!(code, -32602);
        assert_eq!(message, "thread not found");
        assert!(matches!(
            outgoing.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn list_thread_turns_preserves_raw_turns_and_follows_a_caller_supplied_cursor() {
        let (client, mut outgoing, _) = ready_client();
        let params = ThreadTurnsListParams {
            thread_id: ThreadId::new("saved-thread"),
            cursor: Some("initial-cursor".to_owned()),
            limit: Some(2),
            sort_direction: Some("asc".to_owned()),
            items_view: Some("full".to_owned()),
        };
        let mut expected_params = json!({
            "threadId": "saved-thread",
            "cursor": "initial-cursor",
            "limit": 2,
            "sortDirection": "asc",
            "itemsView": "full"
        });
        let turns = vec![
            json!({
                "id": "turn-z",
                "status": "completed",
                "itemsView": "full",
                "items": [
                    {
                        "id": "item-z",
                        "type": "userMessage",
                        "clientId": "user-message",
                        "content": [{"type": "text", "text": "Question"}]
                    },
                    {
                        "id": "item-a",
                        "type": "agentMessage",
                        "text": "Answer",
                        "phase": "final_answer",
                        "futureField": {"values": [1, null]}
                    }
                ],
                "rootTurnId": "root-turn",
                "startedAt": 100,
                "completedAt": 101,
                "durationMs": 1000
            }),
            json!({
                "id": "turn-a",
                "status": "futureStatus",
                "items": [
                    {"type": "futureItem", "payload": [1, null]},
                    {"type": "agentMessage", "text": false},
                    null
                ],
                "itemsView": {"future": true},
                "startedAt": "unknown",
                "futureField": [null, false]
            }),
        ];
        let page = list_thread_turns_with_response(
            &client,
            &mut outgoing,
            params.clone(),
            expected_params.clone(),
            json!({"result": {
                "data": turns,
                "nextCursor": "next-cursor",
                "backwardsCursor": "previous-cursor"
            }}),
        )
        .await
        .unwrap();
        assert_eq!(page.data, turns);
        assert_eq!(page.next_cursor.as_deref(), Some("next-cursor"));

        expected_params["cursor"] = json!("next-cursor");
        let last_turn = json!({"id": "turn-b", "status": "completed", "items": []});
        let last_page = list_thread_turns_with_response(
            &client,
            &mut outgoing,
            ThreadTurnsListParams {
                cursor: page.next_cursor,
                ..params
            },
            expected_params,
            json!({"result": {"data": [last_turn], "nextCursor": null}}),
        )
        .await
        .unwrap();
        assert_eq!(last_page.data, vec![last_turn]);
        assert_eq!(last_page.next_cursor, None);
    }

    #[tokio::test]
    async fn list_thread_turns_omits_unset_options_and_accepts_a_missing_next_cursor() {
        let (client, mut outgoing, _) = ready_client();
        let page = list_thread_turns_with_response(
            &client,
            &mut outgoing,
            ThreadTurnsListParams {
                thread_id: ThreadId::new("saved-thread"),
                cursor: None,
                limit: None,
                sort_direction: None,
                items_view: None,
            },
            json!({"threadId": "saved-thread"}),
            json!({"result": {"data": []}}),
        )
        .await
        .unwrap();
        assert!(page.data.is_empty());
        assert_eq!(page.next_cursor, None);
    }

    #[tokio::test]
    async fn list_thread_turns_uses_existing_rpc_and_response_errors() {
        let (client, mut outgoing, _) = ready_client();
        let params = ThreadTurnsListParams {
            thread_id: ThreadId::new("saved-thread"),
            cursor: None,
            limit: None,
            sort_direction: None,
            items_view: None,
        };
        let expected_params = json!({"threadId": "saved-thread"});
        let result = list_thread_turns_with_response(
            &client,
            &mut outgoing,
            params.clone(),
            expected_params.clone(),
            json!({"error": {"code": -32602, "message": "thread not found"}}),
        )
        .await;
        assert!(matches!(
            result,
            Err(ClientError::RpcFailed { method, request_id: 1, code: -32602, message })
                if method == "thread/turns/list" && message == "thread not found"
        ));
        assert_eq!(client.state(), ClientState::Ready);

        let private_content = "private turn content";
        for malformed_result in [
            json!({"data": {}}),
            json!({"data": private_content}),
            json!({"nextCursor": null}),
            json!({"data": [], "nextCursor": 42}),
            json!([[], null]),
            json!({"data": [null]}),
            json!({"data": [{"status": "completed", "items": []}]}),
            json!({"data": [{"id": 42, "status": "completed", "items": []}]}),
            json!({"data": [{"id": "turn", "items": []}]}),
            json!({"data": [{"id": "turn", "status": false, "items": []}]}),
            json!({"data": [{"id": "turn", "status": "completed"}]}),
            json!({"data": [
                {"id": "valid-turn", "status": "completed", "items": []},
                {"id": "invalid-turn", "status": "completed", "items": private_content}
            ]}),
        ] {
            let (client, mut outgoing, _) = ready_client();
            let result = list_thread_turns_with_response(
                &client,
                &mut outgoing,
                params.clone(),
                expected_params.clone(),
                json!({"result": malformed_result}),
            )
            .await;
            assert!(matches!(
                result,
                Err(ClientError::ProtocolViolation { reason })
                    if reason.starts_with("invalid thread/turns/list response payload: ")
                        && !reason.contains(private_content)
            ));
            assert_eq!(client.state(), ClientState::Failed);
        }
    }

    #[tokio::test]
    async fn run_turn_uses_the_response_id_and_delivers_early_completion() {
        let (client, mut outgoing, _) = ready_client();
        let (outcome, ()) = tokio::join!(
            client.run_turn(TurnStartParams::new(crate::ThreadId::new("thread"), vec![])),
            async {
                let Some(Outgoing::Message { value, ack: None }) = outgoing.recv().await else {
                    panic!("expected turn/start request");
                };
                assert_eq!(value["method"], "turn/start");
                for notification in [
                    json!({"method": "item/completed", "params": {
                        "threadId": "thread", "turnId": "turn", "completedAtMs": 1,
                        "item": {"id": "item", "type": "agentMessage", "text": "final", "phase": 42}
                    }}),
                    json!({"method": "turn/completed", "params": {
                        "threadId": "thread", "turn": {"id": "turn", "status": "completed", "items": []}
                    }}),
                ] {
                    client.inner.dispatch(notification).await.unwrap();
                }
                client
                    .inner
                    .dispatch(json!({"id": value["id"], "result": {"turn": {"id": "turn"}}}))
                    .await
                    .unwrap();
            }
        );
        let outcome = outcome.unwrap();
        assert_eq!(outcome.turn_id.as_str(), "turn");
        assert_eq!(outcome.final_agent_message(), Some("final"));
    }

    #[tokio::test]
    async fn raw_claim_returns_consumable_value_and_sends_success_response() {
        let (client, mut outgoing_receiver, _) = ready_client();
        let request_id = RequestId::String("claimed".to_owned());
        register_raw_request(&client, &request_id);

        let claimed = client.claim_server_request(request_id.clone()).unwrap();

        assert!(client.pending_server_requests().is_empty());
        assert!(matches!(
            client.claim_server_request(request_id.clone()),
            Err(ClientError::ServerRequestAlreadyResponded { .. })
        ));
        let response = tokio::join!(
            claimed.respond(json!({"accepted": true})),
            acknowledge_next_message(&mut outgoing_receiver),
        );
        response.0.unwrap();
        assert_eq!(response.1["id"], json!(request_id));
        assert_eq!(response.1["result"], json!({"accepted": true}));
    }

    #[tokio::test]
    async fn failed_claimed_response_send_rolls_back_to_pending() {
        let (client, outgoing_receiver, _) = ready_client();
        let request_id = RequestId::String("rollback".to_owned());
        register_raw_request(&client, &request_id);
        let claimed = client.claim_server_request(request_id).unwrap();
        drop(outgoing_receiver);

        assert!(matches!(
            claimed.respond(Value::Null).await,
            Err(ClientError::StdinWriteFailed { .. })
        ));
        assert_eq!(client.pending_server_requests().len(), 1);
    }

    #[tokio::test]
    async fn existing_respond_server_request_still_sends_success_response() {
        let (client, mut outgoing_receiver, _) = ready_client();
        let request_id = RequestId::String("existing".to_owned());
        register_raw_request(&client, &request_id);

        let response = tokio::join!(
            client.respond_server_request(
                request_id.clone(),
                ServerRequestResult::new(json!({"existing": true})),
            ),
            acknowledge_next_message(&mut outgoing_receiver),
        );

        response.0.unwrap();
        assert_eq!(response.1["id"], json!(request_id));
        assert_eq!(response.1["result"], json!({"existing": true}));
    }

    #[tokio::test]
    async fn claimed_request_keeps_client_alive_after_original_handle_is_dropped() {
        let (client, mut outgoing_receiver, writer_stop_receiver) = ready_client();
        let request_id = RequestId::String("lifetime".to_owned());
        register_raw_request(&client, &request_id);
        let claimed = client.claim_server_request(request_id.clone()).unwrap();
        assert_eq!(client.handles.load(Ordering::Acquire), 2);

        drop(client);

        assert!(!*writer_stop_receiver.borrow());
        let response = tokio::join!(
            claimed.respond(json!({"alive": true})),
            acknowledge_next_message(&mut outgoing_receiver),
        );
        response.0.unwrap();
        assert_eq!(response.1["id"], json!(request_id));
        assert_eq!(response.1["result"], json!({"alive": true}));
        assert!(*writer_stop_receiver.borrow());
    }
}
