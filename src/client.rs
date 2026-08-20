//! caller が使用する high-level client API と、その handle ownership を定義する。
//!
//! `Client` は単一 app-server process を共有する cloneable handle である。最後の handle が drop された場合は
//! async cleanup を待てないため process group を fail-safe に強制終了する。正常終了が必要な caller は明示的に
//! [`Client::shutdown`] を await する。

use crate::event::{EventReceiver, ServerRequest};
use crate::process::{ProcessCommand, spawn_process};
use crate::protocol::{
    ClientInfo, InitializeParams, InitializeResponse, RpcError, ServerRequestId,
    ServerRequestResult, ThreadId, ThreadStartOutcome, ThreadStartParams, ThreadStartResponse,
    TurnId, TurnOutcome, TurnStartParams, TurnStartResponse,
};
use crate::runtime::{RequestPermission, Shared};
use crate::shutdown;
use crate::state::{ClientState, ShutdownReport};
use crate::{ClientConfig, ClientError};
use serde_json::json;
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
            let response: InitializeResponse = inner
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
        let response: ThreadStartResponse = self
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

    /// turn を開始し、対応する terminal `turn/completed` notification まで待機する。
    ///
    /// app-server は `turn/start` response より先に terminal notification を送る可能性があるため、runtime state が
    /// 両者の到着順を吸収する。同一 client 上で別の active turn が存在する場合は開始しない。
    pub async fn run_turn(&self, params: TurnStartParams) -> Result<TurnOutcome, ClientError> {
        // request を送る前に active turn slot を確保し、早着した completion を取りこぼさない。
        let waiter_receiver = self.inner.begin_turn(params.thread_id.clone())?;
        let response = self
            .inner
            .send_request::<_, TurnStartResponse>("turn/start", &params, RequestPermission::Ready)
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
