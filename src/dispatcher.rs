//! deserialize 済み JSON value を response、notification、ServerRequest へ route する dispatcher。
//!
//! envelope を分類し、使用する payload field と request ID・turn lifecycle の整合性を検証する。
//! Generic / Unknown Notification と ServerRequest は raw JSON を保持する。

use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    ClientError,
    event::{Event, RawNotification, ServerRequest, ServerRequestKind},
    protocol::{
        AgentMessageDeltaNotification, IncomingMessage, ItemCompletedNotification,
        ItemStartedNotification, RequestId, RpcError, ServerRequestId,
        ServerRequestResolvedNotification, ServerRequestResult, ThreadId,
        ThreadStatusChangedNotification, TurnCompletedNotification, TurnCompletedParams, TurnId,
        TurnOutcome, TurnStartResponse, TurnStartedNotification, TypedServerRequestKind,
        WireObject, classify_message, is_known_server_notification_method,
        is_known_server_request_method, typed_server_request_kind, validate_permission_profile,
    },
    runtime::Shared,
    state::PendingResponse,
};

impl Shared {
    /// active turn を解放し、その waiter へ同じ failure を返す。
    pub(crate) fn clear_active_turn_with_error(&self, error: ClientError) {
        let waiter = self.lock_state().clear_active_turn();
        if let Some(waiter) = waiter {
            let _ = waiter.send(Err(error));
        }
    }

    /// ServerRequest を送信前に atomically claim し、二重 response と result kind mismatch を拒否する。
    pub(crate) fn claim_server_request(
        &self,
        request_id: &ServerRequestId,
        result: Option<&ServerRequestResult>,
    ) -> Result<(), ClientError> {
        self.lock_state().claim_server_request(request_id, result)
    }

    /// claim 済み ServerRequest response に original request ID を挿入し、stdin flush 完了まで待機する。
    ///
    /// write failure 時は local claim を rollback する。ただし transport failure 自体は client を `Failed` へ
    /// 遷移させる可能性があり、再送可能性を保証するものではない。
    pub(crate) async fn send_claimed_server_request(
        &self,
        request_id: ServerRequestId,
        mut value: Value,
    ) -> Result<(), ClientError> {
        value
            .as_object_mut()
            .expect("response envelope is an object")
            .insert(
                "id".to_owned(),
                serde_json::to_value(&request_id).expect("ServerRequestId is always serializable"),
            );
        if let Err(error) = self.send_value(value, true).await {
            self.lock_state().rollback_server_request_claim(&request_id);
            return Err(error);
        }
        self.lock_state()
            .complete_server_request_response(&request_id);
        Ok(())
    }

    /// 一つの incoming JSON value を envelope 種別に分類し、対応する state transition へ route する。
    pub(crate) async fn dispatch(&self, value: Value) -> Result<(), ClientError> {
        match classify_message(value)? {
            IncomingMessage::Success { id, result } => self.route_response(id, Ok(result)),
            IncomingMessage::Error { id, error } => self.route_rpc_error(id, error),
            IncomingMessage::ServerRequest {
                id,
                method,
                params,
                raw,
            } => self.route_server_request(id, method, params, raw).await,
            IncomingMessage::Notification {
                method,
                params,
                raw,
            } => self.route_notification(method, params, raw),
        }
    }

    /// success response を pending client request と一度だけ対応付ける。
    ///
    /// detached request の response は正常に破棄するが、未発行または既に完了した ID の response は protocol violation とする。
    fn route_response(
        &self,
        id: RequestId,
        result: Result<Value, ClientError>,
    ) -> Result<(), ClientError> {
        let id = client_response_id(id)?;
        let pending = match self.lock_state().take_pending_response(id) {
            PendingResponse::Pending(pending) => pending,
            PendingResponse::Cancelled => return Ok(()),
            PendingResponse::Unknown => {
                return Err(ClientError::ProtocolViolation {
                    reason: format!("response for unknown or completed request ID {id}"),
                });
            }
        };

        if pending.method() == "turn/start"
            && let Ok(value) = &result
        {
            let WireObject(response): WireObject<TurnStartResponse> =
                serde_json::from_value(value.clone()).map_err(|error| {
                    ClientError::ProtocolViolation {
                        reason: format!("invalid turn/start response: {error}"),
                    }
                })?;
            // turn ID が判明する前に completion が早着している場合、bind 時点で waiter へ deliver する。
            self.bind_turn_id(response.turn.id)?;
        }
        pending.complete(result);
        Ok(())
    }

    /// JSON-RPC error response を pending request と対応付ける。
    ///
    /// turn/start が拒否された場合は response waiter だけでなく active turn waiter も同じ error で解放する。
    fn route_rpc_error(&self, id: RequestId, error: RpcError) -> Result<(), ClientError> {
        let id = client_response_id(id)?;
        let pending = match self.lock_state().take_pending_response(id) {
            PendingResponse::Pending(pending) => pending,
            PendingResponse::Cancelled => return Ok(()),
            PendingResponse::Unknown => {
                return Err(ClientError::ProtocolViolation {
                    reason: format!("error response for unknown or completed request ID {id}"),
                });
            }
        };
        let method = pending.method().to_owned();
        let rpc_error = ClientError::RpcFailed {
            method: method.clone(),
            request_id: id,
            code: error.code,
            message: error.message,
        };
        if method == "turn/start" {
            self.clear_active_turn_with_error(rpc_error.clone());
        }
        pending.complete(Err(rpc_error));
        Ok(())
    }

    /// turn/start response の turn ID を active turn slot へ binding する。
    fn bind_turn_id(&self, turn_id: TurnId) -> Result<(), ClientError> {
        if let Some(completed) = self.lock_state().bind_turn_id(turn_id)? {
            completed.deliver();
        }
        Ok(())
    }

    /// notification を typed conversion と state update の後に event として公開する。
    ///
    /// 専用型を持たない method は checked-in Schema を使って分類し、raw event として保持する。
    fn route_notification(
        &self,
        method: String,
        params: Value,
        raw: Value,
    ) -> Result<(), ClientError> {
        let event = match method.as_str() {
            "turn/completed" => {
                let completed =
                    TurnCompletedNotification::from_wire(deserialize_protocol::<
                        TurnCompletedParams,
                    >(&params, &method)?);
                if !completed.turn.status.is_terminal() {
                    return Err(ClientError::ProtocolViolation {
                        reason: "turn/completed contains a non-terminal turn".to_owned(),
                    });
                }
                let outcome = TurnOutcome {
                    thread_id: completed.thread_id.clone(),
                    turn_id: completed.turn.id.clone(),
                    turn: completed.turn.clone(),
                };
                // event 配信前に state を更新し、event consumer が即座に次 turn を開始できるようにする。
                self.receive_turn_completion(outcome)?;
                Event::TurnCompleted(completed)
            }
            "turn/started" => Event::TurnStarted(deserialize_protocol::<TurnStartedNotification>(
                &params, &method,
            )?),
            "item/started" => Event::ItemStarted(deserialize_protocol::<ItemStartedNotification>(
                &params, &method,
            )?),
            "item/completed" => {
                let completed: ItemCompletedNotification = deserialize_protocol(&params, &method)?;
                self.record_completed_agent_message(&completed)?;
                Event::ItemCompleted(completed)
            }
            "item/agentMessage/delta" => Event::AgentMessageDelta(deserialize_protocol::<
                AgentMessageDeltaNotification,
            >(&params, &method)?),
            "thread/status/changed" => Event::ThreadStatusChanged(deserialize_protocol::<
                ThreadStatusChangedNotification,
            >(&params, &method)?),
            "serverRequest/resolved" => {
                let resolved: ServerRequestResolvedNotification =
                    deserialize_protocol(&params, &method)?;
                self.lock_state()
                    .resolve_server_request(&resolved.request_id);
                Event::ServerRequestResolved(resolved)
            }
            _ if is_known_server_notification_method(&method) => {
                Event::GenericNotification(RawNotification {
                    method,
                    params,
                    raw,
                })
            }
            _ => Event::UnknownNotification(RawNotification {
                method,
                params,
                raw,
            }),
        };
        self.publish_event(event);
        Ok(())
    }

    ///完成済み agent message を event 配信前に active turn へ記録する。
    fn record_completed_agent_message(
        &self,
        completed: &ItemCompletedNotification,
    ) -> Result<(), ClientError> {
        self.lock_state().record_completed_agent_message(
            &completed.thread_id,
            &completed.turn_id,
            &completed.item,
        )
    }

    /// terminal outcome を active turn state へ適用し、deliver 可能なら waiter を解放する。
    fn receive_turn_completion(&self, outcome: TurnOutcome) -> Result<(), ClientError> {
        if let Some(completed) = self.lock_state().receive_turn_completion(outcome)? {
            completed.deliver();
        }
        Ok(())
    }

    /// app-server からの request を validation、classification、pending registration 後に event として公開する。
    ///
    /// registration を event 配信より先に行うことで、event consumer が直ちに response API を呼んでも request が必ず存在する。
    async fn route_server_request(
        &self,
        id: RequestId,
        method: String,
        params: Value,
        raw: Value,
    ) -> Result<(), ClientError> {
        let (kind, typed_kind) =
            classify_server_request(&method, is_known_server_request_method(&method));
        // correlation metadata は shutdown cleanup と turn 完了時の stale request 除去に使用する。
        let (thread_id, turn_id) = if kind == ServerRequestKind::Unsupported {
            (
                params.get("threadId").and_then(Value::as_str),
                params.get("turnId").and_then(Value::as_str),
            )
        } else {
            let requires_turn = typed_kind.is_some() || method == "item/tool/call";
            let is_elicitation = method == "mcpServer/elicitation/request";
            (
                server_request_context_id(
                    &params,
                    "threadId",
                    requires_turn || is_elicitation,
                    false,
                )?,
                // MCP elicitation は active turn に紐づかない場合だけ turnId を省略または null にできる。
                server_request_context_id(&params, "turnId", requires_turn, is_elicitation)?,
            )
        };
        let thread_id = thread_id.map(ThreadId::new);
        let turn_id = turn_id.map(TurnId::new);
        if typed_kind == Some(TypedServerRequestKind::PermissionApproval) {
            let permissions =
                params
                    .get("permissions")
                    .ok_or_else(|| ClientError::ProtocolViolation {
                        reason: "permission approval is missing permissions".to_owned(),
                    })?;
            validate_permission_profile(permissions).map_err(|reason| {
                ClientError::ProtocolViolation {
                    reason: format!("invalid permission approval profile: {reason}"),
                }
            })?;
        }
        let request = ServerRequest {
            id,
            method,
            kind,
            params,
            raw,
        };
        let registration = {
            self.lock_state().register_server_request(
                request.clone(),
                typed_kind,
                thread_id,
                turn_id,
            )
        };
        match registration {
            Ok(()) => {}
            Err(error @ ClientError::CapacityExceeded { .. }) => {
                self.send_value(
                    json!({
                        "id": request.id,
                        "error": {"code": -32000, "message": error.to_string()}
                    }),
                    true,
                )
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        self.publish_event(Event::ServerRequest(request));
        Ok(())
    }
}

fn server_request_context_id<'a>(
    params: &'a Value,
    field: &str,
    required: bool,
    nullable: bool,
) -> Result<Option<&'a str>, ClientError> {
    match params.get(field) {
        None if !required => Ok(None),
        Some(Value::Null) if nullable => Ok(None),
        Some(Value::String(id)) => Ok(Some(id)),
        _ => Err(ClientError::ProtocolViolation {
            reason: format!("ServerRequest {field} must be a string"),
        }),
    }
}

/// method を public classification と typed-result validation kind へ変換する。
fn classify_server_request(
    method: &str,
    schema_known: bool,
) -> (ServerRequestKind, Option<TypedServerRequestKind>) {
    if !schema_known {
        return (ServerRequestKind::Unsupported, None);
    }
    let typed_kind = typed_server_request_kind(method);
    let public_kind = match typed_kind {
        Some(TypedServerRequestKind::CommandExecutionApproval) => {
            ServerRequestKind::CommandExecutionApproval
        }
        Some(TypedServerRequestKind::FileChangeApproval) => ServerRequestKind::FileChangeApproval,
        Some(TypedServerRequestKind::PermissionApproval) => ServerRequestKind::PermissionApproval,
        Some(TypedServerRequestKind::UserInput) => ServerRequestKind::UserInput,
        None => ServerRequestKind::Known,
    };
    (public_kind, typed_kind)
}

/// method 固有 payload の deserialize error を protocol violation へ正規化する。
fn deserialize_protocol<T: DeserializeOwned>(
    value: &Value,
    operation: &str,
) -> Result<T, ClientError> {
    let WireObject(payload) =
        serde::Deserialize::deserialize(value).map_err(|error| ClientError::ProtocolViolation {
            reason: format!("invalid {operation} payload: {error}"),
        })?;
    Ok(payload)
}

/// client が発行した response ID の domain を positive integer に制限する。
fn client_response_id(id: RequestId) -> Result<i64, ClientError> {
    match id {
        RequestId::Integer(id) if id > 0 => Ok(id),
        _ => Err(ClientError::ProtocolViolation {
            reason: "client response ID must be a positive integer".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::{broadcast, mpsc, watch};

    use super::*;
    use crate::{state::ClientState, transport::Outgoing};

    fn ready_shared() -> (Shared, mpsc::Receiver<Outgoing>) {
        let (outgoing_sender, outgoing_receiver) = mpsc::channel(1);
        let (event_sender, _) = broadcast::channel(16);
        let (process_sender, _) = mpsc::channel(1);
        let (writer_stop, _) = watch::channel(false);
        let shared = Shared::new(
            outgoing_sender,
            event_sender,
            process_sender,
            writer_stop,
            std::time::Duration::from_secs(1),
        );
        {
            let mut state = shared.lock_state();
            state.begin_initialization();
            state.finish_initialization().unwrap();
        }
        (shared, outgoing_receiver)
    }

    async fn acknowledge_next_message(receiver: &mut mpsc::Receiver<Outgoing>) -> Value {
        let Outgoing::Message { value, ack } = receiver.recv().await.unwrap() else {
            panic!("expected response message");
        };
        ack.unwrap().send(Ok(())).unwrap();
        value
    }

    #[test]
    fn classifies_typed_known_and_unknown_server_requests() {
        assert_eq!(
            classify_server_request("item/commandExecution/requestApproval", true),
            (
                ServerRequestKind::CommandExecutionApproval,
                Some(TypedServerRequestKind::CommandExecutionApproval)
            )
        );
        assert_eq!(
            classify_server_request("item/permissions/requestApproval", true),
            (
                ServerRequestKind::PermissionApproval,
                Some(TypedServerRequestKind::PermissionApproval)
            )
        );
        assert_eq!(
            classify_server_request("account/chatgptAuthTokens/refresh", true),
            (ServerRequestKind::Known, None)
        );
        assert_eq!(
            classify_server_request("future/request", false),
            (ServerRequestKind::Unsupported, None)
        );
    }

    #[tokio::test]
    async fn notifications_validate_typed_fields_and_preserve_raw_events() {
        let (shared, _) = ready_shared();
        let mut events = shared.subscribe();
        let started = json!({
            "method": "turn/started",
            "params": {
                "threadId": "thread",
                "turn": {
                    "id": "turn", "status": "inProgress", "items": [],
                    "itemsView": {"future": true}, "rootTurnId": 42
                },
                "extra": true
            }
        });
        shared.dispatch(started.clone()).await.unwrap();
        let Event::TurnStarted(event) = events.recv().await.unwrap() else {
            panic!("expected typed notification");
        };
        assert_eq!(event.turn.id.as_str(), "turn");

        for (raw, known) in [
            (
                json!({"method": "thread/deleted", "params": {"threadId": 42}, "extra": true}),
                true,
            ),
            (
                json!({"method": "thread/search/partial", "params": {"requestId": 1, "data": []}}),
                true,
            ),
            (
                json!({"method": "thread/section/changed", "params": {"threadId": "thread", "sectionId": null}}),
                true,
            ),
            (
                json!({"method": "future/event", "params": [42], "extra": true}),
                false,
            ),
        ] {
            shared.dispatch(raw.clone()).await.unwrap();
            let event = match (events.recv().await.unwrap(), known) {
                (Event::GenericNotification(event), true)
                | (Event::UnknownNotification(event), false) => event,
                _ => panic!("unexpected raw notification classification"),
            };
            assert_eq!(event.method, raw["method"]);
            assert_eq!(event.params, raw["params"]);
            assert_eq!(event.raw, raw);
        }

        for (turn, detail) in [
            (
                json!({"id": 42, "status": "inProgress", "items": []}),
                "invalid type: integer `42`, expected a string",
            ),
            (
                json!({"id": "turn", "status": "future", "items": []}),
                "unknown variant `future`",
            ),
            (
                json!({"id": "turn", "status": "inProgress"}),
                "missing field `items`",
            ),
            (
                json!(["turn", [], "inProgress", null, null, null, null]),
                "expected a map",
            ),
            (
                json!({"id": "turn", "status": "failed", "items": [], "error": ["msg", null, null]}),
                "expected a map",
            ),
        ] {
            let mut raw = started.clone();
            raw["params"]["turn"] = turn;
            let Err(ClientError::ProtocolViolation { reason }) = shared.dispatch(raw).await else {
                panic!("expected ProtocolViolation");
            };
            assert!(
                reason.starts_with("invalid turn/started payload: "),
                "{reason}"
            );
            assert!(reason.contains(detail), "{reason}");
        }
        assert!(matches!(
            shared
                .dispatch(json!({
                    "method": "thread/status/changed",
                    "params": {"threadId": "thread", "status": ["idle"]}
                }))
                .await,
            Err(ClientError::ProtocolViolation { .. })
        ));
    }

    #[tokio::test]
    async fn server_requests_preserve_raw_payloads_and_result_kind_checks() {
        let (shared, _) = ready_shared();
        let mut events = shared.subscribe();
        for (method, params, kind) in [
            (
                "item/tool/requestUserInput",
                json!({"threadId": "thread", "turnId": "turn", "questions": 42}),
                ServerRequestKind::UserInput,
            ),
            (
                "item/permissions/requestApproval",
                json!({"threadId": "thread", "turnId": "turn", "permissions": {"network": {"enabled": true}}, "cwd": false}),
                ServerRequestKind::PermissionApproval,
            ),
            (
                "mcpServer/elicitation/request",
                json!({"threadId": "thread", "turnId": null, "requestedSchema": {"properties": {}}}),
                ServerRequestKind::Known,
            ),
            (
                "future/request",
                json!([42]),
                ServerRequestKind::Unsupported,
            ),
        ] {
            let raw = json!({"id": method, "method": method, "params": params, "extra": true});
            shared.dispatch(raw.clone()).await.unwrap();
            let Event::ServerRequest(request) = events.recv().await.unwrap() else {
                panic!("expected ServerRequest");
            };
            assert_eq!(request.kind, kind);
            assert_eq!(request.method, method);
            assert_eq!(request.params, raw["params"]);
            assert_eq!(request.raw, raw);
            if matches!(
                kind,
                ServerRequestKind::UserInput | ServerRequestKind::PermissionApproval
            ) {
                assert!(matches!(
                    shared.claim_server_request(
                        &request.id,
                        Some(&ServerRequestResult::new(json!({})))
                    ),
                    Err(ClientError::InvalidServerRequestResult { .. })
                ));
            }
        }
        assert_eq!(shared.pending_server_requests().len(), 4);
    }

    #[tokio::test]
    async fn unknown_server_requests_only_use_string_correlation_ids() {
        let (shared, _) = ready_shared();
        let mut events = shared.subscribe();
        let turn_waiter = shared.begin_turn(ThreadId::new("thread")).unwrap();
        shared.bind_turn_id(TurnId::new("turn")).unwrap();

        for (id, params) in [
            ("bad-thread", json!({"threadId": false, "turnId": "turn"})),
            (
                "bad-turn",
                json!({"threadId": "thread", "turnId": ["turn"]}),
            ),
            ("matching", json!({"threadId": "thread", "turnId": "turn"})),
        ] {
            let raw = json!({"id": id, "method": "future/request", "params": params});
            shared.dispatch(raw.clone()).await.unwrap();
            let Event::ServerRequest(request) = events.try_recv().unwrap() else {
                panic!("expected ServerRequest");
            };
            assert_eq!(request.kind, ServerRequestKind::Unsupported);
            assert_eq!(request.params, params);
            assert_eq!(request.raw, raw);
        }
        assert_eq!(shared.pending_server_requests().len(), 3);

        shared
            .dispatch(json!({"method": "turn/completed", "params": {
                "threadId": "thread", "turn": {"id": "turn", "items": [], "status": "completed"}
            }}))
            .await
            .unwrap();
        turn_waiter.await.unwrap().unwrap();
        let mut pending: Vec<_> = shared
            .pending_server_requests()
            .into_iter()
            .map(|request| request.id.to_string())
            .collect();
        pending.sort();
        assert_eq!(pending, ["bad-thread", "bad-turn"]);
    }

    #[tokio::test]
    async fn server_requests_reject_invalid_correlation_fields_and_permissions() {
        let (shared, _) = ready_shared();
        for (method, params) in [
            (
                "item/commandExecution/requestApproval",
                json!({"turnId": "turn"}),
            ),
            (
                "item/fileChange/requestApproval",
                json!({"threadId": "thread"}),
            ),
            (
                "item/tool/requestUserInput",
                json!({"threadId": 42, "turnId": "turn"}),
            ),
            (
                "item/tool/call",
                json!({"threadId": "thread", "turnId": null}),
            ),
            (
                "mcpServer/elicitation/request",
                json!({"threadId": "thread", "turnId": 42}),
            ),
            (
                "item/commandExecution/requestApproval",
                json!({"threadId": false, "turnId": "turn"}),
            ),
            (
                "item/fileChange/requestApproval",
                json!({"threadId": "thread", "turnId": ["turn"]}),
            ),
            (
                "item/permissions/requestApproval",
                json!({"threadId": "thread", "turnId": "turn"}),
            ),
            (
                "item/permissions/requestApproval",
                json!({"threadId": "thread", "turnId": "turn", "permissions": null}),
            ),
            (
                "item/permissions/requestApproval",
                json!({"threadId": "thread", "turnId": "turn", "permissions": {"network": {"enabled": "yes"}}}),
            ),
        ] {
            assert!(
                matches!(
                    shared
                        .dispatch(json!({"id": 1, "method": method, "params": params}))
                        .await,
                    Err(ClientError::ProtocolViolation { .. })
                ),
                "{method}"
            );
        }
        assert!(shared.pending_server_requests().is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_response_ids_and_turn_completions() {
        let (shared, _) = ready_shared();
        let _waiter = shared.begin_turn(ThreadId::new("thread")).unwrap();
        shared.bind_turn_id(TurnId::new("turn")).unwrap();
        for raw in [
            json!({"id": "1", "result": {}}),
            json!({"id": 1, "result": {}}),
            json!({"method": "turn/completed", "params": {
                "threadId": "thread", "turn": {"id": "turn", "items": [], "status": "inProgress"}
            }}),
            json!({"method": "turn/completed", "params": {
                "threadId": "thread", "turn": {"id": "other", "items": [], "status": "completed"}
            }}),
        ] {
            assert!(matches!(
                shared.dispatch(raw).await,
                Err(ClientError::ProtocolViolation { .. })
            ));
        }
    }

    #[tokio::test]
    async fn turn_start_response_requires_a_named_string_turn_id() {
        for result in [
            json!({}),
            json!({"turn": {"id": 42}}),
            json!({"turn": {}}),
            json!({"turn": ["turn"]}),
        ] {
            let (shared, _) = ready_shared();
            let _waiter = shared.begin_turn(ThreadId::new("thread")).unwrap();
            let (id, _response) = shared
                .lock_state()
                .register_request("turn/start", crate::runtime::RequestPermission::Ready)
                .unwrap();
            assert!(matches!(
                shared.dispatch(json!({"id": id, "result": result})).await,
                Err(ClientError::ProtocolViolation { .. })
            ));
        }
    }

    #[tokio::test]
    async fn successful_server_request_response_removes_tracking_entry() {
        let (shared, mut outgoing_receiver) = ready_shared();
        let request_id = RequestId::String("completed".to_owned());
        shared
            .dispatch(json!({
                "id": request_id.clone(),
                "method": "future/request",
                "params": {}
            }))
            .await
            .unwrap();
        shared.claim_server_request(&request_id, None).unwrap();

        let (send_result, response) = tokio::join!(
            shared.send_claimed_server_request(
                request_id.clone(),
                json!({"id": null, "result": {}}),
            ),
            acknowledge_next_message(&mut outgoing_receiver),
        );
        send_result.unwrap();
        assert_eq!(response["id"], json!(request_id));
        assert!(matches!(
            shared.claim_server_request(&request_id, None),
            Err(ClientError::UnknownServerRequestId { .. })
        ));
    }

    #[tokio::test]
    async fn server_request_capacity_overflow_is_request_local() {
        let (shared, mut outgoing_receiver) = ready_shared();
        let mut events = shared.subscribe();
        let mut turn_waiter = shared.begin_turn(ThreadId::new("thread")).unwrap();
        for index in 0..4 {
            shared
                .dispatch(json!({
                    "id": format!("request-{index}"),
                    "method": "future/request",
                    "params": {}
                }))
                .await
                .unwrap();
        }
        let overflow_id = RequestId::String("overflow".to_owned());

        let (dispatch_result, response) = tokio::join!(
            shared.dispatch(json!({
                "id": overflow_id.clone(),
                "method": "future/request",
                "params": {}
            })),
            acknowledge_next_message(&mut outgoing_receiver),
        );

        dispatch_result.unwrap();
        assert_eq!(response["id"], json!(overflow_id));
        assert!(response["error"].is_object());
        assert_eq!(shared.state(), ClientState::Ready);
        assert_eq!(shared.pending_server_requests().len(), 4);
        assert!(matches!(
            turn_waiter.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(event, Event::Failure(_)));
        }
    }
}
