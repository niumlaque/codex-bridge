//! client lifecycle、request correlation、active turn、pending ServerRequest を一つの state machine で管理する。
//!
//!この module の method は synchronous mutex 内で短時間だけ呼ばれ、I/O や await を行わない。state transition と
//! waiter ownership を同じ critical section で更新することで、response/notification の到着順、shutdown、failure の
//!競合を deterministic に解決する。

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use tokio::sync::oneshot;

use crate::{
    ClientError,
    event::ServerRequest,
    protocol::{
        RequestId, ServerRequestId, ServerRequestResult, ThreadId, ThreadItem, TurnId, TurnOutcome,
        TypedServerRequestKind,
    },
};

///同時に response を待てる client-originated request の最大数。
pub(crate) const MAX_PENDING_REQUESTS: usize = 64;
/// caller response を待つ ServerRequest の最大数。
///
/// raw payload を複数箇所で保持するため、memory 上限の一部として小さい固定値に制限する。
pub(crate) const MAX_PENDING_SERVER_REQUESTS: usize = 4;

/// public client lifecycle の snapshot。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientState {
    /// process 起動前または task setup 中。
    Starting,
    /// initialize handshake 中。通常 API はまだ使用できない。
    Initializing,
    /// request と turn を受理できる。
    Ready,
    /// shutdown sequence 中。新しい operation は受理しない。
    ShuttingDown,
    ///正常 shutdown が完了した。
    Stopped,
    /// primary failure を latch し、fail-closed cleanup 中または完了済み。
    Failed,
}

/// shutdown 時に強制終了が必要だったかを示す report。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShutdownReport {
    /// child process は自然終了したが descendant cleanup などで process group へ signal を送った場合も `true` となる。
    pub forced: bool,
}

/// request method を発行できる lifecycle phase。
#[derive(Clone, Copy)]
pub(crate) enum RequestPermission {
    /// initialize handshake 中だけ許可する request。
    Initializing,
    /// fully initialized な `Ready` state だけで許可する request。
    Ready,
}

/// raw JSON-RPC success value または client error を waiter へ返す型。
pub(crate) type PendingResult = Result<Value, ClientError>;

/// client-originated request と、その response waiter。
pub(crate) struct PendingRequest {
    /// response payload の typed conversion と error reporting に使用する method 名。
    method: String,
    sender: oneshot::Sender<PendingResult>,
}

impl PendingRequest {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    /// pending entry の ownership を消費し、waiter を一度だけ完了する。
    pub(crate) fn complete(self, result: PendingResult) {
        let _ = self.sender.send(result);
    }
}

///受信 response ID の correlation result。
pub(crate) enum PendingResponse {
    ///現在 response を待っている request。
    Pending(PendingRequest),
    /// detached または shutdown で waiter を取消済みの request。response は正常に破棄する。
    Cancelled,
    ///未発行、重複、または既に完了した ID。
    Unknown,
}

/// single-active-turn policy の state と terminal outcome waiter。
struct ActiveTurn {
    /// turn/start request 送信前から既知の parent thread ID。
    thread_id: ThreadId,
    /// turn/start response で初めて確定する turn ID。
    turn_id: Option<TurnId>,
    /// `run_turn` caller へ terminal outcome を返す waiter。
    waiter: Option<oneshot::Sender<Result<TurnOutcome, ClientError>>>,
    /// turn/start response より先に到着した terminal notification。
    early_terminal: Option<TurnOutcome>,
    /// `item/completed` で受信した最後の完成済み agent message。
    latest_agent_message: Option<(TurnId, ThreadItem)>,
}

/// pending ServerRequest の response claim state。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServerRequestState {
    Pending,
    /// caller が response を claim 済み。write failure 時だけ `Pending` へ rollback できる。
    Responded,
}

/// response validation と lifecycle cleanup に必要な ServerRequest metadata。
struct PendingServerRequest {
    state: ServerRequestState,
    /// typed method へ別 kind の result を返さないための expected kind。
    typed_kind: Option<TypedServerRequestKind>,
    /// turn 完了時に関連 request を除去するための optional correlation ID。
    thread_id: Option<ThreadId>,
    turn_id: Option<TurnId>,
    request: ServerRequest,
}

/// state lock の外で waiter delivery を行うために切り離した terminal outcome。
pub(crate) struct CompletedTurn {
    outcome: TurnOutcome,
    waiter: Option<oneshot::Sender<Result<TurnOutcome, ClientError>>>,
}

impl CompletedTurn {
    /// lock を保持せず terminal outcome を caller へ送る。
    pub(crate) fn deliver(self) {
        if let Some(waiter) = self.waiter {
            let _ = waiter.send(Ok(self.outcome));
        }
    }
}

/// first failure 記録時に state から切り離す全 waiter。
pub(crate) struct FailureWaiters {
    pub(crate) pending_requests: Vec<oneshot::Sender<PendingResult>>,
    pub(crate) active_turn: Option<oneshot::Sender<Result<TurnOutcome, ClientError>>>,
}

/// shutdown sequence が lock の外で protocol cleanup と waiter delivery を行うための snapshot。
pub(crate) struct ShutdownPreparation {
    /// turn ID が確定している active turn。best-effort interrupt に使用する。
    pub(crate) active_turn: Option<(ThreadId, TurnId)>,
    pub(crate) active_waiter: Option<oneshot::Sender<Result<TurnOutcome, ClientError>>>,
    pub(crate) pending_requests: Vec<oneshot::Sender<PendingResult>>,
    /// shutdown error response を送る pending ServerRequest ID。
    pub(crate) pending_server_requests: Vec<RequestId>,
    /// shutdown 開始前に primary failure が記録済みだったか。
    pub(crate) was_failed: bool,
}

///一つの app-server process に対する authoritative mutable state。
pub(crate) struct RuntimeState {
    lifecycle: ClientState,
    ///最初に成立した fatal error。後続 error で上書きしない。
    primary_failure: Option<ClientError>,
    ///次に割り当てる positive request ID。`0` は exhausted sentinel。
    next_request_id: i64,
    pending_requests: HashMap<i64, PendingRequest>,
    /// waiter を持たないが、将来の response を正常に破棄すべき request ID。
    cancelled_response_ids: HashSet<i64>,
    active_turn: Option<ActiveTurn>,
    pending_server_requests: HashMap<RequestId, PendingServerRequest>,
    /// idempotent shutdown caller へ再利用する最終 result。
    shutdown_report: Option<Result<ShutdownReport, ClientError>>,
}

impl RuntimeState {
    /// side effect を持たない `Starting` state を生成する。
    pub(crate) fn new() -> Self {
        Self {
            lifecycle: ClientState::Starting,
            primary_failure: None,
            next_request_id: 1,
            pending_requests: HashMap::new(),
            cancelled_response_ids: HashSet::new(),
            active_turn: None,
            pending_server_requests: HashMap::new(),
            shutdown_report: None,
        }
    }

    pub(crate) fn lifecycle(&self) -> ClientState {
        self.lifecycle
    }

    pub(crate) fn primary_failure(&self) -> Option<ClientError> {
        self.primary_failure.clone()
    }

    /// background task が new failure を primary として記録すべきでない lifecycle か判定する。
    pub(crate) fn is_stopping(&self) -> bool {
        matches!(
            self.lifecycle,
            ClientState::ShuttingDown | ClientState::Stopped | ClientState::Failed
        )
    }

    /// process/task setup 後、initialize request 送信前に lifecycle を遷移させる。
    pub(crate) fn begin_initialization(&mut self) {
        self.lifecycle = ClientState::Initializing;
    }

    /// initialize handshake 全体の成功後にだけ `Ready` へ遷移する。
    pub(crate) fn finish_initialization(&mut self) -> Result<(), ClientError> {
        if self.lifecycle != ClientState::Initializing {
            return Err(self
                .primary_failure
                .clone()
                .unwrap_or(ClientError::ClientNotReady));
        }
        self.lifecycle = ClientState::Ready;
        Ok(())
    }

    /// ready-only operation の admission を lifecycle-specific error で判定する。
    pub(crate) fn ensure_ready(&self) -> Result<(), ClientError> {
        match self.lifecycle {
            ClientState::Ready => Ok(()),
            ClientState::ShuttingDown => Err(ClientError::ClientShuttingDown),
            ClientState::Stopped => Err(ClientError::ClientStopped),
            ClientState::Failed => {
                Err(self
                    .primary_failure
                    .clone()
                    .unwrap_or(ClientError::ClientFailed {
                        cause: "unknown primary failure".to_owned(),
                    }))
            }
            ClientState::Starting | ClientState::Initializing => Err(ClientError::ClientNotReady),
        }
    }

    /// method ごとに許可された lifecycle phase を検証する。
    fn ensure_permission(&self, permission: RequestPermission) -> Result<(), ClientError> {
        match permission {
            RequestPermission::Ready => self.ensure_ready(),
            RequestPermission::Initializing if self.lifecycle == ClientState::Initializing => {
                Ok(())
            }
            RequestPermission::Initializing => Err(ClientError::ClientNotReady),
        }
    }

    /// bounded pending table を確認し、再利用しない positive request ID を割り当てる。
    pub(crate) fn allocate_request_id(&mut self) -> Result<i64, ClientError> {
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            return Err(ClientError::CapacityExceeded {
                resource: "pending client requests",
                limit: MAX_PENDING_REQUESTS,
            });
        }
        if self.next_request_id <= 0 {
            return Err(ClientError::RequestIdExhausted);
        }
        let id = self.next_request_id;
        self.next_request_id = if id == i64::MAX { 0 } else { id + 1 };
        Ok(id)
    }

    /// request ID と response waiter を同一 critical section で登録する。
    pub(crate) fn register_request(
        &mut self,
        method: &'static str,
        permission: RequestPermission,
    ) -> Result<(i64, oneshot::Receiver<Result<Value, ClientError>>), ClientError> {
        self.ensure_permission(permission)?;
        let id = self.allocate_request_id()?;
        let (sender, receiver) = oneshot::channel();
        self.pending_requests.insert(
            id,
            PendingRequest {
                method: method.to_owned(),
                sender,
            },
        );
        Ok((id, receiver))
    }

    /// queue send failure 時に未送信 request の registration を rollback する。
    pub(crate) fn remove_pending_request(&mut self, id: i64) {
        self.pending_requests.remove(&id);
    }

    /// response waiter を持たない request ID を登録する。
    pub(crate) fn register_detached_request(&mut self) -> Result<i64, ClientError> {
        let id = self.allocate_request_id()?;
        self.cancelled_response_ids.insert(id);
        Ok(id)
    }

    pub(crate) fn remove_detached_request(&mut self, id: i64) {
        self.cancelled_response_ids.remove(&id);
    }

    /// incoming response ID を pending、cancelled、unknown のいずれかへ atomically 分類する。
    pub(crate) fn take_pending_response(&mut self, id: i64) -> PendingResponse {
        if let Some(pending) = self.pending_requests.remove(&id) {
            PendingResponse::Pending(pending)
        } else if self.cancelled_response_ids.remove(&id) {
            PendingResponse::Cancelled
        } else {
            PendingResponse::Unknown
        }
    }

    /// caller へ公開する未応答 ServerRequest の snapshot を clone する。
    pub(crate) fn pending_server_requests(&self) -> Vec<ServerRequest> {
        self.pending_server_requests
            .values()
            .filter(|pending| pending.state == ServerRequestState::Pending)
            .map(|pending| pending.request.clone())
            .collect()
    }

    /// validated ServerRequest を bounded pending table へ登録する。
    pub(crate) fn register_server_request(
        &mut self,
        request: ServerRequest,
        typed_kind: Option<TypedServerRequestKind>,
        thread_id: Option<ThreadId>,
        turn_id: Option<TurnId>,
    ) -> Result<(), ClientError> {
        if self.pending_server_requests.len() >= MAX_PENDING_SERVER_REQUESTS {
            return Err(ClientError::CapacityExceeded {
                resource: "pending ServerRequests",
                limit: MAX_PENDING_SERVER_REQUESTS,
            });
        }
        if self.pending_server_requests.contains_key(&request.id) {
            return Err(ClientError::ProtocolViolation {
                reason: format!("duplicate ServerRequest ID {}", request.id),
            });
        }
        self.pending_server_requests.insert(
            request.id.clone(),
            PendingServerRequest {
                state: ServerRequestState::Pending,
                typed_kind,
                thread_id,
                turn_id,
                request,
            },
        );
        Ok(())
    }

    /// ServerRequest response の exclusive claim と typed result validation を atomically 実行する。
    ///
    /// actual stdin write は lock の外で行うため、先に `Responded` へ遷移させて concurrent duplicate response を防ぐ。
    pub(crate) fn claim_server_request(
        &mut self,
        request_id: &ServerRequestId,
        result: Option<&ServerRequestResult>,
    ) -> Result<(), ClientError> {
        self.ensure_ready()?;
        let request = match self.pending_server_requests.get_mut(request_id) {
            None => {
                return Err(ClientError::UnknownServerRequestId {
                    request_id: request_id.clone(),
                });
            }
            Some(request) if request.state == ServerRequestState::Responded => {
                return Err(ClientError::ServerRequestAlreadyResponded {
                    request_id: request_id.clone(),
                });
            }
            Some(request) => request,
        };
        if let Some(result) = result {
            match (request.typed_kind, result.typed_kind()) {
                (Some(expected), Some(actual)) if expected == actual => {}
                (None, None) => {}
                _ => {
                    return Err(ClientError::InvalidServerRequestResult {
                        method: request.request.method.clone(),
                    });
                }
            }
        }
        request.state = ServerRequestState::Responded;
        Ok(())
    }

    /// response write failure 後、client がまだ `Ready` の場合だけ claim を再試行可能に戻す。
    pub(crate) fn rollback_server_request_claim(&mut self, request_id: &ServerRequestId) {
        if self.lifecycle == ClientState::Ready
            && let Some(request) = self.pending_server_requests.get_mut(request_id)
        {
            request.state = ServerRequestState::Pending;
        }
    }

    /// app-server 側で解決された ServerRequest を local table から除去する。
    pub(crate) fn resolve_server_request(&mut self, request_id: &RequestId) {
        self.pending_server_requests.remove(request_id);
    }

    /// single active turn slot を確保し、terminal outcome waiter を返す。
    ///
    /// turn/start request より前に登録し、response より早く到着する notification を取りこぼさない。
    pub(crate) fn begin_turn(
        &mut self,
        thread_id: ThreadId,
    ) -> Result<oneshot::Receiver<Result<TurnOutcome, ClientError>>, ClientError> {
        self.ensure_ready()?;
        if self.active_turn.is_some() {
            return Err(ClientError::TurnAlreadyActive);
        }
        let (sender, receiver) = oneshot::channel();
        self.active_turn = Some(ActiveTurn {
            thread_id,
            turn_id: None,
            waiter: Some(sender),
            early_terminal: None,
            latest_agent_message: None,
        });
        Ok(receiver)
    }

    /// active turn に属する完成済み agent message を、terminal outcome 補完用に1件だけ保持する。
    pub(crate) fn record_completed_agent_message(
        &mut self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
        item: &ThreadItem,
    ) -> Result<(), ClientError> {
        if !matches!(item, ThreadItem::AgentMessage { .. }) {
            return Ok(());
        }
        let active = self
            .active_turn
            .as_mut()
            .ok_or_else(|| ClientError::ProtocolViolation {
                reason: "item/completed received without an active turn".to_owned(),
            })?;
        if &active.thread_id != thread_id {
            return Err(ClientError::ProtocolViolation {
                reason: "item/completed thread ID does not match the active turn".to_owned(),
            });
        }
        if let Some(active_turn_id) = active.turn_id.as_ref()
            && active_turn_id != turn_id
        {
            return Err(ClientError::ProtocolViolation {
                reason: "item/completed turn ID does not match the active turn".to_owned(),
            });
        }
        if let Some(early_terminal) = active.early_terminal.as_ref()
            && &early_terminal.turn_id != turn_id
        {
            return Err(ClientError::ProtocolViolation {
                reason: "item/completed turn ID does not match the early turn completion"
                    .to_owned(),
            });
        }
        if let Some((saved_turn_id, _)) = active.latest_agent_message.as_ref()
            && saved_turn_id != turn_id
        {
            return Err(ClientError::ProtocolViolation {
                reason: "completed agent messages have different turn IDs".to_owned(),
            });
        }
        active.latest_agent_message = Some((turn_id.clone(), item.clone()));
        Ok(())
    }

    /// interrupt 対象 ID が current active turn と完全一致することを検証する。
    pub(crate) fn validate_active_turn(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
    ) -> Result<(), ClientError> {
        self.ensure_ready()?;
        let active = self
            .active_turn
            .as_ref()
            .ok_or_else(|| ClientError::NoActiveTurn {
                thread_id: Some(thread_id.clone()),
                turn_id: Some(turn_id.clone()),
            })?;
        if &active.thread_id != thread_id || active.turn_id.as_ref() != Some(turn_id) {
            return Err(ClientError::NoActiveTurn {
                thread_id: Some(thread_id.clone()),
                turn_id: Some(turn_id.clone()),
            });
        }
        Ok(())
    }

    /// turn/start request failure 時に active slot を除去し、waiter ownership を caller へ返す。
    pub(crate) fn clear_active_turn(
        &mut self,
    ) -> Option<oneshot::Sender<Result<TurnOutcome, ClientError>>> {
        self.active_turn
            .take()
            .and_then(|mut active| active.waiter.take())
    }

    /// turn/start response の ID を active turn へ binding し、早着 completion があれば照合して完了する。
    pub(crate) fn bind_turn_id(
        &mut self,
        turn_id: TurnId,
    ) -> Result<Option<CompletedTurn>, ClientError> {
        let completion = {
            let active =
                self.active_turn
                    .as_mut()
                    .ok_or_else(|| ClientError::ProtocolViolation {
                        reason: "turn/start response received without an active turn".to_owned(),
                    })?;
            if let Some((message_turn_id, _)) = active.latest_agent_message.as_ref()
                && message_turn_id != &turn_id
            {
                return Err(ClientError::ProtocolViolation {
                    reason: "completed agent-message turn ID does not match turn/start response"
                        .to_owned(),
                });
            }
            active.turn_id = Some(turn_id.clone());
            if let Some(early) = active.early_terminal.take() {
                if early.turn_id != turn_id {
                    return Err(ClientError::ProtocolViolation {
                        reason: "early turn/completed turn ID does not match turn/start response"
                            .to_owned(),
                    });
                }
                Some(early)
            } else {
                None
            }
        };
        Ok(completion.map(|outcome| self.complete_turn(outcome)))
    }

    /// terminal notification を active turn と照合し、deliver 可能なら completion ownership を返す。
    ///
    /// turn ID がまだ response で確定していない場合は一件だけ early completion として保持する。
    pub(crate) fn receive_turn_completion(
        &mut self,
        outcome: TurnOutcome,
    ) -> Result<Option<CompletedTurn>, ClientError> {
        {
            let active =
                self.active_turn
                    .as_mut()
                    .ok_or_else(|| ClientError::ProtocolViolation {
                        reason: "turn/completed received without an active turn".to_owned(),
                    })?;
            if active.thread_id != outcome.thread_id {
                return Err(ClientError::ProtocolViolation {
                    reason: "turn/completed thread ID does not match the active turn".to_owned(),
                });
            }
            if let Some((message_turn_id, _)) = active.latest_agent_message.as_ref()
                && message_turn_id != &outcome.turn_id
            {
                return Err(ClientError::ProtocolViolation {
                    reason: "completed agent-message turn ID does not match turn/completed"
                        .to_owned(),
                });
            }
            match &active.turn_id {
                Some(turn_id) if turn_id != &outcome.turn_id => {
                    return Err(ClientError::ProtocolViolation {
                        reason: "turn/completed turn ID does not match the active turn".to_owned(),
                    });
                }
                Some(_) => {}
                None if active.early_terminal.is_some() => {
                    return Err(ClientError::ProtocolViolation {
                        reason: "multiple early turn/completed notifications".to_owned(),
                    });
                }
                None => {
                    active.early_terminal = Some(outcome);
                    return Ok(None);
                }
            }
        }
        Ok(Some(self.complete_turn(outcome)))
    }

    /// active turn を除去し、同じ thread/turn に紐づく stale ServerRequest も同時に cleanup する。
    fn complete_turn(&mut self, mut outcome: TurnOutcome) -> CompletedTurn {
        let mut active = self
            .active_turn
            .take()
            .expect("validated active turn must exist");
        let waiter = active.waiter.take();
        if outcome.final_agent_message().is_none()
            && let Some((message_turn_id, message)) = active.latest_agent_message.take()
        {
            debug_assert_eq!(message_turn_id, outcome.turn_id);
            outcome.turn.items.push(message);
        }
        self.pending_server_requests.retain(|_, request| {
            request.thread_id.as_ref() != Some(&outcome.thread_id)
                || request.turn_id.as_ref() != Some(&outcome.turn_id)
        });
        CompletedTurn { outcome, waiter }
    }

    ///最初の fatal error だけを latch し、全 waiter ownership を state から切り離す。
    pub(crate) fn record_failure(&mut self, error: ClientError) -> Option<FailureWaiters> {
        if self.primary_failure.is_some() {
            return None;
        }
        self.primary_failure = Some(error);
        self.lifecycle = ClientState::Failed;
        let pending_requests = self
            .pending_requests
            .drain()
            .map(|(_, pending)| pending.sender)
            .collect();
        let active_turn = self
            .active_turn
            .as_mut()
            .and_then(|active| active.waiter.take());
        self.pending_server_requests.clear();
        Some(FailureWaiters {
            pending_requests,
            active_turn,
        })
    }

    ///完了済み shutdown result の clone を返す。
    pub(crate) fn shutdown_report(&self) -> Option<Result<ShutdownReport, ClientError>> {
        self.shutdown_report.clone()
    }

    /// lifecycle を stopping へ移行し、lock 外 cleanup に必要な waiter と request ID を drain する。
    ///
    /// pending client request ID は cancelled set へ移し、shutdown 中に遅着した response を protocol violation にしない。
    pub(crate) fn prepare_shutdown(&mut self) -> Option<ShutdownPreparation> {
        if self.lifecycle == ClientState::Stopped {
            return None;
        }
        if !matches!(
            self.lifecycle,
            ClientState::Failed | ClientState::ShuttingDown
        ) {
            self.lifecycle = ClientState::ShuttingDown;
        }
        let active_turn = self.active_turn.as_ref().and_then(|active| {
            active
                .turn_id
                .as_ref()
                .map(|turn_id| (active.thread_id.clone(), turn_id.clone()))
        });
        let active_waiter = self
            .active_turn
            .as_mut()
            .and_then(|active| active.waiter.take());
        let drained_requests = self.pending_requests.drain().collect::<Vec<_>>();
        let pending_requests = drained_requests
            .into_iter()
            .map(|(id, pending)| {
                self.cancelled_response_ids.insert(id);
                pending.sender
            })
            .collect();
        let pending_server_requests = self
            .pending_server_requests
            .iter_mut()
            .filter_map(|(id, request)| {
                if request.state == ServerRequestState::Pending {
                    request.state = ServerRequestState::Responded;
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        Some(ShutdownPreparation {
            active_turn,
            active_waiter,
            pending_requests,
            pending_server_requests,
            was_failed: self.primary_failure.is_some(),
        })
    }

    /// shutdown cleanup 後に transient table を消去し、idempotent result を保存する。
    ///
    /// primary failure がある場合は lifecycle を `Failed` のまま維持し、shutdown success で root cause を隠さない。
    pub(crate) fn complete_shutdown(&mut self, result: Result<ShutdownReport, ClientError>) {
        self.pending_requests.clear();
        self.cancelled_response_ids.clear();
        self.active_turn = None;
        self.pending_server_requests.clear();
        if self.primary_failure.is_none() {
            self.lifecycle = ClientState::Stopped;
        }
        self.shutdown_report = Some(result);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        event::ServerRequestKind,
        protocol::{CommandExecutionApprovalDecision, PermissionApprovalDecision},
    };

    fn ready_state() -> RuntimeState {
        let mut state = RuntimeState::new();
        state.begin_initialization();
        state.finish_initialization().unwrap();
        state
    }

    #[test]
    fn request_ids_start_at_one_and_increase() {
        let mut state = RuntimeState::new();
        assert_eq!(state.allocate_request_id().unwrap(), 1);
        assert_eq!(state.allocate_request_id().unwrap(), 2);
    }

    #[test]
    fn request_id_exhaustion_is_reported() {
        let mut state = RuntimeState::new();
        state.next_request_id = i64::MAX;
        assert_eq!(state.allocate_request_id().unwrap(), i64::MAX);
        assert!(matches!(
            state.allocate_request_id(),
            Err(ClientError::RequestIdExhausted)
        ));
    }

    #[test]
    fn pending_request_limit_is_enforced_by_registration() {
        let mut state = ready_state();
        for _ in 0..MAX_PENDING_REQUESTS {
            state
                .register_request("test", RequestPermission::Ready)
                .unwrap();
        }
        assert!(matches!(
            state.register_request("overflow", RequestPermission::Ready),
            Err(ClientError::CapacityExceeded { .. })
        ));
    }

    #[test]
    fn typed_server_request_result_must_match_registered_kind() {
        let mut state = ready_state();
        let request_id = RequestId::String("typed".to_owned());
        state
            .register_server_request(
                ServerRequest {
                    id: request_id.clone(),
                    method: "item/commandExecution/requestApproval".to_owned(),
                    kind: ServerRequestKind::CommandExecutionApproval,
                    params: Value::Null,
                    raw: Value::Null,
                },
                Some(TypedServerRequestKind::CommandExecutionApproval),
                None,
                None,
            )
            .unwrap();
        assert!(matches!(
            state.claim_server_request(&request_id, Some(&ServerRequestResult::new(json!({})))),
            Err(ClientError::InvalidServerRequestResult { .. })
        ));
        state
            .claim_server_request(
                &request_id,
                Some(&ServerRequestResult::command_execution_approval(
                    CommandExecutionApprovalDecision::Accept,
                )),
            )
            .unwrap();
    }

    #[test]
    fn raw_server_request_claim_updates_pending_state_and_is_exclusive() {
        let mut state = ready_state();
        let request_id = RequestId::String("raw".to_owned());
        state
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

        let raw_result = ServerRequestResult::new(json!({"accepted": true}));
        state
            .claim_server_request(&request_id, Some(&raw_result))
            .unwrap();

        assert!(state.pending_server_requests().is_empty());
        assert!(matches!(
            state.claim_server_request(&request_id, Some(&raw_result)),
            Err(ClientError::ServerRequestAlreadyResponded { .. })
        ));
    }

    #[test]
    fn resolved_server_request_cannot_be_claimed() {
        let mut state = ready_state();
        let request_id = RequestId::String("resolved".to_owned());
        state
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
        state.resolve_server_request(&request_id);

        assert!(matches!(
            state.claim_server_request(&request_id, Some(&ServerRequestResult::new(Value::Null))),
            Err(ClientError::UnknownServerRequestId { .. })
        ));
    }

    #[test]
    fn permission_result_must_match_permission_request_kind() {
        let mut state = ready_state();
        let request_id = RequestId::String("permission".to_owned());
        state
            .register_server_request(
                ServerRequest {
                    id: request_id.clone(),
                    method: "item/permissions/requestApproval".to_owned(),
                    kind: ServerRequestKind::PermissionApproval,
                    params: Value::Null,
                    raw: Value::Null,
                },
                Some(TypedServerRequestKind::PermissionApproval),
                None,
                None,
            )
            .unwrap();
        assert!(matches!(
            state.claim_server_request(
                &request_id,
                Some(&ServerRequestResult::command_execution_approval(
                    CommandExecutionApprovalDecision::Accept
                ))
            ),
            Err(ClientError::InvalidServerRequestResult { .. })
        ));
        state
            .claim_server_request(
                &request_id,
                Some(&ServerRequestResult::permission_approval(
                    PermissionApprovalDecision::Decline,
                )),
            )
            .unwrap();
    }

    #[test]
    fn first_failure_is_latched_and_drains_waiters_once() {
        let mut state = ready_state();
        state
            .register_request("test", RequestPermission::Ready)
            .unwrap();
        let first = ClientError::ClientFailed {
            cause: "first".to_owned(),
        };
        let second = ClientError::ClientFailed {
            cause: "second".to_owned(),
        };
        let waiters = state.record_failure(first).unwrap();
        assert_eq!(waiters.pending_requests.len(), 1);
        assert!(state.record_failure(second).is_none());
        assert!(matches!(
            state.primary_failure(),
            Some(ClientError::ClientFailed { cause }) if cause == "first"
        ));
    }

    #[test]
    fn pending_server_request_limit_is_security_bounded() {
        assert_eq!(MAX_PENDING_SERVER_REQUESTS, 4);
    }
}
