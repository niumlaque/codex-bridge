//! public `Client` handle と background task が共有する runtime coordination を定義する。
//!
//! mutable protocol state は短時間だけ保持する synchronous `Mutex` に集約し、lock を保持したまま await しない。
//! I/O は bounded channel 経由で専用 task へ委譲する。最初の fatal error だけを primary failure として latch し、
//! pending waiter、writer、process supervisor を一度に fail-closed へ移行させる。

use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{
    ClientError,
    event::{Event, EventReceiver, ServerRequest},
    process::{ProcessCommand, SpawnedProcess, supervise},
    protocol::{ThreadId, TurnId, TurnOutcome},
    state::{ClientState, RuntimeState},
    transport::{Outgoing, stderr_reader, stdin_writer, stdout_reader},
};

pub(crate) use crate::state::RequestPermission;

///一つの app-server process に属する state、channel、background task の共有 owner。
pub(crate) struct Shared {
    /// lifecycle、request correlation、active turn、pending ServerRequest を直列化する state。
    state: Mutex<RuntimeState>,
    /// single stdin writer task への bounded queue。
    outgoing_sender: mpsc::Sender<Outgoing>,
    /// caller subscriber へ event を配信する bounded broadcast channel。
    event_sender: broadcast::Sender<Event>,
    /// process supervisor へ lifecycle command を送る channel。
    process_sender: mpsc::Sender<ProcessCommand>,
    ///複数 clone からの shutdown sequence を一度に直列化する async lock。
    shutdown_lock: tokio::sync::Mutex<()>,
    /// shutdown 時に join または abort する background task 群。
    tasks: Mutex<Vec<(&'static str, JoinHandle<()>)>>,
    ///全 shutdown sequence に与えられた timeout。
    shutdown_timeout: Duration,
    /// stdin writer を通常 queue 処理外から停止させる watch signal。
    writer_stop: watch::Sender<bool>,
}

impl Shared {
    /// channel endpoint と timeout を受け取り、`Starting` state の runtime を生成する。
    pub(crate) fn new(
        outgoing_sender: mpsc::Sender<Outgoing>,
        event_sender: broadcast::Sender<Event>,
        process_sender: mpsc::Sender<ProcessCommand>,
        writer_stop: watch::Sender<bool>,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            state: Mutex::new(RuntimeState::new()),
            outgoing_sender,
            event_sender,
            process_sender,
            shutdown_lock: tokio::sync::Mutex::new(()),
            tasks: Mutex::new(Vec::new()),
            shutdown_timeout,
            writer_stop,
        }
    }

    /// poisoned mutex からも state ownership を回収する。
    ///
    /// poison 自体を secondary failure として process cleanup を妨げず、既存 state を使って fail-closed を継続する。
    pub(crate) fn lock_state(&self) -> std::sync::MutexGuard<'_, RuntimeState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// current lifecycle の snapshot を返す。
    pub(crate) fn state(&self) -> ClientState {
        self.lock_state().lifecycle()
    }

    /// event stream の新しい receiver を生成する。
    pub(crate) fn subscribe(&self) -> EventReceiver {
        self.event_sender.subscribe()
    }

    /// receiver が存在しない場合を failure にせず event を best-effort 配信する。
    pub(crate) fn publish_event(&self, event: Event) {
        let _ = self.event_sender.send(event);
    }

    ///未応答 ServerRequest の snapshot を返す。
    pub(crate) fn pending_server_requests(&self) -> Vec<ServerRequest> {
        self.lock_state().pending_server_requests()
    }

    /// request 送信前に active turn slot と terminal outcome waiter を登録する。
    pub(crate) fn begin_turn(
        &self,
        thread_id: ThreadId,
    ) -> Result<oneshot::Receiver<Result<TurnOutcome, ClientError>>, ClientError> {
        self.lock_state().begin_turn(thread_id)
    }

    /// interrupt 対象が current active turn と一致することを検証する。
    pub(crate) fn validate_active_turn(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
    ) -> Result<(), ClientError> {
        self.lock_state().validate_active_turn(thread_id, turn_id)
    }

    /// shutdown で ownership を回収できるよう background task を登録する。
    pub(crate) fn push_task(&self, name: &'static str, task: JoinHandle<()>) {
        self.tasks
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((name, task));
    }

    ///共通 absolute deadline まで task を順番に join し、超過時は残りをすべて abort する。
    ///
    /// task ごとに timeout を再計算しないため、task 数に比例して total shutdown 時間が伸びることはない。
    pub(crate) async fn join_tasks_until(&self, deadline: Instant) -> bool {
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        );
        let mut tasks = tasks.into_iter();
        while let Some((_, mut task)) = tasks.next() {
            if timeout_at(deadline, &mut task).await.is_err() {
                task.abort();
                for (_, task) in tasks {
                    task.abort();
                }
                return false;
            }
        }
        true
    }

    /// new work を開始してはならない lifecycle か判定する。
    pub(crate) fn is_stopping(&self) -> bool {
        self.lock_state().is_stopping()
    }

    ///最初に記録された fatal error の clone を返す。
    pub(crate) fn primary_failure(&self) -> Option<ClientError> {
        self.lock_state().primary_failure()
    }

    /// first-cause failure を latch し、全 waiter と background I/O を fail-closed へ移行させる。
    ///
    ///既に primary failure がある場合は何も行わず、後続 error が root cause と waiter result を上書きしないようにする。
    pub(crate) fn fail(&self, error: ClientError) {
        let waiters = self.lock_state().record_failure(error.clone());
        let Some(waiters) = waiters else {
            return;
        };
        for pending in waiters.pending_requests {
            let _ = pending.send(Err(error.clone()));
        }
        if let Some(waiter) = waiters.active_turn {
            let _ = waiter.send(Err(error.clone()));
        }
        self.publish_event(Event::Failure(error));
        let _ = self.writer_stop.send(true);
        let _ = self.process_sender.try_send(ProcessCommand::Kill);
    }

    /// typed client request を登録、送信し、対応する response を deserialize する。
    ///
    /// request ID と waiter は queue send より先に state へ登録する。send failure 時は registration を rollback する。
    /// response payload が expected type に一致しない場合は protocol desynchronization として client 全体を fail させる。
    pub(crate) async fn send_request<P, R>(
        &self,
        method: &'static str,
        params: &P,
        permission: RequestPermission,
    ) -> Result<R, ClientError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let params =
            serde_json::to_value(params).map_err(|error| ClientError::SerializationFailed {
                operation: method,
                detail: error.to_string(),
            })?;
        let (id, receiver) = self.lock_state().register_request(method, permission)?;
        let value = json!({"id": id, "method": method, "params": params});
        if self
            .outgoing_sender
            .send(Outgoing::Message { value, ack: None })
            .await
            .is_err()
        {
            self.lock_state().remove_pending_request(id);
            return Err(ClientError::StdinWriteFailed {
                detail: "stdin writer task is unavailable".to_owned(),
            });
        }
        let result = receiver.await.map_err(|_| {
            self.primary_failure().unwrap_or(ClientError::ClientFailed {
                cause: "request response channel closed".to_owned(),
            })
        })??;
        serde_json::from_value(result).map_err(|error| {
            let error = ClientError::ProtocolViolation {
                reason: format!("invalid {method} response payload: {error}"),
            };
            self.fail(error.clone());
            error
        })
    }

    /// response を待たない client request を bounded queue へ non-blocking 送信する。
    ///
    /// server response が到着した場合に unknown ID と誤判定しないよう、ID は cancelled response set に保持する。
    pub(crate) fn try_send_detached_request(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<(), ClientError> {
        let id = self.lock_state().register_detached_request()?;
        let value = json!({"id": id, "method": method, "params": params});
        if self
            .outgoing_sender
            .try_send(Outgoing::Message { value, ack: None })
            .is_err()
        {
            self.lock_state().remove_detached_request(id);
            return Err(ClientError::StdinWriteFailed {
                detail: "stdin writer queue is unavailable or full".to_owned(),
            });
        }
        Ok(())
    }

    /// raw JSON-RPC value を bounded queue へ non-blocking 送信する。
    pub(crate) fn try_send_value(&self, value: Value) -> Result<(), ClientError> {
        self.outgoing_sender
            .try_send(Outgoing::Message { value, ack: None })
            .map_err(|_| ClientError::StdinWriteFailed {
                detail: "stdin writer queue is unavailable or full".to_owned(),
            })
    }

    /// raw JSON-RPC value を queue へ送信し、必要なら actual stdin flush 完了まで待機する。
    ///
    /// `acknowledge = true` は queue への enqueue ではなく、writer task による write と flush の result を返す。
    pub(crate) async fn send_value(
        &self,
        value: Value,
        acknowledge: bool,
    ) -> Result<(), ClientError> {
        let (ack, receiver) = if acknowledge {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        self.outgoing_sender
            .send(Outgoing::Message { value, ack })
            .await
            .map_err(|_| ClientError::StdinWriteFailed {
                detail: "stdin writer task is unavailable".to_owned(),
            })?;
        if let Some(receiver) = receiver {
            receiver.await.map_err(|_| ClientError::StdinWriteFailed {
                detail: "stdin writer acknowledgement was dropped".to_owned(),
            })??;
        }
        Ok(())
    }

    /// child pipe と process handle を専用 background task へ分配する。
    ///
    /// stdin は single writer、stdout は protocol reader、stderr は raw drain、child handle は supervisor がそれぞれ
    /// exclusive ownership を持つ。各 fatal task error は stopping 中でない場合だけ [`Shared::fail`] へ昇格する。
    pub(crate) fn spawn_tasks(
        self: &Arc<Self>,
        process: SpawnedProcess,
        outgoing_receiver: mpsc::Receiver<Outgoing>,
        process_receiver: mpsc::Receiver<ProcessCommand>,
        mut writer_stop: watch::Receiver<bool>,
        max_message_bytes: usize,
    ) {
        let writer_inner = Arc::downgrade(self);
        self.push_task(
            "stdin writer",
            tokio::spawn(async move {
                tokio::select! {
                    result = stdin_writer(process.stdin, outgoing_receiver) => {
                        if let Err(error) = result
                            && let Some(inner) = Weak::upgrade(&writer_inner)
                            && !inner.is_stopping()
                        {
                            inner.fail(error);
                        }
                    }
                    changed = writer_stop.changed() => {
                        let _ = changed;
                    }
                }
            }),
        );

        let stdout_inner = self.clone();
        self.push_task(
            "stdout reader",
            tokio::spawn(async move {
                let result = stdout_reader(process.stdout, max_message_bytes, |value| {
                    let inner = stdout_inner.clone();
                    async move { inner.dispatch(value) }
                })
                .await;
                match result {
                    Err(error) => stdout_inner.fail(error),
                    Ok(()) if !stdout_inner.is_stopping() => {
                        // EOF と process exit の競合は child handle を所有する supervisor で確定する。
                        let _ = stdout_inner
                            .process_sender
                            .send(ProcessCommand::StdoutClosed)
                            .await;
                    }
                    Ok(()) => {}
                }
            }),
        );

        let stderr_events = self.event_sender.clone();
        self.push_task(
            "stderr reader",
            tokio::spawn(async move {
                // stderr は diagnostic stream であり、drain failure を primary protocol failure には昇格しない。
                let _ = stderr_reader(process.stderr, |bytes| {
                    let _ = stderr_events.send(Event::Stderr(bytes));
                })
                .await;
            }),
        );

        let weak = Arc::downgrade(self);
        self.push_task(
            "process supervisor",
            tokio::spawn(async move {
                supervise(process.child, process_receiver, move |error| {
                    let Some(inner) = Weak::upgrade(&weak) else {
                        return false;
                    };
                    if inner.is_stopping() {
                        return false;
                    }
                    inner.publish_event(Event::ProcessExited {
                        status: error.to_string(),
                    });
                    inner.fail(error);
                    true
                })
                .await;
            }),
        );
    }

    pub(crate) fn process_sender(&self) -> &mpsc::Sender<ProcessCommand> {
        &self.process_sender
    }

    pub(crate) fn outgoing_sender(&self) -> &mpsc::Sender<Outgoing> {
        &self.outgoing_sender
    }

    pub(crate) fn writer_stop(&self) -> &watch::Sender<bool> {
        &self.writer_stop
    }

    pub(crate) fn shutdown_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.shutdown_lock
    }

    pub(crate) fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}
