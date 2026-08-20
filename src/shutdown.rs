//! client shutdown の ordering、deadline 配分、idempotency を定義する。
//!
//! shutdown は active waiter の取消し、best-effort protocol cleanup、stdin close、child process 回収、task join を
//!一つの absolute deadline 内で実行する。複数 handle から同時に呼ばれても `shutdown_lock` で直列化し、完了後は
//!保存済み result を返す。

use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio::{
    sync::oneshot,
    time::{Instant, timeout_at},
};

use crate::{
    ClientError, process::ProcessCommand, runtime::Shared, state::ShutdownReport,
    transport::Outgoing,
};

/// runtime を stopping state へ移行し、child process と background task を bounded time 内で回収する。
pub(crate) async fn shutdown(inner: &Arc<Shared>) -> Result<ShutdownReport, ClientError> {
    // state snapshot と channel 操作を跨ぐ sequence 全体を直列化し、二重 shutdown command を防ぐ。
    let _guard = inner.shutdown_lock().lock().await;
    if let Some(result) = inner.lock_state().shutdown_report() {
        return result;
    }

    let timeout = inner.shutdown_timeout();
    let deadline = Instant::now() + timeout;
    // process supervisor に全時間を使わせず、writer stop と task join のために短い余白を残す。
    let process_deadline = shutdown_process_deadline(deadline, timeout);
    let preparation = inner.lock_state().prepare_shutdown();
    let Some(preparation) = preparation else {
        return Ok(ShutdownReport::default());
    };

    // caller waiter を先に解放し、以後の protocol cleanup が遅延しても application task を拘束しない。
    if let Some(waiter) = preparation.active_waiter {
        let _ = waiter.send(Err(ClientError::ClientShuttingDown));
    }
    for pending in preparation.pending_requests {
        let _ = pending.send(Err(ClientError::ClientShuttingDown));
    }

    let (reply_sender, reply_receiver) = oneshot::channel();
    let supervisor_available = matches!(
        timeout_at(
            deadline,
            inner.process_sender().send(ProcessCommand::Shutdown {
                deadline: process_deadline,
                reply: reply_sender,
            }),
        )
        .await,
        Ok(Ok(()))
    );

    // active turn と pending ServerRequest は best-effort で protocol 上も解決する。queue saturation や
    // writer failure があっても process cleanup を中断しない。
    if let Some((thread_id, turn_id)) = preparation.active_turn {
        let _ = inner.try_send_detached_request(
            "turn/interrupt",
            json!({"threadId": thread_id, "turnId": turn_id}),
        );
    }
    for request_id in preparation.pending_server_requests {
        let _ = inner.try_send_value(json!({
            "id": request_id,
            "error": {"code": -32000, "message": "client is shutting down"}
        }));
    }
    let (close_sender, _) = oneshot::channel();
    let _ = inner
        .outgoing_sender()
        .try_send(Outgoing::Close { ack: close_sender });

    let mut result = if !supervisor_available {
        // supervisor channel が使えない場合も kill command を試み、child ownership の回収を促す。
        let _ = inner.process_sender().try_send(ProcessCommand::Kill);
        if preparation.was_failed {
            // primary failure は既に caller へ通知済みであり、shutdown cleanup 自体は idempotent success とする。
            Ok(ShutdownReport::default())
        } else if Instant::now() >= deadline {
            Err(ClientError::ShutdownTimeout)
        } else {
            Err(ClientError::ProcessExited {
                status: "process supervisor is unavailable".to_owned(),
            })
        }
    } else {
        match timeout_at(deadline, reply_receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ClientError::TaskJoinFailed {
                task: "process supervisor",
                detail: "shutdown reply channel closed".to_owned(),
            }),
            Err(_) => Err(ClientError::ShutdownTimeout),
        }
    };

    let _ = inner.writer_stop().send(true);
    if !inner.join_tasks_until(deadline).await {
        // process shutdown が成功していても task を期限内に回収できなければ total shutdown timeout とする。
        result = Err(ClientError::ShutdownTimeout);
    }
    inner.lock_state().complete_shutdown(result.clone());
    result
}

/// initialize 途中で失敗した process を public shutdown state を経由せず回収する。
///
///まだ caller へ `Client` を返していないため protocol response の整合性より resource cleanup を優先する。
pub(crate) async fn cleanup_failed_spawn(inner: &Arc<Shared>) {
    let timeout = inner.shutdown_timeout();
    let deadline = Instant::now() + timeout;
    let process_deadline = shutdown_process_deadline(deadline, timeout);
    let (reply, receiver) = oneshot::channel();
    if matches!(
        timeout_at(
            deadline,
            inner.process_sender().send(ProcessCommand::Shutdown {
                deadline: process_deadline,
                reply,
            }),
        )
        .await,
        Ok(Ok(()))
    ) {
        let (close_sender, _) = oneshot::channel();
        let _ = inner
            .outgoing_sender()
            .try_send(Outgoing::Close { ack: close_sender });
        let _ = timeout_at(deadline, receiver).await;
    } else {
        let _ = inner.process_sender().try_send(ProcessCommand::Kill);
    }
    let _ = inner.writer_stop().send(true);
    let _ = inner.join_tasks_until(deadline).await;
}

/// total deadline から process supervisor 用 deadline を算出し、task cleanup の時間を予約する。
fn shutdown_process_deadline(deadline: Instant, timeout: Duration) -> Instant {
    let reserve = std::cmp::min(timeout / 4, Duration::from_millis(50));
    deadline.checked_sub(reserve).unwrap_or(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_deadline_reserves_time_for_task_shutdown() {
        let timeout = Duration::from_secs(1);
        let deadline = Instant::now() + timeout;
        assert!(shutdown_process_deadline(deadline, timeout) < deadline);
    }
}
