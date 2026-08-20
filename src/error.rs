//! configuration、process、transport、protocol、lifecycle failure を caller-facing error へ正規化する。
//!
//! server-controlled detail は field として保持して programmatic handling を可能にする一方、`Display` と `Debug` からは
//!原則として除外する。log injection や prompt/tool output の意図しない公開を防ぐため、diagnostic は error category と
//!local metadata だけを表示する。

use std::fmt;

use crate::protocol::{RequestId, ThreadId, TurnId};

/// client/process/protocol boundary で発生する failure。
///
/// variant は将来追加され得るため、exhaustive match を前提にしてはならない。
#[derive(Clone, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// process 起動前の configuration validation に失敗した。
    #[error("invalid configuration for {field}: {reason}")]
    InvalidConfiguration {
        field: &'static str,
        reason: &'static str,
    },
    /// OS に child process の生成を拒否された。
    #[error("failed to spawn app-server: {detail}")]
    SpawnFailed { detail: String },
    /// spawn 済み child から必要な pipe handle を取得できなかった。
    #[error("child process did not provide its {pipe} pipe")]
    MissingPipe { pipe: &'static str },
    /// initialize handshake が configured deadline を超過した。
    #[error("initialize timed out")]
    InitializeTimeout,
    /// initialize request または initialized notification write が失敗した。
    #[error("initialize failed")]
    InitializeFailed { reason: String },
    /// outgoing protocol value を JSON へ変換できなかった。
    #[error("failed to serialize {operation}")]
    SerializationFailed {
        operation: &'static str,
        detail: String,
    },
    /// incoming byte/value を expected protocol type へ変換できなかった。
    #[error("failed to deserialize {operation}")]
    DeserializationFailed {
        operation: &'static str,
        detail: String,
    },
    /// JSON-RPC envelope、Schema、correlation、state transition が protocol contract に違反した。
    #[error("protocol violation")]
    ProtocolViolation { reason: String },
    /// app-server が client request へ JSON-RPC error response を返した。
    #[error("RPC method {method} failed for request {request_id}: code {code}")]
    RpcFailed {
        method: String,
        request_id: i64,
        code: i64,
        message: String,
    },
    /// positive signed 64-bit request ID をこれ以上割り当てられない。
    #[error("client request ID space is exhausted")]
    RequestIdExhausted,
    /// bounded table または queue に新しい entry を登録できない。
    #[error("capacity exceeded for {resource} (limit {limit})")]
    CapacityExceeded {
        resource: &'static str,
        limit: usize,
    },
    /// initialize 完了前に ready-only API が呼ばれた。
    #[error("client is not ready")]
    ClientNotReady,
    /// shutdown 開始後に新しい operation が要求された。
    #[error("client is shutting down")]
    ClientShuttingDown,
    /// shutdown 完了後に operation が要求された。
    #[error("client is stopped")]
    ClientStopped,
    /// runtime が primary failure を記録済みである。
    #[error("client failed")]
    ClientFailed { cause: String },
    /// single-active-turn policy に反して二つ目の turn が開始された。
    #[error("a turn is already active")]
    TurnAlreadyActive,
    /// interrupt または completion の ID が current active turn と一致しない。
    #[error("no matching active turn")]
    NoActiveTurn {
        thread_id: Option<ThreadId>,
        turn_id: Option<TurnId>,
    },
    /// response 対象の ServerRequest が pending table に存在しない。
    #[error("unknown ServerRequest ID {request_id}")]
    UnknownServerRequestId { request_id: RequestId },
    ///同一 ServerRequest が既に claim 済みである。
    #[error("ServerRequest {request_id} was already responded to")]
    ServerRequestAlreadyResponded { request_id: RequestId },
    /// ServerRequest method と typed result variant が一致しない。
    #[error("result type does not match ServerRequest method")]
    InvalidServerRequestResult { method: String },
    /// stdin writer task または child stdin pipe への write が失敗した。
    #[error("stdin write failed: {detail}")]
    StdinWriteFailed { detail: String },
    /// stdout/stderr pipe の read が失敗した。
    #[error("stdout read failed: {detail}")]
    StdoutReadFailed { detail: String },
    /// child が動作中であるにもかかわらず stdout が EOF になった。
    #[error("stdout closed while the child was running")]
    StdoutClosed,
    /// incoming JSONL frame が configured payload limit を超えた。
    #[error("protocol message exceeds {limit} bytes")]
    MessageTooLarge { limit: usize },
    /// shutdown sequence 外で child process が終了した、または wait に失敗した。
    #[error("app-server exited unexpectedly with {status}")]
    ProcessExited { status: String },
    /// graceful shutdown と cleanup が total deadline を超過した。
    #[error("shutdown timed out; child process group was killed and reaped")]
    ShutdownTimeout,
    /// background task の completion channel または join が異常終了した。
    #[error("background task failed to join: {task}")]
    TaskJoinFailed { task: &'static str, detail: String },
}

impl fmt::Debug for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // error field には server output、path、environment detail が含まれ得るため、variant 名だけを公開する。
        formatter.write_str(match self {
            Self::InvalidConfiguration { .. } => "InvalidConfiguration",
            Self::SpawnFailed { .. } => "SpawnFailed",
            Self::MissingPipe { .. } => "MissingPipe",
            Self::InitializeTimeout => "InitializeTimeout",
            Self::InitializeFailed { .. } => "InitializeFailed",
            Self::SerializationFailed { .. } => "SerializationFailed",
            Self::DeserializationFailed { .. } => "DeserializationFailed",
            Self::ProtocolViolation { .. } => "ProtocolViolation",
            Self::RpcFailed { .. } => "RpcFailed",
            Self::RequestIdExhausted => "RequestIdExhausted",
            Self::CapacityExceeded { .. } => "CapacityExceeded",
            Self::ClientNotReady => "ClientNotReady",
            Self::ClientShuttingDown => "ClientShuttingDown",
            Self::ClientStopped => "ClientStopped",
            Self::ClientFailed { .. } => "ClientFailed",
            Self::TurnAlreadyActive => "TurnAlreadyActive",
            Self::NoActiveTurn { .. } => "NoActiveTurn",
            Self::UnknownServerRequestId { .. } => "UnknownServerRequestId",
            Self::ServerRequestAlreadyResponded { .. } => "ServerRequestAlreadyResponded",
            Self::InvalidServerRequestResult { .. } => "InvalidServerRequestResult",
            Self::StdinWriteFailed { .. } => "StdinWriteFailed",
            Self::StdoutReadFailed { .. } => "StdoutReadFailed",
            Self::StdoutClosed => "StdoutClosed",
            Self::MessageTooLarge { .. } => "MessageTooLarge",
            Self::ProcessExited { .. } => "ProcessExited",
            Self::ShutdownTimeout => "ShutdownTimeout",
            Self::TaskJoinFailed { .. } => "TaskJoinFailed",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_debug_do_not_expose_server_controlled_details() {
        let secret = "secret\n\u{1b}[31m";
        for error in [
            ClientError::ProtocolViolation {
                reason: secret.to_owned(),
            },
            ClientError::RpcFailed {
                method: "turn/start".to_owned(),
                request_id: 1,
                code: -1,
                message: secret.to_owned(),
            },
            ClientError::ClientFailed {
                cause: secret.to_owned(),
            },
        ] {
            assert!(!error.to_string().contains(secret));
            assert!(!format!("{error:?}").contains(secret));
        }
    }

    #[test]
    fn process_exit_display_keeps_the_os_status() {
        let error = ClientError::ProcessExited {
            status: "exit status: 9".to_owned(),
        };
        assert!(error.to_string().contains("exit status: 9"));
        assert_eq!(format!("{error:?}"), "ProcessExited");
    }
}
