//! app-server から受信した asynchronous event を caller へ公開する型を定義する。
//!
//! typed event を優先しつつ、Schema 既知だが専用型を持たない notification と、将来追加された未知の
//! notification を raw value のまま失わず配信する。event queue は broadcast channel なので、遅延した
//! subscriber は [`tokio::sync::broadcast::error::RecvError::Lagged`] を処理する必要がある。

use std::fmt;

use serde_json::Value;
use tokio::sync::broadcast;

use crate::{
    ClientError,
    protocol::{
        AgentMessageDeltaNotification, ItemCompletedNotification, ItemStartedNotification,
        ServerRequestId, ServerRequestResolvedNotification, ThreadStatusChangedNotification,
        TurnCompletedNotification, TurnStartedNotification,
    },
};

/// client event stream を受信する bounded broadcast receiver。
pub type EventReceiver = broadcast::Receiver<Event>;

/// app-server process、protocol dispatcher、client runtime が caller へ公開する event。
///
/// variant は将来追加され得るため、exhaustive match を前提にしてはならない。
#[derive(Clone)]
#[non_exhaustive]
pub enum Event {
    /// turn が app-server 上で開始された。
    TurnStarted(TurnStartedNotification),
    /// turn が terminal state へ到達した。
    TurnCompleted(TurnCompletedNotification),
    /// turn 内の item 処理が開始された。
    ItemStarted(ItemStartedNotification),
    /// turn 内の item 処理が完了した。
    ItemCompleted(ItemCompletedNotification),
    /// agent message の incremental delta を受信した。
    AgentMessageDelta(AgentMessageDeltaNotification),
    /// thread の idle、active、error 状態が変化した。
    ThreadStatusChanged(ThreadStatusChangedNotification),
    /// app-server 側で ServerRequest が client 応答以外の経路から解決された。
    ServerRequestResolved(ServerRequestResolvedNotification),
    /// checked-in Schema では既知だが、専用の public 型を持たない notification。
    GenericNotification(RawNotification),
    ///現在の checked-in Schema に存在しない notification。
    ///
    /// forward compatibility のため process failure にはせず、method と raw payload を caller へ渡す。
    UnknownNotification(RawNotification),
    /// app-server が client へ返答を要求する JSON-RPC request。
    ServerRequest(ServerRequest),
    /// child process の stderr から読み取った byte chunk。
    ///
    /// UTF-8 や line 単位を保証せず、内容の sanitization と log policy は caller の責務とする。
    Stderr(Vec<u8>),
    /// child process が shutdown sequence 外で終了した。
    ProcessExited { status: String },
    /// client を fail-closed へ遷移させた primary failure。
    Failure(ClientError),
}

///専用型へ変換しない notification の envelope と payload。
///
/// `params` と `raw` は server-controlled data を含むため、`Debug` では常に redact する。
#[derive(Clone)]
#[non_exhaustive]
pub struct RawNotification {
    /// JSON-RPC method 名。
    pub method: String,
    /// notification の `params` value。存在しない場合は `null`。
    pub params: Value,
    ///将来の field を含む受信 JSON-RPC envelope 全体。
    pub raw: Value,
}

impl fmt::Debug for RawNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawNotification")
            .field("method", &self.method)
            .field("params", &"<redacted>")
            .field("raw", &"<redacted>")
            .finish()
    }
}

/// app-server から受信し、caller の応答を待っている JSON-RPC request。
///
/// request は [`Client::respond_server_request`](crate::Client::respond_server_request) または
/// [`Client::reject_server_request`](crate::Client::reject_server_request) で一度だけ claim できる。
#[derive(Clone)]
pub struct ServerRequest {
    /// response envelope でそのまま返す server-generated request ID。
    pub id: ServerRequestId,
    /// JSON-RPC method 名。
    pub method: String,
    /// caller が dispatch 方針を決めるための coarse-grained classification。
    pub kind: ServerRequestKind,
    /// request の `params` value。存在しない場合は `null`。
    pub params: Value,
    ///将来の field を含む受信 JSON-RPC envelope 全体。
    pub raw: Value,
}

impl fmt::Debug for ServerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerRequest")
            .field("id", &self.id)
            .field("method", &self.method)
            .field("kind", &self.kind)
            .field("params", &"<redacted>")
            .field("raw", &"<redacted>")
            .finish()
    }
}

/// ServerRequest method の public classification。
///
/// typed response を持つ request と、Schema 既知だが raw response が必要な request を区別する。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerRequestKind {
    /// command execution の approval request。
    CommandExecutionApproval,
    /// file change の approval request。
    FileChangeApproval,
    /// standalone sandbox permission の approval request。
    PermissionApproval,
    /// tool が user input を要求している request。
    UserInput,
    /// checked-in Schema では既知だが専用 result 型を持たない request。
    Known,
    /// checked-in Schema に存在しない request。
    Unsupported,
}
