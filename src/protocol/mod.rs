//! codex app-server の JSON-RPC wire model と public protocol type を定義する。
//!
//! envelope classification と checked-in Schema validation は internal implementation とし、caller には
//! request parameter、typed notification、ServerRequest response、thread/turn model だけを公開する。
//! wire protocol の field 名は `serde` attribute で固定し、Rust 側の命名変更から独立させる。

use serde::{Deserialize, Deserializer};

mod envelope;
mod initialize;
mod notification;
mod schema;
mod server_request;
mod thread;
mod turn;

pub(crate) use envelope::{IncomingMessage, classify_message};
pub use envelope::{RequestId, RpcError};
pub use initialize::{ClientCapabilities, InitializeResponse};
pub(crate) use initialize::{ClientInfo, InitializeParams, InitializeRuntimeResponse};
pub use notification::{
    AgentMessageDeltaNotification, ItemCompletedNotification, ItemStartedNotification,
    ServerRequestResolvedNotification, ThreadStatusChangedNotification, TurnCompletedNotification,
    TurnStartedNotification,
};
pub(crate) use schema::{
    is_known_server_notification_method, is_known_server_request_method,
    validate_permission_profile,
};
pub use server_request::{
    CommandExecutionApprovalDecision, FileChangeApprovalDecision, NetworkPolicyAmendment,
    NetworkPolicyRuleAction, PermissionApprovalDecision, PermissionApprovalParams,
    PermissionProfile, ServerRequestId, ServerRequestResult, ToolRequestUserInputAnswer,
    ToolRequestUserInputResponse,
};
pub(crate) use server_request::{TypedServerRequestKind, typed_server_request_kind};
pub(crate) use thread::ThreadStartResponse;
pub use thread::{
    ApprovalsReviewer, AskForApproval, DynamicToolSpec, GranularApprovalPolicy, Personality,
    SandboxMode, SimpleApprovalPolicy, ThreadActiveFlag, ThreadId, ThreadStartOutcome,
    ThreadStartParams, ThreadStartSource, ThreadStatus,
};
pub use turn::{
    AdditionalContextEntry, ByteRange, ImageDetail, NetworkAccess, ReasoningEffort,
    ReasoningSummary, SandboxPolicy, TextElement, ThreadItem, Turn, TurnError, TurnId, TurnOutcome,
    TurnStartParams, TurnStatus, UserInput,
};
pub(crate) use turn::{TurnCompletedParams, TurnStartResponse};

/// Serde の struct が配列からも変換されるため、名前付き field を持つ wire object に限定する。
pub(crate) struct WireObject<T>(pub T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for WireObject<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_object(deserializer).map(Self)
    }
}

fn deserialize_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let fields = serde_json::Map::deserialize(deserializer)?;
    T::deserialize(serde_json::Value::Object(fields)).map_err(serde::de::Error::custom)
}
