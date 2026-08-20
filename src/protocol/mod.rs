//! codex app-server の JSON-RPC wire model と public protocol type を定義する。
//!
//! envelope classification と checked-in Schema validation は internal implementation とし、caller には
//! request parameter、typed notification、ServerRequest response、thread/turn model だけを公開する。
//! wire protocol の field 名は `serde` attribute で固定し、Rust 側の命名変更から独立させる。

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
pub(crate) use initialize::{ClientInfo, InitializeParams};
pub use notification::{
    AgentMessageDeltaNotification, ItemCompletedNotification, ItemStartedNotification,
    ServerRequestResolvedNotification, ThreadStatusChangedNotification, TurnCompletedNotification,
    TurnStartedNotification,
};
pub(crate) use schema::{
    is_known_server_notification_method, is_known_server_request_method,
    validate_server_notification, validate_server_request,
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
    ApprovalsReviewer, AskForApproval, GranularApprovalPolicy, Personality, SandboxMode,
    SimpleApprovalPolicy, ThreadActiveFlag, ThreadId, ThreadStartOutcome, ThreadStartParams,
    ThreadStartSource, ThreadStatus,
};
pub use turn::{
    ByteRange, ImageDetail, NetworkAccess, ReasoningEffort, ReasoningSummary, SandboxPolicy,
    TextElement, ThreadItem, Turn, TurnError, TurnId, TurnOutcome, TurnStartParams, TurnStatus,
    UserInput,
};
pub(crate) use turn::{TurnCompletedParams, TurnStartResponse};
