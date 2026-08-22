//! 単一の `codex app-server` child process を所有し、非同期 JSON-RPC API として公開する client crate。
//!
//! process の起動、initialize handshake、JSONL transport、request/response correlation、
//! turn lifecycle、ServerRequest 応答、bounded event 配信、graceful shutdown を一つの所有権境界へ集約する。
//! 複数の app-server process や複数 turn の同時実行は、この crate の責務には含めない。

mod client;
mod config;
mod dispatcher;
mod error;
mod event;
mod process;
pub mod protocol;
mod runtime;
mod shutdown;
mod state;
mod transport;

pub use client::{ClaimedServerRequest, Client};
pub use config::{ClientConfig, EnvironmentPolicy};
pub use error::ClientError;
pub use event::{Event, EventReceiver, RawNotification, ServerRequest, ServerRequestKind};
pub use protocol::{
    AgentMessageDeltaNotification, ApprovalsReviewer, AskForApproval, ByteRange,
    ClientCapabilities, CommandExecutionApprovalDecision, FileChangeApprovalDecision,
    GranularApprovalPolicy, ImageDetail, InitializeResponse, ItemCompletedNotification,
    ItemStartedNotification, NetworkAccess, NetworkPolicyAmendment, NetworkPolicyRuleAction,
    PermissionApprovalDecision, PermissionApprovalParams, PermissionProfile, Personality,
    ReasoningEffort, ReasoningSummary, RpcError, SandboxMode, SandboxPolicy, ServerRequestId,
    ServerRequestResolvedNotification, ServerRequestResult, SimpleApprovalPolicy, TextElement,
    ThreadActiveFlag, ThreadId, ThreadItem, ThreadStartOutcome, ThreadStartParams,
    ThreadStartSource, ThreadStatus, ThreadStatusChangedNotification, ToolRequestUserInputAnswer,
    ToolRequestUserInputResponse, Turn, TurnCompletedNotification, TurnError, TurnId, TurnOutcome,
    TurnStartParams, TurnStartedNotification, TurnStatus, UserInput,
};
pub use state::{ClientState, ShutdownReport};
