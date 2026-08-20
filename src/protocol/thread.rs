//! app-server thread の開始 parameter、identity、approval policy、runtime status を定義する。
//!
//! public request 型は caller が指定する optional override だけを保持する。thread/start response の完全な wire model は
//! internal 型として deserialize し、public API では thread ID と初期実効設定だけを返す。

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::turn::{ReasoningEffort, SandboxPolicy, Turn};

/// app-server が thread ごとに発行する stable identifier。
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ThreadId(String);

impl ThreadId {
    /// wire value または保存済み value から thread ID を生成する。
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// allocation せず underlying string を参照する。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ThreadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// thread/start で要求する sandbox の coarse-grained mode。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    /// workspace を含めて write を許可しない。
    ReadOnly,
    ///指定された workspace root への write を許可する。
    WorkspaceWrite,
    /// sandbox による filesystem 制限を行わない。
    DangerFullAccess,
}

/// approval request を最終的に判断する主体。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ApprovalsReviewer {
    /// user または caller 側 UI が判断する。
    #[serde(rename = "user")]
    User,
    /// app-server の automatic review が判断する。
    #[serde(rename = "auto_review")]
    AutoReview,
    /// guardian subagent が判断する。
    #[serde(rename = "guardian_subagent")]
    GuardianSubagent,
}

///単純 policy または category ごとの granular policy として表現する approval 設定。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AskForApproval {
    Simple(SimpleApprovalPolicy),
    Granular { granular: GranularApprovalPolicy },
}

///全 approval category に共通で適用する単純 policy。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SimpleApprovalPolicy {
    /// untrusted と判断された操作だけ user approval を要求する。
    #[serde(rename = "untrusted")]
    Untrusted,
    /// app-server が必要と判断した時点で approval を要求する。
    #[serde(rename = "on-request")]
    OnRequest,
    /// approval request を生成しない。
    #[serde(rename = "never")]
    Never,
}

/// approval category ごとに request の有効・無効を指定する policy。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GranularApprovalPolicy {
    /// MCP elicitation に user approval を要求する。
    pub mcp_elicitations: bool,
    /// rule 変更に user approval を要求する。
    pub rules: bool,
    /// sandbox escalation に user approval を要求する。
    pub sandbox_approval: bool,
    /// permission request の override。未指定なら server default を使用する。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_permissions: Option<bool>,
    /// skill 使用 approval の override。未指定なら server default を使用する。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_approval: Option<bool>,
}

/// app-server が応答生成へ適用する personality preset。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Personality {
    None,
    Friendly,
    Pragmatic,
}

/// thread/start request で default 設定を上書きする optional parameter。
///
/// `None` の field は payload から省略し、app-server 側の default または configuration を維持する。
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartParams {
    /// thread の sandbox mode override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    /// approval policy override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<AskForApproval>,
    /// approval reviewer override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<ApprovalsReviewer>,
    /// model へ渡す base instruction override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    /// app-server configuration の raw override map。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<Map<String, Value>>,
    /// thread の working directory override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// developer instruction override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    /// session が開始された user action の分類。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_start_source: Option<ThreadStartSource>,
    /// response personality override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub personality: Option<Personality>,
    /// persistent history の対象外とする thread かどうか。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
    /// request を発行した higher-level service 名。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// caller 固有の thread source label。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_source: Option<String>,
    /// model service tier override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    /// model 名 override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// model provider override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
}

/// thread/start を開始した client-side action。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ThreadStartSource {
    /// application startup に伴う新規 thread。
    Startup,
    ///既存 conversation の clear に伴う新規 thread。
    Clear,
}

/// `thread/start`でapp-serverが返したthread IDと初期実効設定。
#[derive(Clone, Debug)]
pub struct ThreadStartOutcome {
    pub thread_id: ThreadId,
    pub model: String,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub approval_policy: AskForApproval,
    pub approvals_reviewer: ApprovalsReviewer,
    pub sandbox: SandboxPolicy,
    pub cwd: String,
    pub model_provider: String,
}

/// thread/start response の完全な wire model。
///
/// public APIは要求された初期実効設定だけを返すが、response全体をdeserializeしてSchema driftを検出する。
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ThreadStartResponse {
    pub sandbox: SandboxPolicy,
    pub approval_policy: AskForApproval,
    pub approvals_reviewer: ApprovalsReviewer,
    pub cwd: String,
    pub model: String,
    pub model_provider: String,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub thread: WireThread,
}

/// thread/start response に埋め込まれた thread snapshot。
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireThread {
    pub cli_version: String,
    pub created_at: i64,
    pub cwd: String,
    pub ephemeral: bool,
    pub id: ThreadId,
    pub model_provider: String,
    pub preview: String,
    pub session_id: String,
    pub source: SessionSource,
    pub status: ThreadStatus,
    pub turns: Vec<Turn>,
    pub updated_at: i64,
}

/// app-server が返す session source の wire representation。
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum SessionSource {
    Simple(SessionSourceKind),
    Custom {
        custom: String,
    },
    SubAgent {
        #[serde(rename = "subAgent")]
        sub_agent: Value,
    },
}

///既知の simple session source。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SessionSourceKind {
    Cli,
    Vscode,
    Exec,
    AppServer,
    Unknown,
}

/// app-server thread の current activity state。
///
/// variant は protocol 拡張で追加され得るため、exhaustive match を前提にしてはならない。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
#[non_exhaustive]
pub enum ThreadStatus {
    /// thread data が runtime へ load されていない。
    NotLoaded,
    /// active turn がなく、request を受理できる。
    Idle,
    /// thread 固有の system error が発生している。
    SystemError,
    /// turn または approval flow が進行中である。
    Active {
        #[serde(rename = "activeFlags")]
        active_flags: Vec<ThreadActiveFlag>,
    },
}

/// active thread が外部 action を待っている理由。
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum ThreadActiveFlag {
    /// approval response を待機している。
    WaitingOnApproval,
    /// user-input response を待機している。
    WaitingOnUserInput,
}
