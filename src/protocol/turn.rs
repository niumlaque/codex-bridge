//! turn/start parameter、turn snapshot、item model、terminal outcome を定義する。
//!
//! app-server Schema は多数の item kind を持つが、client が直接利用する agent message だけを専用 variant とし、
//!その他は validated raw JSON として保持する。これにより Schema validation を維持しながら future item kind を失わない。

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use super::thread::{ApprovalsReviewer, AskForApproval, Personality, ThreadId};

/// app-server が turn ごとに発行する stable identifier。
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TurnId(String);

impl TurnId {
    /// wire value または保存済み value から turn ID を生成する。
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// allocation せず underlying string を参照する。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TurnId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// turn の execution state。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnStatus {
    ///正常に完了した terminal state。
    Completed,
    /// interrupt により終了した terminal state。
    Interrupted,
    /// error により終了した terminal state。
    Failed,
    ///処理中であり terminal outcome としては扱えない state。
    InProgress,
}

impl TurnStatus {
    /// `turn/completed` notification で許可する terminal state か判定する。
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Interrupted | Self::Failed)
    }
}

/// turn/start へ渡す user input element。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum UserInput {
    /// plain text と、その text 内の structured element。
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        text_elements: Vec<TextElement>,
    },
    /// remote image URL。
    #[serde(rename = "image")]
    Image {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    /// app-server host から参照できる local image path。
    #[serde(rename = "localImage")]
    LocalImage {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    /// name と path で参照する skill。
    #[serde(rename = "skill")]
    Skill { name: String, path: String },
    /// prompt 内の named mention。
    #[serde(rename = "mention")]
    Mention { name: String, path: String },
}

/// image input で要求する decode または fidelity level。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ImageDetail {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "original")]
    Original,
}

/// text input 内の byte range と placeholder metadata。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TextElement {
    /// UTF-8 text buffer 上の byte range。
    pub byte_range: ByteRange,
    /// UI 表示または replacement に使用する optional placeholder。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
}

/// start inclusive、end exclusive の byte range。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

/// turn 単位で app-server へ要求する effective sandbox policy。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub enum SandboxPolicy {
    /// sandbox restriction を適用しない。
    #[serde(rename = "dangerFullAccess")]
    DangerFullAccess,
    /// filesystem write を禁止する policy。
    #[serde(rename = "readOnly")]
    ReadOnly {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_access: Option<bool>,
    },
    /// app-server 外部の sandbox 実装へ enforcement を委譲する policy。
    #[serde(rename = "externalSandbox")]
    ExternalSandbox {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_access: Option<NetworkAccess>,
    },
    ///指定 root への write を許可する policy。
    #[serde(rename = "workspaceWrite")]
    WorkspaceWrite {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exclude_slash_tmp: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exclude_tmpdir_env_var: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_access: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writable_roots: Option<Vec<String>>,
    },
}

/// external sandbox へ要求する network access level。
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NetworkAccess {
    Restricted,
    Enabled,
}

/// turn/start request parameter。
///
/// [`TurnStartParams::new`] は thread ID と input だけを必須とし、その他の field は thread default を
///上書きする場合だけ送信する。
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartParams {
    /// turn を開始する既存 thread。
    pub thread_id: ThreadId,
    ///今回の user input sequence。
    pub input: Vec<UserInput>,
    /// thread policy を turn 単位で上書きする approval policy。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<AskForApproval>,
    /// approval reviewer override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<ApprovalsReviewer>,
    /// caller が user message と対応付ける optional ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_user_message_id: Option<String>,
    /// turn 単位の sandbox policy override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<SandboxPolicy>,
    /// turn 単位の working directory override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// reasoning effort override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<ReasoningEffort>,
    /// model service tier override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    /// model override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// response personality override。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub personality: Option<Personality>,
    /// structured output を要求する JSON Schema。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// reasoning summary の生成方針。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<ReasoningSummary>,
}

impl TurnStartParams {
    ///必須 field だけを設定した turn/start parameter を生成する。
    pub fn new(thread_id: ThreadId, input: Vec<UserInput>) -> Self {
        Self {
            thread_id,
            input,
            approval_policy: None,
            approvals_reviewer: None,
            client_user_message_id: None,
            sandbox_policy: None,
            cwd: None,
            effort: None,
            service_tier: None,
            model: None,
            personality: None,
            output_schema: None,
            summary: None,
        }
    }
}

/// reasoning summary の出力 detail。
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ReasoningSummary {
    Auto,
    Concise,
    Detailed,
    None,
}

/// app-server が受理する reasoning effort 名。
///
/// protocol が将来追加する値を crate update なしで指定できるよう string newtype とし、空文字だけを拒否する。
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct ReasoningEffort(String);

impl<'de> Deserialize<'de> for ReasoningEffort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl ReasoningEffort {
    /// non-empty reasoning effort value を生成する。
    pub fn new(value: impl Into<String>) -> Result<Self, crate::ClientError> {
        let value = value.into();
        if value.is_empty() {
            return Err(crate::ClientError::InvalidConfiguration {
                field: "effort",
                reason: "must be non-empty",
            });
        }
        Ok(Self(value))
    }

    /// allocation せず underlying protocol value を参照する。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// app-server が返す turn snapshot。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    pub id: TurnId,
    /// turn に属する item の authoritative sequence。
    pub items: Vec<ThreadItem>,
    pub status: TurnStatus,
    /// app-server clock 上の完了時刻。
    #[serde(default)]
    pub completed_at: Option<i64>,
    /// app-server が計測した処理時間。単位は millisecond。
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// failed turn の error detail。
    #[serde(default)]
    pub error: Option<TurnError>,
    /// app-server clock 上の開始時刻。
    #[serde(default)]
    pub started_at: Option<i64>,
}

/// failed turn に付随する server-controlled error detail。
///
///内容は prompt、tool output、downstream error を含み得るため custom `Debug` ではすべて redact する。
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnError {
    pub message: String,
    #[serde(default)]
    pub additional_details: Option<String>,
    #[serde(default)]
    pub codex_error_info: Option<Value>,
}

impl fmt::Debug for TurnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnError")
            .field("message", &"<redacted>")
            .field(
                "additional_details",
                &self.additional_details.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "codex_error_info",
                &self.codex_error_info.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// turn 内の item。
///
///最終 response 抽出に必要な agent message は typed variant とし、その他は Schema validation 済み raw JSON を保持する。
#[derive(Clone)]
pub enum ThreadItem {
    /// model が生成した agent message。
    AgentMessage { id: String, text: String },
    ///専用 public 型を持たない Schema-known item。
    Other {
        id: String,
        kind: String,
        raw: Value,
    },
}

impl fmt::Debug for ThreadItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AgentMessage { id, .. } => formatter
                .debug_struct("AgentMessage")
                .field("id", id)
                .field("text", &"<redacted>")
                .finish(),
            Self::Other { id, kind, .. } => formatter
                .debug_struct("ThreadItem")
                .field("id", id)
                .field("kind", kind)
                .field("raw", &"<redacted>")
                .finish(),
        }
    }
}

impl<'de> Deserialize<'de> for ThreadItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Value::deserialize(deserializer)?;
        // kind 固有 field を読む前に checked-in Schema 全体で validation し、partially typed な不正 item を拒否する。
        super::schema::validate_thread_item(&raw).map_err(serde::de::Error::custom)?;
        let object = raw
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("thread item must be an object"))?;
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("thread item id must be a string"))?
            .to_owned();
        let kind = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("thread item type must be a string"))?;
        if kind == "agentMessage" {
            let text = object
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| serde::de::Error::custom("agentMessage text must be a string"))?
                .to_owned();
            Ok(Self::AgentMessage { id, text })
        } else {
            Ok(Self::Other {
                id,
                kind: kind.to_owned(),
                raw,
            })
        }
    }
}

/// terminal turn と、その correlation ID をまとめた `run_turn` result。
#[derive(Clone, Debug)]
pub struct TurnOutcome {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    /// authoritative terminal turn snapshot。
    pub turn: Turn,
}

impl TurnOutcome {
    /// terminal turn の item sequence から最後の agent message text を返す。
    ///
    /// delta notification は欠落または lag の可能性があるため、最終 message の真実源にはこの method を使用する。
    pub fn final_agent_message(&self) -> Option<&str> {
        self.turn.items.iter().rev().find_map(|item| match item {
            ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
            ThreadItem::Other { .. } => None,
        })
    }
}

/// turn/start response の wire payload。
#[derive(Debug, Deserialize)]
pub(crate) struct TurnStartResponse {
    pub turn: Turn,
}

/// turn/completed notification の wire payload。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TurnCompletedParams {
    pub thread_id: ThreadId,
    pub turn: Turn,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn final_agent_message_uses_last_authoritative_item() {
        let turn: Turn = serde_json::from_value(json!({
            "id": "turn-1",
            "status": "completed",
            "items": [
                {"id": "a", "type": "agentMessage", "text": "first"},
                {"id": "r", "type": "reasoning"},
                {"id": "b", "type": "agentMessage", "text": "last"}
            ]
        }))
        .unwrap();
        let outcome = TurnOutcome {
            thread_id: ThreadId::new("thread-1"),
            turn_id: turn.id.clone(),
            turn,
        };
        assert_eq!(outcome.final_agent_message(), Some("last"));
    }

    #[test]
    fn known_thread_items_require_nested_schema_types() {
        assert!(
            serde_json::from_value::<ThreadItem>(json!({
                "id": "cmd-1",
                "type": "commandExecution",
                "command": "echo ok",
                "commandActions": [42],
                "cwd": "/tmp",
                "status": "completed"
            }))
            .is_err()
        );
    }

    #[test]
    fn user_message_validates_nested_user_input() {
        assert!(
            serde_json::from_value::<ThreadItem>(json!({
                "id": "user-1",
                "type": "userMessage",
                "content": [{"type": "text", "text": "hello"}]
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<ThreadItem>(json!({
                "id": "user-1",
                "type": "userMessage",
                "content": [{"type": "text", "text": 1}]
            }))
            .is_err()
        );
    }

    #[test]
    fn final_agent_message_can_be_absent() {
        let turn: Turn = serde_json::from_value(json!({
            "id": "turn-1", "status": "interrupted", "items": []
        }))
        .unwrap();
        let outcome = TurnOutcome {
            thread_id: ThreadId::new("thread-1"),
            turn_id: turn.id.clone(),
            turn,
        };
        assert_eq!(outcome.final_agent_message(), None);
    }

    #[test]
    fn turn_params_use_schema_field_names_and_enum_values() {
        let mut params = TurnStartParams::new(
            ThreadId::new("thread-1"),
            vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
        );
        params.sandbox_policy = Some(SandboxPolicy::ExternalSandbox {
            network_access: Some(NetworkAccess::Restricted),
        });
        params.effort = Some(ReasoningEffort::new("medium").unwrap());
        let value = serde_json::to_value(params).unwrap();
        assert_eq!(value["threadId"], "thread-1");
        assert_eq!(value["input"][0]["type"], "text");
        assert_eq!(value["sandboxPolicy"]["type"], "externalSandbox");
        assert_eq!(value["sandboxPolicy"]["networkAccess"], "restricted");
        assert!(value.get("approvalPolicy").is_none());
    }

    #[test]
    fn reasoning_effort_must_be_non_empty() {
        assert!(ReasoningEffort::new("").is_err());
        assert_eq!(ReasoningEffort::new("high").unwrap().as_str(), "high");

        let effort: ReasoningEffort = serde_json::from_value(json!("custom")).unwrap();
        assert_eq!(effort.as_str(), "custom");
        assert!(serde_json::from_value::<ReasoningEffort>(json!("")).is_err());
    }

    #[test]
    fn optional_reasoning_effort_accepts_missing_and_null() {
        #[derive(Deserialize)]
        struct Payload {
            effort: Option<ReasoningEffort>,
        }

        let missing: Payload = serde_json::from_value(json!({})).unwrap();
        assert!(missing.effort.is_none());

        let null: Payload = serde_json::from_value(json!({"effort": null})).unwrap();
        assert!(null.effort.is_none());
    }

    #[test]
    fn turn_error_debug_redacts_server_controlled_content() {
        let secret = "super-secret-value";
        let error = TurnError {
            message: secret.to_owned(),
            additional_details: Some(secret.to_owned()),
            codex_error_info: Some(serde_json::json!({"detail": secret})),
        };

        let debug = format!("{error:?}");
        assert!(!debug.contains(secret));
        assert_eq!(debug.matches("<redacted>").count(), 3);
    }
}
