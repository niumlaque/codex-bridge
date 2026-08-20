//! app-server から client へ送られる ServerRequest の分類と response payload を定義する。
//!
//! security-sensitive な approval method には専用 result 型を割り当て、別 method 用の payload を誤送信できないよう
//! runtime state でも kind を照合する。Schema 既知だが専用型を持たない method と未知 method だけが raw result を使う。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;

/// app-server が生成した JSON-RPC request ID。
///
/// response では受信した値を型と内容を変えずに返す。
pub type ServerRequestId = super::RequestId;

/// typed result を要求する ServerRequest method の internal classification。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TypedServerRequestKind {
    CommandExecutionApproval,
    FileChangeApproval,
    PermissionApproval,
    UserInput,
}

/// method 名と typed result kind の対応を一元管理する descriptor。
#[derive(Clone, Copy)]
struct TypedServerRequestDescriptor {
    method: &'static str,
    kind: TypedServerRequestKind,
}

/// typed response を強制する ServerRequest method の authoritative table。
const TYPED_SERVER_REQUESTS: &[TypedServerRequestDescriptor] = &[
    TypedServerRequestDescriptor {
        method: "item/commandExecution/requestApproval",
        kind: TypedServerRequestKind::CommandExecutionApproval,
    },
    TypedServerRequestDescriptor {
        method: "item/fileChange/requestApproval",
        kind: TypedServerRequestKind::FileChangeApproval,
    },
    TypedServerRequestDescriptor {
        method: "item/permissions/requestApproval",
        kind: TypedServerRequestKind::PermissionApproval,
    },
    TypedServerRequestDescriptor {
        method: "item/tool/requestUserInput",
        kind: TypedServerRequestKind::UserInput,
    },
];

/// method が typed response を要求する場合、その kind を返す。
pub(crate) fn typed_server_request_kind(method: &str) -> Option<TypedServerRequestKind> {
    TYPED_SERVER_REQUESTS
        .iter()
        .find(|descriptor| descriptor.method == method)
        .map(|descriptor| descriptor.kind)
}

/// command execution approval request に対する decision。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandExecutionApprovalDecision {
    ///今回の command execution だけを許可する。
    Accept,
    ///現在の session 中は同種の command execution を許可する。
    AcceptForSession,
    /// exec policy の amendment を同時に適用して許可する。
    AcceptWithExecpolicyAmendment { execpolicy_amendment: Vec<String> },
    /// network policy の amendment を適用して許可する。
    ApplyNetworkPolicyAmendment(NetworkPolicyAmendment),
    /// request を拒否する。
    Decline,
    /// approval flow 自体を取消す。
    Cancel,
}

impl CommandExecutionApprovalDecision {
    /// app-server Schema が要求する tagged JSON value へ変換する。
    fn into_value(self) -> Value {
        match self {
            Self::Accept => json!("accept"),
            Self::AcceptForSession => json!("acceptForSession"),
            Self::AcceptWithExecpolicyAmendment {
                execpolicy_amendment,
            } => json!({
                "acceptWithExecpolicyAmendment": {
                    "execpolicy_amendment": execpolicy_amendment
                }
            }),
            Self::ApplyNetworkPolicyAmendment(amendment) => json!({
                "applyNetworkPolicyAmendment": {
                    "network_policy_amendment": amendment
                }
            }),
            Self::Decline => json!("decline"),
            Self::Cancel => json!("cancel"),
        }
    }
}

/// network policy rule を追加する際の action。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkPolicyRuleAction {
    Allow,
    Deny,
}

impl Serialize for NetworkPolicyRuleAction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        })
    }
}

/// command execution approval と同時に適用する host 単位の network policy amendment。
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NetworkPolicyAmendment {
    /// rule の対象 host。
    pub host: String,
    /// host への通信を許可または拒否する action。
    pub action: NetworkPolicyRuleAction,
}

/// file change approval request に対する decision。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileChangeApprovalDecision {
    ///今回の file change だけを許可する。
    Accept,
    ///現在の session 中は同種の file change を許可する。
    AcceptForSession,
    /// request を拒否する。
    Decline,
    /// approval flow 自体を取消す。
    Cancel,
}

impl FileChangeApprovalDecision {
    /// app-server Schema が要求する string value へ変換する。
    fn into_value(self) -> Value {
        json!(match self {
            Self::Accept => "accept",
            Self::AcceptForSession => "acceptForSession",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        })
    }
}

/// Standalone permission request に含まれる requested/granted permission profile。
///
/// Checked-in Schema が検証した profile を lossless に保持し、response では requested subset を
/// そのまま grant できる。Field は非公開とし、caller が未検証の profile を組み立てられないようにする。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PermissionProfile(Value);

impl PermissionProfile {
    /// Terminal 表示や永続化のため、検証済み profile を JSON value として参照する。
    pub fn as_value(&self) -> &Value {
        &self.0
    }
}

/// `item/permissions/requestApproval` の checked-in Schema に対応する params。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PermissionApprovalParams {
    pub cwd: PathBuf,
    pub environment_id: Option<String>,
    pub item_id: String,
    pub permissions: PermissionProfile,
    pub reason: Option<String>,
    pub started_at_ms: i64,
    pub thread_id: String,
    pub turn_id: String,
}

/// Standalone permission approval request に対する user decision。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PermissionApprovalDecision {
    /// Requested permission profile 全体を current turn に限り grant する。
    GrantForTurn(PermissionProfile),
    /// Requested permission profile 全体を current session に grant する。
    GrantForSession(PermissionProfile),
    /// Requested permission を一つも grant しない。
    Decline,
}

impl PermissionApprovalDecision {
    fn into_value(self) -> Value {
        let (permissions, scope) = match self {
            Self::GrantForTurn(permissions) => (permissions.0, "turn"),
            Self::GrantForSession(permissions) => (permissions.0, "session"),
            Self::Decline => (json!({}), "turn"),
        };
        json!({
            "permissions": permissions,
            "scope": scope,
        })
    }
}

///一つの user-input question に対する回答群。
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolRequestUserInputAnswer {
    /// question が複数選択を許す場合を含む answer の一覧。
    pub answers: Vec<String>,
}

/// tool request 内の question ID と回答を対応付ける response。
///
/// `BTreeMap` を使用し、serialization 順序を deterministic に保つ。
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ToolRequestUserInputResponse {
    pub answers: BTreeMap<String, ToolRequestUserInputAnswer>,
}

/// ServerRequest へ返す success result。
///
/// typed variant は method と result shape の取り違えを防ぐ。`Unsupported` は Schema 既知だが専用型を
///持たない method、または未知 method へ caller が明示的に raw JSON を返す場合だけ使用する。
#[derive(Clone)]
pub enum ServerRequestResult {
    CommandExecutionApproval(CommandExecutionApprovalDecision),
    FileChangeApproval(FileChangeApprovalDecision),
    PermissionApproval(PermissionApprovalDecision),
    UserInput(ToolRequestUserInputResponse),
    Unsupported(Value),
}

impl std::fmt::Debug for ServerRequestResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::CommandExecutionApproval(_) => "CommandExecutionApproval",
            Self::FileChangeApproval(_) => "FileChangeApproval",
            Self::PermissionApproval(_) => "PermissionApproval",
            Self::UserInput(_) => "UserInput",
            Self::Unsupported(_) => "Unsupported",
        };
        // approval 内容や user input を log へ流さず、variant 名だけを diagnostic に残す。
        formatter
            .debug_tuple("ServerRequestResult")
            .field(&kind)
            .finish()
    }
}

impl ServerRequestResult {
    /// Schema 既知だが typed result を持たない method、または未知 method 用の raw result を生成する。
    ///
    ///最初から typed 対応している approval/input method へ渡すと、送信前の kind validation で拒否される。
    pub fn new(value: Value) -> Self {
        Self::Unsupported(value)
    }

    /// command execution approval 用の typed result を生成する。
    pub fn command_execution_approval(decision: CommandExecutionApprovalDecision) -> Self {
        Self::CommandExecutionApproval(decision)
    }

    /// file change approval 用の typed result を生成する。
    pub fn file_change_approval(decision: FileChangeApprovalDecision) -> Self {
        Self::FileChangeApproval(decision)
    }

    /// standalone permission approval 用の typed result を生成する。
    pub fn permission_approval(decision: PermissionApprovalDecision) -> Self {
        Self::PermissionApproval(decision)
    }

    /// tool user-input request 用の typed result を生成する。
    pub fn user_input(response: ToolRequestUserInputResponse) -> Self {
        Self::UserInput(response)
    }

    /// app-server へ送信される result payload の clone を取得する。
    pub fn as_value(&self) -> Value {
        self.clone().into_value()
    }

    /// runtime state が request method との整合性を検証するための internal kind。
    pub(crate) fn typed_kind(&self) -> Option<TypedServerRequestKind> {
        match self {
            Self::CommandExecutionApproval(_) => {
                Some(TypedServerRequestKind::CommandExecutionApproval)
            }
            Self::FileChangeApproval(_) => Some(TypedServerRequestKind::FileChangeApproval),
            Self::PermissionApproval(_) => Some(TypedServerRequestKind::PermissionApproval),
            Self::UserInput(_) => Some(TypedServerRequestKind::UserInput),
            Self::Unsupported(_) => None,
        }
    }

    /// public result 型を wire Schema に一致する JSON value へ変換する。
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::CommandExecutionApproval(decision) => {
                json!({"decision": decision.into_value()})
            }
            Self::FileChangeApproval(decision) => {
                json!({"decision": decision.into_value()})
            }
            Self::PermissionApproval(decision) => decision.into_value(),
            Self::UserInput(response) => serde_json::to_value(response)
                .expect("ToolRequestUserInputResponse is always serializable"),
            Self::Unsupported(value) => value,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn typed_server_request_methods_exist_in_checked_in_schema() {
        for descriptor in TYPED_SERVER_REQUESTS {
            assert!(
                crate::protocol::is_known_server_request_method(descriptor.method),
                "typed ServerRequest method is absent from checked-in Schema: {}",
                descriptor.method
            );
        }
    }

    #[test]
    fn typed_server_request_methods_are_unique() {
        let mut methods = HashSet::new();
        for descriptor in TYPED_SERVER_REQUESTS {
            assert!(methods.insert(descriptor.method));
        }
    }

    #[test]
    fn typed_server_request_lookup_uses_the_descriptor_table() {
        assert_eq!(
            typed_server_request_kind("item/commandExecution/requestApproval"),
            Some(TypedServerRequestKind::CommandExecutionApproval)
        );
        assert_eq!(
            typed_server_request_kind("item/fileChange/requestApproval"),
            Some(TypedServerRequestKind::FileChangeApproval)
        );
        assert_eq!(
            typed_server_request_kind("item/permissions/requestApproval"),
            Some(TypedServerRequestKind::PermissionApproval)
        );
        assert_eq!(
            typed_server_request_kind("item/tool/requestUserInput"),
            Some(TypedServerRequestKind::UserInput)
        );
        assert_eq!(typed_server_request_kind("future/request"), None);
    }

    #[test]
    fn typed_result_shapes_follow_checked_in_schema() {
        assert_eq!(
            ServerRequestResult::command_execution_approval(
                CommandExecutionApprovalDecision::Accept
            )
            .as_value(),
            json!({"decision": "accept"})
        );
        assert_eq!(
            ServerRequestResult::file_change_approval(FileChangeApprovalDecision::Decline)
                .as_value(),
            json!({"decision": "decline"})
        );
        let permissions: PermissionProfile = serde_json::from_value(json!({
            "fileSystem": {"write": ["/workspace/shared"]},
            "network": {"enabled": true}
        }))
        .unwrap();
        let grant = ServerRequestResult::permission_approval(
            PermissionApprovalDecision::GrantForSession(permissions),
        )
        .as_value();
        crate::protocol::schema::validate_test_document(
            include_str!("../../schema/PermissionsRequestApprovalResponse.json"),
            &grant,
            "PermissionsRequestApprovalResponse",
        )
        .unwrap();
        assert_eq!(
            grant,
            json!({
                "permissions": {
                    "fileSystem": {"write": ["/workspace/shared"]},
                    "network": {"enabled": true}
                },
                "scope": "session"
            })
        );
        let decline = ServerRequestResult::permission_approval(PermissionApprovalDecision::Decline)
            .as_value();
        crate::protocol::schema::validate_test_document(
            include_str!("../../schema/PermissionsRequestApprovalResponse.json"),
            &decline,
            "PermissionsRequestApprovalResponse",
        )
        .unwrap();
        assert_eq!(
            ServerRequestResult::user_input(ToolRequestUserInputResponse::default()).as_value(),
            json!({"answers": {}})
        );
    }
}
