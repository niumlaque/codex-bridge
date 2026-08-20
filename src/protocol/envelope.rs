//! stdout から受信した JSON value を JSON-RPC response、ServerRequest、notification へ分類する。
//!
//! method 固有 payload の validation より前に envelope の排他条件を検証し、曖昧な message を受理しない。
//! diagnostic に出す server-generated ID は control character を escape し、長さを制限する。

use std::fmt::{self, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ClientError;

/// diagnostic へ出力する string request ID の最大文字数。
const MAX_DIAGNOSTIC_ID_CHARS: usize = 128;

/// JSON-RPC request/response correlation に使用する ID。
///
/// app-server 由来の ServerRequest は string ID と integer ID の両方を取り得る。client が発行する request は
/// positive integer だけを使用し、dispatcher 側でさらに制約を検証する。
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RequestId {
    /// server-generated string ID。
    String(String),
    /// signed 64-bit integer ID。
    Integer(i64),
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => write_escaped(formatter, value),
            Self::Integer(value) => value.fmt(formatter),
        }
    }
}

/// untrusted string ID を single-line diagnostic として安全に整形する。
fn write_escaped(formatter: &mut fmt::Formatter<'_>, value: &str) -> fmt::Result {
    let mut chars = value.chars();
    for character in chars.by_ref().take(MAX_DIAGNOSTIC_ID_CHARS) {
        for escaped in character.escape_default() {
            formatter.write_char(escaped)?;
        }
    }
    if chars.next().is_some() {
        formatter.write_char('…')?;
    }
    Ok(())
}

/// JSON-RPC error object。
///
/// `message` と `data` は app-server または downstream service が生成した内容を含むため、`Debug` では
///公開しない。明示的に field を読む caller は、自身の log policy に従って取り扱う必要がある。
#[derive(Clone, Deserialize, Serialize)]
pub struct RpcError {
    /// protocol または application 固有の error code。
    pub code: i64,
    /// human-readable error message。
    pub message: String,
    /// method 固有の optional error data。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl fmt::Debug for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcError")
            .field("code", &self.code)
            .field("message", &"<redacted>")
            .field("data", &self.data.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// envelope shape の validation 後に dispatcher へ渡す incoming message。
#[derive(Debug)]
pub(crate) enum IncomingMessage {
    /// client request に対する success response。
    Success { id: RequestId, result: Value },
    /// client request に対する error response。
    Error { id: RequestId, error: RpcError },
    /// app-server から client への response 必須 request。
    ServerRequest {
        id: RequestId,
        method: String,
        params: Value,
        ///未知 field を失わないために保持する original envelope。
        raw: Value,
    },
    /// response を要求しない app-server notification。
    Notification {
        method: String,
        params: Value,
        ///未知 field を失わないために保持する original envelope。
        raw: Value,
    },
}

/// JSON value の envelope shape を検証し、mutually exclusive な incoming message 種別へ分類する。
///
/// `result` と `error` の同居、response への `method` 混入、ID のない response などは protocol violation とする。
/// `jsonrpc` field は optional だが、存在する場合は必ず `"2.0"` でなければならない。
pub(crate) fn classify_message(value: Value) -> Result<IncomingMessage, ClientError> {
    let object = value
        .as_object()
        .ok_or_else(|| violation("JSON-RPC envelope must be an object"))?;

    if let Some(jsonrpc) = object.get("jsonrpc")
        && jsonrpc.as_str() != Some("2.0")
    {
        return Err(violation("jsonrpc must be \"2.0\" when present"));
    }

    let id = object
        .get("id")
        .map(|id| {
            serde_json::from_value::<RequestId>(id.clone())
                .map_err(|_| violation("id must be a string or signed 64-bit integer"))
        })
        .transpose()?;
    let method = object
        .get("method")
        .map(|method| {
            method
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| violation("method must be a string"))
        })
        .transpose()?;
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");

    if has_result && has_error {
        return Err(violation("response contains both result and error"));
    }
    if has_result || has_error {
        if method.is_some() {
            return Err(violation("response must not contain method"));
        }
        let id = id.ok_or_else(|| violation("response is missing id"))?;
        if has_result {
            return Ok(IncomingMessage::Success {
                id,
                result: object.get("result").cloned().expect("checked above"),
            });
        }
        let error = serde_json::from_value(object.get("error").cloned().expect("checked above"))
            .map_err(|_| violation("invalid RPC error object"))?;
        return Ok(IncomingMessage::Error { id, error });
    }

    match (id, method) {
        (Some(id), Some(method)) => Ok(IncomingMessage::ServerRequest {
            id,
            method,
            params: object.get("params").cloned().unwrap_or(Value::Null),
            raw: value,
        }),
        (None, Some(method)) => Ok(IncomingMessage::Notification {
            method,
            params: object.get("params").cloned().unwrap_or(Value::Null),
            raw: value,
        }),
        _ => Err(violation(
            "message is not a response, request, or notification",
        )),
    }
}

/// envelope validation error を統一した protocol error へ変換する。
fn violation(reason: impl Into<String>) -> ClientError {
    ClientError::ProtocolViolation {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn classifies_success_error_notification_and_server_request() {
        assert!(matches!(
            classify_message(json!({"id": 1, "result": {}})).unwrap(),
            IncomingMessage::Success { .. }
        ));
        assert!(matches!(
            classify_message(json!({"id": 1, "error": {"code": -1, "message": "no"}})).unwrap(),
            IncomingMessage::Error { .. }
        ));
        assert!(matches!(
            classify_message(json!({"method": "event", "params": {}})).unwrap(),
            IncomingMessage::Notification { .. }
        ));
        assert!(matches!(
            classify_message(json!({"id": "server", "method": "ask", "params": {}})).unwrap(),
            IncomingMessage::ServerRequest { .. }
        ));
    }

    #[test]
    fn validates_jsonrpc_and_envelope_shape() {
        for value in [
            json!({"jsonrpc": "1.0", "method": "event"}),
            json!({"id": 1, "result": {}, "error": {"code": 1, "message": "x"}}),
            json!({"id": 1}),
            json!([]),
        ] {
            assert!(matches!(
                classify_message(value),
                Err(ClientError::ProtocolViolation { .. })
            ));
        }
    }

    #[test]
    fn request_id_display_escapes_controls_and_limits_length() {
        let id = RequestId::String(format!("line\n\u{1b}[31m{}", "x".repeat(256)));
        let displayed = id.to_string();
        assert!(!displayed.contains('\n'));
        assert!(!displayed.contains('\u{1b}'));
        assert!(displayed.ends_with('…'));
    }
}
