//! frequently used app-server notification を typed event payload へ変換する型を定義する。
//!
//! notification payload は server-controlled data である。message body や error detail を含む型は custom `Debug`
//! implementation を経由し、通常の diagnostic 出力から内容を除外する。

use std::fmt;

use serde::Deserialize;

use super::{
    server_request::ServerRequestId,
    thread::{ThreadId, ThreadStatus},
    turn::{ThreadItem, Turn, TurnCompletedParams, TurnId},
};

/// turn が開始されたことを通知する payload。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TurnStartedNotification {
    /// turn を所有する thread。
    pub thread_id: ThreadId,
    ///開始時点の turn snapshot。
    pub turn: Turn,
}

/// turn が terminal state へ到達したことを通知する payload。
///
/// dispatcher は公開前に status が terminal であることと active turn との ID 整合性を検証する。
/// `turn.items` は空の場合があるため、完成済み item の取得には `item/completed` を使用する。
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TurnCompletedNotification {
    /// turn を所有する thread。
    pub thread_id: ThreadId,
    /// terminal state を含む turn snapshot。
    pub turn: Turn,
}

impl TurnCompletedNotification {
    /// internal wire type から public notification 型へ変換する。
    pub(crate) fn from_wire(value: TurnCompletedParams) -> Self {
        Self {
            thread_id: value.thread_id,
            turn: value.turn,
        }
    }
}

/// thread item の処理開始を通知する payload。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ItemStartedNotification {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    ///開始した item の typed または raw representation。
    pub item: ThreadItem,
    /// app-server が記録した開始時刻。単位は millisecond。
    pub started_at_ms: i64,
}

/// thread item の処理完了を通知する payload。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ItemCompletedNotification {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    ///完了後の canonical item snapshot。
    pub item: ThreadItem,
    /// app-server が記録した完了時刻。単位は millisecond。
    pub completed_at_ms: i64,
}

/// agent message の incremental text delta。
///
/// `delta` は model output を含むため、custom `Debug` では redact する。完全な最終 message が必要な処理は
/// completed agent-message item を使用し、delta の連結だけを真実源にしてはならない。
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AgentMessageDeltaNotification {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    /// delta が属する agent-message item ID。
    pub item_id: String,
    ///新たに追加された text fragment。
    pub delta: String,
}

impl fmt::Debug for AgentMessageDeltaNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentMessageDeltaNotification")
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("item_id", &self.item_id)
            .field("delta", &"<redacted>")
            .finish()
    }
}

/// thread status の変更を通知する payload。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ThreadStatusChangedNotification {
    pub thread_id: ThreadId,
    pub status: ThreadStatus,
}

/// pending ServerRequest が app-server 側で解決されたことを通知する payload。
///
///受信時に local pending table から request が除去されるため、それ以降の response attempt は unknown request として拒否される。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ServerRequestResolvedNotification {
    pub request_id: ServerRequestId,
    pub thread_id: ThreadId,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn typed_notifications_use_schema_field_names() {
        let delta: AgentMessageDeltaNotification = serde_json::from_value(json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "item-1",
            "delta": "hello"
        }))
        .unwrap();
        assert_eq!(delta.thread_id.as_str(), "thread-1");
        assert_eq!(delta.turn_id.as_str(), "turn-1");
        assert_eq!(delta.item_id, "item-1");
        assert_eq!(delta.delta, "hello");

        let resolved: ServerRequestResolvedNotification = serde_json::from_value(json!({
            "requestId": "request-1",
            "threadId": "thread-1"
        }))
        .unwrap();
        assert_eq!(resolved.thread_id.as_str(), "thread-1");
    }

    #[test]
    fn item_notifications_materialize_thread_items() {
        let started: ItemStartedNotification = serde_json::from_value(json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {"id": "item-1", "type": "agentMessage", "text": "hello"},
            "startedAtMs": 1
        }))
        .unwrap();
        assert!(matches!(started.item, ThreadItem::AgentMessage { .. }));
        assert_eq!(started.started_at_ms, 1);
    }

    #[test]
    fn agent_message_delta_debug_redacts_content() {
        let secret = "super-secret-value";
        let delta = AgentMessageDeltaNotification {
            thread_id: ThreadId::new("thread-1"),
            turn_id: TurnId::new("turn-1"),
            item_id: "item-1".to_owned(),
            delta: secret.to_owned(),
        };

        let debug = format!("{delta:?}");
        assert!(debug.contains("thread-1"));
        assert!(!debug.contains(secret));
        assert!(debug.contains("<redacted>"));
    }
}
