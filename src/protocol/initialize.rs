//! initialize handshake で交換する client capability と server metadata を定義する。

use serde::{Deserialize, Serialize};

/// initialize request で app-server へ宣言する optional capability。
///
/// `None` の field は wire payload から省略し、server default を変更しない。caller が理解していない
/// experimental capability を暗黙に有効化してはならない。
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCapabilities {
    /// experimental API を使用する意思を明示する。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental_api: Option<bool>,
    /// MCP server の OpenAI form elicitation を処理できることを宣言する。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_server_openai_form_elicitation: Option<bool>,
    /// caller が受信を希望しない notification method の一覧。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opt_out_notification_methods: Option<Vec<String>>,
    /// request attestation を要求するかどうかを明示する。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_attestation: Option<bool>,
}

/// app-server へ送信する initialize request parameter。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InitializeParams<'a> {
    /// diagnostic と protocol negotiation に使用する client identity。
    pub client_info: ClientInfo<'a>,
    /// caller が明示した capability。未指定なら field 自体を送信しない。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<ClientCapabilities>,
}

/// initialize request に埋め込む client 名と crate version。
#[derive(Debug, Serialize)]
pub(crate) struct ClientInfo<'a> {
    pub name: &'a str,
    pub version: &'a str,
}

/// initialize response で app-server が返す実行環境 metadata。
///
/// handshake 成功の確認に加え、diagnostic や platform-dependent behavior の選択に使用できる。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResponse {
    /// app-server が実際に使用している `CODEX_HOME`。
    pub codex_home: String,
    /// platform の大分類。
    pub platform_family: String,
    /// platform の OS 名。
    pub platform_os: String,
    /// app-server が外部 request に使用する user agent。
    pub user_agent: String,
}
