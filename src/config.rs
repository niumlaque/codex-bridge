//! app-server process の起動条件と、client 内で保持可能な resource 上限を定義する。
//!
//! validation は process 起動前に完了させる。無効な directory、ゼロ timeout、または worst-case memory
//! 上限を超える設定で child process や background task を開始してはならない。

use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::PathBuf,
    time::Duration,
};

use crate::{ClientError, protocol::ClientCapabilities};

/// event queue が payload の clone を保持した場合に許容する最大 memory 量。
const MAX_EVENT_RETAINED_BYTES: usize = 64 * 1024 * 1024;
/// 未応答 ServerRequest が raw payload を重複保持した場合に許容する最大 memory 量。
const MAX_SERVER_REQUEST_RETAINED_BYTES: usize = 64 * 1024 * 1024;
/// broadcast channel と event value が同一 payload を保持する worst-case copy 数。
const EVENT_RETAINED_COPIES: usize = 2;
/// state、event、caller 側 clone を含む ServerRequest payload の worst-case copy 数。
const SERVER_REQUEST_RETAINED_COPIES: usize = 4;

/// app-server へ公開する parent process の environment variable 範囲。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvironmentPolicy {
    /// inherited environment を消去し、executable 検索に必要な `PATH` だけを残す。
    ///
    /// caller が明示的に隔離された execution context を必要とする場合に選択する。
    Clean,
    /// parent process の environment をすべて継承する。
    ///
    /// shell、toolchain、SDK を caller と同じ execution context で利用する default policy。
    Inherit,
}

/// 単一の app-server child process を起動するための設定。
///
/// [`Client::spawn`](crate::Client::spawn) は全 field を validation してから process を生成する。
/// queue capacity と message size は独立ではなく、worst-case retained memory の積として制限される。
#[derive(Clone)]
pub struct ClientConfig {
    /// 起動する `codex` executable。
    ///
    /// relative path の場合は child process に残した `PATH` を使用して解決される。
    pub executable: PathBuf,
    /// child process へ `CODEX_HOME` として渡す既存 directory。
    pub codex_home: PathBuf,
    /// app-server process の current working directory。
    pub working_directory: PathBuf,
    /// parent process から継承する environment variable の基準 policy。
    pub environment_policy: EnvironmentPolicy,
    /// app-server の stderr を parent process へ直接継承するか。
    ///
    /// `false` の場合は pipe し、`Event::Stderr` として subscriber へ配信する。
    pub inherit_stderr: bool,
    ///基準 policy の適用後に明示追加する environment variable。
    ///
    /// `CODEX_HOME` は専用 field と競合するため、この map から上書きできない。
    pub environment: HashMap<OsString, OsString>,
    /// initialize request と `initialized` notification の完了を待つ最大時間。
    pub initialize_timeout: Duration,
    /// graceful shutdown、強制終了、background task 回収を含む全 shutdown 処理の最大時間。
    pub shutdown_timeout: Duration,
    /// stdout から受信する単一 JSONL message の最大 payload byte 数。
    pub max_message_bytes: usize,
    /// stdin writer へ未送信 message を保持できる件数。
    pub outgoing_queue_capacity: usize,
    /// subscriber が追従できない間に broadcast channel が保持する event 件数。
    pub event_queue_capacity: usize,
    /// initialize request で app-server へ宣言する optional capability。
    pub capabilities: Option<ClientCapabilities>,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("executable", &self.executable)
            .field("codex_home", &self.codex_home)
            .field("working_directory", &self.working_directory)
            .field("environment_policy", &self.environment_policy)
            .field("inherit_stderr", &self.inherit_stderr)
            // environment value は token や credential を含み得るため、key だけを diagnostic に残す。
            .field("environment", &self.environment.keys().collect::<Vec<_>>())
            .field("initialize_timeout", &self.initialize_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("outgoing_queue_capacity", &self.outgoing_queue_capacity)
            .field("event_queue_capacity", &self.event_queue_capacity)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

impl ClientConfig {
    /// parent process と同じ実行環境を使用する client 設定を生成する。
    ///
    /// environment はすべて継承し、queue と message size は worst-case retained memory が
    /// 64 MiB を超えない組み合わせに設定される。
    pub fn new(codex_home: impl Into<PathBuf>, working_directory: impl Into<PathBuf>) -> Self {
        Self {
            executable: PathBuf::from("codex"),
            codex_home: codex_home.into(),
            working_directory: working_directory.into(),
            environment_policy: EnvironmentPolicy::Inherit,
            inherit_stderr: false,
            environment: HashMap::new(),
            initialize_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
            max_message_bytes: 4 * 1024 * 1024,
            outgoing_queue_capacity: 64,
            event_queue_capacity: 8,
            capabilities: None,
        }
    }

    /// process や task を開始する前に、path、timeout、capacity、memory 上限を検証する。
    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        if !self.codex_home.is_dir() {
            return Err(ClientError::InvalidConfiguration {
                field: "codex_home",
                reason: "must be an existing directory",
            });
        }
        if !self.working_directory.is_dir() {
            return Err(ClientError::InvalidConfiguration {
                field: "working_directory",
                reason: "must be an existing directory",
            });
        }
        if self.initialize_timeout.is_zero() {
            return Err(ClientError::InvalidConfiguration {
                field: "initialize_timeout",
                reason: "must be non-zero",
            });
        }
        if self.shutdown_timeout.is_zero() {
            return Err(ClientError::InvalidConfiguration {
                field: "shutdown_timeout",
                reason: "must be non-zero",
            });
        }
        if self.max_message_bytes == 0 {
            return Err(ClientError::InvalidConfiguration {
                field: "max_message_bytes",
                reason: "must be non-zero",
            });
        }
        if self.outgoing_queue_capacity == 0 {
            return Err(ClientError::InvalidConfiguration {
                field: "outgoing_queue_capacity",
                reason: "must be non-zero",
            });
        }
        if self.event_queue_capacity == 0 {
            return Err(ClientError::InvalidConfiguration {
                field: "event_queue_capacity",
                reason: "must be non-zero",
            });
        }
        if retained_bytes(
            self.event_queue_capacity,
            self.max_message_bytes,
            EVENT_RETAINED_COPIES,
        )
        .is_none_or(|bytes| bytes > MAX_EVENT_RETAINED_BYTES)
        {
            return Err(ClientError::InvalidConfiguration {
                field: "event_queue_capacity",
                reason: "worst-case retained event payload exceeds 64 MiB",
            });
        }
        if retained_bytes(
            crate::state::MAX_PENDING_SERVER_REQUESTS,
            self.max_message_bytes,
            SERVER_REQUEST_RETAINED_COPIES,
        )
        .is_none_or(|bytes| bytes > MAX_SERVER_REQUEST_RETAINED_BYTES)
        {
            return Err(ClientError::InvalidConfiguration {
                field: "max_message_bytes",
                reason: "worst-case retained ServerRequest payload exceeds 64 MiB",
            });
        }
        if self
            .environment
            .keys()
            .any(|key| key == OsStr::new("CODEX_HOME"))
        {
            return Err(ClientError::InvalidConfiguration {
                field: "environment",
                reason: "must not override CODEX_HOME",
            });
        }
        Ok(())
    }
}

/// count、message size、copy 数の積を overflow-safe に計算する。
///
/// `None` は許容値ではなく、表現範囲を超えるほど大きい設定として caller が拒否する。
fn retained_bytes(count: usize, message_bytes: usize, copies: usize) -> Option<usize> {
    count.checked_mul(message_bytes)?.checked_mul(copies)
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use super::*;

    fn dirs() -> (PathBuf, PathBuf) {
        let test_name = std::thread::current()
            .name()
            .unwrap_or("test")
            .replace("::", "__");
        let root = std::env::temp_dir().join(format!(
            "codex-client-config-{}-{}",
            std::process::id(),
            test_name
        ));
        let home = root.join("home");
        let cwd = root.join("cwd");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&cwd).unwrap();
        (home, cwd)
    }

    #[test]
    fn validates_normal_config_and_defaults() {
        let (home, cwd) = dirs();
        let config = ClientConfig::new(home, cwd);
        assert_eq!(config.executable, PathBuf::from("codex"));
        assert_eq!(config.environment_policy, EnvironmentPolicy::Inherit);
        assert!(!config.inherit_stderr);
        assert_eq!(config.initialize_timeout, Duration::from_secs(30));
        assert_eq!(config.max_message_bytes, 4 * 1024 * 1024);
        assert_eq!(config.event_queue_capacity, 8);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_missing_directories_zero_values_codex_home_override_and_memory_excess() {
        let (home, cwd) = dirs();
        let mut cases = Vec::new();
        let mut missing_home = ClientConfig::new("/definitely/missing/codex-home", &cwd);
        cases.push(missing_home.clone());
        missing_home.codex_home = home.clone();
        missing_home.working_directory = PathBuf::from("/definitely/missing/cwd");
        cases.push(missing_home);

        let mut zero_timeout = ClientConfig::new(&home, &cwd);
        zero_timeout.initialize_timeout = Duration::ZERO;
        cases.push(zero_timeout);
        let mut zero_shutdown = ClientConfig::new(&home, &cwd);
        zero_shutdown.shutdown_timeout = Duration::ZERO;
        cases.push(zero_shutdown);
        let mut zero_message = ClientConfig::new(&home, &cwd);
        zero_message.max_message_bytes = 0;
        cases.push(zero_message);
        let mut zero_outgoing = ClientConfig::new(&home, &cwd);
        zero_outgoing.outgoing_queue_capacity = 0;
        cases.push(zero_outgoing);
        let mut zero_event = ClientConfig::new(&home, &cwd);
        zero_event.event_queue_capacity = 0;
        cases.push(zero_event);
        let mut excessive_event_memory = ClientConfig::new(&home, &cwd);
        excessive_event_memory.event_queue_capacity = 9;
        cases.push(excessive_event_memory);
        let mut excessive_request_memory = ClientConfig::new(&home, &cwd);
        excessive_request_memory.max_message_bytes = 8 * 1024 * 1024;
        cases.push(excessive_request_memory);
        let mut environment = ClientConfig::new(home, cwd);
        environment
            .environment
            .insert("CODEX_HOME".into(), "override".into());
        cases.push(environment);

        assert!(cases.into_iter().all(|config| matches!(
            config.validate(),
            Err(ClientError::InvalidConfiguration { .. })
        )));
    }
}
