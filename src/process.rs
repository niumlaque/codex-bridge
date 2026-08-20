//! app-server child process の起動、process group ownership、終了監視を担当する。
//!
//! child だけでなく、その子孫 process も cleanup 対象にするため dedicated process group を作成する。
//!正常 shutdown、unexpected exit、client handle の Drop のどの経路でも、最終的に child を reap して zombie と
//! orphan process を残さない。

use std::{env, path::Path, process::Stdio};

use tokio::{
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot},
    time::{Duration, Instant, sleep},
};

use crate::{ClientConfig, ClientError, EnvironmentPolicy, state::ShutdownReport};

/// Unix process group 全体を即時終了する signal。
const SIGKILL: i32 = 9;

unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, signal: i32) -> i32;
}

/// spawn 後に runtime task へ ownership を分配する child process と pipe 群。
pub(crate) struct SpawnedProcess {
    /// process supervisor が終了と reap を所有する child handle。
    pub child: Child,
    /// single writer task だけが所有する stdin pipe。
    pub stdin: ChildStdin,
    /// JSONL reader task だけが所有する stdout pipe。
    pub stdout: ChildStdout,
    /// stderr drain task だけが所有する stderr pipe。
    pub stderr: ChildStderr,
}

/// runtime から process supervisor へ送る lifecycle command。
pub(crate) enum ProcessCommand {
    /// deadline まで natural exit を待ち、超過時は process group を強制終了する。
    Shutdown {
        deadline: Instant,
        reply: oneshot::Sender<Result<ShutdownReport, ClientError>>,
    },
    /// stdout EOF を検出した。child status と突き合わせて failure cause を確定する。
    StdoutClosed,
    /// shutdown handshake を行わず process group を直ちに強制終了する。
    Kill,
}

/// validated config から dedicated process group 内に app-server を起動する。
///
/// pipe の一つでも取得できない場合は partially spawned process を返さず error とする。`kill_on_drop` は setup 中の
/// early return に対する safety net であり、通常の終了 ownership は process supervisor が持つ。
pub(crate) fn spawn_process(config: &ClientConfig) -> Result<SpawnedProcess, ClientError> {
    let mut command = Command::new(&config.executable);
    if config.environment_policy == EnvironmentPolicy::Clean {
        command.env_clear();
        if let Some(path) = env::var_os("PATH") {
            command.env("PATH", path);
        }
    }
    command
        .arg("app-server")
        .current_dir(&config.working_directory)
        .env("CODEX_HOME", &config.codex_home)
        .envs(&config.environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        // group ID 0 は child 自身の PID を leader とする新しい process group を作成する。
        .process_group(0);
    let mut child = command.spawn().map_err(|error| ClientError::SpawnFailed {
        detail: error.to_string(),
    })?;
    child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .ok_or_else(|| ClientError::SpawnFailed {
            detail: "app-server did not expose a valid process ID".to_owned(),
        })?;
    let stdin = child
        .stdin
        .take()
        .ok_or(ClientError::MissingPipe { pipe: "stdin" })?;
    let stdout = child
        .stdout
        .take()
        .ok_or(ClientError::MissingPipe { pipe: "stdout" })?;
    let stderr = child
        .stderr
        .take()
        .ok_or(ClientError::MissingPipe { pipe: "stderr" })?;
    Ok(SpawnedProcess {
        child,
        stdin,
        stdout,
        stderr,
    })
}

/// child exit と runtime command を一箇所で競合なく監視する process supervisor。
///
/// `biased` select により、同時に ready になった場合は明示的な shutdown/kill command を process exit より先に扱う。
///これにより正常 shutdown 中の exit を unexpected failure と誤分類しにくくする。
pub(crate) async fn supervise<F>(
    mut child: Child,
    mut receiver: mpsc::Receiver<ProcessCommand>,
    on_unexpected_exit: F,
) where
    F: Fn(ClientError) -> bool,
{
    let process_group = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .unwrap_or_default();
    tokio::select! {
        biased;
        command = receiver.recv() => match command {
            Some(ProcessCommand::Shutdown { deadline, reply }) => {
                let result = shutdown_child(&mut child, process_group, deadline).await;
                let _ = reply.send(result);
            }
            Some(ProcessCommand::StdoutClosed) => {
                let error = match child.try_wait() {
                    Ok(Some(status)) => {
                        // child が終了済みでも、同じ group に残った descendant は明示的に cleanup する。
                        kill_process_group(process_group);
                        ClientError::ProcessExited { status: status.to_string() }
                    }
                    Ok(None) => ClientError::StdoutClosed,
                    Err(error) => ClientError::ProcessExited {
                        status: format!("wait failed after stdout closed: {error}"),
                    },
                };
                if on_unexpected_exit(error) {
                    kill_and_reap(&mut child, process_group).await;
                } else {
                    finish_stopping(&mut child, process_group, &mut receiver).await;
                }
            }
            Some(ProcessCommand::Kill) | None => {
                kill_and_reap(&mut child, process_group).await;
            }
        },
        status = child.wait() => {
            kill_process_group(process_group);
            let error = match status {
                Ok(status) => ClientError::ProcessExited { status: status.to_string() },
                Err(error) => ClientError::ProcessExited { status: format!("wait failed: {error}") },
            };
            if !on_unexpected_exit(error) {
                finish_stopping(&mut child, process_group, &mut receiver).await;
            }
        }
    }
}

/// failure が既に runtime へ記録された後も、shutdown caller へ一意な reply を返すまで supervisor を維持する。
async fn finish_stopping(
    child: &mut Child,
    process_group: i32,
    receiver: &mut mpsc::Receiver<ProcessCommand>,
) {
    loop {
        match receiver.recv().await {
            Some(ProcessCommand::Shutdown { deadline, reply }) => {
                let result = shutdown_child(child, process_group, deadline).await;
                let _ = reply.send(result);
                return;
            }
            // stdout EOF は既に処理済みであり、追加の state transition は不要。
            Some(ProcessCommand::StdoutClosed) => {}
            Some(ProcessCommand::Kill) | None => {
                kill_and_reap(child, process_group).await;
                return;
            }
        }
    }
}

/// deadline までは child の natural exit を polling し、超過後は process group を kill して reap する。
async fn shutdown_child(
    child: &mut Child,
    process_group: i32,
    deadline: Instant,
) -> Result<ShutdownReport, ClientError> {
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                // leader が終了していても descendant が残っている可能性を cleanup result に反映する。
                let forced = kill_process_group(process_group);
                return Ok(ShutdownReport { forced });
            }
            Ok(None) if Instant::now() < deadline => sleep(Duration::from_millis(10)).await,
            Ok(None) => {
                kill_and_reap(child, process_group).await;
                return Err(ClientError::ShutdownTimeout);
            }
            Err(error) => {
                kill_and_reap(child, process_group).await;
                return Err(ClientError::ProcessExited {
                    status: format!("wait failed during shutdown: {error}"),
                });
            }
        }
    }
}

/// process group と child handle の両方へ kill を発行し、wait 完了まで ownership を保持する。
async fn kill_and_reap(child: &mut Child, process_group: i32) {
    kill_process_group(process_group);
    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// child を leader とする process group 全体へ SIGKILL を送る。
///
/// return value は signal delivery syscall が成功したかを示し、既に process group が消滅している場合は `false` となる。
fn kill_process_group(process_group: i32) -> bool {
    if process_group <= 0 {
        return false;
    }
    // SAFETY: negative PID は、この child 用に作成した process group を指定する。
    // SIGKILL は failure、Drop、shutdown cleanup の経路だけで使用する。
    unsafe { libc_kill(-process_group, SIGKILL) == 0 }
}

#[allow(dead_code)]
fn _path_is_used_for_documentation(_: &Path) {}
