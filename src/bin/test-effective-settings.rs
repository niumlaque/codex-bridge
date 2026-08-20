use std::env;
use std::path::PathBuf;

use codex_bridge::{Client, ClientConfig, ThreadStartParams};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let codex_home = PathBuf::from(env::var_os("CODEX_HOME").ok_or("CODEX_HOME is not set")?);
    let working_directory = env::current_dir()?;
    let client = Client::spawn(ClientConfig::new(codex_home, working_directory)).await?;

    println!("codex_home: {}", client.codex_home());

    let thread = client.start_thread(ThreadStartParams::default()).await?;

    println!("thread_id: {}", thread.thread_id.as_str());
    println!("model: {}", thread.model);
    match thread.reasoning_effort.as_ref() {
        Some(reasoning_effort) => {
            println!("reasoning_effort: Some({})", reasoning_effort.as_str());
        }
        None => println!("reasoning_effort: None"),
    }
    println!("approval_policy: {:?}", thread.approval_policy);
    println!("approvals_reviewer: {:?}", thread.approvals_reviewer);
    println!("sandbox: {:?}", thread.sandbox);
    println!("cwd: {}", thread.cwd);
    println!("model_provider: {}", thread.model_provider);

    client.shutdown().await?;
    Ok(())
}
