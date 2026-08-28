use std::{env, path::PathBuf};

use codex_bridge::{Client, ClientConfig, ThreadStartParams, TurnStartParams, UserInput};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let codex_home = PathBuf::from(env::var_os("CODEX_HOME").ok_or("CODEX_HOME is not set")?);
    let mut config = ClientConfig::new(codex_home, env::current_dir()?);
    config.inherit_stderr = true;

    let client = Client::spawn(config).await?;
    let thread = client.start_thread(ThreadStartParams::default()).await?;
    let outcome = client
        .run_turn(TurnStartParams::new(
            thread.thread_id,
            vec![UserInput::Text {
                text: "これはテストです".to_owned(),
                text_elements: Vec::new(),
            }],
        ))
        .await?;

    if let Some(message) = outcome.final_agent_message() {
        println!("{message}");
    }

    client.shutdown().await?;
    Ok(())
}
