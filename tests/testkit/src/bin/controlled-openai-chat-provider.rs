use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use musteragent_testkit::ControlledOpenAiChatProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = env::var("MUSTERAGENT_CONTROLLED_PROVIDER_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:56081".to_owned())
        .parse::<SocketAddr>()?;
    let delay_ms = env::var("MUSTERAGENT_CONTROLLED_PROVIDER_DELAY_MS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(0);
    if delay_ms > 60_000 {
        return Err("MUSTERAGENT_CONTROLLED_PROVIDER_DELAY_MS must not exceed 60000".into());
    }
    let provider =
        ControlledOpenAiChatProvider::start_with_delay(address, Duration::from_millis(delay_ms))
            .await?;
    eprintln!(
        "musteragent controlled OpenAI Chat Provider ready at {}",
        provider.endpoint()
    );
    tokio::signal::ctrl_c().await?;
    provider.shutdown().await?;
    Ok(())
}
