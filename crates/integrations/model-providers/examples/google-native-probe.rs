//! Read synthetic canary events from a private pipe; never print replay bytes.
use musteragent_identity::VersionedKey;
use musteragent_model::ApiFormat;
use musteragent_model_providers::ProviderNormalizer;
use musteragent_runtime_model::{ModelRouteRef, ModelStreamEvent, ModelStreamState};
use serde_json::{Value, json};
use std::io::{Read as _, Write as _};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let format = match std::env::args().nth(1).as_deref() {
        Some("google") => ApiFormat::GoogleGenerateContent,
        Some("google_interactions") => ApiFormat::GoogleInteractions,
        _ => return Err("native format required".into()),
    };
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(32 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 32 * 1024 * 1024 {
        return Err("canary input exceeds bound".into());
    }
    let frames: Vec<Value> = serde_json::from_slice(&bytes)?;
    let mut normalizer = ProviderNormalizer::new(
        format,
        ModelRouteRef::from_key(VersionedKey::new("gemini.canary@1")?),
        "gemini-4-carbon",
        "canary-invocation",
    )?;
    let mut state = ModelStreamState::default();
    let mut text = String::new();
    let mut private_items = 0;
    let mut tools = 0;
    for frame in frames {
        let kind = frame
            .get("event_type")
            .or_else(|| frame.get("type"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        for event in normalizer.observe(kind.as_deref(), frame)? {
            state.accept(&event)?;
            match event {
                ModelStreamEvent::TextDelta { delta, .. } => text.push_str(delta.as_str()),
                ModelStreamEvent::ProviderReasoningReplay { .. } => private_items += 1,
                ModelStreamEvent::ToolCallCompleted { .. } => tools += 1,
                _ => {}
            }
        }
    }
    for event in normalizer.finish()? {
        state.accept(&event)?;
    }
    std::io::stdout().write_all(json!({"terminal":state.is_terminal(),"text":text,"private_items":private_items,"tool_calls":tools}).to_string().as_bytes())?;
    Ok(())
}
