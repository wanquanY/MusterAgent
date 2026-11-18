use std::convert::Infallible;
use std::env;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_stream::iter;

const DEFAULT_PORT: u16 = 18_080;
const NORMAL_DELAY_MS: u64 = 50;
const SLOW_DELAY_MS: u64 = 5_000;

#[derive(Debug, Default)]
struct ProviderMetrics {
    request_count: u64,
    text_count: u64,
    tool_count: u64,
    slow_count: u64,
    error_count: u64,
}

#[derive(Debug, Serialize)]
struct ProviderMetricsView {
    schema_version: &'static str,
    request_count: u64,
    text_count: u64,
    tool_count: u64,
    slow_count: u64,
    error_count: u64,
    normal_delay_ms: u64,
    slow_delay_ms: u64,
}

impl ProviderMetrics {
    fn view(&self) -> ProviderMetricsView {
        ProviderMetricsView {
            schema_version: "musteragent.legacy.agent-runtime.legacy-provider-baseline.v1",
            request_count: self.request_count,
            text_count: self.text_count,
            tool_count: self.tool_count,
            slow_count: self.slow_count,
            error_count: self.error_count,
            normal_delay_ms: NORMAL_DELAY_MS,
            slow_delay_ms: SLOW_DELAY_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Scenario {
    Text,
    Tool,
    Slow,
    Error,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = env::var("MUSTERAGENT_BASELINE_PROVIDER_PORT")
        .ok()
        .map(|value| value.parse::<u16>())
        .transpose()?
        .unwrap_or(DEFAULT_PORT);
    let metrics = Arc::new(Mutex::new(ProviderMetrics::default()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(metrics);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    println!(
        "{}",
        json!({
            "schema_version": "musteragent.legacy.agent-runtime.legacy-provider-baseline.v1",
            "status": "ready",
            "base_url": format!("http://127.0.0.1:{port}/v1"),
        })
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn get_metrics(
    State(metrics): State<Arc<Mutex<ProviderMetrics>>>,
) -> Json<ProviderMetricsView> {
    Json(metrics.lock().await.view())
}

async fn chat_completions(
    State(metrics): State<Arc<Mutex<ProviderMetrics>>>,
    Json(request): Json<Value>,
) -> Response {
    let scenario = classify(&request);
    {
        let mut guard = metrics.lock().await;
        guard.request_count += 1;
        match scenario {
            Scenario::Text => guard.text_count += 1,
            Scenario::Tool => guard.tool_count += 1,
            Scenario::Slow => guard.slow_count += 1,
            Scenario::Error => guard.error_count += 1,
        }
    }
    if scenario == Scenario::Error {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "message": "deterministic baseline provider failure",
                    "type": "baseline_provider_error",
                    "code": "baseline_503",
                }
            })),
        )
            .into_response();
    }
    let delay = match scenario {
        Scenario::Slow => SLOW_DELAY_MS,
        Scenario::Text | Scenario::Tool | Scenario::Error => NORMAL_DELAY_MS,
    };
    tokio::time::sleep(Duration::from_millis(delay)).await;
    let frames = if scenario == Scenario::Tool && !has_tool_result_for_latest_turn(&request) {
        tool_frames()
    } else {
        text_frames(scenario)
    };
    let events = frames
        .into_iter()
        .map(|data| Ok::<_, Infallible>(Event::default().data(data)));
    Sse::new(iter(events)).into_response()
}

fn classify(request: &Value) -> Scenario {
    let latest_user_content = messages(request)
        .and_then(|items| {
            items
                .iter()
                .rev()
                .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        })
        .and_then(|message| message.get("content"))
        .map(Value::to_string)
        .unwrap_or_default();
    if latest_user_content.contains("[baseline:error]") {
        Scenario::Error
    } else if latest_user_content.contains("[baseline:slow]") {
        Scenario::Slow
    } else if latest_user_content.contains("[baseline:tool]") {
        Scenario::Tool
    } else {
        Scenario::Text
    }
}

fn messages(request: &Value) -> Option<&Vec<Value>> {
    request.get("messages").and_then(Value::as_array)
}

fn has_tool_result_for_latest_turn(request: &Value) -> bool {
    let Some(messages) = messages(request) else {
        return false;
    };
    let Some(latest_user_index) = messages
        .iter()
        .rposition(|message| message.get("role").and_then(Value::as_str) == Some("user"))
    else {
        return false;
    };
    messages
        .iter()
        .skip(latest_user_index + 1)
        .any(|message| message.get("role").and_then(Value::as_str) == Some("tool"))
}

fn text_frames(scenario: Scenario) -> Vec<String> {
    let text = if scenario == Scenario::Tool {
        "musteragent baseline tool completed."
    } else if scenario == Scenario::Slow {
        "musteragent baseline slow response completed."
    } else {
        "musteragent baseline response completed."
    };
    vec![
        chunk(
            json!({"role": "assistant", "content": ""}),
            Value::Null,
            None,
        ),
        chunk(json!({"content": text}), Value::Null, None),
        chunk(json!({}), json!("stop"), Some(usage())),
        "[DONE]".to_owned(),
    ]
}

fn tool_frames() -> Vec<String> {
    vec![
        chunk(
            json!({
                "role": "assistant",
                "tool_calls": [{
                    "index": 0,
                    "id": "call_baseline_time",
                    "type": "function",
                    "function": {
                        "name": "get_current_time",
                        "arguments": "{}",
                    }
                }]
            }),
            Value::Null,
            None,
        ),
        chunk(json!({}), json!("tool_calls"), Some(usage())),
        "[DONE]".to_owned(),
    ]
}

fn usage() -> Value {
    json!({
        "prompt_tokens": 64,
        "completion_tokens": 8,
        "total_tokens": 72,
        "prompt_tokens_details": {"cached_tokens": 0},
        "completion_tokens_details": {"reasoning_tokens": 0},
    })
}

fn chunk(delta: Value, finish_reason: Value, usage: Option<Value>) -> String {
    json!({
        "id": "chatcmpl_musteragent_baseline",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "musteragent-baseline-model",
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }],
        "usage": usage,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_is_selected_from_the_latest_user_turn_only() {
        let request = json!({
            "messages": [
                {"role": "user", "content": "[baseline:error] old turn"},
                {"role": "assistant", "content": "old failure"},
                {"role": "user", "content": "[baseline:slow] current turn"}
            ]
        });

        assert_eq!(classify(&request), Scenario::Slow);
    }

    #[test]
    fn an_older_tool_result_does_not_complete_a_new_tool_turn() {
        let mut request = json!({
            "messages": [
                {"role": "user", "content": "[baseline:tool] old turn"},
                {"role": "tool", "content": "old result"},
                {"role": "assistant", "content": "old completion"},
                {"role": "user", "content": "[baseline:tool] current turn"}
            ]
        });

        assert!(!has_tool_result_for_latest_turn(&request));

        let Some(messages) = request["messages"].as_array_mut() else {
            panic!("messages must be an array");
        };
        messages.push(json!({"role": "tool", "content": "current result"}));
        assert!(has_tool_result_for_latest_turn(&request));
    }

    #[test]
    fn an_old_marker_does_not_pollute_a_regular_turn() {
        let request = json!({
            "messages": [
                {"role": "user", "content": "[baseline:error] old turn"},
                {"role": "user", "content": "regular current turn"}
            ]
        });

        assert_eq!(classify(&request), Scenario::Text);
    }
}
