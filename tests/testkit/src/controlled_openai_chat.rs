use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::iter;

const SUCCESS_TEXT: &str = "musteragent runtime E2E succeeded";
const TOOL_RESULT_TEXT: &str = "musteragent completed the tool call and continued";
const CRASH_RECOVERY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

/// Deterministic OpenAI Chat Completions SSE server used only by integration
/// and browser acceptance tests. Scenario selection lives in explicit prompt
/// markers, so this server never attempts to emulate model intelligence.
#[derive(Debug)]
pub struct ControlledOpenAiChatProvider {
    endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
    request_count: Arc<AtomicUsize>,
    request_prompts: Arc<Mutex<Vec<String>>>,
    request_gate: Option<Arc<RequestGate>>,
}

impl ControlledOpenAiChatProvider {
    pub async fn start(address: SocketAddr) -> Result<Self, ControlledProviderError> {
        Self::start_with_options(address, Duration::ZERO, false).await
    }

    pub async fn start_with_delay(
        address: SocketAddr,
        delay_all_responses: Duration,
    ) -> Result<Self, ControlledProviderError> {
        Self::start_with_options(address, delay_all_responses, false).await
    }

    /// Starts a Provider whose HTTP requests remain open until the test
    /// explicitly releases them. This gives process-level acceptance tests a
    /// deterministic start barrier without replacing any Runtime component.
    pub async fn start_paused(address: SocketAddr) -> Result<Self, ControlledProviderError> {
        Self::start_with_options(address, Duration::ZERO, true).await
    }

    async fn start_with_options(
        address: SocketAddr,
        delay_all_responses: Duration,
        paused: bool,
    ) -> Result<Self, ControlledProviderError> {
        Self::start_script(address, delay_all_responses, paused, None, None).await
    }

    /// Pin a test scenario independently of runtime recovery follow-up prompts.
    /// The original HTTP messages are still captured and tool results still
    /// determine the second turn. This is a scripted provider, not an agent.
    pub async fn start_pinned_scenario(
        address: SocketAddr,
        scenario: String,
        paused: bool,
    ) -> Result<Self, ControlledProviderError> {
        Self::start_script(address, Duration::ZERO, paused, Some(scenario), None).await
    }

    /// Hold only requests containing this task marker, leaving parent requests
    /// live so a test can kill or cancel precisely during child execution.
    pub async fn start_paused_marker(
        address: SocketAddr,
        marker: String,
    ) -> Result<Self, ControlledProviderError> {
        Self::start_script(address, Duration::ZERO, true, None, Some(marker)).await
    }

    async fn start_script(
        address: SocketAddr,
        delay_all_responses: Duration,
        paused: bool,
        scenario: Option<String>,
        pause_marker: Option<String>,
    ) -> Result<Self, ControlledProviderError> {
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_prompts = Arc::new(Mutex::new(Vec::new()));
        let request_gate = paused.then(|| Arc::new(RequestGate::new()));
        let state = ControlledProviderState {
            pause_marker,
            scenario,
            request_count: Arc::clone(&request_count),
            request_prompts: Arc::clone(&request_prompts),
            delay_all_responses,
            request_gate: request_gate.clone(),
            retry_attempts: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(state);
        let listener = TcpListener::bind(address).await?;
        let bound_address = listener.local_addr()?;
        let (shutdown, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = receiver.await;
                })
                .await
        });
        Ok(Self {
            endpoint: format!("http://{bound_address}/v1/chat/completions"),
            shutdown: Some(shutdown),
            task,
            request_count,
            request_prompts,
            request_gate,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn request_prompts(&self) -> Vec<String> {
        lock(&self.request_prompts).clone()
    }

    /// Waits until at least `expected` physical HTTP requests have reached a
    /// paused Provider. The caller still owns the release decision.
    pub async fn wait_for_requests(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> Result<(), ControlledProviderError> {
        let gate = self
            .request_gate
            .as_ref()
            .ok_or(ControlledProviderError::NotPaused)?;
        let wait = async {
            loop {
                let notified = gate.arrived.notified();
                if self.request_count() >= expected {
                    break;
                }
                notified.await;
            }
        };
        tokio::time::timeout(timeout, wait).await.map_err(|_| {
            ControlledProviderError::BarrierTimeout {
                expected,
                observed: self.request_count(),
            }
        })
    }

    /// Releases all requests held by a paused Provider. Release is monotonic
    /// and idempotent so teardown cannot accidentally re-arm the barrier.
    pub fn release(&self) -> Result<(), ControlledProviderError> {
        let gate = self
            .request_gate
            .as_ref()
            .ok_or(ControlledProviderError::NotPaused)?;
        gate.released.store(true, Ordering::SeqCst);
        gate.release.notify_waiters();
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), ControlledProviderError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await??;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ControlledProviderState {
    pause_marker: Option<String>,
    scenario: Option<String>,
    request_count: Arc<AtomicUsize>,
    request_prompts: Arc<Mutex<Vec<String>>>,
    delay_all_responses: Duration,
    request_gate: Option<Arc<RequestGate>>,
    retry_attempts: Arc<Mutex<BTreeMap<String, usize>>>,
}

#[derive(Debug)]
struct RequestGate {
    released: AtomicBool,
    arrived: Notify,
    release: Notify,
}

impl RequestGate {
    fn new() -> Self {
        Self {
            released: AtomicBool::new(false),
            arrived: Notify::new(),
            release: Notify::new(),
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.release.notified();
            if self.released.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    }
}

async fn chat_completions(
    State(state): State<ControlledProviderState>,
    Json(request): Json<Value>,
) -> Response {
    state.request_count.fetch_add(1, Ordering::SeqCst);
    let prompt = latest_user_text(&request);
    lock(&state.request_prompts).push(prompt.clone());
    if let Some(gate) = &state.request_gate {
        gate.arrived.notify_waiters();
        if state
            .pause_marker
            .as_ref()
            .is_none_or(|marker| prompt.contains(marker))
        {
            gate.wait().await;
        }
    }
    let delay = if state.delay_all_responses.is_zero() {
        prompt
            .contains("[e2e:delay]")
            .then_some(CRASH_RECOVERY_DELAY)
    } else {
        Some(state.delay_all_responses)
    };
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await;
    }
    if state.scenario.as_deref() == Some("[e2e:always-unavailable]")
        || request["messages"].as_array().is_some_and(|messages| {
            messages.iter().any(|m| {
                m["role"] == "user"
                    && flatten_text(&m["content"])
                        .starts_with("Delegated task:\n[e2e:failure-child]")
            })
        })
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error":{"type":"controlled_unavailable","message":"controlled permanent outage"}
            })),
        )
            .into_response();
    }
    if prompt.contains("[e2e:retry=") && !has_tool_result(&request) {
        let mut retries = lock(&state.retry_attempts);
        let attempts = retries.entry(prompt).or_default();
        *attempts = attempts.saturating_add(1);
        if *attempts == 1 {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "type": "controlled_transient_failure",
                        "message": "retry this physical invocation"
                    }
                })),
            )
                .into_response();
        }
    }
    let response = response_for(&request, state.scenario.as_deref());
    let events = response
        .into_iter()
        .map(|data| Ok::<_, Infallible>(Event::default().data(data)));
    Sse::new(iter(events)).into_response()
}

#[path = "controlled_openai_chat_delegation.rs"]
mod delegation;
#[path = "controlled_openai_chat_schedules.rs"]
mod schedules;

fn response_for(request: &Value, scenario: Option<&str>) -> Vec<String> {
    if let Some(response) = schedules::response(request) {
        return response;
    }
    if let Some(response) = delegation::response(request) {
        return response;
    }
    let prompt = scenario
        .map(str::to_owned)
        .unwrap_or_else(|| latest_user_text(request));
    if has_tool_result(request) {
        return text_response(TOOL_RESULT_TEXT);
    }

    if prompt.contains("[e2e:parallel-search]") && offered_tool(request, "web_search") {
        return tool_response_many(
            (0..4)
                .map(|index| {
                    (
                        "web_search",
                        json!({
                            "query": format!("release-order-{index}"),
                            "count": 1,
                            "time_range": "past_day"
                        }),
                    )
                })
                .collect(),
        );
    }
    if prompt.contains("[e2e:parallel-time]") && offered_tool(request, "get_current_time") {
        return tool_response_many(
            (0..4)
                .map(|_| ("get_current_time", json!({"timezone": "Asia/Shanghai"})))
                .collect(),
        );
    }

    let scenario = if prompt.contains("[e2e:crash-read-only]") {
        Some((
            "web_search",
            json!({
                "query": "crash-read-only",
                "count": 1,
                "time_range": "past_day"
            }),
        ))
    } else if prompt.contains("[e2e:crash-idempotent]") {
        Some((
            "image_generation_tool",
            json!({"prompt": "crash-idempotent"}),
        ))
    } else if prompt.contains("[e2e:crash-query-applied]") {
        Some((
            "image_generation_tool",
            json!({"prompt": "crash-query-applied"}),
        ))
    } else if prompt.contains("[e2e:crash-query-not-applied]") {
        Some((
            "image_generation_tool",
            json!({"prompt": "crash-query-not-applied"}),
        ))
    } else if prompt.contains("[e2e:crash-query-unknown]") {
        Some((
            "image_generation_tool",
            json!({"prompt": "crash-query-unknown"}),
        ))
    } else if prompt.contains("[e2e:crash-not-replayable]") {
        Some((
            "video_generation_tool",
            json!({"prompt": "crash-not-replayable"}),
        ))
    } else if prompt.contains("[e2e:schedule-create]") {
        Some((
            "schedules_create",
            json!({
                "expected_revision":0,
                "action":{"Create":{
                    "definition":{
                        "title":"Provider-created schedule",
                        "instructions":"Read the current time.",
                        "timing":{"Interval":{"every_millis":86400000,"anchor":0}},
                        "timezone":"UTC","concurrency":"Skip","lateness":"CatchUp",
                        "lateness_window_millis":1000,
                        "retry":{"max_attempts":2,"base_backoff_millis":100,"factor":2,"max_backoff_millis":500}
                    },
                    "owner":"Cloud"
                }}
            }),
        ))
    } else if prompt.contains("[e2e:sandbox]") {
        Some(("data_python_run", json!({"code":"print(6 * 7)"})))
    } else if prompt.contains("[e2e:time]") {
        Some(("get_current_time", json!({"timezone": "Asia/Shanghai"})))
    } else if prompt.contains("[e2e:note]") {
        Some((
            "note_tool",
            json!({
                "title": "musteragent Rust Runtime E2E Note",
                "content": "由新的 Rust Agent Runtime 端到端创建。",
                "category": "e2e",
                "tags": ["rust-runtime", "e2e"]
            }),
        ))
    } else if prompt.contains("[e2e:tool-failure]") {
        Some((
            "web_search",
            json!({
                "query": "controlled-invalid-tool-failure",
                "count": 1,
                "time_range": "past_day"
            }),
        ))
    } else if prompt.contains("[e2e:interaction]") {
        Some((
            "interaction_request",
            json!({
                "kind": "single_select",
                "question": "请选择 Rust Runtime 验收项",
                "description": "此交互由新的 Rust Agent Runtime 发起。",
                "choices": ["pass", "review"],
                "allow_other": false,
                "required": true
            }),
        ))
    } else {
        None
    };

    match scenario {
        Some((tool_name, arguments)) if offered_tool(request, tool_name) => {
            tool_response(tool_name, arguments)
        }
        _ => text_response(SUCCESS_TEXT),
    }
}

fn has_tool_result(request: &Value) -> bool {
    request
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|messages| messages.last())
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str)
        == Some("tool")
}

fn offered_tool(request: &Value, expected: &str) -> bool {
    request
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| {
                tool.pointer("/function/name").and_then(Value::as_str) == Some(expected)
            })
        })
}

fn latest_user_text(request: &Value) -> String {
    request
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .rev()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        .and_then(|message| message.get("content"))
        .map(flatten_text)
        .unwrap_or_default()
}

fn flatten_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .or_else(|| part.as_str())
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn text_response(text: &str) -> Vec<String> {
    vec![
        json!({
            "id": "chatcmpl_musteragent_controlled",
            "choices": [{
                "index": 0,
                "delta": {"content": text},
                "finish_reason": null
            }]
        })
        .to_string(),
        json!({
            "id": "chatcmpl_musteragent_controlled",
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 32,
                "completion_tokens": 8,
                "total_tokens": 40
            }
        })
        .to_string(),
        "[DONE]".to_owned(),
    ]
}

fn tool_response(tool_name: &str, arguments: Value) -> Vec<String> {
    tool_response_many(vec![(tool_name, arguments)])
}

fn tool_response_many(calls: Vec<(&str, Value)>) -> Vec<String> {
    let tool_calls = calls
        .into_iter()
        .enumerate()
        .map(|(index, (tool_name, arguments))| {
            json!({
                "index": index,
                "id": format!("call_musteragent_controlled_{index}"),
                "type": "function",
                "function": {
                    "name": tool_name,
                    "arguments": arguments.to_string()
                }
            })
        })
        .collect::<Vec<_>>();
    vec![
        json!({
            "id": "chatcmpl_musteragent_controlled",
            "choices": [{
                "index": 0,
                "delta": {
                    "tool_calls": tool_calls
                },
                "finish_reason": null
            }]
        })
        .to_string(),
        json!({
            "id": "chatcmpl_musteragent_controlled",
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "tool_calls"
            }],
            "usage": {
                "prompt_tokens": 32,
                "completion_tokens": 12,
                "total_tokens": 44
            }
        })
        .to_string(),
        "[DONE]".to_owned(),
    ]
}

#[derive(Debug, Error)]
pub enum ControlledProviderError {
    #[error("controlled Provider I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("controlled Provider task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("controlled Provider was not started in paused mode")]
    NotPaused,
    #[error(
        "controlled Provider barrier timed out waiting for {expected} requests; observed {observed}"
    )]
    BarrierTimeout { expected: usize, observed: usize },
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
