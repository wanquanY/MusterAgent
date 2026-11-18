use std::convert::Infallible;
use std::future::pending;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::iter;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProviderSseFrame {
    pub event: String,
    pub data: String,
}

impl ProviderSseFrame {
    #[must_use]
    pub fn new(event: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            data: data.into(),
        }
    }
}

/// Provider response script. `disconnect_after` models an early EOF.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProviderSseScenario {
    pub status: u16,
    pub frames: Vec<ProviderSseFrame>,
    pub disconnect_after: Option<usize>,
    pub error_body: Option<String>,
}

impl ProviderSseScenario {
    #[must_use]
    pub fn successful(frames: Vec<ProviderSseFrame>) -> Self {
        Self {
            status: StatusCode::OK.as_u16(),
            frames,
            disconnect_after: None,
            error_body: None,
        }
    }

    #[must_use]
    pub fn failure(status: u16, error_body: impl Into<String>) -> Self {
        Self {
            status,
            frames: Vec::new(),
            disconnect_after: None,
            error_body: Some(error_body.into()),
        }
    }
}

#[derive(Debug)]
pub struct MockProviderSse {
    endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
    closed_streams: Arc<AtomicUsize>,
    captured_requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MockProviderSse {
    pub async fn start(scenario: ProviderSseScenario) -> Result<Self, ProviderSseServerError> {
        Self::start_internal(scenario, false, Duration::ZERO, None).await
    }

    pub async fn start_hanging(
        scenario: ProviderSseScenario,
    ) -> Result<Self, ProviderSseServerError> {
        Self::start_internal(scenario, true, Duration::ZERO, None).await
    }

    pub async fn start_delayed(
        scenario: ProviderSseScenario,
        first_frame_delay: Duration,
    ) -> Result<Self, ProviderSseServerError> {
        Self::start_internal(scenario, false, first_frame_delay, None).await
    }

    pub async fn start_delayed_with_heartbeats(
        scenario: ProviderSseScenario,
        first_frame_delay: Duration,
        heartbeat_interval: Duration,
    ) -> Result<Self, ProviderSseServerError> {
        Self::start_internal(scenario, false, first_frame_delay, Some(heartbeat_interval)).await
    }

    async fn start_internal(
        scenario: ProviderSseScenario,
        hang_after_frames: bool,
        first_frame_delay: Duration,
        heartbeat_interval: Option<Duration>,
    ) -> Result<Self, ProviderSseServerError> {
        StatusCode::from_u16(scenario.status)
            .map_err(|_| ProviderSseServerError::InvalidStatus(scenario.status))?;
        let closed_streams = Arc::new(AtomicUsize::new(0));
        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(ProviderSseState {
            scenario,
            hang_after_frames,
            first_frame_delay,
            heartbeat_interval,
            closed_streams: Arc::clone(&closed_streams),
            captured_requests: Arc::clone(&captured_requests),
        });
        let app = Router::new()
            .route("/v1/responses", post(stream_response))
            .with_state(state);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = receiver.await;
                })
                .await
        });
        Ok(Self {
            endpoint: format!("http://{address}/v1/responses"),
            shutdown: Some(shutdown),
            task,
            closed_streams,
            captured_requests,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn closed_stream_count(&self) -> usize {
        self.closed_streams.load(Ordering::SeqCst)
    }

    pub async fn captured_requests(&self) -> Vec<Vec<u8>> {
        self.captured_requests.lock().await.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ProviderSseServerError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await??;
        Ok(())
    }
}

#[derive(Debug)]
struct ProviderSseState {
    scenario: ProviderSseScenario,
    hang_after_frames: bool,
    first_frame_delay: Duration,
    heartbeat_interval: Option<Duration>,
    closed_streams: Arc<AtomicUsize>,
    captured_requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

struct StreamDropEvidence(Arc<AtomicUsize>);

impl Drop for StreamDropEvidence {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn stream_response(
    State(state): State<Arc<ProviderSseState>>,
    body: axum::body::Bytes,
) -> Response {
    state.captured_requests.lock().await.push(body.to_vec());
    let scenario = &state.scenario;
    let status = StatusCode::from_u16(scenario.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    if status != StatusCode::OK {
        return (
            status,
            [("content-type", "application/json")],
            scenario.error_body.clone().unwrap_or_default(),
        )
            .into_response();
    }
    let count = scenario
        .disconnect_after
        .unwrap_or(scenario.frames.len())
        .min(scenario.frames.len());
    let frames = scenario
        .frames
        .iter()
        .take(count)
        .cloned()
        .collect::<Vec<_>>();
    if !state.hang_after_frames && state.first_frame_delay.is_zero() {
        let events = frames
            .into_iter()
            .map(|frame| Ok::<_, Infallible>(Event::default().event(frame.event).data(frame.data)));
        return Sse::new(iter(events)).into_response();
    }
    let closed_streams = Arc::clone(&state.closed_streams);
    let first_frame_delay = state.first_frame_delay;
    let hang_after_frames = state.hang_after_frames;
    let events = async_stream::stream! {
        let _drop_evidence = StreamDropEvidence(closed_streams);
        if !first_frame_delay.is_zero() {
            tokio::time::sleep(first_frame_delay).await;
        }
        for frame in frames {
            yield Ok::<_, Infallible>(
                Event::default().event(frame.event).data(frame.data)
            );
        }
        if hang_after_frames {
            pending::<()>().await;
        }
    };
    let response = Sse::new(events);
    if let Some(interval) = state.heartbeat_interval {
        response
            .keep_alive(KeepAlive::new().interval(interval))
            .into_response()
    } else {
        response.into_response()
    }
}

#[derive(Debug, Error)]
pub enum ProviderSseServerError {
    #[error("invalid HTTP status for Provider scenario: {0}")]
    InvalidStatus(u16),
    #[error("Provider mock server I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("Provider mock server task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
