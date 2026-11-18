use std::convert::Infallible;
use std::future::pending;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProviderWireScenario {
    pub status: u16,
    pub content_type: String,
    pub chunks: Vec<Vec<u8>>,
    pub hang_after_chunks: bool,
}

impl ProviderWireScenario {
    #[must_use]
    pub fn successful(content_type: impl Into<String>, chunks: Vec<Vec<u8>>) -> Self {
        Self {
            status: StatusCode::OK.as_u16(),
            content_type: content_type.into(),
            chunks,
            hang_after_chunks: false,
        }
    }

    #[must_use]
    pub fn hanging(content_type: impl Into<String>, chunks: Vec<Vec<u8>>) -> Self {
        Self {
            status: StatusCode::OK.as_u16(),
            content_type: content_type.into(),
            chunks,
            hang_after_chunks: true,
        }
    }
}

type CapturedRequests = Arc<Mutex<Vec<(std::time::Instant, Vec<u8>)>>>;

#[derive(Debug)]
pub struct MockProviderWire {
    endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
    closed_streams: Arc<AtomicUsize>,
    captured_requests: CapturedRequests,
}

impl MockProviderWire {
    pub async fn start(scenario: ProviderWireScenario) -> Result<Self, ProviderWireServerError> {
        Self::start_with_retry_after(scenario, None).await
    }

    /// Accept an actual request prefix, then close the connection without
    /// sending any status line or response headers.
    pub async fn start_closing_before_headers() -> Result<Self, ProviderWireServerError> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let (shutdown, mut stopped) = oneshot::channel();
        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::clone(&captured_requests);
        let task = tokio::spawn(async move {
            loop {
                let socket = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted?.0,
                };
                let mut prefix = [0u8; 128];
                loop {
                    tokio::select! {
                        _ = &mut stopped => return Ok(()),
                        ready = socket.readable() => ready?,
                    }
                    match socket.try_read(&mut prefix) {
                        Ok(0) => break,
                        Ok(n) => {
                            requests
                                .lock()
                                .await
                                .push((std::time::Instant::now(), prefix[..n].to_vec()));
                            break;
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            endpoint: format!("http://{address}/invoke"),
            shutdown: Some(shutdown),
            task,
            closed_streams: Arc::new(AtomicUsize::new(0)),
            captured_requests,
        })
    }

    pub async fn start_with_retry_after(
        scenario: ProviderWireScenario,
        retry_after_seconds: Option<u32>,
    ) -> Result<Self, ProviderWireServerError> {
        StatusCode::from_u16(scenario.status)
            .map_err(|_| ProviderWireServerError::InvalidStatus(scenario.status))?;
        HeaderValue::from_str(&scenario.content_type)
            .map_err(|_| ProviderWireServerError::InvalidContentType)?;
        let closed_streams = Arc::new(AtomicUsize::new(0));
        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(ProviderWireState {
            scenario,
            retry_after_seconds,
            closed_streams: Arc::clone(&closed_streams),
            captured_requests: Arc::clone(&captured_requests),
        });
        let app = Router::new()
            .route("/invoke", post(stream_response))
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
            endpoint: format!("http://{address}/invoke"),
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
        self.captured_requests
            .lock()
            .await
            .iter()
            .map(|(_, bytes)| bytes.clone())
            .collect()
    }

    pub async fn received_at(&self) -> Vec<std::time::Instant> {
        self.captured_requests
            .lock()
            .await
            .iter()
            .map(|(at, _)| *at)
            .collect()
    }

    pub async fn shutdown(mut self) -> Result<(), ProviderWireServerError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await??;
        Ok(())
    }
}

#[derive(Debug)]
struct ProviderWireState {
    scenario: ProviderWireScenario,
    retry_after_seconds: Option<u32>,
    closed_streams: Arc<AtomicUsize>,
    captured_requests: CapturedRequests,
}

struct StreamDropEvidence(Arc<AtomicUsize>);

impl Drop for StreamDropEvidence {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn stream_response(
    State(state): State<Arc<ProviderWireState>>,
    request: axum::body::Bytes,
) -> Response {
    state
        .captured_requests
        .lock()
        .await
        .push((std::time::Instant::now(), request.to_vec()));
    let status =
        StatusCode::from_u16(state.scenario.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let content_type = HeaderValue::from_str(&state.scenario.content_type)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    let chunks = state.scenario.chunks.clone();
    let hang_after_chunks = state.scenario.hang_after_chunks;
    let closed_streams = Arc::clone(&state.closed_streams);
    let body = Body::from_stream(async_stream::stream! {
        let _drop_evidence = StreamDropEvidence(closed_streams);
        for chunk in chunks {
            yield Ok::<_, Infallible>(chunk);
            tokio::task::yield_now().await;
        }
        if hang_after_chunks {
            pending::<()>().await;
        }
    });
    let mut response = (status, [(header::CONTENT_TYPE, content_type)], body).into_response();
    if let Some(seconds) = state.retry_after_seconds {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
    }
    response
}

#[derive(Debug, Error)]
pub enum ProviderWireServerError {
    #[error("invalid HTTP status for Provider wire scenario: {0}")]
    InvalidStatus(u16),
    #[error("invalid Provider wire content type")]
    InvalidContentType,
    #[error("Provider wire server I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("Provider wire server task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
