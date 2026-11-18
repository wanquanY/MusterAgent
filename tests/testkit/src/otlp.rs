use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{Metric, metric};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct CapturedSpan {
    pub service_name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub name: String,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct CapturedMetric {
    pub service_name: String,
    pub name: String,
    pub attribute_sets: Vec<BTreeMap<String, String>>,
}

#[derive(Clone, Default)]
struct CollectorState {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
    metrics: Arc<Mutex<Vec<CapturedMetric>>>,
}

pub struct LocalOtlpCollector {
    address: SocketAddr,
    state: CollectorState,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl LocalOtlpCollector {
    pub async fn start() -> Result<Self, io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let state = CollectorState::default();
        let trace = TraceReceiver {
            state: state.clone(),
        };
        let metrics = MetricsReceiver {
            state: state.clone(),
        };
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(trace))
                .add_service(MetricsServiceServer::new(metrics))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Ok(Self {
            address,
            state,
            shutdown: Some(shutdown),
            task,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    #[must_use]
    pub fn spans(&self) -> Vec<CapturedSpan> {
        lock(&self.state.spans).clone()
    }

    #[must_use]
    pub fn metrics(&self) -> Vec<CapturedMetric> {
        lock(&self.state.metrics).clone()
    }

    pub async fn wait_for_span(
        &self,
        predicate: impl Fn(&CapturedSpan) -> bool,
        timeout: Duration,
    ) -> Vec<CapturedSpan> {
        let deadline = Instant::now() + timeout;
        loop {
            let spans = self.spans();
            if spans.iter().any(&predicate) {
                return spans;
            }
            assert!(Instant::now() < deadline, "OTLP span capture timed out");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .await
            .expect("join local OTLP collector")
            .expect("local OTLP collector shutdown");
    }
}

struct TraceReceiver {
    state: CollectorState,
}

#[tonic::async_trait]
impl TraceService for TraceReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let mut captured = Vec::new();
        for resource_spans in request.into_inner().resource_spans {
            let resource = attributes(
                resource_spans
                    .resource
                    .as_ref()
                    .map_or(&[][..], |resource| resource.attributes.as_slice()),
            );
            let service_name = resource.get("service.name").cloned().unwrap_or_default();
            for scope in resource_spans.scope_spans {
                for span in scope.spans {
                    captured.push(CapturedSpan {
                        service_name: service_name.clone(),
                        trace_id: hex(&span.trace_id),
                        span_id: hex(&span.span_id),
                        parent_span_id: hex(&span.parent_span_id),
                        name: span.name,
                        attributes: attributes(&span.attributes),
                    });
                }
            }
        }
        lock(&self.state.spans).extend(captured);
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

struct MetricsReceiver {
    state: CollectorState,
}

#[tonic::async_trait]
impl MetricsService for MetricsReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut captured = Vec::new();
        for resource_metrics in request.into_inner().resource_metrics {
            let resource = attributes(
                resource_metrics
                    .resource
                    .as_ref()
                    .map_or(&[][..], |resource| resource.attributes.as_slice()),
            );
            let service_name = resource.get("service.name").cloned().unwrap_or_default();
            for scope in resource_metrics.scope_metrics {
                captured.extend(
                    scope
                        .metrics
                        .into_iter()
                        .map(|metric: Metric| CapturedMetric {
                            service_name: service_name.clone(),
                            attribute_sets: metric_attribute_sets(&metric),
                            name: metric.name,
                        }),
                );
            }
        }
        lock(&self.state.metrics).extend(captured);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

fn metric_attribute_sets(metric: &Metric) -> Vec<BTreeMap<String, String>> {
    match metric.data.as_ref() {
        Some(metric::Data::Gauge(value)) => value
            .data_points
            .iter()
            .map(|point| attributes(&point.attributes))
            .collect(),
        Some(metric::Data::Sum(value)) => value
            .data_points
            .iter()
            .map(|point| attributes(&point.attributes))
            .collect(),
        Some(metric::Data::Histogram(value)) => value
            .data_points
            .iter()
            .map(|point| attributes(&point.attributes))
            .collect(),
        Some(metric::Data::ExponentialHistogram(value)) => value
            .data_points
            .iter()
            .map(|point| attributes(&point.attributes))
            .collect(),
        Some(metric::Data::Summary(value)) => value
            .data_points
            .iter()
            .map(|point| attributes(&point.attributes))
            .collect(),
        None => Vec::new(),
    }
}

fn attributes(values: &[KeyValue]) -> BTreeMap<String, String> {
    values
        .iter()
        .filter_map(|value| {
            value
                .value
                .as_ref()
                .map(|inner| (value.key.clone(), any_value(inner)))
        })
        .collect()
}

fn any_value(value: &AnyValue) -> String {
    match value.value.as_ref() {
        Some(any_value::Value::StringValue(value)) => value.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::ArrayValue(value)) => value
            .values
            .iter()
            .map(any_value)
            .collect::<Vec<_>>()
            .join(","),
        Some(any_value::Value::KvlistValue(value)) => attributes(&value.values)
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(","),
        Some(any_value::Value::BytesValue(value)) => hex(value),
        Some(any_value::Value::StringValueStrindex(value)) => value.to_string(),
        None => String::new(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
