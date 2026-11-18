use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufReader, Read as _};
use std::time::Duration;

use musteragent_telemetry::machine::{MachineTelemetry, MachineTelemetryBackend};
use musteragent_telemetry::{
    AttemptTelemetry, CommandTelemetry, DataEdgeTelemetry, DbCommitTelemetry, InvocationTelemetry,
    MetricOperation, RuntimeTelemetry, TelemetryOutcome, TelemetrySignal,
};
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::bounded_log::BoundedLogSink;
use crate::bounded_trace::BoundedTraceProcessor;
use crate::events::OtlpMachineTelemetry;
use crate::logging;
use crate::metrics::HealthMetricExporter;
use crate::resource::TelemetryResourceIdentity;
use crate::{
    OtlpMode, RuntimeMetrics, TelemetryConfig, TelemetryDeploymentSnapshot, TelemetryHealth,
};

pub struct TelemetryRuntime {
    health: TelemetryHealth,
    metrics: RuntimeMetrics,
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    log_sink: BoundedLogSink,
    shutdown_timeout: Duration,
    deployment: TelemetryDeploymentSnapshot,
    machine: MachineTelemetry,
}

impl TelemetryRuntime {
    pub fn install(config: TelemetryConfig) -> Result<Self, TelemetryInstallError> {
        // The release binary links rustls with both aws-lc-rs and ring in the
        // dependency graph, so rustls cannot auto-select a process-level
        // CryptoProvider. Install ring explicitly before any TLS client is
        // built; ignore the error when another component already installed one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let deployment = config.deployment_snapshot(current_executable_sha256()?);
        let health = TelemetryHealth::default();
        let enabled = config.mode != OtlpMode::Disabled;
        health.configure(TelemetrySignal::Trace, enabled, config.readiness_required());
        health.configure(
            TelemetrySignal::Metric,
            enabled,
            config.readiness_required(),
        );
        health.configure(TelemetrySignal::Log, true, config.readiness_required());
        global::set_text_map_propagator(TraceContextPropagator::new());
        let resource_identity = TelemetryResourceIdentity::from_config(&config);
        let resource = Resource::builder_empty()
            .with_attributes(resource_identity.otel_attributes())
            .build();

        let (tracer_provider, meter_provider) = if let Some(endpoint) = config.endpoint.as_deref() {
            let span_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(config.export_timeout)
                .build()?;
            let processor = BoundedTraceProcessor::new(
                span_exporter,
                tokio::runtime::Handle::current(),
                config.queue_capacity.get(),
                config.batch_size.get(),
                config.export_interval,
                config.shutdown_timeout,
                health.clone(),
            )?;
            let tracer_provider = SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(processor)
                .build();

            let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(config.export_timeout)
                .build()?;
            let reader =
                PeriodicReader::builder(HealthMetricExporter::new(metric_exporter, health.clone()))
                    .with_interval(config.export_interval)
                    .build();
            let meter_provider = SdkMeterProvider::builder()
                .with_resource(resource)
                .with_reader(reader)
                .build();
            (Some(tracer_provider), Some(meter_provider))
        } else {
            (None, None)
        };

        let meter = meter_provider.as_ref().map_or_else(
            || global::meter("musteragent-runtime"),
            |provider| {
                use opentelemetry::metrics::MeterProvider as _;
                provider.meter("musteragent-runtime")
            },
        );
        let metrics = RuntimeMetrics::new(meter);
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info,kernel_agent=info".into());
        let (log_writer, log_sink) = BoundedLogSink::new(
            config.log_queue_capacity.get(),
            config.shutdown_timeout,
            health.clone(),
        )?;
        logging::diagnostic_subscriber(resource_identity.clone(), log_writer.clone(), env_filter)
            .try_init()?;
        let machine_backend: std::sync::Arc<dyn MachineTelemetryBackend> =
            OtlpMachineTelemetry::new(
                tracer_provider
                    .as_ref()
                    .map(|provider| provider.tracer("musteragent-runtime")),
                log_writer,
                metrics.clone(),
                resource_identity,
            );
        let machine = MachineTelemetry::from_backend(machine_backend);
        global::set_meter_provider(
            meter_provider
                .clone()
                .unwrap_or_else(SdkMeterProvider::default),
        );

        let runtime = Self {
            health,
            metrics,
            tracer_provider,
            meter_provider,
            log_sink,
            shutdown_timeout: config.shutdown_timeout,
            deployment,
            machine,
        };
        emit_bootstrap_span(&runtime.machine);
        runtime.machine.runtime().observe_started();
        runtime.metrics.record_operation(
            MetricOperation::Runtime,
            TelemetryOutcome::Success,
            Duration::ZERO,
        );
        if let Some(provider) = &runtime.tracer_provider {
            let _ = provider.force_flush();
        }
        if let Some(provider) = &runtime.meter_provider {
            let _ = provider.force_flush();
        }
        let _ = runtime.log_sink.readiness_handshake();
        Ok(runtime)
    }

    #[must_use]
    pub fn health(&self) -> TelemetryHealth {
        self.health.clone()
    }

    #[must_use]
    pub fn metrics(&self) -> RuntimeMetrics {
        self.metrics.clone()
    }

    #[must_use]
    pub fn command_telemetry(&self) -> CommandTelemetry {
        self.machine.command()
    }

    #[must_use]
    pub fn attempt_telemetry(&self) -> AttemptTelemetry {
        self.machine.attempt()
    }

    #[must_use]
    pub fn invocation_telemetry(&self) -> InvocationTelemetry {
        self.machine.invocation()
    }

    #[must_use]
    pub fn data_edge_telemetry(&self) -> DataEdgeTelemetry {
        self.machine.data_edge()
    }

    #[must_use]
    pub fn db_commit_telemetry(&self) -> DbCommitTelemetry {
        self.machine.db_commit()
    }

    #[must_use]
    pub fn runtime_telemetry(&self) -> RuntimeTelemetry {
        self.machine.runtime()
    }

    #[must_use]
    pub fn deployment(&self) -> TelemetryDeploymentSnapshot {
        self.deployment.clone()
    }

    pub fn shutdown(&self) {
        if let Some(provider) = &self.meter_provider {
            let _ = provider.shutdown_with_timeout(self.shutdown_timeout);
        }
        if let Some(provider) = &self.tracer_provider {
            let _ = provider.shutdown_with_timeout(self.shutdown_timeout);
        }
        let _ = self.log_sink.shutdown();
    }
}

fn current_executable_sha256() -> Result<String, TelemetryInstallError> {
    let executable = std::env::current_exe().map_err(TelemetryInstallError::ArtifactIdentity)?;
    let file = File::open(executable).map_err(TelemetryInstallError::ArtifactIdentity)?;
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(TelemetryInstallError::ArtifactIdentity)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn emit_bootstrap_span(machine: &MachineTelemetry) {
    let bootstrap = machine.runtime().span();
    bootstrap.record_outcome(TelemetryOutcome::Success);
    drop(bootstrap);
}

impl Drop for TelemetryRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Error)]
pub enum TelemetryInstallError {
    #[error("OTLP exporter configuration is invalid")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
    #[error("bounded telemetry worker could not be started")]
    ExportThread(#[from] std::io::Error),
    #[error("global tracing subscriber was already installed")]
    Subscriber(#[from] tracing_subscriber::util::TryInitError),
    #[error("running executable identity could not be measured")]
    ArtifactIdentity(std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use opentelemetry_sdk::trace::{SpanData, SpanExporter};

    use super::*;
    use crate::bounded_trace::BoundedTraceProcessor;

    #[derive(Debug, Clone)]
    struct FakeCollector {
        fail: bool,
    }

    impl SpanExporter for FakeCollector {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            if self.fail {
                Err(OTelSdkError::InternalFailure(
                    "collector unavailable".to_owned(),
                ))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, value: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("capture lock")
                .extend_from_slice(value);
            Ok(value.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn protected_startup_is_ready_with_application_log_filter_off() {
        let (_runtime, provider, sink, health, capture) = startup_fixture(false);
        provider.force_flush().expect("collector handshake");
        sink.readiness_handshake().expect("log sink handshake");

        assert!(health.ready());
        let output = String::from_utf8(capture.0.lock().expect("capture lock").clone())
            .expect("safe log output");
        assert!(output.contains("\"telemetry_internal\":\"log_sink_readiness\""));
        assert!(!output.contains("\"event.key\""));
        assert!(!output.contains("runtime.bootstrap"));
        provider.shutdown().expect("trace shutdown");
        sink.shutdown().expect("log shutdown");
    }

    #[test]
    fn protected_startup_stays_red_when_collector_is_down_even_with_log_filter_off() {
        let (_runtime, provider, sink, health, _capture) = startup_fixture(true);
        assert!(provider.force_flush().is_err());
        sink.readiness_handshake().expect("log sink handshake");

        let snapshot = health.snapshot();
        assert!(!snapshot.ready);
        assert_eq!(snapshot.trace.export_failures, 1);
        assert!(snapshot.log.healthy);
        let _ = provider.shutdown();
        sink.shutdown().expect("log shutdown");
    }

    fn startup_fixture(
        collector_fails: bool,
    ) -> (
        tokio::runtime::Runtime,
        SdkTracerProvider,
        BoundedLogSink,
        TelemetryHealth,
        CaptureWriter,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .expect("test runtime");
        let health = TelemetryHealth::default();
        health.configure(TelemetrySignal::Trace, true, true);
        health.configure(TelemetrySignal::Metric, false, false);
        health.configure(TelemetrySignal::Log, true, true);
        let processor = BoundedTraceProcessor::new(
            FakeCollector {
                fail: collector_fails,
            },
            runtime.handle().clone(),
            8,
            4,
            Duration::from_secs(60),
            Duration::from_secs(1),
            health.clone(),
        )
        .expect("trace processor");
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .build();
        let capture = CaptureWriter::default();
        let (log_writer, sink) = BoundedLogSink::new_with_writer(
            8,
            Duration::from_secs(1),
            health.clone(),
            capture.clone(),
        )
        .expect("log sink");
        let metrics = RuntimeMetrics::new(opentelemetry::global::meter("startup-test"));
        let backend: std::sync::Arc<dyn MachineTelemetryBackend> = OtlpMachineTelemetry::new(
            Some(provider.tracer("startup-test")),
            log_writer,
            metrics,
            TelemetryResourceIdentity::new(
                "test".to_owned(),
                "1".to_owned(),
                "test".to_owned(),
                "test-cluster".to_owned(),
                "test-deployment".to_owned(),
                "test-build".to_owned(),
            ),
        );
        let machine = MachineTelemetry::from_backend(backend);
        emit_bootstrap_span(&machine);
        (runtime, provider, sink, health, capture)
    }
}
