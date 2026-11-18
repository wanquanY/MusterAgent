use std::sync::Arc;
use std::time::{Duration, SystemTime};

use musteragent_telemetry::machine::{
    MachineEventIdentity, MachineTelemetryBackend, RuntimeSpanDescriptor, TypedMachineEvent,
};
use musteragent_telemetry::{
    DataEdgeOperation, DbCommitKind, DbTransactionPhase, DbTransactionResult, RuntimeSpan,
    TELEMETRY_SCHEMA_VERSION, TelemetryEventKind, TelemetryOutcome,
};
use opentelemetry_sdk::trace::SdkTracer;
use serde_json::{Map, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::bounded_log::BoundedLogMakeWriter;
use crate::metrics::RuntimeMetrics;
use crate::propagation::{build_runtime_span, current_trace_ids};
use crate::resource::TelemetryResourceIdentity;

pub(crate) struct OtlpMachineTelemetry {
    tracer: Option<SdkTracer>,
    logs: BoundedLogMakeWriter,
    metrics: RuntimeMetrics,
    resource: TelemetryResourceIdentity,
}

impl OtlpMachineTelemetry {
    pub(crate) fn new(
        tracer: Option<SdkTracer>,
        logs: BoundedLogMakeWriter,
        metrics: RuntimeMetrics,
        resource: TelemetryResourceIdentity,
    ) -> Arc<Self> {
        Arc::new(Self {
            tracer,
            logs,
            metrics,
            resource,
        })
    }
}

impl MachineTelemetryBackend for OtlpMachineTelemetry {
    fn start_span(&self, descriptor: RuntimeSpanDescriptor) -> RuntimeSpan {
        self.tracer
            .as_ref()
            .map_or_else(RuntimeSpan::none, |tracer| {
                build_runtime_span(tracer, descriptor)
            })
    }

    fn emit(&self, event: TypedMachineEvent) {
        if let Ok(mut bytes) = serialize_event(&self.resource, &event) {
            bytes.push(b'\n');
            self.logs.emit_line(bytes);
        }
    }

    fn record_data_edge(
        &self,
        operation: DataEdgeOperation,
        outcome: TelemetryOutcome,
        duration: Duration,
    ) {
        self.metrics.record_data_edge(operation, outcome, duration);
    }

    fn record_db_commit(&self, kind: DbCommitKind, outcome: TelemetryOutcome, duration: Duration) {
        self.metrics.record_db_commit(kind, outcome, duration);
    }

    fn record_db_transaction(
        &self,
        kind: DbCommitKind,
        phase: DbTransactionPhase,
        result: DbTransactionResult,
        duration: Duration,
    ) {
        self.metrics
            .record_db_transaction(kind, phase, result, duration);
    }
}

fn serialize_event(
    resource: &TelemetryResourceIdentity,
    event: &TypedMachineEvent,
) -> Result<Vec<u8>, serde_json::Error> {
    let timestamp = OffsetDateTime::from(SystemTime::now())
        .format(&Rfc3339)
        .unwrap_or_else(|_| "invalid-time".to_owned());
    let mut envelope = Map::new();
    envelope.insert("ts".to_owned(), Value::String(timestamp));
    envelope.insert(
        "level".to_owned(),
        Value::String(event_log_level(event.kind(), event.outcome()).to_owned()),
    );
    resource.insert_log_fields(&mut envelope);
    envelope.insert(
        "telemetry_schema".to_owned(),
        Value::String(TELEMETRY_SCHEMA_VERSION.to_owned()),
    );
    envelope.insert(
        "event.key".to_owned(),
        Value::String(event.kind().as_str().to_owned()),
    );
    envelope.insert(
        "operation".to_owned(),
        Value::String(event.operation().as_str().to_owned()),
    );
    envelope.insert(
        "outcome.class".to_owned(),
        Value::String(event.outcome().as_str().to_owned()),
    );
    if let Some(code) = event.error_code() {
        envelope.insert(
            "error.code".to_owned(),
            Value::String(code.as_str().to_owned()),
        );
    }
    if let Some(class) = event.error_class() {
        envelope.insert(
            "error.class".to_owned(),
            Value::String(class.as_str().to_owned()),
        );
    }
    if let Some(observation_id) = event.observation_id() {
        insert_uuid(&mut envelope, "observation_id", observation_id);
    }
    if let Some((trace_id, span_id)) = current_trace_ids() {
        envelope.insert("trace_id".to_owned(), Value::String(trace_id));
        envelope.insert("span_id".to_owned(), Value::String(span_id));
    }
    insert_identity(&mut envelope, event.identity());
    serde_json::to_vec(&envelope)
}

const fn event_log_level(kind: TelemetryEventKind, outcome: TelemetryOutcome) -> &'static str {
    if matches!(kind, TelemetryEventKind::DataEdgeCompleted)
        && matches!(outcome, TelemetryOutcome::Error)
    {
        "warn"
    } else {
        "info"
    }
}

fn insert_identity(envelope: &mut Map<String, Value>, identity: &MachineEventIdentity) {
    match identity {
        MachineEventIdentity::Runtime => {}
        MachineEventIdentity::Command(context) => {
            insert_uuid(envelope, "tenant_id", context.tenant_id);
            if let Some(conversation_id) = context.conversation_id {
                insert_uuid(envelope, "conversation_id", conversation_id);
            }
            insert_uuid(envelope, "command_id", context.command_id);
            insert_uuid(envelope, "correlation_id", context.correlation_id);
        }
        MachineEventIdentity::Attempt(context) => insert_attempt(envelope, context),
        MachineEventIdentity::Invocation(context) => {
            insert_attempt(envelope, &context.attempt);
            insert_uuid(envelope, "invocation_id", context.invocation_id);
        }
        MachineEventIdentity::DataEdge(context) => {
            insert_uuid(envelope, "tenant_id", context.tenant_id);
            insert_uuid(envelope, "conversation_id", context.conversation_id);
            insert_uuid(envelope, "correlation_id", context.correlation_id);
        }
        MachineEventIdentity::DbCommit(context) => {
            insert_uuid(envelope, "tenant_id", context.tenant_id);
            insert_uuid(envelope, "conversation_id", context.conversation_id);
            insert_uuid(envelope, "correlation_id", context.correlation_id);
            envelope.insert(
                "commit.kind".to_owned(),
                Value::String(context.kind.as_str().to_owned()),
            );
        }
    }
}

fn insert_attempt(
    envelope: &mut Map<String, Value>,
    context: &musteragent_telemetry::AttemptTelemetryContext,
) {
    insert_uuid(envelope, "tenant_id", context.tenant_id);
    insert_uuid(envelope, "conversation_id", context.conversation_id);
    insert_uuid(envelope, "activity_id", context.activity_id);
    insert_uuid(envelope, "run_id", context.run_id);
    insert_uuid(envelope, "attempt_id", context.attempt_id);
    insert_uuid(envelope, "correlation_id", context.correlation_id);
    envelope.insert(
        "attempt_number".to_owned(),
        Value::Number(u64::from(context.attempt_number.get()).into()),
    );
    envelope.insert(
        "fencing_token".to_owned(),
        Value::Number(context.fencing_token.get().into()),
    );
}

fn insert_uuid<T: std::fmt::Display>(envelope: &mut Map<String, Value>, key: &str, value: T) {
    envelope.insert(key.to_owned(), Value::String(value.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_edge_failures_are_warning_logs_without_reclassifying_other_events() {
        assert_eq!(
            event_log_level(
                TelemetryEventKind::DataEdgeCompleted,
                TelemetryOutcome::Error,
            ),
            "warn"
        );
        assert_eq!(
            event_log_level(
                TelemetryEventKind::CommandCompleted,
                TelemetryOutcome::Success,
            ),
            "info"
        );
    }
}
