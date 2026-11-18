use std::sync::Arc;

use musteragent_runtime_contract::TraceContext;
use musteragent_telemetry::machine::RuntimeSpanDescriptor;
use musteragent_telemetry::{
    AttemptTelemetryContext, ControlledBaggage, ErrorClass, RuntimeSpan, RuntimeSpanBackend,
    TelemetryOutcome, WaitReasonClass,
};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::{SpanKind, Status, TraceContextExt as _, Tracer as _};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::SdkTracer;

struct CanonicalTraceCarrier<'a> {
    context: &'a TraceContext,
}

impl Extractor for CanonicalTraceCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => Some(self.context.traceparent.as_str()),
            "tracestate" => self.context.tracestate.as_ref().map(|value| value.as_str()),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        if self.context.tracestate.is_some() {
            vec!["traceparent", "tracestate"]
        } else {
            vec!["traceparent"]
        }
    }
}

fn remote_parent(context: &TraceContext) -> Context {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&CanonicalTraceCarrier { context })
    })
}

fn current_or_remote(context: &TraceContext) -> Context {
    let current = Context::current();
    if current.span().span_context().is_valid() {
        current
    } else {
        remote_parent(context)
    }
}

pub fn attach_controlled_baggage(span: &RuntimeSpan, baggage: &ControlledBaggage) {
    span.record_baggage(baggage);
}

#[must_use]
pub fn current_trace_ids() -> Option<(String, String)> {
    let context = Context::current();
    let span = context.span();
    let span_context = span.span_context();
    span_context.is_valid().then(|| {
        (
            span_context.trace_id().to_string(),
            span_context.span_id().to_string(),
        )
    })
}

pub(crate) fn build_runtime_span(
    tracer: &SdkTracer,
    descriptor: RuntimeSpanDescriptor,
) -> RuntimeSpan {
    let (name, parent, attributes) = descriptor_parts(descriptor);
    let span = tracer
        .span_builder(name)
        .with_kind(SpanKind::Internal)
        .with_attributes(attributes)
        .start_with_context(tracer, &parent);
    RuntimeSpan::from_backend(Arc::new(OtlpRuntimeSpanBackend {
        context: parent.with_span(span),
    }))
}

fn descriptor_parts(descriptor: RuntimeSpanDescriptor) -> (&'static str, Context, Vec<KeyValue>) {
    match descriptor {
        RuntimeSpanDescriptor::RuntimeBootstrap => (
            "runtime.bootstrap",
            Context::current(),
            vec![KeyValue::new("operation", "runtime.bootstrap")],
        ),
        RuntimeSpanDescriptor::Command { operation, context } => {
            let mut attributes = vec![
                KeyValue::new("operation", operation.as_str()),
                KeyValue::new("tenant_id", context.tenant_id.to_string()),
                KeyValue::new("command_id", context.command_id.to_string()),
                KeyValue::new("correlation_id", context.correlation_id.to_string()),
            ];
            if let Some(conversation_id) = context.conversation_id {
                attributes.push(KeyValue::new(
                    "conversation_id",
                    conversation_id.to_string(),
                ));
            }
            (
                "runtime.command",
                remote_parent(&context.trace_context),
                attributes,
            )
        }
        RuntimeSpanDescriptor::Attempt(context) => (
            "runtime.attempt",
            remote_parent(&context.trace_context),
            attempt_attributes(&context),
        ),
        RuntimeSpanDescriptor::Invocation { operation, context } => {
            let mut attributes = attempt_attributes(&context.attempt);
            attributes.push(KeyValue::new("operation", operation.as_str()));
            attributes.push(KeyValue::new(
                "invocation_id",
                context.invocation_id.to_string(),
            ));
            (
                "runtime.invocation",
                current_or_remote(&context.attempt.trace_context),
                attributes,
            )
        }
        RuntimeSpanDescriptor::DataEdge { operation, context } => {
            let parent = context
                .trace_context
                .as_ref()
                .map_or_else(Context::current, remote_parent);
            (
                "runtime.data_edge",
                parent,
                vec![
                    KeyValue::new("operation", operation.as_str()),
                    KeyValue::new("tenant_id", context.tenant_id.to_string()),
                    KeyValue::new("conversation_id", context.conversation_id.to_string()),
                    KeyValue::new("correlation_id", context.correlation_id.to_string()),
                ],
            )
        }
        RuntimeSpanDescriptor::DbCommit(context) => (
            "runtime.db_commit",
            Context::current(),
            vec![
                KeyValue::new("operation", "db.commit"),
                KeyValue::new("commit.kind", context.kind.as_str()),
                KeyValue::new("tenant_id", context.tenant_id.to_string()),
                KeyValue::new("conversation_id", context.conversation_id.to_string()),
                KeyValue::new("correlation_id", context.correlation_id.to_string()),
            ],
        ),
    }
}

fn attempt_attributes(context: &AttemptTelemetryContext) -> Vec<KeyValue> {
    vec![
        KeyValue::new("operation", "attempt.execute"),
        KeyValue::new("tenant_id", context.tenant_id.to_string()),
        KeyValue::new("conversation_id", context.conversation_id.to_string()),
        KeyValue::new("activity_id", context.activity_id.to_string()),
        KeyValue::new("run_id", context.run_id.to_string()),
        KeyValue::new("attempt_id", context.attempt_id.to_string()),
        KeyValue::new("attempt_number", i64::from(context.attempt_number.get())),
        KeyValue::new(
            "fencing_token",
            i64::try_from(context.fencing_token.get()).unwrap_or(i64::MAX),
        ),
        KeyValue::new("correlation_id", context.correlation_id.to_string()),
    ]
}

struct OtlpRuntimeSpanBackend {
    context: Context,
}

impl RuntimeSpanBackend for OtlpRuntimeSpanBackend {
    fn record_outcome(&self, outcome: TelemetryOutcome) {
        self.context
            .span()
            .set_attribute(KeyValue::new("outcome.class", outcome.as_str()));
        if outcome == TelemetryOutcome::Error {
            self.context.span().set_status(Status::error(""));
        }
    }

    fn record_error(&self, error: ErrorClass) {
        self.context
            .span()
            .set_attribute(KeyValue::new("error.class", error.as_str()));
        self.context.span().set_status(Status::error(""));
    }

    fn record_wait(&self, wait: WaitReasonClass) {
        self.context
            .span()
            .set_attribute(KeyValue::new("wait.reason", wait.as_str()));
    }

    fn record_baggage(&self, baggage: &ControlledBaggage) {
        for (key, value) in baggage.entries() {
            self.context
                .span()
                .set_attribute(KeyValue::new(key, value.to_owned()));
        }
    }

    fn record_observation_id(&self, observation_id: musteragent_identity::LedgerRecordId) {
        self.context
            .span()
            .set_attribute(KeyValue::new("observation_id", observation_id.to_string()));
    }

    fn in_scope(&self, callback: &mut dyn FnMut()) {
        let _guard = self.context.clone().attach();
        callback();
    }
}

impl Drop for OtlpRuntimeSpanBackend {
    fn drop(&mut self) {
        self.context.span().end();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use musteragent_identity::{
        ActivityId, AttemptId, AttemptNumber, ConversationId, CorrelationId, FencingToken,
        InvocationId, RunId, TenantId,
    };
    use musteragent_runtime_contract::{TraceContext, TraceParent};
    use musteragent_telemetry::{
        AttemptTelemetryContext, InvocationOperation, InvocationTelemetryContext,
    };
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    use super::*;

    #[test]
    fn typed_invocations_share_required_identity_and_generic_tracing_is_not_exported() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("identity-schema-test");
        let attempt = attempt_context();
        for operation in [InvocationOperation::Model, InvocationOperation::Tool] {
            let span = build_runtime_span(
                &tracer,
                RuntimeSpanDescriptor::Invocation {
                    operation,
                    context: InvocationTelemetryContext {
                        attempt: attempt.clone(),
                        invocation_id: InvocationId::generate(),
                    },
                },
            );
            span.record_outcome(TelemetryOutcome::Success);
            drop(span);
        }
        let fake = tracing::info_span!(
            "runtime.invocation",
            operation = "tool",
            tenant_id = %attempt.tenant_id
        );
        drop(fake);
        provider.force_flush().expect("flush invocation spans");

        let spans = exporter.get_finished_spans().expect("finished spans");
        assert_eq!(spans.len(), 2);
        for span in spans {
            let attributes = span
                .attributes
                .iter()
                .map(|attribute| (attribute.key.as_str(), attribute.value.to_string()))
                .collect::<BTreeMap<_, _>>();
            for required in [
                "operation",
                "tenant_id",
                "conversation_id",
                "activity_id",
                "run_id",
                "attempt_id",
                "attempt_number",
                "fencing_token",
                "invocation_id",
                "correlation_id",
            ] {
                assert!(attributes.contains_key(required));
            }
            assert_eq!(
                span.span_context.trace_id().to_string(),
                "4bf92f3577b34da6a3ce929d0e0e4736"
            );
        }
    }

    fn attempt_context() -> AttemptTelemetryContext {
        AttemptTelemetryContext {
            tenant_id: TenantId::generate(),
            conversation_id: ConversationId::generate(),
            activity_id: ActivityId::generate(),
            run_id: RunId::generate(),
            attempt_id: AttemptId::generate(),
            attempt_number: AttemptNumber::new(2).expect("attempt number"),
            fencing_token: FencingToken::new(7).expect("fencing token"),
            correlation_id: CorrelationId::generate(),
            trace_context: TraceContext {
                traceparent: TraceParent::new(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                )
                .expect("traceparent"),
                tracestate: None,
            },
        }
    }
}
