use std::collections::BTreeMap;
use std::fmt;
use std::time::SystemTime;

use musteragent_runtime_contract::ErrorCode;
use musteragent_telemetry::{
    DbCommitKind, ErrorClass, LearningFailureStage, ModelResponseStage, QueueKind,
    TELEMETRY_SCHEMA_VERSION, TelemetryOperation, TelemetryOutcome, WaitReasonClass,
};
use serde_json::{Map, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::Event;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

use crate::current_trace_ids;
use crate::resource::TelemetryResourceIdentity;

#[derive(Debug, Clone)]
pub(crate) struct SafeJsonFields;

impl<'writer> FormatFields<'writer> for SafeJsonFields {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut visitor = SafeValueVisitor::default();
        fields.record(&mut visitor);
        writer.write_str(&serde_json::to_string(&visitor.values).map_err(|_| fmt::Error)?)
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &tracing::span::Record<'_>,
    ) -> fmt::Result {
        let mut values = if current.is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_str(current).map_err(|_| fmt::Error)?
        };
        let mut visitor = SafeValueVisitor { values };
        fields.record(&mut visitor);
        values = visitor.values;
        current.fields = serde_json::to_string(&values).map_err(|_| fmt::Error)?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct SafeJsonEventFormatter {
    resource: TelemetryResourceIdentity,
}

impl SafeJsonEventFormatter {
    pub(crate) fn new(resource: TelemetryResourceIdentity) -> Self {
        Self { resource }
    }
}

impl<S> FormatEvent<S, SafeJsonFields> for SafeJsonEventFormatter
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, SafeJsonFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = BTreeMap::new();
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                if let Some(formatted) = extensions.get::<FormattedFields<SafeJsonFields>>()
                    && let Ok(values) = serde_json::from_str::<BTreeMap<String, Value>>(formatted)
                {
                    fields.extend(values);
                }
            }
        }
        let mut visitor = SafeValueVisitor { values: fields };
        event.record(&mut visitor);
        if visitor.values.is_empty() {
            return Ok(());
        }
        let timestamp = OffsetDateTime::from(SystemTime::now())
            .format(&Rfc3339)
            .map_err(|_| fmt::Error)?;
        let mut envelope = Map::new();
        envelope.insert("ts".to_owned(), Value::String(timestamp));
        envelope.insert(
            "level".to_owned(),
            Value::String(event.metadata().level().as_str().to_ascii_lowercase()),
        );
        self.resource.insert_log_fields(&mut envelope);
        envelope.insert(
            "telemetry_schema".to_owned(),
            Value::String(TELEMETRY_SCHEMA_VERSION.to_owned()),
        );
        envelope.insert("diagnostic".to_owned(), Value::Bool(true));
        if let Some((trace_id, span_id)) = current_trace_ids() {
            envelope.insert("trace_id".to_owned(), Value::String(trace_id));
            envelope.insert("span_id".to_owned(), Value::String(span_id));
        }
        for (key, value) in visitor.values {
            envelope.insert(key, value);
        }
        writer.write_str(&serde_json::to_string(&envelope).map_err(|_| fmt::Error)?)?;
        writeln!(writer)
    }
}

#[derive(Default)]
struct SafeValueVisitor {
    values: BTreeMap<String, Value>,
}

impl SafeValueVisitor {
    fn insert(&mut self, field: &Field, value: Value) {
        self.values.insert(field.name().to_owned(), value);
    }
}

impl Visit for SafeValueVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if matches!(
            field.name(),
            "otlp.enabled" | "otlp.required" | "provider.retryable"
        ) {
            self.insert(field, Value::Bool(value));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if integer_field(field.name(), value) {
            self.insert(field, Value::Number(value.into()));
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if integer_field(field.name(), i64::try_from(value).unwrap_or(i64::MAX)) {
            self.insert(field, Value::Number(value.into()));
        }
    }

    fn record_f64(&mut self, _field: &Field, _value: f64) {}

    fn record_str(&mut self, field: &Field, value: &str) {
        if typed_string_field(field.name(), value) {
            self.insert(field, Value::String(value.to_owned()));
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

fn integer_field(field: &str, value: i64) -> bool {
    match field {
        "attempt_number"
        | "fencing_token"
        | "queue.depth"
        | "remaining_attempts"
        | "deadline_ms"
        | "retry_after_ms"
        | "consecutive_failures"
        | "model.response.elapsed_us"
        | "task.handoff.elapsed_us"
        | "dispatch.prepare.duration_us"
        | "snapshot.prepare.duration_us"
        | "context.plan.duration_us" => value >= 0,
        "http_status" | "provider.http_status" => (100..=599).contains(&value),
        _ => false,
    }
}

fn typed_string_field(field: &str, value: &str) -> bool {
    match field {
        "dispatch.prepare.stage" => matches!(
            value,
            "recovery_candidates"
                | "ready_candidates"
                | "claim_commit"
                | "due_work"
                | "claim"
                | "cancellation"
        ),
        "context.plan.outcome" => matches!(value, "ok" | "error" | "cancelled"),
        "snapshot.prepare.outcome" => matches!(value, "ok" | "error"),
        "snapshot.prepare.stage" => matches!(
            value,
            "agent_material" | "personalization" | "build_content"
        ),
        "goal.admission.stage" => matches!(value, "prepare" | "grant" | "commit"),
        "task.handoff.stage" => matches!(
            value,
            "prepare"
                | "grant"
                | "projection"
                | "proof"
                | "selection"
                | "commit"
                | "commit_contract"
                | "message_record"
                | "admission_prefix"
                | "snapshot_source"
                | "source"
                | "source_reload"
                | "foundation"
        ),
        "task.handoff.error" => matches!(
            value,
            "authority_unavailable"
                | "policy_denied"
                | "preparation_changed"
                | "admission_failed"
                | "dependency_unavailable"
                | "invalid_contract"
        ),
        "provider.protocol_error" => {
            musteragent_telemetry::ProviderProtocolError::from_safe_key(value).is_some()
        }
        "model.response.stage" => ModelResponseStage::from_safe_key(value).is_some(),
        "provider.error_code" => matches!(
            value,
            "authentication_failed"
                | "authorization_failed"
                | "rate_limited"
                | "quota_exceeded"
                | "invalid_request"
                | "provider_server"
                | "protocol_violation"
                | "deadline_exceeded"
                | "transport_failed"
                | "cancelled"
                | "credential_resolution_failed"
                | "content_resolution_failed"
                | "route_configuration_invalid"
        ),
        "model.stream.failure" => value == "canonical_idle_timeout",
        "provider.failure" => matches!(
            value,
            "connect_timeout"
                | "transport_timeout"
                | "connect_failed"
                | "body_failed"
                | "transport_failed"
                | "http_status"
                | "response_start_timeout"
                | "stream_read_idle_timeout"
                | "stream_eof_before_terminal"
                | "response_normalization"
                | "canonical_stream_validation"
                | "response_finish"
        ),
        "vision.stream.failure" => matches!(
            value,
            "provider_error"
                | "invalid_event_sequence"
                | "unexpected_event"
                | "incomplete_response"
                | "empty_analysis"
                | "analysis_too_large"
                | "invalid_stop_reason"
                | "missing_usage"
                | "missing_response_id"
                | "missing_model_key"
                | "inactivity_timeout"
        ),
        "task_id" | "goal_id" | "tenant_id" | "conversation_id" | "activity_id" | "run_id"
        | "attempt_id" | "invocation_id" | "command_id" | "correlation_id" | "content_id" => {
            uuid::Uuid::parse_str(value).is_ok()
        }
        "operation" => TelemetryOperation::from_safe_key(value).is_some(),
        "outcome.class" => TelemetryOutcome::from_safe_key(value).is_some(),
        "error.class" => ErrorClass::from_safe_key(value).is_some(),
        "wait.reason" => WaitReasonClass::from_safe_key(value).is_some(),
        "error.code" => ErrorCode::from_safe_code(value).is_some(),
        "failure.stage" => {
            LearningFailureStage::from_safe_key(value).is_some() || runtime_failure_stage(value)
        }
        "context.plan.stage" => context_plan_stage(value),
        "context.plan.error" => canonical_model_error(value),
        "vision.model.stage" => matches!(
            value,
            "begin"
                | "provider_stream"
                | "response_store"
                | "usage_fact"
                | "metering_encode"
                | "complete_success"
                | "complete_failure"
        ),
        "model.persistence.error" => matches!(
            value,
            "request_hash_mismatch"
                | "terminal_timestamp_mismatch"
                | "started_contains_outcome"
                | "succeeded_outcome_incomplete"
                | "failed_without_error"
                | "cancelled_contains_failure"
                | "usage_total_below_components"
                | "cache_write_one_hour_exceeds_total"
                | "metering_publication_invalid"
                | "invocation_not_terminal"
                | "usage_fact_publication_mismatch"
                | "usage_fact_identity_mismatch"
                | "admission_route_invalid"
                | "admission_route_mismatch"
                | "begin_requires_started"
                | "idempotency_conflict"
                | "terminal_conflict"
                | "corrupt_state"
                | "numeric_range"
                | "protection"
                | "digest"
                | "audit"
                | "database"
                | "transaction"
        ),
        "tool.persistence.error" => matches!(
            value,
            "invalid_transition"
                | "transition_conflict"
                | "not_found"
                | "idempotency_conflict"
                | "content_unavailable"
                | "corrupt"
                | "database"
                | "ledger"
                | "execution"
                | "interaction"
                | "protection"
                | "numeric_conversion"
                | "commit_lane"
                | "transaction"
                | "audit"
        ),
        "diagnostic_ref" => snapshot_diagnostic_ref(value),
        "snapshot.error" => snapshot_contract_error(value),
        "snapshot.projection.error" => matches!(
            value,
            "schema"
                | "agent"
                | "memory"
                | "instincts"
                | "model_class"
                | "capabilities"
                | "primary"
                | "fallbacks"
                | "auxiliary"
        ),
        "media.stage" => matches!(
            value,
            "queued" | "running" | "provider_completed" | "committed"
        ),
        "material.stage" => matches!(
            value,
            "unspecified"
                | "source_run_owner"
                | "ledger_message_run_identity"
                | "tool_terminal_missing"
                | "tool_terminal_result_contract"
                | "dialogue_checkpoint_manifest"
                | "dialogue_page_codec"
                | "dialogue_page_coverage"
                | "dialogue_bound_coverage"
                | "dialogue_legacy_page_source"
                | "dialogue_page_format"
        ),
        "material.error" => matches!(
            value,
            "corrupt_state"
                | "no_model_input"
                | "stale_fence"
                | "content"
                | "content_not_found"
                | "content_unauthorized"
                | "content_too_large"
                | "content_length_mismatch"
                | "content_hash_mismatch"
                | "content_identity_conflict"
                | "content_dependency_unavailable"
                | "content_invariant"
                | "database"
                | "database_unavailable"
                | "database_invalid"
                | "numeric"
                | "tool_not_pinned"
                | "tool_not_exposed"
                | "corrupt"
        ),
        "content.error" => matches!(
            value,
            "not_found"
                | "cache_invalidated"
                | "cache_unavailable"
                | "unauthorized"
                | "too_large"
                | "length_mismatch"
                | "hash_mismatch"
                | "identity_conflict"
                | "dependency_unavailable"
                | "invariant"
                | "blob_not_found"
                | "blob_too_large"
                | "blob_identity_conflict"
                | "blob_unavailable"
                | "protection_unknown_key"
                | "protection_decrypt_failed"
                | "protection_invalid"
                | "database_unavailable"
                | "transaction_unavailable"
        ),
        "database.failure" => matches!(
            value,
            "database"
                | "pool_timeout"
                | "pool_closed"
                | "io"
                | "tls"
                | "worker_crashed"
                | "begin_failed"
                | "row_not_found"
                | "protocol"
                | "codec"
                | "other"
        ),
        "file_catalog.outcome" => matches!(value, "dependency_unavailable" | "corrupt"),
        "database.sqlstate" => {
            value.len() == 5
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        }
        "queue.kind" => QueueKind::from_safe_key(value).is_some(),
        "commit.kind" => DbCommitKind::from_safe_key(value).is_some(),
        "grpc_address" | "ops_address" | "data_edge_address" => {
            value.parse::<std::net::SocketAddr>().is_ok()
        }
        "telemetry.schema" => value == TELEMETRY_SCHEMA_VERSION,
        _ => false,
    }
}

fn canonical_model_error(value: &str) -> bool {
    matches!(
        value,
        "empty_text"
            | "text_too_long"
            | "text_control_character"
            | "json_too_deep"
            | "non_finite_number"
            | "invalid_json_object_key"
            | "json_object_required"
            | "empty_message"
            | "invalid_user_message"
            | "invalid_assistant_message"
            | "invalid_tool_message"
            | "tool_schema_object_required"
            | "invalid_provider_replay"
            | "duplicate_tool_call_id"
            | "orphan_tool_result"
            | "tool_identity_mismatch"
            | "incomplete_tool_exchange"
            | "missing_prompt_or_input"
            | "missing_turn_stimulus"
            | "runtime_event_on_auxiliary_route"
            | "invocation_identity_mismatch"
            | "invalid_temperature"
            | "invalid_route"
            | "output_budget_exceeds_route"
            | "invalid_stream_budget"
            | "continuation_on_stateless_route"
    )
}

fn context_plan_stage(value: &str) -> bool {
    matches!(
        value,
        "total"
            | "material_cache_begin"
            | "dialogue_prepare"
            | "dialogue_primary_cut"
            | "dialogue_input_capacity"
            | "dialogue_response_schema"
            | "input_validation"
            | "material_load"
            | "dialogue_window"
            | "dialogue_bind"
            | "dialogue_compaction"
            | "dialogue_material"
            | "dialogue_capacity_delta"
            | "dialogue_material_validation"
            | "dialogue_compaction_partition"
            | "dialogue_coverage"
            | "dialogue_compression_route"
            | "dialogue_compression_budget"
            | "dialogue_summary"
            | "dialogue_checkpoint"
            | "tool_result_visibility"
            | "delivery_repair_material"
            | "plan_lineage"
            | "file_tool_guidance"
            | "provider_history_projection"
            | "retained_provider_history_projection"
            | "delivery_repair_request"
            | "model_request_persist"
            | "material_validation"
            | "active_model_route"
            | "iteration_identity"
            | "instruction_materialization"
            | "runtime_event_projection"
            | "tool_surface_materialization"
            | "tool_surface_authority_instruction"
            | "run_progress_directive"
            | "empty_response_recovery_instruction"
            | "model_action_rejection_instruction"
            | "model_action_correction_turn"
            | "summary_decode"
            | "baseline_resolution"
            | "logical_history_projection"
            | "provider_input_selection"
            | "summary_instruction"
            | "planning_budget_resolution"
            | "provider_context_projection"
            | "tool_history_projection"
            | "route_presentation"
            | "usage_anchor_validation"
            | "compaction_selection"
            | "summary_projection"
            | "summary_encoding"
            | "summary_generation"
            | "summary_persist"
            | "retained_provider_context_projection"
            | "retained_tool_history_projection"
            | "retained_route_presentation"
            | "causation_resolution"
            | "output_budget_resolution"
            | "canonical_request_validation"
            | "model_request_encoding"
            | "context_snapshot_construction"
            | "context_plan_commit"
            | "stored_identity_validation"
    )
}

fn runtime_failure_stage(value: &str) -> bool {
    matches!(
        value,
        "load_policy"
            | "policy_gate"
            | "policy_hook"
            | "attempt_limit"
            | "load_checkpoint"
            | "validate_checkpoint"
            | "recover_checkpoint"
            | "context_planning"
            | "begin_model"
            | "model_execution"
            | "fallback_policy"
            | "fallback_commit"
            | "finish_model"
            | "tool_execution"
            | "commit_tool_result"
            | "complete_run"
            | "cancellation_commit"
            | "budget_wait_commit"
            // Submit Message admission and immutable Snapshot construction.
            | "admission_replay"
            | "admission_denial_read"
            | "admission_authority"
            | "assistant_identity"
            | "snapshot_resolution"
            | "admission_content_staging"
            | "admission_durable_commit"
            | "admission_rejection_commit"
            | "admission_cancellation_commit"
            | "retry_admission_replay"
            | "retry_admission_denial_read"
            | "retry_admission_authority"
            | "retry_source_snapshot"
            | "retry_snapshot_reseal"
            | "retry_admission_durable_commit"
            | "prepare"
            | "build-content"
            | "seal"
            | "snapshot_contract_validation"
            | "active_tool_catalog_projection"
            | "granted_tool_contract_missing"
            | "tool_catalog_identity_collision"
            | "resolved_tool_catalog_validation"
            | "dialogue_snapshot_source_validation"
            | "dialogue_snapshot_contract_validation"
            | "skill_catalog_projection"
            | "tool_catalog_projection"
            | "explicit_skill_disclosure_selection"
            | "product_surface_context"
            | "product_capability_selection"
            | "active_tool_catalog"
            | "active_skill_catalog"
            | "prior_decision_content"
            | "user_profile_content"
            | "instinct_selection"
            | "memory_guidance_content"
            | "agent_autonomy_content"
            | "product_surface_content"
            | "product_capability_content"
            // Security Audit persistence stages. These names are static
            // adapter vocabulary; database messages and dynamic values remain
            // excluded from the diagnostic envelope.
            | "admission_denial_audit"
            | "admission_denial_trace"
            | "admission_denial_transaction"
            | "admission_denial_lock"
            | "admission_denial_insert"
            | "admission_denial_verify"
            | "admission_denial_append_audit"
            | "admission_denial_commit"
    )
}

fn snapshot_diagnostic_ref(value: &str) -> bool {
    let Some((prefix, correlation_id)) = value.rsplit_once(':') else {
        return false;
    };
    matches!(
        prefix,
        "snapshot.prepare" | "snapshot.build-content" | "snapshot.seal"
    ) && uuid::Uuid::parse_str(correlation_id).is_ok()
}

fn snapshot_contract_error(value: &str) -> bool {
    matches!(
        value,
        "identity"
            | "key"
            | "agent"
            | "skill"
            | "tool"
            | "memory"
            | "model"
            | "schema_mismatch"
            | "product_namespace_mismatch"
            | "discovery_schema_mismatch"
            | "agent_version_schema_mismatch"
            | "instincts_not_canonical"
            | "invalid_agent_version_snapshot"
            | "auxiliary_schema_mismatch"
            | "auxiliary_models_not_canonical"
            | "auxiliary_contains_fallback_plan"
            | "invalid_discovery"
            | "invalid_skill_catalog"
            | "invalid_private_evaluation"
            | "non_musteragent"
            | "prompt_sections_not_canonical"
            | "skills_not_canonical"
            | "duplicate_tool"
            | "tool_metadata_empty"
            | "tool_schema_bytes_mismatch"
            | "invalid_file_operation_metadata"
            | "tool_execution_contract_mismatch"
            | "capabilities_not_canonical"
            | "duplicate_capability"
            | "skill_capability_not_granted"
            | "tool_capability_not_granted"
            | "tool_capability_effect_mismatch"
            | "tool_capability_location_mismatch"
            | "tool_capability_scope_mismatch"
            | "invalid_context_budget"
            | "invalid_run_execution_policy"
            | "invalid_model_stream_budget"
            | "fallback_plan_mismatch"
            | "nested_fallback_plan"
            | "empty_encoding"
    )
}

pub(crate) fn fmt_layer<S, W>(
    resource: TelemetryResourceIdentity,
    writer: W,
) -> impl Layer<S> + Send + Sync
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .fmt_fields(SafeJsonFields)
        .event_format(SafeJsonEventFormatter::new(resource))
}

/// This subscriber has one diagnostic sink; filtering belongs to the subscriber.
/// A per-layer filter retains thread-local decisions from LogTracer::enabled
/// probes (e.g. SQLx), suppressing the next independently enabled event.
pub(crate) fn diagnostic_subscriber<W>(
    resource: TelemetryResourceIdentity,
    writer: W,
    filter: tracing_subscriber::EnvFilter,
) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    use tracing_subscriber::layer::SubscriberExt as _;
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer(resource, writer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_telemetry::TelemetryEventKind;
    use std::io;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    fn test_resource() -> TelemetryResourceIdentity {
        TelemetryResourceIdentity::new(
            "test-service".to_owned(),
            "test-version".to_owned(),
            "test".to_owned(),
            "test-cluster".to_owned(),
            "test-deployment".to_owned(),
            "test-build".to_owned(),
        )
    }

    impl io::Write for CaptureWriter {
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

    impl<'writer> MakeWriter<'writer> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CaptureWriter(Arc::clone(&self.0))
        }
    }

    struct PanicOnDebug;

    impl fmt::Debug for PanicOnDebug {
        fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("rejected Debug field must never be formatted")
        }
    }

    #[test]
    fn latency_diagnostics_retain_only_closed_stages_and_safe_timing() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(
                "task.handoff.stage" = "foundation",
                "task.handoff.elapsed_us" = 125_000_u64,
                "context.plan.stage" = "dialogue_capacity_delta",
                "context.plan.duration_us" = 300_000_u64,
                "context.plan.outcome" = "ok",
                "snapshot.prepare.stage" = "build_content",
                "snapshot.prepare.duration_us" = 120_000_u64,
                "snapshot.prepare.outcome" = "ok",
                "dispatch.prepare.stage" = "claim_commit",
                "dispatch.prepare.duration_us" = 90_000_u64,
                payload = "private conversation content"
            );
            tracing::debug!(
                "context.plan.stage" = "private conversation content",
                "context.plan.outcome" = "arbitrary error",
                "snapshot.prepare.stage" = "private conversation content",
                "snapshot.prepare.outcome" = "arbitrary error",
                "snapshot.prepare.duration_us" = -1_i64,
                "dispatch.prepare.stage" = "private conversation content",
                "dispatch.prepare.duration_us" = -1_i64,
                "context.plan.duration_us" = -1_i64
            );
        });
        let output = String::from_utf8(
            capture
                .0
                .lock()
                .expect("captured structured test log")
                .clone(),
        )
        .expect("captured structured test log");
        let rows: Vec<serde_json::Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).expect("captured structured test log"))
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["task.handoff.elapsed_us"], 125_000);
        assert_eq!(rows[0]["context.plan.duration_us"], 300_000);
        assert_eq!(rows[0]["context.plan.stage"], "dialogue_capacity_delta");
        assert_eq!(rows[0]["snapshot.prepare.stage"], "build_content");
        assert_eq!(rows[0]["snapshot.prepare.duration_us"], 120_000);
        assert_eq!(rows[0]["snapshot.prepare.outcome"], "ok");
        assert_eq!(rows[0]["dispatch.prepare.stage"], "claim_commit");
        assert_eq!(rows[0]["dispatch.prepare.duration_us"], 90_000);
        assert!(!output.contains("private conversation content"));
        assert!(!output.contains("arbitrary error"));
    }

    #[test]
    fn task_handoff_diagnostics_are_closed_and_survive_production_logging() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                "task_id" = "4d208d59-7517-59ab-9dc2-a4c206cfd689",
                "task.handoff.stage" = "commit",
                "task.handoff.error" = "invalid_contract",
                payload = "private task content"
            );
            tracing::warn!(
                "task.handoff.stage" = "private task content",
                "task.handoff.error" = "arbitrary provider error"
            );
        });
        let output = String::from_utf8(
            capture
                .0
                .lock()
                .expect("captured structured test log")
                .clone(),
        )
        .expect("captured structured test log");
        assert_eq!(output.lines().count(), 1);
        let record: Value =
            serde_json::from_str(output.trim()).expect("captured structured test log");
        assert_eq!(record["task.handoff.stage"], "commit");
        assert_eq!(record["task.handoff.error"], "invalid_contract");
        assert!(!output.contains("private task content"));
    }

    #[test]
    fn admission_diagnostics_preserve_only_typed_failure_identity() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        let command = uuid::Uuid::from_u128(1).to_string();
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                command_id = command.as_str(),
                failure.stage = "admission_content_staging",
                content.error = "database_unavailable",
                database.failure = "database",
                database.sqlstate = "42501",
                error = ?PanicOnDebug
            );
        });
        let output = String::from_utf8(
            capture
                .0
                .lock()
                .expect("captured structured test log")
                .clone(),
        )
        .expect("captured structured test log");
        let record: Value =
            serde_json::from_str(output.trim()).expect("captured structured test log");
        assert_eq!(record["command_id"], command);
        assert_eq!(record["failure.stage"], "admission_content_staging");
        assert_eq!(record["content.error"], "database_unavailable");
        assert_eq!(record["database.failure"], "database");
        assert_eq!(record["database.sqlstate"], "42501");
        assert!(record.get("error").is_none());
    }

    #[test]
    fn owner_goal_admission_logs_stage_and_identity_without_task_content() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        let goal = uuid::Uuid::from_u128(2).to_string();
        tracing::subscriber::with_default(subscriber, || {
            for stage in ["prepare", "grant", "commit"] {
                tracing::warn!(
                    goal_id = goal.as_str(),
                    goal.admission.stage = stage,
                    error.code = "resource_revision_conflict",
                    error = ?PanicOnDebug,
                    "private task content"
                );
            }
            tracing::warn!(
                goal_id = "private title",
                goal.admission.stage = "private objective"
            );
        });
        let output = String::from_utf8(
            capture
                .0
                .lock()
                .expect("captured structured test log")
                .clone(),
        )
        .expect("captured structured test log");
        let records: Vec<Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).expect("captured structured test log"))
            .collect();
        assert_eq!(records.len(), 3);
        for (record, stage) in records.iter().zip(["prepare", "grant", "commit"]) {
            assert_eq!(record["goal_id"], goal);
            assert_eq!(record["goal.admission.stage"], stage);
            assert_eq!(record["error.code"], "resource_revision_conflict");
        }
        assert!(!output.contains("private"));
    }

    #[test]
    fn production_response_timing_survives_safe_formatter_once_per_invocation_stage() {
        use musteragent_identity::{InvocationId, RunId};
        use musteragent_telemetry::ModelResponseTiming;

        let capture = Capture::default();
        let subscriber = diagnostic_subscriber(
            test_resource(),
            capture.clone(),
            tracing_subscriber::EnvFilter::new("info,musteragent_context=debug"),
        );
        let run_id = RunId::generate();
        let invocations = [InvocationId::generate(), InvocationId::generate()];
        let stages = [
            (ModelResponseStage::RequestPrepared, "request_prepared"),
            (ModelResponseStage::HttpSendStarted, "http_send_started"),
            (ModelResponseStage::ResponseHeaders, "response_headers"),
            (
                ModelResponseStage::FirstTransportByte,
                "first_transport_byte",
            ),
            (
                ModelResponseStage::FirstProviderEvent,
                "first_provider_event",
            ),
            (ModelResponseStage::FirstText, "first_text"),
            (ModelResponseStage::FirstAction, "first_action"),
        ];
        tracing::subscriber::with_default(subscriber, || {
            for invocation_id in invocations {
                // Invoke the production emitter, not a copy of its tracing call.
                // No parent span is needed to recover run/invocation identity.
                let timing = ModelResponseTiming::new(run_id, invocation_id);
                for (stage, _) in stages {
                    // SQL drivers probe disabled diagnostics after every await.
                    // Such a probe must not suppress the following model stage.
                    let _ = log::Log::enabled(
                        &tracing_log::LogTracer::default(),
                        &log::Metadata::builder()
                            .level(log::Level::Debug)
                            .target("sqlx::query")
                            .build(),
                    );
                    timing.observe(stage);
                    timing.observe(stage);
                }
            }
        });
        let output = String::from_utf8(capture.0.lock().expect("capture").clone()).expect("UTF-8");
        let records = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("diagnostic JSON"))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), invocations.len() * stages.len());
        for (records, invocation_id) in records.chunks_exact(stages.len()).zip(invocations) {
            let mut previous_elapsed = 0;
            for (record, (stage, key)) in records.iter().zip(stages) {
                assert_eq!(record["run_id"], run_id.to_string());
                assert_eq!(record["invocation_id"], invocation_id.to_string());
                assert_eq!(record["model.response.stage"], key);
                assert_eq!(ModelResponseStage::from_safe_key(key), Some(stage));
                let elapsed = record["model.response.elapsed_us"]
                    .as_u64()
                    .expect("typed nonnegative microseconds");
                assert!(elapsed >= previous_elapsed);
                previous_elapsed = elapsed;
                assert_eq!(record["level"], "info");
                assert_eq!(record["diagnostic"], true);
                assert_eq!(record["telemetry_schema"], TELEMETRY_SCHEMA_VERSION);
                assert!(record.get("message").is_none());
                assert!(record.get("event.key").is_none());
            }
        }
    }

    #[test]
    fn response_timing_filter_preserves_privacy_and_rejects_invalid_values() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                run_id = "3817aa22-b38d-89f5-8744-ca50ec6f3118",
                model.response.stage = "first_text",
                model.response.elapsed_us = 123_456_u64,
                raw_response = "private response",
                reasoning = "private reasoning",
                authorization = "Bearer private-credential",
                error = ?PanicOnDebug,
                "private prompt"
            );
            // An arbitrary stage, malformed IDs and a string duration must not
            // provide another way to smuggle request data into diagnostics.
            tracing::info!(
                run_id = "private run",
                invocation_id = "private invocation",
                model.response.stage = "private stage",
                model.response.elapsed_us = "private duration"
            );
            tracing::info!(model.response.elapsed_us = -1_i64);
        });
        let output = String::from_utf8(capture.0.lock().expect("capture").clone()).expect("UTF-8");
        let records = output.lines().collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        let value: Value = serde_json::from_str(records[0]).expect("diagnostic JSON");
        assert_eq!(value["model.response.stage"], "first_text");
        assert_eq!(value["model.response.elapsed_us"], 123_456);
        assert_eq!(value["run_id"], "3817aa22-b38d-89f5-8744-ca50ec6f3118");
        assert!(!output.contains("private"));
        assert!(!output.contains("Bearer"));
        assert!(value.get("error").is_none());
    }

    #[test]
    fn typed_string_fields_are_closed_vocabulary() {
        for stage in ["prepare", "grant", "commit"] {
            assert!(typed_string_field("goal.admission.stage", stage));
        }
        assert!(!typed_string_field(
            "goal.admission.stage",
            "private goal content"
        ));
        assert!(typed_string_field("outcome.class", "success"));
        assert!(!typed_string_field("outcome.class", "customer supplied"));
        assert!(!typed_string_field("provider_stage", "Bearer top-secret"));
        assert!(!typed_string_field("provider_code", "safe-looking"));
        assert!(!typed_string_field("event_name", "chunk"));
        assert!(!typed_string_field("event.key", "attempt.completed"));
        assert!(!typed_string_field("event.key", "user supplied event"));
        assert!(typed_string_field("error.code", "dependency_timeout"));
        assert!(!typed_string_field("error.code", "provider body"));
        assert!(typed_string_field(
            "operation",
            "evolution.learning.schedule"
        ));
        assert!(typed_string_field("failure.stage", "evidence_load"));
        assert!(typed_string_field(
            "failure.stage",
            "maintenance_authorization"
        ));
        assert!(typed_string_field("failure.stage", "tool_execution"));
        assert!(typed_string_field(
            "failure.stage",
            "admission_rejection_commit"
        ));
        assert!(typed_string_field(
            "failure.stage",
            "admission_cancellation_commit"
        ));
        assert!(typed_string_field("failure.stage", "build-content"));
        assert!(typed_string_field(
            "context.plan.stage",
            "tool_history_projection"
        ));
        assert!(!typed_string_field(
            "context.plan.stage",
            "customer supplied"
        ));
        assert!(typed_string_field(
            "context.plan.error",
            "missing_turn_stimulus"
        ));
        assert!(!typed_string_field(
            "context.plan.error",
            "customer supplied"
        ));
        assert!(typed_string_field(
            "failure.stage",
            "admission_denial_append_audit"
        ));
        assert!(!typed_string_field("failure.stage", "customer supplied"));
        assert!(typed_string_field(
            "diagnostic_ref",
            "snapshot.build-content:01a05ced-b103-7e83-ba5f-1e2f015b5af7"
        ));
        assert!(!typed_string_field(
            "diagnostic_ref",
            "snapshot.build-content:not-a-correlation-id"
        ));
        assert!(typed_string_field(
            "snapshot.error",
            "skill_capability_not_granted"
        ));
        assert!(!typed_string_field("snapshot.error", "customer supplied"));
        assert!(typed_string_field(
            "failure.stage",
            "skill_catalog_projection"
        ));
        assert!(typed_string_field(
            "failure.stage",
            "tool_catalog_projection"
        ));
        assert!(typed_string_field(
            "snapshot.error",
            "invalid_skill_catalog"
        ));
        assert!(typed_string_field("material.error", "no_model_input"));
        assert!(typed_string_field("material.error", "tool_not_exposed"));
        assert!(typed_string_field("material.error", "database_invalid"));
        assert!(typed_string_field("media.stage", "provider_completed"));
        assert!(!typed_string_field("media.stage", "customer supplied"));
        assert!(typed_string_field(
            "material.stage",
            "tool_terminal_missing"
        ));
        assert!(typed_string_field(
            "material.stage",
            "tool_terminal_result_contract"
        ));
        for stage in [
            "dialogue_checkpoint_manifest",
            "dialogue_page_codec",
            "dialogue_page_coverage",
            "dialogue_bound_coverage",
            "dialogue_legacy_page_source",
            "dialogue_page_format",
        ] {
            assert!(typed_string_field("material.stage", stage));
        }
        assert!(!typed_string_field("material.stage", "customer supplied"));
        assert!(typed_string_field(
            "material.error",
            "content_hash_mismatch"
        ));
        assert!(typed_string_field(
            "content.error",
            "protection_decrypt_failed"
        ));
        assert!(typed_string_field("content.error", "cache_invalidated"));
        assert!(typed_string_field("content.error", "cache_unavailable"));
        assert!(typed_string_field(
            "content_id",
            "01a03382-f82d-71b1-b24a-6ddc35632c9b"
        ));
        assert!(!typed_string_field("content.error", "provider response"));
        assert!(typed_string_field("database.sqlstate", "42703"));
        assert!(!typed_string_field("database.sqlstate", "column missing"));
        assert!(typed_string_field("database.failure", "pool_timeout"));
        assert!(!typed_string_field("database.failure", ""));
        assert!(!typed_string_field(
            "database.failure",
            "connection refused at 10.0.0.1"
        ));
        assert!(typed_string_field("file_catalog.outcome", "corrupt"));
        assert!(!typed_string_field("file_catalog.outcome", "report.pdf"));
        assert!(!typed_string_field("material.error", "customer supplied"));
        assert!(integer_field("retry_after_ms", 60_000));
        assert!(integer_field("consecutive_failures", 1));
    }

    #[test]
    fn event_with_only_rejected_dynamic_fields_is_completely_suppressed() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "musteragent_telemetry_otlp::events",
                tracing::Level::WARN,
                event.key = TelemetryEventKind::ExportFailed.as_str(),
                user_input = "repeat my private diary",
                authorization = "Bearer sk-live-secret",
                provider_body = "{\"password\":\"hunter2\"}",
                provider_code = "user-controlled-code",
                provider_stage = "request_mapping",
                "repeat my private diary; Authorization: Bearer sk-live-secret"
            );
        });
        let output = String::from_utf8(capture.0.lock().expect("capture lock").clone())
            .expect("JSON log is utf-8");
        assert!(output.is_empty());
        for forbidden in [
            "repeat my private diary",
            "Authorization",
            "Bearer",
            "sk-live-secret",
            "password",
            "hunter2",
            "user-controlled-code",
        ] {
            assert!(!output.contains(forbidden), "leaked {forbidden}: {output}");
        }
    }

    #[test]
    fn rejected_debug_field_is_dropped_before_its_formatter_runs() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "musteragent_telemetry_otlp::events",
                tracing::Level::WARN,
                event.key = TelemetryEventKind::ExportFailed.as_str(),
                error = ?PanicOnDebug,
            );
        });
        let output = String::from_utf8(capture.0.lock().expect("capture lock").clone())
            .expect("JSON log is utf-8");
        assert!(output.is_empty());
    }

    #[test]
    fn arbitrary_callsite_cannot_become_machine_classifiable() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "musteragent_telemetry_otlp::events",
                tracing::Level::INFO,
                event.key = TelemetryEventKind::CommandCompleted.as_str(),
                outcome.class = "success",
                "arbitrary callsite"
            );
        });
        let output = String::from_utf8(capture.0.lock().expect("capture lock").clone())
            .expect("diagnostic log is utf-8");
        let value: Value = serde_json::from_str(output.trim()).expect("valid diagnostic JSON");
        assert_eq!(value["diagnostic"], true);
        assert_eq!(value["environment"], "test");
        assert_eq!(value["cluster"], "test-cluster");
        assert_eq!(value["deployment"], "test-deployment");
        assert_eq!(value["build"], "test-build");
        assert!(value.get("event.key").is_none());
    }

    #[test]
    fn vision_persistence_diagnostics_keep_only_closed_fields() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(
                vision.model.stage = "complete_success",
                model.persistence.error = "usage_fact_identity_mismatch",
                tool.persistence.error = "corrupt",
                database.sqlstate = "23514",
                provider.failure = "connect_timeout",
                provider.http_status = 503_u64,
                vision.stream.failure = "missing_usage",
                error = ?PanicOnDebug,
                "secret prompt must not be logged"
            );
        });
        let output = String::from_utf8(capture.0.lock().expect("capture").clone()).expect("UTF-8");
        let value: Value = serde_json::from_str(output.trim()).expect("diagnostic JSON");
        assert_eq!(value["vision.model.stage"], "complete_success");
        assert_eq!(
            value["model.persistence.error"],
            "usage_fact_identity_mismatch"
        );
        assert_eq!(value["tool.persistence.error"], "corrupt");
        assert_eq!(value["database.sqlstate"], "23514");
        assert_eq!(value["provider.failure"], "connect_timeout");
        assert_eq!(value["provider.http_status"], 503);
        assert_eq!(value["vision.stream.failure"], "missing_usage");
        assert!(!integer_field("provider.http_status", 999));
        assert!(!output.contains("secret prompt"));
        for key in [
            "vision.model.stage",
            "model.persistence.error",
            "tool.persistence.error",
            "provider.failure",
            "vision.stream.failure",
        ] {
            assert!(!typed_string_field(key, "customer image or credential"));
        }
    }
    #[test]
    fn model_provider_failure_keeps_typed_cause_and_invocation_without_payloads() {
        let capture = Capture::default();
        let subscriber =
            tracing_subscriber::registry().with(fmt_layer(test_resource(), capture.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(invocation_id = "3817aa22-b38d-89f5-8744-ca50ec6f3118",
                provider.error_code = "transport_failed", provider.retryable = true,
                provider.failure = "stream_eof_before_terminal",
                provider.protocol_error = "tool_identity_changed",
                raw_response = "secret prompt and credential", error = ?PanicOnDebug);
        });
        let output = String::from_utf8(capture.0.lock().expect("capture").clone()).expect("UTF-8");
        let value: Value = serde_json::from_str(output.trim()).expect("JSON");
        assert_eq!(value["provider.error_code"], "transport_failed");
        assert_eq!(value["provider.protocol_error"], "tool_identity_changed");
        assert!(!typed_string_field(
            "provider.protocol_error",
            "secret raw provider content"
        ));
        assert_eq!(value["provider.retryable"], true);
        assert_eq!(
            value["invocation_id"],
            "3817aa22-b38d-89f5-8744-ca50ec6f3118"
        );
        assert_eq!(value["provider.failure"], "stream_eof_before_terminal");
        assert!(!output.contains("secret prompt"));
        assert!(!typed_string_field(
            "provider.error_code",
            "secret raw error"
        ));
    }
}
