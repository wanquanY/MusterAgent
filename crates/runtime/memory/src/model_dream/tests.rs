use std::collections::{BTreeMap, BTreeSet};

use musteragent_activity::ActivityKind;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_evolution::{
    BranchEvidenceState, DreamJob, DreamJobStatus, DreamWorkload, EvidenceSpan, LearningOutcome,
    LearningSignal, LearningSignalKind, RunTerminalEvidence,
};
use musteragent_identity::{
    AgentId, AgentVersionId, AttemptId, ContentId, DreamJobId, LearningSignalId, LedgerOffset,
    LedgerRecordId, MemoryCollectionId, PrincipalId, RunId, TenantId, UtcTimestamp, VersionNumber,
    VersionedKey,
};
use musteragent_model::AuxiliaryModelRole;
use musteragent_run::RunStatus;
use musteragent_runtime_model::{JsonValue, ModelInvocationPurpose, ModelInvocationRouteRole};
use serde_json::{Value, json};

use super::{
    CompletedDreamModelRun, DREAM_MODEL_INPUT_MEDIA_TYPE, DREAM_MODEL_OUTPUT_MEDIA_TYPE,
    DREAM_MODEL_OUTPUT_SCHEMA, DreamEvidenceMaterial, DreamLearningInput, DreamModelRunError,
    DreamModelRunRequest, DreamSubmissionValidationError, ModelDreamError,
    bounded_model_input_bytes, bounded_record_selection, dream_maintenance_activity_id,
    dream_maintenance_agent_participant_id, dream_maintenance_command_id,
    dream_maintenance_conversation_id, dream_maintenance_invocation_id, dream_maintenance_run_id,
    dream_maintenance_runtime_snapshot_id, dream_maintenance_service_participant_id,
    is_dream_model_input_media_type, is_dream_model_input_schema, is_dream_model_output_media_type,
    is_dream_model_output_schema, model_evidence_json, parse_model_output,
    validate_and_encode_dream_submission,
};
use crate::{DREAM_INPUT_MANIFEST_MEDIA_TYPE, ExperienceClaimDocument, ExperienceClaimOrigin};

#[test]
fn writers_publish_musteragent_dream_contracts_and_readers_accept_frozen_legacy_contracts() {
    assert!(DREAM_MODEL_INPUT_MEDIA_TYPE.contains("musteragent.dream"));
    assert!(DREAM_MODEL_OUTPUT_MEDIA_TYPE.contains("musteragent.dream"));
    assert!(is_dream_model_input_schema(
        "musteragent.legacy.dream-model-input.v2"
    ));
    assert!(is_dream_model_output_schema(
        "musteragent.legacy.dream-model-output.v1"
    ));
    assert!(is_dream_model_input_media_type(
        "application/vnd.musteragent.legacy.dream-model-input.v2+json"
    ));
    assert!(is_dream_model_output_media_type(
        "application/vnd.musteragent.legacy.dream-model-output.v1+json"
    ));
}

#[test]
fn strict_model_output_derives_scope_and_provenance_from_frozen_signals() {
    let fixture = fixture();
    let response = serde_json::to_vec(&json!({
        "schema":DREAM_MODEL_OUTPUT_SCHEMA,
        "proposals":[{
            "kind":"preference",
            "claim":"When changing this service, preserve bounded queues.",
            "conditions":{"component":"runtime"},
            "outcome":"avoids overload regressions",
            "confidence_bps":8200,
            "signal_ids":[fixture.input.signals[0].signal_id.to_string()]
        }]
    }))
    .expect("model output");
    let proposed_by = PrincipalId::generate();
    let drafts = parse_model_output(&fixture.input, &fixture.receipt, &response, proposed_by)
        .expect("strict output accepted");
    assert_eq!(drafts.len(), 1);
    let draft = &drafts[0];
    assert_eq!(draft.proposed_by, proposed_by);
    assert_eq!(
        draft.target_scope,
        musteragent_evolution::EvolutionScope::Agent {
            agent_id: fixture.input.job.target_agent_id
        }
    );
    assert_eq!(
        draft.source_run_ids,
        BTreeSet::from([fixture.source_run_id])
    );
    assert_eq!(
        draft.evidence_record_ids,
        BTreeSet::from([fixture.record_id])
    );
    let claim = ExperienceClaimDocument::decode(&draft.claim_bytes).expect("claim document");
    assert_eq!(claim.origin, ExperienceClaimOrigin::DreamEvidence);
    assert_eq!(
        claim.metadata["model_run_id"],
        json!(fixture.receipt.maintenance_run_id.to_string())
    );
    assert_eq!(
        claim.metadata["model_invocation_id"],
        json!(fixture.receipt.invocation_id.to_string())
    );
}

#[test]
fn private_submission_tool_owns_the_output_envelope_and_validates_evidence() {
    let fixture = fixture();
    let model_input = bounded_model_input_bytes(
        &fixture.input,
        BTreeMap::from([(
            fixture.record_id,
            DreamEvidenceMaterial {
                record_id: fixture.record_id,
                payload_hash: Sha256::digest(b"evidence"),
                excerpt: "The user explicitly asked to preserve bounded queues.".to_owned(),
                truncated: false,
            },
        )]),
        1,
    )
    .expect("frozen input");
    let arguments = canonical_json(json!({
        "proposals":[{
            "kind":"preference",
            "claim":"Preserve bounded queues when changing this service.",
            "conditions":{"component":"runtime"},
            "outcome":"avoids overload regressions",
            "confidence_bps":8200,
            "signal_ids":[fixture.input.signals[0].signal_id.to_string()]
        }]
    }));
    let encoded = validate_and_encode_dream_submission(&model_input, &arguments)
        .expect("tool arguments accepted");
    let output: serde_json::Value = serde_json::from_slice(&encoded).expect("canonical output");
    assert_eq!(output["schema"], json!(DREAM_MODEL_OUTPUT_SCHEMA));
    assert_eq!(output["proposals"].as_array().expect("proposals").len(), 1);

    let incompatible = canonical_json(json!({
        "proposals":[{
            "kind":"procedure",
            "claim":"Treat a preference as a procedure.",
            "conditions":{},
            "outcome":"unsafe",
            "confidence_bps":5000,
            "signal_ids":[fixture.input.signals[0].signal_id.to_string()]
        }]
    }));
    let error = validate_and_encode_dream_submission(&model_input, &incompatible)
        .expect_err("procedure must reject user-instruction evidence");
    assert_eq!(
        error,
        DreamSubmissionValidationError::IncompatibleEvidence {
            proposal_index: 0,
            signal_id: fixture.input.signals[0].signal_id.to_string(),
            signal_kind: fixture.input.signals[0].kind,
        }
    );
    assert!(error.feedback().contains("Candidate 1"));
    assert_eq!(error.diagnostic()["code"], json!("incompatible_evidence"));
    assert_eq!(
        error.diagnostic()["allowed_candidate_kinds"],
        json!(["preference", "semantic", "instinct", "persona_patch"])
    );
}

#[test]
fn dream_contract_rejects_an_unconditional_instinct_at_both_ingress_boundaries() {
    let fixture = fixture();
    let model_input = bounded_model_input_bytes(
        &fixture.input,
        BTreeMap::from([(
            fixture.record_id,
            DreamEvidenceMaterial {
                record_id: fixture.record_id,
                payload_hash: Sha256::digest(b"evidence"),
                excerpt: "The user sent a short status ping.".to_owned(),
                truncated: false,
            },
        )]),
        1,
    )
    .expect("frozen input");
    let proposal = json!({
        "kind":"instinct",
        "claim":"Reply concisely to a minimal ping.",
        "conditions":{},
        "outcome":"avoid unnecessary verbosity",
        "confidence_bps":8200,
        "signal_ids":[fixture.input.signals[0].signal_id.to_string()]
    });
    let arguments = canonical_json(json!({"proposals":[proposal.clone()]}));
    assert_eq!(
        validate_and_encode_dream_submission(&model_input, &arguments),
        Err(DreamSubmissionValidationError::InvalidArguments),
    );

    let response = serde_json::to_vec(&json!({
        "schema":DREAM_MODEL_OUTPUT_SCHEMA,
        "proposals":[proposal],
    }))
    .expect("model output");
    assert_eq!(
        parse_model_output(
            &fixture.input,
            &fixture.receipt,
            &response,
            PrincipalId::generate(),
        ),
        Err(ModelDreamError::InvalidOutput),
    );
}

#[test]
fn model_projection_is_bounded_without_weakening_durable_provenance() {
    let fixture = fixture();
    let span = &fixture.input.signals[0].evidence[0];
    let projected = model_evidence_json(span, &BTreeSet::from([fixture.record_id]));
    assert_eq!(projected["record_count"], json!(1));
    assert!(projected.get("records_digest").is_some());
    assert!(projected.get("record_ids").is_none());
    assert!(projected.get("record_hashes").is_none());
    assert_eq!(
        projected["selected_record_ids"],
        json!([fixture.record_id.to_string()])
    );

    let records = (0..101)
        .map(|_| LedgerRecordId::generate())
        .collect::<Vec<_>>();
    let selected = bounded_record_selection(&records, 5);
    assert_eq!(
        selected,
        [
            records[0],
            records[25],
            records[50],
            records[75],
            records[100]
        ]
    );

    let oversized = (0..24)
        .map(|_| {
            let record_id = LedgerRecordId::generate();
            (
                record_id,
                DreamEvidenceMaterial {
                    record_id,
                    payload_hash: Sha256::digest(b"evidence"),
                    excerpt: "x".repeat(4 * 1024),
                    truncated: true,
                },
            )
        })
        .collect();
    let mut constrained_input = fixture.input.clone();
    constrained_input.job.workload.evidence_context_tokens = 16_384;
    let request = bounded_model_input_bytes(&constrained_input, oversized, 4_049)
        .expect("oversized evidence projection is reduced to budget");
    assert!(request.len() <= constrained_input.job.workload.evidence_context_tokens as usize * 4);
    let document: serde_json::Value = serde_json::from_slice(&request).expect("model input");
    assert_eq!(document["evidence_projection"]["total_record_count"], 4_049);
    let selected_count = document["evidence_projection"]["selected_record_count"]
        .as_u64()
        .expect("selected record count");
    assert_eq!(selected_count, 24);
    assert_eq!(
        document["records"].as_array().expect("records").len(),
        selected_count as usize
    );
    assert!(
        document["records"]
            .as_array()
            .expect("records")
            .iter()
            .all(|record| record["truncated"] == json!(true)
                && record["excerpt"].as_str().expect("excerpt").len() < 4 * 1024)
    );
}

#[test]
fn invented_or_kind_incompatible_signal_selection_fails_closed() {
    let fixture = fixture();
    let invented = serde_json::to_vec(&json!({
        "schema":DREAM_MODEL_OUTPUT_SCHEMA,
        "proposals":[{
            "kind":"preference",
            "claim":"invented",
            "conditions":{},
            "outcome":"unsafe",
            "confidence_bps":5000,
            "signal_ids":[LearningSignalId::generate().to_string()]
        }]
    }))
    .expect("invented output");
    assert_eq!(
        parse_model_output(
            &fixture.input,
            &fixture.receipt,
            &invented,
            PrincipalId::generate()
        ),
        Err(ModelDreamError::InventedEvidence)
    );

    let incompatible = serde_json::to_vec(&json!({
        "schema":DREAM_MODEL_OUTPUT_SCHEMA,
        "proposals":[{
            "kind":"procedure",
            "claim":"misclassified preference",
            "conditions":{},
            "outcome":"unsafe",
            "confidence_bps":5000,
            "signal_ids":[fixture.input.signals[0].signal_id.to_string()]
        }]
    }))
    .expect("incompatible output");
    assert_eq!(
        parse_model_output(
            &fixture.input,
            &fixture.receipt,
            &incompatible,
            PrincipalId::generate()
        ),
        Err(ModelDreamError::IncompatibleEvidence)
    );
}

#[test]
fn completion_receipt_cannot_reuse_source_run_or_cross_the_lease_deadline() {
    let fixture = fixture();
    let maintenance_conversation_id = dream_maintenance_conversation_id(
        fixture.input.job.tenant_id,
        fixture.input.job.target_agent_id,
    )
    .expect("conversation identity");
    let request = DreamModelRunRequest {
        dream_job_id: fixture.input.job.dream_job_id,
        tenant_id: fixture.input.job.tenant_id,
        target_agent_id: fixture.input.job.target_agent_id,
        agent_version_id: fixture.input.job.agent_version_id,
        policy_version_ref: fixture.input.job.policy_version_ref.clone(),
        growth_profile_ref: fixture.input.job.growth_profile_ref.clone(),
        growth_profile_hash: fixture.input.job.growth_profile_hash,
        maintenance_conversation_id,
        maintenance_service_participant_id: dream_maintenance_service_participant_id(
            maintenance_conversation_id,
        )
        .expect("service Participant identity"),
        maintenance_agent_participant_id: dream_maintenance_agent_participant_id(
            maintenance_conversation_id,
            fixture.input.job.target_agent_id,
        )
        .expect("Agent Participant identity"),
        maintenance_command_id: dream_maintenance_command_id(fixture.input.job.dream_job_id)
            .expect("command identity"),
        maintenance_activity_id: dream_maintenance_activity_id(fixture.input.job.dream_job_id)
            .expect("activity identity"),
        maintenance_run_id: dream_maintenance_run_id(fixture.input.job.dream_job_id)
            .expect("run identity"),
        maintenance_runtime_snapshot_id: dream_maintenance_runtime_snapshot_id(
            fixture.input.job.dream_job_id,
        )
        .expect("Snapshot identity"),
        invocation_id: dream_maintenance_invocation_id(fixture.input.job.dream_job_id)
            .expect("invocation identity"),
        input_manifest_ref: fixture.input.job.input_manifest_ref.clone(),
        model_input_ref: content_ref(DREAM_MODEL_INPUT_MEDIA_TYPE, b"input"),
        model_route_ref: fixture.input.job.model_route_ref.clone(),
        model_route_hash: fixture.input.job.model_route_hash,
        workload: fixture.input.job.workload,
        source_run_ids: BTreeSet::from([fixture.source_run_id]),
        deadline: fixture.input.job.lease_expires_at.expect("lease"),
    };
    let mut receipt = fixture.receipt;
    receipt.model_input_ref = request.model_input_ref.clone();
    receipt.maintenance_run_id = fixture.source_run_id;
    assert_eq!(
        receipt.validate_for(&request),
        Err(DreamModelRunError::InvalidReceipt)
    );
    receipt.maintenance_run_id = request.maintenance_run_id;
    receipt.completed_at =
        UtcTimestamp::from_unix_millis(request.deadline.unix_millis().saturating_add(1));
    assert_eq!(
        receipt.validate_for(&request),
        Err(DreamModelRunError::InvalidReceipt)
    );

    receipt.completed_at = request.deadline;
    receipt.activity_kind = ActivityKind::Chat;
    assert_eq!(
        receipt.validate_for(&request),
        Err(DreamModelRunError::InvalidReceipt)
    );
    receipt.activity_kind = ActivityKind::Maintenance;
    receipt.invocation_purpose = ModelInvocationPurpose::EvolutionEvaluation;
    assert_eq!(
        receipt.validate_for(&request),
        Err(DreamModelRunError::InvalidReceipt)
    );
    receipt.invocation_purpose = ModelInvocationPurpose::EvolutionInduction;
    receipt.route_role = ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Compression);
    assert_eq!(
        receipt.validate_for(&request),
        Err(DreamModelRunError::InvalidReceipt)
    );
}

#[test]
fn maintenance_execution_identities_are_stable_and_job_scoped() {
    let fixture = fixture();
    let job_id = fixture.input.job.dream_job_id;
    let conversation_id = dream_maintenance_conversation_id(
        fixture.input.job.tenant_id,
        fixture.input.job.target_agent_id,
    )
    .expect("conversation identity");
    assert_eq!(
        dream_maintenance_conversation_id(
            fixture.input.job.tenant_id,
            fixture.input.job.target_agent_id,
        ),
        dream_maintenance_conversation_id(
            fixture.input.job.tenant_id,
            fixture.input.job.target_agent_id,
        )
    );
    assert_eq!(
        dream_maintenance_command_id(job_id),
        dream_maintenance_command_id(job_id)
    );
    assert_eq!(
        dream_maintenance_activity_id(job_id),
        dream_maintenance_activity_id(job_id)
    );
    assert_eq!(
        dream_maintenance_run_id(job_id),
        dream_maintenance_run_id(job_id)
    );
    assert_eq!(
        dream_maintenance_invocation_id(job_id),
        dream_maintenance_invocation_id(job_id)
    );
    let command_id = dream_maintenance_command_id(job_id).expect("Command identity");
    let admission = musteragent_runtime_admission::AdmissionIdentities::derive_legacy(command_id)
        .expect("frozen Dream admission identities");
    assert_eq!(
        dream_maintenance_activity_id(job_id).expect("Activity identity"),
        admission.activity_id
    );
    assert_eq!(
        dream_maintenance_run_id(job_id).expect("Run identity"),
        admission.run_id
    );
    assert_eq!(
        dream_maintenance_runtime_snapshot_id(job_id).expect("Snapshot identity"),
        admission.runtime_snapshot_id
    );
    assert_eq!(
        dream_maintenance_service_participant_id(conversation_id),
        dream_maintenance_service_participant_id(conversation_id)
    );
    assert_eq!(
        dream_maintenance_agent_participant_id(conversation_id, fixture.input.job.target_agent_id,),
        dream_maintenance_agent_participant_id(conversation_id, fixture.input.job.target_agent_id,)
    );
    assert_ne!(
        dream_maintenance_service_participant_id(conversation_id)
            .expect("service Participant identity"),
        dream_maintenance_agent_participant_id(conversation_id, fixture.input.job.target_agent_id,)
            .expect("Agent Participant identity")
    );

    let other_job = DreamJobId::generate();
    assert_ne!(
        dream_maintenance_run_id(job_id),
        dream_maintenance_run_id(other_job)
    );
    assert_ne!(
        dream_maintenance_invocation_id(job_id),
        dream_maintenance_invocation_id(other_job)
    );
}

struct Fixture {
    input: DreamLearningInput,
    receipt: CompletedDreamModelRun,
    source_run_id: RunId,
    record_id: LedgerRecordId,
}

fn fixture() -> Fixture {
    let tenant_id = TenantId::generate();
    let target_agent_id = AgentId::generate();
    let source_run_id = RunId::generate();
    let record_id = LedgerRecordId::generate();
    let record_hash = Sha256::digest(b"record");
    let evidence = EvidenceSpan {
        conversation_id: musteragent_identity::ConversationId::generate(),
        run_id: source_run_id,
        branch_id: None,
        branch_state: BranchEvidenceState::Active,
        cursor_from: LedgerOffset::new(1),
        cursor_to: LedgerOffset::new(2),
        record_ids: vec![record_id],
        record_hashes: vec![record_hash],
        terminal: RunTerminalEvidence::Completed,
        outcome: LearningOutcome::Success,
    };
    let mut signal = LearningSignal {
        signal_id: LearningSignalId::generate(),
        tenant_id,
        target_agent_id,
        kind: LearningSignalKind::UserCorrection,
        evidence: vec![evidence],
        extractor_version_ref: VersionedKey::new("musteragent.learning.extractor@2")
            .expect("extractor"),
        policy_version_ref: VersionedKey::new("musteragent.learning.policy@2").expect("policy"),
        dedup_key: Sha256::digest([]),
        created_at: UtcTimestamp::from_unix_millis(1_000),
    };
    signal.dedup_key = signal.canonical_dedup_key();
    let lease_expires_at = UtcTimestamp::from_unix_millis(20_000);
    let mut job = DreamJob {
        dream_job_id: DreamJobId::generate(),
        tenant_id,
        target_agent_id,
        agent_version_id: AgentVersionId::generate(),
        memory_collection_id: MemoryCollectionId::generate(),
        memory_collection_version: VersionNumber::new(1).expect("version"),
        ledger_watermark: LedgerOffset::new(2),
        input_manifest_ref: content_ref(DREAM_INPUT_MANIFEST_MEDIA_TYPE, b"manifest"),
        policy_version_ref: VersionedKey::new("musteragent.dream.policy@2").expect("policy"),
        growth_profile_ref: VersionedKey::new("musteragent.growth-profile@2")
            .expect("growth profile"),
        growth_profile_hash: Sha256::digest(b"musteragent.growth-profile@2"),
        manual_request_generation: 0,
        model_route_ref: VersionedKey::new("musteragent.dream.model@2").expect("route"),
        model_route_hash: Sha256::digest(b"musteragent.dream.model@2"),
        model_timeout_ms: 10_000,
        workload: DreamWorkload {
            evidence_context_tokens: 32_768,
            candidate_batch_capacity: 32,
        },
        idempotency_key: Sha256::digest([]),
        status: DreamJobStatus::Running,
        retry_count: 0,
        lease_owner: Some("dream-worker".to_owned()),
        lease_expires_at: Some(lease_expires_at),
        created_at: UtcTimestamp::from_unix_millis(1_000),
        completed_at: None,
    };
    job.idempotency_key = job.canonical_idempotency_key();
    let model_input_ref = content_ref(DREAM_MODEL_INPUT_MEDIA_TYPE, b"input");
    let receipt = CompletedDreamModelRun {
        dream_job_id: job.dream_job_id,
        tenant_id,
        maintenance_conversation_id: dream_maintenance_conversation_id(tenant_id, target_agent_id)
            .expect("conversation identity"),
        maintenance_command_id: dream_maintenance_command_id(job.dream_job_id)
            .expect("command identity"),
        maintenance_activity_id: dream_maintenance_activity_id(job.dream_job_id)
            .expect("activity identity"),
        maintenance_run_id: dream_maintenance_run_id(job.dream_job_id).expect("run identity"),
        maintenance_runtime_snapshot_id: dream_maintenance_runtime_snapshot_id(job.dream_job_id)
            .expect("Snapshot identity"),
        attempt_id: AttemptId::generate(),
        invocation_id: dream_maintenance_invocation_id(job.dream_job_id)
            .expect("invocation identity"),
        model_input_ref,
        model_output_ref: content_ref(DREAM_MODEL_OUTPUT_MEDIA_TYPE, b"output"),
        model_route_ref: job.model_route_ref.clone(),
        activity_kind: ActivityKind::Maintenance,
        run_status: RunStatus::Completed,
        invocation_purpose: ModelInvocationPurpose::EvolutionInduction,
        route_role: ModelInvocationRouteRole::Auxiliary(AuxiliaryModelRole::Evolution),
        completed_at: UtcTimestamp::from_unix_millis(10_000),
    };
    Fixture {
        input: DreamLearningInput {
            job,
            signals: vec![signal],
        },
        receipt,
        source_run_id,
        record_id,
    }
}

fn content_ref(media_type: &str, bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

fn canonical_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(value) => JsonValue::Bool(value),
        Value::Number(value) => value.as_i64().map_or_else(
            || JsonValue::Number(value.as_f64().expect("finite number")),
            JsonValue::Integer,
        ),
        Value::String(value) => JsonValue::String(value),
        Value::Array(values) => JsonValue::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(values) => JsonValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}
