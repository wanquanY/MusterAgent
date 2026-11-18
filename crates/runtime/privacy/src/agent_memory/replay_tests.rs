use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, PrincipalId, PrincipalKind};

use super::*;
use crate::{ErasureJobStatus, ErasureTransition, ErasureTransitionSequence};

fn fixture() -> (
    RequestAgentMemoryErasure,
    AgentMemoryErasureLineage,
    ErasureJob,
) {
    let tenant_id = TenantId::generate();
    let command = RequestAgentMemoryErasure {
        tenant_id,
        requested_by: Actor {
            tenant_id,
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        },
        agent_id: AgentId::generate(),
        memory_id: MemoryId::generate(),
        expected_memory_version_id: MemoryVersionId::generate(),
        reason_code: DottedKey::new("user.permanent-forget-requested")
            .expect("valid erasure fixture"),
        idempotency_key: IdempotencyKey::new("erase:original-intent")
            .expect("valid erasure fixture"),
        requested_at: UtcTimestamp::from_unix_millis(100),
        complete_by: UtcTimestamp::from_unix_millis(10_100),
    };
    let content = |bytes: &[u8]| ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("text/plain").expect("valid erasure fixture"),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    };
    let lineage = AgentMemoryErasureLineage {
        tenant_id,
        erasure_job_id: ErasureJobId::generate(),
        agent_id: command.agent_id,
        memory_id: command.memory_id,
        memory_version_id: command.expected_memory_version_id,
        content_ref: content(b"current"),
        related_content_refs: vec![content(b"historical")],
        requested_by: command.requested_by,
        reason_code: command.reason_code.clone(),
        frozen_at: command.requested_at,
    };
    let (request, plan) =
        build_agent_memory_erasure_facts(&command, &lineage).expect("valid erasure fixture");
    let job = ErasureJob {
        request_fingerprint: request.fingerprint().expect("valid erasure fixture"),
        request,
        plans: vec![plan],
        transitions: [
            ErasureJobStatus::PendingPlan,
            ErasureJobStatus::Planned,
            ErasureJobStatus::Fenced,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, status)| ErasureTransition {
            sequence: ErasureTransitionSequence::new(index as u64 + 1)
                .expect("valid erasure fixture"),
            status,
            occurred_at: command.requested_at,
        })
        .collect(),
        receipt: None,
    };
    job.validate().expect("valid erasure fixture");
    (command, lineage, job)
}

#[test]
fn same_intent_recovers_original_facts_without_renewing_deadlines() {
    let (command, lineage, job) = fixture();
    let original = (lineage.clone(), job.clone());
    for (requested_at, complete_by) in [(0, 50), (150, 10_150), (50_000, 60_000)] {
        let retry = RequestAgentMemoryErasure {
            requested_at: UtcTimestamp::from_unix_millis(requested_at),
            complete_by: UtcTimestamp::from_unix_millis(complete_by),
            ..command.clone()
        };
        assert_eq!(retry.matches_admitted_intent(&job, &lineage), Ok(true));
        assert_eq!((&lineage, &job), (&original.0, &original.1));
    }
}

#[test]
fn every_user_intent_field_remains_bound() {
    let (command, lineage, job) = fixture();
    let mutations: [fn(&mut RequestAgentMemoryErasure); 8] = [
        |c| {
            c.tenant_id = TenantId::generate();
            c.requested_by.tenant_id = c.tenant_id;
        },
        |c| c.requested_by.principal_id = PrincipalId::generate(),
        |c| c.requested_by.principal_kind = PrincipalKind::Service,
        |c| c.agent_id = AgentId::generate(),
        |c| c.memory_id = MemoryId::generate(),
        |c| c.expected_memory_version_id = MemoryVersionId::generate(),
        |c| c.reason_code = DottedKey::new("user.different-reason").expect("valid erasure fixture"),
        |c| {
            c.idempotency_key =
                IdempotencyKey::new("erase:different-intent").expect("valid erasure fixture")
        },
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut retry = command.clone();
        mutate(&mut retry);
        assert_eq!(
            retry.matches_admitted_intent(&job, &lineage),
            Ok(false),
            "{index}"
        );
    }
}

#[test]
fn replay_does_not_bypass_command_validation() {
    let (mut command, lineage, job) = fixture();
    command.complete_by = command.requested_at;
    assert_eq!(
        command.matches_admitted_intent(&job, &lineage),
        Err(ErasureContractError::InvalidCompletionDeadline)
    );
    command.requested_by.tenant_id = TenantId::generate();
    assert_eq!(
        command.matches_admitted_intent(&job, &lineage),
        Err(ErasureContractError::ActorTenantMismatch)
    );
}

#[test]
fn mixed_persisted_lineages_fail_closed_instead_of_replaying() {
    let (command, lineage, job) = fixture();
    let mutations: [fn(&mut AgentMemoryErasureLineage); 12] = [
        |l| l.tenant_id = TenantId::generate(),
        |l| l.erasure_job_id = ErasureJobId::generate(),
        |l| l.requested_by.tenant_id = TenantId::generate(),
        |l| l.requested_by.principal_id = PrincipalId::generate(),
        |l| l.requested_by.principal_kind = PrincipalKind::System,
        |l| l.agent_id = AgentId::generate(),
        |l| l.memory_id = MemoryId::generate(),
        |l| l.memory_version_id = MemoryVersionId::generate(),
        |l| l.reason_code = DottedKey::new("user.different-reason").expect("valid erasure fixture"),
        |l| l.frozen_at = UtcTimestamp::from_unix_millis(90),
        |l| l.content_ref.sha256 = Sha256::digest(b"different"),
        |l| l.related_content_refs.clear(),
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut mixed = lineage.clone();
        mutate(&mut mixed);
        assert_eq!(
            command.matches_admitted_intent(&job, &mixed),
            Err(ErasureContractError::PlanLineageMismatch),
            "{index}"
        );
    }
}

fn rehash_plan(plan: &ErasurePlanSnapshot) -> ErasurePlanSnapshot {
    ErasurePlanSnapshot::new(
        plan.schema_version.clone(),
        plan.erasure_job_id,
        plan.plan_version,
        plan.root_resource_refs.clone(),
        plan.data_classes.clone(),
        plan.retention_exceptions.clone(),
        plan.lineage_fingerprint,
        plan.frozen_at,
    )
    .expect("valid erasure fixture")
}

#[test]
fn recomputed_hashes_cannot_hide_cross_record_identity_or_root_drift() {
    let (command, lineage, job) = fixture();
    let mutations: [fn(&mut ErasureJob); 5] = [
        |j| j.request.requested_at = UtcTimestamp::from_unix_millis(90),
        |j| {
            j.request.tenant_id = TenantId::generate();
            j.request.requested_by.tenant_id = j.request.tenant_id;
        },
        |j| j.request.requested_by.principal_id = PrincipalId::generate(),
        |j| j.plans[0].lineage_fingerprint = Sha256::digest(b"mixed-lineage"),
        |j| {
            j.request.root_resource_refs.remove(0);
            j.plans[0].root_resource_refs = j.request.root_resource_refs.clone();
        },
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut mixed = job.clone();
        mutate(&mut mixed);
        mixed.request_fingerprint = mixed.request.fingerprint().expect("valid erasure fixture");
        mixed.plans[0] = rehash_plan(&mixed.plans[0]);
        mixed.validate().expect("individually valid persisted job");
        assert_eq!(
            command.matches_admitted_intent(&mixed, &lineage),
            Err(ErasureContractError::PlanLineageMismatch),
            "{index}"
        );
    }
}

#[test]
fn corrupted_or_missing_job_facts_are_not_duplicates() {
    let (command, lineage, job) = fixture();
    let mut corrupt = job.clone();
    corrupt.request_fingerprint = Sha256::digest(b"bad");
    assert_eq!(
        command.matches_admitted_intent(&corrupt, &lineage),
        Err(ErasureContractError::RequestFingerprintMismatch)
    );
    let mut corrupt = job.clone();
    corrupt.plans[0].plan_hash = Sha256::digest(b"bad");
    assert_eq!(
        command.matches_admitted_intent(&corrupt, &lineage),
        Err(ErasureContractError::PlanHashMismatch)
    );
    let mut corrupt = job;
    corrupt.plans.clear();
    assert_eq!(
        command.matches_admitted_intent(&corrupt, &lineage),
        Err(ErasureContractError::PlanRequired)
    );
}

#[test]
fn generic_request_fingerprints_still_bind_exact_admission_times() {
    let (_, _, job) = fixture();
    let mut request = job.request.clone();
    request.requested_at = UtcTimestamp::from_unix_millis(101);
    assert_ne!(
        request.fingerprint().expect("valid erasure fixture"),
        job.request_fingerprint
    );
    let mut request = job.request;
    request.complete_by = UtcTimestamp::from_unix_millis(10_101);
    assert_ne!(
        request.fingerprint().expect("valid erasure fixture"),
        job.request_fingerprint
    );
}
