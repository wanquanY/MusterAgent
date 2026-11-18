//! The same original-admission replay assertions run against both real stores.

use std::fmt::Debug;

use musteragent_identity::{
    AgentId, DottedKey, IdempotencyKey, MemoryId, MemoryVersionId, PrincipalId, PrincipalKind,
    TenantId, UtcTimestamp,
};
use musteragent_runtime_privacy::{
    AgentMemoryErasureAdmission, AgentMemoryErasureAdmissionStore, ErasureAppendDisposition,
    RequestAgentMemoryErasure,
};

pub async fn assert_replay<S>(
    store: &S,
    command: &RequestAgentMemoryErasure,
    expected: &AgentMemoryErasureAdmission,
    at: i64,
) where
    S: AgentMemoryErasureAdmissionStore,
    S::Error: Debug,
{
    let replay = store
        .request_agent_memory_erasure(RequestAgentMemoryErasure {
            requested_at: UtcTimestamp::from_unix_millis(at),
            complete_by: UtcTimestamp::from_unix_millis(at + 2_592_000_000),
            ..command.clone()
        })
        .await
        .expect("recover original admission after service-clock advance");
    assert_eq!(replay.disposition, ErasureAppendDisposition::Duplicate);
    assert_eq!(replay.lineage, expected.lineage);
    // Includes the original deadline, plan hashes, roots, transitions and any
    // terminal receipt. A duplicate must not update or append any of them.
    assert_eq!(replay.job, expected.job);
}

pub async fn assert_conflicting_intents<S>(store: &S, command: &RequestAgentMemoryErasure)
where
    S: AgentMemoryErasureAdmissionStore,
    S::Error: Debug,
{
    let mutations: [fn(&mut RequestAgentMemoryErasure); 10] = [
        |c| {
            c.tenant_id = TenantId::generate();
            c.requested_by.tenant_id = c.tenant_id;
        },
        |c| c.requested_by.principal_id = PrincipalId::generate(),
        |c| c.requested_by.principal_kind = PrincipalKind::Service,
        |c| c.agent_id = AgentId::generate(),
        |c| c.memory_id = MemoryId::generate(),
        |c| c.expected_memory_version_id = MemoryVersionId::generate(),
        |c| c.reason_code = DottedKey::new("user.different-reason").expect("reason"),
        |c| c.idempotency_key = IdempotencyKey::new("erase:other-intent").expect("key"),
        |c| c.complete_by = c.requested_at,
        |c| c.requested_by.tenant_id = TenantId::generate(),
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut retry = command.clone();
        mutate(&mut retry);
        assert!(
            store.request_agent_memory_erasure(retry).await.is_err(),
            "{index}"
        );
    }
}
