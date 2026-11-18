use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::{AgentDialogueId, AgentId, PrincipalId, ProductId, TenantId};

#[test]
fn goal_subject_projects_broad_account_grants_without_interactive_authority() {
    let policy = crate::DIALOGUE_DISCOVERY_POLICY;
    for key in [
        "capability.request@2",
        "schedules.list@1",
        "agents.self.inspect@1",
        "tasks.handoff@1",
        "agents.create@1",
        "goal.create@1",
    ] {
        assert!(!owner_goal_capability_available(policy, key), "{key}");
    }
    for key in [
        "agenda.todo.create@1",
        "agenda.get@1",
        "tasks.executions.list@2",
    ] {
        assert!(owner_goal_capability_available(policy, key), "{key}");
        assert!(!owner_goal_capability_available(
            crate::PRIVATE_EVALUATION_DISCOVERY_POLICY_V2,
            key
        ));
    }
}

fn authority() -> OwnerGoalSnapshotAuthority {
    let mut cut = DialogueContextCut {
        tenant_id: TenantId::generate(),
        owner_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
        dialogue_id: AgentDialogueId::generate(),
        through: DialogueCursor { epoch: 1, seq: 0 },
        entry_count: 0,
        source_chain_sha256: Sha256::digest([]),
    };
    cut.source_chain_sha256 = cut.empty_chain_hash();
    OwnerGoalSnapshotAuthority {
        goal_id: GoalId::generate(),
        pursuit_id: GoalPursuitId::generate(),
        binding_revision: 1,
        permission_revision: 1,
        manifest_revision: 1,
        instructions_revision: 1,
        context_cut: cut,
    }
}

#[test]
fn forward_authority_cannot_be_smuggled_into_old_or_foreign_snapshot_subjects() {
    let value = authority();
    let agent = value.context_cut.agent_id;
    assert!(
        validate_binding(
            Some(&value),
            SNAPSHOT_SCHEMA_VERSION,
            agent,
            Some(ProductId::KERNEL)
        )
        .is_ok()
    );
    // Optional goal authority must never be read under an unsupported schema.
    assert!(
        validate_binding(
            Some(&value),
            "unsupported.snapshot.v1",
            agent,
            Some(ProductId::KERNEL)
        )
        .is_err()
    );
    assert!(
        validate_binding(
            None,
            SNAPSHOT_SCHEMA_VERSION,
            agent,
            Some(ProductId::KERNEL)
        )
        .is_ok()
    );
    assert!(
        validate_binding(
            Some(&value),
            SNAPSHOT_SCHEMA_VERSION,
            AgentId::generate(),
            Some(ProductId::KERNEL)
        )
        .is_err()
    );
    assert!(validate_binding(Some(&value), SNAPSHOT_SCHEMA_VERSION, agent, None).is_err());
}

#[test]
fn per_run_ceiling_rejects_invalid_revisions_content_and_foreign_context_cut() {
    let original = authority();
    for revision in [0, i64::MAX as u64 + 1] {
        let mut value = original.clone();
        value.permission_revision = revision;
        assert!(value.validate().is_err());
    }
    let mut value = original.clone();
    value.context_cut.owner_id = PrincipalId::generate();
    assert!(
        value.validate().is_err(),
        "empty dialogue cut is bound to its exact owner"
    );
}
