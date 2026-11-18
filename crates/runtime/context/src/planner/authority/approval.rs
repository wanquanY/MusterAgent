//! Runtime-owned lifecycle facts, appended outside all narrative compaction.
use super::*;
use musteragent_execution_context::DialogueApprovalContext;
pub(in crate::planner) fn require_cut(
    v: &DialogueApprovalContext,
    cut: Option<&musteragent_execution_context::DialogueContextCut>,
) -> Result<(), ContextPlanError> {
    v.validate().map_err(|_| ContextPlanError::invariant())?;
    if cut != Some(&v.frozen_cut) {
        return Err(ContextPlanError::invariant());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::Sha256;
    use musteragent_execution_context::*;
    use musteragent_execution_context::{
        DialogueApprovalContextKind as Kind, DialogueApprovalContextState as State,
    };
    use musteragent_identity::*;
    #[test]
    fn original_approval_facts_require_the_exact_prepared_public_cut() {
        let cut = DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 2 },
            entry_count: 2,
            source_chain_sha256: Sha256::digest(b"source"),
        };
        let mut context = DialogueApprovalContext {
            frozen_cut: cut.clone(),
            from_seq: 1,
            observations: vec![DialogueApprovalObservation {
                entry_id: DialogueEntryId::generate(),
                sequence: 2,
                interaction_id: InteractionId::generate(),
                run_id: RunId::generate(),
                visible_revision: std::num::NonZeroU64::new(2).unwrap(),
                control_revision: Sha256::digest(b"denied"),
                kind: Kind::Approval,
                state: State::Denied,
            }],
        };
        assert!(require_cut(&context, Some(&cut)).is_ok());
        assert!(require_cut(&context, None).is_err());
        context.frozen_cut.agent_id = AgentId::generate();
        assert!(require_cut(&context, Some(&cut)).is_err());
    }
}
