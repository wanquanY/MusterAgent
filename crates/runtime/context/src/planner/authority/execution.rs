//! Read-only cross-execution ownership, refreshed by original qualified owners.
//! This stays outside every narrative summary and confers no action authority.
use super::*;
use musteragent_execution_context::*;
use musteragent_identity::RunId;
pub(in crate::planner) fn require_scope(
    v: &DialogueExecutionContext,
    cut: Option<&DialogueContextCut>,
    run: RunId,
) -> Result<(), ContextPlanError> {
    v.validate().map_err(|_| ContextPlanError::invariant())?;
    if cut != Some(&v.frozen_cut) || v.current.run_id != run {
        return Err(ContextPlanError::invariant());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::Sha256;
    use musteragent_identity::*;
    #[test]
    fn original_execution_observation_cannot_reassign_a_different_run_or_cut() {
        let own = DialogueExecutionObservation {
            source: musteragent_execution_context::DialogueExecutionSource::Intent {
                intent_id: UserIntentId::generate(),
                task_id: None,
            },
            run_id: RunId::generate(),
            input: None,
            accepted: DialogueCursor { epoch: 1, seq: 1 },
            phase: DialogueExecutionContextPhase::Running,
            committed_publications: 0,
            committed_results: 0,
            committed_artifact_cards: 0,
        };
        let mut peer = own.clone();
        peer.run_id = RunId::generate();
        peer.source = musteragent_execution_context::DialogueExecutionSource::Intent {
            intent_id: UserIntentId::generate(),
            task_id: None,
        };
        peer.phase = DialogueExecutionContextPhase::WaitingApproval;
        // The status question may publish three sample files while the actual
        // test still waits. The new Run's committed cards cannot verify its peer.
        let mut own = own;
        own.committed_publications = 3;
        own.committed_artifact_cards = 3;
        let v = DialogueExecutionContext {
            reply_binding: None,

            frozen_cut: DialogueContextCut {
                tenant_id: TenantId::generate(),
                owner_id: PrincipalId::generate(),
                agent_id: AgentId::generate(),
                dialogue_id: AgentDialogueId::generate(),
                through: DialogueCursor { epoch: 1, seq: 1 },
                entry_count: 1,
                source_chain_sha256: Sha256::digest(b"fixed"),
            },
            current: own,
            active: vec![peer],
            active_truncated: false,
            recent_terminal: vec![],
            recent_terminal_truncated: false,
        };
        assert!(require_scope(&v, Some(&v.frozen_cut), v.current.run_id).is_ok());
        assert!(require_scope(&v, Some(&v.frozen_cut), v.active[0].run_id).is_err());
        assert!(require_scope(&v, None, v.current.run_id).is_err());
    }
}
