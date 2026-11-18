//! Logical source identity is independent from physical recovery generations.
use crate::ContextPlanError;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::DialogueContextCut;
use musteragent_identity::{CorrelationId, DialogueContextCheckpointId, InvocationId, RunId};

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub enum CompactionSource {
    #[default]
    RuntimeLedger,
    /// Authenticated ControlConsumed record that superseded an already-sealed input.
    AfterControl(musteragent_identity::LedgerRecordId),
    DialogueHistory {
        coverage: DialogueContextCut,
        parent_checkpoint_id: Option<DialogueContextCheckpointId>,
    },
}
impl CompactionSource {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        if let Self::DialogueHistory { coverage, .. } = self {
            coverage
                .validate()
                .map_err(|_| ContextPlanError::invariant())?;
            if coverage.through.seq == 0 {
                return Err(ContextPlanError::invariant());
            }
        }
        Ok(())
    }
    /// The original Home compares the authenticated current consumption against
    /// the first sealed runtime slot before allowing a new physical effect.
    pub fn matches_superseding_control(
        &self,
        cut: Option<crate::ContextPlanControlCut>,
        cutoff: musteragent_identity::ConversationSeq,
    ) -> bool {
        match (self, cut) {
            (Self::RuntimeLedger, None) => true,
            (Self::AfterControl(id), Some(control)) => {
                *id == control.record_id && cutoff == control.sequence
            }
            _ => false,
        }
    }
    // Exact, length-framed source identity. Keep RuntimeLedger's historical
    // key and invocation IDs unchanged across this additive extension.
    pub fn key(&self) -> String {
        match self {
            Self::RuntimeLedger => "runtime-ledger".to_owned(),
            Self::AfterControl(id) => format!("control:{id}"),
            Self::DialogueHistory {
                coverage: c,
                parent_checkpoint_id,
            } => {
                let mut bytes = b"musteragent.dialogue-compaction-source.v1\0".to_vec();
                for value in [
                    c.tenant_id.to_string(),
                    c.owner_id.to_string(),
                    c.agent_id.to_string(),
                    c.dialogue_id.to_string(),
                ] {
                    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                }
                bytes.extend_from_slice(&c.through.epoch.to_be_bytes());
                bytes.extend_from_slice(&c.through.seq.to_be_bytes());
                bytes.extend_from_slice(&c.source_chain_sha256.into_bytes());
                if let Some(id) = parent_checkpoint_id {
                    bytes.push(1);
                    bytes.extend_from_slice(id.into_uuid().as_bytes());
                } else {
                    bytes.push(0);
                }
                format!("dialogue_history:{}", Sha256::digest(bytes))
            }
        }
    }
    pub(crate) fn generation_ids(
        &self,
        run: RunId,
        iteration: u32,
        generation: std::num::NonZeroU32,
    ) -> Result<(InvocationId, CorrelationId), ContextPlanError> {
        let (invocation, correlation) =
            crate::iteration_identity::compaction_generation_ids(run, iteration, generation)?;
        if matches!(self, Self::RuntimeLedger) {
            return Ok((invocation, correlation));
        }
        let key = self.key();
        Ok((
            InvocationId::from_uuid(uuid::Uuid::new_v5(&invocation.into_uuid(), key.as_bytes()))
                .map_err(|_| ContextPlanError::invariant())?,
            CorrelationId::from_uuid(uuid::Uuid::new_v5(&correlation.into_uuid(), key.as_bytes()))
                .map_err(|_| ContextPlanError::invariant())?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_execution_context::DialogueCursor;
    use musteragent_identity::{AgentDialogueId, AgentId, PrincipalId, TenantId};
    #[test]
    fn stripping_or_forging_a_superseding_control_cannot_replay_an_old_paid_request() {
        use crate::ContextPlanControlCut;
        use musteragent_identity::{ConversationSeq, LedgerRecordId};
        use musteragent_run::RunControlKind;
        let cut = ContextPlanControlCut {
            sequence: ConversationSeq::new(9).unwrap(),
            record_id: LedgerRecordId::generate(),
            kind: RunControlKind::Steering,
        };
        let mut request = crate::model_compaction::tests::request();
        let original = request.slot.plan_fingerprint;
        let source = CompactionSource::AfterControl(cut.record_id);
        let oldest = CompactionSource::RuntimeLedger;
        let unrelated = CompactionSource::AfterControl(LedgerRecordId::generate());
        assert!(source.matches_superseding_control(Some(cut), cut.sequence));
        assert!(!oldest.matches_superseding_control(Some(cut), cut.sequence));
        assert!(!unrelated.matches_superseding_control(Some(cut), cut.sequence));
        assert!(!source.matches_superseding_control(None, cut.sequence));
        assert!(!source.matches_superseding_control(Some(cut), ConversationSeq::new(8).unwrap()));
        let run = request.invocation.run_context.run_id;
        let generation = std::num::NonZeroU32::MIN;
        assert_ne!(
            source.generation_ids(run, 1, generation).unwrap(),
            oldest.generation_ids(run, 1, generation).unwrap()
        );
        assert_ne!(
            source.generation_ids(run, 1, generation).unwrap(),
            unrelated.generation_ids(run, 1, generation).unwrap()
        );
        request.slot.source = source.clone();
        request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
        assert!(
            request.validate().is_err(),
            "a new source cannot borrow an older invocation"
        );
        (
            request.invocation.invocation_id,
            request.invocation.correlation_id,
        ) = source
            .generation_ids(run, request.slot.loop_iteration, request.slot.generation)
            .unwrap();
        request.slot.plan_fingerprint = request.expected_plan_fingerprint().unwrap();
        assert_ne!(original, request.slot.plan_fingerprint);
        request.validate().unwrap();
        // A reader predating the new optional protobuf field would decode this
        // otherwise identical message as RuntimeLedger. The signed seal fails.
        request.slot.source = oldest;
        assert!(request.validate().is_err());
    }
    #[test]
    fn logical_prefixes_and_parents_are_distinct_without_changing_runtime_identities() {
        let run = RunId::generate();
        let generation = std::num::NonZeroU32::MIN;
        assert_eq!(
            CompactionSource::RuntimeLedger
                .generation_ids(run, 1, generation)
                .unwrap(),
            crate::iteration_identity::compaction_generation_ids(run, 1, generation).unwrap()
        );
        let cut = DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 64 },
            entry_count: 64,
            source_chain_sha256: Sha256::digest(b"prefix"),
        };
        let source = CompactionSource::DialogueHistory {
            coverage: cut.clone(),
            parent_checkpoint_id: None,
        };
        source.validate().unwrap();
        let original = source.generation_ids(run, 1, generation).unwrap();
        assert_ne!(
            original,
            CompactionSource::RuntimeLedger
                .generation_ids(run, 1, generation)
                .unwrap()
        );
        assert_eq!(source.key().len(), 81);
        let parent = CompactionSource::DialogueHistory {
            coverage: cut.clone(),
            parent_checkpoint_id: Some(DialogueContextCheckpointId::generate()),
        };
        assert_ne!(source.key(), parent.key());
        assert_ne!(original, parent.generation_ids(run, 1, generation).unwrap());
        let mut later = cut;
        later.through.seq = 128;
        later.entry_count = 128;
        later.source_chain_sha256 = Sha256::digest(b"next prefix");
        let later = CompactionSource::DialogueHistory {
            coverage: later,
            parent_checkpoint_id: None,
        };
        assert_ne!(source.key(), later.key());
        assert_ne!(original, later.generation_ids(run, 1, generation).unwrap());
        assert_ne!(
            original,
            source
                .generation_ids(run, 1, std::num::NonZeroU32::new(2).unwrap())
                .unwrap()
        );
    }
}
