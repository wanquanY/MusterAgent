//! Closed public projection; no private grant, claim or device identifiers.
use crate::wire;
use musteragent_execution_context::{
    DialogueAdmissionBlocker as Blocker, DialogueAdmissionProgress, DialogueAdmissionStage as Stage,
};
use musteragent_identity::UtcTimestamp;
pub fn encode(v: &DialogueAdmissionProgress) -> wire::DialogueAdmissionProgress {
    wire::DialogueAdmissionProgress {
        intent_id: Some(wire::UserIntentId {
            value: v.intent_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: v.agent_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: v.dialogue_id.to_string(),
        }),
        entry_id: Some(wire::DialogueEntryId {
            value: v.entry_id.to_string(),
        }),
        revision: v.revision,
        stage: match v.stage {
            Stage::WaitingDevice => 1,
            Stage::ConfirmationPending => 2,
            Stage::Admitted => 3,
            Stage::Failed => 4,
            Stage::Cancelled => 5,
        },
        blocker: match v.blocker {
            None => 0,
            Some(Blocker::AuthorizationCheckFailed) => 1,
            Some(Blocker::DependencyUnavailable) => 2,
            Some(Blocker::SourceChanged) => 3,
            Some(Blocker::RuntimeContractRejected) => 4,
        },
        observed_at_ms: v.observed_at.unix_millis(),
        execution_finished: v.execution_finished,
    }
}
pub fn decode(
    v: wire::DialogueAdmissionProgress,
) -> Result<DialogueAdmissionProgress, &'static str> {
    let value = DialogueAdmissionProgress {
        intent_id: crate::dialogue_codec::id(v.intent_id.map(|v| v.value)).map_err(|_| "intent")?,
        agent_id: crate::dialogue_codec::id(v.agent_id.map(|v| v.value)).map_err(|_| "agent")?,
        dialogue_id: crate::dialogue_codec::id(v.dialogue_id.map(|v| v.value))
            .map_err(|_| "dialogue")?,
        entry_id: crate::dialogue_codec::id(v.entry_id.map(|v| v.value)).map_err(|_| "entry")?,
        revision: v.revision,
        stage: match v.stage {
            1 => Stage::WaitingDevice,
            2 => Stage::ConfirmationPending,
            3 => Stage::Admitted,
            4 => Stage::Failed,
            5 => Stage::Cancelled,
            _ => return Err("stage"),
        },
        blocker: match v.blocker {
            0 => None,
            1 => Some(Blocker::AuthorizationCheckFailed),
            2 => Some(Blocker::DependencyUnavailable),
            3 => Some(Blocker::SourceChanged),
            4 => Some(Blocker::RuntimeContractRejected),
            _ => return Err("blocker"),
        },
        observed_at: UtcTimestamp::from_unix_millis(v.observed_at_ms),
        execution_finished: v.execution_finished,
    };
    if !value.valid() {
        return Err("progress");
    }
    Ok(value)
}
