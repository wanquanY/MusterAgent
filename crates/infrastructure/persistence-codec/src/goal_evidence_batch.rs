//! Immutable batch identity from the original Goal context Ledger fact.
use crate::wire;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_identity::{GoalId, GoalPursuitId};
use prost::Message;

#[derive(Debug, thiserror::Error)]
#[error("invalid Goal evidence batch")]
pub struct GoalEvidenceBatchError;

pub struct GoalEvidenceBatch {
    pub fingerprint: Sha256,
    pub inputs: Vec<ContentRef>,
}

pub fn decode(
    bytes: &[u8],
    hash: &[u8],
) -> Result<Option<GoalEvidenceBatch>, GoalEvidenceBatchError> {
    if Sha256::digest(bytes).into_bytes().as_slice() != hash {
        return Err(GoalEvidenceBatchError);
    }
    let event = wire::DomainEvent::decode(bytes).map_err(|_| GoalEvidenceBatchError)?;
    let Some(wire::domain_event::Event::StandingGoalContextProjected(mut value)) = event.event
    else {
        return Ok(None);
    };
    let _: GoalId = value
        .goal_id
        .as_ref()
        .ok_or(GoalEvidenceBatchError)?
        .value
        .parse()
        .map_err(|_| GoalEvidenceBatchError)?;
    let _: GoalPursuitId = value
        .pursuit_id
        .as_ref()
        .ok_or(GoalEvidenceBatchError)?
        .value
        .parse()
        .map_err(|_| GoalEvidenceBatchError)?;
    crate::wire_common::decode_content_ref(
        value.objective_ref.clone().ok_or(GoalEvidenceBatchError)?,
    )
    .map_err(|_| GoalEvidenceBatchError)?;
    if value.input_refs.is_empty() {
        return Ok(None);
    }
    if value.input_refs.len() > usize::from(musteragent_goal::MAX_GOAL_INPUT_BATCH) {
        return Err(GoalEvidenceBatchError);
    }
    let inputs = value
        .input_refs
        .iter()
        .cloned()
        .map(crate::wire_common::decode_content_ref)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| GoalEvidenceBatchError)?;
    // The directive changes between processing rounds; source authority does not.
    value.previous_evaluation_summary_ref = None;
    Ok(Some(GoalEvidenceBatch {
        fingerprint: Sha256::digest(value.encode_to_vec()),
        inputs,
    }))
}
