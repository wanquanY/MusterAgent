use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ContextSnapshotId, CorrelationId, InvocationId, MessageId, OutputId, RunId,
};
use std::num::NonZeroU32;
use uuid::Uuid;

use crate::{ContextPlanError, ContextPlanInput, StoredContextPlanIdentity};

pub(crate) struct IterationIds {
    generation: NonZeroU32,
    pub snapshot_id: ContextSnapshotId,
    pub invocation_id: InvocationId,
    pub assistant_message_id: MessageId,
    pub assistant_output_id: OutputId,
    pub reasoning_output_id: OutputId,
    pub correlation_id: CorrelationId,
    legacy_assistant_message_id: MessageId,
}

impl IterationIds {
    pub(crate) fn derive(
        input: &ContextPlanInput,
        assistant_response_ordinal: NonZeroU32,
        generation: NonZeroU32,
    ) -> Result<Self, ContextPlanError> {
        if generation.get() > i32::MAX as u32 {
            return Err(ContextPlanError::invariant());
        }
        let id = |tag: &[u8]| {
            deterministic_run_iteration_uuid(
                input.attempt.run_id,
                input.next_iteration,
                generation.get(),
                tag,
            )
        };
        let legacy_assistant_message_id = MessageId::from_uuid(deterministic_run_uuid(
            input.attempt.run_id,
            b"assistant-message",
        ))
        .map_err(|_| ContextPlanError::invariant())?;
        Ok(Self {
            generation,
            snapshot_id: ContextSnapshotId::from_uuid(id(b"context-snapshot"))
                .map_err(|_| ContextPlanError::invariant())?,
            invocation_id: InvocationId::from_uuid(id(b"model-invocation"))
                .map_err(|_| ContextPlanError::invariant())?,
            // A Run can consume multiple durable inputs (for example a parent
            // Agent follow-up or an asynchronous ChildReport). One response
            // turn owns one visible Assistant Message, while all physical
            // tool/model iterations inside that turn share it. Ordinal one
            // deliberately preserves the original identity.
            assistant_message_id: MessageId::from_uuid(assistant_message_uuid(
                input.attempt.run_id,
                assistant_response_ordinal,
            ))
            .map_err(|_| ContextPlanError::invariant())?,
            assistant_output_id: OutputId::from_uuid(id(b"assistant-output"))
                .map_err(|_| ContextPlanError::invariant())?,
            reasoning_output_id: OutputId::from_uuid(id(b"reasoning-output"))
                .map_err(|_| ContextPlanError::invariant())?,
            correlation_id: CorrelationId::from_uuid(id(b"correlation"))
                .map_err(|_| ContextPlanError::invariant())?,
            legacy_assistant_message_id,
        })
    }

    pub(crate) fn validate_stored(
        &self,
        run_id: RunId,
        iteration: u32,
        stored: &StoredContextPlanIdentity,
    ) -> Result<(), ContextPlanError> {
        if stored.run_id != run_id
            || stored.iteration != iteration
            || stored.generation != self.generation
            || stored.snapshot_id != self.snapshot_id
            || stored.invocation_id != self.invocation_id
            || (stored.assistant_message_id != self.assistant_message_id
                // A one-message-per-Run plan may already be in flight during
                // a rolling upgrade. Honor its immutable identity; the
                // persistence compatibility path publishes it without a
                // duplicate visible Message.
                && stored.assistant_message_id != self.legacy_assistant_message_id)
            || stored.assistant_output_id != self.assistant_output_id
            || stored.reasoning_output_id != self.reasoning_output_id
        {
            return Err(ContextPlanError::idempotency_conflict());
        }
        Ok(())
    }
}

fn assistant_message_uuid(run_id: RunId, response_ordinal: NonZeroU32) -> Uuid {
    if response_ordinal.get() == 1 {
        deterministic_run_uuid(run_id, b"assistant-message")
    } else {
        derived_uuid(&[
            run_id.into_uuid().as_bytes(),
            b"assistant-message-response",
            &response_ordinal.get().to_be_bytes(),
        ])
    }
}

fn deterministic_run_uuid(run_id: RunId, tag: &[u8]) -> Uuid {
    derived_uuid(&[run_id.into_uuid().as_bytes(), tag])
}

fn deterministic_run_iteration_uuid(
    run_id: RunId,
    iteration: u32,
    generation: u32,
    tag: &[u8],
) -> Uuid {
    derived_uuid(&[
        run_id.into_uuid().as_bytes(),
        &iteration.to_be_bytes(),
        &generation.to_be_bytes(),
        tag,
    ])
}

fn derived_uuid(parts: &[&[u8]]) -> Uuid {
    let bytes = parts.concat();
    let digest = Sha256::digest(bytes).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x80;
    value[8] = (value[8] & 0x3f) | 0x80;
    Uuid::from_bytes(value)
}

pub(crate) fn compaction_generation_ids(
    run_id: RunId,
    iteration: u32,
    generation: NonZeroU32,
) -> Result<(InvocationId, CorrelationId), ContextPlanError> {
    Ok((
        InvocationId::from_uuid(deterministic_run_iteration_uuid(
            run_id,
            iteration,
            generation.get(),
            b"context-compaction",
        ))
        .map_err(|_| ContextPlanError::invariant())?,
        CorrelationId::from_uuid(deterministic_run_iteration_uuid(
            run_id,
            iteration,
            generation.get(),
            b"context-compaction-correlation",
        ))
        .map_err(|_| ContextPlanError::invariant())?,
    ))
}

#[cfg(test)]
mod tests {
    use musteragent_identity::RunId;
    use std::num::NonZeroU32;

    use crate::CONTEXT_PLAN_GENERATION;

    use super::{assistant_message_uuid, deterministic_run_iteration_uuid, deterministic_run_uuid};

    #[test]
    fn visible_assistant_message_identity_is_stable_per_response_turn() {
        let run_id = RunId::generate();
        assert_eq!(
            assistant_message_uuid(run_id, NonZeroU32::new(1).expect("ordinal")),
            deterministic_run_uuid(run_id, b"assistant-message"),
        );
        assert_ne!(
            assistant_message_uuid(run_id, NonZeroU32::new(1).expect("ordinal")),
            assistant_message_uuid(run_id, NonZeroU32::new(2).expect("ordinal")),
        );
        assert_eq!(
            assistant_message_uuid(run_id, NonZeroU32::new(2).expect("ordinal")),
            assistant_message_uuid(run_id, NonZeroU32::new(2).expect("ordinal")),
        );
        assert_ne!(
            assistant_message_uuid(run_id, NonZeroU32::new(2).expect("ordinal")),
            assistant_message_uuid(RunId::generate(), NonZeroU32::new(2).expect("ordinal"),),
        );
    }

    #[test]
    fn first_generation_keeps_frozen_legacy_identity_bytes() {
        let run = RunId::from_uuid(
            uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
        )
        .unwrap();
        for (tag, expected) in [
            ("context-snapshot", "0bd4c4a9-ec27-8d7e-a4d8-c0127af7785f"),
            ("model-invocation", "83586a0c-71f5-8de4-a73f-6e31a4fd6ee0"),
            ("assistant-output", "3a92a06e-ba40-83ab-b3fa-af4696b70b6f"),
            ("reasoning-output", "f541f3e4-1661-834c-8328-955d5512822e"),
            ("correlation", "296d3356-c2ca-895c-ad6d-1302de1e0f2b"),
        ] {
            let legacy = uuid::Uuid::parse_str(expected).unwrap();
            assert_eq!(
                deterministic_run_iteration_uuid(run, 2, 1, tag.as_bytes()),
                legacy
            );
            assert_ne!(
                deterministic_run_iteration_uuid(run, 2, 2, tag.as_bytes()),
                legacy
            );
        }
    }

    #[test]
    fn compaction_identity_is_stable_per_run_iteration_and_distinct_across_iterations() {
        let run_id = RunId::generate();
        assert_eq!(
            deterministic_run_iteration_uuid(
                run_id,
                3,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
            deterministic_run_iteration_uuid(
                run_id,
                3,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
        );
        assert_ne!(
            deterministic_run_iteration_uuid(
                run_id,
                3,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
            deterministic_run_iteration_uuid(
                run_id,
                4,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
        );
        assert_ne!(
            deterministic_run_iteration_uuid(
                run_id,
                3,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
            deterministic_run_iteration_uuid(
                RunId::generate(),
                3,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
        );
        assert_ne!(
            deterministic_run_iteration_uuid(
                run_id,
                3,
                CONTEXT_PLAN_GENERATION.get(),
                b"context-compaction",
            ),
            deterministic_run_iteration_uuid(
                run_id,
                3,
                CONTEXT_PLAN_GENERATION.get() + 1,
                b"context-compaction",
            ),
        );
    }
}
