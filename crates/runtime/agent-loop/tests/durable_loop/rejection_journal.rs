use super::*;
impl FixtureJournal {
    pub(super) fn finish_action_rejection(
        &self,
        commit: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            let batches = self.batches();
            let durable_events = batches
                .iter()
                .filter(|batch| batch.invocation_id == commit.observation.invocation_id)
                .flat_map(|batch| &batch.events)
                .collect::<Vec<_>>();
            let durable_public_output = durable_events.iter().any(|event| event.is_public_output());
            let durable_replay_barrier = durable_events
                .iter()
                .any(|event| event.blocks_automatic_replay());
            let durable_identity = durable_events.iter().find_map(|event| match event {
                ModelStreamEvent::ResponseStarted {
                    response_ref,
                    response_model_key,
                } => Some((response_ref, response_model_key)),
                _ => None,
            });
            if commit.observation.replay_barrier_observed != durable_replay_barrier
                || commit.observation.public_output_observed != durable_public_output
                || commit.observation.provider_response_ref.as_ref()
                    != durable_identity.map(|(response_ref, _)| response_ref)
                || commit.observation.response_model_key.as_ref()
                    != durable_identity.map(|(_, model_key)| model_key)
            {
                return Err(port_error(ErrorCode::InternalInvariantViolated, false));
            }
            let mut checkpoint = lock(&self.checkpoint);
            if !matches!(
                checkpoint.phase,
                LoopPhase::ModelInFlight { invocation_id }
                    if invocation_id == commit.observation.invocation_id
            ) {
                return Err(port_error(ErrorCode::InternalInvariantViolated, false));
            }
            checkpoint.phase = if commit.rejection.terminates_delivery_episode() {
                LoopPhase::Failed
            } else {
                LoopPhase::ReadyForModel
            };
            checkpoint.consecutive_empty_responses = 0;
            checkpoint.continuation_ref = None;
            checkpoint.continuation_source_invocation_id = None;
            lock(&self.action_rejections).push(commit);
            Ok(checkpoint.clone())
        })
    }
}
