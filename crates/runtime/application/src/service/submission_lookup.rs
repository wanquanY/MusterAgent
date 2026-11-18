//! Canonical receipt inspection never verifies a new grant or admits work.
use super::*;

impl RuntimeApplication {
    /// Inspect an original RetryRun admission, without a new signature, grant
    /// verification or dispatch. Absence is not a fence against a racing owner.
    pub async fn lookup_retry_receipt(
        &self,
        command: &CommandEnvelope<RetryRun>,
        selection: &AdmissionSelection,
    ) -> Result<Option<CommandAck>, RuntimeError> {
        command.validate().map_err(|_| invalid_retry(command))?;
        self.home_policy
            .validate_execution_scope(
                command.metadata.actor.tenant_id,
                command.payload.execution_scope,
            )
            .map_err(|_| invalid_retry(command))?;
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid_retry(command))?;
        let view = self
            .get_conversation(GetConversation {
                actor: command.metadata.actor,
                conversation_id: guard.conversation_id,
            })
            .await?;
        if view.conversation.control_home != self.home_policy.control_home()
            || view.conversation.home_epoch != guard.expected_home_epoch
            || !selection.requested_skill_refs.is_empty()
        {
            return Err(invalid_retry(command));
        }
        let ids = admission_identities_for_command(command).ok_or_else(|| invariant(command))?;
        let replay = self
            .store
            .load_admission_replay(
                CommandReplayQuery {
                    tenant_id: command.metadata.actor.tenant_id,
                    conversation_id: guard.conversation_id,
                    home_epoch: guard.expected_home_epoch,
                    idempotency_key: command.metadata.idempotency_key.clone(),
                    command_fingerprint: retry_run_command_fingerprint(command, selection),
                },
                ids.runtime_snapshot_id,
                command.metadata.correlation_id,
            )
            .await?;
        if replay.is_some() {
            self.store
                .get_conversation(command.metadata.actor, guard.conversation_id)
                .await?;
        }
        Ok(replay.map(|replay| {
            duplicate_ack(
                command.metadata.command_id,
                replay.ledger,
                vec![
                    ResourceRef::Activity(ids.activity_id),
                    ResourceRef::Run(ids.run_id),
                ],
            )
        }))
    }

    /// Inspect the exact original submission under current conversation access.
    /// `None` is only an observation, NOT a cancellation/fence. A native owner
    /// releasing delivery custody must exclude and join all submitters itself.
    pub async fn lookup_submission_receipt(
        &self,
        command: &CommandEnvelope<SubmitMessage>,
        selection: &AdmissionSelection,
    ) -> Result<Option<CommandAck>, RuntimeError> {
        self.lookup_message_admission_receipt(command, selection, None)
            .await
    }

    /// Inspect the original combined admission; never activate a Draft or
    /// redispatch a message. A later Goal deletion does not erase this receipt.
    pub async fn lookup_goal_start_receipt(
        &self,
        command: &CommandEnvelope<SubmitMessage>,
        selection: &AdmissionSelection,
        start: crate::StandingGoalStart,
    ) -> Result<Option<CommandAck>, RuntimeError> {
        start.validate().map_err(|_| invalid_submit(command))?;
        self.lookup_message_admission_receipt(command, selection, Some(start))
            .await
    }

    async fn lookup_message_admission_receipt(
        &self,
        command: &CommandEnvelope<SubmitMessage>,
        selection: &AdmissionSelection,
        start: Option<crate::StandingGoalStart>,
    ) -> Result<Option<CommandAck>, RuntimeError> {
        command.validate().map_err(|_| invalid_submit(command))?;
        command
            .payload
            .validate()
            .map_err(|_| invalid_submit(command))?;
        self.home_policy
            .validate_execution_scope(
                command.metadata.actor.tenant_id,
                command.payload.execution_scope,
            )
            .map_err(|_| invalid_submit(command))?;
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid_submit(command))?;
        let view = self
            .get_conversation(GetConversation {
                actor: command.metadata.actor,
                conversation_id: guard.conversation_id,
            })
            .await?;
        if view.conversation.control_home != self.home_policy.control_home()
            || view.conversation.home_epoch != guard.expected_home_epoch
        {
            return Err(invalid_submit(command));
        }
        let fingerprint = submit_command_fingerprint(command, selection);
        self.load_submission_receipt_with_fingerprint(
            command,
            start.map_or(fingerprint, |start| start.command_fingerprint(fingerprint)),
        )
        .await
    }

    pub(super) async fn load_submission_receipt_with_fingerprint(
        &self,
        command: &CommandEnvelope<SubmitMessage>,
        command_fingerprint: Sha256,
    ) -> Result<Option<CommandAck>, RuntimeError> {
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid_submit(command))?;
        let ids = admission_identities_for_command(command).ok_or_else(|| invariant(command))?;
        let replay = self
            .store
            .load_admission_replay(
                CommandReplayQuery {
                    tenant_id: command.metadata.actor.tenant_id,
                    conversation_id: guard.conversation_id,
                    home_epoch: guard.expected_home_epoch,
                    idempotency_key: command.metadata.idempotency_key.clone(),
                    command_fingerprint,
                },
                ids.runtime_snapshot_id,
                command.metadata.correlation_id,
            )
            .await?;
        if replay.is_some() {
            self.store
                .get_conversation(command.metadata.actor, guard.conversation_id)
                .await?;
        }
        Ok(replay.map(|replay| {
            admission_ack(
                command.metadata.command_id,
                command.payload.message_id,
                ids,
                replay.ledger,
            )
        }))
    }
}
