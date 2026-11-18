use super::*;
use musteragent_run::ResumeRun;

impl RuntimeApplication {
    pub(super) async fn resume_run_inner(
        &self,
        command: CommandEnvelope<ResumeRun>,
    ) -> Result<CommandAck, RuntimeError> {
        command.validate().map_err(|_| invalid_resume(&command))?;
        if command.metadata.actor.principal_kind != musteragent_identity::PrincipalKind::User {
            return Err(invalid_resume(&command));
        }
        let guard = command
            .metadata
            .conversation_guard
            .ok_or_else(|| invalid_resume(&command))?;
        let command_fingerprint = resume_run_command_fingerprint(&command);
        if let Some(receipt) = self
            .load_guarded_replay(&command, command_fingerprint)
            .await?
        {
            return Ok(duplicate_ack(
                command.metadata.command_id,
                receipt,
                vec![ResourceRef::Run(command.payload.run_id)],
            ));
        }
        let authority = self
            .store
            .authorize_run_control(
                command.metadata.actor,
                command.payload.run_id,
                command.payload.participant_id,
                guard.expected_home_epoch,
                command.metadata.correlation_id,
            )
            .await?;
        if authority.conversation_id != guard.conversation_id
            || authority.home_epoch != guard.expected_home_epoch
        {
            return Err(invalid_resume(&command));
        }
        let now = self.clock.now();
        if now > command.metadata.deadline {
            return Err(runtime_error(
                ErrorCode::DependencyTimeout,
                command.metadata.correlation_id,
                true,
                "Resume command deadline elapsed",
            ));
        }
        let record = domain_record(
            self.encoder.as_ref(),
            RecordIdentity {
                record_id: create_command_record_id(&command, 0)
                    .ok_or_else(|| invariant(&command))?,
                activity_id: Some(authority.activity_id),
                run_id: Some(authority.run_id),
                run_seq: Some(
                    RunSeq::new(
                        authority
                            .last_run_sequence
                            .get()
                            .checked_add(1)
                            .ok_or_else(|| invariant(&command))?,
                    )
                    .map_err(|_| invariant(&command))?,
                ),
                participant_id: Some(command.payload.participant_id),
            },
            DomainEvent::Run(RunEvent::RecoveryRequested {
                expected_attempt_number: command.payload.expected_attempt_number,
                action: command.payload.action,
                requested_by: command.metadata.actor.principal_id,
            }),
            now,
            command.metadata.correlation_id,
        )
        .map_err(|code| build_error(code, &command))?;
        let dispatch = format!("run:{}", command.payload.run_id).into_bytes();
        let commit = crate::RunResumeCommit {
            actor: command.metadata.actor,
            request: command.payload,
            activity_id: authority.activity_id,
            expected_last_run_sequence: authority.last_run_sequence,
            append: AppendRequest {
                tenant_id: authority.tenant_id,
                conversation_id: authority.conversation_id,
                expected_home_epoch: authority.home_epoch,
                expected_offset: guard
                    .expected_conversation_seq
                    .map_or(authority.current_offset, |seq| LedgerOffset::new(seq.get())),
                idempotency_key: command.metadata.idempotency_key.clone(),
                command_fingerprint,
                records: vec![record],
                outbox: vec![ProposedOutbox {
                    message_id: create_command_outbox_id(&command, 0)
                        .ok_or_else(|| invariant(&command))?,
                    topic: DottedKey::new("run.dispatch").map_err(|_| invariant(&command))?,
                    payload_hash: Sha256::digest(&dispatch),
                    payload: dispatch,
                }],
            },
        };
        commit
            .validate()
            .map_err(|code| build_error(code, &command))?;
        let receipt = self
            .store
            .resume_run(command.metadata.correlation_id, commit)
            .await?;
        Ok(CommandAck {
            command_id: command.metadata.command_id,
            disposition: match receipt.disposition {
                AppendDisposition::Committed => CommandDisposition::Committed,
                AppendDisposition::Duplicate => CommandDisposition::Duplicate,
            },
            committed_at: receipt.committed_at,
            first_conversation_seq: Some(receipt.first_sequence),
            last_conversation_seq: Some(receipt.last_sequence),
            resource_refs: vec![ResourceRef::Run(command.payload.run_id)],
        })
    }
}

fn invalid_resume(command: &CommandEnvelope<ResumeRun>) -> RuntimeError {
    runtime_error(
        ErrorCode::InvalidCommand,
        command.metadata.correlation_id,
        false,
        "Resume requires the current guarded repair wait",
    )
}
