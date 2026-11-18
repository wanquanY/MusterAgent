//! Tool input/approval waits compose with the original Interaction owner.
use super::*;
pub(super) struct JournalInteractionRequest {
    pub(super) interaction_id: InteractionId,
    pub(super) invocation_id: InvocationId,
    pub(super) kind: InteractionKind,
    pub(super) prompt_ref: ContentRef,
    pub(super) response_schema: SchemaRef,
    pub(super) requested_at: UtcTimestamp,
    pub(super) expires_at: Option<UtcTimestamp>,
}

impl PostgresFencedToolInvocationJournal {
    pub(super) async fn request_interaction_inner(
        &self,
        attempt: &AttemptContext,
        request: JournalInteractionRequest,
    ) -> Result<DurableToolInvocation, ToolCommitError> {
        let (mut transaction, state) = self.begin_commit_lane(attempt).await?;
        let current = self
            .load_invocation_on(&mut transaction, state.tenant_id, request.invocation_id)
            .await?
            .ok_or(ToolCommitError::NotFound)?;
        if current.invocation.status == ToolInvocationStatus::WaitingInteraction {
            if current.invocation.interaction_id == Some(request.interaction_id) {
                return Ok(current);
            }
            return Err(ToolCommitError::IdempotencyConflict);
        }
        let valid_source = current.invocation.status == ToolInvocationStatus::Requested
            || (request.kind != InteractionKind::Approval
                && current.invocation.status == ToolInvocationStatus::Running);
        if !valid_source || request.invocation_id != current.invocation.invocation_id {
            return Err(ToolCommitError::InvalidTransition);
        }
        if request.response_schema.schema_key.as_str()
            == musteragent_runtime_interaction::CAPABILITY_RESPONSE_SCHEMA
        {
            let prior: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM approval.interactions WHERE tenant_id=$1 AND conversation_id=$2 AND response_schema_key=$3 AND (response_schema_hash=$4 OR status=2) AND interaction_id<>$5)")
                .bind(state.tenant_id.into_uuid()).bind(state.conversation_id.into_uuid()).bind(request.response_schema.schema_key.as_str()).bind(request.response_schema.schema_hash.into_bytes().to_vec()).bind(request.interaction_id.into_uuid()).fetch_one(&mut *transaction).await?;
            if prior {
                return Err(ToolCommitError::CapabilityAlreadyRequested);
            }
        }
        let interaction = Interaction {
            interaction_id: request.interaction_id,
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            activity_id: state.activity_id,
            run_id: attempt.run_id,
            requested_by_invocation_id: request.invocation_id,
            kind: request.kind,
            status: InteractionStatus::Pending,
            prompt_ref: request.prompt_ref.clone(),
            response_schema: request.response_schema.clone(),
            response_ref: None,
            expires_at: request.expires_at,
            requested_at: request.requested_at,
            responded_by: None,
            responded_at: None,
        };
        let mut events = vec![
            DomainEvent::Interaction(InteractionEvent::Requested {
                interaction_id: request.interaction_id,
                invocation_id: request.invocation_id,
                kind: request.kind,
                prompt_ref: request.prompt_ref,
                response_schema: request.response_schema,
                expires_at: request.expires_at,
            }),
            DomainEvent::Tool(ToolEvent::InvocationWaitingInteraction {
                invocation_id: request.invocation_id,
                interaction_id: request.interaction_id,
            }),
            DomainEvent::Run(RunEvent::AttemptSuspended {
                attempt_number: attempt.attempt_number,
                interaction_id: Some(request.interaction_id),
                invocation_id: Some(request.invocation_id),
            }),
            DomainEvent::Run(RunEvent::Waiting {
                reason: if matches!(
                    request.kind,
                    InteractionKind::Approval | InteractionKind::Confirmation
                ) {
                    RunWaitReason::Approval {
                        interaction_id: request.interaction_id,
                    }
                } else {
                    RunWaitReason::Input {
                        interaction_id: request.interaction_id,
                    }
                },
                resume_not_before: None,
            }),
        ];
        let mut records = self.records(
            &state,
            attempt,
            request.invocation_id,
            request.requested_at,
            "approval",
            events.clone(),
        )?;
        let liveness = self
            .prepare_liveness_wait(
                &mut transaction,
                &state,
                attempt,
                &current.invocation,
                &records[3],
            )
            .await?;
        if let Some(change) = &liveness {
            events.push(change.event());
            records = self.records(
                &state,
                attempt,
                request.invocation_id,
                request.requested_at,
                "approval",
                events,
            )?;
        }
        let semantic_records = InteractionSemanticRecords {
            interaction_record_id: records[0].record_id,
            tool_record_id: Some(records[1].record_id),
            run_record_id: Some(records[3].record_id),
        };
        let append = state.append(format!("tool:approval:{}", request.invocation_id), records)?;
        let store = PostgresInteractionStore::with_security_audit(
            self.pool.clone(),
            Arc::clone(&self.reference_keys),
            Arc::clone(&self.decoder),
            Arc::clone(&self.audit),
        );
        if let Some(change) = &liveness {
            change
                .persist(&mut transaction)
                .await
                .map_err(ToolCommitError::Liveness)?;
        }
        store
            .request_in_transaction(
                transaction,
                RequestInteractionCommit {
                    interaction,
                    fence: attempt_fence(attempt, state.home_epoch),
                    expected_last_run_sequence: state.last_run_sequence,
                    append,
                    semantic_records,
                },
            )
            .await?;
        self.required_invocation(state.tenant_id, request.invocation_id)
            .await
    }
}
