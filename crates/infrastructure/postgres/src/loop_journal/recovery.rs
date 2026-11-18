mod provider_round;
use super::*;

enum InflightModelTermination {
    Abandoned,
    Cancelled(Option<ModelInvocationObservation>),
    Failed(ModelFailureCommit),
    // Only recovery, after permanent validation failure of the stored result,
    // may reject a sealed completion. Ordinary provider errors cannot do so.
    InvalidCompletion(ModelFailureCommit),
    ActionRejected(Box<ModelActionRejectionCommit>),
}

struct InflightClosePlan {
    observation: Option<ModelInvocationObservation>,
    event: ModelEvent,
    status: i16,
    terminal_error: Option<Vec<u8>>,
    partial_reason: musteragent_conversation::PartialMessageReason,
    transition: InflightCloseTransition,
    cancelled: bool,
    action_rejection: Option<ModelActionRejection>,
}

#[derive(Debug, Copy, Clone)]
enum InflightCloseTransition {
    ContinueAttempt {
        next_route_ordinal: Option<u32>,
    },
    RetryNewAttempt {
        failure: ModelFailure,
        retry_route_ordinal: Option<u32>,
    },
    WaitForProviderRepair {
        failure: ModelFailure,
    },
    FailRun {
        failure: ModelFailure,
    },
    CancelRun,
    ExternalOutcomeUnknown,
}

impl PostgresAgentLoopJournal {
    pub(super) async fn confirm_committed_model_completion(
        &self,
        attempt: &AttemptContext,
        state: &JournalState,
        completion: &ModelCompletionCommit,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        if self.model_liveness(state)?.is_some() {
            self.confirm_completion_fingerprint(attempt, state.tenant_id, completion)
                .await?;
        }
        validate_tool_calls(&completion.tool_calls)?;
        if let Some(usage) = completion.usage {
            usage
                .validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
        }
        let row = sqlx::query(
            "SELECT status, provider_request_ref, response_model_key, usage_fact,
                    continuation_output_ref, reasoning_replay_content_hash,
                    reasoning_replay_content_bytes, assistant_message_id
             FROM runtime.model_invocations
             WHERE invocation_id = $1 AND run_id = $2 AND tenant_id = $3",
        )
        .bind(completion.invocation_id.into_uuid())
        .bind(attempt.run_id.into_uuid())
        .bind(state.tenant_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresAgentLoopJournalError::CheckpointConflict)?;
        if row.try_get::<i16, _>("status")? != 2 {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let provider_response_ref = open_optional(
            &self.reference_keys,
            completion.invocation_id,
            "provider-request",
            row.try_get("provider_request_ref")?,
        )?
        .map(OpaqueExternalRef::new)
        .transpose()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
        .map(ProviderRequestRef::from_opaque);
        let response_model_key = row
            .try_get::<Option<String>, _>("response_model_key")?
            .map(musteragent_model::ModelKey::new)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let usage = row
            .try_get::<Option<Vec<u8>>, _>("usage_fact")?
            .as_deref()
            .map(decode_usage)
            .transpose()?;
        let continuation_ref = open_optional(
            &self.reference_keys,
            completion.invocation_id,
            "continuation-output",
            row.try_get("continuation_output_ref")?,
        )?
        .map(OpaqueExternalRef::new)
        .transpose()
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
        .map(musteragent_runtime_model::ContinuationRef::from_opaque);
        if provider_response_ref.as_ref() != Some(&completion.provider_response_ref)
            || response_model_key.as_ref() != Some(&completion.response_model_key)
            || usage != completion.usage
            || continuation_ref != completion.continuation_ref
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }

        let persisted_reasoning_hash = row
            .try_get::<Option<Vec<u8>>, _>("reasoning_replay_content_hash")?
            .map(Sha256::from_bytes)
            .transpose()
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
        let persisted_reasoning_bytes = row
            .try_get::<Option<String>, _>("reasoning_replay_content_bytes")?
            .map(parse_u64)
            .transpose()?;
        let expected_reasoning = completion.reasoning_replay.as_ref().map(|value| {
            (
                Sha256::digest(value.as_str().as_bytes()),
                u64::try_from(value.as_str().len()),
            )
        });
        let expected_reasoning = expected_reasoning
            .map(|(hash, bytes)| bytes.map(|bytes| (hash, bytes)))
            .transpose()?;
        if persisted_reasoning_hash.zip(persisted_reasoning_bytes) != expected_reasoning {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }

        let assistant_message_id = row
            .try_get::<Option<uuid::Uuid>, _>("assistant_message_id")?
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let assistant_message_committed: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM conversation.messages
                 WHERE message_id = $1 AND run_id = $2 AND lifecycle = 1
             )",
        )
        .bind(assistant_message_id)
        .bind(attempt.run_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        // Dialogue responses publish through their durable candidate owner;
        // they intentionally never create public Conversation.messages.
        let dialogue_candidate_committed = if state.dialogue_model_output {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM agent.public_delivery_candidates
                 WHERE tenant_id=$1 AND run_id=$2 AND invocation_id=$3)",
            )
            .bind(state.tenant_id.into_uuid())
            .bind(attempt.run_id.into_uuid())
            .bind(completion.invocation_id.into_uuid())
            .fetch_one(&self.pool)
            .await?
        } else {
            false
        };
        let completion_matches = match (&completion.kind, &state.checkpoint.phase) {
            (ModelCompletionKind::AssistantContinuation, LoopPhase::ReadyForModel) => {
                !assistant_message_committed && completion.tool_calls.is_empty()
            }
            (ModelCompletionKind::ReasoningContinuation, LoopPhase::ReadyForModel) => {
                !assistant_message_committed && completion.tool_calls.is_empty()
            }
            (ModelCompletionKind::AssistantResponse, LoopPhase::ReadyForModel) => {
                (assistant_message_committed || dialogue_candidate_committed)
                    && completion.tool_calls.is_empty()
            }
            (ModelCompletionKind::EmptyResponse, LoopPhase::ReadyForModel) => {
                !assistant_message_committed
                    && completion.tool_calls.is_empty()
                    && state.checkpoint.consecutive_empty_responses > 0
            }
            (
                ModelCompletionKind::ToolCalls,
                LoopPhase::ToolsPending {
                    invocation_id,
                    remaining_tool_call_ids,
                },
            ) => {
                *invocation_id == completion.invocation_id
                    && !assistant_message_committed
                    && remaining_tool_call_ids
                        == &completion
                            .tool_calls
                            .iter()
                            .map(|call| call.call_id.clone())
                            .collect::<Vec<_>>()
            }
            _ => false,
        };
        if !completion_matches {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        if completion.kind == ModelCompletionKind::ToolCalls {
            let expected_ids = completion
                .tool_calls
                .iter()
                .map(|call| call.call_id.clone())
                .collect::<Vec<_>>();
            if self.load_pending_tools(attempt, &expected_ids).await? != completion.tool_calls {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
        }
        Ok(state.checkpoint.clone())
    }

    pub(super) async fn recover_abandoned_model(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        constraints: ModelRecoveryConstraints,
        provider: Option<&dyn musteragent_runtime_model::ModelProvider>,
    ) -> Result<
        (
            Option<RecoveredModelCompletion>,
            bool,
            Option<musteragent_loop::RecoverableModelInvocation>,
        ),
        PostgresAgentLoopJournalError,
    > {
        let state = self.load_state(attempt).await?;
        let recovered = match self
            .recovered_completion(attempt, &state, invocation_id)
            .await
        {
            Ok(recovered) => recovered,
            Err(error) if intent_recovery_is_permanently_invalid(&error) => {
                let failure = ModelFailure {
                    code: ErrorCode::ContentIntegrityFailed,
                    retryable: false,
                };
                return self
                    .close_inflight_model(
                        attempt,
                        invocation_id,
                        InflightModelTermination::InvalidCompletion(ModelFailureCommit {
                            recovery_policy: constraints.recovery_policy,
                            observation: state.model_observation(invocation_id),
                            provider_failure: failure,
                            retry_after_ms: None,
                            resolution: ModelFailureResolution::FailRun { failure },
                        }),
                    )
                    .await
                    .map(|_| (None, false, None));
            }
            Err(error) => return Err(error),
        };
        if let Some(recovered) = recovered {
            let completion_kind = recovered.0;
            if state.dialogue_model_output
                && recovered.1.kind == ModelCompletionKind::AssistantResponse
                && let Some(rejection) = self
                    .validate_dialogue_completion(attempt, &recovered.1)
                    .await?
            {
                self.reject_inflight_model_action(
                    attempt,
                    ModelActionRejectionCommit {
                        observation: state.model_observation(invocation_id),
                        rejection,
                    },
                )
                .await?;
                return Ok((None, false, None));
            }
            self.finish_model_transition(attempt, recovered.1).await?;
            return Ok((Some(completion_kind), false, None));
        }
        if let Some(provider) = provider
            && let Some(recovery) = self
                .prepare_provider_recovery(attempt, &state, invocation_id, provider)
                .await?
        {
            return Ok((None, false, Some(recovery)));
        }
        self.recover_model_liveness(attempt).await?;
        if state.terminal_stop_reason.is_some() {
            // A successful Provider terminal is not replay authority. If an
            // older writer or a failed staging boundary left no exact intent
            // for a shape that cannot be reconstructed, fail the Run while
            // atomically sealing identity, optional usage and partial output.
            // Re-entering ReadyForModel here would duplicate a physical
            // Provider invocation whose terminal outcome is already durable.
            let failure = ModelFailure {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            };
            return self
                .close_inflight_model(
                    attempt,
                    invocation_id,
                    InflightModelTermination::Failed(ModelFailureCommit {
                        recovery_policy: constraints.recovery_policy,
                        observation: state.model_observation(invocation_id),
                        provider_failure: failure,
                        retry_after_ms: None,
                        resolution: ModelFailureResolution::FailRun { failure },
                    }),
                )
                .await
                .map(|_| (None, false, None));
        }
        if !state.provider_dispatched {
            if state.provider_response_ref.is_some()
                || state.observed_usage.is_some()
                || state.terminal_stop_reason.is_some()
                || state.public_output_observed
                || state.next_assistant_frame_index > 0
                || state.next_reasoning_frame_index > 0
            {
                let failure = ModelFailure {
                    code: ErrorCode::InternalInvariantViolated,
                    retryable: false,
                };
                return self
                    .close_inflight_model(
                        attempt,
                        invocation_id,
                        InflightModelTermination::Failed(ModelFailureCommit {
                            recovery_policy: constraints.recovery_policy,
                            observation: state.model_observation(invocation_id),
                            provider_failure: failure,
                            retry_after_ms: None,
                            resolution: ModelFailureResolution::FailRun { failure },
                        }),
                    )
                    .await
                    .map(|_| (None, false, None));
            }
            return self
                .close_inflight_model(attempt, invocation_id, InflightModelTermination::Abandoned)
                .await
                .map(|_| (None, false, None));
        }

        let capability = state
            .provider_recovery_capability
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let provider_failure = ModelFailure {
            code: ErrorCode::DependencyUnavailable,
            retryable: true,
        };
        let (resolution, replacement_scheduled) = if capability
            == ProviderRecoveryCapability::NonResumable
            && constraints
                .recovery_policy
                .permits_sealed_continuation(state.replay_barrier_observed)
            && self
                .recovery_budget(attempt)
                .await
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
                .permits_provider_retry(constraints.recovery_policy)
            && provider_failure.permits_new_attempt()
        {
            (ModelFailureResolution::RetryNewAttempt, true)
        } else {
            let failure = ModelFailure {
                code: ErrorCode::DependencyUnavailable,
                retryable: false,
            };
            (ModelFailureResolution::FailRun { failure }, false)
        };
        self.close_inflight_model(
            attempt,
            invocation_id,
            InflightModelTermination::Failed(ModelFailureCommit {
                recovery_policy: constraints.recovery_policy,
                observation: state.model_observation(invocation_id),
                provider_failure,
                retry_after_ms: None,
                resolution,
            }),
        )
        .await
        .map(|_| (None, replacement_scheduled, None))
    }

    pub(super) async fn cancel_inflight_model(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        observation: Option<ModelInvocationObservation>,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        self.close_inflight_model(
            attempt,
            invocation_id,
            InflightModelTermination::Cancelled(observation),
        )
        .await
    }

    pub(super) async fn fail_inflight_model(
        &self,
        attempt: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let invocation_id = commit.observation.invocation_id;
        self.close_inflight_model(
            attempt,
            invocation_id,
            InflightModelTermination::Failed(commit),
        )
        .await
    }

    pub(super) async fn reject_inflight_model_action(
        &self,
        attempt: &AttemptContext,
        commit: ModelActionRejectionCommit,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let invocation_id = commit.observation.invocation_id;
        if let ModelActionRejection::InvalidPublicDelivery { contract_ref, .. }
        | ModelActionRejection::InvalidPublicDeliveryWithCandidate { contract_ref, .. }
        | ModelActionRejection::PublicDeliveryFormatRejected { contract_ref, .. }
        | ModelActionRejection::InvalidPublicDeliveryUnreplayable { contract_ref, .. } =
            &commit.rejection
        {
            if musteragent_execution_context::PublicDeliveryContract::from_key(
                contract_ref.as_str(),
            )
            .is_none()
            {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
            let state = self.load_state(attempt).await?;
            let response = state
                .provider_response_ref
                .clone()
                .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?;
            let model = state
                .response_model_key
                .clone()
                .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?;
            let completion = ModelCompletionCommit {
                invocation_id,
                kind: ModelCompletionKind::AssistantResponse,
                tool_calls: vec![],
                reasoning_replay: None,
                usage: state.observed_usage,
                provider_response_ref: response,
                response_model_key: model,
                continuation_ref: None,
            };
            if self
                .validate_dialogue_completion(attempt, &completion)
                .await?
                .as_ref()
                != Some(&commit.rejection)
            {
                return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
            }
        }
        self.close_inflight_model(
            attempt,
            invocation_id,
            InflightModelTermination::ActionRejected(Box::new(commit)),
        )
        .await
    }

    async fn close_inflight_model(
        &self,
        attempt: &AttemptContext,
        invocation_id: InvocationId,
        termination: InflightModelTermination,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let retry_eligible = matches!(&termination, InflightModelTermination::Failed(commit) if commit.provider_failure.permits_new_attempt());
        let recovery_policy = match &termination {
            InflightModelTermination::Failed(commit)
            | InflightModelTermination::InvalidCompletion(commit) => commit.recovery_policy,
            _ => std::num::NonZeroU32::MIN.into(),
        };
        let provider_retry_after_ms = match &termination {
            InflightModelTermination::Failed(commit)
            | InflightModelTermination::InvalidCompletion(commit) => commit.retry_after_ms,
            InflightModelTermination::Abandoned
            | InflightModelTermination::ActionRejected(_)
            | InflightModelTermination::Cancelled(_) => None,
        };
        let rejects_invalid_completion = match &termination {
            InflightModelTermination::InvalidCompletion(_) => true,
            InflightModelTermination::ActionRejected(commit) => matches!(
                commit.rejection,
                ModelActionRejection::InvalidPublicDelivery { .. }
                    | ModelActionRejection::InvalidPublicDeliveryWithCandidate { .. }
                    | ModelActionRejection::PublicDeliveryFormatRejected { .. }
                    | ModelActionRejection::InvalidPublicDeliveryUnreplayable { .. }
            ),
            _ => false,
        };
        let mut cancellation = matches!(&termination, InflightModelTermination::Cancelled(_));
        let failure = matches!(
            &termination,
            InflightModelTermination::Failed(_)
                | InflightModelTermination::InvalidCompletion(_)
                | InflightModelTermination::ActionRejected(_)
        );
        let observed = self.load_state(attempt).await?;
        let assistant_output_id = observed
            .assistant_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let reasoning_output_id = observed
            .reasoning_output_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let planned_assistant_message_id = observed
            .assistant_message_id
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let public_assistant_message_id = self
            .public_assistant_message_id(invocation_id, planned_assistant_message_id)
            .await?;
        let durable_observation = observed.model_observation(invocation_id);
        let exhausted = match &termination {
            InflightModelTermination::Failed(commit) => match commit.resolution {
                ModelFailureResolution::ExhaustedProviderRoundV1 {
                    cause,
                    retry_route_ordinal,
                } => Some((cause, retry_route_ordinal)),
                _ => None,
            },
            _ => None,
        };
        let round_fence = match &termination {
            InflightModelTermination::Failed(commit)
            | InflightModelTermination::InvalidCompletion(commit) => {
                musteragent_persistence_codec::ProviderRoundFence::for_failure(commit)
            }
            _ => None,
        };
        let mut plan = close_plan(&observed, durable_observation, termination)?;
        let assistant_bytes = if observed.next_assistant_frame_index > 0 {
            Some(
                self.load_output_prefix_bytes(&observed, invocation_id, assistant_output_id)
                    .await?,
            )
        } else {
            None
        };
        let partial_ref =
            if let Some(bytes) = assistant_bytes.filter(|_| !observed.private_model_output) {
                Some(
                    self.content
                        .put(PutContent {
                            tenant_id: observed.tenant_id,
                            media_type: MediaType::new("text/plain;charset=utf-8")
                                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                            bytes,
                        })
                        .await?,
                )
            } else {
                None
            };

        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if failure && state.cancellation_requested {
            // Resolve against the durable stop in the same commit lane, not
            // the supervisor heartbeat. Preserve the observed physical prefix.
            plan = close_plan(
                &state,
                state.model_observation(invocation_id),
                InflightModelTermination::Cancelled(plan.observation.clone()),
            )?;
            cancellation = true;
        }
        let sealed: bool = sqlx::query_scalar(
            "SELECT (completion_intent_content_id IS NOT NULL OR completion_intent_protected IS NOT NULL)
             FROM runtime.model_invocations WHERE invocation_id = $1",
        ).bind(invocation_id.into_uuid()).fetch_one(&mut *transaction).await?;
        if sealed && !cancellation && !rejects_invalid_completion {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        if !matches!(
            state.checkpoint.phase,
            LoopPhase::ModelInFlight { invocation_id: active } if active == invocation_id
        ) || state.assistant_output_id != observed.assistant_output_id
            || state.reasoning_output_id != observed.reasoning_output_id
            || state.assistant_message_id != observed.assistant_message_id
            || state.next_assistant_frame_index != observed.next_assistant_frame_index
            || state.next_assistant_byte_offset != observed.next_assistant_byte_offset
            || state.next_reasoning_frame_index != observed.next_reasoning_frame_index
            || state.next_reasoning_byte_offset != observed.next_reasoning_byte_offset
            || plan.observation.as_ref() != Some(&state.model_observation(invocation_id))
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let reconciliation = if matches!(
            plan.transition,
            InflightCloseTransition::ExternalOutcomeUnknown
        ) {
            Some(
                self.require_unconfirmed_model_stop(
                    &mut transaction,
                    &state,
                    attempt,
                    invocation_id,
                )
                .await?,
            )
        } else {
            None
        };
        let budget = self
            .read_recovery_budget(&mut transaction, attempt, state.tenant_id)
            .await?;
        if !cancellation && let Some(fence) = round_fence {
            provider_round::require(&mut transaction, &state, attempt, fence).await?;
        }
        if !cancellation
            && let Some((cause, target)) = exhausted
            && target
                != ModelFailureResolution::retry_ordinal(
                    cause,
                    state.checkpoint.active_model_route_ordinal,
                    budget,
                )
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        if let InflightCloseTransition::RetryNewAttempt { failure, .. } = plan.transition
            && !budget.permits_failure_retry(recovery_policy, failure)
        {
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        let now = database_now_ms(&mut transaction).await?;
        let usage_recorded = plan
            .observation
            .as_ref()
            .and_then(|observation| observation.usage.map(|usage| (observation, usage)))
            .map(|(observation, usage)| {
                interrupted_usage(&state, invocation_id, observation, usage, plan.cancelled)
            })
            .transpose()?;

        let recovery_batch_index = u32::MAX - 1;
        let mut payloads = Vec::with_capacity(10);
        // ResponseCompleted, its terminal output markers, the frame
        // checkpoints and terminal_stop_reason are committed in one database
        // transaction. When that durable terminal fact exists, recovery must
        // not append a second terminal marker to an already sealed stream.
        // A Provider error has no terminal_stop_reason and still needs its
        // partial outputs sealed here.
        let should_seal_outputs = recovery_outputs_need_sealing(state.terminal_stop_reason);
        if state.next_assistant_frame_index > 0 && should_seal_outputs {
            payloads.push(LedgerPayload::OutputFrame(output_frame(
                invocation_id,
                recovery_batch_index,
                payloads.len(),
                assistant_output_id,
                Some(public_assistant_message_id),
                OutputChannel::AssistantText,
                state.next_assistant_frame_index,
                state.next_assistant_byte_offset,
                InlineFrame::final_marker(),
                true,
            )?));
        }
        if state.next_reasoning_frame_index > 0 && should_seal_outputs {
            payloads.push(LedgerPayload::OutputFrame(output_frame(
                invocation_id,
                recovery_batch_index,
                payloads.len(),
                reasoning_output_id,
                Some(public_assistant_message_id),
                OutputChannel::ReasoningSummary,
                state.next_reasoning_frame_index,
                state.next_reasoning_byte_offset,
                InlineFrame::final_marker(),
                true,
            )?));
        }
        payloads.push(LedgerPayload::DomainEvent(DomainEvent::Model(plan.event)));
        if let Some(usage) = usage_recorded.clone() {
            payloads.push(LedgerPayload::DomainEvent(DomainEvent::Model(
                ModelEvent::UsageRecorded(usage),
            )));
        }
        let partial_message_index = if let Some(reference) = &partial_ref {
            let index = payloads.len();
            payloads.push(LedgerPayload::DomainEvent(DomainEvent::Conversation(
                musteragent_conversation::ConversationEvent::MessagePartiallyCommitted {
                    message_id: public_assistant_message_id,
                    content_ref: reference.clone(),
                    content_hash: reference.sha256,
                    content_bytes: reference.byte_length,
                    reason: plan.partial_reason,
                },
            )));
            Some(index)
        } else {
            None
        };
        let retry_not_before = match plan.transition {
            InflightCloseTransition::RetryNewAttempt {
                failure,
                retry_route_ordinal,
            } => {
                let error = error_info(failure.code, true, "Agent attempt will be retried")?;
                let not_before = musteragent_loop::replacement_attempt_not_before(
                    now,
                    attempt,
                    std::num::NonZeroU32::new(budget.provider_failures.saturating_add(1))
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    provider_retry_after_ms.filter(|_| {
                        retry_route_ordinal
                            .is_none_or(|n| n == state.checkpoint.active_model_route_ordinal)
                    }),
                    recovery_policy,
                );
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::AttemptFailed {
                        attempt_number: attempt.attempt_number,
                        error: error.clone(),
                        retry_disposition: RetryDisposition::RetryNewAttempt,
                    },
                )));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::RetryScheduled {
                        failed_attempt_id: attempt.attempt_id,
                        next_attempt_number: musteragent_identity::AttemptNumber::new(
                            attempt
                                .attempt_number
                                .get()
                                .checked_add(1)
                                .ok_or(PostgresAgentLoopJournalError::NumericRange)?,
                        )
                        .map_err(|_| PostgresAgentLoopJournalError::NumericRange)?,
                        not_before,
                        error: error.clone(),
                    },
                )));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::Waiting {
                        reason: RunWaitReason::RetryBackoff,
                        resume_not_before: Some(not_before),
                    },
                )));
                Some(not_before)
            }
            InflightCloseTransition::WaitForProviderRepair { failure } => {
                payloads.extend(super::repair_wait::repair_payloads(attempt, failure.code)?);
                None
            }
            InflightCloseTransition::FailRun { failure } => {
                let error = error_info(
                    failure.code,
                    false,
                    &musteragent_loop::runtime_failure_message(
                        failure.code,
                        (retry_eligible && !budget.permits_provider_retry(recovery_policy))
                            .then_some(recovery_policy.terminal_limit())
                            .flatten(),
                    ),
                )?;
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::AttemptFailed {
                        attempt_number: attempt.attempt_number,
                        error: error.clone(),
                        retry_disposition: RetryDisposition::DoNotRetry,
                    },
                )));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::Failed {
                        error: error.clone(),
                        last_attempt_id: Some(attempt.attempt_id),
                    },
                )));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Failed { error },
                )));
                None
            }
            InflightCloseTransition::ExternalOutcomeUnknown => {
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::AttemptAbandoned {
                    attempt_number: attempt.attempt_number,
                    reason: musteragent_runtime_contract::InterruptionReason::ExternalOutcomeUnknown,
                })));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Interrupted {
                    reason: musteragent_runtime_contract::InterruptionReason::ExternalOutcomeUnknown,
                    last_attempt_id: Some(attempt.attempt_id),
                    reconciliation_ref: reconciliation.clone(),
                })));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Failed {
                        error: error_info(
                            ErrorCode::DependencyUnavailable,
                            false,
                            "Model Provider outcome requires reconciliation",
                        )?,
                    },
                )));
                None
            }
            InflightCloseTransition::CancelRun => {
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::Cancelled {
                        reason: musteragent_runtime_contract::CancelReason::UserRequested,
                        last_attempt_id: Some(attempt.attempt_id),
                    },
                )));
                payloads.push(LedgerPayload::DomainEvent(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Cancelled {
                        reason: musteragent_runtime_contract::CancelReason::UserRequested,
                    },
                )));
                None
            }
            InflightCloseTransition::ContinueAttempt { .. } => None,
        };
        let records = payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| {
                self.record(
                    &state,
                    attempt,
                    RecordSpec {
                        run_sequence: state
                            .next_run_sequence(u64::try_from(index)?.saturating_add(1))?,
                        occurred_at: now,
                        producer: Producer::Worker,
                        payload,
                        record_id: deterministic_record_id(
                            invocation_id,
                            recovery_batch_index,
                            index,
                            b"model-interruption",
                        )?,
                    },
                )
            })
            .collect::<Result<Vec<_>, PostgresAgentLoopJournalError>>()?;
        let _partial_message_record_id = partial_message_index
            .map(|index| {
                records
                    .get(index)
                    .map(|record| record.record_id)
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)
            })
            .transpose()?;
        let mut append =
            state.append_request(format!("loop:model-close:{invocation_id}"), records)?;
        let liveness = if reconciliation.is_some() {
            // The original scope stays StopUnconfirmed after local output seals.
            None
        } else {
            self.stage_model_terminal_liveness(
                &mut transaction,
                &state,
                attempt,
                invocation_id,
                true,
                &mut append,
            )
            .await?
        };
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self.load_after_model_close(attempt, plan.transition).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let record_count = u64::try_from(append.records.len())?;
        let mut staged = prepared.stage(&append).await?;
        let transaction_now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            transaction_now,
        )
        .await?;
        lock_checkpoint(staged.transaction_mut(), attempt, &state, 2, invocation_id).await?;
        let provider_request = protect_optional(
            &self.reference_keys,
            invocation_id,
            "provider-request",
            plan.observation
                .as_ref()
                .and_then(|value| value.provider_response_ref.as_ref())
                .map(ProviderRequestRef::as_str),
        )?;
        let usage = plan
            .observation
            .as_ref()
            .and_then(|observation| observation.usage);
        let response_model_key = plan
            .observation
            .as_ref()
            .and_then(|observation| observation.response_model_key.as_ref())
            .map(musteragent_model::ModelKey::as_str);
        let invocation = sqlx::query(
            "UPDATE runtime.model_invocations
             SET status = $2, provider_request_ref = $3,
                 response_model_key = $4, usage_fact = $5,
                 finished_at_ms = $6, terminal_error = $7,
                 continuation_output_ref = NULL
             WHERE invocation_id = $1 AND status = 1",
        )
        .bind(invocation_id.into_uuid())
        .bind(plan.status)
        .bind(provider_request)
        .bind(response_model_key)
        .bind(usage.map(encode_usage))
        .bind(transaction_now.unix_millis())
        .bind(plan.terminal_error)
        .execute(&mut *staged.transaction_mut())
        .await?;
        require_one(invocation.rows_affected())?;
        if let Some(rejection) = &plan.action_rejection {
            let typed = super::public_delivery_repair::persist(
                staged.transaction_mut(),
                &state,
                invocation_id,
                rejection,
                transaction_now,
            )
            .await?;
            if !typed {
                let (kind, attempted_name, contract, issue, validation_issues) = match rejection {
                    ModelActionRejection::ToolNotExposed { attempted_name } => (
                        "tool",
                        Some(attempted_name.as_str()),
                        None,
                        None,
                        serde_json::json!([]),
                    ),
                    ModelActionRejection::InvalidToolArguments {
                        attempted_name,
                        issues,
                    } => (
                        "tool",
                        Some(attempted_name.as_str()),
                        None,
                        None,
                        encode_validation_issues(issues)?,
                    ),
                    ModelActionRejection::InvalidPublicDelivery {
                        contract_ref,
                        issue,
                    } => (
                        "public_delivery",
                        None,
                        Some(contract_ref.as_str()),
                        Some(issue.as_str()),
                        serde_json::json!([]),
                    ),
                    _ => return Err(PostgresAgentLoopJournalError::InvalidModelBatch),
                };
                sqlx::query(
                    "INSERT INTO runtime.model_action_rejections (
                     invocation_id,error_code,attempted_wire_name,validation_issues,rejected_at_ms,
                     rejection_kind,delivery_contract_key,delivery_issue
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                )
                .bind(invocation_id.into_uuid())
                .bind(rejection.code().as_str())
                .bind(attempted_name)
                .bind(validation_issues)
                .bind(transaction_now.unix_millis())
                .bind(kind)
                .bind(contract)
                .bind(issue)
                .execute(&mut *staged.transaction_mut())
                .await?;
            }
        }
        completion_intent::request_completion_intent_erasure(
            staged.transaction_mut(),
            invocation_id,
            transaction_now,
        )
        .await?;

        if let (Some(_observation), Some(usage), Some(usage_recorded)) =
            (&plan.observation, usage, usage_recorded)
        {
            let model_attempt_id = state
                .model_attempt_id
                .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
            let fact = AttributedModelUsageFact {
                envelope: ModelUsageEnvelope {
                    attribution: Some(
                        crate::model::usage_attribution(staged.transaction_mut(), attempt.run_id)
                            .await
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    ),
                    tenant_id: state.tenant_id,
                    conversation_id: state.conversation_id,
                    run_id: attempt.run_id,
                    attempt_id: model_attempt_id,
                    observed_at: transaction_now,
                },
                usage: usage_recorded,
                provider_total_tokens: usage.total_tokens,
            };
            fact.validate()
                .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
            let publication = self.metering.encode_billable_usage(
                &fact,
                deterministic_outbox_id(invocation_id, b"model-usage")?,
            )?;
            if let Some(publication) = &publication {
                publication
                    .validate()
                    .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
                crate::model::insert_metering_outbox(staged.transaction_mut(), &fact, publication)
                    .await?;
            }
            crate::model::insert_usage_fact(staged.transaction_mut(), &fact, publication.as_ref())
                .await?;
        }
        self.append_model_terminal_audit(staged.transaction_mut(), state.tenant_id, invocation_id)
            .await?;
        if let Some(reference) = &partial_ref {
            insert_partial_message(
                staged.transaction_mut(),
                &state,
                attempt,
                public_assistant_message_id,
                reference,
                record_count,
                transaction_now,
            )
            .await?;
        }
        if let Some(change) = &liveness {
            change.persist(staged.transaction_mut()).await?;
        }
        let last_run_sequence = state.next_run_sequence(record_count)?;
        match plan.transition {
            InflightCloseTransition::ContinueAttempt { next_route_ordinal } => {
                let checkpoint = sqlx::query(
                    "UPDATE runtime.loop_checkpoints
                     SET phase = 1, model_invocation_id = NULL,
                         remaining_tool_call_ids = '{}', waiting_interaction_id = NULL,
                         active_model_route_ordinal = COALESCE($6, active_model_route_ordinal),
                         consecutive_empty_responses = 0,
                         continuation_ref = NULL, continuation_invocation_id = NULL,
                         next_assistant_frame_index = 0,
                         next_assistant_byte_offset = 0,
                         next_reasoning_frame_index = 0,
                         next_reasoning_byte_offset = 0,
                         checkpoint_version = checkpoint_version + 1,
                         updated_at_ms = $4
                     WHERE run_id = $1 AND attempt_id = $2
                       AND checkpoint_version = $3::bigint AND phase = 2
                       AND model_invocation_id = $5",
                )
                .bind(attempt.run_id.into_uuid())
                .bind(attempt.attempt_id.into_uuid())
                .bind(state.checkpoint_version.to_string())
                .bind(transaction_now.unix_millis())
                .bind(invocation_id.into_uuid())
                .bind(next_route_ordinal.map(i32::try_from).transpose()?)
                .execute(&mut *staged.transaction_mut())
                .await?;
                require_one(checkpoint.rows_affected())?;
                advance_run_sequence(staged.transaction_mut(), attempt.run_id, last_run_sequence)
                    .await?;
            }
            InflightCloseTransition::RetryNewAttempt {
                retry_route_ordinal,
                ..
            } => {
                retry_attempt_rows(
                    staged.transaction_mut(),
                    &state,
                    attempt,
                    retry_not_before.ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    retry_route_ordinal,
                    last_run_sequence,
                    transaction_now,
                )
                .await?;
            }
            InflightCloseTransition::WaitForProviderRepair { failure } => {
                super::repair_wait::repair_wait_rows(
                    staged.transaction_mut(),
                    &state,
                    attempt,
                    RunWaitReason::RecoveryRequired { code: failure.code },
                    last_run_sequence,
                    transaction_now,
                )
                .await?;
            }
            InflightCloseTransition::FailRun { failure } => {
                let error = error_info(
                    failure.code,
                    false,
                    &musteragent_loop::runtime_failure_message(
                        failure.code,
                        (retry_eligible && !budget.permits_provider_retry(recovery_policy))
                            .then_some(recovery_policy.terminal_limit())
                            .flatten(),
                    ),
                )?;
                crate::execution::discard_pending_run_controls(
                    staged.transaction_mut(),
                    state.tenant_id,
                    state.checkpoint.run_id,
                    crate::execution::RunControlDiscardReason::Failed,
                    transaction_now,
                )
                .await?;
                terminalize_rows(
                    staged.transaction_mut(),
                    &state,
                    attempt,
                    6,
                    6,
                    4,
                    5,
                    Some(&error),
                    last_run_sequence,
                    transaction_now,
                )
                .await?;
            }
            InflightCloseTransition::ExternalOutcomeUnknown => {
                crate::execution::discard_pending_run_controls(
                    staged.transaction_mut(),
                    state.tenant_id,
                    state.checkpoint.run_id,
                    crate::execution::RunControlDiscardReason::Interrupted,
                    transaction_now,
                )
                .await?;
                terminalize_rows(
                    staged.transaction_mut(),
                    &state,
                    attempt,
                    8,
                    8,
                    6,
                    5,
                    None,
                    last_run_sequence,
                    transaction_now,
                )
                .await?;
            }
            InflightCloseTransition::CancelRun => {
                crate::execution::discard_pending_run_controls(
                    staged.transaction_mut(),
                    state.tenant_id,
                    state.checkpoint.run_id,
                    crate::execution::RunControlDiscardReason::Cancelled,
                    transaction_now,
                )
                .await?;
                terminalize_rows(
                    staged.transaction_mut(),
                    &state,
                    attempt,
                    7,
                    7,
                    5,
                    6,
                    None,
                    last_run_sequence,
                    transaction_now,
                )
                .await?;
            }
        }
        crate::dialogue_publication::terminal::register_terminal_in_transaction(
            staged.transaction_mut(),
            crate::dialogue_publication::terminal::TerminalRunPosition {
                tenant: state.tenant_id,
                conversation: state.conversation_id,
                run: attempt.run_id,
                iteration: state.checkpoint.iteration,
                last_sequence: last_run_sequence,
            },
            &append.records,
            self.decoder.as_ref(),
        )
        .await?;
        staged.commit().await?;
        let _ = self.purge_completion_intent(invocation_id).await;
        self.load_after_model_close(attempt, plan.transition).await
    }

    async fn load_after_model_close(
        &self,
        attempt: &AttemptContext,
        transition: InflightCloseTransition,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        match transition {
            InflightCloseTransition::ContinueAttempt { .. } => {
                self.load_checkpoint_value(attempt).await
            }
            InflightCloseTransition::RetryNewAttempt { .. }
            | InflightCloseTransition::WaitForProviderRepair { .. } => {
                self.load_unowned_checkpoint(attempt.run_id).await
            }
            InflightCloseTransition::FailRun { .. }
            | InflightCloseTransition::CancelRun
            | InflightCloseTransition::ExternalOutcomeUnknown => {
                self.load_terminal_checkpoint(attempt.run_id).await
            }
        }
    }
    async fn recovered_completion(
        &self,
        attempt: &AttemptContext,
        state: &JournalState,
        invocation_id: InvocationId,
    ) -> Result<
        Option<(RecoveredModelCompletion, ModelCompletionCommit)>,
        PostgresAgentLoopJournalError,
    > {
        let Some(stop_reason) = state.terminal_stop_reason else {
            return Ok(None);
        };
        let exact = if let Some(completion) = state.legacy_completion_intent.clone() {
            Some(completion)
        } else if let Some((completion, intent)) = self
            .load_completion_intent(attempt, state.tenant_id, invocation_id)
            .await?
        {
            self.link_completion_intent(attempt, invocation_id, &intent)
                .await?;
            Some(completion)
        } else {
            None
        };
        if let Some(completion) = exact {
            if completion.invocation_id != invocation_id {
                return Err(PostgresAgentLoopJournalError::CorruptState);
            }
            let recovered = match completion.kind {
                ModelCompletionKind::AssistantContinuation => {
                    RecoveredModelCompletion::AssistantContinuation
                }
                ModelCompletionKind::ReasoningContinuation => {
                    RecoveredModelCompletion::ReasoningContinuation
                }
                ModelCompletionKind::AssistantResponse => {
                    RecoveredModelCompletion::AssistantResponse
                }
                ModelCompletionKind::ToolCalls => RecoveredModelCompletion::ToolCalls,
                ModelCompletionKind::EmptyResponse => RecoveredModelCompletion::EmptyResponse,
            };
            return Ok(Some((recovered, completion)));
        }
        let provider_response_ref = state
            .provider_response_ref
            .clone()
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let response_model_key = state
            .response_model_key
            .clone()
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let provider_state_policy = state
            .provider_state_policy
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
        let (recovered, kind) =
            if state.next_assistant_frame_index > 0 && stop_reason.is_token_limited() {
                (
                    RecoveredModelCompletion::AssistantContinuation,
                    ModelCompletionKind::AssistantContinuation,
                )
            } else if state.next_assistant_frame_index > 0
                && !matches!(
                    stop_reason,
                    musteragent_runtime_model::StopReason::ToolCalls
                )
            {
                (
                    RecoveredModelCompletion::AssistantResponse,
                    ModelCompletionKind::AssistantResponse,
                )
            } else if !state.public_output_observed
                && stop_reason == musteragent_runtime_model::StopReason::Stop
            {
                (
                    RecoveredModelCompletion::EmptyResponse,
                    ModelCompletionKind::EmptyResponse,
                )
            } else {
                // Tool and mixed public terminals require their full protected replay
                // intent. Until that shape is durably reconstructable they follow the
                // interruption close path; they are never guessed from model names.
                return Ok(None);
            };
        let continuation_ref = if provider_state_policy
            == musteragent_model::ProviderStatePolicy::OpaqueContinuation
        {
            Some(musteragent_runtime_model::ContinuationRef::from_opaque(
                OpaqueExternalRef::new(provider_response_ref.as_str())
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
            ))
        } else {
            None
        };
        Ok(Some((
            recovered,
            ModelCompletionCommit {
                invocation_id,
                kind,
                tool_calls: Vec::new(),
                reasoning_replay: None,
                usage: state.observed_usage,
                provider_response_ref,
                response_model_key,
                continuation_ref,
            },
        )))
    }
}

fn encode_validation_issues(
    issues: &[musteragent_runtime_contract::ToolArgumentValidationIssue],
) -> Result<serde_json::Value, PostgresAgentLoopJournalError> {
    if !musteragent_runtime_contract::tool_argument_validation_issues_are_canonical(issues) {
        return Err(PostgresAgentLoopJournalError::CorruptState);
    }
    Ok(serde_json::Value::Array(
        issues
            .iter()
            .map(|issue| {
                serde_json::json!({
                    "kind": issue.kind().as_str(),
                    "path": issue.path(),
                })
            })
            .collect(),
    ))
}

fn intent_recovery_is_permanently_invalid(error: &PostgresAgentLoopJournalError) -> bool {
    match error {
        PostgresAgentLoopJournalError::Protection | PostgresAgentLoopJournalError::CorruptState => {
            true
        }
        // Content owns the permanent/transient distinction. Do not omit a
        // newly added integrity classification from the recovery boundary.
        PostgresAgentLoopJournalError::Content(source) => !source.retryable(),
        _ => false,
    }
}

fn close_plan(
    state: &JournalState,
    durable_observation: ModelInvocationObservation,
    termination: InflightModelTermination,
) -> Result<InflightClosePlan, PostgresAgentLoopJournalError> {
    let invocation_id = durable_observation.invocation_id;
    let replay_barrier_observed = durable_observation.replay_barrier_observed;
    match termination {
        InflightModelTermination::Abandoned => Ok(InflightClosePlan {
            observation: Some(durable_observation),
            event: ModelEvent::InvocationCancelled { invocation_id },
            status: 4,
            terminal_error: None,
            partial_reason: musteragent_conversation::PartialMessageReason::Interrupted,
            transition: InflightCloseTransition::ContinueAttempt {
                next_route_ordinal: None,
            },
            cancelled: true,
            action_rejection: None,
        }),
        InflightModelTermination::Cancelled(observation) => {
            validate_observation(observation.as_ref(), &durable_observation)?;
            Ok(InflightClosePlan {
                observation: Some(durable_observation),
                event: ModelEvent::InvocationCancelled { invocation_id },
                status: 4,
                terminal_error: None,
                partial_reason: musteragent_conversation::PartialMessageReason::Cancelled,
                transition: InflightCloseTransition::CancelRun,
                cancelled: true,
                action_rejection: None,
            })
        }
        InflightModelTermination::Failed(commit)
        | InflightModelTermination::InvalidCompletion(commit) => {
            validate_observation(Some(&commit.observation), &durable_observation)?;
            let (transition, retry_disposition) = match commit.resolution {
                ModelFailureResolution::RetryFallbackRoute {
                    next_route_ordinal: next,
                }
                | ModelFailureResolution::ProviderFallbackV1 {
                    next_route_ordinal: next,
                    ..
                } if commit
                    .resolution
                    .permits_fallback(commit.provider_failure, commit.recovery_policy)
                    && !replay_barrier_observed
                    && next
                        == state
                            .checkpoint
                            .active_model_route_ordinal
                            .checked_add(1)
                            .ok_or(PostgresAgentLoopJournalError::NumericRange)? =>
                {
                    (
                        InflightCloseTransition::ContinueAttempt {
                            next_route_ordinal: Some(next),
                        },
                        RetryDisposition::RetryFallbackRoute,
                    )
                }
                ModelFailureResolution::RetryNewAttempt
                    if commit.provider_failure.permits_new_attempt()
                        && commit
                            .recovery_policy
                            .permits_sealed_continuation(replay_barrier_observed) =>
                {
                    (
                        InflightCloseTransition::RetryNewAttempt {
                            failure: commit.provider_failure,
                            retry_route_ordinal: None,
                        },
                        RetryDisposition::RetryNewAttempt,
                    )
                }
                ModelFailureResolution::ExternalOutcomeUnknown => (
                    InflightCloseTransition::ExternalOutcomeUnknown,
                    RetryDisposition::DoNotRetry,
                ),
                ModelFailureResolution::ExhaustedProviderRoundV1 {
                    cause,
                    retry_route_ordinal,
                } if cause.permits_alternate_provider()
                    && cause.runtime_code() == commit.provider_failure.code
                    && !replay_barrier_observed
                    && matches!(
                        commit.recovery_policy,
                        musteragent_runtime_snapshot::RecoveryPolicySnapshot::Persistent { .. }
                    ) =>
                {
                    match retry_route_ordinal {
                        Some(next) if next <= state.checkpoint.active_model_route_ordinal => (
                            InflightCloseTransition::RetryNewAttempt {
                                failure: commit.provider_failure,
                                retry_route_ordinal: Some(next),
                            },
                            RetryDisposition::RetryNewAttempt,
                        ),
                        None => (
                            InflightCloseTransition::WaitForProviderRepair {
                                failure: commit.provider_failure,
                            },
                            RetryDisposition::DoNotRetry,
                        ),
                        _ => return Err(PostgresAgentLoopJournalError::CheckpointConflict),
                    }
                }
                ModelFailureResolution::FailRun { failure } if !failure.retryable => (
                    InflightCloseTransition::FailRun { failure },
                    RetryDisposition::DoNotRetry,
                ),
                _ => return Err(PostgresAgentLoopJournalError::CheckpointConflict),
            };
            let error = error_info(
                commit.provider_failure.code,
                commit.provider_failure.retryable
                    && !matches!(transition, InflightCloseTransition::ExternalOutcomeUnknown),
                "Model Provider invocation interrupted",
            )?;
            Ok(InflightClosePlan {
                observation: Some(durable_observation),
                event: ModelEvent::InvocationFailed {
                    invocation_id,
                    error: error.clone(),
                    retry_disposition,
                },
                status: 3,
                terminal_error: Some(error.code.as_str().as_bytes().to_vec()),
                partial_reason: musteragent_conversation::PartialMessageReason::ProviderFailure,
                transition,
                cancelled: false,
                action_rejection: None,
            })
        }
        InflightModelTermination::ActionRejected(commit) => {
            validate_observation(Some(&commit.observation), &durable_observation)?;
            let code = commit.rejection.code();
            let transition = if commit.rejection.terminates_delivery_episode() {
                InflightCloseTransition::FailRun {
                    failure: ModelFailure {
                        code,
                        retryable: false,
                    },
                }
            } else {
                InflightCloseTransition::ContinueAttempt {
                    next_route_ordinal: None,
                }
            };
            let error = error_info(
                code,
                false,
                "Model action was rejected before Tool admission",
            )?;
            Ok(InflightClosePlan {
                observation: Some(durable_observation),
                event: ModelEvent::InvocationFailed {
                    invocation_id,
                    error: error.clone(),
                    retry_disposition: RetryDisposition::DoNotRetry,
                },
                status: 3,
                terminal_error: Some(error.code.as_str().as_bytes().to_vec()),
                partial_reason: musteragent_conversation::PartialMessageReason::ProviderFailure,
                transition,
                cancelled: false,
                action_rejection: Some(commit.rejection),
            })
        }
    }
}

fn validate_observation(
    observation: Option<&ModelInvocationObservation>,
    durable: &ModelInvocationObservation,
) -> Result<(), PostgresAgentLoopJournalError> {
    let Some(observation) = observation else {
        return Ok(());
    };
    if observation != durable {
        return Err(PostgresAgentLoopJournalError::CheckpointConflict);
    }
    if let Some(usage) = durable.usage {
        usage
            .validate()
            .map_err(|_| PostgresAgentLoopJournalError::InvalidModelBatch)?;
    }
    Ok(())
}

fn interrupted_usage(
    state: &JournalState,
    invocation_id: InvocationId,
    observation: &ModelInvocationObservation,
    usage: ModelUsage,
    cancelled: bool,
) -> Result<UsageRecorded, PostgresAgentLoopJournalError> {
    Ok(UsageRecorded {
        invocation_id,
        purpose: super::model_profile(state.activity_kind, &state.snapshot_schema)?.0,
        model_route_ref: state
            .model_route_ref
            .clone()
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
        credential_grant_ref: state
            .credential_grant_ref
            .clone()
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
        requested_model_key: state
            .requested_model_key
            .clone()
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
        response_model_key: observation
            .response_model_key
            .clone()
            .ok_or(PostgresAgentLoopJournalError::InvalidModelBatch)?,
        pricing_version_ref: state
            .pricing_version_ref
            .clone()
            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
        input_tokens: usage.input_tokens_uncached,
        output_tokens: usage.output_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        provider_cache_read_tokens: usage.provider_cache_read_tokens,
        provider_cache_write_tokens: usage.provider_cache_write_tokens,
        provider_cache_write_1h_tokens: usage.provider_cache_write_1h_tokens,
        cancelled,
        metering_dedup_key: IdempotencyKey::new(format!("usage:{invocation_id}"))
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
    })
}

#[allow(clippy::too_many_arguments)]
async fn insert_partial_message(
    transaction: &mut sqlx::PgConnection,
    state: &JournalState,
    attempt: &AttemptContext,
    assistant_message_id: MessageId,
    reference: &musteragent_content_ref::ContentRef,
    record_count: u64,
    transaction_now: UtcTimestamp,
) -> Result<(), PostgresAgentLoopJournalError> {
    let committed_sequence = state
        .conversation_offset
        .get()
        .checked_add(record_count)
        .ok_or(PostgresAgentLoopJournalError::NumericRange)?;
    sqlx::query(
        "INSERT INTO conversation.messages (
             message_id, tenant_id, conversation_id, participant_id, run_id,
             role, content_id, content_hash, content_bytes, lifecycle,
             committed_sequence, committed_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, 2, $6, $7, $8::bigint, 2,
             $9::bigint, $10
         )",
    )
    .bind(assistant_message_id.into_uuid())
    .bind(state.tenant_id.into_uuid())
    .bind(state.conversation_id.into_uuid())
    .bind(state.executor_participant_id.into_uuid())
    .bind(attempt.run_id.into_uuid())
    .bind(reference.content_id.into_uuid())
    .bind(reference.sha256.into_bytes().to_vec())
    .bind(reference.byte_length.to_string())
    .bind(committed_sequence.to_string())
    .bind(transaction_now.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

const fn recovery_outputs_need_sealing(
    terminal_stop_reason: Option<musteragent_runtime_model::StopReason>,
) -> bool {
    terminal_stop_reason.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_terminal_batch_already_owns_output_sealing() {
        assert!(!recovery_outputs_need_sealing(Some(
            musteragent_runtime_model::StopReason::Stop
        )));
        assert!(!recovery_outputs_need_sealing(Some(
            musteragent_runtime_model::StopReason::ToolCalls
        )));
        assert!(recovery_outputs_need_sealing(None));
    }
}
