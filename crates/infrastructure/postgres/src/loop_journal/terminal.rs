use super::*;

/// Fixed user-facing notice projected to the originating Channel when a Run
/// reaches a terminal failure. Keep verbatim in sync with the SQLite adapter.
const CHANNEL_RUN_FAILURE_NOTICE: &str =
    "抱歉，处理这条消息时出错了。请重试，或发送 /new 开始新会话。";

#[derive(Debug)]
struct ActiveTool {
    invocation_id: InvocationId,
    status: i16,
    effect: i16,
    retry_contract: i16,
    protected_reconciliation_ref: Option<Vec<u8>>,
}

#[derive(Debug)]
enum ActiveToolTerminal {
    Cancelled {
        invocation_id: InvocationId,
        expected_status: i16,
        reason: musteragent_runtime_contract::CancelReason,
    },
    Failed {
        invocation_id: InvocationId,
        expected_status: i16,
        error: ErrorInfo,
    },
    Indeterminate {
        invocation_id: InvocationId,
        expected_status: i16,
        reason: SafeDisplayText,
        reconciliation_ref: OpaqueExternalRef,
        protected_reconciliation_ref: Vec<u8>,
    },
}

impl ActiveToolTerminal {
    fn event(&self) -> DomainEvent {
        DomainEvent::Tool(match self {
            Self::Cancelled {
                invocation_id,
                reason,
                ..
            } => musteragent_tool::ToolEvent::InvocationCancelled {
                invocation_id: *invocation_id,
                reason: *reason,
            },
            Self::Failed {
                invocation_id,
                error,
                ..
            } => musteragent_tool::ToolEvent::InvocationFailed {
                invocation_id: *invocation_id,
                error: error.clone(),
                retry_disposition: RetryDisposition::DoNotRetry,
                diagnostic_result_ref: None,
                diagnostic_result_hash: None,
            },
            Self::Indeterminate {
                invocation_id,
                reason,
                reconciliation_ref,
                ..
            } => musteragent_tool::ToolEvent::InvocationIndeterminate {
                invocation_id: *invocation_id,
                reason: reason.clone(),
                reconciliation_ref: reconciliation_ref.clone(),
                observed_error: None,
                diagnostic_result_ref: None,
                diagnostic_result_hash: None,
            },
        })
    }

    fn invocation_id(&self) -> InvocationId {
        match self {
            Self::Cancelled { invocation_id, .. }
            | Self::Failed { invocation_id, .. }
            | Self::Indeterminate { invocation_id, .. } => *invocation_id,
        }
    }

    fn expected_status(&self) -> i16 {
        match self {
            Self::Cancelled {
                expected_status, ..
            }
            | Self::Failed {
                expected_status, ..
            }
            | Self::Indeterminate {
                expected_status, ..
            } => *expected_status,
        }
    }
}

impl PostgresAgentLoopJournal {
    pub(super) async fn finish_run(
        &self,
        attempt: &AttemptContext,
        mutation: TerminalMutation,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let state = self.load_state(attempt).await?;
        if state.checkpoint.phase.is_terminal()
            || matches!(state.checkpoint.phase, LoopPhase::Waiting { .. })
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        if let TerminalMutation::Failed {
            code,
            retryable: true,
            ..
        } = mutation
        {
            let _ = code;
            return Err(PostgresAgentLoopJournalError::InvalidModelBatch);
        }
        if matches!(mutation, TerminalMutation::Completed)
            && state.checkpoint.phase != LoopPhase::ReadyForModel
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        // Stage the fixed Channel failure notice ahead of the commit lane. The
        // notice is cosmetic: staging is best-effort and any failure degrades
        // to skipping the projection, never to blocking the terminal mutation.
        let failure_notice_ref = if matches!(
            mutation,
            TerminalMutation::Failed {
                retryable: false,
                ..
            }
        ) {
            match MediaType::new("text/plain") {
                Ok(media_type) => self
                    .content
                    .put(PutContent {
                        tenant_id: state.tenant_id,
                        media_type,
                        bytes: CHANNEL_RUN_FAILURE_NOTICE.as_bytes().to_vec(),
                    })
                    .await
                    .ok(),
                Err(_) => None,
            }
        } else {
            None
        };
        let expected_checkpoint_version = state.checkpoint_version;
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if state.checkpoint_version != expected_checkpoint_version {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let mutation = if matches!(
            mutation,
            TerminalMutation::Completed | TerminalMutation::Failed { .. }
        ) && state.cancellation_requested
        {
            TerminalMutation::Cancelled
        } else {
            mutation
        };
        if matches!(mutation, TerminalMutation::Completed) {
            let controls_pending: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1
                     FROM runtime.loop_controls
                     WHERE tenant_id = $1 AND run_id = $2 AND status = 1
                 )",
            )
            .bind(state.tenant_id.into_uuid())
            .bind(attempt.run_id.into_uuid())
            .fetch_one(&mut *transaction)
            .await?;
            if controls_pending {
                return Err(PostgresAgentLoopJournalError::PendingRunControls);
            }
            let delegated:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.delegations WHERE parent_run_id=$1 AND status=1 AND completion_mode=1) OR EXISTS(SELECT 1 FROM runtime.delegation_deliveries WHERE parent_run_id=$1 AND state IN (1,2))")
                .bind(attempt.run_id.into_uuid()).fetch_one(&mut *transaction).await?;
            if delegated {
                return Err(PostgresAgentLoopJournalError::PendingDelegation);
            }
            if super::dialogue_tool_completion::readiness(&mut transaction, &state).await?
                == musteragent_loop::RunCompletionReadiness::Continue
            {
                return Err(PostgresAgentLoopJournalError::PendingRequiredWork);
            }
        }
        // Read under the original fenced commit lane. A second pool checkout
        // here deadlocks a one-connection pool and escapes its transaction view.
        let latest_assistant_message = if matches!(mutation, TerminalMutation::Completed) {
            Some(Self::latest_assistant_message(&mut transaction, attempt.run_id).await?)
        } else {
            None
        };
        let now = database_now_ms(&mut transaction).await?;
        let leave_delegated_participant = if state.activity_kind == 4
            && state.executor_participant_id != state.execution_owner_participant_id
        {
            sqlx::query_scalar::<_, bool>(
                "SELECT NOT EXISTS (
                     SELECT 1 FROM runtime.runs
                     WHERE tenant_id = $1 AND conversation_id = $2
                       AND executor_participant_id = $3 AND run_id != $4
                       AND status BETWEEN 1 AND 4
                 )",
            )
            .bind(state.tenant_id.into_uuid())
            .bind(state.conversation_id.into_uuid())
            .bind(state.executor_participant_id.into_uuid())
            .bind(attempt.run_id.into_uuid())
            .fetch_one(&mut *transaction)
            .await?
        } else {
            false
        };
        let error = match mutation {
            TerminalMutation::Failed {
                code,
                retryable,
                recovery_limit,
            } => Some(error_info(
                code,
                retryable,
                &musteragent_loop::runtime_failure_message(code, recovery_limit),
            )?),
            _ => None,
        };
        let active_tool_terminals = if matches!(mutation, TerminalMutation::Completed) {
            Vec::new()
        } else {
            self.active_tool_terminals(&mut transaction, &state, mutation, error.as_ref())
                .await?
        };
        let mut events = active_tool_terminals
            .iter()
            .map(ActiveToolTerminal::event)
            .collect::<Vec<_>>();
        if let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase {
            events.push(DomainEvent::Model(match mutation {
                TerminalMutation::Cancelled | TerminalMutation::InterruptedCoordination => {
                    ModelEvent::InvocationCancelled { invocation_id }
                }
                _ => ModelEvent::InvocationFailed {
                    invocation_id,
                    error: error
                        .clone()
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    retry_disposition: RetryDisposition::DoNotRetry,
                },
            }));
        }
        let mut run_failed_event_index = None;
        match mutation {
            TerminalMutation::Completed => {
                events.push(DomainEvent::Run(RunEvent::AttemptSucceeded {
                    attempt_number: attempt.attempt_number,
                }));
                events.push(DomainEvent::Run(RunEvent::Completed {
                    assistant_message_id: latest_assistant_message
                        .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    result_refs: Vec::new(),
                }));
                events.push(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Completed {
                        result_refs: Vec::new(),
                    },
                ));
            }
            TerminalMutation::Failed { .. } => {
                let error = error
                    .clone()
                    .ok_or(PostgresAgentLoopJournalError::CorruptState)?;
                events.push(DomainEvent::Run(RunEvent::AttemptFailed {
                    attempt_number: attempt.attempt_number,
                    error: error.clone(),
                    retry_disposition: RetryDisposition::DoNotRetry,
                }));
                run_failed_event_index = Some(events.len());
                events.push(DomainEvent::Run(RunEvent::Failed {
                    error: error.clone(),
                    last_attempt_id: Some(attempt.attempt_id),
                }));
                events.push(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Failed { error },
                ));
            }
            TerminalMutation::Cancelled => {
                events.push(DomainEvent::Run(RunEvent::Cancelled {
                    reason: musteragent_runtime_contract::CancelReason::UserRequested,
                    last_attempt_id: Some(attempt.attempt_id),
                }));
                events.push(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Cancelled {
                        reason: musteragent_runtime_contract::CancelReason::UserRequested,
                    },
                ));
            }
            TerminalMutation::InterruptedCoordination => {
                events.push(DomainEvent::Run(RunEvent::Interrupted {
                    reason: musteragent_runtime_contract::InterruptionReason::CoordinationRequested,
                    last_attempt_id: Some(attempt.attempt_id),
                    reconciliation_ref: None,
                }));
                events.push(DomainEvent::Activity(
                    musteragent_activity::ActivityEvent::Cancelled {
                        reason: musteragent_runtime_contract::CancelReason::CoordinationInterrupted,
                    },
                ));
            }
        }
        if leave_delegated_participant {
            events.push(DomainEvent::Conversation(
                ConversationEvent::ParticipantLeft,
            ));
        }
        let terminal_tag: &[u8] = match mutation {
            TerminalMutation::Completed => b"run-complete",
            TerminalMutation::Failed { .. } => b"run-fail",
            TerminalMutation::Cancelled => b"run-cancel",
            TerminalMutation::InterruptedCoordination => b"run-interrupt",
        };
        let records = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                self.record(
                    &state,
                    attempt,
                    RecordSpec {
                        run_sequence: state
                            .next_run_sequence(u64::try_from(index)?.saturating_add(1))?,
                        occurred_at: now,
                        producer: Producer::Worker,
                        payload: LedgerPayload::DomainEvent(event),
                        record_id: deterministic_run_record_id(
                            attempt.run_id,
                            index,
                            terminal_tag,
                        )?,
                    },
                )
            })
            .collect::<Result<Vec<_>, PostgresAgentLoopJournalError>>()?;
        // The `RunEvent::Failed` Ledger record anchors the Channel failure
        // notice projection; its identity is the same deterministic id the
        // append above assigned to that event index.
        let run_failed_record_id = run_failed_event_index
            .map(|index| deterministic_run_record_id(attempt.run_id, index, b"run-fail"))
            .transpose()?;
        let mut append = state.append_request(
            format!(
                "loop:terminal:{}:{}",
                attempt.run_id,
                terminal_tag_name(mutation)
            ),
            records,
        )?;
        let model_liveness =
            if let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase {
                self.stage_model_terminal_liveness(
                    &mut transaction,
                    &state,
                    attempt,
                    invocation_id,
                    true,
                    &mut append,
                )
                .await?
            } else {
                None
            };
        let record_count = u64::try_from(append.records.len())?;
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self.load_terminal_checkpoint(attempt.run_id).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let mut staged = prepared.stage(&append).await?;
        let transaction_now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            transaction_now,
        )
        .await?;
        if let Some(change) = &model_liveness {
            change.persist(staged.transaction_mut()).await?;
        }
        if let LoopPhase::ModelInFlight { invocation_id } = state.checkpoint.phase {
            let (status, terminal_error) = match mutation {
                TerminalMutation::Cancelled | TerminalMutation::InterruptedCoordination => {
                    (4_i16, None)
                }
                _ => (
                    3_i16,
                    error.as_ref().map(|value| value.code.as_str().as_bytes()),
                ),
            };
            let invocation = sqlx::query(
                "UPDATE runtime.model_invocations
                 SET status = $2, finished_at_ms = $3, terminal_error = $4
                 WHERE invocation_id = $1 AND status = 1",
            )
            .bind(invocation_id.into_uuid())
            .bind(status)
            .bind(transaction_now.unix_millis())
            .bind(terminal_error)
            .execute(&mut *staged.transaction_mut())
            .await?;
            require_one(invocation.rows_affected())?;
            self.append_model_terminal_audit(
                staged.transaction_mut(),
                state.tenant_id,
                invocation_id,
            )
            .await?;
        }
        self.persist_active_tool_terminals(
            staged.transaction_mut(),
            state.tenant_id,
            &active_tool_terminals,
            transaction_now,
        )
        .await?;
        let control_discard_reason = match mutation {
            TerminalMutation::Completed => None,
            TerminalMutation::Failed { .. } => {
                Some(crate::execution::RunControlDiscardReason::Failed)
            }
            TerminalMutation::Cancelled => {
                Some(crate::execution::RunControlDiscardReason::Cancelled)
            }
            TerminalMutation::InterruptedCoordination => {
                Some(crate::execution::RunControlDiscardReason::Interrupted)
            }
        };
        if let Some(reason) = control_discard_reason {
            crate::execution::discard_pending_run_controls(
                staged.transaction_mut(),
                state.tenant_id,
                state.checkpoint.run_id,
                reason,
                transaction_now,
            )
            .await?;
        }
        let (checkpoint_phase, run_status, attempt_status, activity_status) = match mutation {
            TerminalMutation::Completed => (5_i16, 5_i16, super::ATTEMPT_STATUS_SUCCEEDED, 4_i16),
            TerminalMutation::Failed { .. } => (6_i16, 6_i16, 4_i16, 5_i16),
            TerminalMutation::Cancelled => (7_i16, 7_i16, 5_i16, 6_i16),
            TerminalMutation::InterruptedCoordination => (8_i16, 8_i16, 6_i16, 6_i16),
        };
        terminalize_rows(
            staged.transaction_mut(),
            &state,
            attempt,
            checkpoint_phase,
            run_status,
            attempt_status,
            activity_status,
            error.as_ref(),
            state.next_run_sequence(record_count)?,
            transaction_now,
        )
        .await?;
        // Project the staged failure notice back to the originating Channel in
        // the same transaction as the terminal mutation. Runs that were not
        // admitted from a Channel match no ingress and insert zero rows.
        if let (Some(_content_ref), Some(_record_id)) = (failure_notice_ref, run_failed_record_id) {
        }
        if leave_delegated_participant {
            let participant = sqlx::query(
                "UPDATE conversation.participants SET left_at_ms = $1
                 WHERE tenant_id = $2 AND conversation_id = $3
                   AND participant_id = $4 AND kind = 2 AND left_at_ms IS NULL",
            )
            .bind(transaction_now.unix_millis())
            .bind(state.tenant_id.into_uuid())
            .bind(state.conversation_id.into_uuid())
            .bind(state.executor_participant_id.into_uuid())
            .execute(&mut *staged.transaction_mut())
            .await?;
            require_one(participant.rows_affected())?;
        }
        if matches!(mutation, TerminalMutation::Completed) {
            crate::agent_dialogue::capability::stage_successor(
                staged.transaction_mut(),
                state.tenant_id,
                attempt.run_id,
            )
            .await?;
        }
        crate::dialogue_publication::terminal::register_terminal_in_transaction(
            staged.transaction_mut(),
            crate::dialogue_publication::terminal::TerminalRunPosition {
                tenant: state.tenant_id,
                conversation: state.conversation_id,
                run: attempt.run_id,
                iteration: state.checkpoint.iteration,
                last_sequence: state.next_run_sequence(record_count)?,
            },
            &append.records,
            self.decoder.as_ref(),
        )
        .await?;
        staged.commit().await?;
        self.load_terminal_checkpoint(attempt.run_id).await
    }

    pub(super) async fn interrupt_for_external_outcome(
        &self,
        attempt: &AttemptContext,
        reconciliation_ref: OpaqueExternalRef,
    ) -> Result<DurableLoopCheckpoint, PostgresAgentLoopJournalError> {
        let (mut transaction, state) = self.load_state_in_commit_lane(attempt).await?;
        if state.checkpoint.phase.is_terminal()
            || matches!(state.checkpoint.phase, LoopPhase::Waiting { .. })
        {
            return Err(PostgresAgentLoopJournalError::CheckpointConflict);
        }
        let unknown_tools = sqlx::query(
            "SELECT invocation_id, retry_contract, reconciliation_ref_protected
             FROM tool.tool_invocations
             WHERE tenant_id = $1 AND run_id = $2 AND status = 6
             FOR UPDATE",
        )
        .bind(state.tenant_id.into_uuid())
        .bind(attempt.run_id.into_uuid())
        .fetch_all(&mut *transaction)
        .await?;
        let maintenance_invocation: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT invocation_id
                 FROM runtime.evolution_maintenance_executions
                 WHERE tenant_id = $1 AND run_id = $2
                   AND outcome_status = 5",
        )
        .bind(state.tenant_id.into_uuid())
        .bind(attempt.run_id.into_uuid())
        .fetch_optional(&mut *transaction)
        .await?;
        let mut reference_matches = maintenance_invocation.is_some_and(|invocation_id| {
            reconciliation_ref.as_str()
                == format!("evolution-maintenance:{invocation_id}:provider-outcome-unknown")
        });
        for row in unknown_tools {
            let invocation_id = InvocationId::from_uuid(row.try_get("invocation_id")?)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            let protected = ProtectedOpaqueRef::from_bytes(
                row.try_get::<Vec<u8>, _>("reconciliation_ref_protected")?,
            )
            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            let context = format!("tool-invocation:{invocation_id}:reconciliation");
            let opened = self
                .reference_keys
                .open(context.as_bytes(), &protected)
                .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
            reference_matches |= matches!(row.try_get::<i16, _>("retry_contract")?, 3 | 4)
                && opened == reconciliation_ref.as_str();
        }
        if !reference_matches {
            return Err(PostgresAgentLoopJournalError::CorruptState);
        }

        let now = database_now_ms(&mut transaction).await?;
        let activity_error = error_info(
            ErrorCode::DependencyUnavailable,
            false,
            if maintenance_invocation.is_some() {
                "Evolution Maintenance Provider outcome requires operator reconciliation"
            } else {
                "An external Tool outcome requires operator reconciliation"
            },
        )?;
        let active_tool_terminals = self
            .active_tool_terminals(
                &mut transaction,
                &state,
                TerminalMutation::Failed {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: false,
                    recovery_limit: None,
                },
                Some(&activity_error),
            )
            .await?;
        let mut events = active_tool_terminals
            .iter()
            .map(ActiveToolTerminal::event)
            .collect::<Vec<_>>();
        events.push(DomainEvent::Run(RunEvent::AttemptAbandoned {
            attempt_number: attempt.attempt_number,
            reason: musteragent_runtime_contract::InterruptionReason::ExternalOutcomeUnknown,
        }));
        events.push(DomainEvent::Run(RunEvent::Interrupted {
            reason: musteragent_runtime_contract::InterruptionReason::ExternalOutcomeUnknown,
            last_attempt_id: Some(attempt.attempt_id),
            reconciliation_ref: Some(reconciliation_ref),
        }));
        events.push(DomainEvent::Activity(
            musteragent_activity::ActivityEvent::Failed {
                error: activity_error,
            },
        ));
        let records = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                self.record(
                    &state,
                    attempt,
                    RecordSpec {
                        run_sequence: state
                            .next_run_sequence(u64::try_from(index)?.saturating_add(1))?,
                        occurred_at: now,
                        producer: Producer::Worker,
                        payload: LedgerPayload::DomainEvent(event),
                        record_id: deterministic_run_record_id(
                            attempt.run_id,
                            index,
                            b"external-outcome-unknown",
                        )?,
                    },
                )
            })
            .collect::<Result<Vec<_>, PostgresAgentLoopJournalError>>()?;
        let record_count = u64::try_from(records.len())?;
        let append = state.append_request(
            format!("loop:terminal:{}:external-outcome-unknown", attempt.run_id),
            records,
        )?;
        let prepared = match self
            .ledger
            .begin_append_in_transaction(transaction, &append)
            .await?
        {
            BeginAppend::Duplicate(_) => {
                return self.load_terminal_checkpoint(attempt.run_id).await;
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let mut staged = prepared.stage(&append).await?;
        let transaction_now = database_now_ms(staged.transaction_mut()).await?;
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &attempt_fence(attempt, state.home_epoch),
            state.last_run_sequence,
            transaction_now,
        )
        .await?;
        self.persist_active_tool_terminals(
            staged.transaction_mut(),
            state.tenant_id,
            &active_tool_terminals,
            transaction_now,
        )
        .await?;
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
            state.next_run_sequence(record_count)?,
            transaction_now,
        )
        .await?;
        crate::dialogue_publication::terminal::register_terminal_in_transaction(
            staged.transaction_mut(),
            crate::dialogue_publication::terminal::TerminalRunPosition {
                tenant: state.tenant_id,
                conversation: state.conversation_id,
                run: attempt.run_id,
                iteration: state.checkpoint.iteration,
                last_sequence: state.next_run_sequence(record_count)?,
            },
            &append.records,
            self.decoder.as_ref(),
        )
        .await?;
        staged.commit().await?;
        self.load_terminal_checkpoint(attempt.run_id).await
    }

    async fn active_tool_terminals(
        &self,
        transaction: &mut sqlx::PgConnection,
        state: &JournalState,
        mutation: TerminalMutation,
        error: Option<&ErrorInfo>,
    ) -> Result<Vec<ActiveToolTerminal>, PostgresAgentLoopJournalError> {
        let rows = sqlx::query(
            "SELECT invocation.invocation_id, invocation.status, invocation.effect,
                    invocation.retry_contract, invocation.reconciliation_ref_protected
             FROM tool.tool_invocations AS invocation
             LEFT JOIN runtime.pending_tool_calls AS pending
               ON pending.run_id = invocation.run_id
              AND pending.tool_call_id = invocation.tool_call_id
             WHERE invocation.tenant_id = $1 AND invocation.run_id = $2
               AND invocation.status IN (1, 2, 3)
             ORDER BY pending.ordinal NULLS LAST, invocation.invocation_id
             FOR UPDATE OF invocation",
        )
        .bind(state.tenant_id.into_uuid())
        .bind(state.checkpoint.run_id.into_uuid())
        .fetch_all(&mut *transaction)
        .await?;
        rows.into_iter()
            .map(|row| {
                let active = ActiveTool {
                    invocation_id: InvocationId::from_uuid(row.try_get("invocation_id")?)
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    status: row.try_get("status")?,
                    effect: row.try_get("effect")?,
                    retry_contract: row.try_get("retry_contract")?,
                    protected_reconciliation_ref: row.try_get("reconciliation_ref_protected")?,
                };
                if !(1..=3).contains(&active.status)
                    || !(1..=4).contains(&active.effect)
                    || !(1..=4).contains(&active.retry_contract)
                {
                    return Err(PostgresAgentLoopJournalError::CorruptState);
                }
                if active.status == 3 && active.effect != 1 {
                    let reason =
                        SafeDisplayText::new("The Run stopped after external Tool execution began")
                            .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
                    let context =
                        format!("tool-invocation:{}:reconciliation", active.invocation_id);
                    let existing =
                        active
                            .protected_reconciliation_ref
                            .as_ref()
                            .and_then(|protected| {
                                let protected =
                                    ProtectedOpaqueRef::from_bytes(protected.clone()).ok()?;
                                let value = self
                                    .reference_keys
                                    .open(context.as_bytes(), &protected)
                                    .ok()?;
                                OpaqueExternalRef::new(value)
                                    .ok()
                                    .map(|reference| (reference, protected.as_bytes().to_vec()))
                            });
                    let (reconciliation_ref, protected_reconciliation_ref) =
                        if matches!(active.retry_contract, 3 | 4) {
                            match existing {
                                Some(existing) => existing,
                                None => runtime_operator_reconciliation_ref(
                                    &self.reference_keys,
                                    active.invocation_id,
                                )?,
                            }
                        } else {
                            runtime_operator_reconciliation_ref(
                                &self.reference_keys,
                                active.invocation_id,
                            )?
                        };
                    return Ok(ActiveToolTerminal::Indeterminate {
                        invocation_id: active.invocation_id,
                        expected_status: active.status,
                        reason,
                        reconciliation_ref,
                        protected_reconciliation_ref,
                    });
                }
                match mutation {
                    TerminalMutation::Cancelled | TerminalMutation::InterruptedCoordination => {
                        Ok(ActiveToolTerminal::Cancelled {
                            invocation_id: active.invocation_id,
                            expected_status: active.status,
                            reason: if matches!(mutation, TerminalMutation::InterruptedCoordination)
                            {
                                musteragent_runtime_contract::CancelReason::CoordinationInterrupted
                            } else {
                                musteragent_runtime_contract::CancelReason::UserRequested
                            },
                        })
                    }
                    TerminalMutation::Failed { .. } => Ok(ActiveToolTerminal::Failed {
                        invocation_id: active.invocation_id,
                        expected_status: active.status,
                        error: error
                            .cloned()
                            .ok_or(PostgresAgentLoopJournalError::CorruptState)?,
                    }),
                    TerminalMutation::Completed => Err(PostgresAgentLoopJournalError::CorruptState),
                }
            })
            .collect()
    }

    async fn persist_active_tool_terminals(
        &self,
        transaction: &mut sqlx::PgConnection,
        tenant_id: musteragent_identity::TenantId,
        terminals: &[ActiveToolTerminal],
        now: UtcTimestamp,
    ) -> Result<(), PostgresAgentLoopJournalError> {
        for terminal in terminals {
            let invocation_id = terminal.invocation_id();
            let expected_status = terminal.expected_status();
            let (status, finished_at, terminal_error, terminal_reason, reconciliation_ref) =
                match terminal {
                    ActiveToolTerminal::Cancelled { .. } => {
                        (7_i16, Some(now.unix_millis()), None, None, None)
                    }
                    ActiveToolTerminal::Failed { error, .. } => (
                        5_i16,
                        Some(now.unix_millis()),
                        Some(error.code.as_str().as_bytes()),
                        None,
                        None,
                    ),
                    ActiveToolTerminal::Indeterminate {
                        reason,
                        protected_reconciliation_ref,
                        ..
                    } => (
                        6_i16,
                        None,
                        None,
                        Some(reason.as_str().as_bytes()),
                        Some(protected_reconciliation_ref.as_slice()),
                    ),
                };
            let updated = sqlx::query(
                "UPDATE tool.tool_invocations
                 SET status = $3, finished_at_ms = $4, terminal_error = $5,
                     terminal_reason = $6, reconciliation_ref_protected = $7,
                     indeterminate_observed_at_ms = CASE WHEN $3 = 6 THEN $8
                         ELSE indeterminate_observed_at_ms END
                 WHERE invocation_id = $1 AND status = $2",
            )
            .bind(invocation_id.into_uuid())
            .bind(expected_status)
            .bind(status)
            .bind(finished_at)
            .bind(terminal_error)
            .bind(terminal_reason)
            .bind(reconciliation_ref)
            .bind(now.unix_millis())
            .execute(&mut *transaction)
            .await?;
            require_one(updated.rows_affected())?;
            let closed_interactions = sqlx::query(
                "UPDATE approval.interactions
                 SET status = 4, closed_at_ms = $2
                 WHERE requested_by_invocation_id = $1 AND status = 1
                   AND tenant_id = $3
                 RETURNING interaction_id, run_id",
            )
            .bind(invocation_id.into_uuid())
            .bind(now.unix_millis())
            .bind(tenant_id.into_uuid())
            .fetch_all(&mut *transaction)
            .await?;
            for interaction in closed_interactions {
                crate::dialogue_publication::interaction::changed(
                    transaction,
                    tenant_id,
                    musteragent_identity::InteractionId::from_uuid(
                        interaction.try_get("interaction_id")?,
                    )
                    .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                    musteragent_identity::RunId::from_uuid(interaction.try_get("run_id")?)
                        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?,
                )
                .await
                .map_err(|error| PostgresExecutionError::InteractionPublication(Box::new(error)))?;
            }
            self.audit
                .append_tool_fact_in_transaction(transaction, tenant_id, invocation_id)
                .await?;
        }
        Ok(())
    }
}

fn runtime_operator_reconciliation_ref(
    keys: &musteragent_storage_protection::OpaqueRefKeyring,
    invocation_id: InvocationId,
) -> Result<(OpaqueExternalRef, Vec<u8>), PostgresAgentLoopJournalError> {
    let reference = OpaqueExternalRef::new(format!("runtime-operator:{invocation_id}"))
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?;
    let context = format!("tool-invocation:{invocation_id}:reconciliation");
    let protected = keys
        .seal(context.as_bytes(), reference.as_str())
        .map_err(|_| PostgresAgentLoopJournalError::CorruptState)?
        .as_bytes()
        .to_vec();
    Ok((reference, protected))
}
