use super::*;
pub struct Journal {
    context: ExtensionContext,
    home: Arc<Home>,
    ledger: Arc<musteragent_ledger::InMemoryLedger<Clock>>,
    checkpoint: Mutex<DurableLoopCheckpoint>,
    pub results: Mutex<Vec<ToolResultCommit>>,
    pub tools: Mutex<Option<Arc<executor::ToolFixture>>>,
    pub calls: Mutex<Vec<PlannedToolCall>>,
    pub batches: Mutex<Vec<ModelEventBatch>>,
    pub sealed: Mutex<Vec<CanonicalModelRequest>>,
    pub failures: Mutex<Vec<ErrorCode>>,
}
impl Journal {
    pub fn new(
        context: ExtensionContext,
        home: Arc<Home>,
        ledger: Arc<musteragent_ledger::InMemoryLedger<Clock>>,
    ) -> Self {
        Self {
            ledger,
            checkpoint: Mutex::new(checkpoint(&context)),
            context,
            home,
            results: Mutex::new(vec![]),
            tools: Mutex::new(None),
            calls: Mutex::new(vec![]),
            batches: Mutex::new(vec![]),
            sealed: Mutex::new(vec![]),
            failures: Mutex::new(vec![]),
        }
    }
    fn append_fact(&self, topic: &str, payload: Vec<u8>) {
        use musteragent_event_envelope::{LedgerRecordKind, Producer};
        use musteragent_ledger::{AppendRequest, ProposedOutbox, ProposedRecord};
        let hash = Sha256::digest(&payload);
        let record_id = LedgerRecordId::generate();
        self.ledger
            .append(AppendRequest {
                tenant_id: self.context.run.tenant_id,
                conversation_id: self.context.run.conversation_id,
                expected_home_epoch: self.context.run.home_epoch,
                expected_offset: self
                    .ledger
                    .current_offset(self.context.run.conversation_id)
                    .unwrap_or(LedgerOffset::ZERO),
                idempotency_key: IdempotencyKey::new(format!("test:{record_id}"))
                    .expect("valid fixture and successful tested operation"),
                command_fingerprint: hash,
                records: vec![ProposedRecord {
                    schema_version: DottedKey::new(topic)
                        .expect("valid fixture and successful tested operation"),
                    record_id,
                    record_kind: LedgerRecordKind::DomainEvent,
                    activity_id: None,
                    run_id: None,
                    run_seq: None,
                    attempt_id: None,
                    participant_id: None,
                    producer: Producer::RuntimeApi,
                    occurred_at: UtcTimestamp::from_unix_millis(1000),
                    causation_id: None,
                    correlation_id: CorrelationId::generate(),
                    payload: payload.clone(),
                    payload_hash: hash,
                }],
                outbox: vec![ProposedOutbox {
                    message_id: OutboxMessageId::generate(),
                    topic: DottedKey::new(topic)
                        .expect("valid fixture and successful tested operation"),
                    payload,
                    payload_hash: hash,
                }],
            })
            .expect("valid fixture and successful tested operation");
    }
    fn snapshot(&self) -> DurableLoopCheckpoint {
        lock(&self.checkpoint).clone()
    }
    fn validate(&self, a: &AttemptContext) {
        assert_eq!(a, &self.context.attempt);
    }
}
fn unavailable() -> LoopPortError {
    LoopPortError {
        code: ErrorCode::CapabilityUnavailable,
        retryable: false,
    }
}
impl AgentLoopJournal for Journal {
    fn recovery_budget(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RecoveryBudget, LoopPortError>> {
        Box::pin(async { Ok(RecoveryBudget::default()) })
    }
    fn load_checkpoint(
        &self,
        a: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.validate(a);
        Box::pin(async { Ok(self.snapshot()) })
    }
    fn load_model_observation(
        &self,
        _: &AttemptContext,
        id: InvocationId,
    ) -> ApplicationFuture<'_, Result<ModelInvocationObservation, LoopPortError>> {
        Box::pin(async move {
            let mut observation = ModelInvocationObservation {
                invocation_id: id,
                provider_response_ref: None,
                response_model_key: None,
                usage: None,
                public_output_observed: false,
                replay_barrier_observed: false,
            };
            for event in lock(&self.batches)
                .iter()
                .filter(|b| b.invocation_id == id)
                .flat_map(|b| &b.events)
            {
                observation.public_output_observed |= event.is_public_output();
                observation.replay_barrier_observed |= event.blocks_automatic_replay();
                match event {
                    ModelStreamEvent::ResponseStarted {
                        response_ref,
                        response_model_key,
                    } => {
                        observation.provider_response_ref = Some(response_ref.clone());
                        observation.response_model_key = Some(response_model_key.clone());
                    }
                    ModelStreamEvent::Usage(usage) => observation.usage = Some(*usage),
                    _ => {}
                }
            }
            Ok(observation)
        })
    }
    fn recover_checkpoint(
        &self,
        _: &AttemptContext,
        checkpoint: DurableLoopCheckpoint,
        _: ModelRecoveryConstraints,
    ) -> ApplicationFuture<'_, Result<RecoveredLoopState, LoopPortError>> {
        Box::pin(async move {
            Ok(RecoveredLoopState {
                pending_model_recovery: None,
                checkpoint,
                pending_tool_calls: vec![],
                recovered_model_completion: None,
                replacement_attempt_scheduled: false,
            })
        })
    }
    fn begin_model(
        &self,
        a: &AttemptContext,
        i: &ModelIteration,
        _: ProviderRecoveryCapability,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.validate(a);
        let i = i.clone();
        Box::pin(async move {
            self.home.begin(
                &self.context,
                i.invocation_id,
                i.request_ref.sha256,
                Arc::new(
                    execution_snapshot()
                        .liveness
                        .expect("valid fixture and successful tested operation")
                        .model_policy(),
                ),
            );
            let mut c = lock(&self.checkpoint);
            c.iteration = i.iteration;
            c.phase = LoopPhase::ModelInFlight {
                invocation_id: i.invocation_id,
            };
            Ok(c.clone())
        })
    }
    fn mark_model_dispatched(
        &self,
        _: &AttemptContext,
        _: InvocationId,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async { Ok(self.snapshot()) })
    }
    fn append_model_events(
        &self,
        _: &AttemptContext,
        batch: ModelEventBatch,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.batches).push(batch);
            Ok(self.snapshot())
        })
    }
    fn stage_model_completion(
        &self,
        _: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            self.home.close(completion.invocation_id);
            Ok(self.snapshot())
        })
    }
    fn finish_model(
        &self,
        _: &AttemptContext,
        completion: ModelCompletionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            let mut c = lock(&self.checkpoint);
            c.phase = if completion.kind == ModelCompletionKind::ToolCalls {
                LoopPhase::ToolsPending {
                    invocation_id: completion.invocation_id,
                    remaining_tool_call_ids: completion
                        .tool_calls
                        .iter()
                        .map(|c| c.call_id.clone())
                        .collect(),
                }
            } else {
                LoopPhase::ReadyForModel
            };
            lock(&self.calls).extend(completion.tool_calls);
            Ok(c.clone())
        })
    }
    fn finish_failed_model(
        &self,
        a: &AttemptContext,
        commit: ModelFailureCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.home.close(commit.observation.invocation_id);
        self.fail(
            a,
            commit.provider_failure.code,
            false,
            None,
            commit.recovery_policy,
        )
    }
    fn finish_rejected_model_action(
        &self,
        _: &AttemptContext,
        _: ModelActionRejectionCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async { Err(unavailable()) })
    }
    fn commit_tool_result(
        &self,
        a: &AttemptContext,
        result: ToolResultCommit,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        self.validate(a);
        Box::pin(async move {
            let mut c = lock(&self.checkpoint);
            let LoopPhase::ToolsPending {
                remaining_tool_call_ids,
                ..
            } = &mut c.phase
            else {
                return Err(unavailable());
            };
            remaining_tool_call_ids.retain(|id| id != &result.call_id);
            if remaining_tool_call_ids.is_empty() {
                c.phase = LoopPhase::ReadyForModel;
            }
            c.completed_tool_calls += 1;
            self.append_fact(
                "test.tool.result",
                format!("{:?}", result.message).into_bytes(),
            );
            lock(&self.results).push(result);
            Ok(c.clone())
        })
    }
    fn wait_for_reconciliation(
        &self,
        _: &AttemptContext,
        _: InvocationId,
        _: UtcTimestamp,
        _: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), LoopPortError>> {
        Box::pin(async { Err(unavailable()) })
    }
    fn complete(
        &self,
        _: &AttemptContext,
    ) -> ApplicationFuture<'_, Result<RunCompletionDisposition, LoopPortError>> {
        Box::pin(async {
            lock(&self.checkpoint).phase = LoopPhase::Completed;
            self.append_fact("test.run.completed", b"completed".to_vec());
            Ok(RunCompletionDisposition::Completed(self.snapshot()))
        })
    }
    fn fail(
        &self,
        _: &AttemptContext,
        code: ErrorCode,
        _: bool,
        _: Option<u64>,
        _: musteragent_runtime_snapshot::RecoveryPolicySnapshot,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async move {
            lock(&self.failures).push(code);
            lock(&self.checkpoint).phase = LoopPhase::Failed;
            Ok(self.snapshot())
        })
    }
    fn interrupt_external_outcome(
        &self,
        _: &AttemptContext,
        _: OpaqueExternalRef,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async { Err(unavailable()) })
    }
    fn acknowledge_coordination_interrupt(
        &self,
        _: &AttemptContext,
        _: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async { Err(unavailable()) })
    }
    fn acknowledge_cancellation(
        &self,
        _: &AttemptContext,
        _: Option<ModelInvocationObservation>,
    ) -> ApplicationFuture<'_, Result<DurableLoopCheckpoint, LoopPortError>> {
        Box::pin(async {
            lock(&self.checkpoint).phase = LoopPhase::Cancelled;
            Ok(self.snapshot())
        })
    }
}

impl ToolHistory for Journal {
    fn requested<'a>(
        &'a self,
        context: &'a ExtensionContext,
        call_id: &'a str,
    ) -> ApplicationFuture<'a, Result<bool, ExtensionError>> {
        Box::pin(async move {
            if context.attempt != self.context.attempt {
                return Err(ExtensionError::IdentityMismatch);
            }
            Ok(lock(&self.tools).as_ref().is_some_and(|tools| {
                lock(&tools.invocations)
                    .values()
                    .any(|i| i.invocation.tool_call_id.as_str() == call_id)
            }))
        })
    }
    fn recent<'a>(
        &'a self,
        context: &'a ExtensionContext,
        through: Option<&'a str>,
        limit: usize,
        result_tools: &'a BTreeSet<String>,
    ) -> ApplicationFuture<'a, Result<Vec<ToolHistoryEntry>, ExtensionError>> {
        Box::pin(async move {
            if context.attempt != self.context.attempt {
                return Err(ExtensionError::IdentityMismatch);
            }
            let calls = lock(&self.calls);
            let end = through
                .and_then(|id| calls.iter().position(|c| c.call_id.as_str() == id))
                .map_or(calls.len(), |i| i + 1);
            let results = lock(&self.results);
            let requested: BTreeSet<_> = lock(&self.tools)
                .as_ref()
                .map(|tools| {
                    lock(&tools.invocations)
                        .values()
                        .map(|i| i.invocation.tool_call_id.clone())
                        .collect()
                })
                .unwrap_or_default();
            let calls: Vec<_> = calls[..end]
                .iter()
                .filter(|call| {
                    through == Some(call.call_id.as_str())
                        || requested.contains(&call.call_id)
                        || results.iter().any(|result| result.call_id == call.call_id)
                })
                .collect();
            Ok(calls[calls.len().saturating_sub(limit)..]
                .iter()
                .map(|call| ToolHistoryEntry {
                    call: ToolSample {
                        call_id: call.call_id.as_str().into(),
                        tool: call.name.as_str().into(),
                        arguments: canonical_arguments(&call.arguments),
                    },
                    result: results
                        .iter()
                        .find(|r| {
                            r.call_id == call.call_id && result_tools.contains(call.name.as_str())
                        })
                        .map(|r| {
                            serde_json::json!(
                                r.message
                                    .content
                                    .iter()
                                    .map(|p| match p {
                                        CanonicalContentPart::Text(t) => t.as_str(),
                                        _ => "attachment",
                                    })
                                    .collect::<Vec<_>>()
                            )
                        }),
                })
                .collect())
        })
    }
}
