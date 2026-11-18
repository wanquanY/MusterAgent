use super::*;
use musteragent_context::ContextCompactionModelFailure;
use musteragent_loop::ExecutionBoundary;
use musteragent_runtime_model::SupervisedModelCompletion;

impl PersistedContextCompactor {
    #[must_use]
    pub fn with_execution_supervision(
        mut self,
        supervision: musteragent_loop::ExecutionSupervision,
    ) -> Self {
        self.supervision = Some(supervision);
        self
    }

    pub(super) async fn persist_completion(
        &self,
        completion: CompleteModelInvocation,
        supervised: Option<(&musteragent_run::InvocationContext, bool)>,
    ) -> Result<(), CompactionFailure> {
        match supervised {
            Some((context, native_terminal)) => {
                self.store
                    .complete_supervised(SupervisedModelCompletion {
                        context: context.clone(),
                        completion,
                        native_terminal,
                    })
                    .await
            }
            None => self.store.complete(completion).await,
        }
        .map_err(slot_error)?;
        Ok(())
    }

    pub(super) async fn compact_activity(
        &self,
        mut request: ContextCompactionRequest,
        slot: PersistedContextCompactionSlot,
        mut persisted: ModelInvocation,
    ) -> Result<musteragent_context::ContextCompactionStep, CompactionFailure> {
        let supervision = self
            .supervision
            .as_ref()
            .ok_or_else(CompactionFailure::invariant)?;
        request.invocation = slot.invocation;
        let start = self
            .store
            .begin_supervised(&request, &persisted)
            .await
            .map_err(slot_error)?;
        if start.disposition == ModelPersistenceDisposition::Duplicate {
            return Err(CompactionFailure::indeterminate());
        }
        let request = &request;
        self.invoker
            .invoke_supervised(
                request,
                start.record,
                supervision,
                async {
                    match self
                        .store
                        .mark_dispatched(request)
                        .await
                        .map_err(|e| slot_error(e).into_context())?
                    {
                        ModelPersistenceDisposition::Applied => Ok(()),
                        ModelPersistenceDisposition::Duplicate => {
                            Err(ContextPlanError::indeterminate())
                        }
                    }
                },
                |output, terminal| async move {
                    let output = match output {
                        Ok(output)
                            if output.response_model_key != persisted.requested_model_key =>
                        {
                            persisted.provider_request_ref = Some(output.provider_request_ref);
                            persisted.response_model_key = Some(output.response_model_key);
                            Err(ContextCompactionModelFailure {
                                error: ContextPlanError::invariant(),
                                usage: Some(output.usage),
                            })
                        }
                        output => output,
                    };
                    match output {
                        Ok(output) => match self
                            .complete_success(
                                persisted,
                                output,
                                &request.invocation,
                                Some(terminal),
                            )
                            .await
                        {
                            Ok(result) => ExecutionBoundary::Committed(Ok(result)),
                            Err(error) => interrupted(error),
                        },
                        Err(failure) => match self
                            .complete_failure(
                                &mut persisted,
                                CompactionFailure::from_context(failure.error.clone()),
                                failure.usage,
                                Some((&request.invocation, terminal)),
                            )
                            .await
                        {
                            Ok(()) => ExecutionBoundary::Committed(Err(failure.error)),
                            Err(error) => interrupted(error),
                        },
                    }
                },
            )
            .await
            .map_err(CompactionFailure::from_context)
    }
}
fn interrupted(
    error: CompactionFailure,
) -> ExecutionBoundary<Result<musteragent_context::ContextCompactionStep, ContextPlanError>> {
    ExecutionBoundary::Interrupted(
        musteragent_execution_liveness::LivenessJournalError {
            code: error.code,
            retryable: error.retryable,
            rejection: None,
        }
        .into(),
    )
}
