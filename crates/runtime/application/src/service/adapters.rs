//! Port trait adapters for the Runtime application service.

use super::*;

impl ConversationApplication for RuntimeApplication {
    fn create_conversation(
        &self,
        command: CommandEnvelope<musteragent_conversation::CreateConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.create_inner(command).await })
    }

    fn recover_conversation_creation(
        &self,
        command: CommandEnvelope<musteragent_conversation::CreateConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.recover_creation_inner(command).await })
    }

    fn submit_message(
        &self,
        command: CommandEnvelope<SubmitMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move {
            Err(runtime_error(
                ErrorCode::Unauthenticated,
                command.metadata.correlation_id,
                false,
                "Authorized admission is required",
            ))
        })
    }

    fn retract_message(
        &self,
        command: CommandEnvelope<RetractMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.retract_message_inner(command).await })
    }

    fn fork_context(
        &self,
        command: CommandEnvelope<ForkConversationContext>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.fork_context_inner(command).await })
    }

    fn activate_context_branch(
        &self,
        command: CommandEnvelope<ActivateConversationContextBranch>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.activate_context_branch_inner(command).await })
    }

    fn edit_and_resubmit_message(
        &self,
        command: CommandEnvelope<EditAndResubmitMessage>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move {
            Err(runtime_error(
                ErrorCode::Unauthenticated,
                command.metadata.correlation_id,
                false,
                "Authorized admission is required",
            ))
        })
    }

    fn archive_conversation(
        &self,
        command: CommandEnvelope<ArchiveConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.archive_conversation_inner(command).await })
    }

    fn unarchive_conversation(
        &self,
        command: CommandEnvelope<UnarchiveConversation>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.unarchive_conversation_inner(command).await })
    }

    fn set_conversation_title(
        &self,
        command: CommandEnvelope<SetConversationTitle>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.set_conversation_title_inner(command).await })
    }

    fn get_conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<musteragent_conversation::ConversationView, RuntimeError>>
    {
        Box::pin(async move {
            self.store
                .get_conversation(query.actor, query.conversation_id)
                .await
        })
    }

    fn list_conversations(
        &self,
        query: musteragent_conversation::ListConversations,
    ) -> ApplicationFuture<'_, Result<musteragent_conversation::ConversationPage, RuntimeError>>
    {
        Box::pin(async move { self.store.list_conversations(query).await })
    }
}

impl AdmissionApplication for RuntimeApplication {
    fn submit_message_authorized(
        &self,
        command: CommandEnvelope<SubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.submit_inner(command, signed_grant, selection).await })
    }

    fn edit_and_resubmit_message_authorized(
        &self,
        command: CommandEnvelope<EditAndResubmitMessage>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move {
            self.edit_and_resubmit_inner(command, signed_grant, selection)
                .await
        })
    }

    fn retry_run_authorized(
        &self,
        command: CommandEnvelope<RetryRun>,
        signed_grant: SignedExecutionGrant,
        selection: AdmissionSelection,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.retry_run_inner(command, signed_grant, selection).await })
    }
}

impl RunApplication for RuntimeApplication {
    fn resume_run(
        &self,
        command: CommandEnvelope<musteragent_run::ResumeRun>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.resume_run_inner(command).await })
    }

    fn cancel_run(
        &self,
        command: CommandEnvelope<musteragent_run::CancelRun>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.controls.cancel_run(command).await })
    }

    fn get_run(
        &self,
        query: musteragent_run::GetRun,
    ) -> ApplicationFuture<'_, Result<musteragent_run::RunView, RuntimeError>> {
        Box::pin(async move {
            // The authorized Run query is the source of retry settings. Composer
            // preferences belong to the next new message, not this execution.
            let mut view = self.store.get_run(query.actor, query.run_id).await?;
            let context = view.run.context;
            if context.activity_kind == musteragent_activity::ActivityKind::Chat
                && view.run.state.status() == musteragent_run::RunStatus::Failed
            {
                let snapshot = self
                    .store
                    .load_run_runtime_snapshot(
                        context.tenant_id,
                        context.conversation_id,
                        context.run_id,
                        context.runtime_snapshot_id,
                        None,
                        musteragent_identity::CorrelationId::generate(),
                    )
                    .await?;
                view.retry_configuration = Some(musteragent_run::RunRetryConfiguration {
                    source_product_release: snapshot.product_release.clone(),
                    source_capability_keys: snapshot
                        .capabilities
                        .iter()
                        .map(|c| c.key.as_str().to_owned())
                        .collect(),
                    requested_model_class: snapshot.model.requested_model_class.clone(),
                    reasoning_effort: snapshot.model.route.reasoning_effort().map(str::to_owned),
                    reasoning_summary: snapshot.output_policy.reasoning_summary,
                    permission_mode: snapshot.policy.permission_mode.as_str().to_owned(),
                });
            }
            Ok(view)
        })
    }

    fn enqueue_run_control(
        &self,
        command: CommandEnvelope<EnqueueRunControl>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.enqueue_run_control_inner(command).await })
    }
}

impl InteractionApplication for RuntimeApplication {
    fn respond_interaction(
        &self,
        command: CommandEnvelope<RespondInteraction>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.controls.respond_interaction(command).await })
    }

    fn cancel_interaction(
        &self,
        command: CommandEnvelope<musteragent_approval::CancelInteraction>,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.controls.cancel_interaction(command).await })
    }

    fn get_interaction(
        &self,
        query: GetInteraction,
    ) -> ApplicationFuture<'_, Result<InteractionRead, RuntimeError>> {
        Box::pin(async move {
            self.interactions
                .get_interaction(
                    query.actor,
                    query.interaction_id,
                    musteragent_identity::CorrelationId::generate(),
                )
                .await
        })
    }
}
