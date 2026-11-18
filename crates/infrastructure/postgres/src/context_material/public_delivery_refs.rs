use super::*;

impl PostgresContextMaterialSource {
    pub(super) async fn project_public_delivery_refs(
        &self,
        context: &RunContext,
        snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
        entries: &mut [ContextEntry],
    ) -> Result<(), PostgresContextMaterialError> {
        use musteragent_execution_context::{RunOutputContract, WorkItemResultStep};
        let contract = snapshot
            .prompt
            .sections
            .iter()
            .find(|section| section.section_key.as_str() == "dialogue.output-contract")
            .and_then(|section| RunOutputContract::from_prompt_ref(&section.content_ref));
        let private = matches!(
            contract,
            Some(RunOutputContract::TaskResult(
                WorkItemResultStep::SubmitV1 | WorkItemResultStep::ReturnToRequester
            ))
        );
        if !private
            && !matches!(contract,Some(RunOutputContract::Dialogue(c)) if c.uses_tool_result_delivery_references())
        {
            return Ok(());
        }
        let calls = entries
            .iter()
            .filter(|entry| {
                entry.source_run_id == Some(context.run_id)
                    && entry.message.role == musteragent_runtime_model::CanonicalRole::Tool
            })
            .filter_map(|entry| {
                entry
                    .message
                    .tool_call_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned())
            })
            .collect::<Vec<_>>();
        if private && !calls.is_empty() {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Context,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await?;
            let references =
                crate::artifact::task_result::private_context_references(&mut tx, context, &calls)
                    .await
                    .map_err(|error| PostgresContextMaterialError::DialogueAuthority {
                        stage: "verified_task_result_locator",
                        error,
                    })?;
            tx.commit().await?;
            for entry in entries
                .iter_mut()
                .filter(|e| e.source_run_id == Some(context.run_id))
            {
                if let Some(items) = entry
                    .message
                    .tool_call_id
                    .as_ref()
                    .and_then(|call| references.get(call))
                {
                    musteragent_context::project_dialogue_publication_references(
                        &mut entry.message,
                        items,
                    )
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                }
            }
            return Ok(());
        }
        let references = crate::dialogue_publication::context_refs::load(
            &self.pool,
            context.tenant_id,
            context.run_id,
            &calls,
        )
        .await?;
        for entry in entries {
            if entry.source_run_id != Some(context.run_id) {
                continue;
            }
            if let Some(references) = entry
                .message
                .tool_call_id
                .as_ref()
                .and_then(|id| references.get(id))
            {
                musteragent_context::project_public_delivery_references(
                    &mut entry.message,
                    references,
                )
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            }
        }
        Ok(())
    }
}
