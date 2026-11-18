//! Read the admission-sealed identity through the original material owner.
use super::*;
impl PostgresToolExecutionMaterialSource {
    pub(crate) async fn original_receipt_material(
        &self,
        attempt: &AttemptContext,
        key: &musteragent_tool::ToolKey,
    ) -> Result<
        (
            Actor,
            RunContext,
            musteragent_runtime_snapshot::ToolDefinitionSnapshot,
        ),
        ToolExecutionPortError,
    > {
        async {
            let source = self.load_run(attempt).await?;
            let snapshot = self
                .snapshots
                .decode_runtime_snapshot(&source.encoded_snapshot)
                .map_err(|_| MaterialError::Corrupt)?;
            snapshot
                .validate_sealed()
                .map_err(|_| MaterialError::Corrupt)?;
            if source
                .owned_agent_id
                .is_some_and(|agent| agent != snapshot.agent.agent_id)
            {
                return Err(MaterialError::Corrupt);
            }
            let resolved = musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(
                &snapshot,
                self.application_catalogs.as_deref(),
            )
            .await
            .map_err(MaterialError::application)?;
            snapshot
                .validate_resolved_tool_catalog(&resolved.catalog)
                .map_err(|_| MaterialError::Corrupt)?;
            let definition = resolved
                .executable(&snapshot, self.application_catalogs.as_deref(), key)
                .await
                .map_err(MaterialError::application)?;
            Ok((source.actor, source.run_context, definition))
        }
        .await
        .map_err(MaterialError::port)
    }
}
