//! Restricted P5 port uses the same fenced admission; it never invents a Tool
//! invocation or Tool success to represent an extension's spawn operation.
use super::*;
use musteragent_extension::{
    ExtensionContext, HookError, SubagentReceipt, SubagentRequest, SubagentSpawner,
};
use std::collections::BTreeSet;

struct RestrictedSpawner {
    owner: PostgresCloudDelegation,
    agents: BTreeSet<String>,
}
impl PostgresCloudDelegation {
    /// Empty allow-lists deny all extension spawning. Tool spawning continues
    /// to use its own sealed capability grant and configurable parent limits.
    pub fn restricted_spawner(
        &self,
        agents: impl IntoIterator<Item = String>,
    ) -> Arc<dyn SubagentSpawner> {
        Arc::new(RestrictedSpawner {
            owner: self.clone(),
            agents: agents.into_iter().collect(),
        })
    }
    async fn extension_spawn(
        &self,
        parent: &ExtensionContext,
        request: SubagentRequest,
    ) -> Result<SubagentReceipt, Error> {
        if request.idempotency_key.is_empty() || request.idempotency_key.len() > 256 {
            return Err(Error::Invalid);
        }
        let operation = InvocationId::from_uuid(uuid::Uuid::new_v5(
            &parent.run.run_id.into_uuid(),
            format!("extension-spawn:{}", request.idempotency_key).as_bytes(),
        ))
        .map_err(|_| Error::Corrupt)?;
        let (mut tx, state) = crate::commit_lane::acquire(&self.database, &parent.attempt)
            .await
            .map_err(|_| Error::StaleAttempt)?;
        if parent.run.tenant_id != state.tenant_id
            || parent.run.conversation_id != state.conversation_id
            || parent.run.activity_id != state.activity_id
        {
            return Err(Error::PermissionDenied);
        }
        let source = crate::cloud_admission::source(&mut tx, parent.run.run_id)
            .await
            .map_err(source_error)?;
        let spec = SpawnSubagent {
            task: request.task,
            agent: Some(request.agent),
            deny_tools: Vec::new(),
            placement: DelegationPlacement::Cloud,
        };
        if let Some(row)=sqlx::query("SELECT child_run_id,spawn_spec FROM runtime.delegations WHERE parent_run_id=$1 AND parent_invocation_id=$2")
            .bind(parent.run.run_id.into_uuid()).bind(operation.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)? {
            if row.try_get::<serde_json::Value,_>("spawn_spec").map_err(db)?!=serde_json::to_value(&spec).map_err(|_|Error::Corrupt)? {return Err(Error::Invalid);}
            return Ok(SubagentReceipt {run_id:RunId::from_uuid(row.try_get("child_run_id").map_err(db)?).map_err(|_|Error::Corrupt)?});
        }
        let catalog = musteragent_tool::BuiltinToolCatalog::for_product_release(
            &musteragent_tool::compiled_product_release(ProductId::KERNEL),
            musteragent_tool::ProductExecutionTarget::Cloud,
        )
        .map_err(|_| Error::Corrupt)?;
        let definition = catalog
            .definitions()
            .iter()
            .find(|d| d.key.as_str() == "delegation.spawn@5")
            .ok_or(Error::Corrupt)?;
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        let input = ChildAdmissionIdentity {
            actor: source.actor,
            run_context: parent.run,
            invocation_id: operation,
            requested_at: now,
            result_schema: definition.output_schema.clone(),
        };
        let (mut tx, state, result) = self
            .stage_spawn(tx, state, &parent.attempt, input, source, spec, None)
            .await?;
        crate::tool_commit::atomic_effect::validate_live_state(&mut tx, &parent.attempt, state)
            .await
            .map_err(|_| Error::StaleAttempt)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(SubagentReceipt {
            run_id: result["run_id"]
                .as_str()
                .ok_or(Error::Corrupt)?
                .parse()
                .map_err(|_| Error::Corrupt)?,
        })
    }
}
impl SubagentSpawner for RestrictedSpawner {
    fn owns_lifecycle(&self) -> bool {
        true
    }
    fn spawn<'a>(
        &'a self,
        parent: &'a ExtensionContext,
        request: SubagentRequest,
    ) -> ApplicationFuture<'a, Result<SubagentReceipt, HookError>> {
        Box::pin(async move {
            if !self.agents.contains(&request.agent) {
                return Err(HookError::new(
                    "delegation.agent_not_allowed",
                    "Extension is not configured to use this agent",
                ));
            }
            self.owner
                .extension_spawn(parent, request)
                .await
                .map_err(|e| HookError::new(e.to_string(), e.to_string()))
        })
    }
}
