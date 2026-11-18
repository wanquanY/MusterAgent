//! Resolve current instructions at each physical model-planning boundary.
//! Admission snapshots and already committed canonical requests remain immutable.
use super::*;
use musteragent_context::AgentInstructionMaterial;
use std::num::NonZeroU64;

#[derive(Default)]
pub(super) struct CurrentDialogueContext {
    pub(super) instructions: Option<AgentInstructionMaterial>,
    pub(super) approvals: Option<musteragent_execution_context::DialogueApprovalContext>,
    pub(super) executions: Option<musteragent_execution_context::DialogueExecutionContext>,
}

impl PostgresContextMaterialSource {
    pub(super) fn current_dialogue_context<'a>(
        &'a self,
        input: &'a ContextPlanInput,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<CurrentDialogueContext, PostgresContextMaterialError>,
    > {
        Box::pin(async move {
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Context,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await?;
            // Original Run/task ownership binds agent+account; never accept an Agent ID from a prompt.
            let Some(cut) =
                crate::dialogue_context::run::run_cut(&mut tx, input.attempt.run_id, false)
                    .await
                    .map_err(|error| {
                        PostgresContextMaterialError::dialogue_authority(
                            "dialogue_instruction_authority",
                            error,
                        )
                    })?
            else {
                return Ok(CurrentDialogueContext::default());
            };
            let row = sqlx::query("SELECT c.instructions_revision,c.instructions_content_ref FROM agent.owned_agent_configurations c JOIN agent.owned_agents a USING(agent_id) JOIN agent.owner_spaces s USING(space_id) WHERE c.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND a.status='active'")
            .bind(cut.agent_id.into_uuid()).bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid())
            .fetch_optional(&mut *tx).await?.ok_or(PostgresContextMaterialError::CorruptState)?;
            let revision = NonZeroU64::new(u64::try_from(
                row.try_get::<i64, _>("instructions_revision")?,
            )?)
            .ok_or(PostgresContextMaterialError::CorruptState)?;
            let reference = row
                .try_get::<Option<Vec<u8>>, _>("instructions_content_ref")?
                .map(crate::dialogue_codec::content_from_bytes)
                .transpose()
                .map_err(|_| PostgresContextMaterialError::CorruptState)?;
            let actor = musteragent_identity::Actor {
                tenant_id: cut.tenant_id,
                principal_id: cut.owner_id,
                principal_kind: musteragent_identity::PrincipalKind::User,
            };
            let approvals = crate::dialogue_context::approval_context::load(&mut tx, actor, &cut)
                .await
                .map_err(|e| {
                    PostgresContextMaterialError::dialogue_authority("dialogue_approval_context", e)
                })?;
            let executions = crate::dialogue_execution_activity::context::load(
                &mut tx,
                actor,
                &cut,
                input.attempt.run_id,
                false,
            )
            .await
            .map_err(|e| {
                PostgresContextMaterialError::dialogue_authority("dialogue_execution_context", e)
            })?;
            let scene_members = crate::owned_agent::scene_members(&mut tx, actor, cut.agent_id)
                .await
                .map_err(|e| {
                    PostgresContextMaterialError::dialogue_authority("agent_scene_directory", e)
                })?;
            tx.commit().await?;
            let content = match reference {
                None => None,
                Some(content_ref) => {
                    let bytes = self.content.read(cut.tenant_id, &content_ref).await?;
                    verify_content(&content_ref, &bytes)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                    Some(MaterializedContent { content_ref, bytes })
                }
            };
            Ok(CurrentDialogueContext {
                instructions: Some(AgentInstructionMaterial {
                    agent_id: cut.agent_id,
                    scene_members,
                    revision,
                    content,
                }),
                approvals: Some(approvals),
                executions: Some(executions),
            })
        })
    }
}
