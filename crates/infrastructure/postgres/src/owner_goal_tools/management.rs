//! Cloud admission and atomic Tool receipt; shared planning owns Goal semantics.
use super::*;
use serde_json::Value;

impl PostgresOwnerGoalTools {
    pub(super) async fn manage(
        &self,
        attempt: &AttemptContext,
        command: DialogueToolInvocation,
    ) -> Result<DialogueToolResult, Error> {
        let bytes = self
            .content
            .read(command.actor.tenant_id, &command.request_ref)
            .await
            .map_err(|_| Error::Infrastructure)?;
        verify_content(&command.request_ref, &bytes).map_err(|_| Error::Corrupt)?;
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        let own_goal = super::authority::authorize(&mut tx, attempt, &command, state).await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        let read = matches!(command.tool_key.as_str(), "goal.get@3" | "goal.list@2");
        let planned = planning::GoalToolPlanner {
            database: self.ledger.database().clone(),
            content: self.content.clone(),
        }
        .prepare(
            planning::GoalToolScope {
                actor: command.actor,
                agent_id: command.agent_id,
                own_goal,
            },
            &command.tool_key,
            &bytes,
            command.requested_at,
        )
        .await?;
        let result = self.management_result(&command, &planned.value).await?;
        // Source authority is rechecked after independent reads/content staging.
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        if super::authority::authorize(&mut tx, attempt, &command, state).await? != own_goal {
            return Err(Error::Forbidden);
        }
        if read {
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
        } else {
            crate::resource_commit::lock_invocation(
                &mut tx,
                state.tenant_id,
                command.invocation_id,
                attempt,
            )
            .await
            .map_err(|_| Error::StaleFence)?;
            effect::commit(
                &mut tx,
                planning::GoalToolScope {
                    actor: command.actor,
                    agent_id: command.agent_id,
                    own_goal,
                },
                planned.effect.ok_or(Error::Corrupt)?,
                effect::Custody {
                    run_id: attempt.run_id,
                    invocation_id: command.invocation_id,
                    device: false,
                },
            )
            .await?;
            crate::tool_commit::atomic_effect::AtomicEffectCommitter {
                ledger: &self.ledger,
                encoder: self.encoder.as_ref(),
                keys: self.keys.as_ref(),
                audit: self.audit.as_ref(),
                clock: self.clock.as_deref(),
            }
            .commit(
                tx,
                state,
                attempt,
                crate::tool_commit::atomic_effect::AtomicEffectIdentity {
                    actor: command.actor,
                    run_context: command.run_context,
                    invocation_id: command.invocation_id,
                    tool_call_id: &command.tool_call_id,
                    tool_key: &command.tool_key,
                    request_ref: &command.request_ref,
                    occurred_at: command.requested_at,
                    record_tag: b"owner-goal-managed@1",
                    idempotency_prefix: "owner-goal-tool",
                },
                result.clone(),
                Vec::new(),
            )
            .await
            .map_err(commit_error)?;
        }
        Ok(DialogueToolResult {
            result_ref: result.result_ref,
            result_schema: result.result_schema,
            authorized_content: planned.authorized_content,
        })
    }
    pub(super) async fn management_result(
        &self,
        command: &DialogueToolInvocation,
        value: &Value,
    ) -> Result<CapabilityResult, Error> {
        let bytes = serde_json::to_vec(value).map_err(|_| Error::Corrupt)?;
        let id = ContentId::from_uuid(uuid::Uuid::new_v5(
            &command.invocation_id.into_uuid(),
            format!("owner-goal-tool-result@1:{}", Sha256::digest(&bytes)).as_bytes(),
        ))
        .map_err(|_| Error::Corrupt)?;
        let result_ref = self
            .content
            .put_reclaimable_at(
                id,
                PutContent {
                    tenant_id: command.actor.tenant_id,
                    media_type: MediaType::new("application/json").map_err(|_| Error::Corrupt)?,
                    bytes,
                },
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: command.result_schema.clone(),
            provider_request_ref: None,
        })
    }
}
