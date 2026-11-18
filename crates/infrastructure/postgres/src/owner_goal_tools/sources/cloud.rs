use super::*;

async fn authorize(
    tx: &mut sqlx::PgConnection,
    attempt: &AttemptContext,
    command: &DialogueToolInvocation,
    state: crate::commit_lane::CommitLaneState,
) -> Result<SourceScope, Error> {
    if super::super::authority::authorize(tx, attempt, command, state)
        .await?
        .is_some()
    {
        return Err(Error::Forbidden);
    }
    let source = crate::agent_self_configuration::source::load_direct_user_tool_source(
        tx,
        crate::agent_self_configuration::source::DirectToolInvocation {
            actor: command.actor,
            agent_id: command.agent_id,
            run_id: attempt.run_id,
            invocation_id: command.invocation_id,
            tool_call_id: &command.tool_call_id,
            request_ref: Some(&command.request_ref),
        },
        &KEYS,
    )
    .await
    .map_err(super::super::errors::source_error)?;
    let row=sqlx::query("SELECT * FROM agent.dialogue_user_intents WHERE intent_id=$1 AND tenant_id=$2 AND owner_id=$3")
        .bind(source.intent_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(command.actor.principal_id.into_uuid()).fetch_one(tx).await.map_err(|_|Error::Infrastructure)?;
    let intent =
        crate::dialogue_codec::intent_row(&row).map_err(super::super::errors::store_error)?;
    if intent.origin.surface == DialogueInputSurface::Schedule {
        return Err(Error::Forbidden);
    }
    Ok(SourceScope {
        subject: OwnerGoalSourceSubject {
            actor: command.actor,
            agent_id: command.agent_id,
            surface: intent.origin.surface,
        },
        intent_id: source.intent_id,
    })
}
impl PostgresOwnerGoalTools {
    pub(in crate::owner_goal_tools) async fn sources(
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
        let scope = authorize(&mut tx, attempt, &command, state).await?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        let plan = SourcePlanner {
            database: self.ledger.database().clone(),
            selection: self.source_selection.clone(),
        }
        .prepare(
            scope.clone(),
            &command.tool_key,
            &bytes,
            command.requested_at,
        )
        .await?;
        let result = self.management_result(&command, &plan.value).await?;
        let (mut tx, state) = crate::commit_lane::acquire(self.ledger.database(), attempt)
            .await
            .map_err(lane_error)?;
        if authorize(&mut tx, attempt, &command, state).await? != scope {
            return Err(Error::Forbidden);
        }
        if plan.change.is_some() {
            crate::resource_commit::lock_invocation(
                &mut tx,
                state.tenant_id,
                command.invocation_id,
                attempt,
            )
            .await
            .map_err(|_| Error::StaleFence)?;
            commit(&mut tx, plan).await?;
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
                    record_tag: b"owner-goal-source-configured@1",
                    idempotency_prefix: "owner-goal-source-tool",
                },
                result.clone(),
                Vec::new(),
            )
            .await
            .map_err(commit_error)?;
        } else {
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
        }
        Ok(DialogueToolResult {
            result_ref: result.result_ref,
            result_schema: result.result_schema,
            authorized_content: Vec::new(),
        })
    }
}
