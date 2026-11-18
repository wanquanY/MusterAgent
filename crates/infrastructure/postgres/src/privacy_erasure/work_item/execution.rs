//! Preparation never deletes bytes while an original producer is still live.
use super::*;

pub(super) async fn require_reserved_task(
    tx: &mut sqlx::PgConnection,
    command: &RequestWorkItemErasure,
) -> Result<(), Error> {
    // Preserve the original v1 refusal on deployments that predate Task Run
    // admission; absence of the new Owner is never an authorization shortcut.
    let installed: bool = sqlx::query_scalar(
        "SELECT to_regclass('collaboration.work_item_execution_reservations') IS NOT NULL",
    )
    .fetch_one(&mut *tx)
    .await?;
    if !installed {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    let reserved: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations x JOIN collaboration.work_items t USING(work_item_id) WHERE x.work_item_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3 AND x.space_id=$4 AND x.requesting_agent_id=$5 AND t.tenant_id=x.tenant_id AND t.owner_id=x.owner_id AND t.space_id=x.space_id AND t.agent_id=x.requesting_agent_id)")
        .bind(command.work_item_id.into_uuid()).bind(command.actor.tenant_id.into_uuid())
        .bind(command.actor.principal_id.into_uuid()).bind(command.space_id.into_uuid())
        .bind(command.agent_id.into_uuid()).fetch_one(&mut *tx).await?;
    if !reserved {
        return Err(Error::WorkItemExecutionClosureRequired);
    }
    Ok(())
}

impl PostgresPrivacyErasureStore {
    pub(crate) async fn prepare_work_item_execution_erasure(
        &self,
        job: ErasureJob,
        now: UtcTimestamp,
    ) -> Result<ErasureJob, Error> {
        if job.request.schema_version.as_str() != WORK_ITEM_EXECUTION_ERASURE_SCHEMA {
            return Ok(job);
        }
        let tenant = job.request.tenant_id;
        let job_id = job.request.erasure_job_id;
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        super::super::recovery::require_execution_claim(
            &mut tx,
            tenant,
            job_id,
            self.execution_claim,
        )
        .await?;
        let lineage = load_lineage(&mut tx, tenant, job_id)
            .await?
            .ok_or(Error::WorkItemExecutionClosureRequired)?;
        let rows = sqlx::query("SELECT r.run_id,r.conversation_id,r.home_epoch::text AS home_epoch,revision.control_revision,r.status,EXISTS(SELECT 1 FROM runtime.leases lease WHERE lease.run_id=r.run_id) AS leased
            FROM agent.dialogue_sessions s JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
            JOIN collaboration.work_items t ON t.work_item_id=s.work_item_id AND t.tenant_id=s.tenant_id AND t.owner_id=s.owner_id
            JOIN collaboration.work_item_revisions revision ON revision.work_item_id=t.work_item_id AND revision.state_revision=t.state_revision
            WHERE s.work_item_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND s.subject_kind IN ('work_item','work_item_report') AND t.erasure_job_id=$4 ORDER BY r.run_id")
            .bind(lineage.work_item_id.into_uuid()).bind(tenant.into_uuid()).bind(lineage.actor.principal_id.into_uuid())
            .bind(job_id.into_uuid()).fetch_all(&mut *tx).await?;
        let mut cancellations = Vec::new();
        // A Native source may already have been materialized before local Run
        // admission. Neither terminal nor NotAdmitted proves those bytes erased.
        // Keep the original job pending until the Device erasure owner supplies
        // complete coverage; do not let the Cloud-only inventory report success.
        let mut waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM collaboration.work_item_device_custody d JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) WHERE d.work_item_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3)",
        )
        .bind(lineage.work_item_id.into_uuid())
        .bind(tenant.into_uuid())
        .bind(lineage.actor.principal_id.into_uuid())
        .fetch_one(&mut *tx).await?;
        for row in rows {
            let status: i16 = row.try_get("status")?;
            let leased: bool = row.try_get("leased")?;
            waiting |= !matches!(status, 5..=7) || leased;
            if !matches!(status, 5..=7) {
                cancellations.push(WorkItemErasureCancellation {
                    actor: lineage.actor,
                    job_id,
                    task_id: lineage.work_item_id,
                    control_revision: super::super::from_i64(
                        row.try_get("control_revision")?,
                        "control revision",
                    )?,
                    run_id: RunId::from_uuid(row.try_get("run_id")?)
                        .map_err(|_| Error::CorruptState("run id"))?,
                    conversation_id: ConversationId::from_uuid(row.try_get("conversation_id")?)
                        .map_err(|_| Error::CorruptState("conversation id"))?,
                    home_epoch: HomeEpoch::new(
                        row.try_get::<String, _>("home_epoch")?
                            .parse()
                            .map_err(|_| Error::CorruptState("home epoch"))?,
                    )
                    .map_err(|_| Error::CorruptState("home epoch"))?,
                    requested_at: lineage.frozen_at,
                });
            }
        }
        // Release the erasure job/queue transaction before the original Cancel
        // application takes target Ledger then Task locks. No pool nesting.
        tx.commit().await?;
        if !cancellations.is_empty() {
            let control = self
                .work_item_control
                .as_ref()
                .ok_or(Error::WorkItemExecutionClosureRequired)?;
            for cancellation in cancellations {
                control
                    .cancel(cancellation)
                    .await
                    .map_err(|_| Error::WorkItemExecutionClosureRequired)?;
            }
        }
        if waiting {
            return Err(Error::WorkItemExecutionClosureRequired);
        }
        let prepared = self.freeze_work_item_execution_coverage(job, now).await?;
        // Source credentials may already have been revoked. Erasure is an
        // internal original-owner operation, not a new source read capability.
        self.erase_source_evidence(&prepared).await?;
        Ok(prepared)
    }

    async fn freeze_work_item_execution_coverage(
        &self,
        job: ErasureJob,
        now: UtcTimestamp,
    ) -> Result<ErasureJob, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::PrivacyErasure)
            .await?;
        super::super::recovery::require_execution_claim(
            &mut tx,
            job.request.tenant_id,
            job.request.erasure_job_id,
            self.execution_claim,
        )
        .await?;
        let lineage = load_lineage(&mut tx, job.request.tenant_id, job.request.erasure_job_id)
            .await?
            .ok_or(Error::WorkItemExecutionClosureRequired)?;
        // Same order as public source writers: Space, participating Dialogues,
        // then Task/provider rows. This serializes source-page validation and
        // checkpoint publication with the erase cut without cancelling User runs.
        sqlx::query("SELECT space_id FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR UPDATE")
            .bind(lineage.space_id.into_uuid()).bind(lineage.actor.tenant_id.into_uuid())
            .bind(lineage.actor.principal_id.into_uuid()).fetch_one(&mut *tx).await?;
        let _:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT dialogue_id FROM agent.agent_dialogues WHERE tenant_id=$1 AND owner_id=$2 AND (dialogue_id=$3 OR dialogue_id IN (SELECT executing_dialogue_id FROM collaboration.work_item_execution_reservations WHERE work_item_id=$4)) ORDER BY dialogue_id FOR UPDATE")
            .bind(lineage.actor.tenant_id.into_uuid()).bind(lineage.actor.principal_id.into_uuid())
            .bind(lineage.source_dialogue_id.into_uuid()).bind(lineage.work_item_id.into_uuid()).fetch_all(&mut *tx).await?;
        let scope = super::coverage::provider_scope(&mut tx, &lineage)
            .await
            .inspect_err(|_| tracing::warn!(stage = "scope", "Task erasure closure incomplete"))?;
        if super::frozen::load(&mut tx, &lineage, &scope)
            .await?
            .is_some()
        {
            // The preparation transaction commits the entire redaction cut.
            // Recovery reuses that frozen cut; it never inventories fewer roots
            // after a previous executor has already purged their bodies.
            let reloaded = super::super::loading::load_jobs(
                &mut tx,
                job.request.tenant_id,
                &[job.request.erasure_job_id.into_uuid()],
            )
            .await?
            .pop()
            .ok_or(Error::JobNotFound)?;
            tx.commit().await?;
            return Ok(reloaded);
        }
        let source_commands = super::source_commands::collect(&mut tx, &scope, None).await?;
        let private = super::private_payload::collect(&mut tx, &scope)
            .await
            .inspect_err(|_| {
                tracing::warn!(stage = "private", "Task erasure closure incomplete")
            })?;
        let public =
            crate::dialogue_publication::work_item_erasure::collect(&mut tx, &scope).await?;
        let replies = crate::agent_dialogue::reply_retention::collect(
            &mut tx,
            lineage.actor,
            &scope,
            &public,
        )
        .await?;
        let context = crate::dialogue_context::work_item_erasure::collect(
            &mut tx,
            &scope,
            &public.entries,
            None,
        )
        .await?;
        let intents =
            crate::loop_journal::collect_work_item_completion_intents(&mut tx, &scope).await?;
        let resources = crate::resource::work_item_erasure::collect(&mut tx, &scope).await?;
        let artifacts = crate::artifact::collect_work_item_artifacts(&mut tx, &scope).await?;
        let memories = crate::memory::collect_work_item_memories(&mut tx, &scope).await?;
        let mut contents = lineage.goal_content_refs.clone();
        contents.extend(private);
        contents.extend(source_commands.contents.iter().cloned());
        contents.extend(resources.contents.iter().cloned());
        contents.extend(public.contents.iter().cloned());
        contents.extend(context.contents.iter().cloned());
        contents.extend(scope.report_contents().iter().cloned());
        contents.extend(intents);
        contents.extend(artifacts.version_contents.iter().cloned());
        contents.extend(memories.version_contents.iter().cloned());
        super::contents::require_exclusive(
            &mut tx,
            &lineage,
            &scope,
            &contents,
            &artifacts.version_ids,
            &memories.version_ids,
            &public.entries,
            &context.checkpoints,
            &context.bindings,
            &source_commands,
            &context,
            &replies,
        )
        .await
        .inspect_err(|_| tracing::warn!(stage = "exclusive", "Task erasure closure incomplete"))?;
        let frozen = super::frozen::persist(&mut tx, &job, &lineage, &scope, contents, now).await?;
        crate::agent_dialogue::reply_retention::erase(&mut tx, lineage.actor, &replies).await?;
        let job_id = job.request.erasure_job_id;
        crate::dialogue_publication::work_item_erasure::erase(
            &mut tx, &scope, &public, job_id, now,
        )
        .await?;
        crate::dialogue_context::work_item_erasure::invalidate(&mut tx, &scope, &context, job_id)
            .await?;
        crate::resource::work_item_erasure::erase(&mut tx, &resources, job_id, now).await?;
        crate::artifact::erase_work_item_artifacts(&mut tx, &scope, &artifacts, job_id, now)
            .await?;
        crate::memory::erase_work_item_memories(&mut tx, &scope, &memories, job_id, now).await?;
        crate::loop_journal::request_work_item_completion_intent_erasure(
            &mut tx, &scope, job_id, now,
        )
        .await?;
        super::private_payload::erase(&mut tx, &scope, job_id).await?;
        super::source_commands::erase(&mut tx, &source_commands, job_id).await?;
        // Fence stale Context material and every other Content reader in the
        // same transaction as the exact provider tombstones. Physical removal
        // is still performed and verified by the original Content executor.
        crate::content::tombstone_in_transaction(&mut tx, job.request.tenant_id, &frozen.contents)
            .await
            .map_err(Error::Content)?;
        sqlx::query("DELETE FROM agent.dialogue_content_references WHERE tenant_id=$1 AND owner_id=$2 AND ((reference_kind='work_item_result' AND reference_id IN (SELECT result_id FROM collaboration.work_item_result_submissions WHERE tenant_id=$1 AND owner_id=$2 AND work_item_id=$4)) OR (reference_kind='work_item_report' AND reference_id=ANY($3)) OR (reference_kind='work_item_revision' AND content_id IN (SELECT goal_content_id FROM collaboration.work_item_revisions WHERE work_item_id=$4)))")
            .bind(scope.tenant()).bind(lineage.actor.principal_id.into_uuid()).bind(scope.runs())
            .bind(lineage.work_item_id.into_uuid()).execute(&mut *tx).await?;
        let reloaded =
            super::super::loading::load_jobs(&mut tx, job.request.tenant_id, &[job_id.into_uuid()])
                .await?
                .pop()
                .ok_or(Error::JobNotFound)?;
        tx.commit().await?;
        Ok(reloaded)
    }
}
