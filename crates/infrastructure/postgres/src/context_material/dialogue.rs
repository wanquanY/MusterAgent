use super::*;
use musteragent_context::DialogueContextWindow;
use musteragent_execution_context::DialogueContextSourcePage;
use musteragent_identity::{Actor, PrincipalKind};

impl PostgresContextMaterialSource {
    pub(super) async fn dialogue_window_inner(
        &self,
        input: &ContextPlanInput,
    ) -> Result<Option<DialogueContextWindow>, PostgresContextMaterialError> {
        let (_, window) = self.load_run_source(input).await?;
        if let Some(window) = &window {
            self.require_dialogue_window_current(input, window).await?;
        }
        Ok(window)
    }

    pub(super) async fn require_dialogue_window_current(
        &self,
        input: &ContextPlanInput,
        window: &DialogueContextWindow,
    ) -> Result<(), PostgresContextMaterialError> {
        // The first governed snapshot may finish after a concurrent archive,
        // revocation or erasure. Recheck authority in a fresh snapshot without
        // reloading checkpoints, bindings and page bodies. Commit and dispatch
        // independently perform their original currentness checks as well.
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let current = crate::dialogue_context::run::run_cut(&mut tx, input.attempt.run_id, false)
            .await
            .map_err(|error| {
                PostgresContextMaterialError::dialogue_authority("dialogue_run_cut", error)
            })?;
        if current.as_ref() != Some(&window.frozen_cut) {
            return Err(PostgresContextMaterialError::CorruptStateAt(
                "dialogue_window_cut",
            ));
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn dialogue_window_for_source(
        &self,
        source: &run_source::RunSource,
        iteration: u32,
    ) -> Result<Option<DialogueContextWindow>, PostgresContextMaterialError> {
        let run = source.run_context.run_id;
        let policy = &source.snapshot.context_policy.compaction_policy_ref;
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let Some(cut) = crate::dialogue_context::run::run_cut(&mut tx, run, false)
            .await
            .map_err(|error| {
                PostgresContextMaterialError::dialogue_authority("dialogue_run_cut", error)
            })?
        else {
            return Ok(None);
        };
        let row=sqlx::query("SELECT through_seq,page_ordinal,frozen_through_seq,epoch,source_chain_sha256 FROM runtime.dialogue_context_bindings WHERE run_id=$1 ORDER BY page_ordinal DESC LIMIT 1")
            .bind(run.into_uuid()).fetch_optional(&mut *tx).await?;
        let bound_through = if let Some(row) = &row {
            if row.try_get::<i64, _>("frozen_through_seq")? != i64::try_from(cut.through.seq)?
                || row.try_get::<i64, _>("epoch")? != i64::try_from(cut.through.epoch)?
                || row.try_get::<Vec<u8>, _>("source_chain_sha256")?
                    != cut.source_chain_sha256.into_bytes()
            {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            u64::try_from(row.try_get::<i64, _>("through_seq")?)?
        } else {
            0
        };
        let checkpoint = if bound_through > 0 {
            crate::dialogue_context::checkpoint::select(
                &mut tx,
                &cut,
                Some(run),
                bound_through,
                policy,
            )
            .await
            .map_err(|error| {
                PostgresContextMaterialError::dialogue_source("dialogue_checkpoint_select", error)
            })?
        } else {
            None
        };
        let checkpoint = if checkpoint.is_none() && bound_through > 0 {
            let id:Option<uuid::Uuid>=sqlx::query_scalar("SELECT checkpoint_id FROM runtime.dialogue_context_bindings WHERE run_id=$1 AND checkpoint_id IS NOT NULL ORDER BY page_ordinal LIMIT 1").bind(run.into_uuid()).fetch_optional(&mut *tx).await?.flatten();
            if let Some(id) = id {
                Some(
                    crate::dialogue_context::checkpoint::by_id(
                        &mut tx,
                        &cut,
                        musteragent_identity::DialogueContextCheckpointId::from_uuid(id)
                            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
                    )
                    .await
                    .map_err(|error| {
                        PostgresContextMaterialError::dialogue_source(
                            "dialogue_checkpoint_by_id",
                            error,
                        )
                    })?,
                )
            } else {
                None
            }
        } else {
            checkpoint
        };
        if checkpoint.as_ref().is_some_and(|c| &c.policy_ref != policy) {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        let next_page = if bound_through < cut.through.seq
            && (checkpoint.as_ref().map_or(0, |c| c.coverage.through.seq) == bound_through
                || musteragent_runtime_snapshot::uses_capacity_based_dialogue_history(
                    policy.as_str(),
                )) {
            let initial = if bound_through == 0 {
                crate::dialogue_context::checkpoint::select(
                    &mut tx,
                    &cut,
                    None,
                    cut.through.seq,
                    policy,
                )
                .await
                .map_err(|error| {
                    PostgresContextMaterialError::dialogue_source(
                        "dialogue_initial_checkpoint",
                        error,
                    )
                })?
            } else {
                None
            };
            if let Some(checkpoint) = initial {
                Some(DialogueContextSourcePage {
                    frozen_cut: cut.clone(),
                    from_seq: 1,
                    through_seq: checkpoint.coverage.through.seq,
                    entries: vec![],
                    prefix_chain_sha256: cut.empty_chain_hash(),
                    through_chain_sha256: checkpoint.coverage.source_chain_sha256,
                    checkpoint: Some(checkpoint),
                })
            } else {
                Some(
                    crate::dialogue_context::load_page(
                        &mut tx,
                        Actor {
                            tenant_id: cut.tenant_id,
                            principal_id: cut.owner_id,
                            principal_kind: PrincipalKind::User,
                        },
                        &cut,
                        bound_through + 1,
                    )
                    .await
                    .map_err(|error| {
                        PostgresContextMaterialError::dialogue_source("dialogue_page", error)
                    })?,
                )
            }
        } else {
            None
        };
        let last_ordinal = row
            .as_ref()
            .map(|r| r.try_get::<i32, _>("page_ordinal"))
            .transpose()?
            .unwrap_or(0);
        let next_page_ordinal = std::num::NonZeroU32::new(
            u32::try_from(last_ordinal)?
                .checked_add(1)
                .ok_or(PostgresContextMaterialError::CorruptState)?,
        )
        .ok_or(PostgresContextMaterialError::CorruptState)?;
        let bound_source_chain_sha256 =
            crate::dialogue_context::chain_at(&mut tx, &cut, bound_through)
                .await
                .map_err(|error| {
                    PostgresContextMaterialError::dialogue_source("dialogue_bound_chain", error)
                })?;
        let mut window = DialogueContextWindow {
            frozen_cut: cut,
            bound_through_seq: bound_through,
            bound_source_chain_sha256,
            next_page_ordinal,
            checkpoint,
            next_page,
            started_coverage: None,
        };
        if window.has_uncompacted_prefix() {
            let pending = sqlx::query(r#"
                SELECT s.source_key, b.from_seq, b.through_seq
                FROM runtime.context_compaction_slots s
                JOIN runtime.dialogue_context_bindings b ON b.run_id=s.run_id
                  AND b.ledger_record_id=s.summarized_record_ids[cardinality(s.summarized_record_ids)]
                WHERE s.tenant_id=$1 AND s.run_id=$2 AND s.loop_iteration=$3
                  AND s.source_key LIKE 'dialogue_history:%' AND s.generation=1
                  AND NOT EXISTS (SELECT 1 FROM agent.dialogue_context_checkpoints c
                    JOIN runtime.context_compaction_slots done ON done.invocation_id=c.generator_invocation_id
                    WHERE done.run_id=s.run_id AND done.source_key=s.source_key)
                ORDER BY b.through_seq LIMIT 1"#)
                .bind(window.frozen_cut.tenant_id.into_uuid()).bind(run.into_uuid()).bind(i32::try_from(iteration)?)
                .fetch_optional(&mut *tx).await?;
            if let Some(slot) = pending {
                let key: String = slot.try_get("source_key")?;
                let rows=sqlx::query("SELECT seq,chain_sha256 FROM agent.dialogue_context_source_entries WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND dialogue_id=$4 AND epoch=$5 AND seq BETWEEN $6 AND $7 ORDER BY seq")
                    .bind(window.frozen_cut.tenant_id.into_uuid()).bind(window.frozen_cut.owner_id.into_uuid()).bind(window.frozen_cut.agent_id.into_uuid()).bind(window.frozen_cut.dialogue_id.into_uuid())
                    .bind(i64::try_from(window.frozen_cut.through.epoch)?).bind(slot.try_get::<i64,_>("from_seq")?).bind(slot.try_get::<i64,_>("through_seq")?).fetch_all(&mut *tx).await?;
                for row in rows {
                    let seq = u64::try_from(row.try_get::<i64, _>("seq")?)?;
                    let chain = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("chain_sha256")?)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                    if let Some(coverage) = window.match_started_cut(&key, seq, chain) {
                        window.started_coverage = Some(coverage);
                        break;
                    }
                }
                if window.started_coverage.is_none() {
                    return Err(PostgresContextMaterialError::CorruptStateAt(
                        "dialogue_compaction_coverage",
                    ));
                }
            }
        }
        window
            .validate()
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        tx.commit().await?;
        Ok(Some(window))
    }
}
