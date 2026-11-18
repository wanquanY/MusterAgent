//! An original paid compaction may publish a shared Dialogue checkpoint only
//! when every summarized native record proves public-only, contiguous coverage.
use super::*;
use musteragent_context::{ContextPlanContentKind, DialogueContextCheckpointCommit};
use musteragent_persistence_codec::{dialogue_context_codec, encode_content_ref};
use prost::Message;

impl PostgresContextPlanStore {
    pub(super) async fn dialogue_checkpoint_inner(
        &self,
        commit: DialogueContextCheckpointCommit,
    ) -> Result<ContextPlanDisposition, PostgresContextPlanError> {
        commit
            .validate()
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let c = &commit.checkpoint;
        let r = &commit.resolution.resolution;
        let mut tx =
            crate::commit_lane::acquire_for_run(self.ledger.database(), commit.attempt.run_id)
                .await?
                .ok_or(PostgresContextPlanError::FenceConflict)?;
        let state = self.load_run_state_on(&mut tx, &commit.attempt).await?;
        let fence = AttemptFence {
            run_id: commit.attempt.run_id,
            attempt_id: commit.attempt.attempt_id,
            attempt_number: commit.attempt.attempt_number,
            worker_id: commit.attempt.worker_id.clone(),
            lease_id: commit.attempt.lease_id,
            fencing_token: commit.attempt.fencing_token,
            home_epoch: state.home_epoch,
        };
        crate::execution::lock_and_validate_fence(
            &mut tx,
            &fence,
            state.last_run_sequence,
            state.now,
        )
        .await?;
        let full = crate::dialogue_context::run::run_cut(&mut tx, commit.attempt.run_id, true)
            .await
            .map_err(|_| PostgresContextPlanError::SourceChanged)?
            .ok_or(PostgresContextPlanError::SourceChanged)?;
        if !full.same_dialogue(&c.coverage) || c.coverage.through.seq > full.through.seq {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        let actor = musteragent_identity::Actor {
            tenant_id: full.tenant_id,
            principal_id: full.owner_id,
            principal_kind: musteragent_identity::PrincipalKind::User,
        };
        let actual = crate::dialogue_context::load_cut(
            &mut tx,
            actor,
            full.agent_id,
            full.dialogue_id,
            c.coverage.through,
        )
        .await
        .map_err(|_| PostgresContextPlanError::SourceChanged)?;
        if actual != c.coverage {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        let manifest = dialogue_context_codec::cut(&c.coverage).encode_to_vec();
        if c.source_manifest_ref.sha256 != Sha256::digest(&manifest)
            || c.source_manifest_ref.byte_length != manifest.len() as u64
            || c.source_manifest_ref.media_type
                != ContextPlanContentKind::OwnerSourceManifest.media_type()
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        source_fence::authorize_source(
            &mut tx,
            state.tenant_id,
            state.conversation_id,
            &commit.source_fence,
        )
        .await?;
        let mut outputs = commit.source_fence.clone();
        outputs.content_refs = vec![c.summary_ref.clone(), c.source_manifest_ref.clone()];
        source_fence::authorize_source(&mut tx, state.tenant_id, state.conversation_id, &outputs)
            .await?;
        let proof=sqlx::query("SELECT s.source_key,s.summarized_record_ids,s.compaction_policy_ref,s.input_tokens_before,m.response_content_id,b.content_hash,b.byte_length::text AS byte_length FROM runtime.context_compaction_slots s JOIN runtime.model_invocations m ON m.invocation_id=s.invocation_id AND m.run_id=s.run_id AND m.tenant_id=s.tenant_id AND m.status=2 JOIN artifact.content_refs b ON b.content_id=m.response_content_id AND b.tenant_id=m.tenant_id AND b.state=2 WHERE s.invocation_id=$1 AND s.run_id=$2 AND s.tenant_id=$3 AND s.loop_iteration=$4 FOR SHARE OF s,m,b")
            .bind(c.generator_invocation_id.into_uuid()).bind(commit.attempt.run_id.into_uuid()).bind(full.tenant_id.into_uuid()).bind(i32::try_from(r.loop_iteration)?).fetch_optional(&mut *tx).await?.ok_or(PostgresContextPlanError::SourceChanged)?;
        let ids = commit
            .summarized_record_ids
            .iter()
            .map(|id| id.into_uuid())
            .collect::<Vec<_>>();
        let source = musteragent_context::CompactionSource::DialogueHistory {
            coverage: c.coverage.clone(),
            parent_checkpoint_id: c.parent_checkpoint_id,
        };
        if proof.try_get::<String, _>("source_key")? != source.key()
            || proof.try_get::<Vec<uuid::Uuid>, _>("summarized_record_ids")? != ids
            || proof.try_get::<String, _>("compaction_policy_ref")? != c.policy_ref.as_str()
            || proof.try_get::<i64, _>("input_tokens_before")?
                != i64::try_from(r.input_tokens_before)?
            || proof.try_get::<Vec<u8>, _>("content_hash")? != c.summary_ref.sha256.into_bytes()
            || proof
                .try_get::<String, _>("byte_length")?
                .parse::<u64>()
                .map_err(|_| PostgresContextPlanError::CorruptState)?
                != c.summary_ref.byte_length
            || c.summary_ref.media_type != ContextPlanContentKind::CompactionSummary.media_type()
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        verify_coverage(
            &mut tx,
            commit.attempt.run_id,
            &c.coverage,
            c.parent_checkpoint_id,
            &ids,
        )
        .await?;
        let existing =
            sqlx::query("SELECT * FROM agent.dialogue_context_checkpoints WHERE checkpoint_id=$1")
                .bind(c.checkpoint_id.into_uuid())
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(row) = existing {
            if crate::dialogue_context::checkpoint::decode(&row)
                .map_err(|_| PostgresContextPlanError::CorruptState)?
                != *c
            {
                return Err(PostgresContextPlanError::IdempotencyConflict);
            }
            return Ok(ContextPlanDisposition::Duplicate);
        }
        let record_id = derived_record_id(r.invocation_id, b"resolved")
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
        let payload = self
            .encoder
            .encode(LedgerPayload::DomainEvent(DomainEvent::Context(
                ContextEvent::CompactionResolved {
                    resolution: Box::new(r.clone()),
                },
            )));
        let next = RunSeq::new(
            state
                .last_run_sequence
                .get()
                .checked_add(1)
                .ok_or(PostgresContextPlanError::NumericRange)?,
        )
        .map_err(|_| PostgresContextPlanError::NumericRange)?;
        let append = AppendRequest {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            expected_home_epoch: state.home_epoch,
            expected_offset: state.ledger_offset,
            command_fingerprint: Sha256::digest(
                dialogue_context_codec::checkpoint(c).encode_to_vec(),
            ),
            idempotency_key: IdempotencyKey::new(format!(
                "dialogue-checkpoint:{}",
                c.checkpoint_id
            ))
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
            records: vec![ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(state.activity_id),
                run_id: Some(commit.attempt.run_id),
                run_seq: Some(next),
                attempt_id: Some(commit.attempt.attempt_id),
                participant_id: None,
                producer: Producer::Worker,
                occurred_at: state.now,
                causation_id: commit.summarized_record_ids.last().copied(),
                correlation_id: CorrelationId::from_uuid(r.invocation_id.into_uuid())
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                payload_hash: Sha256::digest(&payload),
                payload,
            }],
            outbox: vec![],
        };
        let prepared = match self.ledger.begin_append_in_transaction(tx, &append).await? {
            BeginAppend::Ready(v) => v,
            BeginAppend::Duplicate(_) => return Err(PostgresContextPlanError::CorruptState),
        };
        let mut staged = prepared.stage(&append).await?;
        let tx = staged.transaction_mut();
        sqlx::query("INSERT INTO agent.dialogue_context_checkpoints(checkpoint_id,tenant_id,owner_id,agent_id,dialogue_id,epoch,through_seq,source_chain_sha256,parent_checkpoint_id,summary_content_id,summary_ref,source_manifest_content_id,source_manifest_ref,generator_invocation_id,policy_ref,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)")
            .bind(c.checkpoint_id.into_uuid()).bind(full.tenant_id.into_uuid()).bind(full.owner_id.into_uuid()).bind(full.agent_id.into_uuid()).bind(full.dialogue_id.into_uuid()).bind(i64::try_from(full.through.epoch)?).bind(i64::try_from(c.coverage.through.seq)?).bind(c.coverage.source_chain_sha256.into_bytes().to_vec()).bind(c.parent_checkpoint_id.map(|id|id.into_uuid())).bind(c.summary_ref.content_id.into_uuid()).bind(encode_content_ref(&c.summary_ref).encode_to_vec()).bind(c.source_manifest_ref.content_id.into_uuid()).bind(encode_content_ref(&c.source_manifest_ref).encode_to_vec()).bind(c.generator_invocation_id.into_uuid()).bind(c.policy_ref.as_str()).bind(state.now.unix_millis()).execute(&mut *tx).await?;
        // Monotonic same-epoch CAS. A concurrent later checkpoint wins; this
        // immutable candidate remains available without moving the head back.
        sqlx::query("INSERT INTO agent.dialogue_context_heads(dialogue_id,epoch,through_seq,checkpoint_id,revision) VALUES($1,$2,$3,$4,1) ON CONFLICT(dialogue_id) DO UPDATE SET through_seq=EXCLUDED.through_seq,checkpoint_id=EXCLUDED.checkpoint_id,revision=agent.dialogue_context_heads.revision+1 WHERE agent.dialogue_context_heads.epoch=EXCLUDED.epoch AND agent.dialogue_context_heads.through_seq<EXCLUDED.through_seq")
            .bind(full.dialogue_id.into_uuid()).bind(i64::try_from(full.through.epoch)?).bind(i64::try_from(c.coverage.through.seq)?).bind(c.checkpoint_id.into_uuid()).execute(&mut *tx).await?;
        for reference in [&c.summary_ref, &c.source_manifest_ref] {
            sqlx::query("INSERT INTO runtime.dialogue_context_content_references(run_id,tenant_id,content_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(commit.attempt.run_id.into_uuid()).bind(full.tenant_id.into_uuid()).bind(reference.content_id.into_uuid()).execute(&mut *tx).await?;
        }
        crate::execution::lock_and_validate_fence(tx, &fence, state.last_run_sequence, state.now)
            .await?;
        let changed=sqlx::query("UPDATE runtime.runs SET last_run_sequence=$2,run_version=run_version+1 WHERE run_id=$1 AND last_run_sequence=$3").bind(commit.attempt.run_id.into_uuid()).bind(i64::try_from(next.get())?).bind(i64::try_from(state.last_run_sequence.get())?).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(PostgresContextPlanError::FenceConflict);
        }
        staged.commit().await?;
        Ok(ContextPlanDisposition::Applied)
    }
}

pub(crate) async fn verify_coverage(
    tx: &mut sqlx::PgConnection,
    run: RunId,
    coverage: &musteragent_execution_context::DialogueContextCut,
    parent_checkpoint_id: Option<musteragent_identity::DialogueContextCheckpointId>,
    records: &[uuid::Uuid],
) -> Result<(), PostgresContextPlanError> {
    super::dialogue::require_live_bindings(tx, run).await?;
    let mut through = 0u64;
    let mut expected = Vec::new();
    if let Some(parent_id) = parent_checkpoint_id {
        let parent = crate::dialogue_context::checkpoint::by_id(tx, coverage, parent_id)
            .await
            .map_err(|_| PostgresContextPlanError::SourceChanged)?;
        through = parent.coverage.through.seq;
        let imported:Option<uuid::Uuid>=sqlx::query_scalar("SELECT ledger_record_id FROM runtime.dialogue_context_bindings WHERE run_id=$1 AND checkpoint_id=$2 AND from_seq=1").bind(run.into_uuid()).bind(parent_id.into_uuid()).fetch_optional(&mut *tx).await?;
        if let Some(record) = imported {
            expected.push(record)
        } else {
            let native = derived_record_id(parent.generator_invocation_id, b"resolved")
                .map_err(|_| PostgresContextPlanError::CorruptState)?;
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM event_store.ledger_records WHERE record_id=$1 AND run_id=$2 AND tenant_id=$3)").bind(native.into_uuid()).bind(run.into_uuid()).bind(coverage.tenant_id.into_uuid()).fetch_one(&mut *tx).await?;
            if !exists {
                return Err(PostgresContextPlanError::SourceChanged);
            }
            expected.push(native.into_uuid());
        }
    }
    let rows=sqlx::query("SELECT from_seq,through_seq,ledger_record_id,checkpoint_id FROM runtime.dialogue_context_bindings WHERE run_id=$1 AND through_seq>$2 AND from_seq<=$3 ORDER BY from_seq")
        .bind(run.into_uuid()).bind(i64::try_from(through)?).bind(i64::try_from(coverage.through.seq)?).fetch_all(tx).await?;
    for row in rows {
        if row.try_get::<i64, _>("from_seq")? > i64::try_from(through + 1)?
            || row
                .try_get::<Option<uuid::Uuid>, _>("checkpoint_id")?
                .is_some()
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        through = u64::try_from(row.try_get::<i64, _>("through_seq")?)?.min(coverage.through.seq);
        expected.push(row.try_get("ledger_record_id")?);
    }
    if through != coverage.through.seq || expected != records {
        return Err(PostgresContextPlanError::IdentityMismatch);
    }
    Ok(())
}
