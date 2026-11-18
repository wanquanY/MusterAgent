//! The original Context commit lane owns private source binding. It shares the
//! live Attempt fence and Run sequence; it does not introduce an execution queue.
use super::*;
use musteragent_context::{ContextSourceFence, DialogueContextBindCommit};
use musteragent_execution_context::DialogueContextBound;
use musteragent_identity::Actor;
use musteragent_persistence_codec::{dialogue_context_codec, encode_content_ref};
use prost::Message;

fn source_error(_: musteragent_runtime_session::DialogueStoreError) -> PostgresContextPlanError {
    PostgresContextPlanError::SourceChanged
}

impl PostgresContextPlanStore {
    pub(super) async fn bind_dialogue_inner(
        &self,
        commit: DialogueContextBindCommit,
    ) -> Result<ContextPlanDisposition, PostgresContextPlanError> {
        self.bind_dialogue_batch_inner(vec![commit]).await
    }

    pub(super) async fn bind_dialogue_batch_inner(
        &self,
        commits: Vec<DialogueContextBindCommit>,
    ) -> Result<ContextPlanDisposition, PostgresContextPlanError> {
        if commits.is_empty() || commits.len() > musteragent_context::DIALOGUE_SOURCE_BATCH_PAGES {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        let first = &commits[0];
        for (index, commit) in commits.iter().enumerate() {
            commit
                .binding
                .validate()
                .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
            if commit.attempt != first.attempt
                || commit.binding.frozen_cut != first.binding.frozen_cut
            {
                return Err(PostgresContextPlanError::IdentityMismatch);
            }
            if index > 0 {
                let previous = &commits[index - 1].binding;
                if previous.completes_cut
                    || commit.binding.checkpoint_id.is_some()
                    || previous.page_ordinal.get().checked_add(1)
                        != Some(commit.binding.page_ordinal.get())
                    || previous.through_seq.checked_add(1) != Some(commit.binding.from_seq)
                {
                    return Err(PostgresContextPlanError::IdentityMismatch);
                }
            }
        }
        let mut tx =
            crate::commit_lane::acquire_for_run(self.ledger.database(), first.attempt.run_id)
                .await?
                .ok_or(PostgresContextPlanError::FenceConflict)?;
        let state = self.load_run_state_on(&mut tx, &first.attempt).await?;
        let fence = AttemptFence {
            run_id: first.attempt.run_id,
            attempt_id: first.attempt.attempt_id,
            attempt_number: first.attempt.attempt_number,
            worker_id: first.attempt.worker_id.clone(),
            lease_id: first.attempt.lease_id,
            fencing_token: first.attempt.fencing_token,
            home_epoch: state.home_epoch,
        };
        crate::execution::lock_and_validate_fence(
            &mut tx,
            &fence,
            state.last_run_sequence,
            state.now,
        )
        .await?;
        require_live_bindings(&mut tx, first.attempt.run_id).await?;
        let cut = crate::dialogue_context::run::run_cut(&mut tx, first.attempt.run_id, true)
            .await
            .map_err(source_error)?
            .ok_or(PostgresContextPlanError::SourceChanged)?;
        if cut != first.binding.frozen_cut {
            return Err(PostgresContextPlanError::SourceChanged);
        }
        let actor = Actor {
            tenant_id: cut.tenant_id,
            principal_id: cut.owner_id,
            principal_kind: musteragent_identity::PrincipalKind::User,
        };
        let mut validated = Vec::with_capacity(commits.len());
        let mut duplicates = 0;
        let raw: Vec<_> = commits
            .iter()
            .filter(|c| c.binding.checkpoint_id.is_none())
            .collect();
        let mut raw_pages = match raw.first() {
            Some(first) => crate::dialogue_context::load_pages(
                &mut tx,
                actor,
                &cut,
                first.binding.from_seq,
                raw.len(),
            )
            .await
            .map_err(source_error)?,
            None => Vec::new(),
        }
        .into_iter();
        for commit in &commits {
            let binding = &commit.binding;
            let page = if let Some(id) = binding.checkpoint_id {
                if binding.page_ordinal.get() != 1 || binding.from_seq != 1 {
                    return Err(PostgresContextPlanError::IdentityMismatch);
                }
                let checkpoint = crate::dialogue_context::checkpoint::by_id(&mut tx, &cut, id)
                    .await
                    .map_err(source_error)?;
                musteragent_execution_context::DialogueContextSourcePage {
                    frozen_cut: cut.clone(),
                    from_seq: 1,
                    through_seq: checkpoint.coverage.through.seq,
                    entries: vec![],
                    prefix_chain_sha256: cut.empty_chain_hash(),
                    through_chain_sha256: checkpoint.coverage.source_chain_sha256,
                    checkpoint: Some(checkpoint),
                }
            } else {
                let page = raw_pages
                    .next()
                    .ok_or(PostgresContextPlanError::IdentityMismatch)?;
                if page.from_seq != binding.from_seq {
                    return Err(PostgresContextPlanError::IdentityMismatch);
                }
                page
            };
            let bytes = dialogue_context_codec::page_bytes(&page)
                .map_err(|_| PostgresContextPlanError::CorruptState)?;
            if page.through_seq != binding.through_seq
                || binding.source_page_ref.sha256 != Sha256::digest(&bytes)
                || binding.source_page_ref.byte_length != bytes.len() as u64
                || binding.source_page_ref.media_type
                    != musteragent_context::ContextPlanContentKind::DialogueSourcePage.media_type()
            {
                return Err(PostgresContextPlanError::IdentityMismatch);
            }
            let existing = sqlx::query("SELECT source_page_ref,from_seq,through_seq,completes_cut FROM runtime.dialogue_context_bindings WHERE run_id=$1 AND page_ordinal=$2")
                .bind(first.attempt.run_id.into_uuid()).bind(i32::try_from(binding.page_ordinal.get())?).fetch_optional(&mut *tx).await?;
            if let Some(row) = existing {
                if row.try_get::<Vec<u8>, _>("source_page_ref")?
                    != encode_content_ref(&binding.source_page_ref).encode_to_vec()
                    || row.try_get::<i64, _>("from_seq")? != i64::try_from(binding.from_seq)?
                    || row.try_get::<i64, _>("through_seq")? != i64::try_from(binding.through_seq)?
                    || row.try_get::<bool, _>("completes_cut")? != binding.completes_cut
                {
                    return Err(PostgresContextPlanError::IdempotencyConflict);
                }
                duplicates += 1;
            }
            validated.push(dialogue_context_codec::page_wire_version(&page));
        }
        if duplicates == commits.len() {
            return Ok(ContextPlanDisposition::Duplicate);
        }
        // This adapter commits the whole batch atomically. A mixed old/new
        // submission is not a replay of that transaction; reload its window.
        if duplicates != 0 {
            return Err(PostgresContextPlanError::IdempotencyConflict);
        }
        let previous = sqlx::query("SELECT page_ordinal,through_seq FROM runtime.dialogue_context_bindings WHERE run_id=$1 ORDER BY page_ordinal DESC LIMIT 1")
            .bind(first.attempt.run_id.into_uuid()).fetch_optional(&mut *tx).await?;
        let (ordinal, through) = if let Some(row) = previous {
            (
                u32::try_from(row.try_get::<i32, _>("page_ordinal")?)?,
                u64::try_from(row.try_get::<i64, _>("through_seq")?)?,
            )
        } else {
            (0, 0)
        };
        if ordinal.checked_add(1) != Some(first.binding.page_ordinal.get())
            || through.checked_add(1) != Some(first.binding.from_seq)
        {
            return Err(PostgresContextPlanError::IdentityMismatch);
        }
        source_fence::authorize_source(
            &mut tx,
            state.tenant_id,
            state.conversation_id,
            &ContextSourceFence {
                content_refs: commits
                    .iter()
                    .map(|c| c.binding.source_page_ref.clone())
                    .collect(),
                active_branch_id: None,
                ledger_cutoff: musteragent_identity::ConversationSeq::new(
                    state.ledger_offset.get(),
                )
                .map_err(|_| PostgresContextPlanError::CorruptState)?,
            },
        )
        .await?;
        let mut records = Vec::with_capacity(commits.len());
        for (index, commit) in commits.iter().enumerate() {
            let record_id = LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                &first.attempt.run_id.into_uuid(),
                format!("dialogue-context-page:{}", commit.binding.page_ordinal).as_bytes(),
            ))
            .map_err(|_| PostgresContextPlanError::IdentityMismatch)?;
            let encoded = self
                .encoder
                .encode(LedgerPayload::DomainEvent(DomainEvent::Context(
                    ContextEvent::DialogueBound {
                        binding: Box::new(commit.binding.clone()),
                    },
                )));
            let next = RunSeq::new(
                state
                    .last_run_sequence
                    .get()
                    .checked_add(u64::try_from(index)? + 1)
                    .ok_or(PostgresContextPlanError::NumericRange)?,
            )
            .map_err(|_| PostgresContextPlanError::NumericRange)?;
            records.push(ProposedRecord {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(state.activity_id),
                run_id: Some(first.attempt.run_id),
                run_seq: Some(next),
                attempt_id: Some(first.attempt.attempt_id),
                participant_id: None,
                producer: Producer::Worker,
                occurred_at: state.now,
                causation_id: None,
                correlation_id: CorrelationId::from_uuid(record_id.into_uuid())
                    .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
                payload_hash: Sha256::digest(&encoded),
                payload: encoded,
            });
        }
        let mut fingerprint = Vec::new();
        for record in &records {
            if records.len() > 1 {
                fingerprint.extend_from_slice(&(record.payload.len() as u64).to_be_bytes());
            }
            fingerprint.extend_from_slice(&record.payload);
        }
        let last = records
            .last()
            .ok_or(PostgresContextPlanError::CorruptState)?;
        let next = last.run_seq.ok_or(PostgresContextPlanError::CorruptState)?;
        let key = if records.len() == 1 {
            format!("dialogue-source:{}", records[0].record_id)
        } else {
            format!(
                "dialogue-source-batch:{}:{}",
                records[0].record_id, last.record_id
            )
        };
        let append = AppendRequest {
            tenant_id: state.tenant_id,
            conversation_id: state.conversation_id,
            expected_home_epoch: state.home_epoch,
            expected_offset: state.ledger_offset,
            command_fingerprint: Sha256::digest(&fingerprint),
            idempotency_key: IdempotencyKey::new(key)
                .map_err(|_| PostgresContextPlanError::IdentityMismatch)?,
            records,
            outbox: vec![],
        };
        let prepared = match self.ledger.begin_append_in_transaction(tx, &append).await? {
            BeginAppend::Ready(value) => value,
            BeginAppend::Duplicate(_) => return Err(PostgresContextPlanError::CorruptState),
        };
        let mut staged = prepared.stage(&append).await?;
        for ((commit, wire_version), record) in commits.iter().zip(validated).zip(&append.records) {
            insert_binding(
                staged.transaction_mut(),
                first.attempt.run_id,
                &commit.binding,
                record.record_id,
                wire_version,
            )
            .await?;
        }
        crate::execution::lock_and_validate_fence(
            staged.transaction_mut(),
            &fence,
            state.last_run_sequence,
            state.now,
        )
        .await?;
        let changed = sqlx::query("UPDATE runtime.runs SET last_run_sequence=$2,run_version=run_version+1 WHERE run_id=$1 AND last_run_sequence=$3")
            .bind(first.attempt.run_id.into_uuid()).bind(i64::try_from(next.get())?).bind(i64::try_from(state.last_run_sequence.get())?)
            .execute(&mut *staged.transaction_mut()).await?;
        if changed.rows_affected() != 1 {
            return Err(PostgresContextPlanError::FenceConflict);
        }
        staged.commit().await?;
        Ok(ContextPlanDisposition::Applied)
    }
}
async fn insert_binding(
    tx: &mut sqlx::PgConnection,
    run: RunId,
    binding: &DialogueContextBound,
    record: LedgerRecordId,
    source_wire_version: i16,
) -> Result<(), PostgresContextPlanError> {
    let cut = &binding.frozen_cut;
    sqlx::query("INSERT INTO runtime.dialogue_context_bindings(run_id,page_ordinal,tenant_id,dialogue_id,epoch,frozen_through_seq,source_chain_sha256,from_seq,through_seq,ledger_record_id,source_page_content_id,source_page_ref,checkpoint_id,completes_cut,source_wire_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)")
        .bind(run.into_uuid()).bind(i32::try_from(binding.page_ordinal.get())?).bind(cut.tenant_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(i64::try_from(cut.through.epoch)?).bind(i64::try_from(cut.through.seq)?).bind(cut.source_chain_sha256.into_bytes().to_vec()).bind(i64::try_from(binding.from_seq)?).bind(i64::try_from(binding.through_seq)?).bind(record.into_uuid()).bind(binding.source_page_ref.content_id.into_uuid()).bind(encode_content_ref(&binding.source_page_ref).encode_to_vec()).bind(binding.checkpoint_id.map(|id|id.into_uuid())).bind(binding.completes_cut).bind(source_wire_version).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO runtime.dialogue_context_content_references(run_id,tenant_id,content_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(run.into_uuid()).bind(cut.tenant_id.into_uuid()).bind(binding.source_page_ref.content_id.into_uuid()).execute(tx).await?;
    Ok(())
}

/// Primary plans cannot cross the admission cut before all pages are bound.
/// Enforced at the owning transaction, independently of caller/planner behavior.
pub(crate) async fn require_complete_cut(
    tx: &mut sqlx::PgConnection,
    run: RunId,
    cut: &musteragent_execution_context::DialogueContextCut,
) -> Result<(), PostgresContextPlanError> {
    require_live_bindings(tx, run).await?;
    let row=sqlx::query("SELECT through_seq,frozen_through_seq,epoch,source_chain_sha256,completes_cut FROM runtime.dialogue_context_bindings WHERE run_id=$1 ORDER BY page_ordinal DESC LIMIT 1")
        .bind(run.into_uuid()).fetch_optional(tx).await?;
    if cut.through.seq == 0 && row.is_none() {
        return Ok(());
    }
    let row = row.ok_or(PostgresContextPlanError::SourceChanged)?;
    if row.try_get::<i64, _>("through_seq")? != i64::try_from(cut.through.seq)?
        || row.try_get::<i64, _>("frozen_through_seq")? != i64::try_from(cut.through.seq)?
        || row.try_get::<i64, _>("epoch")? != i64::try_from(cut.through.epoch)?
        || row.try_get::<Vec<u8>, _>("source_chain_sha256")? != cut.source_chain_sha256.into_bytes()
        || !row.try_get::<bool, _>("completes_cut")?
    {
        return Err(PostgresContextPlanError::SourceChanged);
    }
    Ok(())
}

/// Old source pages can remain as provenance, but never authorize another model
/// request or compaction after their exact source entry has been erased.
pub(crate) async fn require_live_bindings(
    tx: &mut sqlx::PgConnection,
    run: RunId,
) -> Result<(), PostgresContextPlanError> {
    let erased: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.dialogue_context_bindings b WHERE b.run_id=$1 AND (to_jsonb(b)->>'erasure_job_id') IS NOT NULL)")
        .bind(run.into_uuid()).fetch_one(&mut *tx).await?;
    if erased {
        return Err(PostgresContextPlanError::SourceChanged);
    }
    Ok(())
}
