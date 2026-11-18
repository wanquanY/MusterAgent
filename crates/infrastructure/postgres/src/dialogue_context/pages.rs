//! Read a bounded consecutive range under one original source authorization.
//! Page identities, wire bytes and chain boundaries remain the same as single
//! reads; this only removes repeated SQL against the same governed snapshot.
use super::*;

pub(crate) async fn load_page(
    tx: &mut PgConnection,
    actor: Actor,
    cut: &DialogueContextCut,
    from_seq: u64,
) -> Result<DialogueContextSourcePage, Error> {
    load_pages(tx, actor, cut, from_seq, 1)
        .await?
        .pop()
        .ok_or(Error::Corrupt)
}

pub(crate) fn load_pages<'a>(
    tx: &'a mut PgConnection,
    actor: Actor,
    cut: &'a DialogueContextCut,
    from_seq: u64,
    page_count: usize,
) -> musteragent_runtime_contract::ApplicationFuture<
    'a,
    Result<Vec<DialogueContextSourcePage>, Error>,
> {
    // Keep the range-reader state on the heap: embedding its SQL/row decoder
    // future in every single-page caller needlessly grows nested Agent frames.
    Box::pin(async move {
        cut.validate().map_err(|_| Error::Corrupt)?;
        if actor.tenant_id != cut.tenant_id || actor.principal_id != cut.owner_id {
            return Err(Error::Unavailable);
        }
        if from_seq == 0
            || from_seq > cut.through.seq
            || page_count == 0
            || page_count > musteragent_context::DIALOGUE_SOURCE_BATCH_PAGES
        {
            return Err(Error::Corrupt);
        }
        let actual = load_cut(tx, actor, cut.agent_id, cut.dialogue_id, cut.through).await?;
        if actual != *cut {
            return Err(Error::RevisionConflict);
        }
        let through_seq = cut
            .through
            .seq
            .min(from_seq.saturating_add((DIALOGUE_CONTEXT_PAGE_ENTRIES * page_count) as u64 - 1));
        let mut chain = chain_at(tx, cut, from_seq - 1).await?;
        let rows = sqlx::query("SELECT e.*,s.source_visible_revision,s.payload_sha256,s.previous_sha256,s.chain_sha256 FROM agent.dialogue_context_source_entries s JOIN agent.agent_dialogue_entries e ON e.entry_id=s.entry_id AND e.tenant_id=s.tenant_id AND e.owner_id=s.owner_id AND e.dialogue_id=s.dialogue_id AND e.epoch=s.epoch AND e.seq=s.seq WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4 AND s.epoch=$5 AND s.seq BETWEEN $6 AND $7 ORDER BY s.seq")
        .bind(cut.tenant_id.into_uuid()).bind(cut.owner_id.into_uuid()).bind(cut.agent_id.into_uuid()).bind(cut.dialogue_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(from_seq)?).bind(integer(through_seq)?)
        .fetch_all(&mut *tx).await.map_err(db)?;
        let mut rows = rows.into_iter();
        let mut pages = Vec::with_capacity(page_count);
        for page_from in (from_seq..=through_seq).step_by(DIALOGUE_CONTEXT_PAGE_ENTRIES) {
            let prefix = chain;
            let mut entries = Vec::with_capacity(DIALOGUE_CONTEXT_PAGE_ENTRIES);
            for row in rows.by_ref().take(DIALOGUE_CONTEXT_PAGE_ENTRIES) {
                // A row decode failure is corruption, not a transient outage.
                let mut entry =
                    crate::agent_dialogue::entry_row(&row).map_err(|_| Error::Corrupt)?;
                entry.visible_revision = nz(unsigned(
                    row.try_get("source_visible_revision").map_err(db)?,
                )?)?;
                let payload = dialogue_context_entry_hash(&entry, content_bytes);
                let hash_column = |key: &str| -> Result<Sha256, Error> {
                    Sha256::from_bytes(row.try_get::<Vec<u8>, _>(key).map_err(db)?)
                        .map_err(|_| Error::Corrupt)
                };
                if hash_column("payload_sha256")? != payload
                    || hash_column("previous_sha256")? != chain
                {
                    return Err(Error::Corrupt);
                }
                let mut bytes = chain.into_bytes().to_vec();
                bytes.extend(payload.into_bytes());
                chain = Sha256::digest(bytes);
                if hash_column("chain_sha256")? != chain {
                    return Err(Error::Corrupt);
                }
                entries.push(entry);
            }
            let page = DialogueContextSourcePage {
                frozen_cut: cut.clone(),
                from_seq: page_from,
                through_seq: through_seq
                    .min(page_from.saturating_add(DIALOGUE_CONTEXT_PAGE_ENTRIES as u64 - 1)),
                entries,
                checkpoint: None,
                prefix_chain_sha256: prefix,
                through_chain_sha256: chain,
            };
            page.validate().map_err(|_| Error::Corrupt)?;
            pages.push(page);
        }
        Ok(pages)
    })
}
