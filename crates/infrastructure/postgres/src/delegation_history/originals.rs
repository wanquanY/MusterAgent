use super::*;
use musteragent_runtime_content::{ContentBatch, ContentStore};

impl PostgresDelegationHistorySource {
    pub(super) async fn original_page(
        &self,
        access: &HistoryAccess,
        content: Arc<dyn ContentStore>,
        page: u64,
    ) -> Result<OriginalPage, HistoryError> {
        if access.actor.principal_kind != PrincipalKind::User
            || access.tool_fence.is_none()
            || page == 0
        {
            return Err(HistoryError::Denied);
        }
        if let Some(dialogue) = self
            .dialogue_originals(access, content.clone(), page)
            .await?
        {
            return Ok(dialogue);
        }
        let scope = self
            .read_inner(access, &HistoryInput::original_scope(), true)
            .await?;
        let through = page.saturating_add(127).min(scope.cursor.head);

        let ids:Vec<String>=sqlx::query_scalar("SELECT record_id::text FROM event_store.ledger_records WHERE tenant_id=$1 AND conversation_id=$2::text::uuid AND conversation_seq BETWEEN $3::text::numeric AND $4::text::numeric AND record_kind=1 ORDER BY conversation_seq").bind(access.actor.tenant_id.into_uuid()).bind(&scope.cursor.conversation).bind(page.to_string()).bind(through.to_string()).fetch_all(self.database.pool()).await.map_err(db)?;

        let mut items = Vec::new();
        for id in ids {
            let record = match self
                .read_inner(access, &HistoryInput::original_record(id), true)
                .await
            {
                Ok(record) => record,
                Err(HistoryError::Denied) => continue,
                Err(err) => return Err(err),
            };
            for entry in &record.entries {
                if let Some(mut item) =
                    original_record_text(entry, content.as_ref(), access).await?
                {
                    if let Some(id) = entry.result_invocation {
                        let key:String=sqlx::query_scalar("SELECT tool_ref FROM tool.tool_invocations WHERE tenant_id=$1 AND invocation_id=$2").bind(access.actor.tenant_id.into_uuid()).bind(id.into_uuid()).fetch_one(self.database.pool()).await.map_err(db)?;
                        musteragent_context::project_original_json(&key, &mut item.text);
                    }
                    items.push(item);
                }
            }
        }
        let current = self
            .read_inner(access, &HistoryInput::original_scope(), true)
            .await?;
        if current.cursor != scope.cursor {
            return Err(HistoryError::Denied);
        }
        Ok(OriginalPage {
            items,
            next: (through < scope.cursor.head).then(|| through + 1),
        })
    }
}

impl PostgresDelegationHistorySource {
    async fn dialogue_cut(
        &self,
        access: &HistoryAccess,
    ) -> Result<Option<musteragent_execution_context::DialogueContextCut>, HistoryError> {
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Context,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(|_| HistoryError::Unavailable)?;
        require_tool_fence(&mut tx, access, false, true).await?;
        let cut = crate::dialogue_context::run::run_cut(&mut tx, access.current_run_id, false)
            .await
            .map_err(|_| HistoryError::Denied)?;
        tx.commit().await.map_err(|_| HistoryError::Unavailable)?;
        if cut.as_ref().is_some_and(|cut| {
            cut.tenant_id != access.actor.tenant_id || cut.owner_id != access.actor.principal_id
        }) {
            return Err(HistoryError::Denied);
        }
        Ok(cut)
    }
    async fn dialogue_originals(
        &self,
        access: &HistoryAccess,
        content: Arc<dyn ContentStore>,
        page: u64,
    ) -> Result<Option<OriginalPage>, HistoryError> {
        let Some(cut) = self.dialogue_cut(access).await? else {
            return Ok(None);
        };
        if page > cut.through.seq {
            return Ok(Some(OriginalPage {
                items: vec![],
                next: None,
            }));
        }
        let page = crate::PostgresDialogueContextSourceStore::new(self.database.clone())
            .page(access.actor, &cut, page)
            .await
            .map_err(|_| HistoryError::Denied)?;
        let refs = musteragent_context::dialogue_history_text_refs(&page);
        let bodies = ContentBatch::load(content.as_ref(), cut.tenant_id, &refs)
            .await
            .map_err(|_| HistoryError::Unavailable)?;
        let source = crate::PostgresContextMaterialSource::new(
            self.database.clone(),
            content,
            Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
            self.decoder.clone(),
        );
        let original = source
            .dialogue_history(access.current_run_id, &page, &bodies)
            .await
            .map_err(|_| HistoryError::Unavailable)?;
        if self.dialogue_cut(access).await?.as_ref() != Some(&cut) {
            return Err(HistoryError::Denied);
        }
        Ok(Some(OriginalPage {
            items: original
                .original_text()
                .into_iter()
                .map(|(role, text)| OriginalText { role, text })
                .collect(),
            next: (page.through_seq < cut.through.seq).then(|| page.through_seq + 1),
        }))
    }
}
