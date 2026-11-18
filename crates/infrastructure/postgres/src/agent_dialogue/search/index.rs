use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use sqlx::postgres::PgRow;

struct Document {
    text: String,
    files: bool,
    images: bool,
    source: Option<ContentId>,
}
impl PostgresAgentDialogueStore {
    pub(super) async fn refresh_history_search(
        &self,
        content: &crate::PostgresContentStore,
        actor: Option<Actor>,
        limit: i64,
    ) -> Result<(), Error> {
        let rows = sqlx::query(
            "SELECT e.* FROM agent.dialogue_history_search_pending q
            JOIN agent.agent_dialogue_entries e USING(entry_id)
            WHERE q.next_attempt_at_ms <= (extract(epoch FROM clock_timestamp())*1000)::bigint
            AND ($1::uuid IS NULL OR e.tenant_id=$1) AND ($2::uuid IS NULL OR e.owner_id=$2)
            ORDER BY q.next_attempt_at_ms,q.entry_id LIMIT $3",
        )
        .bind(actor.map(|a| a.tenant_id.into_uuid()))
        .bind(actor.map(|a| a.principal_id.into_uuid()))
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        for row in rows {
            let entry = entry_row(&row)?;
            let result = self.project_search_document(content, &entry).await;
            match result {
                Ok(document) => self.commit_search_document(&entry, document).await?,
                Err(error) => {
                    // Retry a failed object independently so it cannot starve
                    // other accounts. Never mark a failed body as indexed.
                    sqlx::query("UPDATE agent.dialogue_history_search_pending SET next_attempt_at_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint+5000 WHERE entry_id=$1")
                        .bind(entry.entry_id.into_uuid()).execute(&self.pool).await.map_err(db)?;
                    tracing::warn!(error=%error,"public search source remains pending");
                }
            }
        }
        Ok(())
    }

    async fn project_search_document(
        &self,
        content: &crate::PostgresContentStore,
        entry: &AgentDialogueEntry,
    ) -> Result<Document, Error> {
        let mut document = Document {
            text: String::new(),
            files: false,
            images: false,
            source: None,
        };
        match &entry.content {
            AgentDialogueContent::Erased { .. } => {}
            AgentDialogueContent::UserMessage {
                content_ref,
                input_attachments,
                ..
            } => {
                document.source = Some(content_ref.content_id);
                document.text = public_text(content, entry.tenant_id, content_ref).await?;
                for attachment in input_attachments {
                    document.text.push('\n');
                    document.text.push_str(attachment.display_name.as_str());
                    document.files = true;
                    document.images |= attachment
                        .content_ref
                        .media_type
                        .as_str()
                        .starts_with("image/");
                }
            }
            AgentDialogueContent::AgentMessage { content_ref } => {
                document.source = Some(content_ref.content_id);
                document.text = public_text(content, entry.tenant_id, content_ref).await?;
            }
            AgentDialogueContent::PartnerCreated(partner) => {
                document.text = format!("{}\n{}", partner.display_name, partner.description)
            }
            AgentDialogueContent::Artifact {
                artifact_id,
                artifact_version_id,
            } => {
                let row=sqlx::query("SELECT a.title,c.media_type FROM artifact.artifacts a
                    JOIN artifact.artifact_versions v ON v.artifact_id=a.artifact_id AND v.artifact_version_id=$2
                    JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=a.tenant_id
                    JOIN agent.dialogue_publications p ON p.entry_id=$3 AND p.tenant_id=a.tenant_id AND p.owner_id=$4
                    WHERE a.artifact_id=$1 AND a.tenant_id=$5 AND resource.agent_artifact_owned(a.tenant_id,p.agent_id,a.artifact_id)")
                    .bind(artifact_id.into_uuid()).bind(artifact_version_id.into_uuid()).bind(entry.entry_id.into_uuid())
                    .bind(entry.owner_id.into_uuid()).bind(entry.tenant_id.into_uuid()).fetch_optional(&self.pool).await.map_err(db)?.ok_or(Error::Unavailable)?;
                document.text = row.try_get("title").map_err(db)?;
                document.files = true;
                document.images = row
                    .try_get::<String, _>("media_type")
                    .map_err(db)?
                    .starts_with("image/");
            }
            AgentDialogueContent::Interaction { interaction_id } => {
                // Only a committed public prompt. Responses, credentials,
                // OAuth/secure refs, tool arguments and runtime logs are absent.
                let native = {
                    let mut conn = self.pool.acquire().await.map_err(db)?;
                    crate::PostgresNativeDialogueInteractionStore::published_prompt(
                        &mut conn,
                        Actor {
                            tenant_id: entry.tenant_id,
                            principal_id: entry.owner_id,
                            principal_kind: PrincipalKind::User,
                        },
                        entry.dialogue_id,
                        entry.entry_id,
                    )
                    .await
                    .map_err(|error| match error.code {
                        musteragent_runtime_contract::ErrorCode::ContentIntegrityFailed => {
                            Error::Corrupt
                        }
                        _ => Error::Infrastructure,
                    })?
                };
                let (reference, secure) = if let Some((kind, reference)) = native {
                    use musteragent_approval::InteractionKind;
                    (
                        reference,
                        matches!(kind, InteractionKind::Credential | InteractionKind::OAuth),
                    )
                } else {
                    let row=sqlx::query("SELECT i.kind,c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
                    FROM approval.interactions i
                    JOIN agent.dialogue_publications p ON p.entry_id=$2 AND p.tenant_id=i.tenant_id AND p.owner_id=$3 AND p.source_run_id=i.run_id AND p.execution_home='cloud' AND p.cloud_interaction_id=i.interaction_id
                    JOIN artifact.content_refs c ON c.content_id=i.prompt_content_id AND c.tenant_id=i.tenant_id AND c.state=2
                    WHERE i.interaction_id=$1 AND i.tenant_id=$4")
                    .bind(interaction_id.into_uuid()).bind(entry.entry_id.into_uuid()).bind(entry.owner_id.into_uuid()).bind(entry.tenant_id.into_uuid())
                    .fetch_optional(&self.pool).await.map_err(db)?.ok_or(Error::Unavailable)?;
                    (
                        reference(&row)?,
                        matches!(row.try_get::<i16, _>("kind").map_err(db)?, 5 | 6),
                    )
                };
                document.source = Some(reference.content_id);
                let prompt = public_text(content, entry.tenant_id, &reference).await?;
                document.text = prompt_labels(&prompt, reference.media_type.as_str(), secure)?;
            }
        }
        if document.text.len() > 2 * 1024 * 1024 {
            return Err(Error::Corrupt);
        }
        // UTF-8 public messages may contain NUL; PostgreSQL text cannot. Only the
        // rebuildable search projection normalizes it, never the source body.
        document.text = document.text.replace('\0', " ");
        Ok(document)
    }
    async fn commit_search_document(
        &self,
        entry: &AgentDialogueEntry,
        document: Document,
    ) -> Result<(), Error> {
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(transaction_error)?;
        let revision:Option<i64>=sqlx::query_scalar("SELECT visible_revision FROM agent.agent_dialogue_entries WHERE entry_id=$1 AND tenant_id=$2 AND owner_id=$3 FOR SHARE")
            .bind(entry.entry_id.into_uuid()).bind(entry.tenant_id.into_uuid()).bind(entry.owner_id.into_uuid())
            .fetch_optional(&mut *tx).await.map_err(db)?;
        if revision != Some(integer(entry.visible_revision.get())?) {
            return Err(Error::RevisionConflict);
        }
        if matches!(entry.content, AgentDialogueContent::Erased { .. }) {
            sqlx::query("DELETE FROM agent.dialogue_history_search_documents WHERE entry_id=$1")
                .bind(entry.entry_id.into_uuid())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("DELETE FROM agent.dialogue_history_search_pending WHERE entry_id=$1")
                .bind(entry.entry_id.into_uuid())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            tx.commit().await.map_err(transaction_error)?;
            return Ok(());
        }
        let links = document.text.to_ascii_lowercase().contains("https://")
            || document.text.to_ascii_lowercase().contains("http://");
        sqlx::query("INSERT INTO agent.dialogue_history_search_documents(entry_id,visible_revision,search_text,has_files,has_images,has_links,source_content_id)
            VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(entry_id) DO UPDATE SET visible_revision=EXCLUDED.visible_revision,search_text=EXCLUDED.search_text,
            has_files=EXCLUDED.has_files,has_images=EXCLUDED.has_images,has_links=EXCLUDED.has_links,source_content_id=EXCLUDED.source_content_id")
            .bind(entry.entry_id.into_uuid()).bind(integer(entry.visible_revision.get())?).bind(document.text)
            .bind(document.files).bind(document.images).bind(links).bind(document.source.map(ContentId::into_uuid)).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("DELETE FROM agent.dialogue_history_search_pending WHERE entry_id=$1")
            .bind(entry.entry_id.into_uuid())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(transaction_error)
    }
}
async fn public_text(
    content: &crate::PostgresContentStore,
    tenant: TenantId,
    reference: &ContentRef,
) -> Result<String, Error> {
    if reference.byte_length > MAX_DIALOGUE_MESSAGE_TEXT_BYTES {
        return Err(Error::Corrupt);
    }
    let bytes = content
        .read(tenant, reference)
        .await
        .map_err(|_| Error::Infrastructure)?;
    String::from_utf8(bytes).map_err(|_| Error::Corrupt)
}
fn reference(row: &PgRow) -> Result<ContentRef, Error> {
    Ok(ContentRef {
        content_id: ContentId::from_uuid(row.try_get("content_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        byte_length: row
            .try_get::<String, _>("byte_length")
            .map_err(db)?
            .parse()
            .map_err(|_| Error::Corrupt)?,
        sha256: Sha256::from_bytes(&row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
    })
}
fn prompt_labels(prompt: &str, media_type: &str, secure: bool) -> Result<String, Error> {
    // Secure cards expose only title/description. Their response references and
    // any other JSON fields are never part of public search.
    let media = media_type.split(';').next().unwrap_or("").trim();
    if secure && media == "text/plain" {
        return Ok(prompt.to_owned());
    }
    if media != "application/json" && !media.ends_with("+json") {
        return Err(Error::Corrupt);
    }
    let json: serde_json::Value = serde_json::from_str(prompt).map_err(|_| Error::Corrupt)?;
    let mut labels = Vec::new();
    for key in ["title", "description"] {
        if let Some(value) = json.get(key).and_then(|v| v.as_str()) {
            labels.push(value);
        }
    }
    if !secure && let Some(items) = json.get("options").and_then(|v| v.as_array()) {
        for item in items {
            for key in ["label", "description"] {
                if let Some(value) = item.get(key).and_then(|v| v.as_str()) {
                    labels.push(value);
                }
            }
        }
    }
    Ok(labels.join("\n"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prompt_never_indexes_secure_or_internal_fields() {
        let text=prompt_labels(r#"{"title":"连接邮箱","description":"允许访问","secret":"password","internal":{"title":"trace"},"options":[{"label":"允许","value":"opaque token"}]}"#, "application/json", false).unwrap();
        assert_eq!(text, "连接邮箱\n允许访问\n允许");
        assert_eq!(
            prompt_labels(
                r#"{"title":"连接邮箱","options":[{"label":"private broker detail"}]}"#,
                "application/json",
                true
            )
            .unwrap(),
            "连接邮箱"
        );
        assert_eq!(
            prompt_labels("请连接账户", "text/plain;charset=utf-8", true).unwrap(),
            "请连接账户"
        );
    }
}
