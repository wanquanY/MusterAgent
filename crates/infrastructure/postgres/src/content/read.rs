//! Metadata-first batches. Hot reads do not transfer inline bodies or read
//! object storage. Full-reference authorization precedes every cache access;
//! a durable revision plus publication lock closes deletion/reconnect races.
//! When that global revision changes, exact PostgreSQL row versions distinguish
//! a revoked input from unrelated writes; cache churn is not a read failure.
use super::{
    BatchContentRow, CONTENT_READY, CONTENT_TOMBSTONED, MAX_CONCURRENT_EXTERNAL_CONTENT_READS,
    PostgresContentError, PostgresContentStore, protection_context,
    runtime_release_storage_tenant_id, tenant_blob_identity,
};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{ContentId, TenantId};
use musteragent_runtime_content::{
    ContentCacheError, ContentStoreError, StorageLocator, verify_content,
};
use musteragent_storage_protection::ProtectedOpaqueRef;
use sqlx::Row as _;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;

const METADATA_BATCH_SIZE: usize = 128;

impl PostgresContentStore {
    pub(super) async fn read_batch_inner(
        &self,
        tenant: TenantId,
        references: &[ContentRef],
    ) -> Result<Vec<Vec<u8>>, PostgresContentError> {
        match self.read_batch_authorized(tenant, references, true).await {
            Err(PostgresContentError::Cache(
                ContentCacheError::Invalidated | ContentCacheError::Unavailable,
            )) => self.read_batch_authorized(tenant, references, false).await,
            result => result,
        }
    }

    async fn read_batch_authorized(
        &self,
        tenant: TenantId,
        references: &[ContentRef],
        use_cache: bool,
    ) -> Result<Vec<Vec<u8>>, PostgresContentError> {
        if references.is_empty() {
            return Ok(Vec::new());
        }
        let mut token = if use_cache && self.content_cache.online.load(Ordering::Acquire) {
            cache_hint(self.content_cache.bytes.begin_read(tenant))?
        } else {
            None
        };
        let mut positions = HashMap::new();
        let mut unique = Vec::new();
        let mut order = Vec::with_capacity(references.len());
        for reference in references {
            let next = unique.len();
            let index = *positions.entry(reference).or_insert_with(|| {
                unique.push(reference);
                next
            });
            order.push(index);
        }
        let release = runtime_release_storage_tenant_id()?;
        let mut persisted = BTreeMap::new();
        let mut revision = None;
        for group in unique.chunks(METADATA_BATCH_SIZE) {
            let ids: Vec<_> = group.iter().map(|r| r.content_id.into_uuid()).collect();
            let rows = sqlx::query(
                "SELECT rev.revision, content.content_id, content.xmin::text AS reference_version,
                        inline.xmin::text AS inline_version, content.tenant_id, content.media_type,
                        content.byte_length::text AS byte_length, content.content_hash,
                        content.state, content.storage_ref, content.storage_ref_protected,
                        content.blob_content_id,
                        inline.byte_length AS inline_byte_length, inline.content_hash AS inline_content_hash
                 FROM artifact.content_cache_revision rev
                 LEFT JOIN artifact.content_refs content ON content.content_id=ANY($1)
                 LEFT JOIN artifact.inline_content_blobs inline
                   ON inline.content_id=COALESCE(content.blob_content_id, content.content_id)
                   AND inline.tenant_id=COALESCE(content.tenant_id, $2)
                 WHERE rev.singleton"
            ).bind(ids).bind(release.into_uuid()).fetch_all(&self.pool).await?;
            if rows.is_empty() {
                return Err(PostgresContentError::Corrupt);
            }
            for row in rows {
                let current: i64 = row.try_get("revision")?;
                if cache_hint(self.content_cache.observe(current))?.is_none() {
                    token = None;
                }
                revision.get_or_insert(current);
                let Some(id) = row.try_get::<Option<uuid::Uuid>, _>("content_id")? else {
                    continue;
                };
                persisted.insert(
                    ContentId::from_uuid(id).map_err(|_| PostgresContentError::Corrupt)?,
                    BatchContentRow {
                        reference_version: row.try_get("reference_version")?,
                        inline_version: row.try_get("inline_version")?,
                        owner: row.try_get("tenant_id")?,
                        media_type: row.try_get("media_type")?,
                        byte_length: row.try_get("byte_length")?,
                        content_hash: row.try_get("content_hash")?,
                        state: row.try_get("state")?,
                        storage_ref: row.try_get("storage_ref")?,
                        storage_ref_protected: row.try_get("storage_ref_protected")?,
                        inline_byte_length: row.try_get("inline_byte_length")?,
                        inline_content_hash: row.try_get("inline_content_hash")?,
                        blob_content_id: row.try_get("blob_content_id")?,
                    },
                );
            }
        }
        let revision = revision.ok_or(PostgresContentError::Corrupt)?;
        let mut payloads: Vec<Option<Vec<u8>>> = vec![None; unique.len()];
        let mut publish_miss = vec![false; unique.len()];
        let mut inline = BTreeMap::<TenantId, Vec<_>>::new();
        let mut external = Vec::new();
        for (index, reference) in unique.iter().enumerate() {
            let row = persisted
                .get(&reference.content_id)
                .ok_or(ContentStoreError::NotFound)?;
            if row.content_ref(reference.content_id)? != **reference {
                return Err(ContentStoreError::Invariant.into());
            }
            match row.state {
                CONTENT_READY => (),
                CONTENT_TOMBSTONED => return Err(ContentStoreError::NotFound.into()),
                _ => return Err(ContentStoreError::Invariant.into()),
            }
            let storage_tenant = match row.owner {
                None if row.storage_ref_protected.is_none() => {
                    // Historical builtin URIs retain their original resolver.
                    payloads[index] = Some(self.read_uncached_inner(tenant, reference).await?);
                    continue;
                }
                None => {
                    if row.blob_content_id.is_some() {
                        return Err(ContentStoreError::Invariant.into());
                    }
                    release
                }
                Some(owner) if owner == tenant.into_uuid() => tenant,
                Some(_) => return Err(ContentStoreError::Unauthorized.into()),
            };
            if row.storage_ref.is_some() {
                return Err(ContentStoreError::Invariant.into());
            }
            let protected = row
                .storage_ref_protected
                .as_ref()
                .ok_or(ContentStoreError::Invariant)?;
            let identity =
                tenant_blob_identity(storage_tenant, reference.content_id, row.blob_content_id)?;
            let locator = StorageLocator::new(self.reference_keys.open(
                &protection_context(identity),
                &ProtectedOpaqueRef::from_bytes(protected.clone())?,
            )?)?;
            self.blobs.validate_locator(identity, &locator)?;
            let is_inline = match (row.inline_byte_length, row.inline_content_hash.as_deref()) {
                (Some(length), Some(hash))
                    if u64::try_from(length).ok() == Some(reference.byte_length)
                        && hash == reference.sha256.into_bytes().as_slice() =>
                {
                    true
                }
                (None, None) => false,
                _ => return Err(ContentStoreError::Invariant.into()),
            };
            if let Some(token) = &token
                && let Some(Some(bytes)) = cache_hint(
                    self.content_cache
                        .bytes
                        .get_after_authorization(token, reference),
                )?
            {
                payloads[index] = Some(bytes.to_vec());
                continue;
            }
            publish_miss[index] = true;
            if is_inline {
                inline.entry(storage_tenant).or_default().push((
                    index,
                    identity.content_id,
                    *reference,
                ));
            } else {
                external.push((index, identity, locator, (*reference).clone()));
            }
        }
        // Fetch only authorized misses. No object I/O holds a SQL transaction.
        // Public release objects use their own storage namespace. They share
        // batching and verified retention, never a caller's private blob scope.
        for (storage_tenant, pending) in inline {
            for group in pending.chunks(METADATA_BATCH_SIZE) {
                let ids: Vec<_> = group.iter().map(|(_, blob, _)| blob.into_uuid()).collect();
                let rows = sqlx::query("SELECT content_id, content_bytes FROM artifact.inline_content_blobs WHERE tenant_id=$1 AND content_id=ANY($2)")
                .bind(storage_tenant.into_uuid()).bind(ids).fetch_all(&self.pool).await?;
                let mut bytes = BTreeMap::new();
                for row in rows {
                    bytes.insert(
                        row.try_get::<uuid::Uuid, _>("content_id")?,
                        row.try_get::<Vec<u8>, _>("content_bytes")?,
                    );
                }
                for (index, blob, reference) in group {
                    // Several references of one batch may share an inline body.
                    let body = bytes
                        .get(&blob.into_uuid())
                        .cloned()
                        .ok_or(ContentStoreError::NotFound)?;
                    verify_content(reference, &body)?;
                    payloads[*index] = Some(body);
                }
            }
        }
        let materialized = stream::iter(external.into_iter().map(
            |(index, identity, locator, reference)| {
                let blobs = self.blobs.clone();
                let permits = self.read_permits.clone();
                async move {
                    let _permit = permits
                        .acquire()
                        .await
                        .map_err(|_| ContentStoreError::Unavailable)?;
                    let bytes = blobs.read(identity, &locator).await?;
                    verify_content(&reference, &bytes)?;
                    Ok::<_, PostgresContentError>((index, bytes))
                }
            },
        ))
        .buffer_unordered(MAX_CONCURRENT_EXTERNAL_CONTENT_READS)
        .try_collect::<Vec<_>>()
        .await?;
        for (index, bytes) in materialized {
            payloads[index] = Some(bytes);
        }
        // Readers share this tiny lock. A deletion updates the same revision
        // in its own commit, so late reads cannot republish across it.
        let mut transaction = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Content)
            .await?;
        let current: i64 = sqlx::query_scalar(
            "SELECT revision FROM artifact.content_cache_revision WHERE singleton FOR SHARE",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if cache_hint(self.content_cache.observe(current))?.is_none() {
            token = None;
        }
        if current != revision {
            validate_read_versions(&mut transaction, release, &persisted).await?;
        }
        if let Some(token) = &token {
            for (index, (reference, body)) in unique.iter().zip(&payloads).enumerate() {
                if publish_miss[index] {
                    cache_hint(self.content_cache.bytes.publish_after_authorization(
                        token,
                        reference,
                        body.as_ref().ok_or(PostgresContentError::Corrupt)?.clone(),
                    ))?;
                }
            }
        }
        transaction.commit().await?;
        order
            .into_iter()
            .map(|index| payloads[index].clone().ok_or(PostgresContentError::Corrupt))
            .collect()
    }
}

// The cache is an optional accelerator. Its notification token can expire after
// authoritative metadata/body verification without invalidating those bytes.
// Integrity failures remain fatal; only cache synchronization is disposable.
fn cache_hint<T>(result: Result<T, ContentCacheError>) -> Result<Option<T>, PostgresContentError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ContentCacheError::Invalidated | ContentCacheError::Unavailable) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn validate_read_versions(
    connection: &mut sqlx::PgConnection,
    release: TenantId,
    expected: &BTreeMap<ContentId, BatchContentRow>,
) -> Result<(), PostgresContentError> {
    // The caller holds the shared revocation lock. No body/object I/O occurs in
    // this transaction. xmin witnesses both metadata and inline bytes, including
    // deletion/reinsertion or body changes whose declared hash was left intact.
    let ids: Vec<_> = expected.keys().map(|id| id.into_uuid()).collect();
    for group in ids.chunks(METADATA_BATCH_SIZE) {
        let rows = sqlx::query(
            "SELECT content.content_id, content.xmin::text AS reference_version,
                    inline.xmin::text AS inline_version
             FROM artifact.content_refs content
             LEFT JOIN artifact.inline_content_blobs inline
               ON inline.content_id=COALESCE(content.blob_content_id, content.content_id)
               AND inline.tenant_id=COALESCE(content.tenant_id, $2)
             WHERE content.content_id=ANY($1)",
        )
        .bind(group)
        .bind(release.into_uuid())
        .fetch_all(&mut *connection)
        .await?;
        if rows.len() != group.len() {
            return Err(ContentCacheError::Invalidated.into());
        }
        for row in rows {
            let id = ContentId::from_uuid(row.try_get("content_id")?)
                .map_err(|_| PostgresContentError::Corrupt)?;
            let previous = expected.get(&id).ok_or(PostgresContentError::Corrupt)?;
            if row.try_get::<String, _>("reference_version")? != previous.reference_version
                || row.try_get::<Option<String>, _>("inline_version")? != previous.inline_version
            {
                // Repeat fresh authorization/materialization only for an input
                // that actually changed, never for another tenant's activity.
                return Err(ContentCacheError::Invalidated.into());
            }
        }
    }
    Ok(())
}
