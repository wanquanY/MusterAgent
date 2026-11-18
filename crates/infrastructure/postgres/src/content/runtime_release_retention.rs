//! Relocate a legacy public builtin into durable storage without rewriting any
//! ContentRef field. Bytes are verified and stored before the pointer CAS, so a
//! failed upload leaves the original readable object untouched.
use super::*;

impl PostgresContentStore {
    pub async fn retain_runtime_release_at(
        &self,
        reference: &ContentRef,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, ContentStoreError> {
        self.retain_runtime_release_inner(reference, bytes)
            .await
            .map_err(Into::into)
    }
    async fn retain_runtime_release_inner(
        &self,
        reference: &ContentRef,
        bytes: Vec<u8>,
    ) -> Result<ContentRef, PostgresContentError> {
        verify_content(reference, &bytes)?;
        let tenant = runtime_release_storage_tenant_id()?;
        let persisted = self
            .put_runtime_release_at(
                reference.content_id,
                PutContent {
                    tenant_id: tenant,
                    media_type: reference.media_type.clone(),
                    bytes: bytes.clone(),
                },
            )
            .await?;
        if persisted != *reference {
            return Err(ContentStoreError::IdentityConflict.into());
        }
        let legacy:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE content_id=$1 AND tenant_id IS NULL AND ownership_kind=2 AND state=2 AND storage_ref LIKE 'runtime-builtin://skill/%' AND storage_ref_protected IS NULL)")
            .bind(reference.content_id.into_uuid()).fetch_one(&self.pool).await?;
        if legacy {
            let identity = BlobIdentity {
                tenant_id: tenant,
                content_id: reference.content_id,
            };
            let receipt = self
                .blobs
                .put(PutBlob {
                    identity,
                    sha256: reference.sha256,
                    bytes,
                })
                .await?;
            let durable = self.blobs.read(identity, &receipt.locator).await?;
            verify_content(reference, &durable)?;
            let protected = self
                .reference_keys
                .seal(&protection_context(identity), receipt.locator.as_str())?;
            sqlx::query("UPDATE artifact.content_refs SET ownership_kind=3,storage_ref=NULL,storage_ref_protected=$2,updated_at_ms=(EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT WHERE content_id=$1 AND tenant_id IS NULL AND ownership_kind=2 AND state=2 AND storage_ref LIKE 'runtime-builtin://skill/%' AND storage_ref_protected IS NULL AND media_type=$3 AND byte_length=$4::bigint AND content_hash=$5")
                .bind(reference.content_id.into_uuid()).bind(protected.as_bytes()).bind(reference.media_type.as_str())
                .bind(reference.byte_length.to_string()).bind(reference.sha256.into_bytes().to_vec()).execute(&self.pool).await?;
        }
        let durable:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifact.content_refs WHERE content_id=$1 AND tenant_id IS NULL AND ownership_kind=3 AND state=2 AND storage_ref IS NULL AND storage_ref_protected IS NOT NULL)")
            .bind(reference.content_id.into_uuid()).fetch_one(&self.pool).await?;
        if !durable {
            return Err(ContentStoreError::Invariant.into());
        }
        let read = self.read_uncached_inner(tenant, reference).await?;
        verify_content(reference, &read)?;
        Ok(reference.clone())
    }
}
