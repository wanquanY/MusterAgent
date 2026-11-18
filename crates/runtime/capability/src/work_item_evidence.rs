//! Bounded immutable evidence verification for WorkItem result transactions.
#[cfg(test)]
use musteragent_content_ref::MediaType;
use musteragent_runtime_content::ContentStore;
#[cfg(test)]
use musteragent_runtime_content::PutContent;
#[cfg(test)]
use musteragent_runtime_contract::ApplicationFuture;
// Evidence can be a large media asset. Keep this separate from the small JSON
// material reads: the original Content owner verifies the complete object with
// bounded buffers, then Task commit rechecks its live authority under lock.
pub async fn require_work_item_evidence(
    content: &dyn ContentStore,
    tenant: musteragent_identity::TenantId,
    reference: &musteragent_content_ref::ContentRef,
) -> Result<(), musteragent_runtime_content::ContentStoreError> {
    let verified = content.open_verified(tenant, reference).await?;
    if verified.reference() != reference {
        return Err(musteragent_runtime_content::ContentStoreError::Invariant);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, Sha256};
    use musteragent_identity::{ContentId, TenantId};
    use musteragent_runtime_content::{
        ContentRangeSource, ContentStoreError, VerifiedContentReader,
    };
    struct Repeated(u64);
    impl ContentRangeSource for Repeated {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<usize, ContentStoreError> {
            let count = self.0.saturating_sub(offset).min(buffer.len() as u64) as usize;
            buffer[..count].fill(7);
            Ok(count)
        }
    }
    struct EvidenceStore {
        tenant: TenantId,
    }
    impl ContentStore for EvidenceStore {
        fn put(
            &self,
            _: PutContent,
        ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
            Box::pin(async { panic!("verification must never write evidence") })
        }
        fn read<'a>(
            &'a self,
            _: TenantId,
            _: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
            Box::pin(async { panic!("media verification must not buffer the entire object") })
        }
        fn open_verified<'a>(
            &'a self,
            tenant: TenantId,
            reference: &'a ContentRef,
        ) -> ApplicationFuture<'a, Result<VerifiedContentReader, ContentStoreError>> {
            Box::pin(async move {
                if tenant != self.tenant {
                    return Err(ContentStoreError::Unauthorized);
                }
                VerifiedContentReader::verify(
                    reference.clone(),
                    Box::new(Repeated(reference.byte_length)),
                    &|| false,
                )
            })
        }
    }
    #[tokio::test]
    async fn large_media_evidence_uses_original_bounded_integrity_and_tenant_checks() {
        let tenant = TenantId::generate();
        let content = EvidenceStore { tenant };
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("video/mp4").unwrap(),
            byte_length: 17 * 1024 * 1024,
            sha256: Sha256::digest(vec![7; 17 * 1024 * 1024]),
        };
        require_work_item_evidence(&content, tenant, &reference)
            .await
            .unwrap();
        assert_eq!(
            require_work_item_evidence(&content, TenantId::generate(), &reference).await,
            Err(ContentStoreError::Unauthorized)
        );
        let mut corrupted = reference;
        corrupted.sha256 = Sha256::digest(b"wrong media bytes");
        assert_eq!(
            require_work_item_evidence(&content, tenant, &corrupted).await,
            Err(ContentStoreError::HashMismatch)
        );
    }
}
