//! S3-compatible endpoints can return success while ignoring a conditional
//! header. Exercise each publication primitive on private scratch keys before
//! letting it address a content identity. Share successful checks across clones.
use super::*;
use crate::staging::StagingObject;
use tokio::sync::OnceCell;

#[derive(Default)]
pub(super) struct WriteOnceChecks {
    put: OnceCell<()>,
    multipart_copy: OnceCell<()>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Publication {
    Put,
    MultipartCopy,
}

impl ObjectStoreBlobStore {
    pub(super) async fn check_write_once(
        &self,
        publication: Publication,
    ) -> object_store::Result<()> {
        let Some(checks) = &self.s3_checks else {
            return Ok(());
        };
        let checked = match publication {
            Publication::Put => &checks.put,
            Publication::MultipartCopy => &checks.multipart_copy,
        };
        checked
            .get_or_try_init(|| async {
                let result = self.probe_publication(publication).await;
                if result.is_err() {
                    tracing::error!(?publication, "S3 write-once capability check failed");
                }
                result
            })
            .await
            .map(|_| ())
    }

    async fn probe_publication(&self, publication: Publication) -> object_store::Result<()> {
        let mut target = StagingObject::new(self);
        let check = self.check_collision(publication, &target.path).await;
        let cleanup = target.cleanup().await;
        check.and(cleanup)
    }

    async fn check_collision(
        &self,
        publication: Publication,
        target: &Path,
    ) -> object_store::Result<()> {
        const ORIGINAL: &[u8] = b"write-once probe";
        const OTHER: &[u8] = b"collision probe";
        let result = match publication {
            Publication::Put => {
                self.store
                    .put_opts(target, ORIGINAL.into(), PutMode::Create.into())
                    .await?;
                self.store
                    .put_opts(target, OTHER.into(), PutMode::Create.into())
                    .await
                    .map(|_| ())
            }
            Publication::MultipartCopy => {
                let mut source = StagingObject::new(self);
                self.store.put(target, ORIGINAL.into()).await?;
                self.store.put(&source.path, OTHER.into()).await?;
                let result = self.store.copy_if_not_exists(&source.path, target).await;
                source.cleanup().await?;
                result
            }
        };
        match result {
            Err(object_store::Error::AlreadyExists { .. }) => {
                let original = self.store.get(target).await?;
                if original.meta.size == ORIGINAL.len() as u64
                    && original.bytes().await? == ORIGINAL
                {
                    return Ok(());
                }
            }
            Err(error) => return Err(error),
            Ok(()) => {}
        }
        tracing::error!(
            ?publication,
            "S3 accepted a duplicate create or changed the original bytes"
        );
        Err(object_store::Error::NotSupported {
            source: "bucket did not enforce the configured write-once precondition".into(),
        })
    }
}
