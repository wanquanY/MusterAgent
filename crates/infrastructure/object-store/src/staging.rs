//! Owns private staging keys and aborts unfinished uploads on cancellation.
use super::*;

pub(super) struct StagingObject {
    store: Arc<dyn ObjectStore>,
    pub(super) path: Path,
    pub(super) upload: Option<Box<dyn object_store::MultipartUpload>>,
    cleaned: bool,
}
impl StagingObject {
    pub(super) fn new(adapter: &ObjectStoreBlobStore) -> Self {
        Self {
            store: adapter.store.clone(),
            path: Path::from(format!(
                "{}/.staging/{}",
                adapter.prefix,
                uuid::Uuid::now_v7()
            )),
            upload: None,
            cleaned: false,
        }
    }

    pub(super) async fn cleanup(&mut self) -> object_store::Result<()> {
        if let Some(mut upload) = self.upload.take() {
            let _ = upload.abort().await;
        }
        self.store.delete(&self.path).await?;
        self.cleaned = true;
        Ok(())
    }
}
impl Drop for StagingObject {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        let store = self.store.clone();
        let path = self.path.clone();
        let upload = self.upload.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(mut upload) = upload {
                    let _ = upload.abort().await;
                }
                let _ = store.delete(&path).await;
            });
        }
    }
}
