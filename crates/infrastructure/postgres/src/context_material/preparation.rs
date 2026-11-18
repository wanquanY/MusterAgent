//! Decode immutable snapshot bytes once per physical planning invocation.
//! The caller still reads the Run fence and verifies its stored digest on every
//! access. No lease, source window, content authorization or live grant is held.
use super::{ContextPlanInput, PostgresContextMaterialError as Error, Sha256};
use musteragent_runtime_snapshot::RuntimeSnapshotContent;
use std::sync::{Arc, Mutex};

pub(super) struct SnapshotPreparation {
    input: ContextPlanInput,
    snapshot: Mutex<Option<(Sha256, Arc<RuntimeSnapshotContent>)>>,
}

impl SnapshotPreparation {
    pub(super) fn new(input: ContextPlanInput) -> Self {
        Self {
            input,
            snapshot: Mutex::new(None),
        }
    }

    pub(super) fn require(&self, input: &ContextPlanInput) -> Result<(), Error> {
        if self.input != *input {
            return Err(Error::StaleFence);
        }
        Ok(())
    }

    pub(super) fn snapshot(
        &self,
        digest: Sha256,
        sealed: Arc<RuntimeSnapshotContent>,
    ) -> Result<Arc<RuntimeSnapshotContent>, Error> {
        let mut retained = self.snapshot.lock().map_err(|_| Error::CorruptState)?;
        if let Some((original, snapshot)) = retained.as_ref() {
            if *original != digest {
                // The same Run cannot switch its sealed Snapshot mid-plan.
                return Err(Error::CorruptStateAt("preparation_snapshot_changed"));
            }
            return Ok(snapshot.clone());
        }
        *retained = Some((digest, sealed.clone()));
        Ok(sealed)
    }
}
