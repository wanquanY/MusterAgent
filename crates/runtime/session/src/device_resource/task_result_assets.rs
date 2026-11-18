//! Exact original Artifact producer facts are part of the immutable command.
//! Root bytes are attached only to the renewable signed execution request.
use super::*;
use crate::{DeviceArtifactContents, DeviceArtifactExportManifest, WorkItemResultSubmissionInput};
impl DeviceResourceCommand {
    pub fn result_artifact_manifest(&self) -> Option<&DeviceArtifactExportManifest> {
        self.result_artifact_manifest.as_ref()
    }
    pub fn result_artifact_commitment(&self) -> Option<Sha256> {
        self.result_metadata
            .as_deref()
            .map(|b| digest(b"musteragent.native-task-result-export-metadata.v1\0", b))
    }
    pub fn result_artifact_fingerprint(&self) -> Result<Sha256, Error> {
        if self.operation != DeviceResourceOperation::TaskSubmitResult
            || self.admission.work_item.is_none()
        {
            return Err(Error::Forbidden);
        }
        let mut base = self.clone();
        base.result_artifact_manifest = None;
        base.result_metadata = None;
        Ok(digest(
            b"musteragent.native-task-result-export.v1\0",
            &base.encode()?,
        ))
    }
    pub fn with_result_artifact_manifest(
        mut self,
        manifest: DeviceArtifactExportManifest,
    ) -> Result<Self, Error> {
        let input = WorkItemResultSubmissionInput::parse(&self.request_bytes)
            .map_err(|_| Error::InvalidArguments)?;
        // The fingerprint covers the entire submitted command, including every
        // canonical file locator. Only locally produced Artifacts need exports.
        let mut local_selection = input.delivery.publication;
        local_selection.artifacts.retain(|r| {
            matches!(
                r.artifact,
                musteragent_execution_context::AgentArtifactLocator::Artifact(_)
            )
        });
        manifest
            .validate_selection(
                self.result_artifact_fingerprint()?,
                self.invocation.invocation_id,
                &local_selection,
            )
            .map_err(|_| Error::Forbidden)?;
        if self.result_artifact_manifest.is_some()
            || manifest.scope.tenant_id != self.run_context().tenant_id
            || manifest.scope.conversation_id != self.run_context().conversation_id
            || manifest.entries.iter().any(|e| {
                e.version.committed_by.participant_id()
                    != Some(self.run_context().executor_participant_id)
            })
        {
            return Err(Error::Forbidden);
        }
        for e in &manifest.entries {
            DeviceArtifactContents::require_supported_result_entry(e)
                .map_err(|_| Error::ReferenceUnavailable)?;
        }
        self.result_metadata = Some(
            manifest
                .result_metadata()
                .map_err(|_| Error::InvalidArguments)?,
        );
        self.result_artifact_manifest = Some(manifest);
        self.commitment = digest(b"musteragent.device-resource-command.v1\0", &self.encode()?);
        Ok(self)
    }
}
impl DeviceResourceRequest {
    pub fn with_result_roots(mut self, roots: Vec<Vec<u8>>) -> Result<Self, Error> {
        let manifest = self
            .command
            .result_artifact_manifest()
            .ok_or(Error::Forbidden)?;
        if self.is_memory_approval() || roots.len() != manifest.entries.len() {
            return Err(Error::InvalidArguments);
        }
        for (root, entry) in roots.iter().zip(&manifest.entries) {
            DeviceArtifactContents::validate_result_root(entry, root)
                .map_err(|_| Error::InvalidArguments)?;
        }
        self.result_roots = roots;
        Ok(self)
    }
    pub fn result_roots(&self) -> &[Vec<u8>] {
        &self.result_roots
    }
    pub fn without_result_roots(&self) -> Self {
        Self {
            command: self.command.clone(),
            attempt: self.attempt.clone(),
            memory_approval_only: self.memory_approval_only,
            result_roots: vec![],
        }
    }
}
