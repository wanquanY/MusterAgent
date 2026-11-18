//! Immutable candidate metadata. File bytes and dependencies always remain in ContentStore.
use super::*;
use musteragent_artifact::{ArtifactOrigin, ArtifactSource, ArtifactVersionAuthor};
use musteragent_identity::{
    ActivityId, IdempotencyKey, ParticipantId, ResourceChangeSetId, ResourceRevision, UtcTimestamp,
    VersionNumber, VersionedKey,
};
use musteragent_resource::ResourceName;
use musteragent_runtime_artifact::{ArtifactMutation, CommitPreparedArtifactTool};
use musteragent_runtime_resource::{
    PreparedResourceFilePlacement, ResourceCommandContext, ResourceCommandSource,
    ResourceMutationIdentity,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Capsule {
    pub schema: u8,
    pub native: Option<super::native::Witness>,
    pub tenant: Uuid,
    pub task: Uuid,
    pub producer: Uuid,
    pub snapshot: Uuid,
    pub conversation: Uuid,
    pub activity: Option<Uuid>,
    pub invocation: Uuid,
    pub author: Uuid,
    pub artifact: Uuid,
    pub version: Uuid,
    pub number: u64,
    pub at: i64,
    pub root: Vec<u8>,
    pub dependencies: Vec<Vec<u8>>,
    pub change: Change,
    pub placement: Option<Placement>,
    pub base_node: Option<Uuid>,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Change {
    Create {
        artifact_kind: i16,
        lifecycle: i16,
        title: String,
        created_at: i64,
    },
    Revise {
        expected: u64,
        expected_id: Uuid,
        expected_root: Vec<u8>,
        title: Option<String>,
    },
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Placement {
    pub node: Uuid,
    pub parent: Option<Uuid>,
    pub revision: u64,
    change: Uuid,
    key: String,
    fingerprint: [u8; 32],
    path: Vec<String>,
    sort: String,
    tool: String,
}
impl Placement {
    pub(super) fn capture(
        value: &PreparedResourceFilePlacement,
        revision: u64,
        scope: &WorkItemResultScope,
    ) -> Result<Self, Error> {
        let context = &value.context;
        value.validate().map_err(|_| Error::Unavailable)?;
        if !matches!(context.source, ResourceCommandSource::DialogueExecution {agent_id,..} | ResourceCommandSource::DeviceExecution {agent_id,..} if agent_id==scope.producing_agent_id)
            || context.actor != actor(scope)
            || value.expected_revision.is_some_and(|r| r.get() != revision)
        {
            return Err(Error::RevisionConflict);
        }
        Ok(Self {
            node: value.node_id.into_uuid(),
            parent: value.parent_node_id.map(ResourceNodeId::into_uuid),
            revision,
            change: value.identity.change_set_id.into_uuid(),
            key: value.identity.idempotency_key.as_str().into(),
            fingerprint: value.identity.command_fingerprint.into_bytes(),
            path: value.path.iter().map(|p| p.as_str().to_owned()).collect(),
            sort: value.sort_key.clone(),
            tool: context
                .tool_key
                .as_ref()
                .ok_or(Error::Corrupt)?
                .as_str()
                .to_owned(),
        })
    }
    pub(super) fn material(
        &self,
        cap: &Capsule,
        scope: &WorkItemResultScope,
    ) -> Result<PreparedResourceFilePlacement, Error> {
        let p = PreparedResourceFilePlacement {
            context: ResourceCommandContext {
                tenant_id: scope.tenant_id,
                actor: actor(scope),
                source: cap.resource_source(scope)?,
                invocation_id: Some(id(cap.invocation)?),
                tool_key: Some(VersionedKey::new(self.tool.clone()).map_err(|_| Error::Corrupt)?),
                now: UtcTimestamp::from_unix_millis(cap.at),
            },
            identity: ResourceMutationIdentity {
                change_set_id: ResourceChangeSetId::from_uuid(self.change)
                    .map_err(|_| Error::Corrupt)?,
                idempotency_key: IdempotencyKey::new(self.key.clone())
                    .map_err(|_| Error::Corrupt)?,
                command_fingerprint: Sha256::from_bytes(self.fingerprint)
                    .map_err(|_| Error::Corrupt)?,
            },
            expected_revision: Some(ResourceRevision::new(self.revision)),
            node_id: ResourceNodeId::from_uuid(self.node).map_err(|_| Error::Corrupt)?,
            parent_node_id: self
                .parent
                .map(ResourceNodeId::from_uuid)
                .transpose()
                .map_err(|_| Error::Corrupt)?,
            path: self
                .path
                .iter()
                .map(|n| ResourceName::new(n.clone()).map_err(|_| Error::Corrupt))
                .collect::<Result<_, _>>()?,
            sort_key: self.sort.clone(),
        };
        p.validate().map_err(|_| Error::Corrupt)?;
        Ok(p)
    }
}
impl Capsule {
    pub(super) fn resource_source(
        &self,
        s: &WorkItemResultScope,
    ) -> Result<ResourceCommandSource, Error> {
        let conversation_id = id(self.conversation)?;
        Ok(match &self.native {
            None => ResourceCommandSource::DialogueExecution {
                conversation_id,
                agent_id: s.producing_agent_id,
            },
            Some(w) => ResourceCommandSource::DeviceExecution {
                conversation_id,
                agent_id: s.producing_agent_id,
                intent_id: s.source_intent_id,
                work_item: Some(musteragent_runtime_resource::DeviceWorkItemSource {
                    work_item_id: s.work_item_id,
                    goal_revision: s.goal_revision,
                    control_revision: s.control_revision,
                    assignment_epoch: s.assignment_epoch,
                }),
                run_id: s.producing_run_id,
                device_id: id(w.device)?,
                installation_id: id(w.installation)?,
                source_digest: Sha256::from_bytes(w.source_digest).map_err(|_| Error::Corrupt)?,
                snapshot_hash: Sha256::from_bytes(w.snapshot).map_err(|_| Error::Corrupt)?,
                grant_proof_hash: Sha256::from_bytes(w.grant).map_err(|_| Error::Corrupt)?,
                command_commitment: Sha256::from_bytes(w.commitment).map_err(|_| Error::Corrupt)?,
            },
        })
    }
    pub(super) fn capture(
        c: &CommitPreparedArtifactTool,
        scope: &WorkItemResultScope,
        change: Change,
        placement: Option<Placement>,
    ) -> Result<Self, Error> {
        let v = c.mutation.version();
        let author = v.committed_by.participant_id().ok_or(Error::Unavailable)?;
        if author != c.run_context.executor_participant_id
            || v.source_invocation_id != Some(c.invocation_id)
        {
            return Err(Error::Unavailable);
        }
        Ok(Self {
            schema: 1,
            native: None,
            tenant: scope.tenant_id.into_uuid(),
            task: scope.work_item_id.into_uuid(),
            producer: scope.producing_run_id.into_uuid(),
            snapshot: scope.producing_snapshot_id.into_uuid(),
            conversation: c.run_context.conversation_id.into_uuid(),
            activity: Some(c.run_context.activity_id.into_uuid()),
            invocation: c.invocation_id.into_uuid(),
            author: author.into_uuid(),
            artifact: v.artifact_id.into_uuid(),
            version: v.artifact_version_id.into_uuid(),
            number: v.version.get(),
            at: v.committed_at.unix_millis(),
            root: content_bytes(&v.content_ref),
            dependencies: v.dependency_refs.iter().map(content_bytes).collect(),
            change,
            placement,
            base_node: c
                .resource_replacement
                .as_ref()
                .map(|r| r.file.node_id.into_uuid()),
        })
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|_| Error::Corrupt)
    }
    pub(super) fn decode(
        bytes: &[u8],
        expected: &[u8],
        scope: &WorkItemResultScope,
    ) -> Result<Self, Error> {
        if Sha256::digest(bytes).into_bytes().as_slice() != expected {
            return Err(Error::Corrupt);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)?;
        if value.schema != 1
            || value.tenant != scope.tenant_id.into_uuid()
            || value.task != scope.work_item_id.into_uuid()
            || value.snapshot != scope.producing_snapshot_id.into_uuid()
            || value.producer != scope.producing_run_id.into_uuid()
            || value.encode() != Ok(bytes.to_vec())
        {
            return Err(Error::Corrupt);
        }
        value.mutation()?;
        Ok(value)
    }
    pub(super) fn mutation(&self) -> Result<ArtifactMutation, Error> {
        let root = content_from_bytes(self.root.clone())?;
        let version = ArtifactVersion {
            artifact_id: id(self.artifact)?,
            artifact_version_id: id(self.version)?,
            version: VersionNumber::new(self.number).map_err(|_| Error::Corrupt)?,
            content_hash: root.sha256,
            content_ref: root,
            dependency_refs: self
                .dependencies
                .iter()
                .cloned()
                .map(content_from_bytes)
                .collect::<Result<_, _>>()?,
            source_invocation_id: if self.native.is_some() {
                None
            } else {
                Some(id(self.invocation)?)
            },
            committed_by: ArtifactVersionAuthor::Participant(
                ParticipantId::from_uuid(self.author).map_err(|_| Error::Corrupt)?,
            ),
            committed_at: UtcTimestamp::from_unix_millis(self.at),
        };
        version.validate().map_err(|_| Error::Corrupt)?;
        match &self.change {
            Change::Create {
                artifact_kind,
                lifecycle,
                title,
                created_at,
            } if self.number == 1 => {
                let artifact = Artifact {
                    artifact_id: version.artifact_id,
                    tenant_id: id(self.tenant)?,
                    source: ArtifactSource::Conversation(id(self.conversation)?),
                    origin: ArtifactOrigin::Runtime {
                        activity_id: ActivityId::from_uuid(self.activity.ok_or(Error::Corrupt)?)
                            .map_err(|_| Error::Corrupt)?,
                        created_by_run_id: id(self
                            .native
                            .as_ref()
                            .map_or(self.producer, |n| n.creation_run))?,
                    },
                    kind: super::super::decode_kind(*artifact_kind).map_err(|_| Error::Corrupt)?,
                    lifecycle: super::super::decode_lifecycle(*lifecycle)
                        .map_err(|_| Error::Corrupt)?,
                    current_version: version.version,
                    title: title.clone(),
                    created_at: UtcTimestamp::from_unix_millis(*created_at),
                    updated_at: version.committed_at,
                };
                artifact.validate().map_err(|_| Error::Corrupt)?;
                Ok(ArtifactMutation::Create { artifact, version })
            }
            Change::Revise {
                expected, title, ..
            } if expected.checked_add(1) == Some(self.number) => Ok(ArtifactMutation::Update {
                artifact_id: version.artifact_id,
                expected_version: VersionNumber::new(*expected).map_err(|_| Error::Corrupt)?,
                version,
                title: title.clone(),
            }),
            _ => Err(Error::Corrupt),
        }
    }
    pub(super) fn node(&self) -> Option<Uuid> {
        self.placement
            .as_ref()
            .map(|p| p.node)
            .or(self.base_node)
            .or_else(|| {
                self.native
                    .as_ref()
                    .and_then(|w| w.root_revision)
                    .map(|_| Uuid::new_v5(&self.artifact, b"device-file-placement@1"))
            })
    }
}
