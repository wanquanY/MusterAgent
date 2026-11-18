//! Private, versioned Host facts. These types are deliberately absent from the
//! business package ABI. Decoding preserves original facts, never authorization.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_identity::{ContentId, PrincipalId, RunId, WorkspaceId};

macro_rules! id {
    ($kind:ident, $value:expr) => {
        $kind::from_uuid($value).map_err(|_| MemoryToolError::InvalidResult)?
    };
}

macro_rules! scalar_enum {
    ($wire:ident, $domain:ident, [$($variant:ident),+]) => {
        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub(super) enum $wire { $($variant),+ }
        impl From<$domain> for $wire {
            fn from(value: $domain) -> Self { match value {$($domain::$variant => Self::$variant),+} }
        }
        impl From<$wire> for $domain {
            fn from(value: $wire) -> Self { match value {$($wire::$variant => Self::$variant),+} }
        }
    };
}
scalar_enum!(
    Kind,
    MemoryKind,
    [Preference, Semantic, Procedure, FailureLesson]
);
scalar_enum!(
    Lifecycle,
    MemoryLifecycle,
    [Active, Superseded, Revoked, Expired]
);
scalar_enum!(
    Source,
    MemorySource,
    [UserAuthored, VerifiedExperience, GovernedImport]
);
scalar_enum!(
    Classification,
    DataClassification,
    [Public, Internal, Confidential, Restricted]
);

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Scope {
    User(Uuid),
    Tenant(Uuid),
    Workspace(Uuid),
    Agent(Uuid),
}
impl From<MemoryScope> for Scope {
    fn from(value: MemoryScope) -> Self {
        match value {
            MemoryScope::User { principal_id } => Self::User(principal_id.into_uuid()),
            MemoryScope::Tenant { tenant_id } => Self::Tenant(tenant_id.into_uuid()),
            MemoryScope::Workspace { workspace_id } => Self::Workspace(workspace_id.into_uuid()),
            MemoryScope::Agent { agent_id } => Self::Agent(agent_id.into_uuid()),
        }
    }
}
impl Scope {
    fn restore(self) -> Result<MemoryScope, MemoryToolError> {
        Ok(match self {
            Self::User(id) => MemoryScope::User {
                principal_id: id!(PrincipalId, id),
            },
            Self::Tenant(id) => MemoryScope::Tenant {
                tenant_id: id!(TenantId, id),
            },
            Self::Workspace(id) => MemoryScope::Workspace {
                workspace_id: id!(WorkspaceId, id),
            },
            Self::Agent(id) => MemoryScope::Agent {
                agent_id: id!(AgentId, id),
            },
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Content {
    id: Uuid,
    media: String,
    length: u64,
    sha256: [u8; 32],
}
impl From<&ContentRef> for Content {
    fn from(value: &ContentRef) -> Self {
        Self {
            id: value.content_id.into_uuid(),
            media: value.media_type.as_str().into(),
            length: value.byte_length,
            sha256: value.sha256.into_bytes(),
        }
    }
}
impl Content {
    pub(super) fn restore(self) -> Result<ContentRef, MemoryToolError> {
        Ok(ContentRef {
            content_id: id!(ContentId, self.id),
            media_type: MediaType::new(self.media).map_err(|_| MemoryToolError::InvalidResult)?,
            byte_length: self.length,
            sha256: Sha256::from_bytes(self.sha256).map_err(|_| MemoryToolError::InvalidResult)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Origin {
    record_id: Uuid,
    content: Content,
}
impl From<&UserAuthoredRunOrigin> for Origin {
    fn from(value: &UserAuthoredRunOrigin) -> Self {
        Self {
            record_id: value.record_id.into_uuid(),
            content: (&value.content_ref).into(),
        }
    }
}
impl Origin {
    pub(super) fn restore(self) -> Result<UserAuthoredRunOrigin, MemoryToolError> {
        Ok(UserAuthoredRunOrigin {
            record_id: id!(LedgerRecordId, self.record_id),
            content_ref: self.content.restore()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    memory_id: Uuid,
    tenant_id: Uuid,
    scope: Scope,
    key: String,
    kind: Kind,
    lifecycle: Lifecycle,
    current_version_id: Uuid,
    created_at: i64,
    updated_at: i64,
    revoked_at: Option<i64>,
    version: Version,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Version {
    id: Uuid,
    memory_id: Uuid,
    number: u64,
    source: Source,
    content: Content,
    confidence_bps: u16,
    classification: Classification,
    source_run_ids: BTreeSet<Uuid>,
    evidence_record_ids: BTreeSet<Uuid>,
    created_at: i64,
    valid_until: Option<i64>,
}

impl From<&MemorySearchHit> for Head {
    fn from(value: &MemorySearchHit) -> Self {
        let r = &value.record;
        let v = &value.version;
        Self {
            memory_id: r.memory_id.into_uuid(),
            tenant_id: r.tenant_id.into_uuid(),
            scope: r.scope.into(),
            key: r.memory_key.as_str().into(),
            kind: r.kind.into(),
            lifecycle: r.lifecycle.into(),
            current_version_id: r.current_version_id.into_uuid(),
            created_at: r.created_at.unix_millis(),
            updated_at: r.updated_at.unix_millis(),
            revoked_at: r.revoked_at.map(UtcTimestamp::unix_millis),
            version: Version {
                id: v.memory_version_id.into_uuid(),
                memory_id: v.memory_id.into_uuid(),
                number: v.version.get(),
                source: v.source.into(),
                content: (&v.content_ref).into(),
                confidence_bps: v.confidence.get(),
                classification: v.data_classification.into(),
                source_run_ids: v.source_run_ids.iter().map(|id| id.into_uuid()).collect(),
                evidence_record_ids: v
                    .evidence_record_ids
                    .iter()
                    .map(|id| id.into_uuid())
                    .collect(),
                created_at: v.created_at.unix_millis(),
                valid_until: v.valid_until.map(UtcTimestamp::unix_millis),
            },
        }
    }
}

impl Head {
    pub(super) fn restore(self) -> Result<MemorySearchHit, MemoryToolError> {
        let v = self.version;
        let record = MemoryRecord {
            memory_id: id!(MemoryId, self.memory_id),
            tenant_id: id!(TenantId, self.tenant_id),
            scope: self.scope.restore()?,
            memory_key: IdempotencyKey::new(self.key)
                .map_err(|_| MemoryToolError::InvalidResult)?,
            kind: self.kind.into(),
            lifecycle: self.lifecycle.into(),
            current_version_id: id!(MemoryVersionId, self.current_version_id),
            created_at: UtcTimestamp::from_unix_millis(self.created_at),
            updated_at: UtcTimestamp::from_unix_millis(self.updated_at),
            revoked_at: self.revoked_at.map(UtcTimestamp::from_unix_millis),
        };
        let version = MemoryVersion {
            memory_version_id: id!(MemoryVersionId, v.id),
            memory_id: id!(MemoryId, v.memory_id),
            version: VersionNumber::new(v.number).map_err(|_| MemoryToolError::InvalidResult)?,
            source: v.source.into(),
            content_ref: v.content.restore()?,
            confidence: MemoryConfidenceBps::new(v.confidence_bps)
                .map_err(|_| MemoryToolError::InvalidResult)?,
            data_classification: v.classification.into(),
            source_run_ids: v
                .source_run_ids
                .into_iter()
                .map(|id| RunId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult))
                .collect::<Result<_, _>>()?,
            evidence_record_ids: v
                .evidence_record_ids
                .into_iter()
                .map(|id| LedgerRecordId::from_uuid(id).map_err(|_| MemoryToolError::InvalidResult))
                .collect::<Result<_, _>>()?,
            created_at: UtcTimestamp::from_unix_millis(v.created_at),
            valid_until: v.valid_until.map(UtcTimestamp::from_unix_millis),
        };
        record
            .validate()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        version
            .validate()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        if record.memory_id != version.memory_id
            || record.current_version_id != version.memory_version_id
        {
            return Err(MemoryToolError::InvalidResult);
        }
        Ok(MemorySearchHit { record, version })
    }
}
