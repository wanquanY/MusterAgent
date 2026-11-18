use super::*;

pub(crate) fn decode_memory_scope(
    kind: i16,
    scope_id: Option<Uuid>,
) -> Result<MemoryScope, PostgresMemoryEvolutionError> {
    let scope_id = scope_id.ok_or(PostgresMemoryEvolutionError::CorruptDatabase)?;
    match kind {
        1 => Ok(MemoryScope::User {
            principal_id: id(scope_id, "memory.scope_id")?,
        }),
        2 => Ok(MemoryScope::Tenant {
            tenant_id: id(scope_id, "memory.scope_id")?,
        }),
        3 => Ok(MemoryScope::Workspace {
            workspace_id: id(scope_id, "memory.scope_id")?,
        }),
        4 => Ok(MemoryScope::Agent {
            agent_id: id(scope_id, "memory.scope_id")?,
        }),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_memory_kind(
    value: Option<i16>,
) -> Result<MemoryKind, PostgresMemoryEvolutionError> {
    match value {
        Some(1) => Ok(MemoryKind::Preference),
        Some(2) => Ok(MemoryKind::Semantic),
        Some(3) => Ok(MemoryKind::Procedure),
        Some(4) => Ok(MemoryKind::FailureLesson),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_memory_lifecycle(
    value: i16,
) -> Result<MemoryLifecycle, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(MemoryLifecycle::Active),
        2 => Ok(MemoryLifecycle::Superseded),
        3 => Ok(MemoryLifecycle::Revoked),
        4 => Ok(MemoryLifecycle::Expired),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_memory_source(
    value: i16,
) -> Result<MemorySource, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(MemorySource::UserAuthored),
        2 => Ok(MemorySource::VerifiedExperience),
        3 => Ok(MemorySource::GovernedImport),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_classification(
    value: i16,
) -> Result<DataClassification, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(DataClassification::Public),
        2 => Ok(DataClassification::Internal),
        3 => Ok(DataClassification::Confidential),
        4 => Ok(DataClassification::Restricted),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_experience_kind(
    value: i16,
    artifact_kind: Option<i16>,
) -> Result<ExperienceKind, PostgresMemoryEvolutionError> {
    match (value, artifact_kind) {
        (1, None) => Ok(ExperienceKind::Semantic),
        (2, None) => Ok(ExperienceKind::Procedure),
        (3, None) => Ok(ExperienceKind::Failure),
        (4, None) => Ok(ExperienceKind::Preference),
        (5, None) => Ok(ExperienceKind::PolicyProposal),
        (5, Some(1)) => Ok(ExperienceKind::Skill),
        (5, Some(2)) => Ok(ExperienceKind::Instinct),
        (5, Some(3)) => Ok(ExperienceKind::PersonaPatch),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_candidate_status(
    value: i16,
) -> Result<CandidateStatus, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(CandidateStatus::Candidate),
        2 => Ok(CandidateStatus::Quarantined),
        3 => Ok(CandidateStatus::Verified),
        4 => Ok(CandidateStatus::Rejected),
        5 => Ok(CandidateStatus::Promoted),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) fn decode_candidate_origin(
    value: i16,
) -> Result<ExperienceCandidateOrigin, PostgresMemoryEvolutionError> {
    match value {
        1 => Ok(ExperienceCandidateOrigin::RuntimeTool),
        2 => Ok(ExperienceCandidateOrigin::DreamInduction),
        3 => Ok(ExperienceCandidateOrigin::UserDraft),
        _ => Err(PostgresMemoryEvolutionError::CorruptDatabase),
    }
}

pub(crate) trait FromDatabaseUuid: Sized {
    fn from_database_uuid(value: Uuid) -> Result<Self, ()>;
}

macro_rules! database_id {
    ($($type:ty),+ $(,)?) => {
        $(
            impl FromDatabaseUuid for $type {
                fn from_database_uuid(value: Uuid) -> Result<Self, ()> {
                    Self::from_uuid(value).map_err(|_| ())
                }
            }
        )+
    };
}

database_id!(
    TenantId,
    PrincipalId,
    WorkspaceId,
    AgentId,
    MemoryId,
    MemoryVersionId,
    MemoryDraftId,
    RunId,
    LedgerRecordId,
    EvolutionProposalId,
    EvolutionEvaluationId,
    EvolutionPromotionId,
    musteragent_identity::InteractionId,
    musteragent_identity::InstinctId,
    musteragent_identity::StudioRevisionDraftId,
    musteragent_identity::ContentId,
);

pub(crate) fn id<T: FromDatabaseUuid>(
    value: Uuid,
    _field: &'static str,
) -> Result<T, PostgresMemoryEvolutionError> {
    T::from_database_uuid(value).map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}

pub(crate) fn digest(value: Vec<u8>) -> Result<Sha256, PostgresMemoryEvolutionError> {
    Sha256::from_bytes(value).map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}

pub(crate) fn parse_u64(
    value: String,
    _field: &'static str,
) -> Result<u64, PostgresMemoryEvolutionError> {
    value
        .parse()
        .map_err(|_| PostgresMemoryEvolutionError::CorruptDatabase)
}
