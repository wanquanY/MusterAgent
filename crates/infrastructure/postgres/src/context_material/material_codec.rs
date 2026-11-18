use super::*;

pub(super) fn canonical_role(value: MessageRole) -> (CanonicalRole, bool) {
    match value {
        MessageRole::User => (CanonicalRole::User, false),
        MessageRole::Assistant => (CanonicalRole::Assistant, false),
        MessageRole::SystemNotice => (CanonicalRole::User, true),
    }
}

pub(super) fn activity_kind(value: i16) -> Result<ActivityKind, PostgresContextMaterialError> {
    match value {
        1 => Ok(ActivityKind::Chat),
        2 => Ok(ActivityKind::Coordination),
        3 => Ok(ActivityKind::Automation),
        4 => Ok(ActivityKind::Delegation),
        5 => Ok(ActivityKind::Maintenance),
        6 => Ok(ActivityKind::Evaluation),
        _ => Err(PostgresContextMaterialError::CorruptState),
    }
}

pub(super) fn execution_scope(
    kind: i16,
    tenant_id: TenantId,
    workspace_id: Option<uuid::Uuid>,
    device_id: Option<uuid::Uuid>,
) -> Result<ExecutionScope, PostgresContextMaterialError> {
    match (kind, workspace_id, device_id) {
        (1, None, None) => Ok(ExecutionScope::Tenant { tenant_id }),
        (2, Some(workspace), None) => Ok(ExecutionScope::Workspace {
            workspace_id: WorkspaceId::from_uuid(workspace)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        }),
        (3, workspace, Some(device)) => Ok(ExecutionScope::DevicePrivate {
            device_id: DeviceId::from_uuid(device)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            workspace_id: workspace
                .map(WorkspaceId::from_uuid)
                .transpose()
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        }),
        _ => Err(PostgresContextMaterialError::CorruptState),
    }
}

pub(super) fn runtime_home(
    kind: i16,
    device_id: Option<uuid::Uuid>,
) -> Result<RuntimeHome, PostgresContextMaterialError> {
    match (kind, device_id) {
        (1, None) => Ok(RuntimeHome::Cloud),
        (2, Some(device)) => Ok(RuntimeHome::Device {
            device_id: DeviceId::from_uuid(device)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        }),
        _ => Err(PostgresContextMaterialError::CorruptState),
    }
}

pub(super) fn content_ref_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ContentRef, PostgresContextMaterialError> {
    content_ref_from_columns(
        row.try_get("content_id")?,
        row.try_get("media_type")?,
        row.try_get("byte_length")?,
        row.try_get("content_hash")?,
    )
}

pub(super) fn content_ref_from_columns(
    content_id: uuid::Uuid,
    media_type: String,
    byte_length: String,
    content_hash: Vec<u8>,
) -> Result<ContentRef, PostgresContextMaterialError> {
    Ok(ContentRef {
        content_id: id(content_id)?,
        media_type: MediaType::new(media_type)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        byte_length: parse_u64(byte_length)?,
        sha256: Sha256::from_bytes(content_hash)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
    })
}

pub(super) fn content_ref_from_optional_columns(
    content_id: Option<uuid::Uuid>,
    media_type: Option<String>,
    byte_length: Option<String>,
    content_hash: Option<Vec<u8>>,
) -> Result<Option<ContentRef>, PostgresContextMaterialError> {
    match (content_id, media_type, byte_length, content_hash) {
        (None, None, None, None) => Ok(None),
        (Some(id), Some(media), Some(length), Some(hash)) => {
            content_ref_from_columns(id, media, length, hash).map(Some)
        }
        _ => Err(PostgresContextMaterialError::CorruptState),
    }
}

pub(super) fn serde_to_json(
    value: serde_json::Value,
) -> Result<JsonValue, PostgresContextMaterialError> {
    match value {
        serde_json::Value::Null => Ok(JsonValue::Null),
        serde_json::Value::Bool(value) => Ok(JsonValue::Bool(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(JsonValue::Integer(value))
            } else {
                value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .map(JsonValue::Number)
                    .ok_or(PostgresContextMaterialError::CorruptState)
            }
        }
        serde_json::Value::String(value) => Ok(JsonValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(serde_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, serde_to_json(value)?)))
            .collect::<Result<BTreeMap<_, _>, PostgresContextMaterialError>>()
            .map(JsonValue::Object),
    }
}

pub(super) fn ensure_entry_provenance(
    entries: &[ContextEntry],
) -> Result<(), PostgresContextMaterialError> {
    if entries.is_empty() {
        return Err(PostgresContextMaterialError::NoModelInput);
    }
    let mut records = BTreeSet::new();
    for entry in entries {
        entry
            .validate_messages()
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        if entry.source_record_ids.is_empty()
            || entry
                .source_record_ids
                .iter()
                .any(|record_id| !records.insert(*record_id))
        {
            return Err(PostgresContextMaterialError::CorruptState);
        }
    }
    Ok(())
}

pub(super) fn id<T: DatabaseId>(value: uuid::Uuid) -> Result<T, PostgresContextMaterialError> {
    T::from_database(value).map_err(|_| PostgresContextMaterialError::CorruptState)
}

pub(super) fn optional_id<T: DatabaseId>(
    value: Option<uuid::Uuid>,
) -> Result<Option<T>, PostgresContextMaterialError> {
    value.map(id).transpose()
}

pub(super) trait DatabaseId: Sized {
    fn from_database(value: uuid::Uuid) -> Result<Self, ()>;
}

macro_rules! database_ids {
    ($($type:ty),+ $(,)?) => {
        $(
            impl DatabaseId for $type {
                fn from_database(value: uuid::Uuid) -> Result<Self, ()> {
                    Self::from_uuid(value).map_err(|_| ())
                }
            }
        )+
    };
}

database_ids!(
    ActivityId,
    ContentId,
    ConversationId,
    DelegationId,
    InvocationId,
    LedgerRecordId,
    MemoryId,
    MemoryVersionId,
    MessageId,
    ParticipantId,
    RunId,
    RuntimeSnapshotId,
);

pub(super) fn parse_u64(value: String) -> Result<u64, PostgresContextMaterialError> {
    value
        .parse()
        .map_err(|_| PostgresContextMaterialError::CorruptState)
}
