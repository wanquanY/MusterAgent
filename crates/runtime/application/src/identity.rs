use musteragent_content_ref::Sha256;
use musteragent_identity::{
    CommandId, CorrelationId, LedgerRecordId, MessageId, OutboxMessageId, ParticipantId, RunId,
};
use musteragent_runtime_contract::CommandEnvelope;

const KERNEL_RUNTIME_IDENTITY_DOMAIN_V2: &[u8] = b"musteragent.runtime.application.v2\0";
const LEGACY_RUNTIME_IDENTITY_DOMAIN_V1: &[u8] = b"musteragent.legacy.runtime.application.v1\0";

pub(crate) type AdmissionIdentities = musteragent_runtime_admission::AdmissionIdentities;

pub(crate) fn admission_identities_for_command<T>(
    command: &CommandEnvelope<T>,
) -> Option<AdmissionIdentities> {
    admission_identities_for_command_schema(
        command.metadata.command_id,
        command.metadata.schema_version.as_str(),
    )
}

fn admission_identities_for_command_schema(
    command_id: CommandId,
    command_schema: &str,
) -> Option<AdmissionIdentities> {
    if command_schema == "musteragent.legacy.runtime.command.v1" {
        AdmissionIdentities::derive_legacy(command_id)
    } else {
        AdmissionIdentities::derive(command_id)
    }
}

pub(crate) fn conversation_participant_id(
    command_id: CommandId,
    command_schema: &str,
    role: &[u8],
) -> Option<ParticipantId> {
    ParticipantId::from_uuid(derive_for_command_schema(command_id, command_schema, role)).ok()
}

pub(crate) fn create_command_record_id<T>(
    command: &CommandEnvelope<T>,
    index: u32,
) -> Option<LedgerRecordId> {
    let mut label = b"command-record:".to_vec();
    label.extend_from_slice(&index.to_be_bytes());
    LedgerRecordId::from_uuid(derive_for_command_schema(
        command.metadata.command_id,
        command.metadata.schema_version.as_str(),
        &label,
    ))
    .ok()
}

pub(crate) fn create_command_outbox_id<T>(
    command: &CommandEnvelope<T>,
    index: u32,
) -> Option<OutboxMessageId> {
    let mut label = b"command-outbox:".to_vec();
    label.extend_from_slice(&index.to_be_bytes());
    OutboxMessageId::from_uuid(derive_for_command_schema(
        command.metadata.command_id,
        command.metadata.schema_version.as_str(),
        &label,
    ))
    .ok()
}

/// Deterministic identities of one standing-Goal continuation or judge Run.
/// The target command identity is chosen by the supervisor, so a retried
/// admission always names the same Run, Activity and records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StandingGoalIdentities {
    pub command_id: CommandId,
    pub correlation_id: CorrelationId,
    pub synthetic_message_id: MessageId,
    pub admission: AdmissionIdentities,
}

impl StandingGoalIdentities {
    pub(crate) fn derive(command_id: CommandId) -> Option<Self> {
        Some(Self {
            command_id,
            correlation_id: CorrelationId::from_uuid(derive(
                command_id,
                b"standing-goal-correlation",
            ))
            .ok()?,
            synthetic_message_id: MessageId::from_uuid(derive(
                command_id,
                b"standing-goal-snapshot-input",
            ))
            .ok()?,
            admission: AdmissionIdentities::derive(command_id)?,
        })
    }
}

/// Derive the stable Run identity shared by hosts and admission persistence.
pub fn derive_admission_run_id(
    command_id: CommandId,
) -> Result<RunId, musteragent_runtime_contract::ErrorCode> {
    musteragent_runtime_admission::derive_admission_run_id(command_id)
        .ok_or(musteragent_runtime_contract::ErrorCode::InternalInvariantViolated)
}

fn derive(command_id: CommandId, label: &[u8]) -> uuid::Uuid {
    derive_in_domain(KERNEL_RUNTIME_IDENTITY_DOMAIN_V2, command_id, label)
}

fn derive_for_command_schema(
    command_id: CommandId,
    command_schema: &str,
    label: &[u8],
) -> uuid::Uuid {
    let domain = if command_schema == "musteragent.legacy.runtime.command.v1" {
        LEGACY_RUNTIME_IDENTITY_DOMAIN_V1
    } else {
        KERNEL_RUNTIME_IDENTITY_DOMAIN_V2
    };
    derive_in_domain(domain, command_id, label)
}

fn derive_in_domain(domain: &[u8], command_id: CommandId, label: &[u8]) -> uuid::Uuid {
    let mut seed = domain.to_vec();
    seed.extend_from_slice(command_id.into_uuid().as_bytes());
    seed.extend_from_slice(label);
    let digest = Sha256::digest(seed).into_bytes();
    let mut value = [0_u8; 16];
    value.copy_from_slice(&digest[..16]);
    value[6] = (value[6] & 0x0f) | 0x50;
    value[8] = (value[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(value)
}

#[cfg(test)]
mod tests {
    use musteragent_identity::CommandId;
    use musteragent_runtime_contract::COMMAND_SCHEMA_VERSION;

    use super::{
        admission_identities_for_command_schema, derive_admission_run_id, derive_for_command_schema,
    };

    #[test]
    fn admission_run_id_matches_the_cross_language_control_plane_vector() {
        let command_id = CommandId::from_uuid(
            uuid::Uuid::parse_str("018f4f70-814a-7a42-8a84-1258cf7419b1").expect("UUID"),
        )
        .expect("command ID");
        assert_eq!(
            derive_admission_run_id(command_id)
                .expect("Run ID")
                .to_string(),
            "bf895e33-dd00-5756-8830-6682b860396a"
        );
    }

    #[test]
    fn command_schema_versions_select_distinct_frozen_identity_domains() {
        let command_id = CommandId::from_uuid(
            uuid::Uuid::parse_str("018f4f70-814a-7a42-8a84-1258cf7419b1").expect("UUID"),
        )
        .expect("command ID");
        let label = b"command-record:\0\0\0\0";

        assert_eq!(
            derive_for_command_schema(command_id, COMMAND_SCHEMA_VERSION, label).to_string(),
            "fdaf6ec8-7c24-5571-a7be-7be054ef9c8e"
        );
        assert_eq!(
            derive_for_command_schema(command_id, "musteragent.legacy.runtime.command.v1", label)
                .to_string(),
            "61ac8311-4637-5381-830e-2a18ef5669e4"
        );
        assert_eq!(
            admission_identities_for_command_schema(command_id, COMMAND_SCHEMA_VERSION)
                .expect("current admission")
                .run_id
                .to_string(),
            "bf895e33-dd00-5756-8830-6682b860396a"
        );
        assert_eq!(
            admission_identities_for_command_schema(
                command_id,
                "musteragent.legacy.runtime.command.v1",
            )
            .expect("legacy admission")
            .run_id
            .to_string(),
            "e53960b3-96a0-5354-94dc-62cea3831dc4"
        );
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct CoordinationIdentities {
    pub command_id: CommandId,
    pub correlation_id: CorrelationId,
    pub synthetic_message_id: MessageId,
    pub admission: AdmissionIdentities,
}

impl CoordinationIdentities {
    pub(crate) fn derive(command_id: CommandId) -> Option<Self> {
        Some(Self {
            command_id,
            correlation_id: CorrelationId::from_uuid(derive(
                command_id,
                b"coordination-correlation",
            ))
            .ok()?,
            synthetic_message_id: MessageId::from_uuid(derive(
                command_id,
                b"coordination-snapshot-input",
            ))
            .ok()?,
            admission: AdmissionIdentities::derive(command_id)?,
        })
    }
}
