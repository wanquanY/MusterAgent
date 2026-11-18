//! Deterministic identities shared by every Runtime Run-admission entrypoint.

#![forbid(unsafe_code)]

use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ActivityId, CommandId, CorrelationId, LedgerRecordId, OutboxMessageId, RunId, RuntimeSnapshotId,
};

const MUSTERAGENT_ADMISSION_IDENTITY_DOMAIN_V2: &[u8] = b"musteragent.runtime.application.v2\0";
const LEGACY_ADMISSION_IDENTITY_DOMAIN_V1: &[u8] = b"musteragent.legacy.runtime.application.v1\0";

/// Standard deterministic identities for one Run admission.
///
/// The byte domain is mirrored by the Backend grant signer. Every entrypoint,
/// including autonomous Maintenance, must use this contract so a signed
/// `run_id` can never diverge from the Activity/Snapshot/Ledger identities
/// committed by Runtime.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AdmissionIdentities {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub runtime_snapshot_id: RuntimeSnapshotId,
    /// Visible Message record for message-backed admission, or the private
    /// Activity context record for a message-free admission.
    pub message_record_id: LedgerRecordId,
    pub title_record_id: LedgerRecordId,
    pub activity_record_id: LedgerRecordId,
    pub run_record_id: LedgerRecordId,
    pub dispatch_id: OutboxMessageId,
}

impl AdmissionIdentities {
    /// Derives identities for newly admitted Musteragent Runs.
    #[must_use]
    pub fn derive(command_id: CommandId) -> Option<Self> {
        Self::derive_in_domain(command_id, MUSTERAGENT_ADMISSION_IDENTITY_DOMAIN_V2)
    }

    /// Reproduces the frozen identity set used by persisted Legacy assistant admissions.
    #[must_use]
    pub fn derive_legacy(command_id: CommandId) -> Option<Self> {
        Self::derive_in_domain(command_id, LEGACY_ADMISSION_IDENTITY_DOMAIN_V1)
    }

    fn derive_in_domain(command_id: CommandId, domain: &[u8]) -> Option<Self> {
        Some(Self {
            activity_id: ActivityId::from_uuid(derive(domain, command_id, b"activity")).ok()?,
            run_id: RunId::from_uuid(derive(domain, command_id, b"run")).ok()?,
            runtime_snapshot_id: RuntimeSnapshotId::from_uuid(derive(
                domain,
                command_id,
                b"snapshot",
            ))
            .ok()?,
            message_record_id: LedgerRecordId::from_uuid(derive(
                domain,
                command_id,
                b"message-record",
            ))
            .ok()?,
            title_record_id: LedgerRecordId::from_uuid(derive(domain, command_id, b"title-record"))
                .ok()?,
            activity_record_id: LedgerRecordId::from_uuid(derive(
                domain,
                command_id,
                b"activity-record",
            ))
            .ok()?,
            run_record_id: LedgerRecordId::from_uuid(derive(domain, command_id, b"run-record"))
                .ok()?,
            dispatch_id: OutboxMessageId::from_uuid(derive(domain, command_id, b"dispatch"))
                .ok()?,
        })
    }
}

#[must_use]
pub fn derive_admission_run_id(command_id: CommandId) -> Option<RunId> {
    AdmissionIdentities::derive(command_id).map(|identities| identities.run_id)
}

/// Reproduces the Run identity carried by a frozen Legacy assistant admission.
#[must_use]
pub fn derive_legacy_admission_run_id(command_id: CommandId) -> Option<RunId> {
    AdmissionIdentities::derive_legacy(command_id).map(|identities| identities.run_id)
}

/// Stable correlation identity for a message-free system admission.
/// Interactive commands keep their caller-supplied correlation ID.
#[must_use]
pub fn derive_system_admission_correlation_id(command_id: CommandId) -> Option<CorrelationId> {
    CorrelationId::from_uuid(derive(
        MUSTERAGENT_ADMISSION_IDENTITY_DOMAIN_V2,
        command_id,
        b"system-admission-correlation",
    ))
    .ok()
}

/// Reproduces the correlation identity carried by a frozen Legacy assistant admission.
#[must_use]
pub fn derive_legacy_system_admission_correlation_id(
    command_id: CommandId,
) -> Option<CorrelationId> {
    CorrelationId::from_uuid(derive(
        LEGACY_ADMISSION_IDENTITY_DOMAIN_V1,
        command_id,
        b"system-admission-correlation",
    ))
    .ok()
}

/// One private lane per immutable presentation request. The executor is a
/// built-in service agent, not a user AgentDefinition or a Dream memory owner.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PresentationAdmissionLane {
    pub conversation_id: musteragent_identity::ConversationId,
    pub scheduler_participant_id: musteragent_identity::ParticipantId,
    pub executor_participant_id: musteragent_identity::ParticipantId,
}

impl PresentationAdmissionLane {
    #[must_use]
    pub fn derive(command_id: CommandId) -> Option<Self> {
        const DOMAIN: &[u8] = b"musteragent.presentation.private-admission.v1\0";
        Some(Self {
            conversation_id: musteragent_identity::ConversationId::from_uuid(derive(
                DOMAIN,
                command_id,
                b"conversation",
            ))
            .ok()?,
            scheduler_participant_id: musteragent_identity::ParticipantId::from_uuid(derive(
                DOMAIN,
                command_id,
                b"scheduler",
            ))
            .ok()?,
            executor_participant_id: musteragent_identity::ParticipantId::from_uuid(derive(
                DOMAIN,
                command_id,
                b"executor",
            ))
            .ok()?,
        })
    }
}

fn derive(domain: &[u8], command_id: CommandId, label: &[u8]) -> uuid::Uuid {
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

    use super::{
        AdmissionIdentities, derive_legacy_system_admission_correlation_id,
        derive_system_admission_correlation_id,
    };

    #[test]
    fn current_and_legacy_admission_identities_match_backend_known_answers() {
        let command_id = CommandId::from_uuid(
            uuid::Uuid::parse_str("018f4f70-814a-7a42-8a84-1258cf7419b1").expect("UUID"),
        )
        .expect("Command ID");
        assert_eq!(
            AdmissionIdentities::derive(command_id)
                .expect("Admission identities")
                .run_id
                .to_string(),
            "bf895e33-dd00-5756-8830-6682b860396a"
        );
        assert_eq!(
            AdmissionIdentities::derive_legacy(command_id)
                .expect("legacy admission identities")
                .run_id
                .to_string(),
            "e53960b3-96a0-5354-94dc-62cea3831dc4"
        );
        assert_eq!(
            derive_system_admission_correlation_id(command_id)
                .expect("current correlation")
                .to_string(),
            "7ff7a8a2-7225-59b4-959a-7106b796e7b2"
        );
        assert_eq!(
            derive_legacy_system_admission_correlation_id(command_id)
                .expect("legacy correlation")
                .to_string(),
            "004a80ac-18e8-5144-930d-34a283b60f73"
        );
    }
}
