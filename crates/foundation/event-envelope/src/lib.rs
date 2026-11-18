//! Shared Ledger identity, home, scope and cursor contracts.

#![forbid(unsafe_code)]

use musteragent_content_ref::Sha256;
use musteragent_identity::{
    ActivityId, AttemptId, ConversationId, ConversationSeq, CorrelationId, DeviceId, DottedKey,
    HomeBindingId, HomeEpoch, IdempotencyKey, LedgerOffset, LedgerRecordId, ParticipantId,
    PrincipalId, ProjectId, RunId, RunSeq, TenantId, UtcTimestamp, WorkspaceId,
};
use thiserror::Error;

/// Current immutable Runtime Ledger record contract.
pub const RECORD_SCHEMA_VERSION: &str = "musteragent.runtime.record.v1";

/// Pre-rename name of the same record contract. The wire format is unchanged;
/// only the protobuf package was renamed, which protobuf bytes never carry.
pub const LEGACY_RECORD_SCHEMA_VERSION: &str = "musteragent.legacy.runtime.record.v1";

/// The Control or Execution home for a Conversation or Run.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RuntimeHome {
    /// Cloud-owned execution.
    Cloud,
    /// Device-owned execution.
    Device { device_id: DeviceId },
}

/// Cloud-issued immutable identity of a hosted project's sole Device writer.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct HostedHomeBinding {
    pub binding_id: HomeBindingId,
    pub device_id: DeviceId,
    pub home_epoch: HomeEpoch,
}

/// Shared scope used by Ledger, policy and authority proofs. Requester and payer
/// are separate identities and cannot be inferred from the resource tenant.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct HostedProjectScope {
    pub project_id: ProjectId,
    pub workspace_id: WorkspaceId,
    pub resource_tenant_id: TenantId,
    pub owner_principal_id: PrincipalId,
    pub home: HostedHomeBinding,
}

impl HostedProjectScope {
    /// Fixed v1 identity body: project/workspace/resource tenant/Owner/binding/
    /// device UUID bytes, then an unsigned big-endian Home epoch. This is NOT
    /// the COSE scope-digest representation, which has its own CBOR domain.
    #[must_use]
    pub fn identity_bytes(self) -> [u8; 104] {
        let mut bytes = [0; 104];
        for (index, id) in [
            self.project_id.into_uuid(),
            self.workspace_id.into_uuid(),
            self.resource_tenant_id.into_uuid(),
            self.owner_principal_id.into_uuid(),
            self.home.binding_id.into_uuid(),
            self.home.device_id.into_uuid(),
        ]
        .into_iter()
        .enumerate()
        {
            bytes[index * 16..(index + 1) * 16].copy_from_slice(id.as_bytes());
        }
        bytes[96..].copy_from_slice(&self.home.home_epoch.get().to_be_bytes());
        bytes
    }

    pub fn from_identity_bytes(bytes: &[u8]) -> Result<Self, HostedScopeEncodingError> {
        if bytes.len() != 104 {
            return Err(HostedScopeEncodingError);
        }
        let id = |offset| {
            uuid::Uuid::from_slice(&bytes[offset..offset + 16])
                .map_err(|_| HostedScopeEncodingError)
        };
        Ok(Self {
            project_id: ProjectId::from_uuid(id(0)?).map_err(|_| HostedScopeEncodingError)?,
            workspace_id: WorkspaceId::from_uuid(id(16)?).map_err(|_| HostedScopeEncodingError)?,
            resource_tenant_id: TenantId::from_uuid(id(32)?)
                .map_err(|_| HostedScopeEncodingError)?,
            owner_principal_id: PrincipalId::from_uuid(id(48)?)
                .map_err(|_| HostedScopeEncodingError)?,
            home: HostedHomeBinding {
                binding_id: HomeBindingId::from_uuid(id(64)?)
                    .map_err(|_| HostedScopeEncodingError)?,
                device_id: DeviceId::from_uuid(id(80)?).map_err(|_| HostedScopeEncodingError)?,
                home_epoch: HomeEpoch::new(u64::from_be_bytes(
                    bytes[96..]
                        .try_into()
                        .map_err(|_| HostedScopeEncodingError)?,
                ))
                .map_err(|_| HostedScopeEncodingError)?,
            },
        })
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
#[error("invalid hosted project identity bytes")]
pub struct HostedScopeEncodingError;

/// A structured capability and isolation scope.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ExecutionScope {
    /// Tenant-wide cloud scope.
    Tenant { tenant_id: TenantId },
    /// An explicit workspace.
    Workspace { workspace_id: WorkspaceId },
    /// Private state on a device, optionally constrained to a workspace.
    DevicePrivate {
        device_id: DeviceId,
        workspace_id: Option<WorkspaceId>,
    },
    /// Project-owned shared state on one Home; never a tenant/private fallback.
    HostedProject { scope: HostedProjectScope },
}

impl ExecutionScope {
    /// Stable scope identity used by command fingerprints. Existing 1/2/3
    /// encodings are unchanged; shared project scope has discriminator 4.
    pub fn append_identity_bytes(self, bytes: &mut Vec<u8>) {
        match self {
            Self::Tenant { tenant_id } => {
                bytes.push(1);
                bytes.extend_from_slice(tenant_id.into_uuid().as_bytes());
            }
            Self::Workspace { workspace_id } => {
                bytes.push(2);
                bytes.extend_from_slice(workspace_id.into_uuid().as_bytes());
            }
            Self::DevicePrivate {
                device_id,
                workspace_id,
            } => {
                bytes.push(3);
                bytes.extend_from_slice(device_id.into_uuid().as_bytes());
                match workspace_id {
                    Some(id) => {
                        bytes.push(1);
                        bytes.extend_from_slice(id.into_uuid().as_bytes());
                    }
                    None => bytes.push(0),
                }
            }
            Self::HostedProject { scope } => {
                bytes.push(4);
                bytes.extend_from_slice(&scope.identity_bytes());
            }
        }
    }
}

/// The two record kinds sharing one Conversation sequence.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum LedgerRecordKind {
    /// A durable domain fact.
    DomainEvent,
    /// A durable coalesced stream frame.
    OutputFrame,
}

/// A producer allowed to append through the canonical commit protocol.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Producer {
    RuntimeApi,
    Worker,
    Scheduler,
    Projector,
    Importer,
}

/// Resume cursor for a single Conversation home epoch.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct LedgerCursor {
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub after_conversation_seq: LedgerOffset,
}

/// Stable metadata shared by Domain Events and Output Frames.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct LedgerMetadata {
    pub schema_version: DottedKey,
    pub record_id: LedgerRecordId,
    pub record_kind: LedgerRecordKind,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub conversation_seq: ConversationSeq,
    pub activity_id: Option<ActivityId>,
    pub run_id: Option<RunId>,
    pub run_seq: Option<RunSeq>,
    pub attempt_id: Option<AttemptId>,
    pub participant_id: Option<ParticipantId>,
    pub home_epoch: HomeEpoch,
    pub producer: Producer,
    pub occurred_at: UtcTimestamp,
    pub committed_at: UtcTimestamp,
    pub causation_id: Option<LedgerRecordId>,
    pub correlation_id: CorrelationId,
    pub idempotency_key: IdempotencyKey,
    pub payload_hash: Sha256,
}

/// A Ledger record was internally inconsistent.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum LedgerContractError {
    #[error("run_seq and attempt_id require run_id")]
    RunIdentityMissing,
    #[error("attempt_id requires run_seq")]
    RunSequenceMissing,
}

impl LedgerMetadata {
    /// Validates cross-field identity requirements.
    pub fn validate(&self) -> Result<(), LedgerContractError> {
        if self.run_id.is_none() && (self.run_seq.is_some() || self.attempt_id.is_some()) {
            return Err(LedgerContractError::RunIdentityMissing);
        }
        if self.attempt_id.is_some() && self.run_seq.is_none() {
            return Err(LedgerContractError::RunSequenceMissing);
        }
        Ok(())
    }
}

#[cfg(test)]
mod hosted_scope_tests {
    use super::*;

    fn scope() -> HostedProjectScope {
        HostedProjectScope {
            project_id: ProjectId::from_uuid(uuid::Uuid::from_u128(1))
                .expect("valid hosted scope test fixture"),
            workspace_id: WorkspaceId::from_uuid(uuid::Uuid::from_u128(2))
                .expect("valid hosted scope test fixture"),
            resource_tenant_id: TenantId::from_uuid(uuid::Uuid::from_u128(3))
                .expect("valid hosted scope test fixture"),
            owner_principal_id: PrincipalId::from_uuid(uuid::Uuid::from_u128(4))
                .expect("valid hosted scope test fixture"),
            home: HostedHomeBinding {
                binding_id: HomeBindingId::from_uuid(uuid::Uuid::from_u128(5))
                    .expect("valid hosted scope test fixture"),
                device_id: DeviceId::from_uuid(uuid::Uuid::from_u128(6))
                    .expect("valid hosted scope test fixture"),
                home_epoch: HomeEpoch::new(0x0102030405060708)
                    .expect("valid hosted scope test fixture"),
            },
        }
    }

    #[test]
    fn hosted_identity_has_fixed_field_order_and_network_byte_order() {
        let scope = scope();
        let mut expected = Vec::new();
        for value in 1_u128..=6 {
            expected.extend_from_slice(&value.to_be_bytes());
        }
        expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(scope.identity_bytes().as_slice(), expected);
        assert_eq!(
            HostedProjectScope::from_identity_bytes(&expected)
                .expect("valid hosted scope test fixture"),
            scope
        );
        let mut fingerprint = Vec::new();
        ExecutionScope::HostedProject { scope }.append_identity_bytes(&mut fingerprint);
        assert_eq!(fingerprint[0], 4);
        assert_eq!(&fingerprint[1..], expected);
    }

    #[test]
    fn rejects_missing_nil_trailing_and_zero_epoch_identity() {
        let bytes = scope().identity_bytes();
        for len in 0..104 {
            assert!(HostedProjectScope::from_identity_bytes(&bytes[..len]).is_err());
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(HostedProjectScope::from_identity_bytes(&trailing).is_err());
        for offset in (0..96).step_by(16) {
            let mut nil = bytes;
            nil[offset..offset + 16].fill(0);
            assert!(HostedProjectScope::from_identity_bytes(&nil).is_err());
        }
        let mut zero_epoch = bytes;
        zero_epoch[96..].fill(0);
        assert!(HostedProjectScope::from_identity_bytes(&zero_epoch).is_err());
    }

    #[test]
    fn legacy_scope_fingerprints_are_unchanged() {
        let scope = scope();
        let cases = [
            (
                ExecutionScope::Tenant {
                    tenant_id: scope.resource_tenant_id,
                },
                1,
                3_u128,
                None,
            ),
            (
                ExecutionScope::Workspace {
                    workspace_id: scope.workspace_id,
                },
                2,
                2,
                None,
            ),
            (
                ExecutionScope::DevicePrivate {
                    device_id: scope.home.device_id,
                    workspace_id: None,
                },
                3,
                6,
                Some(None),
            ),
            (
                ExecutionScope::DevicePrivate {
                    device_id: scope.home.device_id,
                    workspace_id: Some(scope.workspace_id),
                },
                3,
                6,
                Some(Some(2_u128)),
            ),
        ];
        for (scope, kind, id, workspace) in cases {
            let mut expected = vec![kind];
            expected.extend_from_slice(&id.to_be_bytes());
            if let Some(workspace) = workspace {
                expected.push(u8::from(workspace.is_some()));
                if let Some(workspace) = workspace {
                    expected.extend_from_slice(&workspace.to_be_bytes());
                }
            }
            let mut actual = Vec::new();
            scope.append_identity_bytes(&mut actual);
            assert_eq!(actual, expected);
        }
    }
}
