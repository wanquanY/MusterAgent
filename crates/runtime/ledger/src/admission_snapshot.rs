//! Read-only persistence view shared by every storage adapter. It preserves
//! distinct interactive and Dream maintenance snapshot contracts.
use crate::{MaintenanceRuntimeSnapshotBlob, RuntimeSnapshotBlob};

#[derive(Copy, Clone)]
pub enum AdmissionSnapshotRef<'a> {
    Interactive(&'a RuntimeSnapshotBlob),
    Maintenance(&'a MaintenanceRuntimeSnapshotBlob),
}

impl<'a> AdmissionSnapshotRef<'a> {
    pub fn runtime_snapshot_id(self) -> musteragent_identity::RuntimeSnapshotId {
        match self {
            Self::Interactive(snapshot) => snapshot.runtime_snapshot_id,
            Self::Maintenance(snapshot) => snapshot.runtime_snapshot_id,
        }
    }

    pub fn schema_version(self) -> &'a musteragent_identity::DottedKey {
        match self {
            Self::Interactive(snapshot) => &snapshot.schema_version,
            Self::Maintenance(snapshot) => &snapshot.snapshot.content().schema_version,
        }
    }

    pub fn encoded_snapshot(self) -> &'a [u8] {
        match self {
            Self::Interactive(snapshot) => &snapshot.encoded_snapshot,
            Self::Maintenance(snapshot) => snapshot.snapshot.encoded_content(),
        }
    }

    pub fn content_hash(self) -> musteragent_content_ref::Sha256 {
        match self {
            Self::Interactive(snapshot) => snapshot.content_hash,
            Self::Maintenance(snapshot) => snapshot.snapshot.content_hash(),
        }
    }

    pub fn created_at(self) -> musteragent_identity::UtcTimestamp {
        match self {
            Self::Interactive(snapshot) => snapshot.created_at,
            Self::Maintenance(snapshot) => snapshot.snapshot.created_at(),
        }
    }

    pub fn execution_grant(self) -> &'a musteragent_execution_grant::ExecutionGrant {
        match self {
            Self::Interactive(snapshot) => &snapshot.execution_grant,
            Self::Maintenance(snapshot) => &snapshot.execution_grant,
        }
    }

    pub fn execution_grant_proof(
        self,
    ) -> Option<&'a musteragent_execution_grant::EncodedExecutionGrantProof> {
        match self {
            Self::Interactive(snapshot) => snapshot.execution_grant_proof.as_ref(),
            Self::Maintenance(snapshot) => snapshot.execution_grant_proof.as_ref(),
        }
    }

    pub fn memory_agent_id(self) -> Option<musteragent_identity::AgentId> {
        match self {
            Self::Interactive(snapshot) => Some(snapshot.memory_agent_id),
            Self::Maintenance(snapshot) => Some(snapshot.memory_agent_id),
        }
    }

    pub fn memory_selection(self) -> Option<&'a musteragent_memory::MemorySelectionSnapshot> {
        match self {
            Self::Interactive(snapshot) => Some(&snapshot.memory_selection),
            Self::Maintenance(_) => None,
        }
    }

    pub fn instinct_selections(
        self,
    ) -> Option<&'a [musteragent_runtime_snapshot::InstinctSelectionSnapshot]> {
        match self {
            Self::Interactive(snapshot) => Some(&snapshot.instinct_selections),
            Self::Maintenance(_) => None,
        }
    }

    pub fn primary_route_role(self) -> u8 {
        match self {
            Self::Maintenance(snapshot)
                if snapshot.execution_grant.schema_version.as_str()
                    == musteragent_execution_grant::EVOLUTION_MAINTENANCE_GRANT_SCHEMA_V1 =>
            {
                musteragent_model::AuxiliaryModelRole::Evolution.canonical_code()
            }
            Self::Interactive(_) | Self::Maintenance(_) => 0,
        }
    }
}
