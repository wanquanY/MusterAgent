//! The private Agent policy comes from its sealed original Snapshot. This is
//! decoding only: it never substitutes current defaults for historical bytes.
use crate::{ProtobufMaintenanceSnapshotCodec, SnapshotCodecError};
use musteragent_runtime_memory::MaintenanceSnapshotCodec;
use musteragent_runtime_snapshot::RunExecutionPolicySnapshot;
pub fn decode_private_run_execution_policy(
    schema: &str,
    bytes: &[u8],
) -> Result<Option<RunExecutionPolicySnapshot>, SnapshotCodecError> {
    let policy = match schema {
        musteragent_runtime_memory::MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION => {
            let content = ProtobufMaintenanceSnapshotCodec.decode_content(bytes)?;
            if content.schema_version.as_str() != schema {
                return Err(SnapshotCodecError::InvalidContract);
            }
            content
                .execution_policy
                .ok_or(SnapshotCodecError::MissingField)?
        }

        _ => return Ok(None),
    };
    policy
        .validate()
        .map_err(|_| SnapshotCodecError::InvalidContract)?;
    if policy.liveness.is_none() {
        return Err(SnapshotCodecError::InvalidContract);
    }
    Ok(Some(policy))
}

pub fn decode_private_tool_policy(
    schema: &str,
    bytes: &[u8],
    key: &str,
    evaluation_run: bool,
    ordinary: &dyn musteragent_context::RuntimeSnapshotDecoder,
) -> Result<Option<musteragent_runtime_snapshot::PrivateToolPolicy>, SnapshotCodecError> {
    use musteragent_runtime_snapshot::PrivateToolPolicy;
    let private = match schema {
        musteragent_runtime_memory::MAINTENANCE_RUNTIME_SNAPSHOT_SCHEMA_VERSION => {
            let content = ProtobufMaintenanceSnapshotCodec.decode_content(bytes)?;
            if content.schema_version.as_str() != schema || content.execution_policy.is_none() {
                return Err(SnapshotCodecError::InvalidContract);
            }
            PrivateToolPolicy::dream(key).map(|policy| {
                policy.with_minimum_window(
                    content
                        .execution_policy
                        .expect("checked policy")
                        .private_tool_minimum_window,
                )
            })
        }

        _ => {
            let snapshot = ordinary
                .decode_runtime_snapshot(bytes)
                .map_err(|_| SnapshotCodecError::InvalidContract)?;
            snapshot
                .validate_liveness_schema()
                .map_err(|_| SnapshotCodecError::InvalidContract)?;
            if snapshot.execution_policy.liveness.is_none() {
                return Err(SnapshotCodecError::InvalidContract);
            }
            // The original locked Run owns the role. Current evaluators retain
            // ordinary tools and Skills; a closed legacy discovery profile is
            // neither required nor sufficient to authorize their protocol tools.
            if evaluation_run {
                PrivateToolPolicy::evaluation(key).map(|policy| {
                    policy
                        .with_minimum_window(snapshot.execution_policy.private_tool_minimum_window)
                })
            } else {
                None
            }
        }
    };
    Ok(private)
}
