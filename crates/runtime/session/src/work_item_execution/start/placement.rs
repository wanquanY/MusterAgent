//! Model-facing placement selects a class of target, never a host proof.
use super::*;

pub(super) use musteragent_placement::{
    CoordinatorArguments, PlacementArguments as LocationArguments,
};
pub use musteragent_placement::{WorkItemCoordinatorDelivery, WorkItemStartPlacement};

/// Bind two previously authenticated authorities. Original expiry is not renewed:
/// this fresh action needs the same original host subject and a live transport.
/// The private data installation is independently bound at its admission row.
pub fn work_item_origin_host(
    actor: Actor,
    accepted_proof: &[u8],
    current: &crate::OriginDeviceExecutionTarget,
    now: UtcTimestamp,
) -> Result<crate::DeviceHostAuthority, DialogueStoreError> {
    current.require_actor(actor)?;
    current.require_current_at(now)?;
    let parse = |bytes: &[u8]| {
        crate::DeviceHostAuthority::from_trusted_account_transport(actor, bytes)
            .map_err(|_| DialogueStoreError::Unavailable)
    };
    let accepted = parse(accepted_proof)?;
    let live = parse(current.membership_authority())?;
    if accepted.destination() != live.destination()
        || accepted.subject() != live.subject()
        || live.destination().device_id != current.device_id()
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    Ok(live)
}
