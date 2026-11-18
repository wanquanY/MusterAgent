//! Execution placement contract.
//!
//! A work item is started in exactly one place: the Cloud worker pool, the
//! device that submitted it, or a specific paired device. Placement selects a
//! *class* of target; it never carries host proof. Hosts authenticate the
//! selected target separately before they accept the assignment.
#![forbid(unsafe_code)]

use musteragent_identity::CommandId;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum PlacementError {
    #[error("device selector identity or epoch is invalid")]
    InvalidDeviceSelector,
    #[error("coordinator delivery list is empty, too long or contains duplicates")]
    InvalidCoordinatorDelivery,
}

/// A paired device chosen by its membership and the membership epoch the
/// caller observed. A stale epoch must be refused by the placement owner.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DeviceSelector {
    pub membership_id: uuid::Uuid,
    pub expected_membership_epoch: u64,
}

impl DeviceSelector {
    pub fn validate(&self) -> Result<(), PlacementError> {
        if self.membership_id.is_nil()
            || self.expected_membership_epoch == 0
            || self.expected_membership_epoch > i64::MAX as u64
        {
            return Err(PlacementError::InvalidDeviceSelector);
        }
        Ok(())
    }
}

/// Where a work item starts executing.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemStartPlacement {
    Cloud,
    CurrentDevice,
    SelectedDevice(DeviceSelector),
}

impl WorkItemStartPlacement {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Cloud => "cloud",
            Self::CurrentDevice => "current_device",
            Self::SelectedDevice(_) => "selected_device",
        }
    }

    pub const fn runs_on_device(&self) -> bool {
        !matches!(self, Self::Cloud)
    }
}

/// How the coordinating agent learns about the started work item.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WorkItemCoordinatorDelivery {
    TaskOnly,
    ImmediateAlreadyDelivered { delivery_ids: Vec<CommandId> },
}

pub const MAX_COORDINATOR_DELIVERIES: usize = 16;

/// Model-facing JSON arguments for placement (`{"kind": "cloud"}` and so on).
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlacementArguments {
    Cloud {},
    CurrentDevice {},
    PairedDevice {
        membership_id: uuid::Uuid,
        expected_membership_epoch: u64,
    },
}

impl PlacementArguments {
    pub fn resolve(self) -> Result<WorkItemStartPlacement, PlacementError> {
        Ok(match self {
            Self::Cloud {} => WorkItemStartPlacement::Cloud,
            Self::CurrentDevice {} => WorkItemStartPlacement::CurrentDevice,
            Self::PairedDevice {
                membership_id,
                expected_membership_epoch,
            } => {
                let selector = DeviceSelector {
                    membership_id,
                    expected_membership_epoch,
                };
                selector.validate()?;
                WorkItemStartPlacement::SelectedDevice(selector)
            }
        })
    }
}

/// Model-facing JSON arguments for coordinator delivery.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoordinatorArguments {
    TaskOnly {},
    ImmediateAlreadyDelivered { delivery_ids: Vec<uuid::Uuid> },
}

impl CoordinatorArguments {
    pub fn resolve(self) -> Result<WorkItemCoordinatorDelivery, PlacementError> {
        match self {
            Self::TaskOnly {} => Ok(WorkItemCoordinatorDelivery::TaskOnly),
            Self::ImmediateAlreadyDelivered { delivery_ids } => {
                let unique = delivery_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                if !(1..=MAX_COORDINATOR_DELIVERIES).contains(&delivery_ids.len())
                    || unique != delivery_ids.len()
                {
                    return Err(PlacementError::InvalidCoordinatorDelivery);
                }
                let delivery_ids = delivery_ids
                    .into_iter()
                    .map(|id| {
                        CommandId::from_uuid(id)
                            .map_err(|_| PlacementError::InvalidCoordinatorDelivery)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(WorkItemCoordinatorDelivery::ImmediateAlreadyDelivered { delivery_ids })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement(json: &str) -> Result<WorkItemStartPlacement, PlacementError> {
        serde_json::from_str::<PlacementArguments>(json)
            .expect("well-formed arguments")
            .resolve()
    }

    #[test]
    fn resolves_every_placement_class() {
        assert_eq!(
            placement(r#"{"kind":"cloud"}"#),
            Ok(WorkItemStartPlacement::Cloud)
        );
        assert_eq!(
            placement(r#"{"kind":"current_device"}"#),
            Ok(WorkItemStartPlacement::CurrentDevice)
        );
        let selected = placement(
            r#"{"kind":"paired_device","membership_id":"0195f3a0-0000-7000-8000-000000000001","expected_membership_epoch":3}"#,
        )
        .expect("valid selector");
        assert!(selected.runs_on_device());
        assert_eq!(selected.as_str(), "selected_device");
    }

    #[test]
    fn refuses_invalid_selector_and_unknown_fields() {
        assert_eq!(
            placement(
                r#"{"kind":"paired_device","membership_id":"00000000-0000-0000-0000-000000000000","expected_membership_epoch":1}"#
            ),
            Err(PlacementError::InvalidDeviceSelector)
        );
        assert_eq!(
            placement(
                r#"{"kind":"paired_device","membership_id":"0195f3a0-0000-7000-8000-000000000001","expected_membership_epoch":0}"#
            ),
            Err(PlacementError::InvalidDeviceSelector)
        );
        assert!(
            serde_json::from_str::<PlacementArguments>(r#"{"kind":"cloud","host":"x"}"#).is_err()
        );
    }

    #[test]
    fn coordinator_delivery_is_bounded_and_unique() {
        let id = "0195f3a0-0000-7000-8000-000000000002";
        let ok = serde_json::from_str::<CoordinatorArguments>(&format!(
            r#"{{"kind":"immediate_already_delivered","delivery_ids":["{id}"]}}"#
        ))
        .expect("arguments")
        .resolve();
        assert!(matches!(
            ok,
            Ok(WorkItemCoordinatorDelivery::ImmediateAlreadyDelivered { ref delivery_ids }) if delivery_ids.len() == 1
        ));
        let duplicate = serde_json::from_str::<CoordinatorArguments>(&format!(
            r#"{{"kind":"immediate_already_delivered","delivery_ids":["{id}","{id}"]}}"#
        ))
        .expect("arguments")
        .resolve();
        assert_eq!(duplicate, Err(PlacementError::InvalidCoordinatorDelivery));
        let empty = serde_json::from_str::<CoordinatorArguments>(
            r#"{"kind":"immediate_already_delivered","delivery_ids":[]}"#,
        )
        .expect("arguments")
        .resolve();
        assert_eq!(empty, Err(PlacementError::InvalidCoordinatorDelivery));
    }
}
