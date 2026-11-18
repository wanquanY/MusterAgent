//! The original Task control obligations projected to their exact Native host.
use super::*;

pub const WORK_ITEM_DEVICE_CONTROL_PAGE_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkItemDeviceControlCursor {
    pub created_at: UtcTimestamp,
    pub run_id: RunId,
}
impl WorkItemDeviceControlCursor {
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.created_at.unix_millis() <= 0 {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDeviceControlPage {
    pub controls: Vec<WorkItemDeviceControl>,
    pub next_cursor: Option<WorkItemDeviceControlCursor>,
    pub scan_exhausted: bool,
}
impl WorkItemDeviceControlPage {
    pub fn validate_after(
        &self,
        after: Option<WorkItemDeviceControlCursor>,
    ) -> Result<(), DialogueStoreError> {
        if let Some(after) = after {
            after.validate()?;
        }
        if self.controls.len() > WORK_ITEM_DEVICE_CONTROL_PAGE_SIZE
            || self.scan_exhausted == self.next_cursor.is_some()
        {
            return Err(DialogueStoreError::Corrupt);
        }
        if let Some(next) = self.next_cursor {
            next.validate()?;
            if after.is_some_and(|after| next <= after) {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for control in &self.controls {
            control.validate()?;
            if !seen.insert(control.run_id) {
                return Err(DialogueStoreError::Corrupt);
            }
        }
        Ok(())
    }
}

pub trait WorkItemDeviceControlStore: Send + Sync {
    fn list_device_controls<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        after: Option<WorkItemDeviceControlCursor>,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceControlPage, DialogueStoreError>>;
    fn record_not_admitted<'a>(
        &'a self,
        authority: &'a crate::DeviceHostAuthority,
        receipt: WorkItemDeviceNotAdmittedReceipt,
    ) -> ApplicationFuture<'a, Result<bool, DialogueStoreError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_denied_page_advances_but_missing_or_backwards_cursor_is_invalid() {
        let after = WorkItemDeviceControlCursor {
            created_at: UtcTimestamp::from_unix_millis(100),
            run_id: RunId::generate(),
        };
        let next = WorkItemDeviceControlCursor {
            created_at: UtcTimestamp::from_unix_millis(101),
            ..after
        };
        let mut page = WorkItemDeviceControlPage {
            controls: vec![],
            next_cursor: Some(next),
            scan_exhausted: false,
        };
        assert!(page.validate_after(Some(after)).is_ok());
        page.next_cursor = Some(after);
        assert!(page.validate_after(Some(after)).is_err());
        page.next_cursor = None;
        assert!(page.validate_after(Some(after)).is_err());
        page.scan_exhausted = true;
        assert!(page.validate_after(Some(after)).is_ok());
        page.next_cursor = Some(next);
        assert!(page.validate_after(Some(after)).is_err());
        page.next_cursor = Some(WorkItemDeviceControlCursor {
            created_at: UtcTimestamp::from_unix_millis(0),
            ..next
        });
        page.scan_exhausted = false;
        assert!(page.validate_after(None).is_err());
    }
}
