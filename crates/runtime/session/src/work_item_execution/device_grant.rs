//! Device grant decision for the original reserved Task, separate from Cloud admission.
use super::*;
use musteragent_execution_grant::SignedExecutionGrant;
use musteragent_scheduler::AdmissionSelection;
use musteragent_work_item::SignedWorkItemAdmissionRejection;
pub struct WorkItemDeviceGrant {
    pub source: WorkItemDeviceExecutionPreparation,
    pub signed: SignedExecutionGrant,
    pub selection: AdmissionSelection,
}
#[expect(
    clippy::large_enum_variant,
    reason = "The public enum exposes these payloads by value to host implementations."
)]
pub enum WorkItemDeviceGrantDecision {
    Granted(WorkItemDeviceGrant),
    Rejected(SignedWorkItemAdmissionRejection),
}
pub trait WorkItemDeviceGrantPort: Send + Sync {
    fn issue_device<'a>(
        &'a self,
        preparation: &'a WorkItemExecutionPreparation,
        previous: Option<&'a WorkItemDeviceGrantCustody>,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceGrantDecision, WorkItemHandoffFailure>>;
    fn issue_device_in_workspace<'a>(
        &'a self,
        placement: &'a WorkItemDeviceExecutionPlacement,
        previous: Option<&'a WorkItemDeviceGrantCustody>,
    ) -> ApplicationFuture<'a, Result<WorkItemDeviceGrantDecision, WorkItemHandoffFailure>> {
        Box::pin(async move {
            if placement.device_workspace_id.is_some() {
                return Err(WorkItemHandoffFailure::InvalidContract);
            }
            self.issue_device(&placement.preparation, previous).await
        })
    }
}
