/// Presentation of closed source-tool refusals. These messages carry no
/// authority and never suggest a particular installation or consent action.
pub enum OwnerGoalSourceRefusal {
    InvalidArguments,
    AccessDenied,
    RevisionConflict,
    UnavailableReference,
}
impl OwnerGoalSourceRefusal {
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidArguments => {
                "Source selection is invalid. Use exact catalog/resource identifiers and valid task/source revisions. No source change was committed."
            }
            Self::AccessDenied => {
                "The current request or application source is not authorized. Inspect the source catalog for the exact requirement; use the original capability request card when user preparation is required."
            }
            Self::RevisionConflict => {
                "The task/source revision or immutable command no longer matches. Inspect current task/source state. To recover a prior command, preserve its original action key and arguments; do not repeat an uncertain write."
            }
            Self::UnavailableReference => {
                "The task or application source is unavailable. Inspect the current task and source catalog before choosing the next action."
            }
        }
    }
}
