//! Verified prerequisite output travels as original Content, never copied prose
//! or an inferred answer. Completed Tasks are immutable; erasure revokes the bytes.
use super::*;
use musteragent_content_ref::ContentRef;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemDependencyResult {
    pub work_item_id: WorkItemId,
    pub goal_revision: u64,
    pub control_revision: u64,
    pub assignment_epoch: u64,
    pub producing_run_id: RunId,
    pub content_ref: ContentRef,
}

impl WorkItemDependencyResult {
    pub fn section_key(&self) -> String {
        format!("dialogue.task-dependency.task-{}", self.work_item_id)
    }
    pub fn validate(&self) -> Result<(), DialogueStoreError> {
        if self.goal_revision == 0
            || self.control_revision == 0
            || self.assignment_epoch == 0
            || self.content_ref.media_type.as_str()
                != "application/vnd.musteragent.task-report+json"
            || self.content_ref.byte_length == 0
            || self.content_ref.byte_length > 8 * 1024 * 1024
        {
            return Err(DialogueStoreError::Corrupt);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_task_identifiers_have_valid_prompt_section_keys() {
        let result = WorkItemDependencyResult {
            work_item_id: WorkItemId::from_uuid(uuid::Uuid::from_u128(1))
                .expect("valid test fixture"),
            goal_revision: 1,
            control_revision: 1,
            assignment_epoch: 1,
            producing_run_id: RunId::generate(),
            content_ref: ContentRef {
                content_id: musteragent_identity::ContentId::generate(),
                media_type: musteragent_content_ref::MediaType::new(
                    "application/vnd.musteragent.task-report+json",
                )
                .expect("valid test fixture"),
                byte_length: 2,
                sha256: musteragent_content_ref::Sha256::digest(b"{}"),
            },
        };
        result.validate().expect("valid test fixture");
        musteragent_identity::DottedKey::new(result.section_key()).expect("valid test fixture");
    }
}
