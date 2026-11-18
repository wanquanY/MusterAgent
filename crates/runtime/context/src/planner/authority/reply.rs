//! An explicit user selection carries only references its consuming tools accept.
use super::*;
use musteragent_execution_context::{DialogueReplyBinding, DialogueReplySubject};

pub(in crate::planner) fn material(
    binding: &DialogueReplyBinding,
) -> Result<ModelText, ContextPlanError> {
    let subject = match &binding.subject {
        DialogueReplySubject::AcceptedInput { .. } => serde_json::json!({"kind":"user_message"}),
        DialogueReplySubject::IntentOutput { .. } => serde_json::json!({"kind":"delivered_result"}),
        DialogueReplySubject::WorkItemResult { action_target, .. } => {
            serde_json::json!({"kind":"task_result", "task_id":action_target.target.work_item_id.to_string(),
                "goal_revision":action_target.target.goal_revision,"control_revision":action_target.target.control_revision})
        }
        DialogueReplySubject::GoalResult { goal_id, .. } => {
            serde_json::json!({"kind":"goal_result","goal_id":goal_id.to_string()})
        }
    };
    let contents = binding.content_refs().into_iter().map(|r|
        serde_json::json!({"content_id":r.content_id.to_string(), "media_type":r.media_type.as_str()})
    ).collect::<Vec<_>>();
    let reference = serde_json::json!({"subject":subject, "readable_contents":contents});
    ModelText::new(format!(
        "User-selected reference for this message: {reference}"
    ))
    .map_err(|_| ContextPlanError::invariant())
}
