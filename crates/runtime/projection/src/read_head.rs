use crate::ConversationProjectionWindow;
use musteragent_conversation::ConversationView;
use musteragent_identity::{Actor, ConversationId};

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ConversationReadHeadError {
    #[error("conversation read authority does not match the requested owner")]
    AuthorityMismatch,
    #[error("conversation epoch changed after the captured projection")]
    EpochChanged,
    #[error("conversation head does not cover the captured projection")]
    ReadSkew,
}

/// A cold-open captures Projection before Conversation, then resumes Tail from
/// that exact Snapshot. Native and Cloud adapters share this relation; neither
/// may substitute an independently newer cursor for the projection authority.
pub fn validate_conversation_read_head(
    actor: Actor,
    conversation_id: ConversationId,
    window: &ConversationProjectionWindow,
    conversation: &ConversationView,
) -> Result<(), ConversationReadHeadError> {
    let projection = &window.projection;
    let view = &conversation.conversation;
    let observed = conversation.latest_cursor;
    if view.tenant_id != actor.tenant_id
        || projection.tenant_id != actor.tenant_id
        || view.conversation_id != conversation_id
        || projection.conversation_id != conversation_id
        || observed.conversation_id != conversation_id
        || window.head_cursor != projection.cursor()
        || window.authority_snapshot_id != projection.snapshot_id
    {
        return Err(ConversationReadHeadError::AuthorityMismatch);
    }
    if view.home_epoch != projection.home_epoch || observed.home_epoch != projection.home_epoch {
        return Err(ConversationReadHeadError::EpochChanged);
    }
    if observed.after_conversation_seq < projection.through_conversation_seq {
        return Err(ConversationReadHeadError::ReadSkew);
    }
    Ok(())
}
