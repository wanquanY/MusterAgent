//! Original Interaction application storage boundary.
use super::*;
use musteragent_runtime_contract::CommandMetadata;

pub trait RuntimeInteractionApplicationStore: Send + Sync {
    fn read_dialogue_interaction_control(
        &self,
        _actor: Actor,
        _dialogue: musteragent_identity::AgentDialogueId,
        _entry: musteragent_identity::DialogueEntryId,
        _command: musteragent_identity::CommandId,
    ) -> ApplicationFuture<'_, Result<crate::DialogueInteractionControlReceipt, RuntimeError>> {
        Box::pin(async {
            Err(crate::dialogue_interaction::unavailable(
                CorrelationId::generate(),
            ))
        })
    }
    /// Device-owned requests are read from authenticated observations, never
    /// reconstructed as Cloud Interaction authority.
    fn load_native_dialogue_interaction(
        &self,
        _actor: Actor,
        _dialogue: musteragent_identity::AgentDialogueId,
        _entry: musteragent_identity::DialogueEntryId,
        _correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<crate::DialogueInteractionView>, RuntimeError>> {
        Box::pin(async { Ok(None) })
    }
    /// None identifies an original Cloud entry. A Device command is queued
    /// without fabricating the original execution Owner's committed ACK.
    fn submit_native_dialogue_interaction(
        &self,
        _metadata: CommandMetadata,
        _target: crate::DialogueInteractionTarget,
        _response: crate::DialogueInteractionControlResponse,
    ) -> ApplicationFuture<'_, Result<Option<crate::DialogueInteractionControlReceipt>, RuntimeError>>
    {
        Box::pin(async { Ok(None) })
    }
    fn list_pending_dialogue_interactions(
        &self,
        _actor: Actor,
        _query: crate::DialoguePendingInteractionQuery,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<crate::DialoguePendingInteractionPage, RuntimeError>> {
        Box::pin(async move { Err(crate::dialogue_interaction::unavailable(correlation)) })
    }
    /// Original entry authority and the exact succeeded Tool receipt are
    /// revalidated; clients cannot query arbitrary account memory identities.
    fn load_owner_memory_current(
        &self,
        _actor: Actor,
        _dialogue: musteragent_identity::AgentDialogueId,
        _entry: musteragent_identity::DialogueEntryId,
        _result_ref: musteragent_content_ref::ContentRef,
        _receipt: crate::OwnerMemoryReceipt,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<crate::OwnerMemoryCurrentAuthority, RuntimeError>> {
        Box::pin(async move { Err(crate::dialogue_interaction::unavailable(correlation)) })
    }
    fn load_dialogue_interaction(
        &self,
        _actor: Actor,
        _dialogue_id: musteragent_identity::AgentDialogueId,
        _entry_id: musteragent_identity::DialogueEntryId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<crate::DialogueInteractionAuthority, RuntimeError>> {
        Box::pin(async move { Err(crate::dialogue_interaction::unavailable(correlation_id)) })
    }
    fn load_dialogue_interaction_replay(
        &self,
        _actor: Actor,
        _target: crate::DialogueInteractionTarget,
        _query: CommandReplayQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<AppendReceipt>, RuntimeError>> {
        Box::pin(async move { Err(crate::dialogue_interaction::unavailable(correlation_id)) })
    }
    fn respond_dialogue_interaction(
        &self,
        _target: crate::DialogueInteractionTarget,
        correlation_id: CorrelationId,
        _commit: RespondInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>> {
        Box::pin(async move { Err(crate::dialogue_interaction::unavailable(correlation_id)) })
    }
    fn close_dialogue_interaction(
        &self,
        _actor: Actor,
        _target: crate::DialogueInteractionTarget,
        correlation_id: CorrelationId,
        _commit: CloseInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>> {
        Box::pin(async move { Err(crate::dialogue_interaction::unavailable(correlation_id)) })
    }

    /// Current authority for receipt reads, independent of factual Participant
    /// enrollment. A read must never join the Conversation as a side effect.
    fn load_interaction(
        &self,
        actor: Actor,
        interaction_id: musteragent_identity::InteractionId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<InteractionAuthority, RuntimeError>>;

    fn respond_interaction(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: RespondInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>>;

    fn close_interaction(
        &self,
        correlation_id: musteragent_identity::CorrelationId,
        commit: CloseInteractionCommit,
    ) -> ApplicationFuture<'_, Result<InteractionCommitReceipt, RuntimeError>>;

    fn get_interaction(
        &self,
        actor: Actor,
        interaction_id: musteragent_identity::InteractionId,
        correlation_id: musteragent_identity::CorrelationId,
    ) -> ApplicationFuture<'_, Result<InteractionRead, RuntimeError>>;
}
