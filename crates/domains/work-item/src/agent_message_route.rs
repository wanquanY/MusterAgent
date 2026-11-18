//! Stable Dialogue business addresses in the original collaboration domain.
//! These are consistency facts, never a grant to contact an arbitrary Agent.
use super::OwnedAgentWorkItemScope;
/// Invalid or stale private Agent route.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum AgentRouteError {
    #[error("invalid private Agent route")]
    InvalidDialogueRoute,
}
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    AgentId, AssignmentEpoch, ConversationId, GoalId, InterSessionMessageId, UtcTimestamp,
    WorkItemId,
};

/// A relationship is owned by an accepted Goal binding or WorkItem assignment.
/// Changing its owner revision invalidates future old-route execution without
/// erasing the receipt for an earlier message.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentCollaborationRelationship {
    Goal {
        goal_id: GoalId,
        binding_revision: u64,
        processing_rules_version: Sha256,
    },
    WorkItem {
        work_item_id: WorkItemId,
        goal_revision: u64,
        control_revision: u64,
        assignment_epoch: AssignmentEpoch,
    },
}

/// Coordinator is a private context within this relationship, not the Agent's
/// public Dialogue or an unrelated active user conversation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AgentCollaborationSubject {
    Coordinator,
    Goal(GoalId),
    WorkItem(WorkItemId),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentCollaborationAddress {
    pub agent_id: AgentId,
    pub subject: AgentCollaborationSubject,
}

/// Resolved from the current original Goal binding or WorkItem assignment.
/// This projection is not a new relationship registry or an execution grant.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentCollaborationBinding {
    pub scope: OwnedAgentWorkItemScope,
    pub relationship: AgentCollaborationRelationship,
    pub worker: AgentCollaborationAddress,
    pub worker_conversation_id: ConversationId,
    pub coordinator_conversation_id: ConversationId,
}

impl AgentCollaborationBinding {
    /// Never interpret a public Dialogue UUID or arbitrary Conversation as a
    /// stable private address. Both endpoints come from the same original owner.
    pub fn resolve(
        &self,
        address: AgentCollaborationAddress,
    ) -> Result<ConversationId, AgentRouteError> {
        let expected_worker = match self.relationship {
            AgentCollaborationRelationship::Goal { goal_id, .. } => {
                AgentCollaborationSubject::Goal(goal_id)
            }
            AgentCollaborationRelationship::WorkItem { work_item_id, .. } => {
                AgentCollaborationSubject::WorkItem(work_item_id)
            }
        };
        if self.worker.subject != expected_worker
            || self.worker_conversation_id == self.coordinator_conversation_id
        {
            return Err(AgentRouteError::InvalidDialogueRoute);
        }
        if address == self.worker {
            return Ok(self.worker_conversation_id);
        }
        if address
            == (AgentCollaborationAddress {
                agent_id: self.scope.agent_id,
                subject: AgentCollaborationSubject::Coordinator,
            })
        {
            return Ok(self.coordinator_conversation_id);
        }
        Err(AgentRouteError::InvalidDialogueRoute)
    }

    pub fn validate_route(
        &self,
        route: &AgentMessageRoute,
        source: ConversationId,
        target: ConversationId,
    ) -> Result<(), AgentRouteError> {
        if source == target
            || self.scope != route.scope
            || self.relationship != route.relationship
            || self.resolve(route.source)? != source
            || self.resolve(route.target)? != target
        {
            return Err(AgentRouteError::InvalidDialogueRoute);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AgentMessageRoute {
    /// Reuses the original business ownership shared with WorkItem return routing.
    pub scope: OwnedAgentWorkItemScope,
    pub relationship: AgentCollaborationRelationship,
    pub source: AgentCollaborationAddress,
    pub target: AgentCollaborationAddress,
    pub root_message_id: InterSessionMessageId,
    pub in_reply_to: Option<InterSessionMessageId>,
    /// Original owner checks that this is exactly the parent's depth plus one.
    pub reply_depth: u16,
    pub expires_at: Option<UtcTimestamp>,
}

impl AgentMessageRoute {
    pub fn validate(
        &self,
        message_id: InterSessionMessageId,
        created_at: UtcTimestamp,
        work_item_id: Option<WorkItemId>,
    ) -> Result<(), AgentRouteError> {
        use AgentCollaborationRelationship as Relationship;
        use AgentCollaborationSubject as Subject;
        let valid_revision = |revision: u64| revision > 0 && revision <= i64::MAX as u64;
        let worker = match self.relationship {
            Relationship::Goal {
                goal_id,
                binding_revision,
                ..
            } if valid_revision(binding_revision) => {
                if work_item_id.is_some() {
                    return Err(AgentRouteError::InvalidDialogueRoute);
                }
                Subject::Goal(goal_id)
            }
            Relationship::WorkItem {
                work_item_id: id,
                goal_revision,
                control_revision,
                assignment_epoch,
                ..
            } if valid_revision(goal_revision)
                && valid_revision(control_revision)
                && valid_revision(assignment_epoch.get())
                && work_item_id == Some(id) =>
            {
                Subject::WorkItem(id)
            }
            _ => return Err(AgentRouteError::InvalidDialogueRoute),
        };
        let valid_endpoint = |address: AgentCollaborationAddress| {
            if address.subject == Subject::Coordinator {
                address.agent_id == self.scope.agent_id
            } else {
                address.subject == worker
            }
        };
        if !valid_endpoint(self.source)
            || !valid_endpoint(self.target)
            || self.source.subject == self.target.subject
            || self.expires_at.is_some_and(|expiry| expiry <= created_at)
        {
            return Err(AgentRouteError::InvalidDialogueRoute);
        }
        // The first message owns the causal root. Replies cannot reset it or
        // claim a new root to escape the original relationship's bounds.
        let lineage = match self.in_reply_to {
            None => self.reply_depth == 0 && self.root_message_id == message_id,
            Some(parent) => {
                self.reply_depth > 0 && parent != message_id && self.root_message_id != message_id
            }
        };
        if !lineage {
            return Err(AgentRouteError::InvalidDialogueRoute);
        }
        Ok(())
    }

    /// Called against the immutable parent fetched by the owner, after current
    /// relationship authority has also been checked. Reading is not consuming.
    pub fn validate_reply(
        &self,
        parent_id: InterSessionMessageId,
        parent: &Self,
        created_at: UtcTimestamp,
        parent_created_at: UtcTimestamp,
    ) -> Result<(), AgentRouteError> {
        if self.in_reply_to != Some(parent_id)
            || self.scope != parent.scope
            || self.relationship != parent.relationship
            || self.root_message_id != parent.root_message_id
            || Some(self.reply_depth) != parent.reply_depth.checked_add(1)
            || self.source != parent.target
            || self.target != parent.source
            || self.expires_at != parent.expires_at
            || created_at < parent_created_at
            || parent.expires_at.is_some_and(|expiry| created_at >= expiry)
        {
            return Err(AgentRouteError::InvalidDialogueRoute);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::{OwnerSpaceId, PrincipalId};

    fn route() -> AgentMessageRoute {
        let agent_id = AgentId::generate();
        let goal_id = GoalId::generate();
        AgentMessageRoute {
            scope: OwnedAgentWorkItemScope {
                space_id: OwnerSpaceId::generate(),
                owner_id: PrincipalId::generate(),
                agent_id,
            },
            relationship: AgentCollaborationRelationship::Goal {
                goal_id,
                binding_revision: 1,
                processing_rules_version: Sha256::digest(b"accepted rules"),
            },
            source: AgentCollaborationAddress {
                agent_id,
                subject: AgentCollaborationSubject::Goal(goal_id),
            },
            target: AgentCollaborationAddress {
                agent_id,
                subject: AgentCollaborationSubject::Coordinator,
            },
            root_message_id: InterSessionMessageId::generate(),
            in_reply_to: None,
            reply_depth: 0,
            expires_at: Some(UtcTimestamp::from_unix_millis(500)),
        }
    }

    #[test]
    fn a_private_goal_and_coordinator_can_share_an_agent_without_sharing_an_address() {
        let first = route();
        let now = UtcTimestamp::from_unix_millis(100);
        first.validate(first.root_message_id, now, None).unwrap();
        let mut invalid = first;
        invalid.target = invalid.source;
        assert!(
            invalid
                .validate(invalid.root_message_id, now, None)
                .is_err()
        );
        invalid = first;
        invalid.target.agent_id = AgentId::generate();
        assert!(
            invalid
                .validate(invalid.root_message_id, now, None)
                .is_err()
        );
    }

    #[test]
    fn replies_keep_the_original_relationship_and_causal_root() {
        let first = route();
        let now = UtcTimestamp::from_unix_millis(100);
        let later = UtcTimestamp::from_unix_millis(200);
        let reply_id = InterSessionMessageId::generate();
        let reply = AgentMessageRoute {
            source: first.target,
            target: first.source,
            in_reply_to: Some(first.root_message_id),
            reply_depth: 1,
            ..first
        };
        reply.validate(reply_id, later, None).unwrap();
        reply
            .validate_reply(first.root_message_id, &first, later, now)
            .unwrap();
        let mut other = reply;
        other.root_message_id = InterSessionMessageId::generate();
        assert!(
            other
                .validate_reply(first.root_message_id, &first, later, now)
                .is_err()
        );
        other = reply;
        other.scope.owner_id = PrincipalId::generate();
        assert!(
            other
                .validate_reply(first.root_message_id, &first, later, now)
                .is_err()
        );
        assert!(
            reply
                .validate_reply(
                    first.root_message_id,
                    &first,
                    UtcTimestamp::from_unix_millis(500),
                    now
                )
                .is_err()
        );
    }

    #[test]
    fn current_binding_rejects_changed_rules_wrong_worker_and_context_aliases() {
        let route = route();
        let binding = AgentCollaborationBinding {
            scope: route.scope,
            relationship: route.relationship,
            worker: route.source,
            worker_conversation_id: ConversationId::generate(),
            coordinator_conversation_id: ConversationId::generate(),
        };
        binding
            .validate_route(
                &route,
                binding.worker_conversation_id,
                binding.coordinator_conversation_id,
            )
            .unwrap();
        let mut wrong = route;
        wrong.source.agent_id = AgentId::generate();
        assert!(
            binding
                .validate_route(
                    &wrong,
                    binding.worker_conversation_id,
                    binding.coordinator_conversation_id
                )
                .is_err()
        );
        wrong = route;
        let AgentCollaborationRelationship::Goal {
            ref mut processing_rules_version,
            ..
        } = wrong.relationship
        else {
            panic!("goal")
        };
        *processing_rules_version = Sha256::digest(b"changed rules");
        assert!(
            binding
                .validate_route(
                    &wrong,
                    binding.worker_conversation_id,
                    binding.coordinator_conversation_id
                )
                .is_err()
        );
        assert!(
            binding
                .validate_route(
                    &route,
                    binding.coordinator_conversation_id,
                    binding.worker_conversation_id
                )
                .is_err()
        );
        let aliased = AgentCollaborationBinding {
            coordinator_conversation_id: binding.worker_conversation_id,
            ..binding
        };
        assert!(aliased.resolve(route.source).is_err());
    }
}
