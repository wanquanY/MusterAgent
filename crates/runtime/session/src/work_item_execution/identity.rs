use super::*;
use musteragent_content_ref::Sha256;

/// External callers name a reservation; only the WorkItem owner resolves its
/// requester scope and executing Agent from durable authority.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemExecutionRequest {
    pub actor: Actor,
    pub work_item_id: WorkItemId,
    pub goal_revision: WorkItemGoalRevision,
    pub control_revision: WorkItemControlRevision,
    pub assignment_epoch: AssignmentEpoch,
    pub report_from_run_id: Option<RunId>,
}

/// Business execution identity. Assignment epoch is allocated by the WorkItem
/// owner; a model, tool retry or reader cannot increment it.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemExecutionKey {
    pub actor: Actor,
    pub scope: OwnedAgentWorkItemScope,
    pub work_item_id: WorkItemId,
    pub goal_revision: WorkItemGoalRevision,
    pub control_revision: WorkItemControlRevision,
    pub assignment_epoch: AssignmentEpoch,
    pub report_from_run_id: Option<RunId>,
    pub executing_agent_id: AgentId,
}
impl WorkItemExecutionKey {
    pub fn request(self) -> WorkItemExecutionRequest {
        WorkItemExecutionRequest {
            actor: self.actor,
            work_item_id: self.work_item_id,
            goal_revision: self.goal_revision,
            control_revision: self.control_revision,
            assignment_epoch: self.assignment_epoch,
            report_from_run_id: self.report_from_run_id,
        }
    }
    pub fn validate(self) -> Result<(), DialogueStoreError> {
        if self.actor.principal_kind != PrincipalKind::User
            || self.actor.principal_id != self.scope.owner_id
            || (self.report_from_run_id.is_some() && self.executing_agent_id != self.scope.agent_id)
        {
            Err(DialogueStoreError::Unavailable)
        } else {
            Ok(())
        }
    }
    pub fn identities(self) -> WorkItemExecutionIdentities {
        let mut seed = if self.report_from_run_id.is_some() {
            b"musteragent.work-item-report.v1\0".to_vec()
        } else {
            b"musteragent.work-item-execution.v1\0".to_vec()
        };
        for id in [
            self.actor.tenant_id.into_uuid(),
            self.scope.owner_id.into_uuid(),
            self.scope.space_id.into_uuid(),
            self.scope.agent_id.into_uuid(),
            self.work_item_id.into_uuid(),
            self.executing_agent_id.into_uuid(),
        ] {
            seed.extend_from_slice(id.as_bytes());
        }
        for value in [
            self.goal_revision.get(),
            self.control_revision.get(),
            self.assignment_epoch.get(),
        ] {
            seed.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(run) = self.report_from_run_id {
            seed.extend_from_slice(run.into_uuid().as_bytes());
        }
        let id = |label: &[u8]| {
            let mut source = seed.clone();
            source.extend_from_slice(label);
            let hash = Sha256::digest(source).into_bytes();
            let mut bytes = [0; 16];
            bytes.copy_from_slice(&hash[..16]);
            bytes[6] = (bytes[6] & 0x0f) | 0x50;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            uuid::Uuid::from_bytes(bytes)
        };
        WorkItemExecutionIdentities {
            command_id: CommandId::from_uuid(id(b"command")).expect("nonzero derived UUID"),
            conversation_id: ConversationId::from_uuid(id(b"conversation"))
                .expect("nonzero derived UUID"),
            requested_by: ParticipantId::from_uuid(id(b"requester")).expect("nonzero derived UUID"),
            user: ParticipantId::from_uuid(id(b"user")).expect("nonzero derived UUID"),
            executor: ParticipantId::from_uuid(id(b"executor")).expect("nonzero derived UUID"),
            message_id: MessageId::from_uuid(id(b"message")).expect("nonzero derived UUID"),
        }
    }
}
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WorkItemExecutionIdentities {
    pub command_id: CommandId,
    pub conversation_id: ConversationId,
    pub requested_by: ParticipantId,
    pub user: ParticipantId,
    pub executor: ParticipantId,
    pub message_id: MessageId,
}
impl WorkItemExecutionIdentities {
    pub fn run_id(self) -> Result<RunId, DialogueStoreError> {
        musteragent_runtime_admission::AdmissionIdentities::derive(self.command_id)
            .map(|v| v.run_id)
            .ok_or(DialogueStoreError::Corrupt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn golden() -> WorkItemExecutionKey {
        let id = |s: &str| uuid::Uuid::parse_str(s).expect("valid test fixture");
        let owner = PrincipalId::from_uuid(id("018f4f70-704a-77c0-81d4-005f66dc891c"))
            .expect("valid test fixture");
        WorkItemExecutionKey {
            actor: Actor {
                tenant_id: TenantId::from_uuid(id("018f4f70-704a-77c0-81d4-005f66dc891b"))
                    .expect("valid test fixture"),
                principal_id: owner,
                principal_kind: PrincipalKind::User,
            },
            scope: OwnedAgentWorkItemScope {
                owner_id: owner,
                space_id: OwnerSpaceId::from_uuid(id("019b2f80-0000-7000-8000-000000000002"))
                    .expect("valid test fixture"),
                agent_id: AgentId::from_uuid(id("019b2f80-0000-7000-8000-000000000003"))
                    .expect("valid test fixture"),
            },
            work_item_id: WorkItemId::from_uuid(id("019b2f80-0000-7000-8000-000000000001"))
                .expect("valid test fixture"),
            executing_agent_id: AgentId::from_uuid(id("019b2f80-0000-7000-8000-000000000004"))
                .expect("valid test fixture"),
            goal_revision: WorkItemGoalRevision::new(2).expect("valid test fixture"),
            control_revision: WorkItemControlRevision::new(3).expect("valid test fixture"),
            assignment_epoch: AssignmentEpoch::new(4).expect("valid test fixture"),
            report_from_run_id: None,
        }
    }
    #[test]
    fn execution_identity_matches_independent_node_and_backend_golden() {
        let ids = golden().identities();
        for (actual, expected) in [
            (
                ids.command_id.to_string(),
                "1b184aaf-a51f-5071-84cb-ab3ace1da5c9",
            ),
            (
                ids.conversation_id.to_string(),
                "35b7461d-e3a7-5189-ad12-1333f82fa536",
            ),
            (
                ids.requested_by.to_string(),
                "fa6b5e6a-2e28-5ff5-8d33-7a7d94776200",
            ),
            (ids.user.to_string(), "f1f54695-32b4-5b76-a0c0-060f90ac4daa"),
            (
                ids.executor.to_string(),
                "7a3d2434-2c35-5375-ba57-94093230a20e",
            ),
            (
                ids.message_id.to_string(),
                "afa52736-092c-570e-996e-e7356fe80b43",
            ),
        ] {
            assert_eq!(actual, expected);
        }
    }
    #[test]
    fn report_identity_has_a_distinct_domain_and_exact_producing_run() {
        let mut k = golden();
        k.executing_agent_id = k.scope.agent_id;
        k.report_from_run_id = Some(
            RunId::from_uuid(
                uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000005")
                    .expect("valid test fixture"),
            )
            .expect("valid test fixture"),
        );
        let ids = k.identities();
        for (actual, expected) in [
            (
                ids.command_id.to_string(),
                "b296b158-ef2e-5e00-8960-0f827fc98786",
            ),
            (
                ids.conversation_id.to_string(),
                "3bc840fa-1cfd-5b0c-96fd-f86e2b571ca5",
            ),
            (
                ids.requested_by.to_string(),
                "678881a4-7a14-5509-97d5-0be6729a191c",
            ),
            (ids.user.to_string(), "c94cb2d7-8fb3-5acf-adfa-70aae885f2f9"),
            (
                ids.executor.to_string(),
                "b4cbe86a-a75b-5fb3-9516-118406abfead",
            ),
            (
                ids.message_id.to_string(),
                "e58d24e1-61b1-5ac6-b864-ea9cf8d6b102",
            ),
        ] {
            assert_eq!(actual, expected);
        }
        k.report_from_run_id = Some(RunId::generate());
        assert_ne!(k.identities(), ids);
    }
    #[test]
    fn authority_and_each_revision_separate_admission_identity() {
        let first = golden();
        let original = first.identities();
        let mut variants = Vec::new();
        let mut k = first;
        k.goal_revision = WorkItemGoalRevision::new(3).expect("valid test fixture");
        variants.push(k);
        let mut k = first;
        k.control_revision = WorkItemControlRevision::new(4).expect("valid test fixture");
        variants.push(k);
        let mut k = first;
        k.assignment_epoch = AssignmentEpoch::new(5).expect("valid test fixture");
        variants.push(k);
        let mut k = first;
        k.executing_agent_id = AgentId::generate();
        variants.push(k);
        let mut k = first;
        k.scope.agent_id = AgentId::generate();
        variants.push(k);
        let mut k = first;
        k.scope.owner_id = PrincipalId::generate();
        k.actor.principal_id = k.scope.owner_id;
        variants.push(k);
        for key in variants {
            assert_ne!(key.identities(), original);
            assert_eq!(key.request().work_item_id, first.work_item_id);
        }
    }
}
