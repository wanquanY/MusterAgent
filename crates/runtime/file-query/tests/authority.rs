mod support;
use musteragent_conversation::*;
use musteragent_event_envelope::{ExecutionScope, LedgerCursor, RuntimeHome};
use musteragent_identity::*;
use musteragent_run::*;
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_contract::{ApplicationFuture, DisplayText, RuntimeError};
use musteragent_runtime_file_query::*;
use std::sync::{Arc, Mutex};
use support::*;
struct Clock;
impl RuntimeClock for Clock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(100)
    }
}
struct State {
    conversation: Mutex<ConversationView>,
    run: Mutex<RunView>,
}
impl FileQueryStateSource for State {
    fn conversation(
        &self,
        _: GetConversation,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        Box::pin(async { Ok(self.conversation.lock().expect("fixture").clone()) })
    }
    fn run(&self, _: GetRun) -> ApplicationFuture<'_, Result<RunView, RuntimeError>> {
        Box::pin(async { Ok(self.run.lock().expect("fixture").clone()) })
    }
}
fn fixture() -> (Arc<State>, FileQueryAccess, CurrentFileQueryAuthority) {
    let tenant_id = TenantId::generate();
    let principal = PrincipalId::generate();
    let cid = ConversationId::generate();
    let actor = Actor {
        tenant_id,
        principal_id: principal,
        principal_kind: PrincipalKind::User,
    };
    let workspace_id = WorkspaceId::generate();
    let participant_id = ParticipantId::generate();
    let now = UtcTimestamp::from_unix_millis(1);
    let home_epoch = HomeEpoch::new(1).expect("epoch");
    let context = RunContext {
        tenant_id,
        conversation_id: cid,
        activity_id: ActivityId::generate(),
        activity_kind: musteragent_activity::ActivityKind::Chat,
        run_id: RunId::generate(),
        executor_participant_id: ParticipantId::generate(),
        requested_by_participant_id: participant_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: ExecutionScope::Workspace { workspace_id },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch,
        runtime_snapshot_id: RuntimeSnapshotId::generate(),
    };
    let attempt = AttemptContext {
        attempt_id: AttemptId::generate(),
        run_id: context.run_id,
        attempt_number: AttemptNumber::new(1).expect("number"),
        worker_id: WorkerId::new("query-test").expect("worker"),
        lease_id: LeaseId::generate(),
        fencing_token: FencingToken::new(1).expect("fence"),
        lease_expires_at: UtcTimestamp::from_unix_millis(200),
    };
    let cursor = LedgerCursor {
        conversation_id: cid,
        home_epoch,
        after_conversation_seq: LedgerOffset::new(1),
    };
    let conversation = ConversationView {
        conversation: Conversation {
            conversation_id: cid,
            tenant_id,
            workspace_id: Some(workspace_id),
            kind: ConversationKind::Direct,
            execution_owner_participant_id: context.executor_participant_id,
            title: None,
            control_home: RuntimeHome::Cloud,
            home_epoch,
            next_conversation_seq: ConversationSeq::new(2).expect("sequence"),
            lifecycle: ConversationLifecycle::Active,
            created_by: principal,
            created_at: now,
            updated_at: now,
            archived_at: None,
        },
        participants: vec![Participant {
            participant_id,
            conversation_id: cid,
            kind: ParticipantKind::User,
            principal_ref: Some(principal),
            agent_ref: None,
            service_ref: None,
            display_snapshot: ParticipantDisplaySnapshot {
                display_name: DisplayText::new("user").expect("name"),
                avatar_ref: None,
            },
            joined_at: now,
            left_at: None,
        }],
        latest_cursor: cursor,
        active_activity_ids: vec![context.activity_id],
        active_run_ids: vec![context.run_id],
        active_context_branch_id: None,
        context_branches: vec![],
    };
    let run = RunView {
        retry_configuration: None,
        run: Run {
            context,
            state: RunState::rehydrate(RunStatus::Running, None, Some(attempt.attempt_id))
                .expect("running"),
            next_attempt_number: AttemptNumber::new(2).expect("next"),
            not_before: None,
            cancel_requested_at: None,
            admitted_at: now,
            started_at: Some(now),
            finished_at: None,
            terminal_error: None,
        },
        active_attempt: Some(attempt.clone()),
        pending_interaction_ids: vec![],
        latest_cursor: cursor,
    };
    let state = Arc::new(State {
        conversation: Mutex::new(conversation),
        run: Mutex::new(run),
    });
    let access = FileQueryAccess {
        actor,
        conversation_id: Some(cid),
        workspace_id: None,
        run: Some(RunFileQueryAccess {
            invocation_id: None,
            context,
            attempt,
            authorized_content: vec![],
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(199) },
        }),
    };
    let authority =
        CurrentFileQueryAuthority::from_source(state.clone(), Arc::new(Clock), RuntimeHome::Cloud);
    (state, access, authority)
}
#[tokio::test]
async fn current_member_project_and_branch_authority_are_resolved_from_original_owners() {
    let (state, mut access, authority) = fixture();
    access.run = None;
    let initial = authority.resolve(&access).await.expect("current member");
    assert!(initial.project_workspace_id.is_some());
    assert!(initial.content_allowlist.is_none());
    state
        .conversation
        .lock()
        .expect("fixture")
        .active_context_branch_id = Some(ContextBranchId::generate());
    assert_ne!(
        initial.authority_revision,
        authority
            .resolve(&access)
            .await
            .expect("current branch")
            .authority_revision
    );
    state.conversation.lock().expect("fixture").participants[0].left_at =
        Some(UtcTimestamp::from_unix_millis(50));
    assert_eq!(
        authority.resolve(&access).await.err(),
        Some(FileQueryError::Denied)
    );
}
#[tokio::test]
async fn child_content_grants_are_distinct_from_explicit_project_execution_scope() {
    let (state, mut access, authority) = fixture();
    let run = access.run.as_mut().expect("run");
    run.context.delegation_id = Some(DelegationId::generate());
    run.context.parent_run_id = Some(RunId::generate());
    state.run.lock().expect("fixture").run.context = run.context;
    let granted = authority
        .resolve(&access)
        .await
        .expect("current delegated run");
    assert_eq!(granted.content_allowlist, Some(vec![]));
    assert!(granted.project_workspace_id.is_some());
    let run = access.run.as_mut().expect("run");
    run.context.execution_scope = ExecutionScope::Tenant {
        tenant_id: access.actor.tenant_id,
    };
    state.run.lock().expect("fixture").run.context = run.context;
    assert!(
        authority
            .resolve(&access)
            .await
            .expect("content only run")
            .project_workspace_id
            .is_none()
    );
}
#[tokio::test]
async fn stale_fence_expired_lease_cancel_and_workspace_reassignment_deny_run_access() {
    for variation in 0..4 {
        let (state, access, authority) = fixture();
        match variation {
            0 => {
                state
                    .run
                    .lock()
                    .expect("fixture")
                    .active_attempt
                    .as_mut()
                    .expect("attempt")
                    .fencing_token = FencingToken::new(2).expect("new fence")
            }
            1 => {
                state
                    .run
                    .lock()
                    .expect("fixture")
                    .active_attempt
                    .as_mut()
                    .expect("attempt")
                    .lease_expires_at = UtcTimestamp::from_unix_millis(99)
            }
            2 => {
                state.run.lock().expect("fixture").run.cancel_requested_at =
                    Some(UtcTimestamp::from_unix_millis(99))
            }
            _ => {
                state
                    .conversation
                    .lock()
                    .expect("fixture")
                    .conversation
                    .workspace_id = Some(WorkspaceId::generate())
            }
        }
        assert_eq!(
            authority.resolve(&access).await.err(),
            Some(FileQueryError::Denied)
        );
    }
}
struct MutableAuthority(Mutex<FileQueryContext>);
impl FileQueryAuthority for MutableAuthority {
    fn resolve<'a>(
        &'a self,
        _: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async { Ok(self.0.lock().expect("fixture").clone()) })
    }
}
#[tokio::test]
async fn permission_change_while_reading_discards_even_successful_body_io() {
    let f = Fixture::new();
    f.attachment("a.txt", "text/plain", b"secret");
    let reference = f.first().await;
    let authority = Arc::new(MutableAuthority(Mutex::new(f.context.clone())));
    let changed = authority.clone();
    *f.bytes.on_read.lock().expect("fixture") = Some(Arc::new(move || {
        changed.0.lock().expect("fixture").authority_revision = "revoked".into()
    }));
    let service = FileQueryService::new(authority, Arc::new(f.application));
    let access = FileQueryAccess {
        actor: f.context.actor,
        conversation_id: f.context.conversation_id,
        workspace_id: None,
        run: None,
    };
    assert_eq!(
        service.read(&access, read(reference)).await.err(),
        Some(FileQueryError::Denied)
    );
}

#[tokio::test]
async fn a_different_home_never_reads_an_ambient_cloud_project() {
    let (state, mut access, authority) = fixture();
    access.run = None;
    state
        .conversation
        .lock()
        .expect("lock")
        .conversation
        .control_home = RuntimeHome::Device {
        device_id: DeviceId::generate(),
    };
    assert_eq!(
        authority
            .resolve(&access)
            .await
            .expect_err("route to actual Home"),
        FileQueryError::Unavailable
    );
}

struct WorkspaceAuthority {
    context: FileQueryContext,
}
impl WorkspaceFileQueryAuthority for WorkspaceAuthority {
    fn resolve_workspace<'a>(
        &'a self,
        actor: Actor,
        workspace: WorkspaceId,
        _: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async move {
            if actor != self.context.actor || Some(workspace) != self.context.project_workspace_id {
                return Err(FileQueryError::Denied);
            }
            Ok(self.context.clone())
        })
    }
}
#[tokio::test]
async fn standalone_workspace_access_requires_its_own_authority_and_cannot_carry_a_run() {
    let (_, mut access, authority) = fixture();
    let mut context = authority.resolve(&access).await.expect("fixture authority");
    context.conversation_id = None;
    access.workspace_id = context.project_workspace_id;
    let authority = authority.with_workspace_authority(Arc::new(WorkspaceAuthority {
        context: context.clone(),
    }));
    assert_eq!(
        authority.resolve(&access).await.err(),
        Some(FileQueryError::Denied)
    );
    access.conversation_id = None;
    assert_eq!(
        authority.resolve(&access).await.err(),
        Some(FileQueryError::Denied)
    );
    access.run = None;
    let current = authority
        .resolve(&access)
        .await
        .expect("workspace authority");
    assert!(current.conversation_id.is_none());
    assert_eq!(current.project_workspace_id, context.project_workspace_id);
    access.actor.principal_id = PrincipalId::generate();
    assert_eq!(
        authority.resolve(&access).await.err(),
        Some(FileQueryError::Denied)
    );
}

#[tokio::test]
async fn later_authorized_run_preserves_view_revision_but_old_attempt_is_rejected() {
    let (state, mut access, authority) = fixture();
    let first = authority.resolve(&access).await.expect("first run");
    let old_access = access.clone();
    let run = access.run.as_mut().expect("run");
    run.context.run_id = RunId::generate();
    run.context.runtime_snapshot_id = RuntimeSnapshotId::generate();
    run.attempt.run_id = run.context.run_id;
    run.attempt.attempt_id = AttemptId::generate();
    run.attempt.lease_id = LeaseId::generate();
    {
        let mut current = state.run.lock().expect("fixture");
        current.run.context = run.context;
        current.active_attempt = Some(run.attempt.clone());
    }
    let second = authority.resolve(&access).await.expect("later run");
    assert_eq!(first.authority_revision, second.authority_revision);
    assert_eq!(first.project_workspace_id, second.project_workspace_id);
    assert_eq!(
        authority.resolve(&old_access).await.err(),
        Some(FileQueryError::Denied)
    );
}

#[tokio::test]
async fn activity_file_access_has_no_implicit_lifetime_but_keeps_original_authority() {
    use musteragent_runtime_contract::execution_liveness::{
        ActivityExecutionTiming, BusinessDeadline, InvocationExecutionTiming, LivenessPolicyRef,
    };
    let (state, mut access, authority) = fixture();
    let run = access.run.as_mut().expect("run");
    run.timing = InvocationExecutionTiming::LegacyFixed {
        deadline: UtcTimestamp::from_unix_millis(99),
    };
    assert!(matches!(
        authority.resolve(&access).await,
        Err(FileQueryError::Denied)
    ));
    let activity = ActivityExecutionTiming {
        policy: LivenessPolicyRef {
            key: VersionedKey::new("files.activity@1").expect("key"),
            digest: musteragent_content_ref::Sha256::digest(b"sealed policy"),
        },
        business_deadline: None,
    };
    access.run.as_mut().expect("run").timing =
        InvocationExecutionTiming::ActivityV1(activity.clone());
    authority
        .resolve(&access)
        .await
        .expect("live original authority without wall deadline");
    let mut expired = activity.clone();
    expired.business_deadline = Some(BusinessDeadline {
        at: UtcTimestamp::from_unix_millis(100),
        source: VersionedKey::new("job.deadline@1").expect("source"),
    });
    access.run.as_mut().expect("run").timing = InvocationExecutionTiming::ActivityV1(expired);
    assert!(matches!(
        authority.resolve(&access).await,
        Err(FileQueryError::Denied)
    ));
    access.run.as_mut().expect("run").timing = InvocationExecutionTiming::ActivityV1(activity);
    state
        .run
        .lock()
        .expect("state")
        .active_attempt
        .as_mut()
        .expect("attempt")
        .lease_expires_at = UtcTimestamp::from_unix_millis(100);
    assert!(
        matches!(
            authority.resolve(&access).await,
            Err(FileQueryError::Denied)
        ),
        "activity cannot renew a lease"
    );
    state
        .run
        .lock()
        .expect("state")
        .active_attempt
        .as_mut()
        .expect("attempt")
        .lease_expires_at = UtcTimestamp::from_unix_millis(200);
    state.run.lock().expect("state").run.cancel_requested_at =
        Some(UtcTimestamp::from_unix_millis(100));
    assert!(
        matches!(
            authority.resolve(&access).await,
            Err(FileQueryError::Denied)
        ),
        "activity cannot override cancellation"
    );
}
