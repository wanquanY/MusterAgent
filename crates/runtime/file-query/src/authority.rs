//! Current access is resolved by the original Conversation and Run owners.
//! These inputs are internal; neither the model nor Product wire can choose
//! a permission revision, delegated allowlist or another execution root.
use std::sync::Arc;

use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_conversation::{ConversationApplication, GetConversation};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{Actor, ConversationId, UtcTimestamp};
use musteragent_run::{AttemptContext, GetRun, RunApplication, RunContext};
use musteragent_runtime_capability::RuntimeClock;
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode, RuntimeError};

use crate::{FileQueryContext, FileQueryError};

#[derive(Clone, Debug)]
pub struct RunFileQueryAccess {
    pub invocation_id: Option<musteragent_identity::InvocationId>,
    pub context: RunContext,
    pub attempt: AttemptContext,
    pub authorized_content: Vec<ContentRef>,
    pub timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
}

#[derive(Clone, Debug)]
pub struct FileQueryAccess {
    pub actor: Actor,
    pub conversation_id: Option<ConversationId>,
    pub workspace_id: Option<musteragent_identity::WorkspaceId>,
    /// Supplied only after the sealed invocation's capability authorization.
    pub run: Option<RunFileQueryAccess>,
}

/// Standalone Product project access, implemented by the original placement
/// authority. This never manufactures a Conversation or a Run grant.
pub trait WorkspaceFileQueryAuthority: Send + Sync {
    fn resolve_workspace<'a>(
        &'a self,
        actor: Actor,
        workspace: musteragent_identity::WorkspaceId,
        now: UtcTimestamp,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>>;
}

pub trait FileQueryAuthority: Send + Sync {
    fn resolve<'a>(
        &'a self,
        access: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>>;
}

/// Read-only ports implemented by the existing Conversation / Run persistence
/// owners. Workers do not need to instantiate the command application.
pub trait FileQueryStateSource: Send + Sync {
    fn conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<musteragent_conversation::ConversationView, RuntimeError>>;
    fn run(
        &self,
        query: GetRun,
    ) -> ApplicationFuture<'_, Result<musteragent_run::RunView, RuntimeError>>;
}
struct ApplicationStateSource {
    conversations: Arc<dyn ConversationApplication>,
    runs: Arc<dyn RunApplication>,
}
impl FileQueryStateSource for ApplicationStateSource {
    fn conversation(
        &self,
        query: GetConversation,
    ) -> ApplicationFuture<'_, Result<musteragent_conversation::ConversationView, RuntimeError>>
    {
        self.conversations.get_conversation(query)
    }
    fn run(
        &self,
        query: GetRun,
    ) -> ApplicationFuture<'_, Result<musteragent_run::RunView, RuntimeError>> {
        self.runs.get_run(query)
    }
}
pub struct CurrentFileQueryAuthority {
    source: Arc<dyn FileQueryStateSource>,
    clock: Arc<dyn RuntimeClock>,
    home: RuntimeHome,
    workspaces: Option<Arc<dyn WorkspaceFileQueryAuthority>>,
}
impl CurrentFileQueryAuthority {
    #[must_use]
    pub fn new(
        conversations: Arc<dyn ConversationApplication>,
        runs: Arc<dyn RunApplication>,
        clock: Arc<dyn RuntimeClock>,
        home: RuntimeHome,
    ) -> Self {
        Self::from_source(
            Arc::new(ApplicationStateSource {
                conversations,
                runs,
            }),
            clock,
            home,
        )
    }
    #[must_use]
    pub fn from_source(
        source: Arc<dyn FileQueryStateSource>,
        clock: Arc<dyn RuntimeClock>,
        home: RuntimeHome,
    ) -> Self {
        Self {
            source,
            clock,
            home,
            workspaces: None,
        }
    }
    #[must_use]
    pub fn with_workspace_authority(
        mut self,
        authority: Arc<dyn WorkspaceFileQueryAuthority>,
    ) -> Self {
        self.workspaces = Some(authority);
        self
    }
}

impl FileQueryAuthority for CurrentFileQueryAuthority {
    fn resolve<'a>(
        &'a self,
        access: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async move {
            if let Some(workspace) = access.workspace_id {
                if access.conversation_id.is_some() || access.run.is_some() {
                    return Err(FileQueryError::Denied);
                }
                let context = self
                    .workspaces
                    .as_ref()
                    .ok_or(FileQueryError::Unavailable)?
                    .resolve_workspace(access.actor, workspace, self.clock.now())
                    .await?;
                context.validate()?;
                if context.actor != access.actor
                    || context.conversation_id.is_some()
                    || context.project_workspace_id != Some(workspace)
                    || context.content_allowlist.is_some()
                {
                    return Err(FileQueryError::Integrity);
                }
                return Ok(context);
            }
            let conversation_id = access.conversation_id.ok_or(FileQueryError::InvalidQuery)?;
            let view = self
                .source
                .conversation(GetConversation {
                    actor: access.actor,
                    conversation_id,
                })
                .await
                .map_err(query_error)?;
            let conversation = &view.conversation;
            if conversation.tenant_id != access.actor.tenant_id
                || conversation.conversation_id != conversation_id
            {
                return Err(FileQueryError::Integrity);
            }
            // A cloud API cannot query a local/Hosted replica through the
            // managed Resource catalog. Placement is an authority decision.
            if conversation.control_home != self.home {
                return Err(FileQueryError::Unavailable);
            }
            let mut membership = view
                .participants
                .iter()
                .filter(|p| {
                    p.principal_ref == Some(access.actor.principal_id)
                        && p.conversation_id == conversation_id
                        && p.left_at.is_none()
                })
                .map(|p| (p.participant_id.to_string(), p.joined_at.unix_millis()))
                .collect::<Vec<_>>();
            membership.sort();
            if membership.is_empty() {
                return Err(FileQueryError::Denied);
            }
            let now = self.clock.now();
            let mut project_workspace_id = conversation.workspace_id;
            let mut content_allowlist = None;
            if let Some(run_access) = &access.run {
                let current = self
                    .source
                    .run(GetRun {
                        actor: access.actor,
                        run_id: run_access.context.run_id,
                    })
                    .await
                    .map_err(query_error)?;
                let run = &current.run;
                let valid_attempt = current.active_attempt.as_ref().is_some_and(|attempt| {
                    attempt.attempt_id == run_access.attempt.attempt_id
                        && attempt.run_id == run_access.context.run_id
                        && attempt.lease_id == run_access.attempt.lease_id
                        && attempt.fencing_token == run_access.attempt.fencing_token
                        && attempt.attempt_number == run_access.attempt.attempt_number
                        && attempt.lease_expires_at > now
                });
                if run.context != run_access.context
                    || run.context.tenant_id != access.actor.tenant_id
                    || run.context.conversation_id != conversation_id
                    || run.context.control_home != self.home
                    || run.context.home_epoch != conversation.home_epoch
                    || !valid_attempt
                    || run.cancel_requested_at.is_some()
                    || run.state.status().is_terminal()
                    || explicit_timing_expired(&run_access.timing, now)
                {
                    return Err(FileQueryError::Denied);
                }
                project_workspace_id = match run.context.execution_scope {
                    ExecutionScope::Workspace { workspace_id } => Some(workspace_id),
                    ExecutionScope::DevicePrivate { workspace_id, .. } => workspace_id,
                    ExecutionScope::HostedProject { scope } => Some(scope.workspace_id),
                    ExecutionScope::Tenant { .. } => None,
                };
                if project_workspace_id.is_some()
                    && project_workspace_id != conversation.workspace_id
                {
                    return Err(FileQueryError::Denied);
                }
                // A child may only discover its durable content grant. An explicit
                // workspace execution scope remains a separate project permission.
                if run.context.delegation_id.is_some() {
                    content_allowlist = Some(run_access.authorized_content.clone());
                }
            }
            // Execution fences are checked on every request above. They are not
            // the identity of the file view: a later authorized Run may continue
            // the same query. Cursor fingerprints separately bind workspace and
            // delegated content grants, while this revision binds membership/home.
            let revision = serde_json::to_vec(&(
                access.actor.tenant_id.to_string(),
                access.actor.principal_id.to_string(),
                conversation_id.to_string(),
                conversation.home_epoch.get(),
                conversation.workspace_id.map(|id| id.to_string()),
                view.active_context_branch_id.map(|id| id.to_string()),
                membership,
            ))
            .map_err(|_| FileQueryError::Integrity)?;
            Ok(FileQueryContext {
                dialogue_source: None,
                actor: access.actor,
                conversation_id: Some(conversation_id),
                now,
                authority_revision: Sha256::digest(revision).to_string(),
                content_allowlist,
                project_workspace_id,
            })
        })
    }
}

pub(crate) fn query_error(error: RuntimeError) -> FileQueryError {
    match error.code {
        ErrorCode::PolicyDenied
        | ErrorCode::Unauthenticated
        | ErrorCode::ParticipantNotAuthorized => FileQueryError::Denied,
        ErrorCode::ConversationNotFound | ErrorCode::RunNotFound => FileQueryError::NotFound,
        ErrorCode::ContentIntegrityFailed | ErrorCode::InternalInvariantViolated => {
            FileQueryError::Integrity
        }
        _ => FileQueryError::Unavailable,
    }
}

fn explicit_timing_expired(
    timing: &musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming,
    now: UtcTimestamp,
) -> bool {
    use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
    match timing {
        InvocationExecutionTiming::LegacyFixed { deadline } => *deadline <= now,
        InvocationExecutionTiming::ActivityV1(activity) => activity
            .business_deadline
            .as_ref()
            .is_some_and(|deadline| deadline.at <= now),
    }
}
