//! Native transport for the same account Goal planner and creation owner.
//! Admission custody and the original device command transaction own authority.
use super::*;
use crate::owner_goal_tools::{
    creation, effect,
    planning::{GoalToolPlanner, GoalToolScope, PreparedGoalTool},
    sources,
};
use musteragent_runtime_capability::CapabilityResult;

pub(super) enum Prepared {
    Create {
        request: CreateOwnerGoalDraft,
        receipt: OwnerGoalDraftReceipt,
        run: RunId,
        invocation: InvocationId,
    },
    Sources(sources::PreparedSourceTool),
    Manage {
        scope: GoalToolScope,
        plan: Box<PreparedGoalTool>,
        custody: effect::Custody,
    },
}
impl Prepared {
    pub fn authorized_content(&self) -> &[ContentRef] {
        match self {
            Self::Create { .. } | Self::Sources(_) => &[],
            Self::Manage { plan, .. } => &plan.authorized_content,
        }
    }
}

pub(super) async fn prepare(
    store: &PostgresDeviceResourceStore,
    auth: Arc<authority::Authority>,
) -> Result<prepare::Prepared, Error> {
    let (mut tx, context, custody) = auth.current_snapshot().await?;
    let super::super::device_custody::DeviceCustody::Intent(source) = custody else {
        return Err(Error::Forbidden);
    };
    let command = auth.request.command();
    let inv = command.invocation();
    if (!command.operation().is_read_only() || command.operation().is_goal_source())
        && source.intent.origin.surface
            == musteragent_execution_context::DialogueInputSurface::Schedule
    {
        return Err(Error::Forbidden);
    }
    if command.operation() == DeviceResourceOperation::GoalDraftSave {
        let original = crate::agent_dialogue::capability::verified_root_id(
            &mut tx,
            auth.actor,
            source.scope.agent_id,
            source.intent.intent_id,
        )
        .await
        .map_err(store_error)?;
        crate::owner_goal_tools::draft_registration::require_direct_input(
            original.is_direct_user_root,
            source.intent.task_target.is_some(),
        )
        .map_err(error)?;
    }
    let scope = GoalToolScope {
        actor: auth.actor,
        agent_id: source.scope.agent_id,
        own_goal: None,
    };
    let (plan, value) = if command.operation() == DeviceResourceOperation::GoalCreate {
        let prepared = creation::prepare(
            &mut tx,
            scope.actor,
            scope.agent_id,
            source.intent.intent_id,
            source.intent.dialogue_id,
            command.request_bytes(),
            context.now,
        )
        .await
        .map_err(error)?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        prepared
            .stage(store.content.as_ref())
            .await
            .map_err(|e| match e {
                OwnerGoalCreationError::Invalid => Error::InvalidArguments,
                OwnerGoalCreationError::Source(e) => store_error(e),
                OwnerGoalCreationError::Content(_) => Error::Infrastructure,
            })?;
        let receipt = crate::PostgresStandingGoalProductStore::new(store.database.clone())
            .read_draft_receipt(scope.actor, prepared.request.command_id)
            .await
            .map_err(store_error)?
            .unwrap_or(OwnerGoalDraftReceipt {
                command_id: prepared.request.command_id,
                binding: prepared.request.binding.clone(),
                created_at: prepared.request.goal.created_at,
            });
        let value = creation::result(
            &receipt,
            prepared.request.goal.goal_id,
            &prepared.request.goal.title,
        );
        (
            Prepared::Create {
                request: prepared.request,
                receipt,
                run: command.run_context().run_id,
                invocation: inv.invocation_id,
            },
            value,
        )
    } else if command.operation().is_goal_source() {
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        let plan = sources::SourcePlanner {
            database: store.database.clone(),
            selection: store.source_selection.clone(),
        }
        .prepare(
            sources::SourceScope {
                subject: owner_event_sources::OwnerGoalSourceSubject {
                    actor: scope.actor,
                    agent_id: scope.agent_id,
                    surface: source.intent.origin.surface.clone(),
                },
                intent_id: source.intent.intent_id,
            },
            inv.tool_key.as_str(),
            command.request_bytes(),
            context.now,
        )
        .await
        .map_err(error)?;
        let value = plan.value.clone();
        (Prepared::Sources(plan), value)
    } else {
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        let plan = GoalToolPlanner {
            database: store.database.clone(),
            content: store.content.clone(),
        }
        .prepare(
            scope,
            inv.tool_key.as_str(),
            command.request_bytes(),
            context.now,
        )
        .await
        .map_err(error)?;
        let value = plan.value.clone();
        (
            Prepared::Manage {
                scope,
                plan: Box::new(plan),
                custody: effect::Custody {
                    run_id: command.run_context().run_id,
                    invocation_id: inv.invocation_id,
                    device: true,
                },
            },
            value,
        )
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| Error::Corrupt)?;
    let reference = store
        .stage(auth.actor, command, b"owner-goal-result@1", &bytes)
        .await?;
    Ok(prepare::Prepared::Goal(
        Box::new(plan),
        CapabilityResult {
            result_hash: reference.sha256,
            result_ref: reference,
            result_schema: inv.result_schema.clone(),
            provider_request_ref: None,
        },
    ))
}

pub(super) async fn commit(
    conn: &mut sqlx::PgConnection,
    prepared: Prepared,
    result: CapabilityResult,
) -> Result<(CapabilityResult, Vec<ContentRef>), Error> {
    let refs = match prepared {
        Prepared::Create {
            request,
            receipt,
            run,
            invocation,
        } => {
            let actor = request.actor;
            let reference = AgentWorkReference {
                kind: AgentWorkKind::Goal,
                id: request.goal.goal_id.into_uuid(),
                agent_id: request.goal.target_agent_id,
            };
            let title = request.goal.title.clone();
            let committed =
                crate::standing_goal::owner_goal::creation::create_in_transaction(conn, request)
                    .await
                    .map_err(store_error)?;
            if committed != receipt {
                return Err(Error::Conflict);
            }
            let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.agent_work_creation_publications WHERE tenant_id=$1 AND owner_id=$2 AND work_kind=3 AND work_id=$3)")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(reference.id).fetch_one(&mut *conn).await.map_err(db)?;
            if !recorded {
                crate::agent_work_creations::record_device(
                    conn,
                    crate::agent_work_creations::Creation {
                        actor,
                        reference,
                        run_id: run,
                        invocation_id: invocation,
                        revision: 1,
                        title: &title,
                        created_at: receipt.created_at,
                    },
                )
                .await
                .map_err(store_error)?;
            }
            Vec::new()
        }
        Prepared::Sources(plan) => {
            sources::commit(conn, plan).await.map_err(error)?;
            Vec::new()
        }
        Prepared::Manage {
            scope,
            plan,
            custody,
        } => {
            if let Some(planned) = plan.effect {
                effect::commit(conn, scope, planned, custody)
                    .await
                    .map_err(error)?;
            }
            plan.authorized_content
        }
    };
    Ok((result, refs))
}

pub(super) async fn validate_read(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
    refs: &[ContentRef],
) -> Result<(), Error> {
    for reference in refs {
        let visible: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.standing_goals g JOIN agent.dialogue_sessions s ON s.source_goal_id=g.goal_id AND s.conversation_id=g.conversation_id AND s.subject_kind='goal' JOIN resource.device_commands c ON c.invocation_id=$1 AND c.agent_id=g.target_agent_id WHERE g.tenant_id=$2 AND g.owner_principal_id=$3 AND g.objective_content_id=$4 AND s.agent_id=c.agent_id AND s.owner_id=c.owner_id AND s.tenant_id=c.tenant_id AND NOT EXISTS(SELECT 1 FROM runtime.goal_deletions d WHERE d.tenant_id=g.tenant_id AND d.goal_id=g.goal_id))")
            .bind(command.invocation().invocation_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(reference.content_id.into_uuid()).fetch_one(&mut *conn).await.map_err(db)?;
        if !visible {
            return Err(Error::ReferenceUnavailable);
        }
    }
    Ok(())
}
fn error(e: OwnerGoalToolError) -> Error {
    match e {
        OwnerGoalToolError::Forbidden => Error::Forbidden,
        OwnerGoalToolError::InvalidArguments => Error::InvalidArguments,
        OwnerGoalToolError::Conflict | OwnerGoalToolError::SourceRevisionConflict => {
            Error::Conflict
        }
        OwnerGoalToolError::StaleFence => Error::StaleFence,
        OwnerGoalToolError::Infrastructure => Error::Infrastructure,
        OwnerGoalToolError::Corrupt => Error::Corrupt,
    }
}
fn store_error(e: DialogueStoreError) -> Error {
    match e {
        DialogueStoreError::RevisionConflict | DialogueStoreError::IdempotencyConflict => {
            Error::Conflict
        }
        DialogueStoreError::Unavailable | DialogueStoreError::Inactive => Error::Forbidden,
        DialogueStoreError::Corrupt => Error::Corrupt,
        _ => Error::Infrastructure,
    }
}
