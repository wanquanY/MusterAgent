//! Native signed transport is an adapter of the original WorkItem authority.
//! The resource receipt and all Task effects commit in its one transaction.
use super::*;
use crate::agent_resource::device_custody::DeviceCustody;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_resource::{ResourceCommandContext, ResourceCommandSource};
use std::sync::Arc;
mod handoff;
mod query;
mod result_submission;
pub(crate) use result_submission::stage_message as stage_result_message;

pub(crate) struct NativeWorkItemAuthority<'a> {
    pub(crate) actor: Actor,
    pub(crate) custody: &'a DeviceCustody,
    pub(crate) authenticated_origin: &'a OriginDeviceExecutionTarget,
}

type Error = DeviceResourceError;

/// Before the original reservation is committed, distinguish a changing policy
/// checkpoint from a new physical destination, consent, or membership.
fn check_prepared_target(
    prepared: &WorkItemExecutionTarget,
    current: &WorkItemExecutionTarget,
) -> Result<(), Error> {
    if prepared == current {
        return Ok(());
    }
    if let (WorkItemExecutionTarget::RemoteDevice(old), WorkItemExecutionTarget::RemoteDevice(new)) =
        (prepared, current)
        && old.device_id == new.device_id
        && old.installation_id == new.installation_id
        && old.membership_id == new.membership_id
        && old.membership_epoch == new.membership_epoch
        && old.consent_id == new.consent_id
        && old.consent_sha256 == new.consent_sha256
    {
        return Err(Error::TargetPolicyChanged);
    }
    Err(Error::Conflict)
}

pub(crate) struct PreparedNativeWorkItem {
    actor: Actor,
    command: DeviceResourceCommand,
    source: Option<WorkItemStartSource>,
    scope: OwnedAgentWorkItemScope,
    effect: Effect,
    bytes: Vec<u8>,
    authorized: Vec<ContentRef>,
}
enum Effect {
    Read,
    Submission(Box<result_submission::Prepared>),
    Start {
        input: Box<WorkItemStartInput>,
        goal: ContentRef,
        partner: Option<ContentRef>,
        target: WorkItemExecutionTarget,
        current_origin: Option<Box<OriginDeviceExecutionTarget>>,
    },
    Mutate {
        mutation: WorkItemMutation,
        command_id: CommandId,
        goal: Option<ContentRef>,
    },
    Handoff(WorkItemHandoffMaterial),
}
impl PreparedNativeWorkItem {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) async fn stage_submitted_result(
        &self,
        content: &dyn ContentStore,
    ) -> Result<Option<ContentRef>, Error> {
        match &self.effect {
            Effect::Submission(plan) => plan.stage_content(self.actor, content).await.map(Some),
            _ => Ok(None),
        }
    }
    pub(crate) fn authorized_content(&self) -> &[ContentRef] {
        &self.authorized
    }
}
fn error(e: DialogueStoreError) -> Error {
    match e {
        DialogueStoreError::Infrastructure => Error::Infrastructure,
        DialogueStoreError::Corrupt => Error::Corrupt,
        DialogueStoreError::RevisionConflict | DialogueStoreError::IdempotencyConflict => {
            Error::Conflict
        }
        _ => Error::Forbidden,
    }
}
fn invocation(
    actor: Actor,
    agent: AgentId,
    command: &DeviceResourceCommand,
) -> ResolveWorkItemStart {
    let i = command.invocation();
    ResolveWorkItemStart {
        actor,
        requesting_agent_id: agent,
        run_context: *command.run_context(),
        invocation_id: i.invocation_id,
        tool_call_id: i.tool_call_id.clone(),
        request_ref: i.request_ref.clone(),
    }
}
fn require_context(
    context: &ResourceCommandContext,
    actor: Actor,
    command: &DeviceResourceCommand,
) -> Result<(), Error> {
    let b = command.admission();
    let ResourceCommandSource::DeviceExecution {
        run_id,
        intent_id,
        device_id,
        installation_id,
        source_digest,
        snapshot_hash,
        grant_proof_hash,
        command_commitment,
        ..
    } = context.source
    else {
        return Err(Error::Forbidden);
    };
    if context.actor != actor
        || context.tenant_id != actor.tenant_id
        || context.invocation_id != Some(command.invocation().invocation_id)
        || context.tool_key.as_ref().map(|k| k.as_str())
            != Some(command.invocation().tool_key.as_str())
        || run_id != b.run_id
        || intent_id != b.intent_id
        || device_id != b.device_id
        || installation_id != b.installation_id
        || source_digest != b.source_digest
        || snapshot_hash != b.snapshot_hash
        || grant_proof_hash != b.grant_proof_hash
        || command_commitment != command.commitment()
    {
        return Err(Error::Forbidden);
    }
    Ok(())
}
async fn stage(
    content: &dyn ContentStore,
    actor: Actor,
    id: ContentId,
    media: &str,
    bytes: Vec<u8>,
) -> Result<ContentRef, Error> {
    content
        .put_at(
            id,
            PutContent {
                tenant_id: actor.tenant_id,
                media_type: MediaType::new(media).map_err(|_| Error::Corrupt)?,
                bytes,
            },
        )
        .await
        .map_err(|_| Error::Infrastructure)
}

pub(crate) async fn prepare(
    tx: &mut PgConnection,
    NativeWorkItemAuthority {
        actor,
        custody,
        authenticated_origin,
    }: NativeWorkItemAuthority<'_>,
    context: &ResourceCommandContext,
    command: &DeviceResourceCommand,
    content: Arc<dyn ContentStore>,
    targets: Option<&dyn WorkItemDeviceTargetAuthority>,
    result_message: Option<&ContentRef>,
) -> Result<PreparedNativeWorkItem, Error> {
    require_context(context, actor, command)?;
    let (scope, source) = match custody {
        DeviceCustody::Intent(p) => {
            let scope = OwnedAgentWorkItemScope {
                owner_id: actor.principal_id,
                space_id: p.agent.space_id,
                agent_id: p.scope.agent_id,
            };
            let source = WorkItemStartSource {
                actor,
                scope,
                source_dialogue_id: p.scope.dialogue_id,
                source_intent_id: p.intent.intent_id,
                source_run_id: command.admission().run_id,
                source_invocation_id: command.invocation().invocation_id,
                source_snapshot_id: command.run_context().runtime_snapshot_id,
            };
            (scope, Some(source))
        }
        DeviceCustody::WorkItem(p) => (p.preparation().key.scope, None),
    };
    let key = command.invocation().tool_key.as_str();
    if matches!(key, "tasks.list@1" | "tasks.inspect@1") {
        let result = query::read(tx, actor, custody, command)
            .await
            .map_err(error)?;
        let authorized = query::references(&result);
        return Ok(PreparedNativeWorkItem {
            actor,
            command: command.clone(),
            source,
            scope,
            effect: Effect::Read,
            bytes: work_item_query_result_bytes(&result),
            authorized,
        });
    }
    if key == WORK_ITEM_RESULT_SUBMIT_TOOL {
        let DeviceCustody::WorkItem(task) = custody else {
            return Err(Error::Forbidden);
        };
        let result = result_submission::prepare(tx, command, task, result_message).await?;
        let bytes = result
            .material
            .manifest
            .receipt()
            .map_err(error)?
            .canonical_bytes();
        return Ok(PreparedNativeWorkItem {
            actor,
            command: command.clone(),
            source: None,
            scope,
            effect: Effect::Submission(Box::new(result)),
            bytes,
            authorized: vec![],
        });
    }
    let s = source.as_ref().ok_or(Error::Forbidden)?;
    let root = crate::agent_dialogue::capability::verified_root_id(
        tx,
        actor,
        scope.agent_id,
        s.source_intent_id,
    )
    .await
    .map_err(error)?;
    if !root.is_direct_user_root {
        return Err(Error::Forbidden);
    }
    let (effect, bytes) = match key {
        "tasks.start@3" | "tasks.start@4" => {
            if input_actions::target_read(tx, s)
                .await
                .map_err(error)?
                .is_some()
            {
                return Err(Error::Forbidden);
            }
            let input = parse_work_item_start_for(key, command.request_bytes()).map_err(error)?;
            let executor = input.executing_agent_id.unwrap_or(scope.agent_id);
            let current_origin = (input.placement == WorkItemStartPlacement::CurrentDevice)
                .then(|| Box::new(authenticated_origin.clone()));
            let target =
                super::start::placement::resolve(tx, s, &input, current_origin.as_deref(), targets)
                    .await
                    .map_err(error)?;
            let goal = stage(
                content.as_ref(),
                actor,
                work_item_goal_content_id(s.work_item_id(), s.command_id()),
                WORK_ITEM_GOAL_MEDIA_TYPE,
                work_item_goal_bytes(&input.spec),
            )
            .await?;
            let partner = if executor != scope.agent_id {
                Some(
                    stage(
                        content.as_ref(),
                        actor,
                        work_item_partner_input_id(s.source_invocation_id),
                        "text/plain",
                        work_item_partner_input_bytes(&input.spec),
                    )
                    .await?,
                )
            } else {
                None
            };
            let bytes = work_item_start_result_bytes(s, &input, &command.invocation().tool_call_id);
            (
                Effect::Start {
                    input: Box::new(input),
                    goal,
                    partner,
                    target,
                    current_origin,
                },
                bytes,
            )
        }
        "tasks.control@1" | "tasks.amend@2" => {
            let mutation = parse_work_item_mutation(key, command.request_bytes()).map_err(error)?;
            let command_id = input_actions::prepare_read(tx, s, &mutation)
                .await
                .map_err(error)?;
            let goal = match &mutation {
                WorkItemMutation::Amend { spec, .. } => Some(
                    stage(
                        content.as_ref(),
                        actor,
                        work_item_goal_content_id(mutation.task_id(), command_id),
                        WORK_ITEM_GOAL_MEDIA_TYPE,
                        work_item_goal_bytes(spec),
                    )
                    .await?,
                ),
                _ => None,
            };
            let bytes = work_item_mutation_result_bytes(&mutation).map_err(error)?;
            (
                Effect::Mutate {
                    mutation,
                    command_id,
                    goal,
                },
                bytes,
            )
        }
        "tasks.handoff@1" => {
            if input_actions::target_read(tx, s)
                .await
                .map_err(error)?
                .is_some()
            {
                return Err(Error::Forbidden);
            }
            let material = handoff::load(
                tx,
                s,
                &WorkItemHandoffInput::parse(command.request_bytes()).map_err(error)?,
            )
            .await
            .map_err(error)?;
            let bytes = material.result_bytes();
            (Effect::Handoff(material), bytes)
        }
        // Reports are admitted by the existing Cloud report owner. Accepted
        // WorkItemResult replies are likewise formally placed in its coordinator.
        // A Native intent cannot impersonate either authority just by tool name.
        "tasks.verify@2" | "tasks.verify@3" | "tasks.followup@1" => return Err(Error::Forbidden),
        _ => return Err(Error::InvalidArguments),
    };
    Ok(PreparedNativeWorkItem {
        actor,
        command: command.clone(),
        source,
        scope,
        effect,
        bytes,
        authorized: vec![],
    })
}

/// Before custody/Agent SHARE locks: the same account -> ordered dialogues ->
/// ordered Agent lock owner as Cloud creation/amendment. Never upgrade a lock.
pub(crate) async fn lock_prepared(
    tx: &mut PgConnection,
    context: &ResourceCommandContext,
    plan: &PreparedNativeWorkItem,
) -> Result<(), Error> {
    require_context(context, plan.actor, &plan.command)?;
    if matches!(&plan.effect, Effect::Read) {
        return Ok(());
    }
    let target = match &plan.effect {
        // Acquire the original Task owner locks before Device custody. This
        // includes the exclusive space lock and avoids an upgrade after staging.
        Effect::Submission(prepared) => {
            return super::result_receipts::lock_current(tx, &prepared.material.manifest.scope)
                .await
                .map(|_| ())
                .map_err(error);
        }
        Effect::Start { input, .. } => input.executing_agent_id.unwrap_or(plan.scope.agent_id),
        _ => plan.scope.agent_id,
    };
    preparation::lock_agents(tx, plan.actor, plan.scope, target)
        .await
        .map_err(error)
}

pub(crate) async fn commit(
    tx: &mut PgConnection,
    context: &ResourceCommandContext,
    plan: PreparedNativeWorkItem,
    result: CapabilityResult,
    targets: Option<&dyn WorkItemDeviceTargetAuthority>,
) -> Result<(CapabilityResult, Vec<ContentRef>), Error> {
    require_context(context, plan.actor, &plan.command)?;
    if result.result_hash != Sha256::digest(&plan.bytes)
        || result.result_ref.sha256 != result.result_hash
        || result.result_ref.byte_length != plan.bytes.len() as u64
        || result.result_schema != plan.command.invocation().result_schema
    {
        return Err(Error::Corrupt);
    }
    if matches!(&plan.effect, Effect::Read) {
        validate_read(tx, plan.actor, &plan.command, &plan.bytes, &plan.authorized).await?;
        return Ok((result, plan.authorized));
    }
    if let Effect::Submission(prepared) = &plan.effect {
        result_submission::commit(tx, &plan.command, prepared, &result.result_ref, context.now)
            .await?;
        return Ok((result, vec![]));
    }
    let s = plan.source.as_ref().ok_or(Error::Forbidden)?;
    let root = crate::agent_dialogue::capability::verified_root_id(
        tx,
        plan.actor,
        plan.scope.agent_id,
        s.source_intent_id,
    )
    .await
    .map_err(error)?;
    if !root.is_direct_user_root {
        return Err(Error::Forbidden);
    }
    persist_source(tx, plan.actor, &plan.command)
        .await
        .map_err(error)?;
    let retained = super::native_source::load(tx, plan.actor, s.source_invocation_id)
        .await
        .map_err(error)?
        .ok_or(Error::Corrupt)?;
    if retained.source != *s {
        return Err(Error::Corrupt);
    }
    super::native_source::require_current(tx, &retained)
        .await
        .map_err(error)?;
    lock_ready_content_reference(
        tx,
        plan.actor.tenant_id,
        &plan.command.invocation().request_ref,
    )
    .await
    .map_err(error)?;
    let i = invocation(plan.actor, plan.scope.agent_id, &plan.command);
    match plan.effect {
        Effect::Read | Effect::Submission(_) => unreachable!(),
        Effect::Start {
            input,
            goal,
            partner,
            target,
            current_origin,
        } => {
            if input_actions::target(tx, s).await.map_err(error)?.is_some() {
                return Err(Error::Forbidden);
            }
            let current =
                super::start::placement::resolve(tx, s, &input, current_origin.as_deref(), targets)
                    .await
                    .map_err(error)?;
            check_prepared_target(&target, &current)?;
            let request = CommitWorkItemStart {
                original_tool: plan.command.invocation().tool_key.clone(),
                source: s.clone(),
                invocation: i,
                request_bytes: plan.command.request_bytes().to_vec(),
                goal,
                result: result.result_ref.clone(),
                partner_input: partner,
                result_schema: result.result_schema.clone(),
                committed_at: context.now,
            };
            // Native result identity is owned by the original resource receipt;
            // shared reservation receives the same validated parser and source.
            let reserved = start_reservation::reserve(tx, &request, &input, &target)
                .await
                .map_err(error)?;
            // A task explicitly assigned to the same physical Native host keeps
            // that source's admitted directory; another host cannot inherit it.
            if let WorkItemExecutionTarget::RemoteDevice(device) = &target {
                let same_origin = input.placement == WorkItemStartPlacement::CurrentDevice
                    || (input.coordinator_delivery.is_none()
                        && device.device_id == plan.command.admission().device_id
                        && device.installation_id == plan.command.admission().installation_id);
                if same_origin {
                    sqlx::query("UPDATE collaboration.work_item_execution_reservations SET device_placement_captured=TRUE,device_workspace_id=$2 WHERE work_item_id=$1 AND goal_revision=1 AND control_revision=1 AND assignment_epoch=1 AND NOT device_placement_captured")
                        .bind(reserved.work_item_id.into_uuid()).bind(retained.preparation.device_workspace_id.map(WorkspaceId::into_uuid))
                        .execute(&mut *tx).await.map_err(|e|error(db(e)))?;
                }
            }
            crate::agent_work_creations::record_device(
                tx,
                crate::agent_work_creations::Creation {
                    actor: plan.actor,
                    reference: AgentWorkReference {
                        kind: AgentWorkKind::WorkItem,
                        id: s.work_item_id().into_uuid(),
                        agent_id: plan.scope.agent_id,
                    },
                    run_id: s.source_run_id,
                    invocation_id: s.source_invocation_id,
                    revision: 1,
                    title: &input.spec.objective,
                    created_at: context.now,
                },
            )
            .await
            .map_err(error)?;
        }
        Effect::Mutate {
            mutation,
            command_id,
            goal,
        } => {
            let c = CommitWorkItemMutation {
                command_id,
                invocation: i,
                command: mutation,
                request_bytes: plan.command.request_bytes().to_vec(),
                goal,
                result: result.result_ref.clone(),
                result_schema: result.result_schema.clone(),
                changed_at: context.now,
            };
            mutations::apply(tx, s, &c).await.map_err(error)?;
        }
        Effect::Handoff(material) => {
            let current = handoff::load(
                tx,
                s,
                &WorkItemHandoffInput::parse(plan.command.request_bytes()).map_err(error)?,
            )
            .await
            .map_err(error)?;
            if current != material {
                return Err(Error::Conflict);
            }
        }
    }
    Ok((result, vec![]))
}

pub(crate) async fn validate_read(
    tx: &mut PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
    result: &[u8],
    authorized: &[ContentRef],
) -> Result<(), Error> {
    query::validate(tx, actor, command, result, authorized)
        .await
        .map_err(error)
}
async fn persist_source(
    tx: &mut PgConnection,
    actor: Actor,
    command: &DeviceResourceCommand,
) -> Result<(), DialogueStoreError> {
    let b = command.admission();
    sqlx::query("INSERT INTO collaboration.work_item_native_sources(invocation_id,snapshot_id,source_digest,snapshot_hash,grant_proof_hash)
        SELECT $1,$2,$3,$4,$5 FROM resource.device_commands c WHERE c.invocation_id=$1 AND c.tenant_id=$6 AND c.owner_id=$7 AND c.command_commitment=$8 AND c.admission_kind='intent'")
        .bind(command.invocation().invocation_id.into_uuid()).bind(command.run_context().runtime_snapshot_id.into_uuid())
        .bind(b.source_digest.into_bytes().to_vec()).bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec())
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.commitment().into_bytes().to_vec())
        .execute(tx).await.map_err(db)?;
    Ok(())
}

#[cfg(test)]
mod selection_tests {
    use super::*;

    #[test]
    fn reselect_only_the_same_device_and_original_consent_before_acceptance() {
        let target = WorkItemDeviceTarget {
            device_id: DeviceId::generate(),
            installation_id: DeviceInstallationId::generate(),
            membership_id: uuid::Uuid::now_v7(),
            membership_epoch: 1,
            consent_id: uuid::Uuid::now_v7(),
            consent_sha256: Sha256::digest(b"consent"),
            consent_anchor: None,
        };
        let old = WorkItemExecutionTarget::RemoteDevice(target.clone());
        let mut new = target;
        new.consent_anchor = Some(WorkItemDeviceConsentAnchor {
            authority_ref: "authority".into(),
            policy_ref: "policy".into(),
            effective_capabilities_sha256: Sha256::digest(b"fresh ceiling"),
        });
        let selected = WorkItemExecutionTarget::RemoteDevice(new.clone());
        assert_eq!(check_prepared_target(&old, &old), Ok(()));
        assert_eq!(check_prepared_target(&selected, &selected), Ok(()));
        assert_eq!(
            check_prepared_target(&old, &selected),
            Err(Error::TargetPolicyChanged)
        );
        new.consent_anchor.as_mut().unwrap().policy_ref.push('2');
        assert_eq!(
            check_prepared_target(
                &selected,
                &WorkItemExecutionTarget::RemoteDevice(new.clone())
            ),
            Err(Error::TargetPolicyChanged)
        );
        new.consent_id = uuid::Uuid::now_v7();
        assert_eq!(
            check_prepared_target(&selected, &WorkItemExecutionTarget::RemoteDevice(new)),
            Err(Error::Conflict)
        );
        assert_eq!(
            check_prepared_target(&selected, &WorkItemExecutionTarget::Cloud),
            Err(Error::Conflict)
        );
    }
}
