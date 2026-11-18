//! Private submission from the original admitted Task device. The signed
//! Resource operation owns its transport receipt and shares the result writer
//! with Cloud Task tooling, including item pins and assignment checks.
use super::*;

pub(super) struct Prepared {
    pub material: WorkItemResultSubmissionMaterial,
    pub receipt_ref: ContentRef,
    manifest_ref: ContentRef,
    message: Option<ContentRef>,
}

pub(super) async fn prepare(
    tx: &mut PgConnection,
    command: &DeviceResourceCommand,
    task: &WorkItemDeviceExecutionPreparation,
    message: Option<&ContentRef>,
) -> Result<Prepared, Error> {
    let p = task.preparation();
    let k = &p.key;
    let b = command.admission();
    let inv = command.invocation();
    let input = WorkItemResultSubmissionInput::parse(command.request_bytes())
        .map_err(|_| Error::InvalidArguments)?;
    let run = p.identities().run_id().map_err(error)?;
    let snapshot =
        musteragent_runtime_admission::AdmissionIdentities::derive(p.identities().command_id)
            .ok_or(Error::Corrupt)?
            .runtime_snapshot_id;
    if !p
        .delegation
        .product_release
        .as_ref()
        .map(musteragent_tool::verified_task_results_for_release)
        .transpose()
        .map_err(|_| Error::Forbidden)?
        .unwrap_or(false)
        || k.report_from_run_id.is_some()
        || p.report.is_some()
        || b.run_id != run
        || b.source_digest != task.source_sha256().map_err(error)?
        || command.run_context().runtime_snapshot_id != snapshot
        || input.task_id != k.work_item_id
        || input.goal_revision != k.goal_revision.get()
        || input.control_revision != k.control_revision.get()
        || input.assignment_epoch != k.assignment_epoch.get()
    {
        return Err(Error::Forbidden);
    }
    if command.result_artifact_manifest().is_some_and(|m| {
        m.scope.dialogue_id != p.executing_agent.dialogue_id
            || m.scope.agent_id != k.executing_agent_id
            || m.scope.owner_id != k.actor.principal_id
    }) {
        return Err(Error::Forbidden);
    }
    let scope = WorkItemResultScope {
        tenant_id: k.actor.tenant_id,
        owner_id: k.actor.principal_id,
        work_item_id: k.work_item_id,
        goal_revision: k.goal_revision.get(),
        control_revision: k.control_revision.get(),
        assignment_epoch: k.assignment_epoch.get(),
        producing_run_id: run,
        producing_snapshot_id: snapshot,
        producing_agent_id: k.executing_agent_id,
        requesting_agent_id: k.scope.agent_id,
        source_dialogue_id: p.work_item.source_dialogue_id,
        source_intent_id: p.work_item.source_intent_id,
        producer_home: WorkItemResultHome::Device,
    };
    let invocation = inv.invocation_id;
    input.validate_message(invocation, message).map_err(error)?;
    let material = material(tx, &scope, command, &input, message, false).await?;
    let reference = |content_id, media: &str, bytes: Vec<u8>| -> Result<ContentRef, Error> {
        Ok(ContentRef {
            content_id,
            media_type: MediaType::new(media).map_err(|_| Error::Corrupt)?,
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(&bytes),
        })
    };
    let manifest_ref = reference(
        work_item_result_manifest_id(invocation),
        WORK_ITEM_RESULT_MANIFEST_MEDIA,
        material.manifest.canonical_bytes().map_err(error)?,
    )?;
    let receipt_ref = reference(
        work_item_result_submit_receipt_id(invocation),
        "application/json",
        material
            .manifest
            .receipt()
            .map_err(error)?
            .canonical_bytes(),
    )?;
    Ok(Prepared {
        material,
        receipt_ref,
        manifest_ref,
        message: message.cloned(),
    })
}

pub(super) async fn commit(
    tx: &mut PgConnection,
    command: &DeviceResourceCommand,
    prepared: &Prepared,
    result: &ContentRef,
    at: UtcTimestamp,
) -> Result<(), Error> {
    let inv = command.invocation();
    let input = WorkItemResultSubmissionInput::parse(command.request_bytes())
        .map_err(|_| Error::InvalidArguments)?;
    let scope = &prepared.material.manifest.scope;
    if result != &prepared.receipt_ref
        || command.run_context().run_id != scope.producing_run_id
        || command.run_context().runtime_snapshot_id != scope.producing_snapshot_id
    {
        return Err(Error::Conflict);
    }
    input
        .validate_message(inv.invocation_id, prepared.message.as_ref())
        .map_err(error)?;
    // Resource commit already holds current consent, the original both-Agent
    // locks and the original admitted custody lock. Re-resolve every item now.
    let current = material(tx, scope, command, &input, prepared.message.as_ref(), true).await?;
    if current != prepared.material {
        return Err(Error::Conflict);
    }
    super::super::result_submission::record_submission(
        tx,
        &current,
        &inv.tool_call_id,
        &prepared.manifest_ref,
        result,
        at,
    )
    .await
    .map_err(error)
}

async fn material(
    tx: &mut PgConnection,
    scope: &WorkItemResultScope,
    command: &DeviceResourceCommand,
    input: &WorkItemResultSubmissionInput,
    message: Option<&ContentRef>,
    commit: bool,
) -> Result<WorkItemResultSubmissionMaterial, Error> {
    let invocation = command.invocation().invocation_id;
    if input.delivery.publication.artifacts.is_empty() {
        if command.result_artifact_manifest().is_some() {
            return Err(Error::Forbidden);
        }
        return crate::dialogue_publication::task_result::prepare(
            tx,
            scope,
            invocation,
            input,
            command.invocation().request_ref.sha256,
            message,
        )
        .await
        .map_err(error);
    }
    let exports = command.result_artifact_manifest();
    if let Some(manifest) = exports
        && (manifest.scope.tenant_id != scope.tenant_id
            || manifest.scope.owner_id != scope.owner_id
            || manifest.scope.agent_id != scope.producing_agent_id
            || manifest.scope.conversation_id != command.run_context().conversation_id)
    {
        return Err(Error::Forbidden);
    }
    let mut local = exports.into_iter().flat_map(|m| &m.entries);
    let result_id = work_item_result_id(invocation);
    let mut items = Vec::with_capacity(input.delivery.entry_count());
    if let Some(reference) = message {
        lock_ready_content_reference(tx, scope.tenant_id, reference)
            .await
            .map_err(error)?;
        items.push(WorkItemResultItem {
            item_id: work_item_result_item_id(result_id, 0),
            content_ref: reference.clone(),
            asset: None,
        });
    }
    for locator in &input.delivery.publication.artifacts {
        use musteragent_execution_context::AgentArtifactLocator;
        let (reference, asset) = match locator.artifact {
            AgentArtifactLocator::Artifact(id) => {
                let entry = local.next().ok_or(Error::Forbidden)?;
                if entry.artifact_id != id || entry.producer.tool_call_id != locator.tool_call_id {
                    return Err(Error::Forbidden);
                }
                if commit {
                    crate::artifact::task_result::stage_native_result_asset(
                        tx, scope, command, entry,
                    )
                    .await
                } else {
                    crate::artifact::task_result::prepare_native_result_asset(
                        tx, scope, command, entry,
                    )
                    .await
                }
            }
            AgentArtifactLocator::FileNode(_) => {
                crate::artifact::task_result::prepare_result_asset(
                    tx,
                    scope,
                    crate::artifact::task_result::TaskAssetLocator::Producer(locator),
                )
                .await
            }
        }
        .map_err(error)?;
        items.push(WorkItemResultItem {
            item_id: work_item_result_item_id(result_id, items.len()),
            content_ref: reference,
            asset: Some(asset),
        });
    }
    if local.next().is_some() {
        return Err(Error::Forbidden);
    }
    let manifest = WorkItemResultManifest {
        scope: scope.clone(),
        submission_invocation_id: invocation,
        request_sha256: command.invocation().request_ref.sha256,
        items,
    };
    manifest.validate().map_err(error)?;
    Ok(WorkItemResultSubmissionMaterial { manifest })
}

pub(crate) async fn stage_message(
    actor: Actor,
    command: &DeviceResourceCommand,
    content: &dyn ContentStore,
) -> Result<Option<ContentRef>, Error> {
    if command.operation() != DeviceResourceOperation::TaskSubmitResult {
        return Ok(None);
    }
    if command.admission().work_item.is_none() {
        return Err(Error::Forbidden);
    }
    let input = WorkItemResultSubmissionInput::parse(command.request_bytes())
        .map_err(|_| Error::InvalidArguments)?;
    match input.delivery.publication.text {
        Some(text) => stage(
            content,
            actor,
            work_item_result_message_id(command.invocation().invocation_id),
            "text/markdown;charset=utf-8",
            text.into_bytes(),
        )
        .await
        .map(Some),
        None => Ok(None),
    }
}
impl Prepared {
    pub(super) async fn stage_content(
        &self,
        actor: Actor,
        content: &dyn ContentStore,
    ) -> Result<ContentRef, Error> {
        for (expected, bytes) in [
            (
                &self.manifest_ref,
                self.material.manifest.canonical_bytes().map_err(error)?,
            ),
            (
                &self.receipt_ref,
                self.material
                    .manifest
                    .receipt()
                    .map_err(error)?
                    .canonical_bytes(),
            ),
        ] {
            if stage(
                content,
                actor,
                expected.content_id,
                expected.media_type.as_str(),
                bytes,
            )
            .await?
                != *expected
            {
                return Err(Error::Corrupt);
            }
        }
        Ok(self.receipt_ref.clone())
    }
}
