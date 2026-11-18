//! A private submission uses exact prepared content and owner-qualified asset
//! references. The delivery authority is the successful device terminal or the
//! requester's accepted report.
use super::*;
use crate::artifact::task_result::{TaskAssetLocator, prepare_result_asset, promote_result_asset};
use crate::work_item_execution::result_verification::AcceptedWorkItemResult;
use musteragent_content_ref::{ContentRef, Sha256};
use sqlx::PgConnection;

pub(crate) async fn prepare(
    tx: &mut PgConnection,
    scope: &WorkItemResultScope,
    invocation: InvocationId,
    input: &WorkItemResultSubmissionInput,
    request_sha256: Sha256,
    message: Option<&ContentRef>,
) -> Result<WorkItemResultSubmissionMaterial, DialogueStoreError> {
    input.validate_message(invocation, message)?;
    if (
        input.task_id,
        input.goal_revision,
        input.control_revision,
        input.assignment_epoch,
    ) != (
        scope.work_item_id,
        scope.goal_revision,
        scope.control_revision,
        scope.assignment_epoch,
    ) {
        return Err(DialogueStoreError::RevisionConflict);
    }
    let result_id = work_item_result_id(invocation);
    let mut items = Vec::with_capacity(input.delivery.entry_count());
    if let Some(reference) = message {
        lock_ready_content_reference(tx, scope.tenant_id, reference).await?;
        items.push(WorkItemResultItem {
            item_id: work_item_result_item_id(result_id, 0),
            content_ref: reference.clone(),
            asset: None,
        });
    }
    let locators = input
        .delivery
        .publication
        .artifacts
        .iter()
        .map(TaskAssetLocator::Producer)
        .chain(input.delivery.files.iter().map(TaskAssetLocator::File));
    for locator in locators {
        let (reference, asset) = prepare_result_asset(tx, scope, locator).await?;
        items.push(WorkItemResultItem {
            item_id: work_item_result_item_id(result_id, items.len()),
            content_ref: reference,
            asset: Some(asset),
        });
    }
    let manifest = WorkItemResultManifest {
        scope: scope.clone(),
        submission_invocation_id: invocation,
        request_sha256,
        items,
    };
    manifest.validate()?;
    Ok(WorkItemResultSubmissionMaterial { manifest })
}

#[derive(Clone)]
pub(super) struct AuthorizedResultItem {
    pub(super) entry_id: DialogueEntryId,
    pub(super) item_id: uuid::Uuid,
    publication_run_id: RunId,
    scope: WorkItemResultScope,
    pub(super) content: PublicDeliveryContent,
    reference: ContentRef,
}
impl AuthorizedResultItem {
    pub(super) fn reference_for(
        &self,
        source: &Source,
        run: RunId,
        entry: DialogueEntryId,
        content: &PublicDeliveryContent,
    ) -> Result<ContentRef, Error> {
        if self.entry_id != entry
            || self.publication_run_id != run
            || source.actor.tenant_id != self.scope.tenant_id
            || source.actor.principal_id != self.scope.owner_id
            || source.agent_id != self.scope.requesting_agent_id
            || source.dialogue_id != self.scope.source_dialogue_id
            || &self.content != content
        {
            return Err(DialogueStoreError::Unavailable.into());
        }
        Ok(self.reference.clone())
    }
}

// A terminal receipt can return only the frozen producer's submitted items to
// the requester recorded in this exact reservation. It grants no device action.
async fn device_result_source(
    tx: &mut PgConnection,
    s: &WorkItemResultScope,
) -> Result<Source, DialogueStoreError> {
    let row = sqlx::query("SELECT cfg.*, d.conversation_id FROM collaboration.work_item_result_submissions sub
        JOIN collaboration.work_item_device_admitted_custody d ON d.run_id=sub.producing_run_id AND d.work_item_id=sub.work_item_id
        JOIN collaboration.work_item_device_terminals t ON t.run_id=d.run_id AND t.outcome='completed' AND t.result_cut_commitment IS NOT NULL
        JOIN agent.owned_agent_configurations cfg ON cfg.agent_id=sub.requesting_agent_id
        WHERE sub.tenant_id=$1 AND sub.owner_id=$2 AND sub.producing_run_id=$3 AND sub.requesting_agent_id=$4 AND sub.source_dialogue_id=$5 AND sub.source_intent_id=$6
          AND sub.producer_home='device' AND d.admitted_snapshot_hash IS NOT NULL FOR SHARE OF cfg")
        .bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.producing_run_id.into_uuid())
        .bind(s.requesting_agent_id.into_uuid()).bind(s.source_dialogue_id.into_uuid()).bind(s.source_intent_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let config = crate::dialogue_codec::config_row(&row)?;
    Ok(Source {
        execution_home: PublicationHome::Device(config.permission_mode),
        permission_revision: config.permission_revision.get(),
        actor: Actor {
            tenant_id: s.tenant_id,
            principal_id: s.owner_id,
            principal_kind: PrincipalKind::User,
        },
        agent_id: s.requesting_agent_id,
        dialogue_id: s.source_dialogue_id,
        subject: PublicationSubject::WorkItem(s.source_intent_id),
        conversation_id: ConversationId::from_uuid(row.try_get("conversation_id").map_err(db)?)
            .map_err(|_| DialogueStoreError::Corrupt)?,
    })
}

pub(crate) async fn promote(
    tx: &mut PgConnection,
    accepted: &AcceptedWorkItemResult<'_>,
) -> Result<(), DialogueStoreError> {
    let s = accepted.scope();
    let source = match accepted.report_run_id() {
        Some(report) => load_source(tx, report).await.map_err(store)?,
        None => device_result_source(tx, s).await?,
    };
    if source.actor.tenant_id != s.tenant_id
        || source.actor.principal_id != s.owner_id
        || source.agent_id != s.requesting_agent_id
        || source.dialogue_id != s.source_dialogue_id
        || source.subject.intent() != Some(s.source_intent_id)
    {
        return Err(DialogueStoreError::RevisionConflict);
    }
    lock_publication_authority(tx, &source)
        .await
        .map_err(store)?;
    let delivery = work_item_result_delivery_id(accepted.result_id(), s.source_dialogue_id);
    sqlx::query("INSERT INTO collaboration.work_item_result_deliveries(result_id,delivery_id,verification_invocation_id,report_run_id,device_terminal_run_id,accepted_state_revision)
        VALUES($1,$2,$3,$4,$5,$6)")
        .bind(accepted.result_id()).bind(delivery.into_uuid())
        .bind(accepted.verification_invocation_id().map(InvocationId::into_uuid))
        .bind(accepted.report_run_id().map(RunId::into_uuid))
        .bind(accepted.report_run_id().is_none().then(|| s.producing_run_id.into_uuid()))
        .bind(integer(accepted.accepted_state_revision())?).execute(&mut *tx).await.map_err(db)?;
    for item in &accepted.manifest().items {
        let entry_id = DialogueEntryId::generate();
        let purpose = DialoguePublicationPurpose::Result;
        let content = if let Some(asset) = &item.asset {
            promote_result_asset(tx, accepted, item, entry_id).await?;
            PublicDeliveryContent::Artifact {
                artifact_id: asset.artifact_id,
                artifact_version_id: asset.artifact_version_id,
            }
        } else {
            PublicDeliveryContent::Message(item.content_ref.clone())
        };
        let id = CommandId::from_uuid(uuid::Uuid::new_v5(
            &delivery.into_uuid(),
            item.item_id.as_bytes(),
        ))
        .map_err(|_| DialogueStoreError::Corrupt)?;
        let fingerprint = public_delivery_fingerprint(
            id,
            accepted.publication_run_id(),
            NonZeroU64::new(source.permission_revision).ok_or(DialogueStoreError::Corrupt)?,
            purpose,
            &content,
        );
        let authorized = AuthorizedResultItem {
            entry_id,
            item_id: item.item_id,
            publication_run_id: accepted.publication_run_id(),
            scope: s.clone(),
            content: content.clone(),
            reference: item.content_ref.clone(),
        };
        publish_in_transaction(
            tx,
            &source,
            &Delivery {
                delivery_id: id,
                run_id: accepted.publication_run_id(),
                purpose,
                content,
                fingerprint,
                source: PublicationSource::TaskResultOwner(authorized),
            },
        )
        .await
        .map_err(store)?;
    }
    Ok(())
}

pub(crate) async fn verified_subject(
    tx: &mut PgConnection,
    tenant: TenantId,
    run: RunId,
) -> Result<bool, DialogueStoreError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_item_execution_reservations x
        LEFT JOIN collaboration.work_item_execution_reports r ON r.producing_run_id=x.run_id
        WHERE x.tenant_id=$1 AND x.result_protocol='verified_result_v1' AND (x.run_id=$2 OR (r.report_run_id=$2 AND NOT x.requester_uses_result)))")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_one(tx).await.map_err(db)
}

pub(super) async fn require_delivery_contract(
    tx: &mut PgConnection,
    source: &Source,
    delivery: &Delivery,
) -> Result<(), Error> {
    if !matches!(source.subject, PublicationSubject::WorkItem(_))
        || matches!(
            delivery.source,
            PublicationSource::TaskResultOwner(_) | PublicationSource::InteractionOwner(_)
        )
    {
        return Ok(());
    }
    // An old publication Tool, model-output adapter, or terminal recovery must
    // never turn an unverified candidate into visible output under the new role.
    let private = verified_subject(tx, source.actor.tenant_id, delivery.run_id).await?;
    if private && let PublicationSource::TerminalOwner(record) = &delivery.source {
        let original_notice:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_terminal_deliveries WHERE tenant_id=$1 AND run_id=$2 AND source_record_id=$3 AND delivery_id=$4 AND state='pending')")
            .bind(source.actor.tenant_id.into_uuid()).bind(delivery.run_id.into_uuid()).bind(record.into_uuid()).bind(delivery.delivery_id.into_uuid()).fetch_one(tx).await.map_err(db)?;
        if original_notice && delivery.purpose == DialoguePublicationPurpose::Blocked {
            return Ok(());
        }
    }
    if private {
        return Err(DialogueStoreError::Unavailable.into());
    }
    Ok(())
}
fn store(error: Error) -> DialogueStoreError {
    match error {
        Error::Store(e) => e,
        Error::Contract(_) => DialogueStoreError::Corrupt,
        _ => DialogueStoreError::RevisionConflict,
    }
}

/// A requester uses the usual artifact delivery tool with a reference from the
/// private submission. Selection and publication share the original transaction.
pub(super) async fn selected_file(
    tx: &mut PgConnection,
    source: &Source,
    run: RunId,
    invocation: InvocationId,
    reference: &AgentArtifactReference,
) -> Result<Option<(ArtifactId, AuthorizedResultItem)>, DialogueStoreError> {
    let AgentArtifactLocator::Artifact(artifact_id) = reference.artifact else {
        return Ok(None);
    };
    let row=sqlx::query("SELECT r.producing_run_id,w.state_revision FROM collaboration.work_item_execution_reports r
        JOIN collaboration.work_item_result_submissions s ON s.result_id=r.private_result_id AND s.manifest_sha256=r.private_result_sha256 AND s.submission_call_id=$2
        JOIN collaboration.work_item_requester_actions a ON a.report_run_id=r.report_run_id AND a.next_run_id IS NULL
        JOIN collaboration.work_items w ON w.work_item_id=r.work_item_id AND w.erasure_job_id IS NULL
        WHERE r.report_run_id=$1 AND r.tenant_id=$3 AND r.owner_id=$4 AND r.requesting_agent_id=$5 AND r.source_dialogue_id=$6")
        .bind(run.into_uuid()).bind(reference.tool_call_id.as_str()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid()).bind(source.agent_id.into_uuid()).bind(source.dialogue_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let producer = RunId::from_uuid(row.try_get("producing_run_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let result = crate::work_item_execution::result_submission::load_submission(tx, producer)
        .await?
        .ok_or(DialogueStoreError::Unavailable)?;
    let item = result
        .manifest
        .items
        .iter()
        .find(|i| {
            i.asset
                .as_ref()
                .is_some_and(|a| a.artifact_id == artifact_id)
        })
        .ok_or(DialogueStoreError::Unavailable)?;
    let version = item
        .asset
        .as_ref()
        .ok_or(DialogueStoreError::Corrupt)?
        .artifact_version_id;
    let proof = AcceptedWorkItemResult::from_publication(
        &result.manifest,
        run,
        invocation,
        unsigned(row.try_get("state_revision").map_err(db)?)?,
    );
    let entry_id = DialogueEntryId::generate();
    promote_result_asset(tx, &proof, item, entry_id).await?;
    Ok(Some((
        artifact_id,
        AuthorizedResultItem {
            entry_id,
            item_id: item.item_id,
            publication_run_id: run,
            scope: result.manifest.scope.clone(),
            content: PublicDeliveryContent::Artifact {
                artifact_id,
                artifact_version_id: version,
            },
            reference: item.content_ref.clone(),
        },
    )))
}
