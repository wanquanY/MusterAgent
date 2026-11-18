//! Native terminal observations enter the existing Publication transaction.
//! The Home Journal owns terminality. Cloud retains only a digest and closed
//! facts; it never imports the private Run, Tool journal or completion payload.
use super::*;
use crate::agent_resource::device_custody;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_persistence_codec::device_terminal_codec as codec;
use musteragent_runtime_content::{ContentStore, PutContent};
use std::sync::Arc;
mod coverage;
mod receipt;
use device_tool::source;

#[derive(Clone)]
pub struct PostgresDeviceTerminalStore {
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    hosts: Arc<dyn WorkItemDeviceTargetAuthority>,
}
impl PostgresDeviceTerminalStore {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        hosts: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
            hosts,
        }
    }

    async fn record_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        terminal: &DeviceTerminalReceipt,
    ) -> Result<DeviceTerminalAcknowledgement, DialogueStoreError> {
        terminal.validate()?;
        target.require_actor(actor)?;
        let commitment =
            codec::receipt_commitment(terminal).map_err(|_| DialogueStoreError::Corrupt)?;
        let b = &terminal.binding;
        let authority = DeviceHostAuthority::from_trusted_account_transport(
            actor,
            target.membership_authority(),
        )
        .map_err(|_| DialogueStoreError::Unavailable)?;
        let selection = WorkItemDeviceSelection {
            membership_id: authority.destination().membership_id,
            expected_membership_epoch: authority.destination().membership_epoch.get(),
        };
        // Establish custody before even staging a constant notice. This is an
        // autocommit read; no connection/lock crosses Content or Backend IO.
        let (original, recorded) = {
            let mut conn = self.database.pool().acquire().await.map_err(db)?;
            let original = device_custody::load_local(&mut conn, actor, target, b, false)
                .await
                .map_err(custody_error)?;
            let recorded: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.native_dialogue_terminals WHERE tenant_id=$1 AND owner_id=$2 AND run_id=$3)")
                .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
                .bind(b.run_id.into_uuid()).fetch_one(&mut *conn).await.map_err(db)?;
            (original, recorded)
        };
        // An immutable ACK must remain recoverable without re-reading or
        // re-creating content. This observation only skips staging; the exact
        // receipt and live custody are still checked in the final transaction.
        let notice = terminal
            .failure_code
            .filter(|_| !recorded && terminal.completion.is_none())
            .map(DialogueTerminalNotice::from_final_failure);
        let body = match notice {
            Some(notice) => Some(
                self.content
                    .put(PutContent {
                        tenant_id: actor.tenant_id,
                        media_type: MediaType::new("text/markdown;charset=utf-8")
                            .map_err(|_| DialogueStoreError::Corrupt)?,
                        bytes: notice.message().as_bytes().to_vec(),
                    })
                    .await
                    .map_err(|_| DialogueStoreError::Infrastructure)?,
            ),
            None => None,
        };
        // Qualify AFTER external Content IO; then obtain a fresh governed
        // snapshot. A preceding repeatable read cannot conceal revocation.
        let host = self
            .hosts
            .qualify_retained_host(&authority, selection)
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        source::lock(&mut tx, b).await.map_err(publication_error)?;
        let current = device_custody::load_local(&mut tx, actor, target, b, false)
            .await
            .map_err(custody_error)?;
        if current != original {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let mut source = source::load_retained_binding(&mut tx, actor, target, b, None)
            .await
            .map_err(publication_error)?;
        require_host(&mut tx, &host, &authority, selection).await?;
        if let Some(ack) = receipt::replay(&mut tx, actor, terminal, commitment).await? {
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            return Ok(ack);
        }
        // Preserve the Publication owner's space/dialogue -> entry/content
        // lock order before inspecting current fulfillment or adding a notice.
        let active = super::terminal::lock_current_scope(&mut tx, &mut source)
            .await
            .map_err(store_error)?;
        super::work_item_device::verify_device_publication_cut(
            &mut tx,
            actor,
            b.run_id,
            b.snapshot_hash,
            b.grant_proof_hash,
            terminal.settlement.outcome,
            &terminal.settlement.publication_cut,
        )
        .await?;
        let complete = coverage::complete(&mut tx, &source, terminal).await?;
        if terminal.completion.is_some() && !complete {
            return Err(DialogueStoreError::Corrupt);
        }
        // Like the existing Cloud terminal Owner, current account visibility
        // permits a fixed impact notice; it does not restore old permissions or
        // authorize any artifact/Tool replay. Inactive agents add no old notice.
        let entry_id = if terminal.settlement.outcome == WorkItemDeviceRunOutcome::Failed
            && !complete
            && active
        {
            let body = body.ok_or(DialogueStoreError::Corrupt)?;
            crate::artifact::authorize_content_ref(&mut tx, actor.tenant_id, &body)
                .await
                .map_err(|_| DialogueStoreError::Unavailable)?;
            let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
                &b.run_id.into_uuid(),
                b"musteragent.native-dialogue-terminal-publication.v1",
            ))
            .map_err(|_| DialogueStoreError::Corrupt)?;
            let mut bytes = b"musteragent.native-dialogue-terminal-notice.v1\0".to_vec();
            bytes.extend(commitment.into_bytes());
            bytes.extend(body.sha256.into_bytes());
            Some(
                publish_in_transaction(
                    &mut tx,
                    &source,
                    &Delivery {
                        delivery_id,
                        run_id: b.run_id,
                        purpose: DialoguePublicationPurpose::Blocked,
                        content: PublicDeliveryContent::Message(body),
                        fingerprint: Sha256::digest(bytes),
                        source: PublicationSource::NativeTerminalOwner(b.run_id),
                    },
                )
                .await
                .map_err(store_error)?
                .entry_id,
            )
        } else {
            None
        };
        let ack = DeviceTerminalAcknowledgement {
            binding: b.clone(),
            receipt_commitment: commitment,
            resolution: if entry_id.is_some() {
                DeviceTerminalResolution::Published
            } else {
                DeviceTerminalResolution::Suppressed
            },
            entry_id,
        };
        ack.validate_for(terminal, commitment)?;
        receipt::insert(&mut tx, &source, terminal, &ack).await?;
        crate::agent_dialogue::capability::stage_native_successor(&mut tx, actor, terminal).await?;
        require_host(&mut tx, &host, &authority, selection).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(ack)
    }
}
impl DeviceTerminalStore for PostgresDeviceTerminalStore {
    fn record<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        terminal: &'a DeviceTerminalReceipt,
    ) -> ApplicationFuture<'a, Result<DeviceTerminalAcknowledgement, DialogueStoreError>> {
        Box::pin(self.record_inner(actor, target, terminal))
    }
}
async fn require_host(
    tx: &mut sqlx::PgConnection,
    host: &WorkItemDeviceHostQualification,
    authority: &DeviceHostAuthority,
    selection: WorkItemDeviceSelection,
) -> Result<(), DialogueStoreError> {
    let now = crate::ledger::database_now_ms(tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    host.require_selection(authority, selection, now)
}
fn custody_error(e: DeviceResourceError) -> DialogueStoreError {
    match e {
        DeviceResourceError::Corrupt => DialogueStoreError::Corrupt,
        DeviceResourceError::Infrastructure => DialogueStoreError::Infrastructure,
        _ => DialogueStoreError::Unavailable,
    }
}
fn publication_error(e: DialogueToolPublicationError) -> DialogueStoreError {
    match e {
        DialogueToolPublicationError::Corrupt => DialogueStoreError::Corrupt,
        DialogueToolPublicationError::Infrastructure => DialogueStoreError::Infrastructure,
        _ => DialogueStoreError::Unavailable,
    }
}
fn store_error(e: DialoguePublicationError) -> DialogueStoreError {
    match e {
        DialoguePublicationError::Store(e) => e,
        _ => DialogueStoreError::Unavailable,
    }
}
