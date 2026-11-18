//! Bind external authority to an original card, without retaining a database
//! connection while the membership or WorkItem target owner is contacted.
use super::*;
use crate::agent_resource::device_custody::{self, DeviceCustody};
use std::time::Instant;

pub(super) struct QualifiedControl {
    pub authority: NativeDialogueInteractionControlAuthority,
    subject: QualifiedSubject,
}

pub(super) struct QualifiedSubject {
    device: OriginDeviceExecutionTarget,
    live: bool,
    task: Option<Box<QualifiedTask>>,
}
struct QualifiedTask {
    original: WorkItemDeviceExecutionPreparation,
    host: DeviceHostAuthority,
    owner: TaskOwner,
}

enum TaskOwner {
    Live(WorkItemDeviceTargetQualification),
    Retained(WorkItemDeviceHostQualification),
}

fn scope_error(error: DialogueStoreError) -> RuntimeError {
    fail(match error {
        DialogueStoreError::Infrastructure => ErrorCode::DependencyUnavailable,
        DialogueStoreError::Corrupt => ErrorCode::ContentIntegrityFailed,
        _ => ErrorCode::PolicyDenied,
    })
}

fn custody_error(error: DeviceResourceError) -> RuntimeError {
    fail(match error {
        DeviceResourceError::Forbidden => ErrorCode::PolicyDenied,
        DeviceResourceError::Corrupt => ErrorCode::ContentIntegrityFailed,
        _ => ErrorCode::DependencyUnavailable,
    })
}

impl PostgresNativeDialogueInteractionStore {
    pub(super) async fn qualify_control(
        &self,
        actor: Actor,
        agent: AgentId,
        p: &NativeDialogueInteractionProjection,
        correlation: CorrelationId,
    ) -> Result<QualifiedControl, RuntimeError> {
        let started = Instant::now();
        let authority = self
            .controls
            .qualify(
                actor,
                agent,
                &p.admission,
                p.approval_policy
                    .as_ref()
                    .map(|policy| policy.policy_sha256),
            )
            .await;
        tracing::info!(%correlation, stage = "membership", elapsed_ms = started.elapsed().as_millis() as u64,
            succeeded = authority.is_ok(), "Native interaction eligibility");
        let authority = authority?;
        let subject = self
            .qualify_subject(actor, &authority.target, p, true, correlation)
            .await?;
        Ok(QualifiedControl { authority, subject })
    }

    pub(super) async fn qualify_subject(
        &self,
        actor: Actor,
        device: &OriginDeviceExecutionTarget,
        p: &NativeDialogueInteractionProjection,
        live: bool,
        correlation: CorrelationId,
    ) -> Result<QualifiedSubject, RuntimeError> {
        device.require_actor(actor).map_err(scope_error)?;
        if device.device_id() != p.admission.device_id
            || device.installation_id() != p.admission.installation_id
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        let task = if p.admission.work_item.is_some() {
            let source = {
                let mut conn = self.database.pool().acquire().await.map_err(database)?;
                crate::work_item_execution::device::load_admitted_binding_local(
                    &mut conn,
                    actor,
                    device,
                    &p.admission,
                    live,
                )
                .await
                .map_err(scope_error)?
            };
            let host = DeviceHostAuthority::from_trusted_account_transport(
                actor,
                device.membership_authority(),
            )
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
            let started = Instant::now();
            let target = if live {
                crate::work_item_execution::device::qualify_live_authority(
                    self.targets.as_ref(),
                    &host,
                    source.preparation(),
                )
                .await
                .map(TaskOwner::Live)
            } else {
                let selected = source.target();
                self.targets
                    .qualify_retained_host(
                        &host,
                        WorkItemDeviceSelection {
                            membership_id: selected.membership_id,
                            expected_membership_epoch: selected.membership_epoch,
                        },
                    )
                    .await
                    .map(TaskOwner::Retained)
            };
            tracing::info!(%correlation, stage = "work_item_target", elapsed_ms = started.elapsed().as_millis() as u64,
                succeeded = target.is_ok(), "Native interaction eligibility");
            Some(Box::new(QualifiedTask {
                original: source,
                host,
                owner: target.map_err(scope_error)?,
            }))
        } else {
            None
        };
        Ok(QualifiedSubject {
            device: device.clone(),
            live,
            task,
        })
    }
}

impl QualifiedControl {
    pub(super) async fn verify(
        &self,
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        row: &PgRow,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<(), RuntimeError> {
        self.subject.verify(conn, actor, row, p).await
    }
}

impl QualifiedSubject {
    /// Rebind the separately qualified original host to a fresh database
    /// snapshot, using retained custody for historical receipts only.
    pub(super) async fn source(
        &self,
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<Source, RuntimeError> {
        let custody =
            device_custody::load_local(conn, actor, &self.device, &p.admission, self.live)
                .await
                .map_err(custody_error)?;
        let now = crate::ledger::database_now_ms(&mut *conn)
            .await
            .map_err(database)?;
        self.device
            .require_current_at(now)
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        match (&custody, &self.task) {
            (DeviceCustody::WorkItem(current), Some(task))
                if current.as_ref() == &task.original =>
            {
                match &task.owner {
                    TaskOwner::Live(proof) => {
                        proof.require_for(&task.host, actor, current.target(), now)
                    }
                    TaskOwner::Retained(proof) => {
                        proof.require_for(&task.host, actor, current.target(), now)
                    }
                }
                .map_err(scope_error)?;
            }
            (DeviceCustody::Intent(_), None) => {}
            _ => return Err(fail(ErrorCode::PolicyDenied)),
        }
        let source = source::from_custody(actor, &p.admission, &custody);
        if source.conversation_id != p.conversation_id {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        if self.live {
            source::require_current_agent(conn, &source)
                .await
                .map_err(source_error)?;
        }
        Ok(source)
    }

    pub(super) fn allows_new_publication(&self) -> bool {
        self.live
    }

    pub(super) async fn verify(
        &self,
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        row: &PgRow,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<(), RuntimeError> {
        let source = self.source(conn, actor, p).await?;
        if source.agent_id != row_id(row, "agent_id")?
            || source.dialogue_id != row_id(row, "dialogue_id")?
            || row
                .try_get::<i64, _>("permission_revision")
                .map_err(database)?
                != source.permission_revision as i64
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        Ok(())
    }
}
