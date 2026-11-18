//! Authenticated Device ingress into the existing canonical Resource owner.
//! Remote commit records a Resource effect; only the real Device Tool owner
//! may subsequently record successful local Tool completion and publication.
mod authority;
mod executions;
mod failure;
mod file_material;
mod file_transfer;
mod goals;
pub(crate) mod mutation;
mod native_input;
mod prepare;
pub(crate) mod receipt;
mod tasks;
pub(crate) use authority::authorize;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_runtime_content::{ContentStore, RecordingContentStore};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_resource::{ResourceCommandContext, ResourceCommandSource};
use musteragent_runtime_session::*;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_telemetry::DbCommitKind;
use sqlx::Row;
use std::sync::Arc;
type Error = DeviceResourceError;
fn db(_: sqlx::Error) -> Error {
    Error::Infrastructure
}

#[derive(Clone)]
pub struct PostgresDeviceResourceStore {
    pub(crate) database: crate::PostgresDatabaseRuntime,
    pub(crate) content: Arc<dyn ContentStore>,
    reference_keys: Arc<OpaqueRefKeyring>,
    file_material: Arc<std::sync::Mutex<file_material::FileMaterial>>,
    task_targets: Option<Arc<dyn WorkItemDeviceTargetAuthority>>,
    source_selection: Option<Arc<dyn owner_event_sources::OwnerGoalSourceSelection>>,
}
impl PostgresDeviceResourceStore {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
            reference_keys,
            file_material: Arc::new(std::sync::Mutex::new(file_material::FileMaterial::default())),
            task_targets: None,
            source_selection: None,
        }
    }

    pub fn with_source_selection(
        mut self,
        selection: Arc<dyn owner_event_sources::OwnerGoalSourceSelection>,
    ) -> Self {
        self.source_selection = Some(selection);
        self
    }
    pub fn with_work_item_targets(
        mut self,
        targets: Arc<dyn WorkItemDeviceTargetAuthority>,
    ) -> Self {
        self.task_targets = Some(targets);
        self
    }
    async fn current_task(
        &self,
        conn: &mut sqlx::PgConnection,
        custody: &super::device_custody::DeviceCustody,
        target: &OriginDeviceExecutionTarget,
    ) -> Result<(), Error> {
        if let super::device_custody::DeviceCustody::WorkItem(p) = custody {
            crate::work_item_execution::device::require_live_target(
                self.task_targets.as_deref(),
                p,
                target,
                conn,
            )
            .await
            .map_err(super::device_custody::task_error)?;
            crate::work_item_execution::device::require_live_source(conn, p.preparation())
                .await
                .map_err(super::device_custody::task_error)?;
        }
        Ok(())
    }
    async fn admitted(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceResourceRequest,
    ) -> Result<authority::Authority, Error> {
        let mut conn = self.database.pool().acquire().await.map_err(db)?;
        let now = crate::ledger::database_now_ms(&mut conn)
            .await
            .map_err(|_| Error::Infrastructure)?;
        request.validate_at(now)?;
        let command = request.command();
        let binding = command.admission();
        let prepared = super::device_custody::load(
            &mut conn,
            actor,
            target,
            binding,
            self.task_targets.as_deref(),
            true,
        )
        .await?;
        let run = command.run_context();
        let ids = musteragent_runtime_admission::AdmissionIdentities::derive(
            prepared.identities().command_id,
        )
        .ok_or(Error::Corrupt)?;
        if run.activity_id != ids.activity_id
            || run.runtime_snapshot_id != ids.runtime_snapshot_id
            || run.home_epoch.get() != 1
            || run.activity_kind != musteragent_activity::ActivityKind::Chat
            || run.tenant_id != actor.tenant_id
            || run.run_id != binding.run_id
            || run.conversation_id != prepared.identities().conversation_id
            || run.executor_participant_id != prepared.identities().executor
            || run.requested_by_participant_id != prepared.identities().requested_by
            || run.parent_run_id.is_some()
            || run.delegation_id.is_some()
        {
            return Err(Error::Forbidden);
        }
        prepare::validate_custodied_grant(&mut conn, command, &prepared).await?;
        let context = ResourceCommandContext {
            tenant_id: actor.tenant_id,
            actor,
            source: ResourceCommandSource::DeviceExecution {
                conversation_id: run.conversation_id,
                agent_id: prepared.agent_id(),
                intent_id: binding.intent_id,
                work_item: binding.work_item.as_ref().map(|task| {
                    musteragent_runtime_resource::DeviceWorkItemSource {
                        work_item_id: task.work_item_id,
                        goal_revision: task.goal_revision,
                        control_revision: task.control_revision,
                        assignment_epoch: task.assignment_epoch,
                    }
                }),
                run_id: binding.run_id,
                device_id: binding.device_id,
                installation_id: binding.installation_id,
                source_digest: binding.source_digest,
                snapshot_hash: binding.snapshot_hash,
                grant_proof_hash: binding.grant_proof_hash,
                command_commitment: command.commitment(),
            },
            invocation_id: Some(command.invocation().invocation_id),
            tool_key: Some(
                VersionedKey::new(command.invocation().tool_key.as_str())
                    .map_err(|_| Error::InvalidArguments)?,
            ),
            now,
        };
        if !authorize(&mut conn, &context, false).await.map_err(db)? {
            return Err(Error::Forbidden);
        }
        Ok(authority::Authority {
            store: self.clone(),
            actor,
            target: target.clone(),
            request: request.clone(),
            context,
        })
    }
    async fn lookup(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceResourceRequest,
    ) -> Result<Option<DeviceResourceReceipt>, Error> {
        if request.is_memory_approval() {
            return Err(Error::InvalidArguments);
        }
        let auth = self.admitted(actor, target, request).await?;
        let mut conn = self.database.pool().acquire().await.map_err(db)?;
        let result =
            receipt::lookup(self.content.as_ref(), &mut conn, actor, request.command()).await?;
        drop(conn);
        auth.current().await?;
        Ok(result)
    }
    async fn execute(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        request: &DeviceResourceRequest,
    ) -> Result<DeviceResourceReceipt, Error> {
        let auth = self.admitted(actor, target, request).await?;
        let command = request.command();
        {
            let mut conn = self.database.pool().acquire().await.map_err(db)?;
            if let Some(value) =
                receipt::lookup(self.content.as_ref(), &mut conn, actor, command).await?
            {
                drop(conn);
                auth.current().await?;
                return Ok(value);
            }
        }
        if let Some(manifest) = command.result_artifact_manifest() {
            if manifest.scope.tenant_id != actor.tenant_id
                || manifest.scope.owner_id != actor.principal_id
                || manifest.entries.len() != request.result_roots().len()
            {
                return Err(Error::Forbidden);
            }
            for (entry, root) in manifest.entries.iter().zip(request.result_roots()) {
                // The signed canonical request decoder already proved exact bytes,
                // hash, format and dependency closure before this original content put.
                let mut bytes = root.as_slice();
                let actual = self
                    .content
                    .put_stream_at(actor.tenant_id, &entry.version.content_ref, &mut bytes)
                    .await
                    .map_err(|_| Error::ReferenceUnavailable)?;
                if actual != entry.version.content_ref {
                    return Err(Error::Corrupt);
                }
                for dep in &entry.version.dependency_refs {
                    if self
                        .content
                        .open_verified(actor.tenant_id, dep)
                        .await
                        .map_err(|_| Error::ReferenceUnavailable)?
                        .reference()
                        != dep
                    {
                        return Err(Error::ReferenceUnavailable);
                    }
                }
            }
            auth.current().await?;
        }
        // Store the exact original immutable request. No JSON reparsing or model
        // text can change the operation, schema, or invocation identity.
        let mut bytes = command.request_bytes();
        let stored = self
            .content
            .put_stream_at(
                actor.tenant_id,
                &command.invocation().request_ref,
                &mut bytes,
            )
            .await
            .map_err(|_| Error::ReferenceUnavailable)?;
        if stored != command.invocation().request_ref {
            return Err(Error::Corrupt);
        }
        if request.is_memory_approval() {
            let ResourceCommandSource::DeviceExecution { agent_id, .. } = auth.context.source
            else {
                return Err(Error::Forbidden);
            };
            let (media, bytes) = crate::memory::native::approval_prompt(
                self.database.clone(),
                self.content.clone(),
                &prepare::invocation(&auth)?,
                agent_id,
            )
            .await?;
            let bytes = serde_json::to_vec(
                &serde_json::json!({"media_type": media.as_str(), "bytes": bytes}),
            )
            .map_err(|_| Error::Corrupt)?;
            let reference = self
                .stage(
                    actor,
                    command,
                    b"native-memory-approval-statement.v1",
                    &bytes,
                )
                .await?;
            auth.current().await?;
            let receipt = DeviceResourceReceipt {
                command_commitment: command.commitment(),
                invocation_id: command.invocation().invocation_id,
                operation: command.operation(),
                read_dependencies: Vec::new(),
                committed_at: auth.context.now,
                outcome: DeviceResourceOutcome::Succeeded(DeviceResourceSuccess {
                    result_ref: reference,
                    result_schema: command.invocation().result_schema.clone(),
                    result_bytes: bytes,
                    authorized_content: Vec::new(),
                    produced_versions: Vec::new(),
                }),
            };
            receipt.validate_for(command)?;
            return Ok(receipt);
        }
        let command_ref = self
            .stage(
                actor,
                command,
                b"canonical-resource-command.v1",
                &command.encode()?,
            )
            .await?;
        let recording = Arc::new(RecordingContentStore::new(self.content.clone()));
        let prepared = prepare::prepare(self, Arc::new(auth.clone()), recording.clone()).await;
        let mut reads = recording.references().map_err(|_| Error::Infrastructure)?;
        if let Ok(prepared) = &prepared {
            reads.extend(prepared.authorized_content().iter().cloned());
        }
        reads.sort_by_key(|r| r.content_id);
        reads.dedup();
        if reads.len() > MAX_DEVICE_RESOURCE_REFS {
            return Err(Error::InvalidArguments);
        }
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|_| Error::Infrastructure)?;
        // Task authority locks account/dialogue/Agent in its original order
        // before Device admission/grant locks or Resource Agent SHARE locks.
        // The frozen Task plan owns target selection; transport cannot infer it.
        if let Ok(prepare::Prepared::Task(plan, _)) = &prepared {
            crate::work_item_execution::native::lock_prepared(&mut tx, &auth.context, plan).await?;
        }
        // Serialize through the original admission custody. This is a transaction
        // lock, not another scheduler or lease; no pending row survives a commit.
        if command.admission().work_item.is_some() {
            sqlx::query("SELECT run_id FROM collaboration.work_item_device_custody WHERE run_id=$1 FOR UPDATE")
                .bind(command.admission().run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        } else {
            sqlx::query(
                "SELECT intent_id FROM agent.device_admissions WHERE intent_id=$1 FOR UPDATE",
            )
            .bind(command.admission().intent_id.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        }
        let custody = super::device_custody::load(
            &mut tx,
            actor,
            target,
            command.admission(),
            self.task_targets.as_deref(),
            true,
        )
        .await?;
        if !authorize(&mut tx, &auth.context, true).await.map_err(db)? {
            return Err(Error::Forbidden);
        }
        if let Some(value) = receipt::lookup(self.content.as_ref(), &mut tx, actor, command).await?
        {
            let now = crate::ledger::database_now_ms(&mut tx)
                .await
                .map_err(|_| Error::Infrastructure)?;
            request.validate_at(now)?;
            target
                .require_current_at(now)
                .map_err(|_| Error::Forbidden)?;
            tx.commit().await.map_err(|_| Error::Infrastructure)?;
            return Ok(value);
        }
        for reference in std::iter::once(&command_ref)
            .chain(std::iter::once(&command.invocation().request_ref))
            .chain(reads.iter())
        {
            crate::dialogue_codec::lock_ready_content_reference(
                &mut tx,
                actor.tenant_id,
                reference,
            )
            .await
            .map_err(|_| Error::ReferenceUnavailable)?;
        }
        receipt::begin(&mut tx, actor, command, &command_ref).await?;
        // Only original Tool parser/store logical errors produce a durable negative
        // fact. Every mutation is inside this savepoint: a rejected receipt proves
        // that this command's Resource effect has been rolled back completely.
        sqlx::query("SAVEPOINT device_resource_effect")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let applied = match prepared {
            Ok(prepared) => {
                mutation::apply(
                    &mut tx,
                    &auth.context,
                    prepared,
                    self.task_targets.as_deref(),
                )
                .await
            }
            Err(error) => Err(error),
        };
        let outcome = match applied {
            Ok((result, versions, authorized)) => {
                crate::artifact::authorize_result_content(&mut tx, actor.tenant_id, &result)
                    .await
                    .map_err(|_| Error::Infrastructure)?;
                if result.result_ref.byte_length > MAX_DEVICE_RESOURCE_TOOL_BYTES as u64 {
                    return Err(Error::Corrupt);
                }
                let result_bytes = self
                    .content
                    .read(actor.tenant_id, &result.result_ref)
                    .await
                    .map_err(|_| Error::Infrastructure)?;
                sqlx::query("RELEASE SAVEPOINT device_resource_effect")
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                DeviceResourceOutcome::Succeeded(DeviceResourceSuccess {
                    result_ref: result.result_ref,
                    result_schema: result.result_schema,
                    result_bytes,
                    authorized_content: authorized,
                    produced_versions: versions,
                })
            }
            Err(error) => {
                let Some(code) = failure::rejection(error) else {
                    return Err(error);
                };
                sqlx::query("ROLLBACK TO SAVEPOINT device_resource_effect")
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                sqlx::query("RELEASE SAVEPOINT device_resource_effect")
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                DeviceResourceOutcome::Rejected(code)
            }
        };
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        let value = DeviceResourceReceipt {
            command_commitment: command.commitment(),
            invocation_id: command.invocation().invocation_id,
            operation: command.operation(),
            outcome,
            read_dependencies: reads.clone(),
            committed_at: now,
        };
        value.validate_for(command)?;
        let receipt_ref = self
            .stage(
                actor,
                command,
                b"canonical-resource-receipt.v1",
                &value.encode(command)?,
            )
            .await?;
        crate::dialogue_codec::lock_ready_content_reference(&mut tx, actor.tenant_id, &receipt_ref)
            .await
            .map_err(|_| Error::Infrastructure)?;
        if let Some(success) = value.success() {
            crate::dialogue_codec::lock_ready_content_reference(
                &mut tx,
                actor.tenant_id,
                &success.result_ref,
            )
            .await
            .map_err(|_| Error::Infrastructure)?;
        }
        receipt::complete(&mut tx, command, &value, &receipt_ref, &reads).await?;
        self.current_task(&mut tx, &custody, target).await?;
        let final_now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(|_| Error::Infrastructure)?;
        request.validate_at(final_now)?;
        target
            .require_current_at(final_now)
            .map_err(|_| Error::Forbidden)?;
        tx.commit().await.map_err(|_| Error::Infrastructure)?;
        Ok(value)
    }
    async fn stage(
        &self,
        actor: Actor,
        command: &DeviceResourceCommand,
        domain: &[u8],
        bytes: &[u8],
    ) -> Result<ContentRef, Error> {
        let mut identity = domain.to_vec();
        identity.extend_from_slice(&Sha256::digest(bytes).into_bytes());
        let reference = ContentRef {
            content_id: ContentId::from_uuid(uuid::Uuid::new_v5(
                &command.invocation().invocation_id.into_uuid(),
                &identity,
            ))
            .map_err(|_| Error::Corrupt)?,
            media_type: MediaType::new("application/json").map_err(|_| Error::Corrupt)?,
            byte_length: bytes.len() as u64,
            sha256: Sha256::digest(bytes),
        };
        let mut source = bytes;
        self.content
            .put_stream_at(actor.tenant_id, &reference, &mut source)
            .await
            .map_err(|_| Error::ReferenceUnavailable)
    }
}
impl DeviceResourceStore for PostgresDeviceResourceStore {
    fn read_file_content<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceResourceRequest,
        offset: u64,
    ) -> ApplicationFuture<'a, Result<DeviceFileChunk, Error>> {
        Box::pin(async move {
            let result = self.read_source_file(actor, target, request, offset).await;
            if (result.is_err() || result.as_ref().is_ok_and(|chunk| chunk.eof))
                && let Ok(mut cache) = self.file_material.lock()
            {
                cache.remove(&request.command().commitment().to_string());
            }
            result
        })
    }

    fn execute_resource_tool<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceResourceRequest,
    ) -> ApplicationFuture<'a, Result<DeviceResourceReceipt, Error>> {
        Box::pin(async move {
            let result = self.execute(actor, target, request).await;
            if matches!(result, Err(Error::TargetPolicyChanged)) {
                // Only this pre-reservation proof is safe to retry. The first
                // transaction rolled back. Re-enter the original admission,
                // receipt lookup, target selection and commit fence exactly once.
                return self.execute(actor, target, request).await;
            }
            result
        })
    }
    fn lookup_resource_tool<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        request: &'a DeviceResourceRequest,
    ) -> ApplicationFuture<'a, Result<Option<DeviceResourceReceipt>, Error>> {
        Box::pin(self.lookup(actor, target, request))
    }
}
