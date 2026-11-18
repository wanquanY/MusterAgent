//! Dispatch only registered Resource/File contracts through their original
//! parser, preparation, edit, search and read implementations.
use super::*;
use musteragent_runtime_artifact::{ArtifactToolContext, BuiltinFileTools, FileToolPreparation};
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityResult};
use musteragent_runtime_file_query::*;
use musteragent_runtime_resource::*;

pub(super) enum Prepared {
    Task(
        Box<crate::work_item_execution::native::PreparedNativeWorkItem>,
        CapabilityResult,
    ),
    Goal(Box<goals::Prepared>, CapabilityResult),
    File(FileToolPreparation, Vec<ContentRef>),
    Memory(Box<crate::memory::native::NativeMemoryPrepared>),
    Resource(ResourceToolPreparation),
    Edit(FileEditToolPreparation),
    Query(CapabilityResult, Vec<ContentRef>),
}
impl Prepared {
    pub fn authorized_content(&self) -> &[ContentRef] {
        match self {
            Self::Task(plan, _) => plan.authorized_content(),
            Self::Goal(plan, _) => plan.authorized_content(),
            Self::Query(_, refs) | Self::File(_, refs) => refs,
            _ => &[],
        }
    }
}
pub(super) fn invocation(auth: &authority::Authority) -> Result<CapabilityInvocation, Error> {
    let command = auth.request.command();
    let inv = command.invocation();
    let run = command.run_context();
    let attempt = auth.request.attempt();
    // Trace is diagnostic-only; authorization comes from Native custody and the
    // frozen request, never from this propagation identifier.
    let traceparent = format!(
        "00-{}-{}-01",
        inv.invocation_id.into_uuid().simple(),
        &inv.invocation_id.into_uuid().simple().to_string()[..16]
    );
    Ok(CapabilityInvocation {
        actor: auth.actor,
        tenant_id: auth.actor.tenant_id,
        conversation_id: run.conversation_id,
        run_id: run.run_id,
        requested_by_attempt_id: inv.requested_by_attempt_id,
        execution_attempt_id: attempt.attempt_id,
        execution_attempt_number: attempt.attempt_number,
        invocation_id: inv.invocation_id,
        tool_call_id: inv.tool_call_id.clone(),
        tool_key: inv.tool_key.clone(),
        capability_key: inv.capability_key.clone(),
        capability_provider_id: inv.provider_id,
        idempotency_key: inv.idempotency_key.clone(),
        request_ref: inv.request_ref.clone(),
        request_schema: inv.request_schema.clone(),
        result_schema: inv.result_schema.clone(),
        authorized_content: inv.authorized_content.clone(),
        // This adapter invokes the account's canonical Resource owner. Its
        // logical namespace stays Dialogue; the original physical directory
        // remains in RunContext and is checked against the admitted grant.
        workspace_id: None,
        timing: auth.request.timing().clone(),
        trace_context: musteragent_runtime_contract::TraceContext {
            traceparent: musteragent_runtime_contract::TraceParent::new(traceparent)
                .map_err(|_| Error::Corrupt)?,
            tracestate: None,
        },
        execution_grant_proof: None,
    })
}
pub(super) async fn prepare(
    store: &PostgresDeviceResourceStore,
    auth: Arc<authority::Authority>,
    content: Arc<RecordingContentStore>,
) -> Result<Prepared, Error> {
    let inv = invocation(&auth)?;
    if auth.request.command().operation().is_task() {
        return tasks::prepare(store, auth, content).await;
    }
    if auth.request.command().operation().is_goal() {
        return goals::prepare(store, auth).await;
    }

    if auth.request.command().operation().is_memory() {
        let (_, _, custody) = auth.current_snapshot().await?;
        let facts = auth
            .request
            .command()
            .invocation()
            .memory
            .as_ref()
            .ok_or(Error::Corrupt)?;
        if let Some(origin) = &facts.user_origin {
            let super::super::device_custody::DeviceCustody::Intent(p) = &custody else {
                return Err(Error::Forbidden);
            };
            let ids =
                musteragent_runtime_admission::AdmissionIdentities::derive(p.identities.command_id)
                    .ok_or(Error::Corrupt)?;
            if origin.record_id != ids.message_record_id
                || origin.content_ref != p.intent.content_ref
            {
                return Err(Error::Forbidden);
            }
        }
        return crate::memory::native::prepare(
            store.database.clone(),
            content,
            inv,
            custody.agent_id(),
            auth.request.command(),
            auth.context.now,
        )
        .await
        .map(|memory| Prepared::Memory(Box::new(memory)));
    }
    let run = *auth.request.command().run_context();
    let now = auth.context.now;
    let resources = crate::PostgresResourceStore::new(store.database.clone());
    let edits = Arc::new(FileEditApplication::new(resources.clone(), content.clone()));
    let reader = Arc::new(ProjectFileReadApplication::new(
        resources.clone(),
        content.clone(),
    ));
    let context = ResourceToolContext {
        invocation: inv.clone(),
        run_context: run,
        now,
        resolved_source: None,
    };
    match auth.request.command().operation() {
        DeviceResourceOperation::MemoryRemember
        | DeviceResourceOperation::MemoryPropose
        | DeviceResourceOperation::MemoryPublish
        | DeviceResourceOperation::MemorySearch
        | DeviceResourceOperation::MemoryList
        | DeviceResourceOperation::MemoryGet
        | DeviceResourceOperation::ExperienceSearch
        | DeviceResourceOperation::ExperienceInspect => Err(Error::Corrupt),
        DeviceResourceOperation::ExecutionsList(_) => {
            super::executions::prepare(store, auth, content, inv).await
        }
        DeviceResourceOperation::FileCreate => {
            let artifacts = Arc::new(crate::PostgresArtifactStore::new(
                store.database.clone(),
                store.reference_keys.clone(),
            ));
            let tools = BuiltinFileTools::new(content, artifacts.clone(), artifacts)
                .with_resource_authority(auth);
            tools
                .prepare(ArtifactToolContext {
                    execution: None,
                    invocation: inv.clone(),
                    activity_id: run.activity_id,
                    committed_by: run.executor_participant_id,
                    now,
                })
                .await
                .map(|prepared| {
                    let refs = Vec::new();
                    Prepared::File(prepared, refs)
                })
                .map_err(failure::artifact)
        }
        DeviceResourceOperation::FilesList
        | DeviceResourceOperation::FilesSearch
        | DeviceResourceOperation::FilesInspect
        | DeviceResourceOperation::FilesRead => {
            let catalog = Arc::new(ManagedResourceFileCatalog::for_owned_agent(
                auth.clone(),
                Arc::new(resources),
            ));
            let sources = Arc::new(super::native_input::NativeInputSource(auth.clone()));
            let dialogue = Arc::new(
                AgentFileCatalog::new(catalog.clone(), sources.clone()).with_outputs(sources),
            );
            let app = Arc::new(
                FileQueryApplication::new(
                    catalog.clone(),
                    catalog.clone(),
                    Arc::new(ContentFileReader::new(content.clone())),
                )
                .with_dialogue_catalog(dialogue),
            );
            let service = FileQueryService::new(auth.clone(), app);
            let access = FileQueryAccess {
                actor: auth.actor,
                conversation_id: Some(run.conversation_id),
                workspace_id: None,
                run: Some(RunFileQueryAccess {
                    invocation_id: Some(inv.invocation_id),
                    context: run,
                    attempt: auth.request.attempt().clone(),
                    authorized_content: inv.authorized_content.clone(),
                    timing: inv.timing.clone(),
                }),
            };
            let (value, refs) = service
                .execute_tool(
                    &access,
                    inv.tool_key.as_str(),
                    auth.request.command().request_bytes(),
                )
                .await
                .map_err(failure::query)?;
            let bytes = serde_json::to_vec(&value).map_err(|_| Error::Corrupt)?;
            let reference = store
                .stage(
                    auth.actor,
                    auth.request.command(),
                    b"canonical-file-query-result.v1",
                    &bytes,
                )
                .await?;
            Ok(Prepared::Query(
                CapabilityResult {
                    result_hash: reference.sha256,
                    result_ref: reference,
                    result_schema: inv.result_schema,
                    provider_request_ref: None,
                },
                refs,
            ))
        }
        DeviceResourceOperation::FileRead
        | DeviceResourceOperation::EditBegin
        | DeviceResourceOperation::EditAppend
        | DeviceResourceOperation::EditPatch
        | DeviceResourceOperation::EditResume
        | DeviceResourceOperation::EditFinalize
        | DeviceResourceOperation::EditAbort => BuiltinFileEditTools::new(content, edits, reader)
            .with_context_authority(auth)
            .prepare(context)
            .await
            .map(Prepared::Edit)
            .map_err(failure::edit),
        _ => BuiltinResourceTools::new(
            content,
            Arc::new(ResourceApplication::new(resources.clone())),
        )
        .with_search(Arc::new(ResourceSearchApplication::new(resources.clone())))
        .with_file_edits(edits)
        .with_file_names(Arc::new(resources))
        .with_context_authority(auth)
        .prepare(context)
        .await
        .map(Prepared::Resource)
        .map_err(failure::resource),
    }
}
impl FileQueryAuthority for authority::Authority {
    fn resolve<'a>(
        &'a self,
        access: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async move {
            let command = self.request.command();
            let Some(run) = access.run.as_ref() else {
                return Err(FileQueryError::Denied);
            };
            if access.actor != self.actor
                || access.conversation_id != self.context.conversation_id()
                || access.workspace_id.is_some()
                || run.context != *command.run_context()
                || run.attempt != *self.request.attempt()
                || run.invocation_id != self.context.invocation_id
                || run.timing != *self.request.timing()
                || run.authorized_content != command.invocation().authorized_content
            {
                return Err(FileQueryError::Denied);
            }
            let (tx, context, input_view) = self.input_snapshot().await?;
            tx.commit().await.map_err(|_| FileQueryError::Unavailable)?;
            let ResourceCommandSource::DeviceExecution {
                agent_id,
                source_digest,
                ..
            } = context.source
            else {
                return Err(FileQueryError::Denied);
            };
            Ok(FileQueryContext {
                dialogue_source: Some(AgentFileQuerySource {
                    input_view: Some(input_view),
                    agent_id,
                    invocation_id: command.invocation().invocation_id,
                    tool_key: context.tool_key.clone().ok_or(FileQueryError::Denied)?,
                }),
                actor: self.actor,
                conversation_id: context.conversation_id(),
                now: context.now,
                authority_revision: format!("device-resource:{}", source_digest),
                content_allowlist: None,
                project_workspace_id: None,
            })
        })
    }
}

pub(super) async fn validate_custodied_grant(
    conn: &mut sqlx::PgConnection,
    command: &DeviceResourceCommand,
    prepared: &super::super::device_custody::DeviceCustody,
) -> Result<(), Error> {
    use musteragent_persistence_codec::{execution_grant_decode, wire};
    use prost::Message;
    let bytes:Vec<u8>=match prepared {
        super::super::device_custody::DeviceCustody::Intent(_) => sqlx::query_scalar("SELECT v.signed_grant_proof FROM agent.device_grant_heads h JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.admitted_revision WHERE h.intent_id=$1 AND v.signed_grant_hash=$2")
            .bind(command.admission().intent_id.into_uuid()).bind(command.admission().grant_proof_hash.into_bytes().to_vec()).fetch_one(&mut *conn).await.map_err(db)?,
        super::super::device_custody::DeviceCustody::WorkItem(_) => sqlx::query_scalar("SELECT signed_grant_proof FROM collaboration.work_item_device_admitted_custody WHERE run_id=$1 AND signed_grant_hash=$2")
            .bind(command.admission().run_id.into_uuid()).bind(command.admission().grant_proof_hash.into_bytes().to_vec()).fetch_one(&mut *conn).await.map_err(db)?,
    };
    if bytes.len() > 4 * 1024 * 1024
        || Sha256::digest(&bytes) != command.admission().grant_proof_hash
    {
        return Err(Error::Corrupt);
    }
    let signed = execution_grant_decode::signed_execution_grant(Some(
        wire::SignedExecutionGrant::decode(bytes.as_slice()).map_err(|_| Error::Corrupt)?,
    ))
    .map_err(|_| Error::Corrupt)?;
    match prepared {
        super::super::device_custody::DeviceCustody::Intent(p) => {
            validate_device_grant(p, command.admission().device_id, &signed.claims)
        }
        super::super::device_custody::DeviceCustody::WorkItem(p) => {
            validate_work_item_device_grant(p, &signed.claims)
        }
    }
    .map_err(|_| Error::Forbidden)?;
    // The resource namespace remains Dialogue. Its physical execution directory
    // must still match the exact admitted signed proof, never current defaults.
    if signed
        .claims
        .identity_binding
        .ok_or(Error::Forbidden)?
        .device_workspace_id
        != physical_workspace(command.run_context())?
    {
        return Err(Error::Forbidden);
    }
    let inv = command.invocation();
    let definition = tasks::resolve_contract(signed.claims.product_release.as_ref(), command)?;
    if definition.capability_key.as_str() != inv.capability_key.as_str()
        || DeviceResourceOperation::from_tool_key(definition.key.as_str())? != command.operation()
    {
        return Err(Error::Forbidden);
    }
    if !signed.claims.capability_entitlements.iter().any(|grant| {
        grant.key == inv.capability_key
            && grant.provider_id == inv.provider_id
            && grant.location == musteragent_capability::CapabilityLocation::Device
            && grant.effect == definition.effect
            && definition.required_scopes.is_subset(&grant.allowed_scopes)
            && grant.policy_decision != musteragent_policy::Decision::Deny
    }) {
        return Err(Error::Forbidden);
    }
    if signed
        .claims
        .tool_contract_releases
        .iter()
        .any(|release| release.tool_key.as_str() == inv.tool_key.as_str() && !release.available)
    {
        return Err(Error::Forbidden);
    }
    Ok(())
}

fn physical_workspace(run: &musteragent_run::RunContext) -> Result<Option<WorkspaceId>, Error> {
    match run.execution_scope {
        musteragent_event_envelope::ExecutionScope::DevicePrivate { workspace_id, .. } => {
            Ok(workspace_id)
        }
        _ => Err(Error::Forbidden),
    }
}
