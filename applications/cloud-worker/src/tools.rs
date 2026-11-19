//! Kernel handlers share one content owner, fence journal and sandbox route owner.
use crate::{BuildError, SystemRuntimeClock, capability_manifest};
use musteragent_postgres::*;
use musteragent_runtime_artifact::BackendMediaCapabilityProvider;
use musteragent_runtime_capability::*;
use musteragent_runtime_content::ContentStore;
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_tool::{BuiltinToolCatalog, CapabilityAssemblyPlan, CapabilityHandlerKind as Kind};
use musteragent_tool_executor::*;
use musteragent_wire::{ProtobufLedgerPayloadCodec, ProtobufSnapshotCodec};
use std::sync::Arc;

/// Hosts register external capabilities here before freezing the dispatcher.
#[derive(Clone)]
pub struct ToolServices {
    pub database: PostgresDatabaseRuntime,
    pub content: Arc<dyn ContentStore>,
    pub keys: Arc<OpaqueRefKeyring>,
    pub clock: Arc<dyn musteragent_execution_liveness::LivenessHomeClock>,
    pub providers: Vec<Arc<dyn CapabilityProvider>>,
    pub handlers: Vec<Arc<dyn AtomicCapabilityHandler>>,
    pub media: Vec<Arc<dyn BackendMediaCapabilityProvider>>,
    pub embeddings: Option<Arc<dyn musteragent_runtime_memory::MemoryEmbeddingProvider>>,
    pub model: Arc<dyn musteragent_runtime_model::ModelProvider>,
    pub metering: Arc<dyn musteragent_runtime_model::MeteringPublicationEncoder>,
    pub releases: Vec<musteragent_identity::ProductReleaseRef>,
    /// Text for the retryable `browser_execution_unavailable` tool failure.
    pub browser_unavailable_notice: musteragent_tool_executor::BrowserUnavailableNotice,
}

/// Compose default Task behavior per invocation's verified actor, not process identity.
struct ScopedWorkItems {
    content: Arc<dyn ContentStore>,
    store: Arc<PostgresWorkItemStartStore>,
    clock: Arc<dyn RuntimeClock>,
}
impl WorkItemTools for ScopedWorkItems {
    fn invoke<'a>(
        &'a self,
        attempt: &'a musteragent_run::AttemptContext,
        material: &'a ToolExecutionMaterial,
        invocation: &'a CapabilityInvocation,
        arguments: &'a [u8],
        activity: Option<&'a OperationContext>,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        'a,
        Result<WorkItemToolResult, DialogueStoreError>,
    > {
        Box::pin(async move {
            DefaultWorkItemTools::new(
                material.actor,
                self.content.clone(),
                self.store.clone(),
                self.clock.clone(),
            )?
            .invoke(attempt, material, invocation, arguments, activity)
            .await
        })
    }
}

/// Registered external dependencies are explicit; missing ones fail in the registry.
pub fn kernel_dispatcher(
    services: ToolServices,
) -> Result<Arc<dyn CapabilityDispatcher>, BuildError> {
    Ok(Arc::new(product_dispatchers(services)?))
}

/// Hosts with connector executors can attach their PublishedCapabilityFactory
/// to an exact product before sharing the dispatcher with the worker.
pub fn product_dispatchers(
    services: ToolServices,
) -> Result<ProductCapabilityDispatcher, BuildError> {
    let mut products = ProductCapabilityDispatcher::default();
    for release in musteragent_tool::compiled_product_releases_refs()
        .into_iter()
        .chain(services.releases.iter().cloned())
    {
        let catalog = BuiltinToolCatalog::for_product_release(
            &release,
            musteragent_tool::ProductExecutionTarget::Cloud,
        )?;
        let dispatcher = product_dispatcher(services.clone(), &catalog)?;
        products
            .register(release, catalog.definitions().to_vec(), dispatcher)
            .map_err(|e| format!("product registration: {e:?}"))?;
    }
    Ok(products)
}

fn product_dispatcher(
    services: ToolServices,
    catalog: &BuiltinToolCatalog,
) -> Result<Arc<dyn CapabilityDispatcher>, BuildError> {
    let ToolServices {
        database,
        content,
        keys,
        clock: live_clock,
        providers,
        handlers,
        media,
        embeddings,
        model,
        metering,
        releases: _,
        browser_unavailable_notice,
    } = services;
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let clock = Arc::new(SystemRuntimeClock);
    let journal = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            database.clone(),
            codec.clone(),
            codec.clone(),
            keys.clone(),
        )
        .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), live_clock.clone()),
    );
    let assembly = CapabilityAssemblyPlan::from_catalog(catalog)?;
    let mut registry = CapabilityRegistry::new();
    let declares = |manifest: &musteragent_capability::CapabilityManifest,
                    operation: &musteragent_tool::BackendOperation| {
        catalog.definitions().iter().any(|d| {
            d.capability_key.as_str() == manifest.key.as_str()
                && d.target.backend_operation() == Some(operation)
        })
    };
    for provider in providers {
        if provider
            .backend_operation()
            .is_some_and(|op| !declares(provider.manifest(), op))
        {
            continue;
        }
        registry.register(provider)?;
    }
    for entry in assembly
        .entries()
        .iter()
        .filter(|e| e.handler == Kind::Time)
    {
        registry.register(Arc::new(BuiltinTimeCapability::new(
            capability_manifest(
                entry.definition,
                musteragent_tool::BUILTIN_RUST_PROVIDER_ID.parse()?,
            )?,
            content.clone(),
            clock.clone(),
        )?))?;
    }
    let mut dispatcher = CompositeCapabilityDispatcher::new(Arc::new(registry));
    for handler in handlers {
        dispatcher
            .register_atomic(handler)
            .map_err(|e| format!("host capability registration: {e:?}"))?;
    }
    let memory_store = Arc::new(PostgresMemoryEvolutionStore::new(database.clone()));
    let mut memory_tools = musteragent_runtime_memory::BuiltinMemoryTools::new(
        content.clone(),
        memory_store.clone(),
        memory_store,
    )
    .with_retrieval_telemetry(Arc::new(
        musteragent_runtime_memory::TracingMemoryRetrievalTelemetry,
    ));
    if let Some(embedding) = &embeddings {
        memory_tools = memory_tools.with_semantic_search(
            embedding.clone(),
            Arc::new(PostgresMemoryVectorIndex::new(database.clone())),
        );
    }
    let memory_committer = Arc::new(
        PostgresFencedMemoryToolCommitter::new(database.clone(), codec.clone(), keys.clone())
            .with_execution_liveness(live_clock.clone()),
    );
    let resource_store = Arc::new(PostgresResourceStore::new(database.clone()));
    let authority = Arc::new(PostgresResourceContextAuthority::new(
        database.pool().clone(),
    ));
    let file_edit = Arc::new(musteragent_runtime_resource::FileEditApplication::new(
        PostgresResourceStore::new(database.clone()),
        content.clone(),
    ));
    let file_edit_tools = musteragent_runtime_resource::BuiltinFileEditTools::new(
        content.clone(),
        file_edit.clone(),
        Arc::new(
            musteragent_runtime_resource::ProjectFileReadApplication::new(
                PostgresResourceStore::new(database.clone()),
                content.clone(),
            ),
        ),
    )
    .with_context_authority(authority.clone());
    let file_edit_commit = Arc::new(
        PostgresFencedFileEditToolCommitter::new(database.clone(), codec.clone(), keys.clone())
            .with_execution_liveness(live_clock.clone())
            .with_original_tool_receipts(journal.clone()),
    );
    let mut resource_search = musteragent_runtime_resource::ResourceSearchApplication::new(
        PostgresResourceStore::new(database.clone()),
    );
    if let Some(embedding) = &embeddings {
        resource_search = resource_search.with_semantic(Arc::new(
            crate::resource_embeddings::ResourceEmbeddings::new(&database, embedding.clone()),
        ));
    }
    let resource_tools = musteragent_runtime_resource::BuiltinResourceTools::new(
        content.clone(),
        Arc::new(musteragent_runtime_resource::ResourceApplication::new(
            PostgresResourceStore::new(database.clone()),
        )),
    )
    .with_context_authority(authority.clone())
    .with_file_names(resource_store.clone())
    .with_search(Arc::new(resource_search))
    .with_file_edits(file_edit);
    let resource_commit = Arc::new(
        PostgresFencedResourceToolCommitter::new(database.clone(), codec.clone(), keys.clone())
            .with_execution_liveness(live_clock.clone())
            .with_original_tool_receipts(journal.clone()),
    );
    let discovery = Arc::new(
        PostgresFencedDiscoveryToolCommitter::new(database.clone(), codec.clone(), keys.clone())
            .with_execution_liveness(live_clock.clone()),
    );
    let plans = Arc::new(
        PostgresExecutionPlanStore::new(database.clone(), codec.clone(), keys.clone())
            .with_execution_liveness(live_clock.clone()),
    );
    let work = Arc::new(ScopedWorkItems {
        content: content.clone(),
        store: Arc::new(
            PostgresWorkItemStartStore::new(database.clone(), codec.clone(), keys.clone())
                .with_query_content(content.clone()),
        ),
        clock: clock.clone(),
    });
    let files = musteragent_runtime_artifact::BuiltinFileTools::new(
        content.clone(),
        Arc::new(PostgresArtifactStore::new(database.clone(), keys.clone())),
        Arc::new(PostgresArtifactStore::new(database.clone(), keys.clone())),
    )
    .with_resource_authority(authority.clone());
    let artifact_commit = Arc::new(
        PostgresFencedArtifactToolCommitter::new(database.clone(), codec.clone(), keys.clone())
            .with_execution_liveness(live_clock.clone())
            .with_original_tool_receipts(journal.clone()),
    );
    let browser: Arc<dyn BrowserGatewayStore> = Arc::new(
        PostgresBrowserGatewayStore::new(database.clone()).with_sandbox_routing(Arc::new(
            PostgresSandboxRoutingStore::new((*journal).clone(), Arc::new(ProtobufSnapshotCodec)),
        )),
    );
    let artifacts = Arc::new(PostgresArtifactStore::new(database.clone(), keys.clone()));
    let queries = crate::file_queries::compose(
        &database,
        codec.clone(),
        content.clone(),
        artifacts.clone(),
        clock.clone(),
    );
    let skills = Arc::new(PostgresSkillDiscoveryMaterialSource::new(
        database.pool().clone(),
        Arc::new(ProtobufSnapshotCodec),
        codec.clone(),
    ));
    let skill_commit = Arc::new(
        PostgresFencedSkillDisclosureCommitter::with_security_audit(
            database.clone(),
            codec.clone(),
            keys.clone(),
            Arc::new(PostgresSecurityAuditPipeline::standard(database.clone())),
        )
        .with_execution_liveness(live_clock.clone()),
    );
    let goals = Arc::new(
        musteragent_runtime_automation::StandingGoalProductApplication::new(
            Arc::new(PostgresStandingGoalProductStore::new(database.clone())),
            content.clone(),
        ),
    );
    let history = Arc::new(
        musteragent_runtime_data_access::delegation_history::DelegationHistoryReader::new(
            Arc::new(PostgresDelegationHistorySource::new(
                database.clone(),
                codec.clone(),
            )),
            content.clone(),
        ),
    );
    let auxiliary = Arc::new(PostgresFencedExecutionLivenessJournal::new(
        database.clone(),
        Arc::new(ProtobufSnapshotCodec),
        codec.clone(),
        codec.clone(),
        live_clock.clone(),
    ));
    let supervision = musteragent_loop::ExecutionSupervision::new(
        auxiliary.clone(),
        live_clock.clone(),
        Arc::new(musteragent_execution_liveness_io::HomeExecutionActivityFactory),
    );
    let vision = Arc::new(
        PersistedVisionModelInvoker::new(
            model,
            PostgresModelInvocationStore::new(database.clone(), keys.clone())
                .with_execution_liveness(auxiliary),
            content.clone(),
            metering,
            clock.clone(),
        )
        .with_execution_supervision(supervision),
    );
    let lifecycle = Arc::new(PostgresSkillLifecycleStore::new(database.clone()));
    let skill_product = Arc::new(musteragent_runtime_skill::SkillProductApplication::new(
        lifecycle.clone(),
        lifecycle,
        content.clone(),
    ));
    let skill_creator = Arc::new(musteragent_runtime_skill::SkillCreatorApplication::new(
        Arc::new(PostgresSkillPackagePreviewStore::new(database.clone())),
        content.clone(),
        skill_product,
    ));
    let delegation = Arc::new(PostgresCloudDelegation::new(
        database.clone(),
        content.clone(),
        codec.clone(),
        keys.clone(),
        live_clock.clone(),
    ));
    let schedules = Arc::new(PostgresCloudScheduleTools::new(
        database.clone(),
        content.clone(),
        codec.clone(),
        keys.clone(),
        live_clock.clone(),
    ));
    let publication = Arc::new(PostgresFencedDialogueToolPublication::new(
        database.clone(),
        codec.clone(),
        content.clone(),
        keys.clone(),
        Arc::new(PostgresSecurityAuditPipeline::standard(database.clone())),
        clock.clone(),
        live_clock.clone(),
    ));
    for provider in media {
        if !declares(provider.manifest(), provider.operation()) {
            continue;
        }
        dispatcher
            .register_atomic(Arc::new(MediaArtifactCapabilityHandler::new(
                provider,
                Arc::new(PostgresMediaGenerationJournal::new(database.clone())),
                musteragent_runtime_artifact::BuiltinArtifactTools::new(content.clone()),
                artifact_commit.clone(),
                clock.clone(),
            )))
            .map_err(|e| format!("media registration: {e:?}"))?;
    }
    for entry in assembly.entries() {
        let manifest = capability_manifest(
            entry.definition,
            musteragent_tool::BUILTIN_RUST_PROVIDER_ID.parse()?,
        )?;
        let handler: Option<Arc<dyn AtomicCapabilityHandler>> = match entry.handler {
            Kind::Memory => Some(Arc::new(MemoryCapabilityHandler::new(
                manifest,
                memory_tools.clone(),
                memory_committer.clone(),
                clock.clone(),
            ))),
            Kind::Resource => Some(Arc::new(ResourceCapabilityHandler::new(
                manifest,
                resource_tools.clone(),
                resource_commit.clone(),
                clock.clone(),
            ))),
            Kind::Discovery => Some(Arc::new(DiscoveryCapabilityHandler::new(
                manifest,
                content.clone(),
                discovery.clone(),
                clock.clone(),
            ))),
            Kind::ExecutionPlan => Some(Arc::new(ExecutionPlanCapabilityHandler::new(
                manifest,
                content.clone(),
                plans.clone(),
                clock.clone(),
            ))),
            Kind::Interaction => Some(Arc::new(InteractionCapabilityHandler::new(
                manifest,
                journal.clone(),
                content.clone(),
                clock.clone(),
            ))),
            Kind::AgentWorkItem => Some(Arc::new(WorkItemCapabilityHandler::new(
                manifest,
                content.clone(),
                work.clone(),
                clock.clone(),
            ))),
            Kind::FileArtifact => Some(Arc::new(FileArtifactCapabilityHandler::new(
                manifest,
                files.clone(),
                artifact_commit.clone(),
                clock.clone(),
            ))),
            Kind::FileEdit => Some(Arc::new(FileEditCapabilityHandler::new(
                manifest,
                file_edit_tools.clone(),
                file_edit_commit.clone(),
                clock.clone(),
            ))),
            Kind::FileQuery => Some(Arc::new(FileQueryCapabilityHandler::new(
                manifest,
                content.clone(),
                queries.clone(),
            ))),
            Kind::BrowserEnvironment => Some(Arc::new(
                BrowserEnvironmentCapabilityHandler::new(
                    manifest,
                    browser.clone(),
                    content.clone(),
                    clock.clone(),
                )
                .with_unavailable_notice(browser_unavailable_notice.clone()),
            )),
            Kind::BrowserPython if entry.definition.key.as_str() == "data.python.run@11" => {
                Some(Arc::new(
                    BrowserWorkspaceCapabilityHandler::new(
                        manifest,
                        resource_store.clone(),
                        content.clone(),
                        browser.clone(),
                        resource_commit.clone(),
                        clock.clone(),
                        queries.clone(),
                    )
                    .with_unavailable_notice(browser_unavailable_notice.clone()),
                ))
            }
            Kind::BrowserPython => Some(Arc::new(
                BrowserPythonCapabilityHandler::new(
                    manifest,
                    resource_store.clone(),
                    authority.clone(),
                    content.clone(),
                    browser.clone(),
                    files.clone(),
                    artifact_commit.clone(),
                    clock.clone(),
                )
                .with_file_queries(queries.clone())
                .with_unavailable_notice(browser_unavailable_notice.clone()),
            )),
            Kind::SkillPreview => Some(Arc::new(SkillPreviewCapabilityHandler::new(
                manifest,
                content.clone(),
                skill_creator.clone(),
                clock.clone(),
            ))),
            Kind::DialoguePublication => Some(Arc::new(DialoguePublicationCapabilityHandler::new(
                manifest,
                content.clone(),
                publication.clone(),
                clock.clone(),
            ))),
            Kind::SkillDiscovery => Some(Arc::new(SkillDiscoveryCapabilityHandler::new(
                manifest,
                content.clone(),
                skills.clone(),
                skill_commit.clone(),
                clock.clone(),
            ))),
            Kind::Delegation => Some(Arc::new(AccountCapabilityHandler::delegation(
                manifest,
                content.clone(),
                delegation.clone(),
                clock.clone(),
                journal.clone(),
            ))),
            Kind::OwnerSchedule => Some(Arc::new(AccountCapabilityHandler::schedules(
                manifest,
                content.clone(),
                schedules.clone(),
                clock.clone(),
                journal.clone(),
            ))),
            Kind::StandingGoal => Some(Arc::new(StandingGoalCapabilityHandler::new(
                manifest,
                content.clone(),
                goals.clone(),
                clock.clone(),
            ))),
            Kind::History => Some(Arc::new(HistoryCapabilityHandler::new(
                manifest,
                content.clone(),
                history.clone(),
            ))),
            Kind::Vision => Some(Arc::new(VisionCapabilityHandler::new(
                manifest,
                BuiltinVisionCapability::new(content.clone(), vision.clone())
                    .with_artifact_content_authorization(artifacts.clone()),
            ))),
            // These capabilities require host physical ports or later orchestration.
            // The registry returns CapabilityUnavailable without executing effects.
            Kind::Time
            | Kind::Backend
            | Kind::BackendMedia
            | Kind::ExecutorEnvironment
            | Kind::OwnerGoal
            | Kind::SourceAttachment => None,
            other => {
                return Err(format!(
                    "uncomposed kernel handler {other:?}: {}",
                    entry.definition.key.as_str()
                )
                .into());
            }
        };
        if let Some(handler) = handler {
            dispatcher
                .register_atomic(handler)
                .map_err(|e| format!("kernel capability registration: {e:?}"))?;
        }
    }
    Ok(Arc::new(dispatcher))
}
