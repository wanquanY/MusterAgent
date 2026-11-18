// Host composition uses the real neutral builder and PostgreSQL ContentStore.
struct TestSnapshotResolver {
    snapshots: KernelSnapshotBuilder,
}
impl TestSnapshotResolver {
    fn production(_pool: &sqlx::PgPool, content: Arc<dyn ContentStore>) -> Self {
        Self {
            snapshots: KernelSnapshotBuilder::new(
                content,
                RunExecutionPolicySnapshot::production_defaults(),
            ),
        }
    }
}
#[derive(Debug, Copy, Clone)]
struct TestGrantAuthenticator;
impl ExecutionGrantAuthenticator for TestGrantAuthenticator {
    fn authenticate(&self, signed: &SignedExecutionGrant) -> Result<ExecutionGrant, ErrorCode> {
        Ok(signed.claims.clone())
    }
}
impl RuntimeSnapshotResolver for TestSnapshotResolver {
    fn resolve_dialogue(
        &self,
        request: SnapshotResolutionRequest,
        source: musteragent_runtime_session::DialogueExecutionPreparation,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            let material = self
                .snapshots
                .build_dialogue_content(&request, &source)
                .await
                .map_err(|code| {
                    runtime_error(
                        code,
                        request.command.metadata.correlation_id,
                        false,
                        "Private snapshot source invalid",
                    )
                })?;
            RuntimeSnapshot::seal(
                request.run_context.runtime_snapshot_id,
                material,
                request.now,
                &ProtobufSnapshotCodec,
            )
            .map(|snapshot| RuntimeSnapshotResolution { snapshot })
            .map_err(|_| {
                runtime_error(
                    ErrorCode::RuntimeSnapshotInvalid,
                    request.command.metadata.correlation_id,
                    false,
                    "Private snapshot invalid",
                )
            })
        })
    }

    fn resolve(
        &self,
        request: SnapshotResolutionRequest,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            let content = self
                .snapshots
                .build_content(&request)
                .await
                .map_err(|code| {
                    runtime_error(
                        code,
                        request.command.metadata.correlation_id,
                        code == ErrorCode::DependencyUnavailable,
                        "Host snapshot material could not be built",
                    )
                })?;
            RuntimeSnapshot::seal(
                request.run_context.runtime_snapshot_id,
                content,
                request.now,
                &ProtobufSnapshotCodec,
            )
            .map(|snapshot| RuntimeSnapshotResolution { snapshot })
            .map_err(|error| {
                eprintln!("snapshot seal: {error:?}");
                runtime_error(
                    ErrorCode::RuntimeSnapshotInvalid,
                    request.command.metadata.correlation_id,
                    false,
                    "Invalid host snapshot",
                )
            })
        })
    }
    fn reseal_retry(
        &self,
        _tenant: TenantId,
        id: musteragent_identity::RuntimeSnapshotId,
        source: musteragent_runtime_snapshot::RuntimeSnapshotContent,
        now: UtcTimestamp,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            RuntimeSnapshot::seal(id, source, now, &ProtobufSnapshotCodec)
                .map(|snapshot| RuntimeSnapshotResolution { snapshot })
                .map_err(|_| {
                    runtime_error(
                        ErrorCode::RuntimeSnapshotInvalid,
                        correlation,
                        false,
                        "Invalid frozen retry snapshot",
                    )
                })
        })
    }
}
async fn publish_host_agent(pool: &sqlx::PgPool, actor: Actor, content: &dyn ContentStore) {
    use musteragent_definition::{AgentDefinitionStore, PublishedAgentVersionPointer};
    let mut definition = musteragent_conformance::fixtures::agent::system_definition();
    definition.kind = musteragent_definition::AgentKind::Custom;
    definition.built_in = false;
    definition.system_slot = None;
    definition.default_for_product = false;
    definition.deletable = true;
    definition.replaceable = true;
    let version = musteragent_conformance::fixtures::agent::system_version(1, "assistant-base@1");
    let material = version.material.as_ref().expect("host material");
    for (reference, bytes) in [
        (
            &version.persona_content_ref,
            b"synthetic persona".as_slice(),
        ),
        (
            &material.policy_content_ref,
            b"synthetic runtime policy".as_slice(),
        ),
        (
            &material.memory_policy_content_ref,
            b"synthetic memory policy".as_slice(),
        ),
        (
            &material.manifest_ref,
            b"synthetic Agent manifest v1".as_slice(),
        ),
    ] {
        let stored = content
            .put_at(
                reference.content_id,
                PutContent {
                    tenant_id: actor.tenant_id,
                    media_type: reference.media_type.clone(),
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .expect("publish fixture content");
        assert_eq!(&stored, reference);
    }
    let registry = musteragent_postgres::PostgresAgentDefinitionStore::new(pool.clone());
    registry
        .insert_definition(definition.clone())
        .await
        .expect("register host Agent");
    registry
        .insert_version(version.clone())
        .await
        .expect("publish host version");
    registry
        .compare_and_swap_published(
            definition.agent_id,
            None,
            PublishedAgentVersionPointer {
                agent_id: definition.agent_id,
                agent_version_id: version.agent_version_id,
                version: 1,
                published_at: version.published_at,
                published_by: actor.principal_id,
            },
        )
        .await
        .expect("publish host pointer");
}
