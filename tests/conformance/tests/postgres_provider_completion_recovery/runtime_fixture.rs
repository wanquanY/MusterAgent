use super::*;
use musteragent_conformance::fixtures::liveness_admission as admission;
use musteragent_context::{ContextCodec, RuntimeSnapshotDecoder};
use musteragent_runtime_snapshot::{RuntimeSnapshot, RuntimeSnapshotContent};
use musteragent_wire::{CanonicalJsonContextCodec, ProtobufSnapshotCodec};
#[path = "../support/owned_scripted_model.rs"]
mod owned;
use musteragent_conformance::fixtures::source_selection_snapshot as source_selection;
pub(super) use owned::OwnedScriptedModel;

pub(super) fn clock() -> Arc<musteragent_execution_liveness_io::SystemLivenessHomeClock> {
    static CLOCK: std::sync::OnceLock<
        Arc<musteragent_execution_liveness_io::SystemLivenessHomeClock>,
    > = std::sync::OnceLock::new();
    CLOCK.get_or_init(|| Arc::new(Default::default())).clone()
}

pub(super) async fn install_snapshot(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
    route: musteragent_model::ModelRoute,
    exposed: &[CanonicalToolDefinition],
) -> RuntimeSnapshotContent {
    let (encoded, started): (Vec<u8>, bool) = sqlx::query_as(
        "SELECT encoded_snapshot, EXISTS(SELECT 1 FROM runtime.model_invocations WHERE run_id=$2)
         FROM capability.runtime_snapshots WHERE runtime_snapshot_id=$1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    // Configure the complete current admission before its first invocation.
    // Reopened runtimes must retain the original sealed tool authority.
    if started {
        return ProtobufSnapshotCodec
            .decode_runtime_snapshot(&encoded)
            .expect("reuse the original sealed admission");
    }
    let mut controlled = context_snapshot::snapshot_content(
        fixture.run_id,
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref().unwrap(),
    );
    controlled.tools.clear();
    for definition in exposed {
        let Some(tool) = context_snapshot::snapshot_content_with_read_tool(
            fixture.run_id,
            musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref().unwrap(),
            definition.name.as_str(),
        )
        .tools
        .into_iter()
        .find(|tool| tool.manifest.wire_name.as_str() == definition.name.as_str()) else {
            panic!("requested fixture tool must be present");
        };
        controlled.tools.push(tool);
    }
    controlled.model.route = route;
    let mut snapshot = upgrade_snapshot(content, fixture, controlled).await;
    snapshot.execution_policy.max_model_events_per_commit = NonZeroUsize::new(2).unwrap();
    let sealed = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        snapshot,
        database_now(pool).await,
        &ProtobufSnapshotCodec,
    )
    .unwrap();
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$2,encoded_snapshot=$3,content_hash=$4 WHERE runtime_snapshot_id=$1")
        .bind(fixture.runtime_snapshot_id.into_uuid()).bind(sealed.content.schema_version.as_str()).bind(&sealed.encoded_content)
        .bind(sealed.content_hash.into_bytes().to_vec()).execute(pool).await.unwrap();
    sealed.content
}

pub(super) fn journal(
    pool: &sqlx::PgPool,
    content: Arc<dyn ContentStore>,
    recovery_keys: Arc<ProtectedContentKeyring>,
) -> PostgresAgentLoopJournal {
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    PostgresAgentLoopJournal::with_security_audit(
        pool.clone(),
        content,
        codec.clone(),
        codec,
        Arc::new(ProtobufMeteringPublicationEncoder),
        keyring(),
        recovery_keys,
        Arc::new(musteragent_postgres::PostgresSecurityAuditPipeline::standard(pool.clone())),
    )
    .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), clock())
}
pub(super) fn supervision(
    journal: &PostgresAgentLoopJournal,
) -> musteragent_loop::ExecutionSupervision {
    journal
        .execution_supervision(Arc::new(
            musteragent_execution_liveness_io::HomeExecutionActivityFactory,
        ))
        .expect("original Home supervision")
}
pub(super) async fn freeze(content: &dyn ContentStore, iteration: &mut ModelIteration) {
    iteration.request_ref = content
        .put(PutContent {
            tenant_id: iteration.request.invocation.run_context.tenant_id,
            media_type: MediaType::new("application/vnd.musteragent.model-request.v4+json")
                .unwrap(),
            bytes: CanonicalJsonContextCodec
                .encode_model_request(&iteration.request)
                .unwrap(),
        })
        .await
        .unwrap();
}

pub(super) async fn upgrade_snapshot(
    content: Arc<dyn ContentStore>,
    fixture: Fixture,
    controlled: RuntimeSnapshotContent,
) -> RuntimeSnapshotContent {
    let mut snapshot = source_selection::isolated_snapshot(
        content,
        running_loop::run_context(fixture),
        controlled.model.route,
    )
    .await;
    for mut tool in controlled.tools {
        // Both complete fixtures already carry the current kernel bootstrap.
        // Only the additional controlled tool is installed under fixture.*.
        if snapshot
            .content
            .tools
            .iter()
            .any(|current| current.manifest.key == tool.manifest.key)
        {
            continue;
        }
        tool.manifest.key = ToolKey::from_versioned_key(
            VersionedKey::new(format!("fixture.{}@1", tool.manifest.wire_name.as_str())).unwrap(),
        );
        let grant = match snapshot
            .content
            .capabilities
            .iter()
            .find(|grant| grant.key.as_str() == tool.manifest.capability_key.as_str())
        {
            Some(grant) => grant.clone(),
            None => {
                let grant = controlled
                    .capabilities
                    .iter()
                    .find(|grant| grant.key.as_str() == tool.manifest.capability_key.as_str())
                    .expect("controlled fixture capability")
                    .clone();
                snapshot.content.capabilities.push(grant.clone());
                snapshot
                    .content
                    .capabilities
                    .sort_by(|a, b| a.key.cmp(&b.key));
                grant
            }
        };
        tool.manifest.capability_provider_id = grant.provider_id;
        tool.manifest.required_scopes = grant.allowed_scopes.clone();
        let key = tool.manifest.key.clone();
        snapshot.content.tools.push(tool);
        snapshot
            .content
            .discovery
            .as_mut()
            .unwrap()
            .initial_exposed_tool_keys
            .push(key);
    }
    snapshot
        .content
        .tools
        .sort_by(|a, b| a.manifest.key.cmp(&b.manifest.key));
    let policy = RunExecutionPolicySnapshot::production_defaults()
        .liveness
        .unwrap()
        .model_policy();
    admission::seal(snapshot, Arc::new(policy)).content
}

pub(super) async fn recording_tools(
    pool: &sqlx::PgPool,
    blobs: Arc<dyn BlobStore>,
) -> Arc<RecordingTools> {
    let content: Arc<dyn ContentStore> =
        Arc::new(PostgresContentStore::new(pool.clone(), blobs, keyring()));
    let owner = journal(pool, content.clone(), running_loop::recovery_keyring());
    let supervision = supervision(&owner);
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let tool_journal = Arc::new(
        PostgresFencedToolInvocationJournal::new(
            pool.clone(),
            codec.clone(),
            codec.clone(),
            keyring(),
        )
        .with_execution_liveness(Arc::new(ProtobufSnapshotCodec), clock()),
    );
    let inner = Arc::new(
        DurableToolExecutor::new(
            production_tool_material_source(pool, codec),
            tool_journal,
            content.clone(),
            Arc::new(SuccessfulCapability { content }),
            Arc::new(FixedClock(database_now(pool).await)),
            Arc::new(FixedTrace),
        )
        .with_execution_supervision(supervision),
    );
    Arc::new(RecordingTools {
        calls: Mutex::new(Vec::new()),
        inner,
    })
}
