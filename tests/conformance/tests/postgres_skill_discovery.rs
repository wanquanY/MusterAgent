use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

#[path = "support/running_loop.rs"]
mod running_loop;
#[path = "support/skill_catalog_snapshot.rs"]
mod skill_catalog_snapshot;

use musteragent_capability::{CapabilityGrantSnapshot, CapabilityKey, CapabilityLocation};
use musteragent_conformance::fixtures::agent::{FIXTURE_PERSONA_V1, fixture_version_v1};
use musteragent_conformance::fixtures::agent::{
    FIXTURE_RUNTIME_POLICY_V1, fixture_runtime_policy_content_ref,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_discovery::{
    DiscoverySnapshot, PriorSkillDisclosureProvenance,
    SkillDisclosureRequest as CatalogDisclosureRequest, SkillDisclosureState,
};
use musteragent_event_envelope::RuntimeHome;
use musteragent_identity::{
    CapabilityProviderId, ContentId, DottedKey, IdempotencyKey, InvocationId, RunId, ToolCallId,
    UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_memory::MemorySelectionSnapshot;
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute,
    PricingVersionRef, ProviderStatePolicy,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_policy::Decision;
use musteragent_postgres::{
    PostgresContentStore, PostgresFencedSkillDisclosureCommitter, PostgresSecurityAuditPipeline,
    PostgresSkillDiscoveryMaterialSource, PostgresSkillLifecycleStore,
};
use musteragent_run::RunEvent;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::ContentStore;
use musteragent_runtime_contract::{ErrorCode, ModelClass};
use musteragent_runtime_discovery::{
    CommitSkillDisclosure, FencedSkillDisclosureCommitter, SkillDisclosureCommitDisposition,
    SkillDiscoveryMaterialSource,
};
use musteragent_runtime_skill::{ActiveSkillCatalogSource, authorized_skill_catalog};
use musteragent_runtime_skill::{
    OfficialSkillCatalogMaterialApplication, OfficialSkillCatalogSource,
};
use musteragent_runtime_snapshot::{
    AgentVersionSnapshot, CONTEXT_COMPACTION_POLICY_V2, ContextCompactionPolicyV2Spec,
    ContextPolicySnapshot, ModelRouteSnapshot, OutputPolicySnapshot, PinnedAuxiliaryModelRoute,
    PinnedModelRoute, PolicySnapshot, PromptAssemblySnapshot, PromptSectionSnapshot,
    ProtocolVersionSnapshot, RunExecutionPolicySnapshot, RuntimeSnapshot, RuntimeSnapshotContent,
    SNAPSHOT_SCHEMA_VERSION, SnapshotCodec, ToolDefinitionSnapshot, authorized_tool_catalog,
    hash_skill_catalog,
};
use musteragent_skill::{
    ActivationPolicy, SkillAuthorizationRequest, SkillDisclosureRequest, Surface,
};
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::ProtobufSnapshotCodec;
use sqlx::PgPool;

use running_loop::{database_now, keyring, run_context, seed_running_loop};

#[test]
fn current_skill_recovery_requires_complete_consistent_sealed_material() {
    let skills = musteragent_skill::builtin_skill_catalog_entries()
        .expect("kernel Skills")
        .into_iter()
        .filter(|entry| {
            matches!(
                entry.definition.skill_ref.logical_key(),
                "assistant-base" | "project-files" | "data-analysis" | "web-research"
            )
        })
        .map(|entry| entry.definition)
        .collect();
    let snapshot = current_snapshot_content(RunId::generate(), skills);
    snapshot.validate().expect("valid current Snapshot");
    let catalog = snapshot
        .frozen_authorized_skill_catalog()
        .expect("sealed catalog");
    assert_eq!(
        catalog.definitions(),
        snapshot
            .discovery
            .as_ref()
            .expect("discovery")
            .authorized_skills
    );

    let mut wrong_hash = snapshot.clone();
    wrong_hash
        .discovery
        .as_mut()
        .expect("discovery")
        .authorized_skill_catalog_hash = Sha256::digest(b"wrong catalog");
    let mut missing_descriptors = snapshot.clone();
    missing_descriptors
        .discovery
        .as_mut()
        .expect("discovery")
        .authorized_skills
        .clear();
    let mut wrong_definition = snapshot.clone();
    wrong_definition.skills[0].instructions_ref.sha256 = Sha256::digest(b"changed definition");
    let mut wrong_descriptor = snapshot.clone();
    wrong_descriptor
        .discovery
        .as_mut()
        .expect("discovery")
        .authorized_skills[0]
        .title = "changed descriptor".into();
    let mut missing_discovery = snapshot.clone();
    missing_discovery.discovery = None;
    for invalid in [
        wrong_hash,
        missing_descriptors,
        wrong_definition,
        wrong_descriptor,
        missing_discovery,
    ] {
        assert!(matches!(
            invalid.frozen_authorized_skill_catalog(),
            Err(musteragent_runtime_snapshot::SnapshotContractError::InvalidDiscovery)
        ));
    }
    let mut retired = snapshot;
    retired.schema_version =
        DottedKey::new("musteragent.legacy.runtime.snapshot.v4").expect("retired schema");
    assert!(matches!(
        retired.frozen_authorized_skill_catalog(),
        Err(musteragent_runtime_snapshot::SnapshotContractError::SchemaMismatch)
    ));
}

#[tokio::test]
async fn postgres_disclosure_is_fenced_atomic_ledger_derived_and_exactly_carried() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Skill disclosure migration");
    let (fixture, attempt) = seed_running_loop(&pool).await;
    let content = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        keyring(),
    ));
    let skill_store = Arc::new(PostgresSkillLifecycleStore::new(pool.clone()));
    let release_content: Arc<dyn ContentStore> = content.clone();
    let release_application = OfficialSkillCatalogMaterialApplication::new(release_content);
    let first_reconciliation = release_application
        .reconcile_global()
        .await
        .expect("reconcile official Skill catalog material");
    let repeated_reconciliation = release_application
        .reconcile_global()
        .await
        .expect("repeat official Skill catalog material reconciliation");
    assert_eq!(repeated_reconciliation, first_reconciliation);
    release_application
        .verify_global()
        .await
        .expect("verify official Skill catalog material");
    for entry in
        musteragent_skill::builtin_skill_catalog_entries().expect("compiled built-in Skill catalog")
    {
        let reference = entry.definition.instructions_ref;
        let expected = musteragent_skill::builtin_skill_instructions(&reference)
            .expect("compiled built-in Skill instructions");
        let persisted = content
            .read(fixture.tenant_id, &reference)
            .await
            .expect("published built-in Skill instructions");
        assert_eq!(persisted, expected.as_bytes());
    }
    let catalog_source = OfficialSkillCatalogSource::new(skill_store.clone());
    let now = database_now(&pool).await;
    let active_skill_definitions = catalog_source
        .load_active_entries(fixture.tenant_id)
        .await
        .expect("load active Runtime Skills")
        .into_iter()
        // The historical ordinary discovery fixture needs its Base, a dynamic
        // authoring Skill and an undisclosed research Skill for forgery checks.
        // Dialogue-only workflows are qualified by their newer Snapshot contracts.
        .filter(|entry| {
            matches!(
                entry.definition.skill_ref.logical_key(),
                "assistant-base" | "project-files" | "data-analysis" | "web-research"
            )
        })
        .map(|entry| entry.definition)
        .collect::<Vec<_>>();
    let snapshot = install_current_snapshot(
        &pool,
        fixture.runtime_snapshot_id,
        fixture.run_id,
        now,
        active_skill_definitions,
    )
    .await;
    seed_admitted_ledger_prefix(&pool, fixture, now).await;
    let capabilities = snapshot
        .tools
        .iter()
        .map(|tool| tool.manifest.capability_key.clone())
        .collect::<BTreeSet<_>>();
    let entries = catalog_source
        .load_active_entries(fixture.tenant_id)
        .await
        .expect("load active Runtime Skill entries")
        .into_iter()
        .filter(|entry| snapshot.skills.contains(&entry.definition))
        .collect::<Vec<_>>();
    let catalog =
        authorized_skill_catalog(&entries, &capabilities).expect("authorized Skill catalog");
    let discovery = snapshot.discovery.as_ref().expect("discovery Snapshot");
    let initial =
        SkillDisclosureState::from_initial(&catalog, &discovery.initial_disclosed_skill_refs)
            .expect("initial disclosures");
    let analysis = catalog
        .definitions()
        .iter()
        .find(|skill| skill.skill_ref.logical_key() == "data-analysis")
        .expect("exact released analysis Skill")
        .clone();
    let invocation_id = InvocationId::generate();
    let event = catalog
        .disclose(
            &initial,
            CatalogDisclosureRequest {
                skill_ref: analysis.skill_ref.clone(),
                source_invocation_id: invocation_id,
                disclosed_at: now,
            },
        )
        .expect("disclose analysis")
        .event
        .expect("new disclosure event");
    let request_ref = content_ref(b"view analysis", "application/json");
    let result_ref = content_ref(b"view result", "application/json");
    seed_content(&pool, fixture.tenant_id, &request_ref).await;
    seed_content(&pool, fixture.tenant_id, &result_ref).await;
    let view_tool = snapshot
        .tools
        .iter()
        .find(|tool| tool.manifest.capability_key.as_str() == "skills.view@2")
        .expect("skills.view Tool");
    seed_running_view_invocation(
        &pool,
        fixture,
        &attempt,
        invocation_id,
        view_tool.manifest.capability_provider_id,
        &request_ref,
        now,
    )
    .await;
    let command = CommitSkillDisclosure {
        run_context: run_context(fixture),
        invocation_id,
        skill_version_id: analysis.skill_version_id,
        initial_disclosures: initial.items().to_vec(),
        event: event.clone(),
        result: CapabilityResult {
            result_ref: result_ref.clone(),
            result_schema: view_tool.manifest.output_schema.clone(),
            result_hash: result_ref.sha256,
            provider_request_ref: None,
        },
        committed_at: now,
    };
    let codec = Arc::new(ProtobufLedgerPayloadCodec);
    let committer = PostgresFencedSkillDisclosureCommitter::with_security_audit(
        pool.clone(),
        codec.clone(),
        keyring(),
        Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone())),
    );

    install_audit_failure(&pool).await;
    assert!(
        committer
            .commit_skill_disclosure(&attempt, command.clone())
            .await
            .is_err(),
        "terminal Audit failure must roll back Tool, Ledger and projection"
    );
    assert_atomic_state(&pool, fixture.run_id, invocation_id, 3, 0, 0).await;
    remove_audit_failure(&pool).await;

    assert_eq!(
        committer
            .commit_skill_disclosure(&attempt, command.clone())
            .await
            .expect("commit Skill disclosure"),
        SkillDisclosureCommitDisposition::Applied
    );
    assert_atomic_state(&pool, fixture.run_id, invocation_id, 4, 1, 2).await;
    assert_eq!(
        committer
            .commit_skill_disclosure(&attempt, command)
            .await
            .expect("exact retry"),
        SkillDisclosureCommitDisposition::AlreadyApplied
    );
    assert_atomic_state(&pool, fixture.run_id, invocation_id, 4, 1, 2).await;

    let source = PostgresSkillDiscoveryMaterialSource::new(
        pool.clone(),
        Arc::new(ProtobufSnapshotCodec),
        codec,
    );
    let loaded = source
        .load(&attempt, &run_context(fixture))
        .await
        .expect("load Ledger-derived Skill disclosure state");
    assert_recovery_uses_only_sealed_catalog(&pool, &source, &attempt, fixture, &loaded).await;
    assert_eq!(
        loaded
            .disclosure_state
            .ordinal_for(&analysis.skill_ref)
            .map(NonZeroU32::get),
        Some(3)
    );

    sqlx::query("DELETE FROM runtime.skill_disclosures WHERE run_id = $1")
        .bind(fixture.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("remove non-authoritative projection");
    let reconstructed = source
        .load(&attempt, &run_context(fixture))
        .await
        .expect("reconstruct state from canonical Ledger");
    assert_eq!(
        reconstructed
            .disclosure_state
            .ordinal_for(&analysis.skill_ref)
            .map(NonZeroU32::get),
        Some(3),
        "runtime.skill_disclosures is not the disclosure authority"
    );

    let original = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        snapshot.clone(),
        now,
        &ProtobufSnapshotCodec,
    )
    .expect("re-seal original Snapshot");
    let mut forged = snapshot.clone();
    forged
        .skills
        .iter_mut()
        .find(|skill| skill.skill_ref.logical_key() == "web-research")
        .expect("web Skill")
        .activation_policy = ActivationPolicy::Base;
    assert!(
        ProtobufSnapshotCodec.encode_content(&forged).is_err(),
        "current admission rejects forged activation before persistence"
    );
    // Corrupt persisted bytes directly to also exercise the recovery boundary;
    // the current domain encoder correctly refuses this inconsistent catalog.
    use prost::Message as _;
    let mut forged_wire =
        musteragent_wire::wire::RuntimeSnapshotContent::decode(original.encoded_content.as_slice())
            .expect("decode original wire fixture");
    let forged_index = snapshot
        .skills
        .iter()
        .position(|skill| skill.skill_ref.logical_key() == "web-research")
        .expect("web Skill");
    forged_wire.skills[forged_index].activation_policy =
        musteragent_wire::wire::SkillActivationPolicy::Base as i32;
    let forged_bytes = forged_wire.encode_to_vec();
    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET encoded_snapshot = $2, content_hash = $3
         WHERE runtime_snapshot_id = $1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(&forged_bytes)
    .bind(Sha256::digest(&forged_bytes).into_bytes().to_vec())
    .execute(&pool)
    .await
    .expect("install forged Snapshot");
    let forged_recovery = source
        .load(&attempt, &run_context(fixture))
        .await
        .expect_err("recovery must reject a forged Base activation");
    assert_eq!(forged_recovery.code, ErrorCode::InternalInvariantViolated);
    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET encoded_snapshot = $2, content_hash = $3
         WHERE runtime_snapshot_id = $1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .bind(&original.encoded_content)
    .bind(original.content_hash.into_bytes().to_vec())
    .execute(&pool)
    .await
    .expect("restore exact Snapshot");

    sqlx::query("UPDATE runtime.leases SET fencing_token = 2 WHERE run_id = $1")
        .bind(fixture.run_id.into_uuid())
        .execute(&pool)
        .await
        .expect("advance fence");
    let stale = source
        .load(&attempt, &run_context(fixture))
        .await
        .expect_err("stale Attempt must fail closed");
    assert_eq!(stale.code, ErrorCode::AttemptLeaseStale);

    sqlx::query("UPDATE runtime.runs SET status = 5, finished_at_ms = $2 WHERE run_id = $1")
        .bind(fixture.run_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&pool)
        .await
        .expect("finish source Run");
    let prior = source
        .load_prior_disclosures(fixture.tenant_id, fixture.run_id)
        .await
        .expect("load exact terminal prior disclosures");
    assert_eq!(prior.len(), 3);
    let files = snapshot
        .skills
        .iter()
        .find(|skill| skill.skill_ref.logical_key() == "project-files")
        .expect("automatic file Skill");
    assert_eq!(prior[1].skill_version_id, files.skill_version_id);
    assert_eq!(prior[1].disclosure_ordinal.get(), 2);
    assert!(
        matches!(prior[1].provenance, PriorSkillDisclosureProvenance::AdmissionSnapshot { runtime_snapshot_id, .. } if runtime_snapshot_id == fixture.runtime_snapshot_id)
    );
    assert_eq!(prior[0].source_run_id, fixture.run_id);
    assert_eq!(prior[0].disclosure_ordinal.get(), 1);
    assert!(matches!(
        prior[0].provenance,
        PriorSkillDisclosureProvenance::AdmissionSnapshot {
            runtime_snapshot_id,
            ..
        } if runtime_snapshot_id == fixture.runtime_snapshot_id
    ));
    let carried = prior.last().expect("dynamic prior disclosure");
    assert_eq!(carried.source_run_id, fixture.run_id);
    assert_eq!(carried.disclosure_ordinal.get(), 3);
    assert_eq!(carried.skill_version_id, analysis.skill_version_id);
    assert!(matches!(
        carried.provenance,
        PriorSkillDisclosureProvenance::LedgerDisclosure {
            source_invocation_id,
            ..
        } if source_invocation_id == invocation_id
    ));
    assert_eq!(carried.skill_ref, analysis.skill_ref);
    assert_eq!(carried.instructions_ref, analysis.instructions_ref);
    assert_eq!(carried.instructions_hash, analysis.instructions_hash);
}

async fn assert_recovery_uses_only_sealed_catalog(
    pool: &PgPool,
    source: &PostgresSkillDiscoveryMaterialSource,
    attempt: &musteragent_run::AttemptContext,
    fixture: running_loop::Fixture,
    original: &musteragent_runtime_discovery::SkillDiscoveryMaterial,
) {
    // The current Snapshot seals complete descriptors. Recovery must succeed
    // even while the independent live lifecycle catalog is unavailable.
    sqlx::query("ALTER TABLE skill.skill_versions RENAME TO unavailable_skill_versions")
        .execute(pool)
        .await
        .expect("make only the live Skill catalog unavailable");
    let live = PostgresSkillLifecycleStore::new(pool.clone())
        .load_active_entries(fixture.tenant_id)
        .await;
    let recovered = source.load(attempt, &run_context(fixture)).await;
    sqlx::query("ALTER TABLE skill.unavailable_skill_versions RENAME TO skill_versions")
        .execute(pool)
        .await
        .expect("restore live lifecycle catalog");
    assert!(
        live.is_err(),
        "the live catalog must actually be unavailable"
    );
    let recovered = recovered.expect("recover from the sealed Skill catalog");
    assert_eq!(
        recovered.authorized_skills.definitions(),
        original.authorized_skills.definitions()
    );
    assert_eq!(
        recovered.authorized_skill_snapshot_hash,
        original.authorized_skill_snapshot_hash
    );
    assert_eq!(recovered.disclosure_state, original.disclosure_state);
}

async fn install_current_snapshot(
    pool: &PgPool,
    runtime_snapshot_id: musteragent_identity::RuntimeSnapshotId,
    run_id: RunId,
    now: UtcTimestamp,
    active_skill_definitions: Vec<musteragent_skill::SkillDefinition>,
) -> RuntimeSnapshotContent {
    let content = current_snapshot_content(run_id, active_skill_definitions);
    let sealed = RuntimeSnapshot::seal(
        runtime_snapshot_id,
        content.clone(),
        now,
        &ProtobufSnapshotCodec,
    )
    .expect("seal current Snapshot");
    sqlx::query(
        "UPDATE capability.runtime_snapshots
         SET schema_version = $2, encoded_snapshot = $3, content_hash = $4
         WHERE runtime_snapshot_id = $1",
    )
    .bind(runtime_snapshot_id.into_uuid())
    .bind(SNAPSHOT_SCHEMA_VERSION)
    .bind(&sealed.encoded_content)
    .bind(sealed.content_hash.into_bytes().to_vec())
    .execute(pool)
    .await
    .expect("install current Snapshot");
    content
}

async fn seed_admitted_ledger_prefix(
    pool: &PgPool,
    fixture: running_loop::Fixture,
    now: UtcTimestamp,
) {
    let record_id = musteragent_identity::LedgerRecordId::generate();
    let payload = ProtobufLedgerPayloadCodec.encode(LedgerPayload::DomainEvent(DomainEvent::Run(
        RunEvent::Admitted {
            runtime_snapshot_id: fixture.runtime_snapshot_id,
            execution_home: RuntimeHome::Cloud,
            parent_run_id: None,
            delegation_id: None,
            retry_source_message_id: None,
            retry_source_attempt_number: None,
        },
    )));
    let mut transaction = pool.begin().await.expect("begin Ledger prefix");
    sqlx::query(
        "UPDATE event_store.ledger_records
         SET conversation_seq = 2, run_seq = 2
         WHERE tenant_id = $1 AND run_id = $2 AND run_seq = 1",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .expect("move Started after Admitted");
    sqlx::query(
        "INSERT INTO event_store.ledger_records (
             record_id, schema_version, record_kind, tenant_id, conversation_id,
             conversation_seq, activity_id, run_id, run_seq, attempt_id,
             participant_id, home_epoch, producer, occurred_at_ms, committed_at_ms,
             causation_id, correlation_id, idempotency_key, payload_hash
         ) VALUES (
             $1, 'musteragent.runtime.record.v1', 1, $2, $3,
             1, $4, $5, 1, NULL,
             $6, 1, 1, $7, $7,
             NULL, $8, 'seed:admitted', $9
         )",
    )
    .bind(record_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.activity_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(fixture.requester_id.into_uuid())
    .bind(now.unix_millis())
    .bind(musteragent_identity::CorrelationId::generate().into_uuid())
    .bind(Sha256::digest(&payload).into_bytes().to_vec())
    .execute(&mut *transaction)
    .await
    .expect("seed Admitted record");
    sqlx::query(
        "INSERT INTO event_store.domain_events (record_id, encoded_event, payload_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(record_id.into_uuid())
    .bind(&payload)
    .bind(Sha256::digest(&payload).into_bytes().to_vec())
    .execute(&mut *transaction)
    .await
    .expect("seed Admitted event");
    sqlx::query(
        "UPDATE event_store.ledger_streams
         SET last_sequence = 2, updated_at_ms = $3
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(now.unix_millis())
    .execute(&mut *transaction)
    .await
    .expect("advance Ledger stream");
    sqlx::query("UPDATE runtime.runs SET last_run_sequence = 2 WHERE run_id = $1")
        .bind(fixture.run_id.into_uuid())
        .execute(&mut *transaction)
        .await
        .expect("advance Run sequence");
    transaction.commit().await.expect("commit Ledger prefix");
}

fn current_snapshot_content(
    run_id: RunId,
    active_skill_definitions: Vec<musteragent_skill::SkillDefinition>,
) -> RuntimeSnapshotContent {
    let fixture = fixture_version_v1();
    let rust_provider_id = CapabilityProviderId::generate();
    let backend_provider_id = CapabilityProviderId::generate();
    let mut tools = BuiltinToolCatalog::load_cloud()
        .expect("released Cloud Tool catalog")
        .definitions()
        .iter()
        .filter(|definition| {
            let policy = musteragent_runtime_snapshot::DISCOVERY_POLICY_V11;
            musteragent_runtime_snapshot::capability_available_for_dialogue_policy(
                policy,
                definition.key.as_str(),
            ) && musteragent_runtime_snapshot::dialogue_capability_ceiling_for_policy(
                policy,
                definition.capability_key.as_str(),
            )
        })
        .map(|definition| {
            let provider_id = match definition.target {
                CapabilityExecutionTarget::RustRuntime => rust_provider_id,
                CapabilityExecutionTarget::BackendNamed(_) => backend_provider_id,
            };
            // This fixture seals the retained v4 contract. Current pin() writes
            // activity liveness; historical snapshots carry their exact fixed timeout.
            let mut manifest = definition.pin(provider_id);
            manifest.execution_policy = musteragent_tool::ToolExecutionPolicy::LegacyFixed {
                timeout: definition.timeout,
            };
            ToolDefinitionSnapshot {
                manifest,
                execution_target: definition.target.clone(),
                concurrency_class: definition.concurrency,
                idempotency: definition.idempotency,
                file_operation: definition.file_operation.clone(),
                title: definition.title.clone(),
                description: definition.description.clone(),
                input_schema_json: definition.input_schema_json.clone(),
                output_schema_json: definition.output_schema_json.clone(),
            }
        })
        .collect::<Vec<_>>();
    tools.sort_by(|left, right| left.manifest.key.cmp(&right.manifest.key));
    let mut grants = tools
        .iter()
        .map(|tool| CapabilityGrantSnapshot {
            key: CapabilityKey::from_versioned_key(tool.manifest.capability_key.clone()),
            provider_id: tool.manifest.capability_provider_id,
            location: match tool.execution_target {
                CapabilityExecutionTarget::RustRuntime => CapabilityLocation::Cloud,
                CapabilityExecutionTarget::BackendNamed(_) => CapabilityLocation::Backend,
            },
            effect: tool.manifest.effect,
            allowed_scopes: tool.manifest.required_scopes.clone(),
            policy_decision: Decision::Allow,
        })
        .collect::<Vec<_>>();
    grants.sort_by(|left, right| left.key.cmp(&right.key));
    let authorized_capabilities = tools
        .iter()
        .map(|tool| tool.manifest.capability_key.clone())
        .collect::<BTreeSet<_>>();
    for skill in &active_skill_definitions {
        let missing = skill
            .required_capabilities
            .difference(&authorized_capabilities)
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "fixture Skill {} missing {missing:?}",
            skill.skill_ref.as_str()
        );
    }
    let skill_catalog = musteragent_skill::SkillCatalog::new(active_skill_definitions)
        .expect("installed Skill catalog");
    let skills = skill_catalog
        .authorize(&SkillAuthorizationRequest {
            surface: Surface::Web,
            agent_contract: VersionedKey::new("musteragent.runtime-contract@1")
                .expect("Agent contract"),
            authorized_capabilities: authorized_capabilities.clone(),
        })
        .expect("authorized Skills");
    let disclosure = skill_catalog
        .select_initial_disclosures(
            &skills,
            &SkillDisclosureRequest {
                explicitly_selected_skill_refs: BTreeSet::new(),
            },
        )
        .expect("initial disclosures");
    let authorized_tools = authorized_tool_catalog(&tools, &grants).expect("authorized Tools");
    let authorized_skill_catalog_hash = hash_skill_catalog(&skills);
    let model_provider_id: ModelProviderId = "019fa6de-38fd-7000-8000-000000000501"
        .parse()
        .expect("model provider");
    let model_entry_id: ModelEntryId = "019fa6de-38fd-7000-8000-000000000502"
        .parse()
        .expect("model entry");
    let model = model_route(
        "cloud.skill-discovery@1",
        "skill-discovery-model",
        model_provider_id,
        model_entry_id,
    );
    let resolved_context_budgets = ContextCompactionPolicyV2Spec::calibrated_for_route(&model)
        .resolve(&model)
        .expect("resolve pinned @2 Context budgets");
    let compression = model_route(
        "cloud.skill-discovery-compression@1",
        "skill-discovery-compression",
        model_provider_id,
        model_entry_id,
    );
    // Pin the active exact versions while preserving the frozen v8 logical profile.
    let bootstrap_tool_keys = [
        "tool.catalog.search",
        "tool.catalog.activate",
        "interaction.request",
        "skills.search",
        "skills.list",
        "skills.view",
        "plan.get",
        "plan.update",
        "context.read",
        "delegation.inspect",
        "files.list",
        "files.inspect",
        "files.read",
    ]
    .into_iter()
    .map(|logical| {
        tools
            .iter()
            .find(|tool| tool.manifest.key.logical_key() == logical)
            .expect("bootstrap tool in current catalog")
            .manifest
            .key
            .clone()
    })
    .collect::<Vec<_>>();
    let mut content = RuntimeSnapshotContent {
        owner_goal_authority: None,
        application_catalogs: vec![],
        product_release: None,
        schema_version: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("schema"),
        agent: {
            let mut agent = AgentVersionSnapshot::from_resolved(
                &musteragent_conformance::fixtures::agent::system_definition(),
                &fixture,
            )
            .expect("Agent Snapshot");
            agent.resolved_v5 = None;
            agent
        },
        prompt: PromptAssemblySnapshot {
            sections: vec![
                PromptSectionSnapshot {
                    section_key: DottedKey::new("platform.safety").expect("section"),
                    content_ref: fixture_runtime_policy_content_ref().expect("policy content"),
                },
                PromptSectionSnapshot {
                    section_key: DottedKey::new("musteragent.legacy.persona").expect("section"),
                    content_ref: fixture.persona_content_ref,
                },
            ],
            assembled_hash: Sha256::digest(
                [FIXTURE_RUNTIME_POLICY_V1, FIXTURE_PERSONA_V1].concat(),
            ),
        },
        model: ModelRouteSnapshot {
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            route: model,
            credential_grant_ref: CredentialGrantRef::new(format!("grant_{run_id}"))
                .expect("grant"),
            fallback_candidates: Vec::new(),
        },
        auxiliary_models: vec![PinnedAuxiliaryModelRoute {
            role: AuxiliaryModelRole::Compression,
            model: PinnedModelRoute {
                route: compression,
                credential_grant_ref: CredentialGrantRef::new(format!(
                    "grant_compression_{run_id}"
                ))
                .expect("compression grant"),
            },
        }],
        skills,
        tools,
        capabilities: grants,
        policy: PolicySnapshot {
            policy_ref: VersionedKey::new("musteragent.legacy.runtime-policy@1").expect("policy"),
            policy_hash: Sha256::digest(FIXTURE_RUNTIME_POLICY_V1),
            permission_mode: musteragent_policy::PermissionMode::Supervised,
        },
        memory: MemorySelectionSnapshot {
            selected: Vec::new(),
            selection_policy_ref: VersionedKey::new("musteragent.legacy.memory-policy@1")
                .expect("memory"),
            selection_policy_hash: Sha256::digest(b"musteragent.legacy.memory-policy@1"),
        },
        instincts: Vec::new(),
        context_policy: ContextPolicySnapshot {
            max_input_tokens: resolved_context_budgets.hard_input_budget,
            compaction_trigger_tokens: resolved_context_budgets.trigger_budget,
            reserved_output_tokens: resolved_context_budgets.reserved_output_budget,
            compaction_policy_ref: VersionedKey::new(CONTEXT_COMPACTION_POLICY_V2)
                .expect("@2 compaction"),
            resolved_v2: Some(resolved_context_budgets),
            source_selection: None,
        },
        output_policy: OutputPolicySnapshot {
            reasoning_summary: false,
        },
        execution_policy: RunExecutionPolicySnapshot::legacy_defaults(),
        protocol_versions: ProtocolVersionSnapshot {
            command_schema: DottedKey::new(musteragent_wire::COMMAND_SCHEMA_VERSION)
                .expect("command"),
            record_schema: DottedKey::new(musteragent_wire::RECORD_SCHEMA_VERSION).expect("record"),
            snapshot_schema: DottedKey::new(SNAPSHOT_SCHEMA_VERSION).expect("snapshot"),
            model_protocol: VersionedKey::new("musteragent.legacy.model.canonical@1")
                .expect("model protocol"),
        },
        discovery: Some(DiscoverySnapshot {
            authorized_skills: Vec::new(),
            authorized_skill_catalog_hash,
            authorized_tool_catalog_hash: authorized_tools.catalog_hash(),
            bootstrap_tool_keys: bootstrap_tool_keys.clone(),
            initial_exposed_tool_keys: bootstrap_tool_keys,
            initial_disclosed_skill_refs: disclosure.initial_disclosed_skill_refs,
            default_disclosed_skill_refs: Vec::new(),
            explicitly_selected_skill_refs: disclosure.explicitly_selected_skill_refs,
            carry_forward_source_run_id: None,
            carry_forward_decision_ref: None,
            discovery_policy_ref: VersionedKey::new(
                musteragent_runtime_snapshot::DISCOVERY_POLICY_V11,
            )
            .expect("discovery policy"),
            max_exposed_tools: NonZeroU32::new(64).expect("limit"),
            max_search_results: NonZeroU32::new(20).expect("limit"),
        }),
    };
    skill_catalog_snapshot::pin_current_snapshot_with_skills(&mut content, &fixture_version_v1());
    content
}

fn model_route(
    route_ref: &str,
    model_key: &str,
    model_provider_id: ModelProviderId,
    model_entry_id: ModelEntryId,
) -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new(route_ref).expect("route"),
        resolved_model_key: model_key.to_owned(),
        model_provider_id,
        model_entry_id,
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(64_000).expect("context"),
        max_output_tokens: NonZeroU64::new(4_000).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:skill-discovery.price@1")
            .expect("pricing"),
        provider_state_policy: ProviderStatePolicy::Stateless,
    }
}

async fn seed_running_view_invocation(
    pool: &PgPool,
    fixture: running_loop::Fixture,
    attempt: &musteragent_run::AttemptContext,
    invocation_id: InvocationId,
    provider_id: CapabilityProviderId,
    request_ref: &ContentRef,
    now: UtcTimestamp,
) {
    sqlx::query(
        "INSERT INTO tool.tool_invocations (
             invocation_id, tenant_id, conversation_id, run_id,
             requested_by_attempt_id, execution_attempt_id, tool_call_id,
             tool_ref, capability_provider_id, status, effect, approval_policy,
             retry_contract, idempotency_key, request_content_id, request_hash,
             deadline_ms, started_at_ms
         ) VALUES (
             $1, $2, $3, $4, $5, $5, $6,
             'skills.view@2', $7, 3, 2, 1,
             2, $8, $9, $10, $11, $12
         )",
    )
    .bind(invocation_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .bind(fixture.run_id.into_uuid())
    .bind(attempt.attempt_id.into_uuid())
    .bind(ToolCallId::new("skill-view-1").expect("Tool call").as_str())
    .bind(provider_id.into_uuid())
    .bind(
        IdempotencyKey::new(format!("tool:{invocation_id}"))
            .expect("idempotency")
            .as_str(),
    )
    .bind(request_ref.content_id.into_uuid())
    .bind(request_ref.sha256.into_bytes().to_vec())
    .bind(now.unix_millis().saturating_add(60_000))
    .bind(now.unix_millis())
    .execute(pool)
    .await
    .expect("seed skills.view invocation");
}

fn content_ref(bytes: &[u8], media_type: &str) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new(media_type).expect("media type"),
        byte_length: u64::try_from(bytes.len()).expect("length"),
        sha256: Sha256::digest(bytes),
    }
}

async fn seed_content(
    pool: &PgPool,
    tenant_id: musteragent_identity::TenantId,
    reference: &ContentRef,
) {
    sqlx::query(
        "INSERT INTO artifact.content_refs (
             content_id, tenant_id, media_type, byte_length, content_hash,
             storage_ref, created_at_ms, state
         ) VALUES ($1, $2, $3, $4::numeric, $5, $6, 1, 2)",
    )
    .bind(reference.content_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(reference.media_type.as_str())
    .bind(reference.byte_length.to_string())
    .bind(reference.sha256.into_bytes().to_vec())
    .bind(format!("test://{}", reference.content_id))
    .execute(pool)
    .await
    .expect("seed ContentRef");
}

async fn assert_atomic_state(
    pool: &PgPool,
    run_id: RunId,
    invocation_id: InvocationId,
    expected_status: i16,
    expected_disclosures: i64,
    expected_dynamic_records: i64,
) {
    let state: (i16, i64, i64) = sqlx::query_as(
        "SELECT status,
                (SELECT count(*) FROM runtime.skill_disclosures WHERE run_id = $2),
                (SELECT count(*) FROM event_store.ledger_records
                  WHERE run_id = $2 AND run_seq > 2)
         FROM tool.tool_invocations WHERE invocation_id = $1",
    )
    .bind(invocation_id.into_uuid())
    .bind(run_id.into_uuid())
    .fetch_one(pool)
    .await
    .expect("read atomic disclosure state");
    assert_eq!(
        state,
        (
            expected_status,
            expected_disclosures,
            expected_dynamic_records
        )
    );
}

async fn install_audit_failure(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE FUNCTION security_audit.reject_skill_disclosure()
         RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.action = 10 THEN
                 RAISE EXCEPTION 'injected Skill disclosure Audit failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER reject_skill_disclosure
         BEFORE INSERT ON security_audit.records
         FOR EACH ROW EXECUTE FUNCTION security_audit.reject_skill_disclosure();",
    )
    .execute(pool)
    .await
    .expect("install Audit failure");
}

async fn remove_audit_failure(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP TRIGGER reject_skill_disclosure ON security_audit.records;
         DROP FUNCTION security_audit.reject_skill_disclosure();",
    )
    .execute(pool)
    .await
    .expect("remove Audit failure");
}
