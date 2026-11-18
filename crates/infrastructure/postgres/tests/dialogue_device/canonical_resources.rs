//! Full migration + real original Resource/Content/Edit/Artifact owners.
//! Only the trusted Native signature/journal boundary is substituted by fixture
//! data. This does not claim an installed OS Native host passed acceptance.
use super::*;
use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_policy::Decision;
use musteragent_postgres::{
    ContentGcPolicy, PostgresArtifactStore, PostgresDeviceResourceStore,
    PostgresPrivacyErasureStore,
};
use musteragent_run::{AttemptContext, RunContext, WorkerId};
use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};
use musteragent_runtime_privacy::*;
use musteragent_tool::{BuiltinToolCatalog, ToolKey};
use serde_json::{Value, json};

#[path = "canonical_resources/goals.rs"]
mod goals;
#[path = "canonical_resources/memory.rs"]
mod memory;
#[path = "canonical_resources/source_tools.rs"]
mod source_tools;
#[path = "canonical_resources/tasks.rs"]
mod tasks;
#[path = "canonical_resources/tasks_owner.rs"]
mod tasks_owner;

const TOOLS: &[&str] = &[
    "tasks.start@3",
    "tasks.list@1",
    "tasks.inspect@1",
    "tasks.control@1",
    "tasks.amend@2",
    "tasks.handoff@1",
    "tasks.followup@1",
    "goal.sources.catalog@1",
    "goal.sources.resources@1",
    "goal.sources.configure@1",
    "goal.sources.list@1",
    "goal.draft.save@1",
    "goal.create@3",
    "goal.get@3",
    "goal.list@2",
    "goal.update@3",
    "goal.change_status@2",
    "user_memory.remember@2",
    "experience.propose@3",
    "experience.publish@1",
    "memory.search@1",
    "memory.list@1",
    "memory.get@1",
    "experience.search@1",
    "experience.inspect@1",
    "tasks.executions.list@2",
    "resource.file.create@4",
    "files.list@6",
    "files.search@5",
    "files.inspect@6",
    "files.read@6",
    "resource.file.read@5",
    "resource.file.edit.begin@5",
    "resource.file.edit.append@4",
    "resource.file.edit.patch@6",
    "resource.file.edit.resume@5",
    "resource.file.edit.finalize@4",
    "resource.file.edit.abort@4",
    "resource.search@4",
    "resource.tree.list@4",
    "resource.folder.create@5",
    "resource.node.rename@5",
    "resource.node.move@4",
    "resource.node.trash@4",
    "resource.node.restore@4",
    "resource.change-set.commit@6",
];
struct Canonical {
    f: Fixture,
    store: PostgresDeviceResourceStore,
    provider: CapabilityProviderId,
}
impl Canonical {
    async fn new() -> Self {
        Self::with_fixture(Fixture::new().await).await
    }
    async fn with_fixture(mut f: Fixture) -> Self {
        let provider = musteragent_tool::BUILTIN_RUST_PROVIDER_ID.parse().unwrap();
        let mut claims = grant(&f.custody, now(&f.pool).await);
        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        let catalog = BuiltinToolCatalog::for_product_release(
            &release,
            musteragent_tool::ProductExecutionTarget::Device,
        )
        .unwrap();
        let mut entries = std::collections::BTreeMap::new();
        for key in TOOLS {
            let definition = catalog
                .by_key(&ToolKey::from_versioned_key(
                    VersionedKey::new(*key).unwrap(),
                ))
                .unwrap_or_else(|| panic!("current kernel catalog must include {key}"));
            entries.insert(
                definition.capability_key.as_str().to_string(),
                CapabilityEntitlement {
                    key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
                    provider_id: provider,
                    location: CapabilityLocation::Device,
                    effect: definition.effect,
                    allowed_scopes: definition.required_scopes.clone(),
                    policy_decision: Decision::Allow,
                },
            );
        }
        claims.product_release = Some(release);
        claims.capability_entitlements = entries.into_values().collect();
        let proof = f
            .queue
            .bind_device_grant(f.claim, sign(claims))
            .await
            .unwrap();
        f.binding.grant_proof_hash = Sha256::digest(&proof.encoded_proof);
        f.queue
            .acknowledge_device_admission(f.actor, &f.target, &f.binding)
            .await
            .unwrap();
        let key = DottedKey::new("test.canonical.device.resources").unwrap();
        let store = PostgresDeviceResourceStore::new(
            f.pool.clone(),
            f.content.clone(),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [61; 32])]).unwrap()),
        );
        Self { f, store, provider }
    }
    async fn request(&self, key: &str, arguments: Value) -> DeviceResourceRequest {
        let p = &self.f.custody.source;
        let b = &self.f.binding;
        let ids =
            musteragent_runtime_admission::AdmissionIdentities::derive(p.identities.command_id)
                .unwrap();
        let home = RuntimeHome::Device {
            device_id: b.device_id,
        };
        let run = RunContext {
            tenant_id: self.f.actor.tenant_id,
            conversation_id: p.identities.conversation_id,
            activity_id: ids.activity_id,
            activity_kind: musteragent_activity::ActivityKind::Chat,
            run_id: b.run_id,
            executor_participant_id: p.identities.executor,
            requested_by_participant_id: p.identities.requested_by,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: ExecutionScope::DevicePrivate {
                device_id: b.device_id,
                workspace_id: None,
            },
            control_home: home,
            execution_home: home,
            home_epoch: HomeEpoch::new(1).unwrap(),
            runtime_snapshot_id: ids.runtime_snapshot_id,
        };
        let release = musteragent_tool::compiled_product_release(ProductId::KERNEL);
        let catalog = BuiltinToolCatalog::for_product_release(
            &release,
            musteragent_tool::ProductExecutionTarget::Device,
        )
        .unwrap();
        let tool = ToolKey::from_versioned_key(VersionedKey::new(key).unwrap());
        let definition = catalog.by_key(&tool).unwrap();
        let bytes = serde_json::to_vec(&arguments).unwrap();
        let now = now(&self.f.pool).await;
        let invocation = InvocationId::generate();
        let attempt = AttemptId::generate();
        let command = DeviceResourceCommand::new(
            b.clone(),
            DeviceResourceInvocation {
                run_context: run,
                timing: InvocationExecutionTiming::LegacyFixed {
                    deadline: UtcTimestamp::from_unix_millis(now + 120_000),
                },
                invocation_id: invocation,
                requested_by_attempt_id: attempt,
                tool_call_id: ToolCallId::new(format!("call_{invocation}")).unwrap(),
                tool_key: tool,
                capability_key: CapabilityKey::from_versioned_key(
                    definition.capability_key.clone(),
                ),
                provider_id: self.provider,
                idempotency_key: IdempotencyKey::new(format!("invocation:{invocation}")).unwrap(),
                request_ref: reference(&bytes),
                request_schema: definition.input_schema.clone(),
                result_schema: definition.output_schema.clone(),
                authorized_content: vec![],
                memory: DeviceResourceOperation::from_tool_key(key)
                    .unwrap()
                    .is_memory()
                    .then(|| DeviceMemoryContext {
                        evidence_record_id: LedgerRecordId::generate(),
                        approval_interaction_id: None,
                        user_origin: (key == "user_memory.remember@2").then(|| {
                            DeviceMemoryUserOrigin {
                                record_id: ids.message_record_id,
                                content_ref: p.intent.content_ref.clone(),
                            }
                        }),
                    }),
            },
            bytes,
        )
        .unwrap();
        DeviceResourceRequest::new(
            command,
            AttemptContext {
                attempt_id: attempt,
                run_id: b.run_id,
                attempt_number: AttemptNumber::new(1).unwrap(),
                worker_id: WorkerId::new("native-conformance").unwrap(),
                lease_id: LeaseId::generate(),
                fencing_token: FencingToken::new(1).unwrap(),
                lease_expires_at: UtcTimestamp::from_unix_millis(now + 60_000),
            },
        )
        .unwrap()
    }
    async fn execute(&self, request: &DeviceResourceRequest) -> DeviceResourceReceipt {
        self.store
            .execute_resource_tool(self.f.actor, &self.f.target, request)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "{} failed: {error:?}",
                    request.command().invocation().tool_key.as_str()
                )
            })
    }
    async fn create(&self) -> (DeviceResourceRequest, DeviceResourceReceipt) {
        let request=self.request("resource.file.create@4",json!({"path":"验收备注.txt","media_type":"text/plain","content":"First canonical line.\n"})).await;
        let receipt = self.execute(&request).await;
        (request, receipt)
    }
    fn publication(
        &self,
        command: &DeviceResourceCommand,
        receipt: &DeviceResourceReceipt,
    ) -> (DeviceToolDelivery, DeviceResourceDeliveryManifest) {
        let version = receipt.success().unwrap().produced_versions[0].clone();
        let inv = command.invocation();
        let args=serde_json::to_vec(&json!({"purpose":"reply","artifacts":[{"artifact_ref":{"kind":"file_node","id":version.node_id.to_string()},"artifact_tool_call_id":inv.tool_call_id.as_str()}]})).unwrap();
        let delivery = DeviceToolDelivery::new(
            self.f.binding.clone(),
            InvocationId::generate(),
            ARTIFACTS_DELIVER_TOOL.into(),
            reference(&args),
            args,
        )
        .unwrap();
        let proof = DeviceResourceDeliveryReference::new(
            DeviceArtifactExportRole::Delivery,
            command,
            receipt,
            version,
            DeviceArtifactProducer {
                invocation_id: inv.invocation_id,
                tool_call_id: inv.tool_call_id.clone(),
                tool_key: VersionedKey::new(inv.tool_key.as_str()).unwrap(),
                result_ref: receipt.success().unwrap().result_ref.clone(),
                terminal_record_id: LedgerRecordId::generate(),
                terminal_record_hash: Sha256::digest(
                    b"signed Native successful Tool terminal fixture",
                ),
                terminal_run_sequence: RunSeq::new(8).unwrap(),
                terminal_at: receipt.committed_at,
            },
        )
        .unwrap();
        let manifest = DeviceResourceDeliveryManifest {
            request_fingerprint: delivery.fingerprint(),
            entries: vec![proof],
        };
        (delivery, manifest)
    }
    fn artifacts(&self) -> PostgresArtifactStore {
        let key = DottedKey::new("test.canonical.read").unwrap();
        PostgresArtifactStore::new(
            self.f.pool.clone(),
            Arc::new(OpaqueRefKeyring::new(key.clone(), [(key, [62; 32])]).unwrap()),
        )
    }
}
fn reference(bytes: &[u8]) -> ContentRef {
    ContentRef {
        content_id: ContentId::generate(),
        media_type: MediaType::new("application/json").unwrap(),
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(bytes),
    }
}

#[tokio::test]
async fn canonical_device_create_edit_publish_read_history_and_exact_renewed_replay() {
    let f = Canonical::new().await;
    let (create, first) = f.create().await;
    assert_eq!(first.success().unwrap().produced_versions.len(), 1);
    let version = &first.success().unwrap().produced_versions[0];
    let renewed = DeviceResourceRequest::new(
        create.command().clone(),
        AttemptContext {
            attempt_id: AttemptId::generate(),
            attempt_number: AttemptNumber::new(2).unwrap(),
            lease_id: LeaseId::generate(),
            fencing_token: FencingToken::new(2).unwrap(),
            ..create.attempt().clone()
        },
    )
    .unwrap();
    assert_eq!(f.execute(&renewed).await, first);
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &renewed)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let read = f
        .request(
            "resource.file.read@5",
            json!({"file_node_id":version.node_id.to_string()}),
        )
        .await;
    let read_receipt = f.execute(&read).await;
    assert!(
        String::from_utf8(read_receipt.success().unwrap().result_bytes.clone())
            .unwrap()
            .contains("First canonical line")
    );
    let (delivery, manifest) = f.publication(create.command(), &first);
    let published =
        f.f.publisher
            .publish_device_resources(f.f.actor, &f.f.target, &delivery, &manifest)
            .await
            .unwrap();
    assert_eq!(
        f.f.publisher
            .lookup_device_resource_delivery(f.f.actor, &f.f.target, &delivery, &manifest)
            .await
            .unwrap(),
        Some(published.clone())
    );
    let begin = f
        .request(
            "resource.file.edit.begin@5",
            json!({"file_node_id":version.node_id.to_string(),"base_version":1}),
        )
        .await;
    let begun = f.execute(&begin).await;
    let value: Value = serde_json::from_slice(&begun.success().unwrap().result_bytes).unwrap();
    let session = value["session_id"].as_str().unwrap();
    f.execute(
        &f.request(
            "resource.file.edit.append@4",
            json!({"session_id":session,"text":"Second canonical line.\n"}),
        )
        .await,
    )
    .await;
    let finalize = f
        .request(
            "resource.file.edit.finalize@4",
            json!({"session_id":session}),
        )
        .await;
    let second = f.execute(&finalize).await;
    assert_eq!(second.success().unwrap().produced_versions.len(), 1);
    assert_eq!(
        second.success().unwrap().produced_versions[0].artifact_id,
        version.artifact_id
    );
    assert_eq!(
        second.success().unwrap().produced_versions[0].version.get(),
        2
    );
    assert_eq!(
        f.execute(&create).await,
        first,
        "late ACK recovery never rewinds canonical head"
    );
    let entry = published.publication.entry_ids[0];
    let reader = f.artifacts();
    let dialogue = f.f.custody.source.scope.dialogue_id;
    let summary = reader
        .describe(f.f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    assert_eq!(summary.current_version.version.get(), 2);
    let history = reader
        .history(
            AgentArtifactHistoryQuery {
                actor: f.f.actor,
                dialogue_id: dialogue,
                entry_id: entry,
                at_version: None,
                before_version: None,
                limit: 20,
            },
            CorrelationId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(history.versions.len(), 2);
    for item in history.versions {
        assert!(
            matches!(item.author,AgentArtifactAuthor::Agent{agent_id,..} if agent_id==f.f.custody.source.scope.agent_id)
        );
    }
    let facts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM artifact.artifacts),(SELECT count(*) FROM artifact.device_resource_versions),(SELECT count(*) FROM runtime.runs),(SELECT count(*) FROM tool.tool_invocations)").fetch_one(&f.f.pool).await.unwrap();
    assert_eq!(facts, (1, 2, 0, 0));
}

#[tokio::test]
async fn canonical_device_wrong_commitment_foreign_and_revoked_fail_closed() {
    let f = Canonical::new().await;
    let (request, first) = f.create().await;
    let mut inv = request.command().invocation().clone();
    let bytes =
        br#"{"path":"changed.txt","media_type":"text/plain","content":"different"}"#.to_vec();
    inv.request_ref = reference(&bytes);
    let changed = DeviceResourceRequest::new(
        DeviceResourceCommand::new(f.f.binding.clone(), inv, bytes).unwrap(),
        request.attempt().clone(),
    )
    .unwrap();
    assert_eq!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &changed)
            .await,
        Err(DeviceResourceError::Conflict)
    );
    assert!(
        f.store
            .lookup_resource_tool(actor(), &f.f.target, &request)
            .await
            .is_err()
    );
    // Only a permission reduction fences admitted native work (2345e0acca).
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_revision=permission_revision+1,permission_mode='supervised' WHERE agent_id=$1").bind(f.f.custody.source.scope.agent_id.into_uuid()).execute(&f.f.pool).await.unwrap();
    assert!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &request)
            .await
            .is_err()
    );
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &request)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id=$1"
        )
        .bind(
            first.success().unwrap().produced_versions[0]
                .artifact_id
                .into_uuid()
        )
        .fetch_one(&f.f.pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn canonical_device_read_replay_denies_trash_and_privacy_contains_receipts() {
    let f = Canonical::new().await;
    let (created, receipt) = f.create().await;
    let version = &receipt.success().unwrap().produced_versions[0];
    let read = f
        .request(
            "resource.file.read@5",
            json!({"file_node_id":version.node_id.to_string()}),
        )
        .await;
    f.execute(&read).await;
    let roots = PostgresPrivacyErasureStore::new(f.f.pool.clone())
        .device_artifact_erasure_roots(f.f.actor, Some(f.f.custody.source.scope.agent_id))
        .await
        .unwrap();
    assert!(roots.contains(&version.content_ref));
    assert!(roots.contains(&receipt.success().unwrap().result_ref));
    assert!(roots.contains(&created.command().invocation().request_ref));
    sqlx::query("UPDATE resource.nodes SET lifecycle=2 WHERE node_id=$1")
        .bind(version.node_id.into_uuid())
        .execute(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &read)
            .await,
        Err(DeviceResourceError::ReferenceUnavailable)
    );
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &created)
            .await
            .unwrap(),
        Some(receipt),
        "write receipt is a committed fact, not a live-byte authorization"
    );
}

#[tokio::test]
async fn canonical_device_logical_rejection_is_durable_and_mutation_is_rolled_back() {
    let f = Canonical::new().await;
    let invalid = f
        .request("resource.file.create@4", json!({"path":"broken.txt"}))
        .await;
    let rejected = f.execute(&invalid).await;
    assert_eq!(
        rejected.rejection(),
        Some(DeviceResourceRejectionCode::InvalidArguments)
    );
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &invalid)
            .await
            .unwrap(),
        Some(rejected.clone())
    );
    assert_eq!(f.execute(&invalid).await, rejected);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM artifact.artifacts")
            .fetch_one(&f.f.pool)
            .await
            .unwrap(),
        0
    );
    let (_, created) = f.create().await;
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM resource.nodes")
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    let conflict = f
        .request(
            "resource.folder.create@5",
            json!({"name":"must roll back","expected_revision":0}),
        )
        .await;
    let rejected = f.execute(&conflict).await;
    assert_eq!(
        rejected.rejection(),
        Some(DeviceResourceRejectionCode::Conflict)
    );
    assert_eq!(f.execute(&conflict).await, rejected);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM resource.nodes")
            .fetch_one(&f.f.pool)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM artifact.artifact_versions WHERE artifact_id=$1"
        )
        .bind(
            created.success().unwrap().produced_versions[0]
                .artifact_id
                .into_uuid()
        )
        .fetch_one(&f.f.pool)
        .await
        .unwrap(),
        1
    );
    let outcomes: Vec<String> =
        sqlx::query_scalar("SELECT outcome FROM resource.device_commands ORDER BY outcome")
            .fetch_all(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(outcomes, vec!["rejected", "rejected", "succeeded"]);
}

#[tokio::test]
async fn canonical_device_content_gc_and_original_privacy_erase_include_read_and_terminal_receipts()
{
    let f = Canonical::new().await;
    let (create, first) = f.create().await;
    let root = &first.success().unwrap().produced_versions[0].content_ref;
    let read=f.request("resource.file.read@5",json!({"file_node_id":first.success().unwrap().produced_versions[0].node_id.to_string()})).await;
    let read_receipt = f.execute(&read).await;
    assert!(read_receipt.read_dependencies.contains(root));
    let (delivery, manifest) = f.publication(create.command(), &first);
    let publication =
        f.f.publisher
            .publish_device_resources(f.f.actor, &f.f.target, &delivery, &manifest)
            .await
            .unwrap();
    let orphan =
        f.f.content
            .put(PutContent {
                tenant_id: f.f.actor.tenant_id,
                media_type: MediaType::new("text/plain").unwrap(),
                bytes: b"orphan GC candidate".to_vec(),
            })
            .await
            .unwrap();
    sqlx::query("UPDATE artifact.content_refs SET gc_eligible_at_ms=0 WHERE tenant_id=$1")
        .bind(f.f.actor.tenant_id.into_uuid())
        .execute(&f.f.pool)
        .await
        .unwrap();
    let report =
        f.f.content
            .sweep_unreferenced_content(
                UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
                ContentGcPolicy::default(),
            )
            .await
            .unwrap();
    assert!(report.reclaimed >= 1);
    assert!(
        f.f.content
            .read(f.f.actor.tenant_id, &orphan)
            .await
            .is_err()
    );
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &read)
            .await
            .unwrap(),
        Some(read_receipt.clone())
    );
    f.f.content.delete(f.f.actor.tenant_id, root).await.unwrap();
    assert!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &read)
            .await
            .is_err()
    );
    assert_eq!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &create)
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        f.f.publisher
            .lookup_device_resource_delivery(f.f.actor, &f.f.target, &delivery, &manifest)
            .await
            .unwrap(),
        Some(publication)
    );
    let privacy = PostgresPrivacyErasureStore::new(f.f.pool.clone());
    let roots = privacy
        .device_artifact_erasure_roots(f.f.actor, Some(f.f.custody.source.scope.agent_id))
        .await
        .unwrap();
    assert!(roots.contains(root));
    assert!(roots.contains(&read_receipt.success().unwrap().result_ref));
    let base = now(&f.f.pool).await - 100;
    let request = ErasureRequest {
        schema_version: DottedKey::new("test.canonical-resource-erasure.v1").unwrap(),
        erasure_job_id: ErasureJobId::generate(),
        tenant_id: f.f.actor.tenant_id,
        requested_by: f.f.actor,
        root_resource_refs: roots.iter().cloned().map(ResourceRef::Content).collect(),
        retention_policy_ref: DataRetentionPolicyRef::new("privacy.content.standard@1").unwrap(),
        idempotency_key: IdempotencyKey::new("erase-canonical-resource-content").unwrap(),
        requested_at: UtcTimestamp::from_unix_millis(base),
        complete_by: UtcTimestamp::from_unix_millis(base + 60_000),
    };
    privacy.create(request.clone()).await.unwrap();
    let plan = ErasurePlanSnapshot::new(
        request.schema_version.clone(),
        request.erasure_job_id,
        ErasurePlanVersion::new(1).unwrap(),
        request.root_resource_refs.clone(),
        vec![ErasureDataClass::ContentCiphertext],
        vec![],
        request.fingerprint().unwrap(),
        UtcTimestamp::from_unix_millis(base + 1),
    )
    .unwrap();
    privacy
        .append_plan(f.f.actor.tenant_id, plan)
        .await
        .unwrap();
    for (sequence, status) in [
        (2, ErasureJobStatus::Planned),
        (3, ErasureJobStatus::Fenced),
    ] {
        privacy
            .append_transition(
                f.f.actor.tenant_id,
                request.erasure_job_id,
                ErasureTransition {
                    sequence: ErasureTransitionSequence::new(sequence).unwrap(),
                    status,
                    occurred_at: UtcTimestamp::from_unix_millis(base + sequence as i64),
                },
            )
            .await
            .unwrap();
    }
    let completed = ErasureExecutor::new(&privacy, f.f.content.as_ref())
        .execute(
            f.f.actor.tenant_id,
            request.erasure_job_id,
            UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
        )
        .await
        .unwrap();
    assert_eq!(completed.job.status(), Some(ErasureJobStatus::Completed));
    for reference in &roots {
        assert!(
            f.f.content
                .read(f.f.actor.tenant_id, reference)
                .await
                .is_err()
        );
    }
    assert!(
        f.store
            .lookup_resource_tool(f.f.actor, &f.f.target, &create)
            .await
            .is_err()
    );
    assert!(
        f.f.publisher
            .lookup_device_resource_delivery(f.f.actor, &f.f.target, &delivery, &manifest)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM resource.device_commands")
            .fetch_one(&f.f.pool)
            .await
            .unwrap(),
        2,
        "immutable metadata does not obstruct original Content erasure"
    );
}

#[path = "native_inputs.rs"]
mod native_inputs;

#[path = "native_executions.rs"]
mod native_executions;
#[path = "native_file_production.rs"]
mod native_file_production;

#[path = "native_file_redelivery.rs"]
mod native_file_redelivery;
