//! Real original canonical mutation/read and signed-boundary publication owners.
//! No production Tool receipt is synthesized to deliver an earlier Run's file.
use super::*;
#[path = "publication_diagnostics.rs"]
mod diagnostics;

async fn query(f: &Canonical, key: &str, args: Value) -> Value {
    let receipt = f.execute(&f.request(key, args).await).await;
    serde_json::from_slice(&receipt.success().expect("canonical query").result_bytes).unwrap()
}
async fn next_intent(f: Canonical) -> Canonical {
    let mut original = f.f;
    let mut input = input(original.actor, original.custody.source.scope.dialogue_id);
    input.origin = DialogueInputOrigin {
        surface: DialogueInputSurface::Desktop,
        installation_id: Some(original.binding.installation_id),
    };
    input.accepted_at = UtcTimestamp::from_unix_millis(now(&original.pool).await);
    input.content_ref = original
        .content
        .put(PutContent {
            tenant_id: original.actor.tenant_id,
            media_type: MediaType::new("text/plain").unwrap(),
            bytes: b"Deliver the same previous file".to_vec(),
        })
        .await
        .unwrap();
    let intent = PostgresAgentDialogueStore::new(original.pool.clone())
        .accept_on_target(
            input,
            DialogueIntentExecutionTarget::OriginDevice(original.target.clone()),
        )
        .await
        .unwrap()
        .intent
        .intent_id;
    let claim = original
        .queue
        .claim_device(
            original.actor,
            original.target.clone(),
            intent,
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    let custody = original.queue.capture_device_source(claim).await.unwrap();
    original.binding.intent_id = intent;
    original.binding.run_id = custody.source.identities.run_id().unwrap();
    original.binding.source_digest = custody.source_digest;
    original.binding.grant_proof_hash = Sha256::digest(b"awaiting current qualified grant");
    original.claim = claim;
    original.custody = custody;
    Canonical::with_fixture(original).await
}
fn delivery(f: &Canonical, file: Value) -> DeviceToolDelivery {
    let bytes =
        serde_json::to_vec(&json!({"files":[file],"purpose":"reply","caption":"Previous file"}))
            .unwrap();
    DeviceToolDelivery::new(
        f.f.binding.clone(),
        InvocationId::generate(),
        ARTIFACTS_DELIVER_V2_TOOL.into(),
        reference(&bytes),
        bytes,
    )
    .unwrap()
}

#[tokio::test]
async fn native_exact_file_redelivery_uses_next_intent_and_replays_original_version_after_edit() {
    let _diagnostics = diagnostics::install();
    let f = Canonical::new().await;
    let (create, created) = f.create().await;
    let old = created.success().unwrap().produced_versions[0].clone();
    let (original, manifest) = f.publication(create.command(), &created);
    let first =
        f.f.publisher
            .publish_device_resources(f.f.actor, &f.f.target, &original, &manifest)
            .await
            .unwrap();
    let original_entry = *first.publication.entry_ids.last().unwrap();
    assert_public_file_read_chain(&f, original_entry, original_entry, &old).await;
    let producer_run = f.f.binding.run_id;
    let f = next_intent(f).await;
    assert_ne!(f.f.binding.run_id, producer_run);
    let listed = query(&f, "files.list@6", json!({"scope":"owned_agent"})).await;
    let exact = listed["entries"][0]["file_ref"].clone();
    let request = delivery(&f, exact.clone());
    let (a, b) = tokio::join!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &request),
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &request)
    );
    let committed = a.unwrap();
    assert_eq!(committed, b.unwrap());
    committed.validate_for(&request).unwrap();
    let versions:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT e.artifact_version_id FROM agent.agent_dialogue_entries e JOIN agent.dialogue_publications p USING(entry_id) WHERE p.source_tool_invocation_id=$1 AND e.artifact_version_id IS NOT NULL")
        .bind(request.invocation_id().into_uuid()).fetch_all(&f.f.pool).await.unwrap();
    assert_eq!(versions, vec![old.artifact_version_id.into_uuid()]);
    let delivered_entry = *committed.entry_ids.last().unwrap();
    assert_public_file_read_chain(&f, delivered_entry, delivered_entry, &old).await;
    // Both cards remain readable; the directory chooses only the newest delivery.
    assert_public_file_read_chain(&f, original_entry, delivered_entry, &old).await;
    let begin = query(
        &f,
        "resource.file.edit.begin@5",
        json!({"file_node_id":old.node_id.to_string(),"base_version":1}),
    )
    .await;
    query(
        &f,
        "resource.file.edit.append@4",
        json!({"session_id":begin["session_id"],"text":"New revision"}),
    )
    .await;
    query(
        &f,
        "resource.file.edit.finalize@4",
        json!({"session_id":begin["session_id"]}),
    )
    .await;
    assert_eq!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &request)
            .await
            .unwrap(),
        committed,
        "lost ACK must replay the original exact version, not read the new head"
    );
    assert_public_file_read_chain(&f, delivered_entry, delivered_entry, &old).await;
    assert!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &delivery(&f, exact.clone()))
            .await
            .is_err(),
        "a fresh publication cannot silently advance an observed stale revision"
    );
    let latest = query(&f, "files.list@6", json!({"scope":"owned_agent"})).await;
    let latest = latest["entries"][0]["file_ref"].clone();
    assert_ne!(latest, exact);
    let mut wrong_agent = latest.clone();
    wrong_agent["owned_agent_id"] = AgentId::generate().to_string().into();
    assert!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &delivery(&f, wrong_agent))
            .await
            .is_err()
    );
    let mut wrong_hash = latest.clone();
    wrong_hash["revision"] = format!(
        "{}:file:{}:{}",
        f.f.custody.source.scope.agent_id,
        old.artifact_version_id,
        "0".repeat(64)
    )
    .into();
    assert!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &delivery(&f, wrong_hash))
            .await
            .is_err()
    );
    // Original node mutation, no business-table rewrite in this regression.
    query(
        &f,
        "resource.node.trash@4",
        json!({"node_id":old.node_id.to_string()}),
    )
    .await;
    assert!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &delivery(&f, latest))
            .await
            .is_err()
    );
    assert_eq!(
        f.f.publisher
            .publish_device(f.f.actor, &f.f.target, &request)
            .await
            .unwrap(),
        committed,
        "deleting the current node does not manufacture a new receipt or erase the prior public fact"
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent.dialogue_tool_publication_groups WHERE tenant_id=$1",
    )
    .bind(f.f.actor.tenant_id.into_uuid())
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    let reader = f.artifacts();
    assert_eq!(
        reader
            .describe(
                f.f.actor,
                f.f.custody.source.scope.dialogue_id,
                delivered_entry,
                CorrelationId::generate()
            )
            .await
            .unwrap_err()
            .code,
        musteragent_runtime_contract::ErrorCode::ArtifactNotFound,
        "an immutable receipt does not preserve current access after trash"
    );
    let page = reader
        .list(public_files_query(&f), CorrelationId::generate())
        .await
        .unwrap();
    assert!(page.items.is_empty());
}

fn public_files_query(f: &Canonical) -> AgentArtifactDirectoryQuery {
    AgentArtifactDirectoryQuery {
        actor: f.f.actor,
        agent_id: f.f.custody.source.scope.agent_id,
        cursor: None,
        limit: 10,
        origin: None,
        file_types: vec![],
    }
}

/// Exercise the actual product read owners after a committed Native publication.
/// A durable group alone is not evidence that its card or content can be opened.
async fn assert_public_file_read_chain(
    f: &Canonical,
    entry: DialogueEntryId,
    latest_entry: DialogueEntryId,
    exact: &DeviceCanonicalFileVersion,
) {
    let reader = f.artifacts();
    let dialogue = f.f.custody.source.scope.dialogue_id;
    let described = reader
        .describe(f.f.actor, dialogue, entry, CorrelationId::generate())
        .await
        .unwrap();
    assert_eq!(described.artifact_id, exact.artifact_id);
    assert_eq!(described.delivered_version_id, exact.artifact_version_id);
    let listed = reader
        .list(public_files_query(f), CorrelationId::generate())
        .await
        .unwrap();
    assert_eq!(
        listed.items.len(),
        1,
        "redelivery must not remove or duplicate the public file"
    );
    assert_eq!(listed.items[0].entry_id, latest_entry);
    assert_eq!(listed.items[0].artifact.artifact_id, exact.artifact_id);
    let authorized = reader
        .authorize_content(
            f.f.actor,
            dialogue,
            entry,
            exact.artifact_version_id,
            exact.content_ref.clone(),
            CorrelationId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(authorized, exact.content_ref);
    let bytes =
        f.f.content
            .read(f.f.actor.tenant_id, &authorized)
            .await
            .unwrap();
    assert_eq!(bytes, b"First canonical line.\n");
    // Recheck after reading, as the public stream owner does before release.
    assert_eq!(
        reader
            .authorize_content(
                f.f.actor,
                dialogue,
                entry,
                exact.artifact_version_id,
                authorized.clone(),
                CorrelationId::generate()
            )
            .await
            .unwrap(),
        authorized
    );
    let outsider = Actor {
        principal_id: PrincipalId::generate(),
        ..f.f.actor
    };
    assert_eq!(
        reader
            .describe(outsider, dialogue, entry, CorrelationId::generate())
            .await
            .unwrap_err()
            .code,
        musteragent_runtime_contract::ErrorCode::ArtifactNotFound
    );
    assert!(
        reader
            .authorize_content(
                outsider,
                dialogue,
                entry,
                exact.artifact_version_id,
                exact.content_ref.clone(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .describe(
                f.f.actor,
                AgentDialogueId::generate(),
                entry,
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .authorize_content(
                f.f.actor,
                dialogue,
                entry,
                ArtifactVersionId::generate(),
                exact.content_ref.clone(),
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
    let mut wrong = exact.content_ref.clone();
    wrong.sha256 = Sha256::digest(b"not the retained file");
    assert!(
        reader
            .authorize_content(
                f.f.actor,
                dialogue,
                entry,
                exact.artifact_version_id,
                wrong,
                CorrelationId::generate()
            )
            .await
            .is_err()
    );
}
