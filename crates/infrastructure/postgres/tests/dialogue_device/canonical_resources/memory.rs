//! Native Tool -> canonical Memory -> the same management service used by the UI.
//! Trusted signed Native observations are fixtures; all Memory transactions,
//! approval checks, Content and management queries are the production owners.
use super::*;
use musteragent_approval::{InteractionKind, InteractionStatus, InteractionView};
use musteragent_postgres::PostgresMemoryEvolutionStore;
use musteragent_runtime_memory::*;
use std::num::NonZeroU16;

async fn approved(f: &Canonical, request: DeviceResourceRequest) -> DeviceResourceRequest {
    let prompt_request = request.clone().for_memory_approval().unwrap();
    let prompt_result = f.execute(&prompt_request).await;
    let packed: Value =
        serde_json::from_slice(&prompt_result.success().unwrap().result_bytes).unwrap();
    let detail: Vec<u8> = serde_json::from_value(packed["bytes"].clone()).unwrap();
    let original = request.command();
    let inv = original.invocation();
    let envelope = json!({
        "schema":"musteragent.tool.approval.prompt.v4",
        "invocation_id": inv.invocation_id.to_string(),
        "tool": if inv.tool_key.logical_key()=="experience.publish" { "experience__publish__v1" } else { "user_memory_remember" },
        "arguments": serde_json::from_slice::<Value>(original.request_bytes()).unwrap(),
        "request": {"content_id":inv.request_ref.content_id.to_string(), "media_type":inv.request_ref.media_type.as_str(),
            "byte_length":inv.request_ref.byte_length, "sha256":inv.request_ref.sha256.to_string()},
        "capability_detail": {"media_type":packed["media_type"], "byte_length":detail.len(),
            "sha256":Sha256::digest(&detail).to_string(), "content_json":serde_json::from_slice::<Value>(&detail).unwrap(), "content_text":null},
        "consequence_fingerprint":Sha256::digest(b"original-native-policy-decision").to_string(),
        "session_reusable":false,
    });
    let bytes = serde_json::to_vec(&envelope).unwrap();
    let prompt_ref =
        f.f.content
            .put(PutContent {
                tenant_id: f.f.actor.tenant_id,
                media_type: MediaType::new("application/json").unwrap(),
                bytes: bytes.clone(),
            })
            .await
            .unwrap();
    let at = now(&f.f.pool).await;
    let interaction = InteractionId::generate();
    let p = NativeDialogueInteractionProjection {
        admission: original.admission().clone(),
        conversation_id: original.run_context().conversation_id,
        invocation_id: inv.invocation_id,
        interaction: InteractionView {
            interaction_id: interaction,
            run_id: original.run_context().run_id,
            kind: InteractionKind::Approval,
            status: InteractionStatus::Responded,
            prompt_ref,
            response_schema: inv.result_schema.clone(),
            expires_at: None,
            requested_at: UtcTimestamp::from_unix_millis(at - 2),
            responded_at: Some(UtcTimestamp::from_unix_millis(at - 1)),
        },
        prompt_bytes: bytes,
        observed_run_sequence: RunSeq::new(10).unwrap(),
        control_open: false,
        approval_policy: None,
        resolution: Some(DialogueInteractionResolution::Approved),
    };
    p.validate().unwrap();
    let b = &p.admission;
    let source = &f.f.custody.source;
    sqlx::query("INSERT INTO agent.native_dialogue_interaction_projections(interaction_id,tenant_id,owner_id,agent_id,dialogue_id,intent_id,conversation_id,run_id,invocation_id,device_id,installation_id,device_subject_kind,source_digest,snapshot_hash,grant_proof_hash,request_sha256,state_sha256,projection_sha256,projection_bytes,observed_run_sequence,prompt_content_id,control_open,status,requested_at_ms,expires_at_ms,updated_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'intent',$12,$13,$14,$15,$16,$17,$18,10,$19,false,2,$20,NULL,$21)")
        .bind(interaction.into_uuid()).bind(f.f.actor.tenant_id.into_uuid()).bind(f.f.actor.principal_id.into_uuid())
        .bind(source.scope.agent_id.into_uuid()).bind(source.scope.dialogue_id.into_uuid()).bind(b.intent_id.into_uuid())
        .bind(p.conversation_id.into_uuid()).bind(b.run_id.into_uuid()).bind(inv.invocation_id.into_uuid())
        .bind(b.device_id.into_uuid()).bind(b.installation_id.into_uuid()).bind(b.source_digest.into_bytes().to_vec())
        .bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec())
        .bind(p.request_fingerprint().into_bytes().to_vec()).bind(p.state_fingerprint().into_bytes().to_vec())
        .bind(p.fingerprint().into_bytes().to_vec()).bind(musteragent_persistence_codec::native_dialogue_interaction_codec::projection_bytes(&p))
        .bind(p.interaction.prompt_ref.content_id.into_uuid()).bind(at-2).bind(at)
        .execute(&f.f.pool).await.unwrap();
    let mut inv = inv.clone();
    inv.memory.as_mut().unwrap().approval_interaction_id = Some(interaction);
    DeviceResourceRequest::new(
        DeviceResourceCommand::new(
            original.admission().clone(),
            inv,
            original.request_bytes().to_vec(),
        )
        .unwrap(),
        request.attempt().clone(),
    )
    .unwrap()
}

#[tokio::test]
async fn native_remember_is_visible_to_account_management_and_original_agent_reads() {
    let f = Canonical::new().await;
    let request = f
        .request(
            "user_memory.remember@2",
            json!({"fact_key":"identity.name","content":"用户希望被称为小羊。"}),
        )
        .await;
    let request = approved(&f, request).await;
    let receipt = f.execute(&request).await;
    assert!(receipt.success().is_some(), "{receipt:?}");
    let repeated = f.execute(&request).await;
    assert_eq!(repeated, receipt);
    let store = Arc::new(PostgresMemoryEvolutionStore::new(f.f.pool.clone()));
    let service = AccountMemoryManagementService::new(store.clone(), f.f.content.clone());
    let page = service
        .list(ListAccountMemories {
            identity: AccountMemoryManagementIdentity { actor: f.f.actor },
            lifecycle: ManagedMemoryLifecycle::Active,
            observed_at: UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
            after: None,
            limit: NonZeroU16::new(50).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].text, "用户希望被称为小羊。");
    let private = store
        .list_managed_memories(ListManagedMemories {
            identity: AgentMemoryManagementIdentity {
                actor: f.f.actor,
                agent_id: f.f.custody.source.scope.agent_id,
            },
            lifecycle: ManagedMemoryLifecycle::Active,
            observed_at: UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
            after: None,
            limit: NonZeroU16::new(50).unwrap(),
        })
        .await
        .unwrap();
    assert!(
        private.heads.is_empty(),
        "account memory must not be relabeled as agent memory"
    );
    let read = f
        .request(
            "memory.get@1",
            json!({"memory_id":page.items[0].memory_id.to_string()}),
        )
        .await;
    let readback = f.execute(&read).await;
    assert!(readback.success().is_some(), "{readback:?}");
    assert!(
        String::from_utf8(readback.success().unwrap().result_bytes.clone())
            .unwrap()
            .contains("小羊")
    );
    let cloud: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM runtime.runs),(SELECT count(*) FROM tool.tool_invocations)",
    )
    .fetch_one(&f.f.pool)
    .await
    .unwrap();
    assert_eq!(
        cloud,
        (0, 0),
        "Native execution must retain its original physical owner"
    );
    assert_eq!(f.execute(&read).await, readback);
    sqlx::query(
        "UPDATE memory.memories SET lifecycle=3,revoked_at_ms=updated_at_ms WHERE memory_id=$1",
    )
    .bind(page.items[0].memory_id.into_uuid())
    .execute(&f.f.pool)
    .await
    .unwrap();
    assert!(
        f.store
            .execute_resource_tool(f.f.actor, &f.f.target, &read)
            .await
            .is_err(),
        "an old read receipt must not disclose revoked memory"
    );
}

#[tokio::test]
async fn native_memory_requires_original_approval_and_does_not_publish_prompt_preparation() {
    let f = Canonical::new().await;
    let request = f
        .request(
            "user_memory.remember@2",
            json!({"fact_key":"identity.name","content":"unapproved"}),
        )
        .await;
    let prompt = f
        .execute(&request.clone().for_memory_approval().unwrap())
        .await;
    assert!(prompt.success().is_some());
    let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM memory.memories")
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(effects, 0);
    let receipt = f.execute(&request).await;
    assert_eq!(
        receipt.rejection(),
        Some(DeviceResourceRejectionCode::AccessDenied)
    );
    let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM memory.memories")
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(effects, 0);
}

#[tokio::test]
async fn native_agent_publication_uses_original_evaluation_approval_and_management_scope() {
    let f = Canonical::new().await;
    let request = f
        .request(
            "experience.propose@3",
            json!({"scope":"agent_profile","kind":"semantic",
        "claim":"验收批次只在当前 Agent 使用。","conditions":["当前 Agent 的验收任务"],
        "confidence":0.95,"sensitivity":"private"}),
        )
        .await;
    let proposed = f.execute(&request).await;
    assert!(proposed.success().is_some(), "{proposed:?}");
    let store = Arc::new(PostgresMemoryEvolutionStore::new(f.f.pool.clone()));
    let candidate = store
        .load_next_candidate_for_evaluation()
        .await
        .unwrap()
        .unwrap();
    let proposal = candidate.proposal_id;
    EvolutionCandidateEvaluationService::new(store.clone(), f.f.content.clone())
        .evaluate(
            candidate,
            &EvidenceManifestCandidateEvaluator,
            f.f.actor.principal_id,
            UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
        )
        .await
        .unwrap();
    let publish = f
        .request(
            "experience.publish@1",
            json!({"proposal_id":proposal.to_string()}),
        )
        .await;
    let publish = approved(&f, publish).await;
    let receipt = f.execute(&publish).await;
    assert!(receipt.success().is_some(), "{receipt:?}");
    assert_eq!(receipt, f.execute(&publish).await);
    let page = store
        .list_managed_memories(ListManagedMemories {
            identity: AgentMemoryManagementIdentity {
                actor: f.f.actor,
                agent_id: f.f.custody.source.scope.agent_id,
            },
            lifecycle: ManagedMemoryLifecycle::Active,
            observed_at: UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
            after: None,
            limit: NonZeroU16::new(50).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(page.heads.len(), 1);
    let shared = AccountMemoryManagementService::new(store, f.f.content.clone())
        .list(ListAccountMemories {
            identity: AccountMemoryManagementIdentity { actor: f.f.actor },
            lifecycle: ManagedMemoryLifecycle::Active,
            observed_at: UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
            after: None,
            limit: NonZeroU16::new(50).unwrap(),
        })
        .await
        .unwrap();
    assert!(shared.items.is_empty());
}

#[tokio::test]
async fn historical_native_reconciliation_preserves_approval_scope_and_conflict_fences() {
    use musteragent_postgres::NativeMemoryReconciliation;
    let f = Canonical::new().await;
    let original = json!({"fact_key":"identity.name","content":"用户希望被称为小羊。"});
    let request = approved(
        &f,
        f.request("user_memory.remember@2", original.clone()).await,
    )
    .await;
    let bytes = serde_json::to_vec(&original).unwrap();
    let owner = PostgresMemoryEvolutionStore::new(f.f.pool.clone());
    let mut source:NativeMemoryReconciliation=serde_json::from_value(json!({
        "tenant_id":f.f.actor.tenant_id.to_string(),"principal_id":f.f.actor.principal_id.to_string(),
        "agent_id":f.f.custody.source.scope.agent_id.to_string(),
        "interaction_id":request.command().invocation().memory.as_ref().unwrap().approval_interaction_id.unwrap().to_string(),
        "memory_id":MemoryId::generate().to_string(),"memory_key":"user-memory:identity.name",
        "scope":"account","kind":"semantic","source":1,"source_version_id":MemoryVersionId::generate().to_string(),
        "source_version":1,"confidence_bps":10000,"source_created_at_ms":now(&f.f.pool).await,
        "content_id":ContentId::generate().to_string(),"media_type":"application/json",
        "content_sha256":Sha256::digest(&bytes).to_string(),"bytes":bytes,
        "source_run_ids":[f.f.binding.run_id.to_string()],"evidence_record_ids":[],"expected_head":null
    })).unwrap();
    let target = owner
        .reconcile_native_memory(f.f.content.as_ref(), &source, false)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM memory.memories")
        .fetch_one(&f.f.pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "dry run must not publish memory");
    source.scope = "agent".into();
    assert!(
        owner
            .reconcile_native_memory(f.f.content.as_ref(), &source, true)
            .await
            .is_err()
    );
    source.scope = "account".into();
    assert_eq!(
        owner
            .reconcile_native_memory(f.f.content.as_ref(), &source, true)
            .await
            .unwrap(),
        target
    );
    let page = AccountMemoryManagementService::new(
        Arc::new(PostgresMemoryEvolutionStore::new(f.f.pool.clone())),
        f.f.content.clone(),
    )
    .list(ListAccountMemories {
        identity: AccountMemoryManagementIdentity { actor: f.f.actor },
        lifecycle: ManagedMemoryLifecycle::Active,
        observed_at: UtcTimestamp::from_unix_millis(now(&f.f.pool).await),
        after: None,
        limit: NonZeroU16::new(50).unwrap(),
    })
    .await
    .unwrap();
    assert_eq!(page.items[0].text, "用户希望被称为小羊。");
    assert_eq!(
        owner
            .reconcile_native_memory(f.f.content.as_ref(), &source, true)
            .await
            .unwrap(),
        target
    );
    source.source_version_id = MemoryVersionId::generate().into_uuid();
    assert!(
        owner
            .reconcile_native_memory(f.f.content.as_ref(), &source, true)
            .await
            .is_err(),
        "an existing head requires an explicit predecessor"
    );
    source.expected_head = Some(target.into_uuid());
    source.source_created_at_ms = now(&f.f.pool).await;
    let successor = owner
        .reconcile_native_memory(f.f.content.as_ref(), &source, true)
        .await
        .unwrap();
    assert_ne!(successor, target);
    let versions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM memory.memory_versions WHERE memory_id=$1")
            .bind(source.memory_id)
            .fetch_one(&f.f.pool)
            .await
            .unwrap();
    assert_eq!(versions, 2, "old canonical version must remain intact");
    sqlx::query(
        "UPDATE memory.memories SET lifecycle=3,revoked_at_ms=updated_at_ms WHERE memory_id=$1",
    )
    .bind(source.memory_id)
    .execute(&f.f.pool)
    .await
    .unwrap();
    source.source_version_id = MemoryVersionId::generate().into_uuid();
    source.expected_head = Some(successor.into_uuid());
    assert!(
        owner
            .reconcile_native_memory(f.f.content.as_ref(), &source, true)
            .await
            .is_err(),
        "recovery must not reactivate a revoked memory"
    );
}
