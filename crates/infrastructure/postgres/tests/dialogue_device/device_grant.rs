//! Real signed grants and original PG custody, including response-loss replay.
use super::device_target::{now, schema, target};
use super::*;
use ed25519_dalek::{Signer, SigningKey};
use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding, SignedExecutionGrant, canonical_claims,
};
use musteragent_identity::CapabilityProviderId;
use musteragent_model::{
    ApiFormat, CredentialGrantRef, ModelEntryId, ModelProviderId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_policy::{Decision, Effect};
use musteragent_postgres::PostgresDialogueIntentHandoffStore;
use musteragent_runtime_contract::{ModelClass, OpaqueExternalRef};
use musteragent_storage_protection::GrantKeyring;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

pub(super) fn keys() -> Arc<GrantKeyring> {
    Arc::new(
        GrantKeyring::new([(
            DottedKey::new("device.custody.test").unwrap(),
            SigningKey::from_bytes(&[49; 32]).verifying_key(),
        )])
        .unwrap(),
    )
}
pub(super) fn sign(claims: ExecutionGrant) -> SignedExecutionGrant {
    SignedExecutionGrant {
        key_id: DottedKey::new("device.custody.test").unwrap(),
        signature: SigningKey::from_bytes(&[49; 32])
            .sign(&canonical_claims(&claims).unwrap())
            .to_bytes(),
        claims,
        context_transport_annex: vec![],
    }
}
pub(super) fn grant(custody: &DeviceSourceCustody, now: i64) -> ExecutionGrant {
    let p = &custody.source;
    let provider = ModelProviderId::from_uuid(uuid::Uuid::now_v7()).unwrap();
    let entry = ModelEntryId::from_uuid(uuid::Uuid::now_v7()).unwrap();
    ExecutionGrant {
        reviewed_application_source_sha256: Vec::new(),
        product_release: None,
        schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).unwrap(),
        grant_ref: CredentialGrantRef::new("grant_device_custody_test").unwrap(),
        issuer: DottedKey::new("musteragent.backend").unwrap(),
        audience: DottedKey::new(format!(
            "musteragent.runtime.device.id-{}",
            custody.device_id
        ))
        .unwrap(),
        tenant_id: p.scope.tenant_id,
        model_provider_id: provider,
        model_entry_id: entry,
        run_id: p.identities.run_id().unwrap(),
        issued_at: UtcTimestamp::from_unix_millis(now - 1000),
        expires_at: UtcTimestamp::from_unix_millis(now + 60_000),
        nonce: OpaqueExternalRef::new(
            p.local_execution_binding(custody.device_id)
                .unwrap()
                .grant_nonce(),
        )
        .unwrap(),
        requested_model_class: ModelClass::new("chat.balanced").unwrap(),
        model_route: ModelRoute {
            route_ref: VersionedKey::new("cloud.chat-primary@1").unwrap(),
            resolved_model_key: "test-model".into(),
            model_provider_id: provider,
            model_entry_id: entry,
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: VersionedKey::new("openai.responses-adapter@1").unwrap(),
            protocol_version: VersionedKey::new("openai.responses@1").unwrap(),
            context_window: NonZeroU64::new(32_000).unwrap(),
            max_output_tokens: NonZeroU64::new(4_000).unwrap(),
            capabilities: BTreeSet::from([DottedKey::new("tools").unwrap()]),
            parameters: BTreeMap::new(),
            fallback_route_refs: vec![],
            pricing_version_ref: PricingVersionRef::new("catalog:test.price@1").unwrap(),
            provider_state_policy: ProviderStatePolicy::Stateless,
        },
        fallback_model_routes: vec![],
        auxiliary_model_routes: vec![],
        agent_contract: Some(VersionedKey::new("musteragent.runtime-contract@1").unwrap()),
        surface: Some(DottedKey::new("desktop").unwrap()),
        capability_entitlements: vec![CapabilityEntitlement {
            key: CapabilityKey::from_versioned_key(VersionedKey::new("core.time.read@1").unwrap()),
            provider_id: CapabilityProviderId::generate(),
            location: CapabilityLocation::Device,
            effect: Effect::Read,
            allowed_scopes: BTreeSet::from([DottedKey::new("time.read").unwrap()]),
            policy_decision: Decision::Allow,
        }],
        tool_contract_releases: vec![],
        identity_binding: Some(ExecutionGrantIdentityBinding {
            principal_id: p.scope.owner_id,
            authority_scope: ExecutionGrantAuthorityScope::Tenant {
                tenant_id: p.scope.tenant_id,
            },
            device_id: Some(custody.device_id),
            device_workspace_id: None,
        }),
        permission_mode: Some(p.configuration.permission_mode),
        device_direct_provider: None,
        vision_direct_provider: None,
    }
}

#[tokio::test]
async fn device_grant_custody_authenticates_freezes_recovers_and_rechecks_revocation() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    schema(&pool).await;
    let owner = actor();
    let device = DeviceId::generate();
    let install = DeviceInstallationId::generate();
    let authority = target(owner, device, install, now(&pool).await);
    let active = attachment_fixture::activate(&pool, owner).await;
    let mut command = input(owner, active.agent.dialogue_id);
    command.origin = DialogueInputOrigin {
        surface: DialogueInputSurface::Desktop,
        installation_id: Some(install),
    };
    command.accepted_at = UtcTimestamp::from_unix_millis(now(&pool).await);
    register_content(&pool, owner, &command.content_ref).await;
    let accepted = PostgresAgentDialogueStore::new(pool.clone())
        .accept_on_target(
            command,
            DialogueIntentExecutionTarget::OriginDevice(authority.clone()),
        )
        .await;
    if musteragent_runtime_snapshot::dialogue_device_discovery_policy_for_current_release()
        .is_none()
    {
        assert_eq!(accepted, Err(DialogueStoreError::Unavailable));
        return;
    }
    let intent = accepted.unwrap().intent.intent_id;
    let queue =
        PostgresDialogueIntentHandoffStore::new(pool.clone()).with_device_grant_authority(keys());
    let claim = queue
        .claim_device(
            owner,
            authority.clone(),
            intent,
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        queue.read_device_grant(claim).await.is_err(),
        "no source custody"
    );
    let custody = queue.capture_device_source(claim).await.unwrap();
    let mut wrong_attempt = claim;
    wrong_attempt.claim.attempt_count += 1;
    let mut wrong_lease = claim;
    wrong_lease.claim.lease_expires_at =
        claim.claim.lease_expires_at.checked_add_millis(1).unwrap();
    for wrong in [wrong_attempt, wrong_lease] {
        assert_eq!(
            queue.capture_device_source(wrong).await,
            Err(DialogueStoreError::Unavailable)
        );
        assert_eq!(
            queue.read_device_grant(wrong).await,
            Err(DialogueStoreError::Unavailable)
        );
    }
    assert!(queue.read_device_grant(claim).await.unwrap().is_none());
    let claims = grant(&custody, now(&pool).await);
    let signed = sign(claims.clone());
    let mut forged = signed.clone();
    forged.signature[0] ^= 1;
    assert_eq!(
        queue.bind_device_grant(claim, forged).await,
        Err(DialogueStoreError::Corrupt)
    );
    let variants: &[fn(&mut ExecutionGrant)] = &[
        |g| g.nonce = OpaqueExternalRef::new("unbound_source").unwrap(),
        |g| g.run_id = RunId::generate(),
        |g| g.identity_binding.as_mut().unwrap().principal_id = PrincipalId::generate(),
        |g| {
            g.identity_binding.as_mut().unwrap().device_workspace_id = Some(WorkspaceId::generate())
        },
        |g| {
            g.audience = DottedKey::new(format!(
                "musteragent.runtime.device.id-{}",
                DeviceId::generate()
            ))
            .unwrap()
        },
        |g| g.surface = Some(DottedKey::new("mobile").unwrap()),
    ];
    for change in variants {
        let mut changed = claims.clone();
        change(&mut changed);
        assert!(queue.bind_device_grant(claim, sign(changed)).await.is_err());
    }
    let mut expired = claims.clone();
    expired.issued_at = UtcTimestamp::from_unix_millis(now(&pool).await - 2_000);
    expired.expires_at = UtcTimestamp::from_unix_millis(now(&pool).await - 1_000);
    assert_eq!(
        queue.bind_device_grant(claim, sign(expired)).await,
        Err(DialogueStoreError::Unavailable)
    );
    assert!(queue.read_device_grant(claim).await.unwrap().is_none());
    // Cosmetic profile edits are not revocation, and do not rewrite the source.
    sqlx::query("UPDATE agent.owned_agent_configurations SET display_name='Later name',profile_revision=profile_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
        .bind(active.agent.agent_id.into_uuid()).execute(&pool).await.unwrap();
    let committed = queue
        .bind_device_grant(claim, signed.clone())
        .await
        .unwrap();
    assert_eq!(committed.source, custody);
    assert_eq!(
        queue.bind_device_grant(claim, signed).await.unwrap(),
        committed
    );
    let mut different = claims;
    different.grant_ref = CredentialGrantRef::new("grant_another_authority").unwrap();
    assert_eq!(
        queue.bind_device_grant(claim, sign(different)).await,
        Err(DialogueStoreError::IdempotencyConflict)
    );
    sqlx::query(
        "UPDATE agent.dialogue_intent_admissions SET lease_expires_at_ms=0 WHERE intent_id=$1",
    )
    .bind(intent.into_uuid())
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        queue.read_device_grant(claim).await,
        Err(DialogueStoreError::Unavailable)
    );
    let next = queue
        .claim_device(
            owner,
            authority,
            intent,
            CommandId::generate().into_uuid(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(next.claim.generation > claim.claim.generation);
    assert_eq!(
        queue.read_device_grant(next).await.unwrap(),
        Some(committed.clone())
    );
    assert!(
        sqlx::query("UPDATE agent.device_admissions SET signed_grant_proof=$2 WHERE intent_id=$1")
            .bind(intent.into_uuid())
            .bind(vec![1u8])
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_revision=permission_revision+1,manifest_revision=manifest_revision+1 WHERE agent_id=$1")
        .bind(active.agent.agent_id.into_uuid()).execute(&pool).await.unwrap();
    assert_eq!(
        queue.read_device_grant(next).await,
        Err(DialogueStoreError::RevisionConflict)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.runs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}
