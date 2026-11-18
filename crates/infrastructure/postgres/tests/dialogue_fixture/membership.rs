//! Canonical signed membership fixtures shared by original owner tests.
use super::*;
use ed25519_dalek::Signer;
use musteragent_device_authority as membership;

pub(crate) async fn host_authority(
    pool: &PgPool,
    owner: Actor,
    target: DeviceDestination,
) -> DeviceHostAuthority {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(pool)
            .await
            .unwrap();
    host_authority_at(owner, target, now)
}

pub(crate) fn host_authority_at(
    owner: Actor,
    target: DeviceDestination,
    now: i64,
) -> DeviceHostAuthority {
    let envelope = envelope(owner, target, now);
    DeviceHostAuthority::from_trusted_native_transport(
        owner,
        target,
        &membership::canonical_bytes(&envelope).unwrap(),
    )
    .unwrap()
}

pub(crate) fn envelope(
    owner: Actor,
    target: DeviceDestination,
    now: i64,
) -> membership::BackendEnvelope {
    let id = || {
        uuid::Uuid::new_v5(&target.device_id.into_uuid(), b"test-host-authority")
            .to_string()
            .parse()
            .unwrap()
    };
    let digest: membership::Sha256Hex = "11".repeat(32).parse().unwrap();
    let value = membership::Authority {
        schema_version: Default::default(),
        issuer: Default::default(),
        audience: Default::default(),
        operation_id: id(),
        operation_digest: digest.clone(),
        subject: membership::Subject {
            deployment_origin: "https://kernel.test".parse().unwrap(),
            tenant_id: owner.tenant_id.to_string().parse().unwrap(),
            principal_id: owner.principal_id.to_string().parse().unwrap(),
            installation_id: id(),
            installation_key_id: digest.clone(),
            host_id: id(),
            device_id: target.device_id.to_string().parse().unwrap(),
            device_key_id: digest.clone(),
            application_id: membership::ApplicationId::Musteragent,
            client_installation_id: target.installation_id.to_string().parse().unwrap(),
            client_release_ref: membership::ReleaseRef::from_digest(&digest),
            client_release_sha256: digest,
            login_session_id: id(),
            login_revision: membership::LoginRevision::Active,
            account_auth_epoch: "1".parse().unwrap(),
        },
        membership_id: target.membership_id.to_string().parse().unwrap(),
        membership_epoch: target.membership_epoch.get().to_string().parse().unwrap(),
        state: Default::default(),
        issued_at_unix_ms: (now as u64 - 1000).try_into().unwrap(),
        expires_at_unix_ms: (now as u64 + 299_000).try_into().unwrap(),
    };
    let mut envelope = membership::BackendEnvelope {
        schema_version: Default::default(),
        key_id: "fixture.authority.v1".parse().unwrap(),
        payload_base64: membership::PayloadBytes::from_payload(&value).unwrap(),
        signature_base64: membership::Signature::new([0; 64]),
    };
    let key = ed25519_dalek::SigningKey::from_bytes(&[73; 32]);
    envelope.signature_base64 =
        membership::Signature::new(key.sign(&envelope.signing_input().unwrap()).to_bytes());
    key.verifying_key()
        .verify_strict(
            &envelope.signing_input().unwrap(),
            &ed25519_dalek::Signature::from_bytes(envelope.signature()),
        )
        .unwrap();
    envelope
}
