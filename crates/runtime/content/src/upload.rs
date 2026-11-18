//! Original actor-scoped immutable upload identities, shared by adapters.
//! Deriving an identity grants neither tenant access nor publication authority.
use crate::ContentStoreError;
use musteragent_identity::{Actor, ContentId, IdempotencyKey};

/// Largest accepted upload part. Earlier clients send 512 KiB parts, which
/// remain valid; 8 MiB matches the object-store staging part and fits one gRPC
/// request under `CONTENT_UPLOAD_MESSAGE_BYTES`.
pub const CONTENT_UPLOAD_PART_BYTES: usize = 8 * 1024 * 1024;
/// Mirrors the S3/R2 multipart bound of 10,000 parts. A completion request
/// lists every part reference (~120 bytes each), so this also bounds it.
pub const MAX_CONTENT_UPLOAD_PARTS: usize = 10_000;
/// gRPC message bound for upload traffic: one full part plus envelope.
pub const CONTENT_UPLOAD_MESSAGE_BYTES: usize = CONTENT_UPLOAD_PART_BYTES + 64 * 1024;
const NAMESPACE: uuid::Uuid = uuid::Uuid::from_bytes([
    0x63, 0x79, 0xe5, 0x0c, 0xa8, 0x7d, 0x4e, 0x39, 0x91, 0x73, 0x5b, 0x0a, 0xdd, 0xe8, 0xd1, 0x34,
]);

pub fn idempotent_content_id(
    actor: Actor,
    key: &IdempotencyKey,
) -> Result<ContentId, ContentStoreError> {
    let mut seed = Vec::with_capacity(16 + 16 + 1 + key.as_str().len());
    seed.extend_from_slice(actor.tenant_id.into_uuid().as_bytes());
    seed.extend_from_slice(actor.principal_id.into_uuid().as_bytes());
    seed.push(0);
    seed.extend_from_slice(key.as_str().as_bytes());
    ContentId::from_uuid(uuid::Uuid::new_v5(&NAMESPACE, &seed))
        .map_err(|_| ContentStoreError::Invariant)
}

pub fn content_upload_key(
    upload: &str,
    part: Option<usize>,
) -> Result<IdempotencyKey, ContentStoreError> {
    if upload.len() != 64
        || !upload
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || part.is_some_and(|index| index >= MAX_CONTENT_UPLOAD_PARTS)
    {
        return Err(ContentStoreError::Invariant);
    }
    IdempotencyKey::new(match part {
        Some(index) => format!("content-upload:{upload}:part:{index}"),
        None => format!("content-upload:{upload}:complete"),
    })
    .map_err(|_| ContentStoreError::Invariant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_identity::{PrincipalId, PrincipalKind, TenantId};

    #[test]
    fn original_namespaces_and_actor_separation_are_preserved() {
        let actor = Actor {
            tenant_id: "00000000-0000-0000-0000-000000000001"
                .parse()
                .expect("tenant"),
            principal_id: "00000000-0000-0000-0000-000000000002"
                .parse()
                .expect("principal"),
            principal_kind: PrincipalKind::User,
        };
        let key = content_upload_key(&"a".repeat(64), Some(0)).expect("part key");
        assert_eq!(
            key.as_str(),
            format!("content-upload:{}:part:0", "a".repeat(64))
        );
        let original = idempotent_content_id(actor, &key).expect("content identity");
        // Independent byte recipe retained from the original gRPC adapter.
        let mut seed = actor.tenant_id.into_uuid().as_bytes().to_vec();
        seed.extend_from_slice(actor.principal_id.into_uuid().as_bytes());
        seed.push(0);
        seed.extend_from_slice(key.as_str().as_bytes());
        assert_eq!(original.into_uuid(), uuid::Uuid::new_v5(&NAMESPACE, &seed));
        for changed in [
            Actor {
                tenant_id: TenantId::generate(),
                ..actor
            },
            Actor {
                principal_id: PrincipalId::generate(),
                ..actor
            },
        ] {
            assert_ne!(
                idempotent_content_id(changed, &key).expect("changed actor"),
                original
            );
        }
        for part in [Some(1), None] {
            assert_ne!(
                idempotent_content_id(
                    actor,
                    &content_upload_key(&"a".repeat(64), part).expect("distinct key")
                )
                .expect("distinct identity"),
                original
            );
        }
    }

    #[test]
    fn invalid_upload_identity_and_part_index_are_rejected() {
        for upload in [
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "z".repeat(64),
        ] {
            assert!(content_upload_key(&upload, None).is_err());
        }
        assert!(content_upload_key(&"a".repeat(64), Some(9_999)).is_ok());
        assert!(content_upload_key(&"a".repeat(64), Some(10_000)).is_err());
    }
}
