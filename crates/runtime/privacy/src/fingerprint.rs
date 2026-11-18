use musteragent_content_ref::Sha256;
use musteragent_identity::{Actor, IdempotencyKey, UtcTimestamp};
use musteragent_runtime_contract::{DataRetentionPolicyRef, ResourceRef};

pub(crate) struct Fingerprint(Vec<u8>);

impl Fingerprint {
    pub(crate) fn new(domain: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(512);
        push_bytes(&mut bytes, domain);
        Self(bytes)
    }

    pub(crate) fn actor(&mut self, actor: Actor) {
        self.uuid(actor.tenant_id.into_uuid());
        self.uuid(actor.principal_id.into_uuid());
        self.byte(match actor.principal_kind {
            musteragent_identity::PrincipalKind::User => 1,
            musteragent_identity::PrincipalKind::Service => 2,
            musteragent_identity::PrincipalKind::System => 3,
        });
    }

    pub(crate) fn byte(&mut self, value: u8) {
        self.0.push(value);
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn timestamp(&mut self, value: UtcTimestamp) {
        self.0.extend_from_slice(&value.unix_millis().to_be_bytes());
    }

    pub(crate) fn uuid(&mut self, value: uuid::Uuid) {
        self.0.extend_from_slice(value.as_bytes());
    }

    pub(crate) fn idempotency_key(&mut self, value: &IdempotencyKey) {
        push_bytes(&mut self.0, value.as_str().as_bytes());
    }

    pub(crate) fn retention(&mut self, value: &DataRetentionPolicyRef) {
        push_bytes(&mut self.0, value.as_str().as_bytes());
    }

    pub(crate) fn resources(&mut self, values: &[ResourceRef]) {
        self.u64(values.len() as u64);
        for value in values {
            push_bytes(&mut self.0, &value.canonical_bytes());
        }
    }

    pub(crate) fn sha256(&mut self, value: Sha256) {
        self.0.extend_from_slice(&value.into_bytes());
    }

    pub(crate) fn finish(self) -> Sha256 {
        Sha256::digest(self.0)
    }
}

fn push_bytes(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}
