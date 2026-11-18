use crate::{GrantKeyring, GrantVerificationError};
use ed25519_dalek::Signature;
use musteragent_identity::UtcTimestamp;
use musteragent_work_item::SignedWorkItemAdmissionRejection;

impl GrantKeyring {
    /// Same configured trust root, separate signature domain and non-executable
    /// claims. Revalidate time at the durable queue transaction, not just RPC.
    pub fn verify_work_item_admission_rejection(
        &self,
        signed: &SignedWorkItemAdmissionRejection,
        now: UtcTimestamp,
    ) -> Result<(), GrantVerificationError> {
        let key = self
            .verifying_keys
            .get(&signed.key_id)
            .ok_or(GrantVerificationError::UnknownKey)?;
        if !signed.claims.is_valid_at(now) {
            return Err(GrantVerificationError::InvalidSignature);
        }
        key.verify_strict(
            &signed.claims.canonical_bytes(),
            &Signature::from_bytes(&signed.signature),
        )
        .map_err(|_| GrantVerificationError::InvalidSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use musteragent_identity::*;
    use musteragent_work_item::{
        WorkItemAdmissionRejectionClaims, WorkItemAdmissionRejectionReason,
        WorkItemControlRevision, WorkItemGoalRevision,
    };
    #[test]
    fn refusal_signature_rejects_tampering_expiry_and_foreign_keys() {
        let secret = SigningKey::from_bytes(&[19; 32]);
        let key_id = DottedKey::new("test.work-item.authority").unwrap();
        let trust = GrantKeyring::new([(key_id.clone(), secret.verifying_key())]).unwrap();
        let now = UtcTimestamp::from_unix_millis(1_780_000_000_000);
        let claims = WorkItemAdmissionRejectionClaims {
            actor: Actor {
                tenant_id: TenantId::generate(),
                principal_id: PrincipalId::generate(),
                principal_kind: PrincipalKind::User,
            },
            work_item_id: WorkItemId::generate(),
            goal_revision: WorkItemGoalRevision::new(1).unwrap(),
            control_revision: WorkItemControlRevision::new(1).unwrap(),
            assignment_epoch: AssignmentEpoch::new(1).unwrap(),
            decision_id: CommandId::generate(),
            facts_sha256: musteragent_content_ref::Sha256::digest(b"accepted-task-facts"),
            reason: WorkItemAdmissionRejectionReason::ExecutionNotAllowed,
            issued_at: now,
            expires_at: UtcTimestamp::from_unix_millis(now.unix_millis() + 120_000),
        };
        let signed = SignedWorkItemAdmissionRejection {
            key_id,
            signature: secret.sign(&claims.canonical_bytes()).to_bytes(),
            claims,
        };
        assert!(
            trust
                .verify_work_item_admission_rejection(&signed, now)
                .is_ok()
        );
        assert!(
            trust
                .verify_work_item_admission_rejection(&signed, signed.claims.expires_at)
                .is_err()
        );
        let mut changed = signed.clone();
        changed.claims.assignment_epoch = AssignmentEpoch::new(2).unwrap();
        assert!(
            trust
                .verify_work_item_admission_rejection(&changed, now)
                .is_err()
        );
        changed = signed.clone();
        changed.claims.actor.principal_id = PrincipalId::generate();
        assert!(
            trust
                .verify_work_item_admission_rejection(&changed, now)
                .is_err()
        );
        changed = signed.clone();
        changed.signature[0] ^= 1;
        assert!(
            trust
                .verify_work_item_admission_rejection(&changed, now)
                .is_err()
        );
        changed = signed.clone();
        changed.key_id = DottedKey::new("unknown.key").unwrap();
        assert!(
            trust
                .verify_work_item_admission_rejection(&changed, now)
                .is_err()
        );
    }
}
