use crate::{GrantKeyring, GrantVerificationError};
use ed25519_dalek::Signature;
use musteragent_execution_context::SignedDialogueAdmissionRejection;
use musteragent_identity::UtcTimestamp;

impl GrantKeyring {
    /// Same configured trust root, separate signature domain and non-executable
    /// claims. Revalidate time at the durable queue transaction, not just RPC.
    pub fn verify_dialogue_admission_rejection(
        &self,
        signed: &SignedDialogueAdmissionRejection,
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
