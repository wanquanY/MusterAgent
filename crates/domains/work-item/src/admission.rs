//! Non-executable Task admission refusal. This is not a UserIntent outcome.
use super::{WorkItemControlRevision, WorkItemGoalRevision};
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, AssignmentEpoch, CommandId, DottedKey, PrincipalKind, UtcTimestamp, WorkItemId,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkItemAdmissionRejectionReason {
    AccountUnavailable,
    ExecutionNotAllowed,
}
impl WorkItemAdmissionRejectionReason {
    pub const fn code(self) -> u32 {
        match self {
            Self::AccountUnavailable => 1,
            Self::ExecutionNotAllowed => 2,
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccountUnavailable => "account_unavailable",
            Self::ExecutionNotAllowed => "execution_not_allowed",
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkItemAdmissionRejectionClaims {
    pub actor: Actor,
    pub work_item_id: WorkItemId,
    pub goal_revision: WorkItemGoalRevision,
    pub control_revision: WorkItemControlRevision,
    pub assignment_epoch: AssignmentEpoch,
    pub decision_id: CommandId,
    pub facts_sha256: Sha256,
    pub reason: WorkItemAdmissionRejectionReason,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
}
impl WorkItemAdmissionRejectionClaims {
    pub fn is_valid_at(&self, now: UtcTimestamp) -> bool {
        self.actor.principal_kind == PrincipalKind::User
            && self.issued_at.unix_millis() >= 0
            && self.issued_at <= now
            && now < self.expires_at
            && self
                .expires_at
                .unix_millis()
                .checked_sub(self.issued_at.unix_millis())
                .is_some_and(|d| (1..=120_000).contains(&d))
    }
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = b"musteragent.work-item-admission-rejection@1\0".to_vec();
        for id in [
            self.actor.tenant_id.into_uuid(),
            self.actor.principal_id.into_uuid(),
            self.work_item_id.into_uuid(),
            self.decision_id.into_uuid(),
        ] {
            bytes.extend_from_slice(id.as_bytes());
        }
        for revision in [
            self.goal_revision.get(),
            self.control_revision.get(),
            self.assignment_epoch.get(),
        ] {
            bytes.extend_from_slice(&revision.to_be_bytes());
        }
        bytes.extend_from_slice(&self.facts_sha256.into_bytes());
        bytes.extend_from_slice(&self.reason.code().to_be_bytes());
        bytes.extend_from_slice(&self.issued_at.unix_millis().to_be_bytes());
        bytes.extend_from_slice(&self.expires_at.unix_millis().to_be_bytes());
        bytes
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SignedWorkItemAdmissionRejection {
    pub key_id: DottedKey,
    pub claims: WorkItemAdmissionRejectionClaims,
    pub signature: [u8; 64],
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn task_refusal_commitment_matches_kernel_golden_and_binds_assignment() {
        let claims = WorkItemAdmissionRejectionClaims {
            actor: Actor {
                tenant_id: "019b2f80-0000-7000-8000-000000000001".parse().unwrap(),
                principal_id: "019b2f80-0000-7000-8000-000000000002".parse().unwrap(),
                principal_kind: PrincipalKind::User,
            },
            work_item_id: "019b2f80-0000-7000-8000-000000000003".parse().unwrap(),
            decision_id: "019b2f80-0000-7000-8000-000000000004".parse().unwrap(),
            goal_revision: WorkItemGoalRevision::new(2).unwrap(),
            control_revision: WorkItemControlRevision::new(3).unwrap(),
            assignment_epoch: AssignmentEpoch::new(4).unwrap(),
            facts_sha256: Sha256::from_bytes((0..32).collect::<Vec<u8>>()).unwrap(),
            reason: WorkItemAdmissionRejectionReason::ExecutionNotAllowed,
            issued_at: UtcTimestamp::from_unix_millis(1_780_000_000_000),
            expires_at: UtcTimestamp::from_unix_millis(1_780_000_120_000),
        };
        assert_eq!(
            Sha256::digest(claims.canonical_bytes()).to_string(),
            "8ec0cf31b94c86cbc125ba180e22ae9bab3761192653e126ac68b91ebd3304f5"
        );
        assert!(claims.is_valid_at(claims.issued_at));
        assert!(!claims.is_valid_at(claims.expires_at));
        let original = claims.canonical_bytes();
        let mut changed = claims.clone();
        changed.assignment_epoch = AssignmentEpoch::new(5).unwrap();
        assert_ne!(original, changed.canonical_bytes());
        changed = claims.clone();
        changed.goal_revision = WorkItemGoalRevision::new(3).unwrap();
        assert_ne!(original, changed.canonical_bytes());
        changed = claims.clone();
        changed.control_revision = WorkItemControlRevision::new(4).unwrap();
        assert_ne!(original, changed.canonical_bytes());
        changed = claims.clone();
        changed.actor.principal_kind = PrincipalKind::Service;
        assert!(!changed.is_valid_at(claims.issued_at));
        changed = claims.clone();
        changed.expires_at = UtcTimestamp::from_unix_millis(1_780_000_120_001);
        assert!(!changed.is_valid_at(claims.issued_at));
    }
}
