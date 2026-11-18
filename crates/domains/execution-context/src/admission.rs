//! Pre-Run decisions are not Model deliveries and cannot mint a Run fence.
use musteragent_content_ref::Sha256;
use musteragent_identity::{
    Actor, CommandId, DottedKey, PrincipalKind, UserIntentId, UtcTimestamp,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DialogueAdmissionRejectionReason {
    AccountUnavailable,
    ExecutionNotAllowed,
}
impl DialogueAdmissionRejectionReason {
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
    /// Deliberately closed, useful text. No provider, tenant, policy, or error
    /// diagnostics are accepted from a transport response or model output.
    pub const fn message(self) -> &'static str {
        match self {
            Self::AccountUnavailable => {
                "这次请求还没有开始处理。当前账号暂时无法使用 Agent，请检查账号状态，恢复后重新发送。"
            }
            Self::ExecutionNotAllowed => {
                "这次请求还没有开始处理。当前账号尚未获准使用所需的执行能力，请检查账号与订阅状态，恢复后重新发送。"
            }
        }
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueAdmissionRejectionClaims {
    pub actor: Actor,
    pub intent_id: UserIntentId,
    pub decision_id: CommandId,
    pub facts_sha256: Sha256,
    pub reason: DialogueAdmissionRejectionReason,
    pub issued_at: UtcTimestamp,
    pub expires_at: UtcTimestamp,
}
impl DialogueAdmissionRejectionClaims {
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
        let mut result = b"musteragent.dialogue-admission-rejection@1\0".to_vec();
        for id in [
            self.actor.tenant_id.into_uuid(),
            self.actor.principal_id.into_uuid(),
            self.intent_id.into_uuid(),
            self.decision_id.into_uuid(),
        ] {
            result.extend_from_slice(id.as_bytes());
        }
        result.extend_from_slice(&self.facts_sha256.into_bytes());
        result.extend_from_slice(&self.reason.code().to_be_bytes());
        result.extend_from_slice(&self.issued_at.unix_millis().to_be_bytes());
        result.extend_from_slice(&self.expires_at.unix_millis().to_be_bytes());
        result
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SignedDialogueAdmissionRejection {
    pub key_id: DottedKey,
    pub claims: DialogueAdmissionRejectionClaims,
    pub signature: [u8; 64],
}

/// Frozen authorization facts shared by the application and canonical codec.
/// Its permission mode describes desktop preference, never physical authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DialogueExecutionFactBinding {
    pub product_release: Option<musteragent_identity::ProductReleaseRef>,
    /// Original Native physical placement, not a logical Project/data namespace.
    pub device_workspace_id: Option<musteragent_identity::WorkspaceId>,
    pub actor: Actor,
    pub intent_id: UserIntentId,
    pub agent_id: musteragent_identity::AgentId,
    pub agent_version_id: musteragent_identity::AgentVersionId,
    pub dialogue_id: musteragent_identity::AgentDialogueId,
    pub context_conversation_id: musteragent_identity::ConversationId,
    pub command_id: CommandId,
    pub run_id: musteragent_identity::RunId,
    pub context_watermark: crate::DialogueCursor,
    pub manifest_revision: u64,
    pub instructions_revision: u64,
    pub permission_revision: u64,
    pub permission_mode: i32,
    pub source_sha256: Sha256,
    pub default_agent_direct_intent: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialogue_denial_binary_commitment_matches_backend() {
        let claims = DialogueAdmissionRejectionClaims {
            actor: Actor {
                tenant_id: "019b2f80-0000-7000-8000-000000000001".parse().unwrap(),
                principal_id: "019b2f80-0000-7000-8000-000000000002".parse().unwrap(),
                principal_kind: PrincipalKind::User,
            },
            intent_id: "019b2f80-0000-7000-8000-000000000003".parse().unwrap(),
            decision_id: "019b2f80-0000-7000-8000-000000000004".parse().unwrap(),
            facts_sha256: Sha256::from_bytes((0..32).collect::<Vec<u8>>()).unwrap(),
            reason: DialogueAdmissionRejectionReason::ExecutionNotAllowed,
            issued_at: UtcTimestamp::from_unix_millis(1_780_000_000_000),
            expires_at: UtcTimestamp::from_unix_millis(1_780_000_120_000),
        };
        assert_eq!(
            Sha256::digest(claims.canonical_bytes()).to_string(),
            "206e8f042cd2ec022ef4929ab671404ecda016dba408c11f782808b3072f0de6"
        );
        assert!(claims.is_valid_at(claims.issued_at));
        assert!(!claims.is_valid_at(claims.expires_at));
    }
}
