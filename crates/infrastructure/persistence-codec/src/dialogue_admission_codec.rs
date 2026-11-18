//! One canonical facts commitment and one closed non-executable denial codec.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::*;
use musteragent_identity::*;
use prost::Message;

pub fn facts(value: &DialogueExecutionFactBinding) -> wire::GetDialogueExecutionGrantFactsResponse {
    let facts = wire::DialogueExecutionGrantFacts {
        product_release: value
            .product_release
            .as_ref()
            .map(crate::encode_product_release),
        device_workspace_id: value.device_workspace_id.map(|id| wire::WorkspaceId {
            value: id.to_string(),
        }),
        actor: Some(actor(value.actor)),
        intent_id: Some(wire::UserIntentId {
            value: value.intent_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: value.agent_id.to_string(),
        }),
        agent_version_id: Some(wire::AgentVersionId {
            value: value.agent_version_id.to_string(),
        }),
        dialogue_id: Some(wire::AgentDialogueId {
            value: value.dialogue_id.to_string(),
        }),
        context_conversation_id: Some(wire::ConversationId {
            value: value.context_conversation_id.to_string(),
        }),
        command_id: Some(wire::CommandId {
            value: value.command_id.to_string(),
        }),
        run_id: Some(wire::RunId {
            value: value.run_id.to_string(),
        }),
        context_watermark: Some(wire::AgentDialogueCursor {
            epoch: value.context_watermark.epoch,
            seq: value.context_watermark.seq,
        }),
        manifest_revision: value.manifest_revision,
        instructions_revision: value.instructions_revision,
        permission_revision: value.permission_revision,
        permission_mode: value.permission_mode,
        source_sha256: value.source_sha256.into_bytes().to_vec(),
        default_agent_direct_intent: value.default_agent_direct_intent,
    };
    wire::GetDialogueExecutionGrantFactsResponse {
        facts_sha256: Sha256::digest(facts.encode_to_vec()).into_bytes().to_vec(),
        facts: Some(facts),
        applications: None,
        desktop_origin: false,
        origin_device_id: None,
        origin_installation_id: None,
    }
}
fn actor(value: Actor) -> wire::Actor {
    wire::Actor {
        tenant_id: Some(wire::TenantId {
            value: value.tenant_id.to_string(),
        }),
        principal_id: Some(wire::PrincipalId {
            value: value.principal_id.to_string(),
        }),
        principal_kind: wire::PrincipalKind::User as i32,
    }
}
pub fn rejection(
    value: &SignedDialogueAdmissionRejection,
) -> wire::SignedDialogueAdmissionRejection {
    let c = &value.claims;
    wire::SignedDialogueAdmissionRejection {
        key_id: value.key_id.as_str().to_owned(),
        signature: value.signature.to_vec(),
        claims: Some(wire::DialogueAdmissionRejectionClaims {
            actor: Some(actor(c.actor)),
            intent_id: Some(wire::UserIntentId {
                value: c.intent_id.to_string(),
            }),
            decision_id: Some(wire::CommandId {
                value: c.decision_id.to_string(),
            }),
            facts_sha256: c.facts_sha256.into_bytes().to_vec(),
            reason: c.reason.code() as i32,
            issued_at_ms: c.issued_at.unix_millis(),
            expires_at_ms: c.expires_at.unix_millis(),
        }),
    }
}
pub fn decode_rejection(
    value: wire::SignedDialogueAdmissionRejection,
) -> Result<SignedDialogueAdmissionRejection, &'static str> {
    let c = value.claims.ok_or("missing claims")?;
    let a = c.actor.ok_or("missing actor")?;
    if a.principal_kind != wire::PrincipalKind::User as i32 {
        return Err("invalid actor");
    }
    Ok(SignedDialogueAdmissionRejection {
        key_id: DottedKey::new(value.key_id).map_err(|_| "key")?,
        signature: value.signature.try_into().map_err(|_| "signature")?,
        claims: DialogueAdmissionRejectionClaims {
            actor: Actor {
                tenant_id: a
                    .tenant_id
                    .ok_or("tenant")?
                    .value
                    .parse()
                    .map_err(|_| "tenant")?,
                principal_id: a
                    .principal_id
                    .ok_or("principal")?
                    .value
                    .parse()
                    .map_err(|_| "principal")?,
                principal_kind: PrincipalKind::User,
            },
            intent_id: c
                .intent_id
                .ok_or("intent")?
                .value
                .parse()
                .map_err(|_| "intent")?,
            decision_id: c
                .decision_id
                .ok_or("decision")?
                .value
                .parse()
                .map_err(|_| "decision")?,
            facts_sha256: Sha256::from_bytes(&c.facts_sha256).map_err(|_| "hash")?,
            reason: match c.reason {
                1 => DialogueAdmissionRejectionReason::AccountUnavailable,
                2 => DialogueAdmissionRejectionReason::ExecutionNotAllowed,
                _ => return Err("reason"),
            },
            issued_at: UtcTimestamp::from_unix_millis(c.issued_at_ms),
            expires_at: UtcTimestamp::from_unix_millis(c.expires_at_ms),
        },
    })
}
