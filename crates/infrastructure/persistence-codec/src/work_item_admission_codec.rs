//! Closed non-executable Task refusal codec, separate from UserIntent outcomes.
use crate::wire;
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_work_item::*;
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
    value: &SignedWorkItemAdmissionRejection,
) -> wire::SignedWorkItemAdmissionRejection {
    let c = &value.claims;
    wire::SignedWorkItemAdmissionRejection {
        key_id: value.key_id.as_str().to_owned(),
        signature: value.signature.to_vec(),
        claims: Some(wire::WorkItemAdmissionRejectionClaims {
            actor: Some(actor(c.actor)),
            work_item_id: Some(wire::WorkItemId {
                value: c.work_item_id.to_string(),
            }),
            goal_revision: c.goal_revision.get(),
            control_revision: c.control_revision.get(),
            assignment_epoch: c.assignment_epoch.get(),
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
    value: wire::SignedWorkItemAdmissionRejection,
) -> Result<SignedWorkItemAdmissionRejection, &'static str> {
    let c = value.claims.ok_or("missing claims")?;
    let a = c.actor.ok_or("missing actor")?;
    if a.principal_kind != wire::PrincipalKind::User as i32 {
        return Err("invalid actor");
    }
    Ok(SignedWorkItemAdmissionRejection {
        key_id: DottedKey::new(value.key_id).map_err(|_| "key")?,
        signature: value.signature.try_into().map_err(|_| "signature")?,
        claims: WorkItemAdmissionRejectionClaims {
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
            work_item_id: c
                .work_item_id
                .ok_or("task")?
                .value
                .parse()
                .map_err(|_| "task")?,
            goal_revision: WorkItemGoalRevision::new(c.goal_revision)
                .map_err(|_| "goal_revision")?,
            control_revision: WorkItemControlRevision::new(c.control_revision)
                .map_err(|_| "control_revision")?,
            assignment_epoch: AssignmentEpoch::new(c.assignment_epoch)
                .map_err(|_| "assignment_epoch")?,
            decision_id: c
                .decision_id
                .ok_or("decision")?
                .value
                .parse()
                .map_err(|_| "decision")?,
            facts_sha256: Sha256::from_bytes(&c.facts_sha256).map_err(|_| "hash")?,
            reason: match c.reason {
                1 => WorkItemAdmissionRejectionReason::AccountUnavailable,
                2 => WorkItemAdmissionRejectionReason::ExecutionNotAllowed,
                _ => return Err("reason"),
            },
            issued_at: UtcTimestamp::from_unix_millis(c.issued_at_ms),
            expires_at: UtcTimestamp::from_unix_millis(c.expires_at_ms),
        },
    })
}
