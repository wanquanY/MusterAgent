//! Closed source transport for an actual WorkItem assignment. No message
//! parser, synthetic intent or fabricated Cloud Run participates in this codec.
mod facts;
use crate::{
    agent_release_codec as release, dialogue_codec::*, dialogue_context_codec as context, wire,
};
pub use facts::{facts_from_binding, facts_from_device_preparation, facts_from_preparation};
use musteragent_content_ref::Sha256;
use musteragent_identity::*;
use musteragent_runtime_session::*;
use musteragent_work_item::*;
use prost::Message;
type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;
pub const MAX_WORK_ITEM_DEVICE_SOURCE_BYTES: usize = 4 * 1024 * 1024;

pub fn target(v: &WorkItemDeviceTarget) -> wire::WorkItemDeviceTarget {
    wire::WorkItemDeviceTarget {
        device_id: Some(wire::DeviceId {
            value: v.device_id.to_string(),
        }),
        installation_id: Some(wire::DeviceInstallationId {
            value: v.installation_id.to_string(),
        }),
        membership_id: v.membership_id.to_string(),
        membership_epoch: v.membership_epoch,
        consent_id: v.consent_id.to_string(),
        consent_sha256: v.consent_sha256.into_bytes().to_vec(),
        consent_anchor: v
            .consent_anchor
            .as_ref()
            .map(|p| wire::WorkItemDeviceConsentAnchor {
                authority_ref: p.authority_ref.clone(),
                policy_ref: p.policy_ref.clone(),
                effective_capabilities_sha256: p
                    .effective_capabilities_sha256
                    .into_bytes()
                    .to_vec(),
            }),
    }
}
pub fn decode_target(v: wire::WorkItemDeviceTarget) -> Result<WorkItemDeviceTarget, Error> {
    let parsed = WorkItemDeviceTarget {
        device_id: id(v.device_id.clone().map(|x| x.value))?,
        installation_id: id(v.installation_id.clone().map(|x| x.value))?,
        membership_id: v.membership_id.parse().map_err(|_| BAD)?,
        membership_epoch: v.membership_epoch,
        consent_id: v.consent_id.parse().map_err(|_| BAD)?,
        consent_sha256: Sha256::from_bytes(&v.consent_sha256).map_err(|_| BAD)?,
        consent_anchor: v
            .consent_anchor
            .as_ref()
            .map(|p| {
                Ok::<_, Error>(WorkItemDeviceConsentAnchor {
                    authority_ref: p.authority_ref.clone(),
                    policy_ref: p.policy_ref.clone(),
                    effective_capabilities_sha256: Sha256::from_bytes(
                        &p.effective_capabilities_sha256,
                    )
                    .map_err(|_| BAD)?,
                })
            })
            .transpose()?,
    };
    parsed.validate().map_err(|_| BAD)?;
    if target(&parsed) != v
        || parsed.consent_sha256.into_bytes() == [0; 32]
        || parsed.membership_epoch > i64::MAX as u64
    {
        return Err(BAD);
    }
    Ok(parsed)
}
pub fn delegation(v: &WorkItemDelegationCeiling) -> wire::WorkItemDelegationCeiling {
    wire::WorkItemDelegationCeiling {
        product_release: v
            .product_release
            .as_ref()
            .map(crate::encode_product_release),
        source_run_id: Some(wire::RunId {
            value: v.source_run_id.to_string(),
        }),
        source_invocation_id: Some(wire::InvocationId {
            value: v.source_invocation_id.to_string(),
        }),
        source_snapshot_id: Some(wire::RuntimeSnapshotId {
            value: v.source_snapshot_id.to_string(),
        }),
        capabilities: v
            .capabilities
            .iter()
            .map(crate::execution_grant_codec::capability_entitlement)
            .collect(),
        input_refs: v.input_refs.iter().map(crate::encode_content_ref).collect(),
    }
}
pub fn decode_delegation(
    v: wire::WorkItemDelegationCeiling,
) -> Result<WorkItemDelegationCeiling, Error> {
    if v.capabilities.len() > 512 || v.input_refs.len() > 256 {
        return Err(BAD);
    }
    let original = v.clone();
    let parsed = WorkItemDelegationCeiling {
        product_release: v
            .product_release
            .map(crate::product_release_codec::decode)
            .transpose()
            .map_err(|_| BAD)?,
        source_run_id: id(v.source_run_id.map(|x| x.value))?,
        source_invocation_id: id(v.source_invocation_id.map(|x| x.value))?,
        source_snapshot_id: id(v.source_snapshot_id.map(|x| x.value))?,
        capabilities: v
            .capabilities
            .into_iter()
            .map(crate::execution_grant_decode::capability_entitlement)
            .collect::<Result<_, _>>()
            .map_err(|_| BAD)?,
        input_refs: v
            .input_refs
            .into_iter()
            .map(content)
            .collect::<Result<_, _>>()?,
    };
    parsed.validate().map_err(|_| BAD)?;
    if delegation(&parsed) != original {
        return Err(BAD);
    }
    Ok(parsed)
}
fn metadata(p: &WorkItemExecutionPreparation) -> wire::WorkItemDeviceSourceMetadata {
    let r = &p.work_item;
    wire::WorkItemDeviceSourceMetadata {
        state_revision: r.core.revisions.state.get(),
        status: match r.core.status {
            WorkItemStatus::Queued => "queued",
            WorkItemStatus::Ready => "ready",
            WorkItemStatus::Active => "active",
            WorkItemStatus::Blocked => "blocked",
            WorkItemStatus::Verifying => "verifying",
            WorkItemStatus::Completed => "completed",
            WorkItemStatus::Superseded => "superseded",
            WorkItemStatus::Cancelled => "cancelled",
        }
        .into(),
        priority: match r.core.spec.priority {
            WorkItemPriority::Low => "low",
            WorkItemPriority::Normal => "normal",
            WorkItemPriority::High => "high",
            WorkItemPriority::Critical => "critical",
        }
        .into(),
        dependencies: r
            .core
            .spec
            .dependencies
            .iter()
            .map(|x| wire::WorkItemId {
                value: x.to_string(),
            })
            .collect(),
        follow_up_of: r.core.spec.follow_up_of.map(|x| wire::WorkItemId {
            value: x.to_string(),
        }),
        parent_work_item_id: r.parent_work_item_id.map(|x| wire::WorkItemId {
            value: x.to_string(),
        }),
        goal_content_ref: Some(crate::encode_content_ref(&r.goal_content_ref)),
        created_at_ms: r.created_at.unix_millis(),
        updated_at_ms: r.updated_at.unix_millis(),
        directory_seq: r.directory_seq,
    }
}
pub fn encode(source: &WorkItemDeviceExecutionPreparation) -> Result<Vec<u8>, Error> {
    encode_mode(source, false)
}
pub fn encode_frozen(source: &WorkItemDeviceExecutionPreparation) -> Result<Vec<u8>, Error> {
    encode_mode(source, false)
}
fn encode_mode(
    source: &WorkItemDeviceExecutionPreparation,
    frozen: bool,
) -> Result<Vec<u8>, Error> {
    source.validate().map_err(|_| BAD)?;
    let p = source.preparation();
    let facts = facts_from_device_preparation(source)
        .map_err(|_| BAD)?
        .facts;
    let value = wire::WorkItemDeviceExecutionSource {
        schema_version: if p.feedback.is_some() {
            if frozen { 7 } else { 6 }
        } else if frozen {
            if source.device_workspace_id().is_some() {
                5
            } else {
                4
            }
        } else if !p.dependency_results.is_empty() {
            3
        } else if source.device_workspace_id().is_some() {
            2
        } else {
            1
        },
        facts,
        metadata: Some(metadata(p)),
        agent: Some(agent(&p.executing_agent)),
        configuration: Some(configuration(&p.configuration)),
        definition: Some(release::definition(&p.assistant_identity.definition)?),
        version: Some(release::version(&p.assistant_identity.version)?),
        context_cut: Some(context::cut(&p.context_cut)),
        previous_result: p
            .feedback
            .as_ref()
            .map(|f| crate::encode_content_ref(&f.previous_result)),
        feedback_files: p
            .feedback
            .iter()
            .flat_map(|f| &f.files)
            .map(crate::encode_content_ref)
            .collect(),
        feedback_request: p
            .feedback
            .as_ref()
            .map(|f| crate::encode_content_ref(&f.request)),
        input_attachments: p.input_attachments.iter().map(attachment).collect(),
        approved_delegation: Some(delegation(source.approved_delegation())),
        dependency_results: p
            .dependency_results
            .iter()
            .map(|d| wire::WorkItemDependencyResult {
                work_item_id: Some(wire::WorkItemId {
                    value: d.work_item_id.to_string(),
                }),
                goal_revision: d.goal_revision,
                control_revision: d.control_revision,
                assignment_epoch: d.assignment_epoch,
                producing_run_id: Some(wire::RunId {
                    value: d.producing_run_id.to_string(),
                }),
                content_ref: Some(crate::encode_content_ref(&d.content_ref)),
            })
            .collect(),
    };
    let bytes = value.encode_to_vec();
    if bytes.is_empty() || bytes.len() > MAX_WORK_ITEM_DEVICE_SOURCE_BYTES {
        return Err(BAD);
    }
    Ok(bytes)
}
pub fn decode(bytes: &[u8]) -> Result<WorkItemDeviceExecutionPreparation, Error> {
    if bytes.is_empty() || bytes.len() > MAX_WORK_ITEM_DEVICE_SOURCE_BYTES {
        return Err(BAD);
    }
    let v = wire::WorkItemDeviceExecutionSource::decode(bytes).map_err(|_| BAD)?;
    if !matches!(v.schema_version, 1..=7) || v.encode_to_vec() != bytes {
        return Err(BAD);
    }
    let frozen = false;
    let f = required(v.facts)?;
    let workspace = f
        .device_workspace_id
        .clone()
        .map(|v| id(Some(v.value)))
        .transpose()?;
    if v.schema_version
        != if v.feedback_request.is_some() {
            if frozen { 7 } else { 6 }
        } else if frozen {
            if workspace.is_some() { 5 } else { 4 }
        } else if !v.dependency_results.is_empty() {
            3
        } else if workspace.is_some() {
            2
        } else {
            1
        }
        || f.report.is_some()
    {
        return Err(BAD);
    }
    let a = required(f.actor.clone())?;
    if a.principal_kind != wire::PrincipalKind::User as i32 {
        return Err(BAD);
    }
    let actor = Actor {
        tenant_id: id(a.tenant_id.map(|x| x.value))?,
        principal_id: id(a.principal_id.map(|x| x.value))?,
        principal_kind: PrincipalKind::User,
    };
    let scope = OwnedAgentWorkItemScope {
        space_id: id(f.space_id.clone().map(|x| x.value))?,
        owner_id: id(f.owner_id.clone().map(|x| x.value))?,
        agent_id: id(f.requesting_agent_id.clone().map(|x| x.value))?,
    };
    let key = WorkItemExecutionKey {
        actor,
        scope,
        work_item_id: id(f.work_item_id.clone().map(|x| x.value))?,
        goal_revision: WorkItemGoalRevision::new(f.goal_revision).map_err(|_| BAD)?,
        control_revision: WorkItemControlRevision::new(f.control_revision).map_err(|_| BAD)?,
        assignment_epoch: AssignmentEpoch::new(f.assignment_epoch).map_err(|_| BAD)?,
        report_from_run_id: None,
        executing_agent_id: id(f.executing_agent_id.clone().map(|x| x.value))?,
    };
    let m = required(v.metadata)?;
    if m.dependencies.len() > 256
        || m.directory_seq == 0
        || m.created_at_ms < 0
        || m.updated_at_ms < m.created_at_ms
    {
        return Err(BAD);
    }
    let core = WorkItemCoreMetadata {
        work_item_id: key.work_item_id,
        tenant_id: actor.tenant_id,
        scope: WorkItemScope::OwnedAgent(scope),
        revisions: WorkItemRevisions {
            goal: key.goal_revision,
            control: key.control_revision,
            state: WorkItemStateRevision::new(m.state_revision).map_err(|_| BAD)?,
        },
        spec: WorkItemSpecMetadata {
            dependencies: m
                .dependencies
                .into_iter()
                .map(|x| id(Some(x.value)))
                .collect::<Result<_, _>>()?,
            priority: match m.priority.as_str() {
                "low" => WorkItemPriority::Low,
                "normal" => WorkItemPriority::Normal,
                "high" => WorkItemPriority::High,
                "critical" => WorkItemPriority::Critical,
                _ => return Err(BAD),
            },
            follow_up_of: m.follow_up_of.map(|x| id(Some(x.value))).transpose()?,
        },
        status: match m.status.as_str() {
            "queued" => WorkItemStatus::Queued,
            "ready" => WorkItemStatus::Ready,
            "active" => WorkItemStatus::Active,
            "blocked" => WorkItemStatus::Blocked,
            "verifying" => WorkItemStatus::Verifying,
            "completed" => WorkItemStatus::Completed,
            "superseded" => WorkItemStatus::Superseded,
            "cancelled" => WorkItemStatus::Cancelled,
            _ => return Err(BAD),
        },
        assignment_epoch: Some(key.assignment_epoch),
    };
    core.spec
        .validate_contract(core.work_item_id)
        .map_err(|_| BAD)?;
    let p = WorkItemExecutionPreparation {
        key,
        target: WorkItemExecutionTarget::RemoteDevice(decode_target(required(
            f.device_target.clone(),
        )?)?),
        work_item: musteragent_runtime_session::WorkItemAuthorityRecord {
            core,
            goal_content_ref: content(required(m.goal_content_ref)?)?,
            source_dialogue_id: id(f.source_dialogue_id.clone().map(|x| x.value))?,
            source_intent_id: id(f.source_intent_id.clone().map(|x| x.value))?,
            parent_work_item_id: m
                .parent_work_item_id
                .map(|x| id(Some(x.value)))
                .transpose()?,
            created_at: UtcTimestamp::from_unix_millis(m.created_at_ms),
            updated_at: UtcTimestamp::from_unix_millis(m.updated_at_ms),
            directory_seq: m.directory_seq,
        },
        executing_agent: decode_agent(required(v.agent)?)?,
        configuration: decode_configuration(required(v.configuration)?)?,
        assistant_identity: AgentAdmissionIdentity {
            definition: release::decode_definition(required(v.definition)?)?,
            version: release::decode_version(required(v.version)?)?,
        },
        default_agent_id: id(f.default_agent_id.clone().map(|x| x.value))?,
        context_cut: context::decode_cut(required(v.context_cut)?)?,
        delegation: decode_delegation(required(f.delegation.clone())?)?,
        feedback: match (v.previous_result, v.feedback_request) {
            (Some(old), Some(request)) => {
                Some(musteragent_runtime_session::WorkItemFeedbackContext {
                    previous_result: content(old)?,
                    request: content(request)?,
                    files: v
                        .feedback_files
                        .into_iter()
                        .map(content)
                        .collect::<Result<_, _>>()?,
                })
            }
            (None, None) if v.feedback_files.is_empty() => None,
            _ => return Err(BAD),
        },
        input_attachments: decode_attachments(v.input_attachments)?,
        dependency_results: v
            .dependency_results
            .into_iter()
            .map(|d| {
                Ok(WorkItemDependencyResult {
                    work_item_id: id(d.work_item_id.map(|x| x.value))?,
                    goal_revision: d.goal_revision,
                    control_revision: d.control_revision,
                    assignment_epoch: d.assignment_epoch,
                    producing_run_id: id(d.producing_run_id.map(|x| x.value))?,
                    content_ref: content(required(d.content_ref)?)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?,
        report: None,
    };
    p.validate().map_err(|_| BAD)?;
    let source = WorkItemDeviceExecutionPreparation::from_approved_delegation_in_workspace(
        p,
        decode_delegation(required(v.approved_delegation)?)?,
        workspace,
    )
    .map_err(|_| BAD)?;
    if facts_from_device_preparation(&source)
        .map_err(|_| BAD)?
        .facts
        .as_ref()
        != Some(&f)
    {
        return Err(BAD);
    }
    if encode_mode(&source, frozen)? != bytes {
        return Err(BAD);
    }
    Ok(source)
}
