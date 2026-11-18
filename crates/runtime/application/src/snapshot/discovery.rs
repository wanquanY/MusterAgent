//! Carry-forward is reauthorized against the current exact catalog and recorded.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_discovery::{
    ActivationConstraints, AuthorizedToolCatalog, reauthorize_tool_exposures,
};
use musteragent_event_envelope::ExecutionScope;
use musteragent_policy::RequiredContext;
use musteragent_runtime_content::{ContentStore, PutContent};
use std::{collections::BTreeSet, num::NonZeroU32};

pub(super) async fn carry_forward(
    request: &SnapshotResolutionRequest,
    catalog: &AuthorizedToolCatalog,
    content: &dyn ContentStore,
    exposed: &mut Vec<musteragent_tool::ToolKey>,
    max_exposed_tools: NonZeroU32,
) -> Result<(Option<RunId>, Option<ContentRef>), ErrorCode> {
    let Some(prior) = &request.prior_discovery else {
        return Ok((None, None));
    };
    let placements = catalog
        .definitions()
        .iter()
        .map(|d| (d.provider_id, d.location))
        .collect::<BTreeSet<_>>();
    let mut contexts = BTreeSet::new();
    if matches!(
        request.run_context.execution_scope,
        ExecutionScope::Workspace { .. }
            | ExecutionScope::DevicePrivate {
                workspace_id: Some(_),
                ..
            }
    ) {
        contexts.insert(RequiredContext::Workspace);
    }
    if !request.command.payload.attachment_refs.is_empty() {
        contexts.insert(RequiredContext::ConversationAttachments);
    }
    let decision = reauthorize_tool_exposures(
        prior.source_run_id,
        &prior.tool_exposures,
        catalog,
        &ActivationConstraints {
            available_locations: placements.iter().map(|(_, l)| *l).collect(),
            available_contexts: contexts,
            max_exposed_tools,
        },
        &placements,
        &Default::default(),
    );
    let mut existing = exposed.iter().cloned().collect::<BTreeSet<_>>();
    for key in &decision.accepted_tool_refs {
        if existing.insert(key.clone()) {
            exposed.push(key.clone());
        }
    }
    let reference = content
        .put(PutContent {
            tenant_id: request.actor.tenant_id,
            media_type: MediaType::new("application/json")
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
            bytes: decision
                .encode_canonical_json()
                .map_err(|_| ErrorCode::RuntimeSnapshotInvalid)?,
        })
        .await
        .map_err(|error| match error {
            musteragent_runtime_content::ContentStoreError::Unavailable => {
                ErrorCode::DependencyUnavailable
            }
            _ => ErrorCode::ContentIntegrityFailed,
        })?;
    Ok((Some(prior.source_run_id), Some(reference)))
}
