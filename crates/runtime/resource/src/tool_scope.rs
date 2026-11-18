//! Explicit tool scope resolves through the same authority as FileQuery and UI.
//! Removing the selector happens before ordinary operation parsing, not in a
//! second implementation of file mutation semantics.
use crate::{
    ResolveResourceContext, ResourceCommandSource, ResourceContextAuthority, ResourceToolContext,
    ResourceToolError,
};
use serde_json::Value;
use std::sync::Arc;

pub(crate) async fn bind_scope(
    context: &mut ResourceToolContext,
    bytes: Vec<u8>,
    authority: Option<&Arc<dyn ResourceContextAuthority>>,
) -> Result<Vec<u8>, ResourceToolError> {
    let mut value: Value =
        serde_json::from_slice(&bytes).map_err(|_| ResourceToolError::InvalidInput)?;
    let object = value
        .as_object_mut()
        .ok_or(ResourceToolError::InvalidInput)?;
    let Some(scope) = object.remove("scope") else {
        return Ok(bytes);
    };
    if matches!(
        context.resolved_source,
        Some(
            ResourceCommandSource::DialogueExecution { .. }
                | ResourceCommandSource::DeviceExecution { .. }
                | ResourceCommandSource::Account { .. }
        )
    ) {
        return Err(ResourceToolError::InvalidInput);
    }
    context.resolved_source = Some(match scope.as_str() {
        Some("work") => {
            let resolver = authority.ok_or(ResourceToolError::UnsupportedTool)?;
            let resolved = resolver
                .resolve_execution(ResolveResourceContext {
                    actor: context.invocation.actor,
                    conversation_id: context.invocation.conversation_id,
                    invocation_id: Some(context.invocation.invocation_id),
                    now: context.now,
                })
                .await
                .map_err(|_| ResourceToolError::InvalidInput)?;
            if resolved.actor != context.invocation.actor
                || resolved.conversation_id() != Some(context.invocation.conversation_id)
                || resolved.tenant_id != context.invocation.tenant_id
                || resolved.workspace_id() != context.invocation.workspace_id
            {
                return Err(ResourceToolError::IdentityMismatch);
            }
            resolved.source
        }
        Some("project") if context.invocation.workspace_id.is_some() => {
            ResourceCommandSource::Conversation {
                conversation_id: context.invocation.conversation_id,
                workspace_id: context.invocation.workspace_id,
            }
        }
        Some("conversation") => ResourceCommandSource::Conversation {
            conversation_id: context.invocation.conversation_id,
            workspace_id: None,
        },
        _ => return Err(ResourceToolError::InvalidInput),
    });
    serde_json::to_vec(&value).map_err(|_| ResourceToolError::InvalidInput)
}
