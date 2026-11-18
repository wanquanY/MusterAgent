//! Transport adapter only. The original Native Tool receipt/custody/grant
//! remain in this ingress; execution-directory semantics belong to activity.
use super::*;
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityResult};

pub(super) async fn prepare(
    store: &PostgresDeviceResourceStore,
    auth: Arc<authority::Authority>,
    content: Arc<RecordingContentStore>,
    invocation: CapabilityInvocation,
) -> Result<prepare::Prepared, Error> {
    let contract = DialogueExecutionDirectoryContract::from_tool_key(invocation.tool_key.as_str())
        .ok_or(Error::Forbidden)?;
    let query = DialogueExecutionDirectoryQuery::parse(auth.request.command().request_bytes())
        .map_err(|_| Error::InvalidArguments)?;
    let (mut tx, _, custody) = auth.current_snapshot().await?;
    // Only qualified, immutable custody may supply a task selection. Native
    // WorkItem execution keeps its existing dialogue contract; a task target
    // belongs to the accepted user input, not an arbitrary Tool argument.
    let selected = match &custody {
        super::super::device_custody::DeviceCustody::Intent(source) => source.intent.task_target,
        super::super::device_custody::DeviceCustody::WorkItem(_) => None,
    };
    let page = crate::dialogue_execution_activity::directory::list(
        &mut tx,
        content.as_ref(),
        crate::dialogue_execution_activity::directory::ExecutionDirectoryOwner {
            actor: auth.actor,
            agent: custody.agent_id(),
            dialogue: custody.dialogue_id(),
        },
        query,
        contract,
        crate::dialogue_execution_activity::directory::QueryScope::from_task_target(selected),
    )
    .await
    .map_err(|e| match e {
        DialogueStoreError::Infrastructure => Error::Infrastructure,
        DialogueStoreError::Corrupt => Error::Corrupt,
        DialogueStoreError::RevisionConflict => Error::Conflict,
        _ => Error::Forbidden,
    })?;
    tx.commit().await.map_err(|_| Error::Infrastructure)?;
    let bytes = serde_json::to_vec(&dialogue_execution_directory_value(&page))
        .map_err(|_| Error::Corrupt)?;
    let reference = store
        .stage(
            auth.actor,
            auth.request.command(),
            b"dialogue-execution-directory-result.v1",
            &bytes,
        )
        .await?;
    Ok(prepare::Prepared::Query(
        CapabilityResult {
            result_hash: reference.sha256,
            result_ref: reference,
            result_schema: invocation.result_schema,
            provider_request_ref: None,
        },
        page.authorized_content,
    ))
}
