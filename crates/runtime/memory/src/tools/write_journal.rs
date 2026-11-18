//! Host-private facts captured before a fenced write. This journal uses only
//! original Content identities; its contents never grant authority to commit.
use super::preparation::{Binding, content_error, ensure_running, identity, put_exact};
use super::*;
use musteragent_memory_business::preparation::WritePreparation;
use musteragent_runtime_capability::OperationContext;
use musteragent_runtime_content::{ContentStoreError, verify_content};
use serde::de::DeserializeOwned;

const MEDIA: &str = "application/vnd.musteragent.memory-prepared-write+json";
const MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
enum Schema {
    #[serde(rename = "musteragent.memory.prepared-write.v1")]
    V1,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    schema: Schema,
    implementation: String,
    binding: Binding,
    prepared_at_ms: i64,
    business: WritePreparation,
    facts: T,
}

pub(super) struct Frozen<T> {
    pub at: UtcTimestamp,
    pub business: WritePreparation,
    pub facts: T,
}

pub(super) async fn freeze<T, F>(
    content: &dyn ContentStore,
    planner: &dyn crate::MemoryWritePlanner,
    context: &MemoryToolContext,
    request: &[u8],
    execution: Option<&OperationContext>,
    capture: F,
) -> Result<Frozen<T>, MemoryToolError>
where
    T: Serialize + DeserializeOwned,
    F: std::future::Future<Output = Result<T, MemoryToolError>>,
{
    let purpose = match context.invocation.tool_key.as_str() {
        "user_memory.remember@2" => "remember-plan",
        "experience.publish@1" => "publish-plan",
        _ => return Err(MemoryToolError::UnsupportedTool),
    };
    verify_content(&context.invocation.request_ref, request)
        .map_err(|_| MemoryToolError::InvalidResult)?;
    let binding = Binding::from_context(context)?;
    let implementation = planner.identity();
    let id = identity(context.invocation.invocation_id, purpose);
    let read = || async {
        let reference = crate::retrieval::read_stage(
            execution,
            content.find_prepared_at(context.actor.tenant_id, id),
        )
        .await
        .map_err(map_retrieval_error)?
        .map_err(content_error)?;
        let Some(reference) = reference else {
            return Ok(None);
        };
        if reference.content_id != id
            || reference.media_type.as_str() != MEDIA
            || reference.byte_length > MAX_BYTES as u64
        {
            return Err(MemoryToolError::InvalidResult);
        }
        let bytes = crate::retrieval::read_stage(
            execution,
            content.read(context.actor.tenant_id, &reference),
        )
        .await
        .map_err(map_retrieval_error)?
        .map_err(content_error)?;
        verify_content(&reference, &bytes).map_err(|_| MemoryToolError::InvalidResult)?;
        let saved: Envelope<T> =
            serde_json::from_slice(&bytes).map_err(|_| MemoryToolError::InvalidResult)?;
        if saved.binding != binding
            || saved.implementation != implementation.as_str()
            || encode(&saved)? != bytes
        {
            return Err(MemoryToolError::InvalidResult);
        }
        WritePreparation::decode_for_request(
            context.invocation.tool_key.as_str(),
            request,
            &saved
                .business
                .canonical_bytes()
                .map_err(|_| MemoryToolError::InvalidResult)?,
        )
        .map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(Some(saved))
    };
    let saved = match read().await? {
        Some(saved) => saved,
        None => {
            let business = crate::write_planner::prepare(
                planner,
                context.invocation.tool_key.as_str(),
                request,
                execution,
            )
            .await?;
            ensure_running(execution)?;
            // The capture future is never polled on recovery: it cannot reload
            // the latest head, evaluation or approval to change an old decision.
            let facts = capture.await?;
            let saved = Envelope {
                schema: Schema::V1,
                implementation: implementation.as_str().into(),
                binding: binding.clone(),
                prepared_at_ms: context.now.unix_millis(),
                business,
                facts,
            };
            let bytes = encode(&saved)?;
            ensure_running(execution)?;
            match put_exact(content, context.actor.tenant_id, id, MEDIA, bytes).await {
                Ok(_) => saved,
                Err(
                    error @ (ContentStoreError::Unavailable | ContentStoreError::IdentityConflict),
                ) => read().await?.ok_or_else(|| content_error(error))?,
                Err(error) => return Err(content_error(error)),
            }
        }
    };
    ensure_running(execution)?;
    if saved.prepared_at_ms > context.now.unix_millis() {
        return Err(MemoryToolError::Store);
    }
    Ok(Frozen {
        at: UtcTimestamp::from_unix_millis(saved.prepared_at_ms),
        business: saved.business,
        facts: saved.facts,
    })
}

fn encode<T: Serialize>(value: &Envelope<T>) -> Result<Vec<u8>, MemoryToolError> {
    let bytes = serde_json::to_vec(value).map_err(|_| MemoryToolError::InvalidResult)?;
    if bytes.len() > MAX_BYTES {
        return Err(MemoryToolError::InvalidResult);
    }
    Ok(bytes)
}
