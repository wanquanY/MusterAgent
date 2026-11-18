//! Exact content already admitted to this Run, independent of file placement.
//! Provider input is data; knowing a tenant content UUID never grants access.
use super::*;
use musteragent_content_ref::ContentRef;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ContentReadInput {
    pub content_id: String,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "read_budget")]
    pub max_bytes: usize,
}

pub(super) async fn read(
    store: &dyn ContentStore,
    invocation: &CapabilityInvocation,
    material: &ToolExecutionMaterial,
    input: ContentReadInput,
) -> Result<serde_json::Value, LoopPortError> {
    if !(4096..=65536).contains(&input.max_bytes) {
        return Err(port(ErrorCode::InvalidCommand, false));
    }
    let reference = select(
        &input,
        &material.authorized_content,
        &material.prior_tool_results,
    )?;
    if input.offset as u64 > reference.byte_length {
        return Err(port(ErrorCode::InvalidCommand, false));
    }
    // Reuse the Content owner's verified random-access reader. An admitted
    // large text must not become a whole-file allocation for every page.
    let reader = store
        .open_verified(invocation.tenant_id, reference)
        .await
        .map_err(|error| match error {
            musteragent_runtime_content::ContentStoreError::HashMismatch
            | musteragent_runtime_content::ContentStoreError::LengthMismatch => {
                port(ErrorCode::ContentIntegrityFailed, false)
            }
            _ => port(ErrorCode::ContentUnavailable, false),
        })?;
    let offset = input.offset as u64;
    let budget = input.max_bytes / 6;
    let text = musteragent_runtime_content::read_verified_text_page(reader, offset, budget)
        .await
        .map_err(|error| match error {
            musteragent_runtime_content::ContentTextError::Invalid => {
                port(ErrorCode::InvalidCommand, false)
            }
            musteragent_runtime_content::ContentTextError::Content(_) => {
                port(ErrorCode::ContentIntegrityFailed, false)
            }
        })?;
    let end = input.offset + text.len();
    Ok(serde_json::json!({
        "content_id": reference.content_id.to_string(),
        "media_type": reference.media_type.as_str(),
        "sha256": reference.sha256.to_string(),
        "byte_length": reference.byte_length,
        "text": text,
        "offset": input.offset,
        "next_offset": ((end as u64) < reference.byte_length).then_some(end),
        "complete": end as u64 == reference.byte_length,
    }))
}

fn select<'a>(
    input: &ContentReadInput,
    authorized: &'a [ContentRef],
    results: &'a [ContentRef],
) -> Result<&'a ContentRef, LoopPortError> {
    let content_id = input
        .content_id
        .parse::<musteragent_identity::ContentId>()
        .map_err(|_| port(ErrorCode::InvalidCommand, false))?;
    let mut candidates = authorized
        .iter()
        .chain(results)
        .filter(|r| r.content_id == content_id);
    let reference = candidates
        .next()
        .ok_or_else(|| port(ErrorCode::PolicyDenied, false))?;
    if candidates.any(|other| other != reference) {
        return Err(port(ErrorCode::ContentIntegrityFailed, false));
    }
    Ok(reference)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::Sha256;
    use musteragent_identity::ContentId;
    #[test]
    fn reads_only_exact_admitted_input_or_original_tool_result() {
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/plain").unwrap(),
            byte_length: 3,
            sha256: Sha256::digest(b"abc"),
        };
        let input = ContentReadInput {
            content_id: reference.content_id.to_string(),
            offset: 0,
            max_bytes: 4096,
        };
        assert_eq!(
            select(&input, std::slice::from_ref(&reference), &[]).unwrap(),
            &reference
        );
        assert_eq!(
            select(&input, &[], std::slice::from_ref(&reference)).unwrap(),
            &reference
        );
        assert_eq!(
            select(&input, &[], &[]).unwrap_err().code,
            ErrorCode::PolicyDenied
        );
        let mut forged = reference.clone();
        forged.sha256 = Sha256::digest(b"xyz");
        assert_eq!(
            select(&input, &[reference, forged], &[]).unwrap_err().code,
            ErrorCode::ContentIntegrityFailed
        );
    }
}
