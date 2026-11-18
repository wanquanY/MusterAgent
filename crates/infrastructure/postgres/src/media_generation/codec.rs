//! Bounded completion metadata; asset bytes are fetched through the provider.
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::ContentId;
use musteragent_runtime_artifact::{
    BackendMediaAssetProof, BackendMediaSourceKind, BackendMediaSourceProof, MediaJournalError,
};
use musteragent_runtime_contract::OpaqueExternalRef;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proof {
    kind: String,
    source_id: uuid::Uuid,
    task_id: String,
    title: String,
    prompt: String,
    asset_urls: Vec<String>,
    assets: Vec<Asset>,
    thumbnail_url: Option<String>,
    provider_request_ref: Option<String>,
    failure_code: Option<String>,
    provider_model: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Asset {
    id: uuid::Uuid,
    media: String,
    length: u64,
    hash: String,
    delivery_url: Option<String>,
    dimensions: Option<(u32, u32)>,
}
pub(super) fn encode(s: &BackendMediaSourceProof) -> Result<Vec<u8>, MediaJournalError> {
    if s.failure_code.is_some()
        || (s.assets.is_empty() && s.asset_urls.is_empty())
        || s.source_id.is_nil()
        || s.task_id.is_empty()
        || s.task_id.len() > 512
        || s.assets.len() > 16
        || s.asset_urls.len() > 16
        || s.assets.iter().any(|a| !a.content.is_empty())
    {
        return Err(MediaJournalError::Invalid);
    }
    let p = Proof {
        kind: match s.kind {
            BackendMediaSourceKind::ImageGenerationRecord => "image",
            BackendMediaSourceKind::VideoGenerationRecord => "video",
        }
        .into(),
        source_id: s.source_id,
        task_id: s.task_id.clone(),
        title: s.title.clone(),
        prompt: s.prompt.clone(),
        asset_urls: s.asset_urls.clone(),
        thumbnail_url: s.thumbnail_url.clone(),
        provider_request_ref: s.provider_request_ref.as_ref().map(|r| r.as_str().into()),
        failure_code: s.failure_code.clone(),
        provider_model: s.provider_model.clone(),
        assets: s
            .assets
            .iter()
            .map(|a| Asset {
                id: a.content_ref.content_id.into_uuid(),
                media: a.content_ref.media_type.as_str().into(),
                length: a.content_ref.byte_length,
                hash: a.content_ref.sha256.to_string(),
                delivery_url: a.delivery_url.clone(),
                dimensions: a.dimensions,
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&p).map_err(|_| MediaJournalError::Invalid)?;
    if bytes.len() > 1024 * 1024 {
        return Err(MediaJournalError::Invalid);
    }
    Ok(bytes)
}
pub(super) fn decode(bytes: &[u8]) -> Result<BackendMediaSourceProof, MediaJournalError> {
    if bytes.len() > 1024 * 1024 {
        return Err(MediaJournalError::Invalid);
    }
    let p: Proof = serde_json::from_slice(bytes).map_err(|_| MediaJournalError::Invalid)?;
    let source = BackendMediaSourceProof {
        kind: match p.kind.as_str() {
            "image" => BackendMediaSourceKind::ImageGenerationRecord,
            "video" => BackendMediaSourceKind::VideoGenerationRecord,
            _ => return Err(MediaJournalError::Invalid),
        },
        source_id: p.source_id,
        task_id: p.task_id,
        title: p.title,
        prompt: p.prompt,
        asset_urls: p.asset_urls,
        assets: p
            .assets
            .into_iter()
            .map(|a| {
                Ok(BackendMediaAssetProof {
                    content_ref: ContentRef {
                        content_id: ContentId::from_uuid(a.id)
                            .map_err(|_| MediaJournalError::Invalid)?,
                        media_type: MediaType::new(a.media)
                            .map_err(|_| MediaJournalError::Invalid)?,
                        byte_length: a.length,
                        sha256: Sha256::from_hex(&a.hash)
                            .map_err(|_| MediaJournalError::Invalid)?,
                    },
                    content: vec![],
                    delivery_url: a.delivery_url,
                    dimensions: a.dimensions,
                })
            })
            .collect::<Result<_, MediaJournalError>>()?,
        thumbnail_url: p.thumbnail_url,
        provider_request_ref: p
            .provider_request_ref
            .map(OpaqueExternalRef::new)
            .transpose()
            .map_err(|_| MediaJournalError::Invalid)?,
        failure_code: p.failure_code,
        provider_model: p.provider_model,
    };
    encode(&source)?;
    Ok(source)
}
