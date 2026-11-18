//! Successor file reads restore exact durably authorized media. Text references
//! alone never authorize IO, and a model without the modality cannot imply read.
use super::*;
use musteragent_runtime_model::{InputAttachmentKind, input_attachment_kind};

#[derive(serde::Deserialize)]
struct Output {
    #[serde(default)]
    source_content: Vec<VisionToolContentRef>,
    #[serde(default)]
    visual_content: Vec<VisionToolContentRef>,
}
fn parts(
    text: &str,
    authorized: &[ContentRef],
) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
    let output: Output =
        serde_json::from_str(text).map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
    let mut references = output
        .source_content
        .into_iter()
        .chain(output.visual_content)
        .map(VisionToolContentRef::decode)
        .collect::<Result<Vec<_>, _>>()?;
    if references.len() > 64 || references.len() != authorized.len() {
        return Err(port(ErrorCode::ContentIntegrityFailed, false));
    }
    let mut ids = std::collections::BTreeSet::new();
    references
        .drain(..)
        .map(|reference| {
            if !authorized.contains(&reference) || !ids.insert(reference.content_id) {
                return Err(port(ErrorCode::ContentIntegrityFailed, false));
            }
            Ok(match input_attachment_kind(&reference) {
                InputAttachmentKind::Image => CanonicalContentPart::Image(reference),
                InputAttachmentKind::Audio => CanonicalContentPart::Audio(reference),
                InputAttachmentKind::Video => CanonicalContentPart::Video(reference),
                InputAttachmentKind::File => CanonicalContentPart::File(reference),
                InputAttachmentKind::Text => {
                    return Err(port(ErrorCode::ContentIntegrityFailed, false));
                }
            })
        })
        .collect()
}
impl DurableToolExecutor {
    pub(super) async fn file_input_content(
        &self,
        material: &ToolExecutionMaterial,
        authorized: &[ContentRef],
        text: String,
    ) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
        let active = active_pinned_model(&material.model, material.active_model_route_ordinal)
            .ok_or_else(|| port(ErrorCode::RuntimeSnapshotInvalid, false))?;
        materialize(
            self.content.as_ref(),
            material.run_context.tenant_id,
            &active.route,
            authorized,
            text,
        )
        .await
    }
}
async fn materialize(
    store: &dyn ContentStore,
    tenant: musteragent_identity::TenantId,
    route: &musteragent_model::ModelRoute,
    authorized: &[ContentRef],
    text: String,
) -> Result<Vec<CanonicalContentPart>, LoopPortError> {
    let parts = parts(&text, authorized)?;
    // Check the entire batch before any source IO. No unsupported tail is read.
    for part in &parts {
        let capable = match part {
            CanonicalContentPart::Image(_) => route.accepts_native_image(),
            CanonicalContentPart::Audio(_) => route.accepts_native_audio(),
            CanonicalContentPart::Video(_) => route.accepts_native_video(),
            CanonicalContentPart::File(reference) => {
                route.accepts_native_document(reference.media_type.as_str())
            }
            _ => false,
        };
        if !capable {
            return Err(port(ErrorCode::CapabilityUnavailable, false));
        }
    }
    let mut content = text_content(text)?;
    for part in parts {
        let reference = match &part {
            CanonicalContentPart::Image(reference)
            | CanonicalContentPart::Audio(reference)
            | CanonicalContentPart::Video(reference)
            | CanonicalContentPart::File(reference) => reference,
            _ => return Err(port(ErrorCode::ContentIntegrityFailed, false)),
        };
        let bytes = store
            .read(tenant, reference)
            .await
            .map_err(port_from_content_read)?;
        verify_content(reference, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        content.push(part);
    }
    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_media_requires_same_complete_durable_grant() {
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("audio/wav").unwrap(),
            byte_length: 4,
            sha256: Sha256::digest(b"wave"),
        };
        let value = serde_json::json!({"content_id":reference.content_id.to_string(),"media_type":reference.media_type.as_str(),"byte_length":reference.byte_length,"sha256":reference.sha256.to_string()});
        let text = serde_json::json!({"source_content":[value.clone()]}).to_string();
        assert_eq!(
            parts(&text, std::slice::from_ref(&reference)).unwrap(),
            vec![CanonicalContentPart::Audio(reference.clone())]
        );
        assert!(parts(&text, &[]).is_err());
        let duplicate = serde_json::json!({"source_content":[value.clone(),value]}).to_string();
        assert!(parts(&duplicate, &[reference.clone(), reference.clone()]).is_err());
        let mut changed = reference;
        changed.byte_length += 1;
        assert!(parts(&text, &[changed]).is_err());
    }
    fn route() -> musteragent_model::ModelRoute {
        use musteragent_model::*;
        let id = "11111111-1111-4111-8111-111111111111";
        ModelRoute {
            route_ref: musteragent_identity::VersionedKey::new("fixture.route@1").unwrap(),
            resolved_model_key: "fixture".into(),
            model_provider_id: id.parse().unwrap(),
            model_entry_id: id.parse().unwrap(),
            api_format: ApiFormat::GoogleGenerateContent,
            adapter_version: musteragent_identity::VersionedKey::new("fixture.adapter@1").unwrap(),
            protocol_version: musteragent_identity::VersionedKey::new("fixture.protocol@1")
                .unwrap(),
            context_window: std::num::NonZeroU64::new(10000).unwrap(),
            max_output_tokens: std::num::NonZeroU64::new(1000).unwrap(),
            capabilities: [
                "input.image",
                "input.audio",
                "input.video",
                "input.document",
            ]
            .into_iter()
            .map(|cap| musteragent_identity::DottedKey::new(cap).unwrap())
            .collect(),
            parameters: Default::default(),
            fallback_route_refs: vec![],
            pricing_version_ref: PricingVersionRef::new("catalog:fixture.price@1").unwrap(),
            provider_state_policy: ProviderStatePolicy::Stateless,
        }
    }
    #[tokio::test]
    async fn source_media_reconstruction_checks_route_before_io_and_hash_before_observation() {
        use musteragent_runtime_content::PutContent;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Source {
            reads: AtomicUsize,
            bytes: Vec<u8>,
        }
        impl ContentStore for Source {
            fn put(
                &self,
                _: PutContent,
            ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
                Box::pin(async { panic!("read only") })
            }
            fn read<'a>(
                &'a self,
                _: musteragent_identity::TenantId,
                _: &'a ContentRef,
            ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
                Box::pin(async move {
                    self.reads.fetch_add(1, Ordering::SeqCst);
                    Ok(self.bytes.clone())
                })
            }
        }
        let references = ["image/png", "audio/wav", "video/mp4", "application/pdf"]
            .into_iter()
            .map(|mime| ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(mime).unwrap(),
                byte_length: 4,
                sha256: Sha256::digest(b"data"),
            })
            .collect::<Vec<_>>();
        let text = serde_json::json!({"source_content": references.iter().map(|r| serde_json::json!({"content_id":r.content_id.to_string(), "media_type":r.media_type.as_str(), "byte_length":r.byte_length,"sha256":r.sha256.to_string()})).collect::<Vec<_>>()}).to_string();
        let tenant = "11111111-1111-4111-8111-111111111111".parse().unwrap();
        let store = Source {
            reads: AtomicUsize::new(0),
            bytes: b"data".to_vec(),
        };
        let mut route = route();
        route
            .capabilities
            .remove(&musteragent_identity::DottedKey::new("input.document").unwrap());
        assert!(
            materialize(&store, tenant, &route, &references, text.clone())
                .await
                .is_err()
        );
        assert_eq!(store.reads.load(Ordering::SeqCst), 0);
        route
            .capabilities
            .insert(musteragent_identity::DottedKey::new("input.document").unwrap());
        let output = materialize(&store, tenant, &route, &references, text.clone())
            .await
            .unwrap();
        assert!(matches!(
            output.as_slice(),
            [
                CanonicalContentPart::Text(_),
                CanonicalContentPart::Image(_),
                CanonicalContentPart::Audio(_),
                CanonicalContentPart::Video(_),
                CanonicalContentPart::File(_)
            ]
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), 4);
        let bad = Source {
            reads: AtomicUsize::new(0),
            bytes: b"oops".to_vec(),
        };
        assert!(
            materialize(&bad, tenant, &route, &references, text)
                .await
                .is_err()
        );
        assert_eq!(bad.reads.load(Ordering::SeqCst), 1);
    }
}
