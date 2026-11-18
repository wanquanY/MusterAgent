//! A native screenshot is visible only if the same durable tool completion
//! authorized that exact immutable content. A reference inside arbitrary text
//! cannot grant access to another tenant/run's stored image.
use super::*;
#[derive(serde::Deserialize)]
struct AuthorizedImages {
    #[serde(default)]
    image_refs: Vec<VisionToolContentRef>,
}
pub(super) fn from_tool_result(
    text: &str,
    authorized: &[ContentRef],
) -> Result<Vec<ContentRef>, LoopPortError> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Ok(Vec::new());
    };
    if value.get("image_refs").is_none() {
        return Ok(Vec::new());
    }
    authorized_images(value, authorized)
}
#[cfg(test)]
pub(super) fn authorized(
    text: &str,
    authorized: &[ContentRef],
) -> Result<Vec<ContentRef>, LoopPortError> {
    authorized_images(
        serde_json::from_str(text).map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?,
        authorized,
    )
}
fn authorized_images(
    value: serde_json::Value,
    authorized: &[ContentRef],
) -> Result<Vec<ContentRef>, LoopPortError> {
    let output: AuthorizedImages = serde_json::from_value(value)
        .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
    let images = output
        .image_refs
        .into_iter()
        .map(VisionToolContentRef::decode)
        .collect::<Result<Vec<_>, _>>()?;
    if images.iter().any(|image| {
        !authorized.contains(image)
            || !matches!(
                image.media_type.as_str(),
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            )
    }) {
        return Err(port(ErrorCode::ContentIntegrityFailed, false));
    }
    // Bound only inline vision context. All original refs remain in the full
    // persisted tool result and can be inspected through the content lane.
    Ok(images.into_iter().take(4).collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn any_tool_can_supply_durably_authorized_images_without_losing_the_full_result() {
        let refs = (0..6)
            .map(|i| ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("image/webp").expect("type"),
                byte_length: 1,
                sha256: Sha256::digest([i]),
            })
            .collect::<Vec<_>>();
        let text = serde_json::json!({"image_refs": refs.iter().map(|r| serde_json::json!({
            "content_id":r.content_id.to_string(),"media_type":r.media_type.as_str(),"byte_length":r.byte_length,"sha256":r.sha256.to_string()
        })).collect::<Vec<_>>()}).to_string();
        assert_eq!(
            from_tool_result(&text, &refs).expect("authorized"),
            refs[..4]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).expect("original")["image_refs"]
                .as_array()
                .expect("refs")
                .len(),
            6
        );
        assert!(
            from_tool_result(&text, &refs[..4]).is_err(),
            "even non-inline images require exact authorization"
        );
        assert!(
            from_tool_result("plain text", &[])
                .expect("ordinary tool")
                .is_empty()
        );
        assert!(
            from_tool_result(r#"{"data":"no images"}"#, &[])
                .expect("ordinary tool")
                .is_empty()
        );
    }
    #[test]
    fn a_shared_result_cannot_use_an_image_reference_as_its_own_authorization() {
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("image/png").expect("type"),
            byte_length: 12,
            sha256: Sha256::digest(b"image"),
        };
        let text = serde_json::json!({"operation":"screen", "image_refs":[{
            "content_id":reference.content_id.to_string(), "media_type":"image/png", "byte_length":12, "sha256":reference.sha256.to_string()
        }]}).to_string();
        assert!(authorized(&text, &[]).is_err());
        assert_eq!(
            authorized(&text, std::slice::from_ref(&reference)).expect("same durable completion"),
            vec![reference.clone()]
        );
        let mut changed = reference;
        changed.content_id = ContentId::generate();
        assert!(authorized(&text, &[changed]).is_err());
        assert!(
            authorized(r#"{"operation":"release","released":true}"#, &[])
                .expect("no images")
                .is_empty()
        );
    }
}
