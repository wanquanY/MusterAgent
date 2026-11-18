//! A model selects immutable content; the current execution supplies its authority.
//! No global alias catalog, latest-version lookup, or model-supplied integrity facts.
use musteragent_content_ref::ContentRef;
use musteragent_identity::ContentId;
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ModelReferenceError {
    #[error("content reference must be an ID returned by a successful tool or attachment")]
    Invalid,
    #[error("content reference is unavailable in this task; read or import the content again")]
    Unavailable,
    #[error("authorized content identity is inconsistent")]
    Inconsistent,
}

/// Exact resolution inside a caller-owned permission closure. The caller must
/// still enforce media type, purpose and byte integrity at the domain boundary.
pub fn resolve<'a>(
    value: &Value,
    authorized: impl IntoIterator<Item = &'a ContentRef>,
) -> Result<ContentRef, ModelReferenceError> {
    let id: ContentId = value
        .as_str()
        .ok_or(ModelReferenceError::Invalid)?
        .parse()
        .map_err(|_| ModelReferenceError::Invalid)?;
    let mut found = None;
    for reference in authorized {
        if reference.content_id != id {
            continue;
        }
        if found.is_some_and(|prior| prior != reference) {
            return Err(ModelReferenceError::Inconsistent);
        }
        found = Some(reference);
    }
    found.cloned().ok_or(ModelReferenceError::Unavailable)
}

pub fn model_value(reference: &ContentRef) -> Value {
    json!(reference.content_id.to_string())
}

/// Runtime-only JSON for existing typed/native commands. This is never an
/// invitation for the model to compute or repair the stored digest.
pub fn execution_value(reference: &ContentRef) -> Value {
    json!({"content_id":reference.content_id.to_string(),
        "media_type":reference.media_type.as_str(),"byte_length":reference.byte_length,
        "sha256":reference.sha256.to_string()})
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{MediaType, Sha256};

    fn reference() -> ContentRef {
        ContentRef {
            content_id: "01982f80-0000-7000-8000-000000000001"
                .parse()
                .expect("valid fixture for reference"),
            media_type: MediaType::new("image/png").expect("valid fixture for reference"),
            byte_length: 3,
            sha256: Sha256::digest(b"png"),
        }
    }

    #[test]
    fn id_is_not_authority_and_conflicting_identity_is_rejected() {
        let r = reference();
        let id = model_value(&r);
        assert_eq!(resolve(&id, []), Err(ModelReferenceError::Unavailable));
        assert_eq!(resolve(&id, [&r]), Ok(r.clone()));
        assert_eq!(resolve(&id, [&r, &r]), Ok(r.clone()));
        let mut forged = r.clone();
        forged.sha256 = Sha256::digest(b"other");
        assert_eq!(
            resolve(&id, [&r, &forged]),
            Err(ModelReferenceError::Inconsistent)
        );
        assert_eq!(
            resolve(&execution_value(&forged), [&r]),
            Err(ModelReferenceError::Invalid)
        );
    }
}
