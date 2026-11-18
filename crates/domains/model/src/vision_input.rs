use musteragent_identity::DottedKey;
use thiserror::Error;

use crate::{ModelParameter, ModelRoute};

pub const MAX_LOGICAL_VISION_IMAGES: usize = 64;
pub const MAX_VISION_IMAGE_BYTES: u64 = 20 * 1024 * 1024;
pub const MAX_VISION_BATCH_BYTES: u64 = 50 * 1024 * 1024;

const DEFAULT_MAX_IMAGES_PER_REQUEST: usize = 20;
const MAX_IMAGES_PARAMETER: &str = "vision.max_images_per_request";
const MAX_BYTES_PARAMETER: &str = "vision.max_bytes_per_image";
const MAX_TOTAL_BYTES_PARAMETER: &str = "vision.max_total_image_bytes";

/// Admission-frozen physical image capacity for one provider invocation.
///
/// The Admin Registry remains the source of truth. Runtime consumers resolve
/// only the signed scalar projection carried by the exact `ModelRoute`; they
/// never consult mutable Registry state or infer capacity from a model name.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct VisionInputProfile {
    pub max_images_per_request: usize,
    pub max_bytes_per_image: u64,
    pub max_total_image_bytes: u64,
}

impl VisionInputProfile {
    #[must_use]
    pub const fn defaults() -> Self {
        Self {
            max_images_per_request: DEFAULT_MAX_IMAGES_PER_REQUEST,
            max_bytes_per_image: MAX_VISION_IMAGE_BYTES,
            max_total_image_bytes: MAX_VISION_BATCH_BYTES,
        }
    }

    /// Resolves an all-or-nothing signed profile. A missing profile uses the
    /// immutable Runtime default for legacy routes; a partial or malformed
    /// profile fails closed instead of mixing Admin and Runtime values.
    pub fn from_route(route: &ModelRoute) -> Result<Self, VisionInputProfileError> {
        let images = positive_parameter(route, MAX_IMAGES_PARAMETER)?;
        let per_image = positive_parameter(route, MAX_BYTES_PARAMETER)?;
        let total = positive_parameter(route, MAX_TOTAL_BYTES_PARAMETER)?;
        if images.is_none() && per_image.is_none() && total.is_none() {
            return Ok(Self::defaults());
        }
        if !route
            .capabilities
            .iter()
            .any(|capability| matches!(capability.as_str(), "vision" | "input.image"))
        {
            return Err(VisionInputProfileError::NonVisualRoute);
        }
        let profile = Self {
            max_images_per_request: usize::try_from(
                images.ok_or(VisionInputProfileError::PartialProfile)?,
            )
            .map_err(|_| VisionInputProfileError::OutOfRange)?,
            max_bytes_per_image: per_image.ok_or(VisionInputProfileError::PartialProfile)?,
            max_total_image_bytes: total.ok_or(VisionInputProfileError::PartialProfile)?,
        };
        profile.validate()?;
        Ok(profile)
    }

    pub fn validate(self) -> Result<(), VisionInputProfileError> {
        if !(1..=MAX_LOGICAL_VISION_IMAGES).contains(&self.max_images_per_request)
            || !(1..=MAX_VISION_IMAGE_BYTES).contains(&self.max_bytes_per_image)
            || !(1..=MAX_VISION_BATCH_BYTES).contains(&self.max_total_image_bytes)
            || self.max_total_image_bytes < self.max_bytes_per_image
        {
            return Err(VisionInputProfileError::OutOfRange);
        }
        Ok(())
    }
}

fn positive_parameter(
    route: &ModelRoute,
    name: &str,
) -> Result<Option<u64>, VisionInputProfileError> {
    let key = DottedKey::new(name).map_err(|_| VisionInputProfileError::InvalidParameter)?;
    match route.parameters.get(&key) {
        None => Ok(None),
        Some(ModelParameter::Int(value)) => u64::try_from(*value)
            .ok()
            .filter(|value| *value > 0)
            .map(Some)
            .ok_or(VisionInputProfileError::InvalidParameter),
        Some(ModelParameter::Bool(_) | ModelParameter::Double(_) | ModelParameter::String(_)) => {
            Err(VisionInputProfileError::InvalidParameter)
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
pub enum VisionInputProfileError {
    #[error("vision input profile parameters must be positive integers")]
    InvalidParameter,
    #[error("vision input profile parameters must be supplied together")]
    PartialProfile,
    #[error("vision input profile parameters exceed Runtime safety bounds")]
    OutOfRange,
    #[error("vision input profile cannot be attached to a non-visual route")]
    NonVisualRoute,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    use musteragent_identity::{DottedKey, VersionedKey};

    use crate::{
        ApiFormat, ModelEntryId, ModelParameter, ModelProviderId, ModelRoute, PricingVersionRef,
        ProviderStatePolicy,
    };

    use super::{VisionInputProfile, VisionInputProfileError};

    fn route() -> ModelRoute {
        ModelRoute {
            route_ref: VersionedKey::new("registry.vision@1").expect("route"),
            resolved_model_key: "vision-model".to_owned(),
            model_provider_id: "019fa6de-38fd-7000-8000-000000000211"
                .parse::<ModelProviderId>()
                .expect("provider"),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000212"
                .parse::<ModelEntryId>()
                .expect("entry"),
            api_format: ApiFormat::OpenAiResponses,
            adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
            protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
            context_window: NonZeroU64::new(32_768).expect("context"),
            max_output_tokens: NonZeroU64::new(4_096).expect("output"),
            capabilities: BTreeSet::from([DottedKey::new("vision").expect("vision")]),
            parameters: BTreeMap::new(),
            fallback_route_refs: Vec::new(),
            pricing_version_ref: PricingVersionRef::new("catalog:vision.price@1").expect("pricing"),
            provider_state_policy: ProviderStatePolicy::Stateless,
        }
    }

    #[test]
    fn resolves_only_complete_bounded_signed_profiles() {
        let mut route = route();
        assert_eq!(
            VisionInputProfile::from_route(&route).expect("defaults"),
            VisionInputProfile::defaults()
        );
        route.parameters.insert(
            DottedKey::new("vision.max_images_per_request").expect("parameter"),
            ModelParameter::Int(4),
        );
        assert_eq!(
            VisionInputProfile::from_route(&route),
            Err(VisionInputProfileError::PartialProfile)
        );
        route.parameters.insert(
            DottedKey::new("vision.max_bytes_per_image").expect("parameter"),
            ModelParameter::Int(10 * 1024 * 1024),
        );
        route.parameters.insert(
            DottedKey::new("vision.max_total_image_bytes").expect("parameter"),
            ModelParameter::Int(30 * 1024 * 1024),
        );
        assert_eq!(
            VisionInputProfile::from_route(&route).expect("profile"),
            VisionInputProfile {
                max_images_per_request: 4,
                max_bytes_per_image: 10 * 1024 * 1024,
                max_total_image_bytes: 30 * 1024 * 1024,
            }
        );
    }
}
