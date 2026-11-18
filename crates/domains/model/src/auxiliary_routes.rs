//! Signed auxiliary entries preserve role order and each physical route's own
//! identity and credential. A single historical entry retains its semantics.
use super::{AuxiliaryModelRole, CredentialGrantRef, ModelRoute, VisionInputProfile};

/// A role has one primary and at most three explicitly configured alternatives.
pub const MAX_AUXILIARY_MODEL_CANDIDATES: usize = 4;

pub fn valid_auxiliary_model_routes<'a>(
    routes: impl IntoIterator<Item = (AuxiliaryModelRole, &'a ModelRoute, &'a CredentialGrantRef)>,
) -> bool {
    let mut previous = None;
    let mut credentials = std::collections::BTreeSet::new();
    for (role, route, credential) in routes {
        if let Some((previous_role, previous_route)) = previous {
            if role < previous_role {
                return false;
            }
            if role == previous_role {
                if credentials.len() >= MAX_AUXILIARY_MODEL_CANDIDATES
                    || credentials.contains(credential)
                    || !qualified_candidate(role, previous_route)
                    || !qualified_candidate(role, route)
                {
                    return false;
                }
            } else {
                credentials.clear();
            }
        }
        credentials.insert(credential);
        previous = Some((role, route));
    }
    true
}

fn qualified_candidate(role: AuxiliaryModelRole, route: &ModelRoute) -> bool {
    matches!(
        role,
        AuxiliaryModelRole::Compression | AuxiliaryModelRole::Vision
    ) && route.route_ref.as_str()
        == format!(
            "registry.aux.{}.id-{}@1",
            role.as_str(),
            route.model_entry_id
        )
        && !route.api_format.is_decision()
        && route
            .capabilities
            .iter()
            .any(|key| key.as_str() == "streaming")
        && (role != AuxiliaryModelRole::Vision
            || (route
                .capabilities
                .iter()
                .any(|key| key.as_str() == "vision")
                && route
                    .capabilities
                    .iter()
                    .any(|key| key.as_str() == "input.image")
                && VisionInputProfile::from_route(route).is_ok()))
}
