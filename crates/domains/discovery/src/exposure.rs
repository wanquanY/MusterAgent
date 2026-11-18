use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use musteragent_capability::CapabilityLocation;
use musteragent_content_ref::ContentRef;
use musteragent_identity::{InvocationId, UtcTimestamp};
use musteragent_policy::RequiredContext;
use musteragent_tool::ToolKey;

use crate::AuthorizedToolCatalog;
use crate::model::{
    AuthorizedToolDescriptor, DiscoveryContractError, ToolExposureAdded, ToolExposureItem,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActivationConstraints {
    pub available_locations: BTreeSet<CapabilityLocation>,
    pub available_contexts: BTreeSet<RequiredContext>,
    pub max_exposed_tools: NonZeroU32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActivationRequest {
    pub tool_refs: Vec<ToolKey>,
    pub reason_ref: ContentRef,
    pub source_invocation_id: InvocationId,
    pub exposed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ActivationOutcome {
    pub event: Option<ToolExposureAdded>,
    pub ordinals: Vec<NonZeroU32>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExposureState {
    ordered: Vec<ToolExposureItem>,
    ordinals: BTreeMap<ToolKey, NonZeroU32>,
}

impl ExposureState {
    pub fn from_initial(
        catalog: &AuthorizedToolCatalog,
        initial: &[ToolKey],
    ) -> Result<Self, DiscoveryContractError> {
        let mut ordered = Vec::with_capacity(initial.len());
        let mut ordinals = BTreeMap::new();
        for (index, tool_ref) in initial.iter().enumerate() {
            let definition = catalog
                .get_by_key(tool_ref.as_str())
                .ok_or(DiscoveryContractError::InvalidInitialExposure)?;
            let ordinal = ordinal(index.saturating_add(1))?;
            if ordinals.insert(tool_ref.clone(), ordinal).is_some() {
                return Err(DiscoveryContractError::InvalidInitialExposure);
            }
            ordered.push(ToolExposureItem {
                tool_ref: tool_ref.clone(),
                schema_hash: definition.schema_hash,
                ordinal,
            });
        }
        Ok(Self { ordered, ordinals })
    }

    /// Rebuilds the canonical exposure state from an admission-sealed prefix
    /// and its durable ordered projection. Projection rows are treated as
    /// persisted domain facts and therefore must be contiguous, authorized,
    /// unique and schema-exact.
    pub fn from_projection(
        catalog: &AuthorizedToolCatalog,
        initial: &[ToolKey],
        projected: &[ToolExposureItem],
    ) -> Result<Self, DiscoveryContractError> {
        let mut state = Self::from_initial(catalog, initial)?;
        state.apply_items(catalog, projected)?;
        Ok(state)
    }

    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.ordinals.keys().any(|tool| tool.as_str() == key)
    }

    #[must_use]
    pub fn ordinal_for(&self, tool_ref: &ToolKey) -> Option<NonZeroU32> {
        self.ordinals.get(tool_ref).copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ordered.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }

    #[must_use]
    pub fn items(&self) -> &[ToolExposureItem] {
        &self.ordered
    }

    pub fn apply(
        &mut self,
        catalog: &AuthorizedToolCatalog,
        event: &ToolExposureAdded,
    ) -> Result<(), DiscoveryContractError> {
        if event.exposures.is_empty() {
            return Err(DiscoveryContractError::InvalidExposureEvent);
        }
        self.apply_items(catalog, &event.exposures)
    }

    fn apply_items(
        &mut self,
        catalog: &AuthorizedToolCatalog,
        exposures: &[ToolExposureItem],
    ) -> Result<(), DiscoveryContractError> {
        let mut expected = self.ordered.len().saturating_add(1);
        let mut event_keys = BTreeSet::new();
        for exposure in exposures {
            let definition = catalog
                .get_by_key(exposure.tool_ref.as_str())
                .ok_or(DiscoveryContractError::InvalidExposureEvent)?;
            if exposure.schema_hash != definition.schema_hash
                || usize::try_from(exposure.ordinal.get()).ok() != Some(expected)
                || self.ordinals.contains_key(&exposure.tool_ref)
                || !event_keys.insert(exposure.tool_ref.clone())
            {
                return Err(DiscoveryContractError::InvalidExposureEvent);
            }
            expected = expected.saturating_add(1);
        }
        for exposure in exposures {
            self.ordinals
                .insert(exposure.tool_ref.clone(), exposure.ordinal);
            self.ordered.push(exposure.clone());
        }
        Ok(())
    }

    #[must_use]
    pub fn project(&self, catalog: &AuthorizedToolCatalog) -> Vec<AuthorizedToolDescriptor> {
        self.ordered
            .iter()
            .filter_map(|exposure| catalog.get_by_key(exposure.tool_ref.as_str()).cloned())
            .collect()
    }
}

impl AuthorizedToolCatalog {
    pub fn activate(
        &self,
        state: &ExposureState,
        request: ActivationRequest,
        constraints: &ActivationConstraints,
    ) -> Result<ActivationOutcome, DiscoveryContractError> {
        let unique = request.tool_refs.iter().cloned().collect::<BTreeSet<_>>();
        if request.tool_refs.is_empty() || unique.len() != request.tool_refs.len() {
            return Err(DiscoveryContractError::DuplicateActivationRequest);
        }
        let mut ordinals = Vec::with_capacity(request.tool_refs.len());
        let mut new_exposures = Vec::new();
        let mut next = state.len().saturating_add(1);
        for tool_ref in request.tool_refs {
            if let Some(existing) = state.ordinal_for(&tool_ref) {
                ordinals.push(existing);
                continue;
            }
            let definition = self
                .get_by_key(tool_ref.as_str())
                .ok_or(DiscoveryContractError::ToolNotAuthorized)?;
            if !constraints
                .available_locations
                .contains(&definition.location)
            {
                return Err(DiscoveryContractError::LocationUnavailable);
            }
            if !definition
                .required_contexts
                .is_subset(&constraints.available_contexts)
            {
                return Err(DiscoveryContractError::RequiredContextUnavailable);
            }
            if next > usize::try_from(constraints.max_exposed_tools.get()).unwrap_or(usize::MAX) {
                return Err(DiscoveryContractError::ExposureLimitExceeded);
            }
            let exposure_ordinal = ordinal(next)?;
            ordinals.push(exposure_ordinal);
            new_exposures.push(ToolExposureItem {
                tool_ref,
                schema_hash: definition.schema_hash,
                ordinal: exposure_ordinal,
            });
            next = next.saturating_add(1);
        }
        let event = (!new_exposures.is_empty()).then_some(ToolExposureAdded {
            exposures: new_exposures,
            reason_ref: request.reason_ref,
            source_invocation_id: request.source_invocation_id,
            exposed_at: request.exposed_at,
        });
        Ok(ActivationOutcome { event, ordinals })
    }
}

fn ordinal(value: usize) -> Result<NonZeroU32, DiscoveryContractError> {
    u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(DiscoveryContractError::ExposureLimitExceeded)
}
