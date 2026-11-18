use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use musteragent_identity::{InvocationId, RunId, UtcTimestamp, VersionedKey};

use crate::AuthorizedSkillCatalog;
use crate::model::{SkillDisclosed, SkillDisclosureItem, SkillDiscoveryContractError};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosureRequest {
    pub skill_ref: VersionedKey,
    pub source_invocation_id: InvocationId,
    pub disclosed_at: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosureOutcome {
    pub event: Option<SkillDisclosed>,
    pub ordinal: NonZeroU32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillDisclosureState {
    ordered: Vec<SkillDisclosureItem>,
    ordinals: BTreeMap<VersionedKey, NonZeroU32>,
}

impl SkillDisclosureState {
    pub fn from_initial(
        catalog: &AuthorizedSkillCatalog,
        initial: &[VersionedKey],
    ) -> Result<Self, SkillDiscoveryContractError> {
        let mut ordered = Vec::with_capacity(initial.len());
        let mut ordinals = BTreeMap::new();
        for (index, skill_ref) in initial.iter().enumerate() {
            let skill = catalog
                .get(skill_ref.as_str())
                .ok_or(SkillDiscoveryContractError::InvalidInitialSkillDisclosure)?;
            let disclosure_ordinal = ordinal(index.saturating_add(1))?;
            if ordinals
                .insert(skill_ref.clone(), disclosure_ordinal)
                .is_some()
            {
                return Err(SkillDiscoveryContractError::InvalidInitialSkillDisclosure);
            }
            ordered.push(SkillDisclosureItem {
                skill_ref: skill_ref.clone(),
                instructions_ref: skill.instructions_ref.clone(),
                instructions_hash: skill.instructions_hash,
                disclosure_ordinal,
            });
        }
        Ok(Self { ordered, ordinals })
    }

    pub fn from_projection(
        catalog: &AuthorizedSkillCatalog,
        initial: &[VersionedKey],
        projected: &[SkillDisclosed],
    ) -> Result<Self, SkillDiscoveryContractError> {
        let mut state = Self::from_initial(catalog, initial)?;
        for event in projected {
            state.apply(catalog, event)?;
        }
        Ok(state)
    }

    #[must_use]
    pub fn items(&self) -> &[SkillDisclosureItem] {
        &self.ordered
    }

    #[must_use]
    pub fn ordinal_for(&self, skill_ref: &VersionedKey) -> Option<NonZeroU32> {
        self.ordinals.get(skill_ref).copied()
    }

    pub fn apply(
        &mut self,
        catalog: &AuthorizedSkillCatalog,
        event: &SkillDisclosed,
    ) -> Result<(), SkillDiscoveryContractError> {
        let skill = catalog
            .get(event.skill_ref.as_str())
            .ok_or(SkillDiscoveryContractError::InvalidSkillDisclosure)?;
        let expected = self.ordered.len().saturating_add(1);
        if event.instructions_ref != skill.instructions_ref
            || event.instructions_hash != skill.instructions_hash
            || usize::try_from(event.disclosure_ordinal.get()).ok() != Some(expected)
            || self.ordinals.contains_key(&event.skill_ref)
        {
            return Err(SkillDiscoveryContractError::InvalidSkillDisclosure);
        }
        self.ordinals
            .insert(event.skill_ref.clone(), event.disclosure_ordinal);
        self.ordered.push(SkillDisclosureItem {
            skill_ref: event.skill_ref.clone(),
            instructions_ref: event.instructions_ref.clone(),
            instructions_hash: event.instructions_hash,
            disclosure_ordinal: event.disclosure_ordinal,
        });
        Ok(())
    }
}

impl AuthorizedSkillCatalog {
    pub fn disclose(
        &self,
        state: &SkillDisclosureState,
        request: SkillDisclosureRequest,
    ) -> Result<SkillDisclosureOutcome, SkillDiscoveryContractError> {
        let skill = self.executable(request.skill_ref.as_str())?;
        if let Some(existing) = state.ordinal_for(&request.skill_ref) {
            return Ok(SkillDisclosureOutcome {
                event: None,
                ordinal: existing,
            });
        }
        let disclosure_ordinal = ordinal(state.items().len().saturating_add(1))?;
        Ok(SkillDisclosureOutcome {
            event: Some(SkillDisclosed {
                skill_ref: skill.skill_ref.clone(),
                instructions_ref: skill.instructions_ref.clone(),
                instructions_hash: skill.instructions_hash,
                disclosure_ordinal,
                source_invocation_id: request.source_invocation_id,
                disclosed_at: request.disclosed_at,
            }),
            ordinal: disclosure_ordinal,
        })
    }
}

pub fn validate_prior_skill_disclosures(
    source_run_id: RunId,
    prior: &[crate::PriorSkillDisclosure],
) -> Result<Vec<crate::PriorSkillDisclosure>, SkillDiscoveryContractError> {
    let mut canonical = prior.to_vec();
    canonical.sort_by_key(|value| value.disclosure_ordinal);
    if canonical != prior {
        return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
    }
    let mut refs = BTreeSet::new();
    let mut versions = BTreeSet::new();
    let mut ledger_records = BTreeSet::new();
    let mut admission_binding = None;
    let mut ledger_disclosures_started = false;
    for (index, skill) in canonical.iter().enumerate() {
        let expected = u32::try_from(index.saturating_add(1))
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(SkillDiscoveryContractError::InvalidPriorSkillDisclosure)?;
        match skill.provenance {
            crate::PriorSkillDisclosureProvenance::AdmissionSnapshot {
                ledger_record_id,
                runtime_snapshot_id,
            } => {
                if ledger_disclosures_started
                    || admission_binding
                        .is_some_and(|binding| binding != (ledger_record_id, runtime_snapshot_id))
                {
                    return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
                }
                admission_binding = Some((ledger_record_id, runtime_snapshot_id));
            }
            crate::PriorSkillDisclosureProvenance::LedgerDisclosure {
                ledger_record_id, ..
            } => {
                ledger_disclosures_started = true;
                if !ledger_records.insert(ledger_record_id) {
                    return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
                }
            }
            crate::PriorSkillDisclosureProvenance::Untrusted => {
                return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
            }
        }
        if skill.source_run_id != source_run_id
            || skill.disclosure_ordinal != expected
            || skill.instructions_ref.sha256 != skill.instructions_hash
            || !refs.insert(skill.skill_ref.clone())
            || !versions.insert(skill.skill_version_id)
        {
            return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
        }
    }
    if !canonical.is_empty() && admission_binding.is_none() {
        return Err(SkillDiscoveryContractError::InvalidPriorSkillDisclosure);
    }
    Ok(canonical)
}

fn ordinal(value: usize) -> Result<NonZeroU32, SkillDiscoveryContractError> {
    u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(SkillDiscoveryContractError::InvalidSkillDisclosure)
}
