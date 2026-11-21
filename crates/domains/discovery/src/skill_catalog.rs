use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use musteragent_content_ref::Sha256;

use crate::model::{
    AuthorizedSkillDescriptor, SkillAvailability, SkillCatalogActivation, SkillCatalogInstallScope,
    SkillCatalogLifecycle, SkillCatalogProvenance, SkillCatalogReference,
    SkillCatalogReferenceKind, SkillCatalogSummary, SkillCatalogTrust, SkillDiscoveryContractError,
    SkillOrigin, bounded_limit,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogSearchRequest {
    pub query: String,
    pub limit: NonZeroUsize,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogListRequest {
    pub limit: NonZeroUsize,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SkillCatalogPage {
    pub items: Vec<SkillCatalogSummary>,
    pub next_cursor: Option<String>,
}

/// Canonically ordered catalog sealed for one Run. Construction is the Skill
/// validation boundary; trusted publication sources authorize its entries before
/// sealing. Search/list/view can never add an entry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorizedSkillCatalog {
    definitions: Vec<AuthorizedSkillDescriptor>,
    search_index: Vec<crate::search::DocumentSearchIndex>,
    by_ref: BTreeMap<String, usize>,
    catalog_hash: Sha256,
    model_names: crate::ModelSkillNames,
}

impl AuthorizedSkillCatalog {
    pub fn new(
        mut definitions: Vec<AuthorizedSkillDescriptor>,
    ) -> Result<Self, SkillDiscoveryContractError> {
        definitions.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
        let mut by_ref = BTreeMap::new();
        let mut version_ids = BTreeSet::new();
        for (index, skill) in definitions.iter_mut().enumerate() {
            skill.references.sort_by(reference_order);
            skill.references.dedup();
            let unavailable_is_valid = match &skill.availability {
                SkillAvailability::Available => true,
                SkillAvailability::Unavailable {
                    missing_required_tools,
                } => {
                    !missing_required_tools.is_empty()
                        && missing_required_tools.is_subset(&skill.required_tools)
                }
            };
            if skill.title.trim().is_empty()
                || skill.description.trim().is_empty()
                || skill.when_to_use.trim().is_empty()
                || skill.instructions_ref.byte_length == 0
                || skill.instructions_ref.sha256 != skill.instructions_hash
                || skill
                    .required_tools
                    .intersection(&skill.optional_tools)
                    .next()
                    .is_some()
                || skill.compatible_agent_contracts.is_empty()
                || skill.compatible_surfaces.is_empty()
                || skill.compatible_surfaces.iter().any(|surface| {
                    !matches!(
                        surface.as_str(),
                        "web" | "studio" | "api" | "desktop" | "channel" | "mobile"
                    )
                })
                || skill.lifecycle != SkillCatalogLifecycle::Active
                || skill.spec_hash == Sha256::digest([])
                || skill.references.is_empty()
                || !skill.references.iter().any(|reference| {
                    reference.kind == SkillCatalogReferenceKind::Instructions
                        && reference.content_ref == skill.instructions_ref
                })
                || (!matches!(skill.origin, SkillOrigin::Bundled)
                    && !skill.references.iter().any(|reference| {
                        reference.kind == SkillCatalogReferenceKind::Manifest
                            && reference.content_ref.sha256 == skill.spec_hash
                    }))
                || (!matches!(skill.origin, SkillOrigin::Bundled)
                    && !skill.references.iter().any(|reference| {
                        reference.kind == SkillCatalogReferenceKind::Package
                            && reference.content_ref.byte_length > 0
                            && reference.content_ref.sha256 != Sha256::digest([])
                    }))
                || !authority_is_closed(skill)
                || !unavailable_is_valid
            {
                return Err(SkillDiscoveryContractError::InvalidSkillDescriptor);
            }
            if by_ref
                .insert(skill.skill_ref.as_str().to_owned(), index)
                .is_some()
                || !version_ids.insert(skill.skill_version_id)
            {
                return Err(SkillDiscoveryContractError::DuplicateSkill);
            }
        }
        let catalog_hash = hash_definitions(&definitions);
        let model_names =
            crate::ModelSkillNames::new(definitions.iter().map(|skill| &skill.skill_ref))
                .map_err(|_| SkillDiscoveryContractError::InvalidSkillDescriptor)?;
        let search_index = definitions
            .iter()
            .map(|skill| {
                let model_name = model_names
                    .name(&skill.skill_ref)
                    .unwrap_or(skill.skill_ref.logical_key());
                crate::search::DocumentSearchIndex::new(
                    [
                        skill.skill_ref.as_str(),
                        skill.skill_ref.logical_key(),
                        model_name,
                        skill.title.as_str(),
                    ],
                    skill.tags.iter().map(String::as_str),
                    Some(skill.when_to_use.as_str()),
                    skill.description.as_str(),
                )
            })
            .collect();
        Ok(Self {
            definitions,
            search_index,
            by_ref,
            catalog_hash,
            model_names,
        })
    }

    #[must_use]
    pub const fn catalog_hash(&self) -> Sha256 {
        self.catalog_hash
    }

    #[must_use]
    pub fn definitions(&self) -> &[AuthorizedSkillDescriptor] {
        &self.definitions
    }

    pub fn model_names(&self) -> &crate::ModelSkillNames {
        &self.model_names
    }

    #[must_use]
    pub fn get(&self, skill_ref: &str) -> Option<&AuthorizedSkillDescriptor> {
        self.by_ref
            .get(skill_ref)
            .and_then(|index| self.definitions.get(*index))
    }

    /// New execution only. Historical catalog reads and projection replay use
    /// `get` so availability never rewrites a persisted receipt.
    pub fn executable(
        &self,
        skill_ref: &str,
    ) -> Result<&AuthorizedSkillDescriptor, SkillDiscoveryContractError> {
        let skill = self
            .get(skill_ref)
            .ok_or(SkillDiscoveryContractError::SkillNotAuthorized)?;
        if !skill.availability.is_available() {
            return Err(SkillDiscoveryContractError::SkillUnavailable);
        }
        Ok(skill)
    }

    pub fn search(
        &self,
        request: &SkillCatalogSearchRequest,
    ) -> Result<SkillCatalogPage, SkillDiscoveryContractError> {
        let parsed = crate::search::ParsedQuery::parse(&request.query);
        let query = parsed.canonical().to_owned();
        let mut matches = self
            .definitions
            .iter()
            .zip(self.search_index.iter())
            .filter_map(|(skill, index)| {
                let score = parsed.score(index);
                (score > 0).then_some((Reverse(score), skill.skill_ref.as_str(), skill))
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
        self.page(
            matches.into_iter().map(|(_, _, skill)| skill).collect(),
            &query,
            request.limit,
            request.cursor.as_deref(),
        )
    }

    pub fn list(
        &self,
        request: &SkillCatalogListRequest,
    ) -> Result<SkillCatalogPage, SkillDiscoveryContractError> {
        self.page(
            self.definitions.iter().collect(),
            "",
            request.limit,
            request.cursor.as_deref(),
        )
    }

    fn page(
        &self,
        ordered: Vec<&AuthorizedSkillDescriptor>,
        normalized_query: &str,
        limit: NonZeroUsize,
        cursor: Option<&str>,
    ) -> Result<SkillCatalogPage, SkillDiscoveryContractError> {
        let start = cursor.map_or(Ok(0), |cursor| {
            ordered
                .iter()
                .position(|skill| {
                    catalog_cursor(self.catalog_hash, normalized_query, &skill.skill_ref) == cursor
                })
                .map(|index| index.saturating_add(1))
                .ok_or(SkillDiscoveryContractError::InvalidSkillCursor)
        })?;
        let take = bounded_limit(limit, ordered.len().saturating_sub(start));
        let selected = ordered.iter().skip(start).take(take).collect::<Vec<_>>();
        let next_cursor = (start.saturating_add(take) < ordered.len())
            .then(|| {
                selected.last().map(|skill| {
                    catalog_cursor(self.catalog_hash, normalized_query, &skill.skill_ref)
                })
            })
            .flatten();
        Ok(SkillCatalogPage {
            items: selected
                .into_iter()
                .map(|skill| SkillCatalogSummary {
                    skill_ref: skill.skill_ref.clone(),
                    skill_version_id: skill.skill_version_id,
                    title: skill.title.clone(),
                    description: skill.description.clone(),
                    when_to_use: skill.when_to_use.clone(),
                    required_tools: skill.required_tools.clone(),
                    availability: skill.availability.clone(),
                })
                .collect(),
            next_cursor,
        })
    }
}

fn catalog_cursor(
    catalog_hash: Sha256,
    query: &str,
    skill_ref: &musteragent_identity::VersionedKey,
) -> String {
    let mut canonical = Vec::new();
    canonical.extend_from_slice(&catalog_hash.into_bytes());
    append_bytes(&mut canonical, query.as_bytes());
    append_bytes(&mut canonical, skill_ref.as_str().as_bytes());
    format!("v1:{}", Sha256::digest(canonical))
}

fn hash_definitions(definitions: &[AuthorizedSkillDescriptor]) -> Sha256 {
    let mut canonical = Vec::new();
    append_len(&mut canonical, definitions.len());
    for skill in definitions {
        append_bytes(&mut canonical, skill.skill_id.into_uuid().as_bytes());
        append_bytes(
            &mut canonical,
            skill.skill_version_id.into_uuid().as_bytes(),
        );
        append_bytes(&mut canonical, skill.skill_ref.as_str().as_bytes());
        append_bytes(&mut canonical, skill.title.as_bytes());
        append_bytes(&mut canonical, skill.description.as_bytes());
        append_bytes(&mut canonical, skill.when_to_use.as_bytes());
        append_bytes(
            &mut canonical,
            skill.instructions_ref.content_id.into_uuid().as_bytes(),
        );
        append_bytes(
            &mut canonical,
            skill.instructions_ref.media_type.as_str().as_bytes(),
        );
        canonical.extend_from_slice(&skill.instructions_ref.byte_length.to_be_bytes());
        append_bytes(&mut canonical, &skill.instructions_ref.sha256.into_bytes());
        append_len(&mut canonical, skill.required_tools.len());
        for tool in &skill.required_tools {
            append_bytes(&mut canonical, tool.as_str().as_bytes());
        }
        append_len(&mut canonical, skill.optional_tools.len());
        for tool in &skill.optional_tools {
            append_bytes(&mut canonical, tool.as_str().as_bytes());
        }
        append_len(&mut canonical, skill.compatible_agent_contracts.len());
        for contract in &skill.compatible_agent_contracts {
            append_bytes(&mut canonical, contract.as_str().as_bytes());
        }
        append_len(&mut canonical, skill.compatible_surfaces.len());
        for surface in &skill.compatible_surfaces {
            append_bytes(&mut canonical, surface.as_bytes());
        }
        canonical.push(match skill.activation {
            SkillCatalogActivation::Base => 1,
            SkillCatalogActivation::ExplicitOrResolved => 2,
            SkillCatalogActivation::SurfacePinned => 3,
        });
        canonical.push(match skill.lifecycle {
            SkillCatalogLifecycle::Active => 1,
            SkillCatalogLifecycle::Retired => 2,
            SkillCatalogLifecycle::Deleted => 3,
        });
        canonical.push(match skill.origin {
            SkillOrigin::Bundled => 1,
            SkillOrigin::Custom => 2,
            SkillOrigin::Installed => 3,
        });
        canonical.push(match skill.trust {
            SkillCatalogTrust::Official => 1,
            SkillCatalogTrust::Verified => 2,
            SkillCatalogTrust::Community => 3,
            SkillCatalogTrust::Local => 4,
        });
        append_provenance(&mut canonical, &skill.provenance);
        match skill.install_scope {
            SkillCatalogInstallScope::Global => canonical.push(1),
            SkillCatalogInstallScope::Tenant(tenant_id) => {
                canonical.push(2);
                append_bytes(&mut canonical, tenant_id.into_uuid().as_bytes());
            }
        }
        append_bytes(&mut canonical, &skill.spec_hash.into_bytes());
        append_len(&mut canonical, skill.references.len());
        for reference in &skill.references {
            canonical.push(match reference.kind {
                SkillCatalogReferenceKind::Instructions => 1,
                SkillCatalogReferenceKind::Manifest => 2,
                SkillCatalogReferenceKind::Package => 3,
            });
            append_content_ref(&mut canonical, &reference.content_ref);
        }
        match &skill.availability {
            SkillAvailability::Available => canonical.push(1),
            SkillAvailability::Unavailable {
                missing_required_tools,
            } => {
                canonical.push(2);
                append_len(&mut canonical, missing_required_tools.len());
                for tool in missing_required_tools {
                    append_bytes(&mut canonical, tool.as_str().as_bytes());
                }
            }
        }
    }
    if definitions.iter().any(|skill| !skill.tags.is_empty()) {
        append_bytes(&mut canonical, b"skill-discovery-metadata:v1");
        for skill in definitions {
            append_len(&mut canonical, skill.tags.len());
            for tag in &skill.tags {
                append_bytes(&mut canonical, tag.as_bytes());
            }
        }
    }
    Sha256::digest(canonical)
}

/// Canonical encoding primitive for mutation and compatibility proofs. Run
/// admission must still construct [`AuthorizedSkillCatalog`], which performs
/// the fail-closed semantic validation before retaining this digest.
#[must_use]
pub fn canonical_skill_catalog_material_hash(definitions: &[AuthorizedSkillDescriptor]) -> Sha256 {
    let mut definitions = definitions.to_vec();
    definitions.sort_by(|left, right| left.skill_ref.cmp(&right.skill_ref));
    for skill in &mut definitions {
        skill.references.sort_by(reference_order);
        skill.references.dedup();
    }
    hash_definitions(&definitions)
}

fn authority_is_closed(skill: &AuthorizedSkillDescriptor) -> bool {
    let provenance_shape = match (
        &skill.origin,
        &skill.trust,
        &skill.provenance,
        skill.install_scope,
    ) {
        (
            SkillOrigin::Bundled,
            SkillCatalogTrust::Official,
            SkillCatalogProvenance::Bundled { bundle_ref },
            SkillCatalogInstallScope::Global,
        ) => {
            // Provenance was authenticated by the publication source before
            // sealing. Recovery validates the sealed catalog digest, not a
            // mutable or compiled list of historical identities.
            matches!(bundle_ref.logical_key(), "musteragent.skill-bundle")
                && skill.activation
                    == if matches!(skill.skill_ref.logical_key(), "assistant-base") {
                        SkillCatalogActivation::Base
                    } else {
                        SkillCatalogActivation::ExplicitOrResolved
                    }
        }
        (
            SkillOrigin::Custom,
            SkillCatalogTrust::Local,
            SkillCatalogProvenance::Custom { .. },
            SkillCatalogInstallScope::Tenant(_),
        ) => true,
        (
            SkillOrigin::Installed,
            SkillCatalogTrust::Verified | SkillCatalogTrust::Community,
            SkillCatalogProvenance::Installed {
                source,
                source_ref,
                publisher_ref,
            },
            SkillCatalogInstallScope::Tenant(_),
        ) => {
            matches!(source.as_str(), "marketplace" | "hub" | "github" | "url")
                && !source_ref.trim().is_empty()
                && source_ref.len() <= 2_000
                && !publisher_ref.trim().is_empty()
                && publisher_ref.len() <= 500
        }
        _ => false,
    };
    provenance_shape
        && (skill.origin == SkillOrigin::Bundled
            || skill.activation == SkillCatalogActivation::ExplicitOrResolved)
}

fn append_provenance(target: &mut Vec<u8>, provenance: &SkillCatalogProvenance) {
    match provenance {
        SkillCatalogProvenance::Bundled { bundle_ref } => {
            target.push(1);
            append_bytes(target, bundle_ref.as_str().as_bytes());
        }
        SkillCatalogProvenance::Custom { created_by } => {
            target.push(2);
            append_bytes(target, created_by.into_uuid().as_bytes());
        }
        SkillCatalogProvenance::Installed {
            source,
            source_ref,
            publisher_ref,
        } => {
            target.push(3);
            append_bytes(target, source.as_bytes());
            append_bytes(target, source_ref.as_bytes());
            append_bytes(target, publisher_ref.as_bytes());
        }
    }
}

fn append_content_ref(target: &mut Vec<u8>, reference: &musteragent_content_ref::ContentRef) {
    append_bytes(target, reference.content_id.into_uuid().as_bytes());
    append_bytes(target, reference.media_type.as_str().as_bytes());
    target.extend_from_slice(&reference.byte_length.to_be_bytes());
    append_bytes(target, &reference.sha256.into_bytes());
}

fn reference_order(
    left: &SkillCatalogReference,
    right: &SkillCatalogReference,
) -> std::cmp::Ordering {
    let left_key = (
        left.kind,
        left.content_ref.content_id,
        &left.content_ref.media_type,
        left.content_ref.byte_length,
        left.content_ref.sha256,
    );
    let right_key = (
        right.kind,
        right.content_ref.content_id,
        &right.content_ref.media_type,
        right.content_ref.byte_length,
        right.content_ref.sha256,
    );
    left_key.cmp(&right_key)
}

fn append_bytes(target: &mut Vec<u8>, value: &[u8]) {
    append_len(target, value.len());
    target.extend_from_slice(value);
}

fn append_len(target: &mut Vec<u8>, value: usize) {
    target.extend_from_slice(&u64::try_from(value).unwrap_or(u64::MAX).to_be_bytes());
}
