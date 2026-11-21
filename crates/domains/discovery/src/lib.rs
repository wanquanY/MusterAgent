//! Run-scoped authorization, discovery and progressive Tool disclosure.

#![forbid(unsafe_code)]

pub use musteragent_content_ref::Sha256;

mod carry_forward;
mod catalog;
mod exposure;
mod model;
mod model_names;
mod model_skill_names;
pub mod search;
mod skill_catalog;
mod skill_disclosure;

pub use carry_forward::{reauthorize_skill_disclosures, reauthorize_tool_exposures};
pub use catalog::{AuthorizedToolCatalog, ToolCatalogSearchRequest, ToolCatalogSearchResult};
pub use exposure::{ActivationConstraints, ActivationOutcome, ActivationRequest, ExposureState};
pub use model::{
    AuthorizedSkillDescriptor, AuthorizedToolDescriptor, CarryForwardEvidenceError,
    CarryForwardRejectionReason, DiscoveryCarryForwardDecision, DiscoveryCarryForwardEvidence,
    DiscoveryContractError, DiscoveryEvent, DiscoverySnapshot, PriorSkillDisclosure,
    PriorSkillDisclosureProvenance, PriorToolExposure, RejectedSkillCarryForward,
    RejectedToolCarryForward, SkillAvailability, SkillCatalogActivation, SkillCatalogInstallScope,
    SkillCatalogLifecycle, SkillCatalogProvenance, SkillCatalogReference,
    SkillCatalogReferenceKind, SkillCatalogSummary, SkillCatalogTrust, SkillDisclosed,
    SkillDisclosureItem, SkillDiscoveryContractError, SkillOrigin, ToolCatalogSummary,
    ToolExposureAdded, ToolExposureItem,
};
pub use model_names::{ModelToolNames, ToolNameError};
pub use model_skill_names::{ModelSkillNames, SkillNameError};
pub use skill_catalog::{
    AuthorizedSkillCatalog, SkillCatalogListRequest, SkillCatalogPage, SkillCatalogSearchRequest,
    canonical_skill_catalog_material_hash,
};
pub use skill_disclosure::{SkillDisclosureOutcome, SkillDisclosureRequest, SkillDisclosureState};

mod tool_schema;
pub use tool_schema::{ToolSchemaMaterial, indexed_catalog_hash};
