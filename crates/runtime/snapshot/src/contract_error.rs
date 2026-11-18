//! Canonical Snapshot validation failures.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotContractError {
    #[error("Owner-scoped Goal Snapshot authority is missing, invalid, or in a historical schema")]
    InvalidDialogueAuthority,
    #[error(transparent)]
    Identity(#[from] musteragent_identity::IdentityError),
    #[error(transparent)]
    Key(#[from] musteragent_identity::LogicalKeyError),
    #[error(transparent)]
    Agent(#[from] musteragent_definition::AgentContractError),
    #[error(transparent)]
    Skill(#[from] musteragent_skill::SkillContractError),
    #[error(transparent)]
    Tool(#[from] musteragent_tool::ToolContractError),
    #[error(transparent)]
    Memory(#[from] musteragent_memory::MemoryContractError),
    #[error(transparent)]
    Model(#[from] musteragent_model::ModelContractError),
    #[error("Runtime Snapshot schema versions disagree")]
    SchemaMismatch,
    #[error("current Runtime Snapshot contains a legacy or mixed product namespace")]
    ProductNamespaceMismatch,
    #[error("Runtime Snapshot discovery presence disagrees with its schema version")]
    DiscoverySchemaMismatch,
    #[error("Runtime Snapshot AgentVersion material presence disagrees with its schema version")]
    AgentVersionSchemaMismatch,
    #[error("Runtime Snapshot Instinct selections must be bounded, unique and canonical for v6")]
    InstinctsNotCanonical,
    #[error("Runtime Snapshot AgentVersion material is incomplete or internally inconsistent")]
    InvalidAgentVersionSnapshot,
    #[error("Runtime Snapshot auxiliary routes disagree with its schema version")]
    AuxiliarySchemaMismatch,
    #[error("Runtime Snapshot auxiliary routes must be unique and strictly ordered by role")]
    AuxiliaryModelsNotCanonical,
    #[error("Runtime Snapshot auxiliary routes cannot contain fallback plans")]
    AuxiliaryContainsFallbackPlan,
    #[error("decision route requires a complete input profile and the Decision auxiliary role")]
    DecisionRouteContractInvalid,
    #[error("Runtime Snapshot discovery contract is invalid")]
    InvalidDiscovery,
    #[error("Runtime Snapshot discovery invariant failed: {0}")]
    DiscoveryInvariant(&'static str),
    #[error("private standing Goal evaluation Snapshot is not isolated")]
    InvalidPrivateEvaluation,
    #[error("Prompt sections must be non-empty and unique in their semantic assembly order")]
    PromptSectionsNotCanonical,
    #[error("Skill definitions must be non-empty, unique and sorted")]
    SkillsNotCanonical,
    #[error("Tool identity and wire names must be unique")]
    DuplicateTool,
    #[error("Tool Snapshot title and description must be non-empty")]
    ToolMetadataEmpty,
    #[error("Tool Snapshot schema bytes are empty or disagree with their pinned hashes")]
    ToolSchemaBytesMismatch,
    #[error("Tool Snapshot file operation metadata is invalid")]
    InvalidFileOperationMetadata,
    #[error("Tool execution target, idempotency and retry contract disagree")]
    ToolExecutionContractMismatch,
    #[error(
        "Capability grants must be allowed or approval-obligated, non-empty, unique and sorted"
    )]
    CapabilitiesNotCanonical,
    #[error("Capability provider grant is duplicated")]
    DuplicateCapability,
    #[error("Skill requires a Capability absent from this Run Snapshot")]
    SkillCapabilityNotGranted,
    #[error("Tool references a Capability provider not pinned by this Run")]
    ToolCapabilityNotGranted,
    #[error("Tool effect differs from its pinned Capability grant")]
    ToolCapabilityEffectMismatch,
    #[error("Tool execution target differs from its pinned Capability location")]
    ToolCapabilityLocationMismatch,
    #[error("Tool requires an authorization scope absent from its pinned Capability grant")]
    ToolCapabilityScopeMismatch,
    #[error("Context token budget is internally inconsistent")]
    InvalidContextBudget,
    #[error("Run execution policy is invalid or unbounded")]
    InvalidRunExecutionPolicy,
    #[error("model stream budget is invalid or internally inconsistent")]
    InvalidModelStreamBudget,
    #[error("resolved fallback candidates do not match the primary route fallback order")]
    FallbackPlanMismatch,
    #[error("fallback candidates cannot contain another unresolved fallback plan")]
    NestedFallbackPlan,
    #[error("deterministic Snapshot encoding cannot be empty")]
    EmptyEncoding,
}
