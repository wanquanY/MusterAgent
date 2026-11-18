//! PostgreSQL adapters for Context-owned Agent Runtime persistence.

#![forbid(unsafe_code)]

// Shared storage conformance fixtures also run inside this crate.
#[cfg(test)]
extern crate self as musteragent_postgres;

mod delegation_history;
pub use delegation_history::PostgresDelegationHistorySource;
mod admission;
mod admission_refs;
mod agent_definition;
mod artifact;
mod artifact_commit;
mod artifact_proposal;
mod origin_tools;
pub use origin_tools::{
    OriginToolCall, OriginToolRecipient, OriginToolResult, PostgresOriginTools,
};
mod browser_gateway;
mod catalog;
mod commit_lane;
mod content;
mod content_blob;
mod context_compaction_slot;
mod context_material;
mod context_plan;
mod control;
mod conversation_catalog;
mod database_failure;
mod database_runtime;
mod discovery;
mod dream_evidence;
mod evolution_inspection;
mod evolution_maintenance;
mod evolution_review;
mod execution;
mod execution_liveness;
mod public_delivery_repair;
pub use execution_liveness::PostgresFencedExecutionLivenessJournal;
mod execution_grant_proof;
mod execution_plan;
mod exposure_projection;
mod file_edit_commit;
mod instinct;
mod interaction;
mod interaction_application;
mod learning;
mod ledger;
mod loop_journal;
mod loop_policy;
mod management_publication;
mod management_rejection;
mod memory;
mod memory_draft;
mod memory_error;
mod memory_evolution_commit;
mod memory_lifecycle;
mod memory_management;
mod memory_management_authority;
mod memory_publication;
mod memory_search_backfill;
mod memory_search_index;
mod memory_tool_commit;
mod memory_vector_index;
mod migration;
mod model;
mod model_dependency_recovery;
pub use model_dependency_recovery::PostgresModelDependencyRecoveryStore;
mod numeric;
mod privacy_erasure;
mod privacy_erasure_admission;
mod projection;
mod projection_branch;
mod projection_snapshot;
mod projection_snapshot_authority;
mod resource;
mod workspace_assignment;
pub use workspace_assignment::PostgresWorkspaceAssignmentPreparationStore;
mod workspace_resource_root;
pub use workspace_resource_root::PostgresWorkspaceResourceRootAuthority;
pub(crate) mod dialogue_execution_control;
pub(crate) mod native_execution_control;
pub use dialogue_execution_control::PostgresDialogueExecutionCancellationStore;
mod agent_resource;
pub(crate) mod dialogue_execution_activity;
pub use agent_resource::device::PostgresDeviceResourceStore;
pub use dialogue_execution_activity::{
    PostgresDialogueActivityProjection, PostgresNativeDialogueActivityStore,
};
mod execution_workspace;
mod execution_workspace_project;
mod execution_workspace_snapshot;
mod execution_workspace_tree;
mod resource_authority;
pub use execution_workspace::PostgresExecutionWorkspaceAuthority;
mod resource_change_set;
mod resource_commit;
mod resource_edit;
mod resource_embeddings;
mod resource_file_catalog;
mod resource_file_name;
mod resource_file_placements;
mod resource_file_replacement;
mod resource_file_versions;
mod resource_read;
mod resource_retraction;
mod resource_search;
mod resource_work_copy;
mod run_ledger;
mod runtime_application;
mod runtime_application_error;
mod scheduler;
mod security_audit;
mod skill_discovery;
mod skill_lifecycle;
mod skill_preview;
mod standing_goal;
mod standing_goal_judge;
mod tool;
mod tool_commit;
mod tool_material;
mod tool_security_audit;
mod transaction_policy;
mod typed_resource;

pub use admission::{PostgresAdmissionError, PostgresAdmissionStore};
pub use agent_definition::PostgresAgentDefinitionStore;
pub use artifact::{PostgresArtifactError, PostgresArtifactStore};
pub use artifact_commit::{PostgresFencedArtifactError, PostgresFencedArtifactToolCommitter};
pub use artifact_proposal::{PostgresArtifactProposalError, PostgresArtifactProposalStore};
pub use browser_gateway::PostgresBrowserGatewayStore;
pub use catalog::{PostgresAgentCatalog, PostgresAgentCatalogError};
pub use content::{
    ContentGcPolicy, ContentGcReport, PostgresContentCacheWatch, PostgresContentError,
    PostgresContentStore,
};
pub use content_blob::{INLINE_CONTENT_MAX_BYTES, PostgresTieredBlobStore};
pub use context_compaction_slot::{
    ContextCompactionDispatchState, PersistedContextCompactionSlot,
    PostgresContextCompactionSlotError, PostgresContextCompactionSlotStore,
};
pub use context_material::{PostgresContextMaterialError, PostgresContextMaterialSource};
pub use context_plan::{PostgresContextPlanError, PostgresContextPlanStore};
pub use control::PostgresRunControlError;
pub use conversation_catalog::{
    PostgresConversationCatalogError, PostgresConversationCatalogStore,
};
pub use database_runtime::{
    DEFAULT_ACQUIRE_TIMEOUT, DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_LOCK_TIMEOUT, DEFAULT_MAX_LIFETIME, DEFAULT_POOL_OBSERVATION_INTERVAL,
    DEFAULT_STATEMENT_TIMEOUT, PostgresDatabaseRuntime, PostgresPoolConfig,
    PostgresPoolConfigError, PostgresPoolSnapshot, PostgresTimeouts, observe_postgres_pool,
};
pub use discovery::PostgresFencedDiscoveryToolCommitter;
pub use dream_evidence::{PostgresDreamEvidenceError, PostgresDreamEvidenceMaterialSource};
pub use evolution_maintenance::PostgresEvolutionMaintenanceStore;
pub use execution::PostgresExecutionError;
pub use execution_plan::PostgresExecutionPlanStore;
pub use file_edit_commit::{PostgresFencedFileEditToolCommitter, PostgresFencedFileEditToolError};
pub use interaction::{PostgresInteractionError, PostgresInteractionStore};
pub use interaction_application::PostgresInteractionApplicationStore;
pub use learning::PostgresLearningError;
pub use ledger::{PostgresLedgerError, PostgresLedgerStore};
pub use loop_journal::{PostgresAgentLoopJournal, PostgresAgentLoopJournalError};
pub use loop_policy::{PostgresExecutionPolicySource, PostgresLoopControlQueue};
pub use memory::PostgresMemoryEvolutionStore;
pub use memory_error::PostgresMemoryEvolutionError;
pub use memory_tool_commit::{PostgresFencedMemoryToolCommitter, PostgresFencedMemoryToolError};
pub use memory_vector_index::PostgresMemoryVectorIndex;
pub use migration::{
    MigrationVerificationError, RUNTIME_SCHEMAS, RUNTIME_TABLES, migrate, migration_set,
    verify_migrations,
};
pub use model::{
    PersistedModelInvocationRecovery, PersistedModelInvocationRecoveryState,
    PostgresModelInvocationError, PostgresModelInvocationStore,
};
pub use privacy_erasure::{PostgresPrivacyErasureError, PostgresPrivacyErasureStore};
pub use projection::{
    PostgresLedgerChangeFeed, PostgresLedgerRecordSource, PostgresProjectionStore,
};
pub use projection_snapshot::PostgresConversationProjection;
pub use resource::PostgresResourceStore;
pub use resource_authority::PostgresResourceContextAuthority;
pub use resource_commit::{PostgresFencedResourceToolCommitter, PostgresFencedResourceToolError};
pub use resource_embeddings::{
    PostgresResourceEmbeddings, ResourceEmbeddingClaim, ResourceEmbeddingModel,
};
pub use runtime_application::PostgresRuntimeApplicationStore;
pub use scheduler::PostgresDispatchStore;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use security_audit::PostgresSecurityAuditStore as PostgresSecurityAuditTestStore;
pub use security_audit::{
    PostgresSecurityAuditError, PostgresSecurityAuditPipeline, PostgresSecurityAuditReader,
};
pub use skill_discovery::{
    PostgresFencedSkillDisclosureCommitter, PostgresSkillDiscoveryMaterialSource,
};
pub use skill_lifecycle::PostgresSkillLifecycleStore;
pub use skill_preview::PostgresSkillPackagePreviewStore;
pub use standing_goal::PostgresStandingGoalProductStore;
pub use standing_goal_judge::PostgresStandingGoalJudgeRunSource;
pub use tool::{PostgresToolInvocationError, PostgresToolInvocationStore};
pub use tool_commit::PostgresFencedToolInvocationJournal;
pub use tool_material::PostgresToolExecutionMaterialSource;
pub use transaction_policy::{
    GovernedPostgresTransaction, PostgresFailureClass, PostgresSavepointError,
    PostgresTransactionAccess, PostgresTransactionError, PostgresTransactionIsolation,
    PostgresTransactionPolicy, PostgresTransactionPolicyError, TransactionReplaySafety,
    begin_connection_transaction, begin_transaction, classify_postgres_error, execute_transaction,
};

mod agent_file_query;
mod file_query_authority;
pub use agent_file_query::{
    PostgresAgentFileQueryAuthority, PostgresAgentInputFileSource, PostgresAgentOutputFileSource,
};

mod invocation_content;
mod ledger_content;
pub use invocation_content::PostgresInvocationContentStore;

mod owned_agent;
mod owned_agent_activation;
mod owned_agent_lifecycle;
mod work_item_authority;
mod work_item_execution;
pub use work_item_authority::PostgresWorkItemAuthority;
pub use work_item_execution::{
    PostgresWorkItemControlStore, PostgresWorkItemDeviceExecutionStore,
    PostgresWorkItemExecutionHandoffStore, PostgresWorkItemExecutionStore,
    PostgresWorkItemReportStore, PostgresWorkItemStartStore,
};
mod agent_dialogue;
mod dialogue_codec;
mod dialogue_input_attachment;
mod dialogue_input_provenance;
pub use agent_dialogue::PostgresAgentDialogueStore;
pub use dialogue_input_attachment::PostgresDialogueInputAttachmentAccess;
pub use owned_agent::PostgresOwnedAgentStore;

mod account;
mod keyed_signal;
mod owner_event_sources;
pub use owner_event_sources::{PostgresOwnerEventSourceStore, SOURCE_DESIRED_TOPIC};
mod supervisor_wake;
pub use supervisor_wake::SupervisorLane;
mod dialogue_feed;
mod dialogue_publication;
pub use account::PostgresAccountStore;
pub use dialogue_feed::PostgresAccountChangeFeed;
pub use dialogue_publication::PostgresDeviceTerminalStore;
pub use dialogue_publication::PostgresDeviceToolPublication;
pub use dialogue_publication::PostgresDialoguePublicationStore;
pub use dialogue_publication::PostgresFencedDialogueToolPublication;
pub use dialogue_publication::PostgresNativeDialogueInteractionStore;
pub use dialogue_publication::terminal::PostgresPublicTerminalDeliveryStore;

mod owned_reservation;
pub use owned_reservation::PostgresOwnerDefaultReservationStore;

mod dialogue_context;
mod dialogue_execution;
pub use dialogue_context::{PostgresDeviceContextStore, PostgresDialogueContextSourceStore};
mod dialogue_handoff;
pub use dialogue_execution::PostgresDialogueExecutionStore;
pub use dialogue_handoff::PostgresDialogueIntentHandoffStore;

mod agent_self_configuration;
pub use agent_self_configuration::{
    PostgresAgentSelfConfiguration, PostgresAgentSelfDeviceConfiguration,
};

mod agent_running_tasks;
mod agent_work;
mod agent_work_creations;
mod agent_work_events;
mod owner_schedule;
pub use agent_running_tasks::PostgresAgentRunningTasksQuery;
pub use agent_work::PostgresAgentWorkQuery;
pub use owner_schedule::PostgresOwnerScheduleStore;
pub use owner_schedule::codec::{owner_schedule_definition, owner_schedule_wire};

mod owner_goal_tools;
pub use owner_goal_tools::PostgresOwnerGoalTools;
mod owner_schedule_tools;
pub use owner_schedule_tools::PostgresOwnerScheduleTools;

pub use memory::NativeMemoryReconciliation;

mod public_reactions;

mod sandbox_routing;
pub use sandbox_routing::PostgresSandboxRoutingStore;

mod media_generation;
pub use media_generation::PostgresMediaGenerationJournal;

mod notification;
pub use notification::{PostgresWakeChannel, PostgresWakeSignal, PostgresWakeSignalError};

mod extension_journal;
pub use extension_journal::{PostgresExtensionJournal, PostgresExtensionToolHistory};

mod cloud_delegation;
pub use cloud_delegation::PostgresCloudDelegation;
mod cloud_admission;
mod cloud_cancellation;
mod cloud_schedule;
pub use cloud_schedule::{
    PostgresCloudScheduleTools, PostgresScheduleDispatcher, PostgresScheduleService,
};

#[cfg(test)]
#[path = "../tests/dialogue_fixture/mod.rs"]
mod dialogue_fixture;
