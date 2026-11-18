use std::collections::BTreeMap;

use sqlx::{PgPool, Row as _};
use thiserror::Error;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const RUNTIME_SCHEMAS: &[&str] = &[
    "activity",
    "agent",
    "approval",
    "artifact",
    "capability",
    "collaboration",
    "conversation",
    "event_store",
    "evolution",
    "memory",
    "privacy_erasure",
    "projection",
    "resource",
    "runtime",
    "security_audit",
    "skill",
    "tool",
];

pub const RUNTIME_TABLES: &[(&str, &str)] = &[
    ("activity", "activities"),
    ("activity", "activity_edges"),
    ("agent", "agent_definitions"),
    ("agent", "agent_dialogue_entries"),
    ("agent", "agent_dialogues"),
    ("agent", "agent_versions"),
    ("agent", "agent_work_creation_publications"),
    ("agent", "device_admissions"),
    ("agent", "device_grant_heads"),
    ("agent", "device_grant_versions"),
    ("agent", "device_preparation_observations"),
    ("agent", "device_resource_publications"),
    ("agent", "device_source_not_admitted"),
    ("agent", "device_source_refusals"),
    ("agent", "dialogue_activity_dirty"),
    ("agent", "dialogue_admission_outcomes"),
    ("agent", "dialogue_admission_progress"),
    ("agent", "dialogue_content_references"),
    ("agent", "dialogue_context_checkpoints"),
    ("agent", "dialogue_context_heads"),
    ("agent", "dialogue_context_source_entries"),
    ("agent", "dialogue_execution_activity"),
    ("agent", "dialogue_execution_cancellation_applied"),
    ("agent", "dialogue_execution_cancellation_delivery"),
    ("agent", "dialogue_execution_cancellation_roots"),
    ("agent", "dialogue_execution_cancellation_targets"),
    ("agent", "dialogue_execution_cancellations"),
    ("agent", "dialogue_execution_control_snapshots"),
    ("agent", "dialogue_execution_control_targets"),
    ("agent", "dialogue_execution_source_roots"),
    ("agent", "dialogue_history_search_documents"),
    ("agent", "dialogue_history_search_pending"),
    ("agent", "dialogue_input_attachments"),
    ("agent", "dialogue_intent_admissions"),
    ("agent", "dialogue_publications"),
    ("agent", "dialogue_sessions"),
    ("agent", "dialogue_terminal_deliveries"),
    ("agent", "dialogue_tool_publication_groups"),
    ("agent", "dialogue_user_intents"),
    ("agent", "goal_source_bindings"),
    ("agent", "native_dialogue_interaction_commands"),
    ("agent", "native_dialogue_interaction_projections"),
    ("agent", "native_dialogue_terminals"),
    ("agent", "native_execution_activity"),
    ("agent", "native_execution_controls"),
    ("agent", "native_execution_not_admitted"),
    ("agent", "owned_agent_configurations"),
    ("agent", "owned_agent_inbox_entries"),
    ("agent", "owned_agent_inboxes"),
    ("agent", "owned_agents"),
    ("agent", "owner_command_receipts"),
    ("agent", "owner_default_reservations"),
    ("agent", "owner_event_subscriptions"),
    ("agent", "owner_goal_draft_registrations"),
    ("agent", "owner_goal_setup_deliveries"),
    ("agent", "owner_outbox"),
    ("agent", "owner_schedule_commands"),
    ("agent", "owner_schedule_occurrences"),
    ("agent", "owner_schedules"),
    ("agent", "owner_source_input_receipts"),
    ("agent", "owner_spaces"),
    ("agent", "public_delivery_candidates"),
    ("agent", "public_entry_reactions"),
    ("agent", "published_agent_version_pointers"),
    ("agent", "subagent_version_settings"),
    ("approval", "authorization_receipts"),
    ("approval", "evolution_management_interactions"),
    ("approval", "evolution_management_rejections"),
    ("approval", "interaction_responses"),
    ("approval", "interactions"),
    ("approval", "management_interactions"),
    ("approval", "task_result_review_retirements"),
    ("artifact", "admission_content_stages"),
    ("artifact", "artifact_proposals"),
    ("artifact", "artifact_version_dependencies"),
    ("artifact", "artifact_versions"),
    ("artifact", "artifacts"),
    ("artifact", "commit_commands"),
    ("artifact", "content_blob_compaction_cursor"),
    ("artifact", "content_cache_revision"),
    ("artifact", "content_gc_jobs"),
    ("artifact", "content_refs"),
    ("artifact", "device_resource_versions"),
    ("artifact", "device_versions"),
    ("artifact", "dialogue_publication_source_contents"),
    ("artifact", "dialogue_publication_sources"),
    ("artifact", "inline_content_blobs"),
    ("artifact", "invocation_content_budgets"),
    ("artifact", "invocation_content_outputs"),
    ("artifact", "media_generation_results"),
    ("artifact", "work_item_result_asset_candidates"),
    ("artifact", "work_item_result_asset_returns"),
    ("artifact", "work_item_result_native_origins"),
    ("capability", "browser_gateway_ticket_consumptions"),
    ("capability", "browser_job_content_refs"),
    ("capability", "browser_jobs"),
    ("capability", "browser_provider_identities"),
    ("capability", "browser_provider_slots"),
    ("capability", "capabilities"),
    ("capability", "provider_registrations"),
    ("capability", "runtime_snapshot_content_references"),
    ("capability", "runtime_snapshots"),
    ("collaboration", "input_task_action_invocations"),
    ("collaboration", "input_task_actions"),
    ("collaboration", "work_item_commands"),
    ("collaboration", "work_item_device_custody"),
    ("collaboration", "work_item_device_grant_versions"),
    ("collaboration", "work_item_device_stops"),
    ("collaboration", "work_item_device_targets"),
    ("collaboration", "work_item_device_terminals"),
    ("collaboration", "work_item_execution_reports"),
    ("collaboration", "work_item_execution_reservations"),
    ("collaboration", "work_item_followups"),
    ("collaboration", "work_item_native_sources"),
    ("collaboration", "work_item_outbox"),
    ("collaboration", "work_item_requester_actions"),
    ("collaboration", "work_item_result_deliveries"),
    ("collaboration", "work_item_result_items"),
    ("collaboration", "work_item_result_submissions"),
    ("collaboration", "work_item_revisions"),
    ("collaboration", "work_items"),
    ("conversation", "context_branch_state"),
    ("conversation", "context_branches"),
    ("conversation", "conversations"),
    ("conversation", "message_retractions"),
    ("conversation", "messages"),
    ("conversation", "participants"),
    ("conversation", "workspace_assignment_preparations"),
    ("event_store", "command_idempotency"),
    ("event_store", "domain_events"),
    ("event_store", "ledger_content_references"),
    ("event_store", "ledger_records"),
    ("event_store", "ledger_streams"),
    ("event_store", "outbox"),
    ("event_store", "output_frames"),
    ("event_store", "projection_checkpoints"),
    ("event_store", "retention_policies"),
    ("event_store", "snapshots"),
    ("evolution", "approvals"),
    ("evolution", "artifact_promotions"),
    ("evolution", "candidates"),
    ("evolution", "dream_job_candidates"),
    ("evolution", "dream_job_signals"),
    ("evolution", "dream_jobs"),
    ("evolution", "evaluations"),
    ("evolution", "instinct_evidence_records"),
    ("evolution", "instinct_evidence_runs"),
    ("evolution", "instinct_lifecycle_receipts"),
    ("evolution", "instinct_refs"),
    ("evolution", "instincts"),
    ("evolution", "learning_run_queue"),
    ("evolution", "learning_run_signals"),
    ("evolution", "learning_signal_evidence"),
    ("evolution", "learning_signal_evidence_records"),
    ("evolution", "learning_signal_exclusions"),
    ("evolution", "learning_signals"),
    ("evolution", "promotions"),
    ("evolution", "proposal_evidence_records"),
    ("evolution", "proposal_source_runs"),
    ("evolution", "proposals"),
    ("evolution", "studio_revision_drafts"),
    ("memory", "agent_memory_draft_batch_items"),
    ("memory", "agent_memory_draft_batches"),
    ("memory", "agent_memory_draft_publication_batches"),
    ("memory", "agent_memory_draft_publication_items"),
    ("memory", "agent_memory_drafts"),
    ("memory", "agent_memory_erasure_content_roots"),
    ("memory", "agent_memory_erasure_lineage"),
    ("memory", "agent_memory_revocations"),
    ("memory", "embedding_backfill_jobs"),
    ("memory", "embedding_capability"),
    ("memory", "embedding_index"),
    ("memory", "embedding_spaces"),
    ("memory", "explicit_publications"),
    ("memory", "governance_publication_outbox"),
    ("memory", "memories"),
    ("memory", "memory_refs"),
    ("memory", "memory_version_evidence_records"),
    ("memory", "memory_version_source_runs"),
    ("memory", "memory_versions"),
    ("memory", "native_publication_reconciliations"),
    ("memory", "native_tool_approvals"),
    ("memory", "search_backfill_jobs"),
    ("memory", "search_erasure_tombstones"),
    ("memory", "search_index"),
    ("privacy_erasure", "completion_intent_coverage"),
    ("privacy_erasure", "execution_work"),
    ("privacy_erasure", "plan_data_classes"),
    ("privacy_erasure", "plan_retention_exceptions"),
    ("privacy_erasure", "plans"),
    ("privacy_erasure", "receipt_completed_data_classes"),
    ("privacy_erasure", "receipt_retention_exceptions"),
    ("privacy_erasure", "receipts"),
    ("privacy_erasure", "requests"),
    ("privacy_erasure", "root_resources"),
    ("privacy_erasure", "source_evidence_producers"),
    ("privacy_erasure", "transitions"),
    ("privacy_erasure", "work_item_lineage"),
    ("projection", "conversation_catalog"),
    ("projection", "conversation_catalog_actions"),
    ("projection", "conversation_catalog_attention"),
    ("projection", "conversation_catalog_attention_deliveries"),
    ("projection", "conversation_catalog_bootstrap_receipts"),
    ("projection", "conversation_catalog_changes"),
    ("projection", "conversation_catalog_clocks"),
    ("projection", "conversation_catalog_executions"),
    ("projection", "conversation_catalog_projector_checkpoints"),
    ("projection", "conversation_catalog_pursuit_runs"),
    ("projection", "conversation_catalog_sources"),
    ("projection", "conversation_read_receipts"),
    ("projection", "delivery_receipts"),
    ("resource", "change_set_mutations"),
    ("resource", "change_sets"),
    ("resource", "copy_origins"),
    ("resource", "device_command_reads"),
    ("resource", "device_commands"),
    ("resource", "edit_operations"),
    ("resource", "edit_sessions"),
    ("resource", "edit_terminal_commands"),
    ("resource", "embedding_capability"),
    ("resource", "embedding_index"),
    ("resource", "embedding_jobs"),
    ("resource", "embedding_spaces"),
    ("resource", "execution_workspace_preparations"),
    ("resource", "execution_workspace_settings"),
    ("resource", "execution_workspace_snapshot_entries"),
    ("resource", "execution_workspace_writers"),
    ("resource", "execution_workspaces"),
    ("resource", "file_retraction_outcomes"),
    ("resource", "nodes"),
    ("resource", "owned_agent_scope_assignments"),
    ("resource", "scope_heads"),
    ("resource", "scope_history"),
    ("resource", "search_chunks"),
    ("resource", "search_index_jobs"),
    ("resource", "search_manifests"),
    ("resource", "workspace_assignment_receipts"),
    ("resource", "workspace_authority"),
    ("resource", "workspace_history_roots"),
    ("resource", "workspace_roots"),
    ("runtime", "attempts"),
    ("runtime", "context_compaction_slots"),
    ("runtime", "context_plans"),
    ("runtime", "delegation_deliveries"),
    ("runtime", "delegation_events"),
    ("runtime", "delegation_hook_deliveries"),
    ("runtime", "delegation_messages"),
    ("runtime", "delegation_stop_requests"),
    ("runtime", "delegations"),
    ("runtime", "derived_run_authorities"),
    ("runtime", "dialogue_context_bindings"),
    ("runtime", "dialogue_context_content_references"),
    ("runtime", "evolution_maintenance_executions"),
    ("runtime", "execution_grant_consumptions"),
    ("runtime", "execution_grant_invocations"),
    ("runtime", "execution_grant_routes"),
    ("runtime", "execution_liveness"),
    ("runtime", "execution_plans"),
    ("runtime", "extension_facts"),
    ("runtime", "goal_deletions"),
    ("runtime", "goal_evaluation_claims"),
    ("runtime", "goal_evaluation_evidence"),
    ("runtime", "goal_evaluation_health"),
    ("runtime", "goal_evaluation_proposal_evidence"),
    ("runtime", "goal_evaluation_proposals"),
    ("runtime", "goal_evaluations"),
    ("runtime", "goal_inputs"),
    ("runtime", "goal_processing_rules"),
    ("runtime", "goal_pursuits"),
    ("runtime", "goal_result_review_retirements"),
    ("runtime", "goal_review_retirement_scans"),
    ("runtime", "goal_run_processing_rules"),
    ("runtime", "goal_wake_deliveries"),
    ("runtime", "goal_wake_signals"),
    ("runtime", "leases"),
    ("runtime", "loop_checkpoints"),
    ("runtime", "loop_controls"),
    ("runtime", "model_action_rejections"),
    ("runtime", "model_completion_intents"),
    ("runtime", "model_dependency_recovery"),
    ("runtime", "model_event_batches"),
    ("runtime", "model_invocations"),
    ("runtime", "model_usage_facts"),
    ("runtime", "pending_tool_calls"),
    ("runtime", "runs"),
    ("runtime", "sandbox_routing_contexts"),
    ("runtime", "schedule_events"),
    ("runtime", "skill_disclosures"),
    ("runtime", "standing_goals"),
    ("runtime", "tool_exposures"),
    ("security_audit", "admission_denials"),
    ("security_audit", "records"),
    ("security_audit", "resource_refs"),
    ("skill", "active_version_pointers"),
    ("skill", "package_audits"),
    ("skill", "product_account_installations"),
    ("skill", "product_import_commands"),
    ("skill", "retained_admission_catalogs"),
    ("skill", "sandbox_declarations"),
    ("skill", "skill_package_previews"),
    ("skill", "skill_versions"),
    ("skill", "skills"),
    ("tool", "origin_tool_calls"),
    ("tool", "tool_invocations"),
    ("tool", "tool_results"),
];

/// Applies the expand-only Runtime migration set.
/// Installs the kernel schema on a new database, or verifies an existing one.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await?;
    // Restore derived retention roots from immutable ledger bytes. Services
    // only verify migrations; this recovery is owned by the migrator.
    let database = crate::PostgresDatabaseRuntime::from(pool.clone());
    loop {
        let count = crate::ledger_content::backfill(&database, 1)
            .await
            .map_err(|error| {
                sqlx::migrate::MigrateError::Execute(sqlx::Error::Protocol(error.to_string()))
            })?;
        if count < 64 {
            return Ok(());
        }
    }
}

/// Verifies the complete immutable migration set without changing schema.
///
/// Runtime API, Worker and Projector call this at composition time. Applying
/// schema belongs exclusively to the dedicated migration process so a normal
/// service restart can never start an irreversible historical-data workflow.
pub async fn verify_migrations(pool: &PgPool) -> Result<(), MigrationVerificationError> {
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public._sqlx_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if !table_exists {
        return Err(MigrationVerificationError::NotInitialized);
    }

    let mut applied = BTreeMap::new();
    for row in sqlx::query(
        "SELECT version, success, checksum FROM public._sqlx_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await?
    {
        let version: i64 = row.try_get("version")?;
        if !row.try_get::<bool, _>("success")? {
            return Err(MigrationVerificationError::Dirty(version));
        }
        applied.insert(version, row.try_get::<Vec<u8>, _>("checksum")?);
    }

    for (&version, checksum) in &applied {
        let migration = MIGRATOR
            .iter()
            .find(|migration| migration.version == version)
            .ok_or(MigrationVerificationError::UnknownVersion(version))?;
        if migration.checksum.as_ref() != checksum.as_slice() {
            return Err(MigrationVerificationError::ChecksumMismatch(version));
        }
    }
    for migration in MIGRATOR
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
    {
        if !applied.contains_key(&migration.version) {
            return Err(MigrationVerificationError::Pending(migration.version));
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum MigrationVerificationError {
    #[error("Runtime schema is not initialized; run musteragent-database-migrator")]
    NotInitialized,
    #[error("Runtime migration {0} is incomplete")]
    Dirty(i64),
    #[error("Runtime database contains unknown migration {0}")]
    UnknownVersion(i64),
    #[error("Runtime migration {0} is pending; run musteragent-database-migrator")]
    Pending(i64),
    #[error("Runtime migration {0} checksum differs from the immutable source")]
    ChecksumMismatch(i64),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Exposes immutable migration metadata for upgrade-path conformance tests and
/// deployment tooling. Production callers should normally use [`migrate`].
#[doc(hidden)]
pub fn migration_set() -> &'static sqlx::migrate::Migrator {
    &MIGRATOR
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;
