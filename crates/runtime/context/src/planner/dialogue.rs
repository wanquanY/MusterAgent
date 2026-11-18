//! Catch up the fixed public source cut before authorizing a primary request.
//! Uses the original Context compactor, invocation recovery and fenced commit.
use super::*;
mod batch;
mod capacity;
mod turns;
use crate::{DialogueContextBindCommit, DialogueContextCheckpointCommit, DialogueContextWindow};
use musteragent_execution_context::{
    DialogueContextBound, DialogueContextCheckpoint, DialogueContextCut,
};
use musteragent_identity::DialogueContextCheckpointId;

impl DurableContextPlanner {
    pub(super) async fn prepare_dialogue_context(
        &self,
        input: &ContextPlanInput,
        material_reuse: &incremental_material::MaterialReuse,
    ) -> Result<Option<DialogueContextCut>, ContextPlanError> {
        let mut frozen = None;
        let mut capacity = None;
        loop {
            let Some(window) = observe_context_plan_future(
                input.attempt.run_id,
                "dialogue_window",
                self.source.dialogue_window(input),
            )
            .await?
            else {
                return if frozen.is_none() {
                    Ok(None)
                } else {
                    Err(ContextPlanError::invariant())
                };
            };
            window.validate()?;
            if frozen.as_ref().is_some_and(|cut| *cut != window.frozen_cut) {
                return Err(ContextPlanError::invariant());
            }
            frozen = Some(window.frozen_cut.clone());
            if window.complete() && window.started_coverage.is_none() {
                return Ok(frozen);
            }
            let starts_raw_batch = window.bound_through_seq == 0
                && self.source.can_preview_initial_dialogue_pages()
                && window.next_page.as_ref().is_some_and(|page| {
                    page.checkpoint.is_none() && page.through_seq < window.frozen_cut.through.seq
                });
            if (window.has_uncompacted_prefix() || starts_raw_batch)
                && let Some(mut material) = self
                    .dialogue_compaction_material(input, &window, &mut capacity, material_reuse)
                    .await?
            {
                self.project_for_model(&mut material, material_reuse)?;
                let compacted = observe_context_plan_future(
                    input.attempt.run_id,
                    "dialogue_compaction",
                    self.compact_dialogue_prefix(
                        input,
                        &window,
                        &material,
                        capacity.as_ref().map(|c| c.required_savings()),
                    ),
                )
                .await?;
                if compacted {
                    capacity = None;
                    continue;
                }
            }
            if let Some(complete) = self.bind_dialogue_batch(input, &window, &capacity).await? {
                if complete {
                    return Ok(frozen);
                }
                continue;
            }
            if let Some(page) = &window.next_page {
                let bytes = self.codec.encode_dialogue_page(page)?;
                let reference = self
                    .content
                    .put(
                        &input.attempt,
                        window.frozen_cut.tenant_id,
                        ContextPlanContentKind::DialogueSourcePage,
                        bytes,
                    )
                    .await?;
                observe_context_plan_future(
                    input.attempt.run_id,
                    "dialogue_bind",
                    self.plans.bind_dialogue_source(DialogueContextBindCommit {
                        attempt: input.attempt.clone(),
                        binding: DialogueContextBound {
                            frozen_cut: window.frozen_cut.clone(),
                            page_ordinal: window.next_page_ordinal,
                            from_seq: page.from_seq,
                            through_seq: page.through_seq,
                            source_page_ref: reference,
                            checkpoint_id: page.checkpoint.as_ref().map(|c| c.checkpoint_id),
                            completes_cut: page.through_seq == window.frozen_cut.through.seq,
                        },
                    }),
                )
                .await?;
                // The fenced commit has bound this complete immutable cut.
                // Do not fetch the same remote window again just to rediscover
                // completion. Material loading and dispatch recheck currentness.
                if page.through_seq == window.frozen_cut.through.seq {
                    return Ok(frozen);
                }
            } else {
                return Err(ContextPlanError::invariant());
            }
        }
    }

    pub(super) async fn compact_dialogue_prefix(
        &self,
        input: &ContextPlanInput,
        window: &DialogueContextWindow,
        material: &ContextMaterial,
        required_savings: Option<u64>,
    ) -> Result<bool, ContextPlanError> {
        let Some(selection) = turns::select(material, window, required_savings)? else {
            return Ok(false);
        };
        let (compacted, retained) = selection.entries(material)?;
        let coverage = selection.coverage.clone();
        observe_context_plan_stage(
            input.attempt.run_id,
            "dialogue_compaction_partition",
            || {
                if compacted.is_empty() || retained.is_empty() {
                    Err(ContextPlanError::invariant())
                } else {
                    Ok(())
                }
            },
        )?;
        observe_context_plan_stage(input.attempt.run_id, "dialogue_coverage", || {
            validate_coverage(&compacted, &window.frozen_cut, coverage.through.seq)
        })?;
        let model =
            observe_context_plan_stage(input.attempt.run_id, "dialogue_compression_route", || {
                auxiliary_model_route(
                    &material.runtime_snapshot.auxiliary_models,
                    AuxiliaryModelRole::Compression,
                )
            })?;
        let budgets = observe_context_plan_stage(
            input.attempt.run_id,
            "dialogue_compression_budget",
            || planning_budgets(material, &model.route),
        )?;
        let input_budget = budgets.hard_input_budget();
        let before = estimate_entries(&compacted).saturating_add(estimate_entries(&retained));
        let summary_budget = if model_capacity::uses_model_capacity(material) {
            let destination = planning_budgets(material, &material.runtime_snapshot.model.route)?;
            model_capacity::summary_target(destination, estimate_entries(&compacted))
        } else {
            // Historical policy projection, retained only for sealed runs.
            model
                .route
                .max_output_tokens
                .get()
                .min(8192)
                .min(input_budget / 4)
                .max(1)
        };
        let generated = match observe_context_plan_future(
            input.attempt.run_id,
            "dialogue_summary",
            self.generate_summary(
                input,
                material,
                window.history_source_at(coverage.through.seq, coverage.source_chain_sha256)?,
                model,
                &compacted,
                &retained,
                None,
                before,
                input_budget,
                summary_budget,
            ),
        )
        .await?
        {
            SummaryGeneration::Ready(result) => result,
            SummaryGeneration::FailedAfterStart {
                invocation_id,
                error,
            } => {
                let (outcome, error) = compaction_resolution::failure_resolution(
                    error,
                    material.runtime_snapshot.execution_policy.recovery_policy,
                );
                self.plans
                    .resolve_compaction(ContextCompactionResolutionCommit {
                        attempt: input.attempt.clone(),
                        resolution: ContextCompactionResolution {
                            invocation_id,
                            outcome,
                            loop_iteration: input.next_iteration,
                            context_snapshot_id: None,
                            dialogue_checkpoint_id: None,
                            input_tokens_before: before,
                            input_tokens_after: None,
                            summarized_record_count: record_count(&compacted)?,
                            retained_record_count: record_count(&retained)?,
                            error: Some(compaction_error_info(&error)?),
                        },
                    })
                    .await?;
                return Err(error);
            }
        };
        let invocation = generated
            .generator_invocation_id
            .ok_or_else(ContextPlanError::invariant)?;
        let checkpoint_id = DialogueContextCheckpointId::from_uuid(uuid::Uuid::new_v5(
            &invocation.into_uuid(),
            b"dialogue-public-checkpoint",
        ))
        .map_err(|_| ContextPlanError::invariant())?;
        let summary_ref = self
            .content
            .put(
                &input.attempt,
                coverage.tenant_id,
                ContextPlanContentKind::CompactionSummary,
                generated.summary.as_str().as_bytes().to_vec(),
            )
            .await?;
        let source_manifest_ref = self
            .content
            .put(
                &input.attempt,
                coverage.tenant_id,
                ContextPlanContentKind::OwnerSourceManifest,
                self.codec.encode_dialogue_cut(&coverage)?,
            )
            .await?;
        let resolution = ContextCompactionResolution {
            invocation_id: invocation,
            outcome: ContextCompactionOutcome::Succeeded,
            loop_iteration: input.next_iteration,
            context_snapshot_id: None,
            dialogue_checkpoint_id: Some(checkpoint_id),
            input_tokens_before: before,
            input_tokens_after: Some(
                estimate_text_tokens(generated.summary.as_str())
                    .saturating_add(estimate_entries(&retained)),
            ),
            summarized_record_count: record_count(&compacted)?,
            retained_record_count: record_count(&retained)?,
            error: None,
        };
        observe_context_plan_future(
            input.attempt.run_id,
            "dialogue_checkpoint",
            self.plans
                .commit_dialogue_checkpoint(DialogueContextCheckpointCommit {
                    attempt: input.attempt.clone(),
                    checkpoint: DialogueContextCheckpoint {
                        checkpoint_id,
                        coverage,
                        parent_checkpoint_id: window.checkpoint.as_ref().map(|c| c.checkpoint_id),
                        summary_ref,
                        source_manifest_ref,
                        generator_invocation_id: invocation,
                        policy_ref: material
                            .runtime_snapshot
                            .context_policy
                            .compaction_policy_ref
                            .clone(),
                    },
                    source_fence: crate::ContextSourceFence {
                        content_refs: material.source_content_refs.clone(),
                        active_branch_id: material.active_branch_id,
                        ledger_cutoff: material.ledger_cutoff,
                    },
                    summarized_record_ids: compacted
                        .iter()
                        .flat_map(|e| e.source_record_ids.iter().copied())
                        .collect(),
                    resolution: ContextCompactionResolutionCommit {
                        attempt: input.attempt.clone(),
                        resolution,
                    },
                }),
        )
        .await?;
        Ok(true)
    }
}

fn is_dialogue(entry: &ContextEntry) -> bool {
    entry.history_page.is_some()
}
fn ordered_history(material: &ContextMaterial) -> Vec<&ContextEntry> {
    let mut history: Vec<_> = material
        .entries
        .iter()
        .filter(|entry| is_dialogue(entry))
        .collect();
    history.sort_by_key(|e| e.history_page.as_ref().map(|page| page.from_seq));
    history
}
fn validate_coverage(
    entries: &[&ContextEntry],
    cut: &DialogueContextCut,
    through: u64,
) -> Result<(), ContextPlanError> {
    validate_coverage_from(entries, cut, 1, through)
}

fn validate_coverage_from(
    entries: &[&ContextEntry],
    cut: &DialogueContextCut,
    mut next: u64,
    through: u64,
) -> Result<(), ContextPlanError> {
    for entry in entries {
        let page = entry
            .history_page
            .as_ref()
            .ok_or_else(ContextPlanError::invariant)?;
        if &page.cut != cut {
            return Err(ContextPlanError::invariant());
        }
        let (from, to) = (page.from_seq, page.through_seq);
        if from != next || to < from {
            return Err(ContextPlanError::invariant());
        }
        next = to.checked_add(1).ok_or_else(ContextPlanError::invariant)?;
    }
    if next
        != through
            .checked_add(1)
            .ok_or_else(ContextPlanError::invariant)?
    {
        return Err(ContextPlanError::invariant());
    }
    Ok(())
}

pub(super) fn validate_primary_cut(
    material: &ContextMaterial,
    cut: Option<&DialogueContextCut>,
) -> Result<(), ContextPlanError> {
    if let Some(context) = &material.dialogue_execution_context {
        super::authority::execution::require_scope(context, cut, material.run_context.run_id)?;
    }
    if let Some(context) = &material.dialogue_approval_context {
        super::authority::approval::require_cut(context, cut)?;
    }
    let dialogue_snapshot = material
        .runtime_snapshot
        .prompt
        .sections
        .iter()
        .filter(|s| {
            matches!(
                s.section_key.as_str(),
                "dialogue.source" | "agent.work-item-source" | "owner.goal-source"
            )
        })
        .count();
    // Direct input and delegated Task/report use distinct sealed source types.
    // Neither missing nor ambiguous authority may authorize a Dialogue cut.
    let derived_goal = material.derived_source_run_id.is_some()
        && material.runtime_snapshot.owner_goal_authority.is_some();
    if if derived_goal {
        dialogue_snapshot > 1 || cut.is_some()
    } else {
        dialogue_snapshot != usize::from(cut.is_some())
    } {
        return Err(ContextPlanError::invariant());
    }
    if let Some(authority) = &material.runtime_snapshot.owner_goal_authority {
        let closed_evaluation = material.run_context.activity_kind
            == musteragent_activity::ActivityKind::Evaluation
            && material.runtime_snapshot.uses_closed_evaluation_profile();
        // The sealed Goal authority still fences account and current source
        // access. It does not give the evidence Judge a public-history window.
        let valid = if closed_evaluation || derived_goal {
            cut.is_none()
                && material.dialogue_execution_context.is_none()
                && material.dialogue_approval_context.is_none()
                && material.agent_instructions.is_none()
                && !material.entries.iter().any(is_dialogue)
        } else {
            cut == Some(&authority.context_cut)
        };
        if !valid {
            return Err(ContextPlanError::invariant());
        }
    }
    if let Some(cut) = cut {
        let entries = ordered_history(material);
        validate_coverage(&entries, cut, cut.through.seq)?;
    }
    Ok(())
}

pub(super) fn validate_input_capacity(
    material: &ContextMaterial,
    request: &CanonicalModelRequest,
) -> Result<(), ContextPlanError> {
    // Input custody budgets apply to the Dialogue conversation surface.
    if material.runtime_snapshot.uses_dialogue_admission() {
        validate_dialogue_input_capacity(&request.route, &request.input)?;
    }
    Ok(())
}
pub(super) fn validate_dialogue_input_capacity(
    route: &ModelRoute,
    input: &[CanonicalMessage],
) -> Result<(), ContextPlanError> {
    let parts = input.iter().flat_map(|message| &message.content);
    let mut images = 0_usize;
    let mut bytes = 0_u64;
    for part in parts {
        match part {
            CanonicalContentPart::Image(reference) => {
                if !route.accepts_native_image() {
                    return Err(ContextPlanError::invariant());
                }
                let profile = VisionInputProfile::from_route(route)
                    .map_err(|_| ContextPlanError::invariant())?;
                images = images.checked_add(1).ok_or_else(ContextPlanError::quota)?;
                bytes = bytes
                    .checked_add(reference.byte_length)
                    .ok_or_else(ContextPlanError::quota)?;
                if images > profile.max_images_per_request
                    || bytes > profile.max_total_image_bytes
                    || reference.byte_length > profile.max_bytes_per_image
                {
                    return Err(super::capacity::exceeded());
                }
            }
            CanonicalContentPart::Audio(_) if !route.accepts_native_audio() => {
                return Err(ContextPlanError::invariant());
            }
            CanonicalContentPart::Video(_) if !route.accepts_native_video() => {
                return Err(ContextPlanError::invariant());
            }
            CanonicalContentPart::File(reference)
                if !route.accepts_native_document(reference.media_type.as_str()) =>
            {
                return Err(ContextPlanError::invariant());
            }
            _ => {}
        }
    }
    Ok(())
}

/// Migrate the model view of an immutable legacy source pin. Its profile is
/// a single JSON-encoded line and cannot introduce control lines of its own.
pub(super) fn read_sealed_profile(pin: &str) -> Result<ModelText, ContextPlanError> {
    let (_, tail) = pin
        .split_once("\n<agent-profile-data>\n")
        .ok_or_else(ContextPlanError::invariant)?;
    let (profile, suffix) = tail
        .split_once("\n</agent-profile-data>")
        .ok_or_else(ContextPlanError::invariant)?;
    let data: serde_json::Value =
        serde_json::from_str(profile).map_err(|_| ContextPlanError::invariant())?;
    let name = data["name"]
        .as_str()
        .ok_or_else(ContextPlanError::invariant)?;
    let responsibility = data["responsibility"]
        .as_str()
        .ok_or_else(ContextPlanError::invariant)?;
    let mut text = format!(
        "Agent profile\n{}",
        serde_json::json!({ "name":name, "responsibility":responsibility })
    );
    if pin
        .lines()
        .any(|line| line.starts_with("<capability-continuation "))
    {
        text.push_str("\nContinue the original request using the currently available tools. The capability request has already been resolved.");
    }
    for line in suffix.lines() {
        if line.starts_with("<user-task-target ")
            || line.starts_with("The sending computer is the environment ")
        {
            text.push('\n');
            text.push_str(line);
        }
    }
    ModelText::new(text).map_err(|_| ContextPlanError::invariant())
}

/// Older release snapshots also use a sealed source manifest for validation.
/// Only task coordinates accepted by a tool and the actual business rules enter the request.
pub(super) fn task_source_instructions(
    section: &str,
    source: ModelText,
) -> Result<ModelText, ContextPlanError> {
    let raw = source.as_str();
    let invalid = ContextPlanError::invariant;
    let json_section = |open: &str, close: &str| -> Result<serde_json::Value, ContextPlanError> {
        let text = raw
            .split_once(open)
            .and_then(|(_, tail)| tail.rsplit_once(close))
            .map(|(value, _)| value)
            .ok_or_else(invalid)?;
        serde_json::from_str(text).map_err(|_| invalid())
    };
    let result = match section {
        "owner.goal-source" if raw.starts_with("<owner-goal-source>") => {
            let facts = json_section("<source-data>", "</source-data>")?;
            let facts = facts.as_object().ok_or_else(invalid)?;
            let mut rules = serde_json::Map::new();
            for field in [
                "goal_id",
                "profile",
                "constraints",
                "success_criteria",
                "evidence_requirements",
                "failure_conditions",
                "minimum_evidence_items",
            ] {
                rules.insert(
                    field.to_owned(),
                    facts.get(field).ok_or_else(invalid)?.clone(),
                );
            }
            format!(
                "Continue the ongoing goal using its objective and current input. Treat external events as reference material. Report progress using the available goal tools.\n{}",
                serde_json::Value::Object(rules)
            )
        }
        "agent.work-item-source" if raw.starts_with("<work-item-source ") => {
            let header = raw.lines().next().ok_or_else(invalid)?;
            let attr = |name: &str| {
                header
                    .split_once(&format!(" {name}=\u{22}"))
                    .and_then(|(_, tail)| tail.split_once('"'))
                    .map(|(value, _)| value)
            };
            let mut task = serde_json::Map::new();
            for (name, coordinate) in [
                ("task", "task_id"),
                ("goal", "goal_revision"),
                ("control", "control_revision"),
                ("assignment", "assignment_epoch"),
            ] {
                task.insert(
                    coordinate.into(),
                    serde_json::Value::String(attr(name).ok_or_else(invalid)?.into()),
                );
            }
            let instruction = if let Some(outcome) = attr("outcome") {
                task.insert("outcome".into(), outcome.into());
                "Review the returned result against the original objective and success criteria."
            } else {
                "Execute the delegated objective. Use its success criteria and the supplied prerequisite results."
            };
            let profile = json_section("<profile-data>", "</profile-data>")?;
            format!(
                "{instruction}\nTask {}\nAgent profile {}",
                serde_json::Value::Object(task),
                profile
            )
        }
        _ => return Ok(source),
    };
    ModelText::new(result).map_err(|_| invalid())
}
