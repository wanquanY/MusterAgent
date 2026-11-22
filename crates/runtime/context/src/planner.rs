use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;
mod authority;
mod capacity;
mod compaction;
mod compaction_chunks;
mod compaction_resolution;
mod composition;
mod source_baseline;
mod summary_projection;
use summary_projection::build_summary;
mod delegation_completion;
mod delivery_repair;
pub use delivery_repair::validate_public_delivery_repair_request;
mod dialogue;
mod dialogue_images;
mod file_tool_guidance;
mod incremental_material;
mod instruction_rendering;
mod material_validation;
mod model_capacity;
mod native_publication;
mod observation;
mod output_continuation;
mod planning_budget;
mod preparation;
mod route_presentation;
pub use route_presentation::present_file_read_parts_for_route;
mod terminal_tool_guidance;
mod tool_availability;
mod turn_order;

use crate::{
    CONTEXT_PLAN_GENERATION, ContextCodec, ContextCompactionFailureCommit,
    ContextCompactionOutcome, ContextCompactionRequest, ContextCompactionResolution,
    ContextCompactionResolutionCommit, ContextCompactionSlotV1, ContextCompactionStartCommit,
    ContextCompactor, ContextEntry, ContextMaterial, ContextMaterialSource, ContextPlanCommit,
    ContextPlanError, ContextPlanInput, ContextPlanProfile, ContextPlanStore, ContextPlanner,
    ContextSummaryDocument, ContextSummaryItem, ContextToolPolicy, MaterializedContextBaseline,
    ModelIteration, NoopRunProgressCoordinator, RunProgressCoordinator,
};
use crate::{ContextPlanContentKind, ContextPlanContentWriter};
use musteragent_model::{
    AuxiliaryModelRole, CredentialGrantRef, ModelRoute, ProviderStatePolicy, VisionInputProfile,
};
use musteragent_run::{ExecutionPlanDocument, InvocationContext};
use musteragent_runtime_content::{ContentStore, verify_content};
use musteragent_runtime_memory::{render_instinct_context, render_memory_context};
use musteragent_runtime_model::{
    CachePolicy, CanonicalContentPart, CanonicalMessage, CanonicalModelRequest,
    CanonicalToolDefinition, ModelInvocationRouteRole, ModelText,
};
use musteragent_runtime_snapshot::{PinnedAuxiliaryModelRoute, PinnedModelRoute};

use crate::compaction_slot::{
    CompactionPlanFingerprintInput, compaction_plan_fingerprint, last_causation_id,
};
use crate::iteration_identity::IterationIds;
use crate::plan_checksum::build_logical_context_snapshot;
use crate::provider_context::target_route_owns_usage_anchor;
use crate::token_estimate::{
    estimate_instruction_tokens, estimate_message, estimate_messages,
    estimate_runtime_event_tokens, estimate_text_tokens, estimate_tool_tokens,
};
use compaction_resolution::{
    GeneratedSummary, PendingCompactionResolution, SummaryGeneration, compaction_error_info,
    record_count,
};
use delegation_completion::delegation_completion_events;
use instruction_rendering::{
    append_empty_response_recovery_instruction, append_tool_surface_authority_instruction,
    model_action_rejection_turn, render_execution_plan, text_from_content, text_from_skill_content,
};
use material_validation::validate_material;
use observation::{
    log_context_plan_error, observe_context_plan_future, observe_context_plan_stage,
};
use planning_budget::{
    can_fallback_to_uncompressed, compaction_summary_budget, estimate_context_tokens,
    estimate_entries, output_budget, planning_budgets, reasoning_policy, temperature,
};
use route_presentation::present_messages_for_route_with_vision_tool;
#[cfg(test)]
use route_presentation::{present_message_for_route, present_messages_for_route};
use turn_order::order_entries_for_run_turn;
const MAX_SUMMARY_ITEMS: usize = 128;
const MAX_SUMMARY_EXCERPT_CHARS: usize = 256;
const MIN_RECENT_TOOL_RESULTS: usize = 3;
#[derive(Clone)]
pub struct DurableContextPlanner {
    projection: Option<Arc<dyn crate::ContextViewProjection>>,
    source: Arc<dyn ContextMaterialSource>,
    content: Arc<dyn ContextPlanContentWriter>,
    material_cache: Option<Arc<musteragent_runtime_content::VerifiedContentCache>>,
    plans: Arc<dyn ContextPlanStore>,
    codec: Arc<dyn ContextCodec>,
    compactor: Arc<dyn ContextCompactor>,
    progress: Arc<dyn RunProgressCoordinator>,
}

impl ContextPlanner for DurableContextPlanner {
    fn prepare_model_iteration_with_projection(
        &self,
        input: ContextPlanInput,
        profile: Option<ContextPlanProfile>,
        projection: Arc<dyn crate::ContextViewProjection>,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>>
    {
        let mut planner = self.clone();
        planner.projection = Some(projection);
        Box::pin(async move { planner.prepare(input, profile).await })
    }

    fn prepare_model_iteration(
        &self,
        input: ContextPlanInput,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>>
    {
        let run_id = input.attempt.run_id;
        // Own the planner state before wrapping it in observation. Moving the
        // entire preparation Future through nested tracing futures multiplies
        // the stack required to enter planning on a bounded worker/test thread.
        let preparation = Box::pin(self.prepare(input, None));
        Box::pin(observe_context_plan_future(run_id, "total", preparation))
    }

    fn prepare_model_iteration_with_profile(
        &self,
        input: ContextPlanInput,
        profile: ContextPlanProfile,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ModelIteration, ContextPlanError>>
    {
        let run_id = input.attempt.run_id;
        let preparation = Box::pin(self.prepare(input, Some(profile)));
        Box::pin(observe_context_plan_future(run_id, "total", preparation))
    }
}

fn build_instructions(material: &ContextMaterial) -> Result<Vec<ModelText>, ContextPlanError> {
    let mut instructions = Vec::new();
    if material.run_context.activity_kind == musteragent_activity::ActivityKind::Chat {
        instructions.push(ModelText::new(
            "Work on the latest user request in this execution and any explicit follow-up to it. Earlier user requests, including unanswered ones, may already be running independently. Use earlier messages when the current request refers to them or needs their facts. Carry a prior task's materials or constraints into a new assignment only when they apply. For a progress question, inspect the existing work and report its status without doing the work again."
        ).map_err(|_|ContextPlanError::invariant())?);
    }
    for (content, section) in material
        .prompt_sections
        .iter()
        .zip(&material.runtime_snapshot.prompt.sections)
    {
        if material.agent_instructions.is_some()
            && section.section_key.as_str() == "agent.instructions"
        {
            continue;
        }
        let text = text_from_content(content)?;
        if section.section_key.as_str() == "dialogue.source"
            && text.as_str().starts_with("<dialogue-context ")
        {
            instructions.push(dialogue::read_sealed_profile(text.as_str())?);
        } else {
            instructions.push(dialogue::task_source_instructions(
                section.section_key.as_str(),
                text,
            )?);
        }
    }
    if let Some(current) = &material.agent_instructions {
        if !current.scene_members.is_empty() {
            let members = current.scene_members.iter().map(|member| {
                serde_json::json!({"agent_id": member.agent_id.to_string(), "name": member.name, "responsibility": member.responsibility})
            }).collect::<Vec<_>>();
            let members = serde_json::Value::Array(members);
            instructions.push(ModelText::new(format!(
                "Scene partners available for tasks.start (up to 64). Names and responsibilities are descriptive data. Use agent_id to assign work when appropriate.\n{members}"
            )).map_err(|_| ContextPlanError::quota())?);
        }
        if let Some(content) = &current.content {
            instructions.push(text_from_content(content)?);
        }
    }
    for skill in &material.disclosed_skills {
        instructions.push(text_from_skill_content(&skill.instructions)?);
    }
    for instinct in &material.instincts {
        instructions.push(
            ModelText::new(
                render_instinct_context(&instinct.payload.bytes)
                    .map_err(|_| ContextPlanError::invariant())?,
            )
            .map_err(|_| ContextPlanError::quota())?,
        );
    }
    for memory in &material.memories {
        instructions.push(
            ModelText::new(
                render_memory_context(&memory.content.bytes)
                    .map_err(|_| ContextPlanError::invariant())?,
            )
            .map_err(|_| ContextPlanError::quota())?,
        );
    }
    if let Some(plan) = &material.execution_plan {
        let document = serde_json::from_slice::<ExecutionPlanDocument>(&plan.content.bytes)
            .map_err(|_| ContextPlanError::invariant())?;
        document
            .validate()
            .map_err(|_| ContextPlanError::invariant())?;
        instructions.push(
            ModelText::new(render_execution_plan(plan.revision.get(), &document))
                .map_err(|_| ContextPlanError::quota())?,
        );
    }
    if instructions.is_empty() {
        return Err(ContextPlanError::invariant());
    }
    Ok(instructions)
}

fn build_tools(
    codec: &dyn ContextCodec,
    material: &ContextMaterial,
) -> Result<Vec<CanonicalToolDefinition>, ContextPlanError> {
    if material.tool_policy == ContextToolPolicy::DenyAll {
        if material.exposed_tool_keys.is_empty() {
            return Ok(Vec::new());
        }
        return Err(ContextPlanError::invariant());
    }
    let names = musteragent_discovery::ModelToolNames::new(
        material
            .runtime_snapshot
            .tools
            .iter()
            .map(|tool| (&tool.manifest.key, &tool.manifest.wire_name)),
    )
    .map_err(|_| ContextPlanError::invariant())?;
    let progress_notes = crate::progress_note::enabled(material);
    material
        .exposed_tool_keys
        .iter()
        .map(|tool_key| {
            let (tool, resolved_name) = crate::resolved_tools::exposed_tool(
                &material.runtime_snapshot.tools,
                &material.resolved_tools,
                tool_key,
            )?;
            let definition = CanonicalToolDefinition {
                key: tool.manifest.key.clone(),
                model_name: resolved_name
                    .or_else(|| names.name(&tool.manifest.key))
                    .cloned()
                    .ok_or_else(ContextPlanError::invariant)?,
                name: tool.manifest.wire_name.clone(),
                description: ModelText::new(tool.description.clone())
                    .map_err(|_| ContextPlanError::invariant())?,
                input_schema_ref: tool.manifest.input_schema.clone(),
                input_schema: {
                    let mut schema = crate::tool_schema_view::model_schema(
                        codec.decode_json(&tool.input_schema_json)?,
                    );
                    if progress_notes {
                        crate::progress_note::inject(tool.manifest.key.as_str(), &mut schema);
                    }
                    schema
                },
                strict: true,
            };
            definition.validate().map_err(|error| {
                tracing::error!(
                    run_id = %material.run_context.run_id,
                    tool_key = definition.key.as_str(),
                    wire_name = definition.name.as_str(),
                    schema_ref = definition.input_schema_ref.schema_key.as_str(),
                    error = ?error,
                    "Context Planner rejected an exposed Tool contract",
                );
                ContextPlanError::invariant()
            })?;
            Ok(definition)
        })
        .collect()
}

fn select_provider_input<'a>(
    input: &ContextPlanInput,
    material: &'a ContextMaterial,
    logical_entries: &[&'a ContextEntry],
) -> Result<Vec<&'a ContextEntry>, ContextPlanError> {
    if input.continuation_ref.is_none()
        || material.runtime_snapshot.model.route.provider_state_policy
            == ProviderStatePolicy::Stateless
    {
        return Ok(logical_entries.to_vec());
    }
    let anchor = input
        .continuation_source_invocation_id
        .ok_or_else(ContextPlanError::invariant)?;
    let selected = continuation_entries(&material.entries, anchor);
    if selected.is_empty() {
        Err(ContextPlanError::invariant())
    } else {
        Ok(selected)
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ProgressObservationKey {
    calls: Vec<ProgressObservationCallKey>,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ProgressObservationCallKey {
    wire_name: String,
    handles: Vec<String>,
}

/// Projects repeated asynchronous-child observations into semantic Context.
///
/// Ledger and Tool stores retain every invocation and result. This projection
/// only removes an older *complete atomic exchange* when a newer pure
/// `delegate_join`/`delegate_inspect` exchange observes the same ordered
/// handles. Calls with visible Assistant text, reasoning, side effects or a
/// different target set remain independent history.
fn coalesce_progress_observations<'entry>(
    entries: &[&'entry ContextEntry],
) -> Result<Vec<&'entry ContextEntry>, ContextPlanError> {
    let units = atomic_context_units(entries, false)?;
    let mut keep = vec![true; entries.len()];
    let mut newest = BTreeSet::<ProgressObservationKey>::new();
    for unit in units.iter().rev() {
        let Some(key) = progress_observation_key(&entries[unit.start].message) else {
            continue;
        };
        if !newest.insert(key) {
            keep[unit.start..unit.end].fill(false);
        }
    }
    Ok(entries
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(index, entry)| keep[index].then_some(entry))
        .collect())
}

fn progress_observation_key(message: &CanonicalMessage) -> Option<ProgressObservationKey> {
    if message.role != musteragent_runtime_model::CanonicalRole::Assistant
        || !message.content.is_empty()
        || message.reasoning.is_some()
        || message.tool_calls.is_empty()
    {
        return None;
    }
    let calls = message
        .tool_calls
        .iter()
        .map(|call| {
            let wire_name = call.name.as_str();
            if !matches!(wire_name, "delegate_join" | "delegate_inspect") {
                return None;
            }
            let object = call.arguments.as_object().ok()?;
            // Explicit history pages are task evidence, not replaceable status
            // observations. Preserve every view, handle, record and offset.
            if wire_name == "delegate_inspect" && object.keys().any(|key| key != "handles") {
                return None;
            }
            let handles = match object.get("handles") {
                None if wire_name == "delegate_inspect" => Vec::new(),
                Some(musteragent_runtime_model::JsonValue::Array(values)) => values
                    .iter()
                    .map(|value| match value {
                        musteragent_runtime_model::JsonValue::String(value) => Some(value.clone()),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?,
                _ => return None,
            };
            Some(ProgressObservationCallKey {
                wire_name: wire_name.to_owned(),
                handles,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(ProgressObservationKey { calls })
}

fn continuation_entries(
    entries: &[ContextEntry],
    anchor: musteragent_identity::InvocationId,
) -> Vec<&ContextEntry> {
    entries
        .iter()
        .filter(|entry| entry.continuation_delta_for == Some(anchor))
        .collect()
}

#[derive(Debug, Copy, Clone)]
struct CompactionSelectionBudgets {
    hard_input_budget: u64,
    physical_trigger_tokens: u64,
    deterministic_uncompressed_tokens: u64,
    soft_trigger_budget: u64,
    recent_tail_budget: Option<u64>,
}

fn compact_entries<'a>(
    entries: &[&'a ContextEntry],
    prefix_tokens: u64,
    budgets: CompactionSelectionBudgets,
    allow_leading_tool_results: bool,
) -> Result<(Vec<&'a ContextEntry>, Vec<&'a ContextEntry>), ContextPlanError> {
    if budgets.physical_trigger_tokens <= budgets.soft_trigger_budget
        && budgets.deterministic_uncompressed_tokens <= budgets.hard_input_budget
    {
        return Ok((entries.to_vec(), Vec::new()));
    }
    let available = budgets
        .hard_input_budget
        .checked_sub(prefix_tokens)
        .ok_or_else(ContextPlanError::quota)?;
    // The pinned v1 policy retains approximately half of the model input
    // budget as an exact recent tail. The other half remains available for
    // the structured checkpoint. Growing this to 75% silently defeated the
    // documented 70/50 policy and made the next turn immediately compact
    // again on realistic conversations.
    let retained_budget = budgets.recent_tail_budget.unwrap_or(available / 2);
    if retained_budget > available {
        return Err(ContextPlanError::quota());
    }
    let units = atomic_context_units(entries, allow_leading_tool_results)?;
    let required_start = recent_tool_result_protection_start(entries, &units);
    let mut retained_tokens = 0_u64;
    let mut split = entries.len();
    for unit in units.iter().rev() {
        let tokens = entries[unit.start..unit.end]
            .iter()
            .fold(0_u64, |total, entry| {
                total.saturating_add(entry.history_page.as_ref().map_or_else(
                    || estimate_message(&entry.message),
                    |page| page.history.estimated_tokens(),
                ))
            });
        let next_tokens = retained_tokens.saturating_add(tokens);
        let newest_atomic_unit = split == entries.len();
        let protected_recent_tool_result = unit.start >= required_start;
        if next_tokens > available {
            if newest_atomic_unit || protected_recent_tool_result {
                return Err(ContextPlanError::quota());
            }
            break;
        }
        if next_tokens > retained_budget && !newest_atomic_unit && !protected_recent_tool_result {
            break;
        }
        retained_tokens = next_tokens;
        split = unit.start;
    }
    if split == 0 || split == entries.len() {
        return Err(ContextPlanError::quota());
    }
    Ok((entries[split..].to_vec(), entries[..split].to_vec()))
}

fn recent_tool_result_protection_start(entries: &[&ContextEntry], units: &[ContextUnit]) -> usize {
    let mut protected_result_index = None;
    let mut seen = 0_usize;
    for (index, entry) in entries.iter().enumerate().rev() {
        if entry.message.role == musteragent_runtime_model::CanonicalRole::Tool {
            seen = seen.saturating_add(1);
            protected_result_index = Some(index);
            if seen == MIN_RECENT_TOOL_RESULTS {
                break;
            }
        }
    }
    protected_result_index
        .and_then(|index| {
            units
                .iter()
                .find(|unit| unit.start <= index && index < unit.end)
                .map(|unit| unit.start)
        })
        .unwrap_or(entries.len())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct ContextUnit {
    start: usize,
    end: usize,
}

fn atomic_context_units(
    entries: &[&ContextEntry],
    allow_leading_tool_results: bool,
) -> Result<Vec<ContextUnit>, ContextPlanError> {
    let mut units = Vec::new();
    let mut index = 0;
    while index < entries.len() {
        let message = &entries[index].message;
        if message.role == musteragent_runtime_model::CanonicalRole::Tool {
            if !allow_leading_tool_results || index != 0 {
                return Err(ContextPlanError::invariant());
            }
            let start = index;
            while index < entries.len()
                && entries[index].message.role == musteragent_runtime_model::CanonicalRole::Tool
            {
                index += 1;
            }
            units.push(ContextUnit { start, end: index });
            continue;
        }
        let start = index;
        index += 1;
        if message.role == musteragent_runtime_model::CanonicalRole::Assistant
            && !message.tool_calls.is_empty()
        {
            let mut remaining = message
                .tool_calls
                .iter()
                .map(|call| call.call_id.clone())
                .collect::<BTreeSet<_>>();
            while index < entries.len()
                && entries[index].message.role == musteragent_runtime_model::CanonicalRole::Tool
            {
                let call_id = entries[index]
                    .message
                    .tool_call_id
                    .as_ref()
                    .ok_or_else(ContextPlanError::invariant)?;
                if !remaining.remove(call_id) {
                    return Err(ContextPlanError::invariant());
                }
                index += 1;
            }
            if !remaining.is_empty() {
                return Err(ContextPlanError::invariant());
            }
        }
        units.push(ContextUnit { start, end: index });
    }
    Ok(units)
}

struct ResolvedContextBaseline<'material, 'summary> {
    context: Option<&'material MaterializedContextBaseline>,
    summary: Option<&'summary ContextSummaryDocument>,
    entries: Vec<&'material ContextEntry>,
}

fn resolve_context_baseline<'material, 'summary>(
    previous_context: Option<&'material MaterializedContextBaseline>,
    logical_entries: &[&'material ContextEntry],
    previous_summary: Option<&'summary ContextSummaryDocument>,
) -> Result<ResolvedContextBaseline<'material, 'summary>, ContextPlanError> {
    let Some(context) = previous_context else {
        if previous_summary.is_some() {
            return Err(ContextPlanError::invariant());
        }
        return Ok(ResolvedContextBaseline {
            context: None,
            summary: None,
            entries: logical_entries.to_vec(),
        });
    };
    let summary = previous_summary.ok_or_else(ContextPlanError::invariant)?;
    if !summary.compacted
        || summary
            .summary
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || summary.summarized_record_ids.is_empty()
        || context.retained_record_ids.is_empty()
    {
        return Err(ContextPlanError::invariant());
    }
    let summarized = summary
        .summarized_record_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let retained = context
        .retained_record_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if summarized.len() != summary.summarized_record_ids.len()
        || retained.len() != context.retained_record_ids.len()
        || !summarized.is_disjoint(&retained)
    {
        return Err(ContextPlanError::invariant());
    }
    let mut entries = Vec::new();
    for entry in logical_entries {
        let summarized_count = entry
            .source_record_ids
            .iter()
            .filter(|record_id| summarized.contains(record_id))
            .count();
        let retained_count = entry
            .source_record_ids
            .iter()
            .filter(|record_id| retained.contains(record_id))
            .count();
        if summarized_count > 0 && summarized_count != entry.source_record_ids.len()
            || retained_count > 0 && retained_count != entry.source_record_ids.len()
            || summarized_count > 0 && retained_count > 0
        {
            return Err(ContextPlanError::invariant());
        }
        if summarized_count == entry.source_record_ids.len() {
            continue;
        }
        if retained_count == entry.source_record_ids.len()
            || entry.source_sequence > context.source_seq_to
        {
            entries.push(*entry);
        }
        // Records at or before the checkpoint which were neither summarized
        // nor retained were already excluded by the semantic projection (for
        // example a superseded progress observation). They stay omitted.
    }
    if entries.is_empty() {
        return Err(ContextPlanError::invariant());
    }
    Ok(ResolvedContextBaseline {
        context: Some(context),
        summary: Some(summary),
        entries,
    })
}

fn rolling_summary_source_tokens(
    previous: Option<&ContextSummaryDocument>,
    compacted: &[&ContextEntry],
) -> u64 {
    previous
        .and_then(|summary| summary.summary.as_deref())
        .map_or(0, estimate_text_tokens)
        .saturating_add(estimate_entries(compacted))
}

fn collect_file_operations(
    material: &ContextMaterial,
    compacted: &[&ContextEntry],
    previous: Option<&ContextSummaryDocument>,
) -> Result<crate::ContextFileOperations, ContextPlanError> {
    let (mut read, mut written, mut edited) = accumulated_file_operations(previous);
    let mut projections = BTreeMap::new();
    for tool in &material.runtime_snapshot.tools {
        let Some(metadata) = tool.file_operation.as_ref() else {
            continue;
        };
        let default_identity = file_identity_default_from_schema(
            &tool.input_schema_json,
            &metadata.identity_argument,
        )?;
        if projections
            .insert(
                tool.manifest.wire_name.as_str(),
                (metadata, default_identity),
            )
            .is_some()
        {
            return Err(ContextPlanError::invariant());
        }
    }
    for call in compacted
        .iter()
        .flat_map(|entry| entry.message.tool_calls.iter())
    {
        let Some((metadata, default_identity)) = projections.get(call.name.as_str()) else {
            continue;
        };
        record_file_operation(
            metadata,
            &call.arguments,
            default_identity.as_deref(),
            &mut read,
            &mut written,
            &mut edited,
        )?;
    }
    Ok(crate::ContextFileOperations {
        read: read.into_iter().collect(),
        written: written.into_iter().collect(),
        edited: edited.into_iter().collect(),
    })
}

fn accumulated_file_operations(
    previous: Option<&ContextSummaryDocument>,
) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    previous.map_or_else(
        || (BTreeSet::new(), BTreeSet::new(), BTreeSet::new()),
        |summary| {
            (
                summary.file_operations.read.iter().cloned().collect(),
                summary.file_operations.written.iter().cloned().collect(),
                summary.file_operations.edited.iter().cloned().collect(),
            )
        },
    )
}

fn record_file_operation(
    metadata: &musteragent_tool::FileOperationMetadata,
    arguments: &musteragent_runtime_model::JsonValue,
    default_identity: Option<&str>,
    read: &mut BTreeSet<String>,
    written: &mut BTreeSet<String>,
    edited: &mut BTreeSet<String>,
) -> Result<(), ContextPlanError> {
    let identity = match json_pointer(arguments, &metadata.identity_argument) {
        Some(value) => json_identity(value).ok_or_else(ContextPlanError::invariant)?,
        None => default_identity
            .map(str::to_owned)
            .ok_or_else(ContextPlanError::invariant)?,
    };
    match metadata.kind {
        musteragent_tool::FileOperationKind::Read => read.insert(identity),
        musteragent_tool::FileOperationKind::Write => written.insert(identity),
        musteragent_tool::FileOperationKind::Edit => edited.insert(identity),
    };
    Ok(())
}

/// Resolves the identity default from the immutable input schema pinned in the
/// Run snapshot. JSON Schema defaults are annotations rather than argument
/// mutations, so a valid Tool call may omit the field while the executor still
/// applies its declared default. Context projection must use the same pinned
/// contract instead of treating the unmaterialized argument as corruption.
fn file_identity_default_from_schema(
    schema_bytes: &[u8],
    identity_argument: &str,
) -> Result<Option<String>, ContextPlanError> {
    let root = serde_json::from_slice::<serde_json::Value>(schema_bytes)
        .map_err(|_| ContextPlanError::invariant())?;
    let Some(schema) = schema_for_instance_pointer(&root, identity_argument) else {
        return Ok(None);
    };
    let Some(default) = schema.get("default") else {
        return Ok(None);
    };
    schema_json_identity(default)
        .map(Some)
        .ok_or_else(ContextPlanError::invariant)
}

fn schema_for_instance_pointer<'a>(
    root: &'a serde_json::Value,
    pointer: &str,
) -> Option<&'a serde_json::Value> {
    pointer_tokens(pointer)
        .try_fold(root, |schema, token| {
            resolve_local_schema_ref(root, schema)
                .get("properties")
                .and_then(|properties| properties.get(&token))
                .or_else(|| {
                    token.parse::<usize>().ok().and_then(|index| {
                        resolve_local_schema_ref(root, schema)
                            .get("prefixItems")
                            .and_then(|items| items.get(index))
                    })
                })
                .or_else(|| {
                    token
                        .parse::<usize>()
                        .ok()
                        .and_then(|_| resolve_local_schema_ref(root, schema).get("items"))
                })
        })
        .map(|schema| resolve_local_schema_ref(root, schema))
}

fn resolve_local_schema_ref<'a>(
    root: &'a serde_json::Value,
    schema: &'a serde_json::Value,
) -> &'a serde_json::Value {
    schema
        .get("$ref")
        .and_then(serde_json::Value::as_str)
        .and_then(|reference| reference.strip_prefix('#'))
        .and_then(|pointer| root.pointer(pointer))
        .unwrap_or(schema)
}

fn pointer_tokens(pointer: &str) -> impl Iterator<Item = String> + '_ {
    pointer
        .split('/')
        .skip(1)
        .map(|token| token.replace("~1", "/").replace("~0", "~"))
}

fn json_pointer<'a>(
    value: &'a musteragent_runtime_model::JsonValue,
    pointer: &str,
) -> Option<&'a musteragent_runtime_model::JsonValue> {
    pointer_tokens(pointer).try_fold(value, |current, token| match current {
        musteragent_runtime_model::JsonValue::Object(object) => object.get(&token),
        musteragent_runtime_model::JsonValue::Array(array) => token
            .parse::<usize>()
            .ok()
            .and_then(|index| array.get(index)),
        _ => None,
    })
}

fn json_identity(value: &musteragent_runtime_model::JsonValue) -> Option<String> {
    match value {
        musteragent_runtime_model::JsonValue::String(value) if !value.trim().is_empty() => {
            Some(value.clone())
        }
        musteragent_runtime_model::JsonValue::Integer(value) => Some(value.to_string()),
        _ => None,
    }
}

fn schema_json_identity(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        serde_json::Value::Number(value) if value.is_i64() || value.is_u64() => {
            Some(value.to_string())
        }
        _ => None,
    }
}

/// Checkpoint prose retains reference-data priority, including when it quotes
/// an older instruction. Opaque continuations inherit their existing prefix.
fn attach_history_summary(
    messages: &mut Vec<CanonicalMessage>,
    summary: Option<&str>,
) -> Result<(), ContextPlanError> {
    let Some(text) = summary else {
        return Ok(());
    };
    let mut parts: Vec<_> = ModelText::chunked(text)
        .map_err(|_| ContextPlanError::invariant())?
        .into_iter()
        .map(CanonicalContentPart::Text)
        .collect();
    if let Some(user) = messages
        .iter_mut()
        .find(|m| m.role == musteragent_runtime_model::CanonicalRole::User)
    {
        parts.append(&mut user.content);
        user.content = parts;
    } else {
        // Background activity can start from a delegated or tool stimulus.
        // The provider receives a labeled data item; no ledger message is created.
        messages.insert(
            0,
            CanonicalMessage {
                role: musteragent_runtime_model::CanonicalRole::User,
                content: parts,
                tool_calls: vec![],
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
        );
    }
    Ok(())
}

fn render_history_summary(
    summary: &ContextSummaryDocument,
    token_budget: u64,
    deterministic_token_budget: bool,
) -> Result<String, ContextPlanError> {
    let narrative = summary
        .summary
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ContextPlanError::invariant)?;
    let text = format!("Historical reference (summary):\n{narrative}");
    let exceeds_budget = if deterministic_token_budget {
        estimate_text_tokens(&text) > token_budget
    } else {
        // @1 intentionally retains its historical byte-count check.
        text.len() > usize::try_from(token_budget).unwrap_or(usize::MAX)
    };
    if exceeds_budget {
        return Err(ContextPlanError::quota());
    }
    Ok(text)
}

fn message_excerpt(message: &CanonicalMessage) -> String {
    let mut excerpt = String::new();
    for part in &message.content {
        match part {
            CanonicalContentPart::Text(text) => {
                if !excerpt.is_empty() {
                    excerpt.push(' ');
                }
                excerpt.extend(
                    text.as_str()
                        .chars()
                        .take(MAX_SUMMARY_EXCERPT_CHARS.saturating_sub(excerpt.chars().count())),
                );
            }
            CanonicalContentPart::Image(_) => excerpt.push_str("[image]"),
            CanonicalContentPart::Audio(_) => excerpt.push_str("[audio]"),
            CanonicalContentPart::Video(_) => excerpt.push_str("[video]"),
            CanonicalContentPart::File(_) => excerpt.push_str("[file]"),
        }
        if excerpt.chars().count() >= MAX_SUMMARY_EXCERPT_CHARS {
            break;
        }
    }
    if !message.tool_calls.is_empty() {
        excerpt.push_str(" [tool calls]");
    }
    excerpt
}

fn active_model_route(
    material: &ContextMaterial,
    ordinal: u32,
) -> Result<(&ModelRoute, &CredentialGrantRef), ContextPlanError> {
    if ordinal == 0 {
        return Ok((
            &material.runtime_snapshot.model.route,
            &material.runtime_snapshot.model.credential_grant_ref,
        ));
    }
    let index =
        usize::try_from(ordinal.saturating_sub(1)).map_err(|_| ContextPlanError::invariant())?;
    material
        .runtime_snapshot
        .model
        .fallback_candidates
        .get(index)
        .map(|candidate| (&candidate.route, &candidate.credential_grant_ref))
        .ok_or_else(ContextPlanError::invariant)
}

fn auxiliary_model_route(
    routes: &[PinnedAuxiliaryModelRoute],
    role: AuxiliaryModelRole,
) -> Result<&PinnedModelRoute, ContextPlanError> {
    routes
        .iter()
        .find(|candidate| candidate.role == role)
        .map(|candidate| &candidate.model)
        .ok_or_else(ContextPlanError::model_route_unavailable)
}

#[cfg(test)]
mod tests;
