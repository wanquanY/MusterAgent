//! Resume only the original incomplete Native publication turn. The Loop calls
//! planning after its pending Tool/Interaction gates; this adds no admission,
//! counter, history record, or permission. The normal plan source fence remains.
use crate::{
    ContextEntry, ContextMaterial, ContextPlanError, ContextPlanInput, MaterializedContent,
};
use musteragent_execution_context::DialogueOutputContract;
use musteragent_identity::{InvocationId, RunId};
use musteragent_runtime_content::verify_content;
use musteragent_runtime_model::{
    CanonicalRole, CanonicalRuntimeEvent, CanonicalToolDefinition, ModelText,
};
use musteragent_runtime_snapshot::PromptSectionSnapshot;

pub(super) fn append_reply_guidance(
    instructions: &mut Vec<ModelText>,
    material: &ContextMaterial,
    tools: &[CanonicalToolDefinition],
) -> Result<(), ContextPlanError> {
    if material.run_context.activity_kind != musteragent_activity::ActivityKind::Chat {
        return Ok(());
    }
    let Some(tool) = tools
        .iter()
        .find(|tool| tool.key.as_str() == "dialogue.publish@1")
    else {
        return Ok(());
    };
    if sealed_native_contract(
        &material.runtime_snapshot.prompt.sections,
        &material.prompt_sections,
    )? {
        let name = tool.model_name.as_str();
        let text = format!(
            "When the current message only needs a conversational reply (including a greeting), call `{name}` once with purpose `reply` and the answer in `text`. This sends the message to the user. Earlier tasks in the conversation are background unless the user asks to resume them."
        );
        instructions.push(ModelText::new(text).map_err(|_| ContextPlanError::invariant())?);
    }
    Ok(())
}

pub(super) fn event(
    input: &ContextPlanInput,
    material: &ContextMaterial,
    entries: &[&ContextEntry],
    events: &[CanonicalRuntimeEvent],
) -> Result<Option<CanonicalRuntimeEvent>, ContextPlanError> {
    if input.continuation_ref.is_some()
        || input.continuation_source_invocation_id.is_some()
        || material.model_recovery.is_some()
        || material.model_action_rejection.is_some()
        || !events.is_empty()
    {
        return Ok(None);
    }
    let Some(source_invocation_id) = source(entries, input.attempt.run_id) else {
        return Ok(None);
    };
    if !sealed_native_contract(
        &material.runtime_snapshot.prompt.sections,
        &material.prompt_sections,
    )? {
        return Ok(None);
    }
    Ok(Some(CanonicalRuntimeEvent::NativePublicationRequired {
        source_invocation_id,
    }))
}

fn source(entries: &[&ContextEntry], run: RunId) -> Option<InvocationId> {
    let last = entries.last()?;
    if last.source_run_id != Some(run)
        || last.source_record_ids.is_empty()
        || last.message.role != CanonicalRole::Assistant
        || !last.message.tool_calls.is_empty()
        || !last.runtime_events.is_empty()
        || last.continuation_delta_for.is_some()
    {
        return None;
    }
    Some(last.assistant_origin.as_ref()?.invocation_id)
}

fn sealed_native_contract(
    sections: &[PromptSectionSnapshot],
    material: &[MaterializedContent],
) -> Result<bool, ContextPlanError> {
    let mut pins = sections
        .iter()
        .filter(|v| v.section_key.as_str() == "dialogue.output-contract");
    let Some(pin) = pins.next() else {
        return Ok(false);
    };
    if pins.next().is_some() {
        return Err(ContextPlanError::invariant());
    }
    let Some(contract) =
        DialogueOutputContract::from_prompt_ref(&pin.content_ref).filter(|v| v.is_native())
    else {
        return Ok(false);
    };
    let mut contents = material.iter().filter(|v| v.content_ref == pin.content_ref);
    let content = contents.next().ok_or_else(ContextPlanError::invariant)?;
    if contents.next().is_some() || content.bytes != contract.prompt().as_bytes() {
        return Err(ContextPlanError::invariant());
    }
    verify_content(&pin.content_ref, &content.bytes).map_err(|_| ContextPlanError::invariant())?;
    Ok(true)
}

#[cfg(test)]
mod tests;
