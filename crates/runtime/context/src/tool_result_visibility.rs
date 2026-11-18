//! Model-facing views of immutable Tool receipts.
//!
//! ContentRef and receipt digests remain in the content store, Tool schemas and
//! Ledger. Only the request-local view drops storage fingerprints. Run this
//! before budgeting, compaction and provider continuation selection so every
//! model path sees the same view, including replay of historical Tool results.

use std::collections::BTreeMap;

use musteragent_runtime_model::{CanonicalContentPart, CanonicalMessage, CanonicalRole, ModelText};
use serde_json::Value;

use crate::{ContextMaterial, ContextPlanError};

#[derive(Clone, Copy)]
pub(crate) enum ResultView {
    File,
    Search,
    Python,
    Application,
    Memory,
    Vision,
    Media,
    ModelFields(&'static str),
    ReturnedToRequester,
}

impl ResultView {
    pub(crate) fn cache_identity(self) -> &'static [u8] {
        match self {
            Self::File => b"tool-result.file-view.v1",
            Self::Search => b"tool-result.search-view.v1",
            Self::Python => b"tool-result.python-view.v1",
            Self::Application => b"tool-result.application-view.v1",
            Self::Memory => b"tool-result.memory-view.v1",
            Self::Vision => b"tool-result.vision-view.v1",
            Self::Media => b"tool-result.media-view.v1",
            Self::ReturnedToRequester => b"kernel.task-return.receipt.v1",
            Self::ModelFields(key) => key.as_bytes(),
        }
    }
    fn for_tool(key: &str) -> Option<Self> {
        match key {
            "artifact.file.create"
            | "artifact.file.read"
            | "artifact.file.patch"
            | "resource.file.create"
            | "resource.file.read"
            | "resource.file.edit.begin"
            | "resource.file.edit.append"
            | "resource.file.edit.patch"
            | "resource.file.edit.resume"
            | "resource.file.edit.finalize"
            | "resource.file.edit.abort" => Some(Self::File),
            "resource.search" => Some(Self::Search),
            "data.python.run" => Some(Self::Python),
            "application.web.create"
            | "application.web.configure"
            | "application.web.read"
            | "application.web.patch"
            | "application.web.build"
            | "application.web.validate" => Some(Self::Application),
            "memory.search" | "memory.list" | "memory.get" => Some(Self::Memory),
            "vision.analyze" => Some(Self::Vision),
            "artifact.image.create" | "artifact.video.create" => Some(Self::Media),
            _ => None,
        }
    }
}

pub(crate) fn project_material(
    material: &mut ContextMaterial,
    mut project: impl FnMut(&mut CanonicalMessage, ResultView) -> Result<(), ContextPlanError>,
) -> Result<(), ContextPlanError> {
    let legacy_references = material
        .runtime_snapshot
        .tools
        .iter()
        .any(|tool| tool.manifest.key.as_str() == "goal.report_progress@1");
    // Resolve the logical Tool from the sealed snapshot, never from untrusted
    // result JSON or name prefixes that could also belong to third-party tools.
    let policies: BTreeMap<_, _> = material
        .runtime_snapshot
        .tools
        .iter()
        .filter_map(|tool| {
            (if tool.manifest.key.as_str() == "tasks.submit_result@1"
                && material.runtime_snapshot.discovery.as_ref().is_some_and(|d| matches!(d.discovery_policy_ref.as_str(),
                    musteragent_runtime_snapshot::DIALOGUE_TASK_RESULT_DISCOVERY_POLICY | musteragent_runtime_snapshot::DIALOGUE_DEVICE_TASK_RESULT_DISCOVERY_POLICY)) {
                Some(ResultView::ReturnedToRequester)
            } else if !legacy_references {
                crate::model_field_views::contract(tool.manifest.key.as_str())
                    .map(ResultView::ModelFields)
                    .or_else(|| ResultView::for_tool(tool.manifest.key.logical_key()))
            } else {
                ResultView::for_tool(tool.manifest.key.logical_key())
            })
            .map(|view| (tool.manifest.wire_name.clone(), view))
        })
        .collect();
    for entry in material
        .entries
        .iter_mut()
        .flat_map(crate::ContextEntry::model_messages_mut)
    {
        let Some(view) = entry
            .message
            .tool_name
            .as_ref()
            .and_then(|name| policies.get(name))
        else {
            continue;
        };
        project(entry.message, *view)?;
    }
    Ok(())
}

// Look up the immutable execution owner's versioned tool identity. The sealed
// receipt itself remains unchanged and extension-defined fields remain opaque.
fn original_view(key: &str) -> Option<ResultView> {
    let (logical, _) = key.rsplit_once('@')?;
    crate::model_field_views::contract(key)
        .map(ResultView::ModelFields)
        .or_else(|| ResultView::for_tool(logical))
}

pub fn project_original_message(
    key: &str,
    message: &mut CanonicalMessage,
) -> Result<(), ContextPlanError> {
    if let Some(view) = original_view(key) {
        project_message(message, view)?;
    }
    Ok(())
}

pub fn project_original_json(key: &str, text: &mut String) {
    if let Some(view) = original_view(key)
        && let Ok(mut value) = serde_json::from_str::<Value>(text)
        && project_result(&mut value, view)
    {
        *text = value.to_string();
    }
}

pub(crate) fn project_message(
    message: &mut CanonicalMessage,
    view: ResultView,
) -> Result<(), ContextPlanError> {
    if message.role != CanonicalRole::Tool {
        return Ok(());
    }
    let text: String = message
        .content
        .iter()
        .filter_map(|part| match part {
            CanonicalContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
        // Plain-text errors are not metadata. Never pattern-replace file bytes,
        // stdout, diagnostic prose or an arbitrary user's SHA-256 string.
        return Ok(());
    };
    if !project_result(&mut value, view) {
        return Ok(());
    }
    let encoded = serde_json::to_string(&value).map_err(|_| ContextPlanError::invariant())?;
    let mut projected =
        Some(ModelText::chunked(encoded).map_err(|_| ContextPlanError::invariant())?);
    let original = std::mem::take(&mut message.content);
    for part in original {
        match part {
            CanonicalContentPart::Text(_) => {
                if let Some(texts) = projected.take() {
                    message
                        .content
                        .extend(texts.into_iter().map(CanonicalContentPart::Text));
                }
            }
            other => message.content.push(other),
        }
    }
    Ok(())
}

fn project_result(value: &mut Value, view: ResultView) -> bool {
    if matches!(view, ResultView::Media) {
        return musteragent_artifact::media_model_view::result(value);
    }
    if let ResultView::ModelFields(key) = view {
        let mut changed = ResultView::for_tool(key.split('@').next().unwrap_or(key))
            .is_some_and(|base| project_result(value, base));
        changed |= crate::model_field_views::project(value, key);
        return changed;
    }
    let Some(object) = value.as_object_mut() else {
        return false;
    };
    // These receipts contain usable references (e.g. image_refs consumed by
    // presentation_build). Do not remove their required integrity fields.
    match view {
        ResultView::ReturnedToRequester => {
            if object.get("status").and_then(Value::as_str) != Some("submitted_for_verification") {
                return false;
            }
            *value = serde_json::json!({"status":"submitted"});
            return true;
        }
        ResultView::Memory => return crate::tool_receipt_metadata::memory(object),
        ResultView::Vision => return crate::tool_receipt_metadata::vision(object),
        _ => {}
    }
    let mut changed = object.remove("sha256").is_some();
    for field in ["content_ref", "checkpoint"] {
        if let Some(metadata) = object.get_mut(field).and_then(Value::as_object_mut) {
            changed |= metadata.remove("sha256").is_some();
        }
    }
    match view {
        ResultView::File => {}
        ResultView::Search => {
            if let Some(hits) = object.get_mut("hits").and_then(Value::as_array_mut) {
                for hit in hits.iter_mut().filter_map(Value::as_object_mut) {
                    changed |= hit.remove("content_sha256").is_some();
                }
            }
        }
        ResultView::Python => {
            changed |= object.remove("result_hash").is_some();
            if let Some(output) = object.get_mut("output").and_then(Value::as_object_mut) {
                changed |= output.remove("sha256").is_some();
            }
        }
        ResultView::Application => {
            changed |= object.remove("source_revision_hash").is_some();
            changed |= object.remove("result_hash").is_some();
            if let Some(files) = object.get_mut("files").and_then(Value::as_array_mut) {
                for file in files.iter_mut().filter_map(Value::as_object_mut) {
                    changed |= file.remove("sha256").is_some();
                }
            }
        }
        ResultView::Memory
        | ResultView::Media
        | ResultView::Vision
        | ResultView::ReturnedToRequester
        | ResultView::ModelFields(_) => {
            unreachable!()
        }
    }
    changed
}

#[cfg(test)]
#[path = "tool_result_visibility_tests.rs"]
mod tests;
