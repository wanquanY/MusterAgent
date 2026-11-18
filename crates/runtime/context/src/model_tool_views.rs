//! Contract-aware model views. Durable receipts and user/MCP text are untouched.
use std::collections::{BTreeMap, BTreeSet};

use musteragent_discovery::ModelToolNames;
use musteragent_identity::{RunId, VersionedKey};
use musteragent_runtime_model::{CanonicalContentPart, CanonicalMessage, JsonValue, ModelText};
use musteragent_tool::{ToolKey, WireName};
use serde_json::Value;

use crate::{ContextEntry, ContextMaterial, ContextPlanError};

#[derive(Clone, Copy)]
enum View {
    Search,
    Activate,
    Skill,
    SkillContent,
    NamedSkill,
    NamedSkillContent,
    CapabilityRequest,
}

/// The verified source body already rendered in current model instructions.
/// A receipt may also carry derived, task-local execution guidance, which the
/// immutable source hash does not cover and must never deduplicate away.
pub(crate) struct LoadedSkillBody {
    pub(crate) hash: String,
    pub(crate) body: String,
}

trait ToolNames {
    fn name(&self, key: &ToolKey) -> Option<&WireName>;
    fn resolve(&self, name: &str) -> Option<&ToolKey>;
}
impl ToolNames for ModelToolNames {
    fn name(&self, key: &ToolKey) -> Option<&WireName> {
        self.name(key)
    }
    fn resolve(&self, name: &str) -> Option<&ToolKey> {
        self.resolve(name)
    }
}
/// Context retains the exact full-catalogue name chosen at exposure. Rebuilding
/// names from the exposed subset would reinterpret collision suffixes.
struct MaterialNames<'a> {
    inline: ModelToolNames,
    resolved: &'a [crate::ResolvedContextTool],
}
impl<'a> MaterialNames<'a> {
    fn new(material: &'a ContextMaterial) -> Result<Self, ContextPlanError> {
        Ok(Self {
            inline: ModelToolNames::new(
                material
                    .runtime_snapshot
                    .tools
                    .iter()
                    .map(|tool| (&tool.manifest.key, &tool.manifest.wire_name)),
            )
            .map_err(|_| ContextPlanError::invariant())?,
            resolved: &material.resolved_tools,
        })
    }
}
impl ToolNames for MaterialNames<'_> {
    fn name(&self, key: &ToolKey) -> Option<&WireName> {
        self.resolved
            .iter()
            .find(|v| v.tool.manifest.key == *key)
            .map(|v| &v.model_name)
            .or_else(|| self.inline.name(key))
    }
    fn resolve(&self, name: &str) -> Option<&ToolKey> {
        self.resolved
            .iter()
            .find(|v| v.model_name.as_str() == name)
            .map(|v| &v.tool.manifest.key)
            .or_else(|| self.inline.resolve(name))
    }
}

pub(crate) fn project_material(material: &mut ContextMaterial) -> Result<(), ContextPlanError> {
    let current_skills: Vec<_> = material
        .runtime_snapshot
        .skills
        .iter()
        .map(|skill| skill.skill_ref.clone())
        .collect();
    let loaded: BTreeMap<_, _> = material
        .disclosed_skills
        .iter()
        .map(|skill| {
            let source = std::str::from_utf8(&skill.instructions.bytes)
                .map_err(|_| ContextPlanError::invariant())?;
            let body = musteragent_skill::model_instruction_body(source)
                .map_err(|_| ContextPlanError::invariant())?;
            Ok((
                skill.skill_ref.as_str().to_owned(),
                LoadedSkillBody {
                    hash: skill.instructions_hash.to_string(),
                    body: body.to_owned(),
                },
            ))
        })
        .collect::<Result<_, ContextPlanError>>()?;
    let skill_views: BTreeSet<_> = material
        .runtime_snapshot
        .tools
        .iter()
        .filter(|tool| tool.manifest.key.logical_key() == "skills.view")
        .map(|tool| tool.manifest.wire_name.clone())
        .collect();
    project_skill_receipts(
        &mut material.entries,
        material.run_context.run_id,
        &current_skills,
        &loaded,
        &skill_views,
    )?;
    let tools = &material.runtime_snapshot.tools;
    if !tools.iter().any(|tool| {
        tool.manifest.key.logical_key() == "tool.catalog.activate"
            && tool.manifest.key.version() >= 2
    }) {
        return Ok(()); // Only frozen historical @1 runs retain the old interface.
    }
    let inline_names = ModelToolNames::new(
        tools
            .iter()
            .map(|tool| (&tool.manifest.key, &tool.manifest.wire_name)),
    )
    .map_err(|_| ContextPlanError::invariant())?;
    let names = MaterialNames {
        inline: inline_names,
        resolved: &material.resolved_tools,
    };
    let views: BTreeMap<_, _> = tools
        .iter()
        .filter_map(|tool| {
            let view = match tool.manifest.key.logical_key() {
                "tool.catalog.search" => View::Search,
                "tool.catalog.activate" => View::Activate,
                "skills.search" | "skills.list" if tool.manifest.key.version() == 2 => {
                    View::NamedSkill
                }
                "skills.view" if tool.manifest.key.version() == 2 => View::NamedSkillContent,
                "skills.search" | "skills.list" => View::Skill,
                "skills.view" => View::SkillContent,
                "capability.request" if tool.manifest.key.version() == 2 => View::CapabilityRequest,
                _ => return None,
            };
            Some((tool.manifest.wire_name.clone(), view))
        })
        .collect();
    let renamed = tools
        .iter()
        .chain(material.resolved_tools.iter().map(|v| &v.tool))
        .filter(|tool| {
            names
                .name(&tool.manifest.key)
                .is_some_and(|name| name != &tool.manifest.wire_name)
        })
        .map(|tool| tool.manifest.wire_name.clone())
        .collect();
    project_entries(
        &mut material.entries,
        material.run_context.run_id,
        &names,
        &views,
        &renamed,
    )?;
    project_named_skills(material)
}

/// Interface generation is selected by the immutable Tool contract, never by
/// today's installation. Old Runs keep the original exact-ref call surface.
fn project_named_skills(material: &mut ContextMaterial) -> Result<(), ContextPlanError> {
    if !material
        .runtime_snapshot
        .tools
        .iter()
        .any(|tool| tool.manifest.key.as_str() == "skills.view@2")
    {
        return Ok(());
    }
    let catalog = material
        .runtime_snapshot
        .frozen_authorized_skill_catalog()
        .map_err(|_| ContextPlanError::invariant())?;
    let skill_names = catalog.model_names();
    let views: BTreeSet<_> = material
        .runtime_snapshot
        .tools
        .iter()
        .filter(|tool| {
            matches!(
                tool.manifest.key.logical_key(),
                "skills.search" | "skills.list" | "skills.view" | "skills.resource.read"
            )
        })
        .map(|tool| tool.manifest.wire_name.clone())
        .collect();
    let tool_names = MaterialNames::new(material)?;
    let resource_tool = name_for_key(&tool_names, "skills.resource.read@2");
    let run_id = material.run_context.run_id;
    project_named_skill_entries(
        &mut material.entries,
        run_id,
        skill_names,
        &views,
        resource_tool.as_deref(),
    )
}

pub(crate) fn project_named_skill_entries(
    entries: &mut [ContextEntry],
    run_id: RunId,
    skill_names: &musteragent_discovery::ModelSkillNames,
    views: &BTreeSet<WireName>,
    resource_tool: Option<&str>,
) -> Result<(), ContextPlanError> {
    for entry in entries
        .iter_mut()
        .flat_map(ContextEntry::model_messages_mut)
    {
        for call in &mut entry.message.tool_calls {
            if views.contains(&call.name) {
                let changed =
                    crate::model_skill_views::project_arguments(&mut call.arguments, skill_names);
                if changed || entry.source_run_id != Some(run_id) {
                    *entry.assistant_origin = None;
                }
            }
        }
        if entry
            .message
            .tool_name
            .as_ref()
            .is_some_and(|name| views.contains(name))
        {
            project_message(entry.message, |value| {
                crate::model_skill_views::project_result(value, skill_names);
                let Some(object) = value.as_object_mut() else {
                    return;
                };
                object.remove("resource_access");
                if object.get("historical") == Some(&Value::Bool(true))
                    || (!object.contains_key("instructions")
                        && object.get("instructions_loaded") != Some(&Value::Bool(true)))
                {
                    return;
                }
                if let (Some(name), Some(skill)) = (
                    resource_tool,
                    object.get("skill_name").and_then(Value::as_str),
                ) {
                    object.insert("resource_access".into(), serde_json::json!({
                        "tool_name":name, "list_arguments":{"skill_name":skill},
                        "usage":"Activate this tool if necessary. Use list_arguments unchanged to list package files. To read a reference or script, add an exact package-relative file path from the list."
                    }));
                }
            })?;
        }
    }
    Ok(())
}

/// The existing typed history projection, shared with focused regression tests.
/// Only the caller's already sealed Skill set determines current execution eligibility.
pub(crate) fn project_skill_receipts(
    entries: &mut [ContextEntry],
    run_id: RunId,
    current_skills: &[VersionedKey],
    loaded: &BTreeMap<String, LoadedSkillBody>,
    skill_views: &BTreeSet<WireName>,
) -> Result<(), ContextPlanError> {
    for entry in entries
        .iter_mut()
        .flat_map(ContextEntry::model_messages_mut)
    {
        if entry
            .message
            .tool_name
            .as_ref()
            .is_some_and(|name| skill_views.contains(name))
        {
            let historical = entry.source_run_id != Some(run_id);
            project_message(entry.message, |value| {
                if historical {
                    super::skill_history_view::mark_current_version(value, current_skills);
                }
                deduplicate_skill_body(value, loaded)
            })?;
        }
    }
    Ok(())
}

fn project_entries(
    entries: &mut [ContextEntry],
    run_id: musteragent_identity::RunId,
    names: &impl ToolNames,
    views: &BTreeMap<WireName, View>,
    renamed: &BTreeSet<WireName>,
) -> Result<(), ContextPlanError> {
    for entry in entries
        .iter_mut()
        .flat_map(ContextEntry::model_messages_mut)
    {
        // Cross-Run aliases and discovery arguments can have a different
        // meaning even on the same Provider route. Never rewrite signed data.
        // Unknown source also takes the existing portable replay path.
        if entry.source_run_id != Some(run_id)
            && entry.message.tool_calls.iter().any(|call| {
                matches!(
                    views.get(&call.name),
                    Some(View::Activate | View::CapabilityRequest)
                ) || renamed.contains(&call.name)
            })
        {
            *entry.assistant_origin = None;
        }
        for call in &mut entry.message.tool_calls {
            if matches!(views.get(&call.name), Some(View::Activate)) {
                project_activation_arguments(&mut call.arguments, names);
            } else if matches!(views.get(&call.name), Some(View::CapabilityRequest)) {
                project_capability_arguments(&mut call.arguments);
            }
        }
        if let Some(view) = entry
            .message
            .tool_name
            .as_ref()
            .and_then(|name| views.get(name))
        {
            let view = match (view, entry.source_run_id == Some(run_id)) {
                (View::NamedSkill | View::NamedSkillContent, false) => {
                    // Aliases belong to one full Tool catalog. Even an unchanged
                    // Skill pin does not prove another Run assigned the same
                    // aliases to its dependencies.
                    project_message(entry.message, withdraw_historical_tool_names)?;
                    if matches!(view, View::NamedSkillContent) {
                        View::SkillContent
                    } else {
                        View::Skill
                    }
                }
                _ => *view,
            };
            project_result(entry.message, view, names)?;
        }
    }
    Ok(())
}

fn withdraw_historical_tool_names(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if object.contains_key("skill_name") && object.remove("required_tools").is_some() {
        object.insert("tool_discovery".into(), Value::String(
            "Use the current Tool catalog to find and activate this workflow's dependencies. Historical Tool names do not select Tools in this Run.".into()));
    }
    if let Some(items) = object.get_mut("items").and_then(Value::as_array_mut) {
        for item in items {
            withdraw_historical_tool_names(item);
        }
    }
}

fn name_for_key(names: &impl ToolNames, key: &str) -> Option<String> {
    let key = ToolKey::from_versioned_key(VersionedKey::new(key).ok()?);
    names.name(&key).map(|name| name.as_str().to_owned())
}

/// A model history view is not an authorization request. Keep the chosen
/// application and purpose while the exact descriptor stays in the journal.
fn project_capability_arguments(arguments: &mut JsonValue) {
    let JsonValue::Object(object) = arguments else {
        return;
    };
    let Some(JsonValue::Object(capability)) = object.get("capability") else {
        return;
    };
    let (Some(JsonValue::String(kind)), Some(JsonValue::String(slug))) =
        (capability.get("kind"), capability.get("slug"))
    else {
        return;
    };
    if !matches!(kind.as_str(), "plugin" | "mcp" | "skill")
        || slug.is_empty()
        || slug.len() > 128
        || !slug
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_' | b'.'))
    {
        return;
    }
    let name = format!("{kind}/{slug}");
    let feature = capability.get("feature").cloned();
    object.remove("capability");
    object.remove("schema");
    object.insert("capability_name".into(), JsonValue::String(name));
    if let Some(feature) = feature {
        object.insert("feature".into(), feature);
    }
}

fn project_activation_arguments(arguments: &mut JsonValue, names: &impl ToolNames) {
    let JsonValue::Object(object) = arguments else {
        return;
    };
    let Some(JsonValue::Array(keys)) = object.remove("tool_keys") else {
        return;
    };
    object.insert(
        "tool_names".to_owned(),
        JsonValue::Array(
            keys.into_iter()
                .map(|value| match value {
                    JsonValue::String(key) => JsonValue::String(
                        name_for_key(names, &key)
                            .unwrap_or_else(|| "[tool unavailable in this Run]".to_owned()),
                    ),
                    value => value,
                })
                .collect(),
        ),
    );
}

fn project_result(
    message: &mut CanonicalMessage,
    view: View,
    names: &impl ToolNames,
) -> Result<(), ContextPlanError> {
    project_message(message, |value| project_json(value, view, names))
}

fn deduplicate_skill_body(value: &mut Value, loaded: &BTreeMap<String, LoadedSkillBody>) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    let Some(loaded) = object
        .get("skill_ref")
        .and_then(Value::as_str)
        .and_then(|key| loaded.get(key))
        .filter(|loaded| {
            object.get("instructions_hash").and_then(Value::as_str) == Some(loaded.hash.as_str())
        })
    else {
        return;
    };
    let Some(suffix) = object
        .get("instructions")
        .and_then(Value::as_str)
        .and_then(|instructions| instructions.strip_prefix(loaded.body.as_str()))
        .filter(|suffix| suffix.is_empty() || suffix.starts_with("\n\n"))
    else {
        return;
    };
    // Compare and remove only the exact already-loaded source. Preserve any
    // appended runtime context, without interpreting third-party Skill text.
    let suffix = suffix.trim_start().to_owned();
    if suffix.is_empty() {
        object.remove("instructions");
    } else {
        object.insert("instructions".to_owned(), Value::String(suffix));
    }
    object.remove("instructions_ref");
    object.insert("instructions_loaded".to_owned(), Value::Bool(true));
}

fn project_message(
    message: &mut CanonicalMessage,
    project: impl FnOnce(&mut Value),
) -> Result<(), ContextPlanError> {
    if message.role != musteragent_runtime_model::CanonicalRole::Tool {
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
        return Ok(());
    };
    project(&mut value);
    let encoded = serde_json::to_string(&value).map_err(|_| ContextPlanError::invariant())?;
    if encoded == text {
        return Ok(());
    }
    let mut replacement =
        Some(ModelText::chunked(encoded).map_err(|_| ContextPlanError::invariant())?);
    message.content = std::mem::take(&mut message.content)
        .into_iter()
        .flat_map(|part| match part {
            CanonicalContentPart::Text(_) => replacement
                .take()
                .unwrap_or_default()
                .into_iter()
                .map(CanonicalContentPart::Text)
                .collect(),
            part => vec![part],
        })
        .collect();
    Ok(())
}

fn project_json(value: &mut Value, view: View, names: &impl ToolNames) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    match view {
        View::CapabilityRequest => {}
        View::Search => {
            if let Some(items) = object.get_mut("items").and_then(Value::as_array_mut) {
                for item in items {
                    if let Some(item) = item.as_object_mut()
                        && let Some(key) = item.remove("tool_key")
                    {
                        item.insert(
                            "name".to_owned(),
                            key.as_str()
                                .and_then(|key| name_for_key(names, key))
                                .map_or(Value::Null, Value::String),
                        );
                    }
                }
            }
        }
        View::Activate => {
            if let Some(Value::Array(items)) = object.remove("exposures") {
                object.insert("activated".to_owned(), Value::Array(items.into_iter().map(|item| {
                    serde_json::json!({"name": item.get("tool_key").and_then(Value::as_str).and_then(|key| name_for_key(names, key))})
                }).collect()));
            }
            if let Some(error) = object.get_mut("error").and_then(Value::as_object_mut)
                && error
                    .get("code")
                    .and_then(Value::as_str)
                    .is_some_and(|code| code == "policy_denied")
            {
                // The Runtime's historical failure envelope uses `message`.
                // Preserve current readable-name feedback so the model can
                // correct its selection; only the old internal-key receipt changes.
                if error
                    .get("message")
                    .and_then(Value::as_str)
                    .is_some_and(|message| message.starts_with("Tool key '"))
                {
                    error.insert("message".to_owned(), Value::String(
                        "One or more requested tools were unavailable; no tools were activated.".to_owned()));
                }
            }
        }
        View::Skill | View::SkillContent | View::NamedSkill | View::NamedSkillContent => {
            let named = matches!(view, View::NamedSkill | View::NamedSkillContent);
            project_skill(object, names, named);
            if matches!(view, View::SkillContent | View::NamedSkillContent) {
                project_skill_resource_access(object, names);
            }
            if let Some(items) = object.get_mut("items").and_then(Value::as_array_mut) {
                for item in items {
                    if let Some(item) = item.as_object_mut() {
                        project_skill(item, names, named);
                    }
                }
            }
        }
    }
}

/// This is Runtime interface guidance, separate from the immutable Skill body
/// and receipt. Do not make the model discover how to list files by first
/// reading a file in the package it is trying to discover.
fn project_skill_resource_access(
    object: &mut serde_json::Map<String, Value>,
    names: &impl ToolNames,
) {
    if object.get("historical") == Some(&Value::Bool(true))
        || (!object.contains_key("instructions")
            && object.get("instructions_loaded") != Some(&Value::Bool(true)))
    {
        return;
    }
    let Some(skill_ref) = object.get("skill_ref").and_then(Value::as_str) else {
        return;
    };
    let Some(name) = name_for_key(names, "skills.resource.read@1") else {
        return;
    };
    object.insert(
        "resource_access".to_owned(),
        serde_json::json!({
            "tool_name": name,
            "list_arguments": {"skill_ref": skill_ref},
            "usage": "Activate this tool if it is not exposed. To list package files, use list_arguments unchanged: omit path entirely. To read a reference or script, add path with an exact package-relative file from the list or the skill instructions. A directory such as references/ is not a file; index is not a listing command."
        }),
    );
}

fn project_skill(object: &mut serde_json::Map<String, Value>, names: &impl ToolNames, named: bool) {
    // @2 receipts were created by the Skill owner against the full authorized
    // catalog. Context only carries exposed indexed schemas, so recalculating
    // from this subset would drop or rename not-yet-exposed dependencies.
    let preserve_names = named && object.contains_key("skill_name");
    if !preserve_names
        && let Some(tools) = object
            .get_mut("required_tools")
            .and_then(Value::as_array_mut)
    {
        let mut seen = BTreeSet::new();
        tools.retain_mut(|value| {
            let Some(key) = value.as_str() else {
                return false;
            };
            let name =
                name_for_key(names, key).or_else(|| names.resolve(key).map(|_| key.to_owned()));
            let Some(name) = name else {
                return false;
            };
            if !seen.insert(name.clone()) {
                return false;
            }
            *value = Value::String(name);
            true
        });
    }
    object.remove("instructions_hash");
    object.remove("disclosure_ordinal");
    // Calls use skill_ref; the database row ID never selects a skill.
    object.remove("skill_version_id");
    // No model Tool reads Skill storage blobs; skill_ref is the selection key
    // and the instruction body is already present or loaded in instructions.
    object.remove("instructions_ref");
}

/// Compression has no executable Tool declarations; its history must still
/// use the same readable interface, never the canonical storage identity.
pub(crate) fn project_compaction_names(
    entries: &mut [ContextEntry],
    material: &ContextMaterial,
) -> Result<(), ContextPlanError> {
    let names = MaterialNames::new(material)?;
    let by_wire: BTreeMap<&WireName, &WireName> = material
        .runtime_snapshot
        .tools
        .iter()
        .chain(material.resolved_tools.iter().map(|v| &v.tool))
        .map(|tool| {
            Ok((
                &tool.manifest.wire_name,
                names
                    .name(&tool.manifest.key)
                    .ok_or_else(ContextPlanError::invariant)?,
            ))
        })
        .collect::<Result<_, ContextPlanError>>()?;
    for entry in entries
        .iter_mut()
        .flat_map(ContextEntry::model_messages_mut)
    {
        for call in &mut entry.message.tool_calls {
            if let Some(name) = by_wire.get(&call.name) {
                call.name = (*name).clone();
            }
        }
        if let Some(name) = &mut entry.message.tool_name
            && let Some(model) = by_wire.get(name)
        {
            *name = (*model).clone();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_skill_dependencies_keep_full_catalog_aliases_only_in_their_run() {
        let original = serde_json::json!({"skill_ref":"research@7", "skill_name":"research",
            "required_tools":["github_get_me_3"], "instructions":"Read only the requested repository."});
        let mut current = original.clone();
        // This alias belongs to an indexed dependency not yet exposed here.
        project_json(&mut current, View::NamedSkillContent, &names());
        assert_eq!(current["required_tools"], original["required_tools"]);
        let mut historical = original.clone();
        withdraw_historical_tool_names(&mut historical);
        project_json(&mut historical, View::SkillContent, &names());
        assert!(historical.get("required_tools").is_none());
        assert!(historical.get("tool_discovery").is_some());
        assert_eq!(historical["instructions"], original["instructions"]);
    }
    #[test]
    fn capability_history_keeps_application_and_intent_without_version_authority() {
        let original = JsonValue::Object(BTreeMap::from([
            (
                "title".into(),
                JsonValue::String("Read release v2.1 requested by the user".into()),
            ),
            (
                "capability".into(),
                JsonValue::Object(BTreeMap::from([
                    ("kind".into(), JsonValue::String("plugin".into())),
                    ("slug".into(), JsonValue::String("github".into())),
                    ("entry_id".into(), JsonValue::String("private-entry".into())),
                    (
                        "release_id".into(),
                        JsonValue::String("private-release".into()),
                    ),
                    ("action".into(), JsonValue::String("connect".into())),
                    (
                        "feature".into(),
                        JsonValue::String("repository.read".into()),
                    ),
                ])),
            ),
        ]));
        let mut projected = original.clone();
        project_capability_arguments(&mut projected);
        let JsonValue::Object(view) = &projected else {
            panic!("object");
        };
        assert_eq!(
            view.get("capability_name"),
            Some(&JsonValue::String("plugin/github".into()))
        );
        assert_eq!(
            view.get("feature"),
            Some(&JsonValue::String("repository.read".into()))
        );
        assert_eq!(
            view.get("title"),
            Some(&JsonValue::String(
                "Read release v2.1 requested by the user".into()
            ))
        );
        assert!(!view.contains_key("capability"));
        assert_ne!(projected, original);
        let once = projected.clone();
        project_capability_arguments(&mut projected);
        assert_eq!(projected, once);
    }
    #[test]
    fn indexed_receipts_preserve_names_assigned_from_the_complete_catalogue() {
        let tool = crate::resolved_tools::tests::tool();
        let key = tool.manifest.key.clone();
        let resolved = vec![crate::ResolvedContextTool {
            tool,
            // A sibling outside this exposed subset owns the natural name.
            model_name: WireName::new("read_file_3").unwrap(),
        }];
        let names = MaterialNames {
            inline: ModelToolNames::new([]).unwrap(),
            resolved: &resolved,
        };
        assert_eq!(names.name(&key).unwrap().as_str(), "read_file_3");
        assert_eq!(names.resolve("read_file_3"), Some(&key));
        assert!(names.resolve("read_file").is_none());
        let mut receipt = serde_json::json!({"items":[{"tool_key":key.as_str()}]});
        project_json(&mut receipt, View::Search, &names);
        assert_eq!(receipt["items"][0]["name"], "read_file_3");
        let mut activation = serde_json::json!({"exposures":[{"tool_key":key.as_str()}]});
        project_json(&mut activation, View::Activate, &names);
        assert_eq!(activation["activated"][0]["name"], "read_file_3");
    }
    fn names() -> ModelToolNames {
        ModelToolNames::new([(
            &ToolKey::from_versioned_key(VersionedKey::new("tool.read@1").expect("key")),
            &WireName::new("read_file").expect("wire"),
        )])
        .expect("names")
    }
    #[test]
    fn skill_body_is_omitted_only_when_the_identical_pin_is_already_in_instructions() {
        let loaded = BTreeMap::from([(
            "presentations@2".to_owned(),
            LoadedSkillBody {
                hash: "current-hash".into(),
                body: "full instructions".into(),
            },
        )]);
        for hash in ["current-hash", "old-hash"] {
            let mut value = serde_json::json!({"skill_ref":"presentations@2",
                "instructions_hash":hash,"instructions":"full instructions",
                "instructions_ref":{"content_id":"private"}});
            deduplicate_skill_body(&mut value, &loaded);
            assert_eq!(value.get("instructions").is_none(), hash == "current-hash");
            assert_eq!(
                value.get("instructions_ref").is_none(),
                hash == "current-hash"
            );
            assert_eq!(value["skill_ref"], "presentations@2");
        }
    }

    #[test]
    fn historical_receipts_hide_only_owner_metadata() {
        let mut search = serde_json::json!({"items":[{"tool_key":"tool.read@1","description":"user text sha256 remains"}]});
        project_json(&mut search, View::Search, &names());
        assert_eq!(
            search,
            serde_json::json!({"items":[{"name":"read_file","description":"user text sha256 remains"}]})
        );
        let mut activation = serde_json::json!({"exposures":[{"tool_key":"tool.read@1","schema_hash":"private","ordinal":4}]});
        project_json(&mut activation, View::Activate, &names());
        assert_eq!(
            activation,
            serde_json::json!({"activated":[{"name":"read_file"}]})
        );
    }
    #[test]
    fn missing_historical_tools_never_gain_a_current_identity() {
        let mut value =
            serde_json::json!({"items":[{"tool_key":"tool.retired@1","title":"Retired"}]});
        project_json(&mut value, View::Search, &names());
        assert_eq!(value["items"][0]["name"], Value::Null);
        assert!(value["items"][0].get("tool_key").is_none());
    }
    #[test]
    fn skill_dependencies_and_private_metadata_have_one_model_projection() {
        let mut value = serde_json::json!({"required_tools":["tool.read@1","tool.retired@1","read_file"],
            "instructions_hash":"private", "disclosure_ordinal":3,
            "instructions_ref":{"content_id":"readable-resource","sha256":"private"},
            "instructions":"A document's sha256 and tool.read@1 are ordinary quoted content."});
        project_json(&mut value, View::Skill, &names());
        assert_eq!(value["required_tools"], serde_json::json!(["read_file"]));
        assert!(value.get("instructions_hash").is_none());
        assert!(value.get("disclosure_ordinal").is_none());
        assert!(value["instructions_ref"].get("sha256").is_none());
        assert!(
            value["instructions"]
                .as_str()
                .expect("instructions")
                .contains("tool.read@1")
        );
    }

    #[test]
    fn loaded_skill_supplies_a_resource_listing_call_without_rewriting_instructions() {
        let names = ModelToolNames::new([(
            &ToolKey::from_versioned_key(VersionedKey::new("skills.resource.read@1").unwrap()),
            &WireName::new("skills_resource_read").unwrap(),
        )])
        .unwrap();
        let original = serde_json::json!({"skill_ref":"research@2", "instructions_hash":"pin",
            "instructions":"Read the resource index before its references."});
        for already_loaded in [false, true] {
            let mut value = original.clone();
            if already_loaded {
                deduplicate_skill_body(
                    &mut value,
                    &BTreeMap::from([(
                        "research@2".into(),
                        LoadedSkillBody {
                            hash: "pin".into(),
                            body: original["instructions"].as_str().unwrap().into(),
                        },
                    )]),
                );
            }
            project_json(&mut value, View::SkillContent, &names);
            let access = &value["resource_access"];
            assert_eq!(access["tool_name"], "skills_resource_read");
            assert_eq!(
                access["list_arguments"],
                serde_json::json!({"skill_ref":"research@2"}),
            );
            if already_loaded {
                assert_eq!(value["instructions_loaded"], true);
            } else {
                assert_eq!(value["instructions"], original["instructions"]);
            }
        }
        let mut historical = original.clone();
        super::super::skill_history_view::mark_current_version(&mut historical, &[]);
        project_json(&mut historical, View::SkillContent, &names);
        assert!(historical.get("resource_access").is_none());
        let mut denied = serde_json::json!({"ok":false,"error":{"code":"policy_denied"}});
        project_json(&mut denied, View::SkillContent, &names);
        assert!(denied.get("resource_access").is_none());
        let mut unavailable_tool = original.clone();
        project_json(&mut unavailable_tool, View::SkillContent, &self::names());
        assert!(unavailable_tool.get("resource_access").is_none());
        let mut catalog = serde_json::json!({"items":[{"skill_ref":"research@2"}]});
        project_json(&mut catalog, View::Skill, &names);
        assert!(catalog["items"][0].get("resource_access").is_none());
        assert!(original.get("resource_access").is_none());
    }

    #[test]
    fn old_activation_arguments_are_projected_without_modifying_the_original_receipt() {
        let original = JsonValue::Object(BTreeMap::from([
            (
                "tool_keys".to_owned(),
                JsonValue::Array(vec![JsonValue::String("tool.read@1".to_owned())]),
            ),
            (
                "reason".to_owned(),
                JsonValue::String("Quoted tool.read@1 stays unchanged".to_owned()),
            ),
        ]));
        let mut projected = original.clone();
        project_activation_arguments(&mut projected, &names());
        let JsonValue::Object(projected) = projected else {
            panic!("object")
        };
        assert!(!projected.contains_key("tool_keys"));
        assert_eq!(
            projected["tool_names"],
            JsonValue::Array(vec![JsonValue::String("read_file".to_owned())])
        );
        assert_eq!(
            projected["reason"],
            JsonValue::String("Quoted tool.read@1 stays unchanged".to_owned())
        );
        let JsonValue::Object(original) = original else {
            panic!("original")
        };
        assert!(original.contains_key("tool_keys"));
    }

    #[test]
    fn owner_selection_preserves_user_and_third_party_payloads() {
        use musteragent_identity::{ConversationSeq, LedgerRecordId, RunId};
        use musteragent_runtime_model::CanonicalRole;
        let sequence = ConversationSeq::new(1).expect("sequence");
        let run_id = RunId::generate();
        let text = r#"{"items":[{"tool_key":"tool.read@1","schema_hash":"third-party-data"}]}"#;
        let entry = |role, tool_name| ContextEntry {
            history_page: None,
            runtime_events: Vec::new(),
            source_sequence: sequence,
            presentation_order: crate::ContextPresentationOrder::ledger(sequence),
            source_record_ids: vec![LedgerRecordId::generate()],
            source_run_id: Some(run_id),
            assistant_origin: None,
            continuation_delta_for: None,
            message: CanonicalMessage {
                role,
                tool_name,
                content: vec![CanonicalContentPart::Text(
                    ModelText::new(text).expect("text"),
                )],
                tool_calls: vec![],
                tool_call_id: None,
                reasoning: None,
            },
        };
        let entries = vec![
            entry(CanonicalRole::User, None),
            entry(
                CanonicalRole::Tool,
                Some(WireName::new("mcp_external_result").expect("wire")),
            ),
            entry(
                CanonicalRole::Tool,
                Some(WireName::new("tool_catalog_search").expect("wire")),
            ),
        ];
        let mut projected = entries.clone();
        let views = BTreeMap::from([(
            WireName::new("tool_catalog_search").expect("wire"),
            View::Search,
        )]);
        project_entries(&mut projected, run_id, &names(), &views, &BTreeSet::new())
            .expect("projection");
        assert_eq!(&projected[..2], &entries[..2]);
        assert_ne!(projected[2], entries[2]);
        assert_eq!(entries[2].message.content, entries[0].message.content);
    }

    #[test]
    fn a_changed_cross_run_interface_invalidates_native_replay_provenance() {
        use musteragent_identity::{
            ConversationSeq, InvocationId, LedgerRecordId, RunId, ToolCallId,
        };
        use musteragent_model::{ModelKey, SignedModelRouteHash};
        use musteragent_runtime_model::{CanonicalRole, CanonicalToolCall};
        let current = RunId::generate();
        let sequence = ConversationSeq::new(1).expect("seq");
        let wire = WireName::new("tool_catalog_activate").expect("wire");
        let entry = ContextEntry {
            history_page: None,
            runtime_events: Vec::new(),
            source_sequence: sequence,
            presentation_order: crate::ContextPresentationOrder::ledger(sequence),
            source_record_ids: vec![LedgerRecordId::generate()],
            source_run_id: Some(current),
            continuation_delta_for: None,
            assistant_origin: Some(crate::AssistantOrigin {
                invocation_id: InvocationId::generate(),
                model_route_ref: VersionedKey::new("route.test@1").expect("route"),
                route_identity_hash: SignedModelRouteHash::from_bytes(vec![1; 32]).expect("hash"),
                requested_model_key: ModelKey::new("model").expect("model"),
                response_model_key: ModelKey::new("model").expect("model"),
            }),
            message: CanonicalMessage {
                role: CanonicalRole::Assistant,
                content: vec![],
                tool_calls: vec![CanonicalToolCall {
                    call_id: ToolCallId::new("call_activation").expect("call"),
                    name: wire.clone(),
                    arguments: JsonValue::Object(BTreeMap::new()),
                    provider_item_ref: None,
                    provider_replay_ref: None,
                }],
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
        };
        let mut entries = vec![entry.clone(), entry.clone()];
        entries[1].source_run_id = Some(RunId::generate());
        project_entries(
            &mut entries,
            current,
            &names(),
            &BTreeMap::from([(wire, View::Activate)]),
            &BTreeSet::new(),
        )
        .expect("projection");
        assert_eq!(entries[0], entry);
        assert!(entries[1].assistant_origin.is_none());
        assert_eq!(entries[1].message, entry.message);
    }
    #[test]
    fn historical_denial_hides_internal_keys_but_current_feedback_keeps_the_model_name() {
        let mut legacy = serde_json::json!({"ok":false,"error":{"code":"policy_denied","message":"Tool key 'mcp.private_digest@1' is not authorized for this Run; no requested tools were activated","retryable":false}});
        project_json(&mut legacy, View::Activate, &names());
        assert!(!legacy.to_string().contains("private_digest"));
        assert!(legacy["error"].get("safe_message").is_none());
        let current = serde_json::json!({"ok":false,"error":{"code":"policy_denied","message":"Tool 'read_file' is not available in this Run's authorized catalog; no requested tools were activated","retryable":false}});
        let mut projected = current.clone();
        project_json(&mut projected, View::Activate, &names());
        assert_eq!(projected, current);
    }
}
