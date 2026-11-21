//! Platform-neutral ownership plan for built-in Capability handlers.
//!
//! A release decides which Tool exists. This module decides which logical
//! handler family owns it. Cloud and Device composition roots may inject
//! different physical ports, but must not maintain parallel lists of product
//! Tool keys or reinterpret handler ownership.

use thiserror::Error;

use crate::{BuiltinToolCatalog, BuiltinToolDefinition, CapabilityExecutionTarget};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CapabilityHandlerKind {
    Backend,
    BackendMedia,
    Time,
    Vision,
    Decision,
    Delegation,
    History,
    Conversation,
    ProjectControl,
    Discovery,
    SkillDiscovery,
    SkillPreview,
    ExecutionPlan,
    Interaction,
    Artifact,
    PresentationAuthoring,
    FileArtifact,
    DeviceFileArtifact,
    BrowserPython,
    VideoScene,
    AudioNarration,
    VideoTemplate,
    BrowserEnvironment,
    WebApplication,
    Resource,
    FileEdit,
    FileQuery,
    Memory,
    StandingGoal,
    AgentPartner,
    AgentSelfConfiguration,
    OwnerGoal,
    OwnerSchedule,
    DialogueAgenda,
    AgentWorkItem,
    ExecutorEnvironment,
    DialoguePublication,
    SourceAttachment,
    DeviceLocal,
    /// Installed by the shared Executor owner; unavailable to private Runs.
    SharedExecutor,
}

/// Execution dependencies owned by the installed handler, separate from permission.
/// Admission resolves these against immutable Run material, never live configuration.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ToolExecutionRequirement {
    DecisionModel,
}

impl CapabilityHandlerKind {
    /// Handlers that can only run through a live browser execution provider
    /// (a user's open page holding a browser lease). A Run with nobody attached
    /// to a page, such as a scheduled or other unattended execution, can never
    /// satisfy them.
    #[must_use]
    pub const fn requires_browser_execution(self) -> bool {
        matches!(
            self,
            Self::BrowserEnvironment | Self::BrowserPython | Self::WebApplication
        )
    }

    #[must_use]
    pub const fn execution_requirements(self) -> &'static [ToolExecutionRequirement] {
        match self {
            Self::Decision => &[ToolExecutionRequirement::DecisionModel],
            _ => &[],
        }
    }
}

impl BuiltinToolDefinition {
    pub fn execution_requirements(
        &self,
    ) -> Result<&'static [ToolExecutionRequirement], CapabilityAssemblyError> {
        Ok(classify(self)?.execution_requirements())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CapabilityAssemblyEntry<'a> {
    pub definition: &'a BuiltinToolDefinition,
    pub handler: CapabilityHandlerKind,
}

#[derive(Debug, Clone)]
pub struct CapabilityAssemblyPlan<'a> {
    entries: Vec<CapabilityAssemblyEntry<'a>>,
}

impl<'a> CapabilityAssemblyPlan<'a> {
    pub fn from_catalog(catalog: &'a BuiltinToolCatalog) -> Result<Self, CapabilityAssemblyError> {
        Self::from_definitions(catalog.definitions().iter())
    }

    /// Install only handlers belonging to this released product Surface. A
    /// Mobile overlay inherits Desktop definitions for schema lineage; that
    /// inheritance must never make Desktop-only tools executable on the phone.
    pub fn for_surface(
        catalog: &'a BuiltinToolCatalog,
        surface: &str,
    ) -> Result<Self, CapabilityAssemblyError> {
        let plan = Self::from_definitions(catalog.iter_definitions_for_surface(surface))?;
        if plan.entries.is_empty() {
            return Err(CapabilityAssemblyError::EmptySurface(surface.to_owned()));
        }
        Ok(plan)
    }

    /// Execution composition only; this does not publish admission surfaces.
    pub fn from_definitions(
        definitions: impl Iterator<Item = &'a BuiltinToolDefinition>,
    ) -> Result<Self, CapabilityAssemblyError> {
        let entries = definitions
            .map(|definition| {
                Ok(CapabilityAssemblyEntry {
                    definition,
                    handler: classify(definition)?,
                })
            })
            .collect::<Result<Vec<_>, CapabilityAssemblyError>>()?;
        Ok(Self { entries })
    }

    #[must_use]
    pub fn entries(&self) -> &[CapabilityAssemblyEntry<'a>] {
        &self.entries
    }

    pub fn definitions(
        &self,
        handler: CapabilityHandlerKind,
    ) -> impl Iterator<Item = &'a BuiltinToolDefinition> + '_ {
        self.entries
            .iter()
            .filter(move |entry| entry.handler == handler)
            .map(|entry| entry.definition)
    }
}

fn classify(
    definition: &BuiltinToolDefinition,
) -> Result<CapabilityHandlerKind, CapabilityAssemblyError> {
    if let CapabilityExecutionTarget::BackendNamed(operation) = &definition.target {
        return Ok(if operation.media_artifact().is_some() {
            CapabilityHandlerKind::BackendMedia
        } else {
            CapabilityHandlerKind::Backend
        });
    }

    let key = definition.key.logical_key();
    let handler = if key == "system.time.current" {
        CapabilityHandlerKind::Time
    } else if matches!(key, "dialogue.publish" | "artifacts.deliver") {
        CapabilityHandlerKind::DialoguePublication
    } else if matches!(
        key,
        "sources.attachments.list" | "sources.attachments.import"
    ) {
        CapabilityHandlerKind::SourceAttachment
    } else if key.starts_with("tasks.") {
        CapabilityHandlerKind::AgentWorkItem
    } else if matches!(key, "environments.list" | "environments.inspect") {
        CapabilityHandlerKind::ExecutorEnvironment
    } else if matches!(
        key,
        "schedules.history"
            | "schedules.list"
            | "schedules.create"
            | "schedules.update"
            | "schedules.control"
    ) {
        CapabilityHandlerKind::OwnerSchedule
    } else if matches!(
        definition.key.as_str(),
        "goal.create@3"
            | "goal.draft.save@1"
            | "goal.get@3"
            | "goal.list@2"
            | "goal.update@3"
            | "goal.change_status@2"
            | "goal.sources.catalog@1"
            | "goal.sources.resources@1"
            | "goal.sources.configure@1"
            | "goal.sources.list@1"
    ) {
        CapabilityHandlerKind::OwnerGoal
    } else if key.starts_with("agenda.") {
        CapabilityHandlerKind::DialogueAgenda
    } else if matches!(key, "agents.self.inspect" | "agents.self.configure") {
        CapabilityHandlerKind::AgentSelfConfiguration
    } else if key == "agents.create" {
        CapabilityHandlerKind::AgentPartner
    } else if key == "decision.evaluate" {
        CapabilityHandlerKind::Decision
    } else if definition.wire_name.as_str() == "vision_analyze" {
        CapabilityHandlerKind::Vision
    } else if matches!(key, "context.read" | "context.search")
        || definition.key.as_str() == "delegation.inspect@2"
    {
        CapabilityHandlerKind::History
    } else if key.starts_with("delegation.") {
        CapabilityHandlerKind::Delegation
    } else if key.starts_with("conversation.") {
        CapabilityHandlerKind::Conversation
    } else if key.starts_with("project.control.") || key == "project.workspace.reconcile" {
        CapabilityHandlerKind::ProjectControl
    } else if matches!(key, "tool.catalog.search" | "tool.catalog.activate") {
        CapabilityHandlerKind::Discovery
    } else if matches!(
        key,
        "skills.search" | "skills.list" | "skills.view" | "skills.resource.read"
    ) {
        CapabilityHandlerKind::SkillDiscovery
    } else if matches!(key, "plan.get" | "plan.update") {
        CapabilityHandlerKind::ExecutionPlan
    } else if matches!(
        key,
        "interaction.request" | "interaction.question" | "capability.request"
    ) {
        CapabilityHandlerKind::Interaction
    } else if key == "artifact.presentation.author" {
        CapabilityHandlerKind::PresentationAuthoring
    } else if matches!(
        key,
        "artifact.presentation.build" | "artifact.presentation.update"
    ) {
        CapabilityHandlerKind::Artifact
    } else if key == "device.workspace.file.publish" {
        CapabilityHandlerKind::DeviceFileArtifact
    } else if key == "resource.file.create" {
        CapabilityHandlerKind::FileArtifact
    } else if let Some(handler) = browser_execution_handler(key) {
        handler
    } else if matches!(
        key,
        "video.scene.preview" | "video.scene.render" | "video.narrate"
    ) {
        CapabilityHandlerKind::VideoScene
    } else if matches!(
        key,
        "audio.narration.create" | "audio.music.create" | "audio.effect.create"
    ) {
        CapabilityHandlerKind::AudioNarration
    } else if matches!(
        key,
        "video.template.search" | "video.template.use" | "video.template.package"
    ) {
        CapabilityHandlerKind::VideoTemplate
    } else if key.starts_with("files.") {
        CapabilityHandlerKind::FileQuery
    } else if key == "resource.file.read" || key.starts_with("resource.file.edit.") {
        CapabilityHandlerKind::FileEdit
    } else if key.starts_with("resource.") {
        CapabilityHandlerKind::Resource
    } else if key.starts_with("experience.")
        || key.starts_with("memory.")
        || key == "user_memory.remember"
    {
        CapabilityHandlerKind::Memory
    } else if key.starts_with("goal.") {
        CapabilityHandlerKind::StandingGoal
    } else if matches!(
        key,
        "device.resource.read"
            | "device.resource.inspect"
            | "device.resource.write"
            | "device.network.https"
            | "device.interaction"
    ) {
        CapabilityHandlerKind::SharedExecutor
    } else if matches!(
        key,
        "skill.package.preview.create" | "skill.package.preview.update"
    ) {
        CapabilityHandlerKind::SkillPreview
    } else if key.starts_with("device.") {
        CapabilityHandlerKind::DeviceLocal
    } else {
        return Err(CapabilityAssemblyError::UnregisteredRuntimeCapability(
            definition.key.as_str().to_owned(),
        ));
    };
    Ok(handler)
}

/// The single owner of browser-executed handler families, shared by handler
/// assembly and by derivations that must drop them.
fn browser_execution_handler(logical_key: &str) -> Option<CapabilityHandlerKind> {
    if logical_key.starts_with("sandbox.environment.")
        || logical_key.starts_with("sandbox.operation.")
    {
        Some(CapabilityHandlerKind::BrowserEnvironment)
    } else if logical_key == "data.python.run" {
        Some(CapabilityHandlerKind::BrowserPython)
    } else if logical_key.starts_with("application.web.") {
        Some(CapabilityHandlerKind::WebApplication)
    } else {
        None
    }
}

/// Whether a built-in Tool or Capability key (versioned or logical) can only
/// execute through a browser execution provider.
#[must_use]
pub fn requires_browser_execution(key: &str) -> bool {
    let logical = key.rsplit_once('@').map_or(key, |(logical, _)| logical);
    browser_execution_handler(logical)
        .is_some_and(CapabilityHandlerKind::requires_browser_execution)
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum CapabilityAssemblyError {
    #[error("built-in Capability assembly has no released definitions for Surface: {0}")]
    EmptySurface(String),
    #[error("built-in Runtime Capability has no canonical handler owner: {0}")]
    UnregisteredRuntimeCapability(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn decision_dependency_belongs_to_the_handler_on_every_execution_surface() {
        for catalog in [
            BuiltinToolCatalog::load_cloud().unwrap(),
            BuiltinToolCatalog::load_device().unwrap(),
        ] {
            let plan = CapabilityAssemblyPlan::from_catalog(&catalog).unwrap();
            for entry in plan.entries() {
                let required = entry.definition.execution_requirements().unwrap();
                if entry.handler == CapabilityHandlerKind::Decision {
                    assert_eq!(required, &[ToolExecutionRequirement::DecisionModel]);
                } else {
                    assert!(required.is_empty());
                }
            }
        }
    }

    #[test]
    fn question_family_reuses_interaction_owner_without_publishing_a_tool() {
        let catalog = BuiltinToolCatalog::load_cloud().unwrap();
        let mut definition = catalog.by_wire_name("interaction_request").unwrap().clone();
        definition.key = crate::ToolKey::from_versioned_key(
            musteragent_identity::VersionedKey::new("interaction.question@1").unwrap(),
        );
        assert_eq!(
            classify(&definition).unwrap(),
            CapabilityHandlerKind::Interaction
        );
        // Similar names cannot inherit the family through a prefix match.
        definition.key = crate::ToolKey::from_versioned_key(
            musteragent_identity::VersionedKey::new("interaction.questionnaire@1").unwrap(),
        );
        assert!(matches!(
            classify(&definition),
            Err(CapabilityAssemblyError::UnregisteredRuntimeCapability(_))
        ));
    }

    #[test]
    fn active_cloud_and_device_releases_have_closed_handler_ownership() {
        let cloud = BuiltinToolCatalog::load_cloud().expect("Cloud catalog");
        let device = BuiltinToolCatalog::load_device().expect("Device catalog");

        let cloud_plan = CapabilityAssemblyPlan::from_catalog(&cloud).expect("Cloud plan");
        let device_plan = CapabilityAssemblyPlan::from_catalog(&device).expect("Device plan");

        assert_eq!(cloud_plan.entries().len(), cloud.definitions().len());
        assert_eq!(device_plan.entries().len(), device.definitions().len());
    }

    #[test]
    fn shared_logical_tools_have_one_handler_owner_across_platforms() {
        let cloud = BuiltinToolCatalog::load_cloud().expect("Cloud catalog");
        let device = BuiltinToolCatalog::load_device().expect("Device catalog");
        let cloud_plan = CapabilityAssemblyPlan::from_catalog(&cloud).expect("Cloud plan");
        let device_plan = CapabilityAssemblyPlan::from_catalog(&device).expect("Device plan");
        let cloud_kinds = cloud_plan
            .entries()
            .iter()
            .map(|entry| (entry.definition.key.logical_key(), entry.handler))
            .collect::<BTreeMap<_, _>>();

        for entry in device_plan.entries() {
            if let Some(cloud_kind) = cloud_kinds.get(entry.definition.key.logical_key()) {
                assert_eq!(
                    *cloud_kind,
                    entry.handler,
                    "{} changed handler ownership across platforms",
                    entry.definition.key.logical_key()
                );
            }
        }
    }

    #[test]
    fn browser_execution_predicate_matches_handler_ownership() {
        let mut browser = 0;
        for catalog in [
            BuiltinToolCatalog::load_cloud().unwrap(),
            BuiltinToolCatalog::load_device().unwrap(),
        ] {
            let plan = CapabilityAssemblyPlan::from_catalog(&catalog).unwrap();
            for entry in plan.entries() {
                let key = entry.definition.key.as_str();
                assert_eq!(
                    requires_browser_execution(key),
                    entry.handler.requires_browser_execution(),
                    "{key}"
                );
                browser += usize::from(entry.handler.requires_browser_execution());
            }
        }
        assert!(
            browser > 0,
            "no browser-executed tool in the built-in catalogs"
        );
        for key in [
            "data.python.run@11",
            "data.python.run",
            "sandbox.environment.prepare@1",
            "sandbox.operation.inspect@1",
            "application.web.build@1",
        ] {
            assert!(requires_browser_execution(key), "{key}");
        }
        for key in [
            "device.terminal.exec@3",
            "workspace.file.read@1",
            "data.python.runner@1",
            "web.fetch@1",
        ] {
            assert!(!requires_browser_execution(key), "{key}");
        }
    }
}
