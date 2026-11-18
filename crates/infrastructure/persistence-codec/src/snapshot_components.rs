use prost::Message;
use thiserror::Error;

use crate::wire::RuntimeSnapshotContent;

pub const RUNTIME_SNAPSHOT_COMPONENT_COUNT: usize = 10;

#[derive(Debug, Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RuntimeSnapshotComponentKind {
    Schema,
    Agent,
    Prompt,
    Models,
    Skills,
    Tools,
    Authority,
    Personalization,
    RuntimePolicy,
    Discovery,
}

impl RuntimeSnapshotComponentKind {
    pub const ALL: [Self; RUNTIME_SNAPSHOT_COMPONENT_COUNT] = [
        Self::Schema,
        Self::Agent,
        Self::Prompt,
        Self::Models,
        Self::Skills,
        Self::Tools,
        Self::Authority,
        Self::Personalization,
        Self::RuntimePolicy,
        Self::Discovery,
    ];

    #[must_use]
    pub const fn ordinal(self) -> u8 {
        match self {
            Self::Schema => 0,
            Self::Agent => 1,
            Self::Prompt => 2,
            Self::Models => 3,
            Self::Skills => 4,
            Self::Tools => 5,
            Self::Authority => 6,
            Self::Personalization => 7,
            Self::RuntimePolicy => 8,
            Self::Discovery => 9,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Agent => "agent",
            Self::Prompt => "prompt",
            Self::Models => "models",
            Self::Skills => "skills",
            Self::Tools => "tools",
            Self::Authority => "authority",
            Self::Personalization => "personalization",
            Self::RuntimePolicy => "runtime-policy",
            Self::Discovery => "discovery",
        }
    }

    // Exactly the field ownership used by split_runtime_snapshot_components.
    // Reference discovery checks raw fragments before any Prost merge can drop
    // unknown fields or overwrite a singular value from another component.
    pub(crate) fn contains_field(self, number: u32) -> bool {
        match self {
            Self::Schema => number == 1,
            Self::Agent => number == 2,
            Self::Prompt => number == 3,
            Self::Models => matches!(number, 4 | 15),
            Self::Skills => number == 5,
            Self::Tools => matches!(number, 6 | 18),
            Self::Authority => matches!(number, 7 | 8 | 17 | 19),
            Self::Personalization => matches!(number, 9 | 16),
            Self::RuntimePolicy => matches!(number, 10..=13),
            Self::Discovery => number == 14,
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "schema" => Some(Self::Schema),
            "agent" => Some(Self::Agent),
            "prompt" => Some(Self::Prompt),
            "models" => Some(Self::Models),
            "skills" => Some(Self::Skills),
            "tools" => Some(Self::Tools),
            "authority" => Some(Self::Authority),
            "personalization" => Some(Self::Personalization),
            "runtime-policy" => Some(Self::RuntimePolicy),
            "discovery" => Some(Self::Discovery),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeSnapshotComponent {
    pub kind: RuntimeSnapshotComponentKind,
    pub encoded_fragment: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Eq, Error, PartialEq)]
pub enum RuntimeSnapshotComponentsError {
    #[error("runtime Snapshot protobuf is malformed")]
    Malformed,
    #[error("runtime Snapshot component set is incomplete or out of order")]
    InvalidManifest,
}

pub fn split_runtime_snapshot_components(
    canonical: &[u8],
) -> Result<Vec<RuntimeSnapshotComponent>, RuntimeSnapshotComponentsError> {
    let snapshot = RuntimeSnapshotContent::decode(canonical)
        .map_err(|_| RuntimeSnapshotComponentsError::Malformed)?;
    let fragment = |value: RuntimeSnapshotContent| value.encode_to_vec();
    Ok(vec![
        component(
            RuntimeSnapshotComponentKind::Schema,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                application_catalogs: vec![],
                schema_version: snapshot.schema_version,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Agent,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                application_catalogs: vec![],
                agent: snapshot.agent,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Prompt,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                prompt: snapshot.prompt,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Models,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                model: snapshot.model,
                auxiliary_models: snapshot.auxiliary_models,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Skills,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                skills: snapshot.skills,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Tools,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                tools: snapshot.tools,
                application_catalogs: snapshot.application_catalogs,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Authority,
            fragment(RuntimeSnapshotContent {
                application_catalogs: vec![],
                product_release: snapshot.product_release,
                owner_goal_authority: snapshot.owner_goal_authority,
                capabilities: snapshot.capabilities,
                policy: snapshot.policy,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Personalization,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                memory: snapshot.memory,
                instincts: snapshot.instincts,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::RuntimePolicy,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                context_policy: snapshot.context_policy,
                protocol_versions: snapshot.protocol_versions,
                output_policy: snapshot.output_policy,
                execution_policy: snapshot.execution_policy,
                ..Default::default()
            }),
        ),
        component(
            RuntimeSnapshotComponentKind::Discovery,
            fragment(RuntimeSnapshotContent {
                owner_goal_authority: None,
                discovery: snapshot.discovery,
                ..Default::default()
            }),
        ),
    ])
}

pub fn assemble_runtime_snapshot_components(
    components: &[RuntimeSnapshotComponent],
) -> Result<Vec<u8>, RuntimeSnapshotComponentsError> {
    if components.len() != RUNTIME_SNAPSHOT_COMPONENT_COUNT
        || components
            .iter()
            .zip(RuntimeSnapshotComponentKind::ALL)
            .any(|(component, expected)| component.kind != expected)
    {
        return Err(RuntimeSnapshotComponentsError::InvalidManifest);
    }
    let mut snapshot = RuntimeSnapshotContent::default();
    for component in components {
        snapshot
            .merge(component.encoded_fragment.as_slice())
            .map_err(|_| RuntimeSnapshotComponentsError::Malformed)?;
    }
    Ok(snapshot.encode_to_vec())
}

fn component(
    kind: RuntimeSnapshotComponentKind,
    encoded_fragment: Vec<u8>,
) -> RuntimeSnapshotComponent {
    RuntimeSnapshotComponent {
        kind,
        encoded_fragment,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_incomplete_and_reordered_manifests() {
        let mut components = RuntimeSnapshotComponentKind::ALL
            .into_iter()
            .map(|kind| RuntimeSnapshotComponent {
                kind,
                encoded_fragment: Vec::new(),
            })
            .collect::<Vec<_>>();
        components.swap(0, 1);
        assert_eq!(
            assemble_runtime_snapshot_components(&components),
            Err(RuntimeSnapshotComponentsError::InvalidManifest)
        );
        components.pop();
        assert_eq!(
            assemble_runtime_snapshot_components(&components),
            Err(RuntimeSnapshotComponentsError::InvalidManifest)
        );
    }

    #[test]
    fn current_schema_preserves_every_component_byte_for_byte() {
        let schema = "musteragent.runtime.snapshot.v17";
        let snapshot = RuntimeSnapshotContent {
            schema_version: schema.into(),
            agent: Some(Default::default()),
            prompt: Some(Default::default()),
            model: Some(Default::default()),
            auxiliary_models: vec![Default::default()],
            skills: vec![Default::default()],
            tools: vec![Default::default()],
            capabilities: vec![Default::default()],
            policy: Some(Default::default()),
            memory: Some(Default::default()),
            instincts: vec![Default::default()],
            context_policy: Some(Default::default()),
            protocol_versions: Some(Default::default()),
            output_policy: Some(Default::default()),
            execution_policy: Some(Default::default()),
            discovery: Some(Default::default()),
            owner_goal_authority: Some(Default::default()),
            application_catalogs: vec![Default::default()],
            product_release: Some(Default::default()),
        };
        let bytes = snapshot.encode_to_vec();
        let parts = split_runtime_snapshot_components(&bytes).expect("split all fields");
        assert_eq!(parts.len(), RUNTIME_SNAPSHOT_COMPONENT_COUNT);
        assert!(parts.iter().all(|part| !part.encoded_fragment.is_empty()));
        assert_eq!(
            assemble_runtime_snapshot_components(&parts)
                .expect("assemble complete current snapshot"),
            bytes
        );
    }
}

#[cfg(test)]
mod product_binding_test {
    use super::*;
    #[test]
    fn publication_survives_component_storage_without_changing_bytes() {
        let mut snapshot = RuntimeSnapshotContent {
            owner_goal_authority: None,
            application_catalogs: vec![],
            schema_version: "musteragent.runtime.snapshot.v17".into(),
            ..Default::default()
        };
        let legacy = snapshot.encode_to_vec();
        assert_eq!(
            assemble_runtime_snapshot_components(
                &split_runtime_snapshot_components(&legacy).unwrap()
            )
            .unwrap(),
            legacy
        );
        snapshot.product_release = Some(crate::wire::ProductReleaseRef {
            schema_version: "musteragent.runtime.product-release.v1".into(),
            product_id: "musteragent".into(),
            release_id: "musteragent.kernel@1".into(),
            sha256: vec![1; 32],
        });
        let canonical = snapshot.encode_to_vec();
        let components = split_runtime_snapshot_components(&canonical).unwrap();
        assert_eq!(
            assemble_runtime_snapshot_components(&components).unwrap(),
            canonical
        );
        let authority = components
            .iter()
            .find(|c| c.kind == RuntimeSnapshotComponentKind::Authority)
            .unwrap();
        assert_eq!(
            RuntimeSnapshotContent::decode(authority.encoded_fragment.as_slice())
                .unwrap()
                .product_release,
            snapshot.product_release
        );
    }
}
