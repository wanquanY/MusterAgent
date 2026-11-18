//! Reuse pure, verified Snapshot material after this iteration's fresh source
//! load. Entries share the ContentStore's 128 MiB LRU and erasure generation.
//! Per-Run directives and history visibility remain current on every call.
use super::*;
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_runtime_content::{
    ContentCacheGeneration, RetainedContentMaterial, VerifiedContentCache,
};
use musteragent_runtime_model::JsonValue;
use sha2::Digest as _;
mod history;
mod message_identity;
mod tool_views;

pub(super) struct MaterialReuse(Option<(Arc<VerifiedContentCache>, ContentCacheGeneration)>);
impl MaterialReuse {
    pub(super) fn discard(&self, material: &ContextMaterial) {
        if let Some((cache, _)) = &self.0 {
            let _ = cache.invalidate_tenant(material.run_context.tenant_id);
        }
    }
    pub(super) fn begin(cache: Option<Arc<VerifiedContentCache>>) -> Self {
        Self(cache.and_then(|cache| {
            cache
                .begin_generation()
                .ok()
                .map(|generation| (cache, generation))
        }))
    }
    pub(super) fn instructions(
        &self,
        material: &ContextMaterial,
    ) -> Result<Vec<ModelText>, ContextPlanError> {
        self.memo(
            material.run_context.tenant_id,
            "context.instructions.v2",
            || instruction_fingerprint(material),
            || build_instructions(material).map(Instructions),
        )
        .map(|value| value.0.clone())
    }
    pub(super) fn tools(
        &self,
        codec: &dyn ContextCodec,
        material: &ContextMaterial,
    ) -> Result<Vec<CanonicalToolDefinition>, ContextPlanError> {
        let Some(codec_identity) = codec.material_cache_identity() else {
            return build_tools(codec, material);
        };
        self.memo(
            material.run_context.tenant_id,
            "context.tools.v4",
            || tool_fingerprint(material, codec_identity),
            || build_tools(codec, material).map(Tools),
        )
        .map(|value| value.0.clone())
    }
    fn memo<T: RetainedContentMaterial>(
        &self,
        tenant_id: musteragent_identity::TenantId,
        purpose: &'static str,
        fingerprint: impl FnOnce() -> Result<Sha256, ContextPlanError>,
        build: impl FnOnce() -> Result<T, ContextPlanError>,
    ) -> Result<Arc<T>, ContextPlanError> {
        let Some((cache, generation)) = &self.0 else {
            return build().map(Arc::new);
        };
        let read = generation.for_tenant(tenant_id);
        let digest = fingerprint()?;
        if let Ok(Some(value)) = cache.material_after_authorization::<T>(&read, purpose, digest) {
            tracing::debug!(
                context.material_cache.purpose = purpose,
                context.material_cache.hit = true,
                "Context material reused after current authorization"
            );
            return Ok(value);
        }
        let value = Arc::new(build()?);
        // Cache invalidation/capacity never changes the computed model input.
        // The final plan transaction still rejects changed source authority.
        let retained =
            cache.publish_material_arc_after_authorization(&read, purpose, digest, value.clone());
        Ok(retained.unwrap_or(value))
    }
}

struct Fingerprint(sha2::Sha256);
impl Fingerprint {
    fn new(kind: &'static [u8]) -> Self {
        Self(sha2::Sha256::new_with_prefix(kind))
    }
    fn field(&mut self, value: &[u8]) {
        self.number(value.len() as u64);
        self.0.update(value);
    }
    fn number(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }
    fn optional<T>(&mut self, value: Option<T>, append: impl FnOnce(&mut Self, T)) {
        self.number(u64::from(value.is_some()));
        if let Some(value) = value {
            append(self, value);
        }
    }
    fn content(&mut self, reference: &ContentRef) {
        self.field(reference.content_id.into_uuid().as_bytes());
        self.field(reference.media_type.as_str().as_bytes());
        self.number(reference.byte_length);
        self.field(&reference.sha256.into_bytes());
    }
    fn finish(self) -> Sha256 {
        Sha256::from_bytes(self.0.finalize()).expect("SHA-256 output is 32 bytes")
    }
}
fn instruction_fingerprint(material: &ContextMaterial) -> Result<Sha256, ContextPlanError> {
    let mut key = Fingerprint::new(b"musteragent.context.instructions.v1\0");
    key.number(material.prompt_sections.len() as u64);
    for content in &material.prompt_sections {
        key.content(&content.content_ref);
    }
    key.optional(material.agent_instructions.as_ref(), |key, value| {
        key.field(value.agent_id.into_uuid().as_bytes());
        key.number(value.revision.get());
        if !value.scene_members.is_empty() {
            key.field(b"scene-members");
            key.number(value.scene_members.len() as u64);
            for member in &value.scene_members {
                key.field(member.agent_id.into_uuid().as_bytes());
                key.field(member.name.as_bytes());
                key.field(member.responsibility.as_bytes());
            }
        }
        key.optional(value.content.as_ref(), |key, content| {
            key.content(&content.content_ref)
        });
    });
    key.number(material.disclosed_skills.len() as u64);
    for skill in &material.disclosed_skills {
        key.field(skill.skill_ref.as_str().as_bytes());
        key.field(skill.skill_version_id.into_uuid().as_bytes());
        key.number(u64::from(skill.disclosure_ordinal.get()));
        key.field(&skill.instructions_hash.into_bytes());
        key.content(&skill.instructions.content_ref);
    }
    key.number(material.instincts.len() as u64);
    for instinct in &material.instincts {
        key.field(instinct.instinct_id.into_uuid().as_bytes());
        key.number(instinct.revision.get());
        key.content(&instinct.payload.content_ref);
    }
    key.number(material.memories.len() as u64);
    for memory in &material.memories {
        key.field(memory.memory_version_id.into_uuid().as_bytes());
        key.content(&memory.content.content_ref);
    }
    key.number(u64::from(material.execution_plan.is_some()));
    if let Some(plan) = &material.execution_plan {
        key.number(plan.revision.get());
        key.content(&plan.content.content_ref);
    }
    Ok(key.finish())
}
fn tool_fingerprint(
    material: &ContextMaterial,
    codec: &'static str,
) -> Result<Sha256, ContextPlanError> {
    let mut key = Fingerprint::new(b"musteragent.context.tools.v4\0");
    key.number(u64::from(crate::progress_note::enabled(material)));
    for tool in &material.runtime_snapshot.tools {
        key.field(tool.manifest.key.as_str().as_bytes());
        key.field(tool.manifest.wire_name.as_str().as_bytes());
    }
    for selection in &material.runtime_snapshot.application_catalogs {
        key.field(&selection.selection_sha256().into_bytes());
    }
    key.field(codec.as_bytes());
    key.number(u64::from(
        material.tool_policy == ContextToolPolicy::DenyAll,
    ));
    key.number(material.exposed_tool_keys.len() as u64);
    for reference in &material.exposed_tool_keys {
        let (tool, model_name) = crate::resolved_tools::exposed_tool(
            &material.runtime_snapshot.tools,
            &material.resolved_tools,
            reference,
        )?;
        key.field(tool.manifest.key.as_str().as_bytes());
        key.field(tool.manifest.wire_name.as_str().as_bytes());
        key.optional(model_name, |key, value| {
            key.field(value.as_str().as_bytes())
        });
        key.field(&Sha256::digest(tool.description.as_bytes()).into_bytes());
        key.field(tool.manifest.input_schema.schema_key.as_str().as_bytes());
        // Snapshot validation has just verified the exact JSON bytes against
        // this schema hash. The codec identity also participates in the key.
        key.field(&tool.manifest.input_schema.schema_hash.into_bytes());
    }
    Ok(key.finish())
}

struct Instructions(Vec<ModelText>);
impl RetainedContentMaterial for Instructions {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.0
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ModelText>()),
            )
            .saturating_add(self.0.iter().fold(0_usize, |n, text| {
                n.saturating_add(text.as_str().len()).saturating_add(32)
            }))
    }
}
struct Tools(Vec<CanonicalToolDefinition>);
impl RetainedContentMaterial for Tools {
    fn retained_bytes(&self) -> usize {
        self.0.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.0
                    .capacity()
                    .saturating_mul(std::mem::size_of::<CanonicalToolDefinition>()),
            ),
            |n, tool| {
                n.saturating_add(128)
                    .saturating_add(tool.key.as_str().len())
                    .saturating_add(tool.name.as_str().len())
                    .saturating_add(tool.model_name.as_str().len())
                    .saturating_add(tool.description.as_str().len())
                    .saturating_add(32)
                    .saturating_add(tool.input_schema_ref.schema_key.as_str().len())
                    .saturating_add(json_allocations(&tool.input_schema))
            },
        )
    }
}
fn json_allocations(value: &JsonValue) -> usize {
    match value {
        JsonValue::String(value) => value.capacity().saturating_add(32),
        JsonValue::Array(values) => values.iter().fold(
            32_usize.saturating_add(
                values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<JsonValue>()),
            ),
            |n, value| n.saturating_add(json_allocations(value)),
        ),
        // A minimally occupied BTreeMap node still allocates all node slots.
        // Charging a whole conservative 1 KiB node per key avoids assuming
        // that every leaf is full when enforcing the shared memory budget.
        JsonValue::Object(values) => values.iter().fold(0_usize, |n, (key, value)| {
            n.saturating_add(1024)
                .saturating_add(key.capacity())
                .saturating_add(json_allocations(value))
        }),
        _ => 0,
    }
}
