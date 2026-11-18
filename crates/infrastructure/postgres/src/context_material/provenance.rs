//! Physical Assistant source recovery for target-route context compilation.

use std::collections::BTreeMap;

use musteragent_context::AssistantOrigin;
use musteragent_identity::{ConversationSeq, InvocationId, RunId, TenantId, VersionedKey};
use musteragent_model::{ModelKey, SignedModelRouteHash};
use musteragent_runtime_model::CanonicalRole;
use sqlx::{PgPool, Row};

use super::{LedgerFacts, MessageSource, ModelCompletionSource, PostgresContextMaterialError};
use crate::context_material::material_codec::id;

/// Native dialogue prose has a model completion but no public message row.
/// Only its owning chat can use it to finish the latest unpublished turn.
pub(super) fn pending_native_reply(
    kind: musteragent_activity::ActivityKind,
    run: RunId,
    sections: &[musteragent_runtime_snapshot::PromptSectionSnapshot],
    facts: &LedgerFacts,
) -> Option<InvocationId> {
    use musteragent_execution_context::DialogueOutputContract;
    let invocation = facts.primary_invocation_id?;
    if kind != musteragent_activity::ActivityKind::Chat
        || facts.model_completions.get(&invocation)?.run_id != Some(run)
        || facts.retracted_runs.contains(&run)
    {
        return None;
    }
    let mut pins = sections
        .iter()
        .filter(|p| p.section_key.as_str() == "dialogue.output-contract");
    let contract = DialogueOutputContract::from_prompt_ref(&pins.next()?.content_ref)?;
    (pins.next().is_none() && contract.is_native()).then_some(invocation)
}

pub(super) async fn load_assistant_origins(
    pool: &PgPool,
    tenant_id: TenantId,
    facts: &LedgerFacts,
) -> Result<BTreeMap<InvocationId, AssistantOrigin>, PostgresContextMaterialError> {
    let invocation_ids = facts
        .model_completions
        .iter()
        .filter(|(_, completion)| {
            completion
                .run_id
                .is_none_or(|run_id| !facts.retracted_runs.contains(&run_id))
        })
        .map(|(invocation_id, _)| invocation_id.into_uuid())
        .chain(facts.model_recovery.map(|id| id.into_uuid()))
        .collect::<Vec<_>>();
    if invocation_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(
        "SELECT invocation_id, model_route_ref, route_identity_hash,
                requested_model_key, response_model_key
         FROM runtime.model_invocations
         WHERE tenant_id = $1
           AND invocation_id = ANY($2)
           AND (status = 2 OR (status = 3 AND terminal_stop_reason IN (3, 7)))",
    )
    .bind(tenant_id.into_uuid())
    .bind(&invocation_ids)
    .fetch_all(pool)
    .await?;
    let mut origins = BTreeMap::new();
    for row in rows {
        let invocation_id: InvocationId = id(row.try_get("invocation_id")?)?;
        let route_hash = row.try_get::<Option<Vec<u8>>, _>("route_identity_hash")?;
        let requested_model_key = row.try_get::<Option<String>, _>("requested_model_key")?;
        let response_model_key = row.try_get::<Option<String>, _>("response_model_key")?;
        let (Some(route_hash), Some(requested_model_key), Some(response_model_key)) =
            (route_hash, requested_model_key, response_model_key)
        else {
            // Additive migrations deliberately left historical identity
            // unknown. Legacy output remains portable, never exact replay.
            continue;
        };
        let origin = AssistantOrigin {
            invocation_id,
            model_route_ref: VersionedKey::new(row.try_get::<String, _>("model_route_ref")?)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            route_identity_hash: SignedModelRouteHash::from_bytes(route_hash)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            requested_model_key: ModelKey::new(requested_model_key)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            response_model_key: ModelKey::new(response_model_key)
                .map_err(|_| PostgresContextMaterialError::CorruptState)?,
        };
        if origins.insert(invocation_id, origin).is_some() {
            return Err(PostgresContextMaterialError::CorruptState);
        }
    }
    Ok(origins)
}

pub(super) fn message_assistant_origin(
    source: &MessageSource,
    model_completions: &BTreeMap<InvocationId, ModelCompletionSource>,
    assistant_origins: &BTreeMap<InvocationId, AssistantOrigin>,
) -> Option<AssistantOrigin> {
    if source.role != CanonicalRole::Assistant {
        return None;
    }
    source
        .source_invocation_id
        .filter(|invocation_id| model_completions.contains_key(invocation_id))
        .and_then(|invocation_id| assistant_origins.get(&invocation_id))
        .cloned()
}

pub(super) fn latest_completion_for_run(
    model_completions: &BTreeMap<InvocationId, ModelCompletionSource>,
    run_id: Option<RunId>,
    before: ConversationSeq,
) -> Option<InvocationId> {
    let run_id = run_id?;
    model_completions
        .iter()
        .filter(|(_, completion)| completion.run_id == Some(run_id) && completion.sequence < before)
        .max_by_key(|(_, completion)| completion.sequence)
        .map(|(invocation_id, _)| *invocation_id)
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::{ContentId, LedgerRecordId};

    use super::*;

    #[test]
    fn aggregated_assistant_message_binds_to_latest_same_run_completion() {
        let source_run = RunId::generate();
        let other_run = RunId::generate();
        let first = InvocationId::generate();
        let latest = InvocationId::generate();
        let unrelated = InvocationId::generate();
        let completions = BTreeMap::from([
            (first, completion(source_run, 2)),
            (latest, completion(source_run, 7)),
            (unrelated, completion(other_run, 8)),
        ]);

        assert_eq!(
            latest_completion_for_run(
                &completions,
                Some(source_run),
                ConversationSeq::new(9).expect("message sequence"),
            ),
            Some(latest),
        );
    }

    #[test]
    fn private_reply_requires_the_owning_native_chat_and_its_latest_primary() {
        use musteragent_activity::ActivityKind;
        use musteragent_execution_context::{DialogueOutputContract, PublicDeliveryContract};
        use musteragent_runtime_snapshot::PromptSectionSnapshot;

        let run = RunId::generate();
        let invocation = InvocationId::generate();
        let mut facts = LedgerFacts::default();
        facts
            .model_completions
            .insert(invocation, completion(run, 5));
        let pin = |contract: DialogueOutputContract| {
            let prompt = contract.prompt();
            PromptSectionSnapshot {
                section_key: musteragent_identity::DottedKey::new("dialogue.output-contract")
                    .unwrap(),
                content_ref: ContentRef {
                    content_id: ContentId::generate(),
                    media_type: MediaType::new("text/plain").unwrap(),
                    byte_length: prompt.len() as u64,
                    sha256: Sha256::digest(prompt.as_bytes()),
                },
            }
        };
        let native = [pin(DialogueOutputContract::CURRENT)];
        let reply =
            |facts: &LedgerFacts| pending_native_reply(ActivityKind::Chat, run, &native, facts);
        assert_eq!(reply(&facts), None);
        facts.primary_invocation_id = Some(invocation);
        assert_eq!(reply(&facts), Some(invocation));
        assert_eq!(
            pending_native_reply(ActivityKind::Chat, RunId::generate(), &native, &facts),
            None
        );
        assert_eq!(
            pending_native_reply(ActivityKind::Evaluation, run, &native, &facts),
            None
        );
        assert_eq!(
            pending_native_reply(
                ActivityKind::Chat,
                run,
                &[pin(PublicDeliveryContract::V7.into())],
                &facts
            ),
            None
        );
        facts.primary_invocation_id = Some(InvocationId::generate());
        assert_eq!(reply(&facts), None);
        facts.primary_invocation_id = Some(invocation);
        facts.retracted_runs.insert(run);
        assert_eq!(reply(&facts), None);
    }

    fn completion(run_id: RunId, sequence: u64) -> ModelCompletionSource {
        let bytes = b"physical segment";
        ModelCompletionSource {
            sequence: ConversationSeq::new(sequence).expect("sequence"),
            record_id: LedgerRecordId::generate(),
            run_id: Some(run_id),
            response_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("text/plain").expect("media type"),
                byte_length: u64::try_from(bytes.len()).expect("length"),
                sha256: Sha256::digest(bytes),
            },
            branch_id: None,
        }
    }
}
