//! Explicit remember preparation captures the original head once. A retry may
//! not silently rebase an approved update onto a newer fact or turn it into a no-op.
use super::preparation::{content_error, ensure_running, identity, put_exact};
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RememberFacts {
    head: Option<prepared_facts::Head>,
    origin: prepared_facts::Origin,
}

impl<M, E> BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    pub(super) async fn prepare_remember(
        &self,
        mut context: MemoryToolContext,
        bytes: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let approval_interaction_id = context
            .approval_interaction_id
            .ok_or(MemoryToolError::IdentityMismatch)?;
        let origin = context
            .user_authored_origin
            .clone()
            .ok_or(MemoryToolError::IdentityMismatch)?;
        let frozen = write_journal::freeze(
            self.content.as_ref(),
            self.write_planner.as_ref(),
            &context,
            bytes,
            execution,
            async {
                let input =
                    UserMemoryDocument::decode(bytes).map_err(|_| MemoryToolError::InvalidInput)?;
                let key = IdempotencyKey::new(format!("user-memory:{}", input.fact_key))
                    .map_err(|_| MemoryToolError::InvalidInput)?;
                let existing = crate::retrieval::read_stage(
                    execution,
                    self.memory.find_user_by_key(
                        context.actor.tenant_id,
                        context.actor.principal_id,
                        key.clone(),
                    ),
                )
                .await
                .map_err(map_retrieval_error)?
                .map_err(|_| MemoryToolError::Store)?;
                validate_head(&context, &key, existing.as_ref())?;
                Ok(RememberFacts {
                    head: existing.as_ref().map(prepared_facts::Head::from),
                    origin: (&origin).into(),
                })
            },
        )
        .await?;
        let musteragent_memory_business::preparation::WritePlan::Remember(input) =
            frozen.business.plan
        else {
            return Err(MemoryToolError::InvalidResult);
        };
        if frozen.facts.origin.restore()? != origin {
            return Err(MemoryToolError::IdentityMismatch);
        }
        context.now = frozen.at;
        let memory_key = IdempotencyKey::new(format!("user-memory:{}", input.fact_key))
            .map_err(|_| MemoryToolError::InvalidInput)?;
        let existing = frozen
            .facts
            .head
            .map(prepared_facts::Head::restore)
            .transpose()?;
        validate_head(&context, &memory_key, existing.as_ref())?;
        let expected_current_version_id =
            existing.as_ref().map(|hit| hit.record.current_version_id);
        let document = input.encode().map_err(|_| MemoryToolError::InvalidResult)?;
        let new_hash = Sha256::digest(&document);
        let unchanged = existing
            .as_ref()
            .is_some_and(|hit| hit.version.content_ref.sha256 == new_hash);
        let (record, version, changed) = if let Some(hit) = existing {
            if unchanged {
                (hit.record, hit.version, false)
            } else {
                let next = hit
                    .version
                    .version
                    .get()
                    .checked_add(1)
                    .and_then(|value| VersionNumber::new(value).ok())
                    .ok_or(MemoryToolError::VersionOverflow)?;
                let content_ref = put(
                    self.content.as_ref(),
                    &context,
                    "remember-content",
                    document.clone(),
                    execution,
                )
                .await?;
                let version_id = version_id(context.invocation.invocation_id)?;
                (
                    MemoryRecord {
                        current_version_id: version_id,
                        lifecycle: MemoryLifecycle::Active,
                        updated_at: context.now,
                        revoked_at: None,
                        ..hit.record
                    },
                    MemoryVersion {
                        memory_version_id: version_id,
                        memory_id: hit.record.memory_id,
                        version: next,
                        source: MemorySource::UserAuthored,
                        content_ref,
                        confidence: MemoryConfidenceBps::new(10_000)
                            .map_err(|_| MemoryToolError::InvalidResult)?,
                        data_classification: DataClassification::Confidential,
                        source_run_ids: BTreeSet::from([context.run_context.run_id]),
                        evidence_record_ids: BTreeSet::from([context.evidence_record_id]),
                        created_at: context.now,
                        valid_until: None,
                    },
                    true,
                )
            }
        } else {
            let memory_id = memory_id(
                context.actor.tenant_id,
                context.actor.principal_id,
                &input.fact_key,
            )?;
            let version_id = version_id(context.invocation.invocation_id)?;
            let content_ref = put(
                self.content.as_ref(),
                &context,
                "remember-content",
                document.clone(),
                execution,
            )
            .await?;
            (
                MemoryRecord {
                    memory_id,
                    tenant_id: context.actor.tenant_id,
                    scope: MemoryScope::User {
                        principal_id: context.actor.principal_id,
                    },
                    memory_key,
                    kind: memory_kind(&input.fact_key),
                    lifecycle: MemoryLifecycle::Active,
                    current_version_id: version_id,
                    created_at: context.now,
                    updated_at: context.now,
                    revoked_at: None,
                },
                MemoryVersion {
                    memory_version_id: version_id,
                    memory_id,
                    version: VersionNumber::new(1).map_err(|_| MemoryToolError::InvalidResult)?,
                    source: MemorySource::UserAuthored,
                    content_ref,
                    confidence: MemoryConfidenceBps::new(10_000)
                        .map_err(|_| MemoryToolError::InvalidResult)?,
                    data_classification: DataClassification::Confidential,
                    source_run_ids: BTreeSet::from([context.run_context.run_id]),
                    evidence_record_ids: BTreeSet::from([context.evidence_record_id]),
                    created_at: context.now,
                    valid_until: None,
                },
                true,
            )
        };
        let output = serde_json::to_vec(&RememberOutput {
            experience_id: record.memory_id.to_string(),
            version_id: version.memory_version_id.to_string(),
            revision: version.version.get(),
            status: "active",
            changed,
        })
        .map_err(|_| MemoryToolError::InvalidResult)?;
        let result_ref = put(
            self.content.as_ref(),
            &context,
            "remember-result",
            output,
            execution,
        )
        .await?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: context.invocation.result_schema.clone(),
            provider_request_ref: None,
        };
        let search_document = MemorySearchDocument::from_content(&record, &version, &document)
            .map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(MemoryToolPreparation::Commit {
            result,
            mutation: Box::new(MemoryToolMutation::Remember(Box::new(RememberUserMemory {
                expected_current_version_id,
                actor_principal_id: context.actor.principal_id,
                authorization: ExplicitRememberAuthorization {
                    approval_interaction_id,
                    invocation_id: context.invocation.invocation_id,
                    request_ref: context.invocation.request_ref,
                    origin,
                },
                record,
                version,
                search_document,
            }))),
        })
    }
}

fn validate_head(
    context: &MemoryToolContext,
    key: &IdempotencyKey,
    head: Option<&MemorySearchHit>,
) -> Result<(), MemoryToolError> {
    if let Some(head) = head {
        head.record
            .validate()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        head.version
            .validate()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        if head.record.tenant_id != context.actor.tenant_id
            || head.record.scope
                != (MemoryScope::User {
                    principal_id: context.actor.principal_id,
                })
            || head.record.memory_key != *key
            || head.record.memory_id != head.version.memory_id
            || head.record.current_version_id != head.version.memory_version_id
        {
            return Err(MemoryToolError::IdentityMismatch);
        }
        if head.record.updated_at > context.now || head.version.created_at > context.now {
            return Err(MemoryToolError::Store);
        }
        if head.record.lifecycle != MemoryLifecycle::Active
            || head.version.valid_until.is_some_and(|at| at <= context.now)
        {
            return Err(MemoryToolError::InactiveMemory);
        }
    }
    Ok(())
}

async fn put(
    content: &dyn ContentStore,
    context: &MemoryToolContext,
    purpose: &str,
    bytes: Vec<u8>,
    execution: Option<&musteragent_runtime_capability::OperationContext>,
) -> Result<musteragent_content_ref::ContentRef, MemoryToolError> {
    ensure_running(execution)?;
    let reference = put_exact(
        content,
        context.actor.tenant_id,
        identity(context.invocation.invocation_id, purpose),
        "application/json",
        bytes,
    )
    .await
    .map_err(content_error)?;
    ensure_running(execution)?;
    Ok(reference)
}
