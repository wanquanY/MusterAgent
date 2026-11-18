mod approval;
mod inspection;
mod inspection_projection;
mod preparation;
mod prepared_evolution;
mod prepared_facts;
mod proposal;
mod publish;
mod remember;
mod search;
mod write_journal;

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use crate::user_memory_document::UserMemoryDocument;
use musteragent_content_ref::{MediaType, Sha256};
use musteragent_evolution::{
    CandidateStatus, ConfidenceBps, EvolutionScope, ExperienceCandidate, ExperienceKind,
};
use musteragent_identity::{
    Actor, AgentId, EvolutionPromotionId, EvolutionProposalId, IdempotencyKey, InteractionId,
    LedgerRecordId, MemoryId, MemoryVersionId, PrincipalKind, TenantId, UtcTimestamp,
    VersionNumber,
};
use musteragent_memory::{
    MemoryConfidenceBps, MemoryKind, MemoryLifecycle, MemoryRecord, MemoryScope, MemorySource,
    MemoryVersion,
};
use musteragent_policy::DataClassification;
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityResult};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    DynamicMemorySearchRequest, EXPERIENCE_CLAIM_MEDIA_TYPE, EvolutionStore,
    ExperienceClaimDocument, ExperienceSearch, ExplicitRememberAuthorization,
    MemoryApplicationError, MemoryEmbeddingDeadline, MemorySearch, MemorySearchDocument,
    MemorySearchHit, MemorySearchIndex, MemoryStore, PrepareEvolutionPublication,
    PromoteExperience, PublishedMemoryRetriever, RememberUserMemory, UserAuthoredRunOrigin,
    prepare_evolution_publication,
};

const MEMORY_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x01,
]);
const VERSION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x02,
]);
const PROPOSAL_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x03,
]);
const PUBLISHED_MEMORY_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x04,
]);
const PUBLISHED_VERSION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x05,
]);
const PROMOTION_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x01, 0x98, 0x2f, 0x80, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x06,
]);
const MAX_SEARCH_CANDIDATES: u16 = 100;

#[derive(Debug, Clone)]
pub struct MemoryToolContext {
    pub invocation: CapabilityInvocation,
    pub run_context: RunContext,
    pub actor: Actor,
    pub agent_id: AgentId,
    pub approval_interaction_id: Option<InteractionId>,
    pub user_authored_origin: Option<UserAuthoredRunOrigin>,
    pub evidence_record_id: LedgerRecordId,
    pub now: UtcTimestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemoryToolMutation {
    Remember(Box<RememberUserMemory>),
    Propose(Box<ExperienceCandidate>),
    Publish(Box<PromoteExperience>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemoryToolPreparation {
    ReadOnly(CapabilityResult),
    Commit {
        result: CapabilityResult,
        mutation: Box<MemoryToolMutation>,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitPreparedMemoryTool {
    pub run_context: RunContext,
    pub invocation_id: musteragent_identity::InvocationId,
    pub result: CapabilityResult,
    pub mutation: MemoryToolMutation,
    pub committed_at: UtcTimestamp,
}

impl CommitPreparedMemoryTool {
    pub fn validate(&self, attempt: &AttemptContext) -> Result<(), MemoryToolError> {
        self.result
            .validate()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        if attempt.run_id != self.run_context.run_id {
            return Err(MemoryToolError::IdentityMismatch);
        }
        match &self.mutation {
            MemoryToolMutation::Remember(command) => {
                command.validate()?;
                if command.record.tenant_id != self.run_context.tenant_id
                    || command.authorization.invocation_id != self.invocation_id
                    || !command.retains_current_version()
                        && !command.version.source_run_ids.is_empty()
                        && !command
                            .version
                            .source_run_ids
                            .contains(&self.run_context.run_id)
                {
                    return Err(MemoryToolError::IdentityMismatch);
                }
            }
            MemoryToolMutation::Propose(candidate) => {
                candidate
                    .validate()
                    .map_err(|_| MemoryToolError::InvalidInput)?;
                if candidate.tenant_id != self.run_context.tenant_id
                    || !candidate.source_run_ids.contains(&self.run_context.run_id)
                {
                    return Err(MemoryToolError::IdentityMismatch);
                }
            }
            MemoryToolMutation::Publish(command) => {
                command.validate()?;
                if command.candidate.tenant_id != self.run_context.tenant_id
                    || command
                        .tool_approval
                        .as_ref()
                        .is_none_or(|binding| binding.invocation_id() != self.invocation_id)
                {
                    return Err(MemoryToolError::IdentityMismatch);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryToolResult {
    Committed,
    Duplicate,
}

/// Stable application-level classification for adapter-specific durable
/// commit failures. Capability handlers must not turn conflicts or corrupt
/// state into blind dependency retries.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MemoryToolCommitFailure {
    InvalidCommand,
    Authorization,
    ContentIntegrity,
    StaleFence,
    Conflict,
    Corrupt,
    Unavailable,
}

pub trait ClassifyMemoryToolCommitError: Send {
    fn failure(&self) -> MemoryToolCommitFailure;
}

pub trait FencedMemoryToolCommitter: Send + Sync {
    type Error: ClassifyMemoryToolCommitError;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedMemoryTool,
    ) -> ApplicationFuture<'_, Result<MemoryToolResult, Self::Error>>;
}

pub struct BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    content: std::sync::Arc<dyn ContentStore>,
    memory: std::sync::Arc<M>,
    evolution: std::sync::Arc<E>,
    retriever: PublishedMemoryRetriever<M>,
    write_planner: std::sync::Arc<dyn crate::MemoryWritePlanner>,
    read_planner: Option<std::sync::Arc<dyn crate::MemoryReadPlanner>>,
}

impl<M, E> Clone for BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    fn clone(&self) -> Self {
        Self {
            content: std::sync::Arc::clone(&self.content),
            memory: std::sync::Arc::clone(&self.memory),
            evolution: std::sync::Arc::clone(&self.evolution),
            retriever: self.retriever.clone(),
            write_planner: self.write_planner.clone(),
            read_planner: self.read_planner.clone(),
        }
    }
}

impl<M, E> BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    #[must_use]
    pub fn new(
        content: std::sync::Arc<dyn ContentStore>,
        memory: std::sync::Arc<M>,
        evolution: std::sync::Arc<E>,
    ) -> Self {
        let retriever = PublishedMemoryRetriever::new(
            std::sync::Arc::clone(&memory),
            std::sync::Arc::clone(&content),
        );
        Self {
            content,
            memory,
            evolution,
            retriever,
            write_planner: std::sync::Arc::new(crate::write_planner::CompiledMemoryWritePlanner),
            read_planner: None,
        }
    }

    /// Bind one admission-frozen implementation. The journal refuses to replay
    /// a plan under a different release, including a return to the compiled path.
    #[must_use]
    pub fn with_ranker(mut self, ranker: std::sync::Arc<dyn crate::MemoryRanker>) -> Self {
        self.retriever = self.retriever.with_ranker(ranker);
        self
    }

    #[must_use]
    pub fn with_read_planner(
        mut self,
        planner: std::sync::Arc<dyn crate::MemoryReadPlanner>,
    ) -> Self {
        self.read_planner = Some(planner);
        self
    }

    async fn read_plan(
        &self,
        context: &MemoryToolContext,
        request: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<Option<crate::ReadPlan>, MemoryToolError> {
        let Some(planner) = &self.read_planner else {
            return Ok(None);
        };
        let prepared =
            crate::read_planner::complete_computation(execution, planner.prepare(request)).await?;
        let canonical = prepared
            .canonical_bytes()
            .map_err(|_| MemoryToolError::InvalidResult)?;
        // Defaults and query policy may evolve; explicit user targets, filters,
        // text and limits remain bound to the immutable original request.
        let verified = crate::ReadPreparation::decode_for_request(
            context.invocation.tool_key.as_str(),
            request,
            &canonical,
        )
        .map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(Some(verified.plan))
    }

    #[must_use]
    pub fn with_write_planner(
        mut self,
        planner: std::sync::Arc<dyn crate::MemoryWritePlanner>,
    ) -> Self {
        self.write_planner = planner;
        self
    }

    #[must_use]
    pub fn with_semantic_search(
        mut self,
        embeddings: std::sync::Arc<dyn crate::MemoryEmbeddingProvider>,
        vectors: std::sync::Arc<dyn crate::MemoryVectorIndex>,
    ) -> Self {
        self.retriever = self.retriever.with_semantic_search(embeddings, vectors);
        self
    }

    #[must_use]
    pub fn with_retrieval_telemetry(
        mut self,
        telemetry: std::sync::Arc<dyn crate::MemoryRetrievalTelemetry>,
    ) -> Self {
        self.retriever = self.retriever.with_telemetry(telemetry);
        self
    }

    pub async fn prepare(
        &self,
        context: MemoryToolContext,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        self.prepare_with_execution(context, None).await
    }

    pub async fn prepare_with_execution(
        &self,
        context: MemoryToolContext,
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        validate_context(&context)?;
        if context.invocation.tool_key.logical_key() == "experience.propose"
            && context.invocation.request_ref.byte_length
                > musteragent_memory_business::preparation::MAX_PREPARATION_BYTES as u64
        {
            return Err(MemoryToolError::InvalidInput);
        }
        let bytes = crate::retrieval::read_stage(
            execution,
            self.content.read(
                context.invocation.tenant_id,
                &context.invocation.request_ref,
            ),
        )
        .await
        .map_err(map_retrieval_error)?
        .map_err(|_| MemoryToolError::Content)?;
        musteragent_runtime_content::verify_content(&context.invocation.request_ref, &bytes)
            .map_err(|_| MemoryToolError::InvalidResult)?;
        match context.invocation.tool_key.logical_key() {
            "user_memory.remember" => self.prepare_remember(context, &bytes, execution).await,
            "experience.propose" => {
                preparation::prepare_proposal(
                    self.content.as_ref(),
                    self.write_planner.as_ref(),
                    context,
                    &bytes,
                    execution,
                )
                .await
            }
            "experience.search" => self.prepare_search(context, &bytes, execution).await,
            "experience.inspect" => self.prepare_inspect(context, &bytes, execution).await,
            "memory.search" => {
                self.prepare_memory_search(
                    context,
                    &bytes,
                    MemoryEmbeddingDeadline::retrieval(),
                    execution,
                )
                .await
            }
            "memory.list" => self.prepare_memory_list(context, &bytes, execution).await,
            "memory.get" => self.prepare_memory_get(context, &bytes, execution).await,
            "experience.publish" => self.prepare_publish(context, &bytes, execution).await,
            _ => Err(MemoryToolError::UnsupportedTool),
        }
    }

    /// Returns the exact Memory business statement for the Tool executor to
    /// bind to the immutable request in its original Approval Interaction.
    pub async fn approval_prompt(
        &self,
        invocation: &CapabilityInvocation,
        agent_id: AgentId,
    ) -> Result<Option<(MediaType, Vec<u8>)>, MemoryToolError> {
        if !matches!(
            invocation.tool_key.logical_key(),
            "experience.publish" | "user_memory.remember"
        ) {
            return Ok(None);
        }
        let bytes = self
            .content
            .read(invocation.tenant_id, &invocation.request_ref)
            .await
            .map_err(|_| MemoryToolError::Content)?;
        if invocation.tool_key.logical_key() == "user_memory.remember" {
            return approval::remember_prompt(invocation, &bytes).map(Some);
        }
        let input: PublishInput =
            serde_json::from_slice(&bytes).map_err(|_| MemoryToolError::InvalidInput)?;
        let review = self
            .evolution
            .load_verified_review(input.proposal_id()?)
            .await
            .map_err(|_| MemoryToolError::Store)?
            .ok_or(MemoryToolError::PublicationNotReady)?;
        crate::tool_approval::authorize_scope(&review.candidate, invocation, agent_id)?;
        validate_claim_media_type(&review.candidate.claim_ref)?;
        let head = self.publication_head(&review.candidate).await?;
        let expected_current_version_id =
            head.as_ref().map(|value| value.version.memory_version_id);
        let identity = crate::ToolEvolutionApprovalIdentity::from_invocation(invocation, agent_id)?;
        let prompt = crate::prepare_tool_evolution_approval_prompt(
            self.content.as_ref(),
            &identity,
            &review,
            expected_current_version_id,
        )
        .await?;
        Ok(Some((prompt.media_type, prompt.bytes)))
    }

    async fn prepare_memory_search(
        &self,
        context: MemoryToolContext,
        bytes: &[u8],
        embedding_deadline: MemoryEmbeddingDeadline,
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let (input, strategy) = match self.read_plan(&context, bytes, execution).await? {
            Some(crate::ReadPlan::MemorySearch {
                query,
                limit,
                strategy,
            }) => (MemorySearchInput { query, limit }, Some(strategy)),
            Some(_) => return Err(MemoryToolError::InvalidResult),
            None => {
                let input: MemorySearchInput =
                    serde_json::from_slice(bytes).map_err(|_| MemoryToolError::InvalidInput)?;
                input.validate()?;
                (input, None)
            }
        };
        let request = DynamicMemorySearchRequest {
            visibility: memory_visibility(&context, input.limit)?,
            query: input.query,
            embedding_deadline,
        };
        let result = match strategy {
            Some(strategy) => {
                self.retriever
                    .search_planned_with_execution(request, strategy, execution)
                    .await
            }
            None => {
                self.retriever
                    .search_dynamic_with_execution(request, execution)
                    .await
            }
        }
        .map_err(map_retrieval_error)?;
        let output = serde_json::to_vec(&result).map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(MemoryToolPreparation::ReadOnly(
            self.result(&context.invocation, output).await?,
        ))
    }

    async fn prepare_memory_list(
        &self,
        context: MemoryToolContext,
        bytes: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let input = match self.read_plan(&context, bytes, execution).await? {
            Some(crate::ReadPlan::MemoryList { limit }) => MemoryListInput { limit },
            Some(_) => return Err(MemoryToolError::InvalidResult),
            None => {
                let input: MemoryListInput =
                    serde_json::from_slice(bytes).map_err(|_| MemoryToolError::InvalidInput)?;
                input.validate()?;
                input
            }
        };
        let result = self
            .retriever
            .list_dynamic_with_execution(memory_visibility(&context, input.limit)?, execution)
            .await
            .map_err(map_retrieval_error)?;
        let output = serde_json::to_vec(&result).map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(MemoryToolPreparation::ReadOnly(
            self.result(&context.invocation, output).await?,
        ))
    }

    async fn prepare_memory_get(
        &self,
        context: MemoryToolContext,
        bytes: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let input = match self.read_plan(&context, bytes, execution).await? {
            Some(crate::ReadPlan::MemoryGet { memory_id }) => MemoryGetInput { memory_id },
            Some(_) => return Err(MemoryToolError::InvalidResult),
            None => serde_json::from_slice(bytes).map_err(|_| MemoryToolError::InvalidInput)?,
        };
        let memory_id =
            MemoryId::from_uuid(input.memory_id).map_err(|_| MemoryToolError::InvalidInput)?;
        let result = self
            .retriever
            .get_dynamic_with_execution(memory_visibility(&context, 1)?, memory_id, execution)
            .await
            .map_err(map_retrieval_error)?;
        let output = serde_json::to_vec(&result).map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(MemoryToolPreparation::ReadOnly(
            self.result(&context.invocation, output).await?,
        ))
    }

    async fn result(
        &self,
        invocation: &CapabilityInvocation,
        bytes: Vec<u8>,
    ) -> Result<CapabilityResult, MemoryToolError> {
        let result_ref = self.put_json(invocation.tenant_id, bytes).await?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }

    async fn put_json(
        &self,
        tenant_id: TenantId,
        bytes: Vec<u8>,
    ) -> Result<musteragent_content_ref::ContentRef, MemoryToolError> {
        self.content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new("application/json")
                    .map_err(|_| MemoryToolError::InvalidResult)?,
                bytes,
            })
            .await
            .map_err(|_| MemoryToolError::Content)
    }
}

fn validate_claim_media_type(
    reference: &musteragent_content_ref::ContentRef,
) -> Result<(), MemoryToolError> {
    if crate::is_experience_claim_media_type(reference.media_type.as_str()) {
        Ok(())
    } else {
        Err(MemoryToolError::InvalidResult)
    }
}

fn validate_context(context: &MemoryToolContext) -> Result<(), MemoryToolError> {
    if context.actor.principal_kind != PrincipalKind::User
        || context.actor.tenant_id != context.invocation.tenant_id
        || context.run_context.tenant_id != context.invocation.tenant_id
        || context.run_context.conversation_id != context.invocation.conversation_id
        || context.run_context.run_id != context.invocation.run_id
    {
        Err(MemoryToolError::IdentityMismatch)
    } else {
        Ok(())
    }
}

#[derive(Serialize)]
struct RememberOutput {
    experience_id: String,
    version_id: String,
    revision: u64,
    status: &'static str,
    changed: bool,
}

#[derive(Serialize)]
struct ProposalOutput {
    candidate_id: String,
    status: &'static str,
    published: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishInput {
    proposal_id: Uuid,
}

impl PublishInput {
    fn proposal_id(&self) -> Result<EvolutionProposalId, MemoryToolError> {
        EvolutionProposalId::from_uuid(self.proposal_id).map_err(|_| MemoryToolError::InvalidInput)
    }
}

#[derive(Serialize)]
struct PublishOutput {
    proposal_id: String,
    memory_id: String,
    memory_version_id: String,
    status: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchInput {
    query: String,
    #[serde(default)]
    kinds: Vec<String>,
    #[serde(default = "default_search_limit")]
    limit: u16,
}

impl SearchInput {
    fn validate(&self) -> Result<(), MemoryToolError> {
        let unique = self.kinds.iter().collect::<BTreeSet<_>>();
        if self.query.trim().is_empty()
            || self.query.len() > 1_000
            || self.kinds.len() > 5
            || unique.len() != self.kinds.len()
            || self
                .kinds
                .iter()
                .any(|value| experience_kind(value).is_err())
            || !(1..=20).contains(&self.limit)
        {
            Err(MemoryToolError::InvalidInput)
        } else {
            Ok(())
        }
    }
}

#[derive(Serialize)]
struct SearchOutput {
    experiences: Vec<SearchExperience>,
}

#[derive(Serialize)]
struct SearchExperience {
    experience_id: String,
    version_id: String,
    kind: &'static str,
    content: Value,
    conditions: Map<String, Value>,
    confidence: f64,
    source: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemorySearchInput {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: u16,
}

impl MemorySearchInput {
    fn validate(&self) -> Result<(), MemoryToolError> {
        if self.query.trim().is_empty()
            || self.query.len() > 1_000
            || !(1..=20).contains(&self.limit)
        {
            Err(MemoryToolError::InvalidInput)
        } else {
            Ok(())
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryListInput {
    #[serde(default = "default_search_limit")]
    limit: u16,
}

impl MemoryListInput {
    fn validate(&self) -> Result<(), MemoryToolError> {
        if (1..=20).contains(&self.limit) {
            Ok(())
        } else {
            Err(MemoryToolError::InvalidInput)
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryGetInput {
    memory_id: Uuid,
}

fn memory_visibility(
    context: &MemoryToolContext,
    limit: u16,
) -> Result<MemorySearch, MemoryToolError> {
    Ok(MemorySearch {
        tenant_id: context.actor.tenant_id,
        principal_id: context.actor.principal_id,
        workspace_ids: context.invocation.workspace_id.into_iter().collect(),
        agent_ids: BTreeSet::from([context.agent_id]),
        kinds: BTreeSet::from([
            MemoryKind::Preference,
            MemoryKind::Semantic,
            MemoryKind::Procedure,
            MemoryKind::FailureLesson,
        ]),
        limit: NonZeroU16::new(limit).ok_or(MemoryToolError::InvalidInput)?,
        now: context.now,
    })
}

fn map_retrieval_error(error: crate::MemoryRetrievalError) -> MemoryToolError {
    match error {
        crate::MemoryRetrievalError::InvalidQueryContent
        | crate::MemoryRetrievalError::Application(_) => MemoryToolError::InvalidInput,
        crate::MemoryRetrievalError::ContentIntegrity
        | crate::MemoryRetrievalError::Threat(_)
        | crate::MemoryRetrievalError::InvalidSearchDocument
        | crate::MemoryRetrievalError::InvalidBackfillClaim
        | crate::MemoryRetrievalError::Rendering => MemoryToolError::InvalidResult,
        crate::MemoryRetrievalError::ContentStore => MemoryToolError::Content,
        crate::MemoryRetrievalError::Store => MemoryToolError::Store,
        crate::MemoryRetrievalError::Program(error) => error,
        crate::MemoryRetrievalError::Execution(code) => MemoryToolError::Execution(code),
    }
}

const fn default_search_limit() -> u16 {
    8
}

fn memory_id(
    tenant_id: TenantId,
    principal_id: musteragent_identity::PrincipalId,
    fact_key: &str,
) -> Result<MemoryId, MemoryToolError> {
    MemoryId::from_uuid(Uuid::new_v5(
        &MEMORY_ID_NAMESPACE,
        format!("{tenant_id}:{principal_id}:{fact_key}").as_bytes(),
    ))
    .map_err(|_| MemoryToolError::InvalidResult)
}

fn version_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<MemoryVersionId, MemoryToolError> {
    MemoryVersionId::from_uuid(Uuid::new_v5(
        &VERSION_ID_NAMESPACE,
        invocation_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| MemoryToolError::InvalidResult)
}

fn proposal_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<EvolutionProposalId, MemoryToolError> {
    EvolutionProposalId::from_uuid(Uuid::new_v5(
        &PROPOSAL_ID_NAMESPACE,
        invocation_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| MemoryToolError::InvalidResult)
}

fn published_memory_id(proposal_id: EvolutionProposalId) -> Result<MemoryId, MemoryToolError> {
    MemoryId::from_uuid(Uuid::new_v5(
        &PUBLISHED_MEMORY_ID_NAMESPACE,
        proposal_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| MemoryToolError::InvalidResult)
}

fn published_version_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<MemoryVersionId, MemoryToolError> {
    MemoryVersionId::from_uuid(Uuid::new_v5(
        &PUBLISHED_VERSION_ID_NAMESPACE,
        invocation_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| MemoryToolError::InvalidResult)
}

fn promotion_id(
    invocation_id: musteragent_identity::InvocationId,
) -> Result<EvolutionPromotionId, MemoryToolError> {
    EvolutionPromotionId::from_uuid(Uuid::new_v5(
        &PROMOTION_ID_NAMESPACE,
        invocation_id.into_uuid().as_bytes(),
    ))
    .map_err(|_| MemoryToolError::InvalidResult)
}

const fn evolution_memory_scope(scope: EvolutionScope) -> MemoryScope {
    match scope {
        EvolutionScope::User { principal_id } => MemoryScope::User { principal_id },
        EvolutionScope::Agent { agent_id } => MemoryScope::Agent { agent_id },
    }
}

fn evolution_memory_kind(kind: ExperienceKind) -> Result<MemoryKind, MemoryToolError> {
    match kind {
        ExperienceKind::Semantic => Ok(MemoryKind::Semantic),
        ExperienceKind::Procedure => Ok(MemoryKind::Procedure),
        ExperienceKind::Failure => Ok(MemoryKind::FailureLesson),
        ExperienceKind::Preference => Ok(MemoryKind::Preference),
        ExperienceKind::PolicyProposal
        | ExperienceKind::Skill
        | ExperienceKind::Instinct
        | ExperienceKind::PersonaPatch => Err(MemoryToolError::InvalidInput),
    }
}

fn memory_kind(key: &str) -> MemoryKind {
    if key.starts_with("preference.") {
        MemoryKind::Preference
    } else {
        MemoryKind::Semantic
    }
}

fn confidence_bps(value: f64) -> Result<ConfidenceBps, MemoryToolError> {
    let scaled = (value * 10_000.0).round();
    if !scaled.is_finite() || !(0.0..=10_000.0).contains(&scaled) {
        return Err(MemoryToolError::InvalidInput);
    }
    ConfidenceBps::new(scaled as u16).map_err(|_| MemoryToolError::InvalidInput)
}

fn experience_kind(value: &str) -> Result<ExperienceKind, MemoryToolError> {
    match value {
        "semantic" => Ok(ExperienceKind::Semantic),
        "procedure" => Ok(ExperienceKind::Procedure),
        "failure" => Ok(ExperienceKind::Failure),
        "preference" => Ok(ExperienceKind::Preference),
        "policy_proposal" => Ok(ExperienceKind::PolicyProposal),
        "skill" => Ok(ExperienceKind::Skill),
        "instinct" => Ok(ExperienceKind::Instinct),
        "persona_patch" => Ok(ExperienceKind::PersonaPatch),
        _ => Err(MemoryToolError::InvalidInput),
    }
}

const fn experience_kind_name(value: ExperienceKind) -> &'static str {
    match value {
        ExperienceKind::Semantic => "semantic",
        ExperienceKind::Procedure => "procedure",
        ExperienceKind::Failure => "failure",
        ExperienceKind::Preference => "preference",
        ExperienceKind::PolicyProposal => "policy_proposal",
        ExperienceKind::Skill => "skill",
        ExperienceKind::Instinct => "instinct",
        ExperienceKind::PersonaPatch => "persona_patch",
    }
}

fn query_terms(query: &str) -> BTreeSet<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn relevance(terms: &BTreeSet<String>, document: &ExperienceClaimDocument) -> usize {
    let searchable = document.searchable_text().to_lowercase();
    terms
        .iter()
        .filter(|term| searchable.contains(*term))
        .count()
}

#[derive(Debug, Error)]
pub enum MemoryToolError {
    #[error("Memory execution interrupted: {0:?}")]
    Execution(musteragent_runtime_contract::ErrorCode),
    #[error("Memory Tool input is invalid")]
    InvalidInput,
    #[error("Memory Tool result is invalid")]
    InvalidResult,
    #[error("Memory Tool identity differs from the active invocation")]
    IdentityMismatch,
    #[error("Memory version number overflow")]
    VersionOverflow,
    #[error("Memory Tool is unsupported")]
    UnsupportedTool,
    #[error("Memory Content Store is unavailable")]
    Content,
    #[error("Memory business program is unavailable")]
    ProgramUnavailable,
    #[error("Memory persistence is unavailable")]
    Store,
    #[error("Memory is inactive and requires an explicit lifecycle restore")]
    InactiveMemory,
    #[error("Experience proposal is not currently eligible for publication")]
    PublicationNotReady,
    #[error(transparent)]
    Application(#[from] MemoryApplicationError),
}
