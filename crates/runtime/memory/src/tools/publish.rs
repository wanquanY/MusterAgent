//! Publication restores the exact verified review, original approved interaction
//! and expected head. Replays cannot select a more recent evaluation or approval.
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationFacts {
    review: prepared_evolution::Review,
    head: Option<prepared_facts::Head>,
    approval: prepared_evolution::Approval,
}

impl<M, E> BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    pub(super) async fn prepare_publish(
        &self,
        mut context: MemoryToolContext,
        bytes: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let input: PublishInput =
            serde_json::from_slice(bytes).map_err(|_| MemoryToolError::InvalidInput)?;
        let proposal_id = input.proposal_id()?;
        let interaction_id = context
            .approval_interaction_id
            .ok_or(MemoryToolError::IdentityMismatch)?;
        let identity = crate::ToolEvolutionApprovalIdentity::from_invocation(
            &context.invocation,
            context.agent_id,
        )?;
        let frozen = write_journal::freeze(
            self.content.as_ref(),
            self.write_planner.as_ref(),
            &context,
            bytes,
            execution,
            async {
                let review = crate::retrieval::read_stage(
                    execution,
                    self.evolution.load_verified_review(proposal_id),
                )
                .await
                .map_err(map_retrieval_error)?
                .map_err(|_| MemoryToolError::Store)?
                .ok_or(MemoryToolError::PublicationNotReady)?;
                crate::tool_approval::authorize_scope(
                    &review.candidate,
                    &context.invocation,
                    context.agent_id,
                )?;
                validate_claim_media_type(&review.candidate.claim_ref)?;
                let head = crate::retrieval::read_stage(
                    execution,
                    self.publication_head(&review.candidate),
                )
                .await
                .map_err(map_retrieval_error)??;
                let interaction = crate::retrieval::read_stage(
                    execution,
                    self.evolution.load_approved_interaction(interaction_id),
                )
                .await
                .map_err(map_retrieval_error)?
                .map_err(|_| MemoryToolError::Store)?
                .ok_or(MemoryToolError::IdentityMismatch)?;
                crate::retrieval::read_stage(
                    execution,
                    crate::verify_tool_evolution_approval(
                        self.content.as_ref(),
                        &identity,
                        &review,
                        head.as_ref().map(|head| head.version.memory_version_id),
                        &interaction,
                    ),
                )
                .await
                .map_err(map_retrieval_error)??;
                Ok(PublicationFacts {
                    review: prepared_evolution::Review::capture(&review)?,
                    head: head.as_ref().map(prepared_facts::Head::from),
                    approval: (&interaction).into(),
                })
            },
        )
        .await?;
        let musteragent_memory_business::preparation::WritePlan::Publish(input) =
            frozen.business.plan
        else {
            return Err(MemoryToolError::InvalidResult);
        };
        if input.proposal_id != proposal_id.into_uuid() {
            return Err(MemoryToolError::IdentityMismatch);
        }
        context.now = frozen.at;
        let review = frozen.facts.review.restore()?;
        let head = frozen
            .facts
            .head
            .map(prepared_facts::Head::restore)
            .transpose()?;
        let interaction = frozen.facts.approval.restore()?;
        if review.candidate.proposal_id != proposal_id
            || interaction.interaction_id != interaction_id
        {
            return Err(MemoryToolError::IdentityMismatch);
        }
        let (approval, binding) = crate::retrieval::read_stage(
            execution,
            crate::verify_tool_evolution_approval(
                self.content.as_ref(),
                &identity,
                &review,
                head.as_ref().map(|head| head.version.memory_version_id),
                &interaction,
            ),
        )
        .await
        .map_err(map_retrieval_error)??;
        let memory_id = head.as_ref().map_or_else(
            || published_memory_id(proposal_id),
            |value| Ok(value.record.memory_id),
        )?;
        let memory_version_id = published_version_id(context.invocation.invocation_id)?;
        let mut command = crate::retrieval::read_stage(
            execution,
            prepare_evolution_publication(
                self.content.as_ref(),
                PrepareEvolutionPublication {
                    candidate: review.candidate,
                    evaluation: review.evaluation,
                    approval,
                    current_head: head,
                    new_memory_id: memory_id,
                    memory_version_id,
                    promotion_id: promotion_id(context.invocation.invocation_id)?,
                    promoted_by: context.actor.principal_id,
                    promoted_at: context.now,
                },
            ),
        )
        .await
        .map_err(map_retrieval_error)?
        .map_err(|error| match error {
            crate::EvolutionPublicationPreparationError::StaleApproval
            | crate::EvolutionPublicationPreparationError::StaleHead => {
                MemoryToolError::IdentityMismatch
            }
            crate::EvolutionPublicationPreparationError::ContentStore => MemoryToolError::Content,
            crate::EvolutionPublicationPreparationError::ContentIntegrity => {
                MemoryToolError::Content
            }
            crate::EvolutionPublicationPreparationError::InvalidCandidate => {
                MemoryToolError::InvalidInput
            }
            crate::EvolutionPublicationPreparationError::VersionOverflow => {
                MemoryToolError::VersionOverflow
            }
        })?;
        command.tool_approval = Some(binding);
        command.validate()?;
        let output = serde_json::to_vec(&PublishOutput {
            proposal_id: proposal_id.to_string(),
            memory_id: memory_id.to_string(),
            memory_version_id: memory_version_id.to_string(),
            status: "published",
        })
        .map_err(|_| MemoryToolError::InvalidResult)?;
        preparation::ensure_running(execution)?;
        let result_ref = preparation::put_exact(
            self.content.as_ref(),
            context.actor.tenant_id,
            preparation::identity(context.invocation.invocation_id, "publish-result"),
            "application/json",
            output,
        )
        .await
        .map_err(preparation::content_error)?;
        preparation::ensure_running(execution)?;
        let result = CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: context.invocation.result_schema.clone(),
            provider_request_ref: None,
        };
        Ok(MemoryToolPreparation::Commit {
            result,
            mutation: Box::new(MemoryToolMutation::Publish(Box::new(command))),
        })
    }

    pub(super) async fn publication_head(
        &self,
        candidate: &ExperienceCandidate,
    ) -> Result<Option<MemorySearchHit>, MemoryToolError> {
        if candidate.kind == ExperienceKind::PolicyProposal {
            // Policy evolution belongs to the Definition/Studio lifecycle and
            // must never be smuggled into the Memory publication path.
            return Err(MemoryToolError::InvalidInput);
        }
        let Some(memory_id) = candidate.target_memory_id else {
            return Ok(None);
        };
        let head = self
            .memory
            .load_active_head(candidate.tenant_id, memory_id)
            .await
            .map_err(|_| MemoryToolError::Store)?
            .ok_or(MemoryToolError::InactiveMemory)?;
        if head.record.tenant_id != candidate.tenant_id
            || head.record.memory_id != memory_id
            || head.record.scope != evolution_memory_scope(candidate.target_scope)
            || head.record.kind != evolution_memory_kind(candidate.kind)?
            || head.record.lifecycle != MemoryLifecycle::Active
            || head.record.current_version_id != head.version.memory_version_id
        {
            return Err(MemoryToolError::IdentityMismatch);
        }
        Ok(Some(head))
    }
}
