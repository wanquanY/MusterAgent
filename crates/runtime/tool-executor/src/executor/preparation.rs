//! Authorization and validation precede physical dispatch. Preparing a Tool
//! never invokes its Capability; it produces an explicit authorized action or
//! the original durable observation to resolve under the selected execution mode.
use super::*;

pub(super) enum PreparedTool {
    Outcome(ToolExecutionOutcome),
    Authorized {
        material: Box<ToolExecutionMaterial>,
        durable: Box<DurableToolInvocation>,
    },
    Recorded {
        durable: Box<DurableToolInvocation>,
        outcome: ToolTerminalOutcome,
    },
}
impl PreparedTool {
    pub(super) fn authorized(
        material: &ToolExecutionMaterial,
        durable: DurableToolInvocation,
        authorization: InvocationAuthorization,
    ) -> Self {
        let mut material = material.clone();
        material.authorization = authorization;
        Self::Authorized {
            material: Box::new(material),
            durable: Box::new(durable),
        }
    }
}
impl DurableToolExecutor {
    pub(super) async fn prepare_loaded(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        mut durable: DurableToolInvocation,
    ) -> Result<PreparedTool, LoopPortError> {
        if let Some(outcome) = durable.recorded_outcome.clone() {
            return Ok(PreparedTool::Recorded {
                durable: Box::new(durable),
                outcome,
            });
        }

        if let Some(failure) = call.planning_failure {
            let mut outcome = crate::schema_validation::planning_failure_outcome(failure)?;
            if let ToolTerminalOutcome::Failed {
                diagnostic_result, ..
            } = &mut outcome
            {
                *diagnostic_result = Some(
                    crate::diagnostic_result::persist(
                        self.content.as_ref(),
                        material.run_context.tenant_id,
                        durable.invocation.invocation_id,
                        "tool.argument-protocol-diagnostics@1",
                        include_bytes!("../schema_validation/protocol-diagnostic.schema.json"),
                        &failure.diagnostic_json(call)?,
                    )
                    .await?,
                );
            }
            durable = self
                .journal
                .complete(
                    attempt,
                    durable.invocation.invocation_id,
                    outcome.clone(),
                    self.clock.now(),
                )
                .await
                .map_err(port_from_tool)?;
            return self
                .message_for_outcome(call, material, &durable, &outcome)
                .await
                .map(PreparedTool::Outcome);
        }

        let arguments = execution_arguments(&call.arguments, material);
        // Runtime-issued progress narration is the one call the model never
        // wrote. Its body is checked by the strict publication decoder, which
        // accepts only a short `progress` message, instead of the model schema.
        let runtime_progress =
            musteragent_execution_context::is_runtime_progress_call(call.call_id.as_str())
                && material.tool.manifest.key.as_str()
                    == musteragent_execution_context::DIALOGUE_PUBLISH_TOOL;
        if runtime_progress {
            let valid = serde_json::to_vec(&arguments).ok().and_then(|bytes| {
                musteragent_execution_context::decode_native_publication(
                    musteragent_execution_context::DIALOGUE_PUBLISH_TOOL,
                    &bytes,
                )
                .ok()
            });
            if !valid.is_some_and(|publication| {
                publication.purpose
                    == musteragent_execution_context::DialoguePublicationPurpose::Progress
            }) {
                return Err(port(ErrorCode::InternalInvariantViolated, false));
            }
        } else if let Err(validation) = self
            .schemas
            .validate_async(
                material.tool.manifest.input_schema.schema_hash,
                &material.tool.input_schema_json,
                &arguments,
            )
            .await
        {
            // Model-generated Tool arguments are untrusted protocol input, not
            // a Runtime port failure. Persist the rejected invocation and feed
            // a normal Tool result back into the same Run so the model can
            // repair calls after any versioned Tool/Skill schema release.
            let mut outcome = invalid_arguments_outcome(&validation)?;
            if let ToolTerminalOutcome::Failed {
                diagnostic_result, ..
            } = &mut outcome
            {
                *diagnostic_result = Some(
                    crate::diagnostic_result::persist(
                        self.content.as_ref(),
                        material.run_context.tenant_id,
                        durable.invocation.invocation_id,
                        "tool.argument-diagnostics@1",
                        include_bytes!("../schema_validation/diagnostic.schema.json"),
                        &validation.diagnostic_json()?,
                    )
                    .await?,
                );
            }
            durable = self
                .journal
                .complete(
                    attempt,
                    durable.invocation.invocation_id,
                    outcome.clone(),
                    self.clock.now(),
                )
                .await
                .map_err(port_from_tool)?;
            return self
                .message_for_outcome(call, material, &durable, &outcome)
                .await
                .map(PreparedTool::Outcome);
        }

        let mut authorization = material.authorization.clone();
        authorization.approval_satisfied = matches!(
            durable.interaction_resolution,
            Some(InteractionResolution::Approval(
                musteragent_approval::ApprovalDecision::Approve
                    | musteragent_approval::ApprovalDecision::ApproveSession
            ))
        );
        let policy_input = InvocationPolicyInput {
            workspace_id: workspace_id(&material.run_context.execution_scope),
            tool: &material.tool.manifest,
            arguments: &arguments,
            policy_basis: material.policy_basis.as_ref(),
            authorized_content: &material.authorized_content,
        };
        let policy_disposition = match self
            .runtime_invocation_policy
            .evaluate_checked(&policy_input)
            .and_then(|runtime| {
                self.invocation_policy
                    .evaluate_checked(&policy_input)
                    .map(|host| combine_dispositions(runtime, host))
            }) {
            Ok(disposition) => disposition,
            Err(failure) => {
                let reason = serde_json::to_string(&failure)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                let outcome = failed_outcome(ErrorCode::PolicyDenied, &reason)?;
                durable = self
                    .journal
                    .complete(
                        attempt,
                        durable.invocation.invocation_id,
                        outcome.clone(),
                        self.clock.now(),
                    )
                    .await
                    .map_err(port_from_tool)?;
                return self
                    .message_for_outcome(call, material, &durable, &outcome)
                    .await
                    .map(PreparedTool::Outcome);
            }
        };
        let consequence_fingerprint = consequence_fingerprint(&policy_input);
        let session_approval_fingerprint = session_approval_fingerprint(&policy_input);
        let session_reusable = session_authorization_is_reusable(
            material.capability_grant.policy_decision,
            material.tool.manifest.approval,
            policy_disposition,
        );
        let receipt_observed_at = self.clock.now();
        let receipt_query = authorization_receipt_query(
            material,
            session_approval_fingerprint,
            receipt_observed_at,
        );
        if matches!(
            durable.interaction_resolution,
            Some(InteractionResolution::Approval(
                musteragent_approval::ApprovalDecision::ApproveSession
            ))
        ) {
            if !session_reusable {
                return Err(port(ErrorCode::RuntimeSnapshotInvalid, false));
            }
            let source_interaction_id = durable
                .invocation
                .interaction_id
                .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
            let receipt =
                authorization_receipt(&receipt_query, source_interaction_id, receipt_observed_at)?;
            let persisted = self
                .journal
                .issue_authorization_receipt(attempt, receipt.clone())
                .await
                .map_err(port_from_tool)?;
            if persisted.source_interaction_id != source_interaction_id
                || !receipt_matches_query(&persisted, &receipt_query)
            {
                return Err(port(ErrorCode::IdempotencyConflict, false));
            }
        }
        if session_reusable
            && durable.interaction_resolution.is_none()
            && self
                .journal
                .find_active_authorization_receipt(receipt_query.clone())
                .await
                .map_err(port_from_tool)?
                .is_some_and(|receipt| receipt_matches_query(&receipt, &receipt_query))
        {
            authorization.approval_satisfied = true;
        }
        let policy_reason = match policy_disposition {
            InvocationPolicyDisposition::Inherit => None,
            InvocationPolicyDisposition::Allow => {
                // A persisted Interaction is itself a durable authorization
                // boundary. A newer evaluator may tighten policy immediately,
                // but it must never reinterpret an already-present Approval as
                // unnecessary and dispatch while the user still sees a pending
                // decision. Approved recovery continues through
                // `approval_satisfied`; pending/denied/expired recovery remains
                // owned by `require_approval` below.
                authorization.policy_requires_approval =
                    durable.invocation.interaction_id.is_some()
                        && !readiness::is_readiness_interaction(&durable);
                None
            }
            InvocationPolicyDisposition::RequirePolicyApproval(reason) => {
                // This is an argument-aware refinement of `ByPolicy`, not a
                // mandatory floor. The immutable permission mode already set
                // the conservative default in the material source.
                authorization.policy_requires_approval.then_some(reason)
            }
            InvocationPolicyDisposition::RequireApproval(reason) => {
                authorization.policy_requires_approval = true;
                Some(reason)
            }
            InvocationPolicyDisposition::Deny(reason) => {
                let outcome = failed_outcome(ErrorCode::PolicyDenied, reason)?;
                durable = self
                    .journal
                    .complete(
                        attempt,
                        durable.invocation.invocation_id,
                        outcome.clone(),
                        self.clock.now(),
                    )
                    .await
                    .map_err(port_from_tool)?;
                return self
                    .message_for_outcome(call, material, &durable, &outcome)
                    .await
                    .map(PreparedTool::Outcome);
            }
        };
        if let Some(reason) = unavailable_context_reason(
            &material.tool.manifest.required_contexts,
            &authorization.available_contexts,
        ) {
            let outcome = failed_outcome(ErrorCode::CapabilityUnavailable, reason)?;
            durable = self
                .journal
                .complete(
                    attempt,
                    durable.invocation.invocation_id,
                    outcome.clone(),
                    self.clock.now(),
                )
                .await
                .map_err(port_from_tool)?;
            return self
                .message_for_outcome(call, material, &durable, &outcome)
                .await
                .map(PreparedTool::Outcome);
        }
        match authorize_invocation(
            &material.tool.manifest,
            &material.capability_grant,
            &material.tool.execution_target,
            &authorization,
        ) {
            Decision::Deny => {
                let outcome = failed_outcome(
                    ErrorCode::PolicyDenied,
                    "Tool authorization does not match the immutable Runtime Snapshot",
                )?;
                durable = self
                    .journal
                    .complete(
                        attempt,
                        durable.invocation.invocation_id,
                        outcome.clone(),
                        self.clock.now(),
                    )
                    .await
                    .map_err(port_from_tool)?;
                self.message_for_outcome(call, material, &durable, &outcome)
                    .await
                    .map(PreparedTool::Outcome)
            }
            Decision::RequireApproval => {
                let auto_review = review_approval(ApprovalAutoReviewInput {
                    permission_mode: material.permission_mode,
                    effect: material.tool.manifest.effect,
                    approval_requirement: material.tool.manifest.approval,
                    capability_decision: material.capability_grant.policy_decision,
                    invocation_disposition: policy_disposition,
                });
                let readiness_card = readiness::is_readiness_interaction(&durable);
                if (durable.interaction_resolution.is_none() || readiness_card)
                    && auto_review == ApprovalAutoReviewDecision::Approve
                {
                    // `approval_satisfied` means the immutable policy boundary
                    // has been satisfied. In approve-for-me mode the isolated,
                    // versioned reviewer owns that decision; the executing
                    // Agent never reviews its own request.
                    authorization.approval_satisfied = true;
                    if let Some(gated) = self
                        .application_readiness(attempt, call, material, &durable)
                        .await?
                    {
                        return Ok(gated);
                    }
                    return Ok(PreparedTool::authorized(material, durable, authorization));
                }
                if readiness_card {
                    // One invocation owns one Interaction. After a readiness
                    // card, a call that still needs a human approval is
                    // finished so the model can call it again for approval.
                    let outcome = failed_outcome(
                        ErrorCode::PolicyDenied,
                        "The application is ready now, but this call also needs the \
                         user's approval. Call the same tool again to request it.",
                    )?;
                    durable = self
                        .journal
                        .complete(
                            attempt,
                            durable.invocation.invocation_id,
                            outcome.clone(),
                            self.clock.now(),
                        )
                        .await
                        .map_err(port_from_tool)?;
                    return self
                        .message_for_outcome(call, material, &durable, &outcome)
                        .await
                        .map(PreparedTool::Outcome);
                }
                let policy_reason = match auto_review {
                    ApprovalAutoReviewDecision::Escalate(reason) => Some(reason),
                    _ => policy_reason,
                };
                self.require_approval(
                    attempt,
                    call,
                    material,
                    durable,
                    authorization,
                    ApprovalRequestPolicy {
                        policy_reason,
                        consequence_fingerprint,
                        session_reusable,
                    },
                )
                .await
            }
            Decision::Allow => {
                if let Some(gated) = self
                    .application_readiness(attempt, call, material, &durable)
                    .await?
                {
                    return Ok(gated);
                }
                Ok(PreparedTool::authorized(material, durable, authorization))
            }
        }
    }
}

pub(super) fn invocation_timing(
    policy: &musteragent_tool::ToolExecutionPolicy,
    _now: UtcTimestamp,
) -> Result<InvocationExecutionTiming, LoopPortError> {
    match policy {
        musteragent_tool::ToolExecutionPolicy::ActivityV1(policy) => Ok(
            InvocationExecutionTiming::ActivityV1(ActivityExecutionTiming {
                policy: policy.reference().clone(),
                business_deadline: None,
            }),
        ),
        musteragent_tool::ToolExecutionPolicy::LegacyFixed { .. } => {
            Err(port(ErrorCode::UnsupportedSchemaVersion, false))
        }
    }
}

impl DurableToolExecutor {
    pub(super) async fn resolve_authorized_interaction(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<Option<ToolExecutionOutcome>, LoopPortError> {
        match durable.interaction_resolution {
            Some(InteractionResolution::Pending) => {
                return durable
                    .invocation
                    .interaction_id
                    .map(|interaction_id| Some(ToolExecutionOutcome::Waiting { interaction_id }))
                    .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false));
            }
            Some(InteractionResolution::Approval(musteragent_approval::ApprovalDecision::Deny))
                if !durable.permits_interaction_resume() =>
            {
                // Business confirmations may be requested after an allowed
                // dispatch. A denied confirmation still cannot restart a Tool
                // merely because its ordinary capability policy permits work.
                return self
                    .finish_rejected_approval(
                        attempt,
                        call,
                        material,
                        durable.clone(),
                        failed_outcome(
                            ErrorCode::PolicyDenied,
                            "The requested confirmation was declined",
                        )?,
                    )
                    .await
                    .map(Some);
            }
            Some(InteractionResolution::Expired) => {
                if material.tool.manifest.wire_name.as_str() != "interaction_request" {
                    return self
                        .finish_rejected_approval(
                            attempt,
                            call,
                            material,
                            durable.clone(),
                            failed_outcome(
                                ErrorCode::InteractionExpired,
                                "The requested user interaction expired",
                            )?,
                        )
                        .await
                        .map(Some);
                }
                // A user-input timeout is an answer boundary, not a failed
                // business operation. Let the Interaction capability turn it
                // into explicit model context so the Agent can proceed.
            }
            Some(InteractionResolution::RetiredResultReview) => {
                // A retired result question is not permission and cannot resume
                // the suspended effect. Finish only this original Tool, then
                // the existing Run may make a fresh evidence assessment.
                return self
                    .finish_rejected_approval(
                        attempt,
                        call,
                        material,
                        durable.clone(),
                        ToolTerminalOutcome::Cancelled(CancelReason::PolicyRevoked),
                    )
                    .await
                    .map(Some);
            }
            Some(InteractionResolution::Cancelled) => {
                if material.tool.manifest.wire_name.as_str() != "interaction_request" {
                    return self
                        .finish_rejected_approval(
                            attempt,
                            call,
                            material,
                            durable.clone(),
                            ToolTerminalOutcome::Cancelled(CancelReason::UserRequested),
                        )
                        .await
                        .map(Some);
                }
                // Declining a question does not cancel the Run. The
                // capability will return a protocol-defined instruction to
                // continue with best judgement.
            }
            None
            | Some(InteractionResolution::Approval(_))
            | Some(InteractionResolution::Content(_))
            | Some(InteractionResolution::Secure(_)) => {}
        }
        Ok(None)
    }
}
