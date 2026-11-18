use super::*;

impl DurableToolExecutor {
    pub(super) async fn require_approval(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: DurableToolInvocation,
        authorization: InvocationAuthorization,
        policy: ApprovalRequestPolicy,
    ) -> Result<PreparedTool, LoopPortError> {
        let ApprovalRequestPolicy {
            policy_reason,
            consequence_fingerprint,
            session_reusable,
        } = policy;
        match durable.interaction_resolution.clone() {
            Some(InteractionResolution::Pending) => durable
                .invocation
                .interaction_id
                .map(|interaction_id| {
                    PreparedTool::Outcome(ToolExecutionOutcome::Waiting { interaction_id })
                })
                .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false)),
            Some(InteractionResolution::Approval(
                musteragent_approval::ApprovalDecision::Approve,
            ))
            | Some(InteractionResolution::Approval(
                musteragent_approval::ApprovalDecision::ApproveSession,
            )) => Ok(PreparedTool::authorized(material, durable, authorization)),
            Some(InteractionResolution::Approval(musteragent_approval::ApprovalDecision::Deny)) => {
                self.finish_rejected_approval(
                    attempt,
                    call,
                    material,
                    durable,
                    failed_outcome(ErrorCode::PolicyDenied, "The Tool approval was denied")?,
                )
                .await
                .map(PreparedTool::Outcome)
            }
            Some(InteractionResolution::Expired) => self
                .finish_rejected_approval(
                    attempt,
                    call,
                    material,
                    durable,
                    failed_outcome(ErrorCode::InteractionExpired, "The Tool approval expired")?,
                )
                .await
                .map(PreparedTool::Outcome),
            Some(InteractionResolution::RetiredResultReview) => self
                .finish_rejected_approval(
                    attempt,
                    call,
                    material,
                    durable,
                    ToolTerminalOutcome::Cancelled(CancelReason::PolicyRevoked),
                )
                .await
                .map(PreparedTool::Outcome),
            Some(InteractionResolution::Cancelled) => self
                .finish_rejected_approval(
                    attempt,
                    call,
                    material,
                    durable,
                    ToolTerminalOutcome::Cancelled(CancelReason::UserRequested),
                )
                .await
                .map(PreparedTool::Outcome),
            None => {
                let interaction_id = interaction_id(durable.invocation.invocation_id, b"approval")?;
                // The prompt is an immutable authorization statement for this
                // exact invocation. Showing only a Tool title would allow an
                // approval to be socially ambiguous even when the database
                // binds it to an invocation. Include the normalized arguments
                // and immutable request digest so both the user and later
                // auditors can identify precisely what was authorized.
                let invocation = self
                    .capability_invocation(attempt, call, material, &durable)
                    .await?;
                let capability_prompt = self
                    .bound_dispatcher(material)?
                    .approval_prompt(material, &invocation)
                    .await?;
                let capability_detail = capability_prompt.map(|prompt| {
                    let hash = Sha256::digest(&prompt.bytes);
                    let is_json = prompt.media_type.as_str() == "application/json"
                        || prompt.media_type.as_str().ends_with("+json");
                    ApprovalPromptCapabilityDetail {
                        media_type: prompt.media_type.as_str().to_owned(),
                        sha256: hash.to_string(),
                        byte_length: u64::try_from(prompt.bytes.len()).unwrap_or(u64::MAX),
                        content_json: is_json
                            .then(|| serde_json::from_slice(&prompt.bytes).ok())
                            .flatten(),
                        content_text: (!is_json)
                            .then(|| String::from_utf8(prompt.bytes).ok())
                            .flatten(),
                    }
                });
                let policy_arguments = execution_arguments(&call.arguments, material);
                let policy_input = InvocationPolicyInput {
                    workspace_id: workspace_id(&material.run_context.execution_scope),
                    tool: &material.tool.manifest,
                    arguments: &policy_arguments,
                    policy_basis: material.policy_basis.as_ref(),
                    authorized_content: &material.authorized_content,
                };
                let (invocation_policy_ref, invocation_policy_hash) =
                    invocation_policy_identity_for(&policy_input);
                let prompt_bytes = serde_json::to_vec(&ApprovalPromptEnvelope {
                    schema: if invocation_policy_ref.starts_with("musteragent.") {
                        "musteragent.tool.approval.prompt.v4"
                    } else {
                        "musteragent.legacy.tool.approval.prompt.v3"
                    },
                    invocation_id: durable.invocation.invocation_id.to_string(),
                    consequence_fingerprint: consequence_fingerprint.to_string(),
                    session_reusable,
                    current_task_records: material
                        .policy_basis
                        .as_ref()
                        .map(|basis| {
                            basis
                                .source_records
                                .iter()
                                .map(|record| ApprovalPromptTaskProvenanceRecord {
                                    record_id: record.record_id.to_string(),
                                    source_kind: record.source_kind.as_str(),
                                    content_id: record.content_ref.content_id.to_string(),
                                    content_sha256: record.content_ref.sha256.to_string(),
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                    invocation_policy_ref,
                    invocation_policy_hash: invocation_policy_hash.to_string(),
                    agent_policy_ref: material
                        .policy_basis
                        .as_ref()
                        .map(|basis| basis.agent_policy_ref.as_str()),
                    agent_policy_hash: material
                        .policy_basis
                        .as_ref()
                        .map(|basis| basis.agent_policy_hash.to_string()),
                    permission_mode: Some(material.permission_mode.as_str()),
                    title: &material.tool.title,
                    description: &material.tool.description,
                    tool: material.tool.manifest.wire_name.as_str(),
                    effect: format!("{:?}", material.tool.manifest.effect),
                    arguments: policy_arguments,
                    request: ApprovalPromptRequestIdentity {
                        content_id: durable.invocation.request_ref.content_id.to_string(),
                        media_type: durable.invocation.request_ref.media_type.as_str(),
                        byte_length: durable.invocation.request_ref.byte_length,
                        sha256: durable.invocation.request_ref.sha256.to_string(),
                    },
                    reason: policy_reason.unwrap_or_else(|| {
                        if material.capability_grant.policy_decision == Decision::RequireApproval {
                            "The signed Capability policy requires approval"
                        } else if material.tool.manifest.approval
                            == musteragent_policy::ApprovalRequirement::Always
                        {
                            "This Tool always requires approval"
                        } else {
                            "The parameter-aware invocation policy requires approval"
                        }
                    }),
                    capability_detail,
                })
                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                let prompt_media_type = media_type()?;
                if prompt_bytes.is_empty() {
                    return Err(port(ErrorCode::InternalInvariantViolated, false));
                }
                let prompt_ref = self
                    .content
                    .put_at(
                        crate::producer_content::identity(
                            durable.invocation.invocation_id,
                            "approval-prompt",
                        ),
                        PutContent {
                            tenant_id: material.run_context.tenant_id,
                            media_type: prompt_media_type,
                            bytes: prompt_bytes,
                        },
                    )
                    .await
                    .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?;
                let requested_at = self.clock.now();
                let expires_at = requested_at
                    .checked_add_millis(APPROVAL_LIFETIME_MS)
                    .ok_or_else(|| port(ErrorCode::InternalInvariantViolated, false))?;
                let request = ApprovalRequest {
                    interaction_id,
                    invocation_id: durable.invocation.invocation_id,
                    prompt_ref,
                    response_schema: musteragent_content_ref::SchemaRef {
                        schema_key: musteragent_identity::VersionedKey::new(
                            APPROVAL_RESPONSE_SCHEMA_KEY,
                        )
                        .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?,
                        schema_hash: Sha256::digest(APPROVAL_SCHEMA),
                    },
                    requested_at,
                    // Approval authority is intentionally finite. Recovery may
                    // preserve a pending interaction, but it cannot turn one
                    // old click into indefinite authorization.
                    expires_at: Some(expires_at),
                };
                Ok(PreparedTool::Outcome(
                    ToolExecutionOutcome::ApprovalRequired { request },
                ))
            }
            Some(InteractionResolution::Content(_) | InteractionResolution::Secure(_)) => {
                Err(port(ErrorCode::InternalInvariantViolated, false))
            }
        }
    }

    pub(super) async fn finish_rejected_approval(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: DurableToolInvocation,
        outcome: ToolTerminalOutcome,
    ) -> Result<ToolExecutionOutcome, LoopPortError> {
        let completed = self
            .journal
            .complete(
                attempt,
                durable.invocation.invocation_id,
                outcome.clone(),
                self.clock.now(),
            )
            .await
            .map_err(port_from_tool)?;
        self.message_for_outcome(call, material, &completed, &outcome)
            .await
    }
}
