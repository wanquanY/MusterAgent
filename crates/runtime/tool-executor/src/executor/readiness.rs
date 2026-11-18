//! Application readiness gate before an account application call runs.
//!
//! The application owner, not the model, decides whether the user must act
//! (reconnect, grant a permission, turn the app back on). When they must, this
//! Tool call itself waits on the original capability card, so the model never
//! has to notice the problem or copy release identities. Once the owner
//! completes the card, the same call continues in the same Run.
//!
//! The gate only covers calls the Run is already allowed to make: one
//! invocation owns one Interaction, so calls that also need an approval keep
//! the approval flow and the typed failure guidance.
use super::*;
use musteragent_runtime_capability::InputInteractionRequest;
use musteragent_runtime_interaction::{
    CAPABILITY_RESPONSE_SCHEMA, CapabilityRequest, CapabilityRequestResolution,
    CapabilityRequestResolver,
};
use std::time::Duration;

/// Readiness is advisory before dispatch: a slow owner never blocks the call.
const READINESS_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) fn readiness_interaction_id(
    invocation_id: InvocationId,
) -> Result<InteractionId, LoopPortError> {
    interaction_id(invocation_id, b"application-readiness")
}

/// True when this invocation's Interaction is the readiness card, which is
/// never an approval of the call.
pub(super) fn is_readiness_interaction(durable: &DurableToolInvocation) -> bool {
    durable.invocation.interaction_id.is_some_and(|id| {
        readiness_interaction_id(durable.invocation.invocation_id).is_ok_and(|gate| gate == id)
    })
}

fn card_text(action: &str, tool_title: &str) -> (String, String) {
    let title = match action {
        "connect" => "连接应用后继续",
        "authorize" => "授予权限后继续",
        "enable" => "启用应用后继续",
        "update" => "更新应用后继续",
        _ => "完成应用设置后继续",
    };
    let subject: String = tool_title.chars().take(80).collect();
    (
        title.to_owned(),
        format!("「{subject}」需要先完成这一步，完成后会自动继续。"),
    )
}

impl DurableToolExecutor {
    #[must_use]
    pub fn with_application_readiness(
        mut self,
        resolver: Arc<dyn CapabilityRequestResolver>,
    ) -> Self {
        self.application_readiness = Some(resolver);
        self
    }

    async fn prepared_readiness(
        &self,
        tenant: musteragent_identity::TenantId,
        id: musteragent_identity::ContentId,
    ) -> Result<Option<(ContentRef, CapabilityRequest)>, LoopPortError> {
        let Some(reference) = self
            .content
            .find_prepared_at(tenant, id)
            .await
            .map_err(|_| port(ErrorCode::DependencyUnavailable, true))?
        else {
            return Ok(None);
        };
        if reference.content_id != id
            || reference.byte_length > 8192
            || reference.media_type.as_str() != JSON_MEDIA_TYPE
        {
            return Err(port(ErrorCode::ContentIntegrityFailed, false));
        }
        let bytes = self
            .content
            .read(tenant, &reference)
            .await
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        musteragent_runtime_content::verify_content(&reference, &bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        let request = CapabilityRequest::parse(&bytes)
            .map_err(|_| port(ErrorCode::ContentIntegrityFailed, false))?;
        Ok(Some((reference, request)))
    }

    /// `None` lets the call continue; `Some` is the waiting or finished outcome.
    pub(super) async fn application_readiness(
        &self,
        attempt: &AttemptContext,
        call: &PlannedToolCall,
        material: &ToolExecutionMaterial,
        durable: &DurableToolInvocation,
    ) -> Result<Option<PreparedTool>, LoopPortError> {
        let Some(resolver) = self.application_readiness.as_ref() else {
            return Ok(None);
        };
        let Some(binding) = material.tool.manifest.application.as_ref() else {
            return Ok(None);
        };
        let Some(product) = material.product_release.as_ref().map(|r| r.product) else {
            return Ok(None);
        };
        let gate = readiness_interaction_id(durable.invocation.invocation_id)?;
        match durable.invocation.interaction_id {
            Some(id) if id == gate => {
                return match durable.interaction_resolution.clone() {
                    Some(InteractionResolution::Pending) => {
                        Ok(Some(PreparedTool::Outcome(ToolExecutionOutcome::Waiting {
                            interaction_id: gate,
                        })))
                    }
                    // The owner verified completion; dispatch the original call.
                    Some(InteractionResolution::Secure(_)) => Ok(None),
                    Some(InteractionResolution::Cancelled | InteractionResolution::Expired) => {
                        let outcome = failed_outcome(
                            ErrorCode::PolicyDenied,
                            "The user did not finish setting up this application, so the \
                             call was not made. Say what it is needed for and continue \
                             without it unless they set it up.",
                        )?;
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
                            .map(|outcome| Some(PreparedTool::Outcome(outcome)))
                    }
                    _ => Err(port(ErrorCode::InternalInvariantViolated, false)),
                };
            }
            // Another Interaction (an approval) owns this invocation.
            Some(_) => return Ok(None),
            None => {}
        }
        let tenant = material.run_context.tenant_id;
        let id =
            crate::producer_content::identity(durable.invocation.invocation_id, "readiness-prompt");
        // A crash after the prompt prewrite must not re-resolve a release that
        // may have changed since: recover the exact prepared card first.
        let (prompt_ref, request) = match self.prepared_readiness(tenant, id).await? {
            Some(prepared) => prepared,
            None => {
                let resolution = tokio::time::timeout(
                    READINESS_TIMEOUT,
                    resolver.resolve(
                        material.actor,
                        product,
                        Some(material.agent_id),
                        &format!("plugin/{}", binding.selection.plugin_id),
                        None,
                    ),
                )
                .await;
                let descriptor = match resolution {
                    Ok(Ok(CapabilityRequestResolution::Required(descriptor))) => descriptor,
                    // Ready, unknown or slow: dispatch; typed failures still guide the model.
                    _ => return Ok(None),
                };
                let (title, description) = card_text(&descriptor.action, &material.tool.title);
                let bytes = CapabilityRequest {
                    schema: musteragent_runtime_interaction::CAPABILITY_PROMPT_SCHEMA.into(),
                    title,
                    description,
                    capability: descriptor,
                }
                .canonical_prompt_bytes()
                .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                // Re-validate with the same parser the completion owner uses.
                let request = CapabilityRequest::parse(&bytes)
                    .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
                match self
                    .content
                    .put_at(
                        id,
                        PutContent {
                            tenant_id: tenant,
                            media_type: media_type()?,
                            bytes,
                        },
                    )
                    .await
                {
                    Ok(reference) => (reference, request),
                    // A concurrent fenced attempt prepared it first; use theirs.
                    Err(_) => self
                        .prepared_readiness(tenant, id)
                        .await?
                        .ok_or_else(|| port(ErrorCode::DependencyUnavailable, true))?,
                }
            }
        };
        let response_schema = request
            .response_schema()
            .map_err(|_| port(ErrorCode::InternalInvariantViolated, false))?;
        if response_schema.schema_key.as_str() != CAPABILITY_RESPONSE_SCHEMA {
            return Err(port(ErrorCode::InternalInvariantViolated, false));
        }
        let waiting = self
            .journal
            .request_input(
                attempt,
                InputInteractionRequest {
                    interaction_id: gate,
                    invocation_id: durable.invocation.invocation_id,
                    kind: musteragent_approval::InteractionKind::OAuth,
                    prompt_ref,
                    response_schema,
                    requested_at: self.clock.now(),
                    // User think time is closed by the Interaction lifecycle.
                    expires_at: None,
                },
            )
            .await;
        let waiting = match waiting {
            Ok(waiting) => waiting,
            // The same card was already offered or refused in this
            // conversation: never nag. Dispatch and let typed failure speak.
            Err(error) if error.code == ErrorCode::PolicyDenied => return Ok(None),
            Err(error) => return Err(port_from_tool(error)),
        };
        if waiting.invocation.invocation_id != durable.invocation.invocation_id
            || waiting.invocation.status != ToolInvocationStatus::WaitingInteraction
            || waiting.invocation.interaction_id != Some(gate)
        {
            return Err(port(ErrorCode::InternalInvariantViolated, false));
        }
        Ok(Some(PreparedTool::Outcome(ToolExecutionOutcome::Waiting {
            interaction_id: gate,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn card_text_is_bounded_and_names_the_call() {
        let (title, description) = card_text("connect", &"查".repeat(500));
        assert_eq!(title, "连接应用后继续");
        assert!(description.chars().count() < 120);
        assert_eq!(card_text("authorize", "x").0, "授予权限后继续");
        assert_eq!(card_text("unknown", "x").0, "完成应用设置后继续");
    }

    #[test]
    fn readiness_identity_is_stable_and_distinct_from_approval() {
        let invocation = InvocationId::from_uuid(uuid::Uuid::from_u128(7)).unwrap();
        let gate = readiness_interaction_id(invocation).unwrap();
        assert_eq!(gate, readiness_interaction_id(invocation).unwrap());
        assert_ne!(gate, interaction_id(invocation, b"approval").unwrap());
    }
}
