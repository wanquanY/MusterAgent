//! Reconstruct private approval facts from the already-authorized Ledger cut.
//! Both Home stores use this owner. Neither model text nor Tool JSON is evidence.
use crate::{ContextEntry, ContextPlanError, ContextPresentationOrder};
use musteragent_approval::{
    ApprovalDecision, InteractionEvent, InteractionEventResponse, InteractionKind,
};
use musteragent_identity::{
    ConversationSeq, InteractionId, InvocationId, LedgerRecordId, RunId, ToolCallId,
};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalRuntimeEvent, ModelText,
    ToolApprovalObservation, ToolApprovalResolution,
};
use musteragent_tool::{ToolEvent, ToolKey};
use std::collections::BTreeMap;

struct ToolRequest {
    call: ToolCallId,
    key: ToolKey,
    run: RunId,
    record: LedgerRecordId,
}
struct Request {
    kind: InteractionKind,
    resolved: bool,
    invocation: InvocationId,
    run: RunId,
    record: LedgerRecordId,
    sequence: ConversationSeq,
}

#[derive(Default)]
pub struct ApprovalContextCollector {
    tools: BTreeMap<InvocationId, ToolRequest>,
    requests: BTreeMap<InteractionId, Request>,
    resolved: BTreeMap<InteractionId, ContextEntry>,
}
impl ApprovalContextCollector {
    pub fn observe_tool(
        &mut self,
        event: &ToolEvent,
        run: RunId,
        record: LedgerRecordId,
    ) -> Result<(), ContextPlanError> {
        if let ToolEvent::InvocationRequested {
            invocation_id,
            tool_call_id,
            tool_key,
            ..
        } = event
            && self
                .tools
                .insert(
                    *invocation_id,
                    ToolRequest {
                        call: tool_call_id.clone(),
                        key: tool_key.clone(),
                        run,
                        record,
                    },
                )
                .is_some()
        {
            return Err(ContextPlanError::invariant());
        }
        Ok(())
    }
    pub fn observe_interaction(
        &mut self,
        event: &InteractionEvent,
        run: RunId,
        record: LedgerRecordId,
        sequence: ConversationSeq,
    ) -> Result<(), ContextPlanError> {
        let (interaction, resolution) = match event {
            InteractionEvent::Requested {
                interaction_id,
                invocation_id,
                kind: kind @ (InteractionKind::Approval | InteractionKind::Confirmation),
                ..
            } => {
                let tool = self
                    .tools
                    .get(invocation_id)
                    .ok_or_else(ContextPlanError::invariant)?;
                if tool.run != run
                    || self
                        .requests
                        .insert(
                            *interaction_id,
                            Request {
                                kind: *kind,
                                resolved: false,
                                invocation: *invocation_id,
                                run,
                                record,
                                sequence,
                            },
                        )
                        .is_some()
                {
                    return Err(ContextPlanError::invariant());
                }
                return Ok(());
            }
            InteractionEvent::Responded {
                interaction_id,
                response: InteractionEventResponse::Approval(decision),
                ..
            } => (
                *interaction_id,
                match decision {
                    ApprovalDecision::Approve => ToolApprovalResolution::Approved,
                    ApprovalDecision::ApproveSession => ToolApprovalResolution::ApprovedSession,
                    ApprovalDecision::Deny => ToolApprovalResolution::Denied,
                },
            ),
            InteractionEvent::Expired { interaction_id, .. } => {
                (*interaction_id, ToolApprovalResolution::Expired)
            }
            InteractionEvent::Cancelled { interaction_id, .. } => {
                (*interaction_id, ToolApprovalResolution::Cancelled)
            }
            _ => return Ok(()),
        };
        let Some(request) = self.requests.get_mut(&interaction) else {
            // Input and secure interactions can expire/cancel too; they are not approvals.
            if matches!(
                event,
                InteractionEvent::Responded {
                    response: InteractionEventResponse::Approval(_),
                    ..
                }
            ) {
                return Err(ContextPlanError::invariant());
            }
            return Ok(());
        };
        let tool = self
            .tools
            .get(&request.invocation)
            .ok_or_else(ContextPlanError::invariant)?;
        if request.run != run
            || sequence <= request.sequence
            || record == request.record
            || request.resolved
        {
            return Err(ContextPlanError::invariant());
        }
        if request.kind == InteractionKind::Confirmation {
            // A business confirmation shares the decision wire shape with a
            // permission approval, but grants no Tool authority. Its original
            // request and terminal ordering still have to be proven above.
            if resolution == ToolApprovalResolution::ApprovedSession {
                return Err(ContextPlanError::invariant());
            }
            request.resolved = true;
            return Ok(());
        }
        let observation = ToolApprovalObservation {
            invocation_id: request.invocation,
            tool_call_id: tool.call.clone(),
            tool_key: tool.key.clone(),
            interaction_id: interaction,
            request_record_id: request.record,
            resolution_record_id: record,
            resolution,
        };
        let sizing = ModelText::new(observation.model_input_text())
            .map_err(|_| ContextPlanError::invariant())?;
        request.resolved = true;
        self.resolved.insert(
            interaction,
            ContextEntry {
                history_page: None,
                runtime_events: vec![CanonicalRuntimeEvent::ToolApprovalResolved(observation)],
                source_sequence: sequence,
                presentation_order: ContextPresentationOrder::ledger(sequence),
                source_record_ids: vec![tool.record, request.record, record],
                source_run_id: Some(run),
                assistant_origin: None,
                // Sizing/compaction only. Provider context uses the typed private event.
                message: CanonicalMessage {
                    role: CanonicalRole::Assistant,
                    content: vec![CanonicalContentPart::Text(sizing)],
                    tool_calls: vec![],
                    tool_call_id: None,
                    tool_name: None,
                    reasoning: None,
                },
                continuation_delta_for: None,
            },
        );
        Ok(())
    }
    pub fn entries(&self) -> impl Iterator<Item = &ContextEntry> {
        self.resolved.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
    use musteragent_identity::{ContentId, PrincipalId, UtcTimestamp, VersionedKey};
    fn requested() -> (
        ApprovalContextCollector,
        InteractionEvent,
        RunId,
        InteractionId,
    ) {
        let mut collector = ApprovalContextCollector::default();
        let run = RunId::generate();
        let invocation = InvocationId::generate();
        let interaction = InteractionId::generate();
        let reference = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").unwrap(),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        collector.observe_tool(&ToolEvent::InvocationRequested {
            invocation_id: invocation, tool_call_id: ToolCallId::new("call_exact_memory").unwrap(),
            tool_key: ToolKey::from_versioned_key(VersionedKey::new("experience.publish@1").unwrap()),
            effect: musteragent_policy::Effect::WriteReversible, request_ref: reference.clone(),
            timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(1000) },
        }, run, LedgerRecordId::generate()).unwrap();
        let request = InteractionEvent::Requested {
            interaction_id: interaction,
            invocation_id: invocation,
            kind: InteractionKind::Approval,
            prompt_ref: reference,
            response_schema: SchemaRef {
                schema_key: VersionedKey::new("approval.response@1").unwrap(),
                schema_hash: Sha256::digest(b"schema"),
            },
            expires_at: None,
        };
        (collector, request, run, interaction)
    }
    fn respond(id: InteractionId, decision: ApprovalDecision) -> InteractionEvent {
        InteractionEvent::Responded {
            interaction_id: id,
            responded_by: PrincipalId::generate(),
            response: InteractionEventResponse::Approval(decision),
            responded_at: UtcTimestamp::from_unix_millis(500),
        }
    }
    #[test]
    fn approval_lifecycle_is_private_evidence_and_never_claims_an_execution_result() {
        for (decision, expected) in [
            (ApprovalDecision::Approve, ToolApprovalResolution::Approved),
            (
                ApprovalDecision::ApproveSession,
                ToolApprovalResolution::ApprovedSession,
            ),
            (ApprovalDecision::Deny, ToolApprovalResolution::Denied),
        ] {
            let (mut collector, request, run, id) = requested();
            let request_record = LedgerRecordId::generate();
            let response_record = LedgerRecordId::generate();
            collector
                .observe_interaction(
                    &request,
                    run,
                    request_record,
                    ConversationSeq::new(2).unwrap(),
                )
                .unwrap();
            assert_eq!(collector.entries().count(), 0);
            collector
                .observe_interaction(
                    &respond(id, decision),
                    run,
                    response_record,
                    ConversationSeq::new(3).unwrap(),
                )
                .unwrap();
            let entry = collector.entries().next().unwrap();
            let CanonicalRuntimeEvent::ToolApprovalResolved(fact) = &entry.runtime_events[0] else {
                panic!("typed observation")
            };
            assert_eq!(fact.interaction_id, id);
            assert_eq!(fact.resolution, expected);
            assert_eq!(fact.request_record_id, request_record);
            assert_eq!(fact.resolution_record_id, response_record);
            assert_eq!(entry.source_run_id, Some(run));
            assert_eq!(entry.source_record_ids.len(), 3);
            assert!(
                fact.model_input_text()
                    .contains("not a user message or a new authorization")
            );
            assert!(
                fact.model_input_text()
                    .contains("execution outcome are separate")
            );
        }
    }
    #[test]
    fn unmatched_cross_run_duplicate_and_out_of_order_decisions_are_rejected() {
        let (mut collector, request, run, id) = requested();
        let response = respond(id, ApprovalDecision::Approve);
        assert!(
            collector
                .observe_interaction(
                    &response,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(3).unwrap()
                )
                .is_err()
        );
        assert!(
            collector
                .observe_interaction(
                    &request,
                    RunId::generate(),
                    LedgerRecordId::generate(),
                    ConversationSeq::new(2).unwrap()
                )
                .is_err()
        );
        collector
            .observe_interaction(
                &request,
                run,
                LedgerRecordId::generate(),
                ConversationSeq::new(2).unwrap(),
            )
            .unwrap();
        assert!(
            collector
                .observe_interaction(
                    &response,
                    RunId::generate(),
                    LedgerRecordId::generate(),
                    ConversationSeq::new(3).unwrap()
                )
                .is_err()
        );
        assert!(
            collector
                .observe_interaction(
                    &response,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(1).unwrap()
                )
                .is_err()
        );
        collector
            .observe_interaction(
                &response,
                run,
                LedgerRecordId::generate(),
                ConversationSeq::new(3).unwrap(),
            )
            .unwrap();
        assert!(
            collector
                .observe_interaction(
                    &response,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(4).unwrap()
                )
                .is_err()
        );
        assert_eq!(collector.entries().count(), 1);
    }
    #[test]
    fn business_confirmations_preserve_provenance_without_granting_tool_approval() {
        for decision in [ApprovalDecision::Approve, ApprovalDecision::Deny] {
            let (mut collector, mut request, run, id) = requested();
            let InteractionEvent::Requested { kind, .. } = &mut request else {
                unreachable!()
            };
            *kind = InteractionKind::Confirmation;
            collector
                .observe_interaction(
                    &request,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(2).unwrap(),
                )
                .unwrap();
            let response = respond(id, decision);
            for (wrong_run, wrong_sequence) in [(RunId::generate(), 3), (run, 1)] {
                assert!(
                    collector
                        .observe_interaction(
                            &response,
                            wrong_run,
                            LedgerRecordId::generate(),
                            ConversationSeq::new(wrong_sequence).unwrap()
                        )
                        .is_err()
                );
            }
            assert!(
                collector
                    .observe_interaction(
                        &respond(id, ApprovalDecision::ApproveSession),
                        run,
                        LedgerRecordId::generate(),
                        ConversationSeq::new(3).unwrap()
                    )
                    .is_err()
            );
            collector
                .observe_interaction(
                    &response,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(3).unwrap(),
                )
                .unwrap();
            assert_eq!(
                collector.entries().count(),
                0,
                "business confirmation is not execution authorization"
            );
            assert!(
                collector
                    .observe_interaction(
                        &response,
                        run,
                        LedgerRecordId::generate(),
                        ConversationSeq::new(4).unwrap()
                    )
                    .is_err()
            );
        }
    }
    #[test]
    fn confirmation_expiry_and_cancellation_do_not_emit_approval_observations() {
        for expired in [true, false] {
            let (mut collector, mut request, run, id) = requested();
            let InteractionEvent::Requested { kind, .. } = &mut request else {
                unreachable!()
            };
            *kind = InteractionKind::Confirmation;
            collector
                .observe_interaction(
                    &request,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(2).unwrap(),
                )
                .unwrap();
            let event = if expired {
                InteractionEvent::Expired {
                    interaction_id: id,
                    expired_at: UtcTimestamp::from_unix_millis(1001),
                }
            } else {
                InteractionEvent::Cancelled {
                    interaction_id: id,
                    reason: musteragent_runtime_contract::CancelReason::UserRequested,
                }
            };
            collector
                .observe_interaction(
                    &event,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(3).unwrap(),
                )
                .unwrap();
            assert_eq!(collector.entries().count(), 0);
            assert!(
                collector
                    .observe_interaction(
                        &respond(id, ApprovalDecision::Approve),
                        run,
                        LedgerRecordId::generate(),
                        ConversationSeq::new(4).unwrap()
                    )
                    .is_err()
            );
        }
    }
    #[test]
    fn expiry_and_cancellation_are_not_approval() {
        for expired in [true, false] {
            let (mut collector, request, run, id) = requested();
            let event = if expired {
                InteractionEvent::Expired {
                    interaction_id: id,
                    expired_at: UtcTimestamp::from_unix_millis(1001),
                }
            } else {
                InteractionEvent::Cancelled {
                    interaction_id: id,
                    reason: musteragent_runtime_contract::CancelReason::UserRequested,
                }
            };
            // Closures of non-approval interactions do not create approval evidence.
            collector
                .observe_interaction(
                    &event,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(2).unwrap(),
                )
                .unwrap();
            assert_eq!(collector.entries().count(), 0);
            collector
                .observe_interaction(
                    &request,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(2).unwrap(),
                )
                .unwrap();
            collector
                .observe_interaction(
                    &event,
                    run,
                    LedgerRecordId::generate(),
                    ConversationSeq::new(3).unwrap(),
                )
                .unwrap();
            let CanonicalRuntimeEvent::ToolApprovalResolved(value) =
                &collector.entries().next().unwrap().runtime_events[0]
            else {
                panic!("typed")
            };
            assert_eq!(
                value.resolution,
                if expired {
                    ToolApprovalResolution::Expired
                } else {
                    ToolApprovalResolution::Cancelled
                }
            );
        }
    }
}
