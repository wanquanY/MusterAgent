//! Restart-safe authoritative Ledger to Conversation Catalog projector.

use std::sync::Arc;

use musteragent_conversation::{ConversationEvent, MessageRole, PartialMessageReason};
use musteragent_event_envelope::{LedgerCursor, LedgerMetadata, RuntimeHome};
use musteragent_goal::{GoalEvaluation, GoalPursuit, StandingGoal};
use musteragent_identity::{
    Actor, ConversationId, HomeEpoch, LedgerOffset, ParticipantId, PrincipalId, RunId, TenantId,
    UtcTimestamp,
};
use musteragent_ledger::DomainEvent;
use musteragent_runtime_contract::{ApplicationFuture, CancelReason};

use crate::{
    CatalogAttentionAudience, CatalogEventContext, CatalogFactDraft,
    CatalogProjectionApplyDisposition, CatalogRunProjectionContext, SubscriptionError,
    adapt_activity_event, adapt_artifact_event, adapt_conversation_event, adapt_goal_evaluation,
    adapt_goal_pursuit, adapt_interaction_event, adapt_run_event, adapt_standing_goal,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CatalogProjectionAuthority {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub after: LedgerOffset,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CatalogRunCancellationAuthority {
    pub reason: CancelReason,
    pub requested_by: PrincipalId,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CatalogRunAuthority {
    pub parent_run_id: Option<RunId>,
    pub delegation_id: Option<musteragent_identity::DelegationId>,
    pub pursuit_id: Option<musteragent_identity::GoalPursuitId>,
    pub requested_by_participant_id: ParticipantId,
    pub cancellation: Option<CatalogRunCancellationAuthority>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogGoalProjectionSnapshot {
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub goal: StandingGoal,
    pub pursuit: Option<GoalPursuit>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogGoalEvaluationProjectionSnapshot {
    pub control_home: RuntimeHome,
    pub home_epoch: HomeEpoch,
    pub conversation_id: ConversationId,
    pub evaluation: GoalEvaluation,
}

/// One canonical Conversation sequence item consumed by the Catalog projector.
///
/// Output Frame payloads are intentionally absent: Catalog only needs their
/// sequence position so it can advance a checkpoint across reclaimed public
/// output bytes. Legacy gaps are accepted only wholly below the public
/// retained watermark. Domain Event payloads remain hash-verified and typed.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogProjectionRecord {
    pub metadata: LedgerMetadata,
    pub event: Option<DomainEvent>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogProjectionRecordPage {
    pub records: Vec<CatalogProjectionRecord>,
    /// Highest sequence proven consumable by this page. It may be greater
    /// than the last returned record only when the omitted suffix is wholly
    /// below the public retained watermark (legacy Output spine gaps).
    pub through: LedgerOffset,
}

/// Internal rebuild source for the durable Conversation Catalog.
///
/// This is deliberately separate from the public Snapshot + Tail reader. A
/// rebuildable server-side projection must be able to replay canonical Domain
/// Events even after public Output Frame payloads have crossed the retention
/// watermark. Infrastructure adapters still enforce tenant authorization,
/// exact continuity throughout the retained tail, metadata validation and
/// payload hashes.
pub trait ConversationCatalogRecordSource: Send + Sync {
    fn list_catalog_projection_records(
        &self,
        actor: Actor,
        cursor: LedgerCursor,
        limit: usize,
    ) -> ApplicationFuture<'_, Result<CatalogProjectionRecordPage, SubscriptionError>>;
}

#[derive(Debug, thiserror::Error, Copy, Clone, Eq, PartialEq)]
pub enum ConversationCatalogProjectorError {
    #[error("Conversation Catalog projector source is unavailable")]
    Unavailable,
    #[error("Conversation Catalog projector record is corrupt")]
    Corrupt,
    #[error("Conversation Catalog projector authority is stale")]
    AuthorityStale,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CatalogProjectionDeferral {
    pub authority: CatalogProjectionAuthority,
    pub error: ConversationCatalogProjectorError,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ConversationCatalogProjectionPass {
    pub projected_facts: usize,
    pub advanced_authorities: usize,
    pub finalized_bootstraps: usize,
    pub deferred: Vec<CatalogProjectionDeferral>,
}

impl ConversationCatalogProjectionPass {
    #[must_use]
    pub const fn made_progress(&self) -> bool {
        self.projected_facts > 0 || self.advanced_authorities > 0 || self.finalized_bootstraps > 0
    }
}

pub trait ConversationCatalogProjectionPort: Send + Sync {
    fn list_projection_authorities(
        &self,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<CatalogProjectionAuthority>, ConversationCatalogProjectorError>,
    >;

    fn resolve_run_authority(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<CatalogRunAuthority, ConversationCatalogProjectorError>>;

    fn list_goal_projection_snapshots(
        &self,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<CatalogGoalProjectionSnapshot>, ConversationCatalogProjectorError>,
    >;

    fn list_goal_evaluation_projection_snapshots(
        &self,
        limit: u16,
    ) -> ApplicationFuture<
        '_,
        Result<Vec<CatalogGoalEvaluationProjectionSnapshot>, ConversationCatalogProjectorError>,
    >;

    fn apply_catalog_fact(
        &self,
        draft: CatalogFactDraft,
    ) -> ApplicationFuture<
        '_,
        Result<CatalogProjectionApplyDisposition, ConversationCatalogProjectorError>,
    >;

    fn advance_projection_checkpoint(
        &self,
        authority: CatalogProjectionAuthority,
        through: LedgerOffset,
        projected_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<(), ConversationCatalogProjectorError>>;

    /// Finalizes upgrade-time read baselines whose complete historical state
    /// has reached the Catalog. Implementations write every queued Principal
    /// receipt, remove the visibility fence and publish one invalidation in a
    /// single transaction per bounded batch.
    fn finalize_bootstrap_receipts(
        &self,
        limit: u16,
        finalized_at: UtcTimestamp,
    ) -> ApplicationFuture<'_, Result<usize, ConversationCatalogProjectorError>>;
}

pub struct ConversationCatalogLedgerProjector {
    authority_limit: u16,
    record_page_size: usize,
    records: Arc<dyn ConversationCatalogRecordSource>,
    catalog: Arc<dyn ConversationCatalogProjectionPort>,
}

impl ConversationCatalogLedgerProjector {
    pub fn new(
        authority_limit: u16,
        record_page_size: usize,
        records: Arc<dyn ConversationCatalogRecordSource>,
        catalog: Arc<dyn ConversationCatalogProjectionPort>,
    ) -> Result<Self, ConversationCatalogProjectorError> {
        if authority_limit == 0
            || record_page_size == 0
            || record_page_size > musteragent_ledger::MAX_PAGE_SIZE
        {
            return Err(ConversationCatalogProjectorError::Corrupt);
        }
        Ok(Self {
            authority_limit,
            record_page_size,
            records,
            catalog,
        })
    }

    /// Runs one bounded pass. Checkpoint advancement happens after all facts;
    /// a crash before it only replays idempotent source keys.
    pub async fn run_pass(
        &self,
        now: UtcTimestamp,
    ) -> Result<ConversationCatalogProjectionPass, ConversationCatalogProjectorError> {
        let authorities = self
            .catalog
            .list_projection_authorities(self.authority_limit)
            .await?;
        let mut report = ConversationCatalogProjectionPass::default();
        for authority in authorities {
            match self.project_authority(authority, now).await {
                Ok((projected_facts, advanced)) => {
                    report.projected_facts += projected_facts;
                    report.advanced_authorities += usize::from(advanced);
                }
                Err(ConversationCatalogProjectorError::Unavailable) => {
                    // Availability failures normally identify a shared store
                    // outage. Failing fast avoids multiplying a single outage
                    // by the complete bounded authority page.
                    return Err(ConversationCatalogProjectorError::Unavailable);
                }
                Err(error) => report
                    .deferred
                    .push(CatalogProjectionDeferral { authority, error }),
            }
        }
        let goal_snapshots = self
            .catalog
            .list_goal_projection_snapshots(self.authority_limit)
            .await?;
        let goal_page_exhausted = goal_snapshots.len() < usize::from(self.authority_limit);
        for snapshot in goal_snapshots {
            let context = goal_context(
                snapshot.control_home,
                snapshot.home_epoch,
                snapshot.goal.tenant_id,
                snapshot.goal.conversation_id,
                snapshot.goal.updated_at,
            );
            for draft in adapt_standing_goal(context, &snapshot.goal)
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?
            {
                report.projected_facts += usize::from(
                    self.catalog.apply_catalog_fact(draft).await?
                        != CatalogProjectionApplyDisposition::Duplicate,
                );
            }
            if let Some(pursuit) = snapshot.pursuit {
                for draft in adapt_goal_pursuit(context, &pursuit)
                    .map_err(|_| ConversationCatalogProjectorError::Corrupt)?
                {
                    report.projected_facts += usize::from(
                        self.catalog.apply_catalog_fact(draft).await?
                            != CatalogProjectionApplyDisposition::Duplicate,
                    );
                }
            }
        }
        let evaluation_snapshots = self
            .catalog
            .list_goal_evaluation_projection_snapshots(self.authority_limit)
            .await?;
        let evaluation_page_exhausted =
            evaluation_snapshots.len() < usize::from(self.authority_limit);
        for snapshot in evaluation_snapshots {
            let context = goal_context(
                snapshot.control_home,
                snapshot.home_epoch,
                snapshot.evaluation.tenant_id,
                snapshot.conversation_id,
                snapshot.evaluation.occurred_at,
            );
            for draft in adapt_goal_evaluation(context, &snapshot.evaluation)
                .map_err(|_| ConversationCatalogProjectorError::Corrupt)?
            {
                report.projected_facts += usize::from(
                    self.catalog.apply_catalog_fact(draft).await?
                        != CatalogProjectionApplyDisposition::Duplicate,
                );
            }
        }
        // Do not establish an upgrade baseline while bounded auxiliary
        // sources may still contain another page of historical facts. The
        // durable store independently proves each Ledger checkpoint is at its
        // current stream head before releasing a Conversation.
        if goal_page_exhausted && evaluation_page_exhausted {
            report.finalized_bootstraps = self
                .catalog
                .finalize_bootstrap_receipts(self.authority_limit, now)
                .await?;
        }
        Ok(report)
    }

    async fn project_authority(
        &self,
        authority: CatalogProjectionAuthority,
        now: UtcTimestamp,
    ) -> Result<(usize, bool), ConversationCatalogProjectorError> {
        let page = self
            .records
            .list_catalog_projection_records(
                authority.actor,
                LedgerCursor {
                    conversation_id: authority.conversation_id,
                    home_epoch: authority.home_epoch,
                    after_conversation_seq: authority.after,
                },
                self.record_page_size,
            )
            .await
            .map_err(map_record_error)?;
        let page_through = page.through;
        let mut projected = 0;
        let mut through = authority.after;
        for record in page.records {
            let metadata = record.metadata;
            let Some(event) = record.event else {
                through = LedgerOffset::from(metadata.conversation_seq);
                continue;
            };
            let event_context = CatalogEventContext {
                control_home: authority.control_home,
                home_epoch: authority.home_epoch,
                tenant_id: metadata.tenant_id,
                conversation_id: metadata.conversation_id,
                record_id: metadata.record_id,
                audience: audience(
                    &event,
                    metadata.participant_id,
                    authority.actor.principal_id,
                ),
                occurred_at: metadata.occurred_at,
                committed_at: metadata.committed_at,
            };
            let drafts = self
                .adapt(
                    event_context,
                    metadata.run_id,
                    metadata.participant_id,
                    authority.actor.principal_id,
                    event,
                )
                .await?;
            for mut draft in drafts {
                // Only this validated canonical-record boundary supplies the
                // provenance. Goal snapshot adapters use semantic identities.
                draft.source_record_id = Some(metadata.record_id);
                projected += usize::from(
                    self.catalog.apply_catalog_fact(draft).await?
                        != CatalogProjectionApplyDisposition::Duplicate,
                );
            }
            through = LedgerOffset::from(metadata.conversation_seq);
        }
        if page_through < through {
            return Err(ConversationCatalogProjectorError::Corrupt);
        }
        through = page_through;
        let advanced = through > authority.after;
        if advanced {
            self.catalog
                .advance_projection_checkpoint(authority, through, now)
                .await?;
        }
        Ok((projected, advanced))
    }

    async fn adapt(
        &self,
        context: CatalogEventContext,
        run_id: Option<RunId>,
        participant_id: Option<ParticipantId>,
        actor_principal_id: PrincipalId,
        event: DomainEvent,
    ) -> Result<Vec<CatalogFactDraft>, ConversationCatalogProjectorError> {
        let result = match event {
            DomainEvent::Conversation(event) => {
                let context = if matches!(
                    event,
                    ConversationEvent::MessagePartiallyCommitted {
                        reason: PartialMessageReason::Cancelled,
                        ..
                    }
                ) {
                    let audience = if let Some(run_id) = run_id {
                        let run = self
                            .catalog
                            .resolve_run_authority(context.tenant_id, run_id)
                            .await?;
                        cancellation_audience(None, participant_id, &run)
                    } else {
                        participant_id.map_or(
                            CatalogAttentionAudience::AllExceptPrincipal(actor_principal_id),
                            CatalogAttentionAudience::AllExceptParticipant,
                        )
                    };
                    CatalogEventContext {
                        audience,
                        ..context
                    }
                } else {
                    context
                };
                adapt_conversation_event(context, &event)
            }
            DomainEvent::Activity(event) => adapt_activity_event(context, &event),
            DomainEvent::Artifact(event) => adapt_artifact_event(context, &event),
            DomainEvent::Interaction(event) => {
                let context = if matches!(
                    event,
                    musteragent_approval::InteractionEvent::Requested { .. }
                ) {
                    let run_id = run_id.ok_or(ConversationCatalogProjectorError::Corrupt)?;
                    let run = self
                        .catalog
                        .resolve_run_authority(context.tenant_id, run_id)
                        .await?;
                    CatalogEventContext {
                        // An interaction is attention for the principal that
                        // requested the run, not an unread broadcast to every
                        // participant in the conversation.
                        audience: CatalogAttentionAudience::Participant(
                            run.requested_by_participant_id,
                        ),
                        ..context
                    }
                } else {
                    context
                };
                adapt_interaction_event(context, &event)
            }
            DomainEvent::Run(event) => {
                let run_id = run_id.ok_or(ConversationCatalogProjectorError::Corrupt)?;
                let run = self
                    .catalog
                    .resolve_run_authority(context.tenant_id, run_id)
                    .await?;
                let audience = match &event {
                    musteragent_run::RunEvent::Cancelled { reason, .. } => {
                        cancellation_audience(Some(*reason), participant_id, &run)
                    }
                    _ => CatalogAttentionAudience::AllParticipants,
                };
                adapt_run_event(
                    CatalogRunProjectionContext {
                        event: CatalogEventContext {
                            audience,
                            ..context
                        },
                        run_id,
                        parent_run_id: run.parent_run_id,
                        delegation_id: run.delegation_id,
                        pursuit_id: run.pursuit_id,
                    },
                    &event,
                )
            }
            DomainEvent::Model(_)
            | DomainEvent::Tool(_)
            | DomainEvent::Context(_)
            | DomainEvent::Discovery(_) => Ok(Vec::new()),
        };
        result.map_err(|_| ConversationCatalogProjectorError::Corrupt)
    }
}

fn goal_context(
    control_home: RuntimeHome,
    home_epoch: HomeEpoch,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    occurred_at: UtcTimestamp,
) -> CatalogEventContext {
    CatalogEventContext {
        control_home,
        home_epoch,
        tenant_id,
        conversation_id,
        // Goal sources have their own stable identity and revision; this
        // generated Ledger identity is intentionally not part of their key.
        record_id: musteragent_identity::LedgerRecordId::generate(),
        audience: CatalogAttentionAudience::AllParticipants,
        occurred_at,
        committed_at: occurred_at,
    }
}

fn audience(
    event: &DomainEvent,
    participant_id: Option<ParticipantId>,
    actor_principal_id: PrincipalId,
) -> CatalogAttentionAudience {
    match event {
        DomainEvent::Conversation(ConversationEvent::Created { .. }) => participant_id.map_or(
            CatalogAttentionAudience::AllExceptPrincipal(actor_principal_id),
            CatalogAttentionAudience::AllExceptParticipant,
        ),
        DomainEvent::Conversation(
            ConversationEvent::MessageCommitted {
                role: MessageRole::User,
                ..
            }
            | ConversationEvent::MessageImported {
                role: MessageRole::User,
                ..
            },
        ) => participant_id.map_or(
            CatalogAttentionAudience::AllParticipants,
            CatalogAttentionAudience::AllExceptParticipant,
        ),
        _ => CatalogAttentionAudience::AllParticipants,
    }
}

fn is_actor_initiated_cancel(reason: CancelReason) -> bool {
    matches!(
        reason,
        CancelReason::UserRequested | CancelReason::ConversationArchived
    )
}

fn cancellation_audience(
    event_reason: Option<CancelReason>,
    participant_id: Option<ParticipantId>,
    run: &CatalogRunAuthority,
) -> CatalogAttentionAudience {
    if let Some(cancellation) = run.cancellation {
        if is_actor_initiated_cancel(cancellation.reason) {
            return CatalogAttentionAudience::AllExceptPrincipal(cancellation.requested_by);
        }
        return CatalogAttentionAudience::AllParticipants;
    }
    if event_reason.is_none_or(is_actor_initiated_cancel) {
        return participant_id.map_or(
            CatalogAttentionAudience::AllExceptParticipant(run.requested_by_participant_id),
            CatalogAttentionAudience::AllExceptParticipant,
        );
    }
    CatalogAttentionAudience::AllParticipants
}

fn map_record_error(error: SubscriptionError) -> ConversationCatalogProjectorError {
    match error {
        SubscriptionError::CursorEpochStale { .. } | SubscriptionError::CursorCompacted { .. } => {
            ConversationCatalogProjectorError::AuthorityStale
        }
        SubscriptionError::CorruptRecord => ConversationCatalogProjectorError::Corrupt,
        SubscriptionError::SourceUnavailable
        | SubscriptionError::ChangeFeedUnavailable
        | SubscriptionError::Unauthorized
        | SubscriptionError::InvalidQuery
        | SubscriptionError::ReadBudgetExceeded => ConversationCatalogProjectorError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use musteragent_content_ref::Sha256;
    use musteragent_conversation::ConversationKind;
    use musteragent_event_envelope::{LedgerRecordKind, Producer};
    use musteragent_identity::{
        ConversationSeq, CorrelationId, DeviceId, DottedKey, IdempotencyKey, LedgerRecordId,
        PrincipalId, PrincipalKind, ProjectionSnapshotId,
    };
    use musteragent_runtime_contract::CursorRecoveryAuthority;

    use super::*;

    struct IsolatingSource {
        stale: ConversationId,
        healthy: ConversationId,
        tenant_id: TenantId,
        participant_id: ParticipantId,
        home_epoch: HomeEpoch,
        device_id: DeviceId,
    }

    impl ConversationCatalogRecordSource for IsolatingSource {
        fn list_catalog_projection_records(
            &self,
            _actor: Actor,
            cursor: LedgerCursor,
            _limit: usize,
        ) -> ApplicationFuture<'_, Result<CatalogProjectionRecordPage, SubscriptionError>> {
            Box::pin(async move {
                if cursor.conversation_id == self.stale {
                    return Err(SubscriptionError::CursorEpochStale {
                        authority: CursorRecoveryAuthority {
                            snapshot_id: ProjectionSnapshotId::generate(),
                            resume_cursor: cursor,
                        },
                    });
                }
                assert_eq!(cursor.conversation_id, self.healthy);
                Ok(CatalogProjectionRecordPage {
                    records: vec![CatalogProjectionRecord {
                        metadata: LedgerMetadata {
                            schema_version: DottedKey::new("musteragent.runtime.record.v1")
                                .expect("record schema"),
                            record_id: LedgerRecordId::generate(),
                            record_kind: LedgerRecordKind::DomainEvent,
                            tenant_id: self.tenant_id,
                            conversation_id: self.healthy,
                            conversation_seq: ConversationSeq::new(1)
                                .expect("Conversation sequence"),
                            activity_id: None,
                            run_id: None,
                            run_seq: None,
                            attempt_id: None,
                            participant_id: Some(self.participant_id),
                            home_epoch: self.home_epoch,
                            producer: Producer::RuntimeApi,
                            occurred_at: UtcTimestamp::from_unix_millis(1_000),
                            committed_at: UtcTimestamp::from_unix_millis(1_001),
                            causation_id: None,
                            correlation_id: CorrelationId::generate(),
                            idempotency_key: IdempotencyKey::new("healthy-created")
                                .expect("idempotency key"),
                            payload_hash: Sha256::digest(b"healthy-created"),
                        },
                        event: Some(DomainEvent::Conversation(ConversationEvent::Created {
                            kind: ConversationKind::Direct,
                            control_home: RuntimeHome::Device {
                                device_id: self.device_id,
                            },
                            workspace_id: None,
                        })),
                    }],
                    through: LedgerOffset::new(1),
                })
            })
        }
    }

    struct RecordingCatalog {
        authorities: Vec<CatalogProjectionAuthority>,
        applied: Mutex<Vec<ConversationId>>,
        advanced: Mutex<Vec<ConversationId>>,
    }

    impl ConversationCatalogProjectionPort for RecordingCatalog {
        fn list_projection_authorities(
            &self,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<Vec<CatalogProjectionAuthority>, ConversationCatalogProjectorError>,
        > {
            let authorities = self.authorities.clone();
            Box::pin(async move { Ok(authorities) })
        }

        fn resolve_run_authority(
            &self,
            _tenant_id: TenantId,
            _run_id: RunId,
        ) -> ApplicationFuture<'_, Result<CatalogRunAuthority, ConversationCatalogProjectorError>>
        {
            Box::pin(async { Err(ConversationCatalogProjectorError::Corrupt) })
        }

        fn list_goal_projection_snapshots(
            &self,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<Vec<CatalogGoalProjectionSnapshot>, ConversationCatalogProjectorError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn list_goal_evaluation_projection_snapshots(
            &self,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<Vec<CatalogGoalEvaluationProjectionSnapshot>, ConversationCatalogProjectorError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn apply_catalog_fact(
            &self,
            draft: CatalogFactDraft,
        ) -> ApplicationFuture<
            '_,
            Result<CatalogProjectionApplyDisposition, ConversationCatalogProjectorError>,
        > {
            self.applied
                .lock()
                .expect("applied lock")
                .push(draft.conversation_id);
            Box::pin(async { Ok(CatalogProjectionApplyDisposition::Applied) })
        }

        fn advance_projection_checkpoint(
            &self,
            authority: CatalogProjectionAuthority,
            _through: LedgerOffset,
            _projected_at: UtcTimestamp,
        ) -> ApplicationFuture<'_, Result<(), ConversationCatalogProjectorError>> {
            self.advanced
                .lock()
                .expect("advanced lock")
                .push(authority.conversation_id);
            Box::pin(async { Ok(()) })
        }

        fn finalize_bootstrap_receipts(
            &self,
            _limit: u16,
            _finalized_at: UtcTimestamp,
        ) -> ApplicationFuture<'_, Result<usize, ConversationCatalogProjectorError>> {
            Box::pin(async { Ok(0) })
        }
    }

    #[tokio::test]
    async fn one_stale_authority_does_not_block_a_healthy_authority() {
        let tenant_id = TenantId::generate();
        let principal_id = PrincipalId::generate();
        let participant_id = ParticipantId::generate();
        let stale = ConversationId::generate();
        let healthy = ConversationId::generate();
        let device_id = DeviceId::generate();
        let home_epoch = HomeEpoch::new(1).expect("home epoch");
        let actor = Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        };
        let authority = |conversation_id| CatalogProjectionAuthority {
            actor,
            conversation_id,
            control_home: RuntimeHome::Device { device_id },
            home_epoch,
            after: LedgerOffset::ZERO,
        };
        let source = Arc::new(IsolatingSource {
            stale,
            healthy,
            tenant_id,
            participant_id,
            home_epoch,
            device_id,
        });
        let catalog = Arc::new(RecordingCatalog {
            authorities: vec![authority(stale), authority(healthy)],
            applied: Mutex::new(Vec::new()),
            advanced: Mutex::new(Vec::new()),
        });
        let projector = ConversationCatalogLedgerProjector::new(10, 10, source, catalog.clone())
            .expect("Catalog projector");

        let report = projector
            .run_pass(UtcTimestamp::from_unix_millis(2_000))
            .await
            .expect("bounded pass");

        assert_eq!(report.projected_facts, 1);
        assert_eq!(report.advanced_authorities, 1);
        assert_eq!(
            report.deferred,
            vec![CatalogProjectionDeferral {
                authority: authority(stale),
                error: ConversationCatalogProjectorError::AuthorityStale,
            }]
        );
        assert_eq!(
            *catalog.applied.lock().expect("applied lock"),
            vec![healthy]
        );
        assert_eq!(
            *catalog.advanced.lock().expect("advanced lock"),
            vec![healthy]
        );
    }

    struct StaticRecords(Vec<CatalogProjectionRecord>);

    impl ConversationCatalogRecordSource for StaticRecords {
        fn list_catalog_projection_records(
            &self,
            _actor: Actor,
            _cursor: LedgerCursor,
            _limit: usize,
        ) -> ApplicationFuture<'_, Result<CatalogProjectionRecordPage, SubscriptionError>> {
            let records = self.0.clone();
            let through = records.last().map_or(LedgerOffset::ZERO, |item| {
                LedgerOffset::from(item.metadata.conversation_seq)
            });
            Box::pin(async move { Ok(CatalogProjectionRecordPage { records, through }) })
        }
    }

    struct CapturingCatalog {
        authorities: Vec<CatalogProjectionAuthority>,
        run_authority: Option<CatalogRunAuthority>,
        drafts: Mutex<Vec<CatalogFactDraft>>,
    }

    impl ConversationCatalogProjectionPort for CapturingCatalog {
        fn list_projection_authorities(
            &self,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<Vec<CatalogProjectionAuthority>, ConversationCatalogProjectorError>,
        > {
            let authorities = self.authorities.clone();
            Box::pin(async move { Ok(authorities) })
        }

        fn resolve_run_authority(
            &self,
            _tenant_id: TenantId,
            _run_id: RunId,
        ) -> ApplicationFuture<'_, Result<CatalogRunAuthority, ConversationCatalogProjectorError>>
        {
            let result = self
                .run_authority
                .ok_or(ConversationCatalogProjectorError::Corrupt);
            Box::pin(async move { result })
        }

        fn list_goal_projection_snapshots(
            &self,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<Vec<CatalogGoalProjectionSnapshot>, ConversationCatalogProjectorError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn list_goal_evaluation_projection_snapshots(
            &self,
            _limit: u16,
        ) -> ApplicationFuture<
            '_,
            Result<Vec<CatalogGoalEvaluationProjectionSnapshot>, ConversationCatalogProjectorError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn apply_catalog_fact(
            &self,
            draft: CatalogFactDraft,
        ) -> ApplicationFuture<
            '_,
            Result<CatalogProjectionApplyDisposition, ConversationCatalogProjectorError>,
        > {
            self.drafts.lock().expect("drafts lock").push(draft);
            Box::pin(async { Ok(CatalogProjectionApplyDisposition::Applied) })
        }

        fn advance_projection_checkpoint(
            &self,
            _authority: CatalogProjectionAuthority,
            _through: LedgerOffset,
            _projected_at: UtcTimestamp,
        ) -> ApplicationFuture<'_, Result<(), ConversationCatalogProjectorError>> {
            Box::pin(async { Ok(()) })
        }

        fn finalize_bootstrap_receipts(
            &self,
            _limit: u16,
            _finalized_at: UtcTimestamp,
        ) -> ApplicationFuture<'_, Result<usize, ConversationCatalogProjectorError>> {
            Box::pin(async { Ok(0) })
        }
    }

    fn record_fixture(
        tenant_id: TenantId,
        conversation_id: ConversationId,
        home_epoch: HomeEpoch,
        seq: u64,
        run_id: Option<RunId>,
        participant_id: Option<ParticipantId>,
        event: DomainEvent,
    ) -> CatalogProjectionRecord {
        CatalogProjectionRecord {
            metadata: LedgerMetadata {
                schema_version: DottedKey::new("musteragent.runtime.record.v1")
                    .expect("record schema"),
                record_id: LedgerRecordId::generate(),
                record_kind: LedgerRecordKind::DomainEvent,
                tenant_id,
                conversation_id,
                conversation_seq: ConversationSeq::new(seq).expect("seq"),
                activity_id: None,
                run_id,
                run_seq: run_id.map(|_| musteragent_identity::RunSeq::new(seq).expect("run seq")),
                attempt_id: None,
                participant_id,
                home_epoch,
                producer: Producer::RuntimeApi,
                occurred_at: UtcTimestamp::from_unix_millis(1_000 + seq as i64),
                committed_at: UtcTimestamp::from_unix_millis(1_010 + seq as i64),
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                idempotency_key: IdempotencyKey::new(format!("rec-{seq}"))
                    .expect("idempotency key"),
                payload_hash: Sha256::digest(format!("rec-{seq}").as_bytes()),
            },
            event: Some(event),
        }
    }

    #[tokio::test]
    async fn created_without_envelope_participant_falls_back_to_actor_principal() {
        let tenant_id = TenantId::generate();
        let creator = PrincipalId::generate();
        let conversation_id = ConversationId::generate();
        let home_epoch = HomeEpoch::new(1).expect("home epoch");
        let authority = CatalogProjectionAuthority {
            actor: Actor {
                tenant_id,
                principal_id: creator,
                principal_kind: PrincipalKind::User,
            },
            conversation_id,
            control_home: RuntimeHome::Cloud,
            home_epoch,
            after: LedgerOffset::ZERO,
        };
        let source = Arc::new(StaticRecords(vec![record_fixture(
            tenant_id,
            conversation_id,
            home_epoch,
            1,
            None,
            None,
            DomainEvent::Conversation(ConversationEvent::Created {
                kind: ConversationKind::Direct,
                control_home: RuntimeHome::Cloud,
                workspace_id: None,
            }),
        )]));
        let catalog = Arc::new(CapturingCatalog {
            authorities: vec![authority],
            run_authority: None,
            drafts: Mutex::new(Vec::new()),
        });
        let projector =
            ConversationCatalogLedgerProjector::new(10, 10, source, catalog.clone()).unwrap();
        projector
            .run_pass(UtcTimestamp::from_unix_millis(2_000))
            .await
            .expect("run pass");

        let drafts = catalog.drafts.lock().expect("drafts");
        assert_eq!(drafts.len(), 1);
        assert_eq!(drafts[0].kind, crate::CatalogFactKind::ConversationCreated);
        assert_eq!(
            drafts[0].audience,
            CatalogAttentionAudience::AllExceptPrincipal(creator)
        );
    }

    #[tokio::test]
    async fn user_and_archive_cancellations_exclude_requester_while_other_reasons_broadcast() {
        let tenant_id = TenantId::generate();
        let actor_principal = PrincipalId::generate();
        let cancelling_principal = PrincipalId::generate();
        let run_requester_participant = ParticipantId::generate();
        let conversation_id = ConversationId::generate();
        let run_id = RunId::generate();
        let home_epoch = HomeEpoch::new(1).expect("home epoch");
        let content_ref = musteragent_content_ref::ContentRef {
            content_id: musteragent_identity::ContentId::generate(),
            media_type: musteragent_content_ref::MediaType::new("text/plain").expect("media"),
            byte_length: 5,
            sha256: Sha256::digest(b"hello"),
        };
        let authority = CatalogProjectionAuthority {
            actor: Actor {
                tenant_id,
                principal_id: actor_principal,
                principal_kind: PrincipalKind::User,
            },
            conversation_id,
            control_home: RuntimeHome::Cloud,
            home_epoch,
            after: LedgerOffset::ZERO,
        };

        for reason in [
            CancelReason::UserRequested,
            CancelReason::ConversationArchived,
        ] {
            let source = Arc::new(StaticRecords(vec![
                record_fixture(
                    tenant_id,
                    conversation_id,
                    home_epoch,
                    1,
                    Some(run_id),
                    None,
                    DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
                        message_id: musteragent_identity::MessageId::generate(),
                        content_ref: content_ref.clone(),
                        content_hash: content_ref.sha256,
                        content_bytes: content_ref.byte_length,
                        reason: PartialMessageReason::Cancelled,
                    }),
                ),
                record_fixture(
                    tenant_id,
                    conversation_id,
                    home_epoch,
                    2,
                    Some(run_id),
                    None,
                    DomainEvent::Run(musteragent_run::RunEvent::Cancelled {
                        reason,
                        last_attempt_id: None,
                    }),
                ),
            ]));
            let catalog = Arc::new(CapturingCatalog {
                authorities: vec![authority],
                run_authority: Some(CatalogRunAuthority {
                    parent_run_id: None,
                    delegation_id: None,
                    pursuit_id: None,
                    requested_by_participant_id: run_requester_participant,
                    cancellation: Some(CatalogRunCancellationAuthority {
                        reason,
                        requested_by: cancelling_principal,
                    }),
                }),
                drafts: Mutex::new(Vec::new()),
            });
            let projector =
                ConversationCatalogLedgerProjector::new(10, 10, source, catalog.clone()).unwrap();
            projector
                .run_pass(UtcTimestamp::from_unix_millis(3_000))
                .await
                .expect("run pass");

            let drafts = catalog.drafts.lock().expect("drafts");
            assert_eq!(drafts.len(), 3);
            assert_eq!(drafts[0].kind, crate::CatalogFactKind::VisibleOutput);
            assert_eq!(
                drafts[0].audience,
                CatalogAttentionAudience::AllExceptPrincipal(cancelling_principal)
            );
            assert!(matches!(
                drafts[1].kind,
                crate::CatalogFactKind::ExecutionStateChanged {
                    status: crate::CatalogExecutionStatus::Cancelled,
                    visible_outcome: Some(crate::ExecutionOutcome {
                        kind: crate::ExecutionOutcomeKind::Cancelled,
                        ..
                    }),
                    ..
                }
            ));
            assert_eq!(
                drafts[1].audience,
                CatalogAttentionAudience::AllExceptPrincipal(cancelling_principal)
            );
            assert!(matches!(
                drafts[2].kind,
                crate::CatalogFactKind::ActionClosed { .. }
            ));
        }

        // Non-actor cancellation reasons and interrupted partial messages remain broadcast.
        for reason in [
            CancelReason::ParentCancelled,
            CancelReason::PolicyRevoked,
            CancelReason::SystemShutdown,
            CancelReason::CoordinationInterrupted,
        ] {
            let source = Arc::new(StaticRecords(vec![
                record_fixture(
                    tenant_id,
                    conversation_id,
                    home_epoch,
                    1,
                    Some(run_id),
                    None,
                    DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
                        message_id: musteragent_identity::MessageId::generate(),
                        content_ref: content_ref.clone(),
                        content_hash: content_ref.sha256,
                        content_bytes: content_ref.byte_length,
                        reason: PartialMessageReason::Interrupted,
                    }),
                ),
                record_fixture(
                    tenant_id,
                    conversation_id,
                    home_epoch,
                    2,
                    Some(run_id),
                    None,
                    DomainEvent::Run(musteragent_run::RunEvent::Cancelled {
                        reason,
                        last_attempt_id: None,
                    }),
                ),
            ]));
            let catalog = Arc::new(CapturingCatalog {
                authorities: vec![authority],
                run_authority: Some(CatalogRunAuthority {
                    parent_run_id: None,
                    delegation_id: None,
                    pursuit_id: None,
                    requested_by_participant_id: run_requester_participant,
                    cancellation: Some(CatalogRunCancellationAuthority {
                        reason,
                        requested_by: cancelling_principal,
                    }),
                }),
                drafts: Mutex::new(Vec::new()),
            });
            let projector =
                ConversationCatalogLedgerProjector::new(10, 10, source, catalog.clone()).unwrap();
            projector
                .run_pass(UtcTimestamp::from_unix_millis(3_000))
                .await
                .expect("run pass");

            let drafts = catalog.drafts.lock().expect("drafts");
            assert_eq!(
                drafts[0].audience,
                CatalogAttentionAudience::AllParticipants
            );
            assert_eq!(
                drafts[1].audience,
                CatalogAttentionAudience::AllParticipants
            );
        }
    }
}
