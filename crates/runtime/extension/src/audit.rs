//! Typed append-only extension facts and a Ledger/Outbox adapter.
use crate::{ExtensionContext, ExtensionError, Hook, HookFailure, LoopDetection};
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerRecordKind, Producer};
use musteragent_identity::{
    AttemptId, CorrelationId, DottedKey, IdempotencyKey, LedgerOffset, LedgerRecordId,
    OutboxMessageId, RunId,
};
use musteragent_ledger::{
    AppendRequest, Clock, LedgerError, LedgerStore, ProposedOutbox, ProposedRecord,
};
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Dedicated schema. Consumers decode it with `decode_extension_fact`, never
/// reinterpret it as a kernel domain event.
pub const EXTENSION_FACT_SCHEMA: &str = "musteragent.extension.fact.v1";
/// Outbox topic atomically published with every extension fact.
pub const EXTENSION_OUTBOX_TOPIC: &str = "extension.fact";

/// Auditable facts. Original results/arguments are never replaced in storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExtensionFact {
    /// Hook error or panic, with explicit continuation policy.
    HookFailed {
        /// Typed hook identity and failure.
        failure: HookFailure,
        /// Whether execution continued after recording this failure.
        continues: bool,
    },
    /// One compositional argument rewrite.
    ArgumentsRewritten {
        /// Registered owner of this rewrite.
        extension: String,
        /// Original tool call identity.
        call_id: String,
        /// Digest and length of the arguments received by this extension.
        before: crate::PayloadDigest,
        /// Digest and length of the replacement arguments.
        after: crate::PayloadDigest,
        /// Exact replacement in the existing tenant content store.
        content: crate::RewriteContent,
    },
    /// One memory middleware rewrite, before invoking the memory owner.
    MemoryRewritten {
        /// Requesting extension identity.
        extension: String,
        /// Registration-order wrapper index.
        wrapper_index: usize,
        /// Digest and length of the request received by this wrapper.
        before: crate::PayloadDigest,
        /// Digest and length of the replacement request.
        after: crate::PayloadDigest,
        /// Exact replacement in the existing tenant content store.
        content: crate::RewriteContent,
    },
    /// Typed denial. All denying extensions are retained.
    ToolDenied {
        /// Denying owner.
        extension: String,
        /// Rejected tool call.
        call_id: String,
        /// Explicit safe denial code and explanation.
        reason: crate::HookError,
    },
    /// Warning or terminal loop decision.
    LoopDetected {
        /// Pattern, level, count and triggering call.
        detection: LoopDetection,
    },
    /// Ordered lifecycle notification was accepted by an extension.
    Lifecycle {
        /// Observer identity.
        extension: String,
        /// Delivered lifecycle hook.
        hook: Hook,
        /// Child identity for delegation notifications.
        #[serde(with = "optional_id_codec")]
        child_run_id: Option<RunId>,
    },
    /// Command dispatch consumed user input before model projection.
    CommandHandled {
        /// Registered command name.
        command: String,
        /// Original input digest detects conflicting identity reuse.
        input_digest: String,
        /// Receipt shown in the model context.
        response: String,
    },
    /// A model-visible projection was produced; separate from raw facts.
    ContextProjected {
        /// Physical invocation identity.
        invocation_id: String,
        /// Content digest of the projected instructions/messages.
        digest: String,
        /// Number of system instruction sections.
        instruction_count: usize,
        /// Number of context messages.
        message_count: usize,
    },
    /// Outbound header names only; values may carry secrets and are not recorded.
    RequestAnnotated {
        /// Physical invocation identity.
        invocation_id: String,
        /// Validated header names.
        header_names: Vec<String>,
    },
    /// Service successfully crossed its boundary.
    Service {
        /// Registered service identity.
        service: String,
        /// Start or stop boundary.
        hook: Hook,
    },
}

/// Scope and idempotency of a fact. Attempt identity isolates loop histories.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionFactRecord {
    /// Admitted run.
    #[serde(with = "id_codec")]
    pub run_id: RunId,
    /// Original attempt.
    #[serde(with = "id_codec")]
    pub attempt_id: AttemptId,
    /// Stable semantic key within the attempt.
    pub key: String,
    /// Typed payload.
    pub fact: ExtensionFact,
}

/// Indexed selection; implementations must not scan unrelated attempt facts.
#[derive(Debug, Clone)]
pub enum FactQuery {
    /// Reporting only, outside the per-tool path.
    All,
    /// One semantic key.
    Key(String),
    /// Interventions associated with one tool call, in registration order.
    Call(String),
    /// Bounded batch of call identities, for the detector's history view.
    Calls(Vec<String>),
    /// The latest three warnings and any terminal decision.
    LoopState,
}

/// Persistence port. Implementations must atomically append, reject conflicting
/// reuse of a key, enforce the supplied authority/fence, and return ordered facts.
/// A rewrite fact must retain its replacement through the content owner's
/// canonical reference mechanism when that owner supports garbage collection.
/// No update/delete operation exists. Production hosts implement this port in
/// their existing fenced transaction owner.
pub trait ExtensionJournal: Send + Sync {
    /// Append before any associated dispatch; duplicate identical writes are safe.
    fn append<'a>(
        &'a self,
        context: &'a ExtensionContext,
        key: &'a str,
        fact: ExtensionFact,
    ) -> ApplicationFuture<'a, Result<(), ExtensionError>>;
    /// Read this attempt's ordered facts, including those from an earlier process.
    fn read<'a>(
        &'a self,
        context: &'a ExtensionContext,
    ) -> ApplicationFuture<'a, Result<Vec<ExtensionFactRecord>, ExtensionError>> {
        self.select(context, FactQuery::All)
    }
    /// Read through the journal's key/call/decision index.
    fn select<'a>(
        &'a self,
        context: &'a ExtensionContext,
        query: FactQuery,
    ) -> ApplicationFuture<'a, Result<Vec<ExtensionFactRecord>, ExtensionError>>;
}

/// Reference LedgerStore adapter for a host-owned conversation append lane.
/// LedgerStore enforces epoch/offset/idempotency, but is not itself a worker fence.
/// Use a fenced `ExtensionJournal` implementation in a multi-worker deployment.
/// Extension records are conversation-level facts; run/attempt are in the payload,
/// so this adapter never allocates or interferes with the run owner's sequence.
pub struct LedgerExtensionJournal<S> {
    ledger: Arc<S>,
    clock: Arc<dyn Clock>,
    index: tokio::sync::Mutex<crate::audit_index::LedgerIndex>,
}
impl<S> LedgerExtensionJournal<S> {
    /// Wrap an existing append-only ledger and its clock.
    pub fn new(ledger: Arc<S>, clock: Arc<dyn Clock>) -> Self {
        Self {
            ledger,
            clock,
            index: Default::default(),
        }
    }
}
impl<S: LedgerStore<Error = LedgerError>> ExtensionJournal for LedgerExtensionJournal<S> {
    fn append<'a>(
        &'a self,
        context: &'a ExtensionContext,
        key: &'a str,
        fact: ExtensionFact,
    ) -> ApplicationFuture<'a, Result<(), ExtensionError>> {
        Box::pin(async move {
            if context.run.run_id != context.attempt.run_id {
                return Err(ExtensionError::IdentityMismatch);
            }
            let record = ExtensionFactRecord {
                run_id: context.run.run_id,
                attempt_id: context.attempt.attempt_id,
                key: key.into(),
                fact,
            };
            let payload = serde_json::to_vec(&record).map_err(audit_error)?;
            let hash = Sha256::digest(&payload);
            let identity = Sha256::digest(format!(
                "{}:{}:{key}",
                context.run.conversation_id, context.attempt.attempt_id
            ));
            let idempotency_key =
                IdempotencyKey::new(format!("extension:{identity}")).map_err(audit_error)?;
            let occurred_at = self.clock.now();
            let proposed = ProposedRecord {
                schema_version: DottedKey::new(EXTENSION_FACT_SCHEMA).map_err(audit_error)?,
                record_id: LedgerRecordId::generate(),
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: None,
                run_id: None,
                run_seq: None,
                attempt_id: None,
                participant_id: None,
                producer: Producer::RuntimeApi,
                occurred_at,
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload: payload.clone(),
                payload_hash: hash,
            };
            let outbox = ProposedOutbox {
                message_id: OutboxMessageId::generate(),
                topic: DottedKey::new(EXTENSION_OUTBOX_TOPIC).map_err(audit_error)?,
                payload,
                payload_hash: hash,
            };
            // Bounded optimistic retries, never a blind append or overwrite.
            for _ in 0..16 {
                let offset = match self
                    .ledger
                    .current_offset(context.run.conversation_id)
                    .await
                {
                    Ok(offset) => offset,
                    Err(LedgerError::ConversationNotFound) => LedgerOffset::ZERO,
                    Err(error) => return Err(audit_error(error)),
                };
                match self
                    .ledger
                    .append(AppendRequest {
                        tenant_id: context.run.tenant_id,
                        conversation_id: context.run.conversation_id,
                        expected_home_epoch: context.run.home_epoch,
                        expected_offset: offset,
                        idempotency_key: idempotency_key.clone(),
                        command_fingerprint: hash,
                        records: vec![proposed.clone()],
                        outbox: vec![outbox.clone()],
                    })
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(LedgerError::SequenceConflict { .. }) => continue,
                    Err(error) => return Err(audit_error(error)),
                }
            }
            Err(ExtensionError::Audit(
                "append contention budget exhausted".into(),
            ))
        })
    }
    fn select<'a>(
        &'a self,
        context: &'a ExtensionContext,
        query: FactQuery,
    ) -> ApplicationFuture<'a, Result<Vec<ExtensionFactRecord>, ExtensionError>> {
        Box::pin(async move {
            let mut index = self.index.lock().await;
            index.refresh(self.ledger.as_ref(), context).await?;
            Ok(index.select(context, query))
        })
    }
}
/// Decode the dedicated extension schema, including outbox payloads.
pub fn decode_extension_fact(bytes: &[u8]) -> Result<ExtensionFactRecord, ExtensionError> {
    serde_json::from_slice(bytes).map_err(audit_error)
}
fn audit_error(error: impl std::fmt::Display) -> ExtensionError {
    ExtensionError::Audit(error.to_string())
}

mod id_codec {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<T: std::fmt::Display, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, T: std::str::FromStr, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error>
    where
        T::Err: std::fmt::Display,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
mod optional_id_codec {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<T: std::fmt::Display, S: Serializer>(
        value: &Option<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(ToString::to_string)
            .serialize(serializer)
    }
    pub fn deserialize<'de, T: std::str::FromStr, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<T>, D::Error>
    where
        T::Err: std::fmt::Display,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|s| s.parse().map_err(serde::de::Error::custom))
            .transpose()
    }
}
