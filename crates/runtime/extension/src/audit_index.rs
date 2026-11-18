//! Incremental reference-ledger projection. Old records are decoded once, not per call.
use crate::*;
use musteragent_event_envelope::LedgerCursor;
use musteragent_identity::{AttemptId, ConversationId, HomeEpoch, LedgerOffset, RunId, TenantId};
use musteragent_ledger::{LedgerError, LedgerStore};
use std::collections::BTreeMap;

type Scope = (TenantId, ConversationId, HomeEpoch);
#[derive(Default)]
pub(crate) struct LedgerIndex {
    conversations: BTreeMap<Scope, ConversationIndex>,
}
struct ConversationIndex {
    cursor: LedgerOffset,
    attempts: BTreeMap<(RunId, AttemptId), AttemptIndex>,
}
#[derive(Default)]
struct AttemptIndex {
    records: Vec<ExtensionFactRecord>,
    keys: BTreeMap<String, usize>,
    calls: BTreeMap<String, Vec<usize>>,
    warnings: Vec<usize>,
    terminal: Option<usize>,
}
impl AttemptIndex {
    fn insert(&mut self, record: ExtensionFactRecord) {
        let n = self.records.len();
        self.keys.insert(record.key.clone(), n);
        match &record.fact {
            ExtensionFact::ArgumentsRewritten { call_id, .. }
            | ExtensionFact::ToolDenied { call_id, .. } => {
                self.calls.entry(call_id.clone()).or_default().push(n);
            }
            ExtensionFact::LoopDetected { detection } if detection.level == LoopLevel::Warning => {
                self.warnings.push(n)
            }
            ExtensionFact::LoopDetected { .. } => self.terminal = Some(n),
            _ => {}
        }
        self.records.push(record);
    }
    fn select(&self, query: FactQuery) -> Vec<ExtensionFactRecord> {
        let indices = match query {
            FactQuery::All => return self.records.clone(),
            FactQuery::Key(key) => self.keys.get(&key).copied().into_iter().collect(),
            FactQuery::Call(call) => self.calls.get(&call).cloned().unwrap_or_default(),
            FactQuery::Calls(calls) => calls
                .into_iter()
                .flat_map(|call| self.calls.get(&call).cloned().unwrap_or_default())
                .collect(),
            FactQuery::LoopState => self
                .warnings
                .iter()
                .rev()
                .take(3)
                .copied()
                .chain(self.terminal)
                .collect(),
        };
        let mut indices: Vec<usize> = indices;
        indices.sort_unstable();
        indices
            .into_iter()
            .map(|i| self.records[i].clone())
            .collect()
    }
}
impl LedgerIndex {
    pub(crate) async fn refresh<S: LedgerStore<Error = LedgerError>>(
        &mut self,
        ledger: &S,
        context: &ExtensionContext,
    ) -> Result<(), ExtensionError> {
        let scope = (
            context.run.tenant_id,
            context.run.conversation_id,
            context.run.home_epoch,
        );
        let index = self.conversations.entry(scope).or_default();
        let offset = match ledger.current_offset(context.run.conversation_id).await {
            Ok(offset) => offset,
            Err(LedgerError::ConversationNotFound) => return Ok(()),
            Err(e) => return Err(ExtensionError::Audit(e.to_string())),
        };
        while index.cursor < offset {
            let page = ledger
                .load(
                    LedgerCursor {
                        conversation_id: context.run.conversation_id,
                        home_epoch: context.run.home_epoch,
                        after_conversation_seq: index.cursor,
                    },
                    1000,
                )
                .await
                .map_err(|e| ExtensionError::Audit(e.to_string()))?;
            for record in page.records {
                if record.metadata.tenant_id != context.run.tenant_id {
                    return Err(ExtensionError::IdentityMismatch);
                }
                if record.metadata.schema_version.as_str() == EXTENSION_FACT_SCHEMA {
                    let fact = decode_extension_fact(&record.payload)?;
                    index
                        .attempts
                        .entry((fact.run_id, fact.attempt_id))
                        .or_default()
                        .insert(fact);
                }
            }
            let next = page.next_cursor.after_conversation_seq;
            if next <= index.cursor {
                return Err(ExtensionError::Audit(
                    "ledger projection made no progress".into(),
                ));
            }
            index.cursor = next;
        }
        Ok(())
    }
    pub(crate) fn select(
        &self,
        context: &ExtensionContext,
        query: FactQuery,
    ) -> Vec<ExtensionFactRecord> {
        self.conversations
            .get(&(
                context.run.tenant_id,
                context.run.conversation_id,
                context.run.home_epoch,
            ))
            .and_then(|c| {
                c.attempts
                    .get(&(context.run.run_id, context.attempt.attempt_id))
            })
            .map(|a| a.select(query))
            .unwrap_or_default()
    }
}

impl Default for ConversationIndex {
    fn default() -> Self {
        Self {
            cursor: LedgerOffset::ZERO,
            attempts: BTreeMap::new(),
        }
    }
}
