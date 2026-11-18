//! Frozen, accepted Dialogue inputs. This port does not accept ContentIds or
//! upload paths; the original dialogue owner resolves the exact accepted entry.
use crate::{FileQueryContext, FileQueryError, ResolvedFile};
use musteragent_content_ref::ContentRef;
use musteragent_execution_context::DialogueContextCut;
use musteragent_identity::{Actor, AgentId, RunId, WorkItemId};
use musteragent_runtime_contract::ApplicationFuture;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogueInputView {
    pub cut: DialogueContextCut,
    pub delegation: Option<DialogueInputDelegation>,
    /// The source owner checks the entire frozen view, not just the first page.
    pub has_inputs: bool,
}
impl DialogueInputView {
    pub fn validate(&self, actor: Actor, agent: AgentId) -> Result<(), FileQueryError> {
        self.cut.validate().map_err(|_| FileQueryError::Integrity)?;
        if self.cut.tenant_id != actor.tenant_id
            || self.cut.owner_id != actor.principal_id
            || self.cut.through.seq == 0
        {
            return Err(FileQueryError::Denied);
        }
        if let Some(proof) = &self.delegation {
            if proof.executing_agent_id != agent
                || proof.run_id == proof.source_run_id
                || proof.input_refs.len() > 64
                || proof
                    .input_refs
                    .iter()
                    .map(|reference| reference.content_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != proof.input_refs.len()
            {
                return Err(FileQueryError::Denied);
            }
        } else if self.cut.agent_id != agent {
            return Err(FileQueryError::Denied);
        }
        Ok(())
    }
    pub fn permits(&self, reference: &ContentRef) -> bool {
        self.delegation
            .as_ref()
            .is_none_or(|proof| proof.input_refs.contains(reference))
    }
}
/// A frozen source cut alone never transfers another Agent's authority. The
/// storage owner revalidates this exact current Task session and byte ceiling
/// for every query and resolves files through the original accepted-input owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogueInputDelegation {
    pub work_item_id: WorkItemId,
    pub run_id: RunId,
    pub source_run_id: RunId,
    pub executing_agent_id: AgentId,
    pub input_refs: Vec<ContentRef>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DialogueInputPosition {
    pub seq: u64,
    pub ordinal: u32,
}
pub struct DialogueInputPage {
    pub files: Vec<ResolvedFile>,
    /// Progress can cross a page containing only non-input messages.
    pub next: Option<DialogueInputPosition>,
}
pub trait DialogueInputSource: Send + Sync {
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, FileQueryError>>;
    fn resolve<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a crate::FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            reference.validate(context)?;
            let file = self
                .resolve_identity(context, &reference.identity())
                .await?;
            if file.entry.file_ref != *reference {
                return Err(FileQueryError::Changed);
            }
            Ok(file)
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a crate::FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>>;
}
