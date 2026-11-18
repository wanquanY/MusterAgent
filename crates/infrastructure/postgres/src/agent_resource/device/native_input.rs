//! Native custody qualifies the original accepted-input catalog. It does not
//! create a Cloud invocation or expand the immutable Task delegation ceiling.
use super::{Error, authority::Authority};
use crate::agent_resource::device_custody::DeviceCustody;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use musteragent_runtime_resource::ResourceCommandContext;
use std::sync::Arc;

type QueryError = FileQueryError;
fn authority_error(error: Error) -> QueryError {
    match error {
        Error::Infrastructure => QueryError::Unavailable,
        Error::Corrupt => QueryError::Integrity,
        Error::Conflict => QueryError::Changed,
        _ => QueryError::Denied,
    }
}
fn source_error(error: musteragent_runtime_session::DialogueStoreError) -> QueryError {
    use musteragent_runtime_session::DialogueStoreError as E;
    match error {
        E::Infrastructure => QueryError::Unavailable,
        E::Corrupt => QueryError::Integrity,
        E::RevisionConflict => QueryError::Changed,
        _ => QueryError::Denied,
    }
}

impl Authority {
    /// Remote Task qualification finishes before this new DB snapshot. The
    /// exact custody and current permissions are rechecked on every read.
    pub(super) async fn input_snapshot(
        &self,
    ) -> Result<
        (
            crate::GovernedPostgresTransaction<'static>,
            ResourceCommandContext,
            DialogueInputView,
        ),
        QueryError,
    > {
        let (mut tx, context, custody) = self.current_snapshot().await.map_err(authority_error)?;
        let view = match &custody {
            DeviceCustody::Intent(source) => {
                crate::agent_file_query::accepted_intent_view(
                    &mut tx,
                    self.actor,
                    source.scope.agent_id,
                    &source.context_cut,
                    &source.intent,
                )
                .await?
            }
            DeviceCustody::WorkItem(source) => {
                let p = source.preparation();
                let (origin, intent, ceilings) =
                    crate::work_item_execution::work_item_accepted_input_source(
                        &mut tx,
                        self.actor,
                        p.delegation.source_run_id,
                        p.key.scope.agent_id,
                        p.work_item.source_intent_id,
                    )
                    .await
                    .map_err(source_error)?;
                let refs = &p.delegation.input_refs;
                let row = sqlx::query("SELECT i.* FROM agent.dialogue_user_intents i WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3")
                    .bind(intent.into_uuid()).bind(self.actor.tenant_id.into_uuid()).bind(self.actor.principal_id.into_uuid())
                    .fetch_optional(&mut *tx).await.map_err(|_| QueryError::Unavailable)?.ok_or(QueryError::Denied)?;
                let accepted = crate::dialogue_codec::intent_row(&row).map_err(source_error)?;
                if ceilings.iter().any(|ceiling| ceiling != refs)
                    || accepted
                        .input_attachments
                        .iter()
                        .map(|input| &input.attachment)
                        .ne(p.input_attachments.iter())
                {
                    return Err(QueryError::Integrity);
                }
                let mut view = crate::agent_file_query::direct_input_view(
                    &mut tx,
                    self.actor,
                    origin,
                    p.key.scope.agent_id,
                )
                .await?;
                view.has_inputs = !refs.is_empty();
                view.delegation = Some(DialogueInputDelegation {
                    work_item_id: p.key.work_item_id,
                    run_id: self.request.command().run_context().run_id,
                    source_run_id: origin,
                    executing_agent_id: p.key.executing_agent_id,
                    input_refs: refs.clone(),
                });
                view
            }
        };
        view.validate(self.actor, custody.agent_id())?;
        Ok((tx, context, view))
    }
}

pub(super) struct NativeInputSource(pub Arc<Authority>);
impl NativeInputSource {
    async fn current(
        &self,
        context: &FileQueryContext,
    ) -> Result<
        (
            crate::GovernedPostgresTransaction<'static>,
            DialogueInputView,
        ),
        QueryError,
    > {
        context.validate()?;
        let source = context.dialogue_source.as_ref().ok_or(QueryError::Denied)?;
        let command = self.0.request.command();
        if context.actor != self.0.actor
            || context.conversation_id != self.0.context.conversation_id()
            || source.invocation_id != command.invocation().invocation_id
            || source.tool_key.as_str() != command.invocation().tool_key.as_str()
            || !matches!(
                source.tool_key.as_str(),
                "device.workspace.file.import@1"
                    | "files.list@4"
                    | "files.list@5"
                    | "files.list@6"
                    | "files.inspect@6"
                    | "files.read@6"
                    | "files.search@5"
                    | "files.search@4"
                    | "files.inspect@4"
                    | "files.inspect@5"
                    | "files.read@5"
            )
        {
            return Err(QueryError::Denied);
        }
        let (tx, _, view) = self.0.input_snapshot().await?;
        view.validate(context.actor, source.agent_id)?;
        if source.input_view.as_ref() != Some(&view) {
            return Err(QueryError::Changed);
        }
        Ok((tx, view))
    }
}
impl DialogueInputSource for NativeInputSource {
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, QueryError>> {
        Box::pin(async move {
            let (mut tx, view) = self.current(context).await?;
            let page =
                crate::agent_file_query::list_authorized(&mut tx, context, &view.cut, from, limit)
                    .await?;
            tx.commit().await.map_err(|_| QueryError::Unavailable)?;
            Ok(page)
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, QueryError>> {
        Box::pin(async move {
            let (mut tx, view) = self.current(context).await?;
            let file =
                crate::agent_file_query::resolve_authorized(&mut tx, context, &view.cut, reference)
                    .await?;
            tx.commit().await.map_err(|_| QueryError::Unavailable)?;
            Ok(file)
        })
    }
}

impl DialogueOutputSource for NativeInputSource {
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, QueryError>> {
        Box::pin(async move {
            let (mut tx, view) = self.current(context).await?;
            let page = crate::agent_file_query::list_outputs_authorized(
                &mut tx, context, &view.cut, from, limit,
            )
            .await?;
            tx.commit().await.map_err(|_| QueryError::Unavailable)?;
            Ok(page)
        })
    }
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, QueryError>> {
        Box::pin(async move {
            let (mut tx, view) = self.current(context).await?;
            let file = crate::agent_file_query::resolve_output_authorized(
                &mut tx, context, &view.cut, identity,
            )
            .await?;
            tx.commit().await.map_err(|_| QueryError::Unavailable)?;
            Ok(file)
        })
    }
}
