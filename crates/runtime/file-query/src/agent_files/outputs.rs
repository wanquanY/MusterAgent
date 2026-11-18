//! Original public deliverable media, selected through the sealed Dialogue cut.
//! This view never invents Resource nodes or copies files into another owner.
use crate::*;
use musteragent_runtime_contract::ApplicationFuture;

pub trait DialogueOutputSource: Send + Sync {
    fn list<'a>(
        &'a self,
        context: &'a FileQueryContext,
        from: DialogueInputPosition,
        limit: usize,
    ) -> ApplicationFuture<'a, Result<DialogueInputPage, FileQueryError>>;
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>>;
    fn resolve<'a>(
        &'a self,
        context: &'a FileQueryContext,
        reference: &'a FileReference,
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
}

pub(crate) fn output_contract(key: &str) -> bool {
    matches!(
        key,
        "files.list@6"
            | "files.inspect@6"
            | "files.read@6"
            | "files.search@5"
            | "data.python.run@12"
    )
}
