mod support;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::*;

struct Authority(FileQueryContext);
impl FileQueryAuthority for Authority {
    fn resolve<'a>(
        &'a self,
        _: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}
struct OriginalStatus(Arc<AtomicUsize>);
impl FileRetractionStatusSource for OriginalStatus {
    fn retraction_status<'a>(
        &'a self,
        _: &'a FileQueryContext,
    ) -> ApplicationFuture<'a, Result<Option<FileRetractionStatus>, FileQueryError>> {
        Box::pin(async {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(FileRetractionStatus {
                pending: 1,
                applied: 2,
                retained: 3,
                conflict: 4,
                indeterminate: 5,
            }))
        })
    }
}
#[tokio::test]
async fn only_product_reads_original_outcomes_and_frozen_tool_output_stays_unchanged() {
    for restricted in [false, true] {
        let mut f = Fixture::new();
        if restricted {
            f.context.content_allowlist = Some(vec![]);
        }
        let count = Arc::new(AtomicUsize::new(0));
        let catalog = Arc::new(
            ConversationFileCatalog::new(f.projection)
                .with_retraction_status(Arc::new(OriginalStatus(count.clone()))),
        );
        let service = FileQueryService::new(
            Arc::new(Authority(f.context.clone())),
            Arc::new(FileQueryApplication::new(
                catalog,
                Arc::new(NoProject),
                Arc::new(ContentFileReader::new(f.bytes)),
            )),
        );
        let access = FileQueryAccess {
            actor: f.context.actor,
            conversation_id: f.context.conversation_id,
            workspace_id: None,
            run: None,
        };
        let (tool, _) = service
            .execute_tool(&access, "files.list@1", br#"{"scope":"conversation"}"#)
            .await
            .expect("frozen tool");
        assert!(tool.get("retraction_status").is_none());
        assert_eq!(count.load(Ordering::SeqCst), 0);
        let page = service
            .list_product(
                &access,
                ListFiles {
                    produced_by: None,
                    scope: FileScope::Conversation,
                    parent: None,
                    cursor: None,
                    limit: 100,
                    name_query: None,
                },
            )
            .await
            .expect("Product list");
        assert_eq!(page.retraction_status.is_some(), !restricted);
        assert_eq!(count.load(Ordering::SeqCst), usize::from(!restricted));
        let (tool, _) = service
            .execute_tool(&access, "files.list@1", br#"{"scope":"conversation"}"#)
            .await
            .expect("frozen tool after Product");
        assert!(tool.get("retraction_status").is_none());
        assert_eq!(count.load(Ordering::SeqCst), usize::from(!restricted));
    }
}
