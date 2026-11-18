mod support;
use musteragent_content_ref::ContentRef;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use musteragent_runtime_resource::ExtractionQuality;
use std::sync::Arc;
use support::*;

struct Documents {
    state: &'static str,
    corrupt: bool,
}
impl UploadedDocumentReader for Documents {
    fn read<'a>(
        &'a self,
        _: &'a FileQueryContext,
        source: &'a ContentRef,
        offset: usize,
        _: usize,
    ) -> ApplicationFuture<'a, Result<DocumentRepresentation, FileQueryError>> {
        Box::pin(async move {
            let ready = matches!(self.state, "ready" | "partially_ready");
            Ok(DocumentRepresentation {
                state: self.state.into(),
                source_sha256: if self.corrupt {
                    "0".repeat(64)
                } else {
                    source.sha256.to_string()
                },
                text: ready.then(|| "已解析".into()),
                representation_revision: ready.then(|| "manifest-1".into()),
                parser_revision: ready.then(|| "format-owner@1".into()),
                resolved_start: offset,
                resolved_end: if ready { offset + 9 } else { offset },
                next_offset: None,
                quality: ExtractionQuality {
                    pages_total: Some(2),
                    pages_with_text: if ready { vec![1] } else { vec![] },
                    issues: vec![],
                },
            })
        })
    }
}
#[tokio::test]
async fn owned_upload_uses_one_representation_without_raw_decode_on_pending_or_failure() {
    for state in [
        "ready",
        "partially_ready",
        "preparing",
        "failed",
        "unavailable",
    ] {
        let f = Fixture::new();
        // Deliberately not a valid PDF: any alternative decoder would produce a different result.
        f.attachment(
            "report.pdf",
            "application/pdf",
            b"source owned by another parser",
        );
        let reader = Arc::new(
            ContentFileReader::new(f.bytes.clone()).with_uploaded_documents(Arc::new(Documents {
                state,
                corrupt: false,
            })),
        );
        let app = FileQueryApplication::new(f.catalog.clone(), Arc::new(NoProject), reader);
        let page = app
            .list(
                &f.context,
                serde_json::from_value(serde_json::json!({"scope":"conversation"})).unwrap(),
            )
            .await
            .unwrap();
        let result = app
            .read(
                &f.context,
                ReadFile {
                    file_ref: page.entries[0].file_ref.clone(),
                    offset: 0,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.view.extraction_status, state);
        assert_eq!(*f.bytes.reads.lock().unwrap(), 0);
        if state == "partially_ready" {
            assert_eq!(result.view.quality.unwrap().pages_with_text, vec![1]);
        }
    }
}
#[tokio::test]
async fn source_identity_mismatch_fails_closed_without_trying_a_second_parser() {
    let f = Fixture::new();
    f.attachment("report.pdf", "application/pdf", b"original");
    let app = FileQueryApplication::new(
        f.catalog.clone(),
        Arc::new(NoProject),
        Arc::new(
            ContentFileReader::new(f.bytes.clone()).with_uploaded_documents(Arc::new(Documents {
                state: "ready",
                corrupt: true,
            })),
        ),
    );
    let page = app
        .list(
            &f.context,
            serde_json::from_value(serde_json::json!({"scope":"conversation"})).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        app.read(
            &f.context,
            ReadFile {
                file_ref: page.entries[0].file_ref.clone(),
                offset: 0,
                limit: 100
            }
        )
        .await,
        Err(FileQueryError::Integrity)
    ));
    assert_eq!(*f.bytes.reads.lock().unwrap(), 0);
}
