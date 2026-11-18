mod support;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_file_query::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use support::*;

struct Catalog {
    original: ResolvedFile,
    prepared: Mutex<ResolvedFile>,
    preparations: AtomicUsize,
}
impl FileCatalog for Catalog {
    fn resolve_identity<'a>(
        &'a self,
        context: &'a FileQueryContext,
        identity: &'a FileIdentity,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            identity.validate(context)?;
            if self.original.entry.file_ref.identity() != *identity {
                return Err(FileQueryError::InvalidReference);
            }
            Ok(self.original.clone())
        })
    }

    fn list<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: ListFiles,
    ) -> ApplicationFuture<'a, Result<FilePage, FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Unsupported) })
    }
    fn resolve_current<'a>(
        &'a self,
        _: &'a FileQueryContext,
        reference: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async move {
            if reference == &self.original.entry.file_ref {
                Ok(self.original.clone())
            } else {
                Ok(self.prepared.lock().expect("prepared").clone())
            }
        })
    }
    fn prepare_content<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: &'a FileReference,
    ) -> ApplicationFuture<'a, Result<ResolvedFile, FileQueryError>> {
        Box::pin(async {
            self.preparations.fetch_add(1, Ordering::SeqCst);
            Ok(self.prepared.lock().expect("prepared").clone())
        })
    }
}
async fn fixture() -> (Fixture, Arc<Catalog>, FileQueryApplication) {
    let f = Fixture::new();
    f.attachment("selected.txt", "text/plain", b"source");
    let original = f
        .catalog
        .resolve(&f.context, &f.first().await)
        .await
        .expect("original source");
    let mut prepared = original.clone();
    prepared.entry.file_ref.revision = "prepared-exact-version".into();
    let catalog = Arc::new(Catalog {
        original,
        prepared: Mutex::new(prepared),
        preparations: AtomicUsize::new(0),
    });
    let app = FileQueryApplication::new(
        catalog.clone(),
        Arc::new(NoProject),
        Arc::new(ContentFileReader::new(f.bytes.clone())),
    );
    (f, catalog, app)
}

#[tokio::test]
async fn invalid_original_metadata_is_rejected_before_preparation_io() {
    let (f, good, _) = fixture().await;
    let mut original = good.original.clone();
    original.entry.name.clear();
    let catalog = Arc::new(Catalog {
        prepared: Mutex::new(original.clone()),
        original,
        preparations: AtomicUsize::new(0),
    });
    let app = FileQueryApplication::new(
        catalog.clone(),
        Arc::new(NoProject),
        Arc::new(ContentFileReader::new(f.bytes.clone())),
    );
    assert!(
        app.prepare_content(&f.context, &catalog.original.entry.file_ref)
            .await
            .is_err()
    );
    assert_eq!(catalog.preparations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn only_explicit_preparation_can_return_a_stronger_revision_for_the_original_identity() {
    let (f, catalog, app) = fixture().await;
    let reference = &catalog.original.entry.file_ref;
    assert_eq!(
        app.inspect(&f.context, reference)
            .await
            .expect("ordinary inspect")
            .file_ref,
        *reference
    );
    let prepared = app
        .prepare_content(&f.context, reference)
        .await
        .expect("explicit");
    assert_ne!(prepared.file_ref.revision, reference.revision);
    assert_eq!(prepared.file_ref.source, reference.source);
}

#[tokio::test]
async fn preparation_cannot_change_name_source_target_or_dependency_authority() {
    let (f, catalog, app) = fixture().await;
    let good = catalog.prepared.lock().expect("prepared").clone();
    for mutation in 0..7 {
        let mut bad = good.clone();
        match mutation {
            0 => bad.entry.name = "different original".into(),
            1 => {
                bad.entry.file_ref.conversation_id =
                    Some(musteragent_identity::ConversationId::generate().to_string())
            }
            2 => {
                bad.entry.file_ref.source = FileSource::Artifact {
                    artifact_id: musteragent_identity::ArtifactId::generate().to_string(),
                }
            }
            3 => bad.content = None,
            4 => bad.dependencies.push(good.content.clone().expect("body")),
            5 => bad.content = Some(f.bytes.add("text/plain", b"unauthorized replacement")),
            _ => bad.entry.byte_length = Some(7),
        }
        *catalog.prepared.lock().expect("prepared") = bad;
        assert!(
            app.prepare_content(&f.context, &catalog.original.entry.file_ref)
                .await
                .is_err(),
            "mutation {mutation}"
        );
    }
}

#[tokio::test]
async fn prepared_content_is_still_restricted_by_the_original_content_allowlist() {
    let (mut f, catalog, app) = fixture().await;
    f.context.content_allowlist = Some(Vec::new());
    assert_eq!(
        app.prepare_content(&f.context, &catalog.original.entry.file_ref)
            .await
            .expect_err("no new content grant"),
        FileQueryError::Denied
    );
}

struct ChangedReader(Arc<Bytes>);

struct ChangingAuthority {
    context: FileQueryContext,
    calls: std::sync::atomic::AtomicUsize,
}
impl FileQueryAuthority for ChangingAuthority {
    fn resolve<'a>(
        &'a self,
        _: &'a FileQueryAccess,
    ) -> ApplicationFuture<'a, Result<FileQueryContext, FileQueryError>> {
        Box::pin(async move {
            let mut context = self.context.clone();
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                context.authority_revision = "permission changed during preparation".into();
            }
            Ok(context)
        })
    }
}
#[tokio::test]
async fn explicit_preparation_rechecks_product_authority_after_source_work() {
    let (f, catalog, app) = fixture().await;
    let authority = Arc::new(ChangingAuthority {
        context: f.context.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = FileQueryService::new(authority.clone(), Arc::new(app));
    let access = FileQueryAccess {
        actor: f.context.actor,
        conversation_id: f.context.conversation_id,
        workspace_id: None,
        run: None,
    };
    assert_eq!(
        service
            .prepare_content(&access, &catalog.original.entry.file_ref)
            .await
            .expect_err("no returned authority from completed IO"),
        FileQueryError::Denied
    );
    assert_eq!(authority.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}
impl FileContentReader for ChangedReader {
    fn read<'a>(
        &'a self,
        context: &'a FileQueryContext,
        file: ResolvedFile,
        query: ReadFile,
    ) -> ApplicationFuture<'a, Result<FileReadResult, FileQueryError>> {
        Box::pin(async move {
            ContentFileReader::new(self.0.clone())
                .read(context, file, query)
                .await
        })
    }
    fn revalidate<'a>(
        &'a self,
        _: &'a FileQueryContext,
        _: &'a ResolvedFile,
    ) -> ApplicationFuture<'a, Result<(), FileQueryError>> {
        Box::pin(async { Err(FileQueryError::Changed) })
    }
}
#[tokio::test]
async fn semantic_read_and_raw_content_recheck_the_original_reader_after_io() {
    let f = Fixture::new();
    let content = f.attachment("original.txt", "text/plain", b"source");
    let reference = f.first().await;
    let app = FileQueryApplication::new(
        f.catalog.clone(),
        Arc::new(NoProject),
        Arc::new(ChangedReader(f.bytes.clone())),
    )
    .with_content_store(f.bytes.clone());
    assert_eq!(
        app.read(
            &f.context,
            ReadFile {
                file_ref: reference.clone(),
                offset: 0,
                limit: 100
            }
        )
        .await
        .expect_err("original mutable body recheck"),
        FileQueryError::Changed
    );
    assert_eq!(
        app.read_content(&f.context, &reference, &content)
            .await
            .expect_err("raw body recheck"),
        FileQueryError::Changed
    );
}
