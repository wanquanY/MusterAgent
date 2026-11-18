#![cfg(test)]

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
    ArtifactVersionAuthor,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::*;
use musteragent_projection::{
    ConversationProjectionApplication, ConversationProjectionSnapshot,
    ConversationProjectionWindow, GetConversationProjection, GetConversationProjectionWindow,
    PROJECTION_SNAPSHOT_SCHEMA_VERSION, ProjectedArtifact, ProjectionRetraction,
};
use musteragent_runtime_artifact::{
    ArtifactApplication, ArtifactPage, ArtifactSummary, ArtifactView, GetArtifact,
    ListConversationArtifacts, ProjectedArtifactApplication,
};
use musteragent_runtime_contract::{ApplicationFuture, RuntimeError};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Catalog {
    entries: Mutex<Vec<ArtifactSummary>>,
    queries: Mutex<Vec<ListConversationArtifacts>>,
}
impl ArtifactApplication for Catalog {
    fn list_conversation_artifacts(
        &self,
        query: ListConversationArtifacts,
    ) -> ApplicationFuture<'_, Result<ArtifactPage, RuntimeError>> {
        Box::pin(async move {
            self.queries.lock().unwrap().push(query.clone());
            let rows = self.entries.lock().unwrap();
            let mut found = rows
                .iter()
                .filter(|row| {
                    query
                        .before_artifact_id
                        .is_none_or(|before| row.artifact.artifact_id < before)
                        && query
                            .search_query
                            .as_ref()
                            .is_none_or(|search| row.artifact.title.contains(search))
                        && (query.kinds.is_empty() || query.kinds.contains(&row.artifact.kind))
                })
                .take(query.limit + 1)
                .cloned()
                .collect::<Vec<_>>();
            let has_more = found.len() > query.limit;
            found.truncate(query.limit);
            Ok(ArtifactPage {
                next_before_artifact_id: has_more
                    .then(|| found.last().unwrap().artifact.artifact_id),
                artifacts: found,
                has_more,
            })
        })
    }
    fn get_artifact(
        &self,
        _: GetArtifact,
    ) -> ApplicationFuture<'_, Result<ArtifactView, RuntimeError>> {
        panic!("listing files must not fetch an individual artifact")
    }
}
struct Projections {
    snapshot: Mutex<ConversationProjectionSnapshot>,
    calls: Mutex<usize>,
}
impl ConversationProjectionApplication for Projections {
    fn get_head_projection(
        &self,
        query: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        Box::pin(async move {
            *self.calls.lock().unwrap() += 1;
            let snapshot = self.snapshot.lock().unwrap().clone();
            assert_eq!(snapshot.tenant_id, query.actor.tenant_id);
            assert_eq!(snapshot.conversation_id, query.conversation_id);
            assert_eq!(query.preferred_snapshot_id, None);
            Ok(snapshot)
        })
    }
    fn get_conversation_projection(
        &self,
        _: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        panic!("must use the current head")
    }
    fn get_bounded_head_projection(
        &self,
        _: GetConversationProjection,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionSnapshot, RuntimeError>> {
        panic!("listing files must use the complete current projection")
    }
    fn get_product_read_projection_window(
        &self,
        _: GetConversationProjectionWindow,
    ) -> ApplicationFuture<'_, Result<ConversationProjectionWindow, RuntimeError>> {
        panic!("the loaded transcript window is not the complete file scope")
    }
}
struct Fixture {
    catalog: Arc<Catalog>,
    projection: Arc<Projections>,
    application: ProjectedArtifactApplication,
    query: ListConversationArtifacts,
}
impl Fixture {
    fn new() -> Self {
        let tenant_id = TenantId::generate();
        let conversation_id = ConversationId::generate();
        let catalog = Arc::new(Catalog::default());
        let projection = Arc::new(Projections {
            calls: Mutex::new(0),
            snapshot: Mutex::new(ConversationProjectionSnapshot {
                schema_version: PROJECTION_SNAPSHOT_SCHEMA_VERSION.to_owned(),
                snapshot_id: ProjectionSnapshotId::generate(),
                tenant_id,
                conversation_id,
                home_epoch: HomeEpoch::new(1).unwrap(),
                through_conversation_seq: LedgerOffset::new(1),
                messages: vec![],
                tool_timeline: vec![],
                context_compactions: vec![],
                pending_interactions: vec![],
                activities: vec![],
                runs: vec![],
                active_runs: vec![],
                artifacts: vec![],
                stream_outputs: vec![],
                content_manifest: vec![],
                checksum: Sha256::digest(b"snapshot"),
                created_at: UtcTimestamp::from_unix_millis(1),
            }),
        });
        Self {
            application: ProjectedArtifactApplication::new(catalog.clone(), projection.clone()),
            catalog,
            projection,
            query: ListConversationArtifacts {
                actor: Actor {
                    tenant_id,
                    principal_id: PrincipalId::generate(),
                    principal_kind: PrincipalKind::User,
                },
                conversation_id,
                limit: 2,
                before_artifact_id: None,
                search_query: None,
                kinds: vec![],
            },
        }
    }
    fn add(&self, ordinal: u128, origin: ArtifactOrigin) -> ArtifactId {
        let artifact_id = ArtifactId::from_uuid(uuid::Uuid::from_u128(ordinal)).unwrap();
        let now = UtcTimestamp::from_unix_millis(1);
        let content = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("text/markdown").unwrap(),
            byte_length: 4,
            sha256: Sha256::digest(b"plan"),
        };
        let artifact = Artifact {
            artifact_id,
            tenant_id: self.query.actor.tenant_id,
            source: ArtifactSource::Conversation(self.query.conversation_id),
            origin,
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Published,
            current_version: VersionNumber::new(1).unwrap(),
            title: format!("plan-{ordinal}.md"),
            created_at: now,
            updated_at: now,
        };
        let version = ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: artifact.current_version,
            content_ref: content.clone(),
            content_hash: content.sha256,
            dependency_refs: vec![],
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::Participant(ParticipantId::generate()),
            committed_at: now,
        };
        self.projection
            .snapshot
            .lock()
            .unwrap()
            .artifacts
            .push(ProjectedArtifact {
                artifact_id,
                activity_id: artifact.activity_id(),
                created_by_run_id: artifact.created_by_run_id(),
                updated_by_run_id: artifact.created_by_run_id(),
                presentation_run_id: artifact.created_by_run_id(),
                kind: artifact.kind,
                lifecycle: artifact.lifecycle,
                current_version: artifact.current_version,
                title: artifact.title.clone(),
                current_content_ref: content,
                current_dependency_refs: vec![],
                updated_at: now,
            });
        let mut rows = self.catalog.entries.lock().unwrap();
        rows.push(ArtifactSummary {
            artifact,
            current_version: version,
        });
        rows.sort_by_key(|row| std::cmp::Reverse(row.artifact.artifact_id));
        artifact_id
    }
    async fn ids(&self, query: ListConversationArtifacts) -> (Vec<ArtifactId>, ArtifactPage) {
        let page = self
            .application
            .list_conversation_artifacts(query)
            .await
            .unwrap();
        (
            page.artifacts
                .iter()
                .map(|row| row.artifact.artifact_id)
                .collect(),
            page,
        )
    }
}
fn native(run: RunId) -> ArtifactOrigin {
    ArtifactOrigin::Runtime {
        activity_id: ActivityId::generate(),
        created_by_run_id: run,
    }
}

#[tokio::test]
async fn recalled_native_outputs_leave_the_list_but_immutable_catalog_is_retained() {
    let f = Fixture::new();
    let run = RunId::generate();
    let removed = f.add(3, native(run));
    let kept = f.add(2, native(RunId::generate()));
    assert_eq!(f.ids(f.query.clone()).await.0, vec![removed, kept]);
    ProjectionRetraction::new(MessageId::generate(), Some(run))
        .apply_to_snapshot(&mut f.projection.snapshot.lock().unwrap())
        .unwrap();
    assert_eq!(f.ids(f.query.clone()).await.0, vec![kept]);
    assert_eq!(f.catalog.entries.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn filtering_fills_pages_without_skipping_retained_outputs_and_keeps_search_constraints() {
    let f = Fixture::new();
    let removed_run = RunId::generate();
    let kept_run = RunId::generate();
    for id in 1..=8 {
        f.add(id, native(if id % 2 == 0 { removed_run } else { kept_run }));
    }
    ProjectionRetraction::new(MessageId::generate(), Some(removed_run))
        .apply_to_snapshot(&mut f.projection.snapshot.lock().unwrap())
        .unwrap();
    let mut query = f.query.clone();
    query.search_query = Some("plan".to_owned());
    query.kinds = vec![ArtifactKind::File];
    let (first, page) = f.ids(query.clone()).await;
    assert_eq!(
        first
            .iter()
            .map(|id| id.into_uuid().as_u128())
            .collect::<Vec<_>>(),
        vec![7, 5]
    );
    assert!(page.has_more);
    query.before_artifact_id = page.next_before_artifact_id;
    let (second, page) = f.ids(query).await;
    assert_eq!(
        second
            .iter()
            .map(|id| id.into_uuid().as_u128())
            .collect::<Vec<_>>(),
        vec![3, 1]
    );
    assert!(!page.has_more);
    assert_eq!(page.next_before_artifact_id, None);
    assert_eq!(*f.projection.calls.lock().unwrap(), 2);
    assert!(
        f.catalog
            .queries
            .lock()
            .unwrap()
            .iter()
            .all(|query| query.search_query.as_deref() == Some("plan")
                && query.kinds == vec![ArtifactKind::File])
    );
}
#[tokio::test]
async fn independent_archive_and_product_files_do_not_require_a_native_projection() {
    let f = Fixture::new();
    let imported = f.add(3, ArtifactOrigin::GovernedImport);
    let authored = f.add(2, ArtifactOrigin::Product);
    f.projection.snapshot.lock().unwrap().artifacts.clear();
    assert_eq!(f.ids(f.query.clone()).await.0, vec![imported, authored]);
    assert_eq!(*f.projection.calls.lock().unwrap(), 0);
}
#[tokio::test]
async fn current_branch_snapshot_is_the_visibility_authority_even_for_old_catalog_entries() {
    let f = Fixture::new();
    f.add(4, native(RunId::generate()));
    let branch_output = f.add(3, native(RunId::generate()));
    f.projection
        .snapshot
        .lock()
        .unwrap()
        .artifacts
        .retain(|item| item.artifact_id == branch_output);
    assert_eq!(f.ids(f.query.clone()).await.0, vec![branch_output]);
    f.projection.snapshot.lock().unwrap().artifacts.clear();
    let (ids, page) = f.ids(f.query.clone()).await;
    assert!(ids.is_empty());
    assert!(!page.has_more);
    assert_eq!(page.next_before_artifact_id, None);
}

#[tokio::test]
async fn invalid_page_queries_never_reach_storage() {
    let f = Fixture::new();
    let mut query = f.query.clone();
    query.limit = 0;
    let error = f
        .application
        .list_conversation_artifacts(query)
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        musteragent_runtime_contract::ErrorCode::InvalidCommand
    );
    assert!(f.catalog.queries.lock().unwrap().is_empty());
}

#[tokio::test]
async fn catalog_scope_mismatch_is_rejected_instead_of_publishing_foreign_files() {
    let f = Fixture::new();
    f.add(2, ArtifactOrigin::Product);
    f.catalog.entries.lock().unwrap()[0].artifact.tenant_id = TenantId::generate();
    let error = f
        .application
        .list_conversation_artifacts(f.query.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        musteragent_runtime_contract::ErrorCode::InternalInvariantViolated
    );
}
