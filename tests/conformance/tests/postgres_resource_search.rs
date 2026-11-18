#![cfg(test)]

use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::sync::Arc;

#[path = "support/postgres_resource_tree_support.rs"]
mod resource_support;
#[path = "support/running_loop.rs"]
mod running_loop;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_content_ref::MediaType;
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ResourceNodeId, ResourceRevision, UtcTimestamp, VersionNumber,
    WorkspaceId,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_postgres::{PostgresContentStore, PostgresResourceStore};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_resource::{
    CreateResourceFile, ProjectFileContentRepresentation, ProjectFileExtractionStatus,
    ProjectFileReadApplication, ProjectFileReadCoordinateSpace, ReadProjectFile,
    ResourceApplication, ResourceSearchApplication, ResourceSearchBackfillRun,
    ResourceSearchBackfiller, ResourceSemanticSearchState, ResourceStoreError, SearchResourceFiles,
};
use musteragent_testkit::IsolatedPostgres;
use resource_support::{context, mutation, name};

#[tokio::test]
async fn resource_search_indexes_exact_heads_and_never_returns_a_stale_file_version() {
    resource_search_fixture(false).await;
}
#[tokio::test]
async fn resource_vectors_use_current_model_scope_and_version_with_exclusive_leases() {
    resource_search_fixture(true).await;
}
async fn resource_search_fixture(vector: bool) {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start isolated PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    musteragent_conformance::migrate(&pool)
        .await
        .expect("apply Runtime migrations");
    if !vector {
        sqlx::query("DROP TABLE resource.embedding_index")
            .execute(&pool)
            .await
            .expect("disable optional vector index in isolated fixture");
    }
    let (fixture, _) = running_loop::seed_running_loop(&pool).await;
    let workspace_id = WorkspaceId::generate();
    sqlx::query(
        "UPDATE conversation.conversations SET workspace_id=$1
          WHERE tenant_id=$2 AND conversation_id=$3",
    )
    .bind(workspace_id.into_uuid())
    .bind(fixture.tenant_id.into_uuid())
    .bind(fixture.conversation_id.into_uuid())
    .execute(&pool)
    .await
    .expect("attach Conversation to Project");

    let content_store = Arc::new(PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        running_loop::keyring(),
    ));
    let content: Arc<dyn ContentStore> = content_store.clone();
    let first_content = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/markdown;charset=utf-8").expect("Markdown media type"),
            bytes: b"# Architecture\nExact provenance belongs to the immutable vector source.\n"
                .to_vec(),
        })
        .await
        .expect("persist first File bytes");
    let artifact_id = ArtifactId::generate();
    let first_version_id = ArtifactVersionId::generate();
    let node_id = ResourceNodeId::generate();
    let now = running_loop::database_now(&pool).await;
    ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
        .create_file(CreateResourceFile {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis(),
            ),
            identity: mutation("search-source-v1"),
            expected_revision: ResourceRevision::ZERO,
            node_id,
            parent_node_id: None,
            name: name("architecture.md"),
            sort_key: "architecture.md".to_owned(),
            artifact: Artifact {
                artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("first version"),
                title: "architecture.md".to_owned(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: first_version_id,
                artifact_id,
                version: VersionNumber::new(1).expect("first version"),
                content_ref: first_content.clone(),
                dependency_refs: Vec::new(),
                content_hash: first_content.sha256,
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(fixture.executor_id),
                committed_at: now,
            },
        })
        .await
        .expect("create Project File");

    sqlx::query(
        "UPDATE resource.search_index_jobs
            SET status=5,attempt_count=10,available_at_ms=0,
                lease_owner=NULL,lease_expires_at_ms=NULL,
                last_error_code='previous_failure',completed_at_ms=NULL
          WHERE artifact_version_id=$1",
    )
    .bind(first_version_id.into_uuid())
    .execute(&pool)
    .await
    .expect("seed an exhausted retry ahead of healthy work");

    let canary_content = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("text/markdown").expect("Markdown media type"),
            bytes: b"# Queue canary\nHealthy work must not be starved.\n".to_vec(),
        })
        .await
        .expect("persist queue canary bytes");
    let canary_artifact_id = ArtifactId::generate();
    let canary_version_id = ArtifactVersionId::generate();
    ResourceApplication::new(PostgresResourceStore::new(pool.clone()))
        .create_file(CreateResourceFile {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis(),
            ),
            identity: mutation("search-queue-canary"),
            expected_revision: ResourceRevision::new(1),
            node_id: ResourceNodeId::generate(),
            parent_node_id: None,
            name: name("queue-canary.md"),
            sort_key: "queue-canary.md".to_owned(),
            artifact: Artifact {
                artifact_id: canary_artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("first version"),
                title: "queue-canary.md".to_owned(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: canary_version_id,
                artifact_id: canary_artifact_id,
                version: VersionNumber::new(1).expect("first version"),
                content_ref: canary_content.clone(),
                dependency_refs: Vec::new(),
                content_hash: canary_content.sha256,
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(fixture.executor_id),
                committed_at: now,
            },
        })
        .await
        .expect("create queue canary Project File");

    let store = PostgresResourceStore::new(pool.clone());
    let backfiller = ResourceSearchBackfiller::new(store.clone(), Arc::clone(&content));
    let queue_now = running_loop::database_now(&pool).await;
    assert!(matches!(
        backfiller
            .run_once(
                "resource-search-test".to_owned(),
                queue_now,
                UtcTimestamp::from_unix_millis(queue_now.unix_millis() + 60_000),
                UtcTimestamp::from_unix_millis(queue_now.unix_millis() + 1_000),
            )
            .await
            .expect("skip exhausted retry and index healthy work"),
        ResourceSearchBackfillRun::Indexed {
            artifact_version_id,
            chunk_count: 1,
        } if artifact_version_id == canary_version_id
    ));
    let exhausted = sqlx::query_as::<_, (i16, Option<String>)>(
        "SELECT status,last_error_code
           FROM resource.search_index_jobs
          WHERE artifact_version_id=$1",
    )
    .bind(first_version_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read exhausted retry terminal state");
    assert_eq!(exhausted, (4, Some("attempts_exhausted".to_owned())));
    sqlx::query(
        "UPDATE resource.search_index_jobs
            SET status=1,attempt_count=0,available_at_ms=$1,
                last_error_code=NULL,completed_at_ms=NULL,updated_at_ms=$1
          WHERE artifact_version_id=$2",
    )
    .bind(queue_now.unix_millis())
    .bind(first_version_id.into_uuid())
    .execute(&pool)
    .await
    .expect("restore the primary search fixture");
    let first_index_now = running_loop::database_now(&pool).await;
    assert!(matches!(
        backfiller
            .run_once(
                "resource-search-test".to_owned(),
                first_index_now,
                UtcTimestamp::from_unix_millis(first_index_now.unix_millis() + 60_000),
                UtcTimestamp::from_unix_millis(first_index_now.unix_millis() + 1_000),
            )
            .await
            .expect("index first File version"),
        ResourceSearchBackfillRun::Indexed {
            artifact_version_id,
            chunk_count: 1,
        } if artifact_version_id == first_version_id
    ));

    let search = ResourceSearchApplication::new(store.clone());
    let first_page = search
        .search(search_request(
            fixture,
            workspace_id,
            now,
            "exact provenance",
        ))
        .await
        .expect("search first version");
    assert_eq!(
        first_page.semantic_state,
        ResourceSemanticSearchState::Unavailable
    );
    assert_eq!(first_page.hits.len(), 1);
    let first_hit = &first_page.hits[0];
    assert_eq!(first_hit.node_id, node_id);
    assert_eq!(first_hit.artifact_id, artifact_id);
    assert_eq!(first_hit.artifact_version_id, first_version_id);
    assert_eq!(first_hit.file_version.get(), 1);
    assert_eq!(first_hit.content_sha256, first_content.sha256);
    assert_eq!(first_hit.path, "/architecture.md");
    assert_eq!(first_hit.locator.line_start, Some(1));
    assert_eq!(first_hit.locator.section_path, ["Architecture"]);

    let embeddings = musteragent_postgres::PostgresResourceEmbeddings::new(pool.clone());
    let model = musteragent_postgres::ResourceEmbeddingModel {
        key: "configured-embedding".into(),
        revision: "model-1".into(),
        max_input_bytes: 4096,
    };
    assert_eq!(embeddings.available().await.unwrap(), vector);
    if vector {
        assert!(
            embeddings
                .search(
                    &model,
                    &search_request(fixture, workspace_id, now, "source"),
                    &[1., 0.]
                )
                .await
                .unwrap()
                .is_none()
        );
        let first = embeddings
            .claim(&model, now.unix_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        let other = embeddings
            .claim(&model, now.unix_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            first.chunk_id, other.chunk_id,
            "concurrent workers claim disjoint chunks"
        );
        assert!(
            embeddings
                .claim(&model, now.unix_millis(), 60_000)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !embeddings
                .covered(
                    &model,
                    &search_request(fixture, workspace_id, now, "source")
                )
                .await
                .unwrap()
        );
        embeddings.fail(&first, now.unix_millis()).await.unwrap();
        assert!(
            embeddings
                .claim(&model, now.unix_millis(), 60_000)
                .await
                .unwrap()
                .is_none(),
            "provider failure backs off without losing pending work"
        );
        let retry_at: i64 = sqlx::query_scalar(
            "SELECT available_at_ms FROM resource.embedding_jobs WHERE space_id=$1 AND chunk_id=$2",
        )
        .bind(first.space_id)
        .bind(first.chunk_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(retry_at > now.unix_millis());
        // Complete the other lease before advancing the test clock.
        embeddings
            .complete(&model, &other, &[1., 0.], now.unix_millis())
            .await
            .unwrap();
        let retried = embeddings
            .claim(&model, retry_at, 60_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.chunk_id, first.chunk_id);
        assert_ne!(retried.token, first.token);
        assert!(
            embeddings
                .complete(&model, &first, &[1., 0.], retry_at)
                .await
                .is_err(),
            "expired workers cannot overwrite the new lease"
        );
        embeddings
            .complete(&model, &retried, &[1., 0.], retry_at)
            .await
            .unwrap();
        assert!(
            embeddings
                .complete(&model, &first, &[1., 0.], now.unix_millis())
                .await
                .is_err(),
            "stale completion cannot overwrite an indexed chunk"
        );
        assert!(
            embeddings
                .covered(
                    &model,
                    &search_request(fixture, workspace_id, now, "source")
                )
                .await
                .unwrap()
        );
        assert!(
            embeddings
                .claim(&model, now.unix_millis(), 60_000)
                .await
                .unwrap()
                .is_none()
        );
        let hits = embeddings
            .search(
                &model,
                &search_request(fixture, workspace_id, now, "source"),
                &[1., 0.],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(
            hits.iter()
                .any(|hit| hit.artifact_version_id == first_version_id)
        );
        // The Conversation is bound to one Project; naming another Project
        // fails authorization before any vector is compared.
        assert!(
            matches!(
                embeddings
                    .search(
                        &model,
                        &search_request(fixture, WorkspaceId::generate(), now, "source"),
                        &[1., 0.],
                    )
                    .await,
                Err(ResourceStoreError::NodeUnavailable)
            ),
            "vector similarity cannot cross project boundaries"
        );
        let changed = musteragent_postgres::ResourceEmbeddingModel {
            revision: "model-2".into(),
            ..model.clone()
        };
        assert!(
            embeddings
                .search(
                    &changed,
                    &search_request(fixture, workspace_id, now, "source"),
                    &[1., 0.]
                )
                .await
                .unwrap()
                .is_none(),
            "Admin model changes cannot use old vectors"
        );
    }

    let second_content = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: first_content.media_type.clone(),
            bytes: b"# Architecture\nReplacement source is the only searchable head.\n".to_vec(),
        })
        .await
        .expect("persist replacement File bytes");
    let second_version_id = ArtifactVersionId::generate();
    let second_now = UtcTimestamp::from_unix_millis(now.unix_millis() + 10);
    sqlx::query(
        "INSERT INTO artifact.artifact_versions(
             artifact_version_id,artifact_id,version,content_id,content_hash,
             source_invocation_id,committed_by,committed_at_ms
         ) VALUES($1,$2,2,$3,$4,NULL,$5,$6)",
    )
    .bind(second_version_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(second_content.content_id.into_uuid())
    .bind(second_content.sha256.into_bytes().to_vec())
    .bind(fixture.executor_id.into_uuid())
    .bind(second_now.unix_millis())
    .execute(&pool)
    .await
    .expect("append replacement File version");
    sqlx::query(
        "UPDATE artifact.artifacts SET current_version=2,updated_at_ms=$1
          WHERE artifact_id=$2 AND current_version=1",
    )
    .bind(second_now.unix_millis())
    .bind(artifact_id.into_uuid())
    .execute(&pool)
    .await
    .expect("advance File head");

    assert!(
        search
            .search(search_request(
                fixture,
                workspace_id,
                second_now,
                "exact provenance",
            ))
            .await
            .expect("search while replacement projection is pending")
            .hits
            .is_empty(),
        "a stale projection must not be returned after the File head advances"
    );
    let second_index_now = running_loop::database_now(&pool).await;
    assert!(matches!(
        backfiller
            .run_once(
                "resource-search-test".to_owned(),
                second_index_now,
                UtcTimestamp::from_unix_millis(second_index_now.unix_millis() + 60_000),
                UtcTimestamp::from_unix_millis(second_index_now.unix_millis() + 1_000),
            )
            .await
            .expect("index replacement File version"),
        ResourceSearchBackfillRun::Indexed {
            artifact_version_id,
            chunk_count: 1,
        } if artifact_version_id == second_version_id
    ));
    if vector {
        assert!(
            embeddings
                .search(
                    &model,
                    &search_request(fixture, workspace_id, second_now, "source"),
                    &[1., 0.]
                )
                .await
                .unwrap()
                .is_none(),
            "new content waits for its own vector"
        );
        let current = embeddings
            .claim(&model, second_index_now.unix_millis(), 60_000)
            .await
            .unwrap()
            .unwrap();
        assert!(current.text.contains("Replacement"));
        embeddings
            .complete(&model, &current, &[0., 1.], second_index_now.unix_millis())
            .await
            .unwrap();
        let hits = embeddings
            .search(
                &model,
                &search_request(fixture, workspace_id, second_now, "source"),
                &[0., 1.],
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.artifact_version_id == second_version_id)
        );
        assert!(
            !hits
                .iter()
                .any(|hit| hit.artifact_version_id == first_version_id)
        );
    }
    let replacement = search
        .search(search_request(
            fixture,
            workspace_id,
            second_now,
            "replacement source",
        ))
        .await
        .expect("search replacement version");
    assert_eq!(replacement.hits.len(), 1);
    assert_eq!(replacement.hits[0].artifact_version_id, second_version_id);
    assert_eq!(replacement.hits[0].file_version.get(), 2);
    assert_eq!(replacement.hits[0].content_sha256, second_content.sha256);

    let pdf_bytes = searchable_pdf("Exact binary source readback");
    let pdf_content = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/pdf").expect("PDF media type"),
            bytes: pdf_bytes,
        })
        .await
        .expect("persist PDF bytes");
    let pdf_artifact_id = ArtifactId::generate();
    let pdf_version_id = ArtifactVersionId::generate();
    let pdf_node_id = ResourceNodeId::generate();
    ResourceApplication::new(store.clone())
        .create_file(CreateResourceFile {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                now.unix_millis(),
            ),
            identity: mutation("search-pdf-source"),
            expected_revision: ResourceRevision::new(2),
            node_id: pdf_node_id,
            parent_node_id: None,
            name: name("source.pdf"),
            sort_key: "source.pdf".to_owned(),
            artifact: Artifact {
                artifact_id: pdf_artifact_id,
                tenant_id: fixture.tenant_id,
                source: ArtifactSource::Conversation(fixture.conversation_id),
                origin: ArtifactOrigin::Product,
                kind: ArtifactKind::File,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: VersionNumber::new(1).expect("first version"),
                title: "source.pdf".to_owned(),
                created_at: now,
                updated_at: now,
            },
            version: ArtifactVersion {
                artifact_version_id: pdf_version_id,
                artifact_id: pdf_artifact_id,
                version: VersionNumber::new(1).expect("first version"),
                content_ref: pdf_content.clone(),
                dependency_refs: Vec::new(),
                content_hash: pdf_content.sha256,
                source_invocation_id: None,
                committed_by: ArtifactVersionAuthor::Participant(fixture.executor_id),
                committed_at: now,
            },
        })
        .await
        .expect("create PDF Project File");
    let pdf_index_now = running_loop::database_now(&pool).await;
    assert!(matches!(
        backfiller
            .run_once(
                "resource-search-test".to_owned(),
                pdf_index_now,
                UtcTimestamp::from_unix_millis(pdf_index_now.unix_millis() + 60_000),
                UtcTimestamp::from_unix_millis(pdf_index_now.unix_millis() + 1_000),
            )
            .await
            .expect("index PDF File version"),
        ResourceSearchBackfillRun::Indexed {
            artifact_version_id,
            chunk_count: 1,
        } if artifact_version_id == pdf_version_id
    ));
    let pdf_hit = search
        .search(search_request(
            fixture,
            workspace_id,
            pdf_index_now,
            "binary source readback",
        ))
        .await
        .expect("search PDF source");
    assert_eq!(pdf_hit.hits.len(), 1);
    assert_eq!(pdf_hit.hits[0].artifact_version_id, pdf_version_id);
    assert_eq!(pdf_hit.hits[0].path, "/source.pdf");
    let pdf_read = ProjectFileReadApplication::new(store, Arc::clone(&content))
        .read(ReadProjectFile {
            context: context(
                fixture.tenant_id,
                fixture.principal_id,
                fixture.conversation_id,
                workspace_id,
                pdf_index_now.unix_millis(),
            ),
            node_id: pdf_node_id,
            version: Some(VersionNumber::new(1).expect("first version")),
            offset: usize::try_from(pdf_hit.hits[0].locator.start_byte)
                .expect("PDF locator fits usize"),
            limit: 4_096,
            include_outline: true,
        })
        .await
        .expect("read exact PDF version as extracted text");
    assert_eq!(pdf_read.file.artifact_version_id, pdf_version_id);
    assert_eq!(pdf_read.file.content_ref.sha256, pdf_content.sha256);
    assert_eq!(
        pdf_read.content_representation,
        ProjectFileContentRepresentation::ExtractedText
    );
    assert_eq!(
        pdf_read.extraction_status,
        ProjectFileExtractionStatus::Available
    );
    assert_eq!(
        pdf_read.coordinate_space,
        ProjectFileReadCoordinateSpace::ExtractedUtf8
    );
    assert_eq!(
        pdf_read.parser.expect("PDF parser identity").key,
        // The isolated document parser identity since 8169ab6f1e.
        "resource.pdf-text"
    );
    assert!(
        pdf_read
            .content
            .expect("PDF extracted text")
            .contains("Exact binary source readback")
    );
}

fn searchable_pdf(value: &str) -> Vec<u8> {
    let stream = format!("BT /F1 12 Tf 72 720 Td ({value}) Tj ET");
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_owned(),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
        format!("<< /Length {} >>\nstream\n{stream}\nendstream", stream.len()),
    ];
    let mut pdf = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n{}\nendobj\n", index + 1, object).as_bytes());
    }
    let xref = pdf.len();
    pdf.extend_from_slice(
        format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
    );
    for offset in offsets {
        pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    pdf.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    pdf
}

fn search_request(
    fixture: running_loop::Fixture,
    workspace_id: WorkspaceId,
    now: UtcTimestamp,
    query: &str,
) -> SearchResourceFiles {
    SearchResourceFiles {
        context: context(
            fixture.tenant_id,
            fixture.principal_id,
            fixture.conversation_id,
            workspace_id,
            now.unix_millis(),
        ),
        query: query.to_owned(),
        limit: 10,
    }
}
