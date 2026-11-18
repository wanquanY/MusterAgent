//! Lease recovery is part of the Runtime transaction, not UI cleanup or SQL repair.
use super::*;

fn replacement(f: &Fixture, key: &str) -> BeginFileEdit {
    let now = f.context.now.unix_millis() + 60_000;
    BeginFileEdit {
        context: ResourceCommandContext {
            now: UtcTimestamp::from_unix_millis(now),
            ..f.context.clone()
        },
        idempotency_key: IdempotencyKey::new(key).unwrap(),
        command_fingerprint: Sha256::digest(key.as_bytes()),
        expires_at: UtcTimestamp::from_unix_millis(now + 60_000),
        ..f.begin()
    }
}

#[tokio::test]
async fn expired_edit_releases_its_slot_without_losing_the_checkpoint_or_committing_it() {
    let f = Fixture::new().await;
    let previous_finalize = f.prepare().await;
    let edits = f.edits();
    let previous = edits
        .resume(f.context.clone(), previous_finalize.session_id)
        .await
        .unwrap();
    let next = replacement(&f, "after-expiry");
    let mut still_leased = next.clone();
    still_leased.context.now = UtcTimestamp::from_unix_millis(next.context.now.unix_millis() - 1);
    assert_eq!(
        edits.begin(still_leased).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    assert_eq!(
        edits
            .resume(f.context.clone(), previous_finalize.session_id)
            .await
            .unwrap(),
        previous
    );

    let opened = edits
        .begin(next.clone())
        .await
        .expect("expired lease must not block a new editor");
    let expired = edits
        .resume(next.context.clone(), previous_finalize.session_id)
        .await
        .unwrap();
    assert_eq!(expired.session.state, EditSessionState::Expired);
    assert_eq!(
        expired.session.session_revision,
        previous.session.session_revision.next().unwrap()
    );
    assert_eq!(expired.checkpoint_ref, previous.checkpoint_ref);
    assert_eq!(
        f.content
            .read(f.source.tenant_id, &expired.checkpoint_ref)
            .await
            .unwrap(),
        b"# User edit\n"
    );
    assert_eq!(
        f.content
            .read(f.source.tenant_id, &opened.checkpoint_ref)
            .await
            .unwrap(),
        b"# Original\n"
    );
    assert_eq!(
        f.version_count().await,
        1,
        "expiring a draft never publishes its bytes"
    );
    assert_eq!(
        edits.finalize(previous_finalize.clone()).await.unwrap_err(),
        FileEditError::SessionConflict
    );
    assert_eq!(
        edits.begin(next.clone()).await.unwrap(),
        opened,
        "begin replay must not expire twice"
    );

    let applied = edits
        .apply(ApplyFileEdits {
            context: next.context.clone(),
            session_id: next.session_id,
            operation_id: ResourceEditOperationId::generate(),
            idempotency_key: IdempotencyKey::new("new-editor-text").unwrap(),
            expected_session_revision: ResourceRevision::ZERO,
            expected_sequence: 1,
            operations: vec![TextEditOperation::Replace {
                range: TextRange {
                    start_byte: 0,
                    end_byte: 11,
                },
                expected_hash: Sha256::digest(b"# Original\n"),
                text: "# New edit\n".into(),
            }],
        })
        .await
        .unwrap();
    let finalize = FinalizeFileEdit {
        context: next.context,
        session_id: next.session_id,
        command_id: ResourceEditTerminalCommandId::generate(),
        artifact_version_id: ArtifactVersionId::generate(),
        idempotency_key: IdempotencyKey::new("new-editor-finalize").unwrap(),
        expected_session_revision: applied.session_revision,
        ..previous_finalize
    };
    let receipt = edits.finalize(finalize.clone()).await.unwrap();
    assert_eq!(receipt.state, EditSessionState::Committed);
    assert_eq!(receipt.file_version, Some(VersionNumber::new(2).unwrap()));
    assert!(edits.finalize(finalize).await.unwrap().duplicate);
    assert_eq!(f.version_count().await, 2);
}

#[tokio::test]
async fn invalid_replacement_cannot_expire_an_existing_checkpoint() {
    let f = Fixture::new().await;
    let previous = f.prepare().await;
    let edits = f.edits();
    let original = edits
        .resume(f.context.clone(), previous.session_id)
        .await
        .unwrap();
    for mode in ["owner", "tenant", "node", "base-version"] {
        let mut invalid = replacement(&f, mode);
        match mode {
            "owner" => invalid.context.actor.principal_id = PrincipalId::generate(),
            "tenant" => {
                invalid.context.tenant_id = TenantId::generate();
                invalid.context.actor.tenant_id = invalid.context.tenant_id;
            }
            "node" => invalid.file_node_id = ResourceNodeId::generate(),
            _ => invalid.base_version = VersionNumber::new(2).unwrap(),
        }
        assert!(edits.begin(invalid).await.is_err());
        assert_eq!(
            edits
                .resume(f.context.clone(), previous.session_id)
                .await
                .unwrap(),
            original,
            "a denied/invalid begin must roll back any expiration: {mode}"
        );
    }
    assert_eq!(f.version_count().await, 1);
}

#[tokio::test]
async fn simultaneous_replacements_expire_once_and_leave_one_live_editor() {
    let f = Fixture::new().await;
    let previous = f.prepare().await;
    let edits = f.edits();
    let first = replacement(&f, "first-replacement");
    let second = replacement(&f, "second-replacement");
    let (left, right) = tokio::join!(edits.begin(first), edits.begin(second));
    let (Ok(_), Err(error)) = (if left.is_ok() {
        (left, right)
    } else {
        (right, left)
    }) else {
        panic!("exactly one replacement editor must acquire the slot");
    };
    assert_eq!(error, FileEditError::SessionConflict);
    let expired = edits
        .resume(f.context.clone(), previous.session_id)
        .await
        .unwrap();
    assert_eq!(expired.session.state, EditSessionState::Expired);
    assert_eq!(expired.session.session_revision, ResourceRevision::new(2));
    let active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resource.edit_sessions WHERE tenant_id=$1 AND file_artifact_id=$2 AND state IN (1,2)",
    ).bind(f.source.tenant_id.into_uuid()).bind(f.artifact_id.into_uuid()).fetch_one(&f.pool).await.unwrap();
    assert_eq!(active, 1);
    assert_eq!(f.version_count().await, 1);
}
