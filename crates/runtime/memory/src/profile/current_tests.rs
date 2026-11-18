//! Current Dialogue reads preserve original scope/scanner behavior without
//! mistaking unavailable shared authority for an empty confirmed profile.
use super::*;
use crate::{MemoryPersistenceDisposition, MemorySearchHit, RememberUserMemory, RevokeUserMemory};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, IdempotencyKey, VersionNumber};
use musteragent_memory::{MemoryConfidenceBps, MemoryLifecycle, MemoryRecord, MemoryVersion};
use musteragent_policy::DataClassification;
use musteragent_runtime_content::{ContentStoreError, PutContent};
use musteragent_runtime_contract::ApplicationFuture;
use std::sync::Mutex;

struct Store {
    result: Mutex<Result<Vec<MemorySearchHit>, ()>>,
    expected: UserProfilePromptRequest,
}
impl MemoryStore for Store {
    type Error = ();
    fn search(
        &self,
        query: MemorySearch,
    ) -> ApplicationFuture<'_, Result<Vec<MemorySearchHit>, ()>> {
        assert_eq!(query.tenant_id, self.expected.tenant_id);
        assert_eq!(query.principal_id, self.expected.principal_id);
        assert_eq!(query.agent_ids, BTreeSet::from([self.expected.agent_id]));
        assert_eq!(query.workspace_ids, self.expected.workspace_ids);
        Box::pin(async { self.result.lock().unwrap().clone() })
    }
    fn remember_user(
        &self,
        _: RememberUserMemory,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, ()>> {
        panic!("profile read cannot write")
    }
    fn load_published_head(
        &self,
        _: MemorySearch,
        _: MemoryId,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchHit>, ()>> {
        panic!("scope search only")
    }
    fn find_user_by_key(
        &self,
        _: TenantId,
        _: PrincipalId,
        _: IdempotencyKey,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchHit>, ()>> {
        panic!("scope search only")
    }
    fn load_active_head(
        &self,
        _: TenantId,
        _: MemoryId,
    ) -> ApplicationFuture<'_, Result<Option<MemorySearchHit>, ()>> {
        panic!("no unscoped reads")
    }
    fn revoke_user(
        &self,
        _: RevokeUserMemory,
    ) -> ApplicationFuture<'_, Result<MemoryPersistenceDisposition, ()>> {
        panic!("profile read cannot write")
    }
}
struct Content {
    result: Mutex<Result<Vec<u8>, ContentStoreError>>,
}
impl ContentStore for Content {
    fn put(&self, _: PutContent) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        panic!("profile read cannot write")
    }
    fn read<'a>(
        &'a self,
        _: TenantId,
        _: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>> {
        Box::pin(async { self.result.lock().unwrap().clone() })
    }
}
fn request() -> UserProfilePromptRequest {
    UserProfilePromptRequest {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
        workspace_ids: BTreeSet::new(),
        now: UtcTimestamp::from_unix_millis(5000),
    }
}
fn hit(request: &UserProfilePromptRequest, bytes: &[u8]) -> MemorySearchHit {
    let id = MemoryId::generate();
    let version = MemoryVersionId::generate();
    MemorySearchHit {
        record: MemoryRecord {
            memory_id: id,
            tenant_id: request.tenant_id,
            scope: MemoryScope::User {
                principal_id: request.principal_id,
            },
            memory_key: IdempotencyKey::new("user-memory:preference.communication_style").unwrap(),
            kind: MemoryKind::Preference,
            lifecycle: MemoryLifecycle::Active,
            current_version_id: version,
            created_at: request.now,
            updated_at: request.now,
            revoked_at: None,
        },
        version: MemoryVersion {
            memory_version_id: version,
            memory_id: id,
            version: VersionNumber::new(1).unwrap(),
            source: MemorySource::UserAuthored,
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new("application/json").unwrap(),
                byte_length: bytes.len() as u64,
                sha256: Sha256::digest(bytes),
            },
            confidence: MemoryConfidenceBps::new(10000).unwrap(),
            data_classification: DataClassification::Confidential,
            source_run_ids: BTreeSet::new(),
            evidence_record_ids: BTreeSet::new(),
            created_at: request.now,
            valid_until: None,
        },
    }
}

#[tokio::test]
async fn current_profile_requires_authoritative_read_but_preserves_legacy_soft_failure() {
    let request = request();
    let memory = Arc::new(Store {
        result: Mutex::new(Err(())),
        expected: request.clone(),
    });
    let content = Arc::new(Content {
        result: Mutex::new(Err(ContentStoreError::Unavailable)),
    });
    let assembler = PublishedUserProfileAssembler::new(memory.clone(), content);
    assert_eq!(assembler.assemble(request.clone()).await.unwrap(), None);
    assert_eq!(
        assembler.assemble_current(request.clone()).await,
        Err(UserProfilePromptError::Store)
    );
    *memory.result.lock().unwrap() = Ok(vec![]);
    assert_eq!(
        assembler.assemble_current(request.clone()).await.unwrap(),
        None
    );
    *memory.result.lock().unwrap() = Err(());
    assert_eq!(
        assembler.assemble_current(request).await,
        Err(UserProfilePromptError::Store),
        "previous empty result cannot become a cache fallback"
    );
}

#[tokio::test]
async fn current_profile_rejects_unreadable_or_corrupt_source_and_never_reuses_old_fact() {
    let request = request();
    let bytes = br#"{"fact_key":"preference.communication_style","content":"Answer in Chinese, with the conclusion first."}"#.to_vec();
    let original = hit(&request, &bytes);
    let memory = Arc::new(Store {
        result: Mutex::new(Ok(vec![original.clone()])),
        expected: request.clone(),
    });
    let content = Arc::new(Content {
        result: Mutex::new(Ok(bytes.clone())),
    });
    let assembler = PublishedUserProfileAssembler::new(memory.clone(), content.clone());
    let profile = assembler
        .assemble_current(request.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        profile.source_versions[0].memory_version_id,
        original.version.memory_version_id
    );
    assert!(
        String::from_utf8(profile.bytes)
            .unwrap()
            .contains("Answer in Chinese")
    );
    *content.result.lock().unwrap() = Err(ContentStoreError::Unavailable);
    assert_eq!(
        assembler.assemble_current(request.clone()).await,
        Err(UserProfilePromptError::ContentStore)
    );
    assert_eq!(assembler.assemble(request.clone()).await.unwrap(), None);
    *content.result.lock().unwrap() = Ok(b"changed".to_vec());
    assert_eq!(
        assembler.assemble_current(request.clone()).await,
        Err(UserProfilePromptError::ContentIntegrity)
    );
    // A genuine current revocation reads empty even when old bytes remain.
    *content.result.lock().unwrap() = Ok(bytes);
    *memory.result.lock().unwrap() = Ok(vec![]);
    assert_eq!(assembler.assemble_current(request).await.unwrap(), None);
}

#[tokio::test]
async fn current_profile_retains_quarantine_and_does_not_read_another_principals_fact() {
    let request = request();
    let invalid = b"not a canonical profile fact";
    let mut record = hit(&request, invalid);
    let memory = Arc::new(Store {
        result: Mutex::new(Ok(vec![record.clone()])),
        expected: request.clone(),
    });
    let content = Arc::new(Content {
        result: Mutex::new(Ok(invalid.to_vec())),
    });
    let assembler = PublishedUserProfileAssembler::new(memory.clone(), content.clone());
    assert_eq!(
        assembler.assemble_current(request.clone()).await.unwrap(),
        None
    );
    record.record.scope = MemoryScope::User {
        principal_id: PrincipalId::generate(),
    };
    *memory.result.lock().unwrap() = Ok(vec![record]);
    *content.result.lock().unwrap() = Err(ContentStoreError::Unavailable);
    assert_eq!(
        assembler.assemble_current(request).await.unwrap(),
        None,
        "foreign scope must be excluded before content read"
    );
}
