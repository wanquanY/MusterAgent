use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, IdempotencyKey, MemoryId, MemoryVersionId};

fn fixture() -> (ListAgentMemoryDrafts, Vec<AgentMemoryDraft>) {
    let query = ListAgentMemoryDrafts {
        tenant_id: TenantId::generate(),
        owner_principal_id: PrincipalId::generate(),
        agent_id: AgentId::generate(),
        status: AgentMemoryDraftStatus::Draft,
        limit: 2,
        cursor: None,
    };
    let mut drafts: Vec<_> = (0..3)
        .map(|index| AgentMemoryDraft {
            draft_id: MemoryDraftId::generate(),
            tenant_id: query.tenant_id,
            owner_principal_id: query.owner_principal_id,
            agent_id: query.agent_id,
            memory_id: MemoryId::generate(),
            baseline_memory_version_id: MemoryVersionId::generate(),
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(crate::EXPERIENCE_CLAIM_MEDIA_TYPE).expect("media"),
                byte_length: 1,
                sha256: Sha256::digest(b"x"),
            },
            status: query.status,
            proposal_id: None,
            conflicting_memory_version_id: None,
            supersedes_draft_id: None,
            idempotency_key: IdempotencyKey::new(format!("draft:{index}")).expect("key"),
            created_at: UtcTimestamp::from_unix_millis(10),
            updated_at: UtcTimestamp::from_unix_millis(20),
        })
        .collect();
    drafts.sort_by_key(|draft| draft.draft_id);
    (query, drafts)
}

#[test]
fn page_exposes_continuation_without_duplicates_at_equal_timestamps() {
    let (mut query, drafts) = fixture();
    let first = AgentMemoryDraftPage::from_sorted_drafts(&query, drafts.clone()).expect("first");
    assert_eq!(first.drafts, drafts[..2]);
    query.cursor = first.next_cursor;
    assert_eq!(
        query.cursor.as_ref().expect("continuation").draft_id,
        drafts[1].draft_id
    );
    let next =
        AgentMemoryDraftPage::from_sorted_drafts(&query, drafts[2..].to_vec()).expect("next");
    assert_eq!(next.drafts, drafts[2..]);
    assert!(next.next_cursor.is_none());
    assert!(AgentMemoryDraftPage::from_sorted_drafts(&query, drafts[1..].to_vec()).is_err());
    assert!(
        AgentMemoryDraftPage::from_sorted_drafts(&query, vec![])
            .expect("empty")
            .next_cursor
            .is_none()
    );
}

#[test]
fn query_rejects_unbounded_or_cross_scope_continuations() {
    let (mut query, drafts) = fixture();
    query.cursor = AgentMemoryDraftPage::from_sorted_drafts(&query, drafts)
        .expect("page")
        .next_cursor;
    for limit in [0, 51, u32::MAX] {
        assert_eq!(
            ListAgentMemoryDrafts {
                limit,
                ..query.clone()
            }
            .validate(),
            Err(AgentMemoryDraftError::InvalidListQuery)
        );
    }
    for altered in [
        ListAgentMemoryDrafts {
            tenant_id: TenantId::generate(),
            ..query.clone()
        },
        ListAgentMemoryDrafts {
            owner_principal_id: PrincipalId::generate(),
            ..query.clone()
        },
        ListAgentMemoryDrafts {
            agent_id: AgentId::generate(),
            ..query.clone()
        },
        ListAgentMemoryDrafts {
            status: AgentMemoryDraftStatus::Conflict,
            ..query.clone()
        },
    ] {
        assert_eq!(
            altered.validate(),
            Err(AgentMemoryDraftError::InvalidListQuery)
        );
    }
    query.limit = 50;
    assert_eq!(query.validate(), Ok(()));
}

#[test]
fn page_validates_scope_state_order_and_store_read_bound() {
    let (query, drafts) = fixture();
    for altered in [
        AgentMemoryDraft {
            tenant_id: TenantId::generate(),
            ..drafts[0].clone()
        },
        AgentMemoryDraft {
            owner_principal_id: PrincipalId::generate(),
            ..drafts[0].clone()
        },
        AgentMemoryDraft {
            agent_id: AgentId::generate(),
            ..drafts[0].clone()
        },
        AgentMemoryDraft {
            status: AgentMemoryDraftStatus::Abandoned,
            ..drafts[0].clone()
        },
    ] {
        assert_eq!(
            AgentMemoryDraftPage::from_sorted_drafts(&query, vec![altered]),
            Err(AgentMemoryDraftError::InvalidListPage)
        );
    }
    let mut too_many = drafts.clone();
    too_many.push(drafts[2].clone());
    assert!(AgentMemoryDraftPage::from_sorted_drafts(&query, too_many).is_err());
    let mut reverse = drafts.clone();
    reverse.reverse();
    assert!(AgentMemoryDraftPage::from_sorted_drafts(&query, reverse).is_err());
    let mut mixed_time = drafts;
    mixed_time[0].updated_at = UtcTimestamp::from_unix_millis(30);
    mixed_time[2].updated_at = UtcTimestamp::from_unix_millis(10);
    assert!(AgentMemoryDraftPage::from_sorted_drafts(&query, mixed_time).is_ok());
}
