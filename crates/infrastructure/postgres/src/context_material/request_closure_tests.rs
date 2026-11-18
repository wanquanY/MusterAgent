use super::*;
use musteragent_content_ref::Sha256;
use musteragent_execution_context::DialogueCursor;
use musteragent_identity::{AgentDialogueId, AgentId, PrincipalId, TenantId};
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgConnection;

const ADMITTED_AT: i64 = 10_000;

struct Dialogue {
    cut: DialogueContextCut,
    entries: Vec<(u64, uuid::Uuid)>,
}

impl Dialogue {
    fn entry(&self, seq: u64) -> uuid::Uuid {
        self.entries.iter().find(|(s, _)| *s == seq).unwrap().1
    }

    async fn add(
        &mut self,
        db: &mut PgConnection,
        seq: u64,
        kind: &str,
        intent: uuid::Uuid,
    ) -> uuid::Uuid {
        let entry = uuid::Uuid::now_v7();
        let message = matches!(kind, "user_message" | "agent_message");
        let partner = kind == "partner_created";
        sqlx::query(
            "INSERT INTO agent.agent_dialogue_entries (entry_id,tenant_id,owner_id,dialogue_id,epoch,seq,kind,source_intent_id,content_ref,visible_revision,created_at_ms,partner_creation_command_id,partner_agent_id,partner_dialogue_id,partner_display_name,partner_description,partner_profile_revision) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8,1,1,$9,$9,$9,$10,$11,$12)",
        )
        .bind(entry)
        .bind(self.cut.tenant_id.into_uuid())
        .bind(self.cut.owner_id.into_uuid())
        .bind(self.cut.dialogue_id.into_uuid())
        .bind(seq as i64)
        .bind(kind)
        .bind(intent)
        .bind(message.then(|| vec![1_u8]))
        .bind(partner.then(uuid::Uuid::now_v7))
        .bind(partner.then_some("Partner"))
        .bind(partner.then_some(""))
        .bind(partner.then_some(1_i64))
        .execute(&mut *db)
        .await
        .unwrap();
        self.entries.push((seq, entry));
        entry
    }

    /// One task conversation per request, with a run that finished at
    /// `finished` (or is still executing when `None`).
    async fn task(&self, db: &mut PgConnection, intent: uuid::Uuid, finished: Option<i64>) {
        let conversation = uuid::Uuid::now_v7();
        sqlx::query(
            "INSERT INTO agent.dialogue_sessions (conversation_id,tenant_id,owner_id,agent_id,dialogue_id,source_dialogue_id,requesting_agent_id,source_intent_id,purpose,created_at_ms,context_epoch,context_seq,pinned_agent_version_id,permission_revision,manifest_revision,instructions_revision) VALUES ($1,$2,$3,$4,$5,$5,$4,$6,'intent_coordination',1,1,1,$1,1,1,1)",
        )
        .bind(conversation)
        .bind(self.cut.tenant_id.into_uuid())
        .bind(self.cut.owner_id.into_uuid())
        .bind(self.cut.agent_id.into_uuid())
        .bind(self.cut.dialogue_id.into_uuid())
        .bind(intent)
        .execute(&mut *db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runtime.runs (run_id,tenant_id,conversation_id,activity_id,activity_kind,executor_participant_id,requested_by_participant_id,execution_scope_kind,control_home_kind,execution_home_kind,home_epoch,runtime_snapshot_id,admitted_sequence,status,next_attempt_number,admitted_at_ms,finished_at_ms,traceparent) VALUES ($1,$2,$3,$1,1,$1,$1,1,1,1,1,$1,1,$4,1,1,$5,'00-00000000000000000000000000000001-0000000000000001-01')",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(self.cut.tenant_id.into_uuid())
        .bind(conversation)
        .bind(if finished.is_some() { 5_i16 } else { 2_i16 })
        .bind(finished)
        .execute(&mut *db)
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn every_settled_request_closes_at_its_last_entry_whatever_its_reply_shape() {
    let pg = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = pg.pool().await.expect("connect PostgreSQL");
    crate::migrate(&pool).await.expect("migrate");
    let mut db = pool.acquire().await.unwrap();
    // Only the closure columns matter here; skip unrelated owner rows.
    sqlx::query("SET session_replication_role = replica")
        .execute(&mut *db)
        .await
        .unwrap();
    let mut dialogue = Dialogue {
        cut: DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 9 },
            entry_count: 9,
            source_chain_sha256: Sha256::digest(b"cut"),
        },
        entries: Vec::new(),
    };
    let [replied, partner, unanswered, running, late, after_cut] =
        std::array::from_fn(|_| uuid::Uuid::now_v7());
    dialogue.add(&mut db, 1, "user_message", replied).await;
    dialogue.add(&mut db, 2, "agent_message", replied).await;
    dialogue.task(&mut db, replied, Some(ADMITTED_AT - 1)).await;
    dialogue.add(&mut db, 3, "user_message", partner).await;
    dialogue.add(&mut db, 4, "partner_created", partner).await;
    dialogue.task(&mut db, partner, Some(ADMITTED_AT - 1)).await;
    dialogue.add(&mut db, 5, "user_message", unanswered).await;
    dialogue.add(&mut db, 6, "user_message", running).await;
    dialogue.task(&mut db, running, None).await;
    dialogue.add(&mut db, 7, "user_message", late).await;
    dialogue.add(&mut db, 8, "agent_message", late).await;
    dialogue.task(&mut db, late, Some(ADMITTED_AT + 1)).await;
    dialogue.add(&mut db, 9, "user_message", after_cut).await;
    // A reply outside the frozen cut does not hold the request open at it.
    dialogue.add(&mut db, 10, "agent_message", after_cut).await;
    // A new run admitted after the consuming admission cannot change its frozen cut.
    dialogue.task(&mut db, after_cut, None).await;
    sqlx::query("UPDATE runtime.runs SET admitted_at_ms=$2 WHERE conversation_id=(SELECT conversation_id FROM agent.dialogue_sessions WHERE source_intent_id=$1)")
        .bind(after_cut).bind(ADMITTED_AT + 1).execute(&mut *db).await.unwrap();
    drop(db);

    let ids = dialogue
        .entries
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<_>>();
    let closed = settled(&pool, &dialogue.cut, &ids, ADMITTED_AT)
        .await
        .unwrap();
    let expected = [2, 4, 5, 9]
        .map(|seq| DialogueEntryId::from_uuid(dialogue.entry(seq)).unwrap())
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(closed, expected);

    // Only entries of the requested page are reported.
    let page = settled(&pool, &dialogue.cut, &[dialogue.entry(4)], ADMITTED_AT)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert!(
        settled(&pool, &dialogue.cut, &[], ADMITTED_AT)
            .await
            .unwrap()
            .is_empty()
    );
}
