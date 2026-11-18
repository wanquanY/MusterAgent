//! A long text-only history must not be rescanned on each new input.
use super::*;

#[tokio::test]
async fn attachment_cut_uses_partial_index_and_excludes_later_inputs() {
    let pg = IsolatedPostgres::start_with_pgvector().await.unwrap();
    let pool = pg.pool().await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = actor();
    let active = attachment_fixture::activate(&pool, owner).await;
    let store = PostgresAgentDialogueStore::new(pool.clone());
    let mut command = input(owner, active.agent.dialogue_id);
    register_content(&pool, owner, &command.content_ref).await;
    // The default activation publishes its product welcome at seq 1, so the
    // 512 text inputs occupy seq 2..=513.
    for _ in 0..512 {
        command.command_id = CommandId::generate();
        let accepted = store.accept(command.clone()).await.unwrap();
        command.observed_cursor = accepted.intent.accepted_cursor;
    }
    sqlx::query("ANALYZE agent.agent_dialogue_entries")
        .execute(&pool)
        .await
        .unwrap();
    let plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, FORMAT JSON) SELECT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries WHERE tenant_id=$1 AND owner_id=$2 AND dialogue_id=$3 AND epoch=$4 AND seq<=$5 AND input_attachment_count>0)",
    ).bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid())
        .bind(active.agent.dialogue_id.into_uuid()).bind(1_i64).bind(513_i64)
        .fetch_one(&pool).await.unwrap();
    fn find_index(value: &serde_json::Value) -> Option<&serde_json::Value> {
        if value["Index Name"] == "dialogue_input_source_cut" {
            return Some(value);
        }
        match value {
            serde_json::Value::Array(values) => values.iter().find_map(find_index),
            serde_json::Value::Object(values) => values.values().find_map(find_index),
            _ => None,
        }
    }
    let index =
        find_index(&plan).expect("unforced original planner uses the empty attachment index");
    assert_eq!(index["Actual Rows"], 0);
    eprintln!(
        "512 original text entries: {} reads no attachment rows",
        index["Node Type"]
    );

    let upload_id = "e".repeat(64);
    let content_ref = ContentRef {
        content_id: musteragent_runtime_content::upload::idempotent_content_id(
            owner,
            &musteragent_runtime_content::upload::content_upload_key(&upload_id, None).unwrap(),
        )
        .unwrap(),
        media_type: MediaType::new("text/plain").unwrap(),
        byte_length: 1,
        sha256: Sha256::digest("a"),
    };
    register_content(&pool, owner, &content_ref).await;
    command.command_id = CommandId::generate();
    command.input_attachments = vec![DialogueInputAttachment {
        attachment: musteragent_runtime_contract::AttachmentRef {
            content_ref,
            display_name: musteragent_runtime_contract::DisplayText::new("later.txt").unwrap(),
        },
        source: DialogueInputAttachmentSource::Uploaded { upload_id },
    }];
    let accepted = store.accept(command).await.unwrap();
    assert_eq!(accepted.intent.accepted_cursor.seq, 513);
    for (through, expected) in [(512_i64, false), (513, true)] {
        let actual: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries WHERE tenant_id=$1 AND owner_id=$2 AND dialogue_id=$3 AND epoch=$4 AND seq<=$5 AND input_attachment_count>0)")
            .bind(owner.tenant_id.into_uuid()).bind(owner.principal_id.into_uuid())
            .bind(active.agent.dialogue_id.into_uuid()).bind(1_i64).bind(through)
            .fetch_one(&pool).await.unwrap();
        assert_eq!(actual, expected);
    }
}
