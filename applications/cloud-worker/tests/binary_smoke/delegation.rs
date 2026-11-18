use super::*;
#[path = "delegation_extensions.rs"]
pub(super) mod extensions;
const TOOLS: &[&str] = &[
    "delegation.spawn",
    "delegation.send",
    "delegation.receive",
    "delegation.list",
    "delegation.inspect",
    "system.time.current",
];

async fn running_child(
    smoke: &Smoke,
    parent: running_loop::Fixture,
    provider: &ControlledOpenAiChatProvider,
) -> Result<uuid::Uuid, BuildError> {
    tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        loop {
            let child:Option<uuid::Uuid>=sqlx::query_scalar("SELECT d.child_run_id FROM runtime.delegations d JOIN runtime.model_invocations i ON i.run_id=d.child_run_id WHERE d.parent_run_id=$1 AND i.status=1 LIMIT 1").bind(parent.run_id.into_uuid()).fetch_optional(&smoke.pool).await?;
            if let Some(child) = child
                && provider.request_prompts().iter().any(|p| p.contains("[e2e:child]"))
            {
                return Ok::<_, BuildError>(child);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await?
}

fn service(smoke: &Smoke) -> musteragent_postgres::PostgresCloudDelegation {
    musteragent_postgres::PostgresCloudDelegation::new(
        smoke.pool.clone(),
        smoke.content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        running_loop::keyring(),
        Arc::new(musteragent_execution_liveness_io::SystemLivenessHomeClock::default()),
    )
}

#[tokio::test]
async fn depth_and_concurrency_denials_are_visible_to_the_parent_model() -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let parent = smoke
        .seed_tools_with_limits(
            &model,
            false,
            TOOLS,
            b"[e2e:limit-parent] Exercise both limits and report them.",
            Some((1, 1)),
            None,
        )
        .await;
    let mut worker = smoke.worker("delegation-limits", &model, provider.endpoint())?;
    smoke.wait_completed(parent, &mut worker).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.delegations")
            .fetch_one(&smoke.pool)
            .await?,
        1
    );
    let failures:Vec<(String,Vec<u8>)>=sqlx::query_as("SELECT tool_ref,terminal_error FROM tool.tool_invocations WHERE status=5 ORDER BY started_at_ms").fetch_all(&smoke.pool).await?;
    assert_eq!(failures.len(), 2, "both denials must be durable failures");
    let output = parent_text(&smoke, parent).await?;
    assert!(
        output.contains(
            "Parent observed delegation.concurrency_limit and child delegation.depth_limit"
        ),
        "{output}"
    );
    std::fs::write(smoke.dir.join("typed-limits-evidence.txt"), output)?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

async fn parent_text(smoke: &Smoke, parent: running_loop::Fixture) -> Result<String, BuildError> {
    let rows:Vec<(uuid::Uuid,String,i64,Vec<u8>)>=sqlx::query_as("SELECT c.content_id,c.media_type,c.byte_length::bigint,c.content_hash FROM runtime.model_invocations i JOIN artifact.content_refs c ON c.content_id=i.response_content_id WHERE i.run_id=$1 ORDER BY i.started_at_ms").bind(parent.run_id.into_uuid()).fetch_all(&smoke.pool).await?;
    let mut text = String::new();
    for (id, media, len, hash) in rows {
        let reference = musteragent_content_ref::ContentRef {
            content_id: ContentId::from_uuid(id)?,
            media_type: musteragent_content_ref::MediaType::new(media)?,
            byte_length: len as u64,
            sha256: musteragent_content_ref::Sha256::from_bytes(hash)?,
        };
        text.push_str(std::str::from_utf8(
            &smoke.content.read(parent.tenant_id, &reference).await?,
        )?);
    }
    Ok(text)
}

#[tokio::test]
async fn child_failure_is_delivered_and_used_by_parent() -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let parent = smoke
        .seed_tools_with_limits(
            &model,
            false,
            TOOLS,
            b"[e2e:failure-parent] Delegate and report a child failure.",
            None,
            Some(musteragent_goal::RetrySpec {
                max_attempts: 2,
                base_backoff_millis: 100,
                factor: 2,
                max_backoff_millis: 500,
            }),
        )
        .await;
    let mut worker = smoke.worker("delegation-child-failed", &model, provider.endpoint())?;
    smoke.wait_completed(parent, &mut worker).await;
    assert_eq!(
        sqlx::query_scalar::<_, i16>(
            "SELECT status FROM runtime.delegations WHERE parent_run_id=$1"
        )
        .bind(parent.run_id.into_uuid())
        .fetch_one(&smoke.pool)
        .await?,
        3
    );
    let text = parent_text(&smoke, parent).await?;
    assert!(
        text.contains("Parent observed child failure and reported it"),
        "{text}"
    );
    std::fs::write(smoke.dir.join("child-failure-evidence.txt"), text)?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn parent_stop_atomically_cascades_through_waiting_child_to_grandchild()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start_paused_marker(
        "127.0.0.1:0".parse()?,
        "[e2e:tree-grandchild]".into(),
    )
    .await?;
    let parent = smoke
        .seed_tools_with_limits(
            &model,
            false,
            TOOLS,
            b"[e2e:tree-parent] Spawn a tree.",
            Some((3, 2)),
            None,
        )
        .await;
    let mut worker = smoke.worker("delegation-tree-stop", &model, provider.endpoint())?;
    tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        while !provider
            .request_prompts()
            .iter()
            .any(|p| p.contains("[e2e:tree-grandchild]"))
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    service(&smoke)
        .stop_run(
            Actor {
                tenant_id: parent.tenant_id,
                principal_id: parent.principal_id,
                principal_kind: PrincipalKind::User,
            },
            parent.run_id,
            CommandId::generate(),
            running_loop::database_now(&smoke.pool).await,
        )
        .await?;
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM runtime.runs WHERE conversation_id=$1 AND cancel_requested_at_ms IS NOT NULL").bind(parent.conversation_id.into_uuid()).fetch_one(&smoke.pool).await?,3);
    provider.release()?;
    tokio::time::timeout(musteragent_testkit::integration_timeout(), async {
        loop {
            let ended: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM runtime.runs WHERE conversation_id=$1 AND status=7",
            )
            .bind(parent.conversation_id.into_uuid())
            .fetch_one(&smoke.pool)
            .await?;
            if ended == 3 {
                return Ok::<_, BuildError>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    std::fs::write(
        smoke.dir.join("transitive-stop-evidence.json"),
        serde_json::to_vec(
            &serde_json::json!({"parent":parent.run_id.to_string(),"cancelled_runs":3}),
        )?,
    )?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stopping_parent_mid_child_cascades_with_durable_child_failure_report()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start_paused_marker(
        "127.0.0.1:0".parse()?,
        "[e2e:child]".into(),
    )
    .await?;
    let parent = smoke
        .seed_tools(
            &model,
            false,
            TOOLS,
            b"[e2e:parent] Delegate and use result.",
        )
        .await;
    let mut worker = smoke.worker("delegation-stop", &model, provider.endpoint())?;
    let child = running_child(&smoke, parent, &provider).await?;
    service(&smoke)
        .stop_run(
            Actor {
                tenant_id: parent.tenant_id,
                principal_id: parent.principal_id,
                principal_kind: PrincipalKind::User,
            },
            parent.run_id,
            CommandId::generate(),
            running_loop::database_now(&smoke.pool).await,
        )
        .await?;
    // Both authorities close in the same cancellation transaction, before the
    // remote model is released or the reconciliation timer observes the stop.
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM runtime.runs WHERE (run_id=$1 OR run_id=$2) AND cancel_requested_at_ms IS NOT NULL").bind(parent.run_id.into_uuid()).bind(child).fetch_one(&smoke.pool).await?,2);
    provider.release()?;
    tokio::time::timeout(musteragent_testkit::integration_timeout(),async {
        loop {
            let ended:i64=sqlx::query_scalar("SELECT count(*) FROM runtime.runs WHERE (run_id=$1 OR run_id=$2) AND status=7").bind(parent.run_id.into_uuid()).bind(child).fetch_one(&smoke.pool).await?;
            let report:i64=sqlx::query_scalar("SELECT count(*) FROM runtime.delegations WHERE child_run_id=$1 AND status=4 AND report_content_id IS NOT NULL").bind(child).fetch_one(&smoke.pool).await?;
            if ended==2 && report==1 {return Ok::<_,BuildError>(());}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await??;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1 AND status=4"
        )
        .bind(child)
        .fetch_one(&smoke.pool)
        .await?,
        0
    );
    std::fs::write(
        smoke.dir.join("cascade-evidence.json"),
        serde_json::to_vec(
            &serde_json::json!({"parent":parent.run_id.to_string(),"child":child,"both_cancelled":true,"child_report":true}),
        )?,
    )?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn replacement_worker_completes_parent_and_exactly_one_child_after_sigkill()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start_paused_marker(
        "127.0.0.1:0".parse()?,
        "[e2e:child]".into(),
    )
    .await?;
    let parent = smoke
        .seed_tools(
            &model,
            false,
            TOOLS,
            b"[e2e:parent] Delegate and use result.",
        )
        .await;
    let mut doomed = smoke.worker("delegation-killed", &model, provider.endpoint())?;
    let child = running_child(&smoke, parent, &provider).await?;
    doomed.kill().await?;
    doomed.wait().await?;
    provider.release()?;
    let mut replacement = smoke.worker("delegation-replacement", &model, provider.endpoint())?;
    smoke.wait_completed(parent, &mut replacement).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.delegations WHERE parent_run_id=$1"
        )
        .bind(parent.run_id.into_uuid())
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i16>("SELECT status FROM runtime.runs WHERE run_id=$1")
            .bind(child)
            .fetch_one(&smoke.pool)
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(DISTINCT worker_id) FROM runtime.attempts WHERE run_id=$1"
        )
        .bind(child)
        .fetch_one(&smoke.pool)
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.delegation_events WHERE kind='subagent_spawned'"
        )
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.delegation_events WHERE kind='subagent_ended'"
        )
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    std::fs::write(
        smoke.dir.join("child-recovery-evidence.json"),
        serde_json::to_vec(
            &serde_json::json!({"parent":parent.run_id.to_string(),"child":child,"distinct_child_workers":2,"child_count":1}),
        )?,
    )?;
    terminate(&mut replacement).await;
    provider.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn real_parent_and_child_exchange_messages_tools_and_result() -> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let parent = smoke
        .seed_tools(
            &model,
            false,
            &[
                "delegation.spawn",
                "delegation.send",
                "delegation.receive",
                "delegation.list",
                "delegation.inspect",
                "system.time.current",
            ],
            b"[e2e:parent] Delegate a check, exchange messages and use its result.",
        )
        .await;
    let mut worker = smoke.worker("delegation-roundtrip", &model, provider.endpoint())?;
    smoke.wait_completed(parent, &mut worker).await;
    let rows: Vec<(uuid::Uuid, i16)> = sqlx::query_as(
        "SELECT child_run_id,status FROM runtime.delegations WHERE parent_run_id=$1",
    )
    .bind(parent.run_id.into_uuid())
    .fetch_all(&smoke.pool)
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, 2);
    let child = rows[0].0;
    let parent_bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE runtime_snapshot_id=$1",
    )
    .bind(parent.runtime_snapshot_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    let child_bytes:Vec<u8>=sqlx::query_scalar("SELECT s.encoded_snapshot FROM runtime.runs r JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1").bind(child).fetch_one(&smoke.pool).await?;
    let ceiling = musteragent_wire::ProtobufSnapshotCodec.decode_content(&parent_bytes)?;
    let derived = musteragent_wire::ProtobufSnapshotCodec.decode_content(&child_bytes)?;
    assert_eq!(derived.policy, ceiling.policy);
    assert_eq!(derived.execution_policy, ceiling.execution_policy);
    assert_eq!(derived.model.route, ceiling.model.route);
    assert!(
        derived
            .capabilities
            .iter()
            .all(|cap| ceiling.capabilities.contains(cap))
    );
    assert!(
        derived
            .tools
            .iter()
            .all(|tool| ceiling.tools.contains(tool))
    );
    assert!(
        derived
            .capabilities
            .iter()
            .all(|cap| cap.location != musteragent_capability::CapabilityLocation::Device)
    );

    let usages:Vec<(uuid::Uuid,Option<uuid::Uuid>,Option<uuid::Uuid>)>=sqlx::query_as("SELECT run_id,parent_run_id,billing_subject FROM runtime.model_usage_facts WHERE run_id=$1 OR run_id=$2").bind(parent.run_id.into_uuid()).bind(child).fetch_all(&smoke.pool).await?;
    assert!(usages.iter().any(|u| u.0 == child));
    assert!(usages.iter().any(|u| u.0 == parent.run_id.into_uuid()));
    for (run, linked, billing) in usages {
        assert_eq!(billing, Some(parent.principal_id.into_uuid()));
        assert_eq!(linked, (run == child).then_some(parent.run_id.into_uuid()));
    }
    let inspected:i64=sqlx::query_scalar("SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1 AND tool_ref IN('delegation.list@1','delegation.inspect@2') AND status=4").bind(parent.run_id.into_uuid()).fetch_one(&smoke.pool).await?;
    assert_eq!(inspected, 2);

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM tool.tool_invocations WHERE run_id=$1 AND status=4"
        )
        .bind(child)
        .fetch_one(&smoke.pool)
        .await?,
        3
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM runtime.delegation_messages WHERE sender_run_id=$1 OR receiver_run_id=$1").bind(child).fetch_one(&smoke.pool).await?,2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.delegation_deliveries WHERE parent_run_id=$1 AND state=3"
        )
        .bind(parent.run_id.into_uuid())
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    let reports:Vec<Vec<u8>>=sqlx::query_scalar("SELECT payload FROM event_store.outbox WHERE conversation_id=$1 AND topic='subagent_ended'").bind(parent.conversation_id.into_uuid()).fetch_all(&smoke.pool).await?;
    assert_eq!(reports.len(), 1);
    assert!(std::str::from_utf8(&reports[0])?.contains("child_result_42"));
    std::fs::write(smoke.dir.join("delegation-result.json"), &reports[0])?;
    let parent_responses:Vec<(uuid::Uuid,String,i64,Vec<u8>)>=sqlx::query_as("SELECT c.content_id,c.media_type,c.byte_length::bigint,c.content_hash FROM runtime.model_invocations i JOIN artifact.content_refs c ON c.content_id=i.response_content_id WHERE i.run_id=$1 ORDER BY i.started_at_ms DESC").bind(parent.run_id.into_uuid()).fetch_all(&smoke.pool).await?;
    let mut used = false;
    for (id, media, len, hash) in parent_responses {
        let r = musteragent_content_ref::ContentRef {
            content_id: ContentId::from_uuid(id)?,
            media_type: musteragent_content_ref::MediaType::new(media)?,
            byte_length: len as u64,
            sha256: musteragent_content_ref::Sha256::from_bytes(hash)?,
        };
        used |= smoke
            .content
            .read(parent.tenant_id, &r)
            .await?
            .windows(b"Parent used child result: 42".len())
            .any(|s| s == b"Parent used child result: 42");
    }
    std::fs::write(
        smoke.dir.join("model-prompts.json"),
        serde_json::to_vec_pretty(&provider.request_prompts())?,
    )?;
    assert!(
        used,
        "parent must actually consume the delivered result; {}",
        smoke.dir.display()
    );
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}

fn model() -> Result<musteragent_model::ModelRoute, BuildError> {
    let mut model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    model.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    model.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    model.protocol_version = VersionedKey::new("openai.chat@1")?;
    Ok(model)
}
