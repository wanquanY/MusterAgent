use super::*;
use musteragent_definition::*;
use musteragent_extension::*;
use musteragent_runtime_content::PutContent;
use musteragent_runtime_contract::ApplicationFuture;
use sqlx::Row;
use std::sync::Mutex;

struct Extension {
    log: Arc<Mutex<Vec<String>>>,
}
impl KernelExtension for Extension {
    fn id(&self) -> &str {
        "acceptance.child-owner"
    }
    fn lifecycle<'a>(
        &'a self,
        _: &'a ExtensionContext,
        event: &'a LifecycleEvent,
        api: &'a RuntimeApi,
    ) -> ApplicationFuture<'a, Result<(), HookError>> {
        Box::pin(async move {
            self.log
                .lock()
                .expect("hook log")
                .push(format!("{:?}", event.hook()));
            if matches!(event, LifecycleEvent::AgentStart) {
                let request = SubagentRequest {
                    agent: "reviewer".into(),
                    task: "[e2e:time] Read time under the reviewer's prompt.".into(),
                    idempotency_key: "one-child".into(),
                };
                let first = api
                    .spawn_subagent(request.clone())
                    .await
                    .map_err(|e| HookError::new("spawn", e.to_string()))?;
                let again = api
                    .spawn_subagent(request)
                    .await
                    .map_err(|e| HookError::new("replay", e.to_string()))?;
                if first.run_id != again.run_id {
                    return Err(HookError::new("duplicate", "child identity changed"));
                }
            }
            Ok(())
        })
    }
}

pub(crate) async fn publish(
    smoke: &Smoke,
    parent: running_loop::Fixture,
    model: &musteragent_model::ModelRoute,
) -> Result<AgentId, BuildError> {
    publish_definition(smoke, parent, model, "reviewer", false).await
}

async fn publish_definition(
    smoke: &Smoke,
    parent: running_loop::Fixture,
    model: &musteragent_model::ModelRoute,
    key: &str,
    different_policy: bool,
) -> Result<AgentId, BuildError> {
    let mut definition = musteragent_conformance::fixtures::agent::system_definition();
    definition.agent_id = AgentId::generate();
    definition.agent_key = AgentKey::new(key)?;
    definition.display_name = "Private reviewer".into();
    definition.kind = AgentKind::Custom;
    definition.system_slot = None;
    definition.built_in = false;
    definition.default_for_product = false;
    definition.deletable = true;
    definition.replaceable = true;
    let mut version = musteragent_conformance::fixtures::agent::fixture_version_v1();
    version.agent_id = definition.agent_id;
    version.agent_version_id = AgentVersionId::generate();
    version.supersedes = None;
    version.version = VersionNumber::new(1)?;
    version.persona_content_ref = smoke
        .content
        .put(PutContent {
            tenant_id: parent.tenant_id,
            media_type: musteragent_content_ref::MediaType::new("text/markdown")?,
            bytes: b"Private reviewer system prompt: report exact evidence.".to_vec(),
        })
        .await?;
    let policy_bytes: &[u8] = if different_policy {
        b"A different policy that is not the parent ceiling"
    } else {
        b"synthetic runtime policy"
    };
    if different_policy {
        version.policy_ref = VersionedKey::new("runtime.policy.alternative@1")?;
        version.policy_hash = musteragent_content_ref::Sha256::digest(policy_bytes);
    }
    let material = version.material.as_mut().expect("complete version");
    material.policy_content_ref = smoke
        .content
        .put(PutContent {
            tenant_id: parent.tenant_id,
            media_type: material.policy_content_ref.media_type.clone(),
            bytes: policy_bytes.to_vec(),
        })
        .await?;
    material.memory_policy_content_ref = smoke
        .content
        .put(PutContent {
            tenant_id: parent.tenant_id,
            media_type: material.memory_policy_content_ref.media_type.clone(),
            bytes: b"synthetic memory policy".to_vec(),
        })
        .await?;
    material.manifest_ref = smoke
        .content
        .put(PutContent {
            tenant_id: parent.tenant_id,
            media_type: material.manifest_ref.media_type.clone(),
            bytes: b"synthetic Agent manifest v7".to_vec(),
        })
        .await?;
    version.validate_complete_for_publication()?;
    let store = PostgresAgentDefinitionStore::new(smoke.pool.clone());
    assert!(matches!(
        store
            .insert_definition(definition.clone())
            .await
            .expect("insert reviewer definition"),
        DefinitionInsertOutcome::Inserted
    ));
    assert!(matches!(
        store
            .insert_version(version.clone())
            .await
            .expect("insert reviewer version"),
        VersionInsertOutcome::Inserted
    ));
    let settings = SubagentVersionSettings {
        model_entry_id: model.model_entry_id.to_string(),
        deny_tools: vec!["delegation.spawn".into()],
        hidden: true,
    };
    store
        .configure_subagent_version(version.agent_version_id, settings.clone())
        .await
        .expect("configure reviewer");
    assert!(matches!(
        store
            .compare_and_swap_published(
                definition.agent_id,
                None,
                PublishedAgentVersionPointer {
                    agent_id: definition.agent_id,
                    agent_version_id: version.agent_version_id,
                    version: 1,
                    published_at: version.published_at,
                    published_by: parent.principal_id
                }
            )
            .await
            .expect("publish reviewer"),
        PublishedPointerCasOutcome::Swapped(_)
    ));
    assert_eq!(
        store
            .subagent_version_settings(version.agent_version_id)
            .await?,
        Some(settings.clone())
    );
    assert!(
        store
            .configure_subagent_version(version.agent_version_id, settings)
            .await
            .is_err(),
        "published settings are immutable"
    );
    Ok(definition.agent_id)
}

#[tokio::test]
async fn real_extension_spawner_uses_hidden_definition_and_dispatches_both_lifecycle_hooks()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = model()?;
    let provider = ControlledOpenAiChatProvider::start_paused("127.0.0.1:0".parse()?).await?;
    let parent = smoke
        .seed_tools(
            &model,
            false,
            TOOLS,
            b"[e2e:time] Read the time and summarize any child report.",
        )
        .await;
    let agent = publish(&smoke, parent, &model).await?;
    publish_definition(&smoke, parent, &model, "different-policy", true).await?;
    let mut worker = smoke.worker("extension-child", &model, provider.endpoint())?;
    provider
        .wait_for_requests(1, Duration::from_secs(30))
        .await?;
    let row=sqlx::query("SELECT * FROM runtime.attempts WHERE run_id=$1 AND status=1 ORDER BY attempt_number DESC LIMIT 1").bind(parent.run_id.into_uuid()).fetch_one(&smoke.pool).await?;
    let context = ExtensionContext {
        run: running_loop::run_context(parent),
        attempt: musteragent_run::AttemptContext {
            run_id: parent.run_id,
            attempt_id: AttemptId::from_uuid(row.try_get("attempt_id")?)?,
            attempt_number: AttemptNumber::new(row.try_get::<i32, _>("attempt_number")? as u32)?,
            worker_id: musteragent_run::WorkerId::new(row.try_get::<String, _>("worker_id")?)?,
            lease_id: LeaseId::from_uuid(row.try_get("lease_id")?)?,
            fencing_token: FencingToken::new(row.try_get::<i64, _>("fencing_token")? as u64)?,
            lease_expires_at: UtcTimestamp::from_unix_millis(row.try_get("lease_expires_at_ms")?),
        },
        workspace_dir: smoke.dir.join("extension-work"),
        session_key: parent.conversation_id.to_string(),
        channel: None,
        sandboxed: false,
        ingress: None,
    };
    std::fs::create_dir_all(&context.workspace_dir)?;
    let controller = service(&smoke);
    let restricted = controller.restricted_spawner(["reviewer".into(), "different-policy".into()]);
    assert!(
        restricted
            .spawn(
                &context,
                SubagentRequest {
                    agent: "not-configured".into(),
                    task: "denied".into(),
                    idempotency_key: "denied".into()
                }
            )
            .await
            .is_err()
    );
    let denial = restricted
        .spawn(
            &context,
            SubagentRequest {
                agent: "different-policy".into(),
                task: "Must retain the parent permission ceiling".into(),
                idempotency_key: "different-policy".into(),
            },
        )
        .await
        .expect_err("an unproven policy cannot replace the parent ceiling");
    assert_eq!(denial.code, "delegation.permission_denied");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.delegations WHERE parent_run_id=$1"
        )
        .bind(parent.run_id.into_uuid())
        .fetch_one(&smoke.pool)
        .await?,
        0
    );
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ExtensionRegistry::default();
    registry.register_extension(
        Arc::new(Extension { log: log.clone() }),
        serde_json::json!({}),
    )?;
    let host = ExtensionHost::new(
        registry,
        Arc::new(PostgresExtensionJournal::new(smoke.pool.clone())),
        RuntimePorts {
            content: smoke.content.clone(),
            history: Arc::new(PostgresExtensionToolHistory::new(
                smoke.pool.clone(),
                smoke.content.clone(),
            )),
            state_root: smoke.dir.join("extension-state"),
            subagents: Some(restricted),
        },
        Default::default(),
    )?;
    let failures = host.lifecycle(&context, LifecycleEvent::AgentStart).await?;
    assert!(failures.is_empty(), "{failures:?}");
    controller
        .dispatch_lifecycle(
            &host,
            &smoke.dir,
            running_loop::database_now(&smoke.pool).await,
        )
        .await?;
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT s.encoded_snapshot FROM runtime.delegations d JOIN runtime.runs r ON r.run_id=d.child_run_id JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE d.parent_run_id=$1").bind(parent.run_id.into_uuid()).fetch_one(&smoke.pool).await?;
    let snapshot = musteragent_wire::ProtobufSnapshotCodec.decode_content(&bytes)?;
    assert_eq!(snapshot.agent.agent_id, agent);
    assert_eq!(snapshot.model.route.model_entry_id, model.model_entry_id);
    assert!(
        !snapshot
            .tools
            .iter()
            .any(|t| t.manifest.key.logical_key() == "delegation.spawn")
    );
    assert_eq!(
        std::str::from_utf8(
            &smoke
                .content
                .read(parent.tenant_id, &snapshot.agent.persona_content_ref)
                .await?
        )?,
        "Private reviewer system prompt: report exact evidence."
    );
    let hidden:bool=sqlx::query_scalar("SELECT (settings->>'hidden')::boolean FROM agent.subagent_version_settings WHERE agent_version_id=$1").bind(snapshot.agent.agent_version_id.into_uuid()).fetch_one(&smoke.pool).await?;
    assert!(hidden);
    provider.release()?;
    smoke.wait_completed(parent, &mut worker).await;
    controller
        .tick(running_loop::database_now(&smoke.pool).await)
        .await?;
    controller
        .dispatch_lifecycle(
            &host,
            &smoke.dir,
            running_loop::database_now(&smoke.pool).await,
        )
        .await?;
    let events = log.lock().expect("hook log").clone();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.contains("SubagentSpawned"))
            .count(),
        1,
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.contains("SubagentEnded"))
            .count(),
        1,
        "{events:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.delegations WHERE parent_run_id=$1"
        )
        .bind(parent.run_id.into_uuid())
        .fetch_one(&smoke.pool)
        .await?,
        1
    );
    std::fs::write(
        smoke.dir.join("extension-hooks.json"),
        serde_json::to_vec(&events)?,
    )?;
    terminate(&mut worker).await;
    provider.shutdown().await?;
    Ok(())
}
