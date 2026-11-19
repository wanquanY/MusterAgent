use super::*;
type RouteRow = (i16, Option<String>, Option<String>, Option<String>);
use musteragent_runtime_skill::*;
use musteragent_runtime_snapshot::SnapshotCodec;
use musteragent_sandbox::*;
use musteragent_wire::ProtobufSnapshotCodec;

async fn require_server_skill(smoke: &Smoke, fixture: running_loop::Fixture) {
    let lifecycle = Arc::new(PostgresSkillLifecycleStore::new(smoke.pool.clone()));
    let product =
        SkillProductApplication::new(lifecycle.clone(), lifecycle.clone(), smoke.content.clone());
    let archive = build_skill_source_archive(vec![SkillPackageSourceFile { path:"SKILL.md".into(), bytes:b"---\nname: server-fixture\ndescription: Compute the supplied result on the server.\n---\nUse the admitted Python tool to print the result.\n".to_vec() }]).expect("sandbox acceptance fixture");
    let imported = product
        .import_archive_bytes(
            ImportSkillArchive {
                tenant_id: fixture.tenant_id,
                principal_id: fixture.principal_id,
                archive_path: "fixture.zip".into(),
                source: SkillPackageSource::Local,
                source_ref: "fixture:server".into(),
                publisher_ref: "fixture-host".into(),
                suggested_name: None,
                display_name: None,
                expected_archive_hash: None,
                occurred_at: running_loop::database_now(&smoke.pool).await,
            },
            &archive,
            SkillImportLineage::OwnedIdentity,
        )
        .await
        .expect("sandbox acceptance fixture");
    let local = lifecycle
        .load_active_entries(fixture.tenant_id)
        .await
        .expect("sandbox acceptance fixture")
        .into_iter()
        .find(|entry| entry.definition.skill_version_id == imported.skill_version_id)
        .expect("sandbox acceptance fixture");
    let journal = PostgresFencedToolInvocationJournal::new(
        smoke.pool.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
        Arc::new(ProtobufLedgerPayloadCodec),
        running_loop::keyring(),
    );
    let routing = PostgresSandboxRoutingStore::new(journal, Arc::new(ProtobufSnapshotCodec));
    routing
        .publish_skill_compatibility(
            fixture.tenant_id,
            local.definition.skill_version_id,
            local.definition.instructions_hash,
            SandboxCompatibility::ServerRequired,
        )
        .await
        .expect("sandbox acceptance fixture");
    let encoded: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE runtime_snapshot_id=$1",
    )
    .bind(fixture.runtime_snapshot_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await
    .expect("sandbox acceptance fixture");
    let mut snapshot = ProtobufSnapshotCodec
        .decode_content(&encoded)
        .expect("sandbox acceptance fixture");
    let mut entries = musteragent_skill::builtin_skill_catalog_entries()
        .expect("sandbox acceptance fixture")
        .into_iter()
        .filter(|entry| {
            snapshot
                .skills
                .iter()
                .any(|s| s.skill_version_id == entry.definition.skill_version_id)
        })
        .collect::<Vec<_>>();
    snapshot.skills.push(local.definition.clone());
    snapshot
        .skills
        .sort_by(|a, b| a.skill_ref.cmp(&b.skill_ref));
    entries.push(local);
    let capabilities = snapshot
        .capabilities
        .iter()
        .map(|g| VersionedKey::new(g.key.as_str()).expect("sandbox acceptance fixture"))
        .collect();
    let skills =
        authorized_skill_catalog(&entries, &capabilities).expect("sandbox acceptance fixture");
    let discovery = snapshot
        .discovery
        .as_mut()
        .expect("sandbox acceptance fixture");
    discovery.authorized_skills = skills.definitions().to_vec();
    discovery.authorized_skill_catalog_hash = skills.catalog_hash();
    let sealed = RuntimeSnapshot::seal(
        fixture.runtime_snapshot_id,
        snapshot,
        running_loop::database_now(&smoke.pool).await,
        &ProtobufSnapshotCodec,
    )
    .expect("sandbox acceptance fixture");
    sqlx::query("UPDATE capability.runtime_snapshots SET encoded_snapshot=$2,content_hash=$3 WHERE runtime_snapshot_id=$1").bind(fixture.runtime_snapshot_id.into_uuid()).bind(sealed.encoded_content).bind(sealed.content_hash.into_bytes().to_vec()).execute(&smoke.pool).await.expect("sandbox acceptance fixture");
}

#[tokio::test]
async fn worker_persists_server_routes_and_browser_only_fails_without_fallback()
-> Result<(), BuildError> {
    let smoke = Smoke::start().await?;
    let mut model = context_snapshot::snapshot_content(
        RunId::generate(),
        musteragent_conformance::fixtures::agent::fixture_runtime_policy_content_ref()?,
    )
    .model
    .route;
    model.api_format = musteragent_model::ApiFormat::OpenAiChatCompletions;
    model.adapter_version = VersionedKey::new("openai.chat-adapter@1")?;
    model.protocol_version = VersionedKey::new("openai.chat@1")?;
    for (index, required, policy) in [
        (0, false, SandboxRoutingPolicy::PreferBrowser),
        (1, true, SandboxRoutingPolicy::PreferBrowser),
        (2, false, SandboxRoutingPolicy::BrowserOnly),
    ] {
        let fixture = smoke
            .seed_tool(
                &model,
                true,
                "data.python.run",
                b"[e2e:sandbox] Compute six times seven.",
            )
            .await;
        if required {
            require_server_skill(&smoke, fixture).await;
        }
        let journal = PostgresFencedToolInvocationJournal::new(
            smoke.pool.clone(),
            Arc::new(ProtobufLedgerPayloadCodec),
            Arc::new(ProtobufLedgerPayloadCodec),
            running_loop::keyring(),
        );
        PostgresSandboxRoutingStore::new(journal, Arc::new(ProtobufSnapshotCodec))
            .bind_run_context(
                Actor {
                    tenant_id: fixture.tenant_id,
                    principal_id: fixture.principal_id,
                    principal_kind: PrincipalKind::User,
                },
                fixture.run_id,
                SandboxClientSurface::Web,
                SandboxTrigger::Interactive,
                policy,
            )
            .await?;
        let provider = ControlledOpenAiChatProvider::start_pinned_scenario(
            "127.0.0.1:0".parse()?,
            "[e2e:sandbox]".into(),
            true,
        )
        .await?;
        let mut worker = smoke.worker_with_sandbox(
            &format!("sandbox-{index}"),
            &model,
            provider.endpoint(),
            Some(fixture),
        )?;
        provider
            .wait_for_requests(1, Duration::from_secs(30))
            .await?;
        tokio::time::timeout(Duration::from_secs(10),async {
            loop {
                let ready:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capability.browser_provider_slots WHERE tenant_id=$1 AND executor_kind='server' AND active_lease_id IS NOT NULL)").bind(fixture.tenant_id.into_uuid()).fetch_one(&smoke.pool).await.expect("sandbox acceptance fixture");
                if ready { break; } tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await?;
        provider.release()?;
        smoke.wait_completed(fixture, &mut worker).await;
        let routes:Vec<RouteRow>=sqlx::query_as("SELECT status,sandbox_executor,sandbox_reason,sandbox_compatibility FROM tool.tool_invocations WHERE run_id=$1").bind(fixture.run_id.into_uuid()).fetch_all(&smoke.pool).await?;
        use prost::Message;
        let encoded:Vec<Vec<u8>>=sqlx::query_scalar("SELECT encoded_event FROM event_store.domain_events JOIN event_store.ledger_records USING(record_id) WHERE run_id=$1").bind(fixture.run_id.into_uuid()).fetch_all(&smoke.pool).await?;
        let events = encoded
            .iter()
            .map(|bytes| musteragent_wire::wire::DomainEvent::decode(bytes.as_slice()))
            .collect::<Result<Vec<_>, _>>()?;
        let route_facts = events
            .iter()
            .filter_map(|event| match &event.event {
                Some(musteragent_wire::wire::domain_event::Event::ToolSandboxRouted(event)) => {
                    event.route.as_ref()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if index == 2 {
            assert_eq!(
                routes,
                vec![(5, None, None, None)],
                "BrowserOnly cannot manufacture a route or a success"
            );
            let jobs:i64=sqlx::query_scalar("SELECT count(*) FROM capability.browser_jobs j JOIN tool.tool_invocations t USING(invocation_id) WHERE t.run_id=$1").bind(fixture.run_id.into_uuid()).fetch_one(&smoke.pool).await?;
            assert_eq!(jobs, 0);
            assert!(
                route_facts.is_empty(),
                "rejected policy cannot publish a selected executor"
            );
            let failure = events
                .iter()
                .find_map(|event| match &event.event {
                    Some(musteragent_wire::wire::domain_event::Event::ToolInvocationFailed(
                        event,
                    )) => event.error.as_ref(),
                    _ => None,
                })
                .expect("typed sandbox failure event");
            // No online browser client is retryable once the page reopens,
            // and carries a stable code plus the host-configurable text.
            assert_eq!(
                failure.code(),
                musteragent_wire::wire::ErrorCode::BrowserExecutionUnavailable
            );
            assert!(failure.retryable);
            assert_eq!(
                failure.safe_message,
                musteragent_tool_executor::BrowserUnavailableNotice::DEFAULT_TEXT
            );
        } else {
            smoke.assert_tool_completion(fixture).await;
            let reason = if required {
                SandboxRouteReason::CompatibilityRequiresServer
            } else {
                SandboxRouteReason::NoOnlineBrowserClient
            };
            assert_eq!(route_facts.len(), 1, "one immutable ledger route");
            assert_eq!(route_facts[0].executor, "server");
            assert_eq!(route_facts[0].reason, reason.as_str());
            assert_eq!(
                route_facts[0].compatibility,
                if required {
                    "server-required"
                } else {
                    "browser-compatible"
                }
            );
            assert_eq!(
                routes,
                vec![(
                    4,
                    Some("server".into()),
                    Some(reason.as_str().into()),
                    Some(
                        if required {
                            "server-required"
                        } else {
                            "browser-compatible"
                        }
                        .into()
                    )
                )]
            );
        }
        std::fs::write(
            smoke.dir.join(format!("sandbox-{index}-route.json")),
            serde_json::to_vec_pretty(&routes)?,
        )?;
        terminate(&mut worker).await;
        provider.shutdown().await?;
    }
    Ok(())
}
