use super::*;
use musteragent_definition::{AgentDefinitionStore, owned_agent::*};
use musteragent_execution_context::DialogueCursor;
use musteragent_runtime_session::*;
use std::num::NonZeroU64;

#[tokio::test]
async fn schedules_preserve_goal_authority_without_duplicate_goal_and_observe_revocation()
-> Result<(), BuildError> {
    let smoke = Smoke::start_for("p10").await?;
    let model = super::recovery::model()?;
    let source = smoke.seed(&model, false).await;
    let actor = Actor {
        tenant_id: source.tenant_id,
        principal_id: source.principal_id,
        principal_kind: PrincipalKind::User,
    };
    let agent = crate::delegation::extensions::publish(&smoke, source, &model).await?;
    let catalog = PostgresAgentDefinitionStore::new(smoke.pool.clone());
    let definition = catalog
        .load_definition(&AgentKey::new("reviewer")?)
        .await?
        .ok_or("reviewer definition")?;
    let pointer = catalog
        .load_published(agent)
        .await?
        .ok_or("reviewer pointer")?;
    let version = catalog
        .load_version(pointer.agent_version_id)
        .await?
        .ok_or("reviewer version")?;
    let now = running_loop::database_now(&smoke.pool).await;
    let space = OwnerSpaceId::generate();
    let dialogue = AgentDialogueId::generate();
    let activation = PostgresOwnedAgentStore::new(smoke.pool.clone())
        .activate(OwnedAgentActivation {
            actor,
            command_id: CommandId::generate(),
            space_id: space,
            agent: OwnedAgent {
                agent_id: agent,
                space_id: space,
                dialogue_id: dialogue,
                published_version_id: version.agent_version_id,
                status: OwnedAgentStatus::Active,
                revision: NonZeroU64::MIN,
                created_at: now,
                updated_at: now,
            },
            configuration: OwnedAgentConfiguration::initial(
                agent,
                "Reviewer".into(),
                "".into(),
                None,
                None,
            )?,
            definition: definition.clone(),
            version: version.clone(),
            published_pointer_version: NonZeroU64::MIN,
            make_default: true,
        })
        .await?;
    let goal_id = GoalId::generate();
    let pursuit = GoalPursuitId::generate();
    let scope = OwnerGoalScope {
        tenant_id: actor.tenant_id,
        owner_id: actor.principal_id,
        space_id: activation.space.space_id,
        agent_id: agent,
        dialogue_id: dialogue,
        goal_id,
    };
    let objective = smoke
        .content
        .put(musteragent_runtime_content::PutContent {
            tenant_id: actor.tenant_id,
            media_type: musteragent_content_ref::MediaType::new("text/plain")?,
            bytes: b"Keep observing a durable goal".to_vec(),
        })
        .await?;
    let goal = musteragent_goal::StandingGoal {
        input_progress: Default::default(),
        tenant_id: actor.tenant_id,
        goal_id,
        revision: 1,
        owner: actor.principal_id,
        origin: musteragent_goal::GoalOrigin::User,
        mode: musteragent_goal::StandingGoalMode::Continuous,
        creator_agent_id: None,
        target_agent_id: agent,
        conversation_id: scope.conversation_id(),
        title: "Standing source".into(),
        objective_ref: objective,
        constraints: vec![],
        completion: musteragent_goal::GoalCompletionContract {
            risk_class: musteragent_goal::GoalRiskClass::Standard,
            success_criteria: vec!["Evidence observed".into()],
            evidence_requirements: vec!["Recorded evidence".into()],
            failure_conditions: vec![],
            minimum_evidence_items: 1,
        },
        budget: musteragent_goal::GoalBudget {
            max_runs: 10,
            max_elapsed_millis: 100000,
            max_model_tokens: 100000,
            max_tool_invocations: 100,
            max_consecutive_failures: 3,
        },
        status: musteragent_goal::StandingGoalStatus::Draft,
        active_pursuit_id: None,
        created_at: now,
        updated_at: now,
    };
    let goals = PostgresStandingGoalProductStore::new(smoke.pool.clone());
    goals
        .create_draft(CreateOwnerGoalDraft {
            actor,
            command_id: CommandId::generate(),
            goal,
            binding: OwnerGoalBinding {
                scope,
                revision: 1,
                requesting_agent_id: agent,
                source_dialogue_id: dialogue,
            },
        })
        .await?;
    let application = StandingGoalProductApplication::new(Arc::new(goals), smoke.content.clone());
    application
        .change_status(ChangeStandingGoalStatus {
            tenant_id: actor.tenant_id,
            goal_id,
            principal_id: actor.principal_id,
            expected_goal_revision: 1,
            expected_pursuit_revision: None,
            new_pursuit_id: Some(pursuit),
            initial_run_id: None,
            initial_message_id: None,
            status: musteragent_goal::StandingGoalStatus::Active,
            occurred_at: now,
        })
        .await?;
    let pursuits_before: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime.goal_pursuits")
        .fetch_one(&smoke.pool)
        .await?;
    let cut = PostgresDialogueContextSourceStore::new(smoke.pool.clone())
        .cut(actor, agent, dialogue, DialogueCursor { epoch: 1, seq: 0 })
        .await?;
    let bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE runtime_snapshot_id=$1",
    )
    .bind(source.runtime_snapshot_id.into_uuid())
    .fetch_one(&smoke.pool)
    .await?;
    let mut content = musteragent_wire::ProtobufSnapshotCodec.decode_content(&bytes)?;
    let old = content.agent.persona_content_ref.clone();
    content.agent = AgentVersionSnapshot::from_resolved(&definition, &version)?;
    for section in &mut content.prompt.sections {
        if section.content_ref == old {
            section.content_ref = version.persona_content_ref.clone();
        }
    }
    let mut prompt = Vec::new();
    for section in &content.prompt.sections {
        prompt.extend(
            smoke
                .content
                .read(actor.tenant_id, &section.content_ref)
                .await?,
        );
    }
    content.prompt.assembled_hash = musteragent_content_ref::Sha256::digest(&prompt);
    content.schema_version = DottedKey::new(SNAPSHOT_SCHEMA_VERSION)?;
    content.protocol_versions.snapshot_schema = content.schema_version.clone();
    let authority = OwnerGoalSnapshotAuthority {
        goal_id,
        pursuit_id: pursuit,
        binding_revision: 1,
        permission_revision: 1,
        manifest_revision: 1,
        instructions_revision: 1,
        context_cut: cut,
    };
    content.owner_goal_authority = Some(authority.clone());
    let snapshot = RuntimeSnapshot::seal(
        source.runtime_snapshot_id,
        content,
        now,
        &musteragent_wire::ProtobufSnapshotCodec,
    )?;
    sqlx::query("UPDATE capability.runtime_snapshots SET schema_version=$2,encoded_snapshot=$3,content_hash=$4 WHERE runtime_snapshot_id=$1").bind(source.runtime_snapshot_id.into_uuid()).bind(snapshot.content.schema_version.as_str()).bind(snapshot.encoded_content).bind(snapshot.content_hash.into_bytes().as_slice()).execute(&smoke.pool).await?;
    sqlx::query("UPDATE conversation.participants SET agent_ref=$2 WHERE participant_id=$1")
        .bind(source.executor_id.into_uuid())
        .bind(agent.into_uuid())
        .execute(&smoke.pool)
        .await?;
    let create = super::recovery::command(source, now);
    let service = PostgresScheduleService::new(smoke.pool.clone());
    service.change(create.clone()).await?;
    let dispatcher = PostgresScheduleDispatcher::new(
        smoke.pool.clone(),
        smoke.content.clone(),
        Arc::new(ProtobufLedgerPayloadCodec),
    );
    let run = dispatcher
        .fire(
            create.schedule_id,
            ScheduleTriggerOwner::Cloud,
            1,
            UtcTimestamp::from_unix_millis(now.unix_millis() + 500),
        )
        .await?
        .ok_or("goal-derived schedule did not fire")?;
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT s.encoded_snapshot FROM runtime.runs r JOIN capability.runtime_snapshots s ON s.runtime_snapshot_id=r.runtime_snapshot_id WHERE r.run_id=$1").bind(run.into_uuid()).fetch_one(&smoke.pool).await?;
    assert_eq!(
        musteragent_wire::ProtobufSnapshotCodec
            .decode_content(&bytes)?
            .owner_goal_authority,
        Some(authority)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.standing_goals")
            .fetch_one(&smoke.pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtime.goal_pursuits")
            .fetch_one(&smoke.pool)
            .await?,
        pursuits_before
    );
    let provider = ControlledOpenAiChatProvider::start("127.0.0.1:0".parse()?).await?;
    let mut worker = smoke.worker("goal-derived-schedule", &model, provider.endpoint())?;
    let delivery=tokio::time::timeout(Duration::from_secs(45),async {
        loop {
            let payload:Option<Vec<u8>>=sqlx::query_scalar("SELECT payload FROM event_store.outbox WHERE topic='schedule.occurrence.ended' AND conversation_id=$1").bind(source.conversation_id.into_uuid()).fetch_optional(&smoke.pool).await?;
            if let Some(payload)=payload {return Ok::<serde_json::Value,BuildError>(serde_json::from_slice(&payload)?);}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await??;
    assert_eq!(delivery["status"], "completed", "{delivery}");
    terminate(&mut worker).await;
    provider.shutdown().await?;
    // Source revocation is checked at the next admission, not silently skipped.
    sqlx::query("UPDATE agent.owned_agent_configurations SET permission_revision=permission_revision+1 WHERE agent_id=$1").bind(agent.into_uuid()).execute(&smoke.pool).await?;
    let mut denied = create;
    denied.schedule_id = uuid::Uuid::now_v7();
    denied.command_id = CommandId::generate();
    assert_eq!(
        service.change(denied).await,
        Err(ScheduleServiceError::SourceRevoked)
    );
    std::fs::write(
        smoke.dir.join("goal-schedule-evidence.json"),
        serde_json::to_vec(
            &serde_json::json!({"goal":goal_id.to_string(),"scheduled_run":run.to_string(),"goals":1,"pursuits":pursuits_before,"source_revoked":"typed"}),
        )?,
    )?;
    Ok(())
}
