use super::*;
mod acknowledgement;
mod delivery;

impl PostgresNativeDialogueInteractionStore {
    pub(super) async fn read_control_inner(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        command: CommandId,
    ) -> Result<DialogueInteractionControlReceipt, RuntimeError> {
        let mut conn = self.database.pool().acquire().await.map_err(database)?;
        let current = entry_row(&mut conn, actor, dialogue, entry, false)
            .await?
            .ok_or_else(|| fail(ErrorCode::PolicyDenied))?;
        let projection = row_projection(&current)?;
        let row = command_row(&mut conn, actor, command, false)
            .await?
            .ok_or_else(|| fail(ErrorCode::InteractionNotFound))?;
        let bytes: Vec<u8> = row.try_get("command_bytes").map_err(database)?;
        let stored = decode_command(&bytes)?;
        if stored.target.dialogue_id != dialogue
            || stored.target.entry_id != entry
            || stored.interaction_id != projection.interaction.interaction_id
            || stored.admission != projection.admission
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        control_receipt(&row)
    }

    pub(super) async fn read_inner(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        correlation: CorrelationId,
    ) -> Result<Option<DialogueInteractionView>, RuntimeError> {
        use DialogueInteractionEligibility as Eligibility;
        let (row, start_time) = {
            let mut conn = self.database.pool().acquire().await.map_err(database)?;
            let Some(row) = entry_row(&mut conn, actor, dialogue, entry, false).await? else {
                return Ok(None);
            };
            (
                row,
                crate::ledger::database_now_ms(&mut conn)
                    .await
                    .map_err(database)?,
            )
        };
        let p = row_projection(&row)?;
        let (eligibility, actions) = if !p.control_open
            || p.interaction.status != InteractionStatus::Pending
            || p.interaction.expires_at.is_some_and(|t| t <= start_time)
        {
            (Eligibility::Closed, vec![])
        } else {
            let started = std::time::Instant::now();
            // Leave part of the public read deadline for returning the card.
            // Only this observation times out; no decision is attempted.
            const READ_AUTHORITY_BUDGET: std::time::Duration = std::time::Duration::from_secs(4);
            let proof = tokio::time::timeout(
                READ_AUTHORITY_BUDGET,
                self.qualify_control(actor, row_id(&row, "agent_id")?, &p, correlation),
            )
            .await
            .unwrap_or_else(|_| Err(fail(ErrorCode::DependencyUnavailable)));
            // Re-read the original publication after the RPC, including failures.
            // No earlier read grants permission on a replaced or erased request.
            let mut conn = self.database.pool().acquire().await.map_err(database)?;
            let current = entry_row(&mut conn, actor, dialogue, entry, false)
                .await?
                .ok_or_else(|| fail(ErrorCode::PolicyDenied))?;
            if row_projection(&current)?.fingerprint() != p.fingerprint() {
                return Err(fail(ErrorCode::DependencyUnavailable));
            }
            let result = async {
                let qualified = proof?;
                qualified.verify(&mut conn, actor, &current, &p).await?;
                Ok::<_, RuntimeError>(qualified.authority.remote_tool_approval)
            }
            .await;
            let now = crate::ledger::database_now_ms(&mut conn)
                .await
                .map_err(database)?;
            let decision = if p.interaction.expires_at.is_some_and(|t| t <= now) {
                (Eligibility::Closed, vec![])
            } else if pending_command(
                &mut conn,
                p.interaction.interaction_id,
                p.state_fingerprint(),
            )
            .await?
            {
                (Eligibility::AwaitingDevice, vec![])
            } else {
                match result {
                    Ok(remote) => {
                        let actions = p.actions(now, remote);
                        let eligibility = if !actions.is_empty() {
                            Eligibility::Actionable
                        } else if p.interaction.kind == InteractionKind::Approval {
                            Eligibility::DeviceOnly
                        } else {
                            Eligibility::Denied
                        };
                        (eligibility, actions)
                    }
                    Err(e) if e.code == ErrorCode::PolicyDenied => (Eligibility::Denied, vec![]),
                    Err(e) if e.code == ErrorCode::DependencyUnavailable => {
                        (Eligibility::TemporarilyUnavailable, vec![])
                    }
                    Err(e) => return Err(e),
                }
            };
            tracing::info!(%correlation, stage = "read", elapsed_ms = started.elapsed().as_millis() as u64,
                eligibility = ?decision.0, "Native interaction eligibility");
            decision
        };
        Ok(Some(DialogueInteractionView {
            eligibility,
            execution_surface: "desktop".into(),
            entry_id: entry,
            dialogue_id: dialogue,
            visible_revision: NonZeroU64::new(
                row.try_get::<i64, _>("visible_revision")
                    .map_err(database)? as u64,
            )
            .ok_or_else(|| fail(ErrorCode::ContentIntegrityFailed))?,
            control_revision: p.state_fingerprint(),
            allowed_actions: actions,
            prompt_bytes: p.prompt_bytes,
            resolution: p.resolution,
            interaction: p.interaction,
            memory_execution: None,
        }))
    }
    pub(super) async fn submit_inner(
        &self,
        metadata: CommandMetadata,
        target: DialogueInteractionTarget,
        response: DialogueInteractionControlResponse,
    ) -> Result<Option<DialogueInteractionControlReceipt>, RuntimeError> {
        let actor = metadata.actor;
        if actor.principal_kind != PrincipalKind::User
            || metadata.validate().is_err()
            || metadata.conversation_guard.is_some()
        {
            return Err(fail(ErrorCode::InvalidCommand));
        }
        response.validate().map_err(fail)?;
        let mut conn = self.database.pool().acquire().await.map_err(database)?;
        let Some(first) =
            entry_row(&mut conn, actor, target.dialogue_id, target.entry_id, false).await?
        else {
            return Ok(None);
        };
        let initial = row_projection(&first)?;
        drop(conn);
        let proof = self
            .qualify_control(
                actor,
                row_id(&first, "agent_id")?,
                &initial,
                metadata.correlation_id,
            )
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(database)?;
        source::lock(&mut tx, &initial.admission)
            .await
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let row = entry_row(&mut tx, actor, target.dialogue_id, target.entry_id, true)
            .await?
            .ok_or_else(|| fail(ErrorCode::PolicyDenied))?;
        let p = row_projection(&row)?;
        if p.request_fingerprint() != initial.request_fingerprint() {
            return Err(fail(ErrorCode::ConversationSeqConflict));
        }
        proof.verify(&mut tx, actor, &row, &p).await?;
        let authority = proof.authority;
        let command = NativeDialogueInteractionCommand {
            command_id: metadata.command_id,
            interaction_id: p.interaction.interaction_id,
            target,
            admission: p.admission.clone(),
            response,
        };
        let encoded = native_codec::command(&command).encode_to_vec();
        let digest = Sha256::digest(&encoded);
        if let Some(existing) = command_row(&mut tx, actor, command.command_id, true).await? {
            if existing
                .try_get::<Vec<u8>, _>("command_sha256")
                .map_err(database)?
                != digest.into_bytes()
                || existing
                    .try_get::<Vec<u8>, _>("command_bytes")
                    .map_err(database)?
                    != encoded
            {
                return Err(fail(ErrorCode::IdempotencyConflict));
            }
            return Ok(Some(control_receipt(&existing)?));
        }
        let now = crate::ledger::database_now_ms(&mut tx)
            .await
            .map_err(database)?;
        if metadata.deadline < now || metadata.issued_at > now {
            return Err(fail(ErrorCode::InvalidCommand));
        }
        if target.expected_control_revision != p.state_fingerprint() {
            return Err(fail(ErrorCode::ConversationSeqConflict));
        }
        if !p
            .actions(now, authority.remote_tool_approval)
            .contains(&command.response.action())
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        // The original projection row is locked above. Competing browser
        // commands for this exact revision cannot both enter the device queue.
        if pending_command(
            &mut tx,
            command.interaction_id,
            target.expected_control_revision,
        )
        .await?
        {
            return Err(fail(ErrorCode::ConversationSeqConflict));
        }
        sqlx::query("INSERT INTO agent.native_dialogue_interaction_commands(tenant_id,owner_id,command_id,interaction_id,entry_id,expected_control_revision,command_sha256,command_bytes,queued_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.command_id.into_uuid())
            .bind(command.interaction_id.into_uuid()).bind(target.entry_id.into_uuid()).bind(target.expected_control_revision.into_bytes().to_vec())
            .bind(digest.into_bytes().to_vec()).bind(encoded).bind(now.unix_millis()).execute(&mut *tx).await.map_err(database)?;
        crate::dialogue_codec::wake_native_execution(&mut tx, actor)
            .await
            .map_err(database)?;
        authority
            .target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(database)?,
            )
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        tx.commit().await.map_err(database)?;
        Ok(Some(DialogueInteractionControlReceipt {
            command_id: command.command_id,
            delivery: DialogueInteractionControlDelivery::Queued,
        }))
    }
    pub(super) async fn commands_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        namespace: NativeDialogueInteractionInstallationNamespace,
        after: Option<NativeDialogueInteractionCommandCursor>,
        limit: u32,
    ) -> Result<NativeDialogueInteractionCommandPage, RuntimeError> {
        if !(1..=NATIVE_INTERACTION_MAX_COMMANDS).contains(&limit)
            || after.is_some_and(|cursor| cursor.queued_at.unix_millis() <= 0)
        {
            return Err(fail(ErrorCode::InvalidCommand));
        }
        target
            .require_actor(actor)
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let mut conn = self.database.pool().acquire().await.map_err(database)?;
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut conn)
                    .await
                    .map_err(database)?,
            )
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let rows = sqlx::query("SELECT c.command_bytes,c.command_id,c.queued_at_ms FROM agent.native_dialogue_interaction_commands c JOIN agent.native_dialogue_interaction_projections i USING(interaction_id) WHERE c.tenant_id=$1 AND c.owner_id=$2 AND i.tenant_id=$1 AND i.owner_id=$2 AND i.device_id=$3 AND i.installation_id=$4 AND i.device_subject_kind=$8 AND c.outcome IS NULL AND ($5::bigint IS NULL OR (c.queued_at_ms,c.command_id)>($5,$6)) ORDER BY c.queued_at_ms,c.command_id LIMIT $7")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(target.device_id().into_uuid())
            .bind(target.installation_id().into_uuid()).bind(after.map(|c|c.queued_at.unix_millis())).bind(after.map(|c|c.command_id.into_uuid())).bind(i64::from(limit)).bind(match namespace {NativeDialogueInteractionInstallationNamespace::DialogueOrigin=>"intent",NativeDialogueInteractionInstallationNamespace::NativeClient=>"work_item"}).fetch_all(&mut *conn).await.map_err(database)?;
        drop(conn);
        let scan_exhausted = rows.len() < limit as usize;
        let next_cursor = if scan_exhausted {
            None
        } else {
            let last = rows
                .last()
                .ok_or_else(|| fail(ErrorCode::ContentIntegrityFailed))?;
            Some(NativeDialogueInteractionCommandCursor {
                queued_at: UtcTimestamp::from_unix_millis(
                    last.try_get("queued_at_ms").map_err(database)?,
                ),
                command_id: row_id(last, "command_id")?,
            })
        };
        let mut commands = Vec::new();
        for row in rows {
            let candidate = self.deliver_queued_command(actor, target, &row).await;
            match candidate {
                Ok(command) => commands.push(command),
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::PolicyDenied
                            | ErrorCode::InteractionNotFound
                            | ErrorCode::ConversationSeqConflict
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        // Cursor covers scanned rows, including denied ones. No denied command
        // leaves this Owner and no synthetic response/ACK settles its state.
        Ok(NativeDialogueInteractionCommandPage {
            commands,
            next_cursor,
            scan_exhausted,
        })
    }
}
pub(super) async fn entry_row(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entry: DialogueEntryId,
    lock: bool,
) -> Result<Option<PgRow>, RuntimeError> {
    let mut rows = entry_rows(conn, actor, dialogue, &[entry.into_uuid()], lock).await?;
    if rows.len() > 1 {
        return Err(fail(ErrorCode::ContentIntegrityFailed));
    }
    Ok(rows.pop())
}
pub(super) async fn entry_rows(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    dialogue: AgentDialogueId,
    entries: &[uuid::Uuid],
    lock: bool,
) -> Result<Vec<PgRow>, RuntimeError> {
    if actor.principal_kind != PrincipalKind::User {
        return Err(fail(ErrorCode::PolicyDenied));
    }
    sqlx::query(&format!("SELECT i.*,p.permission_revision,e.visible_revision,e.entry_id AS context_entry_id,e.seq AS context_seq FROM agent.native_dialogue_interaction_projections i JOIN agent.dialogue_publications p ON p.source_interaction_id=i.interaction_id AND p.execution_home='device' AND p.tenant_id=i.tenant_id JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=i.tenant_id AND e.owner_id=i.owner_id JOIN agent.owned_agents a ON a.agent_id=i.agent_id JOIN agent.owned_agent_configurations cfg ON cfg.agent_id=a.agent_id WHERE i.tenant_id=$1 AND i.owner_id=$2 AND i.dialogue_id=$3 AND e.entry_id=ANY($4) AND e.kind='interaction' AND e.erasure_job_id IS NULL AND a.status='active' AND NOT EXISTS(SELECT 1 FROM collaboration.work_item_device_custody custody JOIN collaboration.work_items task USING(work_item_id) WHERE custody.run_id=i.run_id AND task.erasure_job_id IS NOT NULL) {}", if lock { "FOR UPDATE OF i FOR SHARE OF a,cfg" } else { "" }))
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(dialogue.into_uuid()).bind(entries)
        .fetch_all(conn).await.map_err(database)
}
async fn pending_command(
    conn: &mut sqlx::PgConnection,
    interaction: InteractionId,
    revision: Sha256,
) -> Result<bool, RuntimeError> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM agent.native_dialogue_interaction_commands WHERE interaction_id=$1 AND expected_control_revision=$2 AND outcome IS NULL)")
        .bind(interaction.into_uuid()).bind(revision.into_bytes().to_vec()).fetch_one(conn).await.map_err(database)
}
async fn command_row(
    conn: &mut sqlx::PgConnection,
    actor: Actor,
    command: CommandId,
    lock: bool,
) -> Result<Option<PgRow>, RuntimeError> {
    sqlx::query(&format!("SELECT * FROM agent.native_dialogue_interaction_commands WHERE tenant_id=$1 AND owner_id=$2 AND command_id=$3 {}", if lock { "FOR UPDATE" } else { "" }))
        .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(command.into_uuid()).fetch_optional(conn).await.map_err(database)
}
fn decode_command(bytes: &[u8]) -> Result<NativeDialogueInteractionCommand, RuntimeError> {
    if bytes.len() > native_codec::COMMAND_LIMIT {
        return Err(fail(ErrorCode::ContentIntegrityFailed));
    }
    let wire = wire::NativeDialogueInteractionCommand::decode(bytes).map_err(corrupt)?;
    if wire.encode_to_vec() != bytes {
        return Err(fail(ErrorCode::ContentIntegrityFailed));
    }
    native_codec::decode_command(Some(wire)).map_err(corrupt)
}
fn control_receipt(row: &PgRow) -> Result<DialogueInteractionControlReceipt, RuntimeError> {
    Ok(DialogueInteractionControlReceipt {
        command_id: row_id(row, "command_id")?,
        delivery: match row
            .try_get::<Option<&str>, _>("outcome")
            .map_err(database)?
        {
            None => DialogueInteractionControlDelivery::Queued,
            Some("applied") => {
                let b: Vec<u8> = row.try_get("owner_ack").map_err(database)?;
                let ack = wire::CommandAck::decode(b.as_slice()).map_err(corrupt)?;
                if ack.encode_to_vec() != b {
                    return Err(fail(ErrorCode::ContentIntegrityFailed));
                }
                DialogueInteractionControlDelivery::Applied(
                    native_codec::decode_command_ack(Some(ack)).map_err(corrupt)?,
                )
            }
            Some("rejected") => DialogueInteractionControlDelivery::Rejected(
                ErrorCode::from_safe_code(
                    row.try_get::<&str, _>("rejected_code").map_err(database)?,
                )
                .ok_or_else(|| fail(ErrorCode::ContentIntegrityFailed))?,
            ),
            _ => return Err(fail(ErrorCode::ContentIntegrityFailed)),
        },
    })
}
