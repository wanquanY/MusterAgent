use super::*;
impl PostgresNativeDialogueInteractionStore {
    pub(super) async fn publish_inner(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<NativeDialogueInteractionPublicationReceipt, RuntimeError> {
        p.validate().map_err(fail)?;
        let proof = self
            .stage_qualified_projection(actor, target, p, false)
            .await?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(database)?;
        source::lock(&mut tx, &p.admission)
            .await
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let s = self.publication_source(&mut tx, actor, &proof, p).await?;
        let result = self.observe(&mut tx, &s, p).await?;
        target
            .require_current_at(
                crate::ledger::database_now_ms(&mut tx)
                    .await
                    .map_err(database)?,
            )
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        tx.commit().await.map_err(database)?;
        Ok(result)
    }
    pub(super) async fn stage_qualified_projection(
        &self,
        actor: Actor,
        target: &OriginDeviceExecutionTarget,
        p: &NativeDialogueInteractionProjection,
        receipt_only: bool,
    ) -> Result<eligibility::QualifiedSubject, RuntimeError> {
        let qualify = async || {
            let live = if receipt_only {
                false
            } else {
                !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM agent.native_dialogue_interaction_projections WHERE interaction_id=$1)")
                    .bind(p.interaction.interaction_id.into_uuid()).fetch_one(self.database.pool()).await.map_err(database)?
            };
            self.qualify_subject(actor, target, p, live, CorrelationId::generate())
                .await
        };
        {
            let proof = qualify().await?;
            let mut conn = self.database.pool().acquire().await.map_err(database)?;
            self.publication_source(&mut conn, actor, &proof, p).await?;
        }
        self.stage_projection(actor, p).await?;
        // The external content write can outlive the first proof. The second
        // proof will be rebound to current custody inside the final transaction.
        qualify().await
    }
    pub(super) async fn publication_source(
        &self,
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        proof: &eligibility::QualifiedSubject,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<Source, RuntimeError> {
        let source = proof.source(conn, actor, p).await?;
        let existing = sqlx::query(
            "SELECT * FROM agent.native_dialogue_interaction_projections WHERE interaction_id=$1",
        )
        .bind(p.interaction.interaction_id.into_uuid())
        .fetch_optional(&mut *conn)
        .await
        .map_err(database)?;
        let Some(row) = existing else {
            return if proof.allows_new_publication() {
                Ok(source)
            } else {
                Err(fail(ErrorCode::InteractionNotFound))
            };
        };
        let original = row_projection(&row)?;
        if original.request_fingerprint() != p.request_fingerprint() {
            return Err(fail(ErrorCode::IdempotencyConflict));
        }
        let publication = receipt(conn, &original).await?;
        let visible = controls::entry_row(
            conn,
            actor,
            publication.dialogue_id,
            publication.entry_id,
            false,
        )
        .await?;
        if visible.is_none() || source.agent_id != row_id(&row, "agent_id")? {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        Ok(source)
    }
    pub(super) async fn observe(
        &self,
        conn: &mut sqlx::PgConnection,
        s: &Source,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<NativeDialogueInteractionPublicationReceipt, RuntimeError> {
        let previous = sqlx::query("SELECT * FROM agent.native_dialogue_interaction_projections WHERE interaction_id=$1 FOR UPDATE")
            .bind(p.interaction.interaction_id.into_uuid()).fetch_optional(&mut *conn).await.map_err(database)?;
        let now = crate::ledger::database_now_ms(&mut *conn)
            .await
            .map_err(database)?;
        crate::artifact::authorize_content_ref(conn, s.actor.tenant_id, &p.interaction.prompt_ref)
            .await
            .map_err(corrupt)?;
        if let Some(DialogueInteractionResolution::Answer { reference, .. }) = &p.resolution {
            crate::artifact::authorize_content_ref(conn, s.actor.tenant_id, reference)
                .await
                .map_err(corrupt)?;
        }
        if let Some(old) = previous {
            let old_p = row_projection(&old)?;
            if row_id::<TenantId>(&old, "tenant_id")? != s.actor.tenant_id
                || row_id::<PrincipalId>(&old, "owner_id")? != s.actor.principal_id
                || row_id::<AgentId>(&old, "agent_id")? != s.agent_id
                || p.request_fingerprint() != old_p.request_fingerprint()
            {
                return Err(fail(ErrorCode::IdempotencyConflict));
            }
            if p.observed_run_sequence < old_p.observed_run_sequence
                || (p.observed_run_sequence == old_p.observed_run_sequence
                    && p.fingerprint() != old_p.fingerprint())
                || (old_p.interaction.status != InteractionStatus::Pending
                    && p.state_fingerprint() != old_p.state_fingerprint())
            {
                return Err(fail(ErrorCode::ConversationSeqConflict));
            }
            if p.fingerprint() != old_p.fingerprint() {
                sqlx::query("UPDATE agent.native_dialogue_interaction_projections SET state_sha256=$2,projection_sha256=$3,projection_bytes=$4,observed_run_sequence=$5::numeric,control_open=$6,status=$7,updated_at_ms=$8 WHERE interaction_id=$1")
                    .bind(p.interaction.interaction_id.into_uuid()).bind(p.state_fingerprint().into_bytes().to_vec())
                    .bind(p.fingerprint().into_bytes().to_vec()).bind(native_codec::projection_bytes(p))
                    .bind(p.observed_run_sequence.get().to_string()).bind(p.control_open).bind(status(p)).bind(now.unix_millis())
                    .execute(&mut *conn).await.map_err(database)?;
                if p.state_fingerprint() != old_p.state_fingerprint() {
                    super::super::interaction::changed(
                        conn,
                        s.actor.tenant_id,
                        p.interaction.interaction_id,
                        p.admission.run_id,
                    )
                    .await
                    .map_err(database)?;
                }
            }
        } else {
            let b = &p.admission;
            sqlx::query("INSERT INTO agent.native_dialogue_interaction_projections(interaction_id,tenant_id,owner_id,agent_id,dialogue_id,intent_id,conversation_id,run_id,invocation_id,device_id,installation_id,device_subject_kind,source_digest,snapshot_hash,grant_proof_hash,request_sha256,state_sha256,projection_sha256,projection_bytes,observed_run_sequence,prompt_content_id,control_open,status,requested_at_ms,expires_at_ms,updated_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20::numeric,$21,$22,$23,$24,$25,$26)")
                .bind(p.interaction.interaction_id.into_uuid()).bind(s.actor.tenant_id.into_uuid()).bind(s.actor.principal_id.into_uuid())
                .bind(s.agent_id.into_uuid()).bind(s.dialogue_id.into_uuid()).bind(b.intent_id.into_uuid())
                .bind(p.conversation_id.into_uuid()).bind(b.run_id.into_uuid()).bind(p.invocation_id.into_uuid())
                .bind(b.device_id.into_uuid()).bind(b.installation_id.into_uuid()).bind(if b.work_item.is_some() { "work_item" } else { "intent" })
                .bind(b.source_digest.into_bytes().to_vec()).bind(b.snapshot_hash.into_bytes().to_vec()).bind(b.grant_proof_hash.into_bytes().to_vec())
                .bind(p.request_fingerprint().into_bytes().to_vec()).bind(p.state_fingerprint().into_bytes().to_vec()).bind(p.fingerprint().into_bytes().to_vec())
                .bind(native_codec::projection_bytes(p)).bind(p.observed_run_sequence.get().to_string()).bind(p.interaction.prompt_ref.content_id.into_uuid())
                .bind(p.control_open).bind(status(p)).bind(p.interaction.requested_at.unix_millis()).bind(p.interaction.expires_at.map(|t| t.unix_millis()))
                .bind(now.unix_millis()).execute(&mut *conn).await.map_err(database)?;
            let delivery_id = CommandId::from_uuid(uuid::Uuid::new_v5(
                &p.interaction.interaction_id.into_uuid(),
                b"native-dialogue-interaction-publication@1",
            ))
            .map_err(corrupt)?;
            publish_in_transaction(
                conn,
                s,
                &Delivery {
                    delivery_id,
                    run_id: b.run_id,
                    purpose: if p.interaction.kind == InteractionKind::Clarification {
                        DialoguePublicationPurpose::Clarification
                    } else {
                        DialoguePublicationPurpose::RequiredDecision
                    },
                    content: PublicDeliveryContent::Interaction(p.interaction.interaction_id),
                    fingerprint: p.request_fingerprint(),
                    source: PublicationSource::InteractionOwner(p.interaction.interaction_id),
                },
            )
            .await
            .map_err(database)?;
        }
        let result = receipt(conn, p).await?;
        if let Some(DialogueInteractionResolution::Answer { reference, .. }) = &p.resolution {
            crate::dialogue_codec::retain_content_refs(
                conn,
                s.actor,
                s.agent_id,
                vec![("public_entry", result.entry_id.into_uuid(), reference)],
            )
            .await
            .map_err(database)?;
        }
        Ok(result)
    }
}
pub(in crate::dialogue_publication) async fn prompt(
    conn: &mut sqlx::PgConnection,
    s: &Source,
    run: RunId,
    interaction: InteractionId,
) -> Result<ContentRef, DialoguePublicationError> {
    let row = sqlx::query("SELECT * FROM agent.native_dialogue_interaction_projections WHERE interaction_id=$1 AND tenant_id=$2 AND owner_id=$3 AND agent_id=$4 AND dialogue_id=$5 AND run_id=$6 AND conversation_id=$7 FOR SHARE")
        .bind(interaction.into_uuid()).bind(s.actor.tenant_id.into_uuid()).bind(s.actor.principal_id.into_uuid())
        .bind(s.agent_id.into_uuid()).bind(s.dialogue_id.into_uuid()).bind(run.into_uuid()).bind(s.conversation_id.into_uuid())
        .fetch_optional(conn).await.map_err(|_| DialogueStoreError::Infrastructure)?.ok_or(DialogueStoreError::Unavailable)?;
    Ok(row_projection(&row)
        .map_err(|_| DialogueStoreError::Corrupt)?
        .interaction
        .prompt_ref)
}
