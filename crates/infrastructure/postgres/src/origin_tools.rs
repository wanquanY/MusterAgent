//! Point-to-point handoff of an original cloud ToolInvocation to the installation
//! that signed the direct intent. No participant, second Run or Task is created.
use crate::dialogue_codec::{content_bytes, content_from_bytes, db, require_user, transaction};
use musteragent_content_ref::ContentRef;
use musteragent_identity::{Actor, DeviceId, DeviceInstallationId, InvocationId, UserIntentId};
use musteragent_runtime_capability::CapabilityInvocation;
use musteragent_runtime_session::DialogueStoreError as Error;
use musteragent_telemetry::DbCommitKind;
use sqlx::Row;
use uuid::Uuid;

#[derive(Clone)]
pub struct PostgresOriginTools(pub crate::PostgresDatabaseRuntime);

/// Authenticated request identity. The installation is taken from its verified
/// message origin. It is never an argument of a model-facing Tool.
pub struct OriginToolRecipient {
    pub actor: Actor,
    pub device_id: DeviceId,
    pub intent_id: UserIntentId,
    pub installation_id: DeviceInstallationId,
}

pub struct OriginToolCall {
    pub invocation_id: InvocationId,
    pub tool_ref: String,
    pub idempotency_key: String,
    pub request_ref: ContentRef,
    pub terminal_scope: Option<(musteragent_identity::ConversationId, bool)>,
    /// Time left before the original invocation's deadline when claimed. The
    /// device bounds its local execution by it instead of a fixed timeout.
    pub remaining_ms: Option<u64>,
}

pub struct OriginToolResult {
    pub content: ContentRef,
    pub failed: bool,
}

impl PostgresOriginTools {
    /// The immutable Tool journal and its dialogue conversation pick the sole
    /// destination. Re-entry only observes the original handoff.
    pub async fn enqueue(
        &self,
        call: &CapabilityInvocation,
        input: &ContentRef,
    ) -> Result<(), Error> {
        require_user(call.actor)?;
        let mut tx = self
            .0
            .write_unit_of_work(DbCommitKind::Interaction)
            .await
            .map_err(transaction)?;
        let inserted = sqlx::query(
            "INSERT INTO tool.origin_tool_calls(invocation_id,intent_id,installation_id,request_ref)
             SELECT t.invocation_id,i.intent_id,i.origin_installation_id,$8
             FROM tool.tool_invocations t
             JOIN agent.dialogue_sessions s ON s.conversation_id=t.conversation_id AND s.tenant_id=t.tenant_id
             JOIN agent.dialogue_user_intents i ON i.intent_id=s.source_intent_id AND i.dialogue_id=s.dialogue_id
                 AND i.tenant_id=s.tenant_id AND i.owner_id=s.owner_id
             JOIN agent.dialogue_intent_admissions q ON q.intent_id=i.intent_id
             JOIN artifact.content_refs b ON b.content_id=t.request_content_id AND b.tenant_id=t.tenant_id AND b.content_hash=$9 AND b.state=2
             WHERE t.tenant_id=$1 AND s.owner_id=$2 AND t.run_id=$3 AND t.invocation_id=$4
                 AND t.tool_ref=$5 AND t.request_content_id=$6 AND t.execution_attempt_id=$7
                 AND t.status=3 AND s.subject_kind='intent' AND s.purpose='intent_coordination'
                 AND i.origin_surface='desktop' AND i.origin_installation_id IS NOT NULL AND q.execution_target='cloud' AND q.origin_tool_device_id IS NOT NULL
             ON CONFLICT (invocation_id) DO UPDATE SET invocation_id=EXCLUDED.invocation_id
             WHERE tool.origin_tool_calls.installation_id=EXCLUDED.installation_id
                 AND tool.origin_tool_calls.intent_id=EXCLUDED.intent_id"
        )
        .bind(call.actor.tenant_id.into_uuid()).bind(call.actor.principal_id.into_uuid())
        .bind(call.run_id.into_uuid()).bind(call.invocation_id.into_uuid()).bind(call.tool_key.as_str())
        .bind(call.request_ref.content_id.into_uuid()).bind(call.execution_attempt_id.into_uuid())
        .bind(content_bytes(input)).bind(call.request_ref.sha256.into_bytes().as_slice()).execute(&mut *tx).await.map_err(db)?;
        if inserted.rows_affected() != 1 {
            return Err(Error::Unavailable);
        }
        tx.commit().await.map_err(transaction)
    }

    /// A lost response may be recovered with the same claim. Another process
    /// cannot take over an effect whose physical outcome is unknown.
    pub async fn claim(
        &self,
        recipient: &OriginToolRecipient,
        claim_id: Uuid,
    ) -> Result<Option<OriginToolCall>, Error> {
        require_user(recipient.actor)?;
        let mut tx = self
            .0
            .write_unit_of_work(DbCommitKind::Interaction)
            .await
            .map_err(transaction)?;
        let row = sqlx::query(
            "WITH next AS (
                SELECT c.invocation_id,t.tool_ref,t.idempotency_key,t.conversation_id,
                       CASE WHEN t.deadline_ms IS NULL THEN NULL ELSE
                         GREATEST(t.deadline_ms-(EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint,0) END AS remaining_ms,
                       CASE WHEN t.tool_ref='device.terminal.exec@4' THEN snap.encoded_snapshot END AS terminal_snapshot,
                       CASE WHEN t.tool_ref='device.terminal.exec@4' THEN snap.content_hash END AS terminal_snapshot_hash
                FROM tool.origin_tool_calls c JOIN tool.tool_invocations t USING(invocation_id)
                JOIN agent.dialogue_user_intents i USING(intent_id) JOIN agent.dialogue_intent_admissions q USING(intent_id)
                JOIN runtime.runs owner ON owner.run_id=t.run_id AND owner.tenant_id=t.tenant_id
                JOIN capability.runtime_snapshots snap ON snap.runtime_snapshot_id=owner.runtime_snapshot_id
                  AND snap.run_id=t.run_id AND snap.conversation_id=t.conversation_id AND snap.tenant_id=t.tenant_id
                WHERE q.origin_tool_device_id=$6 AND c.intent_id=$1 AND c.installation_id=$2 AND i.tenant_id=$3 AND i.owner_id=$4
                  AND EXISTS (SELECT 1 FROM runtime.runs r
                      JOIN runtime.attempts a ON a.run_id=r.run_id AND a.attempt_id=t.execution_attempt_id
                      JOIN runtime.leases l ON l.lease_id=a.lease_id AND l.attempt_id=a.attempt_id AND l.fencing_token=a.fencing_token
                      WHERE r.run_id=t.run_id AND r.tenant_id=t.tenant_id AND r.status=2 AND r.cancel_requested_at_ms IS NULL
                      AND a.status=1 AND l.expires_at_ms > (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint)
                  AND (c.claim_id IS NULL OR c.claim_id=$5) AND c.result_ref IS NULL AND t.status=3
                  AND (t.deadline_ms IS NULL OR t.deadline_ms > (EXTRACT(EPOCH FROM clock_timestamp())*1000)::bigint)
                ORDER BY (c.claim_id IS NOT NULL) DESC,c.invocation_id LIMIT 1 FOR UPDATE OF c SKIP LOCKED
             ) UPDATE tool.origin_tool_calls c SET claim_id=$5 FROM next
             WHERE c.invocation_id=next.invocation_id
             RETURNING c.invocation_id,c.request_ref,next.tool_ref,next.idempotency_key,next.conversation_id,next.terminal_snapshot,next.terminal_snapshot_hash,next.remaining_ms")
            .bind(recipient.intent_id.into_uuid()).bind(recipient.installation_id.into_uuid())
            .bind(recipient.actor.tenant_id.into_uuid()).bind(recipient.actor.principal_id.into_uuid())
            .bind(claim_id).bind(recipient.device_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let call = row
            .map(|row| -> Result<_, Error> {
                Ok(OriginToolCall {
                    invocation_id: InvocationId::from_uuid(
                        row.try_get("invocation_id").map_err(db)?,
                    )
                    .map_err(|_| Error::Corrupt)?,
                    terminal_scope: match row
                        .try_get::<Option<Vec<u8>>, _>("terminal_snapshot")
                        .map_err(db)?
                    {
                        Some(bytes) => {
                            use musteragent_content_ref::Sha256;
                            use musteragent_context::RuntimeSnapshotDecoder;
                            let hash: Vec<u8> =
                                row.try_get("terminal_snapshot_hash").map_err(db)?;
                            if Sha256::digest(&bytes).into_bytes().as_slice() != hash {
                                return Err(Error::Corrupt);
                            }
                            let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
                                .decode_runtime_snapshot(&bytes)
                                .map_err(|_| Error::Corrupt)?;
                            let conversation = musteragent_identity::ConversationId::from_uuid(
                                row.try_get("conversation_id").map_err(db)?,
                            )
                            .map_err(|_| Error::Corrupt)?;
                            Some((
                                conversation,
                                snapshot.policy.permission_mode
                                    == musteragent_policy::PermissionMode::Trusted,
                            ))
                        }
                        None => None,
                    },
                    tool_ref: row.try_get("tool_ref").map_err(db)?,
                    idempotency_key: row.try_get("idempotency_key").map_err(db)?,
                    request_ref: content_from_bytes(row.try_get("request_ref").map_err(db)?)?,
                    remaining_ms: row
                        .try_get::<Option<i64>, _>("remaining_ms")
                        .map_err(db)?
                        .map(|value| u64::try_from(value).map_err(|_| Error::Corrupt))
                        .transpose()?,
                })
            })
            .transpose()?;
        tx.commit().await.map_err(transaction)?;
        Ok(call)
    }

    pub async fn turn_open(&self, recipient: &OriginToolRecipient) -> Result<bool, Error> {
        require_user(recipient.actor)?;
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_user_intents i
            JOIN agent.dialogue_intent_admissions q USING(intent_id)
            WHERE i.intent_id=$1 AND i.origin_installation_id=$2 AND i.tenant_id=$3 AND i.owner_id=$4
            AND q.origin_tool_device_id=$5 AND q.status NOT IN ('failed','rejected','cancelled')
            AND NOT EXISTS (SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r
                ON r.conversation_id=s.conversation_id AND r.tenant_id=s.tenant_id
                WHERE s.source_intent_id=i.intent_id AND s.purpose='intent_coordination' AND r.status IN (4,5,6,7)))")
            .bind(recipient.intent_id.into_uuid()).bind(recipient.installation_id.into_uuid()).bind(recipient.actor.tenant_id.into_uuid())
            .bind(recipient.actor.principal_id.into_uuid()).bind(recipient.device_id.into_uuid()).fetch_one(self.0.pool()).await.map_err(db)
    }

    /// Verify the claim before allocating any result content, including retries.
    pub async fn claimed_result(
        &self,
        recipient: &OriginToolRecipient,
        invocation: InvocationId,
        claim: Uuid,
    ) -> Result<Option<OriginToolResult>, Error> {
        require_user(recipient.actor)?;
        let row = sqlx::query("SELECT c.result_ref,c.failed FROM tool.origin_tool_calls c
            JOIN agent.dialogue_user_intents i USING(intent_id) JOIN agent.dialogue_intent_admissions q USING(intent_id)
            WHERE c.invocation_id=$1 AND c.claim_id=$2 AND c.intent_id=$3 AND c.installation_id=$4
            AND q.origin_tool_device_id=$5 AND i.tenant_id=$6 AND i.owner_id=$7")
            .bind(invocation.into_uuid()).bind(claim).bind(recipient.intent_id.into_uuid()).bind(recipient.installation_id.into_uuid())
            .bind(recipient.device_id.into_uuid()).bind(recipient.actor.tenant_id.into_uuid()).bind(recipient.actor.principal_id.into_uuid())
            .fetch_optional(self.0.pool()).await.map_err(db)?.ok_or(Error::Unavailable)?;
        let bytes: Option<Vec<u8>> = row.try_get("result_ref").map_err(db)?;
        bytes
            .map(|b| {
                Ok(OriginToolResult {
                    content: content_from_bytes(b)?,
                    failed: row.try_get("failed").map_err(db)?,
                })
            })
            .transpose()
    }

    pub async fn finish(
        &self,
        recipient: &OriginToolRecipient,
        call: InvocationId,
        claim_id: Uuid,
        outcome: &OriginToolResult,
    ) -> Result<(), Error> {
        require_user(recipient.actor)?;
        let mut tx = self
            .0
            .write_unit_of_work(DbCommitKind::ToolTerminal)
            .await
            .map_err(transaction)?;
        let result = sqlx::query(
            "UPDATE tool.origin_tool_calls c SET result_ref=$7,failed=$8
             FROM agent.dialogue_user_intents i JOIN agent.dialogue_intent_admissions q USING(intent_id)
             WHERE q.origin_tool_device_id=$9 AND c.invocation_id=$1 AND c.claim_id=$2 AND c.intent_id=$3 AND c.installation_id=$4
                 AND i.intent_id=c.intent_id AND i.tenant_id=$5 AND i.owner_id=$6
                 AND (c.result_ref IS NULL OR (c.result_ref=$7 AND c.failed=$8))")
            .bind(call.into_uuid()).bind(claim_id).bind(recipient.intent_id.into_uuid()).bind(recipient.installation_id.into_uuid())
            .bind(recipient.actor.tenant_id.into_uuid()).bind(recipient.actor.principal_id.into_uuid())
            .bind(content_bytes(&outcome.content)).bind(outcome.failed).bind(recipient.device_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        if result.rows_affected() != 1 {
            return Err(Error::Unavailable);
        }
        tx.commit().await.map_err(transaction)
    }

    /// Records monotonic work of the current claim. A repeat or a lower value
    /// leaves the row unchanged and still succeeds, so retries are harmless.
    pub async fn report_progress(
        &self,
        recipient: &OriginToolRecipient,
        call: InvocationId,
        claim_id: Uuid,
        units: u64,
    ) -> Result<(), Error> {
        require_user(recipient.actor)?;
        let units = i64::try_from(units).map_err(|_| Error::Corrupt)?;
        let found: Option<bool> = sqlx::query_scalar(
            "UPDATE tool.origin_tool_calls c SET progress_units=GREATEST(c.progress_units,$7)
             FROM agent.dialogue_user_intents i JOIN agent.dialogue_intent_admissions q USING(intent_id)
             WHERE q.origin_tool_device_id=$8 AND c.invocation_id=$1 AND c.claim_id=$2 AND c.intent_id=$3
                 AND c.installation_id=$4 AND i.intent_id=c.intent_id AND i.tenant_id=$5 AND i.owner_id=$6
                 AND c.result_ref IS NULL
             RETURNING true")
            .bind(call.into_uuid()).bind(claim_id).bind(recipient.intent_id.into_uuid())
            .bind(recipient.installation_id.into_uuid()).bind(recipient.actor.tenant_id.into_uuid())
            .bind(recipient.actor.principal_id.into_uuid()).bind(units).bind(recipient.device_id.into_uuid())
            .fetch_optional(self.0.pool()).await.map_err(db)?;
        found.map(|_| ()).ok_or(Error::Unavailable)
    }

    /// Work reported so far by the claiming desktop (0 before any report).
    pub async fn progress(&self, call: InvocationId) -> Result<u64, Error> {
        let units: Option<i64> = sqlx::query_scalar(
            "SELECT progress_units FROM tool.origin_tool_calls WHERE invocation_id=$1",
        )
        .bind(call.into_uuid())
        .fetch_optional(self.0.pool())
        .await
        .map_err(db)?;
        u64::try_from(units.unwrap_or(0)).map_err(|_| Error::Corrupt)
    }

    pub async fn result(&self, call: InvocationId) -> Result<Option<OriginToolResult>, Error> {
        sqlx::query("SELECT result_ref,failed FROM tool.origin_tool_calls WHERE invocation_id=$1 AND result_ref IS NOT NULL")
            .bind(call.into_uuid()).fetch_optional(self.0.pool()).await.map_err(db)?
            .map(|r| Ok(OriginToolResult {
                content: content_from_bytes(r.try_get("result_ref").map_err(db)?)?,
                failed: r.try_get("failed").map_err(db)?,
            })).transpose()
    }
}
