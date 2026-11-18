//! Relationship-fenced messages and observation; no transcript impersonation.
use super::*;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Send {
    handle: String,
    message: String,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Handle {
    handle: uuid::Uuid,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Receive {
    after: Option<uuid::Uuid>,
}
impl PostgresCloudDelegation {
    pub(super) async fn send(
        &self,
        tx: &mut sqlx::PgConnection,
        command: &DialogueToolInvocation,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let input: Send = serde_json::from_value(input).map_err(|_| Error::Invalid)?;
        if input.message.trim().is_empty() || input.message.len() > 16000 {
            return Err(Error::Invalid);
        }
        let row=if input.handle=="parent" {
            sqlx::query("SELECT delegation_id,parent_run_id AS target FROM runtime.delegations WHERE tenant_id=$1 AND child_run_id=$2 AND status=1")
                .bind(command.actor.tenant_id.into_uuid()).bind(command.run_context.run_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?
        } else {
            let handle:uuid::Uuid=input.handle.parse().map_err(|_|Error::Invalid)?;
            sqlx::query("SELECT delegation_id,child_run_id AS target FROM runtime.delegations WHERE tenant_id=$1 AND parent_run_id=$2 AND delegation_id=$3 AND status=1")
                .bind(command.actor.tenant_id.into_uuid()).bind(command.run_context.run_id.into_uuid()).bind(handle).fetch_optional(&mut *tx).await.map_err(db)?
        }.ok_or(Error::NotFound)?;
        let target: uuid::Uuid = row.try_get("target").map_err(db)?;
        let live:bool=sqlx::query_scalar("SELECT status NOT IN (5,6,7,8) AND cancel_requested_at_ms IS NULL FROM runtime.runs WHERE run_id=$1").bind(target).fetch_one(&mut *tx).await.map_err(db)?;
        if !live {
            return Err(Error::ParentStopped);
        }
        let delegation: uuid::Uuid = row.try_get("delegation_id").map_err(db)?;
        let id = command.invocation_id.into_uuid();
        let reference = self
            .put(
                command.actor.tenant_id,
                id,
                b"message",
                "text/plain",
                input.message.as_bytes(),
            )
            .await?;
        crate::admission::authorize_published_content_ref(tx, command.actor.tenant_id, &reference)
            .await
            .map_err(|_| Error::Corrupt)?;
        sqlx::query("INSERT INTO runtime.delegation_messages(message_id,tenant_id,delegation_id,sender_run_id,receiver_run_id,content_id,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(id).bind(command.actor.tenant_id.into_uuid()).bind(delegation).bind(command.run_context.run_id.into_uuid()).bind(target).bind(reference.content_id.into_uuid()).bind(command.requested_at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        let response = serde_json::json!({"message_id":id,"receiver_run_id":target});
        event(
            tx,
            command,
            delegation,
            "subagent_message",
            response.clone(),
        )
        .await?;
        Ok(response)
    }
    pub(super) async fn receive(
        &self,
        tx: &mut sqlx::PgConnection,
        command: &DialogueToolInvocation,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let input: Receive = serde_json::from_value(input).map_err(|_| Error::Invalid)?;
        if let Some(after) = input.after {
            let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.delegation_messages WHERE message_id=$1 AND receiver_run_id=$2 AND tenant_id=$3)").bind(after).bind(command.run_context.run_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            if !valid {
                return Err(Error::NotFound);
            }
        }
        let rows=sqlx::query("SELECT m.message_id,m.sender_run_id,m.delegation_id,c.content_id,c.media_type,c.byte_length::bigint AS byte_length,c.content_hash FROM runtime.delegation_messages m JOIN artifact.content_refs c ON c.content_id=m.content_id WHERE m.tenant_id=$1 AND m.receiver_run_id=$2 AND ($3::uuid IS NULL OR (m.created_at_ms,m.message_id)>(SELECT created_at_ms,message_id FROM runtime.delegation_messages WHERE message_id=$3)) ORDER BY m.created_at_ms,m.message_id LIMIT 65")
            .bind(command.actor.tenant_id.into_uuid()).bind(command.run_context.run_id.into_uuid()).bind(input.after).fetch_all(tx).await.map_err(db)?;
        let more = rows.len() > 64;
        let mut messages = Vec::new();
        for row in rows.iter().take(64) {
            let reference = reference(row)?;
            let bytes = self
                .content
                .read(command.actor.tenant_id, &reference)
                .await
                .map_err(|_| Error::Unavailable)?;
            verify_content(&reference, &bytes).map_err(|_| Error::Corrupt)?;
            messages.push(serde_json::json!({"message_id":row.try_get::<uuid::Uuid,_>("message_id").map_err(db)?,"sender_run_id":row.try_get::<uuid::Uuid,_>("sender_run_id").map_err(db)?,"handle":row.try_get::<uuid::Uuid,_>("delegation_id").map_err(db)?,"message":std::str::from_utf8(&bytes).map_err(|_|Error::Corrupt)?}));
        }
        let next = if more {
            messages.last().and_then(|m| m.get("message_id")).cloned()
        } else {
            None
        };
        Ok(serde_json::json!({"messages":messages,"next":next}))
    }
    pub(super) async fn list(
        &self,
        tx: &mut sqlx::PgConnection,
        command: &DialogueToolInvocation,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct List {
            #[serde(default)]
            handles: Vec<uuid::Uuid>,
            after: Option<uuid::Uuid>,
        }
        let input: List = serde_json::from_value(input).map_err(|_| Error::Invalid)?;
        if input.handles.len() > 64
            || (command.tool_key == "delegation.join@4"
                && (input.handles.is_empty() || input.after.is_some()))
        {
            return Err(Error::Invalid);
        }
        if let Some(after) = input.after {
            let owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime.delegations WHERE delegation_id=$1 AND parent_run_id=$2 AND tenant_id=$3)").bind(after).bind(command.run_context.run_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
            if !owned {
                return Err(Error::NotFound);
            }
        }
        let rows=sqlx::query("SELECT d.delegation_id,d.child_run_id,r.status,d.report_content_id FROM runtime.delegations d JOIN runtime.runs r ON r.run_id=d.child_run_id WHERE d.tenant_id=$1 AND d.parent_run_id=$2 AND (cardinality($3::uuid[])=0 OR d.delegation_id=ANY($3)) AND ($4::uuid IS NULL OR (d.created_at_ms,d.delegation_id)>(SELECT created_at_ms,delegation_id FROM runtime.delegations WHERE delegation_id=$4)) ORDER BY d.created_at_ms,d.delegation_id LIMIT 65")
            .bind(command.actor.tenant_id.into_uuid()).bind(command.run_context.run_id.into_uuid()).bind(&input.handles).bind(input.after).fetch_all(tx).await.map_err(db)?;
        if !input.handles.is_empty()
            && rows.len()
                != input
                    .handles
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
        {
            return Err(Error::NotFound);
        }
        let more = rows.len() > 64;
        let children=rows.iter().take(64).map(|r|Ok(serde_json::json!({"handle":r.try_get::<uuid::Uuid,_>("delegation_id").map_err(db)?,"run_id":r.try_get::<uuid::Uuid,_>("child_run_id").map_err(db)?,"status":status(r.try_get("status").map_err(db)?)?,"report_content_id":r.try_get::<Option<uuid::Uuid>,_>("report_content_id").map_err(db)?}))).collect::<Result<Vec<_>,Error>>()?;
        let complete = !more
            && children.iter().all(|c| {
                matches!(
                    c["status"].as_str(),
                    Some("completed" | "failed" | "cancelled" | "interrupted")
                )
            });
        let next = if more {
            children.last().and_then(|c| c.get("handle")).cloned()
        } else {
            None
        };
        Ok(serde_json::json!({"children":children,"complete":complete,"next":next}))
    }
    pub(super) async fn cancel_child(
        &self,
        tx: &mut sqlx::PgConnection,
        command: &DialogueToolInvocation,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let input: Handle = serde_json::from_value(input).map_err(|_| Error::Invalid)?;
        let child:uuid::Uuid=sqlx::query_scalar("SELECT child_run_id FROM runtime.delegations WHERE delegation_id=$1 AND parent_run_id=$2 AND tenant_id=$3")
            .bind(input.handle).bind(command.run_context.run_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::NotFound)?;
        sqlx::query("INSERT INTO runtime.delegation_stop_requests(request_id,tenant_id,run_id,requested_by,created_at_ms) VALUES($1,$2,$3,$4,$5)")
            .bind(command.invocation_id.into_uuid()).bind(command.actor.tenant_id.into_uuid()).bind(child).bind(command.actor.principal_id.into_uuid()).bind(command.requested_at.unix_millis()).execute(&mut *tx).await.map_err(db)?;
        let response = serde_json::json!({"handle":input.handle,"status":"stop_requested"});
        event(
            tx,
            command,
            input.handle,
            "subagent_stop_requested",
            response.clone(),
        )
        .await?;
        Ok(response)
    }
}
pub(crate) fn reference(row: &sqlx::postgres::PgRow) -> Result<ContentRef, Error> {
    Ok(ContentRef {
        content_id: ContentId::from_uuid(row.try_get("content_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        media_type: MediaType::new(row.try_get::<String, _>("media_type").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        byte_length: u64::try_from(row.try_get::<i64, _>("byte_length").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        sha256: Sha256::from_bytes(&row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
    })
}
pub(super) fn status(value: i16) -> Result<&'static str, Error> {
    match value {
        1 => Ok("queued"),
        2 => Ok("running"),
        3 => Ok("waiting"),
        4 => Ok("recovering"),
        5 => Ok("completed"),
        6 => Ok("failed"),
        7 => Ok("cancelled"),
        8 => Ok("interrupted"),
        _ => Err(Error::Corrupt),
    }
}
