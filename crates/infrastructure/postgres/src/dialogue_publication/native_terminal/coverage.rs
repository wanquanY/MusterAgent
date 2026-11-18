//! The Native Journal proves its complete last batch. This Owner separately
//! proves the exact public fulfillment recorded in PostgreSQL, never inferring
//! completion from an earlier card, partial cut or a caller-chosen Tool name.
use super::*;
pub(super) async fn complete(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    terminal: &DeviceTerminalReceipt,
) -> Result<bool, DialogueStoreError> {
    let Some(coverage) = &terminal.completion else {
        return Ok(false);
    };
    let mut complete = false;
    for tool in &coverage.tools {
        if matches!(
            tool.tool_ref.as_str(),
            "capability.request@1" | "capability.request@2"
        ) {
            if !crate::agent_dialogue::capability::native_completed(
                tx,
                source.actor,
                terminal.binding.run_id,
                tool.invocation_id,
            )
            .await?
            {
                return Err(DialogueStoreError::Unavailable);
            }
            complete = true;
            continue;
        }

        if !matches!(
            tool.tool_ref.as_str(),
            "dialogue.publish@1" | "artifacts.deliver@1" | "artifacts.deliver@2"
        ) {
            // Other successful local calls are certified by the Home Journal.
            // A Tool name alone never establishes public fulfillment.
            continue;
        }
        let row = sqlx::query("SELECT g.*,c.media_type,c.byte_length::text AS byte_length,c.content_hash,
             (SELECT count(*) FROM agent.dialogue_publications p JOIN agent.agent_dialogue_entries e USING(entry_id)
                WHERE p.source_tool_invocation_id=g.invocation_id AND p.tenant_id=g.tenant_id
                  AND p.owner_id=$3 AND p.source_run_id=g.source_run_id AND p.source_intent_id=g.source_intent_id
                  AND p.execution_home='device' AND p.device_subject_kind='intent'
                  AND p.purpose=g.purpose AND e.kind<>'erased') AS actual_entries
             FROM agent.dialogue_tool_publication_groups g
             JOIN artifact.content_refs c ON c.tenant_id=g.tenant_id AND c.content_id=g.device_receipt_content_id
             WHERE g.tenant_id=$1 AND g.source_run_id=$2 AND g.invocation_id=$4
               AND g.source_intent_id=$5 AND g.execution_home='device' AND g.device_subject_kind='intent'
               AND c.state=2 FOR SHARE OF g,c")
            .bind(source.actor.tenant_id.into_uuid()).bind(terminal.binding.run_id.into_uuid())
            .bind(source.actor.principal_id.into_uuid()).bind(tool.invocation_id.into_uuid())
            .bind(source.require_intent()?.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(false);
        };
        if row.try_get::<&str, _>("tool_ref").map_err(db)? != tool.tool_ref.as_str()
            || row.try_get::<&str, _>("protocol_ref").map_err(db)?
                != DialogueOutputContract::NativeToolsV1.key()
            || row.try_get::<Vec<u8>, _>("request_hash").map_err(db)?
                != tool.request_sha256.into_bytes()
            || row
                .try_get::<uuid::Uuid, _>("device_receipt_content_id")
                .map_err(db)?
                != tool.result_ref.content_id.into_uuid()
            || row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?
                != tool.result_ref.sha256.into_bytes()
            || row.try_get::<&str, _>("media_type").map_err(db)?
                != tool.result_ref.media_type.as_str()
            || row.try_get::<String, _>("byte_length").map_err(db)?
                != tool.result_ref.byte_length.to_string()
            || i64::from(row.try_get::<i16, _>("entry_count").map_err(db)?)
                != row.try_get::<i64, _>("actual_entries").map_err(db)?
        {
            return Err(DialogueStoreError::Corrupt);
        }
        // Serialize with the original tombstone and Content owners. Preserved
        // immutable publication hashes are not proof that the result remains
        // visible. No body is read or copied into terminal metadata.
        let entries = sqlx::query(
            "SELECT e.entry_id,e.kind FROM agent.dialogue_publications p
            JOIN agent.agent_dialogue_entries e USING(entry_id)
            WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.source_run_id=$3
              AND p.source_tool_invocation_id=$4 ORDER BY e.entry_id LIMIT 17 FOR SHARE OF e",
        )
        .bind(source.actor.tenant_id.into_uuid())
        .bind(source.actor.principal_id.into_uuid())
        .bind(terminal.binding.run_id.into_uuid())
        .bind(tool.invocation_id.into_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        if entries.len() != row.try_get::<i16, _>("entry_count").map_err(db)? as usize
            || entries
                .iter()
                .any(|e| e.try_get::<&str, _>("kind").ok() == Some("erased"))
        {
            return Ok(false);
        }
        let ids = entries
            .iter()
            .map(|e| e.try_get::<uuid::Uuid, _>("entry_id").map_err(db))
            .collect::<Result<Vec<_>, _>>()?;
        let content = sqlx::query(
            "SELECT c.state FROM agent.dialogue_content_references r
            JOIN artifact.content_refs c ON c.tenant_id=r.tenant_id AND c.content_id=r.content_id
            WHERE r.tenant_id=$1 AND r.owner_id=$2 AND r.reference_kind='public_entry'
              AND r.reference_id=ANY($3) ORDER BY c.content_id FOR SHARE OF c",
        )
        .bind(source.actor.tenant_id.into_uuid())
        .bind(source.actor.principal_id.into_uuid())
        .bind(ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        if content.is_empty()
            || content
                .iter()
                .any(|c| c.try_get::<i16, _>("state").ok() != Some(2))
        {
            return Ok(false);
        }
        complete |= matches!(
            row.try_get::<&str, _>("purpose").map_err(db)?,
            "reply" | "result" | "blocked"
        );
    }
    Ok(complete)
}
