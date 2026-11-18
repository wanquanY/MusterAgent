//! The Device Artifact owner contributes its actual stored content closure to
//! the existing privacy owner. This does not create an Agent/account erasure
//! admission, authorize erasure, or treat soft deletion as permanent erasure.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{AgentId, ContentId};

impl PostgresPrivacyErasureStore {
    /// Enumerate the new owner's content contribution for an owning account or
    /// one of its Agents, including archived/deleted Agents. Before turning this
    /// snapshot into an erasure plan, the original scope owner must fence new
    /// producers and include the other owners' roots. This method does neither.
    ///
    /// ContentStore.delete retains exact ContentRef tombstones and destroys
    /// blobs, so immutable provenance/receipt FKs never obstruct byte erasure.
    /// The returned receipt roots must be covered too: deleting a file alone is
    /// not deletion of its durable delivery acknowledgment. Legacy exported
    /// producer results remain Device-local. Canonical Resource
    /// command, successful result, read dependencies and terminal receipts are
    /// stored by this owner and are all included below.
    pub async fn device_artifact_erasure_roots(
        &self,
        actor: Actor,
        agent_id: Option<AgentId>,
    ) -> Result<Vec<ContentRef>, PostgresPrivacyErasureError> {
        if actor.principal_kind != PrincipalKind::User {
            return Err(PostgresPrivacyErasureError::DeviceArtifactUnauthorized);
        }
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::PrivacyErasure,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        // Ownership is checked independently of lifecycle; privacy cleanup
        // must remain possible after the normal UI has hidden a deleted Agent.
        if let Some(agent) = agent_id {
            let owned: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent.owned_agents a
                JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1
                AND s.tenant_id=$2 AND s.owner_id=$3 AND s.product='musteragent')",
            )
            .bind(agent.into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_one(&mut *tx)
            .await?;
            if !owned {
                return Err(PostgresPrivacyErasureError::DeviceArtifactUnauthorized);
            }
        }
        let rows = sqlx::query("WITH owned AS (
            SELECT DISTINCT proof.artifact_id FROM artifact.device_versions proof
            JOIN agent.owned_agents a ON a.agent_id=proof.agent_id AND a.dialogue_id=proof.dialogue_id
            JOIN agent.owner_spaces s ON s.space_id=a.space_id AND s.tenant_id=proof.tenant_id AND s.owner_id=proof.owner_id
            WHERE proof.tenant_id=$1 AND proof.owner_id=$2 AND ($3::uuid IS NULL OR proof.agent_id=$3)
             AND s.product='musteragent'
            UNION SELECT DISTINCT proof.artifact_id FROM artifact.device_resource_versions proof
             JOIN resource.device_commands command USING(invocation_id)
             WHERE command.tenant_id=$1 AND command.owner_id=$2 AND ($3::uuid IS NULL OR command.agent_id=$3)), publications AS (
            SELECT DISTINCT p.source_tool_invocation_id AS invocation_id FROM agent.dialogue_publications p
            JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=p.tenant_id
            WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.execution_home='device'
             AND e.artifact_id IN (SELECT artifact_id FROM owned)), roots AS (
            SELECT v.content_id FROM artifact.artifact_versions v JOIN owned USING(artifact_id)
            UNION SELECT d.content_id FROM artifact.artifact_version_dependencies d
                JOIN artifact.artifact_versions v USING(artifact_version_id) JOIN owned USING(artifact_id)
            UNION SELECT root FROM agent.dialogue_tool_publication_groups g CROSS JOIN LATERAL unnest(ARRAY[g.device_receipt_content_id,g.device_resource_manifest_id]) root
                JOIN publications p ON p.invocation_id=g.invocation_id WHERE g.tenant_id=$1
            UNION SELECT root FROM resource.device_commands command
             CROSS JOIN LATERAL unnest(ARRAY[command.command_content_id,command.request_content_id,command.result_content_id,command.receipt_content_id]) root
             WHERE command.tenant_id=$1 AND command.owner_id=$2 AND ($3::uuid IS NULL OR command.agent_id=$3)
            UNION SELECT r.content_id FROM resource.device_command_reads r
             JOIN resource.device_commands command USING(invocation_id)
             WHERE command.tenant_id=$1 AND command.owner_id=$2 AND ($3::uuid IS NULL OR command.agent_id=$3)
            UNION SELECT pin.content_id FROM agent.dialogue_content_references pin
                JOIN agent.dialogue_publications p ON p.entry_id=pin.reference_id AND p.tenant_id=pin.tenant_id
                JOIN publications selected ON selected.invocation_id=p.source_tool_invocation_id
                WHERE pin.tenant_id=$1 AND pin.owner_id=$2 AND pin.reference_kind='public_entry' )
            SELECT c.content_id,c.media_type,c.byte_length::text AS byte_length,c.content_hash
            FROM roots JOIN artifact.content_refs c USING(content_id) WHERE c.tenant_id=$1
            ORDER BY c.content_id LIMIT 4097")
            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
            .bind(agent_id.map(AgentId::into_uuid)).fetch_all(&mut *tx).await?;
        if rows.len() > 4_096 {
            return Err(PostgresPrivacyErasureError::DeviceArtifactClosureTooLarge);
        }
        let refs = rows
            .into_iter()
            .map(|row| {
                Ok(ContentRef {
                    content_id: ContentId::from_uuid(row.try_get("content_id")?).map_err(|_| {
                        PostgresPrivacyErasureError::CorruptState("device artifact content id")
                    })?,
                    media_type: MediaType::new(row.try_get::<String, _>("media_type")?).map_err(
                        |_| PostgresPrivacyErasureError::CorruptState("device artifact media type"),
                    )?,
                    byte_length: row.try_get::<String, _>("byte_length")?.parse().map_err(
                        |_| PostgresPrivacyErasureError::CorruptState("device artifact length"),
                    )?,
                    sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                        .map_err(|_| {
                            PostgresPrivacyErasureError::CorruptState("device artifact hash")
                        })?,
                })
            })
            .collect::<Result<Vec<_>, PostgresPrivacyErasureError>>()?;
        tx.commit().await?;
        Ok(refs)
    }
}
