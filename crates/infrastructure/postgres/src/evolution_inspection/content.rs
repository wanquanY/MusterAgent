//! Exact point reads share the Growth Owner's current visibility rules.
//! This does not authorize arbitrary ContentStore references or past revoked data.
use super::PostgresMemoryEvolutionError;
use musteragent_runtime_memory::AgentGrowthContentQuery;

pub(super) async fn authorize(
    pool: &sqlx::PgPool,
    q: AgentGrowthContentQuery,
) -> Result<bool, PostgresMemoryEvolutionError> {
    if q.actor.principal_kind != musteragent_identity::PrincipalKind::User {
        return Ok(false);
    }
    // A memory erasure fences its entire frozen closure immediately, before
    // asynchronous byte deletion; retained candidates cannot resurrect it.
    let allowed=sqlx::query_scalar::<_,bool>(
        "WITH visible_proposals AS (
           SELECT p.* FROM evolution.proposals p
           WHERE p.tenant_id=$1 AND p.scope_kind=2 AND p.scope_id=$2 AND p.status IN (1,2,3,5)
             AND p.proposed_at_ms<=$7
             AND (p.target_memory_id IS NULL OR EXISTS(SELECT 1 FROM memory.memories m
               WHERE m.memory_id=p.target_memory_id AND m.tenant_id=p.tenant_id AND m.lifecycle=1))
             AND NOT EXISTS(SELECT 1 FROM evolution.promotions promotion
               JOIN memory.memory_versions v ON v.memory_version_id=promotion.published_memory_version_id
               JOIN memory.memories m ON m.memory_id=v.memory_id
               WHERE promotion.proposal_id=p.proposal_id
                 AND (m.lifecycle<>1 OR (v.valid_to_ms IS NOT NULL AND v.valid_to_ms<=$7)))
         )
         SELECT EXISTS(SELECT 1 FROM artifact.content_refs c
         WHERE c.tenant_id=$1 AND c.content_id=$3 AND c.content_hash=$4
           AND c.media_type=$5 AND c.byte_length=$6::bigint AND c.state=2
           AND NOT EXISTS(SELECT 1 FROM memory.agent_memory_erasure_content_roots erased
             WHERE erased.tenant_id=c.tenant_id AND erased.content_id=c.content_id)
           AND (
             EXISTS(SELECT 1 FROM memory.memories m JOIN memory.memory_versions v
               ON v.memory_version_id=m.current_version_id AND v.memory_id=m.memory_id
               WHERE m.tenant_id=$1 AND m.scope_kind=4 AND m.scope_id=$2 AND m.lifecycle=1
                 AND v.content_id=c.content_id AND v.content_hash=c.content_hash
                 AND v.valid_from_ms<=$7 AND (v.valid_to_ms IS NULL OR v.valid_to_ms>$7)
                 AND (v.source<>1 OR EXISTS(SELECT 1 FROM memory.explicit_publications pub
                   WHERE pub.tenant_id=m.tenant_id AND pub.memory_id=m.memory_id AND pub.memory_version_id=v.memory_version_id)))
             OR EXISTS(SELECT 1 FROM visible_proposals p
               WHERE ((p.candidate_content_id=c.content_id AND p.candidate_content_hash=c.content_hash)
                   OR EXISTS(SELECT 1 FROM evolution.evaluations e WHERE e.proposal_id=p.proposal_id
                     AND e.report_content_id=c.content_id AND e.report_content_hash=c.content_hash
                     AND e.evaluation_id=(SELECT latest.evaluation_id FROM evolution.evaluations latest WHERE latest.proposal_id=p.proposal_id ORDER BY latest.evaluated_at_ms DESC,latest.evaluation_id LIMIT 1))))
             OR EXISTS(SELECT 1 FROM evolution.instincts i JOIN visible_proposals p ON p.proposal_id=i.proposal_id
               WHERE i.tenant_id=$1 AND i.target_agent_id=$2 AND i.lifecycle=1 AND p.status=5
                 AND i.payload_content_id=c.content_id AND i.payload_content_hash=c.content_hash AND i.created_at_ms<=$7)
             OR EXISTS(SELECT 1 FROM evolution.studio_revision_drafts d JOIN visible_proposals p ON p.proposal_id=d.proposal_id
               WHERE d.tenant_id=$1 AND d.target_agent_id=$2 AND p.status=5
                 AND d.payload_content_id=c.content_id AND d.payload_content_hash=c.content_hash AND d.created_at_ms<=$7)
           ))")
        .bind(q.actor.tenant_id.into_uuid()).bind(q.agent_id.into_uuid())
        .bind(q.reference.content_id.into_uuid()).bind(q.reference.sha256.into_bytes().to_vec())
        .bind(q.reference.media_type.as_str()).bind(i64::try_from(q.reference.byte_length).unwrap_or(-1))
        .bind(q.observed_at.unix_millis()).fetch_one(pool).await?;
    Ok(allowed)
}
