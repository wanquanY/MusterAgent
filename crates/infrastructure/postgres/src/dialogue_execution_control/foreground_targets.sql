-- Resolve the original active execution owners before joining the source-root
-- visibility view. Its own six-way provenance join must not be flattened into
-- this three-way ownership join on every activity/control observation.
-- Both groups retain one statement snapshot and every original predicate.
WITH cloud_sources AS MATERIALIZED (
SELECT r.run_id,s.source_intent_id,s.conversation_id,
 s.tenant_id,s.owner_id,s.agent_id,s.dialogue_id
FROM agent.dialogue_sessions s
JOIN runtime.runs r ON r.tenant_id=s.tenant_id
  AND r.conversation_id=s.conversation_id
JOIN conversation.conversations c ON c.tenant_id=s.tenant_id
  AND c.conversation_id=s.conversation_id
  AND c.purpose=s.conversation_purpose
WHERE s.tenant_id=$1
  AND s.owner_id=$2
  AND s.agent_id=$3
  AND s.purpose='intent_coordination'
  AND s.subject_kind='intent'
  AND c.purpose='dialogue_intent_coordination'
  AND r.status IN(1,2,3,4)
  AND r.parent_run_id IS NULL
  AND r.delegation_id IS NULL
  AND r.erasure_job_id IS NULL
  AND r.control_home_kind=1
  AND r.control_device_id IS NULL
  AND r.execution_home_kind=1
  AND r.execution_device_id IS NULL
), visible_roots AS MATERIALIZED (
 SELECT intent_id,tenant_id,owner_id,agent_id,dialogue_id
 FROM agent.dialogue_execution_visible_source_roots
 WHERE tenant_id=$1 AND owner_id=$2 AND agent_id=$3 AND direct_user_root
   -- Restrict provenance work to active Cloud sources, including the empty set.
   -- An InitPlan array preserves this join boundary without truncating targets.
   AND intent_id=ANY(ARRAY(SELECT source_intent_id FROM cloud_sources))
)
SELECT r.run_id AS target_id,r.run_id,r.source_intent_id AS intent_id,
 r.conversation_id,NULL::bytea AS native_target
FROM cloud_sources r
JOIN visible_roots root ON root.intent_id=r.source_intent_id
  AND root.tenant_id=r.tenant_id AND root.owner_id=r.owner_id
  AND root.agent_id=r.agent_id AND root.dialogue_id=r.dialogue_id
ORDER BY r.run_id
