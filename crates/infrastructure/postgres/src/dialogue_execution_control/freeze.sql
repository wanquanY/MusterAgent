-- Appended to the original candidate relation; $1 tenant, $2 owner, $3 Agent,
-- $4 observed revision, $5 dialogue, $6 command, $7 request hash, $8 timestamp,
-- $9 optional task: the direct user request whose root alone is stopped.
, observed AS MATERIALIZED (
 SELECT t.* FROM agent.dialogue_execution_control_targets t WHERE t.agent_id=$3 AND t.activity_revision=$4
  AND ($9::uuid IS NULL OR EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots s
   WHERE s.intent_id=t.intent_id AND s.root_intent_id=$9 AND s.tenant_id=$1 AND s.owner_id=$2))
), root_cuts AS MATERIALIZED (
 SELECT DISTINCT s.tenant_id,s.owner_id,s.agent_id,s.dialogue_id,s.root_intent_id,s.root_entry_id,s.root_content_ref,s.root_epoch,s.root_seq
 FROM observed o JOIN agent.dialogue_execution_source_roots s ON s.intent_id=o.intent_id
 WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$5 AND s.direct_user_root
), desired AS MATERIALIZED (
 SELECT target_id,intent_id,run_id,conversation_id,native_target FROM observed
 UNION ALL
 SELECT c.target_id,c.intent_id,c.run_id,c.conversation_id,c.native_target FROM candidates c
 JOIN agent.dialogue_execution_source_roots s ON s.intent_id=c.intent_id
 JOIN root_cuts f ON f.root_intent_id=s.root_intent_id AND f.tenant_id=s.tenant_id AND f.owner_id=s.owner_id AND f.agent_id=s.agent_id AND f.dialogue_id=s.dialogue_id
  AND f.root_entry_id=s.root_entry_id AND f.root_content_ref=s.root_content_ref AND f.root_epoch=s.root_epoch AND f.root_seq=s.root_seq
 WHERE NOT EXISTS(SELECT 1 FROM observed o WHERE o.intent_id=c.intent_id)
), resolved AS MATERIALIZED (
 SELECT o.target_id,o.intent_id,
 coalesce(d.run_id,r.run_id,o.run_id) AS run_id,
 coalesce(d.conversation_id,r.conversation_id,o.conversation_id) AS conversation_id,
 CASE WHEN d.intent_id IS NOT NULL THEN
   agent.native_control_target_bytes(d.tenant_id,d.owner_id,d.agent_id,d.dialogue_id,d.intent_id,d.run_id,d.conversation_id,d.device_id,d.installation_id,d.source_digest,coalesce(h.current_revision,0),v.signed_grant_hash)
 ELSE o.native_target END AS native_target
 FROM desired o
 LEFT JOIN agent.device_admissions d ON d.intent_id=o.intent_id
  AND d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3 AND d.dialogue_id=$5
  AND (o.run_id IS NULL OR (o.native_target IS NOT NULL AND o.run_id=d.run_id AND o.conversation_id=d.conversation_id))
 LEFT JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id
 LEFT JOIN agent.device_grant_versions v ON v.intent_id=h.intent_id AND v.revision=h.current_revision AND v.source_digest=d.source_digest
 LEFT JOIN agent.dialogue_sessions s ON o.run_id IS NULL AND d.intent_id IS NULL
  AND s.source_intent_id=o.intent_id AND s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$5
  AND s.subject_kind='intent' AND s.purpose='intent_coordination'
 LEFT JOIN runtime.runs r ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
  AND r.parent_run_id IS NULL AND r.delegation_id IS NULL AND r.control_home_kind=1 AND r.execution_home_kind=1
  AND r.control_device_id IS NULL AND r.execution_device_id IS NULL AND r.erasure_job_id IS NULL
), command AS (
 INSERT INTO agent.dialogue_execution_cancellations(command_id,tenant_id,owner_id,agent_id,dialogue_id,activity_revision,request_hash,target_count,created_at_ms,task_intent_id)
 SELECT $6,$1,$2,$3,$5,$4,$7,count(*),$8,$9 FROM resolved RETURNING command_id
), roots AS (
 INSERT INTO agent.dialogue_execution_cancellation_roots(cancellation_id,root_intent_id,tenant_id,owner_id,agent_id,dialogue_id,root_entry_id,root_content_ref,root_epoch,root_seq)
 SELECT command.command_id,f.root_intent_id,f.tenant_id,f.owner_id,f.agent_id,f.dialogue_id,f.root_entry_id,f.root_content_ref,f.root_epoch,f.root_seq FROM root_cuts f CROSS JOIN command RETURNING root_intent_id
), inserted AS (
 INSERT INTO agent.dialogue_execution_cancellation_targets(cancellation_id,command_id,target_id,intent_id,run_id,conversation_id,native_target)
 SELECT command.command_id,gen_random_uuid(),target_id,intent_id,run_id,conversation_id,native_target FROM resolved CROSS JOIN command RETURNING target_id
)
SELECT count(*) FROM inserted
