WITH facts AS NOT MATERIALIZED (
 -- Accepted input owns preparation before an execution exists. intent_id is
 -- only an ordering identity here; no Run or execution receipt is synthesized.
 SELECT i.intent_id AS run_id,
        CASE WHEN coalesce(p.blocker, 0) <> 0 OR q.last_error_code IS NOT NULL THEN 0 ELSE 1 END AS status,
        NULL::int AS wait_reason,NULL::int AS phase,
        CASE WHEN coalesce(p.blocker, 0) = 0 AND q.last_error_code IS NULL THEN 1 END AS native_step,
        NULL::int AS native_outcome,i.accepted_at_ms AS at,NULL::bigint AS retry_at,false AS permission_wait
 FROM agent.dialogue_user_intents i
 JOIN agent.dialogue_intent_admissions q USING(intent_id)
 JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
 LEFT JOIN agent.dialogue_admission_progress p USING(intent_id)
 WHERE i.tenant_id=$1 AND i.owner_id=$2 AND d.agent_id=$3
   AND q.status IN ('pending','claimed')
   AND NOT EXISTS(SELECT 1 FROM agent.native_execution_not_admitted n WHERE n.intent_id=i.intent_id)
   AND NOT EXISTS (SELECT 1 FROM agent.device_admissions a WHERE a.intent_id=i.intent_id AND a.admitted_at_ms IS NOT NULL)
   AND NOT EXISTS (SELECT 1 FROM agent.dialogue_sessions s JOIN runtime.runs r
        ON r.tenant_id=s.tenant_id AND r.conversation_id=s.conversation_id
        WHERE s.source_intent_id=i.intent_id AND s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id)
 UNION ALL
 SELECT r.run_id, r.status::int AS status, r.wait_reason::int AS wait_reason,CASE WHEN c.phase=1 AND c.iteration>0 THEN 2 ELSE c.phase END::int AS phase,
        NULL::int AS native_step,NULL::int AS native_outcome,
        coalesce(r.finished_at_ms,r.started_at_ms,r.admitted_at_ms) AS at,
        CASE WHEN r.next_attempt_trigger=2 AND (r.status=1 OR (r.status=3 AND r.wait_reason=4)) THEN r.not_before_ms END,false
 FROM runtime.runs r JOIN agent.dialogue_sessions s
 ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
 LEFT JOIN runtime.loop_checkpoints c ON c.run_id=r.run_id
 WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3
 UNION ALL
 SELECT d.run_id,
  CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'succeeded' THEN 5 WHEN 'failed' THEN 6 WHEN 'cancelled' THEN 7 END
       WHEN o.run_id IS NULL AND d.admitted_at_ms>$4-60000 THEN 1
       WHEN o.state=4 THEN o.outcome+4 WHEN o.observed_at_ms>$4-60000 AND o.state=2 THEN 2 WHEN o.observed_at_ms>$4-60000 AND o.state=3 THEN 3 ELSE 0 END,
  NULL::int,NULL::int,
  CASE WHEN t.run_id IS NULL AND o.run_id IS NULL AND d.admitted_at_ms>$4-60000 THEN 1
       WHEN t.run_id IS NULL AND o.observed_at_ms>$4-60000 THEN o.step::int END,
  CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'succeeded' THEN 1 WHEN 'failed' THEN 2 WHEN 'cancelled' THEN 3 END ELSE o.outcome::int END,
  coalesce(t.committed_at_ms,o.observed_at_ms,d.admitted_at_ms),
  CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$4-60000 AND o.state IN (2,3) AND o.retry_observation_sha256=o.observation_sha256 THEN o.scheduled_retry_at_ms END,
  coalesce(t.run_id IS NULL AND o.observed_at_ms>$4-60000 AND o.state=3 AND o.permission_observation_sha256=o.observation_sha256 AND o.permission_wait,false)
 FROM agent.device_admissions d LEFT JOIN agent.native_dialogue_terminals t USING(run_id)
 LEFT JOIN agent.native_execution_activity o ON o.run_id=d.run_id
 WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3 AND d.admitted_at_ms IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM agent.native_execution_not_admitted n WHERE n.intent_id=d.intent_id)
 UNION ALL
 SELECT d.run_id,
  CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'completed' THEN 5 WHEN 'failed' THEN 6 WHEN 'cancelled' THEN 7 END
       WHEN o.state=4 THEN o.outcome+4 WHEN o.observed_at_ms>$4-60000 AND o.state=2 THEN 2 WHEN o.observed_at_ms>$4-60000 AND o.state=3 THEN 3 ELSE 0 END,
  NULL::int,NULL::int,
  CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$4-60000 THEN o.step::int END,
  CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'completed' THEN 1 WHEN 'failed' THEN 2 WHEN 'cancelled' THEN 3 END ELSE o.outcome::int END,
  coalesce(t.committed_at_ms,o.observed_at_ms,d.admitted_at_ms),
  CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$4-60000 AND o.state IN (2,3) AND o.retry_observation_sha256=o.observation_sha256 THEN o.scheduled_retry_at_ms END,
  coalesce(t.run_id IS NULL AND o.observed_at_ms>$4-60000 AND o.state=3 AND o.permission_observation_sha256=o.observation_sha256 AND o.permission_wait,false)
 FROM collaboration.work_item_device_custody d
 JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch)
 LEFT JOIN collaboration.work_item_device_terminals t ON t.run_id=d.run_id
 LEFT JOIN agent.native_execution_activity o ON o.run_id=d.run_id
 WHERE x.tenant_id=$1 AND x.owner_id=$2 AND x.executing_agent_id=$3 AND d.admitted_at_ms IS NOT NULL
 UNION ALL
 -- These are original admission cancellations, not invented Runtime terminals.
 SELECT o.intent_id,7,NULL::int,NULL::int,NULL::int,3,o.created_at_ms,NULL::bigint,false
 FROM agent.dialogue_admission_outcomes o
 WHERE o.tenant_id=$1 AND o.owner_id=$2 AND o.agent_id=$3 AND o.outcome='cancelled' AND o.reason='user_requested'
 UNION ALL
 SELECT n.intent_id,7,NULL::int,NULL::int,NULL::int,3,n.recorded_at_ms,NULL::bigint,false
 FROM agent.native_execution_not_admitted n JOIN agent.device_admissions d USING(intent_id,run_id)
 WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3
), last_terminal AS (
 SELECT status,wait_reason,phase,native_step,native_outcome,1::bigint AS count,at,NULL::bigint AS retry_at,false AS permission_wait
 FROM facts WHERE status BETWEEN 5 AND 8 ORDER BY at DESC,run_id DESC LIMIT 1
), current_groups AS (
 SELECT status,wait_reason,phase,native_step,native_outcome,count(*) AS count,max(at) AS at,min(retry_at) AS retry_at,permission_wait
 FROM facts WHERE status BETWEEN 0 AND 4 GROUP BY status,wait_reason,phase,native_step,native_outcome,(retry_at IS NOT NULL),permission_wait
)
SELECT * FROM current_groups UNION ALL SELECT * FROM last_terminal ORDER BY at,status;
