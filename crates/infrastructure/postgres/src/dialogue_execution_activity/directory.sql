-- The three original execution owners, scoped before union. No projection
-- cache, private reasoning, synthetic Run, or late mutation of a frozen cut.
WITH executions AS NOT MATERIALIZED (
 SELECT r.run_id,s.source_intent_id AS intent_id,s.work_item_id AS task_id,
   s.subject_kind,'cloud'::text AS execution_home,
   CASE WHEN s.subject_kind='intent' THEN i.accepted_epoch ELSE s.context_epoch END AS accepted_epoch,
   CASE WHEN s.subject_kind='intent' THEN i.accepted_seq ELSE s.context_seq-1 END AS accepted_seq,
   r.admitted_at_ms,r.status::int AS status,r.wait_reason::int AS wait_reason,CASE WHEN c.phase=1 AND c.iteration>0 THEN 2 ELSE c.phase END::int AS phase,
   NULL::int AS native_step,NULL::int AS native_outcome,
   CASE WHEN e.entry_id IS NOT NULL THEN i.content_ref END AS excerpt_ref,
   v.goal_content_ref AS goal_ref,
   terminal.state AS terminal_notice,e.entry_id AS input_entry_id,e.epoch AS input_epoch,e.seq AS input_seq,
   NULL::uuid AS goal_id,NULL::uuid AS pursuit_id,
   CASE WHEN r.next_attempt_trigger=2 AND (r.status=1 OR (r.status=3 AND r.wait_reason=4)) THEN r.not_before_ms END AS retry_at
 FROM runtime.runs r
 JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
 LEFT JOIN runtime.loop_checkpoints c ON c.run_id=r.run_id
 LEFT JOIN agent.dialogue_terminal_deliveries terminal ON terminal.tenant_id=r.tenant_id AND terminal.run_id=r.run_id
 LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=s.source_intent_id AND i.tenant_id=s.tenant_id AND i.owner_id=s.owner_id AND i.dialogue_id=s.dialogue_id
 LEFT JOIN agent.agent_dialogue_entries e ON e.entry_id=i.source_entry_id AND ((e.kind='user_message' AND e.content_ref=i.content_ref) OR (i.origin_surface='capability' AND e.kind='interaction' AND e.interaction_id=i.continuation_interaction_id)) AND e.erasure_job_id IS NULL
 LEFT JOIN collaboration.work_items w ON w.work_item_id=s.work_item_id AND w.tenant_id=s.tenant_id AND w.owner_id=s.owner_id AND w.erasure_job_id IS NULL
 LEFT JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=w.work_item_id AND x.run_id=coalesce(s.report_from_run_id,r.run_id)
 LEFT JOIN collaboration.work_item_revisions v ON v.work_item_id=x.work_item_id AND v.state_revision=x.accepted_state_revision
 WHERE r.erasure_job_id IS NULL AND s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4
   AND ((s.subject_kind='intent' AND e.entry_id IS NOT NULL)
     OR (s.subject_kind IN ('work_item','work_item_report') AND v.work_item_id IS NOT NULL))
 UNION ALL
 SELECT d.run_id,d.intent_id,NULL::uuid,'intent','native',i.accepted_epoch,i.accepted_seq,coalesce(d.admitted_at_ms,d.created_at_ms),
   CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'succeeded' THEN 5 WHEN 'failed' THEN 6 WHEN 'cancelled' THEN 7 END
        WHEN o.state=4 THEN o.outcome+4 WHEN o.observed_at_ms>$5-60000 AND o.state=2 THEN 2 WHEN o.observed_at_ms>$5-60000 AND o.state=3 THEN 3 ELSE 0 END,
   NULL::int,NULL::int,
   CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$5-60000 THEN o.step::int END,
   CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'succeeded' THEN 1 WHEN 'failed' THEN 2 WHEN 'cancelled' THEN 3 END ELSE o.outcome::int END,
   i.content_ref,NULL::bytea,t.resolution,e.entry_id,e.epoch,e.seq,NULL::uuid,NULL::uuid,
   CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$5-60000 AND o.state IN (2,3) AND o.retry_observation_sha256=o.observation_sha256 THEN o.scheduled_retry_at_ms END
 FROM agent.device_admissions d
 JOIN agent.dialogue_user_intents i ON i.intent_id=d.intent_id AND i.tenant_id=d.tenant_id AND i.owner_id=d.owner_id AND i.dialogue_id=d.dialogue_id
 JOIN agent.agent_dialogue_entries e ON e.entry_id=i.source_entry_id AND ((e.kind='user_message' AND e.content_ref=i.content_ref) OR (i.origin_surface='capability' AND e.kind='interaction' AND e.interaction_id=i.continuation_interaction_id)) AND e.erasure_job_id IS NULL
 LEFT JOIN agent.native_dialogue_terminals t ON t.run_id=d.run_id
 LEFT JOIN agent.native_execution_activity o ON o.run_id=d.run_id
 WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3 AND d.dialogue_id=$4 AND (d.admitted_at_ms IS NOT NULL OR d.run_id=$13::uuid)
 UNION ALL
 SELECT d.run_id,w.source_intent_id,d.work_item_id,'work_item','native',x.context_epoch,x.context_seq,coalesce(d.admitted_at_ms,d.created_at_ms),
   CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'completed' THEN 5 WHEN 'failed' THEN 6 WHEN 'cancelled' THEN 7 END
        WHEN o.state=4 THEN o.outcome+4 WHEN o.observed_at_ms>$5-60000 AND o.state=2 THEN 2 WHEN o.observed_at_ms>$5-60000 AND o.state=3 THEN 3 ELSE 0 END,
   NULL::int,NULL::int,
   CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$5-60000 THEN o.step::int END,
   CASE WHEN t.run_id IS NOT NULL THEN CASE t.outcome WHEN 'completed' THEN 1 WHEN 'failed' THEN 2 WHEN 'cancelled' THEN 3 END ELSE o.outcome::int END,
   CASE WHEN e.entry_id IS NOT NULL THEN i.content_ref END,v.goal_content_ref,NULL::text,e.entry_id,e.epoch,e.seq,NULL::uuid,NULL::uuid,
   CASE WHEN t.run_id IS NULL AND o.observed_at_ms>$5-60000 AND o.state IN (2,3) AND o.retry_observation_sha256=o.observation_sha256 THEN o.scheduled_retry_at_ms END
 FROM collaboration.work_item_device_custody d
 JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch)
 JOIN collaboration.work_items w ON w.work_item_id=x.work_item_id AND w.tenant_id=x.tenant_id AND w.owner_id=x.owner_id AND w.erasure_job_id IS NULL
 JOIN collaboration.work_item_revisions v ON v.work_item_id=x.work_item_id AND v.state_revision=x.accepted_state_revision
 LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=w.source_intent_id AND i.tenant_id=x.tenant_id AND i.owner_id=x.owner_id AND i.dialogue_id=x.executing_dialogue_id
 LEFT JOIN agent.agent_dialogue_entries e ON e.entry_id=i.source_entry_id AND ((e.kind='user_message' AND e.content_ref=i.content_ref) OR (i.origin_surface='capability' AND e.kind='interaction' AND e.interaction_id=i.continuation_interaction_id)) AND e.erasure_job_id IS NULL
 LEFT JOIN collaboration.work_item_device_terminals t ON t.run_id=d.run_id
 LEFT JOIN agent.native_execution_activity o ON o.run_id=d.run_id
 WHERE x.tenant_id=$1 AND x.owner_id=$2 AND x.executing_agent_id=$3 AND x.executing_dialogue_id=$4 AND (d.admitted_at_ms IS NOT NULL OR d.run_id=$13::uuid)
 UNION ALL
 SELECT r.run_id,NULL::uuid,NULL::uuid,'goal','cloud',rules.dialogue_context_epoch,rules.dialogue_context_seq,
   r.admitted_at_ms,r.status::int,r.wait_reason::int,CASE WHEN c.phase=1 AND c.iteration>0 THEN 2 ELSE c.phase END::int,NULL::int,NULL::int,
   NULL::bytea,NULL::bytea,NULL::text,NULL::uuid,NULL::bigint,NULL::bigint,g.goal_id,p.pursuit_id,
   CASE WHEN r.next_attempt_trigger=2 AND (r.status=1 OR (r.status=3 AND r.wait_reason=4)) THEN r.not_before_ms END
 FROM runtime.runs r
 JOIN runtime.goal_run_processing_rules rules ON rules.tenant_id=r.tenant_id AND rules.run_id=r.run_id
 JOIN runtime.standing_goals g ON g.tenant_id=rules.tenant_id AND g.goal_id=rules.goal_id AND g.conversation_id=r.conversation_id
 JOIN runtime.goal_pursuits p ON p.tenant_id=g.tenant_id AND p.goal_id=g.goal_id AND p.pursuit_id=rules.owner_pursuit_id
 JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
   AND s.subject_kind='goal' AND s.source_goal_id=g.goal_id AND s.owner_id=g.owner_principal_id
 LEFT JOIN runtime.loop_checkpoints c ON c.run_id=r.run_id
 -- The independent Judge is internal control work, not another user execution.
 WHERE r.activity_kind=3 AND r.erasure_job_id IS NULL AND s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND s.dialogue_id=$4
), page AS MATERIALIZED (
 SELECT * FROM executions
 WHERE (($13::uuid IS NULL AND accepted_epoch=$6 AND accepted_seq<=$7 AND admitted_at_ms<=$8) OR run_id=$13::uuid)
   AND ($14::uuid IS NULL OR run_id<>$14)
   AND ($15::boolean OR subject_kind<>'goal')
   -- Apply the authenticated task target before pagination and Content reads.
   AND ($16::uuid IS NULL OR task_id=$16)
   AND ($9::bigint IS NULL OR (admitted_at_ms,run_id)<($9,$10::uuid))
   AND ($11='all' OR ($11='active' AND status BETWEEN 0 AND 4) OR ($11='terminal' AND status BETWEEN 5 AND 8))
 ORDER BY admitted_at_ms DESC,run_id DESC LIMIT $12
)
SELECT page.*,reply_input.reply_binding,1::bigint AS count,
   publications.committed_publications,publications.committed_results,publications.committed_artifact_cards
FROM page
LEFT JOIN agent.dialogue_user_intents reply_input ON reply_input.intent_id=page.intent_id AND page.subject_kind='intent'
 AND reply_input.tenant_id=$1 AND reply_input.owner_id=$2 AND reply_input.dialogue_id=$4
LEFT JOIN LATERAL (
 SELECT count(*) AS committed_publications,
   count(*) FILTER(WHERE p.purpose='result') AS committed_results,
   count(*) FILTER(WHERE e.kind='artifact') AS committed_artifact_cards
 FROM agent.dialogue_publications p
 JOIN agent.agent_dialogue_entries e ON e.entry_id=p.entry_id AND e.tenant_id=p.tenant_id AND e.owner_id=p.owner_id AND e.dialogue_id=p.dialogue_id
 WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.agent_id=$3 AND p.dialogue_id=$4 AND p.source_run_id=page.run_id
   AND e.epoch=$6 AND e.kind<>'erased'
   AND (e.kind<>'artifact' OR (resource.agent_artifact_owned(e.tenant_id,p.agent_id,e.artifact_id)
     AND EXISTS(SELECT 1 FROM artifact.artifact_versions v JOIN artifact.content_refs c ON c.content_id=v.content_id AND c.tenant_id=e.tenant_id AND c.state=2
       WHERE v.artifact_id=e.artifact_id AND v.artifact_version_id=e.artifact_version_id)))
) publications ON true
ORDER BY page.admitted_at_ms DESC,page.run_id DESC
