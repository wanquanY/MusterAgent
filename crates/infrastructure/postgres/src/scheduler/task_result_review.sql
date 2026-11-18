-- Separate passive waits from their original report provenance. Keeping the
-- two join graphs bounded avoids expensive planner enumeration on every wake,
-- even when there are no historical reviews. No early LIMIT can hide a valid
-- older candidate; ordering and the batch limit apply after all authority joins.
WITH waiting_reviews AS MATERIALIZED (
 SELECT i.tenant_id,i.conversation_id,i.activity_id,i.run_id,r.activity_kind,
  i.interaction_id,i.requested_at_ms,r.traceparent,r.tracestate,
  r.home_epoch,r.last_run_sequence
 FROM approval.interactions i
 JOIN tool.tool_invocations tool ON tool.tenant_id=i.tenant_id AND tool.invocation_id=i.requested_by_invocation_id
  AND tool.interaction_id=i.interaction_id AND tool.run_id=i.run_id AND tool.conversation_id=i.conversation_id
 JOIN runtime.runs r ON r.tenant_id=i.tenant_id AND r.run_id=i.run_id AND r.conversation_id=i.conversation_id
 WHERE i.status=1 AND i.kind=3
  AND tool.tool_ref='tasks.verify@1' AND tool.status=2
  AND r.status=3 AND r.wait_interaction_id=i.interaction_id AND r.wait_invocation_id=tool.invocation_id
  AND r.control_home_kind=1 AND r.execution_home_kind=1 AND r.control_device_id IS NULL AND r.execution_device_id IS NULL
  AND r.parent_run_id IS NULL AND r.delegation_id IS NULL AND r.erasure_job_id IS NULL
  AND NOT EXISTS(SELECT 1 FROM approval.interaction_responses response WHERE response.interaction_id=i.interaction_id)
)
SELECT r.tenant_id,r.conversation_id,r.activity_id,r.run_id,r.activity_kind,
 r.interaction_id,r.traceparent,r.tracestate,admission.correlation_id,
 r.home_epoch::text AS home_epoch,r.last_run_sequence::text AS last_run_sequence,
 stream.last_sequence::text AS conversation_offset
FROM waiting_reviews r
JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
 AND s.subject_kind='work_item_report' AND s.purpose='work_item_report'
JOIN conversation.conversations conversation ON conversation.tenant_id=s.tenant_id AND conversation.conversation_id=s.conversation_id AND conversation.purpose='agent_work_item_report'
JOIN collaboration.work_item_execution_reports report ON report.work_item_id=s.work_item_id
 AND report.tenant_id=s.tenant_id AND report.owner_id=s.owner_id AND report.requesting_agent_id=s.agent_id
 AND report.report_run_id=r.run_id AND report.producing_run_id=s.report_from_run_id
 AND report.goal_revision=s.work_item_goal_revision AND report.control_revision=s.work_item_control_revision
 AND report.assignment_epoch=s.work_item_assignment_epoch
JOIN event_store.ledger_streams stream ON stream.tenant_id=r.tenant_id AND stream.conversation_id=r.conversation_id
JOIN event_store.ledger_records admission ON admission.tenant_id=r.tenant_id AND admission.run_id=r.run_id AND admission.run_seq=1
ORDER BY r.requested_at_ms,r.interaction_id LIMIT $1
