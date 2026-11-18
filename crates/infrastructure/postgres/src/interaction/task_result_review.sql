SELECT tool.invocation_id,s.work_item_id,s.work_item_goal_revision AS goal_revision,
 s.work_item_control_revision AS control_revision,s.work_item_assignment_epoch AS assignment_epoch,
 report.producing_run_id,report.report_content_id,i.prompt_content_id
FROM approval.interactions i
JOIN tool.tool_invocations tool ON tool.tenant_id=i.tenant_id AND tool.invocation_id=i.requested_by_invocation_id
 AND tool.interaction_id=i.interaction_id AND tool.run_id=i.run_id AND tool.conversation_id=i.conversation_id
JOIN runtime.runs r ON r.tenant_id=i.tenant_id AND r.run_id=i.run_id AND r.conversation_id=i.conversation_id
JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
 AND s.subject_kind='work_item_report' AND s.purpose='work_item_report'
JOIN conversation.conversations conversation ON conversation.tenant_id=s.tenant_id AND conversation.conversation_id=s.conversation_id AND conversation.purpose='agent_work_item_report'
JOIN collaboration.work_item_execution_reports report ON report.work_item_id=s.work_item_id
 AND report.tenant_id=s.tenant_id AND report.owner_id=s.owner_id AND report.requesting_agent_id=s.agent_id
 AND report.report_run_id=r.run_id AND report.producing_run_id=s.report_from_run_id
 AND report.goal_revision=s.work_item_goal_revision AND report.control_revision=s.work_item_control_revision
 AND report.assignment_epoch=s.work_item_assignment_epoch
WHERE i.tenant_id=$1 AND i.interaction_id=$2 AND i.status=1 AND i.kind=3
 AND tool.tool_ref='tasks.verify@1' AND tool.status=2
 AND r.status=3 AND r.wait_interaction_id=i.interaction_id AND r.wait_invocation_id=tool.invocation_id
 AND r.control_home_kind=1 AND r.execution_home_kind=1 AND r.control_device_id IS NULL AND r.execution_device_id IS NULL
 AND r.parent_run_id IS NULL AND r.delegation_id IS NULL AND r.erasure_job_id IS NULL
 AND NOT EXISTS(SELECT 1 FROM approval.interaction_responses response WHERE response.interaction_id=i.interaction_id)
