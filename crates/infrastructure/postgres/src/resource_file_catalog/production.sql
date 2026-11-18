SELECT node.*, version.artifact_version_id, artifact.updated_at_ms AS file_updated_at_ms,
       version.version::text AS version,
       content.content_id, content.media_type, content.byte_length::text AS byte_length,
       content.content_hash, producer.production
FROM resource.nodes AS node
LEFT JOIN artifact.artifacts AS artifact
  ON artifact.tenant_id=node.tenant_id AND artifact.artifact_id=node.file_artifact_id
 AND artifact.lifecycle<>3
LEFT JOIN resource.file_versions AS version
  ON version.artifact_id=artifact.artifact_id AND version.version=artifact.current_version
 AND NOT EXISTS(SELECT 1 FROM artifact.artifact_versions erased
                WHERE erased.artifact_version_id=version.artifact_version_id
                  AND erased.work_item_erasure_job_id IS NOT NULL)
LEFT JOIN artifact.content_refs AS content
  ON content.tenant_id=artifact.tenant_id AND content.content_id=version.content_id
 AND content.content_hash=version.content_hash
-- The two branches are mutually exclusive: Native's original version author
-- is its canonical receipt, never a fabricated Cloud tool invocation.
LEFT JOIN LATERAL (
  SELECT jsonb_build_object(
    'execution_home','cloud','agent_id',session.agent_id,'dialogue_id',session.dialogue_id,
    'intent_id',session.source_intent_id,'task_id',session.work_item_id,
    'run_id',inv.run_id,'invocation_id',inv.invocation_id,
    'committed_at_ms',original.committed_at_ms) AS production
  FROM artifact.artifact_versions original
  JOIN tool.tool_invocations inv ON inv.invocation_id=original.source_invocation_id
   AND inv.tenant_id=node.tenant_id
  JOIN runtime.runs run ON run.run_id=inv.run_id AND run.tenant_id=inv.tenant_id
   AND run.conversation_id=inv.conversation_id AND run.erasure_job_id IS NULL
  JOIN agent.dialogue_sessions session ON session.tenant_id=inv.tenant_id
   AND session.conversation_id=inv.conversation_id AND session.agent_id=node.scope_id
  JOIN agent.owned_agents agent ON agent.agent_id=session.agent_id
   AND agent.dialogue_id=session.dialogue_id AND agent.status<>'deleted'
  JOIN agent.owner_spaces space ON space.space_id=agent.space_id
   AND space.tenant_id=node.tenant_id AND space.owner_id=session.owner_id
  LEFT JOIN collaboration.work_items task ON task.work_item_id=session.work_item_id
   AND task.tenant_id=space.tenant_id AND task.owner_id=space.owner_id
  WHERE node.scope_kind=4 AND original.artifact_version_id=version.artifact_version_id
    AND original.artifact_id=artifact.artifact_id AND original.content_id=version.content_id
    AND original.content_hash=version.content_hash
    AND (session.work_item_id IS NULL OR (task.work_item_id IS NOT NULL AND task.erasure_job_id IS NULL))
  UNION ALL
  SELECT jsonb_build_object(
    'execution_home','native','agent_id',command.agent_id,'dialogue_id',command.dialogue_id,
    'intent_id',command.intent_id,'task_id',custody.work_item_id,
    'run_id',command.run_id,'invocation_id',command.invocation_id,
    'committed_at_ms',receipt.committed_at_ms) AS production
  FROM artifact.device_resource_versions receipt
  JOIN artifact.artifact_versions original ON original.artifact_version_id=receipt.artifact_version_id
   AND original.artifact_id=receipt.artifact_id AND original.source_invocation_id IS NULL
  JOIN resource.device_commands command ON command.invocation_id=receipt.invocation_id
   AND command.tenant_id=node.tenant_id AND command.agent_id=node.scope_id
   AND command.receipt_content_id IS NOT NULL AND command.receipt_commitment IS NOT NULL
   AND command.outcome='succeeded'
  JOIN agent.owned_agents agent ON agent.agent_id=command.agent_id
   AND agent.dialogue_id=command.dialogue_id AND agent.status<>'deleted'
  JOIN agent.owner_spaces space ON space.space_id=agent.space_id
   AND space.tenant_id=command.tenant_id AND space.owner_id=command.owner_id
  LEFT JOIN collaboration.work_item_device_admitted_custody custody
   ON custody.run_id=command.work_item_run_id
  LEFT JOIN collaboration.work_items task ON task.work_item_id=custody.work_item_id
   AND task.tenant_id=space.tenant_id AND task.owner_id=space.owner_id
  WHERE node.scope_kind=4 AND receipt.artifact_version_id=version.artifact_version_id
    AND receipt.artifact_id=artifact.artifact_id AND receipt.node_id=node.node_id
    AND receipt.canonical_version=version.version AND receipt.content_id=version.content_id
    AND receipt.content_hash=version.content_hash
    AND ((command.admission_kind='intent' AND command.work_item_run_id IS NULL)
      OR (command.admission_kind='work_item' AND command.work_item_run_id=command.run_id
        AND custody.work_item_id IS NOT NULL AND task.work_item_id IS NOT NULL AND task.erasure_job_id IS NULL))
) producer ON TRUE
WHERE node.tenant_id=$1 AND node.scope_kind=$2 AND node.scope_id=$3 AND node.lifecycle=1
