SELECT a.artifact_id,a.tenant_id,a.conversation_id,a.workspace_id,a.activity_id,
          a.created_by_run_id,a.origin,a.kind,a.lifecycle,a.current_version::text AS current_version,
          COALESCE(CASE WHEN node.name_erasure_job_id IS NULL THEN node.name END,
           CASE WHEN a.title_erasure_job_id IS NULL THEN a.title ELSE 'Untitled' END) AS title,
          a.created_at_ms,a.updated_at_ms,e.artifact_version_id AS delivered,admitted.agent_id
         FROM agent.agent_dialogue_entries e
         JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id AND p.tenant_id=e.tenant_id
          AND p.owner_id=e.owner_id AND p.dialogue_id=e.dialogue_id AND p.agent_id=e.author_agent_id
          AND p.execution_home='device' AND p.source_kind='tool_owner'
         -- The public entry retains its original execution subject. WorkItem
         -- Native runs have no ordinary Intent admission or Cloud Run surrogate.
         JOIN LATERAL (
           SELECT d.tenant_id,d.owner_id,d.agent_id,d.dialogue_id,d.intent_id,
             d.conversation_id,d.run_id,d.admitted_snapshot_hash,g.signed_grant_hash
           FROM agent.device_admissions d
           JOIN agent.device_grant_heads h ON h.intent_id=d.intent_id
           JOIN agent.device_grant_versions g ON g.intent_id=h.intent_id
             AND g.revision=h.admitted_revision
           WHERE p.device_subject_kind='intent' AND d.tenant_id=p.tenant_id AND d.owner_id=p.owner_id
             AND d.agent_id=p.agent_id AND d.dialogue_id=p.dialogue_id AND d.intent_id=p.source_intent_id
             AND d.conversation_id=p.source_conversation_id AND d.run_id=p.source_run_id
             AND d.admitted_at_ms IS NOT NULL
           UNION ALL
           SELECT r.tenant_id,r.owner_id,r.executing_agent_id,executing.dialogue_id,w.source_intent_id,
             d.conversation_id,d.run_id,d.admitted_snapshot_hash,d.signed_grant_hash
           FROM collaboration.work_item_device_admitted_custody d
           JOIN collaboration.work_item_execution_reservations r
             USING(work_item_id,goal_revision,control_revision,assignment_epoch)
           JOIN collaboration.work_items w ON w.work_item_id=r.work_item_id
             AND w.tenant_id=r.tenant_id AND w.owner_id=r.owner_id AND w.erasure_job_id IS NULL
           JOIN agent.owned_agents executing ON executing.agent_id=r.executing_agent_id
           WHERE p.device_subject_kind='work_item' AND d.run_id=p.source_run_id
             AND r.run_id=d.run_id AND r.conversation_id=d.conversation_id
             AND r.tenant_id=p.tenant_id AND r.owner_id=p.owner_id AND r.executing_agent_id=p.agent_id
             AND executing.dialogue_id=p.dialogue_id AND w.source_intent_id=p.source_intent_id
             AND d.conversation_id=p.source_conversation_id AND d.admitted_at_ms IS NOT NULL
         ) admitted ON TRUE
         JOIN agent.dialogue_tool_publication_groups published ON published.invocation_id=p.source_tool_invocation_id
          AND published.tenant_id=admitted.tenant_id AND published.source_intent_id=admitted.intent_id
          AND published.source_run_id=admitted.run_id AND published.execution_home='device'
          AND published.device_snapshot_hash=admitted.admitted_snapshot_hash AND published.device_grant_proof_hash=admitted.signed_grant_hash
          AND published.device_subject_kind=p.device_subject_kind
          AND (published.device_export_commitment IS NOT NULL OR published.device_resource_commitment IS NOT NULL
            OR (published.tool_ref='artifacts.deliver@2' AND published.device_receipt_content_id IS NOT NULL))
         JOIN agent.owned_agents agent ON agent.agent_id=admitted.agent_id AND agent.dialogue_id=admitted.dialogue_id
         JOIN agent.owner_spaces s ON s.space_id=agent.space_id AND s.tenant_id=admitted.tenant_id
          AND s.owner_id=admitted.owner_id AND s.product='musteragent'
         JOIN artifact.artifacts a ON a.artifact_id=e.artifact_id AND a.tenant_id=admitted.tenant_id
          AND a.lifecycle IN (1,2)
          AND resource.agent_artifact_owned(a.tenant_id,admitted.agent_id,a.artifact_id)
         JOIN artifact.artifact_versions delivered ON delivered.artifact_id=a.artifact_id
          AND delivered.artifact_version_id=e.artifact_version_id AND delivered.version<=a.current_version
          AND ((published.device_export_commitment IS NOT NULL AND a.conversation_id=admitted.conversation_id AND artifact.device_version_owned(a.tenant_id,admitted.owner_id,admitted.agent_id,a.artifact_id,delivered.artifact_version_id))
            OR (published.device_resource_commitment IS NOT NULL AND EXISTS(SELECT 1 FROM agent.device_resource_publications proof JOIN resource.device_commands command ON command.invocation_id=proof.producer_invocation_id WHERE proof.publication_invocation_id=published.invocation_id AND proof.role='delivery' AND proof.artifact_version_id=delivered.artifact_version_id AND command.tenant_id=admitted.tenant_id AND command.owner_id=admitted.owner_id AND command.agent_id=admitted.agent_id AND command.run_id=admitted.run_id AND command.intent_id=admitted.intent_id))
            OR (published.tool_ref='artifacts.deliver@2' AND published.device_export_commitment IS NULL
              AND published.device_resource_commitment IS NULL AND published.device_receipt_content_id IS NOT NULL
              AND delivered.work_item_erasure_job_id IS NULL
              AND EXISTS(SELECT 1 FROM agent.dialogue_content_references retained
                JOIN artifact.content_refs content ON content.content_id=retained.content_id
                  AND content.tenant_id=retained.tenant_id AND content.state=2
                  AND content.content_hash=delivered.content_hash
                WHERE retained.tenant_id=e.tenant_id AND retained.owner_id=e.owner_id
                  AND retained.agent_id=admitted.agent_id AND retained.reference_kind='public_entry'
                  AND retained.reference_id=e.entry_id AND retained.content_id=delivered.content_id)))
         LEFT JOIN resource.nodes node ON node.tenant_id=a.tenant_id AND node.file_artifact_id=a.artifact_id
          AND node.scope_kind=4 AND node.scope_id=admitted.agent_id AND node.kind=2 AND node.lifecycle=1
         WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3 AND e.entry_id=$4 AND e.kind='artifact'
          AND (agent.status='active' OR ($5 AND agent.status='archived'))
