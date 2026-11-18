SELECT i.*,e.kind AS proof_entry_kind,e.source_intent_id AS proof_entry_intent,
        e.content_ref AS proof_entry_content,r.command_kind AS proof_command_kind,
        r.receipt AS proof_receipt,r.payload_sha256 AS proof_fingerprint,
        q.execution_target,q.target_device_id,q.target_installation_id,q.native_subject_proof,
        q.reply_origin_device_id,q.reply_origin_installation_id,q.reply_origin_subject_proof,q.desktop_source_membership_id,q.desktop_source_membership_epoch,CASE WHEN i.origin_surface='desktop' AND q.execution_target='cloud' THEN (to_jsonb(q)->>'origin_tool_device_id')::uuid END AS origin_tool_device_id,
        CASE WHEN i.origin_surface='schedule' THEN EXISTS(SELECT 1 FROM agent.owner_schedule_occurrences o
          JOIN agent.owner_schedules s USING(schedule_id) WHERE o.occurrence_id=i.command_id AND o.intent_id=i.intent_id
            AND o.status='accepted' AND s.tenant_id=i.tenant_id AND s.owner_id=i.owner_id AND s.agent_id=a.agent_id)
          ELSE false END AS proof_schedule
        FROM agent.dialogue_user_intents i
        JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
        JOIN agent.owned_agents a ON a.agent_id=d.agent_id AND a.dialogue_id=d.dialogue_id
        JOIN agent.agent_dialogue_entries e ON e.entry_id=i.source_entry_id AND e.tenant_id=i.tenant_id AND e.owner_id=i.owner_id AND e.dialogue_id=i.dialogue_id
        LEFT JOIN agent.owner_command_receipts r ON r.tenant_id=i.tenant_id AND r.owner_id=i.owner_id AND r.command_id=i.command_id
        LEFT JOIN agent.dialogue_intent_admissions q ON q.intent_id=i.intent_id
        WHERE i.intent_id=ANY($1::uuid[]) AND i.tenant_id=$2 AND i.owner_id=$3 AND a.agent_id=$4
          AND a.status='active' AND e.epoch=d.epoch AND e.erasure_job_id IS NULL
          AND e.epoch=i.accepted_epoch AND e.seq=i.accepted_seq
ORDER BY i.intent_id
