SELECT i.intent_id, q.desktop_source_membership_id, q.desktop_source_membership_epoch
FROM runtime.runs r
JOIN agent.dialogue_sessions s ON s.tenant_id=r.tenant_id AND s.conversation_id=r.conversation_id
JOIN agent.dialogue_user_intents i ON i.intent_id=s.source_intent_id AND i.dialogue_id=s.dialogue_id
    AND i.tenant_id=s.tenant_id AND i.owner_id=s.owner_id
JOIN agent.dialogue_intent_admissions q ON q.intent_id=i.intent_id
WHERE s.agent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND r.run_id=$4 AND i.dialogue_id=$5
  AND s.subject_kind='intent' AND s.purpose='intent_coordination'
  AND i.origin_surface='desktop' AND q.execution_target='cloud' AND q.desktop_source_membership_id IS NOT NULL
