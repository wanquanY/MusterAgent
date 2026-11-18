SELECT candidate.intent_id FROM (
SELECT DISTINCT s.source_intent_id AS intent_id FROM agent.dialogue_sessions s
JOIN runtime.runs r ON r.tenant_id=s.tenant_id
  AND r.conversation_id=s.conversation_id
LEFT JOIN agent.dialogue_execution_source_roots root ON root.intent_id=s.source_intent_id
WHERE s.tenant_id=$1
  AND s.owner_id=$2
  AND s.agent_id=$3
  AND s.purpose='intent_coordination'
  AND s.subject_kind='intent'
  AND r.status IN(1,2,3,4)
  AND root.intent_id IS NULL

UNION
SELECT i.intent_id FROM agent.dialogue_user_intents i JOIN agent.dialogue_intent_admissions q USING(intent_id) JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id WHERE i.tenant_id=$1 AND i.owner_id=$2 AND d.agent_id=$3 AND q.status IN('pending','claimed') AND NOT EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots root WHERE root.intent_id=i.intent_id)
UNION
SELECT d.intent_id FROM agent.device_admissions d
WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3
AND NOT EXISTS(SELECT 1 FROM agent.native_dialogue_terminals t WHERE t.run_id=d.run_id)
AND NOT EXISTS(SELECT 1 FROM agent.native_execution_not_admitted n WHERE n.intent_id=d.intent_id)
AND NOT EXISTS(SELECT 1 FROM agent.device_source_not_admitted n WHERE n.intent_id=d.intent_id)
AND NOT EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots root WHERE root.intent_id=d.intent_id)
) candidate ORDER BY intent_id LIMIT $4
