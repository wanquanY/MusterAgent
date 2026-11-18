SELECT EXISTS(SELECT i.intent_id FROM agent.dialogue_user_intents i
JOIN agent.dialogue_intent_admissions q USING(intent_id)
JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id AND d.tenant_id=i.tenant_id AND d.owner_id=i.owner_id
JOIN agent.dialogue_execution_visible_source_roots root ON root.intent_id=i.intent_id
  AND root.tenant_id=i.tenant_id AND root.owner_id=i.owner_id
  AND root.agent_id=d.agent_id AND root.dialogue_id=d.dialogue_id
WHERE i.tenant_id=$1 AND i.owner_id=$2 AND d.agent_id=$3 AND root.direct_user_root
AND i.intent_id=$4)
