SELECT
 EXISTS(SELECT 1 FROM agent.dialogue_user_intents i JOIN agent.agent_dialogues d USING(dialogue_id)
   JOIN agent.dialogue_intent_admissions q USING(intent_id)
   WHERE i.tenant_id=$1 AND i.owner_id=$2 AND d.agent_id=$3 AND q.status IN ('pending','claimed'))
 OR EXISTS(SELECT 1 FROM runtime.runs r JOIN agent.dialogue_sessions s ON s.conversation_id=r.conversation_id AND s.tenant_id=r.tenant_id
   WHERE s.tenant_id=$1 AND s.owner_id=$2 AND s.agent_id=$3 AND r.status BETWEEN 1 AND 4)
 OR EXISTS(SELECT 1 FROM agent.device_admissions d LEFT JOIN agent.native_dialogue_terminals t USING(run_id)
   WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3 AND t.run_id IS NULL
     AND NOT EXISTS(SELECT 1 FROM agent.device_source_not_admitted n WHERE n.intent_id=d.intent_id)
     AND NOT EXISTS(SELECT 1 FROM agent.native_execution_not_admitted n WHERE n.intent_id=d.intent_id))
 OR EXISTS(SELECT 1 FROM collaboration.work_items t JOIN collaboration.work_item_revisions r USING(work_item_id,state_revision)
   LEFT JOIN collaboration.work_item_execution_reservations x ON x.work_item_id=t.work_item_id AND x.goal_revision=r.goal_revision AND x.control_revision=r.control_revision AND x.assignment_epoch=r.assignment_epoch
   WHERE t.tenant_id=$1 AND t.owner_id=$2 AND (t.agent_id=$3 OR x.executing_agent_id=$3)
   AND t.erasure_job_id IS NULL AND r.status NOT IN ('completed','superseded','cancelled'))
