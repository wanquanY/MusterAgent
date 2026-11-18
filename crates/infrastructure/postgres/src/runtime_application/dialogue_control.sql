SELECT space.space_id FROM agent.dialogue_execution_cancellations stop

JOIN agent.dialogue_execution_cancellation_targets target ON target.cancellation_id=stop.command_id

JOIN agent.dialogue_execution_cancellation_roots stopped_root ON stopped_root.cancellation_id=stop.command_id
  AND stopped_root.tenant_id=stop.tenant_id AND stopped_root.owner_id=stop.owner_id
  AND stopped_root.agent_id=stop.agent_id AND stopped_root.dialogue_id=stop.dialogue_id

JOIN agent.dialogue_sessions session ON session.tenant_id=stop.tenant_id
  AND session.owner_id=stop.owner_id
  AND session.agent_id=stop.agent_id
  AND session.dialogue_id=stop.dialogue_id
  AND session.source_intent_id=target.intent_id
  AND session.conversation_id=target.conversation_id

JOIN agent.dialogue_user_intents intent ON intent.intent_id=session.source_intent_id
  AND intent.tenant_id=session.tenant_id
  AND intent.owner_id=session.owner_id

JOIN agent.agent_dialogue_entries entry ON entry.entry_id=intent.source_entry_id
  AND entry.dialogue_id=intent.dialogue_id
  AND entry.epoch=intent.accepted_epoch
  AND entry.seq=intent.accepted_seq
  AND entry.erasure_job_id IS NULL

JOIN agent.agent_dialogues dialogue ON dialogue.dialogue_id=session.dialogue_id
  AND dialogue.tenant_id=session.tenant_id
  AND dialogue.owner_id=session.owner_id
  AND dialogue.epoch=entry.epoch

JOIN agent.owned_agents agent ON agent.agent_id=session.agent_id
  AND agent.dialogue_id=session.dialogue_id
  AND agent.status<>'deleted'

JOIN agent.owner_spaces space ON space.space_id=agent.space_id
  AND space.tenant_id=stop.tenant_id
  AND space.owner_id=stop.owner_id
  AND space.product='musteragent'

JOIN runtime.runs run ON run.tenant_id=session.tenant_id
  AND run.conversation_id=session.conversation_id
  AND run.run_id=target.run_id

JOIN conversation.conversations conversation ON conversation.tenant_id=run.tenant_id
  AND conversation.conversation_id=run.conversation_id
  AND conversation.purpose=session.conversation_purpose
  AND conversation.created_by=space.owner_id
  AND conversation.home_epoch=run.home_epoch

WHERE stop.command_id=$1
  AND stop.tenant_id=$2
  AND stop.owner_id=$3
  AND stop.agent_id=$4
  AND stop.dialogue_id=$5

  AND target.command_id=$6
  AND target.run_id=$7
  AND target.intent_id=$8
  AND target.conversation_id=$9

  AND session.purpose='intent_coordination'
  AND session.subject_kind='intent'
  AND conversation.purpose='dialogue_intent_coordination'

  AND EXISTS(SELECT 1 FROM agent.dialogue_execution_source_roots source
JOIN agent.dialogue_user_intents original ON original.intent_id=source.root_intent_id
  AND original.tenant_id=source.tenant_id
  AND original.owner_id=source.owner_id
  AND original.dialogue_id=source.dialogue_id
  AND original.source_entry_id=source.root_entry_id
  AND original.content_ref=source.root_content_ref
  AND original.accepted_epoch=source.root_epoch
  AND original.accepted_seq=source.root_seq
JOIN agent.agent_dialogue_entries root_entry ON root_entry.entry_id=source.root_entry_id
  AND root_entry.dialogue_id=source.dialogue_id
  AND root_entry.epoch=source.root_epoch
  AND root_entry.seq=source.root_seq
  AND root_entry.content_ref=source.root_content_ref
  AND root_entry.erasure_job_id IS NULL
WHERE source.intent_id=intent.intent_id
  AND source.tenant_id=intent.tenant_id
  AND source.owner_id=intent.owner_id
  AND source.agent_id=agent.agent_id
  AND source.dialogue_id=dialogue.dialogue_id
  AND source.source_entry_id=intent.source_entry_id
  AND source.source_content_ref=intent.content_ref
  AND source.source_epoch=intent.accepted_epoch
  AND source.source_seq=intent.accepted_seq
  AND source.root_epoch=dialogue.epoch
  AND source.direct_user_root
  AND source.root_intent_id=stopped_root.root_intent_id
  AND source.root_entry_id=stopped_root.root_entry_id
  AND source.root_content_ref=stopped_root.root_content_ref
  AND source.root_epoch=stopped_root.root_epoch
  AND source.root_seq=stopped_root.root_seq)
  AND run.parent_run_id IS NULL
  AND run.delegation_id IS NULL
  AND run.erasure_job_id IS NULL

  AND run.control_home_kind=1
  AND run.control_device_id IS NULL
  AND run.execution_home_kind=1
  AND run.execution_device_id IS NULL

  AND target.run_id IS NOT NULL AND target.native_target IS NULL
