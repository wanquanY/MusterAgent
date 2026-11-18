use musteragent_identity::AgentId;
use std::collections::BTreeSet;

pub const SCENE_MEMBER_CONTEXT_LIMIT: usize = 64;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AgentSceneMember {
    pub agent_id: AgentId,
    pub name: String,
    pub responsibility: String,
}

pub fn valid_scene_members(primary: AgentId, members: &[AgentSceneMember]) -> bool {
    let mut ids = BTreeSet::new();
    members.len() <= SCENE_MEMBER_CONTEXT_LIMIT
        && members.iter().all(|m| {
            m.agent_id != primary
                && ids.insert(m.agent_id)
                && !m.name.trim().is_empty()
                && m.name.chars().count() <= 120
                && m.responsibility.chars().count() <= 160
        })
}
