//! Typed authority codec. Human prompt material is never parsed as authority.
use super::*;
use musteragent_runtime_snapshot::OwnerGoalSnapshotAuthority;

#[allow(deprecated)] // Explicitly absent retired draft field.
pub(crate) fn encode(
    v: &OwnerGoalSnapshotAuthority,
) -> Result<wire::OwnerGoalSnapshotAuthority, SnapshotCodecError> {
    v.validate()
        .map_err(|_| SnapshotCodecError::InvalidContract)?;
    Ok(wire::OwnerGoalSnapshotAuthority {
        goal_id: Some(wire::GoalId {
            value: v.goal_id.to_string(),
        }),
        pursuit_id: Some(wire::GoalPursuitId {
            value: v.pursuit_id.to_string(),
        }),
        binding_revision: v.binding_revision,
        permission_revision: v.permission_revision,
        manifest_revision: v.manifest_revision,
        instructions_revision: v.instructions_revision,
        context_cut: Some(crate::dialogue_context_codec::cut(&v.context_cut)),
    })
}
#[allow(deprecated)] // Reject the retired draft field, never hydrate it.
pub(super) fn decode(
    v: wire::OwnerGoalSnapshotAuthority,
) -> Result<OwnerGoalSnapshotAuthority, SnapshotCodecError> {
    let invalid = || SnapshotCodecError::InvalidContract;
    let value = OwnerGoalSnapshotAuthority {
        goal_id: v
            .goal_id
            .ok_or_else(invalid)?
            .value
            .parse()
            .map_err(|_| invalid())?,
        pursuit_id: v
            .pursuit_id
            .ok_or_else(invalid)?
            .value
            .parse()
            .map_err(|_| invalid())?,
        binding_revision: v.binding_revision,
        permission_revision: v.permission_revision,
        manifest_revision: v.manifest_revision,
        instructions_revision: v.instructions_revision,
        context_cut: crate::dialogue_context_codec::decode_cut(v.context_cut.ok_or_else(invalid)?)
            .map_err(|_| invalid())?,
    };
    value.validate().map_err(|_| invalid())?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use musteragent_execution_context::{DialogueContextCut, DialogueCursor};
    use musteragent_identity::{
        AgentDialogueId, AgentId, GoalId, GoalPursuitId, PrincipalId, TenantId,
    };
    use prost::Message;

    fn authority() -> OwnerGoalSnapshotAuthority {
        let mut cut = DialogueContextCut {
            tenant_id: TenantId::generate(),
            owner_id: PrincipalId::generate(),
            agent_id: AgentId::generate(),
            dialogue_id: AgentDialogueId::generate(),
            through: DialogueCursor { epoch: 1, seq: 0 },
            entry_count: 0,
            source_chain_sha256: Sha256::digest([]),
        };
        cut.source_chain_sha256 = cut.empty_chain_hash();
        OwnerGoalSnapshotAuthority {
            goal_id: GoalId::generate(),
            pursuit_id: GoalPursuitId::generate(),
            binding_revision: 1,
            permission_revision: 2,
            manifest_revision: 3,
            instructions_revision: 4,
            context_cut: cut,
        }
    }

    #[test]
    fn typed_authority_round_trips_and_is_retained_by_the_original_authority_component() {
        let original = authority();
        assert_eq!(decode(encode(&original).unwrap()).unwrap(), original);
        let snapshot = wire::RuntimeSnapshotContent {
            schema_version: SNAPSHOT_SCHEMA_VERSION.into(),
            owner_goal_authority: Some(encode(&original).unwrap()),
            ..Default::default()
        };
        let bytes = snapshot.encode_to_vec();
        let parts = crate::split_runtime_snapshot_components(&bytes).unwrap();
        let authority_part = parts
            .iter()
            .find(|p| p.kind == crate::RuntimeSnapshotComponentKind::Authority)
            .unwrap();
        assert_eq!(
            wire::RuntimeSnapshotContent::decode(authority_part.encoded_fragment.as_slice())
                .unwrap()
                .owner_goal_authority,
            snapshot.owner_goal_authority
        );
        assert_eq!(
            crate::assemble_runtime_snapshot_components(&parts).unwrap(),
            bytes
        );
        let mut changed = snapshot.clone();
        changed
            .owner_goal_authority
            .as_mut()
            .unwrap()
            .permission_revision += 1;
        assert_ne!(
            Sha256::digest(changed.encode_to_vec()),
            Sha256::digest(bytes)
        );
    }

    #[test]
    fn partial_authority_and_invalid_foreign_cut_are_rejected() {
        let original = encode(&authority()).unwrap();
        let mut broken = original.clone();
        broken.goal_id = None;
        assert!(decode(broken).is_err());
        let mut broken = original.clone();
        broken.permission_revision = 0;
        assert!(decode(broken).is_err());
        let mut broken = original.clone();
        broken.context_cut.as_mut().unwrap().owner_id = Some(wire::PrincipalId {
            value: PrincipalId::generate().to_string(),
        });
        assert!(decode(broken).is_err());
    }
}
