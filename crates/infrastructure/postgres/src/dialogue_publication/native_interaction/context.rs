//! Read the original public Native projection, never an unacknowledged command.
use super::*;
use musteragent_execution_context::{
    DialogueApprovalContextKind as Kind, DialogueApprovalContextState as State,
    DialogueApprovalObservation,
};
impl PostgresNativeDialogueInteractionStore {
    pub(crate) async fn published_context_observations(
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        dialogue: AgentDialogueId,
        entries: &[uuid::Uuid],
    ) -> Result<Vec<DialogueApprovalObservation>, RuntimeError> {
        if entries.is_empty() {
            return Ok(vec![]);
        }
        if entries.len() > musteragent_execution_context::DIALOGUE_APPROVAL_CONTEXT_LIMIT {
            return Err(fail(ErrorCode::InvalidCommand));
        }
        let rows = controls::entry_rows(conn, actor, dialogue, entries, false).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let entry = row_id(&row, "context_entry_id")?;
            let sequence = u64::try_from(row.try_get::<i64, _>("context_seq").map_err(database)?)
                .map_err(corrupt)?;
            let p = row_projection(&row)?;
            let kind = match p.interaction.kind {
                InteractionKind::Approval => Kind::Approval,
                InteractionKind::Confirmation => Kind::Confirmation,
                _ => continue,
            };
            let state = match (p.interaction.status, &p.resolution) {
                (InteractionStatus::Pending, None) => State::Pending,
                (InteractionStatus::Expired, None) => State::Expired,
                (InteractionStatus::Cancelled, None) => State::Cancelled,
                (InteractionStatus::Responded, Some(DialogueInteractionResolution::Approved)) => {
                    State::Approved
                }
                (InteractionStatus::Responded, Some(DialogueInteractionResolution::Denied)) => {
                    State::Denied
                }
                (InteractionStatus::Responded, None) => State::ResolvedDecisionUnavailable,
                _ => return Err(fail(ErrorCode::ContentIntegrityFailed)),
            };
            result.push(DialogueApprovalObservation {
                entry_id: entry,
                sequence,
                interaction_id: p.interaction.interaction_id,
                run_id: p.interaction.run_id,
                visible_revision: std::num::NonZeroU64::new(
                    u64::try_from(
                        row.try_get::<i64, _>("visible_revision")
                            .map_err(database)?,
                    )
                    .map_err(corrupt)?,
                )
                .ok_or_else(|| fail(ErrorCode::ContentIntegrityFailed))?,
                control_revision: p.state_fingerprint(),
                kind,
                state,
            });
        }
        Ok(result)
    }
}
