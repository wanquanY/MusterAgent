//! Original Publication reference resolution, with source failure kept separate
//! from recoverable object absence for strict V7 validation.
use super::*;

pub(crate) enum CandidateObject {
    Available(Option<PublicDeliveryContent>),
    ReferenceUnavailable,
}

pub(super) async fn resolve(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    run_id: RunId,
    draft: &PublicDeliveryDraft,
) -> Result<Option<PublicDeliveryContent>, Error> {
    let object = match draft {
        PublicDeliveryDraft::Message(_) => return Ok(None),
        PublicDeliveryDraft::PartnerCreated { .. } => {
            return Err(DialogueStoreError::Unavailable.into());
        }
        PublicDeliveryDraft::Interaction(id) => PublicDeliveryContent::Interaction(*id),
        PublicDeliveryDraft::Artifact {
            artifact_id,
            artifact_version_id,
        } => PublicDeliveryContent::Artifact {
            artifact_id: *artifact_id,
            artifact_version_id: *artifact_version_id,
        },
        PublicDeliveryDraft::ArtifactFromTool {
            artifact,
            artifact_tool_call_id,
        } => {
            references::resolve_tool_artifact(tx, source, run_id, *artifact, artifact_tool_call_id)
                .await?
        }
    };
    references::resolve_reference(tx, source, run_id, &object).await?;
    Ok(Some(object))
}
