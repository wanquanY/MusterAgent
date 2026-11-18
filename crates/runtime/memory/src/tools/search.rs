//! Published experience reads. Scope, provenance and content integrity remain
//! Host responsibilities; the retained program chooses relevance and order.
use super::*;
use crate::{ExperienceRankingCandidate, ReadPlan};

struct VerifiedExperience {
    candidate: ExperienceCandidate,
    document: ExperienceClaimDocument,
}

impl<M, E> BuiltinMemoryTools<M, E>
where
    M: MemoryStore + MemorySearchIndex,
    E: EvolutionStore,
{
    pub(super) async fn prepare_search(
        &self,
        context: MemoryToolContext,
        bytes: &[u8],
        execution: Option<&musteragent_runtime_capability::OperationContext>,
    ) -> Result<MemoryToolPreparation, MemoryToolError> {
        let input = match self.read_plan(&context, bytes, execution).await? {
            Some(ReadPlan::ExperienceSearch {
                query,
                kinds,
                limit,
            }) => SearchInput {
                query,
                kinds,
                limit,
            },
            Some(_) => return Err(MemoryToolError::InvalidResult),
            None => {
                let input: SearchInput =
                    serde_json::from_slice(bytes).map_err(|_| MemoryToolError::InvalidInput)?;
                input.validate()?;
                input
            }
        };
        let kinds = input
            .kinds
            .iter()
            .map(|value| experience_kind(value))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let candidates = crate::retrieval::read_stage(
            execution,
            self.evolution.search_published(ExperienceSearch {
                tenant_id: context.actor.tenant_id,
                principal_id: context.actor.principal_id,
                agent_ids: BTreeSet::from([context.agent_id]),
                kinds: kinds.clone(),
                limit: NonZeroU16::new(MAX_SEARCH_CANDIDATES)
                    .ok_or(MemoryToolError::InvalidResult)?,
            }),
        )
        .await
        .map_err(map_retrieval_error)?
        .map_err(|_| MemoryToolError::Store)?;
        if candidates.len() > usize::from(MAX_SEARCH_CANDIDATES) {
            return Err(MemoryToolError::InvalidResult);
        }
        let mut identities = BTreeSet::new();
        let mut verified = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let visible = match candidate.target_scope {
                EvolutionScope::User { principal_id } => principal_id == context.actor.principal_id,
                EvolutionScope::Agent { agent_id } => agent_id == context.agent_id,
            };
            if candidate.tenant_id != context.actor.tenant_id
                || !visible
                || candidate.status != CandidateStatus::Promoted
                || !kinds.is_empty() && !kinds.contains(&candidate.kind)
                || !identities.insert(candidate.proposal_id)
                || candidate.claim_ref.byte_length > 1024 * 1024
            {
                return Err(MemoryToolError::InvalidResult);
            }
            candidate
                .validate()
                .map_err(|_| MemoryToolError::InvalidResult)?;
            validate_claim_media_type(&candidate.claim_ref)?;
            let raw = crate::retrieval::read_stage(
                execution,
                self.content
                    .read(context.actor.tenant_id, &candidate.claim_ref),
            )
            .await
            .map_err(map_retrieval_error)?
            .map_err(|_| MemoryToolError::Content)?;
            musteragent_runtime_content::verify_content(&candidate.claim_ref, &raw)
                .map_err(|_| MemoryToolError::InvalidResult)?;
            let document = ExperienceClaimDocument::decode(&raw)
                .map_err(|_| MemoryToolError::InvalidResult)?;
            document
                .validate_for_kind(candidate.kind)
                .map_err(|_| MemoryToolError::InvalidResult)?;
            verified.push(VerifiedExperience {
                candidate,
                document,
            });
        }
        let positions = if let Some(planner) = &self.read_planner {
            let order = identities
                .iter()
                .zip(0_u16..)
                .map(|(id, index)| (*id, index))
                .collect::<std::collections::BTreeMap<_, _>>();
            let facts = verified
                .iter()
                .map(|value| ExperienceRankingCandidate {
                    searchable_text: value.document.searchable_text(),
                    created_at_ms: value.candidate.created_at.unix_millis(),
                    identity_ordinal: order[&value.candidate.proposal_id],
                })
                .collect::<Vec<_>>();
            crate::read_planner::complete_computation(
                execution,
                planner.rank_experiences(&input.query, &facts, input.limit),
            )
            .await?
        } else {
            // Finite compatibility for Sources admitted before the read profile.
            let terms = query_terms(&input.query);
            let mut matches = verified
                .iter()
                .zip(0_u16..)
                .filter_map(|(value, index)| {
                    let score = relevance(&terms, &value.document);
                    (score != 0 || terms.is_empty()).then_some((
                        score,
                        value.candidate.created_at,
                        value.candidate.proposal_id,
                        index,
                    ))
                })
                .collect::<Vec<_>>();
            matches.sort_by(|left, right| {
                right
                    .0
                    .cmp(&left.0)
                    .then_with(|| right.1.cmp(&left.1))
                    .then_with(|| left.2.cmp(&right.2))
            });
            matches
                .into_iter()
                .take(usize::from(input.limit))
                .map(|(_, _, _, position)| position)
                .collect()
        };
        validate_positions(&positions, verified.len(), input.limit)?;
        let mut values = verified.into_iter().map(Some).collect::<Vec<_>>();
        let mut experiences = Vec::with_capacity(positions.len());
        for position in positions {
            let value = values[usize::from(position)]
                .take()
                .ok_or(MemoryToolError::InvalidResult)?;
            experiences.push(SearchExperience {
                experience_id: value.candidate.proposal_id.to_string(),
                version_id: value.candidate.claim_ref.content_id.to_string(),
                kind: experience_kind_name(value.candidate.kind),
                content: serde_json::json!({"claim":value.document.claim, "outcome":value.document.outcome}),
                conditions: value.document.conditions,
                confidence: f64::from(value.candidate.confidence.map_or(0, ConfidenceBps::get)) / 10_000.0,
                source: "verified_experience_data",
            });
        }
        let output = serde_json::to_vec(&SearchOutput { experiences })
            .map_err(|_| MemoryToolError::InvalidResult)?;
        Ok(MemoryToolPreparation::ReadOnly(
            self.result(&context.invocation, output).await?,
        ))
    }
}

fn validate_positions(positions: &[u16], count: usize, limit: u16) -> Result<(), MemoryToolError> {
    if positions.len() > usize::from(limit)
        || positions
            .iter()
            .any(|position| usize::from(*position) >= count)
        || positions.iter().collect::<BTreeSet<_>>().len() != positions.len()
    {
        return Err(MemoryToolError::InvalidResult);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn program_may_choose_order_but_cannot_fabricate_duplicate_or_exceed_results() {
        assert!(validate_positions(&[2, 0], 3, 2).is_ok());
        for (positions, count, limit) in [
            (&[0][..], 0, 1),
            (&[3][..], 3, 2),
            (&[1, 1][..], 3, 2),
            (&[0, 1, 2][..], 3, 2),
        ] {
            assert!(validate_positions(positions, count, limit).is_err());
        }
    }
}
