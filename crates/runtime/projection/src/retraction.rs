use std::collections::{BTreeMap, BTreeSet, VecDeque};

use musteragent_identity::{ActivityId, MessageId, RunId};

use crate::{
    ConversationProjectionSnapshot, ProjectedActivity, ProjectedRun, ProjectionManifestError,
    canonical_content_manifest,
};

/// Canonical semantic scope of one `MessageRetracted` product projection fact.
///
/// Storage adapters may keep richer mutable build state, but they must use this
/// scope for every retention decision. This keeps PostgreSQL, future Device
/// storage and terminal Snapshot + Tail reducers on one closed rule set.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectionRetraction {
    message_id: MessageId,
    source_run_id: Option<RunId>,
    retracted_run_ids: BTreeSet<RunId>,
    retracted_activity_ids: BTreeSet<ActivityId>,
}

impl ProjectionRetraction {
    #[must_use]
    pub fn new(message_id: MessageId, source_run_id: Option<RunId>) -> Self {
        let retracted_run_ids = source_run_id.into_iter().collect();
        Self {
            message_id,
            source_run_id,
            retracted_run_ids,
            retracted_activity_ids: BTreeSet::new(),
        }
    }

    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.message_id
    }

    #[must_use]
    pub const fn source_run_id(&self) -> Option<RunId> {
        self.source_run_id
    }

    #[must_use]
    pub fn with_inferred_run(mut self, source_run_id: Option<RunId>) -> Self {
        self.source_run_id = self.source_run_id.or(source_run_id);
        if let Some(run_id) = source_run_id {
            self.retracted_run_ids.insert(run_id);
        }
        self
    }

    /// Expands the direct message/Run scope to the complete derived execution
    /// graph. A Run retraction owns every retry in the same Activity plus every
    /// descendant Run and Activity. This is deliberately computed by the
    /// shared projection layer so Snapshot + Tail and rebuild-from-zero cannot
    /// disagree, and so a failed child without an Assistant message cannot be
    /// left behind as an orphan.
    #[must_use]
    pub fn with_projection_graph<'a>(
        mut self,
        runs: impl IntoIterator<Item = &'a ProjectedRun>,
        activities: impl IntoIterator<Item = &'a ProjectedActivity>,
    ) -> Self {
        let mut run_activities = BTreeMap::<RunId, ActivityId>::new();
        let mut child_runs = BTreeMap::<RunId, Vec<RunId>>::new();
        let mut activity_runs = BTreeMap::<ActivityId, Vec<RunId>>::new();
        for run in runs {
            run_activities.insert(run.run_id, run.activity_id);
            activity_runs
                .entry(run.activity_id)
                .or_default()
                .push(run.run_id);
            if let Some(parent_run_id) = run.parent_run_id {
                child_runs
                    .entry(parent_run_id)
                    .or_default()
                    .push(run.run_id);
            }
            if run.input_message_id == Some(self.message_id)
                || run.retry_source_message_id == Some(self.message_id)
            {
                self.retracted_run_ids.insert(run.run_id);
            }
        }
        let mut child_activities = BTreeMap::<ActivityId, Vec<ActivityId>>::new();
        for activity in activities {
            if let Some(parent_activity_id) = activity.parent_activity_id {
                child_activities
                    .entry(parent_activity_id)
                    .or_default()
                    .push(activity.activity_id);
            }
        }

        let mut pending_runs = self
            .retracted_run_ids
            .iter()
            .copied()
            .collect::<VecDeque<_>>();
        let mut pending_activities = VecDeque::new();
        while !pending_runs.is_empty() || !pending_activities.is_empty() {
            if let Some(run_id) = pending_runs.pop_front() {
                if let Some(activity_id) = run_activities.get(&run_id).copied()
                    && self.retracted_activity_ids.insert(activity_id)
                {
                    pending_activities.push_back(activity_id);
                }
                if let Some(children) = child_runs.get(&run_id) {
                    for child_run_id in children {
                        if self.retracted_run_ids.insert(*child_run_id) {
                            pending_runs.push_back(*child_run_id);
                        }
                    }
                }
                continue;
            }
            let Some(activity_id) = pending_activities.pop_front() else {
                break;
            };
            if let Some(runs) = activity_runs.get(&activity_id) {
                for run_id in runs {
                    if self.retracted_run_ids.insert(*run_id) {
                        pending_runs.push_back(*run_id);
                    }
                }
            }
            if let Some(children) = child_activities.get(&activity_id) {
                for child_activity_id in children {
                    if self.retracted_activity_ids.insert(*child_activity_id) {
                        pending_activities.push_back(*child_activity_id);
                    }
                }
            }
        }
        self
    }

    #[must_use]
    pub fn retracts_message(
        &self,
        candidate_message_id: MessageId,
        candidate_run_id: Option<RunId>,
    ) -> bool {
        candidate_message_id == self.message_id
            || candidate_run_id.is_some_and(|run_id| self.retracted_run_ids.contains(&run_id))
    }

    #[must_use]
    pub fn retracts_run(&self, candidate_run_id: RunId) -> bool {
        self.retracted_run_ids.contains(&candidate_run_id)
    }

    #[must_use]
    pub fn retracts_activity(&self, candidate_activity_id: ActivityId) -> bool {
        self.retracted_activity_ids.contains(&candidate_activity_id)
    }

    #[must_use]
    pub fn retracts_output(
        &self,
        candidate_message_id: Option<MessageId>,
        candidate_run_id: RunId,
    ) -> bool {
        self.retracts_run(candidate_run_id) || candidate_message_id == Some(self.message_id)
    }

    #[must_use]
    pub fn retracts_artifact(
        &self,
        created_by_run_id: Option<RunId>,
        activity_id: Option<ActivityId>,
    ) -> bool {
        created_by_run_id.is_some_and(|run_id| self.retracts_run(run_id))
            || activity_id.is_some_and(|activity_id| self.retracts_activity(activity_id))
    }

    /// Applies a canonical Tail retraction to an already installed product
    /// Snapshot. The caller advances the Cursor and recomputes the Snapshot
    /// checksum after applying the complete Tail batch.
    pub fn apply_to_snapshot(
        self,
        snapshot: &mut ConversationProjectionSnapshot,
    ) -> Result<(), ProjectionManifestError> {
        let inferred = snapshot
            .messages
            .iter()
            .find(|message| message.message_id == self.message_id)
            .and_then(|message| message.source_run_id);
        let scope = self
            .with_inferred_run(inferred)
            .with_projection_graph(snapshot.runs.iter(), snapshot.activities.iter());
        let retracted_message_ids = snapshot
            .messages
            .iter()
            .filter(|message| scope.retracts_message(message.message_id, message.source_run_id))
            .map(|message| message.message_id)
            .collect::<BTreeSet<_>>();

        snapshot
            .messages
            .retain(|message| !scope.retracts_message(message.message_id, message.source_run_id));
        snapshot
            .tool_timeline
            .retain(|tool| !scope.retracts_run(tool.run_id));
        snapshot
            .context_compactions
            .retain(|compaction| !scope.retracts_run(compaction.run_id));
        snapshot
            .pending_interactions
            .retain(|interaction| !scope.retracts_run(interaction.run_id));
        snapshot.runs.retain(|run| !scope.retracts_run(run.run_id));
        snapshot
            .active_runs
            .retain(|run| !scope.retracts_run(run.run_id));
        snapshot
            .activities
            .retain(|activity| !scope.retracts_activity(activity.activity_id));
        snapshot.artifacts.retain(|artifact| {
            !scope.retracts_artifact(artifact.created_by_run_id, artifact.activity_id)
        });
        snapshot.stream_outputs.retain(|output| {
            !scope.retracts_output(output.message_id, output.run_id)
                && output
                    .message_id
                    .is_none_or(|message_id| !retracted_message_ids.contains(&message_id))
        });
        snapshot.content_manifest = canonical_content_manifest(snapshot)?;
        Ok(())
    }
}
