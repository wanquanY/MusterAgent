//! Immutable prepared-plan revisions. Storage selects authoritative source
//! facts; this pure rule never grants model dispatch or consumes a control.
use std::num::NonZeroU32;

use musteragent_content_ref::Sha256;
use musteragent_identity::{ContextSnapshotId, ConversationSeq, LedgerRecordId};
use musteragent_run::RunControlKind;

use crate::{CONTEXT_PLAN_GENERATION, ContextPlanError, StoredContextPlanIdentity};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextPlanControlCut {
    pub sequence: ConversationSeq,
    pub record_id: LedgerRecordId,
    pub kind: RunControlKind,
}

impl ContextPlanControlCut {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        if !matches!(
            self.kind,
            RunControlKind::Steering | RunControlKind::FollowUp
        ) {
            return Err(ContextPlanError::invariant());
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextPlanHeadRef {
    pub snapshot_id: ContextSnapshotId,
    pub generation: NonZeroU32,
    pub checksum: Sha256,
}

/// Forward snapshot metadata. Absence retains the complete legacy generation-1
/// contract; a new chain starts explicitly with Some(generation=1).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextPlanLineage {
    pub generation: NonZeroU32,
    pub predecessor: Option<ContextPlanHeadRef>,
    pub control: Option<ContextPlanControlCut>,
}

impl ContextPlanLineage {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        if self.generation.get() > i32::MAX as u32 {
            return Err(ContextPlanError::invariant());
        }
        if let Some(control) = self.control {
            control.validate()?;
        }
        match self.predecessor {
            None if self.generation == CONTEXT_PLAN_GENERATION => Ok(()),
            Some(previous)
                if previous.generation.get().checked_add(1) == Some(self.generation.get())
                    && self.control.is_some() =>
            {
                Ok(())
            }
            _ => Err(ContextPlanError::invariant()),
        }
    }
}

/// Original Store observation, not a caller-issued revision or authority token.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextPlanHead {
    pub identity: StoredContextPlanIdentity,
    pub checksum: Sha256,
    pub committed_sequence: ConversationSeq,
    pub lineage: Option<ContextPlanLineage>,
    /// True if ANY generation in this logical slot has ModelStarted, including
    /// an unknown or terminal outcome. Such a slot cannot be superseded.
    pub model_started: bool,
}

impl ContextPlanHead {
    #[must_use]
    pub fn reference(&self) -> ContextPlanHeadRef {
        ContextPlanHeadRef {
            snapshot_id: self.identity.snapshot_id,
            generation: self.identity.generation,
            checksum: self.checksum,
        }
    }

    pub fn validate(&self) -> Result<(), ContextPlanError> {
        match self.lineage {
            Some(lineage) => {
                lineage.validate()?;
                if lineage.generation != self.identity.generation
                    || lineage
                        .control
                        .is_some_and(|c| c.sequence > self.committed_sequence)
                {
                    return Err(ContextPlanError::invariant());
                }
            }
            None if self.identity.generation != CONTEXT_PLAN_GENERATION => {
                return Err(ContextPlanError::invariant());
            }
            None => {}
        }
        Ok(())
    }
}

/// The original commit transaction re-reads both expected_head and the exact
/// authenticated source_control. Building a request does not reserve a slot.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextPlanSelection {
    pub expected_head: Option<ContextPlanHeadRef>,
    pub source_control: Option<ContextPlanControlCut>,
    pub lineage: Option<ContextPlanLineage>,
}

impl ContextPlanSelection {
    #[must_use]
    pub fn generation(&self) -> NonZeroU32 {
        self.lineage
            .map_or(CONTEXT_PLAN_GENERATION, |v| v.generation)
    }

    pub fn validate(&self) -> Result<(), ContextPlanError> {
        if let Some(control) = self.source_control {
            control.validate()?;
        }
        if let Some(lineage) = self.lineage {
            lineage.validate()?;
            if self.source_control != lineage.control {
                return Err(ContextPlanError::invariant());
            }
        }
        match (self.expected_head, self.lineage) {
            (None, Some(lineage)) if lineage.generation == CONTEXT_PLAN_GENERATION => Ok(()),
            (Some(head), None) if head.generation == CONTEXT_PLAN_GENERATION => Ok(()),
            (Some(head), Some(lineage))
                if lineage.generation == head.generation || lineage.predecessor == Some(head) =>
            {
                Ok(())
            }
            _ => Err(ContextPlanError::invariant()),
        }
    }
}

/// Reuse the original content window while observing plans and steering at the current head.
pub fn select_context_plan_for_sealed_input(
    head: Option<&ContextPlanHead>,
    current_control: Option<ContextPlanControlCut>,
    source_cutoff: ConversationSeq,
    observed_ledger_head: ConversationSeq,
) -> Result<ContextPlanSelection, ContextPlanError> {
    if source_cutoff > observed_ledger_head
        || current_control.is_some_and(|cut| cut.sequence > source_cutoff)
    {
        return Err(ContextPlanError::unavailable());
    }
    // The original input precedes its own plan commit. Use the observed ledger
    // head to verify that commit while keeping later steering out of its input.
    select_context_plan_generation(head, current_control, observed_ledger_head)
}

/// Same source reuses its exact immutable revision, even across Attempt
/// takeover. Only newly consumed input may replace an unstarted plan. Storage
/// rejects ambiguous legacy rows before constructing `head`.
pub fn select_context_plan_generation(
    head: Option<&ContextPlanHead>,
    source_control: Option<ContextPlanControlCut>,
    source_cutoff: ConversationSeq,
) -> Result<ContextPlanSelection, ContextPlanError> {
    if let Some(control) = source_control {
        control.validate()?;
        if control.sequence > source_cutoff {
            return Err(ContextPlanError::invariant());
        }
    }
    let Some(head) = head else {
        return Ok(ContextPlanSelection {
            expected_head: None,
            source_control,
            lineage: Some(ContextPlanLineage {
                generation: CONTEXT_PLAN_GENERATION,
                predecessor: None,
                control: source_control,
            }),
        });
    };
    head.validate()?;
    if head.committed_sequence > source_cutoff {
        return Err(ContextPlanError::unavailable());
    }
    let same_source = match head.lineage {
        Some(lineage) => {
            if let (Some(current), Some(previous)) = (source_control, lineage.control)
                && current.sequence <= previous.sequence
                && current != previous
            {
                return Err(ContextPlanError::unavailable());
            }
            if source_control.is_none() && lineage.control.is_some() {
                return Err(ContextPlanError::unavailable());
            }
            source_control == lineage.control
        }
        // An old snapshot cannot retroactively gain metadata. A certified
        // consumption after its immutable commit is the only unambiguous
        // forward handoff; older inputs must pass exact legacy checksum replay.
        None => source_control.is_none_or(|c| c.sequence <= head.committed_sequence),
    };
    let lineage = if same_source {
        head.lineage
    } else {
        if head.model_started
            || source_control.is_none_or(|c| c.sequence <= head.committed_sequence)
        {
            return Err(ContextPlanError::unavailable());
        }
        let generation = head
            .identity
            .generation
            .get()
            .checked_add(1)
            .filter(|g| *g <= i32::MAX as u32)
            .and_then(NonZeroU32::new)
            .ok_or_else(ContextPlanError::quota)?;
        Some(ContextPlanLineage {
            generation,
            predecessor: Some(head.reference()),
            control: source_control,
        })
    };
    let selection = ContextPlanSelection {
        expected_head: Some(head.reference()),
        source_control,
        lineage,
    };
    selection.validate()?;
    Ok(selection)
}

#[cfg(test)]
mod tests;
