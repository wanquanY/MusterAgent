use musteragent_activity::ActivityKind;
use musteragent_identity::{ActivityId, ConversationSeq, DelegationId, RunId};
use musteragent_runtime_snapshot::ContextSourceSelectionPolicy;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextSourceOwner {
    pub run_id: RunId,
    pub activity_id: ActivityId,
    pub activity_kind: ActivityKind,
    pub delegation_id: Option<DelegationId>,
    pub admitted_sequence: ConversationSeq,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ContextSourceSelector {
    pub policy: ContextSourceSelectionPolicy,
    pub boundary: ConversationVisibilityBoundary,
    pub target: ContextSourceOwner,
}

impl ContextSourceSelector {
    #[must_use]
    pub fn includes(
        self,
        record_sequence: ConversationSeq,
        source_run_id: Option<RunId>,
        source_admitted_sequence: Option<ConversationSeq>,
        owner: Option<ContextSourceOwner>,
    ) -> bool {
        if self.policy == ContextSourceSelectionPolicy::LegacyAdmissionV1 {
            return self.boundary.includes(
                record_sequence,
                source_run_id,
                source_admitted_sequence,
            );
        }
        let Some(owner) = owner else {
            return false;
        };
        if source_run_id != Some(owner.run_id)
            || source_admitted_sequence != Some(owner.admitted_sequence)
            || self.boundary.target_run_id != self.target.run_id
            || self.boundary.admitted_sequence != self.target.admitted_sequence
        {
            return false;
        }
        if owner.run_id == self.target.run_id {
            return owner == self.target;
        }
        self.target.activity_kind == ActivityKind::Chat
            && self.target.delegation_id.is_none()
            && owner.activity_kind == ActivityKind::Chat
            && owner.delegation_id.is_none()
            && owner.admitted_sequence < self.target.admitted_sequence
    }
}

/// Legacy admission transcript boundary for one Run's model context.
///
/// Under the legacy policy, every Run sees the conversation prefix that existed when it was admitted
/// and its own later records. A serial, root Chat Run additionally sees later
/// records committed by predecessor Runs, so it can consume their completed
/// answers without allowing inputs from subsequently admitted Runs to leak
/// backwards into its request.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ConversationVisibilityBoundary {
    pub target_run_id: RunId,
    pub admitted_sequence: ConversationSeq,
    pub include_predecessor_run_suffixes: bool,
}

impl ConversationVisibilityBoundary {
    #[must_use]
    pub fn includes(
        self,
        record_sequence: ConversationSeq,
        source_run_id: Option<RunId>,
        source_run_admitted_sequence: Option<ConversationSeq>,
    ) -> bool {
        if record_sequence.get() <= self.admitted_sequence.get()
            || matches!(source_run_id, Some(run_id) if run_id == self.target_run_id)
        {
            return true;
        }
        self.include_predecessor_run_suffixes
            && matches!(
                source_run_admitted_sequence,
                Some(sequence) if sequence.get() < self.admitted_sequence.get()
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(kind: ActivityKind, admission: u64) -> ContextSourceOwner {
        ContextSourceOwner {
            run_id: RunId::generate(),
            activity_id: ActivityId::generate(),
            activity_kind: kind,
            delegation_id: None,
            admitted_sequence: ConversationSeq::new(admission).expect("admission"),
        }
    }

    fn selector(target: ContextSourceOwner) -> ContextSourceSelector {
        ContextSourceSelector {
            policy: ContextSourceSelectionPolicy::ExecutionIsolatedV1,
            boundary: ConversationVisibilityBoundary {
                target_run_id: target.run_id,
                admitted_sequence: target.admitted_sequence,
                include_predecessor_run_suffixes: false,
            },
            target,
        }
    }

    fn selected(selector: ContextSourceSelector, owner: ContextSourceOwner) -> bool {
        selector.includes(
            ConversationSeq::new(100).expect("record"),
            Some(owner.run_id),
            Some(owner.admitted_sequence),
            Some(owner),
        )
    }

    #[test]
    fn isolated_root_retains_own_and_predecessor_root_even_without_legacy_suffixes() {
        let target = owner(ActivityKind::Chat, 20);
        let selector = selector(target);
        assert!(selected(selector, target));
        assert!(selected(selector, owner(ActivityKind::Chat, 10)));
        assert!(!selected(selector, owner(ActivityKind::Chat, 20)));
        assert!(!selected(selector, owner(ActivityKind::Chat, 21)));
    }

    #[test]
    fn isolated_root_excludes_all_private_kinds_even_in_admission_prefix() {
        let target = owner(ActivityKind::Chat, 20);
        for kind in [
            ActivityKind::Coordination,
            ActivityKind::Automation,
            ActivityKind::Delegation,
            ActivityKind::Maintenance,
            ActivityKind::Evaluation,
        ] {
            let source = owner(kind, 10);
            assert!(!selector(target).includes(
                ConversationSeq::new(11).expect("prefix"),
                Some(source.run_id),
                Some(source.admitted_sequence),
                Some(source),
            ));
            assert!(!selected(selector(source), target));
            assert!(selected(selector(source), source));
        }
        let child = ContextSourceOwner {
            delegation_id: Some(DelegationId::generate()),
            ..owner(ActivityKind::Chat, 10)
        };
        assert!(!selected(selector(target), child));
        assert!(!selected(selector(child), owner(ActivityKind::Chat, 5)));
        assert!(selected(selector(child), child));
    }

    #[test]
    fn isolated_selection_requires_consistent_owner_and_target() {
        let target = owner(ActivityKind::Chat, 20);
        let selector = selector(target);
        assert!(!selector.includes(
            target.admitted_sequence,
            Some(target.run_id),
            Some(target.admitted_sequence),
            None
        ));
        assert!(!selector.includes(
            target.admitted_sequence,
            Some(RunId::generate()),
            Some(target.admitted_sequence),
            Some(target)
        ));
        assert!(!selector.includes(
            target.admitted_sequence,
            Some(target.run_id),
            Some(ConversationSeq::new(1).expect("admission")),
            Some(target)
        ));
        assert!(!selected(
            selector,
            ContextSourceOwner {
                activity_id: ActivityId::generate(),
                ..target
            }
        ));
        assert!(!selected(
            ContextSourceSelector {
                target: owner(ActivityKind::Chat, 20),
                ..selector
            },
            target
        ));
    }

    #[test]
    fn legacy_selection_is_exact_boundary_parity_without_owner_metadata() {
        let target = owner(ActivityKind::Chat, 20);
        for suffixes in [false, true] {
            let mut selector = selector(target);
            selector.policy = ContextSourceSelectionPolicy::LegacyAdmissionV1;
            selector.boundary.include_predecessor_run_suffixes = suffixes;
            for sequence in [1, 20, 21, 100] {
                for source in [None, Some(target.run_id), Some(RunId::generate())] {
                    for admission in [
                        None,
                        Some(ConversationSeq::new(10).expect("admission")),
                        Some(target.admitted_sequence),
                    ] {
                        let sequence = ConversationSeq::new(sequence).expect("record");
                        assert_eq!(
                            selector.includes(sequence, source, admission, None),
                            selector.boundary.includes(sequence, source, admission)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn serial_chat_includes_predecessor_completion_but_not_future_run_input() {
        let predecessor = RunId::generate();
        let target = RunId::generate();
        let future = RunId::generate();
        let boundary = ConversationVisibilityBoundary {
            target_run_id: target,
            admitted_sequence: ConversationSeq::new(20).expect("target admission"),
            include_predecessor_run_suffixes: true,
        };

        assert!(boundary.includes(
            ConversationSeq::new(25).expect("predecessor completion"),
            Some(predecessor),
            Some(ConversationSeq::new(10).expect("predecessor admission")),
        ));
        assert!(boundary.includes(
            ConversationSeq::new(30).expect("own continuation"),
            Some(target),
            Some(ConversationSeq::new(20).expect("target admission")),
        ));
        assert!(!boundary.includes(
            ConversationSeq::new(21).expect("future input"),
            Some(future),
            Some(ConversationSeq::new(21).expect("future admission")),
        ));
    }

    #[test]
    fn parallel_run_is_strictly_admission_bounded_except_for_its_own_suffix() {
        let target = RunId::generate();
        let sibling = RunId::generate();
        let boundary = ConversationVisibilityBoundary {
            target_run_id: target,
            admitted_sequence: ConversationSeq::new(5).expect("target admission"),
            include_predecessor_run_suffixes: false,
        };

        assert!(boundary.includes(
            ConversationSeq::new(4).expect("shared prefix"),
            Some(sibling),
            Some(ConversationSeq::new(4).expect("sibling admission")),
        ));
        assert!(boundary.includes(
            ConversationSeq::new(7).expect("own suffix"),
            Some(target),
            Some(ConversationSeq::new(5).expect("target admission")),
        ));
        assert!(!boundary.includes(
            ConversationSeq::new(6).expect("sibling suffix"),
            Some(sibling),
            Some(ConversationSeq::new(4).expect("sibling admission")),
        ));
    }
}
