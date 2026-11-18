//! Deterministic, bounded, attempt-local loop detection read from original kernel tool records.
use crate::{ExtensionError, ToolHistoryEntry};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Pattern recognized by the loop policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum LoopPattern {
    /// Same tool and canonical arguments in the history window.
    GenericRepeat,
    /// Explicitly configured poll tool, same call identity and no changed result.
    KnownPollNoProgress,
    /// Two distinct call identities alternating without a third call.
    PingPong,
}
/// Warning is model context; both terminal levels stop the attempt.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum LoopLevel {
    /// Inform the model.
    Warning,
    /// Stop with a recoverable-by-host policy change, not an automatic retry.
    Critical,
    /// Hard attempt circuit breaker.
    CircuitBreaker,
}
/// One admitted call. Arguments are structural JSON, not raw provider JSON text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSample {
    /// Stable call ID; repeated recovery of this ID does not increment history.
    pub call_id: String,
    /// Canonical wire name.
    pub tool: String,
    /// Canonical argument structure.
    pub arguments: serde_json::Value,
}
impl ToolSample {
    fn same_call(&self, other: &Self) -> bool {
        self.tool == other.tool && self.arguments == other.arguments
    }
}
/// Typed reason recorded before stopping or injecting a warning.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LoopDetection {
    /// Pattern.
    pub pattern: LoopPattern,
    /// Level reached.
    pub level: LoopLevel,
    /// Count within the bounded window.
    pub count: usize,
    /// Call that crossed the boundary.
    pub call_id: String,
}
/// Per-pattern thresholds. `None` explicitly disables a level. A disabled
/// critical threshold permits hosts to use warning + circuit breaker only.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LoopThresholds {
    /// Warning count; default 10.
    pub warning: Option<usize>,
    /// Terminal critical count; default 20.
    pub critical: Option<usize>,
    /// Terminal breaker count; default 30.
    pub circuit_breaker: Option<usize>,
}
impl Default for LoopThresholds {
    fn default() -> Self {
        Self {
            warning: Some(10),
            critical: Some(20),
            circuit_breaker: Some(30),
        }
    }
}
impl LoopThresholds {
    fn level(&self, count: usize) -> Option<LoopLevel> {
        if self.circuit_breaker.is_some_and(|n| count >= n) {
            Some(LoopLevel::CircuitBreaker)
        } else if self.critical.is_some_and(|n| count >= n) {
            Some(LoopLevel::Critical)
        } else if self.warning.is_some_and(|n| count >= n) {
            Some(LoopLevel::Warning)
        } else {
            None
        }
    }
    fn validate(&self, history: usize) -> Result<(), ExtensionError> {
        let active: Vec<_> = [self.warning, self.critical, self.circuit_breaker]
            .into_iter()
            .flatten()
            .collect();
        if active.iter().any(|n| *n < 2 || *n > history) || active.windows(2).any(|p| p[0] > p[1]) {
            return Err(ExtensionError::LoopConfig(
                "thresholds must be ordered and within 2..=history".into(),
            ));
        }
        Ok(())
    }
}
/// Configured at composition time, never inferred from a tool's name.
#[derive(Debug, Clone)]
pub struct LoopDetectionConfig {
    /// Maximum recent call count; default 30, capped at 10,000.
    pub history: usize,
    /// Generic repeat levels.
    pub generic_repeat: LoopThresholds,
    /// No-progress poll levels.
    pub known_poll_no_progress: LoopThresholds,
    /// Alternation levels.
    pub ping_pong: LoopThresholds,
    /// Exact wire names classified by the host as polling tools.
    pub poll_tools: BTreeSet<String>,
}
impl Default for LoopDetectionConfig {
    fn default() -> Self {
        Self {
            history: 30,
            generic_repeat: Default::default(),
            known_poll_no_progress: Default::default(),
            ping_pong: Default::default(),
            poll_tools: BTreeSet::new(),
        }
    }
}
impl LoopDetectionConfig {
    /// Reject impossible or unbounded history/threshold combinations.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        if !(2..=10_000).contains(&self.history) {
            return Err(ExtensionError::LoopConfig(
                "history must be within 2..=10000".into(),
            ));
        }
        for thresholds in [
            &self.generic_repeat,
            &self.known_poll_no_progress,
            &self.ping_pong,
        ] {
            thresholds.validate(self.history)?;
        }
        Ok(())
    }
    /// Reconstruct history and evaluate the latest admitted call. Poll detection
    /// waits for its original result. Result changes reset its no-progress streak.
    pub fn detect(&self, history: &[ToolHistoryEntry]) -> Vec<LoopDetection> {
        let samples: Vec<_> = history.iter().map(|entry| &entry.call).collect();
        let results: BTreeMap<_, _> = history
            .iter()
            .filter_map(|entry| {
                entry
                    .result
                    .as_ref()
                    .map(|result| (&entry.call.call_id, result))
            })
            .collect();
        let samples = &samples[samples.len().saturating_sub(self.history)..];
        let Some(latest) = samples.last() else {
            return Vec::new();
        };
        let mut found = Vec::new();
        let mut add = |pattern, count, thresholds: &LoopThresholds| {
            if let Some(level) = thresholds.level(count) {
                found.push(LoopDetection {
                    pattern,
                    level,
                    count,
                    call_id: latest.call_id.clone(),
                });
            }
        };
        add(
            LoopPattern::GenericRepeat,
            samples.iter().filter(|s| s.same_call(latest)).count(),
            &self.generic_repeat,
        );
        if samples.len() >= 4 && !latest.same_call(samples[samples.len() - 2]) {
            let other = samples[samples.len() - 2];
            let count = samples
                .iter()
                .rev()
                .enumerate()
                .take_while(|(i, s)| s.same_call(if i % 2 == 0 { latest } else { other }))
                .count();
            if count >= 4 {
                add(LoopPattern::PingPong, count, &self.ping_pong);
            }
        }
        // Completions can arrive out of admission order. Check the newest
        // completed member of each polling identity, bounded by the same window.
        // A missing earlier result breaks the streak until its fact arrives.
        let mut checked: Vec<&ToolSample> = Vec::new();
        for latest in samples
            .iter()
            .rev()
            .copied()
            .filter(|s| self.poll_tools.contains(&s.tool))
        {
            let Some(result) = results.get(&latest.call_id) else {
                continue;
            };
            if checked.iter().any(|previous| previous.same_call(latest)) {
                continue;
            }
            checked.push(latest);
            let count = samples
                .iter()
                .rev()
                .filter(|s| s.same_call(latest))
                .skip_while(|s| s.call_id != latest.call_id)
                .take_while(|s| results.get(&s.call_id) == Some(result))
                .count();
            if let Some(level) = self.known_poll_no_progress.level(count) {
                found.push(LoopDetection {
                    pattern: LoopPattern::KnownPollNoProgress,
                    level,
                    count,
                    call_id: latest.call_id.clone(),
                });
            }
        }
        found
    }
}
