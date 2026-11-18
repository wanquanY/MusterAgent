//! Provenance of the exact current file version, qualified by its original
//! commit owner. A node's first creator is not the author of later contents.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FileExecutionHome {
    Cloud,
    Native,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AgentFileProduction {
    pub execution_home: FileExecutionHome,
    pub agent_id: String,
    pub dialogue_id: String,
    pub intent_id: String,
    pub task_id: Option<String>,
    pub run_id: String,
    pub invocation_id: String,
    pub committed_at_ms: i64,
}

/// A selector, never a grant to observe another Agent or execution.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileProductionFilter {
    Intent { intent_id: String },
    Task { task_id: String },
    Run { run_id: String },
}

fn canonical_id(value: &str) -> bool {
    uuid::Uuid::parse_str(value)
        .is_ok_and(|id| !id.is_nil() && id.hyphenated().to_string() == value)
}
impl AgentFileProduction {
    pub fn is_valid(&self) -> bool {
        [
            &self.agent_id,
            &self.dialogue_id,
            &self.intent_id,
            &self.run_id,
            &self.invocation_id,
        ]
        .into_iter()
        .all(|id| canonical_id(id))
            && self.task_id.as_ref().is_none_or(|id| canonical_id(id))
    }
}
impl FileProductionFilter {
    pub fn is_valid(&self) -> bool {
        canonical_id(self.selection().1)
    }
    pub fn selection(&self) -> (&'static str, &str) {
        match self {
            Self::Intent { intent_id } => ("intent_id", intent_id),
            Self::Task { task_id } => ("task_id", task_id),
            Self::Run { run_id } => ("run_id", run_id),
        }
    }
    pub fn matches(&self, production: &AgentFileProduction) -> bool {
        match self {
            Self::Intent { intent_id } => production.intent_id == *intent_id,
            Self::Task { task_id } => production.task_id.as_ref() == Some(task_id),
            Self::Run { run_id } => production.run_id == *run_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selectors_are_closed_canonical_identities_not_names_or_authority() {
        for raw in [
            r#"{"kind":"run","run_id":"report.txt"}"#,
            r#"{"kind":"run","run_id":"00000000-0000-0000-0000-000000000000"}"#,
        ] {
            assert!(
                !serde_json::from_str::<FileProductionFilter>(raw)
                    .unwrap()
                    .is_valid()
            );
        }
        assert!(serde_json::from_str::<FileProductionFilter>(
            r#"{"kind":"run","run_id":"01982f80-0000-7000-8000-000000001001","agent_id":"another"}"#
        ).is_err());
        let filter = FileProductionFilter::Run {
            run_id: uuid::Uuid::now_v7().to_string(),
        };
        assert!(filter.is_valid());
        assert_eq!(filter.selection().0, "run_id");
    }
}
