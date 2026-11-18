//! Format-owned extraction evidence. A completed task is not a fidelity claim.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtractionQuality {
    pub pages_total: Option<u32>,
    pub pages_with_text: Vec<u32>,
    pub issues: Vec<ExtractionIssue>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtractionIssue {
    pub code: String,
    pub page: Option<u32>,
    pub occurrences: u64,
}

impl ExtractionQuality {
    pub(crate) fn issue(&mut self, code: &str, page: Option<u32>) {
        if let Some(issue) = self
            .issues
            .iter_mut()
            .find(|i| i.code == code && i.page == page)
        {
            issue.occurrences = issue.occurrences.saturating_add(1);
        } else {
            self.issues.push(ExtractionIssue {
                code: code.into(),
                page,
                occurrences: 1,
            });
        }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.pages_with_text.capacity() * std::mem::size_of::<u32>()
            + self.issues.capacity() * std::mem::size_of::<ExtractionIssue>()
            + self.issues.iter().map(|i| i.code.capacity()).sum::<usize>()
    }
}
