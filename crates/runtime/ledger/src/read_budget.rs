//! Serialized-payload read admission, before persistence materializes blobs.
//!
//! The envelope allowance covers bounded canonical metadata and framing. This
//! is a byte-accounting policy, not a claim about allocator or decoded AST RSS.

use crate::LedgerError;

pub const LEDGER_RECORD_ENVELOPE_BUDGET_BYTES: usize = 1_024;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct LedgerReadBudget {
    bytes: usize,
}

impl LedgerReadBudget {
    pub fn new(bytes: usize) -> Result<Self, LedgerError> {
        if bytes < LEDGER_RECORD_ENVELOPE_BUDGET_BYTES {
            return Err(LedgerError::InvalidReadBudget);
        }
        Ok(Self { bytes })
    }

    /// Sizes must be in canonical sequence order. Never skip an oversized
    /// record to admit later ones; a nonempty prefix can be a short page.
    pub fn admitted_prefix(
        self,
        payload_lengths: impl IntoIterator<Item = usize>,
        max_records: usize,
    ) -> Result<usize, LedgerError> {
        let mut remaining = self.bytes;
        let mut admitted = 0;
        for length in payload_lengths.into_iter().take(max_records) {
            let cost = length.checked_add(LEDGER_RECORD_ENVELOPE_BUDGET_BYTES);
            let Some(next) = cost.and_then(|cost| remaining.checked_sub(cost)) else {
                if admitted == 0 {
                    return Err(LedgerError::ReadBudgetExceeded);
                }
                break;
            };
            remaining = next;
            admitted += 1;
        }
        Ok(admitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_stops_at_first_non_fitting_record_and_never_skips_it() {
        let budget = LedgerReadBudget::new(4_096).expect("budget");
        assert_eq!(budget.admitted_prefix([1_024, 1_024, 0], 3), Ok(2));
        assert_eq!(budget.admitted_prefix([0, 4_096, 0], 3), Ok(1));
        assert_eq!(budget.admitted_prefix([0, 0], 1), Ok(1));
        assert_eq!(budget.admitted_prefix([], 3), Ok(0));
    }

    #[test]
    fn oversized_first_record_and_arithmetic_overflow_are_explicit() {
        let budget = LedgerReadBudget::new(4_096).expect("budget");
        for size in [3_073, usize::MAX] {
            assert_eq!(
                budget.admitted_prefix([size, 0], 2),
                Err(LedgerError::ReadBudgetExceeded)
            );
        }
        assert_eq!(
            LedgerReadBudget::new(1_023),
            Err(LedgerError::InvalidReadBudget)
        );
    }
}
