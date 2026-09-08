//! A validation boundary scoped to one ordered recovery pass, never a live proof.
use super::*;
use crate::local_journal_acceptance::{validate_linear_from, LinearBaseline};
use fluree_db_core::local_journal::{AcceptanceView, Result as JResult};
use std::cell::RefCell;

pub(super) fn validate(
    adapter: &AdapterValidator<'_>,
    records: &[Record],
    checkpoint: Option<&Checkpoint>,
) -> JResult<()> {
    if adapter.prefix.is_some() {
        return Err(JournalError::Invalid(
            "live prefix cannot authorize recovery",
        ));
    }
    if adapter.proof.is_some() != checkpoint.is_some() {
        return Err(JournalError::Invalid("recovery baseline/proof mismatch"));
    }
    let validator = RecoveryValidator {
        adapter,
        state: RefCell::new(State {
            head: checkpoint.map(|c| c.head().bytes.clone()),
            linear: adapter.proof.map(|p| LinearBaseline {
                id: p.linear.id.clone(),
                t: p.linear.t,
            }),
        }),
    };
    // The default walker exposes only this record and previously checked bytes.
    // It validates the bound checkpoint before invoking any record callback.
    // Cached live prefixes are refused; every record is checked anew.
    validator.validate_recovered_from(records, checkpoint)
}

struct State {
    head: Option<Vec<u8>>,
    linear: Option<LinearBaseline>,
}
struct RecoveryValidator<'a, 'b> {
    adapter: &'a AdapterValidator<'b>,
    state: RefCell<State>,
}
impl AcceptanceValidator for RecoveryValidator<'_, '_> {
    fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> JResult<()> {
        self.adapter.validate_checkpoint(checkpoint)
    }
    fn validate(&self, view: &AcceptanceView<'_>) -> JResult<()> {
        let mut state = self.state.borrow_mut();
        if view.transition.expected_head != state.head {
            return Err(JournalError::Invalid(
                "recovery transition skips verified head",
            ));
        }
        validate_linear_from(view, self.adapter.proof.is_some(), state.linear.as_ref())?;
        let linear = AdapterValidator::validate_body(view)?;
        // Advance only after all CID/raw/parent/head/config/default-graph checks.
        // Failure discards this whole private pass; no state survives for a retry.
        state.linear = Some(linear);
        state.head = Some(view.transition.resulting_head.clone());
        Ok(())
    }
}
