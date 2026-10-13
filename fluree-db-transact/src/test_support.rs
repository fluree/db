//! Helpers for this crate's tests.

use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::memory::MemoryNameService;

/// `ledger` carrying the record `nameservice` created for it, rooted at its
/// name, so its commits present the record's fence.
pub(crate) async fn created(
    nameservice: &MemoryNameService,
    mut ledger: LedgerState,
) -> LedgerState {
    let record = fluree_db_nameservice::testing::create_at_name_root(
        nameservice,
        ledger.ledger_id().as_ref(),
    )
    .await
    .expect("create the ledger");
    ledger.ns_record = Some(record);
    ledger
}
