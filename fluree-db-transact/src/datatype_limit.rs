//! The limit on how many datatypes a ledger can hold.
//!
//! Every datatype outside [`DatatypeDictId::RESERVED_IRIS`] takes a
//! dictionary ID the first time a ledger uses it, and IDs are never
//! released. A ledger with more than the index can store can never be
//! indexed again, so every path that writes commits checks the limit before
//! the write, where a refusal is still recoverable.

use crate::commit::binary_store;
use crate::error::{Result, TransactError};
use fluree_db_core::datatypes::is_reserved_datatype;
use fluree_db_core::ids::RuntimeDatatypeId;
use fluree_db_core::{DatatypeDictId, Flake, RuntimeSmallDicts, Sid};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue};
use rustc_hash::FxHashSet;
use std::borrow::Cow;

/// Non-reserved datatypes a ledger can hold: every datatype dictionary ID
/// from `RESERVED_COUNT` through `MAX`.
pub const MAX_NON_RESERVED_DATATYPES: usize =
    (DatatypeDictId::MAX - DatatypeDictId::RESERVED_COUNT) as usize + 1;

/// Refuse a commit on `base` whose new datatypes would not fit.
///
/// Runs under the ledger's write lock against the authoritative base, so
/// writes re-based over one another cannot jointly cross the limit.
pub fn check_commit(base: &LedgerState, flakes: &[Flake], txn_meta: &[TxnMetaEntry]) -> Result<()> {
    let known = known_datatypes(base);
    let meta_datatypes: Vec<Sid> = txn_meta
        .iter()
        .filter_map(|entry| match &entry.value {
            TxnMetaValue::TypedLiteral { dt_ns, dt_name, .. } => Some(Sid::new(*dt_ns, dt_name)),
            _ => None,
        })
        .collect();
    let adding = new_datatypes(&known, flakes.iter().map(|f| &f.dt).chain(&meta_datatypes));
    check_datatype_capacity(&known, adding.len())
}

/// Every datatype `state` holds, in its index and in its novelty.
///
/// `state.runtime_small_dicts` is normally seeded from the attached index
/// store and extended by every commit since. When it was not seeded from
/// that store, a copy is seeded here so datatypes that exist only in the
/// index still count.
pub fn known_datatypes(state: &LedgerState) -> Cow<'_, RuntimeSmallDicts> {
    let dicts = &*state.runtime_small_dicts;
    match binary_store(state) {
        Some(store) if usize::from(dicts.persisted_datatype_count()) != store.dt_sids().len() => {
            let mut seeded =
                RuntimeSmallDicts::from_seeded_sids([], store.dt_sids().iter().cloned());
            for id in 0..dicts.datatype_count() {
                if let Some(sid) = dicts.datatype_sid(RuntimeDatatypeId::from_u16(id)) {
                    seeded.assign_or_lookup_datatype(sid);
                }
            }
            Cow::Owned(seeded)
        }
        _ => Cow::Borrowed(dicts),
    }
}

/// The distinct datatypes in `datatypes` that are neither reserved nor
/// already in `known`.
pub fn new_datatypes<'a>(
    known: &RuntimeSmallDicts,
    datatypes: impl IntoIterator<Item = &'a Sid>,
) -> FxHashSet<&'a Sid> {
    let mut adding = FxHashSet::default();
    let mut last = None;
    for dt in datatypes {
        // Runs of one datatype are common; skip them before any lookup.
        if last == Some(dt) {
            continue;
        }
        last = Some(dt);
        if !is_reserved_datatype(dt) && known.datatype_id(dt).is_none() {
            adding.insert(dt);
        }
    }
    adding
}

/// Refuse `adding` new datatypes if they would not fit beside the ones
/// `known` already holds.
pub fn check_datatype_capacity(known: &RuntimeSmallDicts, adding: usize) -> Result<()> {
    let used = known.non_reserved_datatype_count();
    if used + adding > MAX_NON_RESERVED_DATATYPES {
        return Err(TransactError::DatatypeLimitExceeded {
            used,
            adding,
            max: MAX_NON_RESERVED_DATATYPES,
        });
    }
    Ok(())
}
