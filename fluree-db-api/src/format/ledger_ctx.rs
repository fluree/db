//! Per-ledger view + compactor bundle for the hydration formatter.
//!
//! Single-ledger callers construct [`LedgerFormatContext::Single`]; multi-ledger
//! dataset queries construct [`LedgerFormatContext::Multi`]. The hydration
//! formatter consults this enum for both:
//!
//! - `db.range(...)` reads — to scan a subject's properties from the snapshot
//!   the subject came from, not always the primary's.
//! - `compactor.decode_sid(...)` / `compact_sid(...)` lookups — to decode SIDs
//!   against the namespace dict of the ledger that encoded them.
//!
//! Each ledger has its own `namespace_code → IRI-prefix` map, assigned in
//! insertion order. Decoding a SID against the wrong ledger's dict produces
//! either an [`UnknownNamespace`](super::FormatError::UnknownNamespace) error
//! or — worse — a silently mis-decoded IRI. This indirection is what
//! [`Binding::IriMatch.ledger_alias`](fluree_db_query::binding::Binding)
//! provenance feeds.
//!
//! The single-ledger case (and the multi-ledger-but-all-IriMatch-routes-to-
//! primary case) routes every lookup to the primary entry. Per-ledger
//! routing arrives in the hydration callsites in a follow-up commit
//! (see fluree/db#1259 Issue 2).

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_core::GraphDbRef;

use super::iri::IriCompactor;

/// Per-ledger view + compactor needed by the hydration formatter.
///
/// The compactor is held in an [`Arc`] so it can be shared cheaply across the
/// formatter's recursion without cloning the underlying `HashMap<u16, String>`
/// of namespace prefixes.
pub(crate) struct LedgerEntry<'a> {
    pub db: GraphDbRef<'a>,
    pub compactor: Arc<IriCompactor>,
}

/// Either a single (db, compactor) pair (the common single-ledger case) or a
/// map keyed by canonical ledger identifier matching
/// [`Binding::IriMatch`](fluree_db_query::binding::Binding)'s `ledger_alias`.
///
/// `Single` borrows its `db` and `compactor` from the caller — zero
/// allocation. `Multi` owns per-entry `Arc<IriCompactor>` clones that are
/// shared cheaply across hydration recursion.
pub(crate) enum LedgerFormatContext<'a> {
    Single {
        db: GraphDbRef<'a>,
        compactor: &'a IriCompactor,
    },
    Multi {
        /// Per-ledger entries keyed by canonical ledger id (matches
        /// `Binding::IriMatch.ledger_alias`).
        ledgers: HashMap<Arc<str>, LedgerEntry<'a>>,
        /// Fallback entry used when no per-binding provenance is available
        /// (e.g. a plain `Binding::Sid` or a non-IriMatch root). Must always
        /// be present in `ledgers`.
        primary: Arc<str>,
    },
}

impl<'a> LedgerFormatContext<'a> {
    /// Resolve a `GraphDbRef` for a given ledger.
    ///
    /// - `Single`: returns the lone view regardless of `ledger`.
    /// - `Multi`: returns the entry matching `ledger`, falling back to the
    ///   primary entry when `ledger` is `None` or unknown.
    pub fn db_for(&self, ledger: Option<&Arc<str>>) -> GraphDbRef<'a> {
        match self {
            Self::Single { db, .. } => *db,
            Self::Multi { ledgers, primary } => {
                let key = ledger.unwrap_or(primary);
                ledgers
                    .get(key.as_ref())
                    .or_else(|| ledgers.get(primary.as_ref()))
                    .map(|e| e.db)
                    .expect("Multi context must contain primary entry")
            }
        }
    }

    /// Resolve an `IriCompactor` for a given ledger.
    ///
    /// Same fallback semantics as [`db_for`](Self::db_for).
    pub fn compactor_for(&self, ledger: Option<&Arc<str>>) -> &IriCompactor {
        match self {
            Self::Single { compactor, .. } => compactor,
            Self::Multi { ledgers, primary } => {
                let key = ledger.unwrap_or(primary);
                ledgers
                    .get(key.as_ref())
                    .or_else(|| ledgers.get(primary.as_ref()))
                    .map(|e| e.compactor.as_ref())
                    .expect("Multi context must contain primary entry")
            }
        }
    }

    /// Convenience: primary view's `GraphDbRef`. Used by sites that don't
    /// yet route per-binding provenance.
    pub fn primary_db(&self) -> GraphDbRef<'a> {
        self.db_for(None)
    }

    /// Convenience: primary view's `IriCompactor`.
    pub fn primary_compactor(&self) -> &IriCompactor {
        self.compactor_for(None)
    }
}
