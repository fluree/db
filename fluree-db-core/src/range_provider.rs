//! Abstract range query provider.
//!
//! `RangeProvider` decouples callers of `range_with_overlay()` from the
//! underlying index implementation. When a `RangeProvider` is present on a `Db`,
//! `range_with_overlay()` delegates to it. This allows the binary columnar index
//! to serve all range queries without modifying the 25+ callers across the
//! reasoner, API, policy, and SHACL crates.
//!
//! The trait is defined in `fluree-db-core` (where the callers live) and
//! implemented by the binary index shim in `fluree-db-query`.
//!
//! Note: When no provider is attached, `range_with_overlay()` only supports an
//! overlay-only genesis path (`db.t == 0`). There is no legacy index fallback.

use crate::comparator::IndexType;
use crate::flake::Flake;
use crate::ids::GraphId;
use crate::overlay::OverlayProvider;
use crate::query_bounds::{RangeMatch, RangeOptions, RangeTest};
use crate::sid::Sid;
use crate::tracking::Tracker;
use std::collections::HashMap;

/// Parameters for a range query against a [`RangeProvider`].
///
/// Bundles the read-only references the provider needs to execute a single
/// range scan. All fields are borrows so the caller retains ownership.
pub struct RangeQuery<'a> {
    /// Graph to query (0 = default graph).
    pub g_id: GraphId,
    /// Index order to scan (SPOT, PSOT, POST, OPST).
    pub index: IndexType,
    /// Comparison operator (Eq, Lt, Le, Gt, Ge).
    pub test: RangeTest,
    /// Components to match (subject, predicate, object).
    pub match_val: &'a RangeMatch,
    /// Query options (limit, time bounds, object bounds).
    pub opts: &'a RangeOptions,
    /// Overlay provider for uncommitted novelty flakes.
    pub overlay: &'a dyn OverlayProvider,
    /// Optional fuel tracker. When `Some`, implementations should charge
    /// fuel for leaflet/dict touches during scan and decode.
    pub tracker: Option<&'a Tracker>,
}

/// A range query backend that can execute range queries against an index.
///
/// This trait abstracts the index implementation so callers can use the same
/// `range_with_overlay()` API regardless of which index is active.
///
/// Implementations must return results in the correct index order (SPOT, PSOT,
/// POST, or OPST) matching the requested `IndexType`.
pub trait RangeProvider: Send + Sync {
    /// Downcast hook so downstream crates can recover the concrete type
    /// (e.g., `BinaryRangeProvider`) from `Arc<dyn RangeProvider>`.
    ///
    /// Cost: one `TypeId` comparison at the call site. Called once per
    /// `BinaryScanOperator::open()`, not per row.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Execute a range query, returning matching flakes in index order.
    ///
    /// See [`RangeQuery`] for parameter meanings.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` on I/O failures or if match components cannot be
    /// translated to the index's internal representation.
    fn range(&self, query: &RangeQuery<'_>) -> std::io::Result<Vec<Flake>>;

    /// Execute a bounded range query with explicit start/end flakes.
    ///
    /// This is the bounded-range equivalent of [`range()`](Self::range).
    /// Used for subject-range queries (e.g., SHA prefix scans) that need
    /// to scan between two different subjects.
    ///
    /// The default implementation returns `Unsupported`.  Implementors that
    /// support arbitrary interval scans should override this method.
    fn range_bounded(
        &self,
        _g_id: GraphId,
        _index: IndexType,
        _start_bound: &Flake,
        _end_bound: &Flake,
        _opts: &RangeOptions,
        _overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<Flake>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "range_bounded not supported by this provider",
        ))
    }

    /// Batched lookup: for a fixed predicate, retrieve ref-valued objects for many subjects.
    ///
    /// This supports latency-sensitive callers like policy enforcement (`rdf:type` lookups)
    /// without issuing one range query per subject and without scanning the full predicate
    /// partition.
    ///
    /// Implementations should respect `opts.to_t` and must merge overlay ops.
    ///
    /// Default implementation returns `Unsupported`.
    fn lookup_subject_predicate_refs_batched(
        &self,
        _g_id: GraphId,
        _index: IndexType,
        _predicate: &Sid,
        _subjects: &[Sid],
        _opts: &RangeOptions,
        _overlay: &dyn OverlayProvider,
    ) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "lookup_subject_predicate_refs_batched not supported by this provider",
        ))
    }
}
