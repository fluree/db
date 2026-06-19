//! Inline metadata for the edge-annotation arenas.
//!
//! Lives in `fluree-db-core` (rather than `fluree-db-binary-index`) so
//! [`crate::db::LedgerSnapshot`] can hold an
//! `Option<AnnotationIndexRoot>` directly. The heavy on-disk arena
//! formats (forward/reverse branch + leaf blobs) stay in
//! `fluree-db-binary-index::annotation_arena::format`.
//!
//! ## Empty-vs-absent semantics
//!
//! The indexed-arena guarantee depends on **both** signals on
//! [`crate::db::LedgerSnapshot`]:
//!
//! | `has_annotations` | `annotation_index` | Meaning                                |
//! |-------------------|--------------------|----------------------------------------|
//! | `false`           | `None`             | Hard guarantee: zero attachments. Cascade and reads short-circuit. |
//! | `true`            | `Some(_)`          | Builder ran. Forward/reverse arenas are authoritative for `t ≤ max_t`; novelty supplies the tail. |
//! | `true`            | `None`             | Pre-builder transitional state: snapshot may carry `f:reifies*` flakes but no arena yet — readers fall back to scan, cascade still runs. |
//! | `false`           | `Some(_)`          | Invariant violation. The FIR6 encoder coerces `FLAG_HAS_ANNOTATIONS` whenever an arena is present, so this state never reaches the wire. |
//!
//! Builders that produce arenas must set both signals; the encoder
//! defends against forgetting the sticky bit but cannot fix the
//! inverse (arena present, bool false in memory).
//!
//! See `docs/design/edge-annotations.md` for the design contract.

use crate::ContentId;
use serde::{Deserialize, Serialize};

/// Aggregate counters populated at arena build time. Surfaced for
/// cost-based planning (M3) and storage inspection.
///
/// **Every field is `#[serde(default)]`** so older arena roots —
/// written before any given field landed — deserialize cleanly with
/// zeros. This applies to the *original* four counters as well as
/// the per-slot NDV counters added in the M3.1 follow-up: a missing
/// field on the wire is treated as "no information" by the planner,
/// which falls back to the regular `IndexStats.properties` HLL.
/// Tagging all fields keeps the struct safely reorderable — if a
/// future contributor inserts a new field anywhere in source order,
/// old arenas still load.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationStats {
    /// Total forward-arena rows (one per asserted/retracted attachment event).
    #[serde(default)]
    pub forward_rows: u64,
    /// Total reverse-arena rows (mirror of `forward_rows` after compaction).
    #[serde(default)]
    pub reverse_rows: u64,
    /// Distinct edges with at least one current (live) attachment.
    #[serde(default)]
    pub distinct_edges: u64,
    /// Distinct annotation subjects.
    #[serde(default)]
    pub distinct_annotations: u64,
    /// Number of live `(edge, ann)` attachment pairs.
    ///
    /// Equal to `distinct_annotations` under the v1 single-target-
    /// per-ann invariant (enforced at stage time in
    /// `fluree-db-transact::stage` — re-attaching an SID to a
    /// different edge is a transaction error). When the invariant
    /// holds, this field is redundant; we store it explicitly so the
    /// planner stays accurate when reading older / replayed-from-
    /// corrupt-history ledgers where the same ann SID may have
    /// multiple live targets. The `f:reifies*` row count for the
    /// required slots is `live_attachment_pairs`, not
    /// `distinct_annotations`.
    ///
    /// `#[serde(default)]` so older arena roots written before this
    /// field landed deserialize cleanly with `0`. The merge layer
    /// treats `0` as "use distinct_annotations" — the safe equality
    /// for healthy v1 ledgers.
    #[serde(default)]
    pub live_attachment_pairs: u64,

    // -----------------------------------------------------------------
    // Per-slot NDV counters across the live (currently-asserted) rows.
    //
    // For the three **required** slots (subject, predicate, object),
    // each live `(edge, ann)` pair contributes exactly one row, so
    // the row count equals `live_attachment_pairs` (which equals
    // `distinct_annotations` under the v1 single-target invariant).
    // We track only the NDV here.
    //
    // For the **optional** slots (graph, lang, listIndex), both the
    // row count and the NDV vary: the row count is the number of
    // live `(edge, ann)` pairs whose reified edge carries that slot,
    // the NDV is the number of distinct values observed in that
    // slot across those pairs. (`datatype` is a special case — see
    // its field comment below.)
    // -----------------------------------------------------------------
    /// Distinct subject SIDs across live reified edges.
    /// Used as `ndv_values` for `?ann f:reifiesSubject ?s` probes.
    #[serde(default)]
    pub distinct_reified_subjects: u64,
    /// Distinct predicate SIDs across live reified edges.
    #[serde(default)]
    pub distinct_reified_predicates: u64,
    /// Distinct object values across live reified edges.
    #[serde(default)]
    pub distinct_reified_objects: u64,

    /// Live `f:reifiesGraph` rows = number of live `(edge, ann)`
    /// pairs whose reified edge is in a named graph. Per pair, not
    /// per distinct edge — parallel annotations on the same named-
    /// graph edge each contribute one row. Generally `<
    /// live_attachment_pairs` (the slot is omitted for default-graph
    /// edges) but can exceed `distinct_annotations` when a single
    /// ann SID is attached to multiple named-graph edges (the
    /// multi-target anomaly the v1 stage-time invariant rejects).
    #[serde(default)]
    pub reifies_graph_rows: u64,
    /// Distinct named-graph SIDs across live reified edges.
    #[serde(default)]
    pub distinct_reified_graphs: u64,
    /// Distinct **annotation** SIDs that appear in `f:reifiesGraph`
    /// rows. Used as `ndv_subjects` for `<known_ann> f:reifiesGraph
    /// ?g` probes — the right denominator for BoundSubject
    /// selectivity (each row's subject is the ann SID, and not every
    /// ann SID has a graph row when the slot is sparse). Under the
    /// v1 single-target invariant this equals `reifies_graph_rows`;
    /// under the multi-target anomaly it can be smaller.
    #[serde(default)]
    pub distinct_graph_anns: u64,

    /// **Always 0 from the arena builder.** The arena reconstructs
    /// `EdgeKey.dt` from the flake-level dt of `f:reifiesObject`, so
    /// it cannot tell whether the on-wire bundle actually emitted a
    /// separate `f:reifiesDatatype` flake (full bundle path) or
    /// omitted it (JSON-LD-compatible cascade). Reporting a synth
    /// here would let `merge_annotation_stats` overwrite the real
    /// `IndexStats.properties` HLL with a phantom row count. The
    /// HLL is the source of truth for this slot. Field kept on the
    /// struct for forward-compat in case a future builder tracks
    /// the actual flake presence.
    #[serde(default)]
    pub reifies_datatype_rows: u64,
    /// Always 0 from the arena builder; see `reifies_datatype_rows`.
    #[serde(default)]
    pub distinct_reified_datatypes: u64,

    /// Live `f:reifiesLang` rows. Per `(edge, ann)` pair; same
    /// invariant story as `reifies_graph_rows`.
    #[serde(default)]
    pub reifies_lang_rows: u64,
    /// Distinct language-tag values across live reified edges.
    #[serde(default)]
    pub distinct_reified_langs: u64,
    /// Distinct annotation SIDs that appear in `f:reifiesLang`
    /// rows. See `distinct_graph_anns` for rationale.
    #[serde(default)]
    pub distinct_lang_anns: u64,

    /// Live `f:reifiesListIndex` rows. v1 always 0 — list-element
    /// annotations are deferred (see `docs/concepts/edge-annotations.md` "Current limits").
    #[serde(default)]
    pub reifies_list_index_rows: u64,
    /// Distinct list-index values across live reified edges.
    #[serde(default)]
    pub distinct_reified_list_indices: u64,
    /// Distinct annotation SIDs that appear in `f:reifiesListIndex`
    /// rows. See `distinct_graph_anns` for rationale.
    #[serde(default)]
    pub distinct_list_index_anns: u64,
}

/// Inline section in the binary index root.
///
/// Carries CIDs for the forward/reverse arena root branches plus
/// build-time stats. Always emitted (with empty CIDs and zero stats)
/// when the indexed snapshot might contain attachments — never as
/// `None` if uncertain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnotationIndexRoot {
    /// Format version of the on-disk arena artifacts. Independent of
    /// the parent index root's own version so the arena format can
    /// roll forward without a full FIR6 bump.
    pub version: u8,
    /// Highest commit `t` reflected in either arena. Reads with
    /// `as_of_t` above this fall back to novelty for any newer
    /// attachments.
    pub max_t: i64,
    /// Forward-arena branch CID (`EAFB1`).
    pub forward_branch_cid: ContentId,
    /// Reverse-arena branch CID (`EARB1`).
    pub reverse_branch_cid: ContentId,
    /// Build-time stats. Always present (zero-valued for empty arenas).
    pub stats: AnnotationStats,
}
