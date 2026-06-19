//! `HashJoinOperator` — build/probe inner join for object→subject "path" joins.
//!
//! This is the cure for the BSBM-BI "small + large" two-pattern join slowdown
//! (see `docs/troubleshooting/performance-tracing.md` and the benchmark report
//! `bsbm-bi-fluree-100m-join-scaling.md`). The minimal repro:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?c) WHERE {
//!   ?review  rev:reviewer ?reviewer .            # LARGE: full predicate scan
//!   ?reviewer bsbm:country <US> .                 # SMALL: selective bound object
//! }
//! ```
//!
//! The planner correctly drives from the selective bound-object (country) side,
//! which makes the large `rev:reviewer` pattern a right scan whose **object** is
//! bound from the left. The default `NestedLoopJoinOperator` then resolves it via
//! the batched-OBJECT path (`flush_batched_object_accumulator_binary`), seeking the
//! **object-major global OPST index** once per distinct driving object. Because a
//! single predicate's triples are scattered across the whole OPST key space, that
//! degrades superlinearly (≈47 s at 100M for ~61.8K driving objects).
//!
//! `HashJoinOperator` instead:
//!   1. **Build** a hash table from the small (driving) side, keyed by the join var.
//!   2. **Probe** by scanning the large predicate's *contiguous* PSOT/POST partition
//!      exactly once and looking each row's object up in the table.
//!
//! This turns N scattered object seeks into one sequential predicate scan + hash
//! probes (the large `rev:reviewer` scan alone is ~75 ms at 100M), which is what
//! cardinality-aware engines do for this shape.
//!
//! ## Correctness of the join key
//!
//! Ref bindings can appear as late-materialised `EncodedSid` *or* materialised `Sid`
//! for the *same* entity, and `binding_to_group_key_owned` hashes those to different
//! keys. To make build- and probe-side keys comparable we normalise every resolvable
//! ref to its `u64` subject id via the binary store (mirroring the batched-object
//! path in `join.rs`); unresolvable/non-ref values fall back to the group key, which
//! is consistent across sides in store-less (memory) mode.
//!
//! ## Selection
//!
//! [`HashJoinPlanner`] (below) chooses this operator with a cost model: the shape
//! must match (single shared var = the bound object, fixed predicate, new subject
//! var, no object bounds/inline ops) AND the probe predicate must be large enough
//! that scattered seeks dominate. `FLUREE_HASH_JOIN` is a force-override only
//! (`On`/`Off`). `where_plan` threads one planner through a join block; at `open()`,
//! if the query is a true multi-graph dataset (ledger-local key normalisation would
//! be wrong), the operator falls back to a `NestedLoopJoinOperator` over the same
//! inputs.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use rustc_hash::FxHashMap;

use fluree_db_core::{ObjectBounds, StatsView};

use crate::binary_scan::EmitMask;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::group_aggregate::{
    binding_to_group_key_normalized, binding_to_group_key_owned, GroupKeyOwned,
};
use crate::ir::triple::TriplePattern;
use crate::join::NestedLoopJoinOperator;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::temporal_mode::TemporalMode;
use crate::var_registry::VarId;

/// Target number of output rows per produced batch.
const OUTPUT_BATCH_TARGET: usize = 4096;

// ── Planning: when to use the hash join ──────────────────────────────────────
//
// `HashJoinPlanner` is threaded through a WHERE join block by `where_plan`. It owns
// the force mode, the `StatsView`, and the running cardinality of the driving chain,
// and decides per pattern whether the object→subject join should use this operator.

/// Force-override for the cost-based object→subject hash-join decision.
///
/// `FLUREE_HASH_JOIN`: unset/other => `Auto` (the cost model decides), `1`/`true`
/// => `On` (force the hash join whenever the shape matches, ignoring cost — keeps
/// force-on usable on stats-less ledgers), `0`/`false` => `Off` (never hash-join).
#[derive(Clone, Copy, PartialEq, Eq)]
enum HashJoinForce {
    Auto,
    On,
    Off,
}

fn hash_join_force() -> HashJoinForce {
    match std::env::var("FLUREE_HASH_JOIN") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => HashJoinForce::On,
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("false") => HashJoinForce::Off,
        _ => HashJoinForce::Auto,
    }
}

/// Probe-predicate floor: below this, scattered OPST object seeks stay cheap and the
/// hash join is often a wash.
///
/// The original 250k floor was tuned on smaller indexes and rejected mid-size
/// object stars where a ~70k-row driving set caused tens of thousands of
/// scattered OPST seeks into a ~136k-row probe predicate. On very large
/// indexes, scanning such a predicate once is decisively cheaper, so keep the
/// floor near the observed lower crossover while relying on the scan-ratio cap
/// below to reject small-driving-set pathologies.
const HASH_JOIN_PROBE_MIN: u64 = 50_000;
/// For small driving sets, even a below-floor probe predicate is worth scanning
/// once. Deep object-star chains commonly start from a 1k-5k-row anchor and then
/// suffer thousands of scattered OPST seeks into 3k-35k-row predicates; a compact
/// hash build is cheaper and remains memory-bounded.
const HASH_JOIN_SMALL_BUILD_MAX: f64 = 20_000.0;
const HASH_JOIN_SMALL_BUILD_PROBE_MIN: u64 = 1_000;
/// A wider intermediate is still safe to hash-build when it is in the low
/// hundreds of thousands and the alternative is that many scattered OPST seeks
/// into a smaller predicate. This catches long path-join tails without
/// admitting million-row cyclic intermediates.
const HASH_JOIN_MEDIUM_BUILD_MAX: f64 = 250_000.0;
/// Auto hash-join freely when the build side still looks like a base scan or
/// narrow two-column stream. Wider intermediates are allowed only while their
/// estimated row count stays under the medium-build cap; the operator drains the
/// build side in `open()`, so a wide, high-row intermediate can defeat outer
/// LIMIT.
const HASH_JOIN_AUTO_NARROW_BUILD_SCHEMA: usize = 2;
/// Don't scan a probe predicate more than this many times the driving-set size —
/// a guard against the pathological "scan a huge predicate for a handful of driving
/// rows" case. It is deliberately loose: the alternative we replace is the scattered
/// OPST object seek, measured at ~760 µs/seek (47 s / 61.8K driving rows at 100M)
/// versus ~26 ns per sequential probe row, so the *true* break-even ratio is ≈29,000×.
/// We sit far below that (room for the build-side hash + memory), but well above the
/// estimate error from skewed objects: `driving_est` for a bound object is the AVERAGE
/// object size (`count/ndv`), which undershoots a popular value like BSBM `country=US`
/// (avg ~28K vs actual ~61.8K), so a tight cap (the old 64×) wrongly rejected the very
/// join this operator exists for. See the BI-1 case in `hash_join_cost_wins` tests.
const HASH_JOIN_MAX_SCAN_RATIO: f64 = 1024.0;

/// The hash join wins when the probe predicate is large enough that scattered OPST
/// seeks dominate, but not so large relative to the driving set that the single
/// contiguous scan is wasteful. `false` when probe stats are absent (=> the safe
/// `NestedLoopJoinOperator` default). Constants tuned to BSBM 1M/10M/100M; heuristic.
fn hash_join_cost_wins(probe_count: Option<u64>, driving_est: Option<f64>) -> bool {
    let Some(pc) = probe_count else { return false };
    let drive = driving_est.unwrap_or(0.0).max(1.0);
    let within_scan_budget = (pc as f64) <= drive * HASH_JOIN_MAX_SCAN_RATIO;
    within_scan_budget
        && (pc >= HASH_JOIN_PROBE_MIN
            || (drive <= HASH_JOIN_SMALL_BUILD_MAX && pc >= HASH_JOIN_SMALL_BUILD_PROBE_MIN)
            || (drive <= HASH_JOIN_MEDIUM_BUILD_MAX
                && pc >= HASH_JOIN_SMALL_BUILD_PROBE_MIN
                && (pc as f64) <= drive))
}

/// Why the object→subject hash join was (or was not) chosen. Mirrors the
/// `FallbackReason` pattern in `explain.rs` — surfaced by `EXPLAIN` so a
/// `HashJoin` vs `NestedLoop` choice carries its rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashJoinReason {
    /// `FLUREE_HASH_JOIN=1` forced the hash join.
    ForcedOn,
    /// `FLUREE_HASH_JOIN=0` forced the nested-loop default.
    ForcedOff,
    /// Auto mode: probe is large enough and the scan ratio is within bounds.
    CostWins,
    /// Probe predicate count below [`HASH_JOIN_PROBE_MIN`] — scattered seeks stay cheap.
    ProbeTooSmall,
    /// Probe is more than [`HASH_JOIN_MAX_SCAN_RATIO`]× the driving set — scan too wasteful.
    ScanRatioTooHigh,
    /// No probe-predicate stats, so the cost model can't justify the hash join.
    NoProbeStats,
    /// The object-join shape matched, but the build side is already a wider
    /// intermediate. Auto hash join would drain it in open() and can defeat LIMIT.
    BuildSideTooWide,
    /// The subject (not the object) is bound from the left, so this is a forward
    /// join the object→subject hash can't replace. Reordering to drive the other
    /// end is what helps (the BSBM-BI bowtie case).
    SubjectDriven,
}

impl HashJoinReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            HashJoinReason::ForcedOn => "forced-on",
            HashJoinReason::ForcedOff => "forced-off",
            HashJoinReason::CostWins => "cost-wins",
            HashJoinReason::ProbeTooSmall => "probe-too-small",
            HashJoinReason::ScanRatioTooHigh => "scan-ratio-too-high",
            HashJoinReason::NoProbeStats => "no-probe-stats",
            HashJoinReason::BuildSideTooWide => "build-side-too-wide",
            HashJoinReason::SubjectDriven => "subject-driven-forward-join",
        }
    }
}

/// The annotated object→subject hash-join decision for one probe pattern: the
/// cost inputs the planner weighed and the outcome. Computed at plan time and
/// stashed on the chosen operator so `EXPLAIN` can render *why* without
/// re-deriving (the driving-set estimate depends on per-block chain order).
#[derive(Debug, Clone, Copy)]
pub(crate) struct HashJoinDecision {
    /// The shared join var when the hash join was chosen; `None` for a rejected or
    /// non-object→subject (forward) join, which carries only a reason.
    pub(crate) join_var: Option<VarId>,
    pub(crate) probe_count: Option<u64>,
    pub(crate) driving_est: Option<f64>,
    pub(crate) scan_ratio: Option<f64>,
    pub(crate) chosen: bool,
    pub(crate) reason: HashJoinReason,
}

impl HashJoinDecision {
    /// Render the decision into an `EXPLAIN` plan-node detail map.
    pub(crate) fn write_details(&self, m: &mut serde_json::Map<String, serde_json::Value>) {
        m.insert("hash-join-chosen".into(), self.chosen.into());
        m.insert("hash-join-reason".into(), self.reason.as_str().into());
        if let Some(pc) = self.probe_count {
            m.insert("probe-count".into(), pc.into());
        }
        if let Some(d) = self.driving_est {
            m.insert("driving-est".into(), (d.round() as i64).into());
        }
        if let Some(r) = self.scan_ratio {
            m.insert("scan-ratio".into(), format!("{r:.1}").into());
        }
    }
}

/// Probe-predicate row count from stats, keyed by SID or — for the un-encoded IRI
/// predicates that non-reasoning queries carry — by IRI. Mirrors the planner's
/// `property_stats` fallback so the cost model sees the same numbers reorder did.
fn predicate_count(stats: &StatsView, pred: &crate::ir::triple::Ref) -> Option<u64> {
    if let Some(sid) = pred.as_sid() {
        return stats
            .get_property(sid)
            .map(|p| p.count)
            .or_else(|| stats.has_property_stats().then_some(0));
    }
    if let Some(iri) = pred.as_iri() {
        return stats
            .get_property_by_iri(iri)
            .map(|p| p.count)
            .or_else(|| stats.has_property_stats().then_some(0));
    }
    None
}

/// Classification of a join pattern relative to the object→subject hash join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectHashShape {
    /// Fixed predicate, object bound from the left (the join key), subject new —
    /// the exact shape the hash join replaces (else the batched-OBJECT OPST path).
    Eligible(VarId),
    /// Fixed predicate, but the SUBJECT is bound from the left and the OBJECT is
    /// new: a forward (subject-driven) join. The object→subject hash can't help —
    /// reordering to drive the other end is the fix. Surfaced in EXPLAIN.
    SubjectDriven,
    /// Not an object→subject candidate at all (var predicate, object bounds, a
    /// datatype constraint, inline ops, or both endpoints already bound).
    NotCandidate,
}

/// Classify the right pattern `tp` for the object→subject hash join. A fixed
/// predicate may be a SID or an IRI: non-reasoning SPARQL/JSON-LD queries reach
/// planning with IRI predicates (only reasoning queries are pre-encoded to SIDs in
/// runner.rs), so a SID-only check would silently exclude the exact BSBM joins this
/// operator targets.
fn object_hash_shape(
    left_schema: &[VarId],
    tp: &TriplePattern,
    has_bounds: bool,
    inline_ops_empty: bool,
) -> ObjectHashShape {
    if has_bounds || !inline_ops_empty || tp.dtc.is_some() || tp.p.as_var().is_some() {
        return ObjectHashShape::NotCandidate;
    }
    let (Some(o_var), Some(s_var)) = (tp.o.as_var(), tp.s.as_var()) else {
        return ObjectHashShape::NotCandidate;
    };
    let o_bound = left_schema.contains(&o_var);
    let s_bound = left_schema.contains(&s_var);
    match (o_bound, s_bound) {
        // Object bound from the left, subject new: the hash-join shape.
        (true, false) => ObjectHashShape::Eligible(o_var),
        // Subject bound, object new: a forward join the hash join can't replace.
        (false, true) => ObjectHashShape::SubjectDriven,
        _ => ObjectHashShape::NotCandidate,
    }
}

#[cfg(test)]
fn hash_join_object_join_var(
    left_schema: &[VarId],
    tp: &TriplePattern,
    has_bounds: bool,
    inline_ops_empty: bool,
) -> Option<VarId> {
    match object_hash_shape(left_schema, tp, has_bounds, inline_ops_empty) {
        ObjectHashShape::Eligible(v) => Some(v),
        _ => None,
    }
}

/// Per-WHERE-block planning state for the object→subject hash join.
///
/// One instance is threaded through a join block so `build_triple_operators` and
/// `build_sequential_join_block` share the same cost state — the running driving-set
/// cardinality, the resolved force mode, and stats — instead of each re-deriving it
/// and widening every helper signature.
pub(crate) struct HashJoinPlanner<'a> {
    stats: Option<&'a StatsView>,
    force: HashJoinForce,
    /// Running product of per-pattern estimates for the chain built so far.
    driving_est: f64,
    /// `driving_est` snapshot from the most recent [`before_step`](Self::before_step)
    /// — the driving-set size the next probe is weighed against. `None` until
    /// `before_step` runs with stats present, so single-pattern / stats-less callers
    /// never auto-fire (force-`On` still does).
    step_est: Option<f64>,
}

impl<'a> HashJoinPlanner<'a> {
    pub(crate) fn new(stats: Option<&'a StatsView>) -> Self {
        Self {
            stats,
            force: hash_join_force(),
            driving_est: 1.0,
            step_est: None,
        }
    }

    /// Seed the running driving estimate from the block's incoming LEFT operator
    /// (e.g. a `SubqueryOperator` producing `WITH DISTINCT friend`). Without this
    /// the first probe in the block is costed against `driving_est = 1` — so a
    /// large object predicate trips `scan-ratio-too-high` and the hash join is
    /// wrongly rejected, even though the left side produces hundreds of rows.
    /// `None` (left side has no estimate) leaves the default 1.0.
    pub(crate) fn with_left_estimate(mut self, left_est: Option<usize>) -> Self {
        if let Some(n) = left_est {
            self.driving_est = (n as f64).max(1.0);
        }
        self
    }

    /// Advance the running driving cardinality past one pattern. Call once per
    /// pattern, in chain order, BEFORE building its operator. Snapshots the product
    /// of the patterns to the LEFT of `tp` (this becomes `step_est`, the size the
    /// probe is weighed against), then folds `tp`'s own estimate in for later steps.
    /// The snapshot excludes `tp` on purpose: as a probe its object is bound from the
    /// left, so estimating it would misclassify as a selective BoundObject scan.
    /// `bound` is the set of variables bound by the chain to the left of `tp`.
    pub(crate) fn before_step(&mut self, tp: &TriplePattern, bound: &HashSet<VarId>) {
        self.step_est = self.stats.map(|stats| {
            let snapshot = self.driving_est;
            self.driving_est *= crate::planner::estimate_triple_row_count(tp, bound, Some(stats));
            snapshot
        });
    }

    /// Compute the annotated object→subject hash-join decision for the current
    /// probe pattern: the shared join var (when the shape is eligible), the cost
    /// inputs weighed, and the chosen/why outcome. Returns `None` only when the
    /// pattern is not even a fixed-predicate two-var join (no decision to explain);
    /// a subject-driven forward join returns a decision carrying its reason + the
    /// driving-set size. Honors the force mode; under `Auto` it weighs the probe
    /// predicate's size against the latest `before_step` snapshot.
    ///
    /// `build_scan_or_join` applies `chosen` to pick the operator and stashes the
    /// decision so `EXPLAIN` can render it. The hot path reads only `chosen`.
    pub(crate) fn explain_object_hash_join(
        &self,
        left_schema: &[VarId],
        tp: &TriplePattern,
        has_bounds: bool,
        inline_ops_empty: bool,
    ) -> Option<HashJoinDecision> {
        let driving_est = self.step_est;
        let join_var = match object_hash_shape(left_schema, tp, has_bounds, inline_ops_empty) {
            ObjectHashShape::Eligible(v) => v,
            // Forward join: surface why the NestedLoop was chosen + the driving size
            // (reordering to drive the other end is the fix), but no cost gate runs.
            ObjectHashShape::SubjectDriven => {
                return Some(HashJoinDecision {
                    join_var: None,
                    probe_count: self.stats.and_then(|s| predicate_count(s, &tp.p)),
                    driving_est,
                    scan_ratio: None,
                    chosen: false,
                    reason: HashJoinReason::SubjectDriven,
                });
            }
            ObjectHashShape::NotCandidate => return None,
        };

        if self.force == HashJoinForce::Off {
            return Some(HashJoinDecision {
                join_var: Some(join_var),
                probe_count: None,
                driving_est,
                scan_ratio: None,
                chosen: false,
                reason: HashJoinReason::ForcedOff,
            });
        }

        if self.force == HashJoinForce::Auto
            && left_schema.len() > HASH_JOIN_AUTO_NARROW_BUILD_SCHEMA
            && driving_est.unwrap_or(f64::INFINITY) > HASH_JOIN_MEDIUM_BUILD_MAX
        {
            let probe_count = self.stats.and_then(|s| predicate_count(s, &tp.p));
            let scan_ratio = match (probe_count, driving_est) {
                (Some(pc), Some(d)) => Some(pc as f64 / d.max(1.0)),
                _ => None,
            };
            return Some(HashJoinDecision {
                join_var: Some(join_var),
                probe_count,
                driving_est,
                scan_ratio,
                chosen: false,
                reason: HashJoinReason::BuildSideTooWide,
            });
        }

        let probe_count = self.stats.and_then(|s| predicate_count(s, &tp.p));
        let scan_ratio = match (probe_count, driving_est) {
            (Some(pc), Some(d)) => Some(pc as f64 / d.max(1.0)),
            _ => None,
        };

        let (chosen, reason) = match self.force {
            HashJoinForce::On => (true, HashJoinReason::ForcedOn),
            HashJoinForce::Off => unreachable!("handled above"),
            HashJoinForce::Auto => match probe_count {
                None => (false, HashJoinReason::NoProbeStats),
                Some(pc) if pc < HASH_JOIN_SMALL_BUILD_PROBE_MIN => {
                    (false, HashJoinReason::ProbeTooSmall)
                }
                Some(pc) => {
                    let drive = driving_est.unwrap_or(0.0).max(1.0);
                    let within_scan_budget = (pc as f64) <= drive * HASH_JOIN_MAX_SCAN_RATIO;
                    let probe_large_enough = pc >= HASH_JOIN_PROBE_MIN
                        || drive <= HASH_JOIN_SMALL_BUILD_MAX
                        || (drive <= HASH_JOIN_MEDIUM_BUILD_MAX && (pc as f64) <= drive);
                    if within_scan_budget && probe_large_enough {
                        (true, HashJoinReason::CostWins)
                    } else if !within_scan_budget {
                        (false, HashJoinReason::ScanRatioTooHigh)
                    } else {
                        (false, HashJoinReason::ProbeTooSmall)
                    }
                }
            },
        };

        debug_assert_eq!(
            chosen,
            self.force != HashJoinForce::Off
                && match self.force {
                    HashJoinForce::On => true,
                    _ => hash_join_cost_wins(probe_count, self.step_est),
                },
            "explain_object_hash_join must agree with the cost gate"
        );

        Some(HashJoinDecision {
            join_var: Some(join_var),
            probe_count,
            driving_est,
            scan_ratio,
            chosen,
            reason,
        })
    }
}

/// A join key that is comparable across build/probe sides regardless of whether a
/// ref was delivered as `EncodedSid` or materialised `Sid`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinKey {
    /// A resolved subject id (ledger-local; valid because the operator is
    /// single-store / native).
    Ref(u64),
    /// Fallback for literals or refs that could not be resolved to a subject id.
    Other(GroupKeyOwned),
}

/// How a binding's join-var value participates in the join.
enum JoinKeyClass {
    /// A comparable key — matches probe rows carrying the same value.
    Keyed(JoinKey),
    /// Unbound join var: unconstrained from the left, so it matches EVERY probe row
    /// (the right side fills it — the nested-loop "take the right value" semantics).
    Wildcard,
    /// Poisoned join var: a failed OPTIONAL/required binding *blocks* matching (see
    /// `optional.rs` — Poisoned yields no matches, not "match anything"), so the row
    /// produces no output and is dropped. Collapsing this into `Wildcard` would fan a
    /// failed OPTIONAL out to every probe row instead of producing no match.
    Dead,
}

/// Classify a binding's join key, normalising refs to a `u64` s_id when a store is
/// available. Unbound → wildcard; Poisoned → dead (drop); everything else → keyed.
fn join_key(
    binding: &Binding,
    store: Option<&BinaryIndexStore>,
    gv: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> JoinKeyClass {
    let keyed_ref_or_group = |sid: &fluree_db_core::Sid| {
        store
            .and_then(|s| {
                s.find_subject_id_by_parts(sid.namespace_code, &sid.name)
                    .ok()
                    .flatten()
            })
            .map(JoinKey::Ref)
            .unwrap_or_else(|| JoinKey::Other(binding_to_group_key_owned(binding)))
    };
    match binding {
        Binding::EncodedSid { s_id, .. } => JoinKeyClass::Keyed(JoinKey::Ref(*s_id)),
        Binding::Sid { sid, .. } => JoinKeyClass::Keyed(keyed_ref_or_group(sid)),
        Binding::IriMatch { primary_sid, .. } => {
            JoinKeyClass::Keyed(keyed_ref_or_group(primary_sid))
        }
        Binding::Unbound => JoinKeyClass::Wildcard,
        Binding::Poisoned => JoinKeyClass::Dead,
        // Normalize decoded literals to their encoded form so they key
        // identically to late-materialized scan output.
        other => JoinKeyClass::Keyed(JoinKey::Other(binding_to_group_key_normalized(
            other, store, gv,
        ))),
    }
}

/// Inner hash join: build a table from `build` (the small side), probe with `probe`
/// (a contiguous scan of the large predicate), joining on a single shared variable.
pub struct HashJoinOperator {
    /// Small (driving) side. Consumed in `open()` — drained into the hash table on
    /// the hash path, or handed to the nested-loop on the multi-graph fallback path.
    build: Option<BoxedOperator>,
    /// Large side — streamed once during `next_batch()` / `drain_count()`. Dropped
    /// unused when the fallback path is taken.
    probe: Option<BoxedOperator>,
    /// Probe pattern + params, kept so `open()` can build a `NestedLoopJoinOperator`
    /// fallback when the hash path is not safe (true multi-graph dataset, where the
    /// ledger-local key normalisation would be wrong).
    right_pattern: TriplePattern,
    nl_bounds: Option<ObjectBounds>,
    nl_mode: TemporalMode,
    nl_downstream: Option<Arc<[VarId]>>,
    /// Set in `open()` when the hash path is unsafe; all output is delegated to it.
    fallback: Option<BoxedOperator>,
    /// `build.schema()` captured at construction (column order of build rows).
    build_schema: Arc<[VarId]>,
    /// Full output schema: `build_schema` ++ probe vars not already in build_schema.
    full_schema: Arc<[VarId]>,
    /// Trimmed output schema when downstream only needs a subset.
    out_schema: Option<Arc<[VarId]>>,
    /// Column of the join var within `build_schema`.
    build_key_col: usize,
    /// Column of the join var within `probe.schema()`.
    probe_key_col: usize,
    /// Columns of `probe.schema()` appended to the output (probe vars not in build).
    probe_emit_cols: Vec<usize>,
    /// Hash table: join key → all build rows with that key (each row aligned to
    /// `build_schema`). Multiplicity is preserved for correct COUNT semantics.
    table: FxHashMap<JoinKey, Vec<Vec<Binding>>>,
    /// Build rows whose join key is Unbound/Poisoned. The nested-loop join treats an
    /// unbound shared var as unconstrained — the right side fills it — so these rows
    /// must match EVERY probe row (with the join var taking the probe value), not be
    /// dropped. Empty in the common case (join var always bound), so zero overhead.
    wildcard_rows: Vec<Vec<Binding>>,
    /// Current probe batch being consumed and the next row to process.
    cur_probe: Option<Batch>,
    cur_probe_row: usize,
    state: OperatorState,
    /// The planner's annotated decision, for `EXPLAIN`. Never read on the hot path.
    hj_decision: Option<HashJoinDecision>,
}

impl HashJoinOperator {
    /// Construct from a build (small) side, a probe (large) scan, and the single
    /// shared join variable. `downstream_vars` trims the output when provided.
    ///
    /// Caller (`build_scan_or_join`) guarantees `join_var` is present in both
    /// schemas via its eligibility check, so column lookups cannot fail.
    pub fn new(
        build: BoxedOperator,
        probe: BoxedOperator,
        join_var: VarId,
        downstream_vars: Option<&[VarId]>,
        right_pattern: TriplePattern,
        nl_bounds: Option<ObjectBounds>,
        nl_mode: TemporalMode,
    ) -> Self {
        let build_schema: Arc<[VarId]> = Arc::from(build.schema().to_vec().into_boxed_slice());
        let probe_schema: Vec<VarId> = probe.schema().to_vec();

        let build_key_col = build_schema
            .iter()
            .position(|v| *v == join_var)
            .expect("hash join: join var must be in build schema");
        let probe_key_col = probe_schema
            .iter()
            .position(|v| *v == join_var)
            .expect("hash join: join var must be in probe schema");

        // Output = build columns, then probe vars not already produced by build.
        let mut full = build_schema.to_vec();
        let mut probe_emit_cols = Vec::new();
        for (i, v) in probe_schema.iter().enumerate() {
            if !build_schema.contains(v) {
                probe_emit_cols.push(i);
                full.push(*v);
            }
        }
        let full_schema: Arc<[VarId]> = Arc::from(full.into_boxed_slice());
        let out_schema = compute_trimmed_vars(&full_schema, downstream_vars);
        let nl_downstream: Option<Arc<[VarId]>> =
            downstream_vars.map(|d| Arc::from(d.to_vec().into_boxed_slice()));

        Self {
            build: Some(build),
            probe: Some(probe),
            right_pattern,
            nl_bounds,
            nl_mode,
            nl_downstream,
            fallback: None,
            build_schema,
            full_schema,
            out_schema,
            build_key_col,
            probe_key_col,
            probe_emit_cols,
            table: FxHashMap::default(),
            wildcard_rows: Vec::new(),
            cur_probe: None,
            cur_probe_row: 0,
            state: OperatorState::Created,
            hj_decision: None,
        }
    }

    /// Attach the planner's object→subject hash-join decision for `EXPLAIN`.
    /// Plan-only — does not affect execution.
    pub(crate) fn with_hash_join_decision(mut self, decision: HashJoinDecision) -> Self {
        self.hj_decision = Some(decision);
        self
    }

    /// Drain `build` into the hash table. Called once in `open()` on the hash path.
    async fn build_table(
        &mut self,
        ctx: &ExecutionContext<'_>,
        mut build: BoxedOperator,
    ) -> Result<()> {
        let store = ctx.binary_store.as_deref();
        let gv = ctx.graph_view();
        let ncols = self.build_schema.len();
        build.open(ctx).await?;
        while let Some(batch) = build.next_batch(ctx).await? {
            for row in 0..batch.len() {
                let row_vals: Vec<Binding> = (0..ncols)
                    .map(|c| batch.get_by_col(row, c).clone())
                    .collect();
                match join_key(
                    batch.get_by_col(row, self.build_key_col),
                    store,
                    gv.as_ref(),
                ) {
                    JoinKeyClass::Keyed(key) => self.table.entry(key).or_default().push(row_vals),
                    // Unbound join var: unconstrained, matches every probe row.
                    JoinKeyClass::Wildcard => self.wildcard_rows.push(row_vals),
                    // Poisoned join var: blocks matching — drop the row (no output).
                    JoinKeyClass::Dead => {}
                }
            }
        }
        build.close();
        Ok(())
    }
}

#[async_trait]
impl Operator for HashJoinOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        let mut v = Vec::new();
        if let Some(b) = self.build.as_deref() {
            v.push(crate::plan_node::PlanChild::child(b));
        }
        if let Some(p) = self.probe.as_deref() {
            v.push(crate::plan_node::PlanChild::child(p));
        }
        v
    }
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert(
            "probe".into(),
            crate::explain::format_pattern(&self.right_pattern).into(),
        );
        if let Some(d) = &self.hj_decision {
            d.write_details(&mut m);
        }
        m
    }
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.full_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.state != OperatorState::Created {
            return Err(QueryError::Internal(
                "HashJoinOperator::open() called in invalid state".into(),
            ));
        }
        let build = self.build.take().expect("hash join build side");
        // Join keys normalise refs to ledger-local subject ids, which would collide
        // across ledgers in a true multi-graph dataset (Many with >1 graph). Rather
        // than error (the cost planner can auto-select us), fall back to a nested-loop
        // join over the same driving side + probe pattern, which is graph-correct.
        let multi_graph =
            matches!(ctx.active_graphs(), ActiveGraphs::Many(graphs) if graphs.len() > 1);
        if multi_graph {
            let mut nl = NestedLoopJoinOperator::new(
                build,
                Arc::clone(&self.build_schema),
                self.right_pattern.clone(),
                self.nl_bounds.clone(),
                Vec::new(),
                EmitMask::ALL,
                self.nl_mode,
            )
            .with_out_schema(self.nl_downstream.as_deref());
            nl.open(ctx).await?;
            self.fallback = Some(Box::new(nl));
        } else {
            self.build_table(ctx, build).await?;
            self.probe
                .as_mut()
                .expect("hash join probe")
                .open(ctx)
                .await?;
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if let Some(fallback) = self.fallback.as_mut() {
            return fallback.next_batch(ctx).await;
        }
        let store = ctx.binary_store.as_deref();
        let gv = ctx.graph_view();
        let ncols = self.full_schema.len();
        let build_cols = self.build_schema.len();
        let probe = self.probe.as_mut().expect("hash join probe");

        loop {
            // Ensure we have a probe batch to consume.
            if self.cur_probe.is_none() {
                match probe.next_batch(ctx).await? {
                    Some(b) if !b.is_empty() => {
                        self.cur_probe = Some(b);
                        self.cur_probe_row = 0;
                    }
                    Some(_) => continue,
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            let pb = self.cur_probe.as_ref().unwrap();
            let mut out_cols: Vec<Vec<Binding>> = (0..ncols).map(|_| Vec::new()).collect();
            let mut produced = 0usize;

            while self.cur_probe_row < pb.len() {
                let row = self.cur_probe_row;
                self.cur_probe_row += 1;

                // The probe scan always binds the join var, so a non-keyed probe row
                // (unbound/poisoned) cannot match — skip it.
                let JoinKeyClass::Keyed(key) =
                    join_key(pb.get_by_col(row, self.probe_key_col), store, gv.as_ref())
                else {
                    continue;
                };
                if let Some(matches) = self.table.get(&key) {
                    for build_row in matches {
                        for (c, b) in build_row.iter().enumerate() {
                            out_cols[c].push(b.clone());
                        }
                        for (i, &pc) in self.probe_emit_cols.iter().enumerate() {
                            out_cols[build_cols + i].push(pb.get_by_col(row, pc).clone());
                        }
                        produced += 1;
                    }
                }
                // Unbound-key build rows match every probe row; the join var takes the
                // probe value (the nested-loop "take the right side" semantics).
                if !self.wildcard_rows.is_empty() {
                    let probe_key = pb.get_by_col(row, self.probe_key_col);
                    for wild in &self.wildcard_rows {
                        for (c, b) in wild.iter().enumerate() {
                            out_cols[c].push(if c == self.build_key_col {
                                probe_key.clone()
                            } else {
                                b.clone()
                            });
                        }
                        for (i, &pc) in self.probe_emit_cols.iter().enumerate() {
                            out_cols[build_cols + i].push(pb.get_by_col(row, pc).clone());
                        }
                        produced += 1;
                    }
                }
                if produced >= OUTPUT_BATCH_TARGET {
                    break;
                }
            }

            if self.cur_probe_row >= pb.len() {
                self.cur_probe = None;
            }
            if produced == 0 {
                continue;
            }
            let batch = Batch::new(Arc::clone(&self.full_schema), out_cols)
                .map_err(|e| QueryError::Internal(format!("hash join batch: {e}")))?;
            return Ok(trim_batch(&self.out_schema, batch));
        }
    }

    fn close(&mut self) {
        if let Some(b) = self.build.as_mut() {
            b.close();
        }
        if let Some(p) = self.probe.as_mut() {
            p.close();
        }
        if let Some(f) = self.fallback.as_mut() {
            f.close();
        }
        self.table.clear();
        self.wildcard_rows.clear();
        self.cur_probe = None;
        self.state = OperatorState::Closed;
    }

    /// COUNT(*) fast path: stream the probe side and sum build-side multiplicity per
    /// matching key, without materialising any output bindings.
    async fn drain_count(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if let Some(fallback) = self.fallback.as_mut() {
            return fallback.drain_count(ctx).await;
        }
        let store = ctx.binary_store.as_deref();
        let gv = ctx.graph_view();
        let probe = self.probe.as_mut().expect("hash join probe");
        let mut count: u64 = 0;
        loop {
            match probe.next_batch(ctx).await? {
                Some(batch) if !batch.is_empty() => {
                    for row in 0..batch.len() {
                        let JoinKeyClass::Keyed(key) = join_key(
                            batch.get_by_col(row, self.probe_key_col),
                            store,
                            gv.as_ref(),
                        ) else {
                            continue;
                        };
                        // Each probe row matches its keyed build rows plus every
                        // wildcard (unbound-key) build row.
                        let matched =
                            self.table.get(&key).map_or(0, Vec::len) + self.wildcard_rows.len();
                        count = count.checked_add(matched as u64).ok_or_else(|| {
                            QueryError::execution("COUNT(*) overflow in hash join drain_count")
                        })?;
                    }
                }
                Some(_) => continue,
                None => break,
            }
        }
        self.state = OperatorState::Exhausted;
        Ok(Some(count))
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Output is bounded by the probe side's matching subset; use the probe
        // estimate as a coarse upper bound for downstream sizing.
        if let Some(f) = self.fallback.as_ref() {
            return f.estimated_rows();
        }
        self.probe.as_ref().and_then(|p| p.estimated_rows())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term};
    use fluree_db_core::{PropertyStatData, Sid};

    #[test]
    fn join_key_class_unbound_wildcard_poisoned_dead() {
        // Unbound => matches every probe row; Poisoned => blocks matching (drop).
        // Collapsing both to one class is the bug this guards against.
        assert!(matches!(
            join_key(&Binding::Unbound, None, None),
            JoinKeyClass::Wildcard
        ));
        assert!(matches!(
            join_key(&Binding::Poisoned, None, None),
            JoinKeyClass::Dead
        ));
        assert!(matches!(
            join_key(
                &Binding::lit(fluree_db_core::FlakeValue::Long(1), Sid::new(2, "long")),
                None,
                None
            ),
            JoinKeyClass::Keyed(_)
        ));
    }

    /// Emits one fixed batch then EOF — a stand-in build/probe input.
    struct OnceOp {
        schema: Arc<[VarId]>,
        batch: Option<Batch>,
    }

    #[async_trait]
    impl Operator for OnceOp {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }
        async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
            Ok(())
        }
        async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(self.batch.take())
        }
        fn close(&mut self) {}
    }

    #[tokio::test]
    async fn poisoned_build_key_blocks_matching_not_wildcard() {
        // BSBM/SKOS shape: OPTIONAL { ... ?x } . ?s :p ?x — a failed OPTIONAL leaves
        // ?x Poisoned on the driving side. The object→subject hash join must produce
        // NO match for that row (Poisoned blocks), not fan it out to every probe row.
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{FlakeValue, LedgerSnapshot};

        let snapshot = LedgerSnapshot::genesis("test/main");
        let mut vars = VarRegistry::new();
        let x = vars.get_or_insert("?x"); // join var (bound object)
        let driver = vars.get_or_insert("?driver");
        let s = vars.get_or_insert("?s");
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let key = || Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"));
        let d_ok = Binding::lit(FlakeValue::Long(10), Sid::new(2, "long"));
        let d_poisoned = Binding::lit(FlakeValue::Long(20), Sid::new(2, "long"));

        // Build (driving) side, columns [?x, ?driver]: row0 has a POISONED ?x, row1 keyed.
        let build_schema: Arc<[VarId]> = Arc::from(vec![x, driver].into_boxed_slice());
        let build_batch = Batch::new(
            build_schema.clone(),
            vec![
                vec![Binding::Poisoned, key()], // ?x column
                vec![d_poisoned, d_ok],         // ?driver column
            ],
        )
        .unwrap();

        // Probe side, columns [?x, ?s]: two rows, both ?x = key.
        let probe_schema: Arc<[VarId]> = Arc::from(vec![x, s].into_boxed_slice());
        let probe_batch = Batch::new(
            probe_schema.clone(),
            vec![
                vec![key(), key()], // ?x column
                vec![
                    Binding::lit(FlakeValue::Long(100), Sid::new(2, "long")),
                    Binding::lit(FlakeValue::Long(200), Sid::new(2, "long")),
                ], // ?s column
            ],
        )
        .unwrap();

        let right_pattern =
            TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(1, "p")), Term::Var(x));
        let mut hj = HashJoinOperator::new(
            Box::new(OnceOp {
                schema: build_schema,
                batch: Some(build_batch),
            }),
            Box::new(OnceOp {
                schema: probe_schema,
                batch: Some(probe_batch),
            }),
            x,
            None,
            right_pattern,
            None,
            crate::temporal_mode::PlanningContext::current().mode(),
        );
        hj.open(&ctx).await.unwrap();
        let mut rows = 0;
        while let Some(b) = hj.next_batch(&ctx).await.unwrap() {
            rows += b.len();
        }
        // Only the keyed build row joins (× 2 probe rows). The Poisoned row is dropped;
        // the old wildcard behaviour would have produced 4 rows.
        assert_eq!(
            rows, 2,
            "Poisoned build row must not fan out to every probe row"
        );
    }

    #[test]
    fn explains_subject_driven_forward_join() {
        // `?review rev:reviewer ?reviewer` with ?review bound from the left and
        // ?reviewer new is a forward (subject-driven) join — not an object→subject
        // hash candidate. The decision must still surface a reason + the driving-set
        // size so EXPLAIN's NestedLoop isn't opaque (this was the BSBM-BI F2 case).
        let review = VarId(0);
        let reviewer = VarId(1);
        let left_schema = [review]; // subject bound, object new
        let tp = TriplePattern::new(
            Ref::Var(review),
            Ref::Sid(Sid::new(1, "reviewer")),
            Term::Var(reviewer),
        );
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(1, "reviewer"),
            PropertyStatData {
                count: 2_848_260,
                ndv_values: 570_000,
                ndv_subjects: 2_848_260,
            },
        );
        let d = HashJoinPlanner::new(Some(&stats))
            .explain_object_hash_join(&left_schema, &tp, false, true)
            .expect("a subject-driven join still yields a decision to explain");
        assert!(!d.chosen);
        assert_eq!(d.join_var, None);
        assert_eq!(d.reason, HashJoinReason::SubjectDriven);
        assert_eq!(d.probe_count, Some(2_848_260));
    }

    #[test]
    fn iri_predicate_is_eligible_like_a_sid() {
        // Non-reasoning SPARQL keeps predicates as IRIs (not pre-encoded to SIDs).
        // The probe `?review <rev:reviewer> ?reviewer` with ?reviewer bound from the
        // left must be eligible — a SID-only check here was why the BSBM BI-1 join
        // never reached the cost gate.
        let review = VarId(0);
        let reviewer = VarId(1);
        let left_schema = [reviewer];
        let iri_tp = TriplePattern::new(
            Ref::Var(review),
            Ref::Iri(Arc::from("http://purl.org/stuff/rev#reviewer")),
            Term::Var(reviewer),
        );
        assert_eq!(
            hash_join_object_join_var(&left_schema, &iri_tp, false, true),
            Some(reviewer),
            "IRI-predicate probe must be eligible for the object→subject hash join"
        );

        // A variable predicate is still rejected (can't scan one contiguous partition).
        let var_pred_tp =
            TriplePattern::new(Ref::Var(review), Ref::Var(VarId(2)), Term::Var(reviewer));
        assert_eq!(
            hash_join_object_join_var(&left_schema, &var_pred_tp, false, true),
            None
        );
    }

    #[test]
    fn cost_gate_fires_for_skewed_bound_object_bi1() {
        // BSBM BI-1: probe rev:reviewer (2.85M rows) vs a driving `country=US` set.
        // The cost model estimates the driving side as the AVERAGE country size
        // (count/ndv ≈ 28k), not US's actual 61.8k. The 1024× cap must accept this
        // (28k×1024 = 28.6M ≥ 2.85M); the old 64× cap rejected it (28k×64 = 1.79M <
        // 2.85M), which is the regression this fixes.
        assert!(hash_join_cost_wins(Some(2_848_260), Some(28_000.0)));
        // Also passes against US's true (un-averaged) selectivity.
        assert!(hash_join_cost_wins(Some(2_848_260), Some(61_847.0)));
    }

    #[test]
    fn cost_gate_fires_for_midsize_object_star() {
        // A mid-size object star after reordering:
        //   build ~69,885 rows, probe ~136,131 rows, joined on object ?x.
        // The old 250k probe floor rejected this as "probe-too-small", forcing ~70k
        // scattered OPST object seeks. A single predicate scan is cheaper and allows
        // the outer LIMIT to stop after the first output batch.
        assert!(hash_join_cost_wins(Some(136_131), Some(69_885.0)));
    }

    #[test]
    fn producer_seed_clears_scan_ratio_cap() {
        // Coupling guard: `planner::DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY` (the
        // estimate handed to a downstream object→subject hash join when an
        // anchored `WITH DISTINCT` producer drives it) is load-bearing precisely
        // because it must clear this module's scan-ratio cap. Concretely the seed
        // must keep a probe of `seed × HASH_JOIN_MAX_SCAN_RATIO` rows within
        // budget — otherwise the hash join the producer ordering exists to unlock
        // is re-rejected as scan-ratio-too-high.
        let seed = crate::planner::DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY;
        // A probe sized between a too-small seed's budget (100 × cap = 102,400)
        // and the tuned seed's budget (500 × cap = 512,000): accepted at the
        // tuned seed, rejected if the seed shrinks to 100.
        let probe = 300_000_u64;
        assert!(
            hash_join_cost_wins(Some(probe), Some(seed)),
            "seed {seed} must keep a {probe}-row probe within the scan-ratio budget \
             (seed × {HASH_JOIN_MAX_SCAN_RATIO}); lower either constant and this fails"
        );
        // Demonstrate the coupling is real: a seed of 100 (the value the planner
        // doc warns against) re-rejects the same probe.
        assert!(
            !hash_join_cost_wins(Some(probe), Some(100.0)),
            "a 100-row seed must re-reject the probe — proving the tuned seed is load-bearing"
        );
    }

    #[test]
    fn cost_gate_fires_for_small_build_star_anchors() {
        // First object-star joins from slow deep-star plans (build -> probe rows):
        // 5,092 -> 7,100; 2,196 -> 35,246; 1,068 -> 7,684.
        //
        // The probe predicates are below the generic floor, but the build sides are
        // tiny enough that a hash table is cheap and avoids thousands of OPST seeks.
        assert!(hash_join_cost_wins(Some(7_100), Some(5_092.0)));
        assert!(hash_join_cost_wins(Some(35_246), Some(2_196.0)));
        assert!(hash_join_cost_wins(Some(7_684), Some(1_068.0)));
    }

    #[test]
    fn planner_chooses_hash_join_for_midsize_object_star() {
        let x = VarId(0);
        let y = VarId(1);
        let z = VarId(2);
        let build = TriplePattern::new(
            Ref::Var(z),
            Ref::Sid(Sid::new(100, "buildPred")),
            Term::Var(x),
        );
        let probe = TriplePattern::new(
            Ref::Var(y),
            Ref::Sid(Sid::new(100, "probePred")),
            Term::Var(x),
        );

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "buildPred"),
            PropertyStatData {
                count: 69_885,
                ndv_values: 74_401,
                ndv_subjects: 65_138,
            },
        );
        stats.properties.insert(
            Sid::new(100, "probePred"),
            PropertyStatData {
                count: 136_131,
                ndv_values: 135_901,
                ndv_subjects: 150_921,
            },
        );

        let mut planner = HashJoinPlanner::new(Some(&stats));
        let mut bound = HashSet::new();
        planner.before_step(&build, &bound);
        bound.extend(build.produced_vars());
        planner.before_step(&probe, &bound);

        let d = planner
            .explain_object_hash_join(&[z, x], &probe, false, true)
            .expect("mid-size probe should produce a hash-join decision");
        assert!(d.chosen);
        assert_eq!(d.reason, HashJoinReason::CostWins);
        assert_eq!(d.probe_count, Some(136_131));
        assert_eq!(d.driving_est.map(|v| v.round() as u64), Some(69_885));
    }

    #[test]
    fn planner_rejects_auto_hash_join_for_wide_intermediate() {
        let x = VarId(0);
        let w = VarId(3);
        let probe = TriplePattern::new(
            Ref::Var(w),
            Ref::Sid(Sid::new(100, "probePred")),
            Term::Var(x),
        );

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "probePred"),
            PropertyStatData {
                count: 100_000,
                ndv_values: 80_000,
                ndv_subjects: 90_000,
            },
        );

        let d = HashJoinPlanner::new(Some(&stats))
            .explain_object_hash_join(&[VarId(2), x, VarId(1)], &probe, false, true)
            .expect("wide continuation should still produce a decision");
        assert!(!d.chosen);
        assert_eq!(d.reason, HashJoinReason::BuildSideTooWide);
    }

    #[test]
    fn planner_allows_hash_join_for_small_wide_intermediate() {
        // After the first hash join in a deep star chain: the build-side schema
        // is already wide (?z, ?x, ?y), but the estimated intermediate is only
        // ~10,980 rows and the next predicate has 3,140 rows. Hashing the
        // intermediate avoids thousands of scattered OPST seeks.
        let x = VarId(0);
        let u = VarId(3);
        let probe = TriplePattern::new(
            Ref::Var(u),
            Ref::Sid(Sid::new(100, "probePred")),
            Term::Var(x),
        );

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "probePred"),
            PropertyStatData {
                count: 3_140,
                ndv_values: 166,
                ndv_subjects: 3_179,
            },
        );

        let planner = HashJoinPlanner {
            stats: Some(&stats),
            force: HashJoinForce::Auto,
            driving_est: 10_980.0,
            step_est: Some(10_980.0),
        };
        let d = planner
            .explain_object_hash_join(&[VarId(2), x, VarId(1)], &probe, false, true)
            .expect("final probe should produce a hash-join decision");
        assert!(d.chosen);
        assert_eq!(d.reason, HashJoinReason::CostWins);
        assert_eq!(d.probe_count, Some(3_140));
        assert_eq!(d.driving_est.map(|v| v.round() as u64), Some(10_980));
    }

    #[test]
    fn planner_allows_hash_join_for_medium_wide_path_tail() {
        // Long path-join tail: the left intermediate is wide, but still only
        // ~237k rows. Scanning the 24,874-row probe predicate once is cheaper
        // than hundreds of thousands of scattered object seeks.
        let x2 = VarId(1);
        let x1 = VarId(0);
        let probe = TriplePattern::new(
            Ref::Var(x1),
            Ref::Sid(Sid::new(100, "probePred")),
            Term::Var(x2),
        );

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "probePred"),
            PropertyStatData {
                count: 24_874,
                ndv_values: 408,
                ndv_subjects: 15_508,
            },
        );

        let planner = HashJoinPlanner {
            stats: Some(&stats),
            force: HashJoinForce::Auto,
            driving_est: 237_440.0,
            step_est: Some(237_440.0),
        };
        let d = planner
            .explain_object_hash_join(&[VarId(2), VarId(3), x2, VarId(4)], &probe, false, true)
            .expect("path tail should produce a hash-join decision");
        assert!(d.chosen);
        assert_eq!(d.reason, HashJoinReason::CostWins);
        assert_eq!(d.probe_count, Some(24_874));
    }

    #[test]
    fn planner_rejects_hash_join_for_million_row_wide_cycle_tail() {
        let x2 = VarId(1);
        let x1 = VarId(0);
        let probe = TriplePattern::new(Ref::Var(x1), Ref::Sid(Sid::new(658, "P40")), Term::Var(x2));

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(658, "P40"),
            PropertyStatData {
                count: 1_966_039,
                ndv_values: 1_161_505,
                ndv_subjects: 1_205_280,
            },
        );

        let planner = HashJoinPlanner {
            stats: Some(&stats),
            force: HashJoinForce::Auto,
            driving_est: 1_085_090.0,
            step_est: Some(1_085_090.0),
        };
        let d = planner
            .explain_object_hash_join(&[VarId(2), VarId(3), x2, VarId(4)], &probe, false, true)
            .expect("S3 tail should produce a hash-join decision");
        assert!(!d.chosen);
        assert_eq!(d.reason, HashJoinReason::BuildSideTooWide);
    }

    #[test]
    fn cost_gate_still_guards_pathological_and_small_probes() {
        // Huge probe for a handful of driving rows is still rejected (ratio ≫ 1024×).
        assert!(!hash_join_cost_wins(Some(2_848_260), Some(100.0)));
        // Very small probe stays below the small-build exception.
        assert!(!hash_join_cost_wins(Some(500), Some(100.0)));
        // Below-floor probe with a non-small build still falls back to nested loop.
        assert!(!hash_join_cost_wins(Some(40_000), Some(25_000.0)));
        // No probe stats => fall back to the safe nested-loop default.
        assert!(!hash_join_cost_wins(None, Some(1_000_000.0)));
    }
}
