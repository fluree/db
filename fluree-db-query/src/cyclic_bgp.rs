//! Targeted physical operator for small cyclic fixed-predicate BGPs.
//!
//! This is intentionally narrower than a general leapfrog triejoin. It covers
//! the cyclic shapes that otherwise fall through to left-deep nested-loop
//! joins: triangles and 4-edge cycles whose joins are all ref-valued subject/object
//! variables. Unsupported shapes keep using the existing fallback operator tree.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_psot_cursor_for_predicate, cached_overlay_ops, cursor_projection_sid_otype_okey,
    normalize_pred_sid, parallel_map_pooled, subject_probe_lane_plan, ProbeLanePlan, ProbeOps,
    PsotSubjectSeek, RowFate, SharedOverlayOps,
};
use crate::fast_path_outcome::{FastPathFallback, FastPathOutcome};
use crate::index_scope::{overlay_carries_novelty, IndexMember, IndexScope, MemberSet, ScopeKind};
use crate::ir::triple::{Ref, TriplePattern};
use crate::object_binding::{late_materialized_object_binding, materialized_object_binding};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::plan_node::PlanChild;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryGraphView, ColumnProjection};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{GraphId, PropertyStatData, StatsView};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, OnceLock};

const OUTPUT_BATCH_SIZE: usize = 1024;
const DEFAULT_MAX_PREDICATE_ROWS: u64 = 10_000_000;
const DEFAULT_MAX_SQUARE_WEDGE_PAIRS: usize = 5_000_000;
const DEFAULT_MAX_BOUNDED_PROBE_SUBJECTS: usize = 65_536;
const DEFAULT_BOUNDED_PROBE_SCAN_RATIO: u64 = 64;

fn cyclic_bgp_enabled() -> bool {
    !matches!(
        std::env::var("FLUREE_CYCLIC_BGP"),
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("false")
    )
}

fn max_predicate_rows() -> u64 {
    std::env::var("FLUREE_CYCLIC_BGP_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_PREDICATE_ROWS)
}

fn max_square_wedge_pairs() -> usize {
    std::env::var("FLUREE_CYCLIC_BGP_MAX_WEDGE_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_SQUARE_WEDGE_PAIRS)
}

fn max_bounded_probe_subjects() -> usize {
    std::env::var("FLUREE_CYCLIC_BGP_MAX_BOUNDED_SUBJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_BOUNDED_PROBE_SUBJECTS)
}

fn bounded_probe_scan_ratio() -> u64 {
    std::env::var("FLUREE_CYCLIC_BGP_PROBE_SCAN_RATIO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_BOUNDED_PROBE_SCAN_RATIO)
}

/// Probe an edge per-subject instead of scanning its whole relation only when
/// the frontier is small enough AND the estimated scan is at least
/// `bounded_probe_scan_ratio()` rows per probe. A stats-absent estimate means
/// the predicate is likely empty (populated stats omit only empty predicates),
/// so a full scan is already trivially cheap — don't probe.
fn should_probe_edge(frontier_len: usize, estimate: Option<u64>) -> bool {
    frontier_len <= max_bounded_probe_subjects()
        && estimate.is_some_and(|est| {
            (frontier_len as u64).saturating_mul(bounded_probe_scan_ratio()) <= est
        })
}

/// Minimum largest-edge estimate before parallel edge scans are worth the
/// thread-pool overhead.
const PARALLEL_EDGE_SCAN_MIN_ROWS: u64 = 50_000;

/// `ndv_*` stats are HLL estimates (~2% error); require this multiple of the
/// probe subject cap before declaring a frontier bound "provably over cap",
/// so estimation noise near the boundary can't steal a probe win.
const NO_PROBE_NDV_MARGIN: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CyclicBgpShape {
    Triangle,
    Square,
}

impl CyclicBgpShape {
    fn as_str(self) -> &'static str {
        match self {
            CyclicBgpShape::Triangle => "triangle",
            CyclicBgpShape::Square => "square",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CyclicJoinMode {
    RefOnly,
    EncodedObject,
}

impl CyclicJoinMode {
    fn as_str(self) -> &'static str {
        match self {
            CyclicJoinMode::RefOnly => "iri-ref",
            CyclicJoinMode::EncodedObject => "encoded",
        }
    }
}

#[derive(Debug, Clone)]
struct CyclicEdge {
    subject: VarId,
    object: VarId,
    predicate: Ref,
    estimate: Option<u64>,
    /// Distinct subjects / object values from stats (HLL estimates). Used by
    /// the no-probe predictor that gates parallel edge scans.
    ndv_subjects: Option<u64>,
    ndv_values: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct CyclicBgpPlan {
    shape: CyclicBgpShape,
    vars: Arc<[VarId]>,
    edges: Arc<[CyclicEdge]>,
}

impl CyclicBgpPlan {
    pub(crate) fn shape_name(&self) -> &'static str {
        self.shape.as_str()
    }
}

fn property_stats<'a>(stats: &'a StatsView, pred: &Ref) -> Option<&'a PropertyStatData> {
    if let Some(sid) = pred.as_sid() {
        return stats.get_property(sid);
    }
    if let Some(iri) = pred.as_iri() {
        return stats.get_property_by_iri(iri);
    }
    None
}

/// Detect a simple triangle or 4-cycle over fixed-predicate ref joins.
pub(crate) fn analyze_cyclic_bgp(
    triples: &[TriplePattern],
    stats: Option<&StatsView>,
) -> Option<CyclicBgpPlan> {
    if !cyclic_bgp_enabled() || !(triples.len() == 3 || triples.len() == 4) {
        return None;
    }

    let mut vars: Vec<VarId> = Vec::new();
    let mut degree: FxHashMap<VarId, usize> = FxHashMap::default();
    let mut edges = Vec::with_capacity(triples.len());
    let row_cap = max_predicate_rows();

    for tp in triples {
        if tp.dtc.is_some() || tp.p.as_var().is_some() {
            return None;
        }
        let (Some(subject), Some(object)) = (tp.s.as_var(), tp.o.as_var()) else {
            return None;
        };
        if subject == object {
            return None;
        }
        for v in [subject, object] {
            if !vars.contains(&v) {
                vars.push(v);
            }
            *degree.entry(v).or_insert(0) += 1;
        }
        let prop = stats.and_then(|s| property_stats(s, &tp.p));
        let estimate = prop.map(|p| p.count);
        if estimate.is_some_and(|count| count > row_cap) {
            return None;
        }
        edges.push(CyclicEdge {
            subject,
            object,
            predicate: tp.p.clone(),
            estimate,
            ndv_subjects: prop.map(|p| p.ndv_subjects),
            ndv_values: prop.map(|p| p.ndv_values),
        });
    }

    if vars.len() != triples.len() || degree.values().any(|count| *count != 2) {
        return None;
    }

    // Ensure the variable graph is one connected cycle, not two disjoint 2-edge
    // components that happen to have degree two.
    let mut seen = FxHashSet::default();
    let mut stack = vec![vars[0]];
    while let Some(v) = stack.pop() {
        if !seen.insert(v) {
            continue;
        }
        for edge in &edges {
            if edge.subject == v && !seen.contains(&edge.object) {
                stack.push(edge.object);
            } else if edge.object == v && !seen.contains(&edge.subject) {
                stack.push(edge.subject);
            }
        }
    }
    if seen.len() != vars.len() {
        return None;
    }

    Some(CyclicBgpPlan {
        shape: if triples.len() == 3 {
            CyclicBgpShape::Triangle
        } else {
            CyclicBgpShape::Square
        },
        vars: Arc::from(vars.into_boxed_slice()),
        edges: Arc::from(edges.into_boxed_slice()),
    })
}

#[derive(Clone, Copy, Debug)]
struct RawEdgeRow {
    subject: u64,
    o_type: u16,
    object: u64,
    p_id: u32,
}

/// Join identity of an encoded object value, mirroring the term equality the
/// fallback operator tree computes for the same rows: refs compare by s_id,
/// blank nodes by handle, literals by `(o_type, o_key)` with `p_id`
/// participating only for NUM_BIG (per-predicate arena).
///
/// Keying literals on `o_type` matches `Binding` equality, in two layers:
///
/// - For the o_types `late_materialized_object_binding` encodes, `o_type` ↔
///   `(o_kind, dt_id, lang_id)` is a bijection: each arm reads those fields
///   off the o_type alone, and no two o_types share a triple — pinned
///   exhaustively by `encoded_lit_identity_fields_are_injective_over_o_types`.
///   So this partitions rows exactly as `Binding::eq` does on `EncodedLit`.
/// - The string-dictionary datatypes the materializer declines (#1729 —
///   `xsd:anyURI`, `xsd:token`, …, customer datatypes, whose only dict id is
///   per-ledger and doesn't fit `EncodedLit`) still have `(o_type, o_key)` as
///   their full term identity within one store view: `o_key` is the interned
///   lexical form and `o_type` names the datatype. Every relation in one
///   operator is scanned through one store: one graph, or the graphs of one
///   ledger that [`IndexScope::Union`] certifies share it and its dictionary
///   novelty — the lane never opens over several ledgers — so a per-ledger id
///   never meets another ledger's. These rows decode at emit
///   ([`EncObj::to_binding`]) into the same materialized `Binding::Lit` the
///   fallback scan produces, so nothing per-ledger leaves the operator.
///
/// Across graphs the keys stay one term each except for the arena-backed
/// o_types (NumBig, vector), whose `o_key` is a handle into a per-graph,
/// per-predicate arena; the lane declines those over several graphs
/// ([`is_arena_o_type`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum EncIdentity {
    Ref(u64),
    Blank(u64),
    Lit {
        o_type: u16,
        o_key: u64,
        num_big_p_id: u32,
    },
}

/// Copy-able encoded object for the cyclic join data plane. Hash/Eq/Ord use
/// only `id`; `o_type`/`p_id`/`o_key` ride along so emit can rebuild the
/// exact `Binding` via [`late_materialized_object_binding`].
#[derive(Clone, Copy, Debug)]
struct EncObj {
    id: EncIdentity,
    o_type: u16,
    p_id: u32,
    o_key: u64,
}

impl PartialEq for EncObj {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for EncObj {}

impl std::hash::Hash for EncObj {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialOrd for EncObj {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EncObj {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl EncObj {
    fn from_ref(s_id: u64) -> Self {
        Self {
            id: EncIdentity::Ref(s_id),
            o_type: OType::IRI_REF.as_u16(),
            p_id: 0,
            o_key: s_id,
        }
    }

    fn ref_s_id(self) -> Option<u64> {
        match self.id {
            EncIdentity::Ref(s_id) => Some(s_id),
            _ => None,
        }
    }

    /// Encode a raw object column value, or `None` for o_types the join
    /// cannot carry. Identity is `(o_type, o_key)` — see [`EncIdentity`] for
    /// why that matches the term equality the fallback computes. The late
    /// materializer still decides which non-string o_types are join-safe, so
    /// the decline set (booleans, `xsd:int`/`xsd:short`/…, the minor
    /// temporals, geo/spatial) cannot drift from the scan path's.
    fn encode(o_type: u16, o_key: u64, p_id: u32) -> Option<Self> {
        let ot = OType::from_u16(o_type);
        if ot.decode_kind() == DecodeKind::BlankNode {
            // Blank handles compare by o_key; skip the per-row label format!.
            return Some(Self {
                id: EncIdentity::Blank(o_key),
                o_type,
                p_id,
                o_key,
            });
        }
        let id = match late_materialized_object_binding(
            o_type,
            o_key,
            p_id,
            0,
            u32::MAX,
            None,
            fluree_db_core::NO_ARENA_GRAPH,
        ) {
            Some(Binding::EncodedSid { s_id, .. }) => EncIdentity::Ref(s_id),
            Some(Binding::EncodedLit { o_kind, .. }) => EncIdentity::Lit {
                o_type,
                o_key,
                num_big_p_id: if o_kind == ObjKind::NUM_BIG.as_u8() {
                    p_id
                } else {
                    0
                },
            },
            // The materializer only builds other variants for blank nodes,
            // handled above.
            Some(_) => return None,
            // String-dictionary datatypes without a reserved dict id (#1729).
            // They have no `EncodedLit` form, but `(o_type, o_key)` is their
            // full term identity within this store view, so they stay in the
            // join; the rows that survive it decode at emit.
            None if ot.decode_kind() == DecodeKind::StringDict => EncIdentity::Lit {
                o_type,
                o_key,
                num_big_p_id: 0,
            },
            None => return None,
        };
        Some(Self {
            id,
            o_type,
            p_id,
            o_key,
        })
    }

    /// Rebuild the exact `Binding` the scan path would have produced: the
    /// encoded form where one exists, otherwise (non-reserved string-dict
    /// datatypes) the decoded `Binding::Lit` carrying its exact datatype
    /// `Sid`, built by the same `materialized_object_binding` the fallback
    /// scan uses for these rows. The decode runs once per row that
    /// *survives* the join, not per row scanned.
    fn to_binding(self, emit: &Emit) -> Result<Binding> {
        if let Some(binding) = late_materialized_object_binding(
            self.o_type,
            self.o_key,
            self.p_id,
            0,
            u32::MAX,
            None,
            emit.arena_graph,
        ) {
            return Ok(binding);
        }
        let Some(view) = emit.view.as_ref() else {
            return Err(QueryError::Internal(
                "cyclic bgp emit: string-dict object binding requires a graph view".into(),
            ));
        };
        let val = view
            .decode_value(self.o_type, self.o_key, self.p_id)
            .map_err(|e| {
                QueryError::dictionary_lookup(format!(
                    "cyclic bgp emit decode: o_type={}, o_key={}, p_id={}: {e}",
                    self.o_type, self.o_key, self.p_id
                ))
            })?;
        Ok(materialized_object_binding(
            view.store(),
            self.o_type,
            self.p_id,
            val,
            None,
            None,
        ))
    }
}

/// Whether `o_type`'s `o_key` is a handle into a per-graph, per-predicate
/// arena (NumBig, vector; spatial, which no reader decodes yet), so that
/// equal keys in two graphs need not be one value.
fn is_arena_o_type(o_type: u16) -> bool {
    matches!(
        OType::from_u16(o_type).decode_kind(),
        DecodeKind::NumBigArena | DecodeKind::VectorArena | DecodeKind::SpatialArena
    )
}

/// An edge's rows over a default graph of several graphs: its facts, each
/// once. A fact is `(subject, object, list index)` (storage's fact identity,
/// #2109's `FactIdentity`; the predicate is the edge's), which is what the
/// fallback's `BatchDeduper` keeps too: only a fact two members both hold
/// collapses, so two list items with one value stay two rows, which the join
/// takes twice. `list_index[i]` is row `i`'s. This is the one place the
/// union's identity lives.
///
/// Each member's rows arrive grouped by subject, and a full scan of a ref
/// predicate arrives sorted, so the stable sort's run detection merges the
/// members' runs rather than sorting from scratch.
fn set_union<O: Ord + Copy>(rows: &mut Vec<(u64, O)>, list_index: &[u32]) {
    debug_assert_eq!(rows.len(), list_index.len());
    let mut facts: Vec<(u64, O, u32)> = rows
        .iter()
        .zip(list_index)
        .map(|(&(s, o), &i)| (s, o, i))
        .collect();
    facts.sort();
    facts.dedup();
    rows.clear();
    rows.extend(facts.into_iter().map(|(s, o, _)| (s, o)));
}

/// The cursor columns a full scan reads: subject and object, and with
/// `list_index` the list index too (a union's fact identity, [`set_union`]).
fn scan_projection(list_index: bool) -> ColumnProjection {
    let mut projection = cursor_projection_sid_otype_okey();
    if list_index {
        projection.output.insert(ColumnId::OI);
    }
    projection
}

/// One graph a relation load reads: the member, and the context its cursors
/// run under, which carries that graph's novelty and `t` (for a member of a
/// default graph of several graphs, the context `DatasetOperator` gives it).
/// `probe_ops` holds the member's resolved novelty per edge once probes are
/// planned ([`CyclicBgpOperator::plan_probes`]).
struct MemberRead<'c, 'a> {
    ctx: &'c ExecutionContext<'a>,
    member: IndexMember<'c>,
    probe_ops: Vec<Option<SharedOverlayOps>>,
}

impl<'c, 'a> MemberRead<'c, 'a> {
    fn new(ctx: &'c ExecutionContext<'a>, member: IndexMember<'c>) -> Self {
        Self {
            ctx,
            member,
            probe_ops: Vec::new(),
        }
    }
}

/// How the lane's rows leave it: encoded, as a scan of its scope emits them,
/// over one graph or several (a union's term view decodes them later).
struct Emit {
    /// Decodes the string-dictionary objects that have no encoded form
    /// (EncodedObject mode only). Strings are ledger-wide, so any member's
    /// view serves a union.
    view: Option<BinaryGraphView>,
    /// The graph an arena handle (a big number, a vector) is numbered in:
    /// the one graph's, and none over several graphs, which decline arena
    /// objects while loading; one that reached emit would fail to decode
    /// rather than read another member's arena.
    arena_graph: GraphId,
}

impl Emit {
    fn none() -> Self {
        Self {
            view: None,
            arena_graph: fluree_db_core::NO_ARENA_GRAPH,
        }
    }
}

/// Relation index over subject-grouped `(subject, object)` edge rows.
///
/// CSR-style layout: `rows` holds the pairs grouped by subject (scan order),
/// `by_subject` maps each subject to its row range, and `by_object` maps each
/// object value to a range of `obj_order`, which lists row indices sorted by
/// `(object, subject)`. Compared to map-of-Vec indexes this allocates a few
/// large buffers instead of one Vec per distinct key, stores Copy values
/// instead of cloned `Binding`s, and replaces the `(s, o)` pair hash set with
/// a binary search over the object range.
struct RelationIdx<O> {
    edge: CyclicEdge,
    rows: Vec<(u64, O)>,
    by_subject: FxHashMap<u64, (u32, u32)>,
    obj_order: Vec<u32>,
    by_object: FxHashMap<O, (u32, u32)>,
}

type RefRelationIndex = RelationIdx<u64>;
type RelationIndex = RelationIdx<EncObj>;

impl<O: Copy + Eq + std::hash::Hash + Ord> RelationIdx<O> {
    fn new(edge: CyclicEdge, mut rows: Vec<(u64, O)>) -> Self {
        debug_assert!(rows.len() < u32::MAX as usize);
        // Scans and prune passes produce subject-grouped rows; normalize any
        // other source (unit tests) with a stable sort that keeps each
        // subject's object order intact.
        if !rows.is_sorted_by_key(|row| row.0) {
            rows.sort_by_key(|row| row.0);
        }

        let mut by_subject: FxHashMap<u64, (u32, u32)> = FxHashMap::default();
        let mut start = 0usize;
        while start < rows.len() {
            let subject = rows[start].0;
            let mut end = start + 1;
            while end < rows.len() && rows[end].0 == subject {
                end += 1;
            }
            by_subject.insert(subject, (start as u32, end as u32));
            start = end;
        }

        let mut obj_order: Vec<u32> = (0..rows.len() as u32).collect();
        obj_order.sort_unstable_by_key(|&i| {
            let row = &rows[i as usize];
            (row.1, row.0)
        });
        let mut by_object: FxHashMap<O, (u32, u32)> = FxHashMap::default();
        let mut start = 0usize;
        while start < obj_order.len() {
            let object = rows[obj_order[start] as usize].1;
            let mut end = start + 1;
            while end < obj_order.len() && rows[obj_order[end] as usize].1 == object {
                end += 1;
            }
            by_object.insert(object, (start as u32, end as u32));
            start = end;
        }

        Self {
            edge,
            rows,
            by_subject,
            obj_order,
            by_object,
        }
    }

    /// All `(subject, object)` rows for `s` (empty if absent).
    fn subject_rows(&self, s: u64) -> &[(u64, O)] {
        self.by_subject
            .get(&s)
            .map(|&(lo, hi)| &self.rows[lo as usize..hi as usize])
            .unwrap_or(&[])
    }

    /// Subjects of every row whose object equals `o` (with multiplicity).
    fn object_subjects(&self, o: &O) -> impl Iterator<Item = u64> + '_ {
        let (lo, hi) = self.by_object.get(o).copied().unwrap_or((0, 0));
        self.obj_order[lo as usize..hi as usize]
            .iter()
            .map(|&i| self.rows[i as usize].0)
    }

    fn object_degree(&self, o: &O) -> usize {
        self.by_object
            .get(o)
            .map(|&(lo, hi)| (hi - lo) as usize)
            .unwrap_or(0)
    }

    /// How many rows are exactly `(s, o)`: a pair stored twice (two list
    /// items with the same value) is two rows, and the join takes it twice,
    /// as the subject and object scans of the other edges do. The object
    /// range is sorted by subject, so this is a hash lookup plus a binary
    /// search, and a step per repeat.
    fn pair_count(&self, s: u64, o: &O) -> usize {
        let Some(&(lo, hi)) = self.by_object.get(o) else {
            return 0;
        };
        let range = &self.obj_order[lo as usize..hi as usize];
        let subject = |pos: usize| self.rows[range[pos] as usize].0;
        // A miss costs the one search a membership test costs; a hit then
        // steps outward over the (rare) repeats.
        let Ok(hit) = range.binary_search_by_key(&s, |&i| self.rows[i as usize].0) else {
            return 0;
        };
        let before = (0..hit).rev().take_while(|&pos| subject(pos) == s).count();
        let after = (hit + 1..range.len())
            .take_while(|&pos| subject(pos) == s)
            .count();
        1 + before + after
    }

    fn distinct_subjects(&self) -> usize {
        self.by_subject.len()
    }

    fn distinct_objects(&self) -> usize {
        self.by_object.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WedgePairKey(EncObj, EncObj);

#[derive(Debug, Clone)]
struct EncodedSquareWedgePlan {
    build_center: VarId,
    probe_center: VarId,
    key_a: VarId,
    key_b: VarId,
    build_edge_a: usize,
    build_edge_b: usize,
    probe_edge_a: usize,
    probe_edge_b: usize,
    build_pairs: usize,
    probe_pairs: usize,
}

struct EncodedProbeWedgeCursor {
    center: EncObj,
    values_a: Vec<EncObj>,
    values_b: Vec<EncObj>,
    a_pos: usize,
    b_pos: usize,
    matches: Vec<EncObj>,
    match_pos: usize,
}

struct EncodedSquareWedgeState {
    plan: EncodedSquareWedgePlan,
    table: FxHashMap<WedgePairKey, Vec<EncObj>>,
    probe_centers: Vec<EncObj>,
    probe_center_pos: usize,
    current: Option<EncodedProbeWedgeCursor>,
}

/// Routing stamp of the cyclic-join lane over one graph, recorded at open:
/// `proceed` when the lane answers, a fallback when the generic join plan does.
pub(crate) const CYCLIC_BGP_SITE: &str = "cyclic_bgp";

/// Routing stamp of the cyclic-join lane over a default graph of several
/// graphs of one ledger, or of none ([`IndexScope::Union`],
/// [`IndexScope::Empty`]), recorded at open. Its fallbacks say why.
pub(crate) const CYCLIC_BGP_UNION_SITE: &str = "cyclic_bgp_union";

pub(crate) struct CyclicBgpOperator {
    plan: CyclicBgpPlan,
    join_mode: CyclicJoinMode,
    schema: Arc<[VarId]>,
    schema_positions: Arc<[usize]>,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    ref_relations: Vec<RefRelationIndex>,
    relations: Vec<RelationIndex>,
    driver_idx: usize,
    driver_pos: usize,
    ref_pending: VecDeque<Vec<u64>>,
    pending: VecDeque<Vec<EncObj>>,
    square_wedge: Option<EncodedSquareWedgeState>,
    probed_edges: usize,
    used_fast_path: bool,
    raw_relation_rows: usize,
    pruned_relation_rows: usize,
    /// How many graphs the lane reads: 1, or a union's members; 0 before open
    /// and over a default graph of none.
    graphs: usize,
    /// How rows leave the lane. Set when the fast path opens; no view in
    /// RefOnly mode and on the fallback path.
    emit: Emit,
    /// The first reason the lane stood down, stamped on the union site.
    decline: OnceLock<FastPathFallback>,
}

impl CyclicBgpOperator {
    fn log_fast_path_bail(&self, reason: &'static str, edge: Option<&CyclicEdge>) {
        self.decline_with(FastPathFallback::GateDeclined, reason, edge);
    }

    /// Stand the lane down for `fallback`, logging `reason`.
    fn decline_with(
        &self,
        fallback: FastPathFallback,
        reason: &'static str,
        edge: Option<&CyclicEdge>,
    ) {
        let _ = self.decline.set(fallback);
        tracing::debug!(
            reason,
            shape = self.plan.shape_name(),
            mode = self.join_mode.as_str(),
            predicate = edge.map(|e| predicate_display(&e.predicate)),
            "cyclic bgp fast path bail"
        );
    }

    pub(crate) fn new(
        plan: CyclicBgpPlan,
        required_where_vars: Option<&[VarId]>,
        fallback: BoxedOperator,
    ) -> Self {
        let required: Option<HashSet<VarId>> =
            required_where_vars.map(|vars| vars.iter().copied().collect());
        let mut schema = Vec::new();
        let mut schema_positions = Vec::new();
        for (idx, var) in plan.vars.iter().copied().enumerate() {
            if required.as_ref().is_none_or(|r| r.contains(&var)) {
                schema.push(var);
                schema_positions.push(idx);
            }
        }
        let join_mode = if plan.edges.iter().any(|edge| {
            !plan
                .edges
                .iter()
                .any(|candidate| candidate.subject == edge.object)
        }) {
            CyclicJoinMode::EncodedObject
        } else {
            CyclicJoinMode::RefOnly
        };
        Self {
            plan,
            join_mode,
            schema: Arc::from(schema.into_boxed_slice()),
            schema_positions: Arc::from(schema_positions.into_boxed_slice()),
            state: OperatorState::Created,
            fallback: Some(fallback),
            ref_relations: Vec::new(),
            relations: Vec::new(),
            driver_idx: 0,
            driver_pos: 0,
            ref_pending: VecDeque::new(),
            pending: VecDeque::new(),
            square_wedge: None,
            probed_edges: 0,
            used_fast_path: false,
            raw_relation_rows: 0,
            pruned_relation_rows: 0,
            graphs: 0,
            emit: Emit::none(),
            decline: OnceLock::new(),
        }
    }

    fn var_pos(&self, v: VarId) -> usize {
        self.plan
            .vars
            .iter()
            .position(|candidate| *candidate == v)
            .expect("cyclic plan edge var must be in plan vars")
    }

    fn var_is_subject(&self, v: VarId) -> bool {
        self.plan.edges.iter().any(|edge| edge.subject == v)
    }

    fn encode_object_for_edge(&self, edge: &CyclicEdge, raw: RawEdgeRow) -> Option<EncObj> {
        if self.var_is_subject(edge.object) {
            if raw.o_type != OType::IRI_REF.as_u16() {
                return None;
            }
            Some(EncObj::from_ref(raw.object))
        } else {
            EncObj::encode(raw.o_type, raw.object, raw.p_id)
        }
    }

    /// The predicate's id in the member's store: `Ok(None)` for no rows (a
    /// predicate the store and the member's novelty both lack), `Err(())`
    /// when the lane must stand down (a predicate only novelty holds, which
    /// has no base rows to scan).
    fn member_p_id(
        &self,
        read: &MemberRead<'_, '_>,
        edge: &CyclicEdge,
    ) -> Result<std::result::Result<Option<(fluree_db_core::Sid, u32)>, ()>> {
        let store = read.member.store();
        let pred_sid = normalize_pred_sid(store, &edge.predicate)?;
        Ok(match store.sid_to_p_id(&pred_sid) {
            Some(p_id) => Ok(Some((pred_sid, p_id))),
            None if overlay_carries_novelty(read.member.overlay()) => {
                self.decline_with(
                    FastPathFallback::Novelty,
                    "novelty-only-predicate",
                    Some(edge),
                );
                Err(())
            }
            None => Ok(None),
        })
    }

    /// Whether `read` is a member of a default graph of several graphs that
    /// holds nothing for the predicate `pred_sid`, though the index has no
    /// PSOT branch for its graph. With no novelty it holds nothing at all.
    /// With novelty (which belongs to the ledger, not the graph), it holds
    /// nothing when its graph's novelty has nothing for the predicate. Then
    /// the member contributes no edges, where a missing branch otherwise
    /// declines the lane: rows could live only in novelty. A union default
    /// graph whose own graph is empty is one. One graph keeps declining, as
    /// before.
    fn member_holds_nothing(
        &self,
        read: &MemberRead<'_, '_>,
        pred_sid: &fluree_db_core::Sid,
    ) -> Result<bool> {
        let (store, g_id) = (read.member.store(), read.member.g_id());
        if self.graphs <= 1 || store.branch_for_order(g_id, RunSortOrder::Psot).is_some() {
            return Ok(false);
        }
        if !overlay_carries_novelty(read.member.overlay()) {
            return Ok(true);
        }
        Ok(
            cached_overlay_ops(read.ctx, store, g_id, RunSortOrder::Psot, pred_sid)?
                .is_some_and(|ops| ops.is_empty()),
        )
    }

    /// The edge's full relation in `read`'s graph. With `list_index`, each
    /// row's list index is pushed there too (a union's fact identity).
    fn scan_ref_relation(
        &self,
        read: &MemberRead<'_, '_>,
        edge: &CyclicEdge,
        mut list_index: Option<&mut Vec<u32>>,
    ) -> Result<Option<Vec<(u64, u64)>>> {
        let span = tracing::debug_span!(
            "cyclic_scan_relation",
            predicate = %predicate_display(&edge.predicate),
            mode = "full",
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let store = read.member.store();
        let (pred_sid, p_id) = match self.member_p_id(read, edge)? {
            Ok(Some(pred)) => pred,
            Ok(None) => return Ok(Some(Vec::new())),
            Err(()) => return Ok(None),
        };
        let mut cursor = match build_psot_cursor_for_predicate(
            read.ctx,
            store,
            read.member.g_id(),
            pred_sid.clone(),
            p_id,
            scan_projection(list_index.is_some()),
        )? {
            Some(cursor) => cursor,
            None => {
                if self.member_holds_nothing(read, &pred_sid)? {
                    return Ok(Some(Vec::new()));
                }
                self.log_fast_path_bail("cursor-unavailable", Some(edge));
                return Ok(None);
            }
        };

        let row_cap = max_predicate_rows();
        let mut rows: Vec<(u64, u64)> = Vec::new();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cyclic bgp ref cursor: {e}")))?
        {
            for row in 0..batch.row_count {
                if batch.o_type.get(row) != OType::IRI_REF.as_u16() {
                    self.log_fast_path_bail("non-ref-object-in-ref-mode", Some(edge));
                    return Ok(None);
                }
                rows.push((batch.s_id.get(row), batch.o_key.get(row)));
                if let Some(list_index) = list_index.as_mut() {
                    list_index.push(batch.o_i.get_or(row, u32::MAX));
                }
                if rows.len() as u64 > row_cap {
                    self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                    return Ok(None);
                }
            }
        }
        span.record("rows", rows.len() as u64);
        Ok(Some(rows))
    }

    /// [`Self::scan_ref_relation`] for encoded objects.
    fn scan_relation(
        &self,
        read: &MemberRead<'_, '_>,
        edge: &CyclicEdge,
        mut list_index: Option<&mut Vec<u32>>,
    ) -> Result<Option<Vec<(u64, EncObj)>>> {
        let span = tracing::debug_span!(
            "cyclic_scan_relation",
            predicate = %predicate_display(&edge.predicate),
            mode = "full",
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let store = read.member.store();
        let (pred_sid, p_id) = match self.member_p_id(read, edge)? {
            Ok(Some(pred)) => pred,
            Ok(None) => return Ok(Some(Vec::new())),
            Err(()) => return Ok(None),
        };
        let mut cursor = match build_psot_cursor_for_predicate(
            read.ctx,
            store,
            read.member.g_id(),
            pred_sid.clone(),
            p_id,
            scan_projection(list_index.is_some()),
        )? {
            Some(cursor) => cursor,
            None => {
                if self.member_holds_nothing(read, &pred_sid)? {
                    return Ok(Some(Vec::new()));
                }
                self.log_fast_path_bail("cursor-unavailable", Some(edge));
                return Ok(None);
            }
        };

        let row_cap = max_predicate_rows();
        let mut raw_rows = Vec::new();
        while let Some(batch) = cursor
            .next_batch()
            .map_err(|e| QueryError::Internal(format!("cyclic bgp cursor: {e}")))?
        {
            for row in 0..batch.row_count {
                raw_rows.push(RawEdgeRow {
                    subject: batch.s_id.get(row),
                    o_type: batch.o_type.get(row),
                    object: batch.o_key.get(row),
                    p_id,
                });
                if let Some(list_index) = list_index.as_mut() {
                    list_index.push(batch.o_i.get_or(row, u32::MAX));
                }
                if raw_rows.len() as u64 > row_cap {
                    self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                    return Ok(None);
                }
            }
        }
        self.encode_raw_rows(edge, raw_rows, &span)
    }

    /// Encode a relation's raw rows for the join, or `None` (logged) for a
    /// row it cannot carry: an object the edge's mode cannot bind, or, over
    /// several graphs, an arena-backed object, whose key names a value only
    /// in its own graph.
    fn encode_raw_rows(
        &self,
        edge: &CyclicEdge,
        raw_rows: Vec<RawEdgeRow>,
        span: &tracing::Span,
    ) -> Result<Option<Vec<(u64, EncObj)>>> {
        let mut rows = Vec::with_capacity(raw_rows.len());
        for raw in raw_rows {
            if self.graphs > 1 && is_arena_o_type(raw.o_type) {
                self.decline_with(
                    FastPathFallback::ArenaType,
                    "arena-object-over-several-graphs",
                    Some(edge),
                );
                return Ok(None);
            }
            let Some(object) = self.encode_object_for_edge(edge, raw) else {
                self.log_fast_path_bail("unsupported-object-binding", Some(edge));
                return Ok(None);
            };
            rows.push((raw.subject, object));
        }
        span.record("rows", rows.len() as u64);
        Ok(Some(rows))
    }

    /// The seek reads the BASE PSOT index at its latest state; callers gate
    /// via `compute_probe_overlay_state` (overlay-free HEAD, or `to_t >=
    /// max_t` with the edge's resolved ops in `overlay_ops`, merged here per
    /// probed subject).
    fn scan_relation_for_subjects(
        &self,
        read: &MemberRead<'_, '_>,
        edge: &CyclicEdge,
        subjects: &FxHashSet<u64>,
        overlay_ops: Option<&SharedOverlayOps>,
        mut list_index: Option<&mut Vec<u32>>,
    ) -> Result<Option<Vec<(u64, EncObj)>>> {
        let span = tracing::debug_span!(
            "cyclic_scan_relation",
            predicate = %predicate_display(&edge.predicate),
            mode = "probed",
            subjects = subjects.len(),
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let store = read.member.store();
        let (pred_sid, p_id) = match self.member_p_id(read, edge)? {
            Ok(Some(pred)) => pred,
            Ok(None) => return Ok(Some(Vec::new())),
            Err(()) => return Ok(None),
        };

        let mut ordered_subjects: Vec<u64> = subjects.iter().copied().collect();
        ordered_subjects.sort_unstable();

        // One amortized monotonic pass over the predicate's PSOT leaves instead
        // of a fresh cursor per subject. None means the PSOT branch is
        // unavailable, never "no rows" — decline rather than dropping rows.
        let Some(mut seek) = PsotSubjectSeek::new_with_identity(store, read.member.g_id(), p_id)
        else {
            if self.member_holds_nothing(read, &pred_sid)? {
                return Ok(Some(Vec::new()));
            }
            self.log_fast_path_bail("cursor-unavailable", Some(edge));
            return Ok(None);
        };
        let mut probe = overlay_ops.and_then(|ops| ProbeOps::new(ops.clone()));
        let row_cap = max_predicate_rows();
        let mut raw_rows: Vec<RawEdgeRow> = Vec::new();
        for subject in ordered_subjects {
            let probe_window = probe
                .as_ref()
                .map(|p| p.subject_window(subject))
                .filter(|w| !w.is_empty());
            seek.rows_for_subject_identity(subject, |o_type, o_key, o_i| {
                if let (Some(p), Some(win)) = (probe.as_mut(), &probe_window) {
                    if p.base_row_fate(win, p_id, o_type, o_key, o_i) == RowFate::Drop {
                        return;
                    }
                }
                raw_rows.push(RawEdgeRow {
                    subject,
                    o_type,
                    object: o_key,
                    p_id,
                });
                if let Some(list_index) = list_index.as_mut() {
                    list_index.push(o_i);
                }
            })?;
            if let Some(p) = probe.as_mut() {
                p.drain_asserts_for_subject(subject, |op| {
                    raw_rows.push(RawEdgeRow {
                        subject,
                        o_type: op.o_type,
                        object: op.o_key,
                        p_id,
                    });
                    if let Some(list_index) = list_index.as_mut() {
                        list_index.push(op.o_i);
                    }
                    Ok(())
                })?;
            }
            if raw_rows.len() as u64 > row_cap {
                self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                return Ok(None);
            }
        }

        self.encode_raw_rows(edge, raw_rows, &span)
    }

    /// O(1) upper bound on the frontier size for `var`: the smallest distinct
    /// count among scanned relations incident to it. `None` when no scanned
    /// relation is incident. Lets the probe loop reject over-cap candidates
    /// without materializing any value set.
    fn frontier_size_bound(relations: &[RelationIndex], var: VarId) -> Option<usize> {
        relations
            .iter()
            .filter_map(|rel| {
                if rel.edge.subject == var {
                    Some(rel.distinct_subjects())
                } else if rel.edge.object == var {
                    Some(rel.distinct_objects())
                } else {
                    None
                }
            })
            .min()
    }

    /// Intersected subject-id frontier for `var` across every already-scanned
    /// relation incident to it. Each new scan tightens the frontier, so probe
    /// candidates are re-derived per cascade level. Only the smallest incident
    /// value set is materialized; the rest intersect via O(1) lookups.
    /// Non-sid object bindings are filtered rather than fatal: a subject var
    /// only ever joins through `Binding::encoded_sid`, so rows binding it to a
    /// literal can never complete an assignment. Returns `None` when no
    /// scanned relation is incident to `var`.
    fn frontier_for_var(&self, relations: &[RelationIndex], var: VarId) -> Option<FxHashSet<u64>> {
        let incident: Vec<&RelationIndex> = relations
            .iter()
            .filter(|rel| rel.edge.subject == var || rel.edge.object == var)
            .collect();
        let seed_pos = incident
            .iter()
            .enumerate()
            .min_by_key(|(_, rel)| {
                if rel.edge.subject == var {
                    rel.distinct_subjects()
                } else {
                    rel.distinct_objects()
                }
            })?
            .0;

        let seed_rel = incident[seed_pos];
        let mut frontier: FxHashSet<u64> = if seed_rel.edge.subject == var {
            seed_rel.by_subject.keys().copied().collect()
        } else {
            seed_rel
                .by_object
                .keys()
                .filter_map(|o| o.ref_s_id())
                .collect()
        };
        for (pos, rel) in incident.iter().enumerate() {
            if pos == seed_pos {
                continue;
            }
            if rel.edge.subject == var {
                frontier.retain(|s| rel.by_subject.contains_key(s));
            } else {
                frontier.retain(|s| rel.by_object.contains_key(&EncObj::from_ref(*s)));
            }
        }
        Some(frontier)
    }

    fn ref_frontier_size_bound(relations: &[RefRelationIndex], var: VarId) -> Option<usize> {
        relations
            .iter()
            .filter_map(|rel| {
                if rel.edge.subject == var {
                    Some(rel.distinct_subjects())
                } else if rel.edge.object == var {
                    Some(rel.distinct_objects())
                } else {
                    None
                }
            })
            .min()
    }

    fn ref_frontier_for_var(relations: &[RefRelationIndex], var: VarId) -> Option<FxHashSet<u64>> {
        let incident: Vec<&RefRelationIndex> = relations
            .iter()
            .filter(|rel| rel.edge.subject == var || rel.edge.object == var)
            .collect();
        let seed_pos = incident
            .iter()
            .enumerate()
            .min_by_key(|(_, rel)| {
                if rel.edge.subject == var {
                    rel.distinct_subjects()
                } else {
                    rel.distinct_objects()
                }
            })?
            .0;

        let seed_rel = incident[seed_pos];
        let mut frontier: FxHashSet<u64> = if seed_rel.edge.subject == var {
            seed_rel.by_subject.keys().copied().collect()
        } else {
            seed_rel.by_object.keys().copied().collect()
        };
        for (pos, rel) in incident.iter().enumerate() {
            if pos == seed_pos {
                continue;
            }
            if rel.edge.subject == var {
                frontier.retain(|s| rel.by_subject.contains_key(s));
            } else {
                frontier.retain(|s| rel.by_object.contains_key(s));
            }
        }
        Some(frontier)
    }

    /// The seek reads the BASE PSOT index at its latest state; callers gate
    /// via `compute_probe_overlay_state` (overlay-free HEAD, or `to_t >=
    /// max_t` with the edge's resolved ops in `overlay_ops`, merged here per
    /// probed subject).
    fn scan_ref_relation_for_subjects(
        &self,
        read: &MemberRead<'_, '_>,
        edge: &CyclicEdge,
        subjects: &FxHashSet<u64>,
        overlay_ops: Option<&SharedOverlayOps>,
        mut list_index: Option<&mut Vec<u32>>,
    ) -> Result<Option<Vec<(u64, u64)>>> {
        let span = tracing::debug_span!(
            "cyclic_scan_relation",
            predicate = %predicate_display(&edge.predicate),
            mode = "probed",
            subjects = subjects.len(),
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let store = read.member.store();
        let (pred_sid, p_id) = match self.member_p_id(read, edge)? {
            Ok(Some(pred)) => pred,
            Ok(None) => return Ok(Some(Vec::new())),
            Err(()) => return Ok(None),
        };

        let mut ordered_subjects: Vec<u64> = subjects.iter().copied().collect();
        ordered_subjects.sort_unstable();

        // One amortized monotonic pass over the predicate's PSOT leaves instead
        // of a fresh cursor per subject. None means the PSOT branch is
        // unavailable, never "no rows" — decline rather than dropping rows.
        let Some(mut seek) = PsotSubjectSeek::new_with_identity(store, read.member.g_id(), p_id)
        else {
            if self.member_holds_nothing(read, &pred_sid)? {
                return Ok(Some(Vec::new()));
            }
            self.log_fast_path_bail("cursor-unavailable", Some(edge));
            return Ok(None);
        };
        let mut probe = overlay_ops.and_then(|ops| ProbeOps::new(ops.clone()));
        let row_cap = max_predicate_rows();
        let mut rows: Vec<(u64, u64)> = Vec::new();
        let mut non_ref_object = false;
        for subject in ordered_subjects {
            let probe_window = probe
                .as_ref()
                .map(|p| p.subject_window(subject))
                .filter(|w| !w.is_empty());
            seek.rows_for_subject_identity(subject, |o_type, o_key, o_i| {
                if let (Some(p), Some(win)) = (probe.as_mut(), &probe_window) {
                    if p.base_row_fate(win, p_id, o_type, o_key, o_i) == RowFate::Drop {
                        return;
                    }
                }
                if o_type != OType::IRI_REF.as_u16() {
                    non_ref_object = true;
                } else {
                    rows.push((subject, o_key));
                    if let Some(list_index) = list_index.as_mut() {
                        list_index.push(o_i);
                    }
                }
            })?;
            if let Some(p) = probe.as_mut() {
                // Injected asserts respect the mode split exactly like base
                // rows: a non-ref novelty object disqualifies ref mode.
                p.drain_asserts_for_subject(subject, |op| {
                    if op.o_type != OType::IRI_REF.as_u16() {
                        non_ref_object = true;
                    } else {
                        rows.push((subject, op.o_key));
                        if let Some(list_index) = list_index.as_mut() {
                            list_index.push(op.o_i);
                        }
                    }
                    Ok(())
                })?;
            }
            if non_ref_object {
                self.log_fast_path_bail("non-ref-object-in-ref-mode", Some(edge));
                return Ok(None);
            }
            if rows.len() as u64 > row_cap {
                self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
                return Ok(None);
            }
        }
        span.record("rows", rows.len() as u64);
        Ok(Some(rows))
    }

    /// Open the lane over the scope `ctx` reads, or decline: the site to
    /// stamp and its outcome.
    ///
    /// - [`IndexScope::One`]: the one-graph lane, its site and behavior
    ///   unchanged.
    /// - [`IndexScope::Union`]: the union variant ([`Self::open_union`]).
    /// - [`IndexScope::Empty`]: the union variant answers nothing, reading no
    ///   index.
    ///
    /// The union variant sits behind the fast-path kill switch, so the
    /// general plan stays a differential oracle for it; the one-graph lane
    /// does not, as before.
    fn open_fast_path(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<(&'static str, FastPathOutcome)> {
        let scope = ctx.index_scope();
        if let Some(IndexScope::One(member)) = scope {
            self.graphs = 1;
            self.emit.arena_graph = member.g_id();
            let mut reads = [MemberRead::new(ctx, member)];
            let fast = self.open_reads(ctx, &mut reads)?;
            return Ok((
                CYCLIC_BGP_SITE,
                proceed_if(fast, FastPathFallback::GateDeclined),
            ));
        }
        if !matches!(ctx.scope_kind(), ScopeKind::Union | ScopeKind::Empty) {
            self.log_fast_path_bail("context-unsupported", None);
            return Ok((
                CYCLIC_BGP_SITE,
                FastPathOutcome::Fallback(FastPathFallback::GateDeclined),
            ));
        }
        if crate::execute::fast_paths_disabled() {
            return Ok((
                CYCLIC_BGP_UNION_SITE,
                FastPathOutcome::Fallback(FastPathFallback::KillSwitch),
            ));
        }
        let outcome = match scope {
            Some(IndexScope::Union(set)) => self.open_union(ctx, &set)?,
            Some(IndexScope::Empty) => {
                // A default graph of none holds no edges.
                self.used_fast_path = true;
                FastPathOutcome::Proceed
            }
            _ => {
                self.log_fast_path_bail("context-unsupported", None);
                // No index scope for the default graph of several graphs: a
                // policy that filters, a history range, no store, or members
                // that do not share this store and its dictionary novelty.
                let shared_store_only =
                    ctx.binary_store.is_some() && !ctx.is_history_range() && ctx.allow_unfiltered();
                FastPathOutcome::Fallback(if shared_store_only {
                    FastPathFallback::StoreMismatch
                } else {
                    FastPathFallback::GateDeclined
                })
            }
        };
        Ok((CYCLIC_BGP_UNION_SITE, outcome))
    }

    /// The union variant over a default graph of several graphs of one
    /// ledger: every edge's relation is read from each member, under the
    /// context `DatasetOperator` gives that member (its graph, novelty and
    /// `t`), and kept as a set ([`set_union`]), which is what the general
    /// plan's per-pattern `BatchDeduper` keeps. A triangle over set-valued
    /// relations is itself a set: each solution fixes the one row each edge
    /// contributes.
    fn open_union(
        &mut self,
        ctx: &ExecutionContext<'_>,
        set: &MemberSet<'_>,
    ) -> Result<FastPathOutcome> {
        let store_mismatch = |op: &Self, reason| {
            op.decline_with(FastPathFallback::StoreMismatch, reason, None);
            Ok(FastPathOutcome::Fallback(FastPathFallback::StoreMismatch))
        };
        let ActiveGraphs::Many(graphs) = ctx.active_graphs() else {
            return store_mismatch(self, "union-without-member-graphs");
        };
        if graphs.len() != set.members().len() {
            return store_mismatch(self, "union-member-count");
        }
        let member_ctxs: Vec<ExecutionContext<'_>> =
            graphs.iter().map(|g| ctx.with_graph_ref(g)).collect();
        let mut reads = Vec::with_capacity(member_ctxs.len());
        for (member_ctx, certified) in member_ctxs.iter().zip(set.members()) {
            // Each member reads one graph through the union's store, as the
            // scope certified it.
            match member_ctx.index_scope() {
                Some(IndexScope::One(member))
                    if Arc::ptr_eq(member.store(), set.store())
                        && member.g_id() == certified.g_id()
                        && member.to_t() == certified.to_t() =>
                {
                    reads.push(MemberRead::new(member_ctx, member));
                }
                _ => return store_mismatch(self, "union-member-outside-its-store"),
            }
        }
        // Novelty is translated per member. The ids translation mints for
        // datatypes only novelty holds come from the runtime dictionaries, so
        // the members' encoded objects compare only through one set of them.
        let novelty = reads
            .iter()
            .any(|read| overlay_carries_novelty(read.member.overlay()));
        let dicts = |read: &MemberRead<'_, '_>| {
            read.ctx
                .runtime_small_dicts
                .map(|d| d as *const fluree_db_core::RuntimeSmallDicts)
        };
        if novelty && !reads.windows(2).all(|w| dicts(&w[0]) == dicts(&w[1])) {
            return store_mismatch(self, "union-members-runtime-dicts");
        }

        self.graphs = reads.len();
        // The rows leave encoded, as `DatasetOperator`'s members emit them:
        // the union's term view decodes them downstream.
        self.emit.arena_graph = fluree_db_core::NO_ARENA_GRAPH;
        let fast = self.open_reads(ctx, &mut reads)?;
        let reason = self
            .decline
            .get()
            .copied()
            .unwrap_or(FastPathFallback::GateDeclined);
        Ok(proceed_if(fast, reason))
    }

    /// Load the relations over `reads` and plan the join, in the plan's mode.
    fn open_reads(
        &mut self,
        ctx: &ExecutionContext<'_>,
        reads: &mut [MemberRead<'_, '_>],
    ) -> Result<bool> {
        match self.join_mode {
            CyclicJoinMode::RefOnly => self.open_ref_fast_path(ctx, reads),
            CyclicJoinMode::EncodedObject => {
                // Emit-side decoder for string-dictionary objects: same view
                // construction as `ExecutionContext::one_graph_view`, against
                // the first graph the relation scans below read (strings are
                // ledger-wide).
                let member = &reads[0].member;
                self.emit.view = Some(
                    member
                        .store()
                        .graph_with_novelty(member.g_id(), ctx.dict_novelty.clone())
                        .with_namespace_codes_fallback(ctx.namespace_codes_fallback.clone())
                        .with_tracker(ctx.tracker.clone()),
                );
                self.open_encoded_fast_path(ctx, reads)
            }
        }
    }

    /// The relation of `edge` over `reads`, each read by `scan`: one graph's
    /// rows as its scan gives them, several graphs' as a set. `None` when a
    /// scan declines, or when the set passes the row cap.
    fn load_relation<O: Ord + Copy>(
        &self,
        reads: &[MemberRead<'_, '_>],
        edge: &CyclicEdge,
        mut scan: impl FnMut(
            &MemberRead<'_, '_>,
            Option<&mut Vec<u32>>,
        ) -> Result<Option<Vec<(u64, O)>>>,
    ) -> Result<Option<Vec<(u64, O)>>> {
        if let [read] = reads {
            return scan(read, None);
        }
        let mut rows: Vec<(u64, O)> = Vec::new();
        let mut list_index: Vec<u32> = Vec::new();
        for read in reads {
            let Some(part) = scan(read, Some(&mut list_index))? else {
                return Ok(None);
            };
            rows.extend(part);
        }
        set_union(&mut rows, &list_index);
        if rows.len() as u64 > max_predicate_rows() {
            self.log_fast_path_bail("predicate-row-cap-exceeded", Some(edge));
            return Ok(None);
        }
        Ok(Some(rows))
    }

    /// Plan each member's probes ([`Self::compute_probe_overlay_state`]),
    /// keeping its ops in its read. Probes run only if every member allows
    /// them; the second value is each edge's novelty rows over all members,
    /// which the probe gate counts as scan cost.
    fn plan_probes(&self, reads: &mut [MemberRead<'_, '_>]) -> Result<(bool, Vec<u64>)> {
        let mut allowed = true;
        let mut novelty_rows = vec![0u64; self.plan.edges.len()];
        for read in reads.iter_mut() {
            let (member_allowed, ops) = self.compute_probe_overlay_state(read)?;
            allowed &= member_allowed;
            for (rows, ops) in novelty_rows.iter_mut().zip(&ops) {
                *rows += ops.as_ref().map_or(0, |ops| ops.len() as u64);
            }
            read.probe_ops = ops;
        }
        Ok((allowed, novelty_rows))
    }

    /// Per-edge probe eligibility under one member's overlay.
    ///
    /// Overlay-free HEAD keeps plain probes everywhere (`ops = None` for
    /// every edge). With novelty present, probing stays allowed when every
    /// edge's resolved ops are mergeable (empty → `None`, non-empty →
    /// `Some(ops)` merged per probed subject); any unmergeable edge
    /// (translation failure, novelty-only predicate, eager materialization)
    /// disables probes for the cascade — full scans, which merge through the
    /// overlay cursor, are unaffected. Historical snapshots keep probes off:
    /// the seek reads latest base state with no replay.
    fn compute_probe_overlay_state(
        &self,
        read: &MemberRead<'_, '_>,
    ) -> Result<(bool, Vec<Option<SharedOverlayOps>>)> {
        let n = self.plan.edges.len();
        if read.member.index_is_current() {
            return Ok((true, vec![None; n]));
        }
        let store = read.member.store();
        if read.member.to_t() < store.max_t() {
            return Ok((false, vec![None; n]));
        }
        let mut ops_per_edge = Vec::with_capacity(n);
        for edge in self.plan.edges.iter() {
            let pred_sid = normalize_pred_sid(store, &edge.predicate)?;
            match subject_probe_lane_plan(read.ctx, store, &pred_sid)? {
                ProbeLanePlan::Clean => ops_per_edge.push(None),
                ProbeLanePlan::Merge(ops) => ops_per_edge.push(Some(ops)),
                ProbeLanePlan::Decline => {
                    tracing::debug!(
                        predicate = ?edge.predicate,
                        "cyclic cascade: bounded probes disabled (overlay unmergeable)"
                    );
                    return Ok((false, vec![None; n]));
                }
            }
        }
        Ok((true, ops_per_edge))
    }

    /// Pick the bounded-probe candidate with the smallest frontier among the
    /// remaining edges, logging every gate decision at debug level.
    /// `size_bound` / `materialize_frontier` abstract over the ref / encoded
    /// relation index types.
    fn select_bounded_probe(
        &self,
        remaining: &[usize],
        novelty_rows: &[u64],
        size_bound: impl Fn(VarId) -> Option<usize>,
        materialize_frontier: impl Fn(VarId) -> Option<FxHashSet<u64>>,
    ) -> Option<(usize, FxHashSet<u64>)> {
        let mut probe: Option<(usize, FxHashSet<u64>)> = None;
        for (pos, &edge_idx) in remaining.iter().enumerate() {
            let edge = &self.plan.edges[edge_idx];
            // O(1) rejects before materializing any frontier set. The size
            // bound is an over-estimate of the intersection, so rejecting on
            // it is conservative (never unsound).
            let Some(estimate) = edge.estimate else {
                tracing::debug!(
                    predicate = ?edge.predicate,
                    "cyclic probe gate declined: stats-absent predicate (full scan is trivially cheap)"
                );
                continue;
            };
            let Some(bound) = size_bound(edge.subject) else {
                tracing::debug!(
                    predicate = ?edge.predicate,
                    "cyclic probe gate declined: no scanned relation bounds the subject var"
                );
                continue;
            };
            if bound > max_bounded_probe_subjects() {
                tracing::debug!(
                    predicate = ?edge.predicate,
                    frontier_bound = bound,
                    cap = max_bounded_probe_subjects(),
                    "cyclic probe gate declined: frontier bound over subject cap"
                );
                continue;
            }
            let Some(frontier) = materialize_frontier(edge.subject) else {
                continue;
            };
            // Stats exclude novelty; count the edge's overlay ops into the
            // scan-cost side so novelty-heavy edges don't look cheaper to
            // scan than they are.
            let estimate = estimate + novelty_rows[edge_idx];
            if !should_probe_edge(frontier.len(), Some(estimate)) {
                tracing::debug!(
                    predicate = ?edge.predicate,
                    frontier = frontier.len(),
                    estimate,
                    scan_ratio = bounded_probe_scan_ratio(),
                    "cyclic probe gate declined: probe-vs-scan ratio"
                );
                continue;
            }
            if probe
                .as_ref()
                .is_none_or(|(_, best)| frontier.len() < best.len())
            {
                probe = Some((pos, frontier));
            }
        }
        probe
    }

    /// Whether to bypass the sequential cascade and scan every edge's
    /// relation concurrently. Requires populated stats on all edges, at least
    /// one scan big enough to amortize the pool, and proof that no bounded
    /// probe could engage: by induction, if no edge's subject var is exposed
    /// by any other edge with distinct counts under the cap (with margin),
    /// every cascade level is a full scan whose distinct counts equal those
    /// stats — the probe gate can never pass, so the scans are
    /// order-independent.
    fn parallel_scan_eligible(&self, probing_allowed: bool) -> bool {
        let edges = self.plan.edges.as_ref();
        if !edges.iter().all(|e| e.estimate.is_some_and(|c| c > 0)) {
            // Stats-absent / empty predicates want the sequential cascade's
            // cheapest-first early exit instead.
            return false;
        }
        if !edges
            .iter()
            .any(|e| e.estimate.unwrap_or(0) >= PARALLEL_EDGE_SCAN_MIN_ROWS)
        {
            return false;
        }
        if !probing_allowed {
            return true;
        }
        let cap = (max_bounded_probe_subjects() as u64).saturating_mul(NO_PROBE_NDV_MARGIN);
        edges.iter().enumerate().all(|(idx, edge)| {
            edges.iter().enumerate().all(|(other_idx, other)| {
                if other_idx == idx {
                    return true;
                }
                let bound = if other.subject == edge.subject {
                    other.ndv_subjects
                } else if other.object == edge.subject {
                    other.ndv_values
                } else {
                    return true;
                };
                bound.is_some_and(|b| b > cap)
            })
        })
    }

    fn open_ref_fast_path_parallel(&mut self, reads: &[MemberRead<'_, '_>]) -> Result<bool> {
        tracing::debug!(
            edges = self.plan.edges.len(),
            "cyclic cascade: parallel full scan of all edges (no probe can engage)"
        );
        let edges: Vec<CyclicEdge> = self.plan.edges.to_vec();
        let results = parallel_map_pooled(edges, |edge| {
            let rows = self.load_relation(reads, &edge, |read, list_index| {
                self.scan_ref_relation(read, &edge, list_index)
            })?;
            Ok(rows.map(|rows| {
                let span = tracing::debug_span!(
                    "cyclic_index_build",
                    predicate = %predicate_display(&edge.predicate),
                    rows = rows.len(),
                );
                let _guard = span.enter();
                RefRelationIndex::new(edge, rows)
            }))
        });
        let mut relations = Vec::with_capacity(results.len());
        let mut raw_rows = 0usize;
        for result in results {
            let Some(rel) = result? else {
                return Ok(false);
            };
            raw_rows += rel.rows.len();
            relations.push(rel);
        }
        self.raw_relation_rows = raw_rows;
        self.probed_edges = 0;
        self.used_fast_path = true;
        if let Some(empty_idx) = relations.iter().position(|rel| rel.rows.is_empty()) {
            self.driver_idx = empty_idx;
            self.ref_relations = relations;
            self.pruned_relation_rows = 0;
            return Ok(true);
        }
        let relations = self.prune_ref_relations(relations);
        self.pruned_relation_rows = relations.iter().map(|rel| rel.rows.len()).sum();
        self.driver_idx = self.choose_ref_driver(&relations);
        self.ref_relations = relations;
        Ok(true)
    }

    fn open_encoded_fast_path_parallel(&mut self, reads: &[MemberRead<'_, '_>]) -> Result<bool> {
        tracing::debug!(
            edges = self.plan.edges.len(),
            "cyclic cascade: parallel full scan of all edges (no probe can engage)"
        );
        let edges: Vec<CyclicEdge> = self.plan.edges.to_vec();
        let results = parallel_map_pooled(edges, |edge| {
            let rows = self.load_relation(reads, &edge, |read, list_index| {
                self.scan_relation(read, &edge, list_index)
            })?;
            Ok(rows.map(|rows| {
                let span = tracing::debug_span!(
                    "cyclic_index_build",
                    predicate = %predicate_display(&edge.predicate),
                    rows = rows.len(),
                );
                let _guard = span.enter();
                RelationIndex::new(edge, rows)
            }))
        });
        let mut relations = Vec::with_capacity(results.len());
        let mut raw_rows = 0usize;
        for result in results {
            let Some(rel) = result? else {
                return Ok(false);
            };
            raw_rows += rel.rows.len();
            relations.push(rel);
        }
        self.raw_relation_rows = raw_rows;
        self.probed_edges = 0;
        self.used_fast_path = true;
        if let Some(empty_idx) = relations.iter().position(|rel| rel.rows.is_empty()) {
            self.driver_idx = empty_idx;
            self.relations = relations;
            self.pruned_relation_rows = 0;
            return Ok(true);
        }
        let relations = self.prune_relations(relations);
        self.pruned_relation_rows = relations.iter().map(|rel| rel.rows.len()).sum();
        self.square_wedge = self.open_encoded_square_wedge(&relations);
        self.driver_idx = self.choose_driver(&relations);
        self.relations = relations;
        Ok(true)
    }

    /// Cascading relation loader shared by both join modes: scan the cheapest
    /// edge first, then at each level either probe a remaining edge per-subject
    /// (when an already-scanned relation bounds its subject var to a small
    /// frontier and the probe-vs-scan gate passes) or fall back to a full scan
    /// of the cheapest remaining edge. Probed relations are subject-restricted
    /// semi-joins; `prune_relations` re-establishes global consistency after.
    fn open_ref_fast_path(
        &mut self,
        ctx: &ExecutionContext<'_>,
        reads: &mut [MemberRead<'_, '_>],
    ) -> Result<bool> {
        let (probing_allowed, novelty_rows) = self.plan_probes(reads)?;
        let reads: &[MemberRead<'_, '_>] = reads;
        if !probing_allowed {
            tracing::debug!(
                "cyclic cascade: bounded probes disabled (off-HEAD or overlay unmergeable)"
            );
        }
        if self.parallel_scan_eligible(probing_allowed) {
            return self.open_ref_fast_path_parallel(reads);
        }
        let mut remaining: Vec<usize> = (0..self.plan.edges.len()).collect();
        let mut relations: Vec<RefRelationIndex> = Vec::with_capacity(remaining.len());
        let mut raw_rows = 0usize;
        let mut probed_edges = 0usize;

        while !remaining.is_empty() {
            ctx.check_cancelled()?;
            let probe = if probing_allowed && !relations.is_empty() {
                self.select_bounded_probe(
                    &remaining,
                    &novelty_rows,
                    |var| Self::ref_frontier_size_bound(&relations, var),
                    |var| Self::ref_frontier_for_var(&relations, var),
                )
            } else {
                None
            };

            let (pos, rows) = match probe {
                Some((pos, frontier)) => {
                    let edge = &self.plan.edges[remaining[pos]];
                    tracing::debug!(
                        predicate = ?edge.predicate,
                        frontier = frontier.len(),
                        "cyclic cascade: probing edge per-subject"
                    );
                    let edge_idx = remaining[pos];
                    let rows = if frontier.is_empty() {
                        Vec::new()
                    } else {
                        match self.load_relation(reads, edge, |read, list_index| {
                            self.scan_ref_relation_for_subjects(
                                read,
                                edge,
                                &frontier,
                                read.probe_ops[edge_idx].as_ref(),
                                list_index,
                            )
                        })? {
                            Some(rows) => rows,
                            None => return Ok(false),
                        }
                    };
                    probed_edges += 1;
                    (pos, rows)
                }
                None => {
                    let pos = Self::cheapest_remaining(&self.plan.edges, &remaining);
                    let edge = &self.plan.edges[remaining[pos]];
                    tracing::debug!(
                        predicate = ?edge.predicate,
                        estimate = edge.estimate.unwrap_or(0),
                        "cyclic cascade: full-scanning edge"
                    );
                    let rows = match self.load_relation(reads, edge, |read, list_index| {
                        self.scan_ref_relation(read, edge, list_index)
                    })? {
                        Some(rows) => rows,
                        None => return Ok(false),
                    };
                    (pos, rows)
                }
            };
            let edge_idx = remaining.swap_remove(pos);
            raw_rows += rows.len();
            let empty = rows.is_empty();
            let edge = self.plan.edges[edge_idx].clone();
            let rel = {
                let span = tracing::debug_span!(
                    "cyclic_index_build",
                    predicate = %predicate_display(&edge.predicate),
                    rows = rows.len(),
                );
                let _guard = span.enter();
                RefRelationIndex::new(edge, rows)
            };
            relations.push(rel);
            if empty {
                self.driver_idx = relations.len() - 1;
                self.ref_relations = relations;
                self.raw_relation_rows = raw_rows;
                self.pruned_relation_rows = 0;
                self.probed_edges = probed_edges;
                self.used_fast_path = true;
                return Ok(true);
            }
        }

        relations = self.prune_ref_relations(relations);
        self.raw_relation_rows = raw_rows;
        self.pruned_relation_rows = relations.iter().map(|rel| rel.rows.len()).sum();
        self.probed_edges = probed_edges;
        self.driver_idx = self.choose_ref_driver(&relations);
        self.ref_relations = relations;
        self.used_fast_path = true;
        Ok(true)
    }

    fn open_encoded_fast_path(
        &mut self,
        ctx: &ExecutionContext<'_>,
        reads: &mut [MemberRead<'_, '_>],
    ) -> Result<bool> {
        let (probing_allowed, novelty_rows) = self.plan_probes(reads)?;
        let reads: &[MemberRead<'_, '_>] = reads;
        if !probing_allowed {
            tracing::debug!(
                "cyclic cascade: bounded probes disabled (off-HEAD or overlay unmergeable)"
            );
        }
        if self.parallel_scan_eligible(probing_allowed) {
            return self.open_encoded_fast_path_parallel(reads);
        }
        let mut remaining: Vec<usize> = (0..self.plan.edges.len()).collect();
        let mut relations: Vec<RelationIndex> = Vec::with_capacity(remaining.len());
        let mut raw_rows = 0usize;
        let mut probed_edges = 0usize;

        while !remaining.is_empty() {
            ctx.check_cancelled()?;
            let probe = if probing_allowed && !relations.is_empty() {
                self.select_bounded_probe(
                    &remaining,
                    &novelty_rows,
                    |var| Self::frontier_size_bound(&relations, var),
                    |var| self.frontier_for_var(&relations, var),
                )
            } else {
                None
            };

            let (pos, rows) = match probe {
                Some((pos, frontier)) => {
                    let edge = &self.plan.edges[remaining[pos]];
                    tracing::debug!(
                        predicate = ?edge.predicate,
                        frontier = frontier.len(),
                        "cyclic cascade: probing edge per-subject"
                    );
                    let edge_idx = remaining[pos];
                    let rows = if frontier.is_empty() {
                        Vec::new()
                    } else {
                        match self.load_relation(reads, edge, |read, list_index| {
                            self.scan_relation_for_subjects(
                                read,
                                edge,
                                &frontier,
                                read.probe_ops[edge_idx].as_ref(),
                                list_index,
                            )
                        })? {
                            Some(rows) => rows,
                            None => return Ok(false),
                        }
                    };
                    probed_edges += 1;
                    (pos, rows)
                }
                None => {
                    let pos = Self::cheapest_remaining(&self.plan.edges, &remaining);
                    let edge = &self.plan.edges[remaining[pos]];
                    tracing::debug!(
                        predicate = ?edge.predicate,
                        estimate = edge.estimate.unwrap_or(0),
                        "cyclic cascade: full-scanning edge"
                    );
                    let rows = match self.load_relation(reads, edge, |read, list_index| {
                        self.scan_relation(read, edge, list_index)
                    })? {
                        Some(rows) => rows,
                        None => return Ok(false),
                    };
                    (pos, rows)
                }
            };
            let edge_idx = remaining.swap_remove(pos);
            raw_rows += rows.len();
            let empty = rows.is_empty();
            let edge = self.plan.edges[edge_idx].clone();
            let rel = {
                let span = tracing::debug_span!(
                    "cyclic_index_build",
                    predicate = %predicate_display(&edge.predicate),
                    rows = rows.len(),
                );
                let _guard = span.enter();
                RelationIndex::new(edge, rows)
            };
            relations.push(rel);
            if empty {
                self.driver_idx = relations.len() - 1;
                self.relations = relations;
                self.raw_relation_rows = raw_rows;
                self.pruned_relation_rows = 0;
                self.probed_edges = probed_edges;
                self.used_fast_path = true;
                return Ok(true);
            }
        }

        relations = self.prune_relations(relations);
        self.raw_relation_rows = raw_rows;
        self.pruned_relation_rows = relations.iter().map(|rel| rel.rows.len()).sum();
        self.square_wedge = self.open_encoded_square_wedge(&relations);
        self.probed_edges = probed_edges;
        self.driver_idx = self.choose_driver(&relations);
        self.relations = relations;
        self.used_fast_path = true;
        Ok(true)
    }

    /// Position (within `remaining`) of the cheapest edge to full-scan next.
    /// A stats-absent estimate sorts first: with populated stats, absent means
    /// the predicate is empty, making it the ideal early-exit scan.
    fn cheapest_remaining(edges: &[CyclicEdge], remaining: &[usize]) -> usize {
        remaining
            .iter()
            .enumerate()
            .min_by_key(|(_, &edge_idx)| edges[edge_idx].estimate.unwrap_or(0))
            .map(|(pos, _)| pos)
            .expect("cheapest_remaining called with non-empty remaining")
    }

    fn prune_ref_relations(&self, relations: Vec<RefRelationIndex>) -> Vec<RefRelationIndex> {
        Self::prune_relation_set(relations, |s| s)
    }

    fn prune_relations(&self, relations: Vec<RelationIndex>) -> Vec<RelationIndex> {
        Self::prune_relation_set(relations, EncObj::from_ref)
    }

    /// Semi-join prune to a fixpoint: per var, intersect the value sets every
    /// incident relation exposes, then drop rows outside the intersection.
    /// `make_subject` lifts a subject id into the object value domain (`O`)
    /// so subject and object occurrences of a var share one set.
    fn prune_relation_set<O: Copy + Eq + std::hash::Hash + Ord>(
        mut relations: Vec<RelationIdx<O>>,
        make_subject: impl Fn(u64) -> O,
    ) -> Vec<RelationIdx<O>> {
        let span = tracing::debug_span!(
            "cyclic_prune",
            rows_before = relations.iter().map(|rel| rel.rows.len()).sum::<usize>() as u64,
            rows_after = tracing::field::Empty,
            passes = tracing::field::Empty,
        );
        let _guard = span.enter();
        let mut passes = 0u64;
        loop {
            let before: usize = relations.iter().map(|rel| rel.rows.len()).sum();
            let allowed = Self::allowed_values_for(&relations, &make_subject);
            relations = relations
                .into_iter()
                .map(|rel| {
                    let subject_allowed = allowed.get(&rel.edge.subject);
                    let object_allowed = allowed.get(&rel.edge.object);
                    let rows = rel
                        .rows
                        .into_iter()
                        .filter(|&(s, o)| {
                            subject_allowed.is_none_or(|set| set.contains(&make_subject(s)))
                                && object_allowed.is_none_or(|set| set.contains(&o))
                        })
                        .collect();
                    RelationIdx::new(rel.edge, rows)
                })
                .collect();
            passes += 1;
            let after: usize = relations.iter().map(|rel| rel.rows.len()).sum();
            if after == before {
                span.record("rows_after", after as u64);
                span.record("passes", passes);
                return relations;
            }
        }
    }

    fn allowed_values_for<O: Copy + Eq + std::hash::Hash + Ord>(
        relations: &[RelationIdx<O>],
        make_subject: &impl Fn(u64) -> O,
    ) -> FxHashMap<VarId, FxHashSet<O>> {
        let mut allowed: FxHashMap<VarId, FxHashSet<O>> = FxHashMap::default();
        for rel in relations {
            Self::intersect_allowed(
                &mut allowed,
                rel.edge.subject,
                rel.by_subject.keys().map(|&s| make_subject(s)).collect(),
            );
            Self::intersect_allowed(
                &mut allowed,
                rel.edge.object,
                rel.by_object.keys().copied().collect(),
            );
        }
        allowed
    }

    fn intersect_allowed<O: Eq + std::hash::Hash>(
        allowed: &mut FxHashMap<VarId, FxHashSet<O>>,
        var: VarId,
        values: FxHashSet<O>,
    ) {
        allowed
            .entry(var)
            .and_modify(|existing| existing.retain(|value| values.contains(value)))
            .or_insert(values);
    }

    fn open_encoded_square_wedge(
        &self,
        relations: &[RelationIndex],
    ) -> Option<EncodedSquareWedgeState> {
        if self.plan.shape != CyclicBgpShape::Square || relations.len() != 4 {
            return None;
        }
        let span = tracing::debug_span!("cyclic_wedge_build");
        let _guard = span.enter();

        let cap = max_square_wedge_pairs();
        let plan = self
            .encoded_square_wedge_plans(relations)
            .into_iter()
            .min_by_key(|plan| (plan.build_pairs, plan.probe_pairs))?;
        if plan.build_pairs > cap {
            tracing::debug!(
                build_pairs = plan.build_pairs,
                probe_pairs = plan.probe_pairs,
                cap,
                "cyclic bgp square wedge bypassed: build pair cap exceeded"
            );
            return None;
        }

        let table = self.build_encoded_wedge_table(relations, &plan, cap)?;
        let probe_centers = self.wedge_center_intersection(
            &relations[plan.probe_edge_a],
            &relations[plan.probe_edge_b],
            plan.probe_center,
        );

        tracing::debug!(
            build_pairs = plan.build_pairs,
            probe_pairs = plan.probe_pairs,
            build_center = plan.build_center.0,
            probe_center = plan.probe_center.0,
            table_keys = table.len(),
            probe_centers = probe_centers.len(),
            "cyclic bgp square wedge selected"
        );

        Some(EncodedSquareWedgeState {
            plan,
            table,
            probe_centers,
            probe_center_pos: 0,
            current: None,
        })
    }

    fn encoded_square_wedge_plans(
        &self,
        relations: &[RelationIndex],
    ) -> Vec<EncodedSquareWedgePlan> {
        let vars = self.plan.vars.as_ref();
        let mut plans = Vec::new();
        for i in 0..vars.len() {
            for j in (i + 1)..vars.len() {
                let center_a = vars[i];
                let center_b = vars[j];
                if self.edge_between(relations, center_a, center_b).is_some() {
                    continue;
                }
                let keys: Vec<VarId> = vars
                    .iter()
                    .copied()
                    .filter(|v| *v != center_a && *v != center_b)
                    .collect();
                if keys.len() != 2 {
                    continue;
                }
                let key_a = keys[0];
                let key_b = keys[1];
                let Some(a_edge_a) = self.edge_between(relations, center_a, key_a) else {
                    continue;
                };
                let Some(a_edge_b) = self.edge_between(relations, center_a, key_b) else {
                    continue;
                };
                let Some(b_edge_a) = self.edge_between(relations, center_b, key_a) else {
                    continue;
                };
                let Some(b_edge_b) = self.edge_between(relations, center_b, key_b) else {
                    continue;
                };

                let a_pairs =
                    self.wedge_pair_count(&relations[a_edge_a], &relations[a_edge_b], center_a);
                let b_pairs =
                    self.wedge_pair_count(&relations[b_edge_a], &relations[b_edge_b], center_b);
                if a_pairs <= b_pairs {
                    plans.push(EncodedSquareWedgePlan {
                        build_center: center_a,
                        probe_center: center_b,
                        key_a,
                        key_b,
                        build_edge_a: a_edge_a,
                        build_edge_b: a_edge_b,
                        probe_edge_a: b_edge_a,
                        probe_edge_b: b_edge_b,
                        build_pairs: a_pairs,
                        probe_pairs: b_pairs,
                    });
                } else {
                    plans.push(EncodedSquareWedgePlan {
                        build_center: center_b,
                        probe_center: center_a,
                        key_a,
                        key_b,
                        build_edge_a: b_edge_a,
                        build_edge_b: b_edge_b,
                        probe_edge_a: a_edge_a,
                        probe_edge_b: a_edge_b,
                        build_pairs: b_pairs,
                        probe_pairs: a_pairs,
                    });
                }
            }
        }
        plans
    }

    fn edge_between(&self, relations: &[RelationIndex], a: VarId, b: VarId) -> Option<usize> {
        relations.iter().position(|rel| {
            (rel.edge.subject == a && rel.edge.object == b)
                || (rel.edge.subject == b && rel.edge.object == a)
        })
    }

    fn wedge_pair_count(
        &self,
        edge_a: &RelationIndex,
        edge_b: &RelationIndex,
        center: VarId,
    ) -> usize {
        self.wedge_center_intersection(edge_a, edge_b, center)
            .into_iter()
            .map(|center_value| {
                self.relation_degree_for_center(edge_a, center, &center_value)
                    .saturating_mul(self.relation_degree_for_center(edge_b, center, &center_value))
            })
            .sum()
    }

    fn wedge_center_intersection(
        &self,
        edge_a: &RelationIndex,
        edge_b: &RelationIndex,
        center: VarId,
    ) -> Vec<EncObj> {
        let values_a = self.relation_center_values(edge_a, center);
        let values_b = self.relation_center_values(edge_b, center);
        values_a
            .into_iter()
            .filter(|value| values_b.contains(value))
            .collect()
    }

    fn relation_center_values(&self, rel: &RelationIndex, center: VarId) -> FxHashSet<EncObj> {
        if rel.edge.subject == center {
            rel.by_subject
                .keys()
                .map(|&s_id| EncObj::from_ref(s_id))
                .collect()
        } else if rel.edge.object == center {
            rel.by_object.keys().copied().collect()
        } else {
            FxHashSet::default()
        }
    }

    fn relation_degree_for_center(
        &self,
        rel: &RelationIndex,
        center: VarId,
        center_value: &EncObj,
    ) -> usize {
        if rel.edge.subject == center {
            center_value
                .ref_s_id()
                .map(|s_id| rel.subject_rows(s_id).len())
                .unwrap_or(0)
        } else if rel.edge.object == center {
            rel.object_degree(center_value)
        } else {
            0
        }
    }

    fn relation_values_for_center(
        &self,
        rel: &RelationIndex,
        center: VarId,
        center_value: &EncObj,
    ) -> Vec<EncObj> {
        if rel.edge.subject == center {
            center_value
                .ref_s_id()
                .map(|s_id| rel.subject_rows(s_id).iter().map(|&(_, o)| o).collect())
                .unwrap_or_default()
        } else if rel.edge.object == center {
            rel.object_subjects(center_value)
                .map(EncObj::from_ref)
                .collect()
        } else {
            Vec::new()
        }
    }

    fn build_encoded_wedge_table(
        &self,
        relations: &[RelationIndex],
        plan: &EncodedSquareWedgePlan,
        cap: usize,
    ) -> Option<FxHashMap<WedgePairKey, Vec<EncObj>>> {
        let edge_a = &relations[plan.build_edge_a];
        let edge_b = &relations[plan.build_edge_b];
        let centers = self.wedge_center_intersection(edge_a, edge_b, plan.build_center);
        let mut table: FxHashMap<WedgePairKey, Vec<EncObj>> = FxHashMap::default();
        let mut total = 0usize;
        for center in centers {
            let values_a = self.relation_values_for_center(edge_a, plan.build_center, &center);
            let values_b = self.relation_values_for_center(edge_b, plan.build_center, &center);
            for &value_a in &values_a {
                for &value_b in &values_b {
                    total = total.saturating_add(1);
                    if total > cap {
                        return None;
                    }
                    table
                        .entry(WedgePairKey(value_a, value_b))
                        .or_default()
                        .push(center);
                }
            }
        }
        Some(table)
    }

    fn choose_ref_driver(&self, relations: &[RefRelationIndex]) -> usize {
        relations
            .iter()
            .enumerate()
            .min_by_key(|(idx, rel)| {
                let mut used = vec![false; relations.len()];
                used[*idx] = true;
                let assigned = self.ref_assigned_for_edge(&rel.edge);
                (
                    rel.rows.len().saturating_mul(
                        self.choose_next_ref_relation_with(relations, &assigned, &used)
                            .map(|next| {
                                self.ref_bound_fanout_score(&relations[next], &assigned)
                                    .max(1)
                            })
                            .unwrap_or(1),
                    ),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    fn ref_assigned_for_edge(&self, edge: &CyclicEdge) -> Vec<Option<u64>> {
        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(edge.subject)] = Some(0);
        assigned[self.var_pos(edge.object)] = Some(0);
        assigned
    }

    fn choose_driver(&self, relations: &[RelationIndex]) -> usize {
        relations
            .iter()
            .enumerate()
            .min_by_key(|(idx, rel)| {
                let mut used = vec![false; relations.len()];
                used[*idx] = true;
                let assigned = self.assigned_for_edge(&rel.edge);
                (
                    rel.rows.len().saturating_mul(
                        self.choose_next_relation_with(relations, &assigned, &used)
                            .map(|next| self.bound_fanout_score(&relations[next], &assigned).max(1))
                            .unwrap_or(1),
                    ),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    }

    fn assigned_for_edge(&self, edge: &CyclicEdge) -> Vec<Option<EncObj>> {
        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(edge.subject)] = Some(EncObj::from_ref(0));
        assigned[self.var_pos(edge.object)] = Some(EncObj::from_ref(0));
        assigned
    }

    fn choose_next_ref_relation(&self, assigned: &[Option<u64>], used: &[bool]) -> Option<usize> {
        self.choose_next_ref_relation_with(&self.ref_relations, assigned, used)
    }

    fn choose_next_ref_relation_with(
        &self,
        relations: &[RefRelationIndex],
        assigned: &[Option<u64>],
        used: &[bool],
    ) -> Option<usize> {
        relations
            .iter()
            .enumerate()
            .filter(|(idx, _)| !used[*idx])
            .min_by_key(|(_, rel)| {
                let bound_count = self.ref_bound_count(rel, assigned);
                (
                    std::cmp::Reverse(bound_count),
                    self.ref_bound_fanout_score(rel, assigned),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
    }

    fn ref_bound_count(&self, rel: &RefRelationIndex, assigned: &[Option<u64>]) -> u8 {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        s_bound as u8 + o_bound as u8
    }

    fn ref_bound_fanout_score(&self, rel: &RefRelationIndex, assigned: &[Option<u64>]) -> usize {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        match (s_bound, o_bound) {
            (true, true) => 1,
            (true, false) => average_bucket_size(rel.rows.len(), rel.distinct_subjects()),
            (false, true) => average_bucket_size(rel.rows.len(), rel.distinct_objects()),
            (false, false) => rel.rows.len(),
        }
    }

    fn ref_relation_candidates(
        &self,
        rel: &RefRelationIndex,
        assigned: &[Option<u64>],
    ) -> Vec<(u64, u64)> {
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        match (assigned[s_pos], assigned[o_pos]) {
            // The callers take a bound pair without candidates; kept total.
            (Some(s), Some(o)) => vec![(s, o); rel.pair_count(s, &o)],
            (Some(s), None) => rel.subject_rows(s).to_vec(),
            (None, Some(o)) => rel.object_subjects(&o).map(|s| (s, o)).collect(),
            (None, None) => rel.rows.clone(),
        }
    }

    fn choose_next_relation(&self, assigned: &[Option<EncObj>], used: &[bool]) -> Option<usize> {
        self.choose_next_relation_with(&self.relations, assigned, used)
    }

    fn choose_next_relation_with(
        &self,
        relations: &[RelationIndex],
        assigned: &[Option<EncObj>],
        used: &[bool],
    ) -> Option<usize> {
        relations
            .iter()
            .enumerate()
            .filter(|(idx, _)| !used[*idx])
            .min_by_key(|(_, rel)| {
                let bound_count = self.bound_count(rel, assigned);
                (
                    std::cmp::Reverse(bound_count),
                    self.bound_fanout_score(rel, assigned),
                    rel.rows.len(),
                )
            })
            .map(|(idx, _)| idx)
    }

    fn bound_count(&self, rel: &RelationIndex, assigned: &[Option<EncObj>]) -> u8 {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        s_bound as u8 + o_bound as u8
    }

    fn bound_fanout_score(&self, rel: &RelationIndex, assigned: &[Option<EncObj>]) -> usize {
        let s_bound = assigned[self.var_pos(rel.edge.subject)].is_some();
        let o_bound = assigned[self.var_pos(rel.edge.object)].is_some();
        match (s_bound, o_bound) {
            (true, true) => 1,
            (true, false) => average_bucket_size(rel.rows.len(), rel.distinct_subjects()),
            (false, true) => average_bucket_size(rel.rows.len(), rel.distinct_objects()),
            (false, false) => rel.rows.len(),
        }
    }

    fn extend_ref_assignments(
        &self,
        assigned: &mut [Option<u64>],
        used: &mut [bool],
        out: &mut VecDeque<Vec<u64>>,
    ) {
        let Some(rel_idx) = self.choose_next_ref_relation(assigned, used) else {
            out.push_back(
                assigned
                    .iter()
                    .map(|v| v.expect("all cyclic vars assigned before emit"))
                    .collect(),
            );
            return;
        };
        let rel = &self.ref_relations[rel_idx];
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        used[rel_idx] = true;
        if let (Some(s), Some(o)) = (assigned[s_pos], assigned[o_pos]) {
            // Both ends bound: the edge only multiplies the assignment by the
            // rows it holds for the pair.
            for _ in 0..rel.pair_count(s, &o) {
                self.extend_ref_assignments(assigned, used, out);
            }
            used[rel_idx] = false;
            return;
        }
        let candidates = self.ref_relation_candidates(rel, assigned);
        for (subject, object) in candidates {
            let old_s = assigned[s_pos];
            let old_o = assigned[o_pos];
            if old_s.is_some_and(|s| s != subject) || old_o.is_some_and(|o| o != object) {
                continue;
            }
            assigned[s_pos] = Some(subject);
            assigned[o_pos] = Some(object);
            self.extend_ref_assignments(assigned, used, out);
            assigned[s_pos] = old_s;
            assigned[o_pos] = old_o;
        }
        used[rel_idx] = false;
    }

    fn relation_candidates(
        &self,
        rel: &RelationIndex,
        assigned: &[Option<EncObj>],
    ) -> Vec<(u64, EncObj)> {
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        match (assigned[s_pos], assigned[o_pos]) {
            // The callers take a bound pair without candidates; kept total.
            (Some(s), Some(o)) => s
                .ref_s_id()
                .map_or_else(Vec::new, |s_id| vec![(s_id, o); rel.pair_count(s_id, &o)]),
            (Some(s), None) => s
                .ref_s_id()
                .map(|s_id| rel.subject_rows(s_id).to_vec())
                .unwrap_or_default(),
            (None, Some(o)) => rel.object_subjects(&o).map(|s| (s, o)).collect(),
            (None, None) => rel.rows.clone(),
        }
    }

    fn extend_assignments(
        &self,
        assigned: &mut [Option<EncObj>],
        used: &mut [bool],
        out: &mut VecDeque<Vec<EncObj>>,
    ) {
        let Some(rel_idx) = self.choose_next_relation(assigned, used) else {
            out.push_back(
                assigned
                    .iter()
                    .map(|v| v.expect("all cyclic vars assigned before emit"))
                    .collect(),
            );
            return;
        };
        let rel = &self.relations[rel_idx];
        let s_pos = self.var_pos(rel.edge.subject);
        let o_pos = self.var_pos(rel.edge.object);
        used[rel_idx] = true;
        if let (Some(s), Some(o)) = (assigned[s_pos], assigned[o_pos]) {
            // Both ends bound: the edge only multiplies the assignment by the
            // rows it holds for the pair (none when the subject isn't a ref).
            let rows = s.ref_s_id().map_or(0, |s_id| rel.pair_count(s_id, &o));
            for _ in 0..rows {
                self.extend_assignments(assigned, used, out);
            }
            used[rel_idx] = false;
            return;
        }
        let candidates = self.relation_candidates(rel, assigned);
        for (subject_id, object) in candidates {
            let subject = EncObj::from_ref(subject_id);
            let old_s = assigned[s_pos];
            let old_o = assigned[o_pos];
            if old_s.is_some_and(|s| s != subject) || old_o.is_some_and(|o| o != object) {
                continue;
            }
            assigned[s_pos] = Some(subject);
            assigned[o_pos] = Some(object);
            self.extend_assignments(assigned, used, out);
            assigned[s_pos] = old_s;
            assigned[o_pos] = old_o;
        }
        used[rel_idx] = false;
    }

    fn seed_next_driver(&mut self) {
        if self.relations.is_empty() {
            return;
        }
        let driver = &self.relations[self.driver_idx];
        if self.driver_pos >= driver.rows.len() {
            return;
        }
        let (subject, object) = driver.rows[self.driver_pos];
        self.driver_pos += 1;

        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(driver.edge.subject)] = Some(EncObj::from_ref(subject));
        assigned[self.var_pos(driver.edge.object)] = Some(object);
        let mut used = vec![false; self.relations.len()];
        used[self.driver_idx] = true;
        let mut out = VecDeque::new();
        self.extend_assignments(&mut assigned, &mut used, &mut out);
        self.pending.extend(out);
    }

    fn seed_next_ref_driver(&mut self) {
        if self.ref_relations.is_empty() {
            return;
        }
        let driver = &self.ref_relations[self.driver_idx];
        if self.driver_pos >= driver.rows.len() {
            return;
        }
        let (subject, object) = driver.rows[self.driver_pos];
        self.driver_pos += 1;

        let mut assigned = vec![None; self.plan.vars.len()];
        assigned[self.var_pos(driver.edge.subject)] = Some(subject);
        assigned[self.var_pos(driver.edge.object)] = Some(object);
        let mut used = vec![false; self.ref_relations.len()];
        used[self.driver_idx] = true;
        let mut out = VecDeque::new();
        self.extend_ref_assignments(&mut assigned, &mut used, &mut out);
        self.ref_pending.extend(out);
    }

    /// Materialize one fully-assigned row into the output columns. Vars that
    /// appear in subject position bind as plain `EncodedSid` (matching the
    /// scan path); object-only vars rebuild the late-materialized binding
    /// (decoding non-reserved string-dict values through the emit view).
    /// Over several graphs both decode, as the members' scans do there.
    fn assignment_to_columns(
        &self,
        assignment: &[EncObj],
        cols: &mut [Vec<Binding>],
    ) -> Result<()> {
        for (out_idx, var_idx) in self.schema_positions.iter().copied().enumerate() {
            let var = self.plan.vars[var_idx];
            let value = assignment[var_idx];
            let binding = if self.var_is_subject(var) {
                let s_id = value
                    .ref_s_id()
                    .expect("subject-positioned cyclic vars only bind refs");
                Binding::encoded_sid(s_id)
            } else {
                value.to_binding(&self.emit)?
            };
            cols[out_idx].push(binding);
        }
        Ok(())
    }

    fn next_square_wedge_match(
        &self,
        state: &mut EncodedSquareWedgeState,
    ) -> Option<(EncObj, EncObj, EncObj, EncObj)> {
        loop {
            if state.current.is_none() {
                while state.probe_center_pos < state.probe_centers.len() {
                    let center = state.probe_centers[state.probe_center_pos];
                    state.probe_center_pos += 1;
                    let values_a = self.relation_values_for_center(
                        &self.relations[state.plan.probe_edge_a],
                        state.plan.probe_center,
                        &center,
                    );
                    let values_b = self.relation_values_for_center(
                        &self.relations[state.plan.probe_edge_b],
                        state.plan.probe_center,
                        &center,
                    );
                    if !values_a.is_empty() && !values_b.is_empty() {
                        state.current = Some(EncodedProbeWedgeCursor {
                            center,
                            values_a,
                            values_b,
                            a_pos: 0,
                            b_pos: 0,
                            matches: Vec::new(),
                            match_pos: 0,
                        });
                        break;
                    }
                }
                state.current.as_ref()?;
            }

            let cursor = state.current.as_mut().expect("square wedge cursor");
            if cursor.match_pos < cursor.matches.len() {
                let build_center = cursor.matches[cursor.match_pos];
                cursor.match_pos += 1;
                return Some((
                    build_center,
                    cursor.center,
                    cursor.values_a[cursor.a_pos],
                    cursor.values_b[cursor.b_pos],
                ));
            }
            if !cursor.matches.is_empty() {
                advance_probe_cursor(cursor);
            }

            while cursor.a_pos < cursor.values_a.len() {
                if cursor.b_pos >= cursor.values_b.len() {
                    cursor.a_pos += 1;
                    cursor.b_pos = 0;
                    continue;
                }
                let key_a = cursor.values_a[cursor.a_pos];
                let key_b = cursor.values_b[cursor.b_pos];
                if let Some(matches) = state.table.get(&WedgePairKey(key_a, key_b)) {
                    cursor.matches = matches.clone();
                    cursor.match_pos = 0;
                    if !cursor.matches.is_empty() {
                        break;
                    }
                }
                advance_probe_cursor(cursor);
            }

            if cursor.a_pos >= cursor.values_a.len() {
                state.current = None;
            }
        }
    }

    fn next_encoded_square_wedge_batch(&mut self) -> Result<Option<Batch>> {
        let span = tracing::debug_span!(
            "cyclic_enumerate",
            strategy = "square_wedge",
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let mut state = self
            .square_wedge
            .take()
            .expect("square wedge state must exist for wedge batch");
        let mut cols: Vec<Vec<Binding>> = self.schema.iter().map(|_| Vec::new()).collect();
        let mut produced = 0usize;
        while produced < OUTPUT_BATCH_SIZE {
            let Some((build_center, probe_center, key_a, key_b)) =
                self.next_square_wedge_match(&mut state)
            else {
                break;
            };
            if cols.is_empty() {
                produced += 1;
                continue;
            }
            // All four square vars are assigned below; the placeholder is
            // never emitted.
            let mut assignment = vec![EncObj::from_ref(0); self.plan.vars.len()];
            assignment[self.var_pos(state.plan.build_center)] = build_center;
            assignment[self.var_pos(state.plan.probe_center)] = probe_center;
            assignment[self.var_pos(state.plan.key_a)] = key_a;
            assignment[self.var_pos(state.plan.key_b)] = key_b;
            if let Err(e) = self.assignment_to_columns(&assignment, &mut cols) {
                self.square_wedge = Some(state);
                return Err(e);
            }
            produced += 1;
        }
        self.square_wedge = Some(state);
        span.record("rows", produced as u64);

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if cols.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Batch::new(Arc::clone(&self.schema), cols)
            .map(Some)
            .map_err(|e| QueryError::Internal(format!("cyclic bgp square wedge batch: {e}")))
    }

    fn ref_assignment_to_columns(
        &self,
        assignment: &[u64],
        cols: &mut [Vec<Binding>],
    ) -> Result<()> {
        for (out_idx, var_idx) in self.schema_positions.iter().copied().enumerate() {
            let s_id = assignment[var_idx];
            cols[out_idx].push(Binding::encoded_sid(s_id));
        }
        Ok(())
    }

    fn next_ref_batch(&mut self) -> Result<Option<Batch>> {
        let span = tracing::debug_span!(
            "cyclic_enumerate",
            strategy = "ref",
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let mut cols: Vec<Vec<Binding>> = self.schema.iter().map(|_| Vec::new()).collect();
        let mut produced = 0usize;
        while produced < OUTPUT_BATCH_SIZE {
            if let Some(assignment) = self.ref_pending.pop_front() {
                if cols.is_empty() {
                    produced += 1;
                } else {
                    self.ref_assignment_to_columns(&assignment, &mut cols)?;
                    produced += 1;
                }
                continue;
            }
            if self.ref_relations.is_empty()
                || self.driver_pos >= self.ref_relations[self.driver_idx].rows.len()
            {
                break;
            }
            self.seed_next_ref_driver();
        }
        span.record("rows", produced as u64);

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if cols.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Batch::new(Arc::clone(&self.schema), cols)
            .map(Some)
            .map_err(|e| QueryError::Internal(format!("cyclic bgp ref batch: {e}")))
    }

    fn next_encoded_batch(&mut self) -> Result<Option<Batch>> {
        if self.square_wedge.is_some() {
            return self.next_encoded_square_wedge_batch();
        }
        let span = tracing::debug_span!(
            "cyclic_enumerate",
            strategy = "encoded",
            rows = tracing::field::Empty,
        );
        let _guard = span.enter();
        let mut cols: Vec<Vec<Binding>> = self.schema.iter().map(|_| Vec::new()).collect();
        let mut produced = 0usize;
        while produced < OUTPUT_BATCH_SIZE {
            if let Some(assignment) = self.pending.pop_front() {
                if cols.is_empty() {
                    produced += 1;
                } else {
                    self.assignment_to_columns(&assignment, &mut cols)?;
                    produced += 1;
                }
                continue;
            }
            if self.relations.is_empty()
                || self.driver_pos >= self.relations[self.driver_idx].rows.len()
            {
                break;
            }
            self.seed_next_driver();
        }
        span.record("rows", produced as u64);

        if produced == 0 {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        if cols.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(produced)));
        }
        Batch::new(Arc::clone(&self.schema), cols)
            .map(Some)
            .map_err(|e| QueryError::Internal(format!("cyclic bgp batch: {e}")))
    }
}

/// `Proceed` when the lane opened, else a fallback for `reason`.
fn proceed_if(fast: bool, reason: FastPathFallback) -> FastPathOutcome {
    if fast {
        FastPathOutcome::Proceed
    } else {
        FastPathOutcome::Fallback(reason)
    }
}

fn average_bucket_size(rows: usize, distinct: usize) -> usize {
    if rows == 0 {
        0
    } else {
        rows.div_ceil(distinct.max(1))
    }
}

fn advance_probe_cursor(cursor: &mut EncodedProbeWedgeCursor) {
    cursor.matches.clear();
    cursor.match_pos = 0;
    cursor.b_pos += 1;
    if cursor.b_pos >= cursor.values_b.len() {
        cursor.a_pos += 1;
        cursor.b_pos = 0;
    }
}

fn predicate_display(predicate: &Ref) -> String {
    match predicate {
        Ref::Sid(sid) => format!("{}:{}", sid.namespace_code, sid.name),
        Ref::Iri(iri) => iri.to_string(),
        Ref::Var(v) => format!("?v{}", v.0),
    }
}

#[async_trait]
impl Operator for CyclicBgpOperator {
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("strategy".into(), "cyclic_bgp_join".into());
        m.insert("shape".into(), self.plan.shape_name().into());
        m.insert(
            "enabled".into(),
            serde_json::Value::Bool(cyclic_bgp_enabled()),
        );
        m.insert("max-predicate-rows".into(), max_predicate_rows().into());
        m.insert("object-only-values".into(), self.join_mode.as_str().into());
        m.insert("pruning".into(), "semi_join".into());
        if self.graphs > 1 {
            m.insert("default-graph-members".into(), self.graphs.into());
            m.insert("relations".into(), "set_union".into());
        }
        m.insert("driver-selection".into(), "pruned_bound_fanout".into());
        if let Some(state) = &self.square_wedge {
            m.insert("square-strategy".into(), "wedge_pair_hash".into());
            m.insert("square-build-pairs".into(), state.plan.build_pairs.into());
            m.insert("square-probe-pairs".into(), state.plan.probe_pairs.into());
            m.insert(
                "square-wedge-pair-cap".into(),
                max_square_wedge_pairs().into(),
            );
        }
        if self.probed_edges > 0 {
            m.insert(
                "bounded-probe-strategy".into(),
                "cascading_subject_probe".into(),
            );
            m.insert("bounded-probe-edges".into(), self.probed_edges.into());
            m.insert(
                "bounded-probe-subject-cap".into(),
                max_bounded_probe_subjects().into(),
            );
            m.insert(
                "bounded-probe-scan-ratio".into(),
                bounded_probe_scan_ratio().into(),
            );
        }
        if self.raw_relation_rows > 0 {
            m.insert("raw-relation-rows".into(), self.raw_relation_rows.into());
            m.insert(
                "pruned-relation-rows".into(),
                self.pruned_relation_rows.into(),
            );
        }
        let predicates: Vec<serde_json::Value> = self
            .plan
            .edges
            .iter()
            .map(|edge| predicate_display(&edge.predicate).into())
            .collect();
        m.insert("predicates".into(), serde_json::Value::Array(predicates));
        let predicate_estimates: Vec<serde_json::Value> = self
            .plan
            .edges
            .iter()
            .map(|edge| edge.estimate.map_or(serde_json::Value::Null, Into::into))
            .collect();
        m.insert(
            "predicate-row-estimates".into(),
            serde_json::Value::Array(predicate_estimates),
        );
        m
    }

    fn plan_children(&self) -> Vec<PlanChild<'_>> {
        self.fallback
            .as_deref()
            .map(|op| PlanChild::fallback(op as &dyn Operator))
            .into_iter()
            .collect()
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.state != OperatorState::Created {
            return Err(QueryError::Internal(
                "CyclicBgpOperator::open() called in invalid state".into(),
            ));
        }
        let (site, outcome) = self.open_fast_path(ctx)?;
        crate::fast_path_outcome::stamp_fast_path(site, outcome);
        if outcome == FastPathOutcome::Proceed {
            self.state = OperatorState::Open;
            return Ok(());
        }
        if let Some(fallback) = self.fallback.as_mut() {
            fallback.open(ctx).await?;
            self.state = OperatorState::Open;
            return Ok(());
        }
        self.state = OperatorState::Exhausted;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        ctx.check_cancelled()?;
        if !self.used_fast_path {
            return match self.fallback.as_mut() {
                Some(fallback) => fallback.next_batch(ctx).await,
                None => Ok(None),
            };
        }

        match self.join_mode {
            CyclicJoinMode::RefOnly => self.next_ref_batch(),
            CyclicJoinMode::EncodedObject => self.next_encoded_batch(),
        }
    }

    fn close(&mut self) {
        if let Some(fallback) = self.fallback.as_mut() {
            fallback.close();
        }
        self.ref_relations.clear();
        self.relations.clear();
        self.ref_pending.clear();
        self.pending.clear();
        self.square_wedge = None;
        self.probed_edges = 0;
        self.emit = Emit::none();
        self.state = OperatorState::Closed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::Term;
    use crate::seed::EmptyOperator;
    use fluree_db_core::Sid;

    fn triple(s: u16, p: &str, o: u16) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(VarId(s)),
            Ref::Sid(Sid::new(100, p)),
            Term::Var(VarId(o)),
        )
    }

    fn ref_rel(edge: &CyclicEdge, rows: &[(u64, u64)]) -> RefRelationIndex {
        RefRelationIndex::new(edge.clone(), rows.to_vec())
    }

    fn rel(edge: &CyclicEdge, rows: &[(u64, u64)]) -> RelationIndex {
        RelationIndex::new(
            edge.clone(),
            rows.iter()
                .map(|&(subject, object)| (subject, EncObj::from_ref(object)))
                .collect(),
        )
    }

    fn operator_for(triples: &[TriplePattern]) -> CyclicBgpOperator {
        let plan = analyze_cyclic_bgp(triples, None).expect("test shape should be cyclic");
        CyclicBgpOperator::new(plan, None, Box::new(EmptyOperator::new()))
    }

    #[test]
    fn object_only_cycle_vars_accept_encoded_literals() {
        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::EncodedObject);
        let object = op.encode_object_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::XSD_STRING.as_u16(),
                object: 42,
                p_id: 7,
            },
        );

        let object = object.expect("encoded literal object should be accepted");
        assert!(matches!(object.id, EncIdentity::Lit { o_key: 42, .. }));
        assert!(matches!(
            object
                .to_binding(&Emit::none())
                .expect("reserved string-dict o_types need no view"),
            Binding::EncodedLit { o_key: 42, .. }
        ));
    }

    #[test]
    fn object_only_cycle_vars_accept_encoded_temporal_values() {
        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::EncodedObject);
        let object = op.encode_object_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::XSD_DATE.as_u16(),
                object: 12_345,
                p_id: 7,
            },
        );

        let object = object.expect("encoded temporal object should be accepted");
        assert!(matches!(object.id, EncIdentity::Lit { o_key: 12_345, .. }));
        assert!(matches!(
            object
                .to_binding(&Emit::none())
                .expect("encoded temporal o_types need no view"),
            Binding::EncodedLit { o_key: 12_345, .. }
        ));
    }

    /// Non-reserved string-dictionary datatypes (#1729) stay in the encoded
    /// join keyed by `(o_type, o_key)`: same key unifies, different datatype
    /// over the same lexical form splits, and the emit-side rebuild demands a
    /// graph view (the decode has no `EncodedLit` form to fall back on).
    #[test]
    fn object_only_cycle_vars_accept_non_reserved_string_dict_objects() {
        use fluree_db_core::ids::DatatypeDictId;

        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::EncodedObject);

        let encode = |o_type: OType, o_key: u64| {
            op.encode_object_for_edge(
                &op.plan.edges[0],
                RawEdgeRow {
                    subject: 10,
                    o_type: o_type.as_u16(),
                    object: o_key,
                    p_id: 7,
                },
            )
        };

        let any_uri =
            encode(OType::XSD_ANY_URI, 42).expect("xsd:anyURI object should stay on the fast path");
        let customer = encode(OType::customer_datatype(DatatypeDictId::RESERVED_COUNT), 42)
            .expect("customer-datatype object should stay on the fast path");
        let string = encode(OType::XSD_STRING, 42).expect("xsd:string object");

        // Same dictionary key, three distinct terms (#1729's failure shape).
        assert_ne!(any_uri, string);
        assert_ne!(customer, string);
        assert_ne!(any_uri, customer);
        // Same datatype + key unifies; a different key splits.
        assert_eq!(any_uri, encode(OType::XSD_ANY_URI, 42).unwrap());
        assert_ne!(any_uri, encode(OType::XSD_ANY_URI, 43).unwrap());

        // Emit needs the store: there is no encoded binding to rebuild.
        assert!(any_uri.to_binding(&Emit::none()).is_err());
        assert!(customer.to_binding(&Emit::none()).is_err());
    }

    /// `EncIdentity::Lit` keys on `o_type` where `Binding::eq` on `EncodedLit`
    /// compares `(o_kind, dt_id, lang_id)` — sound only while the materializer
    /// maps o_types to those triples injectively. Walk the entire u16 space:
    /// a future encoded arm that reuses an existing triple for a second
    /// o_type must fail here, because it would make the cyclic join split
    /// terms the rest of the engine unifies.
    #[test]
    fn encoded_lit_identity_fields_are_injective_over_o_types() {
        let mut seen: FxHashMap<(u8, u16, u16), u16> = FxHashMap::default();
        for raw in 0..=u16::MAX {
            let Some(Binding::EncodedLit {
                o_kind,
                dt_id,
                lang_id,
                ..
            }) = late_materialized_object_binding(raw, 1, 1, 0, u32::MAX, None, 0)
            else {
                continue;
            };
            if let Some(prev) = seen.insert((o_kind, dt_id, lang_id), raw) {
                panic!(
                    "o_types {prev:#06x} and {raw:#06x} share EncodedLit identity fields \
                     (o_kind={o_kind}, dt_id={dt_id}, lang_id={lang_id}); \
                     EncIdentity::Lit's o_type keying would split what Binding::eq unifies"
                );
            }
        }
        // The walk must have covered the encoded arms, not vacuously skipped.
        assert!(
            seen.len() > 10,
            "expected the full encoded o_type set, saw {}",
            seen.len()
        );
    }

    #[test]
    fn subject_bridge_cycle_vars_still_require_ref_objects() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);
        assert_eq!(op.join_mode, CyclicJoinMode::RefOnly);

        let literal = op.encode_object_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::XSD_STRING.as_u16(),
                object: 42,
                p_id: 7,
            },
        );
        assert!(literal.is_none());

        let iri = op.encode_object_for_edge(
            &op.plan.edges[0],
            RawEdgeRow {
                subject: 10,
                o_type: OType::IRI_REF.as_u16(),
                object: 99,
                p_id: 7,
            },
        );
        assert!(matches!(
            iri,
            Some(EncObj {
                id: EncIdentity::Ref(99),
                ..
            })
        ));
    }

    #[test]
    fn ref_pruning_keeps_only_values_supported_by_all_incident_edges() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

        let relations = vec![
            ref_rel(&op.plan.edges[0], &[(1, 10), (9, 90)]),
            ref_rel(&op.plan.edges[1], &[(10, 20), (90, 99)]),
            ref_rel(&op.plan.edges[2], &[(20, 1)]),
        ];

        let pruned = op.prune_ref_relations(relations);
        let sizes: Vec<usize> = pruned.iter().map(|rel| rel.rows.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
        assert_eq!(pruned[0].rows[0].0, 1);
        assert_eq!(pruned[1].rows[0].1, 20);
    }

    #[test]
    fn next_relation_prefers_lower_bound_endpoint_fanout_over_total_rows() {
        let triples = vec![
            triple(0, "driver", 1),
            triple(0, "low_fanout", 2),
            triple(1, "high_fanout", 3),
            triple(3, "close", 2),
        ];
        let op = operator_for(&triples);

        let low_fanout_rows: Vec<(u64, u64)> = (1..=100).map(|v| (v, 10_000 + v)).collect();
        let high_fanout_rows: Vec<(u64, u64)> = (1..=50).map(|v| (1, 20_000 + v)).collect();
        let relations = vec![
            ref_rel(&op.plan.edges[0], &[(1, 1)]),
            ref_rel(&op.plan.edges[1], &low_fanout_rows),
            ref_rel(&op.plan.edges[2], &high_fanout_rows),
            ref_rel(&op.plan.edges[3], &[(20_001, 10_001)]),
        ];
        let mut assigned = vec![None; op.plan.vars.len()];
        assigned[op.var_pos(VarId(0))] = Some(1);
        assigned[op.var_pos(VarId(1))] = Some(1);
        let used = vec![true, false, false, false];

        let next = op
            .choose_next_ref_relation_with(&relations, &assigned, &used)
            .expect("one relation should be selected");
        assert_eq!(next, 1);
    }

    #[test]
    fn encoded_square_wedge_uses_smaller_exact_pair_side() {
        let triples = vec![
            triple(0, "left_a", 1),
            triple(0, "left_b", 2),
            triple(3, "right_a", 1),
            triple(3, "right_b", 2),
        ];
        let op = operator_for(&triples);
        let relations = vec![
            // Center ?v0 has 2 * 2 = 4 pairs.
            rel(&op.plan.edges[0], &[(1, 10), (1, 11)]),
            rel(&op.plan.edges[1], &[(1, 20), (1, 21)]),
            // Center ?v3 has 1 * 1 = 1 pair, so it should be the build side.
            rel(&op.plan.edges[2], &[(3, 10)]),
            rel(&op.plan.edges[3], &[(3, 20)]),
        ];

        let state = op
            .open_encoded_square_wedge(&relations)
            .expect("square wedge should be selected");
        assert!(matches!(state.plan.build_center, VarId(1 | 3)));
        assert_ne!(state.plan.build_center, state.plan.probe_center);
        assert_eq!(state.plan.build_pairs, 1);
        assert!(state.plan.probe_pairs >= state.plan.build_pairs);
    }

    #[test]
    fn encoded_square_wedge_streams_probe_pairs_and_outputs_matches() {
        let triples = vec![
            triple(0, "left_a", 1),
            triple(0, "left_b", 2),
            triple(3, "right_a", 1),
            triple(3, "right_b", 2),
        ];
        let mut op = operator_for(&triples);
        let relations = vec![
            rel(&op.plan.edges[0], &[(1, 10), (2, 99)]),
            rel(&op.plan.edges[1], &[(1, 20), (2, 20)]),
            rel(&op.plan.edges[2], &[(3, 10)]),
            rel(&op.plan.edges[3], &[(3, 20)]),
        ];
        let relations = op.prune_relations(relations);
        let state = op
            .open_encoded_square_wedge(&relations)
            .expect("square wedge should be selected");
        op.relations = relations;
        op.square_wedge = Some(state);

        let batch = op
            .next_encoded_batch()
            .expect("square wedge batch should succeed")
            .expect("one batch should be produced");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.get_by_col(0, 0), &Binding::encoded_sid(1));
        assert_eq!(batch.get_by_col(0, 1), &Binding::encoded_sid(10));
        assert_eq!(batch.get_by_col(0, 2), &Binding::encoded_sid(20));
        assert_eq!(batch.get_by_col(0, 3), &Binding::encoded_sid(3));
    }

    #[test]
    fn probe_gate_requires_known_estimate_and_scan_ratio() {
        // Absent estimate means "likely empty" — full scan is already cheap.
        assert!(!should_probe_edge(10, None));
        // Probe only when the estimated scan is >= ratio rows per probe.
        let ratio = bounded_probe_scan_ratio();
        assert!(should_probe_edge(100, Some(100 * ratio)));
        assert!(!should_probe_edge(100, Some(100 * ratio - 1)));
        // Frontier above the subject cap never probes.
        let over_cap = max_bounded_probe_subjects() + 1;
        assert!(!should_probe_edge(over_cap, Some(u64::MAX)));
        // An empty frontier trivially passes (the caller skips I/O entirely).
        assert!(should_probe_edge(0, Some(1)));
    }

    #[test]
    fn frontier_intersects_across_all_scanned_relations() {
        // Directed triangle: ?0 -p1-> ?1 -p2-> ?2 -p3-> ?0 (RefOnly shape, but
        // the encoded frontier helper sees the same edge endpoints).
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

        // Var ?1 is object of edge0 and subject of edge1. With both scanned,
        // the frontier is the intersection of edge0's objects and edge1's subjects.
        let scanned = vec![
            rel(&op.plan.edges[0], &[(1, 10), (2, 11), (3, 12)]),
            rel(&op.plan.edges[1], &[(10, 20), (12, 21), (99, 22)]),
        ];
        let frontier = op
            .frontier_for_var(&scanned, VarId(1))
            .expect("frontier should be derivable");
        let mut got: Vec<u64> = frontier.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 12]);

        // A var not exposed by any scanned relation has no frontier.
        assert!(op.frontier_for_var(&scanned[..1], VarId(2)).is_none());
    }

    #[test]
    fn frontier_from_object_side_requires_encoded_sids() {
        let triples = vec![
            triple(0, "p1", 1),
            triple(0, "p2", 2),
            triple(3, "p3", 1),
            triple(3, "p4", 2),
        ];
        let op = operator_for(&triples);

        // ?1 appears only as an object; EncodedSid objects yield a frontier.
        let scanned = vec![rel(&op.plan.edges[0], &[(1, 10), (2, 11)])];
        let frontier = op
            .frontier_for_var(&scanned, VarId(1))
            .expect("encoded-sid objects should bound the var");
        assert_eq!(frontier.len(), 2);

        // Non-ref object values can never join a subject var, so they're
        // filtered out of the frontier (here: down to empty, meaning the
        // overall result is provably empty).
        let lit = op
            .encode_object_for_edge(
                &op.plan.edges[0],
                RawEdgeRow {
                    subject: 1,
                    o_type: OType::XSD_STRING.as_u16(),
                    object: 42,
                    p_id: 7,
                },
            )
            .expect("object-only var accepts encoded literals");
        let lit_rel = RelationIndex::new(op.plan.edges[0].clone(), vec![(1, lit)]);
        let frontier = op
            .frontier_for_var(&[lit_rel], VarId(1))
            .expect("incident relation still yields a frontier");
        assert!(frontier.is_empty());
    }

    #[test]
    fn ref_frontier_intersects_subject_and_object_sides() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);

        let scanned = vec![
            ref_rel(&op.plan.edges[0], &[(1, 10), (2, 11)]),
            ref_rel(&op.plan.edges[1], &[(10, 20), (11, 21), (50, 22)]),
        ];
        let frontier = CyclicBgpOperator::ref_frontier_for_var(&scanned, VarId(1))
            .expect("frontier should be derivable");
        let mut got: Vec<u64> = frontier.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![10, 11]);
    }

    #[test]
    fn cheapest_remaining_treats_absent_estimate_as_empty() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);
        let mut edges: Vec<CyclicEdge> = op.plan.edges.to_vec();
        edges[0].estimate = Some(5);
        edges[1].estimate = None;
        edges[2].estimate = Some(1);

        // Stats-absent (edge 1) sorts before every known estimate.
        let remaining = vec![0, 1, 2];
        let pos = CyclicBgpOperator::cheapest_remaining(&edges, &remaining);
        assert_eq!(remaining[pos], 1);

        let remaining = vec![0, 2];
        let pos = CyclicBgpOperator::cheapest_remaining(&edges, &remaining);
        assert_eq!(remaining[pos], 2);
    }

    /// Over several graphs an edge's rows are its facts, each once: a fact
    /// two members both hold collapses, while two list items with one value
    /// (two facts) stay two rows, and so does a plain fact beside a list item
    /// with the same value. Every index over the relation then counts a
    /// repeated pair as often as it is stored, the pair count as the subject
    /// and object scans do.
    #[test]
    fn set_union_keeps_each_fact_once() {
        let triples = vec![triple(0, "p1", 1), triple(1, "p2", 2), triple(2, "p3", 0)];
        let op = operator_for(&triples);
        let edge = &op.plan.edges[0];
        const PLAIN: u32 = u32::MAX;
        // Member one: (1, 2) as list items 0 and 1, and (1, 3). Member two:
        // the same two list items, (1, 2) as a plain fact, and (4, 5).
        let mut rows = vec![(1, 2), (1, 2), (1, 3), (1, 2), (1, 2), (1, 2), (4, 5)];
        let list_index = [0, 1, PLAIN, 0, 1, PLAIN, PLAIN];
        set_union(&mut rows, &list_index);
        assert_eq!(rows, vec![(1, 2), (1, 2), (1, 2), (1, 3), (4, 5)]);

        let rel = ref_rel(edge, &rows);
        assert_eq!(rel.subject_rows(1).iter().filter(|r| r.1 == 2).count(), 3);
        assert_eq!(rel.object_subjects(&2).filter(|&s| s == 1).count(), 3);
        assert_eq!(rel.pair_count(1, &2), 3);
        assert_eq!(rel.pair_count(1, &3), 1);
        assert_eq!(rel.pair_count(4, &2), 0);
        assert_eq!(rel.pair_count(1, &9), 0);
    }

    /// Encoded objects are kept once per fact: equal identity at one list
    /// index collapses, while a string and a ref that share an `o_key` stay
    /// two terms.
    #[test]
    fn set_union_of_encoded_objects_keys_on_term_identity() {
        let string = |o_key| {
            EncObj::encode(OType::XSD_STRING.as_u16(), o_key, 7).expect("xsd:string encodes")
        };
        let mut rows = vec![
            (1, string(10)),
            (1, EncObj::from_ref(10)),
            (1, string(10)),
            (1, EncObj::from_ref(10)),
        ];
        set_union(&mut rows, &[u32::MAX; 4]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .filter(|(_, o)| o.ref_s_id() == Some(10))
                .count(),
            1
        );
    }

    #[test]
    fn arena_o_types_are_the_per_graph_handles() {
        for arena in [
            OType::VECTOR,
            OType::NUM_BIG_OVERFLOW,
            OType::from_u16(0x800C),
        ] {
            assert!(is_arena_o_type(arena.as_u16()), "{arena:?}");
        }
        for ledger_wide in [
            OType::IRI_REF,
            OType::XSD_STRING,
            OType::XSD_INTEGER,
            OType::BLANK_NODE,
            OType::from_u16(0x8008),
        ] {
            assert!(!is_arena_o_type(ledger_wide.as_u16()), "{ledger_wide:?}");
        }
    }
}
