//! Forward-arena probe operator — the physical counterpart to a Cypher
//! relationship binding (`MATCH (a)-[r:P]->(b) … RETURN r.prop`).
//!
//! ## Why this exists
//!
//! Binding a relationship variable reifies the edge: Fluree's RDF-star
//! lowering turns `[r:P]` into a base edge plus a `f:reifies*` sidecar
//! (`?r f:reifiesSubject a`, `?r f:reifiesPredicate P`, `?r
//! f:reifiesObject b`). Executed as generic triple joins those three
//! lookups scatter across the *whole* annotation sidecar in the base
//! index — the cost behind IC5's timeout.
//!
//! This operator replaces those three joins with one **forward-arena
//! merge-scan**: it drives a stream of fully-bound base edges, builds an
//! [`EdgeKey`] per row, and probes the annotation arena's forward index
//! (`EdgeKey → ann`) in a single sorted pass. The reifier variable `?r`
//! is bound directly; the relationship-property reads downstream
//! (`?r joinDate ?d`) then plan as ordinary subject-keyed lookups.
//!
//! ## Where it slots
//!
//! Recognized and built inside [`crate::default_graph_source`]'s
//! single-graph delegate, where the expanded chain `[base edge + 3
//! f:reifies* + body]` is already grouped. The base edge plans normally
//! (so visibility + policy filtering still happen on it), this operator
//! enriches each surviving edge with its reifier, and the body plans
//! normally on top. When any gate fails the caller keeps the generic
//! join chain — a slower but identical-result fallback.
//!
//! ## Gates (all checked before this operator is built)
//!
//! - a forward annotation arena is sealed on the snapshot,
//! - current-state query (history falls back — the arena reader's
//!   visibility model is `as_of_t`, but ranged history is out of scope),
//! - the attachment overlay is empty (so the indexed arena is
//!   authoritative; with annotation novelty the per-edge merged path is
//!   required and we fall back),
//! - root / no policy (the base edge and body stay policy-filtered via
//!   their own scans; the structural `f:reifies*` binding comes from the
//!   arena, so we gate it to root to avoid leaking a reifier a policy
//!   would hide).

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::{Pattern, Ref, Term, TriplePattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::annotation_arena::AnnotationArenaReader;
use fluree_db_core::edge::{id_datatype_sid, EdgeKey};
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{AnnotationIndexRoot, FlakeValue, Sid, StatsView};
use std::collections::HashMap;
use std::sync::Arc;

/// The recognized `[base edge + 3 f:reifies* + body]` shape, decomposed
/// into what the probe operator and its surrounding plan need.
pub(crate) struct AnnotationEdgeShape {
    /// Base-edge triple, planned normally (visibility + policy).
    pub base: Pattern,
    /// Reifier variable bound by the probe.
    pub ann_var: VarId,
    pub s_pos: EdgePos,
    /// Constant relationship predicate. Cypher lowers relationship types
    /// to `Ref::Iri`; the caller resolves it to a `Sid` (with `ctx`)
    /// before constructing the operator, falling back if it can't.
    pub p_pred: Ref,
    pub o_pos: EdgePos,
    /// Remaining patterns (relationship-property reads, filters), planned
    /// normally on top of the probe with `ann_var` bound.
    pub body: Vec<Pattern>,
}

/// True iff `r` is the constant `f:<name>` predicate ref.
fn is_reifies_pred(r: &Ref, name: &str) -> bool {
    matches!(r, Ref::Sid(sid)
        if sid.namespace_code == fluree_vocab::namespaces::FLUREE_DB
            && sid.name.as_ref() == name)
}

/// Recognize the expanded edge-annotation chain produced by
/// `expand_edge_annotation_patterns`: a base-edge triple followed
/// immediately by the three `f:reifies{Subject,Predicate,Object}`
/// triples (all sharing one reifier var, their objects matching the
/// base edge's s/p/o), then arbitrary body patterns.
///
/// Returns `None` (→ generic-join fallback) unless every structural and
/// fast-path-eligibility condition holds: a constant relationship
/// predicate, ref-valued subject and object (node-to-node edge), and the
/// three reifies triples in canonical position.
pub(crate) fn recognize_annotation_edge(patterns: &[Pattern]) -> Option<AnnotationEdgeShape> {
    use fluree_vocab::db::{REIFIES_OBJECT, REIFIES_PREDICATE, REIFIES_SUBJECT};

    if patterns.len() < 4 {
        return None;
    }
    let Pattern::Triple(base) = &patterns[0] else {
        return None;
    };
    let (Pattern::Triple(r_subj), Pattern::Triple(r_pred), Pattern::Triple(r_obj)) =
        (&patterns[1], &patterns[2], &patterns[3])
    else {
        return None;
    };

    // Constant relationship type (a typed Cypher relationship lowers to
    // `Ref::Iri`; a `Ref::Sid` is also accepted) or a variable predicate
    // (untyped `-[p]->`): the probe resolves a variable per row from the
    // base-edge binding, so both are probeable.
    let p_pred = base.p.clone();
    // Subject and object must be node refs (no IRI/literal objects in v1).
    let s_pos = EdgePos::from_ref(&base.s)?;
    let o_pos = EdgePos::from_term(&base.o)?;

    // The three reifies triples share one reifier var as subject.
    let ann_var = r_subj.s.as_var()?;
    if r_pred.s.as_var() != Some(ann_var) || r_obj.s.as_var() != Some(ann_var) {
        return None;
    }
    if !is_reifies_pred(&r_subj.p, REIFIES_SUBJECT)
        || !is_reifies_pred(&r_pred.p, REIFIES_PREDICATE)
        || !is_reifies_pred(&r_obj.p, REIFIES_OBJECT)
    {
        return None;
    }
    // Their objects must reference the base edge's s / p / o.
    if r_subj.o != Term::from(base.s.clone())
        || r_pred.o != Term::from(base.p.clone())
        || r_obj.o != base.o
    {
        return None;
    }

    Some(AnnotationEdgeShape {
        base: patterns[0].clone(),
        ann_var,
        s_pos,
        p_pred,
        o_pos,
        body: patterns[4..].to_vec(),
    })
}

/// How to obtain one position (subject / object) of the base edge for a
/// given child row. Predicate is always a constant for a typed Cypher
/// relationship, so it is stored directly as a `Sid` on the operator.
#[derive(Clone)]
pub(crate) enum EdgePos {
    /// Constant ref taken straight from the pattern.
    Const(Sid),
    /// Ref carried by a child-row variable binding.
    Var(VarId),
}

impl EdgePos {
    /// A subject/predicate ref position. `None` (→ recognition falls
    /// back) for cross-ledger `Iri` refs, which this single-ledger fast
    /// path cannot probe.
    pub(crate) fn from_ref(r: &Ref) -> Option<Self> {
        match r {
            Ref::Sid(sid) => Some(EdgePos::Const(sid.clone())),
            Ref::Var(v) => Some(EdgePos::Var(*v)),
            Ref::Iri(_) => None,
        }
    }

    /// An object ref position. `None` for literal/IRI objects — only
    /// node-ref edges are handled in v1.
    pub(crate) fn from_term(t: &Term) -> Option<Self> {
        match t {
            Term::Sid(sid) => Some(EdgePos::Const(sid.clone())),
            Term::Var(v) => Some(EdgePos::Var(*v)),
            Term::Iri(_) | Term::Value(_) => None,
        }
    }
}

/// Probe the forward annotation arena to bind a reifier variable from a
/// stream of base edges. See the module docs for the recognized shape.
pub struct AnnotationEdgeProbeOperator {
    child: BoxedOperator,
    /// Reifier variable to bind (`?r`).
    ann_var: VarId,
    /// Base-edge subject source.
    s_pos: EdgePos,
    /// Base-edge predicate source: constant for a typed relationship,
    /// per-row for an untyped `-[p]->` (the base scan binds it as a Sid).
    p_pos: EdgePos,
    /// Base-edge object source.
    o_pos: EdgePos,
    schema: Arc<[VarId]>,
    state: OperatorState,
    /// Owned arena root + store, captured at `open` from the snapshot.
    root: Option<AnnotationIndexRoot>,
    store: Option<Arc<dyn ContentStore>>,
    as_of_t: i64,
    /// Output rows, filled by a single probe pass over the whole child
    /// stream on the first `next_batch`, then drained in chunks. One pass
    /// = one arena reader, so the forward branch/leaves decode once rather
    /// than per child batch.
    probed: bool,
    result_buffer: Vec<Vec<Binding>>,
    buffer_pos: usize,
}

/// Output rows emitted per `next_batch` once the probe pass has filled
/// the buffer. Keeps any single output batch bounded.
const PROBE_OUTPUT_CHUNK: usize = 4096;

impl AnnotationEdgeProbeOperator {
    pub(crate) fn new(
        child: BoxedOperator,
        ann_var: VarId,
        s_pos: EdgePos,
        p_pos: EdgePos,
        o_pos: EdgePos,
    ) -> Self {
        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        if !schema_vec.contains(&ann_var) {
            schema_vec.push(ann_var);
        }
        let schema = Arc::from(schema_vec.into_boxed_slice());

        Self {
            child,
            ann_var,
            s_pos,
            p_pos,
            o_pos,
            schema,
            state: OperatorState::Created,
            root: None,
            store: None,
            as_of_t: 0,
            probed: false,
            result_buffer: Vec::new(),
            buffer_pos: 0,
        }
    }

    /// Build the `EdgeKey` for one child row. Returns `None` only when a
    /// position binding is absent (unbound/poisoned) — those rows can
    /// carry no reifier and are dropped, matching the generic-join
    /// semantics (an unbound edge position matches no `f:reifies*` row).
    fn edge_key_for_row(
        &self,
        batch: &Batch,
        row: usize,
        view: Option<&fluree_db_binary_index::BinaryGraphView>,
    ) -> Result<Option<EdgeKey>> {
        let Some(s) = resolve_pos_ref(batch, row, &self.s_pos, view)? else {
            return Ok(None);
        };
        let Some(p) = resolve_pos_pred(batch, row, &self.p_pos, view)? else {
            return Ok(None);
        };
        // Object: ref-valued for a relationship edge. Resolve to a Sid
        // and wrap as a ref FlakeValue with the `@id` datatype, matching
        // how the arena stored the edge. A literal object (a wildcard
        // `?s ?p ?o` base scan also delivers literal-valued triples) has
        // no probeable ref edge in this v1 fast path — the row is
        // dropped, matching Cypher's node-to-node relationship
        // semantics. (Literal-reified quoted triples take the generic
        // chain whenever recognition declines; probing the arena by
        // materialized literal `EdgeKey` is a known follow-up.)
        let Some(o) = resolve_obj_ref(batch, row, &self.o_pos, view)? else {
            return Ok(None);
        };
        Ok(Some(EdgeKey {
            g: None,
            s,
            p,
            o: FlakeValue::Ref(o),
            dt: id_datatype_sid(),
            lang: None,
            list_i: None,
        }))
    }
}

/// Resolve the predicate position of a base edge. A variable predicate
/// (untyped `-[p]->`) arrives from the base-edge scan either as an eager
/// `Binding::Sid` or late-materialized as `Binding::EncodedPid` — decode
/// the latter through the store's predicate table. An id absent from the
/// persisted table is a shape violation (both probe paths run on planned
/// scans whose predicate ids come from that table) — surface it loudly
/// rather than silently dropping the row.
pub(crate) fn resolve_pos_pred(
    batch: &Batch,
    row: usize,
    pos: &EdgePos,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Result<Option<Sid>> {
    match pos {
        EdgePos::Const(sid) => Ok(Some(sid.clone())),
        EdgePos::Var(v) => match batch.get(row, *v) {
            Some(Binding::Sid { sid, .. }) => Ok(Some(sid.clone())),
            Some(Binding::EncodedPid { p_id }) => {
                let view = view.ok_or_else(|| {
                    QueryError::execution(
                        "annotation edge probe: encoded predicate with no binary graph view",
                    )
                })?;
                view.store()
                    .p_sid_table()
                    .get(*p_id as usize)
                    .cloned()
                    .map(Some)
                    .ok_or_else(|| {
                        QueryError::execution(format!(
                            "annotation edge probe: resolve encoded predicate {p_id}"
                        ))
                    })
            }
            Some(Binding::Unbound | Binding::Poisoned) | None => Ok(None),
            Some(other) => Err(QueryError::execution(format!(
                "annotation edge probe: predicate position bound to non-Sid {other:?}"
            ))),
        },
    }
}

/// Resolve an edge ref position to a concrete `Sid`. Handles the two
/// ref-valued binding representations a base-edge scan can emit:
/// eagerly-resolved `Sid` and late-materialized `EncodedSid`. The
/// latter is decoded **directly** through the subject dictionary
/// (`BinaryGraphView::resolve_subject_sid`) — an IRI round-trip
/// (`resolve_subject_iri` + `encode_iri`) silently returns `None` for
/// subjects whose IRI doesn't re-encode, which would drop rows
/// non-deterministically (a subject may arrive eager or late depending
/// on scan timing). A failure here is a loud error, never a dropped row.
pub(crate) fn resolve_pos_ref(
    batch: &Batch,
    row: usize,
    pos: &EdgePos,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Result<Option<Sid>> {
    match pos {
        EdgePos::Const(sid) => Ok(Some(sid.clone())),
        EdgePos::Var(v) => match batch.get(row, *v) {
            Some(Binding::Sid { sid, .. }) => Ok(Some(sid.clone())),
            Some(Binding::EncodedSid { s_id, .. }) => {
                let view = view.ok_or_else(|| {
                    QueryError::execution(
                        "annotation edge probe: encoded subject with no binary graph view",
                    )
                })?;
                let sid = view.resolve_subject_sid(*s_id).map_err(|e| {
                    QueryError::execution(format!(
                        "annotation edge probe: resolve encoded subject {s_id}: {e}"
                    ))
                })?;
                Ok(Some(sid))
            }
            Some(Binding::Unbound | Binding::Poisoned) | None => Ok(None),
            // A non-ref binding in an edge ref position means the
            // recognized shape's invariant was violated. Surface it
            // loudly rather than silently dropping the row.
            Some(other) => Err(QueryError::execution(format!(
                "annotation edge probe: edge ref position bound to non-ref {other:?}"
            ))),
        },
    }
}

/// Like [`resolve_pos_ref`] for the base edge's OBJECT position, where a
/// non-ref binding is NOT a shape violation: a wildcard (`?s ?p ?o`)
/// base scan also delivers literal-valued triples, which carry no
/// probeable ref edge — `None` drops the row instead of erroring.
pub(crate) fn resolve_obj_ref(
    batch: &Batch,
    row: usize,
    pos: &EdgePos,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Result<Option<Sid>> {
    if let EdgePos::Var(v) = pos {
        match batch.get(row, *v) {
            Some(Binding::Sid { .. } | Binding::EncodedSid { .. }) => {}
            Some(_) | None => return Ok(None),
        }
    }
    resolve_pos_ref(batch, row, pos, view)
}

/// Normalize a binding to a raw `Sid` — decoding late-materialized
/// subjects through the graph view and late-materialized predicates (an
/// untyped `-[p]->` base scan can emit `EncodedPid`) through the store's
/// predicate table. `None` for unbound/poisoned or non-ref bindings —
/// such a position matches no `f:reifies*` row.
pub(crate) fn binding_sid(
    b: &Binding,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Result<Option<Sid>> {
    match b {
        Binding::Sid { sid, .. } => Ok(Some(sid.clone())),
        Binding::EncodedSid { s_id, .. } => {
            let view = view.ok_or_else(|| {
                QueryError::execution("annotation probe: encoded subject with no binary graph view")
            })?;
            view.resolve_subject_sid(*s_id).map(Some).map_err(|e| {
                QueryError::execution(format!(
                    "annotation probe: resolve encoded subject {s_id}: {e}"
                ))
            })
        }
        Binding::EncodedPid { p_id } => {
            let view = view.ok_or_else(|| {
                QueryError::execution(
                    "annotation probe: encoded predicate with no binary graph view",
                )
            })?;
            Ok(view.store().p_sid_table().get(*p_id as usize).cloned())
        }
        _ => Ok(None),
    }
}

/// The three `f:reifies*` lookups, drained once through ordinary planned
/// scans (overlay-merged and policy-filtered like any scan) and
/// hash-indexed. Shared by the value-only OPTIONAL lane
/// ([`crate::optional::AnnotationValueOptionalBuilder`]) and the required
/// lane ([`HashAnnotationEdgeProbeOperator`]).
pub(crate) struct AnnotationSidecarMaps {
    pub(crate) s_to_anns: HashMap<Sid, Vec<Sid>>,
    ann_preds: HashMap<Sid, Vec<Sid>>,
    /// Object keyed by [`GroupKeyOwned`] (representation-normalized), so
    /// ref objects AND literal objects (a literal-valued reified edge)
    /// both match across encoded/materialized binding representations.
    ann_objs: HashMap<Sid, Vec<GroupKeyOwned>>,
}

impl AnnotationSidecarMaps {
    pub(crate) fn matches(&self, ann: &Sid, p: &Sid, o: &GroupKeyOwned) -> bool {
        self.ann_preds
            .get(ann)
            .is_some_and(|preds| preds.contains(p))
            && self.ann_objs.get(ann).is_some_and(|objs| objs.contains(o))
    }

    /// Drain the three reifies triples and build the lookup maps.
    pub(crate) async fn build(
        r_subj: &TriplePattern,
        r_pred: &TriplePattern,
        r_obj: &TriplePattern,
        stats: Option<Arc<StatsView>>,
        planning: &PlanningContext,
        ctx: &ExecutionContext<'_>,
        view: Option<&fluree_db_binary_index::BinaryGraphView>,
    ) -> Result<Self> {
        let mut s_to_anns: HashMap<Sid, Vec<Sid>> = HashMap::new();
        for (ann, s) in drain_pairs(r_subj, stats.clone(), planning, ctx, view).await? {
            s_to_anns.entry(s).or_default().push(ann);
        }
        let mut ann_preds: HashMap<Sid, Vec<Sid>> = HashMap::new();
        for (ann, pred) in drain_pairs(r_pred, stats.clone(), planning, ctx, view).await? {
            ann_preds.entry(ann).or_default().push(pred);
        }
        let mut ann_objs: HashMap<Sid, Vec<GroupKeyOwned>> = HashMap::new();
        for (ann, obj) in drain_object_keys(r_obj, stats, planning, ctx, view).await? {
            ann_objs.entry(ann).or_default().push(obj);
        }
        Ok(Self {
            s_to_anns,
            ann_preds,
            ann_objs,
        })
    }
}

/// The probe-side counterpart to [`drain_object_keys`]'s normalization:
/// one base-edge row's object position as a [`GroupKeyOwned`].
/// `GroupKeyOwned::Absent` (never matched by the maps — absent pairs are
/// skipped at build) for unbound/poisoned positions.
pub(crate) fn row_obj_key(
    batch: &Batch,
    row: usize,
    pos: &EdgePos,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> GroupKeyOwned {
    let store = view.map(fluree_db_binary_index::BinaryGraphView::store);
    match pos {
        EdgePos::Const(sid) => {
            binding_to_group_key_normalized(&Binding::sid(sid.clone()), store, view)
        }
        EdgePos::Var(v) => match batch.get(row, *v) {
            Some(b) => binding_to_group_key_normalized(b, store, view),
            None => GroupKeyOwned::Absent,
        },
    }
}

/// Drain one reifies triple's whole predicate through a planned scan,
/// yielding `(reifier Sid, object Sid)` pairs. Non-ref objects (a
/// literal-valued reified edge) are skipped — the probe only matches
/// ref positions, mirroring `EdgePos::from_term`.
async fn drain_pairs(
    triple: &TriplePattern,
    stats: Option<Arc<StatsView>>,
    planning: &PlanningContext,
    ctx: &ExecutionContext<'_>,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Result<Vec<(Sid, Sid)>> {
    let ann_v = match &triple.s {
        Ref::Var(v) => *v,
        _ => {
            return Err(QueryError::execution(
                "annotation probe: reifies subject must be the reifier var",
            ))
        }
    };
    let o_v = match &triple.o {
        Term::Var(v) => Some(*v),
        _ => None,
    };
    let mut op = crate::execute::build_where_operators_seeded(
        None,
        std::slice::from_ref(&Pattern::Triple(triple.clone())),
        stats,
        None,
        planning,
    )?;
    op.open(ctx).await?;
    let mut out = Vec::new();
    while let Some(batch) = op.next_batch(ctx).await? {
        ctx.check_cancelled()?;
        for r in 0..batch.len() {
            let Some(ann) = batch
                .get(r, ann_v)
                .map(|b| binding_sid(b, view))
                .transpose()?
                .flatten()
            else {
                continue;
            };
            let obj = match o_v {
                Some(v) => batch
                    .get(r, v)
                    .map(|b| binding_sid(b, view))
                    .transpose()?
                    .flatten(),
                // Constant object (typed relationship / fixed endpoint):
                // the scan already filtered to it; record the constant.
                None => match &triple.o {
                    Term::Sid(sid) => Some(sid.clone()),
                    _ => None,
                },
            };
            if let Some(obj) = obj {
                out.push((ann, obj));
            }
        }
    }
    op.close();
    Ok(out)
}

/// Drain the `f:reifiesObject` triple, yielding `(reifier Sid, object
/// key)` pairs with the object normalized via
/// [`binding_to_group_key_normalized`] — ref objects and literal objects
/// (literal-valued reified edges) both key canonically. Absent objects
/// are skipped, so `GroupKeyOwned::Absent` never enters the maps.
async fn drain_object_keys(
    triple: &TriplePattern,
    stats: Option<Arc<StatsView>>,
    planning: &PlanningContext,
    ctx: &ExecutionContext<'_>,
    view: Option<&fluree_db_binary_index::BinaryGraphView>,
) -> Result<Vec<(Sid, GroupKeyOwned)>> {
    let ann_v = match &triple.s {
        Ref::Var(v) => *v,
        _ => {
            return Err(QueryError::execution(
                "annotation probe: reifies subject must be the reifier var",
            ))
        }
    };
    let o_v = match &triple.o {
        Term::Var(v) => Some(*v),
        _ => None,
    };
    let store = view.map(fluree_db_binary_index::BinaryGraphView::store);
    let mut op = crate::execute::build_where_operators_seeded(
        None,
        std::slice::from_ref(&Pattern::Triple(triple.clone())),
        stats,
        None,
        planning,
    )?;
    op.open(ctx).await?;
    let mut out = Vec::new();
    while let Some(batch) = op.next_batch(ctx).await? {
        ctx.check_cancelled()?;
        for r in 0..batch.len() {
            let Some(ann) = batch
                .get(r, ann_v)
                .map(|b| binding_sid(b, view))
                .transpose()?
                .flatten()
            else {
                continue;
            };
            let obj = match o_v {
                Some(v) => batch
                    .get(r, v)
                    .map(|b| binding_to_group_key_normalized(b, store, view))
                    .unwrap_or(GroupKeyOwned::Absent),
                // Constant object (typed relationship / fixed endpoint):
                // the scan already filtered to it; record the constant.
                None => match &triple.o {
                    Term::Sid(sid) => {
                        binding_to_group_key_normalized(&Binding::sid(sid.clone()), store, view)
                    }
                    _ => GroupKeyOwned::Absent,
                },
            };
            if !matches!(obj, GroupKeyOwned::Absent) {
                out.push((ann, obj));
            }
        }
    }
    op.close();
    Ok(out)
}

#[async_trait]
impl Operator for AnnotationEdgeProbeOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.root = ctx.active_snapshot.annotation_index.clone();
        self.store = ctx.active_snapshot.content_store.clone();
        self.as_of_t = ctx.to_t;
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.probed = false;
        self.result_buffer.clear();
        self.buffer_pos = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        if !self.probed {
            self.probe_all(ctx).await?;
            self.probed = true;
        }
        let out = self.drain_chunk();
        if out.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(out)
    }

    fn close(&mut self) {
        self.child.close();
        self.result_buffer.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // ~1 reifier per base edge.
        self.child.estimated_rows()
    }
}

impl AnnotationEdgeProbeOperator {
    /// Drain the whole child stream once, probe the forward arena in a
    /// single merge-scan (one reader → one branch/leaf decode), and fill
    /// `result_buffer` with the fanned-out output rows. The base-edge
    /// stream is bounded by the relationship's cardinality, so
    /// materializing it is cheap relative to the per-batch reader rebuild
    /// it replaces.
    async fn probe_all(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // The reifier binding is appended as the final schema column.
        debug_assert_eq!(self.schema.last(), Some(&self.ann_var));
        let parent_schema_len = self.child.schema().len();

        // Collect every child row's parent-schema bindings plus its
        // EdgeKey (None → the row carries no probeable edge).
        let mut saved_rows: Vec<Vec<Binding>> = Vec::new();
        let mut edges: Vec<EdgeKey> = Vec::new();
        let mut edge_of_row: Vec<Option<usize>> = Vec::new();
        // One subject-dictionary view for the whole pass — decoding an
        // EncodedSid edge endpoint goes straight through it.
        let view = ctx.graph_view();
        while let Some(batch) = self.child.next_batch(ctx).await? {
            if batch.is_empty() {
                continue;
            }
            for row in 0..batch.len() {
                let mut rb = Vec::with_capacity(parent_schema_len);
                for var in self.child.schema() {
                    rb.push(batch.get(row, *var).cloned().unwrap_or(Binding::Unbound));
                }
                match self.edge_key_for_row(&batch, row, view.as_ref())? {
                    Some(ek) => {
                        edge_of_row.push(Some(edges.len()));
                        edges.push(ek);
                    }
                    None => edge_of_row.push(None),
                }
                saved_rows.push(rb);
            }
        }

        let (Some(root), Some(store)) = (self.root.as_ref(), self.store.as_ref()) else {
            // Gates guarantee both are present; defensive only.
            return Ok(());
        };
        let anns_per_edge = {
            let reader = AnnotationArenaReader::new(root, store.as_ref());
            reader
                .current_annotations_batch(&edges, self.as_of_t)
                .await
                .map_err(|e| {
                    QueryError::execution(format!("annotation forward arena probe: {e}"))
                })?
        };

        for (i, mut rb) in saved_rows.into_iter().enumerate() {
            let Some(edge_idx) = edge_of_row[i] else {
                continue;
            };
            let anns = &anns_per_edge[edge_idx];
            match anns.as_slice() {
                [] => {}
                [single] => {
                    rb.push(Binding::sid(single.clone()));
                    self.result_buffer.push(rb);
                }
                many => {
                    // Fan out: one output row per live reifier.
                    for ann in many {
                        let mut row = rb.clone();
                        row.push(Binding::sid(ann.clone()));
                        self.result_buffer.push(row);
                    }
                }
            }
        }
        Ok(())
    }

    /// Emit up to [`PROBE_OUTPUT_CHUNK`] buffered output rows as one batch.
    fn drain_chunk(&mut self) -> Option<Batch> {
        if self.buffer_pos >= self.result_buffer.len() {
            return None;
        }
        let end = (self.buffer_pos + PROBE_OUTPUT_CHUNK).min(self.result_buffer.len());
        let num_cols = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(end - self.buffer_pos))
            .collect();
        for row in &self.result_buffer[self.buffer_pos..end] {
            for (col, b) in row.iter().enumerate() {
                if col < columns.len() {
                    columns[col].push(b.clone());
                }
            }
        }
        self.buffer_pos = end;
        if columns.is_empty() || columns[0].is_empty() {
            return None;
        }
        Batch::new(self.schema.clone(), columns).ok()
    }
}

/// One base-edge row collected by the sweep: the normalized subject key it
/// hash-joins the driving stream on, the concrete Sids the sidecar maps
/// are probed with, and the original bindings for the pattern's VAR
/// positions (`None` for constants — those columns aren't in the schema).
struct SweptEdge {
    s_sid: Sid,
    p_sid: Sid,
    o_key: GroupKeyOwned,
    s_b: Option<Binding>,
    p_b: Option<Binding>,
    o_b: Option<Binding>,
}

/// Required-lane counterpart to [`AnnotationEdgeProbeOperator`] for ledgers
/// WITHOUT a sealed annotation arena (bulk-imported roots): instead of
/// per-row `f:reifies*` joins — whose planned chain drives from a
/// bound-object probe per driving row — drain the three reifies predicates
/// ONCE into [`AnnotationSidecarMaps`] and answer every base-edge row by
/// hash lookup. Rows with no matching reifier are dropped (the required
/// chain's semantics: an unreified edge matches no `f:reifies*` row).
///
/// The base edge is NOT joined per driving row either: re-opening a scan
/// per row costs ~ms each (the KB `UNWIND` shape spent ~30 s over 1k rows
/// this way). Instead ONE unseeded planned scan of the base pattern runs
/// after the child drains, keeping only rows whose subject occurs in the
/// driving stream, and the surviving edges hash-join the driving rows.
/// The planner gates this operator on total ledger size (see
/// `build_single_graph_delegate`), so the sweep is bounded.
///
/// All scans here (sidecar + base) are ordinary planned scans, so overlay
/// novelty and policy filtering apply — unlike the arena path, no
/// empty-overlay or root-policy gate is needed.
pub struct HashAnnotationEdgeProbeOperator {
    child: BoxedOperator,
    /// The base-edge triple, executed as ONE unseeded planned scan.
    base: TriplePattern,
    ann_var: VarId,
    s_pos: EdgePos,
    p_pos: EdgePos,
    o_pos: EdgePos,
    r_subj: TriplePattern,
    r_pred: TriplePattern,
    r_obj: TriplePattern,
    stats: Option<Arc<StatsView>>,
    planning: PlanningContext,
    schema: Arc<[VarId]>,
    state: OperatorState,
    probed: bool,
    result_buffer: Vec<Vec<Binding>>,
    buffer_pos: usize,
}

impl HashAnnotationEdgeProbeOperator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        child: BoxedOperator,
        base: TriplePattern,
        ann_var: VarId,
        s_pos: EdgePos,
        p_pos: EdgePos,
        o_pos: EdgePos,
        r_subj: TriplePattern,
        r_pred: TriplePattern,
        r_obj: TriplePattern,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        // Child columns, then the base pattern's var positions the sweep
        // produces, then the reifier.
        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        for pos in [&s_pos, &p_pos, &o_pos] {
            if let EdgePos::Var(v) = pos {
                if !schema_vec.contains(v) {
                    schema_vec.push(*v);
                }
            }
        }
        if !schema_vec.contains(&ann_var) {
            schema_vec.push(ann_var);
        }
        let schema = Arc::from(schema_vec.into_boxed_slice());
        Self {
            child,
            base,
            ann_var,
            s_pos,
            p_pos,
            o_pos,
            r_subj,
            r_pred,
            r_obj,
            stats,
            planning,
            schema,
            state: OperatorState::Created,
            probed: false,
            result_buffer: Vec::new(),
            buffer_pos: 0,
        }
    }

    /// One pass over the base pattern via an unseeded planned scan,
    /// keeping rows whose subject key occurs in `driving` (or every row
    /// when `keep_all` — some driving row leaves the subject unbound).
    async fn sweep_base_edges(
        &self,
        ctx: &ExecutionContext<'_>,
        view: Option<&fluree_db_binary_index::BinaryGraphView>,
        driving: &std::collections::HashSet<GroupKeyOwned>,
        keep_all: bool,
    ) -> Result<HashMap<GroupKeyOwned, Vec<SweptEdge>>> {
        let store = view.map(fluree_db_binary_index::BinaryGraphView::store);
        let mut op = crate::execute::build_where_operators_seeded(
            None,
            std::slice::from_ref(&Pattern::Triple(self.base.clone())),
            self.stats.clone(),
            None,
            &self.planning,
        )?;
        let mut edges: HashMap<GroupKeyOwned, Vec<SweptEdge>> = HashMap::new();
        op.open(ctx).await?;
        while let Some(batch) = op.next_batch(ctx).await? {
            ctx.check_cancelled()?;
            for row in 0..batch.len() {
                let (s_key, s_b) = match &self.s_pos {
                    EdgePos::Const(sid) => (
                        binding_to_group_key_normalized(&Binding::sid(sid.clone()), store, view),
                        None,
                    ),
                    EdgePos::Var(v) => {
                        let b = batch.get(row, *v).cloned().unwrap_or(Binding::Unbound);
                        (binding_to_group_key_normalized(&b, store, view), Some(b))
                    }
                };
                if matches!(s_key, GroupKeyOwned::Absent)
                    || (!keep_all && !driving.contains(&s_key))
                {
                    continue;
                }
                let Some(s_sid) = resolve_pos_ref(&batch, row, &self.s_pos, view)? else {
                    continue;
                };
                let Some(p_sid) = resolve_pos_pred(&batch, row, &self.p_pos, view)? else {
                    continue;
                };
                let o_key = row_obj_key(&batch, row, &self.o_pos, view);
                if matches!(o_key, GroupKeyOwned::Absent) {
                    continue;
                }
                let var_binding = |pos: &EdgePos| match pos {
                    EdgePos::Const(_) => None,
                    EdgePos::Var(v) => {
                        Some(batch.get(row, *v).cloned().unwrap_or(Binding::Unbound))
                    }
                };
                let edge = SweptEdge {
                    s_sid,
                    p_sid,
                    o_key,
                    s_b,
                    p_b: var_binding(&self.p_pos),
                    o_b: var_binding(&self.o_pos),
                };
                edges.entry(s_key).or_default().push(edge);
            }
        }
        op.close();
        Ok(edges)
    }

    /// Emit one output row: child bindings, then the base edge's var
    /// positions, then the reifier.
    fn emit_row(&mut self, child_batch: &Batch, child_row: usize, edge: &SweptEdge, ann: &Sid) {
        let mut rb = Vec::with_capacity(self.schema.len());
        for var in self.schema.iter() {
            let binding = if child_batch.schema().contains(var) {
                let b = child_batch
                    .get(child_row, *var)
                    .cloned()
                    .unwrap_or(Binding::Unbound);
                // An unbound driving subject takes the edge's value (the
                // join binds it), like any join would.
                if matches!(b, Binding::Unbound) {
                    self.pos_binding_for_var(*var, edge)
                        .unwrap_or(Binding::Unbound)
                } else {
                    b
                }
            } else if *var == self.ann_var {
                Binding::sid(ann.clone())
            } else {
                self.pos_binding_for_var(*var, edge)
                    .unwrap_or(Binding::Unbound)
            };
            rb.push(binding);
        }
        self.result_buffer.push(rb);
    }

    /// The swept edge's binding for a base-pattern var, if `var` is one of
    /// its positions.
    fn pos_binding_for_var(&self, var: VarId, edge: &SweptEdge) -> Option<Binding> {
        for (pos, b) in [
            (&self.s_pos, &edge.s_b),
            (&self.p_pos, &edge.p_b),
            (&self.o_pos, &edge.o_b),
        ] {
            if matches!(pos, EdgePos::Var(v) if *v == var) {
                return b.clone();
            }
        }
        None
    }

    /// Drain the whole child stream, build the sidecar maps and the
    /// base-edge sweep once, and fill `result_buffer` with each driving
    /// row hash-joined to its reified edges (rows with none are dropped).
    /// The child drains first so an empty driving stream never pays for
    /// either sweep.
    async fn probe_all(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let view = ctx.graph_view();
        let view = view.as_ref();
        let store = view.map(fluree_db_binary_index::BinaryGraphView::store);

        let mut child_batches: Vec<Batch> = Vec::new();
        let mut driving: std::collections::HashSet<GroupKeyOwned> =
            std::collections::HashSet::new();
        let mut keep_all = false;
        while let Some(batch) = self.child.next_batch(ctx).await? {
            ctx.check_cancelled()?;
            if batch.is_empty() {
                continue;
            }
            for row in 0..batch.len() {
                match &self.s_pos {
                    EdgePos::Const(sid) => {
                        driving.insert(binding_to_group_key_normalized(
                            &Binding::sid(sid.clone()),
                            store,
                            view,
                        ));
                    }
                    EdgePos::Var(v) => match batch.get(row, *v) {
                        None | Some(Binding::Unbound) => keep_all = true,
                        Some(b) => {
                            driving.insert(binding_to_group_key_normalized(b, store, view));
                        }
                    },
                }
            }
            child_batches.push(batch);
        }
        if child_batches.is_empty() {
            return Ok(());
        }

        let maps = AnnotationSidecarMaps::build(
            &self.r_subj,
            &self.r_pred,
            &self.r_obj,
            self.stats.clone(),
            &self.planning,
            ctx,
            view,
        )
        .await?;
        let edges = self.sweep_base_edges(ctx, view, &driving, keep_all).await?;

        for batch in std::mem::take(&mut child_batches) {
            ctx.check_cancelled()?;
            for row in 0..batch.len() {
                // A child-bound var position constrains the join like the
                // per-row substitution it replaces: the edge must carry the
                // SAME value there. Unbound positions are free — the edge
                // binds them.
                let row_key = |pos: &EdgePos| -> Option<GroupKeyOwned> {
                    match pos {
                        EdgePos::Const(_) => None, // constrained by the scan pattern itself
                        EdgePos::Var(v) => match batch.get(row, *v) {
                            None | Some(Binding::Unbound) => None,
                            Some(b) => Some(binding_to_group_key_normalized(b, store, view)),
                        },
                    }
                };
                let s_key = row_key(&self.s_pos);
                // Predicate compares in Sid space (a normalized key would
                // straddle the predicate/subject id spaces).
                let p_bound_sid = match &self.p_pos {
                    EdgePos::Const(_) => None,
                    EdgePos::Var(v) => match batch.get(row, *v) {
                        None | Some(Binding::Unbound) => None,
                        Some(b) => binding_sid(b, view)?,
                    },
                };
                let o_key = row_key(&self.o_pos);
                let row_edges: Vec<&SweptEdge> = match &s_key {
                    // Unbound driving subject: every swept edge is a candidate.
                    None => edges.values().flatten().collect(),
                    Some(k) => edges.get(k).map(|v| v.iter().collect()).unwrap_or_default(),
                };
                for edge in row_edges {
                    if p_bound_sid.as_ref().is_some_and(|ps| *ps != edge.p_sid) {
                        continue;
                    }
                    if o_key.as_ref().is_some_and(|k| *k != edge.o_key) {
                        continue;
                    }
                    let Some(cands) = maps.s_to_anns.get(&edge.s_sid) else {
                        continue;
                    };
                    let matching: Vec<Sid> = cands
                        .iter()
                        .filter(|ann| maps.matches(ann, &edge.p_sid, &edge.o_key))
                        .cloned()
                        .collect();
                    for ann in matching {
                        self.emit_row(&batch, row, edge, &ann);
                    }
                }
            }
        }
        tracing::debug!(
            driving = driving.len(),
            rows = self.result_buffer.len(),
            "annotation required-lane hash probe complete"
        );
        Ok(())
    }

    /// Emit up to [`PROBE_OUTPUT_CHUNK`] buffered output rows as one batch.
    fn drain_chunk(&mut self) -> Option<Batch> {
        if self.buffer_pos >= self.result_buffer.len() {
            return None;
        }
        let end = (self.buffer_pos + PROBE_OUTPUT_CHUNK).min(self.result_buffer.len());
        let num_cols = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(end - self.buffer_pos))
            .collect();
        for row in &self.result_buffer[self.buffer_pos..end] {
            for (col, b) in row.iter().enumerate() {
                if col < columns.len() {
                    columns[col].push(b.clone());
                }
            }
        }
        self.buffer_pos = end;
        if columns.is_empty() || columns[0].is_empty() {
            return None;
        }
        Batch::new(self.schema.clone(), columns).ok()
    }
}

#[async_trait]
impl Operator for HashAnnotationEdgeProbeOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.probed = false;
        self.result_buffer.clear();
        self.buffer_pos = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        if !self.probed {
            self.probe_all(ctx).await?;
            self.probed = true;
        }
        let out = self.drain_chunk();
        if out.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(out)
    }

    fn close(&mut self) {
        self.child.close();
        self.result_buffer.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // ~1 reifier per base edge.
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::TriplePattern;
    use fluree_vocab::db::{REIFIES_OBJECT, REIFIES_PREDICATE, REIFIES_SUBJECT};
    use fluree_vocab::namespaces::FLUREE_DB;

    fn v(n: u16) -> VarId {
        VarId(n)
    }
    fn pred(name: &str) -> Sid {
        Sid::new(FLUREE_DB, name)
    }
    fn user_sid(n: u16, name: &str) -> Sid {
        Sid::new(n, name)
    }

    /// Canonical expanded chain for `(friend)<-[m:HAS_MEMBER]-(forum)`
    /// plus a `m.joinDate` body read, as `expand_edge_annotation_patterns`
    /// would emit it: base edge, the three reifies triples, then body.
    fn canonical_chain() -> Vec<Pattern> {
        let forum = v(1);
        let friend = v(2);
        let ann = v(3);
        let jd = v(4);
        let has_member = Ref::Sid(user_sid(15, "HAS_MEMBER"));
        let base = TriplePattern {
            s: Ref::Var(forum),
            p: has_member.clone(),
            o: Term::Var(friend),
            dtc: None,
        };
        let r_subj = TriplePattern {
            s: Ref::Var(ann),
            p: Ref::Sid(pred(REIFIES_SUBJECT)),
            o: Term::Var(forum),
            dtc: None,
        };
        let r_pred = TriplePattern {
            s: Ref::Var(ann),
            p: Ref::Sid(pred(REIFIES_PREDICATE)),
            o: Term::from(has_member),
            dtc: None,
        };
        let r_obj = TriplePattern {
            s: Ref::Var(ann),
            p: Ref::Sid(pred(REIFIES_OBJECT)),
            o: Term::Var(friend),
            dtc: None,
        };
        let body = TriplePattern {
            s: Ref::Var(ann),
            p: Ref::Sid(user_sid(16, "joinDate")),
            o: Term::Var(jd),
            dtc: None,
        };
        vec![
            Pattern::Triple(base),
            Pattern::Triple(r_subj),
            Pattern::Triple(r_pred),
            Pattern::Triple(r_obj),
            Pattern::Triple(body),
        ]
    }

    #[test]
    fn recognizes_canonical_edge_annotation_chain() {
        let shape = recognize_annotation_edge(&canonical_chain()).expect("should recognize");
        assert_eq!(shape.ann_var, v(3));
        assert_eq!(shape.p_pred, Ref::Sid(user_sid(15, "HAS_MEMBER")));
        assert!(matches!(shape.s_pos, EdgePos::Var(x) if x == v(1)));
        assert!(matches!(shape.o_pos, EdgePos::Var(x) if x == v(2)));
        assert_eq!(shape.body.len(), 1, "joinDate read stays in body");
    }

    #[test]
    fn recognizes_iri_predicate_the_way_cypher_lowers_it() {
        // Cypher lowers a typed relationship to a `Ref::Iri` predicate;
        // the reifiesPredicate triple's object is the same IRI.
        let mut chain = canonical_chain();
        let iri: Arc<str> = Arc::from("http://ldbc.example/HAS_MEMBER");
        if let Pattern::Triple(t) = &mut chain[0] {
            t.p = Ref::Iri(iri.clone());
        }
        if let Pattern::Triple(t) = &mut chain[2] {
            t.o = Term::Iri(iri.clone());
        }
        let shape = recognize_annotation_edge(&chain).expect("should recognize iri pred");
        assert_eq!(shape.p_pred, Ref::Iri(iri));
    }

    #[test]
    fn rejects_when_reifies_objects_do_not_match_base_edge() {
        let mut chain = canonical_chain();
        // Corrupt reifiesObject to point at the wrong var.
        if let Pattern::Triple(t) = &mut chain[3] {
            t.o = Term::Var(v(99));
        }
        assert!(recognize_annotation_edge(&chain).is_none());
    }

    #[test]
    fn rejects_variable_predicate() {
        let mut chain = canonical_chain();
        if let Pattern::Triple(t) = &mut chain[0] {
            t.p = Ref::Var(v(50));
        }
        assert!(recognize_annotation_edge(&chain).is_none());
    }

    #[test]
    fn rejects_mismatched_reifier_var() {
        let mut chain = canonical_chain();
        // reifiesPredicate uses a different reifier subject var.
        if let Pattern::Triple(t) = &mut chain[2] {
            t.s = Ref::Var(v(77));
        }
        assert!(recognize_annotation_edge(&chain).is_none());
    }

    #[test]
    fn rejects_too_short_chain() {
        let chain = canonical_chain();
        assert!(recognize_annotation_edge(&chain[..3]).is_none());
    }

    #[test]
    fn annotation_sidecar_maps_preserve_multi_target_values() {
        let ann = Sid::new(1, "ann");
        let p1 = Sid::new(2, "p1");
        let p2 = Sid::new(2, "p2");
        let ok = |s: &Sid| binding_to_group_key_normalized(&Binding::sid(s.clone()), None, None);
        let o1 = ok(&Sid::new(3, "o1"));
        let o2 = ok(&Sid::new(3, "o2"));
        let maps = AnnotationSidecarMaps {
            s_to_anns: HashMap::new(),
            ann_preds: HashMap::from([(ann.clone(), vec![p1.clone(), p2.clone()])]),
            ann_objs: HashMap::from([(ann.clone(), vec![o1.clone(), o2.clone()])]),
        };

        assert!(maps.matches(&ann, &p1, &o1));
        assert!(maps.matches(&ann, &p2, &o2));
    }
}
