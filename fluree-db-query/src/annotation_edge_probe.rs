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
use crate::ir::{Pattern, Ref, Term};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::annotation_arena::AnnotationArenaReader;
use fluree_db_core::edge::{id_datatype_sid, EdgeKey};
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{AnnotationIndexRoot, FlakeValue, Sid};
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

    // Predicate must be a constant relationship type (a typed Cypher
    // relationship lowers to `Ref::Iri`; a `Ref::Sid` is also accepted).
    // A variable predicate is not a fixed edge type → fall back.
    let p_pred = match &base.p {
        Ref::Iri(_) | Ref::Sid(_) => base.p.clone(),
        Ref::Var(_) => return None,
    };
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
    /// Base-edge predicate (constant for a typed relationship).
    p_sid: Sid,
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
        p_sid: Sid,
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
            p_sid,
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
        let Some(s) = self.resolve_ref(batch, row, &self.s_pos, view)? else {
            return Ok(None);
        };
        // Object: ref-valued for a relationship edge. Resolve to a Sid
        // and wrap as a ref FlakeValue with the `@id` datatype, matching
        // how the arena stored the edge.
        let Some(o) = self.resolve_ref(batch, row, &self.o_pos, view)? else {
            return Ok(None);
        };
        Ok(Some(EdgeKey {
            g: None,
            s,
            p: self.p_sid.clone(),
            o: FlakeValue::Ref(o),
            dt: id_datatype_sid(),
            lang: None,
            list_i: None,
        }))
    }

    /// Resolve an edge position to a concrete `Sid`. Handles the two
    /// ref-valued binding representations a base-edge scan can emit:
    /// eagerly-resolved `Sid` and late-materialized `EncodedSid`. The
    /// latter is decoded **directly** through the subject dictionary
    /// (`BinaryGraphView::resolve_subject_sid`) — an IRI round-trip
    /// (`resolve_subject_iri` + `encode_iri`) silently returns `None` for
    /// subjects whose IRI doesn't re-encode, which would drop rows
    /// non-deterministically (a subject may arrive eager or late depending
    /// on scan timing). A failure here is a loud error, never a dropped row.
    fn resolve_ref(
        &self,
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
}
