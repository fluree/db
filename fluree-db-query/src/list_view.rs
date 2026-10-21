//! SPARQL's view of RDF lists.
//!
//! A list stored compactly — its members values of the owning subject and
//! predicate, each carrying its position — is, as RDF, an owner edge
//! `s p head` and a chain of cells: `cellK rdf:first memberK` and
//! `cellK rdf:rest` the next cell or `rdf:nil`. SPARQL sees that graph: a
//! member is not a value of its predicate. The cells, which nothing stores,
//! are blank nodes the request names (`_:l{list}-{position}`); they join and
//! render like any blank node, but name nothing in a later request. JSON-LD
//! queries keep Fluree's view, where the members are the values.
//!
//! [`rewrite_patterns`] turns each triple pattern a list can match — on a
//! predicate holding list members, on `rdf:first` / `rdf:rest`, or on a
//! variable predicate — into a [`ListViewPattern`], which
//! [`ListViewOperator`] answers with the pattern's stored triples that are not
//! list members and the list triples that match it. Every other pattern is
//! planned as before. Lists stored as explicit `rdf:first`/`rdf:rest` chains
//! are stored triples, and read as such.
//!
//! Over a dataset spanning ledgers the view is not applied: their subjects are
//! not comparable, so such a query sees the members as values.

use crate::binary_scan::{BinaryScanOperator, EmitMask};
use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, PathGraph};
use crate::error::{QueryError, Result};
use crate::ir::{Pattern, Ref, Term, TriplePattern};
use crate::join::NestedLoopJoinOperator;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::BatchSeedOperator;
use crate::temporal_mode::TemporalMode;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::namespaces::{is_rdf_first, is_rdf_rest};
use fluree_db_core::{
    range_with_overlay, DatatypeConstraint, Flake, FlakeValue, GraphId, IndexType, LedgerSnapshot,
    OverlayProvider, RangeMatch, RangeOptions, RangeTest, Sid,
};
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::predicates::{RDF_FIRST, RDF_NIL, RDF_REST};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

/// A list, named by the graph, subject and predicate that own it.
type Owner = (GraphId, Sid, Sid);

/// A list and its members in position order.
type List = (Owner, Vec<Flake>);

/// Which predicates hold list members, and the cells one request has named.
#[derive(Debug)]
pub struct ListView {
    /// Predicates whose values include list members; `None` when that is
    /// not known, and any may.
    predicates: Option<HashSet<Sid>>,
    /// The lists this request has named a cell of: list `n` is `owners[n]`.
    lists: Mutex<ListTable>,
    /// The lists each read of this request found, so a walk along a list's
    /// cells reads it once.
    reads: Mutex<HashMap<ListRead, Arc<[List]>>>,
}

#[derive(Debug, Default)]
struct ListTable {
    ids: HashMap<Owner, usize>,
    owners: Vec<Owner>,
}

/// The graph, its `t`, and the subject and predicate a list read asks for.
type ListRead = (GraphId, i64, Want, Want);

impl ListView {
    /// The view for a query of `snapshot` with `overlay`, or `None` when the
    /// ledger provably holds no list members.
    pub fn for_db(
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        store: Option<&Arc<BinaryIndexStore>>,
    ) -> Result<Option<Arc<Self>>> {
        let mut predicates: Option<HashSet<Sid>> = Some(HashSet::new());
        if snapshot.has_list_meta != Some(false) {
            predicates = match store {
                Some(store) => {
                    let ids = store
                        .list_predicate_ids()
                        .map_err(|e| QueryError::from_io("list predicates", e))?;
                    Some(ids.iter().filter_map(|&p| store.predicate_sid(p)).collect())
                }
                None => None,
            };
        }
        match (predicates.as_mut(), overlay.list_predicates()) {
            (Some(known), Some(novel)) => known.extend(novel.iter().cloned()),
            (_, None) => predicates = None,
            (None, Some(_)) => {}
        }
        if predicates.as_ref().is_some_and(HashSet::is_empty) {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            predicates,
            lists: Mutex::new(ListTable::default()),
            reads: Mutex::new(HashMap::new()),
        })))
    }

    /// The predicates holding list members, when known.
    pub(crate) fn predicates(&self) -> Option<&HashSet<Sid>> {
        self.predicates.as_ref()
    }

    pub(crate) fn may_hold_lists(&self, p: &Sid) -> bool {
        self.predicates.as_ref().is_none_or(|ps| ps.contains(p))
    }

    /// Whether a triple on predicate `p` can be a list triple: an owner edge
    /// or a cell's `rdf:first` / `rdf:rest`.
    fn matches_predicate(&self, p: &Sid) -> bool {
        self.may_hold_lists(p) || is_rdf_first(p) || is_rdf_rest(p)
    }

    /// The blank node for cell `k` of `owner`'s list.
    fn cell(&self, owner: &Owner, k: usize) -> Sid {
        let mut table = self
            .lists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let n = match table.ids.get(owner) {
            Some(&n) => n,
            None => {
                let n = table.owners.len();
                table.owners.push(owner.clone());
                table.ids.insert(owner.clone(), n);
                n
            }
        };
        Sid::new(BLANK_NODE, format!("l{n}-{k}"))
    }

    /// The list and position `sid` names, when it is a cell of this request.
    fn decode(&self, sid: &Sid) -> Option<(Owner, usize)> {
        if sid.namespace_code != BLANK_NODE {
            return None;
        }
        let (n, k) = sid.name.strip_prefix('l')?.split_once('-')?;
        let (n, k): (usize, usize) = (n.parse().ok()?, k.parse().ok()?);
        let table = self
            .lists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Some((table.owners.get(n)?.clone(), k))
    }
}

/// A triple pattern answered in SPARQL's view of lists (see the
/// [module docs](self)).
#[derive(Debug, Clone)]
pub struct ListViewPattern {
    pub triple: TriplePattern,
    pub view: Arc<ListView>,
}

impl ListViewPattern {
    pub fn referenced_vars(&self) -> Vec<VarId> {
        self.triple.referenced_vars()
    }

    pub fn produced_vars(&self) -> Vec<VarId> {
        self.triple.produced_vars()
    }
}

/// Answer every triple pattern of `patterns` a list can match in `view`
/// (see the [module docs](self)). `encode` resolves a predicate IRI the
/// patterns did not encode.
pub fn rewrite_patterns(
    patterns: Vec<Pattern>,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) -> Vec<Pattern> {
    patterns
        .into_iter()
        .map(|p| rewrite_pattern(p, view, encode))
        .collect()
}

/// `patterns` as a query of `snapshot` with `overlay` runs them: in SPARQL's
/// view of lists, unless the ledger provably holds no list members.
pub fn apply(
    patterns: Vec<Pattern>,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    store: Option<&Arc<BinaryIndexStore>>,
) -> Result<Vec<Pattern>> {
    let store = store
        .cloned()
        .or_else(|| crate::context::ExecutionContext::extract_binary_store(snapshot));
    Ok(match ListView::for_db(snapshot, overlay, store.as_ref())? {
        Some(view) => rewrite_patterns(patterns, &view, &|iri| snapshot.encode_iri(iri)),
        None => patterns,
    })
}

fn rewrite_pattern(
    pattern: Pattern,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) -> Pattern {
    match pattern {
        Pattern::Triple(tp) => {
            let matches = match &tp.p {
                Ref::Var(_) => true,
                Ref::Sid(p) => view.matches_predicate(p),
                Ref::Iri(iri) => encode(iri).is_some_and(|p| view.matches_predicate(&p)),
            };
            if matches {
                Pattern::ListView(ListViewPattern {
                    triple: tp,
                    view: Arc::clone(view),
                })
            } else {
                Pattern::Triple(tp)
            }
        }
        Pattern::PropertyPath(mut pp) => {
            let steps = pp
                .sequence_steps
                .iter()
                .flat_map(|step| step.predicates.iter());
            if pp
                .predicates
                .iter()
                .chain(steps)
                .any(|p| view.matches_predicate(p))
            {
                pp.list_view = Some(Arc::clone(view));
            }
            Pattern::PropertyPath(pp)
        }
        // A remote endpoint answers its own patterns.
        Pattern::Service(_) => pattern,
        other => other.map_subpatterns(&mut |xs| rewrite_patterns(xs, view, encode)),
    }
}

/// Whether `patterns` hold a [`ListViewPattern`] anywhere.
pub fn has_list_view(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|p| match p {
        Pattern::ListView(_) => true,
        Pattern::Optional(xs)
        | Pattern::Minus(xs)
        | Pattern::Exists(xs)
        | Pattern::NotExists(xs)
        | Pattern::Graph { patterns: xs, .. }
        | Pattern::DefaultGraphSource { patterns: xs } => has_list_view(xs),
        Pattern::Union(branches) => branches.iter().any(|b| has_list_view(b)),
        Pattern::Subquery(sq) => has_list_view(&sq.patterns),
        Pattern::EdgeAnnotation { body, .. } | Pattern::AnnotationTarget { body, .. } => {
            has_list_view(body)
        }
        _ => false,
    })
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

/// Joins its child with a [`ListViewPattern`]: the pattern's stored triples
/// that are not list members, through a nested-loop join that hides them,
/// and the list triples that match it, computed here.
pub struct ListViewOperator {
    child: BoxedOperator,
    pattern: ListViewPattern,
    mode: TemporalMode,
    schema: Arc<[VarId]>,
    state: OperatorState,
    pending: VecDeque<Batch>,
}

impl ListViewOperator {
    pub fn new(child: BoxedOperator, pattern: ListViewPattern, mode: TemporalMode) -> Self {
        let mut schema: Vec<VarId> = child.schema().to_vec();
        let t = &pattern.triple;
        for v in [t.s.as_var(), t.p.as_var(), t.o.as_var()]
            .into_iter()
            .flatten()
        {
            if !schema.contains(&v) {
                schema.push(v);
            }
        }
        Self {
            child,
            pattern,
            mode,
            schema: schema.into(),
            state: OperatorState::Created,
            pending: VecDeque::new(),
        }
    }

    /// `input` joined with the pattern's stored triples that are not list
    /// members.
    async fn stored(&self, ctx: &ExecutionContext<'_>, input: &Batch) -> Result<Vec<Batch>> {
        let seed = BatchSeedOperator::from_batch(input.clone());
        let mut join = NestedLoopJoinOperator::new(
            Box::new(seed),
            input.schema_arc(),
            self.pattern.triple.clone(),
            None,
            Vec::new(),
            EmitMask::ALL,
            self.mode,
        );
        if !ctx.is_multi_ledger() {
            join = join.hiding_list_members();
        }
        join.open(ctx).await?;
        let mut out = Vec::new();
        while let Some(batch) = join.next_batch(ctx).await? {
            if !batch.is_empty() {
                out.push(batch);
            }
        }
        join.close();
        Ok(out)
    }

    /// `input` joined with the list triples that match the pattern.
    async fn lists(&self, ctx: &ExecutionContext<'_>, input: &Batch) -> Result<Option<Batch>> {
        if ctx.is_multi_ledger() {
            return Ok(None);
        }
        let graphs = ctx.path_graphs()?;
        let t = &self.pattern.triple;
        let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); self.schema.len()];
        let mut rows = 0;
        let mut cache: HashMap<(Want, Want, ObjectWant), Vec<ListTriple>> = HashMap::new();
        for row in 0..input.len() {
            let bound = |var: Option<VarId>| var.and_then(|v| input.get(row, v));
            let s = node_want(ctx, bound(t.s.as_var()), &t.s);
            let p = node_want(ctx, bound(t.p.as_var()), &t.p);
            let o = object_want(ctx, bound(t.o.as_var()), &t.o, t.dtc.as_ref());
            let key = (s.clone(), p.clone(), o.clone());
            if !cache.contains_key(&key) {
                let found = self.list_triples(ctx, &graphs, &s, &p, &o).await?;
                cache.insert(key.clone(), found);
            }
            let width = input.schema().len();
            for triple in &cache[&key] {
                let mut values: Vec<Binding> = (0..width)
                    .map(|c| input.get_by_col(row, c).clone())
                    .collect();
                let mut ok = true;
                for (var, value) in [
                    (t.s.as_var(), Binding::sid(triple.s.clone())),
                    (t.p.as_var(), Binding::sid(triple.p.clone())),
                    (t.o.as_var(), triple.o.clone()),
                ] {
                    let Some(var) = var else {
                        continue;
                    };
                    let col = self
                        .schema
                        .iter()
                        .position(|v| *v == var)
                        .expect("in schema");
                    if col < width {
                        // The input's value, which the triple was found by;
                        // an unbound one takes the triple's.
                        if values[col] == Binding::Unbound {
                            values[col] = value;
                        }
                    } else if col < values.len() {
                        // A variable this triple repeats (`?x ?p ?x`).
                        ok &= values[col] == value;
                    } else {
                        values.push(value);
                    }
                }
                if !ok {
                    continue;
                }
                for (col, value) in columns.iter_mut().zip(values) {
                    col.push(value);
                }
                rows += 1;
            }
        }
        if rows == 0 {
            return Ok(None);
        }
        Ok(Some(Batch::new(Arc::clone(&self.schema), columns)?))
    }

    /// The list triples matching `s p o` in `graphs`.
    async fn list_triples(
        &self,
        ctx: &ExecutionContext<'_>,
        graphs: &[PathGraph<'_>],
        s: &Want,
        p: &Want,
        o: &ObjectWant,
    ) -> Result<Vec<ListTriple>> {
        let view = &self.pattern.view;
        if matches!(s, Want::Nothing) || matches!(p, Want::Nothing) {
            return Ok(Vec::new());
        }
        let (first, rest) = (Sid::new(RDF, RDF_FIRST), Sid::new(RDF, RDF_REST));
        let cell = match s {
            Want::Node(sid) => view.decode(sid),
            _ => None,
        };
        let mut out = Vec::new();
        for graph in graphs {
            // Owner edges: `s p head`, for a subject that is not a cell.
            let owner_edges = match p {
                Want::Any => true,
                Want::Node(p) => view.may_hold_lists(p),
                Want::Nothing => false,
            };
            if owner_edges && cell.is_none() {
                for (owner, _) in lists_in(ctx, graph, view, s, p).await?.iter() {
                    let head = view.cell(owner, 0);
                    out.push(ListTriple {
                        s: owner.1.clone(),
                        p: owner.2.clone(),
                        o: Binding::sid(head),
                    });
                }
            }
            // Cells: `cellK rdf:first memberK ; rdf:rest next`.
            let (want_first, want_rest) = match p {
                Want::Any => (true, true),
                Want::Node(p) => (is_rdf_first(p), is_rdf_rest(p)),
                Want::Nothing => (false, false),
            };
            if !(want_first || want_rest) {
                continue;
            }
            let lists = match (s, &cell) {
                (_, Some((owner, _))) => {
                    if owner.0 != graph.g_id {
                        continue;
                    }
                    let at = Want::Node(owner.1.clone());
                    let of = Want::Node(owner.2.clone());
                    lists_in(ctx, graph, view, &at, &of).await?
                }
                (Want::Node(_), None) => continue,
                _ => lists_in(ctx, graph, view, &Want::Any, &Want::Any).await?,
            };
            for (owner, members) in lists.iter() {
                let positions = match &cell {
                    Some((_, k)) => *k..members.len().min(k + 1),
                    None => 0..members.len(),
                };
                for k in positions {
                    let member = &members[k];
                    let here = view.cell(owner, k);
                    if want_first {
                        out.push(ListTriple {
                            s: here.clone(),
                            p: first.clone(),
                            o: member_binding(member),
                        });
                    }
                    if want_rest {
                        let next = if k + 1 == members.len() {
                            Sid::new(RDF, RDF_NIL)
                        } else {
                            view.cell(owner, k + 1)
                        };
                        out.push(ListTriple {
                            s: here,
                            p: rest.clone(),
                            o: Binding::sid(next),
                        });
                    }
                }
            }
        }
        out.retain(|t| o.matches(&t.o));
        Ok(out)
    }
}

/// A list triple: an owner edge or a cell's `rdf:first` / `rdf:rest`.
#[derive(Debug, Clone)]
struct ListTriple {
    s: Sid,
    p: Sid,
    o: Binding,
}

/// What a subject or predicate position asks for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Want {
    Any,
    Node(Sid),
    /// A term that is no node, or one the ledger cannot name: matches nothing.
    Nothing,
}

/// What an object position asks for.
#[derive(Debug, Clone, PartialEq)]
enum ObjectWant {
    Any,
    Node(Sid),
    Value(FlakeValue, Option<DatatypeConstraint>),
    Nothing,
}

impl Eq for ObjectWant {}

impl std::hash::Hash for ObjectWant {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        if let ObjectWant::Node(sid) = self {
            sid.hash(state);
        }
    }
}

impl ObjectWant {
    fn matches(&self, o: &Binding) -> bool {
        match (self, o) {
            (ObjectWant::Any, _) => true,
            (ObjectWant::Node(want), Binding::Sid { sid, .. }) => want == sid,
            (ObjectWant::Value(want, dtc), Binding::Lit { val, dtc: have, .. }) => {
                want == val && dtc.as_ref().is_none_or(|d| d == have)
            }
            _ => false,
        }
    }
}

fn decoded(ctx: &ExecutionContext<'_>, b: &Binding) -> Binding {
    crate::group_aggregate::materialize_encoded(b, ctx.graph_view().as_ref())
}

fn iri_sid(ctx: &ExecutionContext<'_>, iri: &str) -> Want {
    match ctx.active_snapshot.encode_iri(iri) {
        Some(sid) => Want::Node(sid),
        None => Want::Nothing,
    }
}

fn node_want(ctx: &ExecutionContext<'_>, bound: Option<&Binding>, term: &Ref) -> Want {
    match (bound, term) {
        (Some(b), _) => match decoded(ctx, b) {
            Binding::Unbound => Want::Any,
            Binding::Sid { sid, .. } => Want::Node(sid),
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => iri_sid(ctx, &iri),
            _ => Want::Nothing,
        },
        (None, Ref::Var(_)) => Want::Any,
        (None, Ref::Sid(sid)) => Want::Node(sid.clone()),
        (None, Ref::Iri(iri)) => iri_sid(ctx, iri),
    }
}

fn object_want(
    ctx: &ExecutionContext<'_>,
    bound: Option<&Binding>,
    term: &Term,
    dtc: Option<&DatatypeConstraint>,
) -> ObjectWant {
    let node = |w: Want| match w {
        Want::Node(sid) => ObjectWant::Node(sid),
        Want::Any => ObjectWant::Any,
        Want::Nothing => ObjectWant::Nothing,
    };
    match (bound, term) {
        (Some(b), _) => match decoded(ctx, b) {
            Binding::Unbound => ObjectWant::Any,
            Binding::Sid { sid, .. } => ObjectWant::Node(sid),
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => node(iri_sid(ctx, &iri)),
            Binding::Lit { val, dtc, .. } => ObjectWant::Value(val, Some(dtc)),
            _ => ObjectWant::Nothing,
        },
        (None, Term::Var(_)) => ObjectWant::Any,
        (None, Term::Sid(sid)) => ObjectWant::Node(sid.clone()),
        (None, Term::Iri(iri)) => node(iri_sid(ctx, iri)),
        (None, Term::Value(FlakeValue::Ref(sid))) => ObjectWant::Node(sid.clone()),
        (None, Term::Value(v)) => ObjectWant::Value(v.clone(), dtc.cloned()),
    }
}

/// A list member as the binding of its value.
fn member_binding(member: &Flake) -> Binding {
    match &member.o {
        FlakeValue::Ref(r) => Binding::sid_with_t(r.clone(), member.t),
        v => {
            let dtc = match member.m.as_ref().and_then(|m| m.lang.as_ref()) {
                Some(lang) => DatatypeConstraint::LangTag(Arc::from(lang.as_str())),
                None => DatatypeConstraint::Explicit(member.dt.clone()),
            };
            Binding::Lit {
                val: v.clone(),
                dtc,
                t: Some(member.t),
                op: None,
                p_id: None,
            }
        }
    }
}

/// One property-path hop over list triples in `graph`: the nodes `node pred
/// ?next` reaches, or, `backward`, the nodes `?prev pred node` comes from.
pub(crate) async fn path_steps(
    ctx: &ExecutionContext<'_>,
    graph: &PathGraph<'_>,
    view: &ListView,
    node: &Sid,
    pred: &Sid,
    backward: bool,
) -> Result<Vec<Sid>> {
    let mut out = Vec::new();
    let cell = view.decode(node).filter(|(owner, _)| owner.0 == graph.g_id);
    let nil = Sid::new(RDF, RDF_NIL);
    let list_of = |owner: &Owner| (Want::Node(owner.1.clone()), Want::Node(owner.2.clone()));
    match (backward, &cell) {
        (false, Some((owner, k))) if is_rdf_first(pred) || is_rdf_rest(pred) => {
            let (s, p) = list_of(owner);
            for (found, members) in lists_in(ctx, graph, view, &s, &p).await?.iter() {
                if found != owner || *k >= members.len() {
                    continue;
                }
                if is_rdf_rest(pred) {
                    out.push(if k + 1 == members.len() {
                        nil.clone()
                    } else {
                        view.cell(owner, k + 1)
                    });
                } else if let FlakeValue::Ref(member) = &members[*k].o {
                    out.push(member.clone());
                }
            }
        }
        (false, None) if view.may_hold_lists(pred) => {
            let (s, p) = (Want::Node(node.clone()), Want::Node(pred.clone()));
            for (owner, _) in lists_in(ctx, graph, view, &s, &p).await?.iter() {
                out.push(view.cell(owner, 0));
            }
        }
        (true, Some((owner, k))) if is_rdf_rest(pred) && *k > 0 => {
            out.push(view.cell(owner, k - 1));
        }
        (true, Some((owner, 0))) if owner.2 == *pred => out.push(owner.1.clone()),
        (true, None) if is_rdf_rest(pred) && *node == nil => {
            for (owner, members) in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
                .await?
                .iter()
            {
                out.push(view.cell(owner, members.len() - 1));
            }
        }
        (true, None) if is_rdf_first(pred) => {
            for (owner, members) in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
                .await?
                .iter()
            {
                for (k, member) in members.iter().enumerate() {
                    if member.o == FlakeValue::Ref(node.clone()) {
                        out.push(view.cell(owner, k));
                    }
                }
            }
        }
        _ => {}
    }
    Ok(out)
}

/// Every list edge on `pred` in `graphs`, for a property path's closure:
/// owner edges for a predicate holding lists, a cell's next cell for
/// `rdf:rest`, and its member, when a node, for `rdf:first`.
pub(crate) async fn path_edges(
    ctx: &ExecutionContext<'_>,
    graphs: &[PathGraph<'_>],
    view: &ListView,
    pred: &Sid,
) -> Result<Vec<(Sid, Sid)>> {
    let mut out = Vec::new();
    let (first, rest) = (is_rdf_first(pred), is_rdf_rest(pred));
    for graph in graphs {
        if !(first || rest) {
            let p = Want::Node(pred.clone());
            for (owner, _) in lists_in(ctx, graph, view, &Want::Any, &p).await?.iter() {
                out.push((owner.1.clone(), view.cell(owner, 0)));
            }
            continue;
        }
        for (owner, members) in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
            .await?
            .iter()
        {
            for (k, member) in members.iter().enumerate() {
                let here = view.cell(owner, k);
                if rest {
                    let next = if k + 1 == members.len() {
                        Sid::new(RDF, RDF_NIL)
                    } else {
                        view.cell(owner, k + 1)
                    };
                    out.push((here, next));
                } else if let FlakeValue::Ref(m) = &member.o {
                    out.push((here, m.clone()));
                }
            }
        }
    }
    Ok(out)
}

/// Every cell in `graphs`, and `rdf:nil` when a list ends there: the nodes
/// the list triples add to a graph, for a zero-length path's universe.
pub(crate) async fn cell_nodes(
    ctx: &ExecutionContext<'_>,
    graphs: &[PathGraph<'_>],
    view: &ListView,
) -> Result<Vec<Sid>> {
    let mut out = Vec::new();
    for graph in graphs {
        for (owner, members) in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
            .await?
            .iter()
        {
            out.extend((0..members.len()).map(|k| view.cell(owner, k)));
        }
    }
    if !out.is_empty() {
        out.push(Sid::new(RDF, RDF_NIL));
    }
    Ok(out)
}

/// Whether `flake` is a list member a view hides from its predicate.
pub(crate) fn is_member(flake: &Flake) -> bool {
    flake.m.as_ref().is_some_and(|m| m.i.is_some())
}

/// The lists in `graph` owned by a subject matching `s` under a predicate
/// matching `p`, each with its members in position order; each read once per
/// request.
async fn lists_in(
    ctx: &ExecutionContext<'_>,
    graph: &PathGraph<'_>,
    view: &ListView,
    s: &Want,
    p: &Want,
) -> Result<Arc<[List]>> {
    let key: ListRead = (graph.g_id, graph.to_t, s.clone(), p.clone());
    let reads = || {
        view.reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    };
    if let Some(found) = reads().get(&key) {
        return Ok(Arc::clone(found));
    }
    let found: Arc<[List]> = read_lists_in(ctx, graph, view, s, p).await?.into();
    reads().insert(key, Arc::clone(&found));
    Ok(found)
}

async fn read_lists_in(
    ctx: &ExecutionContext<'_>,
    graph: &PathGraph<'_>,
    view: &ListView,
    s: &Want,
    p: &Want,
) -> Result<Vec<List>> {
    let reads: Vec<(IndexType, RangeMatch)> = match (s, p) {
        (Want::Nothing, _) | (_, Want::Nothing) => return Ok(Vec::new()),
        (Want::Node(s), Want::Node(p)) => {
            if !view.may_hold_lists(p) {
                return Ok(Vec::new());
            }
            vec![(
                IndexType::Spot,
                RangeMatch::new()
                    .with_subject(s.clone())
                    .with_predicate(p.clone()),
            )]
        }
        (Want::Node(s), Want::Any) => {
            vec![(IndexType::Spot, RangeMatch::new().with_subject(s.clone()))]
        }
        (Want::Any, Want::Node(p)) => {
            if !view.may_hold_lists(p) {
                return Ok(Vec::new());
            }
            vec![(IndexType::Psot, RangeMatch::new().with_predicate(p.clone()))]
        }
        (Want::Any, Want::Any) => match &view.predicates {
            Some(predicates) => predicates
                .iter()
                .map(|p| (IndexType::Psot, RangeMatch::new().with_predicate(p.clone())))
                .collect(),
            None => vec![(IndexType::Spot, RangeMatch::new())],
        },
    };
    let mut by_owner: Vec<List> = Vec::new();
    let mut slot: HashMap<Owner, usize> = HashMap::new();
    for (index, range_match) in reads {
        let flakes = range_with_overlay(
            graph.snapshot,
            graph.g_id,
            graph.overlay,
            index,
            RangeTest::Eq,
            range_match,
            RangeOptions::new().with_to_t(graph.to_t),
        )
        .await?;
        let members: Vec<Flake> = flakes
            .into_iter()
            .filter(|f| f.op && f.m.as_ref().is_some_and(|m| m.i.is_some()))
            .collect();
        ctx.tracker.consume_fuel(
            members.len() as u64 * fluree_db_core::tracking::schedule::PER_ROW_MICRO_FUEL,
        )?;
        let members = BinaryScanOperator::filter_flakes_by_enforcer(
            ctx,
            graph.policy_enforcer.as_ref(),
            graph.snapshot,
            graph.overlay,
            graph.to_t,
            graph.g_id,
            members,
        )
        .await?;
        for member in members {
            let owner = (graph.g_id, member.s.clone(), member.p.clone());
            let n = *slot.entry(owner.clone()).or_insert_with(|| {
                by_owner.push((owner, Vec::new()));
                by_owner.len() - 1
            });
            by_owner[n].1.push(member);
        }
    }
    for (_, members) in &mut by_owner {
        members.sort_by_key(|f| f.m.as_ref().and_then(|m| m.i));
    }
    Ok(by_owner)
}

#[async_trait]
impl Operator for ListViewOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.pending.clear();
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        loop {
            if let Some(batch) = self.pending.pop_front() {
                return Ok(Some(batch));
            }
            if self.state != OperatorState::Open {
                return Ok(None);
            }
            let Some(input) = self.child.next_batch(ctx).await? else {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            };
            if input.is_empty() {
                continue;
            }
            ctx.check_cancelled()?;
            let stored = self.stored(ctx, &input).await?;
            self.pending.extend(stored);
            if let Some(lists) = self.lists(ctx, &input).await? {
                self.pending.push_back(lists);
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.pending.clear();
        self.state = OperatorState::Closed;
    }
}
