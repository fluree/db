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

use crate::binary_scan::BinaryScanOperator;
use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, PathGraph};
use crate::error::{QueryError, Result};
use crate::ir::{Pattern, Ref, Term, TriplePattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A list, named by the graph, subject and predicate that own it.
pub type Owner = (GraphId, Sid, Sid);

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
    /// Whether each subject this request asked about may own a list, per
    /// graph and `t` ([`Self::may_meet`]).
    subjects: Mutex<HashMap<(GraphId, i64, Subject), bool>>,
}

/// A subject [`ListView::may_meet`] is asked about.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Subject {
    /// An index subject id; such bindings arise only over an empty overlay.
    Id(u64),
    Node(Sid),
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
            subjects: Mutex::new(HashMap::new()),
        })))
    }

    /// Whether a list triple can have `subject` as its subject in `graphs`:
    /// an unbound subject, a cell this request named, or a node with a list
    /// row in novelty or in the index. A `false` is exact.
    fn may_meet(
        &self,
        ctx: &ExecutionContext<'_>,
        graphs: &[PathGraph<'_>],
        subject: &Binding,
    ) -> Result<bool> {
        let subject = match subject {
            Binding::Unbound => return Ok(true),
            Binding::EncodedSid { s_id, .. } => Subject::Id(*s_id),
            Binding::Sid { sid, .. }
            | Binding::IriMatch {
                primary_sid: sid, ..
            } => {
                if self.decode(sid).is_some() {
                    return Ok(true);
                }
                Subject::Node(sid.clone())
            }
            Binding::Iri(iri) => match ctx.active_snapshot.encode_iri(iri) {
                Some(sid) => Subject::Node(sid),
                None => return Ok(false),
            },
            _ => return Ok(false),
        };
        let known = || {
            self.subjects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        };
        for graph in graphs {
            let key = (graph.g_id, graph.to_t, subject.clone());
            let cached = known().get(&key).copied();
            let held = match cached {
                Some(held) => held,
                None => {
                    let held = subject_may_hold_lists(graph, &subject)?;
                    known().insert(key, held);
                    held
                }
            };
            if held {
                return Ok(true);
            }
        }
        Ok(false)
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
    pub(crate) fn matches_predicate(&self, p: &Sid) -> bool {
        self.may_hold_lists(p) || is_rdf_first(p) || is_rdf_rest(p)
    }

    /// The blank node for cell `k` of `owner`'s list.
    pub fn cell(&self, owner: &Owner, k: usize) -> Sid {
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

    /// The list and position `sid` names, when it is a cell this request
    /// named.
    pub fn cell_of(&self, sid: &Sid) -> Option<(Owner, usize)> {
        self.decode(sid)
    }

    /// The members of `owner`'s list as this request read them, in position
    /// order: cell `k` holds member `k`.
    pub fn members_of(&self, owner: &Owner) -> Option<Vec<Flake>> {
        let reads = self
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reads
            .values()
            .flat_map(|lists| lists.iter())
            .find(|(o, _)| o == owner)
            .map(|(_, members)| members.clone())
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

/// The triples of an inner-join block that a [`ListView`] answers. They are
/// planned with the block like any triple, and their joins wrapped to hide
/// list members and add list triples.
#[derive(Debug, Clone, Default)]
pub struct BlockListViews {
    view: Option<Arc<ListView>>,
    triples: Vec<TriplePattern>,
}

impl BlockListViews {
    pub fn push(&mut self, lv: &ListViewPattern) {
        self.view.get_or_insert_with(|| Arc::clone(&lv.view));
        self.triples.push(lv.triple.clone());
    }

    /// The view answering `tp`, when it is one of the block's list-view
    /// triples.
    pub fn view_for(&self, tp: &TriplePattern) -> Option<&Arc<ListView>> {
        self.view.as_ref().filter(|_| self.triples.contains(tp))
    }

    pub fn is_empty(&self) -> bool {
        self.triples.is_empty()
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

/// `query` as it runs over `snapshot` with `overlay`: in SPARQL's view of
/// lists, unless the ledger provably holds no list members.
pub fn apply(
    query: &mut crate::ir::Query,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    store: Option<&Arc<BinaryIndexStore>>,
) -> Result<()> {
    if let Some(view) = view_for(snapshot, overlay, store)? {
        rewrite_query(query, &view, &|iri| snapshot.encode_iri(iri));
    }
    Ok(())
}

/// [`rewrite_patterns`] for every pattern of `query`: its WHERE and the
/// `EXISTS` its grouping and ordering expressions hold.
pub fn rewrite_query(
    query: &mut crate::ir::Query,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) {
    query.patterns = rewrite_patterns(std::mem::take(&mut query.patterns), view, encode);
    rewrite_modifiers_of(query, view, encode);
}

/// [`rewrite_query`] for the `EXISTS` of `query`'s grouping and ordering
/// expressions alone.
pub fn rewrite_modifiers_of(
    query: &mut crate::ir::Query,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) {
    rewrite_modifiers(&mut query.grouping, &mut query.order_binds, view, encode);
}

fn rewrite_modifiers(
    grouping: &mut Option<crate::ir::Grouping>,
    order_binds: &mut [(VarId, crate::ir::Expression)],
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) {
    if let Some(grouping) = grouping {
        grouping.for_each_expression_mut(&mut |expr| rewrite_expression(expr, view, encode));
    }
    for (_, expr) in order_binds {
        rewrite_expression(expr, view, encode);
    }
}

/// Whether `query` holds a [`ListViewPattern`] anywhere: in its WHERE, or
/// in an `EXISTS` of its grouping or ordering.
pub fn query_has_list_view(query: &crate::ir::Query) -> bool {
    has_list_view(&query.patterns) || modifiers_have_list_view(&query.grouping, &query.order_binds)
}

pub(crate) fn modifiers_have_list_view(
    grouping: &Option<crate::ir::Grouping>,
    order_binds: &[(VarId, crate::ir::Expression)],
) -> bool {
    let mut exprs: Vec<&crate::ir::Expression> = order_binds.iter().map(|(_, e)| e).collect();
    if let Some(grouping) = grouping {
        exprs.extend(grouping.having());
        exprs.extend(grouping.binds().map(|(_, e)| e));
    }
    exprs.into_iter().any(expression_has_list_view)
}

fn expression_has_list_view(expr: &crate::ir::Expression) -> bool {
    let mut found = false;
    expr.for_each_embedded_patterns(&mut |patterns| found |= has_list_view(patterns));
    found
}

/// [`ListView::for_db`], reading the snapshot's own index store when `store`
/// is not given.
pub fn view_for(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    store: Option<&Arc<BinaryIndexStore>>,
) -> Result<Option<Arc<ListView>>> {
    let store = store
        .cloned()
        .or_else(|| crate::context::ExecutionContext::extract_binary_store(snapshot));
    ListView::for_db(snapshot, overlay, store.as_ref())
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
            // A zero-length match pairs every node of the graph with itself,
            // the cells and `rdf:nil` included, whatever the path's predicates.
            let zero_length = matches!(
                pp.modifier,
                crate::ir::PathModifier::ZeroOrMore | crate::ir::PathModifier::ZeroOrOne
            );
            if zero_length || walks_lists(&pp, view) {
                pp.list_view = Some(Arc::clone(view));
            }
            Pattern::PropertyPath(pp)
        }
        // An `EXISTS` inside an expression reads the same graph.
        Pattern::Filter(mut expr) => {
            rewrite_expression(&mut expr, view, encode);
            Pattern::Filter(expr)
        }
        Pattern::Bind { var, mut expr } => {
            rewrite_expression(&mut expr, view, encode);
            Pattern::Bind { var, expr }
        }
        Pattern::Unwind { var, mut list } => {
            rewrite_expression(&mut list, view, encode);
            Pattern::Unwind { var, list }
        }
        Pattern::Subquery(mut sq) => {
            sq.patterns = rewrite_patterns(std::mem::take(&mut sq.patterns), view, encode);
            rewrite_modifiers(&mut sq.grouping, &mut sq.order_binds, view, encode);
            Pattern::Subquery(sq)
        }
        // A remote endpoint answers its own patterns.
        Pattern::Service(_) => pattern,
        other => other.map_subpatterns(&mut |xs| rewrite_patterns(xs, view, encode)),
    }
}

/// `patterns` with each list-view pattern its triple again, for a graph that
/// holds no RDF lists, such as an R2RML graph source.
pub fn without_list_view(patterns: &[Pattern]) -> Vec<Pattern> {
    patterns.iter().cloned().map(without_list_view_in).collect()
}

fn without_list_view_in(pattern: Pattern) -> Pattern {
    match pattern {
        Pattern::ListView(lv) => Pattern::Triple(lv.triple),
        other => {
            other.map_subpatterns(&mut |xs| xs.into_iter().map(without_list_view_in).collect())
        }
    }
}

/// Whether `pp` can step along a list triple in `view`.
pub(crate) fn walks_lists(pp: &crate::ir::PropertyPathPattern, view: &ListView) -> bool {
    let steps = pp
        .sequence_steps
        .iter()
        .flat_map(|step| step.predicates.iter());
    pp.predicates
        .iter()
        .chain(steps)
        .any(|p| view.matches_predicate(p))
}

fn rewrite_expression(
    expr: &mut crate::ir::Expression,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) {
    expr.for_each_embedded_patterns_mut(&mut |patterns| {
        *patterns = rewrite_patterns(std::mem::take(patterns), view, encode);
    });
}

/// Whether `patterns` hold a [`ListViewPattern`] anywhere.
pub fn has_list_view(patterns: &[Pattern]) -> bool {
    patterns.iter().any(|p| match p {
        Pattern::ListView(_) => true,
        Pattern::Filter(expr) | Pattern::Bind { expr, .. } | Pattern::Unwind { list: expr, .. } => {
            expression_has_list_view(expr)
        }
        Pattern::Optional(xs)
        | Pattern::Minus(xs)
        | Pattern::Exists(xs)
        | Pattern::NotExists(xs)
        | Pattern::Graph { patterns: xs, .. }
        | Pattern::DefaultGraphSource { patterns: xs } => has_list_view(xs),
        Pattern::Union(branches) => branches.iter().any(|b| has_list_view(b)),
        Pattern::Subquery(sq) => {
            has_list_view(&sq.patterns) || modifiers_have_list_view(&sq.grouping, &sq.order_binds)
        }
        Pattern::EdgeAnnotation { body, .. } | Pattern::AnnotationTarget { body, .. } => {
            has_list_view(body)
        }
        _ => false,
    })
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

/// The input rows a list-view triple's join pulled whose subject may meet a
/// list triple, held by a [`ListTeeOperator`] for the [`ListAugmentOperator`]
/// above the join.
#[derive(Clone, Default)]
pub struct HeldRows(Arc<Mutex<Vec<Batch>>>);

impl HeldRows {
    fn push(&self, batch: Batch) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(batch);
    }

    fn take(&self) -> Vec<Batch> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Feeds a list-view triple's join its input unchanged, holding the rows
/// whose subject may meet a list triple of the pattern
/// ([`ListView::may_meet`]).
pub struct ListTeeOperator {
    child: BoxedOperator,
    pattern: TriplePattern,
    view: Arc<ListView>,
    held: HeldRows,
}

impl ListTeeOperator {
    pub fn new(
        child: BoxedOperator,
        pattern: TriplePattern,
        view: Arc<ListView>,
        held: HeldRows,
    ) -> Self {
        Self {
            child,
            pattern,
            view,
            held,
        }
    }
}

/// A list-view triple's join, which reads the stored triples that are not
/// list members, followed by the list triples that match the pattern: for
/// the input rows a [`ListTeeOperator`] held, or, for a pattern with no
/// input, for the empty solution once.
pub struct ListAugmentOperator {
    join: BoxedOperator,
    pattern: TriplePattern,
    view: Arc<ListView>,
    /// `None` for a pattern with no input.
    held: Option<HeldRows>,
    schema: Arc<[VarId]>,
    inputs: std::collections::VecDeque<Batch>,
    joined: bool,
    unit_done: bool,
    state: OperatorState,
}

impl ListAugmentOperator {
    pub fn new(
        join: BoxedOperator,
        pattern: TriplePattern,
        view: Arc<ListView>,
        held: Option<HeldRows>,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(join.schema());
        Self {
            join,
            pattern,
            view,
            held,
            schema,
            inputs: std::collections::VecDeque::new(),
            joined: false,
            unit_done: false,
            state: OperatorState::Created,
        }
    }

    /// The next batch of list triples, joined with the inputs waiting.
    async fn next_list_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if ctx.is_multi_ledger() {
            return Ok(None);
        }
        loop {
            if self.inputs.is_empty() {
                match &self.held {
                    Some(held) => self.inputs.extend(held.take()),
                    // After the stored rows, so a LIMIT they meet reads no list.
                    None if self.joined && !self.unit_done => {
                        self.unit_done = true;
                        self.inputs.push_back(Batch::single_empty());
                    }
                    None => {}
                }
            }
            let Some(input) = self.inputs.pop_front() else {
                return Ok(None);
            };
            ctx.check_cancelled()?;
            if let Some(batch) =
                list_rows(ctx, &self.view, &self.pattern, &self.schema, &input).await?
            {
                return Ok(Some(batch));
            }
        }
    }
}

/// `rows` of `batch`, in order.
fn select_rows(batch: &Batch, rows: &[usize]) -> Result<Batch> {
    if batch.schema().is_empty() {
        return Ok(Batch::empty_schema_with_len(rows.len()));
    }
    let columns = (0..batch.schema().len())
        .map(|col| {
            rows.iter()
                .map(|&row| batch.get_by_col(row, col).clone())
                .collect()
        })
        .collect();
    Ok(Batch::new(batch.schema_arc(), columns)?)
}

/// `input` joined with the list triples that match `t`, in `schema`.
async fn list_rows(
    ctx: &ExecutionContext<'_>,
    view: &ListView,
    t: &TriplePattern,
    schema: &Arc<[VarId]>,
    input: &Batch,
) -> Result<Option<Batch>> {
    let graphs = ctx.path_graphs()?;
    // Which triple position each output column takes, when not the input's.
    let positions: Vec<Option<usize>> = schema
        .iter()
        .map(|v| {
            [t.s.as_var(), t.p.as_var(), t.o.as_var()]
                .iter()
                .position(|pv| *pv == Some(*v))
        })
        .collect();
    let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); schema.len()];
    let mut rows = 0;
    let mut cache: HashMap<(Want, Want, ObjectWant), Vec<ListTriple>> = HashMap::new();
    for row in 0..input.len() {
        let bound = |var: Option<VarId>| var.and_then(|v| input.get(row, v));
        let s = node_want(ctx, bound(t.s.as_var()), &t.s)?;
        let p = node_want(ctx, bound(t.p.as_var()), &t.p)?;
        let o = object_want(ctx, bound(t.o.as_var()), &t.o, t.dtc.as_ref())?;
        let key = (s.clone(), p.clone(), o.clone());
        if !cache.contains_key(&key) {
            let found = list_triples(ctx, view, &graphs, &s, &p, &o).await?;
            cache.insert(key.clone(), found);
        }
        for triple in cache[&key].iter().filter(|triple| repeats_agree(t, triple)) {
            for ((var, position), column) in schema.iter().zip(&positions).zip(&mut columns) {
                // The input's value, which the triple was found by; an unbound
                // one takes the triple's.
                let value = match (input.get(row, *var), position) {
                    (Some(b), _) if *b != Binding::Unbound => b.clone(),
                    (_, Some(0)) => Binding::sid(triple.s.clone()),
                    (_, Some(1)) => Binding::sid(triple.p.clone()),
                    (_, Some(_)) => triple.o.clone(),
                    (b, None) => b.cloned().unwrap_or(Binding::Unbound),
                };
                column.push(value);
            }
            rows += 1;
        }
    }
    if rows == 0 {
        return Ok(None);
    }
    if schema.is_empty() {
        return Ok(Some(Batch::empty_schema_with_len(rows)));
    }
    Ok(Some(Batch::new(Arc::clone(schema), columns)?))
}

/// The list triples matching `s p o` in `graphs`.
async fn list_triples(
    ctx: &ExecutionContext<'_>,
    view: &ListView,
    graphs: &[PathGraph<'_>],
    s: &Want,
    p: &Want,
    o: &ObjectWant,
) -> Result<Vec<ListTriple>> {
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

/// Whether `triple` gives each variable `t` repeats (`?x ?p ?x`) one term.
/// Checked on the triple, not the input, whose binding for the variable can
/// be encoded where the triple's is not.
fn repeats_agree(t: &TriplePattern, triple: &ListTriple) -> bool {
    let (s, p, o) = (t.s.as_var(), t.p.as_var(), t.o.as_var());
    let o_is = |sid: &Sid| matches!(&triple.o, Binding::Sid { sid: o, .. } if o == sid);
    (s.is_none() || s != p || triple.s == triple.p)
        && (s.is_none() || s != o || o_is(&triple.s))
        && (p.is_none() || p != o || o_is(&triple.p))
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

fn decoded(ctx: &ExecutionContext<'_>, b: &Binding) -> Result<Binding> {
    crate::group_aggregate::materialize_encoded(b, ctx.term_view().as_ref())
}

fn iri_sid(ctx: &ExecutionContext<'_>, iri: &str) -> Want {
    match ctx.active_snapshot.encode_iri(iri) {
        Some(sid) => Want::Node(sid),
        None => Want::Nothing,
    }
}

fn node_want(ctx: &ExecutionContext<'_>, bound: Option<&Binding>, term: &Ref) -> Result<Want> {
    Ok(match (bound, term) {
        (Some(b), _) => match decoded(ctx, b)? {
            Binding::Unbound => Want::Any,
            Binding::Sid { sid, .. } => Want::Node(sid),
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => iri_sid(ctx, &iri),
            _ => Want::Nothing,
        },
        (None, Ref::Var(_)) => Want::Any,
        (None, Ref::Sid(sid)) => Want::Node(sid.clone()),
        (None, Ref::Iri(iri)) => iri_sid(ctx, iri),
    })
}

fn object_want(
    ctx: &ExecutionContext<'_>,
    bound: Option<&Binding>,
    term: &Term,
    dtc: Option<&DatatypeConstraint>,
) -> Result<ObjectWant> {
    let node = |w: Want| match w {
        Want::Node(sid) => ObjectWant::Node(sid),
        Want::Any => ObjectWant::Any,
        Want::Nothing => ObjectWant::Nothing,
    };
    Ok(match (bound, term) {
        (Some(b), _) => match decoded(ctx, b)? {
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
    })
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

/// Whether `subject` may have a list row in `graph`: in its novelty, or in
/// the index ([`BinaryIndexStore::subject_holds_lists`]), or, read before the
/// index's own `t`, in either.
fn subject_may_hold_lists(graph: &PathGraph<'_>, subject: &Subject) -> Result<bool> {
    let io = |e| QueryError::from_io("list view subject", e);
    if let Subject::Node(sid) = subject {
        let (first, last) = (
            Flake::min_for_subject(sid.clone()),
            Flake::max_for_subject(sid.clone()),
        );
        let mut found = false;
        graph.overlay.for_each_overlay_flake(
            graph.g_id,
            IndexType::Spot,
            Some(&first),
            Some(&last),
            false,
            graph.to_t,
            &mut |flake| found |= is_member(flake),
        );
        if found {
            return Ok(true);
        }
    }
    let Some(store) = ExecutionContext::extract_binary_store(graph.snapshot) else {
        return Ok(false);
    };
    // Read as of a `t` before the index's own, a member a later commit
    // retracted is history, which leaflet flags do not cover.
    if graph.to_t < store.max_t() {
        return Ok(true);
    }
    let s_id = match subject {
        Subject::Id(s_id) => Some(*s_id),
        Subject::Node(sid) => store
            .find_subject_id_by_parts(sid.namespace_code, &sid.name)
            .map_err(io)?,
    };
    match s_id {
        Some(s_id) => store.subject_holds_lists(graph.g_id, s_id).map_err(io),
        None => Ok(false),
    }
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
        (Want::Node(s), Want::Any) => match &view.predicates {
            Some(predicates) => predicates
                .iter()
                .map(|p| {
                    let at = RangeMatch::new()
                        .with_subject(s.clone())
                        .with_predicate(p.clone());
                    (IndexType::Spot, at)
                })
                .collect(),
            None => vec![(IndexType::Spot, RangeMatch::new().with_subject(s.clone()))],
        },
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
impl Operator for ListTeeOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        self.child.schema()
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        let Some(batch) = self.child.next_batch(ctx).await? else {
            return Ok(None);
        };
        if batch.is_empty() || ctx.is_multi_ledger() {
            return Ok(Some(batch));
        }
        let graphs = ctx.path_graphs()?;
        let mut rows = Vec::new();
        for row in 0..batch.len() {
            let subject = match &self.pattern.s {
                Ref::Var(v) => batch.get(row, *v).cloned().unwrap_or(Binding::Unbound),
                Ref::Sid(sid) => Binding::sid(sid.clone()),
                Ref::Iri(iri) => Binding::Iri(iri.clone()),
            };
            if self.view.may_meet(ctx, &graphs, &subject)? {
                rows.push(row);
            }
        }
        if !rows.is_empty() {
            self.held.push(select_rows(&batch, &rows)?);
        }
        Ok(Some(batch))
    }

    fn close(&mut self) {
        self.child.close();
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.child.estimated_rows()
    }
}

#[async_trait]
impl Operator for ListAugmentOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.join.as_ref())]
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.join.open(ctx).await?;
        self.inputs.clear();
        self.joined = false;
        self.unit_done = false;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        loop {
            if self.state != OperatorState::Open {
                return Ok(None);
            }
            if let Some(batch) = self.next_list_batch(ctx).await? {
                return Ok(Some(batch));
            }
            if self.joined {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
            match self.join.next_batch(ctx).await? {
                Some(batch) => return Ok(Some(batch)),
                None => self.joined = true,
            }
        }
    }

    fn close(&mut self) {
        self.join.close();
        self.inputs.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.join.estimated_rows()
    }
}
