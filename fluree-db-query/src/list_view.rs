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
//! The view covers every graph a query reads, each in its own ledger's
//! namespace table. A default graph spanning ledgers is refused, as for any
//! property path ([`ExecutionContext::path_graphs`]).

use crate::binary_scan::BinaryScanOperator;
use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, PathGraph};
use crate::error::{QueryError, Result};
use crate::ir::{Pattern, Ref, Term, TriplePattern};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::list_index::{self, Item, Leaf};
use fluree_db_core::namespaces::{is_rdf_first, is_rdf_rest};
use fluree_db_core::range::range_with_overlay_tracked;
use fluree_db_core::{
    DatatypeConstraint, Flake, FlakeValue, GraphId, IndexType, LedgerId, LedgerSnapshot,
    OverlayProvider, RangeMatch, RangeOptions, RangeTest, Sid,
};
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::predicates::{RDF_FIRST, RDF_NIL, RDF_REST};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// A list, named by the graph, subject and predicate that own it and which
/// of their lists it is.
pub type Owner = (GraphId, Sid, Sid, u32);

/// A list, its members in position order, and the cells SPARQL sees it as.
#[derive(Debug)]
struct List {
    owner: Owner,
    members: Vec<Flake>,
    /// A list holding lists has a cell per item of each list in it, the
    /// top-level list's first; `None` for a flat list, whose cell `k` is
    /// member `k`'s.
    tree: Option<Vec<TreeCell>>,
}

/// What a cell's `rdf:first` is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum First {
    Member(usize),
    /// The head cell of the list the item is.
    Head(usize),
}

/// A cell of a list holding lists.
#[derive(Debug, Clone, Copy)]
struct TreeCell {
    first: First,
    /// The next cell of its list; `None` for the last, whose `rdf:rest` is
    /// `rdf:nil`.
    rest: Option<usize>,
    /// The cell before it in its list.
    prev: Option<usize>,
    /// For a list's head, the cell whose `rdf:first` it is.
    up: Option<usize>,
}

impl List {
    fn flat(owner: Owner, members: Vec<Flake>) -> Self {
        List {
            owner,
            members,
            tree: None,
        }
    }

    /// `members` as their tree's cells, `visible` the leaves of `all`, the
    /// whole tree's in position order, a query may read: a hidden leaf
    /// reads as deleted.
    fn of_tree(owner: Owner, members: Vec<Flake>, all: &[Flake]) -> Self {
        let list = owner.3;
        let shown: HashSet<i32> = members.iter().filter_map(|f| f.m.as_ref()?.i).collect();
        let mut next = 0;
        let leaves = all.iter().filter_map(|f| {
            let i = f.m.as_ref()?.i?;
            let kept = shown.contains(&i).then(|| {
                next += 1;
                next - 1
            });
            Some((kept, Leaf::of(list, i)))
        });
        let items = list_index::kept_items(leaves);
        let mut cells = Vec::new();
        build_cells(&items, None, &mut cells);
        List {
            owner,
            members,
            tree: Some(cells),
        }
    }

    fn cells(&self) -> usize {
        self.tree.as_ref().map_or(self.members.len(), Vec::len)
    }

    fn first(&self, k: usize) -> First {
        self.tree
            .as_ref()
            .map_or(First::Member(k), |cells| cells[k].first)
    }

    fn rest(&self, k: usize) -> Option<usize> {
        match &self.tree {
            Some(cells) => cells[k].rest,
            None => (k + 1 < self.members.len()).then_some(k + 1),
        }
    }

    fn prev(&self, k: usize) -> Option<usize> {
        match &self.tree {
            Some(cells) => cells[k].prev,
            None => k.checked_sub(1),
        }
    }

    fn up(&self, k: usize) -> Option<usize> {
        self.tree.as_ref().and_then(|cells| cells[k].up)
    }
}

/// The cells of `items`, a list `up`'s `rdf:first` heads (`None` for the
/// top-level list), appended to `cells`; the head's index.
fn build_cells(items: &[Item<usize>], up: Option<usize>, cells: &mut Vec<TreeCell>) -> usize {
    let base = cells.len();
    let last = items.len().saturating_sub(1);
    for (k, item) in items.iter().enumerate() {
        cells.push(TreeCell {
            first: match item {
                Item::Value(m) => First::Member(*m),
                Item::List(_) => First::Head(usize::MAX),
            },
            rest: (k < last).then_some(base + k + 1),
            prev: k.checked_sub(1).map(|j| base + j),
            up: if k == 0 { up } else { None },
        });
    }
    for (k, item) in items.iter().enumerate() {
        if let Item::List(inner) = item {
            let head = build_cells(inner, Some(base + k), cells);
            cells[base + k].first = First::Head(head);
        }
    }
    base
}

/// A ledger a [`ListView`] reads, with the predicates holding list members
/// there in its own namespace table (`None`: any may).
///
/// Ordered: a read with an open predicate reads them in this order, which is
/// the order its lists are answered in and their cells are labelled in. A std
/// set seeded each view at random, so the same query answered its lists in a
/// different order on each execution.
type LedgerLists = (LedgerId, Option<BTreeSet<Sid>>);

/// Which predicates hold list members, and the cells one request has named.
#[derive(Debug)]
pub struct ListView {
    /// The index ids of the primary ledger's list predicates, sorted; `None`
    /// when they are not known, or the request reads several ledgers.
    predicate_ids: Option<Vec<u32>>,
    /// The ledger the query was lowered against (ledger 0), and each other
    /// ledger the request reads (ledger `n` is `others[n - 1]`). Graphs of
    /// one ledger share its namespace table and those of two do not, so
    /// every read below is of one ledger ([`Self::ledger_of`]).
    primary: LedgerLists,
    others: Vec<LedgerLists>,
    /// The lists this request has named a cell of: list `n` is `owners[n]`.
    lists: Mutex<ListTable>,
    /// The lists each read of this request found, so a walk along a list's
    /// cells reads it once.
    reads: Mutex<HashMap<ListRead, Arc<[List]>>>,
    /// Whether each subject this request asked about may own a list, per
    /// ledger, graph and `t` ([`Self::may_meet`]).
    subjects: Mutex<HashMap<(usize, GraphId, i64, Subject), bool>>,
}

/// A graph a [`ListView`] is built over: its snapshot, novelty and index.
pub(crate) struct ListSource<'a> {
    snapshot: &'a LedgerSnapshot,
    overlay: &'a dyn OverlayProvider,
    store: Option<Arc<BinaryIndexStore>>,
}

impl ListSource<'_> {
    /// What the source's list predicates are read from: two graphs of one
    /// snapshot, novelty and index read the same ones.
    fn key(
        &self,
    ) -> (
        *const LedgerSnapshot,
        *const (),
        Option<*const BinaryIndexStore>,
    ) {
        (
            self.snapshot,
            (self.overlay as *const dyn OverlayProvider).cast(),
            self.store.as_ref().map(Arc::as_ptr),
        )
    }
}

/// A subject [`ListView::may_meet`] is asked about.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Subject {
    /// An index subject id; such bindings arise only over an empty overlay.
    Id(u64),
    Node(Sid),
}

/// The cells' lists, each with the ledger it is in ([`ListView::ledger_of`]).
#[derive(Debug, Default)]
struct ListTable {
    /// Each list's place in `owners`: the primary ledger's, and the others'
    /// by ledger (ledger `n` at `others[n - 1]`).
    ids: HashMap<Owner, usize>,
    others: Vec<HashMap<Owner, usize>>,
    owners: Vec<(usize, Owner)>,
}

impl ListTable {
    fn ids_of(&self, l: usize) -> Option<&HashMap<Owner, usize>> {
        match l {
            0 => Some(&self.ids),
            l => self.others.get(l - 1),
        }
    }

    fn ids_of_mut(&mut self, l: usize) -> &mut HashMap<Owner, usize> {
        if l == 0 {
            return &mut self.ids;
        }
        if self.others.len() < l {
            self.others.resize_with(l, HashMap::new);
        }
        &mut self.others[l - 1]
    }
}

/// The ledger, graph, its `t`, and the subject and predicate a list read
/// asks for.
type ListRead = (usize, GraphId, i64, Want, Want);

/// The predicates holding list members in `snapshot` with `overlay`:
/// `None` when that is not known, and empty when there are none.
fn list_predicates(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    store: Option<&Arc<BinaryIndexStore>>,
) -> Result<Option<BTreeSet<Sid>>> {
    let mut predicates: Option<BTreeSet<Sid>> = Some(BTreeSet::new());
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
    Ok(predicates)
}

/// `known` with the predicates `found` in another graph of its ledger.
fn merge_predicates(known: &mut Option<BTreeSet<Sid>>, found: Option<BTreeSet<Sid>>) {
    match (known.as_mut(), found) {
        (Some(known), Some(found)) => known.extend(found),
        (_, None) => *known = None,
        (None, Some(_)) => {}
    }
}

impl ListView {
    /// The view for a query of `snapshot` with `overlay`, or `None` when the
    /// ledger provably holds no list members.
    pub fn for_db(
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        store: Option<&Arc<BinaryIndexStore>>,
    ) -> Result<Option<Arc<Self>>> {
        Self::for_sources([ListSource {
            snapshot,
            overlay,
            store: store.cloned(),
        }])
    }

    /// The view for a query reading `sources`, the first the graph it was
    /// lowered against, or `None` when none provably holds list members.
    pub(crate) fn for_sources<'a>(
        sources: impl IntoIterator<Item = ListSource<'a>>,
    ) -> Result<Option<Arc<Self>>> {
        let mut sources = sources.into_iter();
        let Some(first) = sources.next() else {
            return Ok(None);
        };
        let read = |s: &ListSource<'_>| list_predicates(s.snapshot, s.overlay, s.store.as_ref());
        let mut primary: LedgerLists = (first.snapshot.ledger_id.clone(), read(&first)?);
        let mut others: Vec<LedgerLists> = Vec::new();
        let first_key = first.key();
        let mut read_from = Vec::new();
        for source in sources {
            let key = source.key();
            if key == first_key || read_from.contains(&key) {
                continue;
            }
            read_from.push(key);
            let found = read(&source)?;
            let id = &source.snapshot.ledger_id;
            if *id == primary.0 {
                merge_predicates(&mut primary.1, found);
                continue;
            }
            match others.iter_mut().find(|(other, _)| other == id) {
                Some((_, known)) => merge_predicates(known, found),
                None => others.push((id.clone(), found)),
            }
        }
        let holds = |(_, ps): &LedgerLists| ps.as_ref().is_none_or(|ps| !ps.is_empty());
        if !holds(&primary) && !others.iter().any(holds) {
            return Ok(None);
        }
        let predicate_ids = match (&primary.1, &first.store) {
            (Some(predicates), Some(store)) if others.is_empty() => {
                let mut ids: Vec<u32> = predicates
                    .iter()
                    .filter_map(|p| store.predicate_id_for_sid(p))
                    .collect();
                ids.sort_unstable();
                Some(ids)
            }
            _ => None,
        };
        Ok(Some(Arc::new(Self {
            predicate_ids,
            primary,
            others,
            lists: Mutex::new(ListTable::default()),
            reads: Mutex::new(HashMap::new()),
            subjects: Mutex::new(HashMap::new()),
        })))
    }

    /// A view of one ledger whose list members are on `predicates`, for
    /// planning tests.
    #[cfg(test)]
    pub(crate) fn of_predicates(predicates: impl IntoIterator<Item = Sid>) -> Arc<Self> {
        Arc::new(Self {
            predicate_ids: None,
            primary: (
                LedgerId::parse("lists:main").expect("a valid ledger id"),
                Some(predicates.into_iter().collect()),
            ),
            others: Vec::new(),
            lists: Mutex::new(ListTable::default()),
            reads: Mutex::new(HashMap::new()),
            subjects: Mutex::new(HashMap::new()),
        })
    }

    /// Which ledger of the view `snapshot` is of (see [`Self::primary`]);
    /// past them for a ledger it was not built over, whose predicates are
    /// not known.
    fn ledger_of(&self, snapshot: &LedgerSnapshot) -> usize {
        if self.others.is_empty() || snapshot.ledger_id == self.primary.0 {
            return 0;
        }
        self.others
            .iter()
            .position(|(id, _)| *id == snapshot.ledger_id)
            .map_or(self.others.len() + 1, |n| n + 1)
    }

    /// The predicates holding list members in ledger `l`, when known.
    fn ledger_predicates(&self, l: usize) -> Option<&BTreeSet<Sid>> {
        match l {
            0 => self.primary.1.as_ref(),
            l => self.others.get(l - 1).and_then(|(_, ps)| ps.as_ref()),
        }
    }

    /// Whether `p`, in ledger `l`'s namespace table, may hold list members.
    fn may_hold_lists_in(&self, l: usize, p: &Sid) -> bool {
        self.ledger_predicates(l).is_none_or(|ps| ps.contains(p))
    }

    /// Whether a list triple can have `subject` as its subject in `graphs`:
    /// an unbound subject, a cell this request named, or a node with a list
    /// row in novelty or in the index. A `false` is exact.
    fn may_meet(&self, graphs: &[PathGraph<'_>], subject: &Binding) -> Result<bool> {
        let Some(path) = graphs.first() else {
            return Ok(false);
        };
        let l = self.ledger_of(path.snapshot);
        let subject = match subject {
            Binding::Unbound => return Ok(true),
            Binding::EncodedSid { s_id, .. } => Subject::Id(*s_id),
            Binding::Sid { sid, .. }
            | Binding::IriMatch {
                primary_sid: sid, ..
            } => {
                if self.decode_in(l, sid).is_some() {
                    return Ok(true);
                }
                Subject::Node(sid.clone())
            }
            Binding::Iri(iri) => match path.snapshot.encode_iri(iri) {
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
            let key = (l, graph.g_id, graph.to_t, subject.clone());
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

    /// The predicates holding list members, in the namespace table the
    /// query was lowered against; `None` when that is not known, or the
    /// request reads several ledgers, and any may.
    pub(crate) fn predicates(&self) -> Option<&BTreeSet<Sid>> {
        match self.others.is_empty() {
            true => self.primary.1.as_ref(),
            false => None,
        }
    }

    pub(crate) fn may_hold_lists(&self, p: &Sid) -> bool {
        self.predicates().is_none_or(|ps| ps.contains(p))
    }

    /// [`Self::may_hold_lists`] for the predicate with index id `p_id`,
    /// without decoding it.
    pub(crate) fn may_hold_lists_id(&self, p_id: u32) -> bool {
        self.predicate_ids
            .as_ref()
            .is_none_or(|ids| ids.binary_search(&p_id).is_ok())
    }

    /// Whether a triple on predicate `p` can be a list triple: an owner edge
    /// or a cell's `rdf:first` / `rdf:rest`.
    pub(crate) fn matches_predicate(&self, p: &Sid) -> bool {
        self.may_hold_lists(p) || is_rdf_first(p) || is_rdf_rest(p)
    }

    /// The blank node for cell `k` of `owner`'s list, in ledger `l`.
    fn cell(&self, l: usize, owner: &Owner, k: usize) -> Sid {
        cell_sid(self.list_number(l, owner), k)
    }

    /// The number this request names `owner`'s list in ledger `l` by, given
    /// now if it has none: lists are numbered in the order a cell of theirs
    /// is first named.
    fn list_number(&self, l: usize, owner: &Owner) -> usize {
        let mut table = self
            .lists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match table.ids_of(l).and_then(|ids| ids.get(owner)) {
            Some(&n) => n,
            None => {
                let n = table.owners.len();
                table.owners.push((l, owner.clone()));
                table.ids_of_mut(l).insert(owner.clone(), n);
                n
            }
        }
    }

    /// The list and position `sid` names, when it is a cell of this request.
    pub fn decode(&self, sid: &Sid) -> Option<(Owner, usize)> {
        self.decode_cell(sid).map(|(_, owner, k)| (owner, k))
    }

    /// [`Self::decode`] for a cell of a list in ledger `l`.
    fn decode_in(&self, l: usize, sid: &Sid) -> Option<(Owner, usize)> {
        let (at, owner, k) = self.decode_cell(sid)?;
        (at == l).then_some((owner, k))
    }

    fn decode_cell(&self, sid: &Sid) -> Option<(usize, Owner, usize)> {
        if sid.namespace_code != BLANK_NODE {
            return None;
        }
        let (n, k) = sid.name.strip_prefix('l')?.split_once('-')?;
        let (n, k): (usize, usize) = (n.parse().ok()?, k.parse().ok()?);
        let table = self
            .lists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (l, owner) = table.owners.get(n)?.clone();
        Some((l, owner, k))
    }

    /// The members of each list of `owners` this request has read, in
    /// position order: member `k` is the one cell `k` of the list names.
    pub fn read_members(&self, owners: &HashSet<Owner>) -> HashMap<Owner, Vec<Flake>> {
        let reads = self
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out = HashMap::with_capacity(owners.len());
        for list in reads.values().flat_map(|lists| lists.iter()) {
            if owners.contains(&list.owner) && !out.contains_key(&list.owner) {
                out.insert(list.owner.clone(), list.members.clone());
                if out.len() == owners.len() {
                    break;
                }
            }
        }
        out
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
    modifiers_hold(grouping, order_binds, &is_list_view)
}

fn is_list_view(pattern: &Pattern) -> bool {
    matches!(pattern, Pattern::ListView(_))
}

/// Whether an `EXISTS` of `grouping` or `order_binds` holds a pattern `is`
/// accepts.
fn modifiers_hold(
    grouping: &Option<crate::ir::Grouping>,
    order_binds: &[(VarId, crate::ir::Expression)],
    is: &dyn Fn(&Pattern) -> bool,
) -> bool {
    let mut exprs: Vec<&crate::ir::Expression> = order_binds.iter().map(|(_, e)| e).collect();
    if let Some(grouping) = grouping {
        exprs.extend(grouping.having());
        exprs.extend(grouping.binds().map(|(_, e)| e));
    }
    exprs.into_iter().any(|expr| expression_holds(expr, is))
}

fn expression_holds(expr: &crate::ir::Expression, is: &dyn Fn(&Pattern) -> bool) -> bool {
    let mut found = false;
    expr.for_each_embedded_patterns(&mut |patterns| found |= holds(patterns, is));
    found
}

/// [`ListView::for_db`], reading the snapshot's own index store when `store`
/// is not given.
pub fn view_for(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    store: Option<&Arc<BinaryIndexStore>>,
) -> Result<Option<Arc<ListView>>> {
    view_for_dataset(snapshot, overlay, store, None)
}

/// [`view_for`] over every ledger graph of `dataset` as well, so a list in
/// any graph the query reads is in the view, in its own ledger.
pub fn view_for_dataset(
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    store: Option<&Arc<BinaryIndexStore>>,
    dataset: Option<&crate::dataset::DataSet<'_>>,
) -> Result<Option<Arc<ListView>>> {
    let store_of = crate::context::ExecutionContext::extract_binary_store;
    let primary = ListSource {
        snapshot,
        overlay,
        store: store.cloned().or_else(|| store_of(snapshot)),
    };
    let graphs = dataset.into_iter().flat_map(|dataset| {
        dataset
            .default_graphs()
            .iter()
            .chain(dataset.named_graphs_iter().map(|(_, g)| g))
    });
    let members = graphs
        .filter(|graph| graph.kind != crate::dataset::MemberKind::GraphSource)
        .map(|graph| ListSource {
            snapshot: graph.snapshot,
            overlay: graph.overlay,
            store: store_of(graph.snapshot),
        });
    ListView::for_sources(std::iter::once(primary).chain(members))
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

/// Mark the `rdf:rest*` of a lowered `p/rdf:rest*/rdf:first` (its owner
/// edge, the walk and the `rdf:first` step) as a list walk.
pub fn mark_list_walk(chain: &mut [Pattern]) {
    for pattern in chain {
        if let Pattern::PropertyPath(pp) = pattern {
            pp.list_walk = true;
        }
    }
}

/// Whether `query` holds a list walk ([`crate::ir::PropertyPathPattern::list_walk`]).
pub fn query_has_list_walk(query: &crate::ir::Query) -> bool {
    has_list_walk(&query.patterns)
        || modifiers_hold(&query.grouping, &query.order_binds, &is_list_walk)
}

/// Whether `patterns` hold a list walk anywhere.
pub fn has_list_walk(patterns: &[Pattern]) -> bool {
    holds(patterns, &is_list_walk)
}

fn is_list_walk(pattern: &Pattern) -> bool {
    matches!(pattern, Pattern::PropertyPath(pp) if pp.list_walk)
}

/// Read each list walk of `patterns`, with the owner edge that binds its
/// start and the `rdf:first` step that reads its end, in SPARQL's view of
/// lists: `p/rdf:rest*/rdf:first` reaches a list's members through its cells
/// and a stored chain through its triples. Every other pattern keeps reading
/// members as values.
pub fn rewrite_list_walks(
    patterns: Vec<Pattern>,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) -> Vec<Pattern> {
    // A walk's endpoints are fresh variables only its two triples share.
    let ends: Vec<(VarId, VarId)> = patterns
        .iter()
        .filter_map(|p| match p {
            Pattern::PropertyPath(pp) if pp.list_walk => {
                Some((pp.subject.as_var()?, pp.object.as_var()?))
            }
            _ => None,
        })
        .collect();
    patterns
        .into_iter()
        .map(|pattern| {
            let in_walk = match &pattern {
                Pattern::PropertyPath(pp) => pp.list_walk,
                Pattern::Triple(tp) => ends.iter().any(|&(head, cell)| {
                    tp.o.as_var() == Some(head) || tp.s.as_var() == Some(cell)
                }),
                _ => false,
            };
            if in_walk {
                rewrite_pattern(pattern, view, encode)
            } else {
                rewrite_walks_within(pattern, view, encode)
            }
        })
        .collect()
}

/// Read each triple of `patterns` with a subject or object in `cells` (the
/// cells of a DELETE WHERE's collections) in SPARQL's view of lists: a
/// collection matches a list's owner edge and cells, and a stored chain's
/// triples. Every other pattern keeps reading members as values.
pub fn rewrite_cells(
    patterns: Vec<Pattern>,
    cells: &[VarId],
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) -> Vec<Pattern> {
    let names_cell = |tp: &TriplePattern| {
        [tp.s.as_var(), tp.o.as_var()]
            .into_iter()
            .flatten()
            .any(|v| cells.contains(&v))
    };
    patterns
        .into_iter()
        .map(|pattern| match pattern {
            Pattern::Triple(tp) if names_cell(&tp) => {
                rewrite_pattern(Pattern::Triple(tp), view, encode)
            }
            Pattern::Triple(_) => pattern,
            other => other.map_subpatterns(&mut |xs| rewrite_cells(xs, cells, view, encode)),
        })
        .collect()
}

/// [`rewrite_list_walks`] for the `EXISTS` of `query`'s grouping and
/// ordering expressions.
pub fn rewrite_list_walks_of_modifiers(
    query: &mut crate::ir::Query,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) {
    if let Some(grouping) = &mut query.grouping {
        grouping.for_each_expression_mut(&mut |expr| walks_in_expression(expr, view, encode));
    }
    for (_, expr) in &mut query.order_binds {
        walks_in_expression(expr, view, encode);
    }
}

fn rewrite_walks_within(
    pattern: Pattern,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) -> Pattern {
    match pattern {
        Pattern::Filter(mut expr) => {
            walks_in_expression(&mut expr, view, encode);
            Pattern::Filter(expr)
        }
        Pattern::Bind { var, mut expr } => {
            walks_in_expression(&mut expr, view, encode);
            Pattern::Bind { var, expr }
        }
        Pattern::Unwind { var, mut list } => {
            walks_in_expression(&mut list, view, encode);
            Pattern::Unwind { var, list }
        }
        Pattern::Subquery(mut sq) => {
            sq.patterns = rewrite_list_walks(std::mem::take(&mut sq.patterns), view, encode);
            if let Some(grouping) = &mut sq.grouping {
                grouping
                    .for_each_expression_mut(&mut |expr| walks_in_expression(expr, view, encode));
            }
            for (_, expr) in &mut sq.order_binds {
                walks_in_expression(expr, view, encode);
            }
            Pattern::Subquery(sq)
        }
        Pattern::Service(_) => pattern,
        other => other.map_subpatterns(&mut |xs| rewrite_list_walks(xs, view, encode)),
    }
}

fn walks_in_expression(
    expr: &mut crate::ir::Expression,
    view: &Arc<ListView>,
    encode: &dyn Fn(&str) -> Option<Sid>,
) {
    expr.for_each_embedded_patterns_mut(&mut |patterns| {
        *patterns = rewrite_list_walks(std::mem::take(patterns), view, encode);
    });
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
    holds(patterns, &is_list_view)
}

/// Whether `patterns` hold a pattern `is` accepts anywhere, an `EXISTS`
/// included.
fn holds(patterns: &[Pattern], is: &dyn Fn(&Pattern) -> bool) -> bool {
    patterns.iter().any(|p| {
        is(p)
            || match p {
                Pattern::Filter(expr)
                | Pattern::Bind { expr, .. }
                | Pattern::Unwind { list: expr, .. } => expression_holds(expr, is),
                Pattern::Optional(xs)
                | Pattern::Minus(xs)
                | Pattern::Exists(xs)
                | Pattern::NotExists(xs)
                | Pattern::Graph { patterns: xs, .. }
                | Pattern::DefaultGraphSource { patterns: xs } => holds(xs, is),
                Pattern::Union(branches) => branches.iter().any(|b| holds(b, is)),
                Pattern::Subquery(sq) => {
                    holds(&sq.patterns, is) || modifiers_hold(&sq.grouping, &sq.order_binds, is)
                }
                Pattern::EdgeAnnotation { body, .. } => holds(body, is),
                _ => false,
            }
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
    /// The input being joined with its list triples, a batch at a time.
    pending: Option<PendingRows>,
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
            pending: None,
            joined: false,
            unit_done: false,
            state: OperatorState::Created,
        }
    }

    /// The next batch of list triples, joined with the inputs waiting.
    async fn next_list_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        loop {
            if let Some(pending) = self.pending.as_mut() {
                ctx.check_cancelled()?;
                match pending.next_batch(&self.pattern, &self.schema, ctx.batch_size)? {
                    Some(batch) => return Ok(Some(batch)),
                    None => self.pending = None,
                }
            }
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
            self.pending =
                Some(PendingRows::read(ctx, &self.view, &self.pattern, &self.schema, input).await?);
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

/// An input joined with the list triples that match `t` for each of its
/// rows, made a batch at a time, so a read of many lists never holds all their
/// triples at once on top of the batches they go into.
struct PendingRows {
    input: Batch,
    /// Which triple position each output column takes, when not the input's.
    positions: Vec<Option<usize>>,
    /// The lists each row matches; rows asking for the same ones share them.
    matched: Vec<Arc<Matched>>,
    row: usize,
    triples: Option<ListTriples>,
}

impl PendingRows {
    /// Reads, and numbers, every list the rows of `input` match before any
    /// triple is made, so the cells are labelled as when all of them were made
    /// at once, whatever reads the first batches before the next is made.
    async fn read(
        ctx: &ExecutionContext<'_>,
        view: &ListView,
        t: &TriplePattern,
        schema: &Arc<[VarId]>,
        input: Batch,
    ) -> Result<Self> {
        let graphs = ctx.path_graphs()?;
        let names = Names::of(ctx, &graphs);
        let positions = schema
            .iter()
            .map(|v| {
                [t.s.as_var(), t.p.as_var(), t.o.as_var()]
                    .iter()
                    .position(|pv| *pv == Some(*v))
            })
            .collect();
        let mut by_key: HashMap<(Want, Want, ObjectWant), Arc<Matched>> = HashMap::new();
        let mut matched = Vec::with_capacity(input.len());
        for row in 0..input.len() {
            let bound = |var: Option<VarId>| var.and_then(|v| input.get(row, v));
            let s = node_want(ctx, &names, bound(t.s.as_var()), &t.s)?;
            let p = node_want(ctx, &names, bound(t.p.as_var()), &t.p)?;
            let o = object_want(ctx, &names, bound(t.o.as_var()), &t.o, t.dtc.as_ref())?;
            let key = (s, p, o);
            let found = match by_key.get(&key) {
                Some(found) => Arc::clone(found),
                None => {
                    let (s, p, o) = &key;
                    let found = Arc::new(Matched::read(ctx, view, &graphs, s, p, o.clone()).await?);
                    by_key.insert(key, Arc::clone(&found));
                    found
                }
            };
            matched.push(found);
        }
        Ok(Self {
            input,
            positions,
            matched,
            row: 0,
            triples: None,
        })
    }

    /// The next rows, at most `limit` of them; `None` once the input is done.
    fn next_batch(
        &mut self,
        t: &TriplePattern,
        schema: &Arc<[VarId]>,
        limit: usize,
    ) -> Result<Option<Batch>> {
        let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); schema.len()];
        let mut rows = 0;
        while self.row < self.input.len() && rows < limit.max(1) {
            let row = self.row;
            let triples = self
                .triples
                .get_or_insert_with(|| ListTriples::new(Arc::clone(&self.matched[row])));
            let Some(triple) = triples.next() else {
                self.triples = None;
                self.row += 1;
                continue;
            };
            if !repeats_agree(t, &triple) {
                continue;
            }
            for ((var, position), column) in schema.iter().zip(&self.positions).zip(&mut columns) {
                // The input's value, which the triple was found by; an unbound
                // one takes the triple's.
                let value = match (self.input.get(row, *var), position) {
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
        if rows == 0 {
            return Ok(None);
        }
        if schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(rows)));
        }
        Ok(Some(Batch::new(Arc::clone(schema), columns)?))
    }
}

/// The lists one `s p o` matches, read and numbered: per graph, the lists
/// whose owner edges may match and the lists whose cells may, each with its
/// list number.
struct Matched {
    graphs: Vec<GraphLists>,
    want_first: bool,
    want_rest: bool,
    /// The cell the subject is, when it is one: only that cell is read.
    cell: Option<(Owner, usize)>,
    o: ObjectWant,
    first: Sid,
    rest: Sid,
}

#[derive(Default)]
struct GraphLists {
    owners: Option<(Arc<[List]>, Vec<usize>)>,
    cells: Option<(Arc<[List]>, Vec<usize>)>,
}

impl Matched {
    /// The lists matching `s p o` in `graphs`, numbered in the order their
    /// triples are made: per graph, the owner edges' lists, then each list
    /// with a cell to read.
    async fn read(
        ctx: &ExecutionContext<'_>,
        view: &ListView,
        graphs: &[PathGraph<'_>],
        s: &Want,
        p: &Want,
        o: ObjectWant,
    ) -> Result<Self> {
        let mut out = Matched {
            graphs: Vec::new(),
            want_first: false,
            want_rest: false,
            cell: None,
            o,
            first: Sid::new(RDF, RDF_FIRST),
            rest: Sid::new(RDF, RDF_REST),
        };
        if matches!(s, Want::Nothing) || matches!(p, Want::Nothing) {
            return Ok(out);
        }
        // The graphs of one read are of one ledger.
        let l = graphs.first().map_or(0, |g| view.ledger_of(g.snapshot));
        out.cell = match s {
            Want::Node(sid) => view.decode_in(l, sid),
            _ => None,
        };
        (out.want_first, out.want_rest) = match p {
            Want::Any => (true, true),
            Want::Node(p) => (is_rdf_first(p), is_rdf_rest(p)),
            Want::Nothing => (false, false),
        };
        for graph in graphs {
            let mut lists = GraphLists::default();
            // Owner edges: `s p head`, for a subject that is not a cell.
            let owner_edges = match p {
                Want::Any => true,
                Want::Node(p) => view.may_hold_lists_in(l, p),
                Want::Nothing => false,
            };
            if owner_edges && out.cell.is_none() {
                let found = lists_in(ctx, graph, view, s, p).await?;
                let numbers = found
                    .iter()
                    .map(|list| view.list_number(l, &list.owner))
                    .collect();
                lists.owners = Some((found, numbers));
            }
            // Cells: `cellK rdf:first memberK ; rdf:rest next`.
            if out.want_first || out.want_rest {
                let found = match (s, &out.cell) {
                    (_, Some((owner, _))) if owner.0 != graph.g_id => None,
                    (_, Some((owner, _))) => {
                        let at = Want::Node(owner.1.clone());
                        let of = Want::Node(owner.2.clone());
                        Some(lists_in(ctx, graph, view, &at, &of).await?)
                    }
                    (Want::Node(_), None) => None,
                    _ => Some(lists_in(ctx, graph, view, &Want::Any, &Want::Any).await?),
                };
                if let Some(found) = found {
                    // A list with no cell to read here gets no number from it.
                    let numbers = found
                        .iter()
                        .map(|list| {
                            if out.positions(&list.owner, list.cells()).is_empty() {
                                usize::MAX
                            } else {
                                view.list_number(l, &list.owner)
                            }
                        })
                        .collect();
                    lists.cells = Some((found, numbers));
                }
            }
            out.graphs.push(lists);
        }
        Ok(out)
    }

    /// The cells of `owner`'s list of `len` cells that are read: all of
    /// them, or the one cell the subject is.
    fn positions(&self, owner: &Owner, len: usize) -> std::ops::Range<usize> {
        match &self.cell {
            Some((at, _)) if at != owner => 0..0,
            Some((_, k)) => *k..len.min(k + 1),
            None => 0..len,
        }
    }
}

/// The triples of a [`Matched`], made one at a time, in the order they are
/// answered: per graph, the owner edges, then each list's cells, a cell's
/// `rdf:first` before its `rdf:rest`.
struct ListTriples {
    matched: Arc<Matched>,
    graph: usize,
    /// Past the owner edges, at the cells.
    at_cells: bool,
    list: usize,
    /// The position in the current list, once it has been entered.
    k: Option<usize>,
    /// The current cell, between its `rdf:first` and its `rdf:rest`.
    here: Option<Sid>,
}

impl ListTriples {
    fn new(matched: Arc<Matched>) -> Self {
        Self {
            matched,
            graph: 0,
            at_cells: false,
            list: 0,
            k: None,
            here: None,
        }
    }

    /// On to the next graph's lists.
    fn next_graph(&mut self) {
        self.graph += 1;
        self.at_cells = false;
        self.list = 0;
        self.k = None;
        self.here = None;
    }

    fn next(&mut self) -> Option<ListTriple> {
        loop {
            let m = &self.matched;
            let g = m.graphs.get(self.graph)?;
            if !self.at_cells {
                if let Some(list) = g.owners.as_ref().and_then(|(l, _)| l.get(self.list)) {
                    let n = g.owners.as_ref().map(|(_, numbers)| numbers[self.list])?;
                    let triple = ListTriple {
                        s: list.owner.1.clone(),
                        p: list.owner.2.clone(),
                        o: Binding::sid(cell_sid(n, 0)),
                    };
                    let keep = m.o.matches(&triple.o);
                    self.list += 1;
                    if keep {
                        return Some(triple);
                    }
                    continue;
                }
                self.at_cells = true;
                self.list = 0;
                continue;
            }
            let Some((lists, numbers)) = &g.cells else {
                self.next_graph();
                continue;
            };
            let Some(list) = lists.get(self.list) else {
                self.next_graph();
                continue;
            };
            let range = m.positions(&list.owner, list.cells());
            let k = *self.k.get_or_insert(range.start);
            if k >= range.end {
                self.list += 1;
                self.k = None;
                continue;
            }
            let n = numbers[self.list];
            let triple = match self.here.take() {
                // The cell's `rdf:first`; its `rdf:rest` comes next.
                None => {
                    let here = cell_sid(n, k);
                    self.here = Some(here.clone());
                    if !m.want_first {
                        continue;
                    }
                    ListTriple {
                        s: here,
                        p: m.first.clone(),
                        o: match list.first(k) {
                            First::Member(i) => member_binding(&list.members[i]),
                            First::Head(h) => Binding::sid(cell_sid(n, h)),
                        },
                    }
                }
                Some(here) => {
                    self.k = Some(k + 1);
                    if !m.want_rest {
                        continue;
                    }
                    let next = list
                        .rest(k)
                        .map_or_else(|| Sid::new(RDF, RDF_NIL), |j| cell_sid(n, j));
                    ListTriple {
                        s: here,
                        p: m.rest.clone(),
                        o: Binding::sid(next),
                    }
                }
            };
            if m.o.matches(&triple.o) {
                return Some(triple);
            }
        }
    }
}

/// The blank node for cell `k` of list `n`.
fn cell_sid(n: usize, k: usize) -> Sid {
    Sid::new(BLANK_NODE, format!("l{n}-{k}"))
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
    Ok(crate::group_aggregate::materialize_encoded(
        b,
        ctx.term_view().as_ref(),
    )?)
}

/// How the terms of a list read name nodes: in the namespace table of the
/// ledger the read's graphs are of, which a constant the query was lowered
/// against another ledger is re-encoded into, as a property path's is.
struct Names<'a> {
    snapshot: &'a LedgerSnapshot,
    foreign: bool,
}

impl<'a> Names<'a> {
    fn of(ctx: &ExecutionContext<'a>, graphs: &[PathGraph<'a>]) -> Self {
        let snapshot = graphs.first().map_or(ctx.active_snapshot, |g| g.snapshot);
        Self {
            snapshot,
            foreign: snapshot.ledger_id != ctx.original_snapshot.ledger_id,
        }
    }

    fn iri(&self, iri: &str) -> Want {
        match self.snapshot.encode_iri(iri) {
            Some(sid) => Want::Node(sid),
            None => Want::Nothing,
        }
    }

    fn constant(&self, ctx: &ExecutionContext<'_>, sid: &Sid) -> Sid {
        match self.foreign {
            true => {
                crate::context::reencode_sid(ctx, self.snapshot, sid).unwrap_or_else(|| sid.clone())
            }
            false => sid.clone(),
        }
    }
}

fn node_want(
    ctx: &ExecutionContext<'_>,
    names: &Names<'_>,
    bound: Option<&Binding>,
    term: &Ref,
) -> Result<Want> {
    Ok(match (bound, term) {
        (Some(b), _) => match decoded(ctx, b)? {
            Binding::Unbound => Want::Any,
            Binding::Sid { sid, .. } => Want::Node(sid),
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => names.iri(&iri),
            _ => Want::Nothing,
        },
        (None, Ref::Var(_)) => Want::Any,
        (None, Ref::Sid(sid)) => Want::Node(names.constant(ctx, sid)),
        (None, Ref::Iri(iri)) => names.iri(iri),
    })
}

fn object_want(
    ctx: &ExecutionContext<'_>,
    names: &Names<'_>,
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
            Binding::IriMatch { iri, .. } | Binding::Iri(iri) => node(names.iri(&iri)),
            Binding::Lit { val, dtc, .. } => ObjectWant::Value(val, Some(dtc)),
            _ => ObjectWant::Nothing,
        },
        (None, Term::Var(_)) => ObjectWant::Any,
        (None, Term::Sid(sid)) => ObjectWant::Node(names.constant(ctx, sid)),
        (None, Term::Iri(iri)) => node(names.iri(iri)),
        (None, Term::Value(FlakeValue::Ref(sid))) => ObjectWant::Node(names.constant(ctx, sid)),
        (None, Term::Value(v)) => ObjectWant::Value(v.clone(), dtc.cloned()),
    })
}

/// A list member as the binding of its value.
pub(crate) fn member_binding(member: &Flake) -> Binding {
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
    let l = view.ledger_of(graph.snapshot);
    let cell = view
        .decode_in(l, node)
        .filter(|(owner, _)| owner.0 == graph.g_id);
    let nil = Sid::new(RDF, RDF_NIL);
    let list_of = |owner: &Owner| (Want::Node(owner.1.clone()), Want::Node(owner.2.clone()));
    match (backward, &cell) {
        (false, Some((owner, k))) if is_rdf_first(pred) || is_rdf_rest(pred) => {
            let (s, p) = list_of(owner);
            for list in lists_in(ctx, graph, view, &s, &p).await?.iter() {
                if list.owner != *owner || *k >= list.cells() {
                    continue;
                }
                if is_rdf_rest(pred) {
                    out.push(
                        list.rest(*k)
                            .map_or_else(|| nil.clone(), |j| view.cell(l, owner, j)),
                    );
                } else {
                    match list.first(*k) {
                        First::Member(i) => {
                            if let FlakeValue::Ref(member) = &list.members[i].o {
                                out.push(member.clone());
                            }
                        }
                        First::Head(h) => out.push(view.cell(l, owner, h)),
                    }
                }
            }
        }
        (false, None) if view.may_hold_lists_in(l, pred) => {
            let (s, p) = (Want::Node(node.clone()), Want::Node(pred.clone()));
            for list in lists_in(ctx, graph, view, &s, &p).await?.iter() {
                out.push(view.cell(l, &list.owner, 0));
            }
        }
        (true, Some((owner, 0))) if owner.2 == *pred => out.push(owner.1.clone()),
        (true, Some((owner, k))) if is_rdf_first(pred) || is_rdf_rest(pred) => {
            let (s, p) = list_of(owner);
            for list in lists_in(ctx, graph, view, &s, &p).await?.iter() {
                if list.owner != *owner || *k >= list.cells() {
                    continue;
                }
                let from = if is_rdf_rest(pred) {
                    list.prev(*k)
                } else {
                    list.up(*k)
                };
                out.extend(from.map(|j| view.cell(l, owner, j)));
            }
        }
        (true, None) if is_rdf_rest(pred) && *node == nil => {
            for list in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
                .await?
                .iter()
            {
                for k in (0..list.cells()).filter(|&k| list.rest(k).is_none()) {
                    out.push(view.cell(l, &list.owner, k));
                }
            }
        }
        (true, None) if is_rdf_first(pred) => {
            for list in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
                .await?
                .iter()
            {
                for k in 0..list.cells() {
                    if let First::Member(i) = list.first(k) {
                        if list.members[i].o == FlakeValue::Ref(node.clone()) {
                            out.push(view.cell(l, &list.owner, k));
                        }
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
        let l = view.ledger_of(graph.snapshot);
        if !(first || rest) {
            let p = Want::Node(pred.clone());
            for list in lists_in(ctx, graph, view, &Want::Any, &p).await?.iter() {
                out.push((list.owner.1.clone(), view.cell(l, &list.owner, 0)));
            }
            continue;
        }
        for list in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
            .await?
            .iter()
        {
            let owner = &list.owner;
            for k in 0..list.cells() {
                let here = view.cell(l, owner, k);
                if rest {
                    let next = list
                        .rest(k)
                        .map_or_else(|| Sid::new(RDF, RDF_NIL), |j| view.cell(l, owner, j));
                    out.push((here, next));
                } else {
                    match list.first(k) {
                        First::Member(i) => {
                            if let FlakeValue::Ref(m) = &list.members[i].o {
                                out.push((here, m.clone()));
                            }
                        }
                        First::Head(h) => out.push((here, view.cell(l, owner, h))),
                    }
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
        let l = view.ledger_of(graph.snapshot);
        for list in lists_in(ctx, graph, view, &Want::Any, &Want::Any)
            .await?
            .iter()
        {
            out.extend((0..list.cells()).map(|k| view.cell(l, &list.owner, k)));
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
    let key: ListRead = (
        view.ledger_of(graph.snapshot),
        graph.g_id,
        graph.to_t,
        s.clone(),
        p.clone(),
    );
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
    let l = view.ledger_of(graph.snapshot);
    let reads: Vec<(IndexType, RangeMatch)> = match (s, p) {
        (Want::Nothing, _) | (_, Want::Nothing) => return Ok(Vec::new()),
        (Want::Node(s), Want::Node(p)) => {
            if !view.may_hold_lists_in(l, p) {
                return Ok(Vec::new());
            }
            vec![(
                IndexType::Spot,
                RangeMatch::new()
                    .with_subject(s.clone())
                    .with_predicate(p.clone()),
            )]
        }
        (Want::Node(s), Want::Any) => match view.ledger_predicates(l) {
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
            if !view.may_hold_lists_in(l, p) {
                return Ok(Vec::new());
            }
            vec![(IndexType::Psot, RangeMatch::new().with_predicate(p.clone()))]
        }
        (Want::Any, Want::Any) => match view.ledger_predicates(l) {
            Some(predicates) => predicates
                .iter()
                .map(|p| (IndexType::Psot, RangeMatch::new().with_predicate(p.clone())))
                .collect(),
            None => vec![(IndexType::Spot, RangeMatch::new())],
        },
    };
    let mut by_owner: Vec<(Owner, Vec<Flake>)> = Vec::new();
    let mut slot: HashMap<Owner, usize> = HashMap::new();
    // Every leaf of each list holding lists, those a policy hides included:
    // the tree reads around its hidden leaves.
    let mut trees: HashMap<Owner, Vec<Flake>> = HashMap::new();
    let owner_of = |f: &Flake| {
        let list = f.m.as_ref().map_or(0, |m| m.list);
        (graph.g_id, f.s.clone(), f.p.clone(), list)
    };
    for (index, range_match) in reads {
        let flakes = range_with_overlay_tracked(
            graph.snapshot,
            graph.g_id,
            graph.overlay,
            index,
            RangeTest::Eq,
            range_match,
            RangeOptions::new().with_to_t(graph.to_t),
            Some(&ctx.tracker),
        )
        .await?;
        let members: Vec<Flake> = flakes
            .into_iter()
            .filter(|f| f.op && f.m.as_ref().is_some_and(|m| m.i.is_some()))
            .collect();
        ctx.tracker.consume_fuel(
            members.len() as u64 * fluree_db_core::tracking::schedule::PER_ROW_MICRO_FUEL,
        )?;
        let in_tree = |f: &&Flake| f.m.as_ref().is_some_and(|m| list_index::is_nested(m.list));
        for leaf in members.iter().filter(in_tree) {
            trees.entry(owner_of(leaf)).or_default().push(leaf.clone());
        }
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
            let owner = owner_of(&member);
            let n = *slot.entry(owner.clone()).or_insert_with(|| {
                by_owner.push((owner, Vec::new()));
                by_owner.len() - 1
            });
            by_owner[n].1.push(member);
        }
    }
    let position = |f: &Flake| f.m.as_ref().and_then(|m| m.i);
    Ok(by_owner
        .into_iter()
        .map(|(owner, mut members)| {
            members.sort_by_key(position);
            match trees.get_mut(&owner) {
                Some(all) => {
                    all.sort_by_key(position);
                    List::of_tree(owner, members, all)
                }
                None => List::flat(owner, members),
            }
        })
        .collect())
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
        if batch.is_empty() {
            return Ok(Some(batch));
        }
        let graphs = ctx.path_graphs()?;
        let names = Names::of(ctx, &graphs);
        let mut rows = Vec::new();
        for row in 0..batch.len() {
            let subject = match &self.pattern.s {
                Ref::Var(v) => batch.get(row, *v).cloned().unwrap_or(Binding::Unbound),
                Ref::Sid(sid) => Binding::sid(names.constant(ctx, sid)),
                Ref::Iri(iri) => Binding::Iri(iri.clone()),
            };
            if self.view.may_meet(&graphs, &subject)? {
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
        self.pending = None;
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
        self.pending = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.join.estimated_rows()
    }
}
