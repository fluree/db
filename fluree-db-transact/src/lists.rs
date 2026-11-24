//! Storing the lists a transaction writes.
//!
//! A list — a Turtle `( … )` or JSON-LD `@list` in object position — is
//! stored compactly: its members become values of the owning graph, subject
//! and predicate, each carrying its place, which list of the owner it is in
//! and its position there ([`FlakeMeta::place`]). A second list on a property,
//! in the transaction or beside a list the ledger holds and the transaction
//! keeps, takes the next list id, so the lists stay apart. Only a property
//! holding every list id stores further lists as explicit
//! `rdf:first`/`rdf:rest` chains of fresh blank nodes; both shapes are the
//! same RDF.
//!
//! A list with the same members, in the same order, as a list its property
//! already holds is that list: writing it again restates it as stored, the
//! compact list's members in the list and at the positions they are stored
//! at, a chain's triples with its cells, as writing a stored fact again
//! restates the fact.
//!
//! A list id is local to its owner and names nothing beyond one state: a
//! list whose members are all deleted is gone, and when its id was the
//! highest there, the next list written takes it again.

use crate::current_facts::{CurrentFacts, ListFree, Slot};
use crate::error::Result;
use crate::namespace::BLANK_NODE_ID_PREFIX;
use fluree_db_core::list_index::{self, Item, Leaf, NESTED};
use fluree_db_core::{id_datatype_sid, Flake, FlakeMeta, FlakeValue, Sid};
use fluree_db_ledger::LedgerState;
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::predicates::{RDF_FIRST, RDF_NIL, RDF_REST};
use std::collections::{BTreeMap, HashMap, HashSet};

/// A list a transaction writes, before it is decided how to store it.
#[derive(Debug, Clone)]
pub struct PendingList {
    /// The members in compact form: assertions of one graph, subject and
    /// predicate, each carrying its position, in position order. A list
    /// holding lists is a tree: its members are its leaves, their list
    /// [`NESTED`] ([`fluree_db_core::list_index`]).
    members: Vec<Flake>,
    /// Cell `k` of the chain, should the list be stored as one, is the blank
    /// node `fdb-{cells}{k}`; cell `j` of the list it holds at `k` is
    /// `fdb-{cells}{k}_{j}`.
    cells: String,
}

/// The graph, subject and predicate a list belongs to.
pub type Owner = (Option<Sid>, Sid, Sid);

/// The next list id of each owner a list is stored on, shared by the parts of
/// one write stored separately — the chunks of a bulk import — so a second
/// list on one property, in any part, takes an id of its own.
pub type ClaimedOwners = std::sync::Arc<std::sync::Mutex<HashMap<Owner, u32>>>;

impl PendingList {
    /// A list of `members`, compact assertions of one graph, subject and
    /// predicate carrying their positions. Its chain's cells, should it be
    /// stored as one, are the blank nodes `fdb-{cells}{k}`: `cells` must be
    /// unique to this list in the transaction.
    pub fn new(mut members: Vec<Flake>, cells: String) -> Self {
        members.sort_by_key(|f| f.m.as_ref().and_then(|m| m.i));
        Self { members, cells }
    }

    /// The members, in position order.
    pub fn members(&self) -> &[Flake] {
        &self.members
    }

    fn owner(&self) -> Option<Owner> {
        let first = self.members.first()?;
        Some((first.g.clone(), first.s.clone(), first.p.clone()))
    }

    /// The members as list `list` of their owner, a tree's id [`NESTED`].
    fn into_compact_as(self, list: u32) -> Vec<Flake> {
        let mut members = self.members;
        for member in &mut members {
            if let Some(m) = member.m.as_mut() {
                m.list = list | (m.list & NESTED);
            }
        }
        members
    }

    /// `s p cell0`, then `cellK rdf:first memberK` and `cellK rdf:rest` the
    /// next cell or `rdf:nil`. A list the tree holds at `k` is a chain of its
    /// own, headed by `cellK`'s `rdf:first`.
    pub(crate) fn into_chain(self) -> Vec<Flake> {
        let Some(first) = self.members.first() else {
            return Vec::new();
        };
        let (g, s, p, t) = (first.g.clone(), first.s.clone(), first.p.clone(), first.t);
        let items = list_index::items(self.members.into_iter().map(|f| {
            let leaf = f.m.as_ref().and_then(FlakeMeta::leaf);
            (f, leaf.unwrap_or_default())
        }));
        let chain = Chain {
            g,
            t,
            cells: self.cells,
        };
        let mut out = Vec::new();
        out.push(chain.flake(
            s,
            p,
            FlakeValue::Ref(chain.cell("0")),
            id_datatype_sid(),
            None,
        ));
        chain.write(items, "", &mut out);
        out
    }
}

/// Where [`PendingList::into_chain`] writes a list's cells.
struct Chain {
    g: Option<Sid>,
    t: i64,
    cells: String,
}

impl Chain {
    fn cell(&self, label: &str) -> Sid {
        Sid::new(
            BLANK_NODE,
            format!("{BLANK_NODE_ID_PREFIX}-{}{label}", self.cells),
        )
    }

    fn flake(&self, s: Sid, p: Sid, o: FlakeValue, dt: Sid, m: Option<FlakeMeta>) -> Flake {
        match &self.g {
            Some(g) => Flake::new_in_graph(g.clone(), s, p, o, dt, self.t, true, m),
            None => Flake::new(s, p, o, dt, self.t, true, m),
        }
    }

    /// The cells of `items`, cell `k` labelled `{prefix}{k}`.
    fn write(&self, items: Vec<Item<Flake>>, prefix: &str, out: &mut Vec<Flake>) {
        let (rdf_first, rdf_rest) = (Sid::new(RDF, RDF_FIRST), Sid::new(RDF, RDF_REST));
        let last = items.len().saturating_sub(1);
        for (k, item) in items.into_iter().enumerate() {
            let cell = self.cell(&format!("{prefix}{k}"));
            let inner = format!("{prefix}{k}_");
            let held = match item {
                Item::Value(member) => {
                    let lang = member.m.as_ref().and_then(|m| m.lang.as_deref());
                    let m = FlakeMeta::from_parts(lang, None);
                    out.push(self.flake(cell.clone(), rdf_first.clone(), member.o, member.dt, m));
                    None
                }
                Item::List(list) => {
                    let head = FlakeValue::Ref(self.cell(&format!("{inner}0")));
                    out.push(self.flake(
                        cell.clone(),
                        rdf_first.clone(),
                        head,
                        id_datatype_sid(),
                        None,
                    ));
                    Some(list)
                }
            };
            let rest = if k == last {
                Sid::new(RDF, RDF_NIL)
            } else {
                self.cell(&format!("{prefix}{}", k + 1))
            };
            out.push(self.flake(
                cell,
                rdf_rest.clone(),
                FlakeValue::Ref(rest),
                id_datatype_sid(),
                None,
            ));
            if let Some(list) = held {
                self.write(list, &inner, out);
            }
        }
    }
}

/// The lists among the assertions `flakes` written as explicit chains, each
/// as its members in compact form (see [`PendingList::new`]), and the
/// assertions left. A chain is an edge, by a subject that is no cell and not
/// on `rdf:first` / `rdf:rest`, to a cell, each cell holding one `rdf:first`
/// and one `rdf:rest` in the edge's graph, the last's `rdf:nil`, and named
/// by nothing else. `is_cell` says which nodes may be cells: blank nodes
/// nothing outside the assertions can name.
pub fn chain_lists(
    flakes: Vec<Flake>,
    is_cell: impl Fn(&Sid) -> bool,
) -> (Vec<Flake>, Vec<Vec<Flake>>) {
    let (first, rest, nil) = (
        Sid::new(RDF, RDF_FIRST),
        Sid::new(RDF, RDF_REST),
        Sid::new(RDF, RDF_NIL),
    );
    let mut of: HashMap<&Sid, Vec<usize>> = HashMap::new();
    let mut into: HashMap<&Sid, usize> = HashMap::new();
    for f in &flakes {
        if f.p == first && is_cell(&f.s) {
            of.entry(&f.s).or_default();
        }
    }
    if of.is_empty() {
        return (flakes, Vec::new());
    }
    for (i, f) in flakes.iter().enumerate() {
        if let Some(triples) = of.get_mut(&f.s) {
            triples.push(i);
        }
        // A triple term about a cell names it too: an annotated cell's.
        let (term_s, o) = match &f.o {
            FlakeValue::TripleTerm(term) => (Some(&term.s), &term.o),
            o => (None, o),
        };
        for named in term_s.into_iter().chain(match o {
            FlakeValue::Ref(o) => Some(o),
            _ => None,
        }) {
            if of.contains_key(named) {
                *into.entry(named).or_default() += 1;
            }
        }
    }
    let mut taken = vec![false; flakes.len()];
    let mut lists = Vec::new();
    'edges: for (e, edge) in flakes.iter().enumerate() {
        let FlakeValue::Ref(head) = &edge.o else {
            continue;
        };
        if !of.contains_key(head) || is_cell(&edge.s) || edge.p == first || edge.p == rest {
            continue;
        }
        let mut used = vec![e];
        let mut firsts = Vec::new();
        let mut at = head;
        while *at != nil {
            let Some(triples) = of.get(at) else {
                continue 'edges;
            };
            let [a, b] = triples[..] else {
                continue 'edges;
            };
            let (f, r) = if flakes[a].p == first { (a, b) } else { (b, a) };
            let (fl, rl) = (&flakes[f], &flakes[r]);
            let plain = fl.p == first
                && rl.p == rest
                && fl.g == edge.g
                && rl.g == edge.g
                && into.get(at) == Some(&1)
                && !used.contains(&f);
            let FlakeValue::Ref(next) = &rl.o else {
                continue 'edges;
            };
            if !plain {
                continue 'edges;
            }
            used.extend([f, r]);
            firsts.push(f);
            at = next;
        }
        for &u in &used {
            taken[u] = true;
        }
        lists.push(
            firsts
                .iter()
                .enumerate()
                .map(|(k, &f)| {
                    let member = &flakes[f];
                    let lang = member.m.as_ref().and_then(|m| m.lang.as_deref());
                    Flake {
                        g: edge.g.clone(),
                        s: edge.s.clone(),
                        p: edge.p.clone(),
                        o: member.o.clone(),
                        dt: member.dt.clone(),
                        t: edge.t,
                        op: true,
                        m: FlakeMeta::from_parts(lang, Some(k as i32)),
                    }
                })
                .collect(),
        );
    }
    let left = flakes
        .into_iter()
        .zip(taken)
        .filter(|(_, taken)| !taken)
        .map(|(f, _)| f)
        .collect();
    (left, lists)
}

/// `lists` with each member that heads a list of the document's own cells
/// taken in as the list it holds, and `flakes` without the cells' triples.
///
/// A member heads such a list when it alone names a cell `is_cell` admits,
/// each cell holding one `rdf:first` and one `rdf:rest` in the member's graph
/// and named by nothing else, the last's `rdf:rest` `rdf:nil`. A cell's
/// `rdf:first` may head one in turn. This is how a Turtle `((1 2) 3)` arrives:
/// the outer list's items, the inner list as the chain `( … )` denotes. A list
/// past the limits of a tree ([`list_index::TreeLimit`]) keeps its inner lists
/// as chains.
pub fn nest_lists(
    lists: Vec<PendingList>,
    flakes: Vec<Flake>,
    is_cell: impl Fn(&Sid) -> bool,
) -> (Vec<Flake>, Vec<PendingList>) {
    let heads_cell = |f: &Flake| matches!(&f.o, FlakeValue::Ref(o) if is_cell(o));
    if !lists.iter().any(|l| l.members.iter().any(heads_cell)) {
        return (flakes, lists);
    }
    let (first, rest) = (Sid::new(RDF, RDF_FIRST), Sid::new(RDF, RDF_REST));
    // Every triple about each cell that holds an `rdf:first`: a cell saying
    // anything more is no plain cell.
    let mut of: HashMap<&Sid, Vec<usize>> = HashMap::new();
    for f in &flakes {
        if f.p == first && is_cell(&f.s) {
            of.entry(&f.s).or_default();
        }
    }
    if of.is_empty() {
        return (flakes, lists);
    }
    for (i, f) in flakes.iter().enumerate() {
        if let Some(triples) = of.get_mut(&f.s) {
            triples.push(i);
        }
    }
    // What names each cell: a reference to it, or a triple term about it.
    let mut into: HashMap<&Sid, usize> = HashMap::new();
    for f in flakes
        .iter()
        .chain(lists.iter().flat_map(|l| l.members.iter()))
    {
        let mut name = |sid: &Sid| {
            if let Some((&cell, _)) = of.get_key_value(sid) {
                *into.entry(cell).or_default() += 1;
            }
        };
        match &f.o {
            FlakeValue::Ref(o) => name(o),
            FlakeValue::TripleTerm(term) => {
                name(&term.s);
                if let FlakeValue::Ref(o) = &term.o {
                    name(o);
                }
            }
            _ => {}
        }
    }
    let cells = Cells {
        flakes: &flakes,
        of: &of,
        into: &into,
        first: &first,
        rest: &rest,
    };
    let mut taken = vec![false; flakes.len()];
    let lists: Vec<PendingList> = lists
        .into_iter()
        .map(|list| {
            if !list.members.iter().any(heads_cell) {
                return list;
            }
            let mut used = Vec::new();
            let items: Option<Vec<Item<Flake>>> = list
                .members
                .iter()
                .map(|m| cells.expand(m, m, 0, &mut used))
                .collect();
            let Some(items) = items.filter(|items| list_index::holds_list(items)) else {
                return list;
            };
            let owner = &list.members[0];
            let nil = || Flake {
                o: FlakeValue::Ref(Sid::new(RDF, RDF_NIL)),
                dt: id_datatype_sid(),
                m: None,
                ..owner.clone()
            };
            let Ok(leaves) = list_index::leaves(items, nil) else {
                return list;
            };
            for i in used {
                taken[i] = true;
            }
            let members = leaves
                .into_iter()
                .map(|(mut f, leaf)| {
                    let lang = f.m.as_ref().and_then(|m| m.lang.clone());
                    f.m = FlakeMeta::from_place(
                        lang.as_deref(),
                        Some((NESTED, leaf.nested_position())),
                    );
                    f
                })
                .collect();
            PendingList::new(members, list.cells)
        })
        .collect();
    let left = flakes
        .into_iter()
        .zip(taken)
        .filter(|(_, taken)| !taken)
        .map(|(f, _)| f)
        .collect();
    (left, lists)
}

/// A document's cells, for [`nest_lists`].
struct Cells<'a> {
    flakes: &'a [Flake],
    of: &'a HashMap<&'a Sid, Vec<usize>>,
    into: &'a HashMap<&'a Sid, usize>,
    first: &'a Sid,
    rest: &'a Sid,
}

impl Cells<'_> {
    /// `value`, an item of `owner`'s list `depth` lists below its top level:
    /// the list it heads, or itself. `None` when a list it holds would sit
    /// deeper than a tree can. The cells' triples taken go to `used`.
    fn expand(
        &self,
        owner: &Flake,
        value: &Flake,
        depth: u8,
        used: &mut Vec<usize>,
    ) -> Option<Item<Flake>> {
        let leaf = || {
            let lang = value.m.as_ref().and_then(|m| m.lang.as_deref());
            Item::Value(Flake {
                o: value.o.clone(),
                dt: value.dt.clone(),
                m: FlakeMeta::from_parts(lang, None),
                ..owner.clone()
            })
        };
        let Some(firsts) = self.chain(owner, &value.o) else {
            return Some(leaf());
        };
        if depth == list_index::MAX_DEPTH {
            return None;
        }
        let mut inner = Vec::with_capacity(firsts.len());
        let mut taken = Vec::new();
        for (f, r) in firsts {
            taken.extend([f, r]);
            inner.push(self.expand(owner, &self.flakes[f], depth + 1, &mut taken)?);
        }
        used.extend(taken);
        Some(Item::List(inner))
    }

    /// The `rdf:first` and `rdf:rest` triples of the chain `head` heads, in
    /// order, when it is one of the document's own in `owner`'s graph.
    fn chain(&self, owner: &Flake, head: &FlakeValue) -> Option<Vec<(usize, usize)>> {
        let FlakeValue::Ref(head) = head else {
            return None;
        };
        let mut at = head;
        let mut cells = Vec::new();
        while !(at.namespace_code == RDF && at.name.as_ref() == RDF_NIL) {
            let triples = self.of.get(at)?;
            let [a, b] = triples[..] else {
                return None;
            };
            let (f, r) = if self.flakes[a].p == *self.first {
                (a, b)
            } else {
                (b, a)
            };
            let (fl, rl) = (&self.flakes[f], &self.flakes[r]);
            if fl.p != *self.first
                || rl.p != *self.rest
                || fl.g != owner.g
                || rl.g != owner.g
                || self.into.get(at) != Some(&1)
            {
                return None;
            }
            let FlakeValue::Ref(next) = &rl.o else {
                return None;
            };
            cells.push((f, r));
            at = next;
        }
        Some(cells)
    }
}

/// The flakes that store `lists`: each compact under the next list id of its
/// graph, subject and predicate, past the lists there that survive the
/// transaction, or its stored members again when it restates one of them.
///
/// `retracted` says whether the transaction retracts a stored fact, and must
/// be asked before retractions cancel against assertions: an upsert that
/// restates a stored list retracts its members and asserts them again.
/// Subjects that are blank nodes minted by transaction `txn_id` hold nothing
/// stored, and are not read.
pub async fn store_lists(
    ledger: &LedgerState,
    lists: Vec<PendingList>,
    txn_id: &str,
    retracted: impl Fn(&Flake) -> bool,
) -> Result<Vec<Flake>> {
    if lists.is_empty() {
        return Ok(Vec::new());
    }
    let held = lists_held(ledger, &lists, txn_id, retracted).await?;
    Ok(encode(lists, held))
}

/// [`store_lists`] for a document no ledger holds: each list compact, under
/// the next list id of its graph, subject and predicate.
pub fn store_new_lists(lists: Vec<PendingList>) -> Vec<Flake> {
    encode(lists, HashMap::new())
}

/// The lists an owner holds: each compact list's members in position order,
/// by list id, and each list it holds as a chain, as the chain's triples: the
/// owner's edge, then each cell's `rdf:first` and `rdf:rest`.
#[derive(Debug, Default)]
struct Held {
    compact: BTreeMap<u32, Vec<Flake>>,
    chains: Vec<Vec<Flake>>,
}

impl Held {
    /// The id a new compact list takes: one past the highest held, flat or
    /// a tree, or `None` when every id is taken.
    fn next_list(&self) -> Option<u32> {
        let next = match self.compact.keys().map(|&l| list_index::id_of(l)).max() {
            Some(last) => last + 1,
            None => 0,
        };
        (next < list_index::MAX_LISTS).then_some(next)
    }
}

/// Whether `chain`, a chain's triples, holds `members` in order.
fn chain_holds(chain: &[Flake], members: &[Flake]) -> bool {
    let firsts: Vec<Flake> = chain
        .iter()
        .filter(|f| f.p.namespace_code == RDF && f.p.name.as_ref() == RDF_FIRST)
        .cloned()
        .collect();
    same_members(&firsts, members)
}

/// [`store_new_lists`] for one part of a write whose parts are stored
/// separately: each list takes its owner's next id across every part.
///
/// Which list takes which id depends on the order parts finish, not on the
/// data; the stored RDF is the same either way.
pub fn store_lists_claiming(lists: Vec<PendingList>, claimed: &ClaimedOwners) -> Vec<Flake> {
    let mut out = Vec::new();
    for list in lists {
        let Some(owner) = list.owner() else {
            continue;
        };
        let id = claimed.lock().ok().and_then(|mut owners| {
            let next = owners.entry(owner).or_insert(0);
            let id = *next;
            (id < list_index::MAX_LISTS).then(|| {
                *next += 1;
                id
            })
        });
        match id {
            Some(id) => out.extend(list.into_compact_as(id)),
            None => out.extend(list.into_chain()),
        }
    }
    out
}

/// Each list compact under its owner's next list id, restating the one there
/// that holds the same members, a chain when every id is taken. `held` starts
/// as the lists the ledger holds, by owner, and gains each stored here.
fn encode(lists: Vec<PendingList>, mut held: HashMap<Owner, Held>) -> Vec<Flake> {
    let mut out = Vec::new();
    for list in lists {
        let Some(owner) = list.owner() else {
            continue;
        };
        let here = held.entry(owner.clone()).or_default();
        // A list held is restated as stored: a compact list in its own list
        // and at its own positions, which a delete can leave apart, a chain
        // with its cells.
        let stored = here
            .compact
            .values()
            .find(|members| same_members(members, &list.members))
            .or_else(|| here.chains.iter().find(|c| chain_holds(c, &list.members)));
        if let Some(stored) = stored {
            let (g, t) = (owner.0.clone(), list.members.first().map_or(0, |f| f.t));
            out.extend(stored.iter().map(|f| Flake {
                g: g.clone(),
                t,
                op: true,
                ..f.clone()
            }));
        } else if let Some(id) = here.next_list() {
            let members = list.into_compact_as(id);
            here.compact.insert(id, members.clone());
            out.extend(members);
        } else {
            let chain = list.into_chain();
            here.chains.push(chain.clone());
            out.extend(chain);
        }
    }
    out
}

/// Whether two lists hold the same members in the same order, and the same
/// lists: a tree's leaves agree on their shape too. A chain's members, which
/// carry no place, are a flat list's.
fn same_members(a: &[Flake], b: &[Flake]) -> bool {
    let lang = |f: &Flake| f.m.as_ref().and_then(|m| m.lang.clone());
    let shape = |f: &Flake| {
        f.m.as_ref()
            .and_then(FlakeMeta::leaf)
            .map_or((0, 0), Leaf::shape)
    };
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.o == y.o && x.dt == y.dt && lang(x) == lang(y) && shape(x) == shape(y))
}

/// The lists the ledger holds on the owners of `lists`, leaving out the
/// members, and the chains' edges, the transaction retracts (see
/// [`store_lists`]).
async fn lists_held(
    ledger: &LedgerState,
    lists: &[PendingList],
    txn_id: &str,
    retracted: impl Fn(&Flake) -> bool,
) -> Result<HashMap<Owner, Held>> {
    if ListFree::check(ledger).is_some() {
        return Ok(HashMap::new());
    }
    let minted = format!("{BLANK_NODE_ID_PREFIX}-{txn_id}-");
    let mut owners: Vec<Owner> = Vec::new();
    let mut seen: HashSet<Owner> = HashSet::new();
    for owner in lists.iter().filter_map(PendingList::owner) {
        let fresh = owner.1.namespace_code == BLANK_NODE && owner.1.name.starts_with(&minted);
        if !fresh && seen.insert(owner.clone()) {
            owners.push(owner);
        }
    }
    let slots: Vec<(Owner, Slot)> = owners
        .into_iter()
        .filter_map(|o| Slot::in_ledger(ledger, &o.0, &o.1, &o.2).map(|slot| (o, slot)))
        .collect();
    if slots.is_empty() {
        return Ok(HashMap::new());
    }
    let read: Vec<Slot> = slots.iter().map(|(_, slot)| slot.clone()).collect();
    let facts = CurrentFacts::new(ledger).of_slots(&read).await?;
    let mut held = HashMap::new();
    for ((owner, slot), facts) in slots.into_iter().zip(facts) {
        let mut here = Held::default();
        for f in facts.iter().map(crate::current_facts::StoredFact::flake) {
            if retracted(f) {
                continue;
            }
            if let Some((list, _)) = f.m.as_ref().and_then(FlakeMeta::place) {
                here.compact.entry(list).or_default().push(f.clone());
                continue;
            }
            let FlakeValue::Ref(head) = &f.o else {
                continue;
            };
            if head.namespace_code != BLANK_NODE {
                continue;
            }
            let db = ledger.as_graph_db_ref(slot.g_id);
            let Some(firsts) = fluree_db_core::rdf_list::chain_members(db, head).await? else {
                continue;
            };
            let mut chain = Vec::with_capacity(2 * firsts.len() + 1);
            chain.push(f.clone());
            for (k, first) in firsts.iter().enumerate() {
                let next = firsts
                    .get(k + 1)
                    .map_or_else(|| Sid::new(RDF, RDF_NIL), |n| n.s.clone());
                chain.push(first.clone());
                chain.push(Flake::new(
                    first.s.clone(),
                    Sid::new(RDF, RDF_REST),
                    FlakeValue::Ref(next),
                    id_datatype_sid(),
                    first.t,
                    true,
                    None,
                ));
            }
            here.chains.push(chain);
        }
        for members in here.compact.values_mut() {
            members.sort_by_key(|f| f.m.as_ref().and_then(|m| m.i));
        }
        if !(here.compact.is_empty() && here.chains.is_empty()) {
            held.insert(owner, here);
        }
    }
    Ok(held)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::list_index::ListPlace;

    fn member(o: i64, i: i32) -> Flake {
        Flake::new(
            Sid::new(100, "s"),
            Sid::new(100, "p"),
            FlakeValue::Long(o),
            Sid::new(2, "integer"),
            1,
            true,
            FlakeMeta::from_parts(None, Some(i)),
        )
    }

    fn cell(name: &str) -> Sid {
        Sid::new(BLANK_NODE, format!("{BLANK_NODE_ID_PREFIX}-{name}"))
    }

    fn places(flakes: &[Flake]) -> Vec<(FlakeValue, Option<ListPlace>)> {
        flakes
            .iter()
            .map(|f| (f.o.clone(), f.m.as_ref().and_then(FlakeMeta::place)))
            .collect()
    }

    /// A list with a held list's members, in its order, is that list and
    /// restates them; one that differs, in order alone, is another list.
    #[test]
    fn a_list_restating_a_held_one_restates_its_members() {
        let restated = store_new_lists(vec![
            PendingList::new(vec![member(1, 0), member(2, 1)], "t--l0-".into()),
            PendingList::new(vec![member(1, 0), member(2, 1)], "t--l1-".into()),
        ]);
        assert_eq!(restated[2..], restated[..2], "the same members again");
        let reordered = store_new_lists(vec![
            PendingList::new(vec![member(1, 0), member(2, 1)], "t--l0-".into()),
            PendingList::new(vec![member(2, 0), member(1, 1)], "t--l1-".into()),
        ]);
        let long = FlakeValue::Long;
        assert_eq!(
            places(&reordered),
            [
                (long(1), Some((0, 0))),
                (long(2), Some((0, 1))),
                (long(2), Some((1, 0))),
                (long(1), Some((1, 1))),
            ]
        );
    }

    /// Each list on a property is stored compactly under its own list, in
    /// the order the transaction writes them.
    #[test]
    fn a_second_list_on_one_property_takes_the_next_list() {
        let flakes = store_new_lists(vec![
            PendingList::new(vec![member(2, 1), member(1, 0)], "t--l0-".into()),
            PendingList::new(vec![member(3, 0), member(4, 1)], "t--l1-".into()),
        ]);
        let long = FlakeValue::Long;
        assert_eq!(
            places(&flakes),
            [
                (long(1), Some((0, 0))),
                (long(2), Some((0, 1))),
                (long(3), Some((1, 0))),
                (long(4), Some((1, 1))),
            ]
        );
    }

    /// The parts of a write stored separately share their owners' ids.
    #[test]
    fn parts_stored_separately_take_distinct_lists() {
        let claimed = ClaimedOwners::default();
        let one = store_lists_claiming(
            vec![PendingList::new(vec![member(1, 0)], "t--l0-".into())],
            &claimed,
        );
        let two = store_lists_claiming(
            vec![PendingList::new(vec![member(1, 0)], "t--l1-".into())],
            &claimed,
        );
        assert_eq!(places(&one), [(FlakeValue::Long(1), Some((0, 0)))]);
        assert_eq!(places(&two), [(FlakeValue::Long(1), Some((1, 0)))]);
    }

    fn every_id_held(s: &Sid, p: &Sid) -> HashMap<Owner, Held> {
        let last_id = list_index::MAX_LISTS - 1;
        let mut last = member(9, 0);
        last.m = FlakeMeta::from_place(None, Some((last_id, 0)));
        HashMap::from([(
            (None, s.clone(), p.clone()),
            Held {
                compact: BTreeMap::from([(last_id, vec![last])]),
                chains: Vec::new(),
            },
        )])
    }

    /// A property holding every list id stores a further list as the chain
    /// a Turtle `( … )` denotes, its members with no place.
    #[test]
    fn a_property_holding_every_list_stores_a_chain() {
        let (s, p) = (Sid::new(100, "s"), Sid::new(100, "p"));
        let held = every_id_held(&s, &p);
        let flakes = encode(
            vec![PendingList::new(
                vec![member(3, 0), member(4, 1)],
                "t--l1-".into(),
            )],
            held,
        );
        let (first, rest) = (Sid::new(RDF, RDF_FIRST), Sid::new(RDF, RDF_REST));
        let ref_o = |s: &str| FlakeValue::Ref(cell(s));
        let triples: Vec<(Sid, Sid, FlakeValue, Option<ListPlace>)> = flakes
            .iter()
            .map(|f| {
                (
                    f.s.clone(),
                    f.p.clone(),
                    f.o.clone(),
                    f.m.as_ref().and_then(FlakeMeta::place),
                )
            })
            .collect();
        assert_eq!(
            triples,
            [
                (s, p, ref_o("t--l1-0"), None),
                (cell("t--l1-0"), first.clone(), FlakeValue::Long(3), None),
                (cell("t--l1-0"), rest.clone(), ref_o("t--l1-1"), None),
                (cell("t--l1-1"), first, FlakeValue::Long(4), None),
                (
                    cell("t--l1-1"),
                    rest,
                    FlakeValue::Ref(Sid::new(RDF, RDF_NIL)),
                    None
                ),
            ]
        );
    }

    /// A nested list stored as a chain holds each inner list as a chain of
    /// its own, headed by its cell's `rdf:first`.
    #[test]
    fn a_nested_list_stored_as_a_chain_nests_its_chains() {
        let (s, p) = (Sid::new(100, "s"), Sid::new(100, "p"));
        let leaf = |o: i64, pos, depth, opens| {
            let mut f = member(o, 0);
            let leaf = Leaf { pos, depth, opens };
            f.m = FlakeMeta::from_place(None, Some((NESTED, leaf.nested_position())));
            f
        };
        // ((1 2) 3)
        let tree = vec![leaf(1, 0, 1, 1), leaf(2, 1, 1, 0), leaf(3, 2, 0, 0)];
        let flakes = encode(
            vec![PendingList::new(tree, "t--l1-".into())],
            every_id_held(&s, &p),
        );
        let ref_o = |s: &str| FlakeValue::Ref(cell(s));
        let nil = FlakeValue::Ref(Sid::new(RDF, RDF_NIL));
        let triples: Vec<(Sid, String, FlakeValue)> = flakes
            .iter()
            .map(|f| (f.s.clone(), f.p.name.to_string(), f.o.clone()))
            .collect();
        let row = |s: &str, p: &str, o: FlakeValue| (cell(s), p.to_string(), o);
        assert_eq!(
            triples,
            [
                (s, "p".to_string(), ref_o("t--l1-0")),
                row("t--l1-0", RDF_FIRST, ref_o("t--l1-0_0")),
                row("t--l1-0", RDF_REST, ref_o("t--l1-1")),
                row("t--l1-0_0", RDF_FIRST, FlakeValue::Long(1)),
                row("t--l1-0_0", RDF_REST, ref_o("t--l1-0_1")),
                row("t--l1-0_1", RDF_FIRST, FlakeValue::Long(2)),
                row("t--l1-0_1", RDF_REST, nil.clone()),
                row("t--l1-1", RDF_FIRST, FlakeValue::Long(3)),
                row("t--l1-1", RDF_REST, nil),
            ]
        );
        assert!(flakes
            .iter()
            .all(|f| f.m.as_ref().and_then(FlakeMeta::place).is_none()));
    }
}
