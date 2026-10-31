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
use fluree_db_core::{id_datatype_sid, Flake, FlakeMeta, FlakeValue, Sid};
use fluree_db_ledger::LedgerState;
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::predicates::{RDF_FIRST, RDF_NIL, RDF_REST};
use std::collections::{BTreeMap, HashMap, HashSet};

/// A list a transaction writes, before it is decided how to store it.
#[derive(Debug, Clone)]
pub struct PendingList {
    /// The members in compact form: assertions of one graph, subject and
    /// predicate, each carrying its position, in position order.
    members: Vec<Flake>,
    /// Cell `k` of the chain, should the list be stored as one, is the blank
    /// node `fdb-{cells}{k}`.
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

    fn owner(&self) -> Option<Owner> {
        let first = self.members.first()?;
        Some((first.g.clone(), first.s.clone(), first.p.clone()))
    }

    /// The members as list `list` of their owner.
    fn into_compact_as(self, list: u32) -> Vec<Flake> {
        let mut members = self.members;
        for member in &mut members {
            if let Some(m) = member.m.as_mut() {
                m.list = list;
            }
        }
        members
    }

    /// `s p cell0`, then `cellK rdf:first memberK` and `cellK rdf:rest` the
    /// next cell or `rdf:nil`.
    pub(crate) fn into_chain(self) -> Vec<Flake> {
        let Some(first) = self.members.first() else {
            return Vec::new();
        };
        let (g, t) = (first.g.clone(), first.t);
        let cell = |k: usize| {
            Sid::new(
                BLANK_NODE,
                format!("{BLANK_NODE_ID_PREFIX}-{}{k}", self.cells),
            )
        };
        let id = id_datatype_sid();
        let make = |s: Sid, p: Sid, o: FlakeValue, dt: Sid, m: Option<FlakeMeta>| match &g {
            Some(g) => Flake::new_in_graph(g.clone(), s, p, o, dt, t, true, m),
            None => Flake::new(s, p, o, dt, t, true, m),
        };
        let (rdf_first, rdf_rest) = (Sid::new(RDF, RDF_FIRST), Sid::new(RDF, RDF_REST));
        let last = self.members.len() - 1;
        let mut out = Vec::with_capacity(2 * self.members.len() + 1);
        out.push(make(
            first.s.clone(),
            first.p.clone(),
            FlakeValue::Ref(cell(0)),
            id.clone(),
            None,
        ));
        for (k, member) in self.members.iter().enumerate() {
            let lang = member.m.as_ref().and_then(|m| m.lang.as_deref());
            out.push(make(
                cell(k),
                rdf_first.clone(),
                member.o.clone(),
                member.dt.clone(),
                FlakeMeta::from_parts(lang, None),
            ));
            let rest = if k == last {
                Sid::new(RDF, RDF_NIL)
            } else {
                cell(k + 1)
            };
            out.push(make(
                cell(k),
                rdf_rest.clone(),
                FlakeValue::Ref(rest),
                id.clone(),
                None,
            ));
        }
        out
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
        if let FlakeValue::Ref(o) = &f.o {
            if of.contains_key(o) {
                *into.entry(o).or_default() += 1;
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
    /// The id a new compact list takes: one past the highest held, or
    /// `None` when every id is taken.
    fn next_list(&self) -> Option<u32> {
        match self.compact.keys().next_back() {
            Some(&last) => last.checked_add(1),
            None => Some(0),
        }
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
            *next = next.checked_add(1)?;
            Some(id)
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

/// Whether two lists hold the same members in the same order.
fn same_members(a: &[Flake], b: &[Flake]) -> bool {
    let lang = |f: &Flake| f.m.as_ref().and_then(|m| m.lang.clone());
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.o == y.o && x.dt == y.dt && lang(x) == lang(y))
}

/// The lists the ledger holds on the owners of `lists`, leaving out the
/// members, and the chains' edges, the transaction retracts.
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

    /// A property holding every list id stores a further list as the chain
    /// a Turtle `( … )` denotes, its members with no place.
    #[test]
    fn a_property_holding_every_list_stores_a_chain() {
        let (s, p) = (Sid::new(100, "s"), Sid::new(100, "p"));
        let mut last = member(9, 0);
        last.m = FlakeMeta::from_place(None, Some((u32::MAX, 0)));
        let held = HashMap::from([(
            (None, s.clone(), p.clone()),
            Held {
                compact: BTreeMap::from([(u32::MAX, vec![last])]),
                chains: Vec::new(),
            },
        )]);
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
}
