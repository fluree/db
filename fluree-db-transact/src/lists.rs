//! Storing the lists a transaction writes.
//!
//! A list — a Turtle `( … )` or JSON-LD `@list` in object position — is
//! stored compactly when it can be: its members become values of the owning
//! graph, subject and predicate, each carrying its position
//! ([`FlakeMeta::i`]). A position names no list, so only one list on a
//! property can be stored that way. Every other list there — a second one in
//! the transaction, or one beside list members the ledger holds and the
//! transaction keeps — is stored as an explicit `rdf:first`/`rdf:rest` chain
//! of fresh blank nodes. Both shapes are the same RDF; two compact lists on
//! one property would merge into one.

use crate::current_facts::{CurrentFacts, ListFree, Slot};
use crate::error::Result;
use crate::namespace::BLANK_NODE_ID_PREFIX;
use fluree_db_core::{id_datatype_sid, Flake, FlakeMeta, FlakeValue, Sid};
use fluree_db_ledger::LedgerState;
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::predicates::{RDF_FIRST, RDF_NIL, RDF_REST};
use std::collections::HashSet;

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

/// The owners whose list is stored compactly, shared by the parts of one
/// write stored separately — the chunks of a bulk import — so a second list
/// on one property, in any part, is stored as a chain.
pub type ClaimedOwners = std::sync::Arc<std::sync::Mutex<HashSet<Owner>>>;

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

    fn into_compact(self) -> Vec<Flake> {
        self.members
    }

    /// `s p cell0`, then `cellK rdf:first memberK` and `cellK rdf:rest` the
    /// next cell or `rdf:nil`.
    fn into_chain(self) -> Vec<Flake> {
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

/// The flakes that store `lists`: each compact when it is the first list on
/// its graph, subject and predicate and the ledger holds no list members
/// there that survive the transaction, a chain otherwise.
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
    let held = owners_holding_lists(ledger, &lists, txn_id, retracted).await?;
    Ok(encode(lists, &held))
}

/// [`store_lists`] for a document no ledger holds: each list compact when it
/// is the first on its graph, subject and predicate, a chain otherwise.
pub fn store_new_lists(lists: Vec<PendingList>) -> Vec<Flake> {
    encode(lists, &HashSet::new())
}

/// [`store_new_lists`] for one part of a write whose parts are stored
/// separately: an owner another part has claimed is taken.
///
/// Which part's list is the compact one depends on the order parts finish,
/// not on the data; the stored RDF is the same either way.
pub fn store_lists_claiming(lists: Vec<PendingList>, claimed: &ClaimedOwners) -> Vec<Flake> {
    let mut out = Vec::new();
    for list in lists {
        let Some(owner) = list.owner() else {
            continue;
        };
        let compact = claimed
            .lock()
            .map(|mut owners| owners.insert(owner))
            .unwrap_or(false);
        if compact {
            out.extend(list.into_compact());
        } else {
            out.extend(list.into_chain());
        }
    }
    out
}

fn encode(lists: Vec<PendingList>, held: &HashSet<Owner>) -> Vec<Flake> {
    let mut claimed: HashSet<Owner> = HashSet::new();
    let mut out = Vec::new();
    for list in lists {
        let Some(owner) = list.owner() else {
            continue;
        };
        if !held.contains(&owner) && claimed.insert(owner) {
            out.extend(list.into_compact());
        } else {
            out.extend(list.into_chain());
        }
    }
    out
}

/// The owners among `lists` where the ledger holds list members that the
/// transaction does not retract.
async fn owners_holding_lists(
    ledger: &LedgerState,
    lists: &[PendingList],
    txn_id: &str,
    retracted: impl Fn(&Flake) -> bool,
) -> Result<HashSet<Owner>> {
    if ListFree::check(ledger).is_some() {
        return Ok(HashSet::new());
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
        return Ok(HashSet::new());
    }
    let read: Vec<Slot> = slots.iter().map(|(_, slot)| slot.clone()).collect();
    let facts = CurrentFacts::new(ledger).of_slots(&read).await?;
    Ok(slots
        .into_iter()
        .zip(facts)
        .filter(|(_, facts)| {
            facts
                .iter()
                .map(crate::current_facts::StoredFact::flake)
                .any(|f| f.m.as_ref().and_then(|m| m.i).is_some() && !retracted(f))
        })
        .map(|((owner, _), _)| owner)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// The first list on a property is stored compactly; a second is the
    /// chain a Turtle `( … )` denotes, its members with no position.
    #[test]
    fn a_second_list_on_one_property_is_a_chain() {
        let flakes = store_new_lists(vec![
            PendingList::new(vec![member(2, 1), member(1, 0)], "t--l0-".into()),
            PendingList::new(vec![member(3, 0), member(4, 1)], "t--l1-".into()),
        ]);
        let (first, rest) = (Sid::new(RDF, RDF_FIRST), Sid::new(RDF, RDF_REST));
        let ref_o = |s: &str| FlakeValue::Ref(cell(s));
        let triples: Vec<(Sid, Sid, FlakeValue, Option<i32>)> = flakes
            .iter()
            .map(|f| {
                (
                    f.s.clone(),
                    f.p.clone(),
                    f.o.clone(),
                    f.m.as_ref().and_then(|m| m.i),
                )
            })
            .collect();
        let (s, p) = (Sid::new(100, "s"), Sid::new(100, "p"));
        assert_eq!(
            triples,
            [
                (s.clone(), p.clone(), FlakeValue::Long(1), Some(0)),
                (s.clone(), p.clone(), FlakeValue::Long(2), Some(1)),
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
