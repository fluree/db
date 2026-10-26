//! Lists a write states as RDF does: as an owner's edge to a head cell and
//! a chain of `rdf:first`/`rdf:rest` cells.
//!
//! Two kinds of cell reach a write: those a SPARQL WHERE row names, which
//! nothing stores (`fluree_db_query::list_view` names them per request), and
//! the blank nodes the transaction mints, for a `( … )` or a chain written
//! out. The writes that name either are held here, with the triples of each
//! stored list a row names, and applied to them: deletions first, then
//! insertions (SPARQL 1.1 Update §3.1.3). Every chain then hanging from an
//! owner's edge, its cells plain and named by nothing else, is stored
//! compactly ([`crate::lists`]); every other triple as it is, a WHERE cell
//! as a blank node minted for it. Deleting the owner's edge leaves the
//! cells, as it does for any RDF list.
//!
//! Storing a list again can stage facts the write did not change as RDF:
//! the members of a list stored as triples instead, and the triples that
//! restate its cells. Those are reported apart ([`StoredEdits::unjudged`]),
//! so a modify policy judges what the write changed.

use crate::current_facts::{CurrentFacts, RetractIntent, Retraction, Slot, StoredFact};
use crate::error::{Result, TransactError};
use crate::lists::PendingList;
use crate::namespace::BLANK_NODE_ID_PREFIX;
use fluree_db_core::{id_datatype_sid, Flake, FlakeMeta, FlakeValue, GraphId, Sid};
use fluree_db_ledger::LedgerState;
use fluree_db_query::list_view::{ListView, Owner};
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::predicates::{RDF_FIRST, RDF_NIL, RDF_REST};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The writes of one SPARQL transaction that name a cell.
pub(crate) struct ListEdits {
    /// The view the WHERE read, when the ledger may hold lists.
    view: Option<Arc<ListView>>,
    txn_id: String,
    /// The prefix of the blank nodes the transaction mints.
    minted: String,
    retracts: Vec<Flake>,
    asserts: Vec<Flake>,
}

/// What storing the edited lists takes.
#[derive(Default)]
pub(crate) struct StoredEdits {
    /// The stored members of every list that changed.
    pub retractions: Vec<Retraction>,
    /// The triples stored as they are.
    pub assertions: Vec<Flake>,
    /// The lists stored compactly, unless their owner holds another.
    pub lists: Vec<PendingList>,
    /// Of the retractions and assertions, those that store again RDF the
    /// write leaves as it is: the members of a list none of whose triples it
    /// deletes, and the triples restating a list's cells.
    pub unjudged: Vec<Flake>,
}

/// A triple of the edited lists, in the graph it is in.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Triple {
    g: GraphId,
    s: Sid,
    p: Sid,
    o: FlakeValue,
    dt: Sid,
    lang: Option<String>,
}

impl ListEdits {
    /// The edits of transaction `txn_id`, whose WHERE read `view`.
    pub(crate) fn new(view: Option<Arc<ListView>>, txn_id: &str) -> Self {
        Self {
            view,
            txn_id: txn_id.to_string(),
            minted: format!("{BLANK_NODE_ID_PREFIX}-{txn_id}-"),
            retracts: Vec::new(),
            asserts: Vec::new(),
        }
    }

    /// Whether `templates` write a cell: a blank node's `rdf:first`.
    pub(crate) fn writes_chain(templates: &[crate::ir::TripleTemplate]) -> bool {
        templates.iter().any(|t| {
            matches!(t.subject, crate::ir::TemplateTerm::BlankNode(_))
                && matches!(&t.predicate, crate::ir::TemplateTerm::Sid(p) if fluree_db_core::is_rdf_first(p))
        })
    }

    pub(crate) fn view(&self) -> Option<&Arc<ListView>> {
        self.view.as_ref()
    }

    fn view_cell(&self, sid: &Sid) -> Option<(Owner, usize)> {
        if sid.namespace_code != BLANK_NODE {
            return None;
        }
        self.view.as_ref()?.cell_of(sid)
    }

    fn is_cell(&self, sid: &Sid) -> bool {
        sid.namespace_code == BLANK_NODE
            && (sid.name.starts_with(&self.minted) || self.view_cell(sid).is_some())
    }

    fn names_cell(&self, f: &Flake) -> bool {
        self.is_cell(&f.s) || ref_of(&f.o).is_some_and(|o| self.is_cell(o))
    }

    /// The intents that name no cell; those that do are held.
    pub(crate) fn divert_intents(&mut self, intents: Vec<RetractIntent>) -> Vec<RetractIntent> {
        let (cells, rest): (Vec<_>, Vec<_>) = intents
            .into_iter()
            .partition(|intent| self.names_cell(intent.flake()));
        self.retracts
            .extend(cells.into_iter().map(RetractIntent::into_flake));
        rest
    }

    /// The assertions that name no cell; those that do are held.
    pub(crate) fn divert_assertions(&mut self, flakes: Vec<Flake>) -> Vec<Flake> {
        let (cells, rest): (Vec<_>, Vec<_>) = flakes.into_iter().partition(|f| self.names_cell(f));
        self.asserts.extend(cells);
        rest
    }

    /// The held writes applied, and stored (see the [module docs](self)).
    /// `graph_of` gives a write's ledger graph.
    pub(crate) async fn store(
        self,
        ledger: &LedgerState,
        graph_of: impl Fn(&Flake) -> Result<GraphId>,
        t: i64,
    ) -> Result<StoredEdits> {
        if self.retracts.is_empty() && self.asserts.is_empty() {
            return Ok(StoredEdits::default());
        }
        let mut graph_sids: HashMap<GraphId, Option<Sid>> = HashMap::new();
        let mut triple = |f: &Flake| -> Result<Triple> {
            let g = graph_of(f)?;
            graph_sids.entry(g).or_insert_with(|| f.g.clone());
            Ok(Triple {
                g,
                s: f.s.clone(),
                p: f.p.clone(),
                o: f.o.clone(),
                dt: f.dt.clone(),
                lang: lang_of(f),
            })
        };
        let retracts: HashSet<Triple> = self
            .retracts
            .iter()
            .map(&mut triple)
            .collect::<Result<_>>()?;
        let asserts: Vec<Triple> = self
            .asserts
            .iter()
            .map(&mut triple)
            .collect::<Result<_>>()?;

        // The stored lists a write names a cell of, in the order named.
        let mut owners: Vec<Owner> = Vec::new();
        let mut seen: HashSet<Owner> = HashSet::new();
        for t in retracts.iter().chain(&asserts) {
            for sid in [Some(&t.s), ref_of(&t.o)].into_iter().flatten() {
                if let Some((owner, _)) = self.view_cell(sid) {
                    if seen.insert(owner.clone()) {
                        owners.push(owner);
                    }
                }
            }
        }
        let lists = self.read_lists(ledger, &owners, &mut graph_sids).await?;

        // The stored lists as triples, edited.
        let (first, rest, nil) = (rdf(RDF_FIRST), rdf(RDF_REST), rdf(RDF_NIL));
        let id = id_datatype_sid();
        let node = |g: GraphId, s: Sid, p: &Sid, o: Sid| Triple {
            g,
            s,
            p: p.clone(),
            o: FlakeValue::Ref(o),
            dt: id.clone(),
            lang: None,
        };
        let mut graph: Vec<Triple> = Vec::new();
        let mut of_list: Vec<usize> = Vec::new();
        if let Some(view) = &self.view {
            for (i, (owner, (read, _))) in owners.iter().zip(&lists).enumerate() {
                let start = graph.len();
                let (g, s, p) = owner;
                graph.push(node(*g, s.clone(), p, view.cell(owner, 0)));
                for (k, member) in read.iter().enumerate() {
                    let cell = view.cell(owner, k);
                    graph.push(Triple {
                        g: *g,
                        s: cell.clone(),
                        p: first.clone(),
                        o: member.o.clone(),
                        dt: member.dt.clone(),
                        lang: lang_of(member),
                    });
                    let next = if k + 1 == read.len() {
                        nil.clone()
                    } else {
                        view.cell(owner, k + 1)
                    };
                    graph.push(node(*g, cell, &rest, next));
                }
                of_list.extend(std::iter::repeat_n(i, graph.len() - start));
            }
        }
        // The lists the write changes: it deletes one of their triples, or
        // states a new one about a cell. A changed list's members are judged
        // as its owner's facts; the triples left of each list are restated.
        let mut edited: HashSet<usize> = HashSet::new();
        let mut restated: HashSet<Triple> = HashSet::new();
        for (triple, &list) in graph.iter().zip(&of_list) {
            if retracts.contains(triple) {
                edited.insert(list);
            } else {
                restated.insert(triple.clone());
            }
        }
        for t in asserts.iter().filter(|t| !restated.contains(*t)) {
            if let Some((owner, _)) = self.view_cell(&t.s) {
                edited.extend(owners.iter().position(|o| *o == owner));
            }
        }
        graph.retain(|t| !retracts.contains(t));
        let mut present: HashSet<Triple> = graph.iter().cloned().collect();
        for t in asserts {
            if present.insert(t.clone()) {
                graph.push(t);
            }
        }

        let mut flakes = Vec::with_capacity(graph.len());
        let mut restating: HashSet<FlakeKey> = HashSet::new();
        for triple in graph {
            let g_sid = graph_sid(ledger, &mut graph_sids, triple.g)?;
            let m = FlakeMeta::from_parts(triple.lang.as_deref(), None);
            let restates = restated.contains(&triple);
            let Triple { s, p, o, dt, .. } = triple;
            let flake = match g_sid {
                Some(g) => Flake::new_in_graph(g, s, p, o, dt, t, true, m),
                None => Flake::new(s, p, o, dt, t, true, m),
            };
            if restates {
                restating.insert(key_of(&flake));
            }
            flakes.push(flake);
        }
        let (left, chained) = crate::lists::chain_lists(flakes, |sid| self.is_cell(sid));

        // A WHERE cell no chain holds is stored as a blank node minted for it.
        let stored_as = |sid: &Sid| match self.view_cell(sid) {
            Some(_) => Sid::new(BLANK_NODE, format!("{}-v{}", self.minted, sid.name)),
            None => sid.clone(),
        };
        let value_as = |o: &mut FlakeValue| {
            if let FlakeValue::Ref(sid) = o {
                *sid = stored_as(sid);
            }
        };

        // A stored list chained again from its own edge, with its members,
        // is unchanged: neither retracted nor stored again.
        let mut chained: Vec<Option<Vec<Flake>>> = chained.into_iter().map(Some).collect();
        let mut out = StoredEdits::default();
        for (i, (owner, (read, stored))) in owners.iter().zip(lists).enumerate() {
            let g_sid = graph_sid(ledger, &mut graph_sids, owner.0)?;
            let same = chained.iter().position(|list| {
                list.as_ref().is_some_and(|members| {
                    members.len() == read.len()
                        && members.iter().zip(&read).all(|(new, old)| {
                            new.g == g_sid
                                && new.s == owner.1
                                && new.p == owner.2
                                && new.o == old.o
                                && new.dt == old.dt
                                && lang_of(new) == lang_of(old)
                        })
                })
            });
            match same {
                Some(n) => chained[n] = None,
                None => {
                    for fact in stored {
                        let retraction = fact.retract(t);
                        if !edited.contains(&i) {
                            out.unjudged.push(retraction.flake().clone());
                        }
                        out.retractions.push(retraction);
                    }
                }
            }
        }
        for (i, mut members) in chained.into_iter().flatten().enumerate() {
            for member in &mut members {
                value_as(&mut member.o);
            }
            out.lists
                .push(PendingList::new(members, format!("{}--e{i}-", self.txn_id)));
        }
        for mut flake in left {
            let restates = restating.contains(&key_of(&flake));
            flake.s = stored_as(&flake.s);
            value_as(&mut flake.o);
            if restates {
                out.unjudged.push(flake.clone());
            }
            out.assertions.push(flake);
        }
        Ok(out)
    }

    /// Each list's members as the view read them and as they are stored,
    /// which must be the same: a member the view could not see would keep a
    /// position the list stored again also takes.
    async fn read_lists(
        &self,
        ledger: &LedgerState,
        owners: &[Owner],
        graph_sids: &mut HashMap<GraphId, Option<Sid>>,
    ) -> Result<Vec<(Vec<Flake>, Vec<StoredFact>)>> {
        let Some(view) = &self.view else {
            return Ok(Vec::new());
        };
        let mut slots = Vec::with_capacity(owners.len());
        for (g, s, p) in owners {
            slots.push(Slot {
                g_id: *g,
                g_sid: graph_sid(ledger, graph_sids, *g)?,
                s: s.clone(),
                p: p.clone(),
            });
        }
        let facts = CurrentFacts::new(ledger).of_slots(&slots).await?;
        let mut lists = Vec::with_capacity(owners.len());
        for (owner, facts) in owners.iter().zip(facts) {
            let read = view.members_of(owner).unwrap_or_default();
            let stored: Vec<StoredFact> =
                facts.into_iter().filter(|f| is_member(f.flake())).collect();
            let keys: HashSet<_> = stored.iter().map(|f| member_key(f.flake())).collect();
            if read.len() != stored.len() || !read.iter().all(|f| keys.contains(&member_key(f))) {
                return Err(TransactError::UnsupportedFeature(
                    "an update that edits a list holding members the request cannot read"
                        .to_string(),
                ));
            }
            lists.push((read, stored));
        }
        Ok(lists)
    }
}

fn ref_of(o: &FlakeValue) -> Option<&Sid> {
    match o {
        FlakeValue::Ref(sid) => Some(sid),
        _ => None,
    }
}

fn rdf(name: &str) -> Sid {
    Sid::new(RDF, name)
}

fn lang_of(f: &Flake) -> Option<String> {
    f.m.as_ref().and_then(|m| m.lang.clone())
}

fn is_member(f: &Flake) -> bool {
    f.m.as_ref().is_some_and(|m| m.i.is_some())
}

/// A fact as the triples of a list name it, its graph included.
type FlakeKey = (Option<Sid>, Sid, Sid, FlakeValue, Sid, Option<String>);

fn key_of(f: &Flake) -> FlakeKey {
    (
        f.g.clone(),
        f.s.clone(),
        f.p.clone(),
        f.o.clone(),
        f.dt.clone(),
        lang_of(f),
    )
}

fn member_key(f: &Flake) -> (FlakeValue, Sid, Option<FlakeMeta>) {
    (f.o.clone(), f.dt.clone(), f.m.clone())
}

/// The graph Sid graph `g` is written under.
fn graph_sid(
    ledger: &LedgerState,
    known: &mut HashMap<GraphId, Option<Sid>>,
    g: GraphId,
) -> Result<Option<Sid>> {
    if let Some(sid) = known.get(&g) {
        return Ok(sid.clone());
    }
    let sid = if g == fluree_db_core::DEFAULT_GRAPH_ID {
        None
    } else {
        let sid = ledger
            .snapshot
            .graph_registry
            .iri_for_graph_id(g)
            .and_then(|iri| ledger.snapshot.encode_iri(iri))
            .ok_or_else(|| TransactError::InvariantViolation(format!("graph {g} has no IRI")))?;
        Some(sid)
    };
    known.insert(g, sid.clone());
    Ok(sid)
}
