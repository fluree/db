//! Lists a SPARQL write states as RDF does: an owner's edge to a head cell
//! and a chain of `rdf:first`/`rdf:rest` cells, the blank nodes the
//! transaction mints for a `( … )` or a chain written out.
//!
//! The assertions naming such a cell are held here. Every chain then hanging
//! from an owner's edge, its cells plain and named by nothing else, is stored
//! compactly ([`crate::lists`]); every other triple as it is.

use crate::error::{Result, TransactError};
use crate::lists::PendingList;
use crate::namespace::BLANK_NODE_ID_PREFIX;
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, GraphId, Sid};
use fluree_db_ledger::LedgerState;
use fluree_vocab::namespaces::BLANK_NODE;
use std::collections::{HashMap, HashSet};

/// The assertions of one SPARQL transaction that name a cell it mints.
pub(crate) struct ListEdits {
    txn_id: String,
    /// The prefix of the blank nodes the transaction mints.
    minted: String,
    asserts: Vec<Flake>,
}

/// What storing the stated lists takes.
#[derive(Default)]
pub(crate) struct StoredEdits {
    /// The triples stored as they are.
    pub assertions: Vec<Flake>,
    /// The lists stored compactly, unless their owner holds another.
    pub lists: Vec<PendingList>,
}

/// A triple of the stated lists, in the graph it is in.
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
    /// The edits of transaction `txn_id`.
    pub(crate) fn new(txn_id: &str) -> Self {
        Self {
            txn_id: txn_id.to_string(),
            minted: format!("{BLANK_NODE_ID_PREFIX}-{txn_id}-"),
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

    fn is_cell(&self, sid: &Sid) -> bool {
        sid.namespace_code == BLANK_NODE && sid.name.starts_with(&self.minted)
    }

    /// The assertions that name no cell; those that do are held.
    pub(crate) fn divert_assertions(&mut self, flakes: Vec<Flake>) -> Vec<Flake> {
        let (cells, rest): (Vec<_>, Vec<_>) = flakes
            .into_iter()
            .partition(|f| self.is_cell(&f.s) || ref_of(&f.o).is_some_and(|o| self.is_cell(o)));
        self.asserts.extend(cells);
        rest
    }

    /// The held assertions, stored (see the [module docs](self)). `graph_of`
    /// gives an assertion's ledger graph.
    pub(crate) fn store(
        self,
        ledger: &LedgerState,
        graph_of: impl Fn(&Flake) -> Result<GraphId>,
        t: i64,
    ) -> Result<StoredEdits> {
        if self.asserts.is_empty() {
            return Ok(StoredEdits::default());
        }
        let mut graph_sids: HashMap<GraphId, Option<Sid>> = HashMap::new();
        let mut present: HashSet<Triple> = HashSet::new();
        let mut flakes = Vec::with_capacity(self.asserts.len());
        for f in &self.asserts {
            let g = graph_of(f)?;
            graph_sids.entry(g).or_insert_with(|| f.g.clone());
            let triple = Triple {
                g,
                s: f.s.clone(),
                p: f.p.clone(),
                o: f.o.clone(),
                dt: f.dt.clone(),
                lang: lang_of(f),
            };
            if !present.insert(triple.clone()) {
                continue;
            }
            let g_sid = graph_sid(ledger, &mut graph_sids, triple.g)?;
            let m = FlakeMeta::from_parts(triple.lang.as_deref(), None);
            let Triple { s, p, o, dt, .. } = triple;
            flakes.push(match g_sid {
                Some(g) => Flake::new_in_graph(g, s, p, o, dt, t, true, m),
                None => Flake::new(s, p, o, dt, t, true, m),
            });
        }
        let (left, chained) = crate::lists::chain_lists(flakes, |sid| self.is_cell(sid));
        Ok(StoredEdits {
            assertions: left,
            lists: chained
                .into_iter()
                .enumerate()
                .map(|(i, members)| PendingList::new(members, format!("{}--e{i}-", self.txn_id)))
                .collect(),
        })
    }
}

fn ref_of(o: &FlakeValue) -> Option<&Sid> {
    match o {
        FlakeValue::Ref(sid) => Some(sid),
        _ => None,
    }
}

fn lang_of(f: &Flake) -> Option<String> {
    f.m.as_ref().and_then(|m| m.lang.clone())
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
