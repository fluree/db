//! RDF collections a writer spells inline: Turtle as `( … )`, JSON-LD as
//! `{"@list": [ … ]}`.

use fluree_graph_ir::datatype::iri as dt_iri;
use fluree_graph_ir::{Dataset, Graph, Term, Triple};
use std::collections::{HashMap, HashSet};

/// Chains of blank nodes, each with exactly one `rdf:first` and one
/// `rdf:rest` and nothing else, named once, and ending at `rdf:nil`. A stored
/// blank node with a stable label (`_:fdb-…`) keeps its label, so it is never
/// folded.
pub(crate) struct Collections<'g> {
    /// Each collection's head and its items.
    pub heads: HashMap<&'g Term, Vec<&'g Term>>,
    /// The cells folded into some collection.
    pub cells: HashSet<&'g Term>,
}

impl<'g> Collections<'g> {
    /// `shared` holds the nodes another graph of the same document names.
    pub fn of(
        graph: &'g Graph,
        reifiers: &HashMap<&'g Triple, Vec<&'g Term>>,
        shared: &HashSet<&'g Term>,
    ) -> Self {
        let foldable =
            |t: &Term| is_blank(t) && t.as_iri().is_none_or(|iri| !iri.starts_with("_:fdb-"));
        // A reified triple, or one a reifier names, keeps its cells as written.
        let mut named: HashSet<&Term> = shared.clone();
        for t in reifiers.keys() {
            named.insert(t.subject());
            named.insert(t.object());
        }
        let mut cells: HashMap<&Term, (Option<&Term>, Option<&Term>, usize)> = HashMap::new();
        for t in graph.triples() {
            if !foldable(t.subject()) || named.contains(t.subject()) {
                continue;
            }
            let cell = cells.entry(t.subject()).or_default();
            cell.2 += 1;
            match t.predicate().as_iri() {
                Some(dt_iri::RDF_FIRST) => cell.0 = Some(t.object()),
                Some(dt_iri::RDF_REST) => cell.1 = Some(t.object()),
                _ => {}
            }
        }
        cells.retain(|_, (first, rest, n)| first.is_some() && rest.is_some() && *n == 2);
        let mut inbound: HashMap<&Term, usize> = HashMap::new();
        let mut follows: HashSet<&Term> = HashSet::new();
        for t in graph.triples() {
            if cells.contains_key(t.object()) {
                *inbound.entry(t.object()).or_default() += 1;
                if cells.contains_key(t.subject())
                    && t.predicate().as_iri() == Some(dt_iri::RDF_REST)
                {
                    follows.insert(t.object());
                }
            }
        }
        let mut out = Collections {
            heads: HashMap::new(),
            cells: HashSet::new(),
        };
        for &head in cells.keys() {
            if inbound.get(head) != Some(&1) || follows.contains(head) {
                continue;
            }
            let mut items = Vec::new();
            let mut chain = Vec::new();
            let mut at = head;
            let complete = loop {
                if at.as_iri() == Some(dt_iri::RDF_NIL) {
                    break true;
                }
                let Some((Some(first), Some(rest), _)) = cells.get(at) else {
                    break false;
                };
                if (at != head && inbound.get(at) != Some(&1)) || chain.contains(&at) {
                    break false;
                }
                chain.push(at);
                items.push(*first);
                at = rest;
            };
            if complete {
                out.cells.extend(chain);
                out.heads.insert(head, items);
            }
        }
        out
    }
}

/// A document scopes a blank-node label to all its graphs, so a node more
/// than one graph names must keep its label.
pub(crate) fn blank_nodes_in_several_graphs(dataset: &Dataset) -> HashSet<&Term> {
    let mut shared = HashSet::new();
    if dataset.named.is_empty() {
        return shared;
    }
    let mut seen: HashMap<&Term, usize> = HashMap::new();
    let graphs = std::iter::once(&dataset.default).chain(dataset.named.values());
    for (i, graph) in graphs.enumerate() {
        for t in graph.triples() {
            for term in [t.subject(), t.object()] {
                if is_blank(term) && *seen.entry(term).or_insert(i) != i {
                    shared.insert(term);
                }
            }
        }
    }
    shared
}

fn is_blank(term: &Term) -> bool {
    match term {
        Term::BlankNode(_) => true,
        Term::Iri(iri) => iri.starts_with("_:"),
        Term::Literal { .. } => false,
    }
}
