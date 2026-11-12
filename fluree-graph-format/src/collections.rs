//! RDF collections a writer spells inline: Turtle as `( … )`, JSON-LD as
//! `{"@list": [ … ]}`.

use fluree_graph_ir::datatype::iri as dt_iri;
use fluree_graph_ir::{Dataset, Graph, Term, Triple};
// The fold hashes every cell several times, and its output is looked up once
// per triple written. hashbrown's default hasher (foldhash) is several times
// faster than std's SipHash on short terms, and is still seeded at random,
// so terms taken from stored data can't be chosen to collide.
use hashbrown::{HashMap, HashSet};
use std::collections::{HashMap as StdMap, HashSet as StdSet};

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
        reifiers: &StdMap<&'g Triple, Vec<&'g Term>>,
        shared: &StdSet<&'g Term>,
    ) -> Self {
        let foldable =
            |t: &Term| is_blank(t) && t.as_iri().is_none_or(|iri| !iri.starts_with("_:fdb-"));
        // A reified triple, or one a reifier names, keeps its cells as written.
        let mut named: HashSet<&Term> = HashSet::new();
        for t in reifiers.keys() {
            named.insert(t.subject());
            named.insert(t.object());
        }
        let kept = |t: &Term| named.contains(t) || shared.contains(t);
        let mut cells: HashMap<&Term, (Option<&Term>, Option<&Term>, usize)> = HashMap::new();
        for t in graph.triples() {
            if !foldable(t.subject()) || kept(t.subject()) {
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
        // The subject naming each cell, when one does.
        let mut named_by: HashMap<&Term, &Term> = HashMap::new();
        for t in graph.triples() {
            if cells.contains_key(t.object()) {
                *inbound.entry(t.object()).or_default() += 1;
                named_by.insert(t.object(), t.subject());
                if cells.contains_key(t.subject())
                    && t.predicate().as_iri() == Some(dt_iri::RDF_REST)
                {
                    follows.insert(t.object());
                }
            }
        }
        let mut chains: HashMap<&Term, (Vec<&Term>, Vec<&Term>)> = HashMap::new();
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
                // Each cell has one `rdf:rest`, so a walk that meets a cell
                // twice never ends, and once it holds every cell the next is
                // a repeat. (The inbound check stops it sooner: a cell met
                // again is named by two cells, or is the head, which no
                // `rdf:rest` names.)
                if (at != head && inbound.get(at) != Some(&1)) || chain.len() == cells.len() {
                    break false;
                }
                chain.push(at);
                items.push(*first);
                at = rest;
            };
            if complete {
                chains.insert(head, (chain, items));
            }
        }
        // A collection is written inline where the triple naming its head
        // is written: by a node no collection folds, or from inside one
        // written so itself. One named only from its own cells, or from a
        // cycle of collections, has nowhere to go, and keeps its triples.
        let in_chain: HashMap<&Term, &Term> = chains
            .iter()
            .flat_map(|(&head, (chain, _))| chain.iter().map(move |&cell| (cell, head)))
            .collect();
        // The collections named from inside each collection; those named
        // from outside every collection are written, and so, in turn, is
        // each collection named from inside one that is.
        let mut inner: HashMap<&Term, Vec<&Term>> = HashMap::new();
        let mut next: Vec<&Term> = Vec::new();
        for &head in chains.keys() {
            match in_chain.get(named_by[head]) {
                Some(&outer) => inner.entry(outer).or_default().push(head),
                None => next.push(head),
            }
        }
        let mut written: HashSet<&Term> = HashSet::new();
        while let Some(head) = next.pop() {
            if written.insert(head) {
                next.extend(inner.get(head).into_iter().flatten().copied());
            }
        }
        let mut out = Collections {
            heads: HashMap::new(),
            cells: HashSet::new(),
        };
        for (head, (chain, items)) in chains {
            if written.contains(head) {
                out.cells.extend(chain);
                out.heads.insert(head, items);
            }
        }
        out
    }
}

/// A document scopes a blank-node label to all its graphs, so a node more
/// than one graph names must keep its label.
pub(crate) fn blank_nodes_in_several_graphs(dataset: &Dataset) -> StdSet<&Term> {
    let mut shared = StdSet::new();
    if dataset.named.is_empty() {
        return shared;
    }
    let mut seen: StdMap<&Term, usize> = StdMap::new();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    const EX: &str = "http://example.org/";

    fn ex(local: &str) -> Term {
        Term::iri(format!("{EX}{local}"))
    }

    fn first() -> Term {
        Term::iri(dt_iri::RDF_FIRST)
    }

    fn rest() -> Term {
        Term::iri(dt_iri::RDF_REST)
    }

    fn nil() -> Term {
        Term::iri(dt_iri::RDF_NIL)
    }

    /// The collections of `graph`: the number of items in each, longest
    /// first, and the number of cells folded.
    fn fold(graph: &Graph) -> (Vec<usize>, usize) {
        let collections = Collections::of(graph, &StdMap::new(), &StdSet::new());
        let mut lens: Vec<usize> = collections.heads.values().map(Vec::len).collect();
        lens.sort_unstable_by(|a, b| b.cmp(a));
        (lens, collections.cells.len())
    }

    /// `f` on a thread of its own, refused once it runs past `limit`: a fold
    /// that is quadratic in its input takes minutes on these graphs, a
    /// linear one well under a second.
    fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let started = Instant::now();
            let out = f();
            eprintln!("folded in {:?}", started.elapsed());
            let _ = tx.send(out);
        });
        rx.recv_timeout(limit)
            .unwrap_or_else(|_| panic!("the fold ran past {limit:?}"))
    }

    const LIMIT: Duration = Duration::from_secs(60);

    /// One list of 200k members, its cells labeled as the Graph Store `GET`
    /// and CONSTRUCT label them, folds in time linear in its length.
    #[test]
    fn a_long_collection_folds_in_linear_time() {
        const N: usize = 200_000;
        let folded = within(LIMIT, || {
            let cell = |k: usize| Term::blank(format!("l0-{k}"));
            let mut g = Graph::new();
            g.add_triple(ex("s"), ex("items"), cell(0));
            for k in 0..N {
                let next = if k + 1 == N { nil() } else { cell(k + 1) };
                g.add_triple(cell(k), first(), Term::integer(k as i64));
                g.add_triple(cell(k), rest(), next);
            }
            fold(&g)
        });
        assert_eq!(folded, (vec![N], N));
    }

    /// 20k collections, each the one member of the one before, fold in time
    /// linear in their number, not in their number times their depth.
    #[test]
    fn deeply_nested_collections_fold_in_linear_time() {
        const DEPTH: usize = 20_000;
        let folded = within(LIMIT, || {
            let head = |i: usize| Term::blank(format!("n{i}"));
            let mut g = Graph::new();
            g.add_triple(ex("s"), ex("items"), head(0));
            for i in 0..DEPTH {
                let member = if i + 1 == DEPTH {
                    Term::integer(1)
                } else {
                    head(i + 1)
                };
                g.add_triple(head(i), first(), member);
                g.add_triple(head(i), rest(), nil());
            }
            fold(&g)
        });
        assert_eq!(folded, (vec![1; DEPTH], DEPTH));
    }
}
