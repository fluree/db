//! Where `fluree export` gets "which reifiers point at this edge".
//!
//! RDF 1.2 annotation syntax names the reifier at the base edge
//! (`s p o ~ <r>`), so serializing it needs the edge → reifier direction of
//! the `rdf:reifies` links. The scan is in SPOT order, and a reifier whose IRI
//! sorts after its base edge's subject arrives too late to mark the line
//! already written, so the links are read once, up front: `O(annotations)`,
//! not `O(dataset)`.

use fluree_db_core::comparator::IndexType;
use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::{EdgeKey, FlakeValue, GraphId, Sid, TripleTermValue};
use std::collections::{HashMap, HashSet};
use std::io;
use std::marker::PhantomData;
use std::sync::Mutex;

use crate::{LedgerState, Result};

/// Edge → reifier lookup for the duration of one export.
///
/// Only `ExportBuilder` constructs one; the type is public because it appears
/// on the public `ExportConfig`. An external caller building an `ExportConfig`
/// by hand passes `annotations: None` and gets the links as ordinary triples.
pub struct AnnotationProbe<'a> {
    /// Live links at the export's `t` whose triple is asserted, per graph,
    /// by the edge they name (its `g` cleared). Each becomes a marker on
    /// its edge.
    links: HashMap<GraphId, HashMap<EdgeKey, Vec<Sid>>>,
    /// Live links whose triple is not asserted in their graph: no edge
    /// carries their marker, so they are written as rows.
    unasserted: HashSet<(GraphId, Sid, EdgeKey)>,
    /// Reifiers named by a `~ <r>` marker somewhere in this export.
    named: Mutex<HashSet<Sid>>,
    /// Reifiers whose link the scan passed — i.e. whose own subject is inside
    /// the exported selection, so their properties are in the file. Populated
    /// from the rows the writers suppress, which costs nothing extra: they are
    /// already being visited and discarded.
    in_scope: Mutex<HashSet<Sid>>,
    _ledger: PhantomData<&'a LedgerState>,
}

impl<'a> AnnotationProbe<'a> {
    /// Record that a `~ <r>` marker (or its per-format equivalent) was
    /// emitted for `reifier`.
    pub(crate) fn note_reifier_named(&self, reifier: &Sid) {
        if let Ok(mut named) = self.named.lock() {
            if !named.contains(reifier) {
                named.insert(reifier.clone());
            }
        }
    }

    /// Record that the scan passed `s_id`'s link, so that reifier's own
    /// triples are inside this export.
    pub(crate) fn note_link_in_scope(&self, resolver: &dyn ReifierSubject, s_id: u64) {
        let Ok(sid) = resolver.reifier_sid(s_id) else {
            return;
        };
        self.note_link_sid(sid);
    }

    /// As [`Self::note_link_in_scope`], for callers that already hold the
    /// reifier's `Sid`.
    ///
    /// Untranslated overlay rows carry a fully-decoded subject, so they need
    /// no resolver round-trip. They still have to be *counted*: suppressing a
    /// link without noting it turns a visible leak into an annotation that
    /// vanishes with no marker, no link and no number.
    pub(crate) fn note_link_sid(&self, sid: Sid) {
        if let Ok(mut in_scope) = self.in_scope.lock() {
            in_scope.insert(sid);
        }
    }

    /// Reifiers named in the output whose own description is not in it.
    ///
    /// Read once, after every graph of an export has been written: a link
    /// can legitimately be emitted in a later graph than the marker, so the
    /// answer is only meaningful for the file as a whole.
    pub(crate) fn out_of_scope_count(&self) -> u64 {
        let (Ok(named), Ok(in_scope)) = (self.named.lock(), self.in_scope.lock()) else {
            return 0;
        };
        named.difference(&in_scope).count() as u64
    }

    /// Links the export suppressed whose reifier it never named: the link
    /// rows were dropped because annotation syntax was going to replace them,
    /// and then no marker was emitted. Reported so that gap can never be a
    /// silent truncation.
    pub(crate) fn unresolved_count(&self) -> u64 {
        let (Ok(named), Ok(in_scope)) = (self.named.lock(), self.in_scope.lock()) else {
            return 0;
        };
        in_scope.difference(&named).count() as u64
    }

    /// Read `ledger`'s live links as of `as_of_t`, or establish that it has
    /// none. `Ok(None)` when neither the index nor novelty has ever held an
    /// annotation, so a ledger without annotations pays two boolean reads.
    /// Each link's triple is looked up once per `(graph, subject,
    /// predicate)` to tell markers from rows.
    pub(crate) async fn for_ledger(ledger: &'a LedgerState, as_of_t: i64) -> Result<Option<Self>> {
        if !ledger.snapshot.has_annotations && !ledger.novelty.has_annotations() {
            return Ok(None);
        }
        fluree_db_query::term_components::require_link_index(&ledger.snapshot)?;

        let graphs = std::iter::once(0).chain(
            ledger
                .snapshot
                .graph_registry
                .iter_entries()
                .map(|(g_id, _)| g_id),
        );
        let mut links: HashMap<GraphId, HashMap<EdgeKey, Vec<Sid>>> = HashMap::new();
        let mut unasserted: HashSet<(GraphId, Sid, EdgeKey)> = HashSet::new();
        for g_id in graphs {
            let flakes = range_with_overlay(
                &ledger.snapshot,
                g_id,
                ledger.novelty.as_ref(),
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::predicate(fluree_db_core::rdf_reifies_sid().clone()),
                RangeOptions::new().with_to_t(as_of_t),
            )
            .await?;
            // Moved, not cloned: the map holds each term's parts.
            let graph_links = links.entry(g_id).or_default();
            for flake in flakes {
                let FlakeValue::TripleTerm(term) = flake.o else {
                    continue;
                };
                let TripleTermValue { s, p, o, dt, lang } = *term;
                let edge = EdgeKey {
                    g: None,
                    s,
                    p,
                    o,
                    dt,
                    lang,
                    list_i: None,
                };
                let reifiers = graph_links.entry(edge).or_default();
                if !reifiers.contains(&flake.s) {
                    reifiers.push(flake.s);
                }
            }
        }
        for (&g_id, graph_links) in &mut links {
            // One lookup per (subject, predicate), grouped over borrowed keys.
            let mut edges: Vec<&EdgeKey> = graph_links.keys().collect();
            edges.sort_unstable_by(|a, b| (&a.s, &a.p).cmp(&(&b.s, &b.p)));
            let mut missing: Vec<EdgeKey> = Vec::new();
            for group in edges.chunk_by(|a, b| a.s == b.s && a.p == b.p) {
                let asserted: HashSet<EdgeKey> = range_with_overlay(
                    &ledger.snapshot,
                    g_id,
                    ledger.novelty.as_ref(),
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(group[0].s.clone(), group[0].p.clone()),
                    RangeOptions::new().with_to_t(as_of_t),
                )
                .await?
                .iter()
                .map(|flake| EdgeKey {
                    g: None,
                    list_i: None,
                    ..EdgeKey::from_flake(flake)
                })
                .collect();
                missing.extend(
                    group
                        .iter()
                        .filter(|edge| !asserted.contains(**edge))
                        .map(|edge| (*edge).clone()),
                );
            }
            for edge in missing {
                for reifier in graph_links.remove(&edge).unwrap_or_default() {
                    unasserted.insert((g_id, reifier, edge.clone()));
                }
            }
        }
        for reifiers in links.values_mut().flat_map(HashMap::values_mut) {
            reifiers.sort();
        }
        Ok(Some(Self {
            links,
            unasserted,
            named: Mutex::new(HashSet::new()),
            in_scope: Mutex::new(HashSet::new()),
            _ledger: PhantomData,
        }))
    }

    /// Whether any live link names a triple its graph does not assert.
    pub(crate) fn has_unasserted(&self) -> bool {
        !self.unasserted.is_empty()
    }

    /// Whether `reifier`'s link to `term` in graph `g_id` names a triple the
    /// graph does not assert, so the link is written as a row rather than
    /// replaced by a marker.
    pub(crate) fn link_is_unasserted(
        &self,
        g_id: GraphId,
        reifier: &Sid,
        term: &fluree_db_core::TripleTermValue,
    ) -> bool {
        !self.unasserted.is_empty()
            && self
                .unasserted
                .contains(&(g_id, reifier.clone(), term_edge(term)))
    }

    /// Live reifiers for each edge of graph `g_id`, index-aligned with
    /// `edges`; entry `i` is empty when `edges[i]` carries no annotation.
    pub(crate) fn live_reifiers(&self, g_id: GraphId, edges: &[EdgeKey]) -> Vec<Vec<Sid>> {
        let Some(links) = self.links.get(&g_id) else {
            return vec![Vec::new(); edges.len()];
        };
        edges
            .iter()
            .map(|edge| {
                let key = EdgeKey {
                    g: None,
                    ..edge.clone()
                };
                links.get(&key).cloned().unwrap_or_default()
            })
            .collect()
    }
}

/// The edge a triple term names, keyed as the probe keys it.
fn term_edge(term: &fluree_db_core::TripleTermValue) -> EdgeKey {
    EdgeKey {
        g: None,
        s: term.s.clone(),
        p: term.p.clone(),
        o: term.o.clone(),
        dt: term.dt.clone(),
        lang: term.lang.clone(),
        list_i: None,
    }
}

/// Resolves a subject id to the `Sid` a reifier was stored under.
///
/// A trait so this module does not have to know about the export writers'
/// resolver, which owns the store and dictionary-novelty handles.
pub(crate) trait ReifierSubject {
    fn reifier_sid(&self, s_id: u64) -> io::Result<Sid>;
}
