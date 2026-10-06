//! Where `fluree export` gets "which reifiers point at this edge".
//!
//! RDF 1.2 annotation syntax names the reifier at the base edge
//! (`s p o ~ <r>`), so serializing it needs the edge → reifier direction of
//! the `rdf:reifies` links. The scan is in SPOT order, and a reifier whose IRI
//! sorts after its base edge's subject arrives too late to mark the line
//! already written, so each batch's edges look their links up as the batch
//! is written, and each link row checks whether its triple is asserted. Only
//! the reifier bookkeeping lasts the whole export, at a word per annotation.

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::{EdgeKey, FlakeValue, GraphId, Sid, TripleTermValue};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::{Arc, Mutex};

use crate::{LedgerState, Result};

/// Edge → reifier lookups for the duration of one export.
///
/// Only `ExportBuilder` constructs one; the type is public because it appears
/// on the public `ExportConfig`. An external caller building an `ExportConfig`
/// by hand passes `annotations: None` and gets the links as ordinary triples.
pub struct AnnotationProbe<'a> {
    ledger: &'a LedgerState,
    as_of_t: i64,
    /// Every live link, read up front when the index counts few enough;
    /// otherwise each batch probes its own edges and links.
    preloaded: Option<Preloaded>,
    /// Reifiers named by a `~ <r>` marker somewhere in this export, by
    /// [`reifier_key`].
    named: Mutex<HashSet<u64>>,
    /// Reifiers whose link the scan passed — i.e. whose own subject is inside
    /// the exported selection, so their properties are in the file. Populated
    /// from the rows the writers suppress, which costs nothing extra: they are
    /// already being visited and discarded.
    in_scope: Mutex<HashSet<u64>>,
}

/// Links up to this many are read up front: lookups then cost a hash probe,
/// at a few hundred bytes per link. Past it, per-batch probes keep memory
/// to the batch.
pub(crate) const PRELOAD_MAX_LINKS: u64 = 1_000_000;

/// The live links at the export's `t`, read once.
struct Preloaded {
    /// Links whose triple is asserted, per graph, by the edge they name (its
    /// `g` cleared). Each becomes a marker on its edge.
    links: HashMap<GraphId, HashMap<EdgeKey, Vec<Sid>>>,
    /// Edges, per graph, that live links name but the graph does not assert:
    /// no edge carries their marker, so the links are written as rows.
    unasserted: HashSet<(GraphId, EdgeKey)>,
}

/// A reifier as the bookkeeping sets hold it: a hash, so they cost a word per
/// annotation rather than an IRI.
fn reifier_key(sid: &Sid) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sid.hash(&mut hasher);
    hasher.finish()
}

impl<'a> AnnotationProbe<'a> {
    /// Record that a `~ <r>` marker (or its per-format equivalent) was
    /// emitted for `reifier`.
    pub(crate) fn note_reifier_named(&self, reifier: &Sid) {
        if let Ok(mut named) = self.named.lock() {
            named.insert(reifier_key(reifier));
        }
    }

    /// Record that the scan passed `s_id`'s link, so that reifier's own
    /// triples are inside this export.
    pub(crate) fn note_link_in_scope(&self, resolver: &dyn ReifierSubject, s_id: u64) {
        let Ok(sid) = resolver.reifier_sid(s_id) else {
            return;
        };
        self.note_link_sid(&sid);
    }

    /// As [`Self::note_link_in_scope`], for callers that already hold the
    /// reifier's `Sid`.
    ///
    /// Untranslated overlay rows carry a fully-decoded subject, so they need
    /// no resolver round-trip. They still have to be *counted*: suppressing a
    /// link without noting it turns a visible leak into an annotation that
    /// vanishes with no marker, no link and no number.
    pub(crate) fn note_link_sid(&self, sid: &Sid) {
        if let Ok(mut in_scope) = self.in_scope.lock() {
            in_scope.insert(reifier_key(sid));
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

    /// A probe of `ledger`'s links as of `as_of_t`, or `Ok(None)` when
    /// neither the index nor novelty has ever held an annotation, so a ledger
    /// without annotations pays two boolean reads. Nothing is read up front:
    /// the writers probe each batch's edges and links.
    pub(crate) async fn for_ledger(
        ledger: &'a LedgerState,
        as_of_t: i64,
        preload_max_links: u64,
    ) -> Result<Option<Self>> {
        if !ledger.snapshot.has_annotations && !ledger.novelty.has_annotations() {
            return Ok(None);
        }
        fluree_db_query::term_components::require_link_index(&ledger.snapshot)?;
        let mut probe = Self {
            ledger,
            as_of_t,
            preloaded: None,
            named: Mutex::new(HashSet::new()),
            in_scope: Mutex::new(HashSet::new()),
        };
        // Unknown counts on an index mean an index of unknown size; without
        // one, the links are novelty's.
        let indexed_links = match ledger.snapshot.stats.as_ref() {
            Some(stats) => stats
                .links
                .as_ref()
                .map(|links| links.iter().map(|l| l.count).sum::<u64>()),
            None => Some(0),
        };
        if indexed_links.is_some_and(|n| n <= preload_max_links) {
            probe.preloaded = Some(
                probe
                    .preload()
                    .await
                    .map_err(|e| crate::ApiError::internal(e.to_string()))?,
            );
        }
        Ok(Some(probe))
    }

    /// Read every live link, telling markers from rows by one lookup per
    /// `(graph, subject, predicate)`.
    async fn preload(&self) -> io::Result<Preloaded> {
        let graphs = std::iter::once(0).chain(
            self.ledger
                .snapshot
                .graph_registry
                .iter_entries()
                .map(|(g_id, _)| g_id),
        );
        let mut links: HashMap<GraphId, HashMap<EdgeKey, Vec<Sid>>> = HashMap::new();
        let mut unasserted: HashSet<(GraphId, EdgeKey)> = HashSet::new();
        for g_id in graphs {
            let flakes = self
                .range(
                    g_id,
                    IndexType::Psot,
                    RangeMatch::predicate(fluree_db_core::rdf_reifies_sid().clone()),
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
                let asserted: HashSet<EdgeKey> = self
                    .range(
                        g_id,
                        IndexType::Spot,
                        RangeMatch::subject_predicate(group[0].s.clone(), group[0].p.clone()),
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
                graph_links.remove(&edge);
                unasserted.insert((g_id, edge));
            }
        }
        for reifiers in links.values_mut().flat_map(HashMap::values_mut) {
            reifiers.sort();
        }
        Ok(Preloaded { links, unasserted })
    }

    async fn range(
        &self,
        g_id: GraphId,
        index: IndexType,
        rm: RangeMatch,
    ) -> io::Result<Vec<fluree_db_core::Flake>> {
        range_with_overlay(
            &self.ledger.snapshot,
            g_id,
            self.ledger.novelty.as_ref(),
            index,
            RangeTest::Eq,
            rm,
            RangeOptions::new().with_to_t(self.as_of_t),
        )
        .await
        .map_err(|e| io::Error::other(e.to_string()))
    }

    /// Whether graph `g_id` does not assert the triple `term` names, so a
    /// link to it is written as a row rather than replaced by a marker.
    pub(crate) async fn link_is_unasserted(
        &self,
        g_id: GraphId,
        term: &TripleTermValue,
    ) -> io::Result<bool> {
        let edge = term_edge(term);
        if let Some(preloaded) = &self.preloaded {
            return Ok(preloaded.unasserted.contains(&(g_id, edge)));
        }
        let rows = self
            .range(
                g_id,
                IndexType::Spot,
                RangeMatch::subject_predicate(term.s.clone(), term.p.clone()),
            )
            .await?;
        Ok(!rows.iter().any(|flake| {
            EdgeKey {
                g: None,
                list_i: None,
                ..EdgeKey::from_flake(flake)
            } == edge
        }))
    }

    /// Live reifiers for each edge of graph `g_id`, index-aligned with
    /// `edges` and sorted; entry `i` is empty when `edges[i]` carries no
    /// annotation. An edge no term dictionary names has no link, so only an
    /// annotated edge pays a link lookup.
    pub(crate) async fn live_reifiers(
        &self,
        g_id: GraphId,
        edges: &[EdgeKey],
        store: &BinaryIndexStore,
        dict_novelty: Option<&Arc<DictNovelty>>,
    ) -> io::Result<Vec<Vec<Sid>>> {
        if let Some(preloaded) = &self.preloaded {
            let links = preloaded.links.get(&g_id);
            return Ok(edges
                .iter()
                .map(|edge| {
                    let key = EdgeKey {
                        g: None,
                        ..edge.clone()
                    };
                    links
                        .and_then(|links| links.get(&key))
                        .cloned()
                        .unwrap_or_default()
                })
                .collect());
        }
        let mut out = Vec::with_capacity(edges.len());
        for edge in edges {
            let term = TripleTermValue {
                s: edge.s.clone(),
                p: edge.p.clone(),
                o: edge.o.clone(),
                dt: edge.dt.clone(),
                lang: edge.lang.clone(),
            };
            if !may_be_linked(&term, store, dict_novelty)? {
                out.push(Vec::new());
                continue;
            }
            let links = self
                .range(
                    g_id,
                    IndexType::Post,
                    RangeMatch::predicate_object(
                        fluree_db_core::rdf_reifies_sid().clone(),
                        FlakeValue::TripleTerm(Box::new(term)),
                    ),
                )
                .await?;
            let mut reifiers: Vec<Sid> = links.into_iter().map(|flake| flake.s).collect();
            reifiers.sort();
            reifiers.dedup();
            out.push(reifiers);
        }
        Ok(out)
    }
}

/// Whether any link can name `term`: a dictionary holds it. A term neither
/// the index nor dictionary novelty names has never been written, so its edge
/// has no link; without initialized dictionary novelty, assume it may.
fn may_be_linked(
    term: &TripleTermValue,
    store: &BinaryIndexStore,
    dict_novelty: Option<&Arc<DictNovelty>>,
) -> io::Result<bool> {
    match fluree_db_query::binary_scan::compose_term_handle(term, store, dict_novelty) {
        Ok(_) => Ok(true),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::Unsupported
            ) =>
        {
            Ok(dict_novelty.is_none_or(|dn| !dn.is_initialized() || dn.terms.find(term).is_some()))
        }
        Err(e) => Err(e),
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

#[cfg(test)]
mod tests {
    use crate::export::ExportFormat;
    use crate::{FlureeBuilder, ReindexOptions};

    /// Per-batch probes write exactly what the up-front read writes, in every
    /// format, over indexed links and novelty ones; a term only novelty holds
    /// decodes through dictionary novelty.
    #[tokio::test]
    async fn per_batch_probes_match_the_preloaded_links() {
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "export/probe-modes:main";
        let ledger = fluree.create_ledger(id).await.unwrap();
        let ledger = fluree
            .upsert_turtle(
                ledger,
                "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
                 ex:alice ex:worksFor ex:acme {| ex:role \"Engineer\" |} .\n\
                 ex:bob ex:knows ex:carol ~ ex:claim1 {| ex:confidence 0.9 |} .\n\
                 ex:bob ex:knows ex:carol ~ ex:claim2 {| ex:confidence 0.5 |} .\n\
                 ex:bob ex:says \"chat\"@fr ~ ex:claim3 {| ex:src ex:hr |} .\n\
                 << ex:dave ex:knows ex:erin ~ ex:claim4 >> ex:confidence 0.1 .\n",
            )
            .await
            .unwrap()
            .ledger;
        drop(ledger);
        fluree.reindex(id, ReindexOptions::default()).await.unwrap();
        let ledger = fluree.ledger(id).await.unwrap();
        fluree
            .upsert_turtle(
                ledger,
                "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
                 ex:frank ex:knows ex:alice ~ ex:claim5 {| ex:confidence 0.7 |} .\n\
                 << ex:gina ex:knows ex:hal ~ ex:claim6 >> ex:confidence 0.2 .\n\
                 ex:doc ex:mentions <<( ex:ivy ex:knows ex:jo )>> .\n",
            )
            .await
            .unwrap();

        for format in [
            ExportFormat::Turtle,
            ExportFormat::JsonLd,
            ExportFormat::NTriples,
        ] {
            let mut outputs = Vec::new();
            for limit in [u64::MAX, 0] {
                let mut out = Vec::new();
                fluree
                    .export(id)
                    .format(format)
                    .preload_max_links(limit)
                    .write_to(&mut out)
                    .await
                    .unwrap();
                outputs.push(String::from_utf8(out).unwrap());
            }
            assert_eq!(outputs[0], outputs[1], "{format:?}");
            assert!(
                outputs[0].contains("ivy"),
                "{format:?} writes a novelty term value"
            );
            if matches!(format, ExportFormat::Turtle) {
                for marker in ["claim1", "claim2", "claim3", "claim5"] {
                    assert!(
                        outputs[0].contains(&format!("~ <http://example.org/{marker}>")),
                        "{format:?} marks {marker}:\n{}",
                        outputs[0]
                    );
                }
            }
        }
    }
}
