//! Streaming RDF export from the binary SPOT index.
//!
//! Writes N-Triples, Turtle, N-Quads, or TriG directly to a `Write` sink,
//! one leaflet-batch at a time.  Memory usage is O(leaflet_size), not O(dataset).

use crate::export_annotations::AnnotationProbe;
use fluree_db_binary_index::format::branch::BranchManifest;
use fluree_db_binary_index::read::types::sort_overlay_ops;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryIndexStore, ColumnBatch, ColumnProjection, RunSortOrder,
};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::edge::EdgeKey;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{DecodeKind, Flake, GraphId, OType, OverlayProvider, Sid};
use fluree_db_query::binary_scan::{
    translate_overlay_flakes_with_untranslated, EphemeralPredicateMap,
};
use fluree_graph_ir::{canonical_xsd_double, syntax};
use fluree_vocab::{namespaces, xsd};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Configuration for a single-graph streaming export.
pub struct ExportConfig<'a> {
    /// Target graph ID (0 = default graph).
    pub g_id: GraphId,
    /// If `Some`, emit as N-Quads with this graph IRI as the 4th term.
    pub graph_iri: Option<String>,
    /// Time bound for the export. Rows with `t > to_t` are excluded.
    pub to_t: i64,
    /// Novelty overlay provider (committed-but-not-yet-indexed transactions).
    pub overlay: Option<&'a dyn OverlayProvider>,
    /// Dictionary novelty for resolving IDs from committed-but-not-yet-indexed transactions.
    pub dict_novelty: Option<&'a Arc<DictNovelty>>,
    /// Forward annotation lookup. `None` means this export emits the raw
    /// `f:reifies*` system facts as ordinary triples (`--raw-reifies`, or a
    /// ledger that has never carried an annotation) and the writers run
    /// exactly the loop they ran before RDF 1.2 output existed.
    pub annotations: Option<&'a AnnotationProbe<'a>>,
    /// SID of the graph being scanned, as `EdgeKey.g` recorded it. `None` for
    /// the default graph. Only read when `annotations` is `Some`.
    pub graph_sid: Option<Sid>,
}

/// Counters returned after export completes.
///
/// The CLI and the HTTP route both surface these. Before they did, an export
/// that quietly dropped every named graph in the ledger was indistinguishable
/// from one that had nothing to drop — the complaint in #1847 was precisely
/// that "nothing in the output suggested anything was missing".
#[derive(Debug, Default)]
pub struct ExportStats {
    /// Base triples written. Annotation markers are not counted: RDF 1.2
    /// spells the same reifier as a suffix in Turtle and as its own
    /// `rdf:reifies` statement in N-Triples, so a count that moved with the
    /// format would say nothing about the data.
    pub triples_written: u64,
    /// Rows the writers could not represent: an unresolvable predicate id, or
    /// a value that decoded to `FlakeValue::Null`.
    pub rows_skipped: u64,
    /// Graphs that contributed at least one triple, counting the default
    /// graph. Accumulated by the builder, not the per-graph writers.
    pub graphs_written: u64,
    /// User-visible named graphs in the ledger's registry that this export did
    /// not cover, because no graph selector asked for them. System graphs are
    /// not counted: they are never user data.
    pub named_graphs_omitted: u64,
    /// Reifiers named by an annotation marker in the output whose own
    /// description is not in the output.
    ///
    /// `EdgeKey` carries a graph, and a bundle may live in a different graph
    /// from the edge it reifies, so a `--graph <IRI>` export can legitimately
    /// emit `~ <r>` while `<r>`'s own triples fall outside the selection. That
    /// is reported rather than silently dropped — and rather than suppressed,
    /// which would lose the fact that the edge is annotated at all.
    pub annotations_out_of_scope: u64,
    /// Annotation bundles the export dropped without emitting a marker for
    /// them — the count that means the output is not a faithful
    /// serialization. See `AnnotationProbe::unresolved_count` for the one case
    /// that reaches it today.
    pub annotations_unresolved: u64,
}

/// Output format for streaming export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// N-Triples — one triple per line, full IRIs.
    NTriples,
    /// Turtle — prefixed names, subject grouping with `;`.
    Turtle,
    /// N-Quads — N-Triples with optional 4th graph term.
    NQuads,
    /// TriG — Turtle with `GRAPH <iri> { }` blocks.
    TriG,
    /// JSON-LD — streaming from binary index with `@context` + `@graph`.
    JsonLd,
}

/// System graph IDs excluded from dataset exports.
pub const SYSTEM_GRAPH_TXN_META: GraphId = 1;
pub const SYSTEM_GRAPH_CONFIG: GraphId = 2;

/// Returns `true` if `g_id` is a system-internal graph.
///
/// `#txn-meta` and `#config` carry a ledger's own commit metadata and
/// configuration under IRIs derived from its name. Exporting them as ordinary
/// named graphs produces a file that either collides with the target ledger's
/// reserved graph ids on re-import (#1846) or lands a foreign ledger's commit
/// history in a user graph, so `--all-graphs` filters them out unless
/// `system_graphs()` is set.
pub fn is_system_graph(g_id: GraphId) -> bool {
    g_id == SYSTEM_GRAPH_TXN_META || g_id == SYSTEM_GRAPH_CONFIG
}

/// The SPOT branch to scan for `g_id`, or an empty one.
///
/// A graph with no branch is not a graph with no rows: an un-indexed ledger has
/// no branch for *any* graph while holding its whole contents in the novelty
/// overlay, and an indexed ledger has no branch for a graph first written after
/// the last index build. Returning an empty [`BranchManifest`] rather than
/// bailing lets [`BinaryCursor`] exhaust its (zero-length) leaf range and fall
/// through to its overlay-only tail, which emits exactly those rows. Bailing
/// instead is what made `fluree export` fail on a never-indexed ledger.
fn spot_branch(store: &Arc<BinaryIndexStore>, g_id: GraphId) -> Arc<BranchManifest> {
    match store.branch_for_order(g_id, RunSortOrder::Spot) {
        Some(b) => Arc::clone(b),
        None => Arc::new(BranchManifest { leaves: Vec::new() }),
    }
}

/// Configure a `BinaryCursor` with time-travel bounds and novelty overlay.
///
/// Returns the ephemeral predicate map for novelty-only predicates and any
/// overlay flakes that could not be encoded into V3 overlay ops (e.g.
/// language-tagged literals whose BCP-47 tag is not yet in the persisted
/// language dictionary, or vector/bigint/decimal values). These "untranslated"
/// flakes carry their fully-decoded `(s, p, o, dt, m)` and are emitted directly
/// by the per-format writers so committed-but-not-yet-indexed novelty is never
/// silently dropped from the export.
fn apply_time_travel(
    cursor: &mut BinaryCursor,
    config: &ExportConfig,
    store: &Arc<BinaryIndexStore>,
) -> (EphemeralPredicateMap, Vec<Flake>) {
    cursor.set_to_t(config.to_t);
    if let Some(overlay) = config.overlay {
        let (mut ops, untranslated, ephemeral_preds) = translate_overlay_flakes_with_untranslated(
            overlay,
            store,
            config.dict_novelty,
            None, // no runtime_small_dicts during export
            config.to_t,
            config.g_id,
        );
        if !ops.is_empty() {
            sort_overlay_ops(&mut ops, RunSortOrder::Spot);
            cursor.set_overlay_ops(ops.into());
        }
        (ephemeral_preds, surviving_untranslated(untranslated))
    } else {
        (HashMap::new(), Vec::new())
    }
}

/// Resolve assert/retract set-semantics among the untranslated overlay flakes.
///
/// Untranslated flakes bypass the cursor's overlay merge, so we apply the same
/// rule here: for each fact identity `(s, p, o, dt, m)` keep the highest-`t`
/// flake and emit it only if it is an assertion. `Flake`'s `Eq`/`Hash` key on
/// fact identity (ignoring `t`/`op`), so the map collapses each identity.
fn surviving_untranslated(flakes: Vec<Flake>) -> Vec<Flake> {
    let mut latest: HashMap<Flake, Flake> = HashMap::with_capacity(flakes.len());
    for f in flakes {
        match latest.get(&f) {
            Some(existing) if existing.t >= f.t => {}
            _ => {
                latest.insert(f.clone(), f);
            }
        }
    }
    let mut out: Vec<Flake> = latest.into_values().filter(|f| f.op).collect();
    // Deterministic order. `HashMap::into_values` yields in the
    // randomly-seeded hasher's order, so two exports of the same ledger
    // produced different bytes run to run whenever untranslated rows existed
    // — and #1574 makes untranslated rows the normal case rather than a
    // corner. Intra-block predicate order carries no meaning in Turtle, but
    // diffing two exports, checksumming one, or content-addressing a backup
    // all require the bytes to be stable.
    //
    // The key is the whole of `Flake`'s fact identity — `s, p, o, dt, m` per
    // its hand-written `Eq` — so it is total over the map's own key and no
    // two surviving rows can tie into an unspecified order.
    fn meta_key(f: &Flake) -> (Option<&str>, Option<i32>) {
        (
            f.m.as_ref().and_then(|m| m.lang.as_deref()),
            f.m.as_ref().and_then(|m| m.i),
        )
    }
    out.sort_unstable_by(|a, b| {
        a.s.cmp(&b.s)
            .then_with(|| a.p.cmp(&b.p))
            .then_with(|| a.o.cmp(&b.o))
            .then_with(|| a.dt.cmp(&b.dt))
            .then_with(|| meta_key(a).cmp(&meta_key(b)))
    });
    out
}

// ---------------------------------------------------------------------------
// ExportResolver — novelty-aware ID resolution for export
// ---------------------------------------------------------------------------

/// Wraps `BinaryIndexStore` with fallback to `DictNovelty` and an ephemeral
/// predicate map, so that export can resolve IDs for data that has been
/// committed but not yet persisted to the binary index.
struct ExportResolver<'a> {
    store: &'a Arc<BinaryIndexStore>,
    dict_novelty: Option<&'a Arc<DictNovelty>>,
    /// Reverse map: ephemeral p_id → Sid (inverted from translate_overlay_flakes_with_untranslated).
    ephemeral_preds_reverse: HashMap<u32, Sid>,
}

impl<'a> ExportResolver<'a> {
    fn new(
        store: &'a Arc<BinaryIndexStore>,
        dict_novelty: Option<&'a Arc<DictNovelty>>,
        ephemeral_preds: &EphemeralPredicateMap,
    ) -> Self {
        // Invert the Sid→p_id map to p_id→Sid for O(1) reverse lookup.
        let ephemeral_preds_reverse: HashMap<u32, Sid> = ephemeral_preds
            .iter()
            .map(|(sid, &pid)| (pid, sid.clone()))
            .collect();
        Self {
            store,
            dict_novelty,
            ephemeral_preds_reverse,
        }
    }

    /// Resolve a subject ID to an IRI string.
    ///
    /// Falls back to `DictNovelty` for IDs above the persisted watermark.
    fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        match self.store.resolve_subject_iri(s_id) {
            Ok(iri) => Ok(iri),
            Err(_) => {
                if let Some(dn) = self.dict_novelty {
                    if dn.is_initialized() {
                        if let Some((ns_code, suffix)) = dn.subjects.resolve_subject(s_id) {
                            if namespaces::is_full_iri(ns_code) {
                                return Ok(suffix.to_string());
                            }
                            let prefix = self.store.namespace_prefix(ns_code)?;
                            return Ok(format!("{prefix}{suffix}"));
                        }
                    }
                }
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("subject s_id {s_id} not found in store or DictNovelty"),
                ))
            }
        }
    }

    /// Resolve a subject ID to the `Sid` the write path stored it under.
    ///
    /// Mirrors `BinaryGraphView::resolve_subject_sid_uncached`: a novel id
    /// (above its namespace's watermark) resolves straight from `DictNovelty`
    /// to `Sid(ns_code, suffix)`, which is the exact value the transaction
    /// wrote; anything persisted round-trips through the subject dictionary.
    /// Exactness matters because the resulting `EdgeKey` is a seek key into
    /// arena leaves sorted by the derived `Ord` — a Sid that differs in any
    /// position lands on the wrong span and reports "no annotations" rather
    /// than failing.
    fn resolve_subject_sid(&self, s_id: u64) -> io::Result<Sid> {
        if let Some(dn) = self.dict_novelty {
            if dn.is_initialized() {
                let sid64 = fluree_db_core::subject_id::SubjectId::from_u64(s_id);
                if sid64.local_id() > dn.subjects.watermark_for_ns(sid64.ns_code()) {
                    if let Some((ns_code, suffix)) = dn.subjects.resolve_subject(s_id) {
                        return Ok(Sid::new(ns_code, suffix));
                    }
                }
            }
        }
        let iri = self.resolve_subject_iri(s_id)?;
        Ok(self
            .store
            .find_subject_sid(&iri)?
            .unwrap_or_else(|| self.store.encode_iri(&iri)))
    }

    /// Resolve a predicate ID to its `Sid`. The ephemeral map holds the
    /// original `Sid` for novelty-only predicates, so no re-encoding is needed
    /// on that branch.
    fn resolve_predicate_sid(&self, p_id: u32) -> Option<Sid> {
        self.store
            .predicate_sid(p_id)
            .or_else(|| self.ephemeral_preds_reverse.get(&p_id).cloned())
    }

    /// Resolve a predicate ID to an IRI string.
    ///
    /// Falls back to the ephemeral predicate map for novelty-only predicates.
    fn resolve_predicate_iri(&self, p_id: u32) -> Option<String> {
        if let Some(iri) = self.store.resolve_predicate_iri(p_id) {
            return Some(iri.to_string());
        }
        self.ephemeral_preds_reverse
            .get(&p_id)
            .and_then(|sid| self.store.sid_to_iri(sid))
    }

    /// Decode an object value from its binary representation.
    ///
    /// Falls back to `DictNovelty` for string-dict and IRI-ref values
    /// that are not in the persisted index.
    fn decode_value(
        &self,
        o_type: u16,
        o_key: u64,
        p_id: u32,
        g_id: GraphId,
    ) -> io::Result<FlakeValue> {
        match self.store.decode_value_v3(o_type, o_key, p_id, g_id) {
            Ok(val) => Ok(val),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // The persisted store couldn't find a dict entry. Try DictNovelty
                // for the decode kinds that use subject/string dictionaries.
                let ot = OType::from_u16(o_type);
                match ot.decode_kind() {
                    DecodeKind::StringDict | DecodeKind::JsonArena => {
                        self.decode_string_novelty(ot.decode_kind(), o_key)
                    }
                    DecodeKind::IriRef => self.decode_iri_ref_novelty(o_key),
                    _ => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Fallback for StringDict / JsonArena: resolve via DictNovelty.
    fn decode_string_novelty(&self, kind: DecodeKind, o_key: u64) -> io::Result<FlakeValue> {
        let str_id = o_key as u32;
        if let Some(dn) = self.dict_novelty {
            if dn.is_initialized() {
                if let Some(value) = dn.strings.resolve_string(str_id) {
                    return Ok(match kind {
                        DecodeKind::JsonArena => FlakeValue::Json(value.to_string()),
                        _ => FlakeValue::String(value.to_string()),
                    });
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("string id {str_id} not found in store or DictNovelty"),
        ))
    }

    /// Fallback for IriRef: resolve subject ID via DictNovelty.
    fn decode_iri_ref_novelty(&self, o_key: u64) -> io::Result<FlakeValue> {
        let iri = self.resolve_subject_iri(o_key)?;
        let sid = self.store.encode_iri(&iri);
        Ok(FlakeValue::Ref(sid))
    }
}

impl crate::export_annotations::ReifierSubject for ExportResolver<'_> {
    fn reifier_sid(&self, s_id: u64) -> io::Result<Sid> {
        self.resolve_subject_sid(s_id)
    }
}

// ---------------------------------------------------------------------------
// RDF 1.2 annotations
// ---------------------------------------------------------------------------

/// Per-export state for annotation emission: which predicate ids to suppress,
/// and the bookkeeping behind `ExportStats::annotations_out_of_scope`.
///
/// Built once per graph scan; absent entirely when the export is not emitting
/// annotation syntax, so the common path allocates nothing.
struct AnnotationContext<'a> {
    probe: &'a AnnotationProbe<'a>,
    /// `p_id`s of the seven `f:reifies*` predicates in this store's id space,
    /// persisted and ephemeral. Hoisted out of the row loop: suppression is
    /// then a scan of at most fourteen `u32`s, not an IRI comparison.
    reifies_p_ids: Vec<u32>,
    graph_sid: Option<Sid>,
}

impl<'a> AnnotationContext<'a> {
    fn new(resolver: &ExportResolver<'_>, config: &'a ExportConfig<'a>) -> Option<Self> {
        let probe = config.annotations?;
        let mut reifies_p_ids: Vec<u32> = fluree_vocab::reifies_iris::ALL
            .iter()
            .filter_map(|iri| resolver.store.find_predicate_id(iri))
            .collect();
        // Novelty-only predicates never reach the persisted dictionary; on a
        // never-indexed ledger *every* `f:reifies*` id is ephemeral.
        reifies_p_ids.extend(
            resolver
                .ephemeral_preds_reverse
                .iter()
                .filter(|(_, sid)| fluree_db_core::namespaces::is_reserved_reifies_predicate(sid))
                .map(|(p_id, _)| *p_id),
        );
        Some(Self {
            probe,
            reifies_p_ids,
            graph_sid: config.graph_sid.clone(),
        })
    }

    #[inline]
    fn is_reifies_row(&self, p_id: u32) -> bool {
        self.reifies_p_ids.contains(&p_id)
    }
}

/// Live reifiers for every row of `batch`, row-aligned.
///
/// Returns an empty vec when the export is not emitting annotation syntax;
/// callers treat a missing entry as "no reifiers", so no writer needs a
/// branch on the mode.
///
/// This re-decodes each row's subject, predicate and object to build its
/// `EdgeKey` — work the row writer then does again. That duplication is
/// deliberate path separation: it happens only for ledgers that carry
/// annotations, and it keeps the row writers' existing loop untouched for
/// every ledger that does not.
async fn batch_reifiers(
    resolver: &ExportResolver<'_>,
    ann: Option<&AnnotationContext<'_>>,
    batch: &ColumnBatch,
    g_id: GraphId,
) -> io::Result<Vec<Vec<Sid>>> {
    let Some(ann) = ann else {
        return Ok(Vec::new());
    };
    let mut edges: Vec<EdgeKey> = Vec::new();
    let mut edge_row: Vec<usize> = Vec::new();
    for row in 0..batch.row_count {
        let p_id = batch.p_id.get_or(row, 0);
        if ann.is_reifies_row(p_id) {
            continue; // the bundle itself is never an annotated edge
        }
        let o_type = batch.o_type.get_or(row, 0);
        let o_key = batch.o_key.get(row);
        let Some(p) = resolver.resolve_predicate_sid(p_id) else {
            continue;
        };
        let Ok(s) = resolver.resolve_subject_sid(batch.s_id.get(row)) else {
            continue;
        };
        let Ok(o) = resolver.decode_value(o_type, o_key, p_id, g_id) else {
            continue;
        };
        if matches!(o, FlakeValue::Null) {
            continue;
        }
        // `resolve_datatype_sid_for_value`, not `resolve_datatype_sid`. The
        // `NUM_BIG_OVERFLOW` arena holds both overflow `xsd:integer` and
        // `xsd:decimal`, so the o_type alone names no datatype and the plain
        // form returns `None` — which made this `continue` silently skip
        // building a seek key for those rows, so they could never be matched
        // against the arena and lost their `~ <r>` marker whatever the
        // annotation source. The value-aware form exists for exactly this
        // ambiguity (added for #1329, where the same gap rendered big
        // numerics with an empty `@type`); this call site had not adopted it.
        let Some(dt) = resolver.store.resolve_datatype_sid_for_value(o_type, &o) else {
            continue;
        };
        edges.push(EdgeKey {
            g: ann.graph_sid.clone(),
            s,
            p,
            o,
            dt,
            lang: resolver.store.resolve_lang_tag(o_type).map(str::to_owned),
            // v1 stores `None` for every edge; list-occurrence annotations
            // are deferred (see `EdgeKey::list_i`).
            list_i: None,
        });
        edge_row.push(row);
    }
    let per_edge = ann.probe.live_reifiers(&edges).await?;
    let mut out = vec![Vec::new(); batch.row_count];
    for (i, row) in edge_row.into_iter().enumerate() {
        out[row] = per_edge[i].clone();
    }
    Ok(out)
}

/// Reifiers for one row, or the empty slice.
#[inline]
fn row_reifiers(reifiers: &[Vec<Sid>], row: usize) -> &[Sid] {
    reifiers.get(row).map_or(&[], Vec::as_slice)
}

/// IRI → prefixed name compression for Turtle/TriG (and JSON-LD compact IRIs).
pub use fluree_graph_format::PrefixMap;

// ---------------------------------------------------------------------------
// Turtle streaming export
// ---------------------------------------------------------------------------

/// Stream triples from the SPOT index of one graph as Turtle to `writer`.
///
/// Uses subject grouping (`;` between predicates of the same subject)
/// and prefixed names where possible.
pub async fn export_graph_turtle<W: Write>(
    store: &Arc<BinaryIndexStore>,
    config: &ExportConfig<'_>,
    prefixes: &PrefixMap,
    writer: &mut W,
) -> io::Result<ExportStats> {
    let branch = spot_branch(store, config.g_id);

    let filter = BinaryFilter::default();
    // Full identity projection (incl. OI): when a novelty overlay is attached,
    // the cursor merges base rows against overlay ops on the full V3 identity
    // (s_id, p_id, o_type, o_key, o_i). A narrower projection makes the missing
    // columns read as defaults and corrupts the merge.
    let projection = ColumnProjection::all();

    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        RunSortOrder::Spot,
        branch,
        filter,
        projection,
    );
    let (ephemeral_preds, untranslated) = apply_time_travel(&mut cursor, config, store);
    let resolver = ExportResolver::new(store, config.dict_novelty, &ephemeral_preds);
    let ann = AnnotationContext::new(&resolver, config);

    let mut stats = ExportStats::default();
    let mut prev_subject: Option<String> = None;

    // Untranslated overlay rows are folded into the subject block they belong
    // to rather than appended after the stream, so a subject never opens twice
    // (see `UntranslatedBySubject`).
    let (untranslated, untranslated_reifiers) =
        resolve_untranslated(ann.as_ref(), untranslated).await?;
    let mut untranslated = UntranslatedBySubject::new(store, untranslated);

    while let Some(batch) = cursor.next_batch()? {
        let reifiers = batch_reifiers(&resolver, ann.as_ref(), &batch, config.g_id).await?;
        write_turtle_batch(
            &resolver,
            ann.as_ref(),
            &reifiers,
            &untranslated_reifiers,
            &batch,
            config.g_id,
            prefixes,
            &mut prev_subject,
            &mut untranslated,
            &mut stats,
            writer,
        )?;
    }

    // Close last subject if any, folding in its untranslated rows first.
    if let Some(s_iri) = prev_subject.take() {
        if let Some(flakes) = untranslated.take(&s_iri) {
            write_untranslated_turtle_continuations(
                &resolver,
                ann.as_ref(),
                &untranslated_reifiers,
                &flakes,
                prefixes,
                &mut stats,
                writer,
            )?;
        }
        writeln!(writer, " .")?;
    }

    // Subjects the base stream never reached get their own blocks.
    let (remaining, unresolved) = untranslated.into_remaining();
    for (s_iri, flakes) in &remaining {
        write_untranslated_turtle_block(
            &resolver,
            ann.as_ref(),
            &untranslated_reifiers,
            s_iri,
            flakes,
            prefixes,
            &mut stats,
            writer,
        )?;
    }
    for flake in &unresolved {
        write_raw_flake_turtle(&resolver, flake, prefixes, &mut stats, writer)?;
    }

    Ok(stats)
}

/// Split untranslated overlay rows into the bundle rows annotation syntax
/// replaces and the base rows that may carry a marker, resolving every
/// reifier in one probe call.
///
/// Untranslated rows never pass through `is_reifies_row` — only the
/// translated writers call it — so before this they reached the output raw:
/// a *partial* `f:reifies*` bundle (the rows that did translate were
/// suppressed) and no `~ <r>` on the edge it described. Round-tripping that
/// file plants a reserved predicate in the target ledger as ordinary data.
///
/// Filtering them alone would have been worse than the leak. The unresolved
/// counter only moves where the translated path calls `note_bundle_in_scope`,
/// so a silent filter converts a visible wrong answer into an invisible one.
/// Suppression and accounting are the same change.
///
/// One `live_reifiers` call for the whole untranslated set rather than one
/// per row: `batch_reifiers` is already a per-row probe on annotated ledgers,
/// and stacking a second one is the wrong direction for that cost.
async fn resolve_untranslated(
    ann: Option<&AnnotationContext<'_>>,
    rows: Vec<Flake>,
) -> io::Result<(Vec<Flake>, HashMap<EdgeKey, Vec<Sid>>)> {
    let Some(ann) = ann else {
        // `--raw-reifies` and annotation-free ledgers want the rows verbatim.
        return Ok((rows, HashMap::new()));
    };
    let mut base: Vec<Flake> = Vec::with_capacity(rows.len());
    for f in rows {
        if fluree_db_core::namespaces::is_reserved_reifies_predicate(&f.p) {
            ann.probe.note_bundle_sid(f.s.clone());
            continue;
        }
        base.push(f);
    }
    let keys: Vec<EdgeKey> = base.iter().map(EdgeKey::from_flake).collect();
    let live = ann.probe.live_reifiers(&keys).await?;
    let mut map: HashMap<EdgeKey, Vec<Sid>> = HashMap::new();
    for (key, reifiers) in keys.into_iter().zip(live) {
        if !reifiers.is_empty() {
            map.insert(key, reifiers);
        }
    }
    Ok((base, map))
}

/// Reifiers for an untranslated row, from the map `resolve_untranslated`
/// built. Empty when the row is not an annotated edge.
fn untranslated_reifiers_for<'m>(map: &'m HashMap<EdgeKey, Vec<Sid>>, flake: &Flake) -> &'m [Sid] {
    map.get(&EdgeKey::from_flake(flake))
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// Write a batch of rows as Turtle, grouping by subject.
#[allow(clippy::too_many_arguments)]
fn write_turtle_batch<W: Write>(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifiers: &[Vec<Sid>],
    untranslated_reifiers: &HashMap<EdgeKey, Vec<Sid>>,
    batch: &ColumnBatch,
    g_id: GraphId,
    prefixes: &PrefixMap,
    prev_subject: &mut Option<String>,
    untranslated: &mut UntranslatedBySubject,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    for row in 0..batch.row_count {
        let s_id = batch.s_id.get(row);
        let p_id = batch.p_id.get_or(row, 0);
        let o_type = batch.o_type.get_or(row, 0);
        let o_key = batch.o_key.get(row);

        // The `f:reifies*` bundle is the on-disk encoding of an annotation,
        // not a triple the ledger was asked to hold. It is replaced by the
        // `~ <r>` markers emitted below, and re-emitting it too would produce
        // a file the write path refuses to ingest.
        if let Some(ann) = ann {
            if ann.is_reifies_row(p_id) {
                ann.probe.note_bundle_in_scope(resolver, s_id);
                continue;
            }
        }

        let s_iri = resolver.resolve_subject_iri(s_id)?;
        let p_iri = match resolver.resolve_predicate_iri(p_id) {
            Some(p) => p,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };
        let value = resolver.decode_value(o_type, o_key, p_id, g_id)?;
        if matches!(value, FlakeValue::Null) {
            stats.rows_skipped += 1;
            continue;
        }

        let same_subject = prev_subject.as_deref() == Some(&s_iri);

        if same_subject {
            // Continue same subject — semicolon separator
            write!(writer, " ;\n    ")?;
        } else {
            // New subject — close previous if any, folding in the untranslated
            // rows that belong to it so the block is written once.
            if let Some(prev) = prev_subject.take() {
                if let Some(flakes) = untranslated.take(&prev) {
                    write_untranslated_turtle_continuations(
                        resolver,
                        ann,
                        untranslated_reifiers,
                        &flakes,
                        prefixes,
                        stats,
                        writer,
                    )?;
                }
                writeln!(writer, " .")?;
            }
            // Write subject
            write_turtle_iri_or_bnode(writer, &s_iri, prefixes)?;
            write!(writer, "\n    ")?;
            *prev_subject = Some(s_iri);
        }

        // Write predicate
        if p_iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
            writer.write_all(b"a")?;
        } else {
            prefixes.write_iri(writer, &p_iri)?;
        }
        writer.write_all(b" ")?;

        // Write object
        write_turtle_object(writer, &value, resolver.store, o_type, prefixes)?;

        // RDF 1.2 reifier markers: `s p o ~ <r1> ~ <r2>`. The reifiers' own
        // property blocks are left where the scan puts them, later in the
        // stream as ordinary subjects — inlining a `{| … |}` body would need
        // a random seek per reifier, out of scan order, at the exact moment
        // the base edge is written.
        if let Some(ann) = ann {
            for reifier in row_reifiers(reifiers, row) {
                let Some(iri) = resolver.store.sid_to_iri(reifier) else {
                    continue;
                };
                writer.write_all(b" ~ ")?;
                write_turtle_iri_or_bnode(writer, &iri, prefixes)?;
                ann.probe.note_reifier_named(reifier);
            }
        }

        stats.triples_written += 1;
    }
    Ok(())
}

/// Write a subject term as Turtle (prefixed name, `<iri>`, or `_:bnode`).
///
/// Blank-node labels are written verbatim, deliberately: an export has to
/// round-trip, and rewriting a label to fit the Turtle grammar would merge two
/// distinct nodes onto one id (contrast `crate::validate`, whose report output
/// sanitizes because it does not round-trip).
///
/// Current blank-node ids are all writable as-is: import mints `[0-9a-z-]`
/// (see `fluree_db_core::skolem`), staged transactions mint hex, `BNODE()`
/// mints a UUID. Ledgers imported by Fluree 4.1.4 or earlier hold ids that
/// embedded the ledger id, so they contain `/` and `:` and an export of such a
/// ledger emits Turtle that strict parsers reject — the identity is preserved,
/// the syntax is not. Re-importing the source fixes it.
fn write_turtle_iri_or_bnode<W: Write>(
    w: &mut W,
    iri: &str,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    if iri.starts_with("_:") {
        w.write_all(iri.as_bytes())
    } else {
        prefixes.write_iri(w, iri)
    }
}

/// Write a Turtle object term (with prefix compression for IRI refs).
fn write_turtle_object<W: Write>(
    w: &mut W,
    value: &FlakeValue,
    store: &BinaryIndexStore,
    o_type: u16,
    prefixes: &PrefixMap,
) -> io::Result<()> {
    match value {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            write_turtle_iri_or_bnode(w, &iri, prefixes)
        }
        // For all literal types, reuse the N-Triples formatting
        // (Turtle literal syntax is a superset of N-Triples)
        _ => write_object(w, value, store, o_type),
    }
}

// ---------------------------------------------------------------------------
// JSON-LD streaming export
// ---------------------------------------------------------------------------

/// Stream triples from the SPOT index of one graph as JSON-LD to `writer`.
///
/// Produces a JSON-LD document with `@context` and `@graph`.  Streams one
/// subject at a time — memory is O(largest subject), not O(dataset).
///
/// Value rules:
/// - `xsd:string` → plain JSON string (no `@type`)
/// - `xsd:boolean` → native JSON boolean
/// - `xsd:integer`/`xsd:long`/etc. → `{"@value": n, "@type": "xsd:integer"}`
/// - `xsd:decimal` → `{"@value": "...", "@type": "xsd:decimal"}`
/// - `xsd:double` → `{"@value": n, "@type": "xsd:double"}`
/// - Language strings → `{"@value": "...", "@language": "..."}`
/// - Other typed literals → `{"@value": "...", "@type": "..."}`
/// - Refs → `{"@id": "iri"}`
/// - Single-cardinality properties are unwrapped (not in `[]`)
pub async fn export_graph_jsonld<W: Write>(
    store: &Arc<BinaryIndexStore>,
    config: &ExportConfig<'_>,
    prefixes: &PrefixMap,
    writer: &mut W,
) -> io::Result<ExportStats> {
    let branch = spot_branch(store, config.g_id);

    let filter = BinaryFilter::default();
    // Full identity projection (incl. OI): when a novelty overlay is attached,
    // the cursor merges base rows against overlay ops on the full V3 identity
    // (s_id, p_id, o_type, o_key, o_i). A narrower projection makes the missing
    // columns read as defaults and corrupts the merge.
    let projection = ColumnProjection::all();

    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        RunSortOrder::Spot,
        branch,
        filter,
        projection,
    );
    let (ephemeral_preds, untranslated) = apply_time_travel(&mut cursor, config, store);
    let resolver = ExportResolver::new(store, config.dict_novelty, &ephemeral_preds);
    let ann = AnnotationContext::new(&resolver, config);

    let mut stats = ExportStats::default();

    // Accumulate properties for the current subject.
    // Key = predicate IRI, Value = list of JSON-LD values.
    let mut current_subject: Option<String> = None;
    let mut current_props: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
    let mut first_node = true;
    let (untranslated, untranslated_reifiers) =
        resolve_untranslated(ann.as_ref(), untranslated).await?;
    let mut untranslated = UntranslatedBySubject::new(store, untranslated);

    while let Some(batch) = cursor.next_batch()? {
        let reifiers = batch_reifiers(&resolver, ann.as_ref(), &batch, config.g_id).await?;
        for row in 0..batch.row_count {
            let s_id = batch.s_id.get(row);
            let p_id = batch.p_id.get_or(row, 0);
            let o_type = batch.o_type.get_or(row, 0);
            let o_key = batch.o_key.get(row);

            if let Some(ann) = ann.as_ref() {
                if ann.is_reifies_row(p_id) {
                    ann.probe.note_bundle_in_scope(&resolver, s_id);
                    continue;
                }
            }

            let s_iri = resolver.resolve_subject_iri(s_id)?;
            let p_iri = match resolver.resolve_predicate_iri(p_id) {
                Some(p) => p.to_string(),
                None => {
                    stats.rows_skipped += 1;
                    continue;
                }
            };
            let value = resolver.decode_value(o_type, o_key, p_id, config.g_id)?;
            if matches!(value, FlakeValue::Null) {
                stats.rows_skipped += 1;
                continue;
            }

            // Convert to JSON-LD value
            let jval = flake_to_jsonld(&value, store, o_type, prefixes);
            // One value per reifier, each carrying `@annotation` — the JSON-LD
            // shape `parse/edge_annotations.rs` ingests. Repeating the base
            // value is how the keyword attaches to an edge: two reifiers on
            // one edge are two annotated occurrences of the same triple, which
            // re-ingest to one triple and two bundles.
            let jvals = match ann.as_ref() {
                Some(ann) => annotated_jsonld_values(
                    &resolver,
                    ann,
                    &jval,
                    row_reifiers(&reifiers, row),
                    prefixes,
                ),
                None => vec![jval],
            };

            // Check if we've moved to a new subject
            let same_subject = current_subject.as_deref() == Some(&s_iri);
            if !same_subject {
                // Flush previous subject, folding in its untranslated rows.
                if let Some(subj_iri) = current_subject.take() {
                    if let Some(flakes) = untranslated.take(&subj_iri) {
                        merge_untranslated_jsonld(
                            &resolver,
                            ann.as_ref(),
                            &untranslated_reifiers,
                            &flakes,
                            prefixes,
                            &mut current_props,
                            &mut stats,
                        );
                    }
                    write_jsonld_node(writer, &subj_iri, &current_props, prefixes, first_node)?;
                    first_node = false;
                }
                current_subject = Some(s_iri);
                current_props.clear();
            }

            // Append value to the right predicate bucket
            let compact_p = compact_iri(&p_iri, prefixes);
            if let Some(entry) = current_props.iter_mut().find(|(k, _)| *k == compact_p) {
                entry.1.extend(jvals);
            } else {
                current_props.push((compact_p, jvals));
            }

            stats.triples_written += 1;
        }
    }

    // Flush last subject, folding in its untranslated rows.
    if let Some(subj_iri) = current_subject.take() {
        if let Some(flakes) = untranslated.take(&subj_iri) {
            merge_untranslated_jsonld(
                &resolver,
                ann.as_ref(),
                &untranslated_reifiers,
                &flakes,
                prefixes,
                &mut current_props,
                &mut stats,
            );
        }
        write_jsonld_node(writer, &subj_iri, &current_props, prefixes, first_node)?;
        first_node = false;
    }

    // Subjects the base stream never reached get their own node objects. Rows
    // whose subject IRI does not resolve cannot be placed and are counted as
    // skipped, matching every other writer.
    let (remaining, unresolved) = untranslated.into_remaining();
    for (subj_iri, flakes) in &remaining {
        let mut props: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
        merge_untranslated_jsonld(
            &resolver,
            ann.as_ref(),
            &untranslated_reifiers,
            flakes,
            prefixes,
            &mut props,
            &mut stats,
        );
        if props.is_empty() {
            continue;
        }
        write_jsonld_node(writer, subj_iri, &props, prefixes, first_node)?;
        first_node = false;
    }
    stats.rows_skipped += unresolved.len() as u64;

    Ok(stats)
}

/// Fold a subject's untranslated rows into the property list about to be
/// written for that subject, so the node object is emitted once.
///
/// Mirrors the accumulator in `export_graph_jsonld`: same predicate bucketing,
/// same compaction. A repeated `@id` node object is legal JSON-LD and merges
/// on parse, but it is still a shape that depends on index state, which is
/// what `UntranslatedBySubject` exists to remove.
fn merge_untranslated_jsonld(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifier_map: &HashMap<EdgeKey, Vec<Sid>>,
    flakes: &[Flake],
    prefixes: &PrefixMap,
    props: &mut Vec<(String, Vec<serde_json::Value>)>,
    stats: &mut ExportStats,
) {
    let store = resolver.store;
    for flake in flakes {
        let (Some(p_iri), Some(jval)) = (
            store.sid_to_iri(&flake.p),
            flake_to_jsonld_raw(flake, store, prefixes),
        ) else {
            stats.rows_skipped += 1;
            continue;
        };
        // Same `@annotation` shape the translated path emits, via the same
        // helper — an untranslated row is no less annotated.
        let values = match ann {
            Some(ann) => annotated_jsonld_values(
                resolver,
                ann,
                &jval,
                untranslated_reifiers_for(reifier_map, flake),
                prefixes,
            ),
            None => vec![jval],
        };
        let compact_p = compact_iri(&p_iri, prefixes);
        if let Some(entry) = props.iter_mut().find(|(k, _)| *k == compact_p) {
            entry.1.extend(values);
        } else {
            props.push((compact_p, values));
        }
        stats.triples_written += 1;
    }
}

/// JSON-LD value for an untranslated overlay flake, deriving the language tag
/// from `flake.m` and the datatype from `flake.dt`. Returns `None` for value
/// variants that should never reach the untranslated set.
fn flake_to_jsonld_raw(
    flake: &Flake,
    store: &BinaryIndexStore,
    prefixes: &PrefixMap,
) -> Option<serde_json::Value> {
    let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
    let dt_iri = || store.sid_to_iri(&flake.dt);
    let typed = |s: String, dt: String| serde_json::json!({ "@value": s, "@type": compact_iri(&dt, prefixes) });
    match &flake.o {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            Some(serde_json::json!({ "@id": compact_iri(&iri, prefixes) }))
        }
        FlakeValue::String(s) => {
            if let Some(lang) = lang {
                return Some(serde_json::json!({ "@value": s, "@language": lang }));
            }
            match dt_iri().as_deref() {
                None | Some(xsd::STRING) => Some(serde_json::Value::String(s.clone())),
                Some(dt) => {
                    Some(serde_json::json!({ "@value": s, "@type": compact_iri(dt, prefixes) }))
                }
            }
        }
        FlakeValue::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        FlakeValue::Long(n) => Some(serde_json::json!({
            "@value": n,
            "@type": compact_iri(&dt_iri().unwrap_or_else(|| xsd::LONG.to_string()), prefixes)
        })),
        FlakeValue::BigInt(n) => Some(typed(
            n.to_string(),
            dt_iri().unwrap_or_else(|| xsd::INTEGER.to_string()),
        )),
        FlakeValue::Decimal(d) => Some(typed(
            d.to_string(),
            dt_iri().unwrap_or_else(|| xsd::DECIMAL.to_string()),
        )),
        FlakeValue::Double(f) => {
            let dt = compact_iri(
                &dt_iri().unwrap_or_else(|| xsd::DOUBLE.to_string()),
                prefixes,
            );
            if f.is_finite() {
                Some(serde_json::json!({ "@value": f, "@type": dt }))
            } else {
                let lexical = if f.is_nan() {
                    "NaN"
                } else if f.is_sign_positive() {
                    "INF"
                } else {
                    "-INF"
                };
                Some(serde_json::json!({ "@value": lexical, "@type": dt }))
            }
        }
        FlakeValue::Vector(v) => Some(serde_json::json!({
            "@value": v,
            "@type": compact_iri(&dt_iri().unwrap_or_else(|| "https://ns.flur.ee/db#vector".to_string()), prefixes)
        })),
        // Temporal / other types always encode into V3 ops.
        _ => None,
    }
}

/// One JSON-LD value per reifier, each carrying an `@annotation` block; the
/// bare value when the edge has none.
///
/// A scalar value (`"ex:name": "Alice"`) has nowhere to hang a keyword, so an
/// annotated one is promoted to its `{"@value": …}` object form. `{"@id": …}`
/// objects take the keyword directly.
fn annotated_jsonld_values(
    resolver: &ExportResolver,
    ann: &AnnotationContext<'_>,
    jval: &serde_json::Value,
    reifiers: &[Sid],
    prefixes: &PrefixMap,
) -> Vec<serde_json::Value> {
    if reifiers.is_empty() {
        return vec![jval.clone()];
    }
    let mut out = Vec::with_capacity(reifiers.len());
    for reifier in reifiers {
        let Some(r_iri) = resolver.store.sid_to_iri(reifier) else {
            continue;
        };
        let mut obj = match jval {
            serde_json::Value::Object(map) => map.clone(),
            scalar => {
                let mut map = serde_json::Map::new();
                map.insert("@value".to_string(), scalar.clone());
                map
            }
        };
        obj.insert(
            "@annotation".to_string(),
            serde_json::json!({ "@id": compact_iri(&r_iri, prefixes) }),
        );
        out.push(serde_json::Value::Object(obj));
        ann.probe.note_reifier_named(reifier);
    }
    if out.is_empty() {
        out.push(jval.clone());
    }
    out
}

/// Write the JSON-LD document header: `{"@context": {...}, "@graph": [`
pub fn write_jsonld_header<W: Write>(prefixes: &PrefixMap, writer: &mut W) -> io::Result<()> {
    writer.write_all(b"{\n  \"@context\": ")?;

    // Build context object
    let mut ctx = serde_json::Map::new();
    // Sort alphabetically for deterministic output
    let mut sorted: Vec<(&str, &str)> = prefixes.iter().collect();
    sorted.sort_by_key(|(p, _)| *p);
    for (prefix, ns) in sorted {
        ctx.insert(
            prefix.to_string(),
            serde_json::Value::String(ns.to_string()),
        );
    }

    let ctx_json =
        serde_json::to_string_pretty(&serde_json::Value::Object(ctx)).unwrap_or_default();
    // Indent context to match nesting
    for (i, line) in ctx_json.lines().enumerate() {
        if i > 0 {
            writer.write_all(b"\n  ")?;
        }
        writer.write_all(line.as_bytes())?;
    }

    writer.write_all(b",\n  \"@graph\": [")?;
    Ok(())
}

/// Write the JSON-LD document footer: `]}`
pub fn write_jsonld_footer<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(b"\n  ]\n}\n")
}

/// Write a single JSON-LD node object for a subject.
fn write_jsonld_node<W: Write>(
    writer: &mut W,
    subject_iri: &str,
    props: &[(String, Vec<serde_json::Value>)],
    prefixes: &PrefixMap,
    first: bool,
) -> io::Result<()> {
    if !first {
        writer.write_all(b",")?;
    }
    writer.write_all(b"\n    {")?;

    // @id
    let compact_id = compact_iri(subject_iri, prefixes);
    write!(writer, "\"@id\": \"{}\"", escape_json_string(&compact_id))?;

    // Separate @type from other properties
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let rdf_type_compact = compact_iri(rdf_type, prefixes);

    for (pred, values) in props {
        // rdf:type gets special @type treatment
        if *pred == rdf_type_compact || *pred == rdf_type {
            writer.write_all(b", \"@type\": ")?;
            // Extract @id values for types
            let type_iris: Vec<&str> = values
                .iter()
                .filter_map(|v| {
                    v.as_object()
                        .and_then(|o| o.get("@id"))
                        .and_then(|id| id.as_str())
                })
                .collect();
            if type_iris.len() == 1 {
                write!(writer, "\"{}\"", escape_json_string(type_iris[0]))?;
            } else {
                writer.write_all(b"[")?;
                for (i, t) in type_iris.iter().enumerate() {
                    if i > 0 {
                        writer.write_all(b", ")?;
                    }
                    write!(writer, "\"{}\"", escape_json_string(t))?;
                }
                writer.write_all(b"]")?;
            }
            continue;
        }

        write!(writer, ", \"{}\": ", escape_json_string(pred))?;

        if values.len() == 1 {
            // Single cardinality — unwrap
            let json_str = serde_json::to_string(&values[0]).unwrap_or_default();
            writer.write_all(json_str.as_bytes())?;
        } else {
            // Multi cardinality — array
            writer.write_all(b"[")?;
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b", ")?;
                }
                let json_str = serde_json::to_string(v).unwrap_or_default();
                writer.write_all(json_str.as_bytes())?;
            }
            writer.write_all(b"]")?;
        }
    }

    writer.write_all(b"}")
}

/// Convert a FlakeValue to a JSON-LD value representation.
fn flake_to_jsonld(
    value: &FlakeValue,
    store: &BinaryIndexStore,
    o_type: u16,
    prefixes: &PrefixMap,
) -> serde_json::Value {
    match value {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            let compact = compact_iri(&iri, prefixes);
            serde_json::json!({ "@id": compact })
        }

        FlakeValue::String(s) => {
            // Language-tagged string
            if let Some(lang) = store.resolve_lang_tag(o_type) {
                return serde_json::json!({ "@value": s, "@language": lang });
            }

            // Resolve datatype
            let dt_iri = resolve_datatype_iri(store, o_type);
            match dt_iri.as_deref() {
                None | Some(xsd::STRING) => {
                    // Plain string — no @type needed
                    serde_json::Value::String(s.clone())
                }
                Some(dt) => {
                    let compact_dt = compact_iri(dt, prefixes);
                    serde_json::json!({ "@value": s, "@type": compact_dt })
                }
            }
        }

        FlakeValue::Boolean(b) => {
            // Native JSON boolean
            serde_json::Value::Bool(*b)
        }

        FlakeValue::Long(n) => {
            let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::LONG.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": n, "@type": compact_dt })
        }

        FlakeValue::Double(f) => {
            let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::DOUBLE.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            if f.is_finite() {
                serde_json::json!({ "@value": f, "@type": compact_dt })
            } else {
                // NaN/Infinity must be string-encoded
                let lexical = if f.is_nan() {
                    "NaN"
                } else if f.is_sign_positive() {
                    "INF"
                } else {
                    "-INF"
                };
                serde_json::json!({ "@value": lexical, "@type": compact_dt })
            }
        }

        FlakeValue::BigInt(n) => {
            let dt =
                resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::INTEGER.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            // Try to fit in i64 for native JSON number
            let s = n.to_string();
            if let Ok(i) = s.parse::<i64>() {
                serde_json::json!({ "@value": i, "@type": compact_dt })
            } else {
                serde_json::json!({ "@value": s, "@type": compact_dt })
            }
        }

        FlakeValue::Decimal(d) => {
            let dt =
                resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::DECIMAL.to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": d.to_string(), "@type": compact_dt })
        }

        // Temporal types
        FlakeValue::DateTime(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::DATE_TIME, prefixes)
        }
        FlakeValue::Date(v) => typed_value_display(v.as_ref(), store, o_type, xsd::DATE, prefixes),
        FlakeValue::Time(v) => typed_value_display(v.as_ref(), store, o_type, xsd::TIME, prefixes),
        FlakeValue::GYear(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::G_YEAR, prefixes)
        }
        FlakeValue::GYearMonth(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::G_YEAR_MONTH, prefixes)
        }
        FlakeValue::GMonth(v) => {
            typed_value_display(v.as_ref(), store, o_type, xsd::G_MONTH, prefixes)
        }
        FlakeValue::GDay(v) => typed_value_display(v.as_ref(), store, o_type, xsd::G_DAY, prefixes),
        FlakeValue::GMonthDay(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gMonthDay",
            prefixes,
        ),
        FlakeValue::YearMonthDuration(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
            prefixes,
        ),
        FlakeValue::DayTimeDuration(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
            prefixes,
        ),
        FlakeValue::Duration(v) => typed_value_display(
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#duration",
            prefixes,
        ),

        // Extension types
        FlakeValue::Json(s) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON".to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            // Try to parse the JSON string into a native JSON value
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                serde_json::json!({ "@value": parsed, "@type": compact_dt })
            } else {
                serde_json::json!({ "@value": s, "@type": compact_dt })
            }
        }

        FlakeValue::Vector(v) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "https://ns.flur.ee/db#vector".to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": v, "@type": compact_dt })
        }

        FlakeValue::GeoPoint(bits) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.opengis.net/ont/geosparql#wktLiteral".to_string());
            let compact_dt = compact_iri(&dt, prefixes);
            serde_json::json!({ "@value": bits.to_string(), "@type": compact_dt })
        }

        FlakeValue::Null => serde_json::Value::Null,
    }
}

/// Helper: create a `{"@value": "display", "@type": "dt"}` JSON-LD value.
fn typed_value_display<T: std::fmt::Display>(
    value: &T,
    store: &BinaryIndexStore,
    o_type: u16,
    fallback_dt: &str,
    prefixes: &PrefixMap,
) -> serde_json::Value {
    let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| fallback_dt.to_string());
    let compact_dt = compact_iri(&dt, prefixes);
    serde_json::json!({ "@value": value.to_string(), "@type": compact_dt })
}

/// Compact an IRI using the prefix map, falling back to the full IRI.
fn compact_iri(iri: &str, prefixes: &PrefixMap) -> String {
    // Blank nodes pass through
    if iri.starts_with("_:") {
        return iri.to_string();
    }
    prefixes.compact(iri).unwrap_or_else(|| iri.to_string())
}

/// Escape a string for use as a JSON string value (without outer quotes).
fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let cp = c as u32;
                out.push_str(&format!("\\u{cp:04X}"));
            }
            c => out.push(c),
        }
    }
    out
}

/// Stream triples/quads from the SPOT index of one graph to `writer`.
///
/// Includes novelty overlay and respects `to_t` for time-travel export.
pub async fn export_graph_ntriples<W: Write>(
    store: &Arc<BinaryIndexStore>,
    config: &ExportConfig<'_>,
    writer: &mut W,
) -> io::Result<ExportStats> {
    let branch = spot_branch(store, config.g_id);

    let filter = BinaryFilter::default();
    // Full identity projection (incl. OI): when a novelty overlay is attached,
    // the cursor merges base rows against overlay ops on the full V3 identity
    // (s_id, p_id, o_type, o_key, o_i). A narrower projection makes the missing
    // columns read as defaults and corrupts the merge.
    let projection = ColumnProjection::all();

    let mut cursor = BinaryCursor::scan_all(
        Arc::clone(store),
        RunSortOrder::Spot,
        branch,
        filter,
        projection,
    );
    let (ephemeral_preds, untranslated) = apply_time_travel(&mut cursor, config, store);
    let resolver = ExportResolver::new(store, config.dict_novelty, &ephemeral_preds);
    let ann = AnnotationContext::new(&resolver, config);
    let (untranslated, untranslated_reifiers) =
        resolve_untranslated(ann.as_ref(), untranslated).await?;

    let mut stats = ExportStats::default();
    let graph_term = config.graph_iri.as_deref().map(|iri| {
        let mut buf = String::with_capacity(iri.len() + 2);
        buf.push('<');
        syntax::push_iri(&mut buf, iri);
        buf.push('>');
        buf
    });

    while let Some(batch) = cursor.next_batch()? {
        let reifiers = batch_reifiers(&resolver, ann.as_ref(), &batch, config.g_id).await?;
        write_batch(
            &resolver,
            ann.as_ref(),
            &reifiers,
            &batch,
            config.g_id,
            graph_term.as_deref(),
            &mut stats,
            writer,
        )?;
    }

    // Emit overlay flakes that could not be encoded into V3 ops (e.g.
    // novelty-only language tags) directly from their decoded form.
    for flake in &untranslated {
        let reifiers = untranslated_reifiers_for(&untranslated_reifiers, flake);
        write_raw_flake_ntriples(
            &resolver,
            ann.as_ref(),
            reifiers,
            flake,
            graph_term.as_deref(),
            &mut stats,
            writer,
        )?;
    }

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Batch → N-Triples / N-Quads
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn write_batch<W: Write>(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifiers: &[Vec<Sid>],
    batch: &ColumnBatch,
    g_id: GraphId,
    graph_term: Option<&str>,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    for row in 0..batch.row_count {
        let s_id = batch.s_id.get(row);
        let p_id = batch.p_id.get_or(row, 0);
        let o_type = batch.o_type.get_or(row, 0);
        let o_key = batch.o_key.get(row);

        if let Some(ann) = ann {
            if ann.is_reifies_row(p_id) {
                ann.probe.note_bundle_in_scope(resolver, s_id);
                continue;
            }
        }

        // Resolve subject
        let s_iri = resolver.resolve_subject_iri(s_id)?;

        // Resolve predicate
        let p_iri = match resolver.resolve_predicate_iri(p_id) {
            Some(p) => p,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };

        // Resolve object value
        let value = resolver.decode_value(o_type, o_key, p_id, g_id)?;
        if matches!(value, FlakeValue::Null) {
            stats.rows_skipped += 1;
            continue;
        }

        // Write subject
        write_iri_or_bnode(writer, &s_iri)?;
        writer.write_all(b" ")?;

        // Write predicate (always an IRI)
        writer.write_all(b"<")?;
        syntax::write_iri(writer, &p_iri)?;
        writer.write_all(b"> ")?;

        // Write object
        write_object(writer, &value, resolver.store, o_type)?;

        // Write optional graph term (N-Quads)
        if let Some(g) = graph_term {
            writer.write_all(b" ")?;
            writer.write_all(g.as_bytes())?;
        }

        writer.write_all(b" .\n")?;
        stats.triples_written += 1;

        // N-Triples has no annotation sugar, by design. The standards-correct
        // spelling is a triple term as the object of `rdf:reifies`, which
        // Fluree's Turtle and N-Quads readers both accept.
        if let Some(ann) = ann {
            for reifier in row_reifiers(reifiers, row) {
                let Some(r_iri) = resolver.store.sid_to_iri(reifier) else {
                    continue;
                };
                write_iri_or_bnode(writer, &r_iri)?;
                writer.write_all(b" <")?;
                syntax::write_iri(writer, fluree_vocab::rdf::REIFIES)?;
                writer.write_all(b"> <<( ")?;
                write_iri_or_bnode(writer, &s_iri)?;
                writer.write_all(b" <")?;
                syntax::write_iri(writer, &p_iri)?;
                writer.write_all(b"> ")?;
                write_object(writer, &value, resolver.store, o_type)?;
                writer.write_all(b" )>>")?;
                if let Some(g) = graph_term {
                    writer.write_all(b" ")?;
                    writer.write_all(g.as_bytes())?;
                }
                writer.write_all(b" .\n")?;
                ann.probe.note_reifier_named(reifier);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Term formatting
// ---------------------------------------------------------------------------

/// Write a subject term: `<iri>` or `_:bnode`.
fn write_iri_or_bnode<W: Write>(w: &mut W, iri: &str) -> io::Result<()> {
    if iri.starts_with("_:") {
        // Blank node — emit as-is (no angle brackets)
        w.write_all(iri.as_bytes())
    } else {
        w.write_all(b"<")?;
        syntax::write_iri(w, iri)?;
        w.write_all(b">")
    }
}

/// Write an object value as an N-Triples term.
/// `@tag` after a literal. A tag has no escape form, so an invalid one would
/// end the literal and read back as more triples: refuse it instead.
fn write_lang_tag<W: Write>(w: &mut W, lang: &str) -> io::Result<()> {
    if !syntax::is_lang_tag(lang) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("language tag {lang:?} cannot be written as Turtle or N-Triples"),
        ));
    }
    w.write_all(b"\"@")?;
    w.write_all(lang.as_bytes())
}

fn write_object<W: Write>(
    w: &mut W,
    value: &FlakeValue,
    store: &BinaryIndexStore,
    o_type: u16,
) -> io::Result<()> {
    match value {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            write_iri_or_bnode(w, &iri)
        }

        FlakeValue::String(s) => {
            // Check for language tag first (takes precedence over datatype)
            if let Some(lang) = store.resolve_lang_tag(o_type) {
                w.write_all(b"\"")?;
                syntax::write_string(w, s)?;
                write_lang_tag(w, lang)?;
                return Ok(());
            }

            // Resolve datatype; omit ^^<xsd:string> (implicit)
            let dt_iri = resolve_datatype_iri(store, o_type);
            w.write_all(b"\"")?;
            syntax::write_string(w, s)?;
            w.write_all(b"\"")?;
            if let Some(dt) = &dt_iri {
                if *dt != xsd::STRING {
                    w.write_all(b"^^<")?;
                    syntax::write_iri(w, dt)?;
                    w.write_all(b">")?;
                }
            }
            Ok(())
        }

        FlakeValue::Boolean(b) => {
            write_typed_literal(w, if *b { "true" } else { "false" }, xsd::BOOLEAN)
        }
        FlakeValue::Long(n) => write_typed_literal(
            w,
            &n.to_string(),
            &resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::LONG.to_string()),
        ),
        FlakeValue::Double(f) => write_typed_literal(
            // W3C canonical xsd:double form (1.0E6; NaN/INF/-INF preserved). Resolve
            // the DECLARED datatype like the Long/BigInt/Decimal arms below rather
            // than hardcoding xsd:double: a value stored as Double but declared under
            // another datatype (e.g. xsd:float) must render its declared type, not be
            // silently re-typed (CRITICAL-3 #1529 review).
            w,
            &canonical_xsd_double(*f),
            &resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::DOUBLE.to_string()),
        ),
        FlakeValue::BigInt(n) => write_typed_literal(
            w,
            &n.to_string(),
            &resolve_datatype_iri(store, o_type).unwrap_or_else(|| xsd::INTEGER.to_string()),
        ),
        FlakeValue::Decimal(d) => write_typed_literal(
            w,
            &d.to_string(),
            &resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#decimal".to_string()),
        ),

        // Temporal types — use Display for canonical lexical form
        FlakeValue::DateTime(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#dateTime",
        ),
        FlakeValue::Date(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#date",
        ),
        FlakeValue::Time(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#time",
        ),
        FlakeValue::GYear(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gYear",
        ),
        FlakeValue::GYearMonth(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gYearMonth",
        ),
        FlakeValue::GMonth(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gMonth",
        ),
        FlakeValue::GDay(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gDay",
        ),
        FlakeValue::GMonthDay(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#gMonthDay",
        ),
        FlakeValue::YearMonthDuration(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#yearMonthDuration",
        ),
        FlakeValue::DayTimeDuration(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#dayTimeDuration",
        ),
        FlakeValue::Duration(v) => write_typed_literal_display(
            w,
            v.as_ref(),
            store,
            o_type,
            "http://www.w3.org/2001/XMLSchema#duration",
        ),

        // Extension types
        FlakeValue::Json(s) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON".to_string());
            w.write_all(b"\"")?;
            syntax::write_string(w, s)?;
            w.write_all(b"\"^^<")?;
            syntax::write_iri(w, &dt)?;
            w.write_all(b">")
        }
        FlakeValue::Vector(v) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "https://ns.flur.ee/db#vector".to_string());
            // Serialize as JSON array string
            let json = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            w.write_all(b"\"")?;
            syntax::write_string(w, &json)?;
            w.write_all(b"\"^^<")?;
            syntax::write_iri(w, &dt)?;
            w.write_all(b">")
        }
        FlakeValue::GeoPoint(bits) => {
            let dt = resolve_datatype_iri(store, o_type)
                .unwrap_or_else(|| "http://www.opengis.net/ont/geosparql#wktLiteral".to_string());
            let wkt = bits.to_string(); // "POINT(lng lat)"
            w.write_all(b"\"")?;
            syntax::write_string(w, &wkt)?;
            w.write_all(b"\"^^<")?;
            syntax::write_iri(w, &dt)?;
            w.write_all(b">")
        }

        FlakeValue::Null => Ok(()), // should have been filtered above
    }
}

// ---------------------------------------------------------------------------
// Raw (untranslated) overlay flake emission
// ---------------------------------------------------------------------------
//
// Overlay flakes that could not be encoded into V3 ops (novelty-only language
// tags, vector/bigint/decimal values, novelty-only custom datatypes) carry
// their fully-decoded `(s, p, o, dt, m)`. These writers emit them directly,
// deriving the language tag from `flake.m` and the datatype IRI from
// `flake.dt`, instead of from an `o_type` the cursor never produced.

/// Resolve a raw flake's object term lexical form + datatype/lang, writing it
/// in N-Triples literal syntax. `prefixes` enables Turtle prefixed-name
/// compression for IRI refs (N-Triples passes `None`). Returns `false` if the
/// value variant is not representable (the caller counts it as skipped).
fn write_raw_object<W: Write>(
    w: &mut W,
    store: &BinaryIndexStore,
    flake: &Flake,
    prefixes: Option<&PrefixMap>,
) -> io::Result<bool> {
    let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
    let dt_iri = || store.sid_to_iri(&flake.dt);
    match &flake.o {
        FlakeValue::Ref(sid) => {
            let iri = store
                .sid_to_iri(sid)
                .unwrap_or_else(|| format!("_:unknown_{sid}"));
            match prefixes {
                Some(p) => write_turtle_iri_or_bnode(w, &iri, p)?,
                None => write_iri_or_bnode(w, &iri)?,
            }
            Ok(true)
        }
        FlakeValue::String(s) => {
            if let Some(lang) = lang {
                w.write_all(b"\"")?;
                syntax::write_string(w, s)?;
                write_lang_tag(w, lang)?;
            } else {
                w.write_all(b"\"")?;
                syntax::write_string(w, s)?;
                w.write_all(b"\"")?;
                if let Some(dt) = dt_iri() {
                    if dt != xsd::STRING {
                        w.write_all(b"^^<")?;
                        syntax::write_iri(w, &dt)?;
                        w.write_all(b">")?;
                    }
                }
            }
            Ok(true)
        }
        FlakeValue::Json(s) => {
            let dt = dt_iri()
                .unwrap_or_else(|| "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON".to_string());
            write_typed_literal(w, s, &dt)?;
            Ok(true)
        }
        FlakeValue::Boolean(b) => {
            write_typed_literal(w, if *b { "true" } else { "false" }, xsd::BOOLEAN)?;
            Ok(true)
        }
        FlakeValue::Long(n) => {
            write_typed_literal(
                w,
                &n.to_string(),
                &dt_iri().unwrap_or_else(|| xsd::LONG.to_string()),
            )?;
            Ok(true)
        }
        FlakeValue::BigInt(n) => {
            write_typed_literal(
                w,
                &n.to_string(),
                &dt_iri().unwrap_or_else(|| xsd::INTEGER.to_string()),
            )?;
            Ok(true)
        }
        FlakeValue::Decimal(d) => {
            write_typed_literal(
                w,
                &d.to_string(),
                &dt_iri().unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#decimal".to_string()),
            )?;
            Ok(true)
        }
        FlakeValue::Double(f) => {
            // W3C canonical xsd:double form (1.0E6; NaN/INF/-INF preserved)
            write_typed_literal(w, &canonical_xsd_double(*f), xsd::DOUBLE)?;
            Ok(true)
        }
        FlakeValue::Vector(v) => {
            let dt = dt_iri().unwrap_or_else(|| "https://ns.flur.ee/db#vector".to_string());
            let json = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            write_typed_literal(w, &json, &dt)?;
            Ok(true)
        }
        // Temporal and other types always encode into V3 ops, so they should
        // never reach the untranslated set. Decline rather than emit a
        // potentially non-canonical lexical form.
        _ => Ok(false),
    }
}

/// Emit a single untranslated overlay flake as an N-Triples / N-Quads statement.
fn write_raw_flake_ntriples<W: Write>(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifiers: &[Sid],
    flake: &Flake,
    graph_term: Option<&str>,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    let (Some(s_iri), Some(p_iri)) = (
        resolver.store.sid_to_iri(&flake.s),
        resolver.store.sid_to_iri(&flake.p),
    ) else {
        stats.rows_skipped += 1;
        return Ok(());
    };

    let mut body: Vec<u8> = Vec::new();
    write_iri_or_bnode(&mut body, &s_iri)?;
    body.write_all(b" <")?;
    syntax::write_iri(&mut body, &p_iri)?;
    body.write_all(b"> ")?;
    if !write_raw_object(&mut body, resolver.store, flake, None)? {
        stats.rows_skipped += 1;
        return Ok(());
    }
    writer.write_all(&body)?;
    if let Some(g) = graph_term {
        writer.write_all(b" ")?;
        writer.write_all(g.as_bytes())?;
    }
    writer.write_all(b" .\n")?;
    stats.triples_written += 1;

    // Same spelling the translated path uses: a triple term as the object of
    // `rdf:reifies`. `body` already holds `s <p> o`, which is exactly the
    // term, so it is reused rather than re-serialised.
    if let Some(ann) = ann {
        for reifier in reifiers {
            let Some(r_iri) = resolver.store.sid_to_iri(reifier) else {
                continue;
            };
            write_iri_or_bnode(writer, &r_iri)?;
            writer.write_all(b" <")?;
            syntax::write_iri(writer, fluree_vocab::rdf::REIFIES)?;
            writer.write_all(b"> <<( ")?;
            writer.write_all(&body)?;
            writer.write_all(b" )>>")?;
            if let Some(g) = graph_term {
                writer.write_all(b" ")?;
                writer.write_all(g.as_bytes())?;
            }
            writer.write_all(b" .\n")?;
            ann.probe.note_reifier_named(reifier);
        }
    }
    Ok(())
}

/// Overlay rows that missed V3 translation, indexed by the subject they belong
/// to.
///
/// `apply_time_travel` hands the cursor the overlay ops it could encode and
/// returns the rest; those bypass the cursor's sorted merge entirely, as
/// `surviving_untranslated`'s own contract says. Emitting them after the stream
/// stranded each one outside the subject block it belongs to, so a subject
/// could open twice in the same file.
///
/// That was always reachable — any commit after the last index build can
/// produce an untranslated row — but #1574 made it the normal case rather than
/// the edge one: a never-indexed ledger has no persisted dictionary to encode
/// against, so most of the ledger misses translation. Export's output *shape*
/// would then depend on whether the ledger happened to be indexed, which is
/// the one property a faithful-round-trip change cannot afford to add.
///
/// Keying by subject lets each writer fold a subject's untranslated rows into
/// that subject's block as it closes, and emit whatever the base stream never
/// reached as its own blocks afterwards. Memory is unchanged: these flakes were
/// already held for the whole export as a `Vec`.
struct UntranslatedBySubject {
    by_subject: BTreeMap<String, Vec<Flake>>,
    /// Rows whose subject IRI does not resolve. They cannot be grouped, and
    /// the per-row writers already count them as skipped; kept separate so
    /// that stays the writers' decision rather than being silently dropped
    /// here.
    unresolved: Vec<Flake>,
}

impl UntranslatedBySubject {
    fn new(store: &BinaryIndexStore, flakes: Vec<Flake>) -> Self {
        let mut by_subject: BTreeMap<String, Vec<Flake>> = BTreeMap::new();
        let mut unresolved = Vec::new();
        for flake in flakes {
            match store.sid_to_iri(&flake.s) {
                Some(s_iri) => by_subject.entry(s_iri).or_default().push(flake),
                None => unresolved.push(flake),
            }
        }
        Self {
            by_subject,
            unresolved,
        }
    }

    /// Rows for `s_iri`, removed so the closing pass cannot emit them twice.
    fn take(&mut self, s_iri: &str) -> Option<Vec<Flake>> {
        self.by_subject.remove(s_iri)
    }

    /// Subjects the base stream never reached, in IRI order for determinism.
    fn into_remaining(self) -> (Vec<(String, Vec<Flake>)>, Vec<Flake>) {
        (self.by_subject.into_iter().collect(), self.unresolved)
    }
}

/// Write one untranslated row's `predicate object` pair into `out`.
///
/// Returns `false` when the value variant is not representable, which is the
/// caller's cue to count a skipped row and emit nothing.
fn write_raw_po_turtle(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifiers: &[Sid],
    flake: &Flake,
    prefixes: &PrefixMap,
    out: &mut Vec<u8>,
) -> io::Result<bool> {
    let Some(p_iri) = resolver.store.sid_to_iri(&flake.p) else {
        return Ok(false);
    };
    if p_iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
        out.write_all(b"a")?;
    } else {
        prefixes.write_iri(out, &p_iri)?;
    }
    out.write_all(b" ")?;
    if !write_raw_object(out, resolver.store, flake, Some(prefixes))? {
        return Ok(false);
    }
    // Same `~ <r>` marker the translated path writes, for a row that reached
    // the output without ever passing through it.
    if let Some(ann) = ann {
        for reifier in reifiers {
            let Some(iri) = resolver.store.sid_to_iri(reifier) else {
                continue;
            };
            out.write_all(b" ~ ")?;
            write_turtle_iri_or_bnode(out, &iri, prefixes)?;
            ann.probe.note_reifier_named(reifier);
        }
    }
    Ok(true)
}

/// Append untranslated rows to the Turtle block that is currently open.
fn write_untranslated_turtle_continuations<W: Write>(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifier_map: &HashMap<EdgeKey, Vec<Sid>>,
    flakes: &[Flake],
    prefixes: &PrefixMap,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    for flake in flakes {
        let mut body: Vec<u8> = Vec::new();
        let reifiers = untranslated_reifiers_for(reifier_map, flake);
        if !write_raw_po_turtle(resolver, ann, reifiers, flake, prefixes, &mut body)? {
            stats.rows_skipped += 1;
            continue;
        }
        write!(writer, " ;\n    ")?;
        writer.write_all(&body)?;
        stats.triples_written += 1;
    }
    Ok(())
}

/// Emit a whole subject block for untranslated rows the base stream never
/// reached.
#[allow(clippy::too_many_arguments)]
fn write_untranslated_turtle_block<W: Write>(
    resolver: &ExportResolver,
    ann: Option<&AnnotationContext<'_>>,
    reifier_map: &HashMap<EdgeKey, Vec<Sid>>,
    s_iri: &str,
    flakes: &[Flake],
    prefixes: &PrefixMap,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    // Render the rows first: a subject whose every row is unrepresentable must
    // not leave a dangling subject term behind.
    let mut bodies: Vec<Vec<u8>> = Vec::new();
    for flake in flakes {
        let mut body: Vec<u8> = Vec::new();
        let reifiers = untranslated_reifiers_for(reifier_map, flake);
        if write_raw_po_turtle(resolver, ann, reifiers, flake, prefixes, &mut body)? {
            bodies.push(body);
        } else {
            stats.rows_skipped += 1;
        }
    }
    let Some((first, rest)) = bodies.split_first() else {
        return Ok(());
    };
    write_turtle_iri_or_bnode(writer, s_iri, prefixes)?;
    write!(writer, "\n    ")?;
    writer.write_all(first)?;
    stats.triples_written += 1;
    for body in rest {
        write!(writer, " ;\n    ")?;
        writer.write_all(body)?;
        stats.triples_written += 1;
    }
    writeln!(writer, " .")?;
    Ok(())
}

/// Emit a single untranslated overlay flake as a standalone Turtle statement.
fn write_raw_flake_turtle<W: Write>(
    resolver: &ExportResolver,
    flake: &Flake,
    prefixes: &PrefixMap,
    stats: &mut ExportStats,
    writer: &mut W,
) -> io::Result<()> {
    let (Some(s_iri), Some(p_iri)) = (
        resolver.store.sid_to_iri(&flake.s),
        resolver.store.sid_to_iri(&flake.p),
    ) else {
        stats.rows_skipped += 1;
        return Ok(());
    };

    let mut body: Vec<u8> = Vec::new();
    write_turtle_iri_or_bnode(&mut body, &s_iri, prefixes)?;
    body.write_all(b" ")?;
    if p_iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
        body.write_all(b"a")?;
    } else {
        prefixes.write_iri(&mut body, &p_iri)?;
    }
    body.write_all(b" ")?;
    if !write_raw_object(&mut body, resolver.store, flake, Some(prefixes))? {
        stats.rows_skipped += 1;
        return Ok(());
    }
    writer.write_all(&body)?;
    writer.write_all(b" .\n")?;
    stats.triples_written += 1;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the datatype IRI for an `o_type` code.
fn resolve_datatype_iri(store: &BinaryIndexStore, o_type: u16) -> Option<String> {
    store
        .resolve_datatype_sid(o_type)
        .and_then(|sid| store.sid_to_iri(&sid))
}

/// Write `"lexical"^^<datatype_iri>`.
fn write_typed_literal<W: Write>(w: &mut W, lexical: &str, datatype_iri: &str) -> io::Result<()> {
    w.write_all(b"\"")?;
    syntax::write_string(w, lexical)?;
    w.write_all(b"\"^^<")?;
    syntax::write_iri(w, datatype_iri)?;
    w.write_all(b">")
}

/// Write a typed literal using the Display impl for the lexical form.
fn write_typed_literal_display<W: Write, T: std::fmt::Display>(
    w: &mut W,
    value: &T,
    store: &BinaryIndexStore,
    o_type: u16,
    fallback_dt: &str,
) -> io::Result<()> {
    let lexical = value.to_string();
    let dt = resolve_datatype_iri(store, o_type).unwrap_or_else(|| fallback_dt.to_string());
    write_typed_literal(w, &lexical, &dt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_iri_or_bnode() {
        let mut buf = Vec::new();
        write_iri_or_bnode(&mut buf, "http://example.org/alice").unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "<http://example.org/alice>"
        );

        let mut buf = Vec::new();
        write_iri_or_bnode(&mut buf, "_:b123").unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "_:b123");
    }

    #[test]
    fn test_compact_iri() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);
        assert_eq!(compact_iri("http://example.org/alice", &pm), "ex:alice");
        assert_eq!(
            compact_iri("http://other.org/bob", &pm),
            "http://other.org/bob"
        );
        // Blank nodes pass through
        assert_eq!(compact_iri("_:b42", &pm), "_:b42");
    }

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "hello");
        assert_eq!(escape_json_string("a\"b"), "a\\\"b");
        assert_eq!(escape_json_string("a\\b"), "a\\\\b");
        assert_eq!(escape_json_string("a\nb\tc"), "a\\nb\\tc");
    }

    #[test]
    fn test_write_jsonld_header_footer() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        write_jsonld_header(&pm, &mut buf).unwrap();
        write_jsonld_footer(&mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("\"@context\""));
        assert!(output.contains("\"ex\": \"http://example.org/\""));
        assert!(output.contains("\"@graph\": ["));
        assert!(output.ends_with("  ]\n}\n"));
    }

    #[test]
    fn test_write_jsonld_node_single_value() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        let props = vec![("ex:name".to_string(), vec![serde_json::json!("Alice")])];
        write_jsonld_node(&mut buf, "http://example.org/alice", &props, &pm, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("\"@id\": \"ex:alice\""));
        // Single value should NOT be wrapped in array
        assert!(output.contains("\"ex:name\": \"Alice\""));
        assert!(!output.contains("[\"Alice\"]"));
    }

    #[test]
    fn test_write_jsonld_node_multi_value() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let mut buf = Vec::new();
        let props = vec![(
            "ex:tag".to_string(),
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )];
        write_jsonld_node(&mut buf, "http://example.org/thing", &props, &pm, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // Multi-value SHOULD be wrapped in array
        assert!(output.contains("\"ex:tag\": [\"a\", \"b\"]"));
    }

    #[test]
    fn test_write_jsonld_node_rdf_type() {
        let ctx = serde_json::json!({"ex": "http://example.org/"});
        let pm = PrefixMap::from_context(&ctx);

        let rdf_type = compact_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", &pm);
        let mut buf = Vec::new();
        let props = vec![(rdf_type, vec![serde_json::json!({"@id": "ex:Person"})])];
        write_jsonld_node(&mut buf, "http://example.org/alice", &props, &pm, true).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // rdf:type should become @type with unwrapped single value
        assert!(output.contains("\"@type\": \"ex:Person\""));
        assert!(!output.contains("rdf:type"));
    }
}
