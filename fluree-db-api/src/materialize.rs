//! Bulk materialization driver for `fluree materialize`.
//!
//! Streams a virtual (R2RML-over-Iceberg) graph source through the whole-graph
//! enumerator in [`fluree_db_r2rml::materialize`], emitting every
//! `(subject, predicate, object)` triple to a [`TripleObserver`]. This is the
//! provider-backed counterpart to the in-memory reference driver
//! [`fluree_db_r2rml::materialize::enumerate_from_batches`]: identical dims-first
//! ordering and the same per-batch `index_batch` / `emit_batch` calls, but the
//! batches stream from the provider's `scan_table` instead of being held in
//! memory.
//!
//! The observer is where the twin build plugs in — an N-Triples collector for a
//! parity diff, or (later chunks) an ingest-sink adapter that streams the
//! triples into the native import pipeline.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;

use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};
use fluree_db_r2rml::materialize::{
    emit_batch, plan, MaterializeStats, ParentIndexSet, TripleObserver,
};
use fluree_db_r2rml::mapping::TriplesMap;
use fluree_db_r2rml::{R2rmlError, RdfTerm};
use fluree_db_transact::import::ParsedChunk;
use fluree_db_transact::import_sink::{ImportSink, SpoolConfig, SpoolContext};
use fluree_db_transact::namespace::WorkerCache;
use fluree_db_transact::SharedNamespaceAllocator;
use fluree_graph_ir::{Datatype, GraphSink, TermId};
use fluree_vocab::UnresolvedDatatypeConstraint;

/// An error raised while materializing a virtual graph source.
#[derive(Debug, thiserror::Error)]
pub enum MaterializeError {
    /// Reading the source (catalog / table scan) failed.
    #[error("scan error: {0}")]
    Scan(#[from] fluree_db_query::error::QueryError),
    /// The mapping or a term failed to materialize.
    #[error(transparent)]
    Mapping(#[from] R2rmlError),
    /// A TriplesMap had no table name (SQL-query logical tables are unsupported
    /// for Iceberg sources, so this should not occur in practice).
    #[error("TriplesMap '{0}' has no logical table name")]
    NoTable(String),
}

/// Columns a scan of `tm` must project: the TriplesMap's own referenced columns
/// (subject template + every predicate-object map, including foreign-key child
/// columns) plus any parent join-key columns needed to index `tm` as a
/// foreign-key parent.
fn scan_projection(tm: &TriplesMap, parents: &ParentIndexSet) -> Vec<String> {
    let mut cols: Vec<String> = tm
        .referenced_columns()
        .into_iter()
        .map(String::from)
        .collect();
    cols.extend(parents.needed_parent_columns(&tm.iri));
    cols.sort();
    cols.dedup();
    cols
}

/// Enumerate every triple of a virtual R2RML graph source, streaming each
/// logical table through the whole-graph enumerator and emitting to `observer`.
/// Returns the [`MaterializeStats`] for the run (triple counts and the
/// per-`(child, predicate)` foreign-key edge counts used by the parity gate).
///
/// Ordering is dims-first: foreign-key parents are scanned — and their
/// key → subject index built — before the children that reference them; cyclic
/// and self-referential parents are fully pre-indexed in a first pass. A parent
/// (dimension) table that is only ever a parent is therefore scanned twice
/// (once to index, once to emit) only when it is cyclic/self-referential;
/// otherwise it is scanned once and indexed lazily during its own emit pass.
///
/// `scan_table` is called with `as_of_t = None`; the Iceberg provider pins the
/// snapshot latest-at-first-touch, which — held across a whole build via one
/// shared provider — gives the per-table build watermark (DEC-003 §C-4). The
/// explicit build-scoped pin and watermark capture land in a later chunk.
pub async fn materialize_graph<P>(
    provider: &P,
    graph_source_id: &str,
    observer: &mut dyn TripleObserver,
) -> Result<MaterializeStats, MaterializeError>
where
    P: R2rmlProvider + R2rmlTableProvider,
{
    let mapping = provider.compiled_mapping(graph_source_id, None).await?;
    let mut parents = ParentIndexSet::new(&mapping)?;
    let materialization = plan(&mapping);

    // Pass 1 — pre-index parents that cannot be indexed lazily (cyclic /
    // self-referential). Scan for the index only; no triples emitted yet.
    for tm_iri in &materialization.preindex {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let mut stream = provider
            .scan_table(graph_source_id, table, &projection, &[], None, None)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            parents.index_batch(tm, &batch)?;
        }
    }

    // Pass 2 — emit in dims-first order; lazily index a parent during its own
    // emit pass unless it was pre-indexed above.
    let mut stats = MaterializeStats::default();
    for tm_iri in &materialization.emit_order {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let lazy_index =
            !materialization.preindex.contains(tm_iri) && parents.is_parent(tm_iri);
        let mut stream = provider
            .scan_table(graph_source_id, table, &projection, &[], None, None)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if lazy_index {
                parents.index_batch(tm, &batch)?;
            }
            emit_batch(tm, &batch, &parents, observer, &mut stats)?;
        }
    }
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Ingestion adapter: materialized triples → ImportSink (native bulk pipeline)
// ---------------------------------------------------------------------------

/// A [`TripleObserver`] that streams materialized triples into an [`ImportSink`],
/// interning terms and encoding flakes exactly as the Turtle / JSON-LD import
/// path does. This is the bridge from the whole-graph enumerator to the native
/// bulk ingestion pipeline.
///
/// Term, datatype, and language fidelity is preserved by routing every literal
/// through the SAME `ImportSink::term_literal` the parsers use: it parses the
/// lexical value into the correct typed `FlakeValue` from the datatype IRI (see
/// `convert_string_literal` in `fluree-db-transact`), so a materialized
/// `"9.99"^^xsd:decimal` interns to a decimal flake and a `"hola"@es` to a
/// lang-tagged string — identical to the same literal arriving from Turtle.
///
/// The caller owns the sink's lifecycle (construction, optional spool context,
/// and `finish()`), so one observer drives one chunk's worth of triples into one
/// commit. Any encoding failure is captured inside the sink and surfaced at
/// `finish()`, matching the parser→sink contract.
pub struct ImportSinkObserver<'a, 'ns> {
    sink: &'a mut ImportSink<'ns>,
}

impl<'a, 'ns> ImportSinkObserver<'a, 'ns> {
    /// Wrap a mutable [`ImportSink`].
    pub fn new(sink: &'a mut ImportSink<'ns>) -> Self {
        Self { sink }
    }
}

/// Resolve a literal's datatype IRI and optional language tag for
/// `ImportSink::term_literal`. This is the one place literal fidelity could
/// silently narrow — lang tag vs explicit datatype vs the implicit
/// `xsd:string` — so it is factored out and unit-tested. A `LangTag` reports
/// its datatype as `rdf:langString`; an `Explicit` reports its own IRI; the
/// absence of a constraint is a plain `xsd:string`.
fn literal_sink_args(dtc: Option<&UnresolvedDatatypeConstraint>) -> (&str, Option<&str>) {
    match dtc {
        Some(c) => (c.datatype_iri(), c.lang_tag()),
        None => (fluree_vocab::xsd::STRING, None),
    }
}

/// Intern one materialized RDF term into the sink, returning its `TermId`.
fn intern_term(sink: &mut ImportSink, term: &RdfTerm) -> TermId {
    match term {
        RdfTerm::Iri(iri) => sink.term_iri(iri),
        RdfTerm::BlankNode(label) => sink.term_blank(Some(label.as_str())),
        RdfTerm::Literal { value, dtc } => {
            let (dt_iri, lang) = literal_sink_args(dtc.as_ref());
            sink.term_literal(value, Datatype::from_iri(dt_iri), lang)
        }
    }
}

impl TripleObserver for ImportSinkObserver<'_, '_> {
    fn observe(
        &mut self,
        subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> Result<(), R2rmlError> {
        let s = intern_term(self.sink, subject);
        let p = self.sink.term_iri(predicate);
        let o = intern_term(self.sink, object);
        self.sink.emit_triple(s, p, o);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Chunk production: materialized triples → ParsedChunk (native import chunks)
// ---------------------------------------------------------------------------

/// A source the bulk builder can drive: it can both compile the R2RML mapping
/// and scan its tables. `Send + Sync` so it can be handed to the materializer's
/// producer thread and into the multi-threaded import pipeline. Blanket-impl'd
/// for any type that is both provider halves — no one implements it directly.
pub trait R2rmlBuildProvider: R2rmlProvider + R2rmlTableProvider + Send + Sync {}
impl<T: R2rmlProvider + R2rmlTableProvider + Send + Sync> R2rmlBuildProvider for T {}

/// Compile-time proof (machine-safety rider #3, the ?Send/Send trait-duality
/// trap): a shared build provider must be `Send + Sync` so it can cross the
/// producer thread boundary; assert it at the type level here rather than
/// discover it at monomorphization in a downstream crate.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn R2rmlBuildProvider>();
    assert_send_sync::<Arc<dyn R2rmlBuildProvider>>();
};

/// One materialized triple, owned so it can be buffered across the async scan
/// boundary before being encoded into a chunk.
type OwnedTriple = (RdfTerm, String, RdfTerm);

/// The shared allocators and directories a virtual chunk needs to encode into a
/// [`ParsedChunk`], mirroring the setup the Turtle parse path uses.
pub struct VirtualChunkContext<'a> {
    /// Shared namespace allocator (one per import; codes are globally unique).
    pub shared_alloc: &'a Arc<SharedNamespaceAllocator>,
    /// Ledger id, for blank-node skolemization txn ids.
    pub ledger_id: &'a str,
    /// Whether to zstd-compress the encoded ops stream.
    pub compress: bool,
    /// Directory for per-chunk spool sidecars.
    pub spool_dir: &'a Path,
    /// Spool allocators — `Some` when the pipeline builds an index (the normal
    /// case; the spool feeds the sorted-run merge). `None` skips the spool
    /// (used by encode-only unit tests).
    pub spool_config: Option<&'a SpoolConfig>,
}

/// Encode a buffer of materialized triples into a [`ParsedChunk`], mirroring
/// `fluree_db_transact::import::parse_chunk` exactly but replaying interned
/// triples (via [`ImportSinkObserver`]) instead of parsing Turtle text. `t` is
/// the caller-assigned transaction number (`chunk_idx + 1`), matching the parse
/// path's contract.
pub fn build_virtual_chunk(
    triples: &[OwnedTriple],
    ctx: &VirtualChunkContext,
    t: i64,
    chunk_idx: usize,
) -> Result<ParsedChunk, MaterializeError> {
    let txn_id = format!("{}-{}", ctx.ledger_id, t);
    let mut worker_cache = WorkerCache::new(Arc::clone(ctx.shared_alloc));
    let mut sink = ImportSink::new_cached(&mut worker_cache, t, txn_id, ctx.compress)
        .map_err(|e| R2rmlError::Materialization(format!("import sink create: {e}")))?;

    if let Some(config) = ctx.spool_config {
        let spool_path = ctx.spool_dir.join(format!("chunk_{chunk_idx}.spool"));
        let spool_ctx = SpoolContext::new(spool_path, chunk_idx, 0, config)
            .map_err(|e| R2rmlError::Materialization(format!("spool create: {e}")))?;
        sink.set_spool_context(spool_ctx);
    }

    {
        let mut observer = ImportSinkObserver::new(&mut sink);
        for (s, p, o) in triples {
            observer.observe(s, p, o)?;
        }
    }

    let (writer, prefix_map, spool_ctx) = sink
        .finish()
        .map_err(|e| R2rmlError::Materialization(format!("flake encode: {e}")))?;
    let op_count = writer.op_count();
    let new_codes = worker_cache.into_new_codes();
    let spool_result = spool_ctx.map(SpoolContext::finish_buffered);

    Ok(ParsedChunk {
        writer,
        op_count,
        new_codes,
        prefix_map,
        spool_result,
    })
}

/// Estimated encoded weight of one triple, for byte-budgeted chunk sizing.
fn triple_weight(subject: &RdfTerm, predicate: &str, object: &RdfTerm) -> usize {
    fn term_weight(t: &RdfTerm) -> usize {
        match t {
            RdfTerm::Iri(s) | RdfTerm::BlankNode(s) => s.len(),
            RdfTerm::Literal { value, .. } => value.len() + 8,
        }
    }
    term_weight(subject) + predicate.len() + term_weight(object) + 16
}

/// A [`TripleObserver`] that buffers materialized triples and cuts them into
/// byte-budgeted chunks. Completed chunks queue up; the driver drains the queue
/// and encodes each. Sizing is by BYTES derived from the memory budget (NOT a
/// fixed row count) — the machine-safety directive's chunk-size knob.
struct ChunkingObserver {
    buffer: Vec<OwnedTriple>,
    buffer_bytes: usize,
    threshold_bytes: usize,
    completed: VecDeque<Vec<OwnedTriple>>,
}

impl ChunkingObserver {
    fn new(threshold_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            buffer_bytes: 0,
            threshold_bytes: threshold_bytes.max(1),
            completed: VecDeque::new(),
        }
    }

    /// The final, sub-threshold buffer, if any (flushed after the last batch).
    fn take_final(&mut self) -> Option<Vec<OwnedTriple>> {
        if self.buffer.is_empty() {
            None
        } else {
            self.buffer_bytes = 0;
            Some(std::mem::take(&mut self.buffer))
        }
    }
}

impl TripleObserver for ChunkingObserver {
    fn observe(&mut self, subject: &RdfTerm, predicate: &str, object: &RdfTerm) -> Result<(), R2rmlError> {
        self.buffer_bytes += triple_weight(subject, predicate, object);
        self.buffer
            .push((subject.clone(), predicate.to_string(), object.clone()));
        if self.buffer_bytes >= self.threshold_bytes {
            self.completed.push_back(std::mem::take(&mut self.buffer));
            self.buffer_bytes = 0;
        }
        Ok(())
    }
}

/// Stream a virtual R2RML source through the whole-graph enumerator, encoding
/// byte-budgeted [`ParsedChunk`]s and handing each to `emit_chunk(idx, chunk)`
/// in contiguous `idx` order (`t = idx + 1`). `emit_chunk` returns `false` when
/// the downstream consumer has gone away, ending the drive early. Dims-first,
/// with the same pre-index pass as the in-memory driver; the chunk byte budget
/// (`chunk_size_bytes`) derives from the import memory budget.
pub async fn drive_virtual_import<F>(
    provider: &dyn R2rmlBuildProvider,
    graph_source_id: &str,
    chunk_size_bytes: usize,
    ctx: &VirtualChunkContext<'_>,
    mut emit_chunk: F,
) -> Result<MaterializeStats, MaterializeError>
where
    F: FnMut(usize, ParsedChunk) -> bool,
{
    let mapping = provider.compiled_mapping(graph_source_id, None).await?;
    let mut parents = ParentIndexSet::new(&mapping)?;
    let materialization = plan(&mapping);

    // Pass 1 — pre-index cyclic / self-referential parents (no triples emitted).
    for tm_iri in &materialization.preindex {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let mut stream = provider
            .scan_table(graph_source_id, table, &projection, &[], None, None)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            parents.index_batch(tm, &batch)?;
        }
    }

    // Pass 2 — emit into the byte-budgeted chunker; encode + ship completed
    // chunks as they close.
    let mut stats = MaterializeStats::default();
    let mut chunker = ChunkingObserver::new(chunk_size_bytes);
    let mut next_idx = 0usize;

    for tm_iri in &materialization.emit_order {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let lazy_index =
            !materialization.preindex.contains(tm_iri) && parents.is_parent(tm_iri);
        let mut stream = provider
            .scan_table(graph_source_id, table, &projection, &[], None, None)
            .await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if lazy_index {
                parents.index_batch(tm, &batch)?;
            }
            emit_batch(tm, &batch, &parents, &mut chunker, &mut stats)?;
            while let Some(triples) = chunker.completed.pop_front() {
                let t = (next_idx + 1) as i64;
                let parsed = build_virtual_chunk(&triples, ctx, t, next_idx)?;
                if !emit_chunk(next_idx, parsed) {
                    return Ok(stats);
                }
                next_idx += 1;
            }
        }
    }

    // Final partial chunk.
    if let Some(triples) = chunker.take_final() {
        let t = (next_idx + 1) as i64;
        let parsed = build_virtual_chunk(&triples, ctx, t, next_idx)?;
        emit_chunk(next_idx, parsed);
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::literal_sink_args;
    use fluree_vocab::UnresolvedDatatypeConstraint as Dtc;

    #[test]
    fn plain_literal_is_xsd_string_no_lang() {
        assert_eq!(literal_sink_args(None), (fluree_vocab::xsd::STRING, None));
    }

    #[test]
    fn lang_tagged_literal_uses_langstring_and_tag() {
        let dtc = Dtc::LangTag("es".into());
        assert_eq!(
            literal_sink_args(Some(&dtc)),
            (fluree_vocab::rdf::LANG_STRING, Some("es")),
            "a language tag must intern as rdf:langString + the tag"
        );
    }

    #[test]
    fn explicit_datatype_passes_through_without_lang() {
        let dtc = Dtc::Explicit(fluree_vocab::xsd::DECIMAL.into());
        assert_eq!(
            literal_sink_args(Some(&dtc)),
            (fluree_vocab::xsd::DECIMAL, None),
            "an explicit datatype must reach term_literal so the value types correctly"
        );
    }

    #[test]
    fn chunker_cuts_by_byte_budget_and_conserves_triples() {
        use super::ChunkingObserver;
        use fluree_db_r2rml::materialize::TripleObserver;
        use fluree_db_r2rml::RdfTerm;

        // A tiny byte budget forces multiple chunk cuts.
        let mut chunker = ChunkingObserver::new(40);
        let s = RdfTerm::iri("http://ex.org/subject");
        let o = RdfTerm::iri("http://ex.org/object");
        for _ in 0..5 {
            chunker.observe(&s, "http://ex.org/p", &o).unwrap();
        }
        assert!(
            !chunker.completed.is_empty(),
            "a tiny byte budget must cut at least one chunk"
        );
        let completed: usize = chunker.completed.iter().map(Vec::len).sum();
        let remainder = chunker.take_final().map(|v| v.len()).unwrap_or(0);
        assert_eq!(
            completed + remainder,
            5,
            "every observed triple must land in exactly one chunk (completed or final)"
        );
    }
}
