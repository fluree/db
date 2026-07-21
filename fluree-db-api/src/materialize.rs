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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;

use futures::StreamExt;
use sha2::{Digest, Sha256};

use fluree_db_core::{TxnMetaEntry, TxnMetaValue};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider, TableWatermark};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, TriplesMap};
use fluree_db_r2rml::materialize::{
    emit_batch, plan, MaterializeStats, ParentIndexSet, TripleObserver,
};
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
    /// The provider reported an empty build watermark for a non-empty table set
    /// (DEC-003 §17(b)). No table snapshots were captured, so the twin cannot be
    /// stamped — a fail-loud guard against publishing an unverifiable twin.
    #[error(
        "materialize watermark is empty for a non-empty table set; the twin cannot be stamped \
         (no table snapshots were captured — did the provider scan the tables?)"
    )]
    EmptyWatermark,
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
        let lazy_index = !materialization.preindex.contains(tm_iri) && parents.is_parent(tm_iri);
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
///
/// `stamp` is `Some` for exactly ONE chunk — the twin's FINAL commit — and its
/// completion stamp is ns-encoded into this chunk's `txn_meta` through the SAME
/// sink, so the stamp predicates' namespace codes publish in this commit's
/// namespace_delta. Every other chunk passes `None`, leaving `txn_meta` empty
/// (byte-identical to a text-import chunk).
pub fn build_virtual_chunk(
    triples: &[OwnedTriple],
    ctx: &VirtualChunkContext,
    t: i64,
    chunk_idx: usize,
    stamp: Option<&WatermarkStamp>,
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

    // Encode the completion stamp into THIS chunk's sink before finishing, so the
    // stamp predicate namespace codes land in `new_codes` (→ namespace_delta).
    let txn_meta = match stamp {
        Some(s) => encode_stamp(&mut sink, s)?,
        None => Vec::new(),
    };

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
        txn_meta,
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
    fn observe(
        &mut self,
        subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> Result<(), R2rmlError> {
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

// ---------------------------------------------------------------------------
// Completion stamp: pin-all pre-pass + watermark + mapping hash + builder version
// ---------------------------------------------------------------------------

/// Builder identity stamped into every twin, so a reader can tell which builder
/// produced it (and an incompatible builder's twin can be refused or annotated).
const BUILDER_VERSION: &str = concat!("fluree-materialize/", env!("CARGO_PKG_VERSION"));

/// The completion stamp written to a twin's FINAL commit (DEC-003 §17). A twin is
/// valid iff a head-walk finds this stamp; a build that dies mid-way leaves the
/// head commit unstamped, so a partial twin is detectable. Carries the per-table
/// snapshot watermark vector (delta-sync reads it), the R2RML mapping hash (a
/// mapping change invalidates the twin), and the builder version.
#[derive(Debug, Clone)]
pub struct WatermarkStamp {
    /// The builder that produced the twin ([`BUILDER_VERSION`]).
    pub builder_version: String,
    /// SHA-256 of the R2RML mapping ([`mapping_hash`]).
    pub mapping_hash: String,
    /// Per-table pinned Iceberg snapshot at build time.
    pub tables: HashMap<String, TableWatermark>,
}

/// SHA-256 (hex) of a canonical serialization of the mapping's TriplesMaps —
/// sorted by IRI, each serialized to JSON with a length prefix. Deterministic
/// because the R2RML mapping types carry no `HashMap` (only the compiled
/// mapping's derived indexes do, and those are excluded): an unchanged mapping
/// hashes identically across builds/platforms, and any edit to a subject /
/// predicate / object map changes the hash, invalidating the twin.
pub fn mapping_hash(mapping: &CompiledR2rmlMapping) -> String {
    let mut sorted: Vec<(&String, &TriplesMap)> = mapping.triples_maps.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let mut hasher = Sha256::new();
    for (iri, tm) in sorted {
        hasher.update(iri.as_bytes());
        hasher.update([0u8]);
        // Canonical: TriplesMap has no HashMap field, so JSON field/element order
        // is stable. Serialization of these plain-data types is infallible.
        let json = serde_json::to_vec(tm).unwrap_or_default();
        hasher.update((json.len() as u64).to_le_bytes());
        hasher.update(&json);
    }
    format!("{:x}", hasher.finalize())
}

/// Fail-loud guard (DEC-003 §17(b)): a mapping with at least one table must
/// yield a non-empty watermark, or the twin cannot be stamped. An empty mapping
/// (no tables) legitimately has an empty watermark.
fn require_nonempty_watermark(
    tables: &HashMap<String, TableWatermark>,
    mapping: &CompiledR2rmlMapping,
) -> Result<(), MaterializeError> {
    let has_tables = mapping
        .triples_maps
        .values()
        .any(|tm| tm.table_name().is_some());
    if has_tables && tables.is_empty() {
        return Err(MaterializeError::EmptyWatermark);
    }
    Ok(())
}

/// Assemble the twin's completion stamp from the provider's captured watermark,
/// the mapping hash, and the builder version. Fails loud on an empty watermark
/// for a non-empty table set.
fn build_watermark_stamp(
    provider: &dyn R2rmlBuildProvider,
    graph_source_id: &str,
    mapping: &CompiledR2rmlMapping,
) -> Result<WatermarkStamp, MaterializeError> {
    let tables = provider.build_watermark(graph_source_id);
    require_nonempty_watermark(&tables, mapping)?;
    Ok(WatermarkStamp {
        builder_version: BUILDER_VERSION.to_string(),
        mapping_hash: mapping_hash(mapping),
        tables,
    })
}

/// Namespace of the twin's completion-stamp predicates.
const MATERIALIZE_NS: &str = "https://ns.flur.ee/materialize#";
/// Local names of the three stamp predicates (namespaced by [`MATERIALIZE_NS`]).
const STAMP_PRED_BUILDER: &str = "builderVersion";
const STAMP_PRED_MAPPING_HASH: &str = "mappingHash";
const STAMP_PRED_WATERMARK: &str = "watermark";

/// ns-encode the completion stamp into `txn_meta` entries, interning each
/// predicate through `sink` so its namespace code is published in this chunk's
/// namespace_delta (and thus resolves when the commit is read back). The
/// watermark vector is stored as one deterministic JSON string (sorted by table)
/// under `materialize:watermark`. The whole stamp must fit the 64 KiB txn_meta
/// cap; a schema with a very large table count would need a compacter encoding
/// (documented residual).
fn encode_stamp(
    sink: &mut ImportSink,
    stamp: &WatermarkStamp,
) -> Result<Vec<TxnMetaEntry>, MaterializeError> {
    let watermark_json = {
        let sorted: BTreeMap<&str, &TableWatermark> =
            stamp.tables.iter().map(|(k, v)| (k.as_str(), v)).collect();
        serde_json::to_string(&sorted)
            .map_err(|e| R2rmlError::Materialization(format!("watermark encode: {e}")))?
    };
    let fields = [
        (STAMP_PRED_BUILDER, stamp.builder_version.clone()),
        (STAMP_PRED_MAPPING_HASH, stamp.mapping_hash.clone()),
        (STAMP_PRED_WATERMARK, watermark_json),
    ];
    let mut entries = Vec::with_capacity(fields.len());
    for (local, value) in fields {
        let (ns, name) = sink.intern_meta_predicate(&format!("{MATERIALIZE_NS}{local}"));
        entries.push(TxnMetaEntry::new(ns, name, TxnMetaValue::String(value)));
    }
    Ok(entries)
}

/// Reader-side of the completion stamp: extract it from a commit's `txn_meta`, if
/// present. A twin is valid iff a head-walk finds a commit whose txn_meta yields
/// `Some` here (all three stamp fields present). Keys on the stable predicate
/// LOCAL NAMES (the namespace code is per-ledger); the local names are
/// materialize-specific, so requiring all three together identifies the stamp.
pub fn read_stamp(txn_meta: &[TxnMetaEntry]) -> Option<WatermarkStamp> {
    let field = |local: &str| -> Option<&str> {
        txn_meta.iter().find_map(|e| match &e.value {
            TxnMetaValue::String(s) if e.predicate_name == local => Some(s.as_str()),
            _ => None,
        })
    };
    let builder_version = field(STAMP_PRED_BUILDER)?.to_string();
    let mapping_hash = field(STAMP_PRED_MAPPING_HASH)?.to_string();
    let tables: HashMap<String, TableWatermark> =
        serde_json::from_str(field(STAMP_PRED_WATERMARK)?).ok()?;
    Some(WatermarkStamp {
        builder_version,
        mapping_hash,
        tables,
    })
}

/// Pin every table's Iceberg snapshot up front, before emission (DEC-003 §17(a)).
/// Awaiting `scan_table` runs the `loadTable` + snapshot capture as the table
/// context resolves; the returned stream is dropped unpolled, so no Parquet data
/// is read. This pins all snapshots within seconds — narrowing cross-table
/// snapshot skew from the whole build duration to this window — and the emit-pass
/// scans reuse the pins (session first-writer-wins), so it adds no duplicate
/// `loadTable` GETs.
async fn pin_all_tables(
    provider: &dyn R2rmlBuildProvider,
    graph_source_id: &str,
    mapping: &CompiledR2rmlMapping,
) -> Result<(), MaterializeError> {
    let mut tables: Vec<&str> = mapping
        .triples_maps
        .values()
        .filter_map(|tm| tm.table_name())
        .collect();
    tables.sort_unstable();
    tables.dedup();
    for table in tables {
        let _ = provider
            .scan_table(graph_source_id, table, &[], &[], None, None)
            .await?;
    }
    Ok(())
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

    // Pin-all pre-pass — pin every table's snapshot before emission so the
    // watermark is complete and cross-table skew is bounded to seconds (§17(a)).
    pin_all_tables(provider, graph_source_id, &mapping).await?;

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
    // chunks as they close. A one-chunk lookahead (`pending`) holds the most
    // recently closed group back, so the LAST group can carry the completion
    // stamp — the stamp lands on the final commit without an extra 0-op commit.
    let mut stats = MaterializeStats::default();
    let mut chunker = ChunkingObserver::new(chunk_size_bytes);
    let mut next_idx = 0usize;
    let mut pending: Option<Vec<OwnedTriple>> = None;

    for tm_iri in &materialization.emit_order {
        let Some(tm) = mapping.triples_maps.get(tm_iri) else {
            continue;
        };
        let table = tm
            .table_name()
            .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
        let projection = scan_projection(tm, &parents);
        let lazy_index = !materialization.preindex.contains(tm_iri) && parents.is_parent(tm_iri);
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
                // Hold the newest group; flush the previously held group (if any)
                // as a non-final chunk, keeping the last group for the stamp.
                if let Some(prev) = pending.replace(triples) {
                    let t = (next_idx + 1) as i64;
                    let parsed = build_virtual_chunk(&prev, ctx, t, next_idx, None)?;
                    if !emit_chunk(next_idx, parsed) {
                        return Ok(stats);
                    }
                    next_idx += 1;
                }
            }
        }
    }

    // Assemble the twin's completion stamp now that every table is pinned and
    // scanned (fail-loud on an empty watermark for a non-empty table set). Chunk D
    // persists it into the FINAL commit's txn_meta.
    let stamp = build_watermark_stamp(provider, graph_source_id, &mapping)?;
    tracing::info!(
        builder_version = %stamp.builder_version,
        mapping_hash = %stamp.mapping_hash,
        watermark_tables = stamp.tables.len(),
        "materialize completion stamp assembled"
    );

    // Fold the trailing sub-threshold buffer into the lookahead as the final
    // group (so it, not a held earlier group, carries the stamp).
    if let Some(triples) = chunker.take_final() {
        if let Some(prev) = pending.replace(triples) {
            let t = (next_idx + 1) as i64;
            let parsed = build_virtual_chunk(&prev, ctx, t, next_idx, None)?;
            if !emit_chunk(next_idx, parsed) {
                return Ok(stats);
            }
            next_idx += 1;
        }
    }

    // FINAL commit — carries the completion stamp. Normally the last held group;
    // an empty stamp-only commit only for a graph that produced no triples. Being
    // the last commit, a head-walk finds the stamp iff the build completed (§17(a)).
    let final_triples = pending.unwrap_or_default();
    let t = (next_idx + 1) as i64;
    let parsed = build_virtual_chunk(&final_triples, ctx, t, next_idx, Some(&stamp))?;
    emit_chunk(next_idx, parsed);

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::literal_sink_args;
    use fluree_vocab::UnresolvedDatatypeConstraint as Dtc;

    #[test]
    fn mapping_hash_is_deterministic_and_change_sensitive() {
        use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, TriplesMap};
        let m1 = CompiledR2rmlMapping::new(vec![
            TriplesMap::new("http://tm/a", "dw.a"),
            TriplesMap::new("http://tm/b", "dw.b"),
        ]);
        // Same content, reversed insertion order — hashes identically (the hash
        // sorts by IRI, so it is insertion-order-independent).
        let m2 = CompiledR2rmlMapping::new(vec![
            TriplesMap::new("http://tm/b", "dw.b"),
            TriplesMap::new("http://tm/a", "dw.a"),
        ]);
        assert_eq!(
            super::mapping_hash(&m1),
            super::mapping_hash(&m2),
            "the mapping hash must be canonical (order-independent)"
        );
        // A changed subject template must change the hash (twin invalidation).
        let m3 = CompiledR2rmlMapping::new(vec![
            TriplesMap::new("http://tm/a", "dw.a").with_subject_template("http://ex/{k}"),
            TriplesMap::new("http://tm/b", "dw.b"),
        ]);
        assert_ne!(
            super::mapping_hash(&m1),
            super::mapping_hash(&m3),
            "any mapping edit must change the hash"
        );
    }

    #[test]
    fn watermark_guard_rejects_empty_for_nonempty_tables() {
        use fluree_db_query::r2rml::TableWatermark;
        use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, TriplesMap};
        use std::collections::HashMap;

        let mapping = CompiledR2rmlMapping::new(vec![TriplesMap::new("http://tm/a", "dw.a")]);
        let empty: HashMap<String, TableWatermark> = HashMap::new();
        assert!(
            matches!(
                super::require_nonempty_watermark(&empty, &mapping),
                Err(super::MaterializeError::EmptyWatermark)
            ),
            "a table-bearing mapping with no captured snapshots must fail loud"
        );

        let mut nonempty = HashMap::new();
        nonempty.insert(
            "dw.a".to_string(),
            TableWatermark {
                metadata_location: "s3://m.json".into(),
                snapshot_id: Some(1),
                sequence_number: Some(1),
            },
        );
        assert!(
            super::require_nonempty_watermark(&nonempty, &mapping).is_ok(),
            "a captured watermark passes the guard"
        );

        // An empty mapping (no tables) legitimately has an empty watermark.
        let empty_mapping = CompiledR2rmlMapping::default();
        assert!(
            super::require_nonempty_watermark(&empty, &empty_mapping).is_ok(),
            "no tables → an empty watermark is fine"
        );
    }

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
