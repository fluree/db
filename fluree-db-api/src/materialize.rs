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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use sha2::{Digest, Sha256};

use fluree_db_core::{TxnMetaEntry, TxnMetaValue, MAX_TXN_META_BYTES};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider, TableWatermark};
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, TriplesMap};
use fluree_db_r2rml::materialize::{
    emit_batch, plan, render_term, MaterializeStats, ParentIndexSet, TripleObserver,
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
    /// The resident foreign-key parent index outgrew its share of the import
    /// memory budget (O6). Fail loud rather than let an unbudgeted structure OOM a
    /// bounded box — the previous behavior. Raise `--memory-budget-mb` or reduce
    /// the FK-parent (dimension) key cardinality.
    #[error(
        "foreign-key parent index needs ~{estimated_mb} MB but the import memory budget allows \
         ~{budget_mb} MB for it; raise --memory-budget-mb or reduce the FK-parent (dimension) \
         key cardinality"
    )]
    ParentIndexBudgetExceeded {
        estimated_mb: usize,
        budget_mb: usize,
    },
    /// The completion stamp's encoded txn-meta would exceed the commit envelope's
    /// [`MAX_TXN_META_BYTES`] (64 KiB) cap. Driven by the table count — each table
    /// contributes its `metadata_location` (S3 URIs + JSON framing run ~350 bytes
    /// per table) to the one watermark JSON string. `encode_stamp` builds
    /// `TxnMetaEntry`s directly, bypassing the TriG/JSON-LD txn-meta validators, so
    /// the cap is enforced there instead — fail loud BEFORE finalize, consistent
    /// with [`EmptyWatermark`]/[`ParentIndexBudgetExceeded`], rather than let an
    /// oversize stamp fail opaquely at commit-encode time. A schema this wide needs
    /// a compacter watermark encoding (the documented residual in `encode_stamp`).
    #[error(
        "materialize completion stamp is ~{actual_bytes} bytes but the commit txn-meta cap is \
         {cap_bytes} bytes ({table_count} tables in the watermark); the twin cannot be stamped — \
         reduce the source table count or shorten table metadata_location URIs"
    )]
    StampTooLarge {
        actual_bytes: usize,
        cap_bytes: usize,
        table_count: usize,
    },
    /// A parent join key resolved to more than one distinct parent subject (a
    /// fan-out this builder does not yet emit). The twin builder REFUSES this by
    /// default: last-wins would bake ONE arbitrary parent per key and silently drop
    /// the others, and — before the deterministic keep-min tie-break — which one was
    /// even kept raced the scan order, breaking the stamp's reproducibility contract.
    /// Overridable with `--allow-duplicate-parent-keys` (the twin then records the
    /// anomaly in its stamp). The true R2RML RefObjectMap fan-out is a tracked
    /// follow-up; once it lands, re-materialization heals existing twins.
    #[error(
        "duplicate parent join keys detected — {summary}. The twin builder refuses these by \
         default (a key mapping to multiple parents makes the foreign-key edge target ambiguous; \
         only one parent is kept — deterministically the lexicographically smallest — and the \
         rest are dropped, an R2RML RefObjectMap fan-out this builder does not yet emit). Re-run \
         with --allow-duplicate-parent-keys to build anyway; the twin's stamp will record the \
         anomaly."
    )]
    DuplicateParentKeys { summary: String },
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

/// A [`TripleObserver`] that interns each materialized triple DIRECTLY into an
/// open [`ImportSink`] as the enumerator emits it (O3a) — no intermediate
/// owned-triple buffer and no second intern pass — while accumulating a byte
/// estimate the producer worker uses to cut byte-budgeted chunks at batch
/// boundaries. This replaces the buffer-then-re-intern `ChunkingObserver` on the
/// hot produce path; a term is now allocated by the enumerator and interned into
/// the sink dictionary exactly once.
struct InterningObserver<'a, 'ns> {
    sink: &'a mut ImportSink<'ns>,
    bytes: &'a mut usize,
}

impl TripleObserver for InterningObserver<'_, '_> {
    fn observe(
        &mut self,
        subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> Result<(), R2rmlError> {
        let s = intern_term(self.sink, subject);
        let p = self.sink.term_iri(predicate);
        let o = intern_term(self.sink, object);
        self.sink
            .emit_triple(s, p, o)
            .map_err(|e| R2rmlError::Materialization(format!("flake encode: {e}")))?;
        *self.bytes += triple_weight(subject, predicate, object);
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

/// Encode the twin's FINAL, stamp-only [`ParsedChunk`], mirroring
/// `fluree_db_transact::import::parse_chunk`'s finalize but carrying NO data
/// triples — only the completion stamp. `t` is the caller-assigned transaction
/// number (`chunk_idx + 1`), matching the parse path's contract.
///
/// This is the sole surviving caller of [`encode_stamp`] (O3a removed the
/// buffer-then-re-intern data path; data chunks now intern directly through
/// [`InterningObserver`] in the produce workers). The stamp is ns-encoded into
/// this chunk's `txn_meta` through the SAME sink, so the stamp predicates'
/// namespace codes publish in this commit's `namespace_delta` and resolve when
/// the commit is read back (see [`read_stamp`]). The chunk holds zero flakes —
/// the already-supported empty-stamp shape — so it always rides the twin's head.
pub fn build_stamp_chunk(
    ctx: &VirtualChunkContext,
    t: i64,
    chunk_idx: usize,
    stamp: &WatermarkStamp,
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

    // Encode the completion stamp into THIS chunk's sink before finishing, so the
    // stamp predicate namespace codes land in `new_codes` (→ namespace_delta).
    let txn_meta = encode_stamp(&mut sink, stamp)?;

    let (writer, prefix_map, spool_ctx) = sink
        .into_parts()
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
    /// Seed for the parity gate's stratified sample. Recorded so the sampled
    /// subjects are auditable and reproducible (and so a later verify can widen
    /// coverage by rotating it). Derived deterministically from `mapping_hash`.
    pub sample_seed: u64,
    /// Ambiguous parent join-key counts per parent TriplesMap IRI, recorded ONLY
    /// when the twin was built with `--allow-duplicate-parent-keys` over a source
    /// that has them (otherwise empty, and the stamp is byte-identical to a clean
    /// twin's). An overridden twin thus self-documents the fan-out anomaly it baked:
    /// each ambiguous key kept ONE deterministically-chosen parent and dropped the
    /// rest (the R2RML RefObjectMap fan-out is a tracked follow-up).
    pub dup_parent_keys: HashMap<String, u64>,
}

/// Derive the stratified-sample seed from a mapping hash (its leading 64 bits).
/// Deterministic, so the build's own post-build gate and any independent verify
/// select the SAME sample without reading the stamp back.
fn sample_seed_from_hash(mapping_hash: &str) -> u64 {
    u64::from_str_radix(mapping_hash.get(..16).unwrap_or("0"), 16).unwrap_or(0)
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
/// for a non-empty table set. `dup_parent_keys` is non-empty only when the build
/// ran with the duplicate-parent-key override — it self-documents the fan-out
/// anomaly the twin baked.
fn build_watermark_stamp(
    provider: &dyn R2rmlBuildProvider,
    graph_source_id: &str,
    mapping: &CompiledR2rmlMapping,
    dup_parent_keys: HashMap<String, u64>,
) -> Result<WatermarkStamp, MaterializeError> {
    let tables = provider.build_watermark(graph_source_id);
    require_nonempty_watermark(&tables, mapping)?;
    let hash = mapping_hash(mapping);
    let sample_seed = sample_seed_from_hash(&hash);
    Ok(WatermarkStamp {
        builder_version: BUILDER_VERSION.to_string(),
        mapping_hash: hash,
        tables,
        sample_seed,
        dup_parent_keys,
    })
}

/// One-clause-per-parent summary of the duplicate-parent-key anomaly for the decline
/// error: the parent table, its join column(s), and the ambiguous-key count. Sorted
/// so the message is deterministic.
fn format_dup_parent_keys(
    dups: &HashMap<String, u64>,
    parents: &ParentIndexSet,
    mapping: &CompiledR2rmlMapping,
) -> String {
    let mut clauses: Vec<String> = dups
        .iter()
        .map(|(tm_iri, count)| {
            let table = mapping
                .triples_maps
                .get(tm_iri)
                .and_then(|tm| tm.table_name())
                .unwrap_or(tm_iri.as_str());
            let cols = parents
                .needed_columns(tm_iri)
                .map(|sets| {
                    let mut cs: Vec<String> = sets
                        .iter()
                        .map(|set| format!("[{}]", set.join(", ")))
                        .collect();
                    cs.sort();
                    cs.join(" / ")
                })
                .unwrap_or_default();
            format!("table '{table}' (join column(s): {cols}): {count} ambiguous key(s)")
        })
        .collect();
    clauses.sort();
    clauses.join("; ")
}

/// Namespace of the twin's completion-stamp predicates.
const MATERIALIZE_NS: &str = "https://ns.flur.ee/materialize#";
/// Local names of the three stamp predicates (namespaced by [`MATERIALIZE_NS`]).
const STAMP_PRED_BUILDER: &str = "builderVersion";
const STAMP_PRED_MAPPING_HASH: &str = "mappingHash";
const STAMP_PRED_WATERMARK: &str = "watermark";
const STAMP_PRED_SAMPLE_SEED: &str = "sampleSeed";
/// Optional stamp predicate: present ONLY when the twin was built with the
/// duplicate-parent-key override over a source that has them. Its absence keeps a
/// clean twin's stamp byte-identical to before this field existed.
const STAMP_PRED_DUP_PARENT_KEYS: &str = "dupParentKeys";

/// Deterministic JSON (sorted by parent TM IRI) of the ambiguous parent-key counts,
/// or `None` when there are none (so the stamp omits the field for a clean twin).
fn dup_parent_keys_json(stamp: &WatermarkStamp) -> Result<Option<String>, MaterializeError> {
    if stamp.dup_parent_keys.is_empty() {
        return Ok(None);
    }
    let sorted: BTreeMap<&str, u64> = stamp
        .dup_parent_keys
        .iter()
        .map(|(k, v)| (k.as_str(), *v))
        .collect();
    serde_json::to_string(&sorted)
        .map(Some)
        .map_err(|e| R2rmlError::Materialization(format!("dup_parent_keys encode: {e}")).into())
}

/// The deterministic watermark JSON string (sorted by table) stored under
/// `materialize:watermark`. Its length is the dominant term in the stamp's
/// txn-meta size, so [`stamp_watermark_within_cap`] computes it once and reuses
/// the result here.
fn watermark_json(stamp: &WatermarkStamp) -> Result<String, MaterializeError> {
    let sorted: BTreeMap<&str, &TableWatermark> =
        stamp.tables.iter().map(|(k, v)| (k.as_str(), v)).collect();
    serde_json::to_string(&sorted)
        .map_err(|e| R2rmlError::Materialization(format!("watermark encode: {e}")).into())
}

/// Fail-loud 64 KiB txn-meta cap enforcement for the completion stamp (item #4).
/// `encode_stamp` builds `TxnMetaEntry`s directly and hands them to
/// `finalize_parsed_chunk`, bypassing the TriG/JSON-LD txn-meta validators
/// (`parse/txn_meta.rs`, `parse/trig_meta.rs`) that enforce [`MAX_TXN_META_BYTES`]
/// on user-supplied metadata — so the cap is enforced HERE or nowhere. The
/// estimate mirrors those validators' `validate_limits` exactly (per entry:
/// 6 + name_len for the predicate, then 1 + value_size; String value_size =
/// 4 + len, Long = 8), so a stamp that passes here would also pass the parser
/// path's budget. Returns the serialized watermark JSON on success so
/// `encode_stamp` need not re-serialize. Sink-free, so it is unit-testable.
fn stamp_watermark_within_cap(stamp: &WatermarkStamp) -> Result<String, MaterializeError> {
    let watermark_json = watermark_json(stamp)?;
    let entry_bytes = |name_len: usize, value_size: usize| 6 + name_len + 1 + value_size;
    let mut estimated = entry_bytes(STAMP_PRED_BUILDER.len(), 4 + stamp.builder_version.len())
        + entry_bytes(STAMP_PRED_MAPPING_HASH.len(), 4 + stamp.mapping_hash.len())
        + entry_bytes(STAMP_PRED_WATERMARK.len(), 4 + watermark_json.len())
        + entry_bytes(STAMP_PRED_SAMPLE_SEED.len(), 8);
    if let Some(dup_json) = dup_parent_keys_json(stamp)? {
        estimated += entry_bytes(STAMP_PRED_DUP_PARENT_KEYS.len(), 4 + dup_json.len());
    }
    if estimated > MAX_TXN_META_BYTES {
        return Err(MaterializeError::StampTooLarge {
            actual_bytes: estimated,
            cap_bytes: MAX_TXN_META_BYTES,
            table_count: stamp.tables.len(),
        });
    }
    Ok(watermark_json)
}

/// ns-encode the completion stamp into `txn_meta` entries, interning each
/// predicate through `sink` so its namespace code is published in this chunk's
/// namespace_delta (and thus resolves when the commit is read back). The
/// watermark vector is stored as one deterministic JSON string (sorted by table)
/// under `materialize:watermark`. The whole stamp must fit the 64 KiB txn_meta
/// cap — enforced fail-loud by [`stamp_watermark_within_cap`] BEFORE any interning;
/// a schema with a very large table count would need a compacter encoding
/// (documented residual).
fn encode_stamp(
    sink: &mut ImportSink,
    stamp: &WatermarkStamp,
) -> Result<Vec<TxnMetaEntry>, MaterializeError> {
    let watermark_json = stamp_watermark_within_cap(stamp)?;
    let fields = [
        (STAMP_PRED_BUILDER, stamp.builder_version.clone()),
        (STAMP_PRED_MAPPING_HASH, stamp.mapping_hash.clone()),
        (STAMP_PRED_WATERMARK, watermark_json),
    ];
    let mut entries = Vec::with_capacity(fields.len() + 1);
    for (local, value) in fields {
        let (ns, name) = sink.intern_meta_predicate(&format!("{MATERIALIZE_NS}{local}"));
        entries.push(TxnMetaEntry::new(ns, name, TxnMetaValue::String(value)));
    }
    // The sample seed as an integer (auditable; a verify reproduces the sample).
    let (ns, name) =
        sink.intern_meta_predicate(&format!("{MATERIALIZE_NS}{STAMP_PRED_SAMPLE_SEED}"));
    entries.push(TxnMetaEntry::new(
        ns,
        name,
        TxnMetaValue::Long(stamp.sample_seed as i64),
    ));
    // Duplicate-parent-key anomaly, ONLY when the override built over one (else the
    // stamp is byte-identical to a clean twin's).
    if let Some(dup_json) = dup_parent_keys_json(stamp)? {
        let (ns, name) =
            sink.intern_meta_predicate(&format!("{MATERIALIZE_NS}{STAMP_PRED_DUP_PARENT_KEYS}"));
        entries.push(TxnMetaEntry::new(ns, name, TxnMetaValue::String(dup_json)));
    }
    Ok(entries)
}

/// Reader-side of the completion stamp: extract it from a commit's `txn_meta`, if
/// present. A twin is valid iff a head-walk finds a commit whose txn_meta yields
/// `Some` here (all three stamp fields present). Each stamp predicate is matched
/// on BOTH its stable local name AND its namespace: `namespace_delta` (the same
/// commit's `CodecEnvelope::namespace_delta`) must resolve the predicate's ns code
/// to [`MATERIALIZE_NS`]. The stamp predicates are freshly interned into the final
/// commit's sink (see [`encode_stamp`]/`intern_meta_predicate`), so their code is
/// published in that commit's delta and resolves here.
///
/// The namespace requirement is load-bearing (item #5): the local names alone are
/// NOT a sufficient signature — a commit carrying `builderVersion`, `mappingHash`,
/// and `watermark` string entries in ANY OTHER namespace must not be mistaken for a
/// twin stamp. `intern_meta_predicate` exists precisely to publish these codes so
/// the read side can insist on the namespace, not just the local name.
pub fn read_stamp(
    txn_meta: &[TxnMetaEntry],
    namespace_delta: &HashMap<u16, String>,
) -> Option<WatermarkStamp> {
    // `canonical_split` guarantees `prefix + local == full_iri`, and each entry is
    // matched on its local name below, so requiring the resolved prefix to equal
    // MATERIALIZE_NS is equivalent to requiring the full predicate IRI to be the
    // materialize stamp predicate.
    let in_materialize_ns = |e: &TxnMetaEntry| -> bool {
        namespace_delta
            .get(&e.predicate_ns)
            .is_some_and(|ns| ns == MATERIALIZE_NS)
    };
    let field = |local: &str| -> Option<&str> {
        txn_meta.iter().find_map(|e| match &e.value {
            TxnMetaValue::String(s) if e.predicate_name == local && in_materialize_ns(e) => {
                Some(s.as_str())
            }
            _ => None,
        })
    };
    let builder_version = field(STAMP_PRED_BUILDER)?.to_string();
    let mapping_hash = field(STAMP_PRED_MAPPING_HASH)?.to_string();
    let tables: HashMap<String, TableWatermark> =
        serde_json::from_str(field(STAMP_PRED_WATERMARK)?).ok()?;
    let sample_seed = txn_meta
        .iter()
        .find_map(|e| match &e.value {
            TxnMetaValue::Long(n)
                if e.predicate_name == STAMP_PRED_SAMPLE_SEED && in_materialize_ns(e) =>
            {
                Some(*n as u64)
            }
            _ => None,
        })
        .unwrap_or_else(|| sample_seed_from_hash(&mapping_hash));
    // Optional: present only for an override-built twin (absent ⇒ empty ⇒ clean twin).
    let dup_parent_keys: HashMap<String, u64> = field(STAMP_PRED_DUP_PARENT_KEYS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    Some(WatermarkStamp {
        builder_version,
        mapping_hash,
        tables,
        sample_seed,
        dup_parent_keys,
    })
}

/// Fraction of the import memory budget the FK parent index may occupy before the
/// build fails loud (O6). The rest of the budget carries the in-flight chunks,
/// spool buffers, and index-build working set; the parent index is resident for
/// the WHOLE build, so it gets a conservative half.
const PARENT_INDEX_BUDGET_FRACTION: f64 = 0.5;

/// The parent-index byte budget derived from the import memory budget. `0` in →
/// `0` out (guard disabled when the budget is unknown / auto).
fn parent_index_budget_bytes(memory_budget_bytes: usize) -> usize {
    (memory_budget_bytes as f64 * PARENT_INDEX_BUDGET_FRACTION) as usize
}

/// Fail loud (O6) if the resident FK parent index has outgrown its budget share.
/// A no-op when `budget_bytes == 0` (budget unknown / guard disabled).
fn check_parent_index_budget(
    parents: &ParentIndexSet,
    budget_bytes: usize,
) -> Result<(), MaterializeError> {
    if budget_bytes == 0 {
        return Ok(());
    }
    let used = parents.estimated_bytes();
    if used > budget_bytes {
        return Err(MaterializeError::ParentIndexBudgetExceeded {
            estimated_mb: used / (1024 * 1024),
            budget_mb: budget_bytes / (1024 * 1024),
        });
    }
    Ok(())
}

/// Pin every table's Iceberg snapshot up front, before emission (DEC-003 §17(a)).
/// Awaiting `scan_table` runs the `loadTable` + snapshot capture as the table
/// context resolves; the returned stream is dropped unpolled, so no Parquet data
/// is read. This pins all snapshots within seconds — narrowing cross-table
/// snapshot skew from the whole build duration to this window — and the emit-pass
/// scans reuse the pins (session first-writer-wins), so it adds no duplicate
/// `loadTable` GETs.
///
/// The pins run CONCURRENTLY, bounded by `parallelism` (O5): each pin is one
/// `loadTable` round-trip, and the serial loop paid that latency per table in
/// series (A2: ~1.85s/GET × 16 tables ≈ 30s of pure round-trip). Concurrency is
/// capped by the knob, so peak in-flight requests stay bounded.
async fn pin_all_tables(
    provider: &dyn R2rmlBuildProvider,
    graph_source_id: &str,
    mapping: &CompiledR2rmlMapping,
    parallelism: usize,
) -> Result<(), MaterializeError> {
    let mut tables: Vec<&str> = mapping
        .triples_maps
        .values()
        .filter_map(|tm| tm.table_name())
        .collect();
    tables.sort_unstable();
    tables.dedup();
    let mut pins = futures::stream::iter(tables.into_iter().map(|table| async move {
        provider
            .scan_table(graph_source_id, table, &[], &[], None, None)
            .await
            .map(|_| ())
    }))
    .buffer_unordered(parallelism.max(1));
    while let Some(res) = pins.next().await {
        res?;
    }
    Ok(())
}

/// Sum one worker's [`MaterializeStats`] into the running total (O1 aggregation).
fn merge_stats(acc: &mut MaterializeStats, other: MaterializeStats) {
    acc.subjects += other.subjects;
    acc.type_triples += other.type_triples;
    acc.data_triples += other.data_triples;
    acc.ref_triples += other.ref_triples;
    acc.ref_dangling += other.ref_dangling;
    acc.null_objects += other.null_objects;
    for (k, v) in other.per_tm {
        *acc.per_tm.entry(k).or_default() += v;
    }
    for (k, v) in other.ref_edges {
        *acc.ref_edges.entry(k).or_default() += v;
    }
    // Emit workers never populate this (it comes from the parent-index build, folded
    // in by the driver); summed for robustness so no path can silently drop it.
    for (k, v) in other.dup_parent_keys {
        *acc.dup_parent_keys.entry(k).or_default() += v;
    }
}

/// One completed chunk's `(idx, ParsedChunk)` or a stringified build error — the
/// item type of the producer→consumer result channel (Send + Clone sender).
type ChunkResult = std::result::Result<(usize, ParsedChunk), String>;

/// One unit of produce work handed to a worker: `(TriplesMap IRI, scanned batch)`.
type WorkItem = (String, fluree_db_tabular::ColumnBatch);

/// The join handles of one wave's produce-worker pool; each yields the worker's
/// aggregated `(MaterializeStats, encode_ms)` or a stringified error.
type WaveWorkerHandles =
    Vec<std::thread::JoinHandle<std::result::Result<(MaterializeStats, u128), String>>>;

/// Spawn `workers` sync render/encode threads for one wave of the parallel produce
/// pool (O1 machinery, reused per wave by O5(c)). Each worker pulls batches from
/// the shared `work_rx`, interns them DIRECTLY into a private per-chunk
/// [`ImportSink`] (O3a), cuts byte-budgeted chunks at batch boundaries, claims a
/// globally-unique `idx` from `next_idx` as each chunk closes, and ships
/// `(idx, chunk)` on `result_tx`. `parents` is the wave's frozen (read-only)
/// cumulative FK parent index. Returns the join handles; the caller drops `work_tx`
/// then joins to drain the wave.
#[allow(clippy::too_many_arguments)]
fn spawn_produce_workers(
    workers: usize,
    work_rx: &Arc<std::sync::Mutex<std::sync::mpsc::Receiver<WorkItem>>>,
    result_tx: &std::sync::mpsc::SyncSender<ChunkResult>,
    parents: &Arc<ParentIndexSet>,
    mapping: &Arc<CompiledR2rmlMapping>,
    next_idx: &Arc<std::sync::atomic::AtomicUsize>,
    chunk_size_bytes: usize,
    ctx: &VirtualChunkContext,
) -> Result<WaveWorkerHandles, MaterializeError> {
    use std::sync::atomic::Ordering;

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let work_rx = Arc::clone(work_rx);
        let worker_tx = result_tx.clone();
        let parents = Arc::clone(parents);
        let mapping = Arc::clone(mapping);
        let next_idx = Arc::clone(next_idx);
        let shared_alloc = Arc::clone(ctx.shared_alloc);
        let ledger = ctx.ledger_id.to_string();
        let spool_dir = ctx.spool_dir.to_path_buf();
        let spool_config = ctx.spool_config.cloned();
        let compress = ctx.compress;
        let handle = std::thread::Builder::new()
            .name("virtual-materializer-worker".into())
            .spawn(
                move || -> std::result::Result<(MaterializeStats, u128), String> {
                    let mut stats = MaterializeStats::default();
                    let mut encode_ms: u128 = 0;

                    'chunks: loop {
                        // Secure the first batch of a chunk BEFORE claiming an idx,
                        // so an idx is never wasted on an empty chunk (a gap would
                        // stall the in-order consumer forever).
                        let first = match work_rx.lock().unwrap().recv() {
                            Ok(item) => item,
                            Err(_) => break 'chunks, // driver closed the work channel
                        };
                        let idx = next_idx.fetch_add(1, Ordering::SeqCst);
                        let t = (idx + 1) as i64;
                        let enc = std::time::Instant::now();

                        // Fresh per-chunk sink. O3a: each batch's triples are
                        // interned DIRECTLY into this sink as they are enumerated —
                        // no intermediate owned-triple buffer, no second intern pass —
                        // and the chunk is cut on the running byte estimate at batch
                        // boundaries (one batch of overshoot at most).
                        let mut worker_cache = WorkerCache::new(Arc::clone(&shared_alloc));
                        let mut sink = ImportSink::new_cached(
                            &mut worker_cache,
                            t,
                            format!("{ledger}-{t}"),
                            compress,
                        )
                        .map_err(|e| format!("import sink create: {e}"))?;
                        if let Some(cfg) = spool_config.as_ref() {
                            let spool_path = spool_dir.join(format!("chunk_{idx}.spool"));
                            let spool_ctx = SpoolContext::new(spool_path, idx, 0, cfg)
                                .map_err(|e| format!("spool create: {e}"))?;
                            sink.set_spool_context(spool_ctx);
                        }

                        let mut est: usize = 0;
                        let mut ended = false;
                        let mut item = Some(first);
                        loop {
                            let (tm_iri, batch) = match item.take() {
                                Some(i) => i,
                                None => match work_rx.lock().unwrap().recv() {
                                    Ok(i) => i,
                                    Err(_) => {
                                        ended = true;
                                        break;
                                    }
                                },
                            };
                            if let Some(tm) = mapping.triples_maps.get(&tm_iri) {
                                let mut obs = InterningObserver {
                                    sink: &mut sink,
                                    bytes: &mut est,
                                };
                                emit_batch(tm, &batch, &parents, &mut obs, &mut stats)
                                    .map_err(|e| e.to_string())?;
                            }
                            if est >= chunk_size_bytes {
                                break;
                            }
                        }

                        // Finish + ship this chunk (always non-empty: it holds
                        // `first`). Mirror build_stamp_chunk's finalize.
                        let (writer, prefix_map, spool_ctx) = sink
                            .into_parts()
                            .map_err(|e| format!("flake encode: {e}"))?;
                        let op_count = writer.op_count();
                        let new_codes = worker_cache.into_new_codes();
                        let spool_result = spool_ctx.map(SpoolContext::finish_buffered);
                        let parsed = ParsedChunk {
                            writer,
                            op_count,
                            new_codes,
                            prefix_map,
                            spool_result,
                            txn_meta: Vec::new(),
                        };
                        encode_ms += enc.elapsed().as_millis();
                        // Err => the consumer dropped the receiver; stop early.
                        if worker_tx.send(Ok((idx, parsed))).is_err() {
                            return Ok((stats, encode_ms));
                        }
                        if ended {
                            break 'chunks;
                        }
                    }
                    Ok((stats, encode_ms))
                },
            )
            .map_err(|e| {
                MaterializeError::from(R2rmlError::Materialization(format!(
                    "spawn materialize worker: {e}"
                )))
            })?;
        handles.push(handle);
    }
    Ok(handles)
}

/// Stream a virtual R2RML source through the whole-graph enumerator with a
/// PARALLEL, WAVE-SCHEDULED produce side (O1 + O5(c)), encoding byte-budgeted
/// [`ParsedChunk`]s and sending each on `result_tx` as `(idx, chunk)`. `idx` is a
/// globally-unique, contiguous sequence from a shared atomic (across all waves);
/// the downstream `commit_parsed_chunks_in_order` consumer reorders by `idx`, so
/// chunks may arrive out of order. The final chunk (highest `idx`, committed last →
/// the twin's head) carries the completion stamp.
///
/// Wave model (O5(c) — eliminate the fact-table double-read): tables are scheduled
/// into topological WAVES ([`fluree_db_r2rml::materialize::MaterializationPlan::waves`])
/// so a foreign-key parent emits in a strictly earlier wave than its children. A
/// parent's key → subject index is built DURING its own single emit scan
/// (index-during-emit, on this driver thread), so a non-preindex parent is scanned
/// exactly ONCE for the build — the previous model pre-indexed every FK parent in a
/// separate Pass-1 and then re-scanned it to emit (a full double read of every big
/// parent fact over cold S3). Only cyclic / self-referential parents keep the
/// pre-index fallback (they cannot be indexed in a single forward pass).
///
/// Parallelism (bounded by `parallelism`, the knob that once only sized the result
/// channel): within a wave, the cumulative parent index (fallback + every earlier
/// wave) is frozen READ-ONLY behind an `Arc`, shared by a fresh pool of
/// `parallelism` sync worker threads that each render + intern + encode batches into
/// their OWN private chunker/sink; this driver scans the wave's tables (async) and
/// dispatches their batches to the pool over a bounded channel (a blocking send is
/// natural backpressure). After the wave drains, its wave-local parent index folds
/// into the cumulative for the next wave. Peak produce memory = `parallelism` ×
/// (one chunk buffer + one encoding sink) for the single live wave, plus the
/// resident cumulative parent index — the SAME N×chunk model the text import path
/// lives under; no new knob. The r2rml scan futures are !Send, but each worker
/// drives only SYNC render/encode (the async scans stay on this driver), so no Send
/// bound is needed.
#[allow(clippy::too_many_arguments)]
pub async fn drive_virtual_import(
    provider: &dyn R2rmlBuildProvider,
    graph_source_id: &str,
    chunk_size_bytes: usize,
    parallelism: usize,
    memory_budget_bytes: usize,
    allow_duplicate_parent_keys: bool,
    ctx: &VirtualChunkContext<'_>,
    result_tx: std::sync::mpsc::SyncSender<ChunkResult>,
) -> Result<MaterializeStats, MaterializeError> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    let mapping = Arc::new(provider.compiled_mapping(graph_source_id, None).await?);
    let mut parents = ParentIndexSet::new(&mapping)?;
    let materialization = plan(&mapping);
    let workers = parallelism.max(1);
    // The FK parent index is fully resident for the whole build and was outside
    // any budget (O6). Charge it against a fraction of the import budget and fail
    // loud if it overflows. `0` (no budget known) disables the guard.
    let parent_index_budget = parent_index_budget_bytes(memory_budget_bytes);

    // Pin-all pre-pass — pin every table's snapshot before emission so the
    // watermark is complete and cross-table skew is bounded to seconds (§17(a)).
    let pin_start = std::time::Instant::now();
    pin_all_tables(provider, graph_source_id, &mapping, workers).await?;
    tracing::info!(
        pin_ms = pin_start.elapsed().as_millis() as u64,
        "materialize.phase pin_all_tables"
    );

    // Pass 1 (fallback) — pre-index ONLY the parents that cannot be indexed during
    // a single forward emit scan: cyclic and self-referential tables. O5(c) keeps
    // exactly this fallback and drops the old "pre-index EVERY FK parent" pass —
    // for an acyclic schema (the common case, incl. sf01) this scans nothing, and
    // every ordinary FK parent is now indexed DURING its own emit wave (below),
    // scanned once instead of twice (the eliminated double-read). Scans run
    // concurrently (O5): each table indexes into its OWN partial index (disjoint
    // parent IRIs), merged back after.
    let preindex_tables: Vec<String> = materialization
        .preindex
        .iter()
        .filter(|iri| parents.is_parent(iri))
        .cloned()
        .collect();
    if !preindex_tables.is_empty() {
        let parents_ref = &parents;
        let mapping_ref = &mapping;
        let results: Vec<Result<Option<ParentIndexSet>, MaterializeError>> =
            futures::stream::iter(preindex_tables.iter().map(move |tm_iri| async move {
                let Some(tm) = mapping_ref.triples_maps.get(tm_iri) else {
                    return Ok::<Option<ParentIndexSet>, MaterializeError>(None);
                };
                let table = tm
                    .table_name()
                    .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
                let mut partial = parents_ref.split_empty();
                let projection = scan_projection(tm, &partial);
                let mut stream = provider
                    .scan_table(graph_source_id, table, &projection, &[], None, None)
                    .await?;
                while let Some(batch) = stream.next().await {
                    partial.index_batch(tm, &batch?)?;
                }
                Ok(Some(partial))
            }))
            .buffer_unordered(workers)
            .collect()
            .await;
        for res in results {
            if let Some(partial) = res? {
                parents.merge_from(partial);
            }
        }
        check_parent_index_budget(&parents, parent_index_budget)?;
    }

    // Shared across all waves: the globally-unique chunk-idx sequence and the
    // running stats / encode-time aggregates. `next_idx` is Arc'd once so chunk
    // indices stay contiguous 0..N across every wave and the final stamp chunk.
    let next_idx = Arc::new(AtomicUsize::new(0));
    let mut total_stats = MaterializeStats::default();
    let mut total_encode_ms: u128 = 0;
    let mut consumer_gone = false;

    // Pass 2 — wave-scheduled parallel produce (O5(c)). Each wave freezes the
    // cumulative parent index (the pre-index fallback + every earlier wave) behind an
    // Arc, shared read-only by a fresh worker pool; this driver scans the wave's
    // tables (serially across tables, file-concurrent within a table), indexing each
    // non-preindex FK parent into a wave-local index DURING its single emit scan
    // (index-during-emit — no separate pre-index pass) while dispatching its batches
    // to the pool. After the wave drains, the wave-local index folds into the
    // cumulative for the next wave, so a non-preindex parent is scanned exactly once.
    'waves: for wave in &materialization.waves {
        let arc_parents = Arc::new(parents);
        let mut wave_local = arc_parents.split_empty();

        let (work_tx, work_rx) =
            std::sync::mpsc::sync_channel::<WorkItem>(workers.saturating_mul(2).max(1));
        let work_rx = Arc::new(Mutex::new(work_rx));
        let handles = spawn_produce_workers(
            workers,
            &work_rx,
            &result_tx,
            &arc_parents,
            &mapping,
            &next_idx,
            chunk_size_bytes,
            ctx,
        )?;

        // Driver: scan each table in this wave, index non-preindex FK parents into
        // the wave-local set DURING the scan, and dispatch batches to the pool. A
        // blocking send is natural backpressure; if the consumer dropped, stop.
        for tm_iri in wave {
            let Some(tm) = mapping.triples_maps.get(tm_iri) else {
                continue;
            };
            let table = tm
                .table_name()
                .ok_or_else(|| MaterializeError::NoTable(tm.iri.clone()))?;
            let index_here =
                !materialization.preindex.contains(tm_iri) && arc_parents.is_parent(tm_iri);
            let projection = scan_projection(tm, &arc_parents);
            let table_start = std::time::Instant::now();
            let mut stream = provider
                .scan_table(graph_source_id, table, &projection, &[], None, None)
                .await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if index_here {
                    wave_local.index_batch(tm, &batch)?;
                }
                if work_tx.send((tm_iri.clone(), batch)).is_err() {
                    consumer_gone = true;
                    break;
                }
            }
            tracing::info!(
                table = %table,
                scan_ms = table_start.elapsed().as_millis() as u64,
                "materialize.phase table_scan"
            );
            if consumer_gone {
                break;
            }
        }
        drop(work_tx); // signal this wave's workers: no more batches

        // Join this wave's workers, aggregating stats + encode time; surface the
        // first worker error.
        let mut worker_err: Option<String> = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok((stats, enc))) => {
                    merge_stats(&mut total_stats, stats);
                    total_encode_ms += enc;
                }
                Ok(Err(e)) => worker_err = worker_err.or(Some(e)),
                Err(_) => {
                    worker_err =
                        worker_err.or(Some("materialize worker thread panicked".to_string()));
                }
            }
        }
        if let Some(e) = worker_err {
            return Err(MaterializeError::from(R2rmlError::Materialization(e)));
        }

        // Reclaim the cumulative index (all workers joined → sole owner) and fold in
        // this wave's freshly-built parent indexes for the next wave. `try_unwrap`
        // cannot fail here (every clone was moved into a now-joined worker).
        parents = Arc::try_unwrap(arc_parents).map_err(|_| {
            MaterializeError::from(R2rmlError::Materialization(
                "internal: parent index still shared after wave workers joined".to_string(),
            ))
        })?;
        parents.merge_from(wave_local);
        check_parent_index_budget(&parents, parent_index_budget)?;

        if consumer_gone {
            break 'waves;
        }
    }

    // Duplicate-parent-key decline gate (dupkey-adjudication.md ruling C2). The
    // deterministic keep-min tie-break in `index_batch` already made WHICH parent
    // survives reproducible; this refuses to silently BAKE the fan-out loss (each
    // ambiguous key keeps one parent, drops the rest) unless the operator opts in.
    // Checked after the full index build (every parent, including those indexed
    // during their own emit wave). Returning Err aborts the build BEFORE the
    // completion stamp is written, so no valid (stamped) twin is ever published for a
    // declined source — the emit chunks already sent stay unstamped and the overall
    // import fails.
    let dup_parent_keys = parents.dup_parent_keys();
    if !dup_parent_keys.is_empty() && !allow_duplicate_parent_keys {
        return Err(MaterializeError::DuplicateParentKeys {
            summary: format_dup_parent_keys(&dup_parent_keys, &parents, &mapping),
        });
    }
    total_stats.dup_parent_keys = dup_parent_keys.clone();

    // Assemble + ship the completion stamp on the FINAL chunk (highest idx →
    // committed last → the twin's head; a head-walk finds the stamp iff the build
    // completed, §17(a)). With parallel workers the stamp always rides its own
    // final commit (0-triple + stamp) — the already-supported empty-stamp shape. The
    // dup-key counts are recorded in the stamp only when the override built over them.
    let stamp = build_watermark_stamp(provider, graph_source_id, &mapping, dup_parent_keys)?;
    tracing::info!(
        builder_version = %stamp.builder_version,
        mapping_hash = %stamp.mapping_hash,
        watermark_tables = stamp.tables.len(),
        "materialize completion stamp assembled"
    );
    let idx = next_idx.fetch_add(1, Ordering::SeqCst);
    let t = (idx + 1) as i64;
    let parsed = build_stamp_chunk(ctx, t, idx, &stamp)?;
    let _ = result_tx.send(Ok((idx, parsed)));
    drop(result_tx);

    tracing::info!(
        encode_ms = total_encode_ms as u64,
        workers,
        chunks = idx + 1,
        total_triples = total_stats.total_triples(),
        "materialize.phase encode_total"
    );
    Ok(total_stats)
}

// ---------------------------------------------------------------------------
// Parity gate: verify the built twin against its virtual source (DEC-003 §8)
// ---------------------------------------------------------------------------

/// How thoroughly to verify a built twin against its virtual source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyMode {
    /// Always-on default: class counts + a stratified per-subject sample.
    Quick,
    /// The full-triple diff — every source triple must be present in the twin and
    /// vice-versa. Reuses the Chunk-A enumerator as the oracle side.
    Full,
}

/// Outcome of one parity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    /// Source and twin agree.
    Match,
    /// A count differs between the source and the twin.
    Mismatch { source: u64, twin: u64 },
    /// Triple sets differ (full or per-subject sample).
    TripleDiff {
        missing_in_twin: usize,
        extra_in_twin: usize,
    },
    /// Deliberately not run (with a reason) — e.g. per-property counts that need
    /// a manifest-sound fast path not available in this run.
    Skipped { reason: String },
}

impl CheckOutcome {
    /// A check passes if it matched or was deliberately skipped.
    fn passed(&self) -> bool {
        matches!(self, CheckOutcome::Match | CheckOutcome::Skipped { .. })
    }
}

/// One named parity check and its outcome.
#[derive(Debug, Clone)]
pub struct ParityCheck {
    pub name: String,
    pub outcome: CheckOutcome,
}

/// The result of verifying a twin: the per-check breakdown and whether the whole
/// gate passed. A failing gate means the twin must NOT be announced (the build is
/// reported FAILED). Because verification needs a queryable — hence already
/// published — twin, the sound ordering is publish-then-verify-then-drop-on-fail:
/// the CLI/orchestrator drops (retracts) the twin when `passed` is false.
#[derive(Debug, Clone)]
pub struct ParityReport {
    pub passed: bool,
    pub checks: Vec<ParityCheck>,
}

impl ParityReport {
    /// Human-readable summary of the failing checks (empty when passed).
    pub fn failures(&self) -> Vec<&ParityCheck> {
        self.checks.iter().filter(|c| !c.outcome.passed()).collect()
    }
}

/// Subjects per class fully compared by the stratified sample (deterministic:
/// the lexicographically first N subjects of each class).
const SAMPLE_SUBJECTS_PER_CLASS: usize = 3;

/// Verify a built native twin against its virtual R2RML source at the pinned
/// snapshots (DEC-003 §8). Both verify modes are MEMORY-BOUNDED: neither ever
/// holds the whole graph resident (the O2 fix — the previous implementation
/// double-buffered every source triple into a `BTreeSet` AND read the entire twin
/// into another, ~26.8 GB footprint at 6.1M triples and a certain OOM at 35M even
/// in Quick mode). The virtual side is the same Chunk-A enumerator the twin was
/// built from (so a quick pass catches ingest/index corruption but shares the
/// enumerator's blind spot — run `--verify full` for the whole-graph diff).
///
/// - [`VerifyMode::Quick`] — peak RSS is O(sampled subjects): class counts come
///   from a single streamed source pass + bounded twin COUNT queries, and the
///   per-subject sample pulls only a seeded window of subjects on each side.
/// - [`VerifyMode::Full`] — spools BOTH sides to disk, external-sorts them under a
///   bounded working set, and streams a k-way diff; peak RSS is O(one sort run).
///
/// `tmp_dir` (Full mode only; Quick ignores it) is the parent directory the
/// on-disk spool + sorted runs are created under. Pass the target's `.fluree`
/// storage area, NOT a tmpfs `/tmp` — on many Linux hosts `std::env::temp_dir()`
/// is RAM-backed, which would put the tens of GB of Full-mode spill back in memory
/// on exactly the large twins the bounded rewrite protects. `None` falls back to
/// `std::env::temp_dir()` (fine for small library callers / tests); the CLI always
/// passes a `.fluree`-based dir. See [`verify_twin_full`] for the SIGKILL-residue
/// note.
///
/// Returns the report; the caller decides whether to announce or drop the twin.
pub async fn verify_twin<P>(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    provider: &P,
    graph_source_id: &str,
    mode: VerifyMode,
    tmp_dir: Option<&Path>,
) -> Result<ParityReport, MaterializeError>
where
    P: R2rmlProvider + R2rmlTableProvider,
{
    match mode {
        VerifyMode::Quick => verify_twin_quick(fluree, ledger, provider, graph_source_id).await,
        VerifyMode::Full => {
            verify_twin_full(fluree, ledger, provider, graph_source_id, tmp_dir).await
        }
    }
}

/// The `per-property-counts` skipped-with-note check (shared by both modes).
fn per_property_skipped() -> ParityCheck {
    ParityCheck {
        name: "per-property-counts".to_string(),
        outcome: CheckOutcome::Skipped {
            reason: "manifest-sound non-nullable per-property counts are the \
                     real-Iceberg fast path (DEC-003 §5); not run over the \
                     enumeration oracle"
                .to_string(),
        },
    }
}

/// The class set a mapping declares.
fn mapping_classes(mapping: &CompiledR2rmlMapping) -> BTreeSet<String> {
    mapping
        .triples_maps
        .values()
        .flat_map(|tm| tm.subject_map.classes.iter().cloned())
        .collect()
}

/// Quick parity gate (default). Memory-bounded to O(sampled subjects):
///
/// 1. **Twin side (bounded queries):** per class, a `COUNT` and a seeded window of
///    [`SAMPLE_SUBJECTS_PER_CLASS`] sample subjects; each sampled subject's full
///    triple set is pulled with a bound-subject query. Never the whole twin.
/// 2. **Source side (one streamed pass):** the enumerator streams through
///    [`SampleAndCountObserver`], which counts per-class `rdf:type` triples and
///    retains full triple sets ONLY for the sampled subjects — everything else is
///    dropped as it streams, so peak RSS is O(sampled subjects), not O(graph).
/// 3. **Compare:** class counts (source-vs-twin) + each sampled subject's triple
///    set, both numeric-value-canonicalized so a lexical re-encoding of a numeric
///    (E-notation vs plain, `"5.00"` vs `"5"`) is not misreported (the decimal
///    false-reject A2 caught).
async fn verify_twin_quick<P>(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    provider: &P,
    graph_source_id: &str,
) -> Result<ParityReport, MaterializeError>
where
    P: R2rmlProvider + R2rmlTableProvider,
{
    let mapping = provider.compiled_mapping(graph_source_id, None).await?;
    let classes = mapping_classes(&mapping);
    // Same deterministic seed the build stamped, so the sampled window is
    // reproducible and auditable across an independent re-verify.
    let seed = sample_seed_from_hash(&mapping_hash(&mapping));

    // 1) Twin side — bounded per class.
    let twin_read_start = std::time::Instant::now();
    let mut twin_class_count: BTreeMap<String, u64> = BTreeMap::new();
    let mut wanted: BTreeSet<String> = BTreeSet::new();
    let mut twin_samples: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for class in &classes {
        let count = twin_count_class(fluree, ledger, class).await?;
        twin_class_count.insert(class.clone(), count);
        let offset = seeded_offset(seed, count, SAMPLE_SUBJECTS_PER_CLASS);
        let subjects =
            twin_sample_subject_iris(fluree, ledger, class, SAMPLE_SUBJECTS_PER_CLASS, offset)
                .await?;
        for subject_iri in subjects {
            let token = format!("<{subject_iri}>");
            let triples = twin_subject_triples(fluree, ledger, &subject_iri).await?;
            wanted.insert(token.clone());
            twin_samples.insert(token, triples);
        }
    }
    tracing::info!(
        verify_twin_read_ms = twin_read_start.elapsed().as_millis() as u64,
        sampled_subjects = wanted.len(),
        "materialize.phase verify_twin_read"
    );

    // 2) Source side — one streamed pass, O(sampled subjects) resident.
    let oracle_start = std::time::Instant::now();
    let mut observer = SampleAndCountObserver::new(wanted);
    materialize_graph(provider, graph_source_id, &mut observer).await?;
    tracing::info!(
        verify_oracle_ms = oracle_start.elapsed().as_millis() as u64,
        classes = observer.class_counts.len(),
        "materialize.phase verify_source_oracle"
    );

    // 3) Compare.
    let mut checks = Vec::new();
    for class in &classes {
        let source = observer.class_counts.get(class).copied().unwrap_or(0);
        let twin = twin_class_count.get(class).copied().unwrap_or(0);
        checks.push(ParityCheck {
            name: format!("count:{class}"),
            outcome: if source == twin {
                CheckOutcome::Match
            } else {
                CheckOutcome::Mismatch { source, twin }
            },
        });
    }
    checks.push(per_property_skipped());
    for (token, twin_triples) in &twin_samples {
        let src = observer.retained.get(token).cloned().unwrap_or_default();
        checks.push(ParityCheck {
            name: format!("sample:{token}"),
            outcome: if &src == twin_triples {
                CheckOutcome::Match
            } else {
                CheckOutcome::TripleDiff {
                    missing_in_twin: src.difference(twin_triples).count(),
                    extra_in_twin: twin_triples.difference(&src).count(),
                }
            },
        });
    }

    let passed = checks.iter().all(|c| c.outcome.passed());
    Ok(ParityReport { passed, checks })
}

/// Full parity gate (`--verify full`). Memory-bounded via external merge-sort:
/// both sides are spooled to on-disk N-Triples (the source streamed through the
/// enumerator, the twin streamed in ONE linear pass over the binary index — the
/// export path's `scan_all` shape, not O(n^2) OFFSET paging), each canonicalized as
/// it is written; the two files are external-sorted under a bounded working set
/// ([`SORT_RUN_LINES`] lines per run) and a streaming k-way diff counts the
/// symmetric difference. Peak RSS is O(one sort run), never O(graph) — this replaces
/// both the old whole-graph double-buffer that OOMed at scale AND the query-side
/// OFFSET paging that re-sorted the whole twin per page.
async fn verify_twin_full<P>(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    provider: &P,
    graph_source_id: &str,
    tmp_dir: Option<&Path>,
) -> Result<ParityReport, MaterializeError>
where
    P: R2rmlProvider + R2rmlTableProvider,
{
    let mapping = provider.compiled_mapping(graph_source_id, None).await?;
    let classes = mapping_classes(&mapping);

    // Spool + sorted runs go under `tmp_dir` (the target's `.fluree` storage area
    // when the CLI drives this — NOT tmpfs `/tmp`, which would put the tens of GB of
    // Full-mode spill back in RAM; item #3). `None` falls back to the system temp
    // dir for small library callers. `TmpDirGuard` removes the whole directory on
    // scope exit (including on error), but NOT on SIGKILL — a crash mid-verify
    // leaves the spill behind; rooting it under `.fluree` keeps that residue
    // discoverable and cleanable rather than orphaned in a shared `/tmp`.
    let base = tmp_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!(
        "fluree-verify-{}-{}",
        sanitize_tmp(graph_source_id),
        std::process::id()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| R2rmlError::Materialization(format!("verify tmp dir: {e}")))?;
    let _guard = TmpDirGuard(dir.clone());

    // Source side → disk (streamed, canonicalized, per-class counts collected).
    let oracle_start = std::time::Instant::now();
    let source_raw = dir.join("source.nt");
    let mut writer = FileWritingObserver::create(&source_raw)?;
    materialize_graph(provider, graph_source_id, &mut writer).await?;
    let source_class_count = writer.finish()?;
    tracing::info!(
        verify_oracle_ms = oracle_start.elapsed().as_millis() as u64,
        "materialize.phase verify_source_oracle"
    );

    // Twin side → disk in a SINGLE linear pass over the binary index (the export
    // path's `BinaryCursor::scan_all` shape), canonicalized as written — NOT the old
    // `ORDER BY ?s ?p ?o LIMIT N OFFSET M` wildcard paging, which re-sorted the whole
    // twin per page (O(n^2)) and materialized each page's bindings on the query side
    // (item #2). Per-class counts come from bounded COUNT queries — the same shape the
    // Quick gate uses — so they cost O(classes), never a whole-twin scan.
    let twin_start = std::time::Instant::now();
    let twin_raw = dir.join("twin.nt");
    spool_twin_ntriples_indexed(ledger, &twin_raw)?;
    let mut twin_class_count: BTreeMap<String, u64> =
        classes.iter().map(|c| (c.clone(), 0)).collect();
    for class in &classes {
        twin_class_count.insert(
            class.clone(),
            twin_count_class(fluree, ledger, class).await?,
        );
    }
    tracing::info!(
        verify_twin_read_ms = twin_start.elapsed().as_millis() as u64,
        "materialize.phase verify_twin_read"
    );

    // External-sort both sides and stream a k-way diff (bounded).
    let source_sorted = external_sort_lines(&source_raw, &dir, "source")?;
    let twin_sorted = external_sort_lines(&twin_raw, &dir, "twin")?;
    let (missing_in_twin, extra_in_twin) = diff_sorted_files(&source_sorted, &twin_sorted)?;

    let mut checks = Vec::new();
    for class in &classes {
        let source = source_class_count.get(class).copied().unwrap_or(0);
        let twin = twin_class_count.get(class).copied().unwrap_or(0);
        checks.push(ParityCheck {
            name: format!("count:{class}"),
            outcome: if source == twin {
                CheckOutcome::Match
            } else {
                CheckOutcome::Mismatch { source, twin }
            },
        });
    }
    checks.push(per_property_skipped());
    checks.push(ParityCheck {
        name: "full-triple-diff".to_string(),
        outcome: if missing_in_twin == 0 && extra_in_twin == 0 {
            CheckOutcome::Match
        } else {
            CheckOutcome::TripleDiff {
                missing_in_twin,
                extra_in_twin,
            }
        },
    });

    let passed = checks.iter().all(|c| c.outcome.passed());
    Ok(ParityReport { passed, checks })
}

/// A [`TripleObserver`] used by the Quick gate: it counts per-class `rdf:type`
/// triples and retains the full (canonicalized) triple set ONLY for a fixed set of
/// wanted subject tokens. Everything else is dropped as it streams, so a whole-
/// graph source pass costs O(sampled subjects) resident — the O2 fix.
struct SampleAndCountObserver {
    /// N-Triples subject tokens (e.g. `<http://ex/3>`) to retain triples for.
    wanted: BTreeSet<String>,
    /// Per-class `rdf:type` triple counts (class IRI → count).
    class_counts: BTreeMap<String, u64>,
    /// Retained canonicalized N-Triples, grouped by wanted subject token.
    retained: BTreeMap<String, BTreeSet<String>>,
}

impl SampleAndCountObserver {
    fn new(wanted: BTreeSet<String>) -> Self {
        Self {
            wanted,
            class_counts: BTreeMap::new(),
            retained: BTreeMap::new(),
        }
    }
}

impl TripleObserver for SampleAndCountObserver {
    fn observe(
        &mut self,
        subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> Result<(), R2rmlError> {
        if predicate == fluree_vocab::rdf::TYPE {
            if let RdfTerm::Iri(class) = object {
                *self.class_counts.entry(class.clone()).or_default() += 1;
            }
        }
        if !self.wanted.is_empty() {
            let token = render_term(subject);
            if self.wanted.contains(&token) {
                let line = canonicalize_value_nt(&format!(
                    "{token} <{predicate}> {} .",
                    render_term(object)
                ));
                self.retained.entry(token).or_default().insert(line);
            }
        }
        Ok(())
    }
}

/// Count of instances of one class in the twin (bounded `COUNT` query). A
/// no-match class returns 0 (SPARQL COUNT with no GROUP BY yields one row).
async fn twin_count_class(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    class: &str,
) -> Result<u64, MaterializeError> {
    let q = format!(
        "SELECT (COUNT(?s) AS ?n) WHERE {{ ?s <{}> <{class}> }}",
        fluree_vocab::rdf::TYPE
    );
    let bindings = twin_query_bindings(fluree, ledger, &q).await?;
    Ok(bindings
        .first()
        .and_then(|b| b["n"]["value"].as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0))
}

/// A seeded window of up to `k` IRI subjects of one class from the twin, in a
/// deterministic order (`ORDER BY ?s LIMIT k OFFSET off`). Blank-node subjects are
/// skipped (they cannot be round-tripped through a bound-subject query); the class
/// COUNT still covers their cardinality.
///
/// SCALE NOTE (item #2): `offset` is the [`seeded_offset`] window position, which can
/// land near the end of a class, so this is a per-class `ORDER BY ?s ... OFFSET N`
/// sort-and-skip. It is bounded at current scale — the sort is over ONE class's
/// subjects (not the whole graph) and `k` is [`SAMPLE_SUBJECTS_PER_CLASS`] — but it
/// is not a bounded scan. The seeded window is deliberate (auditable, reproducible,
/// rotatable to widen coverage), and a bounded seek would need a scan-side offset the
/// query layer does not expose; left as a documented residual rather than dropping
/// the seeded spread. The Full gate's whole-twin read is already a single linear pass
/// (see [`spool_twin_ntriples_indexed`]).
async fn twin_sample_subject_iris(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    class: &str,
    k: usize,
    offset: u64,
) -> Result<Vec<String>, MaterializeError> {
    let q = format!(
        "SELECT ?s WHERE {{ ?s <{}> <{class}> }} ORDER BY ?s LIMIT {k} OFFSET {offset}",
        fluree_vocab::rdf::TYPE
    );
    let bindings = twin_query_bindings(fluree, ledger, &q).await?;
    Ok(bindings
        .iter()
        .filter(|b| b["s"]["type"].as_str() == Some("uri"))
        .filter_map(|b| b["s"]["value"].as_str().map(str::to_string))
        .collect())
}

/// The full (canonicalized) triple set of one subject in the twin (bound-subject
/// wildcard). Rendered with the SAME [`term_to_ntriples`] the source side uses, so
/// the two sides diff byte-identically after numeric canonicalization.
async fn twin_subject_triples(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    subject_iri: &str,
) -> Result<BTreeSet<String>, MaterializeError> {
    let q = format!("SELECT ?p ?o WHERE {{ <{subject_iri}> ?p ?o }}");
    let bindings = twin_query_bindings(fluree, ledger, &q).await?;
    let mut set = BTreeSet::new();
    for b in &bindings {
        let p = b["p"]["value"].as_str().unwrap_or_default();
        let o = term_to_ntriples(&b["o"]);
        set.insert(canonicalize_value_nt(&format!(
            "<{subject_iri}> <{p}> {o} ."
        )));
    }
    Ok(set)
}

/// Run a SPARQL query against the twin and return its `results.bindings` array.
async fn twin_query_bindings(
    fluree: &crate::Fluree,
    ledger: &crate::LedgerState,
    sparql: &str,
) -> Result<Vec<serde_json::Value>, MaterializeError> {
    let db = crate::GraphDb::from_ledger_state(ledger);
    let result = fluree
        .query(&db, sparql)
        .await
        .map_err(|e| R2rmlError::Materialization(format!("twin query: {e}")))?;
    let json = result
        .to_sparql_json(&ledger.snapshot)
        .map_err(|e| R2rmlError::Materialization(format!("twin sparql-json: {e}")))?;
    Ok(json["results"]["bindings"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

/// Seeded starting offset for a `k`-subject sample of a class of `count` members:
/// a deterministic window position derived from the stamp seed, so the sample is
/// spread across the class (not always the lexicographic head) yet reproducible.
fn seeded_offset(seed: u64, count: u64, k: usize) -> u64 {
    let k = k as u64;
    if count > k {
        seed % (count - k)
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Full-mode spooling + bounded external sort (peak RSS = O(one sort run))
// ---------------------------------------------------------------------------

/// Lines held in memory per external-sort run before spilling a sorted run to
/// disk. Bounds the sort's working set; a larger value trades RAM for fewer runs.
const SORT_RUN_LINES: usize = 1_000_000;

/// A [`TripleObserver`] that streams canonicalized N-Triples to a file while
/// tallying per-class `rdf:type` counts. Peak RSS is O(1) — nothing is retained.
struct FileWritingObserver {
    writer: std::io::BufWriter<std::fs::File>,
    class_counts: BTreeMap<String, u64>,
}

impl FileWritingObserver {
    fn create(path: &Path) -> Result<Self, MaterializeError> {
        let file = std::fs::File::create(path)
            .map_err(|e| R2rmlError::Materialization(format!("verify spool create: {e}")))?;
        Ok(Self {
            writer: std::io::BufWriter::new(file),
            class_counts: BTreeMap::new(),
        })
    }

    fn finish(mut self) -> Result<BTreeMap<String, u64>, MaterializeError> {
        use std::io::Write as _;
        self.writer
            .flush()
            .map_err(|e| R2rmlError::Materialization(format!("verify spool flush: {e}")))?;
        Ok(self.class_counts)
    }
}

impl TripleObserver for FileWritingObserver {
    fn observe(
        &mut self,
        subject: &RdfTerm,
        predicate: &str,
        object: &RdfTerm,
    ) -> Result<(), R2rmlError> {
        use std::io::Write as _;
        if predicate == fluree_vocab::rdf::TYPE {
            if let RdfTerm::Iri(class) = object {
                *self.class_counts.entry(class.clone()).or_default() += 1;
            }
        }
        let line = canonicalize_value_nt(&format!(
            "{} <{predicate}> {} .",
            render_term(subject),
            render_term(object)
        ));
        writeln!(self.writer, "{line}")
            .map_err(|e| R2rmlError::Materialization(format!("verify spool write: {e}")))?;
        Ok(())
    }
}

/// Spool the whole twin to `path` as canonicalized N-Triples in a SINGLE linear pass
/// over the binary index — the export path's `BinaryCursor::scan_all` shape, which
/// streams the graph in Spot (s, p, o) order in bounded cursor batches. This replaces
/// the old `ORDER BY ?s ?p ?o LIMIT N OFFSET M` wildcard paging, which was O(n^2)
/// (each page re-sorted the whole twin) and materialized every page's bindings on the
/// query side (item #2). Each line is numeric/temporal value-canonicalized as it is
/// written (via [`CanonicalizingLineWriter`]) so the twin diffs byte-identically
/// against the enumerator-rendered source side. The novelty overlay is included, so
/// committed-but-not-yet-indexed twin triples are read too (e.g. the negative gate's
/// post-build injected corruption). Per-class counts are NOT collected here — the
/// caller derives them from bounded COUNT queries.
fn spool_twin_ntriples_indexed(
    ledger: &crate::LedgerState,
    path: &Path,
) -> Result<(), MaterializeError> {
    use fluree_db_binary_index::BinaryIndexStore;

    let binary_store: Arc<BinaryIndexStore> = ledger
        .binary_store
        .as_ref()
        .and_then(|te| te.0.clone().downcast::<BinaryIndexStore>().ok())
        .ok_or_else(|| {
            R2rmlError::Materialization(
                "twin has no binary index for full verify (is the twin indexed?)".to_string(),
            )
        })?;
    let overlay: &dyn fluree_db_core::OverlayProvider = ledger.novelty.as_ref();
    let config = crate::export::ExportConfig {
        g_id: 0, // default graph
        graph_iri: None,
        to_t: ledger.t(),
        overlay: Some(overlay),
        dict_novelty: Some(&ledger.dict_novelty),
    };

    let file = std::fs::File::create(path)
        .map_err(|e| R2rmlError::Materialization(format!("twin spool create: {e}")))?;
    let mut writer = CanonicalizingLineWriter::new(std::io::BufWriter::new(file));
    crate::export::export_graph_ntriples(&binary_store, &config, &mut writer)
        .map_err(|e| R2rmlError::Materialization(format!("twin index export: {e}")))?;
    writer.finish()
}

/// A [`std::io::Write`] adapter that numeric/temporal value-canonicalizes each
/// complete N-Triples line (via [`canonicalize_value_nt`]) before forwarding it to
/// the inner sink. The export writer emits one triple per line, terminated by ` .\n`,
/// in fragments (subject / predicate / object as separate writes); N-Triples escapes
/// any literal newline, so the only raw `\n` is the line terminator — safe to split
/// on. Peak resident state is one line.
struct CanonicalizingLineWriter<W: std::io::Write> {
    inner: W,
    buf: Vec<u8>,
}

impl<W: std::io::Write> CanonicalizingLineWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            buf: Vec::new(),
        }
    }

    /// Canonicalize + flush any trailing partial line (defensive — the export always
    /// ends every line with `\n`), then flush the inner writer.
    fn finish(mut self) -> Result<(), MaterializeError> {
        if !self.buf.is_empty() {
            let out = canonical_line_bytes(&self.buf);
            self.inner
                .write_all(&out)
                .and_then(|()| self.inner.write_all(b"\n"))
                .map_err(|e| R2rmlError::Materialization(format!("twin spool write: {e}")))?;
        }
        self.inner
            .flush()
            .map_err(|e| R2rmlError::Materialization(format!("twin spool flush: {e}")))?;
        Ok(())
    }
}

/// Canonicalize one N-Triples line (no trailing newline) to bytes. A non-UTF-8 line
/// (never emitted for valid N-Triples) is passed through verbatim rather than failing
/// the whole verify.
fn canonical_line_bytes(line: &[u8]) -> Vec<u8> {
    match std::str::from_utf8(line) {
        Ok(s) => canonicalize_value_nt(s).into_bytes(),
        Err(_) => line.to_vec(),
    }
}

impl<W: std::io::Write> std::io::Write for CanonicalizingLineWriter<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        let mut start = 0;
        while let Some(rel) = self.buf[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel;
            // Owned bytes, so the immutable borrow of `self.buf` ends before the
            // mutable write to `self.inner`.
            let out = canonical_line_bytes(&self.buf[start..end]);
            self.inner.write_all(&out)?;
            self.inner.write_all(b"\n")?;
            start = end + 1;
        }
        self.buf.drain(..start);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// External merge-sort a file of lines with de-duplication (an RDF graph is a set),
/// bounded to [`SORT_RUN_LINES`] lines resident per run. Writes sorted, de-duped
/// runs to `dir` then k-way merges them into `<tag>.sorted`, returning its path.
fn external_sort_lines(input: &Path, dir: &Path, tag: &str) -> Result<PathBuf, MaterializeError> {
    use std::io::{BufRead, BufReader, BufWriter, Write as _};

    let file = std::fs::File::open(input)
        .map_err(|e| R2rmlError::Materialization(format!("sort open {tag}: {e}")))?;
    let mut reader = BufReader::new(file);
    let mut runs: Vec<PathBuf> = Vec::new();
    let mut buf: Vec<String> = Vec::with_capacity(SORT_RUN_LINES.min(4096));
    let mut line = String::new();

    let flush_run = |buf: &mut Vec<String>,
                     runs: &mut Vec<PathBuf>|
     -> Result<(), MaterializeError> {
        if buf.is_empty() {
            return Ok(());
        }
        buf.sort_unstable();
        buf.dedup();
        let run_path = dir.join(format!("{tag}.run{}", runs.len()));
        let mut w = BufWriter::new(
            std::fs::File::create(&run_path)
                .map_err(|e| R2rmlError::Materialization(format!("sort run create {tag}: {e}")))?,
        );
        for l in buf.iter() {
            writeln!(w, "{l}")
                .map_err(|e| R2rmlError::Materialization(format!("sort run write {tag}: {e}")))?;
        }
        w.flush()
            .map_err(|e| R2rmlError::Materialization(format!("sort run flush {tag}: {e}")))?;
        buf.clear();
        runs.push(run_path);
        Ok(())
    };

    loop {
        line.clear();
        let n = reader
            .read_line(&mut line)
            .map_err(|e| R2rmlError::Materialization(format!("sort read {tag}: {e}")))?;
        if n == 0 {
            break;
        }
        buf.push(line.trim_end_matches('\n').to_string());
        if buf.len() >= SORT_RUN_LINES {
            flush_run(&mut buf, &mut runs)?;
        }
    }
    flush_run(&mut buf, &mut runs)?;

    let out = dir.join(format!("{tag}.sorted"));
    kway_merge_dedup(&runs, &out, tag)?;
    Ok(out)
}

/// k-way merge sorted, de-duped runs into one sorted, de-duped file. Holds one
/// front line per run resident (O(runs)); the run count is bounded by input size /
/// [`SORT_RUN_LINES`].
fn kway_merge_dedup(runs: &[PathBuf], out: &Path, tag: &str) -> Result<(), MaterializeError> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use std::io::{BufRead, BufReader, BufWriter, Write as _};

    let mut readers: Vec<BufReader<std::fs::File>> = Vec::with_capacity(runs.len());
    for r in runs {
        readers.push(BufReader::new(std::fs::File::open(r).map_err(|e| {
            R2rmlError::Materialization(format!("merge open {tag}: {e}"))
        })?));
    }
    // Heap of (line, reader_idx); pop the smallest, dedup, refill from that reader.
    let mut heap: BinaryHeap<Reverse<(String, usize)>> = BinaryHeap::new();
    let read_next = |r: &mut BufReader<std::fs::File>| -> Result<Option<String>, MaterializeError> {
        let mut l = String::new();
        let n = r
            .read_line(&mut l)
            .map_err(|e| R2rmlError::Materialization(format!("merge read {tag}: {e}")))?;
        Ok(if n == 0 {
            None
        } else {
            Some(l.trim_end_matches('\n').to_string())
        })
    };
    for (idx, r) in readers.iter_mut().enumerate() {
        if let Some(l) = read_next(r)? {
            heap.push(Reverse((l, idx)));
        }
    }
    let mut w = BufWriter::new(
        std::fs::File::create(out)
            .map_err(|e| R2rmlError::Materialization(format!("merge create {tag}: {e}")))?,
    );
    let mut last: Option<String> = None;
    while let Some(Reverse((l, idx))) = heap.pop() {
        if last.as_deref() != Some(l.as_str()) {
            writeln!(w, "{l}")
                .map_err(|e| R2rmlError::Materialization(format!("merge write {tag}: {e}")))?;
            last = Some(l.clone());
        }
        if let Some(next) = read_next(&mut readers[idx])? {
            heap.push(Reverse((next, idx)));
        }
    }
    w.flush()
        .map_err(|e| R2rmlError::Materialization(format!("merge flush {tag}: {e}")))?;
    Ok(())
}

/// Stream a symmetric diff of two sorted, de-duped line files, returning
/// `(missing_in_twin, extra_in_twin)`. O(1) resident (one front line per side).
fn diff_sorted_files(source: &Path, twin: &Path) -> Result<(usize, usize), MaterializeError> {
    use std::cmp::Ordering;
    use std::io::{BufRead, BufReader};

    let mut a = BufReader::new(
        std::fs::File::open(source)
            .map_err(|e| R2rmlError::Materialization(format!("diff open source: {e}")))?,
    );
    let mut b = BufReader::new(
        std::fs::File::open(twin)
            .map_err(|e| R2rmlError::Materialization(format!("diff open twin: {e}")))?,
    );
    let next = |r: &mut BufReader<std::fs::File>| -> Result<Option<String>, MaterializeError> {
        let mut l = String::new();
        let n = r
            .read_line(&mut l)
            .map_err(|e| R2rmlError::Materialization(format!("diff read: {e}")))?;
        Ok(if n == 0 {
            None
        } else {
            Some(l.trim_end_matches('\n').to_string())
        })
    };
    let (mut missing, mut extra) = (0usize, 0usize);
    let mut sa = next(&mut a)?;
    let mut sb = next(&mut b)?;
    loop {
        match (&sa, &sb) {
            (Some(x), Some(y)) => match x.cmp(y) {
                Ordering::Equal => {
                    sa = next(&mut a)?;
                    sb = next(&mut b)?;
                }
                Ordering::Less => {
                    missing += 1; // in source, not twin
                    sa = next(&mut a)?;
                }
                Ordering::Greater => {
                    extra += 1; // in twin, not source
                    sb = next(&mut b)?;
                }
            },
            (Some(_), None) => {
                missing += 1;
                sa = next(&mut a)?;
            }
            (None, Some(_)) => {
                extra += 1;
                sb = next(&mut b)?;
            }
            (None, None) => break,
        }
    }
    Ok((missing, extra))
}

/// A filesystem-safe fragment of a graph-source id for a temp path.
fn sanitize_tmp(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Best-effort cleanup of the verify temp dir on scope exit (including on error).
struct TmpDirGuard(PathBuf);
impl Drop for TmpDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Render one SPARQL-JSON result term to the N-Triples form the enumerator emits.
fn term_to_ntriples(binding: &serde_json::Value) -> String {
    let value = binding["value"].as_str().unwrap_or_default();
    match binding["type"].as_str().unwrap_or_default() {
        "uri" => format!("<{value}>"),
        "bnode" => format!("_:{value}"),
        _ => {
            let escaped = escape_nt_literal(value);
            if let Some(lang) = binding["xml:lang"].as_str() {
                format!("\"{escaped}\"@{lang}")
            } else {
                match binding["datatype"].as_str() {
                    Some(dt) if dt != fluree_vocab::xsd::STRING => {
                        format!("\"{escaped}\"^^<{dt}>")
                    }
                    // xsd:string is the implicit datatype — rendered bare, matching
                    // the enumerator (a plain literal has no datatype suffix).
                    _ => format!("\"{escaped}\""),
                }
            }
        }
    }
}

/// Escape a literal lexical form per N-Triples, matching the enumerator's
/// `escape_literal` so the two sides render byte-identically.
fn escape_nt_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

/// Canonicalize a typed object literal of an N-Triple so two value-equal but
/// lexically-different forms compare equal — e.g. the source's `"5.00"^^decimal`
/// vs the twin's `"5"^^decimal`, or `"…T…:42.000000Z"^^dateTime` vs `"…T…:42Z"`.
/// Only a typed NUMERIC or TEMPORAL object is touched; IRIs, plain/lang literals,
/// and other typed literals pass through byte-for-byte. Applied to BOTH sides, so
/// whichever canonical form each produces, they agree.
fn canonicalize_value_nt(triple: &str) -> String {
    // `<s> <p> OBJ .` — subject/predicate are `<…>`/`_:…` with no spaces, so the
    // first two spaces delimit them; OBJ (which may contain spaces inside quotes)
    // is the remainder minus the trailing ` .`.
    let mut it = triple.splitn(3, ' ');
    let (Some(s), Some(p), Some(rest)) = (it.next(), it.next(), it.next()) else {
        return triple.to_string();
    };
    let obj = rest.strip_suffix(" .").unwrap_or(rest);
    let Some(caret) = obj.rfind("\"^^<") else {
        return triple.to_string();
    };
    if !obj.starts_with('"') || !obj.ends_with('>') {
        return triple.to_string();
    }
    let lex = &obj[1..caret];
    let dt = &obj[caret + 4..obj.len() - 1];
    let local = dt.rsplit(['#', '/']).next().unwrap_or(dt);
    let canon = if is_numeric_datatype(dt) {
        normalize_numeric_lexical(lex, local)
    } else if is_temporal_datatype(local) {
        normalize_temporal_lexical(lex)
    } else {
        return triple.to_string();
    };
    format!("{s} {p} \"{canon}\"^^<{dt}> .")
}

/// Value-canonical lexical form of a numeric literal, so two lexically-different
/// but value-equal encodings compare equal. THE decimal false-reject A2 caught:
/// Fluree stores `xsd:double` in XSD E-notation (`"-1.5521762E1"`) while the verify
/// oracle re-renders the raw Arrow `Float64` via `f64::to_string()` in plain form
/// (`"-15.521762"`) — the OLD trim passed E-notation through unchanged, so a correct
/// twin was dropped (`term.rs:512` receipt). Fix: for `double`/`float`, parse to
/// `f64` and re-render Rust's shortest round-trip form (never E-notation), so both
/// encodings collapse to it. `decimal` and the integer family use EXACT lexical
/// canonicalization (no float round-trip, so arbitrary-precision decimals are never
/// compared lossily): strip a leading `+` and an insignificant fractional tail.
/// Applied to BOTH sides.
fn normalize_numeric_lexical(lex: &str, dt_local: &str) -> String {
    match dt_local {
        "double" | "float" => match lex.trim().parse::<f64>() {
            // NaN/Inf render as-is; only finite values get the canonical form.
            Ok(f) if f.is_finite() => canonical_f64(f),
            _ => lex.to_string(),
        },
        _ => trim_decimal_lexical(lex),
    }
}

/// Rust's `Display` for `f64` never uses scientific notation, so it is a stable
/// canonical form both `"1.5E1"` and `"15"` collapse to. `-0.0` is normalized to
/// `"0"` so the two zero signs compare equal.
fn canonical_f64(f: f64) -> String {
    if f == 0.0 {
        "0".to_string()
    } else {
        format!("{f}")
    }
}

/// Exact lexical canonicalization for decimal / integer forms: strip a leading `+`
/// and drop insignificant trailing fractional zeros (`"5.00"` → `"5"`,
/// `"9.90"` → `"9.9"`). No float round-trip, so precision is preserved.
fn trim_decimal_lexical(lex: &str) -> String {
    let lex = lex.strip_prefix('+').unwrap_or(lex);
    match lex.split_once('.') {
        Some((int, frac)) => {
            let frac = frac.trim_end_matches('0');
            if frac.is_empty() {
                int.to_string()
            } else {
                format!("{int}.{frac}")
            }
        }
        None => lex.to_string(),
    }
}

/// Whether an XSD datatype IRI names a numeric type (whose lexical form admits
/// value-preserving normalization).
fn is_numeric_datatype(dt: &str) -> bool {
    matches!(
        dt.rsplit(['#', '/']).next().unwrap_or(dt),
        "decimal"
            | "double"
            | "float"
            | "integer"
            | "long"
            | "int"
            | "short"
            | "byte"
            | "nonNegativeInteger"
            | "nonPositiveInteger"
            | "negativeInteger"
            | "positiveInteger"
            | "unsignedLong"
            | "unsignedInt"
            | "unsignedShort"
            | "unsignedByte"
    )
}

/// Whether an XSD datatype LOCAL NAME names a temporal type whose lexical form
/// admits value-preserving normalization (fractional-second padding + timezone).
fn is_temporal_datatype(local: &str) -> bool {
    matches!(local, "dateTime" | "dateTimeStamp" | "date" | "time")
}

/// Value-canonical lexical form of a date/time literal, so two lexically-different
/// but value-equal encodings compare equal. THE dateTime false-reject the 35M
/// acceptance run caught: Fluree stores/exports whole-second timestamps
/// microsecond-padded (`"…T00:16:42.000000Z"`) while the verify oracle's
/// `format_timestamp` (r2rml term.rs) DROPS the fraction when it is zero
/// (`"…T00:16:42Z"`) — the numeric canonicalizer never touched dateTime, so a
/// PROVEN-identical twin was dropped. Fix (mirrors the decimal trailing-zero
/// strip): drop insignificant trailing fractional-second zeros (and a bare `.`),
/// and normalize a zero UTC offset (`+00:00`/`-00:00`) or `Z` to canonical `Z`.
/// A non-zero offset is left as-is (it denotes a different instant without
/// date arithmetic; this schema is all-UTC — a documented residual). Applied to
/// BOTH sides.
fn normalize_temporal_lexical(lex: &str) -> String {
    let (body, tz) = split_timezone(lex);
    let tz_norm = match tz {
        Some("Z" | "+00:00" | "-00:00") => "Z",
        Some(other) => other,
        None => "",
    };
    let body_norm = strip_fraction_zeros(body);
    format!("{body_norm}{tz_norm}")
}

/// Split a trailing timezone designator (`Z` or `(+|-)HH:MM`) off a temporal
/// lexical form. The date part uses `-` separators, but a real offset has `:` at
/// the sign+3 position, which `-MM-DD` never does — so a bare `xsd:date` keeps its
/// whole body.
fn split_timezone(lex: &str) -> (&str, Option<&str>) {
    if let Some(body) = lex.strip_suffix('Z') {
        return (body, Some("Z"));
    }
    if lex.len() >= 6 {
        let tz = &lex[lex.len() - 6..];
        let b = tz.as_bytes();
        if (b[0] == b'+' || b[0] == b'-')
            && b[3] == b':'
            && tz[1..3].bytes().all(|c| c.is_ascii_digit())
            && tz[4..6].bytes().all(|c| c.is_ascii_digit())
        {
            return (&lex[..lex.len() - 6], Some(tz));
        }
    }
    (lex, None)
}

/// Drop insignificant trailing zeros from the fractional-seconds group (and a
/// bare trailing `.`). The date uses `-` and the time uses `:`, so the only `.`
/// in a temporal body is the fractional-second separator.
fn strip_fraction_zeros(body: &str) -> String {
    match body.rsplit_once('.') {
        Some((head, frac)) if !frac.is_empty() && frac.bytes().all(|c| c.is_ascii_digit()) => {
            let trimmed = frac.trim_end_matches('0');
            if trimmed.is_empty() {
                head.to_string()
            } else {
                format!("{head}.{trimmed}")
            }
        }
        _ => body.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::literal_sink_args;
    use fluree_db_transact::import::ParsedChunk;
    use fluree_vocab::UnresolvedDatatypeConstraint as Dtc;
    use std::collections::HashMap;
    use std::sync::Arc;

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
    fn stamp_over_64kib_cap_fails_loud() {
        use fluree_db_query::r2rml::TableWatermark;
        use std::collections::HashMap;

        // A watermark that blows the 64 KiB txn-meta cap: enough tables, each with a
        // realistic-length S3 metadata_location, that the serialized JSON exceeds it.
        // encode_stamp bypasses the parser-path txn-meta validators, so this must be
        // caught fail-loud rather than fail opaquely at commit-encode time (item #4).
        let long_loc = format!(
            "s3://warehouse/db/schema/table/metadata/{}.metadata.json",
            "0".repeat(300)
        );
        let mut tables = HashMap::new();
        for i in 0..300 {
            tables.insert(
                format!("dw.table_{i}"),
                TableWatermark {
                    metadata_location: long_loc.clone(),
                    snapshot_id: Some(i as i64),
                    sequence_number: Some(i as i64),
                },
            );
        }
        let stamp = super::WatermarkStamp {
            builder_version: "fluree-materialize/0.0.0".into(),
            mapping_hash: "deadbeef".into(),
            tables,
            sample_seed: 0,
            dup_parent_keys: HashMap::new(),
        };
        match super::stamp_watermark_within_cap(&stamp) {
            Err(super::MaterializeError::StampTooLarge {
                actual_bytes,
                cap_bytes,
                table_count,
            }) => {
                assert_eq!(cap_bytes, super::MAX_TXN_META_BYTES);
                assert_eq!(
                    table_count, 300,
                    "the table count drives the failure message"
                );
                assert!(
                    actual_bytes > cap_bytes,
                    "the reported stamp size ({actual_bytes}) must exceed the cap ({cap_bytes})"
                );
            }
            other => panic!("an over-cap stamp must fail loud with StampTooLarge, got {other:?}"),
        }

        // A modest two-table stamp fits comfortably (no false positive).
        let mut small = HashMap::new();
        small.insert(
            "dw.a".to_string(),
            TableWatermark {
                metadata_location: "s3://m/a.metadata.json".into(),
                snapshot_id: Some(1),
                sequence_number: Some(1),
            },
        );
        small.insert(
            "dw.b".to_string(),
            TableWatermark {
                metadata_location: "s3://m/b.metadata.json".into(),
                snapshot_id: Some(2),
                sequence_number: Some(2),
            },
        );
        let stamp = super::WatermarkStamp {
            builder_version: "fluree-materialize/0.0.0".into(),
            mapping_hash: "abc123".into(),
            tables: small,
            sample_seed: 7,
            dup_parent_keys: HashMap::new(),
        };
        assert!(
            super::stamp_watermark_within_cap(&stamp).is_ok(),
            "a small stamp must fit the 64 KiB cap"
        );
    }

    #[test]
    fn read_stamp_requires_materialize_namespace() {
        use fluree_db_core::{TxnMetaEntry, TxnMetaValue};
        use fluree_db_query::r2rml::TableWatermark;
        use std::collections::{BTreeMap, HashMap};

        // A well-formed stamp payload — the same local names + value types encode_stamp writes.
        let watermark = {
            let mut t: BTreeMap<&str, TableWatermark> = BTreeMap::new();
            t.insert(
                "dw.a",
                TableWatermark {
                    metadata_location: "s3://m/a.metadata.json".into(),
                    snapshot_id: Some(1),
                    sequence_number: Some(1),
                },
            );
            serde_json::to_string(&t).unwrap()
        };
        let make_entries = |ns: u16| {
            vec![
                TxnMetaEntry::new(
                    ns,
                    super::STAMP_PRED_BUILDER,
                    TxnMetaValue::String("fluree-materialize/0.0.0".into()),
                ),
                TxnMetaEntry::new(
                    ns,
                    super::STAMP_PRED_MAPPING_HASH,
                    TxnMetaValue::String("abc123def456".into()),
                ),
                TxnMetaEntry::new(
                    ns,
                    super::STAMP_PRED_WATERMARK,
                    TxnMetaValue::String(watermark.clone()),
                ),
                TxnMetaEntry::new(ns, super::STAMP_PRED_SAMPLE_SEED, TxnMetaValue::Long(42)),
            ]
        };

        // 1) Predicates whose ns code resolves to MATERIALIZE_NS parse as a stamp.
        let good_code = 100u16;
        let mut good_delta = HashMap::new();
        good_delta.insert(good_code, super::MATERIALIZE_NS.to_string());
        let stamp = super::read_stamp(&make_entries(good_code), &good_delta)
            .expect("a stamp in MATERIALIZE_NS must parse");
        assert_eq!(stamp.mapping_hash, "abc123def456");
        assert_eq!(stamp.sample_seed, 42);
        assert_eq!(stamp.tables.len(), 1);

        // 2) The SAME local names + values in a FOREIGN namespace must NOT parse —
        //    the local names alone are not a sufficient signature (item #5).
        let forged_code = 200u16;
        let mut forged_delta = HashMap::new();
        forged_delta.insert(forged_code, "https://evil.example/materialize#".to_string());
        assert!(
            super::read_stamp(&make_entries(forged_code), &forged_delta).is_none(),
            "a forged stamp carrying the stamp local names in a foreign namespace must be rejected"
        );

        // 3) An unresolvable ns code (absent from the delta) is likewise not a stamp.
        assert!(
            super::read_stamp(&make_entries(300), &HashMap::new()).is_none(),
            "an unresolved ns code must not parse as a stamp"
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

    // --- O2 verify: decimal/double value-canonicalization (the A2 false-reject) ---

    #[test]
    fn double_enotation_and_plain_canonicalize_equal() {
        // THE A2 receipt: Fluree stores xsd:double as E-notation; the enumerator
        // oracle renders the raw Arrow Float64 as plain. A correct twin must NOT be
        // rejected because the two encodings differ lexically.
        let dt = "http://www.w3.org/2001/XMLSchema#double";
        let enot = super::canonicalize_value_nt(&format!(
            "<http://ex/geography/3683> <http://ex/latitude> \"-1.5521762E1\"^^<{dt}> ."
        ));
        let plain = super::canonicalize_value_nt(&format!(
            "<http://ex/geography/3683> <http://ex/latitude> \"-15.521762\"^^<{dt}> ."
        ));
        assert_eq!(
            enot, plain,
            "E-notation and plain double must canonicalize equal"
        );

        let big_enot = super::canonicalize_value_nt(&format!(
            "<http://ex/account/1269> <http://ex/rev> \"2.8897281511E8\"^^<{dt}> ."
        ));
        // 2.8897281511E8 == 288972815.11 (the plain form the enumerator renders).
        let big_plain = super::canonicalize_value_nt(&format!(
            "<http://ex/account/1269> <http://ex/rev> \"288972815.11\"^^<{dt}> ."
        ));
        assert_eq!(
            big_enot, big_plain,
            "large-magnitude double forms must agree"
        );
    }

    #[test]
    fn decimal_trailing_zeros_canonicalize_equal() {
        let dt = "http://www.w3.org/2001/XMLSchema#decimal";
        let a = super::canonicalize_value_nt(&format!("<s> <p> \"5.00\"^^<{dt}> ."));
        let b = super::canonicalize_value_nt(&format!("<s> <p> \"5\"^^<{dt}> ."));
        assert_eq!(a, b, "'5.00' and '5' decimals must canonicalize equal");
        let c = super::canonicalize_value_nt(&format!("<s> <p> \"9.90\"^^<{dt}> ."));
        let d = super::canonicalize_value_nt(&format!("<s> <p> \"9.9\"^^<{dt}> ."));
        assert_eq!(c, d, "'9.90' and '9.9' decimals must canonicalize equal");
    }

    #[test]
    fn decimal_canonicalization_preserves_precision() {
        // A decimal with more digits than f64 can hold must NOT be routed through
        // the float round-trip (that path is double/float only), so two genuinely
        // different high-precision decimals stay distinct.
        let dt = "http://www.w3.org/2001/XMLSchema#decimal";
        let a =
            super::canonicalize_value_nt(&format!("<s> <p> \"123456789012345678.1\"^^<{dt}> ."));
        let b =
            super::canonicalize_value_nt(&format!("<s> <p> \"123456789012345678.2\"^^<{dt}> ."));
        assert_ne!(a, b, "distinct high-precision decimals must not collapse");
    }

    #[test]
    fn datetime_microsecond_padding_canonicalizes_equal() {
        // THE 35M-run receipt: storage pads whole-second timestamps to 6-digit
        // microseconds (".000000Z"); the enumerator oracle drops a zero fraction.
        // A correct twin must NOT be rejected for that.
        let dt = "http://www.w3.org/2001/XMLSchema#dateTime";
        let padded = super::canonicalize_value_nt(&format!(
            "<http://ex/e/1> <http://ex/ts> \"2026-06-28T00:16:42.000000Z\"^^<{dt}> ."
        ));
        let unpadded = super::canonicalize_value_nt(&format!(
            "<http://ex/e/1> <http://ex/ts> \"2026-06-28T00:16:42Z\"^^<{dt}> ."
        ));
        assert_eq!(
            padded, unpadded,
            "padded and unpadded whole-second dateTime must agree"
        );
        // A non-zero sub-second fraction is preserved but its padding equalized.
        let a = super::canonicalize_value_nt(&format!(
            "<s> <p> \"2026-06-28T00:16:42.500000Z\"^^<{dt}> ."
        ));
        let b =
            super::canonicalize_value_nt(&format!("<s> <p> \"2026-06-28T00:16:42.5Z\"^^<{dt}> ."));
        assert_eq!(a, b, "'.500000' and '.5' fractions must agree");
        // Distinct instants must NOT collapse.
        let x =
            super::canonicalize_value_nt(&format!("<s> <p> \"2026-06-28T00:16:42.5Z\"^^<{dt}> ."));
        let y =
            super::canonicalize_value_nt(&format!("<s> <p> \"2026-06-28T00:16:43Z\"^^<{dt}> ."));
        assert_ne!(x, y, "different instants must stay distinct");
    }

    #[test]
    fn datetime_zulu_and_zero_offset_canonicalize_equal() {
        let dt = "http://www.w3.org/2001/XMLSchema#dateTime";
        let zulu =
            super::canonicalize_value_nt(&format!("<s> <p> \"2026-06-28T00:16:42Z\"^^<{dt}> ."));
        let offset = super::canonicalize_value_nt(&format!(
            "<s> <p> \"2026-06-28T00:16:42+00:00\"^^<{dt}> ."
        ));
        assert_eq!(zulu, offset, "'Z' and '+00:00' must canonicalize equal");
        // A genuinely different (non-UTC) offset is left intact, so it does not
        // silently collapse into the UTC form.
        let nonzero = super::canonicalize_value_nt(&format!(
            "<s> <p> \"2026-06-28T00:16:42+05:00\"^^<{dt}> ."
        ));
        assert_ne!(zulu, nonzero, "a non-zero offset must not collapse to Z");
    }

    #[test]
    fn non_numeric_and_plain_literals_pass_through() {
        let s = "<s> <p> \"hello world\" .";
        assert_eq!(
            super::canonicalize_value_nt(s),
            s,
            "plain literal untouched"
        );
        // A bare date (no fractional seconds, no offset) normalizes to itself.
        let dated = "<s> <p> \"2020-01-01\"^^<http://www.w3.org/2001/XMLSchema#date> .";
        assert_eq!(
            super::canonicalize_value_nt(dated),
            dated,
            "whole date normalizes to itself"
        );
        let iri = "<s> <p> <http://ex/o> .";
        assert_eq!(
            super::canonicalize_value_nt(iri),
            iri,
            "IRI object untouched"
        );
    }

    #[test]
    fn seeded_offset_is_in_bounds() {
        // Never offsets past the last full window; count<=k always starts at 0.
        assert_eq!(super::seeded_offset(999, 2, 3), 0, "count<=k → offset 0");
        for seed in [0u64, 1, 7, 12345, u64::MAX] {
            let off = super::seeded_offset(seed, 100, 3);
            assert!(off <= 97, "offset {off} must leave room for k subjects");
        }
    }

    // --- O6 budget honesty: FK parent-index accounting ---

    #[test]
    fn parent_index_budget_is_half_the_import_budget() {
        assert_eq!(
            super::parent_index_budget_bytes(0),
            0,
            "an unknown/auto budget disables the guard"
        );
        let gib = 1024 * 1024 * 1024;
        assert_eq!(super::parent_index_budget_bytes(gib), gib / 2);
    }

    #[test]
    fn parent_index_budget_guard_passes_empty_and_disabled() {
        use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
        use fluree_db_r2rml::materialize::ParentIndexSet;
        let parents = ParentIndexSet::new(&CompiledR2rmlMapping::default()).unwrap();
        // budget 0 → guard off.
        assert!(super::check_parent_index_budget(&parents, 0).is_ok());
        // empty index (0 bytes) under a positive budget passes.
        assert!(super::check_parent_index_budget(&parents, 1024).is_ok());
    }

    // --- O2 verify: bounded external sort + streaming diff ---

    #[test]
    fn external_sort_sorts_and_dedups() {
        let dir = std::env::temp_dir().join(format!("fluree-sort-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("in.nt");
        std::fs::write(&input, "c\na\nb\na\nc\n").unwrap();
        let out = super::external_sort_lines(&input, &dir, "t").unwrap();
        let sorted = std::fs::read_to_string(&out).unwrap();
        assert_eq!(sorted, "a\nb\nc\n", "sorted, de-duplicated");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn diff_sorted_files_counts_symmetric_difference() {
        let dir = std::env::temp_dir().join(format!("fluree-diff-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // source has a,b,c,d ; twin has b,c,e → missing_in_twin {a,d}=2, extra {e}=1.
        let src = dir.join("s.nt");
        let twin = dir.join("t.nt");
        std::fs::write(&src, "a\nb\nc\nd\n").unwrap();
        std::fs::write(&twin, "b\nc\ne\n").unwrap();
        let (missing, extra) = super::diff_sorted_files(&src, &twin).unwrap();
        assert_eq!((missing, extra), (2, 1), "symmetric difference counted");
        // Identical files → zero diff.
        let (m2, e2) = super::diff_sorted_files(&src, &src).unwrap();
        assert_eq!((m2, e2), (0, 0), "identical inputs diff to zero");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- O1 parallel produce: p=1 vs p=2 differential (hermetic) ---

    /// In-memory build provider serving a fixed mapping + batches (no Iceberg /
    /// catalog), so `drive_virtual_import` can be driven hermetically at different
    /// parallelisms and the outputs compared.
    #[derive(Debug)]
    struct MockBuildProvider {
        mapping: Arc<fluree_db_r2rml::mapping::CompiledR2rmlMapping>,
        batches: HashMap<String, Vec<fluree_db_tabular::ColumnBatch>>,
    }

    #[async_trait::async_trait]
    impl fluree_db_query::r2rml::R2rmlProvider for MockBuildProvider {
        async fn has_r2rml_mapping(&self, _graph_source_id: &str) -> bool {
            true
        }
        async fn compiled_mapping(
            &self,
            _graph_source_id: &str,
            _as_of_t: Option<i64>,
        ) -> fluree_db_query::error::Result<Arc<fluree_db_r2rml::mapping::CompiledR2rmlMapping>>
        {
            Ok(Arc::clone(&self.mapping))
        }
        fn build_watermark(
            &self,
            _graph_source_id: &str,
        ) -> HashMap<String, fluree_db_query::r2rml::TableWatermark> {
            // Non-empty so the completion-stamp guard passes (one pin per table).
            self.batches
                .keys()
                .map(|t| {
                    (
                        t.clone(),
                        fluree_db_query::r2rml::TableWatermark {
                            metadata_location: format!("mock://{t}/metadata.json"),
                            snapshot_id: Some(1),
                            sequence_number: Some(1),
                        },
                    )
                })
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl fluree_db_query::r2rml::R2rmlTableProvider for MockBuildProvider {
        async fn scan_table(
            &self,
            _graph_source_id: &str,
            table_name: &str,
            _projection: &[String],
            _filters: &[fluree_db_query::r2rml::ScanFilter],
            _topk: Option<&fluree_db_query::r2rml::ScanTopK>,
            _as_of_t: Option<i64>,
        ) -> fluree_db_query::error::Result<fluree_db_query::r2rml::ColumnBatchStream> {
            let batches: Vec<fluree_db_query::error::Result<fluree_db_tabular::ColumnBatch>> = self
                .batches
                .get(table_name)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(Ok)
                .collect();
            Ok(Box::pin(futures::stream::iter(batches)))
        }
    }

    /// A small star-schema fixture (Customer parent + Order child via a
    /// RefObjectMap), split into multiple batches so a parallel run distributes
    /// work across workers.
    fn star_fixture() -> MockBuildProvider {
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            SubjectMap, TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, Column, FieldInfo, FieldType};

        fn field(name: &str, ty: FieldType, id: i32) -> FieldInfo {
            FieldInfo {
                name: name.to_string(),
                field_type: ty,
                nullable: true,
                field_id: id,
            }
        }
        fn s(v: &str) -> Option<String> {
            Some(v.to_string())
        }

        let mut customer = TriplesMap::new("<#Customer>", "cust");
        customer.subject_map =
            SubjectMap::template("http://ex/c/{c_key}").with_class("http://ex/Customer");
        customer.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/name"),
            object_map: ObjectMap::column("name"),
        }];

        let mut order = TriplesMap::new("<#Order>", "ord");
        order.subject_map =
            SubjectMap::template("http://ex/o/{o_key}").with_class("http://ex/Order");
        order.predicate_object_maps = vec![
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/amount"),
                object_map: ObjectMap::column_typed(
                    "amount",
                    "http://www.w3.org/2001/XMLSchema#decimal",
                ),
            },
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex/placedBy"),
                object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                    "<#Customer>",
                    "cust_key",
                    "c_key",
                )),
            },
        ];

        let cust_schema = std::sync::Arc::new(BatchSchema::new(vec![
            field("c_key", FieldType::Int64, 1),
            field("name", FieldType::String, 2),
        ]));
        let cust_batch = fluree_db_tabular::ColumnBatch::new(
            cust_schema,
            vec![
                Column::Int64(vec![Some(10), Some(20), Some(30), Some(40)]),
                Column::String(vec![s("Acme"), s("Globex"), s("Initech"), s("Umbrella")]),
            ],
        )
        .unwrap();

        let ord_schema = std::sync::Arc::new(BatchSchema::new(vec![
            field("o_key", FieldType::Int64, 1),
            field("amount", FieldType::String, 2),
            field("cust_key", FieldType::Int64, 3),
        ]));
        let ord_batch = |keys: &[i64], amts: &[&str], custs: &[i64]| {
            fluree_db_tabular::ColumnBatch::new(
                ord_schema.clone(),
                vec![
                    Column::Int64(keys.iter().map(|k| Some(*k)).collect()),
                    Column::String(amts.iter().map(|a| s(a)).collect()),
                    Column::Int64(custs.iter().map(|c| Some(*c)).collect()),
                ],
            )
            .unwrap()
        };

        let mut batches = HashMap::new();
        batches.insert("cust".to_string(), vec![cust_batch]);
        // Two batches for the child, so a parallel run has multiple work items.
        batches.insert(
            "ord".to_string(),
            vec![
                ord_batch(
                    &[1, 2, 3, 4],
                    &["9.99", "5.00", "1.00", "2.50"],
                    &[10, 20, 30, 40],
                ),
                ord_batch(
                    &[5, 6, 7, 8],
                    &["3.14", "2.72", "1.41", "1.61"],
                    &[10, 20, 30, 99],
                ),
            ],
        );

        MockBuildProvider {
            mapping: Arc::new(CompiledR2rmlMapping::new(vec![customer, order])),
            batches,
        }
    }

    /// A two-level chain Region → Country → City: Country is BOTH a foreign-key
    /// child (of Region) and a parent (of City), so O5(c) must schedule it in its
    /// own wave between them and build its index DURING its emit scan for City to
    /// resolve. City is split into two batches so a parallel run distributes work.
    fn chain_fixture() -> MockBuildProvider {
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            SubjectMap, TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, Column, FieldInfo, FieldType};

        fn field(name: &str, ty: FieldType, id: i32) -> FieldInfo {
            FieldInfo {
                name: name.to_string(),
                field_type: ty,
                nullable: true,
                field_id: id,
            }
        }

        let mut region = TriplesMap::new("<#Region>", "region");
        region.subject_map =
            SubjectMap::template("http://ex/region/{r_key}").with_class("http://ex/Region");

        let mut country = TriplesMap::new("<#Country>", "country");
        country.subject_map =
            SubjectMap::template("http://ex/country/{c_key}").with_class("http://ex/Country");
        country.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/inRegion"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                "<#Region>",
                "region_key",
                "r_key",
            )),
        }];

        let mut city = TriplesMap::new("<#City>", "city");
        city.subject_map =
            SubjectMap::template("http://ex/city/{y_key}").with_class("http://ex/City");
        city.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/inCountry"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                "<#Country>",
                "country_key",
                "c_key",
            )),
        }];

        let region_schema =
            std::sync::Arc::new(BatchSchema::new(vec![field("r_key", FieldType::Int64, 1)]));
        let region_batch = fluree_db_tabular::ColumnBatch::new(
            region_schema,
            vec![Column::Int64(vec![Some(1), Some(2)])],
        )
        .unwrap();

        let country_schema = std::sync::Arc::new(BatchSchema::new(vec![
            field("c_key", FieldType::Int64, 1),
            field("region_key", FieldType::Int64, 2),
        ]));
        let country_batch = fluree_db_tabular::ColumnBatch::new(
            country_schema,
            vec![
                Column::Int64(vec![Some(10), Some(20)]),
                Column::Int64(vec![Some(1), Some(9)]), // 9 dangling
            ],
        )
        .unwrap();

        let city_schema = std::sync::Arc::new(BatchSchema::new(vec![
            field("y_key", FieldType::Int64, 1),
            field("country_key", FieldType::Int64, 2),
        ]));
        let city_batch = |keys: &[i64], countries: &[i64]| {
            fluree_db_tabular::ColumnBatch::new(
                city_schema.clone(),
                vec![
                    Column::Int64(keys.iter().map(|k| Some(*k)).collect()),
                    Column::Int64(countries.iter().map(|c| Some(*c)).collect()),
                ],
            )
            .unwrap()
        };

        let mut batches = HashMap::new();
        batches.insert("region".to_string(), vec![region_batch]);
        batches.insert("country".to_string(), vec![country_batch]);
        batches.insert(
            "city".to_string(),
            vec![
                city_batch(&[100, 200], &[10, 20]),
                city_batch(&[300, 400], &[10, 99]), // 99 dangling
            ],
        );

        MockBuildProvider {
            mapping: Arc::new(CompiledR2rmlMapping::new(vec![city, country, region])),
            batches,
        }
    }

    /// A mutual foreign-key cycle A ↔ B: both TriplesMaps land in the `preindex`
    /// fallback (they cannot be indexed in a single forward pass), so O5(c) must
    /// pre-index both up front and then emit them; this exercises the fallback path
    /// in the real parallel driver.
    fn cyclic_fixture() -> MockBuildProvider {
        use fluree_db_r2rml::mapping::{
            CompiledR2rmlMapping, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
            SubjectMap, TriplesMap,
        };
        use fluree_db_tabular::{BatchSchema, Column, FieldInfo, FieldType};

        fn field(name: &str, ty: FieldType, id: i32) -> FieldInfo {
            FieldInfo {
                name: name.to_string(),
                field_type: ty,
                nullable: true,
                field_id: id,
            }
        }

        let mut a = TriplesMap::new("<#A>", "a");
        a.subject_map = SubjectMap::template("http://ex/a/{id}").with_class("http://ex/A");
        a.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/toB"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new("<#B>", "b_ref", "id")),
        }];
        let mut b = TriplesMap::new("<#B>", "b");
        b.subject_map = SubjectMap::template("http://ex/b/{id}").with_class("http://ex/B");
        b.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/toA"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new("<#A>", "a_ref", "id")),
        }];

        let a_schema = std::sync::Arc::new(BatchSchema::new(vec![
            field("id", FieldType::Int64, 1),
            field("b_ref", FieldType::Int64, 2),
        ]));
        let a_batch = fluree_db_tabular::ColumnBatch::new(
            a_schema,
            vec![
                Column::Int64(vec![Some(1), Some(2)]),
                Column::Int64(vec![Some(2), None]), // A/1 → B/2; A/2 no ref
            ],
        )
        .unwrap();
        let b_schema = std::sync::Arc::new(BatchSchema::new(vec![
            field("id", FieldType::Int64, 1),
            field("a_ref", FieldType::Int64, 2),
        ]));
        let b_batch = fluree_db_tabular::ColumnBatch::new(
            b_schema,
            vec![
                Column::Int64(vec![Some(2)]),
                Column::Int64(vec![Some(1)]), // B/2 → A/1
            ],
        )
        .unwrap();

        let mut batches = HashMap::new();
        batches.insert("a".to_string(), vec![a_batch]);
        batches.insert("b".to_string(), vec![b_batch]);

        MockBuildProvider {
            mapping: Arc::new(CompiledR2rmlMapping::new(vec![a, b])),
            batches,
        }
    }

    /// Drive one build at `parallelism`, returning the aggregate stats and every
    /// emitted `(idx, ParsedChunk)`.
    fn run_build(
        provider: &MockBuildProvider,
        parallelism: usize,
    ) -> (
        super::MaterializeStats,
        Vec<(usize, ParsedChunk)>,
        Arc<fluree_db_transact::SharedNamespaceAllocator>,
    ) {
        use fluree_db_transact::namespace::{NamespaceRegistry, SharedNamespaceAllocator};

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let shared_alloc = Arc::new(SharedNamespaceAllocator::from_registry(
            &NamespaceRegistry::new(),
        ));
        let spool_dir = std::env::temp_dir().join(format!(
            "fluree-o1-test-{}-{}",
            std::process::id(),
            parallelism
        ));
        std::fs::create_dir_all(&spool_dir).unwrap();

        let (tx, rx) = std::sync::mpsc::sync_channel::<super::ChunkResult>(parallelism.max(1));
        let drain = std::thread::spawn(move || {
            let mut out = Vec::new();
            while let Ok(msg) = rx.recv() {
                out.push(msg.expect("no build error in the mock"));
            }
            out
        });

        let stats = rt.block_on(async {
            let ctx = super::VirtualChunkContext {
                shared_alloc: &shared_alloc,
                ledger_id: "mock-twin",
                compress: false,
                spool_dir: &spool_dir,
                spool_config: None, // encode-only (no spool sidecars) for the test
            };
            super::drive_virtual_import(
                provider,
                "mock-gs",
                64, // tiny chunk budget → many chunks
                parallelism,
                0,    // parent-index budget guard disabled
                true, // allow duplicate parent keys — this differential test isn't the decline gate
                &ctx,
                tx,
            )
            .await
            .expect("build succeeds")
        });

        let chunks = drain.join().unwrap();
        let _ = std::fs::remove_dir_all(&spool_dir);
        (stats, chunks, shared_alloc)
    }

    /// Assert the p=1-vs-p=2 differential + chunk invariants for one fixture: the
    /// wave-scheduled parallel produce (O1 + O5(c)) must emit identical stats and op
    /// counts regardless of parallelism, with contiguous chunk indices and exactly
    /// one stamped final chunk.
    fn assert_parallel_differential(name: &str, provider: &MockBuildProvider) {
        let (stats1, chunks1, alloc1) = run_build(provider, 1);
        let (stats2, chunks2, alloc2) = run_build(provider, 2);

        // 1) Identical enumeration regardless of parallelism (the triples emitted
        //    are the same; only chunk boundaries/order differ).
        assert_eq!(
            stats1, stats2,
            "{name}: p=1 and p=2 must emit identical stats"
        );
        assert!(
            stats1.total_triples() > 0,
            "{name}: the fixture must emit triples"
        );

        // 2) No triple lost or duplicated: total encoded ops match across runs.
        let ops =
            |cs: &[(usize, ParsedChunk)]| cs.iter().map(|(_, c)| c.op_count as u64).sum::<u64>();
        assert_eq!(
            ops(&chunks1),
            ops(&chunks2),
            "{name}: total op_count must match p=1 vs p=2"
        );

        // 3) Chunk indices are unique and contiguous 0..N in each run (the shared
        //    atomic invariant the reordering consumer relies on — now ACROSS waves).
        for (label, chunks, alloc) in [("p1", &chunks1, &alloc1), ("p2", &chunks2, &alloc2)] {
            let mut idxs: Vec<usize> = chunks.iter().map(|(i, _)| *i).collect();
            idxs.sort_unstable();
            let expected: Vec<usize> = (0..chunks.len()).collect();
            assert_eq!(
                idxs, expected,
                "{name}/{label}: idx must be unique + contiguous 0..N"
            );

            // 4) Exactly ONE chunk carries the completion stamp, and it is the
            //    highest idx (committed last → the twin's head). Resolve each chunk's
            //    own ns codes (as the commit's namespace_delta would) so read_stamp's
            //    MATERIALIZE_NS requirement is exercised, not bypassed.
            let stamped: Vec<usize> = chunks
                .iter()
                .filter(|(_, c)| {
                    let ns_delta = alloc.lookup_codes(&c.new_codes);
                    super::read_stamp(&c.txn_meta, &ns_delta).is_some()
                })
                .map(|(i, _)| *i)
                .collect();
            assert_eq!(
                stamped.len(),
                1,
                "{name}/{label}: exactly one stamped final chunk"
            );
            assert_eq!(
                stamped[0],
                chunks.len() - 1,
                "{name}/{label}: the stamp must ride the highest (final) idx"
            );
        }
    }

    #[test]
    fn parallel_produce_matches_serial_and_stamps_once() {
        // Star (one FK level), chain (a parent that is ALSO a child → two-level wave
        // ordering + index-during-emit), and a mutual cycle (the pre-index fallback)
        // must all satisfy the differential + chunk invariants.
        assert_parallel_differential("star", &star_fixture());
        assert_parallel_differential("chain", &chain_fixture());
        assert_parallel_differential("cyclic", &cyclic_fixture());
    }

    /// The chain fixture must resolve BOTH foreign-key levels — proof that O5(c)'s
    /// index-during-emit built Country's index (during its own wave) before City
    /// emitted, and Region's before Country. Counted via the aggregate stats.
    #[test]
    fn chain_resolves_both_fk_levels() {
        let (stats, _, _) = run_build(&chain_fixture(), 2);
        // country/10 → region/1 (country/20 → region/9 dangling): 1 edge.
        // city/100→country/10, city/300→country/10 (200→20, 400→99 → both? 20 exists,
        // 99 dangling): city edges = 100,200,300 resolve = 3.
        assert_eq!(
            stats.ref_triples, 4,
            "chain must resolve 1 inRegion + 3 inCountry edges: {stats:?}"
        );
    }
}
