//! Parsing a document across threads and writing it back in order.
//!
//! The shape is the plan's: a chunker cuts the document at statement
//! boundaries, workers parse chunks concurrently into per-chunk collector
//! sinks, and one thread replays the collected graphs into the real writer in
//! chunk order.
//!
//! # Why the writer stays serial
//!
//! Because blank-node identity is a document-wide property and the output
//! labels have to be assigned by something that sees the whole document in
//! order. Workers that each wrote their own bytes would each run their own
//! relabeller, and two independent relabellers produce the same label for
//! different nodes.
//!
//! That is not a hypothetical. In Turtle a *labelled* blank node is scoped to
//! the document, so `_:x` in chunk 1 and `_:x` in chunk 7 are the same node
//! and must land on one output label; while an *anonymous* `[]` in chunk 1 and
//! another in chunk 7 are different nodes that must not. Per-chunk relabelling
//! gets both wrong in opposite directions — it splits the first pair whenever
//! the chunks contain different numbers of earlier blanks, and merges the
//! second pair whenever they contain the same number.
//!
//! So: workers collect, one replayer writes, one relabeller labels. The cost
//! is memory — the in-flight chunks are materialized as graphs rather than
//! streamed — and it is bounded by chunk size times worker count, which is
//! why [`ParallelConfig::chunk_bytes`] exists.
//!
//! # What the workers must still do
//!
//! Collector sinks mint their own labels for anonymous nodes, starting from
//! the same counter in every chunk. Two chunks therefore both mint `-b1`, and
//! replaying them into one relabeller would merge two unrelated nodes.
//! [`ChunkScopedBlanks`] renames those mints into a per-chunk namespace before
//! they are collected, which is the only reason the replay is sound.

use crate::error::{CliError, CliResult};
use crate::rdf::syntax::RdfSyntax;
use fluree_graph_ir::{
    Datatype, Graph, GraphCollectorSink, GraphSink, LiteralValue, SinkResult, Term, TermId,
};
use fluree_graph_turtle::{splitter, ParserOptions};

/// How the parallel path is configured.
#[derive(Clone, Copy, Debug)]
pub struct ParallelConfig {
    /// Worker threads. One means the serial path is used instead.
    pub workers: usize,
    /// Target chunk size. Real chunks run to the next statement boundary.
    pub chunk_bytes: u64,
}

impl ParallelConfig {
    /// Chunk size when nothing says otherwise.
    ///
    /// 8 MiB, from the M−1 spike: 32 MiB chunks *halved* throughput past 8
    /// workers on a 245 MB corpus because there were too few chunks to keep
    /// the pool fed. Granularity starvation costs more than per-chunk
    /// overhead saves.
    pub const DEFAULT_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

    /// Size chunks so every worker gets several, without going under a floor
    /// where per-chunk overhead dominates.
    pub fn for_input(workers: usize, input_len: usize) -> Self {
        const MIN_CHUNK: u64 = 256 * 1024;
        // Several chunks per worker so a slow one cannot strand the pool.
        let target = (input_len as u64) / (workers.max(1) as u64 * 4);
        Self {
            workers,
            chunk_bytes: target.clamp(MIN_CHUNK, Self::DEFAULT_CHUNK_BYTES),
        }
    }
}

/// Whether a syntax can be produced by the parallel path.
///
/// Every text syntax can, now that the differential gate is equivalence rather
/// than cross-mode byte equality. Two things had to be true for the
/// Turtle family:
///
/// - **Prefixes are declared once.** Each chunk re-parses the header prelude,
///   so every chunk's writer would otherwise emit the same `@prefix` block.
///   Chunk 0 declares for the whole output and the rest suppress via
///   [`WriterConfig::declare_prefixes`](fluree_graph_format::WriterConfig::declare_prefixes);
///   compaction still works everywhere, because the prefix map is populated
///   regardless.
/// - **A subject spanning a boundary is re-declared.** Blocks-tier folding is
///   per-writer, so a run of same-subject statements split across two chunks
///   comes out as two subject blocks instead of one. That is valid
///   blocks-tier Turtle — the tier already declines to regroup a subject that
///   recurs later in the document — and the only cost is a few bytes.
///
/// JSON-LD is the exception, and not for either of those reasons: it is
/// document-at-once, so there are no fragments to concatenate.
pub fn can_run_parallel(syntax: RdfSyntax) -> bool {
    matches!(
        syntax,
        RdfSyntax::NTriples | RdfSyntax::NQuads | RdfSyntax::Turtle | RdfSyntax::TriG
    )
}

/// Renames a collector's anonymous mints into a per-chunk namespace.
///
/// Anonymous nodes arrive as `term_blank(None)` and every collector answers
/// with the same sequence — `-b1`, `-b2`, … — so chunk 1's first anonymous
/// node and chunk 7's first anonymous node are indistinguishable by the time
/// the replayer sees them, and one relabeller maps them to one output label.
/// They are different nodes; merging them is silent data corruption.
///
/// Naming them here fixes that. The `-c{chunk}_` prefix keeps two properties
/// at once: the leading `-` cannot begin a Turtle `BLANK_NODE_LABEL`, so no
/// user-written label can collide with a mint, and the chunk index makes two
/// chunks' mints disjoint from each other. Labelled blank nodes pass through
/// untouched, because those *must* unify across chunks.
struct ChunkScopedBlanks<S> {
    inner: S,
    chunk: usize,
    minted: u64,
}

impl<S: GraphSink> ChunkScopedBlanks<S> {
    fn new(inner: S, chunk: usize) -> Self {
        Self {
            inner,
            chunk,
            minted: 0,
        }
    }

    fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: GraphSink> GraphSink for ChunkScopedBlanks<S> {
    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        match label {
            // A document-scoped label. Two chunks naming it mean one node.
            Some(label) => self.inner.term_blank(Some(label)),
            None => {
                self.minted += 1;
                let scoped = format!("-c{}_{}", self.chunk, self.minted);
                self.inner.term_blank(Some(&scoped))
            }
        }
    }

    fn on_base(&mut self, base_iri: &str) {
        self.inner.on_base(base_iri);
    }
    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.inner.on_prefix(prefix, namespace_iri);
    }
    fn term_iri(&mut self, iri: &str) -> TermId {
        self.inner.term_iri(iri)
    }
    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.inner.term_literal(value, datatype, language)
    }
    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.inner.term_literal_value(value, datatype)
    }
    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        self.inner.emit_triple(s, p, o)
    }
    fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, index: i32) -> SinkResult {
        self.inner.emit_list_item(s, p, o, index)
    }
    fn supports_quads(&self) -> bool {
        self.inner.supports_quads()
    }
    fn emit_quad(&mut self, s: TermId, p: TermId, o: TermId, g: TermId) -> SinkResult {
        self.inner.emit_quad(s, p, o, g)
    }
    fn supports_reified_triples(&self) -> bool {
        self.inner.supports_reified_triples()
    }
    fn emit_reified_triple(&mut self, s: TermId, p: TermId, o: TermId, r: TermId) -> SinkResult {
        self.inner.emit_reified_triple(s, p, o, r)
    }
    fn end_statement(&mut self) {
        self.inner.end_statement();
    }
    fn abort_statement(&mut self) {
        self.inner.abort_statement();
    }
    fn finish(&mut self) -> SinkResult {
        self.inner.finish()
    }
}

/// Rewrites every blank-node label into a form that is correct across chunks
/// without any coordination between them — plan §1.3's bijective rename.
///
/// This is what lets workers produce *bytes* instead of graphs. The
/// collect-then-replay design needed one relabeller on one thread to see the
/// whole document in order, and that serial replay turned out to be half the
/// wall clock. Here the mapping is a pure function of (label, chunk), so every
/// worker can run its own writer and the results simply concatenate.
///
/// Three classes of label come out, and they are pairwise disjoint by
/// construction rather than by check:
///
/// - A user's `_:L` becomes `u{L}`. Prefixing is injective, so two distinct
///   user labels stay distinct, and a label naming the same node in two chunks
///   maps to the same output label in both — which is what document scoping
///   requires.
/// - An anonymous node in chunk `c` becomes `g{c}_{n}`. Different chunks
///   cannot collide because `c` differs; within a chunk `n` differs.
/// - A `_:fdb-…` stable identifier passes through verbatim, preserving
///   #1432's addressability contract.
///
/// Disjointness across the classes is by first character: `u…` never equals
/// `g…`, and neither can equal `fdb-…`. A user label that *looks* like a mint
/// is no threat — `_:g0_1` becomes `ug0_1`, not `g0_1`.
pub struct DeterministicBlanks<S> {
    inner: S,
    chunk: usize,
    minted: u64,
}

/// Labels the writers treat as addressable identifiers rather than syntax.
const FDB_CARVE_OUT: &str = "fdb-";

impl<S: GraphSink> DeterministicBlanks<S> {
    /// Wrap `inner` for chunk `chunk`.
    pub fn new(inner: S, chunk: usize) -> Self {
        Self {
            inner,
            chunk,
            minted: 0,
        }
    }

    /// Unwrap.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// The output label for a user-written one.
    fn rename(label: &str) -> std::borrow::Cow<'_, str> {
        if label.starts_with(FDB_CARVE_OUT) {
            std::borrow::Cow::Borrowed(label)
        } else {
            std::borrow::Cow::Owned(format!("u{label}"))
        }
    }
}

impl<S: GraphSink> GraphSink for DeterministicBlanks<S> {
    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        match label {
            Some(label) => {
                let renamed = Self::rename(label);
                self.inner.term_blank(Some(&renamed))
            }
            None => {
                self.minted += 1;
                let minted = format!("g{}_{}", self.chunk, self.minted);
                self.inner.term_blank(Some(&minted))
            }
        }
    }

    fn on_base(&mut self, base_iri: &str) {
        self.inner.on_base(base_iri);
    }
    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.inner.on_prefix(prefix, namespace_iri);
    }
    fn term_iri(&mut self, iri: &str) -> TermId {
        self.inner.term_iri(iri)
    }
    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.inner.term_literal(value, datatype, language)
    }
    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.inner.term_literal_value(value, datatype)
    }
    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        self.inner.emit_triple(s, p, o)
    }
    fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, index: i32) -> SinkResult {
        self.inner.emit_list_item(s, p, o, index)
    }
    fn supports_quads(&self) -> bool {
        self.inner.supports_quads()
    }
    fn emit_quad(&mut self, s: TermId, p: TermId, o: TermId, g: TermId) -> SinkResult {
        self.inner.emit_quad(s, p, o, g)
    }
    fn supports_reified_triples(&self) -> bool {
        self.inner.supports_reified_triples()
    }
    fn emit_reified_triple(&mut self, s: TermId, p: TermId, o: TermId, r: TermId) -> SinkResult {
        self.inner.emit_reified_triple(s, p, o, r)
    }
    fn end_statement(&mut self) {
        self.inner.end_statement();
    }
    fn abort_statement(&mut self) {
        self.inner.abort_statement();
    }
    fn finish(&mut self) -> SinkResult {
        self.inner.finish()
    }
}

/// One chunk's parsed result, waiting its turn to be written.
struct ChunkResult {
    index: usize,
    graph: Graph,
    /// The chunk's parse failure, if it had one. Carried rather than raised so
    /// the replayer can report failures in document order.
    error: Option<fluree_graph_turtle::TurtleError>,
    /// Time this worker spent parsing, for the per-worker clock aggregation.
    parse_nanos: u128,
}

/// What a parallel run produced.
pub struct ParallelOutcome {
    /// Chunks the document was cut into.
    pub chunks: usize,
    /// Summed parse time across workers — larger than wall by roughly the
    /// speedup, which is what makes it worth reporting separately.
    pub worker_parse_nanos: u128,
    /// Wall time the replayer spent waiting for the next chunk in order.
    /// Non-zero means the pool is the bottleneck; near-zero means the writer
    /// is.
    pub reassembly_wait_nanos: u128,
    /// The first parse failure in document order, if any.
    pub error: Option<fluree_graph_turtle::TurtleError>,
    /// Statements written across all chunks.
    pub statements: u64,
}

/// Parse `text` across `config.workers` threads and replay it into `sink` in
/// document order.
///
/// `sink` is written on the calling thread only. Workers never touch it, which
/// is what lets a single relabeller assign every output label.
pub fn convert_parallel<S: GraphSink>(
    text: &str,
    base: Option<&str>,
    sink: &mut S,
    config: ParallelConfig,
) -> CliResult<ParallelOutcome> {
    let (prefix_block, ranges) =
        splitter::chunk_in_memory(text, config.chunk_bytes).map_err(|e| {
            CliError::Usage(format!("cannot split the input for parallel parsing: {e}"))
        })?;

    let chunk_count = ranges.len();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results = std::thread::scope(|scope| -> Vec<ChunkResult> {
        let next = &next;
        let ranges = &ranges;
        let prefix_block = &prefix_block;

        let mut handles = Vec::with_capacity(config.workers);
        for _ in 0..config.workers.min(chunk_count) {
            handles.push(scope.spawn(|| {
                let mut mine = Vec::new();
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(range) = ranges.get(index) else {
                        break;
                    };
                    mine.push(parse_chunk(index, prefix_block, &text[range.clone()], base));
                }
                mine
            }));
        }

        let mut all: Vec<ChunkResult> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_default())
            .collect();
        all.sort_by_key(|r| r.index);
        all
    });

    let worker_parse_nanos = results.iter().map(|r| r.parse_nanos).sum();
    let mut first_error = None;

    // Replay in document order. One sink, one relabeller, one output.
    let replay_start = std::time::Instant::now();
    for result in results {
        if first_error.is_none() {
            if let Some(e) = result.error {
                first_error = Some(e);
            }
        }
        // Everything before the failing statement in this chunk is real output
        // and is written; the chunk's own collector already rolled the failed
        // statement back.
        replay(&result.graph, sink)?;
        if first_error.is_some() {
            // A parse failure ends the document: statements after it were
            // parsed from chunks the failure did not reach, but emitting them
            // would claim a document that does not exist.
            break;
        }
    }

    Ok(ParallelOutcome {
        chunks: chunk_count,
        worker_parse_nanos,
        // The pool is joined before replay begins, so this is replay wall time
        // rather than a true per-chunk wait. Reported as what it is.
        reassembly_wait_nanos: replay_start.elapsed().as_nanos(),
        error: first_error,
        statements: 0,
    })
}

/// Parse across threads with each worker writing its own bytes, and
/// concatenate the results in chunk order.
///
/// The architecture plan §1.3 specifies, and the reason it is worth the label
/// scheme: no shared relabeller means no serial replay, and the serial replay
/// was ~half the wall clock of the collect-then-replay design it replaces.
///
/// In-flight chunks are bounded by the channel capacity, so peak memory is
/// roughly `(workers + capacity) × chunk output size` rather than the whole
/// output. Out-of-order arrivals wait in a reorder buffer; a worker can always
/// deposit because the capacity is at least the worker count, so no worker
/// blocks on a chunk the writer is waiting for.
pub fn convert_parallel_bytes<W: std::io::Write>(
    text: &str,
    base: Option<&str>,
    out: &mut W,
    syntax: RdfSyntax,
    writer_config: &fluree_graph_format::WriterConfig,
    config: ParallelConfig,
) -> CliResult<ParallelOutcome> {
    let (prefix_block, ranges) =
        splitter::chunk_in_memory(text, config.chunk_bytes).map_err(|e| {
            CliError::Usage(format!("cannot split the input for parallel parsing: {e}"))
        })?;
    let chunk_count = ranges.len();
    let workers = config.workers.min(chunk_count).max(1);

    let next = std::sync::atomic::AtomicUsize::new(0);
    // Capacity >= workers so a worker can always deposit and never blocks
    // holding a chunk the writer is waiting for.
    let (tx, rx) = std::sync::mpsc::sync_channel::<ChunkBytes>(workers.max(2));

    let outcome = std::thread::scope(|scope| -> CliResult<ParallelOutcome> {
        let next = &next;
        let ranges = &ranges;
        let prefix_block = &prefix_block;

        for _ in 0..workers {
            let tx = tx.clone();
            scope.spawn(move || loop {
                let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(range) = ranges.get(index) else {
                    break;
                };
                let produced = write_chunk(
                    index,
                    prefix_block,
                    &text[range.clone()],
                    base,
                    syntax,
                    writer_config,
                );
                if tx.send(produced).is_err() {
                    break;
                }
            });
        }
        // The scope's own sender must go, or `rx` never sees a disconnect.
        drop(tx);

        let mut pending: std::collections::BTreeMap<usize, ChunkBytes> =
            std::collections::BTreeMap::new();
        let mut want = 0usize;
        let mut worker_parse_nanos = 0u128;
        let mut first_error = None;
        let mut statements = 0u64;

        let replay_start = std::time::Instant::now();
        let mut wait_nanos = 0u128;

        while want < chunk_count {
            if let Some(ready) = pending.remove(&want) {
                worker_parse_nanos += ready.parse_nanos;
                statements += ready.statements;
                if first_error.is_none() {
                    first_error = ready.error;
                }
                out.write_all(&ready.bytes)
                    .map_err(|e| CliError::Usage(format!("cannot write output: {e}")))?;
                want += 1;
                // A failed chunk ends the document: what follows was parsed
                // from chunks the failure never reached, and emitting it would
                // claim a document that does not exist.
                if first_error.is_some() {
                    break;
                }
                continue;
            }
            let waited = std::time::Instant::now();
            match rx.recv() {
                Ok(chunk) => {
                    wait_nanos += waited.elapsed().as_nanos();
                    pending.insert(chunk.index, chunk);
                }
                // Every worker is gone and the chunk we want never arrived.
                Err(_) => break,
            }
        }

        Ok(ParallelOutcome {
            chunks: chunk_count,
            worker_parse_nanos,
            reassembly_wait_nanos: wait_nanos.max(replay_start.elapsed().as_nanos() / 1_000),
            error: first_error,
            statements,
        })
    })?;

    Ok(outcome)
}

/// One chunk's bytes, ready to concatenate.
struct ChunkBytes {
    index: usize,
    bytes: Vec<u8>,
    statements: u64,
    parse_nanos: u128,
    error: Option<fluree_graph_turtle::TurtleError>,
}

/// Parse one chunk straight into its own writer.
fn write_chunk(
    index: usize,
    prefix_block: &str,
    body: &str,
    base: Option<&str>,
    syntax: RdfSyntax,
    writer_config: &fluree_graph_format::WriterConfig,
) -> ChunkBytes {
    let mut doc = String::with_capacity(prefix_block.len() + body.len());
    doc.push_str(prefix_block);
    doc.push_str(body);

    // Preserve, because the labels reaching the writer are already the final
    // ones: `DeterministicBlanks` has done the renaming that makes them
    // correct across chunks, and a second relabelling would undo it.
    let config = writer_config
        .clone()
        .with_blank_labels(fluree_graph_format::BlankNodeLabels::Preserve)
        // Chunk 0 declares the prefixes for the whole concatenated document;
        // every later chunk sees the same prelude and would redeclare them.
        .with_prefix_declarations(index == 0);
    let writer = match crate::rdf::writer::AnyWriter::new(
        syntax,
        Vec::new(),
        &config,
        &fluree_graph_format::PrefixMap::new(),
    ) {
        Ok(w) => w,
        Err(e) => {
            return ChunkBytes {
                index,
                bytes: Vec::new(),
                statements: 0,
                parse_nanos: 0,
                error: Some(fluree_graph_turtle::TurtleError::parse(0, e.to_string())),
            }
        }
    };
    let mut sink = DeterministicBlanks::new(writer, index);

    let started = std::time::Instant::now();
    let result = fluree_graph_turtle::parse_with_prefixes_base_options(
        &doc,
        &mut sink,
        &[],
        base,
        ParserOptions::conformant(),
    );
    let parse_nanos = started.elapsed().as_nanos();

    let mut writer = sink.into_inner();
    let finish = GraphSink::finish(&mut writer);
    let statements = writer.stats().statements;
    let bytes = writer.into_inner();

    ChunkBytes {
        index,
        bytes,
        statements,
        parse_nanos,
        error: result
            .err()
            .or_else(|| finish.err().map(fluree_graph_turtle::TurtleError::Sink)),
    }
}

/// Parse one chunk into its own collector, with its mints scoped to the chunk.
fn parse_chunk(index: usize, prefix_block: &str, body: &str, base: Option<&str>) -> ChunkResult {
    let mut doc = String::with_capacity(prefix_block.len() + body.len());
    doc.push_str(prefix_block);
    doc.push_str(body);

    let mut sink = ChunkScopedBlanks::new(GraphCollectorSink::new(), index);
    let started = std::time::Instant::now();
    let result = fluree_graph_turtle::parse_with_prefixes_base_options(
        &doc,
        &mut sink,
        &[],
        base,
        ParserOptions::conformant(),
    );
    let parse_nanos = started.elapsed().as_nanos();

    ChunkResult {
        index,
        graph: sink.into_inner().into_graph(),
        error: result.err(),
        parse_nanos,
    }
}

/// Replay a collected graph into the real sink.
///
/// Terms are re-interned per triple. The collector kept whole `Term`s, so this
/// is the point where they become the sink's ids — and where the single
/// relabeller sees every blank node, in order.
fn replay<S: GraphSink>(graph: &Graph, sink: &mut S) -> CliResult<()> {
    for triple in graph.iter() {
        let s = intern(&triple.s, sink);
        let p = intern(&triple.p, sink);
        let o = intern(&triple.o, sink);
        sink.emit_triple(s, p, o)
            .map_err(|e| CliError::Usage(format!("sink error: {e}")))?;
        sink.end_statement();
    }
    Ok(())
}

fn intern<S: GraphSink>(term: &Term, sink: &mut S) -> TermId {
    match term {
        Term::Iri(iri) => sink.term_iri(iri),
        Term::BlankNode(b) => sink.term_blank(Some(b.as_str())),
        Term::Literal {
            value,
            datatype,
            language,
        } => sink.term_literal(&value.lexical(), datatype.clone(), language.as_deref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole design exists for, at the unit level: two chunks
    /// naming `_:x` mean one node, and two chunks' anonymous nodes mean two.
    #[test]
    fn chunk_scoping_unifies_labels_and_separates_mints() {
        let mut a = ChunkScopedBlanks::new(GraphCollectorSink::new(), 0);
        let a_labelled = a.term_blank(Some("x"));
        let a_anon = a.term_blank(None);
        let a_graph = {
            let p = a.term_iri("http://e/p");
            a.emit_triple(a_labelled, p, a_anon).unwrap();
            a.into_inner().into_graph()
        };

        let mut b = ChunkScopedBlanks::new(GraphCollectorSink::new(), 1);
        let b_labelled = b.term_blank(Some("x"));
        let b_anon = b.term_blank(None);
        let b_graph = {
            let p = b.term_iri("http://e/p");
            b.emit_triple(b_labelled, p, b_anon).unwrap();
            b.into_inner().into_graph()
        };

        let label_of = |g: &Graph, subject: bool| match g.iter().next().map(|t| {
            if subject {
                t.s.clone()
            } else {
                t.o.clone()
            }
        }) {
            Some(Term::BlankNode(b)) => b.as_str().to_string(),
            other => panic!("expected a blank node, got {other:?}"),
        };

        assert_eq!(
            label_of(&a_graph, true),
            label_of(&b_graph, true),
            "a labelled blank node is document-scoped: two chunks naming `_:x` \
             mean the same node and must reach the writer under one label"
        );
        assert_ne!(
            label_of(&a_graph, false),
            label_of(&b_graph, false),
            "anonymous nodes in different chunks are different nodes; sharing a \
             label would merge them at the writer"
        );
    }

    #[test]
    fn a_chunk_mint_can_never_collide_with_a_user_label() {
        // Turtle's BLANK_NODE_LABEL cannot begin with '-', so no document can
        // contain a label shaped like a mint however adversarial it is.
        let mut sink = ChunkScopedBlanks::new(GraphCollectorSink::new(), 7);
        let minted = sink.term_blank(None);
        let p = sink.term_iri("http://e/p");
        sink.emit_triple(minted, p, minted).unwrap();
        let graph = sink.into_inner().into_graph();

        let Some(Term::BlankNode(label)) = graph.iter().next().map(|t| t.s.clone()) else {
            panic!("expected a blank subject");
        };
        assert!(label.as_str().starts_with("-c7_"), "{}", label.as_str());
        assert!(
            !fluree_graph_ir::chars::is_blank_node_label(label.as_str()),
            "a mint that can lex as a user label can be collided with: {}",
            label.as_str()
        );
    }

    /// Serial and parallel must produce the SAME BYTES for a line-based
    /// syntax. Not isomorphic output — identical output. Anything less and a
    /// user cannot switch `--parallelism` without re-verifying their pipeline.
    fn serial_bytes(text: &str) -> String {
        use crate::rdf::writer::AnyWriter;
        use fluree_graph_format::{PrefixMap, WriterConfig};

        let mut w = AnyWriter::new(
            RdfSyntax::NTriples,
            Vec::new(),
            &WriterConfig::new(),
            &PrefixMap::new(),
        )
        .unwrap();
        fluree_graph_turtle::parse_with_prefixes_base_options(
            text,
            &mut w,
            &[],
            None,
            ParserOptions::conformant(),
        )
        .expect("fixture must parse");
        w.finish().unwrap();
        String::from_utf8(w.into_inner()).unwrap()
    }

    fn parallel_bytes(text: &str, workers: usize, chunk_bytes: u64) -> String {
        use crate::rdf::writer::AnyWriter;
        use fluree_graph_format::{PrefixMap, WriterConfig};

        let mut w = AnyWriter::new(
            RdfSyntax::NTriples,
            Vec::new(),
            &WriterConfig::new(),
            &PrefixMap::new(),
        )
        .unwrap();
        let outcome = convert_parallel(
            text,
            None,
            &mut w,
            ParallelConfig {
                workers,
                chunk_bytes,
            },
        )
        .expect("parallel run");
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert!(outcome.chunks >= 2, "fixture did not actually split");
        w.finish().unwrap();
        String::from_utf8(w.into_inner()).unwrap()
    }

    #[test]
    fn serial_and_parallel_produce_identical_bytes() {
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
        for i in 0..400 {
            ttl.push_str(&format!(
                "ex:s{i} ex:name \"person {i}\" ; ex:age {} .\n",
                i % 90
            ));
        }

        let serial = serial_bytes(&ttl);
        for workers in [2, 4, 8] {
            assert_eq!(
                serial,
                parallel_bytes(&ttl, workers, 700),
                "parallelism {workers} changed the output"
            );
        }
    }

    #[test]
    fn a_labelled_blank_node_spanning_chunks_stays_one_node() {
        // `_:shared` is named in the first statement and again far later, with
        // enough between them to land in different chunks. Turtle scopes a
        // labelled blank node to the document, so both are the same node and
        // must reach the output under ONE label. Independent per-chunk
        // relabellers split it.
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
        ttl.push_str("_:shared ex:role \"first\" .\n");
        for i in 0..400 {
            ttl.push_str(&format!("ex:filler{i} ex:p \"v{i}\" .\n"));
        }
        ttl.push_str("_:shared ex:role \"last\" .\n");

        let serial = serial_bytes(&ttl);
        let parallel = parallel_bytes(&ttl, 4, 700);
        assert_eq!(serial, parallel);

        // And the node really is shared in the output: one label carries both
        // roles.
        let label_for = |role: &str| -> String {
            parallel
                .lines()
                .find(|l| l.contains(role))
                .and_then(|l| l.split_whitespace().next())
                .unwrap_or_default()
                .to_string()
        };
        let first = label_for("\"first\"");
        let last = label_for("\"last\"");
        assert!(first.starts_with("_:"), "{first}");
        assert_eq!(
            first, last,
            "`_:shared` was split across chunks into two different nodes"
        );
    }

    #[test]
    fn anonymous_nodes_in_different_chunks_stay_distinct() {
        // Every statement carries its own `[]`. Chunk-local mints all start at
        // the same counter, so without per-chunk scoping the Nth anonymous
        // node of every chunk collapses onto one label — silently merging
        // unrelated nodes.
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
        for i in 0..300 {
            ttl.push_str(&format!("ex:s{i} ex:has [ ex:tag \"t{i}\" ] .\n"));
        }

        let serial = serial_bytes(&ttl);
        let parallel = parallel_bytes(&ttl, 4, 700);
        assert_eq!(serial, parallel);

        // 300 distinct anonymous nodes, not fewer.
        let blanks: std::collections::HashSet<&str> = parallel
            .lines()
            .filter_map(|l| l.split_whitespace().next())
            .filter(|t| t.starts_with("_:"))
            .collect();
        assert_eq!(
            blanks.len(),
            300,
            "anonymous nodes merged across chunks: {} distinct labels for 300 nodes",
            blanks.len()
        );
    }

    #[test]
    fn user_labels_shaped_like_mints_do_not_collide_with_them() {
        // The adversarial fixture the plan calls for: a document whose own
        // blank-node labels imitate the writer's and the chunker's mint
        // patterns. If any minting scheme is not disjoint from what a user can
        // write, these merge two distinct nodes into one.
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
        for label in ["b1", "b2", "c0_1", "c1_1", "g0_1", "u1", "fdbw-1"] {
            ttl.push_str(&format!("_:{label} ex:bait \"{label}\" .\n"));
        }
        for i in 0..300 {
            ttl.push_str(&format!("ex:s{i} ex:has [ ex:tag \"t{i}\" ] .\n"));
        }

        let serial = serial_bytes(&ttl);
        let parallel = parallel_bytes(&ttl, 4, 700);
        assert_eq!(serial, parallel, "the bait labels changed the output");

        // 7 bait nodes + 300 anonymous = 307 distinct blank nodes, no merges.
        let blanks: std::collections::HashSet<&str> = parallel
            .lines()
            .filter_map(|l| l.split_whitespace().next())
            .filter(|t| t.starts_with("_:"))
            .collect();
        assert_eq!(
            blanks.len(),
            307,
            "a mint collided with a user label: {} distinct labels for 307 nodes",
            blanks.len()
        );
    }

    #[test]
    fn one_worker_matches_the_serial_path_exactly() {
        // `--parallelism 1` must be the serial path's output, so the flag is
        // never a correctness decision.
        let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
        for i in 0..100 {
            ttl.push_str(&format!("ex:s{i} ex:p \"v{i}\" .\n"));
        }
        assert_eq!(serial_bytes(&ttl), parallel_bytes(&ttl, 1, 400));
    }

    #[test]
    fn every_text_syntax_runs_parallel_and_jsonld_does_not() {
        for syntax in [
            RdfSyntax::NTriples,
            RdfSyntax::NQuads,
            RdfSyntax::Turtle,
            RdfSyntax::TriG,
        ] {
            assert!(can_run_parallel(syntax), "{syntax} should run parallel");
        }
        // Document-at-once: there are no fragments to concatenate.
        assert!(!can_run_parallel(RdfSyntax::JsonLd));
    }

    #[test]
    fn chunk_sizing_stays_between_the_floor_and_the_spike_ceiling() {
        // The M-1 spike measured 32 MiB chunks HALVING throughput past 8
        // workers: too few chunks to keep the pool fed.
        let big = ParallelConfig::for_input(8, 1_000_000_000);
        assert!(big.chunk_bytes <= ParallelConfig::DEFAULT_CHUNK_BYTES);
        // And a small input does not get chunks smaller than the floor.
        let small = ParallelConfig::for_input(8, 1_000);
        assert_eq!(small.chunk_bytes, 256 * 1024);
        // Several chunks per worker on a mid-size input.
        let mid = ParallelConfig::for_input(4, 40 * 1024 * 1024);
        assert!(mid.chunk_bytes < 8 * 1024 * 1024);
    }
}
