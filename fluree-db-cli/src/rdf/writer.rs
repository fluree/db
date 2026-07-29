//! Choosing a writer at runtime, and getting bytes out to a destination.
//!
//! The writers in `fluree-graph-format` are generic over their output type and
//! are separate types per syntax, which is right for a library and no use to a
//! CLI that learns the output syntax from a flag. [`AnyWriter`] closes that
//! gap with an enum rather than a trait object: `GraphSink` would be
//! object-safe, but `TimingSink<S>` wants a sized `S`, and a five-arm dispatch
//! costs less than the machinery to avoid it.
//!
//! [`TimedWriter`] is the other half — the seam that makes a `write` phase
//! measurable at all. See its docs for why the clock goes there and nowhere
//! else on this path.

use crate::error::{CliError, CliResult};
use crate::rdf::syntax::RdfSyntax;
use fluree_graph_format::{
    ContextPolicy, JsonLdFormatConfig, JsonLdWriter, NQuadsWriter, NTriplesWriter, PrefixMap,
    TrigWriter, TurtleWriter, WriterConfig, WriterStats,
};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::cell::Cell;
use std::io::{self, Write};
use std::rc::Rc;
use std::time::{Duration, Instant};

/// One of the writers, chosen at runtime.
pub enum AnyWriter<W: Write> {
    /// N-Triples.
    NTriples(NTriplesWriter<W>),
    /// N-Quads.
    NQuads(NQuadsWriter<W>),
    /// Turtle, blocks tier.
    Turtle(TurtleWriter<W>),
    /// TriG, blocks tier.
    TriG(TrigWriter<W>),
    /// JSON-LD.
    JsonLd(JsonLdWriter<W>),
}

/// Forward a method to whichever writer this is.
macro_rules! dispatch {
    ($self:ident, $w:ident => $body:expr) => {
        match $self {
            AnyWriter::NTriples($w) => $body,
            AnyWriter::NQuads($w) => $body,
            AnyWriter::Turtle($w) => $body,
            AnyWriter::TriG($w) => $body,
            AnyWriter::JsonLd($w) => $body,
        }
    };
}

impl<W: Write> AnyWriter<W> {
    /// Build the writer for `syntax`, or refuse a syntax with no writer.
    pub fn new(
        syntax: RdfSyntax,
        out: W,
        config: &WriterConfig,
        prefixes: &PrefixMap,
    ) -> CliResult<Self> {
        Ok(match syntax {
            RdfSyntax::NTriples => AnyWriter::NTriples(NTriplesWriter::with_config(out, config)),
            RdfSyntax::NQuads => AnyWriter::NQuads(NQuadsWriter::with_config(out, config)),
            RdfSyntax::Turtle => AnyWriter::Turtle(TurtleWriter::with_config(out, config)),
            RdfSyntax::TriG => AnyWriter::TriG(TrigWriter::with_config(out, config)),
            RdfSyntax::JsonLd => {
                // Turtle and TriG compact through `WriterConfig::prefixes`;
                // JSON-LD takes the same map as its `@context`, so one
                // `--prefixes` means the same thing whichever syntax is asked
                // for.
                let jsonld =
                    JsonLdFormatConfig::new().with_context_policy(prefix_context(prefixes));
                AnyWriter::JsonLd(JsonLdWriter::with_config(out, config, jsonld))
            }
            other => return Err(no_writer(other)),
        })
    }

    /// What this writer has produced.
    pub fn stats(&self) -> WriterStats {
        dispatch!(self, w => w.stats())
    }

    /// Recover the destination.
    ///
    /// Needed because the writer must *own* what it writes to — the parser
    /// holds it for the length of the parse — while the caller still has to
    /// flush it somewhere a failure can be returned. A `BufWriter` left to
    /// flush on drop discards exactly that error.
    pub fn into_inner(self) -> W {
        dispatch!(self, w => w.into_inner())
    }
}

/// A `@context` holding exactly the prefixes the user supplied, or none.
fn prefix_context(prefixes: &PrefixMap) -> ContextPolicy {
    if prefixes.is_empty() {
        return ContextPolicy::None;
    }
    let map: serde_json::Map<String, serde_json::Value> = prefixes
        .iter()
        .map(|(prefix, iri)| {
            (
                prefix.to_string(),
                serde_json::Value::String(iri.to_string()),
            )
        })
        .collect();
    ContextPolicy::UseProvided(serde_json::Value::Object(map))
}

/// Refuse an output syntax that has no writer, naming what it waits on.
fn no_writer(syntax: RdfSyntax) -> CliError {
    let why = match syntax {
        RdfSyntax::RdfXml => "the RDF/XML writer lands with the XML family",
        RdfSyntax::RdfJson => "RDF/JSON lands with the XML family",
        RdfSyntax::Jelly => "Jelly lands last in the format set",
        _ => "no writer exists for it",
    };
    CliError::Usage(format!(
        "cannot write {syntax} yet — {why}\n  {} writable today: turtle, ntriples, nquads, \
         trig, jsonld",
        colored::Colorize::bold(colored::Colorize::cyan("help:")),
    ))
}

/// Whether a writer exists for `syntax` today.
pub fn is_writable(syntax: RdfSyntax) -> bool {
    matches!(
        syntax,
        RdfSyntax::NTriples
            | RdfSyntax::NQuads
            | RdfSyntax::Turtle
            | RdfSyntax::TriG
            | RdfSyntax::JsonLd
    )
}

impl<W: Write> GraphSink for AnyWriter<W> {
    fn on_base(&mut self, base_iri: &str) {
        dispatch!(self, w => w.on_base(base_iri));
    }

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        dispatch!(self, w => w.on_prefix(prefix, namespace_iri));
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        dispatch!(self, w => w.term_iri(iri))
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        dispatch!(self, w => w.term_blank(label))
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        dispatch!(self, w => w.term_literal(value, datatype, language))
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        dispatch!(self, w => w.term_literal_value(value, datatype))
    }

    fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
        dispatch!(self, w => w.emit_triple(s, p, o))
    }

    fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, index: i32) -> SinkResult {
        dispatch!(self, w => w.emit_list_item(s, p, o, index))
    }

    fn supports_quads(&self) -> bool {
        dispatch!(self, w => w.supports_quads())
    }

    fn emit_quad(&mut self, s: TermId, p: TermId, o: TermId, g: TermId) -> SinkResult {
        dispatch!(self, w => w.emit_quad(s, p, o, g))
    }

    fn emit_quad_list_item(
        &mut self,
        s: TermId,
        p: TermId,
        o: TermId,
        index: i32,
        g: TermId,
    ) -> SinkResult {
        dispatch!(self, w => w.emit_quad_list_item(s, p, o, index, g))
    }

    fn supports_reified_triples(&self) -> bool {
        dispatch!(self, w => w.supports_reified_triples())
    }

    fn emit_reified_triple(&mut self, s: TermId, p: TermId, o: TermId, r: TermId) -> SinkResult {
        dispatch!(self, w => w.emit_reified_triple(s, p, o, r))
    }

    fn end_statement(&mut self) {
        dispatch!(self, w => w.end_statement());
    }

    fn abort_statement(&mut self) {
        dispatch!(self, w => w.abort_statement());
    }

    fn finish(&mut self) -> SinkResult {
        dispatch!(self, w => w.finish())
    }
}

/// Wall-clock spent in real output I/O, shared with the driver.
#[derive(Clone, Debug, Default)]
pub struct WriteClock {
    elapsed: Rc<Cell<Duration>>,
    calls: Rc<Cell<u64>>,
}

impl WriteClock {
    /// Time the underlying destination has taken.
    pub fn elapsed(&self) -> Duration {
        self.elapsed.get()
    }

    /// Calls made to the destination — the clock-read count this cost.
    pub fn calls(&self) -> u64 {
        self.calls.get()
    }
}

/// Times the calls that reach the real destination, and nothing else.
///
/// This is where a `write` phase becomes measurable without the per-event
/// clock the review ruled out. The layering is deliberate:
///
/// ```text
///   writer  →  BufWriter  →  TimedWriter  →  file / stdout
///  (formats)  (accumulates)   (clocks)
/// ```
///
/// The writer's many small `write!`s land in the `BufWriter` and cost no
/// clock at all. The `BufWriter` reaches through only when its buffer fills,
/// so the clock reads here are per *chunk* — a handful over a large document
/// rather than two per statement — and what they measure is exactly the time
/// the operating system took, with formatting excluded by construction.
///
/// Serialization time is then the sink estimate minus this, which is the one
/// decomposition available that does not require timing every emit.
pub struct TimedWriter<W> {
    inner: W,
    clock: WriteClock,
}

impl<W: Write> TimedWriter<W> {
    /// Wrap `inner`, reporting into a fresh clock.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            clock: WriteClock::default(),
        }
    }

    /// A handle on the accumulated time, live for the writer's lifetime.
    pub fn clock(&self) -> WriteClock {
        self.clock.clone()
    }

    fn timed<T>(&mut self, f: impl FnOnce(&mut W) -> T) -> T {
        let start = Instant::now();
        let out = f(&mut self.inner);
        self.clock
            .elapsed
            .set(self.clock.elapsed.get() + start.elapsed());
        self.clock.calls.set(self.clock.calls.get() + 1);
        out
    }
}

impl<W: Write> Write for TimedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.timed(|w| w.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.timed(std::io::Write::flush)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_writable_syntax_builds_and_the_rest_refuse_by_name() {
        for syntax in RdfSyntax::ALL {
            let out: Vec<u8> = Vec::new();
            let built = AnyWriter::new(syntax, out, &WriterConfig::new(), &PrefixMap::new());
            assert_eq!(
                built.is_ok(),
                is_writable(syntax),
                "{syntax} writability disagrees with is_writable()"
            );
            if let Err(e) = built {
                let msg = e.to_string();
                assert!(msg.contains(syntax.as_str()), "{msg}");
                assert!(msg.contains("writable today"), "{msg}");
            }
        }
    }

    #[test]
    fn only_the_quad_writers_claim_quad_support() {
        // A quad reaching a triple-only writer is data loss, and the capability
        // probe is what the producer checks before emitting one.
        let quad_capable = |syntax| {
            let w = AnyWriter::new(syntax, Vec::new(), &WriterConfig::new(), &PrefixMap::new())
                .unwrap();
            w.supports_quads()
        };
        assert!(quad_capable(RdfSyntax::NQuads));
        assert!(quad_capable(RdfSyntax::TriG));
        assert!(!quad_capable(RdfSyntax::NTriples));
        assert!(!quad_capable(RdfSyntax::Turtle));
    }

    #[test]
    fn the_dispatch_reaches_the_writer_it_names() {
        // One statement through each arm, checking the syntax of what comes
        // out — a mis-wired arm would still compile and still produce bytes.
        let write_one = |syntax| -> String {
            let mut w = AnyWriter::new(syntax, Vec::new(), &WriterConfig::new(), &PrefixMap::new())
                .unwrap();
            let s = w.term_iri("http://example.org/s");
            let p = w.term_iri("http://example.org/p");
            let o = w.term_iri("http://example.org/o");
            w.emit_triple(s, p, o).unwrap();
            w.end_statement();
            w.finish().unwrap();
            let bytes = dispatch!(w, inner => inner.into_inner());
            String::from_utf8(bytes).unwrap()
        };

        assert!(write_one(RdfSyntax::NTriples).starts_with('<'));
        assert!(write_one(RdfSyntax::NQuads).starts_with('<'));
        assert!(write_one(RdfSyntax::Turtle).contains("http://example.org/s"));
        assert!(write_one(RdfSyntax::TriG).contains("http://example.org/s"));
        assert!(write_one(RdfSyntax::JsonLd).trim_start().starts_with('{'));
    }

    #[test]
    fn stats_come_back_through_the_dispatch() {
        let mut w = AnyWriter::new(
            RdfSyntax::NTriples,
            Vec::new(),
            &WriterConfig::new(),
            &PrefixMap::new(),
        )
        .unwrap();
        let s = w.term_iri("http://example.org/s");
        let p = w.term_iri("http://example.org/p");
        let o = w.term_iri("http://example.org/o");
        w.emit_triple(s, p, o).unwrap();
        w.end_statement();
        w.finish().unwrap();

        let stats = w.stats();
        assert_eq!(stats.statements, 1);
        assert!(stats.bytes > 0);
        assert_eq!(stats.refused, 0);
    }

    #[test]
    fn supplied_prefixes_become_a_jsonld_context() {
        let mut prefixes = PrefixMap::new();
        prefixes.insert("ex", "http://example.org/");
        match prefix_context(&prefixes) {
            ContextPolicy::UseProvided(v) => {
                assert_eq!(v["ex"], "http://example.org/");
            }
            other => panic!("expected a provided context, got {other:?}"),
        }
        // …and no prefixes means no context, rather than an empty one.
        assert!(matches!(
            prefix_context(&PrefixMap::new()),
            ContextPolicy::None
        ));
    }

    #[test]
    fn the_write_clock_counts_only_calls_that_reach_the_destination() {
        // The property the phase split rests on: buffered writes are free,
        // and only the flush through to the destination costs a clock read.
        let mut timed = TimedWriter::new(Vec::new());
        let clock = timed.clock();
        {
            let mut buffered = io::BufWriter::with_capacity(64 * 1024, &mut timed);
            for _ in 0..10_000 {
                writeln!(buffered, "<http://example.org/s> <http://e/p> \"o\" .").unwrap();
            }
            buffered.flush().unwrap();
        }

        assert!(clock.calls() > 0, "something must have reached the sink");
        assert!(
            clock.calls() < 100,
            "10,000 statements took {} destination calls — the buffer is not \
             doing its job and the clock is back on the hot path",
            clock.calls()
        );
        assert!(clock.elapsed() > Duration::ZERO);
    }

    #[test]
    fn the_clock_handle_sees_writes_made_after_it_was_taken() {
        let mut timed = TimedWriter::new(Vec::new());
        let clock = timed.clock();
        assert_eq!(clock.calls(), 0);
        timed.write_all(b"hello").unwrap();
        assert_eq!(clock.calls(), 1, "the handle is live, not a snapshot");
    }
}
