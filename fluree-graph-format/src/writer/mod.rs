//! Streaming RDF writers over the [`GraphSink`](fluree_graph_ir::GraphSink)
//! protocol
//!
//! Each writer is a sink: a parser (or any other producer) emits terms and
//! statements into it and bytes come out the other side. Nothing materializes
//! a whole document first — which is what lets `convert big.ttl | head -5`
//! cost five statements rather than a full parse, and what keeps memory flat
//! over a 100 GB dump.
//!
//! | writer | syntax | quads | memory |
//! |---|---|---|---|
//! | [`NTriplesWriter`] | N-Triples | no | O(1) |
//! | [`NQuadsWriter`] | N-Quads | yes | O(1) |
//! | [`TurtleWriter`] | Turtle, "blocks" tier | no | O(1) |
//! | [`TrigWriter`] | TriG, "blocks" tier | yes | O(1) |
//! | [`JsonLdWriter`] | JSON-LD | no | O(document) |
//!
//! "O(1)" is in the number of *statements*: every writer's term table still
//! holds one entry per distinct IRI, per distinct labeled blank node, and per
//! anonymous blank node, because the protocol makes those ids session-scoped
//! (see [`TermId`](fluree_graph_ir::TermId)). Literal slots recycle per
//! statement. JSON-LD is document-at-once by construction — the format has no
//! streaming form short of NDJSON, which is deferred.
//!
//! # Fidelity tiers
//!
//! [`TurtleWriter`] and [`TrigWriter`] implement the **blocks** tier: they
//! fold *consecutive* runs of the same subject with `;` and of the same
//! subject-and-predicate with `,`, and otherwise preserve document order.
//! They do not regroup a subject that recurs later in the document, do not
//! reconstruct `[ … ]` anonymous-node syntax, and do not re-collapse an
//! `rdf:first`/`rdf:rest` spine into `( … )`. A buffered `--pretty` tier that
//! does those things is a deliberate follow-up, not an oversight: it costs
//! O(document) memory, and riot draws the same line.

mod blank;
mod jsonld;
mod line;
mod terms;
mod turtle;

use fluree_graph_ir::{SinkError, SinkResult};
use std::io::{self, Write};

pub use blank::BlankNodeLabels;
pub use jsonld::JsonLdWriter;
pub use line::{NQuadsWriter, NTriplesWriter};
pub use turtle::{TrigWriter, TurtleWriter};

use crate::prefix::PrefixMap;

/// Shared configuration for every writer in this module.
#[derive(Clone, Debug, Default)]
pub struct WriterConfig {
    /// How blank-node labels reach the output. See [`BlankNodeLabels`].
    pub blank_labels: BlankNodeLabels,

    /// Prefixes to seed the output with.
    ///
    /// Turtle and TriG declare these alongside any that arrive as
    /// `on_prefix` events and use them to compact IRIs. N-Triples, N-Quads
    /// and JSON-LD ignore them: the first two have no prefixed-name syntax,
    /// and JSON-LD takes its `@context` from its own format config.
    pub prefixes: PrefixMap,

    /// Hold each statement's bytes until the producer commits it.
    ///
    /// Off by default, and the difference is visible in the contract for
    /// [`abort_statement`](fluree_graph_ir::GraphSink::abort_statement): bytes
    /// already handed to the underlying writer cannot be unwritten, so an
    /// unbuffered writer's rollback keeps whatever the failed statement
    /// already emitted. With buffering on, a rejected statement contributes
    /// nothing — riot's semantics, and what `--continue-on-error` needs.
    ///
    /// The cost is O(widest statement) memory, but only against producers
    /// that actually delimit statements. A producer that never calls
    /// `end_statement` (the JSON-LD adapter does not) would buffer the entire
    /// document, which is why this is opt-in rather than the default.
    pub buffer_statements: bool,
}

impl WriterConfig {
    /// A default configuration: relabelled blank nodes, no prefixes, no
    /// statement buffering.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the blank-node labelling policy.
    pub fn with_blank_labels(mut self, blank_labels: BlankNodeLabels) -> Self {
        self.blank_labels = blank_labels;
        self
    }

    /// Seed the prefix map used by Turtle and TriG.
    pub fn with_prefixes(mut self, prefixes: PrefixMap) -> Self {
        self.prefixes = prefixes;
        self
    }

    /// Turn statement buffering on, making `abort_statement` a true rollback.
    pub fn with_statement_buffering(mut self, buffer_statements: bool) -> Self {
        self.buffer_statements = buffer_statements;
        self
    }
}

/// What a writer has produced so far.
///
/// Counters only — the writers deliberately take no timestamps. Attributing
/// wall-clock time to a phase is the driver's job, at chunk granularity;
/// an `Instant::now()` per emitted statement would cost more than the write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriterStats {
    /// Statements written: one per triple or quad that reached the output.
    ///
    /// Counted when the bytes are produced, so under
    /// [`WriterConfig::buffer_statements`] a statement the producer later
    /// aborts is *not* counted — it never reached the output.
    pub statements: u64,

    /// Bytes handed to the underlying writer, including punctuation,
    /// prefix declarations and newlines.
    ///
    /// Under statement buffering this counts bytes at commit time, for the
    /// same reason.
    pub bytes: u64,

    /// Statements the producer rolled back with
    /// [`abort_statement`](fluree_graph_ir::GraphSink::abort_statement).
    ///
    /// Counted whether or not the rollback could take the bytes back — an
    /// unbuffered writer cannot, and a `--continue-on-error` run reporting
    /// "3 statements skipped" wants to know either way.
    pub aborted: u64,

    /// Events this writer refused.
    ///
    /// An indexed list item against a text writer, a literal in a position
    /// that cannot hold one, a blank-node label the policy will not emit. The
    /// counter exists so a driver can report refusals without keeping its own
    /// tally of the errors it has already seen — and because the *latched*
    /// refusals that follow the first one are not separate events and must
    /// not inflate it.
    pub refused: u64,
}

/// Every way this writer can have failed, held until something with a return
/// value can report it.
///
/// Several protocol methods have no error channel. `term_blank` returns a
/// [`TermId`](fluree_graph_ir::TermId) and cannot refuse a label the writer
/// must not emit; `end_statement` and `on_prefix` return nothing and cannot
/// report a broken pipe. All of them stash here, and the next `emit_*` — or
/// `finish`, if there is no next emission — raises it.
///
/// # Sticky, and `finish` sees it
///
/// The failure latches. The protocol says a sink that has failed once need not
/// behave sensibly afterwards, but "predictably refuses" is a better answer
/// than "writes the label it just refused", which is what taking the error and
/// forgetting it would do to a producer that ignores one result.
///
/// It must also survive to `finish`, because a refusal can be the *last* event
/// — a closed pipe on the final buffered statement, or a rejection nothing
/// emits after. A `finish` returning `Ok` there would tell a driver the
/// document was written when part of it was lost, which is the one report a
/// converter must never make.
#[derive(Debug, Default)]
pub(crate) struct Deferred {
    pending: Option<SinkError>,
    failed: bool,
    /// Distinct events refused, for [`WriterStats::refused`]. Latched
    /// re-refusals are not events and are not counted.
    refusals: u64,
}

impl Deferred {
    /// Stash a failure raised where nothing can return it — an I/O error from
    /// `end_statement` or `on_prefix`. Not counted as a refusal: the writer
    /// did not decline anything, the output did.
    ///
    /// The first failure wins; it has the context.
    pub(crate) fn stash(&mut self, error: SinkError) {
        if self.pending.is_none() {
            self.pending = Some(error);
        }
    }

    /// Stash a *refusal* raised where nothing can return it — `term_blank`
    /// declining a label the policy will not emit.
    pub(crate) fn stash_refusal(&mut self, error: SinkError) {
        self.refusals += 1;
        self.stash(error);
    }

    /// Count and latch a refusal that is being returned right now.
    ///
    /// Latching is the point: without it, a refusal raised from a method that
    /// *does* return a result would be reported once and then forgotten, and
    /// `finish` would go on to claim success for a document missing whatever
    /// the refused event carried.
    pub(crate) fn refuse(&mut self, message: impl Into<String>) -> SinkError {
        self.refusals += 1;
        self.failed = true;
        SinkError::rejected(message)
    }

    /// Raise a stashed failure, if there is one, or if there ever was one.
    pub(crate) fn check(&mut self) -> SinkResult {
        if let Some(error) = self.pending.take() {
            self.failed = true;
            return Err(error);
        }
        if self.failed {
            return Err(SinkError::rejected(
                "this writer already refused an event; its output is incomplete",
            ));
        }
        Ok(())
    }

    /// How many distinct events were refused.
    pub(crate) fn refusals(&self) -> u64 {
        self.refusals
    }
}

/// A `Write` that counts, and optionally holds back the statement in flight.
///
/// Every writer in this module sends its bytes through one of these, so the
/// byte counter and the buffering contract are implemented once.
pub(crate) struct Out<W: Write> {
    inner: W,
    bytes: u64,
    /// `Some` when [`WriterConfig::buffer_statements`] is on.
    buffer: Option<Vec<u8>>,
}

impl<W: Write> Out<W> {
    pub(crate) fn new(inner: W, buffered: bool) -> Self {
        Self {
            inner,
            bytes: 0,
            buffer: buffered.then(Vec::new),
        }
    }

    /// Whether a rollback can actually take anything back.
    pub(crate) fn is_buffered(&self) -> bool {
        self.buffer.is_some()
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Release the statement in flight to the underlying writer.
    pub(crate) fn commit_statement(&mut self) -> io::Result<()> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Ok(());
        };
        if buffer.is_empty() {
            return Ok(());
        }
        let mut staged = std::mem::take(buffer);
        self.inner.write_all(&staged)?;
        self.bytes += staged.len() as u64;
        // Put the allocation back: statement buffers are all about one size.
        staged.clear();
        self.buffer = Some(staged);
        Ok(())
    }

    /// Drop the statement in flight. A no-op when unbuffered, because the
    /// bytes are already gone.
    pub(crate) fn discard_statement(&mut self) {
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.clear();
        }
    }

    /// Commit anything staged, then flush the underlying writer.
    pub(crate) fn finish(&mut self) -> io::Result<()> {
        self.commit_statement()?;
        self.inner.flush()
    }

    /// Give the underlying writer back.
    pub(crate) fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for Out<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.extend_from_slice(buf);
            return Ok(buf.len());
        }
        let n = self.inner.write(buf)?;
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Deliberately does NOT commit the statement in flight: `flush` can
        // be called by anything holding the writer, and releasing a statement
        // the producer has not committed would defeat the rollback contract.
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unbuffered_out_writes_straight_through_and_counts() {
        let mut buf = Vec::new();
        let mut out = Out::new(&mut buf, false);
        out.write_all(b"hello").unwrap();
        assert_eq!(out.bytes(), 5);
        assert!(!out.is_buffered());
        // Nothing to take back.
        out.discard_statement();
        out.finish().unwrap();
        assert_eq!(buf, b"hello");
    }

    #[test]
    fn a_buffered_out_holds_the_statement_until_it_commits() {
        let mut buf = Vec::new();
        let mut out = Out::new(&mut buf, true);
        out.write_all(b"kept").unwrap();
        assert_eq!(out.bytes(), 0, "nothing has reached the writer yet");
        out.commit_statement().unwrap();
        assert_eq!(out.bytes(), 4);

        out.write_all(b"dropped").unwrap();
        out.discard_statement();
        out.finish().unwrap();
        assert_eq!(buf, b"kept", "the discarded statement never appeared");
    }

    #[test]
    fn flush_does_not_release_the_statement_in_flight() {
        let mut buf = Vec::new();
        let mut out = Out::new(&mut buf, true);
        out.write_all(b"staged").unwrap();
        out.flush().unwrap();
        assert_eq!(
            out.bytes(),
            0,
            "flush must not commit on the producer's behalf"
        );
        out.discard_statement();
        out.finish().unwrap();
        assert!(buf.is_empty());
    }
}
