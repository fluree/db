//! Per-phase timing and event counting for RDF pipelines.
//!
//! A conversion run is a chain of phases — read the bytes, decompress them,
//! parse them, hand the events to a sink, serialize, write — and when one of
//! them gets slower the only useful first question is *which one*. Nothing in
//! the repo answered that before: `ImportPhase` is display-only and rates are
//! computed at render time and thrown away.
//!
//! Two pieces live here:
//!
//! - [`PhaseTimings`], a lane-switching stopwatch. The caller says which phase
//!   it is entering; the accumulator closes out the previous lane and opens
//!   the new one. Four clock reads for a four-phase run, not four hundred.
//! - [`TimingSink`], a decorator that wraps any [`GraphSink`] and instruments
//!   it from the outside, so both the Turtle and JSON-LD parsers are measured
//!   without either of them knowing.
//!
//! # Why the sink is sampled and not stopwatched
//!
//! The obvious implementation — bracket every forwarded call with
//! `Instant::now()` — is the one that cannot be trusted. A `sink` phase for a
//! parse that emits ten million triples would take twenty million clock reads,
//! and on the counting sinks used by `check`/`count` a clock read costs more
//! than the work it is measuring, so the profile would report a phase
//! breakdown of its own overhead.
//!
//! So: **count on every call, take clocks only at statement boundaries.**
//! Counters are integer increments with no clock at all. Timing runs on one
//! statement in [`SAMPLE_STRIDE`], where the decorator does bracket each
//! forwarded call, and the measured cost is scaled by the call ratio to
//! estimate the whole run. The estimate is marked as an estimate everywhere it
//! surfaces, and [`clock_pair_cost`] lets a caller price its own instrument and
//! disclose the overhead rather than assert it is negligible.

use crate::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::time::{Duration, Instant};

/// A phase of an RDF pipeline run.
///
/// Extending this is deliberately cheap: add a variant, add it to
/// [`Phase::ALL`], and every consumer (accumulator, human table, JSON report)
/// picks it up. Phases a given verb never enters simply report zero and are
/// omitted from reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Phase {
    /// Pulling bytes from a file, a pipe, or the network.
    Read,
    /// Decoding a compression layer (`.gz`, `.zst`) off those bytes.
    Decompress,
    /// Lexing and parsing the decoded text into sink events.
    Parse,
    /// Sink dispatch: what the consumer of the events does with them.
    Sink,
    /// Rendering terms back into an output syntax.
    Serialize,
    /// Pushing serialized bytes to their destination.
    Write,
}

impl Phase {
    /// Every phase, in pipeline order. Report ordering follows this.
    pub const ALL: [Phase; 6] = [
        Phase::Read,
        Phase::Decompress,
        Phase::Parse,
        Phase::Sink,
        Phase::Serialize,
        Phase::Write,
    ];

    /// Stable machine-readable name — the key used in `--profile=json`.
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Read => "read",
            Phase::Decompress => "decompress",
            Phase::Parse => "parse",
            Phase::Sink => "sink",
            Phase::Serialize => "serialize",
            Phase::Write => "write",
        }
    }

    fn index(self) -> usize {
        match self {
            Phase::Read => 0,
            Phase::Decompress => 1,
            Phase::Parse => 2,
            Phase::Sink => 3,
            Phase::Serialize => 4,
            Phase::Write => 5,
        }
    }
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Wall-clock time attributed to each [`Phase`] of one run.
///
/// Used as a lane-switching stopwatch: [`enter`](Self::enter) closes whatever
/// lane is open, attributes the elapsed time to it, and opens the named one.
/// A run therefore costs one clock read per phase transition rather than one
/// per unit of work, which is what keeps the instrument from measuring itself.
///
/// Time can also be attributed out of band with [`add`](Self::add) — that is
/// how the sink phase, which is estimated by sampling inside [`TimingSink`]
/// rather than bracketed, gets into the report.
///
/// ```
/// use fluree_graph_ir::{Phase, PhaseTimings};
///
/// let mut timings = PhaseTimings::start();
/// timings.enter(Phase::Read);
/// // … read the file …
/// timings.enter(Phase::Parse);
/// // … parse it …
/// timings.finish();
///
/// assert!(timings.total() <= timings.wall());
/// ```
#[derive(Clone, Debug)]
pub struct PhaseTimings {
    totals: [Duration; Phase::ALL.len()],
    open: Option<(Phase, Instant)>,
    started: Instant,
    /// Clock reads this accumulator has taken, for overhead disclosure.
    clock_reads: u64,
}

impl PhaseTimings {
    /// Begin a run. The wall clock starts now.
    pub fn start() -> Self {
        Self {
            totals: [Duration::ZERO; Phase::ALL.len()],
            open: None,
            started: Instant::now(),
            clock_reads: 1,
        }
    }

    /// Close the open phase (if any) and open `phase`.
    ///
    /// Re-entering a phase accumulates into the same lane, so an interleaved
    /// read/parse loop can switch back and forth and still report one total
    /// per phase.
    pub fn enter(&mut self, phase: Phase) {
        let now = Instant::now();
        self.clock_reads += 1;
        if let Some((open, since)) = self.open {
            self.totals[open.index()] += now.saturating_duration_since(since);
        }
        self.open = Some((phase, now));
    }

    /// Close the open phase without opening another. Idempotent.
    pub fn finish(&mut self) {
        if let Some((open, since)) = self.open.take() {
            self.clock_reads += 1;
            self.totals[open.index()] += since.elapsed();
        }
    }

    /// Attribute `elapsed` to `phase` directly, on top of anything the
    /// stopwatch has already accumulated there.
    pub fn add(&mut self, phase: Phase, elapsed: Duration) {
        self.totals[phase.index()] += elapsed;
    }

    /// Overwrite `phase`'s total. Used for a phase whose cost is *estimated*
    /// rather than measured — the sink, whose time is sampled by
    /// [`TimingSink`] and would be double-counted if merely added to a lane
    /// that also ran the parser.
    pub fn set(&mut self, phase: Phase, elapsed: Duration) {
        self.totals[phase.index()] = elapsed;
    }

    /// Time attributed to one phase.
    pub fn elapsed(&self, phase: Phase) -> Duration {
        self.totals[phase.index()]
    }

    /// Sum of all phase totals. May exceed [`wall`](Self::wall) when an
    /// estimated phase (the sink) overlaps a measured one (the parse) — the
    /// sink runs *inside* the parse call, so the two are deliberately not
    /// disjoint and shares are reported against the wall clock.
    pub fn total(&self) -> Duration {
        self.totals.iter().copied().sum()
    }

    /// Wall-clock time since [`start`](Self::start).
    pub fn wall(&self) -> Duration {
        self.started.elapsed()
    }

    /// A phase's share of the wall clock, as a percentage.
    ///
    /// Shares are taken against the wall clock rather than the phase sum so
    /// that unattributed time (process startup, allocator warmup) shows up as
    /// the gap instead of being silently redistributed.
    pub fn share_pct(&self, phase: Phase, wall: Duration) -> f64 {
        let wall_ns = wall.as_nanos();
        if wall_ns == 0 {
            return 0.0;
        }
        (self.elapsed(phase).as_nanos() as f64 / wall_ns as f64) * 100.0
    }

    /// Non-zero phases in pipeline order, for reporting.
    pub fn nonzero(&self) -> impl Iterator<Item = (Phase, Duration)> + '_ {
        Phase::ALL
            .into_iter()
            .map(move |p| (p, self.elapsed(p)))
            .filter(|(_, d)| !d.is_zero())
    }

    /// How many `Instant::now()` calls this accumulator has made — the input
    /// to an honest overhead disclosure.
    pub fn clock_reads(&self) -> u64 {
        self.clock_reads
    }
}

impl Default for PhaseTimings {
    fn default() -> Self {
        Self::start()
    }
}

/// Events seen by a [`TimingSink`], counted exactly.
///
/// Counting is unconditional and clock-free: a `+= 1` per event is cheap
/// enough that `count` can be implemented as "parse with this decorator and
/// read the counters", with no separate counting pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SinkCounts {
    /// `emit_triple` calls.
    pub triples: u64,
    /// `emit_quad` calls — non-zero only once a quad producer exists.
    pub quads: u64,
    /// `emit_list_item` calls. Under the parser's indexed-items collection
    /// style these are collection members that did *not* become spine triples,
    /// so a count-vs-count comparison across styles is expected to differ.
    pub list_items: u64,
    /// `emit_reified_triple` calls (RDF 1.2 reifiers).
    pub reified: u64,
    /// Statements that ended successfully.
    pub statements: u64,
    /// Statements rolled back by `abort_statement`.
    pub aborted_statements: u64,
    /// `term_iri` calls. Producers cache IRI ids, so this counts *mints*, not
    /// occurrences.
    pub terms_iri: u64,
    /// `term_blank` calls.
    pub terms_blank: u64,
    /// `term_literal` + `term_literal_value` calls. Literals are minted per
    /// occurrence, so this one does track occurrences.
    pub terms_literal: u64,
    /// `on_prefix` declarations.
    pub prefixes: u64,
    /// `on_base` declarations.
    pub bases: u64,
}

impl SinkCounts {
    /// Total RDF statements emitted: triples, quads, list items, reifiers.
    pub fn emitted(&self) -> u64 {
        self.triples + self.quads + self.list_items + self.reified
    }

    /// Total term mints across all classes.
    pub fn terms(&self) -> u64 {
        self.terms_iri + self.terms_blank + self.terms_literal
    }

    /// Total forwarded calls — the denominator for the sink-time estimate.
    fn calls(&self) -> u64 {
        self.emitted()
            + self.terms()
            + self.statements
            + self.aborted_statements
            + self.prefixes
            + self.bases
    }
}

/// Time one statement in this many, when sampling sink dispatch.
///
/// Prime on purpose. A document is often structurally periodic — N triples per
/// subject, repeated — and a power-of-two stride can land on the same position
/// within every period and sample one shape of statement forever. A prime
/// stride walks the period instead.
pub const SAMPLE_STRIDE: u64 = 127;

/// Wraps any [`GraphSink`] to count its events and estimate its dispatch cost.
///
/// See the module docs for why the cost is sampled rather than bracketed. The
/// decorator is transparent: every method forwards, return values and errors
/// included, so wrapping a sink can change what a pipeline *measures* but never
/// what it *produces*.
pub struct TimingSink<S> {
    inner: S,
    counts: SinkCounts,
    /// Measured dispatch time over sampled statements only.
    sampled_time: Duration,
    /// Calls made during sampled statements — the numerator's denominator.
    sampled_calls: u64,
    /// Whether the statement in flight is being timed.
    sampling: bool,
    clock_reads: u64,
}

impl<S: GraphSink> TimingSink<S> {
    /// Wrap `inner`.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            counts: SinkCounts::default(),
            sampled_time: Duration::ZERO,
            sampled_calls: 0,
            sampling: true, // the first statement is always sampled
            clock_reads: 0,
        }
    }

    /// Events counted so far. Exact.
    pub fn counts(&self) -> SinkCounts {
        self.counts
    }

    /// Estimated total time spent inside the wrapped sink.
    ///
    /// Sampled dispatch time scaled by the ratio of all forwarded calls to
    /// sampled ones. Zero when nothing was sampled. This is an estimate and
    /// callers must present it as one.
    pub fn estimated_sink_time(&self) -> Duration {
        if self.sampled_calls == 0 {
            return Duration::ZERO;
        }
        let scale = self.counts.calls() as f64 / self.sampled_calls as f64;
        Duration::from_nanos((self.sampled_time.as_nanos() as f64 * scale) as u64)
    }

    /// Fraction of forwarded calls that were actually timed, in percent.
    /// A run whose sample is tiny should say so next to the estimate.
    pub fn sampled_pct(&self) -> f64 {
        let calls = self.counts.calls();
        if calls == 0 {
            return 0.0;
        }
        (self.sampled_calls as f64 / calls as f64) * 100.0
    }

    /// Clock reads this decorator made — the other half of the overhead
    /// disclosure, alongside [`PhaseTimings::clock_reads`].
    pub fn clock_reads(&self) -> u64 {
        self.clock_reads
    }

    /// Unwrap, returning the sink that was being measured.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Borrow the wrapped sink.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Run `f` against the inner sink, timing it only on sampled statements.
    ///
    /// On the ~99.2% of statements that are not sampled this compiles down to
    /// the inner call plus a predictable branch.
    #[inline]
    fn forward<T>(&mut self, f: impl FnOnce(&mut S) -> T) -> T {
        if !self.sampling {
            return f(&mut self.inner);
        }
        let start = Instant::now();
        let out = f(&mut self.inner);
        self.sampled_time += start.elapsed();
        self.sampled_calls += 1;
        self.clock_reads += 2;
        out
    }

    /// Decide whether the next statement is sampled. Called at every
    /// statement boundary.
    fn roll_sample(&mut self) {
        let seen = self.counts.statements + self.counts.aborted_statements;
        self.sampling = seen.is_multiple_of(SAMPLE_STRIDE);
    }
}

impl<S: GraphSink> GraphSink for TimingSink<S> {
    fn on_base(&mut self, base_iri: &str) {
        self.counts.bases += 1;
        self.forward(|s| s.on_base(base_iri));
    }

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.counts.prefixes += 1;
        self.forward(|s| s.on_prefix(prefix, namespace_iri));
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.counts.terms_iri += 1;
        self.forward(|s| s.term_iri(iri))
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.counts.terms_blank += 1;
        self.forward(|s| s.term_blank(label))
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.counts.terms_literal += 1;
        self.forward(|s| s.term_literal(value, datatype, language))
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.counts.terms_literal += 1;
        self.forward(|s| s.term_literal_value(value, datatype))
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.counts.triples += 1;
        self.forward(|s| s.emit_triple(subject, predicate, object))
    }

    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        self.counts.list_items += 1;
        self.forward(|s| s.emit_list_item(subject, predicate, object, index))
    }

    fn supports_quads(&self) -> bool {
        self.inner.supports_quads()
    }

    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        self.counts.quads += 1;
        self.forward(|s| s.emit_quad(subject, predicate, object, graph))
    }

    fn supports_reified_triples(&self) -> bool {
        self.inner.supports_reified_triples()
    }

    fn emit_reified_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        reifier: TermId,
    ) -> SinkResult {
        self.counts.reified += 1;
        self.forward(|s| s.emit_reified_triple(subject, predicate, object, reifier))
    }

    fn end_statement(&mut self) {
        self.forward(GraphSink::end_statement);
        self.counts.statements += 1;
        self.roll_sample();
    }

    fn abort_statement(&mut self) {
        self.forward(GraphSink::abort_statement);
        self.counts.aborted_statements += 1;
        self.roll_sample();
    }

    fn finish(&mut self) -> SinkResult {
        // Always timed: `finish` runs once and is where a writing sink flushes,
        // which is exactly the cost worth knowing.
        let start = Instant::now();
        let out = self.inner.finish();
        self.sampled_time += start.elapsed();
        self.sampled_calls += 1;
        self.clock_reads += 2;
        out
    }
}

/// Measure the cost of one `Instant::now()` pair on this host, in nanoseconds.
///
/// A profiler that cannot price its own instrument has no business reporting
/// phase shares. Callers multiply this by their clock-read count to state the
/// overhead they imposed, rather than asserting it was negligible.
///
/// Deliberately cheap (a few microseconds) so it can run on every profiled
/// invocation instead of being a separate calibration mode.
pub fn clock_pair_cost() -> Duration {
    const ROUNDS: u32 = 1_000;
    // One warmup pass so the first read doesn't pay for a cold clock source.
    for _ in 0..64 {
        std::hint::black_box(Instant::now());
    }
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let t = Instant::now();
        std::hint::black_box(t.elapsed());
    }
    start.elapsed() / ROUNDS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GraphCollectorSink;

    #[test]
    fn phases_accumulate_per_lane_and_reentering_adds() {
        let mut t = PhaseTimings::start();
        t.enter(Phase::Read);
        std::thread::sleep(Duration::from_millis(2));
        t.enter(Phase::Parse);
        std::thread::sleep(Duration::from_millis(2));
        t.enter(Phase::Read); // back to a lane already used
        std::thread::sleep(Duration::from_millis(2));
        t.finish();

        assert!(t.elapsed(Phase::Read) >= Duration::from_millis(4));
        assert!(t.elapsed(Phase::Parse) >= Duration::from_millis(2));
        assert_eq!(t.elapsed(Phase::Write), Duration::ZERO);
        assert!(t.total() <= t.wall());
    }

    #[test]
    fn finish_is_idempotent_and_closes_the_open_lane() {
        let mut t = PhaseTimings::start();
        t.enter(Phase::Parse);
        std::thread::sleep(Duration::from_millis(1));
        t.finish();
        let after_first = t.elapsed(Phase::Parse);
        t.finish();
        assert_eq!(
            t.elapsed(Phase::Parse),
            after_first,
            "a second finish must not re-attribute time"
        );
    }

    #[test]
    fn a_four_phase_run_costs_a_handful_of_clock_reads_not_one_per_event() {
        // The whole reason the accumulator switches lanes instead of
        // bracketing work: cost is O(transitions), not O(work).
        let mut t = PhaseTimings::start();
        for phase in [Phase::Read, Phase::Decompress, Phase::Parse, Phase::Write] {
            t.enter(phase);
        }
        t.finish();
        assert_eq!(t.clock_reads(), 6, "start + 4 transitions + finish");
    }

    #[test]
    fn shares_are_taken_against_the_wall_clock_so_gaps_stay_visible() {
        let mut t = PhaseTimings::start();
        t.add(Phase::Parse, Duration::from_millis(25));
        let wall = Duration::from_millis(100);
        assert!((t.share_pct(Phase::Parse, wall) - 25.0).abs() < 0.001);
        // Unattributed time is not redistributed into the phases that ran.
        assert_eq!(t.share_pct(Phase::Write, wall), 0.0);
    }

    #[test]
    fn share_of_a_zero_length_run_is_zero_not_a_division_by_zero() {
        let t = PhaseTimings::start();
        assert_eq!(t.share_pct(Phase::Parse, Duration::ZERO), 0.0);
    }

    #[test]
    fn nonzero_reports_phases_in_pipeline_order() {
        let mut t = PhaseTimings::start();
        t.add(Phase::Write, Duration::from_millis(1));
        t.add(Phase::Read, Duration::from_millis(1));
        let order: Vec<Phase> = t.nonzero().map(|(p, _)| p).collect();
        assert_eq!(order, vec![Phase::Read, Phase::Write]);
    }

    #[test]
    fn set_replaces_rather_than_adds_for_estimated_phases() {
        // The sink estimate must not stack on whatever a lane already held —
        // sink time runs inside the parse call and would double-count.
        let mut t = PhaseTimings::start();
        t.add(Phase::Sink, Duration::from_millis(5));
        t.set(Phase::Sink, Duration::from_millis(2));
        assert_eq!(t.elapsed(Phase::Sink), Duration::from_millis(2));
    }

    // ------------------------------------------------------------------
    // TimingSink
    // ------------------------------------------------------------------

    fn drive(sink: &mut TimingSink<GraphCollectorSink>, statements: u64) {
        let s = sink.term_iri("http://example.org/s");
        let p = sink.term_iri("http://example.org/p");
        for i in 0..statements {
            let o = sink.term_literal(&format!("v{i}"), Datatype::xsd_string(), None);
            sink.emit_triple(s, p, o).unwrap();
            sink.end_statement();
        }
    }

    #[test]
    fn counts_every_event_class_exactly() {
        let mut sink = TimingSink::new(GraphCollectorSink::new());
        sink.on_base("http://example.org/");
        sink.on_prefix("ex", "http://example.org/");
        drive(&mut sink, 10);
        let b = sink.term_blank(Some("b0"));
        let p = sink.term_iri("http://example.org/list");
        let o = sink.term_literal("x", Datatype::xsd_string(), None);
        sink.emit_list_item(b, p, o, 0).unwrap();
        sink.abort_statement();

        let c = sink.counts();
        assert_eq!(c.triples, 10);
        assert_eq!(c.list_items, 1);
        assert_eq!(c.statements, 10);
        assert_eq!(c.aborted_statements, 1);
        assert_eq!(c.terms_literal, 11);
        assert_eq!(c.terms_iri, 3, "s, p, and the list predicate");
        assert_eq!(c.terms_blank, 1);
        assert_eq!(c.prefixes, 1);
        assert_eq!(c.bases, 1);
        assert_eq!(c.emitted(), 11);
        assert_eq!(c.terms(), 15);
    }

    #[test]
    fn decoration_does_not_change_what_the_inner_sink_produces() {
        let mut wrapped = TimingSink::new(GraphCollectorSink::new());
        drive(&mut wrapped, 25);
        let via_decorator = wrapped.into_inner().into_graph();

        let mut bare = GraphCollectorSink::new();
        let s = bare.term_iri("http://example.org/s");
        let p = bare.term_iri("http://example.org/p");
        for i in 0..25 {
            let o = bare.term_literal(&format!("v{i}"), Datatype::xsd_string(), None);
            bare.emit_triple(s, p, o).unwrap();
            bare.end_statement();
        }
        let direct = bare.into_graph();

        assert_eq!(via_decorator.len(), direct.len());
        assert_eq!(
            via_decorator.iter().collect::<Vec<_>>(),
            direct.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn clock_reads_scale_with_statements_over_the_stride_not_with_events() {
        // The F5 lesson made mechanical: 10_000 statements' worth of events
        // must not buy 10_000 statements' worth of clock reads.
        let statements = 10_000u64;
        let mut sink = TimingSink::new(GraphCollectorSink::new());
        drive(&mut sink, statements);

        let calls = sink.counts().calls();
        assert!(calls > 30_000, "sanity: the run really did emit a lot");
        assert!(
            sink.clock_reads() < calls / 20,
            "sampled timing took {} clock reads across {calls} calls — that is \
             per-call timing wearing a hat",
            sink.clock_reads()
        );
        assert!(
            sink.sampled_pct() < 5.0,
            "sampled {}% of calls",
            sink.sampled_pct()
        );
    }

    #[test]
    fn the_first_statement_is_always_sampled_so_short_runs_still_report() {
        // A three-statement document is shorter than the stride; without the
        // always-sample-the-first rule its sink estimate would be a flat zero.
        let mut sink = TimingSink::new(GraphCollectorSink::new());
        drive(&mut sink, 3);
        assert!(sink.clock_reads() > 0);
        assert!(sink.sampled_pct() > 0.0);
    }

    #[test]
    fn estimated_sink_time_is_zero_when_nothing_was_forwarded() {
        let sink: TimingSink<GraphCollectorSink> = TimingSink::new(GraphCollectorSink::new());
        assert_eq!(sink.estimated_sink_time(), Duration::ZERO);
        assert_eq!(sink.sampled_pct(), 0.0);
    }

    #[test]
    fn capability_probes_pass_through_to_the_wrapped_sink() {
        // A decorator that answered these itself would let a quad producer
        // think a triple-only sink could take quads.
        let sink = TimingSink::new(GraphCollectorSink::new());
        assert!(!sink.supports_quads());
        assert!(!sink.supports_reified_triples());
    }

    #[test]
    fn errors_from_the_wrapped_sink_propagate_unchanged() {
        struct RefusingSink;
        impl GraphSink for RefusingSink {
            fn on_base(&mut self, _: &str) {}
            fn on_prefix(&mut self, _: &str, _: &str) {}
            fn term_iri(&mut self, _: &str) -> TermId {
                TermId::new(0)
            }
            fn term_blank(&mut self, _: Option<&str>) -> TermId {
                TermId::new(0)
            }
            fn term_literal(&mut self, _: &str, _: Datatype, _: Option<&str>) -> TermId {
                TermId::new(0)
            }
            fn term_literal_value(&mut self, _: LiteralValue, _: Datatype) -> TermId {
                TermId::new(0)
            }
            fn emit_triple(&mut self, _: TermId, _: TermId, _: TermId) -> SinkResult {
                Err(crate::SinkError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "closed",
                )))
            }
        }

        let mut sink = TimingSink::new(RefusingSink);
        let id = TermId::new(0);
        let err = sink.emit_triple(id, id, id).unwrap_err();
        assert!(
            err.is_broken_pipe(),
            "the decorator must not swallow or reshape the failure"
        );
        assert_eq!(sink.counts().triples, 1, "the refused call still counted");
    }

    #[test]
    fn clock_pair_cost_is_positive_and_plausible() {
        let cost = clock_pair_cost();
        assert!(cost > Duration::ZERO, "a clock read is not free");
        assert!(
            cost < Duration::from_micros(10),
            "a clock pair costing {cost:?} means the calibration loop measured \
             something other than the clock"
        );
    }
}
