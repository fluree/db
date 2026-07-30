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
//! # Why the sink is sampled, and what that costs
//!
//! The obvious implementation — bracket every forwarded call with
//! `Instant::now()` — is the one that cannot be trusted. A `sink` phase for a
//! parse that emits ten million triples would take twenty million clock reads,
//! and on the counting sinks used by `check`/`count` a clock read costs *more
//! than the work it is measuring*: measured on an M-series host, a bracketed
//! `DiscardSink` call costs 20.3 ns of which ~19 ns is the clock pair. A
//! fully-bracketed profile of that sink reports, almost entirely, itself.
//!
//! So: **count on every call, take clocks on a sample of statements.**
//! Counters are integer increments with no clock at all. On a sampled
//! statement the decorator brackets each forwarded call, and the measured cost
//! is scaled by the call ratio.
//!
//! That estimator has three failure modes, and all three are handled here
//! rather than left for a reader to discover:
//!
//! 1. **The clock is inside the sample.** Scaling raw sampled time multiplies
//!    the clock artifact along with the work. [`SinkTiming`] subtracts
//!    `sampled_calls × clock_pair_cost` *before* scaling, and publishes the
//!    extrapolated artifact ([`SinkTiming::artifact`]) so a consumer can see
//!    how much of the run the instrument could account for.
//! 2. **Small differences are unresolvable.** Below
//!    [`FLOOR_MULTIPLE`]× the artifact, a scaled estimate is indistinguishable
//!    from calibration error. [`SinkTiming::body`] is `None` there —
//!    "below the measurement floor", not a number.
//! 3. **Periodic corpora alias against a fixed stride.** A corpus whose
//!    statement shapes repeat with period P, sampled every 127 statements,
//!    shows the sampler only the residues of P that 127 reaches; when 127
//!    divides P that is a single shape forever, and the estimate is wrong by
//!    whatever ratio that shape differs from the mean. Measured with the
//!    review probe: 425× over-reported when the expensive shape is the one
//!    sampled. Mitigated by *jittering* the gap — see [`SAMPLE_STRIDE`].
//!
//! [`finish`](GraphSink::finish) is timed exactly and kept out of the scaled
//! body entirely ([`SinkTiming::finish`]): it runs once, and scaling a
//! writer's one-time flush by the sample ratio turned 50 ms into 6.4 s.
//!
//! # What this is not
//!
//! It is not a defence against a corpus built to defeat it. The sampling
//! schedule is a deterministic function of the corpus — that is what makes two
//! profiles of one input comparable — and both [`corpus_seed`] and the
//! generator are public, so anyone able to choose the input can compute
//! exactly which statements will be timed and put the expensive work
//! elsewhere. A reviewer did precisely that and hid 315 ms of real sink cost.
//!
//! Reproducible and unpredictable are incompatible, and reproducible is the
//! one a baseline needs. So the guarantee here is against *accidental*
//! structure — the periodic corpora that occur naturally, which is what the
//! probe found and what a fixed stride mishandles — and not against a
//! constructed one. The floor and [`SinkTiming::relative_std_error`] bound how
//! much an ordinary sample can be trusted; neither can see work that was
//! deliberately placed where nobody was looking. Treat the sink estimate as an
//! observation about a cooperating corpus, never as a measurement of an
//! untrusted one.

use crate::{Datatype, GraphSink, LiteralValue, SinkResult, TermId, TermScope};
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
    /// Cutting the document into independently-parseable chunks.
    ///
    /// Single-threaded and whole-document, so it is the parallel path's Amdahl
    /// term: it runs to completion before the first worker starts and no
    /// thread count touches it.
    ///
    /// It measures the ATTEMPT, not the outcome. A run that never tries —
    /// `--parallelism 1`, an input under the size gate — reports zero. A run
    /// that tries and then falls back to serial, because a mid-file directive
    /// or an oversized header makes the document unchunkable, reports what the
    /// attempt cost while also reporting `threads_used: 1`. That pairing is
    /// not a contradiction to be explained away: the scan really did run, and
    /// on a fallback it is pure overhead, which is exactly the case worth
    /// being able to see.
    ///
    /// It has its own lane because it went unattributed for a whole bucket and
    /// turned out to be 43% of the wall at 16 threads. Time that no lane owns
    /// is time nobody optimizes.
    Chunk,
    /// Lexing and parsing the decoded text into sink events.
    Parse,
    /// Parse time summed across parallel workers.
    ///
    /// Deliberately not part of the sequential sum: it is a sum over threads,
    /// so on a run that scales it EXCEEDS the wall clock, and adding it to the
    /// pipeline total would claim more time than the run took. Read it against
    /// `parse` to see the speedup — `workers / parse` is the effective width.
    Workers,
    /// Wall time the ordered replay took, waiting on chunks and writing them.
    ///
    /// Near-zero means the worker pool is the bottleneck; large means
    /// reassembly is, which is the number that says whether more threads would
    /// help.
    Reassembly,
    /// Sink dispatch: what the consumer of the events does with them.
    Sink,
    /// Rendering terms back into an output syntax.
    Serialize,
    /// Pushing serialized bytes to their destination.
    Write,
}

impl Phase {
    /// Every phase, in pipeline order. Report ordering follows this.
    pub const ALL: [Phase; 9] = [
        Phase::Read,
        Phase::Decompress,
        Phase::Chunk,
        Phase::Parse,
        Phase::Workers,
        Phase::Reassembly,
        Phase::Sink,
        Phase::Serialize,
        Phase::Write,
    ];

    /// Phases that run one after another and together account for the wall
    /// clock. Everything else happens *inside* one of these.
    ///
    /// The distinction is what makes "unattributed time" meaningful. On the
    /// streaming path the sink, the serializer and the writer all run during
    /// the parse — the writer is called from inside the parse loop — and the
    /// parallel phases are stranger still: `Workers` is a sum across threads
    /// that exceeds the wall clock whenever the run scales at all. Adding any
    /// of them to a pipeline total would claim more time than the run took,
    /// and the gap that is supposed to reveal unmeasured work would saturate
    /// to zero and reveal nothing.
    pub const SEQUENTIAL: [Phase; 4] = [Phase::Read, Phase::Decompress, Phase::Chunk, Phase::Parse];

    /// Whether this phase runs inside another rather than beside it.
    pub fn is_nested(self) -> bool {
        !Self::SEQUENTIAL.contains(&self)
    }

    /// Stable machine-readable name — the key used in `--profile=json`.
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Read => "read",
            Phase::Decompress => "decompress",
            Phase::Chunk => "chunk",
            Phase::Parse => "parse",
            Phase::Workers => "workers",
            Phase::Reassembly => "reassembly",
            Phase::Sink => "sink",
            Phase::Serialize => "serialize",
            Phase::Write => "write",
        }
    }

    fn index(self) -> usize {
        match self {
            Phase::Read => 0,
            Phase::Decompress => 1,
            Phase::Chunk => 2,
            Phase::Parse => 3,
            Phase::Workers => 4,
            Phase::Reassembly => 5,
            Phase::Sink => 6,
            Phase::Serialize => 7,
            Phase::Write => 8,
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
    /// *Grammar* statements that ended successfully — Turtle's
    /// `statement ::= directive | triples '.'`, so a `@prefix` line counts as
    /// one and a `s p o1, o2 ; q o3 .` line also counts as one while emitting
    /// three triples.
    ///
    /// Deliberately not the same quantity as [`Self::triples`], and never
    /// labelled just "statements" at a user boundary: the two differ by both
    /// directives and predicate-object lists.
    pub statements: u64,
    /// Grammar statements rolled back by `abort_statement`.
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
    /// Total RDF triples emitted, counting quads, list items and reifiers —
    /// everything that becomes an edge in the graph. This is the number a user
    /// means by "how big is this file", and the one `fluree rdf count` prints.
    pub fn emitted(&self) -> u64 {
        self.triples + self.quads + self.list_items + self.reified
    }

    /// Total term mints across all classes.
    pub fn terms(&self) -> u64 {
        self.terms_iri + self.terms_blank + self.terms_literal
    }

    /// Total forwarded calls — the denominator for the sink-time estimate.
    pub fn calls(&self) -> u64 {
        self.emitted()
            + self.terms()
            + self.statements
            + self.aborted_statements
            + self.prefixes
            + self.bases
    }
}

/// Mean number of statements between sampled ones.
///
/// The gap is *jittered*, not fixed: after each sampled statement the next one
/// is drawn uniformly from `1..2×SAMPLE_STRIDE`, so the mean gap is this value
/// and no gap is predictable from the last.
///
/// A fixed stride cannot survive a periodic corpus. Sampling every `k`
/// statements of a corpus whose shapes repeat with period `P` reaches only
/// `P / gcd(k, P)` of the `P` shapes; a prime `k` makes that all of them
/// *unless `k` divides `P`*, and then it is exactly one shape, forever. The
/// review probe built that corpus — one fat statement per 127 — and measured a
/// 425× over-report when the fat shape was the sampled one and a complete miss
/// when it was not. Choosing the offset per corpus would only move which
/// corpora are wrong.
///
/// Jitter addresses the failure mode rather than relocating it: an aperiodic
/// schedule has no residue class to be confined to, so no *naturally* periodic
/// corpus — whatever its period — can systematically hide from it or
/// systematically dominate it. That is the class the probe found.
///
/// It is not unpredictable. The draw is seeded from the corpus
/// ([`corpus_seed`]) so a given input samples the same statements on every
/// run, which is what makes two profiles of it comparable; and since seed and
/// generator are both public, anyone who chooses the input can replay the
/// schedule and arrange for the expensive statements to fall in the gaps. See
/// the module docs — that trade is deliberate, and it means the estimate
/// describes a cooperating corpus, not an adversarial one.
///
/// What remains for an ordinary corpus is sampling error, which shrinks as
/// `1/√n` in the number of sampled statements, is reported per-run as
/// [`SinkTiming::relative_std_error`], and is bounded from below by
/// [`SinkTiming::floor`].
pub const SAMPLE_STRIDE: u64 = 127;

/// How many times the instrument artifact an estimate must exceed before it is
/// reported as a number rather than as "below the measurement floor".
///
/// Three is the review's bound. Below it, a scaled estimate is within the
/// error of the calibration that was subtracted from it, and reporting it
/// would be reporting the instrument.
pub const FLOOR_MULTIPLE: u32 = 3;

/// Derive a deterministic sampling seed from a corpus.
///
/// FNV-1a over the head of the document plus its length: cheap enough to run
/// unconditionally (no relation to the optional `--no-hash` fingerprint, which
/// is a SHA-256 over the whole input), and stable, so re-running a profile on
/// the same corpus samples the same statements and produces a comparable
/// number.
pub fn corpus_seed(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x1000_0000_01b3;
    // A 4 KiB head is plenty to separate corpora, and bounds the cost on a
    // multi-gigabyte input.
    let head = &bytes[..bytes.len().min(4096)];
    let mut hash = FNV_OFFSET;
    for byte in head {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    for byte in (bytes.len() as u64).to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// What a [`TimingSink`] can honestly say about the sink it wrapped.
///
/// Deliberately not a single `Duration`. The per-event body is an estimate
/// that may not be resolvable at all; `finish` is exact; and the artifact and
/// floor are what let a consumer judge the first two rather than trust them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SinkTiming {
    /// Estimated time inside the sink's per-event methods, with the clock
    /// artifact removed before scaling.
    ///
    /// `None` when the estimate does not clear [`FLOOR_MULTIPLE`]× [`Self::floor`]
    /// — the sink is cheap enough that this instrument cannot separate its cost
    /// from its own. Callers must render that as "below the measurement floor",
    /// never as zero: the cost is unresolved, not absent.
    pub body: Option<Duration>,
    /// Time inside [`GraphSink::finish`], measured exactly.
    ///
    /// Kept out of [`Self::body`] because it runs once: folding it into the
    /// sampled body would scale a writer's one-time flush by the sample ratio.
    /// Zero when the measurement is within a clock pair of nothing — a sink
    /// whose `finish` just returns `Ok(())` has no flush cost to report.
    pub finish: Duration,
    /// Clock cost embedded in an estimate scaled to the full call count:
    /// `calls × clock_pair_cost`. The resolution limit of the instrument.
    pub artifact: Duration,
    /// Total forwarded calls.
    pub calls: u64,
    /// Calls that were actually timed.
    pub sampled_calls: u64,
    /// Statements that were sampled.
    pub sampled_statements: u64,
    /// `Instant::now()` calls the decorator made.
    pub clock_reads: u64,
    /// Measured cost of one clock pair on this host.
    pub clock_pair: Duration,
    /// Standard error of the mean sampled statement cost, as a fraction of
    /// that mean. `None` with fewer than two samples.
    ///
    /// The spread the extrapolation is riding on: a corpus of uniform
    /// statements gives a small number, one whose statements vary wildly gives
    /// a large one, and a large one means [`Self::body`] should be read as an
    /// order of magnitude rather than a figure. It bounds ordinary sampling
    /// error only — see the module docs for what it cannot see.
    pub relative_std_error: Option<f64>,
}

impl SinkTiming {
    /// The smallest body estimate that would be reported rather than
    /// suppressed: [`FLOOR_MULTIPLE`] × [`Self::artifact`].
    ///
    /// This is the actual threshold, not the artifact it derives from —
    /// printing the artifact under the name "floor" understated the bar by
    /// a factor of three.
    pub fn floor(&self) -> Duration {
        self.artifact
            .saturating_mul(FLOOR_MULTIPLE)
            .min(Duration::MAX)
    }

    /// The floor expressed per forwarded call: the per-event cost the sink
    /// would have to exceed for this instrument to see it.
    ///
    /// The aggregate floor is the honest threshold but a useless quantity to
    /// show a reader — "under 82 ms" across 720,004 calls sounds enormous and
    /// means 114 ns. `None` when nothing was forwarded.
    pub fn floor_per_call(&self) -> Option<Duration> {
        (self.calls > 0).then(|| self.floor() / u32::try_from(self.calls).unwrap_or(u32::MAX))
    }

    /// Whether the per-event cost could not be resolved.
    pub fn below_floor(&self) -> bool {
        self.body.is_none()
    }

    /// What to attribute to the sink phase: the resolvable body plus the exact
    /// finish. Zero when neither resolved — pair with [`Self::below_floor`] so
    /// "unresolved" is never rendered as "free".
    pub fn reportable(&self) -> Duration {
        self.body.unwrap_or(Duration::ZERO) + self.finish
    }

    /// Share of forwarded calls that were timed.
    pub fn sampled_pct(&self) -> f64 {
        if self.calls == 0 {
            return 0.0;
        }
        (self.sampled_calls as f64 / self.calls as f64) * 100.0
    }
}

/// Wraps any [`GraphSink`] to count its events and estimate its dispatch cost.
///
/// See the module docs for why the cost is sampled rather than bracketed. The
/// decorator is transparent: every method forwards, return values and errors
/// included, so wrapping a sink can change what a pipeline *measures* but never
/// what it *produces*.
pub struct TimingSink<S> {
    inner: S,
    counts: SinkCounts,
    /// Measured dispatch time over sampled statements only, `finish` excluded.
    sampled_time: Duration,
    /// Calls made during sampled statements — the estimate's denominator.
    sampled_calls: u64,
    /// Statements sampled, for the sampling-error picture.
    sampled_statements: u64,
    /// Time accumulated by the statement currently being sampled, so each
    /// sampled statement contributes one observation to the variance.
    current_statement: Duration,
    /// Welford running mean and sum-of-squared-deviations over per-statement
    /// sampled costs, in nanoseconds. One-pass and numerically stable, which
    /// a naive sum-of-squares is not at nanosecond scale.
    welford_mean: f64,
    welford_m2: f64,
    /// Whether the statement in flight is being timed.
    sampling: bool,
    /// Statement index at which to start sampling again.
    next_sample_at: u64,
    /// xorshift64 state, seeded from the corpus.
    rng: u64,
    /// `finish()` cost, measured exactly and never scaled.
    finish_time: Duration,
    /// Cost of one clock pair on this host, measured once at construction so
    /// the correction and the disclosure use the same number.
    clock_pair: Duration,
    clock_reads: u64,
    /// Which statements were sampled. Test-only: the sampler's distribution is
    /// the property under test and there is no way to observe it from outside.
    #[cfg(test)]
    sampled_statement_indices: Vec<u64>,
}

impl<S: GraphSink> TimingSink<S> {
    /// Wrap `inner`, sampling from a fixed seed.
    ///
    /// Prefer [`TimingSink::with_corpus`] where the input is known: a
    /// corpus-derived seed keeps a given input sampling the same statements
    /// across runs, which is what makes two profiles of it comparable.
    pub fn new(inner: S) -> Self {
        Self::with_seed(inner, 0x9e37_79b9_7f4a_7c15)
    }

    /// Wrap `inner`, seeding the sampler from the corpus about to be parsed.
    pub fn with_corpus(inner: S, corpus: &[u8]) -> Self {
        Self::with_seed(inner, corpus_seed(corpus))
    }

    /// Wrap `inner` with an explicit sampling seed.
    pub fn with_seed(inner: S, seed: u64) -> Self {
        // xorshift64 is dead at zero.
        let mut sink = Self {
            inner,
            counts: SinkCounts::default(),
            sampled_time: Duration::ZERO,
            sampled_calls: 0,
            sampled_statements: 0,
            current_statement: Duration::ZERO,
            welford_mean: 0.0,
            welford_m2: 0.0,
            sampling: false,
            next_sample_at: 0,
            rng: if seed == 0 {
                0xdead_beef_cafe_f00d
            } else {
                seed
            },
            finish_time: Duration::ZERO,
            clock_pair: clock_pair_cost(),
            clock_reads: 0,
            #[cfg(test)]
            sampled_statement_indices: Vec::new(),
        };
        sink.next_sample_at = sink.draw_gap();
        sink.sampling = sink.next_sample_at == 0;
        sink
    }

    /// Events counted so far. Exact.
    pub fn counts(&self) -> SinkCounts {
        self.counts
    }

    /// What can honestly be said about the wrapped sink's cost.
    ///
    /// The correction, in order: subtract the clock pair the decorator itself
    /// added to each *sampled* call, scale what is left by the call ratio, and
    /// refuse to report the result at all unless it clears
    /// [`FLOOR_MULTIPLE`]× the artifact that scaling would have carried.
    pub fn sink_timing(&self) -> SinkTiming {
        let calls = self.counts.calls();
        let clock_ns = self.clock_pair.as_nanos();
        let artifact_ns = clock_ns.saturating_mul(u128::from(calls));

        let body = self.estimate_body_ns(calls, clock_ns).and_then(|ns| {
            let floor = artifact_ns.saturating_mul(u128::from(FLOOR_MULTIPLE));
            (ns >= floor).then(|| duration_from_nanos_u128(ns))
        });

        // The same rule, applied to the one exact measurement. `finish` is a
        // single bracketed call, so its floor is a single clock pair rather
        // than a scaled one — but not *only* that: a no-op `finish` still
        // measures 100–200 ns of dispatch and clock variance, which is over
        // three clock pairs and would print as a flush cost that does not
        // exist. A flush worth naming is not a sub-microsecond one, so the
        // floor is whichever of the two is larger.
        const FINISH_FLOOR_NS: u128 = 1_000;
        let finish_floor = clock_ns
            .saturating_mul(u128::from(FLOOR_MULTIPLE))
            .max(FINISH_FLOOR_NS);
        let finish = if self.finish_time.as_nanos() >= finish_floor {
            self.finish_time
        } else {
            Duration::ZERO
        };

        SinkTiming {
            body,
            finish,
            artifact: duration_from_nanos_u128(artifact_ns),
            calls,
            sampled_calls: self.sampled_calls,
            sampled_statements: self.sampled_statements,
            clock_reads: self.clock_reads,
            clock_pair: self.clock_pair,
            relative_std_error: self.relative_std_error(),
        }
    }

    /// Sampled time net of the decorator's own clock cost, scaled to the full
    /// call count. `None` when nothing was sampled.
    fn estimate_body_ns(&self, calls: u64, clock_ns: u128) -> Option<u128> {
        if self.sampled_calls == 0 {
            return None;
        }
        // Every sampled call paid for one clock pair that the sink did not.
        // Subtracting before scaling is the whole correction: scaling first
        // multiplies the artifact by the same ratio as the work.
        let sampled_artifact = clock_ns.saturating_mul(u128::from(self.sampled_calls));
        let net = self
            .sampled_time
            .as_nanos()
            .saturating_sub(sampled_artifact);
        Some(net.saturating_mul(u128::from(calls)) / u128::from(self.sampled_calls))
    }

    /// Clock reads this decorator made — the other half of the overhead
    /// disclosure, alongside [`PhaseTimings::clock_reads`].
    pub fn clock_reads(&self) -> u64 {
        self.clock_reads
    }

    /// Fold the statement that just finished into the running variance, then
    /// reset the per-statement accumulator.
    ///
    /// The observation is per *statement*, not per call: statements are what
    /// the sampler chooses between, so they are the unit whose spread bounds
    /// the extrapolation.
    fn observe_statement(&mut self) {
        let x = self.current_statement.as_nanos() as f64;
        self.current_statement = Duration::ZERO;
        let n = self.sampled_statements as f64;
        let delta = x - self.welford_mean;
        self.welford_mean += delta / n;
        self.welford_m2 += delta * (x - self.welford_mean);
    }

    /// Relative standard error of the mean sampled statement cost.
    fn relative_std_error(&self) -> Option<f64> {
        let n = self.sampled_statements;
        if n < 2 || self.welford_mean <= 0.0 {
            return None;
        }
        let variance = self.welford_m2 / (n - 1) as f64;
        let std_error = variance.sqrt() / (n as f64).sqrt();
        Some(std_error / self.welford_mean)
    }

    /// Draw the next jittered gap: uniform over `0..2×SAMPLE_STRIDE - 1`, so
    /// the mean gap between sampled statements is [`SAMPLE_STRIDE`].
    fn draw_gap(&mut self) -> u64 {
        self.next_u64() % (2 * SAMPLE_STRIDE - 1)
    }

    /// xorshift64. Not cryptographic and does not need to be — it needs to be
    /// cheap enough to run at every statement boundary and to have no period
    /// a corpus could share.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
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
        let elapsed = start.elapsed();
        self.sampled_time += elapsed;
        self.current_statement += elapsed;
        self.sampled_calls += 1;
        self.clock_reads += 2;
        out
    }

    /// Decide whether the next statement is sampled. Called at every
    /// statement boundary.
    fn roll_sample(&mut self) {
        let seen = self.counts.statements + self.counts.aborted_statements;
        if self.sampling {
            self.sampled_statements += 1;
            self.observe_statement();
            // `seen` already counts the statement that just ended.
            #[cfg(test)]
            self.sampled_statement_indices.push(seen - 1);
        }
        if seen >= self.next_sample_at {
            self.sampling = true;
            self.next_sample_at = seen + 1 + self.draw_gap();
        } else {
            self.sampling = false;
        }
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

    /// The same event as `term_iri`, counted and timed identically — a sink
    /// decorator that let this one through uncounted would silently drop every
    /// IRI from the profile the moment a producer started sharing.
    fn term_iri_shared(&mut self, iri: &std::sync::Arc<str>) -> TermId {
        self.counts.terms_iri += 1;
        self.forward(|s| s.term_iri_shared(iri))
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

    /// Forwarded: a decorator that swallowed this would leave the sink on the
    /// conservative scope and silently give up the recycling the producer
    /// offered.
    fn declare_term_scope(&mut self, scope: TermScope) {
        self.inner.declare_term_scope(scope);
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

    fn emit_quad_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
        graph: TermId,
    ) -> SinkResult {
        self.counts.quads += 1;
        self.forward(|s| s.emit_quad_list_item(subject, predicate, object, index, graph))
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
        // Always timed, and into its OWN accumulator. `finish` runs once — it
        // is where a writing sink flushes — so it belongs in no sample: a
        // 50 ms flush routed through the sampled body came back out the other
        // side of the call-ratio scaling as 6.4 seconds.
        let start = Instant::now();
        let out = self.inner.finish();
        self.finish_time += start.elapsed();
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

/// `Duration` from a `u128` nanosecond count, saturating rather than wrapping.
///
/// Every quantity here is derived by multiplying a measured time by a call
/// count, so the arithmetic is done in `u128` and only narrowed at the edge.
fn duration_from_nanos_u128(ns: u128) -> Duration {
    Duration::from_nanos(u64::try_from(ns).unwrap_or(u64::MAX))
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

    // ------------------------------------------------------------------
    // Estimator fixtures.
    //
    // The three sinks below are the review probe's, reduced to what a unit
    // test needs: the cheap sink the estimator got wrong, a sink with real
    // measurable work, and the periodic corpus that defeats a fixed stride.
    // Original probe: scratchpad/sink_bias_probe.rs from the `fluree rdf`
    // adversarial review.
    // ------------------------------------------------------------------

    /// The CLI's `DiscardSink`: accepts everything, keeps nothing. Costs less
    /// per call than the clock used to measure it.
    #[derive(Default)]
    struct DiscardSink {
        next: u32,
        /// Which IRI entry point the decorator forwarded to.
        copying_calls: usize,
        shared_calls: usize,
    }

    impl DiscardSink {
        fn mint(&mut self) -> TermId {
            self.next = self.next.wrapping_add(1);
            TermId::new(self.next)
        }
    }

    impl GraphSink for DiscardSink {
        fn on_base(&mut self, _: &str) {}
        fn on_prefix(&mut self, _: &str, _: &str) {}
        fn term_iri(&mut self, _: &str) -> TermId {
            self.copying_calls += 1;
            self.mint()
        }
        fn term_iri_shared(&mut self, _: &std::sync::Arc<str>) -> TermId {
            self.shared_calls += 1;
            self.mint()
        }
        fn term_blank(&mut self, _: Option<&str>) -> TermId {
            self.mint()
        }
        fn term_literal(&mut self, _: &str, _: Datatype, _: Option<&str>) -> TermId {
            self.mint()
        }
        fn term_literal_value(&mut self, _: LiteralValue, _: Datatype) -> TermId {
            self.mint()
        }
        fn emit_triple(&mut self, _: TermId, _: TermId, _: TermId) -> SinkResult {
            Ok(())
        }
    }

    /// Burns a fixed, real amount of CPU per emitted triple — the sink the
    /// estimator is supposed to be able to measure.
    struct SpinSink {
        per_call: Duration,
        inner: DiscardSink,
    }

    impl SpinSink {
        fn new(per_call: Duration) -> Self {
            Self {
                per_call,
                inner: DiscardSink::default(),
            }
        }
    }

    #[test]
    fn a_shared_iri_reaches_the_inner_sink_shared_and_is_counted_the_same() {
        // Two ways a decorator can quietly break this, neither of which shows
        // up in output bytes. Forwarding `term_iri_shared` to the inner sink's
        // `term_iri` puts the allocation back — the inner sink is storing, and
        // it has to copy. Not counting it drops every IRI out of the profile
        // the moment a producer starts sharing, so `--profile` would report
        // zero IRI terms for a document full of them.
        let mut sink = TimingSink::new(DiscardSink::default());
        let shared: std::sync::Arc<str> = std::sync::Arc::from("http://example.org/a");
        sink.term_iri_shared(&shared);
        sink.term_iri_shared(&shared);
        sink.term_iri("http://example.org/b");

        assert_eq!(
            sink.counts().terms_iri,
            3,
            "both entry points are the same event and must be counted alike"
        );
        let inner = sink.into_inner();
        assert_eq!(
            inner.shared_calls, 2,
            "the decorator must forward the shared form as the shared form"
        );
        assert_eq!(inner.copying_calls, 1, "and the copying form as itself");
    }

    /// Busy-wait. A `sleep` this short is dominated by scheduler granularity;
    /// spinning actually costs the CPU time the test is asserting about.
    fn spin_for(d: Duration) {
        let start = Instant::now();
        while start.elapsed() < d {
            std::hint::spin_loop();
        }
    }

    impl GraphSink for SpinSink {
        fn on_base(&mut self, _: &str) {}
        fn on_prefix(&mut self, _: &str, _: &str) {}
        fn term_iri(&mut self, i: &str) -> TermId {
            self.inner.term_iri(i)
        }
        fn term_blank(&mut self, l: Option<&str>) -> TermId {
            self.inner.term_blank(l)
        }
        fn term_literal(&mut self, v: &str, d: Datatype, l: Option<&str>) -> TermId {
            self.inner.term_literal(v, d, l)
        }
        fn term_literal_value(&mut self, v: LiteralValue, d: Datatype) -> TermId {
            self.inner.term_literal_value(v, d)
        }
        fn emit_triple(&mut self, _: TermId, _: TermId, _: TermId) -> SinkResult {
            spin_for(self.per_call);
            Ok(())
        }
    }

    /// Expensive on exactly one statement residue mod [`SAMPLE_STRIDE`] — the
    /// shape a fixed stride either locks onto or never sees.
    struct PeriodicSink {
        hot_residue: u64,
        statement: u64,
        inner: DiscardSink,
    }

    impl PeriodicSink {
        fn new(hot_residue: u64) -> Self {
            Self {
                hot_residue,
                statement: 0,
                inner: DiscardSink::default(),
            }
        }
    }

    impl GraphSink for PeriodicSink {
        fn on_base(&mut self, _: &str) {}
        fn on_prefix(&mut self, _: &str, _: &str) {}
        fn term_iri(&mut self, i: &str) -> TermId {
            self.inner.term_iri(i)
        }
        fn term_blank(&mut self, l: Option<&str>) -> TermId {
            self.inner.term_blank(l)
        }
        fn term_literal(&mut self, v: &str, d: Datatype, l: Option<&str>) -> TermId {
            self.inner.term_literal(v, d, l)
        }
        fn term_literal_value(&mut self, v: LiteralValue, d: Datatype) -> TermId {
            self.inner.term_literal_value(v, d)
        }
        fn emit_triple(&mut self, _: TermId, _: TermId, _: TermId) -> SinkResult {
            if self.statement % SAMPLE_STRIDE == self.hot_residue {
                spin_for(Duration::from_micros(5));
            }
            Ok(())
        }
        fn end_statement(&mut self) {
            self.statement += 1;
        }
    }

    /// One triple per statement, against any sink.
    fn drive_discard<S: GraphSink>(sink: &mut TimingSink<S>, statements: u64) {
        let s = sink.term_iri("http://example.org/s");
        let p = sink.term_iri("http://example.org/p");
        for _ in 0..statements {
            let o = sink.term_literal("v", Datatype::xsd_string(), None);
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
        let sampled_pct = sink.sink_timing().sampled_pct();
        assert!(sampled_pct < 5.0, "sampled {sampled_pct}% of calls");
    }

    #[test]
    fn a_cheap_sink_is_reported_as_below_the_floor_not_as_a_number() {
        // THE bug this estimator shipped with. A `DiscardSink` costs less per
        // call than the clock pair used to measure it — the review probe put
        // the bracketed call at 20.3 ns against a ~19 ns clock — so a raw
        // scaled estimate was ~98% instrument. It must decline to answer.
        let mut sink = TimingSink::new(DiscardSink::default());
        drive_discard(&mut sink, 20_000);
        let t = sink.sink_timing();

        assert!(
            t.below_floor(),
            "a sink cheaper than the clock reported {:?}, floor {:?}",
            t.body,
            t.floor()
        );
        assert_eq!(t.reportable(), Duration::ZERO);
        assert!(t.artifact > Duration::ZERO);
        assert!(t.calls > 60_000);
    }

    #[test]
    fn a_sink_far_above_the_floor_is_reported_as_a_number() {
        // The estimator has to still work when there is something to measure.
        // 40 µs per statement over 2_000 statements is ~80 ms of real work,
        // orders above any clock artifact.
        let mut sink = TimingSink::new(SpinSink::new(Duration::from_micros(40)));
        drive_discard(&mut sink, 2_000);
        let t = sink.sink_timing();

        let body = t.body.expect("real work must clear the floor");
        assert!(body > t.floor(), "{body:?} vs floor {:?}", t.floor());

        // A LOWER bound only, and the asymmetry is the point.
        //
        // The estimator extrapolates a mean over ~16 sampled statements. A
        // mean is not robust to outliers, and the outlier this test actually
        // meets is the scheduler: run alongside 968 other tests, one sampled
        // statement gets preempted mid-spin and its 40 µs reads as 10 ms,
        // which drags the extrapolation orders high. Preemption can only
        // inflate a wall-clock sample, never deflate it — so an upper bound
        // here is a bound on machine load, and it flaked exactly that way
        // before this comment existed.
        //
        // What is worth gating is the direction load cannot fake: the
        // estimator must not LOSE a real cost. Accuracy in the other
        // direction is verified by the differential probe
        // (`fluree-graph-turtle/examples/sink_bias_probe.rs`) on a quiet
        // machine, which is the right place for it.
        let truth = Duration::from_micros(40) * 2_000;
        assert!(
            body >= truth / 10,
            "estimate {body:?} is an order of magnitude under the {truth:?} of \
             work actually done — the estimator is losing real sink cost"
        );

        // EXACTLY ONE artifact is removed. This is the real gate, and unlike
        // the bound above it is algebra over a single measured `clock_pair`
        // rather than a second timing — so it holds under any load. That
        // matters now that a real writer clears the floor: a doubled
        // subtraction takes a whole extra `calls × clock_pair` out of the
        // estimate, which at N-Triples scale is enough to delete a serialize
        // row worth 30% of wall, with every other assertion here still green.
        let uncorrected = sink
            .sampled_time
            .as_nanos()
            .saturating_mul(u128::from(t.calls))
            / u128::from(t.sampled_calls);
        let removed = uncorrected.saturating_sub(body.as_nanos());
        let artifact = t.artifact.as_nanos();
        assert!(
            removed.abs_diff(artifact) < artifact / 2,
            "subtracted {removed}ns where one artifact is {artifact}ns — off by \
             {:.2}×, so the correction is not being applied exactly once",
            removed as f64 / artifact.max(1) as f64
        );
    }

    #[test]
    fn the_clock_artifact_is_subtracted_before_scaling_not_after() {
        // Order matters: scaling first multiplies the artifact by the same
        // ~127× the work gets, which is how 0.6 ms of clock reads became a
        // 76 ms "sink phase".
        let mut sink = TimingSink::new(DiscardSink::default());
        drive_discard(&mut sink, 20_000);
        let t = sink.sink_timing();

        // What the uncorrected estimator would have said, versus what the
        // corrected one computes. Compared as algebra over the same measured
        // clock_pair rather than against a second measurement of it — two
        // independent timings of a nanosecond-scale quantity race each other
        // on a loaded machine, and the property under test is not a race.
        let uncorrected = sink
            .sampled_time
            .as_nanos()
            .saturating_mul(u128::from(t.calls))
            / u128::from(t.sampled_calls);
        let corrected = sink
            .estimate_body_ns(t.calls, t.clock_pair.as_nanos())
            .expect("something was sampled");

        assert!(
            corrected < uncorrected,
            "nothing was subtracted: {corrected} vs {uncorrected}"
        );
        let removed = uncorrected - corrected;
        assert!(
            removed <= t.artifact.as_nanos() + u128::from(t.calls),
            "removed {removed}ns, more than the {}ns artifact it is allowed to \
             remove (plus one ns per call of integer-division slack)",
            t.artifact.as_nanos()
        );
        assert!(t.below_floor(), "and the corrected estimator declines it");
    }

    #[test]
    fn a_costly_finish_is_measured_exactly_and_never_scaled() {
        // A writer's flush runs once. Routed through the sampled body it came
        // back out multiplied by the call ratio: 50 ms → 6.4 s.
        struct SlowFinish(DiscardSink);
        impl GraphSink for SlowFinish {
            fn on_base(&mut self, _: &str) {}
            fn on_prefix(&mut self, _: &str, _: &str) {}
            fn term_iri(&mut self, i: &str) -> TermId {
                self.0.term_iri(i)
            }
            fn term_blank(&mut self, l: Option<&str>) -> TermId {
                self.0.term_blank(l)
            }
            fn term_literal(&mut self, v: &str, d: Datatype, l: Option<&str>) -> TermId {
                self.0.term_literal(v, d, l)
            }
            fn term_literal_value(&mut self, v: LiteralValue, d: Datatype) -> TermId {
                self.0.term_literal_value(v, d)
            }
            fn emit_triple(&mut self, a: TermId, b: TermId, c: TermId) -> SinkResult {
                self.0.emit_triple(a, b, c)
            }
            fn finish(&mut self) -> SinkResult {
                std::thread::sleep(Duration::from_millis(20));
                Ok(())
            }
        }

        let mut sink = TimingSink::new(SlowFinish(DiscardSink::default()));
        drive_discard(&mut sink, 5_000);
        sink.finish().unwrap();
        let t = sink.sink_timing();

        assert!(
            t.finish >= Duration::from_millis(20) && t.finish < Duration::from_millis(200),
            "finish must be the measured 20ms, not a scaled multiple: {:?}",
            t.finish
        );
        // And it is not hiding inside the body estimate.
        assert!(
            t.body.is_none_or(|b| b < Duration::from_millis(20)),
            "finish leaked into the scaled body: {:?}",
            t.body
        );
    }

    #[test]
    fn a_no_op_finish_reports_no_flush_cost() {
        // `DiscardSink::finish` is the default `Ok(())`. Timing it yields tens
        // of nanoseconds of clock, and reporting that as a flush would put the
        // instrument back into the report through the one exactly-measured
        // door.
        let mut sink = TimingSink::new(DiscardSink::default());
        drive_discard(&mut sink, 500);
        sink.finish().unwrap();
        assert_eq!(sink.sink_timing().finish, Duration::ZERO);
        // The raw measurement was not zero — dispatch and clock variance put a
        // no-op `finish` in the 100-200ns range, comfortably over three clock
        // pairs. It is the sub-microsecond floor that suppresses it.
        assert!(sink.finish_time < Duration::from_micros(1));
    }

    #[test]
    fn sampling_survives_a_corpus_periodic_at_the_stride() {
        // The probe's adversarial shape: expensive work on exactly one residue
        // mod 127. A fixed 127-stride either locks onto that residue (425×
        // over-report) or never sees it. Jitter means the sampled set is not a
        // residue class at all, so neither happens.
        let expensive_residue = 0u64;
        let mut sink = TimingSink::new(PeriodicSink::new(expensive_residue));
        drive_discard(&mut sink, 127 * 60);

        let sampled = sink.sampled_statement_indices.clone();
        assert!(sampled.len() > 20, "sanity: {} samples", sampled.len());

        let residues: std::collections::HashSet<u64> =
            sampled.iter().map(|i| i % SAMPLE_STRIDE).collect();
        assert!(
            residues.len() > 10,
            "the sample collapsed onto {} residue class(es) — a fixed stride in \
             disguise: {residues:?}",
            residues.len()
        );
    }

    #[test]
    fn the_same_corpus_samples_the_same_statements_every_run() {
        // Reproducibility is what makes two profiles of one corpus comparable.
        let indices = |seed: u64| {
            let mut sink = TimingSink::with_seed(DiscardSink::default(), seed);
            drive_discard(&mut sink, 2_000);
            sink.sampled_statement_indices.clone()
        };
        let seed = corpus_seed(b"@prefix ex: <http://e/> .\nex:a ex:b \"c\" .\n");
        assert_eq!(indices(seed), indices(seed));
        // …and a different corpus samples differently, which is the point.
        let other = corpus_seed(b"<http://e/s> <http://e/p> <http://e/o> .\n");
        assert_ne!(seed, other);
        assert_ne!(indices(seed), indices(other));
    }

    #[test]
    fn the_mean_sampling_gap_is_the_stride() {
        let mut sink = TimingSink::new(DiscardSink::default());
        let statements = 127 * 200;
        drive_discard(&mut sink, statements);
        let n = sink.sink_timing().sampled_statements;
        let mean_gap = statements as f64 / n as f64;
        assert!(
            (mean_gap - SAMPLE_STRIDE as f64).abs() < 25.0,
            "mean gap {mean_gap:.1} is not ~{SAMPLE_STRIDE}"
        );
    }

    #[test]
    fn nothing_forwarded_means_no_estimate_rather_than_a_zero() {
        let sink: TimingSink<GraphCollectorSink> = TimingSink::new(GraphCollectorSink::new());
        let t = sink.sink_timing();
        assert!(t.below_floor());
        assert_eq!(t.body, None);
        assert_eq!(t.finish, Duration::ZERO);
        assert_eq!(t.sampled_pct(), 0.0);
        assert_eq!(t.calls, 0);
    }

    #[test]
    fn the_floor_is_the_threshold_not_the_artifact_it_derives_from() {
        // `floor()` returning the artifact understated the actual bar by a
        // factor of FLOOR_MULTIPLE, so a report printing it named a threshold
        // that was not the one being applied.
        let mut sink = TimingSink::new(DiscardSink::default());
        drive_discard(&mut sink, 5_000);
        let t = sink.sink_timing();

        assert_eq!(t.floor(), t.artifact * FLOOR_MULTIPLE);
        assert!(t.floor() > t.artifact);
    }

    #[test]
    fn the_per_call_floor_is_the_aggregate_divided_by_the_calls() {
        // The aggregate is the honest threshold and a useless thing to show:
        // "under 82ms" across 720k calls reads as enormous and means ~114ns.
        let mut sink = TimingSink::new(DiscardSink::default());
        drive_discard(&mut sink, 5_000);
        let t = sink.sink_timing();

        let per_call = t.floor_per_call().expect("calls were forwarded");
        assert!(
            per_call < Duration::from_micros(10),
            "a per-call floor of {per_call:?} is not a per-call quantity"
        );
        assert_eq!(per_call, t.floor() / u32::try_from(t.calls).unwrap());

        // Nothing forwarded, nothing to divide by.
        let empty: TimingSink<DiscardSink> = TimingSink::new(DiscardSink::default());
        assert_eq!(empty.sink_timing().floor_per_call(), None);
    }

    #[test]
    fn the_sampling_error_bound_is_reported_and_grows_with_dispersion() {
        // A uniform corpus and a wildly varying one must not report the same
        // confidence in an extrapolation from the same number of samples.
        // Both arms are measured as the MINIMUM over three trials. Contention
        // only ever adds dispersion — a preempted statement is indistinguishable
        // from a slow one — so the minimum is each arm's least-corrupted
        // estimate. Comparing single trials made this test flaky on a loaded
        // machine: one unlucky pause in the uniform arm could out-disperse a
        // corpus that genuinely varies by 200x.
        let uniform_err = (0..3)
            .map(|_| {
                let mut uniform = TimingSink::new(SpinSink::new(Duration::from_micros(20)));
                drive_discard(&mut uniform, 3_000);
                uniform
                    .sink_timing()
                    .relative_std_error
                    .expect("more than one sample")
            })
            .fold(f64::INFINITY, f64::min);

        struct Erratic {
            statement: u64,
            inner: DiscardSink,
        }
        impl GraphSink for Erratic {
            fn on_base(&mut self, _: &str) {}
            fn on_prefix(&mut self, _: &str, _: &str) {}
            fn term_iri(&mut self, i: &str) -> TermId {
                self.inner.term_iri(i)
            }
            fn term_blank(&mut self, l: Option<&str>) -> TermId {
                self.inner.term_blank(l)
            }
            fn term_literal(&mut self, v: &str, d: Datatype, l: Option<&str>) -> TermId {
                self.inner.term_literal(v, d, l)
            }
            fn term_literal_value(&mut self, v: LiteralValue, d: Datatype) -> TermId {
                self.inner.term_literal_value(v, d)
            }
            fn emit_triple(&mut self, _: TermId, _: TermId, _: TermId) -> SinkResult {
                // Two wildly different populations, alternating.
                if self.statement.is_multiple_of(2) {
                    spin_for(Duration::from_micros(200));
                }
                Ok(())
            }
            fn end_statement(&mut self) {
                self.statement += 1;
            }
        }

        let erratic_err = (0..3)
            .map(|_| {
                let mut erratic = TimingSink::new(Erratic {
                    statement: 0,
                    inner: DiscardSink::default(),
                });
                drive_discard(&mut erratic, 3_000);
                erratic
                    .sink_timing()
                    .relative_std_error
                    .expect("more than one sample")
            })
            .fold(f64::INFINITY, f64::min);

        assert!(
            erratic_err > uniform_err,
            "a corpus whose statements vary by 200x must report a wider bound: \
             erratic {erratic_err:.4} vs uniform {uniform_err:.4}"
        );
    }

    #[test]
    fn the_sampling_error_bound_needs_at_least_two_samples() {
        let mut sink = TimingSink::new(DiscardSink::default());
        drive_discard(&mut sink, 1);
        assert_eq!(
            sink.sink_timing().relative_std_error,
            None,
            "one observation has no spread to report"
        );
    }

    #[test]
    fn corpus_seed_is_stable_and_separates_inputs() {
        assert_eq!(corpus_seed(b"abc"), corpus_seed(b"abc"));
        assert_ne!(corpus_seed(b"abc"), corpus_seed(b"abd"));
        // Length participates, so a prefix is not its own seed.
        assert_ne!(corpus_seed(b"abc"), corpus_seed(b"abcabc"));
        // An empty corpus still produces a usable (non-panicking) seed.
        let _ = corpus_seed(b"");
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
