//! `--profile`: where the time went, in a form a human can read and in a form
//! a baseline file can diff.
//!
//! This is the repo's first machine-readable performance emission. The shape
//! is fixed by what the benchmark strategy needs a Tier-1 run to carry: a
//! corpus fingerprint, the host and its thread count, the tool version, a
//! per-phase breakdown with shares, and — the part that is easy to skip and
//! the reason to trust the rest — the profiler's disclosure of its own
//! overhead, with anything above [`OVERHEAD_TRUST_LIMIT_PCT`] marked untrusted
//! rather than quietly reported.
//!
//! Two numbers in the report are not measured the same way as the others and
//! say so in the output: the `sink` phase is a sampled estimate (see
//! [`fluree_graph_ir::TimingSink`]), and the wall-minus-phases gap is
//! deliberately left unattributed instead of being redistributed.

use crate::rdf::syntax::{Compression, RdfSyntax, SyntaxSource};
use fluree_bench_support::report::{print_summary, SummaryRow};
use fluree_graph_ir::{clock_pair_cost, Phase, PhaseTimings, SinkCounts};
use serde::Serialize;
use std::time::Duration;

/// Schema identifier carried by every `--profile=json` document. Bump the
/// version when a consumer would have to change; add fields freely without.
pub const PROFILE_SCHEMA: &str = "fluree.rdf.profile.v1";

/// Above this share of wall clock, the profiler's own clock reads are a large
/// enough part of the measurement that the phase breakdown should not be
/// trusted — reported as such rather than silently.
pub const OVERHEAD_TRUST_LIMIT_PCT: f64 = 2.0;

/// How `--profile` renders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ProfileFormat {
    /// A box table on stderr, so it never contaminates piped output.
    #[default]
    Human,
    /// One JSON document on stderr, for baseline diffing.
    Json,
}

/// What the run was, as opposed to how long it took.
pub struct RunContext {
    /// The verb that ran (`check`, `count`, …).
    pub verb: &'static str,
    /// How the input is named in the report.
    pub input: String,
    /// Syntax it was parsed as, and the rule that decided.
    pub syntax: RdfSyntax,
    /// Which resolution rule produced `syntax`.
    pub syntax_source: SyntaxSource,
    /// Compression layer stripped before parsing.
    pub compression: Compression,
    /// Bytes pulled off the wire — compressed size, when compressed.
    pub bytes_on_wire: u64,
    /// Bytes handed to the parser.
    pub bytes_decoded: u64,
    /// SHA-256 of the *decoded* document, or `None` under `--no-hash`.
    pub sha256: Option<String>,
}

#[derive(Serialize)]
struct HostInfo {
    os: &'static str,
    arch: &'static str,
    /// Cores this host offers. Reported alongside `threads_used` so a
    /// single-threaded number is never mistaken for a saturated one.
    available_parallelism: usize,
    threads_used: usize,
}

#[derive(Serialize)]
struct CorpusInfo {
    input: String,
    syntax: &'static str,
    syntax_source: &'static str,
    compression: &'static str,
    bytes_on_wire: u64,
    bytes_decoded: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

#[derive(Serialize)]
struct PhaseEntry {
    phase: &'static str,
    ns: u128,
    share_pct: f64,
    /// True for a phase whose cost is sampled rather than bracketed.
    estimated: bool,
}

#[derive(Serialize)]
struct CountsInfo {
    triples: u64,
    quads: u64,
    list_items: u64,
    reified: u64,
    statements: u64,
    terms_iri: u64,
    terms_blank: u64,
    terms_literal: u64,
    prefixes: u64,
}

#[derive(Serialize)]
struct RatesInfo {
    statements_per_sec: f64,
    decoded_mib_per_sec: f64,
}

#[derive(Serialize)]
struct SelfCalibration {
    clock_reads: u64,
    clock_pair_ns: u128,
    overhead_pct: f64,
    /// False when `overhead_pct` exceeds [`OVERHEAD_TRUST_LIMIT_PCT`]: the
    /// instrument is a large enough share of the measurement that the phase
    /// breakdown is not usable as a baseline.
    trusted: bool,
    /// Percentage of sink calls that were actually timed, backing the
    /// estimated `sink` phase.
    sink_sampled_pct: f64,
}

/// One profiled run, ready to render.
#[derive(Serialize)]
pub struct ProfileReport {
    schema: &'static str,
    tool_version: &'static str,
    verb: &'static str,
    host: HostInfo,
    corpus: CorpusInfo,
    wall_ns: u128,
    /// Wall clock not attributed to any phase — process startup, hashing,
    /// allocator warmup. Left visible rather than folded into the phases.
    unattributed_ns: u128,
    phases: Vec<PhaseEntry>,
    counts: CountsInfo,
    rates: RatesInfo,
    self_calibration: SelfCalibration,
}

impl ProfileReport {
    /// Assemble a report from a finished run.
    ///
    /// `sink_estimate` is [`fluree_graph_ir::TimingSink::estimated_sink_time`]
    /// and is written over the `sink` lane rather than added to it: sink
    /// dispatch happens *inside* the parse call, so adding would count the
    /// same nanoseconds twice.
    pub fn build(
        ctx: &RunContext,
        timings: &PhaseTimings,
        wall: Duration,
        counts: SinkCounts,
        sink_estimate: Duration,
        sink_clock_reads: u64,
        sink_sampled_pct: f64,
    ) -> Self {
        let mut timings = timings.clone();
        timings.set(Phase::Sink, sink_estimate);

        let phases: Vec<PhaseEntry> = Phase::ALL
            .into_iter()
            .map(|phase| PhaseEntry {
                phase: phase.as_str(),
                ns: timings.elapsed(phase).as_nanos(),
                share_pct: timings.share_pct(phase, wall),
                estimated: phase == Phase::Sink,
            })
            .filter(|e| e.ns > 0)
            .collect();

        // The sink runs inside the parse, so it is not part of the sequential
        // sum; counting it would make "unattributed" negative on a fast run.
        let sequential: u128 = Phase::ALL
            .into_iter()
            .filter(|p| *p != Phase::Sink)
            .map(|p| timings.elapsed(p).as_nanos())
            .sum();

        let clock_reads = timings.clock_reads() + sink_clock_reads;
        let clock_pair = clock_pair_cost();
        let overhead_ns = (clock_reads as u128) * clock_pair.as_nanos() / 2;
        let wall_ns = wall.as_nanos();
        let overhead_pct = pct(overhead_ns as f64, wall_ns as f64);

        let secs = wall.as_secs_f64();
        Self {
            schema: PROFILE_SCHEMA,
            tool_version: env!("CARGO_PKG_VERSION"),
            verb: ctx.verb,
            host: HostInfo {
                os: std::env::consts::OS,
                arch: std::env::consts::ARCH,
                available_parallelism: std::thread::available_parallelism()
                    .map_or(1, std::num::NonZeroUsize::get),
                // `check` and `count` parse on the calling thread. The
                // parallel pipeline reports its real width here.
                threads_used: 1,
            },
            corpus: CorpusInfo {
                input: ctx.input.clone(),
                syntax: ctx.syntax.as_str(),
                syntax_source: ctx.syntax_source.as_str(),
                compression: ctx.compression.as_str(),
                bytes_on_wire: ctx.bytes_on_wire,
                bytes_decoded: ctx.bytes_decoded,
                sha256: ctx.sha256.clone(),
            },
            wall_ns,
            unattributed_ns: wall_ns.saturating_sub(sequential),
            phases,
            counts: CountsInfo {
                triples: counts.triples,
                quads: counts.quads,
                list_items: counts.list_items,
                reified: counts.reified,
                statements: counts.statements,
                terms_iri: counts.terms_iri,
                terms_blank: counts.terms_blank,
                terms_literal: counts.terms_literal,
                prefixes: counts.prefixes,
            },
            rates: RatesInfo {
                statements_per_sec: rate(counts.emitted() as f64, secs),
                decoded_mib_per_sec: rate(ctx.bytes_decoded as f64 / (1024.0 * 1024.0), secs),
            },
            self_calibration: SelfCalibration {
                clock_reads,
                clock_pair_ns: clock_pair.as_nanos(),
                overhead_pct,
                trusted: overhead_pct <= OVERHEAD_TRUST_LIMIT_PCT,
                sink_sampled_pct,
            },
        }
    }

    /// Render to stderr in the requested format.
    ///
    /// Always stderr: `fluree rdf count big.ttl --profile | …` must keep
    /// delivering counts to the pipe, and a convert run must not have a
    /// profile table spliced into its Turtle.
    pub fn emit(&self, format: ProfileFormat) -> Result<(), serde_json::Error> {
        match format {
            ProfileFormat::Json => {
                eprintln!("{}", serde_json::to_string_pretty(self)?);
            }
            ProfileFormat::Human => self.print_human(),
        }
        Ok(())
    }

    fn print_human(&self) {
        let rows: Vec<SummaryRow> = self
            .phases
            .iter()
            .map(|p| {
                SummaryRow::new(if p.estimated {
                    format!("{} (est)", p.phase)
                } else {
                    p.phase.to_string()
                })
                .add("ms", p.ns as f64 / 1_000_000.0)
                .add("% wall", p.share_pct)
            })
            .collect();
        print_summary("phase", &rows);

        eprintln!(
            "  {} {} · {} · {}",
            self.verb,
            self.corpus.input,
            self.corpus.syntax,
            human_bytes(self.corpus.bytes_decoded),
        );
        eprintln!(
            "  wall {} · {} unattributed · {} statements · {}",
            human_duration(self.wall_ns),
            human_duration(self.unattributed_ns),
            self.counts.triples + self.counts.quads + self.counts.list_items,
            human_rate(self.rates.statements_per_sec),
        );
        let cal = &self.self_calibration;
        eprintln!(
            "  profiler overhead {:.3}% of wall ({} clock reads @ {}ns/pair){}",
            cal.overhead_pct,
            cal.clock_reads,
            cal.clock_pair_ns,
            if cal.trusted {
                String::new()
            } else {
                format!(
                    " — UNTRUSTED, above the {OVERHEAD_TRUST_LIMIT_PCT}% limit; \
                     phase shares are not usable as a baseline"
                )
            }
        );
        eprintln!(
            "  sink phase is estimated from {:.2}% of calls; \
             shares are of wall clock, and sink time runs inside parse",
            cal.sink_sampled_pct,
        );
    }
}

/// SHA-256 of the decoded document, lowercase hex.
///
/// The *decoded* bytes on purpose: it fingerprints the RDF, so the same corpus
/// stored plain, gzipped, and zstd'd all produce one identifier, which is what
/// makes a cross-compression comparison legible. It is not the checksum a
/// download page would publish for the file.
pub fn sha256_hex(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn pct(part: f64, whole: f64) -> f64 {
    if whole <= 0.0 {
        return 0.0;
    }
    (part / whole) * 100.0
}

fn rate(units: f64, secs: f64) -> f64 {
    if secs <= 0.0 {
        return 0.0;
    }
    units / secs
}

/// Nanoseconds in the units a reader wants. Same thresholds as
/// `commands/query.rs`'s `format_duration`, on the integer type this module
/// carries.
fn human_duration(ns: u128) -> String {
    let secs = ns as f64 / 1e9;
    if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.1}ms", secs * 1e3)
    } else {
        format!("{}μs", ns / 1_000)
    }
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= KIB * KIB * KIB {
        format!("{:.2} GiB", b / (KIB * KIB * KIB))
    } else if b >= KIB * KIB {
        format!("{:.2} MiB", b / (KIB * KIB))
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn human_rate(per_sec: f64) -> String {
    if per_sec >= 1e6 {
        format!("{:.2}M/s", per_sec / 1e6)
    } else if per_sec >= 1e3 {
        format!("{:.1}K/s", per_sec / 1e3)
    } else {
        format!("{per_sec:.0}/s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RunContext {
        RunContext {
            verb: "count",
            input: "corpus.ttl".to_string(),
            syntax: RdfSyntax::Turtle,
            syntax_source: SyntaxSource::Extension,
            compression: Compression::None,
            bytes_on_wire: 4096,
            bytes_decoded: 4096,
            sha256: Some("abc123".to_string()),
        }
    }

    fn timings() -> PhaseTimings {
        let mut t = PhaseTimings::start();
        t.add(Phase::Read, Duration::from_millis(10));
        t.add(Phase::Parse, Duration::from_millis(90));
        t
    }

    fn counts() -> SinkCounts {
        SinkCounts {
            triples: 1_000,
            statements: 500,
            terms_iri: 40,
            terms_literal: 1_000,
            prefixes: 3,
            ..SinkCounts::default()
        }
    }

    fn report() -> ProfileReport {
        ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_millis(100),
            counts(),
            Duration::from_millis(5),
            120,
            1.5,
        )
    }

    #[test]
    fn json_carries_every_field_the_bench_protocol_requires() {
        // Tier-1 baselines are diffed field by field; a silently dropped key
        // is a baseline that compares two different things.
        let v: serde_json::Value = serde_json::to_value(report()).unwrap();

        assert_eq!(v["schema"], PROFILE_SCHEMA);
        assert!(!v["tool_version"].as_str().unwrap().is_empty());
        assert_eq!(v["verb"], "count");
        assert!(v["host"]["os"].is_string());
        assert!(v["host"]["arch"].is_string());
        assert!(v["host"]["available_parallelism"].as_u64().unwrap() >= 1);
        assert_eq!(v["host"]["threads_used"], 1);
        assert_eq!(v["corpus"]["input"], "corpus.ttl");
        assert_eq!(v["corpus"]["syntax"], "turtle");
        assert_eq!(v["corpus"]["syntax_source"], "extension");
        assert_eq!(v["corpus"]["compression"], "none");
        assert_eq!(v["corpus"]["bytes_decoded"], 4096);
        assert_eq!(v["corpus"]["sha256"], "abc123");
        assert!(v["wall_ns"].as_u64().unwrap() > 0);
        assert_eq!(v["counts"]["triples"], 1000);
        assert!(v["rates"]["statements_per_sec"].as_f64().unwrap() > 0.0);
        assert!(v["self_calibration"]["clock_reads"].as_u64().unwrap() >= 120);
        assert!(v["self_calibration"]["overhead_pct"].is_number());
        assert!(v["self_calibration"]["trusted"].is_boolean());
    }

    #[test]
    fn phases_are_reported_in_pipeline_order_and_zero_phases_are_omitted() {
        let v: serde_json::Value = serde_json::to_value(report()).unwrap();
        let names: Vec<&str> = v["phases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["phase"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["read", "parse", "sink"]);
        assert!(
            !v["phases"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["phase"] == "write"),
            "a phase that never ran must not appear as a zero row"
        );
    }

    #[test]
    fn only_the_sink_phase_is_flagged_estimated() {
        let v: serde_json::Value = serde_json::to_value(report()).unwrap();
        for p in v["phases"].as_array().unwrap() {
            assert_eq!(
                p["estimated"].as_bool().unwrap(),
                p["phase"] == "sink",
                "phase {} carries the wrong estimation flag",
                p["phase"]
            );
        }
    }

    #[test]
    fn sink_time_overwrites_its_lane_so_it_is_not_double_counted() {
        // Sink dispatch happens inside the parse call. If the estimate were
        // added to a lane the parse had also filled, the report would claim
        // more time than the run took.
        let mut t = timings();
        t.add(Phase::Sink, Duration::from_millis(50));
        let r = ProfileReport::build(
            &ctx(),
            &t,
            Duration::from_millis(100),
            counts(),
            Duration::from_millis(5),
            10,
            1.0,
        );
        let sink = r.phases.iter().find(|p| p.phase == "sink").unwrap();
        assert_eq!(sink.ns, Duration::from_millis(5).as_nanos());
    }

    #[test]
    fn unattributed_time_is_the_gap_and_excludes_the_nested_sink() {
        let r = report();
        // 100ms wall, 10ms read + 90ms parse sequential → no gap. The 5ms
        // sink estimate is nested inside parse and must not push this
        // negative (it saturates at zero, which would hide the bug).
        assert_eq!(r.unattributed_ns, 0);

        let mut sparse = PhaseTimings::start();
        sparse.add(Phase::Read, Duration::from_millis(10));
        let r = ProfileReport::build(
            &ctx(),
            &sparse,
            Duration::from_millis(100),
            counts(),
            Duration::ZERO,
            0,
            0.0,
        );
        assert_eq!(r.unattributed_ns, Duration::from_millis(90).as_nanos());
    }

    #[test]
    fn overhead_above_the_limit_marks_the_run_untrusted() {
        // A microsecond-scale run with a large clock-read count: the
        // instrument is most of the measurement and the report has to say so
        // rather than print confident shares.
        let r = ProfileReport::build(
            &ctx(),
            &PhaseTimings::start(),
            Duration::from_micros(10),
            counts(),
            Duration::ZERO,
            10_000,
            50.0,
        );
        assert!(r.self_calibration.overhead_pct > OVERHEAD_TRUST_LIMIT_PCT);
        assert!(!r.self_calibration.trusted);

        // And a long run with a handful of reads is trusted.
        let r = ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_secs(10),
            counts(),
            Duration::ZERO,
            20,
            1.0,
        );
        assert!(r.self_calibration.trusted);
    }

    #[test]
    fn a_zero_length_run_reports_zeroes_rather_than_dividing_by_zero() {
        let r = ProfileReport::build(
            &ctx(),
            &PhaseTimings::start(),
            Duration::ZERO,
            SinkCounts::default(),
            Duration::ZERO,
            0,
            0.0,
        );
        assert_eq!(r.rates.statements_per_sec, 0.0);
        assert_eq!(r.rates.decoded_mib_per_sec, 0.0);
        assert_eq!(r.self_calibration.overhead_pct, 0.0);
    }

    #[test]
    fn no_hash_leaves_the_fingerprint_out_of_the_document_entirely() {
        let mut c = ctx();
        c.sha256 = None;
        let r = ProfileReport::build(
            &c,
            &timings(),
            Duration::from_millis(100),
            counts(),
            Duration::ZERO,
            0,
            0.0,
        );
        let v = serde_json::to_value(r).unwrap();
        assert!(
            v["corpus"].get("sha256").is_none(),
            "an absent fingerprint must be absent, not null — a consumer \
             should not have to distinguish the two"
        );
    }

    #[test]
    fn sha256_matches_the_known_digest_of_the_empty_string() {
        assert_eq!(
            sha256_hex(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(sha256_hex("a").len(), 64);
        assert_ne!(sha256_hex("a"), sha256_hex("b"));
    }

    #[test]
    fn the_report_round_trips_through_serde() {
        let json = serde_json::to_string(&report()).unwrap();
        let back: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back["schema"], PROFILE_SCHEMA);
    }

    #[test]
    fn human_helpers_pick_sensible_units() {
        assert_eq!(human_duration(1_500_000_000), "1.50s");
        assert_eq!(human_duration(1_500_000), "1.5ms");
        assert_eq!(human_duration(1_500), "1μs");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
        assert_eq!(human_rate(2_500_000.0), "2.50M/s");
        assert_eq!(human_rate(2_500.0), "2.5K/s");
        assert_eq!(human_rate(25.0), "25/s");
    }
}
