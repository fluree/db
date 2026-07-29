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
use fluree_graph_ir::{Phase, PhaseTimings, SinkCounts, SinkTiming, FLOOR_MULTIPLE};
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
    /// Comparability class for baselines. Two runs may only be diffed on
    /// absolute timings when this matches.
    ///
    /// From `FLUREE_BENCH_HOST_CLASS` when set — a CI runner or a named
    /// instance type has an identity that `{os}-{arch}` does not capture —
    /// otherwise derived, so the field is never absent.
    host_class: String,
    /// Cores this host offers. Reported alongside `threads_used` so a
    /// single-threaded number is never mistaken for a saturated one.
    available_parallelism: usize,
    threads_used: usize,
    /// Peak resident set size for the process, in bytes, normalized across
    /// platforms. `None` where it cannot be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    peak_rss_bytes: Option<u64>,
}

/// Environment variable naming the host's comparability class.
pub const HOST_CLASS_ENV: &str = "FLUREE_BENCH_HOST_CLASS";

/// The host class for this run.
fn host_class() -> String {
    std::env::var(HOST_CLASS_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH))
}

/// The build this binary came from, discovered at runtime.
///
/// `git rev-parse` in the working directory, not a build script: the
/// workspace has none and adding the first one to stamp a profile field would
/// put a git dependency in front of every release build. The cost is one
/// process spawn, paid only under `--profile`, and `"unknown"` is a perfectly
/// good answer from an installed binary that is nowhere near a checkout.
fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Peak resident set size for this process, in bytes.
///
/// `ru_maxrss` is the one field here that means different things on different
/// kernels: bytes on Darwin, kilobytes on Linux. Normalizing it in exactly one
/// place is the bench lane's standing lesson — a factor of 1024 that only
/// appears on one platform is not a bug anyone catches by reading a number.
#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    // SAFETY: `getrusage` fills a caller-provided struct and touches nothing
    // else; the zeroed struct is a valid target for it.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return None;
        }
        usage
    };
    Some(normalize_max_rss(usage.ru_maxrss as i64))
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

/// Convert a raw `ru_maxrss` to bytes for the current platform.
///
/// Split out from the syscall so the unit conversion — the part that is
/// actually easy to get wrong — is testable without one.
fn normalize_max_rss(raw: i64) -> u64 {
    let raw = raw.max(0) as u64;
    if cfg!(target_os = "macos") || cfg!(target_os = "ios") {
        raw
    } else {
        raw.saturating_mul(1024)
    }
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

/// The sink's own story, kept out of `phases` because the interesting answer
/// is often "this could not be measured", which is not a duration.
#[derive(Serialize)]
struct SinkInfo {
    /// Estimated per-event cost. `null` when below the measurement floor —
    /// which is not the same as zero, and must not be read as zero.
    body_ns: Option<u128>,
    /// True when the sink is cheaper than the instrument can resolve.
    below_measurement_floor: bool,
    /// The threshold `body_ns` had to clear.
    floor_ns: u128,
    /// Clock cost that a scaled estimate carries: `calls × clock_pair_ns`.
    artifact_ns: u128,
    /// `finish()` — measured exactly, never scaled.
    finish_ns: u128,
    calls: u64,
    sampled_calls: u64,
    sampled_statements: u64,
    sampled_pct: f64,
}

#[derive(Serialize)]
struct CountsInfo {
    triples: u64,
    quads: u64,
    list_items: u64,
    reified: u64,
    /// Turtle `statement` productions, directives included — deliberately not
    /// called "statements", because it is not the triple count.
    grammar_statements: u64,
    terms_iri: u64,
    terms_blank: u64,
    terms_literal: u64,
    prefixes: u64,
}

#[derive(Serialize)]
struct RatesInfo {
    /// Triples per second, over every emitted edge (triples, quads, list
    /// items, reifiers) — the same quantity `fluree rdf count` prints.
    triples_per_sec: f64,
    decoded_mib_per_sec: f64,
}

#[derive(Serialize)]
struct SelfCalibration {
    clock_reads: u64,
    clock_pair_ns: u128,
    /// Wall clock the instrument actually consumed: the clock reads it took,
    /// priced at the measured rate. This is the profiler's distortion of the
    /// run, and it is small by construction.
    measured_overhead_pct: f64,
    /// Clock cost embedded in a sink estimate scaled to the full call count.
    /// Large whenever the sink is cheap, which is why the sink estimate is
    /// floored rather than trusted — see the `sink` block.
    estimator_artifact_pct: f64,
    /// The larger of the two above; the figure `trusted` gates on. They
    /// overlap (the reads actually taken are a subset of the extrapolation),
    /// so this is a max and not a sum.
    overhead_pct: f64,
    /// False when `overhead_pct` exceeds [`OVERHEAD_TRUST_LIMIT_PCT`]. When
    /// only `estimator_artifact_pct` drove it over, the read/decompress/parse
    /// phases are still sound — it is the sink estimate that is
    /// instrument-dominated, and the `sink` block says whether it was
    /// reported at all.
    trusted: bool,
}

/// One profiled run, ready to render.
#[derive(Serialize)]
pub struct ProfileReport {
    schema: &'static str,
    tool_version: &'static str,
    /// Commit this binary was built from, read at runtime. `"unknown"` when
    /// there is no checkout to ask.
    git_sha: String,
    verb: &'static str,
    host: HostInfo,
    corpus: CorpusInfo,
    /// The measured window: from the first byte of input handling to the end
    /// of parsing.
    ///
    /// It does **not** cover process startup and argument parsing, which
    /// happen before the window opens, nor the SHA-256 fingerprint, which is
    /// computed after it closes — so `--profile` without `--no-hash` costs
    /// real time that appears in neither this figure nor `unattributed_ns`.
    /// That is deliberate: hashing is the profiler's cost, not the pipeline's,
    /// and charging it to a phase would make the profile describe itself.
    wall_ns: u128,
    /// Wall clock *inside the window* that no phase claimed: syntax
    /// resolution, allocation, scheduling. Left visible rather than
    /// redistributed into the phases that did run.
    unattributed_ns: u128,
    phases: Vec<PhaseEntry>,
    sink: SinkInfo,
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
        sink: SinkTiming,
    ) -> Self {
        let mut timings = timings.clone();
        timings.set(Phase::Sink, sink.reportable());

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

        let clock_reads = timings.clock_reads() + sink.clock_reads;
        let clock_pair = sink.clock_pair;
        let wall_ns = wall.as_nanos();

        // Two different overheads, and conflating them is what let a sink
        // phase that was 98% clock ship marked "trusted".
        let measured_ns = u128::from(clock_reads) * clock_pair.as_nanos() / 2;
        let measured_pct = pct(measured_ns as f64, wall_ns as f64);
        let artifact_pct = pct(sink.artifact.as_nanos() as f64, wall_ns as f64);
        let overhead_pct = measured_pct.max(artifact_pct);

        let secs = wall.as_secs_f64();
        Self {
            schema: PROFILE_SCHEMA,
            tool_version: env!("CARGO_PKG_VERSION"),
            git_sha: git_sha(),
            verb: ctx.verb,
            host: HostInfo {
                os: std::env::consts::OS,
                arch: std::env::consts::ARCH,
                host_class: host_class(),
                available_parallelism: std::thread::available_parallelism()
                    .map_or(1, std::num::NonZeroUsize::get),
                // `check` and `count` parse on the calling thread. The
                // parallel pipeline reports its real width here.
                threads_used: 1,
                peak_rss_bytes: peak_rss_bytes(),
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
            sink: SinkInfo {
                body_ns: sink.body.map(|b| b.as_nanos()),
                below_measurement_floor: sink.below_floor(),
                floor_ns: sink.floor().as_nanos() * u128::from(FLOOR_MULTIPLE),
                artifact_ns: sink.artifact.as_nanos(),
                finish_ns: sink.finish.as_nanos(),
                calls: sink.calls,
                sampled_calls: sink.sampled_calls,
                sampled_statements: sink.sampled_statements,
                sampled_pct: sink.sampled_pct(),
            },
            counts: CountsInfo {
                triples: counts.triples,
                quads: counts.quads,
                list_items: counts.list_items,
                reified: counts.reified,
                grammar_statements: counts.statements,
                terms_iri: counts.terms_iri,
                terms_blank: counts.terms_blank,
                terms_literal: counts.terms_literal,
                prefixes: counts.prefixes,
            },
            rates: RatesInfo {
                triples_per_sec: rate(counts.emitted() as f64, secs),
                decoded_mib_per_sec: rate(ctx.bytes_decoded as f64 / (1024.0 * 1024.0), secs),
            },
            self_calibration: SelfCalibration {
                clock_reads,
                clock_pair_ns: clock_pair.as_nanos(),
                measured_overhead_pct: measured_pct,
                estimator_artifact_pct: artifact_pct,
                overhead_pct,
                trusted: overhead_pct <= OVERHEAD_TRUST_LIMIT_PCT,
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
            "  {} {} · {} · {} · {}",
            self.verb,
            self.corpus.input,
            self.corpus.syntax,
            human_bytes(self.corpus.bytes_decoded),
            self.host.host_class,
        );
        eprintln!(
            "  wall {} (input → parse; excludes startup{}) · {} unattributed",
            human_duration(self.wall_ns),
            if self.corpus.sha256.is_some() {
                " and fingerprinting"
            } else {
                ""
            },
            human_duration(self.unattributed_ns),
        );
        eprintln!(
            "  {} triples · {} · {} grammar statements{}",
            self.counts.triples + self.counts.quads + self.counts.list_items + self.counts.reified,
            human_rate(self.rates.triples_per_sec),
            self.counts.grammar_statements,
            self.host
                .peak_rss_bytes
                .map(|b| format!(" · peak RSS {}", human_bytes(b)))
                .unwrap_or_default(),
        );

        // The sink line is where an unresolvable number used to be printed as
        // though it were a measurement.
        let sink = &self.sink;
        if sink.below_measurement_floor {
            eprintln!(
                "  sink: below the measurement floor — across {} calls its per-event cost \
                 is under {}, which is where the clock's own {} stops being separable from it",
                sink.calls,
                human_duration(sink.floor_ns),
                human_duration(sink.artifact_ns),
            );
        } else {
            eprintln!(
                "  sink: ~{} estimated from {:.2}% of calls (±sampling error), inside the parse phase",
                human_duration(sink.body_ns.unwrap_or(0)),
                sink.sampled_pct,
            );
        }
        if sink.finish_ns > 0 {
            eprintln!(
                "  sink finish: {} (measured exactly, not scaled)",
                human_duration(sink.finish_ns)
            );
        }

        let cal = &self.self_calibration;
        eprintln!(
            "  profiler cost {:.3}% of wall ({} clock reads @ {}ns/pair)",
            cal.measured_overhead_pct, cal.clock_reads, cal.clock_pair_ns,
        );
        if !cal.trusted {
            // Say which part is untrustworthy. "Untrusted" stamped on a report
            // whose read and parse phases are sound is a warning people learn
            // to ignore.
            let detail = if cal.estimator_artifact_pct > cal.measured_overhead_pct {
                "the sink estimator's extrapolated clock artifact — the sink figure is \
                 not a baseline; read, decompress and parse are unaffected"
            } else {
                "the profiler's own clock reads — no phase share here is usable as a baseline"
            };
            eprintln!("  UNTRUSTED: {:.1}% of wall is {detail}", cal.overhead_pct);
        }
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
    } else if ns >= 1_000 {
        format!("{}μs", ns / 1_000)
    } else {
        // Integer-dividing sub-microsecond values into μs printed "0μs",
        // which reads as "free" for something that was measured.
        format!("{ns}ns")
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

    /// A sink whose cost was resolvable: body well over the floor.
    fn resolved_sink() -> SinkTiming {
        SinkTiming {
            body: Some(Duration::from_millis(5)),
            finish: Duration::ZERO,
            artifact: Duration::from_micros(60),
            calls: 3_000,
            sampled_calls: 24,
            sampled_statements: 8,
            clock_reads: 48,
            clock_pair: Duration::from_nanos(20),
        }
    }

    /// A sink too cheap to measure — the `count` case.
    fn unresolved_sink() -> SinkTiming {
        SinkTiming {
            body: None,
            ..resolved_sink()
        }
    }

    fn report() -> ProfileReport {
        ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_millis(100),
            counts(),
            resolved_sink(),
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
        assert_eq!(v["counts"]["grammar_statements"], 500);
        assert!(v["rates"]["triples_per_sec"].as_f64().unwrap() > 0.0);
        assert!(v["self_calibration"]["clock_reads"].as_u64().unwrap() >= 48);
        assert!(v["self_calibration"]["measured_overhead_pct"].is_number());
        assert!(v["self_calibration"]["estimator_artifact_pct"].is_number());
        assert!(v["self_calibration"]["overhead_pct"].is_number());
        assert!(v["self_calibration"]["trusted"].is_boolean());

        // Added by the review: a baseline that cannot say which host or which
        // build produced it is not a baseline.
        assert!(!v["host"]["host_class"].as_str().unwrap().is_empty());
        assert!(!v["git_sha"].as_str().unwrap().is_empty());
        // And the sink block, which carries the answer `phases` cannot.
        assert_eq!(v["sink"]["below_measurement_floor"], false);
        assert!(v["sink"]["floor_ns"].is_number());
        assert!(v["sink"]["artifact_ns"].is_number());
        assert_eq!(v["sink"]["calls"], 3000);
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
            resolved_sink(),
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
            unresolved_sink(),
        );
        assert_eq!(r.unattributed_ns, Duration::from_millis(90).as_nanos());
    }

    #[test]
    fn overhead_above_the_limit_marks_the_run_untrusted() {
        // A microsecond-scale run with a large clock-read count: the
        // instrument is most of the measurement and the report has to say so
        // rather than print confident shares.
        let noisy = SinkTiming {
            clock_reads: 10_000,
            ..unresolved_sink()
        };
        let r = ProfileReport::build(
            &ctx(),
            &PhaseTimings::start(),
            Duration::from_micros(10),
            counts(),
            noisy,
        );
        assert!(r.self_calibration.overhead_pct > OVERHEAD_TRUST_LIMIT_PCT);
        assert!(!r.self_calibration.trusted);

        // And a long run with a handful of reads is trusted.
        let r = ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_secs(10),
            counts(),
            resolved_sink(),
        );
        assert!(r.self_calibration.trusted);
    }

    #[test]
    fn a_zero_length_run_reports_zeroes_rather_than_dividing_by_zero() {
        let empty = SinkTiming {
            body: None,
            finish: Duration::ZERO,
            artifact: Duration::ZERO,
            calls: 0,
            sampled_calls: 0,
            sampled_statements: 0,
            clock_reads: 0,
            clock_pair: Duration::from_nanos(20),
        };
        let r = ProfileReport::build(
            &ctx(),
            &PhaseTimings::start(),
            Duration::ZERO,
            SinkCounts::default(),
            empty,
        );
        assert_eq!(r.rates.triples_per_sec, 0.0);
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
            resolved_sink(),
        );
        let v = serde_json::to_value(r).unwrap();
        assert!(
            v["corpus"].get("sha256").is_none(),
            "an absent fingerprint must be absent, not null — a consumer \
             should not have to distinguish the two"
        );
    }

    #[test]
    fn an_unresolvable_sink_is_reported_as_such_and_not_as_zero() {
        // The correction the review forced: when the sink is cheaper than the
        // clock, the report must say it could not be measured. A `body_ns` of
        // 0 would read as "the sink is free", which is a different claim.
        let r = ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_millis(100),
            counts(),
            unresolved_sink(),
        );
        let v = serde_json::to_value(r).unwrap();

        assert_eq!(v["sink"]["below_measurement_floor"], true);
        assert!(
            v["sink"]["body_ns"].is_null(),
            "unresolved must be null, never 0: {}",
            v["sink"]["body_ns"]
        );
        // And it does not appear as a phase row pretending to be a measurement.
        assert!(
            !v["phases"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["phase"] == "sink"),
            "an unresolved sink must not occupy a phase row"
        );
    }

    #[test]
    fn a_measured_finish_reaches_the_sink_phase_even_when_the_body_does_not() {
        // A writer's flush is exact. It must survive the body being suppressed
        // — otherwise suppressing an unresolvable estimate would also throw
        // away the one sink number that *was* measured.
        let sink = SinkTiming {
            body: None,
            finish: Duration::from_millis(7),
            ..resolved_sink()
        };
        let r = ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_millis(100),
            counts(),
            sink,
        );
        let v = serde_json::to_value(r).unwrap();

        assert_eq!(v["sink"]["finish_ns"], 7_000_000u64);
        let row = v["phases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["phase"] == "sink")
            .expect("finish alone still earns the row");
        assert_eq!(row["ns"], 7_000_000u64);
    }

    #[test]
    fn a_large_estimator_artifact_marks_the_run_untrusted_on_its_own() {
        // The `>2%` marker exists for exactly this: the wall cost of the clock
        // reads actually taken is negligible, but the artifact carried by the
        // *scaled* estimate is most of the run. Before the fix only the first
        // was counted, so the marker could never fire on a `count`.
        let sink = SinkTiming {
            artifact: Duration::from_millis(60),
            clock_reads: 40,
            ..unresolved_sink()
        };
        let r = ProfileReport::build(
            &ctx(),
            &timings(),
            Duration::from_millis(100),
            counts(),
            sink,
        );
        let cal = &r.self_calibration;

        assert!(
            cal.measured_overhead_pct < OVERHEAD_TRUST_LIMIT_PCT,
            "the reads actually taken are cheap: {}%",
            cal.measured_overhead_pct
        );
        assert!(cal.estimator_artifact_pct > 50.0);
        assert!(!cal.trusted, "the artifact alone must trip the marker");
        assert_eq!(cal.overhead_pct, cal.estimator_artifact_pct);
    }

    #[test]
    fn host_class_is_the_env_override_when_set_and_derived_otherwise() {
        let derived = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
        // Not asserted against the live env var: this process may be running
        // under one. The derivation is the part worth pinning.
        assert!(derived.contains('-'));
        assert!(!host_class().is_empty(), "the field is never absent");
        assert_eq!(HOST_CLASS_ENV, "FLUREE_BENCH_HOST_CLASS");
    }

    #[test]
    fn max_rss_is_normalized_to_bytes_per_platform() {
        // The one place the Darwin-bytes / Linux-kilobytes split is handled.
        // Getting it wrong is a silent factor of 1024 on one platform only.
        let raw = 4096;
        let expected = if cfg!(any(target_os = "macos", target_os = "ios")) {
            4096
        } else {
            4096 * 1024
        };
        assert_eq!(normalize_max_rss(raw), expected);
        // A negative reading (some kernels, on failure) is floored, not wrapped.
        assert_eq!(normalize_max_rss(-1), 0);
    }

    #[test]
    fn peak_rss_is_readable_on_this_platform() {
        if cfg!(unix) {
            let rss = peak_rss_bytes().expect("getrusage works on unix");
            assert!(
                rss > 1024 * 1024,
                "a running test process holds more than a MiB: {rss}"
            );
        }
    }

    #[test]
    fn git_sha_is_always_a_usable_string() {
        // Either a real short SHA from the surrounding checkout, or the
        // documented fallback — never empty, so a consumer need not branch.
        let sha = git_sha();
        assert!(!sha.is_empty());
        assert!(!sha.contains('\n'), "{sha:?}");
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
        // Sub-microsecond is rendered in ns, not truncated to "0μs" — a
        // measured value must never print as nothing.
        assert_eq!(human_duration(150), "150ns");
        assert_eq!(human_duration(0), "0ns");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
        assert_eq!(human_rate(2_500_000.0), "2.50M/s");
        assert_eq!(human_rate(2_500.0), "2.5K/s");
        assert_eq!(human_rate(25.0), "25/s");
    }
}
