//! Bias probe for the [`TimingSink`] sampling estimator.
//!
//! Originally written as a scratch harness for the `fluree rdf` adversarial
//! review, and kept because it is the only thing that can answer the question
//! the estimator raises: *is the number it reports the sink, or the clock?*
//! Unit tests in `fluree-graph-ir::timing` cover the same properties at debug
//! speed; this runs the release-mode, million-statement version that produced
//! the numbers the estimator was corrected against.
//!
//! Four sections:
//!
//! - **A** — the cheap sink `count` actually uses, against a fully-bracketed
//!   reference and against differential wall time. The estimator must decline
//!   to report here: a bracketed `DiscardSink` call costs ~19 ns of clock and
//!   ~1 ns of sink.
//! - **B** — a corpus periodic at the sample stride, with the expensive shape
//!   parked on one residue. A fixed stride either locks onto it (over-report)
//!   or never sees it (miss); jittered sampling does neither.
//! - **C** — which statements were sampled, printed so the sample can be
//!   eyeballed for structure.
//! - **D** — a costly `finish()`, which must be reported exactly and never
//!   scaled by the sample ratio.
//!
//! Usage: cargo run --release -p fluree-graph-turtle --example sink_bias_probe

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId, TimingSink};
use fluree_graph_turtle::{parse_with_prefixes_base_options, ParserOptions};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- sinks

/// The CLI's DiscardSink, verbatim in behaviour.
#[derive(Default)]
struct DiscardSink {
    next: u32,
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
    fn emit_list_item(&mut self, _: TermId, _: TermId, _: TermId, _: i32) -> SinkResult {
        Ok(())
    }
    fn supports_quads(&self) -> bool {
        true
    }
    fn emit_quad(&mut self, _: TermId, _: TermId, _: TermId, _: TermId) -> SinkResult {
        Ok(())
    }
    fn supports_reified_triples(&self) -> bool {
        true
    }
    fn emit_reified_triple(&mut self, _: TermId, _: TermId, _: TermId, _: TermId) -> SinkResult {
        Ok(())
    }
}

/// A sink whose per-triple cost depends on where in a 127-statement period
/// the statement falls: expensive only on the residue the stride locks onto.
struct PeriodicSink {
    next: u32,
    stmt: u64,
    /// residue (mod 127) that is expensive
    hot_residue: u64,
    spin: u64,
    acc: u64,
}
impl PeriodicSink {
    fn new(hot_residue: u64, spin: u64) -> Self {
        Self {
            next: 0,
            stmt: 0,
            hot_residue,
            spin,
            acc: 0,
        }
    }
    fn mint(&mut self) -> TermId {
        self.next = self.next.wrapping_add(1);
        TermId::new(self.next)
    }
    fn work(&mut self) {
        if self.stmt % 127 == self.hot_residue {
            for i in 0..self.spin {
                self.acc = self.acc.wrapping_add(i ^ self.acc).rotate_left(7);
            }
            std::hint::black_box(self.acc);
        }
    }
}
impl GraphSink for PeriodicSink {
    fn on_base(&mut self, _: &str) {}
    fn on_prefix(&mut self, _: &str, _: &str) {}
    fn term_iri(&mut self, _: &str) -> TermId {
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
        self.work();
        Ok(())
    }
    fn emit_list_item(&mut self, _: TermId, _: TermId, _: TermId, _: i32) -> SinkResult {
        Ok(())
    }
    fn supports_quads(&self) -> bool {
        true
    }
    fn emit_quad(&mut self, _: TermId, _: TermId, _: TermId, _: TermId) -> SinkResult {
        Ok(())
    }
    fn supports_reified_triples(&self) -> bool {
        true
    }
    fn emit_reified_triple(&mut self, _: TermId, _: TermId, _: TermId, _: TermId) -> SinkResult {
        Ok(())
    }
    fn end_statement(&mut self) {
        self.stmt += 1;
    }
}

/// Times EVERY forwarded call — the "fully bracketed" reference the sampled
/// estimator is supposed to approximate.
struct FullSink<S> {
    inner: S,
    total: Duration,
    calls: u64,
}
impl<S: GraphSink> FullSink<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            total: Duration::ZERO,
            calls: 0,
        }
    }
    #[inline]
    fn f<T>(&mut self, g: impl FnOnce(&mut S) -> T) -> T {
        let t = Instant::now();
        let out = g(&mut self.inner);
        self.total += t.elapsed();
        self.calls += 1;
        out
    }
}
impl<S: GraphSink> GraphSink for FullSink<S> {
    fn on_base(&mut self, b: &str) {
        self.f(|s| s.on_base(b));
    }
    fn on_prefix(&mut self, p: &str, n: &str) {
        self.f(|s| s.on_prefix(p, n));
    }
    fn term_iri(&mut self, i: &str) -> TermId {
        self.f(|s| s.term_iri(i))
    }
    fn term_blank(&mut self, l: Option<&str>) -> TermId {
        self.f(|s| s.term_blank(l))
    }
    fn term_literal(&mut self, v: &str, d: Datatype, l: Option<&str>) -> TermId {
        self.f(|s| s.term_literal(v, d, l))
    }
    fn term_literal_value(&mut self, v: LiteralValue, d: Datatype) -> TermId {
        self.f(|s| s.term_literal_value(v, d))
    }
    fn emit_triple(&mut self, a: TermId, b: TermId, c: TermId) -> SinkResult {
        self.f(|s| s.emit_triple(a, b, c))
    }
    fn emit_list_item(&mut self, a: TermId, b: TermId, c: TermId, i: i32) -> SinkResult {
        self.f(|s| s.emit_list_item(a, b, c, i))
    }
    fn supports_quads(&self) -> bool {
        self.inner.supports_quads()
    }
    fn emit_quad(&mut self, a: TermId, b: TermId, c: TermId, g: TermId) -> SinkResult {
        self.f(|s| s.emit_quad(a, b, c, g))
    }
    fn supports_reified_triples(&self) -> bool {
        self.inner.supports_reified_triples()
    }
    fn emit_reified_triple(&mut self, a: TermId, b: TermId, c: TermId, r: TermId) -> SinkResult {
        self.f(|s| s.emit_reified_triple(a, b, c, r))
    }
    fn end_statement(&mut self) {
        self.f(GraphSink::end_statement);
    }
    fn abort_statement(&mut self) {
        self.f(GraphSink::abort_statement);
    }
    fn finish(&mut self) -> SinkResult {
        self.f(GraphSink::finish)
    }
}

// ---------------------------------------------------------------- corpora

/// Uniform N-Triples-ish Turtle: every statement the same shape.
fn uniform(statements: usize) -> String {
    let mut s = String::new();
    for i in 0..statements {
        s.push_str(&format!(
            "<http://example.org/s{i}> <http://example.org/p> \"value number {i}\" .\n"
        ));
    }
    s
}

/// 127-statement period, mixed statement sizes: one statement in every 127 is
/// a fat one (long IRIs + many literals), the other 126 are thin.
fn periodic_127(periods: usize) -> String {
    let mut s = String::new();
    for p in 0..periods {
        for k in 0..127usize {
            let i = p * 127 + k;
            if k == 0 {
                // fat statement: long terms, many objects
                s.push_str(&format!(
                    "<http://example.org/a/very/long/namespace/path/that/keeps/going/subject{i}> \
                     <http://example.org/a/very/long/namespace/path/predicate> \
                     \"{}\" .\n",
                    "x".repeat(400)
                ));
            } else {
                s.push_str(&format!("<http://e/s{i}> <http://e/p> \"v\" .\n"));
            }
        }
    }
    s
}

// ---------------------------------------------------------------- drivers

fn parse_bare<S: GraphSink>(text: &str, sink: &mut S) -> Duration {
    let t = Instant::now();
    let r = parse_with_prefixes_base_options(text, sink, &[], None, ParserOptions::conformant());
    let _ = sink.finish();
    let d = t.elapsed();
    r.expect("corpus must parse");
    d
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    println!("== A. cheap sink (DiscardSink — what `count` actually uses) ==");
    for statements in [200_000usize, 1_000_000] {
        let doc = uniform(statements);
        // differential "true" sink cost: parse with the sink vs. with nothing
        // measurable to do is not separable here, so instead measure the
        // fully-bracketed reference AND the raw parse wall.
        let mut bare = DiscardSink::default();
        let wall_bare = parse_bare(&doc, &mut bare);

        let mut sampled = TimingSink::new(DiscardSink::default());
        let wall_sampled = parse_bare(&doc, &mut sampled);
        let t = sampled.sink_timing();
        let est = t.reportable();

        let mut full = FullSink::new(DiscardSink::default());
        let wall_full = parse_bare(&doc, &mut full);
        let measured_all = full.total;

        println!(
            "  n={statements:>9}  wall(bare)={:8.2}ms  wall(sampled)={:8.2}ms  wall(full-bracket)={:8.2}ms",
            ms(wall_bare),
            ms(wall_sampled),
            ms(wall_full)
        );
        println!(
            "     TimingSink body  = {}   floor={:.3}ms  artifact={:.3}ms",
            match t.body {
                Some(b) => format!("{:.3}ms", ms(b)),
                None => "BELOW FLOOR".to_string(),
            },
            ms(t.floor()),
            ms(t.artifact),
        );
        println!(
            "     reportable       = {:8.3}ms  ({:5.2}% of its own wall)   sampled_pct={:.3}%",
            ms(est),
            ms(est) / ms(wall_sampled) * 100.0,
            t.sampled_pct()
        );
        println!(
            "     full-bracket sum = {:8.3}ms over {} calls  ({:.2} ns/call incl. clock)",
            ms(measured_all),
            full.calls,
            measured_all.as_nanos() as f64 / full.calls as f64
        );
        println!(
            "     wall delta bare→full-bracket = {:8.3}ms  (this is ~pure instrument cost)",
            ms(wall_full) - ms(wall_bare)
        );
        println!(
            "     wall delta bare→sampled      = {:8.3}ms  (real cost the estimate should own)",
            ms(wall_sampled) - ms(wall_bare)
        );
        println!();
    }

    println!("== B. periodicity: 127-statement period, expensive work on ONE residue ==");
    let doc = periodic_127(8_000); // ~1.016M statements
    let statements = 8_000 * 127;
    for hot in [0u64, 1, 63] {
        // reference: real cost of the periodic work, by differential wall time
        let mut none = PeriodicSink::new(hot, 0);
        let wall_nowork = parse_bare(&doc, &mut none);
        let mut work = PeriodicSink::new(hot, 4_000);
        let wall_work = parse_bare(&doc, &mut work);
        let true_cost = wall_work.saturating_sub(wall_nowork);

        let mut sampled = TimingSink::new(PeriodicSink::new(hot, 4_000));
        let _ = parse_bare(&doc, &mut sampled);
        let t = sampled.sink_timing();

        println!(
            "  hot_residue={hot:>3}  true(differential)={:9.2}ms   TimingSink={:>12}   ratio={}",
            ms(true_cost),
            match t.body {
                Some(b) => format!("{:.2}ms", ms(b)),
                None => "BELOW FLOOR".to_string(),
            },
            match t.body {
                Some(b) => format!("{:6.2}x", ms(b) / ms(true_cost).max(1e-9)),
                None => "   n/a".to_string(),
            }
        );
    }
    println!("  (statements = {statements})");

    println!();
    println!("== C. which statement indices are sampled? ==");
    {
        struct Spy {
            n: u64,
            sampled: Vec<u64>,
        }
        impl GraphSink for Spy {
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
                Ok(())
            }
            fn end_statement(&mut self) {
                self.n += 1;
            }
        }
        // Instrument by observing which statements get non-zero sampled_calls
        // growth. Easiest proxy: drive statements one at a time and diff.
        let mut sink = TimingSink::new(Spy {
            n: 0,
            sampled: vec![],
        });
        let mut prev = 0.0f64;
        let mut hits = Vec::new();
        for i in 0..400u64 {
            let s = sink.term_iri("http://e/s");
            let p = sink.term_iri("http://e/p");
            let o = sink.term_iri("http://e/o");
            let _ = sink.emit_triple(s, p, o);
            sink.end_statement();
            let now = sink.clock_reads() as f64;
            if now > prev {
                hits.push(i);
            }
            prev = now;
        }
        println!("  statement indices that were timed: {hits:?}");
        let _ = sink.into_inner().sampled;
    }

    println!();
    println!("== D. does a costly finish() get multiplied by the stride? ==");
    {
        struct SlowFinish(DiscardSink);
        impl GraphSink for SlowFinish {
            fn on_base(&mut self, b: &str) {
                self.0.on_base(b);
            }
            fn on_prefix(&mut self, p: &str, n: &str) {
                self.0.on_prefix(p, n);
            }
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
            fn supports_quads(&self) -> bool {
                true
            }
            fn supports_reified_triples(&self) -> bool {
                true
            }
            fn finish(&mut self) -> SinkResult {
                // stand-in for a writer flushing its buffer
                std::thread::sleep(Duration::from_millis(50));
                Ok(())
            }
        }
        let doc = uniform(200_000);
        let mut sink = TimingSink::new(SlowFinish(DiscardSink::default()));
        let wall = parse_bare(&doc, &mut sink);
        println!(
            "  real finish() cost = 50.00ms; whole run wall = {:.2}ms",
            ms(wall)
        );
        let t = sink.sink_timing();
        println!(
            "  TimingSink finish = {:.2}ms (exact, unscaled);  body = {}",
            ms(t.finish),
            match t.body {
                Some(b) => format!("{:.2}ms", ms(b)),
                None => "BELOW FLOOR".to_string(),
            },
        );
    }
}

// Appended probe: does a costly finish() get extrapolated by the stride?
#[allow(dead_code)]
fn finish_probe() {}
