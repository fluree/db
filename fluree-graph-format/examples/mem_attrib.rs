//! Where the bytes live in a `fluree rdf convert` run.
//!
//! The competitor matrix put `convert` at ~1 GB of peak RSS on a 118 MB Turtle
//! corpus while `serdi` streamed the same document in 1.5 MB. This is the
//! attribution instrument for that gap: a tracking global allocator plus the
//! convert pipeline taken apart into stages, so the answer is measured bytes
//! per stage rather than a reading of the code.
//!
//! Run it against a corpus:
//!
//! ```text
//! cargo run --release --example mem_attrib -- <file.ttl|file.nt> [--nocheck]
//! ```
//!
//! The stages mirror `fluree rdf convert` exactly:
//!
//! 1. `read`   — the CLI's input path, byte for byte (BufReader, `take`, `read_to_end`).
//! 2. `parse`  — the same parse into a sink that only counts, so the peak is
//!    the parser's own footprint on top of the document.
//! 3. `write`  — the same parse into the real N-Triples writer over a null
//!    destination, so the delta against stage 2 is what the writer holds.
//!
//! Peak is peak *live heap*, not RSS: RSS includes whatever the allocator has
//! not returned, which is exactly the number that cannot be attributed to a
//! line of code. The two are reconciled at the end by also reporting the
//! process high-water mark.

use fluree_graph_format::{NTriplesWriter, WriterConfig};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, Term, TermId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------- allocator

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOCED: AtomicUsize = AtomicUsize::new(0);

/// The system allocator with a live-bytes counter around it.
///
/// Peak is maintained on every allocation with `fetch_max` rather than sampled,
/// so the reported high-water mark is the real one and not whatever a sampler
/// happened to catch. That costs an atomic per allocation, which is why this
/// lives in an example and not behind a feature flag on the shipping binary.
struct Tracking;

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOCED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(p, layout);
    }

    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Counted as the allocator sees it: during a growing realloc both
        // blocks can be live at once, so the peak is charged the sum. That is
        // the transient a doubling `Vec` actually costs.
        LIVE.fetch_add(new_size, Ordering::Relaxed);
        let live = LIVE.load(Ordering::Relaxed);
        PEAK.fetch_max(live, Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOCED.fetch_add(new_size, Ordering::Relaxed);
        let q = System.realloc(p, layout, new_size);
        if q.is_null() {
            LIVE.fetch_sub(new_size, Ordering::Relaxed);
        } else {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Tracking = Tracking;

/// Counters as of now.
#[derive(Clone, Copy)]
struct Snapshot {
    live: usize,
    peak: usize,
    allocs: usize,
    alloced: usize,
}

fn snapshot() -> Snapshot {
    Snapshot {
        live: LIVE.load(Ordering::Relaxed),
        peak: PEAK.load(Ordering::Relaxed),
        allocs: ALLOCS.load(Ordering::Relaxed),
        alloced: ALLOCED.load(Ordering::Relaxed),
    }
}

/// Re-arm the peak at the current live level so the next stage's peak is its
/// own and not a leftover from the last one.
fn rearm() -> Snapshot {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    snapshot()
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn report(stage: &str, before: Snapshot, after: Snapshot, elapsed: std::time::Duration) {
    println!(
        "{stage:<28} peak {:>9.1} MiB   live-after {:>9.1} MiB   \
         held-by-stage {:>9.1} MiB   allocs {:>12}   churn {:>9.1} MiB   {:>7.2} s",
        mib(after.peak),
        mib(after.live),
        mib(after.live.saturating_sub(before.live)),
        after.allocs - before.allocs,
        mib(after.alloced - before.alloced),
        elapsed.as_secs_f64(),
    );
}

// --------------------------------------------------------------------- sinks

/// A sink that consumes the same events the writer does and holds nothing.
///
/// Every `term_*` call is one the parser could not serve from its own cache, so
/// these counters are the corpus's distinct-term census as the parser sees it —
/// which is the multiplier on everything the writer keeps per term.
#[derive(Default)]
struct CountingSink {
    iris: u64,
    blanks: u64,
    literals: u64,
    triples: u64,
    statements: u64,
}

impl GraphSink for CountingSink {
    fn on_base(&mut self, _b: &str) {}
    fn on_prefix(&mut self, _p: &str, _n: &str) {}

    fn term_iri(&mut self, _iri: &str) -> TermId {
        self.iris += 1;
        TermId::new(0)
    }

    fn term_blank(&mut self, _label: Option<&str>) -> TermId {
        self.blanks += 1;
        TermId::new(0)
    }

    fn term_literal(&mut self, _v: &str, _d: Datatype, _l: Option<&str>) -> TermId {
        self.literals += 1;
        TermId::new(0)
    }

    fn term_literal_value(&mut self, _v: LiteralValue, _d: Datatype) -> TermId {
        self.literals += 1;
        TermId::new(0)
    }

    fn emit_triple(&mut self, _s: TermId, _p: TermId, _o: TermId) -> SinkResult {
        self.triples += 1;
        Ok(())
    }

    fn end_statement(&mut self) {
        self.statements += 1;
    }
}

/// A `Write` that keeps the byte count and drops the bytes.
///
/// `io::sink()` would do, but the count is worth having: it proves the writer
/// stage did the same work as a real conversion instead of short-circuiting.
struct NullOut(u64);

impl Write for NullOut {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// --------------------------------------------------------------------- input

/// The CLI's input path, reproduced exactly — `BufReader` at the import
/// buffer size, a `take` one byte past the parser's addressable limit, and
/// `read_to_end` into a fresh `Vec`.
fn read_like_the_cli(path: &str) -> String {
    const IO_BUF: usize = 256 * 1024;
    const LIMIT: u64 = fluree_graph_turtle::error::MAX_INPUT_BYTES as u64;

    let file = std::fs::File::open(path).expect("open input");
    let mut reader = std::io::BufReader::with_capacity(IO_BUF, file);
    let mut buf = Vec::new();
    let r: &mut dyn BufRead = &mut reader;
    r.take(LIMIT + 1).read_to_end(&mut buf).expect("read input");
    String::from_utf8(buf).expect("utf-8")
}

/// The same read with the file's own size reserved up front.
///
/// The difference between this and [`read_like_the_cli`] is the cost of the
/// `take` wrapper: it erases the `File` specialization `read_to_end` uses to
/// size the buffer once, leaving the generic doubling path.
fn read_with_capacity(path: &str) -> String {
    let file = std::fs::File::open(path).expect("open input");
    let size = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
    let mut reader = std::io::BufReader::with_capacity(256 * 1024, file);
    let mut buf = Vec::with_capacity(size);
    reader.read_to_end(&mut buf).expect("read input");
    String::from_utf8(buf).expect("utf-8")
}

// ---------------------------------------------------------------------- main

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .get(1)
        .unwrap_or_else(|| panic!("usage: mem_attrib <file.ttl|file.nt> [--nocheck]"))
        .clone();
    let nocheck = args.iter().any(|a| a == "--nocheck");
    let is_nt = path.ends_with(".nt");

    println!("corpus     {path}");
    println!(
        "options    validation={}   syntax={}",
        !nocheck,
        if is_nt { "n-triples" } else { "turtle" }
    );
    println!(
        "sizes      Term={} B   TermId={} B   Datatype={} B   LiteralValue={} B",
        std::mem::size_of::<Term>(),
        std::mem::size_of::<TermId>(),
        std::mem::size_of::<Datatype>(),
        std::mem::size_of::<LiteralValue>(),
    );
    println!();

    let options = fluree_graph_turtle::ParserOptions::conformant().with_validation(!nocheck);

    // Stage 1 — read, twice, to price the `take` wrapper separately.
    let before = rearm();
    let t0 = std::time::Instant::now();
    let text = read_like_the_cli(&path);
    let after = snapshot();
    report("read (as the CLI reads)", before, after, t0.elapsed());
    let doc_bytes = text.len();
    drop(text);

    let before = rearm();
    let t0 = std::time::Instant::now();
    let text = read_with_capacity(&path);
    let after = snapshot();
    report("read (size reserved)", before, after, t0.elapsed());

    // Stage 2 — parse only. The sink holds nothing, so the peak over the
    // document is the parser's.
    let before = rearm();
    let t0 = std::time::Instant::now();
    let mut counting = CountingSink::default();
    let parsed = if is_nt {
        fluree_graph_turtle::parse_ntriples(&text, &mut counting)
    } else {
        fluree_graph_turtle::parse_with_prefixes_base_options(
            &text,
            &mut counting,
            &[],
            None,
            options,
        )
    };
    let after = snapshot();
    parsed.expect("parse");
    report("parse -> counting sink", before, after, t0.elapsed());
    let parse_only_peak = after.peak;
    let census = (
        counting.iris,
        counting.blanks,
        counting.literals,
        counting.triples,
        counting.statements,
    );

    // Stage 3 — the real writer over a null destination. Everything above plus
    // whatever the writer keeps.
    let before = rearm();
    let t0 = std::time::Instant::now();
    let config = WriterConfig::new();
    let mut writer = NTriplesWriter::with_config(NullOut(0), &config);
    let parsed = if is_nt {
        fluree_graph_turtle::parse_ntriples(&text, &mut writer)
    } else {
        fluree_graph_turtle::parse_with_prefixes_base_options(
            &text,
            &mut writer,
            &[],
            None,
            options,
        )
    };
    let after_parse = snapshot();
    parsed.expect("parse");
    writer.finish().expect("finish");
    let stats = writer.stats();
    let out_bytes = writer.into_inner().0;
    report(
        "parse -> ntriples writer",
        before,
        after_parse,
        t0.elapsed(),
    );

    println!();
    println!(
        "document   {:.1} MiB in, {:.1} MiB out, {} statements written",
        mib(doc_bytes),
        mib(out_bytes as usize),
        stats.statements,
    );
    println!(
        "census     distinct-iri {}   blank {}   literal {}   triple {}   statement {}",
        census.0, census.1, census.2, census.3, census.4
    );
    // Both parse stages were re-armed at the same live level (the document,
    // and nothing else), so their peaks are directly comparable and the
    // difference is what the writer holds that the counting sink did not.
    println!(
        "writer-add {:.1} MiB — the writer's own footprint, parse peak {:.1} \
         MiB vs write peak {:.1} MiB",
        mib(after_parse.peak.saturating_sub(parse_only_peak)),
        mib(parse_only_peak),
        mib(after_parse.peak),
    );

    // The process high-water mark, for reconciling live-heap against the RSS
    // the benchmark matrix measured from outside.
    println!("max-rss    {:.1} MiB", mib(max_rss_bytes()));
}

/// Peak resident set size of this process, in bytes.
///
/// `getrusage(RUSAGE_SELF)` reports `ru_maxrss` in bytes on macOS and in
/// kilobytes on Linux — the one difference that matters here, and the reason
/// this is not a one-liner.
fn max_rss_bytes() -> usize {
    #[repr(C)]
    #[derive(Default)]
    struct Rusage {
        ru_utime: [i64; 2],
        ru_stime: [i64; 2],
        ru_maxrss: i64,
        rest: [i64; 14],
    }
    extern "C" {
        fn getrusage(who: i32, usage: *mut Rusage) -> i32;
    }
    let mut usage = Rusage::default();
    let rc = unsafe { getrusage(0, &mut usage) };
    if rc != 0 {
        return 0;
    }
    if cfg!(target_os = "macos") {
        usage.ru_maxrss as usize
    } else {
        usage.ru_maxrss as usize * 1024
    }
}
