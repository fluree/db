//! The end-to-end witness that the Arc handover is real.
//!
//! The three tests that shipped with the handover check the mechanism from the
//! sink's side: the parser takes the sharing path, the writer stores a
//! pointer-equal `Arc`, the decorator forwards. A hostile parser satisfies all
//! three and still allocates twice — hand the sink a reference, then build a
//! SECOND `Arc` for the cache key. Every existing assertion passes, and 800K
//! duplicate allocations come back. The party that could prove otherwise is the
//! parser's own cache, and it is dropped before any test can look at it.
//!
//! So this measures from outside, and it measures a SLOPE rather than a count.
//! An absolute allocation count moves with hashmap growth, `Vec` doubling and
//! anything else that allocates along the way; the marginal cost of one more
//! distinct IRI does not. Two documents with the same statement count, differing
//! only in how many distinct subjects they name, isolate exactly that: whatever
//! else allocates appears in both arms and subtracts out.
//!
//! Under sharing the slope is ~1 allocation per distinct IRI — the cache key,
//! which the sink then clones for free. Under the mutation it is ~2. The bound
//! sits between them.

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicUsize = AtomicUsize::new(0);

/// Counts allocations while armed. Arming is a flag rather than a fresh
/// allocator so the test harness's own churn — which happens before and after
/// the window — stays out of the number.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) == 1 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) == 1 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(p, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

// The counters above are PROCESS-GLOBAL. This file is safe today only because
// it holds exactly ONE test that arms them: `cargo test` runs a binary's tests
// on threads of one process, so a second measuring test added here would
// measure this one's allocations as well as its own — silently, and in the
// direction that accuses the product rather than the test.
//
// So: one measuring test per file, or serialize the arm/measure/disarm window
// behind a mutex the way `term_cache_efficacy.rs` does. Nextest's
// process-per-test isolation hides the difference, which means a green run
// there is not evidence either way.

/// A sink that holds nothing, so the only per-term allocation in the window is
/// the producer's.
#[derive(Default)]
struct HollowSink;

impl GraphSink for HollowSink {
    fn on_base(&mut self, _b: &str) {}
    fn on_prefix(&mut self, _p: &str, _n: &str) {}
    fn term_iri(&mut self, _iri: &str) -> TermId {
        TermId::new(0)
    }
    fn term_blank(&mut self, _l: Option<&str>) -> TermId {
        TermId::new(0)
    }
    fn term_literal(&mut self, _v: &str, _d: Datatype, _l: Option<&str>) -> TermId {
        TermId::new(0)
    }
    fn term_literal_value(&mut self, _v: LiteralValue, _d: Datatype) -> TermId {
        TermId::new(0)
    }
    fn emit_triple(&mut self, _s: TermId, _p: TermId, _o: TermId) -> SinkResult {
        Ok(())
    }
}

/// `statements` lines; `distinct` of them get their own subject IRI and the
/// rest reuse one. Statement count, literal count and byte length per line are
/// held as close to constant as the differing subject names allow.
fn corpus(statements: usize, distinct: usize) -> String {
    // ABSOLUTE IRIs, deliberately. A prefixed name costs the expanded-IRI
    // String, the Arc built from it, AND a second Arc keyed by the span text
    // for the prefixed-name cache — about four allocations per distinct term,
    // which buries the one allocation this test is about. The absolute form
    // goes straight down `sink_term_iri`, which is the path the handover
    // changed.
    let mut doc = String::new();
    for i in 0..statements {
        let subject = if i < distinct { i } else { 0 };
        doc.push_str(&format!(
            "<http://example.org/s{subject:07}> <http://example.org/p> \"v{i:07}\" .\n"
        ));
    }
    doc
}

fn allocations_for(doc: &str) -> usize {
    let mut sink = HollowSink;
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(1, Ordering::Relaxed);
    let result = fluree_graph_turtle::parse_with_options(
        doc,
        &mut sink,
        fluree_graph_turtle::ParserOptions::conformant(),
    );
    COUNTING.store(0, Ordering::Relaxed);
    result.expect("parses");
    ALLOCS.load(Ordering::Relaxed)
}

#[test]
fn a_distinct_iri_costs_one_allocation_and_not_two() {
    const STATEMENTS: usize = 4000;
    const FEW: usize = 100;
    const MANY: usize = 3100;

    // Warm anything lazily initialized on the first parse so it lands outside
    // both measurements rather than inside the first one.
    let _ = allocations_for(&corpus(64, 8));

    let few = allocations_for(&corpus(STATEMENTS, FEW));
    let many = allocations_for(&corpus(STATEMENTS, MANY));

    let slope = (many as f64 - few as f64) / (MANY - FEW) as f64;
    assert!(
        slope < 1.5,
        "a distinct IRI cost {slope:.2} allocations; at ~1 the parser is handing \
         the sink its own cache key, at ~2 it is building a second Arc for the \
         same bytes and the sharing is defeated ({few} allocs at {FEW} distinct, \
         {many} at {MANY})"
    );
    // The other side of the bound: a slope near zero would mean the corpora are
    // not actually differing in distinct-IRI count, which would make the test
    // vacuous rather than passing.
    assert!(
        slope > 0.5,
        "a distinct IRI cost only {slope:.2} allocations — the two corpora are \
         not exercising different distinct-IRI counts, so this test proves nothing"
    );
}
