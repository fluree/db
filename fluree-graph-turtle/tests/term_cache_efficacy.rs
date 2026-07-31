//! The hint must actually size the caches — not merely be accepted.
//!
//! `term_cache_sizing.rs` pins the SAFETY half of the contract: whatever the
//! reservation does, the document denotes the same RDF. That half is satisfied
//! by an implementation that plumbs the hint through and then ignores it:
//!
//! ```ignore
//! let _ = options.distinct_terms_hint;
//! let reserve = INITIAL_TERM_CACHE;
//! ```
//!
//! Every assertion there passes, the crate stays green, and the fix is silently
//! reverted — the same shape as a sink that receives a shared `Arc` and copies
//! it anyway. A test that asserts only the second half of its own name leaves
//! the first half unguarded.
//!
//! So this measures the reservation from outside, in bytes allocated. The exact
//! byte counts are hashbrown's business and are not asserted; what is asserted
//! is a RATIO between two parses of the same tiny document, which is not
//! plausibly reachable by anything except the hint doing its job. A reserved
//! two-million-entry table is about three orders of magnitude larger than the
//! floor, so the bound sits far from both.

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

static BYTES: AtomicUsize = AtomicUsize::new(0);
static ARMED: AtomicUsize = AtomicUsize::new(0);

/// The counters above are PROCESS-GLOBAL, so only one measurement may be in
/// flight at a time. `cargo test` runs a binary's tests on threads of one
/// process, so without this the two tests here arm the same counter and each
/// measures the other's allocations: the floor came out at 104,960,128 bytes
/// instead of ~102,520, inflated by the clamp test's two-million-entry table
/// allocating alongside it.
///
/// That is worse than a flaky test. The assertion that fired blamed the
/// PRODUCT — "the hint is being accepted and ignored" — for a defect in the
/// measurement, which is the most expensive kind of false alarm: it sends the
/// next reader to audit working code.
///
/// It stayed hidden because every green in this bucket came from nextest,
/// which runs each test in its own process and so isolates the statics for
/// free. CI runs nextest, so this gated nothing; a developer typing
/// `cargo test` hit it on the first try. A test that is green only under the
/// runner CI happens to use is a test whose isolation is accidental.
///
/// Poisoning is recovered rather than propagated: these tests assert, so a
/// genuine failure would otherwise turn every sibling into a confusing
/// poison error instead of its own verdict.
static MEASURING: Mutex<()> = Mutex::new(());

struct Measuring;

unsafe impl GlobalAlloc for Measuring {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) == 1 {
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) == 1 {
            BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        System.realloc(p, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: Measuring = Measuring;

struct Hollow;

impl GraphSink for Hollow {
    fn on_base(&mut self, _b: &str) {}
    fn on_prefix(&mut self, _p: &str, _n: &str) {}
    fn term_iri(&mut self, _i: &str) -> TermId {
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

/// Bytes allocated parsing a deliberately tiny document under `hint`.
///
/// Tiny on purpose: the document's own cost is a rounding error, so what the
/// number reports is essentially the reservation.
fn bytes_for(hint: Option<usize>) -> usize {
    let _measuring = MEASURING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    const DOC: &str = "<http://ex/s> <http://ex/p> <http://ex/o> .\n";
    let mut options = fluree_graph_turtle::ParserOptions::default();
    if let Some(n) = hint {
        options = options.with_distinct_terms_hint(n);
    }
    let mut sink = Hollow;
    BYTES.store(0, Ordering::Relaxed);
    ARMED.store(1, Ordering::Relaxed);
    let r =
        fluree_graph_turtle::parse_with_prefixes_base_options(DOC, &mut sink, &[], None, options);
    ARMED.store(0, Ordering::Relaxed);
    r.expect("parses");
    BYTES.load(Ordering::Relaxed)
}

#[test]
fn the_hint_actually_sizes_the_caches() {
    // Warm anything lazily initialized so it lands outside both measurements.
    let _ = bytes_for(None);

    let floor = bytes_for(None);
    let hinted = bytes_for(Some(2_000_000));

    assert!(
        hinted > floor * 50,
        "a two-million-term hint allocated {hinted} bytes against {floor} at the \
         floor — under 50x apart, the hint is being accepted and ignored. This is \
         the null implementation the safety test cannot see: plumb the hint, then \
         reserve the floor anyway."
    );
}

#[test]
fn a_hint_past_the_clamp_reserves_the_clamp_and_not_the_hint() {
    // The clamp is also dodgeable — dropping it satisfies every other
    // assertion here — and its failure mode is an allocation the machine
    // cannot serve rather than a wrong answer.
    let _ = bytes_for(None);

    let at_clamp = bytes_for(Some(2_000_000));
    let absurd = bytes_for(Some(usize::MAX / 4));

    assert!(
        absurd <= at_clamp * 2,
        "a hint of usize::MAX/4 allocated {absurd} bytes against {at_clamp} at the \
         clamp — the clamp is not binding, and a caller whose estimate is wrong by \
         orders of magnitude will take the machine down rather than be corrected"
    );
}
