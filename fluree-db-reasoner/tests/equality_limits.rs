//! The equality rules stop at a budget and at cancellation, as the rounds do.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as Atomic};

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, OverlayProvider, QueryCancellation, Sid};
use fluree_db_reasoner::fixpoint::run_fixpoint;
use fluree_db_reasoner::{ReasonerError, ReasoningOptions};

/// Facts in each index order. On its `cancel_at`-th OPST read it cancels
/// `cancel`.
struct Overlay {
    spot: Vec<Flake>,
    psot: Vec<Flake>,
    post: Vec<Flake>,
    opst: Vec<Flake>,
    opst_reads: AtomicUsize,
    cancel_at: usize,
    cancel: QueryCancellation,
}

impl Overlay {
    fn new(flakes: Vec<Flake>, cancel_at: usize, cancel: QueryCancellation) -> Self {
        let sorted = |mut v: Vec<Flake>, i: IndexType| {
            v.sort_by(|a, b| i.compare(a, b));
            v
        };
        Self {
            spot: sorted(flakes.clone(), IndexType::Spot),
            psot: sorted(flakes.clone(), IndexType::Psot),
            post: sorted(flakes.clone(), IndexType::Post),
            opst: sorted(flakes, IndexType::Opst),
            opst_reads: AtomicUsize::new(0),
            cancel_at,
            cancel,
        }
    }
}

impl OverlayProvider for Overlay {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn epoch(&self) -> u64 {
        1
    }
    fn for_each_overlay_flake(
        &self,
        _g: fluree_db_core::GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        cb: &mut dyn FnMut(&Flake),
    ) {
        let list = match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => {
                if self.opst_reads.fetch_add(1, Atomic::SeqCst) + 1 == self.cancel_at {
                    self.cancel.cancel();
                }
                &self.opst
            }
        };
        for f in list {
            if f.t > to_t {
                continue;
            }
            if !leftmost && first.is_some_and(|lb| index.compare(f, lb) != Ordering::Greater) {
                continue;
            }
            if rhs.is_some_and(|rb| index.compare(f, rb) == Ordering::Greater) {
                continue;
            }
            cb(f);
        }
    }
}

fn ex(l: &str) -> Sid {
    Sid::new(100, l)
}

fn fact(s: Sid, p: Sid, o: Sid) -> Flake {
    Flake::new(s, p, FlakeValue::Ref(o), Sid::new(1, "id"), 1, true, None)
}

fn snapshot() -> LedgerSnapshot {
    let mut snapshot = LedgerSnapshot::genesis("test:main");
    for (code, iri) in [
        (3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        (4, "http://www.w3.org/2000/01/rdf-schema#"),
        (6, "http://www.w3.org/2002/07/owl#"),
        (100, "http://example.org/"),
    ] {
        let _ = snapshot.insert_namespace_code(code, iri.to_string());
    }
    snapshot
}

const MEMBERS: usize = 30;

/// `m0 … m29`, one equality set, and `m0 knows m0`: every member knows every
/// member, 900 facts from one.
fn one_large_set() -> Vec<Flake> {
    let same_as = Sid::new(6, "sameAs");
    let mut facts: Vec<Flake> = (1..MEMBERS)
        .map(|i| {
            fact(
                ex(&format!("m{}", i - 1)),
                same_as.clone(),
                ex(&format!("m{i}")),
            )
        })
        .collect();
    facts.push(fact(ex("m0"), ex("knows"), ex("m0")));
    facts
}

/// The 870 `owl:sameAs` pairs fit the budget; the 899 copies of one fact do
/// not. The run stops within a fact of the budget, not one source fact past it.
#[tokio::test]
async fn a_budget_stops_equality_within_a_fact_of_it() {
    let snapshot = snapshot();
    let overlay = Overlay::new(one_large_set(), 0, QueryCancellation::new());
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let mut opts = ReasoningOptions::default();
    opts.budget.max_facts = 1_000;

    let (flakes, _, diagnostics) = run_fixpoint(db, &opts).await.unwrap();
    assert!(diagnostics.capped, "{diagnostics:?}");
    assert_eq!(diagnostics.capped_reason.as_deref(), Some("facts"));
    assert!(flakes.len() <= 1_001, "{} facts", flakes.len());
}

/// A cancel that lands during the last member's read fails the run: a run
/// cancelled after its last check is not complete.
#[tokio::test]
async fn a_cancel_during_the_last_read_fails_the_run() {
    let snapshot = snapshot();
    // How many OPST reads a whole run makes; the last is a member's.
    let uncancelled = Overlay::new(one_large_set(), usize::MAX, QueryCancellation::new());
    let db = GraphDbRef::new(&snapshot, 0, &uncancelled, 10);
    run_fixpoint(db, &ReasoningOptions::default())
        .await
        .unwrap();
    let reads = uncancelled.opst_reads.load(Atomic::SeqCst);
    assert!(reads >= MEMBERS, "{reads} OPST reads");

    let cancel = QueryCancellation::new();
    let overlay = Overlay::new(one_large_set(), reads, cancel.clone());
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let opts = ReasoningOptions {
        cancellation: cancel,
        ..ReasoningOptions::default()
    };

    let result = run_fixpoint(db, &opts).await;
    assert!(
        matches!(result, Err(ReasonerError::Cancelled(_))),
        "{:?}",
        result.map(|(_, _, d)| d)
    );
    assert_eq!(overlay.opst_reads.load(Atomic::SeqCst), reads);
}
