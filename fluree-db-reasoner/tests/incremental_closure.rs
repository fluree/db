//! Closure maintenance under insertions (`run_fixpoint_incremental`): from a
//! closed state plus inserted facts, exactly what the closure gains.

use std::cmp::Ordering;
use std::collections::HashSet;

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, OverlayProvider, Sid};
use fluree_db_reasoner::fixpoint::run_fixpoint_incremental;
use fluree_db_reasoner::ReasoningOptions;

struct SortedOverlay {
    spot: Vec<Flake>,
    psot: Vec<Flake>,
    post: Vec<Flake>,
    opst: Vec<Flake>,
}

impl SortedOverlay {
    fn new(flakes: Vec<Flake>) -> Self {
        let sorted = |mut v: Vec<Flake>, i: IndexType| {
            v.sort_by(|a, b| i.compare(a, b));
            v
        };
        Self {
            spot: sorted(flakes.clone(), IndexType::Spot),
            psot: sorted(flakes.clone(), IndexType::Psot),
            post: sorted(flakes.clone(), IndexType::Post),
            opst: sorted(flakes, IndexType::Opst),
        }
    }
}

impl OverlayProvider for SortedOverlay {
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
            IndexType::Opst => &self.opst,
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

fn ty() -> Sid {
    Sid::new(3, "type")
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

/// What maintaining `state` (already closed, `inserted` among it) yields.
async fn gained(state: Vec<Flake>, inserted: &[Flake]) -> Option<HashSet<Flake>> {
    let snapshot = snapshot();
    let overlay = SortedOverlay::new(state);
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    run_fixpoint_incremental(db, db, inserted, &ReasoningOptions::default())
        .await
        .unwrap()
        .map(|(flakes, _)| flakes.into_iter().collect())
}

#[tokio::test]
async fn an_insertion_yields_only_what_the_closure_gains() {
    let ontology = vec![
        fact(ex("Student"), Sid::new(4, "subClassOf"), ex("Person")),
        fact(ex("partOf"), ty(), Sid::new(6, "TransitiveProperty")),
    ];
    // The previous closure: asserted facts and what they entailed.
    let closed = vec![
        fact(ex("alice"), ty(), ex("Student")),
        fact(ex("alice"), ty(), ex("Person")),
        fact(ex("a"), ex("partOf"), ex("b")),
        fact(ex("b"), ex("partOf"), ex("c")),
        fact(ex("a"), ex("partOf"), ex("c")),
    ];
    let name = Flake::new(
        ex("bob"),
        ex("name"),
        FlakeValue::String("Bob".into()),
        Sid::new(2, "string"),
        2,
        true,
        None,
    );
    let inserted = vec![
        fact(ex("bob"), ty(), ex("Student")),
        name,
        fact(ex("c"), ex("partOf"), ex("d")),
    ];
    let state: Vec<Flake> = ontology
        .into_iter()
        .chain(closed)
        .chain(inserted.iter().cloned())
        .collect();

    let gained = gained(state, &inserted).await.expect("maintainable");
    let expected: HashSet<Flake> = [
        fact(ex("bob"), ty(), ex("Person")),
        fact(ex("b"), ex("partOf"), ex("d")),
        fact(ex("a"), ex("partOf"), ex("d")),
    ]
    .into_iter()
    .collect();
    assert_eq!(gained, expected);
}

#[tokio::test]
async fn identity_rules_and_same_as_are_left_to_a_full_run() {
    let base = vec![fact(ex("Student"), Sid::new(4, "subClassOf"), ex("Person"))];
    let inserted = vec![fact(ex("bob"), ty(), ex("Student"))];

    let functional = [
        base.clone(),
        vec![fact(ex("p"), ty(), Sid::new(6, "FunctionalProperty"))],
        inserted.clone(),
    ]
    .concat();
    assert!(gained(functional, &inserted).await.is_none());

    let same_as = [
        base.clone(),
        vec![fact(ex("x"), Sid::new(6, "sameAs"), ex("y"))],
        inserted.clone(),
    ]
    .concat();
    assert!(gained(same_as, &inserted).await.is_none());

    let plain = [base, inserted.clone()].concat();
    assert!(gained(plain, &inserted).await.is_some());
}

/// A conclusion of a predicate no rule reads (a super-property, a chain's
/// head) that the state already holds is not gained, whether asserted or
/// derived before; one it does not hold is.
#[tokio::test]
async fn a_conclusion_the_state_already_holds_is_not_gained() {
    let rdf = |l: &str| Sid::new(3, l);
    let ontology = vec![
        fact(ex("advisor"), Sid::new(4, "subPropertyOf"), ex("knows")),
        fact(
            ex("grandparent"),
            Sid::new(6, "propertyChainAxiom"),
            ex("l1"),
        ),
        fact(ex("l1"), rdf("first"), ex("parent")),
        fact(ex("l1"), rdf("rest"), ex("l2")),
        fact(ex("l2"), rdf("first"), ex("parent")),
        fact(ex("l2"), rdf("rest"), rdf("nil")),
    ];
    let held = vec![
        fact(ex("a"), ex("knows"), ex("b")),
        fact(ex("b"), ex("parent"), ex("c")),
        fact(ex("a"), ex("grandparent"), ex("c")),
    ];
    let inserted = vec![
        fact(ex("a"), ex("advisor"), ex("b")),
        fact(ex("a"), ex("parent"), ex("b")),
        fact(ex("a"), ex("advisor"), ex("z")),
    ];
    let state: Vec<Flake> = ontology
        .into_iter()
        .chain(held)
        .chain(inserted.iter().cloned())
        .collect();

    let gained = gained(state, &inserted).await.expect("maintainable");
    let expected: HashSet<Flake> = [fact(ex("a"), ex("knows"), ex("z"))].into_iter().collect();
    assert_eq!(gained, expected);
}

/// A join can need a fact derived rounds before, no longer in the delta
/// and not in the closed state: `a q b` (round 1) meets `b q c`, which takes
/// an inverse and then a sub-property step (round 2), in round 3.
#[tokio::test]
async fn a_join_reaches_facts_derived_rounds_before() {
    let sub = Sid::new(4, "subPropertyOf");
    let ontology = vec![
        fact(ex("p"), sub.clone(), ex("q")),
        fact(ex("t"), Sid::new(6, "inverseOf"), ex("u")),
        fact(ex("u"), sub, ex("q")),
        fact(ex("q"), ty(), Sid::new(6, "TransitiveProperty")),
    ];
    let inserted = vec![
        fact(ex("a"), ex("p"), ex("b")),
        fact(ex("c"), ex("t"), ex("b")),
    ];
    let state: Vec<Flake> = ontology
        .into_iter()
        .chain(inserted.iter().cloned())
        .collect();

    let gained = gained(state, &inserted).await.expect("maintainable");
    let expected: HashSet<Flake> = [
        fact(ex("a"), ex("q"), ex("b")),
        fact(ex("b"), ex("u"), ex("c")),
        fact(ex("b"), ex("q"), ex("c")),
        fact(ex("a"), ex("q"), ex("c")),
    ]
    .into_iter()
    .collect();
    assert_eq!(gained, expected);
}

/// A property chain learns each link's key from the read before it: an
/// insertion at the head of a three-link chain reaches two links into the
/// closed state.
#[tokio::test]
async fn a_property_chain_learns_each_key_from_the_read_before() {
    let rdf = |l: &str| Sid::new(3, l);
    let ontology = vec![
        fact(
            ex("greatGrandparent"),
            Sid::new(6, "propertyChainAxiom"),
            ex("l1"),
        ),
        fact(ex("l1"), rdf("first"), ex("parent")),
        fact(ex("l1"), rdf("rest"), ex("l2")),
        fact(ex("l2"), rdf("first"), ex("parent")),
        fact(ex("l2"), rdf("rest"), ex("l3")),
        fact(ex("l3"), rdf("first"), ex("parent")),
        fact(ex("l3"), rdf("rest"), rdf("nil")),
    ];
    let held = vec![
        fact(ex("b"), ex("parent"), ex("c")),
        fact(ex("c"), ex("parent"), ex("d")),
        fact(ex("c"), ex("parent"), ex("e")),
    ];
    let inserted = vec![fact(ex("a"), ex("parent"), ex("b"))];
    let state: Vec<Flake> = ontology
        .into_iter()
        .chain(held)
        .chain(inserted.iter().cloned())
        .collect();

    let gained = gained(state, &inserted).await.expect("maintainable");
    let expected: HashSet<Flake> = [
        fact(ex("a"), ex("greatGrandparent"), ex("d")),
        fact(ex("a"), ex("greatGrandparent"), ex("e")),
    ]
    .into_iter()
    .collect();
    assert_eq!(gained, expected);
}

/// The closed state, counting its subject-predicate reads and taking `delay`
/// over each.
struct CountedOverlay {
    inner: SortedOverlay,
    reads: std::sync::atomic::AtomicUsize,
    delay: std::time::Duration,
}

impl CountedOverlay {
    fn new(state: Vec<Flake>, delay: std::time::Duration) -> Self {
        Self {
            inner: SortedOverlay::new(state),
            reads: std::sync::atomic::AtomicUsize::new(0),
            delay,
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl OverlayProvider for CountedOverlay {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn epoch(&self) -> u64 {
        1
    }
    fn for_each_overlay_flake(
        &self,
        g: fluree_db_core::GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        cb: &mut dyn FnMut(&Flake),
    ) {
        if index == IndexType::Spot {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(self.delay);
        }
        self.inner
            .for_each_overlay_flake(g, index, first, rhs, leftmost, to_t, cb);
    }
}

/// 64 students inserted at once: each needs its own read.
fn many_students() -> (Vec<Flake>, Vec<Flake>) {
    let inserted: Vec<Flake> = (0..64)
        .map(|i| fact(ex(&format!("s{i}")), ty(), ex("Student")))
        .collect();
    let mut state = vec![fact(ex("Student"), Sid::new(4, "subClassOf"), ex("Person"))];
    state.extend(inserted.iter().cloned());
    (state, inserted)
}

/// The reads a stage is waiting for count against the working bound, so a
/// bound they already exceed stops the run before it reads anything.
#[tokio::test]
async fn pending_reads_count_against_the_working_bound() {
    let snapshot = snapshot();
    let (state, inserted) = many_students();
    let overlay = CountedOverlay::new(state, std::time::Duration::ZERO);
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let mut opts = ReasoningOptions::default();
    opts.maintenance.working_bytes = 1;
    let result = run_fixpoint_incremental(db, db, &inserted, &opts)
        .await
        .unwrap();
    assert!(result.is_none(), "outgrown: computed in full");
    assert_eq!(overlay.reads(), 0);
}

/// The working bound is checked before every read, not after a batch: a
/// bound with room for some of a stage's reads stops partway through them.
#[tokio::test]
async fn the_working_bound_is_checked_between_reads() {
    let snapshot = snapshot();
    let (state, inserted) = many_students();
    let overlay = CountedOverlay::new(state, std::time::Duration::ZERO);
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let mut opts = ReasoningOptions::default();
    opts.maintenance.working_bytes = 8 * 1024;
    let result = run_fixpoint_incremental(db, db, &inserted, &opts)
        .await
        .unwrap();
    assert!(result.is_none(), "outgrown: computed in full");
    let reads = overlay.reads();
    assert!(reads > 0 && reads < 64, "{reads} of 64 reads");
}

/// A run that reaches its fixpoint after its deadline is capped, not
/// complete.
#[tokio::test]
async fn a_run_past_its_deadline_is_capped() {
    let snapshot = snapshot();
    let inserted = vec![fact(ex("s"), ty(), ex("Student"))];
    let state = vec![
        fact(ex("Student"), Sid::new(4, "subClassOf"), ex("Person")),
        fact(ex("s"), ty(), ex("Person")),
        inserted[0].clone(),
    ];
    let overlay = CountedOverlay::new(state, std::time::Duration::from_millis(150));
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let mut opts = ReasoningOptions::default();
    opts.budget.max_duration = std::time::Duration::from_millis(100);
    let (_, diagnostics) = run_fixpoint_incremental(db, db, &inserted, &opts)
        .await
        .unwrap()
        .expect("a budget caps the run");
    assert!(diagnostics.capped);
    assert_eq!(diagnostics.capped_reason.as_deref(), Some("time"));
}
