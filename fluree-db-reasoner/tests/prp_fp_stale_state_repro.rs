//! Repro for stale prp-fp/prp-ifp semi-naive state after a sameAs merge.
//!
//! The identity rules fold each iteration's delta into a persistent grouping
//! keyed by canonical subject/object. sameAs facts they derive are unioned
//! *after* the fold ran — so the next iteration's re-union is a no-op and
//! reports no change, leaving the fold branch running against keys
//! canonicalized *before* the merge. A new fact canonicalizing to the merged
//! root then misses the stale entry: vacant insert, no conflict, no sameAs.
//!
//! Derivation chain that must hold:
//!   prp-ifp: (A q v), (B q v), q inverse-functional → sameAs(A, B)
//!   prp-spo1: (A r y2), r ⊑ p                       → A p y2   (next iteration)
//!   prp-fp:  merged {A,B} has p-objects {y1, y2}    → sameAs(y1, y2)
use std::cmp::Ordering;

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, OverlayProvider, Sid};
use fluree_db_reasoner::{reason_owl2rl, ReasoningCache, ReasoningOptions};

struct SortedOverlay {
    epoch: u64,
    spot: Vec<Flake>,
    psot: Vec<Flake>,
    post: Vec<Flake>,
    opst: Vec<Flake>,
}
impl SortedOverlay {
    fn new(epoch: u64, flakes: Vec<Flake>) -> Self {
        let s = |mut v: Vec<Flake>, i: IndexType| {
            v.sort_by(|a, b| i.compare(a, b));
            v
        };
        Self {
            epoch,
            spot: s(flakes.clone(), IndexType::Spot),
            psot: s(flakes.clone(), IndexType::Psot),
            post: s(flakes.clone(), IndexType::Post),
            opst: s(flakes, IndexType::Opst),
        }
    }
    fn list(&self, i: IndexType) -> &[Flake] {
        match i {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }
}
impl OverlayProvider for SortedOverlay {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn epoch(&self) -> u64 {
        self.epoch
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
        for f in self.list(index) {
            if f.t > to_t {
                continue;
            }
            if !leftmost {
                if let Some(lb) = first {
                    if index.compare(f, lb) != Ordering::Greater {
                        continue;
                    }
                }
            }
            if let Some(rb) = rhs {
                if index.compare(f, rb) == Ordering::Greater {
                    continue;
                }
            }
            cb(f);
        }
    }
}

fn rdf(l: &str) -> Sid {
    Sid::new(3, l)
}
fn rdfs(l: &str) -> Sid {
    Sid::new(4, l)
}
fn owl(l: &str) -> Sid {
    Sid::new(6, l)
}
fn ex(l: &str) -> Sid {
    Sid::new(100, l)
}
fn ref_dt() -> Sid {
    Sid::new(1, "id")
}
fn tf(s: Sid, p: Sid, o: Sid) -> Flake {
    Flake::new(s, p, FlakeValue::Ref(o), ref_dt(), 1, true, None)
}

/// Run reasoning over the given facts and assert the prp-fp conflict on the
/// merged subject class was derived. `fp_subject` holds the pre-existing
/// functional-property fact (`fp_subject p y1`); `spo_subject` gains `p y2`
/// one iteration later via prp-spo1.
async fn assert_fp_conflict_after_merge(fp_subject: &str, spo_subject: &str) {
    let ty = rdf("type");

    let mut f = vec![
        // ---- ontology ----
        tf(ex("p"), ty.clone(), owl("FunctionalProperty")),
        tf(ex("q"), ty.clone(), owl("InverseFunctionalProperty")),
        tf(ex("r"), rdfs("subPropertyOf"), ex("p")),
        // ---- data ----
        tf(ex(fp_subject), ex("p"), ex("y1")),
        tf(ex("A"), ex("q"), ex("v")),
        tf(ex("B"), ex("q"), ex("v")),
        tf(ex(spo_subject), ex("r"), ex("y2")),
    ];
    f.sort_by(|a, b| IndexType::Spot.compare(a, b));

    let mut snapshot = LedgerSnapshot::genesis("test/main");
    for (code, iri) in [
        (3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        (4, "http://www.w3.org/2000/01/rdf-schema#"),
        (6, "http://www.w3.org/2002/07/owl#"),
        (100, "http://example.org/"),
    ] {
        let _ = snapshot.insert_namespace_code(code, iri.to_string());
    }
    let overlay = SortedOverlay::new(1234, f);
    let cache = ReasoningCache::with_default_capacity();
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let res = reason_owl2rl(db, &ReasoningOptions::default(), &cache)
        .await
        .unwrap();
    let same_as = res.overlay.same_as();

    eprintln!("rules_fired: {:?}", res.diagnostics.rules_fired);

    // Sanity: prp-ifp must have merged A and B.
    assert_eq!(
        same_as.canonical(ex("A")),
        same_as.canonical(ex("B")),
        "prp-ifp: A and B share object v under inverse-functional q"
    );

    // The regression: prp-fp must see {y1, y2} under the merged subject class.
    assert_eq!(
        same_as.canonical(ex("y1")),
        same_as.canonical(ex("y2")),
        "prp-fp must derive sameAs(y1, y2) after the A≡B merge \
         ({fp_subject} p y1 ; {spo_subject} r y2 ; r ⊑ p)"
    );
}

// Both mirrors so the test cannot pass by luck of union-find root selection.

#[tokio::test]
async fn prp_fp_detects_conflict_after_ifp_merge() {
    assert_fp_conflict_after_merge("B", "A").await;
}

#[tokio::test]
async fn prp_fp_detects_conflict_after_ifp_merge_mirrored() {
    assert_fp_conflict_after_merge("A", "B").await;
}

/// The merge can also land on what would otherwise be the *final* iteration:
/// base facts `A p y1 ; B p y2` produce no new delta on their own, so once
/// prp-ifp merges A≡B the loop must run one more pass to let prp-fp see the
/// merged subject class — `sameAs(y1, y2)` is required with no prp-spo1 step
/// involved at all.
#[tokio::test]
async fn prp_fp_detects_conflict_when_merge_is_final_iteration() {
    let ty = rdf("type");

    let mut f = vec![
        tf(ex("p"), ty.clone(), owl("FunctionalProperty")),
        tf(ex("q"), ty.clone(), owl("InverseFunctionalProperty")),
        tf(ex("A"), ex("p"), ex("y1")),
        tf(ex("B"), ex("p"), ex("y2")),
        tf(ex("A"), ex("q"), ex("v")),
        tf(ex("B"), ex("q"), ex("v")),
    ];
    f.sort_by(|a, b| IndexType::Spot.compare(a, b));

    let mut snapshot = LedgerSnapshot::genesis("test/main");
    for (code, iri) in [
        (3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        (4, "http://www.w3.org/2000/01/rdf-schema#"),
        (6, "http://www.w3.org/2002/07/owl#"),
        (100, "http://example.org/"),
    ] {
        let _ = snapshot.insert_namespace_code(code, iri.to_string());
    }
    let overlay = SortedOverlay::new(1234, f);
    let cache = ReasoningCache::with_default_capacity();
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let res = reason_owl2rl(db, &ReasoningOptions::default(), &cache)
        .await
        .unwrap();
    let same_as = res.overlay.same_as();

    assert_eq!(
        same_as.canonical(ex("A")),
        same_as.canonical(ex("B")),
        "prp-ifp: A and B share object v under inverse-functional q"
    );
    assert_eq!(
        same_as.canonical(ex("y1")),
        same_as.canonical(ex("y2")),
        "prp-fp must derive sameAs(y1, y2) on the post-merge iteration \
         (A p y1 ; B p y2)"
    );
}
