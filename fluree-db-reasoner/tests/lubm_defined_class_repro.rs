//! Repro for LUBM defined-class inference (someValuesFrom + intersectionOf) over
//! DERIVED facts. Mirrors the Univ-Bench `Student` and `Chair` definitions:
//!
//!   Student = Person AND (takesCourse some Course)
//!   Chair   = Professor AND (headOf some Department)
//!   GraduateStudent subClassOf Person ; GraduateCourse subClassOf Course
//!   FullProfessor subClassOf Professor
//!
//! A graduate student takes a GraduateCourse (typed Course only via inference);
//! a full professor headOf a Department. Both should be inferred Student / Chair.
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
// Blank-node namespace (code 10, "_:") — list/restriction nodes are blank nodes
// in the real Univ-Bench ontology.
fn bn(l: &str) -> Sid {
    Sid::new(10, l)
}
fn ref_dt() -> Sid {
    Sid::new(1, "id")
}
fn tf(s: Sid, p: Sid, o: Sid) -> Flake {
    Flake::new(s, p, FlakeValue::Ref(o), ref_dt(), 1, true, None)
}

#[tokio::test]
async fn lubm_grad_student_and_chair_defined_classes() {
    let ty = rdf("type");
    let sco = rdfs("subClassOf");

    let mut f = vec![
        // ---- ontology: class hierarchy ----
        tf(ex("GraduateStudent"), sco.clone(), ex("Person")),
        tf(ex("GraduateCourse"), sco.clone(), ex("Course")),
        tf(ex("FullProfessor"), sco.clone(), ex("Professor")),
        // ---- Student = Person AND (takesCourse some Course) ----
        tf(ex("Student"), owl("intersectionOf"), bn("S_list1")),
        tf(bn("S_list1"), rdf("first"), ex("Person")),
        tf(bn("S_list1"), rdf("rest"), bn("S_list2")),
        tf(bn("S_list2"), rdf("first"), bn("S_restr")),
        tf(bn("S_list2"), rdf("rest"), rdf("nil")),
        tf(bn("S_restr"), ty.clone(), owl("Restriction")),
        tf(bn("S_restr"), owl("onProperty"), ex("takesCourse")),
        tf(bn("S_restr"), owl("someValuesFrom"), ex("Course")),
        // ---- Chair = Professor AND (headOf some Department) ----
        tf(ex("Chair"), owl("intersectionOf"), bn("C_list1")),
        tf(bn("C_list1"), rdf("first"), ex("Professor")),
        tf(bn("C_list1"), rdf("rest"), bn("C_list2")),
        tf(bn("C_list2"), rdf("first"), bn("C_restr")),
        tf(bn("C_list2"), rdf("rest"), rdf("nil")),
        tf(bn("C_restr"), ty.clone(), owl("Restriction")),
        tf(bn("C_restr"), owl("onProperty"), ex("headOf")),
        tf(bn("C_restr"), owl("someValuesFrom"), ex("Department")),
        // ---- data ----
        // grad student g takes a graduate course gc (typed Course only via inference)
        tf(ex("g"), ty.clone(), ex("GraduateStudent")),
        tf(ex("g"), ex("takesCourse"), ex("gc")),
        tf(ex("gc"), ty.clone(), ex("GraduateCourse")),
        // full professor p headOf department d (d typed Department directly)
        tf(ex("p"), ty.clone(), ex("FullProfessor")),
        tf(ex("p"), ex("headOf"), ex("d")),
        tf(ex("d"), ty.clone(), ex("Department")),
    ];
    f.sort_by(|a, b| IndexType::Spot.compare(a, b));

    let mut snapshot = LedgerSnapshot::genesis("test/main");
    for (code, iri) in [
        (3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        (4, "http://www.w3.org/2000/01/rdf-schema#"),
        (6, "http://www.w3.org/2002/07/owl#"),
        (100, "http://example.org/"),
        (10, "_:"),
    ] {
        let _ = snapshot.insert_namespace_code(code, iri.to_string());
    }
    let overlay = SortedOverlay::new(1234, f);
    let cache = ReasoningCache::with_default_capacity();
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let res = reason_owl2rl(db, &ReasoningOptions::default(), &cache)
        .await
        .unwrap();

    // Collect derived rdf:type facts.
    let mut derived: Vec<(Sid, Sid)> = Vec::new(); // (subject, class)
    res.overlay
        .for_each_overlay_flake(0, IndexType::Spot, None, None, true, i64::MAX, &mut |fl| {
            if fl.p == rdf("type") {
                if let FlakeValue::Ref(c) = &fl.o {
                    derived.push((fl.s.clone(), c.clone()));
                }
            }
        });
    let has = |s: &str, c: &str| derived.iter().any(|(x, k)| *x == ex(s) && *k == ex(c));

    eprintln!("rules_fired: {:?}", res.diagnostics.rules_fired);
    eprintln!(
        "g types: {:?}",
        derived
            .iter()
            .filter(|(x, _)| *x == ex("g"))
            .map(|(_, c)| snapshot.decode_sid(c))
            .collect::<Vec<_>>()
    );
    eprintln!(
        "p types: {:?}",
        derived
            .iter()
            .filter(|(x, _)| *x == ex("p"))
            .map(|(_, c)| snapshot.decode_sid(c))
            .collect::<Vec<_>>()
    );

    assert!(has("g", "Person"), "cax-sco: g should be Person");
    assert!(has("gc", "Course"), "cax-sco: gc should be Course");
    assert!(
        has("g", "Student"),
        "DEFINED CLASS: grad student g should be inferred Student (Person AND takesCourse some Course)"
    );
    assert!(
        has("p", "Chair"),
        "DEFINED CLASS: full professor p should be inferred Chair (Professor AND headOf some Department)"
    );
}
