//! Integration tests for OWL2-RL property-rule materialization via the query layer.
//!
//! These are end-to-end tests of:
//! - `fluree-db-query::execute_with_overlay_at` reasoning pipeline (owl2rl)
//! - `fluree-db-reasoner` Phase 1 property rules (domain/range + propertyChainAxiom)
//!
//! We avoid requiring on-disk fixtures by using an in-memory DB and a small overlay
//! that provides the facts and schema triples needed for reasoning.

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::overlay::OverlayProvider;
use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, Sid};
use fluree_db_query::binding::{Binding, RowAccess};
use fluree_db_query::execute::{execute_with_overlay, ExecutableQuery};
use fluree_db_query::options::QueryOptions;
use fluree_db_query::parse::{parse_query, MemoryEncoder};
use fluree_db_query::rewrite::ReasoningModes;
use fluree_db_query::var_registry::VarRegistry;
use fluree_db_reasoner::{reason_owl2rl, ReasoningCache, ReasoningOptions};
use serde_json::json;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Parameters recorded from psot lookup calls: (start, end, inclusive, limit)
type PsotCallParams = (Option<Flake>, Option<Flake>, bool, i64);

/// Small overlay that stores a fixed set of flakes, pre-sorted for all indexes,
/// and respects range boundaries.
///
/// This is intentionally minimal but correct: `range_with_overlay` relies on the
/// overlay to emit flakes in index order and within the requested bounds.
struct SortedOverlay {
    epoch: u64,
    spot: Vec<Flake>,
    psot: Vec<Flake>,
    post: Vec<Flake>,
    opst: Vec<Flake>,
    psot_last_call: std::sync::Mutex<Option<PsotCallParams>>,
}

impl SortedOverlay {
    fn new(epoch: u64, flakes: Vec<Flake>) -> Self {
        fn sorted(mut v: Vec<Flake>, idx: IndexType) -> Vec<Flake> {
            v.sort_by(|a, b| idx.compare(a, b));
            v
        }

        Self {
            epoch,
            spot: sorted(flakes.clone(), IndexType::Spot),
            psot: sorted(flakes.clone(), IndexType::Psot),
            post: sorted(flakes.clone(), IndexType::Post),
            opst: sorted(flakes, IndexType::Opst),
            psot_last_call: std::sync::Mutex::new(None),
        }
    }

    fn list_for_index(&self, index: IndexType) -> &[Flake] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    fn in_bounds(
        index: IndexType,
        f: &Flake,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> bool {
        // Left boundary semantics (see fluree-db-core OverlayProvider docs):
        // - If leftmost=false: left boundary is EXCLUSIVE (> first)
        // - If leftmost=true:  no left boundary (start from beginning)
        if !leftmost {
            if let Some(lb) = first {
                if index.compare(f, lb) != Ordering::Greater {
                    return false;
                }
            }
        }

        // Right boundary is inclusive.
        if let Some(rb) = rhs {
            if index.compare(f, rb) == Ordering::Greater {
                return false;
            }
        }

        true
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
        _g_id: fluree_db_core::GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        if index == IndexType::Psot {
            *self.psot_last_call.lock().unwrap() =
                Some((first.cloned(), rhs.cloned(), leftmost, to_t));
        }
        for f in self.list_for_index(index) {
            if f.t > to_t {
                continue;
            }
            if !Self::in_bounds(index, f, first, rhs, leftmost) {
                continue;
            }
            callback(f);
        }
    }
}

fn ref_dt() -> Sid {
    Sid::new(1, "id")
}

fn sid_ex(local: &str) -> Sid {
    Sid::new(100, local)
}

fn sid_rdf(local: &str) -> Sid {
    Sid::new(3, local)
}

fn sid_rdfs(local: &str) -> Sid {
    Sid::new(4, local)
}

fn sid_owl(local: &str) -> Sid {
    Sid::new(6, local)
}

fn flake_ref(s: Sid, p: Sid, o: Sid, t: i64) -> Flake {
    Flake::new(s, p, FlakeValue::Ref(o), ref_dt(), t, true, None)
}

fn flake_str(s: Sid, p: Sid, o: &str, t: i64) -> Flake {
    Flake::new(
        s,
        p,
        FlakeValue::String(o.into()),
        Sid::new(2, "string"),
        t,
        true,
        None,
    )
}

fn overlay_epoch_from_flakes(flakes: &[Flake]) -> u64 {
    // The query layer uses a global reasoning cache keyed by (db_epoch, overlay_epoch, to_t, ...).
    // Make the overlay epoch reflect the overlay contents so test overlays never collide.
    let mut h = DefaultHasher::new();
    for f in flakes {
        f.s.hash(&mut h);
        f.p.hash(&mut h);
        format!("{:?}", f.o).hash(&mut h);
        f.dt.hash(&mut h);
        f.t.hash(&mut h);
        f.op.hash(&mut h);
    }
    1u64.wrapping_add(h.finish())
}

#[tokio::test]
async fn owl2rl_domain_range_and_chain_visible_via_execute_with_overlay() {
    // Base DB is empty; all facts come from overlay.
    let mut snapshot = LedgerSnapshot::genesis("test/main");
    // The query parser lowers IRIs as `Term::Iri` and scan time encodes them via `snapshot.encode_iri`.
    // Since this test constructs facts directly as SIDs in an overlay, we must teach the DB
    // the namespace codes used by those SIDs so encoding succeeds.
    // Use insert_namespace_code to keep the reverse map in sync (needed for
    // canonical encode_iri lookups). Codes 3, 4, 6 already exist in defaults
    // but are harmless no-ops; code 100 is the critical new one.
    snapshot
        .insert_namespace_code(3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string())
        .unwrap();
    snapshot
        .insert_namespace_code(4, "http://www.w3.org/2000/01/rdf-schema#".to_string())
        .unwrap();
    snapshot
        .insert_namespace_code(6, "http://www.w3.org/2002/07/owl#".to_string())
        .unwrap();
    snapshot
        .insert_namespace_code(100, "http://example.org/".to_string())
        .unwrap();

    // Vocabulary
    let person = sid_ex("Person");
    let parent_of = sid_ex("parentOf");
    let grandparent_of = sid_ex("grandparentOf");

    let alice = sid_ex("alice");
    let bob = sid_ex("bob");
    let charlie = sid_ex("charlie");

    // RDF/RDFS/OWL predicates
    let rdf_type = sid_rdf("type");
    let rdf_first = sid_rdf("first");
    let rdf_rest = sid_rdf("rest");
    let rdf_nil = sid_rdf("nil");

    let rdfs_domain = sid_rdfs("domain");
    let rdfs_range = sid_rdfs("range");
    let owl_chain = sid_owl("propertyChainAxiom");

    // RDF list nodes for chain (grandparentOf = parentOf o parentOf)
    let list1 = sid_ex("list1");
    let list2 = sid_ex("list2");

    // Overlay facts (t=1)
    let mut flakes: Vec<Flake> = vec![
        // Domain/range axioms: parentOf domain Person; range Person
        flake_ref(parent_of.clone(), rdfs_domain.clone(), person.clone(), 1),
        flake_ref(parent_of.clone(), rdfs_range, person.clone(), 1),
        // propertyChainAxiom: grandparentOf chain (parentOf parentOf)
        flake_ref(grandparent_of.clone(), owl_chain, list1.clone(), 1),
        flake_ref(list1.clone(), rdf_first.clone(), parent_of.clone(), 1),
        flake_ref(list1.clone(), rdf_rest.clone(), list2.clone(), 1),
        flake_ref(list2.clone(), rdf_first, parent_of.clone(), 1),
        flake_ref(list2.clone(), rdf_rest, rdf_nil, 1),
        // Data: alice parentOf bob; bob parentOf charlie
        flake_ref(alice.clone(), parent_of.clone(), bob.clone(), 1),
        flake_ref(bob.clone(), parent_of.clone(), charlie.clone(), 1),
        // A domain-only case with a literal object (still implies alice rdf:type Person)
        flake_ref(sid_ex("age"), rdfs_domain.clone(), person.clone(), 1),
        flake_str(alice.clone(), sid_ex("age"), "30", 1),
        // No explicit rdf:type assertions in base data.
    ];

    // Ensure we didn't accidentally add explicit rdf:type facts.
    assert!(!flakes.iter().any(|f| f.p == rdf_type));

    let overlay_epoch = overlay_epoch_from_flakes(&flakes);
    let overlay = SortedOverlay::new(overlay_epoch, std::mem::take(&mut flakes));

    // Sanity check: core range_with_overlay can see the schema axioms in overlay.
    let domain_flakes = range_with_overlay(
        &snapshot,
        0,
        &overlay,
        IndexType::Psot,
        RangeTest::Eq,
        RangeMatch {
            p: Some(rdfs_domain.clone()),
            ..Default::default()
        },
        RangeOptions {
            to_t: Some(10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    if domain_flakes.is_empty() {
        let last = overlay.psot_last_call.lock().unwrap().clone();
        panic!(
            "Expected to read rdfs:domain axioms via range_with_overlay; overlay boundary logic may be incorrect. last Psot call = {last:?}"
        );
    }

    // Encoder for parsing query JSON into Sids consistent with our overlay.
    let mut encoder = MemoryEncoder::with_common_namespaces();
    encoder
        .add_namespace("http://www.w3.org/2000/01/rdf-schema#", 4)
        .add_namespace("http://www.w3.org/2002/07/owl#", 6)
        .add_namespace("http://example.org/", 100);

    // Query A: find all ?x of type ex:Person.
    let mut vars_a = VarRegistry::new();
    let q_type = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "select": ["?x"],
        "where": { "@id": "?x", "@type": "ex:Person" }
    });
    let parsed_a = parse_query(&q_type, &encoder, &mut vars_a, None).unwrap();

    // Run without owl2rl: should be empty (no explicit rdf:type triples).
    let exec_no = ExecutableQuery::new(
        parsed_a.clone(),
        QueryOptions::new().with_reasoning(ReasoningModes::default()),
    );
    let source = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let res_no = execute_with_overlay(source, &vars_a, &exec_no)
        .await
        .unwrap();
    let total_rows_no: usize = res_no.iter().map(fluree_db_query::Batch::len).sum();
    assert_eq!(
        total_rows_no, 0,
        "Expected no results without owl2rl materialization"
    );

    // Run with owl2rl: domain/range should materialize rdf:type(x, Person).
    let exec_yes = ExecutableQuery::new(
        parsed_a,
        QueryOptions::new().with_reasoning(ReasoningModes::default().with_owl2rl()),
    );
    let source = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let res_yes = execute_with_overlay(source, &vars_a, &exec_yes)
        .await
        .unwrap();

    // Sanity check: reasoner itself produces derived facts from this overlay.
    let cache = ReasoningCache::with_default_capacity();
    let db = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let reasoner_res = reason_owl2rl(db, &ReasoningOptions::default(), &cache)
        .await
        .unwrap();
    assert!(
        !reasoner_res.overlay.is_empty(),
        "Expected reasoner to materialize derived facts (domain/range/chain)"
    );

    let x = vars_a.get_or_insert("?x");
    let mut got: Vec<Sid> = Vec::new();
    for batch in res_yes {
        for row_idx in 0..batch.len() {
            let row = batch.row_view(row_idx).unwrap();
            match row.get(x) {
                Some(Binding::Sid(sid)) => got.push(sid.clone()),
                Some(other) => panic!("Expected Sid binding for ?x, got {other:?}"),
                None => panic!("Expected binding for ?x"),
            }
        }
    }
    got.sort();
    got.dedup();

    // alice: domain(parentOf) + domain(age)
    // bob: domain(parentOf) + range(parentOf)
    // charlie: range(parentOf)
    assert_eq!(got, vec![alice.clone(), bob.clone(), charlie.clone()]);

    // Query B: alice ex:grandparentOf ?o should return charlie via prp-spo2 materialization.
    let mut vars_b = VarRegistry::new();
    let q_chain = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?o"],
        "where": { "@id": "ex:alice", "ex:grandparentOf": "?o" }
    });
    let parsed_b = parse_query(&q_chain, &encoder, &mut vars_b, None).unwrap();

    let exec_chain = ExecutableQuery::new(
        parsed_b,
        QueryOptions::new().with_reasoning(ReasoningModes::default().with_owl2rl()),
    );
    let source = GraphDbRef::new(&snapshot, 0, &overlay, 10);
    let res_chain = execute_with_overlay(source, &vars_b, &exec_chain)
        .await
        .unwrap();

    let o = vars_b.get_or_insert("?o");
    let mut got_o: Vec<Sid> = Vec::new();
    for batch in res_chain {
        for row_idx in 0..batch.len() {
            let row = batch.row_view(row_idx).unwrap();
            match row.get(o) {
                Some(Binding::Sid(sid)) => got_o.push(sid.clone()),
                Some(other) => panic!("Expected Sid binding for ?o, got {other:?}"),
                None => panic!("Expected binding for ?o"),
            }
        }
    }
    got_o.sort();
    got_o.dedup();
    assert_eq!(got_o, vec![charlie]);
}
