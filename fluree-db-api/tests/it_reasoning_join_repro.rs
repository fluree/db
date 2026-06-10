//! Reproduction for REASONING_JOIN_BUG.md
//!
//! A derived (inferred) `rdf:type` pattern fails to join with a node/ref-valued
//! property pattern on the same subject, while it works with a literal-valued
//! property and works when the type is asserted (base) rather than derived.

mod support;

use fluree_db_api::{FlureeBuilder, QueryInput};
use serde_json::json;
use support::{genesis_ledger, normalize_rows, rebuild_and_publish_index};

/// Insert `data`, build+publish the binary index, then run `q` against the
/// reloaded indexed view (binary store attached — the path that diverged from
/// in-memory novelty in the LUBM retest).
async fn run_indexed(
    ledger_id: &str,
    data: &serde_json::Value,
    q: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let canonical = fluree.insert(ledger0, data).await.unwrap().ledger;
    let lid = canonical.snapshot.ledger_id.to_string();
    rebuild_and_publish_index(&fluree, &lid).await;
    let view = fluree.db(&lid).await.expect("load indexed view");
    let res = fluree
        .query(&view, QueryInput::JsonLd(q))
        .await
        .expect("indexed query");
    normalize_rows(&res.to_jsonld(&view.snapshot).expect("to_jsonld"))
}

/// LUBM-shaped fixture: GraduateStudent ⊑ Student ⊑ Person.
/// `ex:g` is an asserted GraduateStudent, inferred Student and Person.
/// It has a ref-valued property (takesCourse → ex:c0), another ref-valued
/// property (advisor → ex:p1), and a literal-valued property (name).
async fn lubm_fixture() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "reasoning/join-repro");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:GraduateStudent", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Student"}},
            {"@id": "ex:Student", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Person"}},
            {"@id": "ex:c0", "@type": "ex:GraduateCourse"},
            {"@id": "ex:c65", "@type": "ex:GraduateCourse"},
            {"@id": "ex:p1", "@type": "ex:Professor"},
            {
                "@id": "ex:g",
                "@type": "ex:GraduateStudent",
                "ex:takesCourse": [{"@id": "ex:c0"}, {"@id": "ex:c65"}],
                "ex:advisor": {"@id": "ex:p1"},
                "ex:name": "GraduateStudent101"
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    (fluree, ledger)
}

async fn run(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
    q: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let rows = support::query_jsonld(fluree, ledger, q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    normalize_rows(&rows)
}

// #1 single derived-type pattern (generator)
#[tokio::test]
async fn case01_single_derived_type() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Student"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert!(
        r.contains(&json!("ex:g")),
        "single derived-type should include ex:g, got {r:?}"
    );
}

// #4 derived type + LITERAL property (expected: works)
#[tokio::test]
async fn case04_derived_type_plus_literal_prop() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?n",
        "where": {"@id": "ex:g", "@type": "ex:Student", "ex:name": "?n"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("GraduateStudent101")],
        "derived type + literal prop"
    );
}

// #5 BASE type + REF property (expected: works)
#[tokio::test]
async fn case05_base_type_plus_ref_prop() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?c",
        "where": {"@id": "ex:g", "@type": "ex:GraduateStudent", "ex:takesCourse": "?c"},
        "reasoning": "owl2rl"
    });
    let mut r = run(&fluree, &ledger, &q).await;
    r.sort_by_key(ToString::to_string);
    assert_eq!(
        r,
        vec![json!("ex:c0"), json!("ex:c65")],
        "base type + ref prop"
    );
}

// #6 derived type + REF property (BUG: returns empty)
#[tokio::test]
async fn case06_derived_type_plus_ref_prop() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?c",
        "where": {"@id": "ex:g", "@type": "ex:Student", "ex:takesCourse": "?c"},
        "reasoning": "owl2rl"
    });
    let mut r = run(&fluree, &ledger, &q).await;
    r.sort_by_key(ToString::to_string);
    assert_eq!(
        r,
        vec![json!("ex:c0"), json!("ex:c65")],
        "BUG: derived type + ref prop"
    );
}

// #8 derived type + non-reasoned REF property (advisor)
#[tokio::test]
async fn case08_derived_type_plus_advisor() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?o",
        "where": {"@id": "ex:g", "@type": "ex:Student", "ex:advisor": "?o"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("ex:p1")],
        "BUG: derived type + advisor ref prop"
    );
}

// #12 variable subject join, bound ref object
#[tokio::test]
async fn case12_var_subject_bound_ref_object() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": {"@id": "?x", "@type": "ex:Student", "ex:takesCourse": {"@id": "ex:c0"}},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert!(
        r.contains(&json!("ex:g")),
        "BUG: var subject + bound ref object should include ex:g, got {r:?}"
    );
}

// #11 variable subject join, variable ref object
#[tokio::test]
async fn case11_var_subject_var_ref_object() {
    let (fluree, ledger) = lubm_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": {"@id": "?x", "@type": "ex:Student", "ex:takesCourse": "?c"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert!(
        r.contains(&json!("ex:g")),
        "var subject + var ref object should include ex:g, got {r:?}"
    );
}

// ===================== DEEPER VARIANT (3+ pattern chains, derived property) =====================

/// LUBM q07/q09-shaped: 3-pattern chain whose FIRST pattern's type is derived.
/// GraduateStudent ⊑ Student; ex:g a GraduateStudent (=> derived Student);
/// ex:g memberOf ex:d0 ; ex:d0 a Department (base).
async fn chain_fixture() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "reasoning/chain-repro");
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:GraduateStudent", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Student"}},
            {"@id": "ex:d0", "@type": "ex:Department"},
            {"@id": "ex:ug", "@type": "ex:UndergraduateStudent", "ex:memberOf": {"@id": "ex:d0"}},
            {"@id": "ex:g", "@type": "ex:GraduateStudent", "ex:memberOf": {"@id": "ex:d0"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    (fluree, ledger)
}

// CONTROL: 3-pattern chain, BASE type in first pattern. Expected to work.
#[tokio::test]
async fn chain_base_type_three_pattern() {
    let (fluree, ledger) = chain_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:UndergraduateStudent"},
            {"@id": "?x", "ex:memberOf": "?d"},
            {"@id": "?d", "@type": "ex:Department"}
        ],
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(r, vec![json!("ex:ug")], "3-pattern base-type chain: {r:?}");
}

// BUG: same 3-pattern chain but FIRST pattern's type (Student) is derived.
#[tokio::test]
async fn chain_derived_type_three_pattern() {
    let (fluree, ledger) = chain_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:Student"},
            {"@id": "?x", "ex:memberOf": "?d"},
            {"@id": "?d", "@type": "ex:Department"}
        ],
        "reasoning": "owl2rl"
    });
    let mut r = run(&fluree, &ledger, &q).await;
    r.sort_by_key(ToString::to_string);
    // ex:g is a derived Student; ex:ug is also a Student? No — ug is Undergraduate,
    // not a subclass of Student here. Only ex:g is a (derived) Student.
    assert_eq!(
        r,
        vec![json!("ex:g")],
        "3-pattern derived-type chain (BUG): {r:?}"
    );
}

// CONTROL: 2-pattern derived-type chain (no 3rd pattern) — known to work post-fix.
#[tokio::test]
async fn chain_derived_type_two_pattern() {
    let (fluree, ledger) = chain_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:Student"},
            {"@id": "?x", "ex:memberOf": "?d"}
        ],
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("ex:g")],
        "2-pattern derived-type chain: {r:?}"
    );
}

/// q11-shaped: join where the matched fact is a DERIVED property fact.
/// subOrgOf transitive: ex:rg0 subOrgOf ex:d0 ; ex:d0 subOrgOf ex:u0
/// => derived ex:rg0 subOrgOf ex:u0. ex:rg0 a ResearchGroup (base).
async fn transitive_fixture() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "reasoning/transitive-repro");
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:subOrgOf", "@type": "owl:TransitiveProperty"},
            {"@id": "ex:rg0", "@type": "ex:ResearchGroup", "ex:subOrgOf": {"@id": "ex:d0"}},
            {"@id": "ex:d0", "@type": "ex:Department", "ex:subOrgOf": {"@id": "ex:u0"}},
            {"@id": "ex:u0", "@type": "ex:University"}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    (fluree, ledger)
}

// CONTROL: the derived transitive edge is visible on its own.
#[tokio::test]
async fn transitive_edge_single_pattern() {
    let (fluree, ledger) = transitive_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": {"@id": "?x", "ex:subOrgOf": {"@id": "ex:u0"}},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    // rg0 (derived, transitively) and d0 (base) are both subOrgOf u0.
    assert!(
        r.contains(&json!("ex:rg0")),
        "derived transitive edge rg0->u0 should be visible: {r:?}"
    );
}

// BUG: join base-type pattern with the DERIVED transitive-property fact.
#[tokio::test]
async fn transitive_join_derived_property() {
    let (fluree, ledger) = transitive_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:ResearchGroup"},
            {"@id": "?x", "ex:subOrgOf": {"@id": "ex:u0"}}
        ],
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("ex:rg0")],
        "join on derived transitive property (BUG): {r:?}"
    );
}

// ----- INDEXED (binary-store) variants of the deeper bug -----

fn chain_data() -> serde_json::Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:GraduateStudent", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Student"}},
            {"@id": "ex:d0", "@type": "ex:Department"},
            {"@id": "ex:ug", "@type": "ex:UndergraduateStudent", "ex:memberOf": {"@id": "ex:d0"}},
            {"@id": "ex:g", "@type": "ex:GraduateStudent", "ex:memberOf": {"@id": "ex:d0"}}
        ]
    })
}

#[tokio::test]
async fn idx_chain_derived_type_three_pattern() {
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:Student"},
            {"@id": "?x", "ex:memberOf": "?d"},
            {"@id": "?d", "@type": "ex:Department"}
        ],
        "reasoning": "owl2rl"
    });
    let r = run_indexed("reasoning/idx-chain", &chain_data(), &q).await;
    assert_eq!(
        r,
        vec![json!("ex:g")],
        "INDEXED 3-pattern derived-type chain: {r:?}"
    );
}

fn transitive_data() -> serde_json::Value {
    json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:subOrgOf", "@type": "owl:TransitiveProperty"},
            {"@id": "ex:rg0", "@type": "ex:ResearchGroup", "ex:subOrgOf": {"@id": "ex:d0"}},
            {"@id": "ex:d0", "@type": "ex:Department", "ex:subOrgOf": {"@id": "ex:u0"}},
            {"@id": "ex:u0", "@type": "ex:University"}
        ]
    })
}

#[tokio::test]
async fn idx_transitive_edge_single_pattern() {
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": {"@id": "?x", "ex:subOrgOf": {"@id": "ex:u0"}},
        "reasoning": "owl2rl"
    });
    let r = run_indexed("reasoning/idx-trans-1", &transitive_data(), &q).await;
    assert!(
        r.contains(&json!("ex:rg0")),
        "INDEXED derived transitive edge: {r:?}"
    );
}

#[tokio::test]
async fn idx_transitive_join_derived_property() {
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:ResearchGroup"},
            {"@id": "?x", "ex:subOrgOf": {"@id": "ex:u0"}}
        ],
        "reasoning": "owl2rl"
    });
    let r = run_indexed("reasoning/idx-trans-2", &transitive_data(), &q).await;
    assert_eq!(
        r,
        vec![json!("ex:rg0")],
        "INDEXED join on derived transitive property: {r:?}"
    );
}

// ===================== ISOLATION EXPERIMENTS =====================

/// Fixture where `ex:g` gets type `ex:Pupil` ONLY by owl:equivalentClass
/// inference (Student ≡ Pupil). Pupil has no subclasses, so the RDFS UNION
/// expansion of `?x a Pupil` does NOT fire — isolating the "derived type
/// joins ref property" path from the subclass-UNION double-match.
async fn equiv_fixture() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "reasoning/equiv-repro");
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:Student", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:Pupil"}},
            {
                "@id": "ex:g",
                "@type": "ex:Student",
                "ex:takesCourse": [{"@id": "ex:c0"}, {"@id": "ex:c65"}],
                "ex:advisor": {"@id": "ex:p1"},
                "ex:name": "GraduateStudent101"
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    (fluree, ledger)
}

// B-iso-1: PURE derived type (no UNION expansion) + TWO-valued ref property.
#[tokio::test]
async fn iso_pure_derived_type_two_valued_ref() {
    let (fluree, ledger) = equiv_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?c",
        "where": {"@id": "ex:g", "@type": "ex:Pupil", "ex:takesCourse": "?c"},
        "reasoning": "owl2rl"
    });
    let mut r = run(&fluree, &ledger, &q).await;
    r.sort_by_key(ToString::to_string);
    assert_eq!(
        r,
        vec![json!("ex:c0"), json!("ex:c65")],
        "pure derived type + 2-valued ref"
    );
}

// B-iso-2: PURE derived type (no UNION) + ONE-valued ref property.
#[tokio::test]
async fn iso_pure_derived_type_one_valued_ref() {
    let (fluree, ledger) = equiv_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?o",
        "where": {"@id": "ex:g", "@type": "ex:Pupil", "ex:advisor": "?o"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(r, vec![json!("ex:p1")], "pure derived type + 1-valued ref");
}

// B-iso-3: PURE derived type (no UNION) + literal property.
#[tokio::test]
async fn iso_pure_derived_type_literal() {
    let (fluree, ledger) = equiv_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?n",
        "where": {"@id": "ex:g", "@type": "ex:Pupil", "ex:name": "?n"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("GraduateStudent101")],
        "pure derived type + literal"
    );
}

/// Fixture asserting `ex:g` as BOTH base types so a manual UNION matches both
/// branches with NO reasoning involved — reproduces the post-RDFS-rewrite shape.
async fn dual_base_type_fixture() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "reasoning/manual-union");
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {
                "@id": "ex:g",
                "@type": ["ex:Student", "ex:GraduateStudent"],
                "ex:takesCourse": [{"@id": "ex:c0"}, {"@id": "ex:c65"}],
                "ex:advisor": {"@id": "ex:p1"},
                "ex:name": "GraduateStudent101"
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    (fluree, ledger)
}

// MANUAL UNION (reasoning OFF) + TWO-valued ref: replicates the rewritten shape.
#[tokio::test]
async fn manual_union_two_valued_ref_no_reasoning() {
    let (fluree, ledger) = dual_base_type_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?c",
        "where": [
            ["union",
                {"@id": "ex:g", "@type": "ex:Student"},
                {"@id": "ex:g", "@type": "ex:GraduateStudent"}],
            {"@id": "ex:g", "ex:takesCourse": "?c"}
        ],
        "reasoning": "none"
    });
    let mut r = run(&fluree, &ledger, &q).await;
    r.sort_by_key(ToString::to_string);
    // bag semantics: 2 union branches × 2 courses = 4 rows
    assert_eq!(
        r,
        vec![
            json!("ex:c0"),
            json!("ex:c0"),
            json!("ex:c65"),
            json!("ex:c65")
        ],
        "manual union + 2-valued ref, NO reasoning"
    );
}

// PURE CROSS PRODUCT (no union, no reasoning): two constant-subject patterns,
// no shared variable, the second multi-valued. Does the cross product collapse?
#[tokio::test]
async fn cross_product_second_multivalued_no_union() {
    let (fluree, ledger) = dual_base_type_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?n", "?c"],
        "where": [
            {"@id": "ex:g", "ex:name": "?n"},
            {"@id": "ex:g", "ex:takesCourse": "?c"}
        ],
        "reasoning": "none"
    });
    let r = run(&fluree, &ledger, &q).await;
    // 1 name × 2 courses = 2 rows
    assert_eq!(
        r.len(),
        2,
        "cross product name×takesCourse, NO union: got {r:?}"
    );
}

// UNIT pattern (constant S,P,O -> binds no variable) × multi-valued, NO union.
#[tokio::test]
async fn unit_pattern_times_multivalued_no_union() {
    let (fluree, ledger) = dual_base_type_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?c",
        "where": [
            {"@id": "ex:g", "@type": "ex:Student"},
            {"@id": "ex:g", "ex:takesCourse": "?c"}
        ],
        "reasoning": "none"
    });
    let mut r = run(&fluree, &ledger, &q).await;
    r.sort_by_key(ToString::to_string);
    assert_eq!(
        r,
        vec![json!("ex:c0"), json!("ex:c65")],
        "unit pattern × multivalued, NO union: {r:?}"
    );
}

// SHARED-variable join (the common path) — sanity that normal joins are fine.
#[tokio::test]
async fn shared_var_join_multivalued_sanity() {
    let (fluree, ledger) = dual_base_type_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "selectDistinct": "?x",
        "where": [
            {"@id": "?x", "@type": "ex:Student"},
            {"@id": "?x", "ex:takesCourse": "?c"}
        ],
        "reasoning": "none"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert!(r.contains(&json!("ex:g")), "shared-var join sanity: {r:?}");
}

// Entity that is BOTH base-asserted AND inferred for the same type must not be
// double-counted by the materialized overlay (derived fact == base fact).
#[tokio::test]
async fn base_and_derived_same_type_not_doubled() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "reasoning/base-plus-derived");
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:GraduateStudent", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Student"}},
            // ex:e is BOTH directly typed Student (base) AND a GraduateStudent (=> inferred Student).
            {"@id": "ex:e", "@type": ["ex:Student", "ex:GraduateStudent"]}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "?x", "@type": "ex:Student"},
        "reasoning": "owl2rl"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("ex:e")],
        "base+derived same type must not double: {r:?}"
    );
}

// MANUAL UNION (reasoning OFF) + ONE-valued ref.
#[tokio::test]
async fn manual_union_one_valued_ref_no_reasoning() {
    let (fluree, ledger) = dual_base_type_fixture().await;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?o",
        "where": [
            ["union",
                {"@id": "ex:g", "@type": "ex:Student"},
                {"@id": "ex:g", "@type": "ex:GraduateStudent"}],
            {"@id": "ex:g", "ex:advisor": "?o"}
        ],
        "reasoning": "none"
    });
    let r = run(&fluree, &ledger, &q).await;
    assert_eq!(
        r,
        vec![json!("ex:p1"), json!("ex:p1")],
        "manual union + 1-valued ref, NO reasoning"
    );
}
