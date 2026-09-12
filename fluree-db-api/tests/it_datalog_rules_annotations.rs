//! Datalog rules that read edge annotations (claims), and the loud-rejection
//! contract of the rules engine.
//!
//! Scenario: two sources make independently identified claims about the same
//! fact — `alice knows bob` with confidence 0.8 from source A and 0.9 from
//! source B — and a rule derives `trustedKnows` from claims above a threshold.
//! Before the executor-backed engine, every one of these rule shapes silently
//! derived nothing (JSON-LD `@annotation` parsed as a bogus predicate; the
//! SPARQL forms were rejected in a log line the caller never saw).

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Four claims over three edges: alice→bob (0.8 from A, 0.9 from B),
/// bob→carol (0.5 from A), carol→dave (0.95 from B).
async fn claims_ledger(fluree: &MemoryFluree, name: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, name);
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob",
                "@annotation": {"@id": "ex:claim1", "ex:confidence": 0.8, "ex:source": {"@id": "ex:sourceA"}}}},
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob",
                "@annotation": {"@id": "ex:claim2", "ex:confidence": 0.9, "ex:source": {"@id": "ex:sourceB"}}}},
            {"@id": "ex:bob", "ex:knows": {"@id": "ex:carol",
                "@annotation": {"@id": "ex:claim3", "ex:confidence": 0.5, "ex:source": {"@id": "ex:sourceA"}}}},
            {"@id": "ex:carol", "ex:knows": {"@id": "ex:dave",
                "@annotation": {"@id": "ex:claim4", "ex:confidence": 0.95, "ex:source": {"@id": "ex:sourceB"}}}}
        ]
    });
    fluree.insert(ledger0, &data).await.unwrap().ledger
}

async fn trusted_pairs(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    rules: serde_json::Value,
) -> Vec<serde_json::Value> {
    let q = json!({
        "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
        "select": ["?a", "?b"],
        "where": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}},
        "reasoning": "datalog",
        "rules": rules
    });
    let rows = support::query_jsonld(fluree, ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let mut results = normalize_rows(&rows);
    results.sort_by_key(ToString::to_string);
    results
}

fn expected_trusted() -> Vec<serde_json::Value> {
    // claim2 (0.9) and claim4 (0.95) clear 0.85; claim1 (0.8) and claim3 (0.5) do not.
    let mut v = vec![
        json!(["ex:alice", "ex:bob"]),
        json!(["ex:carol", "ex:dave"]),
    ];
    v.sort_by_key(ToString::to_string);
    v
}

// =============================================================================
// Rule bodies that read claims
// =============================================================================

#[tokio::test]
async fn jsonld_rule_reads_inline_annotation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/inline").await;
    let rules = json!([{
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"@id": "?a", "ex:knows": {"@id": "?b", "@annotation": {"ex:confidence": "?c"}}},
            ["filter", "(> ?c 0.85)"]
        ],
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn jsonld_rule_reads_claim_first_via_reifies() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/reifies").await;
    // Claim-first: start from the claim's metadata and walk back to the edge.
    let rules = json!([{
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"ex:confidence": "?c", "ex:source": {"@id": "ex:sourceB"},
             "@reifies": {"@id": "?a", "ex:knows": {"@id": "?b"}}},
            ["filter", "(> ?c 0.85)"]
        ],
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn sparql_rule_reads_annotation_tail() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/sparql-tail").await;
    let rules = json!([{
        "@type": "f:sparql",
        "@value": "PREFIX ex: <http://example.org/> \
                   CONSTRUCT { ?a ex:trustedKnows ?b } \
                   WHERE { ?a ex:knows ?b {| ex:confidence ?c |} FILTER(?c > 0.85) }"
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn sparql_rule_joins_the_reifier_as_a_subject() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/sparql-reifier-var").await;
    // Bind the reifier and read its properties as ordinary triples.
    let rules = json!([{
        "@type": "f:sparql",
        "@value": "PREFIX ex: <http://example.org/> \
                   CONSTRUCT { ?a ex:trustedKnows ?b } \
                   WHERE { ?a ex:knows ?b ~ ?claim . ?claim ex:confidence ?c ; ex:source ?src \
                           FILTER(?c > 0.85 && ?src = ex:sourceB) }"
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn sparql_rule_reads_rdf_reifies_triple_term() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/rdf-reifies").await;
    let rules = json!([{
        "@type": "f:sparql",
        "@value": "PREFIX ex: <http://example.org/> \
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                   CONSTRUCT { ?a ex:trustedKnows ?b } \
                   WHERE { ?claim rdf:reifies <<( ?a ex:knows ?b )>> ; ex:confidence ?c \
                           FILTER(?c > 0.85) }"
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn stored_rule_reads_annotations_and_is_walkable_by_property_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/stored").await;
    let rule_data = json!({
        "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
        "@id": "ex:trustedRule",
        "f:rule": {
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?a ex:trustedKnows ?b } \
                       WHERE { ?a ex:knows ?b {| ex:confidence ?c |} FILTER(?c > 0.85) }"
        }
    });
    let ledger = fluree.insert(ledger, &rule_data).await.unwrap().ledger;

    // The stored rule applies with only `reasoning: datalog`.
    assert_eq!(
        trusted_pairs(&fluree, &ledger, json!([])).await,
        expected_trusted()
    );

    // A derived predicate is walkable with a property path (the derived
    // overlay feeds path evaluation; pinned here because nothing else does).
    // Add a trusted claim on bob→carol so alice reaches dave in three trusted hops.
    let more = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:bob", "ex:knows": {"@id": "ex:carol",
            "@annotation": {"@id": "ex:claim5", "ex:confidence": 0.99, "ex:source": {"@id": "ex:sourceB"}}}
    });
    let ledger = fluree.insert(ledger, &more).await.unwrap().ledger;
    let q = json!({
        "@context": {"ex": "http://example.org/", "trusted+": {"@path": ["+", "ex:trustedKnows"]}},
        "select": "?x",
        "where": {"@id": "ex:alice", "trusted+": {"@id": "?x"}},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let mut reach = normalize_rows(&rows);
    reach.sort_by_key(ToString::to_string);
    assert_eq!(
        reach,
        vec![json!("ex:bob"), json!("ex:carol"), json!("ex:dave")],
        "the derived predicate must be walkable transitively"
    );
}

#[tokio::test]
async fn query_time_rule_body_may_use_union_bind_and_full_filters() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/rich-body").await;
    // UNION over the two trusted sources, BIND of a derived score, and a
    // compound filter — none of which the old restricted rule language had.
    let rules = json!([{
        "@type": "f:sparql",
        "@value": "PREFIX ex: <http://example.org/> \
                   CONSTRUCT { ?a ex:trustedKnows ?b } \
                   WHERE { { ?a ex:knows ?b {| ex:confidence ?c ; ex:source ex:sourceB |} } \
                           UNION { ?a ex:knows ?b {| ex:confidence ?c ; ex:source ex:sourceA |} } \
                           BIND(?c * 100 AS ?pct) FILTER(?pct > 85 && ?pct <= 100) }"
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

// =============================================================================
// Loud rejection
// =============================================================================

// =============================================================================
// JSON-LD classification (#1558): the body goes through the standard parser
// =============================================================================

#[tokio::test]
async fn jsonld_rule_expands_bare_context_mapped_keys_and_types() {
    // A bare term (`knows`) and a bare class (`Person`) defined directly in the
    // rule's @context — plain JSON-LD convention — must resolve exactly like
    // their prefixed spellings. The old rule parser encoded them literally and
    // silently matched nothing.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "rules/annotations/bare-terms");
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "@type": "ex:Robot", "ex:knows": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    let rules = json!([{
        "@context": {
            "ex": "http://example.org/",
            "knows": "http://example.org/knows",
            "Person": "http://example.org/Person"
        },
        "where": {"@id": "?a", "@type": "Person", "knows": {"@id": "?b"}},
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        vec![json!(["ex:alice", "ex:bob"])],
        "only the Person's edge derives"
    );
}

#[tokio::test]
async fn jsonld_rule_with_a_stray_keyword_key_is_rejected_not_fabricated() {
    // `@reverse` is a JSON-LD keyword, not a predicate; the old parser minted
    // a predicate literally named "@reverse" and the rule matched nothing.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/annotations/keyword-key").await;
    let rules = json!([{
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?a", "@reverse": {"ex:knows": {"@id": "?b"}}},
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    }]);
    let message = rejection(&fluree, &ledger, rules).await;
    assert!(
        message.contains("@reverse"),
        "unexpected rejection message: {message}"
    );
}

async fn rejection(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    rules: serde_json::Value,
) -> String {
    let q = json!({
        "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
        "select": ["?a", "?b"],
        "where": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}},
        "reasoning": "datalog",
        "rules": rules
    });
    support::query_jsonld(fluree, ledger, &q)
        .await
        .expect_err("the rule must be rejected and the query must fail")
        .to_string()
}

#[tokio::test]
async fn optional_in_a_jsonld_rule_body_is_rejected_by_name() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/optional").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?a", "ex:knows": {"@id": "?b"}},
                ["optional", {"@id": "?a", "ex:name": "?n"}]
            ],
            "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
        }]),
    )
    .await;
    assert!(
        message.contains("OPTIONAL"),
        "must name the construct: {message}"
    );
}

#[tokio::test]
async fn not_exists_in_a_rule_body_is_rejected_not_ignored() {
    // Issue #1786: `["not-exists", …]` used to be silently ignored, so the
    // rule derived as if the negation were not there. Now it is rejected.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/not-exists").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?a", "ex:knows": {"@id": "?b"}},
                ["not-exists", {"@id": "?a", "ex:blocked": {"@id": "?b"}}]
            ],
            "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
        }]),
    )
    .await;
    assert!(
        message.contains("NOT EXISTS"),
        "must name the construct: {message}"
    );
}

#[tokio::test]
async fn annotation_in_a_rule_head_is_rejected_by_name() {
    // Reifier minting in heads is a follow-up (docs/design/rules-engine.md);
    // until then the gap is loud rather than silently deriving nothing.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/head-annotation").await;
    let message = rejection(&fluree, &ledger, json!([{
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?a", "ex:knows": {"@id": "?b", "@annotation": {"@id": "?claim"}}},
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b", "@annotation": {"ex:derivedFrom": {"@id": "?claim"}}}}
    }])).await;
    assert!(
        message.contains("@annotation"),
        "must name the construct: {message}"
    );
}

#[tokio::test]
async fn sparql_rule_with_minus_is_rejected_by_name() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/minus").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?a ex:trustedKnows ?b } \
                       WHERE { ?a ex:knows ?b MINUS { ?a ex:blocked ?b } }"
        }]),
    )
    .await;
    assert!(
        message.contains("MINUS"),
        "must name the construct: {message}"
    );
}

// =============================================================================
// Cache and budget
// =============================================================================

#[tokio::test]
async fn datalog_materialization_is_cached_across_identical_queries() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/cache").await;
    let rules = json!([{
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"@id": "?a", "ex:knows": {"@id": "?b", "@annotation": {"ex:confidence": "?c"}}},
            ["filter", "(> ?c 0.85)"]
        ],
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    }]);
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?b"],
        "where": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}},
        "reasoning": "datalog",
        "rules": rules
    });
    let first = support::query_jsonld_tracked(&fluree, &ledger, &q)
        .await
        .unwrap();
    let second = support::query_jsonld_tracked(&fluree, &ledger, &q)
        .await
        .unwrap();
    let first_tally = first.reasoning.expect("first run reports reasoning");
    let second_tally = second.reasoning.expect("second run reports reasoning");
    assert_eq!(first_tally.derived_facts, 2);
    assert_eq!(
        second_tally.derived_facts, first_tally.derived_facts,
        "a cache hit must report the cached materialization's tally"
    );
    assert!(
        !fluree_db_query::reasoning::global_reasoning_cache().is_empty(),
        "the datalog materialization must be inserted into the reasoning cache"
    );
}

#[tokio::test]
async fn fact_budget_bounds_a_single_round() {
    // 40 nodes in a line, `knows` transitive through a recursive rule: the
    // closure is 780 facts. A cap of 50 must stop INSIDE the round that
    // crosses it, not after the round has derived everything.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "rules/budget");
    let nodes: Vec<serde_json::Value> = (0..40)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:knows": {"@id": format!("ex:n{}", i + 1)}}))
        .collect();
    let data = json!({"@context": {"ex": "http://example.org/"}, "@graph": nodes});
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?b"],
        "where": {"@id": "?a", "ex:reach": {"@id": "?b"}},
        "reasoning": "datalog",
        "reasoningBudget": {"maxFacts": 50, "maxSeconds": 30},
        "rules": [
            {"@context": {"ex": "http://example.org/"},
             "where": {"@id": "?a", "ex:knows": {"@id": "?b"}},
             "insert": {"@id": "?a", "ex:reach": {"@id": "?b"}}},
            {"@context": {"ex": "http://example.org/"},
             "where": {"@id": "?a", "ex:reach": {"ex:knows": "?c"}},
             "insert": {"@id": "?a", "ex:reach": {"@id": "?c"}}}
        ]
    });
    let tracked = support::query_jsonld_tracked(&fluree, &ledger, &q)
        .await
        .unwrap();
    let tally = tracked.reasoning.expect("reasoning tally");
    assert!(tally.capped, "the budget must cap the closure: {tally:?}");
    assert_eq!(tally.capped_reason.as_deref(), Some("facts"));
    assert!(
        tally.derived_facts <= 51,
        "the fact cap must hold within a round, got {} derived facts",
        tally.derived_facts
    );
}

// =============================================================================
// IRI operands in JSON-LD filters (fixed at the query level too)
// =============================================================================

#[tokio::test]
async fn jsonld_query_filter_compares_iris_by_identity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/query-iri-filter").await;
    // `(= ?p ex:knows)` used to compare against the string "ex:knows" and
    // match nothing; `(!= ?p ex:knows)` kept every row.
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?b"],
        "where": [
            {"@id": "?a", "?p": {"@id": "?b"}},
            ["filter", "(= ?p ex:knows)"]
        ]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let mut results = normalize_rows(&rows);
    results.sort_by_key(ToString::to_string);
    let mut expected = vec![
        json!(["ex:alice", "ex:bob"]),
        json!(["ex:bob", "ex:carol"]),
        json!(["ex:carol", "ex:dave"]),
    ];
    expected.sort_by_key(ToString::to_string);
    assert_eq!(results, expected);

    let q_ne = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?p", "?b"],
        "where": [
            {"@id": "?a", "?p": {"@id": "?b"}},
            ["filter", "(!= ?p ex:knows)"]
        ]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q_ne)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);
    assert!(
        results.iter().all(|r| r[1] != json!("ex:knows")),
        "`!=` against an IRI must exclude that predicate, got {results:?}"
    );
    assert!(
        results.iter().any(|r| r[1] == json!("ex:source")),
        "`!=` against an IRI must keep the other predicates, got {results:?}"
    );
}
