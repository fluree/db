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
async fn bang_exists_is_rejected_like_not_exists() {
    // #1786's defect class has two spellings. `NOT EXISTS { … }` lowers to
    // `Exists { negated: true }`; `!EXISTS { … }` lowers to `Not` wrapping a
    // plain `Exists`. The first cut of the monotonicity walk matched only the
    // first, so negation reached the fixpoint through the second — inside the
    // change that closes #1786. In a fixpoint that is unsound, not merely
    // unsupported: if the head predicate appears in the negated pattern, the
    // answer depends on which round evaluated it, and is then cached.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/bang-exists").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?a ex:trustedKnows ?b } \
                       WHERE { ?a ex:knows ?b . FILTER(!EXISTS { ?b ex:knows ?c }) }"
        }]),
    )
    .await;
    assert!(
        message.contains("NOT EXISTS"),
        "unexpected rejection message: {message}"
    );
}

#[tokio::test]
async fn typed_literals_in_a_rule_head_are_coerced_to_their_datatype() {
    // A head used to write the value exactly as spelled, tagged with a
    // datatype it did not match: `{"@value": "2024-01-01", "@type": "xsd:date"}`
    // derived a string labelled `xsd:date`. `DATATYPE()` said date while
    // `YEAR()` was unbound — the same value asserted through a transaction
    // coerces, so the derived fact did not even dedup against it. `YEAR()` is
    // the sharpest probe: it binds only for a real date.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/head-typed-literal").await;
    let q = json!({
        "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
        "select": ["?y"],
        "where": [
            {"@id": "ex:alice", "ex:joined": "?d"},
            ["bind", "?y", "(year ?d)"]
        ],
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
            "where": {"@id": "?a", "ex:knows": {"@id": "?b"}},
            "insert": {"@id": "?a", "ex:joined": {"@value": "2024-01-01", "@type": "xsd:date"}}
        }]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("query over the derived date")
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let cell = rows[0].as_array().map_or(&rows[0], |r| &r[0]);
    assert_eq!(
        cell.as_i64(),
        Some(2024),
        "YEAR() must bind on the derived date, which it only does for a real \
         xsd:date rather than a string wearing its label: {rows}"
    );
}

#[tokio::test]
async fn a_head_literal_its_datatype_rejects_fails_the_rule() {
    // Coercion is also a validity check: a value the declared datatype cannot
    // accept would otherwise be stored mislabelled.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/head-bad-literal").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
            "where": {"@id": "?a", "ex:knows": {"@id": "?b"}},
            "insert": {"@id": "?a", "ex:joined": {"@value": "not-a-date", "@type": "xsd:date"}}
        }]),
    )
    .await;
    assert!(
        message.contains("date"),
        "unexpected rejection message: {message}"
    );
}

#[tokio::test]
async fn exists_outside_a_positive_position_is_rejected() {
    // Counting `Not` wrappers is not enough: a truth value can be negated by
    // anything that reads it. Each of these derived facts under a parity-only
    // check, and each is negation — the first by comparison, the second by a
    // branch, the third by carrying the value into a variable and negating it
    // somewhere the walk cannot follow.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/exists-positions").await;
    for (label, body) in [
        (
            "compared against a boolean",
            "?a ex:knows ?b . FILTER(EXISTS { ?b ex:knows ?c } = false)",
        ),
        (
            "an IF branch",
            "?a ex:knows ?b . FILTER(IF(EXISTS { ?b ex:knows ?c }, false, true))",
        ),
        (
            "bound to a variable, negated later",
            "?a ex:knows ?b . BIND(EXISTS { ?b ex:knows ?c } AS ?e) FILTER(!?e)",
        ),
    ] {
        let message = rejection(
            &fluree,
            &ledger,
            json!([{
                "@type": "f:sparql",
                "@value": format!(
                    "PREFIX ex: <http://example.org/> \
                     CONSTRUCT {{ ?a ex:trustedKnows ?b }} WHERE {{ {body} }}"
                )
            }]),
        )
        .await;
        assert!(
            message.contains("EXISTS"),
            "{label}: unexpected rejection message: {message}"
        );
    }
}

#[tokio::test]
async fn exists_under_and_or_stays_allowed() {
    // The rule is positional, not a ban: AND and OR pass a truth value
    // through unchanged, so an EXISTS under them is still monotone and must
    // still run. Without this, the rejection tests above would also pass
    // against a blanket ban on EXISTS.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/allow/exists-under-and").await;
    let rules = json!([{
        "@type": "f:sparql",
        "@value": "PREFIX ex: <http://example.org/> \
                   CONSTRUCT { ?a ex:trustedKnows ?b } \
                   WHERE { ?a ex:knows ?b {| ex:confidence ?c |} \
                           FILTER(?c > 0.85 && EXISTS { ?a ex:knows ?b }) }"
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn double_negated_exists_stays_allowed() {
    // The check tracks the PARITY of the `Not` wrappers rather than banning
    // `Exists` outright: `!!EXISTS` is monotone, so it must still run. A
    // blanket ban would pass the test above while quietly removing a legal
    // construct.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/allow/double-negated-exists").await;
    let rules = json!([{
        "@type": "f:sparql",
        "@value": "PREFIX ex: <http://example.org/> \
                   CONSTRUCT { ?a ex:trustedKnows ?b } \
                   WHERE { ?a ex:knows ?b {| ex:confidence ?c |} \
                           FILTER(?c > 0.85 && !(!EXISTS { ?a ex:knows ?b })) }"
    }]);
    assert_eq!(
        trusted_pairs(&fluree, &ledger, rules).await,
        expected_trusted()
    );
}

#[tokio::test]
async fn subquery_aggregate_in_a_rule_body_is_rejected() {
    // The docs promise "subqueries without aggregates"; the walk recursed into
    // a subquery's patterns but never looked at its own GROUP BY, so an
    // aggregating sub-SELECT computed counts inside the fixpoint.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/subquery-group-by").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?a ex:trustedKnows ?b } \
                       WHERE { { SELECT ?a (COUNT(?b) AS ?n) WHERE { ?a ex:knows ?b } \
                                 GROUP BY ?a } ?a ex:knows ?b }"
        }]),
    )
    .await;
    assert!(
        message.contains("GROUP BY") && message.contains("subquery"),
        "unexpected rejection message: {message}"
    );
}

#[tokio::test]
async fn subquery_limit_in_a_rule_body_is_rejected() {
    // Worse than unsupported: which row survives a LIMIT depends on what has
    // been derived so far, so the rule's output changes with round order.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/subquery-limit").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?a ex:trustedKnows ?b } \
                       WHERE { { SELECT ?a ?b WHERE { ?a ex:knows ?b } LIMIT 1 } }"
        }]),
    )
    .await;
    assert!(
        message.contains("LIMIT") && message.contains("subquery"),
        "unexpected rejection message: {message}"
    );
}

#[tokio::test]
async fn body_level_limit_is_rejected_rather_than_silently_cleared() {
    // `build_rule` clears the rule body's own modifiers before execution, so
    // without a check a LIMIT would run as though it had not been written.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/body-limit").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@type": "f:sparql",
            "@value": "PREFIX ex: <http://example.org/> \
                       CONSTRUCT { ?a ex:trustedKnows ?b } \
                       WHERE { ?a ex:knows ?b } LIMIT 1"
        }]),
    )
    .await;
    assert!(
        message.contains("LIMIT"),
        "unexpected rejection message: {message}"
    );
}

#[tokio::test]
async fn head_var_bound_only_in_a_filter_is_rejected() {
    // Range restriction asks which variables a matched row BINDS. It was
    // reading `referenced_vars`, which includes filter operands by design, so
    // a head variable the body only mentions inside a FILTER passed the check
    // and then derived nothing at runtime, silently.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/reject/filter-only-head-var").await;
    let message = rejection(
        &fluree,
        &ledger,
        json!([{
            "@context": {"ex": "http://example.org/"},
            "where": [
                {"@id": "?a", "ex:knows": {"@id": "?b"}},
                ["filter", "(> ?x 0)"]
            ],
            "insert": {"@id": "?a", "ex:trustedKnows": "?x"}
        }]),
    )
    .await;
    assert!(
        message.contains("?x"),
        "unexpected rejection message: {message}"
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
    // The hit counter is what makes this test discriminate. Comparing two
    // runs' tallies does not: a recompute on a four-triple ledger reports an
    // identical tally. Asserting the cache is non-empty does not either: under
    // plain `cargo test` any other test in the binary satisfies that. Entry
    // counts would race — the cache is process-wide and tests run in
    // parallel — but a hit is attributable, because this ledger's name makes
    // its key unique to this test.
    let cache = fluree_db_query::reasoning::global_reasoning_cache();
    let hits_before = cache.hits();

    let first = support::query_jsonld_tracked(&fluree, &ledger, &q)
        .await
        .unwrap();
    let first_tally = first.reasoning.expect("first run reports reasoning");
    assert_eq!(first_tally.derived_facts, 2);
    assert_eq!(
        cache.hits(),
        hits_before,
        "the first run has nothing to hit"
    );

    let second = support::query_jsonld_tracked(&fluree, &ledger, &q)
        .await
        .unwrap();
    let second_tally = second.reasoning.expect("second run reports reasoning");
    assert_eq!(
        second_tally.derived_facts, first_tally.derived_facts,
        "a cache hit must report the cached materialization's tally"
    );
    assert_eq!(
        cache.hits(),
        hits_before + 1,
        "an identical query must HIT the entry the first run inserted"
    );

    // A changed rule must miss: the key folds a content hash of the rule set,
    // so editing a rule cannot serve the previous materialization.
    let mut wider = q.clone();
    // 0.4 rather than 0.5: it has to admit a claim on a THIRD edge
    // (bob->carol at 0.5). Admitting claim1 (0.8) alone would derive the
    // alice->bob pair a second time and dedup back to the same tally, which
    // would not distinguish a re-materialization from a hit.
    wider["rules"][0]["where"][1] = json!(["filter", "(> ?c 0.4)"]);
    let third = support::query_jsonld_tracked(&fluree, &ledger, &wider)
        .await
        .unwrap();
    let third_tally = third.reasoning.expect("third run reports reasoning");
    assert_eq!(
        third_tally.derived_facts, 3,
        "the widened filter must re-materialize, not reuse the cached closure"
    );
    assert_eq!(
        cache.hits(),
        hits_before + 1,
        "a changed rule must MISS rather than serve the previous closure"
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

#[tokio::test]
async fn unquoted_atoms_are_iris_only_where_terms_are_compared() {
    // An unquoted atom means an IRI in exactly one place: as the operand of an
    // RDF-term identity comparison. Letting the classifier reach every
    // argument position broke string functions, `(count *)` in HAVING, and —
    // worst — changed what a `bind` WRITES. Each row below is a position the
    // classifier must NOT claim.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/atom-positions").await;

    // String functions take strings. `ex:knows` as a string prefix of the
    // predicate's IRI text would match nothing if it were lowered to an IRI,
    // and the comparison would be a type error rather than a false.
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": [
            {"@id": "?s", "?p": {"@id": "?o"}},
            ["filter", "(strStarts (str ?p) http://example.org/kn)"]
        ]
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("an unquoted URL inside strStarts stays a string")
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows.as_array().map(std::vec::Vec::len).unwrap_or(0),
        3,
        "strStarts must still match on the IRI's text: {rows}"
    );

    // `(count *)` in HAVING: `*` is an aggregate marker, not a term.
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s", "(as (count ?o) ?n)"],
        "where": {"@id": "?s", "ex:knows": {"@id": "?o"}},
        "groupBy": ["?s"],
        "having": "(> (count *) 0)"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("(count *) in HAVING must still parse")
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        rows.as_array().map(std::vec::Vec::len).unwrap_or(0) > 0,
        "HAVING (count *) must keep its groups: {rows}"
    );

    // And the position that decides what a transaction WRITES: a bare URL
    // bound by `bind` is a literal, as it was before 4.2, not a ref.
    let written = fluree
        .insert(
            genesis_ledger(&fluree, "rules/atom-positions-bind"),
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:seed": 1}),
        )
        .await
        .expect("seed");
    let updated = fluree
        .update(
            written.ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "where": [
                    {"@id": "?s", "ex:seed": "?v"},
                    ["bind", "?link", "http://example.org/home"]
                ],
                "insert": {"@id": "?s", "ex:link": "?link"}
            }),
        )
        .await
        .expect("bind update");
    let rows = support::query_jsonld(
        &fluree,
        &updated.ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?l"],
            "where": {"@id": "ex:a", "ex:link": "?l"}
        }),
    )
    .await
    .expect("read back")
    .to_jsonld(&updated.ledger.snapshot)
    .unwrap();
    let cell = rows[0].as_array().map_or(&rows[0], |r| &r[0]);
    assert_eq!(
        cell.as_str(),
        Some("http://example.org/home"),
        "a bound bare URL must be written as a literal, not a ref: {rows}"
    );
}

#[tokio::test]
async fn filter_iri_operand_forms_across_spellings() {
    // What an unquoted atom means in a filter, pinned across every spelling,
    // because the answer differs by form and the difference is silent.
    //
    // The s-expression form has quoting, so it can tell an IRI operand from a
    // string: a prefixed name resolves through the query's `@context`, a bare
    // absolute URL is an IRI, and a quoted value is a string. The array form
    // has no such syntax — every element is a JSON string — so a bare atom is
    // always a string there, and `iri(…)` is the way to mean the IRI. Note
    // `iri(…)` does NOT expand a prefix (SPARQL's `IRI()` resolves against the
    // base, not the prefix map), so the array form needs the full IRI.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = claims_ledger(&fluree, "rules/filter-forms").await;

    let matching = 3; // alice->bob, bob->carol, carol->dave
    for (label, filter, expected) in [
        (
            "s-expr prefixed name",
            json!(["filter", "(= ?p ex:knows)"]),
            matching,
        ),
        (
            "s-expr absolute URL",
            json!(["filter", "(= ?p http://example.org/knows)"]),
            matching,
        ),
        (
            "s-expr quoted string",
            json!(["filter", "(= ?p \"ex:knows\")"]),
            0,
        ),
        (
            "array bare atom",
            json!(["filter", ["=", "?p", "ex:knows"]]),
            0,
        ),
        (
            "array iri() with a prefixed name (not expanded)",
            json!(["filter", ["=", "?p", ["iri", "ex:knows"]]]),
            0,
        ),
        (
            "array iri() with an absolute URL",
            json!(["filter", ["=", "?p", ["iri", "http://example.org/knows"]]]),
            matching,
        ),
    ] {
        let q = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?s"],
            "where": [{"@id": "?s", "?p": {"@id": "?o"}}, filter]
        });
        let rows = support::query_jsonld(&fluree, &ledger, &q)
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"))
            .to_jsonld(&ledger.snapshot)
            .unwrap()
            .as_array()
            .map(std::vec::Vec::len)
            .unwrap_or(0);
        assert_eq!(rows, expected, "{label}");
    }
}
