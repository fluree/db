//! Property path integration tests using `@path` context aliases.
//!
//! Tests the `@path` mechanism for defining property path aliases in `@context`
//! and using them in WHERE node-maps. Both string form (SPARQL syntax) and
//! array form (S-expression) are tested.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_knows_chain(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:b","ex:knows":[{"@id":"ex:c"},{"@id":"ex:d"}]},
            {"@id":"ex:d","ex:knows":{"@id":"ex:e"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

/// Seed data for property chain (/) tests.
///
/// Graph:
///   ex:alice --ex:friend--> ex:bob
///   ex:bob   --ex:name--> "Bob"
///   ex:bob   --ex:friend--> ex:carol
///   ex:carol --ex:name--> "Carol"
///   ex:carol --ex:address--> ex:addr1
///   ex:addr1 --ex:city--> "Springfield"
///   ex:bob   --ex:parent--> ex:alice  (inverse: alice is bob's parent)
async fn seed_chain_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:alice","ex:friend":{"@id":"ex:bob"}},
            {"@id":"ex:bob","ex:name":"Bob","ex:friend":{"@id":"ex:carol"},"ex:parent":{"@id":"ex:alice"}},
            {"@id":"ex:carol","ex:name":"Carol","ex:address":{"@id":"ex:addr1"}},
            {"@id":"ex:addr1","ex:city":"Springfield"}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

async fn seed_y_chain(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:y":[{"@id":"ex:b"},{"@id":"ex:g"}]},
            {"@id":"ex:b","ex:y":{"@id":"ex:c"}},
            {"@id":"ex:c","ex:y":{"@id":"ex:d"}},
            {"@id":"ex:d","ex:y":{"@id":"ex:e"}},
            {"@id":"ex:e","ex:y":{"@id":"ex:f"}},
            {"@id":"ex:g","ex:y":[{"@id":"ex:h"},{"@id":"ex:j"}]},
            {"@id":"ex:h","ex:y":{"@id":"ex:i"}},
            {"@id":"ex:j","ex:y":{"@id":"ex:k"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

#[tokio::test]
async fn property_path_one_or_more_no_vars_matches_transitively() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_y_chain(&fluree, "property/path-oneplus-no-vars:main").await;

    // Sanity: ensure the chain exists in the DB.
    let sanity = json!({
        "@context": {"ex":"http://example.org/"},
        "select": ["?o"],
        "where": {"@id":"ex:e","ex:y":"?o"}
    });
    let sanity_rows = support::query_jsonld(&fluree, &ledger, &sanity)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(sanity_rows, json!([["ex:f"]]));

    // Property path traversal from ex:a should reach ex:f.
    let sanity_path = json!({
        "@context": {
            "ex": "http://example.org/",
            "yPlus": {"@path": "ex:y+"}
        },
        "select": ["?o"],
        "where": [{"@id":"ex:a","yPlus":"?o"}]
    });
    let sanity_path_rows = support::query_jsonld(&fluree, &ledger, &sanity_path)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(normalize_rows(&sanity_path_rows).contains(&json!(["ex:f"])));

    // Use VALUES to seed a single solution row and treat the (non-)transitive pattern
    // as a filter, so we can assert reachability without relying on expansion output.
    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "values": [["?dummy"], [[1]]],
        "where": [{"@id":"ex:a","ex:y":{"@id":"ex:f"}}],
        "select": ["?dummy"]
    });
    let r_non = support::query_jsonld(&fluree, &ledger, &q_non)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(r_non, json!([]));

    let q_plus = json!({
        "@context": {
            "ex": "http://example.org/",
            "yPlus": {"@path": "ex:y+"}
        },
        "values": [["?dummy"], [[1]]],
        "where": [{"@id":"ex:a","yPlus":{"@id":"ex:f"}}],
        "select": ["?dummy"]
    });
    let r_plus = support::query_jsonld(&fluree, &ledger, &q_plus)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(r_plus, json!([[1]]));
}

#[tokio::test]
async fn property_path_one_or_more_object_var_with_and_without_cycle() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-oneplus-o:main").await;

    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"ex:a","ex:knows":"?who"}],
        "select": ["?who"]
    });
    let non = support::query_jsonld(&fluree, &ledger1, &q_non)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(non, json!([["ex:b"]]));

    // Use @path with string form
    let q_plus = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsPlus": {"@path": "ex:knows+"}
        },
        "where": [{"@id":"ex:a","knowsPlus":"?who"}],
        "select": ["?who"]
    });
    let plus = support::query_jsonld(&fluree, &ledger1, &q_plus)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&plus),
        normalize_rows(&json!([["ex:b"], ["ex:c"], ["ex:d"], ["ex:e"]]))
    );

    // Add cycle: e knows a. One-or-more from a should now include a as reachable.
    let tx_cycle = json!({"@context":{"ex":"http://example.org/"},"insert":{"@id":"ex:e","ex:knows":{"@id":"ex:a"}}});
    let ledger2 = fluree.update(ledger1, &tx_cycle).await.unwrap().ledger;

    let q_plus2 = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsPlus": {"@path": "ex:knows+"}
        },
        "where": [{"@id":"ex:a","knowsPlus":"?who"}],
        "select": ["?who"]
    });
    let plus2 = support::query_jsonld(&fluree, &ledger2, &q_plus2)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&plus2),
        normalize_rows(&json!([["ex:a"], ["ex:b"], ["ex:c"], ["ex:d"], ["ex:e"]]))
    );
}

#[tokio::test]
async fn property_path_one_or_more_subject_var_with_and_without_cycle() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-oneplus-s:main").await;

    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?who","ex:knows":{"@id":"ex:e"}}],
        "select": ["?who"]
    });
    let non = support::query_jsonld(&fluree, &ledger1, &q_non)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(non, json!([["ex:d"]]));

    let q_plus = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsPlus": {"@path": "ex:knows+"}
        },
        "where": [{"@id":"?who","knowsPlus":{"@id":"ex:e"}}],
        "select": ["?who"]
    });
    let plus = support::query_jsonld(&fluree, &ledger1, &q_plus)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&plus),
        normalize_rows(&json!([["ex:d"], ["ex:b"], ["ex:a"]]))
    );

    // Add cycle: e knows a. Now e is also in the reverse transitive set.
    let tx_cycle = json!({"@context":{"ex":"http://example.org/"},"insert":{"@id":"ex:e","ex:knows":{"@id":"ex:a"}}});
    let ledger2 = fluree.update(ledger1, &tx_cycle).await.unwrap().ledger;

    let plus2 = support::query_jsonld(&fluree, &ledger2, &q_plus)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&plus2),
        normalize_rows(&json!([["ex:d"], ["ex:b"], ["ex:a"], ["ex:e"]]))
    );
}

#[tokio::test]
async fn property_path_one_or_more_subject_and_object_vars_transitive_closure() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "property/path-oneplus-xy:main");

    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:1","ex:knows":{"@id":"ex:2"}},
            {"@id":"ex:2","ex:knows":{"@id":"ex:3"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q_non = json!({
        "@context": {"ex":"http://example.org/"},
        "where": [{"@id":"?s","ex:knows":"?o"}],
        "select": ["?s","?o"]
    });
    let non = support::query_jsonld(&fluree, &ledger1, &q_non)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&non),
        normalize_rows(&json!([["ex:1", "ex:2"], ["ex:2", "ex:3"]]))
    );

    let q_plus = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsPlus": {"@path": "ex:knows+"}
        },
        "where": [{"@id":"?x","knowsPlus":"?y"}],
        "select": ["?x","?y"]
    });
    let plus = support::query_jsonld(&fluree, &ledger1, &q_plus)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&plus),
        normalize_rows(&json!([
            ["ex:1", "ex:2"],
            ["ex:2", "ex:3"],
            ["ex:1", "ex:3"]
        ]))
    );

    // Add cycle 3 -> 1, producing reachability including self via non-zero paths.
    let tx_cycle = json!({"@context":{"ex":"http://example.org/"},"insert":{"@id":"ex:3","ex:knows":{"@id":"ex:1"}}});
    let ledger2 = fluree.update(ledger1, &tx_cycle).await.unwrap().ledger;

    let plus2 = support::query_jsonld(&fluree, &ledger2, &q_plus)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&plus2),
        normalize_rows(&json!([
            ["ex:1", "ex:1"],
            ["ex:1", "ex:2"],
            ["ex:1", "ex:3"],
            ["ex:2", "ex:1"],
            ["ex:2", "ex:2"],
            ["ex:2", "ex:3"],
            ["ex:3", "ex:1"],
            ["ex:3", "ex:2"],
            ["ex:3", "ex:3"]
        ]))
    );
}

#[tokio::test]
async fn property_path_zero_or_more_object_var_and_subject_object_vars() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-zeroplus-o:main").await;

    let q_star = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsStar": {"@path": "ex:knows*"}
        },
        "where": [{"@id":"ex:a","knowsStar":"?who"}],
        "select": ["?who"]
    });
    let star = support::query_jsonld(&fluree, &ledger1, &q_star)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&star),
        normalize_rows(&json!([["ex:a"], ["ex:b"], ["ex:c"], ["ex:d"], ["ex:e"]]))
    );

    // Subject+object vars, disjoint graphs
    let ledger0 = genesis_ledger(&fluree, "property/path-zeroplus-xy:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:1","ex:knows":{"@id":"ex:2"}},
            {"@id":"ex:2","ex:knows":{"@id":"ex:3"}},
            {"@id":"ex:4","ex:knows":{"@id":"ex:5"}},
            {"@id":"ex:5","ex:knows":{"@id":"ex:6"}}
        ]
    });
    let ledger2 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q_xy = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsStar": {"@path": "ex:knows*"}
        },
        "where": [{"@id":"?x","knowsStar":"?y"}],
        "select": ["?x","?y"]
    });
    let xy = support::query_jsonld(&fluree, &ledger2, &q_xy)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&xy),
        normalize_rows(&json!([
            ["ex:1", "ex:1"],
            ["ex:1", "ex:2"],
            ["ex:1", "ex:3"],
            ["ex:2", "ex:2"],
            ["ex:2", "ex:3"],
            ["ex:3", "ex:3"],
            ["ex:4", "ex:4"],
            ["ex:4", "ex:5"],
            ["ex:4", "ex:6"],
            ["ex:5", "ex:5"],
            ["ex:5", "ex:6"],
            ["ex:6", "ex:6"]
        ]))
    );
}

// -- Additional tests for @path features --

#[tokio::test]
async fn property_path_array_form() {
    // Test the S-expression array form: ["+", "ex:knows"]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-array-form:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsPlus": {"@path": ["+", "ex:knows"]}
        },
        "where": [{"@id":"ex:a","knowsPlus":"?who"}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger1, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:b"], ["ex:c"], ["ex:d"], ["ex:e"]]))
    );
}

#[tokio::test]
async fn property_path_unsupported_operator_error() {
    // Zero-or-one (?) is parsed but not yet supported for execution
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-unsupported:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "maybeFriend": {"@path": "ex:knows?"}
        },
        "where": [{"@id":"ex:a","maybeFriend":"?who"}],
        "select": ["?who"]
    });
    let err = support::query_jsonld(&fluree, &ledger1, &q).await;
    assert!(err.is_err(), "Zero-or-one paths should fail at execution");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("not yet supported"),
        "Error should mention 'not yet supported', got: {msg}"
    );
}

#[tokio::test]
async fn property_path_reverse_and_path_mutually_exclusive() {
    // @path + @reverse on the same term definition should error at parse time
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger1 = seed_knows_chain(&fluree, "property/path-reverse-error:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "bad": {"@path": "ex:knows+", "@reverse": "ex:knows"}
        },
        "where": [{"@id":"ex:a","bad":"?who"}],
        "select": ["?who"]
    });
    let err = support::query_jsonld(&fluree, &ledger1, &q).await;
    assert!(err.is_err(), "@path + @reverse should error");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("mutually exclusive"),
        "Error should mention 'mutually exclusive', got: {msg}"
    );
}

// -- Inverse (^) tests --

#[tokio::test]
async fn property_path_inverse_object_var() {
    // ^ex:knows from ex:b should return ex:a (since a knows b)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_chain(&fluree, "property/path-inverse-o:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knownBy": {"@path": "^ex:knows"}
        },
        "where": [{"@id":"ex:b","knownBy":"?who"}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalize_rows(&result), normalize_rows(&json!([["ex:a"]])));
}

#[tokio::test]
async fn property_path_inverse_subject_var() {
    // ^ex:knows to ex:a with subject var: who has inverse-knows a? → b (since a knows b)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_chain(&fluree, "property/path-inverse-s:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knownBy": {"@path": "^ex:knows"}
        },
        "where": [{"@id":"?who","knownBy":{"@id":"ex:a"}}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalize_rows(&result), normalize_rows(&json!([["ex:b"]])));
}

// -- Alternative (|) tests --

#[tokio::test]
async fn property_path_alternative_object_var() {
    // ex:knows|ex:likes from ex:a should return both knows targets and likes targets
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "property/path-alt-o:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:a","ex:likes":{"@id":"ex:x"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsOrLikes": {"@path": "ex:knows|ex:likes"}
        },
        "where": [{"@id":"ex:a","knowsOrLikes":"?o"}],
        "select": ["?o"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:b"], ["ex:x"]]))
    );
}

#[tokio::test]
async fn property_path_alternative_with_inverse() {
    // ex:knows|^ex:knows from ex:b should return forward (c, d) and inverse (a)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_chain(&fluree, "property/path-alt-inv:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsBoth": {"@path": "ex:knows|^ex:knows"}
        },
        "where": [{"@id":"ex:b","knowsBoth":"?who"}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:a"], ["ex:c"], ["ex:d"]]))
    );
}

#[tokio::test]
async fn property_path_alternative_array_form() {
    // Array form: ["|", "ex:knows", "ex:likes"] should produce same results as string form
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "property/path-alt-array:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:a","ex:likes":{"@id":"ex:x"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsOrLikes": {"@path": ["|", "ex:knows", "ex:likes"]}
        },
        "where": [{"@id":"ex:a","knowsOrLikes":"?o"}],
        "select": ["?o"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:b"], ["ex:x"]]))
    );
}

#[tokio::test]
async fn property_path_alternative_duplicate_semantics() {
    // When both predicates match the same (s,o) pair, UNION bag semantics
    // produces the result twice (one per branch).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "property/path-alt-dup:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:a","ex:knows":{"@id":"ex:b"}},
            {"@id":"ex:a","ex:likes":{"@id":"ex:b"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "knowsOrLikes": {"@path": "ex:knows|ex:likes"}
        },
        "where": [{"@id":"ex:a","knowsOrLikes":"?o"}],
        "select": ["?o"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // Bag semantics: ex:b appears once per matching branch
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:b"], ["ex:b"]]))
    );
}

#[tokio::test]
async fn property_path_nested_alternative_under_transitive_errors() {
    // (ex:knows|ex:likes)+ — alternative under transitive modifier should error
    // because transitive paths require a simple predicate IRI
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_chain(&fluree, "property/path-alt-trans-err:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "bad": {"@path": "(ex:knows|ex:likes)+"}
        },
        "where": [{"@id":"ex:a","bad":"?who"}],
        "select": ["?who"]
    });
    let err = support::query_jsonld(&fluree, &ledger, &q).await;
    assert!(err.is_err(), "(a|b)+ should error");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("simple predicate IRI"),
        "Error should mention 'simple predicate IRI', got: {msg}"
    );
}

// =============================================================================
// Sequence (/) property chain tests
// =============================================================================

#[tokio::test]
async fn property_path_sequence_two_step_string_form() {
    // ex:friend/ex:name — follow friend, then get name
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-seq-2step:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendName": {"@path": "ex:friend/ex:name"}
        },
        "where": [{"@id":"ex:alice","friendName":"?name"}],
        "select": ["?name"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // alice's friend is bob, bob's name is "Bob"
    assert_eq!(normalize_rows(&result), normalize_rows(&json!([["Bob"]])));
}

#[tokio::test]
async fn property_path_sequence_two_step_array_form() {
    // Array form: ["/", "ex:friend", "ex:name"]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-seq-2step-arr:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendName": {"@path": ["/", "ex:friend", "ex:name"]}
        },
        "where": [{"@id":"ex:alice","friendName":"?name"}],
        "select": ["?name"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalize_rows(&result), normalize_rows(&json!([["Bob"]])));
}

#[tokio::test]
async fn property_path_sequence_three_step() {
    // ex:friend/ex:address/ex:city — follow friend, then address, then city
    // alice -> bob -> carol -> addr1 -> "Springfield"
    // Needs two hops from alice: alice's friend bob's friend carol's address city
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-seq-3step:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendFriendCity": {"@path": "ex:friend/ex:friend/ex:address/ex:city"}
        },
        "where": [{"@id":"ex:alice","friendFriendCity":"?city"}],
        "select": ["?city"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // alice -> friend bob -> friend carol -> address addr1 -> city "Springfield"
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Springfield"]]))
    );
}

#[tokio::test]
async fn property_path_sequence_with_inverse_step() {
    // ^ex:parent/ex:name — traverse parent link backwards (find children), then get name
    // bob has parent alice, so ^ex:parent from alice finds bob, then bob's name is "Bob"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-seq-inv:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "childName": {"@path": "^ex:parent/ex:name"}
        },
        "where": [{"@id":"ex:alice","childName":"?name"}],
        "select": ["?name"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // alice <--parent-- bob, bob's name is "Bob"
    assert_eq!(normalize_rows(&result), normalize_rows(&json!([["Bob"]])));
}

#[tokio::test]
async fn property_path_sequence_wildcard_hides_internal_vars() {
    // Wildcard select with sequence path: ?__pp* variables must NOT appear in output
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-seq-wildcard:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendName": {"@path": "ex:friend/ex:name"}
        },
        "where": [{"@id":"ex:alice","friendName":"?name"}],
        "select": "*"
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let arr = result.as_array().expect("Wildcard result should be array");
    assert!(!arr.is_empty(), "Should have at least one result");

    for row in arr {
        let obj = row.as_object().expect("Each row should be an object");
        for key in obj.keys() {
            assert!(
                !key.starts_with("?__"),
                "Internal variable '{key}' should not appear in wildcard output"
            );
        }
    }
}

#[tokio::test]
async fn property_path_sequence_transitive_step_allowed() {
    // ex:friend+/ex:name — transitive modifier inside sequence should work
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-seq-trans-err:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendNamePlus": {"@path": "ex:friend+/ex:name"}
        },
        "where": [{"@id":"ex:alice","friendNamePlus":"?name"}],
        "select": ["?name"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Bob"], ["Carol"]]))
    );
}

// =============================================================================
// Sequence-in-Alternative (ex:a/ex:b | ex:c/ex:d) tests
// =============================================================================

/// Seed data for alternative-of-sequences tests.
///
/// Graph:
///   ex:alice --ex:friend--> ex:bob
///   ex:alice --ex:colleague--> ex:carol
///   ex:bob   --ex:name--> "Bob"
///   ex:carol --ex:name--> "Carol"
async fn seed_alt_seq_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:alice","ex:friend":{"@id":"ex:bob"},"ex:colleague":{"@id":"ex:carol"}},
            {"@id":"ex:bob","ex:name":"Bob"},
            {"@id":"ex:carol","ex:name":"Carol"}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

#[tokio::test]
async fn property_path_alternative_of_sequences() {
    // ex:friend/ex:name | ex:colleague/ex:name
    // alice -> friend bob (name "Bob") and colleague carol (name "Carol")
    // Both branches should match
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_alt_seq_data(&fluree, "property/path-alt-seq:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "contactName": {"@path": "ex:friend/ex:name|ex:colleague/ex:name"}
        },
        "where": [{"@id":"ex:alice","contactName":"?name"}],
        "select": ["?name"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Bob"], ["Carol"]]))
    );
}

#[tokio::test]
async fn property_path_alternative_mixed_simple_and_sequence() {
    // ex:name | ex:friend/ex:name — direct name OR friend's name
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_alt_seq_data(&fluree, "property/path-alt-mix:main").await;

    // Give alice a direct name too
    let insert2 = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [{"@id":"ex:alice","ex:name":"Alice"}]
    });
    let ledger = fluree.insert(ledger, &insert2).await.unwrap().ledger;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "nameOrFriendName": {"@path": "ex:name|ex:friend/ex:name"}
        },
        "where": [{"@id":"ex:alice","nameOrFriendName":"?val"}],
        "select": ["?val"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Alice"], ["Bob"]]))
    );
}

#[tokio::test]
async fn property_path_alternative_of_sequences_wildcard_hides_vars() {
    // Wildcard select with alternative-of-sequences: ?__pp* variables must not appear
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_alt_seq_data(&fluree, "property/path-alt-seq-wc:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "contactName": {"@path": "ex:friend/ex:name|ex:colleague/ex:name"}
        },
        "where": [{"@id":"ex:alice","contactName":"?name"}],
        "select": "*"
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let arr = result.as_array().expect("Wildcard result should be array");
    assert!(!arr.is_empty(), "Should have at least one result");

    for row in arr {
        let obj = row.as_object().expect("Each row should be an object");
        for key in obj.keys() {
            assert!(
                !key.starts_with("?__"),
                "Internal variable '{key}' should not appear in wildcard output"
            );
        }
    }
}

#[tokio::test]
async fn property_path_alternative_of_sequences_duplicate_semantics() {
    // When both sequence branches resolve to the same value,
    // UNION bag semantics produces the result twice (one per branch).
    // ex:friend → ex:bob, ex:colleague → ex:bob, bob's name → "Bob"
    // So (friend/name)|(colleague/name) should return "Bob" twice.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "property/path-alt-seq-dup:main");
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {"@id":"ex:alice","ex:friend":{"@id":"ex:bob"},"ex:colleague":{"@id":"ex:bob"}},
            {"@id":"ex:bob","ex:name":"Bob"}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "contactName": {"@path": "ex:friend/ex:name|ex:colleague/ex:name"}
        },
        "where": [{"@id":"ex:alice","contactName":"?name"}],
        "select": ["?name"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // Bag semantics: "Bob" appears once per matching branch
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Bob"], ["Bob"]]))
    );
}

// =============================================================================
// Inverse-transitive (^p+ / ^p*) tests
// =============================================================================

#[tokio::test]
async fn property_path_inverse_one_or_more() {
    // ^ex:knows+ from ex:c → reverse one-or-more: who knows c? b. who knows b? a.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_chain(&fluree, "property/path-inv-plus:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "invKnowsPlus": {"@path": "^ex:knows+"}
        },
        "where": [{"@id":"ex:c","invKnowsPlus":"?x"}],
        "select": ["?x"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:a"], ["ex:b"]]))
    );
}

#[tokio::test]
async fn property_path_inverse_zero_or_more() {
    // ^ex:knows* from ex:b → reverse zero-or-more (includes self):
    // zero hops: b. who knows b? a. who knows a? nobody.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_knows_chain(&fluree, "property/path-inv-star:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "invKnowsStar": {"@path": "^ex:knows*"}
        },
        "where": [{"@id":"ex:b","invKnowsStar":"?x"}],
        "select": ["?x"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:a"], ["ex:b"]]))
    );
}

// =============================================================================
// Alternative-in-Sequence distribution tests
// =============================================================================

async fn seed_alt_in_seq_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex":"http://example.org/"},
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:name": "Alice",
                "ex:nick": "Ali",
                "ex:friend": {"@id": "ex:bob"}
            },
            {
                "@id": "ex:bob",
                "ex:name": "Bob",
                "ex:nick": "Bobby"
            }
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

#[tokio::test]
async fn property_path_sequence_with_alternative_step() {
    // ex:friend/(ex:name|ex:nick) — friend's name or nick
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_alt_in_seq_data(&fluree, "property/path-alt-in-seq:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendLabel": {"@path": "ex:friend/(ex:name|ex:nick)"}
        },
        "where": [{"@id":"ex:alice","friendLabel":"?val"}],
        "select": ["?val"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Bob"], ["Bobby"]]))
    );
}

#[tokio::test]
async fn property_path_sequence_with_alternative_step_array_form() {
    // Array form: ["/", "ex:friend", ["|", "ex:name", "ex:nick"]]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_alt_in_seq_data(&fluree, "property/path-alt-in-seq-arr:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendLabel": {"@path": ["/", "ex:friend", ["|", "ex:name", "ex:nick"]]}
        },
        "where": [{"@id":"ex:alice","friendLabel":"?val"}],
        "select": ["?val"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["Bob"], ["Bobby"]]))
    );
}

#[tokio::test]
async fn property_path_sequence_with_alternative_step_wildcard() {
    // Wildcard select — internal ?__pp variables should not appear in output keys
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_alt_in_seq_data(&fluree, "property/path-alt-in-seq-wc:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "friendLabel": {"@path": "ex:friend/(ex:name|ex:nick)"}
        },
        "where": [{"@id":"ex:alice","friendLabel":"?val"}],
        "select": "*"
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let arr = result.as_array().expect("Expected array result");
    for row in arr {
        if let Some(obj) = row.as_object() {
            for key in obj.keys() {
                assert!(
                    !key.starts_with("?__"),
                    "Internal variable {key} should not appear in wildcard output",
                );
            }
        }
    }
}

// =============================================================================
// Inverse of complex paths tests
// =============================================================================

#[tokio::test]
async fn property_path_inverse_of_sequence() {
    // ^(ex:friend/ex:friend): reverse and invert → (^ex:friend)/(^ex:friend)
    // Subject: ex:carol, Object: ?who
    // Semantics: ?who friend/friend carol → alice→bob→carol
    // Expected: ?who = ex:alice
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-inv-seq:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "invFF": {"@path": "^(ex:friend/ex:friend)"}
        },
        "where": [{"@id":"ex:carol","invFF":"?who"}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:alice"]])),
    );
}

#[tokio::test]
async fn property_path_inverse_of_alternative() {
    // ^(ex:friend|ex:parent): distribute → (^ex:friend)|(^ex:parent)
    // Subject: ex:bob, Object: ?who
    // Semantics: ?who (friend|parent) bob → alice has friend=bob
    // Expected: ?who = ex:alice
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-inv-alt:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "invRel": {"@path": "^(ex:friend|ex:parent)"}
        },
        "where": [{"@id":"ex:bob","invRel":"?who"}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:alice"]])),
    );
}

#[tokio::test]
async fn property_path_inverse_of_three_step_sequence() {
    // ^(ex:friend/ex:friend/ex:address): reverse and invert
    // Subject: ex:addr1, Object: ?who
    // Semantics: ?who friend/friend/address addr1 → alice→bob→carol→addr1
    // Expected: ?who = ex:alice
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_chain_data(&fluree, "property/path-inv-seq3:main").await;

    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "invChain": {"@path": "^(ex:friend/ex:friend/ex:address)"}
        },
        "where": [{"@id":"ex:addr1","invChain":"?who"}],
        "select": ["?who"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([["ex:alice"]])),
    );
}
