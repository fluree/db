//! Aggregates whose input variable is also a GROUP BY key.
//!
//! `SELECT ?k (COUNT(?k) AS ?n) … GROUP BY ?k` is legal SPARQL 1.1 (§18.2.4.1
//! partitions the multiset; §18.5.1.1 counts the argument's values within a
//! group — nothing excludes an expression that also appears in GROUP BY). It
//! used to be rejected outright with `err:db/InvalidQuery`.
//!
//! Both grouping implementations must be pinned. The streaming
//! `GroupAggregateOperator` reads the pre-grouping column and was always
//! correct here; the traditional `GroupByOperator` + `AggregateOperator` pair
//! collapses key columns to a scalar and would have answered with the key term
//! itself. A test that only exercises the streaming path would pass even with
//! the old hazard intact, so the traditional-path test below asserts through
//! EXPLAIN which operators actually ran.

use crate::support;
use crate::support::{
    genesis_ledger, graphdb_from_ledger, normalize_rows, MemoryFluree, MemoryLedger,
};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

/// 2 makers, 5 models: maker1 has 2, maker2 has 1, and m4/m5 have no maker at
/// all so an OPTIONAL-bound grouping key produces a genuinely unbound group.
async fn seed_makers(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:maker1", "@type": "ex:Maker", "ex:name": "Acme"},
            {"@id": "ex:maker2", "@type": "ex:Maker", "ex:name": "Globex"},
            {"@id": "ex:m1", "@type": "ex:Model", "ex:ofMaker": {"@id": "ex:maker1"}},
            {"@id": "ex:m2", "@type": "ex:Model", "ex:ofMaker": {"@id": "ex:maker1"}},
            {"@id": "ex:m3", "@type": "ex:Model", "ex:ofMaker": {"@id": "ex:maker2"}},
            {"@id": "ex:m4", "@type": "ex:Model"},
            {"@id": "ex:m5", "@type": "ex:Model"}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

/// Recursively search an EXPLAIN `plan.physical` tree for an operator name.
fn physical_contains_op(node: &JsonValue, name: &str) -> bool {
    if node["op"] == name {
        return true;
    }
    node["children"]
        .as_array()
        .is_some_and(|cs| cs.iter().any(|e| physical_contains_op(&e["node"], name)))
}

/// The reported case: one solution per maker, so each group counts 1.
#[tokio::test]
async fn sparql_count_over_group_by_key() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-count:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?c (COUNT(?c) AS ?n) WHERE { ?c a ex:Maker } GROUP BY ?c";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT over a GROUP BY key is legal SPARQL 1.1")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:maker1", 1], ["ex:maker2", 1]]))
    );
}

/// Group sizes larger than one, so a wrong answer cannot hide behind a count
/// that happens to be 1.
#[tokio::test]
async fn sparql_count_over_group_by_key_multi_row_groups() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-count-multi:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?c (COUNT(?c) AS ?n) WHERE { ?m ex:ofMaker ?c } GROUP BY ?c";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT over a GROUP BY key is legal SPARQL 1.1")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:maker1", 2], ["ex:maker2", 1]]))
    );
}

/// HAVING aggregates lower into the same aggregate list, so they take the same
/// rewrite. Both a passing and a filtering threshold, so the HAVING is doing
/// real work rather than being trivially true.
#[tokio::test]
async fn sparql_having_count_over_group_by_key() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-having:main").await;

    let keeps_all = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?c WHERE { ?m ex:ofMaker ?c } GROUP BY ?c HAVING (COUNT(?c) >= 1)",
    )
    .await
    .expect("HAVING over a GROUP BY key is legal")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&keeps_all),
        normalize_rows(&json!([["ex:maker1"], ["ex:maker2"]]))
    );

    let filters = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?c WHERE { ?m ex:ofMaker ?c } GROUP BY ?c HAVING (COUNT(?c) >= 2)",
    )
    .await
    .expect("HAVING over a GROUP BY key is legal")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&filters),
        normalize_rows(&json!([["ex:maker1"]]))
    );
}

/// `COUNT(DISTINCT ?k)` over the key was rejected on the same path — the
/// reporter's "just add DISTINCT" workaround did not actually work. Every value
/// in a group is the key, so the distinct count is 1 per group.
#[tokio::test]
async fn sparql_count_distinct_over_group_by_key() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-count-distinct:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?c (COUNT(DISTINCT ?c) AS ?n) WHERE { ?m ex:ofMaker ?c } GROUP BY ?c";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT(DISTINCT key) over a GROUP BY key is legal")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:maker1", 1], ["ex:maker2", 1]]))
    );
}

/// The case that makes the `COUNT(*)` rewrite-workaround wrong, and the reason
/// the copy is `Expression::Var(k)` rather than a synthetic constant: with an
/// OPTIONAL-bound key, the unbound group counts 0 solutions for `COUNT(?k)`
/// while `COUNT(*)` counts its 2 rows.
#[tokio::test]
async fn sparql_count_over_optional_bound_group_key() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-optional:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?k (COUNT(?k) AS ?nk) (COUNT(*) AS ?nall)
        WHERE { ?s a ex:Model OPTIONAL { ?s ex:ofMaker ?k } }
        GROUP BY ?k";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT over an OPTIONAL-bound GROUP BY key is legal")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["ex:maker1", 2, 2],
            ["ex:maker2", 1, 1],
            [null, 0, 2]
        ]))
    );
}

/// Pins the TRADITIONAL grouping path, where the old hazard actually lived:
/// `GroupByOperator` writes the scalar key into key columns, so an aggregate
/// reading a key column would have returned the key term instead of a count.
/// `GROUP_CONCAT` is not streamable, which forces the pair; the EXPLAIN
/// assertion is the positive marker that it really did.
#[tokio::test]
async fn sparql_count_over_group_by_key_traditional_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-traditional:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?c (COUNT(?c) AS ?n) (GROUP_CONCAT(?nm; SEPARATOR="|") AS ?g)
        WHERE { ?m ex:ofMaker ?c . ?c ex:name ?nm } GROUP BY ?c"#;

    let db = graphdb_from_ledger(&ledger);
    let explained = fluree
        .explain_sparql(&db, query)
        .await
        .expect("explain_sparql");
    let physical = &explained["plan"]["physical"];
    assert!(
        physical_contains_op(physical, "GroupByOperator")
            && physical_contains_op(physical, "AggregateOperator"),
        "expected the traditional grouping pair, got: {physical}"
    );
    assert!(
        !physical_contains_op(physical, "GroupAggregateOperator"),
        "GROUP_CONCAT should have forced off the streaming path: {physical}"
    );

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT over a GROUP BY key is legal on the traditional path")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // Every row in a group repeats its maker's single name, so the
    // concatenation is order-independent: maker1's group has two rows,
    // maker2's has one.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["ex:maker1", 2, "Acme|Acme"],
            ["ex:maker2", 1, "Globex"]
        ]))
    );
}

/// Controls: the shapes that already worked must be untouched by the rewrite.
#[tokio::test]
async fn sparql_aggregate_over_non_key_var_unchanged() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-controls:main").await;

    let non_key = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?c (COUNT(?m) AS ?n) WHERE { ?m ex:ofMaker ?c } GROUP BY ?c",
    )
    .await
    .expect("COUNT over a non-key variable")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&non_key),
        normalize_rows(&json!([["ex:maker1", 2], ["ex:maker2", 1]]))
    );

    let count_all = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?c (COUNT(*) AS ?n) WHERE { ?m ex:ofMaker ?c } GROUP BY ?c",
    )
    .await
    .expect("COUNT(*)")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&count_all),
        normalize_rows(&json!([["ex:maker1", 2], ["ex:maker2", 1]]))
    );
}

/// An aggregate reading a key must not disturb one reading a non-key variable
/// in the same query, and two aggregates over the same key must share one copy.
#[tokio::test]
async fn sparql_mixed_key_and_non_key_aggregates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/agg-key-mixed:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?c (COUNT(?c) AS ?nk) (COUNT(?m) AS ?nm) (MIN(?c) AS ?mink)
        WHERE { ?m ex:ofMaker ?c } GROUP BY ?c";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("mixed key and non-key aggregates")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["ex:maker1", 2, 2, "ex:maker1"],
            ["ex:maker2", 1, 1, "ex:maker2"]
        ]))
    );
}
