//! `COUNT(DISTINCT *)` — the count of distinct SOLUTIONS in a group.
//!
//! Well-defined in SPARQL 1.1 §18.5.1.1 and exercised by the W3C
//! `agg-count-rows-distinct` test. The parser accepted it and lowering rejected
//! it as "not yet implemented", so `SELECT ?s (COUNT(DISTINCT *) AS ?n) …`
//! returned HTTP 400.
//!
//! It is the only aggregate that reads the whole row rather than one column, so
//! the tests below pin three things a single happy-path test would miss: that
//! duplicate solutions actually collapse, that projection trimming does not
//! erase the column that makes two solutions differ, and that the traditional
//! grouping path computes it as well as the streaming one.

use crate::support;
use crate::support::{
    genesis_ledger, graphdb_from_ledger, normalize_rows, MemoryFluree, MemoryLedger,
};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

/// maker1 carries two properties, maker2 one — so the two makers have
/// different group sizes and a wrong answer cannot look right by symmetry.
async fn seed_makers(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:maker1", "@type": "ex:Maker", "ex:name": "Acme"},
            {"@id": "ex:maker2", "ex:name": "Globex"}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

fn physical_contains_op(node: &JsonValue, name: &str) -> bool {
    if node["op"] == name {
        return true;
    }
    node["children"]
        .as_array()
        .is_some_and(|cs| cs.iter().any(|e| physical_contains_op(&e["node"], name)))
}

/// The W3C `agg-count-rows-distinct` shape. Every solution in a group has a
/// distinct `(?p, ?o)`, so the distinct count equals the group size.
#[tokio::test]
async fn sparql_count_distinct_star_grouped() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/cds-grouped:main").await;

    let query = "SELECT ?s (COUNT(DISTINCT *) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?s";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT(DISTINCT *) is well-defined in SPARQL 1.1")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["http://example.org/maker1", 2],
            ["http://example.org/maker2", 1]
        ]))
    );
}

/// No GROUP BY: one implicit group over the whole solution sequence.
#[tokio::test]
async fn sparql_count_distinct_star_ungrouped() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/cds-ungrouped:main").await;

    let jsonld = support::query_sparql(
        &fluree,
        &ledger,
        "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { ?s ?p ?o }",
    )
    .await
    .expect("COUNT(DISTINCT *) without GROUP BY")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([[3]])));
}

/// The load-bearing case: duplicate solutions must collapse. A UNION of a
/// pattern with itself doubles every solution, so `COUNT(*)` counts twice what
/// `COUNT(DISTINCT *)` does. An implementation that quietly answered `COUNT(*)`
/// would pass every test above and fail this one.
#[tokio::test]
async fn sparql_count_distinct_star_collapses_duplicate_solutions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/cds-dupes:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?all) (COUNT(DISTINCT *) AS ?distinct)
        WHERE { { ?s ex:name ?n } UNION { ?s ex:name ?n } }";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("COUNT(DISTINCT *) alongside COUNT(*)")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([[4, 2]])));
}

/// Projection trimming must not fire. Nothing here projects `?s` or `?n`, and
/// `COUNT(DISTINCT *)` reports no input variable — so a trimmed WHERE would
/// drop both columns and collapse all four solutions into one.
#[tokio::test]
async fn sparql_count_distinct_star_survives_projection_trimming() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/cds-trim:main").await;

    let jsonld = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT (COUNT(DISTINCT *) AS ?n) WHERE { ?s ex:name ?nm }",
    )
    .await
    .expect("COUNT(DISTINCT *) with nothing else projected")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), normalize_rows(&json!([[2]])));
}

/// The traditional grouping path reconstructs the group's solutions by zipping
/// its `Grouped` columns, which is a different implementation from the
/// streaming `HashSet`. `GROUP_CONCAT` forces that path; EXPLAIN is the marker
/// that it really ran.
#[tokio::test]
async fn sparql_count_distinct_star_traditional_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/cds-traditional:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s (COUNT(DISTINCT *) AS ?n) (GROUP_CONCAT(?nm; SEPARATOR="|") AS ?g)
        WHERE { { ?s ex:name ?nm } UNION { ?s ex:name ?nm } } GROUP BY ?s"#;

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
        .expect("COUNT(DISTINCT *) on the traditional path")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // Each maker's group holds its one solution twice: 1 distinct, 2 concatenated.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            ["ex:maker1", 1, "Acme|Acme"],
            ["ex:maker2", 1, "Globex|Globex"]
        ]))
    );
}

/// HAVING over `COUNT(DISTINCT *)`, and the plain `COUNT(*)` control alongside
/// it, so the two no-input aggregates cannot be confused for one another.
#[tokio::test]
async fn sparql_count_distinct_star_with_having_and_count_all() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/cds-having:main").await;

    let query = "SELECT ?s (COUNT(*) AS ?all) (COUNT(DISTINCT *) AS ?d)
                 WHERE { ?s ?p ?o } GROUP BY ?s HAVING (COUNT(DISTINCT *) > 1)";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("HAVING over COUNT(DISTINCT *)")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["http://example.org/maker1", 2, 2]]))
    );
}
