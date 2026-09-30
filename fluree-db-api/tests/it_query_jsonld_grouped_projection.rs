//! JSON-LD twins of the SPARQL grouped-projection semantics (#1978). SPARQL
//! and JSON-LD share the IR, so a grouping rule holds on both surfaces; the
//! SPARQL side lives in `it_query_sparql_grouped_projection.rs`.
//!
//! Fixture: e1–e3 Net, e4–e5 Local, e6 Remote — three groups whose entity
//! IRIs sort apart (e1–e3 < e4–e5 < e6), so any SAMPLE of `?e` orders the
//! groups the same way.

use crate::support::{self, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

async fn seed_areas(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:e1", "ex:area": "Net"},
            {"@id": "ex:e2", "ex:area": "Net"},
            {"@id": "ex:e3", "ex:area": "Net"},
            {"@id": "ex:e4", "ex:area": "Local"},
            {"@id": "ex:e5", "ex:area": "Local"},
            {"@id": "ex:e6", "ex:area": "Remote"}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("seed").ledger
}

/// Run `query` (its `@context` and `where` filled in) and render JSON-LD rows.
async fn rows(fluree: &MemoryFluree, ledger: &MemoryLedger, query: JsonValue) -> JsonValue {
    let mut query = query;
    query["@context"] = json!({"ex": "http://example.org/"});
    query["where"] = json!({"@id": "?e", "ex:area": "?a"});
    let result = support::query_jsonld(fluree, ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("{e}\n{query}"));
    result.to_jsonld(&ledger.snapshot).expect("to_jsonld")
}

/// `having` / `orderBy` reading a non-key variable of a grouped query means
/// `SAMPLE(?v)` (SPARQL 1.1 §18.2.4.1). This HAVING holds for any sample, so
/// every group survives. It returned no groups with `count` (the streaming
/// lane read the variable as unbound) and panicked a debug build with
/// `groupconcat` (the list reached scalar evaluation).
#[tokio::test]
async fn jsonld_having_on_a_non_key_variable_samples_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/having-sample:main").await;
    for aggregate in ["(as (count ?e) ?n)", "(as (groupconcat ?e \",\") ?n)"] {
        let found = rows(
            &fluree,
            &ledger,
            json!({
                "select": ["?a", aggregate],
                "groupBy": ["?a"],
                "having": "(strStarts (str ?e) \"http://example.org/e\")"
            }),
        )
        .await;
        assert_eq!(
            found.as_array().map(Vec::len),
            Some(3),
            "every group qualifies with {aggregate}: {found}"
        );
    }
}

#[tokio::test]
async fn jsonld_order_by_a_non_key_variable_samples_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/order-sample:main").await;
    let found = rows(
        &fluree,
        &ledger,
        json!({
            "select": ["?a", "(as (count ?e) ?n)"],
            "groupBy": ["?a"],
            "orderBy": ["?e"]
        }),
    )
    .await;
    assert_eq!(found, json!([["Net", 3], ["Local", 2], ["Remote", 1]]));
}

/// `having` on a query that does not group is a filter over its solutions
/// (§18.2.4.2); it used to be dropped. It cannot see the query's SELECT
/// expressions, so `(bound ?s)` is false for every solution.
#[tokio::test]
async fn jsonld_having_without_grouping_filters() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/having-filter:main").await;
    let found = rows(
        &fluree,
        &ledger,
        json!({"select": ["?a"], "having": "(= ?a \"Net\")"}),
    )
    .await;
    assert_eq!(
        normalize_rows(&found),
        normalize_rows(&json!([["Net"], ["Net"], ["Net"]]))
    );

    let found = rows(
        &fluree,
        &ledger,
        json!({"select": ["?a", "(as (str ?a) ?s)"], "having": "(bound ?s)"}),
    )
    .await;
    assert_eq!(found, json!([]));
}

/// A JSON-LD per-group list has no SPARQL-results rendering: SPARQL JSON and
/// XML refuse it (they used to expand it into one row per element, and drop the
/// row of an empty list). The JSON-LD formats keep rendering it as an array.
#[tokio::test]
async fn jsonld_per_group_list_is_refused_by_sparql_results_formats() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/list-formats:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?e"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("JSON-LD grouped-list projection");

    assert!(result.to_sparql_json(&ledger.snapshot).is_err());
    assert!(fluree_db_api::format::format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &fluree_db_api::FormatterConfig::sparql_xml(),
    )
    .is_err());

    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r[1].is_array()), "{rows:?}");
}

/// J3: a select expression that reads only `groupBy` keys, aggregate outputs
/// or earlier per-group aliases — or nothing — is evaluated once per group,
/// as in SPARQL. It used to be a per-group list of the same value repeated
/// (`["network", "network", "network"]`).
#[tokio::test]
async fn jsonld_key_only_select_expression_is_one_value_per_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/j3-scalar:main").await;

    let found = rows(
        &fluree,
        &ledger,
        json!({
            "select": [
                "(as (if (= ?a \"Net\") \"network\" \"other\") ?seg)",
                "(as (count ?e) ?n)"
            ],
            "groupBy": ["?a"]
        }),
    )
    .await;
    assert_eq!(
        normalize_rows(&found),
        normalize_rows(&json!([["network", 3], ["other", 2], ["other", 1]]))
    );

    let found = rows(
        &fluree,
        &ledger,
        json!({"select": ["(as (strlen ?a) ?len)", "(as (count ?e) ?n)"], "groupBy": ["?a"]}),
    )
    .await;
    assert_eq!(
        normalize_rows(&found),
        normalize_rows(&json!([[3, 3], [5, 2], [6, 1]]))
    );

    // A constant under implicit grouping — including over no solutions, where
    // the one implicit group still has a row.
    let found = rows(
        &fluree,
        &ledger,
        json!({"select": ["(as (str \"x\") ?c)", "(as (count ?e) ?n)"]}),
    )
    .await;
    assert_eq!(found, json!([["x", 6]]));
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(as (str \"x\") ?c)", "(as (count ?e) ?n)"],
        "where": {"@id": "?e", "ex:nope": "?a"}
    });
    let found = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(found, json!([["x", 0]]));
}

/// What J3 leaves alone: an expression over a non-key variable keeps the
/// documented per-group list (so does a chain over it), and so does an alias an
/// aggregate reads; an alias that is itself a `groupBy` key is one value per
/// group.
#[tokio::test]
async fn jsonld_non_key_select_expression_stays_a_per_group_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/j3-lists:main").await;
    let net = |found: &JsonValue| -> JsonValue {
        found
            .as_array()
            .expect("rows")
            .iter()
            .find(|r| r[0] == "Net")
            .cloned()
            .unwrap_or_else(|| panic!("a Net row: {found}"))
    };

    let found = rows(
        &fluree,
        &ledger,
        json!({
            "select": ["?a", "(as (str ?e) ?es)", "(as (strlen ?es) ?len)"],
            "groupBy": ["?a"]
        }),
    )
    .await;
    let row = net(&found);
    let mut es: Vec<&str> = row[1]
        .as_array()
        .unwrap_or_else(|| panic!("?es is a list: {found}"))
        .iter()
        .map(|v| v.as_str().expect("string"))
        .collect();
    es.sort_unstable();
    assert_eq!(
        es,
        vec![
            "http://example.org/e1",
            "http://example.org/e2",
            "http://example.org/e3"
        ]
    );
    assert_eq!(row[2], json!([21, 21, 21]), "{found}");

    let found = rows(
        &fluree,
        &ledger,
        json!({"select": ["?a", "(as (str ?a) ?s)", "(as (count ?s) ?c)"], "groupBy": ["?a"]}),
    )
    .await;
    assert_eq!(net(&found), json!(["Net", ["Net", "Net", "Net"], 3]));

    let found = rows(
        &fluree,
        &ledger,
        json!({"select": ["(as (str ?a) ?k)", "(as (count ?e) ?n)"], "groupBy": ["?k"]}),
    )
    .await;
    assert_eq!(
        normalize_rows(&found),
        normalize_rows(&json!([["Net", 3], ["Local", 2], ["Remote", 1]]))
    );
}

/// A select expression evaluated per group cannot take the name of a variable
/// the WHERE binds: it would silently replace that variable's value after
/// grouping (here, the `groupBy` key itself). SPARQL rejects the same alias.
#[tokio::test]
async fn jsonld_select_alias_cannot_shadow_a_where_variable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/alias-shadow:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(as (count ?e) ?n)", "(as (str ?n) ?a)"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    let err = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect_err("a per-group alias onto a WHERE variable");
    assert!(
        err.to_string()
            .contains("select alias ?a is already bound by the where clause"),
        "{err}"
    );
}

/// A bare aggregate column keeps its implicit name (`(count ?e)` → `?count`),
/// which downstream code reads by name.
#[tokio::test]
async fn jsonld_bare_aggregate_keeps_its_implicit_name() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/count-name:main").await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "(count ?e)"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    let typed = support::query_jsonld_format(
        &fluree,
        &ledger,
        &query,
        &fluree_db_api::FormatterConfig::typed_json(),
    )
    .await
    .expect("typed json");
    let rows = typed.as_array().expect("rows");
    assert_eq!(rows.len(), 3, "{typed}");
    for row in rows {
        let mut keys: Vec<&str> = row
            .as_object()
            .unwrap_or_else(|| panic!("typed-json row object: {typed}"))
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["?a", "?count"], "{typed}");
    }
}

/// A subquery cannot return a per-group list: its projection is plain
/// variables, so projecting a variable its grouping does not produce is a plan
/// error. The list used to cross into the enclosing query — rendered as lists
/// when projected there, and silently matching nothing when joined on.
#[tokio::test]
async fn jsonld_subquery_cannot_return_a_per_group_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_areas(&fluree, "jsonld-grouped/subquery-list:main").await;
    let ctx = json!({"ex": "http://example.org/"});
    let subquery = json!(["query", {
        "@context": ctx,
        "select": ["?a", "?e"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    }]);
    for query in [
        // The enclosing query projects the list.
        json!({"@context": ctx, "select": ["?a", "?e"], "where": [subquery]}),
        // The enclosing query joins on it.
        json!({
            "@context": ctx,
            "select": ["?a"],
            "where": [subquery, {"@id": "?e", "ex:area": "?other"}]
        }),
    ] {
        let err = support::query_jsonld(&fluree, &ledger, &query)
            .await
            .expect_err("a subquery projecting a non-key variable of its grouping");
        let msg = err.to_string();
        assert!(
            msg.contains("is neither a GROUP BY key nor an aggregate result"),
            "{query}: {msg}"
        );
    }
}
