//! Aggregate query integration tests
//!
//! All inserts and queries are explicit with `@context`.
//!
//! Aggregate S-expression syntax:
//! - `(count ?x)` - simple aggregate, auto-generates output var `?count`
//! - `(as (count ?x) ?alias)` - aggregate with explicit alias
//! - `(count *)` - count all rows in group
//! - `(groupconcat ?x ", ")` - separator as second argument

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    // Seed dataset with typed birthDate fields used by aggregate tests.
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 50,
                "ex:favNums": [42, 76, 9],
                "schema:birthDate": {"@value": "1974-09-26", "@type": "xsd:date"}
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:favNums": [5, 10]
            },
            {
                "@id": "ex:liam",
                "@type": "ex:User",
                "schema:name": "Liam",
                "schema:email": "liam@example.org",
                "schema:age": 13,
                "ex:favNums": [42, 11],
                "schema:birthDate": {"@value": "2011-09-26", "@type": "xsd:date"}
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

fn extract_single_column_numbers(rows: &JsonValue) -> Vec<i64> {
    rows.as_array()
        .expect("rows array")
        .iter()
        .map(|row| {
            // JSON-LD formatter flattens single-column selects to a flat array
            // Accept both flat and legacy 1-element row arrays.
            if let Some(n) = row.as_i64() {
                return n;
            }
            row.as_array()
                .and_then(|a| a.first())
                .and_then(serde_json::Value::as_i64)
                .expect("i64 (flat or row[0])")
        })
        .collect()
}

#[tokio::test]
async fn aggregates_explicit_grouping_count() {
    // Scenario: "with explicit grouping" (count favNums by name)
    // Equivalent syntax: :select '[?name (as (count ?favNums) ?count)]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "(as (count ?favNums) ?count)"],
        "where": { "schema:name": "?name", "ex:favNums": "?favNums" },
        "groupBy": ["?name"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Alice", 3],
            ["Brian", 1],
            ["Cam", 2],
            ["Liam", 2]
        ]))
    );
}

#[tokio::test]
async fn aggregates_with_bind_ucase_and_count() {
    // Scenario: "with data function syntax" (ucase via bind + count)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-bind:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?upperName", "(as (count ?favNums) ?count)"],
        "where": [
            {"schema:name": "?name", "ex:favNums": "?favNums"},
            ["bind", "?upperName", ["expr", ["ucase", "?name"]]]
        ],
        "groupBy": ["?upperName"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["ALICE", 3],
            ["BRIAN", 1],
            ["CAM", 2],
            ["LIAM", 2]
        ]))
    );
}

#[tokio::test]
async fn aggregates_singular_selector_count_per_group() {
    // Scenario: "with singular function selector" expects [3 1 2 2]
    // Using simple (count ?favNums) which auto-generates ?count
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-singular:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(count ?favNums)"],
        "where": { "schema:name": "?name", "ex:favNums": "?favNums" },
        "groupBy": ["?name"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    let mut counts = extract_single_column_numbers(&json_rows);
    counts.sort();
    assert_eq!(counts, vec![1, 2, 2, 3]);
}

#[tokio::test]
async fn aggregates_implicit_grouping_count_all() {
    // Scenario: "with implicit grouping" => [4]
    // Equivalent syntax: :select '[(count ?name)]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-implicit:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(count ?name)"],
        "where": { "schema:name": "?name" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(json_rows, json!([4]));
}

#[tokio::test]
async fn aggregates_min_implicit_grouping() {
    // Scenario: "with min and implicit grouping" => [5]
    // Equivalent syntax: :select '[(min ?nums)]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-min:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(min ?nums)"],
        "where": { "ex:favNums": "?nums" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(json_rows, json!([5]));
}

#[tokio::test]
async fn aggregates_max_date_implicit_grouping() {
    // Scenario: "with implicit grouping and comparable data types" expects 2011-09-26.
    // Rust returns a typed JSON-LD value map for xsd:date.
    // Equivalent syntax: :select ['(max ?birthDate)]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-max-date:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(max ?birthDate)"],
        "where": { "schema:birthDate": "?birthDate" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        json_rows,
        json!([{"@value": "2011-09-26", "@type": "xsd:date"}])
    );
}

#[tokio::test]
async fn aggregates_with_ordering_on_count() {
    // Scenario: "with ordering" (order by ?count)
    // Equivalent syntax: :select '[?name (as (count ?favNums) ?count)]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-order:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "(as (count ?favNums) ?count)"],
        "where": { "schema:name": "?name", "ex:favNums": "?favNums" },
        "groupBy": ["?name"],
        "orderBy": "?count"
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Verify multiset of results
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["Brian", 1],
            ["Cam", 2],
            ["Liam", 2],
            ["Alice", 3]
        ]))
    );

    // Verify non-decreasing counts (tie order is not specified)
    let counts: Vec<i64> = json_rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_array().unwrap()[1].as_i64().unwrap())
        .collect();
    assert!(counts.windows(2).all(|w| w[0] <= w[1]));
}

#[tokio::test]
async fn aggregates_count_all_favnums_implicit_grouping() {
    // Note: some clients return [8] vs [[8]] depending on row-shape configuration.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-count-all:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(count ?favNums)"],
        "where": [{"@id": "?s", "ex:favNums": "?favNums"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(json_rows, json!([8]));
}

#[tokio::test]
async fn aggregates_groupconcat_default_separator() {
    // Scenario: groupconcat without explicit separator (defaults to " ")
    // Equivalent syntax: :select ['?s '(groupconcat ?favNums)]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-gc:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?s", "(as (groupconcat ?favNums) ?nums)"],
        "where": [{"@id": "?s", "ex:favNums": "?favNums"}],
        "groupBy": ["?s"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Note: concatenation order depends on underlying binding order; compare as sets of acceptable strings.
    // We assert the exact strings for this dataset after sorting numbers ascending in storage is not guaranteed,
    // so keep this order-insensitive by comparing the set of pairs.
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["ex:cam", "5 10"],
            ["ex:brian", "7"],
            ["ex:alice", "9 42 76"],
            ["ex:liam", "11 42"]
        ]))
    );
}

#[tokio::test]
async fn aggregates_groupconcat_custom_separator() {
    // Scenario: groupconcat with explicit separator ", "
    // Equivalent syntax: :select ['?s '(groupconcat ?favNums ", ")]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-gc2:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?s", "(as (groupconcat ?favNums \", \") ?nums)"],
        "where": [{"@id": "?s", "ex:favNums": "?favNums"}],
        "groupBy": ["?s"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["ex:cam", "5, 10"],
            ["ex:brian", "7"],
            ["ex:alice", "9, 42, 76"],
            ["ex:liam", "11, 42"]
        ]))
    );
}

#[tokio::test]
async fn aggregates_groupby_multiple_vars_with_grouped_selects() {
    // Scenario: "with multiple variables" (groupBy adult/gender, select grouped ?name list)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-multi:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?adult", "?gender", "?name"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?adult", ["expr", ["if", [">=", "?age", 18], "adult", "minor"]]],
            ["bind", "?gender", ["expr", ["if", ["=", "?name", "Alice"], "female", "male"]]]
        ],
        "groupBy": ["?adult", "?gender"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Note: grouped lists have no guaranteed ordering.
    // We normalize by sorting rows, and within each grouped list by JSON string.
    let mut rows = json_rows.as_array().unwrap().clone();
    for row in &mut rows {
        let cols = row.as_array_mut().unwrap();
        if let Some(list) = cols.get_mut(2).and_then(|v| v.as_array_mut()) {
            list.sort_by(|a, b| {
                serde_json::to_string(a)
                    .unwrap()
                    .cmp(&serde_json::to_string(b).unwrap())
            });
        }
    }
    let normalized = JsonValue::Array(rows);

    assert_eq!(
        normalize_rows(&normalized),
        normalize_rows(&json!([
            ["adult", "female", ["Alice"]],
            ["adult", "male", ["Brian", "Cam"]],
            ["minor", "male", ["Liam"]]
        ]))
    );
}

#[tokio::test]
async fn aggregates_count_star() {
    // Scenario: "with count *" - COUNT(*) counts all rows in each group
    // Equivalent syntax: :select '["?adult" "?gender" "(count *)"]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-countstar:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?adult", "?gender", "(as (count *) ?count)"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?adult", ["expr", ["if", [">=", "?age", 18], "adult", "minor"]]],
            ["bind", "?gender", ["expr", ["if", ["=", "?name", "Alice"], "female", "male"]]]
        ],
        "groupBy": ["?adult", "?gender"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["adult", "female", 1],
            ["adult", "male", 2],
            ["minor", "male", 1]
        ]))
    );
}

#[tokio::test]
async fn aggregates_count_star_direct() {
    // Direct COUNT(*) syntax using S-expression - counts all rows in each group
    // Using simple (count *) which auto-generates ?count
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "query/aggregate-countstar-direct:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?adult", "?gender", "(count *)"],
        "where": [
            {"@id": "?s", "schema:name": "?name", "schema:age": "?age"},
            ["bind", "?adult", ["expr", ["if", [">=", "?age", 18], "adult", "minor"]]],
            ["bind", "?gender", ["expr", ["if", ["=", "?name", "Alice"], "female", "male"]]]
        ],
        "groupBy": ["?adult", "?gender"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Same expected results as the emulated version
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([
            ["adult", "female", 1],
            ["adult", "male", 2],
            ["minor", "male", 1]
        ]))
    );
}
