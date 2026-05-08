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
        "select": "(count ?name)",
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
        "select": "(min ?nums)",
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
        "select": "(max ?birthDate)",
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
        "select": "(count ?favNums)",
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

// =============================================================================
// Inline aggregates inside BIND expressions
//
// JSON-LD parity for SPARQL's `SELECT (expr_with_aggregates AS ?alias)`. When
// a JSON-LD WHERE BIND references an aggregate function like `["min", "?p"]`
// (B1 data form) or `"(min ?p)"` (B2 string form), the aggregate is hoisted
// into `QueryOptions.aggregates` with a synthetic `?__bind_agg{N}` alias and
// the BIND is routed to post-aggregation execution via
// `QueryOptions.post_binds`. See where_clause.rs::"bind" for the wiring.
//
// Test data is uniform-integer typed so these tests do not depend on the
// arithmetic type-promotion fixes tracked separately (db-r#50,
// W3C type-promotion-01..22).
// =============================================================================

async fn seed_uniform_ints(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:gx", "ex:p": [1, 2, 3, 4]},
            {"@id": "ex:gy", "ex:p": [10, 20, 30, 40]}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

#[tokio::test]
async fn aggregates_inline_control_explicit_select_agg() {
    // Control: same data shape as aggregates_inline_*, but uses select-clause
    // aggregate (existing path) instead of BIND inline. Confirms `?p` is
    // bound by the array-form WHERE node-map and reachable to AggregateOperator.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_uniform_ints(&fluree, "query/agg-inline-control:main").await;
    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["?g", "(as (min ?p) ?min)"],
        "where": [{"@id": "?g", "ex:p": "?p"}],
        "groupBy": ["?g"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("control query should succeed");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let mut rows = json_rows.as_array().expect("rows array").clone();
    rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], json!(1));
    assert_eq!(rows[1][1], json!(10));
}

#[tokio::test]
async fn aggregates_inline_in_bind_data_form() {
    // B1 form — JSON-array nested expression. Equivalent SPARQL:
    //   SELECT ?g ((MIN(?p) + MAX(?p)) / 2 AS ?c) WHERE { ?g :p ?p } GROUP BY ?g
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_uniform_ints(&fluree, "query/agg-inline-b1:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?g", "?c"],
        "where": [
            {"@id": "?g", "ex:p": "?p"},
            ["bind", "?c", ["/", ["+", ["min", "?p"], ["max", "?p"]], 2]]
        ],
        "groupBy": ["?g"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // ex:gx values [1,2,3,4]: (1+4)/2 = 2; ex:gy values [10,20,30,40]: (10+40)/2 = 25.
    let mut rows = json_rows.as_array().expect("rows array").clone();
    rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], json!(2));
    assert_eq!(rows[1][1], json!(25));
}

#[tokio::test]
async fn aggregates_inline_in_bind_sexpr_form() {
    // B2 form — string s-expression. Same shape as B1, different surface.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_uniform_ints(&fluree, "query/agg-inline-b2:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?g", "?c"],
        "where": [
            {"@id": "?g", "ex:p": "?p"},
            ["bind", "?c", "(/ (+ (min ?p) (max ?p)) 2)"]
        ],
        "groupBy": ["?g"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    let mut rows = json_rows.as_array().expect("rows array").clone();
    rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], json!(2));
    assert_eq!(rows[1][1], json!(25));
}

#[tokio::test]
async fn aggregates_inline_groupconcat_b1() {
    // B1 form — GROUP_CONCAT with explicit separator. Each group gets a
    // single comma-separated string of its ?p values.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_uniform_ints(&fluree, "query/agg-inline-gc-b1:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?g", "?cs"],
        "where": [
            {"@id": "?g", "ex:p": "?p"},
            ["bind", "?cs", ["group-concat", "?p", ", "]]
        ],
        "groupBy": ["?g"]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    let mut rows = json_rows.as_array().expect("rows array").clone();
    rows.sort_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
    assert_eq!(rows.len(), 2);
    // GROUP_CONCAT does not guarantee ordering within a group; verify each
    // result string contains all expected values separated by ", ".
    for (row, expected_values) in rows
        .iter()
        .zip([vec!["1", "2", "3", "4"], vec!["10", "20", "30", "40"]])
    {
        let s = row[1].as_str().expect("group-concat result is a string");
        for v in &expected_values {
            assert!(s.contains(v), "expected {v} in group-concat result {s:?}");
        }
        let actual_count = s.matches(", ").count() + 1;
        assert_eq!(
            actual_count,
            expected_values.len(),
            "group-concat string {s:?} should have {} comma-separated parts",
            expected_values.len()
        );
    }
}

#[tokio::test]
async fn aggregates_inline_rejects_nested_aggregate_b1() {
    // ["min", ["max", "?p"]] is rejected because the outer MIN's input
    // arg is a Call (the inner aggregate), not a variable. SPARQL §18.5
    // forbids nested aggregates.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_uniform_ints(&fluree, "query/agg-inline-nested:main").await;

    let query = json!({
        "@context": context_ex_schema(),
        "select": ["?g", "?c"],
        "where": [
            {"@id": "?g", "ex:p": "?p"},
            ["bind", "?c", ["min", ["max", "?p"]]]
        ],
        "groupBy": ["?g"]
    });

    let err = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("variable") || msg.to_lowercase().contains("nested aggregate"),
        "expected error mentioning variable/nested-aggregate input, got: {msg}"
    );
}

// =============================================================================
// SUM / AVG numeric type promotion (W3C SPARQL §17.4.1.7)
//
// Parity for db-r#62: aggregate output type tracks the widest tier observed
// across the group: integer → decimal → float → double. AVG is one twist —
// an all-integer group widens to xsd:decimal because SPARQL's integer ÷
// integer is decimal-typed. Empty AVG returns 0 xsd:integer per W3C
// agg-avg-03.
// =============================================================================

async fn seed_typed_numerics(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();
    // Three subjects each carrying values of a uniform numeric type:
    //  ex:ints      → xsd:integer values 1, 2, 3
    //  ex:decs      → xsd:decimal values 1.0, 2.2, 3.5
    //  ex:doubles   → xsd:double  values 100, 2000, 30000 (scientific)
    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id": "ex:ints", "ex:n": [1, 2, 3]},
            {"@id": "ex:decs", "ex:n": [
                {"@value": "1.0", "@type": "xsd:decimal"},
                {"@value": "2.2", "@type": "xsd:decimal"},
                {"@value": "3.5", "@type": "xsd:decimal"}
            ]},
            {"@id": "ex:doubles", "ex:n": [
                {"@value": "1.0E2", "@type": "xsd:double"},
                {"@value": "2.0E3", "@type": "xsd:double"},
                {"@value": "3.0E4", "@type": "xsd:double"}
            ]}
        ]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

#[tokio::test]
async fn aggregates_sum_preserves_xsd_integer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed_numerics(&fluree, "query/agg-tiers-sum-int:main").await;
    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["?s", "(as (sum ?n) ?total)"],
        "where": {"@id": "?s", "ex:n": "?n"},
        "groupBy": ["?s"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let typed = result.to_typed_json(&ledger.snapshot).expect("typed json");
    let arr = typed.as_array().expect("rows");
    // Find the row for ex:ints; assert its ?total is xsd:integer 6.
    let ints_row = arr
        .iter()
        .find(|r| {
            r.as_object()
                .and_then(|o| o.get("?s"))
                .and_then(|v| v.get("@id"))
                .and_then(|s| s.as_str())
                .map(|s| s.ends_with(":ints") || s.ends_with("/ints"))
                .unwrap_or(false)
        })
        .expect("ex:ints row");
    let total = ints_row
        .as_object()
        .and_then(|o| o.get("?total"))
        .expect("?total binding");
    let total_obj = total
        .as_object()
        .expect("integer rendered as @value object");
    assert_eq!(
        total_obj.get("@type").and_then(|v| v.as_str()),
        Some("xsd:integer"),
        "SUM of integers should retain xsd:integer datatype, got {total:?}"
    );
    assert_eq!(total_obj.get("@value"), Some(&json!(6)));
}

#[tokio::test]
async fn aggregates_sum_decimal_returns_xsd_decimal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed_numerics(&fluree, "query/agg-tiers-sum-dec:main").await;
    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["?s", "(as (sum ?n) ?total)"],
        "where": {"@id": "?s", "ex:n": "?n"},
        "groupBy": ["?s"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let typed = result.to_typed_json(&ledger.snapshot).expect("typed json");
    let arr = typed.as_array().expect("rows");
    let dec_row = arr
        .iter()
        .find(|r| {
            r.as_object()
                .and_then(|o| o.get("?s"))
                .and_then(|v| v.get("@id"))
                .and_then(|s| s.as_str())
                .map(|s| s.ends_with(":decs") || s.ends_with("/decs"))
                .unwrap_or(false)
        })
        .expect("ex:decs row");
    let total = dec_row
        .as_object()
        .and_then(|o| o.get("?total"))
        .expect("?total binding");
    let total_obj = total.as_object().expect("decimal rendered as object");
    assert_eq!(
        total_obj.get("@type").and_then(|v| v.as_str()),
        Some("xsd:decimal"),
        "SUM of decimals should be xsd:decimal, got {total:?}"
    );
}

#[tokio::test]
async fn aggregates_avg_of_integers_returns_xsd_decimal() {
    // SPARQL §17.4.1.7: AVG of integers widens to xsd:decimal because
    // integer ÷ integer is decimal-typed in the spec's division semantics.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed_numerics(&fluree, "query/agg-tiers-avg-int:main").await;
    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["?s", "(as (avg ?n) ?avg)"],
        "where": {"@id": "?s", "ex:n": "?n"},
        "groupBy": ["?s"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let typed = result.to_typed_json(&ledger.snapshot).expect("typed json");
    let arr = typed.as_array().expect("rows");
    let ints_row = arr
        .iter()
        .find(|r| {
            r.as_object()
                .and_then(|o| o.get("?s"))
                .and_then(|v| v.get("@id"))
                .and_then(|s| s.as_str())
                .map(|s| s.ends_with(":ints") || s.ends_with("/ints"))
                .unwrap_or(false)
        })
        .expect("ex:ints row");
    let avg = ints_row
        .as_object()
        .and_then(|o| o.get("?avg"))
        .expect("?avg binding");
    let avg_obj = avg.as_object().expect("decimal rendered as object");
    assert_eq!(
        avg_obj.get("@type").and_then(|v| v.as_str()),
        Some("xsd:decimal"),
        "AVG of integers should be xsd:decimal, got {avg:?}"
    );
}

#[tokio::test]
async fn aggregates_sum_double_returns_xsd_double() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed_numerics(&fluree, "query/agg-tiers-sum-dbl:main").await;
    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["?s", "(as (sum ?n) ?total)"],
        "where": {"@id": "?s", "ex:n": "?n"},
        "groupBy": ["?s"]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let typed = result.to_typed_json(&ledger.snapshot).expect("typed json");
    let arr = typed.as_array().expect("rows");
    let dbl_row = arr
        .iter()
        .find(|r| {
            r.as_object()
                .and_then(|o| o.get("?s"))
                .and_then(|v| v.get("@id"))
                .and_then(|s| s.as_str())
                .map(|s| s.ends_with(":doubles") || s.ends_with("/doubles"))
                .unwrap_or(false)
        })
        .expect("ex:doubles row");
    let total = dbl_row
        .as_object()
        .and_then(|o| o.get("?total"))
        .expect("?total binding");
    let total_obj = total.as_object().expect("double rendered as object");
    assert_eq!(
        total_obj.get("@type").and_then(|v| v.as_str()),
        Some("xsd:double"),
        "SUM of doubles should be xsd:double, got {total:?}"
    );
}

#[tokio::test]
async fn aggregates_avg_empty_group_returns_zero_xsd_integer() {
    // W3C SPARQL agg-avg-03: AVG of empty input → 0 xsd:integer.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_uniform_ints(&fluree, "query/agg-tiers-avg-empty:main").await;
    let ctx = context_ex_schema();
    // ex:nope predicate is not present in any data → empty input to AVG.
    let query = json!({
        "@context": ctx,
        "select": "(avg ?n)",
        "where": {"ex:nope": "?n"}
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        json_rows,
        json!([0]),
        "AVG of empty input should be 0 xsd:integer"
    );
}
