//! Filter query benchmark for characterizing filter optimization performance.
//!
//! Measures query execution time for different filter patterns:
//! - Range filters (pushdown to ObjectBounds)
//! - Complex expression filters (inline evaluation)
//! - Filters before/after triple patterns (dependency injection)
//! - Filters in nested patterns (UNION, OPTIONAL)
//!
//! Run with: cargo run --release -p bench-filter-query

use std::hint::black_box;
use std::time::Instant;

use fluree_db_api::FlureeBuilder;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value as JsonValue};

// ============================================================================
// Type aliases
// ============================================================================

type MemoryFluree = fluree_db_api::Fluree;

// ============================================================================
// Configuration
// ============================================================================

/// Number of entities to create in the test database
const ENTITY_COUNT: usize = 1_000;

/// Number of iterations per benchmark (median is reported)
const ITERATIONS: usize = 5;

/// Number of query executions per iteration (for very fast queries)
const QUERIES_PER_ITERATION: usize = 10;

// ============================================================================
// Data generation
// ============================================================================

/// Generate test data with predictable distribution for benchmarking.
///
/// Creates entities with:
/// - name: "Entity_{i}"
/// - age: random 1-100
/// - score: random 1-1000
/// - category: one of ["A", "B", "C", "D"]
/// - active: boolean
fn generate_test_data(count: usize, seed: u64) -> JsonValue {
    let mut rng = SmallRng::seed_from_u64(seed);
    let categories = ["A", "B", "C", "D"];

    let entities: Vec<JsonValue> = (0..count)
        .map(|i| {
            let category = categories[rng.gen_range(0..categories.len())];
            json!({
                "@id": format!("ex:entity_{i}"),
                "@type": "ex:Entity",
                "ex:name": format!("Entity_{i}"),
                "ex:age": rng.gen_range(1..=100),
                "ex:score": rng.gen_range(1..=1000),
                "ex:category": category,
                "ex:active": rng.gen_bool(0.7)
            })
        })
        .collect();

    json!(entities)
}

// ============================================================================
// Benchmark harness
// ============================================================================

struct BenchResult {
    name: &'static str,
    description: &'static str,
    median_ms: f64,
    min_ms: f64,
    max_ms: f64,
    result_count: usize,
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[mid - 1], values[mid])
    } else {
        values[mid]
    }
}

async fn run_query_bench<F>(
    name: &'static str,
    description: &'static str,
    fluree: &MemoryFluree,
    ledger: &fluree_db_api::LedgerState,
    query_fn: F,
) -> BenchResult
where
    F: Fn() -> JsonValue,
{
    let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
    let mut times: Vec<f64> = Vec::with_capacity(ITERATIONS);
    let mut last_result_count = 0;

    for _ in 0..ITERATIONS {
        let query = query_fn();

        let start = Instant::now();
        for _ in 0..QUERIES_PER_ITERATION {
            let result = fluree.query(&db, &query).await.unwrap();
            last_result_count = result.row_count();
            black_box(&result);
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0 / QUERIES_PER_ITERATION as f64;
        times.push(elapsed);
    }

    let min_ms = times.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_ms = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let median_ms = median(&mut times);

    BenchResult {
        name,
        description,
        median_ms,
        min_ms,
        max_ms,
        result_count: last_result_count,
    }
}

fn print_results(results: &[BenchResult]) {
    println!("\n{}", "=".repeat(100));
    println!("  Filter Query Benchmark Results");
    println!("{}", "=".repeat(100));
    println!(
        "  {:<30} {:>12} {:>12} {:>12} {:>10}",
        "Benchmark", "Median", "Min", "Max", "Rows"
    );
    println!("  {}", "-".repeat(80));

    for r in results {
        println!(
            "  {:<30} {:>10.3}ms {:>10.3}ms {:>10.3}ms {:>10}",
            r.name, r.median_ms, r.min_ms, r.max_ms, r.result_count
        );
        println!("    {}", r.description);
    }

    println!("  {}", "-".repeat(80));
    println!();

    // Analysis section
    println!("Analysis:");
    println!("  - Queries returning 0 rows may indicate filter/parser issues");
    println!("  - Queries >100ms for 1000 entities warrant investigation");
    println!("  - Range filters should use ObjectBounds pushdown (similar to baseline)");
    println!("  - Multi-property patterns use property joins (can be slow)");
    println!();
}

// ============================================================================
// Benchmark queries
// ============================================================================

fn context() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Baseline: no filter
fn query_no_filter() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age"],
        "where": {"@id": "?s", "ex:age": "?age"}
    })
}

/// Simple range filter: age > 50
fn query_range_filter_gt() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age"],
        "where": [
            {"@id": "?s", "ex:age": "?age"},
            ["filter", "(> ?age 50)"]
        ]
    })
}

/// Range filter with two bounds: 30 <= age <= 60
fn query_range_filter_between() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age"],
        "where": [
            {"@id": "?s", "ex:age": "?age"},
            ["filter", "(and (>= ?age 30) (<= ?age 60))"]
        ]
    })
}

/// Complex filter: multiple conditions on different variables
fn query_complex_filter() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age", "ex:score": "?score"},
            ["filter", "(and (> ?age 25) (< ?score 500))"]
        ]
    })
}

/// Filter before triple pattern (tests dependency injection)
fn query_filter_before_triple() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age"],
        "where": [
            ["filter", "(> ?age 50)"],
            {"@id": "?s", "ex:age": "?age"}
        ]
    })
}

/// Filter referencing multiple patterns (applied after all vars bound)
fn query_filter_multi_pattern() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age"},
            ["filter", "(and (> ?age 25) (< ?score 500))"],
            {"@id": "?s", "ex:score": "?score"}
        ]
    })
}

/// String filter: category = "A"
fn query_string_equality_filter() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?cat"],
        "where": [
            {"@id": "?s", "ex:category": "?cat"},
            ["filter", "(= ?cat \"A\")"]
        ]
    })
}

/// Boolean filter: active = true
fn query_boolean_filter() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?active"],
        "where": [
            {"@id": "?s", "ex:active": "?active"},
            ["filter", "?active"]
        ]
    })
}

/// OPTIONAL with filter inside
fn query_optional_with_filter() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age"},
            ["filter", "(> ?age 50)"],
            ["optional", {"@id": "?s", "ex:score": "?score"}]
        ]
    })
}

/// Filter after OPTIONAL (references optional variable)
fn query_filter_after_optional() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age"},
            ["optional", {"@id": "?s", "ex:score": "?score"}],
            ["filter", "(or (not (bound ?score)) (> ?score 500))"]
        ]
    })
}

/// Arithmetic filter expression
fn query_arithmetic_filter() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age", "ex:score": "?score"},
            ["filter", "(> (+ ?age ?score) 100)"]
        ]
    })
}

/// Multiple independent filters (should be combined)
fn query_multiple_filters() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age", "ex:score": "?score"},
            ["filter", "(> ?age 25)"],
            ["filter", "(< ?score 800)"],
            ["filter", "(> ?score 200)"]
        ]
    })
}

/// Highly selective filter (returns few rows)
fn query_highly_selective() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age", "?score"],
        "where": [
            {"@id": "?s", "ex:age": "?age", "ex:score": "?score"},
            ["filter", "(and (= ?age 42) (> ?score 900))"]
        ]
    })
}

/// Non-selective filter (returns most rows)
fn query_non_selective() -> JsonValue {
    json!({
        "@context": context(),
        "select": ["?s", "?age"],
        "where": [
            {"@id": "?s", "ex:age": "?age"},
            ["filter", "(> ?age 0)"]
        ]
    })
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    println!("Filter Query Benchmark");
    println!("======================");
    println!();
    println!("Configuration:");
    println!("  Entity count: {ENTITY_COUNT}");
    println!("  Iterations per benchmark: {ITERATIONS}");
    println!("  Queries per iteration: {QUERIES_PER_ITERATION}");
    println!();

    // Create database and load test data
    println!("Creating test database...");
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("bench/filter:main").await.unwrap();

    println!("Generating {ENTITY_COUNT} entities...");
    let data = generate_test_data(ENTITY_COUNT, 42);

    let insert = json!({
        "@context": context(),
        "@graph": data
    });

    println!("Inserting data...");
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;
    println!("Data loaded.\n");

    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger);

    // Warmup
    println!("Warming up...");
    for _ in 0..3 {
        let _ = fluree.query(&db, &query_no_filter()).await;
        let _ = fluree.query(&db, &query_range_filter_gt()).await;
    }
    println!("Warmup complete.\n");

    // Run benchmarks
    println!("Running benchmarks...");
    let mut results: Vec<BenchResult> = Vec::new();

    // Baseline
    results.push(
        run_query_bench(
            "no_filter",
            "Baseline: scan all entities",
            &fluree,
            &ledger,
            query_no_filter,
        )
        .await,
    );

    // Range filters (can use ObjectBounds pushdown)
    results.push(
        run_query_bench(
            "range_gt",
            "Range filter: age > 50 (ObjectBounds pushdown)",
            &fluree,
            &ledger,
            query_range_filter_gt,
        )
        .await,
    );

    results.push(
        run_query_bench(
            "range_between",
            "Range filter: 30 <= age <= 60 (ObjectBounds pushdown)",
            &fluree,
            &ledger,
            query_range_filter_between,
        )
        .await,
    );

    // Complex filters (inline evaluation)
    results.push(
        run_query_bench(
            "complex_multi_var",
            "Complex: age > 25 AND score < 500 (inline eval)",
            &fluree,
            &ledger,
            query_complex_filter,
        )
        .await,
    );

    // Filter placement tests
    results.push(
        run_query_bench(
            "filter_before_triple",
            "Filter before triple (dependency injection)",
            &fluree,
            &ledger,
            query_filter_before_triple,
        )
        .await,
    );

    results.push(
        run_query_bench(
            "filter_multi_pattern",
            "Filter between patterns (deferred application)",
            &fluree,
            &ledger,
            query_filter_multi_pattern,
        )
        .await,
    );

    // Type-specific filters
    results.push(
        run_query_bench(
            "string_equality",
            "String filter: category = 'A'",
            &fluree,
            &ledger,
            query_string_equality_filter,
        )
        .await,
    );

    results.push(
        run_query_bench(
            "boolean_filter",
            "Boolean filter: active = true",
            &fluree,
            &ledger,
            query_boolean_filter,
        )
        .await,
    );

    // OPTIONAL interactions
    results.push(
        run_query_bench(
            "optional_with_filter",
            "Filter + OPTIONAL pattern",
            &fluree,
            &ledger,
            query_optional_with_filter,
        )
        .await,
    );

    results.push(
        run_query_bench(
            "filter_after_optional",
            "Filter on optional variable (BOUND check)",
            &fluree,
            &ledger,
            query_filter_after_optional,
        )
        .await,
    );

    // Expression complexity
    results.push(
        run_query_bench(
            "arithmetic_filter",
            "Arithmetic: age + score > 100",
            &fluree,
            &ledger,
            query_arithmetic_filter,
        )
        .await,
    );

    results.push(
        run_query_bench(
            "multiple_filters",
            "Multiple separate FILTER clauses",
            &fluree,
            &ledger,
            query_multiple_filters,
        )
        .await,
    );

    // Selectivity tests
    results.push(
        run_query_bench(
            "highly_selective",
            "Highly selective: age=42 AND score>900",
            &fluree,
            &ledger,
            query_highly_selective,
        )
        .await,
    );

    results.push(
        run_query_bench(
            "non_selective",
            "Non-selective: age > 0 (matches all)",
            &fluree,
            &ledger,
            query_non_selective,
        )
        .await,
    );

    // Print results
    print_results(&results);

    println!("Done.");
}
