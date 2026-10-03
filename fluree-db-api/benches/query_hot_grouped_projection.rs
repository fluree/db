//! Hot-cache cost of grouped queries that project a SELECT expression.
//!
//! A SELECT expression of a grouped query level is an `Extend` over the group
//! rows (SPARQL 1.1 §18.2.4.4): evaluated once per group, after aggregation
//! and HAVING. Every scenario below projects one, and each is measured end to
//! end — execution plus the SPARQL-JSON rendering a client receives — because
//! a per-row evaluation of the expression shows up in both: the grouping stage
//! has to carry every input row, and the rendered result grows with it.
//! Allocation peak and churn are recorded next to the timings (this bench
//! installs the tracking allocator).
//!
//! ## Scenarios
//!
//! Fixture: `n` entities `ex:eN ex:area "A{N mod g}"`, so `g` groups of
//! `n / g` entities each.
//!
//! 1. **`key_count`** — `?a (COUNT(?e) AS ?n) … GROUP BY ?a`: the control.
//!    The expression scenarios should cost about what this costs.
//! 2. **`expr_count`** — `(IF(?a = "A1", …) AS ?seg) (COUNT(?e) AS ?n) …
//!    GROUP BY ?a` (#1978).
//! 3. **`expr_min`** — the same expression with `MIN(?e)`, a
//!    duplicate-insensitive aggregate (WHERE-level early dedup applies).
//! 4. **`dedup_expr`** — the expression with no aggregate: a dedup-only
//!    `GROUP BY ?a`.
//! 5. **`implicit_const_count`** — `("x" AS ?c) (COUNT(*) AS ?n)` with no
//!    GROUP BY: one implicit group.
//! 6. **`subselect_expr`** — `expr_count` inside a sub-SELECT.
//! 7. **`jsonld_keyonly_expr`** — the JSON-LD twin of `expr_count`, rendered
//!    as JSON-LD.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → entities, `entities / 200` groups
//!              (Tiny=2_000, Small=20_000, Medium=100_000, Large=200_000),
//!              one transaction, then indexed
//!   metric:    ns/query (criterion); peak and total bytes (mem sidecar)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_grouped_projection
//!   cargo bench -p fluree-db-api --bench query_hot_grouped_projection -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_grouped_projection

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_alloc::TrackingAllocator;
use fluree_bench_support::mem::{record_scenario, MemMetrics};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig, TxnOpts};
use serde_json::{json, Value as JsonValue};
use std::fmt::Write as _;

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator::new();

const GROUP: &str = "query_hot_grouped_projection";

/// Entities per group.
const GROUP_SIZE: usize = 200;

fn scale_entities(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 2_000,
        BenchScale::Small => 20_000,
        BenchScale::Medium => 100_000,
        BenchScale::Large => 200_000,
    }
}

const Q_KEY_COUNT: &str = r"
PREFIX ex: <http://example.org/>
SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a } GROUP BY ?a
";

const Q_EXPR_COUNT: &str = r#"
PREFIX ex: <http://example.org/>
SELECT (IF(?a = "A1", "first", "other") AS ?seg) (COUNT(?e) AS ?n)
WHERE { ?e ex:area ?a } GROUP BY ?a
"#;

const Q_EXPR_MIN: &str = r#"
PREFIX ex: <http://example.org/>
SELECT (IF(?a = "A1", "first", "other") AS ?seg) (MIN(?e) AS ?m)
WHERE { ?e ex:area ?a } GROUP BY ?a
"#;

const Q_DEDUP_EXPR: &str = r#"
PREFIX ex: <http://example.org/>
SELECT (IF(?a = "A1", "first", "other") AS ?seg)
WHERE { ?e ex:area ?a } GROUP BY ?a
"#;

const Q_IMPLICIT_CONST_COUNT: &str = r#"
PREFIX ex: <http://example.org/>
SELECT ("x" AS ?c) (COUNT(*) AS ?n) WHERE { ?e ex:area ?a }
"#;

const Q_SUBSELECT_EXPR: &str = r#"
PREFIX ex: <http://example.org/>
SELECT ?seg ?n WHERE {
  { SELECT (IF(?a = "A1", "first", "other") AS ?seg) (COUNT(?e) AS ?n)
    WHERE { ?e ex:area ?a } GROUP BY ?a }
}
"#;

fn jsonld_keyonly_expr() -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(as (if (= ?a \"A1\") \"first\" \"other\") ?seg)", "(as (count ?e) ?n)"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    })
}

/// `n` entities spread round-robin over `n / GROUP_SIZE` areas.
fn areas_turtle(n: usize) -> String {
    let groups = (n / GROUP_SIZE).max(1);
    let mut ttl = String::with_capacity(n * 40);
    ttl.push_str("@prefix ex: <http://example.org/> .\n");
    for i in 0..n {
        let _ = writeln!(ttl, "ex:e{i} ex:area \"A{}\" .", i % groups);
    }
    ttl
}

/// Populated, indexed file-backed Fluree (same discipline as
/// `query_hot_negation_count.rs`).
async fn setup_indexed(n: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-grouped-projection");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    // High thresholds so populate doesn't race background indexing; the
    // explicit reindex below builds the index.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            &areas_turtle(n),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("populate insert");

    let _ = fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("reindex");

    (db_dir, fluree, alias)
}

/// One scenario's query and the format its client reads it in.
enum Scenario {
    Sparql(&'static str),
    JsonLd(JsonValue),
}

fn bench_query_hot_grouped_projection(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = scale_entities(scale);

    eprintln!(
        "  [{GROUP}] scale={} entities={n} groups={}",
        scale.as_str(),
        (n / GROUP_SIZE).max(1)
    );

    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n));
    let db = rt.block_on(async { fluree.db(&alias).await.expect("db") });

    let run = |scenario: &Scenario| {
        rt.block_on(async {
            match scenario {
                Scenario::Sparql(q) => db
                    .query(&fluree)
                    .sparql(q)
                    .format(FormatterConfig::sparql_json())
                    .execute_formatted_string()
                    .await
                    .expect("sparql query"),
                Scenario::JsonLd(q) => db
                    .query(&fluree)
                    .jsonld(q)
                    .format(FormatterConfig::jsonld())
                    .execute_formatted_string()
                    .await
                    .expect("jsonld query"),
            }
        })
    };

    let mut group = c.benchmark_group(GROUP);
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, scenario) in [
        ("key_count", Scenario::Sparql(Q_KEY_COUNT)),
        ("expr_count", Scenario::Sparql(Q_EXPR_COUNT)),
        ("expr_min", Scenario::Sparql(Q_EXPR_MIN)),
        ("dedup_expr", Scenario::Sparql(Q_DEDUP_EXPR)),
        (
            "implicit_const_count",
            Scenario::Sparql(Q_IMPLICIT_CONST_COUNT),
        ),
        ("subselect_expr", Scenario::Sparql(Q_SUBSELECT_EXPR)),
        (
            "jsonld_keyonly_expr",
            Scenario::JsonLd(jsonld_keyonly_expr()),
        ),
    ] {
        // One run on its own, so the output size and per-query peak read
        // alongside the timing.
        let reset_base = fluree_bench_alloc::reset_peak();
        let bytes = run(&scenario).len();
        let m = fluree_bench_alloc::snapshot();
        eprintln!(
            "  [{GROUP}] {name}: output={bytes}B peak={}B allocated={}B",
            m.peak_bytes.saturating_sub(reset_base),
            m.total_allocated_bytes
        );

        let reset_base = fluree_bench_alloc::reset_peak();
        group.bench_with_input(BenchmarkId::new(name, scale.as_str()), &n, |b, _| {
            b.iter(|| black_box(run(&scenario).len()));
        });
        let m = fluree_bench_alloc::snapshot();
        record_scenario(
            GROUP,
            &format!("{GROUP}/{name}/{}", scale.as_str()),
            MemMetrics {
                peak_bytes: (m.peak_bytes as u64).saturating_sub(reset_base as u64),
                total_allocated_bytes: m.total_allocated_bytes as u64,
            },
        );
    }

    group.finish();
    drop(db);
    drop(fluree);
}

criterion_group!(benches, bench_query_hot_grouped_projection);
criterion_main!(benches);
