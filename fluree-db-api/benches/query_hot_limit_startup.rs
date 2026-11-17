//! Hot-cache latency of `LIMIT` queries that should stop early.
//!
//! A small `LIMIT` gives joins a startup goal, and a star probes its subjects
//! in windows sized from the `LIMIT` and then from the rows found so far.
//! Getting either wrong costs a multiple:
//!
//! 1. **`chain_limit`** — two-hop chain, `LIMIT 1`. The startup goal lets the
//!    join hand back its first row instead of buffering the whole join.
//! 2. **`chain_distinct_limit`** — the same through `DISTINCT`.
//! 3. **`chain_distinct_drain`** — `DISTINCT` over a chain whose `LIMIT` can
//!    never be reached (twice the cities there are). Statistics say so, so the
//!    join keeps its throughput plan rather than the startup one.
//! 4. **`star_limit`** — same-subject star, `LIMIT 10`, every subject matching:
//!    the property join's first window is the `LIMIT`.
//! 5. **`star_selective_limit`** — the same star filtered to one person in a
//!    hundred, with a `LIMIT` that needs about a tenth of the people. Later
//!    windows are sized from the yield so far; a fixed eightfold step would
//!    overshoot the rows still needed several times over.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_persons, each with a name, an age, a city and
//!              three `ex:knows` edges
//!              (Tiny=2_000, Small=10_000, Medium=50_000, Large=200_000)
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_limit_startup
//!   cargo bench -p fluree-db-api --bench query_hot_limit_startup -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_limit_startup

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use std::fmt::Write as _;

/// `ex:knows` edges per person.
const KNOWS: usize = 3;

fn scale_n_persons(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 2_000,
        BenchScale::Small => 10_000,
        BenchScale::Medium => 50_000,
        BenchScale::Large => 200_000,
    }
}

fn cities(n_persons: usize) -> usize {
    (n_persons / 100).max(1)
}

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

fn queries(n_persons: usize) -> Vec<(&'static str, String)> {
    vec![
        (
            "chain_limit",
            "SELECT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1".to_string(),
        ),
        (
            "chain_distinct_limit",
            "SELECT DISTINCT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1"
                .to_string(),
        ),
        (
            "chain_distinct_drain",
            format!(
                "SELECT DISTINCT ?city WHERE {{ ?p ex:knows ?f . ?f ex:livesIn ?city }} LIMIT {}",
                2 * cities(n_persons)
            ),
        ),
        (
            "star_limit",
            "SELECT ?p ?n WHERE { ?p a ex:Person ; ex:name ?n ; ex:age ?a } LIMIT 10".to_string(),
        ),
        (
            // Names end in "42" for one person in a hundred.
            "star_selective_limit",
            format!(
                "SELECT ?p ?n WHERE {{ ?p a ex:Person ; ex:name ?n ; ex:age ?a \
                 FILTER(STRENDS(?n, \"42\")) }} LIMIT {}",
                (n_persons / 1000).max(1)
            ),
        ),
    ]
    .into_iter()
    .map(|(name, query)| (name, format!("{PREFIX}{query}")))
    .collect()
}

/// A deterministic index in `0..n` for person `i`'s `k`th draw.
fn pick(i: usize, k: usize, n: usize) -> usize {
    let x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (k as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    ((x ^ (x >> 29)) % n as u64) as usize
}

fn people_turtle(n_persons: usize) -> String {
    let cities = cities(n_persons);
    let mut ttl = String::with_capacity(n_persons * 180);
    ttl.push_str("@prefix ex: <http://example.org/> .\n");
    for c in 0..cities {
        let _ = writeln!(ttl, "ex:city{c} a ex:City .");
    }
    for i in 0..n_persons {
        let _ = write!(
            ttl,
            "ex:person{i} a ex:Person ; ex:name \"Person {i}\" ; ex:age {} ; ex:livesIn ex:city{}",
            18 + pick(i, 0, 73),
            pick(i, 1, cities)
        );
        for k in 0..KNOWS {
            let _ = write!(ttl, " ; ex:knows ex:person{}", pick(i, 2 + k, n_persons));
        }
        ttl.push_str(" .\n");
    }
    ttl
}

/// Populated, indexed file-backed Fluree (same discipline as
/// `query_hot_optional.rs`).
async fn setup_indexed(n_persons: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-limit-startup");
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
            &people_turtle(n_persons),
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

fn bench_query_hot_limit_startup(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_persons = scale_n_persons(scale);

    eprintln!(
        "  [query_hot_limit_startup] scale={} n_persons={n_persons}",
        scale.as_str()
    );

    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_persons));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_limit_startup");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, query) in queries(n_persons) {
        group.bench_with_input(
            BenchmarkId::new(name, scale.as_str()),
            &n_persons,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = snapshot
                            .query()
                            .sparql(&query)
                            .execute()
                            .await
                            .unwrap_or_else(|e| panic!("{name} execute: {e}"));
                        black_box(result);
                    });
                });
            },
        );
    }

    group.finish();
    drop(snapshot);
    drop(fluree);
}

criterion_group!(benches, bench_query_hot_limit_startup);
criterion_main!(benches);
