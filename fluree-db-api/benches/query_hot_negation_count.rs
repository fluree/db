//! Hot-cache latency of negation and join-count lanes.
//!
//! Each scenario is a lane that answers without materializing joined rows it
//! would only discard. Losing one shows up as a multiple, not a percentage:
//!
//! 1. **`minus_count`** — `COUNT(*)` over `MINUS`. The right side is built in
//!    its own scope and a single shared reference key is held in a compact
//!    subject-ID set (`fluree-db-query/src/minus.rs`).
//! 2. **`not_exists_after_optional`** — `FILTER NOT EXISTS` whose key is left
//!    unbound by an `OPTIONAL` on some rows. The semijoin answers those rows
//!    from a projected key set instead of evaluating the body per row
//!    (`fluree-db-query/src/semijoin.rs`).
//! 3. **`join_count`** — ungrouped `COUNT(*)` over a two-hop join, counted
//!    as matches rather than emitted as rows.
//! 4. **`grouped_join_count`** — grouped `COUNT(*)` over the same join, with
//!    matches folded into group totals on the driving side.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_persons, each with an age, a city, three
//!              `ex:knows` edges and (four in five) an employer
//!              (Tiny=2_000, Small=10_000, Medium=50_000, Large=200_000)
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_negation_count
//!   cargo bench -p fluree-db-api --bench query_hot_negation_count -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_negation_count

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

const Q_MINUS_COUNT: &str = r"
PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person MINUS { ?p ex:worksFor ?org } }
";

const Q_NOT_EXISTS_AFTER_OPTIONAL: &str = r"
PREFIX ex: <http://example.org/>
SELECT ?p ?org WHERE {
  ?p a ex:Person
  OPTIONAL { ?p ex:worksFor ?org }
  FILTER NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org }
}
";

const Q_JOIN_COUNT: &str = r"
PREFIX ex: <http://example.org/>
SELECT (COUNT(*) AS ?n) WHERE {
  ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof FILTER(?fof != ?p)
}
";

const Q_GROUPED_JOIN_COUNT: &str = r"
PREFIX ex: <http://example.org/>
SELECT ?city (COUNT(*) AS ?n) WHERE {
  ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof
} GROUP BY ?city
";

/// A deterministic index in `0..n` for person `i`'s `k`th draw.
fn pick(i: usize, k: usize, n: usize) -> usize {
    let x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (k as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    ((x ^ (x >> 29)) % n as u64) as usize
}

/// One city per hundred people and one employer per twenty; every fifth
/// person has no employer, so `MINUS` and the `OPTIONAL` both have misses.
fn people_turtle(n_persons: usize) -> String {
    let cities = (n_persons / 100).max(1);
    let orgs = (n_persons / 20).max(1);
    let mut ttl = String::with_capacity(n_persons * 200);
    ttl.push_str("@prefix ex: <http://example.org/> .\n");
    for c in 0..cities {
        let _ = writeln!(ttl, "ex:city{c} a ex:City .");
    }
    for o in 0..orgs {
        let _ = writeln!(ttl, "ex:org{o} a ex:Org ; ex:name \"Org {o}\" .");
    }
    for i in 0..n_persons {
        let _ = write!(
            ttl,
            "ex:person{i} a ex:Person ; ex:name \"Person {i}\" ; ex:age {} ; ex:livesIn ex:city{}",
            18 + pick(i, 0, 73),
            pick(i, 1, cities)
        );
        if i % 5 != 0 {
            let _ = write!(ttl, " ; ex:worksFor ex:org{}", pick(i, 2, orgs));
        }
        for k in 0..KNOWS {
            let _ = write!(ttl, " ; ex:knows ex:person{}", pick(i, 3 + k, n_persons));
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

    let alias = next_ledger_alias("query-hot-negation-count");
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

fn bench_query_hot_negation_count(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_persons = scale_n_persons(scale);

    eprintln!(
        "  [query_hot_negation_count] scale={} n_persons={n_persons}",
        scale.as_str()
    );

    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_persons));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_negation_count");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, query) in [
        ("minus_count", Q_MINUS_COUNT),
        ("not_exists_after_optional", Q_NOT_EXISTS_AFTER_OPTIONAL),
        ("join_count", Q_JOIN_COUNT),
        ("grouped_join_count", Q_GROUPED_JOIN_COUNT),
    ] {
        group.bench_with_input(
            BenchmarkId::new(name, scale.as_str()),
            &n_persons,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = snapshot
                            .query()
                            .sparql(query)
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

criterion_group!(benches, bench_query_hot_negation_count);
criterion_main!(benches);
