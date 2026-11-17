//! Time travel to a `t` the index covers, while commits after the index are
//! not yet indexed.
//!
//! A read as of such a `t` answers from the index, but it must still learn of
//! any graph a commit after the index dropped: a drop takes what the graph
//! held as of every earlier `t`. The cached head already knows those drops,
//! so the read takes them from it. Without a head at that commit, it reads
//! one commit envelope per unindexed commit for them, which is serial (each
//! envelope names its parent) and, on object storage, one GET each.
//!
//! "Indexing lags the head" is the normal state of a server under write load,
//! so this bench guards the read against paying per unindexed commit again.
//!
//! ## Scenarios
//!
//! 1. `as_of_index` — `db_at(t = index_t)` plus a one-pattern query, through
//!    the cached head. Should stay flat as unindexed commits grow.
//! 2. `as_of_index_no_head` — `ledger_view_at(t = index_t)`, the loader with
//!    no head at hand (as for a handle another process left stale). Grows
//!    with the unindexed commits; it is the cost scenario 1 avoids.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → unindexed commits after the index
//!              (Tiny=50, Small=200, Medium=500, Large=2000)
//!   metric:    ns/read
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_time_travel_below_index
//!   cargo bench -p fluree-db-api --bench query_time_travel_below_index -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_time_travel_below_index

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions, TimeSpec};

fn unindexed_commits(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 50,
        BenchScale::Small => 200,
        BenchScale::Medium => 500,
        BenchScale::Large => 2_000,
    }
}

const QUERY: &str = "SELECT ?o WHERE { <http://example.org/s0> <http://example.org/p> ?o }";

/// A file-backed ledger indexed at its first commit, with `commits` more
/// after the index. Returns the ledger and the index's `t`.
async fn populate(fluree: &Fluree, commits: usize) -> (String, i64) {
    let alias = next_ledger_alias("time-travel-below-index");
    fluree.create_ledger(&alias).await.expect("create_ledger");
    let insert = |i: usize| format!("<http://example.org/s{i}> <http://example.org/p> {i} .");
    fluree
        .graph(&alias)
        .transact()
        .insert_turtle(&insert(0))
        .commit()
        .await
        .expect("first commit");
    fluree
        .reindex(&alias, ReindexOptions::default())
        .await
        .expect("index the first commit");
    let index_t = fluree.ledger(&alias).await.expect("load").t();
    for i in 1..=commits {
        fluree
            .graph(&alias)
            .transact()
            .insert_turtle(&insert(i))
            .commit()
            .await
            .expect("unindexed commit");
    }
    (alias, index_t)
}

fn bench_query_time_travel_below_index(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let commits = unindexed_commits(scale);

    eprintln!(
        "  [query_time_travel_below_index] scale={} unindexed_commits={commits}",
        scale.as_str()
    );

    let _runtime = rt.enter();
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build file-backed Fluree");
    let (alias, index_t) = rt.block_on(populate(&fluree, commits));

    let mut group = c.benchmark_group("query_time_travel_below_index");
    group.sample_size(profile.sample_size());

    group.bench_with_input(
        BenchmarkId::new("as_of_index", scale.as_str()),
        &index_t,
        |b, &t| {
            b.iter(|| {
                rt.block_on(async {
                    let db = fluree
                        .db_at(&alias, TimeSpec::AtT(t))
                        .await
                        .expect("view as of the index");
                    let result = fluree.query(&db, QUERY).await.expect("query");
                    black_box(result);
                });
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("as_of_index_no_head", scale.as_str()),
        &index_t,
        |b, &t| {
            b.iter(|| {
                rt.block_on(async {
                    let view = fluree
                        .ledger_view_at(&alias, t)
                        .await
                        .expect("view as of the index");
                    black_box(view);
                });
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_query_time_travel_below_index);
criterion_main!(benches);
