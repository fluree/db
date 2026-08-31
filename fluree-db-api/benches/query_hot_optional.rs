//! Hot-cache latency of the four `OPTIONAL` execution lanes.
//!
//! `OptionalOperator` (`fluree-db-query/src/optional.rs`) is a hot operator
//! with four builders, each with its own admission gates, its own batched
//! probe, and its own fall-back to a per-row subplan rebuild — and until this
//! bench, nothing in `regression-budget.json` covered any of it. The lanes are
//! separated deliberately: a change that widens one builder's decline
//! condition moves only that scenario, and the per-row rebuild it falls back
//! to is an order of magnitude slower than the batched probe it replaces, so
//! the signal is unmissable when it happens.
//!
//! 1. **`single_triple_probe`** — `OPTIONAL { ?s ex:email ?e }` with a
//!    left-bound subject. `PatternOptionalBuilder::build_batch`'s batched
//!    subject probe, plus the result cache on the fan-out.
//! 2. **`object_correlated`** — `?s ex:tag ?t . OPTIONAL { ?s ex:altTag ?t }`.
//!    The object variable is shared with the required side, so the batched
//!    probe (which reads the object off the plan-time template) is declined
//!    and the per-row substituted scan runs instead. This is also the shape
//!    whose result-cache key must key the pushed-down literal, so it is where
//!    a cache-hit-rate change shows up.
//! 3. **`multi_pattern_hash_join`** — a two-pattern correlated `OPTIONAL`
//!    routing to `PlanTreeOptionalBuilder`'s batched hash left-join: one inner
//!    scan for the whole coalesced driving side, partitioned by correlation
//!    key.
//! 4. **`unbound_filter_operand`** — the same hash-join lane, but a `UNION`
//!    leaves the `FILTER`'s operand unbound on half the driving rows. The
//!    operand is a correlation variable the inner can only READ, so those rows
//!    must stay on the batched lane; if the lane's unbound-correlation bail
//!    ever widens back to every correlation column, the whole driving side
//!    falls to the per-row rebuild and this scenario blows out by an order of
//!    magnitude (measured 46.7s → 555.2s on a 7,500-row driving side, debug).
//!
//! ## Setup discipline
//!
//! Mirrors `query_hot_property_path.rs`: build once per scale, populate a
//! file-backed ledger, full reindex behind the binary columnar index, then
//! reuse the `GraphSnapshot` for all `b.iter` calls (warm-cache). The indexed
//! view matters here — three of the four lanes only exist behind a binary
//! store.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n_persons, each with `FRIENDS` friend edges,
//!              a name, a tag, an alternate tag, and (half of them) an email
//!              (Tiny=200, Small=1_000, Medium=5_000, Large=20_000)
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_optional
//!   cargo bench -p fluree-db-api --bench query_hot_optional -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_optional

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};
use std::fmt::Write as _;

/// Friend edges per person. Small and fixed so the left join's fan-out stays
/// linear in `n_persons`.
const FRIENDS: usize = 4;

/// Distinct tag values, so the object-correlated scenario has real fan-out on
/// the required side rather than one row per subject.
const TAGS: usize = 8;

fn scale_n_persons(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 200,
        BenchScale::Small => 1_000,
        BenchScale::Medium => 5_000,
        BenchScale::Large => 20_000,
    }
}

/// Lane 1: single-triple `OPTIONAL` on a left-bound subject — the batched
/// subject probe. Half the subjects have no email, so the no-match (padded
/// row) path is exercised too.
const Q_SINGLE: &str = r"
PREFIX ex: <http://example.org/opt/>
SELECT ?s ?e WHERE { ?s ex:name ?n . OPTIONAL { ?s ex:email ?e } }
";

/// Lane 2: the object variable is shared with the required side, so the
/// batched probe is declined and the per-row substituted scan runs with the
/// row's own literal pushed into the object slot.
const Q_OBJECT_CORR: &str = r"
PREFIX ex: <http://example.org/opt/>
SELECT ?s ?t WHERE { ?s ex:tag ?t . OPTIONAL { ?s ex:altTag ?t } }
";

/// Lane 3: two-pattern correlated `OPTIONAL` — `PlanTreeOptionalBuilder`'s
/// batched hash left-join over the coalesced driving side.
const Q_MULTI: &str = r"
PREFIX ex: <http://example.org/opt/>
SELECT ?s ?f ?fn WHERE {
  ?s ex:name ?n .
  OPTIONAL { ?s ex:friend ?f . ?f ex:name ?fn }
}
";

/// Lane 3 with an unbound FILTER operand on half the driving rows. `?age` is a
/// correlation variable the inner can only READ, so the rows that leave it
/// unbound are answered by the padded row and must not take the batch off the
/// hash-join lane.
const Q_UNBOUND_FILTER: &str = r"
PREFIX ex: <http://example.org/opt/>
SELECT ?s ?f WHERE {
  { { ?s ex:name ?n . ?s ex:age ?age } UNION { ?s ex:name ?n } }
  OPTIONAL { ?s ex:friend ?f . FILTER(?age > 20) }
}
";

/// Generate the person graph as Turtle.
///
/// Per person `i`:
/// - `ex:name` (the driving-side triple every scenario starts from)
/// - `ex:age` (bound on one UNION branch, unbound on the other)
/// - `ex:tag` / `ex:altTag` — the same literal, so the object-correlated
///   `OPTIONAL` matches rather than padding
/// - `FRIENDS` `ex:friend` edges into the ring
/// - `ex:email` on even `i` only, so the single-triple lane exercises both the
///   match and the no-match path
fn person_graph_turtle(n_persons: usize) -> String {
    let mut ttl = String::with_capacity(n_persons * 160);
    ttl.push_str("@prefix ex: <http://example.org/opt/> .\n");
    ttl.push_str("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
    for i in 0..n_persons {
        let tag = i % TAGS;
        let _ = writeln!(ttl, "ex:p{i} ex:name \"person-{i}\" .");
        let _ = writeln!(ttl, "ex:p{i} ex:age \"{}\"^^xsd:integer .", 18 + (i % 50));
        let _ = writeln!(ttl, "ex:p{i} ex:tag \"tag-{tag}\" .");
        let _ = writeln!(ttl, "ex:p{i} ex:altTag \"tag-{tag}\" .");
        if i % 2 == 0 {
            let _ = writeln!(ttl, "ex:p{i} ex:email \"p{i}@example.org\" .");
        }
        for k in 1..=FRIENDS {
            let f = (i + k) % n_persons;
            let _ = writeln!(ttl, "ex:p{i} ex:friend ex:p{f} .");
        }
    }
    ttl
}

/// Build a populated, indexed file-backed Fluree ready for hot-cache OPTIONAL
/// benchmarking (same discipline as `query_hot_property_path.rs`).
async fn setup_indexed(n_persons: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-optional");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    let turtle = person_graph_turtle(n_persons);

    // High thresholds during populate so the foreground commit doesn't race
    // with background indexing — we run an explicit reindex below.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            &turtle,
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

fn bench_query_hot_optional(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_persons = scale_n_persons(scale);

    eprintln!(
        "  [query_hot_optional] scale={} n_persons={} (x{} friend edges)",
        scale.as_str(),
        n_persons,
        FRIENDS
    );

    // Setup once per scale; `snapshot` borrows from `fluree`, both held in
    // scope for the group's duration.
    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_persons));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_optional");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, query) in [
        ("single_triple_probe", Q_SINGLE),
        ("object_correlated", Q_OBJECT_CORR),
        ("multi_pattern_hash_join", Q_MULTI),
        ("unbound_filter_operand", Q_UNBOUND_FILTER),
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

criterion_group!(benches, bench_query_hot_optional);
criterion_main!(benches);
