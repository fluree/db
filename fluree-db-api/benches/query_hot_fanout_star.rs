//! Hot-cache latency for a **same-subject star with a fan-out predicate** whose
//! object variable nothing downstream projects.
//!
//! This is the shape `PropertyJoinOperator`'s existence-only (semijoin)
//! demotion exists to make cheap, and the shape whose plan #1700 changed. It
//! had no bench coverage in either direction, which is how a 6.7x CONSTRUCT
//! regression could land and be measured only by hand:
//!
//! * **`construct_blank_free`** — the demotion is *licensed*: a CONSTRUCT result
//!   is an RDF graph whose serializers canonicalize, so the fan-out rows are
//!   discarded and pruning them is free. This is the regression guard. If the
//!   license at `execute::operator_tree::construct_result_is_multiplicity_blind`
//!   is lost, this case materializes the full cartesian product to produce a
//!   byte-identical graph and the number moves by roughly the fan-out factor.
//! * **`select_unprojected_object`** — the demotion is *not* licensed: SPARQL
//!   projects a bag, so every fan-out row is an answer row. This case is
//!   deliberately the expensive one; it is here so the cost of correctness is
//!   tracked rather than rediscovered, and so a future optimization that
//!   carries multiplicity without materializing the object bindings has a
//!   number to beat.
//! * **`select_distinct`** — the control. `SELECT DISTINCT` licenses the
//!   demotion, so this must stay flat whatever happens to the other two. If all
//!   three move together the run is measuring the box, not the planner.
//!
//! Separate fixture from `query_hot_bsbm` on purpose: BSBM has no multi-valued
//! same-subject predicate, and adding one would shift that bench's committed
//! baseline for unrelated cases.
//!
//! ## Matrix
//!
//!   inputs:   BenchScale → n_subjects × TAGS_PER_SUBJECT fan-out
//!             (Tiny=200, Small=1k, Medium=5k, Large=20k subjects; 40 tags each)
//!   metric:   ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_fanout_star
//!   cargo bench -p fluree-db-api --bench query_hot_fanout_star -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
    BenchScale,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, TxnOpts};

/// Objects per subject on the fan-out predicate. The whole point of the shape
/// is that this is > 1, so the pruned and unpruned plans differ by a factor of
/// it rather than by a constant.
const TAGS_PER_SUBJECT: usize = 40;

fn scale_n_subjects(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Tiny => 200,
        BenchScale::Small => 1_000,
        BenchScale::Medium => 5_000,
        BenchScale::Large => 20_000,
    }
}

/// `?s a ex:Gadget . ?s ex:tag ?o` — `?o` never leaves the WHERE clause.
/// The template is blank-free and the query unsliced, so the result graph is
/// exactly one node per subject however many tags each carries.
const CONSTRUCT_BLANK_FREE: &str = r#"
PREFIX ex: <http://example.org/>
CONSTRUCT { ?s ex:flag "y" }
WHERE { ?s a ex:Gadget . ?s ex:tag ?o }
"#;

/// The same star projected as a bag: every fan-out row is an answer row.
const SELECT_UNPROJECTED_OBJECT: &str = r"
PREFIX ex: <http://example.org/>
SELECT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o }
";

/// Control: DISTINCT licenses the demotion, so this keeps the pruned plan.
const SELECT_DISTINCT: &str = r"
PREFIX ex: <http://example.org/>
SELECT DISTINCT ?s WHERE { ?s a ex:Gadget . ?s ex:tag ?o }
";

fn fanout_turtle(n_subjects: usize) -> String {
    // ~24 bytes per tag line plus the subject header.
    let mut out = String::with_capacity(n_subjects * TAGS_PER_SUBJECT * 24 + 64);
    out.push_str("@prefix ex: <http://example.org/> .\n");
    for s in 0..n_subjects {
        out.push_str(&format!("ex:g{s} a ex:Gadget ; ex:tag "));
        for t in 0..TAGS_PER_SUBJECT {
            if t > 0 {
                out.push_str(" , ");
            }
            out.push_str(&format!("\"t{t}\""));
        }
        out.push_str(" .\n");
    }
    out
}

/// Build a populated, indexed file-backed Fluree. Mirrors `query_hot_bsbm`'s
/// setup discipline: populate behind a raised reindex threshold, then reindex
/// explicitly so the measured queries traverse the binary columnar index rather
/// than novelty replay.
async fn setup_indexed(n_subjects: usize) -> (tempfile::TempDir, Fluree, String) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let alias = next_ledger_alias("query-hot-fanout-star");
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            &fanout_turtle(n_subjects),
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

fn bench_query_hot_fanout_star(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n_subjects = scale_n_subjects(scale);

    eprintln!(
        "  [query_hot_fanout_star] scale={} n_subjects={} pairs={}",
        scale.as_str(),
        n_subjects,
        n_subjects * TAGS_PER_SUBJECT
    );

    let (_db_dir, fluree, alias) = rt.block_on(setup_indexed(n_subjects));
    let snapshot = rt.block_on(async { fluree.graph(&alias).load().await.expect("graph load") });

    let mut group = c.benchmark_group("query_hot_fanout_star");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);

    for (name, sparql) in [
        ("construct_blank_free", CONSTRUCT_BLANK_FREE),
        ("select_unprojected_object", SELECT_UNPROJECTED_OBJECT),
        ("select_distinct", SELECT_DISTINCT),
    ] {
        group.bench_with_input(
            BenchmarkId::new(name, scale.as_str()),
            &n_subjects,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = snapshot
                            .query()
                            .sparql(sparql)
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

criterion_group!(benches, bench_query_hot_fanout_star);
criterion_main!(benches);
