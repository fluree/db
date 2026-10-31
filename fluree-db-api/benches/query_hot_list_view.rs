//! What one list costs queries that do not read it.
//!
//! A query reads a list's members as values of the property holding it, so a
//! SELECT pays nothing for a list it does not read. A CONSTRUCT does: where a
//! template triple restates a triple of the WHERE, it writes the lists that
//! triple read (`fluree-db-query`'s `list_entries`), which looks at each
//! row's predicate on any ledger that has held a list. A ledger that has
//! never held one skips that entirely.
//!
//! Each scenario runs against two ledgers that differ in one unrelated
//! two-member list (`ex:config ex:order (ex:a ex:b)`). The `listed / plain`
//! ratio is what that list costs the query.
//!
//! ## Scenarios
//!
//! 1. **count/whole_graph**: `COUNT(*)` over `?s ?p ?o`, read from index
//!    metadata.
//! 2. **count/by_predicate**: `GROUP BY ?p` counts, the same way.
//! 3. **join/var_predicate_count**: `COUNT(*)` over `?s ex:knows ?f . ?f ?p ?o`,
//!    "fetch everything about each node a pattern finds".
//! 4. **join/var_predicate_select**: the same join, projected.
//! 5. **join/fixed_predicate**: `?s ex:knows ?f . ?f ex:name ?o`.
//! 6. **subject/bound**: `ex:n7 ?p ?o`.
//! 7. **list/members**: `ex:config ex:order ?x`, the list's members. Empty on
//!    the plain ledger.
//! 8. **construct/fixed_predicate**: `CONSTRUCT { ?s ex:name ?o } WHERE
//!    { ?s ex:name ?o }`, on a predicate no list is held on.
//! 9. **construct/var_predicate**: `CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }`,
//!    whose every row is asked whether its predicate holds a list.
//!
//! ## Acceptance
//!
//! Every scenario but 7 should run on the listed ledger within a few percent
//! of the plain one: 1 and 2 count the list's two members as two values, and
//! 9 writes the one list as its cells.
//!
//! ## Matrix
//!
//!   inputs:    BenchScale → n subjects (Tiny=1k, Small=10k, Medium=100k,
//!              Large=1M), each with `ex:name` and one `ex:knows`
//!   metric:    ns/query (criterion default)
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench query_hot_list_view
//!   cargo bench -p fluree-db-api --bench query_hot_list_view -- --test
//!   FLUREE_BENCH_SCALE=medium cargo bench -p fluree-db-api --bench query_hot_list_view

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_bench_support::{
    bench_runtime, current_profile, current_scale, init_tracing_for_bench, next_ledger_alias,
};
use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, FormatterConfig, GraphDb, IndexConfig, TxnOpts,
};

const PREFIXES: &str = "PREFIX ex: <http://example.org/>
";

const SCENARIOS: &[(&str, &str)] = &[
    (
        "count/whole_graph",
        "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
    ),
    (
        "count/by_predicate",
        "SELECT ?p (COUNT(?s) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
    ),
    (
        "join/var_predicate_count",
        "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:knows ?f . ?f ?p ?o }",
    ),
    (
        "join/var_predicate_select",
        "SELECT ?s ?f ?p ?o WHERE { ?s ex:knows ?f . ?f ?p ?o }",
    ),
    (
        "join/fixed_predicate",
        "SELECT ?s ?f ?o WHERE { ?s ex:knows ?f . ?f ex:name ?o }",
    ),
    ("subject/bound", "SELECT ?p ?o WHERE { ex:n7 ?p ?o }"),
    ("list/members", "SELECT ?x WHERE { ex:config ex:order ?x }"),
    (
        "construct/fixed_predicate",
        "CONSTRUCT { ?s ex:name ?o } WHERE { ?s ex:name ?o }",
    ),
    (
        "construct/var_predicate",
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    ),
];

/// `n` subjects in a ring of `ex:knows`, each with an `ex:name`.
fn ring_turtle(n: usize) -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..n {
        ttl.push_str(&format!(
            "ex:n{i} ex:name \"n{i}\" ; ex:knows ex:n{} .\n",
            (i + 1) % n
        ));
    }
    ttl
}

/// A file-backed ledger holding `turtle`, reindexed so it reads from a HEAD
/// index with no novelty.
async fn setup_indexed(fluree: &Fluree, name: &str, turtle: &str) -> GraphDb {
    let alias = next_ledger_alias(name);
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
    // High thresholds so the populate commit does not race background
    // indexing; the explicit reindex below publishes the index.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            turtle,
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
    fluree.db(&alias).await.expect("indexed GraphDb")
}

async fn count(fluree: &Fluree, db: &GraphDb, body: &str) -> serde_json::Value {
    let result = db
        .query(fluree)
        .sparql(&format!("{PREFIXES}{body}"))
        .execute()
        .await
        .expect("count query");
    result.to_jsonld(&db.snapshot).expect("to_jsonld")
}

/// The listed ledger holds the list and writes it as a list, and the plain
/// one holds none: a setup where the list was stored as triples would time
/// nothing.
async fn sanity_check(fluree: &Fluree, plain: &GraphDb, listed: &GraphDb, n: usize) {
    let whole = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";
    assert_eq!(
        count(fluree, plain, whole).await,
        serde_json::json!([[2 * n]])
    );
    // The list's two members, as values of `ex:order`.
    assert_eq!(
        count(fluree, listed, whole).await,
        serde_json::json!([[2 * n + 2]])
    );
    let written = listed
        .query(fluree)
        .sparql(&format!(
            "{PREFIXES}CONSTRUCT {{ ex:config ?p ?o }} WHERE {{ ex:config ?p ?o }}"
        ))
        .format(FormatterConfig::ntriples())
        .execute_formatted_string()
        .await
        .expect("construct");
    assert!(
        written.contains("<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"),
        "the listed ledger's CONSTRUCT writes its list: {written}"
    );
}

fn bench_query_hot_list_view(c: &mut Criterion) {
    init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let profile = current_profile();
    let n = scale.elements_default() as usize;
    eprintln!("  [query_hot_list_view] scale={} n={n}", scale.as_str());

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    // `build` spawns the background indexer, which needs the runtime.
    let fluree = rt.block_on(async {
        FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .build()
            .expect("build file-backed Fluree")
    });
    let ring = ring_turtle(n);
    let plain = rt.block_on(setup_indexed(&fluree, "list-view-plain", &ring));
    let listed = rt.block_on(setup_indexed(
        &fluree,
        "list-view-listed",
        &format!("{ring}ex:config ex:order (ex:a ex:b) .\n"),
    ));
    rt.block_on(sanity_check(&fluree, &plain, &listed, n));

    let mut group = c.benchmark_group("query_hot_list_view");
    group.sample_size(profile.sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    for (name, body) in SCENARIOS {
        let query = format!("{PREFIXES}{body}");
        for (ledger, db) in [("plain", &plain), ("listed", &listed)] {
            group.bench_with_input(
                BenchmarkId::new(format!("{name}/{ledger}"), scale.as_str()),
                &query,
                |b, q| {
                    b.iter(|| {
                        rt.block_on(async {
                            let result = db
                                .query(&fluree)
                                .sparql(q)
                                .execute()
                                .await
                                .unwrap_or_else(|e| panic!("{name} {ledger}: {e}"));
                            black_box(result);
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_query_hot_list_view);
criterion_main!(benches);
