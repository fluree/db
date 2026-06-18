//! Measure the per-query COMPILE breakdown to size a query-plan cache.
//!
//! A plan cache would skip parse + lower + plan and re-run only execution. This
//! bench times, per representative query shape:
//! - raw_parse: `parse_sparql` (SPARQL text -> AST). Upper bound on what a
//!   lexer-level skeleton normalizer would cost (a normalizer is cheaper than a
//!   full parse), i.e. the cache's per-hit overhead.
//! - compile: `explain_sparql` = parse + lower (IRI->Sid, schema) + plan
//!   (selectivity, operator tree). This is what the cache SKIPS. (Slight
//!   overestimate: explain also serializes the plan to JSON.)
//! - total: `query` = compile + execute + result build.
//! - exec~: total - compile (what always runs, cache or not).
//!
//! Decision inputs: compile% of total (is the cache worth it?) and
//! compile-vs-raw_parse (does a cheap normalizer stay << the savings?).
//!
//!   cargo test -p fluree-db-api --test it_compile_breakdown --features native \
//!       --profile dev-fast -- --ignored --nocapture compile_breakdown

#![cfg(feature = "native")]

mod support;

use std::hint::black_box;
use std::time::Instant;

use fluree_db_api::{parse_sparql, FlureeBuilder, ReindexOptions};
use serde_json::json;
use support::graphdb_from_ledger;

const SEED: usize = 8000;
const ITERS: usize = 2000;
const WARMUP: usize = 200;

// BSBM-Explore-class shapes, increasing complexity.
const QUERIES: &[(&str, &str)] = &[
    (
        "lookup (1 subject)",
        "SELECT ?p ?o WHERE { <http://ex/p42> ?p ?o }",
    ),
    (
        "star+filter (3 patterns)",
        "PREFIX ex: <http://ex/> \
         SELECT ?label ?value WHERE { \
           ?s a ex:Product ; ex:label ?label ; ex:value ?value . \
           FILTER(?value > 5000) }",
    ),
    (
        "join+order+limit (4 patterns)",
        "PREFIX ex: <http://ex/> \
         SELECT ?s ?label WHERE { \
           ?s a ex:Product ; ex:label ?label ; ex:value ?value ; ex:feature ex:f7 . \
           FILTER(?value > 1000) } ORDER BY ?label LIMIT 10",
    ),
];

fn avg_us<F: FnMut()>(mut f: F) -> f64 {
    for _ in 0..WARMUP {
        f();
    }
    let start = Instant::now();
    for _ in 0..ITERS {
        f();
    }
    start.elapsed().as_secs_f64() * 1e6 / ITERS as f64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual compile-breakdown bench"]
async fn compile_breakdown() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let ledger_id = "bench/compile:main";

    let l0 = fluree.create_ledger(ledger_id).await.expect("create");
    let g: Vec<_> = (0..SEED)
        .map(|i| {
            json!({
                "@id": format!("http://ex/p{i}"),
                "@type": "http://ex/Product",
                "http://ex/label": format!("label-{i}"),
                "http://ex/value": i,
                "http://ex/feature": { "@id": format!("http://ex/f{}", i % 50) }
            })
        })
        .collect();
    fluree
        .insert(l0, &json!({ "@graph": g }))
        .await
        .expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let db = graphdb_from_ledger(&ledger);

    eprintln!("\n=== compile breakdown (avg of {ITERS} iters, microseconds) ===");
    eprintln!(
        "{:<32} {:>10} {:>10} {:>10} {:>10} {:>9}",
        "query", "raw_parse", "compile", "exec~", "total", "compile%"
    );
    for (name, q) in QUERIES {
        // Sanity: it runs.
        let _ = fluree.query(&db, *q).await.expect("query");

        let raw_parse = avg_us(|| {
            black_box(parse_sparql(black_box(q)));
        });
        // explain_sparql / query are async — time them with a tiny runtime-blocking loop.
        let compile = {
            let start = Instant::now();
            for _ in 0..ITERS {
                black_box(fluree.explain_sparql(&db, q).await.expect("explain"));
            }
            start.elapsed().as_secs_f64() * 1e6 / ITERS as f64
        };
        let total = {
            let start = Instant::now();
            for _ in 0..ITERS {
                black_box(fluree.query(&db, *q).await.expect("query"));
            }
            start.elapsed().as_secs_f64() * 1e6 / ITERS as f64
        };
        let exec = (total - compile).max(0.0);
        let compile_pct = if total > 0.0 {
            compile / total * 100.0
        } else {
            0.0
        };
        eprintln!(
            "{name:<32} {raw_parse:>10.1} {compile:>10.1} {exec:>10.1} {total:>10.1} {compile_pct:>8.0}%"
        );
    }
    eprintln!("=== end ===\n");
    eprintln!("note: `compile` (explain) includes plan->JSON serialization, so it slightly");
    eprintln!("overestimates real compile; `raw_parse` is an upper bound on a lexer normalizer.");
}
