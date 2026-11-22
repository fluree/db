//! Per-phase SPARQL profiler: where does one query's elapsed time go?
//!
//! Sizes what reusable query preparation (parsed-template cache, lowered-IR
//! reuse, plan reuse keyed on the ledger watermark) could remove, and what
//! result encoding costs, on a realistic mix. Every figure is elapsed
//! (wall-clock) time for a phase of one sequential run, not CPU time: a phase
//! that waits — on I/O, a lock, the scheduler — counts its wait.
//!
//! Every timed run goes through the production entry point
//! (`Fluree::query`, then the SPARQL Results XML / RDF/XML formatter the
//! server uses). The run is split at the engine's own tracing span
//! boundaries, read from timestamps taken within that same run, so no phase
//! is a difference of separately measured medians:
//!
//!   parse   - entry → `sparql_lower` opens (params, parse, validate, dataset check)
//!   lower   - `sparql_lower` (AST → IR against the snapshot)
//!   setup   - `sparql_lower` closes → `query_prepare` opens (config defaults,
//!             reasoning resolution, IR clone)
//!   rewrite - `pattern_rewrite` (reasoning/geo/list-view rewrites, dedup)
//!   plan    - `plan` (stats-view cache lookup + `build_operator_tree`: reorder,
//!             estimates, lane selection, operator construction)
//!   prep*   - rest of `query_prepare` (reasoning prep, list-entries wrap)
//!   ctx     - `query_prepare` closes → `query_run` opens (execution context)
//!   run     - `query_run` (open + drain the operator tree)
//!   result  - `query_run` closes → `query` returns (result assembly, drops)
//!   format  - result serialization
//!   drop    - dropping the formatted output and the result
//!
//! Queries are `<template>_<instance>.rq` files (e.g. `q02_i007.rq`). Each
//! instance runs `PROF_ITERS` times after `PROF_WARMUP`; per-template means
//! are over every run of every instance, and the summary weights templates by
//! `mix.json` (`{"q02": 6, ...}`) in the query directory when present.
//!
//! The ledger is bulk-imported then read through its binary index, so plans
//! use real stats and execution takes the indexed scan path.
//!
//! CAVEAT: examples compile `fluree-db-query`/`fluree-db-binary-index` with
//! this crate's dev-only `residency` + `test-counters` features, which touch
//! the scan path; treat `run` as indicative. Parse, lower, setup, rewrite and
//! plan are unaffected.
//!
//! ## Config (env vars)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `PROF_DATA` | (required) | data file or directory for `create().import()` |
//! | `PROF_QUERY_DIR` | (required) | directory of `*.rq` files (+ optional `mix.json`) |
//! | `PROF_ITERS` | `40` | timed runs per instance |
//! | `PROF_WARMUP` | `5` | untimed runs per instance |
//!
//! ## Run
//! ```bash
//! PROF_DATA=../benchmark-db/benchmarks/bsbm/datasets/bsbm-1m/td/dataset.nt \
//! PROF_QUERY_DIR=/path/to/rq \
//!   cargo run --release --example sparql_phase_profile -p fluree-db-api
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use fluree_db_api::{FlureeBuilder, FormatterConfig, GraphDb, LedgerManagerConfig, QueryInput};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const SPANS: &[&str] = &[
    "sparql_lower",
    "query_prepare",
    "pattern_rewrite",
    "plan",
    "query_run",
];

/// Closed spans of the current run: (name, opened, closed).
static CLOSED: Mutex<Vec<(&'static str, Instant, Instant)>> = Mutex::new(Vec::new());

struct Opened(Instant);

/// Records when each span in [`SPANS`] opens and closes.
struct SpanClock;

impl<S> Layer<S> for SpanClock
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Opened(Instant::now()));
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        let closed = Instant::now();
        if let Some(span) = ctx.span(&id) {
            if let Some(Opened(opened)) = span.extensions().get::<Opened>() {
                CLOSED.lock().unwrap().push((span.name(), *opened, closed));
            }
        }
    }
}

const PHASES: &[&str] = &[
    "parse", "lower", "setup", "rewrite", "plan", "prep*", "ctx", "run", "result", "format", "drop",
];

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn us(from: Instant, to: Instant) -> f64 {
    to.saturating_duration_since(from).as_secs_f64() * 1e6
}

/// One production run of `text`, split into [`PHASES`] (µs).
async fn run_once(
    fluree: &fluree_db_api::Fluree,
    db: &GraphDb,
    name: &str,
    text: &str,
    graph_output: bool,
) -> [f64; 11] {
    CLOSED.lock().unwrap().clear();
    let start = Instant::now();
    let result = fluree
        .query(db, QueryInput::Sparql(text))
        .await
        .unwrap_or_else(|e| panic!("{name}: query failed: {e}"));
    let returned = Instant::now();
    let config = if graph_output {
        FormatterConfig::rdf_xml()
    } else {
        FormatterConfig::sparql_xml()
    };
    let formatted = fluree_db_api::format::format_results_string(
        &result,
        &result.context,
        &db.snapshot,
        &config,
    )
    .unwrap_or_else(|e| panic!("{name}: format failed: {e}"));
    let formatted_at = Instant::now();
    drop(std::hint::black_box(formatted));
    drop(result);
    let dropped = Instant::now();

    let closed = std::mem::take(&mut *CLOSED.lock().unwrap());
    // The outermost instance: a nested execution (subquery, DESCRIBE
    // expansion) opens its own spans inside the query's.
    let span = |want: &str| {
        let outer = closed
            .iter()
            .filter(|(n, _, _)| *n == want)
            .min_by_key(|(_, opened, _)| *opened)
            .unwrap_or_else(|| panic!("{name}: no `{want}` span (spans: {closed:?})"));
        (outer.1, outer.2)
    };
    let (lower_open, lower_close) = span("sparql_lower");
    let (prep_open, prep_close) = span("query_prepare");
    let (rw_open, rw_close) = span("pattern_rewrite");
    let (plan_open, plan_close) = span("plan");
    let (run_open, run_close) = span("query_run");
    let rewrite = us(rw_open, rw_close);
    let plan = us(plan_open, plan_close);
    [
        us(start, lower_open),
        us(lower_open, lower_close),
        us(lower_close, prep_open),
        rewrite,
        plan,
        (us(prep_open, prep_close) - rewrite - plan).max(0.0),
        us(prep_close, run_open),
        us(run_open, run_close),
        us(run_close, returned),
        us(returned, formatted_at),
        us(formatted_at, dropped),
    ]
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::registry()
        .with(
            SpanClock.with_filter(tracing_subscriber::filter::filter_fn(|meta| {
                meta.is_span() && SPANS.contains(&meta.name())
            })),
        )
        .init();

    let data = PathBuf::from(std::env::var("PROF_DATA").expect("PROF_DATA"));
    let query_dir = PathBuf::from(std::env::var("PROF_QUERY_DIR").expect("PROF_QUERY_DIR"));
    let iters = env_usize("PROF_ITERS", 40);
    let warmup = env_usize("PROF_WARMUP", 5);

    let mut queries: Vec<(String, String)> = std::fs::read_dir(&query_dir)
        .expect("read PROF_QUERY_DIR")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "rq").then(|| {
                let name = path.file_stem().unwrap().to_string_lossy().into_owned();
                (name, std::fs::read_to_string(&path).expect("read query"))
            })
        })
        .collect();
    queries.sort();
    let weights: BTreeMap<String, f64> = std::fs::read_to_string(query_dir.join("mix.json"))
        .ok()
        .map(|s| serde_json::from_str(&s).expect("mix.json"))
        .unwrap_or_default();

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build()
        .expect("fluree");
    let load = Instant::now();
    fluree
        .create("prof:main")
        .import(&data)
        .execute()
        .await
        .expect("import");
    let db = fluree.db("prof:main").await.expect("db");
    eprintln!(
        "imported {} in {:.1}s (t={}, binary store: {})",
        data.display(),
        load.elapsed().as_secs_f64(),
        db.t,
        db.binary_store().is_some()
    );

    // template -> (summed phases, runs)
    let mut by_template: BTreeMap<String, ([f64; 11], usize)> = BTreeMap::new();
    for (name, text) in &queries {
        let template = name.split('_').next().unwrap_or(name).to_string();
        let upper = text.to_ascii_uppercase();
        let graph_output = upper.contains("CONSTRUCT") || upper.contains("DESCRIBE");
        for _ in 0..warmup {
            run_once(&fluree, &db, name, text, graph_output).await;
        }
        let entry = by_template.entry(template).or_insert(([0.0; 11], 0));
        for _ in 0..iters {
            let phases = run_once(&fluree, &db, name, text, graph_output).await;
            for (sum, v) in entry.0.iter_mut().zip(phases) {
                *sum += v;
            }
            entry.1 += 1;
        }
    }

    print!("{:<6}", "tmpl");
    for p in PHASES {
        print!(" {p:>8}");
    }
    println!(" {:>9} {:>6}", "total", "runs");
    let mut weighted = [0.0; 11];
    let mut weight_sum = 0.0;
    for (template, (sums, runs)) in &by_template {
        let mean: Vec<f64> = sums.iter().map(|s| s / *runs as f64).collect();
        print!("{template:<6}");
        for v in &mean {
            print!(" {v:>8.1}");
        }
        println!(" {:>9.1} {runs:>6}", mean.iter().sum::<f64>());
        let w = weights.get(template).copied().unwrap_or(1.0);
        for (acc, v) in weighted.iter_mut().zip(&mean) {
            *acc += w * v;
        }
        weight_sum += w;
    }
    let per_query: Vec<f64> = weighted.iter().map(|v| v / weight_sum).collect();
    let total: f64 = per_query.iter().sum();
    print!("{:<6}", "MIX");
    for v in &per_query {
        print!(" {v:>8.1}");
    }
    println!(" {total:>9.1}");
    print!("{:<6}", "share");
    for v in &per_query {
        print!(" {:>7.1}%", v / total * 100.0);
    }
    println!();
    let prep: f64 = per_query[..6].iter().sum();
    println!(
        "\nmean µs per query (mix-weighted{}): parse..prep* (what reuse could skip) = {prep:.1} ({:.1}%), \
         run = {:.1}, format = {:.1}",
        if weights.is_empty() { " — no mix.json, equal weights" } else { "" },
        prep / total * 100.0,
        per_query[7],
        per_query[9]
    );
}
