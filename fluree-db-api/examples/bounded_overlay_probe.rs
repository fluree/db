//! Isolate the SCAN cost of a bound-subject pattern against growing novelty.
//!
//! `delete_where_growth_slope` measures a whole DELETE transaction, which also
//! pays commit write, fsync, novelty apply and dedup — each with its own
//! novelty-linear term — so it cannot attribute a change to the scan alone.
//! This probe issues **read-only queries** (no commit at all) against a ledger
//! whose novelty is grown between rounds, so the only thing varying is the
//! overlay work done when a scan opens.
//!
//! NOTE on what this probe currently shows: after fluree/db#1722 the overlay
//! translation inside `operator_open` is a seek (`overlay_translate` p50 ~0.6us),
//! but END-TO-END query latency here is still novelty-linear, because
//! `query_prepare:plan` rebuilds the planner's stats view via
//! `assemble_fast_stats`, which walks ALL novelty (`iter_flakes(Post)`) behind a
//! cache keyed on the overlay epoch — so it misses on every commit, exactly like
//! the translation cache did. Run with
//! `RUST_LOG=fluree_db_query=debug` and compare the `operator_open` and
//! `query_prepare:plan` spans to see the split.
//!
//! It reports, per novelty level, the mean/p50/p90 latency of a bound-subject
//! query `{"@id": <s>, "?p": "?o"}` where `<s>` is present in the PERSISTED
//! index (the path that translated the whole overlay before fluree/db#1722),
//! and fits latency against novelty size.
//!
//! ```bash
//! cargo run --release --example bounded_overlay_probe -p fluree-db-api --features native
//! ```
//!
//! Knobs: PROBE_SUBJECTS (default 4000), PROBE_ROUNDS (default 8),
//! PROBE_FILLER (filler nodes per round, default 400), PROBE_QUERIES
//! (queries timed per round, default 200).

use std::time::Instant;

use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::json;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Least-squares fit of y = slope*x + intercept; returns (slope, intercept, r2).
fn linreg(points: &[(f64, f64)]) -> (f64, f64, f64) {
    let n = points.len() as f64;
    if n < 2.0 {
        return (0.0, 0.0, 0.0);
    }
    let mx = points.iter().map(|p| p.0).sum::<f64>() / n;
    let my = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx = points.iter().map(|p| (p.0 - mx).powi(2)).sum::<f64>();
    let sxy = points.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum::<f64>();
    let slope = if sxx != 0.0 { sxy / sxx } else { 0.0 };
    let intercept = my - slope * mx;
    let syy = points.iter().map(|p| (p.1 - my).powi(2)).sum::<f64>();
    let ss_res = points
        .iter()
        .map(|p| (p.1 - (intercept + slope * p.0)).powi(2))
        .sum::<f64>();
    let r2 = if syy != 0.0 { 1.0 - ss_res / syy } else { 0.0 };
    (slope, intercept, r2)
}

fn pct(sorted_us: &[u128], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let i = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[i] as f64
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // `RUST_LOG=fluree_db_query::binary_scan=debug` surfaces the
    // `overlay_translate` span (bounded / cache_hit / segments / ops_len).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .try_init();

    let subjects = env_usize("PROBE_SUBJECTS", 4000);
    let rounds = env_usize("PROBE_ROUNDS", 8);
    let filler = env_usize("PROBE_FILLER", 400);
    let queries = env_usize("PROBE_QUERIES", 200);

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .with_indexing_thresholds(usize::MAX / 4, usize::MAX / 2)
        .build()
        .expect("build");

    let mut ledger = fluree
        .create_ledger("probe/overlay:main")
        .await
        .expect("create_ledger");

    // Base: subjects that will live in the PERSISTED dictionary.
    let chunk = 1000;
    let mut lo = 0;
    while lo < subjects {
        let hi = (lo + chunk).min(subjects);
        let graph: Vec<_> = (lo..hi)
            .map(|i| {
                json!({
                    "@id": format!("http://example.org/offer{i}"),
                    "http://example.org/name": format!("offer {i}"),
                    "http://example.org/price": i as i64,
                    "http://example.org/tag": "base"
                })
            })
            .collect();
        ledger = fluree
            .insert(ledger, &json!({ "@graph": graph }))
            .await
            .expect("seed")
            .ledger;
        lo = hi;
    }
    fluree
        .reindex("probe/overlay:main", ReindexOptions::default())
        .await
        .expect("reindex");

    println!("subjects={subjects} rounds={rounds} filler/round={filler} queries/round={queries}");
    println!("{:>12}  {:>10}  {:>10}  {:>10}", "novelty", "mean_us", "p50_us", "p90_us");

    let mut pts: Vec<(f64, f64)> = Vec::with_capacity(rounds);

    for round in 0..rounds {
        // Grow novelty (not touching the queried subjects).
        let graph: Vec<_> = (0..filler)
            .map(|k| {
                json!({
                    "@id": format!("http://example.org/filler{round}-{k}"),
                    "http://example.org/name": format!("filler {round} {k}"),
                    "http://example.org/price": k as i64,
                    "http://example.org/tag": "filler"
                })
            })
            .collect();
        ledger = fluree
            .insert(ledger, &json!({ "@graph": graph }))
            .await
            .expect("filler")
            .ledger;

        let novelty = ledger.novelty().len() as f64;

        // Read-only: a fresh view per query so each pays a cold execution
        // context, exactly as a separate request would.
        let mut times: Vec<u128> = Vec::with_capacity(queries);
        for q in 0..queries {
            let subject = format!("http://example.org/offer{}", q % subjects);
            let query = json!({
                "select": ["?p", "?o"],
                "where": {"@id": subject, "?p": "?o"}
            });
            let view = fluree.db("probe/overlay:main").await.expect("view");
            let start = Instant::now();
            let out = fluree
                .query(&view, QueryInput::JsonLd(&query))
                .await
                .expect("query");
            times.push(start.elapsed().as_micros());
            // Force materialization so nothing is lazily skipped.
            let _ = out.to_jsonld(&view.snapshot).expect("jsonld");
        }

        times.sort_unstable();
        let mean = times.iter().sum::<u128>() as f64 / times.len() as f64;
        println!(
            "{novelty:>12.0}  {mean:>10.1}  {:>10.1}  {:>10.1}",
            pct(&times, 0.50),
            pct(&times, 0.90)
        );
        pts.push((novelty, mean));
    }

    let (slope_per_flake, intercept, r2) = linreg(&pts);
    println!("\n=================== bounded overlay probe ===================");
    println!(
        "QUERY_SLOPE_US_PER_1K_NOVELTY_FLAKES = {:.3}",
        slope_per_flake * 1000.0
    );
    println!("QUERY_R2 = {r2:.4}");
    println!("fixed cost (intercept) = {intercept:.1} us/query");
    if let (Some(first), Some(last)) = (pts.first(), pts.last()) {
        println!(
            "first_round_mean={:.1}us last_round_mean={:.1}us ratio={:.2}x",
            first.1,
            last.1,
            last.1 / first.1.max(1.0)
        );
    }
    println!("=============================================================");
}
