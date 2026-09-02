//! Measurement harness for per-commit write cost vs accumulated novelty.
//!
//! This is an `#[ignore]`d manual measurement, not a standing CI gate: the
//! absolute slope is hardware-dependent. It becomes an opt-in gate when
//! `GROWTH_MAX_SLOPE_US_PER_1K` is set (e.g. a CI job pinned to known hardware
//! or compared against a committed baseline), failing if the slope exceeds it.
//!
//! Confirms (and lets us track the fix to) the O(accumulated-novelty)-per-commit
//! behavior of the write pipeline: with auto-indexing disabled, novelty grows
//! monotonically and each *constant-size* commit's latency is measured as the
//! ledger fills. We fit `elapsed_us` against `novelty_flakes` (least squares)
//! and report the SLOPE (µs per 1k novelty flakes) and R².
//!
//! The acceptance metric is the SLOPE, not absolute latency. On `main` the slope
//! is large and R² ≈ 1 (cost is linear in novelty — the bug). The novelty-apply
//! fix should collapse the slope toward ~0 while leaving the per-commit fixed
//! cost intact. Absolute numbers are hardware-dependent; the slope is the signal,
//! so this test PRINTS it rather than hard-asserting a hardware-specific bound.
//!
//! Manual run (release matters for absolute numbers; the slope is the signal):
//!   cargo test -p fluree-db-api --test grp_transact it_transact_growth_slope --features native \
//!     --release -- --ignored --nocapture
//!
//! Knobs: GROWTH_TEST_COMMITS (default 3000), GROWTH_TEST_NPC (default 10),
//! GROWTH_MAX_SLOPE_US_PER_1K (unset = report only; set = assert slope <= value).
//!
//! The richer profiling driver (CSV + flamegraph modes) lives at
//! `fluree-db-api/examples/transact_growth_profile.rs`; this test is the
//! self-contained, dependency-light gate.

#![cfg(feature = "native")]

use std::time::Instant;

use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_jsonld};
use fluree_db_api::{FlureeBuilder, ReindexOptions};

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

#[tokio::test(flavor = "current_thread")]
#[ignore = "perf measurement; run manually with --release --ignored --nocapture"]
async fn transact_growth_slope() {
    let commits = env_usize("GROWTH_TEST_COMMITS", 3000);
    let npc = env_usize("GROWTH_TEST_NPC", 10);

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing() // novelty grows monotonically; no auto-reindex
        .build()
        .expect("build");

    let mut ledger = fluree
        .create_ledger("growth/slope:main")
        .await
        .expect("create_ledger");

    // (novelty_flakes, elapsed_us) per commit.
    let mut pts: Vec<(f64, f64)> = Vec::with_capacity(commits);
    let mut first_us = 0u128;
    let mut last_us = 0u128;

    for i in 0..commits {
        let json = txn_data_to_jsonld(&generate_txn_data(i, npc));
        let start = Instant::now();
        let res = fluree.insert(ledger, &json).await.expect("insert");
        let elapsed_us = start.elapsed().as_micros();
        ledger = res.ledger;

        let novelty = ledger.novelty().len() as f64;
        pts.push((novelty, elapsed_us as f64));
        if i == 0 {
            first_us = elapsed_us;
        }
        last_us = elapsed_us;
    }

    let (slope_per_flake, intercept, r2) = linreg(&pts);
    let slope_per_1k = slope_per_flake * 1000.0;
    let final_novelty = ledger.novelty().len();
    let ratio = last_us as f64 / first_us.max(1) as f64;

    eprintln!("\n==================== transact growth slope ====================");
    eprintln!("commits={commits} nodes/commit={npc} final_novelty={final_novelty} flakes");
    eprintln!("GROWTH_SLOPE_US_PER_1K_NOVELTY_FLAKES = {slope_per_1k:.3}");
    eprintln!("GROWTH_R2 = {r2:.4}");
    eprintln!("fixed cost (intercept) = {intercept:.1} us/commit");
    eprintln!("first_commit_us={first_us} last_commit_us={last_us} ratio={ratio:.1}x");
    eprintln!("===============================================================");
    eprintln!("(metric: SLOPE should collapse after the novelty-apply fix)");

    // Sanity.
    assert_eq!(pts.len(), commits, "recorded one point per commit");
    assert!(final_novelty > 0, "novelty accumulated");

    // Opt-in regression gate: only enforced when a ceiling is provided, since the
    // absolute slope is hardware-dependent. Use on pinned CI hardware or against a
    // committed baseline.
    if let Ok(raw) = std::env::var("GROWTH_MAX_SLOPE_US_PER_1K") {
        let max: f64 = raw
            .parse()
            .expect("GROWTH_MAX_SLOPE_US_PER_1K must be a number");
        assert!(
            slope_per_1k <= max,
            "growth slope {slope_per_1k:.1} us/1k novelty flakes exceeds ceiling {max:.1}"
        );
    }
}

/// Per-DELETE cost vs accumulated novelty — the WHERE-execution slope.
///
/// [`transact_growth_slope`] drives pure inserts, which carry no WHERE clause
/// and so never open a scan. This lane measures the other half: a bound-subject
/// `DELETE { <s> ?p ?o } WHERE { <s> ?p ?o }` against a subject that is present
/// in the PERSISTED index (the base is reindexed once up front), which is the
/// shape whose `where_exec` p90 tracked novelty linearly in fluree/db#1722.
///
/// Each iteration commits one filler insert to grow novelty, then times one
/// delete. Only the delete is timed, and its cost is fitted against novelty
/// size. Before the bounded-overlay fix the scan translated the whole overlay
/// on every open, so the slope was large with R² ≈ 1; after it the walk seeks a
/// subject-bracketed window and the slope should collapse toward the intercept.
///
/// Manual run:
///   cargo test -p fluree-db-api --test grp_transact delete_where_growth_slope \
///     --features native --release -- --ignored --nocapture
///
/// Knobs: DELETE_GROWTH_SUBJECTS (default 4000), DELETE_GROWTH_DELETES
/// (default 400), DELETE_GROWTH_FILLER (nodes per filler commit, default 10),
/// DELETE_GROWTH_MAX_SLOPE_US_PER_1K (unset = report only; set = assert).
#[tokio::test(flavor = "current_thread")]
#[ignore = "perf measurement; run manually with --release --ignored --nocapture"]
async fn delete_where_growth_slope() {
    use serde_json::json;

    let subjects = env_usize("DELETE_GROWTH_SUBJECTS", 4000);
    let deletes = env_usize("DELETE_GROWTH_DELETES", 400);
    let filler = env_usize("DELETE_GROWTH_FILLER", 10);
    assert!(deletes < subjects, "need a distinct subject per delete");

    let dir = tempfile::tempdir().expect("tempdir");
    // Thresholds far above anything this test produces: novelty grows
    // monotonically after the one explicit reindex below.
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .with_indexing_thresholds(usize::MAX / 4, usize::MAX / 2)
        .build()
        .expect("build");

    let mut ledger = fluree
        .create_ledger("growth/delete:main")
        .await
        .expect("create_ledger");

    // Base: offer-shaped subjects, ~6 triples each.
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

    // Put the base behind the persisted index. Subjects now resolve in the
    // store dictionary, so a bound-subject scan takes the indexed path (a
    // novelty-only subject would divert to the overlay-only fallback, which
    // already seeks and would hide the regression this lane exists to catch).
    fluree
        .reindex("growth/delete:main", ReindexOptions::default())
        .await
        .expect("reindex");

    // (novelty_flakes, delete_us) per iteration.
    let mut pts: Vec<(f64, f64)> = Vec::with_capacity(deletes);
    let mut first_us = 0u128;
    let mut last_us = 0u128;

    for i in 0..deletes {
        // Grow novelty with data unrelated to the subject being deleted.
        let graph: Vec<_> = (0..filler)
            .map(|k| {
                json!({
                    "@id": format!("http://example.org/filler{i}-{k}"),
                    "http://example.org/name": format!("filler {i} {k}"),
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

        let subject = format!("http://example.org/offer{i}");
        let update = json!({
            "where":  {"@id": subject, "?p": "?o"},
            "delete": {"@id": subject, "?p": "?o"}
        });

        let novelty_before = ledger.novelty().len() as f64;
        let start = Instant::now();
        let res = fluree.update(ledger, &update).await.expect("delete");
        let elapsed_us = start.elapsed().as_micros();
        ledger = res.ledger;

        pts.push((novelty_before, elapsed_us as f64));
        if i == 0 {
            first_us = elapsed_us;
        }
        last_us = elapsed_us;
    }

    let (slope_per_flake, intercept, r2) = linreg(&pts);
    let slope_per_1k = slope_per_flake * 1000.0;
    let final_novelty = ledger.novelty().len();
    let ratio = last_us as f64 / first_us.max(1) as f64;

    eprintln!("\n================= delete-where growth slope =================");
    eprintln!(
        "base_subjects={subjects} deletes={deletes} filler_nodes/commit={filler} \
         final_novelty={final_novelty} flakes"
    );
    eprintln!("DELETE_SLOPE_US_PER_1K_NOVELTY_FLAKES = {slope_per_1k:.3}");
    eprintln!("DELETE_R2 = {r2:.4}");
    eprintln!("fixed cost (intercept) = {intercept:.1} us/delete");
    eprintln!("first_delete_us={first_us} last_delete_us={last_us} ratio={ratio:.1}x");
    eprintln!("=============================================================");
    eprintln!("(metric: SLOPE should collapse toward the intercept after #1722)");

    assert_eq!(pts.len(), deletes, "recorded one point per delete");
    assert!(final_novelty > 0, "novelty accumulated");

    if let Ok(raw) = std::env::var("DELETE_GROWTH_MAX_SLOPE_US_PER_1K") {
        let max: f64 = raw
            .parse()
            .expect("DELETE_GROWTH_MAX_SLOPE_US_PER_1K must be a number");
        assert!(
            slope_per_1k <= max,
            "delete-where slope {slope_per_1k:.1} us/1k novelty flakes exceeds ceiling {max:.1}"
        );
    }
}
