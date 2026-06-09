//! Novelty SEGMENT fan-out microbench (read path only).
//!
//! The query-level profiler (`novelty_read_fanout`) showed that over a large
//! *unindexed* novelty the query cost is dominated by an O(novelty) overlay/stats
//! pass (and joins are O(N^2)-pathological) — which swamps any segment fan-out
//! signal. This microbench strips all of that away and times the novelty read
//! primitive directly (`OverlayProvider::for_each_overlay_flake`) against a
//! `Novelty` built with a *controlled segment count at fixed total size*, so the
//! ONLY variable is how fragmented the same data is. That isolates the pure
//! k-way-merge fan-out tax — one binary-search probe per segment per range read —
//! which is exactly what compaction would remove, and tells us whether compaction
//! must be synchronous, tiered, or just reindex-triggered.
//!
//! Read shapes mirror what the query engine issues:
//! - `point` — SPOT range bounding ONE subject (~4 flakes). Result is tiny, so
//!   latency ≈ fan-out setup (S probes). THE signal.
//! - `narrow` — POST range for one predicate+value (~N/48 flakes).
//! - `full` — whole-graph SPOT scan (all flakes): merge throughput.
//!
//! Same total N across every config; only the segment count varies.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `MB_SUBJECTS` | `120000` | total subjects (~4 flakes each); fixed across the sweep |
//! | `MB_SEGMENTS` | `1,10,100,1000,10000,40000` | segment counts to sweep |
//! | `MB_POINT_ITERS` | `2000` | repeats for the point read |
//! | `MB_NARROW_ITERS` | `200` | repeats for the narrow read |
//! | `MB_FULL_ITERS` | `10` | repeats for the full scan |
//!
//! ## Run
//! ```bash
//! cargo run --release --example novelty_segment_microbench -p fluree-db-api --features native
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use fluree_db_core::{Flake, FlakeValue, GraphId, IndexType, OverlayProvider, Sid};
use fluree_db_novelty::Novelty;

const NS: u16 = 100;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn sid(name: impl Into<String>) -> Sid {
    Sid::new(NS, name.into())
}

/// Four flakes for subject `i`: type, name, age, email. Predicate names are
/// prefixed a/b/c/d so SPOT order within a subject is obvious.
fn person_flakes(i: usize, t: i64) -> Vec<Flake> {
    let s = sid(format!("p{i:08}"));
    vec![
        Flake::new(
            s.clone(),
            sid("a_type"),
            FlakeValue::Ref(sid("Person")),
            sid("x_id"),
            t,
            true,
            None,
        ),
        Flake::new(
            s.clone(),
            sid("b_name"),
            FlakeValue::String(format!("Name {i}")),
            sid("x_str"),
            t,
            true,
            None,
        ),
        Flake::new(
            s.clone(),
            sid("c_age"),
            FlakeValue::Long(18 + (i % 48) as i64),
            sid("x_long"),
            t,
            true,
            None,
        ),
        Flake::new(
            s,
            sid("d_email"),
            FlakeValue::String(format!("e{i}@x")),
            sid("x_str"),
            t,
            true,
            None,
        ),
    ]
}

/// Build novelty holding `n_subjects` spread across ~`segments` commits (== one
/// segment per commit). Same data for every `segments` value.
fn build(n_subjects: usize, segments: usize) -> Novelty {
    let rg: HashMap<Sid, GraphId> = HashMap::new();
    let mut nov = Novelty::new(0);
    let per = n_subjects.div_ceil(segments.max(1));
    let mut t = 0i64;
    let mut start = 0usize;
    while start < n_subjects {
        let end = (start + per).min(n_subjects);
        t += 1;
        let mut batch = Vec::with_capacity((end - start) * 4);
        for i in start..end {
            batch.extend(person_flakes(i, t));
        }
        nov.apply_commit(batch, t, &rg).expect("apply_commit");
        start = end;
    }
    nov
}

/// SPOT bounds bracketing exactly subject `k`'s flakes (left-exclusive,
/// right-inclusive), using predicate-name sentinels below/above the real ones.
fn point_bounds(k: usize) -> (Flake, Flake) {
    let s = sid(format!("p{k:08}"));
    let lo = Flake::new(
        s.clone(),
        sid(""),
        FlakeValue::Long(i64::MIN),
        sid(""),
        0,
        true,
        None,
    );
    let hi = Flake::new(
        s,
        sid("~"),
        FlakeValue::Long(i64::MAX),
        sid("~"),
        i64::MAX,
        true,
        None,
    );
    (lo, hi)
}

/// POST bounds selecting predicate `c_age` with object value exactly `age`
/// (left-exclusive at age-1, right-inclusive at age), subject sentinel at max.
fn narrow_bounds(age: i64) -> (Flake, Flake) {
    let s_max = sid("~~~~~~~~");
    let lo = Flake::new(
        s_max.clone(),
        sid("c_age"),
        FlakeValue::Long(age - 1),
        sid("x_long"),
        i64::MAX,
        true,
        None,
    );
    let hi = Flake::new(
        s_max,
        sid("c_age"),
        FlakeValue::Long(age),
        sid("x_long"),
        i64::MAX,
        true,
        None,
    );
    (lo, hi)
}

/// Count flakes a range read yields (also the timed work).
fn count_range(
    nov: &Novelty,
    index: IndexType,
    first: Option<&Flake>,
    rhs: Option<&Flake>,
    leftmost: bool,
) -> usize {
    let mut n = 0usize;
    nov.for_each_overlay_flake(0, index, first, rhs, leftmost, i64::MAX, &mut |_f| {
        n += 1;
    });
    n
}

/// Median per-call latency (ns) over `iters` runs, after one warmup.
fn time_ns(iters: usize, mut f: impl FnMut() -> usize) -> u128 {
    black_box(f()); // warmup
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let r = f();
        samples.push(start.elapsed().as_nanos());
        black_box(r);
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn main() {
    let n = env_usize("MB_SUBJECTS", 120_000);
    let point_iters = env_usize("MB_POINT_ITERS", 2000);
    let narrow_iters = env_usize("MB_NARROW_ITERS", 200);
    let full_iters = env_usize("MB_FULL_ITERS", 10);
    let seg_list: Vec<usize> = std::env::var("MB_SEGMENTS")
        .unwrap_or_else(|_| "1,10,100,1000,10000,40000".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&s: &usize| s > 0 && s <= n)
        .collect();

    println!(
        "novelty segment microbench: {n} subjects (~{} flakes), segments {seg_list:?}",
        n * 4
    );
    println!(
        "(point = one-subject SPOT read; narrow = one-age POST read; full = whole SPOT scan)\n"
    );

    let point_k = n / 2;
    let (plo, phi) = point_bounds(point_k);
    let (nlo, nhi) = narrow_bounds(40);

    // rows[i] = (segments, point_ns, narrow_ns, full_ns, point_cnt, narrow_cnt, full_cnt)
    let mut rows: Vec<(usize, u128, u128, u128, usize, usize, usize)> = Vec::new();

    for &segments in &seg_list {
        let t0 = Instant::now();
        let nov = build(n, segments);
        let build_s = t0.elapsed().as_secs_f64();

        let pc = count_range(&nov, IndexType::Spot, Some(&plo), Some(&phi), false);
        let nc = count_range(&nov, IndexType::Post, Some(&nlo), Some(&nhi), false);
        let fc = count_range(&nov, IndexType::Spot, None, None, true);

        let p_ns = time_ns(point_iters, || {
            count_range(&nov, IndexType::Spot, Some(&plo), Some(&phi), false)
        });
        let n_ns = time_ns(narrow_iters, || {
            count_range(&nov, IndexType::Post, Some(&nlo), Some(&nhi), false)
        });
        let f_ns = time_ns(full_iters, || {
            count_range(&nov, IndexType::Spot, None, None, true)
        });

        println!(
            "segments={segments:<6} built {build_s:>5.1}s  point={:>9.1}us (n={pc})  narrow={:>9.1}us (n={nc})  full={:>9.1}us (n={fc})",
            p_ns as f64 / 1000.0,
            n_ns as f64 / 1000.0,
            f_ns as f64 / 1000.0,
        );
        rows.push((segments, p_ns, n_ns, f_ns, pc, nc, fc));
    }

    // Summary: latency vs segments + fan-out ratio (max-seg / min-seg).
    println!("\n================ novelty segment fan-out (median latency) ================");
    println!(
        "{:>9}{:>14}{:>14}{:>14}",
        "segments", "point_us", "narrow_us", "full_us"
    );
    for &(s, p, nn, f, _, _, _) in &rows {
        println!(
            "{s:>9}{:>14.2}{:>14.2}{:>14.2}",
            p as f64 / 1000.0,
            nn as f64 / 1000.0,
            f as f64 / 1000.0
        );
    }
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        let ratio = |a: u128, b: u128| if a > 0 { b as f64 / a as f64 } else { 0.0 };
        println!(
            "\nfan-out ratio ({}seg / {}seg):  point={:.1}x  narrow={:.1}x  full={:.1}x",
            last.0,
            first.0,
            ratio(first.1, last.1),
            ratio(first.2, last.2),
            ratio(first.3, last.3),
        );
        println!(
            "point read result is ~constant ({} flakes) across configs, so its growth is pure fan-out.",
            first.4
        );
        println!("~1x => fan-out negligible (reindex-triggered compaction suffices).");
        println!("~linear in segment count => active compaction needed (tiered/synchronous).");
    }
}
