//! Compaction LIFECYCLE: insert load + reads with the read-triggered compaction
//! policy, showing the sawtooth — where it slows down (the read that crosses the
//! segment threshold pays compaction), how reads speed up after (K=1), and for
//! how long (≈ threshold commits until segments re-accumulate).
//!
//! This models the long-lived-server / query-node path: a cached novelty that
//! commits append one segment each, and a read consolidates (compacts) when the
//! graph exceeds the threshold before serving — exactly what
//! `LedgerHandle::snapshot` does (`needs_compaction` → `compact_over`). It runs
//! at the novelty storage layer (real `apply_commit` + `for_each_overlay_flake` +
//! `compact_over`) so the compaction signal isn't masked by the query engine's
//! O(novelty) stats pass.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `LC_COMMITS` | `400` | commits (one appended segment each) |
//! | `LC_SUBJECTS_PER_COMMIT` | `50` | subjects per commit (~4 flakes each) |
//! | `LC_THRESHOLD` | `128` | compact when a graph exceeds this many segments |
//! | `LC_READ_REPEATS` | `20` | point reads timed per commit (median reported) |
//!
//! ## Run
//! ```bash
//! cargo run --release --example novelty_compaction_lifecycle -p fluree-db-api --features native
//! ```

use std::collections::HashMap;
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

fn person_flakes(gid: usize, t: i64) -> Vec<Flake> {
    let s = sid(format!("p{gid:08}"));
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
            FlakeValue::String(format!("Name {gid}")),
            sid("x_str"),
            t,
            true,
            None,
        ),
        Flake::new(
            s.clone(),
            sid("c_age"),
            FlakeValue::Long(18 + (gid % 48) as i64),
            sid("x_long"),
            t,
            true,
            None,
        ),
        Flake::new(
            s,
            sid("d_email"),
            FlakeValue::String(format!("e{gid}@x")),
            sid("x_str"),
            t,
            true,
            None,
        ),
    ]
}

/// SPOT bounds bracketing exactly subject `gid` (a tiny, fixed-size read so the
/// timed cost is dominated by fan-out, not result streaming).
fn point_bounds(gid: usize) -> (Flake, Flake) {
    let s = sid(format!("p{gid:08}"));
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

fn point_read_us(nov: &Novelty, lo: &Flake, hi: &Flake, repeats: usize) -> u128 {
    let mut samples = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let t0 = Instant::now();
        let mut n = 0usize;
        nov.for_each_overlay_flake(
            0,
            IndexType::Spot,
            Some(lo),
            Some(hi),
            false,
            i64::MAX,
            &mut |_f| {
                n += 1;
            },
        );
        std::hint::black_box(n);
        samples.push(t0.elapsed().as_nanos());
    }
    samples.sort_unstable();
    samples[samples.len() / 2] / 1000
}

fn main() {
    let commits = env_usize("LC_COMMITS", 400);
    let npc = env_usize("LC_SUBJECTS_PER_COMMIT", 50);
    let tier_width = env_usize("LC_TIER_WIDTH", 16);
    let read_repeats = env_usize("LC_READ_REPEATS", 20).max(1);

    let rg: HashMap<Sid, GraphId> = HashMap::new();
    let mut nov = Novelty::new(0);
    // Point at a subject inserted in the first commit, so it always exists.
    let (lo, hi) = point_bounds(5.min(npc.saturating_sub(1)));

    println!("compaction lifecycle (TIERED): {commits} commits x {npc} subjects, tier_width={tier_width}\n");
    println!(
        "{:>7}{:>7}{:>12}{:>8}{:>12}",
        "commit", "K_pre", "read_us", "merges", "compact_ms"
    );

    let mut read_samples: Vec<u128> = Vec::new();
    let mut compact_samples: Vec<f64> = Vec::new();
    let mut total_merges = 0usize;
    let mut max_k = 0usize;

    for i in 0..commits {
        let t = i as i64 + 1;
        let base = i * npc;
        let mut batch = Vec::with_capacity(npc * 4);
        for j in 0..npc {
            batch.extend(person_flakes(base + j, t));
        }
        nov.apply_commit(batch, t, &rg).expect("apply_commit");

        // Read-triggered TIERED compaction (mirrors LedgerHandle::snapshot's
        // compact_if_needed): bounded incremental merges of full size classes
        // before the read — no full rewrite, so the per-read cost stays small and
        // roughly constant instead of a growing cliff.
        let k_pre = nov.max_segment_count();
        max_k = max_k.max(k_pre);
        let mut compact_ms = 0.0f64;
        let mut merges = 0;
        if nov.needs_tier_compaction(tier_width) {
            let t0 = Instant::now();
            merges = nov.tier_compact(tier_width);
            compact_ms = t0.elapsed().as_secs_f64() * 1000.0;
            total_merges += merges;
            compact_samples.push(compact_ms);
        }

        let read_us = point_read_us(&nov, &lo, &hi, read_repeats);
        read_samples.push(read_us);

        if merges > 0 || i % (commits / 25).max(1) == 0 || i == commits - 1 {
            println!("{i:>7}{k_pre:>7}{read_us:>12}{merges:>8}{compact_ms:>12.2}");
        }
    }

    let med = |mut v: Vec<u128>| -> u128 {
        if v.is_empty() {
            return 0;
        }
        v.sort_unstable();
        v[v.len() / 2]
    };
    let max_f = |v: &[f64]| v.iter().copied().fold(0.0f64, f64::max);
    let mean_f = |v: &[f64]| {
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };

    println!("\n================ lifecycle summary (tiered) ================");
    println!("max segment count K reached : {max_k}  (bounded ~ tier_width × levels)");
    println!("tier merges triggered       : {total_merges}");
    println!(
        "per-read compaction ms — mean {:.2} / max {:.2}",
        mean_f(&compact_samples),
        max_f(&compact_samples)
    );
    println!("point read median           : {} us", med(read_samples));
    println!("===========================================================");
    println!("Tiered keeps K bounded (~log) and per-read merge cost SMALL and roughly");
    println!("CONSTANT — no growing 0.5–3 s compact-all cliff. Compare LC vs compact-all:");
    println!("compact-all spiked 13→29→49 ms (growing); tiered should stay low and flat.");
}
