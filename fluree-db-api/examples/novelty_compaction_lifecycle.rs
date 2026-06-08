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
        Flake::new(s.clone(), sid("a_type"), FlakeValue::Ref(sid("Person")), sid("x_id"), t, true, None),
        Flake::new(s.clone(), sid("b_name"), FlakeValue::String(format!("Name {gid}")), sid("x_str"), t, true, None),
        Flake::new(s.clone(), sid("c_age"), FlakeValue::Long(18 + (gid % 48) as i64), sid("x_long"), t, true, None),
        Flake::new(s, sid("d_email"), FlakeValue::String(format!("e{gid}@x")), sid("x_str"), t, true, None),
    ]
}

/// SPOT bounds bracketing exactly subject `gid` (a tiny, fixed-size read so the
/// timed cost is dominated by fan-out, not result streaming).
fn point_bounds(gid: usize) -> (Flake, Flake) {
    let s = sid(format!("p{gid:08}"));
    let lo = Flake::new(s.clone(), sid(""), FlakeValue::Long(i64::MIN), sid(""), 0, true, None);
    let hi = Flake::new(s, sid("~"), FlakeValue::Long(i64::MAX), sid("~"), i64::MAX, true, None);
    (lo, hi)
}

fn point_read_us(nov: &Novelty, lo: &Flake, hi: &Flake, repeats: usize) -> u128 {
    let mut samples = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let t0 = Instant::now();
        let mut n = 0usize;
        nov.for_each_overlay_flake(0, IndexType::Spot, Some(lo), Some(hi), false, i64::MAX, &mut |_f| {
            n += 1;
        });
        std::hint::black_box(n);
        samples.push(t0.elapsed().as_nanos());
    }
    samples.sort_unstable();
    samples[samples.len() / 2] / 1000
}

fn main() {
    let commits = env_usize("LC_COMMITS", 400);
    let npc = env_usize("LC_SUBJECTS_PER_COMMIT", 50);
    let threshold = env_usize("LC_THRESHOLD", 128);
    let read_repeats = env_usize("LC_READ_REPEATS", 20).max(1);

    let rg: HashMap<Sid, GraphId> = HashMap::new();
    let mut nov = Novelty::new(0);
    // Point at a subject inserted in the first commit, so it always exists.
    let (lo, hi) = point_bounds(5.min(npc.saturating_sub(1)));

    println!(
        "compaction lifecycle: {commits} commits x {npc} subjects, threshold={threshold}\n"
    );
    println!(
        "{:>7}{:>7}{:>12}{:>9}{:>14}",
        "commit", "K_pre", "read_us", "compact", "compact_ms"
    );

    let mut steady_reads: Vec<u128> = Vec::new();
    let mut spike_reads: Vec<u128> = Vec::new();
    let mut cycle_lengths: Vec<usize> = Vec::new();
    let mut last_compact_commit: Option<usize> = None;

    for i in 0..commits {
        let t = i as i64 + 1;
        let base = i * npc;
        let mut batch = Vec::with_capacity(npc * 4);
        for j in 0..npc {
            batch.extend(person_flakes(base + j, t));
        }
        nov.apply_commit(batch, t, &rg).expect("apply_commit");

        // Read-triggered compaction policy (mirrors LedgerHandle::snapshot):
        // consolidate before the read if the graph is over the threshold. The
        // read that triggers it eats the compaction cost; later reads are fast.
        let k_pre = nov.max_segment_count();
        let mut compact_ms = 0.0f64;
        let compacted = nov.needs_compaction(threshold);
        if compacted {
            let t0 = Instant::now();
            nov.compact_over(threshold);
            compact_ms = t0.elapsed().as_secs_f64() * 1000.0;
            if let Some(prev) = last_compact_commit {
                cycle_lengths.push(i - prev);
            }
            last_compact_commit = Some(i);
        }

        let read_us = point_read_us(&nov, &lo, &hi, read_repeats);
        if compacted {
            spike_reads.push(read_us);
        } else {
            steady_reads.push(read_us);
        }

        // Print every commit near a compaction boundary, else sample.
        let near_boundary = compacted || nov.max_segment_count() >= threshold.saturating_sub(2);
        if near_boundary || i % (commits / 20).max(1) == 0 || i == commits - 1 {
            println!(
                "{i:>7}{k_pre:>7}{read_us:>12}{:>9}{compact_ms:>14.1}",
                if compacted { "YES" } else { "" }
            );
        }
    }

    let med = |mut v: Vec<u128>| -> u128 {
        if v.is_empty() {
            return 0;
        }
        v.sort_unstable();
        v[v.len() / 2]
    };
    let avg_cycle = if cycle_lengths.is_empty() {
        0.0
    } else {
        cycle_lengths.iter().sum::<usize>() as f64 / cycle_lengths.len() as f64
    };

    println!("\n================ lifecycle summary ================");
    println!("compactions triggered : {}", last_compact_commit.map_or(0, |_| spike_reads.len()));
    println!("cycle length (commits between compactions): {avg_cycle:.0}  (≈ threshold {threshold})");
    println!("point read — steady (K<threshold) median : {} us", med(steady_reads));
    println!(
        "point read — on the compacting commit median : {} us (includes compaction)",
        med(spike_reads)
    );
    println!("===================================================");
    println!("Reads creep up with segment count, the threshold-crossing read pays compaction");
    println!("(the spike), then reads drop back to K=1 — repeating every ~{threshold} commits.");
    println!("A tiered compaction would replace the spike with small incremental merges.");
}
