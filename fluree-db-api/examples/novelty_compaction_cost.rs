//! Compaction cost profiler for the structural compact-all path.
//!
//! Measures how long `Novelty::compact_all` (rewrite K segments per graph into
//! one, preserving every flake) takes as a function of total novelty size, to
//! decide whether synchronous compaction is cheap enough or whether it must move
//! to a tiered/background scheme. Builds novelty with a fixed segment count
//! (default 1000, well over the 128 threshold) at several total sizes, then times
//! a single compact_all and verifies it collapses to K=1 without losing flakes.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `CC_SUBJECTS` | `120000,250000,500000` | total subjects per run (~4 flakes each → 480k/1M/2M flakes) |
//! | `CC_SEGMENTS` | `1000` | segments to fragment into before compacting |
//! | `CC_REPEATS` | `5` | timed compactions per size (rebuilds fresh each time) |
//!
//! ## Run
//! ```bash
//! cargo run --release --example novelty_compaction_cost -p fluree-db-api --features native
//! ```

use std::collections::HashMap;
use std::time::Instant;

use fluree_db_core::{Flake, FlakeValue, GraphId, Sid};
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

fn person_flakes(i: usize, t: i64) -> Vec<Flake> {
    let s = sid(format!("p{i:08}"));
    vec![
        Flake::new(s.clone(), sid("a_type"), FlakeValue::Ref(sid("Person")), sid("x_id"), t, true, None),
        Flake::new(s.clone(), sid("b_name"), FlakeValue::String(format!("Name {i}")), sid("x_str"), t, true, None),
        Flake::new(s.clone(), sid("c_age"), FlakeValue::Long(18 + (i % 48) as i64), sid("x_long"), t, true, None),
        Flake::new(s, sid("d_email"), FlakeValue::String(format!("e{i}@x")), sid("x_str"), t, true, None),
    ]
}

/// Build novelty holding `n_subjects` spread across `segments` commits/segments.
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

fn main() {
    let segments = env_usize("CC_SEGMENTS", 1000);
    let repeats = env_usize("CC_REPEATS", 5).max(1);
    let subject_list: Vec<usize> = std::env::var("CC_SUBJECTS")
        .unwrap_or_else(|_| "120000,250000,500000".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!("compaction cost: fragment into {segments} segments, then compact_all ({repeats} repeats)\n");
    println!(
        "{:>10}{:>10}{:>10}{:>14}{:>14}",
        "subjects", "flakes", "pre_K", "compact_ms", "post_K"
    );

    for &subjects in &subject_list {
        let mut samples = Vec::with_capacity(repeats);
        let mut pre_k = 0;
        let mut post_k = 0;
        let mut flakes = 0;
        for _ in 0..repeats {
            let mut nov = build(subjects, segments);
            pre_k = nov.max_segment_count();
            flakes = nov.len();
            let before = nov.len();
            let t0 = Instant::now();
            let n = nov.compact_all();
            samples.push(t0.elapsed().as_secs_f64() * 1000.0);
            post_k = nov.max_segment_count();
            assert!(n >= 1, "expected a compaction");
            assert_eq!(nov.len(), before, "compaction must preserve flake count");
            assert_eq!(post_k, 1, "compact_all must reach K=1");
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples[samples.len() / 2];
        println!("{subjects:>10}{flakes:>10}{pre_k:>10}{median:>14.1}{post_k:>14}");
    }
    println!("\nmedian compact_all wall time per size. Sub-ms..low-ms => synchronous compaction is viable;");
    println!("tens+ of ms => prefer tiered/background compaction off the hot path.");
}
