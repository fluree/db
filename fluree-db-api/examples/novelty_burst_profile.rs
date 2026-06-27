//! Novelty BURST profiler — the decision harness for "handle update bursts well".
//!
//! Models the real target workload: an *occasional burst* of many small
//! transactions (not a sustained firehose), then quiet. It answers two questions
//! the segmented-novelty design hinges on:
//!
//! 1. **Write — burst absorption.** Each commit appends one immutable segment, so
//!    absorbing a burst of N small txns is **O(N)** (flat per-commit). The
//!    pre-segmentation design re-merged the whole novelty vector every commit, so
//!    a burst was **O(N^2)** (per-commit cost grew as the burst went). We emulate
//!    that old cost with a full re-sort (`compact_all`) after each commit — same
//!    super-linear shape — to show the flat-vs-growing contrast in-process.
//!
//! 2. **Read — fan-out during the burst.** At the burst's peak, novelty holds K =
//!    (#commits) segments, and every range read does one binary-search probe per
//!    segment. We measure read latency at peak K against the **K=1 baseline** (the
//!    same data after `compact_all`, which is also what `main`'s single sorted
//!    vector and a cold lambda's `bulk_apply_commits` both give). The ratio is
//!    "how many x slower are reads during the burst". Budget: **<= 3x**.
//!
//! Then it sweeps `tier_width` to show how far read-path tiered compaction pulls
//! K (and the read ratio) back toward baseline, and runs the **adversarial probe**
//! (a single broad read right after a no-read burst, where bounded-per-call
//! tiered can't rescue the first read).
//!
//! Read shapes mirror what the query engine issues:
//! - `point`  — SPOT range bounding ONE subject (~4 flakes); latency ~ fan-out.
//! - `narrow` — POST range for one predicate+value.
//! - `full`   — whole-graph SPOT scan (merge throughput).
//! - `join`   — J point probes over distinct subjects: the per-probe tax compounds
//!   once per binding, the join-heavy worst case.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `BURST_PEAK_COMMITS` | `8000` | #commits (== peak segment count K) for the read budget |
//! | `BURST_COMMIT_SUBJECTS` | `10` | subjects per commit (~x4 flakes); "small txn" size |
//! | `BURST_ABSORB_COMMITS` | `800` | #commits for the O(N) vs O(N^2) absorption demo (kept small — the emulation is quadratic by design) |
//! | `BURST_TIERS` | `4,8,16` | tier_width values to sweep |
//! | `BURST_JOIN_PROBES` | `1000` | point probes per join "query" |
//! | `BURST_BUDGET` | `3.0` | pass/fail read-regression multiple |
//!
//! ## Run
//! ```bash
//! cargo run --release --example novelty_burst_profile -p fluree-db-api --features native
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

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn sid(name: impl Into<String>) -> Sid {
    Sid::new(NS, name.into())
}

/// Four flakes for subject `i`: type, name, age, email.
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

/// One commit's worth of flakes for subjects `[start, end)`.
fn commit_batch(start: usize, end: usize, t: i64) -> Vec<Flake> {
    let mut batch = Vec::with_capacity((end - start) * 4);
    for i in start..end {
        batch.extend(person_flakes(i, t));
    }
    batch
}

/// SPOT bounds bracketing exactly subject `k`'s flakes.
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

/// POST bounds selecting predicate `c_age` with object value exactly `age`.
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

/// The four read shapes' median latency (ns) over a fixed novelty.
struct ReadSuite {
    point: u128,
    narrow: u128,
    full: u128,
    join: u128,
    point_cnt: usize,
}

fn read_suite(nov: &Novelty, n_subjects: usize, join_probes: usize) -> ReadSuite {
    let (plo, phi) = point_bounds(n_subjects / 2);
    let (nlo, nhi) = narrow_bounds(40);
    // Distinct subjects spread across the keyspace for the join probes.
    let join_bounds: Vec<(Flake, Flake)> = (0..join_probes)
        .map(|j| point_bounds((j * n_subjects / join_probes.max(1)).min(n_subjects - 1)))
        .collect();

    let point_cnt = count_range(nov, IndexType::Spot, Some(&plo), Some(&phi), false);

    let point = time_ns(2000, || {
        count_range(nov, IndexType::Spot, Some(&plo), Some(&phi), false)
    });
    let narrow = time_ns(200, || {
        count_range(nov, IndexType::Post, Some(&nlo), Some(&nhi), false)
    });
    let full = time_ns(10, || count_range(nov, IndexType::Spot, None, None, true));
    let join = time_ns(20, || {
        let mut total = 0usize;
        for (lo, hi) in &join_bounds {
            total += count_range(nov, IndexType::Spot, Some(lo), Some(hi), false);
        }
        total
    });

    ReadSuite {
        point,
        narrow,
        full,
        join,
        point_cnt,
    }
}

fn us(ns: u128) -> f64 {
    ns as f64 / 1000.0
}

fn ratio(base: u128, val: u128) -> f64 {
    if base > 0 {
        val as f64 / base as f64
    } else {
        0.0
    }
}

fn main() {
    let peak_commits = env_usize("BURST_PEAK_COMMITS", 8000);
    let commit_subjects = env_usize("BURST_COMMIT_SUBJECTS", 10);
    let absorb_commits = env_usize("BURST_ABSORB_COMMITS", 800);
    let join_probes = env_usize("BURST_JOIN_PROBES", 1000);
    let budget = env_f64("BURST_BUDGET", 3.0);
    let tiers: Vec<usize> = std::env::var("BURST_TIERS")
        .unwrap_or_else(|_| "4,8,16".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&t: &usize| t >= 2)
        .collect();

    let rg: HashMap<Sid, GraphId> = HashMap::new();

    println!("======================================================================");
    println!("  NOVELTY BURST PROFILE  (occasional burst of small txns, not firehose)");
    println!("======================================================================");
    println!(
        "  commit size: {commit_subjects} subjects (~{} flakes) per txn",
        commit_subjects * 4
    );
    println!("  read budget: <= {budget:.1}x the K=1 baseline\n");

    // ---------------------------------------------------------------
    // PART A — burst absorption: O(N) append vs O(N^2) full-resort-per-commit
    // ---------------------------------------------------------------
    println!("---- PART A: burst absorption ({absorb_commits} commits) ----");
    let batches: Vec<Vec<Flake>> = (0..absorb_commits)
        .map(|c| {
            let start = c * commit_subjects;
            commit_batch(start, start + commit_subjects, (c + 1) as i64)
        })
        .collect();

    // Branch: append-only segment per commit.
    let mut branch_per_commit = Vec::with_capacity(absorb_commits);
    {
        let mut nov = Novelty::new(0);
        for (c, batch) in batches.iter().enumerate() {
            let b = batch.clone();
            let t0 = Instant::now();
            nov.apply_commit(b, (c + 1) as i64, &rg)
                .expect("apply_commit");
            branch_per_commit.push(t0.elapsed().as_nanos());
        }
    }

    // Emulated old design: full re-sort (compact_all) after each commit.
    let mut emul_per_commit = Vec::with_capacity(absorb_commits);
    {
        let mut nov = Novelty::new(0);
        for (c, batch) in batches.iter().enumerate() {
            let b = batch.clone();
            let t0 = Instant::now();
            nov.apply_commit(b, (c + 1) as i64, &rg)
                .expect("apply_commit");
            nov.compact_all();
            emul_per_commit.push(t0.elapsed().as_nanos());
        }
    }

    let early = |v: &[u128]| {
        let n = 10.min(v.len());
        v[..n].iter().sum::<u128>() / n as u128
    };
    let late = |v: &[u128]| {
        let n = 10.min(v.len());
        v[v.len() - n..].iter().sum::<u128>() / n as u128
    };
    let total = |v: &[u128]| v.iter().sum::<u128>();

    println!(
        "  {:<22}{:>14}{:>14}{:>16}",
        "mode", "first-10 avg", "last-10 avg", "burst total"
    );
    println!(
        "  {:<22}{:>12.2}us{:>12.2}us{:>13.2}ms   <- flat => O(N)",
        "branch (append)",
        us(early(&branch_per_commit)),
        us(late(&branch_per_commit)),
        total(&branch_per_commit) as f64 / 1_000_000.0,
    );
    println!(
        "  {:<22}{:>12.2}us{:>12.2}us{:>13.2}ms   <- grows => O(N^2)",
        "emulated old (resort)",
        us(early(&emul_per_commit)),
        us(late(&emul_per_commit)),
        total(&emul_per_commit) as f64 / 1_000_000.0,
    );
    println!(
        "  per-commit growth:  branch {:.1}x   emulated-old {:.1}x  (last-10 / first-10)",
        ratio(early(&branch_per_commit), late(&branch_per_commit)),
        ratio(early(&emul_per_commit), late(&emul_per_commit)),
    );

    // ---------------------------------------------------------------
    // PART B — read fan-out as a function of segment count K (vs K=1 baseline)
    // ---------------------------------------------------------------
    let n_subjects = peak_commits * commit_subjects;
    println!("\n---- PART B: read latency vs segment count K ({n_subjects} subjects) ----");

    // Build novelty holding `n_subjects` across exactly `k` equal commits/segments.
    let build_at_k = |k: usize| -> Novelty {
        let mut nov = Novelty::new(0);
        let per = n_subjects.div_ceil(k.max(1));
        let mut start = 0usize;
        let mut t = 0i64;
        while start < n_subjects {
            let end = (start + per).min(n_subjects);
            t += 1;
            nov.apply_commit(commit_batch(start, end, t), t, &rg)
                .expect("apply");
            start = end;
        }
        nov
    };

    // K=1 baseline == main's single sorted vector == cold lambda's bulk_apply.
    let base = build_at_k(1);
    let base_rs = read_suite(&base, n_subjects, join_probes);

    let mut k_list: Vec<usize> = vec![1, 2, 4, 8, 16, 32, 64, 256, 1024];
    if !k_list.contains(&peak_commits) {
        k_list.push(peak_commits);
    }
    k_list.retain(|&k| k <= n_subjects);

    println!(
        "  {:<7}{:>9}{:>9}{:>9}{:>9}{:>12}",
        "K", "point", "narrow", "full", "join", "worst(shape)"
    );
    let shapes = ["point", "narrow", "full", "join"];
    for &k in &k_list {
        let nov = build_at_k(k);
        let rs = read_suite(&nov, n_subjects, join_probes);
        let rs_ratios = [
            ratio(base_rs.point, rs.point),
            ratio(base_rs.narrow, rs.narrow),
            ratio(base_rs.full, rs.full),
            ratio(base_rs.join, rs.join),
        ];
        let (wi, &wr) = rs_ratios
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .unwrap();
        let flag = if wr <= budget { "ok" } else { "OVER" };
        println!(
            "  {k:<7}{:>8.1}x{:>8.1}x{:>8.1}x{:>8.1}x{:>8.1}x {}({})",
            rs_ratios[0], rs_ratios[1], rs_ratios[2], rs_ratios[3], wr, shapes[wi], flag
        );
    }

    // Peak novelty (no compaction) reused by Parts C/D.
    let peak = build_at_k(peak_commits);
    let peak_k = peak.max_segment_count();
    println!(
        "  baseline K=1: point={:.2}us narrow={:.2}us full={:.2}us join={:.2}us (point result={} flakes)",
        us(base_rs.point),
        us(base_rs.narrow),
        us(base_rs.full),
        us(base_rs.join),
        base_rs.point_cnt
    );
    println!("  >> ALL shapes pure-novelty here; production dilutes broad reads with the K=1 base index.");

    // ---------------------------------------------------------------
    // PART C — tier_width mitigation: drain peak-K, re-measure
    // ---------------------------------------------------------------
    println!("\n---- PART C: tier_width compaction from peak K={peak_k} (drained fully) ----");
    println!(
        "  {:<8}{:>8}{:>9}{:>8}{:>8}{:>8}{:>8}{:>13}",
        "tier_w", "K_after", "merges", "point", "narrow", "full", "join", "worst(shape)"
    );
    let shapes = ["point", "narrow", "full", "join"];
    for &t in &tiers {
        let mut nov = peak.clone();
        // Drain the backlog fully (as repeated reads would over time).
        let mut merges = 0usize;
        while nov.needs_tier_compaction(t) {
            merges += nov.tier_compact(t);
        }
        let k_after = nov.max_segment_count();
        let rs = read_suite(&nov, n_subjects, join_probes);
        let rr = [
            ratio(base_rs.point, rs.point),
            ratio(base_rs.narrow, rs.narrow),
            ratio(base_rs.full, rs.full),
            ratio(base_rs.join, rs.join),
        ];
        let (wi, &wr) = rr
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .unwrap();
        let flag = if wr <= budget { "ok" } else { "OVER" };
        println!(
            "  {t:<8}{k_after:>8}{merges:>9}{:>7.1}x{:>7.1}x{:>7.1}x{:>7.1}x{:>8.1}x {}({})",
            rr[0], rr[1], rr[2], rr[3], wr, shapes[wi], flag
        );
    }

    // ---------------------------------------------------------------
    // PART D — adversarial: ONE broad read right after a no-read burst.
    // Bounded-per-call tiered can't rescue the first read; this is the spike.
    // ---------------------------------------------------------------
    println!("\n---- PART D: adversarial first-read after no-read burst (tier_w=16) ----");
    {
        let mut nov = peak.clone();
        // Exactly what LedgerHandle::snapshot does before serving: ONE bounded
        // tier_compact pass, then the read.
        let merges = nov.tier_compact(16);
        let k_after = nov.max_segment_count();
        let rs = read_suite(&nov, n_subjects, join_probes);
        let r_full = ratio(base_rs.full, rs.full);
        let r_join = ratio(base_rs.join, rs.join);
        println!("  after ONE bounded pass: K {peak_k} -> {k_after} ({merges} merges)");
        println!(
            "  first broad read: full={:.1}x  join={:.1}x vs K=1  ({})",
            r_full,
            r_join,
            if r_full.max(r_join) <= budget {
                "within budget"
            } else {
                "OVER budget -> needs post-burst nudge / faster index"
            }
        );
    }

    // ---------------------------------------------------------------
    // PART E — production shape: base index (1 segment) + overlay (K segments).
    // While novelty is non-empty the base<->overlay merge is unavoidable; this
    // isolates how much OVERLAY fragmentation adds ON TOP, which is bounded by
    // novelty size relative to the base. Baseline = base + ONE overlay segment.
    // ---------------------------------------------------------------
    let base_n = env_usize("BURST_BASE_SUBJECTS", 800_000);
    let overlay_n = n_subjects; // the burst from Part B
    println!(
        "\n---- PART E: base+overlay broad reads (base={base_n} subj, overlay={overlay_n} subj, ratio {:.0}:1) ----",
        base_n as f64 / overlay_n.max(1) as f64
    );

    // Build the base ONCE (one big consolidated segment), then clone + add overlay.
    let mut base_only = Novelty::new(0);
    base_only
        .apply_commit(commit_batch(0, base_n, 1), 1, &rg)
        .expect("base");

    let build_bo = |overlay_k: usize| -> Novelty {
        let mut nov = base_only.clone();
        let per = overlay_n.div_ceil(overlay_k.max(1));
        let mut start = base_n;
        let end_all = base_n + overlay_n;
        let mut t = 1i64;
        while start < end_all {
            let end = (start + per).min(end_all);
            t += 1;
            nov.apply_commit(commit_batch(start, end, t), t, &rg)
                .expect("overlay");
            start = end;
        }
        nov
    };

    let (e_nlo, e_nhi) = narrow_bounds(40);
    let bo_full =
        |nov: &Novelty| time_ns(5, || count_range(nov, IndexType::Spot, None, None, true));
    let bo_narrow = |nov: &Novelty| {
        time_ns(100, || {
            count_range(nov, IndexType::Post, Some(&e_nlo), Some(&e_nhi), false)
        })
    };

    let bo_base = build_bo(1);
    let f1 = bo_full(&bo_base);
    let n1 = bo_narrow(&bo_base);
    println!(
        "  baseline (base + 1 overlay seg): full={:.1}us  narrow={:.1}us",
        us(f1),
        us(n1)
    );
    println!("  {:<12}{:>10}{:>12}", "overlay_K", "full_x", "narrow_x");
    for ok in [2usize, 8, 64, 1024, peak_commits] {
        let nov = build_bo(ok);
        let fr = ratio(f1, bo_full(&nov));
        let nr = ratio(n1, bo_narrow(&nov));
        let flag = if fr.max(nr) <= budget { "ok" } else { "OVER" };
        println!("  {ok:<12}{fr:>9.1}x{nr:>11.1}x  {flag}");
    }
    println!("  (regression here = overlay fragmentation on top of the base<->overlay merge;");
    println!("   bigger base => more dilution. Compare to pure-novelty Part B to see the gap.)");

    // ---------------------------------------------------------------
    // PART F — compaction cost: the bridge. Writes land fast (Part A); the next
    // read is slow (Part B). If compaction collapses K cheaply, the read AFTER it
    // is fast again. This times that bridge on the peak burst's novelty.
    // ---------------------------------------------------------------
    let peak_flakes = peak.len();
    println!("\n---- PART F: compaction cost to bridge the burst (peak K={peak_k}, {peak_flakes} flakes) ----");
    {
        // compact_all: one full re-sort to K=1 (== main / cold-lambda read speed).
        let mut nov = peak.clone();
        let t0 = Instant::now();
        nov.compact_all();
        let ca_ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "  compact_all -> K={}  {:.1}ms  ({:.3}us/flake)  -> next reads at K=1 baseline",
            nov.max_segment_count(),
            ca_ms,
            ca_ms * 1000.0 / peak_flakes.max(1) as f64,
        );

        // tier_compact full drain: many small bounded merges (read-path style).
        for &t in &tiers {
            let mut nov = peak.clone();
            let t0 = Instant::now();
            let (mut merges, mut passes) = (0usize, 0usize);
            while nov.needs_tier_compaction(t) {
                merges += nov.tier_compact(t);
                passes += 1;
            }
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            println!(
                "  tier({t:>2})   -> K={:>3}  {:.1}ms ({passes} passes, {merges} merges, {:.2}ms/pass)",
                nov.max_segment_count(),
                ms,
                ms / passes.max(1) as f64,
            );
        }
        println!(
            "  => one background compact_all bridges to K=1 in the time above; cost is O(novelty),"
        );
        println!("     bounded by reindex_max_bytes, and amortized over every read until the next burst.");
    }

    println!("\n======================================================================");
    println!("  Budget {budget:.1}x. Point/join ride the zone-map prune; narrow/full are");
    println!("  the deciders. Part C: tiered can't save broad reads. Part D: first read");
    println!("  after a silent burst spikes. Part E: a large base index dilutes it.");
    println!("  Part F: cost to bridge back to K=1 reads.");
    println!("======================================================================");
}
