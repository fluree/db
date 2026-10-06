//! Live one-line-per-second progress + end-of-run summary.
//!
//! The progress task pulls a snapshot every tick and emits a single
//! line: total ops, TPS over the last tick, success rate, and the
//! aggregate p50 / p99 latency. The summary at the end breaks down
//! per op kind with the same percentiles, plus per-outcome counts.

use crate::metrics::{HistogramSnapshot, Metrics, MetricsSnapshot};
use crate::ops::{OpKind, Outcome};
use crate::runner::RunSummary;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Background printer that emits one progress line per `tick`.
/// Returns when `shutdown` flips to `true`.
pub async fn live_progress(
    metrics: Arc<Metrics>,
    tick: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let started = Instant::now();
    let mut last_total: u64 = 0;
    let mut last_t = started;
    println!(
        "{:>6}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "t", "total", "tps", "ok%", "p50", "p99"
    );
    loop {
        let sleep = tokio::time::sleep(tick);
        tokio::pin!(sleep);
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = &mut sleep => {}
        }
        if *shutdown.borrow() {
            return;
        }
        let snap = metrics.snapshot();
        let now = Instant::now();
        let dt = now.duration_since(last_t).as_secs_f64().max(0.001);
        let issued_in_tick = snap.total.saturating_sub(last_total);
        let tps = issued_in_tick as f64 / dt;
        let total_landed: u64 = snap
            .per_kind_outcome
            .iter()
            .filter(|((_, o), _)| o.is_landed())
            .map(|(_, c)| *c)
            .sum();
        let ok_rate = if snap.total == 0 {
            0.0
        } else {
            (total_landed as f64 / snap.total as f64) * 100.0
        };
        println!(
            "{:>5.1}s  {:>10}  {:>10.0}  {:>9.2}%  {:>10}  {:>10}",
            started.elapsed().as_secs_f64(),
            snap.total,
            tps,
            ok_rate,
            fmt_dur(&snap.aggregate.p50),
            fmt_dur(&snap.aggregate.p99),
        );
        last_total = snap.total;
        last_t = now;
    }
}

/// Final per-kind / per-outcome breakdown printed once the run ends.
pub fn print_summary(snap: &MetricsSnapshot, run: &RunSummary) {
    println!();
    println!("─── Summary ───");
    let tps = snap.total as f64 / run.elapsed.as_secs_f64().max(0.001);
    println!(
        "elapsed: {:.2}s   total: {}   ledgers landed: {}   overall tps: {:.0}",
        run.elapsed.as_secs_f64(),
        snap.total,
        run.ledger_count,
        tps,
    );

    println!();
    println!(
        "{:<14} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "op", "count", "landed%", "p50", "p95", "p99", "p99.9", "max"
    );
    for &kind in OpKind::ALL {
        let Some(h) = snap.per_kind.get(&kind) else {
            continue;
        };
        if h.count == 0 {
            continue;
        }
        let issued = snap.count_for(kind);
        let landed = snap.landed_for(kind);
        let pct = if issued == 0 {
            0.0
        } else {
            (landed as f64 / issued as f64) * 100.0
        };
        println!(
            "{:<14} {:>10} {:>9.2}% {:>10} {:>10} {:>10} {:>10} {:>10}",
            kind.label(),
            issued,
            pct,
            fmt_dur(&h.p50),
            fmt_dur(&h.p95),
            fmt_dur(&h.p99),
            fmt_dur(&h.p999),
            fmt_dur(&h.max),
        );
    }
    print_aggregate(&snap.aggregate);
    print_outcome_breakdown(snap);
    print_leader_change_events(snap);
    print_ledger_distribution(snap);
}

fn print_leader_change_events(snap: &MetricsSnapshot) {
    let mut rows: Vec<(OpKind, u64)> = OpKind::ALL
        .iter()
        .filter_map(|&kind| {
            let n = snap
                .per_kind_leader_change_retries
                .get(&kind)
                .copied()
                .unwrap_or(0);
            (n > 0).then_some((kind, n))
        })
        .collect();
    if rows.is_empty() {
        return;
    }
    rows.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    println!();
    println!("─── Leader-change events ───");
    println!("  (first-attempt LeaderChange; retry outcome landed separately in Outcomes)");
    for (kind, count) in rows {
        println!("    {:<16} {:>10}", kind.label(), count);
    }
}

fn print_ledger_distribution(snap: &MetricsSnapshot) {
    if snap.per_ledger.is_empty() {
        return;
    }
    let mut rows: Vec<(&String, &u64)> = snap.per_ledger.iter().collect();
    rows.sort_by(|a, b| b.1.cmp(a.1));
    println!();
    println!("─── Top ledgers by op count ───");
    let total: u64 = snap.per_ledger.values().sum();
    let shown = rows.iter().take(10);
    for (name, count) in shown {
        let pct = if total == 0 {
            0.0
        } else {
            (**count as f64 / total as f64) * 100.0
        };
        println!("  {:<40} {:>10} {:>6.2}%", trim(name, 40), count, pct);
    }
    if rows.len() > 10 {
        let tail: u64 = rows.iter().skip(10).map(|(_, c)| **c).sum();
        let pct = if total == 0 {
            0.0
        } else {
            (tail as f64 / total as f64) * 100.0
        };
        println!(
            "  {:<40} {:>10} {:>6.2}%",
            format!("... ({} more)", rows.len() - 10),
            tail,
            pct,
        );
    }
}

fn trim(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max - 1).collect();
        format!("{head}…")
    }
}

fn print_aggregate(h: &HistogramSnapshot) {
    if h.count == 0 {
        return;
    }
    println!(
        "{:<14} {:>10} {:>9} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "(aggregate)",
        h.count,
        "-",
        fmt_dur(&h.p50),
        fmt_dur(&h.p95),
        fmt_dur(&h.p99),
        fmt_dur(&h.p999),
        fmt_dur(&h.max),
    );
}

fn print_outcome_breakdown(snap: &MetricsSnapshot) {
    println!();
    println!("─── Outcomes ───");
    for &kind in OpKind::ALL {
        let mut rows: Vec<(Outcome, u64)> = Outcome::ALL
            .iter()
            .filter_map(|&o| {
                let count = snap.per_kind_outcome.get(&(kind, o)).copied().unwrap_or(0);
                if count == 0 {
                    None
                } else {
                    Some((o, count))
                }
            })
            .collect();
        if rows.is_empty() {
            continue;
        }
        rows.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        println!("  {}", kind.label());
        for (outcome, count) in rows {
            println!("    {:<16} {:>10}", outcome.label(), count);
        }
    }
}

fn fmt_dur(d: &Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}
