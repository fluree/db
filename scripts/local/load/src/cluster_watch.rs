//! Optional `--watch-cluster` annotation task.
//!
//! Polls `/cluster/status` on one of a list of URLs at a configurable
//! interval, prints a marker whenever:
//!
//! - the leader changes,
//! - the voter set changes (ownership recalc relevant: every change
//!   to the voter set redistributes work-queue ownership via the
//!   rendezvous-hash mirror in `crate::ownership`),
//! - the term advances (often coincident with a leader change, but
//!   worth surfacing separately to catch election storms).
//!
//! Multiple URLs are supported so a chaos run that kills the node
//! whose raft port we were watching doesn't leave the watcher blind
//! for the rest of the run. On each tick the poller walks the list
//! in order and uses the first URL that responds; the previously
//! successful URL is tried first the next tick, so a healthy watch
//! stays sticky.
//!
//! When the voter set changes, the task also computes — locally,
//! via the rendezvous mirror — how many of the currently-registered
//! ledgers' main branches would reassign owners. That number is the
//! one the user explicitly asked to surface: "23/100 branches
//! reassigned at t=15.2s."

use crate::ledger_state::LedgerState;
use crate::ownership;
use reqwest::Client;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Mirror of `fluree_db_consensus::raft::admin::ClusterStatus`'s
/// wire shape. Kept here as a leaf type so the load tool doesn't
/// depend on the consensus crate.
#[derive(Debug, Clone, Deserialize)]
struct ClusterStatus {
    current_leader: Option<u64>,
    current_term: u64,
    #[allow(dead_code)]
    last_applied_index: Option<u64>,
    voters: BTreeSet<u64>,
    #[allow(dead_code)]
    learners: BTreeSet<u64>,
}

pub async fn run(
    urls: Vec<String>,
    interval: Duration,
    ledgers: LedgerState,
    mut shutdown: watch::Receiver<bool>,
) {
    if urls.is_empty() {
        eprintln!("[watch-cluster] no URLs supplied; watcher exiting");
        return;
    }
    let http = match Client::builder().timeout(Duration::from_secs(3)).build() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[watch-cluster] could not build HTTP client: {e}");
            return;
        }
    };
    let started = Instant::now();
    let status_urls: Vec<String> = urls
        .iter()
        .map(|u| format!("{}/cluster/status", u.trim_end_matches('/')))
        .collect();
    let mut last: Option<ClusterStatus> = None;
    // Sticky preference for the URL that last succeeded, so a healthy
    // watch doesn't cycle through the list on every tick.
    let mut preferred: usize = 0;

    loop {
        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = &mut sleep => {}
        }
        if *shutdown.borrow() {
            return;
        }

        let (next, chosen_idx) = match poll(&http, &status_urls, preferred).await {
            Ok(v) => v,
            Err(errors) => {
                let msg = errors.join("; ");
                eprintln!(
                    "[watch-cluster t={:>5.1}s] all polls failed: {msg}",
                    started.elapsed().as_secs_f64()
                );
                continue;
            }
        };
        if chosen_idx != preferred {
            println!(
                "[watch-cluster t={:>5.1}s] failover: now polling {}",
                started.elapsed().as_secs_f64(),
                urls[chosen_idx],
            );
            preferred = chosen_idx;
        }

        if let Some(prev) = &last {
            announce_changes(&started, prev, &next, &ledgers);
        } else {
            println!(
                "[watch-cluster t={:>5.1}s] initial: leader={:?} term={} voters={:?}",
                started.elapsed().as_secs_f64(),
                next.current_leader,
                next.current_term,
                next.voters,
            );
        }
        last = Some(next);
    }
}

/// Walk the URL list starting from `preferred`; return the first
/// successful response and the URL's index. On complete failure
/// return the collected per-URL errors so the caller can log a
/// single line rather than N.
async fn poll(
    http: &Client,
    urls: &[String],
    preferred: usize,
) -> Result<(ClusterStatus, usize), Vec<String>> {
    let n = urls.len();
    let mut errors = Vec::with_capacity(n);
    for offset in 0..n {
        let idx = (preferred + offset) % n;
        match fetch(http, &urls[idx]).await {
            Ok(s) => return Ok((s, idx)),
            Err(e) => errors.push(format!("{}: {e}", urls[idx])),
        }
    }
    Err(errors)
}

async fn fetch(http: &Client, url: &str) -> Result<ClusterStatus, String> {
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| format!("get failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("status {}", resp.status()));
    }
    let body: ClusterStatus = resp.json().await.map_err(|e| format!("decode: {e}"))?;
    Ok(body)
}

fn announce_changes(
    started: &Instant,
    prev: &ClusterStatus,
    next: &ClusterStatus,
    ledgers: &LedgerState,
) {
    let t = started.elapsed().as_secs_f64();
    if prev.current_leader != next.current_leader {
        println!(
            "[watch-cluster t={t:>5.1}s] leader change: {:?} → {:?} (term {} → {})",
            prev.current_leader, next.current_leader, prev.current_term, next.current_term,
        );
    } else if prev.current_term != next.current_term {
        println!(
            "[watch-cluster t={t:>5.1}s] term advance: {} → {} (leader unchanged: {:?})",
            prev.current_term, next.current_term, next.current_leader,
        );
    }
    if prev.voters != next.voters {
        let pre: Vec<u64> = prev.voters.iter().copied().collect();
        let post: Vec<u64> = next.voters.iter().copied().collect();
        let reassigned = count_reassignments(&pre, &post, ledgers);
        let pool = ledgers.len();
        println!(
            "[watch-cluster t={t:>5.1}s] voter set change: {pre:?} → {post:?} \
             — {reassigned}/{pool} known ledger main-branch owners reassigned",
        );
    }
}

fn count_reassignments(pre: &[u64], post: &[u64], ledgers: &LedgerState) -> usize {
    let names = ledgers.snapshot();
    let mut moved = 0;
    for name in &names {
        let before = ownership::owner(name, "main", pre);
        let after = ownership::owner(name, "main", post);
        if before != after {
            moved += 1;
        }
    }
    moved
}
