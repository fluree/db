//! Worker pool that drives the workload at a closed-loop concurrency
//! level until the configured duration or total-ops cap is reached.
//!
//! Open-loop (rate-paced) dispatch is intentionally not implemented
//! in this first cut — it's a different control regime (rate
//! scheduling, queue management when targets blacklist, coordinated
//! omission handling) and adds enough complexity that it would dwarf
//! the closed-loop path. Closed-loop with N concurrent workers
//! produces the saturation curve operators most often want: "what's
//! the steady-state TPS at N in-flight requests."

use crate::client::ClusterClient;
use crate::ledger_state::LedgerState;
use crate::metrics::Metrics;
use crate::ops::OpKind;
use crate::workload::Workload;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// How the runner decides when to stop.
#[derive(Debug, Clone)]
pub enum StopCondition {
    /// Run for at most this much wall-clock; workers see a shutdown
    /// signal as soon as the deadline passes.
    Duration(Duration),
    /// Stop once this many ops have been dispatched (regardless of
    /// outcome). Each worker checks before issuing.
    TotalOps(u64),
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub concurrency: usize,
    pub stop: StopCondition,
}

pub async fn run(
    config: RunnerConfig,
    workload: Workload,
    ledgers: LedgerState,
    client: ClusterClient,
    metrics: Arc<Metrics>,
) -> RunSummary {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let issued = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let started = Instant::now();
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let workload = workload.clone();
        let ledgers = ledgers.clone();
        let client = client.clone();
        let metrics = Arc::clone(&metrics);
        let issued = Arc::clone(&issued);
        let stop = config.stop.clone();
        let shutdown_rx = shutdown_rx.clone();
        handles.push(tokio::spawn(worker(
            workload,
            ledgers,
            client,
            metrics,
            issued,
            stop,
            shutdown_rx,
        )));
    }

    // Watchdog: trigger shutdown when the duration elapses. For
    // total-ops mode the workers shut themselves down via `issued`,
    // so no watchdog needed.
    if let StopCondition::Duration(d) = &config.stop {
        let shutdown_tx = shutdown_tx.clone();
        let d = *d;
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            let _ = shutdown_tx.send(true);
        });
    }

    // Also catch Ctrl-C so a manual interrupt produces a clean
    // summary rather than a torn-off process.
    let shutdown_tx_for_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx_for_ctrlc.send(true);
        }
    });

    for h in handles {
        let _ = h.await;
    }
    let elapsed = started.elapsed();

    RunSummary {
        elapsed,
        ledger_count: ledgers.snapshot().len(),
    }
}

async fn worker(
    workload: Workload,
    ledgers: LedgerState,
    client: ClusterClient,
    metrics: Arc<Metrics>,
    issued: Arc<std::sync::atomic::AtomicU64>,
    stop: StopCondition,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        if let StopCondition::TotalOps(cap) = &stop {
            if issued.load(std::sync::atomic::Ordering::Relaxed) >= *cap {
                return;
            }
        }
        let Some(op) = workload.next() else {
            // Workload had nothing to issue (e.g. transact-only with
            // empty pool, or wide-fanout in its pre-creates window).
            // Briefly yield and retry — beats spinning.
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        let op_kind = op.kind;
        let ledger = op.ledger.clone();
        issued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result = tokio::select! {
            _ = shutdown_rx.changed() => return,
            r = client.dispatch(op) => r,
        };
        metrics.record(
            result.kind,
            &result.ledger,
            result.outcome,
            result.latency_ns,
        );
        // A landed CreateLedger adds to the pool every subsequent op
        // can target. Done after recording so the metric is stamped
        // regardless of pool-registration ordering.
        if matches!(op_kind, OpKind::CreateLedger) && result.outcome.is_landed() {
            ledgers.register(&ledger);
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub elapsed: Duration,
    pub ledger_count: usize,
}
