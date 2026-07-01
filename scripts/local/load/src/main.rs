//! `fluree-load` — HTTP load harness for Fluree single-node and Raft
//! cluster deployments.
//!
//! Same tool against either backend; the only thing that changes is
//! the URL list. Designed to exercise the parts of the Raft path that
//! the in-process bench suite doesn't touch: real wire-level latency,
//! per-branch work queues, idempotency cache behavior, rendezvous-hash
//! ownership recalculation under chaos.

mod client;
mod cluster_watch;
mod ledger_state;
mod metrics;
mod ops;
mod ownership;
mod reporter;
mod runner;
mod workload;

use clap::Parser;
use reqwest::Url;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::client::ClusterClient;
use crate::ledger_state::LedgerState;
use crate::metrics::Metrics;
use crate::runner::{RunnerConfig, StopCondition};
use crate::workload::{Workload, WorkloadShape, WorkloadTuning};

#[derive(Parser, Debug)]
#[command(
    name = "fluree-load",
    about = "HTTP load harness for Fluree single-node and Raft clusters.",
    long_about = "Drives configurable workloads against one or more Fluree HTTP \
endpoints, reports live TPS / latency percentiles, and emits a per-op \
summary at the end. Works against a single-node server or a Raft cluster \
— same tool, different `--addrs`.\n\n\
EXAMPLES\n  \
fluree-load --addrs http://localhost:8090 --workload single-pound --duration 30s\n  \
fluree-load --addrs http://localhost:8091,http://localhost:8092,http://localhost:8093 \
--workload wide-fanout --duration 60s --concurrency 64"
)]
struct Cli {
    /// Comma-separated list of base URLs to dispatch against. The
    /// tool round-robins requests across them; for a single-node
    /// server pass one URL.
    #[arg(long, value_delimiter = ',', required = true)]
    addrs: Vec<Url>,

    /// Workload shape. See README for per-shape semantics.
    #[arg(long, default_value = "single-pound", value_parser = parse_workload_shape)]
    workload: WorkloadShape,

    /// Run wall-clock duration. Mutually exclusive with `--total`.
    /// Accepts simple Go-style strings: `30s`, `5m`, `1h`.
    #[arg(long, value_parser = parse_duration, conflicts_with = "total")]
    duration: Option<Duration>,

    /// Stop after this many dispatched ops. Mutually exclusive with
    /// `--duration`.
    #[arg(long, conflicts_with = "duration")]
    total: Option<u64>,

    /// Concurrent worker tasks dispatching ops in parallel
    /// (closed-loop). Each worker is one in-flight request.
    #[arg(long, default_value_t = 32)]
    concurrency: usize,

    /// Per-request HTTP timeout. Trips the `Timeout` outcome class.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    request_timeout: Duration,

    /// Progress-line cadence.
    #[arg(long, value_parser = parse_duration, default_value = "1s")]
    progress_interval: Duration,

    /// Consecutive failures before a target is blacklisted from
    /// round-robin. 0 disables blacklisting.
    #[arg(long, default_value_t = 10)]
    blacklist_threshold: u64,

    /// How long a blacklisted target sits out before becoming
    /// eligible again.
    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    blacklist_window: Duration,

    /// `wide-fanout`: total ledgers to create over the run.
    #[arg(long, default_value_t = 50)]
    target_ledger_count: u64,

    /// `wide-fanout`: every Nth op is a CreateLedger until the cap
    /// is reached.
    #[arg(long, default_value_t = 50)]
    wide_fanout_create_every: u64,

    /// `multitenant`: every Nth op is a CreateLedger.
    #[arg(long, default_value_t = 100)]
    multitenant_create_every: u64,

    /// Prefix used for generated ledger names. Run-id (a ULID) is
    /// appended automatically so concurrent runs against the same
    /// cluster never collide.
    #[arg(long, default_value = "load")]
    ledger_prefix: String,

    /// `transact-only`: comma-separated ledger names the workload
    /// assumes already exist. Required when running this shape.
    #[arg(long, value_delimiter = ',')]
    seeded_ledger: Vec<String>,

    /// Background poll of `/cluster/status` on this URL. When the
    /// leader, term, or voter set changes, an annotation line is
    /// printed; voter-set changes also report how many of the
    /// currently-known ledger main-branch owners would reassign
    /// (computed locally via the rendezvous-hash mirror).
    #[arg(long)]
    watch_cluster: Option<Url>,

    /// `--watch-cluster` poll interval.
    #[arg(long, value_parser = parse_duration, default_value = "500ms")]
    watch_interval: Duration,
}

fn parse_workload_shape(s: &str) -> Result<WorkloadShape, String> {
    WorkloadShape::from_str(s)
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    // Minimal Go-style suffix parser. Sufficient for CLI; not meant
    // to cover every humantime corner case.
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("empty duration".into());
    }
    let (num_part, unit, multiplier_ns) = if let Some(rest) = trimmed.strip_suffix("ms") {
        (rest, "ms", 1_000_000u128)
    } else if let Some(rest) = trimmed.strip_suffix('s') {
        (rest, "s", 1_000_000_000u128)
    } else if let Some(rest) = trimmed.strip_suffix('m') {
        (rest, "m", 60u128 * 1_000_000_000)
    } else if let Some(rest) = trimmed.strip_suffix('h') {
        (rest, "h", 3600u128 * 1_000_000_000)
    } else {
        return Err(format!(
            "unknown duration suffix in {trimmed:?}; expected ms/s/m/h"
        ));
    };
    let value: u64 = num_part
        .parse()
        .map_err(|e| format!("could not parse {num_part:?} as integer ({unit}): {e}"))?;
    let ns = (value as u128) * multiplier_ns;
    let ns_u64 = u64::try_from(ns).map_err(|_| format!("duration overflows u64 ns: {trimmed}"))?;
    Ok(Duration::from_nanos(ns_u64))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if matches!(
        cli.workload,
        WorkloadShape::TransactOnly | WorkloadShape::QueryOnly
    ) && cli.seeded_ledger.is_empty()
    {
        return Err(format!(
            "--workload {} requires --seeded-ledger NAME[,NAME...]",
            match cli.workload {
                WorkloadShape::TransactOnly => "transact-only",
                WorkloadShape::QueryOnly => "query-only",
                _ => unreachable!(),
            }
        )
        .into());
    }

    let stop = match (cli.duration, cli.total) {
        (Some(d), None) => StopCondition::Duration(d),
        (None, Some(t)) => StopCondition::TotalOps(t),
        (None, None) => StopCondition::Duration(Duration::from_secs(30)),
        (Some(_), Some(_)) => unreachable!("clap conflicts_with prevents both being set"),
    };

    let client = ClusterClient::new(
        cli.addrs.clone(),
        cli.request_timeout,
        cli.blacklist_threshold.max(1),
        cli.blacklist_window,
    )?;

    let ledgers = LedgerState::new();
    let workload = Workload::new(
        cli.workload,
        WorkloadTuning {
            target_ledger_count: cli.target_ledger_count,
            wide_fanout_create_every: cli.wide_fanout_create_every,
            multitenant_create_every: cli.multitenant_create_every,
            ledger_prefix: cli.ledger_prefix,
            seeded_ledgers: cli.seeded_ledger,
        },
        ledgers.clone(),
    );

    let metrics = Arc::new(Metrics::new());
    let (progress_shutdown_tx, progress_shutdown_rx) = watch::channel(false);
    let progress_handle = tokio::spawn(reporter::live_progress(
        Arc::clone(&metrics),
        cli.progress_interval,
        progress_shutdown_rx,
    ));

    let watch_handle = cli.watch_cluster.as_ref().map(|url| {
        let url = url.to_string();
        let interval = cli.watch_interval;
        let ledgers = ledgers.clone();
        let shutdown_rx = progress_shutdown_tx.subscribe();
        tokio::spawn(crate::cluster_watch::run(
            url,
            interval,
            ledgers,
            shutdown_rx,
        ))
    });

    let run_config = RunnerConfig {
        concurrency: cli.concurrency,
        stop,
    };
    let summary = runner::run(run_config, workload, ledgers, client, Arc::clone(&metrics)).await;

    // Stop the live progress task and wait for it before printing the
    // final summary, so the summary doesn't get interleaved with a
    // last progress line.
    let _ = progress_shutdown_tx.send(true);
    let _ = progress_handle.await;
    if let Some(h) = watch_handle {
        let _ = h.await;
    }

    let final_snap = metrics.snapshot();
    reporter::print_summary(&final_snap, &summary);

    Ok(())
}
