// JSON-LD vs Turtle insert benchmarks.
//
// Measures throughput of sequential inserts using identical triples in both
// serialization formats, across a matrix of transaction counts and node sizes.
//
// ## Data model
//
// ~10% Company nodes with `ex:employees` and `ex:customers` refs to Person
// nodes within the same transaction.  ~90% Person nodes with scalar properties.
// All data is fully deterministic (no RNG) for reproducibility and future
// data-integrity tests.
//
// ## Matrix
//
//   formats:   jsonld, turtle
//   txn counts: 10, 100
//   nodes/txn:  10, 100, 1000
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench insert_formats
//
// Quick validation (1 iteration each, no stats):
//
//   cargo bench -p fluree-db-api --bench insert_formats -- --test

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TxnOpts};
use fluree_bench_support::gen::people::{generate_txn_data, txn_data_to_jsonld, txn_data_to_turtle};
use fluree_bench_support::{init_tracing_for_bench, next_ledger_alias};
use serde_json::Value as JsonValue;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Transaction counts to benchmark.
const TXN_COUNTS: &[usize] = &[10, 100];

/// Nodes per transaction to benchmark.
const NODES_PER_TXN: &[usize] = &[10, 100, 1_000];

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type BenchFluree = fluree_db_api::Fluree;
type BenchLedger = fluree_db_api::LedgerState;

// ---------------------------------------------------------------------------
// Pre-generated bench data
// ---------------------------------------------------------------------------
//
// `TxnData`, `PersonData`, `CompanyData`, `generate_txn_data`,
// `txn_data_to_jsonld`, and `txn_data_to_turtle` were lifted into
// `fluree_bench_support::gen::people` so other benches can reuse them.
// `PregenData` is bench-specific (couples Fluree-side calibration) and
// stays here.

struct PregenData {
    jsonld_txns: Vec<JsonValue>,
    turtle_txns: Vec<String>,
    /// Total flakes produced by inserting all transactions (calibrated once).
    total_flakes: u64,
}

/// Pre-generate all transaction data and do one calibration insert pass to
/// count total flakes. The flake count is deterministic (same data every run)
/// and identical for both formats, so one pass suffices.
fn pregen(
    rt: &Runtime,
    fluree: &BenchFluree,
    index_config: &IndexConfig,
    txn_count: usize,
    nodes_per_txn: usize,
) -> PregenData {
    let mut jsonld_txns = Vec::with_capacity(txn_count);
    let mut turtle_txns = Vec::with_capacity(txn_count);

    for txn_idx in 0..txn_count {
        let data = generate_txn_data(txn_idx, nodes_per_txn);
        jsonld_txns.push(txn_data_to_jsonld(&data));
        turtle_txns.push(txn_data_to_turtle(&data));
    }

    // Calibration: insert all txns once via Turtle (cheapest path) to count flakes.
    let total_flakes = rt.block_on(async {
        let alias = next_ledger_alias("cal");
        let mut ledger = fluree.create_ledger(&alias).await.unwrap();
        let mut flakes = 0u64;
        for txn in &turtle_txns {
            let result = fluree
                .insert_turtle_with_opts(
                    ledger,
                    txn,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    index_config,
                )
                .await
                .unwrap();
            flakes += result.receipt.flake_count as u64;
            ledger = result.ledger;
        }
        flakes
    });

    PregenData {
        jsonld_txns,
        turtle_txns,
        total_flakes,
    }
}

// ---------------------------------------------------------------------------
// Insert runners
// ---------------------------------------------------------------------------

async fn run_jsonld_inserts(
    fluree: &BenchFluree,
    ledger: BenchLedger,
    txns: &[JsonValue],
    index_config: &IndexConfig,
) -> BenchLedger {
    let mut ledger = ledger;
    for txn in txns {
        let result = fluree
            .insert_with_opts(
                ledger,
                txn,
                TxnOpts::default(),
                CommitOpts::default(),
                index_config,
            )
            .await
            .unwrap();
        ledger = result.ledger;
    }
    ledger
}

async fn run_turtle_inserts(
    fluree: &BenchFluree,
    ledger: BenchLedger,
    txns: &[String],
    index_config: &IndexConfig,
) -> BenchLedger {
    let mut ledger = ledger;
    for txn in txns {
        let result = fluree
            .insert_turtle_with_opts(
                ledger,
                txn,
                TxnOpts::default(),
                CommitOpts::default(),
                index_config,
            )
            .await
            .unwrap();
        ledger = result.ledger;
    }
    ledger
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

struct ScenarioResult {
    txn_count: usize,
    nodes_per_txn: usize,
    total_flakes: u64,
    jsonld_ms: f64,
    turtle_ms: f64,
}

fn format_flakes_per_sec(flakes: u64, ms: f64) -> String {
    let per_sec = flakes as f64 / (ms / 1000.0);
    if per_sec >= 1_000_000.0 {
        format!("{:.2}M", per_sec / 1_000_000.0)
    } else if per_sec >= 1_000.0 {
        format!("{:.1}K", per_sec / 1_000.0)
    } else {
        format!("{per_sec:.0}")
    }
}

fn print_summary(results: &[ScenarioResult]) {
    eprintln!();
    eprintln!("==========================================================================");
    eprintln!("  INSERT BENCHMARK SUMMARY");
    eprintln!("==========================================================================");
    eprintln!(
        "  {:>6} x {:>5}  {:>8}  {:>10} {:>10}  {:>12} {:>12}  {:>6}",
        "txns", "nodes", "flakes", "jsonld", "turtle", "jsonld fl/s", "turtle fl/s", "ratio"
    );
    eprintln!("  {}", "-".repeat(82));

    for r in results {
        let jld_fps = format_flakes_per_sec(r.total_flakes, r.jsonld_ms);
        let ttl_fps = format_flakes_per_sec(r.total_flakes, r.turtle_ms);
        let ratio = r.jsonld_ms / r.turtle_ms;
        eprintln!(
            "  {:>6} x {:>5}  {:>8}  {:>8.1}ms {:>8.1}ms  {:>12} {:>12}  {:>5.2}x",
            r.txn_count,
            r.nodes_per_txn,
            r.total_flakes,
            r.jsonld_ms,
            r.turtle_ms,
            jld_fps,
            ttl_fps,
            ratio,
        );
    }

    eprintln!("  {}", "-".repeat(82));
    eprintln!("  ratio = jsonld_time / turtle_time (>1 means turtle is faster)");
    eprintln!();
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

/// Time a single run of all inserts, returning elapsed milliseconds.
fn time_jsonld_run(
    rt: &Runtime,
    fluree: &BenchFluree,
    txns: &[JsonValue],
    index_config: &IndexConfig,
) -> f64 {
    rt.block_on(async {
        let alias = next_ledger_alias("sum-jld");
        let ledger = fluree.create_ledger(&alias).await.unwrap();
        let start = std::time::Instant::now();
        let _ = run_jsonld_inserts(fluree, ledger, txns, index_config).await;
        start.elapsed().as_secs_f64() * 1000.0
    })
}

fn time_turtle_run(
    rt: &Runtime,
    fluree: &BenchFluree,
    txns: &[String],
    index_config: &IndexConfig,
) -> f64 {
    rt.block_on(async {
        let alias = next_ledger_alias("sum-ttl");
        let ledger = fluree.create_ledger(&alias).await.unwrap();
        let start = std::time::Instant::now();
        let _ = run_turtle_inserts(fluree, ledger, txns, index_config).await;
        start.elapsed().as_secs_f64() * 1000.0
    })
}

fn bench_insert_formats(c: &mut Criterion) {
    init_tracing_for_bench();

    let rt = Runtime::new().unwrap();
    let fluree = FlureeBuilder::memory().build_memory();

    let index_config = IndexConfig {
        reindex_min_bytes: 500_000_000,
        reindex_max_bytes: 500_000_000,
    };

    let mut group = c.benchmark_group("insert_formats");
    group.sample_size(10);

    let mut summary: Vec<ScenarioResult> = Vec::new();

    for &txn_count in TXN_COUNTS {
        for &nodes_per_txn in NODES_PER_TXN {
            let total_nodes = txn_count * nodes_per_txn;

            eprintln!(
                "  [pregen] {txn_count} txns x {nodes_per_txn} nodes/txn = {total_nodes} total nodes ..."
            );
            let data = pregen(&rt, &fluree, &index_config, txn_count, nodes_per_txn);

            eprintln!(
                "  [calibrated] {} total flakes ({:.1} flakes/node)",
                data.total_flakes,
                data.total_flakes as f64 / total_nodes as f64
            );

            // Throughput in flakes so Criterion reports flakes/second.
            group.throughput(Throughput::Elements(data.total_flakes));

            let param_label = format!("{txn_count}txn_{nodes_per_txn}nodes");

            // --- JSON-LD ---
            group.bench_with_input(
                BenchmarkId::new("jsonld", &param_label),
                &data,
                |b, data| {
                    b.iter(|| {
                        let alias = next_ledger_alias("jld");
                        rt.block_on(async {
                            let ledger = fluree.create_ledger(&alias).await.unwrap();
                            black_box(
                                run_jsonld_inserts(
                                    &fluree,
                                    ledger,
                                    &data.jsonld_txns,
                                    &index_config,
                                )
                                .await,
                            )
                        })
                    });
                },
            );

            // --- Turtle ---
            group.bench_with_input(
                BenchmarkId::new("turtle", &param_label),
                &data,
                |b, data| {
                    b.iter(|| {
                        let alias = next_ledger_alias("ttl");
                        rt.block_on(async {
                            let ledger = fluree.create_ledger(&alias).await.unwrap();
                            black_box(
                                run_turtle_inserts(
                                    &fluree,
                                    ledger,
                                    &data.turtle_txns,
                                    &index_config,
                                )
                                .await,
                            )
                        })
                    });
                },
            );

            // Collect a single timed run for the summary table.
            let jsonld_ms = time_jsonld_run(&rt, &fluree, &data.jsonld_txns, &index_config);
            let turtle_ms = time_turtle_run(&rt, &fluree, &data.turtle_txns, &index_config);
            summary.push(ScenarioResult {
                txn_count,
                nodes_per_txn,
                total_flakes: data.total_flakes,
                jsonld_ms,
                turtle_ms,
            });
        }
    }

    group.finish();

    print_summary(&summary);
}

criterion_group!(benches, bench_insert_formats);
criterion_main!(benches);
