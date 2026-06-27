//! BSBM Explore-and-Update — mixed query+update throughput with a LIVE indexer.
//!
//! The holistic signal for the segmented-novelty (LSM) tradeoff: it collapses the
//! whole write/read story into one number — operations/sec (and queries/hr) under
//! a realistic mix of update transactions and Explore queries, with a REAL
//! background indexer draining novelty on its thresholds (production shape).
//!
//! - Updates get much cheaper with LSM (O(batch) append vs O(novelty) re-merge).
//! - Queries split: point lookups hit the base index (unaffected by the overlay);
//!   scans/joins pay an O(overlay) penalty until the index drains.
//! - Net throughput is the integral of both, gated by whether the background
//!   indexer keeps novelty bounded under the update rate.
//!
//! Run this on `main` and on the LSM branch and compare ops/s. If LSM lifts the
//! mixed number, the write win outweighs the read penalty; if it nets flat/down,
//! the read penalty (or indexer falling behind) eats the win.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `BX_PRODUCTS` | `20000` | base dataset size (products; ~5x more triples) |
//! | `BX_DURATION_S` | `20` | timed mixed-loop duration |
//! | `BX_QUERIES_PER_UPDATE` | `8` | queries per update txn (mix ratio) |
//! | `BX_MIN_BYTES` | `4000000` | reindex_min_bytes (background index START trigger) |
//! | `BX_MAX_BYTES` | `64000000` | reindex_max_bytes (commit backpressure ceiling) |
//! | `BX_DB_DIR` | `/dev/shm/bsbm-xu` | storage dir |
//!
//! ## Run
//! ```bash
//! BX_DB_DIR=/dev/shm/bx cargo run --release \
//!   --example bsbm_explore_update -p fluree-db-api --features native
//! ```

use std::time::{Duration, Instant};

use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, TxnOpts};
use serde_json::{json, Value as JsonValue};

use fluree_bench_support::gen::bsbm::{bsbm_data_to_turtle, generate_dataset};

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn ctx() -> JsonValue {
    json!({ "ex": "http://example.org/ns/", "bsbm": "http://example.org/bsbm/" })
}

/// One update transaction: a new product + one review for it (a realistic BSBM
/// write — a moderate insert, NOT a bulk load).
fn update_doc(pid: usize, vendor_id: usize, person_id: usize) -> JsonValue {
    json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": format!("ex:product-{pid:06}"),
                "@type": "bsbm:Product",
                "bsbm:label": format!("Product {pid:06}"),
                "bsbm:productType": "Electronics",
                "bsbm:vendor": { "@id": format!("ex:vendor-{vendor_id:06}") },
                "bsbm:price": 1000 + ((pid * 37) % 50000) as i64
            },
            {
                "@id": format!("ex:review-x{pid:06}"),
                "@type": "bsbm:Review",
                "bsbm:reviewFor": { "@id": format!("ex:product-{pid:06}") },
                "bsbm:reviewer": { "@id": format!("ex:person-{person_id:06}") },
                "bsbm:rating": (1 + (pid % 5)) as i64,
                "bsbm:text": "fresh review"
            }
        ]
    })
}

/// The realistic Explore mix: a point lookup (hits the index, overlay-pruned),
/// a predicate scan, and a 2-pattern join (both pay the overlay penalty).
#[derive(Clone, Copy, PartialEq)]
enum QType {
    Lookup,
    Scan,
    Join,
}

fn query_for(qt: QType, lookup_pid: usize) -> JsonValue {
    match qt {
        QType::Lookup => json!({
            "@context": ctx(),
            "select": { format!("ex:product-{lookup_pid:06}"): ["*"] }
        }),
        QType::Scan => json!({
            "@context": ctx(),
            "select": ["?p"],
            "where": { "id": "?p", "bsbm:productType": "Electronics" },
            "limit": 50
        }),
        QType::Join => json!({
            "@context": ctx(),
            "select": ["?p", "?rating"],
            "where": [
                { "id": "?p", "bsbm:productType": "Electronics" },
                { "id": "?r", "bsbm:reviewFor": "?p", "bsbm:rating": "?rating" }
            ],
            "limit": 50
        }),
    }
}

/// 50% lookups, 25% scan, 25% join — lookup-heavy like real BSBM Explore.
const MIX: [QType; 8] = [
    QType::Lookup,
    QType::Scan,
    QType::Lookup,
    QType::Join,
    QType::Lookup,
    QType::Scan,
    QType::Lookup,
    QType::Join,
];

fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn report(name: &str, mut v: Vec<u128>) {
    v.sort_unstable();
    let mean = if v.is_empty() {
        0
    } else {
        v.iter().sum::<u128>() / v.len() as u128
    };
    println!(
        "  {name:<16} n={:<7} mean={:>8}us  p50={:>8}us  p99={:>9}us",
        v.len(),
        mean,
        pct(&v, 0.50),
        pct(&v, 0.99),
    );
}

async fn insert_with_backpressure(
    fluree: &Fluree,
    ledger_id: &str,
    doc: &JsonValue,
) -> (u128, u128) {
    let mut stall = Duration::ZERO;
    loop {
        let base = fluree.ledger(ledger_id).await.expect("reload base");
        let t0 = Instant::now();
        match fluree.insert(base, doc).await {
            Ok(_res) => return (t0.elapsed().as_micros(), stall.as_micros()),
            Err(e) if format!("{e:?}").contains("NoveltyAtMax") => {
                let s = Duration::from_micros(500);
                tokio::time::sleep(s).await;
                stall += s;
                assert!(
                    stall < Duration::from_secs(60),
                    "indexer not draining (>60s at MAX)"
                );
            }
            Err(e) => panic!("update insert failed: {e:?}"),
        }
    }
}

async fn run() {
    let n_products = env_usize("BX_PRODUCTS", 20_000);
    let duration_s = env_usize("BX_DURATION_S", 20);
    let qpu = env_usize("BX_QUERIES_PER_UPDATE", 8);
    let min_bytes = env_usize("BX_MIN_BYTES", 4_000_000);
    let max_bytes = env_usize("BX_MAX_BYTES", 64_000_000);
    let dir = env_str("BX_DB_DIR", "/dev/shm/bsbm-xu");
    let ledger_id = "bsbm/xu:main";

    let _ = std::fs::remove_dir_all(&dir);
    let fluree = FlureeBuilder::file(dir.clone())
        .with_indexing_thresholds(min_bytes, max_bytes)
        .build()
        .expect("build (file + live indexing)");

    println!("======================================================================");
    println!("  BSBM EXPLORE-AND-UPDATE  (live background indexer)");
    println!("======================================================================");
    println!("  base: {n_products} products | mix: 1 update : {qpu} queries | run {duration_s}s");
    println!("  indexer: min={min_bytes}B max={max_bytes}B\n");

    // ---- Base: load + let the background indexer drain it behind the index ----
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    let turtle = bsbm_data_to_turtle(&generate_dataset(n_products));
    let idx_cfg = IndexConfig {
        reindex_min_bytes: min_bytes,
        reindex_max_bytes: max_bytes,
    };
    let t0 = Instant::now();
    let after_base = fluree
        .insert_turtle_with_opts(
            ledger0,
            &turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &idx_cfg,
            None,
        )
        .await
        .expect("base insert")
        .ledger;
    let base_t = after_base.t();
    println!(
        "  base loaded ({base_t} commits) in {:.1}s; waiting for index to drain...",
        t0.elapsed().as_secs_f64()
    );

    // Poll until the background index catches up (index_t -> t).
    let w0 = Instant::now();
    loop {
        let l = fluree.ledger(ledger_id).await.expect("reload");
        if l.index_t() >= base_t || w0.elapsed() > Duration::from_secs(180) {
            println!(
                "  base indexed: index_t={} t={} novelty={:.2}MiB after {:.1}s\n",
                l.index_t(),
                l.t(),
                l.novelty_size() as f64 / (1024.0 * 1024.0),
                w0.elapsed().as_secs_f64()
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Sanity: every query type must execute (catch FQL issues before timing).
    {
        let base = fluree.ledger(ledger_id).await.expect("reload");
        let db = GraphDb::from_ledger_state(&base);
        for qt in [QType::Lookup, QType::Scan, QType::Join] {
            fluree
                .query(&db, &query_for(qt, n_products / 2))
                .await
                .expect("sanity query");
        }
    }

    // ---- Timed mixed loop ----
    let lookup_pid = n_products / 2; // a stable base product (always indexed)
    let n_vendors = (n_products / 50).max(1);
    let n_persons = (n_products / 10).max(1);
    let mut next_pid = n_products;
    let mut mix_i = 0usize;

    let mut update_us: Vec<u128> = Vec::new();
    let mut stall_us: Vec<u128> = Vec::new();
    let mut q_lookup: Vec<u128> = Vec::new();
    let mut q_scan: Vec<u128> = Vec::new();
    let mut q_join: Vec<u128> = Vec::new();
    let mut max_novelty_mib = 0.0f64;

    let run0 = Instant::now();
    let deadline = run0 + Duration::from_secs(duration_s as u64);
    while Instant::now() < deadline {
        // One update.
        let doc = update_doc(next_pid, next_pid % n_vendors, next_pid % n_persons);
        let (uus, sus) = insert_with_backpressure(&fluree, ledger_id, &doc).await;
        update_us.push(uus);
        if sus > 0 {
            stall_us.push(sus);
        }
        next_pid += 1;

        // qpu queries against the freshly reloaded state (reflects overlay + drains).
        let ledger = fluree.ledger(ledger_id).await.expect("reload for query");
        max_novelty_mib = max_novelty_mib.max(ledger.novelty_size() as f64 / (1024.0 * 1024.0));
        let db = GraphDb::from_ledger_state(&ledger);
        for _ in 0..qpu {
            let qt = MIX[mix_i % MIX.len()];
            mix_i += 1;
            let q = query_for(qt, lookup_pid);
            let t0 = Instant::now();
            fluree.query(&db, &q).await.expect("query");
            let us = t0.elapsed().as_micros();
            match qt {
                QType::Lookup => q_lookup.push(us),
                QType::Scan => q_scan.push(us),
                QType::Join => q_join.push(us),
            }
        }
    }
    let wall = run0.elapsed();

    // ---- Report ----
    let updates = update_us.len();
    let queries = q_lookup.len() + q_scan.len() + q_join.len();
    let ops = updates + queries;
    let ops_s = ops as f64 / wall.as_secs_f64();
    let final_l = fluree.ledger(ledger_id).await.expect("final reload");

    println!("---- throughput ({:.1}s wall) ----", wall.as_secs_f64());
    println!("  updates={updates}  queries={queries}  total ops={ops}");
    println!(
        "  OPS/SEC = {ops_s:.0}   (queries/hr = {:.0})",
        queries as f64 / wall.as_secs_f64() * 3600.0
    );
    println!("\n---- latency ----");
    report("update", update_us);
    report("query:lookup", q_lookup);
    report("query:scan", q_scan);
    report("query:join", q_join);
    println!("\n---- indexer / novelty ----");
    println!(
        "  stalled updates (backpressure) : {}   final t={} index_t={} (lag {})",
        stall_us.len(),
        final_l.t(),
        final_l.index_t(),
        final_l.t() - final_l.index_t()
    );
    println!("  max novelty during run         : {max_novelty_mib:.2} MiB");
    println!("\n======================================================================");
    println!("  Compare OPS/SEC on main vs this branch. Lookups should stay flat both");
    println!("  ways; updates much cheaper on LSM; scan/join sensitive to overlay size");
    println!("  (how well the indexer kept novelty drained). Net ops/s is the verdict.");
    println!("======================================================================");

    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("multi-thread runtime");
    rt.block_on(run());
}
