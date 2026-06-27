//! BSBM Explore+Update on the REAL server path — cached handle + live indexer.
//!
//! Unlike `bsbm_explore_update` (which reloaded `fluree.ledger()` per op — the
//! non-cached path a performant server never uses), this drives the production
//! posture: a long-lived **cached `LedgerHandle`** (`ledger_cached`), updates via
//! `stage().insert().execute()` (commit in place, segments accumulate), queries
//! via `handle.snapshot()` (warm), and a **live background indexer**
//! (`with_indexing_thresholds`) draining the overlay as it grows.
//!
//! Run on `main` and on this branch and compare ops/sec under a realistic
//! Explore+Update mix with indexing on.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `BM_PRODUCTS` | `20000` | indexed base size |
//! | `BM_DURATION_S` | `20` | mixed-loop duration |
//! | `BM_QUERIES_PER_UPDATE` | `8` | Explore queries per update txn |
//! | `BM_MIN_BYTES` | `1000000` | reindex_min_bytes (background index trigger) |
//! | `BM_MAX_BYTES` | `268435456` | reindex_max_bytes (commit backpressure ceiling) |
//! | `BM_DB_DIR` | `/dev/shm/bm` | storage dir |
//!
//! ## Run
//! ```bash
//! BM_DB_DIR=/tmp/bm cargo run --release \
//!   --example bsbm_server_mixed -p fluree-db-api --features native
//! ```

use std::time::{Duration, Instant};

use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, LedgerManagerConfig, TxnOpts,
};
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

#[derive(Clone, Copy)]
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

async fn rebuild_and_publish(fluree: &Fluree, ledger_id: &str) {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ledger record");
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("index rebuild");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");
}

async fn run() {
    let n_products = env_usize("BM_PRODUCTS", 20_000);
    let duration_s = env_usize("BM_DURATION_S", 20);
    let qpu = env_usize("BM_QUERIES_PER_UPDATE", 8);
    let min_bytes = env_usize("BM_MIN_BYTES", 1_000_000);
    let max_bytes = env_usize("BM_MAX_BYTES", 268_435_456);
    let dir = env_str("BM_DB_DIR", "/dev/shm/bm");
    let ledger_id = "bsbm/srv:main";

    let _ = std::fs::remove_dir_all(&dir);
    // Real server posture: caching ON + a live background indexer.
    let fluree = FlureeBuilder::file(dir.clone())
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .with_indexing_thresholds(min_bytes, max_bytes)
        .build()
        .expect("build (cache + indexing)");

    println!("======================================================================");
    println!("  BSBM EXPLORE+UPDATE — real server path (cached handle + live indexer)");
    println!("======================================================================");
    println!("  base: {n_products} products | mix 1 update : {qpu} queries | run {duration_s}s");
    println!("  indexer: min={min_bytes}B max={max_bytes}B\n");

    // ---- Base: insert + explicit index, then load the CACHED handle ----
    let t0 = Instant::now();
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    let turtle = bsbm_data_to_turtle(&generate_dataset(n_products));
    let base_cfg = IndexConfig {
        reindex_min_bytes: usize::MAX,
        reindex_max_bytes: usize::MAX,
    };
    fluree
        .insert_turtle_with_opts(
            ledger0,
            &turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &base_cfg,
            None,
        )
        .await
        .expect("base insert");
    rebuild_and_publish(&fluree, ledger_id).await;

    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("ledger_cached");
    let base_t = handle.snapshot().await.t;
    println!(
        "  base loaded + indexed in {:.1}s (cached handle at t={base_t})",
        t0.elapsed().as_secs_f64()
    );

    // ---- Verify stage() lands a commit on the cached handle ----
    {
        let doc = update_doc(n_products, 0, 0);
        fluree
            .stage(&handle)
            .insert(&doc)
            .execute()
            .await
            .expect("stage probe");
        let v = handle.snapshot().await;
        let ls = v.to_ledger_state();
        println!(
            "  stage probe: t {base_t} -> {} (novelty {} flakes) {}",
            ls.t(),
            ls.novelty().len(),
            if ls.t() > base_t {
                "OK — commits land on the cached handle"
            } else {
                "FAIL — stage did not update the handle"
            }
        );
        assert!(ls.t() > base_t, "stage() must update the cached handle");
    }

    // ---- Timed mixed loop ----
    let lookup_pid = n_products / 2;
    let n_vendors = (n_products / 50).max(1);
    let n_persons = (n_products / 10).max(1);
    let mut next_pid = n_products + 1;
    let mut mix_i = 0usize;

    let mut update_us: Vec<u128> = Vec::new();
    let mut q_lookup: Vec<u128> = Vec::new();
    let mut q_scan: Vec<u128> = Vec::new();
    let mut q_join: Vec<u128> = Vec::new();
    let mut max_novelty_mib = 0.0f64;
    let mut backpressure = 0usize;

    let run0 = Instant::now();
    let deadline = run0 + Duration::from_secs(duration_s as u64);
    while Instant::now() < deadline {
        // One update via the cached commit path (NoveltyAtMax → backpressure).
        let doc = update_doc(next_pid, next_pid % n_vendors, next_pid % n_persons);
        next_pid += 1;
        loop {
            let t = Instant::now();
            match fluree.stage(&handle).insert(&doc).execute().await {
                Ok(_) => {
                    update_us.push(t.elapsed().as_micros());
                    break;
                }
                Err(e) if format!("{e:?}").contains("NoveltyAtMax") => {
                    backpressure += 1;
                    tokio::time::sleep(Duration::from_micros(500)).await;
                }
                Err(e) => panic!("update failed: {e:?}"),
            }
        }

        // qpu warm queries on the cached snapshot.
        let ls = handle.snapshot().await.to_ledger_state();
        max_novelty_mib = max_novelty_mib.max(ls.novelty().len() as f64 * 80.0 / (1024.0 * 1024.0));
        let db = GraphDb::from_ledger_state(&ls);
        for _ in 0..qpu {
            let qt = MIX[mix_i % MIX.len()];
            mix_i += 1;
            let q = query_for(qt, lookup_pid);
            let t = Instant::now();
            fluree.query(&db, &q).await.expect("query");
            let us = t.elapsed().as_micros();
            match qt {
                QType::Lookup => q_lookup.push(us),
                QType::Scan => q_scan.push(us),
                QType::Join => q_join.push(us),
            }
        }
    }
    let wall = run0.elapsed();

    let updates = update_us.len();
    let queries = q_lookup.len() + q_scan.len() + q_join.len();
    let ops = updates + queries;
    let final_v = handle.snapshot().await;

    println!("\n---- throughput ({:.1}s wall) ----", wall.as_secs_f64());
    println!("  updates={updates}  queries={queries}  total ops={ops}");
    println!(
        "  OPS/SEC = {:.0}   (queries/hr = {:.0})",
        ops as f64 / wall.as_secs_f64(),
        queries as f64 / wall.as_secs_f64() * 3600.0
    );
    println!("\n---- latency ----");
    report("update", update_us);
    report("query:lookup", q_lookup);
    report("query:scan", q_scan);
    report("query:join", q_join);
    println!("\n---- indexer / novelty ----");
    println!(
        "  backpressure waits: {backpressure}   final t={} index_t={} (lag {})",
        final_v.t,
        final_v.index_t(),
        final_v.t - final_v.index_t()
    );
    println!("  est max novelty during run: {max_novelty_mib:.2} MiB");
    println!("\n======================================================================");

    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(run());
}
