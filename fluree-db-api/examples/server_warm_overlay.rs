//! Warm overlay benchmark — the REALISTIC read path (stable segments, warm cache).
//!
//! Every other harness here either reloaded `fluree.ledger()` per op (rebuilding
//! novelty with fresh `seg_id`s every time) or measured warm reps over a *static*
//! overlay. Neither models a real server, and neither can see the segment-aware
//! overlay-translation cache (Tier 2): its win is that a query after a commit
//! re-translates only the **newly-appended segment** and reuses cached
//! translations of the older immutable segments.
//!
//! This models it faithfully: an indexed base, then a long-lived state that we
//! **thread through `insert`** (so each commit appends ONE `Arc<Segment>` with a
//! stable `seg_id` — exactly what a cached server's handle does in place) and we
//! **query the threaded state directly** (no reload). The process-global
//! translation cache is keyed by `(store_id, seg_id)`, so threading exercises the
//! same warm-cache behavior as a cached server.
//!
//! We commit + query in a loop so the overlay GROWS, and print scan/join/point
//! latency vs overlay size. WITHOUT Tier 2 a post-commit query re-translates the
//! whole overlay (epoch bumps each commit) → latency grows O(overlay). WITH Tier
//! 2 it translates only the delta → latency stays ~flat. Run on both branches and
//! compare the curve.
//!
//! ## Config (env)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `SW_PRODUCTS` | `20000` | indexed base size |
//! | `SW_STEPS` | `1200` | commit+query steps (overlay grows over the run) |
//! | `SW_PRODUCTS_PER_STEP` | `4` | products per commit (overlay growth rate) |
//! | `SW_SAMPLE_EVERY` | `100` | print a latency sample every N steps |
//! | `SW_DB_DIR` | `/dev/shm/sw` | storage dir |
//!
//! ## Run
//! ```bash
//! SW_DB_DIR=/tmp/sw cargo run --release \
//!   --example server_warm_overlay -p fluree-db-api --features native
//! ```

use std::time::Instant;

use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, LedgerState, TxnOpts,
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

fn product_node(pid: usize, vendor_id: usize, person_id: usize) -> Vec<JsonValue> {
    vec![
        json!({
            "@id": format!("ex:product-{pid:06}"),
            "@type": "bsbm:Product",
            "bsbm:label": format!("Product {pid:06}"),
            "bsbm:productType": "Electronics",
            "bsbm:vendor": { "@id": format!("ex:vendor-{vendor_id:06}") },
            "bsbm:price": 1000 + ((pid * 37) % 50000) as i64
        }),
        json!({
            "@id": format!("ex:review-x{pid:06}"),
            "@type": "bsbm:Review",
            "bsbm:reviewFor": { "@id": format!("ex:product-{pid:06}") },
            "bsbm:reviewer": { "@id": format!("ex:person-{person_id:06}") },
            "bsbm:rating": (1 + (pid % 5)) as i64,
            "bsbm:text": "fresh review"
        }),
    ]
}

fn scan_query() -> JsonValue {
    json!({
        "@context": ctx(),
        "select": ["?p"],
        "where": { "id": "?p", "bsbm:productType": "Electronics" },
        "limit": 50
    })
}
fn join_query() -> JsonValue {
    json!({
        "@context": ctx(),
        "select": ["?p", "?rating"],
        "where": [
            { "id": "?p", "bsbm:productType": "Electronics" },
            { "id": "?r", "bsbm:reviewFor": "?p", "bsbm:rating": "?rating" }
        ],
        "limit": 50
    })
}
fn point_query(pid: usize) -> JsonValue {
    json!({ "@context": ctx(), "select": { format!("ex:product-{pid:06}"): ["*"] } })
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
    let n_products = env_usize("SW_PRODUCTS", 20_000);
    let steps = env_usize("SW_STEPS", 1200);
    let pps = env_usize("SW_PRODUCTS_PER_STEP", 4);
    let sample_every = env_usize("SW_SAMPLE_EVERY", 100);
    let dir = env_str("SW_DB_DIR", "/dev/shm/sw");
    let ledger_id = "srv/warm:main";

    let _ = std::fs::remove_dir_all(&dir);
    let fluree = FlureeBuilder::file(dir.clone())
        .without_indexing()
        .build()
        .expect("build");

    println!("======================================================================");
    println!("  WARM OVERLAY (stable segments, warm translation cache)");
    println!("======================================================================");
    println!("  base: {n_products} products (indexed) | {steps} steps x {pps} products/commit\n");

    // ---- Base: insert + explicit index (drain to the binary index) ----
    let t0 = Instant::now();
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    let turtle = bsbm_data_to_turtle(&generate_dataset(n_products));
    let idx_cfg = IndexConfig {
        reindex_min_bytes: usize::MAX,
        reindex_max_bytes: usize::MAX,
    };
    fluree
        .insert_turtle_with_opts(
            ledger0,
            &turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &idx_cfg,
            None,
        )
        .await
        .expect("base insert");
    rebuild_and_publish(&fluree, ledger_id).await;

    // Reload the indexed base: novelty empty, base behind the binary index.
    let mut state: LedgerState = fluree.ledger(ledger_id).await.expect("reload indexed base");
    println!(
        "  base loaded + indexed in {:.1}s (novelty={} flakes)\n",
        t0.elapsed().as_secs_f64(),
        state.novelty().len()
    );

    // Sanity + warm.
    {
        let db = GraphDb::from_ledger_state(&state);
        for q in [scan_query(), join_query(), point_query(n_products / 2)] {
            fluree.query(&db, &q).await.expect("sanity query");
        }
    }

    println!(
        "  {:>6}{:>7}{:>8}{:>9}{:>12}{:>12}{:>11}{:>10}",
        "step", "t", "segs", "nov_flk", "scan_cold", "scan_warm", "join_us", "point_us"
    );
    let lookup_pid = n_products / 2;
    let n_vendors = (n_products / 50).max(1);
    let n_persons = (n_products / 10).max(1);
    let mut next_pid = n_products;

    let scan_q = scan_query();
    let join_q = join_query();
    let point_q = point_query(lookup_pid);

    for step in 0..steps {
        // Commit: append `pps` products as ONE segment to the threaded state's
        // novelty (stable seg_id; the old segments keep theirs).
        let mut graph = Vec::with_capacity(pps * 2);
        for _ in 0..pps {
            graph.extend(product_node(
                next_pid,
                next_pid % n_vendors,
                next_pid % n_persons,
            ));
            next_pid += 1;
        }
        let txn = json!({ "@context": ctx(), "@graph": graph });
        state = fluree.insert(state, &txn).await.expect("insert").ledger;

        // Query the threaded state directly (warm: the translation cache holds
        // the older segments; only the new segment is cold).
        let t_now = state.t();
        let segs = state.novelty().max_segment_count();
        let nov = state.novelty().len();
        let db = GraphDb::from_ledger_state(&state);

        // scan_cold = first query in this epoch (pays the per-epoch overlay
        // assembly); scan_warm = immediate repeat in the SAME epoch (global
        // translation cache hit → assembly avoided). The gap is the assembly
        // cost a real server amortizes over many queries/commit; scan_warm is
        // the per-query residual the LIMIT pushdown would target.
        let t = Instant::now();
        fluree.query(&db, &scan_q).await.expect("scan cold");
        let scan_cold = t.elapsed().as_micros();
        let t = Instant::now();
        fluree.query(&db, &scan_q).await.expect("scan warm");
        let scan_warm = t.elapsed().as_micros();

        let t = Instant::now();
        fluree.query(&db, &join_q).await.expect("join");
        let join_us = t.elapsed().as_micros();

        let t = Instant::now();
        fluree.query(&db, &point_q).await.expect("point");
        let point_us = t.elapsed().as_micros();

        if step % sample_every == 0 || step == steps - 1 {
            println!(
                "  {step:>6}{t_now:>7}{segs:>8}{nov:>9}{scan_cold:>12}{scan_warm:>12}{join_us:>11}{point_us:>10}"
            );
        }
    }

    println!("\n======================================================================");
    println!("  scan/join latency vs overlay size IS the signal: flat => the overlay");
    println!("  translation cache (Tier 2) re-translates only the delta; growing =>");
    println!("  whole-overlay re-translation per post-commit query. point stays flat");
    println!("  (zone-pruned). Compare this curve on LSM vs the optimization branch.");
    println!("======================================================================");

    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(run());
}
