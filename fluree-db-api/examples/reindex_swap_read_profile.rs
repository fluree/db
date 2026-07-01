//! Read-after-reindex-swap cold-cost profiler.
//!
//! Isolates the micro-cost that the "warm-on-write" work targets, independent
//! of the macro QMpH sweep. The BSBM `explore+update` sweep shows that a low
//! `reindex_min_bytes` (continuous reindex) collapses product-read latency
//! ~30x; this harness reproduces the *per-swap* mechanism deterministically so
//! any fix is measurable on a single number rather than inferred from QMpH.
//!
//! ## What it does, per burst (one controlled index generation)
//! 1. INSERT a small BSBM-shape burst (new products + their reviews).
//! 2. `trigger_index` — build the incremental index and publish it (waits).
//! 3. Reload the graph snapshot so reads see the new generation (the swap).
//! 4. Point-read a product *written in this burst* (its FLI3 leaf was just
//!    rewritten → new CID → cold decode path) and, as a control, a product
//!    from the base commit (its leaf is usually CID-stable → warm).
//!
//! Background auto-indexing is suppressed during commits (high thresholds) so
//! exactly ONE swap happens per burst and the read that follows is clean.
//!
//! ## Signals (per-burst CSV row + end summary)
//! - `index_ms` — `trigger_index` build+publish latency.
//! - `swap` — 1 if `index_t` advanced (a new generation published).
//! - `read_new_ms` — point-read of a just-written product (cold-hot region).
//! - `read_old_ms` — point-read of a base product (warm control).
//! - `cache_ins_new` — leaflet-cache entries added *during* the new-product
//!   read (a proxy for cold misses; ~0 means fully warm).
//! - `cache_ins_old` — same for the control read.
//! - `cache_entries` / `cache_mb` — cache occupancy after the cycle.
//!
//! A warm-on-write fix should drive `read_new_ms` toward `read_old_ms` and
//! `cache_ins_new` toward ~0. Note: `entry_count()` is moka's
//! eventually-consistent estimate, so `cache_ins_*` is a proxy, not an exact
//! hit/miss count — precise counters can be added to `LeafletCache` later.
//!
//! ## Config (env vars)
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `RSR_TOTAL_PRODUCTS` | `20000` | full dataset product count |
//! | `RSR_BASE_PRODUCTS`  | `5000`  | products in the base indexed commit |
//! | `RSR_BURST_PRODUCTS` | `20`    | new products per burst |
//! | `RSR_BURSTS`         | `50`    | measured burst cycles |
//! | `RSR_WARMUP_BURSTS`  | `3`     | untimed warmup cycles (excluded) |
//! | `RSR_DB_DIR`         | (tempdir) | persistent storage dir |
//! | `RSR_CSV`            | `target/reindex-swap-read.csv` | per-burst CSV |
//!
//! ## Run
//! ```bash
//! cargo run --release --example reindex_swap_read_profile -p fluree-db-api
//! RSR_BURSTS=100 RSR_BURST_PRODUCTS=40 \
//!   cargo run --release --example reindex_swap_read_profile -p fluree-db-api
//! ```

use std::time::Instant;

use fluree_bench_support::gen::bsbm::{
    bsbm_data_to_turtle, generate_dataset, BsbmData, Product, Review,
};
use fluree_db_api::admin::{ReindexOptions, TriggerIndexOptions};
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, LedgerState, TxnOpts};

/// `ex:` prefix expansion used by `bsbm_data_to_turtle`; product ids are
/// `ex:product-NNNNNN`, so the absolute IRI is this + `product-NNNNNN`.
const EX_NS: &str = "http://example.org/ns/";

/// High enough that a foreground commit never triggers background indexing;
/// we drive every build explicitly via `trigger_index` so swaps are 1:1 with
/// bursts.
const SUPPRESS_INDEX_BYTES: usize = 5_000_000_000;

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A burst carries only the new products and their reviews; vendors and
/// persons are committed once in the base and referenced by IRI (RDF needs no
/// prior definition of an object IRI), so a burst stays small and BSBM-shaped.
fn slice_burst(full: &BsbmData, p0: usize, p1: usize) -> BsbmData {
    let products: Vec<Product> = full.products[p0..p1].to_vec();
    // Reviews are laid out 3-per-product contiguously (review i → product i/3).
    let reviews: Vec<Review> = full.reviews[p0 * 3..p1 * 3].to_vec();
    BsbmData {
        vendors: Vec::new(),
        products,
        persons: Vec::new(),
        reviews,
    }
}

async fn commit_turtle(fluree: &Fluree, ledger: LedgerState, turtle: &str) -> (LedgerState, f64) {
    let index_config = IndexConfig {
        reindex_min_bytes: SUPPRESS_INDEX_BYTES,
        reindex_max_bytes: SUPPRESS_INDEX_BYTES,
    };
    let t0 = Instant::now();
    let out = fluree
        .insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("insert burst");
    (out.ledger, t0.elapsed().as_secs_f64() * 1e3)
}

/// Read every triple about one product — a point read that lands in the
/// product's FLI3 leaf(let). Returns (elapsed_ms, cache_entries_inserted).
async fn point_read_product(fluree: &Fluree, alias: &str, product_local_id: &str) -> (f64, i64) {
    let iri = format!("{EX_NS}{product_local_id}");
    let query = format!("SELECT ?p ?o WHERE {{ <{iri}> ?p ?o }}");

    let entries_before = fluree.leaflet_cache().entry_count() as i64;
    let t0 = Instant::now();
    let snapshot = fluree
        .graph(alias)
        .load()
        .await
        .expect("graph load for read");
    let result = snapshot
        .query()
        .sparql(&query)
        .execute()
        .await
        .expect("point read execute");
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1e3;
    std::hint::black_box(result);
    let entries_after = fluree.leaflet_cache().entry_count() as i64;
    (elapsed_ms, entries_after - entries_before)
}

#[derive(Default)]
struct Agg {
    read_new: Vec<f64>,
    read_old: Vec<f64>,
    index_ms: Vec<f64>,
    ins_new: Vec<i64>,
    ins_old: Vec<i64>,
    swaps: usize,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn summarize(label: &str, samples: &mut [f64]) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = samples.iter().sum::<f64>() / samples.len().max(1) as f64;
    eprintln!(
        "  {label:<14} n={:<4} mean={:>8.3}ms  p50={:>8.3}ms  p90={:>8.3}ms  max={:>8.3}ms",
        samples.len(),
        mean,
        pct(samples, 0.50),
        pct(samples, 0.90),
        samples.last().copied().unwrap_or(0.0),
    );
}

fn run() {
    let total_products = env_usize("RSR_TOTAL_PRODUCTS", 20_000);
    let base_products = env_usize("RSR_BASE_PRODUCTS", 5_000);
    let burst_products = env_usize("RSR_BURST_PRODUCTS", 20);
    let bursts = env_usize("RSR_BURSTS", 50);
    let warmup = env_usize("RSR_WARMUP_BURSTS", 3);
    let csv_path = env_str("RSR_CSV", "target/reindex-swap-read.csv");

    let needed = base_products + (warmup + bursts) * burst_products;
    assert!(
        total_products >= needed,
        "RSR_TOTAL_PRODUCTS ({total_products}) < base + (warmup+bursts)*burst ({needed}); \
         raise RSR_TOTAL_PRODUCTS or lower the burst/count knobs"
    );

    eprintln!(
        "[reindex_swap_read] total={total_products} base={base_products} \
         burst={burst_products} bursts={bursts} warmup={warmup}"
    );

    // Build fluree runtime + full dataset once.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async move {
        let full = generate_dataset(total_products);

        // Storage: persistent dir if requested, else a tempdir kept alive for
        // the whole run (`_tempdir` holds it open until the closure ends).
        let mut _tempdir: Option<tempfile::TempDir> = None;
        let storage_path = match std::env::var("RSR_DB_DIR") {
            Ok(dir) => dir,
            Err(_) => {
                let td = tempfile::tempdir().expect("db tmpdir");
                let p = td.path().to_string_lossy().to_string();
                _tempdir = Some(td);
                p
            }
        };
        let fluree = FlureeBuilder::file(storage_path)
            .build()
            .expect("build file-backed Fluree");

        let alias = "reindex-swap-read/bench:main".to_string();
        let mut ledger = fluree.create_ledger(&alias).await.expect("create_ledger");

        // --- Base commit: all vendors + persons + the first `base_products`
        // products and their reviews. One commit keeps the chain shallow. ---
        let base = BsbmData {
            vendors: full.vendors.clone(),
            persons: full.persons.clone(),
            products: full.products[..base_products].to_vec(),
            reviews: full.reviews[..base_products * 3].to_vec(),
        };
        let base_turtle = bsbm_data_to_turtle(&base);
        let (l, base_commit_ms) = commit_turtle(&fluree, ledger, &base_turtle).await;
        drop(l);

        // Baseline index behind the binary columnar store (full reindex).
        let base_reindex = Instant::now();
        let _ = fluree
            .reindex(&alias, ReindexOptions::default())
            .await
            .expect("baseline reindex");
        // Reload so `ledger` tracks the published index head; each burst threads
        // it forward and the next trigger_index runs incrementally.
        ledger = fluree
            .ledger(&alias)
            .await
            .expect("reload after baseline reindex");
        eprintln!(
            "  base: commit={base_commit_ms:.1}ms reindex={:.1}ms products=0..{base_products}",
            base_reindex.elapsed().as_secs_f64() * 1e3,
        );

        // A stable product from the base for the warm control read.
        let old_product_id = full.products[base_products / 2].id.clone();
        let old_local = old_product_id.trim_start_matches("ex:").to_string();

        // CSV header.
        let mut csv = String::new();
        csv.push_str(
            "burst,phase,commit_ms,index_ms,index_t,fuel,swap,read_new_ms,read_old_ms,\
             cache_ins_new,cache_ins_old,cache_entries,cache_mb\n",
        );

        let mut agg = Agg::default();
        let mut prev_index_t: i64 = -1;
        let total_cycles = warmup + bursts;

        for cycle in 0..total_cycles {
            let is_warmup = cycle < warmup;
            let p0 = base_products + cycle * burst_products;
            let p1 = p0 + burst_products;

            // 1. INSERT the burst.
            let burst = slice_burst(&full, p0, p1);
            let turtle = bsbm_data_to_turtle(&burst);
            let (l, commit_ms) = commit_turtle(&fluree, ledger, &turtle).await;
            ledger = l;

            // 2. Build + publish the incremental index (waits for completion).
            let t_idx = Instant::now();
            let res = fluree
                .trigger_index(&alias, TriggerIndexOptions::default())
                .await
                .expect("trigger_index");
            let index_ms = t_idx.elapsed().as_secs_f64() * 1e3;
            let swapped = res.index_t > prev_index_t;
            prev_index_t = res.index_t;

            // 3 + 4. Read a just-written product (cold-hot) and a base product
            // (warm control). Each reload picks up the published generation.
            let new_local = full.products[p1 - 1].id.trim_start_matches("ex:").to_string();
            let (read_new_ms, ins_new) = point_read_product(&fluree, &alias, &new_local).await;
            let (read_old_ms, ins_old) = point_read_product(&fluree, &alias, &old_local).await;

            let cache_entries = fluree.leaflet_cache().entry_count();
            let cache_mb = fluree.leaflet_cache().weighted_size_bytes() as f64 / (1024.0 * 1024.0);

            csv.push_str(&format!(
                "{cycle},{phase},{commit_ms:.3},{index_ms:.3},{index_t},{fuel:.1},{swap},\
                 {read_new_ms:.3},{read_old_ms:.3},{ins_new},{ins_old},{cache_entries},{cache_mb:.2}\n",
                phase = if is_warmup { "warmup" } else { "measure" },
                index_t = res.index_t,
                fuel = res.fuel.unwrap_or(0.0),
                swap = u8::from(swapped),
            ));

            if !is_warmup {
                agg.read_new.push(read_new_ms);
                agg.read_old.push(read_old_ms);
                agg.index_ms.push(index_ms);
                agg.ins_new.push(ins_new);
                agg.ins_old.push(ins_old);
                if swapped {
                    agg.swaps += 1;
                }
            }
        }

        std::fs::write(&csv_path, csv).unwrap_or_else(|e| {
            eprintln!("  (could not write {csv_path}: {e})");
        });

        // --- Summary ---
        eprintln!("\n[reindex_swap_read] summary over {bursts} measured bursts:");
        summarize("read_new", &mut agg.read_new);
        summarize("read_old", &mut agg.read_old);
        summarize("index_ms", &mut agg.index_ms);
        let sum_i64 = |v: &[i64]| v.iter().sum::<i64>();
        let n = agg.ins_new.len().max(1) as i64;
        eprintln!(
            "  swaps={}/{bursts}  cache_ins_new(avg)={}  cache_ins_old(avg)={}  \
             final_cache: {} entries, {:.1} MB",
            agg.swaps,
            sum_i64(&agg.ins_new) / n,
            sum_i64(&agg.ins_old) / n,
            fluree.leaflet_cache().entry_count(),
            fluree.leaflet_cache().weighted_size_bytes() as f64 / (1024.0 * 1024.0),
        );
        eprintln!("  CSV: {csv_path}");
        eprintln!(
            "  READ (cold-hot region) is the warm-on-write target: expect read_new >> read_old \
             and cache_ins_new > 0 today; a fix drives them together toward ~0."
        );
    });
}

fn main() {
    run();
}
