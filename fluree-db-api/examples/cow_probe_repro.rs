//! Copy-on-write ownership probe for the commit path.
//!
//! Every commit calls `Arc::make_mut` on the ledger's dictionaries. That
//! mutates in place only when the commit uniquely owns them; any other live
//! holder turns it into a deep clone costing O(dictionary entries accumulated
//! since the last index) — quadratic across an unindexed window. The commit
//! path emits its ownership count on the `fluree::cow_probe` target; this
//! harness captures it, and times commits so the copy shows up as latency
//! growth rather than a number nobody can price.
//!
//! Modes (`MODE`):
//! - `cached`     — commit through a cached [`LedgerHandle`] (`stage(&handle)`),
//!   the shape every server transact route uses.
//! - `owned`      — thread a `LedgerState` through `stage_owned`, which never
//!   involves the ledger cache. Control group.
//! - `clonebench` — grow the dictionaries on the owned path and time an
//!   explicit deep clone of exactly what `Arc::make_mut` copies.
//!
//! ```bash
//! MODE=cached INDEXING=off N=1500 NODES=20 \
//!   cargo run --release -p fluree-db-api --example cow_probe_repro
//! ```
//!
//! `FLUREE_STORAGE_FSYNC=off` is worth setting for the timed modes: with
//! fsync on, per-commit disk latency buries the copy.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::json;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

static LAST_DICT: AtomicU64 = AtomicU64::new(0);
static LAST_RSD: AtomicU64 = AtomicU64::new(0);
static PROBE_HITS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct ProbeVisitor {
    dict: Option<u64>,
    rsd: Option<u64>,
}

impl Visit for ProbeVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "dict_novelty_strong" => self.dict = Some(value),
            "runtime_small_dicts_strong" => self.rsd = Some(value),
            _ => {}
        }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_u64(field, value as u64);
    }
    fn record_bool(&mut self, _field: &Field, _value: bool) {}
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

/// Records the most recent `fluree::cow_probe` event so each commit row can
/// report the ownership count the commit path saw.
struct ProbeLayer;

impl<S: tracing::Subscriber> Layer<S> for ProbeLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "fluree::cow_probe" {
            return;
        }
        let mut visitor = ProbeVisitor::default();
        event.record(&mut visitor);
        if let Some(dict) = visitor.dict {
            LAST_DICT.store(dict, Ordering::Relaxed);
        }
        if let Some(rsd) = visitor.rsd {
            LAST_RSD.store(rsd, Ordering::Relaxed);
        }
        PROBE_HITS.fetch_add(1, Ordering::Relaxed);
    }
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn payload(commit: usize, nodes: usize) -> serde_json::Value {
    let graph: Vec<serde_json::Value> = (0..nodes)
        .map(|n| {
            let id = commit * nodes + n;
            json!({
                "@id": format!("ex:person-{id}"),
                "ex:name": format!("Person {id}"),
                "ex:email": format!("p{id}@example.org"),
                "ex:city": format!("City {}", id % 997),
                "ex:age": (id % 90) as i64,
                "ex:note": format!("note-{id}-{}", id % 31),
            })
        })
        .collect();
    json!({"@context": {"ex": "http://example.org/"}, "@graph": graph})
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::registry().with(ProbeLayer).init();

    let mode = env_str("MODE", "cached");
    let n = env_usize("N", 200);
    let nodes = env_usize("NODES", 10);
    let indexing = env_str("INDEXING", "off");

    let tempdir = tempfile::tempdir().expect("tmpdir");
    let mut builder = FlureeBuilder::file(tempdir.path().to_string_lossy().to_string());
    builder = if indexing == "on" {
        builder.with_indexing_thresholds(100, 1 << 30)
    } else {
        builder.without_indexing()
    };
    let fluree: Fluree = builder.build().expect("build");

    let alias = "cowprobe/bench:main";
    let genesis = fluree.create_ledger(alias).await.expect("create_ledger");

    println!("mode={mode} n={n} nodes={nodes} indexing={indexing}");
    println!(
        "i,ms,t,probe_dict,probe_rsd,dict_outside,novelty_outside,snapshot_outside,provider,\
         novelty_bytes"
    );

    match mode.as_str() {
        "cached" => {
            let handle = fluree.ledger_cached(alias).await.expect("ledger_cached");
            drop(genesis);
            for i in 0..n {
                let data = payload(i, nodes);
                let started = Instant::now();
                let res = fluree
                    .stage(&handle)
                    .insert(&data)
                    .execute()
                    .await
                    .expect("execute");
                let ms = started.elapsed().as_secs_f64() * 1e3;
                // Ownership census from outside the commit: this view holds
                // one reference, the cached state another, and a
                // `BinaryRangeProvider` attached to the cached snapshot a
                // third.
                let view = handle.snapshot().await;
                println!(
                    "{i},{ms:.3},{},{},{},{},{},{},{},{}",
                    res.receipt.t,
                    LAST_DICT.load(Ordering::Relaxed),
                    LAST_RSD.load(Ordering::Relaxed),
                    Arc::strong_count(&view.dict_novelty),
                    Arc::strong_count(&view.novelty),
                    Arc::strong_count(&view.snapshot),
                    view.snapshot.range_provider.is_some(),
                    view.novelty.size,
                );
            }
        }
        "owned" => {
            let mut ledger = genesis;
            for i in 0..n {
                let data = payload(i, nodes);
                let started = Instant::now();
                let res = fluree
                    .stage_owned(ledger)
                    .insert(&data)
                    .execute()
                    .await
                    .expect("execute");
                let ms = started.elapsed().as_secs_f64() * 1e3;
                ledger = res.ledger;
                println!(
                    "{i},{ms:.3},{},{},{},{},{},{},{},{}",
                    res.receipt.t,
                    LAST_DICT.load(Ordering::Relaxed),
                    LAST_RSD.load(Ordering::Relaxed),
                    Arc::strong_count(&ledger.dict_novelty),
                    Arc::strong_count(&ledger.novelty),
                    Arc::strong_count(&ledger.snapshot),
                    ledger.snapshot.range_provider.is_some(),
                    ledger.novelty.size,
                );
            }
        }
        "clonebench" => {
            let mut ledger = genesis;
            println!("# commits,subjects,strings,dict_clone_ms,rsd_clone_ms,snapshot_clone_ms");
            for i in 0..n {
                let data = payload(i, nodes);
                let res = fluree
                    .stage_owned(ledger)
                    .insert(&data)
                    .execute()
                    .await
                    .expect("execute");
                ledger = res.ledger;
                if (i + 1) % 100 == 0 || i + 1 == n {
                    let reps = 20;
                    let started = Instant::now();
                    for _ in 0..reps {
                        std::hint::black_box((*ledger.dict_novelty).clone());
                    }
                    let dict_ms = started.elapsed().as_secs_f64() * 1e3 / reps as f64;
                    let started = Instant::now();
                    for _ in 0..reps {
                        std::hint::black_box((*ledger.runtime_small_dicts).clone());
                    }
                    let rsd_ms = started.elapsed().as_secs_f64() * 1e3 / reps as f64;
                    let started = Instant::now();
                    for _ in 0..reps {
                        std::hint::black_box((*ledger.snapshot).clone());
                    }
                    let snapshot_ms = started.elapsed().as_secs_f64() * 1e3 / reps as f64;
                    println!(
                        "{},{},{},{dict_ms:.3},{rsd_ms:.3},{snapshot_ms:.3}",
                        i + 1,
                        ledger.dict_novelty.subjects.len(),
                        ledger.dict_novelty.strings.len(),
                    );
                }
            }
        }
        other => panic!("unknown MODE={other}"),
    }

    eprintln!("probe events: {}", PROBE_HITS.load(Ordering::Relaxed));
}
