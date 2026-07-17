//! Time pattern-form deletes (`<s> ?p ?o` — delete every triple of one
//! subject) against an indexed base, sequentially. Mirrors the update-mix
//! delete shape: bound subject, variable predicate/object, staged retraction.
//!
//! ```bash
//! # Full deletes (WHERE + staging + commit), per-op stats
//! cargo run --release --example delete_where_probe -p fluree-db-api
//! # Internal-WHERE meter: no-match deletes (nothing stages or commits),
//! # isolating the update pipeline's WHERE execution under live novelty
//! PROBE_NOMATCH=1 cargo run --release --example delete_where_probe -p fluree-db-api
//! ```

use std::time::Instant;

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::json;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let subjects = env_usize("PROBE_SUBJECTS", 4000);
    let deletes = env_usize("PROBE_DELETES", 800);
    println!("subjects={subjects} deletes={deletes}");

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("fluree");
    let mut ledger = fluree.create_ledger("probe:main").await.expect("ledger");

    // Offer-shaped subjects: ~12 triples each (type + literals + refs).
    let chunk = 1000;
    let mut i0 = 0;
    while i0 < subjects {
        let hi = (i0 + chunk).min(subjects);
        let graph: Vec<_> = (i0..hi)
            .map(|i| {
                json!({
                    "@id": format!("offer{i}"),
                    "@type": "Offer",
                    "price": (i % 5000) as f64 + 0.5,
                    "validFrom": format!("2024-01-{:02}", (i % 28) + 1),
                    "validTo": format!("2025-01-{:02}", (i % 28) + 1),
                    "deliveryDays": (i % 14),
                    "offerWebpage": format!("http://example.org/offer{i}"),
                    "vendor": {"@id": format!("vendor{}", i % 100)},
                    "product": {"@id": format!("product{}", i % 1000)},
                    "publisher": {"@id": format!("publisher{}", i % 50)},
                    "number1": (i % 997),
                    "number2": (i % 991)
                })
            })
            .collect();
        ledger = fluree
            .insert(ledger, &json!({"@graph": graph}))
            .await
            .expect("seed")
            .ledger;
        i0 = hi;
    }
    let t = Instant::now();
    fluree
        .reindex("probe:main", ReindexOptions::default())
        .await
        .expect("reindex");
    println!("reindex: {:?}", t.elapsed());

    // No-match mode: modest novelty, then time no-match pattern deletes only
    // (the update pipeline's internal WHERE — no staging, no commit).
    if std::env::var("PROBE_NOMATCH").is_ok() {
        for i in 0..50 {
            let subject = format!("offer{i}");
            let update = json!({
                "where":  {"@id": subject, "?p": "?o"},
                "delete": {"@id": subject, "?p": "?o"}
            });
            ledger = fluree
                .update(ledger, &update)
                .await
                .expect("novelty")
                .ledger;
        }
        let t = Instant::now();
        for i in 0..300 {
            let update = json!({
                "where":  {"@id": format!("nosuch{i}"), "?p": "?o"},
                "delete": {"@id": format!("nosuch{i}"), "?p": "?o"}
            });
            match fluree.update(ledger, &update).await {
                Ok(r) => ledger = r.ledger,
                Err(e) => panic!("nomatch delete errored: {e}"),
            }
        }
        println!("nomatch-only: n=300 total={:?}", t.elapsed());
        return;
    }

    // Sequential pattern-form deletes: one subject per update, variable
    // predicate/object — the staged-retraction shape of the update mix.
    let mut times: Vec<std::time::Duration> = Vec::with_capacity(deletes);
    for i in 0..deletes {
        let subject = format!("offer{i}");
        let update = json!({
            "where":  {"@id": subject, "?p": "?o"},
            "delete": {"@id": subject, "?p": "?o"}
        });
        let t = Instant::now();
        let receipt = fluree.update(ledger, &update).await.expect("delete");
        times.push(t.elapsed());
        ledger = receipt.ledger;
    }

    // Sanity: deleted subjects are gone, undeleted ones remain.
    let view = fluree.db("probe:main").await.expect("view");
    let count = |subject: String| {
        let fluree = &fluree;
        let view = &view;
        async move {
            let q = json!({"where": {"@id": subject, "?p": "?o"}, "select": ["?p", "?o"]});
            let res = fluree
                .query(view, fluree_db_api::QueryInput::JsonLd(&q))
                .await
                .expect("verify query");
            res.to_jsonld(&view.snapshot)
                .expect("jsonld")
                .as_array()
                .map(Vec::len)
                .unwrap_or(0)
        }
    };
    assert_eq!(count("offer0".into()).await, 0, "offer0 must be deleted");
    assert!(
        count(format!("offer{deletes}")).await > 0,
        "undeleted subject must keep its triples"
    );

    times.sort_unstable();
    let sum: std::time::Duration = times.iter().sum();
    let mean = sum / times.len() as u32;
    let p50 = times[times.len() / 2];
    let p95 = times[times.len() * 95 / 100];
    let max = times[times.len() - 1];
    println!(
        "delete-where: n={} mean={mean:?} p50={p50:?} p95={p95:?} max={max:?} total={sum:?}",
        times.len()
    );
}
