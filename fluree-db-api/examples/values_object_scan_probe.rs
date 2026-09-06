//! Repro for the "VALUES-bound object var does not narrow the scan" report.
//!
//! Shape (customer): a claim star where the *object* of one predicate is
//! supplied by `VALUES ?sub { <iri> }` and a second predicate carries a
//! constant literal. Inlining the same IRI as a constant object is ~10x
//! faster than the VALUES form, which says the VALUES binding never reaches
//! the scan.
//!
//! ```bash
//! cargo run --release --example values_object_scan_probe -p fluree-db-api
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

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let claims = env_usize("PROBE_CLAIMS", 200_000);
    let subjects = env_usize("PROBE_SUBJECTS", 20_000);
    let iters = env_usize("PROBE_ITERS", 5);
    let batch = 20_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: Fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("fluree");
    let mut ledger = fluree.create_ledger("probe:claims").await.expect("ledger");

    let dims = ["legal-control", "financial", "operational"];

    let t_load = Instant::now();
    let mut start = 0usize;
    while start < claims {
        let end = (start + batch).min(claims);
        let graph: Vec<_> = (start..end)
            .map(|i| {
                json!({
                    "@id": format!("http://example.org/claim{i}"),
                    "@type": "http://example.org/Claim",
                    "http://example.org/relSubject": {
                        "@id": format!("http://example.org/sub{}", i % subjects)
                    },
                    "http://example.org/dimension": dims[i % dims.len()],
                    "http://example.org/status": if i % 7 == 0 { "closed" } else { "open" },
                    "http://example.org/label": format!("claim {i}"),
                    "http://example.org/seq": i as i64,
                })
            })
            .collect();
        ledger = fluree
            .insert(ledger, &json!({"@graph": graph}))
            .await
            .expect("seed")
            .ledger;
        start = end;
    }
    eprintln!(
        "loaded {claims} claims in {:.1}s; reindexing…",
        t_load.elapsed().as_secs_f64()
    );
    fluree
        .reindex("probe:claims", ReindexOptions::default())
        .await
        .expect("reindex");
    let db = fluree.db("probe:claims").await.expect("db");

    // Subject 42 owns claims 42, 42+subjects, … — a handful out of `claims`.
    let target = "http://example.org/sub42";

    let queries: Vec<(&str, String)> = vec![
        (
            "A values_obj + dimension",
            format!(
                "{PREFIX}SELECT ?claim WHERE {{ VALUES ?sub {{ <{target}> }} \
                 ?claim ex:relSubject ?sub . ?claim ex:dimension \"legal-control\" }}"
            ),
        ),
        (
            "B inlined_const + dimension",
            format!(
                "{PREFIX}SELECT ?claim WHERE {{ \
                 ?claim ex:relSubject <{target}> . ?claim ex:dimension \"legal-control\" }}"
            ),
        ),
        (
            "C values_obj alone",
            format!(
                "{PREFIX}SELECT ?claim WHERE {{ VALUES ?sub {{ <{target}> }} \
                 ?claim ex:relSubject ?sub }}"
            ),
        ),
        (
            "D inlined_const alone",
            format!("{PREFIX}SELECT ?claim WHERE {{ ?claim ex:relSubject <{target}> }}"),
        ),
        (
            "E values_obj + 4 more preds",
            format!(
                "{PREFIX}SELECT ?claim ?dim ?st ?lb ?sq WHERE {{ VALUES ?sub {{ <{target}> }} \
                 ?claim ex:relSubject ?sub . ?claim ex:dimension ?dim . \
                 ?claim ex:status ?st . ?claim ex:label ?lb . ?claim ex:seq ?sq }}"
            ),
        ),
        (
            "F inlined + 4 more preds",
            format!(
                "{PREFIX}SELECT ?claim ?dim ?st ?lb ?sq WHERE {{ \
                 ?claim ex:relSubject <{target}> . ?claim ex:dimension ?dim . \
                 ?claim ex:status ?st . ?claim ex:label ?lb . ?claim ex:seq ?sq }}"
            ),
        ),
        (
            "G union_3_inlined",
            format!(
                "{PREFIX}SELECT ?claim WHERE {{ \
                 {{ ?claim ex:relSubject <http://example.org/sub42> }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub43> }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub44> }} }}"
            ),
        ),
        (
            "H values_3_rows + dimension",
            format!(
                "{PREFIX}SELECT ?claim WHERE {{ VALUES ?sub {{ \
                 <http://example.org/sub42> <http://example.org/sub43> \
                 <http://example.org/sub44> }} \
                 ?claim ex:relSubject ?sub . ?claim ex:dimension \"legal-control\" }}"
            ),
        ),
        (
            "I dimension + union_3_inlined",
            format!(
                "{PREFIX}SELECT ?claim WHERE {{ ?claim ex:dimension \"legal-control\" . \
                 {{ ?claim ex:relSubject <http://example.org/sub42> }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub43> }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub44> }} }}"
            ),
        ),
        (
            "J union_3_full_stars",
            format!(
                "{PREFIX}SELECT ?claim ?dim WHERE {{ \
                 {{ ?claim ex:relSubject <http://example.org/sub42> . ?claim ex:dimension ?dim . \
                    ?claim ex:status ?st . ?claim ex:label ?lb . ?claim ex:seq ?sq }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub43> . ?claim ex:dimension ?dim . \
                    ?claim ex:status ?st . ?claim ex:label ?lb . ?claim ex:seq ?sq }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub44> . ?claim ex:dimension ?dim . \
                    ?claim ex:status ?st . ?claim ex:label ?lb . ?claim ex:seq ?sq }} }}"
            ),
        ),
        (
            "K union_3_stars_after_scan",
            format!(
                "{PREFIX}SELECT ?claim ?st WHERE {{ ?claim ex:status ?st . \
                 {{ ?claim ex:relSubject <http://example.org/sub42> }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub43> }} UNION \
                 {{ ?claim ex:relSubject <http://example.org/sub44> }} }}"
            ),
        ),
        (
            "L values_obj + 4 preds + 2 optional",
            format!(
                "{PREFIX}SELECT ?claim ?dim ?st WHERE {{ VALUES ?sub {{ <{target}> }} \
                 ?claim ex:relSubject ?sub . ?claim ex:dimension ?dim . ?claim ex:status ?st . \
                 OPTIONAL {{ ?claim ex:label ?lb }} OPTIONAL {{ ?claim ex:seq ?sq }} }}"
            ),
        ),
    ];

    let only = std::env::var("PROBE_ONLY").ok();

    for (name, text) in &queries {
        if let Some(only) = &only {
            if !name.contains(only.as_str()) {
                continue;
            }
        }
        // Warm the caches once, and record the row count for a sanity check.
        let warm = fluree.query(&db, text.as_str()).await.expect("warmup");
        let rows = warm.row_count();

        let mut times = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t0 = Instant::now();
            let r = fluree.query(&db, text.as_str()).await.expect("query");
            std::hint::black_box(&r);
            times.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eprintln!(
            "{name:<30} rows={rows:<6} median={:.1}ms  min={:.1}ms  max={:.1}ms",
            times[times.len() / 2],
            times[0],
            times[times.len() - 1]
        );
    }

    if std::env::var("PROBE_EXPLAIN").is_ok() {
        for (name, text) in &queries {
            if let Some(only) = &only {
                if !name.contains(only.as_str()) {
                    continue;
                }
            }
            let plan = fluree
                .explain_sparql(&db, text.as_str())
                .await
                .expect("explain");
            eprintln!(
                "\n=== EXPLAIN {name} ===\n{}",
                serde_json::to_string_pretty(&plan).unwrap()
            );
        }
    }
}
