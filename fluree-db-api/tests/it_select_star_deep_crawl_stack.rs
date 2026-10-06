//! Regression guard for fluree/db#1408:
//! `SELECT *` over the type-erased `build_client()` / `query_from()` path must
//! not overflow the default ~2 MB worker-thread stack.
//!
//! ## What overflowed
//!
//! A plain `{"select": {iri: ["*"]}}` connection query (no `depth`, so the
//! hydration crawl does *not* recurse) drove a deep async call chain:
//! `execute_tracked` → `query_connection_*` → view prep → **ledger load +
//! novelty rebuild** → and then query execution + hydration. In debug builds
//! each of those `async fn`s stores its awaited sub-futures *inline* in its own
//! state machine, so the frames are fat; `execute_tracked` alone reserved
//! ~470 KB (it materialized the largest of its ~8 dispatch arms). Stacked up
//! across the chain this exceeded the default ~2 MB stack and SIGABRT'd —
//! breaking solo CI (`cargo test --workspace --all-features`, no
//! `RUST_MIN_STACK`) and risking a SIGABRT on AWS Lambda's ~2 MB worker stacks.
//!
//! The fix boxes the fat futures along that path (`fluree-db-api`
//! `query/builder.rs`, `query/connection.rs`, `view/dataset_builder.rs`,
//! `view/query.rs`) so each frame costs O(1) stack.
//!
//! ## Why a small synthetic ledger reproduces it
//!
//! The peak is the *depth/fatness of the async chain*, which is independent of
//! the data volume (`Novelty::bulk_apply_commits` is a flat loop). All that's
//! required is (a) the type-erased `build_client()` connection, (b) inserted-
//! but-unindexed data so `query_from` rebuilds novelty on load, and (c) a
//! `select *` hydration query. This mirrors solo's
//! `select_star_crawl::file_build_client_select_star_via_query_from`, which is
//! where the regression was first observed.
//!
//! The query runs on a thread with a fixed 2 MB stack so the outcome is
//! independent of the ambient `RUST_MIN_STACK`. Set `CRAWL_STACK_KB` to probe
//! the overflow threshold / headroom.

#![cfg(feature = "native")]

use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::{json, Value};

/// 2 MB — AWS Lambda tokio worker default; the issue's "default ~2MB" stack.
const STACK_2MB: usize = 2 * 1024 * 1024;

/// Stack size for the crawl thread. Defaults to 2 MB (the prod/CI default);
/// override with `CRAWL_STACK_KB` to probe the overflow threshold / margin.
fn crawl_stack_bytes() -> usize {
    std::env::var("CRAWL_STACK_KB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(STACK_2MB)
}

fn context() -> Value {
    json!({
        "ex": "http://example.org/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Number of extra classes padded into the ontology.
///
/// Calibrated so the query holds a large-enough ontology `Value` that, *before*
/// the fix, the deep async chain overflows the 2 MB stack (empirically it starts
/// overflowing around ~200 extra subjects; 500 leaves margin). *After* the fix
/// the same query clears 2 MB with ~600 KB of headroom. This keeps the test a
/// real regression guard (red pre-fix, green post-fix) rather than a query that
/// always fit. Override with `ONTO_N` when probing.
const CALIBRATION_SUBJECTS: usize = 500;

/// A realistic ontology (classes + typed properties), padded to
/// [`CALIBRATION_SUBJECTS`] extra classes — see that constant for why.
fn ontology() -> Value {
    let n: usize = std::env::var("ONTO_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(CALIBRATION_SUBJECTS);
    let mut graph = vec![
        json!({"@id": "ex:Provider", "@type": "rdfs:Class", "rdfs:label": "Provider"}),
        json!({"@id": "ex:User", "@type": "rdfs:Class", "rdfs:label": "User"}),
        json!({"@id": "ex:providerId", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}}),
        json!({"@id": "ex:name", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}}),
        json!({"@id": "ex:kind", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:string"}}),
        json!({"@id": "ex:enabled", "@type": "rdf:Property", "rdfs:range": {"@id": "xsd:boolean"}}),
        json!({"@id": "ex:createdBy", "@type": "rdf:Property", "rdfs:range": {"@id": "ex:User"}}),
        json!({"@id": "ex:config", "@type": "rdf:Property", "rdfs:range": {"@id": "rdf:JSON"}}),
    ];
    for i in 0..n {
        graph.push(json!({"@id": format!("ex:Class{i}"), "@type": "rdfs:Class", "rdfs:label": format!("Class {i}"), "rdfs:comment": "calibration subject"}));
    }
    json!({"@context": context(), "@graph": graph})
}

const IRI: &str = "ex:provider-1";

/// One flat entity: several literals, a `@json` value, and a ref.
fn entity() -> Value {
    json!({
        "@context": context(),
        "@id": IRI,
        "@type": "ex:Provider",
        "ex:providerId": "provider-1",
        "ex:name": "Crawl Test",
        "ex:kind": "saml",
        "ex:enabled": true,
        "ex:config": {"@value": {"email": "email", "groups": "groups"}, "@type": "@json"},
        "ex:createdBy": {"@id": "ex:user-1"}
    })
}

/// The actual crawl, run inside a fixed-stack thread's tokio runtime.
fn run_crawl() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    rt.block_on(async {
        let tmp = tempfile::tempdir().expect("create temp dir");
        // `build_client()` yields the type-erased `FlureeClient`
        // (`AnyStorage` / `AnyNameService`) — the production Lambda connection
        // path, and the one #1408 overflowed on.
        let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
            .build_client()
            .await
            .expect("build_client");

        let ledger0 = fluree.create_ledger("test:main").await.expect("create");
        let receipt = fluree
            .insert(ledger0, &ontology())
            .await
            .expect("bootstrap ontology");
        // Insert-without-reindex: the entity stays in novelty, so `query_from`
        // rebuilds novelty on load (the deep `bulk_apply_commits` path).
        let _receipt = fluree
            .insert(receipt.ledger, &entity())
            .await
            .expect("insert entity");

        let query = json!({"@context": context(), "select": { IRI: ["*"] }, "from": "test:main"});
        let config = FormatterConfig::typed_json().with_normalize_arrays();

        let result = fluree
            .query_from()
            .jsonld(&query)
            .format(config)
            .execute_tracked()
            .await
            .expect("query_from select *");

        let formatted = serde_json::to_value(&result.result).expect("serialize");
        let node = formatted
            .as_array()
            .and_then(|arr| arr.first())
            .expect("should return one result");
        let keys: Vec<&String> = node.as_object().expect("object").keys().collect();
        assert!(
            keys.len() > 1,
            "select * should return properties beyond @id, got only: {keys:?}\nfull: {node}"
        );
    });
}

#[test]
fn file_build_client_select_star_via_query_from_2mb_stack() {
    let child = std::thread::Builder::new()
        .name("crawl-2mb".into())
        .stack_size(crawl_stack_bytes())
        .spawn(run_crawl)
        .expect("spawn crawl thread");

    // Before the fix this thread overflows the 2 MB stack and the process
    // aborts (SIGABRT) — the fluree/db#1408 repro. After the fix the crawl
    // completes and `join()` returns cleanly.
    child
        .join()
        .expect("crawl thread panicked / overflowed its stack");
}
