//! Integration tests for `Fluree::multi_query()` — the in-process
//! multi-query envelope API.
//!
//! These exercise the public builder against a `MemoryFluree` directly,
//! without going through HTTP. They are the canonical "downstream Rust
//! consumer" smoke tests — equivalent to a library user calling:
//!
//! ```ignore
//! let fluree: Arc<Fluree> = Arc::new(FlureeBuilder::memory().build_memory());
//! let response = fluree.multi_query()
//!     .envelope(envelope)
//!     .execute()
//!     .await?;
//! ```
//!
//! Per-piece coverage (envelope shape, validation rules, snapshot
//! resolution, response assembly, span emission) lives in lib unit
//! tests and the server / CLI integration suites. This file's job is
//! to confirm the public API is wired together correctly end to end
//! for non-server consumers.

use std::sync::Arc;

use crate::support::{genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::query::multi::MultiQueryError;
use fluree_db_api::query::multi::{
    MultiQueryBounds, MultiQueryRequest, MultiQueryStatus, MultiQueryValidationError,
};
use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::json;

/// Seed two ledgers with a couple of named entities each so we can run
/// envelopes that hit both.
async fn seed_two_ledgers(fluree: &MemoryFluree) -> (MemoryLedger, MemoryLedger) {
    let users = fluree
        .insert(
            genesis_ledger(fluree, "mq:users"),
            &json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "@graph": [
                    { "@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice" },
                    { "@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian" }
                ]
            }),
        )
        .await
        .expect("seed users")
        .ledger;
    let orders = fluree
        .insert(
            genesis_ledger(fluree, "mq:orders"),
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    { "@id": "ex:o1", "ex:orderId": "ord-1" },
                    { "@id": "ex:o2", "ex:orderId": "ord-2" }
                ]
            }),
        )
        .await
        .expect("seed orders")
        .ledger;
    (users, orders)
}

/// Convert the seeded ledgers into something we can ignore — the
/// envelope's `from` carries the ledger id, so we don't need to thread
/// the LedgerState into the multi-query call. This helper just exists
/// so the seed function's return shape stays informative.
fn ignore_ledgers(_: (MemoryLedger, MemoryLedger)) {}

// =============================================================================
// Public API end-to-end
// =============================================================================

#[tokio::test]
async fn multi_query_builder_runs_envelope_in_process() {
    // The headline test: `Fluree::multi_query()` works for a downstream
    // Rust consumer with no server involvement. Two aliases (one JSON-LD,
    // one SPARQL) against the same ledger; envelope `@context` lifts to
    // both sub-queries.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "queries": {
            "people_jsonld": {
                "language": "jsonld",
                "query": {
                    "from":   "mq:users",
                    "select": ["?name"],
                    "where":  { "@id": "?p", "schema:name": "?name" }
                }
            },
            "people_sparql": {
                "language": "sparql",
                "query": "SELECT ?name FROM <mq:users> WHERE { ?p schema:name ?name }"
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.status, MultiQueryStatus::Ok);
    assert_eq!(response.results.len(), 2);
    assert!(
        response.errors.is_empty(),
        "no per-alias errors expected, got: {:?}",
        response.errors
    );

    // The snapshot map echoes the per-ledger `t` so a downstream caller
    // can replay against the same moment.
    assert!(
        response.snapshot.ledgers.get("mq:users").is_some(),
        "snapshot.ledgers should contain mq:users, got: {:?}",
        response.snapshot.ledgers
    );

    // Both aliases produced results that mention at least one of the
    // seeded names.
    let jsonld_dump = serde_json::to_string(&response.results["people_jsonld"]).unwrap();
    assert!(
        jsonld_dump.contains("Alice") || jsonld_dump.contains("Brian"),
        "JSON-LD alias should return seeded names, got: {jsonld_dump}"
    );
    let sparql_dump = serde_json::to_string(&response.results["people_sparql"]).unwrap();
    assert!(
        sparql_dump.contains("Alice") || sparql_dump.contains("Brian"),
        "SPARQL alias should return seeded names, got: {sparql_dump}"
    );
}

#[tokio::test]
async fn multi_query_two_ledgers_share_one_snapshot() {
    // Two sub-queries hit two different ledgers. The snapshot block
    // should contain both ledgers' resolved `t`, proving the envelope
    // resolved one moment and applied it across.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "queries": {
            "users":  {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "orders": {
                "language": "jsonld",
                "query": { "from": "mq:orders",
                           "select": ["?id"],
                           "where": { "@id": "?o", "ex:orderId": "?id" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.status, MultiQueryStatus::Ok);
    assert!(response.snapshot.ledgers.contains_key("mq:users"));
    assert!(response.snapshot.ledgers.contains_key("mq:orders"));
}

// =============================================================================
// Per-alias tracking surface
// =============================================================================

#[tokio::test]
async fn multi_query_envelope_meta_populates_per_alias_tracking() {
    // Envelope-level opts.meta enables tracking for every sub-query.
    // Response's `tracking` map mirrors `results` with per-alias
    // time / fuel / policy siblings; `meta` carries the envelope-level
    // rollup.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "opts": { "meta": true },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "b": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?u"],
                           "where": { "@id": "?u", "schema:name": "Alice" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    // Both aliases tracked → both appear in the per-alias tracking map.
    assert_eq!(response.tracking.len(), 2);
    assert!(response.tracking.contains_key("a"));
    assert!(response.tracking.contains_key("b"));
    // At least one of time / fuel was recorded for each.
    for (alias, tally) in &response.tracking {
        assert!(
            tally.time.is_some() || tally.fuel.is_some(),
            "tracking[{alias}] should record at least time or fuel, got: {tally:?}"
        );
    }
    // Envelope-level rollup populated when opts.meta is true.
    let meta = response.meta.unwrap();
    assert!(meta.elapsed_ms.is_some());
}

#[tokio::test]
async fn multi_query_per_subquery_opts_meta_only_tracks_that_alias() {
    // Per-sub-query opts.meta on one alias only → the response's
    // tracking map holds only that alias.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "tracked": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } },
                "opts": { "meta": true }
            },
            "untracked": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.tracking.len(), 1);
    assert!(response.tracking.contains_key("tracked"));
    assert!(!response.tracking.contains_key("untracked"));
}

// =============================================================================
// Failure plumbing surfaces through the public error type
// =============================================================================

#[tokio::test]
async fn multi_query_validation_error_surfaces_as_typed_variant() {
    // Envelope with no sub-queries should surface as
    // MultiQueryError::Validation, not panic or lose error context.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "queries": {}
    }))
    .expect("envelope deserialises");

    let err = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect_err("empty envelope should error");

    assert!(
        matches!(err, MultiQueryError::Validation(_)),
        "expected MultiQueryError::Validation, got: {err:?}"
    );
}

#[tokio::test]
async fn multi_query_partial_failure_per_alias_via_in_process_api() {
    // Two aliases — one good JSON-LD, one SPARQL that parses but fails when
    // it runs (its WHERE uses a prefix it never declares). Validation
    // passes; the dispatcher's per-alias error path lands the failing
    // SPARQL in `response.errors` while the good alias produces results.
    // Status is "partial."
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "good": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "bad": {
                "language": "sparql",
                "query": "SELECT ?x FROM <mq:users> WHERE { ?x undeclared:p ?y }"
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes even with one failing sub-query");

    assert_eq!(response.status, MultiQueryStatus::Partial);
    assert!(response.results.contains_key("good"));
    assert!(response.errors.contains_key("bad"));
}

/// SPARQL with a parse error the parser recovers from, naming `mq:users`.
const RECOVERED: &str = "SELECT ?n FROM <mq:users> WHERE { ?u <http://schema.org/name> ?n . ?u }";

#[tokio::test]
async fn multi_query_sub_queries_that_do_not_parse_are_reported_per_alias_via_in_process_api() {
    // A sub-query that does not parse is reported under its alias, in either
    // language, and its siblings still run.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "queries": {
            "good": {
                "language": "jsonld",
                "query": { "@context": { "schema": "http://schema.org/" },
                           "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "bad_sparql": {
                "language": "sparql",
                "query": "SELECT ?x FROM <mq:orders> WHERE { this is not valid SPARQL }"
            },
            "bad_jsonld": {
                "language": "jsonld",
                "query": { "from": "mq:users", "select": 42, "where": { "@id": "?u" } }
            },
            "bad_recovered": {
                "language": "sparql",
                "query": RECOVERED
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("an envelope with sub-queries that do not parse still runs");

    assert_eq!(response.status, MultiQueryStatus::Partial);
    assert!(response.results.contains_key("good"));
    for alias in ["bad_sparql", "bad_jsonld", "bad_recovered"] {
        assert!(
            response.errors.contains_key(alias),
            "{alias}: {:?}",
            response.errors
        );
    }
    // A parse error the parser recovers from is reported for the text as
    // written, though the sibling puts its `FROM` ledger in the snapshot.
    let parsed = fluree_db_sparql::parse_sparql(RECOVERED);
    assert!(
        parsed.has_errors() && parsed.ast.is_some(),
        "the fixture must parse with an error and still yield an AST"
    );
    let first = parsed.errors().next().expect("an error").message.clone();
    let reported = response.errors["bad_recovered"].to_string();
    assert!(reported.contains(&first), "{first:?} not in {reported}");
    assert!(response.snapshot.ledgers.contains_key("mq:users"));
    // The SPARQL sub-query that does not parse reads nothing, so its `FROM`
    // is not in the snapshot.
    assert!(
        !response.snapshot.ledgers.contains_key("mq:orders"),
        "{:?}",
        response.snapshot.ledgers
    );
}

// =============================================================================
// Builder shape (drop-in for `.query()` discipline)
// =============================================================================

#[tokio::test]
async fn multi_query_builder_envelope_required_error_when_unset() {
    // Programmer error: calling .execute() without .envelope() returns
    // a typed error rather than panicking.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    let err = fluree
        .multi_query()
        .execute()
        .await
        .expect_err("envelope required");
    assert!(matches!(err, MultiQueryError::EnvelopeRequired));
}

#[tokio::test]
async fn multi_query_builder_bounds_override_is_threaded_through() {
    // Setting a tiny max_response_size on the builder should reject an
    // envelope whose result would exceed it, surfacing as
    // MultiQueryError::ResponseAssembly. Confirms the .bounds()
    // builder setter actually reaches the assembler.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let tiny = MultiQueryBounds {
        max_response_size_bytes: 16, // smaller than the response overhead
        ..MultiQueryBounds::DEFAULT
    };

    let err = fluree
        .multi_query()
        .envelope(envelope)
        .bounds(tiny)
        .execute()
        .await
        .expect_err("bounds override should make assembly fail");

    assert!(
        matches!(err, MultiQueryError::ResponseAssembly(_)),
        "expected ResponseAssembly error from oversized response, got: {err:?}"
    );
}

// =============================================================================
// Output formatting (envelope-default `.format(...)`)
// =============================================================================

#[tokio::test]
async fn multi_query_builder_typed_json_format_applies_to_jsonld_alias() {
    // `.format(FormatterConfig::typed_json())` on the envelope builder
    // should make every JSON-LD alias's result use the typed-value shape
    // (`@value` / `@type` on literals, `@id` on IRIs). Confirms the
    // envelope-default format actually reaches the JSON-LD sub-query
    // execution path.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "people": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .format(FormatterConfig::typed_json())
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.status, MultiQueryStatus::Ok);
    // TypedJson wraps literals in {"@value": ..., "@type": ...} objects.
    let dump = serde_json::to_string(&response.results["people"]).unwrap();
    assert!(
        dump.contains("@value"),
        "TypedJson alias should emit @value-shaped literals, got: {dump}"
    );
}

#[tokio::test]
async fn multi_query_builder_rejects_non_json_format() {
    // `.format(FormatterConfig::tsv())` produces byte-shaped output that
    // can't be carried inside the JSON envelope. `.execute()` should
    // surface this as `MultiQueryError::UnsupportedFormat` before any
    // sub-query dispatches, so the typo cost stays cheap.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let err = fluree
        .multi_query()
        .envelope(envelope)
        .format(FormatterConfig::tsv())
        .execute()
        .await
        .expect_err("TSV format should be rejected");

    assert!(
        matches!(err, MultiQueryError::UnsupportedFormat { .. }),
        "expected UnsupportedFormat error, got: {err:?}"
    );
}

#[tokio::test]
async fn multi_query_jsonld_normalize_arrays_does_not_change_sparql_alias_shape() {
    // Regression: `.format(FormatterConfig::jsonld().with_normalize_arrays())`
    // on the envelope must apply ONLY to JSON-LD aliases. SPARQL aliases keep
    // their default SPARQL Results JSON shape (`{ head, results: { bindings } }`).
    // The CLI's `--normalize-arrays` flag relies on this — it builds a JsonLd
    // formatter config, and the dispatcher is responsible for routing it
    // per-language.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "jsonld_alias": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "sparql_alias": {
                "language": "sparql",
                "query": "SELECT ?n FROM <mq:users> WHERE { ?u schema:name ?n }"
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .format(FormatterConfig::jsonld().with_normalize_arrays())
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.status, MultiQueryStatus::Ok);

    // SPARQL alias keeps SPARQL Results JSON shape — `head` + `results.bindings`
    // are the canonical SPARQL JSON markers; their absence here would mean
    // the dispatcher coerced the SPARQL result through the JsonLd formatter.
    let sparql_result = &response.results["sparql_alias"];
    assert!(
        sparql_result.get("head").is_some(),
        "SPARQL alias should keep SPARQL Results JSON shape; got: {sparql_result}"
    );
    assert!(
        sparql_result
            .get("results")
            .and_then(|r| r.get("bindings"))
            .is_some(),
        "SPARQL alias should keep SPARQL Results JSON shape; got: {sparql_result}"
    );

    // JSON-LD alias did receive the envelope format — the rows are an
    // array (JSON-LD shape), not a SPARQL Results JSON envelope.
    let jsonld_result = &response.results["jsonld_alias"];
    assert!(
        jsonld_result.is_array(),
        "JSON-LD alias should keep JSON-LD shape; got: {jsonld_result}"
    );
}

#[tokio::test]
async fn multi_query_builder_default_format_unchanged_without_format_call() {
    // No `.format(...)` call → JSON-LD aliases continue to format as
    // JSON-LD (no `@value` wrapping). Regression guard so threading
    // the new builder option through the dispatcher doesn't silently
    // change the per-alias default for existing callers.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "people": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    let dump = serde_json::to_string(&response.results["people"]).unwrap();
    assert!(
        !dump.contains("@value"),
        "default format should NOT wrap literals in @value, got: {dump}"
    );
}

// =============================================================================
// Every member a sub-query reads is read at the snapshot
// =============================================================================

/// `mq:hist` at t=1 holds Alice and Brian; t=2 adds Carol.
async fn seed_two_commits(fluree: &MemoryFluree) {
    let ledger = fluree
        .insert(
            genesis_ledger(fluree, "mq:hist"),
            &json!({
                "@context": { "schema": "http://schema.org/" },
                "@graph": [
                    { "@id": "http://example.org/alice", "schema:name": "Alice" },
                    { "@id": "http://example.org/brian", "schema:name": "Brian" }
                ]
            }),
        )
        .await
        .expect("seed t=1")
        .ledger;
    fluree
        .insert(
            ledger,
            &json!({
                "@context": { "schema": "http://schema.org/" },
                "@id": "http://example.org/carol",
                "schema:name": "Carol"
            }),
        )
        .await
        .expect("seed t=2");
}

/// Each sub-query reads `mq:hist` through another dataset position or
/// spelling; under `asOf: 1` every one reads the ledger at t=1, and the
/// envelope lists the ledger once, by its canonical id.
#[tokio::test]
async fn multi_query_every_member_is_read_at_the_snapshot() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    seed_two_commits(&fluree).await;
    let bs = char::from_u32(0x5c).unwrap();
    let sparql = |text: String| json!({ "language": "sparql", "query": text });
    let names = "?s <http://schema.org/name> ?n";
    let jsonld = |mut query: serde_json::Value| {
        query["select"] = json!(["?n"]);
        json!({ "language": "jsonld", "query": query })
    };
    let where_named = json!([["graph", "h", { "@id": "?s", "http://schema.org/name": "?n" }]]);
    let cases = [
        (
            "sparql urn",
            sparql(format!(
                "SELECT ?n FROM <urn:fluree:mq:hist> WHERE {{ {names} }}"
            )),
        ),
        (
            "sparql escaped",
            sparql(format!(
                "SELECT ?n FROM <mq{bs}u003Ahist> WHERE {{ {names} }}"
            )),
        ),
        (
            "sparql prefixed",
            sparql(format!(
                "PREFIX m: <mq:> SELECT ?n FROM m:hist WHERE {{ {names} }}"
            )),
        ),
        (
            "sparql base",
            sparql(format!(
                "BASE <mq:> SELECT ?n FROM <hist> WHERE {{ {names} }}"
            )),
        ),
        (
            "jsonld fromNamed",
            jsonld(json!({
                "from": "mq:hist",
                "fromNamed": { "h": { "@id": "mq:hist" } },
                "where": where_named
            })),
        ),
        (
            "jsonld from-named",
            jsonld(json!({
                "from": "mq:hist",
                "from-named": [{ "@id": "mq:hist", "alias": "h" }],
                "where": where_named
            })),
        ),
        (
            "jsonld opts.from",
            jsonld(json!({
                "from": "mq:hist",
                "opts": { "from": "urn:fluree:mq:hist" },
                "where": { "@id": "?s", "http://schema.org/name": "?n" }
            })),
        ),
    ];
    let mut failures = Vec::new();
    for (name, sub) in cases {
        let envelope: MultiQueryRequest =
            serde_json::from_value(json!({ "asOf": 1, "queries": { "a": sub } }))
                .expect("envelope deserialises");
        let response = match fluree.multi_query().envelope(envelope).execute().await {
            Ok(response) => response,
            Err(e) => {
                failures.push(format!("{name}: {e}"));
                continue;
            }
        };
        let text = serde_json::to_string(&response.results).unwrap();
        if response.status != MultiQueryStatus::Ok
            || !text.contains("Alice")
            || text.contains("Carol")
            || response.snapshot.ledgers.keys().collect::<Vec<_>>() != ["mq:hist"]
        {
            failures.push(format!(
                "{name}: {:?} {text} {:?} {:?}",
                response.status, response.errors, response.snapshot.ledgers
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// With no `asOf`, a member spelled `urn:fluree:` resolves the snapshot of
/// the ledger it names.
#[tokio::test]
async fn multi_query_a_urn_member_resolves_its_ledgers_snapshot() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    seed_two_commits(&fluree).await;
    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "queries": {
            "a": {
                "language": "sparql",
                "query": "SELECT ?n FROM <urn:fluree:mq:hist> \
                          WHERE { ?s <http://schema.org/name> ?n }"
            }
        }
    }))
    .expect("envelope deserialises");
    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");
    assert_eq!(
        response.status,
        MultiQueryStatus::Ok,
        "{:?}",
        response.errors
    );
    assert_eq!(response.snapshot.ledgers.get("mq:hist"), Some(&json!(2)));
}

/// A ledger a JSON-LD sub-query names only in its named graphs is resolved at
/// envelope entry: listed in the snapshot when it exists, and the envelope's
/// not-found when it does not.
#[tokio::test]
async fn multi_query_named_graph_ledgers_are_resolved_at_entry() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);
    let envelope = |named: &str| -> MultiQueryRequest {
        serde_json::from_value(json!({
            "queries": {
                "a": {
                    "language": "jsonld",
                    "query": {
                        "from": "mq:users",
                        "fromNamed": { "o": { "@id": named } },
                        "select": ["?id"],
                        "where": [["graph", "o", { "@id": "?o", "http://example.org/orderId": "?id" }]]
                    }
                }
            }
        }))
        .expect("envelope deserialises")
    };
    let response = fluree
        .multi_query()
        .envelope(envelope("mq:orders"))
        .execute()
        .await
        .expect("envelope executes");
    assert_eq!(
        response.status,
        MultiQueryStatus::Ok,
        "{:?}",
        response.errors
    );
    let mut listed: Vec<&String> = response.snapshot.ledgers.keys().collect();
    listed.sort();
    assert_eq!(listed, ["mq:orders", "mq:users"], "{listed:?}");

    let err = fluree
        .multi_query()
        .envelope(envelope("mq:absent"))
        .execute()
        .await
        .expect_err("a missing ledger fails the envelope");
    match err {
        MultiQueryError::Snapshot(e) => assert!(e.is_not_found(), "{e}"),
        other => panic!("expected a snapshot not-found, got {other:?}"),
    }
}

/// A named member keeps the name the query wrote it by: under the envelope's
/// snapshot, `GRAPH ?g` over a member written `mq:hist` binds `mq:hist`, on
/// both surfaces, as the same query alone does, and reads it at the snapshot.
#[tokio::test]
async fn multi_query_a_named_member_keeps_its_name_at_the_snapshot() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    seed_two_commits(&fluree).await;
    let sparql = json!({
        "language": "sparql",
        "query": "SELECT ?g ?n FROM NAMED <mq:hist> \
                  WHERE { GRAPH ?g { ?s <http://schema.org/name> ?n } }"
    });
    let jsonld = json!({
        "language": "jsonld",
        "query": {
            "from": "mq:hist",
            "from-named": ["mq:hist"],
            "select": ["?g", "?n"],
            "where": [["graph", "?g", { "@id": "?s", "http://schema.org/name": "?n" }]]
        }
    });
    let mut failures = Vec::new();
    for as_of in [json!(1), serde_json::Value::Null] {
        let mut envelope = json!({ "queries": { "sparql": sparql, "jsonld": jsonld } });
        if !as_of.is_null() {
            envelope["asOf"] = as_of.clone();
        }
        let envelope: MultiQueryRequest =
            serde_json::from_value(envelope).expect("envelope deserialises");
        let response = fluree
            .multi_query()
            .envelope(envelope)
            .execute()
            .await
            .expect("envelope executes");
        let mut sparql_rows: Vec<(String, String)> = response.results["sparql"]["results"]
            ["bindings"]
            .as_array()
            .unwrap_or_else(|| panic!("{:?}", response.errors))
            .iter()
            .map(|b| {
                (
                    b["g"]["value"].as_str().unwrap_or_default().to_string(),
                    b["n"]["value"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        sparql_rows.sort();
        let mut jsonld_rows: Vec<(String, String)> = response.results["jsonld"]
            .as_array()
            .unwrap_or_else(|| panic!("{:?}", response.errors))
            .iter()
            .map(|row| {
                (
                    row[0].as_str().unwrap_or_default().to_string(),
                    row[1].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        jsonld_rows.sort();
        let names: &[&str] = if as_of.is_null() {
            &["Alice", "Brian", "Carol"]
        } else {
            &["Alice", "Brian"]
        };
        let want: Vec<(String, String)> = names
            .iter()
            .map(|n| ("mq:hist".to_string(), (*n).to_string()))
            .collect();
        for (surface, rows) in [("sparql", &sparql_rows), ("jsonld", &jsonld_rows)] {
            if rows != &want {
                failures.push(format!("asOf {as_of}, {surface}: {rows:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// With no envelope `asOf`, a member that names its own time keeps it on
/// both surfaces, while the envelope still resolves the ledger's head.
#[tokio::test]
async fn multi_query_a_member_naming_its_own_time_keeps_it() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    seed_two_commits(&fluree).await;
    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "queries": {
            "sparql": {
                "language": "sparql",
                "query": "SELECT ?n FROM <mq:hist@t:1> WHERE { ?s <http://schema.org/name> ?n }"
            },
            "jsonld": {
                "language": "jsonld",
                "query": {
                    "from": { "@id": "mq:hist", "t": 1 },
                    "select": ["?n"],
                    "where": { "@id": "?s", "http://schema.org/name": "?n" }
                }
            },
            "head": {
                "language": "jsonld",
                "query": {
                    "from": "mq:hist",
                    "select": ["?n"],
                    "where": { "@id": "?s", "http://schema.org/name": "?n" }
                }
            }
        }
    }))
    .expect("envelope deserialises");
    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");
    assert_eq!(
        response.status,
        MultiQueryStatus::Ok,
        "{:?}",
        response.errors
    );
    assert_eq!(response.snapshot.ledgers.get("mq:hist"), Some(&json!(2)));
    let text = |alias: &str| serde_json::to_string(&response.results[alias]).unwrap();
    for alias in ["sparql", "jsonld"] {
        assert!(
            text(alias).contains("Brian") && !text(alias).contains("Carol"),
            "{alias}: {}",
            text(alias)
        );
    }
    assert!(text("head").contains("Carol"), "{}", text("head"));
}

// =============================================================================
// Validation reads the body the dispatcher runs
// =============================================================================

/// `mq:hist` (two commits) and `mq:other` (one).
async fn seed_hist_and_other(fluree: &MemoryFluree) {
    seed_two_commits(fluree).await;
    fluree
        .insert(
            genesis_ledger(fluree, "mq:other"),
            &json!({
                "@context": { "schema": "http://schema.org/" },
                "@id": "http://example.org/olga",
                "schema:name": "Olga"
            }),
        )
        .await
        .expect("seed mq:other");
}

/// One JSON-LD sibling over `mq:other`, the envelope's and the alias's `opts`
/// set as given.
fn layered_envelope(
    envelope_opts: serde_json::Value,
    alias_opts: serde_json::Value,
    body_extra: serde_json::Value,
    as_of: serde_json::Value,
) -> MultiQueryRequest {
    let mut body = json!({
        "from": "mq:other",
        "select": ["?n"],
        "where": [["graph", "h", { "@id": "?s", "http://schema.org/name": "?n" }]]
    });
    for (k, v) in body_extra.as_object().into_iter().flatten() {
        body[k] = v.clone();
    }
    let mut envelope = json!({
        "queries": { "a": { "language": "jsonld", "query": body } }
    });
    if !envelope_opts.is_null() {
        envelope["opts"] = envelope_opts;
    }
    if !alias_opts.is_null() {
        envelope["queries"]["a"]["opts"] = alias_opts;
    }
    if !as_of.is_null() {
        envelope["asOf"] = as_of;
    }
    serde_json::from_value(envelope).expect("envelope deserialises")
}

/// A ledger a sibling's `fromNamed` adds, in its body or in the envelope's or
/// the alias's `opts`, is one the sibling reads: validation counts it against
/// the bound, and the snapshot pins it and lists it, as for its `from`.
#[tokio::test]
async fn multi_query_a_from_named_ledger_is_counted_and_pinned_in_every_layer() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    seed_hist_and_other(&fluree).await;
    let named = json!({ "fromNamed": { "h": { "@id": "mq:hist" } } });
    let none = serde_json::Value::Null;
    let mut failures = Vec::new();
    for (layer, envelope_opts, alias_opts, body_extra) in [
        ("body", none.clone(), none.clone(), named.clone()),
        ("envelope", named.clone(), none.clone(), json!({})),
        ("alias", none.clone(), named.clone(), json!({})),
    ] {
        let envelope = layered_envelope(
            envelope_opts.clone(),
            alias_opts.clone(),
            body_extra.clone(),
            serde_json::Value::Null,
        );
        let ledgers = fluree_db_api::query::multi::validate_envelope_ledgers(
            &envelope,
            &MultiQueryBounds::DEFAULT,
        )
        .expect("validates");
        let distinct: Vec<&String> = ledgers.distinct.iter().collect();
        if distinct != ["mq:hist", "mq:other"] {
            failures.push(format!("{layer}: validation reports {distinct:?}"));
        }
        let one = MultiQueryBounds {
            max_distinct_ledgers: 1,
            ..MultiQueryBounds::DEFAULT
        };
        match fluree
            .multi_query()
            .envelope(envelope)
            .bounds(one)
            .execute()
            .await
        {
            Err(MultiQueryError::Validation(
                MultiQueryValidationError::TooManyDistinctLedgers { actual: 2, .. },
            )) => {}
            other => failures.push(format!("{layer}: bound of 1 gave {other:?}")),
        }
        // An integer `asOf` is for one ledger, and this sibling reads two.
        let envelope = layered_envelope(
            envelope_opts.clone(),
            alias_opts.clone(),
            body_extra.clone(),
            json!(1),
        );
        match fluree.multi_query().envelope(envelope).execute().await {
            Err(MultiQueryError::Validation(
                MultiQueryValidationError::AsOfIntegerMultiLedger { distinct: 2 },
            )) => {}
            other => failures.push(format!("{layer}: integer asOf gave {other:?}")),
        }
        // With no `asOf`, the envelope resolves both ledgers' heads: listed
        // and read.
        let envelope = layered_envelope(
            envelope_opts,
            alias_opts,
            body_extra,
            serde_json::Value::Null,
        );
        match fluree.multi_query().envelope(envelope).execute().await {
            Ok(response) => {
                let mut listed: Vec<&String> = response.snapshot.ledgers.keys().collect();
                listed.sort();
                let text = serde_json::to_string(&response.results).unwrap();
                if listed != ["mq:hist", "mq:other"] || !text.contains("Carol") {
                    failures.push(format!(
                        "{layer}: snapshot {listed:?}, results {text}, errors {:?}",
                        response.errors
                    ));
                }
            }
            Err(e) => failures.push(format!("{layer}: {e}")),
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// How a refused envelope's validation error is recognised.
type RefusedBy = fn(&MultiQueryValidationError) -> bool;

/// The outer `opts` may add to a sibling's dataset but not change a part the
/// body names: a different default graph from the envelope or the alias is a
/// 400, as is a history end, and a pin there collides with an envelope
/// `asOf`. The same default graph, or `opts` that name no dataset, compose.
#[tokio::test]
async fn multi_query_outer_opts_may_not_change_the_dataset_the_body_names() {
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    seed_hist_and_other(&fluree).await;
    let null = serde_json::Value::Null;
    let iso = json!("2099-01-01T00:00:00Z");
    let refused: Vec<(&str, MultiQueryRequest, RefusedBy)> = vec![
        (
            "envelope opts.from",
            layered_envelope(
                json!({ "from": "mq:hist" }),
                null.clone(),
                json!({}),
                json!(1),
            ),
            |e| e.to_string().contains("names its default graphs"),
        ),
        (
            "alias opts.from",
            layered_envelope(
                null.clone(),
                json!({ "from": "mq:hist" }),
                json!({}),
                json!(1),
            ),
            |e| e.to_string().contains("names its default graphs"),
        ),
        (
            "alias opts.ledger",
            layered_envelope(
                null.clone(),
                json!({ "ledger": "mq:hist" }),
                json!({}),
                null.clone(),
            ),
            |e| e.to_string().contains("names its default graphs"),
        ),
        (
            "envelope opts.fromNamed over the body's",
            layered_envelope(
                json!({ "fromNamed": { "h": { "@id": "mq:hist" } } }),
                null.clone(),
                json!({ "fromNamed": { "h": { "@id": "mq:other" } } }),
                null.clone(),
            ),
            |e| e.to_string().contains("names its named graphs"),
        ),
        (
            "envelope opts.to",
            layered_envelope(
                json!({ "to": "mq:other@t:1" }),
                null.clone(),
                json!({}),
                null.clone(),
            ),
            |e| matches!(e, MultiQueryValidationError::HistoryQueryInEnvelope { .. }),
        ),
        (
            "a pin in the envelope's named graphs under asOf",
            layered_envelope(
                json!({ "fromNamed": { "h": { "@id": "mq:hist", "t": 1 } } }),
                null.clone(),
                json!({}),
                iso.clone(),
            ),
            |e| matches!(e, MultiQueryValidationError::AsOfCollision { .. }),
        ),
    ];
    let mut failures = Vec::new();
    for (name, envelope, expected) in refused {
        match fluree.multi_query().envelope(envelope).execute().await {
            Err(MultiQueryError::Validation(e)) if expected(&e) => {}
            other => failures.push(format!("{name}: {other:?}")),
        }
    }
    let composed = [
        (
            "envelope opts.from naming the body's own",
            layered_envelope(
                json!({ "from": "mq:other" }),
                null.clone(),
                json!({}),
                null.clone(),
            ),
        ),
        (
            "the body's own opts.from over the envelope's",
            layered_envelope(
                json!({ "from": "mq:hist" }),
                null.clone(),
                json!({ "opts": { "from": "mq:other" } }),
                null.clone(),
            ),
        ),
        (
            "outer opts naming no dataset",
            layered_envelope(
                json!({ "meta": true }),
                json!({ "max-fuel": 1_000_000 }),
                json!({}),
                null.clone(),
            ),
        ),
    ];
    for (name, envelope) in composed {
        match fluree.multi_query().envelope(envelope).execute().await {
            Ok(response) if response.status == MultiQueryStatus::Ok => {}
            Ok(response) => failures.push(format!("{name}: {:?}", response.errors)),
            Err(e) => failures.push(format!("{name}: {e:?}")),
        }
    }
    // A body that names no `from` of its own is refused as before, whatever
    // the outer `opts` name.
    let mut envelope =
        layered_envelope(json!({ "from": "mq:other" }), null.clone(), json!({}), null);
    envelope.queries["a"]
        .query
        .as_object_mut()
        .unwrap()
        .remove("from");
    match fluree.multi_query().envelope(envelope).execute().await {
        Err(MultiQueryError::Validation(MultiQueryValidationError::MissingFrom { .. })) => {}
        other => failures.push(format!("no body from: {other:?}")),
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
