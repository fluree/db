//! Regression tests for #1280: the JSON-LD result formatters must not compact
//! `@id` node identifiers against `@vocab`.
//!
//! Per JSON-LD 1.1, `@vocab` governs property names, `@type` values, and
//! `@type: @vocab` term values — it must NOT be used to shorten node
//! identifiers (`@id` / reference values). Only `@base` and explicit
//! term/prefix mappings apply at `@id` positions. Before the fix, a result
//! `@id` whose IRI fell under the query `@vocab` was emitted as a bare term
//! (e.g. `"summer"` for `http://example.org/lists/summer`), which a compliant
//! JSON-LD processor would re-resolve against `@base`, not `@vocab`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, query_jsonld_formatted, MemoryFluree, MemoryLedger};

/// Seed two subjects in distinct namespaces:
/// - `http://example.org/lists/summer` (a List, will sit under the query `@vocab`)
/// - `http://example.org/items/q1`     (an Item, referenced by the list)
async fn seed_lists() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/vocab-id:lists");

    let tx = json!({
        "@context": {
            "lists": "http://example.org/lists/",
            "items": "http://example.org/items/"
        },
        "@graph": [
            {
                "@id": "lists:summer",
                "@type": "lists:List",
                "lists:name": "Summer",
                "lists:contains": {"@id": "items:q1"}
            },
            {
                "@id": "items:q1",
                "@type": "items:Item",
                "items:name": "Item One"
            }
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert lists");
    (fluree, committed.ledger)
}

/// Hydration path: the root `@id` under `@vocab` must stay a full IRI, while
/// predicate keys and `@type` values still compact via `@vocab`, and a nested
/// reference `@id` still compacts via its explicit prefix.
#[tokio::test]
async fn hydration_id_under_vocab_is_not_bare_term() {
    let (fluree, ledger) = seed_lists().await;

    let query = json!({
        "@context": {
            "@vocab": "http://example.org/lists/",
            "items": "http://example.org/items/"
        },
        // Wildcard so the row carries @id, @type, every predicate, and the
        // un-expanded `contains` reference (depth 0 → bare nested @id).
        "select": {"?list": ["*"]},
        "where": {"@id": "?list", "@type": "List"}
    });

    let result = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");

    let list = result
        .as_array()
        .and_then(|a| a.first())
        .expect("one list row");

    // The fix: an @id under @vocab keeps its full IRI, never a bare "summer".
    assert_eq!(
        list.get("@id").and_then(JsonValue::as_str),
        Some("http://example.org/lists/summer"),
        "root @id must not be compacted against @vocab: {result:#}"
    );

    // @vocab still governs predicate keys (so the key is the bare "name").
    assert_eq!(
        list.get("name").and_then(JsonValue::as_str),
        Some("Summer"),
        "predicate keys should still compact via @vocab: {result:#}"
    );

    // @vocab still governs @type values (bare "List").
    assert_eq!(
        list.get("@type").and_then(JsonValue::as_str),
        Some("List"),
        "@type values should still compact via @vocab: {result:#}"
    );

    // A nested reference @id still compacts via its explicit prefix (items:),
    // proving @id compaction uses prefixes — just not @vocab.
    assert_eq!(
        list.get("contains")
            .and_then(|c| c.get("@id"))
            .and_then(JsonValue::as_str),
        Some("items:q1"),
        "nested @id should use the explicit prefix: {result:#}"
    );

    // Belt and suspenders: the bare-term form must not appear anywhere.
    let s = result.to_string();
    assert!(
        !s.contains("\"@id\":\"summer\""),
        "the non-conformant bare @id must not appear: {s}"
    );
}

/// Flat-select path (`Binding::Sid` → bare string): a reference column whose
/// IRI falls under `@vocab` must serialize as the full IRI, not a bare term.
#[tokio::test]
async fn flat_select_ref_under_vocab_is_not_bare_term() {
    let (fluree, ledger) = seed_lists().await;

    let query = json!({
        "@context": { "@vocab": "http://example.org/lists/" },
        "select": ["?list"],
        "where": {"@id": "?list", "@type": "List"}
    });

    let result = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");

    // Array-form select → one row `["<iri>"]`.
    let cell = result
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(JsonValue::as_array)
        .and_then(|cols| cols.first())
        .and_then(JsonValue::as_str);

    assert_eq!(
        cell,
        Some("http://example.org/lists/summer"),
        "flat-select reference under @vocab must be the full IRI: {result:#}"
    );
}

/// `@base` and `@vocab` set together (to *distinct* namespaces): each governs
/// only its own position, across both the insert (expansion) and the query
/// (compaction) directions.
///
/// - `@id`        → governed by `@base` (relative form), never by `@vocab`.
/// - `@type` / predicate → governed by `@vocab` (bare term), never by `@base`.
/// - a reference whose target `@id` falls under the `@vocab` namespace stays a
///   full IRI on output (the #1280 guarantee), even though `@vocab` is active.
#[tokio::test]
async fn base_and_vocab_each_govern_their_position() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/vocab-id:base-vocab");

    // Insert: a relative @id (→ @base), bare @type/predicates (→ @vocab), and a
    // reference whose @id is written under the @vocab namespace (via `v:`).
    let insert = json!({
        "@context": {
            "@base": "https://flur.ee/base/",
            "@vocab": "https://flur.ee/vocab/",
            "v": "https://flur.ee/vocab/"
        },
        "@graph": [
            {
                "@id": "alice",
                "@type": "Person",
                "name": "Alice",
                "knows": {"@id": "v:bob"}
            }
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Query back under the same @base + @vocab.
    let query = json!({
        "@context": {
            "@base": "https://flur.ee/base/",
            "@vocab": "https://flur.ee/vocab/"
        },
        "where": {"@id": "?s", "@type": "Person"},
        "select": {"?s": ["*"]}
    });
    let result = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query");
    let row = result.as_array().and_then(|a| a.first()).expect("one row");

    // @id is governed by @base → relative form (NOT shortened by @vocab).
    assert_eq!(
        row.get("@id").and_then(JsonValue::as_str),
        Some("alice"),
        "@id should compact via @base, not @vocab: {result:#}"
    );
    // @type is governed by @vocab → bare term.
    assert_eq!(
        row.get("@type").and_then(JsonValue::as_str),
        Some("Person"),
        "@type should compact via @vocab: {result:#}"
    );
    // Predicate key is governed by @vocab → bare term.
    assert_eq!(
        row.get("name").and_then(JsonValue::as_str),
        Some("Alice"),
        "predicate key should compact via @vocab: {result:#}"
    );
    // The reference's @id falls under the @vocab namespace, yet must remain a
    // full IRI — @vocab must not shorten a node identifier (#1280).
    assert_eq!(
        row.get("knows")
            .and_then(|k| k.get("@id"))
            .and_then(JsonValue::as_str),
        Some("https://flur.ee/vocab/bob"),
        "an @id under @vocab must not collapse to a bare term: {result:#}"
    );

    // Insert-side check: re-query with explicit prefixes to confirm the stored
    // IRIs expanded correctly — `alice` against @base, `Person` against @vocab.
    let prefixed = json!({
        "@context": {
            "base": "https://flur.ee/base/",
            "v": "https://flur.ee/vocab/"
        },
        "where": {"@id": "?s", "@type": "v:Person"},
        "select": {"?s": ["@id"]}
    });
    let result2 = support::query_jsonld_formatted(&fluree, &ledger, &prefixed)
        .await
        .expect("prefixed query");
    assert_eq!(
        result2
            .as_array()
            .and_then(|a| a.first())
            .and_then(|r| r.get("@id"))
            .and_then(JsonValue::as_str),
        Some("base:alice"),
        "stored @id should be https://flur.ee/base/alice (resolved via @base on insert): {result2:#}"
    );
}

/// JSON-LD `@vocab: ""` sets the vocabulary mapping to `@base`. Bare
/// `@type`/predicates then expand against `@base` on insert and must
/// round-trip back to bare terms on output, while the `@id` stays a (matching)
/// base-relative form.
#[tokio::test]
async fn vocab_empty_string_maps_to_base() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/vocab-id:vocab-empty");

    let insert = json!({
        "@context": {"@base": "https://flur.ee/ledger/", "@vocab": ""},
        "@graph": [
            {"@id": "carol", "@type": "Member", "label": "Carol"}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Round-trip under the same @vocab:"" context.
    let query = json!({
        "@context": {"@base": "https://flur.ee/ledger/", "@vocab": ""},
        "where": {"@id": "?s", "@type": "Member"},
        "select": {"?s": ["*"]}
    });
    let result = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query");
    assert_eq!(
        result,
        json!([{"@id": "carol", "@type": "Member", "label": "Carol"}]),
        "@vocab:\"\" should map to @base so the row round-trips: {result:#}"
    );

    // Confirm the stored IRIs are base-resolved by reading them through an
    // explicit prefix: both `carol` and `Member` live under the ledger base.
    let prefixed = json!({
        "@context": {"led": "https://flur.ee/ledger/"},
        "where": {"@id": "?s", "@type": "led:Member"},
        "select": {"?s": ["@id"]}
    });
    let result2 = support::query_jsonld_formatted(&fluree, &ledger, &prefixed)
        .await
        .expect("prefixed query");
    assert_eq!(
        result2
            .as_array()
            .and_then(|a| a.first())
            .and_then(|r| r.get("@id"))
            .and_then(JsonValue::as_str),
        Some("led:carol"),
        "@id/@type under @vocab:\"\" should resolve against @base: {result2:#}"
    );
}
