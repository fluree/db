//! Integration tests for TypedJson expansion formatting and normalize_arrays.
//!
//! Verifies that `to_typed_json_async()` and `FormatterConfig::typed_json()` produce
//! explicit `@type` annotations in expansion results, and that `normalize_arrays`
//! forces array wrapping for single-valued properties.

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::{json, Value as JsonValue};
use support::{
    genesis_ledger, query_jsonld_format, query_jsonld_formatted, MemoryFluree, MemoryLedger,
};

fn ctx() -> JsonValue {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Seed a small graph with various datatypes for testing typed output.
async fn seed_typed_graph() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/typed-json:test");

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "schema:Person",
                "schema:name": "Alice",
                "schema:age": {"@value": 30, "@type": "xsd:long"},
                "schema:knows": {"@id": "ex:bob"},
                "ex:tags": ["rust", "wasm"]
            },
            {
                "@id": "ex:bob",
                "@type": "schema:Person",
                "schema:name": "Bob",
                "schema:age": {"@value": 25, "@type": "xsd:long"},
                "ex:single_tag": "only-one"
            },
            {
                "@id": "ex:config",
                "ex:data": {"@value": {"key": "val", "n": 42}, "@type": "@json"}
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("insert typed graph");
    (fluree, committed.ledger)
}

// ============================================================================
// TypedJson expansion
// ============================================================================

#[tokio::test]
async fn typed_json_expansion_includes_type_annotations() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*"]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json expansion");

    let arr = result.as_array().expect("result is array");
    assert_eq!(arr.len(), 1, "single root entity");
    let alice = &arr[0];

    // @id should still be present
    assert!(alice.get("@id").is_some(), "has @id");

    // schema:name should have explicit @type
    let name = alice.get("schema:name").expect("has schema:name");
    assert!(name.get("@value").is_some(), "name has @value: {name}");
    assert!(name.get("@type").is_some(), "name has @type: {name}");
    assert_eq!(
        name["@value"].as_str(),
        Some("Alice"),
        "name value is Alice"
    );

    // schema:age should have explicit @type
    let age = alice.get("schema:age").expect("has schema:age");
    assert!(age.get("@value").is_some(), "age has @value: {age}");
    assert!(age.get("@type").is_some(), "age has @type: {age}");

    // schema:knows should be a reference with @id
    let knows = alice.get("schema:knows").expect("has schema:knows");
    assert!(knows.get("@id").is_some(), "knows has @id");
}

#[tokio::test]
async fn typed_json_expansion_json_datatype_preserved() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json expansion with @json");

    let arr = result.as_array().expect("result is array");
    let config_obj = &arr[0];

    let data = config_obj.get("ex:data").expect("has ex:data");
    assert_eq!(
        data.get("@type").and_then(|t| t.as_str()),
        Some("@json"),
        "@json datatype preserved: {data}"
    );
    assert!(data.get("@value").is_some(), "@json value wrapped: {data}");
    // The @value should be parsed JSON, not a string
    let inner = &data["@value"];
    assert!(inner.is_object(), "@json inner is object: {inner}");
    assert_eq!(inner["key"], "val");
    assert_eq!(inner["n"], 42);
}

#[cfg(feature = "native")]
#[tokio::test]
async fn typed_json_expansion_novelty_json_value_decodes_via_binary_range_provider() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::range::{RangeMatch, RangeTest};
    use fluree_db_core::value::FlakeValue;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/typed-json-novelty:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Seed base data and build an index (persisted forward packs exist).
    let base_tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:config", "ex:data": {"@value": {"seed": true}, "@type": "@json"} }
        ]
    });
    let mut ledger = fluree
        .insert(ledger0, &base_tx)
        .await
        .expect("insert base")
        .ledger;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex to create persisted index");

    // Add novelty JSON value with a new string ID (above watermark) and DO NOT index it.
    let novelty_tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:config",
                "ex:data": {"@value": {"novel": true, "id": "novel-json-1"}, "@type": "@json"}
            }
        ]
    });
    ledger = fluree
        .insert_with_opts(
            ledger,
            &novelty_tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                // Avoid triggering background indexing; we want novelty-only values.
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert novelty (no index)")
        .ledger;
    let _novelty_commit_t = ledger.t();

    // Load a view so the snapshot uses the binary index + overlay (BinaryRangeProvider path).
    let db = fluree.db(ledger_id).await.expect("load db");

    // 1) Direct range path: decode novelty @json via BinaryRangeProvider.
    let config_sid = db
        .snapshot
        .encode_iri("http://example.org/ns/config")
        .expect("encode ex:config");
    let data_pid = db
        .snapshot
        .encode_iri("http://example.org/ns/data")
        .expect("encode ex:data");

    let dbref = db.as_graph_db_ref();
    let flakes = dbref
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(config_sid.clone(), data_pid.clone()),
        )
        .await
        .expect("range query should decode novelty @json");

    assert!(
        flakes
            .iter()
            .any(|f| matches!(&f.o, FlakeValue::Json(s) if s.contains("novel-json-1"))),
        "expected novelty @json value decoded via range provider, got: {flakes:?}"
    );

    // 2) Typed-json expansion: should format without failing to resolve novelty string IDs.
    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]},
        "from": ledger_id
    });

    let config = FormatterConfig::typed_json();
    let result = fluree
        .query(&db, &query)
        .await
        .expect("query")
        .format_async(dbref, &config)
        .await
        .expect("typed json expansion should succeed");

    let arr = result.as_array().expect("result array");
    let cfg = &arr[0];
    let data = cfg.get("ex:data").expect("has ex:data");

    // ex:data may be single value or array (if both seed+novel exist); normalize to array.
    let values: Vec<&JsonValue> = if let Some(a) = data.as_array() {
        a.iter().collect()
    } else {
        vec![data]
    };

    let is_json_dt = |dt: &str| dt == "@json" || dt.contains("JSON") || dt.contains("json");
    let inner = values
        .into_iter()
        .find_map(|v| {
            let dt = v.get("@type")?.as_str()?;
            if !is_json_dt(dt) {
                return None;
            }
            let inner = v.get("@value")?;
            (inner.get("id")?.as_str()? == "novel-json-1").then_some(inner)
        })
        .unwrap_or_else(|| panic!("find novelty @json value in: {data}"));

    assert!(inner.is_object(), "@json inner should be object: {inner}");
    assert_eq!(inner["id"], "novel-json-1");
    assert_eq!(inner["novel"], true);
}

#[tokio::test]
async fn typed_json_expansion_nested_entities_are_typed() {
    let (fluree, ledger) = seed_typed_graph().await;

    // Graph crawl with nested expansion
    let query = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*", {"schema:knows": ["*"]}]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json nested expansion");

    let alice = &result.as_array().unwrap()[0];
    let knows = alice.get("schema:knows").expect("has schema:knows");

    // The nested entity (bob) should also have typed literals
    let bob_name = knows
        .get("schema:name")
        .expect("nested bob has schema:name");
    assert!(
        bob_name.get("@value").is_some(),
        "nested name has @value: {bob_name}"
    );
    assert!(
        bob_name.get("@type").is_some(),
        "nested name has @type: {bob_name}"
    );
}

// ============================================================================
// JSON-LD expansion (default) does NOT include types for inferable datatypes
// ============================================================================

#[tokio::test]
async fn jsonld_expansion_omits_inferable_types() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*"]},
        "from": "it/typed-json:test"
    });

    let result = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("jsonld expansion");

    let alice = &result.as_array().unwrap()[0];
    let name = alice.get("schema:name").expect("has schema:name");
    // Default JSON-LD: inferable string type → bare string, no @value/@type
    assert!(
        name.is_string(),
        "default jsonld returns bare string for name: {name}"
    );
}

// ============================================================================
// normalize_arrays
// ============================================================================

#[tokio::test]
async fn normalize_arrays_forces_array_for_single_values() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:bob": ["*"]},
        "from": "it/typed-json:test"
    });

    // Without normalize_arrays: single-valued property is a scalar
    let default_result = query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("default expansion");
    let bob_default = &default_result.as_array().unwrap()[0];
    let single_tag_default = bob_default.get("ex:single_tag").expect("has ex:single_tag");
    assert!(
        !single_tag_default.is_array(),
        "default: single value is scalar: {single_tag_default}"
    );

    // With normalize_arrays: even single-valued property is an array
    let config = FormatterConfig::jsonld().with_normalize_arrays();
    let norm_result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("normalized expansion");
    let bob_norm = &norm_result.as_array().unwrap()[0];
    let single_tag_norm = bob_norm.get("ex:single_tag").expect("has ex:single_tag");
    assert!(
        single_tag_norm.is_array(),
        "normalized: single value is array: {single_tag_norm}"
    );
    assert_eq!(single_tag_norm.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn normalize_arrays_combined_with_typed_json() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:bob": ["*"]},
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json().with_normalize_arrays();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed + normalized expansion");
    let bob = &result.as_array().unwrap()[0];

    // Single-valued property is an array
    let single_tag = bob.get("ex:single_tag").expect("has ex:single_tag");
    assert!(
        single_tag.is_array(),
        "typed+normalized: single value is array"
    );

    // Each element in the array has @value/@type
    let first = &single_tag.as_array().unwrap()[0];
    assert!(first.get("@value").is_some(), "element has @value: {first}");
    assert!(first.get("@type").is_some(), "element has @type: {first}");
}

// ============================================================================
// Tabular (non-crawl) typed json still works
// ============================================================================

#[tokio::test]
async fn typed_json_tabular_query_works() {
    let (fluree, ledger) = seed_typed_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": ["?name", "?age"],
        "where": {
            "@id": "ex:alice",
            "schema:name": "?name",
            "schema:age": "?age"
        },
        "from": "it/typed-json:test"
    });

    let config = FormatterConfig::typed_json();
    let result = query_jsonld_format(&fluree, &ledger, &query, &config)
        .await
        .expect("typed json tabular");

    let row = &result.as_array().unwrap()[0];
    let name = row.get("?name").expect("has ?name");
    assert!(name.get("@value").is_some(), "tabular name has @value");
    assert!(name.get("@type").is_some(), "tabular name has @type");
}

// ============================================================================
// Regression: novelty-only @json value equality match
// ============================================================================

/// Regression: querying with a novelty-only @json object as an equality filter
/// must return matches. Before the fix, `FlakeValue::Json` fell through to the
/// catch-all in `binary_range_eq_v3` which failed to set any filter, potentially
/// returning wrong results or missing novelty data entirely.
#[tokio::test]
async fn typed_json_novelty_only_json_equality_match() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::range::{RangeMatch, RangeTest};
    use fluree_db_core::value::FlakeValue;
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/typed-json-eq:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Phase 1: Seed base @json data and build an index.
    let base_tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:cfg1",
                "ex:settings": {"@value": {"mode": "production"}, "@type": "@json"}
            }
        ]
    });
    let mut ledger = fluree
        .insert(ledger0, &base_tx)
        .await
        .expect("insert base")
        .ledger;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // Phase 2: Insert novelty @json data (not indexed).
    let novelty_tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:cfg2",
                "ex:settings": {"@value": {"mode": "staging", "debug": true}, "@type": "@json"}
            }
        ]
    });
    ledger = fluree
        .insert_with_opts(
            ledger,
            &novelty_tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert novelty")
        .ledger;
    let _t = ledger.t();

    // Phase 3: Load a view using the binary index + overlay.
    let db = fluree.db(ledger_id).await.expect("load db");

    // Query the novelty-only subject to verify its @json data is accessible.
    let cfg2_sid = db
        .snapshot
        .encode_iri("http://example.org/ns/cfg2")
        .expect("encode ex:cfg2");
    let settings_pid = db
        .snapshot
        .encode_iri("http://example.org/ns/settings")
        .expect("encode ex:settings");

    let dbref = db.as_graph_db_ref();
    let flakes = dbref
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(cfg2_sid, settings_pid),
        )
        .await
        .expect("range query for novelty @json subject");

    assert!(
        flakes
            .iter()
            .any(|f| matches!(&f.o, FlakeValue::Json(s) if s.contains("staging"))),
        "novelty-only @json value should be returned via range query; got: {flakes:?}"
    );

    // Also verify the indexed @json subject still works.
    let cfg1_sid = db
        .snapshot
        .encode_iri("http://example.org/ns/cfg1")
        .expect("encode ex:cfg1");
    let settings_pid2 = db
        .snapshot
        .encode_iri("http://example.org/ns/settings")
        .expect("encode ex:settings");

    let flakes = dbref
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(cfg1_sid, settings_pid2),
        )
        .await
        .expect("range query for indexed @json subject");

    assert!(
        flakes
            .iter()
            .any(|f| matches!(&f.o, FlakeValue::Json(s) if s.contains("production"))),
        "indexed @json value should still be returned; got: {flakes:?}"
    );
}
