//! JSON-LD basic integration tests
//!

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    // Keep this explicit and stable:
    // - no @vocab (so @id compaction stays prefix-based only)
    // - define "id" -> "@id" for faux-compact-iri parity
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/",
        "wiki": "http://www.wikidata.org/entity/",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "id": "@id"
    })
}

async fn seed_movie_graph() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:movie";

    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Minimal “movie -> book -> author” shape to exercise expansion + depth.
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "wiki:Qmovie",
                "@type": "schema:Movie",
                "schema:name": "The Hitchhiker's Guide to the Galaxy",
                "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
                "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                "schema:isBasedOn": {"@id": "wiki:Qbook"}
            },
            {
                "@id": "wiki:Qbook",
                "@type": "schema:Book",
                "schema:name": "The Hitchhiker's Guide to the Galaxy",
                "schema:isbn": "0-330-25864-8",
                "schema:author": {"@id": "wiki:Qauthor"}
            },
            {
                "@id": "wiki:Qauthor",
                "@type": "schema:Person",
                "schema:name": "Douglas Adams"
            }
        ]
    });

    let committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("insert movie graph");
    (fluree, committed.ledger)
}

/// Extract `(@id of column 0, @id of column 1)` from an array-shaped row,
/// for use as a stable sort key when asserting on multi-column hydration
/// outputs.
fn row_id_pair(row: &JsonValue) -> (String, String) {
    let cols = row.as_array();
    let id_at = |idx: usize| -> String {
        cols.and_then(|cs| cs.get(idx))
            .and_then(|c| c.get("@id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    (id_at(0), id_at(1))
}

fn normalize_object_arrays(value: &mut JsonValue) {
    match value {
        JsonValue::Array(arr) => {
            for item in arr.iter_mut() {
                normalize_object_arrays(item);
            }

            if arr.iter().all(serde_json::Value::is_number) {
                arr.sort_by(|a, b| {
                    a.as_f64()
                        .unwrap_or_default()
                        .partial_cmp(&b.as_f64().unwrap_or_default())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else if arr.iter().all(|v| v.as_object().is_some()) {
                arr.sort_by(|a, b| {
                    let a_id = a
                        .as_object()
                        .and_then(|o| o.get("@id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let b_id = b
                        .as_object()
                        .and_then(|o| o.get("@id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    a_id.cmp(b_id)
                });
            }
        }
        JsonValue::Object(map) => {
            for value in map.values_mut() {
                normalize_object_arrays(value);
            }
        }
        _ => {}
    }
}

async fn seed_simple_subject_crawl() -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:ssc";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let tx = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "ex:last": "Smith",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:favColor": "Green",
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "ex:last": "Smith",
                "schema:email": "alice@example.org",
                "ex:favColor": "Green",
                "schema:age": 42,
                "ex:favNums": [42, 76, 9]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "ex:last": "Jones",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:favColor": "Blue",
                "ex:favNums": [5, 10],
                "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]
            },
            {
                "@id": "ex:david",
                "@type": "ex:User",
                "schema:name": "David",
                "ex:last": "Jones",
                "schema:email": "david@example.org",
                "schema:age": 46,
                "ex:favNums": [15, 70],
                "ex:friend": {"@id": "ex:cam"}
            }
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert ssc data");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn jsonld_basic_wildcard_single_subject_query() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": { "wiki:Qmovie": ["*"] }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let arr = json.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().expect("object row");

    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert!(obj.get("schema:name").is_some(), "expected schema:name");
    assert!(
        obj.get("schema:isBasedOn").is_some(),
        "expected schema:isBasedOn"
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_query_explicit_fields() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "select": { "wiki:Qmovie": ["@id", "schema:name"] }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let arr = json.as_array().expect("array result");
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().expect("object row");

    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert_eq!(
        obj.get("schema:name").and_then(|v| v.as_str()),
        Some("The Hitchhiker's Guide to the Galaxy")
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_query_select_one() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["@id", "schema:name"] }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let obj = json.as_object().expect("object result");

    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("wiki:Qmovie"));
    assert_eq!(
        obj.get("schema:name").and_then(|v| v.as_str()),
        Some("The Hitchhiker's Guide to the Galaxy")
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_query_expansion() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["*", {"schema:isBasedOn": ["*"]}] }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:name": "The Hitchhiker's Guide to the Galaxy",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {
            "@id": "wiki:Qbook",
            "@type": "schema:Book",
            "schema:name": "The Hitchhiker's Guide to the Galaxy",
            "schema:isbn": "0-330-25864-8",
            "schema:author": {"@id": "wiki:Qauthor"}
        }
    });

    assert_eq!(json, expected);
}

#[tokio::test]
async fn jsonld_basic_single_subject_expansion_with_depth() {
    // Mirrors the “depth expansion” behavior:
    // with depth=3 and wildcard selection, refs should auto-expand transitively.
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["*"] },
        "depth": 3
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let movie = json.as_object().expect("movie object");
    assert_eq!(
        movie.get("@id").and_then(|v| v.as_str()),
        Some("wiki:Qmovie")
    );

    // isBasedOn should be an expanded object (depth expansion)
    let book = movie
        .get("schema:isBasedOn")
        .and_then(|v| v.as_object())
        .expect("schema:isBasedOn object");
    assert_eq!(book.get("@id").and_then(|v| v.as_str()), Some("wiki:Qbook"));

    // author should be expanded as well
    let author = book
        .get("schema:author")
        .and_then(|v| v.as_object())
        .expect("schema:author object");
    assert_eq!(
        author.get("@id").and_then(|v| v.as_str()),
        Some("wiki:Qauthor")
    );
}

#[tokio::test]
async fn jsonld_basic_single_subject_expansion_with_depth_and_subselection() {
    let (fluree, ledger) = seed_movie_graph().await;

    let query = json!({
        "@context": ctx(),
        "selectOne": { "wiki:Qmovie": ["*", {"schema:isBasedOn": ["*"]}] },
        "depth": 3
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:name": "The Hitchhiker's Guide to the Galaxy",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {
            "@id": "wiki:Qbook",
            "@type": "schema:Book",
            "schema:name": "The Hitchhiker's Guide to the Galaxy",
            "schema:isbn": "0-330-25864-8",
            "schema:author": {
                "@id": "wiki:Qauthor",
                "@type": "schema:Person",
                "schema:name": "Douglas Adams"
            }
        }
    });

    assert_eq!(json, expected);
}

#[tokio::test]
async fn jsonld_query_with_faux_compact_iri_ids() {
    // Strict compact-IRI guard: "foaf:bar" without `foaf` in @context is
    // now rejected at parse time because it looks like a compact IRI with
    // a missing prefix. If the user really wants a literal IRI-like string
    // as an @id, they must define the prefix in @context first.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:faux-compact";

    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // ── Part 1: undefined prefix is rejected ──
    let tx_bad = json!({
        "@context": ctx(),
        "@graph": [
            {"id":"foo","ex:name":"Foo"},
            {"id":"foaf:bar","ex:name":"Bar"}
        ]
    });
    let err = fluree
        .insert(ledger0.clone(), &tx_bad)
        .await
        .expect_err("should reject undefined compact IRI");
    let msg = err.to_string();
    assert!(
        msg.contains("foaf") && msg.contains("not defined"),
        "error should mention the unresolved prefix: {msg}"
    );

    // ── Part 2: with foaf defined in context, it works ──
    let tx_ok = json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.org/ns/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "id": "@id"
        },
        "@graph": [
            {"id":"foo","ex:name":"Foo"},
            {"id":"foaf:bar","ex:name":"Bar"}
        ]
    });
    let _committed = fluree
        .insert(ledger0, &tx_ok)
        .await
        .expect("insert with defined prefix");
    let loaded = fluree.ledger(ledger_id).await.expect("reload ledger");

    // Subject crawl SELECT (bare word "foo" has no colon so is accepted)
    let q2 = json!({
        "@context": ctx(),
        "select": {"foo": ["*"]}
    });
    let r2 = support::query_jsonld(&fluree, &loaded, &q2)
        .await
        .expect("query crawl");
    let json2 = r2
        .to_jsonld_async(loaded.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let arr2 = json2.as_array().expect("array result");
    assert_eq!(arr2.len(), 1);
    let obj = arr2[0].as_object().expect("object");
    assert_eq!(obj.get("@id").and_then(|v| v.as_str()), Some("foo"));
    assert_eq!(obj.get("ex:name").and_then(|v| v.as_str()), Some("Foo"));
}

#[tokio::test]
async fn jsonld_opts_strict_compact_iri_false_allows_undefined_prefix() {
    // `opts.strictCompactIri: false` opts out of the strict compact-IRI guard
    // for both insert and query, allowing "foaf:bar" through as a literal IRI
    // even when `foaf` is not defined in @context.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:strict-opt-out";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Insert with strict guard disabled
    let tx = json!({
        "@context": ctx(),
        "opts": {"strictCompactIri": false},
        "@graph": [
            {"id":"foo","ex:name":"Foo"},
            {"id":"foaf:bar","ex:name":"Bar"}
        ]
    });
    let _committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("insert should succeed with strict guard disabled");
    let loaded = fluree.ledger(ledger_id).await.expect("reload ledger");

    // Query also with strict guard disabled — count both subjects
    let q = json!({
        "@context": ctx(),
        "opts": {"strictCompactIri": false},
        "select": ["?f", "?n"],
        "where": {"id": "?f", "ex:name": "?n"}
    });
    let r = support::query_jsonld(&fluree, &loaded, &q)
        .await
        .expect("query select");
    let mut rows = r.to_jsonld(&loaded.snapshot).expect("to_jsonld");
    let arr = rows.as_array_mut().expect("rows array");
    arr.sort_by_key(std::string::ToString::to_string);
    assert_eq!(rows, json!([["foaf:bar", "Bar"], ["foo", "Foo"]]));
}

#[tokio::test]
async fn jsonld_opts_strict_compact_iri_true_explicit() {
    // Setting `opts.strictCompactIri: true` explicitly matches the default
    // behavior — undefined prefixes are still rejected.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:strict-opt-in";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let tx = json!({
        "@context": ctx(),
        "opts": {"strictCompactIri": true},
        "@graph": [{"id":"foaf:bar","ex:name":"Bar"}]
    });
    let err = fluree
        .insert(ledger0, &tx)
        .await
        .expect_err("strict mode should still reject undefined prefix");
    assert!(err.to_string().contains("foaf"));
}

#[tokio::test]
async fn jsonld_single_object_insert_opts_does_not_leak_as_data() {
    // Regression: single-object form (no @graph) feeds the raw object into
    // the JSON-LD expander. `opts` must be stripped before expansion so it
    // is never stored as data.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:opts-no-leak-single-object";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Single-object insert (no @graph) with top-level opts
    let tx = json!({
        "@context": ctx(),
        "opts": {"strictCompactIri": true},
        "@id": "ex:alice",
        "ex:name": "Alice"
    });
    let _committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("single-object insert with opts should succeed");
    let loaded = fluree.ledger(ledger_id).await.expect("reload ledger");

    // Subject crawl — verify "opts" was NOT stored as a property of ex:alice
    let q = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*"]}
    });
    let r = support::query_jsonld(&fluree, &loaded, &q)
        .await
        .expect("query crawl");
    let json_out = r
        .to_jsonld_async(loaded.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let arr = json_out.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().expect("object");

    // Primary assertion: no "opts" field made it onto the entity
    assert!(
        !obj.keys().any(|k| k == "opts" || k.ends_with(":opts")),
        "'opts' must not be stored as data; got keys: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    // Sanity: the real data is there
    assert_eq!(obj.get("ex:name").and_then(|v| v.as_str()), Some("Alice"));
}

#[tokio::test]
async fn jsonld_path_alias_honors_strict_opt_out() {
    // Regression: @path expressions inside @context are parsed BEFORE the
    // main query body, but they must still honor `opts.strictCompactIri`.
    // A @path referencing an undefined prefix should:
    //   - default (strict=true): REJECT at parse time
    //   - opts.strictCompactIri=false: ACCEPT (pass through)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:path-alias-opt-out";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Seed minimal data
    let seed = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let _ = fluree.insert(ledger0, &seed).await.expect("seed insert");
    let loaded = fluree.ledger(ledger_id).await.expect("reload");

    // Strict (default): @path with undefined `foo:` prefix should be rejected
    let q_strict = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id",
            "chain": {"@path": "foo:step1/foo:step2"}
        },
        "select": ["?s"],
        "where": {"id": "?s", "chain": "?o"}
    });
    let err = support::query_jsonld(&fluree, &loaded, &q_strict)
        .await
        .expect_err("strict mode should reject undefined prefix inside @path");
    let msg = err.to_string();
    assert!(
        msg.contains("foo") && msg.contains("not defined"),
        "error should mention undefined prefix inside @path: {msg}"
    );

    // Opt-out: same @path expression should parse successfully
    let q_relaxed = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id",
            "chain": {"@path": "foo:step1/foo:step2"}
        },
        "opts": {"strictCompactIri": false},
        "select": ["?s"],
        "where": {"id": "?s", "chain": "?o"}
    });
    // Should parse without error. Empty result set is fine — we're verifying
    // the parse phase honors the opt-out, not execution semantics.
    let _result = support::query_jsonld(&fluree, &loaded, &q_relaxed)
        .await
        .expect("opt-out should allow undefined prefix inside @path");
}

#[tokio::test]
async fn jsonld_typed_literal_datatype_honors_strict_opt_out() {
    // Regression: @type on a @value object is a datatype IRI. It must honor
    // opts.strictCompactIri when the prefix isn't in @context.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:typed-literal-opt-out";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Strict (default): undefined datatype prefix rejected
    let tx_strict = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{
            "@id": "ex:alice",
            "ex:age": {"@value": "42", "@type": "mydt:customInt"}
        }]
    });
    let err = fluree
        .insert(ledger0.clone(), &tx_strict)
        .await
        .expect_err("strict mode should reject undefined datatype prefix");
    let msg = err.to_string();
    assert!(
        msg.contains("mydt") && msg.contains("not defined"),
        "error should mention undefined datatype prefix: {msg}"
    );

    // Opt-out: undefined datatype prefix passes through
    let tx_relaxed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "opts": {"strictCompactIri": false},
        "@graph": [{
            "@id": "ex:alice",
            "ex:age": {"@value": "42", "@type": "mydt:customInt"}
        }]
    });
    let _committed = fluree
        .insert(ledger0, &tx_relaxed)
        .await
        .expect("opt-out should allow undefined datatype prefix");
}

#[tokio::test]
async fn jsonld_txn_meta_datatype_honors_strict_opt_out() {
    // Regression: txn-meta top-level predicates can carry @value objects
    // with a @type datatype IRI. That datatype resolution must also honor
    // opts.strictCompactIri.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:txn-meta-datatype-opt-out";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Strict (default): undefined datatype prefix in txn-meta → reject
    let tx_strict = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}],
        "ex:meta": {"@value": "v1", "@type": "mydt:customStr"}
    });
    let err = fluree
        .insert(ledger0.clone(), &tx_strict)
        .await
        .expect_err("strict mode should reject undefined txn-meta datatype prefix");
    let msg = err.to_string();
    assert!(
        msg.contains("mydt") && msg.contains("not defined"),
        "error should mention undefined datatype prefix in txn-meta: {msg}"
    );

    // Opt-out: undefined datatype prefix in txn-meta passes through
    let tx_relaxed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "opts": {"strictCompactIri": false},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}],
        "ex:meta": {"@value": "v1", "@type": "mydt:customStr"}
    });
    let _committed = fluree
        .insert(ledger0, &tx_relaxed)
        .await
        .expect("opt-out should allow undefined datatype prefix in txn-meta");
}

#[tokio::test]
async fn jsonld_expanding_literal_nodes_wildcard() {
    // Mirrors "expanding literal nodes - with wildcard"
    let (fluree, ledger) = seed_movie_graph().await;

    let q = json!({
        "@context": ctx(),
        "selectOne": {
            "wiki:Qmovie": ["*", {"schema:name": ["*"]}]
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("query expanding literal");
    let json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    // Expected result with expanded literal node
    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:name": {
            "@value": "The Hitchhiker's Guide to the Galaxy",
            "@type": "xsd:string"
        },
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {"@id": "wiki:Qbook"}
    });

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_rdf_type_query_analytical() {
    // Mirrors "json-ld rdf type queries - basic analytical RDF type query"
    let (fluree, ledger) = seed_movie_graph().await;

    let q = json!({
        "@context": ctx(),
        "select": {"?s": ["*", {"schema:isBasedOn": ["*"]}]},
        "where": {
            "@id": "?s",
            "@type": "schema:Movie"
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("query rdf type");
    let json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    // Should return the movie with its properties
    let arr = json_result.as_array().expect("array result");
    assert_eq!(arr.len(), 1);

    let movie = &arr[0];
    assert_eq!(
        movie.get("@id").and_then(|v| v.as_str()),
        Some("wiki:Qmovie")
    );
    assert_eq!(
        movie.get("@type").and_then(|v| v.as_str()),
        Some("schema:Movie")
    );
    assert_eq!(
        movie.get("schema:name").and_then(|v| v.as_str()),
        Some("The Hitchhiker's Guide to the Galaxy")
    );
}

#[tokio::test]
async fn jsonld_expansion_nested_subselect_includes_id() {
    let (fluree, ledger) = seed_movie_graph().await;

    // Regression: nested ref sub-selects should always include @id for identity,
    // even when the sub-select requests specific properties (no "*").
    let q = json!({
        "@context": ctx(),
        "select": {"?s": ["*", {"schema:isBasedOn": ["schema:name"]}]},
        "where": {
            "@id": "?s",
            "@type": "schema:Movie"
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("query nested subselect");
    let json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let arr = json_result.as_array().expect("array result");
    assert_eq!(arr.len(), 1);

    let movie = arr[0].as_object().expect("movie object");
    let based_on = movie
        .get("schema:isBasedOn")
        .expect("schema:isBasedOn present")
        .as_object()
        .expect("schema:isBasedOn object");

    assert_eq!(
        based_on.get("@id").and_then(|v| v.as_str()),
        Some("wiki:Qbook"),
        "nested sub-select ref should include @id for identity; got: {based_on:?}"
    );
    assert_eq!(
        based_on.get("schema:name").and_then(|v| v.as_str()),
        Some("The Hitchhiker's Guide to the Galaxy"),
        "nested sub-select should still include requested properties; got: {based_on:?}"
    );
}

#[tokio::test]
async fn jsonld_list_order_preservation_context_container() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:list-container";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let tx = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id",
            "ex:list": {"@container": "@list"}
        },
        "@graph": [
            {"@id": "list-test", "ex:list": [42, 2, 88, 1]}
        ]
    });

    let committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("insert list container");
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id",
            "ex:list": {"@container": "@list"}
        },
        "selectOne": { "list-test": ["*"] }
    });

    let result = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect("query list container");
    let json_result = result
        .to_jsonld_async(committed.ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        json_result,
        json!({"@id": "list-test", "ex:list": [42, 2, 88, 1]})
    );
}

#[tokio::test]
async fn jsonld_list_order_preservation_explicit_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/jsonld-basic:list-explicit";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let tx = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id"
        },
        "@graph": [
            {"@id": "list-test2", "ex:list": {"@list": [42, 2, 88, 1]}}
        ]
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert list");
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "id": "@id"
        },
        "selectOne": { "list-test2": ["*"] }
    });

    let result = support::query_jsonld(&fluree, &committed.ledger, &query)
        .await
        .expect("query list");
    let json_result = result
        .to_jsonld_async(committed.ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        json_result,
        json!({"@id": "list-test2", "ex:list": [42, 2, 88, 1]})
    );
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_direct_id() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "ex:brian": ["*"] }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(
        json_result,
        json!([{
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favColor": "Green",
            "ex:favNums": 7
        }])
    );
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_where_type() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "@type": "ex:User" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:last": "Smith",
            "schema:email": "alice@example.org",
            "schema:age": 42,
            "ex:favNums": [9, 42, 76],
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7,
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "ex:last": "Jones",
            "schema:email": "cam@example.org",
            "schema:age": 34,
            "ex:favNums": [5, 10],
            "ex:friend": [{"@id": "ex:alice"}, {"@id": "ex:brian"}],
            "ex:favColor": "Blue"
        },
        {
            "@id": "ex:david",
            "@type": "ex:User",
            "schema:name": "David",
            "ex:last": "Jones",
            "schema:email": "david@example.org",
            "schema:age": 46,
            "ex:favNums": [15, 70],
            "ex:friend": {"@id": "ex:cam"}
        }
    ]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_two_hydration_columns_var_roots() {
    // Two var-rooted hydration columns in a single select: each row should be
    // a 2-element array of independently expanded subjects.
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": [
            {"?friender": ["@id", "schema:name"]},
            {"?friend":   ["@id", "schema:name"]}
        ],
        "where": {
            "@id": "?friender",
            "ex:friend": "?friend"
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    // Sort top-level rows by (friender @id, friend @id) so the assertion is
    // independent of solution iteration order.
    if let JsonValue::Array(rows) = &mut json_result {
        rows.sort_by_key(|row| row_id_pair(row));
    }

    let expected = json!([
        [
            {"@id": "ex:cam", "schema:name": "Cam"},
            {"@id": "ex:alice", "schema:name": "Alice"}
        ],
        [
            {"@id": "ex:cam", "schema:name": "Cam"},
            {"@id": "ex:brian", "schema:name": "Brian"}
        ],
        [
            {"@id": "ex:david", "schema:name": "David"},
            {"@id": "ex:cam", "schema:name": "Cam"}
        ]
    ]);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_two_hydration_columns_different_subspecs() {
    // Each hydration column may carry its own NestedSelectSpec — different
    // sub-selections should be honored independently per column.
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": [
            {"?friender": ["@id", "schema:name", "schema:age"]},
            {"?friend":   ["@id"]}
        ],
        "where": {
            "@id": "?friender",
            "ex:friend": "?friend",
            "schema:name": "David"
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let expected = json!([[
        {"@id": "ex:david", "schema:name": "David", "schema:age": 46},
        {"@id": "ex:cam"}
    ]]);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_tuple_name() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "schema:name": "Alice" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let mut expected = json!([{
        "@id": "ex:alice",
        "@type": "ex:User",
        "schema:name": "Alice",
        "ex:last": "Smith",
        "schema:email": "alice@example.org",
        "schema:age": 42,
        "ex:favNums": [42, 76, 9],
        "ex:favColor": "Green"
    }]);
    normalize_object_arrays(&mut json_result);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_tuple_fav_color() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "ex:favColor": "?color" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:last": "Smith",
            "schema:email": "alice@example.org",
            "schema:age": 42,
            "ex:favNums": [9, 42, 76],
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7,
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "ex:last": "Jones",
            "schema:email": "cam@example.org",
            "schema:age": 34,
            "ex:favNums": [5, 10],
            "ex:friend": [{"@id": "ex:alice"}, {"@id": "ex:brian"}],
            "ex:favColor": "Blue"
        }
    ]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_limit_two() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "ex:favColor": "?color" },
        "orderBy": ["?s"],
        "limit": 2
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:last": "Smith",
            "schema:email": "alice@example.org",
            "schema:age": 42,
            "ex:favNums": [9, 42, 76],
            "ex:favColor": "Green"
        },
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "ex:last": "Smith",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7,
            "ex:favColor": "Green"
        }
    ]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_age_and_fav_color() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "schema:age": 42, "ex:favColor": "Green" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([{
        "@id": "ex:alice",
        "@type": "ex:User",
        "schema:name": "Alice",
        "ex:last": "Smith",
        "schema:email": "alice@example.org",
        "schema:age": 42,
        "ex:favNums": [9, 42, 76],
        "ex:favColor": "Green"
    }]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_simple_subject_crawl_age_only() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    let query = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "schema:age": 42 }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let mut expected = json!([{
        "@id": "ex:alice",
        "@type": "ex:User",
        "schema:name": "Alice",
        "ex:last": "Smith",
        "schema:email": "alice@example.org",
        "schema:age": 42,
        "ex:favNums": [9, 42, 76],
        "ex:favColor": "Green"
    }]);
    normalize_object_arrays(&mut expected);

    assert_eq!(json_result, expected);
}

#[tokio::test]
async fn jsonld_expanding_literal_nodes_specific_properties() {
    // Mirrors "expanding literal nodes - with specific virtual properties"
    let (fluree, ledger) = seed_movie_graph().await;

    let q = json!({
        "@context": ctx(),
        "selectOne": {
            "wiki:Qmovie": ["*", {"schema:name": ["@type"]}]
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("query expanding literal specific");
    let json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    // Expected result with only @type from the expanded literal
    let expected = json!({
        "@id": "wiki:Qmovie",
        "@type": "schema:Movie",
        "schema:disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
        "schema:name": {"@type": "xsd:string"},
        "schema:titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "schema:isBasedOn": {"@id": "wiki:Qbook"}
    });

    assert_eq!(json_result, expected);
}

/// Regression test: bare `{"@id": "?s"}` where clause (no additional constraints)
/// should return all subjects with all properties — equivalent to "select all".
#[tokio::test]
async fn jsonld_bare_id_variable_returns_all_subjects() {
    let (fluree, ledger) = seed_simple_subject_crawl().await;

    // This is the exact query pattern reported as returning an empty array.
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/"
        },
        "select": { "?s": ["*"] },
        "where": { "@id": "?s" }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let mut json_result = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    normalize_object_arrays(&mut json_result);

    let arr = json_result.as_array().expect("should be an array");
    // The seed data has 4 subjects (alice, brian, cam, david).
    // A bare where clause should return at least those 4.
    assert!(
        !arr.is_empty(),
        "bare {{\"@id\": \"?s\"}} should NOT return an empty array, got: {json_result}"
    );
    assert!(
        arr.len() >= 4,
        "expected at least 4 subjects, got {}: {json_result}",
        arr.len()
    );
}
