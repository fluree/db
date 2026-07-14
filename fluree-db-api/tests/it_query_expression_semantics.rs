//! Expression-semantics regression tests (W3C burn-down PR-X1).
//!
//! Covers the cheap high-yield expression defects: constant FILTER placement
//! (#1439), DATATYPE()/LANG() expression arguments and SPARQL-scoped
//! non-literal type errors (#1440, decision D-12), xsd:dateTime/date/time
//! casts, BNODE per-solution identity, the regex `q` flag, and bare-integer
//! lowering (#1319).
//!
//! Per `docs/contributing/sparql-compliance.md` § Query Surface Parity these
//! fixes are IR/engine-level, so each one carries a JSON-LD-surface
//! regression alongside the SPARQL one — the W3C submodule only guards the
//! SPARQL surface. The D2b split is deliberate and asserted on BOTH surfaces:
//! SPARQL `DATATYPE`/`LANG` of a non-literal is a type error (row excluded /
//! variable unbound), while the JSON-LD surface keeps Fluree's documented
//! `@id`-datatype extension.

use crate::support;
use crate::support::{genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:age": 30 },
            { "@id": "ex:bob", "ex:name": "Bob", "ex:age": 25 }
        ]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

async fn sparql_rows(fluree: &MemoryFluree, ledger: &MemoryLedger, q: &str) -> JsonValue {
    support::query_sparql(fluree, ledger, q)
        .await
        .expect("query")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async")
}

async fn jsonld_rows(fluree: &MemoryFluree, ledger: &MemoryLedger, q: &JsonValue) -> JsonValue {
    support::query_jsonld(fluree, ledger, q)
        .await
        .expect("query")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async")
}

// =============================================================================
// D1 — a variable-free FILTER must apply to the group's solutions (#1439)
// =============================================================================

#[tokio::test]
async fn sparql_constant_filter_keeps_all_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d1:sparql").await;

    // Constant-true filters must be no-ops for the whole group.
    for q in [
        "ASK { FILTER(1 = 1) }",
        "ASK { FILTER(true) }",
        "ASK { ?s ?p ?o . FILTER(1 = 1) }",
        "ASK { FILTER(2 IN (1, 2, 3)) }",
        "ASK { FILTER(2 NOT IN ()) }",
    ] {
        assert_eq!(
            sparql_rows(&fluree, &ledger, q).await,
            JsonValue::Bool(true),
            "{q}"
        );
    }

    // Constant-false filters must eliminate the whole group.
    for q in ["ASK { FILTER(1 = 2) }", "ASK { ?s ?p ?o . FILTER(false) }"] {
        assert_eq!(
            sparql_rows(&fluree, &ledger, q).await,
            JsonValue::Bool(false),
            "{q}"
        );
    }

    // The filter's position must not gate the row source it precedes.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/ns/> \
         SELECT ?n WHERE { ?s ex:name ?n . FILTER(1 = 1) } ORDER BY ?n",
    )
    .await;
    assert_eq!(rows, json!([["Alice"], ["Bob"]]));

    let rows = sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?v WHERE { VALUES ?v { 1 2 } FILTER(true) } ORDER BY ?v",
    )
    .await;
    assert_eq!(rows, json!([[1], [2]]));
}

#[tokio::test]
async fn jsonld_constant_filter_keeps_all_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d1:jsonld").await;

    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["filter", "(= 1 1)"]
        ],
        "orderBy": "?n"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["Alice"], ["Bob"]])
    );

    let q_false = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["filter", "(= 1 2)"]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_false).await, json!([]));
}

// =============================================================================
// D2 — DATATYPE()/LANG() accept expression arguments (#1440)
// =============================================================================

#[tokio::test]
async fn sparql_datatype_of_expression_arguments() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d2:sparql").await;

    // Arithmetic argument (the type-promotion test shape).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             PREFIX ex: <http://example.org/ns/> \
             ASK { ?s ex:age ?a . FILTER(datatype(?a + ?a) = xsd:integer) }",
        )
        .await,
        JsonValue::Bool(true)
    );

    // Constant literal arguments (SPARQL12_RDF11 langstring-datatype /
    // plain-string-datatype shapes).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            r#"SELECT (DATATYPE("foo"@en) AS ?dt) WHERE {}"#,
        )
        .await,
        json!([["http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"]])
    );
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            r#"SELECT (DATATYPE("foo") AS ?dt) WHERE {}"#,
        )
        .await,
        json!([["http://www.w3.org/2001/XMLSchema#string"]])
    );

    // LANG of a constant lang-tagged literal.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            r#"SELECT (LANG("foo"@en) AS ?l) WHERE {}"#
        )
        .await,
        json!([["en"]])
    );

    // STR result is a plain string.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             PREFIX ex: <http://example.org/ns/> \
             ASK { ?s ex:age ?a . FILTER(datatype(str(?a)) = xsd:string) }",
        )
        .await,
        JsonValue::Bool(true)
    );
}

#[tokio::test]
async fn jsonld_datatype_of_expression_arguments() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d2:jsonld").await;

    // The same engine capability must be reachable from the JSON-LD surface.
    let q = json!({
        "@context": ctx(),
        "select": ["?n", "?dt"],
        "where": [
            { "@id": "?s", "ex:name": "?n", "ex:age": "?age" },
            ["bind", "?dt", ["expr", ["datatype", ["+", "?age", "?age"]]]]
        ],
        "orderBy": "?n"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["Alice", "xsd:integer"], ["Bob", "xsd:integer"]])
    );
}

// =============================================================================
// D2b — non-literal DATATYPE()/LANG(): SPARQL type error vs JSON-LD extension
// (decision D-12: SPARQL-scoped strictness; the JSON-LD `@id` extension is
// deliberate and preserved)
// =============================================================================

#[tokio::test]
async fn sparql_datatype_of_iri_is_type_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d2b:sparql-dt").await;

    // Project expression: the type error leaves ?dt unbound (projexp05 shape).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?s (DATATYPE(?s) AS ?dt) WHERE { ?s ex:name ?n } ORDER BY ?s",
        )
        .await,
        json!([["ex:alice", null], ["ex:bob", null]])
    );

    // FILTER: the type error excludes the row — even under `!=`
    // (dawg-datatype-2 shape).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             ASK { ?s ex:name ?n . FILTER(datatype(?s) != <http://example.org/NotADatatype>) }",
        )
        .await,
        JsonValue::Bool(false)
    );
}

#[tokio::test]
async fn sparql_lang_of_iri_is_type_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d2b:sparql-lang").await;

    // Project expression: unbound.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?s (LANG(?s) AS ?l) WHERE { ?s ex:name ?n } ORDER BY ?s",
        )
        .await,
        json!([["ex:alice", null], ["ex:bob", null]])
    );

    // FILTER: excluded even under `!=` (dawg-lang-1 shape).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             ASK { ?s ex:name ?n . FILTER(lang(?s) != \"fr\") }",
        )
        .await,
        JsonValue::Bool(false)
    );

    // LANG of a plain literal stays "" (not an error).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             ASK { ?s ex:name ?n . FILTER(lang(?n) = \"\") }",
        )
        .await,
        JsonValue::Bool(true)
    );
}

#[tokio::test]
async fn jsonld_datatype_of_iri_keeps_id_extension() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/d2b:jsonld").await;

    // JSON-LD surface: DATATYPE of an @id/ref reports the `@id` ref type —
    // the deliberate Fluree extension this fix must NOT remove (D-12).
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?dt"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["bind", "?dt", ["expr", ["datatype", "?s"]]]
        ],
        "orderBy": "?s"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["ex:alice", "@id"], ["ex:bob", "@id"]])
    );

    // JSON-LD LANG of a non-literal stays the lenient "" (no row loss).
    let q_lang = json!({
        "@context": ctx(),
        "select": ["?s", "?l"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["bind", "?l", ["expr", ["lang", "?s"]]]
        ],
        "orderBy": "?s"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q_lang).await,
        json!([["ex:alice", ""], ["ex:bob", ""]])
    );
}

// =============================================================================
// D3 — xsd:dateTime / xsd:date / xsd:time constructor casts
// (surface note: XSD casts are SPARQL-only syntax; the JSON-LD surface has no
// cast form — recorded in the PR description per compliance § Query Surface
// Parity)
// =============================================================================

#[tokio::test]
async fn sparql_xsd_temporal_casts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "exprsem/d3:sparql");
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:a", "ex:p": "2002-10-10T17:00:00Z" },
            { "@id": "ex:b", "ex:p": "not a date" },
            { "@id": "ex:c", "ex:p": 13 }
        ]
    });
    let ledger = fluree.insert(ledger0, &tx).await.expect("insert").ledger;

    // Only the parseable dateTime string survives the cast (cast-dT shape).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             PREFIX ex: <http://example.org/ns/> \
             SELECT ?s WHERE { ?s ex:p ?v . \
               FILTER(datatype(xsd:dateTime(?v)) = xsd:dateTime) }",
        )
        .await,
        json!([["ex:a"]])
    );

    // xsd:date / xsd:time parse their lexical forms.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             ASK { FILTER(datatype(xsd:date(\"2002-10-10\")) = xsd:date \
                   && datatype(xsd:time(\"17:00:00Z\")) = xsd:time) }",
        )
        .await,
        JsonValue::Bool(true)
    );
}

// =============================================================================
// D9 — BNODE(label): per-solution identity
// =============================================================================

#[tokio::test]
async fn sparql_bnode_label_is_per_solution() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "exprsem/d9:sparql");
    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:s1", "ex:str": "foo" },
            { "@id": "ex:s3", "ex:str": "BAZ" }
        ]
    });
    let ledger = fluree.insert(ledger0, &tx).await.expect("insert").ledger;

    // bnode01 shape: 2x2 cross product; equal args must share a bnode WITHIN
    // a solution and get fresh bnodes ACROSS solutions.
    let rows = sparql_rows(
        &fluree,
        &ledger,
        r"PREFIX : <http://example.org/>
SELECT ?s1 ?s2 (BNODE(?s1) AS ?b1) (BNODE(?s2) AS ?b2)
WHERE {
  ?a :str ?s1 .
  ?b :str ?s2 .
  FILTER (?a = :s1 || ?a = :s3)
  FILTER (?b = :s1 || ?b = :s3)
}",
    )
    .await;

    let rows = rows.as_array().expect("rows array");
    assert_eq!(rows.len(), 4);
    let mut all_bnodes: Vec<String> = Vec::new();
    for row in rows {
        let (s1, s2) = (row[0].as_str().unwrap(), row[1].as_str().unwrap());
        let (b1, b2) = (
            row[2].as_str().expect("b1 bound").to_string(),
            row[3].as_str().expect("b2 bound").to_string(),
        );
        assert!(b1.starts_with("_:") && b2.starts_with("_:"));
        if s1 == s2 {
            assert_eq!(b1, b2, "same label within one solution shares the bnode");
        } else {
            assert_ne!(b1, b2, "different labels get different bnodes");
        }
        all_bnodes.push(b1);
    }
    // The per-row ?b1 bnodes must be distinct across the four solutions.
    all_bnodes.sort();
    all_bnodes.dedup();
    assert_eq!(all_bnodes.len(), 4, "b1 must be fresh per solution");
}

// =============================================================================
// D10 — regex `q` flag (literal pattern, XPath fn:matches)
// =============================================================================

#[tokio::test]
async fn sparql_regex_q_flag_literal_pattern() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "exprsem/d10:sparql");
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:m", "ex:val": "a?+*.{}()[]c" },
            { "@id": "ex:n", "ex:val": "abc" }
        ]
    });
    let ledger = fluree.insert(ledger0, &tx).await.expect("insert").ledger;

    // With `q`, metacharacters match literally (regex-no-metacharacters shape).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?s WHERE { ?s ex:val ?v . FILTER regex(?v, \"a?+*.{}()[]c\", \"q\") }",
        )
        .await,
        json!([["ex:m"]])
    );

    // `q` composes with `i`.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?s WHERE { ?s ex:val ?v . FILTER regex(?v, \"A?+*.{}()[]C\", \"qi\") }",
        )
        .await,
        json!([["ex:m"]])
    );
}

// =============================================================================
// #1319 — bare integers in VALUES/inline data are xsd:integer terms
// =============================================================================

#[tokio::test]
async fn sparql_values_integer_is_xsd_integer_term() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/1319:sparql").await;

    // The result datatype tag matches storage tagging.
    let result = support::query_sparql(&fluree, &ledger, "SELECT ?v WHERE { VALUES ?v { 3 } }")
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    assert_eq!(
        sparql_json["results"]["bindings"][0]["v"]["datatype"],
        json!("http://www.w3.org/2001/XMLSchema#integer")
    );

    // Term identity: a VALUES integer and a stored integer of the same value
    // are the SAME term (sameTerm, DISTINCT collapse).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             ASK { VALUES ?v { 30 } ?s ex:age ?a . FILTER(sameTerm(?v, ?a)) }",
        )
        .await,
        JsonValue::Bool(true)
    );
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT DISTINCT ?v WHERE { { VALUES ?v { 30 } } UNION { ?s ex:age ?v . FILTER(?v = 30) } }",
        )
        .await,
        json!([[30]])
    );
}

#[tokio::test]
async fn jsonld_values_integer_is_xsd_integer_term() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "exprsem/1319:jsonld").await;

    // JSON-LD inline values: grouping a values-integer against stored
    // integers collapses to one group (term identity).
    let q = json!({
        "@context": ctx(),
        "select": ["?v", "?dt"],
        "where": [
            ["values", ["?v", [30]]],
            ["bind", "?dt", ["expr", ["datatype", "?v"]]]
        ]
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([[30, "xsd:integer"]])
    );

    // Values-integer joins/compares as the same term as a stored integer.
    let q_join = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": [
            ["values", ["?v", [30]]],
            { "@id": "?s", "ex:age": "?a" },
            ["filter", "(sameTerm ?v ?a)"]
        ]
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q_join).await,
        json!([["ex:alice"]])
    );
}
