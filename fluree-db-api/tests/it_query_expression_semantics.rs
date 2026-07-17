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

// =============================================================================
// PR-X2 — equality / EBV / numeric-promotion lattice
//
// These fixes are IR/engine-level (value model, arithmetic promotion, EBV,
// RDFterm-equal, aggregate finalize), so each carries a JSON-LD-surface
// regression alongside the SPARQL one — the W3C submodule only guards the
// SPARQL surface (docs/contributing/sparql-compliance.md § Query Surface
// Parity). Cypher is excluded (shared engine, no grammar obligation).
// =============================================================================

/// One node carrying an xsd:float, xsd:decimal and xsd:double property.
async fn seed_typed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:n",
            "ex:f": { "@value": "1", "@type": "xsd:float" },
            "ex:dec": { "@value": "1", "@type": "xsd:decimal" },
            "ex:d": { "@value": 1.0, "@type": "xsd:double" }
        }]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

// D4 — xsd:float stays float under arithmetic; double ∘ decimal widens to
// double, not decimal (XPath op:numeric-* promotion, integer<decimal<float<
// double).
#[tokio::test]
async fn sparql_numeric_promotion_result_datatype() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed(&fluree, "x2/d4:sparql").await;
    let p = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             PREFIX ex: <http://example.org/ns/> ";
    for (expr, dt, expect) in [
        ("?f + ?f", "xsd:float", true),
        ("?d + ?dec", "xsd:double", true),
        ("?d + ?dec", "xsd:decimal", false), // double∘decimal is NOT decimal
        ("?f + ?dec", "xsd:float", true),    // float∘decimal is float
    ] {
        let q = format!(
            "{p} ASK {{ ex:n ex:f ?f ; ex:dec ?dec ; ex:d ?d . \
             FILTER(datatype({expr}) = {dt}) }}"
        );
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(expect),
            "datatype({expr}) = {dt}"
        );
    }
}

#[tokio::test]
async fn jsonld_numeric_promotion_result_datatype() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed(&fluree, "x2/d4:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?ft", "?dt"],
        "where": [
            { "@id": "ex:n", "ex:f": "?f", "ex:d": "?d", "ex:dec": "?dec" },
            ["bind", "?ft", ["expr", ["datatype", ["+", "?f", "?f"]]]],
            ["bind", "?dt", ["expr", ["datatype", ["+", "?d", "?dec"]]]]
        ]
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["xsd:float", "xsd:double"]])
    );
}

/// One node carrying a negative, fractional xsd:float — drives all four numeric
/// builtins (ABS/ROUND/CEIL/FLOOR) and isNumeric across the rounding directions.
async fn seed_float(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [{ "@id": "ex:n", "ex:f": { "@value": "-2.5", "@type": "xsd:float" } }]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

// B1 — a stored xsd:float must flow through ABS/ROUND/CEIL/FLOOR and isNumeric.
// On wave-3 the builtins' `Some(_) => Ok(None)` catch-all swallowed
// ComparableValue::Float, so these returned unbound and isNumeric was false —
// every ASK below was `false`. Regression for the missing Float arm.
#[tokio::test]
async fn sparql_xsd_float_through_numeric_builtins() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_float(&fluree, "x2/floatbuiltins:sparql").await;
    let p = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             PREFIX ex: <http://example.org/ns/> ";
    // Value equality is rendering-independent: the xsd:float compares
    // numerically against the xsd:decimal constants. ROUND is W3C half-toward-+∞.
    for expr in [
        "ABS(?f) = 2.5",
        "ROUND(?f) = -2",
        "CEIL(?f) = -2",
        "FLOOR(?f) = -3",
        "datatype(ABS(?f)) = xsd:float",
        "isNumeric(?f)",
    ] {
        let q = format!("{p} ASK {{ ex:n ex:f ?f FILTER({expr}) }}");
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(true),
            "{expr}"
        );
    }
}

#[tokio::test]
async fn jsonld_xsd_float_through_numeric_builtins() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_float(&fluree, "x2/floatbuiltins:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?abs", "?isnum"],
        "where": [
            { "@id": "ex:n", "ex:f": "?f" },
            ["bind", "?abs", ["expr", ["abs", "?f"]]],
            ["bind", "?isnum", ["expr", ["isnumeric", "?f"]]]
        ]
    });
    // ABS keeps the xsd:float datatype; isNumeric(xsd:float) is true. On wave-3
    // ?abs was unbound (null) and ?isnum false.
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([[{ "@value": "2.5", "@type": "xsd:float" }, true]])
    );
}

// O2 — IN / NOT IN use value equality (rdf_term_equal), matching `=`. On wave-3
// they used the variant-exact derived `==`, so `1 IN (1.0)` was false while
// `1 = 1.0` was true. (The §17.4.1.9 error-in-IN 3-valued half is deferred.)
#[tokio::test]
async fn sparql_in_uses_value_equality() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/in:sparql").await;
    for (filter, expect) in [
        ("1 IN (1.0)", true),
        ("1 = 1.0", true),
        ("1 NOT IN (1.0)", false),
        ("1 IN (2.0, 3)", false),
        ("1 IN (2, 1.0)", true),
    ] {
        let q = format!("ASK {{ FILTER({filter}) }}");
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(expect),
            "{filter}"
        );
    }
}

#[tokio::test]
async fn jsonld_in_uses_value_equality() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/in:jsonld").await;
    // `(in 1 1.0)` holds by value equality → the row survives (empty on wave-3).
    // Shared IR: the same eval_in fix serves both surfaces.
    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "ex:alice", "ex:name": "?n" },
            ["filter", "(in 1 [1.0])"]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q).await, json!([["Alice"]]));
}

/// Four one-property nodes spanning the EBV cases: numeric zero, empty string,
/// a truthy number and a truthy string.
async fn seed_ebv(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:zero", "ex:v": 0 },
            { "@id": "ex:empty", "ex:v": "" },
            { "@id": "ex:num", "ex:v": 5 },
            { "@id": "ex:str", "ex:v": "hi" }
        ]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

// D-EBV — FILTER(?v) keeps a non-zero number and a non-empty string; a
// numeric zero and an empty string are falsy (not "any bound literal is true").
#[tokio::test]
async fn sparql_ebv_is_datatype_aware() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ebv(&fluree, "x2/ebv:sparql").await;
    let rows = sparql_rows(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/ns/> \
         SELECT ?s WHERE { ?s ex:v ?v . FILTER(?v) } ORDER BY ?s",
    )
    .await;
    assert_eq!(rows, json!([["ex:num"], ["ex:str"]]));
}

#[tokio::test]
async fn jsonld_ebv_is_datatype_aware() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ebv(&fluree, "x2/ebv:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": [
            { "@id": "?s", "ex:v": "?v" },
            ["filter", "(bound ?v)"],
            ["filter", "?v"]
        ],
        "orderBy": "?s"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["ex:num"], ["ex:str"]])
    );
}

// D5 — `=` is value equality with numeric promotion (integer = double); a plain
// string is known-unequal to a number (a different value space), not "equal
// because both stringify to 1".
#[tokio::test]
async fn sparql_rdfterm_equal_is_datatype_aware() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed(&fluree, "x2/d5:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    // 1 (integer, via decimal "1") value-equals 1.0 (double): numeric promotion.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ex:n ex:dec ?dec ; ex:d ?d . FILTER(?dec = ?d) }}"),
        )
        .await,
        JsonValue::Bool(true)
    );
    // The plain string "1" is NOT equal to the number 1.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ex:n ex:d ?d . FILTER(\"1\" = ?d) }}"),
        )
        .await,
        JsonValue::Bool(false)
    );
}

#[tokio::test]
async fn jsonld_rdfterm_equal_is_datatype_aware() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_typed(&fluree, "x2/d5:jsonld").await;
    let q_eq = json!({
        "@context": ctx(),
        "select": ["?d"],
        "where": [
            { "@id": "ex:n", "ex:dec": "?dec", "ex:d": "?d" },
            ["filter", "(= ?dec ?d)"]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_eq).await, json!([[1.0]]));

    let q_ne = json!({
        "@context": ctx(),
        "select": ["?d"],
        "where": [
            { "@id": "ex:n", "ex:d": "?d" },
            ["filter", "(= \"1\" ?d)"]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_ne).await, json!([]));
}

/// A foreign-datatype literal and a plain string on the same predicate (so a
/// self-join materializes both as `Binding::Lit`): same lexeme for the `=`
/// case, different lexeme (on a second predicate) for the `!=` case.
async fn seed_foreign(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:fa", "ex:v": { "@value": "zzz", "@type": "ex:myType" } },
            { "@id": "ex:pb", "ex:v": "zzz" },
            { "@id": "ex:fc", "ex:w": { "@value": "abc", "@type": "ex:myType" } },
            { "@id": "ex:pd", "ex:w": "xyz" }
        ]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

// D5 (wave-2-FAILING): a foreign-datatype literal is NOT value-equal to a plain
// string of the same lexeme (`=` → type error, cross pair excluded), and against
// a DIFFERENT lexeme it is a type error under `!=` too (excluded, NOT `!=`-true).
// On wave-2 the datatype was dropped: the `=` pair matched (ASK true) and the
// `!=` pair returned true (ASK true). These assert the branch's exclusions.
#[tokio::test]
async fn sparql_foreign_datatype_is_not_string_equal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_foreign(&fluree, "x2/d5fk:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    // `=`: "zzz"^^:myType vs "zzz" → type error → no cross pair passes.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ?a ex:v ?va . ?b ex:v ?vb . FILTER(?a != ?b && ?va = ?vb) }}"),
        )
        .await,
        JsonValue::Bool(false)
    );
    // `!=`: "abc"^^:myType vs "xyz" → type error → excluded (not `!=`-true).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ?a ex:w ?va . ?b ex:w ?vb . FILTER(?a != ?b && ?va != ?vb) }}"),
        )
        .await,
        JsonValue::Bool(false)
    );
}

#[tokio::test]
async fn jsonld_foreign_datatype_is_not_string_equal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_foreign(&fluree, "x2/d5fk:jsonld").await;
    let q_eq = json!({
        "@context": ctx(),
        "select": ["?a"],
        "where": [
            { "@id": "?a", "ex:v": "?va" },
            { "@id": "?b", "ex:v": "?vb" },
            ["filter", ["and", ["!=", "?a", "?b"], ["=", "?va", "?vb"]]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_eq).await, json!([]));
    let q_ne = json!({
        "@context": ctx(),
        "select": ["?a"],
        "where": [
            { "@id": "?a", "ex:w": "?va" },
            { "@id": "?b", "ex:w": "?vb" },
            ["filter", ["and", ["!=", "?a", "?b"], ["!=", "?va", "?vb"]]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_ne).await, json!([]));
}

// Three-valued OR (§17.2, wave-2-FAILING): an operand type error does not abort a
// disjunction whose other operand is true (open-cmp-02). `(?n < 1) || (1 = 1)`:
// `?n < 1` (string < number) is a type error, but `1 = 1` is true → the row
// survives. Wave-2 propagated the first error → the row was (wrongly) dropped.
#[tokio::test]
async fn sparql_three_valued_or_true_dominates_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/or:sparql").await;
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX ex: <http://example.org/ns/> \
             ASK { ?s ex:name ?n . FILTER((?n < 1) || (1 = 1)) }",
        )
        .await,
        JsonValue::Bool(true)
    );
}

#[tokio::test]
async fn jsonld_three_valued_or_true_dominates_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/or:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["filter", ["or", ["<", "?n", 1], ["=", 1, 1]]]
        ],
        "orderBy": "?n"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["Alice"], ["Bob"]])
    );
}

// agg02 — CONFIRMATORY, not discriminating (deliberate; see pr-description "agg02
// probe"). These assert the predicate-grouped COUNT datatype is xsd:integer, but on
// `FlureeBuilder::memory()` the query runs through the general `group_aggregate`
// finalize, which already emits xsd:integer on wave-2 too — so they pass on both
// branches and do NOT by themselves guard a revert of the agg02 fix. The bug lived
// only on the INDEXED predicate-grouped COUNT fast path (`StatsCountByPredicateOperator`,
// stats_query.rs / fast_group_count_firsts.rs), which a memory ledger bypasses (no
// IndexStats / binary graph view). Making these discriminating is not a small change:
// the datatype can't be read directly off the fast path — `detect_stats_count_by_predicate`
// declines any post-aggregation bind, and xsd:long/xsd:integer both render as bare JSON
// numbers (is_integer_family_dt), so observing the datatype needs an OUTER subquery scope
// wrapping an INDEXED inner (native-gated indexing harness + fast-path-in-subquery
// threading). Since the greened W3C agg02 test already guards that fast-path re-typing
// against indexed storage (reviewer-confirmed: no coverage hole), the indexed+subquery
// variant is deliberately not duplicated here (reviewer finding #4b). Kept as intent /
// round-trip documentation of the fix.
#[tokio::test]
async fn sparql_grouped_count_is_xsd_integer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/agg02:sparql").await;
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             ASK { { SELECT ?p (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p } \
             FILTER(datatype(?c) = xsd:integer) }",
        )
        .await,
        JsonValue::Bool(true)
    );
}

#[tokio::test]
async fn jsonld_grouped_count_is_xsd_integer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/agg02:jsonld").await;
    // Grouped COUNT projected as datatype(count(?o)); asserts xsd:integer. Confirmatory
    // only on the memory path (see the note above sparql_grouped_count_is_xsd_integer:
    // the xsd:long bug is on the indexed fast path a memory ledger bypasses).
    let q = json!({
        "@context": ctx(),
        "select": ["(as (datatype (count ?o)) ?dt)"],
        "where": [
            { "@id": "?s", "ex:name": "?o" }
        ],
        "groupBy": ["?s"]
    });
    let rows = jsonld_rows(&fluree, &ledger, &q).await;
    let arr = rows.as_array().expect("array");
    assert!(!arr.is_empty(), "expected grouped rows, got {rows}");
    for row in arr {
        assert_eq!(
            row,
            &json!(["xsd:integer"]),
            "COUNT datatype must be xsd:integer"
        );
    }
}

// D11 — CONCAT of a non-string argument is a type error (result unbound), not a
// silent numeric-to-string coercion.
#[tokio::test]
async fn sparql_concat_non_string_is_type_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/concat:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    // ex:age is an integer → CONCAT errors → BOUND(?c) is false.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!(
                "{p} ASK {{ ?s ex:age ?a . BIND(CONCAT(\"x\", ?a) AS ?c) FILTER(BOUND(?c)) }}"
            ),
        )
        .await,
        JsonValue::Bool(false)
    );
    // Both string arguments → CONCAT binds.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!(
                "{p} ASK {{ ?s ex:name ?n . BIND(CONCAT(\"x\", ?n) AS ?c) FILTER(BOUND(?c)) }}"
            ),
        )
        .await,
        JsonValue::Bool(true)
    );
}

/// One node carrying a foreign-datatype (non-xsd) string literal, which
/// `as_str()` exposes but CONCAT must reject.
async fn seed_foreign_typed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [{ "@id": "ex:m", "ex:custom": { "@value": "abc", "@type": "ex:myType" } }]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

// N2a — CONCAT type-errors on a foreign-datatype literal (§17.4.3.5). On wave-3
// "abc"^^ex:myType was concatenated (as_str exposed it) and ?c bound; CONCAT now
// rejects it → ?c unbound. as_str() itself is unchanged (STR-family still uses it).
#[tokio::test]
async fn sparql_concat_rejects_foreign_typed_argument() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_foreign_typed(&fluree, "x2/concat-foreign:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    // Foreign-typed arg → CONCAT errors → BOUND(?c) is false.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!(
                "{p} ASK {{ ex:m ex:custom ?v . BIND(CONCAT(?v, \"z\") AS ?c) FILTER(BOUND(?c)) }}"
            ),
        )
        .await,
        JsonValue::Bool(false)
    );
    // Plain-string CONCAT still binds (control — the change is arg-type-specific).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!(
                "{p} ASK {{ ex:m ex:custom ?v . BIND(CONCAT(\"p\", \"q\") AS ?c) FILTER(BOUND(?c)) }}"
            ),
        )
        .await,
        JsonValue::Bool(true)
    );
}

// D11 (JSON-LD sibling) — CONCAT of a non-string argument is a type error → the
// BIND leaves the target unbound (null). Pairs with the SPARQL D11 test above.
#[tokio::test]
async fn jsonld_concat_non_string_is_type_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/concat:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?bad", "?good"],
        "where": [
            { "@id": "ex:alice", "ex:name": "?n", "ex:age": "?a" },
            ["bind", "?bad", ["expr", ["concat", "?n", "?a"]]],
            ["bind", "?good", ["expr", ["concat", "?n", "?n"]]]
        ]
    });
    // ?a is an integer → CONCAT(?n, ?a) errors → ?bad null; CONCAT(?n, ?n) binds.
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([[null, "AliceAlice"]])
    );
}

/// Two groups: one all-numeric, one with a non-numeric (IRI) member.
async fn seed_groups(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:g1", "ex:p": [1, 2, 3] },
            { "@id": "ex:g2", "ex:p": [{ "@id": "ex:notanumber" }, 4] }
        ]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

// agg-err-01 — AVG over a group containing a non-numeric member is a type error
// (the aggregate is unbound), not an average over the numeric subset.
#[tokio::test]
async fn jsonld_avg_poisons_on_non_numeric_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_groups(&fluree, "x2/agg:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?g", "(as (avg ?p) ?avg)"],
        "where": [{ "@id": "?g", "ex:p": "?p" }],
        "groupBy": ["?g"],
        "orderBy": "?g"
    });
    // g1 = avg(1,2,3) = 2 (xsd:decimal, rendered "2"); g2 has a non-numeric (IRI)
    // member so AVG poisons → ?avg is unbound (null), NOT the average of {4}.
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["ex:g1", "2"], ["ex:g2", null]])
    );
}

// agg-err-01 (SPARQL sibling) — AVG over a group with a non-numeric member is a
// type error (the aggregate is unbound), not an average of the numeric subset.
// Pairs with `jsonld_avg_poisons_on_non_numeric_member`.
#[tokio::test]
async fn sparql_avg_poisons_on_non_numeric_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_groups(&fluree, "x2/agg:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    let q =
        format!("{p} SELECT ?g (AVG(?p) AS ?avg) WHERE {{ ?g ex:p ?p }} GROUP BY ?g ORDER BY ?g");
    // g1 = avg(1,2,3) = 2 (xsd:decimal "2"); g2 poisons on its non-numeric member → null.
    assert_eq!(
        sparql_rows(&fluree, &ledger, &q).await,
        json!([["ex:g1", "2"], ["ex:g2", null]])
    );
}

// =============================================================================
// XPath op:numeric-equal NaN semantics — NaN is never equal, including to
// itself (`NaN = NaN` → false, `NaN != NaN` → true). numeric_cmp's bit-level
// total order (kept for ORDER BY stability) would otherwise report two
// identical-bit NaNs as Equal via rdf_term_equal's value fast path.
// =============================================================================

/// One node carrying a stored `"NaN"^^xsd:double`, so the stored/value path
/// (not just constant folding) drives the NaN rule.
async fn seed_nan(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let tx = json!({
        "@context": ctx(),
        "@graph": [{ "@id": "ex:n", "ex:d": { "@value": "NaN", "@type": "xsd:double" } }]
    });
    fluree.insert(ledger0, &tx).await.expect("insert").ledger
}

#[tokio::test]
async fn sparql_nan_is_never_equal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_nan(&fluree, "x2/nan:sparql").await;
    let p = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             PREFIX ex: <http://example.org/ns/> ";
    // Constant form: NaN = NaN → false; NaN != NaN → true; NaN = 1.0 → false.
    for (filter, expect) in [
        (r#""NaN"^^xsd:double = "NaN"^^xsd:double"#, false),
        (r#""NaN"^^xsd:double != "NaN"^^xsd:double"#, true),
        (r#""NaN"^^xsd:double = 1.0"#, false),
    ] {
        let q = format!("{p} ASK {{ FILTER({filter}) }}");
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(expect),
            "{filter}"
        );
    }
    // Stored form: a bound NaN double is not `=`-equal to itself, and `!=` is
    // true (the row survives).
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ex:n ex:d ?v FILTER(?v = ?v) }}"),
        )
        .await,
        JsonValue::Bool(false)
    );
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ex:n ex:d ?v FILTER(?v != ?v) }}"),
        )
        .await,
        JsonValue::Bool(true)
    );
}

// JSON-LD twin (shared rdf_term_equal on the IR): a stored NaN double is
// `=`-unequal to itself and `!=`-true.
#[tokio::test]
async fn jsonld_nan_is_never_equal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_nan(&fluree, "x2/nan:jsonld").await;
    let q_eq = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": [
            { "@id": "?s", "ex:d": "?v" },
            ["filter", ["=", "?v", "?v"]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_eq).await, json!([]));
    let q_ne = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": [
            { "@id": "?s", "ex:d": "?v" },
            ["filter", ["!=", "?v", "?v"]]
        ]
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q_ne).await,
        json!([["ex:n"]])
    );
}

// =============================================================================
// Unary minus coerces cast results; CONCAT reads the tag off the value;
// ROUND/CEIL/FLOOR pass big integers through (review follow-ups).
// =============================================================================

#[tokio::test]
async fn sparql_negate_coerces_cast_results() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/negate:sparql").await;
    let p = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    // A cast result is a string-backed TypedLiteral; unary minus must coerce
    // it like binary arithmetic does (pre-fix: ?v unbound → ASK false).
    for (expr, expect) in [
        ("-xsd:float(\"1.5\") = -1.5", true),
        ("datatype(-xsd:float(\"1.5\")) = xsd:float", true),
        ("-xsd:decimal(\"2.5\") = -2.5", true),
    ] {
        let q = format!("{p} ASK {{ FILTER({expr}) }}");
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(expect),
            "{expr}"
        );
    }
}

#[tokio::test]
async fn jsonld_negate_coerces_cast_results() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/negate:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "ex:alice", "ex:name": "?n" },
            ["filter", ["=", ["negate", ["xsd:float", "1.5"]], -1.5]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q).await, json!([["Alice"]]));
}

// §17.4.3.5: CONCAT keeps the tag when ALL args share it — including constant
// and computed (STRLANG) args, whose tag lives on the VALUE rather than a
// variable binding (pre-fix these came back plain). Tags compare
// case-insensitively (RDF 1.1); a genuine mismatch drops the tag.
#[tokio::test]
async fn sparql_concat_preserves_constant_lang_tags() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/concatlang:sparql").await;
    for (expr, expect) in [
        (r#"CONCAT("a"@en, "b"@en) = "ab"@en"#, true),
        (r#"LANG(CONCAT("a"@en, "b"@en)) = "en""#, true),
        (r#"CONCAT("a"@EN, "b"@en) = "ab"@en"#, true), // case-insensitive tags
        (r#"LANG(CONCAT("a"@en, "b"@fr)) = """#, true), // mismatch → plain
        (r#"CONCAT(STRLANG("a", "en"), "b"@en) = "ab"@en"#, true),
    ] {
        let q = format!("ASK {{ FILTER({expr}) }}");
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(expect),
            "{expr}"
        );
    }
}

#[tokio::test]
async fn jsonld_concat_preserves_constant_lang_tags() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/concatlang:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "ex:alice", "ex:name": "?n" },
            ["filter", ["=",
                ["concat", ["strlang", "a", "en"], ["strlang", "b", "en"]],
                ["strlang", "ab", "en"]]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q).await, json!([["Alice"]]));
}

// fn:round/fn:ceiling/fn:floor are identity on integers; an xsd:integer wider
// than i64 (ComparableValue::BigInt) must pass through, not go unbound.
#[tokio::test]
async fn sparql_round_ceil_floor_pass_big_integers_through() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/bigint:sparql").await;
    let p = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    let big = "99999999999999999999"; // > i64::MAX
    for func in ["ROUND", "CEIL", "FLOOR", "ABS"] {
        let q = format!(
            "{p} ASK {{ FILTER({func}(\"{big}\"^^xsd:integer) = \"{big}\"^^xsd:integer) }}"
        );
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(true),
            "{func}({big})"
        );
    }
}

// =============================================================================
// Three-valued AND (§17.2) — the other half of the dominance table (OR is
// pinned above): F && err = F (the row survives a negated conjunction), and
// err && T = err (the row drops). Wave-2 aborted the whole expression on the
// first operand error, failing both directions.
// =============================================================================

#[tokio::test]
async fn sparql_three_valued_and_dominance() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/and:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    // F && err = F: `1 = 2` is false, `?n < 1` (string < number) errors —
    // false dominates, so the negation is true and the row survives.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ?s ex:name ?n . FILTER(!((?n < 1) && (1 = 2))) }}"),
        )
        .await,
        JsonValue::Bool(true),
        "F && err must be F (not an aborted error)"
    );
    // err && T = err: the error survives a true conjunct → row dropped.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ ?s ex:name ?n . FILTER((?n < 1) && (1 = 1)) }}"),
        )
        .await,
        JsonValue::Bool(false),
        "err && T must stay a type error (row excluded)"
    );
}

#[tokio::test]
async fn jsonld_three_valued_and_dominance() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/and:jsonld").await;
    // F && err = F → not(false) = true → both rows survive.
    let q_not = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["filter", ["not", ["and", ["<", "?n", 1], ["=", 1, 2]]]]
        ],
        "orderBy": "?n"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q_not).await,
        json!([["Alice"], ["Bob"]])
    );
    // err && T = err → rows dropped.
    let q_err = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "?s", "ex:name": "?n" },
            ["filter", ["and", ["<", "?n", 1], ["=", 1, 1]]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_err).await, json!([]));
}

// =============================================================================
// SUM poison (the commit says "aggregates" plural; only AVG was pinned) — a
// non-numeric group member is a type error for SUM too, not a sum over the
// numeric subset.
// =============================================================================

#[tokio::test]
async fn sparql_sum_poisons_on_non_numeric_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_groups(&fluree, "x2/sum:sparql").await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    let q =
        format!("{p} SELECT ?g (SUM(?p) AS ?sum) WHERE {{ ?g ex:p ?p }} GROUP BY ?g ORDER BY ?g");
    // g1 = 1+2+3 = 6; g2 poisons on its non-numeric member → null.
    assert_eq!(
        sparql_rows(&fluree, &ledger, &q).await,
        json!([["ex:g1", 6], ["ex:g2", null]])
    );
}

#[tokio::test]
async fn jsonld_sum_poisons_on_non_numeric_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_groups(&fluree, "x2/sum:jsonld").await;
    let q = json!({
        "@context": ctx(),
        "select": ["?g", "(as (sum ?p) ?sum)"],
        "where": [{ "@id": "?g", "ex:p": "?p" }],
        "groupBy": ["?g"],
        "orderBy": "?g"
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q).await,
        json!([["ex:g1", 6], ["ex:g2", null]])
    );
}

// =============================================================================
// Indexed twins — a reindexed, novelty-free ledger routes scans through the
// binary store (late-materialized bindings + the indexed aggregate paths that
// FlureeBuilder::memory() alone never exercises).
// =============================================================================

/// Insert `tx`, reindex to a binary store, and return the purely-indexed ledger
/// (no trailing novelty) so scans late-materialize to `Binding::EncodedLit`.
async fn seed_indexed(fluree: &MemoryFluree, ledger_id: &str, tx: &JsonValue) -> MemoryLedger {
    use fluree_db_api::ReindexOptions;
    let ledger0 = genesis_ledger(fluree, ledger_id);
    fluree.insert(ledger0, tx).await.expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert!(
        indexed.snapshot.range_provider.is_some(),
        "expected binary range provider after reindex (EncodedLit path)"
    );
    indexed
}

fn groups_tx() -> JsonValue {
    json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:g1", "ex:p": [1, 2, 3] },
            { "@id": "ex:g2", "ex:p": [{ "@id": "ex:notanumber" }, 4] }
        ]
    })
}

// SUM/AVG poison on the INDEXED path: the memory-backed twins above exercise
// only the general/streaming aggregate; a reindexed ledger routes through the
// binary-store scan (and any indexed aggregate specialization), so this pins
// the poison rule where production ledgers actually run.
#[tokio::test]
async fn sparql_sum_avg_poison_on_indexed_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_indexed(&fluree, "x2/aggidx:sparql", &groups_tx()).await;
    let p = "PREFIX ex: <http://example.org/ns/> ";
    let q_sum =
        format!("{p} SELECT ?g (SUM(?p) AS ?sum) WHERE {{ ?g ex:p ?p }} GROUP BY ?g ORDER BY ?g");
    assert_eq!(
        sparql_rows(&fluree, &ledger, &q_sum).await,
        json!([["ex:g1", 6], ["ex:g2", null]])
    );
    let q_avg =
        format!("{p} SELECT ?g (AVG(?p) AS ?avg) WHERE {{ ?g ex:p ?p }} GROUP BY ?g ORDER BY ?g");
    assert_eq!(
        sparql_rows(&fluree, &ledger, &q_avg).await,
        json!([["ex:g1", "2"], ["ex:g2", null]])
    );
}

// =============================================================================
// §17.2.2 EBV of string-backed numeric/boolean literals (review follow-up):
// a cast result (`xsd:float("1.5")` — a String tagged xsd:float) is truthy by
// the numeric rule on BOTH the direct-expression and the BIND'd-binding path,
// and an ILL-FORMED numeric/boolean lexical form is EBV FALSE (rule 1), not a
// type error.
// =============================================================================

#[tokio::test]
async fn sparql_ebv_of_cast_numeric_literals() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/ebvcast:sparql").await;
    let p = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    for (filter, expect) in [
        // Direct expression path (comparable_effective_bool).
        ("xsd:float(\"1.5\")", true),
        ("xsd:float(\"0\")", false),
        ("xsd:decimal(\"2.5\")", true),
        // Ill-formed lexical forms via STRDT: EBV false — so the negation is
        // TRUE (discriminates rule-1 false from a demoted type error, which
        // would fail both directions).
        ("!STRDT(\"abc\", xsd:integer)", true),
        ("STRDT(\"abc\", xsd:integer)", false),
        ("STRDT(\"true\", xsd:boolean)", true),
        ("STRDT(\"nope\", xsd:boolean)", false),
    ] {
        let q = format!("{p} ASK {{ FILTER({filter}) }}");
        assert_eq!(
            sparql_rows(&fluree, &ledger, &q).await,
            JsonValue::Bool(expect),
            "{filter}"
        );
    }
    // BIND'd-binding path (lit_effective_bool): the cast lands as a
    // String-backed Binding::Lit tagged xsd:float; FILTER(?f) must be truthy.
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ BIND(xsd:float(\"1.5\") AS ?f) FILTER(?f) }}"),
        )
        .await,
        JsonValue::Bool(true),
        "BIND'd cast float must be truthy"
    );
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            &format!("{p} ASK {{ BIND(xsd:float(\"0\") AS ?f) FILTER(!?f) }}"),
        )
        .await,
        JsonValue::Bool(true),
        "BIND'd cast zero-float must be falsy (EBV false, not error)"
    );
}

#[tokio::test]
async fn jsonld_ebv_of_cast_numeric_literals() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "x2/ebvcast:jsonld").await;
    // Truthy cast float keeps the row; zero-float drops it (shared EBV).
    let q_truthy = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "ex:alice", "ex:name": "?n" },
            ["filter", ["xsd:float", "1.5"]]
        ]
    });
    assert_eq!(
        jsonld_rows(&fluree, &ledger, &q_truthy).await,
        json!([["Alice"]])
    );
    let q_falsy = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            { "@id": "ex:alice", "ex:name": "?n" },
            ["filter", ["xsd:float", "0"]]
        ]
    });
    assert_eq!(jsonld_rows(&fluree, &ledger, &q_falsy).await, json!([]));
}
