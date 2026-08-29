//! Temporal arithmetic: differences, and shifting by a duration.
//!
//! `?end - ?start` over two `xsd:dateTime`s used to answer *unbound*. The
//! expression evaluator had no temporal arm, so the pair fell to the
//! non-numeric catch-all and raised `ArithmeticError::TypeMismatch` — which is
//! a dynamic value error, and those demote to unbound inside `BIND`/`Extend`
//! rather than surfacing. The query returned a row with an empty column and no
//! diagnostic, which reads like missing data rather than a missing operator.
//!
//! Fluree implements the operator rows SEP-0002 specifies over temporal and
//! duration operands: the three differences (`dateTime`/`date`/`time` minus its
//! own kind, yielding an `xsd:dayTimeDuration`) and the shifts (`± duration`,
//! yielding the same kind as the left operand). Duration ± duration is included
//! too — XPath defines it, and SEP-0002 says outright that its table does not
//! enumerate every relevant operator.
//!
//! Neither SPARQL 1.1 nor the 1.2 editor's draft maps these over temporal
//! operands, so answering a type error was conformant — but every engine that
//! implements this at all (Stardog, GraphDB, RDFox, Jena, Comunica, Oxigraph
//! behind its `sep-0002` feature) agrees on the XPath semantics that SEP-0002
//! writes down. Differences are signed and timezone-normalized; shifts are
//! calendar-aware, so a `yearMonthDuration` clamps to the end of the month
//! rather than overflowing it, and a `time` wraps within its day because it has
//! no date to carry into. That is what is pinned here.
//!
//! Two things beyond the arithmetic are load-bearing:
//!
//!   * **The datatype on the way out.** A duration has no dedicated
//!     `ComparableValue` variant — it rides as a `TypedLiteral` with no
//!     datatype constraint, and that arm of `to_binding` used to stamp
//!     *everything* `xsd:string`. So a correct `P1DT2H30M` could still leave
//!     the engine mislabelled as a string. Only an integration test sees this;
//!     the value assertions alone pass either way.
//!   * **Both query surfaces.** SPARQL and JSON-LD lower into the same IR
//!     (`Function::Sub`), so a fix on one is a fix on both — but only if the
//!     JSON-LD front end really does route `(- ?a ?b)` there. The twin cases
//!     below prove it rather than assuming it.

#![cfg(feature = "native")]

use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig, TxnOpts};
use serde_json::{json, Value};

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

const DATA: &str = r#"
@prefix ex: <http://example.org/ns/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# A day and a half apart, to the minute.
ex:e1 ex:start "2026-01-01T09:30:00Z"^^xsd:dateTime .
ex:e1 ex:end   "2026-01-02T12:00:00Z"^^xsd:dateTime .

# Same instant written in two timezones: the difference must be zero, not the
# three hours the wall-clock readings differ by.
ex:e2 ex:start "2026-01-01T10:00:00Z"^^xsd:dateTime .
ex:e2 ex:end   "2026-01-01T13:00:00+03:00"^^xsd:dateTime .

# End before start — the result is signed, not an absolute magnitude.
ex:e3 ex:start "2026-01-02T00:00:00Z"^^xsd:dateTime .
ex:e3 ex:end   "2026-01-01T00:00:00Z"^^xsd:dateTime .

ex:d1 ex:from "1999-11-28"^^xsd:date .
ex:d1 ex:to   "2000-10-30"^^xsd:date .

ex:t1 ex:from "04:00:00Z"^^xsd:time .
ex:t1 ex:to   "11:12:00Z"^^xsd:time .

# Duration operands. ex:eom is the end-of-month clamping case: adding a month
# to January 31st must land on February 28th, not overflow into March.
ex:p1 ex:at  "2026-01-01T09:30:00Z"^^xsd:dateTime .
ex:p1 ex:day "2026-01-01"^^xsd:date .
ex:p1 ex:tod "23:00:00Z"^^xsd:time .
ex:p1 ex:dur "PT2H30M"^^xsd:dayTimeDuration .
ex:p1 ex:mon "P1M"^^xsd:yearMonthDuration .
ex:eom ex:at "2026-01-31T00:00:00Z"^^xsd:dateTime .
"#;

/// The formatter compacts against the query's `@context`, which declares the
/// `xsd:` prefix, so the datatype comes back in prefixed form.
const XSD_DAY_TIME_DURATION: &str = "xsd:dayTimeDuration";

struct Case {
    name: &'static str,
    /// `?d` is the bound result in both surfaces.
    sparql: &'static str,
    jsonld_where: Value,
    /// Lexical form, or `None` when the expression must answer unbound.
    expected: Option<&'static str>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "dateTime difference",
            sparql: "SELECT ?d WHERE { ex:e1 ex:start ?s . ex:e1 ex:end ?e . \
                     BIND(?e - ?s AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:e1", "ex:start": "?s", "ex:end": "?e"},
                ["bind", "?d", "(- ?e ?s)"]
            ]),
            expected: Some("P1DT2H30M"),
        },
        Case {
            name: "dateTime difference normalizes timezones",
            sparql: "SELECT ?d WHERE { ex:e2 ex:start ?s . ex:e2 ex:end ?e . \
                     BIND(?e - ?s AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:e2", "ex:start": "?s", "ex:end": "?e"},
                ["bind", "?d", "(- ?e ?s)"]
            ]),
            expected: Some("PT0S"),
        },
        Case {
            name: "dateTime difference is signed",
            sparql: "SELECT ?d WHERE { ex:e3 ex:start ?s . ex:e3 ex:end ?e . \
                     BIND(?e - ?s AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:e3", "ex:start": "?s", "ex:end": "?e"},
                ["bind", "?d", "(- ?e ?s)"]
            ]),
            expected: Some("-P1D"),
        },
        Case {
            name: "date difference",
            sparql: "SELECT ?d WHERE { ex:d1 ex:from ?s . ex:d1 ex:to ?e . \
                     BIND(?e - ?s AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:d1", "ex:from": "?s", "ex:to": "?e"},
                ["bind", "?d", "(- ?e ?s)"]
            ]),
            expected: Some("P337D"),
        },
        Case {
            name: "time difference",
            sparql: "SELECT ?d WHERE { ex:t1 ex:from ?s . ex:t1 ex:to ?e . \
                     BIND(?e - ?s AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:t1", "ex:from": "?s", "ex:to": "?e"},
                ["bind", "?d", "(- ?e ?s)"]
            ]),
            expected: Some("PT7H12M"),
        },
        // Only `-` is defined over temporal operands. Adding two dateTimes is
        // meaningless in XPath and in SEP-0002 alike, so it must stay a type
        // error — i.e. still unbound. This is the guard that keeps the new arms
        // from being written as "any operator on two temporals".
        Case {
            name: "dateTime addition stays unbound",
            sparql: "SELECT ?d WHERE { ex:e1 ex:start ?s . ex:e1 ex:end ?e . \
                     BIND(?e + ?s AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:e1", "ex:start": "?s", "ex:end": "?e"},
                ["bind", "?d", "(+ ?e ?s)"]
            ]),
            expected: None,
        },
        // --- temporal ± duration (SEP-0002 / XPath) ---
        Case {
            name: "dateTime + dayTimeDuration",
            sparql: "SELECT ?d WHERE { ex:p1 ex:at ?a . ex:p1 ex:dur ?u . \
                     BIND(?a + ?u AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:at": "?a", "ex:dur": "?u"},
                ["bind", "?d", "(+ ?a ?u)"]
            ]),
            expected: Some("2026-01-01T12:00:00Z"),
        },
        Case {
            name: "dateTime - dayTimeDuration",
            sparql: "SELECT ?d WHERE { ex:p1 ex:at ?a . ex:p1 ex:dur ?u . \
                     BIND(?a - ?u AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:at": "?a", "ex:dur": "?u"},
                ["bind", "?d", "(- ?a ?u)"]
            ]),
            expected: Some("2026-01-01T07:00:00Z"),
        },
        Case {
            name: "dateTime + yearMonthDuration clamps to end of month",
            sparql: "SELECT ?d WHERE { ex:eom ex:at ?a . ex:p1 ex:mon ?m . \
                     BIND(?a + ?m AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:eom", "ex:at": "?a"},
                {"@id": "ex:p1", "ex:mon": "?m"},
                ["bind", "?d", "(+ ?a ?m)"]
            ]),
            expected: Some("2026-02-28T00:00:00Z"),
        },
        Case {
            name: "date + yearMonthDuration",
            sparql: "SELECT ?d WHERE { ex:p1 ex:day ?a . ex:p1 ex:mon ?m . \
                     BIND(?a + ?m AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:day": "?a", "ex:mon": "?m"},
                ["bind", "?d", "(+ ?a ?m)"]
            ]),
            expected: Some("2026-02-01"),
        },
        Case {
            name: "time + dayTimeDuration wraps within the day",
            sparql: "SELECT ?d WHERE { ex:p1 ex:tod ?a . ex:p1 ex:dur ?u . \
                     BIND(?a + ?u AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:tod": "?a", "ex:dur": "?u"},
                ["bind", "?d", "(+ ?a ?u)"]
            ]),
            expected: Some("01:30:00Z"),
        },
        Case {
            name: "duration + duration",
            sparql: "SELECT ?d WHERE { ex:p1 ex:dur ?u . BIND(?u + ?u AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:dur": "?u"},
                ["bind", "?d", "(+ ?u ?u)"]
            ]),
            expected: Some("PT5H"),
        },
        // The two duration families do not mix: months have no fixed length.
        Case {
            name: "dayTimeDuration + yearMonthDuration stays unbound",
            sparql: "SELECT ?d WHERE { ex:p1 ex:dur ?u . ex:p1 ex:mon ?m . \
                     BIND(?u + ?m AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:dur": "?u", "ex:mon": "?m"},
                ["bind", "?d", "(+ ?u ?m)"]
            ]),
            expected: None,
        },
        // A time carries no months.
        Case {
            name: "time + yearMonthDuration stays unbound",
            sparql: "SELECT ?d WHERE { ex:p1 ex:tod ?a . ex:p1 ex:mon ?m . \
                     BIND(?a + ?m AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:p1", "ex:tod": "?a", "ex:mon": "?m"},
                ["bind", "?d", "(+ ?a ?m)"]
            ]),
            expected: None,
        },
        // Mixed temporal kinds have no operator either.
        Case {
            name: "dateTime minus date stays unbound",
            sparql: "SELECT ?d WHERE { ex:e1 ex:end ?e . ex:d1 ex:to ?f . \
                     BIND(?e - ?f AS ?d) }",
            jsonld_where: json!([
                {"@id": "ex:e1", "ex:end": "?e"},
                {"@id": "ex:d1", "ex:to": "?f"},
                ["bind", "?d", "(- ?e ?f)"]
            ]),
            expected: None,
        },
    ]
}

async fn setup() -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let alias = "temporalsub:main".to_string();
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            DATA,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("insert");
    (dir, fluree, alias)
}

async fn run_sparql(fluree: &Fluree, alias: &str, sparql: &str) -> Value {
    let full = format!("{PREFIX}{sparql}");
    let snapshot = fluree.graph(alias).load().await.expect("load");
    snapshot
        .query()
        .sparql(&full)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("sparql {sparql}: {e}"))
}

async fn run_jsonld(fluree: &Fluree, alias: &str, where_clause: &Value) -> Value {
    let q = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "select": ["?d"],
        "where": where_clause,
    });
    let snapshot = fluree.graph(alias).load().await.expect("load");
    snapshot
        .query()
        .jsonld(&q)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("jsonld {where_clause}: {e}"))
}

/// Pull the single `?d` cell out of a one-column, one-row result. Returns the
/// raw JSON so both the lexical form and the datatype stay assertable;
/// `None` means the row came back with `?d` unbound.
fn single_cell(rows: &Value) -> Option<Value> {
    let arr = rows
        .as_array()
        .unwrap_or_else(|| panic!("expected rows array, got {rows}"));
    assert_eq!(arr.len(), 1, "expected exactly one row, got {rows}");
    let cell = match &arr[0] {
        Value::Array(cols) => {
            assert_eq!(cols.len(), 1, "expected one column, got {rows}");
            cols[0].clone()
        }
        other => other.clone(),
    };
    if cell.is_null() {
        None
    } else {
        Some(cell)
    }
}

/// Lexical form of a cell, whether the formatter renders it expanded
/// (`{"@value": …, "@type": …}`) or as a bare string.
fn lexical(cell: &Value) -> String {
    match cell {
        Value::Object(o) => o
            .get("@value")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("no @value in {cell}"))
            .to_string(),
        Value::String(s) => s.clone(),
        other => panic!("unexpected cell shape: {other}"),
    }
}

#[tokio::test]
async fn temporal_arithmetic_on_both_query_surfaces() {
    let (_dir, fluree, alias) = setup().await;

    let mut failures: Vec<String> = Vec::new();
    for case in cases() {
        let sparql = single_cell(&run_sparql(&fluree, &alias, case.sparql).await);
        let jsonld = single_cell(&run_jsonld(&fluree, &alias, &case.jsonld_where).await);

        match case.expected {
            Some(want) => {
                for (surface, got) in [("sparql", &sparql), ("jsonld", &jsonld)] {
                    match got {
                        None => failures.push(format!(
                            "{} [{surface}]: expected {want}, got unbound",
                            case.name
                        )),
                        Some(cell) => {
                            let lex = lexical(cell);
                            if lex != want {
                                failures.push(format!(
                                    "{} [{surface}]: expected {want}, got {lex}",
                                    case.name
                                ));
                            }
                        }
                    }
                }
            }
            None => {
                for (surface, got) in [("sparql", &sparql), ("jsonld", &jsonld)] {
                    if let Some(cell) = got {
                        failures.push(format!(
                            "{} [{surface}]: expected unbound, got {cell}",
                            case.name
                        ));
                    }
                }
            }
        }

        // SPARQL and JSON-LD share the IR; if they ever disagree the twin-test
        // convention has stopped meaning anything.
        if sparql != jsonld {
            failures.push(format!(
                "{}: surfaces disagree — sparql {sparql:?} vs jsonld {jsonld:?}",
                case.name
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn subtraction_result_carries_the_day_time_duration_datatype() {
    // The regression this pins is invisible to the lexical assertions above:
    // a duration has no `ComparableValue` variant of its own, so it leaves the
    // evaluator as a `TypedLiteral` with no datatype constraint — and that arm
    // of `to_binding` used to label every such value `xsd:string`.
    let (_dir, fluree, alias) = setup().await;

    let rows = run_sparql(
        &fluree,
        &alias,
        "SELECT ?d WHERE { ex:e1 ex:start ?s . ex:e1 ex:end ?e . BIND(?e - ?s AS ?d) }",
    )
    .await;
    let cell = single_cell(&rows).expect("?d must be bound");

    let obj = cell.as_object().unwrap_or_else(|| {
        panic!("duration must render as a typed literal, not a bare value: {cell}")
    });
    let ty = obj
        .get("@type")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("no @type on {cell}"));
    assert_eq!(
        ty, XSD_DAY_TIME_DURATION,
        "duration was re-typed on the way out: {cell}"
    );
    assert_eq!(lexical(&cell), "P1DT2H30M");
}
