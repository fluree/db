//! `xsd:double` special values: `INF`, `-INF` and `NaN`.
//!
//! They are values of `xsd:double` and `xsd:float` (XSD 1.1 Part 2 §3.3.4,
//! §3.3.5), so every write surface stores them, every index build encodes
//! them, and every read lane returns them. Queries read them with SPARQL's
//! value semantics, on both query surfaces:
//!
//! - Comparisons are XPath's (SPARQL 1.1 §17.3, F&O 3.1 §4.3): `INF` is above
//!   and `-INF` below every other number, and no `=`, `<` or `>` holds for a
//!   NaN, so `!=` does.
//! - A literal in a triple pattern matches by term (§18.3.1), so
//!   `"NaN"^^xsd:double` there finds a stored NaN, while `FILTER(?v = NaN)` is
//!   false for it.
//! - SUM and AVG add with `op:numeric-add` (§18.5.1.3, §18.5.1.4), so a NaN
//!   member makes them NaN and `INF + -INF` is NaN.
//! - DISTINCT and GROUP BY compare terms (§18.5, §18.5.1): NaN is one value.
//! - ORDER BY places NaN after `INF`. SPARQL leaves that position to the
//!   implementation (§15.1); MIN and MAX follow ORDER BY (§18.5.1.5,
//!   §18.5.1.6), so MAX is NaN when one is present.
//!
//! Spellings outside the XSD lexical space (`inf`, `Infinity`, `nan`) are not
//! `xsd:double` values: JSON-LD and SPARQL refuse them, and Turtle keeps them
//! as ill-typed literals.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState, ReindexOptions};
use serde_json::{json, Value};

const EX: &str = "http://example.org/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const PREFIXES: &str =
    "PREFIX ex: <http://example.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

fn context() -> Value {
    json!({"ex": EX, "xsd": XSD})
}

fn memory_fluree() -> Fluree {
    FlureeBuilder::memory().build_memory()
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

/// `ex:neg`, `ex:zero` and `ex:pos`: -1.5, 0 and 2.5.
async fn write_finite(fluree: &Fluree, ledger_id: &str) {
    let data = json!({"@context": context(), "@graph": [
        {"@id": "ex:neg", "ex:v": {"@value": "-1.5", "@type": "xsd:double"}},
        {"@id": "ex:zero", "ex:v": {"@value": "0", "@type": "xsd:double"}},
        {"@id": "ex:pos", "ex:v": {"@value": "2.5", "@type": "xsd:double"}}
    ]});
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&data)
        .commit()
        .await
        .expect("JSON-LD insert of finite values");
}

/// `ex:ninf`, `ex:inf`, `ex:nan` and `ex:nan2`, one write surface each, plus
/// `ex:big`, an overflowing numeral that XSD 1.1 maps to `INF`, when asked.
async fn write_special(fluree: &Fluree, ledger_id: &str, with_overflow: bool) {
    let jsonld = json!({"@context": context(), "@graph": [
        {"@id": "ex:ninf", "ex:v": {"@value": "-INF", "@type": "xsd:double"}},
        {"@id": "ex:inf", "ex:v": {"@value": "INF", "@type": "xsd:double"}}
    ]});
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&jsonld)
        .commit()
        .await
        .expect("JSON-LD insert of INF and -INF");
    let sparql = format!("{PREFIXES}INSERT DATA {{ ex:nan ex:v \"NaN\"^^xsd:double }}");
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&sparql)
        .commit()
        .await
        .expect("SPARQL insert of NaN");
    let turtle =
        format!("@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\nex:nan2 ex:v \"NaN\"^^xsd:double .");
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(&turtle)
        .commit()
        .await
        .expect("Turtle insert of NaN");
    if with_overflow {
        let big = json!({"@context": context(),
            "@id": "ex:big", "ex:w": {"@value": "1e400", "@type": "xsd:double"}});
        fluree
            .graph(ledger_id)
            .transact()
            .insert(&big)
            .commit()
            .await
            .expect("JSON-LD insert of an overflowing numeral");
    }
}

async fn reindexed(fluree: &Fluree, ledger_id: &str) -> LedgerState {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("full rebuild");
    fully_indexed(fluree, ledger_id).await
}

async fn incrementally_indexed(fluree: &Fluree, ledger_id: &str) -> LedgerState {
    support::build_and_publish_index(fluree, ledger_id).await;
    fully_indexed(fluree, ledger_id).await
}

async fn fully_indexed(fluree: &Fluree, ledger_id: &str) -> LedgerState {
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "reads must go through the binary index"
    );
    assert_eq!(
        ledger.snapshot.t,
        ledger.t(),
        "every commit must be indexed, so no value is served from novelty"
    );
    ledger
}

async fn over_an_index(fluree: &Fluree, ledger_id: &str) -> LedgerState {
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    assert!(ledger.snapshot.range_provider.is_some());
    assert!(
        ledger.snapshot.t < ledger.t(),
        "the special values must still be in novelty"
    );
    ledger
}

// ---------------------------------------------------------------------------
// Reading results as tokens
// ---------------------------------------------------------------------------

/// A double's token: `NaN`, `INF`, `-INF`, or its shortest decimal form.
fn number_token(lexical: &str) -> String {
    match lexical {
        "NaN" | "INF" | "-INF" => lexical.to_string(),
        _ => {
            let value: f64 = lexical
                .parse()
                .unwrap_or_else(|_| panic!("not a numeral: {lexical}"));
            format!("{value}")
        }
    }
}

/// The token of each solution's single projected variable: an IRI's local
/// name, a double's [`number_token`], or another literal's lexical form.
async fn sparql_tokens(fluree: &Fluree, ledger: &LedgerState, body: &str) -> Vec<String> {
    let query = format!("{PREFIXES}{body}");
    let out = support::query_sparql(fluree, ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("{body}: {e}"))
        .to_sparql_json(&ledger.snapshot)
        .expect("SPARQL JSON");
    let vars = out["head"]["vars"].as_array().expect("head.vars");
    assert_eq!(vars.len(), 1, "{body}: one projected variable");
    let var = vars[0].as_str().expect("var name");
    let double = format!("{XSD}double");
    out["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|solution| {
            let Some(term) = solution.get(var) else {
                return "unbound".to_string();
            };
            let lexical = term["value"].as_str().expect("value");
            if term["type"] == "uri" {
                lexical.trim_start_matches(EX).to_string()
            } else if term["datatype"].as_str() == Some(double.as_str()) {
                number_token(lexical)
            } else {
                lexical.to_string()
            }
        })
        .collect()
}

/// [`sparql_tokens`] for a JSON-LD query.
async fn jsonld_tokens(fluree: &Fluree, ledger: &LedgerState, query: &Value) -> Vec<String> {
    let mut query = query.clone();
    query["@context"] = context();
    let out = support::query_jsonld(fluree, ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .to_jsonld(&ledger.snapshot)
        .expect("JSON-LD");
    out.as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let cell = match row {
                Value::Array(cells) => {
                    assert_eq!(cells.len(), 1, "{query}: one selected value");
                    &cells[0]
                }
                cell => cell,
            };
            match cell {
                Value::Null => "unbound".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::String(s) => match s.as_str() {
                    "NaN" | "INF" | "-INF" => s.clone(),
                    s => s
                        .trim_start_matches("ex:")
                        .trim_start_matches(EX)
                        .to_string(),
                },
                Value::Number(n) => n
                    .as_i64()
                    .map(|i| i.to_string())
                    .unwrap_or_else(|| format!("{}", n.as_f64().expect("number"))),
                other => panic!("{query}: unexpected cell {other}"),
            }
        })
        .collect()
}

/// `(subject, token)` for every `ex:v` value, sorted.
async fn values_of(fluree: &Fluree, ledger: &LedgerState) -> Vec<(String, String)> {
    let subjects = sparql_tokens(
        fluree,
        ledger,
        "SELECT ?s WHERE { ?s ex:v ?v } ORDER BY ?s ?v",
    )
    .await;
    let values = sparql_tokens(
        fluree,
        ledger,
        "SELECT ?v WHERE { ?s ex:v ?v } ORDER BY ?s ?v",
    )
    .await;
    subjects.into_iter().zip(values).collect()
}

fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = expected
        .iter()
        .map(|(s, v)| (s.to_string(), v.to_string()))
        .collect();
    pairs.sort();
    pairs
}

const ALL_VALUES: &[(&str, &str)] = &[
    ("inf", "INF"),
    ("nan", "NaN"),
    ("nan2", "NaN"),
    ("neg", "-1.5"),
    ("ninf", "-INF"),
    ("pos", "2.5"),
    ("zero", "0"),
];

// ---------------------------------------------------------------------------
// Writing and indexing
// ---------------------------------------------------------------------------

/// Written through JSON-LD, SPARQL and Turtle, the special values read back as
/// themselves from novelty, from novelty over an index, and after an
/// incremental build and a full rebuild.
#[tokio::test]
async fn special_values_are_written_and_indexed() {
    let fluree = memory_fluree();
    let ledger_id = "special-doubles:write";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_finite(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert_eq!(values_of(&fluree, &ledger).await.len(), 3);

    reindexed(&fluree, ledger_id).await;
    write_special(&fluree, ledger_id, true).await;
    let expected = pairs(ALL_VALUES);
    let overlay = over_an_index(&fluree, ledger_id).await;
    assert_eq!(
        values_of(&fluree, &overlay).await,
        expected,
        "novelty over an index"
    );

    // XSD 1.1 maps a numeral beyond the double range to INF.
    let big = sparql_tokens(&fluree, &overlay, "SELECT ?w WHERE { ex:big ex:w ?w }").await;
    assert_eq!(big, ["INF"], "an overflowing numeral");

    let incremental = incrementally_indexed(&fluree, ledger_id).await;
    assert_eq!(
        values_of(&fluree, &incremental).await,
        expected,
        "incremental build"
    );

    let rebuilt = reindexed(&fluree, ledger_id).await;
    assert_eq!(values_of(&fluree, &rebuilt).await, expected, "full rebuild");
    let big = sparql_tokens(&fluree, &rebuilt, "SELECT ?w WHERE { ex:big ex:w ?w }").await;
    assert_eq!(big, ["INF"], "an overflowing numeral, full rebuild");

    // A ledger that was never indexed reads them from novelty.
    let novelty_id = "special-doubles:write-novelty";
    fluree.create_ledger(novelty_id).await.expect("create");
    write_finite(&fluree, novelty_id).await;
    write_special(&fluree, novelty_id, false).await;
    let novelty = fluree.ledger(novelty_id).await.expect("load");
    assert_eq!(values_of(&fluree, &novelty).await, expected, "novelty");
}

/// The special values of `xsd:double` and `xsd:float`, written in one
/// transaction among every other numeric type (integers, decimals, an integer
/// beyond i64 and a decimal beyond the double range), read back from novelty
/// and after a full rebuild.
#[tokio::test]
async fn special_values_commit_among_every_numeric_type() {
    let fluree = memory_fluree();
    let ledger_id = "special-doubles:numeric-mix";
    fluree.create_ledger(ledger_id).await.expect("create");
    let huge_decimal = format!("1{}.0", "0".repeat(390));
    let turtle = format!(
        "@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\n\
         ex:ninf ex:v \"-INF\"^^xsd:double .\n\
         ex:neg ex:v \"-2.5\"^^xsd:double .\n\
         ex:nzero ex:v \"-0.0\"^^xsd:double .\n\
         ex:zero ex:v \"0.0\"^^xsd:double .\n\
         ex:pos ex:v \"2.5\"^^xsd:double .\n\
         ex:max ex:v \"1.7976931348623157E308\"^^xsd:double .\n\
         ex:inf ex:v \"INF\"^^xsd:double .\n\
         ex:pinf ex:v \"+INF\"^^xsd:double .\n\
         ex:nan ex:v \"NaN\"^^xsd:double .\n\
         ex:nan2 ex:v \"NaN\"^^xsd:double .\n\
         ex:f_ninf ex:f \"-INF\"^^xsd:float .\n\
         ex:f_one ex:f \"1.5\"^^xsd:float .\n\
         ex:f_inf ex:f \"INF\"^^xsd:float .\n\
         ex:f_nan ex:f \"NaN\"^^xsd:float .\n\
         ex:m_ninf ex:m \"-INF\"^^xsd:double .\n\
         ex:m_dec ex:m -2.5 .\n\
         ex:m_im2 ex:m -2 .\n\
         ex:m_big ex:m 100000000000000000000000000 .\n\
         ex:m_dbig ex:m {huge_decimal} .\n\
         ex:m_inf ex:m \"INF\"^^xsd:double .\n\
         ex:m_nan ex:m \"NaN\"^^xsd:double .\n"
    );
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(&turtle)
        .commit()
        .await
        .expect("one Turtle insert of every numeric type");
    let count = "SELECT (COUNT(?o) AS ?c) WHERE { ?s ?p ?o FILTER(?p IN (ex:v, ex:f, ex:m)) }";
    let novelty = fluree.ledger(ledger_id).await.expect("load");
    assert_eq!(
        sparql_tokens(&fluree, &novelty, count).await,
        ["21"],
        "novelty"
    );
    let rebuilt = reindexed(&fluree, ledger_id).await;
    assert_eq!(
        sparql_tokens(&fluree, &rebuilt, count).await,
        ["21"],
        "full rebuild"
    );
}

/// Retracted special values: every lane reads only the values that remain.
#[tokio::test]
async fn retracted_special_values_read_back_on_every_lane() {
    let fluree = memory_fluree();
    let ledger_id = "special-doubles:retract";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_finite(&fluree, ledger_id).await;
    reindexed(&fluree, ledger_id).await;
    write_special(&fluree, ledger_id, false).await;

    let delete = format!(
        "{PREFIXES}DELETE DATA {{ ex:ninf ex:v \"-INF\"^^xsd:double . \
         ex:inf ex:v \"INF\"^^xsd:double . ex:nan ex:v \"NaN\"^^xsd:double . \
         ex:nan2 ex:v \"NaN\"^^xsd:double }}"
    );
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&delete)
        .commit()
        .await
        .expect("retract the special values");
    let finite = pairs(&[("neg", "-1.5"), ("pos", "2.5"), ("zero", "0")]);
    let overlay = over_an_index(&fluree, ledger_id).await;
    assert_eq!(
        values_of(&fluree, &overlay).await,
        finite,
        "novelty over an index"
    );

    let incremental = incrementally_indexed(&fluree, ledger_id).await;
    assert_eq!(
        values_of(&fluree, &incremental).await,
        finite,
        "incremental build"
    );
    let rebuilt = reindexed(&fluree, ledger_id).await;
    assert_eq!(values_of(&fluree, &rebuilt).await, finite, "full rebuild");
}

/// The bulk import path stores the special values as numbers, as every
/// transaction path does.
#[tokio::test]
async fn imported_special_values_stay_numeric() {
    let data = tempfile::TempDir::new().expect("tmp");
    let db = tempfile::TempDir::new().expect("tmp");
    let turtle = data.path().join("values.ttl");
    std::fs::write(
        &turtle,
        format!(
            "@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\n\
             ex:neg ex:v \"-1.5\"^^xsd:double .\n\
             ex:zero ex:v \"0\"^^xsd:double .\n\
             ex:pos ex:v \"2.5\"^^xsd:double .\n\
             ex:ninf ex:v \"-INF\"^^xsd:double .\n\
             ex:inf ex:v \"INF\"^^xsd:double .\n\
             ex:nan ex:v \"NaN\"^^xsd:double .\n\
             ex:nan2 ex:v \"NaN\"^^xsd:double .\n"
        ),
    )
    .expect("write Turtle");
    let fluree = FlureeBuilder::file(db.path().to_string_lossy().to_string())
        .build()
        .expect("file-backed Fluree");
    let ledger_id = "special-doubles:import";
    fluree
        .create(ledger_id)
        .import(&turtle)
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fully_indexed(&fluree, ledger_id).await;

    assert_eq!(values_of(&fluree, &ledger).await, pairs(ALL_VALUES));
    let mut numeric = sparql_tokens(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { ?s ex:v ?v FILTER(isNumeric(?v)) }",
    )
    .await;
    numeric.sort();
    assert_eq!(
        numeric,
        ["inf", "nan", "nan2", "neg", "ninf", "pos", "zero"],
        "every imported value is a number"
    );
}

// ---------------------------------------------------------------------------
// Value semantics
// ---------------------------------------------------------------------------

struct Case {
    what: &'static str,
    sparql: &'static str,
    jsonld: Value,
    expected: &'static [&'static str],
    /// Whether row order is part of the answer (ORDER BY).
    ordered: bool,
}

fn case(
    what: &'static str,
    sparql: &'static str,
    jsonld: Value,
    expected: &'static [&'static str],
) -> Case {
    Case {
        what,
        sparql,
        jsonld,
        expected,
        ordered: false,
    }
}

fn ordered(
    what: &'static str,
    sparql: &'static str,
    jsonld: Value,
    expected: &'static [&'static str],
) -> Case {
    Case {
        ordered: true,
        ..case(what, sparql, jsonld, expected)
    }
}

/// `?s` of every `ex:v` value that passes the JSON-LD filter `filter`.
fn subjects_where(filter: &str) -> Value {
    json!({
        "select": ["?s"],
        "where": [{"@id": "?s", "ex:v": "?v"}, ["filter", filter]]
    })
}

/// The JSON-LD aggregate `aggregate` over the `ex:v` values that pass `filter`.
fn aggregate_where(aggregate: &str, filter: Option<&str>) -> Value {
    let mut where_ = vec![json!({"@id": "?s", "ex:v": "?v"})];
    if let Some(filter) = filter {
        where_.push(json!(["filter", filter]));
    }
    json!({"select": [format!("(as {aggregate} ?out)")], "where": where_})
}

const NOT_NAN: &[&str] = &["inf", "neg", "ninf", "pos", "zero"];
const EVERY_SUBJECT: &[&str] = &["inf", "nan", "nan2", "neg", "ninf", "pos", "zero"];

fn cases() -> Vec<Case> {
    vec![
        case(
            "every value reads back as itself",
            "SELECT ?v WHERE { ?s ex:v ?v }",
            json!({"select": ["?v"], "where": {"@id": "?s", "ex:v": "?v"}}),
            &["-1.5", "-INF", "0", "2.5", "INF", "NaN", "NaN"],
        ),
        ordered(
            "ORDER BY: -INF first and NaN after INF; SPARQL leaves NaN's place to the \
             implementation (§15.1)",
            "SELECT ?v WHERE { ?s ex:v ?v } ORDER BY ?v",
            json!({"select": ["?v"], "where": {"@id": "?s", "ex:v": "?v"}, "orderBy": ["?v"]}),
            &["-INF", "-1.5", "0", "2.5", "INF", "NaN", "NaN"],
        ),
        ordered(
            "ORDER BY DESC",
            "SELECT ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v)",
            json!({"select": ["?v"], "where": {"@id": "?s", "ex:v": "?v"}, "orderBy": "(desc ?v)"}),
            &["NaN", "NaN", "INF", "2.5", "0", "-1.5", "-INF"],
        ),
        case(
            "> 0: INF is above every number and NaN satisfies no comparison \
             (§17.3, F&O §4.3.2)",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v > 0) }",
            subjects_where("(> ?v 0)"),
            &["inf", "pos"],
        ),
        case(
            "< 0: -INF is below every number",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v < 0) }",
            subjects_where("(< ?v 0)"),
            &["neg", "ninf"],
        ),
        case(
            ">= -INF holds for every number but NaN",
            r#"SELECT ?s WHERE { ?s ex:v ?v FILTER(?v >= "-INF"^^xsd:double) }"#,
            subjects_where("(>= ?v -INF)"),
            NOT_NAN,
        ),
        case(
            "<= INF holds for every number but NaN",
            r#"SELECT ?s WHERE { ?s ex:v ?v FILTER(?v <= "INF"^^xsd:double) }"#,
            subjects_where("(<= ?v INF)"),
            NOT_NAN,
        ),
        case(
            "a comparison with NaN is false, not an error, so its negation holds",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(!(?v < 0)) }",
            subjects_where("(not (< ?v 0))"),
            &["inf", "nan", "nan2", "pos", "zero"],
        ),
        case(
            "NaN is not equal to itself (F&O §4.3.1)",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v = ?v) }",
            subjects_where("(= ?v ?v)"),
            NOT_NAN,
        ),
        case(
            "so != holds between a NaN and itself (§17.3)",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v != ?v) }",
            subjects_where("(!= ?v ?v)"),
            &["nan", "nan2"],
        ),
        case(
            "= NaN holds for no value",
            r#"SELECT ?s WHERE { ?s ex:v ?v FILTER(?v = "NaN"^^xsd:double) }"#,
            subjects_where("(= ?v NaN)"),
            &[],
        ),
        case(
            "!= NaN holds for every value",
            r#"SELECT ?s WHERE { ?s ex:v ?v FILTER(?v != "NaN"^^xsd:double) }"#,
            subjects_where("(!= ?v NaN)"),
            EVERY_SUBJECT,
        ),
        case(
            "= INF",
            r#"SELECT ?s WHERE { ?s ex:v ?v FILTER(?v = "INF"^^xsd:double) }"#,
            subjects_where("(= ?v INF)"),
            &["inf"],
        ),
        case(
            "a NaN literal in a triple pattern matches a stored NaN: pattern matching is \
             by term (§18.3.1)",
            r#"SELECT ?s WHERE { ?s ex:v "NaN"^^xsd:double }"#,
            json!({"select": ["?s"], "where": {"@id": "?s",
                "ex:v": {"@value": "NaN", "@type": "xsd:double"}}}),
            &["nan", "nan2"],
        ),
        case(
            "an INF literal in a triple pattern",
            r#"SELECT ?s WHERE { ?s ex:v "INF"^^xsd:double }"#,
            json!({"select": ["?s"], "where": {"@id": "?s",
                "ex:v": {"@value": "INF", "@type": "xsd:double"}}}),
            &["inf"],
        ),
        case(
            "a -INF literal in a triple pattern",
            r#"SELECT ?s WHERE { ?s ex:v "-INF"^^xsd:double }"#,
            json!({"select": ["?s"], "where": {"@id": "?s",
                "ex:v": {"@value": "-INF", "@type": "xsd:double"}}}),
            &["ninf"],
        ),
        case(
            "sameTerm compares terms, and NaN has one lexical form (§17.4.1.8)",
            r#"SELECT ?s WHERE { ?s ex:v ?v FILTER(sameTerm(?v, "NaN"^^xsd:double)) }"#,
            subjects_where("(sameTerm ?v NaN)"),
            &["nan", "nan2"],
        ),
        case(
            "COUNT counts a NaN (§18.5.1.2)",
            "SELECT (COUNT(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(count ?v)", None),
            &["7"],
        ),
        case(
            "DISTINCT keeps one NaN: solutions are compared by term (§18.5)",
            "SELECT DISTINCT ?v WHERE { ?s ex:v ?v }",
            json!({"selectDistinct": ["?v"], "where": {"@id": "?s", "ex:v": "?v"}}),
            &["-1.5", "-INF", "0", "2.5", "INF", "NaN"],
        ),
        case(
            "COUNT(DISTINCT) counts NaN once",
            "SELECT (COUNT(DISTINCT ?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(count-distinct ?v)", None),
            &["6"],
        ),
        case(
            "GROUP BY puts the two NaNs in one group (§18.5.1)",
            "SELECT (COUNT(?s) AS ?out) WHERE { ?s ex:v ?v } GROUP BY ?v",
            json!({"select": ["(as (count ?s) ?out)"],
                "where": {"@id": "?s", "ex:v": "?v"}, "groupBy": ["?v"]}),
            &["1", "1", "1", "1", "1", "2"],
        ),
        case(
            "MIN is -INF (§18.5.1.5)",
            "SELECT (MIN(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(min ?v)", None),
            &["-INF"],
        ),
        case(
            "MAX follows ORDER BY, so it is NaN when one is present (§18.5.1.6)",
            "SELECT (MAX(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(max ?v)", None),
            &["NaN"],
        ),
        case(
            "MAX of the values that are not NaN is INF",
            "SELECT (MAX(?v) AS ?out) WHERE { ?s ex:v ?v FILTER(?v = ?v) }",
            aggregate_where("(max ?v)", Some("(= ?v ?v)")),
            &["INF"],
        ),
        case(
            "SUM with a NaN member is NaN: SUM is op:numeric-add (§18.5.1.3)",
            "SELECT (SUM(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(sum ?v)", None),
            &["NaN"],
        ),
        case(
            "SUM of INF and a finite number is INF",
            "SELECT (SUM(?v) AS ?out) WHERE { ?s ex:v ?v FILTER(?v > 0) }",
            aggregate_where("(sum ?v)", Some("(> ?v 0)")),
            &["INF"],
        ),
        case(
            "SUM of INF and -INF is NaN",
            r#"SELECT (SUM(?v) AS ?out) WHERE { ?s ex:v ?v
                 FILTER(?v = "INF"^^xsd:double || ?v = "-INF"^^xsd:double) }"#,
            aggregate_where("(sum ?v)", Some("(or (= ?v INF) (= ?v -INF))")),
            &["NaN"],
        ),
        case(
            "AVG with a NaN member is NaN (§18.5.1.4)",
            "SELECT (AVG(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(avg ?v)", None),
            &["NaN"],
        ),
        case(
            "AVG of INF and a finite number is INF",
            "SELECT (AVG(?v) AS ?out) WHERE { ?s ex:v ?v FILTER(?v > 0) }",
            aggregate_where("(avg ?v)", Some("(> ?v 0)")),
            &["INF"],
        ),
    ]
}

/// Every case on both query surfaces; returns the failures.
async fn check_cases(
    fluree: &Fluree,
    lane: &str,
    ledger: &LedgerState,
    cases: Vec<Case>,
) -> Vec<String> {
    let mut failures = Vec::new();
    for case in cases {
        let mut expected: Vec<String> = case.expected.iter().map(ToString::to_string).collect();
        let mut sparql = sparql_tokens(fluree, ledger, case.sparql).await;
        let mut jsonld = jsonld_tokens(fluree, ledger, &case.jsonld).await;
        if !case.ordered {
            expected.sort();
            sparql.sort();
            jsonld.sort();
        }
        if sparql != expected {
            failures.push(format!(
                "{lane}, SPARQL: {}\n  expected {expected:?}\n  got      {sparql:?}",
                case.what
            ));
        }
        if jsonld != expected {
            failures.push(format!(
                "{lane}, JSON-LD: {}\n  expected {expected:?}\n  got      {jsonld:?}",
                case.what
            ));
        }
    }
    failures
}

/// The semantics table, on each lane a value can be read from.
#[tokio::test]
async fn special_values_read_with_sparql_value_semantics() {
    let fluree = memory_fluree();
    let mut failures = Vec::new();

    let novelty_id = "special-doubles:semantics-novelty";
    fluree.create_ledger(novelty_id).await.expect("create");
    write_finite(&fluree, novelty_id).await;
    write_special(&fluree, novelty_id, false).await;
    let novelty = fluree.ledger(novelty_id).await.expect("load");
    failures.extend(check_cases(&fluree, "novelty", &novelty, cases()).await);

    let ledger_id = "special-doubles:semantics";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_finite(&fluree, ledger_id).await;
    reindexed(&fluree, ledger_id).await;
    write_special(&fluree, ledger_id, false).await;
    let overlay = over_an_index(&fluree, ledger_id).await;
    failures.extend(check_cases(&fluree, "novelty over an index", &overlay, cases()).await);

    let incremental = incrementally_indexed(&fluree, ledger_id).await;
    failures.extend(check_cases(&fluree, "incremental build", &incremental, cases()).await);

    let rebuilt = reindexed(&fluree, ledger_id).await;
    failures.extend(check_cases(&fluree, "full rebuild", &rebuilt, cases()).await);

    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

// ---------------------------------------------------------------------------
// One value, whatever its bits
// ---------------------------------------------------------------------------

/// `ex:nzero` holds `-0.0`; `ex:wnan` and `ex:wzero` hold NaN and `0.0` on
/// `ex:w`; `ex:qnan` holds the negation of `ex:nan`'s NaN, a NaN with the sign
/// bit set.
async fn write_zero_and_negated_nan(fluree: &Fluree, ledger_id: &str) {
    let data = json!({"@context": context(), "@graph": [
        {"@id": "ex:nzero", "ex:v": {"@value": "-0.0", "@type": "xsd:double"}},
        {"@id": "ex:wnan", "ex:w": {"@value": "NaN", "@type": "xsd:double"}},
        {"@id": "ex:wzero", "ex:w": {"@value": "0.0", "@type": "xsd:double"}}
    ]});
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&data)
        .commit()
        .await
        .expect("JSON-LD insert of -0.0, NaN and 0.0");
    let negated = format!(
        "{PREFIXES}INSERT {{ ex:qnan ex:v ?x }} WHERE {{ ex:nan ex:v ?v BIND(-?v AS ?x) }}"
    );
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&negated)
        .commit()
        .await
        .expect("insert a negated NaN");
}

/// A JSON-LD subquery selecting `vars` from `where_`.
fn subquery(vars: &[&str], where_: Value) -> Value {
    json!(["query", {"@context": context(), "select": vars, "where": where_}])
}

fn cases_one_value() -> Vec<Case> {
    let nan_on = |p: &str, var: &str| {
        json!([{"@id": format!("?{}", if p == "ex:v" { "s" } else { "t" }), p: format!("?{var}")},
               ["filter", format!("(!= ?{var} ?{var})")]])
    };
    vec![
        case(
            "GROUP BY: every NaN is one group, whatever its sign (§18.5.1)",
            "SELECT (COUNT(?s) AS ?c) WHERE { ?s ex:v ?v FILTER(?v != ?v) } GROUP BY ?v",
            json!({"select": ["(as (count ?s) ?c)"], "where": nan_on("ex:v", "v"),
                   "groupBy": "?v"}),
            &["3"],
        ),
        case(
            "COUNT(DISTINCT): every NaN is one value",
            "SELECT (COUNT(DISTINCT ?v) AS ?c) WHERE { ?s ex:v ?v FILTER(?v != ?v) }",
            aggregate_where("(count-distinct ?v)", Some("(!= ?v ?v)")),
            &["1"],
        ),
        case(
            "GROUP BY: -0.0 is 0",
            "SELECT (COUNT(?s) AS ?c) WHERE { ?s ex:v ?v FILTER(?v = 0) } GROUP BY ?v",
            json!({"select": ["(as (count ?s) ?c)"],
                   "where": [{"@id": "?s", "ex:v": "?v"}, ["filter", "(= ?v 0)"]],
                   "groupBy": "?v"}),
            &["2"],
        ),
        case(
            "COUNT(DISTINCT): -0.0 is 0",
            "SELECT (COUNT(DISTINCT ?v) AS ?c) WHERE { ?s ex:v ?v FILTER(?v = 0) }",
            aggregate_where("(count-distinct ?v)", Some("(= ?v 0)")),
            &["1"],
        ),
        case(
            "GROUP BY: a computed negated NaN groups with NaN",
            "SELECT (COUNT(?s) AS ?c) WHERE {
               { ?s ex:v ?v FILTER(?v != ?v) BIND(?v AS ?x) }
               UNION { ?s ex:v ?v FILTER(?v != ?v) BIND(-?v AS ?x) } } GROUP BY ?x",
            json!({"select": ["(as (count ?s) ?c)"],
                   "where": [["union",
                       [{"@id": "?s", "ex:v": "?v"}, ["filter", "(!= ?v ?v)"], ["bind", "?x", "?v"]],
                       [{"@id": "?s", "ex:v": "?v"}, ["filter", "(!= ?v ?v)"], ["bind", "?x", "(- ?v)"]]]],
                   "groupBy": "?x"}),
            &["6"],
        ),
        case(
            "join: a stored negated NaN joins NaN",
            "SELECT ?s WHERE {
               { SELECT ?s ?x WHERE { ?s ex:v ?x FILTER(?x != ?x) } }
               { SELECT ?t ?x WHERE { ?t ex:w ?x FILTER(?x != ?x) } } }",
            json!({"select": ["?s"], "where": [
                subquery(&["?s", "?x"], nan_on("ex:v", "x")),
                subquery(&["?t", "?x"], nan_on("ex:w", "x"))]}),
            &["nan", "nan2", "qnan"],
        ),
        case(
            "join: -0.0 joins 0",
            "SELECT ?s WHERE {
               { SELECT ?s ?x WHERE { ?s ex:v ?x FILTER(?x = 0) } }
               { SELECT ?t ?x WHERE { ?t ex:w ?x FILTER(?x = 0) } } }",
            json!({"select": ["?s"], "where": [
                subquery(&["?s", "?x"], json!([{"@id": "?s", "ex:v": "?x"}, ["filter", "(= ?x 0)"]])),
                subquery(&["?t", "?x"], json!([{"@id": "?t", "ex:w": "?x"}, ["filter", "(= ?x 0)"]]))]}),
            &["nzero", "zero"],
        ),
        case(
            "join: a computed negated NaN joins NaN",
            "SELECT ?s WHERE {
               { SELECT ?s ?x WHERE { ?s ex:v ?v FILTER(?v != ?v) BIND(-?v AS ?x) } }
               { SELECT ?t ?x WHERE { ?t ex:w ?x FILTER(?x != ?x) } } }",
            json!({"select": ["?s"], "where": [
                subquery(&["?s", "?x"], json!([{"@id": "?s", "ex:v": "?v"},
                    ["filter", "(!= ?v ?v)"], ["bind", "?x", "(- ?v)"]])),
                subquery(&["?t", "?x"], nan_on("ex:w", "x"))]}),
            &["nan", "nan2", "qnan"],
        ),
    ]
}

/// GROUP BY, COUNT(DISTINCT) and hash joins identify a double by its value: a NaN
/// with any sign or payload is the one NaN, and -0.0 is 0. Each lane gives the
/// same answer, on both query surfaces.
#[tokio::test]
async fn special_values_group_and_join_as_one_value() {
    let fluree = memory_fluree();
    let mut failures = Vec::new();

    let novelty_id = "special-doubles:one-value-novelty";
    fluree.create_ledger(novelty_id).await.expect("create");
    write_finite(&fluree, novelty_id).await;
    write_special(&fluree, novelty_id, false).await;
    write_zero_and_negated_nan(&fluree, novelty_id).await;
    let novelty = fluree.ledger(novelty_id).await.expect("load");
    failures.extend(check_cases(&fluree, "novelty", &novelty, cases_one_value()).await);

    let ledger_id = "special-doubles:one-value";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_finite(&fluree, ledger_id).await;
    write_special(&fluree, ledger_id, false).await;
    reindexed(&fluree, ledger_id).await;
    write_zero_and_negated_nan(&fluree, ledger_id).await;
    let overlay = over_an_index(&fluree, ledger_id).await;
    failures.extend(
        check_cases(
            &fluree,
            "novelty over an index",
            &overlay,
            cases_one_value(),
        )
        .await,
    );
    let incremental = incrementally_indexed(&fluree, ledger_id).await;
    failures.extend(
        check_cases(
            &fluree,
            "incremental build",
            &incremental,
            cases_one_value(),
        )
        .await,
    );
    let rebuilt = reindexed(&fluree, ledger_id).await;
    failures.extend(check_cases(&fluree, "full rebuild", &rebuilt, cases_one_value()).await);

    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

// ---------------------------------------------------------------------------
// Among integers beyond i64
// ---------------------------------------------------------------------------

/// An `xsd:integer` beyond `i64`, which the index keeps apart from doubles.
const BIG: &str = "99999999999999999999";

/// `ex:nbig`, `ex:big` and `ex:big2`: `-BIG`, `BIG` and `BIG`.
async fn write_big_integers(fluree: &Fluree, ledger_id: &str) {
    let data = json!({"@context": context(), "@graph": [
        {"@id": "ex:nbig", "ex:v": {"@value": format!("-{BIG}"), "@type": "xsd:integer"}},
        {"@id": "ex:big", "ex:v": {"@value": BIG, "@type": "xsd:integer"}},
        {"@id": "ex:big2", "ex:v": {"@value": BIG, "@type": "xsd:integer"}}
    ]});
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&data)
        .commit()
        .await
        .expect("JSON-LD insert of integers beyond i64");
}

fn cases_with_big_integers() -> Vec<Case> {
    vec![
        ordered(
            "ORDER BY: -INF below and INF above the integers beyond i64, NaN last",
            "SELECT ?s WHERE { ?s ex:v ?v } ORDER BY ?v ?s",
            json!({"select": ["?s"], "where": {"@id": "?s", "ex:v": "?v"},
                   "orderBy": ["?v", "?s"]}),
            &[
                "ninf", "nbig", "neg", "zero", "pos", "big", "big2", "inf", "nan", "nan2",
            ],
        ),
        ordered(
            "ORDER BY DESC",
            "SELECT ?s WHERE { ?s ex:v ?v } ORDER BY DESC(?v) ?s",
            json!({"select": ["?s"], "where": {"@id": "?s", "ex:v": "?v"},
                   "orderBy": ["(desc ?v)", "?s"]}),
            &[
                "nan", "nan2", "inf", "big", "big2", "pos", "zero", "neg", "nbig", "ninf",
            ],
        ),
        case(
            "> 1e19: the integers above it and INF, not NaN",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v > 1e19) }",
            subjects_where("(> ?v 1e19)"),
            &["big", "big2", "inf"],
        ),
        case(
            "< -1e19: the integer below it and -INF",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v < -1e19) }",
            subjects_where("(< ?v -1e19)"),
            &["nbig", "ninf"],
        ),
        case(
            "COUNT(DISTINCT): one NaN and one BIG",
            "SELECT (COUNT(DISTINCT ?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(count-distinct ?v)", None),
            &["8"],
        ),
        case(
            "MIN is -INF",
            "SELECT (MIN(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(min ?v)", None),
            &["-INF"],
        ),
        case(
            "MAX follows ORDER BY: NaN",
            "SELECT (MAX(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(max ?v)", None),
            &["NaN"],
        ),
        case(
            "MAX without NaN is INF",
            "SELECT (MAX(?v) AS ?out) WHERE { ?s ex:v ?v FILTER(?v = ?v) }",
            aggregate_where("(max ?v)", Some("(= ?v ?v)")),
            &["INF"],
        ),
    ]
}

/// The datatype of each value, by subject: `xsd:integer` for the integers
/// beyond i64 and `xsd:double` for the rest.
async fn datatypes_of(fluree: &Fluree, ledger: &LedgerState) -> Vec<String> {
    sparql_tokens(
        fluree,
        ledger,
        "SELECT ?d WHERE { ?s ex:v ?v BIND(DATATYPE(?v) AS ?d) } ORDER BY ?s",
    )
    .await
    .into_iter()
    .map(|iri| iri.trim_start_matches(XSD).to_string())
    .collect()
}

/// Integers beyond `i64`, which the index stores apart from doubles, order,
/// compare and keep their datatype among the special values on one property.
#[tokio::test]
async fn special_values_order_among_integers_beyond_i64() {
    let fluree = memory_fluree();
    let mut failures = Vec::new();
    // By subject: big, big2, inf, nan, nan2, nbig, neg, ninf, pos, zero.
    let datatypes = [
        "integer", "integer", "double", "double", "double", "integer", "double", "double",
        "double", "double",
    ];

    let novelty_id = "special-doubles:big-novelty";
    fluree.create_ledger(novelty_id).await.expect("create");
    write_finite(&fluree, novelty_id).await;
    write_big_integers(&fluree, novelty_id).await;
    write_special(&fluree, novelty_id, false).await;
    let novelty = fluree.ledger(novelty_id).await.expect("load");
    failures.extend(check_cases(&fluree, "novelty", &novelty, cases_with_big_integers()).await);
    assert_eq!(datatypes_of(&fluree, &novelty).await, datatypes, "novelty");

    let ledger_id = "special-doubles:big";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_finite(&fluree, ledger_id).await;
    write_big_integers(&fluree, ledger_id).await;
    reindexed(&fluree, ledger_id).await;
    write_special(&fluree, ledger_id, false).await;
    let overlay = over_an_index(&fluree, ledger_id).await;
    failures.extend(
        check_cases(
            &fluree,
            "novelty over an index",
            &overlay,
            cases_with_big_integers(),
        )
        .await,
    );
    assert_eq!(
        datatypes_of(&fluree, &overlay).await,
        datatypes,
        "novelty over an index"
    );

    let incremental = incrementally_indexed(&fluree, ledger_id).await;
    failures.extend(
        check_cases(
            &fluree,
            "incremental build",
            &incremental,
            cases_with_big_integers(),
        )
        .await,
    );
    assert_eq!(
        datatypes_of(&fluree, &incremental).await,
        datatypes,
        "incremental build"
    );

    let rebuilt = reindexed(&fluree, ledger_id).await;
    failures
        .extend(check_cases(&fluree, "full rebuild", &rebuilt, cases_with_big_integers()).await);
    assert_eq!(
        datatypes_of(&fluree, &rebuilt).await,
        datatypes,
        "full rebuild"
    );

    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// A WHERE clause binds the stored special values once they are indexed, so
/// `INSERT … USING` copies them out of a named graph and `DELETE … WHERE`
/// retracts them, from the default graph and with `WITH` from a named graph.
/// Each graph holds an integer beyond i64 on the same property, which the
/// index keeps per graph, so a WHERE scoped to `ex:g` must read `ex:g`'s.
#[tokio::test]
async fn special_values_copy_and_retract_through_a_where_clause() {
    let fluree = memory_fluree();
    let ledger_id = "special-doubles:where";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_finite(&fluree, ledger_id).await;
    write_special(&fluree, ledger_id, false).await;
    let more = format!(
        "{PREFIXES}INSERT DATA {{ ex:dbig ex:v \"-{BIG}\"^^xsd:integer . GRAPH ex:g {{ \
         ex:gnan ex:v \"NaN\"^^xsd:double . ex:ginf ex:v \"INF\"^^xsd:double . \
         ex:gbig ex:v \"{BIG}\"^^xsd:integer }} }}"
    );
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&more)
        .commit()
        .await
        .expect("insert into the default and a named graph");
    let indexed = reindexed(&fluree, ledger_id).await;
    let in_g = "SELECT ?s WHERE { GRAPH ex:g { ?s ex:v ?v } } ORDER BY ?s";
    assert_eq!(
        sparql_tokens(&fluree, &indexed, in_g).await,
        ["gbig", "ginf", "gnan"]
    );

    let copy = format!("{PREFIXES}INSERT {{ ?s ex:copy ?v }} USING ex:g WHERE {{ ?s ex:v ?v }}");
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&copy)
        .commit()
        .await
        .expect("INSERT … USING a named graph");
    // `?v != ?v` holds only for NaN.
    let delete = format!(
        "{PREFIXES}DELETE {{ ?s ex:v ?v }} WHERE {{ ?s ex:v ?v \
         FILTER(?v != ?v || ?v > 1e300 || ?v < -1e300) }}"
    );
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&delete)
        .commit()
        .await
        .expect("DELETE … WHERE in the default graph");
    let delete_g = format!("{PREFIXES}WITH ex:g DELETE {{ ?s ex:v ?v }} WHERE {{ ?s ex:v ?v }}");
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&delete_g)
        .commit()
        .await
        .expect("DELETE … WHERE in a named graph");

    let remaining = pairs(&[
        ("dbig", &format!("-{BIG}")),
        ("neg", "-1.5"),
        ("pos", "2.5"),
        ("zero", "0"),
    ]);
    let copies = "SELECT ?c WHERE { ?s ex:copy ?c } ORDER BY ?c";
    let overlay = over_an_index(&fluree, ledger_id).await;
    assert_eq!(
        values_of(&fluree, &overlay).await,
        remaining,
        "novelty over an index"
    );
    assert!(
        sparql_tokens(&fluree, &overlay, in_g).await.is_empty(),
        "named graph, novelty over an index"
    );
    assert_eq!(
        sparql_tokens(&fluree, &overlay, copies).await,
        [BIG, "INF", "NaN"],
        "copies, novelty over an index"
    );
    let rebuilt = reindexed(&fluree, ledger_id).await;
    assert_eq!(
        values_of(&fluree, &rebuilt).await,
        remaining,
        "full rebuild"
    );
    assert!(
        sparql_tokens(&fluree, &rebuilt, in_g).await.is_empty(),
        "named graph, full rebuild"
    );
    assert_eq!(
        sparql_tokens(&fluree, &rebuilt, copies).await,
        [BIG, "INF", "NaN"],
        "copies, full rebuild"
    );
}

// ---------------------------------------------------------------------------
// Indexes earlier versions built
// ---------------------------------------------------------------------------

/// Copy `from` into `to`, recursively.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

fn cases_bulk_import_text() -> Vec<Case> {
    let over = |subjects: &[&str], aggregate: &str| {
        let ids: Vec<Value> = subjects.iter().map(|s| json!({"@id": s})).collect();
        json!({"select": [format!("(as ({aggregate} ?v) ?out)")],
               "where": [["values", ["?s", ids]], {"@id": "?s", "ex:v": "?v"}]})
    };
    vec![
        case(
            "SUM reads `inf` text as INF",
            "SELECT (SUM(?v) AS ?out) WHERE { VALUES ?s { ex:a ex:inf } ?s ex:v ?v }",
            over(&["ex:a", "ex:inf"], "sum"),
            &["INF"],
        ),
        case(
            "SUM reads `-inf` text as -INF",
            "SELECT (SUM(?v) AS ?out) WHERE { VALUES ?s { ex:a ex:ninf } ?s ex:v ?v }",
            over(&["ex:a", "ex:ninf"], "sum"),
            &["-INF"],
        ),
        case(
            "AVG reads `inf` text as INF",
            "SELECT (AVG(?v) AS ?out) WHERE { VALUES ?s { ex:a ex:b ex:inf } ?s ex:v ?v }",
            over(&["ex:a", "ex:b", "ex:inf"], "avg"),
            &["INF"],
        ),
        case(
            "a comparison reads `inf` text under xsd:float as INF",
            "SELECT ?g WHERE { ?s ex:f ?v BIND(?v > 1 AS ?g) }",
            json!({"select": ["?g"], "where": [{"@id": "?s", "ex:f": "?v"}, ["bind", "?g", "(> ?v 1)"]]}),
            &["true"],
        ),
    ]
}

/// `ex:a` 1.5, plus `inf` / `-inf` text under `xsd:double` (`ex:t`, `ex:n`)
/// and `xsd:float` (`ex:ft`), written as Turtle, which keeps them as text.
async fn write_inf_text(fluree: &Fluree, ledger_id: &str) {
    let turtle = format!(
        "@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\n\
         ex:a ex:v \"1.5\"^^xsd:double .\n\
         ex:b ex:v \"-2.5\"^^xsd:double .\n\
         ex:t ex:v \"inf\"^^xsd:double .\n\
         ex:n ex:v \"-inf\"^^xsd:double .\n\
         ex:ft ex:f \"inf\"^^xsd:float .\n"
    );
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(&turtle)
        .commit()
        .await
        .expect("Turtle insert of inf text");
}

fn cases_inf_text() -> Vec<Case> {
    let over = |subjects: &[&str], aggregate: &str| {
        let ids: Vec<Value> = subjects.iter().map(|s| json!({"@id": s})).collect();
        json!({"select": [format!("(as ({aggregate} ?v) ?out)")],
               "where": [["values", ["?s", ids]], {"@id": "?s", "ex:v": "?v"}]})
    };
    vec![
        case(
            "SUM reads `inf` text as INF",
            "SELECT (SUM(?v) AS ?out) WHERE { VALUES ?s { ex:a ex:t } ?s ex:v ?v }",
            over(&["ex:a", "ex:t"], "sum"),
            &["INF"],
        ),
        case(
            "SUM reads `-inf` text as -INF",
            "SELECT (SUM(?v) AS ?out) WHERE { VALUES ?s { ex:a ex:n } ?s ex:v ?v }",
            over(&["ex:a", "ex:n"], "sum"),
            &["-INF"],
        ),
        case(
            "AVG reads `inf` text as INF",
            "SELECT (AVG(?v) AS ?out) WHERE { VALUES ?s { ex:a ex:b ex:t } ?s ex:v ?v }",
            over(&["ex:a", "ex:b", "ex:t"], "avg"),
            &["INF"],
        ),
        case(
            "a comparison reads `inf` text under xsd:float as INF",
            "SELECT ?g WHERE { ?s ex:f ?v BIND(?v > 1 AS ?g) }",
            json!({"select": ["?g"], "where": [{"@id": "?s", "ex:f": "?v"}, ["bind", "?g", "(> ?v 1)"]]}),
            &["true"],
        ),
        case(
            "arithmetic reads `-inf` text as -INF",
            "SELECT ?x WHERE { ex:n ex:v ?v BIND(?v + 1 AS ?x) }",
            json!({"select": ["?x"], "where": [{"@id": "ex:n", "ex:v": "?v"}, ["bind", "?x", "(+ ?v 1)"]]}),
            &["-INF"],
        ),
        case(
            "isNumeric: the text is not a number",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(isNumeric(?v)) }",
            json!({"select": ["?s"], "where": [{"@id": "?s", "ex:v": "?v"}, ["filter", "(is-numeric ?v)"]]}),
            &["a", "b"],
        ),
        case(
            "a range FILTER compares the text as text",
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v > 1) }",
            json!({"select": ["?s"], "where": [{"@id": "?s", "ex:v": "?v"}, ["filter", "(> ?v 1)"]]}),
            &["a"],
        ),
        ordered(
            "ORDER BY sorts the text after the numbers",
            "SELECT ?s WHERE { ?s ex:v ?v } ORDER BY ?v ?s",
            json!({"select": ["?s"], "where": {"@id": "?s", "ex:v": "?v"}, "orderBy": ["?v", "?s"]}),
            &["b", "a", "n", "t"],
        ),
        case(
            "MAX: the text sorts above the numbers",
            "SELECT (MAX(?v) AS ?out) WHERE { ?s ex:v ?v }",
            aggregate_where("(max ?v)", None),
            &["inf"],
        ),
    ]
}

/// Text `inf` / `-inf` under `xsd:double` or `xsd:float` reads as INF / -INF
/// in arithmetic, expression comparisons, SUM and AVG, from novelty and once
/// indexed: the shape an earlier bulk import left in the index. It stays text
/// to `isNumeric`, ORDER BY, MAX and range filters.
#[tokio::test]
async fn inf_text_reads_as_infinity_in_arithmetic_and_aggregates() {
    let fluree = memory_fluree();
    let mut failures = Vec::new();
    let ledger_id = "special-doubles:inf-text";
    fluree.create_ledger(ledger_id).await.expect("create");
    write_inf_text(&fluree, ledger_id).await;
    let novelty = fluree.ledger(ledger_id).await.expect("load");
    failures.extend(check_cases(&fluree, "novelty", &novelty, cases_inf_text()).await);
    let rebuilt = reindexed(&fluree, ledger_id).await;
    failures.extend(check_cases(&fluree, "indexed", &rebuilt, cases_inf_text()).await);
    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// Bulk import in earlier versions stored `xsd:double` and `xsd:float` `INF`
/// and `-INF` in the index as the text `inf` and `-inf`. The fixture is such
/// an index, written by v4.2.3's bulk import from `source.ttl` beside it. It
/// reads as before: the values come back as stored, and SUM, AVG, arithmetic
/// and comparisons take the text as the number it stands for. The expected
/// answers are v4.2.3's own on this index.
#[tokio::test]
async fn values_a_previous_bulk_import_stored_as_text_read_as_before() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/bulk-import-4.2.3/storage");
    let db = tempfile::TempDir::new().expect("tmp");
    copy_dir(&fixture, db.path());
    let fluree = FlureeBuilder::file(db.path().to_string_lossy().to_string())
        .build()
        .expect("file-backed Fluree");
    let ledger = fluree.ledger("ex:main").await.expect("load the fixture");
    assert_eq!(
        ledger.snapshot.t,
        ledger.t(),
        "the fixture is fully indexed"
    );

    let mut failures = check_cases(
        &fluree,
        "v4.2.3 bulk import",
        &ledger,
        cases_bulk_import_text(),
    )
    .await;
    for (query, expected) in [
        (
            "SELECT ?v WHERE { ?s ex:v ?v } ORDER BY ?s",
            &["1.5", "-2.5", "inf", "NaN", "-inf"][..],
        ),
        (
            "SELECT (isNumeric(?v) AS ?n) WHERE { ?s ex:v ?v } ORDER BY ?s",
            &["true", "true", "false", "false", "false"],
        ),
        (
            "SELECT (SUM(?v) AS ?x) WHERE { ?s ex:v ?v FILTER(?s != ex:nan) }",
            &["NaN"],
        ),
        (
            "SELECT (AVG(?v) AS ?x) WHERE { ?s ex:v ?v FILTER(?s != ex:nan && ?s != ex:ninf) }",
            &["INF"],
        ),
        ("SELECT (MIN(?v) AS ?x) WHERE { ?s ex:v ?v }", &["-2.5"]),
        ("SELECT (MAX(?v) AS ?x) WHERE { ?s ex:v ?v }", &["inf"]),
        (
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v > 1) } ORDER BY ?s",
            &["a"],
        ),
        ("SELECT ?s WHERE { ?s ex:f ?v FILTER(?v > 1) }", &[]),
        (
            "SELECT ?s WHERE { ?s ex:v ?v FILTER(?v < 0) } ORDER BY ?s",
            &["b"],
        ),
        (
            "SELECT ?s WHERE { ?s ex:v ?v } ORDER BY ?v ?s",
            &["b", "a", "ninf", "nan", "inf"],
        ),
        (r#"SELECT ?s WHERE { ?s ex:v "INF"^^xsd:double }"#, &[]),
    ] {
        let got = sparql_tokens(&fluree, &ledger, query).await;
        if got != expected {
            failures.push(format!(
                "SPARQL: {query}\n  expected {expected:?}\n  got      {got:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

// ---------------------------------------------------------------------------
// Lexical forms
// ---------------------------------------------------------------------------

/// Spellings outside the XSD lexical space are not `xsd:double` or
/// `xsd:float` values. JSON-LD and SPARQL refuse them as ill-typed literals;
/// Turtle keeps them as ill-typed literals, which are not numbers.
#[tokio::test]
async fn non_xsd_spellings_are_not_doubles() {
    let fluree = memory_fluree();
    let ledger_id = "special-doubles:lexical";
    fluree.create_ledger(ledger_id).await.expect("create");

    for dt in ["double", "float"] {
        for lexical in ["inf", "-inf", "Infinity", "-Infinity", "nan", "NAN"] {
            // Both surfaces refuse it with the same parse error.
            let refusal = format!("Parse error: Cannot parse '{lexical}' as xsd:{dt}");
            let data = json!({"@context": context(), "@id": "ex:s",
                "ex:v": {"@value": lexical, "@type": format!("xsd:{dt}")}});
            let err = fluree
                .graph(ledger_id)
                .transact()
                .insert(&data)
                .commit()
                .await
                .expect_err("JSON-LD insert must refuse the spelling");
            assert!(err.to_string().contains(&refusal), "JSON-LD insert: {err}");
            let err = fluree
                .graph(ledger_id)
                .transact()
                .upsert(&data)
                .commit()
                .await
                .expect_err("JSON-LD upsert must refuse the spelling");
            assert!(err.to_string().contains(&refusal), "JSON-LD upsert: {err}");

            let sparql = format!("{PREFIXES}INSERT DATA {{ ex:s ex:v \"{lexical}\"^^xsd:{dt} }}");
            let err = fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(&sparql)
                .commit()
                .await
                .expect_err("SPARQL INSERT DATA must refuse the spelling");
            assert!(err.to_string().contains(&refusal), "SPARQL: {err}");
        }
    }

    // A query constant with the spelling is refused like any ill-typed one.
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let query = format!("{PREFIXES}SELECT ?s WHERE {{ ?s ex:v \"inf\"^^xsd:double }}");
    assert!(support::query_sparql(&fluree, &ledger, &query)
        .await
        .is_err());

    // The XSD spellings, and an overflowing numeral, are numbers.
    for (lexical, token) in [
        ("INF", "INF"),
        ("+INF", "INF"),
        ("-INF", "-INF"),
        ("NaN", "NaN"),
        ("-1e400", "-INF"),
    ] {
        let data = json!({"@context": context(), "@id": "ex:ok",
            "ex:v": {"@value": lexical, "@type": "xsd:double"}});
        fluree
            .graph(ledger_id)
            .transact()
            .upsert(&data)
            .commit()
            .await
            .unwrap_or_else(|e| panic!("{lexical}: {e}"));
        let ledger = fluree.ledger(ledger_id).await.expect("load");
        let read = sparql_tokens(&fluree, &ledger, "SELECT ?v WHERE { ex:ok ex:v ?v }").await;
        assert_eq!(read, [token], "{lexical}");
    }

    // Turtle keeps the other spellings as ill-typed literals.
    let turtle = format!(
        "@prefix ex: <{EX}> .\n@prefix xsd: <{XSD}> .\n\
         ex:t1 ex:v \"inf\"^^xsd:double .\nex:t2 ex:v \"nan\"^^xsd:float ."
    );
    fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(&turtle)
        .commit()
        .await
        .expect("Turtle keeps ill-typed literals");
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let kept = sparql_tokens(
        &fluree,
        &ledger,
        "SELECT ?l WHERE { ?s ex:v ?v FILTER(?s IN (ex:t1, ex:t2) && !isNumeric(?v)) \
         BIND(CONCAT(STR(?v), \"^^\", STR(DATATYPE(?v))) AS ?l) }",
    )
    .await;
    let mut kept = kept;
    kept.sort();
    assert_eq!(
        kept,
        [format!("inf^^{XSD}double"), format!("nan^^{XSD}float")]
    );

    // A cast reads the XSD lexical space too (F&O 3.1 §19.2).
    let casts = sparql_tokens(
        &fluree,
        &ledger,
        r#"SELECT ?d WHERE { VALUES ?l { "INF" "-INF" "NaN" "inf" "Infinity" "nan" }
             BIND(xsd:double(?l) AS ?d) }"#,
    )
    .await;
    let mut casts = casts;
    casts.sort();
    assert_eq!(
        casts,
        ["-INF", "INF", "NaN", "unbound", "unbound", "unbound"]
    );
}
