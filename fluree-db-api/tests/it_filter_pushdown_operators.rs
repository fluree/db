//! A filter over a function of the object answers the same over the index as
//! over novelty, whatever the operator.
//!
//! The binary scan compiles some filters into checks on the encoded object
//! (`compile_encoded_pre_filters_and_prune_inline_ops`). `LANG(?o) = "en"`
//! becomes a check on the object's language type, but the rewrite matched the
//! shape `f(LANG(?o), "en")` without checking `f`, so over an indexed ledger
//! `LANG(?v) != "en"` kept only the `en` rows (the complement of its answer)
//! and `langMatches(LANG(?v), "en")` and `STRSTARTS(LANG(?v), "en")` lost
//! `en-gb`. Novelty never reached the rewrite, so the W3C suite, which loads
//! its data by transaction, never saw it. The `COUNT` fast path for
//! `LANG(?o) = "tag"` read `""` as a tag no literal has and counted 0, though
//! `LANG(?v) = ""` holds for every literal without one.
//!
//! The cases cover LANG, DATATYPE, isIRI/isLiteral and STR under `=`, `!=`,
//! `<`, `>`, `langMatches` and `STRSTARTS`, against hand-written answers: with
//! fast paths off both lanes run the same scan, so agreement between them is
//! no evidence. Each runs over novelty and over the index, with fast paths on
//! and off, in SPARQL and, for the LANG cases, in JSON-LD.
//!
//! The index lane also remembers a filter's verdict per encoded value, keyed
//! by the filter and the store. The filter's part hashed only which function
//! each call was, so SPARQL's strict `LANG` / `DATATYPE` and JSON-LD's lenient
//! ones shared verdicts on the same ledger, and the store's part was its
//! address, which a store built after another was dropped can reuse. Both
//! surfaces run on one ledger here, in both orders, and a rebuilt store is
//! checked against a dropped one.

use crate::support::{query_jsonld_formatted, query_sparql_formatted};
use fluree_db_api::{set_fast_paths_disabled, FlureeBuilder, ReindexOptions};
use serde_json::{json, Value};

const DATA: &str = r#"@prefix : <http://example.org/#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
:a :v "abc"@en .
:b :v "abc"@en-gb .
:c :v "abc"@fr .
:d :v "abc" .
:e :v "abd" .
:f :v 5 .
:g :v :iri .
:h :v "2026-01-01"^^xsd:date .
"#;

/// (filter over `?s :v ?v`, the subjects it keeps). LANG, DATATYPE and STR of
/// an IRI are type errors, so an IRI row fails every filter over them.
const CASES: &[(&str, &[&str])] = &[
    (r#"LANG(?v) = "en""#, &["a"]),
    (r#"LANG(?v) != "en""#, &["b", "c", "d", "e", "f", "h"]),
    (r#""en" != LANG(?v)"#, &["b", "c", "d", "e", "f", "h"]),
    (r#"langMatches(LANG(?v), "en")"#, &["a", "b"]),
    (r#"STRSTARTS(LANG(?v), "en")"#, &["a", "b"]),
    (r#"LANG(?v) < "en""#, &["d", "e", "f", "h"]),
    (r#"LANG(?v) > "en""#, &["b", "c"]),
    (r#"LANG(?v) = """#, &["d", "e", "f", "h"]),
    (r#"DATATYPE(?v) = xsd:string"#, &["d", "e"]),
    (r#"DATATYPE(?v) != xsd:string"#, &["a", "b", "c", "f", "h"]),
    (
        r#"STR(DATATYPE(?v)) != "x""#,
        &["a", "b", "c", "d", "e", "f", "h"],
    ),
    (r#"isIRI(?v)"#, &["g"]),
    (r#"isLiteral(?v)"#, &["a", "b", "c", "d", "e", "f", "h"]),
    (r#"STR(?v) = "abc""#, &["a", "b", "c", "d"]),
    (r#"STR(?v) != "abc""#, &["e", "f", "g", "h"]),
    (r#"STRSTARTS(STR(?v), "ab")"#, &["a", "b", "c", "d", "e"]),
    (r#"STR(?v) > "abc""#, &["e", "g"]),
];

/// The LANG cases, and one DATATYPE case, in JSON-LD: (filter, the subjects
/// it keeps). JSON-LD's `lang` and `datatype` are lenient (decision D-12):
/// LANG of an IRI is `""` and its DATATYPE is the `@id` type, not a type
/// error, so the IRI row passes `!=` and `= ""`.
const JSONLD_CASES: &[(&str, &[&str])] = &[
    (r#"(= (lang ?v) "en")"#, &["a"]),
    (
        r#"(!= (lang ?v) "en")"#,
        &["b", "c", "d", "e", "f", "g", "h"],
    ),
    (r#"(langMatches (lang ?v) "en")"#, &["a", "b"]),
    (r#"(strStarts (lang ?v) "en")"#, &["a", "b"]),
    (r#"(> (lang ?v) "en")"#, &["b", "c"]),
    (r#"(= (lang ?v) "")"#, &["d", "e", "f", "g", "h"]),
    (
        r#"(!= (str (datatype ?v)) "x")"#,
        &["a", "b", "c", "d", "e", "f", "g", "h"],
    ),
];

/// Restores fast paths on every exit path; the switch is process-global.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

fn subjects(rows: &Value) -> Vec<String> {
    let mut out: Vec<String> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            let cell = r.as_array().map_or(r, |cells| &cells[0]);
            cell.as_str()
                .expect("a subject")
                .trim_start_matches("ex:")
                .trim_start_matches(':')
                .trim_start_matches("http://example.org/#")
                .to_string()
        })
        .collect();
    out.sort();
    out
}

/// Both surfaces' filter results on one ledger, JSON-LD's first or SPARQL's
/// first in each lane: `(lane, filter, subjects kept)`.
async fn run_surfaces(jsonld_first: bool) -> Vec<(String, String, Vec<String>)> {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/filter-pushdown-operators:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    fluree.insert_turtle(ledger, DATA).await.expect("data");

    let mut out = Vec::new();
    for phase in ["novelty", "indexed"] {
        if phase == "indexed" {
            fluree
                .reindex(id, ReindexOptions::default())
                .await
                .expect("reindex");
        }
        let ledger = fluree.ledger(id).await.expect("ledger");
        assert_eq!(
            ledger.snapshot.range_provider.is_some(),
            phase == "indexed",
            "{phase}: setup"
        );
        for fast in [true, false] {
            set_fast_paths_disabled(!fast);
            let lane = format!("{phase}, fast paths {}", if fast { "on" } else { "off" });
            for jsonld in [jsonld_first, !jsonld_first] {
                if jsonld {
                    for (filter, _) in JSONLD_CASES {
                        let q = json!({
                            "@context": {"ex": "http://example.org/#"},
                            "select": "?s",
                            "where": [{"@id": "?s", "ex:v": "?v"}, ["filter", filter]]
                        });
                        let got = query_jsonld_formatted(&fluree, &ledger, &q).await.unwrap();
                        out.push((lane.clone(), format!("JSON-LD {filter}"), subjects(&got)));
                    }
                    continue;
                }
                for (filter, _) in CASES {
                    let q = format!(
                    "PREFIX : <http://example.org/#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                     SELECT ?s WHERE {{ ?s :v ?v FILTER({filter}) }}"
                );
                    let got = query_sparql_formatted(&fluree, &ledger, &q).await.unwrap();
                    out.push((
                        lane.clone(),
                        format!("SPARQL FILTER({filter})"),
                        subjects(&got),
                    ));
                    let count = format!(
                    "PREFIX : <http://example.org/#> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                     SELECT (COUNT(*) AS ?n) WHERE {{ ?s :v ?v FILTER({filter}) }}"
                );
                    let n = query_sparql_formatted(&fluree, &ledger, &count)
                        .await
                        .unwrap();
                    let n = n
                        .as_array()
                        .and_then(|rows| rows.first())
                        .map(|r| r.as_array().map_or(r, |cells| &cells[0]).clone())
                        .and_then(|v| v.as_u64())
                        .expect("a count");
                    out.push((
                        lane.clone(),
                        format!("SPARQL COUNT FILTER({filter})"),
                        vec![n.to_string()],
                    ));
                }
            }
        }
        set_fast_paths_disabled(false);
    }
    out
}

#[tokio::test]
async fn filters_over_a_function_of_the_object_answer_alike_on_every_lane() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set; the fast-paths-on runs would not run"
    );
    let _guard = FastPathGuard;
    let mut expected: Vec<(String, Vec<String>)> = Vec::new();
    for (filter, keeps) in CASES {
        let keeps: Vec<String> = keeps.iter().map(|s| s.to_string()).collect();
        expected.push((format!("SPARQL FILTER({filter})"), keeps.clone()));
        expected.push((
            format!("SPARQL COUNT FILTER({filter})"),
            vec![keeps.len().to_string()],
        ));
    }
    for (filter, keeps) in JSONLD_CASES {
        expected.push((
            format!("JSON-LD {filter}"),
            keeps.iter().map(|s| s.to_string()).collect(),
        ));
    }

    let mut failures = Vec::new();
    for jsonld_first in [false, true] {
        let order = if jsonld_first {
            "JSON-LD first"
        } else {
            "SPARQL first"
        };
        for (lane, case, got) in run_surfaces(jsonld_first).await {
            let lane = format!("{order}, {lane}");
            let want = &expected.iter().find(|(c, _)| *c == case).expect("a case").1;
            if got != *want {
                failures.push(format!("{lane}: {case}: {got:?}, expected {want:?}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A store built after another was dropped does not answer with the dropped
/// one's verdicts. Each round builds an indexed ledger whose only string is
/// `"abc"` or `"xyz"` in turn, at the same dictionary position, so the encoded
/// value the filter sees is the same while its answer flips, then drops it.
/// The regex is unanchored: an anchored one becomes a prefix range on the
/// index and never reaches the remembered verdicts.
#[tokio::test(flavor = "current_thread")]
async fn a_rebuilt_store_does_not_answer_for_a_dropped_one() {
    let q =
        r#"PREFIX : <http://example.org/#> SELECT ?s WHERE { ?s :v ?v FILTER(REGEX(?v, "b")) }"#;
    let mut failures = Vec::new();
    for round in 0..32 {
        let value = if round % 2 == 0 { "abc" } else { "xyz" };
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "it/filter-pushdown-rebuilt:main";
        let ledger = fluree.create_ledger(id).await.expect("create");
        fluree
            .insert_turtle(
                ledger,
                &format!("@prefix : <http://example.org/#> .\n:s :v \"{value}\" ."),
            )
            .await
            .expect("data");
        fluree
            .reindex(id, ReindexOptions::default())
            .await
            .expect("reindex");
        let ledger = fluree.ledger(id).await.expect("ledger");
        assert!(
            ledger.snapshot.range_provider.is_some(),
            "round {round}: setup"
        );
        let got = subjects(&query_sparql_formatted(&fluree, &ledger, q).await.unwrap());
        let want: Vec<String> = if value == "abc" {
            vec!["s".into()]
        } else {
            vec![]
        };
        if got != want {
            failures.push(format!(
                "round {round} ({value:?}): {got:?}, expected {want:?}"
            ));
        }
        drop(ledger);
        drop(fluree);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
