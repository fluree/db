//! FILTER string comparisons keep the term semantics of `=` and `<` when
//! they are pushed into the scan.
//!
//! `FILTER(?o = "a")`, and `<`, `<=`, `>`, `>=` against a plain-string
//! constant, are pushed into the scan as object bounds and the filter is
//! dropped. The bounds compared only the decoded value, so they accepted
//! every string-backed term with that text: `"a"@en`, `"a"@fr`, `"a"^^ex:T`.
//! The same comparison evaluated (under `||`, or as `!=`) is strict, so
//! `"a"@en` satisfied both `= "a"` and `!= "a"`. A string bound is now the
//! `xsd:string` term on every lane that applies bounds, and an indexed scan
//! seeks only the slices of the predicate that term can occupy.
//!
//! `=` never equates a string with a temporal, while `<` and `>` order one
//! by parsing it, so the same holds for a string bound against stored dates
//! and a date bound against stored strings.
//!
//! Each pushed shape must give the strict answer three ways: as written, with
//! the plan carrying the bounds; with fast paths disabled; and as an
//! evaluated twin, `(F) && BOUND(?o)`, which is not pushed. On a
//! novelty-only ledger, on the binary index, and with novelty over the index.
//! SPARQL, JSON-LD and Cypher. The constant-object leniency of a bare JSON-LD
//! string and of a Cypher property map is a separate, documented product
//! decision, pinned here unchanged.
//!
//! Own test binary, holding this one test: it toggles the process-global
//! fast-path kill switch, and its span capture needs no other test thread
//! hitting the stamp sites first.

#![cfg(feature = "native")]

mod support;

use crate::support::{graphdb_from_ledger, query_jsonld_formatted, query_sparql, span_capture};
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, LedgerState, ReindexOptions};
use serde_json::{json, Value};

const EX: &str = "http://example.org/ns/";
const PREFIX: &str = "PREFIX ex: <http://example.org/ns/> \
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
const LEDGER: &str = "it/string-filter-pushdown:main";
const STRING_BOUND_SEEK: &str = "string_bound_seek";

/// Restores the fast-path kill switch however the test exits.
struct FastPathGuard;

impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

/// Writes the `i`th FILTER `f` of a case into a query body.
type Shape = fn(usize, &str) -> String;

/// One FILTER shape over `?s <pattern> ?o`.
struct Case {
    name: &'static str,
    pattern: &'static str,
    filters: &'static [&'static str],
    /// The strict answer on the seed data, as local names.
    expected: &'static [&'static str],
    /// Whether the plan pushes the filter into the scan as object bounds.
    pushed: bool,
    /// Whether an indexed scan seeks by a string bound.
    string_seek: bool,
}

const fn case(
    name: &'static str,
    pattern: &'static str,
    filters: &'static [&'static str],
    expected: &'static [&'static str],
) -> Case {
    Case {
        name,
        pattern,
        filters,
        expected,
        pushed: true,
        string_seek: true,
    }
}

const fn evaluated(
    name: &'static str,
    filters: &'static [&'static str],
    expected: &'static [&'static str],
) -> Case {
    Case {
        name,
        pattern: "?s ex:p ?o",
        filters,
        expected,
        pushed: false,
        string_seek: false,
    }
}

const P: &str = "?s ex:p ?o";
const WHEN: &str = "?s ex:when ?o";
const AT: &str = "?s ex:at ?o";
const MIX: &str = "?s ex:mix ?o";
const DECISION: &str = "?s a ex:Decision ; ex:at ?o";
const A: &[&str] = &["plain", "xs"];

fn sparql_cases() -> Vec<Case> {
    vec![
        case("=", P, &[r#"?o = "a""#], A),
        case("= reversed", P, &[r#""a" = ?o"#], A),
        case("= xsd:string", P, &[r#"?o = "a"^^xsd:string"#], A),
        case(">= && <=", P, &[r#"?o >= "a" && ?o <= "a""#], A),
        case("<", P, &[r#"?o < "b""#], A),
        case("<=", P, &[r#"?o <= "a""#], A),
        case(">", P, &[r#"?o > """#], &["other", "plain", "xs"]),
        case(">=", P, &[r#"?o >= "b""#], &["other"]),
        case("= && <", P, &[r#"?o = "a" && ?o < "b""#], A),
        case("two filters", P, &[r#"?o >= "a""#, r#"?o < "b""#], A),
        // A string orders against a date by parsing it, but never equals one,
        // and the other way round.
        case(
            "date >= string",
            WHEN,
            &[r#"?o >= "2026-01-01""#],
            &["day", "day_s", "mar"],
        ),
        case(
            "date < string",
            WHEN,
            &[r#"?o < "2026-02-01""#],
            &["day", "day_s"],
        ),
        case("date = string", WHEN, &[r#"?o = "2026-01-01""#], &["day_s"]),
        Case {
            string_seek: false,
            ..case(
                "string = date",
                WHEN,
                &[r#"?o = "2026-01-01"^^xsd:date"#],
                &["day"],
            )
        },
        // Evaluated, not pushed: these were strict already.
        evaluated(
            "|| is evaluated",
            &[r#"?o = "a" || ?o = "zzz""#],
            &["other", "plain", "xs"],
        ),
        evaluated("< || is evaluated", &[r#"?o < "b" || ?o = "nomatch""#], A),
        evaluated(
            "!= is evaluated",
            &[r#"?o != "a""#],
            &["b_en", "en", "fr", "num", "other"],
        ),
        evaluated("= a tagged constant", &[r#"?o = "a"@en"#], &["en"]),
        // A plain string against typed dateTime, date and time values: the
        // ordering operators read the string as a value of the row's kind
        // (where it parses as one), `=` never does, and `>= && <=` with one
        // string is two orderings, not an `=`.
        case(
            "dateTime < string",
            AT,
            &[r#"?o < "2026-01-02T00:00:00Z""#],
            &["at_dt1", "at_s"],
        ),
        case(
            "dateTime <= string",
            AT,
            &[r#"?o <= "2026-01-02T00:00:00Z""#],
            &["at_dt1", "at_s"],
        ),
        case(
            "dateTime > string",
            AT,
            &[r#"?o > "2026-01-01T12:00:00Z""#],
            &["at_dt2"],
        ),
        case(
            "dateTime >= string",
            AT,
            &[r#"?o >= "2026-01-01T10:00:00Z""#],
            &["at_dt1", "at_dt2", "at_s"],
        ),
        case(
            "dateTime = string",
            AT,
            &[r#"?o = "2026-01-01T10:00:00Z""#],
            &["at_s"],
        ),
        case(
            "dateTime >= && <= one string",
            AT,
            &[r#"?o >= "2026-01-01T10:00:00Z" && ?o <= "2026-01-01T10:00:00Z""#],
            &["at_dt1", "at_s"],
        ),
        case(
            "date < string",
            AT,
            &[r#"?o < "2026-01-02""#],
            &["at_d", "at_s"],
        ),
        case("time < string", AT, &[r#"?o < "11:00:00""#], &["at_t"]),
        // A keyset pager over decision timestamps: the standalone form is
        // pushed, the paging form is evaluated (`=` is false there, so only
        // the strictly earlier decision comes back).
        // Pushed into the join's per-subject probes of `ex:at`, which check
        // the bound row by row; there is no POST range to seek.
        Case {
            string_seek: false,
            ..case(
                "pager <",
                DECISION,
                &[r#"?o < "2026-01-02T10:00:00Z""#],
                &["at_dt1"],
            )
        },
        Case {
            pattern: DECISION,
            ..evaluated(
                "pager || =",
                &[
                    r#"?o < "2026-01-02T10:00:00Z" || (?o = "2026-01-02T10:00:00Z" && STR(?s) < "http://example.org/ns/at_dt2")"#,
                ],
                &["at_dt1"],
            )
        },
        // A typed temporal bound orders a plain `xsd:string` row by reading
        // it as a value of that type, on the index too (beside the temporal
        // slice); a tagged or custom-typed string is a term of its own type
        // and is not read as one.
        Case {
            string_seek: false,
            ..case(
                "string rows < typed dateTime",
                AT,
                &[r#"?o < "2026-01-02T00:00:00Z"^^xsd:dateTime"#],
                &["at_dt1", "at_s"],
            )
        },
        Case {
            string_seek: false,
            ..case(
                "tagged and typed rows < typed date",
                "?s ex:pdp ?o",
                &[r#"?o < "2026-02-01"^^xsd:date"#],
                &["pd_d", "pd_s"],
            )
        },
        // `@fulltext` and JSON values are their own datatypes: never equal
        // to, nor ordered against, a plain string.
        case(
            "fulltext =",
            "?s ex:pftp ?o",
            &[r#"?o = "Hello World""#],
            &["pfs"],
        ),
        case("fulltext <", "?s ex:pftp ?o", &[r#"?o < "Z""#], &["pfs"]),
        case("json =", "?s ex:pjp ?o", &[r#"?o = '{"a":1}'"#], &["pjs"]),
        case("json <", "?s ex:pjp ?o", &[r#"?o < "~""#], &["pjs"]),
        // Other value classes against a plain string: a boolean, an IRI, a
        // number and a tagged string are neither equal nor ordered.
        case("mixed classes =", MIX, &[r#"?o = "a""#], &["m_plain"]),
        case("mixed classes <", MIX, &[r#"?o < "b""#], &["m_plain"]),
        case("mixed classes >", MIX, &[r#"?o > """#], &["m_plain"]),
        Case {
            pattern: MIX,
            ..evaluated(
                "mixed classes !=",
                &[r#"?o != "a""#],
                &["m_en", "m_iri", "m_num", "m_yes"],
            )
        },
    ]
}

/// JSON-LD filters over `?s ex:p ?o`: (name, filter, expected, pushed).
fn jsonld_cases() -> Vec<(&'static str, Value, &'static [&'static str], bool)> {
    vec![
        ("data =", json!(["=", "?o", "a"]), A, true),
        ("=", json!(r#"(= ?o "a")"#), A, true),
        ("<", json!(r#"(< ?o "b")"#), A, true),
        (
            "closed range",
            json!(r#"(and (>= ?o "a") (<= ?o "a"))"#),
            A,
            true,
        ),
        ("sandwich", json!(r#"(< "" ?o "b")"#), A, true),
        (
            "or is evaluated",
            json!(r#"(or (= ?o "a") (= ?o "zzz"))"#),
            &["other", "plain", "xs"],
            false,
        ),
        (
            "!= is evaluated",
            json!(r#"(!= ?o "a")"#),
            &["b_en", "en", "fr", "num", "other"],
            false,
        ),
    ]
}

/// The evaluated twin of a JSON-LD filter: the same test, not range-safe.
fn jsonld_twin(filter: &Value) -> Value {
    match filter {
        Value::String(s) => json!(format!("(and {s} (bound ?o))")),
        other => json!(["and", other, ["bound", "?o"]]),
    }
}

/// Local names of every subject in `rows`, sorted.
fn names(rows: &Value) -> Vec<String> {
    fn walk(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Array(items) => items.iter().for_each(|i| walk(i, out)),
            Value::String(s) => out.push(
                s.strip_prefix("ex:")
                    .or_else(|| s.strip_prefix(EX))
                    .unwrap_or(s)
                    .to_string(),
            ),
            other => out.push(other.to_string()),
        }
    }
    let mut out = Vec::new();
    walk(rows, &mut out);
    out.sort();
    out
}

/// `expected` in `phase`: the mixed phase adds a second plain `"a"` and an
/// `"a"@de`, which answer every filter here as `ex:plain` and `ex:fr` do.
fn expect(expected: &[&str], phase: &str) -> Vec<String> {
    let mut out: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
    if phase == "mixed" {
        if expected.contains(&"plain") {
            out.push("plain2".into());
        }
        if expected.contains(&"fr") {
            out.push("de".into());
        }
    }
    out.sort();
    out
}

fn pushes_bounds(plan: &Value) -> bool {
    plan.to_string().contains(r#""object-bounds":true"#)
}

async fn sparql(fluree: &Fluree, view: &LedgerState, body: &str) -> Vec<String> {
    let q = format!("{PREFIX}{body}");
    let rows = query_sparql(fluree, view, &q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    names(&rows.to_jsonld(&view.snapshot).expect("jsonld"))
}

async fn jsonld(fluree: &Fluree, view: &LedgerState, filter: &Value) -> Vec<String> {
    let q = jsonld_query(json!([{"@id": "?s", "ex:p": "?o"}, ["filter", filter]]));
    let rows = query_jsonld_formatted(fluree, view, &q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    names(&rows)
}

fn jsonld_query(r#where: Value) -> Value {
    json!({"@context": {"ex": EX}, "select": "?s", "where": r#where})
}

async fn cypher(fluree: &Fluree, view: &LedgerState, q: &str) -> Vec<String> {
    let db = graphdb_from_ledger(view);
    let cj = fluree
        .query_cypher(&db, q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let rows: Vec<Value> = cj["results"][0]["data"]
        .as_array()
        .unwrap_or_else(|| panic!("{q}: {cj}"))
        .iter()
        .map(|r| r["row"][0].clone())
        .collect();
    names(&Value::Array(rows))
}

/// Runs `run` and returns its answer plus the sites that stamped `proceed`.
async fn routed<F: std::future::Future<Output = Vec<String>>>(
    store: &span_capture::SpanStore,
    run: F,
) -> (Vec<String>, Vec<String>) {
    let before = store.find_events("fast-path outcome").len();
    let rows = run.await;
    let proceeded = store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
        .filter_map(|e| e.fields.get("site").cloned())
        .collect();
    (rows, proceeded)
}

#[tokio::test(flavor = "current_thread")]
async fn string_filter_comparisons_are_term_strict_on_every_lane() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;
    let (store, _tracing) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger(LEDGER).await.unwrap();
    let thing = |id: &str, o: Value| json!({"@id": id, "@type": "ex:Thing", "ex:p": o});
    let seed = json!({"@context": {"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
    "@graph": [
        thing("ex:plain", json!("a")),
        thing("ex:xs", json!({"@value": "a", "@type": "xsd:string"})),
        thing("ex:en", json!({"@value": "a", "@language": "en"})),
        thing("ex:fr", json!({"@value": "a", "@language": "fr"})),
        thing("ex:typed", json!({"@value": "a", "@type": "ex:T"})),
        thing("ex:other", json!("zzz")),
        thing("ex:b_en", json!({"@value": "b", "@language": "en"})),
        thing("ex:num", json!(5)),
        {"@id": "ex:day", "ex:when": {"@value": "2026-01-01", "@type": "xsd:date"}},
        {"@id": "ex:day_s", "ex:when": "2026-01-01"},
        {"@id": "ex:mar", "ex:when": {"@value": "2026-03-01", "@type": "xsd:date"}},
        {"@id": "ex:q0", "ex:q": "base"},
        {"@id": "ex:ft", "ex:ftp": {"@value": "a", "@type": "@fulltext"}},
        {"@id": "ex:ft_plain", "ex:ftp": "a"},
        {"@id": "ex:pft", "ex:pftp": {"@value": "Hello World", "@type": "@fulltext"}},
        {"@id": "ex:pfs", "ex:pftp": "Hello World"},
        {"@id": "ex:pj", "ex:pjp": {"@value": {"a": 1}, "@type": "@json"}},
        {"@id": "ex:pjs", "ex:pjp": "{\"a\":1}"},
        {"@id": "ex:pd_en", "ex:pdp": {"@value": "2026-01-01", "@language": "en"}},
        {"@id": "ex:pd_t", "ex:pdp": {"@value": "2026-01-01", "@type": "ex:T"}},
        {"@id": "ex:pd_s", "ex:pdp": "2026-01-01"},
        {"@id": "ex:pd_d", "ex:pdp": {"@value": "2026-01-01", "@type": "xsd:date"}},
        {"@id": "ex:at_dt1", "@type": "ex:Decision",
         "ex:at": {"@value": "2026-01-01T10:00:00Z", "@type": "xsd:dateTime"}},
        {"@id": "ex:at_dt2", "@type": "ex:Decision",
         "ex:at": {"@value": "2026-01-02T10:00:00Z", "@type": "xsd:dateTime"}},
        {"@id": "ex:at_d", "ex:at": {"@value": "2026-01-01", "@type": "xsd:date"}},
        {"@id": "ex:at_t", "ex:at": {"@value": "10:00:00", "@type": "xsd:time"}},
        {"@id": "ex:at_s", "ex:at": "2026-01-01T10:00:00Z"},
        {"@id": "ex:at_en", "ex:at": {"@value": "2026-01-01T10:00:00Z", "@language": "en"}},
        {"@id": "ex:m_plain", "ex:mix": "a"},
        {"@id": "ex:m_en", "ex:mix": {"@value": "a", "@language": "en"}},
        {"@id": "ex:m_yes", "ex:mix": true},
        {"@id": "ex:m_num", "ex:mix": 5},
        {"@id": "ex:m_iri", "ex:mix": {"@id": "ex:target"}}
    ]});
    fluree.insert(ledger, &seed).await.unwrap();

    let thing_label = format!("`{EX}Thing`");
    let p = format!("`{EX}p`");
    let cypher_eq = format!(r#"MATCH (s:{thing_label}) WHERE s.{p} = "a" RETURN id(s) AS id"#);
    let cypher_lt = format!(r#"MATCH (s:{thing_label}) WHERE s.{p} < "b" RETURN id(s) AS id"#);
    let cypher_map = format!(r#"MATCH (s:{thing_label} {{{p}: "a"}}) RETURN id(s) AS id"#);

    let mut failures: Vec<String> = Vec::new();
    for phase in ["novelty", "indexed", "mixed"] {
        match phase {
            "indexed" => {
                fluree
                    .reindex(LEDGER, ReindexOptions::default())
                    .await
                    .unwrap();
            }
            "mixed" => {
                let head = fluree.ledger(LEDGER).await.unwrap();
                let more = json!({"@context": {"ex": EX}, "@graph": [
                    thing("ex:plain2", json!("a")),
                    thing("ex:de", json!({"@value": "a", "@language": "de"})),
                    // A string the index has never seen, on an indexed
                    // predicate: the seek falls back to novelty alone.
                    {"@id": "ex:fresh", "ex:q": "fresh"},
                    {"@id": "ex:fresh_en", "ex:q": {"@value": "fresh", "@language": "en"}}
                ]});
                fluree.insert(head, &more).await.unwrap();
            }
            _ => {}
        }
        let view = fluree.ledger(LEDGER).await.unwrap();
        assert_eq!(
            view.snapshot.range_provider.is_some(),
            phase != "novelty",
            "{phase}: setup"
        );
        let db = graphdb_from_ledger(&view);

        for c in sparql_cases() {
            // The query as written, and three evaluated oracles that do not
            // share its plan: none of these shapes is range-safe, so the
            // comparison runs in the FILTER evaluator, not the scan.
            let oracles: [(&str, Shape); 3] = [
                ("&& BOUND", |_, f| format!(" FILTER(({f}) && BOUND(?o))")),
                ("|| false", |_, f| format!(" FILTER(({f}) || false)")),
                ("BIND", |i, f| {
                    format!(" BIND(({f}) AS ?keep{i}) FILTER(?keep{i})")
                }),
            ];
            let body = |shape: Shape| -> String {
                let filters: String = c
                    .filters
                    .iter()
                    .enumerate()
                    .map(|(i, f)| shape(i, f))
                    .collect();
                format!("SELECT ?s WHERE {{ {}{} }}", c.pattern, filters)
            };
            let q = body(|_, f| format!(" FILTER({f})"));
            let want = expect(c.expected, phase);
            let what = format!("{phase}: sparql {}", c.name);

            let plan = fluree
                .explain_sparql(&db, &format!("{PREFIX}{q}"))
                .await
                .unwrap();
            if pushes_bounds(&plan) != c.pushed {
                failures.push(format!(
                    "{what}: pushed = {}, want {}: {}",
                    !c.pushed, c.pushed, plan["plan"]["physical"]
                ));
            }

            let (got, proceeded) = routed(&store, sparql(&fluree, &view, &q)).await;
            if got != want {
                failures.push(format!("{what}: got {got:?}, want {want:?}"));
            }
            if c.string_seek
                && phase != "novelty"
                && !proceeded.iter().any(|s| s == STRING_BOUND_SEEK)
            {
                failures.push(format!(
                    "{what}: `{STRING_BOUND_SEEK}` did not proceed [{proceeded:?}]"
                ));
            }
            set_fast_paths_disabled(true);
            let generic = sparql(&fluree, &view, &q).await;
            set_fast_paths_disabled(false);
            if generic != want {
                failures.push(format!("{what}: fast paths off got {generic:?}"));
            }
            for (oracle, shape) in oracles {
                let twin = body(shape);
                let twin_plan = fluree
                    .explain_sparql(&db, &format!("{PREFIX}{twin}"))
                    .await
                    .unwrap();
                if pushes_bounds(&twin_plan) {
                    failures.push(format!("{what}: the {oracle} oracle was pushed"));
                }
                let evaluated = sparql(&fluree, &view, &twin).await;
                if evaluated != want {
                    failures.push(format!("{what}: {oracle} oracle got {evaluated:?}"));
                }
            }
        }

        // Aggregates over the pushed scan, which count lanes may answer.
        let a = expect(A, phase).len().to_string();
        for (select, filter, want) in [
            ("(COUNT(?s) AS ?c)", r#"?o = "a""#, a.clone()),
            ("(COUNT(*) AS ?c)", r#"?o < "b""#, a.clone()),
            ("(COUNT(DISTINCT ?o) AS ?c)", r#"?o = "a""#, "1".to_string()),
        ] {
            let q = format!("SELECT {select} WHERE {{ ?s ex:p ?o FILTER({filter}) }}");
            let what = format!("{phase}: sparql {select} FILTER({filter})");
            let got = sparql(&fluree, &view, &q).await;
            if got != [want.clone()] {
                failures.push(format!("{what}: got {got:?}, want {want}"));
            }
            set_fast_paths_disabled(true);
            let generic = sparql(&fluree, &view, &q).await;
            set_fast_paths_disabled(false);
            if generic != [want.clone()] {
                failures.push(format!("{what}: fast paths off got {generic:?}"));
            }
        }

        for (name, filter, expected, pushed) in jsonld_cases() {
            let what = format!("{phase}: json-ld {name}");
            let want = expect(expected, phase);
            let q = jsonld_query(json!([{"@id": "?s", "ex:p": "?o"}, ["filter", filter]]));
            if pushes_bounds(&fluree.explain(&db, &q).await.unwrap()) != pushed {
                failures.push(format!("{what}: pushed = {}, want {pushed}", !pushed));
            }
            let (got, proceeded) = routed(&store, jsonld(&fluree, &view, &filter)).await;
            if got != want {
                failures.push(format!("{what}: got {got:?}, want {want:?}"));
            }
            if pushed && phase != "novelty" && !proceeded.iter().any(|s| s == STRING_BOUND_SEEK) {
                failures.push(format!(
                    "{what}: `{STRING_BOUND_SEEK}` did not proceed [{proceeded:?}]"
                ));
            }
            let evaluated = jsonld(&fluree, &view, &jsonld_twin(&filter)).await;
            if evaluated != want {
                failures.push(format!("{what}: evaluated twin got {evaluated:?}"));
            }
        }

        // Cypher `=` is pushed; `<` reads the property through an OPTIONAL
        // and is evaluated.
        for (name, q, expected, pushed) in [("=", &cypher_eq, A, true), ("<", &cypher_lt, A, false)]
        {
            let what = format!("{phase}: cypher {name}");
            let want = expect(expected, phase);
            let plan = fluree.explain_cypher(&db, q, None).await.unwrap();
            if pushes_bounds(&plan) != pushed {
                failures.push(format!("{what}: pushed = {}, want {pushed}", !pushed));
            }
            let got = cypher(&fluree, &view, q).await;
            if got != want {
                failures.push(format!("{what}: got {got:?}, want {want:?}"));
            }
        }

        // `@fulltext` is its own datatype, `f:fullText`, so a pushed string
        // bound leaves it out on every lane. (The evaluated comparison reads
        // an indexed `@fulltext` value as a plain string, unlike a novelty
        // one; that split is the evaluator's and is not pinned here.)
        for body in [
            r#"SELECT ?s WHERE { ?s ex:ftp ?o FILTER(?o = "a") }"#,
            r#"SELECT ?s WHERE { ?s ex:ftp ?o FILTER(?o < "b") }"#,
        ] {
            let got = sparql(&fluree, &view, body).await;
            if got != ["ft_plain"] {
                failures.push(format!("{phase}: {body} got {got:?}"));
            }
        }

        // The string builtins read `@fulltext` and JSON values as text, on
        // both lanes; only comparison is term-exact.
        let text = |rows: &[&str]| -> Vec<String> {
            let mut v: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
            v.sort();
            v
        };
        for (body, want) in [
            (
                r#"SELECT ?s WHERE { ?s ex:pftp ?o FILTER(CONTAINS(?o, "World")) }"#,
                text(&["pfs", "pft"]),
            ),
            (
                r#"SELECT ?s WHERE { ?s ex:pftp ?o FILTER(STRSTARTS(?o, "Hello")) }"#,
                text(&["pfs", "pft"]),
            ),
            (
                r#"SELECT ?s WHERE { ?s ex:pftp ?o FILTER(REGEX(?o, "^hello", "i")) }"#,
                text(&["pfs", "pft"]),
            ),
            (
                "SELECT ?s (SUBSTR(?o, 1, 5) AS ?x) WHERE { ?s ex:pftp ?o }",
                text(&["Hello", "Hello", "pfs", "pft"]),
            ),
            (
                "SELECT ?s (STRLEN(?o) AS ?x) WHERE { ?s ex:pftp ?o }",
                text(&["11", "11", "pfs", "pft"]),
            ),
            (
                r#"SELECT ?s (CONCAT(?o, "!") AS ?x) WHERE { ?s ex:pftp ?o }"#,
                text(&["Hello World!", "Hello World!", "pfs", "pft"]),
            ),
            (
                r#"SELECT ?s (STRBEFORE(?o, " ") AS ?x) WHERE { ?s ex:pftp ?o }"#,
                text(&["Hello", "Hello", "pfs", "pft"]),
            ),
            (
                r#"SELECT ?s WHERE { ?s ex:pjp ?o FILTER(CONTAINS(?o, "a")) }"#,
                text(&["pj", "pjs"]),
            ),
            (
                r#"SELECT ?s WHERE { ?s ex:pjp ?o FILTER(CONTAINS(STR(?o), "a")) }"#,
                text(&["pj", "pjs"]),
            ),
            (
                "SELECT ?s (STR(?o) AS ?x) WHERE { ?s ex:pjp ?o }",
                text(&[r#"{"a":1}"#, r#"{"a":1}"#, "pj", "pjs"]),
            ),
            // A JSON value bound by BIND stays `rdf:JSON`.
            (
                "SELECT ?s (DATATYPE(?x) AS ?d) WHERE { ?s ex:pjp ?o BIND(?o AS ?x) }",
                text(&[
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON",
                    "pj",
                    "pjs",
                    "xsd:string",
                ]),
            ),
        ] {
            // A query error is a failure to record, not a panic: on novelty a
            // builtin refusing its argument fails the whole query.
            let q = format!("{PREFIX}{body}");
            match query_sparql(&fluree, &view, &q).await {
                Ok(rows) => {
                    let got = names(&rows.to_jsonld(&view.snapshot).expect("jsonld"));
                    if got != want {
                        failures.push(format!("{phase}: {body} got {got:?}, want {want:?}"));
                    }
                }
                Err(e) => failures.push(format!("{phase}: {body} failed: {e}")),
            }
        }

        if phase == "mixed" {
            for (body, want) in [
                (
                    r#"SELECT ?s WHERE { ?s ex:q ?o FILTER(?o = "fresh") }"#,
                    vec!["fresh"],
                ),
                (
                    r#"SELECT ?s WHERE { ?s ex:q ?o FILTER(?o = "base") }"#,
                    vec!["q0"],
                ),
                (
                    r#"SELECT ?s WHERE { ?s ex:q ?o FILTER(?o < "g") }"#,
                    vec!["fresh", "q0"],
                ),
            ] {
                let got = sparql(&fluree, &view, body).await;
                if got != want {
                    failures.push(format!("{phase}: {body} got {got:?}, want {want:?}"));
                }
            }
        }

        // The constant-object position stays lenient across the string
        // family on JSON-LD and in a Cypher property map, as documented.
        let lenient = expect(&["en", "fr", "plain", "typed", "xs"], phase);
        let got = names(
            &query_jsonld_formatted(
                &fluree,
                &view,
                &jsonld_query(json!({"@id": "?s", "ex:p": "a"})),
            )
            .await
            .unwrap(),
        );
        if got != lenient {
            failures.push(format!("{phase}: json-ld bare constant got {got:?}"));
        }
        let got = cypher(&fluree, &view, &cypher_map).await;
        if got != lenient {
            failures.push(format!("{phase}: cypher property map got {got:?}"));
        }
    }

    // Two FILTERs on one variable whose bounds cannot be ordered against
    // each other (a number and a string) cannot both be pushed: keeping
    // either bound would drop the other FILTER. No value is both `> 1` and
    // `> "a"`.
    let id = "it/string-filter-pushdown-unorderable:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let seed = json!({"@context": {"ex": EX}, "@graph": [
        {"@id": "ex:n", "ex:p": 5},
        {"@id": "ex:s", "ex:p": "b"}
    ]});
    fluree.insert(ledger, &seed).await.unwrap();
    for phase in ["novelty", "indexed"] {
        if phase == "indexed" {
            fluree.reindex(id, ReindexOptions::default()).await.unwrap();
        }
        let view = fluree.ledger(id).await.unwrap();
        for body in [
            r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(?o > 1) FILTER(?o > "a") }"#,
            r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(?o > "a") FILTER(?o > 1) }"#,
        ] {
            let got = sparql(&fluree, &view, body).await;
            if !got.is_empty() {
                failures.push(format!("{phase}: {body} got {got:?}"));
            }
        }
        for filters in [["(> ?o 1)", r#"(> ?o "a")"#], [r#"(> ?o "a")"#, "(> ?o 1)"]] {
            let q = jsonld_query(json!([
                {"@id": "?s", "ex:p": "?o"},
                ["filter", filters[0]],
                ["filter", filters[1]]
            ]));
            let got = names(&query_jsonld_formatted(&fluree, &view, &q).await.unwrap());
            if !got.is_empty() {
                failures.push(format!("{phase}: json-ld {filters:?} got {got:?}"));
            }
        }
    }

    // The integer `=` seek (`FILTER(?v = 7)`) seeks one numeric type only
    // when every leaflet of the predicate holds that type, and skips
    // leaflets of other predicates by `p_const`. Leaflets never span two
    // predicates in POST (every writer cuts them at a `p_id` change), so the
    // skip cannot hide a row. Pinned with two-row leaflets: the predicate's
    // double `7.0` must still answer an integer `= 7`, indexed and with the
    // double arriving in novelty over an all-integer index.
    let id = "it/string-filter-pushdown-uniform:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let seed = json!({"@context": {"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
    "@graph": [
        {"@id": "ex:a", "ex:aa": "x"},
        {"@id": "ex:i1", "ex:v": 1}, {"@id": "ex:i2", "ex:v": 2},
        {"@id": "ex:i3", "ex:v": 3}, {"@id": "ex:i7", "ex:v": 7},
        {"@id": "ex:d7", "ex:v": {"@value": "7.0", "@type": "xsd:double"}},
        {"@id": "ex:z", "ex:zz": "y"},
        {"@id": "ex:u1", "ex:u": 1}, {"@id": "ex:u2", "ex:u": 2},
        {"@id": "ex:u3", "ex:u": 3}, {"@id": "ex:u7", "ex:u": 7}
    ]});
    fluree.insert(ledger, &seed).await.unwrap();
    let tiny = ReindexOptions::default().with_indexer_config(
        fluree_db_indexer::IndexerConfig::default()
            .with_leaflet_rows(2)
            .with_leaflets_per_leaf(2),
    );
    fluree.reindex(id, tiny).await.unwrap();
    let head = fluree.ledger(id).await.unwrap();
    fluree
        .insert(
            head,
            &json!({"@context": {"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
                "@id": "ex:ud7", "ex:u": {"@value": "7.0", "@type": "xsd:double"}}),
        )
        .await
        .unwrap();
    let view = fluree.ledger(id).await.unwrap();
    for (p, want) in [("ex:v", vec!["d7", "i7"]), ("ex:u", vec!["u7", "ud7"])] {
        let pushed = format!("SELECT ?s WHERE {{ ?s {p} ?o FILTER(?o = 7) }}");
        let evaluated = format!("SELECT ?s WHERE {{ ?s {p} ?o FILTER(?o = 7 || false) }}");
        set_fast_paths_disabled(true);
        let generic = sparql(&fluree, &view, &pushed).await;
        set_fast_paths_disabled(false);
        for (shape, got) in [
            ("pushed", sparql(&fluree, &view, &pushed).await),
            ("evaluated", sparql(&fluree, &view, &evaluated).await),
            ("fast paths off", generic),
        ] {
            if got != want {
                failures.push(format!("uniform {p} {shape}: got {got:?}, want {want:?}"));
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
