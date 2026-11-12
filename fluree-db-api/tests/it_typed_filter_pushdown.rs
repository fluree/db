//! `FILTER(?o = "x"^^ex:T)` against a typed literal whose datatype has no
//! value space of its own (a custom datatype, `xsd:anyURI`, `xsd:token`) is
//! pushed into the scan as that one term, and an indexed scan seeks it as
//! `?s ex:p "x"^^ex:T` does. `=` against such a literal is identity of terms,
//! so the same text under any other datatype, a language tag or none never
//! matches. SPARQL lowers the typed literal to `STRDT("x", ex:T)`, and JSON-LD
//! writes `(str-dt "x" "ex:T")`; both fold into the bound.
//!
//! Each case is checked against a hand-written answer, with the plan
//! carrying the bounds (or not), with fast paths off, and against an
//! evaluated twin, `(F) && BOUND(?o)`, that is never pushed; and routed:
//! where the bound is pushed an indexed scan must seek it, and where it may
//! not fold (a numeric datatype, a `||`) nothing may. Over novelty, the index,
//! and novelty over the index, including a datatype and a string only
//! novelty holds; through a join, a property-join star and a dataset of two
//! ledgers that code the datatype's namespace differently.
//!
//! Own test binary: it toggles the process-global fast-path kill switch, and
//! its span capture needs no other test thread hitting the stamp sites first.

#![cfg(feature = "native")]

mod support;

use crate::support::{graphdb_from_ledger, query_jsonld_formatted, query_sparql, span_capture};
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, LedgerState, ReindexOptions};
use serde_json::{json, Value};

const EX: &str = "http://example.org/ns/";
const PREFIX: &str = "PREFIX ex: <http://example.org/ns/> \
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
const LEDGER: &str = "it/typed-filter-pushdown:main";
const OTHER: &str = "it/typed-filter-pushdown-other:main";
const TYPED_SEEK: &str = "typed_string_bound_seek";

/// Restores the fast-path kill switch however the test exits.
struct FastPathGuard;

impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

/// One FILTER over a pattern binding `?o`: (name, pattern, filter, the
/// answer's local names, whether the plan shows the bounds pushed (`None`
/// where the operator reports none: a property join applies them without
/// saying so), and whether an indexed scan must seek the typed term:
/// `Some(true)` must, `Some(false)` must not, `None` either, as the join
/// lanes probe instead).
type Case = (
    &'static str,
    &'static str,
    &'static str,
    &'static [&'static str],
    Option<bool>,
    Option<bool>,
);

const P: &str = "?s ex:p ?o";
const JOIN: &str = "?s a ex:Thing . ?s ex:p ?o";
const STAR: &str = "?s ex:p ?o ; ex:q ?q";
const VALUES: &str = "VALUES ?s { ex:t ex:u ex:plain ex:en ex:t2 } ?s ex:p ?o";
const STAR3: &str = "?s a ex:Thing ; ex:q ?q ; ex:p ?o";
const MUST: Option<bool> = Some(true);
const NEVER: Option<bool> = Some(false);

fn sparql_cases() -> Vec<Case> {
    vec![
        ("custom", P, r#"?o = "x"^^ex:T"#, &["t"], Some(true), MUST),
        ("reversed", P, r#""x"^^ex:T = ?o"#, &["t"], Some(true), MUST),
        (
            "STRDT",
            P,
            r#"?o = STRDT("x", ex:T)"#,
            &["t"],
            Some(true),
            MUST,
        ),
        (
            "anyURI",
            P,
            r#"?o = "x"^^xsd:anyURI"#,
            &["uri"],
            Some(true),
            MUST,
        ),
        (
            "token",
            P,
            r#"?o = "x"^^xsd:token"#,
            &["tok"],
            Some(true),
            MUST,
        ),
        (
            "other text",
            P,
            r#"?o = "y"^^ex:T"#,
            &["ty"],
            Some(true),
            MUST,
        ),
        (
            "no scheme",
            P,
            r#"?o = "x"^^<T>"#,
            &["bare"],
            Some(true),
            MUST,
        ),
        (
            "unseen namespace",
            P,
            r#"?o = "x"^^<http://nowhere.example/T>"#,
            &[],
            Some(true),
            MUST,
        ),
        ("join", JOIN, r#"?o = "x"^^ex:T"#, &["t"], Some(true), None),
        ("star", STAR, r#"?o = "x"^^ex:T"#, &["t"], Some(true), None),
        (
            "values join",
            VALUES,
            r#"?o = "x"^^ex:T"#,
            &["t"],
            Some(true),
            None,
        ),
        (
            "star of three",
            STAR3,
            r#"?o = "x"^^ex:T"#,
            &["t"],
            None,
            None,
        ),
        // Canaries: nothing here may fold into a typed bound.
        (
            "integer",
            P,
            r#"?o = "5"^^xsd:integer"#,
            &["num"],
            Some(true),
            NEVER,
        ),
        (
            "STRDT integer",
            P,
            r#"STRDT("5", xsd:integer) = ?o"#,
            &["num"],
            Some(false),
            NEVER,
        ),
        (
            "or",
            P,
            r#"?o = "x"^^ex:T || false"#,
            &["t"],
            Some(false),
            NEVER,
        ),
        (
            "two datatypes",
            P,
            r#"?o = "x"^^ex:T && ?o = "x"^^ex:U"#,
            &[],
            Some(false),
            NEVER,
        ),
        // A plain and a typed `=` on one variable are two terms: the merge
        // keeps the typed one a filter.
        (
            "plain and typed filters",
            r#"?s ex:p ?o FILTER(?o = "x")"#,
            r#"?o = "x"^^ex:T"#,
            &[],
            Some(true),
            NEVER,
        ),
    ]
}

/// JSON-LD twins over `?s ex:p ?o`: (name, filter, answer, pushed, seek).
fn jsonld_cases() -> Vec<(
    &'static str,
    Value,
    &'static [&'static str],
    bool,
    Option<bool>,
)> {
    vec![
        (
            "str-dt",
            json!(r#"(= ?o (str-dt "x" "ex:T"))"#),
            &["t"],
            true,
            MUST,
        ),
        (
            "str-dt reversed",
            json!(r#"(= (str-dt "x" "ex:T") ?o)"#),
            &["t"],
            true,
            MUST,
        ),
        (
            "anyURI",
            json!(r#"(= ?o (str-dt "x" "xsd:anyURI"))"#),
            &["uri"],
            true,
            MUST,
        ),
        (
            "or",
            json!(r#"(or (= ?o (str-dt "x" "ex:T")) false)"#),
            &["t"],
            false,
            NEVER,
        ),
    ]
}

/// The answer in `phase`: novelty over the index adds a second `"x"^^ex:T`.
fn expect(expected: &[&str], phase: &str) -> Vec<String> {
    let mut out: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
    if phase == "mixed" && expected.contains(&"t") {
        out.push("t2".into());
    }
    out.sort();
    out
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

fn jsonld_query(filter: &Value) -> Value {
    json!({
        "@context": {"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
        "select": "?s",
        "where": [{"@id": "?s", "ex:p": "?o"}, ["filter", filter]]
    })
}

async fn jsonld(fluree: &Fluree, view: &LedgerState, filter: &Value) -> Vec<String> {
    let q = jsonld_query(filter);
    let rows = query_jsonld_formatted(fluree, view, &q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    names(&rows)
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

async fn insert_sparql(fluree: &Fluree, ledger: &str, triples: &str) {
    fluree
        .graph(ledger)
        .transact()
        .sparql_update(&format!("{PREFIX}INSERT DATA {{ {triples} }}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("{triples}: {e}"));
}

#[tokio::test(flavor = "current_thread")]
async fn typed_literal_equality_pushes_as_its_term() {
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set; the routing checks would pin nothing"
    );
    let _guard = FastPathGuard;
    let (store, _tracing) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.unwrap();
    insert_sparql(
        &fluree,
        LEDGER,
        r#"ex:t a ex:Thing ; ex:p "x"^^ex:T ; ex:q 1 .
           ex:u a ex:Thing ; ex:p "x"^^ex:U ; ex:q 1 .
           ex:plain a ex:Thing ; ex:p "x" ; ex:q 1 .
           ex:en a ex:Thing ; ex:p "x"@en ; ex:q 1 .
           ex:uri ex:p "x"^^xsd:anyURI .
           ex:tok ex:p "x"^^xsd:token .
           ex:ty ex:p "y"^^ex:T .
           ex:bare ex:p "x"^^<T> .
           ex:num ex:p 5 ."#,
    )
    .await;

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
                // A datatype and a string the index has never held, and one
                // more fact of an indexed datatype.
                insert_sparql(
                    &fluree,
                    LEDGER,
                    r#"ex:t2 a ex:Thing ; ex:p "x"^^ex:T ; ex:q 1 .
                       ex:n ex:p "x"^^ex:N .
                       ex:z ex:p "zzz"^^ex:T ."#,
                )
                .await;
            }
            _ => {}
        }
        let view = fluree.ledger(LEDGER).await.unwrap();
        let db = graphdb_from_ledger(&view);
        let mut cases = sparql_cases();
        if phase == "mixed" {
            cases.push((
                "novelty datatype",
                P,
                r#"?o = "x"^^ex:N"#,
                &["n"],
                Some(true),
                MUST,
            ));
            cases.push((
                "novelty string",
                P,
                r#"?o = "zzz"^^ex:T"#,
                &["z"],
                Some(true),
                MUST,
            ));
        }

        for (name, pattern, filter, expected, pushed, seek) in cases {
            let what = format!("{phase}: sparql {name}");
            let want = expect(expected, phase);
            let q = format!("SELECT ?s WHERE {{ {pattern} FILTER({filter}) }}");
            let plan = fluree
                .explain_sparql(&db, &format!("{PREFIX}{q}"))
                .await
                .unwrap();
            if pushed.is_some_and(|pushed| pushes_bounds(&plan) != pushed) {
                failures.push(format!(
                    "{what}: pushed = {}, want {pushed:?}: {}",
                    pushes_bounds(&plan),
                    plan["plan"]["physical"]
                ));
            }
            let (got, proceeded) = routed(&store, sparql(&fluree, &view, &q)).await;
            if got != want {
                failures.push(format!("{what}: got {got:?}, want {want:?}"));
            }
            let seeked = proceeded.iter().any(|s| s == TYPED_SEEK);
            if seek == MUST && phase != "novelty" && !seeked {
                failures.push(format!(
                    "{what}: `{TYPED_SEEK}` did not proceed [{proceeded:?}]"
                ));
            }
            if seek == NEVER && seeked {
                failures.push(format!("{what}: `{TYPED_SEEK}` proceeded"));
            }
            set_fast_paths_disabled(true);
            let generic = sparql(&fluree, &view, &q).await;
            set_fast_paths_disabled(false);
            if generic != want {
                failures.push(format!("{what}: fast paths off got {generic:?}"));
            }
            let twin = format!("SELECT ?s WHERE {{ {pattern} FILTER(({filter}) && BOUND(?o)) }}");
            let evaluated = sparql(&fluree, &view, &twin).await;
            if evaluated != want {
                failures.push(format!("{what}: evaluated twin got {evaluated:?}"));
            }
        }

        for (name, filter, expected, pushed, seek) in jsonld_cases() {
            let what = format!("{phase}: json-ld {name}");
            let want = expect(expected, phase);
            if pushes_bounds(&fluree.explain(&db, &jsonld_query(&filter)).await.unwrap()) != pushed
            {
                failures.push(format!("{what}: pushed = {}, want {pushed}", !pushed));
            }
            let (got, proceeded) = routed(&store, jsonld(&fluree, &view, &filter)).await;
            if got != want {
                failures.push(format!("{what}: got {got:?}, want {want:?}"));
            }
            let seeked = proceeded.iter().any(|s| s == TYPED_SEEK);
            if seek == MUST && phase != "novelty" && !seeked {
                failures.push(format!(
                    "{what}: `{TYPED_SEEK}` did not proceed [{proceeded:?}]"
                ));
            }
            if seek == NEVER && seeked {
                failures.push(format!("{what}: `{TYPED_SEEK}` proceeded"));
            }
            let twin = match &filter {
                Value::String(s) => json!(format!("(and {s} (bound ?o))")),
                other => other.clone(),
            };
            let evaluated = jsonld(&fluree, &view, &twin).await;
            if evaluated != want {
                failures.push(format!("{what}: evaluated twin got {evaluated:?}"));
            }
        }
    }

    // Two ledgers that code `ex:` differently: the other ledger registers a
    // namespace first. Each member reads the datatype as it codes it.
    fluree.create_ledger(OTHER).await.unwrap();
    insert_sparql(
        &fluree,
        OTHER,
        r#"<http://other.example/x> <http://other.example/q> "z" ."#,
    )
    .await;
    insert_sparql(
        &fluree,
        OTHER,
        r#"ex:t_b ex:p "x"^^ex:T . ex:u_b ex:p "x"^^ex:U ."#,
    )
    .await;
    for state in ["novelty", "indexed"] {
        if state == "indexed" {
            fluree
                .reindex(OTHER, ReindexOptions::default())
                .await
                .unwrap();
        }
        for (name, body) in [
            (
                "sparql",
                format!(
                    r#"{PREFIX}SELECT ?s FROM <{LEDGER}> FROM <{OTHER}> WHERE {{ ?s ex:p ?o FILTER(?o = "x"^^ex:T) }}"#
                ),
            ),
            (
                "sparql STRDT",
                format!(
                    r#"{PREFIX}SELECT ?s FROM <{LEDGER}> FROM <{OTHER}> WHERE {{ ?s ex:p ?o FILTER(STRDT("x", ex:T) = ?o) }}"#
                ),
            ),
        ] {
            let json = fluree
                .query_from()
                .sparql(&body)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{body}: {e}"));
            let mut got: Vec<String> = json["results"]["bindings"]
                .as_array()
                .expect("bindings")
                .iter()
                .map(|b| {
                    let s = b["s"]["value"].as_str().unwrap();
                    s.strip_prefix(EX).unwrap_or(s).to_string()
                })
                .collect();
            got.sort();
            let want = vec!["t", "t2", "t_b"];
            if got != want {
                failures.push(format!(
                    "two ledgers, {state}: {name}: got {got:?}, want {want:?}"
                ));
            }
        }
        let jsonld = json!({
            "@context": {"ex": EX},
            "from": [LEDGER, OTHER],
            "select": "?s",
            "where": [{"@id": "?s", "ex:p": "?o"}, ["filter", r#"(= ?o (str-dt "x" "ex:T"))"#]]
        });
        let got = names(
            &fluree
                .query_from()
                .jsonld(&jsonld)
                .execute_formatted()
                .await
                .expect("json-ld"),
        );
        if got != ["t", "t2", "t_b"] {
            failures.push(format!("two ledgers, {state}: json-ld: got {got:?}"));
        }
    }

    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
