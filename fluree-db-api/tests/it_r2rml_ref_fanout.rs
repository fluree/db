//! A referencing object map joins each child row to every distinct parent
//! subject its join key matches. R2RML §11 makes one triple per joined parent
//! row, and an RDF graph is a set, so parent rows minting the same subject give
//! one triple.
//!
//! The source is the committed local Iceberg table `sets.parents_dup`
//! (`scripts/local/write_local_iceberg_table.py`): a unique `id`, and `code` and
//! `tier` columns whose values repeat, with a null in each, across two files.
//! One mapping joins the table to itself:
//! - `ex:sibling` (on `code`) and `ex:peer` (on `tier`) reach every item that
//!   shares the value: a parent key matching several subjects (fan-out);
//! - `ex:group` reaches `group/{code}`: a repeated key that mints one subject,
//!   the shape of a history table's versions, so one edge per item;
//! - `ex:band` reaches `band/{tier}` on `code`: a key whose rows mint some
//!   subjects more than once;
//! - `ex:self` and `ex:twin` (on `id`) are referencing maps over a unique key.
//!
//! Every shape runs on SPARQL and on its JSON-LD twin, over the graph source with
//! fast paths on and off, and over a native ledger holding the triples the
//! mapping defines. Those triples are computed here from the fixture's rows by a
//! nested-loop join, independently of the materializer, and a few answers are
//! also pinned by count so the oracle itself is checked. The fused aggregate's
//! routing stamp is pinned: it folds a key matching one subject and declines a
//! key matching several. The table declares its key (`id`, unique), so each
//! item is one row's and the fused aggregate may fold at all. The twin the bulk builder makes from the source holds
//! the same triples.
//!
//! Own binary: it flips the process-wide fast-path switch, sets
//! `FLUREE_ICEBERG_LOCAL_ROOTS`, and asserts routing stamps.

#![cfg(all(feature = "iceberg", feature = "native"))]

#[path = "support/span_capture.rs"]
mod span_capture;

use std::collections::BTreeSet;
use std::sync::Arc;

use fluree_db_api::{
    set_fast_paths_disabled, Fluree, FlureeBuilder, FlureeR2rmlProvider, R2rmlCreateConfig,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Every test flips the process-wide fast-path switch.
static SERIAL: Mutex<()> = Mutex::const_new(());

const EX: &str = "http://example.org/";
const FUSED: &str = "fused_r2rml_aggregate";
const SOURCE: &str = "fanout:main";
const NATIVE: &str = "fanout-native:main";

struct Row {
    id: i64,
    code: Option<&'static str>,
    tier: Option<&'static str>,
    name: &'static str,
}

const fn row(
    id: i64,
    code: Option<&'static str>,
    tier: Option<&'static str>,
    name: &'static str,
) -> Row {
    Row {
        id,
        code,
        tier,
        name,
    }
}

/// `sets.parents_dup`, as the fixture writer writes it.
const ROWS: [Row; 8] = [
    row(1, Some("A"), Some("gold"), "alpha"),
    row(2, Some("A"), Some("silver"), "bravo"),
    row(3, Some("B"), Some("gold"), "charlie"),
    row(4, Some("C"), Some("silver"), "delta"),
    row(5, Some("C"), Some("gold"), "echo"),
    row(6, Some("C"), Some("silver"), "foxtrot"),
    row(7, None, Some("gold"), "golf"),
    row(8, Some("D"), None, "hotel"),
];

const MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix f: <https://ns.flur.ee/db#> .

    <http://example.org/mapping#Item>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.parents_dup" ; f:key ( "id" ) ] ;
        rr:subjectMap [ rr:template "http://example.org/item/{id}" ; rr:class ex:Item ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:code ; rr:objectMap [ rr:column "code" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:tier ; rr:objectMap [ rr:column "tier" ] ] ;
        rr:predicateObjectMap [
            rr:predicate ex:sibling ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Item> ;
                rr:joinCondition [ rr:child "code" ; rr:parent "code" ]
            ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:peer ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Item> ;
                rr:joinCondition [ rr:child "tier" ; rr:parent "tier" ]
            ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:group ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Group> ;
                rr:joinCondition [ rr:child "code" ; rr:parent "code" ]
            ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:band ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Band> ;
                rr:joinCondition [ rr:child "code" ; rr:parent "code" ]
            ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:self ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Item> ;
                rr:joinCondition [ rr:child "id" ; rr:parent "id" ]
            ]
        ] ;
        rr:predicateObjectMap [
            rr:predicate ex:twin ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Item> ;
                rr:joinCondition [ rr:child "id" ; rr:parent "id" ]
            ]
        ] .

    <http://example.org/mapping#Group>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.parents_dup" ] ;
        rr:subjectMap [ rr:template "http://example.org/group/{code}" ] .

    <http://example.org/mapping#Band>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "sets.parents_dup" ] ;
        rr:subjectMap [ rr:template "http://example.org/band/{tier}" ] .
"#;

fn ex(local: &str) -> String {
    format!("{EX}{local}")
}

fn item(id: i64) -> String {
    ex(&format!("item/{id}"))
}

/// The triples the mapping defines over [`ROWS`], as JSON-LD nodes. A
/// referencing object map joins a child row to every parent row whose join
/// column equals the child's; a null join or subject column joins nothing; and
/// the objects of one subject and predicate are a set.
fn mapped_graph() -> Vec<Value> {
    /// A join column's value, or a subject template's IRI, for a row.
    type Term = fn(&Row) -> Option<String>;
    let code: Term = |r| r.code.map(str::to_string);
    let tier: Term = |r| r.tier.map(str::to_string);
    let id: Term = |r| Some(r.id.to_string());
    let item_of: Term = |r| Some(item(r.id));
    let group_of: Term = |r| r.code.map(|c| ex(&format!("group/{c}")));
    let band_of: Term = |r| r.tier.map(|t| ex(&format!("band/{t}")));
    // The ref objects of `child` for a map joining `column` = `column`.
    let refs = |child: &Row, column: Term, parent_subject: Term| -> Vec<Value> {
        let Some(value) = column(child) else {
            return Vec::new();
        };
        let objects: BTreeSet<String> = ROWS
            .iter()
            .filter(|p| column(p).as_ref() == Some(&value))
            .filter_map(parent_subject)
            .collect();
        objects.into_iter().map(|o| json!({ "@id": o })).collect()
    };
    ROWS.iter()
        .map(|r| {
            let mut node = json!({
                "@id": item(r.id),
                "@type": ex("Item"),
                ex("name"): r.name,
            });
            if let Some(c) = r.code {
                node[ex("code")] = json!(c);
            }
            if let Some(t) = r.tier {
                node[ex("tier")] = json!(t);
            }
            let edges = [
                ("sibling", refs(r, code, item_of)),
                ("peer", refs(r, tier, item_of)),
                ("group", refs(r, code, group_of)),
                ("band", refs(r, code, band_of)),
                ("self", refs(r, id, item_of)),
                ("twin", refs(r, id, item_of)),
            ];
            for (pred, objects) in edges {
                if !objects.is_empty() {
                    node[ex(pred)] = Value::Array(objects);
                }
            }
            node
        })
        .collect()
}

/// Allowlist the fixtures directory, before anything builds Iceberg storage.
fn allow_fixture_roots() {
    // SAFETY: set before any storage or scan is built; every test in this
    // binary sets the same value.
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR")),
    );
}

/// Register the graph source and the native ledger holding its triples.
async fn seed(fluree: &Fluree) {
    let root = format!(
        "file://{}/tests/fixtures/iceberg/sets",
        env!("CARGO_MANIFEST_DIR")
    );
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct("fanout", &root, MAPPING)
                .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create graph source");
    let native = fluree
        .create_ledger(NATIVE)
        .await
        .expect("create native ledger");
    fluree
        .insert(native, &json!({ "@graph": mapped_graph() }))
        .await
        .expect("seed native ledger");
}

struct Case {
    name: &'static str,
    /// The SPARQL text; `{from}` names the dataset.
    sparql: String,
    /// The JSON-LD twin, without `from`.
    jsonld: Value,
    /// The output variables, in select order.
    vars: &'static [&'static str],
    /// Whether the fused aggregate must (`Some(true)`) or must not
    /// (`Some(false)`) serve the shape on the graph source.
    fused: Option<bool>,
    /// The native answer's row count, where pinning it checks the oracle.
    rows: Option<usize>,
}

fn case(name: &'static str, sparql: &str, jsonld: Value, vars: &'static [&'static str]) -> Case {
    Case {
        name,
        sparql: format!("PREFIX ex: <{EX}> {sparql}"),
        jsonld,
        vars,
        fused: None,
        rows: None,
    }
}

impl Case {
    fn fused(mut self, fires: bool) -> Self {
        self.fused = Some(fires);
        self
    }

    fn rows(mut self, n: usize) -> Self {
        self.rows = Some(n);
        self
    }
}

/// A term's text: an IRI, or a literal's lexical form.
fn text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Object(o) => o
            .get("value")
            .or_else(|| o.get("@id"))
            .or_else(|| o.get("@value"))
            .map(text)
            .unwrap_or_else(|| panic!("not a term: {v}")),
        Value::Array(_) => panic!("not a term: {v}"),
    }
}

/// A result's rows as the text of `vars`, sorted.
fn result_rows(result: &Value, vars: &[&str]) -> Vec<String> {
    let mut rows: Vec<String> = if let Some(bindings) = result["results"]["bindings"].as_array() {
        bindings
            .iter()
            .map(|b| {
                vars.iter()
                    .map(|v| text(&b[*v]))
                    .collect::<Vec<_>>()
                    .join(" | ")
            })
            .collect()
    } else {
        result
            .as_array()
            .unwrap_or_else(|| panic!("not a result: {result}"))
            .iter()
            .map(|row| match row {
                Value::Array(cells) => cells.iter().map(text).collect::<Vec<_>>().join(" | "),
                cell => text(cell),
            })
            .collect()
    };
    rows.sort();
    rows
}

/// One run's rows and whether the fused aggregate proceeded.
struct Run {
    rows: Vec<String>,
    fused: bool,
}

async fn run(
    fluree: &Fluree,
    store: &span_capture::SpanStore,
    c: &Case,
    from: &str,
    jsonld: bool,
    fast: bool,
) -> Run {
    set_fast_paths_disabled(!fast);
    let before = store.find_events("fast-path outcome").len();
    let result = if jsonld {
        let mut q = c.jsonld.clone();
        q["from"] = json!(from);
        fluree.query_from().jsonld(&q).execute_formatted().await
    } else {
        fluree
            .query_from()
            .sparql(&c.sparql.replace("{from}", from))
            .execute_formatted()
            .await
    };
    set_fast_paths_disabled(false);
    let rows = match result {
        Ok(v) => result_rows(&v, c.vars),
        Err(e) => vec![format!("ERROR {e}")],
    };
    let fused = store.find_events("fast-path outcome")[before..]
        .iter()
        .any(|e| {
            e.fields.get("site").map(String::as_str) == Some(FUSED)
                && e.fields.get("outcome").map(String::as_str) == Some("proceed")
        });
    Run { rows, fused }
}

/// Run every case on both surfaces over `from` (fast and generic) and over the
/// native ledger, and describe every disagreement.
async fn check(fluree: &Fluree, cases: &[Case], from: &str) -> Vec<String> {
    let (store, _guard) = span_capture::init_test_tracing();
    let mut failures = Vec::new();
    for c in cases {
        let mut problems = Vec::new();
        for (surface, jsonld) in [("SPARQL", false), ("JSON-LD", true)] {
            let native = run(fluree, &store, c, NATIVE, jsonld, true).await;
            if let Some(n) = c.rows {
                if native.rows.len() != n {
                    problems.push(format!(
                        "{surface}: the native answer has {} rows, not {n}: {:?}",
                        native.rows.len(),
                        native.rows
                    ));
                }
            }
            for fast in [true, false] {
                let lane = if fast { "fast" } else { "generic" };
                let got = run(fluree, &store, c, from, jsonld, fast).await;
                if got.rows != native.rows {
                    problems.push(format!(
                        "{surface} {lane}: {} rows {:?}, native {} rows {:?}",
                        got.rows.len(),
                        got.rows,
                        native.rows.len(),
                        native.rows
                    ));
                }
                if fast && from == SOURCE {
                    match (c.fused, got.fused) {
                        (Some(true), false) => {
                            problems.push(format!("{surface}: `{FUSED}` must fire"));
                        }
                        (Some(false), true) => {
                            problems.push(format!("{surface}: `{FUSED}` must not fire"));
                        }
                        _ => {}
                    }
                }
            }
        }
        if !problems.is_empty() {
            failures.push(format!("{}:\n  {}", c.name, problems.join("\n  ")));
        }
    }
    failures
}

fn select(vars: &[&str], where_clause: Value) -> Value {
    json!({ "select": vars.iter().map(|v| format!("?{v}")).collect::<Vec<_>>(), "where": where_clause })
}

fn values(var: &str, iris: &[String]) -> Value {
    json!([
        "values",
        [
            format!("?{var}"),
            iris.iter().map(|i| json!({ "@id": i })).collect::<Vec<_>>()
        ]
    ])
}

/// The shapes that read a referencing object map without aggregating.
fn pattern_cases() -> Vec<Case> {
    let sibling = ex("sibling");
    let peer = ex("peer");
    let name = ex("name");
    let some = [item(1), item(4), item(7)];
    vec![
        case(
            "a key matching several subjects joins each",
            "SELECT ?i ?s FROM <{from}> WHERE { ?i ex:sibling ?s }",
            select(&["i", "s"], json!({ "@id": "?i", sibling.clone(): "?s" })),
            &["i", "s"],
        )
        .rows(15),
        case(
            "a repeated key minting one subject joins it once",
            "SELECT ?i ?g FROM <{from}> WHERE { ?i ex:group ?g }",
            select(&["i", "g"], json!({ "@id": "?i", ex("group"): "?g" })),
            &["i", "g"],
        )
        .rows(7),
        case(
            "a key whose rows repeat subjects joins each distinct one",
            "SELECT ?i ?b FROM <{from}> WHERE { ?i ex:band ?b }",
            select(&["i", "b"], json!({ "@id": "?i", ex("band"): "?b" })),
            &["i", "b"],
        )
        .rows(11),
        case(
            "a unique key joins one subject",
            "SELECT ?i ?x FROM <{from}> WHERE { ?i ex:self ?x }",
            select(&["i", "x"], json!({ "@id": "?i", ex("self"): "?x" })),
            &["i", "x"],
        )
        .rows(8),
        case(
            "a star of two fan-outs is their cross product",
            "SELECT ?i ?s ?p FROM <{from}> WHERE { ?i ex:sibling ?s ; ex:peer ?p }",
            select(
                &["i", "s", "p"],
                json!({ "@id": "?i", sibling.clone(): "?s", peer.clone(): "?p" }),
            ),
            &["i", "s", "p"],
        )
        .rows(48),
        case(
            "a star of a column and a fan-out",
            "SELECT ?i ?n ?s FROM <{from}> WHERE { ?i ex:name ?n ; ex:sibling ?s }",
            select(
                &["i", "n", "s"],
                json!({ "@id": "?i", name.clone(): "?n", sibling.clone(): "?s" }),
            ),
            &["i", "n", "s"],
        ),
        case(
            "a bound object matches any subject of the key",
            "SELECT ?i FROM <{from}> WHERE { ?i ex:sibling <http://example.org/item/2> }",
            select(&["i"], json!({ "@id": "?i", sibling.clone(): { "@id": item(2) } })),
            &["i"],
        )
        .rows(2),
        case(
            "a bound object in a star matches any subject of the key",
            "SELECT ?i ?n FROM <{from}> WHERE { ?i ex:name ?n ; ex:sibling <http://example.org/item/5> }",
            select(
                &["i", "n"],
                json!({ "@id": "?i", name.clone(): "?n", sibling.clone(): { "@id": item(5) } }),
            ),
            &["i", "n"],
        )
        .rows(3),
        case(
            "a bound subject joins each subject of its key",
            "SELECT ?s FROM <{from}> WHERE { <http://example.org/item/4> ex:sibling ?s }",
            select(&["s"], json!({ "@id": item(4), sibling.clone(): "?s" })),
            &["s"],
        )
        .rows(3),
        // Code A sits in one of the two files, so the parent is read by a probe
        // on the key alone (code C spans both, so `item/4` reads it whole).
        case(
            "a bound subject whose key one file holds probes its parents",
            "SELECT ?s FROM <{from}> WHERE { <http://example.org/item/2> ex:sibling ?s }",
            select(&["s"], json!({ "@id": item(2), sibling.clone(): "?s" })),
            &["s"],
        )
        .rows(2),
        case(
            "a bound subject's wildcard",
            "SELECT ?p ?o FROM <{from}> WHERE { <http://example.org/item/1> ?p ?o }",
            select(&["p", "o"], json!({ "@id": item(1), "?p": "?o" })),
            &["p", "o"],
        ),
        case(
            "a wildcard",
            "SELECT ?i ?p ?o FROM <{from}> WHERE { ?i ?p ?o }",
            select(&["i", "p", "o"], json!({ "@id": "?i", "?p": "?o" })),
            &["i", "p", "o"],
        ),
        case(
            "a join through a fan-out",
            "SELECT ?i ?s ?n FROM <{from}> WHERE { ?i ex:sibling ?s . ?s ex:name ?n }",
            select(
                &["i", "s", "n"],
                json!([{ "@id": "?i", sibling.clone(): "?s" }, { "@id": "?s", name.clone(): "?n" }]),
            ),
            &["i", "s", "n"],
        ),
        case(
            "a VALUES subject set reads its keys' parents",
            &format!(
                "SELECT ?i ?s FROM <{{from}}> WHERE {{ VALUES ?i {{ {} }} ?i ex:sibling ?s }}",
                some.iter().map(|i| format!("<{i}>")).collect::<Vec<_>>().join(" ")
            ),
            select(
                &["i", "s"],
                json!([values("i", &some), { "@id": "?i", sibling.clone(): "?s" }]),
            ),
            &["i", "s"],
        )
        .rows(5),
        case(
            "one VALUES subject",
            "SELECT ?i ?s FROM <{from}> WHERE { VALUES ?i { <http://example.org/item/1> } ?i ex:sibling ?s }",
            select(
                &["i", "s"],
                json!([values("i", &[item(1)]), { "@id": "?i", sibling.clone(): "?s" }]),
            ),
            &["i", "s"],
        )
        .rows(2),
    ]
}

/// Aggregates over a referencing object map.
fn aggregate_cases() -> Vec<Case> {
    let grouped = |name: &'static str, pred: &str, rows: usize| {
        case(
            name,
            &format!(
                "SELECT ?o (COUNT(?i) AS ?n) FROM <{{from}}> WHERE {{ ?i ex:{pred} ?o }} GROUP BY ?o"
            ),
            json!({
                "select": ["?o", "(as (count ?i) ?n)"],
                "where": { "@id": "?i", ex(pred): "?o" },
                "groupBy": ["?o"],
            }),
            &["o", "n"],
        )
        .rows(rows)
    };
    let across = |name: &'static str, pred: &str| {
        case(
            name,
            &format!(
                "SELECT ?n (COUNT(?i) AS ?c) FROM <{{from}}> \
                 WHERE {{ ?i ex:{pred} ?s . ?s ex:name ?n }} GROUP BY ?n"
            ),
            json!({
                "select": ["?n", "(as (count ?i) ?c)"],
                "where": [{ "@id": "?i", ex(pred): "?s" }, { "@id": "?s", ex("name"): "?n" }],
                "groupBy": ["?n"],
            }),
            &["n", "c"],
        )
    };
    vec![
        grouped("GROUP BY a fan-out's subject", "sibling", 7).fused(false),
        grouped("GROUP BY a repeated key's one subject", "group", 4).fused(true),
        grouped("GROUP BY a unique key's subject", "self", 8).fused(true),
        across("GROUP BY a fan-out subject's column", "sibling").fused(false),
        across("GROUP BY a unique-key subject's column", "self").fused(true),
        case(
            "COUNT over a fan-out",
            "SELECT (COUNT(*) AS ?n) FROM <{from}> WHERE { ?i ex:sibling ?s }",
            json!({
                "select": ["(as (count ?s) ?n)"],
                "where": { "@id": "?i", ex("sibling"): "?s" },
            }),
            &["n"],
        )
        // An ungrouped aggregate over a ref object declines whatever the key.
        .fused(false)
        .rows(1),
        case(
            "a fan-out branch beside a grouped branch",
            "SELECT ?t (COUNT(?i) AS ?n) FROM <{from}> \
             WHERE { ?i ex:self ?x . ?x ex:tier ?t . ?i ex:sibling ?s . ?s ex:code \"A\" } GROUP BY ?t",
            json!({
                "select": ["?t", "(as (count ?i) ?n)"],
                "where": [
                    { "@id": "?i", ex("self"): "?x" },
                    { "@id": "?x", ex("tier"): "?t" },
                    { "@id": "?i", ex("sibling"): "?s" },
                    { "@id": "?s", ex("code"): "A" },
                ],
                "groupBy": ["?t"],
            }),
            &["t", "n"],
        )
        .fused(false),
        case(
            "a unique-key branch beside a grouped branch",
            "SELECT ?t (COUNT(?i) AS ?n) FROM <{from}> \
             WHERE { ?i ex:self ?x . ?x ex:tier ?t . ?i ex:twin ?s . ?s ex:code \"A\" } GROUP BY ?t",
            json!({
                "select": ["?t", "(as (count ?i) ?n)"],
                "where": [
                    { "@id": "?i", ex("self"): "?x" },
                    { "@id": "?x", ex("tier"): "?t" },
                    { "@id": "?i", ex("twin"): "?s" },
                    { "@id": "?s", ex("code"): "A" },
                ],
                "groupBy": ["?t"],
            }),
            &["t", "n"],
        )
        .fused(true),
    ]
}

/// Every shape over a non-unique parent key answers as the native ledger does,
/// on every lane.
#[tokio::test]
async fn a_referencing_map_joins_every_parent_its_key_matches() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let mut cases = pattern_cases();
    cases.extend(aggregate_cases());
    let failures = check(&fluree, &cases, SOURCE).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A scan whose parent fans out cuts its windows by the rows they produce, and
/// puts the rest back in scan order: with windows of two produced rows, every
/// shape still answers as the native ledger does.
#[tokio::test]
async fn windows_cut_by_produced_rows_keep_every_answer() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    struct Window;
    impl Drop for Window {
        fn drop(&mut self) {
            std::env::remove_var("FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS");
        }
    }
    // SAFETY: read per scan, and every test in this binary holds `SERIAL`.
    std::env::set_var("FLUREE_R2RML_MATERIALIZE_WINDOW_ROWS", "2");
    let _window = Window;
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let failures = check(&fluree, &pattern_cases(), SOURCE).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A `LIMIT` reads the parent by its first rows' keys, and answers rows of the
/// whole answer.
#[tokio::test]
async fn a_limit_over_a_fan_out_answers_rows_of_the_whole_answer() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;
    let (store, _guard) = span_capture::init_test_tracing();
    let whole = case(
        "whole",
        "SELECT ?i ?s FROM <{from}> WHERE { ?i ex:sibling ?s }",
        select(&["i", "s"], json!({ "@id": "?i", ex("sibling"): "?s" })),
        &["i", "s"],
    );
    let native = run(&fluree, &store, &whole, NATIVE, false, true).await.rows;
    let mut wrong = Vec::new();
    for k in [1usize, 2, 3, 5, 14, 15, 16] {
        let mut limited = case(
            "limited",
            &format!("SELECT ?i ?s FROM <{{from}}> WHERE {{ ?i ex:sibling ?s }} LIMIT {k}"),
            select(&["i", "s"], json!({ "@id": "?i", ex("sibling"): "?s" })),
            &["i", "s"],
        );
        limited.jsonld["limit"] = json!(k);
        for jsonld in [false, true] {
            let rows = run(&fluree, &store, &limited, SOURCE, jsonld, true)
                .await
                .rows;
            let distinct: BTreeSet<&String> = rows.iter().collect();
            if rows.len() != k.min(native.len())
                || distinct.len() != rows.len()
                || !rows.iter().all(|r| native.contains(r))
            {
                wrong.push(format!("LIMIT {k} (JSON-LD {jsonld}): {rows:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "native {native:?}\n{}", wrong.join("\n"));
}

/// The twin the bulk builder makes from the source holds every edge, whether
/// or not the old duplicate-key override is passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_twin_holds_every_edge_a_non_unique_key_joins() {
    let _serial = SERIAL.lock().await;
    allow_fixture_roots();
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree: &'static Fluree = Box::leak(Box::new(
        FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .expect("file-backed fluree"),
    ));
    seed(fluree).await;
    let provider = Arc::new(FlureeR2rmlProvider::new(fluree));
    fluree
        .create("fanout-twin:main")
        .import_r2rml(Arc::clone(&provider), SOURCE)
        .threads(2)
        .memory_budget_mb(256)
        .execute()
        .await
        .expect("the twin builds over a non-unique parent key");
    #[allow(deprecated)]
    let with_flag = fluree
        .create("fanout-twin-flag:main")
        .import_r2rml(Arc::clone(&provider), SOURCE)
        .threads(2)
        .memory_budget_mb(256)
        .allow_duplicate_parent_keys(true);
    with_flag
        .execute()
        .await
        .expect("the override is accepted and changes nothing");
    let cases = pattern_cases();
    for twin in ["fanout-twin:main", "fanout-twin-flag:main"] {
        let failures = check(fluree, &cases, twin).await;
        assert!(failures.is_empty(), "{twin}:\n{}", failures.join("\n"));
    }
}
