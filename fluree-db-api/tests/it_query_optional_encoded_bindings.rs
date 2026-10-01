//! OPTIONAL correlation and equality over encoded (late-materialized) bindings.
//!
//! On an indexed ledger with no live novelty, scans emit encoded bindings:
//! `EncodedSid` for subjects and ref objects, `EncodedPid` for predicates and
//! `EncodedLit` for literals. With novelty pending they emit decoded `Sid` /
//! `Lit` instead. The answer must not depend on which form a row carries:
//!
//! - a single-triple OPTIONAL must substitute an encoded correlation value into
//!   its pattern in every position (subject, predicate, object), so the lookup
//!   is bound instead of a scan of the whole predicate compared row by row;
//! - every equality surface must treat the encoded forms of one IRI as one term:
//!   the same IRI arrives as `EncodedPid` from a predicate position and as
//!   `EncodedSid` (or a decoded `Sid`) from a subject or object position.
//!
//! Every case runs in three index states and checks a hand-written expectation.
//! The novelty state, whose bindings are decoded, runs the same expectation as a
//! second oracle — not as the reference.
//!
//! - `Fresh`: loaded from storage, index covers every commit (overlay epoch 0);
//! - `Drained`: a write through the cached handle, then an index the handle
//!   adopted (novelty empty, epoch nonzero) — a long-running server's steady
//!   state, which #1967 moved onto the encoded lane;
//! - `Novelty`: a write the index does not cover yet.

use crate::support::rebuild_and_publish_index;
use crate::support::span_capture::{init_test_tracing, SpanStore};
use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerHandle};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";
const PREFIXES: &str = "PREFIX ex: <http://example.org/> \
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#> ";

/// Routing stamp of OPTIONAL's batched bound-object lane.
const OBJECT_PROBE_SITE: &str = "optional_object_probe";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Fresh,
    Drained,
    Novelty,
}

const STATES: [State; 3] = [State::Fresh, State::Drained, State::Novelty];

fn context() -> JsonValue {
    json!({
        "ex": EX,
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "skos": "http://www.w3.org/2004/02/skos/core#"
    })
}

/// Chunks derived from documents, concepts that are `subjectOf` chunks (the
/// reverse OPTIONAL of the reported delete), predicates that are also subjects or
/// objects (cross-position correlation), a small SKOS scheme (#1973), and a
/// bare-name copy of the chunk shape for Cypher.
fn fixture() -> JsonValue {
    json!({
        "@context": context(),
        "@graph": [
            {"@id": "ex:doc1", "@type": "ex:Doc", "ex:title": "Doc 1"},
            {"@id": "ex:doc2", "@type": "ex:Doc", "ex:title": "Doc 2"},
            {"@id": "ex:s1", "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "chunk one",
             "ex:position": 1, "ex:mentions": {"@id": "ex:derivedFrom"}},
            {"@id": "ex:s2", "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "chunk two",
             "ex:position": 2},
            {"@id": "ex:s3", "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "chunk three",
             "ex:position": 3},
            {"@id": "ex:s4", "ex:derivedFrom": {"@id": "ex:doc2"}, "ex:text": "chunk four",
             "ex:position": 4, "ex:usesPredicate": {"@id": "ex:text"}},
            {"@id": "ex:c1", "ex:subjectOf": {"@id": "ex:s1"}, "ex:alias": "chunk one", "ex:rank": 1},
            {"@id": "ex:c2", "ex:subjectOf": {"@id": "ex:s2"}},
            {"@id": "ex:c3", "ex:subjectOf": {"@id": "ex:s2"}, "ex:rank": 3},
            {"@id": "ex:c4", "ex:subjectOf": {"@id": "ex:s4"}},
            {"@id": "ex:derivedFrom", "rdfs:label": "derived from"},
            {"@id": "ex:text", "rdfs:label": "text"},
            {"@id": "ex:k0", "skos:inScheme": {"@id": "ex:scheme"}},
            {"@id": "ex:k1", "skos:inScheme": {"@id": "ex:scheme"}},
            {"@id": "ex:k2", "skos:inScheme": {"@id": "ex:scheme"}},
            {"@id": "ex:k10", "skos:broader": {"@id": "ex:k0"}},
            {"@id": "ex:k11", "skos:broader": {"@id": "ex:k0"}},
            {"@id": "ex:k12", "skos:broader": {"@id": "ex:k1"}},
            {"@id": "cdoc1", "@type": "CDoc", "title": "C Doc 1"},
            {"@id": "cs1", "derivedFrom": {"@id": "cdoc1"}},
            {"@id": "cs2", "derivedFrom": {"@id": "cdoc1"}},
            {"@id": "cc1", "subjectOf": {"@id": "cs1"}}
        ]
    })
}

/// A write that touches none of the queried data.
fn unrelated_write() -> JsonValue {
    json!({"@context": context(), "@id": "ex:unrelated", "ex:note": "novelty"})
}

async fn wait_for_index_t(handle: &LedgerHandle, t: i64) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if handle.snapshot().await.snapshot.t == t {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cached handle adopts the published index");
}

/// A fresh Fluree holding the fixture, with its cached handle in `state`.
async fn ledger_in(state: State, ledger_id: &str) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache genesis");
    fluree
        .stage(&handle)
        .insert(&fixture())
        .execute()
        .await
        .expect("seed fixture");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    wait_for_index_t(&handle, 1).await;

    let handle = match state {
        State::Drained => handle,
        State::Fresh | State::Novelty => {
            fluree.disconnect_ledger(ledger_id).await;
            fluree.ledger_cached(ledger_id).await.expect("reload")
        }
    };
    if state == State::Novelty {
        fluree
            .stage(&handle)
            .insert(&unrelated_write())
            .execute()
            .await
            .expect("pending write");
    }

    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "{state:?}: indexed");
    match state {
        State::Fresh => {
            assert!(view.novelty.is_empty(), "{state:?}: no novelty");
            assert_eq!(view.novelty.epoch, 0, "{state:?}: loaded at epoch 0");
        }
        State::Drained => {
            assert!(view.novelty.is_empty(), "{state:?}: novelty drained");
            assert_ne!(view.novelty.epoch, 0, "{state:?}: epoch survives the index");
        }
        State::Novelty => assert!(!view.novelty.is_empty(), "{state:?}: novelty pending"),
    }
    (fluree, handle)
}

/// One row per solution: each variable's value with `ex:` / the example
/// namespace stripped, `-` when unbound. Rows are sorted.
type Rows = Vec<Vec<String>>;

fn rows(expected: &[&[&str]]) -> Rows {
    let mut out: Rows = expected
        .iter()
        .map(|r| r.iter().map(|s| (*s).to_string()).collect())
        .collect();
    out.sort();
    out
}

fn short(value: &str) -> String {
    value
        .strip_prefix(EX)
        .or_else(|| value.strip_prefix("ex:"))
        .unwrap_or(value)
        .to_string()
}

fn sparql_rows(result: &JsonValue, vars: &[&str]) -> Rows {
    let bindings = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("SPARQL results: {result}"));
    let mut out: Rows = bindings
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| match b.get(*v) {
                    Some(term) => short(term["value"].as_str().expect("value")),
                    None => "-".to_string(),
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

fn json_value(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "-".to_string(),
        JsonValue::String(s) => short(s),
        JsonValue::Object(o) => match o.get("@id").or_else(|| o.get("@value")) {
            Some(inner) => json_value(inner),
            None => v.to_string(),
        },
        other => other.to_string(),
    }
}

fn jsonld_rows(result: &JsonValue) -> Rows {
    let mut out: Rows = result
        .as_array()
        .unwrap_or_else(|| panic!("JSON-LD rows: {result}"))
        .iter()
        .map(|row| match row {
            JsonValue::Array(cols) => cols.iter().map(json_value).collect(),
            single => vec![json_value(single)],
        })
        .collect();
    out.sort();
    out
}

async fn db(handle: &LedgerHandle) -> GraphDb {
    GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state())
}

async fn sparql(fluree: &Fluree, handle: &LedgerHandle, body: &str) -> JsonValue {
    let query = format!("{PREFIXES}{body}");
    db(handle)
        .await
        .query(fluree)
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{body}: {e}"))
}

async fn jsonld(fluree: &Fluree, handle: &LedgerHandle, query: &JsonValue) -> JsonValue {
    db(handle)
        .await
        .query(fluree)
        .jsonld(query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
}

/// Which slot of the OPTIONAL's triple carries the correlated variable.
#[derive(Clone, Copy, Debug)]
enum Slot {
    S,
    P,
    O,
}

/// The `TriplePattern` debug text of every binary scan opened since `before`
/// whose pattern names `marker` (a constant only the OPTIONAL's triple
/// contains, such as `name: "subjectOf"`).
fn scans_naming(store: &SpanStore, before: usize, marker: &str) -> Vec<String> {
    store.all_events()[before..]
        .iter()
        .filter(|e| e.message() == "BinaryScanOperator::open")
        .filter_map(|e| {
            e.fields
                .iter()
                .find(|(k, _)| k.ends_with("pattern"))
                .map(|(_, v)| v.clone())
        })
        .filter(|p| p.contains(marker))
        .collect()
}

/// The text of one slot of a debug-printed `TriplePattern`.
fn slot_text(pattern: &str, slot: Slot) -> &str {
    let start = |key: &str| {
        pattern
            .find(key)
            .unwrap_or_else(|| panic!("no `{key}` in {pattern}"))
            + key.len()
    };
    let (from, to) = match slot {
        Slot::S => (start("{ s: "), pattern.find(", p: ").expect("p slot")),
        Slot::P => (start(", p: "), pattern.find(", o: ").expect("o slot")),
        Slot::O => (start(", o: "), pattern.find(", dtc: ").expect("dtc slot")),
    };
    &pattern[from..to]
}

fn stamps(store: &SpanStore, before: usize, site: &str) -> Vec<String> {
    store.all_events()[before..]
        .iter()
        .filter(|e| e.message() == "fast-path outcome")
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

/// Every mismatch a test finds, reported together so one run shows the whole
/// picture rather than the first case that broke.
#[derive(Default)]
struct Failures(Vec<String>);

impl Failures {
    fn eq<T: PartialEq + std::fmt::Debug>(&mut self, got: T, want: T, label: &str) {
        if got != want {
            self.0
                .push(format!("{label}:\n    got  {got:?}\n    want {want:?}"));
        }
    }

    /// The OPTIONAL looked its triple up with the correlated slot bound: no scan
    /// of that triple left the slot a variable, and at least one bound lookup
    /// ran — a bound scan, or the batched bound-object lane.
    fn bound_lookup(
        &mut self,
        store: &SpanStore,
        before: usize,
        marker: &str,
        slot: Slot,
        label: &str,
    ) {
        let scans = scans_naming(store, before, marker);
        let free: Vec<&String> = scans
            .iter()
            .filter(|p| slot_text(p, slot).starts_with("Var("))
            .collect();
        if !free.is_empty() {
            self.0.push(format!(
                "{label}: OPTIONAL scanned its triple with the {slot:?} slot free: {free:?}"
            ));
        }
        let probe = stamps(store, before, OBJECT_PROBE_SITE);
        if scans.is_empty() && !probe.iter().any(|o| o == "proceed") {
            self.0.push(format!(
                "{label}: no bound lookup of the OPTIONAL's triple ran (probe stamps {probe:?})"
            ));
        }
    }

    /// The batched bound-object lane fired and never declined.
    fn object_probe_fired(&mut self, store: &SpanStore, before: usize, label: &str) {
        let seen = stamps(store, before, OBJECT_PROBE_SITE);
        if !seen.iter().any(|o| o == "proceed") || seen.iter().any(|o| o != "proceed") {
            self.0.push(format!(
                "{label}: `{OBJECT_PROBE_SITE}` must fire and never decline; stamps: {seen:?}"
            ));
        }
    }

    fn assert_none(self) {
        assert!(
            self.0.is_empty(),
            "{} failure(s):\n{}",
            self.0.len(),
            self.0.join("\n")
        );
    }
}

/// `(label, SPARQL body, projected variables, expected rows)`.
type SurfaceCase = (
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static [&'static [&'static str]],
);

struct OptionalCase {
    label: &'static str,
    body: &'static str,
    vars: &'static [&'static str],
    expected: &'static [&'static [&'static str]],
    marker: &'static str,
    slot: Slot,
    /// A ref-valued object correlation the batched lane must answer.
    object_probe: bool,
}

const OPTIONAL_CASES: &[OptionalCase] = &[
    OptionalCase {
        label: "object EncodedSid (reverse OPTIONAL, the reported delete's WHERE)",
        body: "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "c2"], &["s2", "c3"], &["s3", "-"]],
        marker: "name: \"subjectOf\"",
        slot: Slot::O,
        object_probe: true,
    },
    OptionalCase {
        // The chunks come out of a batched join, which emits `EncodedSid` even
        // with novelty pending, where the OPTIONAL's own scan decodes.
        label: "object EncodedSid from a batched join",
        body: "SELECT ?s ?c WHERE { ?d a ex:Doc ; ex:title \"Doc 1\" . ?s ex:derivedFrom ?d . \
               OPTIONAL { ?c ex:subjectOf ?s } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "c2"], &["s2", "c3"], &["s3", "-"]],
        marker: "name: \"subjectOf\"",
        slot: Slot::O,
        object_probe: true,
    },
    OptionalCase {
        label: "subject EncodedPid",
        body: "SELECT ?p ?label WHERE { ex:s1 ?p ?o . OPTIONAL { ?p rdfs:label ?label } }",
        vars: &["p", "label"],
        expected: &[
            &["derivedFrom", "derived from"],
            &["mentions", "-"],
            &["position", "-"],
            &["text", "text"],
        ],
        marker: "name: \"label\"",
        slot: Slot::S,
        object_probe: false,
    },
    OptionalCase {
        label: "object EncodedPid",
        body: "SELECT ?p ?x WHERE { ex:s1 ?p ?o . OPTIONAL { ?x ex:mentions ?p } }",
        vars: &["p", "x"],
        expected: &[
            &["derivedFrom", "s1"],
            &["mentions", "-"],
            &["position", "-"],
            &["text", "-"],
        ],
        marker: "name: \"mentions\"",
        slot: Slot::O,
        object_probe: false,
    },
    OptionalCase {
        label: "predicate EncodedSid",
        body: "SELECT ?q ?v WHERE { ex:s4 ex:usesPredicate ?q . OPTIONAL { ex:s1 ?q ?v } }",
        vars: &["q", "v"],
        expected: &[&["text", "chunk one"]],
        marker: "name: \"s1\"",
        slot: Slot::P,
        object_probe: false,
    },
    OptionalCase {
        label: "object EncodedLit (string)",
        body: "SELECT ?s ?c WHERE { ?s ex:text ?t . OPTIONAL { ?c ex:alias ?t } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "-"], &["s3", "-"], &["s4", "-"]],
        marker: "name: \"alias\"",
        slot: Slot::O,
        object_probe: false,
    },
    OptionalCase {
        label: "object EncodedLit (integer)",
        body: "SELECT ?s ?c WHERE { ?s ex:position ?n . OPTIONAL { ?c ex:rank ?n } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "-"], &["s3", "c3"], &["s4", "-"]],
        marker: "name: \"rank\"",
        slot: Slot::O,
        object_probe: false,
    },
    OptionalCase {
        // Literal and ref values of one variable: the batched lane declines and
        // each row is looked up on its own. Two rows with different encoded
        // objects (doc1, doc2) must not share one cached result.
        label: "mixed literal / EncodedSid objects (per-row cache keys)",
        body: "SELECT ?o ?s WHERE { { ?d ex:title ?o } UNION { ?o a ex:Doc } \
               OPTIONAL { ?s ex:derivedFrom ?o } }",
        vars: &["o", "s"],
        expected: &[
            &["Doc 1", "-"],
            &["Doc 2", "-"],
            &["doc1", "s1"],
            &["doc1", "s2"],
            &["doc1", "s3"],
            &["doc2", "s4"],
        ],
        marker: "name: \"derivedFrom\"",
        slot: Slot::O,
        object_probe: false,
    },
    OptionalCase {
        label: "#1973: skos:broader reverse OPTIONAL",
        body: "SELECT ?c ?child WHERE { ?c skos:inScheme ex:scheme . \
               OPTIONAL { ?child skos:broader ?c } }",
        vars: &["c", "child"],
        expected: &[&["k0", "k10"], &["k0", "k11"], &["k1", "k12"], &["k2", "-"]],
        marker: "name: \"broader\"",
        slot: Slot::O,
        object_probe: true,
    },
];

#[tokio::test(flavor = "current_thread")]
async fn optional_binds_encoded_values_in_every_position() {
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/optional-encoded:main").await;
        let (store, _guard) = init_test_tracing();
        for case in OPTIONAL_CASES {
            let label = format!("{state:?} / {}", case.label);
            let before = store.all_events().len();
            let result = sparql(&fluree, &handle, case.body).await;
            failures.eq(sparql_rows(&result, case.vars), rows(case.expected), &label);
            failures.bound_lookup(&store, before, case.marker, case.slot, &label);
            if case.object_probe {
                failures.object_probe_fired(&store, before, &label);
            }
        }
    }
    failures.assert_none();
}

#[tokio::test(flavor = "current_thread")]
async fn optional_binds_encoded_values_jsonld() {
    let cases = [
        (
            "object EncodedSid",
            json!({
                "@context": context(),
                "select": ["?s", "?c"],
                "where": [
                    {"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                    ["optional", {"@id": "?c", "ex:subjectOf": "?s"}]
                ]
            }),
            rows(&[&["s1", "c1"], &["s2", "c2"], &["s2", "c3"], &["s3", "-"]]),
            "name: \"subjectOf\"",
            Slot::O,
        ),
        (
            "subject EncodedPid",
            json!({
                "@context": context(),
                "select": ["?p", "?label"],
                "where": [
                    {"@id": "ex:s1", "?p": "?o"},
                    ["optional", {"@id": "?p", "rdfs:label": "?label"}]
                ]
            }),
            rows(&[
                &["derivedFrom", "derived from"],
                &["mentions", "-"],
                &["position", "-"],
                &["text", "text"],
            ]),
            "name: \"label\"",
            Slot::S,
        ),
        (
            "predicate EncodedSid",
            json!({
                "@context": context(),
                "select": ["?q", "?v"],
                "where": [
                    {"@id": "ex:s4", "ex:usesPredicate": "?q"},
                    ["optional", {"@id": "ex:s1", "?q": "?v"}]
                ]
            }),
            rows(&[&["text", "chunk one"]]),
            "name: \"s1\"",
            Slot::P,
        ),
        (
            // #1973's own JSON-LD spelling.
            "#1973 reverse OPTIONAL",
            json!({
                "@context": context(),
                "select": ["?c", "?child"],
                "where": [
                    {"@id": "?c", "skos:inScheme": {"@id": "ex:scheme"}},
                    ["optional", {"@id": "?child", "skos:broader": "?c"}]
                ]
            }),
            rows(&[&["k0", "k10"], &["k0", "k11"], &["k1", "k12"], &["k2", "-"]]),
            "name: \"broader\"",
            Slot::O,
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/optional-encoded-jsonld:main").await;
        let (store, _guard) = init_test_tracing();
        for (label, query, expected, marker, slot) in &cases {
            let label = format!("{state:?} / JSON-LD {label}");
            let before = store.all_events().len();
            let result = jsonld(&fluree, &handle, query).await;
            failures.eq(jsonld_rows(&result), expected.clone(), &label);
            failures.bound_lookup(&store, before, marker, *slot, &label);
        }
    }
    failures.assert_none();
}

/// The same IRI as `EncodedPid` (predicate position), `EncodedSid` (subject
/// position) and a decoded `Sid` (VALUES) is one term on every equality surface.
#[tokio::test(flavor = "current_thread")]
async fn equality_surfaces_treat_encoded_forms_of_one_iri_as_one_term() {
    let cases: &[SurfaceCase] = &[
        (
            "DISTINCT",
            "SELECT DISTINCT ?x WHERE { { ex:s1 ?x ?o } UNION { ?x rdfs:label ?l } }",
            &["x"],
            &[&["derivedFrom"], &["mentions"], &["position"], &["text"]],
        ),
        (
            "GROUP BY",
            "SELECT ?x (COUNT(*) AS ?n) WHERE { { ex:s1 ?x ?o } UNION { ?x rdfs:label ?l } } \
             GROUP BY ?x",
            &["x", "n"],
            &[
                &["derivedFrom", "2"],
                &["mentions", "1"],
                &["position", "1"],
                &["text", "2"],
            ],
        ),
        (
            "MINUS",
            "SELECT ?p WHERE { ex:s1 ?p ?o MINUS { ?p rdfs:label ?l } }",
            &["p"],
            &[&["mentions"], &["position"]],
        ),
        (
            "FILTER EXISTS",
            "SELECT ?p WHERE { ex:s1 ?p ?o FILTER EXISTS { ?p rdfs:label ?l } }",
            &["p"],
            &[&["derivedFrom"], &["text"]],
        ),
        (
            "FILTER NOT EXISTS",
            "SELECT ?p WHERE { ex:s1 ?p ?o FILTER NOT EXISTS { ?p rdfs:label ?l } }",
            &["p"],
            &[&["mentions"], &["position"]],
        ),
        (
            // ex:text is also a subject (it has a label); ex:position never is.
            "trailing VALUES",
            "SELECT ?p ?o WHERE { ex:s1 ?p ?o } VALUES ?p { ex:text ex:position }",
            &["p", "o"],
            &[&["position", "1"], &["text", "chunk one"]],
        ),
        (
            "join on a predicate and an object",
            "SELECT ?p ?x WHERE { ex:s1 ?p ?o . ?x ex:mentions ?p }",
            &["p", "x"],
            &[&["derivedFrom", "s1"]],
        ),
        (
            "COUNT(DISTINCT)",
            "SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE { { ex:s1 ?x ?o } UNION { ?x rdfs:label ?l } }",
            &["n"],
            &[&["4"]],
        ),
        // Surfaces that already treated the forms as one term; pinned so the
        // canonical form cannot break them.
        (
            "inner join on a predicate binding",
            "SELECT ?p ?l WHERE { ex:s1 ?p ?o . ?p rdfs:label ?l }",
            &["p", "l"],
            &[&["derivedFrom", "derived from"], &["text", "text"]],
        ),
        (
            "inner join on an object binding used as a predicate",
            "SELECT ?q ?v WHERE { ex:s4 ex:usesPredicate ?q . ex:s1 ?q ?v }",
            &["q", "v"],
            &[&["text", "chunk one"]],
        ),
        (
            "FILTER =",
            "SELECT ?p ?q WHERE { ex:s1 ?p ?o . ?q rdfs:label ?l FILTER(?p = ?q) }",
            &["p", "q"],
            &[&["derivedFrom", "derivedFrom"], &["text", "text"]],
        ),
        (
            "FILTER sameTerm",
            "SELECT ?p ?q WHERE { ex:s1 ?p ?o . ?q rdfs:label ?l FILTER(sameTerm(?p, ?q)) }",
            &["p", "q"],
            &[&["derivedFrom", "derivedFrom"], &["text", "text"]],
        ),
        (
            "leading VALUES",
            "SELECT ?p ?o WHERE { VALUES ?p { ex:text ex:position } ex:s1 ?p ?o }",
            &["p", "o"],
            &[&["position", "1"], &["text", "chunk one"]],
        ),
        (
            "subquery join",
            "SELECT ?p ?l WHERE { ex:s1 ?p ?o . { SELECT ?p ?l WHERE { ?p rdfs:label ?l } } }",
            &["p", "l"],
            &[&["derivedFrom", "derived from"], &["text", "text"]],
        ),
        (
            "group join",
            "SELECT ?p ?l WHERE { { ex:s1 ?p ?o } { ?p rdfs:label ?l } }",
            &["p", "l"],
            &[&["derivedFrom", "derived from"], &["text", "text"]],
        ),
        (
            "multi-pattern OPTIONAL",
            "SELECT ?p ?l WHERE { ex:s1 ?p ?o . \
             OPTIONAL { ?p rdfs:label ?l . FILTER(STRLEN(?l) > 0) } }",
            &["p", "l"],
            &[
                &["derivedFrom", "derived from"],
                &["mentions", "-"],
                &["position", "-"],
                &["text", "text"],
            ],
        ),
    ];
    let jsonld_cases = [
        (
            "JSON-LD selectDistinct",
            json!({
                "@context": context(),
                "selectDistinct": ["?x"],
                "where": [["union",
                    {"@id": "ex:s1", "?x": "?o"},
                    {"@id": "?x", "rdfs:label": "?l"}
                ]]
            }),
            rows(&[&["derivedFrom"], &["mentions"], &["position"], &["text"]]),
        ),
        (
            "JSON-LD minus",
            json!({
                "@context": context(),
                "select": ["?p"],
                "where": [
                    {"@id": "ex:s1", "?p": "?o"},
                    ["minus", {"@id": "?p", "rdfs:label": "?l"}]
                ]
            }),
            rows(&[&["mentions"], &["position"]]),
        ),
        (
            "JSON-LD exists",
            json!({
                "@context": context(),
                "select": ["?p"],
                "where": [
                    {"@id": "ex:s1", "?p": "?o"},
                    ["exists", {"@id": "?p", "rdfs:label": "?l"}]
                ]
            }),
            rows(&[&["derivedFrom"], &["text"]]),
        ),
        (
            "JSON-LD not-exists",
            json!({
                "@context": context(),
                "select": ["?p"],
                "where": [
                    {"@id": "ex:s1", "?p": "?o"},
                    ["not-exists", {"@id": "?p", "rdfs:label": "?l"}]
                ]
            }),
            rows(&[&["mentions"], &["position"]]),
        ),
        (
            "JSON-LD groupBy",
            json!({
                "@context": context(),
                "select": ["?x", "(as (count ?x) ?n)"],
                "where": [["union",
                    {"@id": "ex:s1", "?x": "?o"},
                    {"@id": "?x", "rdfs:label": "?l"}
                ]],
                "groupBy": ["?x"]
            }),
            rows(&[
                &["derivedFrom", "2"],
                &["mentions", "1"],
                &["position", "1"],
                &["text", "2"],
            ]),
        ),
        (
            "JSON-LD values",
            json!({
                "@context": context(),
                "select": ["?p", "?o"],
                "where": [{"@id": "ex:s1", "?p": "?o"}],
                "values": ["?p", [{"@id": "ex:text"}, {"@id": "ex:position"}]]
            }),
            rows(&[&["position", "1"], &["text", "chunk one"]]),
        ),
        (
            "JSON-LD inner join on a predicate binding",
            json!({
                "@context": context(),
                "select": ["?p", "?l"],
                "where": [
                    {"@id": "ex:s1", "?p": "?o"},
                    {"@id": "?p", "rdfs:label": "?l"}
                ]
            }),
            rows(&[&["derivedFrom", "derived from"], &["text", "text"]]),
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/equality-encoded:main").await;
        for (label, body, vars, expected) in cases {
            let result = sparql(&fluree, &handle, body).await;
            failures.eq(
                sparql_rows(&result, vars),
                rows(expected),
                &format!("{state:?} / {label}"),
            );
        }
        for (label, query, expected) in &jsonld_cases {
            let result = jsonld(&fluree, &handle, query).await;
            failures.eq(
                jsonld_rows(&result),
                expected.clone(),
                &format!("{state:?} / {label}"),
            );
        }
    }
    failures.assert_none();
}

/// The WHERE of a SPARQL UPDATE runs on the same lanes.
#[tokio::test(flavor = "current_thread")]
async fn update_where_optional_over_encoded_values() {
    let mut failures = Failures::default();
    for state in STATES {
        let (store, _guard) = init_test_tracing();

        // The reported delete: a document's chunks and the concept links into them.
        let (fluree, handle) = ledger_in(state, "it/update-optional-encoded:main").await;
        let before = store.all_events().len();
        fluree
            .stage(&handle)
            .sparql_update(&format!(
                "{PREFIXES} DELETE {{ ?s ?p ?o . ?c ex:subjectOf ?s }} \
                 WHERE {{ ?s ex:derivedFrom ex:doc1 . ?s ?p ?o . OPTIONAL {{ ?c ex:subjectOf ?s }} }}"
            ))
            .execute()
            .await
            .expect("delete document");
        let label = format!("{state:?} / UPDATE reverse OPTIONAL");
        failures.bound_lookup(&store, before, "name: \"subjectOf\"", Slot::O, &label);
        let links = sparql(
            &fluree,
            &handle,
            "SELECT ?c ?s WHERE { ?c ex:subjectOf ?s }",
        )
        .await;
        failures.eq(
            sparql_rows(&links, &["c", "s"]),
            rows(&[&["c4", "s4"]]),
            &format!("{label}: links into doc1's chunks are gone, doc2's stays"),
        );
        let chunks = sparql(
            &fluree,
            &handle,
            "SELECT ?s ?d WHERE { ?s ex:derivedFrom ?d }",
        )
        .await;
        failures.eq(
            sparql_rows(&chunks, &["s", "d"]),
            rows(&[&["s4", "doc2"]]),
            &format!("{label}: doc1's chunks are gone"),
        );
        let aliases = sparql(&fluree, &handle, "SELECT ?c ?a WHERE { ?c ex:alias ?a }").await;
        failures.eq(
            sparql_rows(&aliases, &["c", "a"]),
            rows(&[&["c1", "chunk one"]]),
            &format!("{label}: a concept keeps its other facts"),
        );

        // Cross-position: the predicates' labels, found through ?p.
        let (fluree, handle) = ledger_in(state, "it/update-optional-encoded-pid:main").await;
        let before = store.all_events().len();
        fluree
            .stage(&handle)
            .sparql_update(&format!(
                "{PREFIXES} DELETE {{ ?p rdfs:label ?l }} \
                 WHERE {{ ex:s1 ?p ?o . OPTIONAL {{ ?p rdfs:label ?l }} }}"
            ))
            .execute()
            .await
            .expect("delete labels");
        let label = format!("{state:?} / UPDATE subject EncodedPid OPTIONAL");
        failures.bound_lookup(&store, before, "name: \"label\"", Slot::S, &label);
        let labels = sparql(&fluree, &handle, "SELECT ?p ?l WHERE { ?p rdfs:label ?l }").await;
        failures.eq(
            sparql_rows(&labels, &["p", "l"]),
            rows(&[]),
            &format!("{label}: both labels were found and deleted"),
        );
    }
    failures.assert_none();
}

/// Cypher's OPTIONAL MATCH over a reverse edge is the same correlated OPTIONAL.
#[tokio::test(flavor = "current_thread")]
async fn cypher_optional_match_reverse_edge_over_encoded_values() {
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/cypher-optional-encoded:main").await;
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let db = db(&handle).await;
        let result = fluree
            .query_cypher(
                &db,
                "MATCH (s)-[:derivedFrom]->(d:CDoc) OPTIONAL MATCH (c)-[:subjectOf]->(s) RETURN s, c",
            )
            .await
            .expect("cypher query")
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        let label = format!("{state:?} / Cypher OPTIONAL MATCH");
        failures.eq(
            jsonld_rows(&result),
            rows(&[&["cs1", "cc1"], &["cs2", "-"]]),
            &label,
        );
        failures.bound_lookup(&store, before, "name: \"subjectOf\"", Slot::O, &label);

        let counted = fluree
            .query_cypher(
                &db,
                "MATCH (s)-[:derivedFrom]->(d:CDoc) OPTIONAL MATCH (c)-[:subjectOf]->(s) \
                 RETURN s, count(c) AS n",
            )
            .await
            .expect("cypher count")
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        failures.eq(
            jsonld_rows(&counted),
            rows(&[&["cs1", "1"], &["cs2", "0"]]),
            &format!("{state:?} / Cypher count over OPTIONAL MATCH"),
        );
    }
    failures.assert_none();
}

/// #1320: an EXISTS inside an expression (here one arm of `||`, which keeps it
/// an `Expression::Exists` for `FilterOperator`) is answered from the cached
/// subject set of its predicate on an overlay-free index. Rows whose subject
/// arrives encoded used to decline that cache and seed one scan each.
#[tokio::test(flavor = "current_thread")]
async fn exists_in_an_expression_answers_encoded_subjects_from_the_cache() {
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/exists-encoded-cache:main").await;
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = sparql(
            &fluree,
            &handle,
            "SELECT ?s WHERE { ?s ex:derivedFrom ex:doc1 . \
             FILTER(EXISTS { ?s ex:mentions ?m } || ?s = ex:s3) }",
        )
        .await;
        let label = format!("{state:?} / EXISTS in an expression");
        failures.eq(
            sparql_rows(&result, &["s"]),
            rows(&[&["s1"], &["s3"]]),
            &label,
        );
        if state != State::Novelty {
            // The cache is built only with no live novelty. A row the cache
            // declines is answered by planning and running the EXISTS body
            // seeded with that row: a nested-loop join opened per row.
            let per_row = store.all_events()[before..]
                .iter()
                .filter(|e| e.message() == "opened nested loop join")
                .count();
            if per_row != 0 {
                failures.0.push(format!(
                    "{label}: {per_row} row(s) declined the cache to a seeded EXISTS"
                ));
            }
            let seen = stamps(&store, before, "exists_semijoin");
            if !seen.iter().any(|o| o == "proceed") {
                failures.0.push(format!(
                    "{label}: the semijoin cache was not built: {seen:?}"
                ));
            }
        }
    }
    failures.assert_none();
}
