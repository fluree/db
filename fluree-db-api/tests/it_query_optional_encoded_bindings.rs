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
    ledger_with(state, ledger_id, &fixture()).await
}

/// A fresh Fluree holding `data`, indexed, with its cached handle in `state`.
async fn ledger_with(state: State, ledger_id: &str, data: &JsonValue) -> (Fluree, LedgerHandle) {
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
        .insert(data)
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
    if value.starts_with("_:") {
        return "BLANK".to_string();
    }
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
                    Some(term) if term["type"].as_str() == Some("bnode") => "BLANK".to_string(),
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
        self.object_probe(store, before, Probe::Fires, label);
    }

    /// The batched bound-object lane did what `want` says since `before`.
    fn object_probe(&mut self, store: &SpanStore, before: usize, want: Probe, label: &str) {
        let seen = stamps(store, before, OBJECT_PROBE_SITE);
        let proceeded = seen.iter().any(|o| o == "proceed");
        let declined = seen.iter().any(|o| o != "proceed");
        let ok = match want {
            Probe::Fires => proceeded && !declined,
            Probe::Declines => declined && !proceeded,
            Probe::DeclinesSome => declined,
            Probe::Absent => seen.is_empty(),
        };
        if !ok {
            self.0.push(format!(
                "{label}: `{OBJECT_PROBE_SITE}` must be {want:?}; stamps: {seen:?}"
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

/// What OPTIONAL's batched bound-object lane must do for a query.
#[derive(Clone, Copy, Debug)]
enum Probe {
    /// Answer every window: `proceed`, never a fallback.
    Fires,
    /// Decline and never answer: every window holds an object it cannot
    /// probe (a literal), or the lane is not admitted.
    Declines,
    /// Decline each window holding an object it cannot probe (a literal, an
    /// IRI bound as a predicate) and answer the others: at least one decline.
    DeclinesSome,
    /// Never consulted: the OPTIONAL's object is not its only correlation, or
    /// the lane is not admitted.
    Absent,
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
    /// A constant only the OPTIONAL's scans name, to check their correlated
    /// slot; `None` when the scan is a subject seek by design.
    marker: Option<&'static str>,
    slot: Slot,
    /// What the batched bound-object lane must do.
    probe: Probe,
}

const OPTIONAL_CASES: &[OptionalCase] = &[
    OptionalCase {
        label: "object EncodedSid (reverse OPTIONAL, the reported delete's WHERE)",
        body: "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "c2"], &["s2", "c3"], &["s3", "-"]],
        marker: Some("name: \"subjectOf\""),
        slot: Slot::O,
        probe: Probe::Fires,
    },
    OptionalCase {
        // The chunks come out of a batched join, which emits `EncodedSid` even
        // with novelty pending, where the OPTIONAL's own scan decodes.
        label: "object EncodedSid from a batched join",
        body: "SELECT ?s ?c WHERE { ?d a ex:Doc ; ex:title \"Doc 1\" . ?s ex:derivedFrom ?d . \
               OPTIONAL { ?c ex:subjectOf ?s } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "c2"], &["s2", "c3"], &["s3", "-"]],
        marker: Some("name: \"subjectOf\""),
        slot: Slot::O,
        probe: Probe::Fires,
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
        marker: Some("name: \"label\""),
        slot: Slot::S,
        probe: Probe::Absent,
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
        marker: Some("name: \"mentions\""),
        slot: Slot::O,
        probe: Probe::DeclinesSome,
    },
    OptionalCase {
        label: "predicate EncodedSid",
        body: "SELECT ?q ?v WHERE { ex:s4 ex:usesPredicate ?q . OPTIONAL { ex:s1 ?q ?v } }",
        vars: &["q", "v"],
        expected: &[&["text", "chunk one"]],
        marker: Some("name: \"s1\""),
        slot: Slot::P,
        probe: Probe::Absent,
    },
    OptionalCase {
        label: "object EncodedLit (string)",
        body: "SELECT ?s ?c WHERE { ?s ex:text ?t . OPTIONAL { ?c ex:alias ?t } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "-"], &["s3", "-"], &["s4", "-"]],
        marker: Some("name: \"alias\""),
        slot: Slot::O,
        probe: Probe::Declines,
    },
    OptionalCase {
        label: "object EncodedLit (integer)",
        body: "SELECT ?s ?c WHERE { ?s ex:position ?n . OPTIONAL { ?c ex:rank ?n } }",
        vars: &["s", "c"],
        expected: &[&["s1", "c1"], &["s2", "-"], &["s3", "c3"], &["s4", "-"]],
        marker: Some("name: \"rank\""),
        slot: Slot::O,
        probe: Probe::Declines,
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
        marker: Some("name: \"derivedFrom\""),
        slot: Slot::O,
        probe: Probe::DeclinesSome,
    },
    OptionalCase {
        // Subject and object both come from the required side: the scan seeks
        // the subject and `unify_check` matches the encoded object.
        label: "subject and object both correlated",
        body: "SELECT ?s ?o ?p WHERE { ?s ex:derivedFrom ?o . OPTIONAL { ?s ?p ?o } }",
        vars: &["s", "o", "p"],
        expected: &[
            &["s1", "doc1", "derivedFrom"],
            &["s2", "doc1", "derivedFrom"],
            &["s3", "doc1", "derivedFrom"],
            &["s4", "doc2", "derivedFrom"],
        ],
        marker: None,
        slot: Slot::S,
        probe: Probe::Absent,
    },
    OptionalCase {
        // With novelty pending the batched joins emit `?s` and `?c` encoded
        // while the OPTIONAL's own scan decodes `?s`: the unify compares two
        // forms of one IRI.
        label: "both correlated, object matched across forms",
        body: "SELECT ?s ?c ?p WHERE { ?d a ex:Doc ; ex:title \"Doc 1\" . \
               ?s ex:derivedFrom ?d . ?c ex:subjectOf ?s . OPTIONAL { ?c ?p ?s } }",
        vars: &["s", "c", "p"],
        expected: &[
            &["s1", "c1", "subjectOf"],
            &["s2", "c2", "subjectOf"],
            &["s2", "c3", "subjectOf"],
        ],
        marker: None,
        slot: Slot::S,
        probe: Probe::Absent,
    },
    OptionalCase {
        label: "#1973: skos:broader reverse OPTIONAL",
        body: "SELECT ?c ?child WHERE { ?c skos:inScheme ex:scheme . \
               OPTIONAL { ?child skos:broader ?c } }",
        vars: &["c", "child"],
        expected: &[&["k0", "k10"], &["k0", "k11"], &["k1", "k12"], &["k2", "-"]],
        marker: Some("name: \"broader\""),
        slot: Slot::O,
        probe: Probe::Fires,
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
            if let Some(marker) = case.marker {
                failures.bound_lookup(&store, before, marker, case.slot, &label);
            }
            failures.object_probe(&store, before, case.probe, &label);
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
            Probe::Fires,
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
            Probe::Absent,
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
            Probe::Absent,
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
            Probe::Fires,
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/optional-encoded-jsonld:main").await;
        let (store, _guard) = init_test_tracing();
        for (label, query, expected, marker, slot, probe) in &cases {
            let label = format!("{state:?} / JSON-LD {label}");
            let before = store.all_events().len();
            let result = jsonld(&fluree, &handle, query).await;
            failures.eq(jsonld_rows(&result), expected.clone(), &label);
            failures.bound_lookup(&store, before, marker, *slot, &label);
            failures.object_probe(&store, before, *probe, &label);
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
        failures.object_probe(&store, before, update_where_probe(state), &label);
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

/// The bound-object lane inside an UPDATE's WHERE: a transaction evaluates
/// its WHERE over a one-member dataset, which the batched probe lanes'
/// shared admission declines once novelty is pending. The per-row lookups
/// answer then, bound as the test's other checks require.
fn update_where_probe(state: State) -> Probe {
    match state {
        State::Fresh | State::Drained => Probe::Fires,
        State::Novelty => Probe::Declines,
    }
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
        failures.object_probe_fired(&store, before, &label);

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

// ---------------------------------------------------------------------------
// Novelty that touches the queried predicates.
//
// With novelty pending a plain scan decodes every term, while a batched lane
// (OPTIONAL's bound-object lane, the join's batched lanes) binds a term by its
// id: a subject minted since the last index by its novelty id, a persisted
// blank node by its subject id. Every equality surface must see both forms as
// one term. The `Novelty` state above writes an unrelated predicate, so these
// lanes never meet a novelty id there.
// ---------------------------------------------------------------------------

/// Chunks of `ex:doc1` and the concepts that are `subjectOf` them; optionally
/// a blank-node concept of `ex:s2` (both with an alias) and a blank-node chunk.
fn chunks_base(blank_concept: bool, blank_chunk: bool) -> JsonValue {
    let mut graph = vec![
        json!({"@id": "ex:doc1", "@type": "ex:Doc", "ex:title": "Doc 1"}),
        json!({"@id": "ex:s1", "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "one"}),
        json!({"@id": "ex:s2", "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "two"}),
        json!({"@id": "ex:s3", "ex:derivedFrom": {"@id": "ex:doc1"}}),
        json!({"@id": "ex:c1", "ex:subjectOf": {"@id": "ex:s1"}, "ex:alias": "c one"}),
        json!({"@id": "ex:c2", "ex:subjectOf": {"@id": "ex:s2"}}),
    ];
    if blank_concept {
        graph.push(json!({"ex:subjectOf": {"@id": "ex:s2"}, "ex:alias": "blank alias"}));
    }
    if blank_chunk {
        graph.push(json!({"ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "blank chunk"}));
    }
    json!({"@context": context(), "@graph": graph})
}

/// `base` indexed and reloaded from storage, then `novelty` written and left
/// pending.
async fn indexed_then_novelty(
    ledger_id: &str,
    base: &JsonValue,
    novelty: &JsonValue,
) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .insert(base)
        .execute()
        .await
        .expect("seed");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
    fluree
        .stage(&handle)
        .insert(novelty)
        .execute()
        .await
        .expect("pending write");
    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "indexed");
    assert!(!view.novelty.is_empty(), "novelty pending");
    (fluree, handle)
}

/// A concept minted since the last index, `subjectOf` an indexed chunk.
fn novelty_concept() -> JsonValue {
    json!({"@context": context(), "@id": "ex:cN",
        "ex:subjectOf": {"@id": "ex:s1"}, "ex:alias": "c new"})
}

/// OPTIONAL's bound-object lane binds `?c = ex:cN` by its novelty id; MINUS,
/// DISTINCT, COUNT(DISTINCT), GROUP BY, sameTerm, `=` and a trailing VALUES
/// meet the same IRI decoded by a scan.
#[tokio::test(flavor = "current_thread")]
async fn optional_lane_novelty_subject_is_one_term_with_its_decoded_form() {
    let (fluree, handle) = indexed_then_novelty(
        "it/optional-lane-novelty-subject:main",
        &chunks_base(false, false),
        &novelty_concept(),
    )
    .await;
    let (store, _guard) = init_test_tracing();
    let mut failures = Failures::default();
    let cases: &[SurfaceCase] = &[
        (
            "plain",
            "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } }",
            &["s", "c"],
            &[&["s1", "c1"], &["s1", "cN"], &["s2", "c2"], &["s3", "-"]],
        ),
        (
            "MINUS",
            "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } \
             MINUS { ?c ex:alias ?a } }",
            &["s", "c"],
            &[&["s2", "c2"], &["s3", "-"]],
        ),
        (
            "DISTINCT",
            "SELECT DISTINCT ?c WHERE { { ?s ex:derivedFrom ex:doc1 . \
             OPTIONAL { ?c ex:subjectOf ?s } } UNION { ?c ex:alias ?a } }",
            &["c"],
            &[&["-"], &["c1"], &["c2"], &["cN"]],
        ),
        (
            "COUNT(DISTINCT)",
            "SELECT (COUNT(DISTINCT ?c) AS ?n) WHERE { { ?s ex:derivedFrom ex:doc1 . \
             OPTIONAL { ?c ex:subjectOf ?s } } UNION { ?c ex:alias ?a } }",
            &["n"],
            &[&["3"]],
        ),
        (
            "GROUP BY",
            "SELECT ?c (COUNT(*) AS ?n) WHERE { { ?s ex:derivedFrom ex:doc1 . \
             OPTIONAL { ?c ex:subjectOf ?s } FILTER(BOUND(?c)) } UNION { ?c ex:alias ?a } } \
             GROUP BY ?c",
            &["c", "n"],
            &[&["c1", "2"], &["c2", "1"], &["cN", "2"]],
        ),
        (
            "sameTerm",
            "SELECT ?s ?c ?x WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } \
             ?x ex:alias ?a FILTER(sameTerm(?c, ?x)) }",
            &["s", "c", "x"],
            &[&["s1", "c1", "c1"], &["s1", "cN", "cN"]],
        ),
        (
            "FILTER =",
            "SELECT ?s ?c ?x WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } \
             ?x ex:alias ?a FILTER(?c = ?x) }",
            &["s", "c", "x"],
            &[&["s1", "c1", "c1"], &["s1", "cN", "cN"]],
        ),
        (
            "trailing VALUES",
            "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } } \
             VALUES ?c { ex:cN }",
            &["s", "c"],
            &[&["s1", "cN"], &["s3", "cN"]],
        ),
    ];
    for (label, body, vars, expected) in cases {
        let label = format!("novelty-minted ?c / {label}");
        let before = store.all_events().len();
        let result = sparql(&fluree, &handle, body).await;
        failures.eq(sparql_rows(&result, vars), rows(expected), &label);
        failures.object_probe_fired(&store, before, &label);
    }

    let jsonld_cases = [
        (
            "JSON-LD MINUS",
            json!({
                "@context": context(),
                "select": ["?s", "?c"],
                "where": [
                    {"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                    ["optional", {"@id": "?c", "ex:subjectOf": "?s"}],
                    ["minus", {"@id": "?c", "ex:alias": "?a"}]
                ]
            }),
            rows(&[&["s2", "c2"], &["s3", "-"]]),
        ),
        (
            "JSON-LD selectDistinct",
            json!({
                "@context": context(),
                "selectDistinct": ["?c"],
                "where": [
                    ["union",
                        [{"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                         ["optional", {"@id": "?c", "ex:subjectOf": "?s"}]],
                        [{"@id": "?c", "ex:alias": "?a"}]]
                ]
            }),
            rows(&[&["-"], &["c1"], &["c2"], &["cN"]]),
        ),
    ];
    for (label, query, expected) in &jsonld_cases {
        let label = format!("novelty-minted ?c / {label}");
        let before = store.all_events().len();
        let result = jsonld(&fluree, &handle, query).await;
        failures.eq(jsonld_rows(&result), expected.clone(), &label);
        failures.object_probe_fired(&store, before, &label);
    }
    failures.assert_none();
}

/// OPTIONAL's bound-object lane binds a persisted blank node by its subject
/// id; MINUS and COUNT(DISTINCT) meet it decoded by a scan.
#[tokio::test(flavor = "current_thread")]
async fn optional_lane_blank_node_is_one_term_with_its_decoded_form() {
    let (fluree, handle) = indexed_then_novelty(
        "it/optional-lane-blank-node:main",
        &chunks_base(true, false),
        &unrelated_write(),
    )
    .await;
    let (store, _guard) = init_test_tracing();
    let mut failures = Failures::default();
    let cases: &[SurfaceCase] = &[
        (
            "plain",
            "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } }",
            &["s", "c"],
            &[&["s1", "c1"], &["s2", "BLANK"], &["s2", "c2"], &["s3", "-"]],
        ),
        (
            "MINUS",
            "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } \
             MINUS { ?c ex:alias ?a } }",
            &["s", "c"],
            &[&["s2", "c2"], &["s3", "-"]],
        ),
        (
            "COUNT(DISTINCT)",
            "SELECT (COUNT(DISTINCT ?c) AS ?n) WHERE { { ?s ex:derivedFrom ex:doc1 . \
             OPTIONAL { ?c ex:subjectOf ?s } } UNION { ?c ex:alias ?a } }",
            &["n"],
            &[&["3"]],
        ),
    ];
    for (label, body, vars, expected) in cases {
        let label = format!("blank-node ?c / {label}");
        let before = store.all_events().len();
        let result = sparql(&fluree, &handle, body).await;
        failures.eq(sparql_rows(&result, vars), rows(expected), &label);
        failures.object_probe_fired(&store, before, &label);
    }
    failures.assert_none();
}

/// The join's batched lane binds a novelty-minted chunk and a persisted
/// blank-node chunk by id; MINUS and COUNT(DISTINCT) meet them decoded.
#[tokio::test(flavor = "current_thread")]
async fn join_lane_novelty_and_blank_subjects_are_one_term_with_their_decoded_forms() {
    let novelty = json!({"@context": context(), "@id": "ex:sN",
        "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:text": "new"});
    let (fluree, handle) = indexed_then_novelty(
        "it/join-lane-novelty-blank:main",
        &chunks_base(false, true),
        &novelty,
    )
    .await;
    let mut failures = Failures::default();
    let cases: &[SurfaceCase] = &[
        (
            "MINUS",
            "SELECT ?s WHERE { ?d a ex:Doc ; ex:title \"Doc 1\" . ?s ex:derivedFrom ?d \
             MINUS { ?s ex:text ?t } }",
            &["s"],
            &[&["s3"]],
        ),
        (
            // s1 s2 s3 sN and the blank chunk.
            "COUNT(DISTINCT)",
            "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { { ?d a ex:Doc ; ex:title \"Doc 1\" . \
             ?s ex:derivedFrom ?d } UNION { ?s ex:text ?t } }",
            &["n"],
            &[&["5"]],
        ),
    ];
    for (label, body, vars, expected) in cases {
        let result = sparql(&fluree, &handle, body).await;
        failures.eq(
            sparql_rows(&result, vars),
            rows(expected),
            &format!("join lane / {label}"),
        );
    }
    failures.assert_none();
}

/// Both OPTIONAL slots correlated after batched joins, the correlated IRIs
/// minted in novelty: the joins bind `?s`/`?c` by novelty id, OPTIONAL seeks
/// the subject and unifies the object with the decoded value its scan binds.
#[tokio::test(flavor = "current_thread")]
async fn optional_unify_matches_a_novelty_minted_object() {
    let novelty = json!({"@context": context(), "@graph": [
        {"@id": "ex:sN", "ex:derivedFrom": {"@id": "ex:doc1"}},
        {"@id": "ex:cX", "ex:subjectOf": {"@id": "ex:sN"}}
    ]});
    let (fluree, handle) = indexed_then_novelty(
        "it/optional-unify-novelty-object:main",
        &chunks_base(false, false),
        &novelty,
    )
    .await;
    let mut failures = Failures::default();
    let result = sparql(
        &fluree,
        &handle,
        "SELECT ?s ?c ?p WHERE { ?d a ex:Doc ; ex:title \"Doc 1\" . ?s ex:derivedFrom ?d . \
         ?c ex:subjectOf ?s . OPTIONAL { ?c ?p ?s } }",
    )
    .await;
    failures.eq(
        sparql_rows(&result, &["s", "c", "p"]),
        rows(&[
            &["s1", "c1", "subjectOf"],
            &["s2", "c2", "subjectOf"],
            &["sN", "cX", "subjectOf"],
        ]),
        "both slots correlated, novelty-minted IRIs",
    );
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// A dataset of several graphs of one ledger.
//
// With two default graphs no graph view exists, and the member scans bind
// every value decoded (#2004). The answers must be the single-graph ones: a
// correlated OPTIONAL bound into its lookup, a join correlated rather than
// repeated, and arena-backed literals that share a handle across the two
// graphs kept apart.
// ---------------------------------------------------------------------------

/// Chunks and concepts spread over two named graphs, and `xsd:decimal`
/// amounts (arena-backed literals) in both, indexed. An arena handle counts
/// from 0 per graph and predicate, so g1's 1.5 and g2's 9.5 share a handle.
async fn two_graph_ledger(ledger_id: &str) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    let trig = r#"
        @prefix ex: <http://example.org/> .
        ex:root ex:note "default graph" .
        GRAPH <urn:g1> {
            ex:s1 ex:derivedFrom ex:doc1 . ex:c1 ex:subjectOf ex:s1 .
            ex:x1 ex:amount 1.5 . ex:x2 ex:amount 9.5 .
        }
        GRAPH <urn:g2> {
            ex:s2 ex:derivedFrom ex:doc1 . ex:c2 ex:subjectOf ex:s2 .
            ex:s3 ex:derivedFrom ex:doc1 .
            ex:y1 ex:amount 9.5 .
        }
    "#;
    fluree
        .stage(&handle)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("seed TriG");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "indexed");
    assert!(view.novelty.is_empty(), "no novelty");
    (fluree, handle)
}

/// `(label, SPARQL body, projected variables, expected rows, the correlated
/// triple's marker and slot when its lookup must be bound)`.
type CorrelatedCase = (
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static [&'static [&'static str]],
    Option<(&'static str, Slot)>,
);

#[tokio::test(flavor = "current_thread")]
async fn correlation_across_two_default_graphs_of_one_ledger() {
    let (fluree, handle) = two_graph_ledger("it/correlation-two-graphs:main").await;
    let (store, _guard) = init_test_tracing();
    let cases: &[CorrelatedCase] = &[
        (
            "OPTIONAL correlated on its object",
            "SELECT ?s ?c FROM <urn:g1> FROM <urn:g2> WHERE { ?s ex:derivedFrom ex:doc1 . \
             OPTIONAL { ?c ex:subjectOf ?s } }",
            &["s", "c"],
            &[&["s1", "c1"], &["s2", "c2"], &["s3", "-"]],
            Some(("name: \"subjectOf\"", Slot::O)),
        ),
        (
            "OPTIONAL correlated on its subject",
            "SELECT ?c ?s ?d FROM <urn:g1> FROM <urn:g2> WHERE { ?c ex:subjectOf ?s . \
             OPTIONAL { ?s ex:derivedFrom ?d } }",
            &["c", "s", "d"],
            &[&["c1", "s1", "doc1"], &["c2", "s2", "doc1"]],
            Some(("name: \"derivedFrom\"", Slot::S)),
        ),
        (
            // Either side may drive; a slot left free must still be
            // correlated rather than match every row.
            "join on an encoded IRI",
            "SELECT ?s ?c FROM <urn:g1> FROM <urn:g2> WHERE { ?s ex:derivedFrom ex:doc1 . \
             ?c ex:subjectOf ?s }",
            &["s", "c"],
            &[&["s1", "c1"], &["s2", "c2"]],
            None,
        ),
        (
            // Arena-backed literals (big numbers): their handles name a value
            // only within one graph, and g1's 1.5 shares a handle with g2's
            // 9.5. The member scans bind every value decoded.
            "projection of an arena-backed literal",
            "SELECT ?x ?v FROM <urn:g1> FROM <urn:g2> WHERE { ?x ex:amount ?v }",
            &["x", "v"],
            &[&["x1", "1.5"], &["x2", "9.5"], &["y1", "9.5"]],
            None,
        ),
        (
            "join on an arena-backed literal",
            "SELECT ?x ?y FROM <urn:g1> FROM <urn:g2> WHERE { ?x ex:amount ?v . \
             ?y ex:amount ?v }",
            &["x", "y"],
            &[
                &["x1", "x1"],
                &["x2", "x2"],
                &["x2", "y1"],
                &["y1", "x2"],
                &["y1", "y1"],
            ],
            None,
        ),
        (
            "OPTIONAL on an arena-backed literal",
            "SELECT ?x ?y FROM <urn:g1> FROM <urn:g2> WHERE { ?x ex:amount ?v . \
             OPTIONAL { ?y ex:amount ?v } }",
            &["x", "y"],
            &[
                &["x1", "x1"],
                &["x2", "x2"],
                &["x2", "y1"],
                &["y1", "x2"],
                &["y1", "y1"],
            ],
            None,
        ),
        (
            "MINUS on an arena-backed literal",
            "SELECT ?x FROM <urn:g1> FROM <urn:g2> WHERE { ?x ex:amount ?v \
             MINUS { ex:y1 ex:amount ?v } }",
            &["x"],
            &[&["x1"]],
            None,
        ),
        (
            "GROUP BY an arena-backed literal",
            "SELECT ?v (COUNT(*) AS ?n) FROM <urn:g1> FROM <urn:g2> \
             WHERE { ?x ex:amount ?v } GROUP BY ?v",
            &["v", "n"],
            &[&["1.5", "1"], &["9.5", "2"]],
            None,
        ),
        (
            "DISTINCT across both graphs",
            "SELECT DISTINCT ?s FROM <urn:g1> FROM <urn:g2> WHERE { { ?s ex:derivedFrom ?d } \
             UNION { ?c ex:subjectOf ?s } }",
            &["s"],
            &[&["s1"], &["s2"], &["s3"]],
            None,
        ),
    ];
    let mut failures = Failures::default();
    for (label, body, vars, expected, bound) in cases {
        let before = store.all_events().len();
        let query = format!("{PREFIXES}{body}");
        let result = db(&handle)
            .await
            .query(&fluree)
            .sparql(&query)
            .execute_formatted()
            .await;
        match result {
            Ok(json) => failures.eq(sparql_rows(&json, vars), rows(expected), label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        if let Some((marker, slot)) = bound {
            failures.bound_lookup(&store, before, marker, *slot, label);
        }
    }

    // One graph, as control.
    let query = format!("{PREFIXES} SELECT ?x ?v FROM <urn:g1> WHERE {{ ?x ex:amount ?v }}");
    let result = db(&handle)
        .await
        .query(&fluree)
        .sparql(&query)
        .execute_formatted()
        .await
        .expect("single-graph query");
    let label = "projection of an arena-backed literal, one graph";
    failures.eq(
        sparql_rows(&result, &["x", "v"]),
        rows(&[&["x1", "1.5"], &["x2", "9.5"]]),
        label,
    );
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// Lanes that read one graph.
//
// A default graph of several graphs of one ledger sets `binary_g_id` to one
// member. The lanes that read a single graph's index (the binary-index fast
// paths, the probe and cyclic lanes, the EXISTS semijoin cache, the
// partitioned GROUP BY, the geo index search) would read that member alone
// there. They gate on `ExecutionContext::reads_exactly_one_graph` (the geo
// rewrite and the partition hint on its planning twin,
// `multi_default_graph`) and stand down for the generic plan. Each test
// pairs scopes of one graph, where the lane must run, with the union default
// graph and `FROM` several graphs, where it must not, all against
// hand-derived answers.

/// A TriG ledger, indexed, with no novelty.
async fn indexed_trig_ledger(ledger_id: &str, trig: &str) -> (Fluree, LedgerHandle) {
    indexed_trig_commits(ledger_id, &[trig]).await
}

/// A ledger of one commit per TriG document, indexed, with no novelty.
async fn indexed_trig_commits(ledger_id: &str, trigs: &[&str]) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    for trig in trigs {
        let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
        fluree
            .stage(&handle)
            .upsert_turtle(trig)
            .execute()
            .await
            .expect("seed TriG");
    }
    rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "indexed");
    assert!(view.novelty.is_empty(), "no novelty");
    (fluree, handle)
}

const UNION_PRAGMA: &str = "# PRAGMA union-default-graph: true\n";

/// One case of a single-graph lane: the query, its columns, the answer, and
/// whether the lane must run (`true`) or stand down (`false`).
type LaneCase = (&'static str, String, &'static [&'static str], Rows, bool);

/// Run SPARQL lane cases, checking each answer and the lane's routing stamp.
async fn check_lane_cases(
    fluree: &Fluree,
    handle: &LedgerHandle,
    site: &str,
    cases: Vec<LaneCase>,
    failures: &mut Failures,
) {
    for (label, query, vars, want, must_fire) in cases {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = db(handle)
            .await
            .query(fluree)
            .sparql(&query)
            .execute_formatted()
            .await;
        match result {
            Ok(json) => failures.eq(sparql_rows(&json, vars), want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        let fired = stamps(&store, before, site).iter().any(|o| o == "proceed");
        failures.eq(fired, must_fire, &format!("{label}: `{site}` ran"));
    }
}

/// One JSON-LD lane case, run through the connection (`from` may name graphs).
struct JsonLaneCase<'a> {
    label: &'a str,
    query: JsonValue,
    want: Rows,
    must_fire: bool,
}

/// Run one JSON-LD lane case, checking its answer and the lane's stamp.
async fn check_lane_jsonld(
    fluree: &Fluree,
    ledger_id: &str,
    site: &str,
    case: JsonLaneCase<'_>,
    failures: &mut Failures,
) {
    let (store, _guard) = init_test_tracing();
    let before = store.all_events().len();
    let result = connection_jsonld(fluree, ledger_id, &case.query).await;
    failures.eq(jsonld_rows(&result), case.want, case.label);
    let fired = stamps(&store, before, site).iter().any(|o| o == "proceed");
    failures.eq(
        fired,
        case.must_fire,
        &format!("{}: `{site}` ran", case.label),
    );
}

/// GROUP BY over one bound-predicate pattern (the partitioned lane, which
/// aggregates key runs instead of hashing). A default graph of several graphs
/// drains its members one after another, each sorted by the key, so one key
/// arrives in one run per member and the lane split its groups (on main too).
#[tokio::test(flavor = "current_thread")]
async fn partitioned_group_by_reads_one_graph() {
    let ledger_id = "it/partitioned-group-by:main";
    let trig = r"
        @prefix ex: <http://example.org/> .
        ex:a1 ex:amount 1.5 ; ex:n 1 .
        GRAPH <urn:g2> { ex:b1 ex:amount 1.5 ; ex:n 1 . ex:b2 ex:amount 2.5 ; ex:n 2 . }
        GRAPH <urn:g3> { ex:c1 ex:amount 1.5 ; ex:n 1 . ex:c2 ex:amount 2.5 ; ex:n 2 . }
    ";
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    let q = |prologue: &str, ds: &str, pred: &str, agg: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?v ({agg} AS ?k) {ds} \
             WHERE {{ ?s ex:{pred} ?v }} GROUP BY ?v"
        )
    };
    let from2 = "FROM <urn:g2> FROM <urn:g3>";
    let vk: &'static [&'static str] = &["v", "k"];
    let cases: Vec<LaneCase> = vec![
        (
            "one graph: the default graph alone",
            q("", "", "n", "COUNT(*)"),
            vk,
            rows(&[&["1", "1"]]),
            true,
        ),
        (
            "one graph: FROM g2 alone",
            q("", "FROM <urn:g2>", "amount", "COUNT(*)"),
            vk,
            rows(&[&["1.5", "1"], &["2.5", "1"]]),
            true,
        ),
        (
            "G1 union, decimal key",
            q(UNION_PRAGMA, "", "amount", "COUNT(*)"),
            vk,
            rows(&[&["1.5", "3"], &["2.5", "2"]]),
            false,
        ),
        (
            "G2 union, integer key",
            q(UNION_PRAGMA, "", "n", "COUNT(*)"),
            vk,
            rows(&[&["1", "3"], &["2", "2"]]),
            false,
        ),
        (
            "G3 FROM g2 + g3, decimal key",
            q("", from2, "amount", "COUNT(*)"),
            vk,
            rows(&[&["1.5", "2"], &["2.5", "2"]]),
            false,
        ),
        (
            "G4 FROM g2 + g3, integer key",
            q("", from2, "n", "COUNT(*)"),
            vk,
            rows(&[&["1", "2"], &["2", "2"]]),
            false,
        ),
        (
            "G6 union, SUM per key",
            q(UNION_PRAGMA, "", "n", "SUM(?v)"),
            vk,
            rows(&[&["1", "3"], &["2", "4"]]),
            false,
        ),
        (
            "G9 FROM NAMED g2 alone: an empty default graph",
            q("", "FROM NAMED <urn:g2>", "n", "COUNT(*)"),
            vk,
            rows(&[]),
            false,
        ),
    ];
    let mut failures = Failures::default();
    check_lane_cases(
        &fluree,
        &handle,
        "partitioned_group_by",
        cases,
        &mut failures,
    )
    .await;
    let group_by = |from: JsonValue, opts: Option<JsonValue>| {
        let mut q = json!({
            "@context": {"ex": EX},
            "from": from,
            "select": ["?v", "(as (count ?s) ?k)"],
            "where": [{"@id": "?s", "ex:n": "?v"}],
            "groupBy": ["?v"]
        });
        if let Some(opts) = opts {
            q["opts"] = opts;
        }
        q
    };
    check_lane_jsonld(
        &fluree,
        ledger_id,
        "partitioned_group_by",
        JsonLaneCase {
            label: "G7 JSON-LD groupBy, union",
            query: group_by(json!(ledger_id), Some(json!({"unionDefaultGraph": true}))),
            want: rows(&[&["1", "3"], &["2", "2"]]),
            must_fire: false,
        },
        &mut failures,
    )
    .await;
    check_lane_jsonld(
        &fluree,
        ledger_id,
        "partitioned_group_by",
        JsonLaneCase {
            label: "G8 JSON-LD groupBy, FROM g2 + g3",
            query: group_by(
                json!([
                    {"@id": ledger_id, "graph": "urn:g2"},
                    {"@id": ledger_id, "graph": "urn:g3"}
                ]),
                None,
            ),
            want: rows(&[&["1", "2"], &["2", "2"]]),
            must_fire: false,
        },
        &mut failures,
    )
    .await;
    failures.assert_none();
}

/// A cyclic BGP (the cyclic-join lane, gated by `allow_cursor_fast_path`).
/// Cycle a→b→c→a spans all three graphs; cycle p→q→r→p lies inside g2. The
/// lane read `binary_g_id`'s graph alone: nothing over the union (whose
/// `binary_g_id` is the default graph's), and nothing under `FROM g3 + g2`.
#[tokio::test(flavor = "current_thread")]
async fn cyclic_lane_reads_one_graph() {
    let trig = r"
        @prefix ex: <http://example.org/> .
        ex:a ex:knows ex:b .
        GRAPH <urn:g2> { ex:b ex:knows ex:c . ex:p ex:knows ex:q . ex:q ex:knows ex:r . ex:r ex:knows ex:p . }
        GRAPH <urn:g3> { ex:c ex:knows ex:a . }
    ";
    let (fluree, handle) = indexed_trig_ledger("it/cyclic-lane-graphs:main", trig).await;
    let tri = |prologue: &str, ds: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?x ?y ?z {ds} \
             WHERE {{ ?x ex:knows ?y . ?y ex:knows ?z . ?z ex:knows ?x }}"
        )
    };
    let xyz: &'static [&'static str] = &["x", "y", "z"];
    let pqr = rows(&[&["p", "q", "r"], &["q", "r", "p"], &["r", "p", "q"]]);
    let cases: Vec<LaneCase> = vec![
        (
            "one graph: FROM g2 alone",
            tri("", "FROM <urn:g2>"),
            xyz,
            pqr.clone(),
            true,
        ),
        (
            "C1 union: one cycle across graphs, one inside g2",
            tri(UNION_PRAGMA, ""),
            xyz,
            rows(&[
                &["a", "b", "c"],
                &["b", "c", "a"],
                &["c", "a", "b"],
                &["p", "q", "r"],
                &["q", "r", "p"],
                &["r", "p", "q"],
            ]),
            false,
        ),
        (
            "C2 FROM g2 + g3",
            tri("", "FROM <urn:g2> FROM <urn:g3>"),
            xyz,
            pqr.clone(),
            false,
        ),
        (
            "C4 FROM g3 + g2",
            tri("", "FROM <urn:g3> FROM <urn:g2>"),
            xyz,
            pqr,
            false,
        ),
        (
            "C6 FROM NAMED g2 alone: an empty default graph",
            tri("", "FROM NAMED <urn:g2>"),
            xyz,
            rows(&[]),
            false,
        ),
    ];
    let mut failures = Failures::default();
    check_lane_cases(&fluree, &handle, "cyclic_bgp", cases, &mut failures).await;
    failures.assert_none();
}

/// The two sites of the EXISTS semijoin cache, and whether each must run.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ExistsLane {
    /// `exists_semijoin`: one graph, its subject set read from that graph.
    One,
    /// `exists_semijoin_union`: a default graph of several graphs, the union
    /// of the members' subject sets; or of none, the empty set.
    Union,
    /// Neither site runs.
    Neither,
}

/// Turns the process-wide kill switch back off when dropped, panic included.
struct FastPathSwitchRestore;
impl Drop for FastPathSwitchRestore {
    fn drop(&mut self) {
        fluree_db_api::set_fast_paths_disabled(false);
    }
}

const EXISTS_ONE_SITE: &str = "exists_semijoin";
const EXISTS_UNION_SITE: &str = "exists_semijoin_union";

/// One case of the EXISTS cache: the query, its columns, the answer, and the
/// site that must run (the other must not).
type ExistsCase = (
    &'static str,
    String,
    &'static [&'static str],
    Rows,
    ExistsLane,
);

/// The outcomes each EXISTS cache site stamped since `before`.
fn exists_stamps(store: &SpanStore, before: usize) -> (Vec<String>, Vec<String>) {
    (
        stamps(store, before, EXISTS_ONE_SITE),
        stamps(store, before, EXISTS_UNION_SITE),
    )
}

/// Check the answer, and that exactly the expected site proceeded.
fn check_exists_routing(
    failures: &mut Failures,
    label: &str,
    (one, union): &(Vec<String>, Vec<String>),
    lane: ExistsLane,
) {
    let ran = |s: &Vec<String>| s.iter().any(|o| o == "proceed");
    failures.eq(
        ran(one),
        lane == ExistsLane::One,
        &format!("{label}: `{EXISTS_ONE_SITE}` ran ({one:?})"),
    );
    failures.eq(
        ran(union),
        lane == ExistsLane::Union,
        &format!("{label}: `{EXISTS_UNION_SITE}` ran ({union:?})"),
    );
}

/// EXISTS inside an expression: the semijoin cache, built at the FILTER's (or
/// HAVING's) `open`. One graph reads its subject set from that graph; a
/// default graph of several graphs of one ledger from the union of the
/// members' sets; a default graph of none from nothing.
///
/// Labels: x1 in the default graph; y1 in g2 and, identically, in g3; w1 in
/// g2, while w1's amount is in g3, so only a union of both graphs' sets
/// answers for it.
#[tokio::test(flavor = "current_thread")]
async fn exists_semijoin_cache_over_one_graph_and_a_union() {
    let ledger_id = "it/exists-cache-graphs:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        ex:x1 ex:amount 1.5 ; ex:label "x one" .
        ex:x2 ex:amount 9.5 .
        GRAPH <urn:g2> {
            ex:y1 ex:amount 7.5 ; ex:label "y one" .
            ex:y2 ex:amount 1.5 .
            ex:w1 ex:label "w one" .
        }
        GRAPH <urn:g3> {
            ex:z1 ex:amount 9.5 .
            ex:w1 ex:amount 2.5 .
            ex:y1 ex:label "y one" .
        }
    "#;
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    // The union site answers to the kill switch: no test may flip it mid-case.
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;
    let filter = |prologue: &str, ds: &str, not: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?x {ds} WHERE {{ ?x ex:amount ?v \
             FILTER({not}EXISTS {{ ?x ex:label ?l }} && ?v > 0) }}"
        )
    };
    let from_named_g2 = |not: &str| {
        format!(
            "{PREFIXES}SELECT ?x FROM NAMED <urn:g2> WHERE {{ \
             GRAPH <urn:g2> {{ ?x ex:amount ?v }} \
             FILTER({not}EXISTS {{ ?x ex:label ?l }} && ?v > 0) }}"
        )
    };
    let x: &'static [&'static str] = &["x"];
    let cases: Vec<ExistsCase> = vec![
        (
            "one graph: FROM g2 alone",
            filter("", "FROM <urn:g2>", ""),
            x,
            rows(&[&["y1"]]),
            ExistsLane::One,
        ),
        (
            "one graph: FROM g3 alone (w1's label is in g2)",
            filter("", "FROM <urn:g3>", ""),
            x,
            rows(&[]),
            ExistsLane::One,
        ),
        (
            "one graph: the default graph",
            filter("", "", ""),
            x,
            rows(&[&["x1"]]),
            ExistsLane::One,
        ),
        (
            "E1 union default graph, FILTER(EXISTS && ...)",
            filter(UNION_PRAGMA, "", ""),
            x,
            rows(&[&["w1"], &["x1"], &["y1"]]),
            ExistsLane::Union,
        ),
        (
            "E3 union default graph, FILTER(!EXISTS && ...)",
            filter(UNION_PRAGMA, "", "!"),
            x,
            rows(&[&["x2"], &["y2"], &["z1"]]),
            ExistsLane::Union,
        ),
        (
            "E4 FROM g3 + g2",
            filter("", "FROM <urn:g3> FROM <urn:g2>", ""),
            x,
            rows(&[&["w1"], &["y1"]]),
            ExistsLane::Union,
        ),
        (
            "E5 FROM g2 + g3, FILTER(!EXISTS && ...)",
            filter("", "FROM <urn:g2> FROM <urn:g3>", "!"),
            x,
            rows(&[&["y2"], &["z1"]]),
            ExistsLane::Union,
        ),
        (
            "E6 FROM default + g2 + g3",
            filter(
                "",
                &format!("FROM <{ledger_id}> FROM <urn:g2> FROM <urn:g3>"),
                "",
            ),
            x,
            rows(&[&["w1"], &["x1"], &["y1"]]),
            ExistsLane::Union,
        ),
        (
            "E7 HAVING(EXISTS), FROM g2 + g3",
            format!(
                "{PREFIXES}SELECT ?x FROM <urn:g2> FROM <urn:g3> WHERE {{ ?x ex:amount ?v }} \
                 GROUP BY ?x HAVING(EXISTS {{ ?x ex:label ?l }} && COUNT(?v) > 0)"
            ),
            x,
            rows(&[&["w1"], &["y1"]]),
            ExistsLane::Union,
        ),
        (
            "E8 FROM NAMED g2 alone: EXISTS over an empty default graph",
            from_named_g2(""),
            x,
            rows(&[]),
            ExistsLane::Union,
        ),
        (
            "E9 FROM NAMED g2 alone: !EXISTS over an empty default graph",
            from_named_g2("!"),
            x,
            rows(&[&["y1"], &["y2"]]),
            ExistsLane::Union,
        ),
        (
            "E2 union default graph, BIND(EXISTS): no cache",
            format!(
                "{UNION_PRAGMA}{PREFIXES}SELECT ?x ?b WHERE {{ ?x ex:amount ?v \
                 BIND(EXISTS {{ ?x ex:label ?l }} AS ?b) }}"
            ),
            &["x", "b"],
            rows(&[
                &["w1", "true"],
                &["x1", "true"],
                &["x2", "false"],
                &["y1", "true"],
                &["y2", "false"],
                &["z1", "false"],
            ]),
            ExistsLane::Neither,
        ),
    ];
    let mut failures = Failures::default();
    for (label, query, vars, want, lane) in cases {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = db(&handle)
            .await
            .query(&fluree)
            .sparql(&query)
            .execute_formatted()
            .await;
        match result {
            Ok(json) => failures.eq(sparql_rows(&json, vars), want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        check_exists_routing(&mut failures, label, &exists_stamps(&store, before), lane);
    }

    // The JSON-LD twins: `exists` inside a filter expression.
    let exists_filter = |from: JsonValue, opts: Option<JsonValue>| {
        let mut q = json!({
            "@context": {"ex": EX},
            "from": from,
            "select": ["?x"],
            "where": [
                {"@id": "?x", "ex:amount": "?v"},
                ["filter", ["and", ["exists", {"@id": "?x", "ex:label": "?l"}], [">", "?v", 0]]]
            ]
        });
        if let Some(opts) = opts {
            q["opts"] = opts;
        }
        q
    };
    let json_cases = [
        (
            "J1 JSON-LD, one graph: g2",
            exists_filter(json!({"@id": ledger_id, "graph": "urn:g2"}), None),
            rows(&[&["y1"]]),
            ExistsLane::One,
        ),
        (
            "J2 JSON-LD, FROM g2 + g3",
            exists_filter(
                json!([
                    {"@id": ledger_id, "graph": "urn:g2"},
                    {"@id": ledger_id, "graph": "urn:g3"}
                ]),
                None,
            ),
            rows(&[&["w1"], &["y1"]]),
            ExistsLane::Union,
        ),
        (
            "J3 JSON-LD, opts.unionDefaultGraph",
            exists_filter(json!(ledger_id), Some(json!({"unionDefaultGraph": true}))),
            rows(&[&["w1"], &["x1"], &["y1"]]),
            ExistsLane::Union,
        ),
    ];
    for (label, query, want, lane) in json_cases {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = connection_jsonld(&fluree, ledger_id, &query).await;
        failures.eq(jsonld_rows(&result), want, label);
        check_exists_routing(&mut failures, label, &exists_stamps(&store, before), lane);
    }

    // Under the kill switch the union lane stands down and says so; the
    // one-graph cache never answered to it, and still doesn't.
    drop(_switch);
    {
        let _switch = crate::support::FAST_PATH_SWITCH.write().await;
        let _restore = FastPathSwitchRestore;
        fluree_db_api::set_fast_paths_disabled(true);
        for (label, query, want, lane) in [
            (
                "K1 FROM g3 + g2 under the kill switch",
                filter("", "FROM <urn:g3> FROM <urn:g2>", ""),
                rows(&[&["w1"], &["y1"]]),
                ExistsLane::Neither,
            ),
            (
                "K2 FROM g2 alone under the kill switch",
                filter("", "FROM <urn:g2>", ""),
                rows(&[&["y1"]]),
                ExistsLane::One,
            ),
        ] {
            let (store, _guard) = init_test_tracing();
            let before = store.all_events().len();
            let json = db(&handle)
                .await
                .query(&fluree)
                .sparql(&query)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{label}: {e}"));
            failures.eq(sparql_rows(&json, x), want, label);
            let stamped = exists_stamps(&store, before);
            check_exists_routing(&mut failures, label, &stamped, lane);
            if lane == ExistsLane::Neither {
                failures.eq(
                    stamped.1,
                    vec!["fallback:kill_switch".to_string()],
                    &format!("{label}: `{EXISTS_UNION_SITE}` decline"),
                );
            }
        }
    }
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;

    // Novelty in one member: the union lane declines, and says why. The
    // answer still counts v1, which only novelty holds.
    fluree
        .stage(&handle)
        .upsert_turtle(
            r#"@prefix ex: <http://example.org/> .
               GRAPH <urn:g2> { ex:v1 ex:amount 3.5 ; ex:label "v one" . }"#,
        )
        .execute()
        .await
        .expect("write to g2 after the index");
    {
        let label = "N1 FROM g3 + g2, novelty in g2";
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let json = db(&handle)
            .await
            .query(&fluree)
            .sparql(&filter("", "FROM <urn:g3> FROM <urn:g2>", ""))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        failures.eq(
            sparql_rows(&json, x),
            rows(&[&["v1"], &["w1"], &["y1"]]),
            label,
        );
        let stamped = exists_stamps(&store, before);
        check_exists_routing(&mut failures, label, &stamped, ExistsLane::Neither);
        failures.eq(
            stamped.1,
            vec!["fallback:novelty".to_string()],
            &format!("{label}: `{EXISTS_UNION_SITE}` decline"),
        );
    }
    failures.assert_none();
}

/// The EXISTS cache over a union where the EXISTS's predicate holds lists.
/// SPARQL reads a list as the RDF it denotes (#2159), an owner edge to the
/// list's head cell, so `EXISTS { ?x ex:tags ?t }` is read through the list
/// view: the cache over several graphs stands down with `ListView` and the
/// general plan answers. JSON-LD reads the members as values, and the cache
/// answers. Either way a subject that owns a list has a tag, so the answers
/// agree.
///
/// Tags: y1 owns a list in g2, y2 one in g3, w1 a plain tag in g2; z1 has
/// none. Amounts: y1 and y2 in g2, w1 and z1 in g3.
#[tokio::test(flavor = "current_thread")]
async fn exists_semijoin_cache_over_a_union_reads_a_list_as_each_surface_does() {
    let ledger_id = "it/exists-cache-lists:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        ex:x1 ex:amount 1.5 .
        GRAPH <urn:g2> {
            ex:y1 ex:amount 7.5 ; ex:tags ( "a" "b" ) .
            ex:y2 ex:amount 1.5 .
            ex:w1 ex:tags "c" .
        }
        GRAPH <urn:g3> {
            ex:w1 ex:amount 2.5 .
            ex:z1 ex:amount 9.5 .
            ex:y2 ex:tags ( "d" ) .
        }
    "#;
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;
    let x: &'static [&'static str] = &["x"];
    let has_tag = rows(&[&["w1"], &["y1"], &["y2"]]);
    let no_tag = rows(&[&["z1"]]);
    let mut failures = Failures::default();
    for (label, not, want) in [
        (
            "L1 FROM g2 + g3, FILTER(EXISTS && ...) on a list predicate",
            "",
            &has_tag,
        ),
        (
            "L2 FROM g2 + g3, FILTER(!EXISTS && ...) on a list predicate",
            "!",
            &no_tag,
        ),
    ] {
        let query = format!(
            "{PREFIXES}SELECT ?x FROM <urn:g2> FROM <urn:g3> WHERE {{ ?x ex:amount ?v \
             FILTER({not}EXISTS {{ ?x ex:tags ?t }} && ?v > 0) }}"
        );
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        match db(&handle)
            .await
            .query(&fluree)
            .sparql(&query)
            .execute_formatted()
            .await
        {
            Ok(json) => failures.eq(sparql_rows(&json, x), want.clone(), label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        let stamped = exists_stamps(&store, before);
        check_exists_routing(&mut failures, label, &stamped, ExistsLane::Neither);
        failures.eq(
            stamped.1,
            vec!["fallback:list_view".to_string()],
            &format!("{label}: `{EXISTS_UNION_SITE}` decline"),
        );
    }
    for (label, exists, want) in [
        (
            "JL1 JSON-LD, FROM g2 + g3, a list's members are tags",
            json!(["exists", {"@id": "?x", "ex:tags": "?t"}]),
            &has_tag,
        ),
        (
            "JL2 JSON-LD, FROM g2 + g3, !EXISTS",
            json!(["not", ["exists", {"@id": "?x", "ex:tags": "?t"}]]),
            &no_tag,
        ),
    ] {
        let query = json!({
            "@context": {"ex": EX},
            "from": [
                {"@id": ledger_id, "graph": "urn:g2"},
                {"@id": ledger_id, "graph": "urn:g3"}
            ],
            "select": ["?x"],
            "where": [
                {"@id": "?x", "ex:amount": "?v"},
                ["filter", ["and", exists, [">", "?v", 0]]]
            ]
        });
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = connection_jsonld(&fluree, ledger_id, &query).await;
        failures.eq(jsonld_rows(&result), want.clone(), label);
        check_exists_routing(
            &mut failures,
            label,
            &exists_stamps(&store, before),
            ExistsLane::Union,
        );
    }
    failures.assert_none();
}

/// A `geof:distance` filter, which the planner rewrites into the geo index
/// search when the scope reads one graph. Paris is in the default graph,
/// London in g2, Brussels and Berlin in g3; London and Brussels lie within
/// 500 km of Paris, Berlin does not.
#[tokio::test(flavor = "current_thread")]
async fn geo_search_reads_one_graph() {
    let ledger_id = "it/geo-search-graphs:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:paris ex:location "POINT(2.3522 48.8566)"^^geo:wktLiteral .
        GRAPH <urn:g2> { ex:london ex:location "POINT(-0.1276 51.5072)"^^geo:wktLiteral . }
        GRAPH <urn:g3> {
            ex:brussels ex:location "POINT(4.3517 50.8503)"^^geo:wktLiteral .
            ex:berlin ex:location "POINT(13.4050 52.5200)"^^geo:wktLiteral .
        }
    "#;
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    let nearby = |from: Option<JsonValue>, opts: Option<JsonValue>| {
        let mut q = json!({
            "@context": {
                "ex": EX,
                "geo": "http://www.opengis.net/ont/geosparql#",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            },
            "select": ["?place"],
            "where": [
                {"@id": "?place", "ex:location": "?loc"},
                ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
                ["filter", "(<= ?dist 500000)"]
            ]
        });
        if let Some(from) = from {
            q["from"] = from;
        }
        if let Some(opts) = opts {
            q["opts"] = opts;
        }
        q
    };
    let mut failures = Failures::default();
    {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let r = jsonld(&fluree, &handle, &nearby(None, None)).await;
        failures.eq(
            jsonld_rows(&r),
            rows(&[&["paris"]]),
            "one graph: the default graph alone",
        );
        let fired = stamps(&store, before, "geo_search")
            .iter()
            .any(|o| o == "proceed");
        failures.eq(fired, true, "one graph: `geo_search` ran");
    }
    check_lane_jsonld(
        &fluree,
        ledger_id,
        "geo_search",
        JsonLaneCase {
            label: "union",
            query: nearby(
                Some(json!(ledger_id)),
                Some(json!({"unionDefaultGraph": true})),
            ),
            want: rows(&[&["brussels"], &["london"], &["paris"]]),
            must_fire: false,
        },
        &mut failures,
    )
    .await;
    check_lane_jsonld(
        &fluree,
        ledger_id,
        "geo_search",
        JsonLaneCase {
            label: "FROM g2 + g3",
            query: nearby(
                Some(json!([
                    {"@id": ledger_id, "graph": "urn:g2"},
                    {"@id": ledger_id, "graph": "urn:g3"}
                ])),
                None,
            ),
            want: rows(&[&["brussels"], &["london"]]),
            must_fire: false,
        },
        &mut failures,
    )
    .await;
    failures.assert_none();
}

const OBJECT_COUNT_SITE: &str = "predicate_object_count";
const OBJECT_COUNT_UNION_SITE: &str = "predicate_object_count_union";
const TOPK_SITES: (&str, &str) = (
    "group_by_object_count_topk",
    "group_by_object_count_topk_union",
);

/// What the bound-object count must do for a query.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ObjectCount {
    /// The one-graph lane answers; the union lane stamps nothing.
    One,
    /// The union lane answers, by this plan when given (`zero`; `rows`, every
    /// slice one member's; `merge`); the one-graph lane declines. A plan the manifest decides on a
    /// fixture this small (it may hold one leaf per graph) is left unchecked.
    Union(Option<&'static str>),
    /// The union lane declines with this label, by this plan when given; the
    /// general plan answers.
    UnionDeclines(&'static str, Option<&'static str>),
}

/// Check one bound-object count's routing: the union site's stamps, and
/// whether the one-graph lane (stamped `proceed` when planned, a decline at
/// open) answered.
fn check_object_count_routing(
    failures: &mut Failures,
    label: &str,
    store: &SpanStore,
    before: usize,
    lane: ObjectCount,
) {
    check_count_routing(
        failures,
        label,
        store,
        before,
        (OBJECT_COUNT_SITE, OBJECT_COUNT_UNION_SITE),
        lane,
    );
}

/// [`check_object_count_routing`] for any count lane with a one-graph site
/// (stamped `proceed` when planned, a decline at open) and a union site.
fn check_count_routing(
    failures: &mut Failures,
    label: &str,
    store: &SpanStore,
    before: usize,
    (one_site, union_site): (&str, &str),
    lane: ObjectCount,
) {
    let one = stamps(store, before, one_site);
    let union = stamps(store, before, union_site);
    let one_answered =
        one.iter().any(|o| o == "proceed") && !one.iter().any(|o| o == "fallback:gate_declined");
    failures.eq(
        one_answered,
        lane == ObjectCount::One,
        &format!("{label}: `{one_site}` answered ({one:?})"),
    );
    let want: Vec<String> = match lane {
        ObjectCount::One => vec![],
        ObjectCount::Union(_) => vec!["proceed".to_string()],
        ObjectCount::UnionDeclines(reason, _) => vec![format!("fallback:{reason}")],
    };
    failures.eq(union, want, &format!("{label}: `{union_site}` stamps"));
    if let ObjectCount::Union(Some(plan)) | ObjectCount::UnionDeclines(_, Some(plan)) = lane {
        let plans: Vec<String> = store.all_events()[before..]
            .iter()
            .filter(|e| e.message() == "union count plan")
            .filter_map(|e| e.fields.get("plan").cloned())
            .collect();
        failures.eq(plans, vec![plan.to_string()], &format!("{label}: plan"));
    }
}

/// `COUNT(?s)` over `?s <p> <o>`: the bound-object count. Over one graph it
/// counts that graph's rows; over a default graph of several graphs of one
/// ledger it counts the facts holding the object in some member, each once
/// (one fact per value and list position), as the general plan does;
/// over a default graph of none, 0.
///
/// Kinds: a and b `T` in g2, a (the identical triple) and d `T` in g3, e `T`
/// in the default graph, c `U` in g2. Only g3 has `ex:flag`. `ex:n 1`: a holds
/// it as an integer and a double in g2, b as an integer in g2 and g3, c as a
/// double in g3. `ex:q 5`: f's list `(5 5 7)` in g2 and g's plain 5 in g3;
/// `ex:r 5`: f's list `(5 5)` in g2 only. `ex:w 5`: u's and v's lists `(5 5)`
/// in g2, whose rows POST orders `(o_i, s)` as u, v, u, v (or v, u, v, u), so
/// one subject's rows are not together. `ex:m 1`: h's decimal `1.0` in g2,
/// i's integer `1` in g3. `ex:zz`, only in g2 and committed later, so that no
/// leaf of g3 can span it: k's `6`, and f's list `(5 5)`.
///
/// `ex:tag`: p00..p09 are introduced in order (by `ex:seq` in the default
/// graph), g2 tags the even ones `I` and g3 the odd ones, so the members'
/// subject ranges interleave with no subject in common; q00..q04 tagged `J` in
/// g2 and q05..q09 in g3 hold disjoint ranges.
///
/// A list reads two ways (#2159). SPARQL reads it as the RDF it denotes, an
/// owner edge to the list's head cell: a member is not a value of its
/// predicate, and a pattern on a predicate holding lists is read through the
/// list view, which the union lanes leave to the general plan, stamping
/// `fallback:list_view`. JSON-LD reads the members as values, and the union
/// lanes count them, one fact per value and list position.
#[tokio::test(flavor = "current_thread")]
async fn object_count_over_one_graph_and_a_union() {
    let ledger_id = "it/object-count-graphs:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:p00 ex:seq 0 . ex:p01 ex:seq 1 . ex:p02 ex:seq 2 . ex:p03 ex:seq 3 . ex:p04 ex:seq 4 . ex:p05 ex:seq 5 . ex:p06 ex:seq 6 . ex:p07 ex:seq 7 . ex:p08 ex:seq 8 . ex:p09 ex:seq 9 .
        ex:q00 ex:seq 10 . ex:q01 ex:seq 11 . ex:q02 ex:seq 12 . ex:q03 ex:seq 13 . ex:q04 ex:seq 14 . ex:q05 ex:seq 15 . ex:q06 ex:seq 16 . ex:q07 ex:seq 17 . ex:q08 ex:seq 18 . ex:q09 ex:seq 19 .
        ex:e ex:kind ex:T .
        GRAPH <urn:g2> {
            ex:p00 ex:tag ex:I . ex:p02 ex:tag ex:I . ex:p04 ex:tag ex:I . ex:p06 ex:tag ex:I . ex:p08 ex:tag ex:I . ex:q00 ex:tag ex:J . ex:q01 ex:tag ex:J . ex:q02 ex:tag ex:J . ex:q03 ex:tag ex:J . ex:q04 ex:tag ex:J .
            ex:a ex:kind ex:T ; ex:n 1 , "1.0e0"^^xsd:double .
            ex:b ex:kind ex:T ; ex:n 1 .
            ex:c ex:kind ex:U .
            ex:f ex:q ( 5 5 7 ) ; ex:r ( 5 5 ) .
            ex:u ex:w ( 5 5 ) .
            ex:v ex:w ( 5 5 ) .
            ex:h ex:m 1.0 .
        }
        GRAPH <urn:g3> {
            ex:a ex:kind ex:T .
            ex:d ex:kind ex:T ; ex:flag ex:on .
            ex:b ex:n 1 .
            ex:c ex:n "1.0e0"^^xsd:double .
            ex:g ex:q 5 .
            ex:i ex:m 1 .
            ex:p01 ex:tag ex:I . ex:p03 ex:tag ex:I . ex:p05 ex:tag ex:I . ex:p07 ex:tag ex:I . ex:p09 ex:tag ex:I . ex:q05 ex:tag ex:J . ex:q06 ex:tag ex:J . ex:q07 ex:tag ex:J . ex:q08 ex:tag ex:J . ex:q09 ex:tag ex:J .
        }
    "#;
    // A second commit: a reindex replays commits in order and numbers each
    // predicate when it first sees it, so `ex:zz` sorts after every predicate
    // of g3 in POST, and no leaf of g3 spans it.
    let later = r"
        @prefix ex: <http://example.org/> .
        GRAPH <urn:g2> { ex:k ex:zz 6 . ex:f ex:zz ( 5 5 ) . }
    ";
    let (fluree, handle) = indexed_trig_commits(ledger_id, &[trig, later]).await;
    // The union lane answers to the kill switch: no test may flip it mid-case.
    let switch = crate::support::FAST_PATH_SWITCH.read().await;
    let count = |prologue: &str, ds: &str, triple: &str| {
        format!("{prologue}{PREFIXES}SELECT (COUNT(?s) AS ?n) {ds} WHERE {{ ?s {triple} }}")
    };
    let from23 = "FROM <urn:g2> FROM <urn:g3>";
    let n = |v: &str| rows(&[&[v]]);
    let cases: Vec<(&str, String, Rows, ObjectCount)> = vec![
        (
            "one graph: the default graph",
            count("", "", "ex:kind ex:T"),
            n("1"),
            ObjectCount::One,
        ),
        (
            "one graph: FROM g2",
            count("", "FROM <urn:g2>", "ex:kind ex:T"),
            n("2"),
            ObjectCount::One,
        ),
        (
            "one graph: FROM g2, a bare number is a bag of slices",
            count("", "FROM <urn:g2>", "ex:n 1"),
            n("3"),
            ObjectCount::One,
        ),
        (
            "D1 FROM g2 + g3: a triple in both counts once",
            count("", from23, "ex:kind ex:T"),
            n("3"),
            ObjectCount::Union(Some("merge")),
        ),
        (
            "D2 union default graph",
            count(UNION_PRAGMA, "", "ex:kind ex:T"),
            n("4"),
            ObjectCount::Union(Some("merge")),
        ),
        (
            "D3 FROM default + g2 + g3",
            count("", &format!("FROM <{ledger_id}> {from23}"), "ex:kind ex:T"),
            n("4"),
            ObjectCount::Union(Some("merge")),
        ),
        (
            "D4 FROM g2 + g3, a predicate only g3 holds",
            count("", from23, "ex:flag ex:on"),
            n("1"),
            ObjectCount::Union(None),
        ),
        (
            "D5 FROM g2 + g3, an object only g2 holds",
            count("", from23, "ex:kind ex:U"),
            n("1"),
            ObjectCount::Union(None),
        ),
        (
            "D6 FROM g2 + g3, a known IRI no graph holds as the object",
            count("", from23, "ex:kind ex:on"),
            n("0"),
            ObjectCount::Union(None),
        ),
        (
            "D7 FROM g2 + g3, an IRI the ledger never saw",
            count("", from23, "ex:kind ex:Nope"),
            n("0"),
            ObjectCount::Union(None),
        ),
        (
            "D8 FROM g2 + g3, a bare number: each slice's facts, the one both hold once",
            count("", from23, "ex:n 1"),
            n("4"),
            ObjectCount::Union(Some("merge")),
        ),
        // SPARQL reads a list as the RDF it denotes (#2159): a member is not
        // a value of its predicate, so a predicate holding lists is read
        // through the list view, which the union lane does not answer.
        (
            "D9 FROM g2 + g3, a list repeating the object: not a value in SPARQL",
            count("", from23, "ex:q 5"),
            n("1"),
            ObjectCount::UnionDeclines("list_view", None),
        ),
        (
            "D10 FROM g2 + g3, a list repeating the object (g2 only): not a value in SPARQL",
            count("", from23, "ex:r 5"),
            n("0"),
            ObjectCount::UnionDeclines("list_view", None),
        ),
        (
            "D15 FROM g2 + g3, two lists repeating the object: not values in SPARQL",
            count("", from23, "ex:w 5"),
            n("0"),
            ObjectCount::UnionDeclines("list_view", None),
        ),
        (
            "D11 FROM g2 + g3, a bare number with a decimal slice in one member",
            count("", from23, "ex:m 1"),
            n("2"),
            ObjectCount::Union(Some("rows")),
        ),
        (
            "D13 FROM g2 + g3, a predicate only g2 holds, past g3's last leaf, beside a list",
            count("", from23, "ex:zz 6"),
            n("1"),
            ObjectCount::UnionDeclines("list_view", None),
        ),
        (
            "D14 FROM g2 + g3, a list repeating the object, past g3's last leaf: not a value in SPARQL",
            count("", from23, "ex:zz 5"),
            n("0"),
            ObjectCount::UnionDeclines("list_view", None),
        ),
        (
            "D16 FROM g2 + g3, interleaved subject ranges, none in common: the merge",
            count("", from23, "ex:tag ex:I"),
            n("10"),
            ObjectCount::Union(Some("merge")),
        ),
        (
            "D17 FROM g2 + g3, disjoint subject ranges",
            count("", from23, "ex:tag ex:J"),
            n("10"),
            ObjectCount::Union(Some("merge")),
        ),
        (
            "D12 FROM NAMED g2 alone: an empty default graph",
            count("", "FROM NAMED <urn:g2>", "ex:kind ex:T"),
            n("0"),
            ObjectCount::Union(None),
        ),
    ];
    let mut failures = Failures::default();
    let run = |query: String| {
        let fluree = &fluree;
        let handle = &handle;
        async move {
            db(handle)
                .await
                .query(fluree)
                .sparql(&query)
                .execute_formatted()
                .await
                .map_err(|e| e.to_string())
        }
    };
    for (label, query, want, lane) in cases {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        match run(query).await {
            Ok(json) => failures.eq(sparql_rows(&json, &["n"]), want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        check_object_count_routing(&mut failures, label, &store, before, lane);
    }

    // The JSON-LD twins.
    let json_count = |from: JsonValue, opts: Option<JsonValue>| {
        let mut q = json!({
            "@context": {"ex": EX},
            "from": from,
            "select": ["(as (count ?s) ?n)"],
            "where": [{"@id": "?s", "ex:kind": {"@id": "ex:T"}}]
        });
        if let Some(opts) = opts {
            q["opts"] = opts;
        }
        q
    };
    for (label, query, want, lane) in [
        (
            "J1 JSON-LD, one graph: g2",
            json_count(json!({"@id": ledger_id, "graph": "urn:g2"}), None),
            n("2"),
            ObjectCount::One,
        ),
        (
            "J2 JSON-LD, FROM g2 + g3",
            json_count(
                json!([
                    {"@id": ledger_id, "graph": "urn:g2"},
                    {"@id": ledger_id, "graph": "urn:g3"}
                ]),
                None,
            ),
            n("3"),
            ObjectCount::Union(None),
        ),
        (
            "J3 JSON-LD, opts.unionDefaultGraph",
            json_count(json!(ledger_id), Some(json!({"unionDefaultGraph": true}))),
            n("4"),
            ObjectCount::Union(None),
        ),
    ] {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = connection_jsonld(&fluree, ledger_id, &query).await;
        failures.eq(jsonld_rows(&result), want, label);
        check_object_count_routing(&mut failures, label, &store, before, lane);
    }

    // The list cases in JSON-LD, which reads a list's members as values of
    // its predicate: over a union each item is a fact (one per value and list
    // position), and the union lane counts them.
    let from23_jsonld = json!([
        {"@id": ledger_id, "graph": "urn:g2"},
        {"@id": ledger_id, "graph": "urn:g3"}
    ]);
    let json_bound = |pred: &str, value: i64| {
        json!({
            "@context": {"ex": EX},
            "from": from23_jsonld.clone(),
            "select": ["(as (count ?s) ?n)"],
            "where": [{"@id": "?s", pred: value}]
        })
    };
    for (label, query, want, lane) in [
        (
            "J9 JSON-LD, FROM g2 + g3, a list repeating the object: every item a fact",
            json_bound("ex:q", 5),
            n("3"),
            ObjectCount::Union(Some("merge")),
        ),
        (
            "J10 JSON-LD, FROM g2 + g3, a list repeating the object (g2 only)",
            json_bound("ex:r", 5),
            n("2"),
            ObjectCount::Union(None),
        ),
        (
            "J13 JSON-LD, FROM g2 + g3, a predicate only g2 holds, past g3's last leaf",
            json_bound("ex:zz", 6),
            n("1"),
            ObjectCount::Union(Some("rows")),
        ),
        (
            "J14 JSON-LD, FROM g2 + g3, a list repeating the object, past g3's last leaf",
            json_bound("ex:zz", 5),
            n("2"),
            ObjectCount::Union(Some("rows")),
        ),
        (
            "J15 JSON-LD, FROM g2 + g3, two lists repeating the object: every item a fact",
            json_bound("ex:w", 5),
            n("4"),
            ObjectCount::Union(None),
        ),
    ] {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = connection_jsonld(&fluree, ledger_id, &query).await;
        failures.eq(jsonld_rows(&result), want, label);
        check_object_count_routing(&mut failures, label, &store, before, lane);
    }

    // The grouped twin: `GROUP BY ?o` with `COUNT(?s)`, top k. Over a union a
    // group counts the facts holding that object in some member, each once
    // (one fact per value and list position), and two datatypes of one number
    // are two groups.
    let topk = |prologue: &str, ds: &str, pred: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?o (COUNT(?s) AS ?n) {ds} \
             WHERE {{ ?s {pred} ?o }} GROUP BY ?o ORDER BY DESC(?n) LIMIT 5"
        )
    };
    let topk_cases: Vec<(&str, String, Rows, ObjectCount)> = vec![
        (
            "T0 one graph: FROM g2",
            topk("", "FROM <urn:g2>", "ex:kind"),
            rows(&[&["T", "2"], &["U", "1"]]),
            ObjectCount::One,
        ),
        (
            "T1 FROM g2 + g3: a triple in both counts once",
            topk("", from23, "ex:kind"),
            rows(&[&["T", "3"], &["U", "1"]]),
            ObjectCount::Union(None),
        ),
        (
            "T2 union default graph",
            topk(UNION_PRAGMA, "", "ex:kind"),
            rows(&[&["T", "4"], &["U", "1"]]),
            ObjectCount::Union(None),
        ),
        (
            "T3 FROM g2 + g3, a number's two datatypes are two groups",
            topk("", from23, "ex:n"),
            rows(&[&["1", "2"], &["1.0E0", "2"]]),
            ObjectCount::Union(None),
        ),
        (
            "T4 FROM g2 + g3, a list: its head cell in SPARQL",
            topk("", from23, "ex:q"),
            rows(&[&["5", "1"], &["BLANK", "1"]]),
            ObjectCount::UnionDeclines("list_view", None),
        ),
        (
            "T5 FROM g2 + g3, decimal objects (arena handles)",
            topk("", from23, "ex:m"),
            // The general plan writes the decimal `1.0` as "1".
            rows(&[&["1", "1"], &["1", "1"]]),
            ObjectCount::UnionDeclines("arena_type", None),
        ),
        (
            "T6 FROM NAMED g2 alone: an empty default graph",
            topk("", "FROM NAMED <urn:g2>", "ex:kind"),
            rows(&[]),
            ObjectCount::Union(None),
        ),
    ];
    for (label, query, want, lane) in topk_cases {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        match run(query).await {
            Ok(json) => failures.eq(sparql_rows(&json, &["o", "n"]), want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        check_count_routing(&mut failures, label, &store, before, TOPK_SITES, lane);
    }
    {
        // T4 in JSON-LD, which reads the list's members as values.
        let label = "JT4 JSON-LD, FROM g2 + g3, list rows: every item a fact";
        let query = json!({
            "@context": {"ex": EX},
            "from": from23_jsonld.clone(),
            "select": ["?o", "(as (count ?s) ?n)"],
            "where": [{"@id": "?s", "ex:q": "?o"}],
            "groupBy": ["?o"],
            "orderBy": ["(desc ?n)"],
            "limit": 5
        });
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = connection_jsonld(&fluree, ledger_id, &query).await;
        failures.eq(
            jsonld_rows(&result),
            rows(&[&["5", "3"], &["7", "1"]]),
            label,
        );
        check_count_routing(
            &mut failures,
            label,
            &store,
            before,
            TOPK_SITES,
            ObjectCount::Union(None),
        );
    }

    // Under the kill switch the planner builds no count lane; the union site
    // says so, and the one-graph site stays silent, as it always has.
    drop(switch);
    {
        let _switch = crate::support::FAST_PATH_SWITCH.write().await;
        let _restore = FastPathSwitchRestore;
        fluree_db_api::set_fast_paths_disabled(true);
        for (label, ds, want, union_stamps) in [
            (
                "K1 FROM g2 + g3 under the kill switch",
                from23,
                "3",
                vec!["fallback:kill_switch"],
            ),
            (
                "K2 FROM g2 under the kill switch",
                "FROM <urn:g2>",
                "2",
                vec![],
            ),
        ] {
            let (store, _guard) = init_test_tracing();
            let before = store.all_events().len();
            match run(count("", ds, "ex:kind ex:T")).await {
                Ok(json) => failures.eq(sparql_rows(&json, &["n"]), n(want), label),
                Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
            }
            failures.eq(
                (
                    stamps(&store, before, OBJECT_COUNT_SITE),
                    stamps(&store, before, OBJECT_COUNT_UNION_SITE),
                ),
                (vec![], union_stamps.into_iter().map(String::from).collect()),
                &format!("{label}: (one-graph, union) stamps"),
            );
        }
        for (label, ds, want, union_stamps) in [
            (
                "K3 top-k, FROM g2 + g3 under the kill switch",
                from23,
                rows(&[&["T", "3"], &["U", "1"]]),
                vec!["fallback:kill_switch"],
            ),
            (
                "K4 top-k, FROM g2 under the kill switch",
                "FROM <urn:g2>",
                rows(&[&["T", "2"], &["U", "1"]]),
                vec![],
            ),
        ] {
            let (store, _guard) = init_test_tracing();
            let before = store.all_events().len();
            match run(topk("", ds, "ex:kind")).await {
                Ok(json) => failures.eq(sparql_rows(&json, &["o", "n"]), want, label),
                Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
            }
            failures.eq(
                (
                    stamps(&store, before, TOPK_SITES.0),
                    stamps(&store, before, TOPK_SITES.1),
                ),
                (vec![], union_stamps.into_iter().map(String::from).collect()),
                &format!("{label}: (one-graph, union) stamps"),
            );
        }
    }
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;

    // Novelty in one member: the union lane declines, and says why. The
    // answer counts j, which only novelty holds.
    fluree
        .stage(&handle)
        .upsert_turtle(
            r"@prefix ex: <http://example.org/> .
               GRAPH <urn:g2> { ex:j ex:kind ex:T . }",
        )
        .execute()
        .await
        .expect("write to g2 after the index");
    let label = "N1 FROM g2 + g3, novelty in g2";
    let (store, _guard) = init_test_tracing();
    let before = store.all_events().len();
    match run(count("", from23, "ex:kind ex:T")).await {
        Ok(json) => failures.eq(sparql_rows(&json, &["n"]), n("4"), label),
        Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
    }
    check_object_count_routing(
        &mut failures,
        label,
        &store,
        before,
        ObjectCount::UnionDeclines("novelty", None),
    );
    let label = "N2 top-k, FROM g2 + g3, novelty in g2";
    let (store, _guard) = init_test_tracing();
    let before = store.all_events().len();
    match run(topk("", from23, "ex:kind")).await {
        Ok(json) => failures.eq(
            sparql_rows(&json, &["o", "n"]),
            rows(&[&["T", "4"], &["U", "1"]]),
            label,
        ),
        Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
    }
    check_count_routing(
        &mut failures,
        label,
        &store,
        before,
        TOPK_SITES,
        ObjectCount::UnionDeclines("novelty", None),
    );
    failures.assert_none();
}

/// A bare decimal or big integer as the bound object of `COUNT(?s)`. It
/// matches every numeric datatype that holds the value exactly: an integer, a
/// double, and the NumBig arena's decimals and big integers, which each graph
/// numbers in its own arena.
///
/// Both count lanes read their slices from one shared function. Over a union,
/// each member translates the value into its own arena. Every case is checked
/// with fast paths on (the lane that answered) and off (the general plan),
/// against the same hand-written answer. A typed decimal is the control: the
/// count detector takes no datatype-constrained object, so neither lane runs.
///
/// g1: a `1.5`, b `1.50`, c `"1.5"^^xsd:double`, e `2`, and h a big integer.
/// g2: a `1.5`, f `1.5`, j `3.25`, and i the same big integer as an
/// `xsd:nonNegativeInteger`.
#[tokio::test(flavor = "current_thread")]
async fn numeric_object_count_over_one_graph_and_a_union() {
    let ledger_id = "it/numeric-object-count:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        GRAPH <urn:g1> {
            ex:a ex:price 1.5 .
            ex:b ex:price 1.50 .
            ex:c ex:price "1.5"^^xsd:double .
            ex:e ex:price 2 .
            ex:h ex:serial 123456789012345678901234567890 .
        }
        GRAPH <urn:g2> {
            ex:a ex:price 1.5 .
            ex:f ex:price 1.5 .
            ex:j ex:price 3.25 .
            ex:i ex:serial "123456789012345678901234567890"^^xsd:nonNegativeInteger .
        }
    "#;
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    let count = |ds: &str, triple: &str| {
        format!(
            "{PREFIXES}PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \
             SELECT (COUNT(?s) AS ?n) {ds} WHERE {{ ?s {triple} }}"
        )
    };
    let (g1, g2, both) = (
        "FROM <urn:g1>",
        "FROM <urn:g2>",
        "FROM <urn:g1> FROM <urn:g2>",
    );
    let big = "ex:serial 123456789012345678901234567890";
    // (label, query, answer, lane: `None` for neither lane)
    let cases: Vec<(&str, String, &str, Option<ObjectCount>)> = vec![
        (
            "1.5, g1: 1.5, 1.50 and the double",
            count(g1, "ex:price 1.5"),
            "3",
            Some(ObjectCount::One),
        ),
        (
            "1.5, g2",
            count(g2, "ex:price 1.5"),
            "2",
            Some(ObjectCount::One),
        ),
        (
            "1.5, g1 + g2: a in both, by value: handles in both members",
            count(both, "ex:price 1.5"),
            "4",
            Some(ObjectCount::UnionDeclines("arena_type", None)),
        ),
        (
            "3.25, g1: in no arena of g1",
            count(g1, "ex:price 3.25"),
            "0",
            Some(ObjectCount::One),
        ),
        (
            "3.25, g2",
            count(g2, "ex:price 3.25"),
            "1",
            Some(ObjectCount::One),
        ),
        (
            "3.25, g1 + g2: g2 only",
            count(both, "ex:price 3.25"),
            "1",
            Some(ObjectCount::Union(None)),
        ),
        (
            "2.0, g1: the integer 2",
            count(g1, "ex:price 2.0"),
            "1",
            Some(ObjectCount::One),
        ),
        (
            "2.0, g1 + g2",
            count(both, "ex:price 2.0"),
            "1",
            Some(ObjectCount::Union(None)),
        ),
        (
            "big integer, g1",
            count(g1, big),
            "1",
            Some(ObjectCount::One),
        ),
        (
            "big integer, g2: as nonNegativeInteger",
            count(g2, big),
            "1",
            Some(ObjectCount::One),
        ),
        (
            "big integer, g1 + g2: handles in both members",
            count(both, big),
            "2",
            Some(ObjectCount::UnionDeclines("arena_type", None)),
        ),
        (
            "typed decimal, g1 + g2: neither lane",
            count(both, "ex:price \"1.5\"^^xsd:decimal"),
            "3",
            None,
        ),
    ];
    let mut failures = Failures::default();
    let run = |query: String| {
        let fluree = &fluree;
        let handle = &handle;
        async move {
            db(handle)
                .await
                .query(fluree)
                .sparql(&query)
                .execute_formatted()
                .await
                .map_err(|e| e.to_string())
        }
    };
    {
        let _switch = crate::support::FAST_PATH_SWITCH.read().await;
        for (label, query, want, lane) in &cases {
            let (store, _guard) = init_test_tracing();
            let before = store.all_events().len();
            match run(query.clone()).await {
                Ok(json) => failures.eq(sparql_rows(&json, &["n"]), rows(&[&[want]]), label),
                Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
            }
            match lane {
                Some(lane) => {
                    check_object_count_routing(&mut failures, label, &store, before, *lane);
                }
                None => failures.eq(
                    (
                        stamps(&store, before, OBJECT_COUNT_SITE),
                        stamps(&store, before, OBJECT_COUNT_UNION_SITE),
                    ),
                    (vec![], vec![]),
                    &format!("{label}: (one-graph, union) stamps"),
                ),
            }
        }
    }
    // The general plan gives the same answers.
    {
        let _switch = crate::support::FAST_PATH_SWITCH.write().await;
        let _restore = FastPathSwitchRestore;
        fluree_db_api::set_fast_paths_disabled(true);
        for (label, query, want, _) in &cases {
            match run(query.clone()).await {
                Ok(json) => failures.eq(
                    sparql_rows(&json, &["n"]),
                    rows(&[&[want]]),
                    &format!("{label}, fast paths off"),
                ),
                Err(e) => failures
                    .0
                    .push(format!("{label}, fast paths off: query failed: {e}")),
            }
        }
    }
    failures.assert_none();
}

/// A union member the index holds nothing of: the default graph of a ledger
/// whose facts are all in named graphs. The index has no branch for it, and
/// with no novelty it holds no fact, so it adds nothing to a union: the union
/// lanes answer as if it were absent, and none declines for it.
///
/// g2: a and b `T`, c `U`, a's label. g3: a (the identical triple) and d `T`,
/// d's label.
#[tokio::test(flavor = "current_thread")]
async fn union_lanes_over_a_member_with_no_index() {
    let ledger_id = "it/union-member-no-index:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        GRAPH <urn:g2> {
            ex:a ex:kind ex:T ; ex:label "a" .
            ex:b ex:kind ex:T .
            ex:c ex:kind ex:U .
        }
        GRAPH <urn:g3> {
            ex:a ex:kind ex:T .
            ex:d ex:kind ex:T ; ex:label "d" .
        }
    "#;
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    {
        use fluree_db_binary_index::RunSortOrder;
        let view = handle.snapshot().await;
        let store = view.binary_store.as_ref().expect("indexed");
        for order in [RunSortOrder::Psot, RunSortOrder::Post] {
            assert!(
                store.branch_for_order(0, order).is_none(),
                "the case under test: the default graph has no {order:?} branch"
            );
        }
    }
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;
    let mut failures = Failures::default();
    let run = |query: String| {
        let fluree = &fluree;
        let handle = &handle;
        async move {
            db(handle)
                .await
                .query(fluree)
                .sparql(&query)
                .execute_formatted()
                .await
                .map_err(|e| e.to_string())
        }
    };
    let from_all = format!("FROM <{ledger_id}> FROM <urn:g2> FROM <urn:g3>");
    for (scope, prologue, ds) in [
        ("M1 union default graph", UNION_PRAGMA, ""),
        ("M2 FROM default + g2 + g3", "", from_all.as_str()),
    ] {
        let label = format!("{scope}: FILTER(EXISTS && ...)");
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        match run(format!(
            "{prologue}{PREFIXES}SELECT ?x {ds} WHERE {{ ?x ex:kind ?k \
             FILTER(EXISTS {{ ?x ex:label ?l }} && ?k != ex:U) }}"
        ))
        .await
        {
            Ok(json) => failures.eq(sparql_rows(&json, &["x"]), rows(&[&["a"], &["d"]]), &label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        check_exists_routing(
            &mut failures,
            &label,
            &exists_stamps(&store, before),
            ExistsLane::Union,
        );

        let label = format!("{scope}: the bound-object count");
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        match run(format!(
            "{prologue}{PREFIXES}SELECT (COUNT(?s) AS ?n) {ds} WHERE {{ ?s ex:kind ex:T }}"
        ))
        .await
        {
            Ok(json) => failures.eq(sparql_rows(&json, &["n"]), rows(&[&["3"]]), &label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        check_object_count_routing(
            &mut failures,
            &label,
            &store,
            before,
            ObjectCount::Union(Some("merge")),
        );

        let label = format!("{scope}: the grouped top-k");
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        match run(format!(
            "{prologue}{PREFIXES}SELECT ?o (COUNT(?s) AS ?n) {ds} \
             WHERE {{ ?s ex:kind ?o }} GROUP BY ?o ORDER BY DESC(?n) LIMIT 5"
        ))
        .await
        {
            Ok(json) => failures.eq(
                sparql_rows(&json, &["o", "n"]),
                rows(&[&["T", "3"], &["U", "1"]]),
                &label,
            ),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        check_count_routing(
            &mut failures,
            &label,
            &store,
            before,
            TOPK_SITES,
            ObjectCount::Union(None),
        );
    }
    failures.assert_none();
}

/// One count-lane case: the lane's routing site, a label, the query, its
/// columns, the answer, and whether the lane must answer (`true`) or decline.
type CountLaneCase = (
    &'static str,
    &'static str,
    String,
    &'static [&'static str],
    Rows,
    bool,
);

/// The count lanes the planner picks outside the fused fast paths, which gate
/// at open through `fast_path_store`: the FIRST-header grouped count and
/// bound-object count, and the per-predicate directory count. Each must answer
/// over one graph, and decline at open over several, where it read the scope's
/// `binary_g_id` member alone; the per-predicate count over several graphs is
/// its union variant's instead. Kinds: a and b `T` (default graph, g2), c `U`
/// (g2), d `T` (g3); tags: a and b `x`, d `y`.
#[tokio::test(flavor = "current_thread")]
async fn count_lanes_read_one_graph() {
    let trig = r#"
        @prefix ex: <http://example.org/> .
        ex:a ex:kind ex:T ; ex:tag "x" .
        GRAPH <urn:g2> { ex:b ex:kind ex:T ; ex:tag "x" . ex:c ex:kind ex:U . }
        GRAPH <urn:g3> { ex:d ex:kind ex:T ; ex:tag "y" . }
    "#;
    let (fluree, handle) = indexed_trig_ledger("it/count-lanes-graphs:main", trig).await;
    let object_count = |prologue: &str, ds: &str| {
        format!("{prologue}{PREFIXES}SELECT (COUNT(?s) AS ?n) {ds} WHERE {{ ?s ex:kind ex:T }}")
    };
    let grouped_topk = |prologue: &str, ds: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?o (COUNT(?s) AS ?n) {ds} \
             WHERE {{ ?s ex:kind ?o }} GROUP BY ?o ORDER BY DESC(?n) LIMIT 5"
        )
    };
    let per_predicate = |prologue: &str, ds: &str| {
        format!(
            "{prologue}{PREFIXES}SELECT ?p (COUNT(?s) AS ?n) {ds} \
             WHERE {{ ?s ?p ?o }} GROUP BY ?p"
        )
    };
    let from2 = "FROM <urn:g2> FROM <urn:g3>";
    let cases: Vec<CountLaneCase> = vec![
        (
            "predicate_object_count",
            "object count, the default graph alone",
            object_count("", ""),
            &["n"],
            rows(&[&["1"]]),
            true,
        ),
        (
            "predicate_object_count",
            "object count, FROM g2 alone",
            object_count("", "FROM <urn:g2>"),
            &["n"],
            rows(&[&["1"]]),
            true,
        ),
        (
            "predicate_object_count",
            "object count, union",
            object_count(UNION_PRAGMA, ""),
            &["n"],
            rows(&[&["3"]]),
            false,
        ),
        (
            "predicate_object_count",
            "object count, FROM g2 + g3",
            object_count("", from2),
            &["n"],
            rows(&[&["2"]]),
            false,
        ),
        (
            "predicate_object_count",
            "object count, FROM g3 + g2",
            object_count("", "FROM <urn:g3> FROM <urn:g2>"),
            &["n"],
            rows(&[&["2"]]),
            false,
        ),
        (
            "predicate_object_count",
            "object count, FROM NAMED g2 alone: an empty default graph",
            object_count("", "FROM NAMED <urn:g2>"),
            &["n"],
            rows(&[&["0"]]),
            false,
        ),
        (
            "group_by_object_count_topk",
            "grouped top-k, the default graph alone",
            grouped_topk("", ""),
            &["o", "n"],
            rows(&[&["T", "1"]]),
            true,
        ),
        (
            "group_by_object_count_topk",
            "grouped top-k, FROM g2 alone",
            grouped_topk("", "FROM <urn:g2>"),
            &["o", "n"],
            rows(&[&["T", "1"], &["U", "1"]]),
            true,
        ),
        (
            "group_by_object_count_topk",
            "grouped top-k, union",
            grouped_topk(UNION_PRAGMA, ""),
            &["o", "n"],
            rows(&[&["T", "3"], &["U", "1"]]),
            false,
        ),
        (
            "group_by_object_count_topk",
            "grouped top-k, FROM g2 + g3",
            grouped_topk("", from2),
            &["o", "n"],
            rows(&[&["T", "2"], &["U", "1"]]),
            false,
        ),
        (
            "COUNT by predicate (directory)",
            "per-predicate count, the default graph alone",
            per_predicate("", ""),
            &["p", "n"],
            rows(&[&["kind", "1"], &["tag", "1"]]),
            true,
        ),
        (
            "COUNT by predicate (directory)",
            "per-predicate count, FROM g2 alone",
            per_predicate("", "FROM <urn:g2>"),
            &["p", "n"],
            rows(&[&["kind", "2"], &["tag", "1"]]),
            true,
        ),
        // Over several graphs the per-predicate count is the union variant's,
        // which reads every member; the one-graph count is not planned.
        (
            "stats_count_by_predicate_union",
            "per-predicate count, union",
            per_predicate(UNION_PRAGMA, ""),
            &["p", "n"],
            rows(&[&["kind", "4"], &["tag", "3"]]),
            true,
        ),
        (
            "stats_count_by_predicate_union",
            "per-predicate count, FROM g2 + g3",
            per_predicate("", from2),
            &["p", "n"],
            rows(&[&["kind", "3"], &["tag", "2"]]),
            true,
        ),
    ];
    let mut failures = Failures::default();
    // Other tests in this binary turn the process-wide kill switch on; the
    // read side keeps it off for these queries unless the environment set it.
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;
    for (site, label, query, vars, want, must_fire) in cases {
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = db(&handle)
            .await
            .query(&fluree)
            .sparql(&query)
            .execute_formatted()
            .await;
        match result {
            Ok(json) => failures.eq(sparql_rows(&json, vars), want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
        // The kill switch keeps the planner from choosing these lanes at all,
        // so under it only the answers are checked (the differential run).
        if fluree_db_api::fast_paths_disabled() {
            continue;
        }
        // The lane was reached either way. The FIRST-header lanes stamp
        // `proceed` when planned and a decline when they fall back at open;
        // the directory count stamps once, at open. Either way the lane
        // answered when it was reached and stamped no decline, of any reason.
        let seen = stamps(&store, before, site);
        let reached = !seen.is_empty();
        let declined = seen.iter().any(|o| o == "fallback:gate_declined");
        let other = seen
            .iter()
            .any(|o| o != "proceed" && o != "fallback:gate_declined");
        failures.eq(
            (reached, declined, other),
            (true, !must_fire, false),
            &format!("{label}: `{site}` (reached, declined, declined otherwise)"),
        );
    }
    failures.assert_none();
}

/// A `geof:distance` filter over a default graph of none (`FROM NAMED` alone):
/// no rows, as over any empty default graph. The planner and the geo index
/// search's own refusal share one definition of "reads one graph", so the
/// rewrite is skipped here exactly as over several graphs. A `GRAPH` scope over
/// the named graph is the control.
#[tokio::test(flavor = "current_thread")]
async fn geo_search_over_a_default_graph_of_none() {
    let ledger_id = "it/geo-search-from-named:main";
    let trig = r#"
        @prefix ex: <http://example.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:paris ex:location "POINT(2.3522 48.8566)"^^geo:wktLiteral .
        GRAPH <urn:g2> { ex:london ex:location "POINT(-0.1276 51.5072)"^^geo:wktLiteral . }
    "#;
    let (fluree, handle) = indexed_trig_ledger(ledger_id, trig).await;
    let near = |ds: &str, graph: bool| {
        let pattern = "?place ex:location ?loc \
                       BIND(geof:distance(?loc, \"POINT(2.3522 48.8566)\"^^geo:wktLiteral) AS ?d) \
                       FILTER(?d <= 500000)";
        let body = if graph {
            format!("GRAPH <urn:g2> {{ {pattern} }}")
        } else {
            pattern.to_string()
        };
        format!(
            "{PREFIXES}PREFIX geo: <http://www.opengis.net/ont/geosparql#> \
             PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
             SELECT ?place {ds} WHERE {{ {body} }}"
        )
    };
    let place: &'static [&'static str] = &["place"];
    let cases: Vec<LaneCase> = vec![
        (
            "one graph: the default graph alone",
            near("", false),
            place,
            rows(&[&["paris"]]),
            true,
        ),
        (
            "FROM NAMED g2 alone: an empty default graph",
            near("FROM NAMED <urn:g2>", false),
            place,
            rows(&[]),
            false,
        ),
        (
            "FROM NAMED g2 alone, GRAPH <g2>",
            near("FROM NAMED <urn:g2>", true),
            place,
            rows(&[&["london"]]),
            false,
        ),
    ];
    let mut failures = Failures::default();
    check_lane_cases(&fluree, &handle, "geo_search", cases, &mut failures).await;
    let from_named = json!({
        "@context": {"ex": EX},
        "fromNamed": [{"@id": ledger_id, "graph": "urn:g2"}],
        "select": ["?place"],
        "where": [
            {"@id": "?place", "ex:location": "?loc"},
            ["bind", "?d", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
            ["filter", "(<= ?d 500000)"]
        ]
    });
    check_lane_jsonld(
        &fluree,
        ledger_id,
        "geo_search",
        JsonLaneCase {
            label: "JSON-LD fromNamed g2 alone",
            query: from_named,
            want: rows(&[]),
            must_fire: false,
        },
        &mut failures,
    )
    .await;
    failures.assert_none();
}

/// Cypher over a ledger whose default graph is the union of its graphs (the
/// ledger's `f:unionDefaultGraph` setting). The count, the cycle and the
/// grouped count reach the same single-graph lanes the SPARQL tests pin; over
/// the union they must stand down and read every graph. A ledger holding the
/// same facts in its default graph alone is the oracle, and shows each lane
/// runs there.
#[tokio::test(flavor = "current_thread")]
async fn cypher_lanes_over_the_union_default_graph() {
    let union_id = "it/cypher-lanes-union:main";
    let facts = |dflt: &str, g2: &str, g3: &str| {
        format!(
            "@prefix ex: <http://example.org/> .\n\
             @prefix f: <https://ns.flur.ee/db#> .\n\
             {dflt}\n\
             GRAPH <urn:g2> {{ {g2} }}\n\
             GRAPH <urn:g3> {{ {g3} }}\n"
        )
    };
    let d = "ex:p1 a ex:Person ; ex:knows ex:p2 ; ex:dept ex:Sales .";
    let g2 = "ex:p2 a ex:Person ; ex:knows ex:p3 ; ex:dept ex:Ops . ex:p5 ex:dept ex:Sales .";
    let g3 = "ex:p3 a ex:Person ; ex:knows ex:p1 ; ex:dept ex:Sales . ex:p4 a ex:Person .";
    let config = format!(
        "GRAPH <urn:fluree:{union_id}#config> {{ \
         <urn:config:main> a f:LedgerConfig ; f:queryDefaults <urn:config:query> . \
         <urn:config:query> f:unionDefaultGraph true . }}"
    );
    let (fluree, union) = indexed_trig_ledger(union_id, &(facts(d, g2, g3) + &config)).await;
    // The oracle, on the same Fluree: every fact in the default graph.
    let merged_id = "it/cypher-lanes-merged:main";
    let merged_trig = format!("@prefix ex: <http://example.org/> .\n{d}\n{g2}\n{g3}\n");
    fluree
        .create_ledger(merged_id)
        .await
        .expect("create merged");
    let merged = fluree.ledger_cached(merged_id).await.expect("cache merged");
    fluree
        .stage(&merged)
        .upsert_turtle(&merged_trig)
        .execute()
        .await
        .expect("seed merged");
    rebuild_and_publish_index(&fluree, merged_id).await;
    fluree.disconnect_ledger(merged_id).await;
    let merged = fluree
        .ledger_cached(merged_id)
        .await
        .expect("reload merged");

    let person = "`http://example.org/Person`";
    let knows = "`http://example.org/knows`";
    let dept = "`http://example.org/dept`";
    // (label, Cypher, the lane's routing site, the answer)
    let cases: Vec<(&str, String, &str, Rows)> = vec![
        (
            "count of a label",
            format!("MATCH (n:{person}) RETURN count(n) AS c"),
            "whole-graph scalar aggregates",
            rows(&[&["4"]]),
        ),
        (
            "a triangle",
            format!("MATCH (a)-[:{knows}]->(b)-[:{knows}]->(c)-[:{knows}]->(a) RETURN a, b, c"),
            "cyclic_bgp",
            rows(&[
                &["p1", "p2", "p3"],
                &["p2", "p3", "p1"],
                &["p3", "p1", "p2"],
            ]),
        ),
        (
            "a count per related node",
            format!("MATCH (n)-[:{dept}]->(d) RETURN d, count(n) AS c"),
            "partitioned_group_by",
            rows(&[&["Ops", "1"], &["Sales", "3"]]),
        ),
    ];
    let mut failures = Failures::default();
    // Other tests in this binary turn the process-wide kill switch on; the
    // read side keeps it off for these queries unless the environment set it.
    let _switch = crate::support::FAST_PATH_SWITCH.read().await;
    for (label, cypher, site, want) in cases {
        for (scope, handle, must_fire) in [("union", &union, false), ("merged", &merged, true)] {
            let (store, _guard) = init_test_tracing();
            let before = store.all_events().len();
            let db = db(handle).await;
            let label = format!("{scope}: {label}");
            match fluree.query_cypher(&db, &cypher).await {
                Ok(result) => match result.to_jsonld_async(db.as_graph_db_ref()).await {
                    Ok(json) => failures.eq(jsonld_rows(&json), want.clone(), &label),
                    Err(e) => failures.0.push(format!("{label}: format failed: {e}")),
                },
                Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
            }
            // The kill switch keeps the whole-graph aggregates out of the plan;
            // under it only the answers are checked.
            if fluree_db_api::fast_paths_disabled() {
                continue;
            }
            let fired = stamps(&store, before, site).iter().any(|o| o == "proceed");
            failures.eq(fired, must_fire, &format!("{label}: `{site}` ran"));
        }
    }
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// A default graph spanning two ledgers.
//
// Its members read each ledger's own index through the range scan (binding
// provenance needs decoded terms), and ledgers code namespaces independently:
// here `ex:` has a different namespace code in each, because ledger A meets
// another namespace first.

const LEDGER_A: &str = "it/two-ledgers-a:main";
const LEDGER_B: &str = "it/two-ledgers-b:main";

/// Ledgers A and B, both indexed. Labels: a and b "chat"@fr, c "chat"@en.
/// Codes: a and b "A1"^^ex:Code, c "A1" (a plain string). Amounts: a and b
/// 1.5, a2 and c 2.5, so each ledger's sorted run holds both values. Places:
/// Paris in A, London in B.
async fn two_ledgers() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    // Each chunk is its own transaction, so namespace codes are allocated in
    // this order.
    let seed = |ledger_id: &'static str, chunks: &'static [&'static str]| {
        let fluree = &fluree;
        async move {
            fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");
            let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
            for ttl in chunks {
                fluree
                    .stage(&handle)
                    .upsert_turtle(ttl)
                    .execute()
                    .await
                    .expect("seed");
            }
            rebuild_and_publish_index(fluree, ledger_id).await;
            fluree.disconnect_ledger(ledger_id).await;
            let view = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("reload")
                .snapshot()
                .await;
            assert!(view.binary_store.is_some(), "{ledger_id}: indexed");
            view.snapshot
                .encode_iri("http://example.org/Code")
                .expect("ex:Code encodes")
                .namespace_code
        }
    };
    let ex_in_a = seed(
        LEDGER_A,
        &[
            r#"@prefix other: <http://other.example/> . other:x other:p "first" ."#,
            r#"
            @prefix ex: <http://example.org/> .
            @prefix geo: <http://www.opengis.net/ont/geosparql#> .
            ex:a ex:label "chat"@fr ; ex:code "A1"^^ex:Code ; ex:amount 1.5 ;
                 ex:location "POINT(2.3522 48.8566)"^^geo:wktLiteral .
            ex:a2 ex:amount 2.5 .
        "#,
        ],
    )
    .await;
    let ex_in_b = seed(
        LEDGER_B,
        &[r#"
            @prefix ex: <http://example.org/> .
            @prefix geo: <http://www.opengis.net/ont/geosparql#> .
            ex:b ex:label "chat"@fr ; ex:code "A1"^^ex:Code ; ex:amount 1.5 ;
                 ex:location "POINT(-0.1276 51.5072)"^^geo:wktLiteral .
            ex:c ex:label "chat"@en ; ex:code "A1" ; ex:amount 2.5 .
        "#],
    )
    .await;
    assert_ne!(ex_in_a, ex_in_b, "the ledgers code `ex:` differently");
    fluree
}

/// A SPARQL query through the connection (so `FROM` can name ledgers), its
/// rows as [`jsonld_rows`] gives them. Formatted through ledger A, the primary
/// of a `FROM <A> FROM <B>` query, or through B when A is not named.
async fn connection_sparql_rows(fluree: &Fluree, query: &str) -> Result<Rows, String> {
    let result = fluree
        .query_connection_sparql(query)
        .await
        .map_err(|e| e.to_string())?;
    let primary = if query.contains(&format!("<{LEDGER_A}>")) {
        LEDGER_A
    } else {
        LEDGER_B
    };
    let ledger = fluree.ledger(primary).await.expect("ledger");
    let json = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .map_err(|e| e.to_string())?;
    Ok(jsonld_rows(&json))
}

/// A JSON-LD query through the connection, so `from` can name ledgers.
async fn connection_jsonld(fluree: &Fluree, ledger_id: &str, query: &JsonValue) -> JsonValue {
    let result = fluree
        .query_connection(query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
}

/// A bound string's datatype is part of the term: a language-tagged literal or
/// one of a custom datatype matches only its own kind. Over a default graph of
/// two ledgers (whose members read through the range scan) a bound one matched
/// nothing, constant, VALUES or joined, because the range read every bound
/// string as `xsd:string`; and a custom datatype is coded in each ledger's own
/// namespace space. Each ledger alone is the control, and plain strings,
/// integers and expression equality are too.
#[tokio::test(flavor = "current_thread")]
async fn typed_string_literals_over_a_default_graph_of_two_ledgers() {
    let fluree = two_ledgers().await;
    let both = format!("FROM <{LEDGER_A}> FROM <{LEDGER_B}>");
    let only_b = format!("FROM <{LEDGER_B}>");
    let q = |ds: &str, select: &str, body: &str| {
        format!("{PREFIXES}SELECT {select} {ds} WHERE {{ {body} }}")
    };
    let cases = [
        (
            "a language-tagged constant",
            q(&both, "?s", "?s ex:label \"chat\"@fr"),
            rows(&[&["a"], &["b"]]),
        ),
        (
            "another tag",
            q(&both, "?s", "?s ex:label \"chat\"@en"),
            rows(&[&["c"]]),
        ),
        (
            "VALUES of a language-tagged literal",
            q(&both, "?s", "VALUES ?l { \"chat\"@fr } ?s ex:label ?l"),
            rows(&[&["a"], &["b"]]),
        ),
        (
            "a join on a language-tagged literal",
            q(
                &both,
                "?x ?y",
                "?x ex:label ?l . ?y ex:label ?l FILTER(?x != ?y)",
            ),
            rows(&[&["a", "b"], &["b", "a"]]),
        ),
        (
            "a custom-datatype constant",
            q(&both, "?s", "?s ex:code \"A1\"^^ex:Code"),
            rows(&[&["a"], &["b"]]),
        ),
        (
            "VALUES of a custom-datatype literal",
            q(&both, "?s", "VALUES ?c { \"A1\"^^ex:Code } ?s ex:code ?c"),
            rows(&[&["a"], &["b"]]),
        ),
        (
            "a join on a custom-datatype literal",
            q(
                &both,
                "?x ?y",
                "?x ex:code ?c . ?y ex:code ?c FILTER(?x != ?y)",
            ),
            rows(&[&["a", "b"], &["b", "a"]]),
        ),
        (
            "control: a plain string",
            q(&both, "?s", "?s ex:code \"A1\""),
            rows(&[&["c"]]),
        ),
        (
            "control: a decimal join",
            q(
                &both,
                "?x ?y",
                "?x ex:amount ?v . ?y ex:amount ?v FILTER(?x != ?y)",
            ),
            rows(&[&["a", "b"], &["a2", "c"], &["b", "a"], &["c", "a2"]]),
        ),
        (
            "control: expression equality",
            q(
                &both,
                "?x ?y",
                "?x ex:label ?a . ?y ex:label ?b FILTER(?a = ?b && ?x != ?y)",
            ),
            rows(&[&["a", "b"], &["b", "a"]]),
        ),
        (
            "control: ledger B alone",
            q(&only_b, "?s", "?s ex:label \"chat\"@fr"),
            rows(&[&["b"]]),
        ),
        (
            "control: ledger B alone, custom datatype",
            q(&only_b, "?s", "?s ex:code \"A1\"^^ex:Code"),
            rows(&[&["b"]]),
        ),
    ];
    let mut failures = Failures::default();
    for (label, query, want) in cases {
        match connection_sparql_rows(&fluree, &query).await {
            Ok(got) => failures.eq(got, want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
    // JSON-LD twins: a language-tagged and a custom-datatype constant.
    for (label, object, want) in [
        (
            "JSON-LD language-tagged constant",
            json!({"@value": "chat", "@language": "fr"}),
            rows(&[&["a"], &["b"]]),
        ),
        (
            "JSON-LD custom-datatype constant",
            json!({"@value": "A1", "@type": "ex:Code"}),
            rows(&[&["a"], &["b"]]),
        ),
    ] {
        let query = json!({
            "@context": {"ex": EX},
            "from": [LEDGER_A, LEDGER_B],
            "select": ["?s"],
            "where": [{"@id": "?s", "ex:label": object.clone()}]
        });
        let query = if label.contains("custom") {
            let mut q = query;
            q["where"] = json!([{"@id": "?s", "ex:code": object}]);
            q
        } else {
            query
        };
        let json = connection_jsonld(&fluree, LEDGER_A, &query).await;
        failures.eq(jsonld_rows(&json), want, label);
    }
    failures.assert_none();
}

/// The same range scan serves one ledger under a view policy that covers the
/// scanned predicate (the binary scan has no per-flake policy filter). A
/// language-tagged and a custom-datatype constant must match there too.
#[tokio::test(flavor = "current_thread")]
async fn typed_string_literals_under_a_view_policy() {
    let fluree = two_ledgers().await;
    let policy = json!([{
        "@id": "http://example.org/viewLabelsAndCodes",
        "f:action": "f:view",
        "f:onProperty": [{"@id": "http://example.org/label"}, {"@id": "http://example.org/code"}],
        "f:allow": true
    }]);
    let view = fluree
        .db_with_policy(
            LEDGER_B,
            &fluree_db_api::GovernanceOptions {
                policy: Some(policy),
                default_allow: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("db_with_policy");
    assert!(!view.is_root(), "an enforcing view");
    let mut failures = Failures::default();
    for (label, body, want) in [
        (
            "a language-tagged constant",
            "?s ex:label \"chat\"@fr",
            rows(&[&["b"]]),
        ),
        ("another tag", "?s ex:label \"chat\"@en", rows(&[&["c"]])),
        (
            "a custom-datatype constant",
            "?s ex:code \"A1\"^^ex:Code",
            rows(&[&["b"]]),
        ),
        (
            "control: a plain string",
            "?s ex:code \"A1\"",
            rows(&[&["c"]]),
        ),
    ] {
        let query = format!("{PREFIXES}SELECT ?s WHERE {{ {body} }}");
        match fluree.query(&view, query.as_str()).await {
            Ok(result) => match result.to_jsonld(&view.snapshot) {
                Ok(json) => failures.eq(jsonld_rows(&json), want, label),
                Err(e) => failures.0.push(format!("{label}: format failed: {e}")),
            },
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
    failures.assert_none();
}

/// SPARQL parameters are substituted as constants before lowering, so a
/// parameterized query answers as the inline one on the lanes that meet a
/// value in two forms: a subject IRI whose predicates an OPTIONAL, a MINUS and
/// an EXISTS correlate as subjects, a number a FILTER compares with an indexed
/// value, and a predicate IRI, in every index state; and a language-tagged and a
/// custom-datatype literal the range scan matches over a default graph of two
/// ledgers.
#[tokio::test(flavor = "current_thread")]
async fn sparql_parameters_answer_as_the_inline_values() {
    let param = |v: JsonValue| -> fluree_db_api::SparqlParamMap {
        v.as_object().expect("a parameter object").clone()
    };
    let s1 = || json!({"s": {"@id": "http://example.org/s1"}});
    let cases: Vec<(&str, &str, JsonValue, &[&str], Rows)> = vec![
        (
            "a subject IRI, OPTIONAL on its predicates",
            "SELECT ?p ?l WHERE { $s ?p ?o OPTIONAL { ?p rdfs:label ?l } }",
            s1(),
            &["p", "l"],
            rows(&[
                &["derivedFrom", "derived from"],
                &["mentions", "-"],
                &["position", "-"],
                &["text", "text"],
            ]),
        ),
        (
            "a subject IRI, MINUS on its predicates",
            "SELECT ?p WHERE { $s ?p ?o MINUS { ?p rdfs:label ?l } }",
            s1(),
            &["p"],
            rows(&[&["mentions"], &["position"]]),
        ),
        (
            "a subject IRI, EXISTS on its predicates",
            "SELECT ?p WHERE { $s ?p ?o FILTER EXISTS { ?p rdfs:label ?l } }",
            s1(),
            &["p"],
            rows(&[&["derivedFrom"], &["text"]]),
        ),
        (
            "a number a FILTER compares",
            "SELECT ?s WHERE { ?s ex:position ?n FILTER(?n = $n) }",
            json!({"n": 2}),
            &["s"],
            rows(&[&["s2"]]),
        ),
        (
            "a predicate IRI",
            "SELECT ?s ?o WHERE { ?s $p ?o }",
            json!({"p": {"@id": "http://example.org/position"}}),
            &["s", "o"],
            rows(&[&["s1", "1"], &["s2", "2"], &["s3", "3"], &["s4", "4"]]),
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/sparql-params:main").await;
        for (label, body, values, vars, want) in &cases {
            let query = format!("{PREFIXES}{body}");
            match db(&handle)
                .await
                .query(&fluree)
                .sparql(&query)
                .params(param(values.clone()))
                .execute_formatted()
                .await
            {
                Ok(json) => failures.eq(
                    sparql_rows(&json, vars),
                    want.clone(),
                    &format!("{state:?} / {label}"),
                ),
                Err(e) => failures
                    .0
                    .push(format!("{state:?} / {label}: query failed: {e}")),
            }
        }
    }

    let fluree = two_ledgers().await;
    let both = format!("FROM <{LEDGER_A}> FROM <{LEDGER_B}>");
    for (label, body, values) in [
        (
            "a language-tagged literal over two ledgers",
            format!("{PREFIXES}SELECT ?s {both} WHERE {{ ?s ex:label $l }}"),
            json!({"l": {"@value": "chat", "@language": "fr"}}),
        ),
        (
            "a custom-datatype literal over two ledgers",
            format!("{PREFIXES}SELECT ?s {both} WHERE {{ ?s ex:code $c }}"),
            json!({"c": {"@value": "A1", "@type": "http://example.org/Code"}}),
        ),
    ] {
        let result = fluree
            .query_from()
            .sparql(&body)
            .params(param(values))
            .format(fluree_db_api::FormatterConfig::sparql_json())
            .execute_formatted()
            .await;
        match result {
            Ok(json) => failures.eq(sparql_rows(&json, &["s"]), rows(&[&["a"], &["b"]]), label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
    failures.assert_none();
}

/// Custom datatypes that only a member ledger codes (#1771). The primary
/// ledger C has never seen `ct:`, so a pattern's `ct:T` is lowered against it
/// as a full IRI, and the member D's literals carry D's own codes for `ct:T`
/// and `ct:U`. The scan re-codes the pattern's datatype into D's space, and
/// D's literal datatypes are re-coded into C's as they leave the member, two
/// datatypes in one column, so COUNT(DISTINCT) keys each as itself; before
/// and after the first index.
#[tokio::test(flavor = "current_thread")]
async fn custom_datatypes_only_a_member_ledger_codes() {
    let mut failures = Failures::default();
    for indexed in [false, true] {
        let state = if indexed { "indexed" } else { "novelty" };
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_c = format!("it/member-datatypes-c-{state}:main");
        let ledger_d = format!("it/member-datatypes-d-{state}:main");
        for (ledger_id, ttl) in [
            (
                &ledger_c,
                r#"@prefix ex: <http://example.org/> . ex:c1 ex:p "a" ."#,
            ),
            (
                &ledger_d,
                r#"@prefix ex: <http://example.org/> . @prefix ct: <http://custom-types.test/> .
                   ex:d1 ex:p "a"^^ct:T . ex:d2 ex:p "a"^^ct:U ."#,
            ),
        ] {
            fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");
            let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
            fluree
                .stage(&handle)
                .upsert_turtle(ttl)
                .execute()
                .await
                .expect("seed");
            if indexed {
                rebuild_and_publish_index(&fluree, ledger_id).await;
                fluree.disconnect_ledger(ledger_id).await;
            }
        }
        let both = format!("FROM <{ledger_c}> FROM <{ledger_d}>");
        let ct = "PREFIX ct: <http://custom-types.test/> ";
        for (label, body, want) in [
            (
                "a constant of a datatype only D codes",
                format!("SELECT ?s {both} WHERE {{ ?s ex:p \"a\"^^ct:T }}"),
                rows(&[&["d1"]]),
            ),
            (
                "each literal's datatype, two from D in one column",
                format!("SELECT ?s (STR(DATATYPE(?o)) AS ?dt) {both} WHERE {{ ?s ex:p ?o }}"),
                rows(&[
                    &["c1", "http://www.w3.org/2001/XMLSchema#string"],
                    &["d1", "http://custom-types.test/T"],
                    &["d2", "http://custom-types.test/U"],
                ]),
            ),
            (
                "COUNT(DISTINCT) over the three literals",
                format!("SELECT (COUNT(DISTINCT ?o) AS ?n) {both} WHERE {{ ?s ex:p ?o }}"),
                rows(&[&["3"]]),
            ),
        ] {
            let label = format!("{state} / {label}");
            let query = format!("{PREFIXES}{ct}{body}");
            match fluree.query_connection_sparql(&query).await {
                Ok(result) => {
                    let ledger = fluree.ledger(&ledger_c).await.expect("ledger C");
                    match result.to_jsonld_async(ledger.as_graph_db_ref(0)).await {
                        Ok(json) => failures.eq(jsonld_rows(&json), want, &label),
                        Err(e) => failures.0.push(format!("{label}: format failed: {e}")),
                    }
                }
                Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
            }
        }
    }
    failures.assert_none();
}

/// The partitioned GROUP BY and the geo index search over a default graph of
/// two ledgers: both read one graph's index, so both stand down and every
/// member answers.
#[tokio::test(flavor = "current_thread")]
async fn lanes_over_a_default_graph_of_two_ledgers() {
    let fluree = two_ledgers().await;
    let both = format!("FROM <{LEDGER_A}> FROM <{LEDGER_B}>");
    let cases = [
        (
            "GROUP BY over one predicate",
            format!(
                "{PREFIXES}SELECT ?v (COUNT(*) AS ?k) {both} WHERE {{ ?s ex:amount ?v }} GROUP BY ?v"
            ),
            rows(&[&["1.5", "2"], &["2.5", "2"]]),
        ),
        (
            "a geof:distance filter",
            format!(
                "{PREFIXES}PREFIX geo: <http://www.opengis.net/ont/geosparql#> \
                 PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
                 SELECT ?s {both} WHERE {{ ?s ex:location ?loc \
                 BIND(geof:distance(?loc, \"POINT(2.3522 48.8566)\"^^geo:wktLiteral) AS ?d) \
                 FILTER(?d <= 500000) }}"
            ),
            rows(&[&["a"], &["b"]]),
        ),
    ];
    let mut failures = Failures::default();
    for (label, query, want) in cases {
        match connection_sparql_rows(&fluree, &query).await {
            Ok(got) => failures.eq(got, want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
    failures.assert_none();
}

/// A `geof:distance` filter over a dataset that spans ledgers through
/// `FROM NAMED`: its default graph is one graph, ledger A, but a dataset
/// spanning ledgers reads every graph through the range scan, with no binary
/// index attached. The geo index search needs that index and has no fallback,
/// so the planner keeps the patterns, at top level and inside `GRAPH <B>`.
#[tokio::test(flavor = "current_thread")]
async fn geo_filter_over_a_dataset_spanning_ledgers_through_from_named() {
    let fluree = two_ledgers().await;
    let ds = format!("FROM <{LEDGER_A}> FROM NAMED <{LEDGER_B}>");
    let near = |graph: bool| {
        let pattern = "?s ex:location ?loc \
                       BIND(geof:distance(?loc, \"POINT(2.3522 48.8566)\"^^geo:wktLiteral) AS ?d) \
                       FILTER(?d <= 500000)";
        let body = if graph {
            format!("GRAPH <{LEDGER_B}> {{ {pattern} }}")
        } else {
            pattern.to_string()
        };
        format!(
            "{PREFIXES}PREFIX geo: <http://www.opengis.net/ont/geosparql#> \
             PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
             SELECT ?s {ds} WHERE {{ {body} }}"
        )
    };
    let mut failures = Failures::default();
    for (label, query, want) in [
        ("top level: ledger A's place", near(false), rows(&[&["a"]])),
        ("GRAPH <B>: ledger B's place", near(true), rows(&[&["b"]])),
    ] {
        match connection_sparql_rows(&fluree, &query).await {
            Ok(got) => failures.eq(got, want, label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
    let jsonld = |graph: bool| {
        let pattern = json!([
            {"@id": "?s", "ex:location": "?loc"},
            ["bind", "?d", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
            ["filter", "(<= ?d 500000)"]
        ]);
        let where_ = if graph {
            // ["graph", name, pattern1, pattern2, ...]
            let mut clause = vec![json!("graph"), json!(LEDGER_B)];
            clause.extend(pattern.as_array().expect("patterns").iter().cloned());
            json!([clause])
        } else {
            pattern
        };
        json!({
            "@context": {"ex": EX},
            "from": LEDGER_A,
            "fromNamed": [LEDGER_B],
            "select": ["?s"],
            "where": where_
        })
    };
    for (label, query, want) in [
        ("JSON-LD top level", jsonld(false), rows(&[&["a"]])),
        ("JSON-LD graph B", jsonld(true), rows(&[&["b"]])),
    ] {
        match fluree.query_connection(&query).await {
            Ok(result) => {
                let ledger = fluree.ledger(LEDGER_A).await.expect("ledger A");
                match result.to_jsonld_async(ledger.as_graph_db_ref(0)).await {
                    Ok(json) => failures.eq(jsonld_rows(&json), want, label),
                    Err(e) => failures.0.push(format!("{label}: format failed: {e}")),
                }
            }
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
    failures.assert_none();
}

/// A `geof:distance` filter on a ledger with no binary index attached (before
/// its first index): the geo index search needs one and has no fallback, so
/// the planner keeps the patterns. Indexed, the same ledger answers the same
/// through the geo index search.
#[tokio::test(flavor = "current_thread")]
async fn geo_filter_before_and_after_the_first_index() {
    let ledger_id = "it/geo-before-index:main";
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(
            r#"
            @prefix ex: <http://example.org/> .
            @prefix geo: <http://www.opengis.net/ont/geosparql#> .
            ex:paris ex:location "POINT(2.3522 48.8566)"^^geo:wktLiteral .
            ex:london ex:location "POINT(-0.1276 51.5072)"^^geo:wktLiteral .
            ex:berlin ex:location "POINT(13.4050 52.5200)"^^geo:wktLiteral .
        "#,
        )
        .execute()
        .await
        .expect("seed");
    assert!(
        handle.snapshot().await.binary_store.is_none(),
        "no index yet"
    );
    let query = format!(
        "{PREFIXES}PREFIX geo: <http://www.opengis.net/ont/geosparql#> \
         PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \
         SELECT ?place WHERE {{ ?place ex:location ?loc \
         BIND(geof:distance(?loc, \"POINT(2.3522 48.8566)\"^^geo:wktLiteral) AS ?d) \
         FILTER(?d <= 500000) }}"
    );
    let near = rows(&[&["london"], &["paris"]]);
    let place: &'static [&'static str] = &["place"];
    let mut failures = Failures::default();
    check_lane_cases(
        &fluree,
        &handle,
        "geo_search",
        vec![(
            "before the first index",
            query.clone(),
            place,
            near.clone(),
            false,
        )],
        &mut failures,
    )
    .await;
    rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
    assert!(handle.snapshot().await.binary_store.is_some(), "indexed");
    check_lane_cases(
        &fluree,
        &handle,
        "geo_search",
        vec![("after the first index", query, place, near, true)],
        &mut failures,
    )
    .await;
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// Arena-backed literals crossing a GRAPH scope.
//
// A GRAPH scope's scans bind big numbers and vectors as handles into its own
// graph's arenas, counted from 0 per graph and predicate. A handle that
// crosses the scope's boundary, out with its rows or in with the row that
// seeds it, must be decoded through the graph that bound it: otherwise it is
// printed or matched as another graph's value, or reaches a union of graphs
// that has no single graph to decode it in.
// ---------------------------------------------------------------------------

/// One vector per graph, each handle 0 of its own arena: x1's in the default
/// graph, y1's in g2, z1's in g3. One big number per graph, each handle 0 too:
/// x1's 1.5, y1's 7.5, z1's 9.5. Indexed.
async fn graph_scope_arena_ledger(ledger_id: &str) -> (Fluree, LedgerHandle) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    let trig = r#"
        @prefix ex: <http://example.org/> .
        @prefix f: <https://ns.flur.ee/db#> .
        ex:x1 ex:emb "[1.0, 0.0]"^^f:embeddingVector ; ex:amount 1.5 .
        GRAPH <urn:g2> {
            ex:y1 ex:emb "[0.0, 1.0]"^^f:embeddingVector ; ex:amount 7.5 .
        }
        GRAPH <urn:g3> {
            ex:z1 ex:emb "[0.5, 0.5]"^^f:embeddingVector ; ex:amount 9.5 .
        }
    "#;
    fluree
        .stage(&handle)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("seed TriG");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
    let view = handle.snapshot().await;
    assert!(view.binary_store.is_some(), "indexed");
    assert!(view.novelty.is_empty(), "no novelty");
    (fluree, handle)
}

/// `(label, SPARQL body, projected variables, expected rows)`.
type SparqlCase<'a> = (&'a str, String, &'a [&'a str], &'a [&'a [&'a str]]);

/// Run each case, recording a mismatch or a query error.
async fn check_sparql_cases(
    fluree: &Fluree,
    handle: &LedgerHandle,
    cases: &[SparqlCase<'_>],
    failures: &mut Failures,
) {
    for (label, body, vars, expected) in cases {
        let query = format!("{PREFIXES}{body}");
        let result = db(handle)
            .await
            .query(fluree)
            .sparql(&query)
            .execute_formatted()
            .await;
        match result {
            Ok(json) => failures.eq(sparql_rows(&json, vars), rows(expected), label),
            Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn vector_leaving_a_graph_scope_keeps_its_graphs_value() {
    let (fluree, handle) = graph_scope_arena_ledger("it/graph-scope-vector-exit:main").await;
    let mut failures = Failures::default();
    let cases: &[SparqlCase<'_>] = &[
        (
            "a default-graph vector (control)",
            "SELECT ?s ?e WHERE { ?s ex:emb ?e }".into(),
            &["s", "e"],
            &[&["x1", "[1.0,0.0]"]],
        ),
        (
            "a vector projected out of GRAPH",
            "SELECT ?s ?e WHERE { GRAPH <urn:g2> { ?s ex:emb ?e } }".into(),
            &["s", "e"],
            &[&["y1", "[0.0,1.0]"]],
        ),
        (
            "a default-graph vector joined against a GRAPH scope",
            "SELECT ?a ?b WHERE { ?a ex:emb ?e . GRAPH <urn:g2> { ?b ex:emb ?e } }".into(),
            &["a", "b"],
            &[],
        ),
    ];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;

    let query = json!({
        "@context": context(),
        "select": ["?s", "?e"],
        "where": [["graph", "urn:g2", {"@id": "?s", "ex:emb": "?e"}]]
    });
    failures.eq(
        jsonld_rows(&jsonld(&fluree, &handle, &query).await),
        rows(&[&["y1", "[0.0,1.0]"]]),
        "a vector projected out of GRAPH (JSON-LD)",
    );
    failures.assert_none();
}

#[tokio::test(flavor = "current_thread")]
async fn arena_literals_entering_a_graph_scope_keep_their_graphs_value() {
    let (fluree, handle) = graph_scope_arena_ledger("it/graph-scope-arena-entry:main").await;
    let mut failures = Failures::default();
    // The OPTIONAL runs after the row that seeds it, so its GRAPH scope
    // receives x1's handles; no graph-2 value equals x1's.
    let cases: &[SparqlCase<'_>] = &[
        (
            "a default-graph vector carried into a GRAPH scope",
            "SELECT ?b WHERE { ex:x1 ex:emb ?e . OPTIONAL { GRAPH <urn:g2> { ?b ex:emb ?e } } }"
                .into(),
            &["b"],
            &[&["-"]],
        ),
        (
            "a default-graph big number carried into a GRAPH scope",
            "SELECT ?y WHERE { ex:x1 ex:amount ?v . \
             OPTIONAL { GRAPH <urn:g2> { ?y ex:amount ?v } } }"
                .into(),
            &["y"],
            &[&["-"]],
        ),
    ];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;

    let query = json!({
        "@context": context(),
        "select": ["?b"],
        "where": [
            {"@id": "ex:x1", "ex:emb": "?e"},
            ["optional", ["graph", "urn:g2", {"@id": "?b", "ex:emb": "?e"}]]
        ]
    });
    failures.eq(
        jsonld_rows(&jsonld(&fluree, &handle, &query).await),
        rows(&[&["-"]]),
        "a default-graph vector carried into a GRAPH scope (JSON-LD)",
    );
    failures.assert_none();
}

#[tokio::test(flavor = "current_thread")]
async fn graph_scope_vector_projected_inside_a_union() {
    let (fluree, handle) = graph_scope_arena_ledger("it/graph-scope-vector-union-proj:main").await;
    let mut failures = Failures::default();
    let cases: &[SparqlCase<'_>] = &[(
        "a vector projected out of GRAPH inside a union",
        "SELECT ?s ?e FROM <urn:g2> FROM <urn:g3> FROM NAMED <urn:g3> \
         WHERE { GRAPH <urn:g3> { ?s ex:emb ?e } }"
            .into(),
        &["s", "e"],
        &[&["z1", "[0.5,0.5]"]],
    )];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;
    failures.assert_none();
}

#[tokio::test(flavor = "current_thread")]
async fn graph_scope_vector_joined_with_a_union() {
    let (fluree, handle) = graph_scope_arena_ledger("it/graph-scope-vector-union-join:main").await;
    let mut failures = Failures::default();
    let cases: &[SparqlCase<'_>] = &[(
        "a GRAPH-scope vector joined with a union",
        "SELECT ?s ?t FROM <urn:g2> FROM <urn:g3> FROM NAMED <urn:g3> \
         WHERE { GRAPH <urn:g3> { ?s ex:emb ?e } ?t ex:emb ?e }"
            .into(),
        &["s", "t"],
        &[&["z1", "z1"]],
    )];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;
    failures.assert_none();
}

/// The union's first graph is the scope's own, so the context its rows return
/// to starts on the scope's graph id, yet spans two graphs and decodes in
/// neither.
#[tokio::test(flavor = "current_thread")]
async fn graph_scope_vector_joined_with_a_union_led_by_its_graph() {
    let (fluree, handle) = graph_scope_arena_ledger("it/graph-scope-vector-union-led:main").await;
    let mut failures = Failures::default();
    let cases: &[SparqlCase<'_>] = &[(
        "a GRAPH-scope vector joined with a union led by its graph",
        "SELECT ?s ?t FROM <urn:g3> FROM <urn:g2> FROM NAMED <urn:g3> \
         WHERE { GRAPH <urn:g3> { ?s ex:emb ?e } ?t ex:emb ?e }"
            .into(),
        &["s", "t"],
        &[&["z1", "z1"]],
    )];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;
    failures.assert_none();
}

/// The big-number twin of the case above.
#[tokio::test(flavor = "current_thread")]
async fn graph_scope_big_number_joined_with_a_union_led_by_its_graph() {
    let (fluree, handle) = graph_scope_arena_ledger("it/graph-scope-numbig-union-led:main").await;
    let mut failures = Failures::default();
    let cases: &[SparqlCase<'_>] = &[(
        "a GRAPH-scope big number joined with a union led by its graph",
        "SELECT ?s ?t FROM <urn:g3> FROM <urn:g2> FROM NAMED <urn:g3> \
         WHERE { GRAPH <urn:g3> { ?s ex:amount ?v } ?t ex:amount ?v }"
            .into(),
        &["s", "t"],
        &[&["z1", "z1"]],
    )];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;
    failures.assert_none();
}

/// A same-ledger SERVICE inside a GRAPH scope reads the dataset's default
/// graph (g2) while the scope around it reads g3: it is the same crossing.
#[tokio::test(flavor = "current_thread")]
async fn service_reading_another_graph_keeps_arena_values() {
    let ledger_id = "it/service-arena-crossing:main";
    let (fluree, handle) = graph_scope_arena_ledger(ledger_id).await;
    let mut failures = Failures::default();
    let cases: &[SparqlCase<'_>] = &[
        (
            "a vector projected out of a SERVICE that reads another graph",
            format!(
                "SELECT ?t ?e FROM <urn:g2> FROM NAMED <urn:g3> WHERE {{ GRAPH <urn:g3> {{ \
                 ?s ex:emb ?x SERVICE <fluree:ledger:{ledger_id}> {{ ?t ex:emb ?e }} }} }}"
            ),
            &["t", "e"],
            &[&["y1", "[0.0,1.0]"]],
        ),
        (
            "a vector carried into a SERVICE that reads another graph",
            format!(
                "SELECT ?s ?t FROM <urn:g2> FROM NAMED <urn:g3> WHERE {{ GRAPH <urn:g3> {{ \
                 ?s ex:emb ?e SERVICE <fluree:ledger:{ledger_id}> {{ ?t ex:emb ?e }} }} }}"
            ),
            &["s", "t"],
            &[],
        ),
    ];
    check_sparql_cases(&fluree, &handle, cases, &mut failures).await;
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// Encoded values outside triple patterns: property-path endpoints and GRAPH.
// ---------------------------------------------------------------------------

/// A predicate bound in predicate position (`EncodedPid`) as a correlated
/// property-path endpoint, and as a graph name. An endpoint the path cannot
/// resolve falls into the full-closure branch and pairs the row with every
/// closure pair. The graph name was already answered right (its binding
/// reaches `GRAPH` materialized); it is pinned here alongside.
#[tokio::test(flavor = "current_thread")]
async fn predicate_bindings_as_path_endpoints_and_graph_names() {
    let mut failures = Failures::default();
    for novelty in [false, true] {
        let ledger_id = if novelty {
            "it/predicate-endpoints-novelty:main"
        } else {
            "it/predicate-endpoints:main"
        };
        let fluree = FlureeBuilder::memory().build_memory();
        fluree
            .create_ledger(ledger_id)
            .await
            .expect("create ledger");
        let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
        let trig = r#"
            @prefix ex: <http://example.org/> .
            ex:s1 ex:text "one" ; ex:derivedFrom ex:doc1 .
            ex:text ex:parentProp ex:content .
            ex:content ex:parentProp ex:any .
            ex:derivedFrom ex:parentProp ex:relation .
            ex:s2 ex:graphNamed "g" .
            GRAPH ex:graphNamed { ex:a ex:b ex:c . }
        "#;
        fluree
            .stage(&handle)
            .upsert_turtle(trig)
            .execute()
            .await
            .expect("seed TriG");
        rebuild_and_publish_index(&fluree, ledger_id).await;
        fluree.disconnect_ledger(ledger_id).await;
        let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
        if novelty {
            fluree
                .stage(&handle)
                .insert(&unrelated_write())
                .execute()
                .await
                .expect("pending write");
        }
        let state = if novelty { "novelty" } else { "indexed" };
        let cases: &[SurfaceCase] = &[
            (
                "path endpoint",
                "SELECT ?p ?super WHERE { ex:s1 ?p ?o . ?p ex:parentProp+ ?super }",
                &["p", "super"],
                &[
                    &["derivedFrom", "relation"],
                    &["text", "any"],
                    &["text", "content"],
                ],
            ),
            (
                "graph name",
                "SELECT ?g ?a WHERE { ex:s2 ?g ?o . GRAPH ?g { ?a ex:b ?c } }",
                &["g", "a"],
                &[&["graphNamed", "a"]],
            ),
        ];
        for (label, body, vars, expected) in cases {
            let result = sparql(&fluree, &handle, body).await;
            failures.eq(
                sparql_rows(&result, vars),
                rows(expected),
                &format!("{state} / {label}"),
            );
        }
    }
    failures.assert_none();
}

/// A batched join binds a chunk minted since the last index by its novelty
/// id; as a correlated path endpoint it must resolve through the novelty
/// dictionary, not fall into the full closure.
#[tokio::test(flavor = "current_thread")]
async fn novelty_minted_subject_as_path_endpoint() {
    let base = json!({"@context": context(), "@graph": [
        {"@id": "ex:doc1", "@type": "ex:Doc", "ex:title": "Doc 1"},
        {"@id": "ex:s1", "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:next": {"@id": "ex:s2"}},
        {"@id": "ex:s2", "ex:next": {"@id": "ex:s3"}},
        {"@id": "ex:t1", "ex:next": {"@id": "ex:t2"}}
    ]});
    let novelty = json!({"@context": context(), "@id": "ex:sN",
        "ex:derivedFrom": {"@id": "ex:doc1"}, "ex:next": {"@id": "ex:s1"}});
    let (fluree, handle) =
        indexed_then_novelty("it/novelty-path-endpoint:main", &base, &novelty).await;
    let mut failures = Failures::default();
    let result = sparql(
        &fluree,
        &handle,
        "SELECT ?s ?n WHERE { ?d a ex:Doc ; ex:title \"Doc 1\" . ?s ex:derivedFrom ?d . \
         ?s ex:next+ ?n }",
    )
    .await;
    failures.eq(
        sparql_rows(&result, &["s", "n"]),
        rows(&[
            &["s1", "s2"],
            &["s1", "s3"],
            &["sN", "s1"],
            &["sN", "s2"],
            &["sN", "s3"],
        ]),
        "novelty-minted path endpoint",
    );
    failures.assert_none();
}

/// The lane's novelty merge: with a base link retracted and links asserted
/// since the last index, the lane drops the retracted row and injects the
/// asserted ones.
#[tokio::test(flavor = "current_thread")]
async fn optional_lane_merges_pending_retracts_and_asserts() {
    let ledger_id = "it/optional-lane-merge:main";
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .insert(&chunks_base(false, false))
        .execute()
        .await
        .expect("seed");
    rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
    fluree
        .stage(&handle)
        .sparql_update(&format!(
            "{PREFIXES} DELETE DATA {{ ex:c1 ex:subjectOf ex:s1 }} ; \
             INSERT DATA {{ ex:c9 ex:subjectOf ex:s3 . ex:c2 ex:subjectOf ex:s1 }}"
        ))
        .execute()
        .await
        .expect("pending update");
    assert!(
        !handle.snapshot().await.novelty.is_empty(),
        "novelty pending"
    );
    let (store, _guard) = init_test_tracing();
    let before = store.all_events().len();
    let mut failures = Failures::default();
    let result = sparql(
        &fluree,
        &handle,
        "SELECT ?s ?c WHERE { ?s ex:derivedFrom ex:doc1 . OPTIONAL { ?c ex:subjectOf ?s } }",
    )
    .await;
    let label = "retract c1->s1, assert c2->s1 and c9->s3";
    failures.eq(
        sparql_rows(&result, &["s", "c"]),
        rows(&[&["s1", "c2"], &["s2", "c2"], &["s3", "c9"]]),
        label,
    );
    failures.object_probe_fired(&store, before, label);
    failures.assert_none();
}

/// An UPDATE's DELETE template instantiated from a `?c` the lane binds by its
/// novelty id, in SPARQL and as a JSON-LD transaction.
#[tokio::test(flavor = "current_thread")]
async fn update_template_from_a_novelty_minted_optional_value() {
    let mut failures = Failures::default();
    for jsonld_txn in [false, true] {
        let ledger_id = if jsonld_txn {
            "it/update-template-novelty-jsonld:main"
        } else {
            "it/update-template-novelty:main"
        };
        let (fluree, handle) =
            indexed_then_novelty(ledger_id, &chunks_base(false, false), &novelty_concept()).await;
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let label = if jsonld_txn {
            "JSON-LD transaction"
        } else {
            "SPARQL UPDATE"
        };
        if jsonld_txn {
            let txn = json!({
                "@context": context(),
                "where": [
                    {"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                    ["optional", {"@id": "?c", "ex:subjectOf": "?s"}]
                ],
                "delete": {"@id": "?c", "ex:subjectOf": "?s"}
            });
            fluree
                .stage(&handle)
                .update(&txn)
                .execute()
                .await
                .expect("JSON-LD update");
        } else {
            fluree
                .stage(&handle)
                .sparql_update(&format!(
                    "{PREFIXES} DELETE {{ ?c ex:subjectOf ?s }} \
                     WHERE {{ ?s ex:derivedFrom ex:doc1 . OPTIONAL {{ ?c ex:subjectOf ?s }} }}"
                ))
                .execute()
                .await
                .expect("SPARQL update");
        }
        failures.object_probe(&store, before, update_where_probe(State::Novelty), label);
        let links = sparql(
            &fluree,
            &handle,
            "SELECT ?c ?s WHERE { ?c ex:subjectOf ?s }",
        )
        .await;
        failures.eq(
            sparql_rows(&links, &["c", "s"]),
            rows(&[]),
            &format!("{label}: every link into doc1's chunks deleted, cN's included"),
        );
    }
    failures.assert_none();
}

/// The lane reads its predicate's index rows raw, so a policy that can touch
/// the predicate must keep it out (the filtered path answers), while a policy
/// that cannot touch it leaves the lane on. Same ledger, both outcomes.
#[tokio::test(flavor = "current_thread")]
async fn optional_lane_under_a_non_root_policy() {
    let ledger_id = "it/optional-lane-policy:main";
    let (fluree, _handle) = ledger_in(State::Fresh, ledger_id).await;
    let (store, _guard) = init_test_tracing();
    let deny = |property: &str| {
        json!([{
            "@id": format!("{EX}deny-{property}"),
            "f:action": "f:view",
            "f:required": true,
            "f:onProperty": [{"@id": format!("{EX}{property}")}],
            "f:allow": false
        }])
    };
    let query = format!(
        "{PREFIXES} SELECT ?s ?c WHERE {{ ?s ex:derivedFrom ex:doc1 . \
         OPTIONAL {{ ?c ex:subjectOf ?s }} }}"
    );
    let mut failures = Failures::default();
    for (property, probe, expected) in [
        (
            "alias",
            Probe::Fires,
            rows(&[&["s1", "c1"], &["s2", "c2"], &["s2", "c3"], &["s3", "-"]]),
        ),
        (
            "subjectOf",
            Probe::Declines,
            rows(&[&["s1", "-"], &["s2", "-"], &["s3", "-"]]),
        ),
    ] {
        let view = fluree
            .db_with_policy(
                ledger_id,
                &fluree_db_api::GovernanceOptions {
                    policy: Some(deny(property)),
                    default_allow: Some(true),
                    ..Default::default()
                },
            )
            .await
            .expect("db_with_policy");
        assert!(!view.is_root(), "an enforcing view");
        let label = format!("policy denying ex:{property}");
        let before = store.all_events().len();
        let result = view
            .query(&fluree)
            .sparql(&query)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("{label}: {e}"));
        failures.eq(sparql_rows(&result, &["s", "c"]), expected, &label);
        failures.object_probe(&store, before, probe, &label);
    }
    failures.assert_none();
}

/// A numeric object correlated alongside a bound subject matches by value
/// across numeric datatypes, the rule the shared substitution applies (the
/// inner join's), in every index state. Main matched the decoded value of the
/// novelty lane by value and the encoded one of the indexed lane as a term.
#[tokio::test(flavor = "current_thread")]
async fn numeric_object_correlated_with_a_bound_subject_matches_by_value() {
    let mut failures = Failures::default();
    for novelty in [false, true] {
        let ledger_id = if novelty {
            "it/optional-numeric-object-novelty:main"
        } else {
            "it/optional-numeric-object:main"
        };
        let fluree = FlureeBuilder::memory().build_memory();
        fluree
            .create_ledger(ledger_id)
            .await
            .expect("create ledger");
        let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
        fluree
            .stage(&handle)
            .insert(&json!({
                "@context": context(),
                "@id": "ex:s1",
                "ex:position": 1,
                "ex:rankLong": {"@value": "1", "@type": "http://www.w3.org/2001/XMLSchema#long"},
                "ex:note": "1"
            }))
            .execute()
            .await
            .expect("seed");
        rebuild_and_publish_index(&fluree, ledger_id).await;
        fluree.disconnect_ledger(ledger_id).await;
        let handle = fluree.ledger_cached(ledger_id).await.expect("reload");
        if novelty {
            fluree
                .stage(&handle)
                .insert(&unrelated_write())
                .execute()
                .await
                .expect("pending write");
        }
        let result = sparql(
            &fluree,
            &handle,
            "SELECT ?s ?p WHERE { ?s ex:position ?n . OPTIONAL { ?s ?p ?n } }",
        )
        .await;
        failures.eq(
            sparql_rows(&result, &["s", "p"]),
            rows(&[&["s1", "position"], &["s1", "rankLong"]]),
            &format!("subject bound, numeric object (novelty={novelty})"),
        );
    }
    failures.assert_none();
}

/// A JSON-LD `bind` onto a variable the pattern already bound keeps the row
/// only when the computed value is the same term: the pattern binds the
/// subject encoded, the expression computes it decoded. Each shape here is
/// answered by the inline BIND check; the BIND operator applies the same one.
#[tokio::test(flavor = "current_thread")]
async fn bind_onto_a_bound_variable_compares_terms_across_forms() {
    let cases = [
        (
            "a constant IRI",
            json!({
                "@context": context(),
                "select": ["?s"],
                "where": [
                    {"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                    ["bind", "?s", "(iri \"http://example.org/s2\")"]
                ]
            }),
            rows(&[&["s2"]]),
        ),
        (
            "a constant IRI after an OPTIONAL",
            json!({
                "@context": context(),
                "select": ["?s", "?c"],
                "where": [
                    {"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                    ["optional", {"@id": "?c", "ex:subjectOf": "?s"}],
                    ["bind", "?s", "(iri \"http://example.org/s2\")"]
                ]
            }),
            rows(&[&["s2", "c2"], &["s2", "c3"]]),
        ),
        (
            "an IRI built from a joined value",
            json!({
                "@context": context(),
                "select": ["?s"],
                "where": [
                    {"@id": "?s", "ex:derivedFrom": {"@id": "ex:doc1"}},
                    {"@id": "?s", "ex:position": "?n"},
                    ["bind", "?s", "(iri (concat \"http://example.org/s\" (str ?n)))"]
                ]
            }),
            rows(&[&["s1"], &["s2"], &["s3"]]),
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_in(state, "it/bind-bound-var:main").await;
        for (label, query, expected) in &cases {
            let result = jsonld(&fluree, &handle, query).await;
            failures.eq(
                jsonld_rows(&result),
                expected.clone(),
                &format!("{state:?} / bind onto a bound subject: {label}"),
            );
        }
    }
    failures.assert_none();
}

/// Values a BIND computes for a variable that a pattern of the same group also
/// binds: numbers of two datatypes (the double 7.0 and the integer 7 are
/// different terms), a string, a subject IRI, and keys for an IRI built from a
/// joined value.
fn bind_target_fixture() -> JsonValue {
    let xsd = |t: &str| format!("http://www.w3.org/2001/XMLSchema#{t}");
    json!({
        "@context": context(),
        "@graph": [
            {"@id": "ex:t1", "ex:w": {"@value": "7.0E0", "@type": xsd("double")}},
            {"@id": "ex:t2", "ex:w": {"@value": "3", "@type": xsd("integer")}},
            {"@id": "ex:t3", "ex:w": {"@value": "2.5E0", "@type": xsd("double")}},
            {"@id": "ex:t4", "ex:w": "seven"},
            {"@id": "ex:t5", "ex:w": {"@value": "7", "@type": xsd("integer")}},
            {"@id": "ex:alice", "ex:name": "Alice"},
            {"@id": "ex:a", "ex:num": {"@value": "3", "@type": xsd("integer")}},
            {"@id": "ex:b", "ex:num": {"@value": "4", "@type": xsd("integer")}},
            {"@id": "ex:v3", "ex:val": "three"}
        ]
    })
}

/// A BIND followed by a pattern that binds the same variable joins on the
/// computed term, in every index state. The planner runs the pattern first and
/// applies the BIND as a check that the row's value is the computed one, so the
/// scan's encoded value meets the expression's decoded one there. A number
/// matches its own datatype only: `7` does not match the double `7.0`. The
/// cases that fail without the fix all reach the join's inline check, the
/// JSON-LD BIND after a MINUS included; none reaches the BIND operator's own
/// check, and the BIND after a UNION passes with either check reverted.
#[tokio::test(flavor = "current_thread")]
async fn bind_then_a_pattern_on_its_target_joins_on_the_term() {
    let sparql_cases: [(&str, &str, &[&str], Rows); 7] = [
        (
            "a double",
            "SELECT ?t WHERE { BIND(7.0e0 AS ?x) ?t ex:w ?x }",
            &["t"],
            rows(&[&["t1"]]),
        ),
        (
            "another double",
            "SELECT ?t WHERE { BIND(2.5e0 AS ?x) ?t ex:w ?x }",
            &["t"],
            rows(&[&["t3"]]),
        ),
        (
            "an integer",
            "SELECT ?t WHERE { BIND(3 AS ?x) ?t ex:w ?x }",
            &["t"],
            rows(&[&["t2"]]),
        ),
        (
            "an integer equal in value to a stored double",
            "SELECT ?t WHERE { BIND(7 AS ?x) ?t ex:w ?x }",
            &["t"],
            rows(&[&["t5"]]),
        ),
        (
            "a string",
            "SELECT ?t WHERE { BIND(\"seven\" AS ?x) ?t ex:w ?x }",
            &["t"],
            rows(&[&["t4"]]),
        ),
        (
            "a subject IRI",
            "SELECT ?n WHERE { BIND(ex:alice AS ?s) ?s ex:name ?n }",
            &["n"],
            rows(&[&["Alice"]]),
        ),
        (
            "an IRI built from a joined value",
            "SELECT ?s ?t ?v WHERE { ?s ex:num ?num \
             BIND(IRI(CONCAT(\"http://example.org/v\", STR(?num))) AS ?t) ?t ex:val ?v }",
            &["s", "t", "v"],
            rows(&[&["a", "v3", "three"]]),
        ),
    ];
    let bind_first = |target: &str, expr: &str, pattern: JsonValue, select: &[&str]| {
        json!({
            "@context": context(),
            "select": select,
            "where": [["bind", target, expr], pattern]
        })
    };
    let w = || json!({"@id": "?t", "ex:w": "?x"});
    let jsonld_cases: Vec<(&str, JsonValue, Rows)> = vec![
        (
            "a double",
            bind_first("?x", "7.0e0", w(), &["?t"]),
            rows(&[&["t1"]]),
        ),
        (
            "an integer",
            bind_first("?x", "3", w(), &["?t"]),
            rows(&[&["t2"]]),
        ),
        (
            "a subject IRI",
            bind_first(
                "?s",
                "(iri \"http://example.org/alice\")",
                json!({"@id": "?s", "ex:name": "?n"}),
                &["?n"],
            ),
            rows(&[&["Alice"]]),
        ),
        (
            "an IRI built from a joined value",
            json!({
                "@context": context(),
                "select": ["?s", "?t", "?v"],
                "where": [
                    {"@id": "?s", "ex:num": "?num"},
                    ["bind", "?t", "(iri (concat \"http://example.org/v\" (str ?num)))"],
                    {"@id": "?t", "ex:val": "?v"}
                ]
            }),
            rows(&[&["a", "v3", "three"]]),
        ),
        // JSON-LD also binds an already-bound variable after a UNION or a
        // MINUS. (SPARQL refuses a BIND onto a variable already in scope.)
        // The value is one no stored value of another datatype equals: a
        // plan that looks the computed value up in the pattern instead would
        // match numbers by value, as the join does.
        (
            "a double, BIND after a UNION",
            json!({
                "@context": context(),
                "select": ["?t"],
                "where": [
                    ["union", [{"@id": "?t", "ex:w": "?x"}], [{"@id": "?t", "ex:name": "?x"}]],
                    ["bind", "?x", "2.5e0"]
                ]
            }),
            rows(&[&["t3"]]),
        ),
        (
            "a double, BIND after a MINUS",
            json!({
                "@context": context(),
                "select": ["?t"],
                "where": [
                    {"@id": "?t", "ex:w": "?x"},
                    ["minus", {"@id": "?t", "ex:name": "?n"}],
                    ["bind", "?x", "2.5e0"]
                ]
            }),
            rows(&[&["t3"]]),
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) =
            ledger_with(state, "it/bind-then-pattern:main", &bind_target_fixture()).await;
        for (label, body, vars, want) in &sparql_cases {
            let result = sparql(&fluree, &handle, body).await;
            failures.eq(
                sparql_rows(&result, vars),
                want.clone(),
                &format!("{state:?} / SPARQL BIND then a pattern: {label}"),
            );
        }
        for (label, query, want) in &jsonld_cases {
            let result = jsonld(&fluree, &handle, query).await;
            failures.eq(
                jsonld_rows(&result),
                want.clone(),
                &format!("{state:?} / JSON-LD bind then a pattern: {label}"),
            );
        }
    }
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// A batched lane over a window it can key only in part
// ---------------------------------------------------------------------------

/// Twenty `ex:link`s, r0..r19 to t0..t19, but r5 and r19 link to a string;
/// `ex:cites` from c{i} to t{i} for even i; and one subject with no link.
fn mixed_window_fixture() -> JsonValue {
    let mut graph: Vec<JsonValue> = (0..20)
        .map(|i| {
            let link = if i == 5 || i == 19 {
                json!(format!("lit{i}"))
            } else {
                json!({"@id": format!("ex:t{i}")})
            };
            json!({"@id": format!("ex:r{i}"), "ex:link": link})
        })
        .collect();
    graph.extend(
        (0..20)
            .step_by(2)
            .map(|i| json!({"@id": format!("ex:c{i}"), "ex:cites": {"@id": format!("ex:t{i}")}})),
    );
    graph.push(json!({"@id": "ex:orphan", "ex:kind": "orphan"}));
    json!({"@context": context(), "@graph": graph})
}

/// The bound-object lane answers the rows of a window it can key and leaves
/// the others (a string object, an unbound one) to the per-row lookups, in
/// one pass over the window. It used to decline the whole window at the first
/// such row and was offered it again from the next row, walking the same
/// prefix once per row.
#[tokio::test(flavor = "current_thread")]
async fn object_lane_answers_the_rows_of_a_mixed_window_it_can_key() {
    let body = "SELECT ?s ?c WHERE { { ?s ex:link ?o } UNION { ?s ex:kind ?k } \
                OPTIONAL { ?c ex:cites ?o } }";
    let mut expected: Vec<Vec<String>> = (0..20)
        .map(|i| {
            let c = if i % 2 == 0 && i != 5 && i != 19 {
                format!("c{i}")
            } else {
                "-".to_string()
            };
            vec![format!("r{i}"), c]
        })
        .collect();
    // The unbound object is compatible with every citation.
    expected.extend(
        (0..20)
            .step_by(2)
            .map(|i| vec!["orphan".to_string(), format!("c{i}")]),
    );
    expected.sort();
    let jsonld_query = json!({
        "@context": context(),
        "select": ["?s", "?c"],
        "where": [
            ["union", {"@id": "?s", "ex:link": "?o"}, {"@id": "?s", "ex:kind": "?k"}],
            ["optional", {"@id": "?c", "ex:cites": "?o"}]
        ]
    });
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) =
            ledger_with(state, "it/mixed-window:main", &mixed_window_fixture()).await;
        let (store, _guard) = init_test_tracing();
        let before = store.all_events().len();
        let label = format!("{state:?} / mixed window");
        let result = sparql(&fluree, &handle, body).await;
        failures.eq(sparql_rows(&result, &["s", "c"]), expected.clone(), &label);
        let seen = stamps(&store, before, OBJECT_PROBE_SITE);
        let proceeded = seen.iter().filter(|o| *o == "proceed").count();
        let declined = seen.len() - proceeded;
        if proceeded > 1 || declined > 1 {
            failures.0.push(format!(
                "{label}: the lane must take the window in one pass; stamps: {seen:?}"
            ));
        }
        if state != State::Novelty && proceeded != 1 {
            failures.0.push(format!(
                "{label}: the lane must answer the rows it can key; stamps: {seen:?}"
            ));
        }
        failures.eq(
            jsonld_rows(&jsonld(&fluree, &handle, &jsonld_query).await),
            expected.clone(),
            &format!("{label} (JSON-LD)"),
        );
    }
    failures.assert_none();
}

// ---------------------------------------------------------------------------
// Path endpoints bound to terms no edge touches
// ---------------------------------------------------------------------------

/// Ages (literals), an `ex:next` chain n1 → n2 → n3, and m1 starting it.
fn path_endpoint_fixture() -> JsonValue {
    json!({
        "@context": context(),
        "@graph": [
            {"@id": "ex:a1", "ex:age": 30},
            {"@id": "ex:a2", "ex:age": 31},
            {"@id": "ex:n1", "ex:next": {"@id": "ex:n2"}},
            {"@id": "ex:n2", "ex:next": {"@id": "ex:n3"}},
            {"@id": "ex:m1", "ex:start": {"@id": "ex:n1"}}
        ]
    })
}

/// A path endpoint a row binds to a literal is a term no edge touches: only a
/// zero-length match (`*`, `?`) reaches it, pairing it with itself when a
/// triple holds it (the W3C `values_and_path` rule). It used to be taken for
/// an unbound endpoint, pairing every row with the whole closure of the path.
/// An IRI the ledger has never seen is a node no edge touches (the control).
#[tokio::test(flavor = "current_thread")]
async fn path_endpoint_bound_to_a_term_no_edge_touches() {
    let cases: &[SurfaceCase] = &[
        (
            "a literal start, one or more steps",
            "SELECT ?s ?n WHERE { ?s ex:age ?a . ?a ex:next+ ?n }",
            &["s", "n"],
            &[],
        ),
        (
            "a literal start, zero or more steps",
            "SELECT ?s ?n WHERE { ?s ex:age ?a . ?a ex:next* ?n }",
            &["s", "n"],
            &[&["a1", "30"], &["a2", "31"]],
        ),
        (
            "a literal end, one or more steps",
            "SELECT ?s ?m WHERE { ?s ex:age ?a . ?m ex:next+ ?a }",
            &["s", "m"],
            &[],
        ),
        (
            "a literal end, zero or one step",
            "SELECT ?s ?m WHERE { ?s ex:age ?a . ?m ex:next? ?a }",
            &["s", "m"],
            &[&["a1", "30"], &["a2", "31"]],
        ),
        (
            "an IRI the ledger has never seen, one or more steps (control: a node)",
            "SELECT ?n WHERE { VALUES ?x { <http://elsewhere.example/z> } ?x ex:next+ ?n }",
            &["n"],
            &[],
        ),
        (
            "a literal no triple holds, zero or more steps",
            "SELECT ?n WHERE { VALUES ?x { 99 } ?x ex:next* ?n }",
            &["n"],
            &[],
        ),
        (
            "a node start (control)",
            "SELECT ?m ?n WHERE { ?m ex:start ?x . ?x ex:next+ ?n }",
            &["m", "n"],
            &[&["m1", "n2"], &["m1", "n3"]],
        ),
    ];
    let mut path_context = context();
    path_context["nextPlus"] = json!({"@path": "ex:next+"});
    path_context["nextStar"] = json!({"@path": "ex:next*"});
    let jsonld_cases = [
        (
            "JSON-LD: a literal start, one or more steps",
            json!({
                "@context": path_context.clone(),
                "select": ["?s", "?n"],
                "where": [{"@id": "?s", "ex:age": "?a"}, {"@id": "?a", "nextPlus": "?n"}]
            }),
            rows(&[]),
        ),
        (
            "JSON-LD: a literal start, zero or more steps",
            json!({
                "@context": path_context,
                "select": ["?s", "?n"],
                "where": [{"@id": "?s", "ex:age": "?a"}, {"@id": "?a", "nextStar": "?n"}]
            }),
            rows(&[&["a1", "30"], &["a2", "31"]]),
        ),
    ];
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_with(
            state,
            "it/path-term-endpoints:main",
            &path_endpoint_fixture(),
        )
        .await;
        for (label, body, vars, expected) in cases {
            let label = format!("{state:?} / {label}");
            match db(&handle)
                .await
                .query(&fluree)
                .sparql(&format!("{PREFIXES}{body}"))
                .execute_formatted()
                .await
            {
                Ok(json) => failures.eq(sparql_rows(&json, vars), rows(expected), &label),
                Err(e) => failures.0.push(format!("{label}: query failed: {e}")),
            }
        }
        for (label, query, expected) in &jsonld_cases {
            failures.eq(
                jsonld_rows(&jsonld(&fluree, &handle, query).await),
                expected.clone(),
                &format!("{state:?} / {label}"),
            );
        }
    }
    failures.assert_none();
}

/// Cypher `WITH a AS a` rebinds a variable to itself: the projection computes
/// the node's decoded IRI while the scan bound it encoded, and the BIND that
/// lowers it compares the two as one term. In every index state.
#[tokio::test(flavor = "current_thread")]
async fn cypher_with_a_variable_as_itself() {
    let data = json!({
        "@context": context(),
        "@graph": [
            {"@id": "alice", "@type": "Person", "name": "Alice"},
            {"@id": "bob", "@type": "Person", "name": "Bob"}
        ]
    });
    let mut failures = Failures::default();
    for state in STATES {
        let (fluree, handle) = ledger_with(state, "it/cypher-with-as-itself:main", &data).await;
        let db = db(&handle).await;
        for query in [
            "MATCH (a:Person) WITH a AS a RETURN a.name",
            "MATCH (a:Person) WITH a AS b RETURN b.name",
        ] {
            let result = fluree
                .query_cypher(&db, query)
                .await
                .expect("cypher query")
                .to_jsonld_async(db.as_graph_db_ref())
                .await
                .expect("jsonld");
            failures.eq(
                jsonld_rows(&result),
                rows(&[&["Alice"], &["Bob"]]),
                &format!("{state:?} / {query}"),
            );
        }
    }
    failures.assert_none();
}
