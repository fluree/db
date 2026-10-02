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

    /// Every scan of the triple naming `marker` since `before` decoded its
    /// arena-backed literals (`want`), or none did.
    fn arena_decoding(
        &mut self,
        store: &SpanStore,
        before: usize,
        marker: &str,
        want: bool,
        label: &str,
    ) {
        let flags: Vec<String> = store.all_events()[before..]
            .iter()
            .filter(|e| e.message() == "BinaryScanOperator::open")
            .filter(|e| {
                e.fields
                    .iter()
                    .any(|(k, v)| k.ends_with("pattern") && v.contains(marker))
            })
            .filter_map(|e| e.fields.get("arena_literals_decoded").cloned())
            .collect();
        let want = want.to_string();
        if flags.is_empty() || flags.iter().any(|f| *f != want) {
            self.0.push(format!(
                "{label}: scans of {marker} must have arena_literals_decoded={want}; saw {flags:?}"
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
// With two default graphs no graph view exists, while each graph's scan
// still binds encoded ids. A correlated value must still decode into its
// slot (the ids a ledger shares across graphs decode without one), and a
// slot that cannot take it must be correlated, never left free to match
// every row.
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
            // Arena-backed literals (big numbers) are bound decoded by the
            // member scans: their handles name a value only within one graph,
            // and g1's 1.5 shares a handle with g2's 9.5.
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
        if label.contains("arena-backed") {
            failures.arena_decoding(&store, before, "name: \"amount\"", true, label);
        }
    }

    // One graph: its scan keeps binding arena handles, decoded nowhere.
    let before = store.all_events().len();
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
    failures.arena_decoding(&store, before, "name: \"amount\"", false, label);
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

/// Run each `(label, SPARQL body, projected variables, expected rows)` case,
/// recording a mismatch or a query error.
async fn check_sparql_cases(
    fluree: &Fluree,
    handle: &LedgerHandle,
    cases: &[(&str, String, &[&str], &[&[&str]])],
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[(
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[(
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[(
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[(
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
    let cases: &[(&str, String, &[&str], &[&[&str]])] = &[
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
