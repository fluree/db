//! Upsert retracts the values it replaces exactly as they are stored.
//!
//! The upsert wave used to rebuild each retraction from a query binding,
//! which carries neither a language tag nor a list position. `"a"@en`
//! was retracted as a tagless `"a"`, list entry `"x"` at position 3 as a
//! plain `"x"`, and neither matched anything: the old values stayed, and an
//! identical refresh committed two flakes every time (#1976). The wave now
//! reads each `(graph, subject, predicate)` slot's current facts from
//! storage and retracts them as stored.
//!
//! Every case runs on three lanes and is checked before and after the next
//! index build:
//! - `novelty`: nothing indexed;
//! - `indexed`: the replaced values are all in the persisted index;
//! - `novelty-over-index`: the first seed commit is indexed and the rest
//!   sit in novelty on top of it, so one slot's values span both.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{range_with_overlay, FlakeValue, RangeMatch, RangeOptions, RangeTest};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/ns/";
const G1: &str = "http://example.org/g1";

fn ctx() -> JsonValue {
    json!({"ex": EX})
}

#[derive(Clone)]
enum Payload {
    JsonLd(JsonValue),
    Turtle(String),
}

fn ttl(body: &str) -> Payload {
    Payload::Turtle(format!("@prefix ex: <{EX}> .\n{body}"))
}

fn jsonld(node: JsonValue) -> Payload {
    let mut doc = json!({"@context": ctx()});
    match node {
        JsonValue::Array(nodes) => doc["@graph"] = JsonValue::Array(nodes),
        JsonValue::Object(map) => {
            for (k, v) in map {
                doc[k] = v;
            }
        }
        other => panic!("not a node: {other}"),
    }
    Payload::JsonLd(doc)
}

#[derive(Clone, Copy, Debug)]
enum Lane {
    Novelty,
    Indexed,
    NoveltyOverIndex,
}

const LANES: [Lane; 3] = [Lane::Novelty, Lane::Indexed, Lane::NoveltyOverIndex];

/// A file-backed ledger, so an index can be built and reloaded mid-test.
struct Harness {
    _dir: tempfile::TempDir,
    fluree: Fluree,
    id: String,
    ledger: Option<LedgerState>,
}

impl Harness {
    async fn new(name: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .expect("file fluree");
        let id = format!("it/upsert-stored-{name}:main");
        let ledger = fluree.create_ledger(&id).await.expect("create ledger");
        Self {
            _dir: dir,
            fluree,
            id,
            ledger: Some(ledger),
        }
    }

    fn ledger(&self) -> &LedgerState {
        self.ledger.as_ref().expect("ledger")
    }

    async fn insert(&mut self, payload: &Payload) {
        let ledger = self.ledger.take().expect("ledger");
        let result = match payload {
            Payload::JsonLd(doc) => self.fluree.insert(ledger, doc).await,
            Payload::Turtle(text) => self.fluree.insert_turtle(ledger, text).await,
        };
        self.ledger = Some(result.expect("seed insert").ledger);
    }

    async fn upsert(&mut self, payload: &Payload) -> fluree_db_api::CommitReceipt {
        let ledger = self.ledger.take().expect("ledger");
        let result = match payload {
            Payload::JsonLd(doc) => self.fluree.upsert(ledger, doc).await,
            // The builder lane reads TriG as well as Turtle.
            Payload::Turtle(text) => {
                self.fluree
                    .stage_owned(ledger)
                    .upsert_turtle(text)
                    .execute()
                    .await
            }
        };
        let result = result.expect("upsert");
        self.ledger = Some(result.ledger);
        result.receipt
    }

    /// Build and publish an index at the current head, then reload.
    async fn reindex(&mut self) {
        drop(self.ledger.take());
        support::rebuild_and_publish_index(&self.fluree, &self.id).await;
        let ledger = self.fluree.ledger(&self.id).await.expect("reload");
        assert!(
            ledger.snapshot.range_provider.is_some(),
            "{}: the reload must see the index",
            self.id
        );
        self.ledger = Some(ledger);
    }

    /// The current facts of `subject` in `graph` (None = default graph), as
    /// `pred=value[@lang][#i]`, sorted. Read at the flake level so list
    /// positions and language tags are visible.
    async fn facts(&self, graph: Option<&str>, subject: &str) -> Vec<String> {
        let ledger = self.ledger();
        let g_id = match graph {
            None => 0,
            Some(iri) => match ledger.snapshot.graph_registry.graph_id_for_iri(iri) {
                Some(g) => g,
                None => return Vec::new(),
            },
        };
        let Some(s) = ledger.snapshot.encode_iri(&format!("{EX}{subject}")) else {
            return Vec::new();
        };
        let flakes = range_with_overlay(
            &ledger.snapshot,
            g_id,
            ledger.novelty.as_ref(),
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::new().with_subject(s),
            RangeOptions::new().with_to_t(ledger.t()),
        )
        .await
        .expect("range");
        let mut out: Vec<String> = flakes
            .iter()
            .filter(|f| f.op)
            .map(|f| {
                let o = match &f.o {
                    FlakeValue::String(s) => s.clone(),
                    FlakeValue::Ref(sid) => format!("<{}>", sid.name),
                    other => format!("{other:?}"),
                };
                let lang =
                    f.m.as_ref()
                        .and_then(|m| m.lang.as_deref())
                        .map(|l| format!("@{l}"))
                        .unwrap_or_default();
                let i =
                    f.m.as_ref()
                        .and_then(|m| m.i)
                        .map(|i| format!("#{i}"))
                        .unwrap_or_default();
                format!("{}={o}{lang}{i}", f.p.name)
            })
            .collect();
        out.sort();
        out
    }
}

/// One upsert-replace scenario.
struct Case {
    name: &'static str,
    /// Seed commits, in order. On the `novelty-over-index` lane the first is
    /// indexed and the rest are committed on top.
    seed: Vec<Payload>,
    upsert: Payload,
    graph: Option<&'static str>,
    /// Facts of `ex:s` after the upsert.
    expect: Vec<&'static str>,
    /// `(retract_count, assert_count)` of the upsert's commit; `(0, 0)` means
    /// no commit at all.
    counts: (usize, usize),
}

async fn run(case: &Case) -> Vec<String> {
    let mut failures = Vec::new();
    for lane in LANES {
        let label = format!("{} [{lane:?}]", case.name);
        let slug = format!("{}-{lane:?}", case.name)
            .to_lowercase()
            .replace(' ', "-");
        let mut h = Harness::new(&slug).await;
        match lane {
            Lane::Novelty => {
                for p in &case.seed {
                    h.insert(p).await;
                }
            }
            Lane::Indexed => {
                for p in &case.seed {
                    h.insert(p).await;
                }
                h.reindex().await;
            }
            Lane::NoveltyOverIndex => {
                // Something must be indexed even for a one-commit seed.
                h.insert(&jsonld(json!({"@id": "ex:unrelated", "ex:u": "u"})))
                    .await;
                let (first, rest) = case.seed.split_first().expect("seed");
                if case.seed.len() > 1 {
                    h.insert(first).await;
                    h.reindex().await;
                } else {
                    h.reindex().await;
                    h.insert(first).await;
                }
                for p in rest {
                    h.insert(p).await;
                }
            }
        }
        let t_before = h.ledger().t();
        let receipt = h.upsert(&case.upsert).await;
        let counts = if receipt.flake_count == 0 {
            if receipt.t != t_before {
                failures.push(format!("{label}: an empty upsert advanced t"));
            }
            (0, 0)
        } else {
            (receipt.retract_count, receipt.assert_count)
        };
        if counts != case.counts {
            failures.push(format!(
                "{label}: (retracts, asserts) = {counts:?}, expected {:?}",
                case.counts
            ));
        }
        let expect: Vec<String> = case.expect.iter().map(ToString::to_string).collect();
        let got = h.facts(case.graph, "s").await;
        if got != expect {
            failures.push(format!(
                "{label}: after upsert {got:?}, expected {expect:?}"
            ));
        }
        h.reindex().await;
        let got = h.facts(case.graph, "s").await;
        if got != expect {
            failures.push(format!(
                "{label}: after the next index build {got:?}, expected {expect:?}"
            ));
        }
    }
    failures
}

async fn run_all(cases: &[Case]) {
    let mut failures = Vec::new();
    for case in cases {
        failures.extend(run(case).await);
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

fn lang(v: &str, tag: &str) -> JsonValue {
    json!({"@value": v, "@language": tag})
}

#[tokio::test]
async fn upsert_replaces_every_language_of_a_slot() {
    run_all(&[
        Case {
            name: "jsonld-lang",
            seed: vec![
                jsonld(json!({"@id": "ex:s", "ex:label": lang("a", "en"), "ex:other": "keep"})),
                jsonld(json!({"@id": "ex:s", "ex:label": lang("a", "fr")})),
            ],
            upsert: jsonld(json!({"@id": "ex:s", "ex:label": lang("b", "en")})),
            graph: None,
            expect: vec!["label=b@en", "other=keep"],
            counts: (2, 1),
        },
        Case {
            name: "turtle-lang",
            seed: vec![
                ttl("ex:s ex:label \"a\"@en ; ex:other \"keep\" ."),
                ttl("ex:s ex:label \"a\"@fr ."),
            ],
            upsert: ttl("ex:s ex:label \"b\"@en ."),
            graph: None,
            expect: vec!["label=b@en", "other=keep"],
            counts: (2, 1),
        },
        // A payload in one language replaces the others too: the unit is
        // the whole (graph, subject, predicate) slot.
        Case {
            name: "partial-language",
            seed: vec![jsonld(json!({
                "@id": "ex:s",
                "ex:label": [lang("a", "en"), lang("a", "fr"), lang("a", "de")]
            }))],
            upsert: jsonld(json!({"@id": "ex:s", "ex:label": lang("b", "fr")})),
            graph: None,
            expect: vec!["label=b@fr"],
            counts: (3, 1),
        },
    ])
    .await;
}

#[tokio::test]
async fn upsert_replaces_a_list_as_stored() {
    run_all(&[
        // Turtle keeps list duplicates; the upsert retracts each position.
        Case {
            name: "duplicates",
            seed: vec![ttl("ex:s ex:items ( \"a\" \"a\" \"b\" ) .")],
            upsert: jsonld(json!({"@id": "ex:s", "ex:items": {"@list": ["c"]}})),
            graph: None,
            expect: vec!["items=c#0"],
            counts: (3, 1),
        },
        // Unchanged positions cancel: one retraction, one assertion.
        Case {
            name: "overlap",
            seed: vec![jsonld(
                json!({"@id": "ex:s", "ex:items": {"@list": ["a", "b"]}}),
            )],
            upsert: jsonld(json!({"@id": "ex:s", "ex:items": {"@list": ["a", "c"]}})),
            graph: None,
            expect: vec!["items=a#0", "items=c#1"],
            counts: (1, 1),
        },
        Case {
            name: "plain-to-list",
            seed: vec![jsonld(json!({"@id": "ex:s", "ex:items": ["a", "b"]}))],
            upsert: jsonld(json!({"@id": "ex:s", "ex:items": {"@list": ["a"]}})),
            graph: None,
            expect: vec!["items=a#0"],
            counts: (2, 1),
        },
        Case {
            name: "list-to-plain",
            seed: vec![jsonld(
                json!({"@id": "ex:s", "ex:items": {"@list": ["a", "b"]}}),
            )],
            upsert: jsonld(json!({"@id": "ex:s", "ex:items": "a"})),
            graph: None,
            expect: vec!["items=a"],
            counts: (2, 1),
        },
        Case {
            name: "lang-list",
            seed: vec![jsonld(json!({
                "@id": "ex:s",
                "ex:items": {"@list": [lang("x", "en"), lang("y", "fr")]}
            }))],
            upsert: jsonld(json!({"@id": "ex:s", "ex:items": {"@list": ["z"]}})),
            graph: None,
            expect: vec!["items=z#0"],
            counts: (2, 1),
        },
    ])
    .await;
}

#[tokio::test]
async fn an_identical_refresh_commits_nothing() {
    run_all(&[
        Case {
            name: "refresh-jsonld",
            seed: vec![jsonld(json!({
                "@id": "ex:s",
                "ex:label": [lang("a", "en"), lang("a", "fr")],
                "ex:items": {"@list": ["x", "x", "y"]},
                "ex:note": "same"
            }))],
            upsert: jsonld(json!({
                "@id": "ex:s",
                "ex:label": [lang("a", "en"), lang("a", "fr")],
                "ex:items": {"@list": ["x", "x", "y"]},
                "ex:note": "same"
            })),
            graph: None,
            expect: vec![
                "items=x#0",
                "items=x#1",
                "items=y#2",
                "label=a@en",
                "label=a@fr",
                "note=same",
            ],
            counts: (0, 0),
        },
        Case {
            name: "refresh-turtle",
            seed: vec![ttl("ex:s ex:label \"a\"@en, \"a\"@fr ; ex:note \"same\" .")],
            upsert: ttl("ex:s ex:label \"a\"@en, \"a\"@fr ; ex:note \"same\" ."),
            graph: None,
            expect: vec!["label=a@en", "label=a@fr", "note=same"],
            counts: (0, 0),
        },
    ])
    .await;
}

#[tokio::test]
async fn upsert_replaces_stored_values_in_a_named_graph() {
    run_all(&[
        // Node-level `@graph` inside an envelope: a top-level `@graph`
        // string would read as the txn-meta envelope.
        Case {
            name: "named-jsonld",
            seed: vec![
                jsonld(json!([{
                    "@id": "ex:s", "@graph": G1,
                    "ex:label": lang("a", "en"),
                    "ex:items": {"@list": ["x", "y"]}
                }])),
                jsonld(json!([{"@id": "ex:s", "@graph": G1, "ex:label": lang("a", "fr")}])),
            ],
            upsert: jsonld(json!([{
                "@id": "ex:s", "@graph": G1,
                "ex:label": lang("b", "en"),
                "ex:items": {"@list": ["y"]}
            }])),
            graph: Some(G1),
            expect: vec!["items=y#0", "label=b@en"],
            counts: (4, 2),
        },
        Case {
            name: "named-trig",
            seed: vec![
                ttl(&format!("GRAPH <{G1}> {{ ex:s ex:label \"a\"@en . }}")),
                ttl(&format!("GRAPH <{G1}> {{ ex:s ex:label \"a\"@fr . }}")),
            ],
            upsert: ttl(&format!("GRAPH <{G1}> {{ ex:s ex:label \"b\"@en . }}")),
            graph: Some(G1),
            expect: vec!["label=b@en"],
            counts: (2, 1),
        },
    ])
    .await;
}

/// An identical re-upsert of a document holding blank nodes commits
/// nothing: the payload-scoped skolem id maps each blank node to the node
/// the first upsert stored, and the wave replaces its values like any other
/// subject's. Before, blank subjects were left out of the wave, so their
/// triples were re-asserted on every refresh.
#[tokio::test]
async fn an_identical_refresh_with_blank_nodes_commits_nothing() {
    for (name, payload) in [
        (
            "bnode-turtle",
            ttl("ex:s ex:p [ ex:q \"x\" ; ex:r [ ex:q \"y\" ] ] ."),
        ),
        (
            "bnode-jsonld",
            jsonld(json!({"@id": "ex:s", "ex:p": {"ex:q": "x", "ex:r": {"ex:q": "y"}}})),
        ),
    ] {
        let mut h = Harness::new(name).await;
        let first = h.upsert(&payload).await;
        assert_eq!(first.assert_count, 4, "{name}: first upsert");
        let t = h.ledger().t();
        let again = h.upsert(&payload).await;
        assert_eq!(
            (again.flake_count, h.ledger().t()),
            (0, t),
            "{name}: identical refresh in novelty"
        );
        h.reindex().await;
        let t = h.ledger().t();
        let again = h.upsert(&payload).await;
        assert_eq!(
            (again.flake_count, h.ledger().t()),
            (0, t),
            "{name}: identical refresh over the index"
        );
    }
}

/// Replacing an annotated language-tagged value retracts the edge as
/// stored, which lets the annotation cascade find and retract the edge's
/// `f:reifies*` bundle. With the tag dropped the retraction named no stored
/// edge, so both the old value and its annotation survived.
#[tokio::test]
async fn upsert_over_an_annotated_lang_value_cascades_the_annotation() {
    for indexed in [false, true] {
        let mut h = Harness::new(&format!("annotated-{indexed}")).await;
        h.insert(&jsonld(json!({
            "@id": "ex:s",
            "ex:label": {
                "@value": "a", "@language": "en",
                "@annotation": {"ex:source": "wiki"}
            }
        })))
        .await;
        if indexed {
            h.reindex().await;
        }
        let receipt = h
            .upsert(&jsonld(json!({"@id": "ex:s", "ex:label": lang("b", "en")})))
            .await;
        assert_eq!(
            h.facts(None, "s").await,
            vec!["label=b@en"],
            "indexed={indexed}"
        );
        // The edge, its bundle and the anonymous annotation's body.
        assert!(
            receipt.retract_count > 1,
            "indexed={indexed}: the annotation cascade must retract the bundle \
             (retracts={})",
            receipt.retract_count
        );
        let annotations =
            support::decode_annotations_for_subject(h.ledger(), 0, &format!("{EX}s")).await;
        assert!(
            annotations.is_empty(),
            "indexed={indexed}: the retracted edge keeps no annotation: {annotations:?}"
        );
    }
}

/// U5: a big integer typed with an XSD subtype loses its declared datatype
/// in the index (`NUM_BIG` stores no datatype), so after an index build an
/// identical re-upsert stages a retraction of the decoded `xsd:integer`
/// value that shares one storage key with the assertion of the declared
/// one, and the retraction wins: the value is deleted. The retraction
/// resolver reads what the index can tell it, which does not include the
/// declared subtype; the fix belongs to the index encoding.
#[tokio::test]
#[ignore = "U5: the index drops the declared subtype of NUM_BIG values (filing held, AJ-4)"]
async fn an_identical_reupsert_keeps_an_indexed_big_integer_subtype() {
    let mut h = Harness::new("u5").await;
    let doc = jsonld(json!({
        "@context": {"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
        "@id": "ex:s",
        "ex:n": {"@value": "18446744073709551615", "@type": "xsd:unsignedLong"}
    }));
    h.upsert(&doc).await;
    h.reindex().await;
    h.upsert(&doc).await;
    assert_eq!(h.facts(None, "s").await.len(), 1, "the value survives");
}

/// The same class for temporals: storage keeps a `dateTime` to the
/// microsecond, so a value written with more digits decodes differently from
/// the term that is re-upserted, and the retraction of the decode and the
/// assertion of the written term share one storage key.
#[tokio::test]
#[ignore = "U5: storage keeps temporals to the microsecond (filing held, AJ-4)"]
async fn an_identical_reupsert_keeps_a_sub_microsecond_datetime() {
    let mut h = Harness::new("u5-datetime").await;
    let doc = jsonld(json!({
        "@context": {"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
        "@id": "ex:s",
        "ex:d": {"@value": "2020-01-01T00:00:00.123456789Z", "@type": "xsd:dateTime"}
    }));
    h.upsert(&doc).await;
    h.reindex().await;
    h.upsert(&doc).await;
    assert_eq!(h.facts(None, "s").await.len(), 1, "the value survives");
}
