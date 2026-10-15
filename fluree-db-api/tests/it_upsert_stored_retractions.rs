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
use fluree_db_core::{
    range_with_overlay, FlakeValue, OverlayProvider, RangeMatch, RangeOptions, RangeTest,
};
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

/// An identical re-upsert keeps an indexed big integer typed with an XSD
/// subtype. The index stores it with no datatype (`NUM_BIG`), so the upsert
/// retracts the stored `xsd:integer` value and asserts the declared one: two
/// terms under one storage key, which storage resolves at one `t` as the
/// retraction. The operation tells facts apart the way storage does
/// (`FactIdentity`), so the two are one fact and cancel.
#[tokio::test]
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

/// The same for a `dateTime` written past the microsecond, which is read to
/// the microsecond storage keeps, so the re-upserted term is the stored one.
#[tokio::test]
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

// =============================================================================
// Ops on one index key are one stored fact
// =============================================================================

/// An integer past 64 bits: the index keys it by value, with no datatype.
const BIG: &str = "123456789012345678901234567890";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn big(dt: &str) -> Payload {
    jsonld(json!({"@id": "ex:s", "ex:n": {"@value": BIG, "@type": dt}}))
}

/// A delete of every `ex:s <p>` value, witnessed by its WHERE triple.
fn delete_where(p: &str) -> JsonValue {
    json!({
        "@context": ctx(),
        "where": {"@id": "ex:s", p: "?o"},
        "delete": {"@id": "ex:s", p: "?o"}
    })
}

impl Harness {
    async fn update(&mut self, txn: &JsonValue) -> fluree_db_api::CommitReceipt {
        let ledger = self.ledger.take().expect("ledger");
        let result = self.fluree.update(ledger, txn).await.expect("update");
        self.ledger = Some(result.ledger);
        result.receipt
    }

    /// The retractions in the default graph's novelty, as `(predicate local
    /// name, datatype IRI)`.
    fn novelty_retractions(&self) -> Vec<(String, String)> {
        let ledger = self.ledger();
        let mut out = Vec::new();
        ledger.novelty.for_each_overlay_flake(
            0,
            IndexType::Spot,
            None,
            None,
            true,
            ledger.t(),
            &mut |f| {
                if !f.op {
                    let dt = ledger.snapshot.decode_sid(&f.dt).unwrap_or_default();
                    out.push((f.p.name.to_string(), dt));
                }
            },
        );
        out
    }

    /// Upsert `payload`, which `ex:s` no longer holds, and check that it
    /// writes the one value again, before and after the next index build.
    async fn upsert_writes_it_again(&mut self, payload: &Payload, what: &str) {
        assert!(self.facts(None, "s").await.is_empty(), "{what}: removed");
        let t = self.ledger().t();
        let receipt = self.upsert(payload).await;
        assert!(
            receipt.t > t && receipt.assert_count == 1,
            "{what}: the upsert writes the value again: {receipt:?}"
        );
        assert_eq!(self.facts(None, "s").await.len(), 1, "{what}");
        self.reindex().await;
        assert_eq!(
            self.facts(None, "s").await.len(),
            1,
            "{what}: after the next index build"
        );
    }
}

/// After a `DELETE WHERE` removes an indexed big integer, upserting the
/// value writes it again: the delete retracts the fact the index holds.
#[tokio::test]
async fn an_upsert_after_a_delete_where_of_an_indexed_big_integer_writes_it() {
    let mut h = Harness::new("key-delete-where").await;
    h.insert(&big(XSD_INTEGER)).await;
    h.reindex().await;
    h.update(&delete_where("ex:n")).await;
    h.upsert_writes_it_again(&big(XSD_INTEGER), "DELETE WHERE")
        .await;
}

/// The retraction a `DELETE WHERE` stages for an indexed big integer
/// carries the datatype it is stored with, `xsd:integer`, not the
/// `xsd:decimal` the index reports for every big number.
#[tokio::test]
async fn a_delete_where_retracts_an_indexed_big_integer_as_xsd_integer() {
    let mut h = Harness::new("key-delete-where-dt").await;
    h.insert(&big(XSD_INTEGER)).await;
    h.reindex().await;
    h.update(&delete_where("ex:n")).await;
    assert_eq!(
        h.novelty_retractions(),
        vec![("n".to_string(), XSD_INTEGER.to_string())]
    );
}

const XSD_NON_NEGATIVE: &str = "http://www.w3.org/2001/XMLSchema#nonNegativeInteger";

/// A revert retracts a big integer under the XSD subtype it was written
/// with, which the index does not keep: once the insert is indexed, the
/// retraction and the stored fact differ by datatype and share an index
/// key, so storage removes the value. The resolver merges them the same
/// way, and upserting the value afterwards writes it again.
#[tokio::test]
async fn an_upsert_after_reverting_an_indexed_subtyped_big_integer_writes_it() {
    let mut h = Harness::new("key-revert").await;
    // A first commit, so the reverted one is not the genesis commit.
    h.insert(&jsonld(json!({"@id": "ex:unrelated", "ex:u": "u"})))
        .await;
    let Payload::JsonLd(doc) = big(XSD_NON_NEGATIVE) else {
        unreachable!("a JSON-LD payload")
    };
    let ledger = h.ledger.take().expect("ledger");
    let written = h.fluree.insert(ledger, &doc).await.expect("insert");
    let commit = written.receipt.commit_id.clone();
    h.ledger = Some(written.ledger);
    h.reindex().await;
    drop(h.ledger.take());
    let (name, branch) = h.id.split_once(':').expect("ledger id");
    h.fluree
        .revert_commit(
            name,
            branch,
            fluree_db_api::CommitRef::Exact(commit),
            fluree_db_api::ConflictStrategy::TakeSource,
        )
        .await
        .expect("revert");
    h.ledger = Some(h.fluree.ledger(&h.id).await.expect("reload"));
    h.upsert_writes_it_again(&big(XSD_NON_NEGATIVE), "revert")
        .await;
}

/// A value in novelty whose index key keeps less than its term: queries over
/// novelty read it through the key, so the row a `DELETE WHERE` matches is
/// the key's decode, not the stored term. Retracted as decoded, the row
/// shares the term's key and storage drops the value, while novelty's
/// set-semantics dedup, which compares terms, still held the stored term as
/// asserted and dropped its re-assertion: the value read as gone. Such rows
/// now read their slot and retract the stored term.
///
/// `seed` is indexed first, so the predicate (and for a big integer, the
/// value's key) is in the index; `doc` is written in novelty on top, deleted
/// through its WHERE row, and upserted again.
async fn a_lossy_value_survives_its_delete_and_upsert(name: &str, seed: Payload, doc: Payload) {
    a_lossy_value_survives(name, seed, doc, delete_where("ex:v")).await;
}

async fn a_lossy_value_survives(name: &str, seed: Payload, doc: Payload, delete: JsonValue) {
    let mut h = Harness::new(name).await;
    h.insert(&seed).await;
    h.reindex().await;
    h.insert(&doc).await;
    h.update(&delete).await;
    h.upsert_writes_it_again(&doc, name).await;
}

fn typed(id: &str, value: &str, dt: &str) -> Payload {
    jsonld(json!({"@id": id, "ex:v": {"@value": value, "@type": dt}}))
}

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

#[tokio::test]
async fn a_sub_microsecond_datetime_survives_its_delete_and_upsert() {
    let dt = format!("{XSD}dateTime");
    a_lossy_value_survives_its_delete_and_upsert(
        "lossy-datetime",
        typed("ex:other", "2020-01-01T00:00:00Z", &dt),
        typed("ex:s", "2020-01-01T00:00:00.123456789Z", &dt),
    )
    .await;
}

#[tokio::test]
async fn a_sub_microsecond_time_survives_its_delete_and_upsert() {
    let dt = format!("{XSD}time");
    a_lossy_value_survives_its_delete_and_upsert(
        "lossy-time",
        typed("ex:other", "12:00:00", &dt),
        typed("ex:s", "12:00:00.123456789", &dt),
    )
    .await;
}

/// The value is in the index already, under another subject, so a query
/// keys the novelty term by it and reads it back as `xsd:integer`.
#[tokio::test]
async fn a_subtyped_big_integer_survives_its_delete_and_upsert() {
    a_lossy_value_survives_its_delete_and_upsert(
        "lossy-big-integer",
        typed("ex:other", BIG, XSD_INTEGER),
        typed("ex:s", BIG, XSD_NON_NEGATIVE),
    )
    .await;
}

/// An indexed value and a novelty term of its index key, in one slot, are
/// one stored fact, so an upsert of the slot retracts one fact. The slot read
/// folds novelty ops into the index rows by key; the index row alone would
/// be named as a second fact.
#[tokio::test]
async fn an_upsert_over_an_indexed_value_and_a_novelty_term_of_its_key_retracts_one_fact() {
    let dt = format!("{XSD}dateTime");
    let mut h = Harness::new("one-key-two-terms").await;
    h.insert(&typed("ex:s", "2020-01-01T00:00:00.123456Z", &dt))
        .await;
    h.reindex().await;
    h.insert(&typed("ex:s", "2020-01-01T00:00:00.123456789Z", &dt))
        .await;
    let receipt = h.upsert(&typed("ex:s", "2021-06-01T00:00:00Z", &dt)).await;
    assert_eq!(receipt.retract_count, 1);
    let facts = h.facts(None, "s").await;
    assert_eq!(facts.len(), 1, "{facts:?}");
    assert!(facts[0].contains("2021"), "{facts:?}");
}

/// A predicate the index does not have yet, matched by a WHERE that binds
/// the predicate: the subject scan keys novelty under a runtime predicate
/// id, so its row is the key's decode, and the resolver keys the stored
/// term the same way.
#[tokio::test]
async fn a_sub_microsecond_datetime_of_a_new_predicate_survives_its_delete_and_upsert() {
    let dt = format!("{XSD}dateTime");
    a_lossy_value_survives(
        "lossy-datetime-new-predicate",
        jsonld(json!({"@id": "ex:other", "ex:u": "u"})),
        typed("ex:s", "2020-01-01T00:00:00.123456789Z", &dt),
        json!({
            "@context": ctx(),
            "where": {"@id": "ex:s", "?p": "?o"},
            "delete": {"@id": "ex:s", "?p": "?o"}
        }),
    )
    .await;
}

// =============================================================================
// upsert.md: replacing one language
// =============================================================================

/// `upsert.md`'s update that replaces one language of a predicate, read
/// from the doc: the SPARQL block under "Replacing One Language".
fn replace_one_language() -> &'static str {
    const DOC: &str = include_str!("../../docs/transactions/upsert.md");
    let section = DOC
        .split_once("### Replacing One Language")
        .expect("upsert.md has the section")
        .1;
    let block = section
        .split_once("```sparql\n")
        .expect("the section has a SPARQL block")
        .1;
    block.split_once("```").expect("the block closes").0
}

impl Harness {
    async fn sparql_update(&mut self, text: &str) {
        let ledger = self.ledger.take().expect("ledger");
        let parsed = fluree_db_sparql::parse_sparql(text);
        assert!(!parsed.has_errors(), "{text}: {:?}", parsed.diagnostics);
        let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
        let txn = fluree_db_transact::lower_sparql_update_ast(
            &parsed.ast.expect("SPARQL AST"),
            &mut ns,
            fluree_db_transact::TxnOpts::default(),
        )
        .expect("lower");
        let result = self
            .fluree
            .stage_owned(ledger)
            .txn(txn)
            .execute()
            .await
            .expect("update");
        self.ledger = Some(result.ledger);
    }
}

/// The recipe replaces the French name and keeps the English one, whether
/// or not a French name is stored, where an upsert of the French name
/// replaces every language.
#[tokio::test]
async fn upsert_md_replaces_one_language_and_keeps_the_others() {
    let name = |v: &str, tag: &str| json!({"@value": v, "@language": tag});
    for lane in [Lane::Novelty, Lane::Indexed] {
        for (what, seed) in [
            (
                "french-stored",
                json!([name("Alice", "en"), name("Alice", "fr")]),
            ),
            ("no-french", name("Alice", "en")),
        ] {
            let mut h = Harness::new(&format!("one-language-{what}-{lane:?}").to_lowercase()).await;
            h.insert(&jsonld(
                json!({"@id": "ex:alice", "http://schema.org/name": seed}),
            ))
            .await;
            if matches!(lane, Lane::Indexed) {
                h.reindex().await;
            }
            h.sparql_update(replace_one_language()).await;
            assert_eq!(
                h.facts(None, "alice").await,
                vec!["name=Alice@en", "name=Alicia@fr"],
                "{what} [{lane:?}]"
            );
        }
    }
}
