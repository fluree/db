//! The witnessed-DELETE lane must actually fire, and stand down when the
//! ledger can hold a list position.
//!
//! A DELETE whose template a WHERE triple witnesses (`DELETE { ?s ex:p ?o }
//! WHERE { ?s ex:p ?o }`) retracts its rows as decoded, with no stored-fact
//! read, when neither the index nor novelty holds an `@list` position; on a
//! list-bearing ledger the rows read their slots for list positions. The
//! answers agree either way, so only the `delete_witnessed` routing stamp
//! can tell which lane ran. Both directions are asserted on the same ledger:
//! `MustFire` while it is list-free, `MustNotFire` after one `@list` lands.
//! A WHERE scoped to one named graph takes the lane too, and its rows are
//! decoded and retracted in that graph.
//!
//! Its own `[[test]]` binary, like `it_upsert_skip_fires`: tracing's
//! callsite-interest cache is process-global, so a sibling test in a bundled
//! binary can leave the stamp's callsite disabled.

mod support;

use crate::support::span_capture;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState, TransactResult};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{range_with_overlay, FlakeValue, RangeMatch, RangeOptions, RangeTest};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/ns/"})
}

fn witnessed_delete(subject: &str) -> JsonValue {
    json!({
        "@context": ctx(),
        "where": {"@id": subject, "ex:tag": "?o"},
        "delete": {"@id": subject, "ex:tag": "?o"}
    })
}

#[tokio::test(flavor = "current_thread")]
async fn delete_witnessed_lane_fires_only_on_a_list_free_ledger() {
    let (store, _guard) = span_capture::init_test_tracing();
    let site = fluree_db_transact::DELETE_WITNESSED_SITE;
    let outcomes = |from: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("it/delete-witness-routing:main")
        .await
        .expect("create");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "ex:tag": "x"},
                    {"@id": "ex:b", "ex:tag": "y"},
                    {"@id": "ex:c", "ex:tag": "z"}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    assert!(!ledger.novelty.has_list_meta);

    // MustFire: list-free.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(ledger, &witnessed_delete("ex:a"))
        .await
        .expect("witnessed delete");
    assert_eq!(r.receipt.retract_count, 1);
    assert_eq!(
        outcomes(before),
        ["proceed"],
        "a witnessed delete on a list-free ledger must take the zero-read lane"
    );

    // A DELETE DATA-shaped delete has no witness and stamps nothing.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(
            r.ledger,
            &json!({"@context": ctx(), "delete": {"@id": "ex:b", "ex:tag": "y"}}),
        )
        .await
        .expect("constant delete");
    assert_eq!(r.receipt.retract_count, 1);
    assert!(outcomes(before).is_empty(), "no witness, no stamp");

    // One @list on the same ledger turns the lane off.
    let ledger = fluree
        .insert(
            r.ledger,
            &json!({"@context": ctx(), "@id": "ex:l", "ex:items": {"@list": ["a", "b"]}}),
        )
        .await
        .expect("list")
        .ledger;
    assert!(ledger.novelty.has_list_meta);

    // MustNotFire: list-bearing.
    let before = store.find_events("fast-path outcome").len();
    let r = fluree
        .update(ledger, &witnessed_delete("ex:c"))
        .await
        .expect("witnessed delete, list-bearing");
    assert_eq!(r.receipt.retract_count, 1, "the delete still retracts");
    assert_eq!(
        outcomes(before),
        ["fallback:gate_declined"],
        "a list-bearing ledger must read list positions"
    );
}

/// An update on either surface.
enum Update {
    JsonLd(JsonValue),
    Sparql(&'static str),
}

async fn apply(fluree: &Fluree, ledger: LedgerState, update: Update) -> TransactResult {
    match update {
        Update::JsonLd(txn) => fluree.update(ledger, &txn).await.expect("JSON-LD update"),
        Update::Sparql(body) => {
            let text = format!("PREFIX ex: <http://example.org/ns/>\n{body}");
            let parsed = fluree_db_sparql::parse_sparql(&text);
            assert!(!parsed.has_errors(), "{text}: {:?}", parsed.diagnostics);
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &parsed.ast.expect("ast"),
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower");
            fluree
                .stage_owned(ledger)
                .txn(txn)
                .execute()
                .await
                .expect("SPARQL update")
        }
    }
}

/// A value bound by the sole triple of an OPTIONAL, or of one UNION branch,
/// is the decode of that triple's fact, so its row takes the witnessed lane:
/// the JSON-LD and SPARQL shapes, and Cypher `SET` and `DETACH DELETE`,
/// which read the old value through an OPTIONAL. A UNION whose other branch
/// binds the object from another predicate stays on the matched lane.
#[tokio::test(flavor = "current_thread")]
async fn optional_and_union_rows_take_the_witnessed_lane() {
    let (store, _guard) = span_capture::init_test_tracing();
    let site = fluree_db_transact::DELETE_WITNESSED_SITE;
    let outcomes = |from: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("it/delete-witness-optional:main")
        .await
        .expect("create");
    let mut ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "@type": "ex:Node", "ex:tag": "x"},
                    {"@id": "ex:b", "@type": "ex:Node", "ex:tag": "y"},
                    {"@id": "ex:c", "@type": "ex:Node", "ex:tag": "z"},
                    {"@id": "ex:d", "@type": "ex:Node", "ex:tag": "w", "ex:other": "v"},
                    {"@id": "ex:e", "@type": "ex:Node", "ex:tag": "u"},
                    {"@id": "ex:f", "@type": "ex:Node", "ex:tag": "t"},
                    {"@id": "ex:g", "@type": "ex:Node", "ex:tag": "s"},
                    {"@id": "ex:h", "@type": "ex:Node", "ex:tag": "r", "ex:other": "q"}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;

    // (case, update, witnessed). Each retracts one fact either way; where
    // the other UNION branch binds the object from another predicate, the
    // row goes through the resolver.
    let cases = [
        (
            "JSON-LD OPTIONAL",
            Update::JsonLd(json!({
                "@context": ctx(),
                "where": [{"@id": "ex:a", "@type": "ex:Node"},
                          ["optional", {"@id": "ex:a", "ex:tag": "?o"}]],
                "delete": {"@id": "ex:a", "ex:tag": "?o"}
            })),
            true,
        ),
        (
            "JSON-LD UNION",
            Update::JsonLd(json!({
                "@context": ctx(),
                "where": [["union", {"@id": "ex:b", "ex:tag": "?o"}, {"@id": "ex:b", "@type": "?t"}]],
                "delete": {"@id": "ex:b", "ex:tag": "?o"}
            })),
            true,
        ),
        (
            "SPARQL OPTIONAL",
            Update::Sparql(
                "DELETE { ex:f ex:tag ?o } WHERE { ex:f a ex:Node OPTIONAL { ex:f ex:tag ?o } }",
            ),
            true,
        ),
        (
            "SPARQL UNION",
            Update::Sparql(
                "DELETE { ex:g ex:tag ?o } WHERE { { ex:g ex:tag ?o } UNION { ex:g a ?t } }",
            ),
            true,
        ),
        (
            "JSON-LD UNION binding the object twice",
            Update::JsonLd(json!({
                "@context": ctx(),
                "where": [["union", {"@id": "ex:d", "ex:tag": "?o"}, {"@id": "ex:d", "ex:other": "?o"}]],
                "delete": {"@id": "ex:d", "ex:tag": "?o"}
            })),
            false,
        ),
        (
            "SPARQL UNION binding the object twice",
            Update::Sparql(
                "DELETE { ex:h ex:tag ?o } WHERE { { ex:h ex:tag ?o } UNION { ex:h ex:other ?o } }",
            ),
            false,
        ),
    ];
    let mut failures = Vec::new();
    for (what, update, witnessed) in cases {
        let before = store.find_events("fast-path outcome").len();
        let r = apply(&fluree, ledger, update).await;
        let stamps = outcomes(before);
        let expected: &[&str] = if witnessed { &["proceed"] } else { &[] };
        if r.receipt.retract_count != 1 || stamps != expected {
            failures.push(format!(
                "{what}: {} retracted, stamps {stamps:?}",
                r.receipt.retract_count
            ));
        }
        ledger = r.ledger;
    }
    assert!(failures.is_empty(), "{failures:#?}");

    for (what, stmt) in [
        (
            "Cypher SET",
            "MATCH (n:`http://example.org/ns/Node`) WHERE n.`http://example.org/ns/tag` = 'z' \
             SET n.`http://example.org/ns/tag` = 'zz'",
        ),
        (
            "Cypher DETACH DELETE",
            "MATCH (n:`http://example.org/ns/Node`) WHERE n.`http://example.org/ns/tag` = 'u' \
             DETACH DELETE n",
        ),
    ] {
        let before = store.find_events("fast-path outcome").len();
        let r = fluree.transact_cypher(ledger, stmt).await.expect(what);
        assert!(r.receipt.retract_count >= 1, "{what}");
        assert!(
            outcomes(before).iter().any(|o| o == "proceed"),
            "{what}: {:?}",
            outcomes(before)
        );
        ledger = r.ledger;
    }
}

/// An integer too large for 64 bits.
const BIG: &str = "123456789012345678901234567890";
/// Another one, in the default graph under the same predicate.
const OTHER_BIG: &str = "987654321098765432109876543210";
const G: &str = "http://example.org/ns/g";

/// The current `ex:v` values of `ex:{subject}` in `graph` (None: the default
/// graph), as `value^^datatype`, read at the flake level.
async fn v_values(ledger: &LedgerState, graph: Option<&str>, subject: &str) -> Vec<String> {
    let g_id = match graph {
        None => 0,
        Some(iri) => ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .expect("graph registered"),
    };
    let s = ledger
        .snapshot
        .encode_iri(&format!("http://example.org/ns/{subject}"))
        .expect("subject");
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
    flakes
        .iter()
        .filter(|f| f.op && &*f.p.name == "v")
        .map(|f| {
            let o = match &f.o {
                FlakeValue::BigInt(n) => n.to_string(),
                other => format!("{other:?}"),
            };
            format!("{o}^^{}", f.dt.name)
        })
        .collect()
}

/// A WHERE scoped to one named graph (`WITH`, `USING`, JSON-LD `graph` and
/// `from`) takes the witnessed lane, decodes its rows through that graph and
/// retracts them there. Both graphs hold an integer too large for 64 bits
/// under `ex:v`, each the first of its graph's values, so a row decoded
/// through the default graph would name the other number.
///
/// The first delete reads `<g>`'s value from the index alone, so the WHERE
/// decodes it. Then `ex:b` gets the same value in `<g>` as a novelty term
/// with an XSD subtype, which the WHERE reads through the index's key as
/// `xsd:integer`: the second delete's row reads the slot and must find the
/// stored term by its key in `<g>`. Retracted as decoded instead, the value
/// reads as gone and the upsert that writes it again is dropped.
#[tokio::test(flavor = "current_thread")]
async fn a_where_scoped_to_one_graph_deletes_and_rewrites_its_big_integer() {
    let (store, _guard) = span_capture::init_test_tracing();
    let site = fluree_db_transact::DELETE_WITNESSED_SITE;
    let outcomes = |from: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };
    let prefixes = "@prefix ex: <http://example.org/ns/> .\n\
                    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
    let seed =
        format!("{prefixes}ex:a ex:v {OTHER_BIG} .\nGRAPH <{G}> {{ ex:other ex:v {BIG} . }}\n");
    let subtyped =
        format!("{prefixes}GRAPH <{G}> {{ ex:b ex:v \"{BIG}\"^^xsd:nonNegativeInteger . }}\n");
    let default_graph = vec![format!("{OTHER_BIG}^^integer")];

    let cases = [
        (
            "SPARQL WITH",
            "WITH <http://example.org/ns/g> DELETE { ?s ex:v ?o } WHERE { ?s ex:v ?o }",
            None,
        ),
        (
            "SPARQL USING",
            "DELETE { GRAPH <http://example.org/ns/g> { ?s ex:v ?o } } \
             USING <http://example.org/ns/g> WHERE { ?s ex:v ?o }",
            None,
        ),
        (
            "JSON-LD graph",
            "",
            Some(json!({
                "@context": ctx(),
                "graph": G,
                "where": {"@id": "?s", "ex:v": "?o"},
                "delete": {"@id": "?s", "ex:v": "?o"}
            })),
        ),
        (
            "JSON-LD from",
            "",
            Some(json!({
                "@context": ctx(),
                "from": G,
                "where": {"@id": "?s", "ex:v": "?o"},
                "delete": {"@id": "?s", "@graph": G, "ex:v": "?o"}
            })),
        ),
    ];
    let mut failures = Vec::new();
    for (i, (what, sparql, jsonld)) in cases.into_iter().enumerate() {
        let update = || match &jsonld {
            Some(txn) => Update::JsonLd(txn.clone()),
            None => Update::Sparql(sparql),
        };
        let fluree = FlureeBuilder::memory().build_memory();
        let id = format!("it/delete-witness-scoped-{i}:main");
        let ledger = fluree.create_ledger(&id).await.expect("create");
        fluree
            .stage_owned(ledger)
            .upsert_turtle(&seed)
            .execute()
            .await
            .expect("seed");
        support::rebuild_and_publish_index(&fluree, &id).await;
        let ledger = fluree.ledger(&id).await.expect("reload");
        assert!(ledger.snapshot.range_provider.is_some(), "{what}: indexed");

        // The indexed value, decoded by the WHERE.
        let before = store.find_events("fast-path outcome").len();
        let r = apply(&fluree, ledger, update()).await;
        let stamps = outcomes(before);
        let left = v_values(&r.ledger, Some(G), "other").await;
        if r.receipt.retract_count != 1 || stamps != ["proceed"] || !left.is_empty() {
            failures.push(format!(
                "{what}, indexed: retracted {}, stamps {stamps:?}, <g> left {left:?}",
                r.receipt.retract_count
            ));
        }
        let kept = v_values(&r.ledger, None, "a").await;
        if kept != default_graph {
            failures.push(format!("{what}: the default graph holds {kept:?}"));
        }

        // The subtyped novelty term, read through the index's key.
        let ledger = fluree
            .stage_owned(r.ledger)
            .upsert_turtle(&subtyped)
            .execute()
            .await
            .expect("subtyped value")
            .ledger;
        assert!(ledger.novelty.has_lossy_index_terms, "{what}");
        assert_eq!(v_values(&ledger, Some(G), "b").await.len(), 1, "{what}");
        let before = store.find_events("fast-path outcome").len();
        let r = apply(&fluree, ledger, update()).await;
        let stamps = outcomes(before);
        let left = v_values(&r.ledger, Some(G), "b").await;
        if r.receipt.retract_count != 1 || stamps != ["proceed"] || !left.is_empty() {
            failures.push(format!(
                "{what}, novelty: retracted {}, stamps {stamps:?}, <g> left {left:?}",
                r.receipt.retract_count
            ));
        }
        let up = fluree
            .stage_owned(r.ledger)
            .upsert_turtle(&subtyped)
            .execute()
            .await
            .expect("upsert");
        let rewritten = v_values(&up.ledger, Some(G), "b").await;
        let asserted = up.receipt.assert_count;
        drop(up);
        support::rebuild_and_publish_index(&fluree, &id).await;
        let reindexed = fluree.ledger(&id).await.expect("reload");
        let reindexed = v_values(&reindexed, Some(G), "b").await;
        if asserted != 1 || rewritten.len() != 1 || reindexed.len() != 1 {
            failures.push(format!(
                "{what}: the upsert asserted {asserted} and left {rewritten:?}, \
                 after the index build {reindexed:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A DELETE naming a few plain values of a slot the index's statistics call
/// wide reads just those values (`delete_point_read` proceeds). More values
/// than the slot's width pays for, a tagged value, or a slot of a predicate
/// with one value per subject read the slot (it falls back). All on one
/// indexed ledger, each delete retracting what it names.
#[tokio::test(flavor = "current_thread")]
async fn a_delete_reads_by_value_only_a_wide_slot_named_a_few_times() {
    let (store, _guard) = span_capture::init_test_tracing();
    let site = fluree_db_transact::DELETE_POINT_READ_SITE;
    let outcomes = |from: usize| -> Vec<String> {
        store.find_events("fast-path outcome")[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/delete-point-routing:main";
    let mut tags: Vec<JsonValue> = (0..40).map(|i| json!(format!("t{i}"))).collect();
    tags.push(json!({"@value": "t2", "@language": "en"}));
    let mut graph = vec![json!({"@id": "ex:s", "ex:tag": tags})];
    graph.extend((0..40).map(|i| json!({"@id": format!("ex:i{i}"), "ex:one": format!("v{i}")})));
    let ledger = fluree.create_ledger(id).await.expect("create");
    fluree
        .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed");
    support::rebuild_and_publish_index(&fluree, id).await;
    let mut ledger = fluree.ledger(id).await.expect("reload");

    let cases = [
        (
            "one value of the wide slot",
            json!({"@id": "ex:s", "ex:tag": "t3"}),
            1,
            "proceed",
        ),
        (
            "five values of the wide slot",
            json!({"@id": "ex:s", "ex:tag": ["t4", "t5", "t6", "t7", "t8"]}),
            5,
            "fallback:gate_declined",
        ),
        (
            "a tagged value",
            json!({"@id": "ex:s", "ex:tag": {"@value": "t2", "@language": "en"}}),
            1,
            "fallback:gate_declined",
        ),
        (
            "the value of a narrow slot",
            json!({"@id": "ex:i0", "ex:one": "v0"}),
            1,
            "fallback:gate_declined",
        ),
        (
            "another value of the wide slot",
            json!({"@id": "ex:s", "ex:tag": "t9"}),
            1,
            "proceed",
        ),
    ];
    let mut failures = Vec::new();
    for (what, delete, retracted, outcome) in cases {
        let before = store.find_events("fast-path outcome").len();
        let r = fluree
            .update(ledger, &json!({"@context": ctx(), "delete": delete}))
            .await
            .expect("delete");
        let stamps = outcomes(before);
        if r.receipt.retract_count != retracted || stamps != [outcome] {
            failures.push(format!(
                "{what}: retracted {}, stamps {stamps:?}",
                r.receipt.retract_count
            ));
        }
        ledger = r.ledger;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
