//! Reasoned heads: registered reasoning programs persisted as merged
//! indexes, served to matching queries at their basis commit.

use crate::support::normalize_rows;
use fluree_db_api::reasoned::RefreshOutcome;
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::reasoned_head::GenerationKind;
use fluree_db_core::ReasonedHeadTally;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl": "http://www.w3.org/2002/07/owl#"
    })
}

/// A tracked query's rows, sorted, and the head that served it, if any.
async fn rows(
    fluree: &Fluree,
    ledger_id: &str,
    query: &JsonValue,
) -> (Vec<JsonValue>, Option<ReasonedHeadTally>) {
    let resp = fluree
        .graph(ledger_id)
        .query()
        .jsonld(query)
        .track_all()
        .execute_tracked()
        .await
        .unwrap_or_else(|e| panic!("query failed: {e:?}"));
    (
        normalize_rows(&resp.result),
        resp.reasoning.and_then(|r| r.head),
    )
}

fn people_query() -> JsonValue {
    json!({
        "@context": ctx(),
        "select": "?p",
        "where": {"@id": "?p", "@type": "ex:Person"},
        "reasoning": "owl2rl"
    })
}

fn published(outcome: RefreshOutcome) -> (u64, GenerationKind) {
    match outcome {
        RefreshOutcome::Published {
            generation, kind, ..
        } => (generation, kind),
        other => panic!("expected a published generation, got {other:?}"),
    }
}

#[tokio::test]
async fn a_head_answers_owl2rl_queries_at_its_commit_and_follows_the_branch() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/owl:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                    {"@id": "ex:alice", "@type": "ex:Student"},
                    {"@id": "ex:bob", "@type": "ex:Person"}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let (expected, head) = rows(&fluree, id, &people_query()).await;
    assert_eq!(head, None, "nothing is registered yet");
    assert_eq!(expected, vec![json!("ex:alice"), json!("ex:bob")]);

    let head_id = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let again = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    assert_eq!(
        head_id, again,
        "registering an active program is idempotent"
    );

    let (generation, kind) = published(fluree.refresh_reasoned_head(id, &head_id).await.unwrap());
    assert_eq!((generation, kind), (1, GenerationKind::Rebuild));

    let (served, head) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served, expected);
    let head = head.expect("the head answers at its basis");
    assert_eq!(
        (head.head_id.as_str(), head.generation),
        (head_id.as_str(), 1)
    );

    // Without reasoning nothing is derived, and the head is not consulted.
    let mut plain = people_query();
    plain.as_object_mut().unwrap().remove("reasoning");
    let (asserted, head) = rows(&fluree, id, &plain).await;
    assert_eq!(head, None);
    assert_eq!(asserted, vec![json!("ex:bob")]);

    // A new commit: the generation is no longer at the query's snapshot,
    // so the query reasons at query time until the next refresh.
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:carol", "@type": "ex:Student"}),
        )
        .await
        .unwrap()
        .ledger;
    let (fallback, head) = rows(&fluree, id, &people_query()).await;
    assert_eq!(head, None, "a stale generation is never served");
    assert_eq!(
        fallback,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );

    let (generation, kind) = published(fluree.refresh_reasoned_head(id, &head_id).await.unwrap());
    assert_eq!((generation, kind), (2, GenerationKind::Logical));
    let (served, head) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served, fallback);
    assert_eq!(head.map(|h| h.generation), Some(2));

    // Retracting the support removes the derived fact.
    let _ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "delete": {"@id": "ex:alice", "@type": "ex:Student"},
                "where": {"@id": "ex:alice", "@type": "ex:Student"}
            }),
        )
        .await
        .unwrap()
        .ledger;
    let outcome = fluree.refresh_reasoned_head(id, &head_id).await.unwrap();
    match &outcome {
        RefreshOutcome::Published {
            generation, stats, ..
        } => {
            assert_eq!(*generation, 3);
            assert_eq!(stats.removed, 1, "{stats:?}");
        }
        other => panic!("expected a published generation, got {other:?}"),
    }
    let (served, head) = rows(&fluree, id, &people_query()).await;
    assert_eq!(head.map(|h| h.generation), Some(3));
    assert_eq!(served, vec![json!("ex:bob"), json!("ex:carol")]);

    assert_eq!(
        fluree.refresh_reasoned_head(id, &head_id).await.unwrap(),
        RefreshOutcome::Current { generation: 3 }
    );
}

async fn ledger_with(fluree: &Fluree, id: &str, data: JsonValue) -> fluree_db_api::LedgerState {
    let ledger = fluree.create_ledger(id).await.unwrap();
    fluree.insert(ledger, &data).await.unwrap().ledger
}

async fn retract(
    fluree: &Fluree,
    ledger: fluree_db_api::LedgerState,
    fact: JsonValue,
) -> fluree_db_api::LedgerState {
    fluree
        .update(
            ledger,
            &json!({"@context": ctx(), "delete": fact.clone(), "where": fact}),
        )
        .await
        .unwrap()
        .ledger
}

/// A fact both asserted and derived stays when it loses either support: its
/// assertion (it is still derived, so it moves into the derived-only set) or
/// its derivation (it is still asserted).
#[tokio::test]
async fn an_asserted_and_derived_fact_survives_losing_either_support() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/overlap:main";
    let ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": ["ex:Student", "ex:Person"]}
            ]
        }),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    match fluree.refresh_reasoned_head(id, &head).await.unwrap() {
        RefreshOutcome::Published { stats, .. } => {
            assert_eq!(
                stats.derived_only, 0,
                "alice is asserted a Person: {stats:?}"
            )
        }
        other => panic!("{other:?}"),
    }

    // Lose the assertion: still derived.
    let ledger = retract(
        &fluree,
        ledger,
        json!({"@id": "ex:alice", "@type": "ex:Person"}),
    )
    .await;
    match fluree.refresh_reasoned_head(id, &head).await.unwrap() {
        RefreshOutcome::Published { stats, kind, .. } => {
            assert_eq!(kind, GenerationKind::Logical);
            assert_eq!(stats.derived_only, 1, "{stats:?}");
            assert_eq!(stats.removed, 0, "{stats:?}");
        }
        other => panic!("{other:?}"),
    }
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some());
    assert_eq!(people, vec![json!("ex:alice")]);

    // In one window, assert it again and lose the derivation: still
    // asserted, so the fold must not retract it.
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "@type": "ex:Person"}),
        )
        .await
        .unwrap()
        .ledger;
    let _ledger = retract(
        &fluree,
        ledger,
        json!({"@id": "ex:alice", "@type": "ex:Student"}),
    )
    .await;
    match fluree.refresh_reasoned_head(id, &head).await.unwrap() {
        RefreshOutcome::Published { stats, .. } => {
            assert_eq!(stats.derived_only, 0, "{stats:?}");
            assert_eq!(
                stats.removed, 0,
                "an asserted fact is never retracted: {stats:?}"
            );
        }
        other => panic!("{other:?}"),
    }
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some());
    assert_eq!(people, vec![json!("ex:alice")]);
}

/// A transitive chain whose support is retracted loses the facts derived
/// through it: each generation recomputes the closure from the inputs rather
/// than from the previous closure.
#[tokio::test]
async fn a_chain_loses_what_its_retracted_link_supported() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/chain:main";
    let ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:partOf", "@type": "owl:TransitiveProperty"},
                {"@id": "ex:a", "ex:partOf": {"@id": "ex:b"}},
                {"@id": "ex:b", "ex:partOf": {"@id": "ex:c"}},
                {"@id": "ex:c", "ex:partOf": {"@id": "ex:d"}}
            ]
        }),
    )
    .await;
    let q = json!({
        "@context": ctx(),
        "select": "?o",
        "where": {"@id": "ex:a", "ex:partOf": "?o"},
        "reasoning": "owl2rl"
    });
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoned_head(id, &head).await.unwrap();
    let (reached, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(reached, vec![json!("ex:b"), json!("ex:c"), json!("ex:d")]);

    let _ledger = retract(
        &fluree,
        ledger,
        json!({"@id": "ex:b", "ex:partOf": {"@id": "ex:c"}}),
    )
    .await;
    let (expected, served) = rows(&fluree, id, &q).await;
    assert!(served.is_none());
    assert_eq!(expected, vec![json!("ex:b")]);
    fluree.refresh_reasoned_head(id, &head).await.unwrap();
    let (reached, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(reached, expected);
}

/// Datalog programs: query-time rules in the program are part of what a
/// query must carry to be served; a different rule set is not served.
#[tokio::test]
async fn a_datalog_program_is_served_only_to_queries_with_its_rules() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/datalog:main";
    let ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:a", "ex:parent": {"@id": "ex:b"}},
                {"@id": "ex:b", "ex:parent": {"@id": "ex:c"}}
            ]
        }),
    )
    .await;
    let rules = json!([
        {"@id": "ex:base", "@context": ctx(),
         "where": {"@id": "?x", "ex:parent": "?y"},
         "insert": {"@id": "?x", "ex:ancestor": {"@id": "?y"}}},
        {"@id": "ex:step", "@context": ctx(),
         "where": [{"@id": "?x", "ex:parent": "?y"}, {"@id": "?y", "ex:ancestor": "?z"}],
         "insert": {"@id": "?x", "ex:ancestor": {"@id": "?z"}}}
    ]);
    let program = json!({"reasoning": "datalog", "rules": rules});
    let q = json!({
        "@context": ctx(),
        "select": "?z",
        "where": {"@id": "ex:a", "ex:ancestor": "?z"},
        "reasoning": "datalog",
        "rules": rules
    });
    let (expected, _) = rows(&fluree, id, &q).await;
    assert_eq!(expected, vec![json!("ex:b"), json!("ex:c")]);

    let head = fluree.register_reasoned_head(id, &program).await.unwrap();
    published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    let (served_rows, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(served_rows, expected);

    // One rule only: a different program, reasoned at query time.
    let mut partial = q.clone();
    partial["rules"] = json!([rules[0].clone()]);
    let (partial_rows, served) = rows(&fluree, id, &partial).await;
    assert!(served.is_none());
    assert_eq!(partial_rows, vec![json!("ex:b")]);

    // The chain grows; the next generation folds it in.
    let _ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:c", "ex:parent": {"@id": "ex:d"}}),
        )
        .await
        .unwrap()
        .ledger;
    let (_, kind) = published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    assert_eq!(kind, GenerationKind::Logical);
    let (served_rows, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(
        served_rows,
        vec![json!("ex:b"), json!("ex:c"), json!("ex:d")]
    );
}

/// A stored rule that derives into a graph the ledger does not have fails
/// the run, and nothing is published.
#[tokio::test]
async fn a_rule_into_a_missing_graph_publishes_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/missing-graph:main";
    let _ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
            "@graph": [
                {"@id": "ex:graphHead", "f:rule": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               CONSTRUCT { GRAPH ex:inferred { ?x ex:derived ?y } } \
                               WHERE { ?x ex:a ?y }"
                }},
                {"@id": "ex:thing", "ex:a": 1}
            ]
        }),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "datalog"}))
        .await
        .unwrap();
    match fluree.refresh_reasoned_head(id, &head).await.unwrap() {
        RefreshOutcome::Failed { reason } => {
            assert!(reason.contains("inferred"), "{reason}")
        }
        other => panic!("expected a failed run, got {other:?}"),
    }
    let doc = fluree.reasoned_heads(id).await.unwrap();
    assert!(doc.heads[&head].published.is_none());
}

/// SPARQL reasoning queries are served from the same head as JSON-LD ones.
#[tokio::test]
async fn a_sparql_reasoning_query_is_served_from_the_head() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/sparql:main";
    ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoned_head(id, &head).await.unwrap();
    let sparql = "# PRAGMA reasoning: owl2rl\nPREFIX ex: <http://example.org/>\n\
                  SELECT ?p WHERE { ?p a ex:Person }";
    let resp = fluree
        .graph(id)
        .query()
        .sparql(sparql)
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    let head_tally = resp.reasoning.and_then(|r| r.head);
    assert_eq!(head_tally.map(|h| h.head_id), Some(head.to_string()));
    let text = resp.result.to_string();
    assert!(text.contains("alice"), "{text}");
}

/// A base that has its own index: the first generation copies it into the
/// head's folder and folds the derived facts in; the base keeps answering
/// unreasoned queries from its own index.
#[tokio::test]
async fn a_head_builds_on_an_indexed_base() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/indexed:main";
    let ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"},
                {"@id": "ex:bob", "@type": "ex:Person"}
            ]
        }),
    )
    .await;
    fluree
        .reindex(id, fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();
    let _ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:carol", "@type": "ex:Student"}),
        )
        .await
        .unwrap()
        .ledger;
    let (expected, _) = rows(&fluree, id, &people_query()).await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let (_, kind) = published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    assert_eq!(kind, GenerationKind::Rebuild);
    let (served_rows, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some());
    assert_eq!(served_rows, expected);
    assert_eq!(
        served_rows,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
}

/// A retired head is never served, and refreshing it publishes nothing.
#[tokio::test]
async fn a_retired_head_is_not_served() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/retired:main";
    ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoned_head(id, &head).await.unwrap();
    assert!(rows(&fluree, id, &people_query()).await.1.is_some());
    fluree.retire_reasoned_head(id, &head).await.unwrap();
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_none());
    assert_eq!(people, vec![json!("ex:alice")]);
    assert!(matches!(
        fluree.refresh_reasoned_head(id, &head).await.unwrap(),
        RefreshOutcome::Discarded { .. }
    ));
}

/// A derived-only fact that a window both asserts and retracts is still
/// derived, so it is still present: applying the window's commits alone
/// would have removed it.
#[tokio::test]
async fn a_derived_fact_retracted_in_the_window_stays_while_derived() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "heads/window-retract:main";
    let ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    let ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "@type": "ex:Person"}),
        )
        .await
        .unwrap()
        .ledger;
    let _ledger = retract(
        &fluree,
        ledger,
        json!({"@id": "ex:alice", "@type": "ex:Person"}),
    )
    .await;
    let (_, kind) = published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    assert_eq!(kind, GenerationKind::Logical);
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some());
    assert_eq!(people, vec![json!("ex:alice")]);
}

/// The branch's own collector keeps the dictionary blobs a head still
/// reaches: a head starts from a copy of the base index, which shares the
/// base's dictionaries, and the base's later builds retire them.
#[tokio::test]
async fn the_branch_collector_keeps_dictionaries_a_head_reaches() {
    use fluree_db_indexer::gc::{plan_garbage, release_garbage_plan, CleanGarbageConfig};
    let (writer, reader) = crate::support::instances_sharing_storage();
    let id = "heads/gc:main";
    let ledger = ledger_with(
        &writer,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student", "ex:name": "Alice"},
                {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
            ]
        }),
    )
    .await;
    writer
        .reindex(id, fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();
    let head = writer
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(writer.refresh_reasoned_head(id, &head).await.unwrap());

    // The base moves on and rebuilds; its collector retires the old index.
    let ledger = writer
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:carol", "@type": "ex:Student", "ex:name": "Carol"}),
        )
        .await
        .unwrap()
        .ledger;
    let rebuilt = writer
        .reindex(id, fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();
    let record = writer.nameservice().lookup(id).await.unwrap().unwrap();
    let store = writer.backend().content_store(&record.storage_namespace());
    let config = CleanGarbageConfig {
        max_old_indexes: Some(0),
        min_time_garbage_mins: Some(0),
        ..Default::default()
    };
    let plan = plan_garbage(store.as_ref(), &rebuilt.root_id, &config)
        .await
        .unwrap();
    release_garbage_plan(
        plan,
        writer.backend(),
        writer.nameservice(),
        &fluree_db_core::LedgerId::parse(id).unwrap(),
        Some(&[]),
    )
    .await
    .unwrap();

    // Bring the head to the new commit, then read it from an instance that
    // has nothing cached.
    published(writer.refresh_reasoned_head(id, &head).await.unwrap());
    drop(ledger);
    let q = json!({
        "@context": ctx(),
        "select": ["?p", "?name"],
        "where": [{"@id": "?p", "@type": "ex:Person"}, {"@id": "?p", "ex:name": "?name"}],
        "reasoning": "owl2rl"
    });
    let (people, served) = rows(&reader, id, &q).await;
    assert!(served.is_some());
    assert_eq!(
        people,
        vec![
            json!(["ex:alice", "Alice"]),
            json!(["ex:bob", "Bob"]),
            json!(["ex:carol", "Carol"])
        ]
    );
}

async fn student_commit(
    fluree: &Fluree,
    ledger: fluree_db_api::LedgerState,
    name: &str,
) -> fluree_db_api::LedgerState {
    fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": format!("ex:{name}"), "@type": "ex:Student"}),
        )
        .await
        .unwrap()
        .ledger
}

async fn chain_len(
    fluree: &Fluree,
    id: &str,
    head: &fluree_db_core::reasoned_head::HeadId,
) -> usize {
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoned_head(head.as_str()));
    let doc = fluree.reasoned_heads(id).await.unwrap();
    let pointer = doc.heads[head].published.clone().unwrap();
    fluree_db_indexer::gc::heads::head_generations(store.as_ref(), &pointer.manifest)
        .await
        .unwrap()
        .len()
}

/// With no retention window, each publication releases the generations it
/// superseded, and the head keeps answering.
#[tokio::test]
async fn superseded_generations_are_released_after_the_guard() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.set_reasoned_head_retention(fluree_db_api::reasoned::HeadRetention {
        min_age_ms: 0,
        max_generations_in_guard: 8,
    });
    let id = "heads/retention:main";
    let mut ledger = ledger_with(
        &fluree,
        id,
        json!({"@context": ctx(), "@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}}),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    for name in ["alice", "bob", "carol"] {
        ledger = student_commit(&fluree, ledger, name).await;
        published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    }
    assert_eq!(chain_len(&fluree, id, &head).await, 1);
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served.map(|h| h.generation), Some(3));
    assert_eq!(
        people,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
}

/// Inside the window nothing is released, and once the head holds as many
/// superseded generations as it may, publication waits.
#[tokio::test]
async fn publication_waits_rather_than_shorten_the_guard() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.set_reasoned_head_retention(fluree_db_api::reasoned::HeadRetention {
        min_age_ms: 3_600_000,
        max_generations_in_guard: 1,
    });
    let id = "heads/throttle:main";
    let mut ledger = ledger_with(
        &fluree,
        id,
        json!({"@context": ctx(), "@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}}),
    )
    .await;
    let head = fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    ledger = student_commit(&fluree, ledger, "alice").await;
    published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    ledger = student_commit(&fluree, ledger, "bob").await;
    published(fluree.refresh_reasoned_head(id, &head).await.unwrap());
    assert_eq!(chain_len(&fluree, id, &head).await, 2);
    let _ledger = student_commit(&fluree, ledger, "carol").await;
    assert_eq!(
        fluree.refresh_reasoned_head(id, &head).await.unwrap(),
        RefreshOutcome::Throttled { in_guard: 1 }
    );
    assert_eq!(chain_len(&fluree, id, &head).await, 2);
}

/// The worker refreshes a head after each commit, with no explicit refresh.
#[tokio::test]
async fn the_worker_follows_commits() {
    let fluree = std::sync::Arc::new(FlureeBuilder::memory().build_memory());
    let id = "heads/worker:main";
    let ledger = ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    let worker = tokio::spawn(
        fluree_db_api::ReasonedHeadWorker::new(std::sync::Arc::clone(&fluree))
            .with_catchup_interval(std::time::Duration::from_secs(3600))
            .run(),
    );
    fluree
        .register_reasoned_head(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let served_at = |generation: u64| {
        let fluree = std::sync::Arc::clone(&fluree);
        async move {
            for _ in 0..200 {
                if let (people, Some(head)) = rows(&fluree, id, &people_query()).await {
                    if head.generation >= generation {
                        return people;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            panic!("generation {generation} was never served");
        }
    };
    assert_eq!(served_at(1).await, vec![json!("ex:alice")]);
    let _ledger = student_commit(&fluree, ledger, "bob").await;
    assert_eq!(served_at(2).await, vec![json!("ex:alice"), json!("ex:bob")]);
    worker.abort();
}
