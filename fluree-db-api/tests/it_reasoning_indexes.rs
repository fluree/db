//! Reasoning indexes: registered reasoning programs persisted as merged
//! indexes, served to matching queries at their basis commit.

use crate::support::normalize_rows;
use fluree_db_api::reasoning_index::RefreshOutcome;
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::reasoning_index::GenerationKind;
use fluree_db_core::ReasoningIndexTally;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl": "http://www.w3.org/2002/07/owl#"
    })
}

/// A tracked query's rows, sorted, and the index that served it, if any.
async fn rows(
    fluree: &Fluree,
    ledger_id: &str,
    query: &JsonValue,
) -> (Vec<JsonValue>, Option<ReasoningIndexTally>) {
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
        resp.reasoning.and_then(|r| r.index).map(|h| *h),
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
async fn an_index_answers_owl2rl_queries_at_its_commit_and_follows_the_branch() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/owl:main";
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

    let (expected, index) = rows(&fluree, id, &people_query()).await;
    assert_eq!(index, None, "nothing is registered yet");
    assert_eq!(expected, vec![json!("ex:alice"), json!("ex:bob")]);

    let index_id = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let again = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    assert_eq!(
        index_id, again,
        "registering an active program is idempotent"
    );

    let (generation, kind) =
        published(fluree.refresh_reasoning_index(id, &index_id).await.unwrap());
    assert_eq!((generation, kind), (1, GenerationKind::Rebuild));

    let (served, index) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served, expected);
    let index = index.expect("the index answers at its basis");
    assert_eq!(
        (index.index_id.as_str(), index.generation),
        (index_id.as_str(), 1)
    );

    // Without reasoning nothing is derived, and the index is not consulted.
    let mut plain = people_query();
    plain.as_object_mut().unwrap().remove("reasoning");
    let (asserted, index) = rows(&fluree, id, &plain).await;
    assert_eq!(index, None);
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
    let (fallback, index) = rows(&fluree, id, &people_query()).await;
    assert_eq!(index, None, "a stale generation is never served");
    assert_eq!(
        fallback,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );

    let (generation, kind) =
        published(fluree.refresh_reasoning_index(id, &index_id).await.unwrap());
    assert_eq!((generation, kind), (2, GenerationKind::Logical));
    let (served, index) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served, fallback);
    assert_eq!(index.map(|h| h.generation), Some(2));

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
    let outcome = fluree.refresh_reasoning_index(id, &index_id).await.unwrap();
    match &outcome {
        RefreshOutcome::Published {
            generation, stats, ..
        } => {
            assert_eq!(*generation, 3);
            assert_eq!(stats.removed, 1, "{stats:?}");
        }
        other => panic!("expected a published generation, got {other:?}"),
    }
    let (served, index) = rows(&fluree, id, &people_query()).await;
    assert_eq!(index.map(|h| h.generation), Some(3));
    assert_eq!(served, vec![json!("ex:bob"), json!("ex:carol")]);

    assert_eq!(
        fluree.refresh_reasoning_index(id, &index_id).await.unwrap(),
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
    let id = "reasoning-indexes/overlap:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
        RefreshOutcome::Published { stats, .. } => {
            assert_eq!(
                stats.derived_only, 0,
                "alice is asserted a Person: {stats:?}"
            );
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
    match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
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
    match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
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
    let id = "reasoning-indexes/chain:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
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
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
    let (reached, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(reached, expected);
}

/// Datalog programs: query-time rules in the program are part of what a
/// query must carry to be served; a different rule set is not served.
#[tokio::test]
async fn a_datalog_program_is_served_only_to_queries_with_its_rules() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/datalog:main";
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

    let index = fluree.register_reasoning_index(id, &program).await.unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
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
    let (_, kind) = published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
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
    let id = "reasoning-indexes/missing-graph:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "datalog"}))
        .await
        .unwrap();
    match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
        RefreshOutcome::Failed { reason } => {
            assert!(reason.contains("inferred"), "{reason}");
        }
        other => panic!("expected a failed run, got {other:?}"),
    }
    let doc = fluree.reasoning_indexes(id).await.unwrap();
    assert!(doc.indexes[&index].head.is_none());
}

/// SPARQL reasoning queries are served from the same index as JSON-LD ones.
#[tokio::test]
async fn a_sparql_reasoning_query_is_served_from_the_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/sparql:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
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
    let index_tally = resp.reasoning.and_then(|r| r.index);
    assert_eq!(index_tally.map(|h| h.index_id), Some(index.to_string()));
    let text = resp.result.to_string();
    assert!(text.contains("alice"), "{text}");
}

/// A base that has its own index: the first generation copies it into the
/// index's folder and folds the derived facts in; the base keeps answering
/// unreasoned queries from its own index.
#[tokio::test]
async fn an_index_builds_on_an_indexed_base() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/indexed:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let (_, kind) = published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    assert_eq!(kind, GenerationKind::Rebuild);
    let (served_rows, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some());
    assert_eq!(served_rows, expected);
    assert_eq!(
        served_rows,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
}

/// A retired index is never served, and refreshing it publishes nothing.
#[tokio::test]
async fn a_retired_index_is_not_served() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/retired:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
    assert!(rows(&fluree, id, &people_query()).await.1.is_some());
    fluree.retire_reasoning_index(id, &index).await.unwrap();
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_none());
    assert_eq!(people, vec![json!("ex:alice")]);
    assert!(matches!(
        fluree.refresh_reasoning_index(id, &index).await.unwrap(),
        RefreshOutcome::Discarded { .. }
    ));
}

/// A derived-only fact that a window both asserts and retracts is still
/// derived, so it is still present: applying the window's commits alone
/// would have removed it.
#[tokio::test]
async fn a_derived_fact_retracted_in_the_window_stays_while_derived() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/window-retract:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
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
    let (_, kind) = published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    assert_eq!(kind, GenerationKind::Logical);
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some());
    assert_eq!(people, vec![json!("ex:alice")]);
}

/// The branch's own collector keeps the dictionary blobs an index still
/// reaches: an index starts from a copy of the base index, which shares the
/// base's dictionaries, and the base's later builds retire them.
#[tokio::test]
async fn the_branch_collector_keeps_dictionaries_an_index_reaches() {
    use fluree_db_indexer::gc::{plan_garbage, release_garbage_plan, CleanGarbageConfig};
    let (writer, reader) = crate::support::instances_sharing_storage();
    let id = "reasoning-indexes/gc:main";
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
    let index = writer
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(writer.refresh_reasoning_index(id, &index).await.unwrap());

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

    // Bring the index to the new commit, then read it from an instance that
    // has nothing cached.
    published(writer.refresh_reasoning_index(id, &index).await.unwrap());
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
    index: &fluree_db_core::reasoning_index::ReasoningIndexId,
) -> usize {
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
    let doc = fluree.reasoning_indexes(id).await.unwrap();
    let pointer = doc.indexes[index].head.clone().unwrap();
    fluree_db_indexer::gc::reasoning_indexes::index_generations(store.as_ref(), &pointer.manifest)
        .await
        .unwrap()
        .len()
}

/// With no retention window, each publication releases the generations it
/// superseded, and the index keeps answering.
#[tokio::test]
async fn superseded_generations_are_released_after_the_guard() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.set_reasoning_index_retention(fluree_db_api::reasoning_index::GenerationRetention {
        min_age_ms: 0,
        max_generations_in_guard: 8,
    });
    let id = "reasoning-indexes/retention:main";
    let mut ledger = ledger_with(
        &fluree,
        id,
        json!({"@context": ctx(), "@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}}),
    )
    .await;
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    for name in ["alice", "bob", "carol"] {
        ledger = student_commit(&fluree, ledger, name).await;
        published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    }
    assert_eq!(chain_len(&fluree, id, &index).await, 1);
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served.map(|h| h.generation), Some(3));
    assert_eq!(
        people,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
}

/// Inside the window nothing is released, and once the index holds as many
/// superseded generations as it may, publication waits.
#[tokio::test]
async fn publication_waits_rather_than_shorten_the_guard() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.set_reasoning_index_retention(fluree_db_api::reasoning_index::GenerationRetention {
        min_age_ms: 3_600_000,
        max_generations_in_guard: 1,
    });
    let id = "reasoning-indexes/throttle:main";
    let mut ledger = ledger_with(
        &fluree,
        id,
        json!({"@context": ctx(), "@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}}),
    )
    .await;
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    ledger = student_commit(&fluree, ledger, "alice").await;
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    ledger = student_commit(&fluree, ledger, "bob").await;
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    assert_eq!(chain_len(&fluree, id, &index).await, 2);
    let _ledger = student_commit(&fluree, ledger, "carol").await;
    assert_eq!(
        fluree.refresh_reasoning_index(id, &index).await.unwrap(),
        RefreshOutcome::Throttled { in_guard: 1 }
    );
    assert_eq!(chain_len(&fluree, id, &index).await, 2);
}

/// The worker refreshes an index after each commit, with no explicit refresh.
#[tokio::test]
async fn the_worker_follows_commits() {
    let fluree = std::sync::Arc::new(FlureeBuilder::memory().build_memory());
    let id = "reasoning-indexes/worker:main";
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
        fluree_db_api::ReasoningIndexWorker::new(std::sync::Arc::clone(&fluree))
            .with_catchup_interval(std::time::Duration::from_secs(3600))
            .run(),
    );
    fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let served_at = |generation: u64| {
        let fluree = std::sync::Arc::clone(&fluree);
        async move {
            for _ in 0..200 {
                if let (people, Some(index)) = rows(&fluree, id, &people_query()).await {
                    if index.generation >= generation {
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

fn with(mut query: JsonValue, key: &str, value: JsonValue) -> JsonValue {
    query.as_object_mut().unwrap().insert(key.into(), value);
    query
}

async fn query_err(fluree: &Fluree, ledger_id: &str, query: &JsonValue) -> String {
    match fluree
        .graph(ledger_id)
        .query()
        .jsonld(query)
        .track_all()
        .execute_tracked()
        .await
    {
        Ok(resp) => panic!("expected an error, got {}", resp.result),
        Err(e) => format!("{e:?}"),
    }
}

async fn school(fluree: &Fluree, id: &str) -> fluree_db_api::LedgerState {
    ledger_with(
        fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:Person", "rdfs:subClassOf": {"@id": "ex:Agent"}},
                {"@id": "ex:alice", "@type": "ex:Student"},
                {"@id": "ex:bob", "@type": "ex:Student"}
            ]
        }),
    )
    .await
}

/// `exact` refuses a capped query-time answer, and an index answers it
/// completely whatever the budget: budgets bound the reasoner, not an index.
#[tokio::test]
async fn exact_refuses_a_capped_answer_and_an_index_gives_the_complete_one() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/exact:main";
    school(&fluree, id).await;
    let q = with(
        with(people_query(), "reasoningBudget", json!({"maxFacts": 1})),
        "reasoningFreshness",
        json!("exact"),
    );
    let err = query_err(&fluree, id, &q).await;
    assert!(err.contains("exact"), "{err}");

    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
    let (people, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(people, vec![json!("ex:alice"), json!("ex:bob")]);
}

/// `strict` never reasons at query time: it waits for a generation at its
/// snapshot, and fails when none arrives.
#[tokio::test]
async fn strict_waits_for_an_index_and_never_reasons_at_query_time() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.set_reasoning_index_strict_wait(std::time::Duration::from_millis(100));
    let id = "reasoning-indexes/strict:main";
    let ledger = school(&fluree, id).await;
    let q = with(people_query(), "reasoningFreshness", json!("strict"));
    let err = query_err(&fluree, id, &q).await;
    assert!(
        err.contains("status: 408") && err.contains("strict"),
        "{err}"
    );

    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
    let (people, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(people, vec![json!("ex:alice"), json!("ex:bob")]);

    // A newer commit with no generation yet: strict fails again rather than
    // answer from an older snapshot.
    let _ledger = student_commit(&fluree, ledger, "carol").await;
    let err = query_err(&fluree, id, &q).await;
    assert!(
        err.contains("status: 408") && err.contains("strict"),
        "{err}"
    );
}

/// `asOf` answers from the newest generation at or before the snapshot,
/// reading the whole query at that generation's basis, and says so.
#[tokio::test]
async fn as_of_reads_the_whole_query_at_the_generation_basis() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/as-of:main";
    let ledger = school(&fluree, id).await;
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
    let _ledger = student_commit(&fluree, ledger, "carol").await;

    let as_of = with(people_query(), "reasoningFreshness", json!("asOf"));
    let (people, served) = rows(&fluree, id, &as_of).await;
    let served = served.expect("the older generation answers");
    assert_eq!(served.basis_t, 1);
    assert_eq!(people, vec![json!("ex:alice"), json!("ex:bob")]);

    // Asserted data comes from the basis too: carol's own type is not read.
    let plain_types = with(
        json!({"@context": ctx(), "select": "?p", "where": {"@id": "?p", "@type": "ex:Student"}}),
        "reasoning",
        json!("owl2rl"),
    );
    let (students, served) = rows(
        &fluree,
        id,
        &with(plain_types, "reasoningFreshness", json!("asOf")),
    )
    .await;
    assert_eq!(served.map(|s| s.basis_t), Some(1));
    assert_eq!(students, vec![json!("ex:alice"), json!("ex:bob")]);

    // The default mode reasons at query time instead, at the current commit.
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_none());
    assert_eq!(
        people,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
}

/// The SPARQL pragma carries freshness as the JSON-LD key does.
#[tokio::test]
async fn the_sparql_freshness_pragma_is_honored() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.set_reasoning_index_strict_wait(std::time::Duration::from_millis(50));
    let id = "reasoning-indexes/sparql-strict:main";
    school(&fluree, id).await;
    let sparql = "# PRAGMA reasoning: owl2rl\n# PRAGMA reasoning-freshness: strict\n\
                  PREFIX ex: <http://example.org/>\nSELECT ?p WHERE { ?p a ex:Person }";
    let refused = fluree
        .graph(id)
        .query()
        .sparql(sparql)
        .track_all()
        .execute_tracked()
        .await;
    assert_eq!(refused.err().map(|e| e.status), Some(408));
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    fluree.refresh_reasoning_index(id, &index).await.unwrap();
    let resp = fluree
        .graph(id)
        .query()
        .sparql(sparql)
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    assert!(resp.reasoning.and_then(|r| r.index).is_some());
}

/// The streaming query path is answered from an index too: under a budget that
/// caps query-time reasoning, the stream still returns the complete answer.
#[tokio::test]
async fn a_streaming_query_is_served_from_the_index() {
    use fluree_db_api::{OwnedStreamQuery, QueryExecutionOptions, Tracker};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/stream:main";
    let students: Vec<JsonValue> = (0..10)
        .map(|i| json!({"@id": format!("ex:s{i}"), "@type": "ex:Student"}))
        .collect();
    let mut graph = vec![json!({"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}})];
    graph.extend(students);
    ledger_with(&fluree, id, json!({"@context": ctx(), "@graph": graph})).await;
    let capped = with(people_query(), "reasoningBudget", json!({"maxFacts": 1}));
    let (query_time, _) = rows(&fluree, id, &capped).await;
    assert!(
        query_time.len() < 10,
        "the budget caps query-time reasoning"
    );

    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());

    let db = fluree.db(id).await.unwrap();
    let plan = fluree
        .plan_stream_query(&db, &OwnedStreamQuery::JsonLd(capped))
        .await
        .unwrap();
    let ledger = fluree.ledger(id).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    fluree
        .run_stream_query(
            ledger,
            plan,
            Tracker::disabled(),
            QueryExecutionOptions::default(),
            tx,
        )
        .await;
    let mut text = String::new();
    while let Some(chunk) = rx.recv().await {
        text.push_str(std::str::from_utf8(&chunk).unwrap());
    }
    let rows = text.lines().filter(|l| l.contains("\"row\"")).count();
    assert_eq!(rows, 10, "{text}");
}

/// A sweep keeps every file a retained generation reaches, and the
/// manifests and derived-only sets it never lists.
#[tokio::test]
async fn a_sweep_keeps_what_the_indexes_reach() {
    let (writer, reader) = crate::support::instances_sharing_storage();
    writer.set_reasoning_index_retention(fluree_db_api::reasoning_index::GenerationRetention {
        min_age_ms: 3_600_000,
        max_generations_in_guard: 8,
    });
    let id = "reasoning-indexes/sweep:main";
    let ledger = school(&writer, id).await;
    writer
        .reindex(id, fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();
    let index = writer
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(writer.refresh_reasoning_index(id, &index).await.unwrap());
    let ledger = student_commit(&writer, ledger, "carol").await;
    published(writer.refresh_reasoning_index(id, &index).await.unwrap());

    writer
        .sweep_index_storage("reasoning-indexes/sweep")
        .await
        .unwrap();

    // Both retained generations still read: the current one served, and the
    // older one through its manifest.
    assert_eq!(chain_len(&reader, id, &index).await, 2);
    let (people, served) = rows(&reader, id, &people_query()).await;
    assert_eq!(served.map(|h| h.generation), Some(2));
    assert_eq!(
        people,
        vec![json!("ex:alice"), json!("ex:bob"), json!("ex:carol")]
    );
    // The next generation folds on the swept index.
    let _ledger = student_commit(&writer, ledger, "dave").await;
    let (_, kind) = published(writer.refresh_reasoning_index(id, &index).await.unwrap());
    assert_eq!(kind, GenerationKind::Logical);
    let (people, served) = rows(&writer, id, &people_query()).await;
    assert_eq!(served.map(|h| h.generation), Some(3));
    assert_eq!(people.len(), 4);
}

/// Dropping a branch deletes its reasoning indexes with it.
#[tokio::test]
async fn a_branch_drop_deletes_its_indexes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = "reasoning-indexes/branch:main";
    let ledger = school(&fluree, main).await;
    drop(ledger);
    fluree
        .create_branch("reasoning-indexes/branch", "dev", None, None)
        .await
        .unwrap();
    let dev = "reasoning-indexes/branch:dev";
    let index = fluree
        .register_reasoning_index(dev, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(dev, &index).await.unwrap());
    let record = fluree.nameservice().lookup(dev).await.unwrap().unwrap();
    let store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
    let manifest = fluree.reasoning_indexes(dev).await.unwrap().indexes[&index]
        .head
        .clone()
        .unwrap()
        .manifest;
    assert!(store.has(&manifest).await.unwrap());

    fluree
        .drop_branch("reasoning-indexes/branch", "dev")
        .await
        .unwrap();
    assert!(
        !store.has(&manifest).await.unwrap(),
        "the index's files are deleted"
    );
    // The main branch is untouched and reasons at query time.
    let (people, served) = rows(&fluree, main, &people_query()).await;
    assert!(served.is_none());
    assert_eq!(people, vec![json!("ex:alice"), json!("ex:bob")]);
}

/// A rule that mints a constant gives the index a dictionary blob of its own,
/// which only the index reaches; a sweep keeps it.
#[tokio::test]
async fn a_sweep_keeps_a_dictionary_only_the_index_reaches() {
    let (writer, reader) = crate::support::instances_sharing_storage();
    let id = "reasoning-indexes/sweep-dict:main";
    school(&writer, id).await;
    writer
        .reindex(id, fluree_db_api::ReindexOptions::default())
        .await
        .unwrap();
    let rules = json!([{
        "@id": "ex:tagRule", "@context": ctx(),
        "where": {"@id": "?x", "@type": "ex:Student"},
        "insert": {"@id": "?x", "ex:tag": "minted-by-the-rule-only"}
    }]);
    let index = writer
        .register_reasoning_index(id, &json!({"reasoning": "datalog", "rules": rules}))
        .await
        .unwrap();
    published(writer.refresh_reasoning_index(id, &index).await.unwrap());

    writer
        .sweep_index_storage("reasoning-indexes/sweep-dict")
        .await
        .unwrap();

    let q = json!({
        "@context": ctx(),
        "select": ["?x", "?tag"],
        "where": {"@id": "?x", "ex:tag": "?tag"},
        "reasoning": "datalog",
        "rules": rules
    });
    let (tagged, served) = rows(&reader, id, &q).await;
    assert!(served.is_some());
    assert_eq!(
        tagged,
        vec![
            json!(["ex:alice", "minted-by-the-rule-only"]),
            json!(["ex:bob", "minted-by-the-rule-only"])
        ]
    );
}

/// A rebase replaces the commits under a published generation: the
/// generation is obsolete, never served, and the next one is a rebuild.
#[tokio::test]
async fn a_rebase_under_an_index_selects_the_rebuild_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = school(&fluree, "reasoning-indexes/rebase:main").await;
    fluree
        .create_branch("reasoning-indexes/rebase", "dev", None, None)
        .await
        .unwrap();
    let dev_id = "reasoning-indexes/rebase:dev";
    let dev = fluree.ledger(dev_id).await.unwrap();
    let _dev = student_commit(&fluree, dev, "dana").await;
    let index = fluree
        .register_reasoning_index(dev_id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(
        fluree
            .refresh_reasoning_index(dev_id, &index)
            .await
            .unwrap(),
    );
    assert!(rows(&fluree, dev_id, &people_query()).await.1.is_some());

    let _main = student_commit(&fluree, main, "mo").await;
    fluree
        .rebase_branch(
            "reasoning-indexes/rebase",
            "dev",
            fluree_db_api::ConflictStrategy::default(),
        )
        .await
        .unwrap();

    let (people, served) = rows(&fluree, dev_id, &people_query()).await;
    assert!(
        served.is_none(),
        "the generation's basis is off the rebased line"
    );
    let expected = vec![
        json!("ex:alice"),
        json!("ex:bob"),
        json!("ex:dana"),
        json!("ex:mo"),
    ];
    assert_eq!(people, expected);

    let (generation, kind) = published(
        fluree
            .refresh_reasoning_index(dev_id, &index)
            .await
            .unwrap(),
    );
    assert_eq!((generation, kind), (2, GenerationKind::Rebuild));
    let (people, served) = rows(&fluree, dev_id, &people_query()).await;
    assert_eq!(served.map(|h| h.generation), Some(2));
    assert_eq!(people, expected);
}

const TRIG: &str = r#"
@prefix ex: <http://example.org/> .
ex:a ex:parent ex:b .
ex:b ex:parent ex:c .
ex:c ex:parent ex:d .
GRAPH ex:inferred { ex:seed ex:note "inferred graph" . }
GRAPH ex:stage { ex:x ex:parent ex:y . ex:y ex:parent ex:z . }
"#;

/// A rule that derives into a named graph: the index holds the derived facts
/// in that graph, and a query of the graph is served with the same answer as
/// query-time reasoning; the default graph never sees them.
#[tokio::test]
async fn derived_facts_land_in_the_rule_s_named_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/named:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let _ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(TRIG)
        .execute()
        .await
        .unwrap()
        .ledger;
    let ex = json!({"ex": "http://example.org/"});
    let rules = json!([{
        "@context": ex,
        "where": [{"@id": "?x", "ex:parent": "?y"}, {"@id": "?y", "ex:parent": "?z"}],
        "insert": {"@id": "?x", "@graph": "ex:inferred", "ex:grandparent": {"@id": "?z"}}
    }]);
    let in_graph = json!({
        "@context": ex,
        "select": ["?x", "?z"],
        "where": [["graph", "http://example.org/inferred", {"@id": "?x", "ex:grandparent": "?z"}]],
        "reasoning": "datalog",
        "rules": rules
    });
    let in_default = json!({
        "@context": ex,
        "select": ["?x", "?z"],
        "where": {"@id": "?x", "ex:grandparent": "?z"},
        "reasoning": "datalog",
        "rules": rules
    });
    let (expected, _) = rows(&fluree, id, &in_graph).await;
    assert_eq!(
        expected,
        vec![json!(["ex:a", "ex:c"]), json!(["ex:b", "ex:d"])]
    );

    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "datalog", "rules": rules}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    let (served_rows, served) = rows(&fluree, id, &in_graph).await;
    assert!(served.is_some());
    assert_eq!(served_rows, expected);
    let (default_rows, served) = rows(&fluree, id, &in_default).await;
    assert!(served.is_some());
    assert!(default_rows.is_empty(), "{default_rows:?}");
}

/// A program that reasons against a named graph is served to queries of
/// that graph, and only those.
#[tokio::test]
async fn a_program_over_a_named_graph_serves_queries_of_that_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/over-graph:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let _ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(TRIG)
        .execute()
        .await
        .unwrap()
        .ledger;
    let ex = json!({"ex": "http://example.org/"});
    let rules = json!([{
        "@context": ex,
        "where": [{"@id": "?x", "ex:parent": "?y"}, {"@id": "?y", "ex:parent": "?z"}],
        "insert": {"@id": "?x", "ex:grandparent": {"@id": "?z"}}
    }]);
    let q = json!({
        "@context": ex,
        "select": ["?x", "?z"],
        "where": {"@id": "?x", "ex:grandparent": "?z"},
        "reasoning": "datalog",
        "rules": rules
    });
    let stage = format!("{id}#http://example.org/stage");
    let (expected, _) = rows(&fluree, &stage, &q).await;
    assert_eq!(expected, vec![json!(["ex:x", "ex:z"])]);

    let index = fluree
        .register_reasoning_index(
            id,
            &json!({"reasoning": "datalog", "rules": rules, "graph": "http://example.org/stage"}),
        )
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    let (served_rows, served) = rows(&fluree, &stage, &q).await;
    assert!(served.is_some(), "a query of the reasoned graph is served");
    assert_eq!(served_rows, expected);
    let (default_rows, served) = rows(&fluree, id, &q).await;
    assert!(served.is_none(), "the default graph is another program");
    assert_eq!(
        default_rows,
        vec![json!(["ex:a", "ex:c"]), json!(["ex:b", "ex:d"])]
    );
}

/// owl:sameAs: what query-time reasoning answers through equality, an index
/// answers the same.
#[tokio::test]
async fn same_as_answers_match_query_time() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/same-as:main";
    ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:carol", "owl:sameAs": {"@id": "ex:carol-lynn"}},
                {"@id": "ex:carol-lynn", "ex:age": 30, "ex:knows": {"@id": "ex:dan"}},
                {"@id": "ex:dan", "@type": "ex:Person"},
                {"@id": "ex:knows", "@type": "owl:SymmetricProperty"}
            ]
        }),
    )
    .await;
    let queries = [
        json!({"@context": ctx(), "select": "?s", "where": {"@id": "ex:carol-lynn", "owl:sameAs": "?s"}, "reasoning": "owl2rl"}),
        json!({"@context": ctx(), "select": "?age", "where": {"@id": "ex:carol", "ex:age": "?age"}, "reasoning": "owl2rl"}),
        json!({"@context": ctx(), "select": "?x", "where": {"@id": "ex:dan", "ex:knows": "?x"}, "reasoning": "owl2rl"}),
        json!({"@context": ctx(), "select": ["?s", "?o"], "where": {"@id": "?s", "ex:knows": "?o"}, "reasoning": "owl2rl"}),
    ];
    let mut expected = Vec::new();
    for q in &queries {
        expected.push(rows(&fluree, id, q).await.0);
    }
    // The equality itself is derived (symmetry); the index must hold it.
    assert!(
        expected[0].contains(&json!("ex:carol")),
        "{:?}",
        expected[0]
    );
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    for (q, expected) in queries.iter().zip(&expected) {
        let (got, served) = rows(&fluree, id, q).await;
        assert!(served.is_some());
        assert_eq!(&got, expected, "{q}");
    }
}

/// Two indexes of one branch publish concurrently; neither entry is lost.
#[tokio::test]
async fn two_indexes_of_a_branch_publish_concurrently() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/two:main";
    school(&fluree, id).await;
    let owl = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    let rules = json!([{"@context": ctx(), "where": {"@id": "?x", "@type": "ex:Student"},
                        "insert": {"@id": "?x", "ex:enrolled": true}}]);
    let datalog = fluree
        .register_reasoning_index(id, &json!({"reasoning": "datalog", "rules": rules}))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        fluree.refresh_reasoning_index(id, &owl),
        fluree.refresh_reasoning_index(id, &datalog)
    );
    published(a.unwrap());
    published(b.unwrap());
    let doc = fluree.reasoning_indexes(id).await.unwrap();
    assert!(doc.indexes[&owl].head.is_some());
    assert!(doc.indexes[&datalog].head.is_some());
    let (people, served) = rows(&fluree, id, &people_query()).await;
    assert_eq!(served.map(|h| h.index_id), Some(owl.to_string()));
    assert_eq!(people, vec![json!("ex:alice"), json!("ex:bob")]);
}

/// A dataset of one ledger's graphs (`from` and `fromNamed` naming the same
/// ledger) is answered from the index, every graph read from it.
#[tokio::test]
async fn a_within_ledger_dataset_is_served_from_the_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/dataset:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let _ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(TRIG)
        .execute()
        .await
        .unwrap()
        .ledger;
    let ex = json!({"ex": "http://example.org/"});
    let rules = json!([{
        "@context": ex,
        "where": [{"@id": "?x", "ex:parent": "?y"}, {"@id": "?y", "ex:parent": "?z"}],
        "insert": {"@id": "?x", "@graph": "ex:inferred", "ex:grandparent": {"@id": "?z"}}
    }]);
    let q = json!({
        "@context": ex,
        "from": id,
        "fromNamed": [format!("{id}#http://example.org/inferred")],
        "select": ["?x", "?z"],
        "where": [["graph", "?g", {"@id": "?x", "ex:grandparent": "?z"}]],
        "reasoning": "datalog",
        "rules": rules
    });
    let run = |q: JsonValue| {
        let fluree = &fluree;
        async move {
            let resp = fluree
                .query_from()
                .jsonld(&q)
                .track_all()
                .execute_tracked()
                .await
                .unwrap_or_else(|e| panic!("{e:?}"));
            (
                normalize_rows(&resp.result),
                resp.reasoning.and_then(|r| r.index).is_some(),
            )
        }
    };
    let (expected, served) = run(q.clone()).await;
    assert!(!served);
    assert_eq!(
        expected,
        vec![json!(["ex:a", "ex:c"]), json!(["ex:b", "ex:d"])]
    );

    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "datalog", "rules": rules}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    let (got, served) = run(q).await;
    assert!(served, "a dataset of one ledger's graphs is served");
    assert_eq!(got, expected);
}

/// A branch does not inherit its source's indexes: they are derived state of
/// the source, and the branch registers its own.
#[tokio::test]
async fn a_new_branch_starts_with_no_indexes() {
    let fluree = FlureeBuilder::memory().build_memory();
    school(&fluree, "reasoning-indexes/inherit:main").await;
    let index = fluree
        .register_reasoning_index(
            "reasoning-indexes/inherit:main",
            &json!({"reasoning": "owl2rl"}),
        )
        .await
        .unwrap();
    published(
        fluree
            .refresh_reasoning_index("reasoning-indexes/inherit:main", &index)
            .await
            .unwrap(),
    );
    fluree
        .create_branch("reasoning-indexes/inherit", "dev", None, None)
        .await
        .unwrap();
    let doc = fluree
        .reasoning_indexes("reasoning-indexes/inherit:dev")
        .await
        .unwrap();
    assert!(doc.indexes.is_empty(), "{doc:?}");
    let (people, served) = rows(&fluree, "reasoning-indexes/inherit:dev", &people_query()).await;
    assert!(served.is_none());
    assert_eq!(people, vec![json!("ex:alice"), json!("ex:bob")]);
}

/// A retired index's files are deleted once the retention window has passed
/// since it was retired, and then its entry; before that both stay.
#[tokio::test]
async fn a_retired_index_is_cleaned_up_after_the_guard() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/cleanup:main";
    school(&fluree, id).await;
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    let manifest = fluree.reasoning_indexes(id).await.unwrap().indexes[&index]
        .head
        .clone()
        .unwrap()
        .manifest;
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let store = fluree
        .backend()
        .content_store(&record.storage_namespace().reasoning_index(index.as_str()));
    fluree.retire_reasoning_index(id, &index).await.unwrap();

    // Inside the window: kept.
    fluree.set_reasoning_index_retention(fluree_db_api::reasoning_index::GenerationRetention {
        min_age_ms: 3_600_000,
        max_generations_in_guard: 8,
    });
    assert!(fluree
        .clean_retired_reasoning_indexes(id)
        .await
        .unwrap()
        .is_empty());
    assert!(store.has(&manifest).await.unwrap());

    // Past it: files, then the entry.
    fluree.set_reasoning_index_retention(fluree_db_api::reasoning_index::GenerationRetention {
        min_age_ms: 0,
        max_generations_in_guard: 8,
    });
    assert_eq!(
        fluree.clean_retired_reasoning_indexes(id).await.unwrap(),
        vec![index.clone()]
    );
    assert!(!store.has(&manifest).await.unwrap());
    assert!(fluree
        .reasoning_indexes(id)
        .await
        .unwrap()
        .indexes
        .is_empty());
}

/// A branch takes at most `MAX_ACTIVE_INDEXES` active indexes.
#[tokio::test]
async fn a_branch_refuses_more_active_indexes_than_its_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/limit:main";
    school(&fluree, id).await;
    let max = fluree_db_api::reasoning_index::MAX_ACTIVE_INDEXES;
    for i in 0..max {
        let rules = json!([{"@context": ctx(), "where": {"@id": "?x", "@type": "ex:Student"},
                            "insert": {"@id": "?x", "ex:mark": i}}]);
        fluree
            .register_reasoning_index(id, &json!({"reasoning": "datalog", "rules": rules}))
            .await
            .unwrap();
    }
    let err = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("active reasoning indexes"),
        "{err}"
    );
}

/// A staged view is answered from its own state, never from an index, even
/// one published at the `t` its transaction would commit at.
#[tokio::test]
async fn a_staged_view_is_never_served_from_an_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/staged:main";
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
    let index = fluree
        .register_reasoning_index(id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());

    let staged = fluree
        .stage_owned(ledger)
        .insert(&json!({"@context": ctx(), "@id": "ex:staged", "@type": "ex:Student"}))
        .stage()
        .await
        .unwrap();
    let preview = fluree_db_api::GraphDb::from_staged(&staged).unwrap();
    let preview_rows = || async {
        let result = fluree.query(&preview, &people_query()).await.unwrap();
        normalize_rows(&result.to_jsonld(&preview.snapshot).unwrap())
    };
    let expected = vec![json!("ex:alice"), json!("ex:bob"), json!("ex:staged")];
    assert_eq!(preview_rows().await, expected);

    // Another transaction commits at the staged `t`, and its index is built.
    let ledger = fluree.ledger(id).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@id": "ex:committed", "@type": "ex:Student"}),
        )
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    let (_, served) = rows(&fluree, id, &people_query()).await;
    assert!(served.is_some(), "the committed state is served");
    assert_eq!(
        preview_rows().await,
        expected,
        "the staged view still reads its own state"
    );
}

/// `rdfs` widens a query rather than materializing, so a query that mixes
/// it with datalog keeps the rewrite when served from the index of that
/// program, in a single view and in a dataset.
#[tokio::test]
async fn an_index_keeps_the_rdfs_rewrite_of_a_mixed_program() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/rdfs-datalog:main";
    ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
                {"@id": "ex:alice", "@type": "ex:Student"},
                {"@id": "ex:bob", "ex:enrolled": {"@id": "ex:cs101"}}
            ]
        }),
    )
    .await;
    let rules = json!([{
        "@id": "ex:enrolledIsStudent", "@context": ctx(),
        "where": {"@id": "?x", "ex:enrolled": "?c"},
        "insert": {"@id": "?x", "@type": "ex:Student"}
    }]);
    let q = json!({
        "@context": ctx(),
        "select": "?p",
        "where": {"@id": "?p", "@type": "ex:Person"},
        "reasoning": ["rdfs", "datalog"],
        "rules": rules
    });
    let dataset_q = with(q.clone(), "from", json!(id));
    let run_dataset = |q: JsonValue| {
        let fluree = &fluree;
        async move {
            let resp = fluree
                .query_from()
                .jsonld(&q)
                .track_all()
                .execute_tracked()
                .await
                .unwrap_or_else(|e| panic!("{e:?}"));
            (
                normalize_rows(&resp.result),
                resp.reasoning.and_then(|r| r.index).is_some(),
            )
        }
    };
    let expected = vec![json!("ex:alice"), json!("ex:bob")];
    let (before, served) = rows(&fluree, id, &q).await;
    assert!(served.is_none());
    assert_eq!(before, expected);
    assert_eq!(
        run_dataset(dataset_q.clone()).await,
        (expected.clone(), false)
    );

    let index = fluree
        .register_reasoning_index(
            id,
            &json!({"reasoning": ["rdfs", "datalog"], "rules": rules}),
        )
        .await
        .unwrap();
    published(fluree.refresh_reasoning_index(id, &index).await.unwrap());
    let (after, served) = rows(&fluree, id, &q).await;
    assert!(served.is_some());
    assert_eq!(after, expected);
    assert_eq!(run_dataset(dataset_q).await, (expected, true));
}

/// Query-time `rdfs` reads the class hierarchy without derived facts, so a
/// program that derives hierarchy edges under it publishes nothing.
#[tokio::test]
async fn a_program_deriving_hierarchy_edges_under_rdfs_publishes_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/derived-hierarchy:main";
    ledger_with(
        &fluree,
        id,
        json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:Person", "ex:narrower": {"@id": "ex:Student"}},
                {"@id": "ex:alice", "@type": "ex:Student"}
            ]
        }),
    )
    .await;
    let rules = json!([{
        "@id": "ex:narrowerIsSubclass", "@context": ctx(),
        "where": {"@id": "?c", "ex:narrower": "?d"},
        "insert": {"@id": "?d", "rdfs:subClassOf": {"@id": "?c"}}
    }]);
    let index = fluree
        .register_reasoning_index(
            id,
            &json!({"reasoning": ["rdfs", "datalog"], "rules": rules}),
        )
        .await
        .unwrap();
    match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
        RefreshOutcome::Failed { reason } => {
            assert!(reason.contains("rdfs:subClassOf"), "{reason}");
        }
        other => panic!("expected a failed refresh, got {other:?}"),
    }
}

/// An OWL 2 RL pass that fails under a positive datalog program leaves the
/// rules to derive what they can, which is not the program's output: the
/// refresh publishes nothing, and an `exact` query refuses the answer.
#[tokio::test]
async fn a_failed_owl_pass_under_datalog_publishes_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "reasoning-indexes/owl-failed:main";
    // A property expression nested past the reasoner's limit fails the
    // OWL 2 RL pass outright.
    let mut graph: Vec<JsonValue> = (0..12)
        .map(|i| {
            json!({"@id": format!("ex:p{i}"), "owl:inverseOf": {"@id": format!("ex:p{}", i + 1)}})
        })
        .collect();
    graph.push(json!({
        "@id": "ex:R", "@type": "owl:Restriction",
        "owl:onProperty": {"@id": "ex:p0"},
        "owl:someValuesFrom": {"@id": "ex:Thing"}
    }));
    graph.push(json!({"@id": "ex:a", "ex:parent": {"@id": "ex:b"}}));
    ledger_with(&fluree, id, json!({"@context": ctx(), "@graph": graph})).await;
    let rules = json!([{
        "@id": "ex:child", "@context": ctx(),
        "where": {"@id": "?x", "ex:parent": "?y"},
        "insert": {"@id": "?y", "ex:child": {"@id": "?x"}}
    }]);
    let index = fluree
        .register_reasoning_index(
            id,
            &json!({"reasoning": ["owl2rl", "datalog"], "rules": rules}),
        )
        .await
        .unwrap();
    match fluree.refresh_reasoning_index(id, &index).await.unwrap() {
        RefreshOutcome::Failed { reason } => {
            assert!(reason.contains("OWL 2 RL"), "{reason}");
        }
        other => panic!("expected a failed refresh, got {other:?}"),
    }
    let q = json!({
        "@context": ctx(),
        "select": "?x",
        "where": {"@id": "ex:b", "ex:child": "?x"},
        "reasoning": ["owl2rl", "datalog"],
        "rules": rules,
        "reasoningFreshness": "exact"
    });
    let err = query_err(&fluree, id, &q).await;
    assert!(err.contains("OWL 2 RL"), "{err}");
}

/// A schema source in another ledger can change what a program settles to
/// without the branch moving: queries stop matching the index at once, and
/// a refresh rebuilds it at the same commit.
#[tokio::test]
async fn a_refresh_rebuilds_when_an_external_schema_changes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "reasoning-indexes/schema-model:main";
    let data_id = "reasoning-indexes/schema-data:main";
    let schema = |turtle: &str| {
        format!(
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
             @prefix ex: <http://example.org/> .\n{turtle}"
        )
    };
    let model = fluree.create_ledger(model_id).await.unwrap();
    let model = fluree
        .stage_owned(model)
        .upsert_turtle(&schema("ex:Manager rdfs:subClassOf ex:Employee ."))
        .execute()
        .await
        .unwrap()
        .ledger;
    let data = fluree.create_ledger(data_id).await.unwrap();
    let config = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        GRAPH <urn:fluree:{data_id}#config> {{
            <urn:cfg:main>       rdf:type            f:LedgerConfig ;
                                 f:reasoningDefaults <urn:cfg:reasoning> .
            <urn:cfg:reasoning>  f:schemaSource      <urn:cfg:schema-ref> .
            <urn:cfg:schema-ref> rdf:type            f:GraphRef ;
                                 f:graphSource       <urn:cfg:schema-src> .
            <urn:cfg:schema-src> f:ledger            <{model_id}> ;
                                 f:graphSelector     f:defaultGraph .
        }}"
    );
    let data = fluree
        .stage_owned(data)
        .upsert_turtle(&config)
        .execute()
        .await
        .unwrap()
        .ledger;
    fluree
        .insert(
            data,
            &json!({"@context": ctx(), "@id": "ex:anita", "@type": "ex:Manager"}),
        )
        .await
        .unwrap();
    let typed = |class: &str| {
        json!({
            "@context": ctx(),
            "select": "?x",
            "where": {"@id": "?x", "@type": class},
            "reasoning": "owl2rl"
        })
    };

    let index = fluree
        .register_reasoning_index(data_id, &json!({"reasoning": "owl2rl"}))
        .await
        .unwrap();
    published(
        fluree
            .refresh_reasoning_index(data_id, &index)
            .await
            .unwrap(),
    );
    let (employees, served) = rows(&fluree, data_id, &typed("ex:Employee")).await;
    assert_eq!(served.map(|h| h.generation), Some(1));
    assert_eq!(employees, vec![json!("ex:anita")]);

    fluree
        .stage_owned(model)
        .upsert_turtle(&schema("ex:Employee rdfs:subClassOf ex:Person ."))
        .execute()
        .await
        .unwrap();
    let (people, served) = rows(&fluree, data_id, &typed("ex:Person")).await;
    assert!(
        served.is_none(),
        "the index was settled under the old schema"
    );
    assert_eq!(people, vec![json!("ex:anita")]);

    let (generation, kind) = published(
        fluree
            .refresh_reasoning_index(data_id, &index)
            .await
            .unwrap(),
    );
    assert_eq!((generation, kind), (2, GenerationKind::Rebuild));
    let (people, served) = rows(&fluree, data_id, &typed("ex:Person")).await;
    assert_eq!(served.map(|h| h.generation), Some(2));
    assert_eq!(people, vec![json!("ex:anita")]);
}
