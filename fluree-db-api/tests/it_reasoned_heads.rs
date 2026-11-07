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
