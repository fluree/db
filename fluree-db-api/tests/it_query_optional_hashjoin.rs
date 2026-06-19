//! Batched correlated-OPTIONAL hash-join end-to-end test.
//!
//! Exercises `PlanTreeOptionalBuilder::build_batch` (the batched hash
//! left-join that replaces the per-driving-row OPTIONAL subplan rebuild —
//! the LDBC IC5 cliff). The query mirrors IC5's exact shape:
//!
//! ```text
//! MATCH (forum:Forum)-[:HAS_MEMBER]->(friend:Person)
//! OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
//! WITH forum, count(post) AS postCount
//! RETURN forum.name AS forumName, postCount ORDER BY postCount DESC, forumName ASC
//! ```
//!
//! The OPTIONAL is correlated on BOTH `friend` and `forum` and introduces the
//! new var `post`, so it routes through the general multi-pattern path and the
//! batched hash-join. The dataset includes a forum (`F3`) whose member has no
//! qualifying post, validating the left-join no-match (count = 0) path.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

#[tokio::test]
async fn correlated_multi_pattern_optional_counts_per_group_with_left_join() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/optional:hashjoin";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // People, forums (with member edges), and posts (creator + container edges).
    //
    // Members:   F1 -> {bob, carol}, F2 -> {bob}, F3 -> {bob}
    // Posts:     p1 (creator bob,   in F1)
    //            p2 (creator carol, in F1)
    //            p3 (creator bob,   in F2)
    //            (F3 has a member, bob, but no post by bob in F3)
    //
    // Per (friend, forum) membership the OPTIONAL finds posts the friend
    // created that are contained in that forum, then count(post) groups by
    // forum:
    //   F1: (bob,F1)->p1  + (carol,F1)->p2  = 2
    //   F2: (bob,F2)->p3                     = 1
    //   F3: (bob,F3)->(none)                 = 0   <- left-join no-match
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:bob",   "@type": "ex:Person"},
            {"@id": "ex:carol", "@type": "ex:Person"},
            {
                "@id": "ex:f1", "@type": "ex:Forum", "ex:name": "F1",
                "ex:HAS_MEMBER": [{"@id": "ex:bob"}, {"@id": "ex:carol"}],
                "ex:CONTAINER_OF": [{"@id": "ex:p1"}, {"@id": "ex:p2"}]
            },
            {
                "@id": "ex:f2", "@type": "ex:Forum", "ex:name": "F2",
                "ex:HAS_MEMBER": [{"@id": "ex:bob"}],
                "ex:CONTAINER_OF": [{"@id": "ex:p3"}]
            },
            {
                "@id": "ex:f3", "@type": "ex:Forum", "ex:name": "F3",
                "ex:HAS_MEMBER": [{"@id": "ex:bob"}]
            },
            {"@id": "ex:p1", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:bob"}},
            {"@id": "ex:p2", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:carol"}},
            {"@id": "ex:p3", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:bob"}}
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    // JSON-LD form of the IC5 shape: the OPTIONAL is correlated on BOTH
    // `?friend` and `?forum` and introduces `?post`, so it routes through the
    // general multi-pattern path and the batched hash-left-join.
    let q = json!({
        "@context": ctx(),
        "select": ["?forumName", "(as (count ?post) ?postCount)"],
        "where": [
            {"@id": "?forum", "@type": "ex:Forum", "ex:name": "?forumName", "ex:HAS_MEMBER": "?friend"},
            {"@id": "?friend", "@type": "ex:Person"},
            ["optional",
                {"@id": "?post", "@type": "ex:Post", "ex:HAS_CREATOR": "?friend"},
                {"@id": "?forum", "ex:CONTAINER_OF": "?post"}
            ]
        ],
        "groupBy": ["?forum", "?forumName"],
        "orderBy": ["(desc ?postCount)", "?forumName"]
    });

    let result = fluree
        .query(&db, &q)
        .await
        .expect("optional hash-join query");

    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format rows");

    let arr = rows.as_array().expect("tabular array result");
    let pairs: Vec<(String, i64)> = arr
        .iter()
        .map(|row| {
            let r = row.as_array().expect("row is array");
            let name = r[0].as_str().expect("forumName string").to_string();
            let count = r[1].as_i64().expect("postCount int");
            (name, count)
        })
        .collect();

    assert_eq!(
        pairs,
        vec![
            ("F1".to_string(), 2),
            ("F2".to_string(), 1),
            ("F3".to_string(), 0),
        ],
        "per-forum post counts (incl. the left-join zero for F3) must match"
    );
}
