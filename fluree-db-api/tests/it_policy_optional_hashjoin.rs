//! View-policy enforcement on the batched correlated-OPTIONAL hash-join.
//!
//! `PlanTreeOptionalBuilder::build_batch` seeds and executes the OPTIONAL inner
//! subplan once per required batch via `build_where_operators_seeded`, reusing
//! the normal WHERE operators and the SAME `ExecutionContext`. So the inner's
//! `BinaryScanOperator`s must see the active policy enforcer and filter the
//! seeded scan exactly as the per-row path would.
//!
//! This guards against the batched fast path becoming a view-policy blind spot:
//! a forbidden optional-side row must not leak into the count, and a row whose
//! only matches are all policy-hidden must collapse to the left-join zero.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{assert_index_defaults, genesis_ledger, normalize_rows};

fn ctx() -> JsonValue {
    json!({ "ex": "http://example.org/ns/" })
}

/// IC5-shaped correlated OPTIONAL (3 inner triples, correlated on both `?friend`
/// and `?forum`, introducing `?post`) so it routes through the general
/// multi-pattern path and the batched hash-left-join — the same shape exercised
/// by `it_query_optional_hashjoin`.
fn count_query(from: &str, opts: Option<JsonValue>) -> JsonValue {
    let mut q = json!({
        "@context": ctx(),
        "from": from,
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
        "orderBy": ["?forumName"]
    });
    if let Some(opts) = opts {
        q.as_object_mut().unwrap().insert("opts".into(), opts);
    }
    q
}

#[tokio::test]
async fn batched_optional_inner_enforces_view_policy() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/optional-hashjoin:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Posts carry ex:level; level < 5 is "secret". p3 and p4 are secret.
    //   F1 -> members {bob, carol}, contains p1(bob,10), p2(carol,10), p3(bob,1)
    //   F2 -> members {bob},        contains p4(bob,1)
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:bob",   "@type": "ex:Person"},
            {"@id": "ex:carol", "@type": "ex:Person"},
            {
                "@id": "ex:f1", "@type": "ex:Forum", "ex:name": "F1",
                "ex:HAS_MEMBER": [{"@id": "ex:bob"}, {"@id": "ex:carol"}],
                "ex:CONTAINER_OF": [{"@id": "ex:p1"}, {"@id": "ex:p2"}, {"@id": "ex:p3"}]
            },
            {
                "@id": "ex:f2", "@type": "ex:Forum", "ex:name": "F2",
                "ex:HAS_MEMBER": [{"@id": "ex:bob"}],
                "ex:CONTAINER_OF": [{"@id": "ex:p4"}]
            },
            {"@id": "ex:p1", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:bob"},   "ex:level": 10},
            {"@id": "ex:p2", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:carol"}, "ex:level": 10},
            {"@id": "ex:p3", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:bob"},   "ex:level": 1},
            {"@id": "ex:p4", "@type": "ex:Post", "ex:HAS_CREATOR": {"@id": "ex:bob"},   "ex:level": 1}
        ]
    });
    let ledger = fluree.insert(ledger0, &txn).await.expect("seed").ledger;

    // --- Root baseline: every post is visible. ---------------------------------
    let root = fluree
        .query_connection(&count_query(ledger_id, None))
        .await
        .expect("root query");
    let root_rows = root.to_jsonld(&ledger.snapshot).expect("root rows");
    assert_eq!(
        normalize_rows(&root_rows),
        normalize_rows(&json!([["F1", 3], ["F2", 1]])),
        "root sees all posts: F1={{p1,p2,p3}}=3, F2={{p4}}=1"
    );

    // --- Policy: a REQUIRED f:view policy scoped to ex:Post that only allows
    // posts with ex:level >= 5. Combined with an allow-all and default-allow
    // true, every flake stays visible EXCEPT Post-subject flakes of secret
    // posts — so p3 and p4 vanish from the OPTIONAL inner scan.
    let policy = json!([
        {
            "@id": "ex:postLevelPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:required": true,
            "f:onClass": [{"@id": "http://example.org/ns/Post"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/ns/"},
                    "where": [
                        {"@id": "?$this", "ex:level": "?l"},
                        ["filter", "(>= ?l 5)"]
                    ]
                }
            }
        },
        {
            "@id": "ex:allowAll",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    let restricted = fluree
        .query_connection(&count_query(
            ledger_id,
            Some(json!({ "policy": policy, "default-allow": true })),
        ))
        .await
        .expect("policy query");
    let restricted_rows = restricted.to_jsonld(&ledger.snapshot).expect("policy rows");

    // p3 (secret, F1) and p4 (secret, F2) are hidden. The batched inner must
    // filter them out of the seeded scan:
    //   F1: only p1, p2 visible             -> 2
    //   F2: bob's only post p4 is hidden    -> 0  (left-join no-match under policy)
    // If the seeded inner skipped policy, this would wrongly read 3 and 1.
    assert_eq!(
        normalize_rows(&restricted_rows),
        normalize_rows(&json!([["F1", 2], ["F2", 0]])),
        "batched OPTIONAL inner must enforce view policy: secret posts excluded, \
         F2 collapses to the left-join zero"
    );
}
