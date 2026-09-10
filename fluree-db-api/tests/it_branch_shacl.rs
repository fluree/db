//! SHACL validation on branch operations (merge, rebase, revert).
//!
//! A normal transaction that would violate an attached shape is rejected at
//! commit time. Merge, rebase, and revert build their commits on a parallel
//! path, and that path must run the same validation: an operation whose
//! resulting state the ledger's own shapes reject fails like a transaction
//! would, leaving the target untouched.
//!
//! Regression coverage for #1782.

#![cfg(feature = "shacl")]

use crate::support;
use fluree_db_api::{
    ApiError, CommitRef, ConflictStrategy, FlureeBuilder, MergePreviewOpts, TransactError,
};
use fluree_db_core::graph_registry::config_graph_iri;
use serde_json::json;

const EX: &str = "http://example.org/ns/";

fn ctx() -> serde_json::Value {
    json!({"ex": EX, "sh": "http://www.w3.org/ns/shacl#"})
}

fn insert_name(id: &str, name: &str) -> serde_json::Value {
    json!({"@context": ctx(), "@graph": [{"@id": id, "ex:name": name}]})
}

/// Replace-style update on `id`'s `ex:name`.
fn replace_name(id: &str, name: &str) -> serde_json::Value {
    json!({
        "@context": ctx(),
        "where": {"@id": id, "ex:name": "?old"},
        "delete": {"@id": id, "ex:name": "?old"},
        "insert": {"@id": id, "ex:name": name}
    })
}

/// `sh:targetNode ex:alice` with `sh:maxCount 1` (and optionally
/// `sh:minCount 1`) on `ex:name`.
fn alice_name_shape(min_count: Option<u32>) -> serde_json::Value {
    let mut property = json!({
        "@id": "ex:AliceNameShape",
        "sh:path": {"@id": "ex:name"},
        "sh:maxCount": 1
    });
    if let Some(min) = min_count {
        property["sh:minCount"] = json!(min);
    }
    json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:AliceShape",
            "@type": "sh:NodeShape",
            "sh:targetNode": {"@id": "ex:alice"},
            "sh:property": property
        }]
    })
}

/// All `ex:name` values on `ledger_id`, sorted.
async fn names(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let q = json!({
        "@context": ctx(),
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    let result = support::query_jsonld(fluree, &ledger, &q).await.unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    let mut out: Vec<String> = support::normalize_rows(&rows)
        .iter()
        .map(|row| {
            row.as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .expect("row should be [name]")
        })
        .collect();
    out.sort();
    out
}

async fn head_t(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    fluree.ledger(ledger_id).await.unwrap().t()
}

fn assert_shacl_violation(err: ApiError, context: &str) {
    assert!(
        matches!(err, ApiError::Transact(TransactError::ShaclViolation(_))),
        "{context}: expected ShaclViolation, got: {err:?}"
    );
}

/// Seed `mydb:main` with `ex:alice ex:name "A"` and the maxCount-1 shape,
/// then fork `dev`. Returns main's state (t=1).
async fn seed_alice_with_shape(fluree: &fluree_db_api::Fluree) -> fluree_db_api::LedgerState {
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let mut seed = alice_name_shape(None);
    seed["@graph"]
        .as_array_mut()
        .unwrap()
        .push(json!({"@id": "ex:alice", "ex:name": "A"}));
    let main = fluree.insert(ledger, &seed).await.unwrap().ledger;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    main
}

// =============================================================================
// Control: the same state via a plain transaction is rejected
// =============================================================================

#[tokio::test]
async fn control_plain_insert_of_second_name_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = seed_alice_with_shape(&fluree).await;

    let err = fluree
        .insert(main, &insert_name("ex:alice", "B"))
        .await
        .expect_err("second name breaches maxCount 1");
    assert_shacl_violation(err, "plain insert");
}

// =============================================================================
// Merge
// =============================================================================

/// Take-both on a conflicting key manufactures two values for a
/// maxCount-1 property. The merge must be rejected and main left as it was.
#[tokio::test]
async fn merge_rejected_when_result_violates_shape() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = seed_alice_with_shape(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .update(main, &replace_name("ex:alice", "C"))
        .await
        .unwrap();
    assert_eq!(head_t(&fluree, "mydb:main").await, 2);

    let err = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBoth)
        .await
        .expect_err("merge producing two names must be rejected");
    assert_shacl_violation(err, "take-both merge");

    // Main is untouched: same head, same value. Dev is untouched too.
    assert_eq!(head_t(&fluree, "mydb:main").await, 2);
    assert_eq!(names(&fluree, "mydb:main").await, vec!["C"]);
    assert_eq!(names(&fluree, "mydb:dev").await, vec!["B"]);
}

/// A shape being installed does not make conforming merges fail.
#[tokio::test]
async fn merge_conforming_result_succeeds_with_shape_installed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = seed_alice_with_shape(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .insert(main, &insert_name("ex:bob", "Bob"))
        .await
        .unwrap();

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .expect("conforming merge");
    assert!(!report.fast_forward);
    assert_eq!(report.conflict_count, 0);
    assert_eq!(names(&fluree, "mydb:main").await, vec!["B", "Bob"]);
}

/// Under `f:ValidationWarn` the violation is logged and the merge proceeds,
/// exactly as a warn-mode transaction would.
#[tokio::test]
async fn merge_warn_mode_logs_and_succeeds() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let mut seed = alice_name_shape(None);
    seed["@graph"]
        .as_array_mut()
        .unwrap()
        .push(json!({"@id": "ex:alice", "ex:name": "A"}));
    let main = fluree.insert(ledger, &seed).await.unwrap().ledger;

    let config_iri = config_graph_iri("mydb:main");
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:validationMode f:ValidationWarn .
        }}
        "
    );
    let main = fluree
        .stage_owned(main)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write")
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .update(main, &replace_name("ex:alice", "C"))
        .await
        .unwrap();

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBoth)
        .await
        .expect("warn mode lets the merge through");
    assert_eq!(report.conflict_count, 1);
    assert_eq!(names(&fluree, "mydb:main").await, vec!["B", "C"]);
}

/// The originating field case: the shape lives in a model ledger and the
/// data ledger's `#config` points `f:shapesSource` at it with `f:ledger`.
/// A merge is authoring, not replay, so the cross-ledger source is
/// resolved and enforced.
#[tokio::test]
async fn merge_rejected_by_cross_ledger_shape() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model = fluree.create_ledger("shapes-model").await.unwrap();
    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix sh:  <http://www.w3.org/ns/shacl#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Person ;
                    sh:property     ex:pshape_name .
                ex:pshape_name
                    sh:path     ex:name ;
                    sh:maxCount 1 .
            }}
            "
        ))
        .execute()
        .await
        .expect("seed model shapes");

    let data = fluree.create_ledger("mydb").await.unwrap();
    let config_iri = config_graph_iri("mydb:main");
    let main = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <shapes-model:main> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
            "
        ))
        .execute()
        .await
        .expect("seed cross-ledger config")
        .ledger;
    let main = fluree
        .insert(
            main,
            &json!({
                "@context": ctx(),
                "@graph": [{"@id": "ex:alice", "@type": "ex:Person", "ex:name": "A"}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    // Control: a second name via a plain insert is rejected by M's shape.
    let err = fluree
        .insert(main.clone(), &insert_name("ex:alice", "X"))
        .await
        .expect_err("control: second name rejected by cross-ledger shape");
    assert_shacl_violation(err, "cross-ledger control");

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .update(main, &replace_name("ex:alice", "C"))
        .await
        .unwrap();

    let err = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBoth)
        .await
        .expect_err("merge producing two names must be rejected by the cross-ledger shape");
    assert_shacl_violation(err, "cross-ledger merge");
    assert_eq!(names(&fluree, "mydb:main").await, vec!["C"]);
}

// =============================================================================
// Rebase
// =============================================================================

/// A commit that conformed on the branch can violate a shape installed on
/// the source since the fork. Its replay must be rejected and the branch
/// left as it was.
#[tokio::test]
async fn rebase_rejected_when_replay_violates_shape() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
        .insert(ledger, &insert_name("ex:alice", "A"))
        .await
        .unwrap()
        .ledger;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // dev: a second name is fine, there is no shape on dev.
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(dev, &insert_name("ex:alice", "B"))
        .await
        .unwrap();
    assert_eq!(names(&fluree, "mydb:dev").await, vec!["A", "B"]);

    // main: install the maxCount-1 shape (alice has one name, conforms).
    fluree
        .insert(main, &alice_name_shape(None))
        .await
        .expect("shape install conforms");

    let err = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .expect_err("replaying the second name onto the shape must be rejected");
    assert_shacl_violation(err, "rebase replay");

    // dev is untouched.
    assert_eq!(head_t(&fluree, "mydb:dev").await, 2);
    assert_eq!(names(&fluree, "mydb:dev").await, vec!["A", "B"]);
}

// =============================================================================
// Revert
// =============================================================================

/// Reverting the commit that asserted a value a later shape requires must
/// be rejected, exactly as deleting that value in a transaction is.
#[tokio::test]
async fn revert_rejected_when_inverse_violates_shape() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    // Genesis cannot be reverted, so seed an unrelated first commit.
    let r0 = fluree
        .insert(ledger, &insert_name("ex:carol", "Carol"))
        .await
        .unwrap();
    let r1 = fluree
        .insert(r0.ledger, &insert_name("ex:alice", "A"))
        .await
        .unwrap();
    let r2 = fluree
        .insert(r1.ledger, &alice_name_shape(Some(1)))
        .await
        .expect("shape install conforms");
    let r3 = fluree
        .insert(r2.ledger, &insert_name("ex:bob", "Bob"))
        .await
        .unwrap();
    assert_eq!(r3.receipt.t, 4);

    // Control: deleting the name in a transaction is rejected (minCount 1).
    let err = fluree
        .update(
            r3.ledger,
            &json!({
                "@context": ctx(),
                "delete": {"@id": "ex:alice", "ex:name": "A"}
            }),
        )
        .await
        .expect_err("control: deleting the only name breaches minCount 1");
    assert_shacl_violation(err, "plain delete");

    let err = fluree
        .revert_commit(
            "mydb",
            "main",
            CommitRef::Exact(r1.receipt.commit_id.clone()),
            ConflictStrategy::TakeSource,
        )
        .await
        .expect_err("reverting the name's commit must be rejected");
    assert_shacl_violation(err, "revert");

    assert_eq!(head_t(&fluree, "mydb:main").await, 4);
    assert_eq!(names(&fluree, "mydb:main").await, vec!["A", "Bob", "Carol"]);
}

// =============================================================================
// Merge preview
// =============================================================================

/// Preview runs the same validation the merge runs. Where the merge would be
/// rejected, the preview says so: `mergeable` is false and the report is the
/// one the merge would fail with.
#[tokio::test]
async fn preview_reports_violation_where_merge_would_fail() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = seed_alice_with_shape(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .update(main, &replace_name("ex:alice", "C"))
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                conflict_strategy: ConflictStrategy::TakeBoth,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .expect("preview");
    assert!(!preview.fast_forward);
    assert_eq!(preview.conflicts.count, 1);
    let validation = preview.validation.expect("validation runs by default");
    assert!(!validation.conforms);
    let report = validation
        .report
        .expect("a rejected preview carries the report");
    assert!(
        report.contains("MaxCountConstraintComponent"),
        "report should name the constraint: {report}"
    );
    assert!(
        !preview.mergeable,
        "a merge that would be rejected is not mergeable"
    );

    // The merge agrees with the preview.
    let err = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBoth)
        .await
        .expect_err("merge is rejected");
    assert_shacl_violation(err, "merge after preview");
}

/// Where the merge conforms, the preview says so and stays mergeable.
#[tokio::test]
async fn preview_conforms_where_merge_would_succeed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = seed_alice_with_shape(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .insert(main, &insert_name("ex:bob", "Bob"))
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with("mydb", "dev", None, MergePreviewOpts::default())
        .await
        .expect("preview");
    assert!(!preview.fast_forward);
    let validation = preview.validation.expect("validation runs by default");
    assert!(validation.conforms);
    assert!(validation.report.is_none());
    assert!(preview.mergeable);

    fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .expect("merge succeeds as previewed");
}

/// Opting out leaves the field absent and `mergeable` back to the
/// strategy-only answer, for count-only previews.
#[tokio::test]
async fn preview_validation_can_be_skipped() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main = seed_alice_with_shape(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_name("ex:alice", "B"))
        .await
        .unwrap();
    fluree
        .update(main, &replace_name("ex:alice", "C"))
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_validation: false,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .expect("preview");
    assert!(preview.validation.is_none());
    assert!(
        preview.mergeable,
        "strategy-only answer when validation is skipped"
    );
}
