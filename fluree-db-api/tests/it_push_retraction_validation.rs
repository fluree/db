//! A pushed commit may retract only facts the target holds, and it may
//! retract facts the target holds in its index.
//!
//! Each case writes a value on a source ledger and then replaces it, pushes
//! the first commit to a target, builds the target's index and reloads it,
//! and pushes the second commit, whose retraction names the indexed value:
//! in a named graph, and stored with less detail than written (a big
//! integer's XSD subtype, which the index does not keep).

use crate::support;
use fluree_db_api::{
    ExportCommitsRequest, FlureeBuilder, GovernanceOptions, IndexConfig, PushCommitsRequest,
};

const PREFIXES: &str = "@prefix ex: <http://example.org/> .\n\
                        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
const BIG: &str = "123456789012345678901234567890";

#[tokio::test]
async fn a_push_retracts_values_the_target_holds_in_its_index() {
    let cases = [
        (
            "a named graph",
            "GRAPH <http://example.org/g> { ex:alice ex:salary \"50000\" . }".to_string(),
            "GRAPH <http://example.org/g> { ex:alice ex:salary \"75000\" . }".to_string(),
            "SELECT ?v WHERE { GRAPH <http://example.org/g> { ex:alice ex:salary ?v } }",
            "75000",
        ),
        (
            "a subtyped big integer",
            format!("ex:alice ex:serial \"{BIG}\"^^xsd:nonNegativeInteger ."),
            "ex:alice ex:serial 7 .".to_string(),
            "SELECT ?v WHERE { ex:alice ex:serial ?v }",
            "7",
        ),
    ];
    let mut failures = Vec::new();
    for (i, (what, first, second, query, expect)) in cases.into_iter().enumerate() {
        let fluree = FlureeBuilder::memory().build_memory();
        let src = format!("it/push-retract-src-{i}:main");
        let tgt = format!("it/push-retract-tgt-{i}:main");

        let ledger = fluree.create_ledger(&src).await.expect("create source");
        let mut ledger = ledger;
        for body in [&first, &second] {
            ledger = fluree
                .stage_owned(ledger)
                .upsert_turtle(&format!("{PREFIXES}{body}\n"))
                .execute()
                .await
                .expect("source commit")
                .ledger;
        }
        let handle = fluree.ledger_cached(&src).await.expect("source handle");
        let export = fluree
            .export_commit_range(
                &handle,
                &ExportCommitsRequest {
                    cursor: None,
                    cursor_id: None,
                    limit: Some(10),
                    ..Default::default()
                },
            )
            .await
            .expect("export");
        // Newest first; pushed oldest first.
        let mut commits = export.commits;
        commits.reverse();
        assert_eq!(commits.len(), 2, "{what}");

        fluree.create_ledger(&tgt).await.expect("create target");
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let mut pushed = Vec::new();
        for (n, commit) in commits.into_iter().enumerate() {
            if n == 1 {
                // The first commit's value is in the target's index only.
                support::rebuild_and_publish_index(&fluree, &tgt).await;
                fluree.disconnect_ledger(&tgt).await;
            }
            let request = PushCommitsRequest {
                commits: vec![commit],
                blobs: export.blobs.clone(),
                missing_blobs: Vec::new(),
                merged_commits: Vec::new(),
            };
            pushed.push(
                fluree
                    .push_commits(&tgt, request, &GovernanceOptions::default(), &config)
                    .await
                    .map(|r| r.accepted)
                    .map_err(|e| e.to_string()),
            );
        }
        if pushed != vec![Ok(1), Ok(1)] {
            failures.push(format!("{what}: pushes {pushed:?}"));
            continue;
        }

        let target = fluree.ledger(&tgt).await.expect("load target");
        let rows = support::query_sparql(
            &fluree,
            &target,
            &format!("PREFIX ex: <http://example.org/>\n{query}"),
        )
        .await
        .expect("query")
        .to_sparql_json(&target.snapshot)
        .expect("json");
        let values: Vec<String> = rows["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["v"]["value"].as_str().expect("value").to_string())
            .collect();
        if values != vec![expect.to_string()] {
            failures.push(format!("{what}: the target holds {values:?}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
