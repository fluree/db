//! Explicit `validate` endpoint: focus-node computation for class targets.
//!
//! Covers subclass expansion (a `sh:targetClass` shape must also select
//! instances of subclasses) and the per-class focus-node memo shared by
//! multiple shapes targeting the same class — the path in
//! `ShaclEngine::validate_all_with_membership` / `get_focus_nodes`.

#![cfg(all(feature = "native", feature = "shacl"))]

use fluree_db_api::validate::ValidateOptions;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

use crate::support::genesis_ledger;

#[tokio::test]
async fn validate_report_shares_focus_nodes_across_shapes_targeting_same_class() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/validate-report/memo:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: data only — a subclass hierarchy plus two instances, one of the
    // subclass. No shapes exist yet, so no transaction-time enforcement fires.
    let r1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex":   "http://example.org/ns/",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                },
                "@graph": [
                    { "@id": "ex:Dog", "rdfs:subClassOf": {"@id": "ex:Animal"} },
                    { "@id": "ex:rex",     "@type": "ex:Dog" },
                    { "@id": "ex:generic", "@type": "ex:Animal" }
                ]
            }),
        )
        .await
        .expect("seed data (no shapes) must succeed");
    let ledger = r1.ledger;

    // Step 2: two node shapes, BOTH targeting ex:Animal — this is what
    // exercises the per-class focus-node memo. Staging shape definitions only
    // validates the shape subjects (not the pre-existing instances), so it
    // commits cleanly.
    let r2 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex":  "http://example.org/ns/",
                    "sh":  "http://www.w3.org/ns/shacl#"
                },
                "@graph": [
                    {
                        "@id": "ex:AnimalNameShape",
                        "@type": "sh:NodeShape",
                        "sh:targetClass": {"@id": "ex:Animal"},
                        "sh:property": { "sh:path": {"@id": "ex:name"}, "sh:minCount": 1 }
                    },
                    {
                        "@id": "ex:AnimalSpeciesShape",
                        "@type": "sh:NodeShape",
                        "sh:targetClass": {"@id": "ex:Animal"},
                        "sh:property": { "sh:path": {"@id": "ex:species"}, "sh:minCount": 1 }
                    }
                ]
            }),
        )
        .await
        .expect("seed shapes must succeed (shape subjects are not Animal instances)");
    let _ledger = r2.ledger;

    // Explicit validation over the default graph against the ledger's own
    // (attached) shapes.
    let report = fluree
        .validate_ledger(ledger_id, &ValidateOptions::default())
        .await
        .expect("validate must succeed");

    assert!(
        !report.conforms,
        "instances violate both shapes: {report:?}"
    );
    assert_eq!(report.shape_count, 2, "both shapes compiled");

    // The report pins the exact snapshot it validated, so a caller measuring
    // anything alongside the results can read at the same t.
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("ledger handle");
    assert_eq!(
        report.t,
        handle.t().await,
        "report.t must match the ledger head"
    );
    assert_eq!(report.t, 2, "two commits precede validation");

    // Subclass expansion: ex:rex (an ex:Dog) must appear as a focus node even
    // though the shapes target ex:Animal. ex:generic is a direct instance.
    let focus: std::collections::HashSet<String> = report
        .results
        .iter()
        .filter_map(|r| r.focus_node.as_str().map(String::from))
        .collect();
    assert!(
        focus.contains("http://example.org/ns/rex"),
        "subclass instance ex:rex must be a focus node: {focus:?}"
    );
    assert!(
        focus.contains("http://example.org/ns/generic"),
        "direct instance ex:generic must be a focus node: {focus:?}"
    );

    // Both shapes fired on the shared class — the memo returned the same focus
    // set to each. 2 instances × 2 shapes = 4 minCount violations.
    let paths: std::collections::HashSet<String> = report
        .results
        .iter()
        .filter_map(|r| r.result_path.clone())
        .collect();
    assert!(
        paths.contains("http://example.org/ns/name"),
        "name shape must fire: {paths:?}"
    );
    assert!(
        paths.contains("http://example.org/ns/species"),
        "species shape must fire: {paths:?}"
    );
    assert_eq!(
        report.violation_count(),
        4,
        "each of 2 instances violates each of 2 shapes: {report:?}"
    );
}

#[tokio::test]
async fn validate_report_t_advances_with_ledger_head() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/validate-report/t-advances:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/", "sh": "http://www.w3.org/ns/shacl#"},
                "@id": "ex:NameShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Thing"},
                "sh:property": { "sh:path": {"@id": "ex:name"}, "sh:minCount": 1 }
            }),
        )
        .await
        .expect("seed shape");

    let first = fluree
        .validate_ledger(ledger_id, &ValidateOptions::default())
        .await
        .expect("validate must succeed");
    assert!(first.conforms, "no instances yet: {first:?}");
    assert_eq!(first.t, 1);

    fluree
        .insert(
            r1.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:thing",
                "@type": "ex:Thing",
                "ex:name": "named"
            }),
        )
        .await
        .expect("conforming instance commits");

    let second = fluree
        .validate_ledger(ledger_id, &ValidateOptions::default())
        .await
        .expect("validate must succeed");
    assert!(second.conforms, "{second:?}");
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("ledger handle");
    assert_eq!(second.t, 2);
    assert_eq!(second.t, handle.t().await);
    assert!(
        second.t > first.t,
        "report.t must move with the ledger head"
    );
}
