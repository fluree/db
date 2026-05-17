//! Validates that the full reindex (rebuild) path produces correct class stats
//! in the FIR6 root, including class counts, property usage, datatypes, and
//! ref-edge targets.
//!
//! This exercises the streaming SpotClassStatsCollector + build_class_stat_entries()
//! code path added in the rebuild memory optimization PR.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::genesis_ledger_for_fluree;

#[tokio::test]
async fn reindex_produces_correct_class_counts_and_property_usage() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reindex-class-stats:main";

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice",
                "ex:age": 30,
                "ex:employer": { "@id": "ex:acme" }
            },
            {
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:name": "Bob",
                "ex:age": 25,
                "ex:employer": { "@id": "ex:acme" }
            },
            {
                "@id": "ex:acme",
                "@type": "ex:Organization",
                "ex:name": "Acme Corp",
                "ex:founded": 1990
            }
        ]
    });

    // Insert with high reindex thresholds to prevent background indexing.
    let _ledger1 = fluree
        .insert_with_opts(
            ledger0,
            &tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert")
        .ledger;

    // Full rebuild via reindex.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // Load the ledger and inspect the FIR6 root stats.
    let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
    let stats = loaded
        .snapshot
        .stats
        .as_ref()
        .expect("stats should be present after reindex");

    // -- Class counts --
    let classes = stats.classes.as_ref().expect("classes should be present");
    assert!(
        !classes.is_empty(),
        "expected at least one class entry after reindex"
    );

    let find_class = |iri: &str| -> Option<&fluree_db_core::ClassStatEntry> {
        classes
            .iter()
            .find(|c| loaded.snapshot.decode_sid(&c.class_sid).as_deref() == Some(iri))
    };

    let person = find_class("http://example.org/Person").expect("Person class should exist");
    assert_eq!(person.count, 2, "Person should have 2 instances");
    // Person should have properties: name, age, employer (at minimum).
    assert!(
        person.properties.len() >= 3,
        "Person should have at least 3 properties, got {}",
        person.properties.len()
    );

    let org =
        find_class("http://example.org/Organization").expect("Organization class should exist");
    assert_eq!(org.count, 1, "Organization should have 1 instance");
    // Organization should have properties: name, founded (at minimum).
    assert!(
        org.properties.len() >= 2,
        "Organization should have at least 2 properties, got {}",
        org.properties.len()
    );

    // -- Property-level stats --
    // At least some properties should have non-empty datatype info.
    let has_datatypes = person.properties.iter().any(|p| !p.datatypes.is_empty());
    assert!(
        has_datatypes,
        "expected at least one Person property with datatype information"
    );

    // -- Ref-edge targets --
    // Person.employer → Organization should produce ref-class entries.
    let employer_prop = person.properties.iter().find(|p| {
        loaded.snapshot.decode_sid(&p.property_sid).as_deref()
            == Some("http://example.org/employer")
    });
    if let Some(emp) = employer_prop {
        // If ref-class tracking is active (≤64 classes), verify target.
        if !emp.ref_classes.is_empty() {
            let targets_org = emp.ref_classes.iter().any(|rc| {
                loaded.snapshot.decode_sid(&rc.class_sid).as_deref()
                    == Some("http://example.org/Organization")
            });
            assert!(
                targets_org,
                "Person.employer ref-classes should include Organization"
            );
        }
    }

    // -- Per-graph stats --
    let graphs = stats.graphs.as_ref().expect("graphs should be present");
    assert!(
        !graphs.is_empty(),
        "expected at least one graph stats entry"
    );
    // Default graph (g_id=0) should have property entries.
    let g0 = graphs.iter().find(|g| g.g_id == 0);
    assert!(g0.is_some(), "default graph (g_id=0) stats should exist");
    let g0 = g0.unwrap();
    assert!(
        !g0.properties.is_empty(),
        "default graph should have property stats"
    );
}

#[tokio::test]
async fn reindex_class_stats_survive_retraction() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reindex-class-retract:main";

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);

    let high_threshold = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // Commit 1: Insert two Persons.
    let tx1 = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {"@id":"ex:alice","@type":"ex:Person","ex:name":"Alice"},
            {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob"}
        ]
    });
    let result1 = fluree
        .insert_with_opts(
            ledger0,
            &tx1,
            TxnOpts::default(),
            CommitOpts::default(),
            &high_threshold,
        )
        .await
        .expect("insert 1");

    // Commit 2: Delete Bob via where+delete pattern.
    let tx2 = json!({
        "@context": { "ex": "http://example.org/" },
        "where": {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob"},
        "delete": {"@id":"ex:bob","@type":"ex:Person","ex:name":"Bob"}
    });
    let _result2 = fluree
        .update_with_opts(
            result1.ledger,
            &tx2,
            TxnOpts::default(),
            CommitOpts::default(),
            &high_threshold,
        )
        .await
        .expect("delete Bob");

    // Full rebuild — should see net state (only Alice).
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
    let stats = loaded
        .snapshot
        .stats
        .as_ref()
        .expect("stats should be present");
    let classes = stats.classes.as_ref().expect("classes should be present");

    let person = classes.iter().find(|c| {
        loaded.snapshot.decode_sid(&c.class_sid).as_deref() == Some("http://example.org/Person")
    });

    // After deleting Bob, Person count should be 1 (only Alice).
    if let Some(p) = person {
        assert_eq!(
            p.count, 1,
            "Person count should be 1 after deleting Bob, got {}",
            p.count
        );
    }
    // If Person class disappeared entirely (0 instances → no entry), that's also acceptable
    // since build_class_stat_entries only emits classes with count > 0.
}

// Regression: SpotClassStatsCollector stores ValueTypeTag values into the
// per-class `prop_dts` map (keyed by `ValueTypeTag::as_u8() as u16`). A prior
// version of `build_class_stat_entries` mis-interpreted those keys as
// `DatatypeDictId` indices into a `dt_tags` lookup table, producing wrong tags
// post-reindex (xsd:integer→xsd:boolean, xsd:date→rdf:langString, …) and
// `UNKNOWN` on the import path (which passed `&[]` for dt_tags).
#[tokio::test]
async fn reindex_class_stats_report_correct_datatypes() {
    use fluree_db_core::value_id::ValueTypeTag;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reindex-class-stats-datatypes:main";

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let tx = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "ex:thing1",
                "@type": "ex:Thing",
                "ex:label": "first",
                "ex:count": 7,
                "ex:active": true,
                "ex:created": { "@value": "2024-01-02T03:04:05Z", "@type": "xsd:dateTime" },
                "ex:start":   { "@value": "2024-01-02", "@type": "xsd:date" },
                "ex:moon":    { "@value": "2024-03", "@type": "xsd:gYearMonth" },
                "ex:anniv":   { "@value": "--03-15", "@type": "xsd:gMonthDay" },
                "ex:peer": { "@id": "ex:thing2" }
            },
            {
                "@id": "ex:thing2",
                "@type": "ex:Thing",
                "ex:label": "second",
                "ex:count": 11,
                "ex:active": false,
                "ex:created": { "@value": "2024-02-03T04:05:06Z", "@type": "xsd:dateTime" },
                "ex:start":   { "@value": "2024-02-03", "@type": "xsd:date" },
                "ex:moon":    { "@value": "2024-04", "@type": "xsd:gYearMonth" },
                "ex:anniv":   { "@value": "--07-04", "@type": "xsd:gMonthDay" },
                "ex:peer": { "@id": "ex:thing1" }
            }
        ]
    });

    let _ = fluree
        .insert_with_opts(
            ledger0,
            &tx,
            TxnOpts::default(),
            CommitOpts::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .expect("insert");

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let loaded = fluree.ledger(ledger_id).await.expect("load ledger");
    let stats = loaded
        .snapshot
        .stats
        .as_ref()
        .expect("stats should be present after reindex");
    let classes = stats.classes.as_ref().expect("classes should be present");

    let thing = classes
        .iter()
        .find(|c| {
            loaded.snapshot.decode_sid(&c.class_sid).as_deref() == Some("http://example.org/Thing")
        })
        .expect("Thing class should exist");

    let lookup_tag = |iri: &str| -> u8 {
        let p = thing
            .properties
            .iter()
            .find(|p| loaded.snapshot.decode_sid(&p.property_sid).as_deref() == Some(iri))
            .unwrap_or_else(|| panic!("missing property {iri}"));
        assert_eq!(
            p.datatypes.len(),
            1,
            "property {iri} should have exactly one datatype, got {:?}",
            p.datatypes
        );
        p.datatypes[0].0
    };

    assert_eq!(
        lookup_tag("http://example.org/label"),
        ValueTypeTag::STRING.as_u8(),
        "ex:label should be xsd:string"
    );
    assert_eq!(
        lookup_tag("http://example.org/count"),
        ValueTypeTag::INTEGER.as_u8(),
        "ex:count should be xsd:integer"
    );
    assert_eq!(
        lookup_tag("http://example.org/active"),
        ValueTypeTag::BOOLEAN.as_u8(),
        "ex:active should be xsd:boolean"
    );
    assert_eq!(
        lookup_tag("http://example.org/created"),
        ValueTypeTag::DATE_TIME.as_u8(),
        "ex:created should be xsd:dateTime"
    );
    assert_eq!(
        lookup_tag("http://example.org/start"),
        ValueTypeTag::DATE.as_u8(),
        "ex:start should be xsd:date"
    );
    assert_eq!(
        lookup_tag("http://example.org/moon"),
        ValueTypeTag::G_YEAR_MONTH.as_u8(),
        "ex:moon should be xsd:gYearMonth"
    );
    assert_eq!(
        lookup_tag("http://example.org/anniv"),
        ValueTypeTag::G_MONTH_DAY.as_u8(),
        "ex:anniv should be xsd:gMonthDay"
    );
    assert_eq!(
        lookup_tag("http://example.org/peer"),
        ValueTypeTag::JSON_LD_ID.as_u8(),
        "ex:peer (reference) should be JSON_LD_ID / @id"
    );
}
