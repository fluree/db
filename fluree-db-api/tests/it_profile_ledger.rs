//! Column profiling over a ledger: the native face of the stats kernel.

use crate::support::MemoryFluree;
use fluree_db_api::profile::ProfileRequest;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

const EX: &str = "http://example.org/ns/";

/// Four divisions of receipt lines. Three price around 500, one around
/// 50; every part keeps one price except `ex:p9`, which carries a wild
/// line; `ex:orphan` has a price but no division.
async fn seed(fluree: &MemoryFluree) -> String {
    let ledger_id = "test/profile:main";
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");
    let mut graph = Vec::new();
    let mut n = 0;
    for (div, base) in [
        ("rome", 500.0),
        ("hollywood", 520.0),
        ("dorchester", 480.0),
        ("montreal", 50.0),
    ] {
        for part in 0..10 {
            for _ in 0..3 {
                n += 1;
                graph.push(json!({
                    "@id": format!("ex:line{n}"),
                    "@type": "ex:ReceiptLine",
                    "ex:division": div,
                    "ex:part": {"@id": format!("ex:p{part}")},
                    "ex:price": base + f64::from(part),
                }));
            }
        }
    }
    graph.push(json!({
        "@id": "ex:wild",
        "@type": "ex:ReceiptLine",
        "ex:division": "rome",
        "ex:part": {"@id": "ex:p9"},
        "ex:price": 55_809.0,
    }));
    graph.push(json!({
        "@id": "ex:orphan",
        "@type": "ex:ReceiptLine",
        "ex:part": {"@id": "ex:p0"},
        "ex:price": 1.0,
    }));
    let txn = json!({"@context": {"ex": EX}, "@graph": graph});
    fluree.insert(ledger, &txn).await.expect("seed");
    ledger_id.to_string()
}

#[tokio::test]
async fn flat_profile_reports_counts_kinds_and_quantiles() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree).await;
    let req = ProfileRequest::columns([
        format!("{EX}price"),
        format!("{EX}part"),
        format!("{EX}nope"),
    ]);
    let report = fluree.profile_ledger(&ledger, &req).await.expect("profile");

    assert_eq!(report.t, Some(1));
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].name, format!("{EX}nope"));
    assert_eq!(report.columns.len(), 2);

    let price = &report.columns[0].summary;
    assert_eq!(price.count, 122);
    assert_eq!(price.null_count, 0);
    let num = price.numeric.as_ref().expect("numeric");
    assert_eq!(num.max, 55_809.0);
    assert_eq!(num.min, 1.0);
    let p50 = num.p50.expect("median");
    assert!((400.0..=530.0).contains(&p50), "median {p50}");

    let part = &report.columns[1].summary;
    assert_eq!(part.distinct, 10);
    assert!(part.distinct_is_exact);
    assert!(part
        .top_values
        .iter()
        .any(|v| v.value == format!("{EX}p9") && v.count == 13));
}

#[tokio::test]
async fn grouped_profile_gives_per_division_baselines() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree).await;
    let req = ProfileRequest::columns([format!("{EX}price")]).group_by([format!("{EX}division")]);
    let report = fluree.profile_ledger(&ledger, &req).await.expect("profile");

    let col = &report.columns[0];
    let grouped = col.grouped.as_ref().expect("grouped");
    assert_eq!(grouped.group_count, 4);
    assert_eq!(grouped.ungrouped, 1, "the orphan has no division");
    assert_eq!(grouped.total.count, 122);

    let montreal = grouped.groups.iter().find(|g| g.key == "montreal").unwrap();
    let rome = grouped.groups.iter().find(|g| g.key == "rome").unwrap();
    let m50 = montreal.summary.numeric.as_ref().unwrap().p50.unwrap();
    let r50 = rome.summary.numeric.as_ref().unwrap().p50.unwrap();
    assert!(m50 < 60.0 && r50 > 490.0, "montreal {m50} rome {r50}");
    assert_eq!(rome.summary.numeric.as_ref().unwrap().max, 55_809.0);
}

#[tokio::test]
async fn grouping_by_two_properties_and_constancy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree).await;
    let req = ProfileRequest::columns([format!("{EX}price")])
        .group_by([format!("{EX}part"), format!("{EX}division")]);
    let report = fluree.profile_ledger(&ledger, &req).await.expect("profile");
    let grouped = report.columns[0].grouped.as_ref().unwrap();
    assert_eq!(grouped.group_count, 40);
    // Every (part, division) is constant except Rome's p9.
    let constant = grouped
        .groups
        .iter()
        .filter(|g| g.summary.is_constant)
        .count();
    assert_eq!(constant, 39);
    let varies = grouped
        .groups
        .iter()
        .find(|g| !g.summary.is_constant)
        .unwrap();
    assert_eq!(varies.key, format!("{EX}p9 | rome"));
}

#[tokio::test]
async fn unknown_group_property_is_an_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree).await;
    let req = ProfileRequest::columns([format!("{EX}price")]).group_by([format!("{EX}missing")]);
    let err = fluree.profile_ledger(&ledger, &req).await.unwrap_err();
    assert!(err.to_string().contains("missing"), "{err}");
}
