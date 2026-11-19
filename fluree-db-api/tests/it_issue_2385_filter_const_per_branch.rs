//! #2385: on an indexed ledger, a `FILTER(?x = <iri>)` in a later UNION or
//! MINUS branch compared against an earlier branch's IRI.
//!
//! A correlated UNION builds each branch's operator tree per input row and
//! drops it before building the next, and each tree's FILTER owns its own copy
//! of the expression. The query-wide memo for `?var = <const>` was keyed by the
//! constant operand's address, so a later branch whose copy landed where an
//! earlier branch's had been read back that branch's subject id. The fast path
//! only runs when the bound side is an indexed subject, so novelty answered
//! right throughout; the oracle here is the same query over novelty.

use crate::support::{
    genesis_ledger, instances_sharing_storage, normalize_rows, query_sparql,
    rebuild_and_publish_index, MemoryFluree, MemoryLedger,
};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
enum Storage {
    NoveltyOnly,
    Indexed,
}

/// `ex:w1` is a widget that is part of `ex:box1` and of `ex:Widget`, so a
/// branch filtering on one of those IRIs and a branch filtering on the other
/// match different rows of the same subject.
fn data() -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:w1",
        "@type": "ex:Widget",
        "ex:partOf": [{"@id": "ex:box1"}, {"@id": "ex:Widget"}]
    })
}

/// A ledger holding [`data`], loaded by a second instance from novelty only or
/// from a published index.
async fn load(ledger_id: &str, storage: Storage) -> (MemoryFluree, MemoryLedger) {
    let (writer, reader) = instances_sharing_storage();
    let ledger = genesis_ledger(&writer, ledger_id);
    writer.insert(ledger, &data()).await.expect("seed");
    if let Storage::Indexed = storage {
        rebuild_and_publish_index(&writer, ledger_id).await;
    }
    let ledger = reader.ledger(ledger_id).await.expect("reader load");
    (reader, ledger)
}

async fn rows(fluree: &MemoryFluree, ledger: &MemoryLedger, query: &str) -> Vec<String> {
    let rows = query_sparql(fluree, ledger, query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format");
    let mut rows: Vec<String> = normalize_rows(&rows)
        .iter()
        .map(ToString::to_string)
        .collect();
    rows.sort();
    rows
}

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

/// Each query, with the rows it must answer over novelty and over an index.
fn cases() -> Vec<(&'static str, String, Vec<Value>)> {
    let first_and_third = vec![json!([1, "ex:Widget"]), json!([3, "ex:box1"])];
    vec![
        (
            "an empty middle branch",
            format!(
                "{PREFIX}SELECT ?b ?x WHERE {{
                   {{ ex:w1 a ?x . FILTER(?x = ex:Widget) BIND(1 AS ?b) }}
                   UNION {{ VALUES ?x {{ }} }}
                   UNION {{ ex:w1 ex:partOf ?x . FILTER(?x = ex:box1) BIND(3 AS ?b) }}
                 }}"
            ),
            first_and_third.clone(),
        ),
        (
            "a middle branch that matches nothing",
            format!(
                "{PREFIX}SELECT ?b ?x WHERE {{
                   {{ ex:w1 a ?x . FILTER(?x = ex:Widget) BIND(1 AS ?b) }}
                   UNION {{ ex:w1 ex:missing ?x }}
                   UNION {{ ex:w1 ex:partOf ?x . FILTER(?x = ex:box1) BIND(3 AS ?b) }}
                 }}"
            ),
            first_and_third,
        ),
        (
            "a filter inside a later branch's MINUS",
            format!(
                "{PREFIX}SELECT ?b ?x WHERE {{
                   {{ ex:w1 a ?x . FILTER(?x = ex:Widget) BIND(1 AS ?b) }}
                   UNION {{ VALUES ?x {{ }} }}
                   UNION {{
                     ex:w1 ex:partOf ?x .
                     MINUS {{ ex:w1 ex:partOf ?x . FILTER(?x = ex:box1) }}
                     BIND(3 AS ?b)
                   }}
                 }}"
            ),
            vec![json!([1, "ex:Widget"]), json!([3, "ex:Widget"])],
        ),
    ]
}

/// Each branch's FILTER compares against its own IRI, over an index as over
/// novelty. Run several times: whether a dropped branch's address is reused
/// depends on what the allocator did in between.
#[tokio::test]
async fn issue_2385_each_union_branch_filters_on_its_own_iri() {
    let mut wrong = Vec::new();
    for storage in [Storage::NoveltyOnly, Storage::Indexed] {
        let ledger_id = format!("issue2385-{storage:?}:main").to_lowercase();
        let (fluree, ledger) = load(&ledger_id, storage).await;
        for (case, query, expected) in cases() {
            let mut expected: Vec<String> = expected.iter().map(ToString::to_string).collect();
            expected.sort();
            for run in 0..5 {
                let got = rows(&fluree, &ledger, &query).await;
                if got != expected {
                    wrong.push(format!(
                        "{case}, {storage:?}, run {run}: got {got:?}, want {expected:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
