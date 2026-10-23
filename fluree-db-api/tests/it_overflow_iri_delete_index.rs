//! Deleting facts whose object is an overflow-namespace IRI, then indexing.
//!
//! Once the namespace table is full, a new IRI is stored whole under the
//! overflow code. A retraction is read from the stored fact, so it must carry
//! the object as stored: under the EMPTY code it names no stored fact, and
//! indexing it leaves the fact in place and gives the IRI a second, empty
//! subject that reads by IRI then find.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState};
use fluree_vocab::namespaces;
use serde_json::json;

const LABEL: &str = "http://example.org/label";
const VENDOR: &str = "http://example.org/vendor";
const PAGE: &str = "http://example.org/page";
const PRICE: &str = "http://example.org/price";
const COUNTRY: &str = "http://example.org/country";
const V: &str = "http://vendors.example/v/1";
const V2: &str = "http://vendors.example/v/2";
const O1: &str = "http://offers.example/o/1";
const O2: &str = "http://offers.example/o/2";
const O3: &str = "http://offers.example/o/3";
const O4: &str = "http://offers.example/o/4";
const O5: &str = "http://offers.example/o/5";

async fn rows(fluree: &Fluree, ledger: &LedgerState, q: &str) -> usize {
    support::query_sparql(fluree, ledger, q)
        .await
        .expect(q)
        .batches
        .iter()
        .map(fluree_db_api::Batch::len)
        .sum()
}

async fn sparql_update(fluree: &Fluree, ledger: LedgerState, body: &str) -> LedgerState {
    let parsed = fluree_db_sparql::parse_sparql(body);
    assert!(!parsed.has_errors(), "{body}: {:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("ast"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("SPARQL update")
        .ledger
}

/// Each offer names the vendor and its own page, both overflow IRIs. O1 goes
/// by `DELETE WHERE`, whose objects come from the WHERE solutions; O2 by
/// `DELETE DATA`, whose objects come from the request; O3 by a JSON-LD
/// where/delete. O4 stays. O5's vendor is replaced by an upsert, which
/// retracts the current value it reads.
#[tokio::test]
async fn deletes_of_overflow_iri_objects_read_the_same_once_indexed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree");
    let id = "it/overflow-iri-delete:main";
    let ledger = fluree.create_ledger(id).await.expect("create");

    // Fill the namespace table so the vendor, offers and pages below are
    // stored under the overflow code.
    let mut graph: Vec<_> = (0..namespaces::OVERFLOW as usize)
        .map(|i| json!({"@id": format!("http://ns{i}.example/s"), LABEL: "filler"}))
        .collect();
    graph.push(json!({"@id": V, LABEL: "vendor one", COUNTRY: {"@id": "http://example.org/US"}}));
    for o in [O1, O2, O3, O4, O5] {
        graph.push(json!({"@id": o, VENDOR: {"@id": V}, PAGE: {"@id": format!("{o}/")}, PRICE: 1}));
    }
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed");
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("reload");

    let ledger = sparql_update(&fluree, ledger, &format!("DELETE WHERE {{ <{O1}> ?p ?o }}")).await;
    let ledger = sparql_update(
        &fluree,
        ledger,
        &format!(
            "DELETE DATA {{ <{O2}> <{VENDOR}> <{V}> . <{O2}> <{PAGE}> <{O2}/> . <{O2}> <{PRICE}> 1 }}"
        ),
    )
    .await;
    let ledger = fluree
        .update(
            ledger,
            &json!({"where": {"@id": O3, "?p": "?o"}, "delete": {"@id": O3, "?p": "?o"}}),
        )
        .await
        .expect("JSON-LD delete")
        .ledger;
    let ledger = fluree
        .upsert(ledger, &json!({"@id": O5, VENDOR: {"@id": V2}}))
        .await
        .expect("upsert")
        .ledger;

    // Pending: reads that bind no overflow IRI. Binding one is #1842.
    let joins = [
        (format!("SELECT ?s WHERE {{ ?s <{VENDOR}> ?v }}"), 2),
        (format!("SELECT ?s WHERE {{ ?s <{PAGE}> ?pg }}"), 2),
        (
            format!("SELECT ?s ?l WHERE {{ ?s <{VENDOR}> ?v . ?v <{LABEL}> ?l }}"),
            1,
        ),
    ];
    let mut failures = Vec::new();
    for (q, want) in &joins {
        let got = rows(&fluree, &ledger, q).await;
        if got != *want {
            failures.push(format!("pending: {q}: {got} rows, want {want}"));
        }
    }
    drop(ledger);

    let mut checks: Vec<(String, usize)> = joins.to_vec();
    for o in [O1, O2, O3] {
        checks.push((format!("SELECT ?p ?o WHERE {{ <{o}> ?p ?o }}"), 0));
    }
    checks.push((format!("SELECT ?s WHERE {{ ?s <{VENDOR}> <{V}> }}"), 1));
    checks.push((format!("SELECT ?s WHERE {{ ?s <{VENDOR}> <{V2}> }}"), 1));
    checks.push((format!("SELECT ?v WHERE {{ <{O5}> <{VENDOR}> ?v }}"), 1));
    checks.push((format!("SELECT ?p ?x WHERE {{ <{V}> ?p ?x }}"), 2));
    support::build_incrementally_and_publish_index(&fluree, id).await;
    let incremental = fluree.ledger(id).await.expect("reload");
    for (q, want) in &checks {
        let got = rows(&fluree, &incremental, q).await;
        if got != *want {
            failures.push(format!("incremental: {q}: {got} rows, want {want}"));
        }
    }
    drop(incremental);
    support::rebuild_and_publish_index(&fluree, id).await;
    let rebuilt = fluree.ledger(id).await.expect("reload");
    for (q, want) in &checks {
        let got = rows(&fluree, &rebuilt, q).await;
        if got != *want {
            failures.push(format!("full rebuild: {q}: {got} rows, want {want}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
