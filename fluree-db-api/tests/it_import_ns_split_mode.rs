//! Regression test: bulk import must persist the namespace split mode it actually
//! used so concrete-IRI lookups resolve.
//!
//! When the bulk-import namespace preflight sees more than `NS_PREFLIGHT_BUDGET`
//! (255) distinct prefixes in the streamed input, it coarsens the dict allocator
//! to `NsSplitMode::HostPlusN`. The index root, predicate sids, and genesis commit
//! must record THAT mode; otherwise the read path splits query IRIs `MostGranular`
//! against a coarse-keyed dictionary and every bound-IRI lookup returns nothing.

#![cfg(feature = "native")]

mod support;

use std::io::Write;

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};

const LEDGER: &str = "test/ns-split:main";
// A class IRI with 3 path segments: its MostGranular split (`.../schema/classes/`)
// differs from HostPlusN(1) (`.../schema/`), which is exactly the case that breaks
// when the recorded split mode disagrees with the dictionary.
const CLASS_IRI: &str = "http://ex.com/schema/classes/Item";
const SUBJECT_COUNT: usize = 14_000;
const DISTINCT_NS: usize = 300; // > 255 budget, so the preflight flips the allocator

/// Write `SUBJECT_COUNT` `rdf:type` triples spread across `DISTINCT_NS` subject
/// namespaces. At ~110 bytes/line this exceeds 1 MB, so `chunk_size_mb(1)` routes
/// it through the streaming reader where the preflight runs.
fn write_dataset(path: &std::path::Path) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    for j in 0..SUBJECT_COUNT {
        writeln!(
            f,
            "<http://ex.com/ns{}/s{j}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{CLASS_IRI}> .",
            j % DISTINCT_NS
        )
        .unwrap();
    }
}

/// Load the ledger fresh and return the row count for `COUNT(*)`-style `sparql`.
async fn count(fluree: &Fluree, sparql: &str) -> u64 {
    let ledger = fluree.ledger(LEDGER).await.expect("load ledger");
    let result = support::query_sparql(fluree, &ledger, sparql)
        .await
        .expect("query");
    let json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    json["results"]["bindings"][0]["c"]["value"]
        .as_str()
        .expect("count value")
        .parse()
        .expect("count is a number")
}

#[tokio::test]
async fn flipped_split_mode_persisted_for_bound_iri_lookups() {
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let nt_path = data_dir.path().join("data.nt");
    write_dataset(&nt_path);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let result = fluree
        .create(LEDGER)
        .import(&nt_path)
        .threads(1)
        .memory_budget_mb(128)
        .chunk_size_mb(1) // force the streaming path (file > 1 MB) so the preflight runs
        .execute()
        .await
        .expect("import");
    assert!(result.root_id.is_some(), "index should have been built");

    let query = format!("SELECT (COUNT(*) AS ?c) WHERE {{ ?s a <{CLASS_IRI}> }}");

    // Fresh-from-index read path (uses root.ns_split_mode).
    assert_eq!(
        count(&fluree, &query).await,
        SUBJECT_COUNT as u64,
        "bound-IRI lookup after import returned nothing — root recorded the wrong split mode"
    );

    // Reindex from the commit chain (uses the genesis commit's split mode).
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    // Read with a fresh instance so the reindexed root is loaded, not a cached one.
    let fluree2 = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build second Fluree");
    assert_eq!(
        count(&fluree2, &query).await,
        SUBJECT_COUNT as u64,
        "bound-IRI lookup after reindex returned nothing — genesis commit recorded the wrong split mode"
    );
}
