//! The S2 spatial query planned in the geospatial docs (`idx:spatial` with
//! `idx:property`, `idx:geometry` and `idx:result`), run against a polygon
//! the ledger holds: indexed, and in novelty only.
//!
//! Ignored until the query exists. No query surface parses `idx:spatial`
//! yet, so today the query reads as ordinary triple patterns and returns
//! `[]` in every case while the control still reads the polygon back. The
//! spatial-query issue tracks the work (a parser, provider routing, novelty
//! geometries and the full rebuild). Un-ignore this when it lands.
//!
//! File-backed with `without_indexing()`, so every index build is the one
//! the test asks for.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use serde_json::{json, Value};

const PFX: &str = "PREFIX ex: <http://example.org/>\n";
const WKT: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";
const PARCEL: &str = "POLYGON((2.0 48.0, 2.5 48.0, 2.5 48.5, 2.0 48.5, 2.0 48.0))";
const REGION: &str = "POLYGON((1.0 47.0, 4.0 47.0, 4.0 50.0, 1.0 50.0, 1.0 47.0))";

fn open(path: &str) -> Fluree {
    FlureeBuilder::file(path.to_string())
        .without_indexing()
        .build()
        .expect("open")
}

async fn write(fluree: &Fluree, id: &str, body: &str) {
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PFX}{body}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("{body}: {e}"));
}

/// The docs' query shape for one operation, over `ex:boundary`.
async fn spatial(fluree: &Fluree, id: &str, op: &str) -> Result<Value, String> {
    let query = json!({
        "@context": {
            "ex": "http://example.org/",
            "idx": "https://ns.flur.ee/index#"
        },
        "where": [{
            "idx:spatial": op,
            "idx:property": "ex:boundary",
            "idx:geometry": REGION,
            "idx:result": "?place"
        }],
        "select": ["?place"]
    });
    fluree
        .graph(id)
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await
        .map_err(|e| e.to_string())
}

/// The polygon itself, read back as a plain triple: the control.
async fn boundary(fluree: &Fluree, id: &str) -> Value {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": [{"@id": "ex:parcel", "ex:boundary": "?g"}],
        "select": ["?g"]
    });
    fluree
        .graph(id)
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("control query")
}

async fn answers(path: &str, id: &str) -> Vec<String> {
    let fluree = open(path);
    let mut out = vec![format!("control boundary: {}", boundary(&fluree, id).await)];
    for op in ["within", "intersects"] {
        let got = match spatial(&fluree, id, op).await {
            Ok(rows) => rows.to_string(),
            Err(e) => format!("error: {e}"),
        };
        out.push(format!("{op}: {got}"));
    }
    out
}

/// A polygon inside the query region is found by `within` and
/// `intersects`, whether the index or novelty holds it.
#[tokio::test]
#[ignore = "no query surface parses idx:spatial yet; see the S2 spatial-query issue"]
async fn a_spatial_query_finds_a_polygon_in_its_region() {
    let mut failures = Vec::new();
    for indexed in [true, false] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let id = "it/spatial-query:main";
        {
            let fluree = open(&path);
            fluree.create_ledger(id).await.expect("create");
            write(&fluree, id, "INSERT DATA { ex:filler ex:k \"keep\" }").await;
            fluree
                .reindex(id, ReindexOptions::default())
                .await
                .expect("base index");
            write(
                &fluree,
                id,
                &format!("INSERT DATA {{ ex:parcel ex:boundary \"{PARCEL}\"^^<{WKT}> }}"),
            )
            .await;
            if indexed {
                support::build_incrementally_and_publish_index(&fluree, id).await;
            }
            fluree.disconnect_ledger(id).await;
        }
        let got = answers(&path, id).await;
        let label = if indexed { "indexed" } else { "novelty" };
        if !got[0].contains("POLYGON") {
            failures.push(format!("{label}: the control lost the polygon: {got:?}"));
            continue;
        }
        for line in &got[1..] {
            if !line.contains("ex:parcel") {
                failures.push(format!("{label}: {line}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
