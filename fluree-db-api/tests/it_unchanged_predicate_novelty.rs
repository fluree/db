//! Unrelated novelty must not disable persisted single-predicate aggregates.
//! Targeted writes and historical snapshots must still use the merged input.
#![cfg(feature = "native")]
#[path = "support/span_capture.rs"]
mod span_capture;
use fluree_db_api::{set_fast_paths_disabled, Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use fluree_db_core::{Flake, GraphId, IndexType, OverlayProvider};
use serde_json::{json, Value};
use std::sync::Arc;

const LEDGER: &str = "it/unchanged-predicate:main";
const EX: &str = "http://example.org/";
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

/// An overlay which can supply facts but has no cheap range-count metadata.
struct UnknownExtent(Arc<dyn OverlayProvider>);
impl OverlayProvider for UnknownExtent {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn epoch(&self) -> u64 {
        self.0.epoch()
    }
    fn for_each_overlay_flake(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.0
            .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, callback);
    }
}

async fn check(fluree: &Fluree, view: &GraphDb, changed: bool, metadata: bool) {
    let cases = [
        ("MIN/MAX string", "SELECT (MIN(?o) AS ?v) { ?s ex:n ?o }", json!([[if changed {-5} else {1}]])),
        ("MIN/MAX string", "SELECT (MAX(?o) AS ?v) { ?s ex:n ?o }", json!([[9]])),
        ("MIN/MAX string", "SELECT (MIN(?o) AS ?v) { ?s ex:title ?o }", json!([[if changed {"a"} else {"alpha"}]])),
        ("MIN/MAX string", "SELECT (MAX(?o) AS ?v) { ?s ex:title ?o }", json!([["zulu"]])),
        ("SUM(STRLEN)", "SELECT (SUM(STRLEN(?o)) AS ?v) { ?s ex:title ?o }", json!([[if changed {9} else {13}]])),
        ("COUNT(CONTAINS)", "SELECT (COUNT(*) AS ?v) { ?s ex:title ?o FILTER CONTAINS(?o, \"ph\") }", json!([[i32::from(!changed)]])),
        ("COUNT(REGEX)", "SELECT (COUNT(*) AS ?v) { ?s ex:title ?o FILTER REGEX(?o, \"a$\") }", json!([[2]])),
        ("SUM(STRLEN(GROUP_CONCAT))", "SELECT (SUM(STRLEN(?cat)) AS ?v) { { SELECT (GROUP_CONCAT(?o; SEPARATOR=\" \") AS ?cat) { ?s ex:title ?o } GROUP BY ?s } }", json!([[if changed {9} else {14}]])),
    ];
    for (site, body, expected) in cases {
        let query = format!("PREFIX ex: <{EX}> {body}");
        let (spans, guard) = span_capture::init_test_tracing();
        let fast: Value = fluree
            .query(view, query.as_str())
            .await
            .unwrap()
            .to_jsonld(&view.snapshot)
            .unwrap();
        let events = spans.find_events("fast-path outcome");
        assert_eq!(
            events
                .iter()
                .any(|e| e.fields.get("site").map(String::as_str) == Some(site)
                    && e.fields.get("outcome").map(String::as_str) == Some("proceed")),
            metadata,
            "t={}, {query}: {events:?}",
            view.t
        );
        drop(guard);
        set_fast_paths_disabled(true);
        let generic = fluree
            .query(view, query.as_str())
            .await
            .unwrap()
            .to_jsonld(&view.snapshot)
            .unwrap();
        set_fast_paths_disabled(false);
        assert_eq!(fast, generic, "pipeline: {query}");
        assert_eq!(fast, expected, "{query}");
    }
}

#[tokio::test]
async fn single_predicate_aggregates_ignore_only_proven_unrelated_novelty() {
    let _reset = Reset;
    let db = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("data.ttl"), format!(
        "@prefix ex: <{EX}> . ex:a ex:n 1 ; ex:title \"alpha\", \"zulu\" . ex:b ex:n 9 ; ex:title \"beta\" ."
    )).unwrap();
    let fluree = FlureeBuilder::file(db.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    fluree
        .create(LEDGER)
        .import(src.path())
        .threads(2)
        .memory_budget_mb(256)
        .leaflet_rows(2)
        .execute()
        .await
        .unwrap();
    let base = fluree.db(LEDGER).await.unwrap();
    check(&fluree, &base, false, true).await;
    let handle = fluree.ledger_cached(LEDGER).await.unwrap();
    fluree
        .stage(&handle)
        .update(&json!({"@context":{"ex":EX},
            "insert":[{"@id":"ex:a","ex:unrelated":"pending"}]
        }))
        .execute()
        .await
        .unwrap();
    let unrelated = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    check(&fluree, &unrelated, false, true).await;
    let mut unknown = unrelated.clone();
    unknown.overlay = Arc::new(UnknownExtent(Arc::clone(&unrelated.overlay)));
    check(&fluree, &unknown, false, false).await;
    fluree
        .stage(&handle)
        .update(&json!({"@context":{"ex":EX},
            "delete":[{"@id":"ex:a","ex:n":1,"ex:title":"alpha"}],
            "insert":[{"@id":"ex:new","ex:n":-5,"ex:title":"a"}]
        }))
        .execute()
        .await
        .unwrap();
    let changed = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    check(&fluree, &changed, true, false).await;
    check(&fluree, &unrelated, false, true).await;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .unwrap();
    fluree.disconnect_ledger(LEDGER).await;
    let historical = fluree.db_at_t(LEDGER, base.t).await.unwrap();
    check(&fluree, &historical, false, false).await;
}
