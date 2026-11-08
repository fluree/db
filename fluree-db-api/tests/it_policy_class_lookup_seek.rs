//! A class lookup reads where its subjects are, and each row at most once.
//!
//! Policy filtering resolves the classes of the subjects it judges with one
//! batched `rdf:type` read. Subjects far apart in id must not cost every row
//! between them (under a class policy a nested-loop join asks once per row,
//! for one subject), and subjects near each other must not cost a cursor each
//! nor share a leaflet across cursors.

use crate::support::genesis_ledger;
use crate::support::span_capture::init_test_tracing;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, ReindexOptions, TxnOpts};
use fluree_db_core::Sid;
use serde_json::json;
use std::collections::HashMap;

const EX: &str = "http://example.org/";
const SUBJECTS: usize = 2_000;

const NO_BACKGROUND: IndexConfig = IndexConfig {
    reindex_min_bytes: 1_000_000_000,
    reindex_max_bytes: 1_000_000_000,
};

/// `SUBJECTS` subjects, each of one of five classes, indexed in four-row
/// leaflets so `rdf:type` spans hundreds of them.
async fn typed_ledger(fluree: &Fluree, id: &str) {
    let ledger = genesis_ledger(fluree, id).await;
    let graph: Vec<_> = (0..SUBJECTS)
        .map(|i| json!({"@id": format!("ex:s{i:04}"), "@type": format!("ex:T{}", i % 5)}))
        .collect();
    fluree
        .insert_with_opts(
            ledger,
            &json!({"@context": {"ex": EX}, "@graph": graph}),
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("insert");
    let opts = ReindexOptions::default().with_indexer_config(
        fluree_db_indexer::IndexerConfig::default()
            .with_leaflet_rows(4)
            .with_leaf_rows(16),
    );
    fluree.reindex(id, opts).await.expect("reindex");
}

fn sid(db: &fluree_db_api::GraphDb, name: &str) -> Sid {
    db.snapshot
        .encode_iri(&format!("{EX}{name}"))
        .unwrap_or_else(|| panic!("{name} encodes"))
}

/// Each subject's classes by local name, sorted.
fn named(
    db: &fluree_db_api::GraphDb,
    classes: HashMap<Sid, Vec<Sid>>,
) -> Vec<(String, Vec<String>)> {
    let name = |s: &Sid| {
        db.snapshot
            .decode_sid(s)
            .expect("decodes")
            .trim_start_matches(EX)
            .to_string()
    };
    let mut out: Vec<(String, Vec<String>)> = classes
        .iter()
        .map(|(s, cs)| {
            let mut cs: Vec<String> = cs.iter().map(name).collect();
            cs.sort();
            (name(s), cs)
        })
        .collect();
    out.sort();
    out
}

struct Lookup {
    classes: Vec<(String, Vec<String>)>,
    cursors: usize,
    rows_read: usize,
}

async fn lookup(id: &str, indices: &[usize]) -> Lookup {
    let fluree = FlureeBuilder::memory().build_memory();
    typed_ledger(&fluree, id).await;
    let db = fluree.db(id).await.expect("db");
    let subjects: Vec<Sid> = indices
        .iter()
        .map(|i| sid(&db, &format!("s{i:04}")))
        .collect();

    let (store, guard) = init_test_tracing();
    let classes = fluree_db_policy::lookup_subject_classes(&subjects, db.as_graph_db_ref())
        .await
        .expect("lookup");
    let events = store.find_events("batched refs lookup");
    drop(guard);

    assert_eq!(events.len(), 1, "{events:?}");
    let field = |name: &str| -> usize {
        events[0].fields[name]
            .parse()
            .unwrap_or_else(|_| panic!("{name}: {:?}", events[0].fields))
    };
    Lookup {
        classes: named(&db, classes),
        cursors: field("cursors"),
        rows_read: field("rows_read"),
    }
}

fn expected(indices: &[usize]) -> Vec<(String, Vec<String>)> {
    indices
        .iter()
        .map(|i| (format!("s{i:04}"), vec![format!("T{}", i % 5)]))
        .collect()
}

/// Every hundredth subject, leaves apart: each is a point read of its own
/// row, not a scan of its leaflet.
#[tokio::test]
async fn lone_subjects_are_point_reads() {
    let indices: Vec<usize> = (0..SUBJECTS).step_by(100).collect();
    let got = lookup("class-lookup/lone:main", &indices).await;
    assert_eq!(got.classes, expected(&indices));
    assert_eq!(got.cursors, indices.len());
    assert_eq!(got.rows_read, indices.len());
}

/// Two subjects a few leaflets apart, within a leaf or two: one cursor,
/// skipping the leaflets between them.
#[tokio::test]
async fn neighbours_share_a_cursor_and_skip_between() {
    let indices = [0, 12];
    let got = lookup("class-lookup/neighbours:main", &indices).await;
    assert_eq!(got.classes, expected(&indices));
    assert_eq!(got.cursors, 1);
    assert!(got.rows_read <= 8, "read {} rows", got.rows_read);
}

/// Every subject: one cursor, each row read once.
#[tokio::test]
async fn a_dense_set_reads_each_row_once() {
    let indices: Vec<usize> = (0..SUBJECTS).collect();
    let got = lookup("class-lookup/dense:main", &indices).await;
    assert_eq!(got.classes, expected(&indices));
    assert_eq!(got.cursors, 1);
    assert_eq!(got.rows_read, SUBJECTS);
}

/// Novelty after the index: one subject gains a class, one loses its only
/// class, and a new subject gets one. Each is its own cursor, merging only
/// its own novelty; the new subject's lies past the index's last leaf.
#[tokio::test]
async fn sought_subjects_see_their_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "class-lookup/seek-novelty:main";
    typed_ledger(&fluree, id).await;
    let last = format!("s{:04}", SUBJECTS - 1);
    let ledger = fluree.ledger(id).await.expect("ledger");
    fluree
        .update_with_opts(
            ledger,
            &json!({
                "@context": {"ex": EX},
                "delete": {"@id": "ex:s0000", "@type": "ex:T0"},
                "insert": [
                    {"@id": format!("ex:{last}"), "@type": "ex:Extra"},
                    {"@id": "ex:fresh", "@type": "ex:New"}
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("update");
    let db = fluree.db(id).await.expect("db");
    let subjects = [sid(&db, "s0000"), sid(&db, &last), sid(&db, "fresh")];
    let classes = fluree_db_policy::lookup_subject_classes(&subjects, db.as_graph_db_ref())
        .await
        .expect("lookup");
    assert_eq!(
        named(&db, classes),
        vec![
            ("fresh".to_string(), vec!["New".to_string()]),
            (
                last,
                vec!["Extra".to_string(), format!("T{}", (SUBJECTS - 1) % 5)]
            ),
        ]
    );
}
