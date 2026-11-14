//! Directory-only aggregates over leaflets whose routing keys history widened.
//!
//! A leaflet's routing keys span the history it holds, so a retraction at a
//! leaflet boundary can widen a key to a group the leaflet holds no row of.
//! Distinct counts, MIN/MAX and the range semijoin's prune must go by the
//! groups of the rows themselves. Each case puts a retraction at the leaflet
//! boundaries (two live rows per group, two rows per leaflet) and checks the
//! fast path, and the general pipeline on the same index, against an unindexed
//! ledger: after a full build and after an incremental one, walking the
//! leaves serially and in parallel chunks. Routing is pinned with the lanes'
//! stamps, so a case cannot pass on the general pipeline.
//!
//! Own test binary: toggles the process-global kill switch and the parallel
//! walk threshold, and captures spans.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig,
    LedgerManagerConfig, QueryInput, TxnOpts,
};
use serde_json::{json, Value};
use support::span_capture::{init_test_tracing, SpanStore};
use support::{
    genesis_ledger_for_fluree, normalize_rows, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// Commits never start an index build on their own: each case triggers the
/// builds it means to test.
const NO_BACKGROUND: IndexConfig = IndexConfig {
    reindex_min_bytes: 1_000_000_000,
    reindex_max_bytes: 1_000_000_000,
};

const PARALLEL_ENV: &str = "FLUREE_PARALLEL_DIR_WALK_MIN_LEAVES";

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
        std::env::remove_var(PARALLEL_ENV);
    }
}

#[derive(Clone, Copy, Debug)]
enum Build {
    /// One build, after the retraction: a full rebuild from commits.
    Full,
    /// A build before the retraction and an incremental one after it.
    Incremental,
}

enum Query {
    Sparql(String),
    JsonLd(Value),
}

/// A query, and the fast-path site that must answer it.
struct Probe {
    label: &'static str,
    site: &'static str,
    query: Query,
}

/// Sites whose answer is a distinct-group walk over leaf chunks.
const WALK_SITES: [&str; 4] = [
    "distinct subject COUNT",
    "distinct object COUNT",
    "COUNT(DISTINCT)",
    "SUM(STRLEN(GROUP_CONCAT))",
];

impl Probe {
    fn sparql(label: &'static str, site: &'static str, body: &str) -> Self {
        Self {
            label,
            site,
            query: Query::Sparql(format!("{PREFIX}{body}")),
        }
    }

    fn jsonld(label: &'static str, site: &'static str, query: Value) -> Self {
        Self {
            label,
            site,
            query: Query::JsonLd(query),
        }
    }

    fn walks(&self) -> bool {
        WALK_SITES.contains(&self.site)
    }
}

fn ctx() -> Value {
    json!({ "ex": "http://example.org/ns/" })
}

async fn rows(fluree: &Fluree, db: &GraphDb, query: &Query) -> Vec<Value> {
    let result = match query {
        Query::Sparql(q) => fluree.query(db, QueryInput::Sparql(q)).await,
        Query::JsonLd(q) => fluree.query(db, QueryInput::JsonLd(q)).await,
    }
    .expect("query");
    normalize_rows(&result.to_jsonld(&db.snapshot).expect("to_jsonld"))
}

fn outcomes(store: &SpanStore, site: &str) -> Vec<String> {
    store
        .find_events("fast-path outcome")
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

/// Runs every probe over `seed` with `retraction` applied, on an unindexed
/// ledger and then on indexed ones.
async fn check(seed: Value, retraction: Value, probes: &[Probe]) {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    let expected = unindexed_rows(&seed, &retraction, probes).await;
    for build in [Build::Full, Build::Incremental] {
        check_build(build, &seed, &retraction, probes, &expected).await;
    }
}

/// Each probe's rows on an unindexed ledger holding `seed` with `retraction`
/// applied: the general pipeline over novelty, which reads no index metadata.
async fn unindexed_rows(seed: &Value, retraction: &Value, probes: &[Probe]) -> Vec<Vec<Value>> {
    let mem = FlureeBuilder::memory().build_memory();
    let id = "it/live-edges-unindexed:main";
    let ledger = mem
        .insert_with_opts(
            genesis_ledger_for_fluree(&mem, id).await,
            seed,
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("seed")
        .ledger;
    let ledger = mem
        .update_with_opts(
            ledger,
            retraction,
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("retraction")
        .ledger;
    let view = mem.db_at_t(id, ledger.t()).await.expect("view");
    let mut expected = Vec::with_capacity(probes.len());
    for probe in probes {
        expected.push(rows(&mem, &view, &probe.query).await);
    }
    expected
}

async fn check_build(
    build: Build,
    seed: &Value,
    retraction: &Value,
    probes: &[Probe],
    expected: &[Vec<Value>],
) {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let id = "it/live-edges:main";
    let mut config = fluree_db_indexer::IndexerConfig::small();
    config.leaflet_rows = 2;
    // Three leaflets a leaf, so every order spans many leaves and a parallel
    // walk splits them into chunks.
    config.leaf_rows = 6;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has a writable nameservice"),
        config,
    );
    local
        .run_until(async move {
            let ledger = fluree
                .insert_with_opts(
                    genesis_ledger_for_fluree(&fluree, id).await,
                    seed,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &NO_BACKGROUND,
                )
                .await
                .expect("seed")
                .ledger;
            if matches!(build, Build::Incremental) {
                trigger_index_and_wait_outcome(&handle, id, ledger.t()).await;
            }
            let ledger = fluree.ledger(id).await.expect("reload");
            let ledger = fluree
                .update_with_opts(
                    ledger,
                    retraction,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &NO_BACKGROUND,
                )
                .await
                .expect("retraction")
                .ledger;
            trigger_index_and_wait_outcome(&handle, id, ledger.t()).await;
            let view = fluree.db_at_t(id, ledger.t()).await.expect("view");
            check_view(
                &fluree,
                &view,
                &format!("{build:?} build"),
                probes,
                expected,
            )
            .await;
        })
        .await;
}

/// Checks each probe on an indexed `view`, walking its leaves serially and
/// then in parallel chunks: the lane answers (its stamp), a parallel walk
/// really splits the leaves, and the fast path and the general pipeline on
/// the same index both give `expected`.
async fn check_view(
    fluree: &Fluree,
    view: &GraphDb,
    context: &str,
    probes: &[Probe],
    expected: &[Vec<Value>],
) {
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    for parallel in [false, true] {
        if parallel {
            std::env::set_var(PARALLEL_ENV, "1");
        } else {
            std::env::remove_var(PARALLEL_ENV);
        }
        for (probe, want) in probes.iter().zip(expected) {
            let label = format!(
                "{} ({context}, {} walk)",
                probe.label,
                if parallel { "parallel" } else { "serial" }
            );

            let (store, guard) = init_test_tracing();
            let fast = rows(fluree, view, &probe.query).await;
            let seen = outcomes(&store, probe.site);
            let chunks: Vec<usize> = store
                .find_events("fast-path: parallel leaf-chunk reduce")
                .iter()
                .filter_map(|e| e.fields.get("chunks")?.parse().ok())
                .collect();
            drop(guard);
            assert!(
                seen.iter().any(|o| o == "proceed"),
                "{label}: `{}` must answer; stamps {seen:?}",
                probe.site
            );
            if parallel && probe.walks() && cores > 1 {
                assert!(
                    chunks.iter().any(|&n| n > 1),
                    "{label}: the walk must split its leaves into chunks; saw {chunks:?}"
                );
            }

            set_fast_paths_disabled(true);
            let generic = rows(fluree, view, &probe.query).await;
            set_fast_paths_disabled(false);

            assert_eq!(
                &generic, want,
                "{label}: the general pipeline on the index differs from the unindexed ledger"
            );
            assert_eq!(
                &fast, want,
                "{label}: the fast path differs from the unindexed ledger"
            );
        }
    }
}

/// `n` subjects, each with every value in `values` on `ex:v`.
fn subjects_with(n: usize, values: &[i64]) -> Value {
    let nodes: Vec<Value> = (0..n)
        .map(|i| json!({ "@id": format!("ex:s{i:03}"), "ex:v": values }))
        .collect();
    json!({ "@context": ctx(), "@graph": nodes })
}

fn retract_v(value: i64) -> Value {
    json!({
        "@context": ctx(),
        "where": [{ "@id": "?s", "ex:v": value }],
        "delete": [{ "@id": "?s", "ex:v": value }]
    })
}

fn distinct_subjects() -> [Probe; 2] {
    [
        Probe::sparql(
            "distinct subjects",
            "distinct subject COUNT",
            "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o }",
        ),
        Probe::jsonld(
            "distinct subjects (JSON-LD)",
            "distinct subject COUNT",
            json!({
                "@context": ctx(),
                "select": ["(as (count-distinct ?s) ?n)"],
                "where": [{ "@id": "?s", "?p": "?o" }]
            }),
        ),
    ]
}

/// Each subject's last fact in sort order is retracted, and each leaflet ends
/// on a subject's last row: the retraction lands in the next leaflet, whose
/// first routing key then names a subject it holds no row of.
#[tokio::test]
async fn distinct_subjects_past_a_retraction_at_each_boundary() {
    check(
        subjects_with(40, &[1, 2, 9]),
        retract_v(9),
        &distinct_subjects(),
    )
    .await;
}

/// The other side of the boundary rule: rows 1 and 2 end one leaflet, the
/// retracted 3 widens the next one's first key, and 5 continues the subject
/// there. The subject spans the boundary and must be counted once.
#[tokio::test]
async fn a_subject_spanning_a_widened_boundary_is_counted_once() {
    check(
        subjects_with(40, &[1, 2, 3, 5]),
        retract_v(3),
        &distinct_subjects(),
    )
    .await;
}

/// 40 integer objects of `ex:p`, each held by `a` and `b`; `z`'s fact is
/// retracted, and sorts after both in OPST and in POST.
fn objects_seed() -> Value {
    let mut nodes = Vec::new();
    for k in 0..40i64 {
        for suffix in ["a", "b", "z"] {
            nodes.push(json!({ "@id": format!("ex:o{k:03}{suffix}"), "ex:p": 1000 + k }));
        }
    }
    json!({ "@context": ctx(), "@graph": nodes })
}

fn objects_retraction() -> Value {
    let facts: Vec<Value> = (0..40i64)
        .map(|k| json!({ "@id": format!("ex:o{k:03}z"), "ex:p": 1000 + k }))
        .collect();
    json!({ "@context": ctx(), "delete": facts })
}

#[tokio::test]
async fn distinct_objects_past_a_retraction_at_each_boundary() {
    check(
        objects_seed(),
        objects_retraction(),
        &[
            Probe::sparql(
                "distinct objects",
                "distinct object COUNT",
                "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }",
            ),
            Probe::jsonld(
                "distinct objects (JSON-LD)",
                "distinct object COUNT",
                json!({
                    "@context": ctx(),
                    "select": ["(as (count-distinct ?o) ?n)"],
                    "where": [{ "@id": "?s", "?p": "?o" }]
                }),
            ),
            Probe::sparql(
                "distinct objects of one predicate",
                "COUNT(DISTINCT)",
                "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ex:p ?o }",
            ),
            Probe::jsonld(
                "distinct objects of one predicate (JSON-LD)",
                "COUNT(DISTINCT)",
                json!({
                    "@context": ctx(),
                    "select": ["(as (count-distinct ?o) ?n)"],
                    "where": [{ "@id": "?s", "ex:p": "?o" }]
                }),
            ),
        ],
    )
    .await;
}

/// The per-predicate subject count (PSOT) behind `SUM(STRLEN(GROUP_CONCAT))`:
/// each subject concatenates `a b`, so a subject counted short adds a
/// separator.
#[tokio::test]
async fn sum_strlen_group_concat_counts_each_subject_once() {
    let nodes: Vec<Value> = (0..40)
        .map(|i| json!({ "@id": format!("ex:s{i:03}"), "ex:w": ["a", "b", "z"] }))
        .collect();
    check(
        json!({ "@context": ctx(), "@graph": nodes }),
        json!({
            "@context": ctx(),
            "where": [{ "@id": "?s", "ex:w": "z" }],
            "delete": [{ "@id": "?s", "ex:w": "z" }]
        }),
        &[Probe::sparql(
            "SUM(STRLEN(GROUP_CONCAT))",
            "SUM(STRLEN(GROUP_CONCAT))",
            "SELECT (SUM(STRLEN(?cat)) AS ?sum) {
               { SELECT (GROUP_CONCAT(?o; SEPARATOR=\" \") AS ?cat) { ?s ex:w ?o } GROUP BY ?s }
             }",
        )],
    )
    .await;
}

/// `ex:n` holds 0 to 9, plus 1000 and -5, which are retracted: history below
/// the predicate's first row and above its last widens the end leaflets'
/// routing keys to them.
#[tokio::test]
async fn min_and_max_skip_retracted_extremes() {
    let mut nodes: Vec<Value> = (0..10i64)
        .map(|k| json!({ "@id": format!("ex:i{k}"), "ex:n": k }))
        .collect();
    nodes.push(json!({ "@id": "ex:hi", "ex:n": 1000 }));
    nodes.push(json!({ "@id": "ex:lo", "ex:n": -5 }));
    check(
        json!({ "@context": ctx(), "@graph": nodes }),
        json!({
            "@context": ctx(),
            "delete": [{ "@id": "ex:hi", "ex:n": 1000 }, { "@id": "ex:lo", "ex:n": -5 }]
        }),
        &[
            Probe::sparql(
                "MIN",
                "MIN/MAX string",
                "SELECT (MIN(?n) AS ?m) WHERE { ?s ex:n ?n }",
            ),
            Probe::sparql(
                "MAX",
                "MIN/MAX string",
                "SELECT (MAX(?n) AS ?m) WHERE { ?s ex:n ?n }",
            ),
            Probe::jsonld(
                "MIN (JSON-LD)",
                "MIN/MAX string",
                json!({
                    "@context": ctx(),
                    "select": ["(as (min ?n) ?m)"],
                    "where": [{ "@id": "?s", "ex:n": "?n" }]
                }),
            ),
            Probe::jsonld(
                "MAX (JSON-LD)",
                "MIN/MAX string",
                json!({
                    "@context": ctx(),
                    "select": ["(as (max ?n) ?m)"],
                    "where": [{ "@id": "?s", "ex:n": "?n" }]
                }),
            ),
        ],
    )
    .await;
}

/// A retracted string on an integer predicate sorts after every integer in
/// POST and widens the last leaflet's routing key into the string type. The
/// range semijoin's prune must not compare that key's string id with integer
/// bounds: the leaflet holds the values in range. (The fold needs a
/// dedup-safe query and at least 256 driving rows.)
#[tokio::test]
async fn range_semijoin_keeps_a_leaflet_a_retracted_string_widened() {
    let mut nodes: Vec<Value> = (0..300i64)
        .map(|k| json!({ "@id": format!("ex:item{k:03}"), "@type": "ex:Item", "ex:n": k }))
        .collect();
    nodes.push(json!({ "@id": "ex:origin", "ex:n": 298 }));
    nodes.push(json!({ "@id": "ex:junk", "ex:n": "zzz" }));
    check(
        json!({ "@context": ctx(), "@graph": nodes }),
        json!({ "@context": ctx(), "delete": [{ "@id": "ex:junk", "ex:n": "zzz" }] }),
        &[Probe::sparql(
            "range semijoin",
            "range-semijoin",
            "SELECT DISTINCT ?p WHERE {
               ex:origin ex:n ?orig .
               ?p a ex:Item .
               ?p ex:n ?sim .
               FILTER (?sim < ?orig + 3 && ?sim > ?orig - 3)
             }",
        )],
    )
    .await;
}

/// One subject's rows fill every leaf, so every leaflet, leaf and chunk
/// boundary cuts through it, some of them widened by its retracted values:
/// a parallel walk must join it across each chunk seam.
#[tokio::test]
async fn a_subject_spanning_every_leaf_is_counted_once() {
    let values: Vec<i64> = (1..=60).collect();
    let retracted: Vec<Value> = (1..=60)
        .filter(|v| v % 5 == 0)
        .map(|v| json!({ "@id": "ex:big", "ex:v": v }))
        .collect();
    check(
        json!({ "@context": ctx(), "@graph": [{ "@id": "ex:big", "ex:v": values }] }),
        json!({ "@context": ctx(), "delete": retracted }),
        &distinct_subjects(),
    )
    .await;
}

/// One object held by every subject of the predicate, so every POST and
/// OPST boundary cuts through its group, some of them widened by retracted
/// holders.
#[tokio::test]
async fn an_object_spanning_every_leaf_is_counted_once() {
    let nodes: Vec<Value> = (0..60)
        .map(|i| json!({ "@id": format!("ex:m{i:02}"), "ex:q": 7 }))
        .collect();
    let retracted: Vec<Value> = (0..60)
        .filter(|i| i % 7 == 0)
        .map(|i| json!({ "@id": format!("ex:m{i:02}"), "ex:q": 7 }))
        .collect();
    check(
        json!({ "@context": ctx(), "@graph": nodes }),
        json!({ "@context": ctx(), "delete": retracted }),
        &[
            Probe::sparql(
                "distinct objects",
                "distinct object COUNT",
                "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }",
            ),
            Probe::sparql(
                "distinct objects of one predicate",
                "COUNT(DISTINCT)",
                "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ex:q ?o }",
            ),
        ],
    )
    .await;
}

// ── v4 indexes ─────────────────────────────────────────────────────────

const V4_FIXTURE: &str = "tests/fixtures/leaflet-edges-4.2.6/storage";

/// The seed and retraction the v4 fixture's `edges` ledgers hold (see its
/// README): retractions at leaflet boundaries on every shape the walks and
/// MIN/MAX read.
fn v4_seed_and_retraction() -> (Value, Value) {
    let mut nodes = Vec::new();
    let mut retracted = Vec::new();
    for i in 0..8i64 {
        nodes.push(json!({ "@id": format!("ex:s{i:02}"), "ex:v": [1, 2, 9] }));
        retracted.push(json!({ "@id": format!("ex:s{i:02}"), "ex:v": 9 }));
        nodes.push(json!({ "@id": format!("ex:x{i:02}"), "ex:u": [1, 2, 3, 5] }));
        retracted.push(json!({ "@id": format!("ex:x{i:02}"), "ex:u": 3 }));
        nodes.push(json!({ "@id": format!("ex:t{i:02}"), "ex:w": ["a", "b", "z"] }));
        retracted.push(json!({ "@id": format!("ex:t{i:02}"), "ex:w": "z" }));
        for suffix in ["a", "b", "z"] {
            nodes.push(json!({ "@id": format!("ex:o{i:02}{suffix}"), "ex:p": 1000 + i }));
        }
        retracted.push(json!({ "@id": format!("ex:o{i:02}z"), "ex:p": 1000 + i }));
        nodes.push(json!({ "@id": format!("ex:i{i:02}"), "ex:n": i }));
    }
    nodes.push(json!({ "@id": "ex:hi", "ex:n": 1000 }));
    nodes.push(json!({ "@id": "ex:lo", "ex:n": -5 }));
    retracted.push(json!({ "@id": "ex:hi", "ex:n": 1000 }));
    retracted.push(json!({ "@id": "ex:lo", "ex:n": -5 }));
    (
        json!({ "@context": ctx(), "@graph": nodes }),
        json!({ "@context": ctx(), "delete": retracted }),
    )
}

fn v4_probes() -> Vec<Probe> {
    let [subjects, subjects_jsonld] = distinct_subjects();
    vec![
        subjects,
        subjects_jsonld,
        Probe::sparql(
            "distinct objects",
            "distinct object COUNT",
            "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }",
        ),
        Probe::sparql(
            "distinct objects of one predicate",
            "COUNT(DISTINCT)",
            "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ex:p ?o }",
        ),
        Probe::sparql(
            "SUM(STRLEN(GROUP_CONCAT))",
            "SUM(STRLEN(GROUP_CONCAT))",
            "SELECT (SUM(STRLEN(?cat)) AS ?sum) {
               { SELECT (GROUP_CONCAT(?o; SEPARATOR=\" \") AS ?cat) { ?s ex:w ?o } GROUP BY ?s }
             }",
        ),
        Probe::sparql(
            "MIN",
            "MIN/MAX string",
            "SELECT (MIN(?n) AS ?m) WHERE { ?s ex:n ?n }",
        ),
        Probe::sparql(
            "MAX",
            "MIN/MAX string",
            "SELECT (MAX(?n) AS ?m) WHERE { ?s ex:n ?n }",
        ),
    ]
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// A writable copy of the v4 fixture: opening it upgrades its nameservice.
fn v4_store() -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().unwrap();
    copy_dir(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(V4_FIXTURE),
        dir.path(),
    );
    dir
}

/// Whether `ledger_id`'s index root is of `main`'s format.
async fn serves_a_v4_index(fluree: &Fluree, ledger_id: &str) -> bool {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    let root_id = record.index_head_id.clone().expect("indexed");
    let bytes = fluree
        .content_store(&record.storage_namespace())
        .get(&root_id)
        .await
        .unwrap();
    !fluree_db_binary_index::format::index_root::is_current_format(&bytes)
}

/// A v4 index stores no live groups: where its leaflets hold history, the
/// walks read a boundary's groups from the leaflets' lead columns, and MIN
/// and MAX the predicate's end leaflets'.
#[tokio::test]
async fn a_v4_index_is_judged_by_its_rows_groups() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    let (seed, retraction) = v4_seed_and_retraction();
    let probes = v4_probes();
    let expected = unindexed_rows(&seed, &retraction, &probes).await;

    let dir = v4_store();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file-backed Fluree");
    for id in ["v4/edges-full:main", "v4/edges-incr:main"] {
        assert!(
            serves_a_v4_index(&fluree, id).await,
            "{id}: the fixture's index is v4"
        );
        let view = fluree.db(id).await.expect("view");
        check_view(&fluree, &view, id, &probes, &expected).await;
    }
}

/// Erasing a fact a v4 index holds only in its history rewrites no v4 leaf
/// in place, which would leave a root mixing two formats: it rebuilds the
/// index.
#[tokio::test]
async fn erasing_a_retracted_fact_from_a_v4_index_rebuilds_it() {
    use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
    use fluree_db_api::retention::FactSelector;

    let dir = v4_store();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure(ErasureConfig {
            prover: std::sync::Arc::new(fluree_db_erasure::testing::NativeProver::default()),
            verifier: std::sync::Arc::new(Verifier::accepting_dev_receipts()),
            retry_ms: 10,
        })
        .build()
        .expect("file-backed Fluree");
    let id = "v4/erase:main";
    assert!(serves_a_v4_index(&fluree, id).await);

    let request = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!("alice@secret.example")),
        }],
        ..Default::default()
    };
    fluree
        .erase_occurrences(id, request, Some("did:example:officer".into()))
        .await
        .expect("erase");
    let mut published = 0;
    for _ in 0..4 {
        let report = fluree.run_erasure_pass(id).await.expect("erasure pass");
        published += report.published;
        if published > 0 {
            break;
        }
    }
    assert!(published > 0, "the erasure publishes");
    // The purge erases from the index: here by rebuilding it.
    let mut rebuilt = 0;
    for _ in 0..4 {
        let report = fluree
            .run_purge_pass(id, std::time::Duration::ZERO)
            .await
            .expect("purge pass");
        assert_eq!(report.leafwise, 0, "a v4 index is not erased leaf by leaf");
        rebuilt += report.rebuilt;
        if rebuilt > 0 {
            break;
        }
    }
    assert_eq!(rebuilt, 1, "the purge rebuilds the v4 index");
    assert!(
        !serves_a_v4_index(&fluree, id).await,
        "the rebuilt index is v5"
    );

    let emails = |subject: &str| {
        json!({
            "@context": { "ex": "http://example.org/" },
            "select": "?o",
            "where": { "@id": subject, "ex:email": "?o" }
        })
    };
    let view = fluree.db(id).await.expect("view");
    assert!(rows(&fluree, &view, &Query::JsonLd(emails("ex:alice")))
        .await
        .is_empty());
    assert_eq!(
        rows(&fluree, &view, &Query::JsonLd(emails("ex:bob"))).await,
        vec![json!("bob@kept.example")]
    );
}
