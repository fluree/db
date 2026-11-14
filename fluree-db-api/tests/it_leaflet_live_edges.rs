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

    let mem = FlureeBuilder::memory().build_memory();
    let id = "it/live-edges-unindexed:main";
    let ledger = mem
        .insert_with_opts(
            genesis_ledger_for_fluree(&mem, id).await,
            &seed,
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
            &retraction,
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

    for build in [Build::Full, Build::Incremental] {
        check_build(build, &seed, &retraction, probes, &expected).await;
    }
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
    config.leaf_target_bytes = 2_000;
    config.leaf_max_bytes = 4_000;
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

            for parallel in [false, true] {
                if parallel {
                    std::env::set_var(PARALLEL_ENV, "1");
                } else {
                    std::env::remove_var(PARALLEL_ENV);
                }
                for (probe, want) in probes.iter().zip(expected) {
                    let label = format!(
                        "{} ({build:?} build, {} walk)",
                        probe.label,
                        if parallel { "parallel" } else { "serial" }
                    );

                    let (store, guard) = init_test_tracing();
                    let fast = rows(&fluree, &view, &probe.query).await;
                    let seen = outcomes(&store, probe.site);
                    drop(guard);
                    assert!(
                        seen.iter().any(|o| o == "proceed"),
                        "{label}: `{}` must answer; stamps {seen:?}",
                        probe.site
                    );

                    set_fast_paths_disabled(true);
                    let generic = rows(&fluree, &view, &probe.query).await;
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
        })
        .await;
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
