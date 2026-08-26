//! Regression pin for issue #1391: fast-stats counted every novelty
//! assertion, including the ones that only restate a fact the base index
//! already holds.
//!
//! `assemble_fast_stats` folds novelty onto the indexed counts as a `+1`/`-1`
//! delta log. Novelty's own set-semantics dedup (`NoveltyFactState`) is
//! window-scoped, so once a fact has been reindexed into the base and dropped
//! from the window, asserting it again is kept as a novelty flake — and
//! charged a second `+1` on top of the base count it duplicates. Every count
//! rendered from those stats then over-reports until the next reindex: the
//! `apoc.meta.data` schema rows LangChain-style tooling reads, and the
//! class/property counts `fluree info` prints.
//!
//! What this file pins, on an indexed ledger whose novelty holds a mix of
//! duplicate re-asserts and genuinely new facts:
//!
//! * every `apoc.meta.data` count equals what a scan of the same
//!   `(class, property)` returns — the stats-served answer and the
//!   pipeline-served answer agree;
//! * `fluree info`'s class and property counts equal the same scan;
//! * the answers are identical with fast paths on and with the
//!   `FLUREE_DISABLE_QUERY_FAST_PATHS` kill switch engaged, which pins the
//!   issue's read-path claim (query answers were never affected — set
//!   semantics make the duplicate idempotent at read time) and would catch a
//!   future COUNT lane that started trusting the drifted stats;
//! * the merge actually reconciled — the `fast-stats novelty merge outcome`
//!   stamp reports `reconciled` with a non-zero duplicate count, so none of
//!   the above can pass by quietly declining to the estimate lane.
//!
//! Own test binary: toggles the process-global kill switch AND asserts
//! routing via span capture, so it must not share a process with other tests.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig,
    TxnOpts,
};
use serde_json::{json, Value};

const ALIAS: &str = "r1391/dup:main";

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// Base ledger, reindexed so these facts live in the persisted index and
/// leave the novelty window.
fn base_doc() -> Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:w1", "@type": "ex:Widget", "ex:name": "w1", "ex:size": 1},
            {"@id": "ex:w2", "@type": "ex:Widget", "ex:name": "w2"},
            {"@id": "ex:g1", "@type": "ex:Gadget", "ex:name": "g1",
             "ex:partOf": {"@id": "ex:w1"}}
        ]
    })
}

/// The second commit, applied AFTER the index boundary so it sits in novelty
/// while the base already holds most of it. Deliberately mixed:
///
/// * `ex:w1` is restated verbatim — three pure duplicates (type, name, size);
/// * `ex:w2` restates its type and name and adds one genuinely new name;
/// * `ex:w3` is entirely new;
/// * `ex:g1` restates its type and one `partOf` edge and adds a new one.
///
/// Each subject restates its `@type` so novelty-side class attribution can
/// see it — a novel property on a subject whose only type fact is already
/// indexed is a separate, documented staleness gap in these shims, not this
/// bug, and mixing the two would muddy the pin.
fn novelty_doc() -> Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:w1", "@type": "ex:Widget", "ex:name": "w1", "ex:size": 1},
            {"@id": "ex:w2", "@type": "ex:Widget", "ex:name": ["w2", "w2-alt"]},
            {"@id": "ex:w3", "@type": "ex:Widget", "ex:name": "w3"},
            {"@id": "ex:g1", "@type": "ex:Gadget",
             "ex:partOf": [{"@id": "ex:w1"}, {"@id": "ex:w3"}]}
        ]
    })
}

/// `(label, property)` pairs the schema shims report, each paired with the
/// SPARQL that counts the same facts by scanning. The hand-pinned number is
/// the current-state truth; both the stats answer and the scan must equal it.
const CASES: &[(&str, &str, &str, u64)] = &[
    (
        "ex:Widget",
        "ex:name",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s a ex:Widget . ?s ex:name ?o }",
        // w1:"w1", w2:"w2", w2:"w2-alt", w3:"w3"
        4,
    ),
    (
        "ex:Widget",
        "ex:size",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s a ex:Widget . ?s ex:size ?o }",
        1,
    ),
    (
        "ex:Gadget",
        "ex:name",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s a ex:Gadget . ?s ex:name ?o }",
        1,
    ),
    (
        "ex:Gadget",
        "ex:partOf",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s a ex:Gadget . ?s ex:partOf ?o }",
        // g1 -> w1 (already indexed), g1 -> w3 (new)
        2,
    ),
];

/// Ledger-wide counts `fluree info` renders, with their scan equivalents.
const LEDGER_CASES: &[(&str, &str, u64)] = &[
    (
        "ex:name",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s ex:name ?o }",
        5,
    ),
    (
        "ex:partOf",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s ex:partOf ?o }",
        2,
    ),
];

/// Class instance counts `fluree info` renders.
const CLASS_CASES: &[(&str, &str, u64)] = &[
    (
        "ex:Widget",
        "SELECT (COUNT(?s) AS ?c) WHERE { ?s a ex:Widget }",
        3,
    ),
    (
        "ex:Gadget",
        "SELECT (COUNT(?s) AS ?c) WHERE { ?s a ex:Gadget }",
        1,
    ),
];

// ---------------------------------------------------------------------------
// Setup + helpers
// ---------------------------------------------------------------------------

/// Insert the base doc, reindex it into the persisted index, then apply the
/// novelty doc — so the second commit's duplicates land in a window whose
/// dedup oracle can no longer see them.
async fn setup() -> (tempfile::TempDir, Fluree) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    // Thresholds high enough that no commit self-indexes; the explicit
    // reindex below is the only index point, and nothing re-indexes after it.
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let ledger = fluree.create_ledger(ALIAS).await.expect("create_ledger");
    let _ = fluree
        .insert_with_opts(
            ledger,
            &base_doc(),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("base insert");
    fluree
        .reindex(ALIAS, ReindexOptions::default())
        .await
        .expect("reindex");

    let ledger = fluree.ledger(ALIAS).await.expect("reload after reindex");
    assert!(
        ledger.novelty.is_empty(),
        "the reindex must drain novelty, or the duplicates below stay \
         window-visible and novelty's own dedup hides the bug"
    );
    let _ = fluree
        .insert_with_opts(
            ledger,
            &novelty_doc(),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("novelty insert");

    let ledger = fluree.ledger(ALIAS).await.expect("reload after novelty");
    assert!(
        !ledger.novelty.is_empty(),
        "the second commit must stay in novelty for this pin to mean anything"
    );
    (dir, fluree)
}

async fn scan_count(fluree: &Fluree, sparql: &str) -> u64 {
    let snapshot = fluree.graph(ALIAS).load().await.expect("load");
    let rows = snapshot
        .query()
        .sparql(&format!("{PREFIX}{sparql}"))
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("scan {sparql}: {e}"));
    rows[0][0]
        .as_u64()
        .unwrap_or_else(|| panic!("count row: {rows}"))
}

/// `apoc.meta.data` rows as `(label, property, count)`, full IRIs.
async fn apoc_counts(fluree: &Fluree) -> Vec<(String, String, u64)> {
    let snapshot = fluree.graph(ALIAS).load().await.expect("load");
    let db = snapshot.db();
    let rows = fluree
        .query_cypher(
            db,
            "CALL apoc.meta.data() YIELD label, property, count RETURN label, property, count",
        )
        .await
        .expect("apoc.meta.data")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    rows.as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            (
                r[0].as_str().expect("label").to_string(),
                r[1].as_str().expect("property").to_string(),
                r[2].as_u64().expect("count"),
            )
        })
        .collect()
}

fn iri(compact: &str) -> String {
    compact.replace("ex:", "http://example.org/ns/")
}

fn apoc_count(rows: &[(String, String, u64)], class: &str, property: &str) -> Option<u64> {
    rows.iter()
        .find(|(l, p, _)| *l == iri(class) && *p == iri(property))
        .map(|(_, _, c)| *c)
}

/// `fluree info`'s class + property counts, keyed by the IRI it renders.
async fn info_counts(fluree: &Fluree) -> (Value, Value) {
    let info = fluree
        .ledger_info(ALIAS)
        .execute()
        .await
        .expect("ledger_info");
    let stats = info
        .get("stats")
        .cloned()
        .unwrap_or_else(|| panic!("no stats block in {info}"));
    (
        stats.get("classes").cloned().unwrap_or(Value::Null),
        stats.get("properties").cloned().unwrap_or(Value::Null),
    )
}

/// Pull a count out of a `fluree info` classes/properties block, whatever
/// shape it renders (array of objects, or IRI-keyed map).
fn info_count(block: &Value, iri_or_curie: &str) -> Option<u64> {
    let wanted = iri(iri_or_curie);
    let matches = |v: &Value| -> bool {
        v.as_str()
            .map(|s| s == wanted || s == iri_or_curie)
            .unwrap_or(false)
    };
    match block {
        Value::Array(entries) => entries.iter().find_map(|e| {
            let named = ["@id", "id", "class", "property", "iri"]
                .iter()
                .any(|k| e.get(*k).map(matches).unwrap_or(false));
            named.then(|| e.get("count").and_then(Value::as_u64))?
        }),
        Value::Object(map) => map
            .iter()
            .find(|(k, _)| **k == wanted || k.as_str() == iri_or_curie)
            .and_then(|(_, v)| {
                v.as_u64()
                    .or_else(|| v.get("count").and_then(Value::as_u64))
            }),
        _ => None,
    }
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

// ---------------------------------------------------------------------------
// The pin
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn issue_1391_duplicate_novelty_asserts_do_not_inflate_stats_counts() {
    // The kill switch ORs with this env var; with it set, the fast phase
    // below runs generically and half the assertions are vacuous.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    let (_dir, fluree) = setup().await;
    let mut failures: Vec<String> = Vec::new();

    // -- Phase 1: fast paths on, under span capture ------------------------
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);

    let apoc_fast = apoc_counts(&fluree).await;
    let (classes, properties) = info_counts(&fluree).await;
    let merges: Vec<_> = store
        .find_events("fast-stats novelty merge outcome")
        .into_iter()
        .map(|e| {
            (
                e.fields.get("outcome").cloned().unwrap_or_default(),
                e.fields
                    .get("duplicates")
                    .and_then(|d| d.parse::<u64>().ok())
                    .unwrap_or(0),
            )
        })
        .collect();

    let mut scans_fast: Vec<u64> = Vec::new();
    for (_, _, sparql, _) in CASES {
        scans_fast.push(scan_count(&fluree, sparql).await);
    }
    for (_, sparql, _) in LEDGER_CASES {
        scans_fast.push(scan_count(&fluree, sparql).await);
    }
    for (_, sparql, _) in CLASS_CASES {
        scans_fast.push(scan_count(&fluree, sparql).await);
    }
    drop(tracing_guard);

    // The merge must have actually reconciled against the base index, and
    // must have found duplicates — otherwise every count below could be
    // right for the wrong reason (an assembly that quietly took the estimate
    // lane on a fixture with no duplicates in it).
    if !merges.iter().any(|(outcome, _)| outcome == "reconciled") {
        failures.push(format!(
            "no `reconciled` fast-stats merge was stamped — the user-facing \
             count surfaces declined to the estimate lane [stamps: {merges:?}]"
        ));
    }
    let duplicates: u64 = merges
        .iter()
        .filter(|(outcome, _)| outcome == "reconciled")
        .map(|(_, d)| *d)
        .max()
        .unwrap_or(0);
    if duplicates < 4 {
        failures.push(format!(
            "the reconciling merge found only {duplicates} duplicate novelty \
             facts; the fixture restates seven already-indexed ones (w1's \
             type/name/size, w2's type/name, g1's type/partOf), so this pin \
             is not exercising the bug [stamps: {merges:?}]"
        ));
    }

    // -- Phase 2: the generic pipeline (kill-switch reference) -------------
    set_fast_paths_disabled(true);
    let apoc_generic = apoc_counts(&fluree).await;
    let mut scans_generic: Vec<u64> = Vec::new();
    for (_, _, sparql, _) in CASES {
        scans_generic.push(scan_count(&fluree, sparql).await);
    }
    for (_, sparql, _) in LEDGER_CASES {
        scans_generic.push(scan_count(&fluree, sparql).await);
    }
    for (_, sparql, _) in CLASS_CASES {
        scans_generic.push(scan_count(&fluree, sparql).await);
    }
    set_fast_paths_disabled(false);

    // -- Assertions --------------------------------------------------------
    if scans_fast != scans_generic {
        failures.push(format!(
            "scan counts differ across the kill switch: fast {scans_fast:?} vs \
             generic {scans_generic:?} — a COUNT fast path is answering from \
             stats that novelty has drifted"
        ));
    }
    if apoc_fast != apoc_generic {
        failures.push(format!(
            "apoc.meta.data differs across the kill switch: {apoc_fast:?} vs \
             {apoc_generic:?}"
        ));
    }

    let mut expected_scans: Vec<u64> = CASES.iter().map(|(_, _, _, n)| *n).collect();
    expected_scans.extend(LEDGER_CASES.iter().map(|(_, _, n)| *n));
    expected_scans.extend(CLASS_CASES.iter().map(|(_, _, n)| *n));
    if scans_fast != expected_scans {
        failures.push(format!(
            "scan counts {scans_fast:?} do not match the hand-pinned truth \
             {expected_scans:?} — either the fixture changed or the read path \
             regressed"
        ));
    }

    for (class, property, _, expected) in CASES {
        match apoc_count(&apoc_fast, class, property) {
            Some(actual) if actual == *expected => {}
            Some(actual) => failures.push(format!(
                "apoc.meta.data reports {class} {property} = {actual}; a scan of \
                 the same facts returns {expected} (duplicate novelty asserts \
                 counted on top of the base index)"
            )),
            None => failures.push(format!(
                "apoc.meta.data has no row for {class} {property} [rows: {apoc_fast:?}]"
            )),
        }
    }

    for (class, _, expected) in CLASS_CASES {
        match info_count(&classes, class) {
            Some(actual) if actual == *expected => {}
            Some(actual) => failures.push(format!(
                "`fluree info` reports {class} instance count {actual}; a scan \
                 returns {expected}"
            )),
            None => failures.push(format!(
                "`fluree info` has no class entry for {class} [classes: {classes}]"
            )),
        }
    }
    for (property, _, expected) in LEDGER_CASES {
        match info_count(&properties, property) {
            Some(actual) if actual == *expected => {}
            Some(actual) => failures.push(format!(
                "`fluree info` reports {property} count {actual}; a scan returns \
                 {expected}"
            )),
            None => failures.push(format!(
                "`fluree info` has no property entry for {property} \
                 [properties: {properties}]"
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "issue #1391 regression pins found {} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
