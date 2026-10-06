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
//! The mirror case has the same root and cost the same accuracy: novelty
//! accepts *every* retraction — `apply_commit`'s dedup gate short-circuits on
//! `flake.op &&` — so a DELETE matching nothing subtracted one from a count
//! that never included it.
//!
//! What this file pins, on an indexed ledger whose novelty holds duplicate
//! re-asserts, genuinely new facts, a real retraction, and a no-op one:
//!
//! * every `apoc.meta.data` count equals what a scan of the same
//!   `(class, property)` returns — the stats-served answer and the
//!   pipeline-served answer agree;
//! * `fluree info`'s class and property counts equal the same scan, on BOTH
//!   arms (`realtime_property_details` true and false — separate call sites);
//! * `db.propertyKeys()` drops a predicate whose every fact was retracted,
//!   which is `merged_stats`' one count-sensitive output;
//! * the answers are identical with fast paths on and with the
//!   `FLUREE_DISABLE_QUERY_FAST_PATHS` kill switch engaged, which pins the
//!   issue's read-path claim (query answers were never affected — set
//!   semantics make the duplicate idempotent at read time) and would catch a
//!   future COUNT lane that started trusting the drifted stats;
//! * each of the four reconciling surfaces stamped `reconciled` ON ITS OWN
//!   SITE with at least the duplicate count the fixture plants. A stamp label
//!   shared across entry points would let one reconciling assembly satisfy the
//!   guard for all four, so the check names the lane it means.
//!
//! Reverting all four `NoveltyMerge::Reconciled` call sites turns this into 18
//! failures; reverting any single one fails that site's assertions alone.
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
/// `ex:w4` is deliberately imported **untyped**: the second commit gives it a
/// `@type` while restating its name verbatim, which is the shape that separates
/// "is this fact in the base index?" from "did the base rollup count it under
/// this class?". See `novelty_doc`.
fn base_doc() -> Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:w1", "@type": "ex:Widget", "ex:name": "w1", "ex:size": 1},
            {"@id": "ex:w2", "@type": "ex:Widget", "ex:name": "w2"},
            {"@id": "ex:w4", "ex:name": "w4"},
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
/// * `ex:w4` gains a `@type` it never had while restating an already-indexed
///   name — the whole-document re-upsert of an untyped import;
/// * `ex:g1` restates its type and one `partOf` edge and adds a new one.
///
/// Each subject restates its `@type` so novelty-side class attribution can
/// see it — a novel property on a subject whose only type fact is already
/// indexed is a separate, documented staleness gap in these shims, not this
/// bug, and mixing the two would muddy the pin.
///
/// `ex:w4` is the exception that earns its place: its name is base-present, so
/// reconciliation charges it zero, but the base rollup filed it under no class
/// at all. Attributing it under `ex:Widget` anyway is the only thing that makes
/// `ex:Widget ex:name` agree with a scan.
fn novelty_doc() -> Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:w1", "@type": "ex:Widget", "ex:name": "w1", "ex:size": 1},
            {"@id": "ex:w2", "@type": "ex:Widget", "ex:name": ["w2", "w2-alt"]},
            {"@id": "ex:w3", "@type": "ex:Widget", "ex:name": "w3"},
            {"@id": "ex:w4", "@type": "ex:Widget", "ex:name": "w4"},
            {"@id": "ex:g1", "@type": "ex:Gadget",
             "ex:partOf": [{"@id": "ex:w1"}, {"@id": "ex:w3"}]}
        ]
    })
}

/// The third commit, covering the *under*-count half of #1391. Novelty accepts
/// every retraction — `apply_commit`'s dedup gate short-circuits on
/// `flake.op &&` and never examines one — so a ground DELETE that matches
/// nothing still lands as a retraction flake and, under a blind delta log,
/// subtracts one from a count that never included it.
///
/// * `ex:w2 ex:name "never-existed"` matches nothing: must charge zero;
/// * `ex:w1 ex:size 1` is a real base fact: must charge `-1`, taking
///   `ex:size`'s ledger-wide count to zero so the property drops out of
///   `apoc.meta.data` and `db.propertyKeys()` entirely.
fn deletion_doc() -> Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": {},
        "delete": [
            {"@id": "ex:w2", "ex:name": "never-existed"},
            {"@id": "ex:w1", "ex:size": 1}
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
        // w1:"w1", w2:"w2", w2:"w2-alt", w3:"w3", w4:"w4" — w4's name is
        // base-present, so it counts only if a class gained in the window
        // still attracts a restatement.
        5,
    ),
    (
        "ex:Widget",
        "ex:size",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s a ex:Widget . ?s ex:size ?o }",
        // Asserted in the base index, restated as a duplicate in novelty, then
        // genuinely retracted: the duplicate must not keep it alive.
        0,
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
        6,
    ),
    (
        "ex:partOf",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s ex:partOf ?o }",
        2,
    ),
    (
        "ex:size",
        "SELECT (COUNT(?o) AS ?c) WHERE { ?s ex:size ?o }",
        0,
    ),
];

/// `db.propertyKeys()` lists a predicate only while its merged count is above
/// zero, so it is `merged_stats`' one count-sensitive output — and the only
/// assertion that pins that call site. `ex:size` is exactly the discriminator:
/// under a blind merge its duplicate re-assert cancels the real retraction and
/// it stays listed; reconciled, it is gone.
/// `ex:partOf` is ref-valued, so `schema_names` files it under
/// `db.relationshipTypes()` rather than here.
const PROPERTY_KEYS_PRESENT: &[&str] = &["ex:name"];
const PROPERTY_KEYS_ABSENT: &[&str] = &["ex:size"];

/// Class instance counts `fluree info` renders.
const CLASS_CASES: &[(&str, &str, u64)] = &[
    (
        "ex:Widget",
        "SELECT (COUNT(?s) AS ?c) WHERE { ?s a ex:Widget }",
        4,
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
    let _ = fluree
        .update_with_opts(
            ledger,
            &deletion_doc(),
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("deletion update");

    let ledger = fluree.ledger(ALIAS).await.expect("reload after deletion");
    let retractions = ledger
        .novelty
        .iter_flakes(fluree_db_core::IndexType::Post)
        .filter(|f| !f.op)
        .count();
    assert_eq!(
        retractions, 2,
        "the fixture needs both retractions in novelty — the no-op one is the \
         whole point of the under-count half"
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

/// `db.propertyKeys()` as full IRIs — `merged_stats`' count-sensitive output.
async fn property_keys(fluree: &Fluree) -> Vec<String> {
    let snapshot = fluree.graph(ALIAS).load().await.expect("load");
    let db = snapshot.db();
    let rows = fluree
        .query_cypher(
            db,
            "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
        )
        .await
        .expect("db.propertyKeys")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    rows.as_array()
        .expect("rows")
        .iter()
        .map(|r| r[0].as_str().expect("propertyKey").to_string())
        .collect()
}

/// `fluree info`'s class + property counts, keyed by the IRI it renders.
///
/// `realtime_property_details` selects the arm: `true` (the default, so what
/// plain `fluree info` runs) takes `assemble_full_stats_with`, `false` takes
/// the lighter `assemble_fast_stats_with`. Both reconcile, and both are
/// asserted, because they are separate call sites.
async fn info_counts_arm(fluree: &Fluree, realtime: bool) -> (Value, Value) {
    let info = fluree
        .ledger_info(ALIAS)
        .with_realtime_property_details(realtime)
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

/// One `fast-stats novelty merge outcome` event.
#[derive(Debug)]
struct Stamp {
    site: String,
    outcome: String,
    duplicates: u64,
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
    let (classes, properties) = info_counts_arm(&fluree, true).await;
    let (classes_fast_arm, properties_fast_arm) = info_counts_arm(&fluree, false).await;
    let keys = property_keys(&fluree).await;
    let merges: Vec<Stamp> = store
        .find_events("fast-stats novelty merge outcome")
        .into_iter()
        .map(|e| Stamp {
            site: e.fields.get("site").cloned().unwrap_or_default(),
            outcome: e.fields.get("outcome").cloned().unwrap_or_default(),
            duplicates: e
                .fields
                .get("duplicates")
                .and_then(|d| d.parse::<u64>().ok())
                .unwrap_or(0),
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

    // Each asserted surface must have reconciled ON ITS OWN SITE, and must
    // have found the duplicates the fixture plants. A shared stamp label would
    // let any one reconciling assembly satisfy the guard for all four, so the
    // check names the lane it means. The `duplicates` floors are what stop a
    // count from being right for the wrong reason — an assembly that quietly
    // took the estimate lane, or one walking a window with nothing duplicated
    // in it.
    //
    // The fixture restates eight already-indexed facts (w1's type/name/size,
    // w2's type/name, w4's name, g1's type/partOf). `apoc.meta.data`'s rollup
    // counts `rdf:type` separately — it reads the resolver there only to learn
    // which memberships are new — so its property pass sees five of them; the
    // whole-window walks see all eight.
    for (site, floor) in [
        ("ledger-info-full", 8),
        ("ledger-info-fast", 8),
        ("apoc-meta-data", 5),
        ("merged-stats", 8),
    ] {
        let reconciled: Vec<&Stamp> = merges
            .iter()
            .filter(|m| m.site == site && m.outcome == "reconciled")
            .collect();
        if reconciled.is_empty() {
            failures.push(format!(
                "`{site}` never stamped a `reconciled` merge — that surface \
                 declined to the estimate lane, or the test never reached it \
                 [stamps: {merges:?}]"
            ));
            continue;
        }
        let best = reconciled.iter().map(|m| m.duplicates).max().unwrap_or(0);
        if best < floor {
            failures.push(format!(
                "`{site}` reconciled but found only {best} duplicate novelty \
                 facts (expected at least {floor}) — this pin is not \
                 exercising the bug on that surface [stamps: {merges:?}]"
            ));
        }
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
        // `apoc.meta.data` emits a row only while the count is above zero, so a
        // missing row reads as zero — which is the correct answer for a
        // property whose every fact has been retracted.
        let actual = apoc_count(&apoc_fast, class, property).unwrap_or(0);
        if actual != *expected {
            failures.push(format!(
                "apoc.meta.data reports {class} {property} = {actual}; a scan of \
                 the same facts returns {expected} [rows: {apoc_fast:?}]"
            ));
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
        // A property whose every fact is retracted is legitimately dropped from
        // the rendered stats, so "absent" reads as zero here.
        let actual = info_count(&properties, property).unwrap_or(0);
        if actual != *expected {
            failures.push(format!(
                "`fluree info` reports {property} count {actual}; a scan returns \
                 {expected} [properties: {properties}]"
            ));
        }
    }

    // The fast `ledger_info` arm is a separate call site from the default one
    // and must agree with it fact for fact.
    for (class, _, expected) in CLASS_CASES {
        match info_count(&classes_fast_arm, class) {
            Some(actual) if actual == *expected => {}
            other => failures.push(format!(
                "`fluree info` (realtime_property_details=false) reports {class} \
                 instance count {other:?}; a scan returns {expected}"
            )),
        }
    }
    for (property, _, expected) in LEDGER_CASES {
        let actual = info_count(&properties_fast_arm, property).unwrap_or(0);
        if actual != *expected {
            failures.push(format!(
                "`fluree info` (realtime_property_details=false) reports \
                 {property} count {actual}; a scan returns {expected}"
            ));
        }
    }

    // `db.propertyKeys()` is the one count-sensitive output of `merged_stats`.
    for key in PROPERTY_KEYS_PRESENT {
        if !keys.contains(&iri(key)) {
            failures.push(format!(
                "db.propertyKeys() omits {key}, which still has facts [keys: {keys:?}]"
            ));
        }
    }
    for key in PROPERTY_KEYS_ABSENT {
        if keys.contains(&iri(key)) {
            failures.push(format!(
                "db.propertyKeys() still lists {key} — every one of its facts was \
                 retracted, and only a duplicate re-assert counted on top of the \
                 base index keeps its merged count above zero [keys: {keys:?}]"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "issue #1391 regression pins found {} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

// ---------------------------------------------------------------------------
// Cost of reconciliation (#1391 asked for a measurement, not an assertion)
// ---------------------------------------------------------------------------

/// A/B the two merge modes over a synthetic novelty window at the sizes the
/// `MAX_RECONCILED_NOVELTY_FLAKES` cap permits, so the cap's cost is a number
/// rather than a claim.
///
/// Worst case by construction: every novelty flake restates an already-indexed
/// fact on a distinct `(graph, subject, predicate)`, so the probe cache misses
/// on every one and issues its maximum base scans. Real windows share `(s, p)`
/// across values and carry genuinely new subjects (whose dictionary lookup
/// misses cheaply), so they land well under this.
///
/// Ignored by default — it is a timing measurement, not a pin. Run with:
///
/// ```text
/// cargo test --release -p fluree-db-api --features native \
///     --test it_fast_stats_1391_regression -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "current_thread")]
#[ignore = "timing measurement; run with --ignored --nocapture"]
async fn issue_1391_reconciliation_cost() {
    for subjects in [5_000usize, 24_000] {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().to_string_lossy().to_string();
        let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
        let alias = "r1391/cost:main";
        let index_config = IndexConfig {
            reindex_min_bytes: 5_000_000_000,
            reindex_max_bytes: 5_000_000_000,
        };
        // Two facts per subject => `2 * subjects` novelty flakes, each on its
        // own (s, p).
        let doc = |n: usize| {
            let graph: Vec<Value> = (0..n)
                .map(|i| {
                    json!({
                        "@id": format!("ex:s{i}"),
                        "@type": "ex:Thing",
                        "ex:name": format!("n{i}")
                    })
                })
                .collect();
            json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": graph})
        };

        let ledger = fluree.create_ledger(alias).await.expect("create_ledger");
        let _ = fluree
            .insert_with_opts(
                ledger,
                &doc(subjects),
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .expect("base insert");
        fluree
            .reindex(alias, ReindexOptions::default())
            .await
            .expect("reindex");
        let ledger = fluree.ledger(alias).await.expect("reload");
        // Restate all of it: every flake is a duplicate of a base fact.
        let _ = fluree
            .insert_with_opts(
                ledger,
                &doc(subjects),
                TxnOpts::default(),
                CommitOpts::default(),
                &index_config,
            )
            .await
            .expect("duplicate insert");

        let ledger = fluree.ledger(alias).await.expect("reload");
        let indexed = ledger.snapshot.stats.clone().unwrap_or_default();
        let flakes = ledger.novelty.len();

        let time_it = |merge| {
            let start = std::time::Instant::now();
            let stats = fluree_db_novelty::assemble_fast_stats_with(
                &indexed,
                &ledger.snapshot,
                ledger.novelty.as_ref(),
                ledger.t(),
                None,
                merge,
            );
            (start.elapsed(), stats.flakes)
        };

        let (estimate, est_flakes) = time_it(fluree_db_novelty::NoveltyMerge::Estimate);
        let (reconciled, rec_flakes) =
            time_it(fluree_db_novelty::NoveltyMerge::Reconciled { site: "cost-probe" });

        println!(
            "novelty={flakes:>6} flakes  estimate={estimate:>10.2?} (flakes {est_flakes})  \
             reconciled={reconciled:>10.2?} (flakes {rec_flakes})  ratio={:.1}x",
            reconciled.as_secs_f64() / estimate.as_secs_f64().max(f64::MIN_POSITIVE)
        );
    }
}
