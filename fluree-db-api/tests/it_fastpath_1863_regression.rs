//! Regression pins for issue #1863: `count_rows_operator` read a persisted
//! predicate-dictionary miss as `COUNT = 0`.
//!
//! The operator has two lanes — a metadata-only lane valid at the indexed head
//! with no overlay, and a base-count-plus-novelty-delta lane. `sid_to_p_id`
//! resolves against the **persisted index dictionary only**, so a predicate
//! that lives solely in an overlay has no entry. Reading that miss as "zero
//! rows" is sound under the first lane and false under the second, and the
//! check sat above both — so it was applied unconditionally.
//!
//! An overlay-only predicate is not exotic. It is:
//!
//! * **any freshly inserted property**, for the whole window between its first
//!   commit and the next index — no reasoning involved, and on a write-active
//!   server that window is permanently open for whatever predicate was
//!   introduced most recently;
//! * **any datalog-derived predicate**, whose window never closes, because a
//!   derived fact never enters the persisted dictionary;
//! * **any OWL2-RL-derived predicate**, for the same reason. (#1863 records
//!   "owl2rl is not affected" as a narrowing fact; that is false. The issue's
//!   probe used a *symmetric-property* axiom, which derives more flakes for a
//!   predicate that already exists in the base index, so it resolved a `p_id`
//!   and escaped. An `owl:inverseOf` axiom that mints a predicate with zero
//!   base assertions fails identically — `owl2rl_inverse_of_*` below.)
//!
//! Every case asserts three things: the fast lane's answer, the generic
//! pipeline's answer (`FLUREE_DISABLE_QUERY_FAST_PATHS`, both against a
//! hand-pinned value so a generic regression is caught as loudly as a
//! fast-path one), and the engine's `fast-path outcome` routing stamp.
//!
//! **On vacuity.** This defect survived three and a half months because the
//! datalog suites are entirely memory-backed: with no binary store the
//! operator returns at its first line, the generic pipeline answers correctly,
//! and the suite is green for the wrong reason. Every ledger here is therefore
//! file-backed and explicitly reindexed. The `MustNotFire` cases cannot prove
//! that on their own — a non-indexed fixture would satisfy them too — so each
//! ledger also carries a `MustFire` canary over an *indexed* predicate under
//! the same conditions. Those canaries are what make the rest of the file
//! non-vacuous; if one regresses to `MustNotFire`, do not "fix" the canary.
//!
//! Own test binary: toggles the process-global kill switch AND asserts
//! fast-path routing via span capture, so it must not share a process with
//! other tests.

#![cfg(feature = "native")]

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig,
    ReindexOptions, TxnOpts,
};
use serde_json::Value;

const EX: &str = "http://example.org/ns/";

const SPARQL_PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// Rows carrying `ex:indexed` / `ex:other` in the base (pre-index) commit.
const N_BASE: usize = 24;
/// Rows carrying the overlay-only predicate `ex:novel` in the tail commit.
const N_NOVEL: usize = 7;
/// Extra `ex:indexed` rows in the tail commit, so the indexed-predicate canary
/// exercises the base-plus-delta lane rather than the at-HEAD metadata lane.
const N_TAIL_INDEXED: usize = 3;

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

/// Base commit: two ordinary predicates, both in the persisted dictionary
/// after the reindex that follows.
fn base_turtle() -> String {
    let mut buf = String::from("@prefix ex: <http://example.org/ns/> .\n\n");
    for i in 0..N_BASE {
        buf.push_str(&format!(
            "ex:s-{i:04} ex:indexed \"v-{i:04}\" ; ex:other ex:o-{i:04} .\n"
        ));
    }
    buf
}

/// Tail commit, applied AFTER the index point. `ex:novel` appears for the
/// first time here, so it exists only in the novelty overlay and has no
/// persisted `p_id` — the #1863 shape, with no reasoning anywhere in sight.
/// The extra `ex:indexed` rows give the canary a real novelty delta to merge.
fn tail_turtle() -> String {
    let mut buf = String::from("@prefix ex: <http://example.org/ns/> .\n\n");
    for i in 0..N_NOVEL {
        buf.push_str(&format!("ex:s-{i:04} ex:novel ex:n-{i:04} .\n"));
    }
    for i in 0..N_TAIL_INDEXED {
        buf.push_str(&format!("ex:t-{i:04} ex:indexed \"tail-{i:04}\" .\n"));
    }
    buf
}

/// Datalog ledger: three asserted `ex:owns` edges, fully indexed, plus a rule
/// deriving `ex:acq` from them. `ex:acq` is never asserted, so it exists only
/// in the reasoning overlay — permanently.
fn datalog_turtle() -> &'static str {
    "@prefix ex: <http://example.org/ns/> .\n\n\
     ex:a ex:owns ex:b .\n\
     ex:b ex:owns ex:c .\n\
     ex:c ex:owns ex:d .\n"
}

fn datalog_rule() -> Value {
    serde_json::json!({
        "@context": {"ex": EX, "f": "https://ns.flur.ee/db#"},
        "@id": "ex:acqRule",
        "f:rule": {
            "@type": "@json",
            "@value": {
                "@context": {"ex": EX},
                "where": {"@id": "?x", "ex:owns": "?y"},
                "insert": {"@id": "?x", "ex:acq": {"@id": "?y"}}
            }
        }
    })
}

/// OWL2-RL ledger: `owl:inverseOf` mints `ex:knownBy`, which has zero base
/// assertions. This is the axiom shape #1863's own probe did not try.
fn owl_turtle() -> &'static str {
    "@prefix ex: <http://example.org/ns/> .\n\
     @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\n\
     ex:knows a owl:ObjectProperty .\n\
     ex:knownBy a owl:ObjectProperty ; owl:inverseOf ex:knows .\n\
     ex:a ex:knows ex:b .\n\
     ex:b ex:knows ex:c .\n\
     ex:c ex:knows ex:d .\n"
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Thresholds high enough that no commit self-indexes. Every index point in
/// this file is an explicit `reindex`, so the base/novelty split is pinned by
/// the test rather than left to an auto-index trigger (#1762 / #1574 would
/// otherwise be able to make these cases vacuous without touching them).
fn no_self_index() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    }
}

async fn new_fluree() -> (tempfile::TempDir, Fluree) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build Fluree");
    (dir, fluree)
}

async fn insert_turtle(fluree: &Fluree, alias: &str, turtle: &str) {
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    fluree
        .insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_self_index(),
            None,
        )
        .await
        .expect("insert turtle");
}

async fn insert_jsonld(fluree: &Fluree, alias: &str, data: &Value) {
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    fluree
        .insert_with_opts(
            ledger,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &no_self_index(),
        )
        .await
        .expect("insert jsonld");
}

/// Base commit, then an index point, then a tail commit whose predicates are
/// therefore overlay-only. With `fold_tail`, a second index point folds the
/// tail into the persisted dictionary — the "and it works again afterwards"
/// control.
async fn setup_split(alias: &str, fold_tail: bool) -> (tempfile::TempDir, Fluree) {
    let (dir, fluree) = new_fluree().await;
    fluree.create_ledger(alias).await.expect("create_ledger");
    insert_turtle(&fluree, alias, &base_turtle()).await;
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex base");
    insert_turtle(&fluree, alias, &tail_turtle()).await;
    if fold_tail {
        fluree
            .reindex(alias, ReindexOptions::default())
            .await
            .expect("reindex tail");
    }
    (dir, fluree)
}

/// Fully indexed ledger with no leftover novelty: the only overlay at query
/// time is the reasoning overlay itself.
async fn setup_reasoning(
    alias: &str,
    turtle: &str,
    rule: Option<Value>,
) -> (tempfile::TempDir, Fluree) {
    let (dir, fluree) = new_fluree().await;
    fluree.create_ledger(alias).await.expect("create_ledger");
    insert_turtle(&fluree, alias, turtle).await;
    if let Some(rule) = rule {
        insert_jsonld(&fluree, alias, &rule).await;
    }
    fluree
        .reindex(alias, ReindexOptions::default())
        .await
        .expect("reindex");
    (dir, fluree)
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ledger {
    /// Indexed base + an unindexed tail introducing `ex:novel`.
    Novelty,
    /// Same commits, but the tail is indexed too.
    Folded,
    /// Indexed `ex:owns` + a rule deriving `ex:acq`.
    Datalog,
    /// Indexed `ex:knows` + `owl:inverseOf` deriving `ex:knownBy`.
    Owl,
}

/// Which query surface the case runs on. All three lower to the same IR and
/// were confirmed to ride this operator.
enum Surface {
    Sparql(&'static str),
    /// JSON-LD query as a literal, parsed at run time.
    JsonLd(&'static str),
    Cypher(&'static str),
}

/// Routing assertion against the engine's `fast-path outcome` stamps.
#[derive(Clone, Copy)]
enum Routing {
    /// This site must `proceed`. On this file's ledgers that doubles as the
    /// index-backed canary: it can only hold if the fixture really did build a
    /// binary index.
    MustFire,
    /// This site must NOT `proceed` — the lane cannot answer an overlay-only
    /// predicate, so it has to defer to the generic pipeline.
    MustNotFire,
}

/// The `label` of `count_rows_operator` (`fluree-db-query/src/fast_count.rs`).
const SITE: &str = "COUNT rows";

struct Case {
    name: &'static str,
    ledger: Ledger,
    surface: Surface,
    /// Hand-pinned count. Both the fast lane and the generic pipeline must
    /// produce exactly this.
    expected: i64,
    routing: Routing,
}

fn cases() -> Vec<Case> {
    use Ledger::{Datalog, Folded, Novelty, Owl};
    use Routing::{MustFire, MustNotFire};
    use Surface::{Cypher, JsonLd, Sparql};

    let novel = N_NOVEL as i64;
    let indexed = (N_BASE + N_TAIL_INDEXED) as i64;

    vec![
        // ---- Novelty-only predicate, NO reasoning: the headline -------------
        Case {
            name: "SPARQL COUNT(*) over a novelty-only predicate",
            ledger: Novelty,
            surface: Sparql("SELECT (COUNT(*) AS ?n) WHERE { ?s ex:novel ?o }"),
            expected: novel,
            routing: MustNotFire,
        },
        Case {
            name: "SPARQL COUNT(?s) over a novelty-only predicate",
            ledger: Novelty,
            surface: Sparql("SELECT (COUNT(?s) AS ?n) WHERE { ?s ex:novel ?o }"),
            expected: novel,
            routing: MustNotFire,
        },
        Case {
            name: "JSON-LD count over a novelty-only predicate",
            ledger: Novelty,
            surface: JsonLd(
                r#"{"@context": {"ex": "http://example.org/ns/"},
                    "select": ["(count ?s)"],
                    "where": {"@id": "?s", "ex:novel": "?o"}}"#,
            ),
            expected: novel,
            routing: MustNotFire,
        },
        Case {
            name: "Cypher count(*) over a novelty-only relationship",
            ledger: Novelty,
            surface: Cypher("MATCH (a)-[:`http://example.org/ns/novel`]->(b) RETURN count(*)"),
            expected: novel,
            routing: MustNotFire,
        },
        // ---- Canaries: the lane must survive the fix, and the fixture must
        // ---- actually be index-backed -------------------------------------
        Case {
            name: "CANARY SPARQL COUNT(*) over an indexed predicate under novelty",
            ledger: Novelty,
            surface: Sparql("SELECT (COUNT(*) AS ?n) WHERE { ?s ex:indexed ?o }"),
            expected: indexed,
            routing: MustFire,
        },
        Case {
            name: "CANARY Cypher count(*) over an indexed relationship under novelty",
            ledger: Novelty,
            surface: Cypher("MATCH (a)-[:`http://example.org/ns/other`]->(b) RETURN count(*)"),
            expected: N_BASE as i64,
            routing: MustFire,
        },
        // ---- Once the tail is indexed, the lane takes it again --------------
        Case {
            name: "CANARY SPARQL COUNT(*) over the same predicate once indexed",
            ledger: Folded,
            surface: Sparql("SELECT (COUNT(*) AS ?n) WHERE { ?s ex:novel ?o }"),
            expected: novel,
            routing: MustFire,
        },
        // ---- Datalog-derived predicate -------------------------------------
        Case {
            name: "SPARQL COUNT(*) over a datalog-derived predicate",
            ledger: Datalog,
            surface: Sparql(
                "# PRAGMA reasoning: datalog\n\
                 SELECT (COUNT(*) AS ?n) WHERE { ?a ex:acq ?b }",
            ),
            expected: 3,
            routing: MustNotFire,
        },
        Case {
            name: "SPARQL COUNT(?a) over a datalog-derived predicate",
            ledger: Datalog,
            surface: Sparql(
                "# PRAGMA reasoning: datalog\n\
                 SELECT (COUNT(?a) AS ?n) WHERE { ?a ex:acq ?b }",
            ),
            expected: 3,
            routing: MustNotFire,
        },
        Case {
            name: "JSON-LD count over a datalog-derived predicate",
            ledger: Datalog,
            surface: JsonLd(
                r#"{"@context": {"ex": "http://example.org/ns/"},
                    "select": ["(count ?a)"],
                    "where": {"@id": "?a", "ex:acq": "?b"},
                    "reasoning": "datalog"}"#,
            ),
            expected: 3,
            routing: MustNotFire,
        },
        Case {
            name: "CANARY SPARQL COUNT(*) over the asserted predicate under datalog",
            ledger: Datalog,
            surface: Sparql(
                "# PRAGMA reasoning: datalog\n\
                 SELECT (COUNT(*) AS ?n) WHERE { ?a ex:owns ?b }",
            ),
            expected: 3,
            routing: MustFire,
        },
        // ---- OWL2-RL-derived predicate -------------------------------------
        Case {
            name: "owl2rl_inverse_of SPARQL COUNT(*) over a derived predicate",
            ledger: Owl,
            surface: Sparql(
                "# PRAGMA reasoning: owl2rl\n\
                 SELECT (COUNT(*) AS ?n) WHERE { ?x ex:knownBy ?y }",
            ),
            expected: 3,
            routing: MustNotFire,
        },
        Case {
            name: "CANARY SPARQL COUNT(*) over the asserted predicate under owl2rl",
            ledger: Owl,
            surface: Sparql(
                "# PRAGMA reasoning: owl2rl\n\
                 SELECT (COUNT(*) AS ?n) WHERE { ?x ex:knows ?y }",
            ),
            expected: 3,
            routing: MustFire,
        },
    ]
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Pull the single scalar out of an ungrouped-aggregate result, accepting both
/// the `[[n]]` and `[n]` renderings.
fn scalar_count(rows: &Value, what: &str) -> i64 {
    let arr = rows
        .as_array()
        .unwrap_or_else(|| panic!("{what}: expected an array of rows, got {rows}"));
    assert_eq!(
        arr.len(),
        1,
        "{what}: an ungrouped aggregate must return exactly one row, got {rows}"
    );
    let cell = match &arr[0] {
        Value::Array(cols) => {
            assert_eq!(cols.len(), 1, "{what}: expected one column, got {rows}");
            &cols[0]
        }
        other => other,
    };
    cell.as_i64()
        .unwrap_or_else(|| panic!("{what}: count cell is not an integer: {rows}"))
}

/// Cypher wraps its rows as
/// `{"results": [{"columns": [...], "data": [{"row": [n], "meta": [...]}]}]}`.
fn cypher_count(out: &Value, what: &str) -> i64 {
    let data = out
        .pointer("/results/0/data")
        .unwrap_or_else(|| panic!("{what}: no /results/0/data in {out}"));
    let rows = data
        .as_array()
        .unwrap_or_else(|| panic!("{what}: data is not an array: {out}"));
    assert_eq!(rows.len(), 1, "{what}: expected exactly one row, got {out}");
    let row = rows[0]
        .get("row")
        .unwrap_or_else(|| panic!("{what}: row object has no `row` key: {out}"));
    scalar_count(row, what)
}

async fn run_case(fluree: &Fluree, alias: &str, case: &Case) -> i64 {
    // Reload per query so no plan/view state is shared across the kill-switch
    // toggle.
    let db: GraphDb = fluree.db(alias).await.expect("load db");
    match &case.surface {
        Surface::Sparql(q) => {
            let full = format!("{SPARQL_PREFIX}{q}");
            let out = fluree
                .query(&db, full.as_str())
                .await
                .unwrap_or_else(|e| panic!("{}: sparql: {e}", case.name));
            let json = out
                .to_jsonld(&db.snapshot)
                .unwrap_or_else(|e| panic!("{}: jsonld render: {e}", case.name));
            scalar_count(&json, case.name)
        }
        Surface::JsonLd(q) => {
            let query: Value = serde_json::from_str(q)
                .unwrap_or_else(|e| panic!("{}: bad query json: {e}", case.name));
            let out = fluree
                .query(&db, &query)
                .await
                .unwrap_or_else(|e| panic!("{}: jsonld query: {e}", case.name));
            let json = out
                .to_jsonld(&db.snapshot)
                .unwrap_or_else(|e| panic!("{}: jsonld render: {e}", case.name));
            scalar_count(&json, case.name)
        }
        Surface::Cypher(q) => {
            let out = fluree
                .query_cypher(&db, q)
                .await
                .unwrap_or_else(|e| panic!("{}: cypher: {e}", case.name));
            let json = out
                .to_cypher_json_async(db.as_graph_db_ref())
                .await
                .unwrap_or_else(|e| panic!("{}: cypher render: {e}", case.name));
            cypher_count(&json, case.name)
        }
    }
}

/// RAII restore of the process-global kill switch, including on panic.
struct FastPathGuard;
impl Drop for FastPathGuard {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn issue_1863_overlay_only_predicates_count_correctly() {
    // The kill switch OR's with this env var; with it set, the fast phase
    // below runs generically and every assertion is vacuous.
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "FLUREE_DISABLE_QUERY_FAST_PATHS is set — the fast-path phase of this \
         test would run generically and pin nothing. Unset it."
    );
    let _guard = FastPathGuard;

    const NOVELTY: &str = "r1863:novelty";
    const FOLDED: &str = "r1863:folded";
    const DATALOG: &str = "r1863:datalog";
    const OWL: &str = "r1863:owl";

    let (_d1, novelty) = setup_split(NOVELTY, false).await;
    let (_d2, folded) = setup_split(FOLDED, true).await;
    let (_d3, datalog) = setup_reasoning(DATALOG, datalog_turtle(), Some(datalog_rule())).await;
    let (_d4, owl) = setup_reasoning(OWL, owl_turtle(), None).await;

    let env = |l: Ledger| -> (&Fluree, &'static str) {
        match l {
            Ledger::Novelty => (&novelty, NOVELTY),
            Ledger::Folded => (&folded, FOLDED),
            Ledger::Datalog => (&datalog, DATALOG),
            Ledger::Owl => (&owl, OWL),
        }
    };
    let cases = cases();

    // Phase 1 — fast paths on, under span capture so each answer can be
    // attributed to the site that served it.
    let (store, tracing_guard) = span_capture::init_test_tracing();
    set_fast_paths_disabled(false);
    let mut fast: Vec<i64> = Vec::new();
    let mut proceeded: Vec<Vec<String>> = Vec::new();
    for c in &cases {
        let (fluree, alias) = env(c.ledger);
        let before = store.find_events("fast-path outcome").len();
        fast.push(run_case(fluree, alias, c).await);
        proceeded.push(
            store.find_events("fast-path outcome")[before..]
                .iter()
                .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
                .filter_map(|e| e.fields.get("site").cloned())
                .collect(),
        );
    }
    drop(tracing_guard);

    // Phase 2 — generic pipeline (the kill-switch reference).
    set_fast_paths_disabled(true);
    let mut generic: Vec<i64> = Vec::new();
    for c in &cases {
        let (fluree, alias) = env(c.ledger);
        generic.push(run_case(fluree, alias, c).await);
    }
    set_fast_paths_disabled(false);

    let mut failures: Vec<String> = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        if fast[i] != c.expected {
            failures.push(format!(
                "{}: fast lane counted {}, expected {} [proceeded: {:?}]",
                c.name, fast[i], c.expected, proceeded[i]
            ));
        }
        if generic[i] != c.expected {
            failures.push(format!(
                "{}: generic pipeline counted {}, expected {} — the hand-pinned \
                 answer is wrong or the general pipeline regressed",
                c.name, generic[i], c.expected
            ));
        }
        match c.routing {
            Routing::MustFire => {
                if !proceeded[i].iter().any(|s| s == SITE) {
                    failures.push(format!(
                        "{}: expected site `{SITE}` to proceed — either the fix \
                         disabled the lane, or this fixture is not index-backed \
                         and every MustNotFire case in this file is vacuous \
                         [proceeded: {:?}]",
                        c.name, proceeded[i]
                    ));
                }
            }
            Routing::MustNotFire => {
                if proceeded[i].iter().any(|s| s == SITE) {
                    failures.push(format!(
                        "{}: site `{SITE}` proceeded on an overlay-only \
                         predicate it cannot answer [proceeded: {:?}]",
                        c.name, proceeded[i]
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "issue #1863 regression pins found {} failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
