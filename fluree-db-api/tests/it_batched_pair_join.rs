//! The nested-loop join's batched subject–object pair lane: a join step whose
//! subject and object are both bound from the left, as the edge closing a
//! cycle is (LUBM Q9: `?x :advisor ?y . ?y :teacherOf ?z . ?x :takesCourse ?z`).
//!
//! Every query runs with fast paths on and off and must give the same
//! multiset; the main ones also match answers computed from the fixture here.
//! Routing is pinned with the lane's `batched-pair-join` stamps, so a case
//! cannot pass on the per-row path.
//!
//! Own test binary: toggles the process-global kill switch and captures spans.

#![cfg(feature = "native")]

mod support;

use std::collections::{BTreeMap, BTreeSet};

use fluree_db_api::{
    set_fast_paths_disabled, CommitOpts, Fluree, FlureeBuilder, GovernanceOptions, GraphDb,
    IndexConfig, LedgerState, ReindexOptions, TxnOpts,
};
use serde_json::{json, Value};
use support::span_capture::{init_test_tracing, SpanStore};

const EX: &str = "http://example.org/ns/";
const SITE: &str = "batched-pair-join";
const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";
const TRIANGLE: &str = "?x a ex:Student . ?y a ex:Faculty . ?z a ex:Course .
     ?x ex:advisor ?y . ?y ex:teacherOf ?z . ?x ex:takesCourse ?z";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const NO_BACKGROUND: IndexConfig = IndexConfig {
    reindex_min_bytes: 1_000_000_000,
    reindex_max_bytes: 1_000_000_000,
};

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_fast_paths_disabled(false);
    }
}

type Edges = BTreeSet<(String, String)>;

/// A small university. Its triangles exercise duplicate `(x, z)` pairs
/// reached through two advisors, advisors and courses the type patterns
/// exclude, and a student taking every course.
#[derive(Clone)]
struct Uni {
    students: BTreeSet<String>,
    faculty: BTreeSet<String>,
    courses: BTreeSet<String>,
    teaches: Edges,
    advises: Edges,
    takes: Edges,
    mentors: Edges,
    credits: Vec<(String, i64)>,
}

fn edge(a: impl Into<String>, b: impl Into<String>) -> (String, String) {
    (a.into(), b.into())
}

impl Uni {
    fn new() -> Self {
        let mut uni = Uni {
            students: (0..30).map(|j| format!("s{j}")).collect(),
            faculty: (0..6).map(|i| format!("f{i}")).collect(),
            courses: (0..18).map(|k| format!("c{k}")).collect(),
            teaches: Edges::new(),
            advises: Edges::new(),
            takes: Edges::new(),
            mentors: Edges::new(),
            credits: (0..18).map(|k| (format!("c{k}"), 3 + (k % 2))).collect(),
        };
        uni.students
            .extend(["sdup".to_string(), "shub".to_string()]);
        for i in 0..6 {
            for k in [3 * i, 3 * i + 1, 3 * i + 2, (3 * i + 3) % 18] {
                uni.teaches.insert(edge(format!("f{i}"), format!("c{k}")));
            }
        }
        // A postdoc (not Faculty) and an untyped course.
        uni.teaches.insert(edge("p0", "c0"));
        uni.teaches.insert(edge("p0", "c1"));
        uni.teaches.insert(edge("f0", "cx"));
        for j in 0..30 {
            let s = format!("s{j}");
            uni.advises.insert(edge(&s, format!("f{}", j % 6)));
            if j % 5 == 0 {
                uni.advises.insert(edge(&s, format!("f{}", (j + 1) % 6)));
            }
            if j % 7 == 0 {
                uni.advises.insert(edge(&s, "p0"));
            }
            for k in [(5 * j) % 18, (7 * j + 1) % 18, (j + 2) % 18, 3 * (j % 6)] {
                uni.takes.insert(edge(&s, format!("c{k}")));
            }
        }
        uni.takes.insert(edge("s0", "cx"));
        // f0 and f5 both teach c0: two rows share the pair (sdup, c0).
        uni.advises.insert(edge("sdup", "f0"));
        uni.advises.insert(edge("sdup", "f5"));
        uni.takes.insert(edge("sdup", "c0"));
        uni.advises.insert(edge("shub", "f0"));
        for k in 0..18 {
            uni.takes.insert(edge("shub", format!("c{k}")));
        }
        for (a, b) in [
            ("s0", "s0"),
            ("s1", "s2"),
            ("s2", "s2"),
            ("s2", "s1"),
            ("s4", "s4"),
        ] {
            uni.mentors.insert(edge(a, b));
        }
        uni
    }

    fn insert(&self) -> Value {
        let mut nodes: BTreeMap<String, serde_json::Map<String, Value>> = BTreeMap::new();
        fn node<'a>(
            nodes: &'a mut BTreeMap<String, serde_json::Map<String, Value>>,
            id: &str,
        ) -> &'a mut serde_json::Map<String, Value> {
            nodes.entry(id.to_string()).or_insert_with(|| {
                let mut m = serde_json::Map::new();
                m.insert("@id".into(), json!(format!("ex:{id}")));
                m
            })
        }
        for (set, class) in [
            (&self.students, "ex:Student"),
            (&self.faculty, "ex:Faculty"),
            (&self.courses, "ex:Course"),
        ] {
            for id in set {
                node(&mut nodes, id).insert("@type".into(), json!(class));
            }
        }
        node(&mut nodes, "p0").insert("@type".into(), json!("ex:Postdoc"));
        for (edges, prop) in [
            (&self.teaches, "ex:teacherOf"),
            (&self.advises, "ex:advisor"),
            (&self.takes, "ex:takesCourse"),
            (&self.mentors, "ex:mentors"),
        ] {
            for (a, b) in edges {
                let values = node(&mut nodes, a)
                    .entry(prop.to_string())
                    .or_insert_with(|| json!([]));
                values
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"@id": format!("ex:{b}")}));
            }
        }
        for (c, n) in &self.credits {
            node(&mut nodes, c).insert("ex:credits".into(), json!(n));
        }
        json!({"@context": {"ex": EX}, "@graph": nodes.into_values().collect::<Vec<_>>()})
    }

    /// Q9's rows, as `x|y|z` IRIs.
    fn triangles(&self) -> Vec<String> {
        let mut rows = Vec::new();
        for (x, y) in &self.advises {
            if !self.students.contains(x) || !self.faculty.contains(y) {
                continue;
            }
            for (t, z) in &self.teaches {
                if t == y && self.courses.contains(z) && self.takes.contains(&edge(x, z)) {
                    rows.push(format!("{EX}{x}|{EX}{y}|{EX}{z}"));
                }
            }
        }
        rows.sort();
        rows
    }
}

async fn ledger_with(fluree: &Fluree, id: &str, data: &Value, leaflet_rows: Option<usize>) {
    let ledger = support::genesis_ledger(fluree, id).await;
    fluree
        .insert_with_opts(
            ledger,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("insert");
    reindex(fluree, id, leaflet_rows).await;
}

async fn reindex(fluree: &Fluree, id: &str, leaflet_rows: Option<usize>) {
    let mut opts = ReindexOptions::default();
    if let Some(rows) = leaflet_rows {
        opts = opts.with_indexer_config(
            fluree_db_indexer::IndexerConfig::default()
                .with_leaflet_rows(rows)
                .with_leaf_rows(rows * 4),
        );
    }
    fluree.reindex(id, opts).await.expect("reindex");
}

async fn transact(fluree: &Fluree, id: &str, update: Value) {
    let ledger: LedgerState = fluree.ledger(id).await.expect("ledger");
    fluree
        .update_with_opts(
            ledger,
            &update,
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("update");
}

/// A SPARQL result's rows as `|`-joined values, variables in name order,
/// sorted.
async fn sparql_rows(fluree: &Fluree, db: &GraphDb, query: &str) -> Vec<String> {
    let result = fluree
        .query(db, query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let json = result.to_sparql_json(&db.snapshot).expect("sparql json");
    let mut vars: Vec<&str> = json["head"]["vars"]
        .as_array()
        .expect("vars")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    vars.sort_unstable();
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            vars.iter()
                .map(|v| b.get(*v).and_then(|t| t["value"].as_str()).unwrap_or(""))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    rows
}

async fn jsonld_rows(fluree: &Fluree, db: &GraphDb, query: &Value) -> Vec<String> {
    let result = fluree
        .query(db, query)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let json = result.to_jsonld(&db.snapshot).expect("jsonld");
    let mut rows: Vec<String> = json
        .as_array()
        .expect("rows")
        .iter()
        .map(ToString::to_string)
        .collect();
    rows.sort();
    rows
}

fn outcomes(store: &SpanStore) -> Vec<String> {
    store
        .find_events("fast-path outcome")
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(SITE))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Lane {
    MustFire,
    MustNotFire,
}

enum Query<'a> {
    Sparql(&'a str),
    JsonLd(&'a Value),
}

async fn rows_of(fluree: &Fluree, db: &GraphDb, query: &Query<'_>) -> Vec<String> {
    match query {
        Query::Sparql(q) => sparql_rows(fluree, db, &format!("{PREFIX}{q}")).await,
        Query::JsonLd(q) => jsonld_rows(fluree, db, q).await,
    }
}

/// The query's rows with fast paths on, routed as `lane` says, after
/// checking that the per-row path (fast paths off, the lane never
/// proceeding) gives the same multiset.
async fn differential(
    fluree: &Fluree,
    db: &GraphDb,
    query: Query<'_>,
    lane: Lane,
    label: &str,
) -> Vec<String> {
    let (store, guard) = init_test_tracing();
    set_fast_paths_disabled(false);
    let fast = rows_of(fluree, db, &query).await;
    let seen = outcomes(&store);
    drop(guard);
    let proceeded = seen.iter().any(|o| o == "proceed");
    let declined = seen.iter().any(|o| o == "fallback:gate_declined");
    match lane {
        Lane::MustFire => assert!(
            proceeded && !declined,
            "{label}: the pair lane must run; stamps {seen:?}"
        ),
        Lane::MustNotFire => assert!(
            declined && !proceeded,
            "{label}: the pair lane must decline; stamps {seen:?}"
        ),
    }

    let (store, guard) = init_test_tracing();
    set_fast_paths_disabled(true);
    let generic = rows_of(fluree, db, &query).await;
    set_fast_paths_disabled(false);
    let seen = outcomes(&store);
    drop(guard);
    assert!(
        !seen.iter().any(|o| o == "proceed"),
        "{label}: the pair lane ran with fast paths off; stamps {seen:?}"
    );
    assert_eq!(
        fast, generic,
        "{label}: the pair lane and the per-row path differ"
    );
    fast
}

/// `n|x` rows of `x`'s row count, sorted.
fn grouped_by_first(rows: &[String]) -> Vec<String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(row.split('|').next().unwrap()).or_default() += 1;
    }
    let mut out: Vec<String> = counts
        .into_iter()
        .map(|(x, n)| format!("{n}|{x}"))
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "current_thread")]
async fn closing_edge_matches_the_fixture_and_the_per_row_path() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    let uni = Uni::new();
    let expected = uni.triangles();
    assert!(
        expected.len() > 50,
        "fixture too small: {} rows",
        expected.len()
    );
    assert!(
        expected.contains(&format!("{EX}sdup|{EX}f0|{EX}c0"))
            && expected.contains(&format!("{EX}sdup|{EX}f5|{EX}c0")),
        "fixture must reach one pair through two advisors"
    );

    // Default leaflets, then four-row leaflets so a subject's rows span
    // leaflets and leaves.
    for leaflet_rows in [None, Some(4)] {
        let label = format!("leaflet rows {leaflet_rows:?}");
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "batched-pair/uni:main";
        ledger_with(&fluree, id, &uni.insert(), leaflet_rows).await;
        let db = fluree.db(id).await.expect("db");

        let q = format!("SELECT ?x ?y ?z WHERE {{ {TRIANGLE} }}");
        let rows = differential(&fluree, &db, Query::Sparql(&q), Lane::MustFire, &label).await;
        assert_eq!(rows, expected, "{label}: triangles");

        let q = format!("SELECT (COUNT(*) AS ?n) WHERE {{ {TRIANGLE} }}");
        let rows = differential(&fluree, &db, Query::Sparql(&q), Lane::MustFire, &label).await;
        assert_eq!(rows, vec![expected.len().to_string()], "{label}: COUNT(*)");

        let q = format!("SELECT ?x (COUNT(*) AS ?n) WHERE {{ {TRIANGLE} }} GROUP BY ?x");
        let rows = differential(&fluree, &db, Query::Sparql(&q), Lane::MustFire, &label).await;
        assert_eq!(rows, grouped_by_first(&expected), "{label}: grouped counts");

        // LIMIT: any 5 of the rows, in both modes.
        for disabled in [false, true] {
            set_fast_paths_disabled(disabled);
            let q = format!("{PREFIX}SELECT ?x ?y ?z WHERE {{ {TRIANGLE} }} LIMIT 5");
            let rows = sparql_rows(&fluree, &db, &q).await;
            set_fast_paths_disabled(false);
            assert_eq!(
                rows.len(),
                5,
                "{label}: LIMIT 5 (fast paths off: {disabled})"
            );
            for row in &rows {
                let have = rows.iter().filter(|r| *r == row).count();
                let may = expected.iter().filter(|r| *r == row).count();
                assert!(have <= may, "{label}: LIMIT row {row} not in the answer");
            }
        }

        // A self-loop: subject and object bound from one column.
        let q = "SELECT ?x WHERE { VALUES ?x { ex:s0 ex:s1 ex:s2 ex:s3 } ?x ex:mentors ?x }";
        let rows = differential(&fluree, &db, Query::Sparql(q), Lane::MustFire, &label).await;
        assert_eq!(
            rows,
            vec![format!("{EX}s0"), format!("{EX}s2")],
            "{label}: self-loop"
        );

        // Rows whose object is unbound or a literal take the per-row path
        // inside the lane: UNDEF extends the row, a string matches no ref.
        let q = "SELECT ?x ?z WHERE {
            VALUES (?x ?z) { (ex:s0 ex:c0) (ex:s0 UNDEF) (ex:s1 \"c3\") (ex:sdup ex:c0) (ex:s1 ex:c17) }
            ?x ex:takesCourse ?z }";
        let rows = differential(&fluree, &db, Query::Sparql(q), Lane::MustFire, &label).await;
        let mut want: Vec<String> = vec![format!("{EX}s0|{EX}c0"), format!("{EX}sdup|{EX}c0")];
        want.extend(
            uni.takes
                .iter()
                .filter(|(x, _)| x == "s0")
                .map(|(x, z)| format!("{EX}{x}|{EX}{z}")),
        );
        want.sort();
        assert_eq!(rows, want, "{label}: unbound and literal objects");

        // Numeric objects keep the per-row path's term rules.
        let q = "SELECT ?z ?n WHERE {
            VALUES (?z ?n) { (ex:c0 3) (ex:c0 3.0) (ex:c1 4) (ex:c1 3) }
            ?z ex:credits ?n }";
        let rows = differential(&fluree, &db, Query::Sparql(q), Lane::MustFire, &label).await;
        assert!(
            rows.contains(&format!("3|{EX}c0")) && rows.contains(&format!("4|{EX}c1")),
            "{label}: numeric objects: {rows:?}"
        );
    }
}

/// List positions repeat a pair; each matching flake is a row, as the
/// per-row path emits them. (JSON-LD reads a list's members as values of its
/// predicate; SPARQL reads list cells.)
#[tokio::test(flavor = "current_thread")]
async fn list_positions_each_make_a_row() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "batched-pair/lists:main";
    let mut graph = vec![
        json!({"@id": "ex:f0", "@type": "ex:Faculty",
               "ex:teacherOf": [{"@id": "ex:c0"}, {"@id": "ex:c1"}, {"@id": "ex:c2"}]}),
        json!({"@id": "ex:plain", "@type": "ex:Student",
               "ex:advisor": {"@id": "ex:f0"}, "ex:takesCourse": {"@id": "ex:c2"}}),
    ];
    for k in 0..3 {
        graph.push(json!({"@id": format!("ex:c{k}"), "@type": "ex:Course"}));
    }
    const LISTED: usize = 10;
    for j in 0..LISTED {
        graph.push(json!({
            "@id": format!("ex:l{j}"),
            "@type": "ex:Student",
            "ex:advisor": {"@id": "ex:f0"},
            "ex:takesCourse": {"@list": [{"@id": "ex:c0"}, {"@id": "ex:c0"}, {"@id": "ex:c1"}]}
        }));
    }
    ledger_with(
        &fluree,
        id,
        &json!({"@context": {"ex": EX}, "@graph": graph}),
        None,
    )
    .await;
    let db = fluree.db(id).await.expect("db");
    let query = json!({
        "@context": {"ex": EX},
        "select": ["?x", "?y", "?z"],
        "where": [
            {"@id": "?x", "@type": "ex:Student", "ex:advisor": "?y", "ex:takesCourse": "?z"},
            {"@id": "?y", "@type": "ex:Faculty", "ex:teacherOf": "?z"},
            {"@id": "?z", "@type": "ex:Course"}
        ]
    });
    let rows = differential(&fluree, &db, Query::JsonLd(&query), Lane::MustFire, "lists").await;
    // Each listed student: c0 twice, c1 once; the plain student: c2 once.
    assert_eq!(rows.len(), LISTED * 3 + 1, "list rows: {rows:?}");
    let twice = rows
        .iter()
        .filter(|r| r.contains("ex:l0") && r.contains("ex:c0"))
        .count();
    assert_eq!(twice, 2, "two positions of one pair: {rows:?}");

    // The triangle may close on another predicate; seeding both ends makes
    // the listed predicate the probed one.
    let seeded = json!({
        "@context": {"ex": EX},
        "select": ["?x", "?z"],
        "values": [["?x", "?z"], [
            [{"@id": "ex:l0"}, {"@id": "ex:c0"}],
            [{"@id": "ex:l1"}, {"@id": "ex:c1"}],
            [{"@id": "ex:l2"}, {"@id": "ex:c2"}],
            [{"@id": "ex:plain"}, {"@id": "ex:c2"}]
        ]],
        "where": {"@id": "?x", "ex:takesCourse": "?z"}
    });
    let rows = differential(
        &fluree,
        &db,
        Query::JsonLd(&seeded),
        Lane::MustFire,
        "seeded lists",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            r#"["ex:l0","ex:c0"]"#,
            r#"["ex:l0","ex:c0"]"#,
            r#"["ex:l1","ex:c1"]"#,
            r#"["ex:plain","ex:c2"]"#,
        ],
        "seeded lists"
    );
}

/// Novelty over the index: a retracted base fact, a new fact, a re-asserted
/// fact and a subject minted after the index.
#[tokio::test(flavor = "current_thread")]
async fn novelty_asserts_and_retracts_merge_into_the_lane() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    for leaflet_rows in [None, Some(4)] {
        let label = format!("novelty, leaflet rows {leaflet_rows:?}");
        let mut uni = Uni::new();
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "batched-pair/novelty:main";
        ledger_with(&fluree, id, &uni.insert(), leaflet_rows).await;
        assert!(uni.triangles().contains(&format!("{EX}s3|{EX}f3|{EX}c9")));

        transact(
            &fluree,
            id,
            json!({
                "@context": {"ex": EX},
                "delete": {"@id": "ex:s3", "ex:takesCourse": {"@id": "ex:c9"}},
                "insert": [
                    {"@id": "ex:s2", "ex:takesCourse": {"@id": "ex:c7"}},
                    {"@id": "ex:s0", "ex:takesCourse": {"@id": "ex:c0"}},
                    {"@id": "ex:snew", "@type": "ex:Student", "ex:advisor": {"@id": "ex:f0"},
                     "ex:takesCourse": [{"@id": "ex:c0"}, {"@id": "ex:c1"}]}
                ]
            }),
        )
        .await;
        uni.takes.remove(&edge("s3", "c9"));
        uni.takes.insert(edge("s2", "c7"));
        uni.students.insert("snew".into());
        uni.advises.insert(edge("snew", "f0"));
        uni.takes.insert(edge("snew", "c0"));
        uni.takes.insert(edge("snew", "c1"));

        let db = fluree.db(id).await.expect("db");
        assert!(
            db.t > db.snapshot.t,
            "{label}: the changes must stay in novelty (t {}, index t {})",
            db.t,
            db.snapshot.t
        );
        let q = format!("SELECT ?x ?y ?z WHERE {{ {TRIANGLE} }}");
        let rows = differential(&fluree, &db, Query::Sparql(&q), Lane::MustFire, &label).await;
        assert_eq!(rows, uni.triangles(), "{label}");
    }
}

/// A view at an earlier `t` over a later index replays leaflet history.
#[tokio::test(flavor = "current_thread")]
async fn an_earlier_t_replays_the_index() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    let at_t1 = Uni::new();
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "batched-pair/history:main";
    ledger_with(&fluree, id, &at_t1.insert(), Some(4)).await;
    transact(
        &fluree,
        id,
        json!({
            "@context": {"ex": EX},
            "delete": {"@id": "ex:s3", "ex:takesCourse": {"@id": "ex:c9"}},
            "insert": {"@id": "ex:s2", "ex:takesCourse": {"@id": "ex:c7"}}
        }),
    )
    .await;
    reindex(&fluree, id, Some(4)).await;
    let mut at_t2 = at_t1.clone();
    at_t2.takes.remove(&edge("s3", "c9"));
    at_t2.takes.insert(edge("s2", "c7"));
    assert_ne!(at_t1.triangles(), at_t2.triangles());

    let q = format!("SELECT ?x ?y ?z WHERE {{ {TRIANGLE} }}");
    for (t, uni) in [(1, &at_t1), (2, &at_t2)] {
        let db = fluree.db_at_t(id, t).await.expect("db at t");
        let label = format!("t={t}");
        let rows = differential(&fluree, &db, Query::Sparql(&q), Lane::MustFire, &label).await;
        assert_eq!(rows, uni.triangles(), "{label}");
    }
}

/// A policy that restricts the probed predicate keeps the lane off; one
/// that restricts another predicate leaves it on.
#[tokio::test(flavor = "current_thread")]
async fn a_policy_on_the_predicate_declines_the_lane() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    let uni = Uni::new();
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "batched-pair/policy:main";
    ledger_with(&fluree, id, &uni.insert(), None).await;
    let deny = |props: &[&str]| {
        json!([{
            "@id": format!("{EX}deny"),
            "f:action": "f:view",
            "f:required": true,
            "f:onProperty": props.iter().map(|p| json!({"@id": format!("{EX}{p}")})).collect::<Vec<_>>(),
            "f:allow": false
        }])
    };
    let view = |policy: Value| {
        let fluree = &fluree;
        async move {
            let view = fluree
                .db_with_policy(
                    id,
                    &GovernanceOptions {
                        policy: Some(policy),
                        default_allow: Some(true),
                        ..Default::default()
                    },
                )
                .await
                .expect("db_with_policy");
            assert!(!view.is_root());
            view
        }
    };
    let q = format!("SELECT ?x ?y ?z WHERE {{ {TRIANGLE} }}");

    let restricted = view(deny(&["takesCourse", "teacherOf", "advisor"])).await;
    let rows = differential(
        &fluree,
        &restricted,
        Query::Sparql(&q),
        Lane::MustNotFire,
        "edges denied",
    )
    .await;
    assert!(rows.is_empty(), "edges denied: {rows:?}");

    let unrelated = view(deny(&["credits"])).await;
    let rows = differential(
        &fluree,
        &unrelated,
        Query::Sparql(&q),
        Lane::MustFire,
        "another predicate denied",
    )
    .await;
    assert_eq!(rows, uni.triangles(), "another predicate denied");
}

/// A replayed leaflet can hold rows its current constants do not describe: a
/// reference retracted at t=2 from a predicate whose remaining value is a
/// string, read back at t=1. One-row leaflets give each row its own type and
/// predicate constants.
#[tokio::test(flavor = "current_thread")]
async fn a_replayed_reference_matches_whatever_the_leaflet_now_holds() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    for leaflet_rows in [None, Some(1), Some(2), Some(4)] {
        let label = format!("mixed types, leaflet rows {leaflet_rows:?}");
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "batched-pair/mixed-history:main";
        ledger_with(
            &fluree,
            id,
            &json!({
                "@context": {"ex": EX},
                "@graph": [
                    {"@id": "ex:s", "ex:p": [{"@id": "ex:o"}, "still present"],
                     "ex:q": {"@id": "ex:o"}, "ex:r": {"@id": "ex:o"}},
                    {"@id": "ex:s2", "ex:label": "second subject", "ex:p": "s2 value"}
                ]
            }),
            leaflet_rows,
        )
        .await;
        transact(
            &fluree,
            id,
            json!({"@context": {"ex": EX}, "delete": {"@id": "ex:s", "ex:p": {"@id": "ex:o"}, "ex:r": {"@id": "ex:o"}}}),
        )
        .await;
        reindex(&fluree, id, leaflet_rows).await;

        // `ex:p` keeps a string; `ex:r` keeps nothing, so its restored row
        // sits among other predicates' rows.
        for pred in ["p", "r"] {
            let q = format!(
                "SELECT ?s ?o WHERE {{ VALUES (?s ?o) {{ (ex:s ex:o) (ex:s2 ex:s) }} ?s ex:{pred} ?o }}"
            );
            for (t, want) in [(1, vec![format!("{EX}o|{EX}s")]), (2, vec![])] {
                let db = fluree.db_at_t(id, t).await.expect("db at t");
                let label = format!("{label}, ex:{pred}, t={t}");
                let rows =
                    differential(&fluree, &db, Query::Sparql(&q), Lane::MustFire, &label).await;
                assert_eq!(rows, want, "{label}");
            }
        }
    }
}

/// Distant pairs open only the leaves holding them, not the leaves between.
#[tokio::test(flavor = "current_thread")]
async fn distant_pairs_open_only_their_leaves() {
    let _serial = SERIAL.lock().await;
    let _reset = Reset;
    const SUBJECTS: usize = 2_000;
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "batched-pair/sparse:main";
    let graph: Vec<Value> = (0..SUBJECTS)
        .map(|i| json!({"@id": format!("ex:s{i:04}"), "ex:p": {"@id": format!("ex:o{i:04}")}}))
        .collect();
    // Four-row leaflets, sixteen-row leaves: the predicate spans over a
    // hundred leaves.
    ledger_with(
        &fluree,
        id,
        &json!({"@context": {"ex": EX}, "@graph": graph}),
        Some(4),
    )
    .await;
    let db = fluree.db(id).await.expect("db");
    let last = SUBJECTS - 1;
    let q = format!(
        "SELECT ?s ?o WHERE {{ VALUES (?s ?o) {{ (ex:s0000 ex:o0000) (ex:s{last:04} ex:o{last:04}) }} ?s ex:p ?o }}"
    );

    let (store, guard) = init_test_tracing();
    let rows = sparql_rows(&fluree, &db, &format!("{PREFIX}{q}")).await;
    let flushes: Vec<(usize, usize)> = store
        .find_events("join batched pair flush complete")
        .iter()
        .map(|e| {
            let field = |name: &str| {
                e.fields
                    .get(name)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| panic!("flush event without {name}: {:?}", e.fields))
            };
            (field("leaves_opened"), field("leaflets_loaded"))
        })
        .collect();
    drop(guard);
    assert_eq!(rows.len(), 2, "both pairs match: {rows:?}");
    assert_eq!(flushes.len(), 1, "one flush: {flushes:?}");
    let (leaves, leaflets) = flushes[0];
    assert!(
        (1..=4).contains(&leaves) && (1..=4).contains(&leaflets),
        "two distant pairs opened {leaves} leaves and loaded {leaflets} leaflets"
    );
}
