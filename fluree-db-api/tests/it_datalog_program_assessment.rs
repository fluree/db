//! A windowed-activity program with an exception, end to end.
//!
//! An account becomes an `ex:Candidate` for review when it has at least
//! `ex:minCount` transactions within `ex:window` of the evaluation time, and
//! neither its holder nor any entity above the holder in the ownership
//! hierarchy carries an `ex:exception`. Ownership is recursive and partly
//! OWL entailments (`ex:subsidiaryOf rdfs:subPropertyOf ex:ownedBy`), the
//! count is an aggregate over the clock, and the exception test is a
//! negation: the program runs in strata over a finished OWL closure.
//!
//! Every state is compared with the answer ordinary queries give over the
//! asserted facts (`GROUP BY`, `FILTER NOT EXISTS`, a property path), and with
//! spelled-out expectations. The states are the ones a commit or the clock
//! changes: an exception added and removed, a transaction deleted, the clock
//! moving past a transaction with nothing committed, and a late transaction
//! dated inside the window.

use crate::support::{
    genesis_ledger, instances_sharing_storage, normalize_rows, query_jsonld, query_sparql,
    rebuild_and_publish_index, MemoryFluree, MemoryLedger,
};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
pub(crate) enum Surface {
    JsonLd,
    Sparql,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Storage {
    NoveltyOnly,
    Indexed,
}

pub(crate) const T0: &str = "2026-06-30T00:00:00Z";
/// Three days on: the window no longer reaches `ex:t2a`.
const T1: &str = "2026-07-03T00:00:00Z";

pub(crate) fn ctx() -> Value {
    json!({
        "ex": "http://example.org/",
        "f": "https://ns.flur.ee/db#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

fn at(time: &str) -> Value {
    json!({"@value": time, "@type": "xsd:dateTime"})
}

fn txn(id: &str, account: &str, time: &str) -> Value {
    json!({"@id": id, "ex:account": {"@id": account}, "ex:at": at(time)})
}

fn data() -> Vec<Value> {
    let mut graph = vec![
        json!({"@id": "ex:policy",
               "ex:window": {"@value": "P30D", "@type": "xsd:dayTimeDuration"},
               "ex:minCount": 3}),
        json!({"@id": "ex:subsidiaryOf", "rdfs:subPropertyOf": {"@id": "ex:ownedBy"}}),
        json!({"@id": "ex:opco", "ex:subsidiaryOf": {"@id": "ex:holdco"}}),
        json!({"@id": "ex:holdco", "ex:subsidiaryOf": {"@id": "ex:parent"}}),
        json!({"@id": "ex:parent", "ex:ownedBy": {"@id": "ex:group"}}),
        json!({"@id": "ex:acct1", "ex:holder": {"@id": "ex:opco"}}),
        json!({"@id": "ex:acct2", "ex:holder": {"@id": "ex:indie"}}),
        json!({"@id": "ex:acct3", "ex:holder": {"@id": "ex:holdco"}}),
        json!({"@id": "ex:acct4", "ex:holder": {"@id": "ex:indie"}}),
    ];
    graph.extend([
        txn("ex:t1a", "ex:acct1", "2026-04-01T09:00:00Z"),
        txn("ex:t1b", "ex:acct1", "2026-06-20T09:00:00Z"),
        txn("ex:t1c", "ex:acct1", "2026-06-25T09:00:00Z"),
        txn("ex:t1d", "ex:acct1", "2026-06-28T09:00:00Z"),
        txn("ex:t2a", "ex:acct2", "2026-06-02T09:00:00Z"),
        txn("ex:t2b", "ex:acct2", "2026-06-15T09:00:00Z"),
        txn("ex:t2c", "ex:acct2", "2026-06-25T09:00:00Z"),
        txn("ex:t4a", "ex:acct4", "2026-06-20T09:00:00Z"),
    ]);
    graph.extend(["10", "12", "14", "16", "18"].iter().map(|day| {
        txn(
            &format!("ex:t3{day}"),
            "ex:acct3",
            &format!("2026-06-{day}T09:00:00Z"),
        )
    }));
    graph
}

/// The program as JSON-LD rules, by rule id.
fn jsonld_rules() -> Vec<(&'static str, Value)> {
    vec![
        (
            "ex:withinOwner",
            json!({
                "where": {"@id": "?e", "ex:ownedBy": "?p"},
                "insert": {"@id": "?e", "ex:within": {"@id": "?p"}}
            }),
        ),
        (
            "ex:withinChain",
            json!({
                "where": [{"@id": "?e", "ex:within": "?p"}, {"@id": "?p", "ex:ownedBy": "?q"}],
                "insert": {"@id": "?e", "ex:within": {"@id": "?q"}}
            }),
        ),
        (
            "ex:coveredAtHolder",
            json!({
                "where": [{"@id": "?a", "ex:holder": "?h"}, {"@id": "?h", "ex:exception": "?x"}],
                "insert": {"@id": "?a", "ex:coveredBy": {"@id": "?x"}}
            }),
        ),
        (
            "ex:coveredAbove",
            json!({
                "where": [
                    {"@id": "?a", "ex:holder": "?h"},
                    {"@id": "?h", "ex:within": "?p"},
                    {"@id": "?p", "ex:exception": "?x"}
                ],
                "insert": {"@id": "?a", "ex:coveredBy": {"@id": "?x"}}
            }),
        ),
        (
            "ex:recentActivity",
            json!({
                "where": [["query", {
                    "@context": ctx(),
                    "select": ["?a", "(as (count ?t) ?n)"],
                    "where": [
                        {"@id": "ex:policy", "ex:window": "?w"},
                        {"@id": "?t", "ex:account": "?a", "ex:at": "?when"},
                        ["filter", "(> ?when (- (now) ?w))"]
                    ],
                    "groupBy": ["?a"]
                }]],
                "insert": {"@id": "?a", "ex:recentCount": "?n"}
            }),
        ),
        (
            "ex:assess",
            json!({
                "where": [
                    {"@id": "?a", "ex:recentCount": "?n"},
                    {"@id": "ex:policy", "ex:minCount": "?m"},
                    ["filter", "(>= ?n ?m)"],
                    ["not-exists", {"@id": "?a", "ex:coveredBy": "?x"}]
                ],
                "insert": {"@id": "?a", "@type": "ex:Candidate"}
            }),
        ),
    ]
}

const SPARQL_PREFIXES: &str = "PREFIX ex: <http://example.org/> \
                               PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

/// The same program as SPARQL rules.
fn sparql_rules() -> Vec<(&'static str, String)> {
    [
        (
            "ex:withinOwner",
            "CONSTRUCT { ?e ex:within ?p } WHERE { ?e ex:ownedBy ?p }",
        ),
        (
            "ex:withinChain",
            "CONSTRUCT { ?e ex:within ?q } WHERE { ?e ex:within ?p . ?p ex:ownedBy ?q }",
        ),
        (
            "ex:coveredAtHolder",
            "CONSTRUCT { ?a ex:coveredBy ?x } WHERE { ?a ex:holder ?h . ?h ex:exception ?x }",
        ),
        (
            "ex:coveredAbove",
            "CONSTRUCT { ?a ex:coveredBy ?x } WHERE { \
             ?a ex:holder ?h . ?h ex:within ?p . ?p ex:exception ?x }",
        ),
        (
            "ex:recentActivity",
            "CONSTRUCT { ?a ex:recentCount ?n } WHERE { \
             { SELECT ?a (COUNT(?t) AS ?n) WHERE { \
               ex:policy ex:window ?w . ?t ex:account ?a ; ex:at ?when \
               FILTER(?when > NOW() - ?w) } GROUP BY ?a } }",
        ),
        (
            "ex:assess",
            "CONSTRUCT { ?a a ex:Candidate } WHERE { \
             ?a ex:recentCount ?n . ex:policy ex:minCount ?m FILTER(?n >= ?m) \
             FILTER NOT EXISTS { ?a ex:coveredBy ?x } }",
        ),
    ]
    .into_iter()
    .map(|(id, body)| (id, format!("{SPARQL_PREFIXES}{body}")))
    .collect()
}

/// The program stored in the ledger, in the surface's rule syntax.
fn stored_rules(surface: Surface) -> Vec<Value> {
    match surface {
        Surface::JsonLd => jsonld_rules()
            .into_iter()
            .map(|(id, mut rule)| {
                rule["@context"] = ctx();
                json!({"@id": id, "f:rule": {"@type": "@json", "@value": rule}})
            })
            .collect(),
        Surface::Sparql => sparql_rules()
            .into_iter()
            .map(|(id, rule)| json!({"@id": id, "f:rule": {"@type": "f:sparql", "@value": rule}}))
            .collect(),
    }
}

/// A ledger holding the data and the stored program, loaded by a second
/// instance from novelty only or from a published index. Later commits land
/// in novelty, so an indexed ledger becomes a mixed one.
pub(crate) async fn load(
    ledger_id: &str,
    surface: Surface,
    storage: Storage,
) -> (MemoryFluree, MemoryLedger) {
    let (writer, reader) = instances_sharing_storage();
    let ledger = genesis_ledger(&writer, ledger_id);
    let mut graph = data();
    graph.extend(stored_rules(surface));
    writer
        .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
        .await
        .expect("seed");
    if let Storage::Indexed = storage {
        rebuild_and_publish_index(&writer, ledger_id).await;
    }
    let ledger = reader.ledger(ledger_id).await.expect("reader load");
    (reader, ledger)
}

/// Per-account recent counts and the candidates.
#[derive(Debug, PartialEq)]
struct Assessment {
    counts: Vec<Value>,
    candidates: Vec<Value>,
}

impl Assessment {
    fn expected(counts: &[(&str, i64)], candidates: &[&str]) -> Self {
        Self {
            counts: counts.iter().map(|(a, n)| json!([a, n])).collect(),
            candidates: candidates.iter().map(|a| json!(a)).collect(),
        }
    }
}

/// Rows sorted, with one-column rows unwrapped.
fn rows(result: fluree_db_api::QueryResult, ledger: &MemoryLedger) -> Vec<Value> {
    let rows = result.to_jsonld(&ledger.snapshot).expect("format");
    normalize_rows(&rows)
        .into_iter()
        .map(|row| match row {
            Value::Array(mut cols) if cols.len() == 1 => cols.remove(0),
            row => row,
        })
        .collect()
}

/// What the program concludes at `time`, asked on `surface`.
async fn program(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    surface: Surface,
    time: &str,
) -> Assessment {
    let mut answers = Vec::new();
    for (select, pattern) in [
        (
            json!(["?a", "?n"]),
            json!({"@id": "?a", "ex:recentCount": "?n"}),
        ),
        (json!(["?a"]), json!({"@id": "?a", "@type": "ex:Candidate"})),
    ] {
        let result = match surface {
            Surface::JsonLd => {
                let query = json!({
                    "@context": ctx(),
                    "select": select,
                    "where": pattern,
                    "reasoning": ["owl2rl", "datalog"],
                    "reasoningTime": time
                });
                query_jsonld(fluree, ledger, &query).await
            }
            Surface::Sparql => {
                let body = if pattern.get("@type").is_some() {
                    "SELECT ?a WHERE { ?a a ex:Candidate }"
                } else {
                    "SELECT ?a ?n WHERE { ?a ex:recentCount ?n }"
                };
                let sparql = format!(
                    "# PRAGMA reasoning: owl2rl, datalog\n# PRAGMA reasoning-time: {time}\n\
                     {SPARQL_PREFIXES}{body}"
                );
                query_sparql(fluree, ledger, &sparql).await
            }
        };
        answers.push(rows(result.expect("program query"), ledger));
    }
    let candidates = answers.pop().unwrap();
    let counts = answers.pop().unwrap();
    Assessment { counts, candidates }
}

/// The same conclusions from ordinary queries over the asserted facts: the
/// count is a `GROUP BY`, ownership a property path over both ownership
/// predicates, and the exception test a `FILTER NOT EXISTS`.
async fn oracle(fluree: &MemoryFluree, ledger: &MemoryLedger, time: &str) -> Assessment {
    let counts = format!(
        "SELECT ?a (COUNT(?t) AS ?n) WHERE {{ \
         ex:policy ex:window ?w . ?t ex:account ?a ; ex:at ?when \
         FILTER(?when > \"{time}\"^^xsd:dateTime - ?w) }} GROUP BY ?a"
    );
    let candidates = format!(
        "SELECT ?a WHERE {{ {{ {counts} }} \
         ex:policy ex:minCount ?m FILTER(?n >= ?m) \
         FILTER NOT EXISTS {{ ?a ex:holder ?h . ?h (ex:ownedBy|ex:subsidiaryOf)* ?p . \
                              ?p ex:exception ?x }} }}"
    );
    let ask = |query: String| async move {
        let result = query_sparql(fluree, ledger, &format!("{SPARQL_PREFIXES}{query}"))
            .await
            .expect("oracle query");
        rows(result, ledger)
    };
    Assessment {
        counts: ask(counts).await,
        candidates: ask(candidates).await,
    }
}

async fn assert_assessment(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    surface: Surface,
    time: &str,
    expected: Assessment,
    state: &str,
) {
    let concluded = program(fluree, ledger, surface, time).await;
    assert_eq!(
        oracle(fluree, ledger, time).await,
        expected,
        "oracle, {state}, {surface:?}"
    );
    assert_eq!(concluded, expected, "{state}, {surface:?}");
}

pub(crate) async fn commit(
    fluree: &MemoryFluree,
    ledger: MemoryLedger,
    update: Value,
) -> MemoryLedger {
    let mut update = update;
    update["@context"] = ctx();
    fluree.update(ledger, &update).await.expect("commit").ledger
}

async fn walk_through(surface: Surface, storage: Storage) {
    let ledger_id = format!("datalog/assessment-{surface:?}-{storage:?}:main").to_lowercase();
    let (fluree, ledger) = load(&ledger_id, surface, storage).await;
    let check = |ledger: &MemoryLedger, time: &'static str, expected: Assessment, state| {
        let fluree = &fluree;
        let ledger = ledger.clone();
        async move { assert_assessment(fluree, &ledger, surface, time, expected, state).await }
    };

    let all_counts = [
        ("ex:acct1", 3),
        ("ex:acct2", 3),
        ("ex:acct3", 5),
        ("ex:acct4", 1),
    ];
    check(
        &ledger,
        T0,
        Assessment::expected(&all_counts, &["ex:acct1", "ex:acct2", "ex:acct3"]),
        "baseline",
    )
    .await;

    // An exception two levels up, one of them an OWL entailment, covers
    // both accounts below it.
    let exception = json!({"@id": "ex:parent", "ex:exception": {"@id": "ex:waiver1"}});
    let ledger = commit(&fluree, ledger, json!({"insert": exception.clone()})).await;
    check(
        &ledger,
        T0,
        Assessment::expected(&all_counts, &["ex:acct2"]),
        "exception inserted",
    )
    .await;

    let ledger = commit(&fluree, ledger, json!({"delete": exception})).await;
    check(
        &ledger,
        T0,
        Assessment::expected(&all_counts, &["ex:acct1", "ex:acct2", "ex:acct3"]),
        "exception deleted",
    )
    .await;

    let ledger = commit(
        &fluree,
        ledger,
        json!({
            "where": {"@id": "ex:t1b", "?p": "?o"},
            "delete": {"@id": "ex:t1b", "?p": "?o"}
        }),
    )
    .await;
    let after_delete = [
        ("ex:acct1", 2),
        ("ex:acct2", 3),
        ("ex:acct3", 5),
        ("ex:acct4", 1),
    ];
    check(
        &ledger,
        T0,
        Assessment::expected(&after_delete, &["ex:acct2", "ex:acct3"]),
        "contributing transaction deleted",
    )
    .await;

    // Nothing is committed; the clock alone moves `ex:t2a` out of the window.
    let t = ledger.t();
    let after_tick = [
        ("ex:acct1", 2),
        ("ex:acct2", 2),
        ("ex:acct3", 5),
        ("ex:acct4", 1),
    ];
    check(
        &ledger,
        T1,
        Assessment::expected(&after_tick, &["ex:acct3"]),
        "clock tick",
    )
    .await;
    assert_eq!(ledger.t(), t);

    // A transaction recorded late, but dated inside the window, counts.
    let ledger = commit(
        &fluree,
        ledger,
        json!({"insert": txn("ex:t2d", "ex:acct2", "2026-07-01T09:00:00Z")}),
    )
    .await;
    let after_late = [
        ("ex:acct1", 2),
        ("ex:acct2", 3),
        ("ex:acct3", 5),
        ("ex:acct4", 1),
    ];
    check(
        &ledger,
        T1,
        Assessment::expected(&after_late, &["ex:acct2", "ex:acct3"]),
        "late transaction",
    )
    .await;
}

#[tokio::test]
async fn json_ld_program_follows_commits_and_the_clock() {
    walk_through(Surface::JsonLd, Storage::NoveltyOnly).await;
}

#[tokio::test]
async fn json_ld_program_follows_commits_and_the_clock_over_an_index() {
    walk_through(Surface::JsonLd, Storage::Indexed).await;
}

#[tokio::test]
async fn sparql_program_follows_commits_and_the_clock() {
    walk_through(Surface::Sparql, Storage::NoveltyOnly).await;
}

#[tokio::test]
async fn sparql_program_follows_commits_and_the_clock_over_an_index() {
    walk_through(Surface::Sparql, Storage::Indexed).await;
}

/// Every fact budget either answers in full or fails. With the exception in
/// place, a budget that stopped the OWL pass, the ownership recursion or the
/// coverage rules early would leave `ex:acct1` and `ex:acct3` looking
/// uncovered, and one that stopped the count early would leave counts low;
/// neither may be answered from.
#[tokio::test]
async fn no_budget_answers_from_a_partial_run() {
    let ledger_id = "datalog/assessment-budgets:main";
    let (fluree, ledger) = load(ledger_id, Surface::JsonLd, Storage::NoveltyOnly).await;
    let ledger = commit(
        &fluree,
        ledger,
        json!({"insert": {"@id": "ex:parent", "ex:exception": {"@id": "ex:waiver1"}}}),
    )
    .await;
    let complete = Assessment::expected(
        &[
            ("ex:acct1", 3),
            ("ex:acct2", 3),
            ("ex:acct3", 5),
            ("ex:acct4", 1),
        ],
        &["ex:acct2"],
    );
    let ask = |max_facts: usize, select: Value, pattern: Value| {
        let query = json!({
            "@context": ctx(),
            "select": select,
            "where": pattern,
            "reasoning": ["owl2rl", "datalog"],
            "reasoningTime": T0,
            "reasoningBudget": {"maxFacts": max_facts}
        });
        let (fluree, ledger) = (&fluree, &ledger);
        async move {
            query_jsonld(fluree, ledger, &query)
                .await
                .map(|result| rows(result, ledger))
        }
    };

    let mut failures: Vec<String> = Vec::new();
    let mut answered_from = None;
    for max_facts in 1..200 {
        let counts = ask(
            max_facts,
            json!(["?a", "?n"]),
            json!({"@id": "?a", "ex:recentCount": "?n"}),
        )
        .await;
        let candidates = ask(
            max_facts,
            json!(["?a"]),
            json!({"@id": "?a", "@type": "ex:Candidate"}),
        )
        .await;
        match (counts, candidates) {
            (Ok(counts), Ok(candidates)) => {
                assert_eq!(
                    Assessment { counts, candidates },
                    complete,
                    "maxFacts {max_facts}"
                );
                answered_from.get_or_insert(max_facts);
            }
            (Err(e), Err(_)) => {
                assert!(
                    answered_from.is_none(),
                    "maxFacts {max_facts} failed after a smaller budget answered: {e}"
                );
                failures.push(e.to_string());
            }
            (counts, candidates) => panic!(
                "maxFacts {max_facts}: the two queries disagree: {counts:?} / {candidates:?}"
            ),
        }
    }
    assert!(answered_from.is_some(), "no budget answered");
    // The failures name what stopped and which rule could not run.
    for needle in [
        "OWL 2 RL materialization hit its budget (facts)",
        "rule http://example.org/recentActivity could finish. It uses an aggregate",
        "rule http://example.org/assess could finish. It uses negation",
    ] {
        assert!(
            failures.iter().any(|f| f.contains(needle)),
            "no failure mentions {needle:?}: {failures:#?}"
        );
    }
}

/// Exempting every account that is not a candidate, while candidacy needs no
/// exemption, has no stratification. The error names the rules on the cycle
/// and the facts negated.
#[tokio::test]
async fn an_exemption_that_negates_candidacy_is_a_negative_cycle() {
    for surface in [Surface::JsonLd, Surface::Sparql] {
        let ledger_id = format!("datalog/assessment-cycle-{surface:?}:main").to_lowercase();
        let (fluree, ledger) = load(&ledger_id, surface, Storage::NoveltyOnly).await;
        let exempt = match surface {
            Surface::JsonLd => json!({
                "@id": "ex:exemptOthers",
                "f:rule": {"@type": "@json", "@value": {
                    "@context": ctx(),
                    "where": [
                        {"@id": "?a", "ex:holder": "?h"},
                        ["not-exists", {"@id": "?a", "@type": "ex:Candidate"}]
                    ],
                    "insert": {"@id": "?a", "ex:coveredBy": {"@id": "ex:standing"}}
                }}
            }),
            Surface::Sparql => json!({
                "@id": "ex:exemptOthers",
                "f:rule": {"@type": "f:sparql", "@value": format!(
                    "{SPARQL_PREFIXES}CONSTRUCT {{ ?a ex:coveredBy ex:standing }} WHERE {{ \
                     ?a ex:holder ?h FILTER NOT EXISTS {{ ?a a ex:Candidate }} }}"
                )}
            }),
        };
        let ledger = commit(&fluree, ledger, json!({"insert": exempt})).await;
        let err = query_jsonld(
            &fluree,
            &ledger,
            &json!({
                "@context": ctx(),
                "select": ["?a"],
                "where": {"@id": "?a", "@type": "ex:Candidate"},
                "reasoning": ["owl2rl", "datalog"],
                "reasoningTime": T0
            }),
        )
        .await
        .expect_err("no stratification exists");
        let message = err.to_string();
        assert!(
            message.contains(
                "datalog rule http://example.org/assess negates http://example.org/coveredBy \
                 facts, which depend on its own conclusions (http://example.org/assess -not-> \
                 http://example.org/exemptOthers -> http://example.org/assess;"
            ),
            "{surface:?}: {message}"
        );
    }
}

/// Stored rules are data: a new instance over the same storage concludes the
/// same, and so does a ledger whose index was rebuilt from its commits. The
/// reasoning cache is process-wide and keyed by the requested time, so the
/// reopened ledger is asked about a time the first instance never was, which
/// makes it load the rules from storage.
#[cfg(feature = "native")]
#[tokio::test]
async fn stored_rules_survive_a_restart_and_a_reindex() {
    use fluree_db_api::ReindexOptions;

    let ledger_id = "datalog/assessment-restart:main";
    let dir = tempfile::tempdir().expect("tempdir");
    let open = || {
        fluree_db_api::FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .expect("build")
    };
    let expected = || {
        Assessment::expected(
            &[
                ("ex:acct1", 3),
                ("ex:acct2", 3),
                ("ex:acct3", 5),
                ("ex:acct4", 1),
            ],
            &["ex:acct1", "ex:acct2", "ex:acct3"],
        )
    };

    {
        let fluree = open();
        let ledger = fluree.create_ledger(ledger_id).await.expect("create");
        let mut graph = data();
        graph.extend(stored_rules(Surface::Sparql));
        let ledger = fluree
            .insert(ledger, &json!({"@context": ctx(), "@graph": graph}))
            .await
            .expect("seed")
            .ledger;
        assert_assessment(
            &fluree,
            &ledger,
            Surface::Sparql,
            T0,
            expected(),
            "first process",
        )
        .await;
    }

    let fluree = open();
    let ledger = fluree.ledger(ledger_id).await.expect("reopen");
    assert_assessment(
        &fluree,
        &ledger,
        Surface::Sparql,
        T1,
        Assessment::expected(
            &[
                ("ex:acct1", 3),
                ("ex:acct2", 2),
                ("ex:acct3", 5),
                ("ex:acct4", 1),
            ],
            &["ex:acct1", "ex:acct3"],
        ),
        "after restart",
    )
    .await;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    assert!(ledger.index_t() > 0, "the reindex published an index");
    assert_assessment(
        &fluree,
        &ledger,
        Surface::Sparql,
        T0,
        expected(),
        "after reindex",
    )
    .await;
}
