//! What a query reads through the binary index, scope by scope
//! (`ExecutionContext::index_scope`): one graph, the union of several graphs
//! of one ledger, a default graph of none, or nothing an index lane may read.
//!
//! Observed through the `index scope` stamp that the query runner emits for
//! the query's scope (`site = "query"`) and the GRAPH operator for each named
//! graph it enters (`site = "graph"`). Each stamp reads
//! `site=scope/members/current`, where `current` says the index alone answers
//! for every member (no novelty, the index at the member's `t`).
//!
//! Its own `[[test]]` target, and its tests take turns (`take_turn`): run in
//! parallel threads beside another test setting and dropping its own capture
//! subscriber, a test here intermittently missed its first query's stamp.

#[path = "support/span_capture.rs"]
mod span_capture;

use fluree_db_api::dataset::GraphSelector;
use fluree_db_api::{
    DatasetSpec, Fluree, FlureeBuilder, GraphDb, GraphSource, LedgerHandle, ReindexOptions,
    TimeSpec,
};
use serde_json::{json, Value as JsonValue};
use span_capture::{init_test_tracing, SpanStore};

const LEDGER: &str = "it/index-scope:main";
const OTHER_LEDGER: &str = "it/index-scope-other:main";
const CONFIGURED: &str = "it/index-scope-configured:main";
const PREFIXES: &str = "PREFIX ex: <http://example.org/> ";
const UNION_PRAGMA: &str = "# PRAGMA union-default-graph: true\n";

/// One test's capture at a time; see the module comment.
async fn take_turn() -> tokio::sync::MutexGuard<'static, ()> {
    static TURN: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    TURN.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// A default graph and two named graphs, each with one triple of `ex:p`.
const TRIG: &str = r"
    @prefix ex: <http://example.org/> .
    ex:a ex:p 1 .
    GRAPH <urn:g1> { ex:b ex:p 2 . }
    GRAPH <urn:g2> { ex:c ex:p 3 . }
";

/// `ledger_id` holding `trig`, indexed, with no novelty.
async fn indexed(fluree: &Fluree, ledger_id: &str, trig: &str) -> LedgerHandle {
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("seed");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(ledger_id).await;
    fluree.ledger_cached(ledger_id).await.expect("reload")
}

/// A write to `urn:g2` after the index: novelty in one member.
async fn write_to_g2(fluree: &Fluree, handle: &LedgerHandle) {
    fluree
        .stage(handle)
        .upsert_turtle("@prefix ex: <http://example.org/> . GRAPH <urn:g2> { ex:d ex:p 4 . }")
        .execute()
        .await
        .expect("write to g2");
}

/// Every `index scope` stamp since `before`, as `site=scope/members/current`.
fn scope_stamps(store: &SpanStore, before: usize) -> Vec<String> {
    store.all_events()[before..]
        .iter()
        .filter(|e| e.message() == "index scope")
        .map(|e| {
            let field = |name: &str| e.fields.get(name).cloned().unwrap_or_default();
            let current = if field("current") == "true" {
                "current"
            } else {
                "stale"
            };
            format!(
                "{}={}/{}/{current}",
                field("site"),
                field("scope"),
                field("members")
            )
        })
        .collect()
}

/// The stamps of the query's own execution: its last `query` stamp (a ledger's
/// config is read through queries of its own first) and the `graph` stamps of
/// the scopes it entered, deduplicated (GRAPH enters a graph per row).
fn query_scope(stamps: &[String]) -> Vec<String> {
    let Some(start) = stamps.iter().rposition(|s| s.starts_with("query=")) else {
        return stamps.to_vec();
    };
    let mut seen: Vec<String> = Vec::new();
    for stamp in &stamps[start..] {
        if !seen.contains(stamp) {
            seen.push(stamp.clone());
        }
    }
    seen
}

/// How one case runs.
enum Run {
    /// SPARQL against `ledger`'s view; its `FROM`s name graphs within it.
    View(&'static str, String),
    /// A JSON-LD query through the connection; its `from` names the dataset.
    Connection(JsonValue),
}

struct Case {
    label: &'static str,
    run: Run,
    want: &'static [&'static str],
}

/// Run each case under span capture and compare its stamps.
async fn check(fluree: &Fluree, cases: Vec<Case>) {
    let mut failures: Vec<String> = Vec::new();
    for case in cases {
        let (store, guard) = init_test_tracing();
        let before = store.all_events().len();
        let result = match &case.run {
            Run::View(ledger_id, sparql) => {
                let handle = fluree.ledger_cached(ledger_id).await.expect("ledger");
                GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state())
                    .query(fluree)
                    .sparql(sparql)
                    .execute_formatted()
                    .await
                    .map(drop)
            }
            Run::Connection(query) => fluree.query_connection(query).await.map(drop),
        };
        let got = query_scope(&scope_stamps(&store, before));
        drop(guard);
        if let Err(e) = result {
            failures.push(format!("{}: query failed: {e}", case.label));
            continue;
        }
        if got != case.want {
            failures.push(format!(
                "{}: stamps {got:?}, want {:?}",
                case.label, case.want
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

fn sparql(body: &str) -> String {
    format!("{PREFIXES}{body}")
}

/// Every scope of design §5's table, through the surface that reaches it.
#[tokio::test(flavor = "current_thread")]
async fn index_scope_of_every_dataset_scope() {
    let _turn = take_turn().await;
    let fluree = FlureeBuilder::memory().build_memory();
    indexed(&fluree, LEDGER, TRIG).await;
    indexed(
        &fluree,
        OTHER_LEDGER,
        "@prefix ex: <http://example.org/> . ex:x ex:p 9 .",
    )
    .await;
    let config = format!(
        "@prefix f: <https://ns.flur.ee/db#> .
         GRAPH <urn:fluree:{CONFIGURED}#config> {{
           <urn:config:main> a f:LedgerConfig ; f:queryDefaults <urn:config:query> .
           <urn:config:query> f:unionDefaultGraph true . }}"
    );
    indexed(&fluree, CONFIGURED, &format!("{TRIG}\n{config}")).await;

    let g = |iri: &str| format!("{LEDGER}#{iri}");
    let policy = json!([
        {"@id": "ex:denyP", "@type": "f:AccessPolicy", "f:action": "f:view",
         "f:onProperty": [{"@id": "http://example.org/p"}], "f:allow": false},
        {"@id": "ex:allowAll", "@type": "f:AccessPolicy", "f:action": "f:view", "f:allow": true}
    ]);
    let select = |from: JsonValue, opts: Option<JsonValue>| {
        let mut q = json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?s"],
            "where": [{"@id": "?s", "ex:p": "?o"}]
        });
        if let Some(opts) = opts {
            q["opts"] = opts;
        }
        q
    };
    let one = &["query=one/1/current"];
    let cases = vec![
        Case {
            label: "the default graph",
            run: Run::View(LEDGER, sparql("SELECT ?s WHERE { ?s ex:p ?o }")),
            want: one,
        },
        Case {
            label: "FROM one graph",
            run: Run::View(
                LEDGER,
                sparql("SELECT ?s FROM <urn:g1> WHERE { ?s ex:p ?o }"),
            ),
            want: one,
        },
        Case {
            label: "FROM two graphs",
            run: Run::View(
                LEDGER,
                sparql("SELECT ?s FROM <urn:g1> FROM <urn:g2> WHERE { ?s ex:p ?o }"),
            ),
            want: &["query=union/2/current"],
        },
        Case {
            label: "FROM three graphs",
            run: Run::View(
                LEDGER,
                sparql(&format!(
                    "SELECT ?s FROM <{LEDGER}> FROM <urn:g1> FROM <urn:g2> \
                     WHERE {{ ?s ex:p ?o }}"
                )),
            ),
            want: &["query=union/3/current"],
        },
        Case {
            label: "FROM two graphs, through the connection",
            run: Run::Connection(select(json!([g("urn:g1"), g("urn:g2")]), None)),
            want: &["query=union/2/current"],
        },
        Case {
            label: "the union default graph, PRAGMA",
            run: Run::View(
                LEDGER,
                format!("{UNION_PRAGMA}{}", sparql("SELECT ?s WHERE { ?s ex:p ?o }")),
            ),
            want: &["query=union/3/current"],
        },
        Case {
            label: "the union default graph, opts",
            run: Run::Connection(select(
                json!(LEDGER),
                Some(json!({"unionDefaultGraph": true})),
            )),
            want: &["query=union/3/current"],
        },
        Case {
            label: "the union default graph, f:unionDefaultGraph",
            run: Run::View(CONFIGURED, sparql("SELECT ?s WHERE { ?s ex:p ?o }")),
            want: &["query=union/3/current"],
        },
        Case {
            label: "FROM NAMED alone",
            run: Run::View(
                LEDGER,
                sparql("SELECT ?s FROM NAMED <urn:g2> WHERE { ?s ex:p ?o }"),
            ),
            want: &["query=empty/0/current"],
        },
        Case {
            label: "FROM NAMED alone, GRAPH <g>",
            run: Run::View(
                LEDGER,
                sparql("SELECT ?s FROM NAMED <urn:g2> WHERE { GRAPH <urn:g2> { ?s ex:p ?o } }"),
            ),
            want: &["query=empty/0/current", "graph=one/1/current"],
        },
        Case {
            label: "GRAPH <g>",
            run: Run::View(
                LEDGER,
                sparql("SELECT ?s WHERE { GRAPH <urn:g2> { ?s ex:p ?o } }"),
            ),
            want: &["query=one/1/current", "graph=one/1/current"],
        },
        Case {
            label: "GRAPH ?g",
            run: Run::View(
                LEDGER,
                sparql("SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:p ?o } }"),
            ),
            want: &["query=one/1/current", "graph=one/1/current"],
        },
        Case {
            label: "GRAPH <g> inside FROM two graphs",
            run: Run::View(
                LEDGER,
                sparql(
                    "SELECT ?s FROM <urn:g1> FROM <urn:g2> FROM NAMED <urn:g2> \
                     WHERE { GRAPH <urn:g2> { ?s ex:p ?o } }",
                ),
            ),
            want: &["query=union/2/current", "graph=one/1/current"],
        },
        Case {
            label: "two ledgers",
            run: Run::Connection(select(json!([LEDGER, OTHER_LEDGER]), None)),
            want: &["query=none/0/stale"],
        },
        Case {
            label: "a history range",
            run: Run::Connection(json!({
                "@context": {"ex": "http://example.org/"},
                "from": format!("{LEDGER}@t:1"),
                "to": format!("{LEDGER}@t:latest"),
                "select": ["?s"],
                "where": [{"@id": "?s", "ex:p": "?o"}]
            })),
            want: &["query=none/0/stale"],
        },
        Case {
            label: "a filtered policy",
            run: Run::Connection(select(
                json!(LEDGER),
                Some(json!({"policy": policy.clone(), "default-allow": true})),
            )),
            want: &["query=none/0/stale"],
        },
        Case {
            label: "a filtered policy, FROM two graphs",
            run: Run::Connection(select(
                json!([g("urn:g1"), g("urn:g2")]),
                Some(json!({"policy": policy, "default-allow": true})),
            )),
            want: &["query=none/0/stale"],
        },
    ];
    check(&fluree, cases).await;
}

/// `FROM <L#g1> FROM <L#g2>`, read at `at` (`None`: the head).
fn from_two_graphs(at: Option<i64>) -> DatasetSpec {
    let source = |iri: &str| {
        let s = GraphSource::new(LEDGER).with_graph(GraphSelector::Iri(iri.into()));
        match at {
            Some(t) => s.with_time(TimeSpec::AtT(t)),
            None => s,
        }
    };
    DatasetSpec::new()
        .with_default(source("urn:g1"))
        .with_default(source("urn:g2"))
}

/// The query's scope stamps over the dataset `spec` builds.
async fn scope_of(fluree: &Fluree, spec: &DatasetSpec) -> Vec<String> {
    let (store, _guard) = init_test_tracing();
    let before = store.all_events().len();
    let dataset = fluree.build_dataset_view(spec).await.expect("dataset");
    let query = sparql("SELECT ?s WHERE { ?s ex:p ?o }");
    fluree.query_dataset(&dataset, &query).await.expect("query");
    query_scope(&scope_stamps(&store, before))
}

/// The members of `FROM <L#g1> FROM <L#g2>` share one store and dictionary
/// novelty however the ledger loads: at a `t` below the index (the historical
/// loader), with novelty in one member, and on a connection without the
/// ledger cache (as the CLI's is), where each source used to load an index of
/// its own and the scope was `none`.
#[tokio::test(flavor = "current_thread")]
async fn from_several_graphs_shares_one_load_of_the_ledger() {
    let _turn = take_turn().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    // No background indexing: it would index the write below before the
    // query reads it, and the "novelty in g2" row would see none.
    let fluree = FlureeBuilder::file(path.clone())
        .without_indexing()
        .build()
        .expect("build");
    let handle = indexed(&fluree, LEDGER, TRIG).await;
    let mut failures: Vec<String> = Vec::new();
    let mut expect = |label: &str, got: Vec<String>, want: &str| {
        if got != [want] {
            failures.push(format!("{label}: stamps {got:?}, want [{want:?}]"));
        }
    };

    expect(
        "at the head",
        scope_of(&fluree, &from_two_graphs(None)).await,
        "query=union/2/current",
    );

    write_to_g2(&fluree, &handle).await;
    expect(
        "novelty in g2",
        scope_of(&fluree, &from_two_graphs(None)).await,
        "query=union/2/stale",
    );

    // Index the write, then read the members at t 1, below the index.
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("index");
    expect(
        "at t 1, below the index",
        scope_of(&fluree, &from_two_graphs(Some(1))).await,
        "query=union/2/stale",
    );

    let uncached = FlureeBuilder::file(path)
        .without_ledger_caching()
        .without_indexing()
        .build()
        .expect("build uncached");
    expect(
        "without the ledger cache",
        scope_of(&uncached, &from_two_graphs(None)).await,
        "query=union/2/current",
    );
    expect(
        "without the ledger cache, at t 1",
        scope_of(&uncached, &from_two_graphs(Some(1))).await,
        "query=union/2/stale",
    );
    // One ledger spelled two ways (`it/index-scope` names the `main` branch).
    let spelled_twice = DatasetSpec::new()
        .with_default(
            GraphSource::new("it/index-scope").with_graph(GraphSelector::Iri("urn:g1".into())),
        )
        .with_default(GraphSource::new(LEDGER).with_graph(GraphSelector::Iri("urn:g2".into())));
    expect(
        "without the ledger cache, one ledger spelled two ways",
        scope_of(&uncached, &spelled_twice).await,
        "query=union/2/current",
    );
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
