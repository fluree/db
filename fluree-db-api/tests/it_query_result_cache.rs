//! Query-result cache: a hit returns what the uncached query returns for the
//! same request and ledger state, and a different state or request never
//! shares an entry.

use crate::support::{genesis_ledger, graphdb_from_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::{
    DropMode, FlureeBuilder, FormatterConfig, GovernanceOptions, GraphDb, ReindexOptions,
    ResultCacheOutcome, ResultCacheStatus,
};
use serde_json::{json, Value as JsonValue};

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\nPREFIX schema: <http://schema.org/>\n";

fn people(names: &[(&str, &str, i64)]) -> JsonValue {
    let graph: Vec<JsonValue> = names
        .iter()
        .map(|(id, name, age)| {
            json!({
                "@id": format!("ex:{id}"),
                "@type": "ex:Person",
                "schema:name": name,
                "ex:age": age,
                "ex:knows": {"@id": "ex:alice"}
            })
        })
        .collect();
    json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": graph
    })
}

async fn seeded(fluree: &MemoryFluree, id: &str, data: &JsonValue) -> MemoryLedger {
    fluree
        .insert(genesis_ledger(fluree, id), data)
        .await
        .expect("seed")
        .ledger
}

async fn uncached(fluree: &MemoryFluree, db: &GraphDb, sparql: &str) -> String {
    uncached_as(fluree, db, sparql, None).await
}

async fn cached(fluree: &MemoryFluree, db: &GraphDb, sparql: &str) -> (String, ResultCacheStatus) {
    cached_as(fluree, db, sparql, None).await
}

async fn uncached_as(
    fluree: &MemoryFluree,
    db: &GraphDb,
    sparql: &str,
    format: Option<FormatterConfig>,
) -> String {
    let request = format!("{PREFIX}{sparql}");
    let query = db.query(fluree).sparql(&request);
    let query = match format {
        Some(format) => query.format(format),
        None => query,
    };
    query
        .execute_formatted_string()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"))
}

async fn cached_as(
    fluree: &MemoryFluree,
    db: &GraphDb,
    sparql: &str,
    format: Option<FormatterConfig>,
) -> (String, ResultCacheStatus) {
    let outcome = ResultCacheOutcome::new();
    let request = format!("{PREFIX}{sparql}");
    let query = db.query(fluree).sparql(&request);
    let query = match format {
        Some(format) => query.format(format),
        None => query,
    };
    let text = query
        .result_cache(&outcome)
        .execute_formatted_string()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    (text, outcome.status().expect("the cache reports a status"))
}

#[tokio::test]
async fn a_hit_returns_the_uncached_answer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/hit:main",
        &people(&[
            ("alice", "Alice", 30),
            ("bob", "Bob", 25),
            ("cam", "Cam", 41),
        ]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);

    for sparql in [
        "SELECT ?s ?name WHERE { ?s schema:name ?name }",
        "SELECT ?name WHERE { ?s schema:name ?name ; ex:age ?age } ORDER BY DESC(?age) LIMIT 2",
        "SELECT ?p (COUNT(?s) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
        "SELECT ?s ?k WHERE { ?s a ex:Person OPTIONAL { ?s ex:knows ?k FILTER(?k != ?s) } }",
        "ASK { ex:bob ex:age 25 }",
    ] {
        let want = uncached(&fluree, &db, sparql).await;
        assert_eq!(
            cached(&fluree, &db, sparql).await,
            (want.clone(), ResultCacheStatus::Miss),
            "{sparql}"
        );
        assert_eq!(
            cached(&fluree, &db, sparql).await,
            (want, ResultCacheStatus::Hit),
            "{sparql}"
        );
    }

    // The JSON terminals keep the serialized DOM: a hit returns the same value,
    // and the same bytes `serde_json` writes for it.
    let jsonld = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "select": ["?name", "?age"],
        "where": {"@id": "?s", "schema:name": "?name", "ex:age": "?age"},
        "orderBy": "?age"
    });
    let want = db
        .query(&fluree)
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .expect("uncached JSON-LD");
    for expected in [ResultCacheStatus::Miss, ResultCacheStatus::Hit] {
        let outcome = ResultCacheOutcome::new();
        let got = db
            .query(&fluree)
            .jsonld(&jsonld)
            .result_cache(&outcome)
            .execute_formatted()
            .await
            .expect("cached JSON-LD");
        assert_eq!((got, outcome.status()), (want.clone(), Some(expected)));
    }
    let outcome = ResultCacheOutcome::new();
    let bytes = db
        .query(&fluree)
        .jsonld(&jsonld)
        .result_cache(&outcome)
        .execute_formatted_json_bytes()
        .await
        .expect("cached JSON-LD bytes");
    assert_eq!(outcome.status(), Some(ResultCacheStatus::Hit));
    assert_eq!(bytes, serde_json::to_vec(&want).unwrap());
}

#[tokio::test]
async fn opts_cache_opts_a_jsonld_query_in() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/opts:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);
    let query = json!({
        "@context": {"schema": "http://schema.org/"},
        "select": ["?name"],
        "where": {"@id": "?s", "schema:name": "?name"},
        "opts": {"cache": true}
    });

    let first = db
        .query(&fluree)
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("query");
    let outcome = ResultCacheOutcome::new();
    let second = db
        .query(&fluree)
        .jsonld(&query)
        .result_cache(&outcome)
        .execute_formatted()
        .await
        .expect("query");
    assert_eq!(outcome.status(), Some(ResultCacheStatus::Hit));
    assert_eq!(first, second);
}

#[tokio::test]
async fn a_commit_is_never_answered_from_the_state_before_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "cache/commit:main";
    let ledger = seeded(&fluree, id, &people(&[("alice", "Alice", 30)])).await;
    let sparql = "SELECT (COUNT(?s) AS ?n) WHERE { ?s a ex:Person }";

    let before = graphdb_from_ledger(&ledger);
    assert_eq!(
        cached(&fluree, &before, sparql).await.1,
        ResultCacheStatus::Miss
    );
    assert_eq!(
        cached(&fluree, &before, sparql).await.1,
        ResultCacheStatus::Hit
    );

    let ledger = fluree
        .insert(ledger, &people(&[("bob", "Bob", 25)]))
        .await
        .expect("insert")
        .ledger;
    let after = graphdb_from_ledger(&ledger);
    let want = uncached(&fluree, &after, sparql).await;
    assert!(want.contains("\"2\""), "{want}");
    assert_eq!(
        cached(&fluree, &after, sparql).await,
        (want, ResultCacheStatus::Miss)
    );
}

/// A ledger dropped and created again under its id restarts at `t` 1, so `t`
/// alone would serve the dropped ledger's answers.
#[tokio::test]
async fn a_recreated_ledger_at_the_same_t_misses() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "cache/recreate:main";
    let sparql = "SELECT ?name WHERE { ?s schema:name ?name }";

    let first = seeded(&fluree, id, &people(&[("alice", "Alice", 30)])).await;
    let first_db = graphdb_from_ledger(&first);
    assert_eq!(
        cached(&fluree, &first_db, sparql).await.1,
        ResultCacheStatus::Miss
    );

    fluree
        .drop_ledger("cache/recreate", DropMode::Hard)
        .await
        .expect("drop");
    let second = seeded(&fluree, id, &people(&[("zed", "Zed", 52)])).await;
    assert_eq!(first.t(), second.t());
    let second_db = graphdb_from_ledger(&second);
    let want = uncached(&fluree, &second_db, sparql).await;
    assert!(want.contains("Zed") && !want.contains("Alice"), "{want}");
    assert_eq!(
        cached(&fluree, &second_db, sparql).await,
        (want, ResultCacheStatus::Miss)
    );
}

/// Indexed, reloaded and with no novelty, a dropped ledger and the one
/// created again under its id agree on `t`, the index `t` and the overlay's
/// content version: only the index root tells their states apart.
#[tokio::test]
async fn a_reloaded_ledger_is_told_apart_by_its_index_root() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "cache/reload:main";
    let sparql = "SELECT ?name WHERE { ?s schema:name ?name }";

    async fn indexed_view(fluree: &MemoryFluree, id: &str, data: &JsonValue) -> GraphDb {
        seeded(fluree, id, data).await;
        fluree
            .reindex(id, ReindexOptions::default())
            .await
            .expect("reindex");
        fluree.disconnect_ledger(id).await;
        fluree.db(id).await.expect("reload")
    }

    let first = indexed_view(&fluree, id, &people(&[("alice", "Alice", 30)])).await;
    assert_eq!(
        cached(&fluree, &first, sparql).await.1,
        ResultCacheStatus::Miss
    );
    assert_eq!(
        cached(&fluree, &first, sparql).await.1,
        ResultCacheStatus::Hit
    );

    fluree
        .drop_ledger("cache/reload", DropMode::Hard)
        .await
        .expect("drop");
    let second = indexed_view(&fluree, id, &people(&[("zed", "Zed", 52)])).await;
    assert_eq!((first.t, first.snapshot.t), (second.t, second.snapshot.t));
    assert_eq!(
        first.overlay.content_version(),
        second.overlay.content_version()
    );
    assert_ne!(
        first.snapshot.index_root_id(),
        second.snapshot.index_root_id()
    );

    let want = uncached(&fluree, &second, sparql).await;
    assert!(want.contains("Zed") && !want.contains("Alice"), "{want}");
    assert_eq!(
        cached(&fluree, &second, sparql).await,
        (want, ResultCacheStatus::Miss)
    );
}

#[tokio::test]
async fn one_query_in_two_formats_keeps_two_answers() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/formats:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);
    let sparql = "SELECT ?name WHERE { ?s schema:name ?name }";

    let as_json = Some(FormatterConfig::sparql_json());
    assert_eq!(
        cached_as(&fluree, &db, sparql, as_json.clone()).await.1,
        ResultCacheStatus::Miss
    );
    assert_eq!(
        cached_as(&fluree, &db, sparql, as_json).await.1,
        ResultCacheStatus::Hit
    );

    let as_jsonld = Some(FormatterConfig::jsonld());
    let want = uncached_as(&fluree, &db, sparql, as_jsonld.clone()).await;
    assert_eq!(
        cached_as(&fluree, &db, sparql, as_jsonld).await,
        (want, ResultCacheStatus::Miss)
    );
}

/// Every staged view of a ledger reads at the same `t`, and so does the
/// commit that follows: only their overlays tell them apart.
#[tokio::test]
async fn staged_views_at_one_t_do_not_share_answers() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/staged:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let sparql = "SELECT ?name WHERE { ?s schema:name ?name }";

    let stage = |name: &'static str| {
        let ledger = ledger.clone();
        let fluree = &fluree;
        async move {
            fluree
                .stage_owned(ledger)
                .insert(&people(&[(name, name, 40)]))
                .stage()
                .await
                .expect("stage")
        }
    };
    let bob = stage("Bob").await;
    let carol = stage("Carol").await;
    let bob_db = GraphDb::from_staged(&bob).expect("bob view");
    let carol_db = GraphDb::from_staged(&carol).expect("carol view");
    assert_eq!(bob_db.t, carol_db.t);

    let bob_answer = uncached(&fluree, &bob_db, sparql).await;
    assert!(bob_answer.contains("Bob"), "{bob_answer}");
    assert_eq!(
        cached(&fluree, &bob_db, sparql).await,
        (bob_answer, ResultCacheStatus::Miss)
    );
    let carol_answer = uncached(&fluree, &carol_db, sparql).await;
    assert!(
        carol_answer.contains("Carol") && !carol_answer.contains("Bob"),
        "{carol_answer}"
    );
    assert_eq!(
        cached(&fluree, &carol_db, sparql).await,
        (carol_answer, ResultCacheStatus::Miss)
    );

    // Carol's insert committed: the ledger is at the staged views' `t`.
    let committed = fluree
        .insert(ledger, &people(&[("Carol", "Carol", 40)]))
        .await
        .expect("commit")
        .ledger;
    let committed_db = graphdb_from_ledger(&committed);
    assert_eq!(committed_db.t, bob_db.t);
    let want = uncached(&fluree, &committed_db, sparql).await;
    assert!(!want.contains("Bob"), "{want}");
    assert_eq!(
        cached(&fluree, &committed_db, sparql).await,
        (want, ResultCacheStatus::Miss)
    );
}

#[tokio::test]
async fn a_different_default_context_misses() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/context:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    // No @context: the view's default context compacts the answer.
    let query = json!({
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "http://example.org/ns/Person"}
    });
    let with_prefix = |prefix: &str| {
        graphdb_from_ledger(&ledger)
            .with_default_context(Some(json!({ prefix: "http://example.org/ns/" })))
    };
    let run = |db: GraphDb| {
        let fluree = &fluree;
        let query = &query;
        async move {
            let outcome = ResultCacheOutcome::new();
            let answer = db
                .query(fluree)
                .jsonld(query)
                .result_cache(&outcome)
                .execute_formatted()
                .await
                .expect("query");
            (answer, outcome.status().expect("status"))
        }
    };

    assert_eq!(
        run(with_prefix("ex")).await,
        (json!([["ex:alice"]]), ResultCacheStatus::Miss)
    );
    assert_eq!(
        run(with_prefix("pfx")).await,
        (json!([["pfx:alice"]]), ResultCacheStatus::Miss)
    );
}

/// A hit from `execute_formatted` parses the stored text back into a JSON
/// value, so the miss's doubles must survive it bit for bit. That holds only
/// while serde_json parses with `float_roundtrip`: without it, about one in
/// eight of these doubles reads back as a neighbour.
#[tokio::test]
async fn a_json_value_hit_returns_the_miss_doubles_exactly() {
    fn numbers(value: &JsonValue, out: &mut Vec<f64>) {
        match value {
            JsonValue::Number(n) => out.extend(n.as_f64()),
            JsonValue::Array(items) => items.iter().for_each(|v| numbers(v, out)),
            JsonValue::Object(map) => map.values().for_each(|v| numbers(v, out)),
            _ => {}
        }
    }
    async fn run(
        fluree: &MemoryFluree,
        db: &GraphDb,
        sparql: &str,
    ) -> (JsonValue, ResultCacheStatus) {
        let outcome = ResultCacheOutcome::new();
        let value = db
            .query(fluree)
            .sparql(sparql)
            .format(FormatterConfig::jsonld())
            .result_cache(&outcome)
            .execute_formatted()
            .await
            .expect("query");
        (value, outcome.status().expect("status"))
    }

    let fluree = FlureeBuilder::memory().build_memory();
    let graph: Vec<JsonValue> = (1..=200)
        .map(|i| json!({"@id": format!("ex:n{i}"), "ex:v": i}))
        .collect();
    let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": graph});
    let ledger = seeded(&fluree, "cache/doubles:main", &data).await;
    let db = graphdb_from_ledger(&ledger);
    let sparql = format!(
        "{PREFIX}SELECT ?x WHERE {{ ?s ex:v ?v BIND(?v / 7.0e0 * 1.1e0 AS ?x) }} ORDER BY ?x"
    );

    let (miss, status) = run(&fluree, &db, &sparql).await;
    assert_eq!(status, ResultCacheStatus::Miss);
    let mut doubles = Vec::new();
    numbers(&miss, &mut doubles);
    assert_eq!(doubles.len(), 200, "{miss}");

    let (hit, status) = run(&fluree, &db, &sparql).await;
    assert_eq!(status, ResultCacheStatus::Hit);
    assert!(hit == miss, "a hit changed the miss's doubles");
}

#[tokio::test]
async fn identities_with_different_policies_do_not_share_answers() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/identity:main",
        &json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "schema": "http://schema.org/",
                "f": "https://ns.flur.ee/db#"
            },
            "@graph": [
                {"@id": "ex:alice", "schema:name": "Alice", "schema:ssn": "111-11-1111"},
                {"@id": "ex:everything", "@type": ["f:AccessPolicy", "ex:FullPolicy"],
                 "f:action": {"@id": "f:view"}, "f:allow": true},
                {"@id": "ex:namesOnly", "@type": ["f:AccessPolicy", "ex:NamesPolicy"],
                 "f:onProperty": [{"@id": "schema:name"}],
                 "f:action": {"@id": "f:view"}, "f:allow": true},
                {"@id": "ex:admin", "f:policyClass": [{"@id": "ex:FullPolicy"}]},
                {"@id": "ex:guest", "f:policyClass": [{"@id": "ex:NamesPolicy"}]}
            ]
        }),
    )
    .await;
    let as_identity = |identity: &str| GovernanceOptions {
        identity: Some(format!("http://example.org/ns/{identity}")),
        default_allow: Some(false),
        ..Default::default()
    };
    let admin = fluree
        .wrap_policy(graphdb_from_ledger(&ledger), &as_identity("admin"))
        .await
        .expect("admin view");
    let guest = fluree
        .wrap_policy(graphdb_from_ledger(&ledger), &as_identity("guest"))
        .await
        .expect("guest view");
    let sparql = "SELECT ?p ?o WHERE { ex:alice ?p ?o }";

    let admin_answer = uncached(&fluree, &admin, sparql).await;
    let guest_answer = uncached(&fluree, &guest, sparql).await;
    assert!(admin_answer.contains("111-11-1111"), "{admin_answer}");
    assert!(!guest_answer.contains("111-11-1111"), "{guest_answer}");

    assert_eq!(
        cached(&fluree, &admin, sparql).await,
        (admin_answer.clone(), ResultCacheStatus::Miss)
    );
    assert_eq!(
        cached(&fluree, &guest, sparql).await,
        (guest_answer.clone(), ResultCacheStatus::Miss)
    );
    assert_eq!(
        cached(&fluree, &admin, sparql).await,
        (admin_answer, ResultCacheStatus::Hit)
    );
    assert_eq!(
        cached(&fluree, &guest, sparql).await,
        (guest_answer, ResultCacheStatus::Hit)
    );
}

#[tokio::test]
async fn reads_beyond_the_ledger_state_bypass() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/bypass:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);

    for (sparql, reason) in [
        ("SELECT (NOW() AS ?t) WHERE {}", "NOW()"),
        (
            "SELECT ?s WHERE { ?s a ex:Person } ORDER BY RAND()",
            "RAND()",
        ),
        (
            "SELECT ?s (UUID() AS ?u) WHERE { ?s a ex:Person }",
            "UUID()",
        ),
        (
            "SELECT ?s WHERE { ?s a ex:Person BIND(STRUUID() AS ?u) }",
            "STRUUID()",
        ),
        (
            "SELECT ?s WHERE { ?s a ex:Person BIND(BNODE() AS ?b) }",
            "BNODE()",
        ),
        (
            "SELECT ?s WHERE { { SELECT ?s WHERE { ?s a ex:Person FILTER(RAND() < 2) } } }",
            "RAND()",
        ),
        (
            "SELECT ?s WHERE { ?s a ex:Person FILTER EXISTS { ?s ex:age ?a FILTER(?a < RAND()) } }",
            "RAND()",
        ),
        (
            "SELECT ?s WHERE { SERVICE SILENT <http://example.org/sparql> { ?s ?p ?o } }",
            "remote or variable SERVICE",
        ),
    ] {
        let want = cached(&fluree, &db, sparql).await.1;
        assert_eq!(want, ResultCacheStatus::Bypass(reason), "{sparql}");
    }

    let reasoning = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });
    let outcome = ResultCacheOutcome::new();
    db.query(&fluree)
        .jsonld(&reasoning)
        .result_cache(&outcome)
        .execute_formatted()
        .await
        .expect("reasoning query");
    assert_eq!(
        outcome.status(),
        Some(ResultCacheStatus::Bypass("reasoning"))
    );

    let outcome = ResultCacheOutcome::new();
    db.query(&fluree)
        .sparql(&format!("{PREFIX}SELECT ?s WHERE {{ ?s a ex:Person }}"))
        .format(FormatterConfig::agent_json())
        .result_cache(&outcome)
        .execute_formatted()
        .await
        .expect("agent JSON query");
    assert_eq!(
        outcome.status(),
        Some(ResultCacheStatus::Bypass("agent JSON"))
    );
}

#[tokio::test]
async fn answers_take_no_memory_until_a_query_opts_in() {
    let fluree = FlureeBuilder::memory().cache_max_mb(1000).build_memory();
    assert_eq!(
        fluree.result_cache_budget_mb(),
        100,
        "10% of the cache budget"
    );
    assert_eq!(
        fluree.cache_budget_mb(),
        1000,
        "the index cache keeps its budget"
    );
    let ledger = seeded(
        &fluree,
        "cache/lazy:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);
    let select = "SELECT ?name WHERE { ?s schema:name ?name }";

    uncached(&fluree, &db, select).await;
    assert_eq!(
        fluree.result_cache_usage(),
        None,
        "no pool without an opt-in"
    );

    assert_eq!(
        cached(&fluree, &db, select).await.1,
        ResultCacheStatus::Miss
    );
    let (entries, bytes) = fluree
        .result_cache_usage()
        .expect("the opt-in built the pool");
    assert!(
        entries <= 1 && bytes <= 100 << 20,
        "{entries} entries, {bytes} bytes"
    );
    assert_eq!(cached(&fluree, &db, select).await.1, ResultCacheStatus::Hit);
}

#[tokio::test]
async fn a_zero_budget_disables_result_caching() {
    let fluree = FlureeBuilder::memory()
        .query_result_cache_max_mb(0)
        .build_memory();
    let ledger = seeded(
        &fluree,
        "cache/off:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);
    let select = "SELECT ?name WHERE { ?s schema:name ?name }";

    let expected = uncached(&fluree, &db, select).await;
    for _ in 0..2 {
        let (text, status) = cached(&fluree, &db, select).await;
        assert_eq!(text, expected);
        assert_eq!(status, ResultCacheStatus::Bypass("result cache disabled"));
    }
    assert_eq!(fluree.result_cache_usage(), None);
}

#[tokio::test]
async fn a_cancelled_request_is_not_answered_from_the_cache() {
    use fluree_db_api::{ApiError, QueryCancellation};
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seeded(
        &fluree,
        "cache/cancel:main",
        &people(&[("alice", "Alice", 30)]),
    )
    .await;
    let db = graphdb_from_ledger(&ledger);
    let select = "SELECT ?name WHERE { ?s schema:name ?name }";
    assert_eq!(
        cached(&fluree, &db, select).await.1,
        ResultCacheStatus::Miss
    );

    let cancelled = QueryCancellation::new();
    cancelled.cancel();
    let outcome = ResultCacheOutcome::new();
    let err = db
        .query(&fluree)
        .sparql(&format!("{PREFIX}{select}"))
        .cancellation(cancelled)
        .result_cache(&outcome)
        .execute_formatted_string()
        .await
        .expect_err("a cancelled request fails as it would uncached");
    assert!(
        matches!(
            err,
            ApiError::Query(fluree_db_query::QueryError::Cancelled { .. })
        ),
        "{err}"
    );
    assert_eq!(cached(&fluree, &db, select).await.1, ResultCacheStatus::Hit);
}
