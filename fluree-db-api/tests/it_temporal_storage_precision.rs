//! A `dateTime` or `time` is held to the microsecond from the moment it is
//! read, because a commit writes six fractional digits and the index keys
//! microseconds. So the value a write holds is the value stored: equal to its
//! six-digit form before the ledger is reloaded and after, and a DELETE that
//! names it as written finds it either way. Every write lane reads its
//! literals through the same parse, and a query constant does too: it
//! compares as the value written with it, for equality and ordering alike.

use crate::support;
use fluree_db_api::{
    CommitOpts, ExportCommitsRequest, FlureeBuilder, GovernanceOptions, IndexConfig,
    PushCommitsRequest, SyncGraphOpts, TransactionOptions, TxnOperation,
};
use serde_json::{json, Value as JsonValue};

const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
const TTL_PFX: &str = "@prefix ex: <http://example.org/ns/> .\n\
                       @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
const G: &str = "http://example.org/ns/g";
const DT_NS: &str = "2020-01-01T00:00:00.123456789Z";
const DT_US: &str = "2020-01-01T00:00:00.123456Z";
const TIME_NS: &str = "12:00:00.123456789";
const TIME_US: &str = "12:00:00.123456";
const LEDGER: &str = "it/temporal-precision:main";

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/ns/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

fn wrap(named: bool, triples: &str) -> String {
    if named {
        format!("GRAPH <{G}> {{ {triples} }}")
    } else {
        triples.to_string()
    }
}

/// `ex:s ex:d <dateTime> ; ex:t <time>`, as SPARQL/Turtle triples.
fn triples(dt: &str, time: &str) -> String {
    format!("ex:s ex:d \"{dt}\"^^xsd:dateTime ; ex:t \"{time}\"^^xsd:time .")
}

fn node() -> JsonValue {
    json!({
        "@id": "ex:s",
        "ex:d": {"@value": DT_NS, "@type": "xsd:dateTime"},
        "ex:t": {"@value": TIME_NS, "@type": "xsd:time"}
    })
}

/// `fluree`'s answer to an ASK, read from the ledger as that instance holds
/// it: the writing instance's own novelty, or a reloaded one.
async fn ask(fluree: &fluree_db_api::Fluree, q: &str) -> bool {
    let db = fluree.db(LEDGER).await.expect("db");
    let json = fluree
        .query(&db, q)
        .await
        .expect("ask")
        .to_sparql_json(&db.snapshot)
        .expect("json");
    json["boolean"].as_bool().expect("boolean")
}

/// Whether `fluree` holds both values at their six-digit form, matched as a
/// bound object (the index's and novelty's lookup by value) and by `FILTER`
/// equality (a comparison of the values).
async fn holds_microsecond_values(fluree: &fluree_db_api::Fluree, named: bool) -> (bool, bool) {
    let bound = format!("{PFX}ASK {{ {} }}", wrap(named, &triples(DT_US, TIME_US)));
    let filtered = format!(
        "{PFX}ASK {{ {} FILTER(?d = \"{DT_US}\"^^xsd:dateTime && ?t = \"{TIME_US}\"^^xsd:time) }}",
        wrap(named, "ex:s ex:d ?d ; ex:t ?t .")
    );
    (ask(fluree, &bound).await, ask(fluree, &filtered).await)
}

/// Write `ex:s ex:d DT_NS ; ex:t TIME_NS` through `lane` on a fresh ledger at
/// `path` (in `ex:g` for the named-graph lanes). Returns the writing
/// instance, and whether the lane writes in the named graph.
async fn write(lane: &str, path: &str) -> (fluree_db_api::Fluree, bool) {
    let fluree = FlureeBuilder::file(path.to_string())
        .build()
        .expect("build");
    let graph = fluree.graph(LEDGER);
    let named = matches!(lane, "TriG insert" | "graph sync");
    if lane == "bulk import" {
        let dir = tempfile::tempdir().expect("data dir");
        let ttl = dir.path().join("data.ttl");
        std::fs::write(&ttl, format!("{TTL_PFX}{}\n", triples(DT_NS, TIME_NS))).expect("ttl");
        fluree
            .create(LEDGER)
            .import(&ttl)
            .cleanup(false)
            .execute()
            .await
            .expect("import");
        return (fluree, named);
    }
    fluree.create_ledger(LEDGER).await.expect("create");
    match lane {
        "SPARQL INSERT DATA" => {
            let update = format!("{PFX}INSERT DATA {{ {} }}", triples(DT_NS, TIME_NS));
            graph
                .transact()
                .sparql_update(&update)
                .commit()
                .await
                .map(drop)
        }
        "JSON-LD insert" => {
            let body = json!({"@context": ctx(), "@graph": [node()]});
            graph.transact().insert(&body).commit().await.map(drop)
        }
        "JSON-LD upsert" => {
            let body = json!({"@context": ctx(), "@graph": [node()]});
            graph.transact().upsert(&body).commit().await.map(drop)
        }
        "Turtle insert" => {
            let ttl = format!("{TTL_PFX}{}\n", triples(DT_NS, TIME_NS));
            graph
                .transact()
                .insert_turtle(&ttl)
                .commit()
                .await
                .map(drop)
        }
        "Turtle upsert" => {
            let ttl = format!("{TTL_PFX}{}\n", triples(DT_NS, TIME_NS));
            graph
                .transact()
                .upsert_turtle(&ttl)
                .commit()
                .await
                .map(drop)
        }
        "TriG insert" => {
            let trig = format!("{TTL_PFX}GRAPH <{G}> {{ {} }}\n", triples(DT_NS, TIME_NS));
            graph
                .transact()
                .insert_turtle(&trig)
                .commit()
                .await
                .map(drop)
        }
        "graph sync" => {
            let body = json!({"@context": ctx(), "@graph": [node()]});
            fluree
                .sync_named_graph(LEDGER, G, &body, SyncGraphOpts::default())
                .await
                .map(drop)
        }
        "Transaction" => {
            let body = json!({"@context": ctx(), "@graph": [node()]});
            let mut txn = fluree
                .begin_transaction(LEDGER, TransactionOptions::default())
                .await
                .expect("begin");
            txn.stage(TxnOperation::Insert(body)).await.expect("stage");
            txn.commit(CommitOpts::default()).await.map(drop)
        }
        "push" => {
            let source = "it/temporal-precision-source:main";
            fluree.create_ledger(source).await.expect("source");
            let update = format!("{PFX}INSERT DATA {{ {} }}", triples(DT_NS, TIME_NS));
            fluree
                .graph(source)
                .transact()
                .sparql_update(&update)
                .commit()
                .await
                .expect("source write");
            let handle = fluree.ledger_cached(source).await.expect("handle");
            let export = fluree
                .export_commit_range(
                    &handle,
                    &ExportCommitsRequest {
                        limit: Some(10),
                        ..Default::default()
                    },
                )
                .await
                .expect("export");
            let mut commits = export.commits;
            commits.reverse();
            fluree
                .push_commits(
                    LEDGER,
                    PushCommitsRequest {
                        commits,
                        blobs: export.blobs,
                        missing_blobs: Vec::new(),
                        merged_commits: Vec::new(),
                    },
                    &GovernanceOptions::default(),
                    &IndexConfig {
                        reindex_min_bytes: 1_000_000_000,
                        reindex_max_bytes: 2_000_000_000,
                    },
                )
                .await
                .map(drop)
        }
        other => panic!("unknown lane {other}"),
    }
    .unwrap_or_else(|e| panic!("{lane}: {e}"));
    (fluree, named)
}

/// Every write lane holds a `dateTime` and a `time` written past the
/// microsecond at their six-digit form, before a reload and after one, and
/// a `DELETE DATA` naming them as written deletes them after the reload (it
/// committed nothing while a reloaded ledger held six digits and the write
/// held nine).
#[tokio::test]
async fn every_write_lane_holds_a_temporal_to_the_microsecond() {
    let lanes = [
        "SPARQL INSERT DATA",
        "JSON-LD insert",
        "JSON-LD upsert",
        "Turtle insert",
        "Turtle upsert",
        "TriG insert",
        "graph sync",
        "Transaction",
        "push",
        "bulk import",
    ];
    let mut failures = Vec::new();
    for lane in lanes {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let (writer, named) = write(lane, &path).await;
        let before = holds_microsecond_values(&writer, named).await;
        drop(writer);
        let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
        let after = holds_microsecond_values(&fluree, named).await;
        let delete = format!(
            "{PFX}DELETE DATA {{ {} }}",
            wrap(named, &triples(DT_NS, TIME_NS))
        );
        let retracted = fluree
            .graph(LEDGER)
            .transact()
            .sparql_update(&delete)
            .commit()
            .await
            .map(|r| r.receipt.retract_count)
            .unwrap_or_else(|e| panic!("{lane}: delete: {e}"));
        if (before, after, retracted) != ((true, true), (true, true), 2) {
            failures.push(format!(
                "{lane}: (bound, filter) before reload {before:?}, after {after:?}; \
                 DELETE DATA as written after the reload retracted {retracted}, expected 2"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The same `DELETE DATA` on the instance that wrote the values, with no
/// reload between.
#[tokio::test]
async fn a_delete_as_written_before_a_reload_deletes_the_temporal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let graph = fluree.graph(LEDGER);
    let insert = format!("{PFX}INSERT DATA {{ {} }}", triples(DT_NS, TIME_NS));
    graph
        .transact()
        .sparql_update(&insert)
        .commit()
        .await
        .expect("insert");
    let delete = format!("{PFX}DELETE DATA {{ {} }}", triples(DT_NS, TIME_NS));
    let receipt = graph
        .transact()
        .sparql_update(&delete)
        .commit()
        .await
        .expect("delete")
        .receipt;
    assert_eq!(receipt.retract_count, 2);
}

/// A Cypher write's temporal constructors read their literal the same way:
/// the stored values equal their six-digit form, before a reload and after.
#[tokio::test]
async fn a_cypher_write_holds_a_temporal_to_the_microsecond() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let count = |fluree: fluree_db_api::Fluree, ledger: fluree_db_api::LedgerState| async move {
        let db = support::graphdb_from_ledger(&ledger);
        let query = format!(
            "MATCH (e:Event) WHERE e.at = datetime(\"{DT_US}\") AND e.t = time(\"{TIME_US}\") \
             RETURN count(e)"
        );
        fluree
            .query_cypher(&db, &query)
            .await
            .expect("query")
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld")
    };
    {
        let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
        let ledger = fluree.create_ledger(LEDGER).await.expect("create");
        let create =
            format!("CREATE (:Event {{at: datetime(\"{DT_NS}\"), t: time(\"{TIME_NS}\")}})");
        // The ledger the write returns holds its values as the write made them.
        let written = fluree
            .transact_cypher(ledger, &create)
            .await
            .expect("create")
            .ledger;
        assert_eq!(
            count(fluree.clone(), written).await,
            json!([[1]]),
            "before a reload"
        );
    }
    let fluree = FlureeBuilder::file(path).build().expect("reload");
    let ledger = fluree.ledger(LEDGER).await.expect("ledger");
    assert_eq!(count(fluree, ledger).await, json!([[1]]), "after a reload");
}

/// A query constant past the microsecond is read to the microsecond too, so
/// it matches the value stored for the same literal, whatever holds it:
/// the writing instance's novelty, a reloaded ledger, or the index.
#[tokio::test]
async fn a_query_constant_past_the_microsecond_matches_the_stored_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let asks = |fluree: fluree_db_api::Fluree| async move {
        let mut out = Vec::new();
        for q in [
            format!("{PFX}ASK {{ {} }}", triples(DT_NS, TIME_NS)),
            format!(
                "{PFX}ASK {{ ex:s ex:d ?d ; ex:t ?t \
                 FILTER(?d = \"{DT_NS}\"^^xsd:dateTime && ?t = \"{TIME_NS}\"^^xsd:time) }}"
            ),
        ] {
            out.push(ask(&fluree, &q).await);
        }
        out
    };
    let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let insert = format!("{PFX}INSERT DATA {{ {} }}", triples(DT_NS, TIME_NS));
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update(&insert)
        .commit()
        .await
        .expect("insert");
    let in_process = asks(fluree.clone()).await;
    let reloaded = asks(FlureeBuilder::file(path.clone()).build().expect("reload")).await;
    support::rebuild_and_publish_index(&fluree, LEDGER).await;
    let indexed = asks(FlureeBuilder::file(path).build().expect("reload")).await;
    assert_eq!(
        (in_process, reloaded, indexed),
        (vec![true, true], vec![true, true], vec![true, true]),
        "(bound object, FILTER equality) in process, reloaded, indexed"
    );
}

/// An identical re-upsert of a temporal written past the microsecond, after
/// a never-indexed ledger is reloaded, commits nothing and keeps the value,
/// and the next index build agrees.
#[tokio::test]
async fn an_identical_reupsert_after_a_reload_keeps_the_temporal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let body = json!({"@context": ctx(), "@graph": [node()]});
    {
        let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
        fluree.create_ledger(LEDGER).await.expect("create");
        fluree
            .graph(LEDGER)
            .transact()
            .upsert(&body)
            .commit()
            .await
            .expect("upsert");
    }
    let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
    let receipt = fluree
        .graph(LEDGER)
        .transact()
        .upsert(&body)
        .commit()
        .await
        .expect("re-upsert")
        .receipt;
    let after = holds_microsecond_values(&fluree, false).await;
    support::rebuild_and_publish_index(&fluree, LEDGER).await;
    let indexed =
        holds_microsecond_values(&FlureeBuilder::file(path).build().expect("reload"), false).await;
    assert_eq!(
        (receipt.flake_count, after, indexed),
        (0, (true, true), (true, true)),
        "flakes committed, values held after the re-upsert, after the index build"
    );
}

/// A constant just past the stored value's microsecond, and the time-of-day
/// one: each is held as the stored `DT_US` / `TIME_US`.
const DT_ABOVE: &str = "2020-01-01T00:00:00.1234569Z";
const TIME_ABOVE: &str = "12:00:00.1234569";
/// Constants a microsecond either side of `DT_US`.
const DT_NEXT: &str = "2020-01-01T00:00:00.123457Z";
const DT_PREV: &str = "2020-01-01T00:00:00.1234559Z";

/// Which of `<`, `<=`, `=`, `>=`, `>` hold for a stored value against a
/// constant it orders `ord` against.
fn holds(op: &str, ord: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::*;
    match op {
        "<" => ord == Less,
        "<=" => ord != Greater,
        "=" => ord == Equal,
        ">=" => ord != Less,
        ">" => ord == Greater,
        _ => unreachable!("{op}"),
    }
}

/// Whether `op` holds between a plain string and a temporal value it orders
/// `ord` against. Ordering reads the string as the temporal it spells, but a
/// plain string is never `=` a temporal: SPARQL has no `=` for an
/// `xsd:string` and an `xsd:dateTime` or `xsd:time`, so `RDFterm-equal` is a
/// type error and the FILTER is false. So `<=` and `>=` can both hold where
/// `=` doesn't.
fn holds_for_a_string(op: &str, ord: std::cmp::Ordering) -> bool {
    op != "=" && holds(op, ord)
}

/// The operator `op` reads as with its operands swapped.
fn mirrored(op: &str) -> &str {
    match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        other => other,
    }
}

const OPS: [&str; 5] = ["<", "<=", "=", ">=", ">"];

/// A `dateTime` or `time` constant past the microsecond is held to it like
/// every value, for ordering as well as equality, so a stored value is
/// exactly one of `<`, `=` and `>` it (a plain-string constant orders but is
/// never `=` it, [`holds_for_a_string`]). The value is written as `DT_NS` and
/// `TIME_NS`, and `>=` that same constant finds it, which is how a client
/// pages from a timestamp it wrote. On SPARQL as a typed literal, a cast, a
/// constant on the left, and one bound first through `BIND` or `VALUES`,
/// which all answer alike; on JSON-LD as a string constant and through
/// `values`; in process, reloaded and indexed.
#[tokio::test]
async fn a_constant_is_held_to_the_microsecond_for_ordering_too() {
    use std::cmp::Ordering::{self, *};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    // (constant, how the stored value orders against it)
    let dt_constants: [(&str, Ordering); 5] = [
        (DT_NS, Equal),
        (DT_ABOVE, Equal),
        (DT_US, Equal),
        (DT_NEXT, Less),
        (DT_PREV, Greater),
    ];
    // (check, SPARQL ASK or JSON-LD query, whether ex:s matches)
    let mut sparql: Vec<(String, String, bool)> = Vec::new();
    let mut jsonld: Vec<(String, JsonValue, bool)> = Vec::new();
    let ask_where = |body: String| format!("{PFX}ASK {{ ex:s ex:d ?d ; ex:t ?t {body} }}");
    for (c, ord) in dt_constants {
        let typed = format!("\"{c}\"^^xsd:dateTime");
        for op in OPS {
            let want = holds(op, ord);
            let mirror = mirrored(op);
            for (form, body) in [
                ("typed", format!("FILTER(?d {op} {typed})")),
                ("cast", format!("FILTER(?d {op} xsd:dateTime(\"{c}\"))")),
                ("on the left", format!("FILTER({typed} {mirror} ?d)")),
                ("BIND", format!("BIND({typed} AS ?c) FILTER(?d {op} ?c)")),
                (
                    "VALUES",
                    format!("VALUES ?c {{ {typed} }} FILTER(?d {op} ?c)"),
                ),
            ] {
                sparql.push((format!("?d {op} {c} ({form})"), ask_where(body), want));
            }
            let filter = |rhs: JsonValue| {
                json!({
                    "@context": ctx(),
                    "select": ["?d"],
                    "where": [{"@id": "ex:s", "ex:d": "?d"}, ["filter", [op, "?d", rhs]]]
                })
            };
            // A JSON-LD filter's bare string is a plain string.
            jsonld.push((
                format!("JSON-LD ?d {op} \"{c}\""),
                filter(json!(c)),
                holds_for_a_string(op, ord),
            ));
            let mut by_values = filter(json!("?c"));
            by_values["values"] = json!(["?c", [{"@value": c, "@type": "xsd:dateTime"}]]);
            jsonld.push((format!("JSON-LD values ?d {op} {c}"), by_values, want));
        }
    }
    for op in OPS {
        let want = holds(op, Equal);
        let typed = format!("\"{TIME_ABOVE}\"^^xsd:time");
        sparql.push((
            format!("?t {op} {TIME_ABOVE}"),
            ask_where(format!("FILTER(?t {op} {typed})")),
            want,
        ));
        sparql.push((
            format!("?t {op} {TIME_ABOVE} (VALUES)"),
            ask_where(format!("VALUES ?c {{ {typed} }} FILTER(?t {op} ?c)")),
            want,
        ));
        jsonld.push((
            format!("JSON-LD ?t {op} \"{TIME_ABOVE}\""),
            json!({
                "@context": ctx(),
                "select": ["?t"],
                "where": [{"@id": "ex:s", "ex:t": "?t"}, ["filter", [op, "?t", TIME_ABOVE]]]
            }),
            holds_for_a_string(op, Equal),
        ));
    }
    sparql.push((
        "sameTerm".to_string(),
        ask_where(format!(
            "FILTER(sameTerm(?d, \"{DT_ABOVE}\"^^xsd:dateTime))"
        )),
        true,
    ));
    sparql.push((
        "a pattern's object".to_string(),
        format!("{PFX}ASK {{ ex:s ex:d \"{DT_ABOVE}\"^^xsd:dateTime }}"),
        true,
    ));
    jsonld.push((
        "JSON-LD a pattern's object".to_string(),
        json!({
            "@context": ctx(),
            "select": ["?s"],
            "where": {"@id": "?s", "ex:d": {"@value": DT_ABOVE, "@type": "xsd:dateTime"}}
        }),
        true,
    ));
    let run = |fluree: fluree_db_api::Fluree| {
        let sparql = sparql.clone();
        let jsonld = jsonld.clone();
        async move {
            let db = fluree.db(LEDGER).await.expect("db");
            let mut wrong = Vec::new();
            for (check, q, want) in &sparql {
                if ask(&fluree, q).await != *want {
                    wrong.push(check.clone());
                }
            }
            for (check, q, want) in &jsonld {
                let rows = fluree
                    .query(&db, q)
                    .await
                    .expect("query")
                    .to_jsonld_async(db.as_graph_db_ref())
                    .await
                    .expect("jsonld");
                let found = rows.as_array().is_some_and(|rows| !rows.is_empty());
                if found != *want {
                    wrong.push(check.clone());
                }
            }
            wrong
        }
    };
    let fluree = FlureeBuilder::file(path.clone())
        .without_indexing()
        .build()
        .expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let insert = format!("{PFX}INSERT DATA {{ {} }}", triples(DT_NS, TIME_NS));
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update(&insert)
        .commit()
        .await
        .expect("insert");
    let in_process = run(fluree.clone()).await;
    let reloaded = run(FlureeBuilder::file(path.clone())
        .without_indexing()
        .build()
        .expect("reload"))
    .await;
    support::rebuild_and_publish_index(&fluree, LEDGER).await;
    let indexed = run(FlureeBuilder::file(path)
        .without_indexing()
        .build()
        .expect("reload"))
    .await;
    let none: Vec<String> = Vec::new();
    assert_eq!(
        (in_process, reloaded, indexed),
        (none.clone(), none.clone(), none),
        "checks answered wrongly in process, reloaded, indexed"
    );
}

/// A plain string that reads as a `dateTime` is ordered against a temporal
/// value as the value it reads as, held to the microsecond: a stored
/// `"…1234565Z"` and a constant `"…1234569Z"^^xsd:dateTime` both read as
/// `…123456Z`, so `<=` and `>=` hold and `<` and `>` don't, either way
/// round, as for a string equal to the constant at every digit. `=` doesn't
/// hold: a plain string is never `=` a temporal ([`holds_for_a_string`]). In
/// process, reloaded and indexed.
#[tokio::test]
async fn a_string_is_ordered_against_a_temporal_at_the_microsecond() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let c = "\"2020-01-01T00:00:00.1234569Z\"^^xsd:dateTime";
    let mut checks: Vec<(String, String, bool)> = Vec::new();
    for op in OPS {
        let want = holds_for_a_string(op, std::cmp::Ordering::Equal);
        let mirror = mirrored(op);
        checks.push((
            format!("?v {op} c"),
            format!("{PFX}ASK {{ ex:s ex:str ?v FILTER(?v {op} {c}) }}"),
            want,
        ));
        checks.push((
            format!("c {mirror} ?v"),
            format!("{PFX}ASK {{ ex:s ex:str ?v FILTER({c} {mirror} ?v) }}"),
            want,
        ));
    }
    let run = |fluree: fluree_db_api::Fluree| {
        let checks = checks.clone();
        async move {
            let mut wrong = Vec::new();
            for (check, q, want) in &checks {
                if ask(&fluree, q).await != *want {
                    wrong.push(check.clone());
                }
            }
            wrong
        }
    };
    let fluree = FlureeBuilder::file(path.clone())
        .without_indexing()
        .build()
        .expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update(&format!(
            "{PFX}INSERT DATA {{ ex:s ex:str \"2020-01-01T00:00:00.1234565Z\" }}"
        ))
        .commit()
        .await
        .expect("insert");
    let in_process = run(fluree.clone()).await;
    let reloaded = run(FlureeBuilder::file(path.clone())
        .without_indexing()
        .build()
        .expect("reload"))
    .await;
    support::rebuild_and_publish_index(&fluree, LEDGER).await;
    let indexed = run(FlureeBuilder::file(path)
        .without_indexing()
        .build()
        .expect("reload"))
    .await;
    let none: Vec<String> = Vec::new();
    assert_eq!(
        (in_process, reloaded, indexed),
        (none.clone(), none.clone(), none),
        "checks answered wrongly in process, reloaded, indexed"
    );
}

/// A Cypher `datetime()` or `time()` constant past the microsecond is held to
/// it too, so a stored value is exactly one of `<`, `=` and `>` it, a
/// component map's `nanosecond` included, before a reload and after.
#[tokio::test]
async fn a_cypher_constant_is_held_to_the_microsecond_for_ordering_too() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let mut conditions: Vec<(String, i64)> = Vec::new();
    for op in OPS {
        let want = i64::from(holds(op, std::cmp::Ordering::Equal));
        let mirror = mirrored(op);
        conditions.push((format!("e.at {op} datetime(\"{DT_ABOVE}\")"), want));
        conditions.push((format!("e.at {op} datetime(\"{DT_NS}\")"), want));
        conditions.push((format!("datetime(\"{DT_ABOVE}\") {mirror} e.at"), want));
        conditions.push((format!("e.t {op} time(\"{TIME_ABOVE}\")"), want));
        conditions.push((
            format!("e.at {op} datetime({{year: 2020, month: 1, day: 1, nanosecond: 123456900}})"),
            want,
        ));
    }
    conditions.push((format!("e.at < datetime(\"{DT_NEXT}\")"), 1));
    conditions.push((format!("e.at > datetime(\"{DT_PREV}\")"), 1));
    let wrong = |fluree: fluree_db_api::Fluree, ledger: fluree_db_api::LedgerState| {
        let conditions = conditions.clone();
        async move {
            let db = support::graphdb_from_ledger(&ledger);
            let mut out = Vec::new();
            for (condition, want) in &conditions {
                let query = format!("MATCH (e:Event) WHERE {condition} RETURN count(e)");
                let rows = fluree
                    .query_cypher(&db, &query)
                    .await
                    .expect("query")
                    .to_jsonld_async(db.as_graph_db_ref())
                    .await
                    .expect("jsonld");
                if rows[0][0].as_i64().expect("count") != *want {
                    out.push(condition.clone());
                }
            }
            out
        }
    };
    let none: Vec<String> = Vec::new();
    {
        let fluree = FlureeBuilder::file(path.clone())
            .without_indexing()
            .build()
            .expect("build");
        let ledger = fluree.create_ledger(LEDGER).await.expect("create");
        let create =
            format!("CREATE (:Event {{at: datetime(\"{DT_NS}\"), t: time(\"{TIME_NS}\")}})");
        let written = fluree
            .transact_cypher(ledger, &create)
            .await
            .expect("create")
            .ledger;
        assert_eq!(
            wrong(fluree.clone(), written).await,
            none,
            "before a reload"
        );
    }
    let fluree = FlureeBuilder::file(path)
        .without_indexing()
        .build()
        .expect("reload");
    let ledger = fluree.ledger(LEDGER).await.expect("ledger");
    assert_eq!(wrong(fluree, ledger).await, none, "after a reload");
}
