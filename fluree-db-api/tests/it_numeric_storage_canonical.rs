//! A number is stored as its value, not as the text it was written with: an
//! `xsd:decimal` without trailing zeros, a `-0.0` double as `0.0`, and an
//! `xsd:float` at single precision. Every write lane, commit decode and query
//! constant reads them so, and the index holds them so, so a value renders the
//! same in the writing process, after a reload and once indexed.

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
const LEDGER: &str = "it/numeric-canonical:main";

/// Each predicate's value as stored, in predicate order: `STR` of it, and the
/// lexical the SPARQL JSON results render.
const STORED: [(&str, &str); 8] = [
    ("bare", "1.5"),
    ("dbl", "0.0E0"),
    ("dbl2", "0.0E0"),
    ("dec", "1.5"),
    ("fbig", "INF"),
    ("flt", "1.2345679E-1"),
    ("whole", "100"),
    ("zero", "0"),
];

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

/// The values as SPARQL/Turtle triples: typed literals, a bare decimal token
/// and a bare double token. The floats have more digits than a float holds,
/// and a magnitude beyond its range.
const TRIPLES: &str = "ex:s ex:dec \"1.50\"^^xsd:decimal ; ex:whole \"100.00\"^^xsd:decimal ; \
                       ex:zero \"-0.00\"^^xsd:decimal ; ex:bare 1.50 ; \
                       ex:dbl \"-0.0\"^^xsd:double ; ex:dbl2 -0.0e0 ; \
                       ex:flt \"0.1234567890123\"^^xsd:float ; ex:fbig \"3.4e39\"^^xsd:float .";

/// The same values in JSON-LD, as value objects: a string `@value`, and
/// `-0.0` and the out-of-range float also as JSON numbers (bare, JSON-LD reads
/// `-0.0` as the integer `0`).
fn node() -> JsonValue {
    json!({
        "@id": "ex:s",
        "ex:dec": {"@value": "1.50", "@type": "xsd:decimal"},
        "ex:whole": {"@value": "100.00", "@type": "xsd:decimal"},
        "ex:zero": {"@value": "-0.00", "@type": "xsd:decimal"},
        "ex:bare": {"@value": "1.50", "@type": "xsd:decimal"},
        "ex:dbl": {"@value": -0.0, "@type": "xsd:double"},
        "ex:dbl2": {"@value": "-0.0", "@type": "xsd:double"},
        "ex:flt": {"@value": "0.1234567890123", "@type": "xsd:float"},
        "ex:fbig": {"@value": 3.4e39, "@type": "xsd:float"}
    })
}

/// `(predicate, STR(o), rendered o)` for every value of `ex:s`, as `fluree`
/// holds the ledger: the writing instance's novelty, a reload, or the index.
async fn read(fluree: &fluree_db_api::Fluree, named: bool) -> Vec<(String, String, String)> {
    let q = format!(
        "{PFX}SELECT ?p (STR(?o) AS ?lex) ?o WHERE {{ {} }} ORDER BY ?p",
        wrap(named, "ex:s ?p ?o .")
    );
    let db = fluree.db(LEDGER).await.expect("db");
    let json = fluree
        .query(&db, &q)
        .await
        .expect("select")
        .to_sparql_json(&db.snapshot)
        .expect("json");
    json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|row| {
            let p = row["p"]["value"].as_str().expect("p");
            (
                p.trim_start_matches("http://example.org/ns/").to_string(),
                row["lex"]["value"].as_str().expect("lex").to_string(),
                row["o"]["value"].as_str().expect("o").to_string(),
            )
        })
        .collect()
}

fn stored() -> Vec<(String, String, String)> {
    STORED
        .iter()
        .map(|(p, lex)| (p.to_string(), lex.to_string(), lex.to_string()))
        .collect()
}

/// Write the values through `lane` on a fresh ledger at `path` (in `ex:g` for
/// the named-graph lanes). Returns the writing instance, and whether the lane
/// writes in the named graph.
async fn write(lane: &str, path: &str) -> (fluree_db_api::Fluree, bool) {
    let fluree = FlureeBuilder::file(path.to_string())
        .build()
        .expect("build");
    let graph = fluree.graph(LEDGER);
    let named = matches!(lane, "TriG insert" | "graph sync");
    if lane == "bulk import" {
        let dir = tempfile::tempdir().expect("data dir");
        let ttl = dir.path().join("data.ttl");
        std::fs::write(&ttl, format!("{TTL_PFX}{TRIPLES}\n")).expect("ttl");
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
            let update = format!("{PFX}INSERT DATA {{ {TRIPLES} }}");
            graph
                .transact()
                .sparql_update(&update)
                .commit()
                .await
                .map(drop)
        }
        "SPARQL INSERT WHERE" => {
            let update = format!("{PFX}INSERT {{ {TRIPLES} }} WHERE {{}}");
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
            let ttl = format!("{TTL_PFX}{TRIPLES}\n");
            graph
                .transact()
                .insert_turtle(&ttl)
                .commit()
                .await
                .map(drop)
        }
        "Turtle upsert" => {
            let ttl = format!("{TTL_PFX}{TRIPLES}\n");
            graph
                .transact()
                .upsert_turtle(&ttl)
                .commit()
                .await
                .map(drop)
        }
        "TriG insert" => {
            let trig = format!("{TTL_PFX}GRAPH <{G}> {{ {TRIPLES} }}\n");
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
            let source = "it/numeric-canonical-source:main";
            fluree.create_ledger(source).await.expect("source");
            let update = format!("{PFX}INSERT DATA {{ {TRIPLES} }}");
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
                        commit_ids: Vec::new(),
                        merged_commit_ids: Vec::new(),
                        ..Default::default()
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

/// Every write lane holds a decimal without its trailing zeros, `-0.0` as
/// `0.0` and a float at single precision, before a reload, after one, and
/// once indexed.
#[tokio::test]
async fn every_write_lane_stores_a_number_as_its_value() {
    let lanes = [
        "SPARQL INSERT DATA",
        "SPARQL INSERT WHERE",
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
        let written = read(&writer, named).await;
        support::rebuild_and_publish_index(&writer, LEDGER).await;
        drop(writer);
        let fluree = FlureeBuilder::file(path.clone())
            .without_storage_lock()
            .build()
            .expect("reload");
        let indexed = read(&fluree, named).await;
        for (state, rows) in [("as written", written), ("indexed", indexed)] {
            if rows != stored() {
                failures.push(format!("{lane}, {state}: {rows:?}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "expected {:?}\n{}",
        stored(),
        failures.join("\n")
    );
}

/// A ledger reloaded from its commits, never indexed, reads the values as
/// stored. (A commit an earlier release wrote with the spelling kept decodes
/// to the value too: `op_codec`'s `decoding_reads_a_number_as_its_value`.)
#[tokio::test]
async fn a_reload_from_commits_reads_a_number_as_its_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let (writer, named) = write("SPARQL INSERT DATA", &path).await;
    drop(writer);
    let fluree = FlureeBuilder::file(path)
        .without_storage_lock()
        .build()
        .expect("reload");
    assert_eq!(read(&fluree, named).await, stored());
}

/// An update that stores a constant its WHERE binds (`VALUES`, `BIND`, and
/// JSON-LD `values`) stores the value, alike before and after indexing.
#[tokio::test]
async fn an_update_storing_a_bound_constant_stores_its_value() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let graph = fluree.graph(LEDGER);
    for update in [
        format!(
            "{PFX}INSERT {{ ex:v ex:dec ?d ; ex:dbl ?f }} \
             WHERE {{ VALUES (?d ?f) {{ (1.50 -0.0e0) }} }}"
        ),
        format!("{PFX}INSERT {{ ex:b ex:dec ?d }} WHERE {{ BIND(1.50 AS ?d) }}"),
    ] {
        graph
            .transact()
            .sparql_update(&update)
            .commit()
            .await
            .unwrap_or_else(|e| panic!("{update}: {e}"));
    }
    let jsonld = json!({
        "@context": ctx(),
        "values": ["?d", [{"@value": "1.50", "@type": "xsd:decimal"}]],
        "insert": {"@id": "ex:j", "ex:dec": "?d"}
    });
    graph
        .transact()
        .update(&jsonld)
        .commit()
        .await
        .expect("jsonld update");

    let read = |fluree: fluree_db_api::Fluree| async move {
        let q = format!(
            "{PFX}SELECT ?s ?p (STR(?o) AS ?lex) WHERE {{ \
             VALUES ?s {{ ex:v ex:b ex:j }} ?s ?p ?o }} ORDER BY ?s ?p"
        );
        let db = fluree.db(LEDGER).await.expect("db");
        let json = fluree
            .query(&db, &q)
            .await
            .expect("select")
            .to_sparql_json(&db.snapshot)
            .expect("json");
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|r| {
                let local = |v: &JsonValue| {
                    v["value"]
                        .as_str()
                        .expect("value")
                        .trim_start_matches("http://example.org/ns/")
                        .to_string()
                };
                (local(&r["s"]), local(&r["p"]), local(&r["lex"]))
            })
            .collect::<Vec<_>>()
    };
    let written = read(fluree.clone()).await;
    support::rebuild_and_publish_index(&fluree, LEDGER).await;
    drop(fluree);
    let indexed = read(
        FlureeBuilder::file(path)
            .without_storage_lock()
            .build()
            .expect("reload"),
    )
    .await;
    let expected: Vec<(String, String, String)> = [
        ("b", "dec", "1.5"),
        ("j", "dec", "1.5"),
        ("v", "dbl", "0.0E0"),
        ("v", "dec", "1.5"),
    ]
    .iter()
    .map(|(s, p, l)| (s.to_string(), p.to_string(), l.to_string()))
    .collect();
    assert_eq!((written, indexed), (expected.clone(), expected));
}

/// A query constant is read as its value too: `VALUES` and `BIND` render a
/// decimal without its trailing zeros, `-0.0` as `0.0` and a float at single
/// precision, in SPARQL and in JSON-LD.
#[tokio::test]
async fn a_query_constant_is_read_as_its_value() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, LEDGER).await;
    let db = support::graphdb_from_ledger(&ledger);

    let sparql = format!(
        "{PFX}SELECT (STR(?x) AS ?lex) ?x WHERE {{ \
         VALUES ?x {{ 1.50 \"100.00\"^^xsd:decimal \"-0.0\"^^xsd:double -0.0e0 \
         \"0.1234567890123\"^^xsd:float }} }}"
    );
    let json = fluree
        .query(&db, &sparql)
        .await
        .expect("sparql")
        .to_sparql_json(&db.snapshot)
        .expect("json");
    let rows: Vec<(&str, &str)> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|r| {
            (
                r["lex"]["value"].as_str().expect("lex"),
                r["x"]["value"].as_str().expect("x"),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("1.5", "1.5"),
            ("100", "100"),
            ("0.0E0", "0.0E0"),
            ("0.0E0", "0.0E0"),
            ("1.2345679E-1", "1.2345679E-1")
        ]
    );

    let jsonld = json!({
        "@context": ctx(),
        "values": ["?x", [
            {"@value": "1.50", "@type": "xsd:decimal"},
            {"@value": "-0.0", "@type": "xsd:double"},
            {"@value": "0.1234567890123", "@type": "xsd:float"},
            {"@value": 0.123_456_789_012_3, "@type": "xsd:float"}
        ]],
        "where": [["bind", "?lex", "(str ?x)"]],
        "select": ["?lex"]
    });
    let rows = fluree
        .query(&db, &jsonld)
        .await
        .expect("jsonld")
        .to_jsonld(&db.snapshot)
        .expect("jsonld rows");
    assert_eq!(
        rows,
        json!([["1.5"], ["0.0E0"], ["1.2345679E-1"], ["1.2345679E-1"]])
    );

    // Arithmetic keeps the scale it computes, as compatibility.md says.
    let sum = format!("{PFX}SELECT (STR(1.50 + 1.50) AS ?lex) WHERE {{}}");
    let json = fluree
        .query(&db, &sum)
        .await
        .expect("sum")
        .to_sparql_json(&db.snapshot)
        .expect("json");
    assert_eq!(json["results"]["bindings"][0]["lex"]["value"], "3.0");
}

/// A JSON-LD query renders the stored value alike before and after indexing.
#[tokio::test]
async fn a_jsonld_query_renders_a_number_alike_before_and_after_indexing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let (writer, _) = write("JSON-LD insert", &path).await;
    let query = json!({
        "@context": ctx(),
        "where": [
            {"@id": "ex:s", "ex:dec": "?d", "ex:whole": "?w", "ex:zero": "?z", "ex:dbl": "?f"},
            ["bind", "?dl", "(str ?d)", "?fl", "(str ?f)"]
        ],
        "select": ["?d", "?w", "?z", "?f", "?dl", "?fl"]
    });
    let rows = |fluree: fluree_db_api::Fluree| {
        let query = query.clone();
        async move {
            let db = fluree.db(LEDGER).await.expect("db");
            fluree
                .query(&db, &query)
                .await
                .expect("query")
                .to_jsonld(&db.snapshot)
                .expect("jsonld")
        }
    };
    let written = rows(writer.clone()).await;
    support::rebuild_and_publish_index(&writer, LEDGER).await;
    drop(writer);
    let indexed = rows(
        FlureeBuilder::file(path)
            .without_storage_lock()
            .build()
            .expect("reload"),
    )
    .await;
    assert_eq!(written, indexed, "as written vs indexed");
    assert_eq!(indexed[0][4], json!("1.5"));
    assert_eq!(indexed[0][5], json!("0.0E0"));
}

/// Each row of a SPARQL `SELECT`, as the values of `vars` the SPARQL JSON
/// results render.
async fn sparql_values(
    fluree: &fluree_db_api::Fluree,
    query: &str,
    vars: &[&str],
) -> Vec<Vec<String>> {
    let db = fluree.db(LEDGER).await.expect("db");
    let json = fluree
        .query(&db, &format!("{PFX}{query}"))
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .to_sparql_json(&db.snapshot)
        .expect("json");
    json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|row| {
            vars.iter()
                .map(|v| row[*v]["value"].as_str().expect("value").to_string())
                .collect()
        })
        .collect()
}

/// A float an expression computes is the same term as the stored float: a
/// cast of the same lexical joins it, and arithmetic and a group key print as
/// the stored value does, on both surfaces. `xsd:string` keeps the XPath cast
/// form.
#[tokio::test]
async fn a_computed_float_is_the_stored_float() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create");
    fluree
        .graph(LEDGER)
        .transact()
        .insert_turtle(&format!(
            "{TTL_PFX}ex:s ex:flt \"0.1234567890123\"^^xsd:float .\n"
        ))
        .commit()
        .await
        .expect("insert");

    let joined = sparql_values(
        &fluree,
        "SELECT ?s WHERE { BIND(xsd:float(\"0.1234567890123\") AS ?f) ?s ex:flt ?f }",
        &["s"],
    )
    .await;
    assert_eq!(joined, [["http://example.org/ns/s"]]);
    let computed = sparql_values(
        &fluree,
        "SELECT ?f (?f + 0 AS ?sum) (xsd:string(?f) AS ?cast) WHERE { ex:s ex:flt ?f }",
        &["f", "sum", "cast"],
    )
    .await;
    assert_eq!(computed, [["1.2345679E-1", "1.2345679E-1", "0.12345679"]]);
    let grouped = sparql_values(
        &fluree,
        "SELECT (GROUP_CONCAT(?f) AS ?all) WHERE { ?s ex:flt ?f } GROUP BY ?f",
        &["all"],
    )
    .await;
    assert_eq!(grouped, [["1.2345679E-1"]]);

    let db = fluree.db(LEDGER).await.expect("db");
    let jsonld = json!({
        "@context": ctx(),
        "select": ["?f", "?sum", "(as (groupconcat ?f) ?all)"],
        "where": [{"@id": "?s", "ex:flt": "?f"}, ["bind", "?sum", "(+ ?f 0)"]],
        "groupBy": ["?f", "?sum"]
    });
    let rows = fluree
        .query(&db, &jsonld)
        .await
        .expect("jsonld")
        .to_jsonld(&db.snapshot)
        .expect("jsonld rows");
    let float = json!({"@value": 0.123_456_79, "@type": "xsd:float"});
    assert_eq!(rows, json!([[float, float, "1.2345679E-1"]]));
}
