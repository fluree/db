//! Misc query integration tests
//!
//! We prioritize query semantics; some scenarios are intentionally out of scope here.

use std::sync::Arc;
mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
#[cfg(feature = "native")]
use support::{start_background_indexer_local, trigger_index_and_wait};

async fn seed_three_people(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:alice","ex:name":"Alice","ex:age":30},
            {"@id":"ex:bob","ex:name":"Bob","ex:age":25},
            {"@id":"ex:charlie","ex:name":"Charlie","ex:age":35}
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger
}

#[tokio::test]
async fn simple_where_select_limit_without_context_returns_full_iri() {
    // Scenario: misc-queries-test/simple-where-select-test (adapted: Rust requires select array)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_three_people(&fluree, "misc/simple-where-select:main").await;

    let query = json!({
        "select": ["?s"],
        // Rust does not treat {"@id":"?s"} alone as a binding pattern; include a predicate.
        "where":  {"@id": "?s", "http://example.org/ns/name": "?name"},
        "orderBy": "?s",
        "limit":  1
    });

    let rows = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!(["http://example.org/ns/alice"]));
}

#[tokio::test]
async fn simple_where_select_limit_with_context_returns_compacted_iri() {
    // Scenario: misc-queries-test/simple-where-select-test (with context)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_three_people(&fluree, "misc/simple-where-select:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?s"],
        // Rust does not treat {"@id":"?s"} alone as a binding pattern; include a predicate.
        "where":  {"@id": "?s", "ex:name": "?name"},
        "orderBy": "?s",
        "limit":  1
    });

    let rows = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows, json!(["ex:alice"]));
}

#[tokio::test]
async fn class_queries_type_and_all_types() {
    // Scenario: misc-queries-test/class-queries (subset: rdf:type queries)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/class-queries:main");
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:email":"alice@flur.ee","schema:age":42},
            {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":22},
            {"@id":"ex:jane","@type":"ex:User","schema:name":"Jane","schema:email":"jane@flur.ee","schema:age":30},
            {"@id":"ex:dave","@type":"ex:nonUser","schema:name":"Dave"}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q1 = json!({
        "@context": ctx,
        "select": ["?class"],
        "where": {"@id":"ex:jane","@type":"?class"}
    });
    let r1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(r1, json!(["ex:User"]));

    let q2 = json!({
        "@context": ctx,
        "select": ["?s","?class"],
        "where": {"@id":"?s","@type":"?class"}
    });
    let r2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&r2),
        normalize_rows(&json!([
            ["ex:alice", "ex:User"],
            ["ex:bob", "ex:User"],
            ["ex:dave", "ex:nonUser"],
            ["ex:jane", "ex:User"]
        ]))
    );
}

// -----------------------------------------------------------------------------
// Parity placeholders (not yet supported / different API surface)
// -----------------------------------------------------------------------------

#[tokio::test]
async fn result_formatting_graph_crawl_variants() {
    // Scenario: misc-queries-test/result-formatting (current query section)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/result-formatting:main");

    let insert = json!({
        "@context": {"id":"@id","ex":"http://example.org/ns/"},
        "@graph": [
            {"@id":"ex:dan","ex:x": 1},
            {"@id":"ex:wes","ex:x": 2}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    // Sanity: the data is queryable via WHERE.
    let sanity = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "select": ["?v"],
        "where": {"@id":"ex:dan","ex:x":"?v"}
    });
    let sanity_rows = support::query_jsonld(&fluree, &ledger, &sanity)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(sanity_rows, json!([1]));

    // 1) default context
    let q1 = json!({
        "@context": {"id":"@id","ex":"http://example.org/ns/"},
        "select": {"ex:dan": ["*"]}
    });
    let r1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // 2) alternate prefix mapping (foo)
    let q2 = json!({
        "@context": [
            {"id":"@id","ex":"http://example.org/ns/"},
            {"foo":"http://example.org/ns/"}
        ],
        "select": {"foo:dan": ["*"]}
    });
    let r2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // 3) override unwrapping with @set on foo:x
    let q3 = json!({
        "@context": [
            {"id":"@id","ex":"http://example.org/ns/"},
            {"foo":"http://example.org/ns/", "foo:x": {"@container":"@set"}}
        ],
        "select": {"foo:dan": ["*"]}
    });
    let r3 = support::query_jsonld(&fluree, &ledger, &q3)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // 4) override unwrapping with @list on foo:x (note key is "foo:x" literal)
    let q4 = json!({
        "@context": [
            {"id":"@id","ex":"http://example.org/ns/"},
            {"foo":"http://example.org/ns/", "foo:x": {"@container":"@list"}}
        ],
        "select": {"foo:dan": ["*"]}
    });
    let r4 = support::query_jsonld(&fluree, &ledger, &q4)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // 5) no context: expanded IRIs + @id key
    let q5 = json!({
        "select": {"http://example.org/ns/dan": ["*"]}
    });
    let r5 = support::query_jsonld(&fluree, &ledger, &q5)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // 6) empty context object
    let q6 = json!({
        "@context": {},
        "select": {"http://example.org/ns/dan": ["*"]}
    });
    let r6 = support::query_jsonld(&fluree, &ledger, &q6)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // 7) empty context array
    let q7 = json!({
        "@context": [],
        "select": {"http://example.org/ns/dan": ["*"]}
    });
    let r7 = support::query_jsonld(&fluree, &ledger, &q7)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    fn canon_ex_over_foo(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(canon_ex_over_foo).collect())
            }
            serde_json::Value::Object(map) => {
                let mut out = serde_json::Map::new();
                for (k, vv) in map {
                    let kk = if k.starts_with("foo:") {
                        k.replacen("foo:", "ex:", 1)
                    } else {
                        k.clone()
                    };
                    out.insert(kk, canon_ex_over_foo(vv));
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::String(s) => {
                if s.starts_with("foo:") {
                    serde_json::Value::String(s.replacen("foo:", "ex:", 1))
                } else {
                    serde_json::Value::String(s.clone())
                }
            }
            other => other.clone(),
        }
    }

    assert_eq!(r1, json!([{"@id":"ex:dan","ex:x":1}]));
    // When multiple prefixes map to the same namespace, compaction may choose either "ex:" or "foo:".
    // Canonicalize to "ex:" for stable assertions.
    assert_eq!(canon_ex_over_foo(&r2), json!([{"@id":"ex:dan","ex:x":1}]));
    assert_eq!(canon_ex_over_foo(&r3), json!([{"@id":"ex:dan","ex:x":[1]}]));
    assert_eq!(canon_ex_over_foo(&r4), json!([{"@id":"ex:dan","ex:x":[1]}]));
    assert_eq!(
        r5,
        json!([{"@id":"http://example.org/ns/dan","http://example.org/ns/x":1}])
    );
    assert_eq!(
        r6,
        json!([{"@id":"http://example.org/ns/dan","http://example.org/ns/x":1}])
    );
    assert_eq!(
        r7,
        json!([{"@id":"http://example.org/ns/dan","http://example.org/ns/x":1}])
    );
}

#[tokio::test]
async fn s_p_o_full_db_queries_parity() {
    // Scenario: misc-queries-test/s+p+o-full-db-queries (partial coverage: skip commit metadata scan)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/s-p-o-full-db:main");
    let ctx = json!([context_ex_schema(), {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}]);

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:email":"alice@flur.ee","schema:age":42},
            {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":22},
            {"@id":"ex:jane","@type":"ex:User","schema:name":"Jane","schema:email":"jane@flur.ee","schema:age":30}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q_all = json!({
        "@context": ctx,
        "select": ["?s","?p","?o"],
        "where": {"@id":"?s","?p":"?o"}
    });
    let r_all = support::query_jsonld(&fluree, &ledger, &q_all)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&r_all),
        normalize_rows(&json!([
            ["ex:alice", "rdf:type", "ex:User"],
            ["ex:alice", "schema:age", 42],
            ["ex:alice", "schema:email", "alice@flur.ee"],
            ["ex:alice", "schema:name", "Alice"],
            ["ex:bob", "rdf:type", "ex:User"],
            ["ex:bob", "schema:age", 22],
            ["ex:bob", "schema:name", "Bob"],
            ["ex:jane", "rdf:type", "ex:User"],
            ["ex:jane", "schema:age", 30],
            ["ex:jane", "schema:email", "jane@flur.ee"],
            ["ex:jane", "schema:name", "Jane"]
        ]))
    );

    let q_graph = json!({
        "@context": ctx,
        "select": {"?s":["*"]},
        "where": {"@id":"?s","?p":"?o"}
    });
    let r_graph = support::query_jsonld(&fluree, &ledger, &q_graph)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    let canon_person = |obj: &serde_json::Map<String, serde_json::Value>| {
        let get_key =
            |keys: &[&str]| -> Option<&serde_json::Value> { keys.iter().find_map(|k| obj.get(*k)) };
        let id = get_key(&["@id", "id"])?;
        let typ = get_key(&[
            "@type",
            "rdf:type",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        ])?;
        let name = get_key(&["schema:name", "http://schema.org/name"])?;
        let age = get_key(&["schema:age", "http://schema.org/age"])?;
        let email = get_key(&["schema:email", "http://schema.org/email"]);
        Some((
            id.clone(),
            typ.clone(),
            name.clone(),
            age.clone(),
            email.cloned(),
        ))
    };

    let expected = [
        (
            json!("ex:alice"),
            json!("ex:User"),
            json!("Alice"),
            json!(42),
            Some(json!("alice@flur.ee")),
        ),
        (
            json!("ex:bob"),
            json!("ex:User"),
            json!("Bob"),
            json!(22),
            None,
        ),
        (
            json!("ex:jane"),
            json!("ex:User"),
            json!("Jane"),
            json!(30),
            Some(json!("jane@flur.ee")),
        ),
    ];

    let rows = r_graph.as_array().expect("graph crawl results array");
    assert_eq!(rows.len(), 11);
    for row in rows {
        let obj = row.as_object().expect("graph crawl row object");
        let canon = canon_person(obj).expect("canonicalize person row");
        assert!(expected.contains(&canon), "unexpected row: {row:?}");
    }
}

#[tokio::test]
async fn commit_db_metadata_spo_queries_parity() {
    // Scenario: misc-queries-test/s+p+o-full-db-queries (commit/db metadata portion)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/commit-metadata:main");
    let ctx = context_ex_schema();

    let tx1 = json!({
        "@context": ctx,
        "@graph": [{"@id":"ex:alice","schema:name":"Alice"}]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;

    let tx2 = json!({
        "@context": ctx,
        "@graph": [{"@id":"ex:bob","schema:name":"Bob"}]
    });
    let ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;

    let q_commit = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "from": "misc/commit-metadata:main#txn-meta",
        "select": ["?c","?alias"],
        "where": {"@id": "?c", "f:alias": "?alias"}
    });
    let r_commit = fluree
        .query_connection(&q_commit)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    let rows = r_commit.as_array().expect("commit rows array");
    assert!(
        rows.len() >= 2,
        "expected at least two commits, got: {r_commit:?}"
    );
    for row in rows {
        let arr = row.as_array().expect("commit row array");
        let subject = arr[0].as_str().unwrap_or_default();
        let ledger_id = arr[1].as_str().unwrap_or_default();
        assert!(
            subject.starts_with("fluree:commit:"),
            "unexpected commit subject: {subject}"
        );
        assert_eq!(ledger_id, "misc/commit-metadata:main");
    }

    let q_db = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "from": "misc/commit-metadata:main#txn-meta",
        "select": ["?c","?t"],
        "where": {"@id": "?c", "f:t": "?t"}
    });
    let r_db = fluree
        .query_connection(&q_db)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    let rows = r_db.as_array().expect("db rows array");
    let mut commit_subjects = std::collections::HashSet::new();
    for row in rows {
        let arr = row.as_array().expect("db row array");
        let subject = arr[0].as_str().unwrap_or_default();
        assert!(
            subject.starts_with("fluree:commit:"),
            "unexpected commit subject: {subject}"
        );
        let t_val = &arr[1];
        let t_ok = if t_val.is_number() || t_val.is_string() {
            true
        } else if let Some(obj) = t_val.as_object() {
            obj.get("@value")
                .map(|v| v.is_number() || v.is_string())
                .unwrap_or(false)
        } else {
            false
        };
        assert!(
            t_ok,
            "expected t value to be number/string or typed literal, got: {t_val:?}"
        );
        commit_subjects.insert(subject.to_string());
    }
    assert!(
        commit_subjects.len() >= 2,
        "expected at least two commit subjects, got: {commit_subjects:?}"
    );
}

#[tokio::test]
async fn illegal_reference_queries_error_on_var_predicate_with_literals() {
    // Scenario: misc-queries-test/illegal-reference-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_three_people(&fluree, "misc/illegal-reference:main").await;
    let ctx = context_ex_schema();

    let q_num = json!({
        "@context": ctx,
        "select": ["?s","?p"],
        "where": {"@id":"?s","?p": 25}
    });
    let err_num = support::query_jsonld(&fluree, &ledger, &q_num)
        .await
        .unwrap_err();
    assert!(
        err_num
            .to_string()
            .contains("variable predicate requires object"),
        "unexpected error: {err_num}"
    );

    let q_str = json!({
        "@context": ctx,
        "select": ["?s","?p"],
        "where": {"@id":"?s","?p": "Bob"}
    });
    let err_str = support::query_jsonld(&fluree, &ledger, &q_str)
        .await
        .unwrap_err();
    assert!(
        err_str
            .to_string()
            .contains("variable predicate requires object"),
        "unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn type_handling_parity() {
    // Scenario: misc-queries-test/type-handling
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/type-handling:main");

    let ctx = json!({
        "id": "@id",
        "type": "@type",
        "ex": "http://example.org/ns/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    });

    let db1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"id":"ex:ace","type":"ex:Spade"},
                    {"id":"ex:king","type":"ex:Heart"},
                    {"id":"ex:queen","type":"ex:Heart"},
                    {"id":"ex:jack","type":"ex:Club"}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger;

    // Query with "type" in WHERE and results
    let q_type = json!({
        "@context": ctx,
        "select": {"?s":["*"]},
        "where": {"id":"?s","type":"ex:Heart"}
    });
    let r_type = support::query_jsonld(&fluree, &db1, &q_type)
        .await
        .unwrap()
        .to_jsonld_async(db1.as_graph_db_ref(0))
        .await
        .unwrap();
    let canon_rows = |rows: &serde_json::Value| -> Vec<serde_json::Value> {
        rows.as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|row| {
                if let Some(obj) = row.as_object() {
                    let mut out = serde_json::Map::new();
                    for (k, v) in obj {
                        let key = match k.as_str() {
                            "@id" => "id",
                            "@type" => "type",
                            _ => k.as_str(),
                        };
                        out.insert(key.to_string(), v.clone());
                    }
                    serde_json::Value::Object(out)
                } else {
                    row.clone()
                }
            })
            .collect()
    };

    let set_type = canon_rows(&r_type);
    assert!(
        set_type.contains(&json!({"id":"ex:queen","type":"ex:Heart"}))
            && set_type.contains(&json!({"id":"ex:king","type":"ex:Heart"})),
        "type query should return queen and king, got: {set_type:?}"
    );

    // Query with rdf:type in WHERE should also work
    let q_rdf_type = json!({
        "@context": ctx,
        "select": {"?s":["*"]},
        "where": {"id":"?s","rdf:type":"ex:Heart"}
    });
    let r_rdf = support::query_jsonld(&fluree, &db1, &q_rdf_type)
        .await
        .unwrap()
        .to_jsonld_async(db1.as_graph_db_ref(0))
        .await
        .unwrap();
    let set_rdf = canon_rows(&r_rdf);
    assert!(
        set_rdf.contains(&json!({"id":"ex:queen","type":"ex:Heart"}))
            && set_rdf.contains(&json!({"id":"ex:king","type":"ex:Heart"})),
        "rdf:type query should return queen and king, got: {set_rdf:?}"
    );

    // Transact with rdf:type predicate should error unless aliased
    let err_db = match fluree
        .insert(
            db1,
            &json!({
                "@context": ctx,
                "@graph": [{"id":"ex:two","rdf:type":"ex:Diamond"}]
            }),
        )
        .await
    {
        Ok(_) => panic!("expected insert error for rdf:type without alias"),
        Err(err) => err,
    };
    assert!(
        err_db
            .to_string()
            .contains("Please use the JSON-LD \"@type\" keyword instead"),
        "unexpected error: {err_db}"
    );

    // Allow rdf:type when explicitly aliased to @type
    let ctx_alias = json!({
        "id": "@id",
        "type": "@type",
        "ex": "http://example.org/ns/",
        "rdf:type": "@type"
    });

    let ledger1b = genesis_ledger(&fluree, "misc/type-handling-alias:main");
    let db1b = fluree
        .insert(
            ledger1b,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"id":"ex:ace","type":"ex:Spade"},
                    {"id":"ex:king","type":"ex:Heart"},
                    {"id":"ex:queen","type":"ex:Heart"},
                    {"id":"ex:jack","type":"ex:Club"}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let db2 = fluree
        .insert(
            db1b,
            &json!({
                "@context": ctx_alias,
                "@graph": [{"id":"ex:two","rdf:type":"ex:Diamond"}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let q_alias = json!({
        "@context": ctx,
        "select": {"?s":["*"]},
        "where": {"id":"?s","type":"ex:Diamond"}
    });
    let r_alias = support::query_jsonld(&fluree, &db2, &q_alias)
        .await
        .unwrap()
        .to_jsonld_async(db2.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        canon_rows(&r_alias),
        vec![json!({"id":"ex:two","type":"ex:Diamond"})]
    );
}

#[tokio::test]
async fn load_with_new_connection_placeholder() {
    // Scenario: misc-queries-test/load-with-new-connection
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(storage_path)
        .build()
        .expect("build file fluree");
    let ledger_id = "new3:main";

    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:created", "ex:createdAt": "now"}]
    });
    let _ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let fluree2 = FlureeBuilder::file(storage_path)
        .build()
        .expect("build file fluree2");
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "from": ledger_id,
        "where": {"@id": "?s", "ex:createdAt": "now"},
        "select": {"?s": ["ex:createdAt"]}
    });
    let result = fluree2.query_connection(&query).await.unwrap();
    let ledger = fluree2.ledger(ledger_id).await.unwrap();
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    let rows = jsonld.as_array().expect("rows");
    assert_eq!(rows.len(), 1);
    let row = rows[0].as_object().expect("row object");
    assert_eq!(row.get("ex:createdAt"), Some(&json!("now")));
}

#[tokio::test]
async fn repeated_transaction_results_parity() {
    // Scenario: misc-queries-test/repeated-transaction-results
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/repeated-tx-results:main");

    let ctx = json!({"ex": "http://example.org/ns/"});
    let tx = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:1", "ex:foo": 30}]
    });
    let ledger1 = fluree.insert(ledger0, &tx).await.unwrap().ledger;
    let ledger2 = fluree.upsert(ledger1, &tx).await.unwrap().ledger;

    let q = json!({
        "@context": ctx,
        "select": {"ex:1": ["*"]}
    });
    let result = support::query_jsonld(&fluree, &ledger2, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        normalize_rows(&result),
        normalize_rows(&json!([{"@id": "ex:1", "ex:foo": 30}]))
    );
}

#[tokio::test]
async fn base_context_parity() {
    // Scenario: misc-queries-test/base-context
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/base-context:main");

    let insert = json!({
        "@context": {"@base": "https://flur.ee/", "ex": "http://example.com/"},
        "@graph": [
            {"@id": "freddy", "@type": "Yeti", "name": "Freddy"},
            {"@id": "ex:betty", "@type": "Yeti", "name": "Betty"}
        ]
    });
    let ledger = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let q1 = json!({
        "@context": {"@base": "https://flur.ee/"},
        "where": [{"@id": "freddy", "?p": "?o"}],
        "select": ["?p", "?o"]
    });
    let r1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    let normalize_type_pred = |rows: &serde_json::Value| -> serde_json::Value {
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        let mut out = Vec::new();
        for row in rows.as_array().unwrap_or(&vec![]) {
            if let Some(arr) = row.as_array() {
                let mut cols = arr.clone();
                if let Some(serde_json::Value::String(pred)) = cols.get_mut(0) {
                    if pred == rdf_type {
                        *pred = "@type".to_string();
                    }
                }
                out.push(serde_json::Value::Array(cols));
            }
        }
        serde_json::Value::Array(out)
    };
    assert_eq!(
        normalize_rows(&normalize_type_pred(&r1)),
        normalize_rows(&json!([["name", "Freddy"], ["@type", "Yeti"]]))
    );

    let q2 = json!({
        "@context": {"@base": "https://flur.ee/"},
        "select": {"freddy": ["*"]}
    });
    let r2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        r2,
        json!([{"@id": "freddy", "@type": "Yeti", "name": "Freddy"}])
    );
}

#[tokio::test]
async fn untyped_value_matching_parity() {
    // Scenario: misc-queries-test/untyped-value-matching-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "misc/untyped-value-matching:main");

    let ctx = json!({"ex": "http://example.org/ns/"});
    let tx1 = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;

    let tx2 = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    let ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;
    let commit_t = ledger2.t();

    let q_typed = json!({
        "@context": {
            "f": "https://ns.flur.ee/db#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "from": "misc/untyped-value-matching:main#txn-meta",
        "select": "?c",
        "where": [{"@id": "?c", "f:t": {"@value": commit_t, "@type": "xsd:int"}}]
    });
    let r_typed = fluree
        .query_connection(&q_typed)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    assert_eq!(r_typed.as_array().map(std::vec::Vec::len), Some(1));
    assert!(
        r_typed[0]
            .as_str()
            .unwrap_or_default()
            .starts_with("fluree:commit:"),
        "expected commit IRI, got: {r_typed:?}"
    );

    let q_untyped = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "from": "misc/untyped-value-matching:main#txn-meta",
        "select": "?c",
        "where": [{"@id": "?c", "f:t": commit_t}]
    });
    let r_untyped = fluree
        .query_connection(&q_untyped)
        .await
        .unwrap()
        .to_jsonld(&ledger2.snapshot)
        .unwrap();
    assert_eq!(r_untyped.as_array().map(std::vec::Vec::len), Some(1));
    assert!(
        r_untyped[0]
            .as_str()
            .unwrap_or_default()
            .starts_with("fluree:commit:"),
        "expected commit IRI, got: {r_untyped:?}"
    );
}

#[cfg(feature = "native")]
#[tokio::test]
async fn indexed_untyped_value_matching_parity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "misc/untyped-value-matching-indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let ctx = json!({"ex": "http://example.org/ns/"});

            let tx1 = json!({
                "@context": ctx,
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            });
            let ledger1 = fluree.insert(ledger0, &tx1).await.unwrap().ledger;

            let tx2 = json!({
                "@context": ctx,
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            });
            let ledger2 = fluree.insert(ledger1, &tx2).await.unwrap().ledger;
            let commit_t = ledger2.t();

            trigger_index_and_wait(&handle, ledger_id, commit_t).await;
            fluree.disconnect_ledger(ledger_id).await;
            let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");
            assert!(
                indexed.snapshot.range_provider.is_some(),
                "expected range_provider after indexing"
            );

            let q_typed = json!({
                "@context": {
                    "f": "https://ns.flur.ee/db#",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "from": "misc/untyped-value-matching-indexed:main#txn-meta",
                "select": "?c",
                "where": [{"@id": "?c", "f:t": {"@value": commit_t, "@type": "xsd:int"}}]
            });
            let r_typed = fluree
                .query_connection(&q_typed)
                .await
                .unwrap()
                .to_jsonld(&indexed.snapshot)
                .unwrap();
            assert_eq!(r_typed.as_array().map(std::vec::Vec::len), Some(1));

            let q_untyped = json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "from": "misc/untyped-value-matching-indexed:main#txn-meta",
                "select": "?c",
                "where": [{"@id": "?c", "f:t": commit_t}]
            });
            let r_untyped = fluree
                .query_connection(&q_untyped)
                .await
                .unwrap()
                .to_jsonld(&indexed.snapshot)
                .unwrap();
            assert_eq!(r_untyped.as_array().map(std::vec::Vec::len), Some(1));
        })
        .await;
}

// =============================================================================
// Index range scan tests (from it_query_index_range.rs)
// =============================================================================

#[tokio::test]
async fn index_range_scans() {
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
    use fluree_db_core::value::FlakeValue;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "query/index-range:main";

    let db0 = genesis_ledger(&fluree, ledger_id);

    let insert_txn = json!({
        "@context": context_ex_schema(),
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 50,
                "ex:favNums": [42, 76, 9]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:favNums": [5, 10],
                "ex:friend": ["ex:brian", "ex:alice"]
            }
        ]
    });

    let ledger = fluree.insert(db0, &insert_txn).await.unwrap().ledger;

    let alice_sid = ledger
        .snapshot
        .encode_iri("http://example.org/ns/alice")
        .unwrap();
    let _cam_sid = ledger
        .snapshot
        .encode_iri("http://example.org/ns/cam")
        .unwrap();

    // Slice for subject id only
    let alice_flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject(alice_sid.clone()),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .unwrap();

    assert_eq!(
        alice_flakes.len(),
        7,
        "Slice should return flakes for only Alice"
    );

    let alice_only_flakes: Vec<_> = alice_flakes
        .into_iter()
        .filter(|f| f.s == alice_sid)
        .collect();
    assert_eq!(alice_only_flakes.len(), 7);

    // Slice for subject + predicate
    let favnums_pid = ledger
        .snapshot
        .encode_iri("http://example.org/ns/favNums")
        .unwrap();

    let alice_favnums_flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(alice_sid.clone(), favnums_pid.clone()),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .unwrap();

    assert_eq!(
        alice_favnums_flakes.len(),
        3,
        "Should return Alice's favNums"
    );

    let values: Vec<_> = alice_favnums_flakes
        .iter()
        .filter_map(|f| match &f.o {
            FlakeValue::Long(v) => Some(*v),
            _ => None,
        })
        .collect();

    assert_eq!(values.len(), 3);
    assert!(values.contains(&42));
    assert!(values.contains(&76));
    assert!(values.contains(&9));

    // Slice for subject + predicate + value
    let alice_favnum_42_flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(alice_sid.clone(), favnums_pid.clone())
            .with_object(FlakeValue::Long(42)),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .unwrap();

    assert_eq!(
        alice_favnum_42_flakes.len(),
        1,
        "Should return only the specific favNum value"
    );

    let flake = &alice_favnum_42_flakes[0];
    assert_eq!(flake.s, alice_sid);
    assert_eq!(flake.p, favnums_pid);
    assert_eq!(flake.o, FlakeValue::Long(42));

    // Slice for subject + predicate + value + datatype
    let integer_dt = ledger
        .snapshot
        .encode_iri("http://www.w3.org/2001/XMLSchema#integer")
        .unwrap();

    let alice_favnum_42_typed_flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(alice_sid.clone(), favnums_pid.clone())
            .with_object(FlakeValue::Long(42))
            .with_datatype(integer_dt),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .unwrap();

    assert_eq!(
        alice_favnum_42_typed_flakes.len(),
        1,
        "Should return favNum with matching datatype"
    );

    // Slice for subject + predicate + value + mismatch datatype
    let string_dt = ledger
        .snapshot
        .encode_iri("http://www.w3.org/2001/XMLSchema#string")
        .unwrap();

    let alice_favnum_42_wrong_type_flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(alice_sid.clone(), favnums_pid.clone())
            .with_object(FlakeValue::Long(42))
            .with_datatype(string_dt),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .unwrap();

    assert_eq!(
        alice_favnum_42_wrong_type_flakes.len(),
        0,
        "Wrong datatype should return no results"
    );
}

// =============================================================================
// UNION query tests (from it_query_union.rs)
// =============================================================================

async fn seed_union_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "ex:last": "Smith",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:favNums": 7,
                "ex:scores": [76, 80, 15]
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "ex:last": "Smith",
                "schema:email": "alice@example.org",
                "ex:favColor": "Green",
                "schema:age": 42,
                "ex:favNums": [42, 76, 9],
                "ex:scores": [102, 92.5, 90]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "ex:last": "Jones",
                "ex:email": "cam@example.org",
                "schema:age": 34,
                "ex:favNums": [5, 10],
                "ex:scores": [97.2, 100, 80],
                "ex:friend": ["ex:brian", "ex:alice"]
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

#[tokio::test]
async fn union_basic_combine_emails() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_union_data(&fluree, "query/union-basic:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["union",
             {"@id": "?s", "ex:email": "?email"},
             {"@id": "?s", "schema:email": "?email"}
            ]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();

    let expected = json!([
        ["Alice", "alice@example.org"],
        ["Brian", "brian@example.org"],
        ["Cam", "cam@example.org"]
    ]);

    assert_eq!(normalize_rows(&rows), normalize_rows(&expected));
}

#[tokio::test]
async fn union_different_variables() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_union_data(&fluree, "query/union-different-vars:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?s", "?email1", "?email2"],
        "where": [
            {"@id": "?s", "@type": "ex:User"},
            ["union",
             {"@id": "?s", "ex:email": "?email1"},
             {"@id": "?s", "schema:email": "?email2"}
            ]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();

    let expected = json!([
        ["ex:alice", null, "alice@example.org"],
        ["ex:brian", null, "brian@example.org"],
        ["ex:cam", "cam@example.org", null]
    ]);

    assert_eq!(normalize_rows(&rows), normalize_rows(&expected));
}

#[tokio::test]
async fn union_passthrough_variables() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_union_data(&fluree, "query/union-passthrough:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?name", "?email1", "?email2"],
        "where": [
            {"@id": "?s", "@type": "ex:User", "schema:name": "?name"},
            ["union",
             {"@id": "?s", "ex:email": "?email1"},
             {"@id": "?s", "schema:email": "?email2"}
            ]
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();

    let expected = json!([
        ["Alice", null, "alice@example.org"],
        ["Brian", null, "brian@example.org"],
        ["Cam", "cam@example.org", null]
    ]);

    assert_eq!(normalize_rows(&rows), normalize_rows(&expected));
}
