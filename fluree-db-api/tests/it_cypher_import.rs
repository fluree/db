//! Cypher bulk import (.cypher CREATE scripts → JSON-LD front-end) — end to
//! end. Proves a Cypher-dump-loaded dataset is queryable from zero-config
//! Cypher, including edge properties carried as `@annotation`.

mod support;

use fluree_db_api::cypher_import::{
    cypher_to_jsonld, CypherImportError, CypherImportOptions, CypherImporter, EdgePolicy,
};
use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, graphdb_from_ledger};

/// Spike-proven contract the importer depends on: bare (relative) `@id`s and
/// bare terms in a JSON-LD insert unify with zero-config Cypher reads.
#[tokio::test]
async fn bare_id_jsonld_insert_reads_from_cypher() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher-import:bare-id");
    let doc = json!({"@graph": [
        {"@id": "User/1", "@type": "User", "id": 1, "name": "a"},
        {"@id": "User/2", "@type": "User", "id": 2, "name": "b"},
        {"@id": "User/1", "Friend": {"@id": "User/2"}}
    ]});
    let l = fluree.insert(ledger0, &doc).await.expect("insert").ledger;
    let db = graphdb_from_ledger(&l);

    let res = fluree
        .query_cypher(&db, "MATCH (n:User {id: 1})-[:Friend]->(m) RETURN m.id")
        .await
        .expect("cypher")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, json!([[2]]), "{res}");
}

// The benchgraph/pokec dump idiom: node CREATEs then MATCH…CREATE edges.
const POKEC_SHAPED: &str = r#"
CREATE (:User {id: 1, gender: "man", age: 26});
CREATE (:User {id: 2, gender: "woman", age: 0});
CREATE (:User {id: 3, completion_percentage: 12});
MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[e: Friend]->(m);
MATCH (n:User {id: 2}), (m:User {id: 3}) CREATE (n)-[e: Friend]->(m);
"#;

#[test]
fn pokec_shaped_conversion_mints_key_derived_ids() {
    let objs = cypher_to_jsonld(POKEC_SHAPED, &CypherImportOptions::default()).expect("convert");
    assert_eq!(objs.len(), 5, "{objs:#?}");
    assert_eq!(
        objs[0],
        json!({"@id": "User/1", "@type": "User", "id": 1, "gender": "man", "age": 26})
    );
    // Property-less edges are plain triples (no @annotation).
    assert_eq!(
        objs[3],
        json!({"@id": "User/1", "Friend": {"@id": "User/2"}})
    );
    assert_eq!(
        objs[4],
        json!({"@id": "User/2", "Friend": {"@id": "User/3"}})
    );
}

#[tokio::test]
async fn pokec_shaped_round_trips_to_cypher() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher-import:pokec");
    let objs = cypher_to_jsonld(POKEC_SHAPED, &CypherImportOptions::default()).expect("convert");
    let l = fluree
        .insert(ledger0, &json!({"@graph": objs}))
        .await
        .expect("insert")
        .ledger;
    let db = graphdb_from_ledger(&l);

    // Anchored lookup + traversal, zero-config (bare ns-0 names).
    let res = fluree
        .query_cypher(
            &db,
            "MATCH (n:User {id: 1})-[:Friend]->(m)-[:Friend]->(o) RETURN n.age, m.gender, o.completion_percentage",
        )
        .await
        .expect("cypher")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, json!([[26, "woman", 12]]), "{res}");

    // Untyped traversal (the benchgraph query idiom) sees the edges too.
    let count = fluree
        .query_cypher(&db, "MATCH (n:User)-->(m) RETURN count(m)")
        .await
        .expect("cypher")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(count, json!([[2]]), "{count}");
}

#[tokio::test]
async fn edge_properties_become_annotations_readable_from_rel_var() {
    let script = r#"
CREATE (:Person {id: 10, name: "Alice"});
CREATE (:Person {id: 20, name: "Bob"});
MATCH (a:Person {id: 10}), (b:Person {id: 20})
CREATE (a)-[k:KNOWS {since: 1999, weight: 0.5}]->(b);
"#;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher-import:edge-props");
    let objs = cypher_to_jsonld(script, &CypherImportOptions::default()).expect("convert");
    let l = fluree
        .insert(ledger0, &json!({"@graph": objs}))
        .await
        .expect("insert")
        .ledger;
    let db = graphdb_from_ledger(&l);

    let res = fluree
        .query_cypher(
            &db,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r.since, b.name",
        )
        .await
        .expect("cypher")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    assert_eq!(res, json!([["Alice", 1999, "Bob"]]), "{res}");
}

#[test]
fn plain_policy_drops_edge_properties() {
    let script = r"
CREATE (:P {id: 1});
CREATE (:P {id: 2});
MATCH (a:P {id: 1}), (b:P {id: 2}) CREATE (a)-[:R {w: 3}]->(b);
";
    let opts = CypherImportOptions {
        edge_policy: EdgePolicy::Plain,
        ..Default::default()
    };
    let objs = cypher_to_jsonld(script, &opts).expect("convert");
    assert_eq!(
        objs[2],
        json!({"@id": "P/1", "R": {"@id": "P/2"}}),
        "{objs:#?}"
    );
}

#[test]
fn inline_path_create_and_incoming_direction() {
    // One CREATE with a whole path, a reused variable, and a reversed arrow.
    let script = r#"
CREATE (a:City {name: "Oslo"})-[:IN]->(c:Country {name: "Norway"}),
       (b:City {name: "Bergen"})-[:IN]->(c),
       (a)<-[:CAPITAL_OF]-(c);
"#;
    let objs = cypher_to_jsonld(script, &CypherImportOptions::default()).expect("convert");
    // No MATCH statements → no learned keys → sequential anon ids.
    assert_eq!(
        objs,
        json!([
            {"@id": "City/_anon-1", "@type": "City", "name": "Oslo"},
            {"@id": "Country/_anon-2", "@type": "Country", "name": "Norway"},
            {"@id": "City/_anon-1", "IN": {"@id": "Country/_anon-2"}},
            {"@id": "City/_anon-3", "@type": "City", "name": "Bergen"},
            {"@id": "City/_anon-3", "IN": {"@id": "Country/_anon-2"}},
            {"@id": "Country/_anon-2", "CAPITAL_OF": {"@id": "City/_anon-1"}},
        ])
        .as_array()
        .unwrap()
        .as_slice(),
        "{objs:#?}"
    );
}

#[test]
fn vocab_mode_prefixes_all_names() {
    let opts = CypherImportOptions {
        vocab: Some("http://example.org/".to_string()),
        ..Default::default()
    };
    let objs = cypher_to_jsonld(
        "CREATE (:User {id: 1});\nMATCH (n:User {id: 1}), (m:User {id: 1}) CREATE (n)-[:F]->(m);",
        &opts,
    )
    .expect("convert");
    assert_eq!(
        objs[0],
        json!({"@id": "http://example.org/User/1", "@type": "http://example.org/User", "http://example.org/id": 1})
    );
    assert_eq!(
        objs[1],
        json!({"@id": "http://example.org/User/1", "http://example.org/F": {"@id": "http://example.org/User/1"}})
    );
}

#[test]
fn statement_splitter_respects_strings_and_comments() {
    // Semicolons inside strings/comments must not split; comments vanish.
    let script = "// header comment; not a statement\n\
        CREATE (:N {v: \"a;b\", w: 'c;d'}); /* mid ; comment */ CREATE (:N {v: \"e\"});\n\
        CREATE (:N {v: \"multi\nline\"})";
    let objs = cypher_to_jsonld(script, &CypherImportOptions::default()).expect("convert");
    assert_eq!(objs.len(), 3, "{objs:#?}");
    assert_eq!(objs[0]["v"], json!("a;b"));
    assert_eq!(objs[0]["w"], json!("c;d"));
    assert_eq!(objs[2]["v"], json!("multi\nline"));
}

#[test]
fn dangling_edge_is_skipped_and_counted() {
    let script = r"
CREATE (:U {id: 1});
MATCH (a:U {id: 1}), (b:U {id: 99}) CREATE (a)-[:F]->(b);
";
    let mut importer = CypherImporter::new(CypherImportOptions::default());
    importer.learn_keys(script.as_bytes()).expect("keys");
    let mut buf = Vec::new();
    importer
        .write_nodes_ndjson(script.as_bytes(), &mut buf)
        .expect("nodes");
    let edges = importer
        .write_edges_ndjson(script.as_bytes(), &mut buf)
        .expect("edges");
    assert_eq!(edges, 0);
    assert_eq!(importer.stats.edges_skipped, 1);
    assert_eq!(importer.stats.nodes, 1);
}

#[test]
fn unsupported_statements_error_with_line_numbers() {
    let merge = cypher_to_jsonld(
        "CREATE (:U {id: 1});\nMERGE (n:U {id: 1});",
        &CypherImportOptions::default(),
    );
    match merge {
        Err(CypherImportError::Unsupported { line, msg }) => {
            assert_eq!(line, 2, "{msg}");
            assert!(msg.contains("MERGE"), "{msg}");
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }

    let read_only = cypher_to_jsonld("MATCH (n) RETURN n;", &CypherImportOptions::default());
    assert!(
        matches!(read_only, Err(CypherImportError::Unsupported { .. })),
        "{read_only:?}"
    );

    let param = cypher_to_jsonld("CREATE (:U {id: $id});", &CypherImportOptions::default());
    assert!(
        matches!(param, Err(CypherImportError::Unsupported { .. })),
        "{param:?}"
    );
}

#[test]
fn conflicting_match_key_sets_error() {
    let script = "MATCH (a:U {id: 1}), (b:U {name: \"x\"}) CREATE (a)-[:F]->(b);";
    let mut importer = CypherImporter::new(CypherImportOptions::default());
    let err = importer.learn_keys(script.as_bytes());
    assert!(
        matches!(err, Err(CypherImportError::ConflictingMatchKeys { .. })),
        "{err:?}"
    );
}
