//! Scan-side top-k over rows that do not all yield a solution: a module of
//! `it_iceberg_local_fs`, over the `silver.versions` fixture.

use super::{allow_fixture_roots, span_capture, FlureeBuilder, R2rmlCreateConfig};

/// The `(?v, ?n)` rows of a SPARQL (bindings) or JSON-LD (array of rows)
/// result, sorted.
fn version_name_rows(result: &serde_json::Value) -> Vec<Vec<String>> {
    let text = |v: &serde_json::Value| -> String {
        match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Object(o) => o
                .get("value")
                .or_else(|| o.get("@id"))
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("not a term: {v}"))
                .to_string(),
            other => panic!("not a term: {other}"),
        }
    };
    let mut rows: Vec<Vec<String>> =
        if let Some(bindings) = result["results"]["bindings"].as_array() {
            bindings
                .iter()
                .map(|b| vec![text(&b["v"]), text(&b["n"])])
                .collect()
        } else {
            result
                .as_array()
                .unwrap_or_else(|| panic!("not a result: {result}"))
                .iter()
                .map(|row| row.as_array().unwrap().iter().map(text).collect())
                .collect()
        };
    rows.sort();
    rows
}

/// `silver.versions`: the first file holds alice (1) and erin (5); the second
/// the top names, hank (2), gina (3), ivan (6, no city) and zed (no `id`, so
/// no subject). `ex:person` references `silver.people` (ids 1 to 5), so
/// ivan's dangles. Both logical tables declare `id` their key: a map whose
/// rows can repeat a triple declines the top-k, and these tests pin the
/// heap's feed, so their maps are ones whose rows are their solutions.
const VERSIONS_R2RML: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .

    <http://example.org/mapping#Version>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.versions" ; <https://ns.flur.ee/db#key> ( "id" ) ] ;
        rr:subjectMap [ rr:template "http://example.org/version/{id}" ; rr:class ex:Version ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] ;
        rr:predicateObjectMap [
            rr:predicate ex:person ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Person> ;
                rr:joinCondition [ rr:child "id" ; rr:parent "id" ]
            ]
        ] .

    <http://example.org/mapping#Person>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "silver.people" ; <https://ns.flur.ee/db#key> ( "id" ) ] ;
        rr:subjectMap [ rr:template "http://example.org/person/{id}" ] .
"#;

/// The `silver.versions` rows that yield triples, as a ledger holds them:
/// `(id, name, city, person)`.
const VERSION_ROWS: [(u32, &str, Option<&str>, Option<u32>); 5] = [
    (1, "alice", Some("c1"), Some(1)),
    (5, "erin", Some("c5"), Some(5)),
    (2, "hank", Some("c2"), Some(2)),
    (3, "gina", Some("c3"), Some(3)),
    (6, "ivan", None, None),
];

async fn versions_with_oracle(fluree: &fluree_db_api::Fluree, alias: &str, oracle: &str) {
    let root = format!(
        "file://{}/tests/fixtures/iceberg/silver",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = R2rmlCreateConfig::new_direct(alias, &root, VERSIONS_R2RML)
        .with_mapping_media_type("text/turtle");
    fluree
        .create_r2rml_graph_source(config)
        .await
        .expect("create graph source");
    let ledger = fluree.create_ledger(oracle).await.expect("oracle ledger");
    let graph: Vec<serde_json::Value> = VERSION_ROWS
        .iter()
        .map(|&(id, name, city, person_id)| {
            let mut node = serde_json::json!({
                "@id": format!("http://example.org/version/{id}"),
                "@type": "http://example.org/Version",
                "http://example.org/name": name,
            });
            if let Some(city) = city {
                node["http://example.org/city"] = serde_json::json!(city);
            }
            if let Some(p) = person_id {
                node["http://example.org/person"] =
                    serde_json::json!({"@id": format!("http://example.org/person/{p}")});
            }
            node
        })
        .collect();
    fluree
        .insert(ledger, &serde_json::json!({"@graph": graph}))
        .await
        .expect("oracle insert");
}

/// `SELECT ?v ?n WHERE { ?v [a ex:Version ;] ex:name ?n [; ex:{member} ?m] }
/// ORDER BY DESC(?n) LIMIT k` over `from`, on both surfaces.
async fn top_names(
    fluree: &fluree_db_api::Fluree,
    from: &str,
    typed: bool,
    member: Option<&str>,
    k: usize,
) -> [Vec<Vec<String>>; 2] {
    let ex = |p: &str| format!("http://example.org/{p}");
    let mut sparql_triples = Vec::new();
    if typed {
        sparql_triples.push(format!("a <{}>", ex("Version")));
    }
    sparql_triples.push(format!("<{}> ?n", ex("name")));
    if let Some(m) = member {
        sparql_triples.push(format!("<{}> ?m", ex(m)));
    }
    let sparql = format!(
        "SELECT ?v ?n FROM <{from}> WHERE {{ ?v {} }} ORDER BY DESC(?n) LIMIT {k}",
        sparql_triples.join(" ; ")
    );
    let sparql_result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    let mut node = serde_json::json!({"@id": "?v", ex("name"): "?n"});
    if typed {
        node["@type"] = serde_json::json!(ex("Version"));
    }
    if let Some(m) = member {
        node[ex(m)] = serde_json::json!("?m");
    }
    let jsonld = serde_json::json!({
        "from": from,
        "select": ["?v", "?n"],
        "where": node,
        "orderBy": "(desc ?n)",
        "limit": k,
    });
    let jsonld_result = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("JSON-LD over {from}: {e}"));
    [
        version_name_rows(&sparql_result),
        version_name_rows(&jsonld_result),
    ]
}

fn version_rows(rows: &[(u32, &str)]) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .iter()
        .map(|&(id, n)| vec![format!("http://example.org/version/{id}"), n.to_string()])
        .collect();
    rows.sort();
    rows
}

/// A scan-side top-k counts only rows that yield a solution. In the second
/// file, which it reads first, zed has no subject (a NULL `id`), ivan no city
/// (a NULL star member) and no person (a dangling reference). Counted, any of
/// them fills the bound above the first file's erin, which belongs in the
/// answer, and that file is skipped. Both surfaces answer as a ledger holding
/// the same triples does.
#[tokio::test]
async fn a_top_k_counts_only_rows_that_yield_a_solution() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    versions_with_oracle(&fluree, "versions", "versions-oracle:main").await;

    let mut wrong = Vec::new();
    for (label, typed, member, k, want) in [
        (
            "a row with no subject",
            false,
            None,
            4,
            version_rows(&[(6, "ivan"), (2, "hank"), (3, "gina"), (5, "erin")]),
        ),
        (
            "a typed row with no subject",
            true,
            None,
            4,
            version_rows(&[(6, "ivan"), (2, "hank"), (3, "gina"), (5, "erin")]),
        ),
        (
            "a star row with a NULL member",
            false,
            Some("city"),
            3,
            version_rows(&[(2, "hank"), (3, "gina"), (5, "erin")]),
        ),
        (
            "a star row with a dangling reference",
            false,
            Some("person"),
            3,
            version_rows(&[(2, "hank"), (3, "gina"), (5, "erin")]),
        ),
    ] {
        for from in ["versions:main", "versions-oracle:main"] {
            let answers = top_names(&fluree, from, typed, member, k).await;
            for (surface, rows) in ["SPARQL", "JSON-LD"].iter().zip(answers) {
                if rows != want {
                    wrong.push(format!("{label}, {from} {surface}: {rows:?}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "wrong answers:\n{}", wrong.join("\n"));
}

/// The top-k still prunes when the first file it reads holds the answer: its
/// rows that yield no solution are skipped, the rest fill the bound, and the
/// other file is never read.
#[tokio::test(flavor = "current_thread")]
async fn a_top_k_skipping_rows_without_a_solution_still_prunes() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    versions_with_oracle(&fluree, "versions-prune", "versions-prune-oracle:main").await;
    let (store, _tracing_guard) = span_capture::init_test_tracing();
    for (member, k, want) in [
        (None, 2, version_rows(&[(6, "ivan"), (2, "hank")])),
        (Some("city"), 2, version_rows(&[(2, "hank"), (3, "gina")])),
    ] {
        let seen = store.find_spans("iceberg.scan_plan").len();
        let [sparql, jsonld] = top_names(&fluree, "versions-prune:main", false, member, k).await;
        assert_eq!(sparql, want, "SPARQL {member:?}");
        assert_eq!(jsonld, want, "JSON-LD {member:?}");
        // Each surface plans the scan (both files), then the top-k reports
        // the files it read and skipped.
        let counts: Vec<(String, String)> = store.find_spans("iceberg.scan_plan")[seen..]
            .iter()
            .map(|s| {
                let field = |f: &str| s.fields.get(f).cloned().unwrap_or_default();
                (field("files_selected"), field("files_pruned"))
            })
            .filter(|(_, pruned)| pruned != "0")
            .collect();
        let read_one = ("1".to_string(), "1".to_string());
        assert_eq!(
            counts,
            [read_one.clone(), read_one],
            "each surface's top-k reads one file and skips the other: {member:?}"
        );
    }
}
