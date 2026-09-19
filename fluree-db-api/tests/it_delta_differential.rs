//! A Delta graph source must answer every query exactly as a native ledger
//! holding the same triples does.
//!
//! The Delta scan skips files by partition value and statistics, filters rows
//! by the comparisons it can state exactly and leaves the rest to the engine,
//! and answers bare counts from the log. A row dropped that the engine would
//! keep, or kept where the engine assumed it filtered, is a difference here.
//! The native twin is restated from the fixture generators' inputs, not read
//! back through the reader.
//!
//! Run with:
//!   cargo test -p fluree-db-api --features delta --test it_delta_differential

#![cfg(all(feature = "delta", feature = "native"))]

use fluree_db_api::{DeltaCreateConfig, Fluree, FlureeBuilder};
use serde_json::{json, Value};

const DELTA: &str = "delta-twin:main";
const NATIVE: &str = "native-twin:main";

const MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Sale>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "partitioned" ] ;
        rr:subjectMap [ rr:template "http://example.org/sale/{id}" ; rr:class ex:Sale ] ;
        rr:predicateObjectMap [ rr:predicate ex:saleId ; rr:objectMap [ rr:column "id" ; rr:datatype xsd:integer ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:amount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:region ; rr:objectMap [ rr:column "region" ] ] .

    <http://example.org/mapping#Store>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "dim_store" ] ;
        rr:subjectMap [ rr:template "http://example.org/store/{store_id}" ; rr:class ex:Store ] ;
        rr:predicateObjectMap [ rr:predicate ex:storeName ; rr:objectMap [ rr:column "name" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "fact_order" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:orderRegion ; rr:objectMap [ rr:column "region" ] ] ;
        rr:predicateObjectMap [
            rr:predicate ex:store ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Store> ;
                rr:joinCondition [ rr:child "store_id" ; rr:parent "store_id" ]
            ]
        ] .

    <http://example.org/mapping#Flag>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "flags" ] ;
        rr:subjectMap [ rr:template "http://example.org/flag/{id}" ; rr:class ex:Flag ] ;
        rr:predicateObjectMap [ rr:predicate ex:shipped ; rr:objectMap [ rr:column "shipped" ; rr:datatype xsd:boolean ] ] .
"#;

/// The triples `MAPPING` yields, from the generators' inputs: `partitioned` is
/// six files of four rows; `fact_order` at its latest version is orders 2–7
/// with a null amount (3) and a null region (4); `flags` has a null flag (5).
fn native_rows() -> Value {
    let mut graph = Vec::new();
    for wave in 0..2 {
        for (r, region) in ["east", "north", "west"].iter().enumerate() {
            for i in 0..4 {
                let id = (wave * 3 + r as i64) * 100 + i;
                graph.push(json!({
                    "@id": format!("ex:sale/{id}"), "@type": "ex:Sale",
                    "ex:saleId": id, "ex:amount": id * 10, "ex:region": region,
                }));
            }
        }
    }
    for (id, name) in [(1, "East shop"), (2, "West shop"), (3, "Unassigned shop")] {
        graph.push(
            json!({"@id": format!("ex:store/{id}"), "@type": "ex:Store", "ex:storeName": name}),
        );
    }
    let orders: [(i64, i64, Option<i64>, Option<&str>); 6] = [
        (2, 2, Some(250), Some("west")),
        (3, 1, None, Some("east")),
        (4, 3, Some(400), None),
        (5, 2, Some(500), Some("west")),
        (6, 1, Some(600), Some("east")),
        (7, 1, Some(700), Some("east")),
    ];
    for (id, store, total, region) in orders {
        let mut order = json!({
            "@id": format!("ex:order/{id}"), "@type": "ex:Order",
            "ex:store": {"@id": format!("ex:store/{store}")},
        });
        if let Some(total) = total {
            order["ex:total"] = json!(total);
        }
        if let Some(region) = region {
            order["ex:orderRegion"] = json!(region);
        }
        graph.push(order);
    }
    for (id, shipped) in [
        (1, Some(true)),
        (2, Some(true)),
        (3, Some(false)),
        (4, Some(true)),
        (5, None),
        (6, Some(false)),
    ] {
        let mut flag = json!({"@id": format!("ex:flag/{id}"), "@type": "ex:Flag"});
        if let Some(shipped) = shipped {
            flag["ex:shipped"] = json!(shipped);
        }
        graph.push(flag);
    }
    json!({"@context": {"ex": "http://example.org/"}, "@graph": graph})
}

/// The fixtures directory. Every test must install the same allowlist: the
/// guard reads it once per process.
fn fixtures() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fluree-db-delta/tests/fixtures")
        .canonicalize()
        .expect("fixtures dir");
    let temp = std::env::temp_dir().canonicalize().expect("temp dir");
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!("{}:{}", root.display(), temp.display()),
    );
    root
}

async fn twins() -> Fluree {
    let root = fixtures();
    let fluree = FlureeBuilder::memory().build_memory();

    let mut config = DeltaCreateConfig::new("delta-twin", root.to_str().unwrap(), MAPPING);
    config.mapping_media_type = Some("text/turtle".to_string());
    let created = fluree
        .create_delta_graph_source(config)
        .await
        .expect("delta twin");
    assert_eq!(created.table_warnings, Vec::<String>::new());

    let ledger = fluree.create_ledger(NATIVE).await.expect("native twin");
    fluree
        .insert(ledger, &native_rows())
        .await
        .expect("seed native twin");
    fluree
}

/// Solutions as an order-insensitive multiset of rendered rows.
async fn solutions(fluree: &Fluree, source: &str, body: &str) -> Vec<String> {
    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n{}",
        body.replace("$FROM", &format!("FROM <{source}>"))
    );
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{source}: {sparql}\n{e}"));
    let mut rows: Vec<String> = result["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings of {sparql}: {result}"))
        .iter()
        .map(|row| {
            let mut vars: Vec<(&String, &Value)> = row.as_object().expect("row").iter().collect();
            vars.sort_by_key(|(name, _)| name.as_str());
            vars.iter()
                .map(|(name, term)| format!("{name}={}^^{}", term["value"], term["datatype"]))
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    rows.sort();
    rows
}

/// Query bodies with `$FROM` where the dataset clause goes. Each is annotated
/// with the engine path it is here to exercise.
const CORPUS: &[&str] = &[
    // Whole-table counts: the log-only row count, with and without a null.
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s a ex:Sale }",
    "SELECT (COUNT(*) AS ?n) $FROM WHERE { ?s ex:amount ?a }",
    "SELECT (COUNT(?t) AS ?n) $FROM WHERE { ?o a ex:Order ; ex:total ?t }",
    "SELECT (COUNT(?o) AS ?n) $FROM WHERE { ?o a ex:Order }",
    "SELECT (COUNT(?b) AS ?n) $FROM WHERE { ?f ex:shipped ?b }",
    "SELECT (COUNT(*) AS ?n) $FROM WHERE { ?o ex:total ?t }",
    "SELECT (COUNT(*) AS ?n) $FROM WHERE { ?f ex:shipped ?b }",
    "SELECT (COUNT(?o) AS ?n) $FROM WHERE { ?o ex:orderRegion ?r }",
    // Every comparison, on a statistics column and on the partition column.
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id = 203) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id = 250) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id != 203) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id < 102) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id <= 102) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id > 401) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id >= 401) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id > 101 && ?id < 302) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id < 2 || ?id > 502) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id IN (1, 250, 402)) }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id NOT IN (1, 250, 402)) }",
    "SELECT ?s $FROM WHERE { ?s ex:region ?r FILTER(?r = \"north\") }",
    "SELECT ?s $FROM WHERE { ?s ex:region ?r FILTER(?r != \"north\") }",
    "SELECT ?s $FROM WHERE { ?s ex:region ?r FILTER(?r > \"f\") }",
    "SELECT ?s $FROM WHERE { ?s ex:region ?r FILTER(?r IN (\"east\", \"south\")) }",
    "SELECT ?s $FROM WHERE { ?s ex:region ?r ; ex:amount ?a FILTER(?r = \"west\" && ?a > 3000) }",
    "SELECT ?s $FROM WHERE { ?s ex:region ?r FILTER(STRSTARTS(?r, \"n\")) }",
    "SELECT ?s ?a $FROM WHERE { VALUES ?id { 0 203 999 } ?s ex:saleId ?id ; ex:amount ?a }",
    // Constant objects and subjects, plain and under each aggregate lane.
    "SELECT ?s $FROM WHERE { ?s ex:region \"north\" }",
    "SELECT ?s $FROM WHERE { ?s ex:saleId 203 }",
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s ex:region \"north\" }",
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s ex:saleId 203 }",
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s a ex:Sale ; ex:region \"north\" }",
    "SELECT (SUM(?a) AS ?t) (MIN(?a) AS ?lo) (MAX(?a) AS ?hi) (AVG(?a) AS ?mean) $FROM WHERE { ?s ex:region \"west\" ; ex:amount ?a }",
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s ex:shipped true }",
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s ex:shipped ?b FILTER(?b = false) }",
    "SELECT ?a ?r $FROM WHERE { <http://example.org/sale/203> ex:amount ?a ; ex:region ?r }",
    "SELECT ?a $FROM WHERE { <http://example.org/sale/250> ex:amount ?a }",
    // Filtered and grouped aggregates.
    "SELECT (COUNT(?s) AS ?n) (SUM(?a) AS ?t) $FROM WHERE { ?s ex:saleId ?id ; ex:amount ?a FILTER(?id >= 300) }",
    "SELECT (COUNT(?s) AS ?n) $FROM WHERE { ?s ex:saleId ?id FILTER(?id = 250) }",
    "SELECT (SUM(?a) AS ?t) $FROM WHERE { ?s ex:saleId ?id ; ex:amount ?a FILTER(?id = 250) }",
    "SELECT ?r (COUNT(?s) AS ?n) (SUM(?a) AS ?t) (MIN(?a) AS ?lo) (MAX(?a) AS ?hi) $FROM WHERE { ?s ex:region ?r ; ex:amount ?a } GROUP BY ?r",
    "SELECT ?r (COUNT(?s) AS ?n) $FROM WHERE { ?s ex:region ?r ; ex:saleId ?id FILTER(?id > 250) } GROUP BY ?r",
    "SELECT ?r (SUM(?a) AS ?t) $FROM WHERE { ?s ex:region ?r ; ex:amount ?a } GROUP BY ?r HAVING(SUM(?a) > 10000)",
    "SELECT (COUNT(DISTINCT ?r) AS ?n) $FROM WHERE { ?s ex:region ?r }",
    "SELECT DISTINCT ?r $FROM WHERE { ?s ex:region ?r ; ex:saleId ?id FILTER(?id < 300) }",
    "SELECT ?r (COUNT(?o) AS ?n) $FROM WHERE { ?o a ex:Order ; ex:orderRegion ?r } GROUP BY ?r",
    // Top-k: the scan ignores the directive, the sort above must not care.
    "SELECT ?a $FROM WHERE { ?s ex:amount ?a } ORDER BY ?a",
    "SELECT ?t $FROM WHERE { ?o ex:total ?t } ORDER BY DESC(?t)",
    "SELECT ?s ?a $FROM WHERE { ?s ex:amount ?a } ORDER BY DESC(?a) LIMIT 3",
    "SELECT ?s ?a $FROM WHERE { ?s ex:amount ?a } ORDER BY ?a LIMIT 3 OFFSET 2",
    "SELECT ?s ?a $FROM WHERE { ?s ex:amount ?a ; ex:saleId ?id FILTER(?id < 300) } ORDER BY DESC(?a) LIMIT 2",
    "SELECT ?s ?a $FROM WHERE { ?s ex:amount ?a ; ex:region \"north\" } ORDER BY DESC(?a) LIMIT 2",
    "SELECT ?o ?t $FROM WHERE { ?o ex:total ?t } ORDER BY ?t LIMIT 2",
    // Nulls: absent triples, OPTIONAL, negation.
    "SELECT ?o ?t $FROM WHERE { ?o a ex:Order OPTIONAL { ?o ex:total ?t } }",
    "SELECT ?o $FROM WHERE { ?o a ex:Order OPTIONAL { ?o ex:total ?t } FILTER(!BOUND(?t)) }",
    "SELECT ?o $FROM WHERE { ?o a ex:Order FILTER NOT EXISTS { ?o ex:orderRegion ?r } }",
    "SELECT ?o $FROM WHERE { ?o a ex:Order MINUS { ?o ex:orderRegion \"east\" } }",
    "SELECT ?f $FROM WHERE { ?f a ex:Flag FILTER NOT EXISTS { ?f ex:shipped ?b } }",
    // Joins across tables, with the restriction on either side.
    "SELECT ?o ?name $FROM WHERE { ?o ex:store ?st . ?st ex:storeName ?name }",
    "SELECT ?o $FROM WHERE { ?o ex:store ?st . ?st ex:storeName \"East shop\" }",
    "SELECT (COUNT(?o) AS ?n) $FROM WHERE { ?o ex:store ?st . ?st ex:storeName \"East shop\" }",
    "SELECT ?name (COUNT(?o) AS ?n) (SUM(?t) AS ?sum) $FROM WHERE { ?o ex:store ?st ; ex:total ?t . ?st ex:storeName ?name } GROUP BY ?name",
    "SELECT ?name (SUM(?t) AS ?sum) $FROM WHERE { ?o ex:store ?st ; ex:total ?t . ?st ex:storeName ?name FILTER(?t > 300) } GROUP BY ?name",
    "SELECT ?o $FROM WHERE { ?o ex:store <http://example.org/store/2> }",
    "SELECT ?name $FROM WHERE { ?st ex:storeName ?name FILTER NOT EXISTS { ?o ex:store ?st ; ex:total ?t FILTER(?t > 450) } }",
    "SELECT ?x $FROM WHERE { { ?x ex:region \"north\" ; ex:saleId ?id FILTER(?id > 400) } UNION { ?x ex:orderRegion \"west\" } }",
    "SELECT ?s ?o $FROM WHERE { ?s ex:saleId ?id . ?o ex:total ?t FILTER(?id = 500 && ?t = ?id) }",
];

#[tokio::test]
async fn a_delta_source_answers_as_its_native_twin_does() {
    let fluree = twins().await;
    let mut differences = Vec::new();
    let mut non_empty = 0;
    for body in CORPUS {
        let native = solutions(&fluree, NATIVE, body).await;
        let delta = solutions(&fluree, DELTA, body).await;
        non_empty += usize::from(!native.is_empty());
        if native != delta {
            differences.push(format!("{body}\n  native: {native:?}\n  delta:  {delta:?}"));
        }
    }
    assert!(
        differences.is_empty(),
        "{} of {} queries differ:\n{}",
        differences.len(),
        CORPUS.len(),
        differences.join("\n")
    );
    // Guard against the corpus agreeing because both sides returned nothing.
    assert!(
        non_empty >= CORPUS.len() - 8,
        "only {non_empty} queries returned rows"
    );
}

fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// Routing stamps. Agreement with the native twin cannot show that a filter
/// skipped files or that a count came from the log — a full scan agrees too.
/// Here the `north` partition's data files are gone: a query succeeds only if
/// it never opens them.
#[tokio::test]
async fn a_filter_skips_files_and_a_bare_count_reads_none() {
    let root = fixtures();
    let lake = tempfile::tempdir().unwrap();
    let lake_root = lake.path().canonicalize().unwrap();
    copy_dir_all(&root.join("partitioned"), &lake_root.join("partitioned"));
    std::fs::remove_dir_all(lake_root.join("partitioned/region=north")).unwrap();

    let fluree = FlureeBuilder::memory().build_memory();
    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <http://example.org/mapping#Sale> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "partitioned" ] ;
            rr:subjectMap [ rr:template "http://example.org/sale/{id}" ; rr:class ex:Sale ] ;
            rr:predicateObjectMap [ rr:predicate ex:saleId ; rr:objectMap [ rr:column "id" ; rr:datatype xsd:integer ] ] ;
            rr:predicateObjectMap [ rr:predicate ex:region ; rr:objectMap [ rr:column "region" ] ] .
    "#;
    let mut config = DeltaCreateConfig::new("holed", lake_root.to_str().unwrap(), mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    fluree
        .create_delta_graph_source(config)
        .await
        .expect("registration reads the log, not the data");

    let run = |body: &'static str| {
        let fluree = fluree.clone();
        async move {
            let sparql = format!(
                "PREFIX ex: <http://example.org/>\n{}",
                body.replace("$FROM", "FROM <holed:main>")
            );
            fluree
                .query_from()
                .sparql(&sparql)
                .execute_formatted()
                .await
                .map(|v| v["results"]["bindings"].as_array().expect("bindings").len())
        }
    };

    // Partition value, statistics, a constant object, a bound subject: each
    // keeps the scan out of `region=north`.
    for (body, rows) in [
        (
            "SELECT ?s $FROM WHERE { ?s ex:region ?r FILTER(?r = \"west\") }",
            8,
        ),
        (
            "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id < 100) }",
            4,
        ),
        (
            "SELECT ?s $FROM WHERE { ?s ex:saleId ?id FILTER(?id IN (1, 502)) }",
            2,
        ),
        ("SELECT ?s $FROM WHERE { ?s ex:region \"east\" }", 8),
        (
            "SELECT ?r $FROM WHERE { <http://example.org/sale/203> ex:region ?r }",
            1,
        ),
    ] {
        match run(body).await {
            Ok(n) => assert_eq!(n, rows, "{body}"),
            Err(e) => panic!("{body} opened a file its filter excludes: {e}"),
        }
    }

    // The same table without a usable filter does need those files.
    let err = run("SELECT ?s $FROM WHERE { ?s ex:region ?r }")
        .await
        .expect_err("a full scan reads every file");
    assert!(err.to_string().contains("can no longer be read"), "{err}");

    // A bare count is answered by the log: all 24 rows, no file opened.
    let count = fluree
        .query_from()
        .sparql(
            "PREFIX ex: <http://example.org/> \
             SELECT (COUNT(?s) AS ?n) FROM <holed:main> WHERE { ?s a ex:Sale }",
        )
        .execute_formatted()
        .await
        .expect("count from the log");
    assert_eq!(
        count["results"]["bindings"][0]["n"]["value"], "24",
        "{count}"
    );
}
