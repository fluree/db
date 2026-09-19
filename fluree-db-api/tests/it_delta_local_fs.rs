//! Delta graph sources end to end, against the committed fixtures under
//! `fluree-db-delta/tests/fixtures` (see that crate's `tests/reader.rs` for
//! their provenance). Expected values are restated from the generators' inputs.
//!
//! Run with:
//!   cargo test -p fluree-db-api --features delta --test it_delta_local_fs

#![cfg(all(feature = "delta", feature = "native"))]

use fluree_db_api::{ApiError, DeltaCreateConfig, Fluree, FlureeBuilder, TimeSpec};
use serde_json::{json, Value};

/// Commit times Delta Spark recorded in the `in_commit_time` fixture's log:
/// v0 = {1}, v1 = {1, 2}, v2 = {2}.
const ICT: [i64; 3] = [1_789_829_202_524, 1_789_829_202_991, 1_789_829_203_546];

const MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Item>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "in_commit_time" ] ;
        rr:subjectMap [ rr:template "http://example.org/item/{id}" ; rr:class ex:Item ] ;
        rr:predicateObjectMap [ rr:predicate ex:amount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .

    <http://example.org/mapping#Reading>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "deletion_vectors" ] ;
        rr:subjectMap [ rr:template "http://example.org/reading/{id}" ; rr:class ex:Reading ] ;
        rr:predicateObjectMap [ rr:predicate ex:value ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .

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
        rr:predicateObjectMap [
            rr:predicate ex:store ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Store> ;
                rr:joinCondition [ rr:child "store_id" ; rr:parent "store_id" ]
            ]
        ] .

    <http://example.org/mapping#Mapped>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "column_mapping" ] ;
        rr:subjectMap [ rr:template "http://example.org/mapped/{id}" ; rr:class ex:Mapped ] ;
        rr:predicateObjectMap [ rr:predicate ex:mappedAmount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .
"#;

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../fluree-db-delta/tests/fixtures")
}

/// Every test must install the same allowlist: the guard reads it once.
fn allow_fixture_roots() {
    let root = fixtures().canonicalize().expect("fixtures dir");
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", root);
}

async fn source(name: &str) -> (Fluree, String) {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mut config = DeltaCreateConfig::new(name, root.to_str().unwrap(), MAPPING);
    config.mapping_media_type = Some("text/turtle".to_string());
    let created = fluree
        .create_delta_graph_source(config)
        .await
        .expect("create delta graph source");
    assert!(created.mapping_validated);
    assert_eq!(created.triples_map_count, 5);
    assert_eq!(
        created.table_warnings,
        Vec::<String>::new(),
        "every mapped fixture table opens at registration"
    );
    assert_eq!(created.table_versions["in_commit_time"], 2);
    assert_eq!(created.table_versions["fact_order"], 4);
    (fluree, format!("{name}:main"))
}

fn iso(ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .expect("valid ms")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn sorted_ints(rows: &Value) -> Vec<i64> {
    let mut out: Vec<i64> = rows
        .as_array()
        .expect("array result")
        .iter()
        .map(|r| {
            r.as_i64()
                .or_else(|| r[0].as_i64())
                .unwrap_or_else(|| panic!("integer row, got {r} in {rows}"))
        })
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test]
async fn delta_tables_query_through_r2rml() {
    let (fluree, alias) = source("delta-basic").await;

    // Deletion vectors: 4096 rows written, 9 deleted in place.
    let count = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(count ?s)"],
        "where": {"@id": "?s", "@type": "ex:Reading"},
    });
    let n = fluree
        .graph(&alias)
        .query()
        .jsonld(&count)
        .execute_formatted()
        .await
        .expect("count readings");
    assert_eq!(sorted_ints(&n), [4087], "got: {n}");

    // A deleted row is gone; its neighbour is not.
    for (id, expected) in [(1024, vec![]), (1025, vec![10250])] {
        let one = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?v"],
            "where": {"@id": format!("ex:reading/{id}"), "ex:value": "?v"},
        });
        let rows = fluree
            .graph(&alias)
            .query()
            .jsonld(&one)
            .execute_formatted()
            .await
            .expect("reading lookup");
        assert_eq!(sorted_ints(&rows), expected, "reading {id}: {rows}");
    }

    // A join across two Delta tables, SPARQL twin included. Orders 2..7 remain
    // at the latest version; order 3 has no amount.
    let join = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?total"],
        "where": {"@id": "?o", "ex:total": "?total", "ex:store": {"ex:storeName": "East shop"}},
    });
    let rows = fluree
        .graph(&alias)
        .query()
        .jsonld(&join)
        .execute_formatted()
        .await
        .expect("join");
    assert_eq!(sorted_ints(&rows), [600, 700], "got: {rows}");
    let sparql = fluree
        .query_from()
        .sparql(&format!(
            "PREFIX ex: <http://example.org/> SELECT ?total FROM <{alias}> WHERE {{ \
             ?o ex:total ?total ; ex:store ?s . ?s ex:storeName \"East shop\" }}"
        ))
        .execute_formatted()
        .await
        .expect("sparql join");
    let mut totals: Vec<String> = sparql["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["total"]["value"].as_str().expect("value").to_string())
        .collect();
    totals.sort();
    assert_eq!(totals, ["600", "700"], "got: {sparql}");

    // No `t`: a Delta source has no Fluree transaction to page against.
    let raw = fluree
        .graph(&alias)
        .query()
        .jsonld(&join)
        .execute()
        .await
        .expect("raw");
    assert_eq!(raw.t, None);
}

#[tokio::test]
async fn pinned_query_on_a_delta_source_reads_that_version() {
    let (fluree, alias) = source("delta-pinned").await;

    let amounts = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?amount"],
        "where": {"@id": "?s", "@type": "ex:Item", "ex:amount": "?amount"},
    });
    let count = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(count ?s)"],
        "where": {"@id": "?s", "@type": "ex:Item"},
    });
    let sparql_amounts = |from: &str| {
        format!(
            "PREFIX ex: <http://example.org/> SELECT ?amount FROM <{from}> \
             WHERE {{ ?s a ex:Item ; ex:amount ?amount }}"
        )
    };
    let sparql_values = |v: &Value| -> Vec<String> {
        let mut out: Vec<String> = v["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["amount"]["value"].as_str().expect("value").to_string())
            .collect();
        out.sort();
        out
    };

    let latest = fluree
        .graph(&alias)
        .query()
        .jsonld(&amounts)
        .execute_formatted()
        .await
        .expect("latest");
    assert_eq!(sorted_ints(&latest), [30000], "got: {latest}");

    // (suffix, spec, amounts at that version)
    let honored: Vec<(String, TimeSpec, Vec<i64>)> = vec![
        ("snapshot:0".into(), TimeSpec::AtSnapshot(0), vec![100]),
        (
            "snapshot:1".into(),
            TimeSpec::AtSnapshot(1),
            vec![100, 30000],
        ),
        ("snapshot:2".into(), TimeSpec::AtSnapshot(2), vec![30000]),
        (
            format!("time:{}", iso(ICT[0])),
            TimeSpec::AtTime(iso(ICT[0])),
            vec![100],
        ),
        (
            format!("time:{}", iso(ICT[2] - 1)),
            TimeSpec::AtTime(iso(ICT[2] - 1)),
            vec![100, 30000],
        ),
        (
            format!("iso:{}", iso(ICT[1])),
            TimeSpec::AtTime(iso(ICT[1])),
            vec![100, 30000],
        ),
        (
            format!("recorded:{}", iso(ICT[1])),
            TimeSpec::AtRecorded(iso(ICT[1])),
            vec![100, 30000],
        ),
        // After the latest commit: the latest state.
        (
            format!("time:{}", iso(ICT[2] + 86_400_000)),
            TimeSpec::AtTime(iso(ICT[2] + 86_400_000)),
            vec![30000],
        ),
    ];
    for (suffix, spec, expected) in &honored {
        let from = format!("{alias}@{suffix}");
        let mut q = amounts.clone();
        q["from"] = Value::String(from.clone());
        let rows = fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("from @{suffix}: {e}"));
        assert_eq!(&sorted_ints(&rows), expected, "from @{suffix}: {rows}");

        let mut q = count.clone();
        q["from"] = Value::String(from.clone());
        let n = fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("count from @{suffix}: {e}"));
        assert_eq!(
            sorted_ints(&n),
            [expected.len() as i64],
            "count from @{suffix}: {n}"
        );

        let rows = fluree
            .graph_at(&alias, spec.clone())
            .query()
            .jsonld(&amounts)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("graph_at({spec:?}): {e}"));
        assert_eq!(&sorted_ints(&rows), expected, "graph_at({spec:?}): {rows}");

        let rows = fluree
            .query_from()
            .sparql(&sparql_amounts(&from))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("SPARQL FROM <{from}>: {e}"));
        let want: Vec<String> = expected.iter().map(ToString::to_string).collect();
        assert_eq!(sparql_values(&rows), want, "SPARQL FROM <{from}>: {rows}");
    }

    let expect_refused = |result: Result<Value, ApiError>, route: &str, needle: &str| {
        let err = match result {
            Ok(rows) => panic!("{route}: pinned query returned {rows} instead of an error"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains(needle), "{route}: unexpected error: {err}");
    };
    let refused: Vec<(String, TimeSpec, &str)> = vec![
        (
            format!("time:{}", iso(ICT[0] - 1)),
            TimeSpec::AtTime(iso(ICT[0] - 1)),
            "no snapshot of table 'in_commit_time' at or before",
        ),
        (
            "snapshot:3".into(),
            TimeSpec::AtSnapshot(3),
            "snapshot 3 not found",
        ),
        (
            "t:1".into(),
            TimeSpec::AtT(1),
            "Graph sources have no transaction numbers or commit hashes",
        ),
    ];
    for (suffix, spec, needle) in &refused {
        let from = format!("{alias}@{suffix}");
        let mut q = amounts.clone();
        q["from"] = Value::String(from.clone());
        let result = fluree.query_from().jsonld(&q).execute_formatted().await;
        expect_refused(result, &format!("from @{suffix}"), needle);

        let mut q = count.clone();
        q["from"] = Value::String(from.clone());
        let result = fluree.query_from().jsonld(&q).execute_formatted().await;
        expect_refused(result, &format!("count from @{suffix}"), needle);

        let result = fluree
            .graph_at(&alias, spec.clone())
            .query()
            .jsonld(&amounts)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("graph_at({spec:?})"), needle);

        let result = fluree
            .query_from()
            .sparql(&sparql_amounts(&from))
            .execute_formatted()
            .await;
        expect_refused(result, &format!("SPARQL FROM <{from}>"), needle);
    }

    // The oldest retained commit is named, so the caller can correct the pin.
    let mut q = amounts.clone();
    q["from"] = Value::String(format!("{alias}@time:{}", iso(ICT[0] - 1)));
    let err = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect_err("before the first commit")
        .to_string();
    assert!(err.contains(&iso(ICT[0])), "{err}");

    // One source at two states in one query is refused.
    let two_states = json!({
        "@context": {"ex": "http://example.org/"},
        "from": [format!("{alias}@snapshot:0"), alias],
        "select": ["?amount"],
        "where": {"@id": "?s", "ex:amount": "?amount"},
    });
    let result = fluree
        .query_from()
        .jsonld(&two_states)
        .execute_formatted()
        .await;
    expect_refused(result, "pinned + latest", "two different states");
}

/// The mapping is always the current one, so a pin to a version whose schema
/// lacks a mapped column is an error naming the column, and a version that has
/// the name back under a new identity reads the new column's values.
#[tokio::test]
async fn a_pinned_version_resolves_columns_against_its_own_schema() {
    let (fluree, alias) = source("delta-mapped").await;
    let q = |from: String| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?amount"],
            "where": {"@id": "?s", "ex:mappedAmount": "?amount"},
        })
    };
    let v0 = fluree
        .query_from()
        .jsonld(&q(format!("{alias}@snapshot:0")))
        .execute_formatted()
        .await
        .expect("v0 has `amount`");
    assert_eq!(sorted_ints(&v0), [100, 30000], "got: {v0}");

    // v1 renamed it away.
    let err = fluree
        .query_from()
        .jsonld(&q(format!("{alias}@snapshot:1")))
        .execute_formatted()
        .await
        .expect_err("v1 has no `amount`")
        .to_string();
    assert!(err.contains("no column 'amount' at version 1"), "{err}");

    // v3 re-added `amount` as a new, all-null column over the same Parquet file.
    let v3 = fluree
        .query_from()
        .jsonld(&q(alias.clone()))
        .execute_formatted()
        .await
        .expect("latest");
    assert_eq!(sorted_ints(&v3), Vec::<i64>::new(), "got: {v3}");
}

/// Materialization reads Iceberg manifests; a Delta source is refused by name
/// rather than failing to parse as an Iceberg config.
#[tokio::test]
async fn materializing_a_delta_source_is_refused() {
    let (fluree, alias) = source("delta-materialize").await;
    let err = fluree
        .materialize_r2rml_graph_source(&alias, "delta-twin:main", true)
        .await
        .expect_err("Delta materialization is not available")
        .to_string();
    assert!(
        err.contains("materialization is not yet available for Delta graph sources"),
        "{err}"
    );
}

/// Registration checks each table for the columns its maps project: a column
/// the table lacks is reported then, and the source still registers.
#[tokio::test]
async fn registration_reports_a_mapped_column_the_table_lacks() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        <http://example.org/mapping#Store>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "dim_store" ] ;
            rr:subjectMap [ rr:template "http://example.org/store/{store_id}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .
        <http://example.org/mapping#Absent>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "not_there" ] ;
            rr:subjectMap [ rr:template "http://example.org/absent/{id}" ] .
    "#;
    let mut config = DeltaCreateConfig::new("delta-warn", root.to_str().unwrap(), mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    let created = fluree
        .create_delta_graph_source(config)
        .await
        .expect("registers despite warnings");
    assert!(created.table_versions.is_empty(), "{created:?}");
    let warnings = created.table_warnings.join("\n");
    assert!(warnings.contains("no column 'city'"), "{warnings}");
    assert!(warnings.contains("table 'not_there'"), "{warnings}");

    // A name that cannot be placed at all is an error, not a warning.
    let mut unplaced = DeltaCreateConfig::new("delta-unplaced", root.to_str().unwrap(), mapping);
    unplaced.mapping_media_type = Some("text/turtle".to_string());
    unplaced.root = None;
    unplaced.tables.insert(
        "dim_store".to_string(),
        format!("{}/dim_store", root.display()),
    );
    let err = fluree
        .create_delta_graph_source(unplaced)
        .await
        .expect_err("`not_there` has no location")
        .to_string();
    assert!(err.contains("'not_there'"), "{err}");
}

/// A literal service-principal secret is stored with the source; the info view
/// every client can read must not return it.
#[tokio::test]
async fn graph_source_info_redacts_a_stored_azure_secret() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mut config = DeltaCreateConfig::new("delta-secret", root.to_str().unwrap(), MAPPING);
    config.mapping_media_type = Some("text/turtle".to_string());
    config.io.azure = Some(fluree_db_api::DeltaAzureAuth::ClientSecret {
        tenant_id: "tenant-visible".to_string(),
        client_id: "client-visible".to_string(),
        client_secret: fluree_db_api::DeltaConfigValue::literal("hunter2-do-not-leak"),
    });
    fluree
        .create_delta_graph_source(config)
        .await
        .expect("create");
    let record = fluree
        .nameservice()
        .lookup_graph_source("delta-secret:main")
        .await
        .expect("lookup")
        .expect("record");
    assert!(
        record.config.contains("hunter2-do-not-leak"),
        "precondition: the literal is what gets stored"
    );
    let info = fluree_db_api::ledger_info::build_graph_source_info(&fluree, &record)
        .await
        .expect("info")
        .to_string();
    assert!(!info.contains("hunter2-do-not-leak"), "{info}");
    assert!(info.contains("client-visible"), "{info}");
}

/// View policy on a Delta source: request policies through the shared R2RML
/// gate, and a registered model ledger supplying rules and default-allow —
/// the record's `model` / `default_allow` are read by Delta-specific code.
#[tokio::test]
async fn view_policies_govern_a_delta_source() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let context = json!({ "ex": "http://example.org/", "f": "https://ns.flur.ee/db#" });

    // `dim_store` has three stores; `fact_order` six orders with five totals.
    let names = |from: &str, opts: Value| {
        let mut q = json!({
            "@context": context.clone(),
            "from": from,
            "select": ["?name"],
            "where": { "@id": "?s", "ex:storeName": "?name" },
        });
        if !opts.is_null() {
            q["opts"] = opts;
        }
        q
    };
    let totals = |from: &str, opts: Value| {
        json!({
            "@context": context.clone(),
            "from": from,
            "opts": opts,
            "select": ["?total"],
            "where": { "@id": "?o", "ex:total": "?total" },
        })
    };
    let count = |fluree: &Fluree, q: Value| {
        let fluree = fluree.clone();
        async move {
            fluree
                .query_from()
                .jsonld(&q)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{q}: {e}"))
                .as_array()
                .map_or(0, Vec::len)
        }
    };

    // --- Request policies on an ungoverned source.
    let mut open = DeltaCreateConfig::new("delta-open", root.to_str().unwrap(), MAPPING);
    open.mapping_media_type = Some("text/turtle".to_string());
    fluree
        .create_delta_graph_source(open)
        .await
        .expect("open source");
    let open = "delta-open:main";
    let hide_names = json!([{
        "@id": "ex:hideNames", "@type": "f:AccessPolicy", "f:action": "f:view",
        "f:onProperty": [{ "@id": "http://example.org/storeName" }], "f:allow": false
    }]);
    let deny_names = json!({ "policy": hide_names, "default-allow": true });
    assert_eq!(count(&fluree, names(open, Value::Null)).await, 3);
    assert_eq!(count(&fluree, names(open, deny_names.clone())).await, 0);
    assert_eq!(
        count(&fluree, totals(open, deny_names)).await,
        5,
        "the deny targets one property only"
    );
    assert_eq!(
        count(
            &fluree,
            names(open, json!({ "policy": [], "default-allow": false }))
        )
        .await,
        0,
        "no matching rule and default-allow false"
    );

    // --- A model ledger: baseline rule hides totals, a reader class grants them.
    const MODEL: &str = "delta-governance:main";
    let ledger = fluree.create_ledger(MODEL).await.expect("model ledger");
    fluree
        .insert(
            ledger,
            &json!({
                "@context": context.clone(),
                "@graph": [
                    { "@id": "ex:hideTotals", "@type": "f:AccessPolicy", "f:action": { "@id": "f:view" },
                      "f:onProperty": [{ "@id": "ex:total" }], "f:allow": false },
                    { "@id": "ex:readTotals", "@type": "ex:ReaderPolicy", "f:action": { "@id": "f:view" },
                      "f:onProperty": [{ "@id": "ex:total" }], "f:allow": true },
                    { "@id": "http://example.org/users/reader", "f:policyClass": { "@id": "ex:ReaderPolicy" } }
                ]
            }),
        )
        .await
        .expect("seed model ledger");
    let mut governed = DeltaCreateConfig::new("delta-governed", root.to_str().unwrap(), MAPPING);
    governed.mapping_media_type = Some("text/turtle".to_string());
    governed.model = Some(MODEL.to_string());
    governed.default_allow = Some(true);
    fluree
        .create_delta_graph_source(governed)
        .await
        .expect("governed source");
    let governed = "delta-governed:main";

    // No request options at all: the registered model and default-allow apply.
    assert_eq!(
        count(&fluree, names(governed, Value::Null)).await,
        3,
        "registered default-allow keeps unrestricted properties readable"
    );
    let mut bare_totals = totals(governed, Value::Null);
    bare_totals.as_object_mut().unwrap().remove("opts");
    assert_eq!(
        count(&fluree, bare_totals).await,
        0,
        "the model's baseline rule hides totals"
    );
    let reader = json!({ "identity": "http://example.org/users/reader", "default-allow": false });
    assert_eq!(count(&fluree, totals(governed, reader)).await, 5);
}
