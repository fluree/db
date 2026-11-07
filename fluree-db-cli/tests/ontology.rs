//! `fluree ontology check` and `fluree ontology from`: files in, a report or
//! an ontology out, no project and nothing written beside them.

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

fn fluree_cmd(work_dir: &TempDir) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(work_dir.path());
    cmd.env("HOME", work_dir.path());
    cmd.env("NO_COLOR", "1");
    cmd
}

/// An OWL/SKOS ontology as published vocabularies write one: SKOS text, an
/// OWL restriction, a second parent, a domain and a range.
const OWL_SKOS: &str = r#"
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <https://example.com/fund/> .
ex:Custodian a owl:Class ;
  skos:prefLabel "custodian"@en ;
  skos:definition "A bank that holds a fund's assets." ;
  rdfs:subClassOf ex:ServiceProvider, ex:Bank ;
  rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:holds ; owl:someValuesFrom ex:Asset ] .
ex:ServiceProvider a rdfs:Class ; rdfs:label "service provider" .
ex:Bank a owl:Class ; skos:prefLabel "bank" .
ex:Asset a owl:Class .
ex:employer_id a rdf:Property ; rdfs:label "employer" ; rdfs:domain ex:Person ; rdfs:range ex:Bank .
ex:Person a rdfs:Class ; rdfs:label "person" .
"#;

fn write(tmp: &TempDir, name: &str, text: &str) {
    std::fs::write(tmp.path().join(name), text).unwrap();
}

fn check_json(tmp: &TempDir, args: &[&str]) -> (serde_json::Value, i32) {
    let out = fluree_cmd(tmp)
        .args(["ontology", "check", "--json"])
        .args(args)
        .output()
        .unwrap();
    let report = serde_json::from_slice(&out.stdout).expect("stdout is the JSON report");
    (report, out.status.code().unwrap_or(-1))
}

#[test]
fn check_reports_what_is_adapted_and_not_shown() {
    let tmp = TempDir::new().unwrap();
    write(&tmp, "fund.ttl", OWL_SKOS);
    let (report, code) = check_json(&tmp, &["fund.ttl"]);
    assert_eq!(code, 0, "{report:#}");
    assert_eq!(report["policy"], "ontology-adapter-v1");
    assert_eq!(report["classes"], 5);
    assert_eq!(report["properties"], 1);
    // the counts an extraction response would report, key for key
    assert_eq!(
        report["adapted"],
        serde_json::json!({
            "blank-node triples dropped": 4,
            "rdfs:comment from skos:definition": 1,
            "rdfs:label from skos:prefLabel": 2,
            "rdfs:label from the IRI": 1,
        })
    );
    assert_eq!(
        report["not_shown"],
        serde_json::json!({
            "owl:onProperty": 1,
            "owl:someValuesFrom": 1,
            "rdf:type owl:Restriction": 1,
            "rdfs:domain": 1,
            "rdfs:range": 1,
            "rdfs:subClassOf": 1,
            "rdfs:subClassOf (a second parent)": 1,
        })
    );
    let advice: Vec<&str> = report["advice"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["code"].as_str().unwrap())
        .collect();
    assert!(advice.contains(&"local-name-not-words"), "{advice:?}");
    assert!(advice.contains(&"domain-range-not-shown"), "{advice:?}");
    assert!(report["will_fail"].as_array().unwrap().is_empty());
    assert!(
        !tmp.path().join(".fluree").exists(),
        "check wrote a project"
    );
}

#[test]
fn check_exits_1_when_a_request_would_fail() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "bad.ttl",
        r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
           @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
           <https://example.com/123> a rdfs:Class .
           <https://example.com/acme> a <https://example.com/123> ; rdfs:comment "no SKOS label" ."#,
    );
    let (report, code) = check_json(&tmp, &["bad.ttl"]);
    assert_eq!(code, 1);
    let codes: Vec<&str> = report["will_fail"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["code"].as_str().unwrap())
        .collect();
    assert_eq!(codes, ["no-label", "catalog-no-label"]);

    fluree_cmd(&tmp)
        .args(["ontology", "check", "bad.ttl"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("Will fail"))
        .stdout(predicate::str::contains(
            "2 problems would make a request fail.",
        ));
}

#[test]
fn check_takes_a_separate_catalog() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "org.ttl",
        r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
           <https://example.com/Org> a rdfs:Class ; rdfs:label "organization" ;
             rdfs:comment "A company." ."#,
    );
    write(
        &tmp,
        "accounts.ttl",
        r#"@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
           <https://example.com/acme> a <https://example.com/Org> ; skos:prefLabel "Acme" .
           <https://example.com/zed> a <https://example.com/Org> ; skos:prefLabel "Zed" ."#,
    );
    let (report, code) = check_json(&tmp, &["org.ttl", "--catalog", "accounts.ttl"]);
    assert_eq!(code, 0, "{report:#}");
    assert_eq!(report["records"], 2);
    let e = &report["estimate"];
    assert!(e["catalog_tokens"].as_u64().unwrap() > 0);
    assert_eq!(e["limit"], 200_000);
}

#[test]
fn from_rewrites_and_check_then_finds_nothing_to_adapt() {
    let tmp = TempDir::new().unwrap();
    write(&tmp, "fund.ttl", OWL_SKOS);
    fluree_cmd(&tmp)
        .args(["ontology", "from", "fund.ttl", "-o", "out.ttl"])
        .assert()
        .success()
        .stderr(predicate::str::contains("5 classes, 1 property"));
    let out = std::fs::read_to_string(tmp.path().join("out.ttl")).unwrap();
    assert!(out.contains("rdfs:label \"custodian\"@en"), "{out}");
    assert!(!out.contains("Restriction"), "{out}");
    let (report, code) = check_json(&tmp, &["out.ttl"]);
    assert_eq!(code, 0);
    assert_eq!(report["adapted"], serde_json::json!({}));

    // JSON-LD out reads back the same
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "fund.ttl",
            "--format",
            "jsonld",
            "-o",
            "out.jsonld",
        ])
        .assert()
        .success();
    let (again, _) = check_json(&tmp, &["out.jsonld"]);
    assert_eq!(again["classes"], 5);
    assert_eq!(again["adapted"], serde_json::json!({}));
}

#[test]
fn seed_slices_a_vocabulary() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "vocab.ttl",
        r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
           @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
           @prefix ex: <https://example.com/> .
           ex:Thing a rdfs:Class ; rdfs:label "thing" .
           ex:Person a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "person" .
           ex:Organization a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "organization" .
           ex:Vehicle a rdfs:Class ; rdfs:subClassOf ex:Thing ; rdfs:label "vehicle" .
           ex:worksFor a rdf:Property ; rdfs:label "works for" ;
             rdfs:domain ex:Person ; rdfs:range ex:Organization .
           ex:wheels a rdf:Property ; rdfs:label "wheels" ; rdfs:domain ex:Vehicle ."#,
    );
    let (report, code) = check_json(&tmp, &["vocab.ttl", "--seed", "person"]);
    assert_eq!(code, 0, "{report:#}");
    assert_eq!(report["classes"], 3);
    assert_eq!(report["properties"], 1);
    assert_eq!(report["slice"]["classes_total"], 4);

    // a label, a prefixed name or an IRI that names no class is refused, never
    // an empty slice
    for seed in ["Spaceship", "ex:Persn", "<https://example.com/Persn>"] {
        fluree_cmd(&tmp)
            .args(["ontology", "check", "vocab.ttl", "--seed", seed])
            .assert()
            .code(2)
            .stderr(predicate::str::contains(
                "no class in the ontology matches --seed",
            ));
        fluree_cmd(&tmp)
            .args(["ontology", "from", "vocab.ttl", "--seed", seed])
            .assert()
            .code(2);
    }
}

#[test]
fn trig_and_unknown_formats_are_refused_plainly() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "request.trig",
        "<urn:g> { <urn:a> <urn:b> <urn:c> . }",
    );
    write(&tmp, "model.toml", "[tables]");
    fluree_cmd(&tmp)
        .args(["ontology", "check", "request.trig"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("TriG isn't read yet"));
    fluree_cmd(&tmp)
        .args(["ontology", "from", "model.toml"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not an RDF file"));
}

const DDL: &str = r"
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL
);
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY,
    employer_id INTEGER REFERENCES companies(company_id),
    full_name TEXT,
    status VARCHAR(10) CHECK (status IN ('active', 'left'))
);
COMMENT ON TABLE companies IS 'Customer accounts and partners.';
";

#[test]
fn from_sql_ddl() {
    let tmp = TempDir::new().unwrap();
    write(&tmp, "crm.sql", DDL);
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "crm.sql",
            "--base",
            "https://example.com/crm/",
            "-o",
            "crm.ttl",
            "--catalog-out",
            "status.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "3 classes, 4 properties; 1 enumeration made a class, with 2 catalog records; 2 primary key columns left out",
        ));
    let ttl = std::fs::read_to_string(tmp.path().join("crm.ttl")).unwrap();
    // a foreign key is a relation labelled without `_id`
    assert!(ttl.contains("crm:employer a rdf:Property"), "{ttl}");
    assert!(ttl.contains("rdfs:range crm:Company"), "{ttl}");
    assert!(
        ttl.contains("rdfs:comment \"Customer accounts and partners.\""),
        "{ttl}"
    );
    assert!(
        !ttl.contains("companyId"),
        "primary keys are left out: {ttl}"
    );
    let records = std::fs::read_to_string(tmp.path().join("status.ttl")).unwrap();
    assert!(records.contains("skos:prefLabel \"active\""), "{records}");

    let (report, code) = check_json(&tmp, &["crm.ttl", "--catalog", "status.ttl"]);
    assert_eq!(code, 0, "{report:#}");
    assert_eq!(report["records"], 2);
    assert_eq!(report["adapted"], serde_json::json!({}));
}

/// `fixtures/ontology/crm.xlsx`: two sheets, Companies (company_id, name,
/// industry) and People (id, full_name, company_id, role), the role
/// repeating two values over eight rows.
fn workbook(tmp: &TempDir) {
    let src =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ontology/crm.xlsx");
    std::fs::copy(src, tmp.path().join("crm.xlsx")).unwrap();
}

#[test]
fn from_a_workbook_links_sheets() {
    let tmp = TempDir::new().unwrap();
    workbook(&tmp);
    let out = fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "crm.xlsx",
            "--base",
            "https://example.com/crm/",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let ttl = String::from_utf8(out).unwrap();
    assert!(ttl.contains("crm:Person a rdfs:Class"), "{ttl}");
    assert!(
        ttl.contains("crm:Role a rdfs:Class"),
        "role is enumerated: {ttl}"
    );
    assert!(
        ttl.contains("rdfs:range crm:Company"),
        "company_id links People to Companies: {ttl}"
    );
}

#[test]
fn catalog_from_rows() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "accounts.csv",
        "account_no,name,aliases,industry\n\
         A-100,Daxin Scientific Supply Co.,Daxin; sales@daxin.example,Lab supplies\n\
         A-101,Dexin Instruments Ltd.,Dexin,Instruments\n",
    );
    write(
        &tmp,
        "ontology.ttl",
        r#"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
           <https://example.com/crm/Organization> a rdfs:Class ; rdfs:label "organization" ;
             rdfs:comment "A company we deal with." ."#,
    );
    fluree_cmd(&tmp)
        .args([
            "catalog",
            "from",
            "accounts.csv",
            "--class",
            "organization",
            "--ontology",
            "ontology.ttl",
            "--label",
            "name",
            "--alt",
            "aliases",
            "--code",
            "account_no",
            "--describe",
            "industry",
            "--id",
            "account_no",
            "--base",
            "https://example.com/crm/account/",
            "-o",
            "accounts.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "2 records of <https://example.com/crm/Organization>",
        ))
        .stderr(predicate::str::contains(
            "serialized tokens, repeated in every extraction window",
        ));
    let ttl = std::fs::read_to_string(tmp.path().join("accounts.ttl")).unwrap();
    assert!(ttl.contains("skos:hiddenLabel \"A-100\""), "{ttl}");
    assert!(ttl.contains("\"sales@daxin.example\""), "{ttl}");
    assert!(
        ttl.contains("skos:definition \"industry: Lab supplies\""),
        "{ttl}"
    );

    let (report, code) = check_json(&tmp, &["ontology.ttl", "--catalog", "accounts.ttl"]);
    assert_eq!(code, 0, "{report:#}");
    assert_eq!(report["records"], 2);
    assert!(
        report["advice"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["code"] != "catalog-class-not-in-ontology"),
        "the records are typed with the ontology's class: {report:#}"
    );

    fluree_cmd(&tmp)
        .args([
            "catalog",
            "from",
            "accounts.csv",
            "--class",
            "Organization",
            "--label",
            "title",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("no column \"title\""));
}

/// A Snowflake semantic view as a data team writes one for Cortex Analyst.
const SEMANTIC_VIEW: &str = r"
name: bike_shop
description: Sales for a chain of bike shops.
tables:
  - name: CUSTOMERS
    synonyms: [riders, clients]
    description: People and clubs who buy from the shops.
    primary_key: {columns: [CUSTOMER_ID]}
    dimensions:
      - name: CUSTOMER_ID
        data_type: TEXT
      - name: FULL_NAME
        description: The customer's name.
        data_type: VARCHAR(200)
      - name: SEGMENT
        description: Kind of customer.
        data_type: TEXT
        sample_values: [Commuter, Racer, Club]
        is_enum: true
  - name: ORDERS
    description: Sales, one row per order.
    primary_key: {columns: [ORDER_ID]}
    dimensions:
      - name: ORDER_ID
        data_type: NUMBER(38,0)
      - name: CUSTOMER_ID
        data_type: TEXT
    facts:
      - name: TOTAL
        description: Order total in pounds.
        data_type: NUMBER(10,2)
relationships:
  - name: orders_to_customers
    left_table: ORDERS
    right_table: CUSTOMERS
    relationship_columns: [{left_column: CUSTOMER_ID, right_column: CUSTOMER_ID}]
metrics:
  - name: REVENUE
    expr: SUM(ORDERS.TOTAL)
verified_queries:
  - name: top riders
    sql: SELECT 1
";

#[test]
fn from_a_snowflake_semantic_view() {
    let tmp = TempDir::new().unwrap();
    write(&tmp, "shop.yaml", SEMANTIC_VIEW);
    fluree_cmd(&tmp)
        .args(["ontology", "from", "shop.yaml", "--base", "https://example.com/shop/", "-o", "shop.ttl"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "3 classes, 4 properties; 1 enumeration made a class, with 3 catalog records; 2 primary key columns left out",
        ));
    let ttl = std::fs::read_to_string(tmp.path().join("shop.ttl")).unwrap();
    assert!(
        ttl.contains("rdfs:comment \"People and clubs who buy from the shops. Also called: riders, clients.\""),
        "{ttl}"
    );
    // the relationship is a relation labelled for its column
    assert!(ttl.contains("shop:customer a rdf:Property"), "{ttl}");
    assert!(ttl.contains("rdfs:range shop:Customer"), "{ttl}");
    assert!(
        !ttl.contains("REVENUE") && !ttl.contains("revenue"),
        "metrics are left out: {ttl}"
    );
    let (report, code) = check_json(&tmp, &["shop.ttl"]);
    assert_eq!(code, 0, "{report:#}");
}

#[test]
fn from_a_genie_export_with_comments() {
    let tmp = TempDir::new().unwrap();
    let space = serde_json::json!({
        "data_sources": {"tables": [
            {"identifier": "main.shop.customers",
             "column_configs": [{"column_name": "id"}, {"column_name": "full_name"}]},
            {"identifier": "main.shop.orders",
             "column_configs": [{"column_name": "order_id"}, {"column_name": "customer_ref"}]}
        ]},
        "instructions": {"join_specs": [{
            "left": {"identifier": "main.shop.orders", "alias": "o"},
            "right": {"identifier": "main.shop.customers", "alias": "c"},
            "sql": ["`o`.`customer_ref` = `c`.`id`"]
        }]}
    });
    write(
        &tmp,
        "space.json",
        &serde_json::json!({"serialized_space": space.to_string()}).to_string(),
    );
    write(
        &tmp,
        "comments.json",
        r#"{"customers": {"": "People who buy from the shops.", "full_name": "Their name."}}"#,
    );
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "space.json",
            "--comments",
            "comments.json",
            "--base",
            "https://example.com/shop/",
            "-o",
            "shop.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("2 classes, 2 properties"));
    let ttl = std::fs::read_to_string(tmp.path().join("shop.ttl")).unwrap();
    assert!(
        ttl.contains("rdfs:comment \"People who buy from the shops.\""),
        "{ttl}"
    );
    assert!(
        ttl.contains("shop:customerRef a rdf:Property")
            || ttl.contains("shop:customer a rdf:Property"),
        "{ttl}"
    );
    assert!(ttl.contains("rdfs:range shop:Customer"), "{ttl}");
}

#[test]
fn from_a_dbt_project_directory() {
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp.path().join("shop/models/marts")).unwrap();
    std::fs::create_dir_all(tmp.path().join("shop/target")).unwrap();
    write(
        &tmp,
        "shop/models/marts/schema.yml",
        "models:\n  - name: stg_orders\n    description: Orders as the shop system records them.\n    columns:\n      - name: status\n        description: Where the order is.\n        data_tests:\n          - accepted_values:\n              arguments:\n                values: [placed, shipped, returned]\n",
    );
    write(
        &tmp,
        "shop/models/marts/semantic.yml",
        "semantic_models:\n  - name: orders\n    model: ref('stg_orders')\n    entities:\n      - {name: order, type: primary, expr: order_id}\n      - {name: customer, type: foreign, expr: customer_id}\n    dimensions:\n      - {name: status, type: categorical}\n      - name: ordered_at\n        type: time\n        type_params: {time_granularity: day}\n    measures:\n      - {name: order_total, agg: sum, description: The order's total in pounds.}\n      - {name: order_count, agg: count, expr: 1}\n  - name: customers\n    description: People who buy from the shops.\n    entities:\n      - {name: customer, type: primary, expr: customer_id}\n",
    );
    // build output is passed over
    write(
        &tmp,
        "shop/target/semantic_manifest.yml",
        "semantic_models: [{name: broken}]\n",
    );
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "shop",
            "--base",
            "https://example.com/shop/",
            "-o",
            "shop.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "3 classes, 4 properties; 1 enumeration made a class, with 3 catalog records",
        ));
    let ttl = std::fs::read_to_string(tmp.path().join("shop.ttl")).unwrap();
    assert!(
        ttl.contains("rdfs:comment \"Orders as the shop system records them.\""),
        "{ttl}"
    );
    assert!(ttl.contains("shop:customer a rdf:Property"), "{ttl}");
    assert!(ttl.contains("rdfs:range shop:Customer"), "{ttl}");
    assert!(
        ttl.contains("rdfs:comment \"Where the order is.\""),
        "{ttl}"
    );
    assert!(!ttl.contains("orderCount"), "counts are left out: {ttl}");
    assert!(!ttl.contains("broken"), "target/ is passed over: {ttl}");
}

#[test]
fn from_an_ossie_ontology_keeps_its_iris() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "flights.ossie.yaml",
        r"version: 0.2.0.dev0
name: Flights
prefixes:
  schema: https://schema.org/
ontology:
- concept: Airport
  type: EntityType
  iri: schema:Airport
  description: An airport identified by its IATA code.
- concept: Flight
  type: EntityType
  relationships:
  - name: departs_from
    roles:
    - concept: Airport
    verbalizes: [ '{Flight} departs from {Airport}' ]
    multiplicity: ManyToOne
  - name: diverted
    verbalizes: [ '{Flight} was diverted' ]
",
    );
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "flights.ossie.yaml",
            "--base",
            "https://example.com/flights/",
            "-o",
            "flights.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("2 classes, 2 properties"));
    let ttl = std::fs::read_to_string(tmp.path().join("flights.ttl")).unwrap();
    assert!(
        ttl.contains("<https://schema.org/Airport> a rdfs:Class"),
        "{ttl}"
    );
    assert!(
        ttl.contains("rdfs:comment \"A flight departs from an airport.\""),
        "{ttl}"
    );
    assert!(
        ttl.contains("rdfs:range xsd:boolean"),
        "a unary relationship is a boolean: {ttl}"
    );
    let (report, code) = check_json(&tmp, &["flights.ttl"]);
    assert_eq!(code, 0, "{report:#}");
}

#[test]
fn from_an_ossie_semantic_model_in_json() {
    let tmp = TempDir::new().unwrap();
    let doc = serde_json::json!({
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {"name": "customers", "source": "sales.public.customers", "primary_key": ["id"],
             "description": "People who buy.", "ai_context": {"synonyms": ["buyers"]},
             "fields": [{"name": "id", "expression": {"dialects": []}},
                        {"name": "signup_date", "datatype": "Date", "expression": {"dialects": []}}]},
            {"name": "orders", "source": "sales.public.orders",
             "fields": [{"name": "customer_id", "expression": {"dialects": []}}]}
        ],
        "relationships": [{"name": "orders_to_customers", "from": "orders", "to": "customers",
                           "from_columns": ["customer_id"], "to_columns": ["id"]}]
    });
    write(&tmp, "sales.json", &doc.to_string());
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "sales.json",
            "--base",
            "https://example.com/sales/",
            "-o",
            "sales.ttl",
        ])
        .assert()
        .success();
    let ttl = std::fs::read_to_string(tmp.path().join("sales.ttl")).unwrap();
    assert!(
        ttl.contains("rdfs:comment \"People who buy. Also called: buyers.\""),
        "{ttl}"
    );
    assert!(ttl.contains("rdfs:range sales:Customer"), "{ttl}");
}

#[test]
fn a_dbt_project_resolves_doc_blocks_and_names_files_it_cannot_read() {
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp.path().join("proj/models")).unwrap();
    write(
        &tmp,
        "proj/models/docs.md",
        "{% docs orders_desc %}\nOne row per order.\n{% enddocs %}\n",
    );
    write(
        &tmp,
        "proj/models/schema.yml",
        "models:\n  - name: orders\n    description: \"{{ doc('orders_desc') }}\"\n    columns:\n      - {name: id, data_tests: [unique, not_null]}\n      - {name: total}\n",
    );
    write(&tmp, "proj/models/broken.yml", "models: [unclosed\n");
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "proj",
            "--base",
            "https://example.com/p/",
            "-o",
            "p.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "broken.yml: skipped, not valid YAML",
        ));
    let ttl = std::fs::read_to_string(tmp.path().join("p.ttl")).unwrap();
    assert!(ttl.contains("rdfs:comment \"One row per order.\""), "{ttl}");
    assert!(!ttl.contains("{{"), "{ttl}");
}

/// `CALL apoc.meta.schema() YIELD value RETURN value`, exported as JSON, for a
/// small movies graph.
const NEO4J_SCHEMA: &str = r#"[{"value": {
  "Person": {"type": "node", "count": 133, "labels": [], "properties": {
      "name": {"type": "STRING", "indexed": true, "unique": false, "existence": false},
      "born": {"type": "INTEGER", "indexed": false, "unique": false, "existence": false}},
    "relationships": {
      "ACTED_IN": {"direction": "out", "count": 172, "labels": ["Movie"],
                   "properties": {"roles": {"type": "LIST", "array": true}}},
      "DIRECTED": {"direction": "out", "count": 44, "labels": ["Movie"], "properties": {}},
      "REVIEWED": {"direction": "out", "count": 9, "labels": ["Movie"],
                   "properties": {"summary": {"type": "STRING"}, "rating": {"type": "INTEGER"}}},
      "FOLLOWS": {"direction": "out", "count": 3, "labels": ["Person"], "properties": {}}}},
  "Movie": {"type": "node", "count": 38, "labels": [], "properties": {
      "title": {"type": "STRING", "indexed": true, "unique": true, "existence": false},
      "released": {"type": "INTEGER", "indexed": false, "unique": false, "existence": false},
      "tagline": {"type": "STRING", "indexed": false, "unique": false, "existence": false}},
    "relationships": {
      "ACTED_IN": {"direction": "in", "count": 172, "labels": ["Person"], "properties": {}}}},
  "ACTED_IN": {"type": "relationship", "count": 172, "properties": {"roles": {"type": "LIST", "array": true}}}
}}]"#;

#[test]
fn from_a_neo4j_schema() {
    let tmp = TempDir::new().unwrap();
    write(&tmp, "movies-schema.json", NEO4J_SCHEMA);
    fluree_cmd(&tmp)
        .args([
            "ontology",
            "from",
            "movies-schema.json",
            "--base",
            "https://example.com/movies/",
            "-o",
            "movies.ttl",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("2 classes, 9 properties"))
        .stderr(predicate::str::contains(
            "3 relationship propert(ies) left out",
        ));
    let ttl = std::fs::read_to_string(tmp.path().join("movies.ttl")).unwrap();
    assert!(ttl.contains("movies:actedIn a rdf:Property"), "{ttl}");
    assert!(
        ttl.contains("rdfs:comment \"A person acted in a movie.\""),
        "{ttl}"
    );
    assert!(ttl.contains("rdfs:label \"acted in\""), "{ttl}");
    assert!(ttl.contains("rdfs:range movies:Movie"), "{ttl}");
    let (report, code) = check_json(&tmp, &["movies.ttl"]);
    assert_eq!(code, 0, "{report:#}");
}

#[test]
fn json_ld_is_never_read_as_another_source() {
    let tmp = TempDir::new().unwrap();
    // JSON-LD whose keys a converter might mistake for its own
    write(
        &tmp,
        "data.json",
        r#"{"@context": {"ex": "https://example.com/"}, "nodes": [], "relationships": [],
            "datasets": [], "@graph": [{"@id": "ex:acme", "ex:name": "Acme"}]}"#,
    );
    fluree_cmd(&tmp)
        .args(["ontology", "from", "data.json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no classes found"));
}
