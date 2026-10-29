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

    fluree_cmd(&tmp)
        .args(["ontology", "check", "vocab.ttl", "--seed", "Spaceship"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "no class in the ontology matches --seed",
        ));
}

#[test]
fn trig_and_unknown_formats_are_refused_plainly() {
    let tmp = TempDir::new().unwrap();
    write(
        &tmp,
        "request.trig",
        "<urn:g> { <urn:a> <urn:b> <urn:c> . }",
    );
    write(&tmp, "schema.sql", "CREATE TABLE t (id int);");
    fluree_cmd(&tmp)
        .args(["ontology", "check", "request.trig"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("TriG isn't read yet"));
    fluree_cmd(&tmp)
        .args(["ontology", "from", "schema.sql"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not an RDF file"));
}
