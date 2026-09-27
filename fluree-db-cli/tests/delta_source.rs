//! `fluree delta map`, then `fluree query --at` on the mapped source, through
//! the binary. Fixtures are the ones committed in `fluree-db-delta`;
//! `in_commit_time` holds v0 = {100}, v1 = {100, 30000}, v2 = {30000}.

#![cfg(feature = "delta")]

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

const MAPPING: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.org/mapping#Item> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "in_commit_time" ] ;
  rr:subjectMap [ rr:template "http://example.org/item/{id}" ] ;
  rr:predicateObjectMap [
    rr:predicate ex:amount ;
    rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ]
  ] .
"#;

const AMOUNTS: &str = "SELECT ?a WHERE { ?s <http://example.org/amount> ?a }";

fn fixtures() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fluree-db-delta/tests/fixtures")
        .canonicalize()
        .expect("fixtures dir")
        .display()
        .to_string()
}

fn fluree_cmd(work_dir: &TempDir) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(work_dir.path());
    cmd.env("HOME", work_dir.path());
    cmd.env("NO_COLOR", "1");
    cmd.env("FLUREE_ICEBERG_LOCAL_ROOTS", fixtures());
    cmd
}

fn mapped_source() -> TempDir {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    std::fs::write(tmp.path().join("items.ttl"), MAPPING).unwrap();
    fluree_cmd(&tmp)
        .args([
            "delta",
            "map",
            "items",
            "--root",
            &fixtures(),
            "--r2rml",
            "items.ttl",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("graph source 'items:main'"))
        .stdout(predicate::str::contains("in_commit_time (version 2)"));
    tmp
}

#[test]
fn delta_source_is_listed_with_its_type() {
    let tmp = mapped_source();
    fluree_cmd(&tmp)
        .args(["delta", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("items").and(predicate::str::contains("Delta")));
}

#[test]
fn at_pins_a_graph_source_query_to_a_table_version() {
    let tmp = mapped_source();
    let query = |at: Option<&str>| {
        let mut cmd = fluree_cmd(&tmp);
        cmd.args(["query", "items", "--format", "json"]);
        if let Some(at) = at {
            cmd.args(["--at", at]);
        }
        cmd.args(["--sparql", AMOUNTS]);
        cmd.assert()
    };

    query(None)
        .success()
        .stdout(predicate::str::contains("30000").and(predicate::str::contains("\"100\"").not()));
    query(Some("snapshot:0"))
        .success()
        .stdout(predicate::str::contains("\"100\"").and(predicate::str::contains("30000").not()));
    query(Some("snapshot:1"))
        .success()
        .stdout(predicate::str::contains("\"100\"").and(predicate::str::contains("30000")));
    query(Some("time:2026-09-19T14:46:42.991Z"))
        .success()
        .stdout(predicate::str::contains("\"100\"").and(predicate::str::contains("30000")));

    query(Some("snapshot:9"))
        .failure()
        .stderr(predicate::str::contains("snapshot 9 not found"));
    query(Some("time:2020-01-01T00:00:00Z"))
        .failure()
        .stderr(predicate::str::contains("oldest retained snapshot"));
    query(Some("t:1"))
        .failure()
        .stderr(predicate::str::contains("no transaction numbers"));
}
