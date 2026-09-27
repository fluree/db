//! `fluree query <graph-source> --at …` through the binary.
//!
//! The CLI used to refuse `--at` on any graph-source target, so a table pin
//! was unreachable from `fluree query` even though the API honors it. The
//! fixture is the two-snapshot `silver/people` table committed in
//! `fluree-db-api`: three rows at its first snapshot, five now.

#![cfg(feature = "iceberg")]

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

const FIRST_SNAPSHOT: &str = "4694811788682220522";

const MAPPING: &str = r#"
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
<http://example.org/mapping#People> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "silver.people" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
"#;

const COUNT: &str = "SELECT (COUNT(?s) AS ?n) WHERE { ?s <http://example.org/name> ?name }";

fn fixtures() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fluree-db-api/tests/fixtures/iceberg")
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

#[test]
fn at_pins_an_iceberg_graph_source_query_to_a_snapshot() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    std::fs::write(tmp.path().join("people.ttl"), MAPPING).unwrap();
    fluree_cmd(&tmp)
        .args(["iceberg", "map", "people", "--mode", "direct"])
        .args([
            "--table-location",
            &format!("file://{}/silver/people", fixtures()),
        ])
        .args(["--r2rml", "people.ttl"])
        .assert()
        .success();

    let count = |at: Option<&str>| {
        let mut cmd = fluree_cmd(&tmp);
        cmd.args(["query", "people", "--format", "json"]);
        if let Some(at) = at {
            cmd.args(["--at", at]);
        }
        cmd.args(["--sparql", COUNT]);
        cmd.assert()
    };
    let value = |n: u32| predicate::str::is_match(format!(r#""value":\s*"{n}""#)).unwrap();

    count(None).success().stdout(value(5));
    count(Some(&format!("snapshot:{FIRST_SNAPSHOT}")))
        .success()
        .stdout(value(3));

    count(Some("snapshot:999"))
        .failure()
        .stderr(predicate::str::contains("snapshot 999 not found"));
    count(Some("t:1"))
        .failure()
        .stderr(predicate::str::contains("no transaction numbers"));
}
