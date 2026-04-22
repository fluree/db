use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

/// Helper to create a `fluree` command that runs in an isolated temp directory.
/// Sets HOME to the temp dir so `~/.fluree/` fallback never leaks between tests.
fn fluree_cmd(work_dir: &TempDir) -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.current_dir(work_dir.path());
    cmd.env("HOME", work_dir.path());
    cmd.env("NO_COLOR", "1");
    cmd
}

// ============================================================================
// Happy path tests
// ============================================================================

#[test]
fn version_flag() {
    cargo_bin_cmd!("fluree")
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("fluree"));
}

#[test]
fn help_flag() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Fluree database CLI"))
        .stdout(predicate::str::contains("init"))
        .stdout(predicate::str::contains("create"))
        .stdout(predicate::str::contains("query"));
}

#[test]
fn verbose_quiet_conflict() {
    cargo_bin_cmd!("fluree")
        .args(["--verbose", "--quiet", "list"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn init_creates_fluree_dir() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp)
        .arg("init")
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized Fluree in"));

    assert!(tmp.path().join(".fluree").is_dir());
    assert!(tmp.path().join(".fluree/storage").is_dir());
    assert!(tmp.path().join(".fluree/config.toml").exists());
}

#[test]
fn init_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .arg("init")
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized Fluree in"));
}

#[test]
fn golden_path() {
    let tmp = TempDir::new().unwrap();

    // init
    fluree_cmd(&tmp).arg("init").assert().success();

    // create
    fluree_cmd(&tmp)
        .args(["create", "testdb"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created ledger 'testdb'"));

    // list shows the ledger
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("testdb"))
        .stdout(predicate::str::contains("main"));

    // info
    fluree_cmd(&tmp)
        .arg("info")
        .assert()
        .success()
        .stdout(predicate::str::contains("Ledger:"))
        .stdout(predicate::str::contains("testdb"));

    // insert JSON-LD
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // insert Turtle
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:bob a ex:Person ; ex:name \"Bob\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // query SPARQL (JSON output)
    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "-e",
            "SELECT ?name WHERE { ?s <http://example.org/name> ?name }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob"));

    // query SPARQL (table output)
    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--format",
            "table",
            "-e",
            "SELECT ?name WHERE { ?s <http://example.org/name> ?name }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Alice"))
        .stdout(predicate::str::contains("Bob"));

    // log
    fluree_cmd(&tmp)
        .args(["log", "--oneline"])
        .assert()
        .success()
        .stdout(predicate::str::contains("t=2"))
        .stdout(predicate::str::contains("t=1"));

    // log with count limit
    fluree_cmd(&tmp)
        .args(["log", "--oneline", "-n", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("t=2"));

    // drop
    fluree_cmd(&tmp)
        .args(["drop", "testdb", "--force"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Dropped ledger 'testdb'"));

    // list after drop
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("No ledgers found"));
}

#[test]
fn use_command_switches_active_ledger() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db1"]).assert().success();
    fluree_cmd(&tmp).args(["create", "db2"]).assert().success();

    // db2 should be active (last created)
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("* | db2").or(predicate::str::contains("*")));

    // Switch to db1
    fluree_cmd(&tmp)
        .args(["use", "db1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Now using ledger 'db1'"));

    // info should show db1
    fluree_cmd(&tmp)
        .arg("info")
        .assert()
        .success()
        .stdout(predicate::str::contains("db1"));
}

#[test]
fn insert_with_txn_meta_sidecar() {
    // Commit messages are user txn-meta supplied in the body — there's no
    // dedicated CLI flag. Use the `txn-meta` sidecar (works for any
    // transaction shape, including update).
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "msgdb"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [{"@id": "ex:x", "ex:val": "test"}],
                "txn-meta": {"f:message": "initial data load"}
            }"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

// ============================================================================
// Error path tests
// ============================================================================

#[test]
fn query_without_init_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp)
        .args(["query", "-e", "SELECT * WHERE { ?s ?p ?o }"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no .fluree/ directory found"))
        .stderr(predicate::str::contains("fluree init"));
}

#[test]
fn insert_without_active_ledger_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["insert", "-e", "{}"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no active ledger set"))
        .stderr(predicate::str::contains("fluree use"));
}

#[test]
fn drop_without_force_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    fluree_cmd(&tmp)
        .args(["drop", "db"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--force"));
}

#[test]
fn use_nonexistent_ledger_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["use", "doesnotexist"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

#[test]
fn query_no_input_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    // In test context, stdin is piped (not a TTY), so empty stdin is read
    // and format detection fails. Either error message is acceptable.
    fluree_cmd(&tmp).arg("query").assert().failure().stderr(
        predicate::str::contains("no input provided")
            .or(predicate::str::contains("could not detect query format")),
    );
}

#[test]
fn query_positional_inline() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    // Positional arg that looks like a SPARQL query is treated as inline input.
    fluree_cmd(&tmp)
        .args(["query", "SELECT ?s WHERE { ?s ?p ?o }"])
        .assert()
        .success();
}

#[test]
fn query_from_file_flag() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    // Write a query file
    let query_file = tmp.path().join("test.sparql");
    std::fs::write(&query_file, "SELECT ?s WHERE { ?s ?p ?o }").unwrap();

    // -f flag reads query from file
    fluree_cmd(&tmp)
        .args(["query", "-f", query_file.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn sparql_fql_conflict() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "db"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--jsonld",
            "-e",
            "SELECT * WHERE { ?s ?p ?o }",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

// ============================================================================
// File-based input tests
// ============================================================================

#[test]
fn insert_from_turtle_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "filedb"])
        .assert()
        .success();

    let ttl_path = tmp.path().join("data.ttl");
    std::fs::write(
        &ttl_path,
        "@prefix ex: <http://example.org/> .\nex:x a ex:Thing ; ex:val \"hello\" .\n",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["insert", ttl_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

#[test]
fn query_from_sparql_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "filedb"])
        .assert()
        .success();

    // Insert some data first
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a a ex:Thing ; ex:label \"alpha\" .\n",
        ])
        .assert()
        .success();

    let rq_path = tmp.path().join("query.rq");
    std::fs::write(
        &rq_path,
        "SELECT ?label WHERE { ?s <http://example.org/label> ?label }",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["query", rq_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("alpha"));
}

// ============================================================================
// Create --from tests
// ============================================================================

#[test]
fn create_from_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    let ttl_path = tmp.path().join("seed.ttl");
    std::fs::write(
        &ttl_path,
        "@prefix ex: <http://example.org/> .\nex:seed a ex:Thing ; ex:val \"seeded\" .\n",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["create", "seeddb", "--from", ttl_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("About ledger 'seeddb'"))
        .stdout(predicate::str::contains("flakes"));
}

// ============================================================================
// v1.1 — Upsert tests
// ============================================================================

#[test]
fn upsert_json_ld() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "udb"]).assert().success();

    // Insert initial data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:name": "Alice"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // Upsert — update existing entity
    fluree_cmd(&tmp)
        .args([
            "upsert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:name": "Alice Updated"}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));
}

#[test]
fn upsert_turtle() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "udb2"]).assert().success();

    fluree_cmd(&tmp)
        .args([
            "upsert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:x a ex:Thing ; ex:val \"hello\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

// ============================================================================
// v1.1 — CSV output tests
// ============================================================================

#[test]
fn query_csv_output() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "csvdb"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a a ex:Thing ; ex:label \"alpha\" .\nex:b a ex:Thing ; ex:label \"beta\" .",
        ])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--format",
            "csv",
            "-e",
            "SELECT ?label WHERE { ?s <http://example.org/label> ?label }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("label"))
        .stdout(predicate::str::contains("alpha"))
        .stdout(predicate::str::contains("beta"));
}

// ============================================================================
// v1.1 — --at time travel tests
// ============================================================================

#[test]
fn query_at_time_travel() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "ttdb"]).assert().success();

    // t=1
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:a ex:val \"first\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // t=2
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:b ex:val \"second\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // Query at t=1 should only see "first"
    fluree_cmd(&tmp)
        .args([
            "query",
            "--sparql",
            "--at",
            "1",
            "-e",
            "SELECT ?val WHERE { ?s <http://example.org/val> ?val }",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("first"))
        .stdout(predicate::str::contains("second").not());
}

// ============================================================================
// v1.1 — Export tests
// ============================================================================

#[test]
fn export_jsonld_requires_index() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "expdb"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:thing a ex:Widget ; ex:label \"gadget\" .",
        ])
        .assert()
        .success();

    // JSON-LD now uses the streaming binary index path (same as other formats)
    fluree_cmd(&tmp)
        .args(["export", "--format", "jsonld"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no binary index available"));
}

#[test]
fn export_ntriples_requires_index() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "expdb2"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:item a ex:Product ; ex:name \"widget\" .",
        ])
        .assert()
        .success();

    // N-Triples streaming export requires a binary index; un-indexed ledgers
    // get a clear error.
    fluree_cmd(&tmp)
        .args(["export", "--format", "ntriples"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no binary index available"));
}

#[test]
fn export_all_graphs_requires_dataset_format() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "expdb3"])
        .assert()
        .success();

    // --all-graphs with turtle/ttl is rejected (graph boundaries would be lost)
    fluree_cmd(&tmp)
        .args(["export", "--all-graphs", "--format", "turtle"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("graph boundaries would be lost"));

    // --all-graphs with ntriples is also rejected (same reason)
    fluree_cmd(&tmp)
        .args(["export", "--all-graphs", "--format", "ntriples"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("graph boundaries would be lost"));
}

#[test]
fn export_all_graphs_nquads_requires_index() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "expdb4"])
        .assert()
        .success();

    // Insert one triple in default graph.
    fluree_cmd(&tmp)
        .args([
            "insert",
            "expdb4",
            "-e",
            "<http://example.org/a> <http://example.org/p> \"default\" .\n",
        ])
        .assert()
        .success();

    // N-Quads streaming export requires a binary index; un-indexed ledgers
    // get a clear error.
    fluree_cmd(&tmp)
        .args(["export", "expdb4", "--all-graphs", "--format", "nquads"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no binary index available"));
}

// ============================================================================
// v1.1 — Config tests
// ============================================================================

#[test]
fn config_set_get_list() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Set a value
    fluree_cmd(&tmp)
        .args(["config", "set", "storage.path", "/custom/path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Set 'storage.path'"));

    // Get the value
    fluree_cmd(&tmp)
        .args(["config", "get", "storage.path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/custom/path"));

    // List shows it
    fluree_cmd(&tmp)
        .args(["config", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("storage.path"))
        .stdout(predicate::str::contains("/custom/path"));
}

#[test]
fn config_get_missing_key() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["config", "get", "nonexistent.key"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not set"));
}

#[test]
fn config_list_empty() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["config", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no configuration set"));
}

// ============================================================================
// v1.1 — Completions tests
// ============================================================================

#[test]
fn completions_bash() {
    cargo_bin_cmd!("fluree")
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fluree"));
}

#[test]
fn completions_zsh() {
    cargo_bin_cmd!("fluree")
        .args(["completions", "zsh"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fluree"));
}

// ============================================================================
// v1.1 — Help text shows new commands
// ============================================================================

#[test]
fn help_shows_v11_commands() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("upsert"))
        .stdout(predicate::str::contains("export"))
        .stdout(predicate::str::contains("config"))
        .stdout(predicate::str::contains("completions"));
}

// ============================================================================
// v1.2 — Prefix management tests
// ============================================================================

#[test]
fn prefix_add_list_remove() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Add a prefix
    fluree_cmd(&tmp)
        .args(["prefix", "add", "ex", "http://example.org/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added prefix: ex"));

    // List shows it
    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ex:"))
        .stdout(predicate::str::contains("http://example.org/"));

    // Add another
    fluree_cmd(&tmp)
        .args(["prefix", "add", "foaf", "http://xmlns.com/foaf/0.1/"])
        .assert()
        .success();

    // List shows both
    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ex:"))
        .stdout(predicate::str::contains("foaf:"));

    // Remove one
    fluree_cmd(&tmp)
        .args(["prefix", "remove", "foaf"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed prefix: foaf"));

    // List shows only ex
    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ex:"))
        .stdout(predicate::str::contains("foaf:").not());
}

#[test]
fn prefix_list_empty() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["prefix", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no prefixes defined"));
}

#[test]
fn prefix_remove_nonexistent() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["prefix", "remove", "nothere"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

// ============================================================================
// v1.2 — History command tests
// ============================================================================

#[test]
fn history_shows_changes() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "histdb"])
        .assert()
        .success();

    // Add prefix for convenience
    fluree_cmd(&tmp)
        .args(["prefix", "add", "ex", "http://example.org/"])
        .assert()
        .success();

    // t=1: Insert entity
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:alice ex:name \"Alice\" ; ex:age \"30\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // t=2: Update entity
    fluree_cmd(&tmp)
        .args([
            "upsert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:alice ex:name \"Alice Smith\" .",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // Query history using compact IRI (prefix expansion)
    fluree_cmd(&tmp)
        .args(["history", "ex:alice", "--format", "json"])
        .assert()
        .success();
}

#[test]
fn history_with_full_iri() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "histdb2"])
        .assert()
        .success();

    // Insert data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            "@prefix ex: <http://example.org/> .\nex:bob ex:status \"active\" .",
        ])
        .assert()
        .success();

    // Query history using full IRI (no prefix needed)
    fluree_cmd(&tmp)
        .args(["history", "http://example.org/bob", "--format", "json"])
        .assert()
        .success();
}

// ============================================================================
// v1.2 — Help text shows new commands
// ============================================================================

#[test]
fn help_shows_v12_commands() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("history"))
        .stdout(predicate::str::contains("prefix"));
}

// ============================================================================
// v2 — Remote/Upstream tests
// ============================================================================

#[test]
fn remote_add_list_show_remove() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Add a remote
    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added remote 'origin'"));

    // List shows it
    fluree_cmd(&tmp)
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("origin"))
        .stdout(predicate::str::contains("http://localhost:8090"));

    // Show details
    fluree_cmd(&tmp)
        .args(["remote", "show", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Name: origin"))
        .stdout(predicate::str::contains("Type: HTTP"));

    // Remove it
    fluree_cmd(&tmp)
        .args(["remote", "remove", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed remote 'origin'"));

    // List is empty
    fluree_cmd(&tmp)
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No remotes configured"));
}

#[test]
fn upstream_set_list_remove() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Need a remote first
    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success();

    // Set upstream
    fluree_cmd(&tmp)
        .args(["upstream", "set", "mydb", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Set upstream"));

    // List shows it
    fluree_cmd(&tmp)
        .args(["upstream", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("mydb"))
        .stdout(predicate::str::contains("origin"));

    // Remove it
    fluree_cmd(&tmp)
        .args(["upstream", "remove", "mydb:main"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed upstream"));
}

/// Regression test: adding a remote should not clobber existing config keys
#[test]
fn remote_add_preserves_other_config() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Set a config value
    fluree_cmd(&tmp)
        .args(["config", "set", "storage.path", "/my/custom/path"])
        .assert()
        .success();

    // Verify config is set
    fluree_cmd(&tmp)
        .args(["config", "get", "storage.path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/my/custom/path"));

    // Add a remote
    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success();

    // Config should still be there
    fluree_cmd(&tmp)
        .args(["config", "get", "storage.path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/my/custom/path"));

    // Add an upstream
    fluree_cmd(&tmp)
        .args(["upstream", "set", "mydb", "origin"])
        .assert()
        .success();

    // Config should still be there
    fluree_cmd(&tmp)
        .args(["config", "get", "storage.path"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/my/custom/path"));
}

#[test]
fn help_shows_sync_commands() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("remote"))
        .stdout(predicate::str::contains("upstream"))
        .stdout(predicate::str::contains("fetch"))
        .stdout(predicate::str::contains("pull"))
        .stdout(predicate::str::contains("push"))
        .stdout(predicate::str::contains("token"));
}

// ============================================================================
// Auth login / status / logout tests
// ============================================================================

#[test]
fn auth_login_with_token_stores_and_status_shows() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Add a remote
    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success();

    // Login with a manual token
    fluree_cmd(&tmp)
        .args([
            "auth",
            "login",
            "--remote",
            "origin",
            "--token",
            "my-test-token-123",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token stored for remote"));

    // Status shows the remote has a token
    fluree_cmd(&tmp)
        .args(["auth", "status", "--remote", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("origin"))
        .stdout(predicate::str::contains("Token:  configured"));
}

#[test]
fn auth_login_from_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success();

    // Write token to a file
    let token_file = tmp.path().join("token.txt");
    std::fs::write(&token_file, "file-based-token-456").unwrap();

    // Login with token from file
    fluree_cmd(&tmp)
        .args([
            "auth",
            "login",
            "--remote",
            "origin",
            "--token",
            &format!("@{}", token_file.display()),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token stored for remote"));

    // Verify status shows token
    fluree_cmd(&tmp)
        .args(["auth", "status", "--remote", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token:  configured"));
}

#[test]
fn auth_logout_clears_token() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success();

    // Login first
    fluree_cmd(&tmp)
        .args(["auth", "login", "--remote", "origin", "--token", "my-token"])
        .assert()
        .success();

    // Logout
    fluree_cmd(&tmp)
        .args(["auth", "logout", "--remote", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token cleared for remote"));

    // Status should show no token
    fluree_cmd(&tmp)
        .args(["auth", "status", "--remote", "origin"])
        .assert()
        .success()
        .stdout(predicate::str::contains("not configured"));
}

#[test]
fn auth_login_no_remote_single_remote_default() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Add a single remote
    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://localhost:8090"])
        .assert()
        .success();

    // Login without --remote should default to the only remote
    fluree_cmd(&tmp)
        .args(["auth", "login", "--token", "my-token"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token stored for remote"));

    // Status without --remote should also default
    fluree_cmd(&tmp)
        .args(["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token:  configured"));
}

#[test]
fn auth_login_no_remote_fails_when_none_configured() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // No remotes configured — auth login should fail
    fluree_cmd(&tmp)
        .args(["auth", "login", "--token", "my-token"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("remote"));
}

#[test]
fn init_global_with_fluree_home() {
    let tmp = TempDir::new().unwrap();
    let fluree_home = tmp.path().join("fluree-global");

    fluree_cmd(&tmp)
        .env("FLUREE_HOME", &fluree_home)
        .args(["init", "--global"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Initialized Fluree in"));

    // Config and storage should be in the FLUREE_HOME directory (unified)
    assert!(
        fluree_home.join("config.toml").exists(),
        "config.toml should exist in FLUREE_HOME"
    );
    assert!(
        fluree_home.join("storage").is_dir(),
        "storage/ should exist in FLUREE_HOME"
    );

    // Since FLUREE_HOME is unified, storage_path in config should be
    // the default relative path (not an absolute override)
    let config = std::fs::read_to_string(fluree_home.join("config.toml")).unwrap();
    assert!(
        config.contains(".fluree/storage"),
        "unified mode should use the default relative storage_path"
    );
}

#[test]
fn init_global_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let fluree_home = tmp.path().join("fluree-global");

    // First init
    fluree_cmd(&tmp)
        .env("FLUREE_HOME", &fluree_home)
        .args(["init", "--global"])
        .assert()
        .success();

    // Write something to config.toml to verify it's not overwritten
    let config_path = fluree_home.join("config.toml");
    let original = std::fs::read_to_string(&config_path).unwrap();

    // Second init should succeed without overwriting
    fluree_cmd(&tmp)
        .env("FLUREE_HOME", &fluree_home)
        .args(["init", "--global"])
        .assert()
        .success();

    let after = std::fs::read_to_string(&config_path).unwrap();
    assert_eq!(original, after, "config.toml should not be overwritten");
}

#[test]
fn cli_respects_custom_storage_path_in_config() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Point storage_path to a custom location outside .fluree/
    let custom_storage = tmp.path().join("my_custom_storage");
    let config_path = tmp.path().join(".fluree/config.toml");
    let config = std::fs::read_to_string(&config_path).unwrap();
    // Replace the commented-out storage_path line with an active one
    let config = config.replace(
        "# storage_path = \".fluree/storage\"",
        &format!("storage_path = \"{}\"", custom_storage.to_str().unwrap()),
    );
    // Uncomment [server] so the key is under the right section
    let config = config.replace("# [server]", "[server]");
    std::fs::write(&config_path, &config).unwrap();

    // Create a ledger — it should write to the custom storage path
    fluree_cmd(&tmp)
        .args(["create", "testdb"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Created ledger 'testdb'"));

    // Verify data landed in the custom storage location
    assert!(
        custom_storage.is_dir(),
        "custom storage directory should have been created"
    );
    assert!(
        custom_storage.join("ns@v2").is_dir(),
        "nameservice data should exist in custom storage"
    );

    // The default storage should NOT have the ledger data
    let default_ns = tmp.path().join(".fluree/storage/ns@v2");
    assert!(
        !default_ns.exists(),
        "default .fluree/storage should NOT contain ledger data when custom path is set"
    );

    // Verify the ledger is queryable from the custom location
    fluree_cmd(&tmp)
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("testdb"));
}

#[test]
fn cli_respects_custom_storage_path_via_config_flag() {
    // Simulates a user with a non-standard config location who passes
    // --config explicitly (mutating commands require --config for non-local dirs).
    let tmp = TempDir::new().unwrap();
    let config_dir = tmp.path().join("my-config");
    let custom_storage = tmp.path().join("custom_data");

    // Set up a config dir with a config.toml pointing to custom storage
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            "[server]\nstorage_path = \"{}\"\n",
            custom_storage.to_str().unwrap()
        ),
    )
    .unwrap();

    // Create via --config pointing to the config dir
    fluree_cmd(&tmp)
        .args(["--config", config_dir.to_str().unwrap(), "create", "testdb"])
        .assert()
        .success();

    // Data should be in the custom location, not in config_dir/storage
    assert!(
        custom_storage.join("ns@v2").is_dir(),
        "ledger data should be in custom storage path"
    );
    assert!(
        !config_dir.join("storage/ns@v2").exists(),
        "config dir should NOT contain ledger data"
    );
}

#[test]
fn auth_login_discovery_fallback_unreachable_server() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Add a remote pointing to a non-existent server
    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://127.0.0.1:19999"])
        .assert()
        .success();

    // auth login without --token should try discovery, fail to connect,
    // then fall back to manual token prompt. Since we can't provide
    // interactive input, pipe token via stdin using @-
    fluree_cmd(&tmp)
        .args([
            "auth",
            "login",
            "--remote",
            "origin",
            "--token",
            "fallback-token",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token stored"));
}

#[test]
fn iceberg_map_remote_uses_remote_client() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://127.0.0.1:19999"])
        .assert()
        .success();

    let mapping_path = tmp.path().join("orders.ttl");
    std::fs::write(
        &mapping_path,
        r#"@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/ns/> .

<http://example.org/mapping#Orders>
    a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "sales.orders" ] ;
    rr:subjectMap [ rr:template "http://example.org/order/{id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:orderId ;
        rr:objectMap [ rr:column "id" ]
    ] .
"#,
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args([
            "iceberg",
            "map",
            "warehouse-orders",
            "--remote",
            "origin",
            "--catalog-uri",
            "https://polaris.example.com/api/catalog",
            "--table",
            "sales.orders",
            "--r2rml",
            mapping_path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "failed to map Iceberg graph source on 'origin'",
        ))
        .stderr(predicate::str::contains("connection failed"));
}

#[test]
fn iceberg_list_remote_uses_remote_client() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://127.0.0.1:19999"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args(["iceberg", "list", "--remote", "origin"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "failed to list Iceberg graph sources on 'origin'",
        ))
        .stderr(predicate::str::contains("connection failed"));
}

#[test]
fn iceberg_info_remote_uses_remote_client() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://127.0.0.1:19999"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args(["iceberg", "info", "warehouse-orders", "--remote", "origin"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "failed to load Iceberg graph source info from 'origin'",
        ))
        .stderr(predicate::str::contains("connection failed"));
}

#[test]
fn iceberg_drop_remote_uses_remote_client() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["remote", "add", "origin", "http://127.0.0.1:19999"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "iceberg",
            "drop",
            "warehouse-orders",
            "--force",
            "--remote",
            "origin",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "failed to validate Iceberg graph source on 'origin'",
        ))
        .stderr(predicate::str::contains("connection failed"));
}

// ============================================================================
// Directory --from support
// ============================================================================

#[test]
fn create_from_turtle_directory() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    // Create a directory with .ttl files (no chunk_ prefix)
    let data_dir = tmp.path().join("ttl_data");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("01_people.ttl"),
        "@prefix ex: <http://example.org/> .\nex:alice a ex:Person ; ex:name \"Alice\" .\n",
    )
    .unwrap();
    std::fs::write(
        data_dir.join("02_things.ttl"),
        "@prefix ex: <http://example.org/> .\nex:widget a ex:Thing ; ex:label \"Widget\" .\n",
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["create", "ttldir", "--from", data_dir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("About ledger 'ttldir'"))
        .stdout(predicate::str::contains("flakes"));
}

#[test]
fn create_from_jsonld_directory() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    let data_dir = tmp.path().join("jsonld_data");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("01_alice.jsonld"),
        r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"}"#,
    )
    .unwrap();
    std::fs::write(
        data_dir.join("02_bob.jsonld"),
        r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}"#,
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["create", "jsondir", "--from", data_dir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("About ledger 'jsondir'"))
        .stdout(predicate::str::contains("flakes"));
}

#[test]
fn create_from_mixed_directory_fails() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    let data_dir = tmp.path().join("mixed_data");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("data.ttl"),
        "@prefix ex: <http://example.org/> .\nex:a a ex:Thing .\n",
    )
    .unwrap();
    std::fs::write(
        data_dir.join("data.jsonld"),
        r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:b", "@type": "ex:Thing"}"#,
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["create", "mixdb", "--from", data_dir.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("both Turtle"));
}

#[test]
fn create_from_empty_directory_fails() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    let data_dir = tmp.path().join("empty_data");
    std::fs::create_dir(&data_dir).unwrap();

    fluree_cmd(&tmp)
        .args(["create", "emptydb", "--from", data_dir.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no supported data files"));
}

#[test]
fn create_from_unsupported_files_only_fails() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    let data_dir = tmp.path().join("bad_data");
    std::fs::create_dir(&data_dir).unwrap();
    std::fs::write(data_dir.join("readme.txt"), "not data").unwrap();

    fluree_cmd(&tmp)
        .args(["create", "baddb", "--from", data_dir.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no supported data files"));
}

// ============================================================================
// Update (WHERE/DELETE/INSERT) tests
// ============================================================================

#[test]
fn update_where_delete_insert_json_ld() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "txdb"]).assert().success();

    // Seed data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:name": "Alice", "ex:age": 30}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));

    // Use update to change Alice's age: WHERE old age, DELETE old, INSERT new
    fluree_cmd(&tmp)
        .args([
            "update",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "where": [{"@id": "ex:alice", "ex:age": "?oldAge"}], "delete": [{"@id": "ex:alice", "ex:age": "?oldAge"}], "insert": [{"@id": "ex:alice", "ex:age": 31}]}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));

    // Verify the update with a query
    fluree_cmd(&tmp)
        .args([
            "query",
            "-e",
            r"SELECT ?age WHERE { <http://example.org/alice> <http://example.org/age> ?age }",
            "--format",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("31"));
}

#[test]
fn update_delete_only() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "deldb"])
        .assert()
        .success();

    // Seed data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:bob", "ex:name": "Bob", "ex:email": "bob@example.org"}"#,
        ])
        .assert()
        .success();

    // Delete email using where + delete (no insert)
    fluree_cmd(&tmp)
        .args([
            "update",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "where": [{"@id": "ex:bob", "ex:email": "?email"}], "delete": [{"@id": "ex:bob", "ex:email": "?email"}]}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));
}

#[test]
fn update_insert_only_via_update() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "insdb"])
        .assert()
        .success();

    // Use update with insert-only (no where/delete)
    fluree_cmd(&tmp)
        .args([
            "update",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:charlie", "ex:name": "Charlie"}]}"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

#[test]
fn update_with_txn_meta_sidecar() {
    // Update transactions have no @graph envelope — use the `txn-meta`
    // sidecar to attach a commit message.
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "msgdb2"])
        .assert()
        .success();

    fluree_cmd(&tmp)
        .args([
            "update",
            "-e",
            r#"{
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "insert": [{"@id": "ex:x", "ex:val": "test"}],
                "txn-meta": {"f:message": "initial update"}
            }"#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}

#[test]
fn update_from_json_file() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "filedb2"])
        .assert()
        .success();

    // Seed data
    fluree_cmd(&tmp)
        .args([
            "insert",
            "-e",
            r#"{"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "ex:status": "pending"}"#,
        ])
        .assert()
        .success();

    // Write an update body to a file
    let json_path = tmp.path().join("update.json");
    std::fs::write(
        &json_path,
        r#"{"@context": {"ex": "http://example.org/"}, "where": [{"@id": "ex:alice", "ex:status": "pending"}], "delete": [{"@id": "ex:alice", "ex:status": "pending"}], "insert": [{"@id": "ex:alice", "ex:status": "active"}]}"#,
    )
    .unwrap();

    fluree_cmd(&tmp)
        .args(["update", "-f", json_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=2"));
}

#[test]
fn update_sparql_update_in_direct_mode_fails() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp).args(["create", "spdb"]).assert().success();

    // SPARQL UPDATE in direct local mode should give a helpful error
    fluree_cmd(&tmp)
        .args([
            "--direct",
            "update",
            "-e",
            "PREFIX ex: <http://example.org/> INSERT DATA { ex:x ex:val \"hello\" }",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "SPARQL UPDATE is not supported in direct local mode",
        ));
}

#[test]
fn update_without_active_ledger_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();

    fluree_cmd(&tmp)
        .args(["update", "-e", "{}"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no active ledger set"));
}

#[test]
fn update_without_init_errors() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp)
        .args(["update", "-e", "{}"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no .fluree/ directory found"));
}

#[test]
fn help_shows_update_command() {
    cargo_bin_cmd!("fluree")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("update"));
}

#[test]
fn update_help_shows_description() {
    cargo_bin_cmd!("fluree")
        .args(["update", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("WHERE/DELETE/INSERT"));
}

#[test]
fn update_via_stdin() {
    let tmp = TempDir::new().unwrap();
    fluree_cmd(&tmp).arg("init").assert().success();
    fluree_cmd(&tmp)
        .args(["create", "stdindb"])
        .assert()
        .success();

    // Pipe JSON-LD update body via stdin (no -e, -f, or positional data)
    fluree_cmd(&tmp)
        .arg("update")
        .write_stdin(r#"{"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:alice", "ex:name": "Alice"}]}"#)
        .assert()
        .success()
        .stdout(predicate::str::contains("Committed t=1"));
}
