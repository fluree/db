//! End-to-end tests for `fluree rdf`.
//!
//! These drive the real binary, because most of what this surface promises is
//! only true at the process boundary: exit codes, which stream output lands
//! on, whether stdin works, whether a `.gz` decodes. Unit tests inside
//! `src/rdf/` cover the resolution and rendering logic; this file covers the
//! contract a script sees.

use assert_cmd::cargo_bin_cmd;
use assert_cmd::Command;
use predicates::prelude::*;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Exit code for a document that did not parse.
const EXIT_DOCUMENT_INVALID: i32 = 1;
/// Exit code for an invocation that was wrong.
const EXIT_USAGE: i32 = 2;

const VALID_TURTLE: &str = "@prefix ex: <http://example.org/> .\n\
                            ex:alice ex:name \"Alice\" ;\n\
                                     ex:age 30 .\n\
                            ex:bob ex:name \"Bob\" .\n";

/// 3 triples: alice's name and age, plus bob's name.
const VALID_TURTLE_TRIPLES: u64 = 3;

const BROKEN_TURTLE: &str = "@prefix ex: <http://example.org/> .\n\
                             ex:alice ex:name \"Alice\" .\n\
                             ex:bob ex:name ?? .\n";

const VALID_NTRIPLES: &str = "<http://example.org/a> <http://example.org/b> \"c\" .\n\
                              <http://example.org/d> <http://example.org/e> \"f\" .\n";

fn rdf_cmd() -> Command {
    let mut cmd = cargo_bin_cmd!("fluree");
    cmd.env("NO_COLOR", "1");
    cmd
}

/// Write `content` into `dir` under `name` and return the path.
fn fixture(dir: &TempDir, name: &str, content: &str) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, content).unwrap();
    path
}

fn gz_fixture(dir: &TempDir, name: &str, content: &str) -> PathBuf {
    let path = dir.path().join(name);
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(content.as_bytes()).unwrap();
    std::fs::write(&path, enc.finish().unwrap()).unwrap();
    path
}

fn zst_fixture(dir: &TempDir, name: &str, content: &str) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(
        &path,
        zstd::stream::encode_all(content.as_bytes(), 3).unwrap(),
    )
    .unwrap();
    path
}

/// stdout of a successful run, as a string.
fn stdout_of(cmd: &mut Command) -> String {
    let out = cmd.assert().success().get_output().stdout.clone();
    String::from_utf8(out).unwrap()
}

// ============================================================================
// check
// ============================================================================

#[test]
fn check_accepts_a_valid_turtle_file() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .success()
        .stderr(predicate::str::contains("no syntax errors"));
}

#[test]
fn check_rejects_a_broken_turtle_file_with_exit_1_and_a_located_diagnostic() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        // line:column, the offending line, and a caret under it — on stderr,
        // where riot and `count` put theirs.
        .stderr(predicate::str::contains("broken.ttl:3:16"))
        .stderr(predicate::str::contains("ex:bob ex:name ?? ."))
        .stderr(predicate::str::contains("^"))
        .stdout(predicate::str::is_empty());
}

#[test]
fn check_reads_ntriples_by_extension() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.nt", VALID_NTRIPLES);
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .success();
}

#[test]
fn check_reads_stdin_when_no_file_is_named() {
    rdf_cmd()
        .args(["rdf", "check", "--syntax", "turtle"])
        .write_stdin(VALID_TURTLE)
        .assert()
        .success()
        .stderr(predicate::str::contains("<stdin>"));
}

#[test]
fn check_reads_stdin_when_the_file_is_a_dash() {
    rdf_cmd()
        .args(["rdf", "check", "-"])
        .write_stdin(VALID_TURTLE)
        .assert()
        .success();
}

#[test]
fn check_sniffs_the_syntax_of_a_pipe_with_no_flag() {
    // Nothing names the syntax: no extension, no --syntax. The leading
    // `@prefix` has to be enough.
    rdf_cmd()
        .args(["rdf", "check"])
        .write_stdin(VALID_TURTLE)
        .assert()
        .success();
}

#[test]
fn check_decompresses_a_gzipped_file() {
    let tmp = TempDir::new().unwrap();
    let path = gz_fixture(&tmp, "valid.ttl.gz", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .success();
}

#[test]
fn check_decompresses_a_gzipped_pipe_from_its_magic_bytes() {
    // A pipe has no suffix to read. Without magic-byte detection this is a
    // UTF-8 error on compressed bytes.
    let tmp = TempDir::new().unwrap();
    let path = gz_fixture(&tmp, "valid.ttl.gz", VALID_TURTLE);
    let bytes = std::fs::read(&path).unwrap();
    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "check", "--syntax", "turtle"]);
    cmd.write_stdin(bytes).assert().success();
}

#[test]
fn check_json_format_reports_the_diagnostic_with_offsets() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);
    let out = rdf_cmd()
        .args(["rdf", "check", "--format", "json"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();

    assert_eq!(v["schema"], "fluree.rdf.check.v1");
    assert_eq!(v["ok"], false);
    assert_eq!(v["syntax"], "turtle");
    assert_eq!(v["diagnostics"][0]["line"], 3);
    assert_eq!(v["diagnostics"][0]["column"], 16);
    assert!(v["diagnostics"][0]["offset"].as_u64().unwrap() > 0);
    assert_eq!(
        v["grammar_statements"], 2,
        "the `@prefix` directive plus the one triple statement that parsed \
         before the failure — Turtle counts a directive as a statement"
    );
}

#[test]
fn check_json_format_on_a_clean_document_reports_an_empty_diagnostic_array() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "check", "--format", "json"]).arg(&path);
    let v: serde_json::Value = serde_json::from_str(&stdout_of(&mut cmd)).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["diagnostics"].as_array().unwrap().len(), 0);
}

#[test]
fn check_under_quiet_says_nothing_on_success_and_still_exits_0() {
    // The loop-over-ten-thousand-files case: the exit code is the answer.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    rdf_cmd()
        .args(["-q", "rdf", "check"])
        .arg(&path)
        .assert()
        .success()
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::is_empty());
}

// ============================================================================
// count
// ============================================================================

#[test]
fn count_reports_the_triple_count_of_a_known_fixture() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "count"])
        .arg(&path)
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "triples: {VALID_TURTLE_TRIPLES}"
        )))
        .stdout(predicate::str::contains("prefixes: 1"));
}

#[test]
fn count_under_quiet_prints_only_the_number() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).arg(&path);
    assert_eq!(
        stdout_of(&mut cmd).trim(),
        VALID_TURTLE_TRIPLES.to_string(),
        "`$(fluree rdf count -q f.ttl)` has to be usable as a number"
    );
}

#[test]
fn count_of_a_collection_matches_the_rdf_spine_not_flattened_list_items() {
    // Three items: 3 rdf:first + 3 rdf:rest + the statement = 7. Fluree's
    // ingest path would flatten this to 3 indexed list items; reporting that
    // number would disagree with every other RDF tool.
    let tmp = TempDir::new().unwrap();
    let path = fixture(
        &tmp,
        "list.ttl",
        "<http://e/s> <http://e/p> ( \"a\" \"b\" \"c\" ) .\n",
    );
    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).arg(&path);
    assert_eq!(stdout_of(&mut cmd).trim(), "7");
}

#[test]
fn count_reads_a_zstd_file() {
    let tmp = TempDir::new().unwrap();
    let path = zst_fixture(&tmp, "valid.ttl.zst", VALID_TURTLE);
    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).arg(&path);
    assert_eq!(stdout_of(&mut cmd).trim(), VALID_TURTLE_TRIPLES.to_string());
}

#[test]
fn count_reads_a_gzipped_file_and_agrees_with_the_plain_one() {
    let tmp = TempDir::new().unwrap();
    let plain = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let gz = gz_fixture(&tmp, "valid.ttl.gz", VALID_TURTLE);

    let mut a = rdf_cmd();
    a.args(["-q", "rdf", "count"]).arg(&plain);
    let mut b = rdf_cmd();
    b.args(["-q", "rdf", "count"]).arg(&gz);
    assert_eq!(stdout_of(&mut a), stdout_of(&mut b));
}

#[test]
fn count_reads_stdin() {
    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).write_stdin(VALID_NTRIPLES);
    assert_eq!(stdout_of(&mut cmd).trim(), "2");
}

#[test]
fn count_refuses_to_print_a_partial_number_for_a_broken_document() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);
    rdf_cmd()
        .args(["rdf", "count"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("before the document stopped"));
}

#[test]
fn count_time_writes_to_stderr_so_stdout_stays_pipeable() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let out = rdf_cmd()
        .args(["-q", "rdf", "count", "--time"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .clone();
    assert_eq!(
        String::from_utf8(out.stdout).unwrap().trim(),
        VALID_TURTLE_TRIPLES.to_string()
    );
    assert!(
        String::from_utf8(out.stderr).unwrap().contains("triples/s"),
        "the timing footer belongs on stderr"
    );
}

// ============================================================================
// syntax resolution at the process boundary
// ============================================================================

#[test]
fn an_explicit_syntax_overrides_a_misleading_extension() {
    // The file is Turtle; the name says N-Quads, which has no reader. The
    // flag has to win, or a mislabelled corpus is unusable.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "actually-turtle.nq", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "check", "--syntax", "turtle"])
        .arg(&path)
        .assert()
        .success();
}

#[test]
fn a_syntax_with_no_reader_is_refused_by_name_with_what_it_waits_on() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "data.trig", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "count"])
        .arg(&path)
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("trig"))
        .stderr(predicate::str::contains("turtle, ntriples"));
}

#[test]
fn an_unidentifiable_input_names_the_flag_instead_of_guessing() {
    // No extension, and content that opens like no RDF syntax does.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "mystery", "42 is not a document\n");
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("--syntax"));
}

#[test]
fn binary_input_is_reported_as_binary_not_as_an_unknown_syntax() {
    // The UTF-8 check runs before syntax resolution, which is the more
    // useful answer: "this is not text" beats "I could not name the syntax".
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("mystery");
    std::fs::write(&path, [0xff, 0xfe, 0x00, 0x01]).unwrap();
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("not valid UTF-8"));
}

#[test]
fn syntax_aliases_are_accepted() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "data.txt", VALID_NTRIPLES);
    for alias in ["nt", "ntriples", "n-triples"] {
        rdf_cmd()
            .args(["rdf", "check", "--syntax", alias])
            .arg(&path)
            .assert()
            .success();
    }
}

// ============================================================================
// exit codes
// ============================================================================

#[test]
fn a_missing_file_exits_2_not_1() {
    // The contract that makes these verbs scriptable: 1 means the RDF is
    // bad, 2 means the invocation is.
    rdf_cmd()
        .args(["rdf", "check", "/nonexistent/nope.ttl"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("nope.ttl"));
}

#[test]
fn a_directory_given_as_input_exits_2() {
    let tmp = TempDir::new().unwrap();
    rdf_cmd()
        .args(["rdf", "count", "--syntax", "turtle"])
        .arg(tmp.path())
        .assert()
        .code(EXIT_USAGE);
}

#[test]
fn every_exit_code_in_the_contract_is_reachable() {
    let tmp = TempDir::new().unwrap();
    let valid = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let broken = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);

    let code = |args: &[&str], path: Option<&Path>| {
        let mut cmd = rdf_cmd();
        cmd.args(args);
        if let Some(p) = path {
            cmd.arg(p);
        }
        cmd.assert().get_output().status.code().unwrap()
    };

    assert_eq!(code(&["rdf", "check"], Some(&valid)), 0);
    assert_eq!(code(&["rdf", "check"], Some(&broken)), 1);
    assert_eq!(code(&["rdf", "check", "/no/such.ttl"], None), 2);
}

// ============================================================================
// convert (stub)
// ============================================================================

#[test]
fn convert_writes_the_default_syntax_when_nothing_names_one() {
    // Was the stub's refusal test. The default is N-Quads, matching riot, and
    // with no `--to` and no output extension that is what should come out.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "convert"]).arg(&path);
    let out = stdout_of(&mut cmd);

    assert_eq!(out.lines().count(), VALID_TURTLE_TRIPLES as usize);
    for line in out.lines() {
        assert!(line.ends_with(" ."), "not a line-based syntax: {line}");
        assert!(line.starts_with('<') || line.starts_with("_:"), "{line}");
    }
}

// ============================================================================
// --profile
// ============================================================================

#[test]
fn profile_json_carries_the_whole_schema_and_lands_on_stderr() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let out = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .clone();

    // stdout is still the count, uncontaminated.
    assert_eq!(
        String::from_utf8(out.stdout).unwrap().trim(),
        VALID_TURTLE_TRIPLES.to_string()
    );

    let v: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(v["schema"], "fluree.rdf.profile.v1");
    assert_eq!(v["verb"], "count");
    assert!(!v["tool_version"].as_str().unwrap().is_empty());
    assert!(v["host"]["os"].is_string());
    assert!(v["host"]["arch"].is_string());
    assert!(v["host"]["available_parallelism"].as_u64().unwrap() >= 1);
    assert_eq!(v["host"]["threads_used"], 1);
    assert_eq!(v["corpus"]["syntax"], "turtle");
    assert_eq!(v["corpus"]["syntax_source"], "extension");
    assert_eq!(v["corpus"]["compression"], "none");
    assert_eq!(
        v["corpus"]["bytes_decoded"].as_u64().unwrap(),
        VALID_TURTLE.len() as u64
    );
    assert_eq!(
        v["corpus"]["sha256"].as_str().unwrap().len(),
        64,
        "the corpus fingerprint is a full sha256"
    );
    assert!(v["wall_ns"].as_u64().unwrap() > 0);
    assert_eq!(v["counts"]["triples"], VALID_TURTLE_TRIPLES);
    assert!(v["counts"]["grammar_statements"].as_u64().unwrap() > 0);
    assert!(!v["host"]["host_class"].as_str().unwrap().is_empty());
    assert!(!v["git_sha"].as_str().unwrap().is_empty());
    assert!(v["sink"]["below_measurement_floor"].is_boolean());
    assert!(v["phases"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["phase"] == "parse"));
    assert!(v["self_calibration"]["overhead_pct"].is_number());
    assert!(v["self_calibration"]["phases_trusted"].is_boolean());
    assert!(v["self_calibration"]["sink_trusted"].is_boolean());
    assert!(v["self_calibration"]["clock_reads"].as_u64().unwrap() > 0);
}

#[test]
fn profile_json_records_which_rule_resolved_the_syntax() {
    // Traceability: a surprising syntax should be attributable to a rule.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);

    let explicit = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--syntax", "turtle"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&explicit).unwrap();
    assert_eq!(v["corpus"]["syntax_source"], "explicit");

    let sniffed = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json"])
        .write_stdin(VALID_TURTLE)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&sniffed).unwrap();
    assert_eq!(v["corpus"]["syntax_source"], "sniff");
    assert_eq!(v["corpus"]["input"], "<stdin>");
}

#[test]
fn profile_json_reports_the_compression_layer_and_both_byte_counts() {
    let tmp = TempDir::new().unwrap();
    let path = gz_fixture(&tmp, "valid.ttl.gz", VALID_TURTLE);
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    assert_eq!(v["corpus"]["compression"], "gzip");
    assert_eq!(
        v["corpus"]["bytes_decoded"].as_u64().unwrap(),
        VALID_TURTLE.len() as u64
    );
    assert_ne!(
        v["corpus"]["bytes_on_wire"], v["corpus"]["bytes_decoded"],
        "the wire figure is the compressed size"
    );
    assert!(
        v["phases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["phase"] == "decompress"),
        "a compressed run must show a decompress phase"
    );
}

#[test]
fn no_hash_omits_the_fingerprint_entirely() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert!(
        v["corpus"].get("sha256").is_none(),
        "an absent fingerprint must be absent, not null"
    );
}

#[test]
fn the_same_document_fingerprints_identically_plain_and_compressed() {
    // The hash is over the decoded RDF, so storage format does not change the
    // corpus identity — which is what makes a cross-compression comparison
    // legible.
    let tmp = TempDir::new().unwrap();
    let plain = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let gz = gz_fixture(&tmp, "valid.ttl.gz", VALID_TURTLE);

    let hash_of = |path: &Path| -> String {
        let stderr = rdf_cmd()
            .args(["-q", "rdf", "count", "--profile=json"])
            .arg(path)
            .assert()
            .success()
            .get_output()
            .stderr
            .clone();
        let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
        v["corpus"]["sha256"].as_str().unwrap().to_string()
    };

    assert_eq!(hash_of(&plain), hash_of(&gz));
}

#[test]
fn profile_human_prints_a_table_to_stderr() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    rdf_cmd()
        .args(["-q", "rdf", "count", "--profile"])
        .arg(&path)
        .assert()
        .success()
        .stderr(predicate::str::contains("phase"))
        .stderr(predicate::str::contains("% wall"))
        .stderr(predicate::str::contains("profiler cost"))
        .stderr(predicate::str::contains("sink:"));
}

#[test]
fn profile_works_on_check_as_well_as_count() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "check", "--profile=json"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(v["verb"], "check");
}

#[test]
fn the_sink_estimate_declines_to_report_a_number_it_cannot_resolve() {
    // The estimator's headline correction. `count`'s sink is a discard sink
    // that costs less per call than the clock used to measure it, so the
    // honest answer is "below the measurement floor" — and specifically not
    // zero, which would read as "the sink is free".
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", &VALID_TURTLE.repeat(400));
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    assert_eq!(v["sink"]["below_measurement_floor"], true);
    assert!(v["sink"]["body_ns"].is_null(), "{}", v["sink"]["body_ns"]);
    assert!(v["sink"]["artifact_ns"].as_u64().unwrap() > 0);
    assert!(v["sink"]["calls"].as_u64().unwrap() > 1_000);
    assert!(
        !v["phases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["phase"] == "sink"),
        "an unresolved sink must not occupy a phase row"
    );
}

#[test]
fn git_sha_names_the_build_not_whatever_checkout_the_shell_is_in() {
    // The field is "which commit produced this binary". Resolved from the
    // working directory it answered "which commit is the shell sitting on",
    // so running a binary from another checkout stamped the profile with a
    // commit that had nothing to do with it — worse than no field, because a
    // baseline would be attributed to the wrong code.
    let tmp = TempDir::new().unwrap();
    let repo = tmp.path().join("elsewhere");
    std::fs::create_dir(&repo).unwrap();

    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .output()
    };
    if git(&["init"]).map(|o| !o.status.success()).unwrap_or(true) {
        return; // no usable git here; the fallback path is covered by unit tests
    }
    let _ = git(&["config", "user.email", "t@example.com"]);
    let _ = git(&["config", "user.name", "T"]);
    std::fs::write(repo.join("f.txt"), "x").unwrap();
    let _ = git(&["add", "."]);
    if git(&["-c", "commit.gpgsign=false", "commit", "-m", "c"])
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        return;
    }
    let other_head = String::from_utf8(git(&["rev-parse", "--short", "HEAD"]).unwrap().stdout)
        .unwrap()
        .trim()
        .to_string();
    assert!(!other_head.is_empty());

    let corpus = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    let stderr = rdf_cmd()
        .current_dir(&repo)
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&corpus)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    let reported = v["git_sha"].as_str().unwrap();

    assert_ne!(
        reported, other_head,
        "git_sha reported the CWD's HEAD, not the build's"
    );
    assert!(!reported.is_empty());
}

#[test]
fn the_sink_line_is_absent_when_nothing_reached_the_sink() {
    // An empty document forwards no events. A floor computed from zero calls
    // is a statement about nothing, so there is no sink line to print.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "empty.ttl", "");
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(v["sink"]["calls"], 0);
    assert!(v["sink"]["floor_ns_per_call"].is_null());

    let human = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let text = String::from_utf8_lossy(&human);
    assert!(
        !text.contains("sink:"),
        "no sink line for a run with no sink calls:\n{text}"
    );
}

#[test]
fn the_below_floor_bound_is_stated_per_call_not_as_an_aggregate() {
    // "under 82ms" across 720,004 calls is seven orders of magnitude away from
    // what a reader will take it to mean. The bound is ~114ns per call.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", &VALID_TURTLE.repeat(400));
    let out = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("per call"), "{text}");

    let json = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let per_call = v["sink"]["floor_ns_per_call"].as_u64().unwrap();
    let aggregate = v["sink"]["floor_ns"].as_u64().unwrap();
    let calls = v["sink"]["calls"].as_u64().unwrap();
    assert!(
        (1..10_000).contains(&per_call),
        "a per-call floor of {per_call}ns is not a per-call number"
    );
    assert!(
        aggregate > per_call * calls / 2,
        "the aggregate still agrees"
    );
}

#[test]
fn the_trusted_verdict_is_split_so_a_gate_can_key_on_the_phases() {
    // The combined flag went false on essentially every `count`, via the sink
    // artifact, which made it useless for gating the phases that were fine.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", &VALID_TURTLE.repeat(400));
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(
        v["self_calibration"]["phases_trusted"], true,
        "the clock reads actually taken are negligible on any real corpus"
    );
    assert!(v["self_calibration"]["sink_trusted"].is_boolean());
}

#[test]
fn profiling_a_broken_document_still_emits_the_profile() {
    // Why a corpus was slow is worth knowing whether or not it parsed.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);
    let stderr = rdf_cmd()
        .args(["rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .get_output()
        .stderr
        .clone();

    let json_start = stderr
        .iter()
        .position(|b| *b == b'{')
        .expect("a profile document must be present alongside the diagnostic");
    let v: serde_json::Value = serde_json::from_slice(&stderr[json_start..]).unwrap();
    assert_eq!(v["verb"], "count");
    assert_eq!(v["counts"]["triples"], 1, "the counts up to the failure");
}

#[test]
fn profile_written_with_a_space_says_so_instead_of_hunting_for_a_file() {
    rdf_cmd()
        .args(["rdf", "count", "--profile", "json"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("--profile=json"));
}

#[test]
fn empty_input_exits_0_whether_it_is_a_file_or_a_pipe() {
    // These disagreed: an empty `.ttl` parsed to nothing and exited 0, while
    // empty stdin had no syntax to resolve and exited 2. An empty document is
    // the empty graph in every syntax.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "empty.ttl", "");
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .success();

    rdf_cmd()
        .args(["rdf", "check"])
        .write_stdin("")
        .assert()
        .success();

    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).write_stdin("");
    assert_eq!(stdout_of(&mut cmd).trim(), "0");
}

#[test]
fn a_byte_order_mark_does_not_defeat_syntax_detection() {
    // A BOM'd file with no extension used to fail as "could not determine the
    // syntax", pointing at the wrong problem. Identification steps over it;
    // whether the *parser* accepts a BOM is the parser's call, not this one's.
    let tmp = TempDir::new().unwrap();
    let path = fixture(
        &tmp,
        "bom_noext",
        "\u{feff}<http://e/s> <http://e/p> \"o\" .\n",
    );
    let stderr = rdf_cmd()
        .args(["-q", "rdf", "count", "--profile=json", "--no-hash"])
        .arg(&path)
        .assert()
        .get_output()
        .stderr
        .clone();
    let text = String::from_utf8_lossy(&stderr);
    assert!(
        !text.contains("--syntax"),
        "the BOM must not be reported as an unknown syntax: {text}"
    );
}

// ============================================================================
// help surface
// ============================================================================

#[test]
fn rdf_help_lists_every_verb() {
    rdf_cmd()
        .args(["rdf", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("check"))
        .stdout(predicate::str::contains("count"))
        .stdout(predicate::str::contains("convert"));
}

#[test]
fn rdf_appears_in_the_top_level_help() {
    rdf_cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("rdf"));
}

#[test]
fn count_help_lists_every_nameable_syntax_including_the_unbuilt_ones() {
    // Naming a syntax is how a user finds out it is not built yet, so the
    // full set has to be discoverable from help.
    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "count", "--help"]);
    let help = stdout_of(&mut cmd);
    for syntax in [
        "turtle", "ntriples", "nquads", "trig", "jsonld", "rdfxml", "rdfjson", "jelly",
    ] {
        assert!(help.contains(syntax), "help omits {syntax}:\n{help}");
    }
}

#[test]
fn no_rdf_verb_requires_a_fluree_directory() {
    // The whole point of the surface: these read files, not ledgers. Run
    // from a directory with no `.fluree/` anywhere above it that we control,
    // with HOME redirected so the global-config fallback cannot rescue it.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);
    for verb in ["check", "count"] {
        let mut cmd = rdf_cmd();
        cmd.current_dir(tmp.path());
        cmd.env("HOME", tmp.path());
        cmd.env("FLUREE_HOME", tmp.path().join("nowhere"));
        cmd.args(["rdf", verb]).arg(&path).assert().success();
    }
}

// ============================================================================
// convert
// ============================================================================

/// Everything a converter can get wrong in one small document: a typed
/// literal, a language tag, an escaped literal, a collection (which is only
/// representable at all under conformant parser options), an anonymous blank
/// node, and a labelled one.
const HOSTILE_TURTLE: &str = r#"@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" ; ex:age 30 ; ex:knows ex:bob .
ex:bob ex:name "Bob"@en ; ex:tags ( "x" "y" ) ; ex:note "quote \" and \\ and \n newline" .
ex:empty ex:list () .
[] ex:anon "yes" .
_:named ex:label "kept" .
"#;

/// N-Triples lines, sorted.
///
/// A stronger comparison than isomorphism, and deliberately so: blank-node
/// relabelling is a bijection assigned in document order, and the blocks-tier
/// Turtle writer preserves document order, so labels are stable across a
/// round trip. If that ever stops being true this test should say so rather
/// than paper over it with a bnode-blind comparison.
fn canonical_lines(nt: &str) -> Vec<String> {
    let mut lines: Vec<String> = nt
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

fn convert_to_string(path: &Path, to: &str) -> String {
    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "convert"]).arg(path).args(["--to", to]);
    stdout_of(&mut cmd)
}

#[test]
fn every_supported_format_pair_converts() {
    // {ttl, nt} in × {ttl, nt, nq, trig, jsonld} out. Each cell has to
    // produce output in the syntax it names, from a document that exercises
    // literals, language tags, collections and both kinds of blank node.
    let tmp = TempDir::new().unwrap();
    let ttl = fixture(&tmp, "hostile.ttl", HOSTILE_TURTLE);
    let nt = fixture(&tmp, "hostile.nt", &convert_to_string(&ttl, "nt"));

    for input in [&ttl, &nt] {
        for to in ["ttl", "nt", "nq", "trig", "jsonld"] {
            let out = convert_to_string(input, to);
            assert!(
                !out.trim().is_empty(),
                "{} → {to} produced nothing",
                input.display()
            );
            let shaped_right = match to {
                "jsonld" => out.trim_start().starts_with('{'),
                "ttl" | "trig" => out.contains("@prefix") || out.contains('<'),
                _ => out.starts_with('<') || out.starts_with("_:"),
            };
            assert!(shaped_right, "{to} output is not {to}: {out}");
            // Whatever the syntax, the payload survived.
            assert!(out.contains("Alice"), "{to} lost a literal: {out}");
        }
    }
}

#[test]
fn convert_parses_in_conformant_mode_never_the_ingest_default() {
    // MANDATED, and not a preference: under the ingest options a collection
    // arrives as indexed list items, which every writer refuses because an
    // indexed list item is a Fluree storage shape with no RDF serialization.
    // So this pins the observable consequence — the rdf:first/rdf:rest spine
    // W3C says a collection is, and rdf:nil for the empty one.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "lists.ttl", HOSTILE_TURTLE);
    let nt = convert_to_string(&path, "nt");

    assert!(
        nt.contains("22-rdf-syntax-ns#first"),
        "no rdf:first — collections did not arrive as a spine:\n{nt}"
    );
    assert!(nt.contains("22-rdf-syntax-ns#rest"), "{nt}");
    assert!(
        nt.contains("22-rdf-syntax-ns#nil"),
        "no rdf:nil — the empty collection was dropped, which is what the \
         ingest options do:\n{nt}"
    );
    // Three spine triples for a two-item list, plus nil for the empty one.
    let firsts = nt.matches("22-rdf-syntax-ns#first").count();
    assert_eq!(firsts, 2, "one rdf:first per list item:\n{nt}");
}

#[test]
fn a_turtle_round_trip_preserves_the_graph() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "hostile.ttl", HOSTILE_TURTLE);

    let direct = convert_to_string(&path, "nt");
    let via_turtle = fixture(&tmp, "rt.ttl", &convert_to_string(&path, "ttl"));
    let round_tripped = convert_to_string(&via_turtle, "nt");

    assert_eq!(
        canonical_lines(&direct),
        canonical_lines(&round_tripped),
        "ttl → ttl → nt disagrees with ttl → nt"
    );
}

#[test]
fn an_ntriples_round_trip_preserves_the_graph() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "hostile.ttl", HOSTILE_TURTLE);
    let once = fixture(&tmp, "once.nt", &convert_to_string(&path, "nt"));
    let twice = convert_to_string(&once, "nt");

    assert_eq!(
        canonical_lines(&std::fs::read_to_string(&once).unwrap()),
        canonical_lines(&twice),
        "nt → nt is not a fixed point"
    );
}

#[test]
fn the_output_file_extension_picks_the_syntax_and_the_flag_overrides_it() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);

    let by_ext = tmp.path().join("out.nt");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&by_ext)
        .assert()
        .success();
    assert!(std::fs::read_to_string(&by_ext).unwrap().starts_with('<'));

    // --to wins over an extension that says otherwise.
    let overridden = tmp.path().join("out2.nt");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&overridden)
        .args(["--to", "turtle"])
        .assert()
        .success();
    assert!(std::fs::read_to_string(&overridden)
        .unwrap()
        .contains("@prefix"));
}

#[test]
fn convert_reads_stdin_and_a_gzipped_file() {
    let tmp = TempDir::new().unwrap();

    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "convert", "--syntax", "turtle", "--to", "nt"])
        .write_stdin(VALID_TURTLE);
    let from_stdin = stdout_of(&mut cmd);
    assert_eq!(from_stdin.lines().count(), VALID_TURTLE_TRIPLES as usize);

    let gz = gz_fixture(&tmp, "in.ttl.gz", VALID_TURTLE);
    assert_eq!(
        canonical_lines(&convert_to_string(&gz, "nt")),
        canonical_lines(&from_stdin)
    );
}

#[test]
fn a_closed_downstream_pipe_ends_the_run_quietly() {
    // `convert big.ttl | head -1` is a normal way to use the tool. Every
    // layer reports EPIPE and the right answer to all of them is to stop with
    // status 0 and say nothing — riot's behaviour.
    use std::process::{Command as StdCommand, Stdio};

    let tmp = TempDir::new().unwrap();
    // Long enough that the writer is still producing when `head` exits.
    let big: String = std::iter::once("@prefix ex: <http://example.org/> .\n".to_string())
        .chain((0..20_000).map(|i| format!("ex:s{i} ex:name \"person {i}\" .\n")))
        .collect();
    let path = fixture(&tmp, "big.ttl", &big);

    let mut convert = StdCommand::new(assert_cmd::cargo::cargo_bin!("fluree"))
        .args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "nt"])
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let head = StdCommand::new("head")
        .arg("-1")
        .stdin(convert.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .output()
        .unwrap();

    let convert_out = convert.wait_with_output().unwrap();
    assert_eq!(
        convert_out.status.code(),
        Some(0),
        "a closed pipe is not a failure; stderr was: {}",
        String::from_utf8_lossy(&convert_out.stderr)
    );
    assert!(
        convert_out.stderr.is_empty(),
        "and it says nothing on the way out: {}",
        String::from_utf8_lossy(&convert_out.stderr)
    );
    assert_eq!(String::from_utf8(head.stdout).unwrap().lines().count(), 1);
}

#[test]
fn converting_a_broken_document_exits_1_and_says_the_output_is_partial() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "nt"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stderr(predicate::str::contains(
            "before the document stopped parsing",
        ))
        .stderr(predicate::str::contains("prefix of the conversion"));
}

#[test]
fn an_output_syntax_with_no_writer_is_refused_by_name() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "in.ttl", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "rdfxml"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("rdfxml"))
        .stderr(predicate::str::contains("writable today"));
}

#[test]
fn a_compressed_output_name_is_refused_rather_than_written_plain() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "in.ttl", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .arg("-o")
        .arg(tmp.path().join("out.nt.gz"))
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("compressed output"));
}

#[test]
fn pretty_is_refused_rather_than_silently_ignored() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "in.ttl", VALID_TURTLE);
    // Wrong syntax for it: the flag itself is the complaint.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "nt", "--pretty"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("--pretty applies to turtle"));

    // Right syntax, but not built: say so rather than emit blocks-tier output
    // under a flag that promised something else.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "turtle", "--pretty"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("not implemented"));
}

#[test]
fn the_bnode_policy_flag_changes_which_labels_come_out() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "bnodes.ttl", HOSTILE_TURTLE);

    let relabelled = convert_to_string(&path, "nt");
    assert!(
        !relabelled.contains("_:named"),
        "the default relabels user labels:\n{relabelled}"
    );

    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "nt", "--bnode-policy", "preserve"]);
    let preserved = stdout_of(&mut cmd);
    assert!(
        preserved.contains("_:named"),
        "preserve keeps the user's label:\n{preserved}"
    );
}

#[test]
fn supplied_prefixes_compact_turtle_output() {
    let tmp = TempDir::new().unwrap();
    // No @prefix in the input, so any compaction comes from the flag alone.
    let path = fixture(
        &tmp,
        "plain.nt",
        "<http://example.org/s> <http://example.org/p> \"o\" .\n",
    );

    let mut cmd = rdf_cmd();
    cmd.args(["rdf", "convert"]).arg(&path).args([
        "--to",
        "turtle",
        "--prefixes",
        r#"{"ex": "http://example.org/"}"#,
    ]);
    let out = stdout_of(&mut cmd);

    assert!(out.contains("@prefix ex:"), "{out}");
    assert!(out.contains("ex:s"), "{out}");
}

#[test]
fn convert_profile_json_carries_the_serialize_and_write_phases() {
    let tmp = TempDir::new().unwrap();
    // Big enough that the sink estimate clears the measurement floor — the
    // decomposition is derived from it and is absent when it does not.
    let big: String = std::iter::once("@prefix ex: <http://example.org/> .\n".to_string())
        .chain(
            (0..60_000).map(|i| format!("ex:s{i} ex:name \"person {i}\" ; ex:age {} .\n", i % 90)),
        )
        .collect();
    let path = fixture(&tmp, "big.ttl", &big);
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["-q", "rdf", "convert"])
        .arg(&path)
        .arg("-o")
        .arg(&out)
        .args(["--to", "nt", "--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    assert_eq!(v["verb"], "convert");
    let phases: Vec<&str> = v["phases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["phase"].as_str().unwrap())
        .collect();
    assert!(phases.contains(&"write"), "phases: {phases:?}");
    assert!(phases.contains(&"serialize"), "phases: {phases:?}");
    // The decomposition supersedes the total; showing both invites a reader
    // to add the sink to its own parts.
    assert!(
        !phases.contains(&"sink"),
        "the sink row must give way to its decomposition: {phases:?}"
    );
    // …and the total is still reported, in the block that carries it.
    assert!(v["sink"]["body_ns"].as_u64().unwrap() > 0);
}

#[test]
fn convert_time_reports_the_statements_it_wrote() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "in.ttl", VALID_TURTLE);
    let out = tmp.path().join("out.nt");
    rdf_cmd()
        .args(["-q", "rdf", "convert"])
        .arg(&path)
        .arg("-o")
        .arg(&out)
        .arg("--time")
        .assert()
        .success()
        .stderr(predicate::str::contains("triples/s"));
}

#[test]
fn a_conversion_to_a_file_reports_what_it_wrote_and_a_pipe_stays_clean() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "in.ttl", VALID_TURTLE);

    // To a file: a summary is useful.
    let out = tmp.path().join("out.nt");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .arg("-o")
        .arg(&out)
        .assert()
        .success()
        .stderr(predicate::str::contains("statements →"));

    // To stdout: the same line would sit beside the data it describes.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "nt"])
        .assert()
        .success()
        .stderr(predicate::str::is_empty());
}

#[test]
fn convert_needs_no_fluree_directory_either() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "in.ttl", VALID_TURTLE);
    let mut cmd = rdf_cmd();
    cmd.current_dir(tmp.path());
    cmd.env("HOME", tmp.path());
    cmd.env("FLUREE_HOME", tmp.path().join("nowhere"));
    cmd.args(["rdf", "convert"])
        .arg(&path)
        .args(["--to", "nt"])
        .assert()
        .success();
}
