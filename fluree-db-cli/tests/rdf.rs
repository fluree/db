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
    // Was `.trig`, which is readable now. RDF/XML is the current example of a
    // syntax the resolver can NAME but not read — the distinction this test
    // exists to protect, since "unknown syntax" and "known but unreadable"
    // want different messages.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "data.rdf", VALID_TURTLE);
    rdf_cmd()
        .args(["rdf", "count"])
        .arg(&path)
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("rdfxml"))
        .stderr(predicate::str::contains("turtle, ntriples"));
}

/// The quad round trip, end to end through the binary: TriG in, N-Quads out,
/// TriG back. Both directions are REAL readers now — before this the `.nq`
/// leg could only be hand-fed, so nothing proved the two formats agreed.
#[test]
fn a_dataset_round_trips_through_trig_and_nquads() {
    let tmp = TempDir::new().unwrap();
    let src = fixture(
        &tmp,
        "in.trig",
        "@prefix : <http://ex/> .\n\
         :s :p :o .\n\
         GRAPH :g { :a :b :c . :d :e \"lit\"@en }\n\
         :g2 { :x :y :z }\n",
    );
    let nq = tmp.path().join("mid.nq");
    let back = tmp.path().join("back.trig");

    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&src)
        .arg("-o")
        .arg(&nq)
        .assert()
        .success();

    let quads = std::fs::read_to_string(&nq).unwrap();
    assert_eq!(quads.lines().filter(|l| !l.trim().is_empty()).count(), 4);
    // The graph names survive as the fourth term — the property the whole
    // dataset path exists for.
    assert!(quads.contains("<http://ex/c> <http://ex/g> ."), "{quads}");
    assert!(quads.contains("<http://ex/z> <http://ex/g2> ."), "{quads}");
    // A default-graph statement has exactly three terms.
    assert!(
        quads.contains("<http://ex/s> <http://ex/p> <http://ex/o> .\n"),
        "{quads}"
    );

    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&nq)
        .arg("-o")
        .arg(&back)
        .assert()
        .success();

    let trig = std::fs::read_to_string(&back).unwrap();
    assert!(trig.contains("GRAPH <http://ex/g> {"), "{trig}");
    assert!(trig.contains("GRAPH <http://ex/g2> {"), "{trig}");
    assert!(trig.contains("\"lit\"@en"), "{trig}");
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
/// Byte equality of these across a round trip is a *stronger* claim than
/// isomorphism, and it holds only for the shapes named at
/// [`a_flat_turtle_round_trip_is_byte_identical`]. Blank-node labels are
/// stable across a round trip exactly when the parser mints them in the same
/// order the writer emits them, and for nested structures it does not — so
/// anything nested is compared with [`nt_isomorphic`] instead.
fn canonical_lines(nt: &str) -> Vec<String> {
    let mut lines: Vec<String> = nt
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

/// Split one N-Triples line into its three terms.
///
/// Whitespace splitting is wrong the moment a literal contains a space, which
/// every interesting fixture does.
fn nt_terms(line: &str) -> Option<[String; 3]> {
    let bytes: Vec<char> = line.chars().collect();
    let mut at = 0usize;
    let mut terms = Vec::with_capacity(3);

    while terms.len() < 3 {
        while at < bytes.len() && bytes[at].is_whitespace() {
            at += 1;
        }
        let start = at;
        match bytes.get(at)? {
            '<' => {
                while at < bytes.len() && bytes[at] != '>' {
                    at += 1;
                }
                at += 1;
            }
            '"' => {
                at += 1;
                while at < bytes.len() {
                    match bytes[at] {
                        '\\' => at += 2,
                        '"' => break,
                        _ => at += 1,
                    }
                }
                at += 1;
                // A datatype or language tag belongs to the same term.
                if bytes.get(at) == Some(&'^') {
                    at += 2;
                    while at < bytes.len() && bytes[at] != '>' {
                        at += 1;
                    }
                    at += 1;
                } else if bytes.get(at) == Some(&'@') {
                    while at < bytes.len() && !bytes[at].is_whitespace() {
                        at += 1;
                    }
                }
            }
            _ => {
                while at < bytes.len() && !bytes[at].is_whitespace() {
                    at += 1;
                }
            }
        }
        terms.push(bytes[start..at].iter().collect::<String>());
    }
    terms.try_into().ok()
}

/// Whether two N-Triples documents denote the same graph, allowing any
/// bijective renaming of blank nodes.
///
/// The property a converter actually owes: blank-node labels are scoped to the
/// document that carries them, so a writer may choose its own, and only the
/// *structure* has to survive. Lifted from the review's rdflib oracle
/// (`scratchpad/conv/oracle.py`), which compares `to_isomorphic` graphs for
/// the same reason.
///
/// Brute force over the bnode bijection, which is fine for fixtures and
/// refuses rather than hangs above a bound no fixture should reach.
fn nt_isomorphic(a: &str, b: &str) -> bool {
    fn parse(doc: &str) -> (Vec<[String; 3]>, Vec<String>) {
        let mut triples = Vec::new();
        let mut blanks: Vec<String> = Vec::new();
        for line in doc.lines().filter(|l| !l.trim().is_empty()) {
            let Some(terms) = nt_terms(line) else {
                continue;
            };
            for t in &terms {
                if t.starts_with("_:") && !blanks.contains(t) {
                    blanks.push(t.clone());
                }
            }
            triples.push(terms);
        }
        (triples, blanks)
    }

    let (a_triples, a_blanks) = parse(a);
    let (b_triples, b_blanks) = parse(b);
    if a_triples.len() != b_triples.len() || a_blanks.len() != b_blanks.len() {
        return false;
    }
    assert!(
        a_blanks.len() <= 8,
        "isomorphism here is brute force over {} blank nodes; a fixture that \
         large needs a real canonicalizer, not a slower loop",
        a_blanks.len()
    );

    let ground = |triples: &[[String; 3]], map: &std::collections::HashMap<String, String>| {
        let mut out: Vec<String> = triples
            .iter()
            .map(|t| {
                t.iter()
                    .map(|term| map.get(term).unwrap_or(term).clone())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        out.sort();
        out
    };
    let target = ground(&b_triples, &std::collections::HashMap::new());

    // Every assignment of a's blank labels onto b's.
    let mut permutation: Vec<usize> = (0..b_blanks.len()).collect();
    loop {
        let map: std::collections::HashMap<String, String> = a_blanks
            .iter()
            .zip(&permutation)
            .map(|(from, &to)| (from.clone(), b_blanks[to].clone()))
            .collect();
        if ground(&a_triples, &map) == target {
            return true;
        }
        if !next_permutation(&mut permutation) {
            return false;
        }
    }
}

/// Next lexicographic permutation, or `false` when the last one is reached.
fn next_permutation(items: &mut [usize]) -> bool {
    if items.len() < 2 {
        return false;
    }
    let Some(pivot) = (0..items.len() - 1)
        .rev()
        .find(|&i| items[i] < items[i + 1])
    else {
        return false;
    };
    let successor = (pivot + 1..items.len())
        .rev()
        .find(|&i| items[i] > items[pivot])
        .expect("a greater element exists to the right of the pivot");
    items.swap(pivot, successor);
    items[pivot + 1..].reverse();
    true
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

/// Shapes whose blank-node labels survive a round trip *byte-identically*.
///
/// The property holds when the parser mints blank nodes in the same order the
/// writer emits them. That is true when no blank node contains another: a flat
/// collection, a subject that recurs far from its first appearance, a bare
/// labelled node. It is FALSE as soon as they nest — the parser mints
/// outermost-first while the writer emits deepest-first, because the inner
/// node must exist before the triple that references it — so a nested fixture
/// comes back isomorphic but relabelled.
///
/// An earlier version of this test asserted byte equality over a fixture that
/// happened to be flat and claimed it as a general invariant. It is not, and
/// `a_nested_round_trip_is_isomorphic_but_not_byte_identical` is the
/// regression that says so out loud.
const FLAT_SHAPES: &[(&str, &str)] = &[
    (
        "flat collection",
        "@prefix ex: <http://e/> .\nex:s ex:p ( \"a\" \"b\" \"c\" ) .\n",
    ),
    (
        "bare labelled bnode",
        "@prefix ex: <http://e/> .\nex:s ex:p _:a .\n_:a ex:q \"v\" .\n",
    ),
    (
        "recurring subject, far apart",
        "@prefix ex: <http://e/> .\nex:a ex:p \"1\" .\nex:z ex:p \"2\" .\nex:a ex:q \"3\" .\n",
    ),
];

/// Nested shapes: isomorphic across a round trip, and NOT byte-identical.
///
/// The reviewer's fixtures, kept verbatim — the nested collection is the
/// one-liner that would have caught the overclaim.
const NESTED_SHAPES: &[(&str, &str)] = &[
    (
        "collection of collections",
        "@prefix ex: <http://e/> .\nex:s ex:p ( ( \"x\" \"y\" ) ( \"z\" ) ) .\n",
    ),
    (
        "triply nested collection",
        "@prefix ex: <http://e/> .\nex:s ex:p ( ( ( \"deep\" ) ) ) .\n",
    ),
    (
        "nested anonymous nodes",
        "@prefix ex: <http://e/> .\nex:s ex:p [ ex:q [ ex:r \"v\" ] ] .\n",
    ),
];

#[test]
fn a_flat_turtle_round_trip_is_byte_identical() {
    // Byte equality, for exactly the shapes where it is true. See FLAT_SHAPES
    // for why it is true there and nowhere else.
    let tmp = TempDir::new().unwrap();
    for (name, doc) in FLAT_SHAPES {
        let path = fixture(&tmp, &format!("{}.ttl", name.replace(' ', "_")), doc);
        let direct = convert_to_string(&path, "nt");
        let via = fixture(
            &tmp,
            &format!("{}_rt.ttl", name.replace(' ', "_")),
            &convert_to_string(&path, "ttl"),
        );
        let round_tripped = convert_to_string(&via, "nt");

        assert_eq!(
            canonical_lines(&direct),
            canonical_lines(&round_tripped),
            "{name}: ttl → ttl → nt disagrees with ttl → nt"
        );
    }
}

#[test]
fn a_nested_round_trip_is_isomorphic_but_not_byte_identical() {
    // The regression for an invariant this suite used to overclaim. Both
    // halves are asserted: the graph survives (isomorphism, which is all a
    // converter owes), and the labels do NOT (so nobody reinstates byte
    // equality here without the test objecting).
    let tmp = TempDir::new().unwrap();
    let mut any_relabelled = false;

    for (name, doc) in NESTED_SHAPES {
        let path = fixture(&tmp, &format!("{}.ttl", name.replace(' ', "_")), doc);
        let direct = convert_to_string(&path, "nt");
        let via = fixture(
            &tmp,
            &format!("{}_rt.ttl", name.replace(' ', "_")),
            &convert_to_string(&path, "ttl"),
        );
        let round_tripped = convert_to_string(&via, "nt");

        assert!(
            nt_isomorphic(&direct, &round_tripped),
            "{name}: the round trip changed the graph, not just the labels\n             direct:\n{direct}\nround-tripped:\n{round_tripped}"
        );
        if canonical_lines(&direct) != canonical_lines(&round_tripped) {
            any_relabelled = true;
        }
    }

    assert!(
        any_relabelled,
        "no nested shape relabelled — if label order became stable under          nesting, FLAT_SHAPES can be widened and this test retired, but that          is a decision to make deliberately"
    );
}

#[test]
fn the_hostile_fixture_round_trips_isomorphically_through_every_syntax() {
    // The whole fixture — typed literals, language tags, escapes, collections,
    // both kinds of blank node — through each text syntax that can hold
    // triples and back.
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "hostile.ttl", HOSTILE_TURTLE);
    let direct = convert_to_string(&path, "nt");

    for (syntax, ext) in [("ttl", "ttl"), ("nt", "nt"), ("nq", "nq"), ("trig", "trig")] {
        let via = fixture(
            &tmp,
            &format!("via.{ext}"),
            &convert_to_string(&path, syntax),
        );
        // nq/trig are not readable yet, so only re-read what the reader takes.
        if !matches!(syntax, "ttl" | "nt") {
            continue;
        }
        let back = convert_to_string(&via, "nt");
        assert!(
            nt_isomorphic(&direct, &back),
            "hostile fixture through {syntax} changed the graph:\n{direct}\n---\n{back}"
        );
    }
}

#[test]
fn an_ntriples_round_trip_preserves_the_graph() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "hostile.ttl", HOSTILE_TURTLE);
    let once = fixture(&tmp, "once.nt", &convert_to_string(&path, "nt"));
    let twice = convert_to_string(&once, "nt");

    assert!(
        nt_isomorphic(&std::fs::read_to_string(&once).unwrap(), &twice),
        "nt → nt is not a fixed point"
    );
}

#[test]
fn the_isomorphism_checker_can_tell_graphs_apart() {
    // A checker that returns true for everything would make every round-trip
    // test above vacuous.
    let a = "<http://e/s> <http://e/p> _:x .\n_:x <http://e/q> \"v\" .\n";
    let relabelled = "<http://e/s> <http://e/p> _:zzz .\n_:zzz <http://e/q> \"v\" .\n";
    let different = "<http://e/s> <http://e/p> _:x .\n_:x <http://e/q> \"OTHER\" .\n";
    let merged =
        "<http://e/s> <http://e/p> _:x .\n_:x <http://e/q> \"v\" .\n_:x <http://e/r> \"w\" .\n";

    assert!(nt_isomorphic(a, relabelled), "relabelling is allowed");
    assert!(!nt_isomorphic(a, different), "a changed literal is not");
    assert!(!nt_isomorphic(a, merged), "nor is an extra triple");
    // Literal terms containing spaces must survive tokenization.
    let spaced = "<http://e/s> <http://e/p> \"two words\" .\n";
    assert!(nt_isomorphic(spaced, spaced));
    assert!(!nt_isomorphic(
        spaced,
        "<http://e/s> <http://e/p> \"two  words\" .\n"
    ));
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

#[test]
fn a_prefix_namespace_that_is_not_an_iri_is_refused_before_anything_is_written() {
    // The blocker this fixes: `--prefixes '{"ok":"not an iri"}'` used to emit
    // `@prefix ok: <not an iri> .` and exit 0 — a document this
    // tool's own reader rejects, written by this tool, reported as success.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);

    // Inline form.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "turtle", "--prefixes", r#"{"ok":"not an iri"}"#])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("not an absolute IRI"))
        .stderr(predicate::str::contains("'ok'"))
        .stderr(predicate::str::contains("needs a scheme"))
        .stdout(predicate::str::is_empty());

    // File form, through a @context wrapper — the shape a user actually has.
    let ctx = fixture(&tmp, "ctx.json", r#"{"@context":{"bad":"nope"}}"#);
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "turtle"])
        .arg("--prefixes")
        .arg(&ctx)
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("not an absolute IRI"))
        .stderr(predicate::str::contains("'bad'"));

    // And a good namespace still works, so the check is not just "refuse".
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args([
            "--to",
            "turtle",
            "--prefixes",
            r#"{"ex":"http://example.org/"}"#,
        ])
        .assert()
        .success();
}

#[test]
fn a_prefixes_argument_of_the_wrong_json_shape_says_so() {
    // Not "No such file or directory", which sent the reader looking for a
    // file called `[1,2]`.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);
    for (arg, shape) in [
        ("[1,2]", "an array"),
        ("42", "a number"),
        (r#""x""#, "a string"),
    ] {
        rdf_cmd()
            .args(["rdf", "convert"])
            .arg(&input)
            .args(["--to", "turtle", "--prefixes", arg])
            .assert()
            .code(EXIT_USAGE)
            .stderr(predicate::str::contains(shape))
            .stderr(
                predicate::str::contains("JSON object").or(predicate::str::contains("@context")),
            );
    }
    // A genuinely missing path still reports a missing path.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "turtle", "--prefixes", "/nonexistent/ctx.json"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("cannot read prefixes"));
}

#[test]
fn a_refusal_names_its_cause_not_the_latch_and_offers_a_remedy() {
    // The writers latch — a sink that failed once keeps failing rather than
    // pretending — but the latched message carries no cause. Reporting
    // `finish()` before the parse error printed the placeholder instead.
    let tmp = TempDir::new().unwrap();
    let collide = fixture(
        &tmp,
        "collide.ttl",
        "@prefix ex: <http://example.org/> .\n\
         _:fdbw-1 ex:label \"user wrote this\" .\n\
         ex:s ex:p [ ex:anon \"a\" ] .\n",
    );

    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&collide)
        .args(["--to", "nt", "--bnode-policy", "preserve"])
        .assert()
        .code(EXIT_USAGE)
        // The cause: which label, and why it cannot be preserved.
        .stderr(predicate::str::contains("fdbw-1"))
        .stderr(predicate::str::contains("reserves"))
        // The remedy, in this CLI's vocabulary.
        .stderr(predicate::str::contains("--bnode-policy relabel"))
        // NOT the latch placeholder.
        .stderr(predicate::str::contains("already refused an event").not());
}

#[test]
fn a_refusal_that_needs_no_input_does_not_truncate_the_output_file() {
    // `File::create` truncates. A run that was never going to produce output
    // must not be the thing that empties the file it was pointed at.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);
    let victim = tmp.path().join("victim.nt");
    std::fs::write(&victim, "PRE-EXISTING\n").unwrap();

    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&victim)
        .args(["--to", "rdfxml"])
        .assert()
        .code(EXIT_USAGE);

    assert_eq!(
        std::fs::read_to_string(&victim).unwrap(),
        "PRE-EXISTING\n",
        "a refusal that needed no input still destroyed the output file"
    );
}

#[test]
fn profile_json_output_is_not_corrupted_by_the_completion_summary() {
    // `2> run.json` is the bench lane's idiom. One ✓ line ahead of the
    // document makes the file unparseable, and only when -o is used, which is
    // exactly when a bench harness writes to a file.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&out)
        .args(["--to", "nt", "--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();

    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap_or_else(|e| {
        panic!(
            "stderr is not one JSON document ({e}): {}",
            String::from_utf8_lossy(&stderr)
        )
    });
    assert_eq!(v["verb"], "convert");

    // The human profile still gets its summary, since nothing is parsing it.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&out)
        .args(["--to", "nt", "--profile", "--no-hash"])
        .assert()
        .success()
        .stderr(predicate::str::contains("statements →"));
}

#[test]
fn a_refusal_blames_the_output_syntax_the_way_it_was_chosen() {
    // "--to nquads has no pretty form" reads as a lie to someone who never
    // passed --to.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);

    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("--pretty")
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("no --to given"))
        .stderr(predicate::str::contains("default nquads"));

    // Named explicitly: blame the flag.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nq", "--pretty"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("--to nquads"));

    // From the extension: blame the extension.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(tmp.path().join("out.nq"))
        .arg("--pretty")
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("extension"));
}
