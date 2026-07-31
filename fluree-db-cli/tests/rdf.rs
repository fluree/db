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
/// Bound to the CLI's own contract rather than restated as a literal: these
/// tests assert the documented "the document is bad" code, so if
/// `EXIT_ERROR` ever moves they move with it instead of quietly checking a
/// number the binary no longer returns.
const EXIT_DOCUMENT_INVALID: i32 = fluree_db_cli::error::EXIT_ERROR;
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
///
/// Refuses a name that differs from one already in `dir` only by case. On APFS
/// and NTFS `base.ttl` and `BASE.ttl` are the same file, so a matrix over
/// `@base`/`@BASE` that names its fixtures after the spelling tests the last
/// one twice and the first one never — and nothing fails, because the fixture
/// silently overwrote its sibling. An exact repeat is left alone: rewriting one
/// fixture is a thing tests do deliberately.
fn fixture(dir: &TempDir, name: &str, content: &str) -> PathBuf {
    for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
        let existing = entry.file_name().to_string_lossy().into_owned();
        assert!(
            existing == name || !existing.eq_ignore_ascii_case(name),
            "fixture {name:?} differs from {existing:?} only by case, and on a \
             case-insensitive filesystem they are one file"
        );
    }
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

/// A malformed `\u` escape must be a parse ERROR, never a panic.
///
/// The scanner sliced the 4/8-byte hex window by byte index after checking
/// only that the bytes existed. A multi-byte character straddling that window
/// is not a char boundary, so the slice panicked and took the process with it
/// — exit 101 out of `fluree rdf check`, on input a user can trivially have.
/// Both escape positions (literal and IRI) went through the same scanner.
#[test]
fn a_multibyte_char_in_an_escape_window_is_an_error_not_a_panic() {
    let tmp = TempDir::new().unwrap();
    let cases = [
        // Two-byte chars inside a \u window, in a literal.
        ("lit2.nq", "<http://e/s> <http://e/p> \"\\u0éé\" .\n"),
        // Same, in IRI position.
        ("iri2.nq", "<http://e/\\u0éé> <http://e/p> <http://e/o> .\n"),
        // A 4-byte char, which straddles differently.
        ("lit4.nq", "<http://e/s> <http://e/p> \"\\u0😀\" .\n"),
        // \U has an 8-byte window with the same hazard.
        ("big.nq", "<http://e/s> <http://e/p> \"\\U000000éé\" .\n"),
        // Truncated at EOF.
        ("trunc.nq", "<http://e/s> <http://e/p> \"\\u00\" .\n"),
    ];

    for (name, body) in cases {
        let path = fixture(&tmp, name, body);
        // EXIT_DOCUMENT_INVALID (a malformed document is a data error, not
        // a usage error), and specifically NOT 101. Asserting an exact code is what
        // makes this a panic test rather than a "did not succeed" test —
        // a panic also fails `success()`, so that weaker assertion would
        // have passed against the bug.
        rdf_cmd()
            .args(["rdf", "check"])
            .arg(&path)
            .assert()
            .code(EXIT_DOCUMENT_INVALID);
        rdf_cmd()
            .args(["rdf", "convert"])
            .arg(&path)
            .arg("--syntax")
            .arg("nquads")
            .arg("-o")
            .arg(tmp.path().join("out.nq"))
            .assert()
            .code(EXIT_DOCUMENT_INVALID);
    }
}

/// `u32::from_str_radix` accepts a leading `+`, so `\u+041` decoded as `A`.
/// The Turtle lexer never had this because it gates on hex digits while
/// scanning; the strict reader sliced first and converted second, and
/// inherited it. The gate now lives in the shared decoder, so neither reader
/// can drift back.
#[test]
fn a_signed_hex_payload_is_refused_not_decoded() {
    let tmp = TempDir::new().unwrap();
    for (name, body) in [
        ("plus.nq", "<http://e/s> <http://e/p> \"\\u+041\" .\n"),
        ("minus.nq", "<http://e/s> <http://e/p> \"\\u-041\" .\n"),
    ] {
        let path = fixture(&tmp, name, body);
        rdf_cmd()
            .args(["rdf", "check"])
            .arg(&path)
            .assert()
            .code(EXIT_DOCUMENT_INVALID);
    }
}

/// The fixes must not have cost the escapes that ARE valid, including a
/// 4-byte scalar via `\U`.
#[test]
fn well_formed_escapes_still_decode() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(
        &tmp,
        "ok.nq",
        "<http://e/s> <http://e/p> \"\\u0041\\U0001F600\" .\n",
    );
    let out = tmp.path().join("ok.out.nq");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&path)
        .arg("-o")
        .arg(&out)
        .assert()
        .success();
    let written = std::fs::read_to_string(&out).unwrap();
    assert!(written.contains("\"A😀\""), "{written}");
}

/// The writers fold only CONSECUTIVE same-subject runs ("blocks" tier), so a
/// subject that recurs non-consecutively is written as a SECOND block for the
/// same subject. That output has to read back — and until there were real
/// quad readers, nothing checked that the writers and readers agreed on it
/// (the L-8 gap, closing from both sides here).
#[test]
fn repeated_subject_blocks_round_trip_through_the_new_readers() {
    let tmp = TempDir::new().unwrap();
    // `:a` recurs non-consecutively in BOTH the default graph and the named
    // one, which is what produces repeated blocks on the way out.
    let src = fixture(
        &tmp,
        "rep.trig",
        "@prefix : <http://ex/> .\n\
         :a :p 1 .\n\
         :b :q 2 .\n\
         :a :r 3 .\n\
         GRAPH :g { :a :s 4 . :b :t 5 . :a :u 6 }\n",
    );

    let written = tmp.path().join("written.trig");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&src)
        .arg("-o")
        .arg(&written)
        .assert()
        .success();

    // The writer really did emit `:a` twice per graph, or this test is
    // exercising nothing.
    let trig = std::fs::read_to_string(&written).unwrap();
    assert!(
        trig.matches("\n:a\n").count() >= 2,
        "expected repeated default-graph blocks for :a, got:\n{trig}"
    );

    // Now read THAT back through the TriG reader and out as N-Quads.
    let quads = tmp.path().join("rep.nq");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&written)
        .arg("-o")
        .arg(&quads)
        .assert()
        .success();

    let nq = std::fs::read_to_string(&quads).unwrap();
    let mut lines: Vec<&str> = nq.lines().filter(|l| !l.trim().is_empty()).collect();
    lines.sort_unstable();
    assert_eq!(lines.len(), 6, "every statement must survive:\n{nq}");

    // Both `:a` blocks survived, in the right graphs.
    assert!(nq.contains("<http://ex/a> <http://ex/p> \"1\"^^"), "{nq}");
    assert!(nq.contains("<http://ex/a> <http://ex/r> \"3\"^^"), "{nq}");
    assert!(
        nq.contains("<http://ex/a> <http://ex/u> \"6\"^^<http://www.w3.org/2001/XMLSchema#integer> <http://ex/g> ."),
        "the second named-graph block for :a must keep its graph:\n{nq}"
    );

    // And back to TriG through the N-Quads reader: a fixpoint in CONTENT.
    // Not in bytes — the first TriG carried a prefix from the source and the
    // N-Quads intermediate has none, so the second writes full IRIs. Prefixes
    // are presentation, not statements.
    let again = tmp.path().join("again.trig");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&quads)
        .arg("-o")
        .arg(&again)
        .assert()
        .success();

    let back = tmp.path().join("back.nq");
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&again)
        .arg("-o")
        .arg(&back)
        .assert()
        .success();

    let mut round2: Vec<String> = std::fs::read_to_string(&back)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect();
    round2.sort();
    let round1: Vec<String> = lines.iter().map(|l| (*l).to_string()).collect();
    assert_eq!(
        round1, round2,
        "trig -> nq -> trig -> nq must be a fixpoint"
    );
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

    // And the flag has to survive the parallel path, which is where it did not:
    // workers rename every blank node into the coordination-free scheme, so
    // `_:named` came out as `_:unamed` — byte-identical to relabel, silently
    // ignoring the flag. `preserve` now downgrades to serial, so the output is
    // the serial output, byte for byte, at every width.
    let big = fixture(
        &tmp,
        "over-threshold-blanks.ttl",
        &over_threshold_blanks("_:named ex:label \"kept\" .\n[] ex:anon \"yes\" .\n"),
    );
    let serially = convert_preserving(&big, "1");
    assert!(
        serially.contains("_:named"),
        "the over-threshold fixture lost the label before parallelism entered it"
    );
    for threads in ["4", "8"] {
        assert_eq!(
            convert_preserving(&big, threads),
            serially,
            "--parallelism {threads} --bnode-policy preserve must match serial preserve"
        );
    }
}

/// A document carrying `head`'s blank nodes, over `MIN_PARALLEL_BYTES` so the
/// parallel path actually engages.
fn over_threshold_blanks(head: &str) -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    ttl.push_str(head);
    for i in 0..120_000 {
        ttl.push_str(&format!(
            "ex:s{i} ex:name \"person {i}\" ; ex:age {} .\n",
            i % 90
        ));
    }
    assert!(
        ttl.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate: \
         the parallel path would not engage and the test would prove nothing",
        ttl.len()
    );
    ttl
}

fn convert_preserving(path: &Path, threads: &str) -> String {
    let mut cmd = rdf_cmd();
    cmd.args(["--parallelism", threads, "rdf", "convert"])
        .arg(path)
        .args(["--to", "nt", "--bnode-policy", "preserve"]);
    stdout_of(&mut cmd)
}

#[test]
fn preserving_labels_says_so_in_the_profile() {
    // A silent downgrade is the defect the reason field exists for: the user
    // asked for eight workers and got one, and only the profile can say why.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "profiled-blanks.ttl", &over_threshold_blanks(""));
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&out)
        .args([
            "--to",
            "nt",
            "--bnode-policy",
            "preserve",
            "--profile=json",
            "--no-hash",
        ])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();

    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(
        v["host"]["threads_used"], 1,
        "reason: {}",
        v["host"]["parallel_reason"]
    );
    assert!(
        v["host"]["parallel_reason"]
            .as_str()
            .unwrap()
            .contains("preserve"),
        "{}",
        v["host"]["parallel_reason"]
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

    // The same refusal must survive `--parallelism 8`. It did not: the parallel
    // path renamed `_:fdbw-1` out of the writer's reserved namespace before the
    // writer ever saw it, so the collision vanished and the run exited 0 with a
    // label the user never wrote. `preserve` downgrading to serial is what puts
    // the refusal back.
    let big = fixture(
        &tmp,
        "over-threshold-collide.ttl",
        &over_threshold_blanks(
            "_:fdbw-1 ex:label \"user wrote this\" .\nex:s ex:p [ ex:anon \"a\" ] .\n",
        ),
    );
    rdf_cmd()
        .args(["--parallelism", "8", "rdf", "convert"])
        .arg(&big)
        .args(["--to", "nt", "--bnode-policy", "preserve"])
        .assert()
        .code(EXIT_USAGE)
        .stderr(predicate::str::contains("fdbw-1"))
        .stderr(predicate::str::contains("--bnode-policy relabel"));
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

// =============================================================================
// Term validation and `--nocheck` (H-8)
// =============================================================================

/// A document whose terms are not RDF terms: it LEXES — ` ` is legal
/// source — and denotes an IRI containing a space. `turtle-eval-bad-01`.
const BAD_TERM_TURTLE: &str = concat!(
    "<http://example.org/s> <http://example.org/p> \"ok\" .\n",
    "<http://example.org/\\u0020> <http://example.org/p> <http://example.org/o> .\n"
);

/// `"string"@1` — a language tag that is not one. `turtle-syntax-bad-lang-01`.
const BAD_LANG_TURTLE: &str = "<http://example.org/s> <http://example.org/p> \"string\"@1 .\n";

#[test]
fn check_rejects_a_document_whose_terms_are_not_terms() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "bad-term.ttl", BAD_TERM_TURTLE);
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        // Located like any other diagnostic: the statement is on line 2.
        .stderr(predicate::str::contains("bad-term.ttl:2:1"))
        .stderr(predicate::str::contains("not allowed in an IRI"))
        .stderr(predicate::str::contains("^"));
}

#[test]
fn check_rejects_a_malformed_language_tag() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "bad-lang.ttl", BAD_LANG_TURTLE);
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stderr(predicate::str::contains("must be letters"));
}

/// `--nocheck` is the disclosed fast path: the same document passes, because
/// the grammar was never the problem.
#[test]
fn nocheck_accepts_what_validation_rejects() {
    let tmp = TempDir::new().unwrap();
    for (name, content) in [("t.ttl", BAD_TERM_TURTLE), ("l.ttl", BAD_LANG_TURTLE)] {
        let path = fixture(&tmp, name, content);
        rdf_cmd()
            .args(["rdf", "check", "--nocheck"])
            .arg(&path)
            .assert()
            .success()
            .stderr(predicate::str::contains("no syntax errors"));
    }
}

/// `--nocheck` must not become a way to launder a syntax error into a pass.
/// It turns off term validation only; the grammar is still the grammar.
#[test]
fn nocheck_does_not_disable_syntax_checking() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "broken.ttl", BROKEN_TURTLE);
    rdf_cmd()
        .args(["rdf", "check", "--nocheck"])
        .arg(&path)
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stderr(predicate::str::contains("broken.ttl:3:16"));
}

#[test]
fn count_validates_by_default_and_nocheck_opts_out() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "bad-term.ttl", BAD_TERM_TURTLE);
    rdf_cmd()
        .args(["rdf", "count"])
        .arg(&path)
        .assert()
        .failure();
    rdf_cmd()
        .args(["rdf", "count", "--nocheck"])
        .arg(&path)
        .assert()
        .success();
}

/// A `--nocheck` measurement is not comparable with a validating tool's, so
/// the profile says which it was. An unlabelled number is a faster answer to
/// an easier question.
#[test]
fn the_profile_reports_whether_terms_were_validated() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "valid.ttl", VALID_TURTLE);

    let out = rdf_cmd()
        .args(["rdf", "count", "--profile=json"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["validated"], serde_json::json!(true));

    let out = rdf_cmd()
        .args(["rdf", "count", "--profile=json", "--nocheck"])
        .arg(&path)
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["validated"], serde_json::json!(false));
}

/// Two statements whose language tags are not language tags. Small, because
/// the site that reads it is serial whatever the document's size.
const BAD_LANG_PAIR: &str = "@prefix ex: <http://example.org/> .\n\
                             ex:a ex:p \"one\"@1 .\n\
                             ex:b ex:p \"two\"@1 .\n";

/// The same defect, past the parallel gate.
///
/// Every statement lexes and parses; only term validation rejects them, which
/// is exactly what `--nocheck` turns off. Sized past the gate on purpose: the
/// flag was honoured on the serial path and hardcoded away everywhere else, so
/// a fixture under the gate would have exercised the one site that worked.
fn bad_langtag_corpus() -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..60_000 {
        ttl.push_str(&format!(
            "ex:s{i} ex:p \"a literal wide enough to reach the parallel threshold {i}\"@1 .\n"
        ));
    }
    assert!(
        ttl.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate — it would \
         convert serially and prove nothing about the parallel site",
        ttl.len()
    );
    ttl
}

/// The three places `convert` can reach a parser. `--nocheck` reached one.
#[derive(Copy, Clone)]
enum ParseSite {
    /// `rdf::parse_into`, which honoured the flag all along.
    Serial,
    /// `write_chunk`, once per worker.
    Parallel,
    /// `parse_recovering`, once per resync.
    Recovery,
}

impl ParseSite {
    const ALL: [Self; 3] = [Self::Serial, Self::Parallel, Self::Recovery];

    fn name(self) -> &'static str {
        match self {
            Self::Serial => "--parallelism 1",
            Self::Parallel => "--parallelism 8",
            Self::Recovery => "--continue-on-error",
        }
    }

    fn command(self, input: &Path, nocheck: bool) -> Command {
        let mut cmd = rdf_cmd();
        match self {
            Self::Serial => cmd.args(["--parallelism", "1"]),
            Self::Parallel => cmd.args(["--parallelism", "8"]),
            Self::Recovery => &mut cmd,
        };
        cmd.args(["rdf", "convert"]).arg(input).args(["--to", "nt"]);
        if matches!(self, Self::Recovery) {
            cmd.arg("--continue-on-error");
        }
        if nocheck {
            cmd.arg("--nocheck");
        }
        cmd
    }
}

#[test]
fn nocheck_reaches_every_parse_site() {
    // `--nocheck` was threaded into `rdf::parse_into` and built from nothing
    // at the other three sites, so above the 4 MiB gate the same document
    // exited 0 at `--parallelism 1` and 1 at `--parallelism 8`. The flag is
    // documented never to be a correctness decision, and that is one.
    let tmp = TempDir::new().unwrap();
    let big = fixture(&tmp, "bad-lang-big.ttl", &bad_langtag_corpus());
    let small = fixture(&tmp, "bad-lang-small.ttl", BAD_LANG_PAIR);

    for site in ParseSite::ALL {
        // Recovery is serial by construction, so its site needs no bulk — and
        // without `--nocheck` it resyncs once per bad statement, which on the
        // big fixture would be 60,000 re-parses.
        let input = match site {
            ParseSite::Recovery => &small,
            _ => &big,
        };
        site.command(input, true)
            .assert()
            .success()
            .stderr(predicate::str::contains("skipped").not());
        site.command(input, false)
            .assert()
            .code(EXIT_DOCUMENT_INVALID);
        let _ = site.name();
    }
}

#[test]
fn nocheck_converts_identically_at_every_width() {
    // Agreeing on the exit code is not enough. `--nocheck` turns off a check,
    // not a parser, so the two widths must produce the same document — the
    // failure this guards against is a parallel path that accepts the file by
    // reading it with different options rather than the same ones.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "bad-lang.ttl", &bad_langtag_corpus());

    let mut produced: Vec<Vec<u8>> = Vec::new();
    for threads in ["1", "8"] {
        let out = tmp.path().join(format!("out{threads}.nt"));
        rdf_cmd()
            .args(["--parallelism", threads, "-q", "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt", "--nocheck"])
            .arg("-o")
            .arg(&out)
            .assert()
            .success();
        produced.push(std::fs::read(&out).unwrap());
    }
    // Compared by hand rather than with assert_eq!, which would print two
    // twenty-megabyte vectors on failure.
    assert!(
        produced[0] == produced[1],
        "--nocheck produced different documents at 1 and 8 workers ({} vs {} bytes)",
        produced[0].len(),
        produced[1].len()
    );
}

/// A BOM-prefixed document is ordinary input, not an error. Windows editors
/// emit them and riot eats them.
#[test]
fn a_byte_order_mark_does_not_break_the_verbs() {
    let tmp = TempDir::new().unwrap();
    let path = fixture(&tmp, "bom.ttl", &format!("\u{FEFF}{VALID_TURTLE}"));
    rdf_cmd()
        .args(["rdf", "check"])
        .arg(&path)
        .assert()
        .success()
        .stderr(predicate::str::contains("no syntax errors"));
}

// ============================================================================
// parallel convert
// ============================================================================

/// A corpus big enough to clear the parallel threshold and split into chunks.
fn parallel_corpus(statements: usize) -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..statements {
        ttl.push_str(&format!(
            "ex:s{i} ex:name \"person {i}\" ; ex:age {} ; ex:note \"a longer literal to make the corpus wide enough to chunk {i}\" .\n",
            i % 90
        ));
    }
    ttl
}

/// An above-threshold `.nt` document that is valid TURTLE and invalid
/// N-Triples, in the way `body` chooses.
fn above_threshold_nt(head: &str, body: impl Fn(usize) -> String) -> String {
    let mut nt = String::from(head);
    for i in 0..100_000 {
        nt.push_str(&body(i));
    }
    assert!(
        nt.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate — it would \
         convert serially and prove nothing about the parallel reader",
        nt.len()
    );
    nt
}

#[test]
fn a_line_format_is_read_strictly_at_every_parallelism() {
    // The strict N-Triples reader exists to reject what Turtle accepts —
    // directives, prefixed names, bare numbers — because that is what every
    // other tool in the field does. The parallel path parsed every chunk with
    // the TURTLE parser whatever the input was, so those documents were
    // refused at `--parallelism 1` and accepted on the default path, which is
    // the path any real-sized `.nt` file takes.
    let tmp = TempDir::new().unwrap();

    let cases: [(&str, String); 2] = [
        // A directive at the top: the chunker lifts it as a header and hands
        // it to every worker, so the prefixed names below resolve — as Turtle.
        (
            "prefixed names under a directive",
            above_threshold_nt("@prefix ex: <http://example.org/> .\n", |i| {
                format!("ex:s{i} ex:p \"a literal wide enough to reach the threshold {i}\" .\n")
            }),
        ),
        // No directive anywhere, so nothing about the header is involved: a
        // bare number is simply not an N-Triples term.
        (
            "bare numbers",
            above_threshold_nt("", |i| {
                format!(
                    "<http://example.org/s{i}> <http://example.org/p> {i} . \
                     <http://example.org/s{i}> <http://example.org/q> \"padding to widen the corpus\" .\n"
                )
            }),
        ),
    ];

    for (name, doc) in &cases {
        let input = fixture(&tmp, &format!("{}.nt", name.replace(' ', "_")), doc);
        for threads in ["1", "8"] {
            rdf_cmd()
                .args(["--parallelism", threads, "rdf", "convert"])
                .arg(&input)
                .args(["--to", "nt"])
                .assert()
                .code(EXIT_DOCUMENT_INVALID)
                .stderr(predicate::str::contains("--parallelism").not());
            let _ = name;
        }
    }
}

#[test]
fn an_input_that_cannot_be_cut_converts_serially() {
    // TriG writes fine as concatenated fragments, which is what the OUTPUT
    // check answers — but its statements live inside `GRAPH … { … }` blocks
    // and the boundary scanner cuts at `.`, so cutting the INPUT lands inside
    // a brace-scoped block. Only the output syntax was consulted, so this was
    // chunked as though it were Turtle.
    let tmp = TempDir::new().unwrap();
    let mut trig = String::from("@prefix ex: <http://example.org/> .\n");
    for g in 0..4_000 {
        trig.push_str(&format!("ex:g{g} {{\n"));
        for i in 0..20 {
            trig.push_str(&format!(
                "  ex:s{g}_{i} ex:p \"a literal wide enough to reach the threshold {g} {i}\" .\n"
            ));
        }
        trig.push_str("}\n");
    }
    assert!(trig.len() > MIN_PARALLEL_BYTES, "{}", trig.len());
    let input = fixture(&tmp, "graphs.trig", &trig);
    let out = tmp.path().join("out.nq");

    let stderr = rdf_cmd()
        .args(["--parallelism", "8", "-q", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nq"])
        .arg("-o")
        .arg(&out)
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();

    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(
        v["host"]["threads_used"], 1,
        "reason: {}",
        v["host"]["parallel_reason"]
    );
    assert!(
        v["host"]["parallel_reason"]
            .as_str()
            .unwrap()
            .contains("cannot be cut"),
        "{}",
        v["host"]["parallel_reason"]
    );
    // And the conversion is whole: 4_000 graphs x 20 statements.
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written.lines().count(), 80_000);
}

#[test]
fn parallelism_does_not_change_the_output_bytes() {
    // The gate the whole parallel design is built around. A user must be able
    // to change --parallelism without re-verifying their pipeline.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &parallel_corpus(30_000));

    let convert_with = |threads: &str, out: &str| -> Vec<u8> {
        let path = tmp.path().join(out);
        rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt"])
            .arg("-o")
            .arg(&path)
            .assert()
            .success();
        std::fs::read(&path).unwrap()
    };

    let serial = convert_with("1", "serial.nt");
    assert!(
        serial.len() > 4 * 1024 * 1024,
        "corpus must clear the threshold"
    );
    for threads in ["2", "4", "8"] {
        assert_eq!(
            serial,
            convert_with(threads, &format!("p{threads}.nt")),
            "--parallelism {threads} changed the output"
        );
    }
}

#[test]
fn a_blank_node_named_in_two_chunks_stays_one_node_through_the_cli() {
    // Turtle scopes a labelled blank node to the document. Split across
    // chunks, independent relabellers would give it two output labels.
    let tmp = TempDir::new().unwrap();
    let mut ttl =
        String::from("@prefix ex: <http://example.org/> .\n_:shared ex:role \"first\" .\n");
    ttl.push_str(
        &parallel_corpus(30_000)
            .lines()
            .skip(1)
            .collect::<Vec<_>>()
            .join("\n"),
    );
    ttl.push_str("\n_:shared ex:role \"last\" .\n");
    let input = fixture(&tmp, "shared.ttl", &ttl);

    let out = tmp.path().join("out.nt");
    rdf_cmd()
        .args(["--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .assert()
        .success();

    let text = std::fs::read_to_string(&out).unwrap();
    let label_for = |role: &str| -> String {
        text.lines()
            .find(|l| l.contains(role))
            .and_then(|l| l.split_whitespace().next())
            .unwrap_or_default()
            .to_string()
    };
    let first = label_for("\"first\"");
    assert!(
        first.starts_with("_:"),
        "expected a blank subject, got {first}"
    );
    assert_eq!(
        first,
        label_for("\"last\""),
        "`_:shared` was split into two nodes across chunks"
    );
}

#[test]
fn a_syntax_that_chunking_would_change_falls_back_to_serial() {
    // Turtle folds consecutive same-subject runs, so a chunk boundary inside
    // one changes the bytes. The run must still succeed — serially — rather
    // than either refusing or silently producing different output.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &parallel_corpus(20_000));

    let with = |threads: &str, out: &str| -> Vec<u8> {
        let path = tmp.path().join(out);
        rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "turtle"])
            .arg("-o")
            .arg(&path)
            .assert()
            .success();
        std::fs::read(&path).unwrap()
    };
    assert_eq!(
        with("1", "s.ttl"),
        with("8", "p.ttl"),
        "turtle output must be identical whatever --parallelism says, because \
         it is produced serially either way"
    );
}

#[test]
fn the_profile_reports_the_parallel_decision_and_its_phases() {
    let tmp = TempDir::new().unwrap();
    // Comfortably over the parallel input-size threshold.
    let input = fixture(&tmp, "big.ttl", &parallel_corpus(60_000));
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["--parallelism", "4", "-q", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    // The decision itself is reported, so "why is this not using my cores" is
    // answerable from the outside.
    assert_eq!(
        v["host"]["threads_used"], 4,
        "reason: {}",
        v["host"]["parallel_reason"]
    );
    assert_eq!(v["host"]["parallel_reason"], "parallel");

    let phases: Vec<&str> = v["phases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["phase"].as_str().unwrap())
        .collect();
    assert!(phases.contains(&"workers"), "phases: {phases:?}");
    assert!(phases.contains(&"reassembly"), "phases: {phases:?}");

    // The pre-scan must be visible and must have fired. A lane that exists but
    // reports nothing is how 43% of the wall went unattributed for a whole
    // bucket, so "the lane is present" is not enough to assert.
    assert!(phases.contains(&"chunk"), "phases: {phases:?}");
    let chunk_ns = v["phases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["phase"] == "chunk")
        .and_then(|p| p["ns"].as_u64())
        .unwrap();
    assert!(
        chunk_ns > 0,
        "the chunk lane reported zero on a chunked run"
    );

    // Worker time is a cross-thread sum, so it may exceed wall — and must not
    // have been folded into the sequential total.
    let unattributed = v["unattributed_ns"].as_u64().unwrap();
    let wall = v["wall_ns"].as_u64().unwrap();
    assert!(
        unattributed <= wall,
        "unattributed {unattributed} > wall {wall}"
    );
    // And the pre-scan is no longer hiding inside it: what the chunker costs
    // now has a name.
    assert!(
        unattributed < chunk_ns,
        "unattributed {unattributed} did not shrink below the chunk lane {chunk_ns} — \
         the scan is still being charged to nobody"
    );
}

#[test]
fn a_serial_run_reports_no_chunk_phase() {
    // The lane is not decoration: the serial path never chunks, so the phase
    // must be absent rather than reported as a suspicious zero.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &parallel_corpus(60_000));
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["--parallelism", "1", "-q", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    let phases: Vec<&str> = v["phases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["phase"].as_str().unwrap())
        .collect();
    assert!(!phases.contains(&"chunk"), "phases: {phases:?}");
}

/// 120,000 statements with one that does not parse, far enough in to land in
/// a late chunk.
///
/// Past the chunker's one-megabyte header scan as well, and that is not
/// incidental: the scan TOKENIZES what it reads, so a document that fails to
/// lex inside the first megabyte cannot be chunked at all and the whole run
/// falls back to serial — testing the path this fixture exists to reach.
fn corpus_with_a_late_error() -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..120_000 {
        if i == 90_000 {
            ttl.push_str("ex:bad ex:p ?? .\n");
        }
        ttl.push_str(&format!(
            "ex:s{i} ex:name \"person {i}\" ; ex:age {} .\n",
            i % 90
        ));
    }
    assert!(
        ttl.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate",
        ttl.len()
    );
    ttl
}

#[test]
fn a_parallel_parse_failure_is_located_like_a_serial_one() {
    // A worker parses `prefix block + its chunk`, so the offset a failing
    // chunk reports is an offset into that synthesized document — on this
    // fixture it names a line about 60,000 short. Rendering it against the
    // real file would be worse than saying nothing, and saying nothing is
    // what the parallel path did: the serial run pointed at
    // `late-error.ttl:90002:13` and the parallel run at `late-error.ttl`.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "late-error.ttl", &corpus_with_a_late_error());

    let mut reports = Vec::new();
    for threads in ["1", "8"] {
        let assert = rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt"])
            .arg("-o")
            .arg(tmp.path().join(format!("out{threads}.nt")))
            .assert()
            .code(EXIT_DOCUMENT_INVALID);
        let stderr = String::from_utf8(assert.get_output().stderr.clone()).unwrap();
        // Line 1 is the header, then 90,000 statements, then this one; the
        // `?` sits at column 13.
        assert!(
            stderr.contains("late-error.ttl:90002:13:"),
            "--parallelism {threads} did not anchor the failure in the document: {stderr}"
        );
        reports.push(stderr);
    }
    assert_eq!(
        reports[0], reports[1],
        "the two widths described the same failure differently"
    );
}

/// A header past the chunker's one-megabyte prefix scan, over a body big
/// enough to clear the parallel gate.
///
/// Directives beyond the scan window are indistinguishable, to the chunker,
/// from directives after data — so this is an unchunkable document that is
/// nevertheless perfectly legal Turtle and must still convert.
fn oversized_header_corpus() -> String {
    // `splitter::PREFIX_SCAN_SIZE`, which is private. Restated rather than
    // exported: the test needs a header the scan cannot finish, and any value
    // at or above the real one gives that.
    const PREFIX_SCAN_SIZE: usize = 1024 * 1024;
    let mut ttl = String::new();
    let mut n = 0;
    while ttl.len() < PREFIX_SCAN_SIZE + 4096 {
        ttl.push_str(&format!("@prefix p{n}: <http://example.org/ns{n}/> .\n"));
        n += 1;
    }
    for i in 0..60_000 {
        ttl.push_str(&format!(
            "p0:s{i} p0:name \"a literal wide enough to push the body past the gate {i}\" .\n"
        ));
    }
    assert!(
        ttl.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate",
        ttl.len()
    );
    ttl
}

#[test]
fn a_run_that_falls_back_to_serial_still_reports_what_the_attempt_cost() {
    // `threads_used: 1` beside a non-zero `chunk` lane is not a contradiction
    // in the profile: the scan really did run, found the document unchunkable,
    // and the run then converted serially. On a fallback that time is pure
    // overhead — the one case where the lane's number is most worth having —
    // and `Phase::Chunk` used to document itself as "a serial run reports
    // zero", which makes this pairing read as a bug in the profile instead.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "bighdr.ttl", &oversized_header_corpus());
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["-q", "--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    assert_eq!(
        v["host"]["threads_used"], 1,
        "reason: {}",
        v["host"]["parallel_reason"]
    );
    let chunk_ns = v["phases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["phase"] == "chunk")
        .and_then(|p| p["ns"].as_u64());
    assert!(
        chunk_ns.is_some_and(|ns| ns > 0),
        "the attempt that forced the fallback was charged to nobody: {}",
        v["phases"]
    );

    // And the document converted, which is the point of falling back rather
    // than refusing.
    assert_eq!(
        std::fs::read_to_string(&out).unwrap().lines().count(),
        60_000
    );
}

#[test]
fn json_profile_stderr_is_parseable_on_a_chunking_fallback() {
    // `2> run.json` is the bench lane's idiom, so stderr under `--profile=json`
    // is a document and not a place for prose. Five of the six prose sites
    // knew that; the fallback note did not, and it is emitted on exactly the
    // documents most likely to be profiled — the big ones that turn out not to
    // chunk. Note the absence of `-q` here: passing it would hide the bug
    // rather than test it.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "bighdr.ttl", &oversized_header_corpus());
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();

    serde_json::from_slice::<serde_json::Value>(&stderr).unwrap_or_else(|e| {
        panic!(
            "stderr is not a JSON document: {e}\n{}",
            String::from_utf8_lossy(&stderr)
        )
    });
}

#[test]
fn quiet_silences_courtesy_lines_and_never_the_diagnostics() {
    // The two levels, pinned apart. `-q` is for the ✓ line and the fallback
    // note; it must never reach a skip, the swallow note, or the closing
    // warning, because a script that asked for quiet still may not read a
    // partial conversion as a whole one. Folding the levels together is the
    // obvious simplification and this is what refuses it.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "partly.ttl", PARTLY_BROKEN);
    let out = tmp.path().join("out.nt");

    for quiet in [false, true] {
        let mut cmd = rdf_cmd();
        if quiet {
            cmd.arg("-q");
        }
        let stderr = cmd
            .args(["rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt", "--continue-on-error"])
            .arg("-o")
            .arg(&out)
            .assert()
            .code(EXIT_DOCUMENT_INVALID)
            .get_output()
            .stderr
            .clone();
        let stderr = String::from_utf8(stderr).unwrap();

        assert!(
            stderr.contains("skipped:"),
            "-q {quiet}: a dropped statement went unreported: {stderr}"
        );
        assert!(
            stderr.contains("2 statement(s) skipped"),
            "-q {quiet}: the closing warning is not a courtesy: {stderr}"
        );
    }

    // And under a JSON profile both levels go quiet, whatever -q says, because
    // stderr is carrying a document either way.
    for quiet in [false, true] {
        let mut cmd = rdf_cmd();
        if quiet {
            cmd.arg("-q");
        }
        let stderr = cmd
            .args(["rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt", "--continue-on-error"])
            .arg("-o")
            .arg(&out)
            .args(["--profile=json", "--no-hash"])
            .assert()
            .code(EXIT_DOCUMENT_INVALID)
            .get_output()
            .stderr
            .clone();
        serde_json::from_slice::<serde_json::Value>(&stderr).unwrap_or_else(|e| {
            panic!(
                "-q {quiet}: stderr is not a JSON document: {e}\n{}",
                String::from_utf8_lossy(&stderr)
            )
        });
    }
}

// ============================================================================
// --continue-on-error
// ============================================================================

const PARTLY_BROKEN: &str = "@prefix ex: <http://example.org/> .\n\
                             ex:a ex:p \"1\" .\n\
                             ex:b ex:p ?? .\n\
                             ex:c ex:p \"3\" .\n\
                             ex:d ex:p ?? .\n\
                             ex:e ex:p \"5\" .\n";

#[test]
fn continue_on_error_keeps_the_good_statements_and_exits_1() {
    // riot semantics: skipping is not success. A script must not be able to
    // read a partial conversion as a whole one, so the exit code says so even
    // though output was produced.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "partly.ttl", PARTLY_BROKEN);

    let out = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt", "--continue-on-error"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stderr(predicate::str::contains("2 statement(s) skipped"))
        .stderr(predicate::str::contains("skipped:"))
        .get_output()
        .stdout
        .clone();

    let text = String::from_utf8(out).unwrap();
    assert_eq!(text.lines().count(), 3, "the three good statements: {text}");
    for good in ["\"1\"", "\"3\"", "\"5\""] {
        assert!(text.contains(good), "lost a good statement {good}: {text}");
    }
    assert!(
        !text.contains("ex:b"),
        "a skipped statement reached the output"
    );
}

#[test]
fn without_the_flag_the_first_error_still_stops_the_run() {
    // The default must not change: a converter that quietly drops input is
    // worse than one that refuses.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "partly.ttl", PARTLY_BROKEN);

    let out = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stderr(predicate::str::contains("prefix of the conversion"))
        .get_output()
        .stdout
        .clone();
    assert_eq!(
        String::from_utf8(out).unwrap().lines().count(),
        1,
        "the default stops at the first bad statement"
    );
}

#[test]
fn a_clean_document_under_continue_on_error_exits_0() {
    // The flag must not turn a good document into a failure.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "clean.ttl", VALID_TURTLE);
    let out = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt", "--continue-on-error"])
        .assert()
        .success()
        .stderr(predicate::str::contains("skipped").not())
        .get_output()
        .stdout
        .clone();
    assert_eq!(
        String::from_utf8(out).unwrap().lines().count(),
        VALID_TURTLE_TRIPLES as usize
    );
}

#[test]
fn a_skipped_statement_leaves_no_fragment_behind() {
    // The parser emits during descent, so a statement with several
    // predicate-object pairs has written part of itself before the failure is
    // known. "Skipped" has to mean nothing of it survives.
    let tmp = TempDir::new().unwrap();
    let input = fixture(
        &tmp,
        "fragments.ttl",
        "@prefix ex: <http://example.org/> .\n\
         ex:good ex:p \"keep\" .\n\
         ex:bad ex:p \"fragment one\" ; ex:q \"fragment two\" ; ex:r ?? .\n\
         ex:after ex:p \"keep too\" .\n",
    );

    let out = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt", "--continue-on-error"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .get_output()
        .stdout
        .clone();

    let text = String::from_utf8(out).unwrap();
    assert!(text.contains("keep"), "{text}");
    assert!(text.contains("keep too"), "{text}");
    assert!(
        !text.contains("fragment"),
        "a rolled-back statement left its first triples in the output:\n{text}"
    );
}

#[test]
fn every_skip_is_located_in_the_original_document() {
    // Diagnostics are positioned against the whole file, not the fragment the
    // resumed parse saw — a user counting lines is counting the file's.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "partly.ttl", PARTLY_BROKEN);
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt", "--continue-on-error"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        // The two bad statements are on lines 3 and 5 of the file.
        .stderr(predicate::str::contains("partly.ttl:3:"))
        .stderr(predicate::str::contains("partly.ttl:5:"));
}

/// Four lines: a header, a good statement, junk carrying no terminator of its
/// own, and the statement whose terminator the resync therefore runs to.
const UNTERMINATED_JUNK: &str = "@prefix ex: <http://example.org/> .\n\
                                 ex:a ex:p \"1\" .\n\
                                 junk with no terminator\n\
                                 ex:c ex:p \"3\" .\n";

/// The same document with the junk terminated, so the resync stops at the
/// junk's own `.` and takes nothing with it.
const TERMINATED_JUNK: &str = "@prefix ex: <http://example.org/> .\n\
                               ex:a ex:p \"1\" .\n\
                               junk with a terminator .\n\
                               ex:c ex:p \"3\" .\n";

/// Convert with `--continue-on-error` and return `(stdout, stderr)`.
fn recover(input: &Path) -> (String, String) {
    let assert = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(input)
        .args(["--to", "nt", "--continue-on-error"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID);
    (
        String::from_utf8(assert.get_output().stdout.clone()).unwrap(),
        String::from_utf8(assert.get_output().stderr.clone()).unwrap(),
    )
}

#[test]
fn a_resync_that_swallows_the_next_statement_says_so() {
    // Recovery resumes at the next statement TERMINATOR, so junk without one
    // ends at the terminator belonging to the statement AFTER it — which is
    // then never parsed, never diagnosed and never counted. The run reported
    // "1 statement(s) skipped" having lost two, with stderr byte-identical to
    // the honest case, and being identical is what made it undetectable.
    let tmp = TempDir::new().unwrap();
    let (out, stderr) = recover(&fixture(&tmp, "unterminated.ttl", UNTERMINATED_JUNK));

    // The loss is real: `ex:c` never reaches the output, and the count says
    // one. That count cannot be repaired — nothing parsed those bytes, so
    // nothing knows how many statements they held.
    assert_eq!(out.lines().count(), 1, "{out}");
    assert!(stderr.contains("1 statement(s) skipped"), "{stderr}");

    // So the span is reported instead, on its own line.
    assert!(
        stderr.contains("note:"),
        "a resync that lost a statement said nothing about it: {stderr}"
    );
    // `ex:c ex:p "3" .` is 15 bytes.
    assert!(
        stderr.contains("consumed 15 more byte(s)"),
        "the note must name how much it swallowed, not merely that it did: {stderr}"
    );
    assert!(
        stderr.contains("resumed at line 4"),
        "the note must name where the parse picked up again: {stderr}"
    );
}

#[test]
fn a_resync_that_stops_at_its_own_terminator_stays_silent() {
    // The negative control, and the reason the note has a line of its own: a
    // run that lost only what it reported keeps exactly the stderr it always
    // had. A note here would be noise on every recoverable document.
    let tmp = TempDir::new().unwrap();
    let (out, stderr) = recover(&fixture(&tmp, "terminated.ttl", TERMINATED_JUNK));

    assert_eq!(out.lines().count(), 2, "{out}");
    assert!(stderr.contains("1 statement(s) skipped"), "{stderr}");
    assert!(
        !stderr.contains("note:") && !stderr.contains("resync consumed"),
        "the honest case grew a note it did not need: {stderr}"
    );
}

#[test]
fn the_swallowed_span_stops_where_the_parse_resumed() {
    // The span is the statement the resync ate, not everything after the
    // error. What follows the resume point on the SAME line survives, and
    // reporting it as lost would be a second wrong number in place of the
    // first one.
    let tmp = TempDir::new().unwrap();
    let (out, stderr) = recover(&fixture(
        &tmp,
        "tail.ttl",
        "@prefix ex: <http://example.org/> .\n\
         ex:a ex:p \"1\" .\n\
         junk with no terminator\n\
         ex:c ex:p \"3\" . ex:d ex:p \"4\" .\n",
    ));

    assert!(
        stderr.contains("consumed 15 more byte(s)") && stderr.contains("resumed at line 4"),
        "{stderr}"
    );
    assert_eq!(out.lines().count(), 2, "{out}");
    assert!(
        out.contains("\"4\""),
        "the statement after the resume point was reported lost, and was not: {out}"
    );
}

#[test]
fn a_multi_line_statement_is_not_reported_as_swallowed() {
    // A statement may span lines and still carry its own terminator, so the
    // resync lands on ITS `.` and eats nothing. The first version of this
    // warning split the error-to-resume span at the first newline and reported
    // whatever followed, which on this document is the statement's own second
    // line: 24 bytes announced as lost while `ex:c` converted fine.
    //
    // The positional shape here is identical to the honest case in
    // `a_resync_that_swallows_the_next_statement_says_so` — error on line 3,
    // resume at the end of line 4 — so nothing about WHERE the bytes are can
    // separate them. Only what they say.
    let tmp = TempDir::new().unwrap();
    let (out, stderr) = recover(&fixture(
        &tmp,
        "multi.ttl",
        "@prefix ex: <http://example.org/> .\n\
         ex:a ex:p \"ok\" .\n\
         ex:bad ~~~ \"still the same statement\"\n\
             ex:more \"and more\" .\n\
         ex:c ex:p \"fine\" .\n",
    ));

    assert_eq!(
        out.lines().count(),
        2,
        "nothing was lost, so nothing may be missing: {out}"
    );
    assert!(stderr.contains("1 statement(s) skipped"), "{stderr}");
    assert!(
        !stderr.contains("resync consumed"),
        "the statement's own second line was reported as swallowed: {stderr}"
    );
}

#[test]
fn a_continuation_after_a_semicolon_is_not_reported_as_swallowed() {
    // The idiomatic multi-line spelling: `;` then an indented predicate. Same
    // requirement as above and the more common shape in real documents.
    let tmp = TempDir::new().unwrap();
    let (out, stderr) = recover(&fixture(
        &tmp,
        "semi-healthy.ttl",
        "@prefix ex: <http://example.org/> .\n\
         ex:a ex:p \"1\" .\n\
         ex:bad ~~~ \"x\" ;\n\
             ex:more \"and more\" .\n\
         ex:c ex:p \"3\" .\n",
    ));

    assert_eq!(out.lines().count(), 2, "{out}");
    assert!(
        !stderr.contains("resync consumed"),
        "a `;` continuation was reported as a swallowed statement: {stderr}"
    );
}

#[test]
fn a_statement_lost_after_a_semicolon_is_still_reported() {
    // The trap, and the reason this file does NOT check for a trailing `;`.
    //
    // That check is the obvious way to recognise a continuation, and it
    // suppresses exactly the case the warning exists for: `ex:bad ~~~ ;` has no
    // terminator of its own, so the resync runs to line 4's `.` and eats
    // `ex:c` whole. With a punctuator check in place this document lost a
    // statement and said nothing — measured, not theorised.
    //
    // The standalone-parse test gets both this and the case above right,
    // because a `;` continuation is `predicate object` and does not parse as a
    // statement on its own, while `ex:c ex:p "3" .` does.
    let tmp = TempDir::new().unwrap();
    let (out, stderr) = recover(&fixture(
        &tmp,
        "semi-lossy.ttl",
        "@prefix ex: <http://example.org/> .\n\
         ex:a ex:p \"1\" .\n\
         ex:bad ~~~ ;\n\
         ex:c ex:p \"3\" .\n",
    ));

    assert_eq!(
        out.lines().count(),
        1,
        "`ex:c` was eaten by the resync: {out}"
    );
    assert!(
        stderr.contains("resync consumed"),
        "a statement was lost and the run said nothing: {stderr}"
    );
}

// ============================================================================
// the three-layered parallel differential
// ============================================================================
//
// Cross-mode byte-identity is deliberately NOT one of these. The parallel path
// assigns blank-node labels by a deterministic function of (label, chunk) so
// that workers need no coordination, and that function does not produce the
// same labels a single serial relabeller does. riot makes no cross-mode byte
// promise either. What IS promised, and gated here:
//
//   (a) determinism  — same input, same K, byte-identical across runs
//   (b) equivalence  — serial and parallel denote the same graph
//   (c) adversarial  — user labels shaped like mint patterns do not collide

/// A corpus with blank nodes of every kind, big enough to chunk.
fn blank_heavy_corpus(statements: usize) -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    ttl.push_str("_:shared ex:role \"first\" .\n");
    for i in 0..statements {
        ttl.push_str(&format!(
            "ex:s{i} ex:has [ ex:tag \"anon {i}\" ] ; ex:note \"a literal wide enough to make the corpus chunk {i}\" .\n"
        ));
        if i % 500 == 0 {
            ttl.push_str(&format!("_:lbl{i} ex:kind \"labelled {i}\" .\n"));
        }
    }
    ttl.push_str("_:shared ex:role \"last\" .\n");
    ttl
}

fn convert_at(tmp: &TempDir, input: &Path, threads: &str, out: &str) -> String {
    let path = tmp.path().join(out);
    rdf_cmd()
        .args(["--parallelism", threads, "rdf", "convert"])
        .arg(input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&path)
        .assert()
        .success();
    std::fs::read_to_string(&path).unwrap()
}

#[test]
fn a_parallel_run_is_byte_identical_to_itself() {
    // (a) Determinism. Thread scheduling must not reach the output — the
    // labels are a function of (label, chunk), and chunks concatenate in
    // order, so nothing about timing can change a byte.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "blanks.ttl", &blank_heavy_corpus(30_000));

    let first = convert_at(&tmp, &input, "8", "run1.nt");
    assert!(first.contains("_:"), "fixture must exercise blank nodes");
    for run in 2..=3 {
        assert_eq!(
            first,
            convert_at(&tmp, &input, "8", &format!("run{run}.nt")),
            "run {run} at the same parallelism produced different bytes"
        );
    }
}

#[test]
fn serial_and_parallel_denote_the_same_graph() {
    // (b) Equivalence, at three levels: isomorphism, blank-node identity
    // count, and triple count. Byte equality is NOT claimed — the labels
    // differ by design, and that is the trade that buys the scaling.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "blanks.ttl", &blank_heavy_corpus(2_000));

    let serial = convert_at(&tmp, &input, "1", "serial.nt");
    let parallel = convert_at(&tmp, &input, "8", "parallel.nt");

    assert_eq!(
        serial.lines().count(),
        parallel.lines().count(),
        "triple counts differ"
    );

    let distinct_blanks = |nt: &str| -> usize {
        let mut set = std::collections::HashSet::new();
        for line in nt.lines() {
            for term in line.split_whitespace() {
                if let Some(label) = term.strip_prefix("_:") {
                    set.insert(label.to_string());
                }
            }
        }
        set.len()
    };
    assert_eq!(
        distinct_blanks(&serial),
        distinct_blanks(&parallel),
        "the two modes disagree on how many distinct blank nodes exist — one \
         of them merged or split something"
    );

    // And the shared labelled node is still ONE node in the parallel output.
    let label_for = |nt: &str, role: &str| -> String {
        nt.lines()
            .find(|l| l.contains(role))
            .and_then(|l| l.split_whitespace().next())
            .unwrap_or_default()
            .to_string()
    };
    let first = label_for(&parallel, "\"first\"");
    assert!(first.starts_with("_:"), "{first}");
    assert_eq!(
        first,
        label_for(&parallel, "\"last\""),
        "`_:shared` was split across chunks"
    );
}

#[test]
fn user_labels_shaped_like_mints_survive_the_parallel_scheme() {
    // (c) The adversarial fixture, which is what caught what the plain
    // differential missed. Every label here imitates a pattern the scheme
    // itself produces: `u{L}` renames, `g{chunk}_{n}` mints, the `fdb-`
    // carve-out, and the writers' own reserved namespace.
    let tmp = TempDir::new().unwrap();
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    let bait = [
        "b1", "b2", "g0_1", "g1_1", "g7_3", "u1", "ug0_1", "ufdb-x", "fdbw-1", "c0_1",
    ];
    for label in bait {
        ttl.push_str(&format!("_:{label} ex:bait \"{label}\" .\n"));
    }
    ttl.push_str(
        &blank_heavy_corpus(2_000)
            .lines()
            .skip(1)
            .collect::<Vec<_>>()
            .join("\n"),
    );
    ttl.push('\n');
    let input = fixture(&tmp, "bait.ttl", &ttl);

    let serial = convert_at(&tmp, &input, "1", "s.nt");
    let parallel = convert_at(&tmp, &input, "8", "p.nt");

    let distinct_blanks = |nt: &str| -> usize {
        let mut set = std::collections::HashSet::new();
        for line in nt.lines() {
            for term in line.split_whitespace() {
                if let Some(label) = term.strip_prefix("_:") {
                    set.insert(label.to_string());
                }
            }
        }
        set.len()
    };
    assert_eq!(
        distinct_blanks(&serial),
        distinct_blanks(&parallel),
        "a bait label collided with a generated one"
    );

    // Every bait node kept its own identity: ten baits, ten distinct labels.
    let bait_labels: std::collections::HashSet<&str> = parallel
        .lines()
        .filter(|l| l.contains("ex:bait") || l.contains("/bait"))
        .filter_map(|l| l.split_whitespace().next())
        .collect();
    assert_eq!(
        bait_labels.len(),
        bait.len(),
        "bait nodes merged: {} labels for {} nodes",
        bait_labels.len(),
        bait.len()
    );
}

#[test]
fn the_parallel_scheme_is_injective_over_its_three_label_classes() {
    // The disjointness argument, mechanized. `u{L}` for user labels,
    // `g{c}_{n}` for mints, `fdb-` verbatim: pairwise disjoint by first
    // character, and injective within each class.
    let renamed = |l: &str| {
        if l.starts_with("fdb-") {
            l.to_string()
        } else {
            format!("u{l}")
        }
    };
    let mut seen = std::collections::HashSet::new();
    for label in ["b1", "g0_1", "u1", "ug0_1", "fdb-x", "fdbw-1", "", "u"] {
        assert!(seen.insert(renamed(label)), "user label {label} collided");
    }
    for chunk in 0..4 {
        for n in 1..4 {
            assert!(
                seen.insert(format!("g{chunk}_{n}")),
                "mint g{chunk}_{n} collided with a renamed user label"
            );
        }
    }
}

#[test]
fn turtle_runs_parallel_and_declares_its_prefixes_exactly_once() {
    // Each chunk re-parses the header prelude, so without suppression every
    // chunk's writer would emit the same `@prefix` block into the middle of
    // the concatenated document.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &parallel_corpus(60_000));

    let convert = |threads: &str, out: &str| -> String {
        let path = tmp.path().join(out);
        rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "turtle"])
            .arg("-o")
            .arg(&path)
            .assert()
            .success();
        std::fs::read_to_string(&path).unwrap()
    };

    let parallel = convert("8", "p.ttl");
    assert_eq!(
        parallel.matches("@prefix").count(),
        1,
        "prefixes declared once for the whole document, not once per chunk"
    );

    // And the result is a document our own reader accepts, with every triple.
    let reparsed = tmp.path().join("p.ttl");
    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).arg(&reparsed);
    let parallel_count = stdout_of(&mut cmd).trim().to_string();

    let serial_path = tmp.path().join("s.ttl");
    std::fs::write(&serial_path, convert("1", "s.ttl")).unwrap();
    let mut cmd = rdf_cmd();
    cmd.args(["-q", "rdf", "count"]).arg(&serial_path);
    assert_eq!(
        parallel_count,
        stdout_of(&mut cmd).trim(),
        "the parallel Turtle document holds a different number of triples"
    );
}

#[test]
fn a_parallel_turtle_run_is_byte_identical_to_itself() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &blank_heavy_corpus(20_000));

    let convert = |out: &str| -> Vec<u8> {
        let path = tmp.path().join(out);
        rdf_cmd()
            .args(["--parallelism", "8", "rdf", "convert"])
            .arg(&input)
            .args(["--to", "turtle"])
            .arg("-o")
            .arg(&path)
            .assert()
            .success();
        std::fs::read(&path).unwrap()
    };
    assert_eq!(convert("a.ttl"), convert("b.ttl"));
}

#[test]
fn the_profile_records_the_load_average() {
    // A timing taken on a loaded machine is not a timing, and there is no way
    // to tell after the fact unless the run says so.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "in.ttl", VALID_TURTLE);
    let out = tmp.path().join("out.nt");

    let stderr = rdf_cmd()
        .args(["-q", "rdf", "convert"])
        .arg(&input)
        .arg("-o")
        .arg(&out)
        .args(["--to", "nt", "--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();

    if cfg!(unix) {
        let load = v["host"]["load_average_1m"]
            .as_f64()
            .expect("unix reports a load average");
        assert!(load >= 0.0, "{load}");
    }
}

// ============================================================================
// dead destinations on the parallel path
// ============================================================================
//
// The serial path's early-termination contract — a closed downstream ends the
// run quietly with status 0 — has to survive parallelism. It did not: the
// writer stopped draining on EPIPE while workers stayed blocked on a full
// bounded channel, and `thread::scope` joined them forever at 0% CPU. Any
// input over the parallel threshold piped to `head` hung, which is the default
// path.
//
// What the matrix below does and does not prove, re-derived rather than
// inherited — the previous claim here was stale and stale in the dangerous
// direction.
//
// It proves the run ENDS on a dead reader, at every width. It does NOT show any
// termination mechanism to be load-bearing. With the prompt receiver drop, the
// write-failure stop and the unconditional post-loop stop ALL removed, it still
// passes: `rx` is a local of the scope closure and drops when that closure
// returns, which is before `thread::scope` joins. The old four-way table
// ("either one alone suffices … only removing BOTH hangs") described a receiver
// shape this file no longer has, and the only way to learn that was to run the
// cells again instead of trusting the sentence.
//
// So what are those mechanisms for? Promptness, which is worth having and is
// not what this matrix measures. Without them a worker keeps taking chunks
// until the channel fills and then parks in `send` until the closure returns —
// on a 4 GiB input piped to `head -1`, converting most of the file for nobody.
// Keep them; just do not believe this matrix is what guards them.
//
// The mechanism that IS load-bearing for liveness is the unconditional
// `budget.stop()` after the reassembly loop, and a different test proves it:
// `a_parse_error_wakes_workers_waiting_on_the_output_budget` hangs at the 90s
// ceiling without it. That case needs a SATURATED budget, which a dead reader
// never produces — which is exactly why this matrix cannot see it, and why
// reading these two tests as covering the same thing is the mistake to avoid.

/// `ParallelPlan::MIN_PARALLEL_BYTES`: below this an input converts serially
/// whatever `--parallelism` says, so a fixture under it tests the wrong path.
/// This is the reason a green suite missed the hang.
const MIN_PARALLEL_BYTES: usize = 4 * 1024 * 1024;

/// A corpus over the parallel input threshold, kept as small as possible
/// because chunking is a full byte scan and these run in a debug build.
fn over_threshold_corpus() -> String {
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    for i in 0..120_000 {
        ttl.push_str(&format!(
            "ex:s{i} ex:name \"person {i}\" ; ex:age {} .\n",
            i % 90
        ));
    }
    assert!(
        ttl.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate: \
         every case below would silently run serially",
        ttl.len()
    );
    ttl
}

/// 2 triples per subject × 120_000 subjects.
const OVER_THRESHOLD_TRIPLES: usize = 240_000;

/// How long one case may take before the run is called hung. A deadlock is
/// only distinguishable from slowness by waiting, and this fixture converts in
/// a couple of seconds in a debug build, so 90s is an order of magnitude of
/// headroom for a loaded machine and still finite.
const HUNG: std::time::Duration = std::time::Duration::from_secs(90);

fn spawn_convert(input: &Path, threads: &str, out: Option<&Path>) -> std::process::Child {
    use std::process::{Command as StdCommand, Stdio};

    let mut cmd = StdCommand::new(assert_cmd::cargo::cargo_bin!("fluree"));
    cmd.args(["--parallelism", threads, "rdf", "convert"])
        .arg(input)
        .args(["--to", "nt"]);
    if let Some(out) = out {
        cmd.arg("-o").arg(out);
    }
    cmd.env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Wait for `child` with a ceiling, killing it and FAILING on expiry.
///
/// A plain `wait()` turns a deadlock into a frozen suite rather than a red
/// test: nextest prints SLOW and the run stalls, and `cargo test` waits
/// forever. The wait happens on another thread so the timeout is enforceable,
/// and `wait_with_output` drains stderr so the child cannot block on a full
/// pipe and look hung when it is only chatty.
fn wait_bounded(child: std::process::Child, case: &str) -> (Option<i32>, String) {
    #[cfg(unix)]
    let pid = child.id();
    let started = std::time::Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });

    match rx.recv_timeout(HUNG) {
        Ok(Ok(out)) => (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ),
        Ok(Err(e)) => panic!("{case}: waiting on the child failed: {e}"),
        Err(_) => {
            #[cfg(unix)]
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
            panic!(
                "{case}: still running after {:?} — hung, not slow",
                started.elapsed()
            );
        }
    }
}

/// What the process on the other end of the pipe does before going away.
#[derive(Copy, Clone)]
enum DeadReader {
    /// The pipe is closed before a single byte is consumed.
    Immediate,
    /// `| head -1`, and `| head -100`: a few statements, then gone.
    Lines(usize),
    /// Closes part-way through a read, so the write failure lands inside the
    /// reassembly loop rather than on the first write.
    MidStream,
}

impl DeadReader {
    const ALL: [Self; 4] = [
        Self::Immediate,
        Self::Lines(1),
        Self::Lines(100),
        Self::MidStream,
    ];

    fn name(self) -> String {
        match self {
            Self::Immediate => "immediate close".into(),
            Self::Lines(n) => format!("head -{n}"),
            Self::MidStream => "close mid-read".into(),
        }
    }

    fn apply(self, stdout: std::process::ChildStdout) {
        use std::io::{BufRead, BufReader, Read};
        match self {
            Self::Immediate => drop(stdout),
            Self::Lines(n) => {
                let mut reader = BufReader::new(stdout);
                for i in 0..n {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    assert!(
                        line.starts_with('<'),
                        "line {i} should be a triple, got {line:?}"
                    );
                }
                // Dropping the reader closes the pipe.
            }
            Self::MidStream => {
                let mut stdout = stdout;
                let mut sink = vec![0u8; 256 * 1024];
                let _ = stdout.read(&mut sink);
            }
        }
    }
}

#[test]
fn a_dead_reader_ends_a_run_quietly_at_every_parallelism() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &over_threshold_corpus());

    // 1 is the serial baseline — it always honoured the contract — and 4 and 8
    // are the widths at which the pool deadlocked.
    for threads in ["1", "4", "8"] {
        for reader in DeadReader::ALL {
            let case = format!("--parallelism {threads} × {}", reader.name());
            let mut child = spawn_convert(&input, threads, None);
            reader.apply(child.stdout.take().unwrap());
            let (code, stderr) = wait_bounded(child, &case);
            assert_eq!(
                code,
                Some(0),
                "{case}: a closed downstream is a normal end, not a failure. stderr: {stderr}"
            );
        }
    }
}

#[test]
fn a_file_destination_completes_at_every_parallelism() {
    // The negative control. `-o FILE` never sees EPIPE and passed before the
    // fix, so if it hangs the pool itself is broken; if only the piped cases
    // hang, the termination path is. It also pins that ending quietly on a
    // dead pipe did not come at the cost of ending correctly on a live one.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.ttl", &over_threshold_corpus());

    for threads in ["1", "4", "8"] {
        let case = format!("--parallelism {threads} × -o FILE");
        let out = tmp.path().join(format!("out{threads}.nt"));
        let child = spawn_convert(&input, threads, Some(&out));
        let (code, stderr) = wait_bounded(child, &case);
        assert_eq!(code, Some(0), "{case}: stderr: {stderr}");
        let written = std::fs::read_to_string(&out).unwrap();
        assert_eq!(
            written.lines().count(),
            OVER_THRESHOLD_TRIPLES,
            "{case}: a live destination must receive the whole document"
        );
    }
}

/// A corpus whose N-Triples output runs many times its Turtle input, with a
/// statement that lexes but does not parse early in the second chunk.
///
/// Both properties are load-bearing. The expansion — every prefixed name blows
/// up to a ~200-byte IRI — is what lets the handful of chunks that can be in
/// flight at once exceed the output budget's 32 MiB floor; without it the
/// budget never binds, no worker ever waits, and the case under test does not
/// arise. And the bad statement must LEX, because the chunker tokenizes the
/// first megabyte looking for the header: a lexical error there makes the
/// document unchunkable and the whole run falls back to serial.
fn output_heavy_corpus_with_a_parse_error() -> String {
    let ns = format!("http://example.org/{}/", "a".repeat(180));
    let mut ttl = format!("@prefix ex: <{ns}> .\n");
    for i in 0..200_000 {
        if i == 12_000 {
            // Three tokens, all valid; the object is simply missing.
            ttl.push_str("ex:bad ex:p .\n");
        }
        ttl.push_str(&format!("ex:s{i} ex:p ex:o{i} .\n"));
    }
    assert!(
        ttl.len() > MIN_PARALLEL_BYTES,
        "the fixture is {} bytes, under the {MIN_PARALLEL_BYTES}-byte gate",
        ttl.len()
    );
    ttl
}

#[test]
fn a_parse_error_wakes_workers_waiting_on_the_output_budget() {
    // The third way out of the reassembly loop, and the one that woke nobody.
    // The writer is the only thread that releases budget, and it leaves three
    // ways: every chunk written, a write failure, and a parse error that cuts
    // the document short with bytes still charged. Only the first two
    // signalled, so a worker waiting for room waited for a release that could
    // never come and `thread::scope` joined it forever — the dead-destination
    // deadlock again, reached through the memory bound instead of the channel.
    //
    // Getting a worker INTO that wait is the whole difficulty. With a fast
    // destination the writer drains faster than the pool fills, the budget
    // never binds, and this passes without exercising anything: that is
    // exactly what a first attempt against `-o FILE` did. So the destination
    // here is a pipe drained more slowly than the pool produces. The writer
    // stalls on the pipe, the workers run ahead until the budget is full, and
    // they are parked in `wait_for_room` when the erroring chunk is written.
    let tmp = TempDir::new().unwrap();
    let input = fixture(
        &tmp,
        "budget.ttl",
        &output_heavy_corpus_with_a_parse_error(),
    );

    let mut child = spawn_convert(&input, "8", None);
    let stdout = child.stdout.take().unwrap();
    // ~2 MB/s, against a debug-build pool that manages several times that. The
    // ratio is the only thing that matters and it fails safe: on a machine so
    // loaded that the pool drops below the reader, the budget simply never
    // fills and this passes without having exercised the wait. It cannot go
    // red for being slow — only for not finishing at all.
    let drain = std::thread::spawn(move || {
        use std::io::Read;
        let mut stdout = stdout;
        let mut buf = vec![0u8; 64 * 1024];
        let mut total = 0usize;
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 {
                break;
            }
            total += n;
            std::thread::sleep(std::time::Duration::from_millis(30));
        }
        total
    });

    let (code, stderr) = wait_bounded(child, "parse error under a saturated output budget");
    let read = drain.join().expect("the drain thread must not panic");

    assert_eq!(
        code,
        Some(EXIT_DOCUMENT_INVALID),
        "a cut-short document must exit as an invalid one. stderr: {stderr}"
    );
    // Line 1 is the header, then 12,000 statements, then this one.
    assert!(
        stderr.contains("budget.ttl:12002:13:"),
        "the run ended, but not on the parse error: {stderr}"
    );
    // Without this the fixture could be silently converting on the serial
    // path, where there are no workers and nothing to deadlock.
    assert!(
        !stderr.contains("converting serially"),
        "the fixture was not chunked, so no worker ever ran: {stderr}"
    );
    assert!(
        read > 0,
        "nothing was written before the failure, so the writer never stalled \
         on the pipe and the budget never filled"
    );
}

#[test]
fn a_mid_file_directive_falls_back_to_serial_rather_than_refusing() {
    // §1.4 specifies a fallback, and the document is legal Turtle: only the
    // CHUNKING is impossible, because a redefinition would reach the first
    // chunk and nothing after it. Refusing meant a legal document could not be
    // converted at all.
    let tmp = TempDir::new().unwrap();
    let mut ttl = String::from("@prefix ex: <http://first.example/> .\n");
    for i in 0..80_000 {
        ttl.push_str(&format!(
            "ex:s{i} ex:p \"a longer literal to push this corpus over the parallel threshold {i}\" .\n"
        ));
    }
    ttl.push_str("@prefix ex: <http://second.example/> .\nex:z ex:p \"z\" .\n");
    let input = fixture(&tmp, "mid.ttl", &ttl);
    let out = tmp.path().join("out.nt");

    rdf_cmd()
        .args(["--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .assert()
        .success()
        .stderr(predicate::str::contains("converting serially"));

    let text = std::fs::read_to_string(&out).unwrap();
    assert_eq!(text.lines().count(), 80_001, "every statement converted");
    // TWO, not one, and the difference is the #1565 parser fix.
    //
    // The trailing statement is `ex:z ex:p "z" .`, so BOTH its subject and its
    // predicate sit after the rebinding and must denote the new namespace.
    // Before #1565 only the subject moved: `ex:z` is a span the document had
    // never used, while `ex:p` had been expanded 80,000 times already and came
    // back from the span cache under the OLD binding. Counting one occurrence
    // was counting the bug.
    assert_eq!(
        text.matches("second.example").count(),
        2,
        "the rebinding must reach the repeated predicate span, not just the fresh subject"
    );
    assert!(
        text.ends_with("<http://second.example/z> <http://second.example/p> \"z\" .\n"),
        "last line was: {:?}",
        text.lines().next_back()
    );

    // And the reason is reported rather than implicit.
    let stderr = rdf_cmd()
        .args(["-q", "--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
    assert_eq!(v["host"]["threads_used"], 1);
    assert!(
        v["host"]["parallel_reason"]
            .as_str()
            .unwrap()
            .contains("unchunkable"),
        "{}",
        v["host"]["parallel_reason"]
    );
}

#[test]
fn recovery_reads_a_line_format_strictly_too() {
    // Recovery re-parses fragments, and it re-parsed them with the TURTLE
    // parser whatever the input was — so `--continue-on-error` on a `.nt` file
    // accepted every Turtle-only construct the strict reader exists to reject.
    // Silently, too: recovery reports what it SKIPS, and a construct that
    // parses is never skipped, so the report said nothing was wrong.
    let tmp = TempDir::new().unwrap();
    // Every line terminates, including the junk one. Resync scans forward to
    // the next terminator, so a junk line WITHOUT one swallows the statement
    // after it — which would hide the very line this test is about.
    let nt = "<http://example.org/a> <http://example.org/p> \"ok\" .\n\
              <http://example.org/b> <http://example.org/p> 42 .\n\
              not a triple at all .\n\
              <http://example.org/c> <http://example.org/p> \"fine\" .\n";
    let input = fixture(&tmp, "recoverable.nt", nt);
    let out = tmp.path().join("out.nt");

    let assert = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt", "--continue-on-error"])
        .arg("-o")
        .arg(&out)
        .assert()
        .code(EXIT_DOCUMENT_INVALID);
    let stderr = String::from_utf8(assert.get_output().stderr.clone()).unwrap();

    // Two skips, not one: the junk line AND the bare number, which is a
    // perfectly good Turtle integer and not an N-Triples term.
    assert!(
        stderr.contains("2 statement(s) skipped"),
        "the bare number must be skipped as well as the junk line: {stderr}"
    );
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(
        written.lines().count(),
        2,
        "only the two valid N-Triples statements survive:\n{written}"
    );
    assert!(
        !written.contains("42"),
        "a bare number reached the output:\n{written}"
    );
}

#[test]
fn continue_on_error_still_emits_the_profile() {
    // Recovery is exactly when profiling matters — resync re-parses from each
    // error — and the two flags were mutually exclusive by accident.
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "partly.ttl", PARTLY_BROKEN);

    let stderr = rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args([
            "--to",
            "nt",
            "--continue-on-error",
            "--profile=json",
            "--no-hash",
        ])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .get_output()
        .stderr
        .clone();

    // stderr is one JSON document: the per-skip diagnostics would make it
    // unparseable, so the count travels inside it instead.
    let v: serde_json::Value = serde_json::from_slice(&stderr).unwrap_or_else(|e| {
        panic!(
            "stderr is not one JSON document ({e}): {}",
            String::from_utf8_lossy(&stderr)
        )
    });
    assert_eq!(v["verb"], "convert");
    assert_eq!(v["skipped_statements"], 2);

    // Without --profile the human diagnostics are still there.
    rdf_cmd()
        .args(["rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt", "--continue-on-error"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID)
        .stderr(predicate::str::contains("skipped:"))
        .stderr(predicate::str::contains("2 statement(s) skipped"));
}

// ============================================================================
// Flag interactions — one test per row of convert.md's table
// ============================================================================
//
// Reviewers found three separate cases of one flag silently overriding
// another (coe x profile, -o x profile-json, parallel x bnode-policy). The
// documented table in `docs/cli/rdf/convert.md` is the contract; these are its
// assertions. Each interaction is exercised in BOTH the serial and the
// parallel path, because two of the three defects existed only on one side.
//
// A new flag adds a row there and a test here.

/// Over `ParallelPlan::MIN_PARALLEL_BYTES`, so `--parallelism` is a real
/// choice rather than a no-op the assertions cannot see.
fn interaction_corpus() -> String {
    let mut nt = String::new();
    for i in 0..120_000 {
        nt.push_str(&format!(
            "<http://e/s{i}> <http://e/p> \"a longer literal to push this corpus over the parallel threshold {i}\" .\n"
        ));
    }
    nt
}

/// A document with one unparseable statement in the middle.
fn broken_doc() -> &'static str {
    "@prefix ex: <http://e/> .\nex:a ex:p \"1\" .\n@@@ broken\nex:b ex:p \"2\" .\n"
}

fn profile_json(stderr: &[u8]) -> serde_json::Value {
    serde_json::from_slice(stderr).expect("--profile=json must emit ONE parseable JSON document")
}

/// Row: `--continue-on-error` + `--profile=json`.
///
/// The whole point is that the per-skip diagnostics must NOT also be written
/// to stderr, or the JSON document stops being one.
#[test]
fn continue_on_error_with_json_profile_keeps_stderr_parseable() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "broken.ttl", broken_doc());

    for threads in ["1", "8"] {
        let out = tmp.path().join(format!("out{threads}.nt"));
        let assert = rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt"])
            .arg("-o")
            .arg(&out)
            .args(["--continue-on-error", "--profile=json", "--no-hash"])
            .assert()
            .code(EXIT_DOCUMENT_INVALID);

        let v = profile_json(&assert.get_output().stderr);
        assert_eq!(
            v["skipped_statements"], 1,
            "the skip count must travel in the JSON at --parallelism {threads}"
        );
    }
}

/// Row: `--continue-on-error` + `--profile` (human). Both are prose; both
/// appear.
#[test]
fn continue_on_error_with_human_profile_prints_both() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "broken.ttl", broken_doc());

    for threads in ["1", "8"] {
        let out = tmp.path().join(format!("h{threads}.nt"));
        rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt"])
            .arg("-o")
            .arg(&out)
            .args(["--continue-on-error", "--profile", "--no-hash"])
            .assert()
            .code(EXIT_DOCUMENT_INVALID)
            .stderr(predicate::str::contains("skipped:"))
            .stderr(predicate::str::contains("phase"));
    }
}

/// Row: `--continue-on-error` + `--parallelism` converts serially and says so.
#[test]
fn continue_on_error_forces_the_serial_path_and_reports_it() {
    let tmp = TempDir::new().unwrap();
    // Over the threshold, and to a LINE output, so every other reason to go
    // serial is excluded and only `--continue-on-error` can explain the
    // choice. Without this the test would pass on a one-line input, because
    // recovery short-circuits the plan BEFORE the size check.
    let mut doc = interaction_corpus();
    assert!(
        doc.len() > MIN_PARALLEL_BYTES,
        "fixture must clear the parallel threshold or this asserts nothing"
    );
    doc.push_str("@@@ broken\n");
    let input = fixture(&tmp, "big-broken.nt", &doc);
    let out = tmp.path().join("out.nt");

    // Control: the same corpus WITHOUT the flag does go parallel.
    let control = rdf_cmd()
        .args(["-q", "--parallelism", "8", "rdf", "convert"])
        .arg(fixture(&tmp, "clean.nt", &interaction_corpus()))
        .args(["--to", "nt"])
        .arg("-o")
        .arg(tmp.path().join("control.nt"))
        .args(["--profile=json", "--no-hash"])
        .assert()
        .success();
    assert!(
        profile_json(&control.get_output().stderr)["host"]["threads_used"]
            .as_u64()
            .unwrap()
            > 1,
        "control must run parallel, or the assertion below proves nothing"
    );

    let assert = rdf_cmd()
        .args(["-q", "--parallelism", "8", "rdf", "convert"])
        .arg(&input)
        .args(["--to", "nt"])
        .arg("-o")
        .arg(&out)
        .args(["--continue-on-error", "--profile=json", "--no-hash"])
        .assert()
        .code(EXIT_DOCUMENT_INVALID);

    let v = profile_json(&assert.get_output().stderr);
    assert_eq!(v["host"]["threads_used"], 1, "recovery is serial");
    assert!(
        v["host"]["parallel_reason"]
            .as_str()
            .unwrap()
            .contains("continue-on-error"),
        "the reason must name the flag that forced it: {}",
        v["host"]["parallel_reason"]
    );
}

/// Row: `-o FILE` + `--profile=json` — stdout stays empty, so the two streams
/// never mix. And without `-o`, the bytes go to stdout while the JSON does
/// not.
#[test]
fn output_file_and_json_profile_never_share_a_stream() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(
        &tmp,
        "ok.ttl",
        "@prefix ex: <http://e/> .\nex:a ex:p \"1\" .\n",
    );

    for threads in ["1", "8"] {
        let out = tmp.path().join(format!("o{threads}.nt"));
        let assert = rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt"])
            .arg("-o")
            .arg(&out)
            .args(["--profile=json", "--no-hash"])
            .assert()
            .success();
        assert!(
            assert.get_output().stdout.is_empty(),
            "-o must leave stdout empty at --parallelism {threads}"
        );
        profile_json(&assert.get_output().stderr);
        assert!(std::fs::read_to_string(&out)
            .unwrap()
            .contains("<http://e/a>"));

        // Without -o the converted bytes take stdout and the JSON still does not.
        let assert = rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt", "--profile=json", "--no-hash"])
            .assert()
            .success();
        assert!(String::from_utf8_lossy(&assert.get_output().stdout).contains("<http://e/a>"));
        profile_json(&assert.get_output().stderr);
    }
}

/// Row: `--bnode-policy preserve` + `--parallelism` converts serially and says
/// so. Silently relabelling here was a real defect.
#[test]
fn preserve_bnode_labels_forces_the_serial_path_and_reports_it() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "big.nt", &interaction_corpus());

    let reason_for = |policy: &str| -> (u64, String) {
        let out = tmp.path().join(format!("out-{policy}.nt"));
        let assert = rdf_cmd()
            .args(["-q", "--parallelism", "8", "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt", "--bnode-policy", policy])
            .arg("-o")
            .arg(&out)
            .args(["--profile=json", "--no-hash"])
            .assert()
            .success();
        let v = profile_json(&assert.get_output().stderr);
        (
            v["host"]["threads_used"].as_u64().unwrap(),
            v["host"]["parallel_reason"].as_str().unwrap().to_string(),
        )
    };

    let (threads, _) = reason_for("relabel");
    assert!(threads > 1, "relabel must still run in parallel");

    let (threads, reason) = reason_for("preserve");
    assert_eq!(threads, 1, "preserve must convert serially");
    assert!(
        reason.contains("preserve"),
        "the reason must name the flag that forced it: {reason}"
    );
}

/// Row: `--base` is inert for the line grammars rather than mis-applied.
#[test]
fn base_is_inert_for_line_formats_in_both_modes() {
    let tmp = TempDir::new().unwrap();
    let input = fixture(&tmp, "abs.nt", "<http://e/s> <http://e/p> <http://e/o> .\n");
    for threads in ["1", "8"] {
        rdf_cmd()
            .args(["--parallelism", threads, "rdf", "convert"])
            .arg(&input)
            .args(["--to", "nt", "--base", "http://other.example/"])
            .assert()
            .success()
            .stdout(predicate::str::contains("<http://e/s>"));
    }
}
