//! Sorted-golden fixtures for the RDF export writers.
//!
//! These exist to pin `export.rs`'s *output* while its formatting primitives
//! move into `fluree-graph-format`. Before this file there were no golden
//! tests over export anywhere, so a refactor of the escaping/prefix layer had
//! nothing holding it to "behavior changes: none".
//!
//! # Why the output is sorted
//!
//! Export is **not** line-deterministic. The indexed rows stream out of the
//! SPOT cursor in a fixed order, but the untranslated-overlay tail is drained
//! from a `HashMap` (`surviving_untranslated` → `HashMap::into_values`), so
//! its statements can appear in any order relative to each other from run to
//! run. Sorting the emitted lines is the workaround: it gives a stable
//! fingerprint of *what* was written without pretending the writer emits a
//! stable *sequence*.
//!
//! The consequence, stated plainly: a sorted Turtle golden is no longer valid
//! Turtle (subject grouping is `;`-continued across lines, and sorting shuffles
//! the continuations away from their subjects). It is a fingerprint, not a
//! document. Anything that changes an IRI's escaping, a literal's lexical
//! form, a prefix declaration, or which rows are skipped moves the
//! fingerprint; only statement *ordering* is deliberately invisible to it.
//!
//! # Regenerating
//!
//! `FLUREE_UPDATE_GOLDEN=1 cargo nextest run -p fluree-db-api export_golden`
//! rewrites the files under `tests/golden/`. Review the diff — the point of
//! the fixtures is that an unreviewed change to them is a red flag.

use fluree_db_api::export::ExportFormat;
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

/// Prefix context handed to Turtle/JSON-LD export.
///
/// `ex` and `schema` are both prefixes of real subjects here, and `xsd` covers
/// the datatype IRIs, so the fixture exercises longest-prefix-first matching
/// as well as the plain case.
fn export_context() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "exdeep": "http://example.org/ns/deep/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// The fixture document.
///
/// Every node carries an explicit `@id`: an id-less node would be skolemized
/// to `_:fdb-<ulid>`, and a ULID is time-and-randomness derived — the golden
/// would differ on every run. Coverage is chosen for what the *formatting*
/// layer has to get right, not for query behavior:
///
/// - plain strings, and a string needing N-Triples escaping (quote, backslash,
///   newline, tab, control character)
/// - an integer, a decimal, a double, a boolean, a dateTime
/// - two language-tagged strings on one predicate (multi-value grouping)
/// - `@type` (the `rdf:type` → `a` abbreviation in Turtle)
/// - references between subjects (`@id` objects — the ref branch)
/// - a predicate whose local name has no valid Turtle PN_LOCAL spelling, so
///   prefix compaction must decline and fall back to `<full-iri>`
/// - an IRI carrying a character that `IRIREF` forbids, so escaping must fire
fn fixture() -> JsonValue {
    json!({
        "@context": export_context(),
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "schema:Person",
                "schema:name": "Alice",
                "ex:note": "quote:\" backslash:\\ newline:\n tab:\t nul:\u{0}",
                "ex:age": 42,
                "ex:score": {"@value": "99.5", "@type": "xsd:decimal"},
                "ex:ratio": {"@value": 0.25, "@type": "xsd:double"},
                "ex:active": true,
                "ex:joined": {"@value": "2020-01-02T03:04:05Z", "@type": "xsd:dateTime"},
                "ex:label": [
                    {"@value": "Alice", "@language": "en"},
                    {"@value": "Alicia", "@language": "es"}
                ],
                "ex:knows": {"@id": "ex:bob"},
                "ex:has space": "local name that cannot be a PN_LOCAL",
                "exdeep:nested": "longest prefix wins"
            },
            {
                "@id": "ex:bob",
                "@type": "schema:Person",
                "schema:name": "Bob",
                "ex:knows": [{"@id": "ex:alice"}, {"@id": "ex:carol"}]
            },
            {
                "@id": "http://example.org/ns/odd|iri",
                "schema:name": "Subject whose IRI needs IRIREF escaping"
            }
        ]
    })
}

/// Build an indexed, file-backed ledger holding [`fixture`].
///
/// File-backed rather than memory-backed because export needs a binary index,
/// and `FlureeBuilder::memory()` leaves the ledger unindexed.
async fn seeded_ledger(dir: &std::path::Path) -> (fluree_db_api::Fluree, String) {
    let fluree = FlureeBuilder::file(dir.to_str().unwrap())
        .build()
        .expect("build file-backed fluree");
    let ledger_id = "export/golden:main";
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    fluree
        .insert(ledger, &fixture())
        .await
        .expect("insert fixture");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    (fluree, ledger_id.to_string())
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(name)
}

/// Compare `actual` against the golden file, or rewrite it under
/// `FLUREE_UPDATE_GOLDEN=1`.
fn assert_golden(name: &str, actual: &str) {
    let path = golden_path(name);
    if std::env::var_os("FLUREE_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).expect("create golden dir");
        std::fs::write(&path, actual).expect("write golden");
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "missing golden {}: {e}\nregenerate with FLUREE_UPDATE_GOLDEN=1",
            path.display()
        )
    });
    assert_eq!(
        expected.trim_end(),
        actual.trim_end(),
        "export output drifted from {} — if the change is intended, \
         regenerate with FLUREE_UPDATE_GOLDEN=1 and review the diff",
        path.display()
    );
}

/// Sort the lines of an export so the untranslated-overlay tail's `HashMap`
/// order cannot make the fixture flap. Blank lines are dropped: they only
/// ever separate the prefix block from the body.
fn sorted_lines(raw: &str) -> String {
    let mut lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
    lines.sort_unstable();
    let mut out = lines.join("\n");
    out.push('\n');
    out
}

async fn export_sorted(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    format: ExportFormat,
    with_context: bool,
) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let builder = fluree.export(ledger_id).format(format);
    let builder = if with_context {
        builder.context(&export_context())
    } else {
        builder
    };
    builder
        .write_to(&mut buf)
        .await
        .unwrap_or_else(|e| panic!("export {format:?} failed: {e}"));
    sorted_lines(&String::from_utf8(buf).expect("export output is UTF-8"))
}

#[tokio::test]
async fn export_golden_turtle() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (fluree, ledger_id) = seeded_ledger(tmp.path()).await;
    let out = export_sorted(&fluree, &ledger_id, ExportFormat::Turtle, true).await;
    assert_golden("export_turtle.sorted", &out);
}

#[tokio::test]
async fn export_golden_ntriples() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (fluree, ledger_id) = seeded_ledger(tmp.path()).await;
    // N-Triples takes no prefix map — every IRI is written in full, which is
    // exactly what makes this the escaping fixture.
    let out = export_sorted(&fluree, &ledger_id, ExportFormat::NTriples, false).await;
    assert_golden("export_ntriples.sorted", &out);
}

#[tokio::test]
async fn export_golden_jsonld() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (fluree, ledger_id) = seeded_ledger(tmp.path()).await;
    let out = export_sorted(&fluree, &ledger_id, ExportFormat::JsonLd, true).await;
    assert_golden("export_jsonld.sorted", &out);
}

/// The unsorted JSON-LD export must still be a parseable JSON document —
/// sorting the lines for the fingerprint destroys that property, so it is
/// checked separately rather than not at all.
#[tokio::test]
async fn export_jsonld_is_valid_json_before_sorting() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (fluree, ledger_id) = seeded_ledger(tmp.path()).await;
    let mut buf: Vec<u8> = Vec::new();
    fluree
        .export(&ledger_id)
        .format(ExportFormat::JsonLd)
        .context(&export_context())
        .write_to(&mut buf)
        .await
        .expect("export jsonld");
    let raw = String::from_utf8(buf).unwrap();
    let parsed: JsonValue = serde_json::from_str(&raw).expect("JSON-LD export is valid JSON");
    assert!(parsed.get("@graph").is_some(), "{raw}");
}
