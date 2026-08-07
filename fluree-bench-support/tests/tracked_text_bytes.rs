//! Reject control bytes in git-tracked text files.
//!
//! ```bash
//! cargo test -p fluree-bench-support --test tracked_text_bytes
//! ```
//!
//! # Why this exists
//!
//! A single U+0000 was committed inside a Rust string fixture in
//! `fluree-graph-format/tests/roundtrip.rs`. It compiled, the test passed, and
//! it survived `cargo test`, `cargo fmt --check`, `clippy`, and the full
//! `fluree-db-api` suite — NUL is valid UTF-8 inside a raw string literal, so
//! nothing in the toolchain had any reason to object.
//!
//! What it *did* break is search. `file(1)` classifies such a source as `data`
//! rather than text, and **grep and ripgrep then silently report no matches for
//! the entire file** — not an error, no diagnostic, just nothing. A reviewer
//! grepping for a symbol in that file is told it does not exist. The byte cost
//! the author of this test ten minutes before he thought to run `file`.
//!
//! It is a cheap class of defect to introduce and an expensive one to find, so
//! it is worth one walk of the tree.
//!
//! # What counts as a control byte
//!
//! Everything below U+0020 except the three whitespace characters that
//! legitimately appear in source: tab (0x09), line feed (0x0A) and carriage
//! return (0x0D). DEL (0x7F) is included — it is as invisible as NUL and has no
//! business in tracked text.
//!
//! Bytes 0x80..=0xFF are NOT inspected: they are ordinary UTF-8 continuation
//! bytes. The check is deliberately byte-oriented rather than char-oriented so
//! a file that is not valid UTF-8 at all is still examined rather than skipped.
//!
//! # Scope
//!
//! Only files git actually tracks, so build output, fetched corpora and
//! `.gitignore`d scratch never reach it. Files whose extension is on
//! [`BINARY_EXTENSIONS`] are exempt; anything else tracked is treated as text,
//! which is the safe default — a new binary fixture fails loudly and is added
//! to the list deliberately rather than sneaking in.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Extensions exempt from the check because their contents are legitimately
/// binary.
///
/// Deliberately short. Note that the repo's golden fixtures are TEXT and are
/// intentionally absent from this list: an invisible byte in a golden is
/// exactly the kind of thing that should fail.
const BINARY_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "ico", "webp", "pdf", "woff", "woff2", "ttf", "otf", "eot", "zip",
    "gz", "xz", "bz2", "zst", "tar", "wasm", "dylib", "so", "a", "o", "rlib", "bin", "flpack",
    "parquet", "avro", "keystore", "p12", "der",
];

/// Individual tracked paths exempt from the check.
///
/// Empty, and it should stay that way: a path here is a file nobody can grep.
/// Prefer fixing the file — a control character can almost always be written as
/// the host language's escape (Rust `\u{0}`, or the syntax's own escape inside a
/// raw string) with identical semantics and no invisible bytes.
const EXEMPT_PATHS: &[&str] = &[];

fn workspace_root() -> PathBuf {
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.parent()
        .unwrap_or_else(|| panic!("CARGO_MANIFEST_DIR has no parent: {}", here.display()))
        .to_path_buf()
}

/// Every path git tracks, relative to the repo root.
///
/// `-z` because a repo may legitimately contain a filename with a newline in
/// it, and splitting on newlines would then silently examine the wrong paths.
fn tracked_files(root: &Path) -> Vec<PathBuf> {
    let out = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(root)
        .output()
        .unwrap_or_else(|e| panic!("run `git ls-files` in {}: {e}", root.display()));

    assert!(
        out.status.success(),
        "`git ls-files` failed in {}: {}",
        root.display(),
        String::from_utf8_lossy(&out.stderr)
    );

    out.stdout
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| PathBuf::from(String::from_utf8_lossy(s).into_owned()))
        .collect()
}

fn is_exempt(rel: &Path) -> bool {
    let as_str = rel.to_string_lossy();
    if EXEMPT_PATHS.contains(&as_str.as_ref()) {
        return true;
    }
    rel.extension()
        .map(|e| e.to_string_lossy().to_ascii_lowercase())
        .is_some_and(|ext| BINARY_EXTENSIONS.contains(&ext.as_str()))
}

/// Whether `byte` is a control byte this check rejects.
fn is_forbidden_control(byte: u8) -> bool {
    match byte {
        b'\t' | b'\n' | b'\r' => false,
        0x00..=0x1F => true,
        0x7F => true,
        _ => false,
    }
}

/// 1-based line number of `offset`, for a message a human can act on.
fn line_of(data: &[u8], offset: usize) -> usize {
    data[..offset].iter().filter(|b| **b == b'\n').count() + 1
}

#[test]
fn tracked_text_files_contain_no_control_bytes() {
    let root = workspace_root();
    let mut offences: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for rel in tracked_files(&root) {
        if is_exempt(&rel) {
            continue;
        }
        let abs = root.join(&rel);
        // A tracked path can be absent from the working tree (sparse checkout,
        // a submodule gitlink). Missing is not an offence.
        let Ok(data) = std::fs::read(&abs) else {
            continue;
        };

        let mut hits: Vec<String> = Vec::new();
        for (offset, byte) in data.iter().enumerate() {
            if is_forbidden_control(*byte) {
                hits.push(format!(
                    "line {}, byte offset {offset}: 0x{byte:02X}",
                    line_of(&data, offset)
                ));
                if hits.len() == 5 {
                    hits.push("… (further occurrences not listed)".to_string());
                    break;
                }
            }
        }
        if !hits.is_empty() {
            offences.insert(rel.to_string_lossy().into_owned(), hits);
        }
    }

    if offences.is_empty() {
        return;
    }

    let mut message = String::from(
        "\ncontrol bytes found in git-tracked text files.\n\n\
         These are invisible in every editor and, worse, make `file(1)` classify\n\
         the source as binary — after which grep and ripgrep report NO matches\n\
         for the whole file, silently. A reviewer searching it is told the symbol\n\
         does not exist.\n\n\
         Fix by writing the character as an escape instead of a raw byte. Inside a\n\
         Rust string that is `\\u{0}`; inside a Rust RAW string (where Rust escapes\n\
         do not apply) use the embedded syntax's own escape — Turtle, JSON and\n\
         friends all have one, and the parsed result is identical.\n\n",
    );
    for (path, hits) in &offences {
        message.push_str(&format!("  {path}\n"));
        for hit in hits {
            message.push_str(&format!("      {hit}\n"));
        }
    }
    message.push_str(
        "\nIf a file here is genuinely binary, add its extension to\n\
         BINARY_EXTENSIONS in this test — deliberately, not reflexively.\n",
    );

    panic!("{message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classifier itself, since the whole guard rests on it.
    #[test]
    fn tabs_newlines_and_returns_are_allowed_everything_else_below_space_is_not() {
        for ok in *b"\t\n\r A~" {
            assert!(!is_forbidden_control(ok), "0x{ok:02X} must be allowed");
        }
        // The byte that motivated this test, plus the rest of C0 and DEL.
        for bad in [0x00_u8, 0x01, 0x07, 0x08, 0x0B, 0x0C, 0x1B, 0x1F, 0x7F] {
            assert!(is_forbidden_control(bad), "0x{bad:02X} must be rejected");
        }
        // UTF-8 continuation bytes are not control bytes.
        for cont in [0x80_u8, 0xC3, 0xFF] {
            assert!(!is_forbidden_control(cont), "0x{cont:02X} is not a control");
        }
    }

    #[test]
    fn line_numbers_are_one_based_and_count_preceding_newlines() {
        let data = b"alpha\nbeta\ngamma";
        assert_eq!(line_of(data, 0), 1);
        assert_eq!(line_of(data, 6), 2);
        assert_eq!(line_of(data, 11), 3);
    }

    #[test]
    fn binary_extensions_are_exempt_and_source_is_not() {
        assert!(is_exempt(Path::new("docs/img/diagram.png")));
        assert!(is_exempt(Path::new("fixtures/data.tar.gz")));
        assert!(!is_exempt(Path::new("src/lib.rs")));
        assert!(!is_exempt(Path::new("tests/golden/export_turtle.sorted")));
        // Case-insensitive, because a `.PNG` is still a PNG.
        assert!(is_exempt(Path::new("docs/IMG.PNG")));
    }
}
