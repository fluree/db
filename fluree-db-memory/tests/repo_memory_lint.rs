//! Lint the repository's own committed memory store.
//!
//! `.fluree-memory/repo.ttl` is a shared team asset: every contributor's agent
//! loads from it, and every session can write to it. This test enforces the
//! mechanical half of the hygiene contract in
//! `docs/memory/guides/hygiene-and-auditing.md` — parseability, the content
//! cap, resolvable repo-relative refs, and no corrupted or binary content —
//! so drift is caught in CI instead of compounding silently.
//!
//! Editorial rules (durability, audience, non-derivability) can't be linted;
//! they're enforced in review per the hygiene guide.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use fluree_db_memory::turtle_io::parse_and_inject_fulltext;
use fluree_db_memory::MAX_CONTENT_LENGTH;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate lives one level under the workspace root")
        .to_path_buf()
}

fn repo_ttl() -> String {
    let path = repo_root().join(".fluree-memory/repo.ttl");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Split the file into `(subject_id, block_text)` pairs using the same
/// column-0 scan as `turtle_io::insert_memory_into_file`.
fn blocks(src: &str) -> Vec<(String, String)> {
    let mut starts: Vec<usize> = Vec::new();
    let mut pos = 0;
    for line in src.split_inclusive('\n') {
        let l = line.trim_end_matches('\n');
        if l.starts_with("mem:") && l.contains(" a mem:") {
            starts.push(pos);
        }
        pos += line.len();
    }
    (0..starts.len())
        .map(|i| {
            let end = starts.get(i + 1).copied().unwrap_or(src.len());
            let block = &src[starts[i]..end];
            let id = block
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_start_matches("mem:")
                .to_string();
            (id, block.to_string())
        })
        .collect()
}

/// Values of a repeated string property within one block (raw, still escaped).
fn prop_values<'a>(block: &'a str, prop: &str) -> Vec<&'a str> {
    let needle = format!("mem:{prop} \"");
    let mut out = Vec::new();
    for line in block.lines() {
        if let Some(rest) = line.trim_start().strip_prefix(needle.as_str()) {
            // Value runs to the last unescaped quote on the line.
            if let Some(end) = find_closing_quote(rest) {
                out.push(&rest[..end]);
            }
        }
    }
    out
}

fn find_closing_quote(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i),
            _ => i += 1,
        }
    }
    None
}

fn unescaped_len(s: &str) -> usize {
    let mut count = 0;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            chars.next();
        }
        count += 1;
    }
    count
}

#[test]
fn repo_memory_file_parses() {
    let src = repo_ttl();
    let parsed = parse_and_inject_fulltext(&src).expect("repo.ttl must be valid Turtle");
    assert!(parsed.is_some(), "repo.ttl parsed to zero memory nodes");
}

#[test]
fn repo_memory_file_has_no_binary_or_debris_content() {
    let src = repo_ttl();
    for (i, b) in src.bytes().enumerate() {
        assert!(
            b == b'\n' || b == b'\t' || !b.is_ascii_control(),
            "control byte 0x{b:02x} at offset {i} — invisible bytes break grep and corrupt recall"
        );
    }
    // Fragments of serialized tool calls have leaked into stored content before;
    // they mark a memory whose body was corrupted at write time.
    for debris in [
        "</content>",
        "</invoke>",
        "</rationale>",
        "<parameter name=",
    ] {
        assert!(
            !src.contains(debris),
            "tool-call debris {debris:?} found — a memory body was corrupted at write time"
        );
    }
}

#[test]
fn repo_memory_blocks_are_well_formed() {
    let src = repo_ttl();
    let all = blocks(&src);
    assert!(!all.is_empty(), "no memory blocks found in repo.ttl");

    let mut seen = HashSet::new();
    for (id, block) in &all {
        let (kind_prefix, ulid) = id
            .split_once('-')
            .unwrap_or_else(|| panic!("{id}: id is not <kind>-<ulid>"));
        assert!(
            matches!(kind_prefix, "fact" | "decision" | "constraint"),
            "{id}: unknown kind prefix"
        );
        assert!(
            ulid.len() == 26
                && ulid
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "{id}: malformed ulid"
        );
        assert!(seen.insert(id.clone()), "{id}: duplicate memory id");

        let type_word = format!(" a mem:{}", capitalize(kind_prefix));
        assert!(
            block
                .lines()
                .next()
                .unwrap_or_default()
                .contains(&type_word),
            "{id}: id kind prefix does not match block type"
        );

        let contents = prop_values(block, "content");
        assert_eq!(contents.len(), 1, "{id}: exactly one mem:content required");
        let len = unescaped_len(contents[0]);
        assert!(
            len <= MAX_CONTENT_LENGTH,
            "{id}: content is {len} chars, over the {MAX_CONTENT_LENGTH}-char cap — split or tighten it"
        );

        let tags = prop_values(block, "tag");
        assert!(
            !tags.is_empty(),
            "{id}: at least one tag required (tags are the recall signal)"
        );
        for t in &tags {
            assert!(
                !t.is_empty() && !t.contains(' ') && *t == t.to_lowercase(),
                "{id}: tag {t:?} must be a lowercase keyword, not prose"
            );
        }

        assert!(
            block.contains("mem:scope mem:repo"),
            "{id}: repo.ttl must only hold repo-scoped memories"
        );

        for sev in prop_values(block, "severity") {
            assert!(
                matches!(sev, "must" | "should" | "prefer"),
                "{id}: invalid severity {sev:?}"
            );
            assert_eq!(
                kind_prefix, "constraint",
                "{id}: severity only belongs on constraints"
            );
        }

        let created = typed_dates(block, "createdAt");
        assert_eq!(created.len(), 1, "{id}: exactly one mem:createdAt required");

        // `updatedAt` is stamped by every update, including the no-field update
        // that means "re-verified at HEAD" — so a block carries zero or one.
        let updated = typed_dates(block, "updatedAt");
        assert!(
            updated.len() <= 1,
            "{id}: at most one mem:updatedAt allowed, found {}",
            updated.len()
        );
        for line in created.iter().chain(updated.iter()) {
            assert!(
                is_datetime_literal_line(line),
                "{id}: {line:?} is not a `\"<RFC 3339>\"^^xsd:dateTime` literal"
            );
        }
    }
}

/// Trimmed lines carrying a `mem:<prop>` timestamp within one block.
fn typed_dates<'a>(block: &'a str, prop: &str) -> Vec<&'a str> {
    let needle = format!("mem:{prop} \"");
    block
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with(needle.as_str()))
        .collect()
}

/// Whether a timestamp line is a typed `xsd:dateTime` literal whose value has
/// the RFC 3339 `YYYY-MM-DDThh:mm:ss` shape.
fn is_datetime_literal_line(line: &str) -> bool {
    let Some(rest) = line.split_once(" \"").map(|(_, r)| r) else {
        return false;
    };
    let Some((value, tail)) = rest.split_once('"') else {
        return false;
    };
    let typed = tail.trim_start().starts_with("^^xsd:dateTime");
    let shaped = value.len() >= 19
        && value.as_bytes()[..4].iter().all(u8::is_ascii_digit)
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[10] == b'T';
    typed && shaped
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

#[test]
fn repo_memory_refs_are_repo_relative_and_resolve() {
    let src = repo_ttl();
    let root = repo_root();
    let mut failures = Vec::new();
    for (id, block) in blocks(&src) {
        for r in prop_values(&block, "artifactRef") {
            if r.starts_with('/') || r.starts_with('~') || r.contains(":\\") {
                failures.push(format!("{id}: ref {r:?} is not repo-relative"));
                continue;
            }
            if r.split('/').any(|seg| seg == "..") {
                failures.push(format!("{id}: ref {r:?} escapes the repo"));
                continue;
            }
            if !root.join(r).exists() {
                failures.push(format!("{id}: ref {r:?} does not exist at HEAD"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "stale or invalid artifactRefs — update or drop them with `fluree memory update`:\n  {}",
        failures.join("\n  ")
    );
}
