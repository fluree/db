//! Turtle serialization and parsing for memory files.
//!
//! Handles writing `Memory` objects to `.ttl` files (canonical format)
//! and parsing `.ttl` files back into JSON-LD with `@fulltext` injection
//! for ledger import.

use crate::error::Result;
use crate::types::{Memory, MemoryKind, Scope};
use crate::vocab;
use serde_json::{json, Value};
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::Path;

/// Standard Turtle prefix declarations for memory files.
const TURTLE_PREFIXES: &str = "\
@prefix mem: <https://ns.flur.ee/memory#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

/// Header comment for repo-scoped memory files.
pub const REPO_HEADER: &str = "\
# Fluree Memory — repo-scoped
# Auto-managed by `fluree memory`. Manual edits are supported.\n";

/// Header comment for user-scoped memory files.
pub const USER_HEADER: &str = "\
# Fluree Memory — user-scoped (private, not shared via git)\n";

// ---------------------------------------------------------------------------
// Serialization: Memory → Turtle
// ---------------------------------------------------------------------------

/// Render a single memory as a Turtle subject block (no `@prefix` header).
///
/// Uses canonical predicate order and sorted multi-values for deterministic output.
pub fn memory_to_turtle_block(mem: &Memory) -> String {
    let local_id = mem_local_id(&mem.id);
    let mut s = String::with_capacity(512);

    // Subject + type (first predicate uses 'a')
    let type_local = match mem.kind {
        MemoryKind::Fact => "Fact",
        MemoryKind::Decision => "Decision",
        MemoryKind::Constraint => "Constraint",
    };
    writeln!(s, "mem:{local_id} a mem:{type_local} ;").unwrap();

    // mem:content (always present)
    writeln!(
        s,
        "    mem:content \"{}\" ;",
        escape_turtle_string(&mem.content)
    )
    .unwrap();

    // mem:tag (sorted, repeated predicates)
    let mut tags: Vec<&str> = mem.tags.iter().map(std::string::String::as_str).collect();
    tags.sort();
    for tag in &tags {
        writeln!(s, "    mem:tag \"{}\" ;", escape_turtle_string(tag)).unwrap();
    }

    // mem:scope
    let scope_ref = match mem.scope {
        Scope::Repo => "mem:repo",
        Scope::User => "mem:user",
    };
    writeln!(s, "    mem:scope {scope_ref} ;").unwrap();

    // mem:severity (optional)
    if let Some(sev) = &mem.severity {
        let sev_str = match sev {
            crate::types::Severity::Must => "must",
            crate::types::Severity::Should => "should",
            crate::types::Severity::Prefer => "prefer",
        };
        writeln!(s, "    mem:severity \"{sev_str}\" ;").unwrap();
    }

    // mem:artifactRef (sorted, repeated predicates)
    let mut refs: Vec<&str> = mem
        .artifact_refs
        .iter()
        .map(std::string::String::as_str)
        .collect();
    refs.sort();
    for aref in &refs {
        writeln!(
            s,
            "    mem:artifactRef \"{}\" ;",
            escape_turtle_string(aref)
        )
        .unwrap();
    }

    // mem:branch (optional)
    if let Some(b) = &mem.branch {
        writeln!(s, "    mem:branch \"{}\" ;", escape_turtle_string(b)).unwrap();
    }

    // mem:createdAt (always present)
    writeln!(
        s,
        "    mem:createdAt \"{}\"^^xsd:dateTime ;",
        escape_turtle_string(&mem.created_at)
    )
    .unwrap();

    // Type-specific optional predicates
    if let Some(r) = &mem.rationale {
        writeln!(s, "    mem:rationale \"{}\" ;", escape_turtle_string(r)).unwrap();
    }
    if let Some(a) = &mem.alternatives {
        writeln!(s, "    mem:alternatives \"{}\" ;", escape_turtle_string(a)).unwrap();
    }
    // Replace the trailing " ;\n" with " .\n" to close the subject block
    if s.ends_with(" ;\n") {
        s.truncate(s.len() - 3);
        s.push_str(" .\n");
    }

    s
}

/// Append a single memory block to an existing `.ttl` file.
///
/// Creates the file with prefix header + comment if it doesn't exist.
pub fn append_memory_to_file(path: &Path, mem: &Memory, header_comment: &str) -> Result<()> {
    use std::io::Write;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    if !path.exists() {
        // Create the file with header + prefixes
        let mut content = String::new();
        content.push_str(header_comment);
        content.push_str(TURTLE_PREFIXES);
        content.push('\n');
        content.push_str(&memory_to_turtle_block(mem));
        fs::write(path, content)?;
    } else {
        let mut f = fs::OpenOptions::new().append(true).open(path)?;
        write!(f, "\n{}", memory_to_turtle_block(mem))?;
    }

    Ok(())
}

/// Write a full `.ttl` file from scratch with all memories.
///
/// Used by `add`, `update`, and `forget` for all file mutations.
/// Memories are sorted by `(branch, id)` so that memories from the same
/// branch cluster together. This reduces merge conflicts: two feature
/// branches adding memories will insert into different regions of the file.
/// **Skips write if the new content is byte-identical** to the existing file.
pub fn write_memory_file(path: &Path, memories: &[Memory], header_comment: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut content = String::new();
    content.push_str(header_comment);
    content.push_str(TURTLE_PREFIXES);

    // Sort by (branch, id): groups memories by originating branch,
    // chronological within each branch (ULID encodes time).
    let mut sorted: Vec<&Memory> = memories.iter().collect();
    sorted.sort_by(|a, b| {
        let branch_a = a.branch.as_deref().unwrap_or("");
        let branch_b = b.branch.as_deref().unwrap_or("");
        branch_a.cmp(branch_b).then_with(|| a.id.cmp(&b.id))
    });

    for mem in sorted {
        content.push('\n');
        content.push_str(&memory_to_turtle_block(mem));
    }

    // Skip write if byte-identical (avoids spurious git diffs)
    if path.exists() {
        if let Ok(existing) = fs::read_to_string(path) {
            if existing == content {
                return Ok(());
            }
        }
    }

    fs::write(path, content)?;
    Ok(())
}

/// Create an empty `.ttl` file with just the header and prefix declarations.
pub fn create_empty_memory_file(path: &Path, header_comment: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut content = String::new();
    content.push_str(header_comment);
    content.push_str(TURTLE_PREFIXES);

    // Skip write if already exists and matches
    if path.exists() {
        if let Ok(existing) = fs::read_to_string(path) {
            if existing == content {
                return Ok(());
            }
        }
    }

    fs::write(path, content)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Import: Turtle file → JSON-LD with @fulltext injection
// ---------------------------------------------------------------------------

/// Properties that should be annotated with `@fulltext` during import.
const FULLTEXT_PROPERTIES: &[&str] = &[
    "mem:content",
    "https://ns.flur.ee/memory#content",
    "mem:rationale",
    "https://ns.flur.ee/memory#rationale",
];

/// Parse a Turtle file and inject `@fulltext` annotations on content/rationale.
///
/// Returns a JSON-LD value with `@context` and `@graph` array ready for
/// batch transact. Returns `None` if the Turtle content has no memory nodes
/// (e.g., empty file with only prefixes).
pub fn parse_and_inject_fulltext(turtle_content: &str) -> Result<Option<Value>> {
    use crate::error::MemoryError;

    let parsed = fluree_graph_turtle::parse_to_json(turtle_content)
        .map_err(|e| MemoryError::TurtleParse(e.to_string()))?;

    // parse_to_json returns either a single node object or an array of nodes
    let mut nodes = match parsed {
        Value::Array(arr) => arr,
        Value::Object(_) => vec![parsed],
        _ => return Ok(None),
    };

    if nodes.is_empty() {
        return Ok(None);
    }

    // Walk each node and inject @fulltext on content/rationale
    for node in &mut nodes {
        if let Value::Object(map) = node {
            for key in FULLTEXT_PROPERTIES {
                if let Some(val) = map.get_mut(*key) {
                    *val = inject_fulltext_value(val.take());
                }
            }
        }
    }

    // Wrap in a @context + @graph structure for transact
    let result = json!({
        "@context": {
            "mem": "https://ns.flur.ee/memory#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": nodes
    });

    Ok(Some(result))
}

/// Convert a value to `{"@value": text, "@type": "@fulltext"}`.
///
/// Handles:
/// - Plain string: `"text"` → `{"@value": "text", "@type": "@fulltext"}`
/// - Already an object with `@value`: extract the string, re-wrap as `@fulltext`
/// - Array of values: apply to each element
fn inject_fulltext_value(val: Value) -> Value {
    match val {
        Value::String(s) => json!({"@value": s, "@type": "@fulltext"}),
        Value::Object(map) => {
            // Extract @value if present, re-wrap as @fulltext
            if let Some(v) = map.get("@value") {
                let text = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                json!({"@value": text, "@type": "@fulltext"})
            } else {
                // Unexpected shape — return as-is
                Value::Object(map)
            }
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(inject_fulltext_value).collect()),
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the local part of a memory ID.
///
/// `"mem:fact-01abc"` → `"fact-01abc"`
/// `"https://ns.flur.ee/memory#fact-01abc"` → `"fact-01abc"`
/// `"fact-01abc"` → `"fact-01abc"` (already local)
fn mem_local_id(id: &str) -> &str {
    if let Some(rest) = id.strip_prefix("mem:") {
        rest
    } else if let Some(rest) = id.strip_prefix(vocab::MEM_NS) {
        rest
    } else {
        id
    }
}

/// Normalize Unicode quotation marks to their ASCII equivalents.
///
/// LLMs frequently produce smart/curly quotes (`"` `"` `'` `'` etc.) which are
/// not valid Turtle string delimiters. This replaces them with ASCII `"` or `'`
/// so that `escape_turtle_string` can then escape them properly.
pub fn normalize_unicode_quotes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            // Double quote variants → ASCII "
            '\u{201C}' | '\u{201D}' | '\u{201E}' | '\u{201F}' | '\u{00AB}' | '\u{00BB}' => {
                out.push('"');
            }
            // Single quote variants → ASCII '
            '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{201B}' => out.push('\''),
            c => out.push(c),
        }
    }
    out
}

/// Escape special characters for Turtle string literals.
///
/// First normalizes Unicode smart quotes to ASCII equivalents, then escapes:
/// `\` → `\\`, `"` → `\"`, newline → `\n`, tab → `\t`, carriage return → `\r`.
pub fn escape_turtle_string(s: &str) -> String {
    let normalized = normalize_unicode_quotes(s);
    let mut out = String::with_capacity(normalized.len());
    for ch in normalized.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

/// Return the path to `repo.ttl` within a memory directory.
pub fn repo_ttl_path(memory_dir: &Path) -> std::path::PathBuf {
    memory_dir.join("repo.ttl")
}

/// Return the path to `.local/user.ttl` within a memory directory.
pub fn user_ttl_path(memory_dir: &Path) -> std::path::PathBuf {
    memory_dir.join(".local").join("user.ttl")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Scope, Severity};

    fn make_test_memory() -> Memory {
        Memory {
            id: "mem:fact-01jdxyz0000000000000000".to_string(),
            kind: MemoryKind::Fact,
            content: "Run tests with: cargo nextest run --workspace".to_string(),
            tags: vec!["ci".to_string(), "testing".to_string()],
            scope: Scope::Repo,
            severity: None,
            artifact_refs: vec!["Cargo.toml".to_string()],
            branch: Some("main".to_string()),
            created_at: "2026-02-24T10:30:00+00:00".to_string(),
            rationale: None,
            alternatives: None,
        }
    }

    #[test]
    fn turtle_block_canonical_output() {
        let mem = make_test_memory();
        let block = memory_to_turtle_block(&mem);

        // Should start with subject + type
        assert!(block.starts_with("mem:fact-01jdxyz0000000000000000 a mem:Fact ;"));
        // Should contain content
        assert!(block.contains("mem:content \"Run tests with: cargo nextest run --workspace\""));
        // Tags should be sorted alphabetically (ci before testing)
        let ci_pos = block.find("mem:tag \"ci\"").unwrap();
        let testing_pos = block.find("mem:tag \"testing\"").unwrap();
        assert!(ci_pos < testing_pos);
        // Should end with " .\n"
        assert!(block.ends_with(" .\n"));
        // Should contain branch
        assert!(block.contains("mem:branch \"main\""));
    }

    #[test]
    fn turtle_block_with_severity() {
        let mut mem = make_test_memory();
        mem.kind = MemoryKind::Constraint;
        mem.severity = Some(Severity::Must);

        let block = memory_to_turtle_block(&mem);
        assert!(block.contains("mem:severity \"must\""));
    }

    #[test]
    fn escape_special_chars() {
        assert_eq!(escape_turtle_string("hello"), "hello");
        assert_eq!(escape_turtle_string("he said \"hi\""), "he said \\\"hi\\\"");
        assert_eq!(escape_turtle_string("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_turtle_string("path\\to"), "path\\\\to");
        assert_eq!(escape_turtle_string("a\tb"), "a\\tb");
    }

    #[test]
    fn normalize_smart_quotes() {
        // Double smart quotes → escaped ASCII "
        assert_eq!(
            escape_turtle_string("he said \u{201C}hi\u{201D}"),
            "he said \\\"hi\\\""
        );
        // Single smart quotes → ASCII '
        assert_eq!(escape_turtle_string("it\u{2019}s"), "it's");
        // Low-9 and guillemets
        assert_eq!(
            escape_turtle_string("\u{201E}quoted\u{201F}"),
            "\\\"quoted\\\""
        );
        assert_eq!(
            escape_turtle_string("\u{00AB}guillemets\u{00BB}"),
            "\\\"guillemets\\\""
        );
        // Mixed: smart quotes inside real content
        assert_eq!(
            escape_turtle_string("error: \u{201C}not found\u{201D}"),
            "error: \\\"not found\\\""
        );
    }

    #[test]
    fn mem_local_id_variants() {
        assert_eq!(mem_local_id("mem:fact-01abc"), "fact-01abc");
        assert_eq!(
            mem_local_id("https://ns.flur.ee/memory#fact-01abc"),
            "fact-01abc"
        );
        assert_eq!(mem_local_id("fact-01abc"), "fact-01abc");
    }

    #[test]
    fn write_and_read_memory_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.ttl");

        let mem1 = make_test_memory();
        let mut mem2 = make_test_memory();
        mem2.id = "mem:fact-01zzzz0000000000000000".to_string();
        mem2.content = "Second memory".to_string();
        mem2.tags = vec![];
        mem2.artifact_refs = vec![];
        mem2.branch = None;

        write_memory_file(&path, &[mem2.clone(), mem1.clone()], REPO_HEADER).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        // Should start with header
        assert!(content.starts_with("# Fluree Memory — repo-scoped"));
        // Should contain prefixes
        assert!(content.contains("@prefix mem:"));
        // mem2 (no branch → "") sorts before mem1 (branch "main")
        let pos1 = content.find("fact-01jdxyz").unwrap();
        let pos2 = content.find("fact-01zzzz").unwrap();
        assert!(
            pos2 < pos1,
            "memories should be sorted by (branch, id) — no-branch before 'main'"
        );
    }

    #[test]
    fn append_to_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.ttl");

        let mem = make_test_memory();
        append_memory_to_file(&path, &mem, REPO_HEADER).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("@prefix mem:"));
        assert!(content.contains("mem:fact-01jdxyz0000000000000000 a mem:Fact"));
    }

    #[test]
    fn append_to_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.ttl");

        let mem1 = make_test_memory();
        append_memory_to_file(&path, &mem1, REPO_HEADER).unwrap();

        let mut mem2 = make_test_memory();
        mem2.id = "mem:fact-01zzzz0000000000000000".to_string();
        mem2.content = "Another fact".to_string();
        append_memory_to_file(&path, &mem2, REPO_HEADER).unwrap();

        let content = fs::read_to_string(&path).unwrap();
        // Should have both memories
        assert!(content.contains("fact-01jdxyz"));
        assert!(content.contains("fact-01zzzz"));
        // Prefixes should appear only once
        assert_eq!(content.matches("@prefix mem:").count(), 1);
    }

    #[test]
    fn skip_write_if_identical() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stable.ttl");

        let mem = make_test_memory();
        write_memory_file(&path, std::slice::from_ref(&mem), REPO_HEADER).unwrap();

        let mtime1 = fs::metadata(&path).unwrap().modified().unwrap();
        // Small delay to ensure mtime would differ
        std::thread::sleep(std::time::Duration::from_millis(50));

        write_memory_file(&path, &[mem], REPO_HEADER).unwrap();

        let mtime2 = fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(mtime1, mtime2, "file should not be rewritten if identical");
    }

    #[test]
    fn inject_fulltext_on_plain_string() {
        let val = inject_fulltext_value(Value::String("hello".to_string()));
        assert_eq!(val, json!({"@value": "hello", "@type": "@fulltext"}));
    }

    #[test]
    fn inject_fulltext_on_typed_object() {
        let val = inject_fulltext_value(json!({"@value": "hello", "@type": "xsd:string"}));
        assert_eq!(val, json!({"@value": "hello", "@type": "@fulltext"}));
    }

    #[test]
    fn all_memory_kinds() {
        for (kind, expected_type) in [
            (MemoryKind::Fact, "mem:Fact"),
            (MemoryKind::Decision, "mem:Decision"),
            (MemoryKind::Constraint, "mem:Constraint"),
        ] {
            let mut mem = make_test_memory();
            mem.kind = kind;
            let block = memory_to_turtle_block(&mem);
            assert!(
                block.contains(&format!("a {expected_type}")),
                "kind {kind:?} should produce type {expected_type}"
            );
        }
    }

    #[test]
    fn decision_with_rationale_and_alternatives() {
        let mut mem = make_test_memory();
        mem.kind = MemoryKind::Decision;
        mem.rationale = Some("Performance reasons".to_string());
        mem.alternatives = Some("Could have used X or Y".to_string());

        let block = memory_to_turtle_block(&mem);
        assert!(block.contains("mem:rationale \"Performance reasons\""));
        assert!(block.contains("mem:alternatives \"Could have used X or Y\""));
    }
}
