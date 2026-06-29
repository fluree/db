//! Proves the memory file's git-merge behavior under a **default** merge (no
//! special `.gitattributes`):
//!   1. two branches each adding a memory in different `(branch, id)` regions
//!      merge cleanly, and
//!   2. two branches changing the *same* memory correctly CONFLICT — the safety
//!      property that a blanket `merge=union` would have silently destroyed.
//!
//! See `turtle_io::insert_memory_into_file` (the sorted splice used by `add`)
//! and `turtle_io::write_memory_file` (the rewrite used by update/forget).

use fluree_db_memory::turtle_io::{
    insert_memory_into_file, repo_ttl_path, write_memory_file, REPO_HEADER,
};
use fluree_db_memory::{Memory, MemoryKind, Scope};
use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run git")
}

fn git_ok(dir: &Path, args: &[&str]) {
    let out = git(dir, args);
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn git_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn mem(id: &str, content: &str, branch: &str) -> Memory {
    Memory {
        id: format!("mem:fact-{id}"),
        kind: MemoryKind::Fact,
        content: content.to_string(),
        tags: vec!["t".to_string()],
        scope: Scope::Repo,
        severity: None,
        artifact_refs: vec![],
        branch: Some(branch.to_string()),
        created_at: "2026-06-26T00:00:00+00:00".to_string(),
        rationale: None,
        alternatives: None,
    }
}

fn init_repo(root: &Path) {
    git_ok(root, &["init", "-q"]);
    git_ok(root, &["config", "user.email", "t@t"]);
    git_ok(root, &["config", "user.name", "t"]);
    git_ok(root, &["config", "commit.gpgsign", "false"]);
}

/// Two branches each add a memory in different `(branch, id)` regions → clean
/// merge under the default driver, both memories present exactly once.
#[test]
fn distinct_adds_merge_cleanly_under_default_merge() {
    if !git_available() {
        eprintln!("git not available — skipping merge test");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let mem_dir = root.join(".fluree-memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    let ttl = repo_ttl_path(&mem_dir);
    init_repo(root);

    // Base: a few memories on branch "main" so the regions are well separated.
    write_memory_file(
        &ttl,
        &[
            mem("00000000000000000000000001", "base one", "main"),
            mem("00000000000000000000000002", "base two", "main"),
        ],
        REPO_HEADER,
    )
    .unwrap();
    git_ok(root, &["add", "-A"]);
    git_ok(root, &["commit", "-qm", "base"]);
    let base = String::from_utf8(git(root, &["branch", "--show-current"]).stdout)
        .unwrap()
        .trim()
        .to_string();

    // feat adds a memory on branch "aaa" → sorts to the very front.
    git_ok(root, &["checkout", "-q", "-b", "feat"]);
    insert_memory_into_file(
        &ttl,
        &mem("aaaaaaaaaaaaaaaaaaaaaaaaaa", "from feat", "aaa"),
        REPO_HEADER,
    )
    .unwrap();
    git_ok(root, &["commit", "-qam", "add on feat"]);

    // base adds a memory on branch "zzz" → sorts to the very end.
    git_ok(root, &["checkout", "-q", &base]);
    insert_memory_into_file(
        &ttl,
        &mem("zzzzzzzzzzzzzzzzzzzzzzzzzz", "from main", "zzz"),
        REPO_HEADER,
    )
    .unwrap();
    git_ok(root, &["commit", "-qam", "add on main"]);

    let merge = git(root, &["merge", "--no-edit", "feat"]);
    let merged = std::fs::read_to_string(&ttl).unwrap();
    assert!(
        merge.status.success(),
        "distinct-region adds should auto-merge; stderr={} file=\n{merged}",
        String::from_utf8_lossy(&merge.stderr)
    );
    assert!(!merged.contains("<<<<<<<") && !merged.contains(">>>>>>>"));
    assert_eq!(
        merged.matches("fact-aaaa").count(),
        1,
        "feat memory present once"
    );
    assert_eq!(
        merged.matches("fact-zzzz").count(),
        1,
        "main memory present once"
    );
    assert_eq!(merged.matches("@prefix mem:").count(), 1, "prefixes once");
}

/// Two branches change the SAME memory → the default merge MUST conflict, so the
/// change is never silently merged/corrupted (the union-merge failure mode).
#[test]
fn concurrent_same_memory_edit_conflicts() {
    if !git_available() {
        eprintln!("git not available — skipping merge test");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let mem_dir = root.join(".fluree-memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    let ttl = repo_ttl_path(&mem_dir);
    init_repo(root);

    let base_mems = |content: &str| {
        vec![
            mem("00000000000000000000000001", "keeper", "main"),
            mem("mmmmmmmmmmmmmmmmmmmmmmmmmm", content, "main"),
        ]
    };
    write_memory_file(&ttl, &base_mems("original M"), REPO_HEADER).unwrap();
    git_ok(root, &["add", "-A"]);
    git_ok(root, &["commit", "-qm", "base"]);
    let base = String::from_utf8(git(root, &["branch", "--show-current"]).stdout)
        .unwrap()
        .trim()
        .to_string();

    git_ok(root, &["checkout", "-q", "-b", "feat"]);
    write_memory_file(&ttl, &base_mems("edited on feat"), REPO_HEADER).unwrap();
    git_ok(root, &["commit", "-qam", "edit M on feat"]);

    git_ok(root, &["checkout", "-q", &base]);
    write_memory_file(&ttl, &base_mems("edited on main"), REPO_HEADER).unwrap();
    git_ok(root, &["commit", "-qam", "edit M on main"]);

    let merge = git(root, &["merge", "--no-edit", "feat"]);
    assert!(
        !merge.status.success(),
        "concurrent edits of the same memory must conflict, not silently merge"
    );
    let conflicted = std::fs::read_to_string(&ttl).unwrap();
    assert!(
        conflicted.contains("<<<<<<<"),
        "the conflict should be surfaced with markers for manual resolution"
    );
}
