//! Structural drift gate: the CLI command tree and `docs/cli/` must stay 1:1.
//!
//! `docs/` is embedded into the binary (`fluree docs` + the MCP docs toolset)
//! and is the reference layer that dependent surfaces — fluree/solo's served
//! docs, the fluree-cli Claude Code plugin — deliberately defer to instead of
//! copying command reference. That deference is only safe while the corpus is
//! complete, so this suite fails when a command ships undocumented, a doc
//! outlives its command, the index skips a page, or the published book (the
//! `SUMMARY.md` TOC) orphans one.
//!
//! Runs under whatever features the test build enables; CI runs
//! `--all-features`, so feature-gated commands (validate/cluster/iceberg) are
//! present and their docs are required.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use clap::CommandFactory;
use fluree_db_cli::cli::Cli;

fn docs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs")
}

/// Non-hidden top-level subcommands (hidden ones — deprecated shims,
/// machine plumbing like `__manifest` — are not documentable surface).
fn visible_top_level() -> Vec<clap::Command> {
    Cli::command()
        .get_subcommands()
        .filter(|c| !c.is_hide_set() && c.get_name() != "help")
        .cloned()
        .collect()
}

/// Every non-hidden descendant subcommand name under `cmd`, any depth.
fn descendant_names(cmd: &clap::Command, out: &mut BTreeSet<String>) {
    for sub in cmd.get_subcommands() {
        if sub.is_hide_set() || sub.get_name() == "help" {
            continue;
        }
        out.insert(sub.get_name().to_string());
        descendant_names(sub, out);
    }
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

#[test]
fn every_command_has_a_doc_page() {
    let cli_docs = docs_dir().join("cli");
    let missing: Vec<String> = visible_top_level()
        .iter()
        .map(|c| c.get_name().to_string())
        .filter(|name| !cli_docs.join(format!("{name}.md")).exists())
        .collect();
    assert!(
        missing.is_empty(),
        "commands with no docs/cli/<name>.md page: {missing:?}"
    );
}

#[test]
fn every_doc_page_has_a_command() {
    // Pages that are deliberately not command docs.
    const NON_COMMAND_PAGES: &[&str] = &["README.md", "server-integration.md"];

    let commands: BTreeSet<String> = visible_top_level()
        .iter()
        .map(|c| c.get_name().to_string())
        .collect();

    let mut orphans = Vec::new();
    for entry in std::fs::read_dir(docs_dir().join("cli")).expect("docs/cli readable") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy().to_string();
        if !name.ends_with(".md") || NON_COMMAND_PAGES.contains(&name.as_str()) {
            continue;
        }
        let stem = name.trim_end_matches(".md");
        if !commands.contains(stem) {
            orphans.push(name);
        }
    }
    assert!(
        orphans.is_empty(),
        "docs/cli pages with no matching command (renamed or removed?): {orphans:?}"
    );
}

#[test]
fn every_nested_action_is_mentioned_in_its_doc() {
    // Presence check, not prose quality: the doc for `fluree branch` must at
    // least mention every visible action (`revert`, ...). Catches the class
    // of gap where an action ships and its parent page never learns.
    let cli_docs = docs_dir().join("cli");
    let mut gaps = Vec::new();
    for top in visible_top_level() {
        let doc_path = cli_docs.join(format!("{}.md", top.get_name()));
        if !doc_path.exists() {
            continue; // every_command_has_a_doc_page reports this
        }
        let doc = read(&doc_path);
        let mut actions = BTreeSet::new();
        descendant_names(&top, &mut actions);
        for action in actions {
            if !doc.contains(&action) {
                gaps.push(format!("{} — `{}`", top.get_name(), action));
            }
        }
    }
    assert!(
        gaps.is_empty(),
        "subcommands never mentioned in their parent's docs/cli page: {gaps:?}"
    );
}

#[test]
fn readme_indexes_every_command() {
    let readme = read(&docs_dir().join("cli/README.md"));
    let missing: Vec<String> = visible_top_level()
        .iter()
        .map(|c| c.get_name().to_string())
        .filter(|name| !readme.contains(&format!("({name}.md)")))
        .collect();
    assert!(
        missing.is_empty(),
        "docs/cli/README.md does not link these command pages: {missing:?}"
    );
}

#[test]
fn summary_includes_every_cli_doc_page() {
    // The published mdBook renders only what SUMMARY.md lists; the embedded
    // corpus embeds the whole folder. A page missing here is invisible on the
    // website while still being served to agents — the worst kind of split.
    let summary = read(&docs_dir().join("SUMMARY.md"));
    let mut orphans = Vec::new();
    for entry in std::fs::read_dir(docs_dir().join("cli")).expect("docs/cli readable") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy().to_string();
        if !name.ends_with(".md") {
            continue;
        }
        if !summary.contains(&format!("cli/{name}")) {
            orphans.push(name);
        }
    }
    assert!(
        orphans.is_empty(),
        "docs/cli pages missing from docs/SUMMARY.md (invisible in the published book): {orphans:?}"
    );
}
