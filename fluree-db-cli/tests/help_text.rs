//! Help-text gate for the clap command tree.
//!
//! A variant's `///` doc comment becomes its `about` / `long_about`, so a doc
//! block that drifts onto the wrong variant leaves one command blank in
//! `fluree --help` and gives another a neighbour's description. Both checks
//! walk every non-hidden command at any depth.

use clap::CommandFactory;
use fluree_db_cli::cli::Cli;

fn walk(cmd: &clap::Command, prefix: &[String], out: &mut Vec<(String, clap::Command)>) {
    for sub in cmd.get_subcommands() {
        if sub.is_hide_set() || sub.get_name() == "help" {
            continue;
        }
        let mut path = prefix.to_vec();
        path.push(sub.get_name().to_string());
        out.push((path.join(" "), sub.clone()));
        walk(sub, &path, out);
    }
}

fn visible_commands() -> Vec<(String, clap::Command)> {
    let mut out = Vec::new();
    walk(&Cli::command(), &[], &mut out);
    out
}

#[test]
fn every_command_has_an_about() {
    let missing: Vec<String> = visible_commands()
        .into_iter()
        .filter(|(_, c)| c.get_about().is_none())
        .map(|(path, _)| path)
        .collect();
    assert!(
        missing.is_empty(),
        "commands with no help description: {missing:?}"
    );
}

/// clap reflows doc comments into paragraphs unless the item carries
/// `verbatim_doc_comment`, which collapses an indented example block onto the
/// `Examples:` line. That also blinds `examples_invoke_their_own_command`.
#[test]
fn example_blocks_keep_their_line_breaks() {
    let mut reflowed = Vec::new();
    for (path, cmd) in visible_commands() {
        let mut texts: Vec<String> = cmd
            .get_long_about()
            .map(ToString::to_string)
            .into_iter()
            .collect();
        texts.extend(
            cmd.get_arguments()
                .filter_map(|a| a.get_long_help())
                .map(ToString::to_string),
        );
        if texts
            .iter()
            .flat_map(|t| t.lines())
            .any(|l| l.trim_start().starts_with("Examples:") && l.trim() != "Examples:")
        {
            reflowed.push(path);
        }
    }
    assert!(
        reflowed.is_empty(),
        "example blocks reflowed onto one line (add verbatim_doc_comment): {reflowed:?}"
    );
}

#[test]
fn examples_invoke_their_own_command() {
    let mut stray = Vec::new();
    for (path, cmd) in visible_commands() {
        let Some(long) = cmd.get_long_about() else {
            continue;
        };
        // Examples may show setup steps (`fluree config set ...`), so only the
        // first invocation of each block has to be the command's own.
        let own = format!("fluree {path}");
        let long = long.to_string();
        let mut in_examples = false;
        for line in long.lines().map(str::trim) {
            if line == "Examples:" {
                in_examples = true;
            } else if in_examples && line.starts_with("fluree ") {
                in_examples = false;
                if !line.starts_with(&own) {
                    stray.push(format!("{path}: {line}"));
                }
            }
        }
    }
    assert!(
        stray.is_empty(),
        "help examples that invoke a different command (misplaced doc comment?):\n{}",
        stray.join("\n")
    );
}
