//! `fluree __manifest` — machine-readable manifest of this binary's CLI
//! surface, generated from the clap definitions so it cannot drift from the
//! binary. Hidden: consumed by CI in dependent repos (fluree/solo,
//! fluree/claude-plugins) to validate the `fluree ...` command strings they
//! ship, and published as a release asset (`fluree-cli-manifest.json`) so
//! consumers validate against a release without building it.
//!
//! Structure only — command paths, flags, value enums, positionals, compiled
//! features. Deliberately no help text: prose belongs to `--help` and the
//! embedded docs, and including it would churn the manifest on every wording
//! edit while tempting consumers to copy reference content instead of
//! deferring to the binary.

use std::path::Path;

use clap::CommandFactory;
use serde_json::{json, Value};

use crate::error::{CliError, CliResult};

/// Manifest schema version. Bump on breaking shape changes; consumers must
/// tolerate added fields.
const MANIFEST_VERSION: u32 = 1;

pub fn run(output: Option<&Path>) -> CliResult<()> {
    let manifest = build_manifest();
    // Infallible: a serde_json::Value has no non-string keys or fallible
    // serializers.
    let rendered =
        serde_json::to_string_pretty(&manifest).expect("manifest Value serializes");
    match output {
        Some(path) => {
            std::fs::write(path, rendered)
                .map_err(|e| CliError::Input(format!("failed to write {}: {e}", path.display())))?;
            eprintln!("wrote {}", path.display());
        }
        None => println!("{rendered}"),
    }
    Ok(())
}

fn build_manifest() -> Value {
    let cmd = crate::cli::Cli::command();

    // Cargo features compiled into this binary that add or remove whole
    // commands — lets a consumer distinguish "not in this build" from
    // "does not exist".
    let features: Vec<&str> = [
        #[cfg(feature = "server")]
        "server",
        #[cfg(feature = "iceberg")]
        "iceberg",
        #[cfg(feature = "shacl")]
        "shacl",
        #[cfg(feature = "aws")]
        "aws",
    ]
    .to_vec();

    let mut commands = Vec::new();
    walk(&cmd, &mut Vec::new(), &mut commands);

    json!({
        "manifest_version": MANIFEST_VERSION,
        "name": cmd.get_name(),
        "version": env!("CARGO_PKG_VERSION"),
        "features": features,
        "global_flags": args_json(&cmd, ArgSet::Global),
        "commands": commands,
    })
}

fn walk(cmd: &clap::Command, path: &mut Vec<String>, out: &mut Vec<Value>) {
    for sub in cmd.get_subcommands() {
        // Hidden commands (deprecated shims, machine plumbing like this one)
        // are not part of the teachable surface.
        if sub.is_hide_set() || sub.get_name() == "help" {
            continue;
        }
        path.push(sub.get_name().to_string());
        let aliases: Vec<&str> = sub.get_visible_aliases().collect();
        out.push(json!({
            "path": path.clone(),
            "aliases": aliases,
            "flags": args_json(sub, ArgSet::Flags),
            "positionals": args_json(sub, ArgSet::Positionals),
            "has_subcommands": sub.get_subcommands().any(|s| !s.is_hide_set()),
        }));
        walk(sub, path, out);
        path.pop();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ArgSet {
    /// Global flags, listed once at the top level (clap propagates them to
    /// every subcommand at parse time).
    Global,
    /// A subcommand's own non-positional, non-global flags.
    Flags,
    /// A subcommand's positional arguments.
    Positionals,
}

fn args_json(cmd: &clap::Command, set: ArgSet) -> Vec<Value> {
    cmd.get_arguments()
        .filter(|a| !a.is_hide_set())
        .filter(|a| {
            let id = a.get_id().as_str();
            id != "help" && id != "version"
        })
        .filter(|a| match set {
            ArgSet::Global => a.is_global_set(),
            ArgSet::Flags => !a.is_positional() && !a.is_global_set(),
            ArgSet::Positionals => a.is_positional(),
        })
        .map(|a| {
            let possible: Vec<String> = a
                .get_possible_values()
                .iter()
                .filter(|p| !p.is_hide_set())
                .map(|p| p.get_name().to_string())
                .collect();
            if a.is_positional() {
                json!({
                    "name": a.get_id().as_str(),
                    "required": a.is_required_set(),
                    "multiple": matches!(a.get_action(), clap::ArgAction::Append),
                    "possible_values": possible,
                })
            } else {
                json!({
                    "long": a.get_long(),
                    "short": a.get_short().map(|c| c.to_string()),
                    "takes_value": a.get_action().takes_values(),
                    "required": a.is_required_set(),
                    "possible_values": possible,
                })
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Value {
        build_manifest()
    }

    fn paths(m: &Value) -> Vec<Vec<String>> {
        m["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                c["path"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| p.as_str().unwrap().to_string())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn manifest_has_core_commands() {
        let m = manifest();
        assert_eq!(m["manifest_version"], 1);
        assert_eq!(m["name"], "fluree");
        let ps = paths(&m);
        assert!(ps.contains(&vec!["query".to_string()]));
        assert!(ps.contains(&vec![
            "model".to_string(),
            "entity".to_string(),
            "define".to_string()
        ]));
        assert!(ps.contains(&vec!["auth".to_string(), "token".to_string()]));
    }

    #[test]
    fn manifest_excludes_hidden_commands() {
        let m = manifest();
        for p in paths(&m) {
            assert!(
                !p.iter().any(|seg| seg.starts_with("__")),
                "hidden machine command leaked into manifest: {p:?}"
            );
        }
    }

    #[test]
    fn manifest_carries_value_enums_and_globals() {
        let m = manifest();
        let commands = m["commands"].as_array().unwrap();
        let query = commands
            .iter()
            .find(|c| c["path"] == json!(["query"]))
            .expect("query command");
        // query --format is a plain string arg today (values validated at
        // runtime), so the manifest correctly reports no value enum — just
        // assert the flag itself is present.
        assert!(
            query["flags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f["long"] == "format"),
            "query --format missing"
        );
        // init --format IS a clap value enum — its values must surface.
        let init = commands
            .iter()
            .find(|c| c["path"] == json!(["init"]))
            .expect("init command");
        let format = init["flags"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["long"] == "format")
            .expect("init --format");
        let values: Vec<&str> = format["possible_values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(values.contains(&"toml"), "init --format values: {values:?}");

        let globals: Vec<&str> = m["global_flags"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|f| f["long"].as_str())
            .collect();
        assert!(globals.contains(&"memory-budget-mb"), "globals: {globals:?}");
    }

    #[test]
    fn manifest_reflects_compiled_features() {
        let m = manifest();
        let ps = paths(&m);
        let has_validate = ps.contains(&vec!["validate".to_string()]);
        assert_eq!(
            has_validate,
            cfg!(feature = "shacl"),
            "validate presence must track the shacl feature"
        );
    }
}
