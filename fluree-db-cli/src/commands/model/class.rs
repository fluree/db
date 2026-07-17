//! `fluree model class` — the reasoning facet's vocabulary.
//!
//! Defines classes and their `rdfs:subClassOf` relations as ordinary data.
//! When RDFS entailment is enabled on a dataset, the engine follows the
//! hierarchy in BOTH query and policy: a view policy on `ex:Contact`
//! covers `ex:Lead` if `ex:Lead rdfs:subClassOf ex:Contact` — which is why
//! the hierarchy lives under governance tooling and not ad-hoc transacts.

use serde_json::{json, Value};

use super::{query, resolve_mode, upsert};
use crate::cli::ModelClassAction;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

const F: &str = "https://ns.flur.ee/db#";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

pub async fn run(action: &ModelClassAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        ModelClassAction::Define {
            dataset,
            class,
            subclass_of,
            label,
            dry_run,
            remote,
        } => {
            run_define(
                dataset,
                class,
                subclass_of,
                label.as_deref(),
                *dry_run,
                remote.as_deref(),
                dirs,
                direct,
            )
            .await
        }
        ModelClassAction::Show { dataset, remote } => {
            run_show(dataset, remote.as_deref(), dirs, direct).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_define(
    dataset: &str,
    class: &str,
    subclass_of: &[String],
    label: Option<&str>,
    dry_run: bool,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    require_absolute_iri("--class", class)?;
    for parent in subclass_of {
        require_absolute_iri("--subclass-of", parent)?;
    }

    let node = compile(class, subclass_of, label);

    println!("Class:      {class}");
    if !subclass_of.is_empty() {
        println!("Subclass of: {}", subclass_of.join(", "));
        println!(
            "  note: with RDFS entailment enabled, queries and policies targeting a\n\
             \x20 parent class also cover this class — a widened hierarchy widens\n\
             \x20 every grant on the parent."
        );
    }

    if dry_run {
        println!("\n-- dry run; compiled JSON-LD --");
        println!("{}", serde_json::to_string_pretty(&node)?);
        return Ok(());
    }

    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    upsert(&mode, &node).await?;
    println!(
        "Defined on '{dataset}'. Re-running with --subclass-of replaces the \
         parent set (list every parent each time)."
    );
    Ok(())
}

/// Compile the class node. `upsert` replaces the listed properties, so
/// re-defining with a different parent set replaces the hierarchy edge
/// rather than accumulating stale ones.
fn compile(class: &str, subclass_of: &[String], label: Option<&str>) -> Value {
    let mut node = json!({
        "@id": class,
        "@type": RDFS_CLASS,
    });
    if let Some(l) = label {
        node[RDFS_LABEL] = json!(l);
    }
    if !subclass_of.is_empty() {
        node[RDFS_SUBCLASS_OF] = json!(subclass_of
            .iter()
            .map(|p| json!({"@id": p}))
            .collect::<Vec<_>>());
    }
    node
}

async fn run_show(
    dataset: &str,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;

    // Policy classes are rdfs:Class too (the access compiler mints them);
    // they're governance plumbing, not domain vocabulary — exclude them.
    // There is no stored intent to consult: a policy class is recognized
    // by its role, the extra `@type` the compiler puts on its
    // `f:AccessPolicy` nodes.
    let policy_classes: Vec<String> = {
        let q = json!({
            "@context": {"f": F},
            "select": ["?class"],
            "where": [
                {"@id": "?p", "@type": "f:AccessPolicy"},
                {"@id": "?p", "@type": "?class"}
            ]
        });
        iri_rows(&query(&mode, &q).await?)
    };

    let q = json!({
        "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
        "select": {"?class": ["*"]},
        "where": [{"@id": "?class", "@type": "rdfs:Class"}]
    });
    let result = query(&mode, &q).await?;
    let rows = result.as_array().cloned().unwrap_or_default();
    let domain: Vec<&Value> = rows
        .iter()
        .filter(|row| {
            row["@id"]
                .as_str()
                .is_some_and(|id| !policy_classes.iter().any(|pc| pc == id))
        })
        .collect();

    if domain.is_empty() {
        println!("No classes defined on '{dataset}'.");
        println!("Define one: fluree model class define {dataset} --class <iri>");
        return Ok(());
    }
    println!("Classes on '{dataset}':\n");
    for row in domain {
        let id = row["@id"].as_str().unwrap_or("-");
        println!("• {id}");
        if let Some(label) = row.get("rdfs:label").map(render_value).filter(|s| s != "-") {
            println!("    label:       {label}");
        }
        let parents = ids_of(row.get("rdfs:subClassOf"));
        if !parents.is_empty() {
            println!("    subclass of: {}", parents.join(", "));
        }
    }
    Ok(())
}

fn require_absolute_iri(flag: &str, v: &str) -> CliResult<()> {
    if v.starts_with("http://") || v.starts_with("https://") || v.starts_with("urn:") {
        Ok(())
    } else {
        Err(CliError::Usage(format!(
            "{flag} must be an absolute IRI (got '{v}') — e.g. https://example.org/Lead"
        )))
    }
}

fn render_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => o
            .get("@id")
            .and_then(|x| x.as_str())
            .unwrap_or("-")
            .to_string(),
        Value::Array(items) => items
            .iter()
            .map(render_value)
            .collect::<Vec<_>>()
            .join(", "),
        other => other.to_string(),
    }
}

fn ids_of(v: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(v) = v else { return out };
    let items: Vec<&Value> = match v {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    for item in items {
        match item {
            Value::String(s) => out.push(s.clone()),
            Value::Object(o) => {
                if let Some(id) = o.get("@id").and_then(|x| x.as_str()) {
                    out.push(id.to_string());
                }
            }
            _ => {}
        }
    }
    out
}

fn iri_rows(result: &Value) -> Vec<String> {
    let rows = match result {
        Value::Array(rows) => rows.as_slice(),
        _ => return vec![],
    };
    rows.iter()
        .filter_map(|row| match row {
            Value::String(s) => Some(s.clone()),
            Value::Array(inner) => inner.first().and_then(|v| v.as_str()).map(String::from),
            Value::Object(o) => o.get("@id").and_then(|v| v.as_str()).map(String::from),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_minimal_class() {
        let node = compile("https://example.org/Lead", &[], None);
        assert_eq!(node["@id"], "https://example.org/Lead");
        assert_eq!(node["@type"], RDFS_CLASS);
        assert!(node.get(RDFS_SUBCLASS_OF).is_none());
    }

    #[test]
    fn compile_with_hierarchy_and_label() {
        let parents = vec!["https://example.org/Contact".to_string()];
        let node = compile("https://example.org/Lead", &parents, Some("Lead"));
        assert_eq!(node[RDFS_LABEL], "Lead");
        assert_eq!(
            node[RDFS_SUBCLASS_OF][0]["@id"],
            "https://example.org/Contact"
        );
    }

    #[test]
    fn ids_of_handles_single_and_array_forms() {
        assert_eq!(
            ids_of(Some(&serde_json::json!({"@id": "https://x/A"}))),
            vec!["https://x/A"]
        );
        assert_eq!(
            ids_of(Some(&serde_json::json!([
                {"@id": "https://x/A"},
                "https://x/B"
            ]))),
            vec!["https://x/A", "https://x/B"]
        );
        assert!(ids_of(None).is_empty());
    }
}
