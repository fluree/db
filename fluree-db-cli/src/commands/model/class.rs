//! `fluree model class` — the reasoning facet's vocabulary.
//!
//! Defines classes and their `rdfs:subClassOf` relations as ordinary data.
//! When RDFS entailment is enabled on a dataset, the engine follows the
//! hierarchy in BOTH query and policy: a view policy on `ex:Contact`
//! covers `ex:Lead` if `ex:Lead rdfs:subClassOf ex:Contact` — which is why
//! the hierarchy lives under governance tooling and not ad-hoc transacts.

use serde_json::{json, Value};

use super::{iri_rows, query, require_absolute_iri, resolve_mode, update, upsert};
use crate::cli::ModelClassAction;
use crate::error::CliResult;
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
            clear_subclass_of,
            label,
            dry_run,
            remote,
        } => {
            run_define(
                dataset,
                class,
                subclass_of,
                *clear_subclass_of,
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
    clear_subclass_of: bool,
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
    if clear_subclass_of {
        println!("Subclass of: (clearing all parents)");
    } else if !subclass_of.is_empty() {
        println!("Subclass of: {}", subclass_of.join(", "));
        println!(
            "  note: with RDFS entailment enabled, queries and policies targeting a\n\
             \x20 parent class also cover this class — a widened hierarchy widens\n\
             \x20 every grant on the parent."
        );
    }

    if dry_run {
        println!("\n-- dry run; compiled JSON-LD --");
        if clear_subclass_of {
            println!(
                "{}",
                serde_json::to_string_pretty(&clear_parents_txn(class, label, &node))?
            );
        } else {
            println!("{}", serde_json::to_string_pretty(&node)?);
        }
        return Ok(());
    }

    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    if clear_subclass_of {
        update(&mode, &clear_parents_txn(class, label, &node)).await?;
    } else {
        upsert(&mode, &node).await?;
    }
    println!(
        "Defined on '{dataset}'. Re-running with --subclass-of replaces the \
         parent set (list every parent each time); --clear-subclass-of \
         removes all parents."
    );
    Ok(())
}

/// Transaction for `--clear-subclass-of`: `upsert` can only replace listed
/// properties, so removing the last parent needs an explicit delete of
/// every `rdfs:subClassOf` edge (and the old label when re-labeling, since
/// the insert side is additive), atomically with writing the node. This
/// matters under RDFS entailment: a stale parent widens every grant on it.
fn clear_parents_txn(class: &str, label: Option<&str>, node: &Value) -> Value {
    let mut where_clause = vec![json!([
        "optional",
        {"@id": class, RDFS_SUBCLASS_OF: "?parent"}
    ])];
    let mut delete = vec![json!({"@id": class, RDFS_SUBCLASS_OF: "?parent"})];
    if label.is_some() {
        where_clause.push(json!(["optional", {"@id": class, RDFS_LABEL: "?oldLabel"}]));
        delete.push(json!({"@id": class, RDFS_LABEL: "?oldLabel"}));
    }
    json!({"where": where_clause, "delete": delete, "insert": node})
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
    let domain = domain_classes(&rows, &policy_classes);

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

/// Domain vocabulary only: drop rows whose id is a policy class the access
/// compiler minted (governance plumbing, not user classes).
fn domain_classes<'a>(rows: &'a [Value], policy_classes: &[String]) -> Vec<&'a Value> {
    rows.iter()
        .filter(|row| {
            row["@id"]
                .as_str()
                .is_some_and(|id| !policy_classes.iter().any(|pc| pc == id))
        })
        .collect()
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
    fn clear_parents_txn_deletes_all_subclass_edges() {
        // `upsert` cannot remove the last parent (compile omits
        // rdfs:subClassOf when empty) — the clear txn must delete
        // explicitly, or a stale parent keeps widening grants under RDFS
        // entailment.
        let node = compile("https://example.org/Lead", &[], None);
        let txn = clear_parents_txn("https://example.org/Lead", None, &node);

        let where_clause = txn["where"].as_array().unwrap();
        assert_eq!(where_clause.len(), 1);
        assert_eq!(where_clause[0][0], "optional");
        assert_eq!(where_clause[0][1][RDFS_SUBCLASS_OF], "?parent");

        let delete = txn["delete"].as_array().unwrap();
        assert_eq!(delete.len(), 1);
        assert_eq!(delete[0]["@id"], "https://example.org/Lead");
        assert_eq!(delete[0][RDFS_SUBCLASS_OF], "?parent");

        assert_eq!(txn["insert"]["@id"], "https://example.org/Lead");
        assert!(txn["insert"].get(RDFS_SUBCLASS_OF).is_none());
    }

    #[test]
    fn clear_parents_txn_replaces_label_when_given() {
        // The insert side is a plain (additive) insert, so re-labeling in
        // the same run must delete the old label too.
        let node = compile("https://example.org/Lead", &[], Some("Lead v2"));
        let txn = clear_parents_txn("https://example.org/Lead", Some("Lead v2"), &node);
        let delete = txn["delete"].as_array().unwrap();
        assert_eq!(delete.len(), 2);
        assert_eq!(delete[1][RDFS_LABEL], "?oldLabel");
    }

    #[test]
    fn show_excludes_policy_classes() {
        let rows = vec![
            serde_json::json!({"@id": "https://example.org/Lead"}),
            serde_json::json!({"@id": "https://example.org/Lead/access/write"}),
        ];
        let policy = vec!["https://example.org/Lead/access/write".to_string()];
        let domain = domain_classes(&rows, &policy);
        assert_eq!(domain.len(), 1);
        assert_eq!(domain[0]["@id"], "https://example.org/Lead");
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
