//! `fluree model entity` — entity definitions as SHACL shapes.
//!
//! The entity shape is the governance model's single source of truth: the
//! property list is written ONCE here. Validation enforces it (use
//! `--closed` so instances may carry ONLY declared properties), SDK types /
//! form fields can be generated from it, and access policies stay thin
//! class-verb grants because the shape owns the property surface.
//!
//! Enforcement note: Fluree runs SHACL at transaction time once any shapes
//! exist in a ledger (reject mode by default — the "shapes-exist"
//! heuristic; a config graph can override per graph). Defining an entity is
//! therefore not just documentation: it activates validation for its class.

use serde_json::{json, Value};

use super::{query, resolve_mode, upsert};
use crate::cli::ModelEntityAction;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

const SH: &str = "http://www.w3.org/ns/shacl#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";

/// One parsed `--property` spec.
#[derive(Debug, PartialEq)]
struct PropertySpec {
    path: String,
    /// XSD datatype IRI, or None for untyped / IRI-kind properties.
    datatype: Option<String>,
    /// `sh:nodeKind sh:IRI` (the `iri` type keyword).
    iri_kind: bool,
    required: bool,
    /// Allowed literal values (`in[a,b,c]`).
    values_in: Vec<String>,
}

/// Parse `"<iri> [type] [required] [in[v1,v2,...]]"`.
fn parse_property_spec(spec: &str) -> CliResult<PropertySpec> {
    let mut tokens = spec.split_whitespace();
    let path = tokens
        .next()
        .ok_or_else(|| CliError::Usage("empty --property spec".into()))?
        .to_string();
    if !(path.starts_with("http://") || path.starts_with("https://") || path.starts_with("urn:")) {
        return Err(CliError::Usage(format!(
            "--property must start with an absolute IRI (got '{path}')"
        )));
    }

    let mut out = PropertySpec {
        path,
        datatype: None,
        iri_kind: false,
        required: false,
        values_in: vec![],
    };
    for token in tokens {
        match token {
            "string" => out.datatype = Some(format!("{XSD}string")),
            "integer" => out.datatype = Some(format!("{XSD}integer")),
            "decimal" => out.datatype = Some(format!("{XSD}decimal")),
            "boolean" => out.datatype = Some(format!("{XSD}boolean")),
            "date" => out.datatype = Some(format!("{XSD}date")),
            "datetime" => out.datatype = Some(format!("{XSD}dateTime")),
            "iri" => out.iri_kind = true,
            "required" => out.required = true,
            t if t.starts_with("in[") && t.ends_with(']') => {
                out.values_in = t[3..t.len() - 1]
                    .split(',')
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect();
                if out.values_in.is_empty() {
                    return Err(CliError::Usage(format!(
                        "in[...] must list at least one value (in '{spec}')"
                    )));
                }
            }
            other => {
                return Err(CliError::Usage(format!(
                    "unknown token '{other}' in --property spec '{spec}' \
                     (expected a type, 'required', or 'in[v1,v2]')"
                )));
            }
        }
    }
    Ok(out)
}

/// Last path segment of an IRI, for stable child-node ids.
fn local_name(iri: &str) -> &str {
    iri.rsplit(['/', '#', ':']).next().unwrap_or(iri)
}

/// Compile the entity definition into its JSON-LD artifacts.
fn compile(entity: &str, label: Option<&str>, specs: &[PropertySpec], closed: bool) -> Value {
    let shape_iri = format!("{}/shape", entity.trim_end_matches('/'));

    let mut class_node = json!({
        "@id": entity,
        "@type": RDFS_CLASS,
    });
    if let Some(l) = label {
        class_node[RDFS_LABEL] = json!(l);
    }

    let property_shapes: Vec<Value> = specs
        .iter()
        .map(|p| {
            let mut node = json!({
                "@id": format!("{shape_iri}/{}", local_name(&p.path)),
                format!("{SH}path"): {"@id": p.path},
            });
            if let Some(dt) = &p.datatype {
                node[format!("{SH}datatype")] = json!({"@id": dt});
            }
            if p.iri_kind {
                node[format!("{SH}nodeKind")] = json!({"@id": format!("{SH}IRI")});
            }
            if p.required {
                node[format!("{SH}minCount")] = json!(1);
            }
            if !p.values_in.is_empty() {
                node[format!("{SH}in")] = json!({"@list": p.values_in});
            }
            node
        })
        .collect();

    let mut shape = json!({
        "@id": shape_iri,
        "@type": format!("{SH}NodeShape"),
        format!("{SH}targetClass"): {"@id": entity},
        format!("{SH}property"): property_shapes,
    });
    if closed {
        // Closed shape: instances may carry ONLY the declared properties —
        // validation owns the property surface, so access policies never
        // need property allow-lists. rdf:type must be carved out or the
        // typing triple itself would violate closure.
        shape[format!("{SH}closed")] = json!(true);
        shape[format!("{SH}ignoredProperties")] = json!({
            "@list": [{"@id": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}]
        });
    }

    json!({"@graph": [class_node, shape]})
}

pub async fn run(action: &ModelEntityAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        ModelEntityAction::Define {
            dataset,
            entity,
            properties,
            label,
            closed,
            dry_run,
            remote,
        } => {
            run_define(
                dataset,
                entity,
                properties,
                label.as_deref(),
                *closed,
                *dry_run,
                remote.as_deref(),
                dirs,
                direct,
            )
            .await
        }
        ModelEntityAction::Show { dataset, remote } => {
            run_show(dataset, remote.as_deref(), dirs, direct).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_define(
    dataset: &str,
    entity: &str,
    property_specs: &[String],
    label: Option<&str>,
    closed: bool,
    dry_run: bool,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    if !(entity.starts_with("http://")
        || entity.starts_with("https://")
        || entity.starts_with("urn:"))
    {
        return Err(CliError::Usage(format!(
            "--entity must be an absolute IRI (got '{entity}')"
        )));
    }
    let specs: Vec<PropertySpec> = property_specs
        .iter()
        .map(|s| parse_property_spec(s))
        .collect::<CliResult<_>>()?;

    let graph = compile(entity, label, &specs, closed);

    println!("Entity:     {entity}");
    println!("Shape:      {}/shape", entity.trim_end_matches('/'));
    println!("Properties: {}", specs.len());
    for p in &specs {
        let mut notes: Vec<String> = vec![];
        if let Some(dt) = &p.datatype {
            notes.push(local_name(dt).to_string());
        }
        if p.iri_kind {
            notes.push("iri".into());
        }
        if p.required {
            notes.push("required".into());
        }
        if !p.values_in.is_empty() {
            notes.push(format!("in[{}]", p.values_in.join(",")));
        }
        println!(
            "  {} {}",
            p.path,
            if notes.is_empty() {
                String::new()
            } else {
                format!("({})", notes.join(", "))
            }
        );
    }

    if dry_run {
        println!("\n-- dry run; compiled JSON-LD --");
        println!("{}", serde_json::to_string_pretty(&graph)?);
        return Ok(());
    }

    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    upsert(&mode, &graph).await?;

    println!("\nDefined. Shape written to '{dataset}'.");
    println!(
        "note: SHACL validation is ACTIVE once shapes exist (reject mode by default) —\n\
         \x20     transactions violating this shape will be rejected. Existing data is\n\
         \x20     not retro-validated; `fluree validate` produces a full report."
    );
    println!(
        "next: fluree model access enable {dataset} --profile write --entity {entity}\n\
         \x20     grants create/update/delete on the class — the shape above governs\n\
         \x20     what valid instances look like."
    );
    Ok(())
}

async fn run_show(
    dataset: &str,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    let q = json!({
        "@context": {"sh": SH},
        "select": {"?shape": ["*", {"sh:property": ["*"]}]},
        "where": [{"@id": "?shape", "@type": "sh:NodeShape"}]
    });
    let result = query(&mode, &q).await?;
    let rows = result.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        println!("No entity definitions (node shapes) on '{dataset}'.");
        println!(
            "Define one: fluree model entity define {dataset} --entity <iri> \
             --property \"<iri> string required\""
        );
        return Ok(());
    }
    println!("Entity definitions on '{dataset}':\n");
    for shape in &rows {
        let target = shape
            .get("sh:targetClass")
            .map(render_ref)
            .unwrap_or_else(|| "-".into());
        println!("• {target}");
        println!("    shape: {}", render_ref(&shape["@id"]));
        let props = match shape.get("sh:property") {
            Some(Value::Array(items)) => items.clone(),
            Some(single) => vec![single.clone()],
            None => vec![],
        };
        for p in &props {
            let path = p
                .get("sh:path")
                .map(render_ref)
                .unwrap_or_else(|| "-".into());
            let mut notes: Vec<String> = vec![];
            if let Some(dt) = p.get("sh:datatype") {
                notes.push(local_name(&render_ref(dt)).to_string());
            }
            if p.get("sh:nodeKind").is_some() {
                notes.push("iri".into());
            }
            if p.get("sh:minCount")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0)
                >= 1
            {
                notes.push("required".into());
            }
            println!(
                "    - {path}{}",
                if notes.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", notes.join(", "))
                }
            );
        }
    }
    Ok(())
}

fn render_ref(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => o
            .get("@id")
            .and_then(|x| x.as_str())
            .unwrap_or("-")
            .to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_spec() {
        let p = parse_property_spec(
            "https://example.org/status string required in[new,qualified,won,lost]",
        )
        .unwrap();
        assert_eq!(p.path, "https://example.org/status");
        assert_eq!(
            p.datatype.as_deref(),
            Some("http://www.w3.org/2001/XMLSchema#string")
        );
        assert!(p.required);
        assert_eq!(p.values_in, vec!["new", "qualified", "won", "lost"]);
    }

    #[test]
    fn parses_bare_iri_as_untyped_optional() {
        let p = parse_property_spec("https://example.org/notes").unwrap();
        assert_eq!(p.datatype, None);
        assert!(!p.required);
        assert!(p.values_in.is_empty());
    }

    #[test]
    fn rejects_unknown_token_and_relative_iri() {
        assert!(parse_property_spec("https://example.org/x strng").is_err());
        assert!(parse_property_spec("ex:name string").is_err());
        assert!(parse_property_spec("https://example.org/x in[]").is_err());
    }

    #[test]
    fn compile_emits_class_and_targeted_shape() {
        let specs = vec![
            parse_property_spec("https://example.org/name string required").unwrap(),
            parse_property_spec("https://example.org/owner iri").unwrap(),
        ];
        let g = compile("https://example.org/Lead", Some("Lead"), &specs, false);
        let nodes = g["@graph"].as_array().unwrap();
        assert_eq!(nodes.len(), 2, "class + shape");

        let shape = &nodes[1];
        assert_eq!(
            shape["http://www.w3.org/ns/shacl#targetClass"]["@id"],
            "https://example.org/Lead"
        );
        let props = shape["http://www.w3.org/ns/shacl#property"]
            .as_array()
            .unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(
            props[0]["http://www.w3.org/ns/shacl#minCount"],
            json!(1),
            "required → minCount 1"
        );
        assert_eq!(
            props[1]["http://www.w3.org/ns/shacl#nodeKind"]["@id"],
            "http://www.w3.org/ns/shacl#IRI"
        );
    }

    #[test]
    fn closed_shape_carves_out_rdf_type() {
        let specs = vec![parse_property_spec("https://example.org/name string required").unwrap()];
        let g = compile("https://example.org/Lead", None, &specs, true);
        let shape = &g["@graph"][1];
        assert_eq!(shape[format!("{SH}closed")], true);
        assert_eq!(
            shape[format!("{SH}ignoredProperties")]["@list"][0]["@id"],
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        );
        // Open by default.
        let g2 = compile("https://example.org/Lead", None, &specs, false);
        assert!(g2["@graph"][1].get(format!("{SH}closed")).is_none());
    }

    #[test]
    fn stable_property_shape_ids_for_idempotent_upsert() {
        let specs = vec![parse_property_spec("https://example.org/name string").unwrap()];
        let g = compile("https://example.org/Lead", None, &specs, false);
        let props = g["@graph"][1]["http://www.w3.org/ns/shacl#property"]
            .as_array()
            .unwrap();
        assert_eq!(props[0]["@id"], "https://example.org/Lead/shape/name");
    }
}
