//! Tables from a Neo4j graph's schema, as JSON: `apoc.meta.schema()`'s map
//! (labels, their typed properties, and relationship types with their
//! direction and the labels at their other end), or the built-in
//! `db.schema.visualization()` (labels and relationship types only).
//!
//! A property graph is close to an ontology already:
//!
//! - a node label is a class (`MovieGenre` → "movie genre");
//! - a node property is a property typed from its Neo4j type; a unique `id`
//!   or `uuid` (or the label's name with `Id`) is a key and is left out;
//! - an outgoing relationship type is a relation from the label to the
//!   label at its other end, labelled in words (`WORKS_AT` → "works at") and
//!   described as the sentence it reads ("A person works at a company.");
//!   one reaching several labels has no range, and says which;
//! - relationship properties have no place on an RDF property, and are left
//!   out and counted.

use super::{class_label, is_jsonld, Column, Model, Table};
#[cfg(test)]
use crate::vocab::{RDFS_COMMENT, RDFS_LABEL};
use crate::words::name_words;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The schema map, unwrapped from the row a query result exports it in
/// (`{"value": {...}}`, or a list of such rows).
fn apoc_map(doc: &Value) -> Option<&serde_json::Map<String, Value>> {
    let doc = match doc {
        Value::Array(rows) if rows.len() == 1 => &rows[0],
        other => other,
    };
    let map = doc.get("value").unwrap_or(doc).as_object()?;
    let typed = map
        .values()
        .filter(|v| {
            matches!(
                v.get("type").and_then(Value::as_str),
                Some("node" | "relationship")
            )
        })
        .count();
    (typed > 0 && typed == map.len()).then_some(map)
}

/// `db.schema.visualization()`'s result: its nodes and relationships.
fn visualization(doc: &Value) -> Option<(&Vec<Value>, &Vec<Value>)> {
    let doc = match doc {
        Value::Array(rows) if rows.len() == 1 => &rows[0],
        other => other,
    };
    Some((
        doc.get("nodes")?.as_array()?,
        doc.get("relationships")?.as_array()?,
    ))
}

/// Whether a JSON document is a Neo4j schema export this module reads.
pub fn is_neo4j(doc: &Value) -> bool {
    !is_jsonld(doc) && (apoc_map(doc).is_some() || visualization(doc).is_some())
}

/// A Neo4j property type as an XSD datatype's local name.
fn datatype(neo4j: &str) -> Option<&'static str> {
    Some(
        match neo4j.to_ascii_uppercase().replace(' ', "_").as_str() {
            "STRING" => "string",
            "INTEGER" | "LONG" | "INT" => "integer",
            "FLOAT" | "DOUBLE" => "decimal",
            "BOOLEAN" => "boolean",
            "DATE" => "date",
            "DATE_TIME" | "DATETIME" | "ZONED_DATE_TIME" | "LOCAL_DATE_TIME" | "LOCALDATETIME" => {
                "dateTime"
            }
            "TIME" | "LOCAL_TIME" | "LOCALTIME" | "ZONED_TIME" => "time",
            _ => return None,
        },
    )
}

fn article(word: &str) -> &'static str {
    if word.starts_with(['a', 'e', 'i', 'o', 'u']) {
        "an"
    } else {
        "a"
    }
}

/// "a person", "an apple, a pear or a plum".
fn some(labels: &[String]) -> String {
    let phrases: Vec<String> = labels
        .iter()
        .map(|t| {
            let l = class_label(t);
            format!("{} {l}", article(&l))
        })
        .collect();
    match phrases.as_slice() {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} or {last}", rest.join(", ")),
    }
}

/// The sentence a relationship reads, from every label it starts at: "A
/// person or a robot works at a company."
fn sentence(starts: &[String], relation: &str, targets: &[String]) -> String {
    let words = name_words(relation).unwrap_or_else(|| relation.to_lowercase());
    let objects = some(targets);
    let mut s = if objects.is_empty() {
        format!("{} {words}.", some(starts))
    } else {
        format!("{} {words} {objects}.", some(starts))
    };
    if let Some(first) = s.get_mut(..1) {
        first.make_ascii_uppercase();
    }
    s
}

/// Relationship columns for each (start label, relationship type, targets),
/// each described for every label the type starts at.
fn add_relations(tables: &mut [Table], found: &[(String, String, Vec<String>)]) {
    let mut starts: std::collections::BTreeMap<&str, Vec<String>> = Default::default();
    for (start, rel, _) in found {
        starts.entry(rel.as_str()).or_default().push(start.clone());
    }
    for (start, rel, targets) in found {
        if let Some(t) = tables.iter_mut().find(|t| t.name == *start) {
            t.columns
                .push(relation(&starts[rel.as_str()], rel, targets));
        }
    }
}

fn is_key(label: &str, property: &str, unique: bool) -> bool {
    let words = name_words(property).unwrap_or_default();
    unique
        && (words == "id"
            || words == "uuid"
            || words
                .strip_suffix(" id")
                .is_some_and(|stem| class_label(stem) == class_label(label)))
}

/// The model, and how many relationship properties were left out.
pub fn read(doc: &Value) -> Option<(Model, usize)> {
    if let Some(map) = apoc_map(doc) {
        return Some(from_apoc(map));
    }
    let (nodes, relationships) = visualization(doc)?;
    Some((from_visualization(nodes, relationships), 0))
}

fn from_apoc(map: &serde_json::Map<String, Value>) -> (Model, usize) {
    let mut tables = Vec::new();
    let mut found: Vec<(String, String, Vec<String>)> = Vec::new();
    let mut left_out = 0;
    for (label, node) in map {
        if node.get("type").and_then(Value::as_str) != Some("node") {
            continue;
        }
        let mut table = Table {
            name: label.clone(),
            ..Table::default()
        };
        if let Some(Value::Object(props)) = node.get("properties") {
            for (name, p) in props {
                let unique = p.get("unique").and_then(Value::as_bool).unwrap_or(false);
                if is_key(label, name, unique) {
                    table.primary_key.push(name.clone());
                }
                table.columns.push(Column {
                    name: name.clone(),
                    datatype: p.get("type").and_then(Value::as_str).and_then(datatype),
                    ..Column::default()
                });
            }
        }
        if let Some(Value::Object(rels)) = node.get("relationships") {
            let label = label.clone();
            for (rel, r) in rels {
                if r.get("direction").and_then(Value::as_str) != Some("out") {
                    continue;
                }
                let targets: Vec<String> = r
                    .get("labels")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|l| l.as_str().map(str::to_string))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                if let Some(Value::Object(props)) = r.get("properties") {
                    left_out += props.len();
                }
                found.push((label.clone(), rel.clone(), targets));
            }
        }
        tables.push(table);
    }
    add_relations(&mut tables, &found);
    (
        Model {
            tables,
            notes: Vec::new(),
        },
        left_out,
    )
}

/// A relationship type as a column of a start label's table, labelled with
/// the verb it is (`HAS_KEY`, "has key") and described as the sentence it
/// reads from every label it starts at.
fn relation(starts: &[String], rel: &str, targets: &[String]) -> Column {
    Column {
        name: rel.to_string(),
        label: Some(name_words(rel).unwrap_or_else(|| rel.to_lowercase())),
        description: Some(sentence(starts, rel, targets)),
        // one label at the other end is the range; several have none
        references: match targets {
            [one] => Some(one.clone()),
            _ => None,
        },
        ..Column::default()
    }
}

fn from_visualization(nodes: &[Value], relationships: &[Value]) -> Model {
    // a node's name is its label; relationships name their ends by node id
    let mut by_id: BTreeMap<String, String> = BTreeMap::new();
    let mut tables: Vec<Table> = Vec::new();
    for n in nodes {
        let label = n
            .get("labels")
            .and_then(Value::as_array)
            .and_then(|l| l.first())
            .and_then(Value::as_str)
            .or_else(|| n.get("name").and_then(Value::as_str))
            .or_else(|| {
                n.get("properties")
                    .and_then(|p| p.get("name"))
                    .and_then(Value::as_str)
            });
        let Some(label) = label else {
            continue;
        };
        for key in ["identity", "id", "elementId"] {
            if let Some(id) = n.get(key) {
                by_id.insert(id.to_string(), label.to_string());
            }
        }
        if !tables.iter().any(|t| t.name == label) {
            tables.push(Table {
                name: label.to_string(),
                ..Table::default()
            });
        }
    }
    let end = |r: &Value, keys: &[&str]| -> Option<String> {
        keys.iter().find_map(|k| match r.get(*k)? {
            Value::Object(node) => node
                .get("labels")
                .and_then(Value::as_array)
                .and_then(|l| l.first())
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    node.get("identity")
                        .and_then(|id| by_id.get(&id.to_string()).cloned())
                }),
            id => by_id.get(&id.to_string()).cloned(),
        })
    };
    let mut rels: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for r in relationships {
        let Some(rel) = r
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| r.get("name").and_then(Value::as_str))
        else {
            continue;
        };
        let (Some(start), Some(target)) = (
            end(r, &["start", "startNode", "startNodeElementId"]),
            end(r, &["end", "endNode", "endNodeElementId"]),
        ) else {
            continue;
        };
        rels.entry((start, rel.to_string()))
            .or_default()
            .insert(target);
    }
    let found: Vec<(String, String, Vec<String>)> = rels
        .into_iter()
        .map(|((start, rel), targets)| (start, rel, targets.into_iter().collect()))
        .collect();
    add_relations(&mut tables, &found);
    Model {
        tables,
        notes: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apoc_meta_schema() {
        let doc = serde_json::json!([{"value": {
            "Person": {"type": "node", "count": 10, "properties": {
                "id": {"type": "STRING", "unique": true, "indexed": true},
                "name": {"type": "STRING", "unique": false},
                "born": {"type": "DATE", "unique": false}
            }, "relationships": {
                "WORKS_AT": {"direction": "out", "labels": ["Company"], "count": 4,
                             "properties": {"since": {"type": "DATE"}}},
                "KNOWS": {"direction": "out", "labels": ["Person"], "properties": {}},
                "EMPLOYS": {"direction": "in", "labels": ["Company"], "properties": {}},
                "ATTENDED": {"direction": "out", "labels": ["School", "Company"], "properties": {}}
            }},
            "Company": {"type": "node", "properties": {"name": {"type": "STRING"}}, "relationships": {}},
            "School": {"type": "node", "properties": {}, "relationships": {}},
            "WORKS_AT": {"type": "relationship", "count": 4, "properties": {"since": {"type": "DATE"}}}
        }}]);
        assert!(is_neo4j(&doc));
        let (m, left_out) = read(&doc).unwrap();
        assert_eq!(left_out, 1, "WORKS_AT.since");
        let person = m.tables.iter().find(|t| t.name == "Person").unwrap();
        assert_eq!(person.primary_key, ["id"]);
        let c = |n: &str| person.columns.iter().find(|c| c.name == n).unwrap().clone();
        assert_eq!(c("born").datatype, Some("date"));
        assert_eq!(c("WORKS_AT").references.as_deref(), Some("Company"));
        assert_eq!(
            c("WORKS_AT").description.as_deref(),
            Some("A person works at a company.")
        );
        assert_eq!(c("ATTENDED").references, None);
        assert_eq!(
            c("ATTENDED").description.as_deref(),
            Some("A person attended a company or a school.")
        );
        assert!(
            person.columns.iter().all(|c| c.name != "EMPLOYS"),
            "incoming relationships are the other label's"
        );
    }

    #[test]
    fn schema_visualization() {
        let doc = serde_json::json!([{
            "nodes": [
                {"identity": -1, "labels": ["Movie"], "properties": {"name": "Movie"}},
                {"identity": -2, "labels": ["Person"], "properties": {"name": "Person"}}
            ],
            "relationships": [
                {"identity": -3, "start": -2, "end": -1, "type": "ACTED_IN", "properties": {"name": "ACTED_IN"}}
            ]
        }]);
        assert!(is_neo4j(&doc));
        let (m, _) = read(&doc).unwrap();
        let person = m.tables.iter().find(|t| t.name == "Person").unwrap();
        assert_eq!(person.columns[0].references.as_deref(), Some("Movie"));
        assert_eq!(
            person.columns[0].description.as_deref(),
            Some("A person acted in a movie.")
        );
    }

    #[test]
    fn not_neo4j() {
        assert!(!is_neo4j(
            &serde_json::json!({"@context": {}, "@graph": []})
        ));
        assert!(!is_neo4j(&serde_json::json!({"data_sources": {}})));
    }

    #[test]
    fn relationship_types_are_verbs_described_for_every_start() {
        let doc = serde_json::json!({
            "Person": {"type": "node", "properties": {}, "relationships": {
                "WORKS_AT": {"direction": "out", "labels": ["Company"], "properties": {}},
                "HAS_KEY": {"direction": "out", "labels": ["Key"], "properties": {}}}},
            "Robot": {"type": "node", "properties": {}, "relationships": {
                "WORKS_AT": {"direction": "out", "labels": ["Company"], "properties": {}}}},
            "Company": {"type": "node", "properties": {}, "relationships": {}},
            "Key": {"type": "node", "properties": {}, "relationships": {}}
        });
        let (m, _) = read(&doc).unwrap();
        let out = super::super::emit(
            &m,
            &super::super::EmitOptions {
                base: "https://ex.com/".into(),
            },
        );
        let literal = |s: &str, p: &str| -> Option<String> {
            out.ontology
                .iter()
                .find(|q| q.subject.as_iri() == Some(s) && q.is(p))
                .and_then(|q| q.object.as_literal().map(|(v, ..)| v.lexical()))
        };
        assert_eq!(
            literal("https://ex.com/hasKey", RDFS_LABEL).as_deref(),
            Some("has key")
        );
        assert_eq!(
            literal("https://ex.com/hasKey", RDFS_COMMENT).as_deref(),
            Some("A person has key a key.")
        );
        assert_eq!(
            literal("https://ex.com/worksAt", RDFS_COMMENT).as_deref(),
            Some("A person or a robot works at a company.")
        );
    }

    #[test]
    fn json_ld_with_nodes_is_not_a_neo4j_schema() {
        assert!(!is_neo4j(&serde_json::json!({
            "@context": {}, "nodes": [], "relationships": []
        })));
    }
}
