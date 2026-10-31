//! Tables from a dbt project's YAML: semantic models (MetricFlow) and model
//! properties (`schema.yml`).
//!
//! A **semantic model** names its entities, dimensions and measures:
//!
//! - a `primary`, `unique` or `natural` entity is the model's key, left out;
//! - a `foreign` entity is a relation to the semantic model whose primary
//!   entity has the same name (`customer` → the model keyed by `customer`),
//!   else to the model its column's `relationships` test names, else to the
//!   model whose name reads as the entity's; it is labelled with the entity's
//!   name;
//! - a dimension is a property (a `time` dimension a date, or a date-time
//!   when its granularity is finer than a day);
//! - a measure that aggregates a column's values (`sum`, `average`, `min`,
//!   `max`, `median`, `percentile`) is a decimal property; counts (including
//!   MetricFlow's `agg: sum, expr: 1`) are about rows, not things, and
//!   metrics and saved queries are left out.
//!
//! A **model's properties** give its columns' descriptions and tests:
//! `accepted_values` makes a column enumerated, `relationships` (or a
//! `foreign_key` constraint, on the column or the model) makes it a foreign
//! key, and a `primary_key` constraint, or `unique` with `not_null` on `id`,
//! makes it the key. A versioned model takes its latest version's columns.
//!
//! A semantic model built on a model (`model: ref('orders')`) stands for it:
//! it takes the model's descriptions and accepted values for the columns it
//! uses, and the model is not repeated. Of the model's other columns, those
//! that carry structure the semantic model lacks (a relation, or accepted
//! values) are kept; the rest (audit columns, surrogate keys, columns only an
//! expression uses) are left out and counted. References to a replaced model
//! resolve to the semantic model. Models and semantic models are separate
//! name spaces in dbt: a model that shares a semantic model's name (and isn't
//! its base) is named `<name>_model`.
//!
//! Descriptions that are `{{ doc('name') }}` blocks are resolved from the
//! project's `{% docs %}` blocks; one whose block isn't found is left out and
//! counted, rather than shipped as template text.
//!
//! YAML is read into typed fields, so values (`accepted_values: [NO, 007]`)
//! are kept as written.

use super::sql::xsd_for;
use super::{class_label, from_yaml, strip_word_suffix, Column, Model, Table};
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Whether a document has anything this module reads.
pub fn is_dbt(doc: &Value) -> bool {
    doc.get("semantic_models").is_some() || doc.get("models").is_some()
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Project {
    models: Option<Vec<DbtModel>>,
    semantic_models: Option<Vec<SemanticModel>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct DbtModel {
    name: Option<String>,
    description: Option<String>,
    columns: Option<Vec<DbtColumn>>,
    constraints: Option<Vec<Constraint>>,
    latest_version: Option<String>,
    versions: Option<Vec<Version>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Version {
    v: Option<String>,
    columns: Option<Vec<VersionColumn>>,
}

/// A version's column: a column of its own, or `include: all` with an
/// `exclude` list of the model's columns.
#[derive(Deserialize, Default)]
#[serde(default)]
struct VersionColumn {
    #[serde(flatten)]
    column: DbtColumn,
    exclude: Option<Vec<String>>,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
struct DbtColumn {
    name: Option<String>,
    description: Option<String>,
    data_type: Option<String>,
    data_tests: Option<Vec<Test>>,
    tests: Option<Vec<Test>>,
    constraints: Option<Vec<Constraint>>,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
struct Constraint {
    #[serde(rename = "type")]
    kind: Option<String>,
    columns: Option<Vec<String>>,
    to: Option<String>,
    /// Before dbt 1.9: `REFERENCES`-style text, `stores (id)`.
    expression: Option<String>,
}

/// A data test: a bare name (`unique`) or a map naming one test with its
/// arguments.
#[derive(Default, Clone)]
enum Test {
    Name(String),
    Map(Box<TestMap>),
    #[default]
    Other,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
struct TestMap {
    accepted_values: Option<AcceptedValues>,
    relationships: Option<Relationships>,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
struct AcceptedValues {
    values: Option<Vec<String>>,
    /// dbt 1.10 puts a test's arguments under `arguments:`.
    arguments: Option<Box<AcceptedValues>>,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
struct Relationships {
    to: Option<String>,
    arguments: Option<Box<Relationships>>,
}

impl<'de> Deserialize<'de> for Test {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Test;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a test name or a map")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Test, E> {
                Ok(Test::Name(v.to_string()))
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Test, E> {
                Ok(Test::Other)
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<Test, E> {
                Ok(Test::Other)
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<Test, E> {
                Ok(Test::Other)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Test, E> {
                Ok(Test::Other)
            }
            fn visit_unit<E: de::Error>(self) -> Result<Test, E> {
                Ok(Test::Other)
            }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Test, A::Error> {
                TestMap::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(|m| Test::Map(Box::new(m)))
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SemanticModel {
    name: Option<String>,
    description: Option<String>,
    model: Option<String>,
    entities: Option<Vec<Entity>>,
    dimensions: Option<Vec<Dimension>>,
    measures: Option<Vec<Measure>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Entity {
    name: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    expr: Option<String>,
    description: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Dimension {
    name: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    expr: Option<String>,
    description: Option<String>,
    type_params: Option<TypeParams>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TypeParams {
    time_granularity: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Measure {
    name: Option<String>,
    agg: Option<String>,
    expr: Option<String>,
    description: Option<String>,
}

fn clean(s: &Option<String>) -> Option<String> {
    s.as_ref()
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
}

/// The model a `ref('orders')` names: its last quoted positional argument,
/// past a package name and ignoring a version (`ref('orders', v=2)`); the
/// table of a `source('raw', 'orders')`; or a bare name.
fn ref_name(text: &str) -> String {
    let t = text.trim();
    let inner = ["ref(", "source("]
        .iter()
        .find_map(|p| t.strip_prefix(p).and_then(|r| r.strip_suffix(')')))
        .unwrap_or(t);
    inner
        .split(',')
        .map(str::trim)
        .rfind(|a| !a.contains('='))
        .unwrap_or(inner)
        .trim_matches(|c| c == '\'' || c == '"')
        .to_string()
}

/// The table a pre-1.9 `foreign_key` constraint's expression names:
/// `stores (id)` or `analytics.stores(id)` → `stores`.
fn expression_table(expression: &str) -> Option<String> {
    let head = expression
        .split(['(', ' '])
        .find(|s| !s.is_empty())?
        .trim_matches(|c| c == '"' || c == '`');
    Some(head.rsplit('.').next().unwrap_or(head).to_string())
}

/// `{% docs name %}...{% enddocs %}` blocks in a project's Markdown.
pub fn doc_blocks(markdown: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = markdown;
    while let Some(open) = rest.find("{%") {
        let after = &rest[open + 2..];
        let Some(close) = after.find("%}") else {
            break;
        };
        let tag = after[..close].trim().trim_matches('-').trim();
        rest = &after[close + 2..];
        let Some(name) = tag.strip_prefix("docs ").map(str::trim) else {
            continue;
        };
        let Some(end) = rest.find("enddocs") else {
            break;
        };
        // the body ends at the `{%` that opens `enddocs`
        let body_end = rest[..end].rfind("{%").unwrap_or(end);
        out.push((name.to_string(), rest[..body_end].trim().to_string()));
        rest = &rest[end..];
    }
    out
}

/// A description with its `{{ doc('name') }}` blocks resolved; `None` (and
/// counted) when template text is left.
fn resolve_doc(
    text: Option<String>,
    blocks: &HashMap<String, String>,
    jinja: &mut usize,
) -> Option<String> {
    let mut text = text?;
    while let Some(open) = text.find("{{") {
        let Some(close) = text[open..].find("}}") else {
            break;
        };
        let inner = text[open + 2..open + close].trim();
        let name = inner
            .strip_prefix("doc(")
            .and_then(|r| r.strip_suffix(')'))
            .map(|n| n.trim().trim_matches(|c| c == '\'' || c == '"'));
        match name.and_then(|n| blocks.get(n)) {
            Some(block) => text.replace_range(open..open + close + 2, block),
            None => break,
        }
    }
    if text.contains("{{") || text.contains("{%") {
        *jinja += 1;
        return None;
    }
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!text.is_empty()).then_some(text)
}

/// The names a semantic model uses: its entities (a foreign entity also by
/// the column it is built from, its `expr` or `<name>_id`), dimensions and
/// measures, and every identifier in their expressions (lowercased).
fn used_names(sm: &SemanticModel) -> HashSet<String> {
    let mut used = HashSet::new();
    let add_expr = |used: &mut HashSet<String>, expr: &Option<String>| {
        if let Some(expr) = expr {
            used.extend(
                expr.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .filter(|t| !t.is_empty())
                    .map(str::to_lowercase),
            );
        }
    };
    for e in sm.entities.iter().flatten() {
        if let Some(name) = clean(&e.name) {
            used.insert(name.to_lowercase());
            if e.expr.is_none() {
                used.insert(format!("{}_id", name.to_lowercase()));
            }
        }
        add_expr(&mut used, &e.expr);
    }
    for d in sm.dimensions.iter().flatten() {
        if let Some(name) = clean(&d.name) {
            used.insert(name.to_lowercase());
        }
        add_expr(&mut used, &d.expr);
    }
    for m in sm.measures.iter().flatten() {
        if let Some(name) = clean(&m.name) {
            used.insert(name.to_lowercase());
        }
        add_expr(&mut used, &m.expr);
    }
    used
}

/// A model reference kept until every model's final name is known.
const MODEL_REF: &str = "\u{0}model:";

fn model_table(m: &DbtModel, blocks: &HashMap<String, String>, jinja: &mut usize) -> Option<Table> {
    let name = clean(&m.name)?;
    let mut table = Table {
        name: name.clone(),
        description: resolve_doc(clean(&m.description), blocks, jinja),
        ..Table::default()
    };
    // a versioned model: its latest version's columns over the model's own
    let mut columns: Vec<DbtColumn> = m.columns.clone().unwrap_or_default();
    if let Some(versions) = &m.versions {
        let latest = m.latest_version.clone().or_else(|| {
            versions.iter().filter_map(|v| v.v.clone()).max_by(|a, b| {
                match (a.parse::<f64>(), b.parse::<f64>()) {
                    (Ok(x), Ok(y)) => x.total_cmp(&y),
                    _ => a.cmp(b),
                }
            })
        });
        if let Some(version) = versions.iter().find(|v| v.v == latest && latest.is_some()) {
            for vc in version.columns.iter().flatten() {
                for gone in vc.exclude.iter().flatten() {
                    columns.retain(|c| {
                        !c.name
                            .as_deref()
                            .is_some_and(|n| n.eq_ignore_ascii_case(gone))
                    });
                }
                let Some(n) = clean(&vc.column.name) else {
                    continue;
                };
                columns.retain(|c| {
                    !c.name
                        .as_deref()
                        .is_some_and(|x| x.eq_ignore_ascii_case(&n))
                });
                columns.push(vc.column.clone());
            }
        }
    }
    for c in &columns {
        let Some(column_name) = clean(&c.name) else {
            continue;
        };
        let mut column = Column {
            datatype: c.data_type.as_deref().and_then(xsd_for),
            description: resolve_doc(clean(&c.description), blocks, jinja),
            name: column_name.clone(),
            ..Column::default()
        };
        let (mut unique, mut not_null) = (false, false);
        for test in c.data_tests.iter().chain(c.tests.iter()).flatten() {
            match test {
                Test::Name(t) if t == "unique" => unique = true,
                Test::Name(t) if t == "not_null" => not_null = true,
                Test::Map(map) => {
                    if let Some(av) = &map.accepted_values {
                        let values = av
                            .arguments
                            .as_ref()
                            .and_then(|a| a.values.clone())
                            .or_else(|| av.values.clone())
                            .unwrap_or_default();
                        column.values = values;
                    }
                    if let Some(rel) = &map.relationships {
                        let to = rel
                            .arguments
                            .as_ref()
                            .and_then(|a| a.to.clone())
                            .or(rel.to.clone());
                        if let Some(to) = to {
                            column.references = Some(format!("{MODEL_REF}{}", ref_name(&to)));
                        }
                    }
                }
                _ => {}
            }
        }
        for constraint in c.constraints.iter().flatten() {
            match constraint.kind.as_deref() {
                Some("primary_key") => table.primary_key.push(column_name.clone()),
                Some("foreign_key") => {
                    let to =
                        constraint.to.as_deref().map(ref_name).or_else(|| {
                            constraint.expression.as_deref().and_then(expression_table)
                        });
                    if let Some(to) = to {
                        column.references = Some(format!("{MODEL_REF}{to}"));
                    }
                }
                _ => {}
            }
        }
        let words = crate::words::name_words(&column_name).unwrap_or_default();
        let own_key = strip_word_suffix(&words, "id")
            .is_some_and(|stem| class_label(stem) == class_label(&name));
        if unique && not_null && (words.eq_ignore_ascii_case("id") || own_key) {
            table.primary_key.push(column_name.clone());
        }
        table.columns.push(column);
    }
    // model-level constraints name their columns
    for constraint in m.constraints.iter().flatten() {
        let columns = constraint.columns.clone().unwrap_or_default();
        match constraint.kind.as_deref() {
            Some("primary_key") => table.primary_key.extend(columns),
            Some("foreign_key") => {
                let to = constraint
                    .to
                    .as_deref()
                    .map(ref_name)
                    .or_else(|| constraint.expression.as_deref().and_then(expression_table));
                if let (Some(to), Some(first)) = (to, columns.first()) {
                    match table
                        .columns
                        .iter_mut()
                        .find(|c| c.name.eq_ignore_ascii_case(first))
                    {
                        Some(c) => c.references = Some(format!("{MODEL_REF}{to}")),
                        None => table.columns.push(Column {
                            name: first.clone(),
                            references: Some(format!("{MODEL_REF}{to}")),
                            ..Column::default()
                        }),
                    }
                }
            }
            _ => {}
        }
    }
    Some(table)
}

/// The model for a dbt project's YAML files, `(name, text)`, with the doc
/// blocks of its Markdown; and how many columns of models that semantic
/// models stand for were left out. Files that aren't valid YAML are noted.
pub fn read(files: &[(String, String)], blocks: &HashMap<String, String>) -> (Model, usize) {
    let mut notes = Vec::new();
    let mut jinja = 0;
    let mut projects = Vec::new();
    for (name, text) in files {
        match from_yaml::<Project>(text) {
            Ok(p) => projects.push(p),
            Err(e) => notes.push(format!("{name}: skipped, not valid YAML ({e})")),
        }
    }
    let mut models: Vec<Table> = projects
        .iter()
        .flat_map(|p| p.models.iter().flatten())
        .filter_map(|m| model_table(m, blocks, &mut jinja))
        .collect();
    let semantic: Vec<&SemanticModel> = projects
        .iter()
        .flat_map(|p| p.semantic_models.iter().flatten())
        .filter(|sm| clean(&sm.name).is_some())
        .collect();
    // each semantic model's primary entity, so a foreign entity finds its table
    let mut keyed_by: HashMap<String, String> = HashMap::new();
    for sm in &semantic {
        let name = clean(&sm.name).unwrap_or_default();
        for e in sm.entities.iter().flatten() {
            if matches!(e.kind.as_deref(), Some("primary" | "unique" | "natural")) {
                if let Some(entity) = clean(&e.name) {
                    keyed_by
                        .entry(entity.to_lowercase())
                        .or_insert(name.clone());
                }
            }
        }
    }
    let model_tables = models.clone();
    let mut replaced: HashMap<String, String> = HashMap::new();
    let mut semantic_tables: Vec<Table> = Vec::new();
    let mut left_out = 0;
    let mut count_measures = 0;
    for sm in &semantic {
        let name = clean(&sm.name).unwrap_or_default();
        let base = clean(&sm.model).map(|m| ref_name(&m));
        let base_table = base
            .as_ref()
            .and_then(|b| model_tables.iter().find(|t| t.name.eq_ignore_ascii_case(b)));
        let described: HashMap<String, Column> = base_table
            .map(|t| {
                t.columns
                    .iter()
                    .map(|c| (c.name.to_lowercase(), c.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let mut table = Table {
            name: name.clone(),
            description: resolve_doc(clean(&sm.description), blocks, &mut jinja)
                .or_else(|| base_table.and_then(|t| t.description.clone())),
            ..Table::default()
        };
        // a column's own description, else the base model's
        let from_model = |expr: &Option<String>, name: &str| -> Option<Column> {
            expr.as_deref()
                .and_then(|e| described.get(&e.trim().to_lowercase()))
                .or_else(|| described.get(&name.to_lowercase()))
                .or_else(|| described.get(&format!("{}_id", name.to_lowercase())))
                .cloned()
        };
        for e in sm.entities.iter().flatten() {
            let Some(entity) = clean(&e.name) else {
                continue;
            };
            let base_column = from_model(&e.expr, &entity);
            let description = resolve_doc(clean(&e.description), blocks, &mut jinja)
                .or_else(|| base_column.as_ref().and_then(|c| c.description.clone()));
            match e.kind.as_deref() {
                Some("foreign") => {
                    // the semantic model keyed by this entity, else the model
                    // the column's test names, else one named for the entity
                    let references = keyed_by
                        .get(&entity.to_lowercase())
                        .cloned()
                        .or_else(|| base_column.as_ref().and_then(|c| c.references.clone()))
                        .unwrap_or_else(|| format!("{MODEL_REF}{entity}"));
                    table.columns.push(Column {
                        name: entity.clone(),
                        description,
                        references: Some(references),
                        ..Column::default()
                    });
                }
                _ => table.primary_key.push(entity.clone()),
            }
        }
        for d in sm.dimensions.iter().flatten() {
            let Some(dimension) = clean(&d.name) else {
                continue;
            };
            let base_column = from_model(&d.expr, &dimension);
            let datatype = match d.kind.as_deref() {
                Some("time") => {
                    let grain = d
                        .type_params
                        .as_ref()
                        .and_then(|p| clean(&p.time_granularity))
                        .unwrap_or_else(|| "day".into());
                    Some(
                        if matches!(grain.as_str(), "hour" | "minute" | "second" | "millisecond") {
                            "dateTime"
                        } else {
                            "date"
                        },
                    )
                }
                _ => base_column
                    .as_ref()
                    .and_then(|c| c.datatype)
                    .or(Some("string")),
            };
            table.columns.push(Column {
                name: dimension,
                datatype,
                description: resolve_doc(clean(&d.description), blocks, &mut jinja)
                    .or_else(|| base_column.as_ref().and_then(|c| c.description.clone())),
                values: base_column.map(|c| c.values).unwrap_or_default(),
                ..Column::default()
            });
        }
        for m in sm.measures.iter().flatten() {
            let Some(measure) = clean(&m.name) else {
                continue;
            };
            let agg = clean(&m.agg).unwrap_or_default();
            // `agg: sum, expr: 1` counts rows, as `count` does
            let counts_rows = m
                .expr
                .as_deref()
                .is_some_and(|e| e.trim().parse::<f64>().is_ok());
            if counts_rows
                || !matches!(
                    agg.as_str(),
                    "sum" | "average" | "min" | "max" | "median" | "percentile"
                )
            {
                count_measures += 1;
                continue;
            }
            let base_column = from_model(&m.expr, &measure);
            table.columns.push(Column {
                name: measure,
                datatype: Some("decimal"),
                description: resolve_doc(clean(&m.description), blocks, &mut jinja)
                    .or_else(|| base_column.and_then(|c| c.description)),
                ..Column::default()
            });
        }
        // the semantic model stands for its base model; the base model's
        // relations and enumerations it didn't use come along
        if let Some(bt) = base_table {
            let used = used_names(sm);
            for c in &bt.columns {
                let n = c.name.to_lowercase();
                if used.contains(&n) || bt.primary_key.iter().any(|k| k.eq_ignore_ascii_case(&n)) {
                    continue;
                }
                if c.references.is_some() || !c.values.is_empty() {
                    table.columns.push(c.clone());
                } else {
                    left_out += 1;
                }
            }
            replaced.insert(bt.name.to_lowercase(), name.clone());
            models.retain(|t| !t.name.eq_ignore_ascii_case(&bt.name));
        }
        semantic_tables.push(table);
    }
    // a model sharing a semantic model's name is another thing: it keeps
    // its own class, under `<name>_model`
    let semantic_names: HashSet<String> = semantic_tables
        .iter()
        .map(|t| t.name.to_lowercase())
        .collect();
    let mut renamed: HashMap<String, String> = HashMap::new();
    for m in &mut models {
        if semantic_names.contains(&m.name.to_lowercase()) {
            let new = format!("{}_model", m.name);
            notes.push(format!(
                "model {:?} shares a semantic model's name, and is read as {new:?}",
                m.name
            ));
            renamed.insert(m.name.to_lowercase(), new.clone());
            m.name = new;
        }
    }
    let mut tables: Vec<Table> = semantic_tables;
    tables.extend(models);
    // model references resolve to the model's final name: the semantic model
    // standing for it, its renamed self, or a table that reads as it does
    let final_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
    for c in tables.iter_mut().flat_map(|t| t.columns.iter_mut()) {
        let Some(r) = c
            .references
            .as_ref()
            .and_then(|r| r.strip_prefix(MODEL_REF))
        else {
            continue;
        };
        let lower = r.to_lowercase();
        let resolved = replaced
            .get(&lower)
            .or_else(|| renamed.get(&lower))
            .cloned()
            .or_else(|| {
                final_names
                    .iter()
                    .find(|n| n.eq_ignore_ascii_case(r))
                    .cloned()
            })
            .or_else(|| {
                let wanted = class_label(r);
                let matching: Vec<&String> = final_names
                    .iter()
                    .filter(|n| class_label(n) == wanted)
                    .collect();
                (matching.len() == 1).then(|| matching[0].clone())
            })
            .unwrap_or_else(|| r.to_string());
        c.references = Some(resolved);
    }
    if count_measures > 0 {
        notes.push(format!(
            "{count_measures} measure(s) that count rows rather than aggregate a column were left out"
        ));
    }
    if jinja > 0 {
        notes.push(format!(
            "{jinja} description(s) left out: Jinja whose doc block wasn't found"
        ));
    }
    (Model { tables, notes }, left_out)
}

/// The model for dbt YAML texts with no doc blocks.
pub fn model(files: &[(String, String)]) -> Model {
    read(files, &HashMap::new()).0
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROJECT: &str = r"
models:
  - name: customers
    description: People and clubs who buy from the shops.
    columns:
      - name: customer_id
        description: The customer's account number.
        data_tests: [unique, not_null]
      - name: segment
        description: Kind of customer.
        data_tests:
          - accepted_values:
              arguments:
                values: [commuter, racer, club]
  - name: stg_orders
    description: Orders as the shop system records them.
    columns:
      - name: status
        description: Where the order is.
        tests:
          - accepted_values:
              values: [placed, shipped, returned]
      - name: store_id
        tests:
          - relationships:
              to: ref('stores')
              field: id
      - name: _loaded_at
semantic_models:
  - name: orders
    model: ref('stg_orders')
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: ordered_at
        type: time
        type_params: {time_granularity: day}
      - name: status
        type: categorical
    measures:
      - name: order_total
        agg: sum
        description: The order's total in pounds.
      - name: order_count
        agg: count
        expr: 1
  - name: customers
    model: ref('customers')
    entities:
      - name: customer
        type: primary
        expr: customer_id
    dimensions:
      - name: segment
        type: categorical
";

    #[test]
    fn semantic_models_and_model_properties() {
        let doc: Value = super::super::parse_document(PROJECT).unwrap();
        assert!(is_dbt(&doc));
        let m = model(&[("project.yml".into(), PROJECT.into())]);
        let names: Vec<&str> = m.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            ["orders", "customers"],
            "semantic models stand for their models"
        );
        let t = |n: &str| m.tables.iter().find(|t| t.name == n).unwrap();
        let c = |t: &Table, n: &str| t.columns.iter().find(|c| c.name == n).unwrap().clone();
        let orders = t("orders");
        assert_eq!(
            orders.description.as_deref(),
            Some("Orders as the shop system records them.")
        );
        assert_eq!(orders.primary_key, ["order"]);
        assert_eq!(
            c(orders, "customer").references.as_deref(),
            Some("customers")
        );
        assert_eq!(c(orders, "ordered_at").datatype, Some("date"));
        assert_eq!(
            c(orders, "status").values,
            ["placed", "shipped", "returned"]
        );
        assert_eq!(
            c(orders, "status").description.as_deref(),
            Some("Where the order is.")
        );
        assert_eq!(c(orders, "order_total").datatype, Some("decimal"));
        assert!(
            orders.columns.iter().all(|c| c.name != "order_count"),
            "counts are left out"
        );
        let customers = t("customers");
        assert_eq!(
            c(customers, "segment").values,
            ["commuter", "racer", "club"]
        );
        // the base model's relation comes along; its plumbing doesn't
        assert_eq!(c(orders, "store_id").references.as_deref(), Some("stores"));
        assert!(orders.columns.iter().all(|c| c.name != "_loaded_at"));
    }

    #[test]
    fn references_follow_the_semantic_model_and_entities_win() {
        let text = String::from(
            r"
models:
  - name: dim_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]
  - name: fct_orders
    columns:
      - name: customer_id
        data_tests:
          - relationships: {to: ref('dim_customers'), field: customer_id}
      - name: amount_cents
      - name: is_gift
      - name: _loaded_at
  - name: returns
    columns:
      - name: order_id
        data_tests:
          - relationships: {to: ref('fct_orders'), field: order_id}
semantic_models:
  - name: customers
    model: ref('dim_customers')
    entities:
      - {name: customer, type: primary, expr: customer_id}
  - name: orders
    model: ref('fct_orders')
    entities:
      - {name: order, type: primary, expr: order_id}
      - {name: customer, type: foreign}
    measures:
      - {name: gift_revenue, agg: sum, expr: case when is_gift then amount_cents / 100 end}
",
        );
        let (m, left_out) = read(&[("p.yml".into(), text)], &HashMap::new());
        let t = |n: &str| m.tables.iter().find(|t| t.name == n).unwrap();
        // returns' key to fct_orders now names the semantic model standing for it
        assert_eq!(
            t("returns").columns[0].references.as_deref(),
            Some("orders")
        );
        // the base model's customer_id is the foreign entity's relation, not a second one
        let orders = t("orders");
        assert_eq!(
            orders
                .columns
                .iter()
                .filter(|c| c.references.as_deref() == Some("customers"))
                .count(),
            1
        );
        // columns only the measure's expression uses aren't repeated; plumbing is left out, counted
        assert!(orders
            .columns
            .iter()
            .all(|c| !matches!(c.name.as_str(), "is_gift" | "amount_cents" | "_loaded_at")));
        assert_eq!(left_out, 1);
    }

    #[test]
    fn model_properties_alone() {
        let m = model(&[(
            "p.yml".into(),
            "models:\n  - name: stores\n    columns:\n      - name: id\n        data_tests: [unique, not_null]\n      - name: city\n".into(),
        )]);
        assert_eq!(m.tables[0].primary_key, ["id"]);
        assert_eq!(ref_name("ref('shop', 'stores')"), "stores");
        assert_eq!(ref_name("ref(\"stores\")"), "stores");
    }

    fn read_one(text: &str) -> (Model, usize) {
        read(&[("p.yml".into(), text.into())], &HashMap::new())
    }

    #[test]
    fn a_second_relation_to_the_same_class_is_kept() {
        let (m, _) = read_one(
            r"
models:
  - name: dim_airports
    columns:
      - {name: airport_id, data_tests: [unique, not_null]}
  - name: fct_flights
    columns:
      - name: origin_airport_id
        data_tests: [{relationships: {to: ref('dim_airports'), field: airport_id}}]
      - name: destination_airport_id
        data_tests: [{relationships: {to: ref('dim_airports'), field: airport_id}}]
semantic_models:
  - name: airports
    model: ref('dim_airports')
    entities: [{name: airport, type: primary, expr: airport_id}]
  - name: flights
    model: ref('fct_flights')
    entities:
      - {name: flight, type: primary, expr: flight_id}
      - {name: airport, type: foreign, expr: origin_airport_id}
",
        );
        let flights = m.tables.iter().find(|t| t.name == "flights").unwrap();
        let to_airports: Vec<&str> = flights
            .columns
            .iter()
            .filter(|c| c.references.as_deref() == Some("airports"))
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(to_airports, ["airport", "destination_airport_id"]);
    }

    #[test]
    fn foreign_entities_find_their_table() {
        let (m, _) = read_one(
            r"
models:
  - name: stores
    columns: [{name: id, data_tests: [unique, not_null]}]
  - name: dim_regions
    columns: [{name: id, data_tests: [unique, not_null]}]
  - name: fct_orders
    columns:
      - name: region_key
        data_tests: [{relationships: {to: ref('dim_regions'), field: id}}]
semantic_models:
  - name: orders
    model: ref('fct_orders')
    entities:
      - {name: order, type: primary, expr: order_id}
      - {name: store, type: foreign, expr: store_id}
      - {name: region, type: foreign, expr: region_key}
",
        );
        let orders = m.tables.iter().find(|t| t.name == "orders").unwrap();
        let r = |n: &str| {
            orders
                .columns
                .iter()
                .find(|c| c.name == n)
                .unwrap()
                .references
                .clone()
        };
        // no semantic model is keyed by store: the model that reads as it
        assert_eq!(r("store").as_deref(), Some("stores"));
        // the column's relationships test names the target
        assert_eq!(r("region").as_deref(), Some("dim_regions"));
    }

    #[test]
    fn versioned_refs_sources_and_versions() {
        assert_eq!(ref_name("ref('dim_customers', v=2)"), "dim_customers");
        assert_eq!(
            ref_name("ref('shop', 'dim_customers', version=3)"),
            "dim_customers"
        );
        assert_eq!(ref_name("source('raw', 'customers')"), "customers");
        let (m, _) = read_one(
            r"
models:
  - name: dim_customers
    latest_version: 2
    columns:
      - {name: customer_id, data_tests: [unique, not_null]}
      - {name: legacy_code}
    versions:
      - v: 1
      - v: 2
        columns:
          - include: all
            exclude: [legacy_code]
          - {name: segment, data_tests: [{accepted_values: {values: [retail, trade]}}]}
semantic_models:
  - name: customers
    model: ref('dim_customers', v=2)
    entities: [{name: customer, type: primary, expr: customer_id}]
",
        );
        let names: Vec<&str> = m.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["customers"], "the versioned ref matches its model");
        let c = &m.tables[0];
        assert!(c.columns.iter().all(|c| c.name != "legacy_code"));
        assert_eq!(
            c.columns
                .iter()
                .find(|c| c.name == "segment")
                .unwrap()
                .values,
            ["retail", "trade"]
        );
    }

    #[test]
    fn counting_measures_are_left_out() {
        let (m, _) = read_one(
            "semantic_models:\n  - name: orders\n    entities: [{name: order, type: primary}]\n    measures:\n      - {name: order_count, agg: sum, expr: 1}\n      - {name: order_total, agg: sum, expr: amount}\n",
        );
        let names: Vec<&str> = m.tables[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, ["order_total"]);
        assert!(
            m.notes.iter().any(|n| n.contains("count rows")),
            "{:?}",
            m.notes
        );
    }

    #[test]
    fn doc_blocks_are_resolved_and_template_text_is_never_kept() {
        let blocks: HashMap<String, String> = doc_blocks(
            "{% docs orders_desc %}\nOne row per order.\n{% enddocs %}\n\n{%- docs status -%}Where it is.{%- enddocs -%}",
        )
        .into_iter()
        .collect();
        assert_eq!(blocks["orders_desc"], "One row per order.");
        assert_eq!(blocks["status"], "Where it is.");
        let (m, _) = read(
            &[(
                "p.yml".into(),
                "models:\n  - name: orders\n    description: \"{{ doc('orders_desc') }}\"\n    columns:\n      - {name: status, description: \"{{ doc('status') }}\"}\n      - {name: total, description: \"{{ doc('missing') }}\"}\n".into(),
            )],
            &blocks,
        );
        let t = &m.tables[0];
        assert_eq!(t.description.as_deref(), Some("One row per order."));
        assert_eq!(t.columns[0].description.as_deref(), Some("Where it is."));
        assert_eq!(t.columns[1].description, None);
        assert!(m.notes.iter().any(|n| n.contains("Jinja")), "{:?}", m.notes);
    }

    #[test]
    fn model_level_and_pre_1_9_constraints() {
        let (m, _) = read_one(
            r"
models:
  - name: stores
    columns: [{name: id}]
    constraints: [{type: primary_key, columns: [id]}]
  - name: orders
    columns:
      - {name: id}
      - {name: store_id}
      - name: region_id
        constraints: [{type: foreign_key, expression: 'analytics.regions (id)'}]
    constraints:
      - {type: primary_key, columns: [id]}
      - {type: foreign_key, columns: [store_id], to: ref('stores'), to_columns: [id]}
  - name: regions
    columns: [{name: id}]
",
        );
        let t = |n: &str| m.tables.iter().find(|t| t.name == n).unwrap();
        assert_eq!(t("stores").primary_key, ["id"]);
        let orders = t("orders");
        assert_eq!(orders.primary_key, ["id"]);
        let r = |n: &str| {
            orders
                .columns
                .iter()
                .find(|c| c.name == n)
                .unwrap()
                .references
                .clone()
        };
        assert_eq!(r("store_id").as_deref(), Some("stores"));
        assert_eq!(r("region_id").as_deref(), Some("regions"));
    }

    #[test]
    fn models_and_semantic_models_are_separate_name_spaces() {
        let (m, _) = read_one(
            r"
models:
  - name: dim_customers
    columns: [{name: customer_id, data_tests: [unique, not_null]}]
  - name: customers
    description: A staging copy, unrelated.
    columns: [{name: id, data_tests: [unique, not_null]}, {name: raw_name}]
semantic_models:
  - name: customers
    model: ref('dim_customers')
    entities: [{name: customer, type: primary, expr: customer_id}]
",
        );
        let names: Vec<&str> = m.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["customers", "customers_model"]);
        assert!(
            m.notes.iter().any(|n| n.contains("customers_model")),
            "{:?}",
            m.notes
        );
    }

    #[test]
    fn values_are_kept_as_written_and_bad_files_are_noted() {
        let (m, _) = read(
            &[
                (
                    "a.yml".into(),
                    "models:\n  - name: countries\n    columns:\n      - name: code\n        data_tests: [{accepted_values: {values: [GB, NO, SE, 007]}}]\n".into(),
                ),
                ("broken.yml".into(), "models: [unclosed".into()),
            ],
            &HashMap::new(),
        );
        assert_eq!(m.tables[0].columns[0].values, ["GB", "NO", "SE", "007"]);
        assert!(
            m.notes.iter().any(|n| n.starts_with("broken.yml")),
            "{:?}",
            m.notes
        );
    }
}
