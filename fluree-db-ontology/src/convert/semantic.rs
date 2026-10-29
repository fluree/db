//! Tables from a warehouse's semantic layer: a Snowflake semantic view (or
//! Cortex Analyst semantic model) as YAML, and a Databricks Genie space as
//! the JSON its API exports.
//!
//! A semantic layer is a data dictionary: tables with descriptions and
//! synonyms, columns with types, sample values and descriptions, and the
//! joins between tables. All of that becomes the same table model as DDL
//! does, so the same rules apply: a join is a foreign key from the left
//! table's column to the right table (a relation labelled for the column,
//! `ACCOUNT_ID` becoming "account"), the right table's join column is a key
//! and is left out, and an `is_enum` dimension with `sample_values` is
//! enumerated. Metrics, filters, verified queries, SQL snippets and
//! instructions describe queries, not things or relations, and are left out.

use super::sql::xsd_for;
use super::{class_label, Column, Model, Table};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum SemanticError {
    #[error("invalid YAML: {0}")]
    Yaml(String),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Shape(String),
}

fn text(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
}

fn strings(v: &Value, key: &str) -> Vec<String> {
    match v.get(key) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|i| match i {
                Value::String(s) => Some(s.trim().to_string()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            })
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// The last part of a dotted name: `main.crm.accounts` → `accounts`.
fn last(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('`')
        .to_string()
}

/// A join, as both formats state one.
struct Join {
    left: String,
    right: String,
    columns: Vec<(String, String)>,
}

/// Apply joins: the left column refers to the right table, and the right
/// column is a key of its table.
fn apply_joins(tables: &mut [Table], joins: &[Join]) {
    for join in joins {
        let Some((left_col, right_col)) = join.columns.first() else {
            continue;
        };
        if let Some(t) = tables
            .iter_mut()
            .find(|t| t.name.eq_ignore_ascii_case(&join.left))
        {
            match t
                .columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(left_col))
            {
                Some(c) => c.references = Some(join.right.clone()),
                None => t.columns.push(Column {
                    name: left_col.clone(),
                    references: Some(join.right.clone()),
                    ..Column::default()
                }),
            }
            // the other columns of a composite key are part of the same relation
            for (other, _) in join.columns.iter().skip(1) {
                t.columns.retain(|c| !c.name.eq_ignore_ascii_case(other));
            }
        }
        if let Some(t) = tables
            .iter_mut()
            .find(|t| t.name.eq_ignore_ascii_case(&join.right))
        {
            if !t
                .primary_key
                .iter()
                .any(|k| k.eq_ignore_ascii_case(right_col))
            {
                t.primary_key.push(right_col.clone());
            }
        }
    }
}

/// A Snowflake semantic view or Cortex Analyst semantic model, as YAML.
pub fn from_snowflake(yaml: &str) -> Result<Model, SemanticError> {
    let doc: Value =
        serde_saphyr::from_str(yaml).map_err(|e| SemanticError::Yaml(e.to_string()))?;
    let Some(table_values) = doc.get("tables").and_then(Value::as_array) else {
        return Err(SemanticError::Shape(
            "no `tables:` list: not a semantic view or semantic model".into(),
        ));
    };
    let mut tables = Vec::new();
    for t in table_values {
        let Some(name) = text(t, "name") else {
            continue;
        };
        let mut table = Table {
            name,
            description: text(t, "description"),
            synonyms: strings(t, "synonyms"),
            primary_key: t
                .get("primary_key")
                .map(|pk| strings(pk, "columns"))
                .unwrap_or_default(),
            ..Table::default()
        };
        for group in ["dimensions", "time_dimensions", "facts", "measures"] {
            for c in t.get(group).and_then(Value::as_array).into_iter().flatten() {
                let Some(name) = text(c, "name") else {
                    continue;
                };
                let enumerated = c.get("is_enum").and_then(Value::as_bool).unwrap_or(false);
                table.columns.push(Column {
                    name,
                    datatype: text(c, "data_type").as_deref().and_then(xsd_for),
                    description: text(c, "description"),
                    synonyms: strings(c, "synonyms"),
                    values: if enumerated {
                        strings(c, "sample_values")
                    } else {
                        Vec::new()
                    },
                    references: None,
                });
            }
        }
        tables.push(table);
    }
    let joins: Vec<Join> = doc
        .get("relationships")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|r| {
            Some(Join {
                left: text(r, "left_table")?,
                right: text(r, "right_table")?,
                columns: r
                    .get("relationship_columns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|c| Some((text(c, "left_column")?, text(c, "right_column")?)))
                    .collect(),
            })
        })
        .collect();
    if tables.is_empty() {
        return Err(SemanticError::Shape(
            "the semantic view names no tables".into(),
        ));
    }
    apply_joins(&mut tables, &joins);
    Ok(Model { tables })
}

/// Whether a JSON document is a Genie space export rather than JSON-LD.
pub fn is_genie(json: &Value) -> bool {
    json.get("serialized_space").is_some() || json.get("data_sources").is_some()
}

/// The `` `alias`.`column` `` references in a join's SQL, for one alias.
fn alias_columns(sql: &str, alias: &str) -> Vec<String> {
    let needle = format!("`{alias}`.`");
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(at) = rest.find(&needle) {
        rest = &rest[at + needle.len()..];
        if let Some(end) = rest.find('`') {
            out.push(rest[..end].to_string());
            rest = &rest[end..];
        }
    }
    out
}

/// A Genie space export. The export carries no descriptions: those are Unity
/// Catalog's table and column comments, given as `comments`
/// (`{"table": {"": "table comment", "column": "column comment"}}`).
pub fn from_genie(export: &Value, comments: Option<&Value>) -> Result<Model, SemanticError> {
    let parsed;
    let mut space = export.get("serialized_space").unwrap_or(export);
    if let Value::String(s) = space {
        parsed = serde_json::from_str::<Value>(s)?;
        space = &parsed;
    }
    let sources = space.get("data_sources").cloned().unwrap_or(Value::Null);
    let comment = |table: &str, column: &str| -> Option<String> {
        comments?
            .get(table)
            .or_else(|| {
                comments?
                    .as_object()?
                    .iter()
                    .find(|(k, _)| {
                        k.eq_ignore_ascii_case(table) || last(k).eq_ignore_ascii_case(table)
                    })
                    .map(|(_, v)| v)
            })?
            .get(column)?
            .as_str()
            .map(str::to_string)
            .filter(|s| !s.is_empty())
    };
    let mut tables = Vec::new();
    for t in ["tables", "metric_views"]
        .iter()
        .filter_map(|k| sources.get(*k).and_then(Value::as_array))
        .flatten()
    {
        let Some(identifier) = text(t, "identifier") else {
            continue;
        };
        let name = last(&identifier);
        let mut table = Table {
            description: text(t, "description").or_else(|| comment(&name, "")),
            name: name.clone(),
            ..Table::default()
        };
        for c in t
            .get("column_configs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(column) = text(c, "column_name") else {
                continue;
            };
            table.columns.push(Column {
                description: text(c, "description").or_else(|| comment(&name, &column)),
                name: column,
                ..Column::default()
            });
        }
        // the export declares no keys: `id`, or the table's name with `_id`
        let own = class_label(&name);
        table.primary_key = table
            .columns
            .iter()
            .filter(|c| {
                let words = crate::words::name_words(&c.name).unwrap_or_default();
                words == "id"
                    || words
                        .strip_suffix(" id")
                        .is_some_and(|stem| class_label(stem) == own)
            })
            .map(|c| c.name.clone())
            .collect();
        tables.push(table);
    }
    if tables.is_empty() {
        return Err(SemanticError::Shape(
            "no tables under `data_sources`: not a Genie space export".into(),
        ));
    }
    let joins: Vec<Join> = space
        .get("instructions")
        .and_then(|i| i.get("join_specs"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|j| {
            let side = |key: &str| -> Option<(String, String)> {
                let s = j.get(key)?;
                let identifier = text(s, "identifier")?;
                let alias = text(s, "alias").unwrap_or_else(|| last(&identifier));
                Some((last(&identifier), alias))
            };
            let (left, left_alias) = side("left")?;
            let (right, right_alias) = side("right")?;
            let sql = strings(j, "sql").join(" ");
            let columns = alias_columns(&sql, &left_alias)
                .into_iter()
                .zip(alias_columns(&sql, &right_alias))
                .collect();
            Some(Join {
                left,
                right,
                columns,
            })
        })
        .collect();
    apply_joins(&mut tables, &joins);
    Ok(Model { tables })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIEW: &str = r"
name: bike_shop
description: Sales for a chain of bike shops.
tables:
  - name: CUSTOMERS
    synonyms: [riders, clients]
    description: People and clubs who buy from the shops.
    primary_key: {columns: [CUSTOMER_ID]}
    dimensions:
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: TEXT
      - name: FULL_NAME
        synonyms: [customer name]
        description: The customer's name.
        expr: FULL_NAME
        data_type: VARCHAR(200)
      - name: SEGMENT
        description: Kind of customer.
        data_type: TEXT
        sample_values: [Commuter, Racer, Club]
        is_enum: true
      - name: HOME_STORE
        data_type: TEXT
        sample_values: [Leeds, York]
  - name: ORDERS
    description: Sales, one row per order.
    primary_key: {columns: [ORDER_ID]}
    dimensions:
      - name: ORDER_ID
        data_type: NUMBER(38,0)
      - name: CUSTOMER_ID
        data_type: TEXT
    time_dimensions:
      - name: ORDERED_AT
        data_type: TIMESTAMP_NTZ
    facts:
      - name: TOTAL
        description: Order total in pounds.
        data_type: NUMBER(10,2)
relationships:
  - name: orders_to_customers
    left_table: ORDERS
    right_table: CUSTOMERS
    relationship_columns: [{left_column: CUSTOMER_ID, right_column: CUSTOMER_ID}]
metrics:
  - name: REVENUE
    expr: SUM(ORDERS.TOTAL)
verified_queries:
  - name: top riders
    sql: SELECT 1
";

    #[test]
    fn snowflake_view() {
        let m = from_snowflake(VIEW).unwrap();
        assert_eq!(m.tables.len(), 2);
        let customers = &m.tables[0];
        assert_eq!(customers.synonyms, ["riders", "clients"]);
        assert_eq!(customers.primary_key, ["CUSTOMER_ID"]);
        let col = |t: &Table, n: &str| t.columns.iter().find(|c| c.name == n).unwrap().clone();
        assert_eq!(
            col(customers, "SEGMENT").values,
            ["Commuter", "Racer", "Club"]
        );
        assert!(
            col(customers, "HOME_STORE").values.is_empty(),
            "samples alone don't enumerate"
        );
        assert_eq!(col(customers, "FULL_NAME").datatype, Some("string"));
        let orders = &m.tables[1];
        assert_eq!(
            col(orders, "CUSTOMER_ID").references.as_deref(),
            Some("CUSTOMERS")
        );
        assert_eq!(col(orders, "ORDERED_AT").datatype, Some("dateTime"));
        assert_eq!(col(orders, "TOTAL").datatype, Some("decimal"));
    }

    #[test]
    fn not_a_view() {
        assert!(from_snowflake("semantic_models: []").is_err());
        assert!(from_snowflake("tables: []").is_err());
        assert!(from_snowflake("tables: [unclosed").is_err());
    }

    #[test]
    fn genie_space() {
        let space = serde_json::json!({
            "data_sources": {
                "tables": [
                    {"identifier": "main.shop.customers",
                     "column_configs": [{"column_name": "id"}, {"column_name": "full_name"}]},
                    {"identifier": "main.shop.orders",
                     "column_configs": [{"column_name": "order_id"}, {"column_name": "customer_ref"},
                                        {"column_name": "total"}]}
                ]
            },
            "instructions": {"join_specs": [{
                "left": {"identifier": "main.shop.orders", "alias": "o"},
                "right": {"identifier": "main.shop.customers", "alias": "c"},
                "sql": ["`o`.`customer_ref` = `c`.`id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
            }]}
        });
        let export = serde_json::json!({"serialized_space": space.to_string()});
        assert!(is_genie(&export));
        let comments =
            serde_json::json!({"customers": {"": "People who buy.", "full_name": "Their name."}});
        let m = from_genie(&export, Some(&comments)).unwrap();
        assert_eq!(m.tables[0].description.as_deref(), Some("People who buy."));
        assert_eq!(
            m.tables[0].columns[1].description.as_deref(),
            Some("Their name.")
        );
        assert_eq!(m.tables[0].primary_key, ["id"]);
        assert_eq!(m.tables[1].primary_key, ["order_id"]);
        assert_eq!(
            m.tables[1].columns[1].references.as_deref(),
            Some("customers")
        );
    }
}
