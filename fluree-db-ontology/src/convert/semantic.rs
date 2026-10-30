//! Tables from a warehouse's semantic layer: a Snowflake semantic view (or
//! Cortex Analyst semantic model) as YAML, and a Databricks Genie space as
//! the JSON its API exports.
//!
//! A semantic layer is a data dictionary: tables with descriptions and
//! synonyms, columns with types, sample values and descriptions, and the
//! joins between tables. All of that becomes the same table model as DDL
//! does, so the same rules apply: a join is a foreign key from its many side
//! to its one side (a relation labelled for the column, `ACCOUNT_ID` becoming
//! "account"), the one side's join column is a key and is left out, and an
//! `is_enum` dimension with `sample_values` is enumerated. Metrics, filters,
//! verified queries, SQL snippets and instructions describe queries, not
//! things or relations, and are left out.
//!
//! YAML is read into typed fields, so values are kept as written: YAML's
//! implicit typing would turn a dimension named `NO` into `false` and a
//! sample value `007` into `7`.

use super::sql::xsd_for;
use super::{class_label, from_yaml, is_jsonld, Column, Model, Table};
use serde::Deserialize;
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

/// A text field: a string, or (as Genie writes them) a list of strings.
fn text(v: &Value, key: &str) -> Option<String> {
    let joined = match v.get(key)? {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
        _ => return None,
    };
    let s = joined.split_whitespace().collect::<Vec<_>>().join(" ");
    (!s.is_empty()).then_some(s)
}

fn strings(v: &Value, key: &str) -> Vec<String> {
    match v.get(key) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|i| i.as_str().map(|s| s.trim().to_string()))
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

/// A join, with its many side on the left (a one-to-many join is turned
/// round). A many-to-many join has no one side, and makes no key.
struct Join {
    left: String,
    right: String,
    columns: Vec<(String, String)>,
    many_to_many: bool,
}

/// Apply joins: the left column refers to the right table, and the right
/// column is a key of its table. Notes what couldn't be placed.
fn apply_joins(tables: &mut [Table], joins: &[Join], notes: &mut Vec<String>) {
    let mut unknown = 0;
    for join in joins {
        let Some((left_col, right_col)) = join.columns.first() else {
            continue;
        };
        let has = |name: &str| tables.iter().any(|t| t.name.eq_ignore_ascii_case(name));
        if !has(&join.left) || !has(&join.right) {
            unknown += 1;
            continue;
        }
        if let Some(t) = tables
            .iter_mut()
            .find(|t| t.name.eq_ignore_ascii_case(&join.left))
        {
            match t
                .columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(left_col))
            {
                // a second join on the same column is a relation of its own,
                // named for the table it reaches, not an overwrite
                Some(c)
                    if c.references
                        .as_ref()
                        .is_some_and(|r| !r.eq_ignore_ascii_case(&join.right)) =>
                {
                    t.columns.push(Column {
                        name: join.right.clone(),
                        references: Some(join.right.clone()),
                        ..Column::default()
                    });
                }
                Some(c) => c.references = Some(join.right.clone()),
                None => t.columns.push(Column {
                    name: left_col.clone(),
                    references: Some(join.right.clone()),
                    ..Column::default()
                }),
            }
        }
        if join.many_to_many {
            continue;
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
    if unknown > 0 {
        notes.push(format!(
            "{unknown} join(s) name a table the input doesn't have, and were passed over"
        ));
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct View {
    tables: Option<Vec<ViewTable>>,
    relationships: Option<Vec<ViewRelationship>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ViewTable {
    name: Option<String>,
    description: Option<String>,
    synonyms: Option<Vec<String>>,
    primary_key: Option<ViewKey>,
    dimensions: Option<Vec<ViewColumn>>,
    time_dimensions: Option<Vec<ViewColumn>>,
    facts: Option<Vec<ViewColumn>>,
    measures: Option<Vec<ViewColumn>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ViewKey {
    columns: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ViewColumn {
    name: Option<String>,
    data_type: Option<String>,
    description: Option<String>,
    synonyms: Option<Vec<String>>,
    sample_values: Option<Vec<String>>,
    is_enum: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ViewRelationship {
    left_table: Option<String>,
    right_table: Option<String>,
    relationship_type: Option<String>,
    relationship_columns: Option<Vec<ViewJoinColumns>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ViewJoinColumns {
    left_column: Option<String>,
    right_column: Option<String>,
}

fn clean(s: Option<String>) -> Option<String> {
    s.map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
}

fn clean_all(v: Option<Vec<String>>) -> Vec<String> {
    v.unwrap_or_default()
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// A Snowflake semantic view or Cortex Analyst semantic model, as YAML.
pub fn from_snowflake(yaml: &str) -> Result<Model, SemanticError> {
    let view: View = from_yaml(yaml).map_err(SemanticError::Yaml)?;
    let Some(view_tables) = view.tables else {
        return Err(SemanticError::Shape(
            "no `tables:` list: not a semantic view or semantic model".into(),
        ));
    };
    let mut notes = Vec::new();
    let mut tables = Vec::new();
    let mut unnamed = 0;
    for t in view_tables {
        let Some(name) = clean(t.name) else {
            unnamed += 1;
            continue;
        };
        let mut table = Table {
            name,
            description: clean(t.description),
            synonyms: clean_all(t.synonyms),
            primary_key: t
                .primary_key
                .map(|k| clean_all(k.columns))
                .unwrap_or_default(),
            ..Table::default()
        };
        for c in [t.dimensions, t.time_dimensions, t.facts, t.measures]
            .into_iter()
            .flatten()
            .flatten()
        {
            let Some(name) = clean(c.name) else {
                unnamed += 1;
                continue;
            };
            table.columns.push(Column {
                name,
                datatype: c.data_type.as_deref().and_then(xsd_for),
                description: clean(c.description),
                synonyms: clean_all(c.synonyms),
                values: if c.is_enum.unwrap_or(false) {
                    clean_all(c.sample_values)
                } else {
                    Vec::new()
                },
                references: None,
            });
        }
        tables.push(table);
    }
    if unnamed > 0 {
        notes.push(format!(
            "{unnamed} table(s) or column(s) without a name were left out"
        ));
    }
    if tables.is_empty() {
        return Err(SemanticError::Shape(
            "the semantic view names no tables".into(),
        ));
    }
    let joins: Vec<Join> = view
        .relationships
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| {
            let columns: Vec<(String, String)> = r
                .relationship_columns
                .unwrap_or_default()
                .into_iter()
                .filter_map(|c| Some((clean(c.left_column)?, clean(c.right_column)?)))
                .collect();
            let kind = r.relationship_type.unwrap_or_default().to_ascii_lowercase();
            let (left, right) = (clean(r.left_table)?, clean(r.right_table)?);
            Some(if kind == "one_to_many" {
                Join {
                    left: right,
                    right: left,
                    columns: columns.into_iter().map(|(l, r)| (r, l)).collect(),
                    many_to_many: false,
                }
            } else {
                Join {
                    left,
                    right,
                    columns,
                    many_to_many: kind == "many_to_many",
                }
            })
        })
        .collect();
    apply_joins(&mut tables, &joins, &mut notes);
    Ok(Model { tables, notes })
}

/// Whether a JSON document is a Genie space export rather than JSON-LD.
pub fn is_genie(json: &Value) -> bool {
    !is_jsonld(json)
        && (json.get("serialized_space").is_some() || json.get("data_sources").is_some())
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

/// A Genie space export. The export carries no descriptions of its own (in
/// some versions, only for columns that have settings): those are Unity
/// Catalog's table and column comments, given as `comments`
/// (`{"table": {"": "table comment", "column": "column comment"}}`). Columns
/// the comments name that the export doesn't list are added.
pub fn from_genie(export: &Value, comments: Option<&Value>) -> Result<Model, SemanticError> {
    let parsed;
    let mut space = export.get("serialized_space").unwrap_or(export);
    if let Value::String(s) = space {
        parsed = serde_json::from_str::<Value>(s)?;
        space = &parsed;
    }
    let sources = space.get("data_sources").cloned().unwrap_or(Value::Null);
    let comments_of = |table: &str| -> Option<&serde_json::Map<String, Value>> {
        let map = comments?.as_object()?;
        map.get(table)
            .or_else(|| {
                map.iter()
                    .find(|(k, _)| {
                        k.eq_ignore_ascii_case(table) || last(k).eq_ignore_ascii_case(table)
                    })
                    .map(|(_, v)| v)
            })?
            .as_object()
    };
    let mut notes = Vec::new();
    let mut tables = Vec::new();
    let mut excluded = 0;
    let mut added = 0;
    for t in ["tables", "metric_views"]
        .iter()
        .filter_map(|k| sources.get(*k).and_then(Value::as_array))
        .flatten()
    {
        let Some(identifier) = text(t, "identifier") else {
            continue;
        };
        let name = last(&identifier);
        let table_comments = comments_of(&name);
        let comment = |column: &str| -> Option<String> {
            table_comments?
                .get(column)?
                .as_str()
                .map(str::to_string)
                .filter(|s| !s.is_empty())
        };
        let mut table = Table {
            description: text(t, "description").or_else(|| comment("")),
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
            if c.get("exclude").and_then(Value::as_bool).unwrap_or(false) {
                excluded += 1;
                continue;
            }
            table.columns.push(Column {
                description: text(c, "description").or_else(|| comment(&column)),
                synonyms: strings(c, "synonyms"),
                name: column,
                ..Column::default()
            });
        }
        // columns the export doesn't list (it may list only those with
        // settings), from Unity Catalog's comments
        if let Some(map) = table_comments {
            for (column, text) in map {
                if column.is_empty()
                    || table
                        .columns
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(column))
                {
                    continue;
                }
                added += 1;
                table.columns.push(Column {
                    name: column.clone(),
                    description: text.as_str().map(str::to_string).filter(|s| !s.is_empty()),
                    ..Column::default()
                });
            }
        }
        // the export declares no keys: `id`, or the table's name with `_id`
        let own = class_label(&name);
        table.primary_key = table
            .columns
            .iter()
            .filter(|c| {
                let words = crate::words::name_words(&c.name).unwrap_or_default();
                words.eq_ignore_ascii_case("id")
                    || super::strip_word_suffix(&words, "id")
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
    if excluded > 0 {
        notes.push(format!(
            "{excluded} column(s) the space excludes were left out"
        ));
    }
    if added > 0 {
        notes.push(format!(
            "{added} column(s) added from --comments that the export doesn't list"
        ));
    }
    if let Some(map) = comments.and_then(Value::as_object) {
        let unmatched = map
            .keys()
            .filter(|k| {
                !tables.iter().any(|t| {
                    t.name.eq_ignore_ascii_case(k) || t.name.eq_ignore_ascii_case(&last(k))
                })
            })
            .count();
        if unmatched > 0 {
            notes.push(format!(
                "{unmatched} table(s) in --comments match no table in the export"
            ));
        }
    }
    let mut unread = 0;
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
            let columns: Vec<(String, String)> = alias_columns(&sql, &left_alias)
                .into_iter()
                .zip(alias_columns(&sql, &right_alias))
                .collect();
            if columns.is_empty() {
                unread += 1;
                return None;
            }
            let kind = sql
                .split("--rt=FROM_RELATIONSHIP_TYPE_")
                .nth(1)
                .and_then(|rest| rest.split("--").next())
                .unwrap_or("MANY_TO_ONE")
                .to_ascii_uppercase();
            Some(if kind == "ONE_TO_MANY" {
                Join {
                    left: right,
                    right: left,
                    columns: columns.into_iter().map(|(l, r)| (r, l)).collect(),
                    many_to_many: false,
                }
            } else {
                Join {
                    left,
                    right,
                    columns,
                    many_to_many: kind == "MANY_TO_MANY",
                }
            })
        })
        .collect();
    if unread > 0 {
        notes.push(format!(
            "{unread} join(s) whose SQL names no `alias`.`column` pairs were passed over"
        ));
    }
    apply_joins(&mut tables, &joins, &mut notes);
    Ok(Model { tables, notes })
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

    #[test]
    fn yaml_values_are_kept_as_written() {
        let m = from_snowflake(
            r"
tables:
  - name: COUNTRIES
    synonyms: [no, yes]
    primary_key: {columns: [CODE]}
    dimensions:
      - name: CODE
        data_type: TEXT
      - name: NO
        description: A column really named NO.
        data_type: TEXT
      - name: REGION
        data_type: TEXT
        is_enum: true
        sample_values: [GB, NO, SE, 007, .inf]
      - name: REGION
        description: the later duplicate wins
        data_type: TEXT
",
        )
        .unwrap();
        let t = &m.tables[0];
        assert_eq!(t.synonyms, ["no", "yes"]);
        assert!(t.columns.iter().any(|c| c.name == "NO"), "{:?}", t.columns);
        let region: Vec<&Column> = t.columns.iter().filter(|c| c.name == "REGION").collect();
        assert_eq!(region.len(), 2, "two dimensions, both kept as listed");
        assert_eq!(region[0].values, ["GB", "NO", "SE", "007", ".inf"]);
    }

    #[test]
    fn duplicate_keys_keep_the_last_value() {
        let m = from_snowflake(
            "tables:\n  - name: A\n    description: first\n    description: second\n",
        )
        .unwrap();
        assert_eq!(m.tables[0].description.as_deref(), Some("second"));
    }

    fn genie(join_sql: &[&str]) -> Value {
        serde_json::json!({"serialized_space": serde_json::json!({
            "data_sources": {"tables": [
                {"identifier": "main.shop.customers", "description": ["People ", "who buy."],
                 "column_configs": [
                    {"column_name": "id"},
                    {"column_name": "full_name", "synonyms": ["name"]},
                    {"column_name": "ssn", "exclude": true}]},
                {"identifier": "main.shop.orders",
                 "column_configs": [{"column_name": "order_id"}, {"column_name": "customer_ref"}]}
            ]},
            "instructions": {"join_specs": [{
                "left": {"identifier": "main.shop.customers", "alias": "c"},
                "right": {"identifier": "main.shop.orders", "alias": "o"},
                "sql": join_sql
            }]}
        }).to_string()})
    }

    #[test]
    fn genie_join_types() {
        // one-to-many from customers to orders: orders.customer_ref is the key
        let m = from_genie(
            &genie(&[
                "`c`.`id` = `o`.`customer_ref`",
                "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--",
            ]),
            None,
        )
        .unwrap();
        let t = |n: &str| m.tables.iter().find(|t| t.name == n).unwrap();
        assert_eq!(t("customers").primary_key, ["id"]);
        assert_eq!(
            t("orders").columns[1].references.as_deref(),
            Some("customers")
        );
        assert!(t("orders").primary_key.iter().all(|k| k != "customer_ref"));
        assert_eq!(
            t("customers").description.as_deref(),
            Some("People who buy.")
        );
        assert_eq!(t("customers").columns[1].synonyms, ["name"]);
        assert!(t("customers").columns.iter().all(|c| c.name != "ssn"));
        assert!(
            m.notes.iter().any(|n| n.contains("excludes")),
            "{:?}",
            m.notes
        );

        // many-to-many makes no key on either side
        let m = from_genie(
            &genie(&[
                "`c`.`id` = `o`.`customer_ref`",
                "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_MANY--",
            ]),
            None,
        )
        .unwrap();
        let orders = m.tables.iter().find(|t| t.name == "orders").unwrap();
        assert_eq!(orders.primary_key, ["order_id"]);

        // join SQL without backticks can't be read, and says so
        let m = from_genie(&genie(&["c.id = o.customer_ref"]), None).unwrap();
        assert!(
            m.notes.iter().any(|n| n.contains("names no")),
            "{:?}",
            m.notes
        );
    }

    #[test]
    fn genie_comments_add_columns_and_unmatched_ones_are_counted() {
        let comments = serde_json::json!({
            "customers": {"": "People who buy.", "email": "Where to write."},
            "suppliers": {"": "Not in the space."}
        });
        let m = from_genie(&genie(&["`c`.`id` = `o`.`customer_ref`"]), Some(&comments)).unwrap();
        let customers = m.tables.iter().find(|t| t.name == "customers").unwrap();
        let email = customers
            .columns
            .iter()
            .find(|c| c.name == "email")
            .unwrap();
        assert_eq!(email.description.as_deref(), Some("Where to write."));
        assert!(
            m.notes.iter().any(|n| n.contains("added from --comments")),
            "{:?}",
            m.notes
        );
        assert!(
            m.notes.iter().any(|n| n.contains("match no table")),
            "{:?}",
            m.notes
        );
    }

    #[test]
    fn json_ld_is_never_a_genie_export() {
        assert!(!is_genie(
            &serde_json::json!({"@context": {}, "data_sources": {}})
        ));
    }

    #[test]
    fn composite_and_repeated_joins_drop_nothing() {
        let m = from_snowflake(
            r"
tables:
  - name: ORDERS
    primary_key: {columns: [ORDER_ID]}
    dimensions: [{name: ORDER_ID}]
  - name: ORDER_LINES
    primary_key: {columns: [ORDER_ID, LINE_NO]}
    dimensions: [{name: ORDER_ID}, {name: LINE_NO}]
  - name: LINE_NOTES
    dimensions: [{name: ORDER_ID}, {name: LINE_NO}, {name: NOTE}]
relationships:
  - name: notes_to_orders
    left_table: LINE_NOTES
    right_table: ORDERS
    relationship_columns: [{left_column: ORDER_ID, right_column: ORDER_ID}]
  - name: notes_to_lines
    left_table: LINE_NOTES
    right_table: ORDER_LINES
    relationship_columns:
      - {left_column: ORDER_ID, right_column: ORDER_ID}
      - {left_column: LINE_NO, right_column: LINE_NO}
  - name: to_nowhere
    left_table: LINE_NOTES
    right_table: SHIPMENTS
    relationship_columns: [{left_column: NOTE, right_column: ID}]
",
        )
        .unwrap();
        let notes = m.tables.iter().find(|t| t.name == "LINE_NOTES").unwrap();
        let col = |n: &str| notes.columns.iter().find(|c| c.name == n);
        assert_eq!(
            col("ORDER_ID").unwrap().references.as_deref(),
            Some("ORDERS")
        );
        assert_eq!(
            col("ORDER_LINES").unwrap().references.as_deref(),
            Some("ORDER_LINES")
        );
        assert!(
            col("LINE_NO").is_some(),
            "a composite join's other column stays"
        );
        assert!(
            m.notes.iter().any(|n| n.contains("doesn't have")),
            "{:?}",
            m.notes
        );
    }
}
