//! Tables from a dbt project's YAML: semantic models (MetricFlow) and model
//! properties (`schema.yml`).
//!
//! A **semantic model** names its entities, dimensions and measures:
//!
//! - a `primary`, `unique` or `natural` entity is the model's key, left out;
//! - a `foreign` entity is a relation to the semantic model whose primary
//!   entity has the same name (`customer` → the model keyed by `customer`),
//!   labelled with the entity's name;
//! - a dimension is a property (a `time` dimension a date, or a date-time
//!   when its granularity is finer than a day);
//! - a measure that aggregates a column's values (`sum`, `average`, `min`,
//!   `max`, `median`, `percentile`) is a decimal property; counts are about
//!   rows, not things, and metrics and saved queries are left out.
//!
//! A **model's properties** give its columns' descriptions and tests:
//! `accepted_values` makes a column enumerated, `relationships` (or a
//! `foreign_key` constraint) makes it a foreign key, and a `primary_key`
//! constraint, or `unique` with `not_null` on `id`, makes it the key.
//!
//! A semantic model built on a model (`model: ref('orders')`) stands for it:
//! it takes the model's descriptions and accepted values for the columns it
//! uses, and the model is not repeated. Of the model's other columns, those
//! that carry structure the semantic model lacks (a relation, from a
//! `relationships` test or `foreign_key` constraint, or accepted values) are
//! kept; the rest (audit columns, surrogate keys, columns only an
//! expression uses) are left out and counted. A semantic model's choice of
//! dimensions is made for metrics, so a relation it didn't need is still
//! something documents talk about; but a mart's every documented column
//! would bring back the breadth that costs an extraction model accuracy.
//! References to a replaced model resolve to the semantic model.

use super::sql::xsd_for;
use super::{class_label, Column, Model, Table};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

fn text(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
}

fn list<'a>(v: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    v.get(key).and_then(Value::as_array).into_iter().flatten()
}

/// The model a `ref('orders')` (or `ref("orders")`, or a bare name) names.
fn ref_name(text: &str) -> String {
    let t = text.trim();
    let inner = t
        .strip_prefix("ref(")
        .and_then(|r| r.strip_suffix(')'))
        .unwrap_or(t);
    // ref('package', 'model') names the model last
    inner
        .rsplit(',')
        .next()
        .unwrap_or(inner)
        .trim()
        .trim_matches(|c| c == '\'' || c == '"')
        .to_string()
}

/// A test's arguments: dbt 1.10 puts them under `arguments:`, earlier
/// versions beside the test's name.
fn arguments(test: &Value) -> &Value {
    test.get("arguments").unwrap_or(test)
}

/// Whether a document has anything this module reads.
pub fn is_dbt(doc: &Value) -> bool {
    doc.get("semantic_models").is_some() || doc.get("models").is_some()
}

/// The names a semantic model uses: its entities, dimensions and measures,
/// and every identifier in their expressions (lowercased).
fn used_names(sm: &Value) -> HashSet<String> {
    let mut used = HashSet::new();
    for group in ["entities", "dimensions", "measures"] {
        for item in list(sm, group) {
            if let Some(name) = text(item, "name") {
                used.insert(name.to_lowercase());
            }
            if let Some(expr) = text(item, "expr") {
                used.extend(
                    expr.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                        .filter(|t| !t.is_empty())
                        .map(str::to_lowercase),
                );
            }
        }
    }
    used
}

/// The model for one or more dbt YAML documents (a project's files).
pub fn model(docs: &[Value]) -> Model {
    read(docs).0
}

/// The model, and how many columns of models that semantic models stand for
/// were left out.
pub fn read(docs: &[Value]) -> (Model, usize) {
    let mut tables: Vec<Table> = Vec::new();
    // model properties first: semantic models take their descriptions
    for doc in docs {
        for m in list(doc, "models") {
            let Some(name) = text(m, "name") else {
                continue;
            };
            let mut table = Table {
                name: name.clone(),
                description: text(m, "description"),
                ..Table::default()
            };
            for c in list(m, "columns") {
                let Some(column_name) = text(c, "name") else {
                    continue;
                };
                let mut column = Column {
                    datatype: text(c, "data_type").as_deref().and_then(xsd_for),
                    description: text(c, "description"),
                    name: column_name.clone(),
                    ..Column::default()
                };
                let (mut unique, mut not_null) = (false, false);
                for test in list(c, "data_tests").chain(list(c, "tests")) {
                    match test {
                        Value::String(t) if t == "unique" => unique = true,
                        Value::String(t) if t == "not_null" => not_null = true,
                        Value::Object(map) => {
                            if let Some(t) = map.get("accepted_values") {
                                column.values = arguments(t)
                                    .get("values")
                                    .and_then(Value::as_array)
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|v| match v {
                                        Value::String(s) => Some(s.clone()),
                                        Value::Number(n) => Some(n.to_string()),
                                        _ => None,
                                    })
                                    .collect();
                            }
                            if let Some(t) = map.get("relationships") {
                                if let Some(to) = arguments(t).get("to").and_then(Value::as_str) {
                                    column.references = Some(ref_name(to));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                for constraint in list(c, "constraints") {
                    match text(constraint, "type").as_deref() {
                        Some("primary_key") => table.primary_key.push(column_name.clone()),
                        Some("foreign_key") => {
                            if let Some(to) = text(constraint, "to") {
                                column.references = Some(ref_name(&to));
                            }
                        }
                        _ => {}
                    }
                }
                let words = crate::words::name_words(&column_name).unwrap_or_default();
                let own_key = words
                    .strip_suffix(" id")
                    .is_some_and(|stem| class_label(stem) == class_label(&name));
                if unique && not_null && (words == "id" || own_key) {
                    table.primary_key.push(column_name.clone());
                }
                table.columns.push(column);
            }
            tables.push(table);
        }
    }
    // each semantic model's primary entity, so a foreign entity finds its table
    let mut keyed_by: HashMap<String, String> = HashMap::new();
    for doc in docs {
        for sm in list(doc, "semantic_models") {
            let Some(name) = text(sm, "name") else {
                continue;
            };
            for e in list(sm, "entities") {
                if matches!(
                    text(e, "type").as_deref(),
                    Some("primary" | "unique" | "natural")
                ) {
                    if let Some(entity) = text(e, "name") {
                        keyed_by
                            .entry(entity.to_lowercase())
                            .or_insert(name.clone());
                    }
                }
            }
        }
    }
    // the models as their properties describe them, before semantic models
    // take their place
    let model_tables = tables.clone();
    let mut replaced: HashMap<String, String> = HashMap::new();
    let mut carried: HashSet<(String, String)> = HashSet::new();
    let mut left_out = 0;
    for doc in docs {
        for sm in list(doc, "semantic_models") {
            let Some(name) = text(sm, "name") else {
                continue;
            };
            let base = text(sm, "model").map(|m| ref_name(&m));
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
            let model_description = base_table.and_then(|t| t.description.clone());
            let mut table = Table {
                name: name.clone(),
                description: text(sm, "description").or(model_description),
                ..Table::default()
            };
            // a column's own description, else the base model's
            let from_model = |expr: &Option<String>, name: &str| -> Option<Column> {
                expr.as_deref()
                    .and_then(|e| described.get(&e.to_lowercase()))
                    .or_else(|| described.get(&name.to_lowercase()))
                    .cloned()
            };
            for e in list(sm, "entities") {
                let Some(entity) = text(e, "name") else {
                    continue;
                };
                let expr = text(e, "expr");
                let base_column = from_model(&expr, &entity);
                let description = text(e, "description")
                    .or_else(|| base_column.as_ref().and_then(|c| c.description.clone()));
                match text(e, "type").as_deref() {
                    Some("foreign") => table.columns.push(Column {
                        name: entity.clone(),
                        description,
                        references: Some(
                            keyed_by
                                .get(&entity.to_lowercase())
                                .cloned()
                                .unwrap_or_else(|| entity.clone()),
                        ),
                        ..Column::default()
                    }),
                    _ => table.primary_key.push(entity.clone()),
                }
            }
            for d in list(sm, "dimensions") {
                let Some(dimension) = text(d, "name") else {
                    continue;
                };
                let expr = text(d, "expr");
                let base_column = from_model(&expr, &dimension);
                let datatype = match text(d, "type").as_deref() {
                    Some("time") => {
                        let grain = d
                            .get("type_params")
                            .and_then(|p| text(p, "time_granularity"))
                            .unwrap_or_else(|| "day".into());
                        Some(
                            if matches!(
                                grain.as_str(),
                                "hour" | "minute" | "second" | "millisecond"
                            ) {
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
                    description: text(d, "description")
                        .or_else(|| base_column.as_ref().and_then(|c| c.description.clone())),
                    values: base_column.map(|c| c.values).unwrap_or_default(),
                    ..Column::default()
                });
            }
            for m in list(sm, "measures") {
                let Some(measure) = text(m, "name") else {
                    continue;
                };
                let agg = text(m, "agg").unwrap_or_default();
                if !matches!(
                    agg.as_str(),
                    "sum" | "average" | "min" | "max" | "median" | "percentile"
                ) {
                    continue;
                }
                let expr = text(m, "expr");
                let base_column = from_model(&expr, &measure);
                table.columns.push(Column {
                    name: measure,
                    datatype: Some("decimal"),
                    description: text(m, "description")
                        .or_else(|| base_column.and_then(|c| c.description)),
                    ..Column::default()
                });
            }
            // the semantic model stands for its base model, which is not
            // repeated; the base model's relations and enumerations it didn't
            // use come along
            if let Some(bt) = base_table {
                let used = used_names(sm);
                for c in &bt.columns {
                    let n = c.name.to_lowercase();
                    if used.contains(&n)
                        || bt.primary_key.iter().any(|k| k.eq_ignore_ascii_case(&n))
                    {
                        continue;
                    }
                    if c.references.is_some() || !c.values.is_empty() {
                        carried.insert((name.to_lowercase(), n));
                        table.columns.push(c.clone());
                    } else {
                        left_out += 1;
                    }
                }
                replaced.insert(bt.name.to_lowercase(), name.clone());
                tables.retain(|t| !t.name.eq_ignore_ascii_case(&bt.name));
            }
            tables.push(table);
        }
    }
    // a reference to a model a semantic model replaced is a reference to the
    // semantic model
    for c in tables.iter_mut().flat_map(|t| t.columns.iter_mut()) {
        if let Some(to) = c
            .references
            .as_ref()
            .and_then(|r| replaced.get(&r.to_lowercase()))
        {
            c.references = Some(to.clone());
        }
    }
    // a carried key that a foreign entity already makes the relation is dropped
    for t in &mut tables {
        let table = t.name.to_lowercase();
        let entity_targets: HashSet<String> = t
            .columns
            .iter()
            .filter(|c| !carried.contains(&(table.clone(), c.name.to_lowercase())))
            .filter_map(|c| c.references.as_ref().map(|r| r.to_lowercase()))
            .collect();
        t.columns.retain(|c| {
            !carried.contains(&(table.clone(), c.name.to_lowercase()))
                || c.references
                    .as_ref()
                    .is_none_or(|r| !entity_targets.contains(&r.to_lowercase()))
        });
    }
    (Model { tables }, left_out)
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
        let doc: Value = serde_saphyr::from_str(PROJECT).unwrap();
        assert!(is_dbt(&doc));
        let m = model(&[doc]);
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
        let doc: Value = serde_saphyr::from_str(
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
        )
        .unwrap();
        let (m, left_out) = read(&[doc]);
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
        let doc: Value = serde_saphyr::from_str(
            "models:\n  - name: stores\n    columns:\n      - name: id\n        data_tests: [unique, not_null]\n      - name: city\n",
        )
        .unwrap();
        let m = model(&[doc]);
        assert_eq!(m.tables[0].primary_key, ["id"]);
        assert_eq!(ref_name("ref('shop', 'stores')"), "stores");
        assert_eq!(ref_name("ref(\"stores\")"), "stores");
    }
}
