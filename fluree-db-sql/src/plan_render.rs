//! Rendering a [`RelPlan`] as one SQL statement.
//!
//! The plan is a left-deep tree of table accesses, key sets, inner joins and
//! left joins under a projection. It renders flat — `FROM a JOIN b ON … LEFT
//! JOIN c ON …` — with a leaf's own filter going to `WHERE` for the driving
//! and inner-joined sides and into the `ON` clause for a left-joined side
//! (a `WHERE` on the nullable side would discard the unmatched rows the
//! left join exists to keep). A left-joined side deeper than one leaf is
//! refused; the lowering keeps optional blocks to one relation.
//!
//! Every literal is rendered against the column's probed type by the same
//! rules as a single-table scan, and a pairing the scan rules would decline is
//! an error here rather than a dropped predicate: the engine only puts a
//! predicate in a plan when it has proven the comparison exact.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_tabular::plan::{
    collect_col_eqs, same_class, ColRef, KeySet, Literal, OrderKey, OutputCol, OutputExpr, Pred,
    PushdownCapabilities, RelNode, RelPlan, RelSource,
};
use fluree_db_tabular::{BatchSchema, FieldType};

use crate::dialect::{binary_string, cmp_sql, render_literal, SqlDialect};
use crate::error::{Result, SqlError};

struct Renderer<'a> {
    dialect: SqlDialect,
    schemas: &'a HashMap<String, Arc<BatchSchema>>,
    keysets: HashMap<String, Vec<(String, Option<FieldType>)>>,
    from: String,
    where_preds: Vec<Pred>,
}

/// Render `plan` for `dialect`. `schemas` must hold a probed schema for every
/// table access alias in the plan.
pub fn render_plan(
    plan: &RelPlan,
    schemas: &HashMap<String, Arc<BatchSchema>>,
    dialect: SqlDialect,
) -> Result<String> {
    let mut r = Renderer {
        dialect,
        schemas,
        keysets: HashMap::new(),
        from: String::new(),
        where_preds: Vec::new(),
    };
    r.collect_keysets(&plan.root);
    r.infer_keyset_types(&plan.root)?;
    r.render_from(&plan.root)?;

    let mut sql = String::from("SELECT ");
    if plan.distinct {
        sql.push_str("DISTINCT ");
    }
    if plan.output.is_empty() {
        return Err(SqlError::Unsupported("plan projects no columns".into()));
    }
    let cols: Vec<String> = plan
        .output
        .iter()
        .map(|o| r.render_output(o))
        .collect::<Result<_>>()?;
    sql.push_str(&cols.join(", "));
    sql.push_str(" FROM ");
    sql.push_str(&r.from);
    if !r.where_preds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&r.render_conjunction(&r.where_preds)?);
    }
    if !plan.group_by.is_empty() {
        let keys: Vec<String> = plan.group_by.iter().map(|c| r.col(c)).collect();
        sql.push_str(" GROUP BY ");
        sql.push_str(&keys.join(", "));
    }
    if !plan.order_by.is_empty() {
        let keys: Vec<String> = plan
            .order_by
            .iter()
            .map(|(k, asc)| {
                let key = match k {
                    OrderKey::Col(c) => r.col(c),
                    OrderKey::Output(name) => dialect.quote_ident(name),
                };
                format!("{key} {}", if *asc { "ASC" } else { "DESC" })
            })
            .collect();
        sql.push_str(" ORDER BY ");
        sql.push_str(&keys.join(", "));
    }
    if let Some(n) = plan.limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

impl Renderer<'_> {
    fn collect_keysets(&mut self, node: &RelNode) {
        match node {
            RelNode::KeySet(k) => {
                self.keysets.insert(k.alias.clone(), k.columns.clone());
            }
            RelNode::Access { .. } => {}
            RelNode::Filter { input, .. } => self.collect_keysets(input),
            RelNode::Join { left, right, .. } | RelNode::LeftJoin { left, right, .. } => {
                self.collect_keysets(left);
                self.collect_keysets(right);
            }
        }
    }

    /// Give every untyped key-set column the type of the table column it is
    /// equated with, so its literals render like a filter on that column.
    fn infer_keyset_types(&mut self, node: &RelNode) -> Result<()> {
        let mut eqs: Vec<(ColRef, ColRef)> = Vec::new();
        collect_col_eqs(node, &mut eqs);
        for (a, b) in eqs {
            for (key, other) in [(&a, &b), (&b, &a)] {
                if self.keysets.contains_key(&key.alias) && !self.keysets.contains_key(&other.alias)
                {
                    let ty = self.col_type(other)?;
                    if let Some(cols) = self.keysets.get_mut(&key.alias) {
                        if let Some(slot) = cols.iter_mut().find(|(n, _)| n == &key.column) {
                            slot.1.get_or_insert(ty);
                        }
                    }
                }
            }
        }
        for (alias, cols) in &self.keysets {
            if let Some((n, _)) = cols.iter().find(|(_, t)| t.is_none()) {
                return Err(SqlError::Unsupported(format!(
                    "key set '{alias}' column '{n}' has no type and no equated column"
                )));
            }
        }
        Ok(())
    }

    fn col(&self, c: &ColRef) -> String {
        format!(
            "{}.{}",
            self.dialect.quote_ident(&c.alias),
            self.dialect.quote_ident(&c.column)
        )
    }

    fn col_type(&self, c: &ColRef) -> Result<FieldType> {
        if let Some(cols) = self.keysets.get(&c.alias) {
            return cols
                .iter()
                .find(|(n, _)| n == &c.column)
                .and_then(|(_, t)| *t)
                .ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "key set '{}' has no column '{}'",
                        c.alias, c.column
                    ))
                });
        }
        let schema = self.schemas.get(&c.alias).ok_or_else(|| {
            SqlError::Unsupported(format!("no probed schema for relation '{}'", c.alias))
        })?;
        schema
            .field_by_name(&c.column)
            .map(|f| f.field_type)
            .ok_or_else(|| {
                SqlError::Config(format!(
                    "column '{}' does not exist in relation '{}'",
                    c.column, c.alias
                ))
            })
    }

    fn render_output(&self, o: &OutputCol) -> Result<String> {
        let name = self.dialect.quote_ident(&o.name);
        let zoned = |c: &ColRef, rendered: String| -> Result<String> {
            Ok(match (self.col_type(c)?, self.dialect) {
                (FieldType::TimestampTz, SqlDialect::Trino) => {
                    format!("{rendered} AT TIME ZONE 'UTC'")
                }
                _ => rendered,
            })
        };
        let distinct = |d: bool| if d { "DISTINCT " } else { "" };
        let expr = match &o.expr {
            OutputExpr::Col(c) => zoned(c, self.col(c))?,
            OutputExpr::CountRows => "COUNT(*)".to_string(),
            OutputExpr::Count { col, distinct: d } => {
                format!("COUNT({}{})", distinct(*d), self.col(col))
            }
            OutputExpr::Sum { col, distinct: d } => {
                format!("SUM({}{})", distinct(*d), self.col(col))
            }
            OutputExpr::Min(c) => zoned(c, format!("MIN({})", self.col(c)))?,
            OutputExpr::Max(c) => zoned(c, format!("MAX({})", self.col(c)))?,
        };
        Ok(format!("{expr} AS {name}"))
    }

    /// A flat `a AND b AND c`: nested conjunctions are spliced in, and only a
    /// disjunction needs its own parentheses.
    fn render_conjunction(&self, preds: &[Pred]) -> Result<String> {
        let mut flat: Vec<&Pred> = Vec::new();
        fn flatten<'p>(p: &'p Pred, out: &mut Vec<&'p Pred>) {
            match p {
                Pred::And(ps) => ps.iter().for_each(|q| flatten(q, out)),
                other => out.push(other),
            }
        }
        preds.iter().for_each(|p| flatten(p, &mut flat));
        let parts: Vec<String> = flat
            .iter()
            .map(|p| {
                let s = self.render_pred(p)?;
                Ok(if matches!(p, Pred::Or(_)) {
                    format!("({s})")
                } else {
                    s
                })
            })
            .collect::<Result<_>>()?;
        Ok(parts.join(" AND "))
    }

    /// A leaf (`Access`, `KeySet`, or a `Filter` directly over one) as a FROM
    /// item plus its own predicates.
    fn render_leaf(&self, node: &RelNode) -> Result<(String, Vec<Pred>)> {
        match node {
            RelNode::Access { alias, source } => {
                Ok((self.render_source(alias, source), Vec::new()))
            }
            RelNode::KeySet(k) => Ok((self.render_keyset(k)?, Vec::new())),
            RelNode::Filter { input, pred } => {
                let (item, mut preds) = self.render_leaf(input)?;
                preds.push(pred.clone());
                Ok((item, preds))
            }
            RelNode::Join { .. } | RelNode::LeftJoin { .. } => Err(SqlError::Unsupported(
                "a joined side deeper than one relation is not rendered".into(),
            )),
        }
    }

    fn render_from(&mut self, node: &RelNode) -> Result<()> {
        match node {
            RelNode::Filter { input, pred }
                if matches!(**input, RelNode::Join { .. } | RelNode::LeftJoin { .. }) =>
            {
                self.render_from(input)?;
                self.where_preds.push(pred.clone());
                Ok(())
            }
            RelNode::Access { .. } | RelNode::KeySet(_) | RelNode::Filter { .. } => {
                let (item, preds) = self.render_leaf(node)?;
                self.from = item;
                self.where_preds.extend(preds);
                Ok(())
            }
            RelNode::Join { left, right, on } => {
                self.render_from(left)?;
                let (item, preds) = self.render_leaf(right)?;
                let on = self.render_conjunction(std::slice::from_ref(on))?;
                self.from.push_str(&format!(" JOIN {item} ON {on}"));
                self.where_preds.extend(preds);
                Ok(())
            }
            RelNode::LeftJoin { left, right, on } => {
                self.render_from(left)?;
                let (item, mut preds) = self.render_leaf(right)?;
                preds.insert(0, on.clone());
                let on = self.render_conjunction(&preds)?;
                self.from.push_str(&format!(" LEFT JOIN {item} ON {on}"));
                Ok(())
            }
        }
    }

    fn render_source(&self, alias: &str, source: &RelSource) -> String {
        let a = self.dialect.quote_ident(alias);
        match source {
            RelSource::Table(t) => format!("{} AS {a}", self.dialect.quote_table(t)),
            RelSource::Query(q) => format!("({}) AS {a}", q.trim().trim_end_matches(';')),
        }
    }

    /// `VALUES` where the dialect names its columns; a `UNION ALL` of
    /// single-row selects elsewhere. Types come from the declared columns.
    fn render_keyset(&self, k: &KeySet) -> Result<String> {
        if k.rows.is_empty() || k.columns.is_empty() {
            return Err(SqlError::Unsupported("empty key set".into()));
        }
        let alias = self.dialect.quote_ident(&k.alias);
        let names: Vec<String> = k
            .columns
            .iter()
            .map(|(n, _)| self.dialect.quote_ident(n))
            .collect();
        let types: Vec<(String, FieldType)> = self
            .keysets
            .get(&k.alias)
            .into_iter()
            .flatten()
            .filter_map(|(n, t)| t.map(|t| (n.clone(), t)))
            .collect();
        let mut rows = Vec::with_capacity(k.rows.len());
        for row in &k.rows {
            if row.len() != types.len() {
                return Err(SqlError::Unsupported("ragged key set row".into()));
            }
            let vals: Vec<String> = row
                .iter()
                .zip(&types)
                .map(|(v, (n, ty))| {
                    render_literal(v, *ty, self.dialect).ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "key set column '{n}' ({ty:?}) cannot carry {v:?}"
                        ))
                    })
                })
                .collect::<Result<_>>()?;
            rows.push(vals);
        }
        Ok(match self.dialect {
            SqlDialect::Trino | SqlDialect::Postgres => {
                let tuples: Vec<String> =
                    rows.iter().map(|r| format!("({})", r.join(", "))).collect();
                format!(
                    "(VALUES {}) AS {alias} ({})",
                    tuples.join(", "),
                    names.join(", ")
                )
            }
            SqlDialect::Mysql | SqlDialect::Sqlite => {
                let selects: Vec<String> = rows
                    .iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let cols: Vec<String> = if i == 0 {
                            r.iter()
                                .zip(&names)
                                .map(|(v, n)| format!("{v} AS {n}"))
                                .collect()
                        } else {
                            r.clone()
                        };
                        format!("SELECT {}", cols.join(", "))
                    })
                    .collect();
                format!("({}) AS {alias}", selects.join(" UNION ALL "))
            }
        })
    }

    fn render_pred(&self, pred: &Pred) -> Result<String> {
        Ok(match pred {
            Pred::Cmp { col, op, value } => {
                let ty = self.col_type(col)?;
                let c = self.col(col);
                match value {
                    Literal::Set(members) => {
                        let rendered: Vec<String> = members
                            .iter()
                            .map(|m| self.literal(m, ty, col))
                            .collect::<Result<_>>()?;
                        if rendered.is_empty() {
                            return Err(SqlError::Unsupported("empty IN list".into()));
                        }
                        format!("{c} IN ({})", rendered.join(", "))
                    }
                    lit => format!("{c} {} {}", cmp_sql(*op), self.literal(lit, ty, col)?),
                }
            }
            Pred::ColEq { left, right } => {
                let lt = self.col_type(left)?;
                let rt = self.col_type(right)?;
                if !same_class(lt, rt) {
                    return Err(SqlError::Unsupported(format!(
                        "cannot join {}.{} ({lt:?}) with {}.{} ({rt:?})",
                        left.alias, left.column, right.alias, right.column
                    )));
                }
                // MySQL joins strings under the column collation; `BINARY`
                // on one side makes the comparison byte-wise, which is what
                // the engine's IRI/literal join does.
                let binary = if lt == FieldType::String && self.dialect == SqlDialect::Mysql {
                    "BINARY "
                } else {
                    ""
                };
                format!("{} = {binary}{}", self.col(left), self.col(right))
            }
            Pred::IsNull(c) => format!("{} IS NULL", self.col(c)),
            Pred::IsNotNull(c) => format!("{} IS NOT NULL", self.col(c)),
            Pred::Like { col, pattern } => {
                let ty = self.col_type(col)?;
                if ty != FieldType::String {
                    return Err(SqlError::Unsupported(format!(
                        "LIKE on {}.{} ({ty:?})",
                        col.alias, col.column
                    )));
                }
                // `BINARY` on MySQL: a widened LIKE is a superset only if the
                // pattern matches at least the byte prefix, and a contraction
                // collation (`ch` as one element) can match fewer.
                let lit = binary_string(pattern, self.dialect).ok_or_else(|| {
                    SqlError::Unsupported(format!("LIKE pattern {pattern:?} cannot be rendered"))
                })?;
                format!("{} LIKE {lit} ESCAPE '!'", self.col(col))
            }
            Pred::And(ps) => self.render_junction(ps, " AND ")?,
            Pred::Or(ps) => self.render_junction(ps, " OR ")?,
            Pred::Not(p) => format!("NOT ({})", self.render_pred(p)?),
        })
    }

    fn render_junction(&self, ps: &[Pred], sep: &str) -> Result<String> {
        if ps.is_empty() {
            return Err(SqlError::Unsupported("empty junction".into()));
        }
        let parts: Vec<String> = ps
            .iter()
            .map(|p| self.render_pred(p).map(|s| format!("({s})")))
            .collect::<Result<_>>()?;
        Ok(parts.join(sep))
    }

    fn literal(&self, lit: &Literal, ty: FieldType, col: &ColRef) -> Result<String> {
        render_literal(lit, ty, self.dialect).ok_or_else(|| {
            SqlError::Unsupported(format!(
                "literal {lit:?} cannot be compared with {}.{} ({ty:?})",
                col.alias, col.column
            ))
        })
    }
}

/// What this dialect can execute for a pushed-down plan.
pub fn capabilities(dialect: SqlDialect) -> PushdownCapabilities {
    // The bridge accepts bodies up to 2 MB; Trino itself has no low cap. One
    // budget keeps key-set chunking identical across backends.
    const STATEMENT_MAX_BYTES: usize = 1 << 20;
    PushdownCapabilities {
        left_join: true,
        keyset_max_rows: 2000,
        statement_max_bytes: STATEMENT_MAX_BYTES,
        // Byte equality: Trino compares code points; a deterministic Postgres
        // collation equates only identical strings; SQLite's default is BINARY;
        // MySQL's default collation case-folds, which the renderer undoes by
        // marking every string literal and column-to-column comparison
        // `BINARY` (see `render_literal` and `render_pred`).
        string_eq_is_binary: true,
        // `GROUP BY` / `DISTINCT` cannot be forced binary on MySQL: with
        // `ONLY_FULL_GROUP_BY` (the default) a projected column must appear in
        // the `GROUP BY` as itself, so grouping folds case-variants together.
        string_distinct_is_binary: !matches!(dialect, SqlDialect::Mysql),
        string_order_is_codepoint: matches!(dialect, SqlDialect::Trino | SqlDialect::Sqlite),
        timestamp_is_text: matches!(dialect, SqlDialect::Sqlite),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_tabular::plan::CmpOp;
    use fluree_db_tabular::FieldInfo;

    fn schema(cols: &[(&str, FieldType)]) -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(
            cols.iter()
                .enumerate()
                .map(|(i, (n, t))| FieldInfo {
                    name: n.to_string(),
                    field_type: *t,
                    nullable: true,
                    field_id: i as i32 + 1,
                })
                .collect(),
        ))
    }

    fn schemas() -> HashMap<String, Arc<BatchSchema>> {
        let mut m = HashMap::new();
        m.insert(
            "o".to_string(),
            schema(&[
                ("id", FieldType::Int64),
                ("customer_id", FieldType::Int64),
                (
                    "total",
                    FieldType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                ),
                ("placed", FieldType::TimestampTz),
            ]),
        );
        m.insert(
            "c".to_string(),
            schema(&[("id", FieldType::Int64), ("name", FieldType::String)]),
        );
        m
    }

    fn access(alias: &str, table: &str) -> RelNode {
        RelNode::Access {
            alias: alias.into(),
            source: RelSource::Table(table.into()),
        }
    }

    fn out(alias: &str, col: &str, name: &str) -> OutputCol {
        OutputCol::column(ColRef::new(alias, col), name)
    }

    #[test]
    fn join_with_filters_and_limit() {
        let plan = RelPlan {
            root: RelNode::Join {
                left: Box::new(RelNode::Filter {
                    input: Box::new(access("o", "sales.orders")),
                    pred: Pred::And(vec![
                        Pred::IsNotNull(ColRef::new("o", "id")),
                        Pred::Cmp {
                            col: ColRef::new("o", "total"),
                            op: CmpOp::Gt,
                            value: Literal::Int(100),
                        },
                    ]),
                }),
                right: Box::new(access("c", "sales.customers")),
                on: Pred::ColEq {
                    left: ColRef::new("o", "customer_id"),
                    right: ColRef::new("c", "id"),
                },
            },
            output: vec![
                out("o", "id", "c0"),
                out("c", "name", "c1"),
                out("o", "placed", "c2"),
            ],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![(OrderKey::Col(ColRef::new("o", "total")), false)],
            limit: Some(10),
        };
        let sql = render_plan(&plan, &schemas(), SqlDialect::Trino).unwrap();
        assert_eq!(
            sql,
            r#"SELECT "o"."id" AS "c0", "c"."name" AS "c1", "o"."placed" AT TIME ZONE 'UTC' AS "c2" FROM "sales"."orders" AS "o" JOIN "sales"."customers" AS "c" ON "o"."customer_id" = "c"."id" WHERE "o"."id" IS NOT NULL AND "o"."total" > 100 ORDER BY "o"."total" DESC LIMIT 10"#
        );
    }

    #[test]
    fn grouped_plan_renders_aggregates_and_orders_by_output() {
        let plan = RelPlan {
            root: RelNode::Join {
                left: Box::new(access("o", "sales.orders")),
                right: Box::new(access("c", "sales.customers")),
                on: Pred::ColEq {
                    left: ColRef::new("o", "customer_id"),
                    right: ColRef::new("c", "id"),
                },
            },
            output: vec![
                out("c", "id", "c0"),
                OutputCol {
                    expr: OutputExpr::CountRows,
                    name: "c1".into(),
                },
                OutputCol {
                    expr: OutputExpr::Sum {
                        col: ColRef::new("o", "total"),
                        distinct: false,
                    },
                    name: "c2".into(),
                },
                OutputCol {
                    expr: OutputExpr::Count {
                        col: ColRef::new("o", "total"),
                        distinct: true,
                    },
                    name: "c3".into(),
                },
                OutputCol {
                    expr: OutputExpr::Max(ColRef::new("o", "placed")),
                    name: "c4".into(),
                },
            ],
            group_by: vec![ColRef::new("c", "id")],
            distinct: false,
            order_by: vec![(OrderKey::Output("c1".into()), false)],
            limit: Some(5),
        };
        let sql = render_plan(&plan, &schemas(), SqlDialect::Trino).unwrap();
        assert_eq!(
            sql,
            r#"SELECT "c"."id" AS "c0", COUNT(*) AS "c1", SUM("o"."total") AS "c2", COUNT(DISTINCT "o"."total") AS "c3", MAX("o"."placed") AT TIME ZONE 'UTC' AS "c4" FROM "sales"."orders" AS "o" JOIN "sales"."customers" AS "c" ON "o"."customer_id" = "c"."id" GROUP BY "c"."id" ORDER BY "c1" DESC LIMIT 5"#
        );
    }

    #[test]
    fn left_join_puts_leaf_filter_in_on_clause() {
        let plan = RelPlan {
            root: RelNode::LeftJoin {
                left: Box::new(access("o", "sales.orders")),
                right: Box::new(RelNode::Filter {
                    input: Box::new(access("c", "sales.customers")),
                    pred: Pred::IsNotNull(ColRef::new("c", "name")),
                }),
                on: Pred::ColEq {
                    left: ColRef::new("o", "customer_id"),
                    right: ColRef::new("c", "id"),
                },
            },
            output: vec![out("o", "id", "c0"), out("c", "name", "c1")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
        };
        let sql = render_plan(&plan, &schemas(), SqlDialect::Postgres).unwrap();
        assert_eq!(
            sql,
            r#"SELECT "o"."id" AS "c0", "c"."name" AS "c1" FROM "sales"."orders" AS "o" LEFT JOIN "sales"."customers" AS "c" ON "o"."customer_id" = "c"."id" AND "c"."name" IS NOT NULL"#
        );
    }

    fn keyset_plan() -> RelPlan {
        RelPlan {
            root: RelNode::Join {
                left: Box::new(RelNode::KeySet(KeySet {
                    alias: "k".into(),
                    columns: vec![("id".into(), None)],
                    rows: vec![vec![Literal::Int(1)], vec![Literal::Int(2)]],
                })),
                right: Box::new(access("c", "sales.customers")),
                on: Pred::ColEq {
                    left: ColRef::new("k", "id"),
                    right: ColRef::new("c", "id"),
                },
            },
            output: vec![out("c", "id", "c0"), out("c", "name", "c1")],
            group_by: Vec::new(),
            distinct: true,
            order_by: vec![],
            limit: None,
        }
    }

    #[test]
    fn keyset_values_form() {
        let sql = render_plan(&keyset_plan(), &schemas(), SqlDialect::Trino).unwrap();
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "c"."id" AS "c0", "c"."name" AS "c1" FROM (VALUES (1), (2)) AS "k" ("id") JOIN "sales"."customers" AS "c" ON "k"."id" = "c"."id""#
        );
    }

    #[test]
    fn keyset_union_form() {
        let sql = render_plan(&keyset_plan(), &schemas(), SqlDialect::Sqlite).unwrap();
        assert_eq!(
            sql,
            r#"SELECT DISTINCT "c"."id" AS "c0", "c"."name" AS "c1" FROM (SELECT 1 AS "id" UNION ALL SELECT 2) AS "k" JOIN "sales"."customers" AS "c" ON "k"."id" = "c"."id""#
        );
    }

    #[test]
    fn mysql_string_equality_is_binary() {
        let plan = RelPlan {
            root: RelNode::Filter {
                input: Box::new(access("c", "customers")),
                pred: Pred::Cmp {
                    col: ColRef::new("c", "name"),
                    op: CmpOp::Eq,
                    value: Literal::Str("Bob".into()),
                },
            },
            output: vec![out("c", "id", "c0")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
        };
        let sql = render_plan(&plan, &schemas(), SqlDialect::Mysql).unwrap();
        assert_eq!(
            sql,
            "SELECT `c`.`id` AS `c0` FROM `customers` AS `c` WHERE `c`.`name` = BINARY 'Bob'"
        );
    }

    /// A `LIKE` carries its own escape character, so a needle's wildcards
    /// survive on every dialect; MySQL marks the pattern `BINARY` so a
    /// contraction collation cannot match fewer strings than the byte
    /// prefix, its backslash-escaping literals decline as string comparisons
    /// do, and a non-string column is refused.
    #[test]
    fn like_renders_with_an_escape_clause() {
        let like = |col: &str, pattern: &str| RelPlan {
            root: RelNode::Filter {
                input: Box::new(access("c", "customers")),
                pred: Pred::Like {
                    col: ColRef::new("c", col),
                    pattern: pattern.to_string(),
                },
            },
            output: vec![out("c", "id", "c0")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
        };
        let pattern = format!("{}%", fluree_db_tabular::plan::like_escape("50%_!"));
        assert_eq!(
            render_plan(&like("name", &pattern), &schemas(), SqlDialect::Postgres).unwrap(),
            r#"SELECT "c"."id" AS "c0" FROM "customers" AS "c" WHERE "c"."name" LIKE '50!%!_!!%' ESCAPE '!'"#
        );
        assert_eq!(
            render_plan(&like("name", "O'B%"), &schemas(), SqlDialect::Mysql).unwrap(),
            "SELECT `c`.`id` AS `c0` FROM `customers` AS `c` WHERE `c`.`name` LIKE BINARY 'O''B%' ESCAPE '!'"
        );
        assert!(render_plan(&like("name", r"c:\%"), &schemas(), SqlDialect::Mysql).is_err());
        assert!(render_plan(&like("id", "1%"), &schemas(), SqlDialect::Postgres).is_err());
    }

    /// A string join on MySQL compares bytes too, and an integer join is
    /// left alone.
    #[test]
    fn mysql_string_join_is_binary() {
        let mut schemas = schemas();
        schemas.insert(
            "t".to_string(),
            schema(&[("tag", FieldType::String), ("owner", FieldType::Int64)]),
        );
        let plan = RelPlan {
            root: RelNode::Join {
                left: Box::new(access("t", "tags")),
                right: Box::new(access("c", "customers")),
                on: Pred::And(vec![
                    Pred::ColEq {
                        left: ColRef::new("t", "tag"),
                        right: ColRef::new("c", "name"),
                    },
                    Pred::ColEq {
                        left: ColRef::new("t", "owner"),
                        right: ColRef::new("c", "id"),
                    },
                ]),
            },
            output: vec![out("c", "id", "c0")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
        };
        let sql = render_plan(&plan, &schemas, SqlDialect::Mysql).unwrap();
        assert_eq!(
            sql,
            "SELECT `c`.`id` AS `c0` FROM `tags` AS `t` JOIN `customers` AS `c` ON `t`.`tag` = BINARY `c`.`name` AND `t`.`owner` = `c`.`id`"
        );
    }

    #[test]
    fn mismatched_join_types_are_an_error() {
        let plan = RelPlan {
            root: RelNode::Join {
                left: Box::new(access("o", "sales.orders")),
                right: Box::new(access("c", "sales.customers")),
                on: Pred::ColEq {
                    left: ColRef::new("o", "id"),
                    right: ColRef::new("c", "name"),
                },
            },
            output: vec![out("o", "id", "c0")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
        };
        assert!(matches!(
            render_plan(&plan, &schemas(), SqlDialect::Trino),
            Err(SqlError::Unsupported(_))
        ));
    }

    #[test]
    fn inexact_literal_is_an_error_not_a_drop() {
        let plan = RelPlan {
            root: RelNode::Filter {
                input: Box::new(access("c", "sales.customers")),
                pred: Pred::Cmp {
                    col: ColRef::new("c", "id"),
                    op: CmpOp::Eq,
                    value: Literal::Str("1".into()),
                },
            },
            output: vec![out("c", "id", "c0")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
        };
        assert!(matches!(
            render_plan(&plan, &schemas(), SqlDialect::Trino),
            Err(SqlError::Unsupported(_))
        ));
    }
}
