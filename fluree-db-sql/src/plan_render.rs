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
    ArithOp, ColRef, Expr, KeySet, Literal, OrderKey, OutputCol, OutputExpr, Pred,
    PushdownCapabilities, RelNode, RelPlan, RelSource,
};
use fluree_db_tabular::{BatchSchema, FieldType};

use crate::dialect::{binary_string, cmp_sql, is_numeric, render_literal, sql_string, SqlDialect};
use crate::error::{Result, SqlError};

struct Renderer<'a> {
    dialect: SqlDialect,
    schemas: &'a HashMap<String, Arc<BatchSchema>>,
    keysets: HashMap<String, Vec<(String, Option<FieldType>)>>,
    /// Column types of every derived table, by alias.
    derived: HashMap<String, Vec<(String, FieldType)>>,
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
        derived: HashMap::new(),
        from: String::new(),
        where_preds: Vec::new(),
    };
    r.collect_keysets(&plan.root);
    r.infer_keyset_types(&plan.root)?;
    r.collect_derived(&plan.root)?;
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
    if let Some(h) = &plan.having {
        sql.push_str(" HAVING ");
        sql.push_str(&r.render_having(h, &plan.output)?);
    }
    if !plan.order_by.is_empty() {
        let keys: Vec<String> = plan
            .order_by
            .iter()
            .map(|(k, asc)| {
                let key = match k {
                    OrderKey::Col(c) => r.col(c),
                    OrderKey::Output(name) => dialect.quote_ident(name),
                    OrderKey::Expr(e) => r.render_expr(e)?,
                };
                Ok(format!("{key} {}", if *asc { "ASC" } else { "DESC" }))
            })
            .collect::<Result<_>>()?;
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
            // A derived table's key sets are its own statement's.
            RelNode::Access { .. } | RelNode::Derived { .. } | RelNode::UnionAll { .. } => {}
            RelNode::Filter { input, .. } => self.collect_keysets(input),
            RelNode::Join { left, right, .. } | RelNode::LeftJoin { left, right, .. } => {
                self.collect_keysets(left);
                self.collect_keysets(right);
            }
        }
    }

    /// Type every derived table's columns from its own plan, so an outer
    /// predicate or join over them renders like one over a table column.
    fn collect_derived(&mut self, node: &RelNode) -> Result<()> {
        match node {
            RelNode::Derived { alias, plan } => {
                let cols = self
                    .output_types(plan)?
                    .into_iter()
                    .filter_map(|(n, t)| t.map(|t| (n, t)))
                    .collect();
                self.derived.insert(alias.clone(), cols);
                Ok(())
            }
            // A union's column is typed by the branches projecting a value
            // there, which must agree, or the database would coerce (or
            // refuse) the union; a branch's NULL padding takes that type.
            RelNode::UnionAll { alias, branches } => {
                let Some(first) = branches.first() else {
                    return Err(SqlError::Unsupported("UNION ALL without branches".into()));
                };
                let mut cols = self.output_types(first)?;
                for b in &branches[1..] {
                    let other = self.output_types(b)?;
                    if other.len() != cols.len() {
                        return Err(SqlError::Unsupported(
                            "UNION ALL branches project different column counts".into(),
                        ));
                    }
                    for ((_, slot), (_, ty)) in cols.iter_mut().zip(other) {
                        match (&slot, ty) {
                            (_, None) => {}
                            (None, Some(t)) => *slot = Some(t),
                            (Some(s), Some(t)) if *s == t => {}
                            _ => {
                                return Err(SqlError::Unsupported(
                                    "UNION ALL branch column types differ".into(),
                                ));
                            }
                        }
                    }
                }
                let cols = cols
                    .into_iter()
                    .filter_map(|(n, t)| t.map(|t| (n, t)))
                    .collect();
                self.derived.insert(alias.clone(), cols);
                Ok(())
            }
            RelNode::Access { .. } | RelNode::KeySet(_) => Ok(()),
            RelNode::Filter { input, .. } => self.collect_derived(input),
            RelNode::Join { left, right, .. } | RelNode::LeftJoin { left, right, .. } => {
                self.collect_derived(left)?;
                self.collect_derived(right)
            }
        }
    }

    /// The output column types of a nested plan, from a renderer of its
    /// own; a `NULL` padding has none.
    fn output_types(&self, plan: &RelPlan) -> Result<Vec<(String, Option<FieldType>)>> {
        let mut inner = Renderer {
            dialect: self.dialect,
            schemas: self.schemas,
            keysets: HashMap::new(),
            derived: HashMap::new(),
            from: String::new(),
            where_preds: Vec::new(),
        };
        inner.collect_keysets(&plan.root);
        inner.infer_keyset_types(&plan.root)?;
        inner.collect_derived(&plan.root)?;
        let mut cols = Vec::with_capacity(plan.output.len());
        for o in &plan.output {
            let ty = match &o.expr {
                OutputExpr::Col(c) | OutputExpr::Min(c) | OutputExpr::Max(c) => {
                    Some(inner.col_type(c)?)
                }
                OutputExpr::Tag(_) | OutputExpr::CountRows | OutputExpr::Count { .. } => {
                    Some(FieldType::Int64)
                }
                OutputExpr::Sum { .. } => Some(FieldType::Decimal {
                    precision: 38,
                    scale: 6,
                }),
                OutputExpr::Null => None,
            };
            cols.push((o.name.clone(), ty));
        }
        Ok(cols)
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
        if let Some(cols) = self.derived.get(&c.alias) {
            return cols
                .iter()
                .find(|(n, _)| n == &c.column)
                .map(|(_, t)| *t)
                .ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "derived table '{}' has no column '{}'",
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

    /// A `HAVING` predicate: comparisons over outputs render the output's
    /// own expression, since Postgres and Trino do not resolve a select
    /// alias there.
    fn render_having(&self, pred: &Pred, outputs: &[OutputCol]) -> Result<String> {
        Ok(match pred {
            Pred::OutputCmp { output, op, value } => {
                let o = outputs.iter().find(|o| &o.name == output).ok_or_else(|| {
                    SqlError::Unsupported(format!("HAVING over unknown output '{output}'"))
                })?;
                format!(
                    "{} {} {}",
                    self.render_output_expr(o)?,
                    cmp_sql(*op),
                    self.expr_literal(value)?
                )
            }
            Pred::And(ps) | Pred::Or(ps) => {
                let sep = if matches!(pred, Pred::And(_)) {
                    " AND "
                } else {
                    " OR "
                };
                let parts: Vec<String> = ps
                    .iter()
                    .map(|p| Ok(format!("({})", self.render_having(p, outputs)?)))
                    .collect::<Result<_>>()?;
                parts.join(sep)
            }
            Pred::Not(p) => format!("NOT ({})", self.render_having(p, outputs)?),
            other => self.render_pred(other)?,
        })
    }

    fn render_output(&self, o: &OutputCol) -> Result<String> {
        let name = self.dialect.quote_ident(&o.name);
        Ok(format!("{} AS {name}", self.render_output_expr(o)?))
    }

    fn render_output_expr(&self, o: &OutputCol) -> Result<String> {
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
            OutputExpr::Tag(n) => n.to_string(),
            OutputExpr::Null => "NULL".to_string(),
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
        Ok(expr)
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
            RelNode::Derived { alias, plan } => Ok((
                format!(
                    "({}) AS {}",
                    render_plan(plan, self.schemas, self.dialect)?,
                    self.dialect.quote_ident(alias)
                ),
                Vec::new(),
            )),
            // Branches go bare: SQLite rejects a parenthesized compound
            // member, so a branch cannot carry its own ORDER BY or LIMIT.
            RelNode::UnionAll { alias, branches } => {
                if branches
                    .iter()
                    .any(|b| !b.order_by.is_empty() || b.limit.is_some())
                {
                    return Err(SqlError::Unsupported(
                        "UNION ALL branch with ORDER BY or LIMIT is not rendered".into(),
                    ));
                }
                let rendered: Vec<String> = branches
                    .iter()
                    .map(|b| render_plan(b, self.schemas, self.dialect))
                    .collect::<Result<_>>()?;
                Ok((
                    format!(
                        "({}) AS {}",
                        rendered.join(" UNION ALL "),
                        self.dialect.quote_ident(alias)
                    ),
                    Vec::new(),
                ))
            }
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
            RelNode::Access { .. }
            | RelNode::KeySet(_)
            | RelNode::Derived { .. }
            | RelNode::UnionAll { .. }
            | RelNode::Filter { .. } => {
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
                let lit = sql_string(pattern, self.dialect).ok_or_else(|| {
                    SqlError::Unsupported(format!("LIKE pattern {pattern:?} cannot be rendered"))
                })?;
                format!("{} LIKE {lit} ESCAPE '!'", self.col(col))
            }
            Pred::ExprCmp { expr, op, value } => {
                let rhs = match value {
                    // Compared, not computed with: bytes on MySQL too.
                    Literal::Str(s) => binary_string(s, self.dialect).ok_or_else(|| {
                        SqlError::Unsupported(format!("string {s:?} cannot be rendered"))
                    })?,
                    other => self.expr_literal(other)?,
                };
                format!("{} {} {rhs}", self.render_expr(expr)?, cmp_sql(*op))
            }
            Pred::OutputCmp { .. } => {
                return Err(SqlError::Unsupported(
                    "an output comparison belongs in HAVING".into(),
                ))
            }
            // Printable ASCII is the range every dialect's case mapping and
            // collation agree on; anything else goes back to the engine.
            Pred::NonAscii(col) => {
                let c = self.col(col);
                match self.dialect {
                    SqlDialect::Postgres => format!("{c} !~ '^[ -~]*$'"),
                    SqlDialect::Mysql => format!("{c} NOT REGEXP '^[ -~]*$'"),
                    SqlDialect::Sqlite => format!("{c} GLOB '*[^ -~]*'"),
                    SqlDialect::Trino => format!("NOT regexp_like({c}, '^[ -~]*$')"),
                }
            }
            Pred::And(ps) => self.render_junction(ps, " AND ")?,
            Pred::Or(ps) => self.render_junction(ps, " OR ")?,
            Pred::Not(p) => format!("NOT ({})", self.render_pred(p)?),
        })
    }

    /// An expression, every operation parenthesized so precedence is the
    /// plan's, not the dialect's.
    fn render_expr(&self, expr: &Expr) -> Result<String> {
        Ok(match expr {
            Expr::Col(c) => {
                let ty = self.col_type(c)?;
                if !is_numeric(ty) && ty != FieldType::String {
                    return Err(SqlError::Unsupported(format!(
                        "expression over {}.{}, neither numeric nor text",
                        c.alias, c.column
                    )));
                }
                self.col(c)
            }
            Expr::Lit(l) => self.expr_literal(l)?,
            Expr::Arith { op, left, right } => {
                for side in [left, right] {
                    self.operand_is(side, is_numeric, "numeric")?;
                }
                let op = match op {
                    ArithOp::Add => "+",
                    ArithOp::Sub => "-",
                    ArithOp::Mul => "*",
                };
                format!(
                    "({} {op} {})",
                    self.render_expr(left)?,
                    self.render_expr(right)?
                )
            }
            Expr::Concat(parts) => {
                for p in parts {
                    self.operand_is(p, |t| t == FieldType::String, "text")?;
                }
                let parts: Vec<String> = parts
                    .iter()
                    .map(|p| self.render_expr(p))
                    .collect::<Result<_>>()?;
                // `||` is OR on MySQL unless the session says otherwise.
                match self.dialect {
                    SqlDialect::Mysql => format!("CONCAT({})", parts.join(", ")),
                    _ => format!("({})", parts.join(" || ")),
                }
            }
            Expr::Strlen(e) => {
                self.operand_is(e, |t| t == FieldType::String, "text")?;
                let f = match self.dialect {
                    SqlDialect::Mysql => "CHAR_LENGTH",
                    _ => "LENGTH",
                };
                format!("{f}({})", self.render_expr(e)?)
            }
            Expr::Substr { expr, start, len } => {
                self.operand_is(expr, |t| t == FieldType::String, "text")?;
                match len {
                    Some(n) => format!("SUBSTR({}, {start}, {n})", self.render_expr(expr)?),
                    None => format!("SUBSTR({}, {start})", self.render_expr(expr)?),
                }
            }
            Expr::Lower(e) | Expr::Upper(e) => {
                self.operand_is(e, |t| t == FieldType::String, "text")?;
                let f = if matches!(expr, Expr::Lower(_)) {
                    "LOWER"
                } else {
                    "UPPER"
                };
                format!("{f}({})", self.render_expr(e)?)
            }
        })
    }

    /// A column operand of an operation must carry the operation's kind.
    fn operand_is(&self, e: &Expr, ok: impl Fn(FieldType) -> bool, kind: &str) -> Result<()> {
        if let Expr::Col(c) = e {
            let ty = self.col_type(c)?;
            if !ok(ty) {
                return Err(SqlError::Unsupported(format!(
                    "{kind} operation over {}.{} ({ty:?})",
                    c.alias, c.column
                )));
            }
        }
        Ok(())
    }

    /// A literal inside an expression, typed by its own kind.
    fn expr_literal(&self, lit: &Literal) -> Result<String> {
        if let Literal::Str(s) = lit {
            return sql_string(s, self.dialect)
                .ok_or_else(|| SqlError::Unsupported(format!("string {s:?} cannot be rendered")));
        }
        let ty = match lit {
            Literal::Int(_) => FieldType::Int64,
            Literal::Decimal { scale, .. } => FieldType::Decimal {
                precision: 38,
                scale: (*scale).max(0),
            },
            Literal::Double(_) => FieldType::Float64,
            other => {
                return Err(SqlError::Unsupported(format!(
                    "literal {other:?} in an expression"
                )))
            }
        };
        render_literal(lit, ty, self.dialect).ok_or_else(|| {
            SqlError::Unsupported(format!("literal {lit:?} cannot be rendered as {ty:?}"))
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

fn collect_col_eqs(node: &RelNode, out: &mut Vec<(ColRef, ColRef)>) {
    fn from_pred(p: &Pred, out: &mut Vec<(ColRef, ColRef)>) {
        match p {
            Pred::ColEq { left, right } => out.push((left.clone(), right.clone())),
            Pred::And(ps) | Pred::Or(ps) => ps.iter().for_each(|q| from_pred(q, out)),
            Pred::Not(q) => from_pred(q, out),
            _ => {}
        }
    }
    match node {
        RelNode::Access { .. }
        | RelNode::KeySet(_)
        | RelNode::Derived { .. }
        | RelNode::UnionAll { .. } => {}
        RelNode::Filter { input, pred } => {
            from_pred(pred, out);
            collect_col_eqs(input, out);
        }
        RelNode::Join { left, right, on } | RelNode::LeftJoin { left, right, on } => {
            from_pred(on, out);
            collect_col_eqs(left, out);
            collect_col_eqs(right, out);
        }
    }
}

fn same_class(a: FieldType, b: FieldType) -> bool {
    use FieldType as F;
    match (a, b) {
        _ if is_numeric(a) && is_numeric(b) => true,
        (F::String, F::String)
        | (F::Boolean, F::Boolean)
        | (F::Date, F::Date)
        | (F::Timestamp, F::Timestamp)
        | (F::TimestampTz, F::TimestampTz)
        | (F::Bytes, F::Bytes) => true,
        _ => false,
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
        // SQLite types a compound's column from the first branch's
        // expression (a NULL literal has none), so a padded slot would come
        // back as text.
        union_null_is_typed: !matches!(dialect, SqlDialect::Sqlite),
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
            having: None,
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
            having: None,
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
            having: None,
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
            having: None,
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
            having: None,
        };
        let sql = render_plan(&plan, &schemas(), SqlDialect::Mysql).unwrap();
        assert_eq!(
            sql,
            "SELECT `c`.`id` AS `c0` FROM `customers` AS `c` WHERE `c`.`name` = BINARY 'Bob'"
        );
    }

    /// A `LIKE` carries its own escape character, so a needle's wildcards
    /// survive on every dialect; MySQL's backslash-escaping literals decline
    /// as string comparisons do, and a non-string column is refused.
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
            having: None,
        };
        let pattern = format!("{}%", fluree_db_tabular::plan::like_escape("50%_!"));
        assert_eq!(
            render_plan(&like("name", &pattern), &schemas(), SqlDialect::Postgres).unwrap(),
            r#"SELECT "c"."id" AS "c0" FROM "customers" AS "c" WHERE "c"."name" LIKE '50!%!_!!%' ESCAPE '!'"#
        );
        assert_eq!(
            render_plan(&like("name", "O'B%"), &schemas(), SqlDialect::Mysql).unwrap(),
            "SELECT `c`.`id` AS `c0` FROM `customers` AS `c` WHERE `c`.`name` LIKE 'O''B%' ESCAPE '!'"
        );
        assert!(render_plan(&like("name", r"c:\%"), &schemas(), SqlDialect::Mysql).is_err());
        assert!(render_plan(&like("id", "1%"), &schemas(), SqlDialect::Postgres).is_err());
    }

    /// An expression is parenthesized at every operation, its literals
    /// typed by their own kind, and it orders a top-k; a string column in
    /// arithmetic is refused.
    #[test]
    fn expressions_render_parenthesized() {
        let times_two = |col: &str| Expr::Arith {
            op: ArithOp::Mul,
            left: Box::new(Expr::Col(ColRef::new("c", col))),
            right: Box::new(Expr::Lit(Literal::Int(2))),
        };
        let plan = |col: &str| RelPlan {
            root: RelNode::Filter {
                input: Box::new(access("c", "customers")),
                pred: Pred::ExprCmp {
                    expr: Expr::Arith {
                        op: ArithOp::Add,
                        left: Box::new(times_two(col)),
                        right: Box::new(Expr::Lit(Literal::Decimal {
                            unscaled: 15,
                            scale: 1,
                        })),
                    },
                    op: CmpOp::Gt,
                    value: Literal::Double(100.0),
                },
            },
            output: vec![out("c", "id", "c0")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![(OrderKey::Expr(times_two(col)), false)],
            limit: Some(2),
            having: None,
        };
        assert_eq!(
            render_plan(&plan("id"), &schemas(), SqlDialect::Postgres).unwrap(),
            r#"SELECT "c"."id" AS "c0" FROM "customers" AS "c" WHERE (("c"."id" * 2) + 1.5) > 1E2 ORDER BY ("c"."id" * 2) DESC LIMIT 2"#
        );
        assert!(render_plan(&plan("name"), &schemas(), SqlDialect::Postgres).is_err());
    }

    /// A derived table renders as its own statement in parentheses, its
    /// outputs typed from that statement so an outer join over them renders
    /// like one over a table column.
    #[test]
    fn derived_table_renders_nested() {
        let inner = RelPlan {
            root: access("o", "orders"),
            output: vec![
                out("o", "customer_id", "c0"),
                OutputCol {
                    expr: OutputExpr::CountRows,
                    name: "c1".into(),
                },
            ],
            group_by: vec![ColRef::new("o", "customer_id")],
            distinct: false,
            order_by: vec![],
            limit: None,
            having: None,
        };
        let plan = RelPlan {
            root: RelNode::Join {
                left: Box::new(access("c", "customers")),
                right: Box::new(RelNode::Derived {
                    alias: "d0".into(),
                    plan: Box::new(inner),
                }),
                on: Pred::ColEq {
                    left: ColRef::new("c", "id"),
                    right: ColRef::new("d0", "c0"),
                },
            },
            output: vec![out("c", "name", "c0"), out("d0", "c1", "c1")],
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
            having: None,
        };
        assert_eq!(
            render_plan(&plan, &schemas(), SqlDialect::Postgres).unwrap(),
            r#"SELECT "c"."name" AS "c0", "d0"."c1" AS "c1" FROM "customers" AS "c" JOIN (SELECT "o"."customer_id" AS "c0", COUNT(*) AS "c1" FROM "orders" AS "o" GROUP BY "o"."customer_id") AS "d0" ON "c"."id" = "d0"."c0""#
        );
    }

    fn plain(root: RelNode, output: Vec<OutputCol>) -> RelPlan {
        RelPlan {
            root,
            output,
            group_by: Vec::new(),
            distinct: false,
            order_by: vec![],
            limit: None,
            having: None,
        }
    }

    /// A union is a derived table of bare branches, each tagged; a filter
    /// over its columns types like one over the first branch's.
    #[test]
    fn union_all_renders_as_a_tagged_derived_table() {
        let branch = |alias: &str, tag: i64| {
            plain(
                RelNode::Filter {
                    input: Box::new(access(alias, "customers")),
                    pred: Pred::IsNotNull(ColRef::new(alias, "name")),
                },
                vec![
                    out(alias, "id", "c0"),
                    out(alias, "name", "c1"),
                    OutputCol {
                        expr: OutputExpr::Tag(tag),
                        name: "c2".into(),
                    },
                ],
            )
        };
        let mut schemas = schemas();
        schemas.insert("c2".to_string(), schemas["c"].clone());
        let plan = plain(
            RelNode::Filter {
                input: Box::new(RelNode::UnionAll {
                    alias: "u0".into(),
                    branches: vec![branch("c", 0), branch("c2", 1)],
                }),
                pred: Pred::Cmp {
                    col: ColRef::new("u0", "c1"),
                    op: CmpOp::Eq,
                    value: Literal::Str("Ada".into()),
                },
            },
            vec![
                out("u0", "c0", "c0"),
                out("u0", "c1", "c1"),
                out("u0", "c2", "c2"),
            ],
        );
        assert_eq!(
            render_plan(&plan, &schemas, SqlDialect::Mysql).unwrap(),
            r"SELECT `u0`.`c0` AS `c0`, `u0`.`c1` AS `c1`, `u0`.`c2` AS `c2` FROM (SELECT `c`.`id` AS `c0`, `c`.`name` AS `c1`, 0 AS `c2` FROM `customers` AS `c` WHERE `c`.`name` IS NOT NULL UNION ALL SELECT `c2`.`id` AS `c0`, `c2`.`name` AS `c1`, 1 AS `c2` FROM `customers` AS `c2` WHERE `c2`.`name` IS NOT NULL) AS `u0` WHERE `u0`.`c1` = BINARY 'Ada'"
        );
    }

    /// String expressions take each dialect's own spelling; a compared
    /// string is bytes on MySQL, a concatenated one is not.
    #[test]
    fn string_expressions_render_per_dialect() {
        let name = || Box::new(Expr::Col(ColRef::new("c", "name")));
        let plan = plain(
            RelNode::Filter {
                input: Box::new(access("c", "customers")),
                pred: Pred::And(vec![
                    Pred::ExprCmp {
                        expr: Expr::Concat(vec![*name(), Expr::Lit(Literal::Str("-".into()))]),
                        op: CmpOp::Eq,
                        value: Literal::Str("Ada-".into()),
                    },
                    Pred::ExprCmp {
                        expr: Expr::Strlen(name()),
                        op: CmpOp::Gt,
                        value: Literal::Int(2),
                    },
                    Pred::ExprCmp {
                        expr: Expr::Substr {
                            expr: name(),
                            start: 1,
                            len: Some(1),
                        },
                        op: CmpOp::Eq,
                        value: Literal::Str("A".into()),
                    },
                    Pred::Or(vec![
                        Pred::ExprCmp {
                            expr: Expr::Lower(name()),
                            op: CmpOp::Eq,
                            value: Literal::Str("ada".into()),
                        },
                        Pred::NonAscii(ColRef::new("c", "name")),
                    ]),
                ]),
            },
            vec![out("c", "id", "c0")],
        );
        let sql = |d| render_plan(&plan, &schemas(), d).unwrap();
        assert_eq!(
            sql(SqlDialect::Postgres),
            r#"SELECT "c"."id" AS "c0" FROM "customers" AS "c" WHERE ("c"."name" || '-') = 'Ada-' AND LENGTH("c"."name") > 2 AND SUBSTR("c"."name", 1, 1) = 'A' AND ((LOWER("c"."name") = 'ada') OR ("c"."name" !~ '^[ -~]*$'))"#
        );
        assert_eq!(
            sql(SqlDialect::Mysql),
            r"SELECT `c`.`id` AS `c0` FROM `customers` AS `c` WHERE CONCAT(`c`.`name`, '-') = BINARY 'Ada-' AND CHAR_LENGTH(`c`.`name`) > 2 AND SUBSTR(`c`.`name`, 1, 1) = BINARY 'A' AND ((LOWER(`c`.`name`) = BINARY 'ada') OR (`c`.`name` NOT REGEXP '^[ -~]*$'))"
        );
        assert_eq!(
            sql(SqlDialect::Sqlite),
            r#"SELECT "c"."id" AS "c0" FROM "customers" AS "c" WHERE ("c"."name" || '-') = 'Ada-' AND LENGTH("c"."name") > 2 AND SUBSTR("c"."name", 1, 1) = 'A' AND ((LOWER("c"."name") = 'ada') OR ("c"."name" GLOB '*[^ -~]*'))"#
        );
        assert!(
            sql(SqlDialect::Trino).ends_with(r#"OR (NOT regexp_like("c"."name", '^[ -~]*$')))"#)
        );
    }

    /// Branches whose columns differ in type are refused, not coerced.
    #[test]
    fn union_all_branches_must_align() {
        let plan = plain(
            RelNode::UnionAll {
                alias: "u0".into(),
                branches: vec![
                    plain(access("c", "customers"), vec![out("c", "name", "c0")]),
                    plain(access("o", "orders"), vec![out("o", "total", "c0")]),
                ],
            },
            vec![out("u0", "c0", "c0")],
        );
        let err = render_plan(&plan, &schemas(), SqlDialect::Postgres).unwrap_err();
        assert!(
            err.to_string()
                .contains("UNION ALL branch column types differ"),
            "{err}"
        );
    }

    /// HAVING repeats the output's expression rather than naming it.
    #[test]
    fn having_renders_the_aggregate_expression() {
        let mut plan = plain(
            access("o", "orders"),
            vec![
                out("o", "customer_id", "c0"),
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
            ],
        );
        plan.group_by = vec![ColRef::new("o", "customer_id")];
        plan.having = Some(Pred::And(vec![
            Pred::OutputCmp {
                output: "c1".into(),
                op: CmpOp::Gt,
                value: Literal::Int(1),
            },
            Pred::OutputCmp {
                output: "c2".into(),
                op: CmpOp::GtEq,
                value: Literal::Decimal {
                    unscaled: 1000,
                    scale: 1,
                },
            },
        ]));
        assert_eq!(
            render_plan(&plan, &schemas(), SqlDialect::Postgres).unwrap(),
            r#"SELECT "o"."customer_id" AS "c0", COUNT(*) AS "c1", SUM("o"."total") AS "c2" FROM "orders" AS "o" GROUP BY "o"."customer_id" HAVING (COUNT(*) > 1) AND (SUM("o"."total") >= 100.0)"#
        );
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
            having: None,
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
            having: None,
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
            having: None,
        };
        assert!(matches!(
            render_plan(&plan, &schemas(), SqlDialect::Trino),
            Err(SqlError::Unsupported(_))
        ));
    }
}
