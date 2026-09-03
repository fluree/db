//! A fake Trino-protocol endpoint that *executes* the statements the engine
//! sends against in-memory tables, so end-to-end tests over a SQL graph
//! source assert real row semantics and the exact SQL at once.
//!
//! Supported: `SELECT 1`; `SELECT * … LIMIT 0` probes; `SELECT COUNT(*) …`;
//! a `FROM` table (or an `rr:sqlQuery` derived table mapped to a fixture
//! table) followed by any number of `JOIN` / `LEFT JOIN` items — tables or
//! `VALUES` key sets — with `ON` predicates; `WHERE` conjunctions of
//! `IS [NOT] NULL`, comparisons, `IN`, `NOT (…)` and `OR`; `GROUP BY` with
//! `HAVING COUNT(*) > n` (the uniqueness probe) or with `COUNT`, `SUM`,
//! `MIN`, `MAX` select items (`DISTINCT` inside them too); `SELECT DISTINCT`;
//! `ORDER BY` on columns and output names; `LIMIT`. Anything else answers
//! with a Trino error naming the statement, so a test fails loudly with the
//! SQL it sent.

#![allow(dead_code)]

use serde_json::{json, Value};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<Value>>,
}

impl Table {
    pub fn new(name: &str, columns: &[(&str, &str)], rows: Vec<Vec<Value>>) -> Self {
        Self {
            name: name.to_string(),
            columns: columns
                .iter()
                .map(|(n, t)| (n.to_string(), t.to_string()))
                .collect(),
            rows,
        }
    }
}

#[derive(Clone, Default)]
pub struct FakeSql {
    tables: Vec<Table>,
    /// `rr:sqlQuery` text → the fixture table it stands for.
    queries: Vec<(String, String)>,
}

/// One relation in a statement: its alias and its rows.
struct Rel {
    alias: String,
    columns: Vec<(String, String)>,
    rows: Vec<Vec<Value>>,
}

/// A row of the join so far: one optional row per relation (None = a left
/// join that found no match).
type Tuple = Vec<Option<Vec<Value>>>;

/// Output columns `(name, trino type)` and rows.
type ResultSet = (Vec<(String, String)>, Vec<Vec<Value>>);

impl FakeSql {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn table(mut self, t: Table) -> Self {
        self.tables.push(t);
        self
    }

    /// Let a derived table `(sql) AS …` read the fixture table `name`.
    pub fn query(mut self, sql: &str, name: &str) -> Self {
        self.queries
            .push((sql.trim().to_string(), name.to_string()));
        self
    }

    pub async fn mount(self) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/v1/statement"))
            .respond_with(self)
            .mount(&server)
            .await;
        server
    }

    fn table_for_source(&self, src: &str) -> Option<&Table> {
        let src = src.trim();
        if let Some(inner) = src.strip_prefix('(') {
            let end = inner.rfind(')')?;
            let q = inner[..end].trim();
            let name = self.queries.iter().find(|(s, _)| s == q).map(|(_, n)| n)?;
            return self.tables.iter().find(|t| &t.name == name);
        }
        let name = src.replace('"', "");
        self.tables.iter().find(|t| t.name == name)
    }

    /// Parse one FROM/JOIN item: a table (with optional alias) or a VALUES
    /// key set. Returns the relation and the unparsed tail.
    fn parse_item<'s>(&self, s: &'s str, default_alias: &str) -> Result<(Rel, &'s str), String> {
        let s = s.trim_start();
        if let Some(r) = s.strip_prefix("(VALUES ") {
            let end = matching_paren(s).ok_or("unbalanced VALUES")?;
            let values_text = &s[8..end];
            let after = s[end + 1..].trim_start();
            let after = after.strip_prefix("AS ").ok_or("VALUES without alias")?;
            let sp = after.find(' ').ok_or("VALUES alias")?;
            let alias = after[..sp].replace('"', "");
            let after = after[sp..].trim_start();
            let cend = matching_paren(after).ok_or("VALUES columns")?;
            let columns: Vec<(String, String)> = after[1..cend]
                .split(',')
                .map(|c| (c.trim().replace('"', ""), "varchar".to_string()))
                .collect();
            let mut rows = Vec::new();
            for tuple in split_top(values_text, ", ") {
                let inner = strip_parens(tuple);
                rows.push(
                    split_top(inner, ", ")
                        .into_iter()
                        .map(parse_literal)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            let _ = r;
            return Ok((
                Rel {
                    alias,
                    columns,
                    rows,
                },
                after[cend + 1..].trim_start(),
            ));
        }
        // A nested statement the lane built, unless it is a registered
        // `rr:sqlQuery` (resolved as a table below).
        if s.starts_with("(SELECT ")
            && matching_paren(s).is_some_and(|end| self.table_for_source(&s[..=end]).is_none())
        {
            let end = matching_paren(s).ok_or("unbalanced derived table")?;
            let (columns, rows) = self.eval(&s[1..end])?;
            let after = s[end + 1..].trim_start();
            let after = after
                .strip_prefix("AS ")
                .ok_or("derived table without alias")?;
            let sp = after.find(' ').unwrap_or(after.len());
            let alias = after[..sp].replace('"', "");
            return Ok((
                Rel {
                    alias,
                    columns,
                    rows,
                },
                after[sp..].trim_start(),
            ));
        }
        let (item, rest) = if s.starts_with('(') {
            let end = matching_paren(s).ok_or("unbalanced derived table")?;
            (&s[..=end], s[end + 1..].trim_start())
        } else {
            let end = s.find(' ').unwrap_or(s.len());
            (&s[..end], s[end..].trim_start())
        };
        let table = self
            .table_for_source(item)
            .ok_or_else(|| format!("unknown table {item}"))?;
        let mut alias = default_alias.to_string();
        let mut rest = rest;
        if let Some(r) = rest.strip_prefix("AS ") {
            let end = r.find(' ').unwrap_or(r.len());
            alias = r[..end].replace('"', "");
            rest = r[end..].trim_start();
        }
        Ok((
            Rel {
                alias,
                columns: table.columns.clone(),
                rows: table.rows.clone(),
            },
            rest,
        ))
    }

    fn eval(&self, sql: &str) -> Result<ResultSet, String> {
        let sql = sql.trim().trim_end_matches(';');
        if sql == "SELECT 1" {
            return Ok((
                vec![("_col0".into(), "integer".into())],
                vec![vec![json!(1)]],
            ));
        }
        let (select_list, rest) =
            split_once_top(sql, " FROM ").ok_or_else(|| format!("no FROM in: {sql}"))?;
        let select_list = select_list
            .strip_prefix("SELECT ")
            .ok_or_else(|| format!("not a SELECT: {sql}"))?;
        let (select_distinct, select_list) = match select_list.strip_prefix("DISTINCT ") {
            Some(rest) => (true, rest),
            None => (false, select_list),
        };

        let (first, mut rest) = self.parse_item(rest, "")?;
        let mut rels: Vec<Rel> = vec![first];
        let mut tuples: Vec<Tuple> = rels[0].rows.iter().map(|r| vec![Some(r.clone())]).collect();

        // JOIN items, in order.
        loop {
            let (left, r) = if let Some(r) = rest.strip_prefix("LEFT JOIN ") {
                (true, r)
            } else if let Some(r) = rest.strip_prefix("JOIN ") {
                (false, r)
            } else {
                break;
            };
            let (rel, r) = self.parse_item(r, "")?;
            let r = r.strip_prefix("ON ").ok_or("JOIN without ON")?;
            let (on_text, tail) = cut_clause(r);
            rels.push(rel);
            let idx = rels.len() - 1;
            let on = parse_pred(on_text)?;
            let resolver = Resolver { rels: &rels };
            let mut next: Vec<Tuple> = Vec::new();
            for t in &tuples {
                let mut matched = false;
                for row in &rels[idx].rows {
                    let mut cand = t.clone();
                    cand.push(Some(row.clone()));
                    if on.eval(&cand, &resolver) {
                        matched = true;
                        next.push(cand);
                    }
                }
                if left && !matched {
                    let mut cand = t.clone();
                    cand.push(None);
                    next.push(cand);
                }
            }
            tuples = next;
            rest = tail;
        }

        let mut where_text = None;
        let mut group_by: Option<Vec<Col>> = None;
        let mut having_min_count: Option<usize> = None;
        let mut order: Vec<(SortKey, bool)> = Vec::new();
        let mut limit = None;
        if let Some(r) = rest.strip_prefix("WHERE ") {
            let (w, tail) = cut_clause(r);
            where_text = Some(w);
            rest = tail;
        }
        if let Some(r) = rest.strip_prefix("GROUP BY ") {
            let (g, tail) = cut_clause(r);
            group_by = Some(split_top(g, ", ").into_iter().map(colref).collect());
            rest = tail;
        }
        if let Some(r) = rest.strip_prefix("HAVING COUNT(*) > ") {
            let (h, tail) = cut_clause(r);
            having_min_count = Some(h.trim().parse::<usize>().map_err(|e| e.to_string())? + 1);
            rest = tail;
        }
        if let Some(r) = rest.strip_prefix("ORDER BY ") {
            let (o, tail) = cut_clause(r);
            for key in split_top(o, ", ") {
                let (c, dir) = key.rsplit_once(' ').ok_or("ORDER BY without direction")?;
                let key = if c.trim().starts_with('(') {
                    SortKey::Expr(parse_expr(c)?)
                } else {
                    SortKey::Col(colref(c))
                };
                order.push((key, dir == "ASC"));
            }
            rest = tail;
        }
        if let Some(r) = rest.strip_prefix("LIMIT ") {
            limit = Some(r.trim().parse::<usize>().map_err(|e| e.to_string())?);
            rest = "";
        }
        if !rest.trim().is_empty() {
            return Err(format!("unparsed tail '{rest}' in: {sql}"));
        }

        let resolver = Resolver { rels: &rels };
        if let Some(w) = where_text {
            let pred = parse_pred(w)?;
            tuples.retain(|t| pred.eval(t, &resolver));
        }

        // Ordering on join columns alone happens before grouping/projection;
        // a key list naming an output happens after, over the projection.
        let mut order_output: Vec<(Col, bool)> = Vec::new();
        if !order.is_empty() {
            // An expression key always reads the tuple; a column key that
            // resolves does too.
            enum Key {
                Cell(usize, usize),
                Expr(Expr),
            }
            let mut keys: Vec<(Key, bool)> = Vec::new();
            let mut by_output = false;
            for (k, asc) in &order {
                match k {
                    SortKey::Expr(e) => keys.push((Key::Expr(e.clone()), *asc)),
                    SortKey::Col(c) => match resolver.resolve(c) {
                        Ok((ri, ci)) => keys.push((Key::Cell(ri, ci), *asc)),
                        Err(e) if c.0.is_some() => return Err(e),
                        Err(_) => by_output = true,
                    },
                }
            }
            if !by_output {
                let value = |t: &Tuple, k: &Key| match k {
                    Key::Cell(ri, ci) => cell(t, *ri, *ci),
                    Key::Expr(e) => e.eval(t, &resolver).map_or(Value::Null, |n| json!(n)),
                };
                tuples.sort_by(|a, b| {
                    keys.iter()
                        .map(|(k, asc)| {
                            let o = cmp_values(&value(a, k), &value(b, k));
                            if *asc {
                                o
                            } else {
                                o.reverse()
                            }
                        })
                        .find(|o| o.is_ne())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else {
                for (k, asc) in &order {
                    match k {
                        SortKey::Col(c) => order_output.push((c.clone(), *asc)),
                        SortKey::Expr(_) => {
                            return Err("ORDER BY mixes an expression with an output name".into())
                        }
                    }
                }
            }
        }

        // Select items.
        let items: Vec<SelectItem> = if select_list.trim() == "*" {
            rels[0]
                .columns
                .iter()
                .map(|(n, _)| SelectItem {
                    expr: SelectExpr::Col((None, n.clone())),
                    name: n.clone(),
                })
                .collect()
        } else if select_list.trim() == "1" {
            vec![SelectItem {
                expr: SelectExpr::One,
                name: "_col0".into(),
            }]
        } else {
            split_top(select_list, ", ")
                .into_iter()
                .map(parse_select_item)
                .collect::<Result<_, _>>()?
        };
        let grouped = group_by.is_some() || items.iter().any(|i| i.expr.is_aggregate());

        let mut out_cols: Vec<(String, String)> = Vec::new();
        let mut data: Vec<Vec<Value>> = Vec::new();
        if grouped {
            let key_idx: Vec<(usize, usize)> = group_by
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|c| resolver.resolve(c))
                .collect::<Result<_, _>>()?;
            let mut groups: Vec<(Vec<Value>, Vec<Tuple>)> = Vec::new();
            for t in &tuples {
                let key: Vec<Value> = key_idx.iter().map(|(ri, ci)| cell(t, *ri, *ci)).collect();
                match groups
                    .iter_mut()
                    .find(|(k, _)| k.iter().zip(&key).all(|(a, b)| values_eq(a, b)))
                {
                    Some((_, members)) => members.push(t.clone()),
                    None => groups.push((key, vec![t.clone()])),
                }
            }
            if key_idx.is_empty() && groups.is_empty() {
                // An implicit group over no rows still yields one row.
                groups.push((Vec::new(), Vec::new()));
            }
            let min = having_min_count.unwrap_or(0);
            for (_, members) in groups.into_iter().filter(|(_, m)| m.len() >= min) {
                let mut row = Vec::with_capacity(items.len());
                for item in &items {
                    let (ty, v) = item.expr.eval_group(&members, &resolver, &rels)?;
                    if data.is_empty() {
                        out_cols.push((item.name.clone(), ty));
                    }
                    row.push(v);
                }
                data.push(row);
            }
            if data.is_empty() {
                for item in &items {
                    out_cols.push((item.name.clone(), item.expr.type_hint(&resolver, &rels)));
                }
            }
        } else {
            for item in &items {
                out_cols.push((item.name.clone(), item.expr.type_hint(&resolver, &rels)));
            }
            for t in &tuples {
                let row = items
                    .iter()
                    .map(|item| item.expr.eval_row(t, &resolver))
                    .collect::<Result<Vec<_>, _>>()?;
                data.push(row);
            }
        }
        if select_distinct {
            let mut seen: Vec<Vec<Value>> = Vec::new();
            data.retain(|r| {
                if seen
                    .iter()
                    .any(|s| s.iter().zip(r).all(|(a, b)| values_eq(a, b)))
                {
                    false
                } else {
                    seen.push(r.clone());
                    true
                }
            });
        }
        if !order_output.is_empty() {
            let keys: Vec<(usize, bool)> = order_output
                .iter()
                .map(|(c, asc)| {
                    let by_name =
                        c.0.is_none()
                            .then(|| out_cols.iter().position(|(n, _)| *n == c.1))
                            .flatten();
                    let idx = by_name
                        .or_else(|| {
                            items
                                .iter()
                                .position(|i| matches!(&i.expr, SelectExpr::Col(k) if k == c))
                        })
                        .ok_or_else(|| format!("ORDER BY unknown output {}", c.1))?;
                    Ok((idx, *asc))
                })
                .collect::<Result<_, String>>()?;
            data.sort_by(|a, b| {
                keys.iter()
                    .map(|(idx, asc)| {
                        let o = cmp_values(&a[*idx], &b[*idx]);
                        if *asc {
                            o
                        } else {
                            o.reverse()
                        }
                    })
                    .find(|o| o.is_ne())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        if let Some(n) = limit {
            data.truncate(n);
        }
        Ok((out_cols, data))
    }
}

struct SelectItem {
    expr: SelectExpr,
    name: String,
}

enum SelectExpr {
    One,
    Col(Col),
    CountRows,
    Count(Col, bool),
    Sum(Col, bool),
    Min(Col),
    Max(Col),
}

impl SelectExpr {
    fn is_aggregate(&self) -> bool {
        !matches!(self, SelectExpr::One | SelectExpr::Col(_))
    }

    fn col_type(&self, resolver: &Resolver<'_>, rels: &[Rel]) -> Result<String, String> {
        match self {
            SelectExpr::Col(c)
            | SelectExpr::Min(c)
            | SelectExpr::Max(c)
            | SelectExpr::Sum(c, _)
            | SelectExpr::Count(c, _) => {
                let (ri, ci) = resolver.resolve(c)?;
                Ok(rels[ri].columns[ci].1.clone())
            }
            _ => Ok("bigint".into()),
        }
    }

    fn type_hint(&self, resolver: &Resolver<'_>, rels: &[Rel]) -> String {
        match self {
            SelectExpr::One | SelectExpr::CountRows | SelectExpr::Count(..) => "bigint".into(),
            SelectExpr::Sum(c, _) => sum_type(&self.col_type(resolver, rels).unwrap_or_default())
                .unwrap_or_else(|| format!("unsummable {}", c.1)),
            _ => self.col_type(resolver, rels).unwrap_or_default(),
        }
    }

    fn eval_row(&self, t: &Tuple, resolver: &Resolver<'_>) -> Result<Value, String> {
        match self {
            SelectExpr::One => Ok(json!(1)),
            SelectExpr::Col(c) => {
                let (ri, ci) = resolver.resolve(c)?;
                Ok(cell(t, ri, ci))
            }
            _ => Err("aggregate outside a group".into()),
        }
    }

    /// `(trino type, value)` over one group.
    fn eval_group(
        &self,
        members: &[Tuple],
        resolver: &Resolver<'_>,
        rels: &[Rel],
    ) -> Result<(String, Value), String> {
        let values = |c: &Col, distinct: bool| -> Result<Vec<Value>, String> {
            let (ri, ci) = resolver.resolve(c)?;
            let mut out: Vec<Value> = Vec::new();
            for t in members {
                let v = cell(t, ri, ci);
                if v.is_null() {
                    continue;
                }
                if distinct && out.iter().any(|o| values_eq(o, &v)) {
                    continue;
                }
                out.push(v);
            }
            Ok(out)
        };
        Ok(match self {
            SelectExpr::One => ("bigint".into(), json!(1)),
            SelectExpr::Col(c) => {
                let (ri, ci) = resolver.resolve(c)?;
                (
                    rels[ri].columns[ci].1.clone(),
                    members
                        .first()
                        .map(|t| cell(t, ri, ci))
                        .unwrap_or(Value::Null),
                )
            }
            SelectExpr::CountRows => ("bigint".into(), json!(members.len())),
            SelectExpr::Count(c, d) => ("bigint".into(), json!(values(c, *d)?.len())),
            SelectExpr::Sum(c, d) => {
                let ty = self.col_type(resolver, rels)?;
                let out_ty = sum_type(&ty).ok_or_else(|| format!("SUM over {ty}"))?;
                let vals = values(c, *d)?;
                if vals.is_empty() {
                    return Ok((out_ty, Value::Null));
                }
                let total: f64 = vals.iter().map(num_of).sum();
                let v = if let Some(scale) = decimal_scale(&out_ty) {
                    json!(format!("{total:.scale$}"))
                } else if out_ty == "bigint" {
                    json!(total as i64)
                } else {
                    json!(total)
                };
                (out_ty, v)
            }
            SelectExpr::Min(c) | SelectExpr::Max(c) => {
                let ty = self.col_type(resolver, rels)?;
                let vals = values(c, false)?;
                let best = vals.into_iter().reduce(|a, b| {
                    let keep_a = match self {
                        SelectExpr::Min(_) => cmp_values(&a, &b).is_le(),
                        _ => cmp_values(&a, &b).is_ge(),
                    };
                    if keep_a {
                        a
                    } else {
                        b
                    }
                });
                (ty, best.unwrap_or(Value::Null))
            }
        })
    }
}

fn num_of(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn decimal_scale(ty: &str) -> Option<usize> {
    let inner = ty.strip_prefix("decimal(")?.strip_suffix(')')?;
    inner.split(',').nth(1)?.trim().parse().ok()
}

fn sum_type(ty: &str) -> Option<String> {
    if let Some(scale) = decimal_scale(ty) {
        return Some(format!("decimal(38,{scale})"));
    }
    match ty {
        "bigint" | "integer" | "smallint" | "tinyint" => Some("bigint".into()),
        "double" | "real" => Some("double".into()),
        _ => None,
    }
}

fn parse_select_item(item: &str) -> Result<SelectItem, String> {
    let (expr, out_name) = match item.rsplit_once(" AS ") {
        Some((e, n)) => (e, Some(n.replace('"', ""))),
        None => (item, None),
    };
    let expr = expr.split(" AT TIME ZONE").next().unwrap_or(expr).trim();
    let agg = |inner: &str| -> (Col, bool) {
        match inner.strip_prefix("DISTINCT ") {
            Some(c) => (colref(c), true),
            None => (colref(inner), false),
        }
    };
    let parsed = if expr == "COUNT(*)" {
        SelectExpr::CountRows
    } else if let Some(inner) = expr
        .strip_prefix("COUNT(")
        .and_then(|r| r.strip_suffix(')'))
    {
        let (c, d) = agg(inner);
        SelectExpr::Count(c, d)
    } else if let Some(inner) = expr.strip_prefix("SUM(").and_then(|r| r.strip_suffix(')')) {
        let (c, d) = agg(inner);
        SelectExpr::Sum(c, d)
    } else if let Some(inner) = expr.strip_prefix("MIN(").and_then(|r| r.strip_suffix(')')) {
        SelectExpr::Min(colref(inner))
    } else if let Some(inner) = expr.strip_prefix("MAX(").and_then(|r| r.strip_suffix(')')) {
        SelectExpr::Max(colref(inner))
    } else {
        SelectExpr::Col(colref(expr))
    };
    let name = match (out_name, &parsed) {
        (Some(n), _) => n,
        (None, SelectExpr::Col(c)) => c.1.clone(),
        (None, _) => "_col0".to_string(),
    };
    Ok(SelectItem { expr: parsed, name })
}

fn cell(t: &Tuple, ri: usize, ci: usize) -> Value {
    t.get(ri)
        .and_then(|r| r.as_ref())
        .and_then(|r| r.get(ci))
        .cloned()
        .unwrap_or(Value::Null)
}

struct Resolver<'a> {
    rels: &'a [Rel],
}

impl Resolver<'_> {
    fn resolve(&self, c: &(Option<String>, String)) -> Result<(usize, usize), String> {
        let candidates: Vec<usize> = match &c.0 {
            Some(a) => self
                .rels
                .iter()
                .enumerate()
                .filter(|(_, r)| &r.alias == a)
                .map(|(i, _)| i)
                .collect(),
            None => (0..self.rels.len()).collect(),
        };
        for ri in candidates {
            if let Some(ci) = self.rels[ri].columns.iter().position(|(n, _)| n == &c.1) {
                return Ok((ri, ci));
            }
        }
        Err(format!("unknown column {:?}.{}", c.0, c.1))
    }
}

impl Respond for FakeSql {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let sql = String::from_utf8_lossy(&request.body).to_string();
        match self.eval(&sql) {
            Ok((columns, data)) => ResponseTemplate::new(200).set_body_json(json!({
                "id": "q",
                "columns": columns.iter().map(|(n, t)| json!({"name": n, "type": t})).collect::<Vec<_>>(),
                "data": data,
                "stats": {"state": "FINISHED"}
            })),
            Err(msg) => ResponseTemplate::new(200).set_body_json(json!({
                "id": "q",
                "stats": {"state": "FAILED"},
                "error": {"message": format!("{msg} [statement: {sql}]"), "errorName": "FIXTURE", "errorCode": 1}
            })),
        }
    }
}

type Col = (Option<String>, String);

/// A parenthesized arithmetic expression over columns and numbers.
#[derive(Debug, Clone)]
enum Expr {
    Col(Col),
    Num(f64),
    Bin(char, Box<Expr>, Box<Expr>),
}

impl Expr {
    fn eval(&self, t: &Tuple, r: &Resolver<'_>) -> Option<f64> {
        match self {
            Expr::Col(c) => {
                let (ri, ci) = r.resolve(c).ok()?;
                number_of(&cell(t, ri, ci))
            }
            Expr::Num(n) => Some(*n),
            Expr::Bin(op, l, r2) => {
                let (a, b) = (l.eval(t, r)?, r2.eval(t, r)?);
                Some(match op {
                    '+' => a + b,
                    '-' => a - b,
                    _ => a * b,
                })
            }
        }
    }
}

fn number_of(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn parse_expr(text: &str) -> Result<Expr, String> {
    let text = text.trim();
    let inner = if text.starts_with('(') && matching_paren(text) == Some(text.len() - 1) {
        &text[1..text.len() - 1]
    } else {
        text
    };
    for op in [" + ", " - ", " * "] {
        if let Some((l, r)) = split_once_top(inner, op) {
            return Ok(Expr::Bin(
                op.trim().chars().next().unwrap(),
                Box::new(parse_expr(l)?),
                Box::new(parse_expr(r)?),
            ));
        }
    }
    if inner.starts_with('"') {
        return Ok(Expr::Col(colref(inner)));
    }
    match parse_literal(inner)? {
        v if number_of(&v).is_some() => Ok(Expr::Num(number_of(&v).unwrap())),
        other => Err(format!("not a number in an expression: {other}")),
    }
}

#[derive(Debug, Clone)]
enum SortKey {
    Col(Col),
    Expr(Expr),
}

enum Pred {
    IsNull(Col, bool),
    Like(Col, String),
    Cmp(Col, String, Value),
    ExprCmp(Expr, String, Value),
    ColEq(Col, Col),
    In(Col, Vec<Value>),
    Not(Box<Pred>),
    And(Vec<Pred>),
    Or(Vec<Pred>),
}

impl Pred {
    fn eval(&self, t: &Tuple, r: &Resolver<'_>) -> bool {
        let get = |c: &Col| r.resolve(c).map(|(ri, ci)| cell(t, ri, ci)).ok();
        match self {
            Pred::IsNull(c, want_null) => get(c).is_none_or(|v| v.is_null()) == *want_null,
            Pred::Like(c, pattern) => match get(c) {
                Some(Value::String(s)) => like_matches(
                    &pattern.chars().collect::<Vec<_>>(),
                    &s.chars().collect::<Vec<_>>(),
                ),
                _ => false,
            },
            Pred::Cmp(c, op, lit) => {
                let Some(v) = get(c) else { return false };
                if v.is_null() {
                    return false;
                }
                let o = cmp_values(&v, lit);
                match op.as_str() {
                    "=" => o.is_eq(),
                    "<>" => o.is_ne(),
                    "<" => o.is_lt(),
                    "<=" => o.is_le(),
                    ">" => o.is_gt(),
                    ">=" => o.is_ge(),
                    _ => false,
                }
            }
            Pred::ExprCmp(e, op, lit) => {
                let Some(v) = e.eval(t, r) else { return false };
                let o = cmp_values(&json!(v), lit);
                match op.as_str() {
                    "=" => o.is_eq(),
                    "<>" => o.is_ne(),
                    "<" => o.is_lt(),
                    "<=" => o.is_le(),
                    ">" => o.is_gt(),
                    ">=" => o.is_ge(),
                    _ => false,
                }
            }
            Pred::ColEq(a, b) => match (get(a), get(b)) {
                (Some(x), Some(y)) => !x.is_null() && !y.is_null() && values_eq(&x, &y),
                _ => false,
            },
            Pred::In(c, lits) => {
                let Some(v) = get(c) else { return false };
                !v.is_null() && lits.iter().any(|l| values_eq(&v, l))
            }
            Pred::Not(p) => !p.eval(t, r),
            Pred::And(ps) => ps.iter().all(|p| p.eval(t, r)),
            Pred::Or(ps) => ps.iter().any(|p| p.eval(t, r)),
        }
    }
}

fn parse_pred(text: &str) -> Result<Pred, String> {
    let text = strip_parens(text.trim());
    let ands = split_top(text, " AND ");
    if ands.len() > 1 {
        return Ok(Pred::And(
            ands.into_iter().map(parse_pred).collect::<Result<_, _>>()?,
        ));
    }
    let ors = split_top(text, " OR ");
    if ors.len() > 1 {
        return Ok(Pred::Or(
            ors.into_iter().map(parse_pred).collect::<Result<_, _>>()?,
        ));
    }
    if let Some(inner) = text.strip_prefix("NOT ") {
        return Ok(Pred::Not(Box::new(parse_pred(inner)?)));
    }
    if let Some(c) = text.strip_suffix(" IS NOT NULL") {
        return Ok(Pred::IsNull(colref(c), false));
    }
    if let Some(c) = text.strip_suffix(" IS NULL") {
        return Ok(Pred::IsNull(colref(c), true));
    }
    if let Some((c, rest)) = split_once_top(text, " LIKE ") {
        let lit = rest
            .strip_suffix(" ESCAPE '!'")
            .ok_or("LIKE without the lane's ESCAPE clause")?;
        let Value::String(pattern) = parse_literal(lit)? else {
            return Err("LIKE pattern is not a string".into());
        };
        return Ok(Pred::Like(colref(c), pattern));
    }
    if let Some((c, list)) = split_once_top(text, " IN ") {
        let inner = strip_parens(list.trim());
        let lits = split_top(inner, ", ")
            .into_iter()
            .map(parse_literal)
            .collect::<Result<_, _>>()?;
        return Ok(Pred::In(colref(c), lits));
    }
    for op in ["<>", "<=", ">=", "=", "<", ">"] {
        if let Some((l, r)) = split_once_top(text, &format!(" {op} ")) {
            if r.trim().starts_with('"') {
                return Ok(Pred::ColEq(colref(l), colref(r)));
            }
            if l.trim().starts_with('(') {
                return Ok(Pred::ExprCmp(
                    parse_expr(l)?,
                    op.to_string(),
                    parse_literal(r)?,
                ));
            }
            return Ok(Pred::Cmp(colref(l), op.to_string(), parse_literal(r)?));
        }
    }
    Err(format!("unparsed predicate: {text}"))
}

/// Case-sensitive `LIKE` with `!` as the escape character.
fn like_matches(pattern: &[char], text: &[char]) -> bool {
    match pattern {
        [] => text.is_empty(),
        ['%', rest @ ..] => (0..=text.len()).any(|i| like_matches(rest, &text[i..])),
        ['_', rest @ ..] => !text.is_empty() && like_matches(rest, &text[1..]),
        ['!', c, rest @ ..] | [c, rest @ ..] => {
            text.first() == Some(c) && like_matches(rest, &text[1..])
        }
    }
}

/// `"alias"."col"` or `"col"` → (alias, col).
fn colref(s: &str) -> Col {
    let s = s.trim();
    let parts: Vec<&str> = s.split("\".\"").collect();
    if parts.len() == 2 {
        (
            Some(parts[0].trim_start_matches('"').to_string()),
            parts[1].trim_end_matches('"').to_string(),
        )
    } else {
        (None, s.replace('"', ""))
    }
}

fn parse_literal(s: &str) -> Result<Value, String> {
    let s = s.trim();
    if s == "TRUE" {
        return Ok(json!(true));
    }
    if s == "FALSE" {
        return Ok(json!(false));
    }
    for prefix in ["DATE ", "TIMESTAMP ", "BINARY "] {
        if let Some(r) = s.strip_prefix(prefix) {
            return parse_literal(r);
        }
    }
    if let Some(inner) = s.strip_prefix('\'') {
        let inner = inner.strip_suffix('\'').ok_or("unterminated string")?;
        return Ok(json!(inner.replace("''", "'")));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(json!(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(json!(f));
    }
    Err(format!("unparsed literal: {s}"))
}

fn values_eq(a: &Value, b: &Value) -> bool {
    cmp_values(a, b).is_eq()
}

fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::Number(x), Value::String(y)) => x
            .as_f64()
            .partial_cmp(&y.parse::<f64>().ok())
            .unwrap_or(Ordering::Equal),
        (Value::String(x), Value::Number(y)) => x
            .parse::<f64>()
            .ok()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        // Decimals arrive as strings; order them as numbers when both parse.
        (Value::String(x), Value::String(y)) => match (x.parse::<f64>(), y.parse::<f64>()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _ => x.cmp(y),
        },
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

/// Split on `sep` outside quotes and parentheses.
fn split_top<'a>(s: &'a str, sep: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut start = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < s.len() {
        let c = bytes[i] as char;
        if c == '\'' {
            in_str = !in_str;
        } else if !in_str {
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
            } else if depth == 0 && s[i..].starts_with(sep) {
                out.push(&s[start..i]);
                i += sep.len();
                start = i;
                continue;
            }
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

fn split_once_top<'a>(s: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let parts = split_top(s, sep);
    if parts.len() < 2 {
        return None;
    }
    let first = parts[0];
    Some((first, &s[first.len() + sep.len()..]))
}

/// Index of the `)` matching the `(` at position 0.
fn matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_str = false;
    for (i, c) in s.char_indices() {
        match c {
            '\'' => in_str = !in_str,
            '(' if !in_str => depth += 1,
            ')' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn strip_parens(s: &str) -> &str {
    let s = s.trim();
    if s.starts_with('(') && matching_paren(s) == Some(s.len() - 1) {
        strip_parens(&s[1..s.len() - 1])
    } else {
        s
    }
}

/// Cut a clause body at the next top-level keyword.
fn cut_clause(s: &str) -> (&str, &str) {
    let mut best: Option<usize> = None;
    for kw in [
        " WHERE ",
        " GROUP BY ",
        " HAVING ",
        " ORDER BY ",
        " LIMIT ",
        " JOIN ",
        " LEFT JOIN ",
    ] {
        if let Some((head, _)) = split_once_top(s, kw) {
            let pos = head.len();
            if best.is_none_or(|b| pos < b) {
                best = Some(pos);
            }
        }
    }
    match best {
        Some(pos) => (&s[..pos], s[pos + 1..].trim_start()),
        None => (s, ""),
    }
}
