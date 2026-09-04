//! A dialect-neutral relational plan for pushing a whole query block down to a
//! tabular backend that can execute joins itself (a SQL database behind a
//! Trino-protocol endpoint today).
//!
//! The query engine lowers a `GRAPH <source> { … }` block over an R2RML
//! mapping into one [`RelPlan`]; a provider renders it for its dialect and
//! streams the result as column batches. The plan speaks only in table
//! columns and typed literals — RDF terms are built from the returned columns
//! by the engine, so no dialect ever sees an IRI template.
//!
//! These types live here rather than in the query or SQL crates so both can
//! use them without depending on each other.

use crate::FieldType;

/// A typed literal in a plan predicate or key set.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Str(String),
    /// Days since 1970-01-01.
    Date(i32),
    Double(f64),
    Decimal {
        unscaled: i128,
        scale: i8,
    },
    /// Micros since the epoch; `tz` = the source literal carried an offset.
    Timestamp {
        micros: i64,
        tz: bool,
    },
    /// A raw column value recovered by reversing a subject template. Its type
    /// is whatever the column's type is; rendered only for int/string columns.
    TemplateKey(String),
    Set(Vec<Literal>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    In,
}

/// `alias.column` — a column of one relation in the plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColRef {
    pub alias: String,
    pub column: String,
}

impl ColRef {
    pub fn new(alias: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            column: column.into(),
        }
    }
}

/// A predicate over plan columns and literals.
#[derive(Debug, Clone, PartialEq)]
pub enum Pred {
    Cmp {
        col: ColRef,
        op: CmpOp,
        value: Literal,
    },
    ColEq {
        left: ColRef,
        right: ColRef,
    },
    IsNull(ColRef),
    IsNotNull(ColRef),
    /// `col LIKE pattern ESCAPE '!'`: `%` and `_` are wildcards unless
    /// escaped with `!` (see [`like_escape`]).
    Like {
        col: ColRef,
        pattern: String,
    },
    And(Vec<Pred>),
    Or(Vec<Pred>),
    Not(Box<Pred>),
}

/// `s` as a literal fragment of a [`Pred::Like`] pattern.
pub fn like_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '%' | '_' | '!') {
            out.push('!');
        }
        out.push(c);
    }
    out
}

impl Pred {
    /// Flatten a conjunction, dropping empty `And`s.
    pub fn and(preds: Vec<Pred>) -> Option<Pred> {
        let mut flat = Vec::with_capacity(preds.len());
        for p in preds {
            match p {
                Pred::And(inner) => flat.extend(inner),
                other => flat.push(other),
            }
        }
        match flat.len() {
            0 => None,
            1 => flat.pop(),
            _ => Some(Pred::And(flat)),
        }
    }
}

/// Where a relation's rows come from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RelSource {
    /// Dotted table name.
    Table(String),
    /// Verbatim SQL text used as a derived table (an `rr:sqlQuery`).
    Query(String),
}

/// A literal relation carrying values the engine already holds (a `VALUES`
/// block, or bindings from the outer query), joined into the plan so the
/// backend does the semi-join instead of the engine re-scanning per row.
#[derive(Debug, Clone, PartialEq)]
pub struct KeySet {
    pub alias: String,
    /// Column names and types. A `None` type is inferred by the renderer from
    /// the table column the key column is equated with.
    pub columns: Vec<(String, Option<FieldType>)>,
    pub rows: Vec<Vec<Literal>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelNode {
    Access {
        alias: String,
        source: RelSource,
    },
    KeySet(KeySet),
    Filter {
        input: Box<RelNode>,
        pred: Pred,
    },
    Join {
        left: Box<RelNode>,
        right: Box<RelNode>,
        on: Pred,
    },
    LeftJoin {
        left: Box<RelNode>,
        right: Box<RelNode>,
        on: Pred,
    },
}

impl RelNode {
    /// Every `(alias, source)` accessed by this subtree, in tree order.
    pub fn accesses(&self) -> Vec<(&str, &RelSource)> {
        let mut out = Vec::new();
        self.collect_accesses(&mut out);
        out
    }

    fn collect_accesses<'a>(&'a self, out: &mut Vec<(&'a str, &'a RelSource)>) {
        match self {
            RelNode::Access { alias, source } => out.push((alias, source)),
            RelNode::KeySet(_) => {}
            RelNode::Filter { input, .. } => input.collect_accesses(out),
            RelNode::Join { left, right, .. } | RelNode::LeftJoin { left, right, .. } => {
                left.collect_accesses(out);
                right.collect_accesses(out);
            }
        }
    }
}

/// What one output column computes: a column, or an aggregate over the
/// groups `RelPlan::group_by` forms (the whole input when it is empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputExpr {
    Col(ColRef),
    /// `COUNT(*)`.
    CountRows,
    /// `COUNT([DISTINCT] col)`: non-null values.
    Count {
        col: ColRef,
        distinct: bool,
    },
    /// `SUM([DISTINCT] col)`; NULL over an empty input.
    Sum {
        col: ColRef,
        distinct: bool,
    },
    Min(ColRef),
    Max(ColRef),
}

impl OutputExpr {
    pub fn col(&self) -> Option<&ColRef> {
        match self {
            OutputExpr::Col(c)
            | OutputExpr::Count { col: c, .. }
            | OutputExpr::Sum { col: c, .. }
            | OutputExpr::Min(c)
            | OutputExpr::Max(c) => Some(c),
            OutputExpr::CountRows => None,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        !matches!(self, OutputExpr::Col(_))
    }
}

/// One projected expression and the unique name it is returned under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCol {
    pub expr: OutputExpr,
    pub name: String,
}

impl OutputCol {
    pub fn column(col: ColRef, name: impl Into<String>) -> Self {
        Self {
            expr: OutputExpr::Col(col),
            name: name.into(),
        }
    }
}

/// An `ORDER BY` key: a column of the join, or one of the plan's outputs by
/// name (the only way to order by an aggregate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderKey {
    Col(ColRef),
    Output(String),
}

/// A complete pushed-down block: a join tree, a projection, and the
/// modifiers a backend can apply exactly.
#[derive(Debug, Clone, PartialEq)]
pub struct RelPlan {
    pub root: RelNode,
    pub output: Vec<OutputCol>,
    /// Grouping columns; every non-aggregate output must be one of them.
    pub group_by: Vec<ColRef>,
    pub distinct: bool,
    /// `(key, ascending)`.
    pub order_by: Vec<(OrderKey, bool)>,
    pub limit: Option<u64>,
}

/// What a provider can execute, consulted by the lowering before it emits a
/// node it could not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushdownCapabilities {
    pub left_join: bool,
    /// Rows per [`KeySet`] before the engine must chunk.
    pub keyset_max_rows: usize,
    /// Rendered statement size the provider will send.
    pub statement_max_bytes: usize,
    /// String `=` against a literal or another column compares bytes (not a
    /// case-folding collation), possibly because the renderer forces it.
    pub string_eq_is_binary: bool,
    /// `DISTINCT`, `GROUP BY` and `COUNT(DISTINCT …)` over a string column
    /// keep byte-distinct values apart. Separate from `string_eq_is_binary`
    /// because a comparison can be forced binary where a grouping cannot
    /// (MySQL's `ONLY_FULL_GROUP_BY` rejects `GROUP BY BINARY col`).
    pub string_distinct_is_binary: bool,
    /// String `<` orders by code point (not a locale collation).
    pub string_order_is_codepoint: bool,
    /// A `timestamp` without a zone is stored as text (SQLite), ordered by
    /// its characters. The date prefix orders correctly whatever the time
    /// separator; a comparison at time-of-day granularity does not.
    pub timestamp_is_text: bool,
}

/// Whether these two column types compare as the same class under a database
/// `=`. Lives here, beside the plan the check is about, so that a lowering
/// deciding whether to push a join and a renderer deciding whether it can emit
/// one read the same verdict: when they drifted, a renderer refusal surfaced as
/// a query error after the lowering had already committed to the pushdown.
pub fn same_class(a: FieldType, b: FieldType) -> bool {
    use FieldType as F;
    let numeric = |t| {
        matches!(
            t,
            F::Int32 | F::Int64 | F::Float32 | F::Float64 | F::Decimal { .. }
        )
    };
    match (a, b) {
        _ if numeric(a) && numeric(b) => true,
        (F::String, F::String)
        | (F::Boolean, F::Boolean)
        | (F::Date, F::Date)
        | (F::Timestamp, F::Timestamp)
        | (F::TimestampTz, F::TimestampTz)
        | (F::Bytes, F::Bytes) => true,
        _ => false,
    }
}

/// Every column-to-column equality this plan will render, from any join `ON` or
/// `WHERE`, so a caller can vet the pairs before committing to the plan.
pub fn collect_col_eqs(node: &RelNode, out: &mut Vec<(ColRef, ColRef)>) {
    fn from_pred(p: &Pred, out: &mut Vec<(ColRef, ColRef)>) {
        match p {
            Pred::ColEq { left, right } => out.push((left.clone(), right.clone())),
            Pred::And(ps) | Pred::Or(ps) => ps.iter().for_each(|q| from_pred(q, out)),
            Pred::Not(q) => from_pred(q, out),
            _ => {}
        }
    }
    match node {
        RelNode::Access { .. } | RelNode::KeySet(_) => {}
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
