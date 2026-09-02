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
    And(Vec<Pred>),
    Or(Vec<Pred>),
    Not(Box<Pred>),
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// One projected column and the unique name it is returned under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCol {
    pub col: ColRef,
    pub name: String,
}

/// A complete pushed-down block: a join tree, a projection, and the
/// modifiers a backend can apply exactly.
#[derive(Debug, Clone, PartialEq)]
pub struct RelPlan {
    pub root: RelNode,
    pub output: Vec<OutputCol>,
    pub distinct: bool,
    /// `(column, ascending)`.
    pub order_by: Vec<(ColRef, bool)>,
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
    /// String `=` compares bytes (not a case-folding collation).
    pub string_eq_is_binary: bool,
    /// String `<` orders by code point (not a locale collation).
    pub string_order_is_codepoint: bool,
}
