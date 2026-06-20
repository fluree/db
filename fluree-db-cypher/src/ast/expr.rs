//! Expression AST.

use crate::span::SourceSpan;

use super::pattern::Pattern;

/// A Cypher variable. Cypher variables are bare identifiers, not
/// `?x` like SPARQL; we keep the name as-is and disambiguate from
/// labels/keys by syntactic position.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Variable {
    pub name: String,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Var(Variable),
    Lit(Literal),
    Param(ParamRef),
    /// `n.prop` — property lookup.
    Prop(Box<Expr>, String, SourceSpan),
    BinOp(BinOp, Box<Expr>, Box<Expr>, SourceSpan),
    UnaryOp(UnaryOp, Box<Expr>, SourceSpan),
    /// `f(arg, arg, ...)`.
    Call(FuncCall),
    /// `expr IN [list]` or `expr IN $param`.
    In(Box<Expr>, Box<Expr>, SourceSpan),
    /// `expr IS NULL`.
    IsNull(Box<Expr>, SourceSpan),
    /// `expr IS NOT NULL`.
    IsNotNull(Box<Expr>, SourceSpan),
    /// `expr STARTS WITH expr`.
    StartsWith(Box<Expr>, Box<Expr>, SourceSpan),
    /// `expr ENDS WITH expr`.
    EndsWith(Box<Expr>, Box<Expr>, SourceSpan),
    /// `expr CONTAINS expr`.
    Contains(Box<Expr>, Box<Expr>, SourceSpan),
    /// `CASE ... WHEN ... THEN ... ELSE ... END`.
    Case(Box<CaseExpr>),
    /// `EXISTS { pattern }` or the subquery form
    /// `EXISTS { MATCH pattern WHERE expr }`. The optional second element is
    /// the inner `WHERE` condition, ANDed into the existence test.
    Exists(Box<Pattern>, Option<Box<Expr>>, SourceSpan),
    /// Inline list literal `[expr, expr, ...]`.
    List(Vec<Expr>, SourceSpan),
    /// Inline map literal `{key: expr, ...}` in expression position (e.g.
    /// `RETURN {name: n.name}`). Keys are insertion-ordered.
    Map(Vec<(String, Expr)>, SourceSpan),
    /// `expr[index]` — list element access.
    Index(Box<Expr>, Box<Expr>, SourceSpan),
    /// List comprehension `[var IN list WHERE filter | map]` (boxed — these
    /// iteration variants are large and would otherwise bloat every `Expr`).
    ListComprehension(Box<ListComprehensionExpr>),
    /// `reduce(acc = init, var IN list | body)`.
    Reduce(Box<ReduceExpr>),
    /// List predicate `all/any/none/single(var IN list WHERE pred)`.
    ListPredicate(Box<ListPredicateExpr>),
    /// Map projection `var{.key, .*, key: expr}` — build a map from a node/map
    /// variable's properties (boxed to keep `Expr` small).
    MapProjection(Box<MapProjectionExpr>),
    /// Pattern comprehension `[(a)-[:T]->(b) WHERE pred | proj]` — a correlated
    /// subquery collecting `proj` over each match (boxed; the pattern is large).
    PatternComprehension(Box<PatternComprehensionExpr>),
}

/// `[pattern WHERE filter | projection]`.
#[derive(Clone, Debug, PartialEq)]
pub struct PatternComprehensionExpr {
    pub pattern: Pattern,
    pub filter: Option<Box<Expr>>,
    pub projection: Box<Expr>,
    pub span: SourceSpan,
}

/// `var{ selector, … }`.
#[derive(Clone, Debug, PartialEq)]
pub struct MapProjectionExpr {
    pub var: Variable,
    pub selectors: Vec<MapProjectionSelector>,
    pub span: SourceSpan,
}

/// One entry of a map projection.
#[derive(Clone, Debug, PartialEq)]
pub enum MapProjectionSelector {
    /// `.key` — include `var.key` under `key`.
    Property(String),
    /// `.*` — include every data property of `var`.
    AllProperties,
    /// `key: expr` — an explicit entry.
    Literal(String, Box<Expr>),
}

/// `[var IN list WHERE filter | map]`.
#[derive(Clone, Debug, PartialEq)]
pub struct ListComprehensionExpr {
    pub var: Variable,
    pub list: Box<Expr>,
    pub filter: Option<Box<Expr>>,
    pub map: Option<Box<Expr>>,
    pub span: SourceSpan,
}

/// `reduce(acc = init, var IN list | body)`.
#[derive(Clone, Debug, PartialEq)]
pub struct ReduceExpr {
    pub acc: Variable,
    pub init: Box<Expr>,
    pub var: Variable,
    pub list: Box<Expr>,
    pub body: Box<Expr>,
    pub span: SourceSpan,
}

/// `all/any/none/single(var IN list WHERE pred)`.
#[derive(Clone, Debug, PartialEq)]
pub struct ListPredicateExpr {
    pub kind: ListPredicateKind,
    pub var: Variable,
    pub list: Box<Expr>,
    pub predicate: Box<Expr>,
    pub span: SourceSpan,
}

/// Which quantifier a list-predicate expression applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListPredicateKind {
    All,
    Any,
    None,
    Single,
}

impl Expr {
    pub fn span(&self) -> SourceSpan {
        match self {
            Expr::Var(v) => v.span,
            Expr::Lit(l) => l.span(),
            Expr::Param(p) => p.span,
            Expr::Prop(_, _, s)
            | Expr::BinOp(_, _, _, s)
            | Expr::UnaryOp(_, _, s)
            | Expr::In(_, _, s)
            | Expr::IsNull(_, s)
            | Expr::IsNotNull(_, s)
            | Expr::StartsWith(_, _, s)
            | Expr::EndsWith(_, _, s)
            | Expr::Contains(_, _, s)
            | Expr::Exists(_, _, s)
            | Expr::List(_, s)
            | Expr::Map(_, s)
            | Expr::Index(_, _, s) => *s,
            Expr::ListComprehension(c) => c.span,
            Expr::Reduce(r) => r.span,
            Expr::ListPredicate(p) => p.span,
            Expr::MapProjection(m) => m.span,
            Expr::PatternComprehension(pc) => pc.span,
            Expr::Call(c) => c.span,
            Expr::Case(c) => c.span,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParamRef {
    pub name: String,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FuncCall {
    pub name: String,
    pub args: Vec<Expr>,
    /// `count(DISTINCT x)`.
    pub distinct: bool,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CaseExpr {
    /// Optional subject expression for the `CASE expr WHEN ... END` form.
    pub subject: Option<Expr>,
    pub branches: Vec<(Expr, Expr)>,
    pub else_branch: Option<Expr>,
    pub span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Integer(i64, SourceSpan),
    Float(f64, SourceSpan),
    String(String, SourceSpan),
    Bool(bool, SourceSpan),
    Null(SourceSpan),
}

impl Literal {
    pub fn span(&self) -> SourceSpan {
        match self {
            Literal::Integer(_, s)
            | Literal::Float(_, s)
            | Literal::String(_, s)
            | Literal::Bool(_, s)
            | Literal::Null(s) => *s,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    /// Exponentiation (`^`).
    Pow,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}
