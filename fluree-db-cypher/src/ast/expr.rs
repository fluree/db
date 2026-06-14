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
    /// `EXISTS { pattern }`.
    Exists(Box<Pattern>, SourceSpan),
    /// Inline list literal `[expr, expr, ...]`.
    List(Vec<Expr>, SourceSpan),
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
            | Expr::Exists(_, s)
            | Expr::List(_, s) => *s,
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
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}
