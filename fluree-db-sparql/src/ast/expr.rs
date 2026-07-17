//! SPARQL expression types.
//!
//! This module defines the AST for SPARQL expressions used in FILTER,
//! BIND, SELECT (expr AS ?var), ORDER BY, and HAVING clauses.
//! All nodes carry source spans for diagnostics.

use super::pattern::GraphPattern;
use super::term::{Iri, Literal, Var};
use crate::span::SourceSpan;
use std::sync::Arc;

/// A SPARQL expression.
///
/// This represents expressions that can appear in FILTER, BIND, SELECT,
/// ORDER BY, and HAVING clauses.
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    /// A variable reference
    Var(Var),

    /// A literal value
    Literal(Literal),

    /// An IRI (can appear in expressions via function calls, IN lists, etc.)
    Iri(Iri),

    /// Binary operation (arithmetic, comparison, boolean)
    Binary {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
        span: SourceSpan,
    },

    /// Unary operation (negation, logical NOT)
    Unary {
        op: UnaryOp,
        operand: Box<Expression>,
        span: SourceSpan,
    },

    /// Function call (built-in or extension)
    FunctionCall {
        name: FunctionName,
        args: Vec<Expression>,
        distinct: bool, // For aggregate functions
        span: SourceSpan,
    },

    /// IF(condition, then, else)
    If {
        condition: Box<Expression>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
        span: SourceSpan,
    },

    /// COALESCE(expr, expr, ...)
    Coalesce {
        args: Vec<Expression>,
        span: SourceSpan,
    },

    /// IN / NOT IN list
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
        span: SourceSpan,
    },

    /// EXISTS { pattern }
    Exists {
        pattern: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// NOT EXISTS { pattern }
    NotExists {
        pattern: Box<GraphPattern>,
        span: SourceSpan,
    },

    /// Aggregate function (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE)
    Aggregate {
        function: AggregateFunction,
        expr: Option<Box<Expression>>, // None for COUNT(*)
        distinct: bool,
        separator: Option<Arc<str>>, // For GROUP_CONCAT
        span: SourceSpan,
    },

    /// Parenthesized expression (preserved for span accuracy)
    Bracketed {
        inner: Box<Expression>,
        span: SourceSpan,
    },
}

impl Expression {
    /// Get the source span of this expression.
    pub fn span(&self) -> SourceSpan {
        match self {
            Expression::Var(v) => v.span,
            Expression::Literal(l) => l.span,
            Expression::Iri(i) => i.span,
            Expression::Binary { span, .. } => *span,
            Expression::Unary { span, .. } => *span,
            Expression::FunctionCall { span, .. } => *span,
            Expression::If { span, .. } => *span,
            Expression::Coalesce { span, .. } => *span,
            Expression::In { span, .. } => *span,
            Expression::Exists { span, .. } => *span,
            Expression::NotExists { span, .. } => *span,
            Expression::Aggregate { span, .. } => *span,
            Expression::Bracketed { span, .. } => *span,
        }
    }

    /// Create a variable expression.
    pub fn var(var: Var) -> Self {
        Expression::Var(var)
    }

    /// Create a literal expression.
    pub fn literal(lit: Literal) -> Self {
        Expression::Literal(lit)
    }

    /// Create an IRI expression.
    pub fn iri(iri: Iri) -> Self {
        Expression::Iri(iri)
    }

    /// Create a binary expression.
    pub fn binary(op: BinaryOp, left: Expression, right: Expression, span: SourceSpan) -> Self {
        Expression::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            span,
        }
    }

    /// Create a unary expression.
    pub fn unary(op: UnaryOp, operand: Expression, span: SourceSpan) -> Self {
        Expression::Unary {
            op,
            operand: Box::new(operand),
            span,
        }
    }

    /// Create a function call expression.
    pub fn function_call(name: FunctionName, args: Vec<Expression>, span: SourceSpan) -> Self {
        Expression::FunctionCall {
            name,
            args,
            distinct: false,
            span,
        }
    }

    /// Unwrap any bracketed expressions to get the innermost expression.
    ///
    /// In SPARQL, parenthesized expressions like `(?var)` are valid and equivalent
    /// to just `?var`. This method recursively unwraps `Bracketed` wrappers to
    /// get to the underlying expression.
    ///
    /// # Examples
    /// - `?var` → `?var`
    /// - `(?var)` → `?var`
    /// - `((?var))` → `?var`
    pub fn unwrap_bracketed(&self) -> &Expression {
        match self {
            Expression::Bracketed { inner, .. } => inner.unwrap_bracketed(),
            _ => self,
        }
    }

    /// Walk this expression tree pre-order (visiting `self` first), calling
    /// `f` on every nested sub-expression.
    ///
    /// Does **not** descend into `EXISTS` / `NOT EXISTS` graph patterns:
    /// those introduce a pattern scope, not an expression scope, so their
    /// contents are never sub-expressions of the surrounding expression
    /// (an aggregate inside an EXISTS filter belongs to the inner scope,
    /// and its variables are not free variables of this expression).
    pub fn walk<'a>(&'a self, f: &mut impl FnMut(&'a Expression)) {
        f(self);
        match self {
            Expression::Var(_) | Expression::Literal(_) | Expression::Iri(_) => {}
            Expression::Binary { left, right, .. } => {
                left.walk(f);
                right.walk(f);
            }
            Expression::Unary { operand, .. } => operand.walk(f),
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    arg.walk(f);
                }
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                condition.walk(f);
                then_expr.walk(f);
                else_expr.walk(f);
            }
            Expression::Coalesce { args, .. } => {
                for arg in args {
                    arg.walk(f);
                }
            }
            Expression::In { expr, list, .. } => {
                expr.walk(f);
                for item in list {
                    item.walk(f);
                }
            }
            Expression::Exists { .. } | Expression::NotExists { .. } => {}
            Expression::Aggregate { expr, .. } => {
                if let Some(inner) = expr {
                    inner.walk(f);
                }
            }
            Expression::Bracketed { inner, .. } => inner.walk(f),
        }
    }

    /// True if this expression contains an aggregate call (`COUNT`, `SUM`,
    /// `AVG`, `MIN`, `MAX`, `GROUP_CONCAT`, `SAMPLE`) at this expression's
    /// scope level. Does not look inside `EXISTS`/`NOT EXISTS` patterns
    /// (see [`Expression::walk`]).
    pub fn contains_aggregate(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| {
            if matches!(e, Expression::Aggregate { .. }) {
                found = true;
            }
        });
        found
    }

    /// Collect every variable referenced by this expression, including
    /// variables inside aggregate arguments. Variables inside
    /// `EXISTS`/`NOT EXISTS` patterns are excluded (see
    /// [`Expression::walk`]). Duplicates are preserved in visit order.
    pub fn variables(&self) -> Vec<&Var> {
        let mut vars = Vec::new();
        self.walk(&mut |e| {
            if let Expression::Var(v) = e {
                vars.push(v);
            }
        });
        vars
    }

    /// Collect the variables of this expression that appear **outside** any
    /// aggregate call. These are the variables that, in a grouped query,
    /// must each be a group key for the expression to be valid in a
    /// projection (SPARQL 1.1 §18.2.4 / grammar note on `SelectClause`).
    pub fn unaggregated_variables(&self) -> Vec<&Var> {
        let mut vars = Vec::new();
        self.collect_unaggregated_variables(&mut vars);
        vars
    }

    fn collect_unaggregated_variables<'a>(&'a self, out: &mut Vec<&'a Var>) {
        match self {
            Expression::Var(v) => out.push(v),
            Expression::Literal(_) | Expression::Iri(_) => {}
            Expression::Binary { left, right, .. } => {
                left.collect_unaggregated_variables(out);
                right.collect_unaggregated_variables(out);
            }
            Expression::Unary { operand, .. } => operand.collect_unaggregated_variables(out),
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    arg.collect_unaggregated_variables(out);
                }
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                condition.collect_unaggregated_variables(out);
                then_expr.collect_unaggregated_variables(out);
                else_expr.collect_unaggregated_variables(out);
            }
            Expression::Coalesce { args, .. } => {
                for arg in args {
                    arg.collect_unaggregated_variables(out);
                }
            }
            Expression::In { expr, list, .. } => {
                expr.collect_unaggregated_variables(out);
                for item in list {
                    item.collect_unaggregated_variables(out);
                }
            }
            Expression::Exists { .. } | Expression::NotExists { .. } => {}
            // Variables inside an aggregate argument are aggregated — skip.
            Expression::Aggregate { .. } => {}
            Expression::Bracketed { inner, .. } => inner.collect_unaggregated_variables(out),
        }
    }
}

/// Binary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    // Logical
    And, // &&
    Or,  // ||

    // Comparison
    Eq, // =
    Ne, // !=
    Lt, // <
    Le, // <=
    Gt, // >
    Ge, // >=

    // Arithmetic
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
}

impl BinaryOp {
    /// Get the operator symbol as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            BinaryOp::And => "&&",
            BinaryOp::Or => "||",
            BinaryOp::Eq => "=",
            BinaryOp::Ne => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::Le => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::Ge => ">=",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
        }
    }

    /// Get the precedence level (higher binds tighter).
    pub fn precedence(&self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge => 3,
            BinaryOp::Add | BinaryOp::Sub => 4,
            BinaryOp::Mul | BinaryOp::Div => 5,
        }
    }
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// Logical NOT (!)
    Not,
    /// Arithmetic negation (-)
    Neg,
    /// Unary plus (+)
    Pos,
}

impl UnaryOp {
    /// Get the operator symbol as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            UnaryOp::Not => "!",
            UnaryOp::Neg => "-",
            UnaryOp::Pos => "+",
        }
    }
}

/// Built-in function names.
///
/// SPARQL has many built-in functions, categorized by their purpose.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionName {
    // Type checking functions
    Bound,
    IsIri,
    IsUri, // Alias for IsIri
    IsBlank,
    IsLiteral,
    IsNumeric,

    // Accessors
    Str,
    Lang,
    Datatype,

    // Constructor functions
    Iri,
    Uri, // Alias for Iri
    BNode,

    // String functions
    Strlen,
    Substr,
    Ucase,
    Lcase,
    StrStarts,
    StrEnds,
    Contains,
    StrBefore,
    StrAfter,
    EncodeForUri,
    Concat,
    LangMatches,
    Regex,
    Replace,
    StrDt,
    StrLang,

    // Numeric functions
    Abs,
    Round,
    Ceil,
    Floor,
    Rand,

    // Date/time functions
    Now,
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
    Timezone,
    Tz,

    // Hash functions
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,

    // Comparison functions
    SameTerm,

    // UUID functions
    Uuid,
    StrUuid,

    // SPARQL 1.1 functions
    If,       // Handled separately as Expression::If but may appear
    Coalesce, // Handled separately as Expression::Coalesce but may appear

    // Vector similarity functions (Fluree extensions)
    DotProduct,
    CosineSimilarity,
    EuclideanDistance,

    // SPARQL 1.2 triple-term functions (RDF-star). Accept-then-defer per
    // burn-down decision D-1: parsed and arity-validated here, but lowering
    // rejects them with `not_implemented` (no evaluable capability yet).
    Triple,    // TRIPLE(s, p, o)
    Subject,   // SUBJECT(t)
    Predicate, // PREDICATE(t)
    Object,    // OBJECT(t)
    IsTriple,  // isTRIPLE(t)

    /// Custom extension function (IRI)
    Extension(Iri),
}

impl FunctionName {
    /// Parse a function name from a string (case-insensitive for built-ins).
    pub fn parse(name: &str) -> Option<Self> {
        // Case-insensitive matching for built-in functions
        match name.to_uppercase().as_str() {
            "BOUND" => Some(FunctionName::Bound),
            "ISIRI" => Some(FunctionName::IsIri),
            "ISURI" => Some(FunctionName::IsUri),
            "ISBLANK" => Some(FunctionName::IsBlank),
            "ISLITERAL" => Some(FunctionName::IsLiteral),
            "ISNUMERIC" => Some(FunctionName::IsNumeric),
            "STR" => Some(FunctionName::Str),
            "LANG" => Some(FunctionName::Lang),
            "DATATYPE" => Some(FunctionName::Datatype),
            "IRI" => Some(FunctionName::Iri),
            "URI" => Some(FunctionName::Uri),
            "BNODE" => Some(FunctionName::BNode),
            "STRLEN" => Some(FunctionName::Strlen),
            "SUBSTR" => Some(FunctionName::Substr),
            "UCASE" => Some(FunctionName::Ucase),
            "LCASE" => Some(FunctionName::Lcase),
            "STRSTARTS" => Some(FunctionName::StrStarts),
            "STRENDS" => Some(FunctionName::StrEnds),
            "CONTAINS" => Some(FunctionName::Contains),
            "STRBEFORE" => Some(FunctionName::StrBefore),
            "STRAFTER" => Some(FunctionName::StrAfter),
            "ENCODEFORURI" | "ENCODE_FOR_URI" => Some(FunctionName::EncodeForUri),
            "CONCAT" => Some(FunctionName::Concat),
            "LANGMATCHES" => Some(FunctionName::LangMatches),
            "REGEX" => Some(FunctionName::Regex),
            "REPLACE" => Some(FunctionName::Replace),
            "STRDT" => Some(FunctionName::StrDt),
            "STRLANG" => Some(FunctionName::StrLang),
            "ABS" => Some(FunctionName::Abs),
            "ROUND" => Some(FunctionName::Round),
            "CEIL" => Some(FunctionName::Ceil),
            "FLOOR" => Some(FunctionName::Floor),
            "RAND" => Some(FunctionName::Rand),
            "NOW" => Some(FunctionName::Now),
            "YEAR" => Some(FunctionName::Year),
            "MONTH" => Some(FunctionName::Month),
            "DAY" => Some(FunctionName::Day),
            "HOURS" => Some(FunctionName::Hours),
            "MINUTES" => Some(FunctionName::Minutes),
            "SECONDS" => Some(FunctionName::Seconds),
            "TIMEZONE" => Some(FunctionName::Timezone),
            "TZ" => Some(FunctionName::Tz),
            "MD5" => Some(FunctionName::Md5),
            "SHA1" => Some(FunctionName::Sha1),
            "SHA256" => Some(FunctionName::Sha256),
            "SHA384" => Some(FunctionName::Sha384),
            "SHA512" => Some(FunctionName::Sha512),
            "SAMETERM" => Some(FunctionName::SameTerm),
            "UUID" => Some(FunctionName::Uuid),
            "STRUUID" => Some(FunctionName::StrUuid),
            "IF" => Some(FunctionName::If),
            "COALESCE" => Some(FunctionName::Coalesce),
            // Vector similarity functions (case-insensitive, with underscore variants)
            "DOTPRODUCT" | "DOT_PRODUCT" => Some(FunctionName::DotProduct),
            "COSINESIMILARITY" | "COSINE_SIMILARITY" => Some(FunctionName::CosineSimilarity),
            "EUCLIDEANDISTANCE" | "EUCLIDEAN_DISTANCE" | "EUCLIDIANDISTANCE" => {
                Some(FunctionName::EuclideanDistance)
            }
            // SPARQL 1.2 triple-term functions (case-insensitive, matching the
            // grammar's BuiltInCall keywords). `isTRIPLE` uppercases to
            // `ISTRIPLE`.
            "TRIPLE" => Some(FunctionName::Triple),
            "SUBJECT" => Some(FunctionName::Subject),
            "PREDICATE" => Some(FunctionName::Predicate),
            "OBJECT" => Some(FunctionName::Object),
            "ISTRIPLE" => Some(FunctionName::IsTriple),
            _ => None,
        }
    }
}

/// Aggregate functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
    Sample,
}

impl AggregateFunction {
    /// Parse an aggregate function name.
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            "GROUP_CONCAT" => Some(AggregateFunction::GroupConcat),
            "SAMPLE" => Some(AggregateFunction::Sample),
            _ => None,
        }
    }

    /// Get the function name as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
            AggregateFunction::GroupConcat => "GROUP_CONCAT",
            AggregateFunction::Sample => "SAMPLE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::term::Literal;

    fn test_span() -> SourceSpan {
        SourceSpan::new(0, 10)
    }

    #[test]
    fn test_binary_op_precedence() {
        // Multiplication binds tighter than addition
        assert!(BinaryOp::Mul.precedence() > BinaryOp::Add.precedence());
        // Addition binds tighter than comparison
        assert!(BinaryOp::Add.precedence() > BinaryOp::Eq.precedence());
        // Comparison binds tighter than AND
        assert!(BinaryOp::Eq.precedence() > BinaryOp::And.precedence());
        // AND binds tighter than OR
        assert!(BinaryOp::And.precedence() > BinaryOp::Or.precedence());
    }

    #[test]
    fn test_expression_span() {
        let var = Expression::var(Var::new("x", test_span()));
        assert_eq!(var.span(), test_span());

        let lit = Expression::literal(Literal::integer(42, test_span()));
        assert_eq!(lit.span(), test_span());
    }

    #[test]
    fn test_binary_expression() {
        let left = Expression::literal(Literal::integer(1, SourceSpan::new(0, 1)));
        let right = Expression::literal(Literal::integer(2, SourceSpan::new(4, 5)));
        let expr = Expression::binary(BinaryOp::Add, left, right, SourceSpan::new(0, 5));

        match expr {
            Expression::Binary { op, .. } => {
                assert_eq!(op, BinaryOp::Add);
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_function_name_parsing() {
        // Case-insensitive
        assert_eq!(FunctionName::parse("BOUND"), Some(FunctionName::Bound));
        assert_eq!(FunctionName::parse("bound"), Some(FunctionName::Bound));
        assert_eq!(FunctionName::parse("Bound"), Some(FunctionName::Bound));

        // Various functions
        assert_eq!(FunctionName::parse("STR"), Some(FunctionName::Str));
        assert_eq!(FunctionName::parse("STRLEN"), Some(FunctionName::Strlen));
        assert_eq!(FunctionName::parse("REGEX"), Some(FunctionName::Regex));

        // Unknown returns None
        assert_eq!(FunctionName::parse("UNKNOWN"), None);
    }

    fn var_expr(name: &str) -> Expression {
        Expression::var(Var::new(name, test_span()))
    }

    fn count_of(expr: Expression) -> Expression {
        Expression::Aggregate {
            function: AggregateFunction::Count,
            expr: Some(Box::new(expr)),
            distinct: false,
            separator: None,
            span: test_span(),
        }
    }

    #[test]
    fn test_contains_aggregate() {
        // Plain variable: no aggregate
        assert!(!var_expr("x").contains_aggregate());

        // Aggregate directly
        assert!(count_of(var_expr("x")).contains_aggregate());

        // Aggregate nested under arithmetic: (1 + COUNT(?x))
        let sum = Expression::binary(
            BinaryOp::Add,
            Expression::literal(Literal::integer(1, test_span())),
            count_of(var_expr("x")),
            test_span(),
        );
        assert!(sum.contains_aggregate());

        // Aggregate nested inside a function call argument
        let call = Expression::function_call(
            FunctionName::Str,
            vec![count_of(var_expr("x"))],
            test_span(),
        );
        assert!(call.contains_aggregate());

        // COUNT(*) (no argument) is still an aggregate
        let count_star = Expression::Aggregate {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            separator: None,
            span: test_span(),
        };
        assert!(count_star.contains_aggregate());
    }

    #[test]
    fn test_variables_collects_all_references() {
        // (?a + COUNT(?b)) — both ?a and ?b are referenced
        let expr = Expression::binary(
            BinaryOp::Add,
            var_expr("a"),
            count_of(var_expr("b")),
            test_span(),
        );
        let names: Vec<_> = expr.variables().iter().map(|v| v.name.as_ref()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_unaggregated_variables_skips_aggregate_args() {
        // (?a + COUNT(?b)) — only ?a is unaggregated
        let expr = Expression::binary(
            BinaryOp::Add,
            var_expr("a"),
            count_of(var_expr("b")),
            test_span(),
        );
        let names: Vec<_> = expr
            .unaggregated_variables()
            .iter()
            .map(|v| v.name.as_ref())
            .collect();
        assert_eq!(names, vec!["a"]);

        // Fully aggregated: COUNT(?b) has no unaggregated variables
        assert!(count_of(var_expr("b")).unaggregated_variables().is_empty());

        // Bracketed wrapper is transparent: ((?a)) -> [?a]
        let bracketed = Expression::Bracketed {
            inner: Box::new(Expression::Bracketed {
                inner: Box::new(var_expr("a")),
                span: test_span(),
            }),
            span: test_span(),
        };
        let names: Vec<_> = bracketed
            .unaggregated_variables()
            .iter()
            .map(|v| v.name.as_ref())
            .collect();
        assert_eq!(names, vec!["a"]);
    }

    #[test]
    fn test_walk_detects_nested_aggregate() {
        // COUNT(COUNT(?x)) — walking the outer aggregate's argument finds
        // the inner aggregate (the SPARQL 1.2 nested-aggregate error case).
        let nested = count_of(count_of(var_expr("x")));
        let mut inner_aggregate_in_arg = false;
        nested.walk(&mut |e| {
            if let Expression::Aggregate {
                expr: Some(inner), ..
            } = e
            {
                if inner.contains_aggregate() {
                    inner_aggregate_in_arg = true;
                }
            }
        });
        assert!(inner_aggregate_in_arg);

        // Non-nested aggregate does not trip the detector
        let flat = count_of(var_expr("x"));
        let mut found = false;
        flat.walk(&mut |e| {
            if let Expression::Aggregate {
                expr: Some(inner), ..
            } = e
            {
                if inner.contains_aggregate() {
                    found = true;
                }
            }
        });
        assert!(!found);
    }

    #[test]
    fn test_aggregate_function_parsing() {
        assert_eq!(
            AggregateFunction::parse("COUNT"),
            Some(AggregateFunction::Count)
        );
        assert_eq!(
            AggregateFunction::parse("count"),
            Some(AggregateFunction::Count)
        );
        assert_eq!(
            AggregateFunction::parse("GROUP_CONCAT"),
            Some(AggregateFunction::GroupConcat)
        );
        assert_eq!(AggregateFunction::parse("UNKNOWN"), None);
    }
}
