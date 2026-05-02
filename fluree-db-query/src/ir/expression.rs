//! Filter / bind expressions: the AST that's evaluated against solutions
//! at FILTER, BIND, and HAVING positions, plus the comparison and
//! arithmetic operators and the built-in function catalog.

use super::pattern::Pattern;
use crate::var_registry::VarId;

/// Filter expression AST
///
/// Represents expressions that can be evaluated against solution bindings.
/// All operations are represented as function calls for uniform dispatch.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Variable reference
    Var(VarId),
    /// Constant value
    Const(FilterValue),
    /// Function call (includes operators like +, -, =, AND, OR, etc.)
    Call {
        func: Function,
        args: Vec<Expression>,
    },
    /// EXISTS / NOT EXISTS subquery inside a compound filter expression.
    ///
    /// Used when EXISTS/NOT EXISTS appears as part of a larger expression
    /// (e.g., `FILTER(?x = ?y || NOT EXISTS { ... })`). Standalone
    /// `FILTER EXISTS { ... }` is handled at the pattern level instead.
    ///
    /// Evaluated asynchronously by the FilterOperator before the main
    /// expression: the result is pre-computed per row and substituted
    /// as a boolean constant.
    Exists {
        patterns: Vec<Pattern>,
        negated: bool,
    },
}

impl Expression {
    /// True if this expression (or any sub-expression / sub-pattern it
    /// contains) calls the given built-in function.
    ///
    /// Used by the query-context setup code as a perf guardrail: queries
    /// that don't call `fulltext(...)` skip building the per-graph fulltext
    /// arena map.
    pub fn contains_function(&self, target: &Function) -> bool {
        match self {
            Expression::Var(_) | Expression::Const(_) => false,
            Expression::Call { func, args } => {
                func == target || args.iter().any(|a| a.contains_function(target))
            }
            Expression::Exists { patterns, .. } => {
                patterns.iter().any(|p| p.contains_function(target))
            }
        }
    }
}

// Manual PartialEq: Pattern doesn't implement PartialEq, so we can't derive.
// EXISTS subqueries are evaluated at runtime, never structurally compared.
impl PartialEq for Expression {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Expression::Var(a), Expression::Var(b)) => a == b,
            (Expression::Const(a), Expression::Const(b)) => a == b,
            (Expression::Call { func: f1, args: a1 }, Expression::Call { func: f2, args: a2 }) => {
                f1 == f2 && a1 == a2
            }
            (Expression::Exists { .. }, Expression::Exists { .. }) => false,
            _ => false,
        }
    }
}

impl Expression {
    // =========================================================================
    // Constructors for common expression types
    // =========================================================================

    /// Create a comparison expression
    pub fn compare(op: impl Into<Function>, left: Expression, right: Expression) -> Self {
        Expression::Call {
            func: op.into(),
            args: vec![left, right],
        }
    }

    /// Create an equality comparison
    pub fn eq(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Eq, left, right)
    }

    /// Create a not-equal comparison
    pub fn ne(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Ne, left, right)
    }

    /// Create a less-than comparison
    pub fn lt(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Lt, left, right)
    }

    /// Create a less-than-or-equal comparison
    pub fn le(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Le, left, right)
    }

    /// Create a greater-than comparison
    pub fn gt(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Gt, left, right)
    }

    /// Create a greater-than-or-equal comparison
    pub fn ge(left: Expression, right: Expression) -> Self {
        Self::compare(Function::Ge, left, right)
    }

    /// Create an arithmetic expression
    pub fn arithmetic(op: impl Into<Function>, left: Expression, right: Expression) -> Self {
        Expression::Call {
            func: op.into(),
            args: vec![left, right],
        }
    }

    /// Create an addition expression
    #[allow(clippy::should_implement_trait)]
    pub fn add(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Add, left, right)
    }

    /// Create a subtraction expression
    #[allow(clippy::should_implement_trait)]
    pub fn sub(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Sub, left, right)
    }

    /// Create a multiplication expression
    #[allow(clippy::should_implement_trait)]
    pub fn mul(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Mul, left, right)
    }

    /// Create a division expression
    #[allow(clippy::should_implement_trait)]
    pub fn div(left: Expression, right: Expression) -> Self {
        Self::arithmetic(Function::Div, left, right)
    }

    /// Create a unary negation expression
    pub fn negate(expr: Expression) -> Self {
        Expression::Call {
            func: Function::Negate,
            args: vec![expr],
        }
    }

    /// Create a logical AND expression
    pub fn and(exprs: Vec<Expression>) -> Self {
        Expression::Call {
            func: Function::And,
            args: exprs,
        }
    }

    /// Create a logical OR expression
    pub fn or(exprs: Vec<Expression>) -> Self {
        Expression::Call {
            func: Function::Or,
            args: exprs,
        }
    }

    /// Create a logical NOT expression
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Expression) -> Self {
        Expression::Call {
            func: Function::Not,
            args: vec![expr],
        }
    }

    /// Create an IF expression
    pub fn if_then_else(
        condition: Expression,
        then_expr: Expression,
        else_expr: Expression,
    ) -> Self {
        Expression::Call {
            func: Function::If,
            args: vec![condition, then_expr, else_expr],
        }
    }

    /// Create an IN expression
    pub fn in_list(expr: Expression, values: Vec<Expression>) -> Self {
        let mut args = vec![expr];
        args.extend(values);
        Expression::Call {
            func: Function::In,
            args,
        }
    }

    /// Create a NOT IN expression
    pub fn not_in_list(expr: Expression, values: Vec<Expression>) -> Self {
        let mut args = vec![expr];
        args.extend(values);
        Expression::Call {
            func: Function::NotIn,
            args,
        }
    }

    /// Create a function call expression
    pub fn call(func: Function, args: Vec<Expression>) -> Self {
        Expression::Call { func, args }
    }

    // =========================================================================
    // Query methods
    // =========================================================================

    /// Variables this expression references when evaluated. Expressions
    /// produce values, never bindings, so there is no `produced_vars`
    /// counterpart.
    pub fn referenced_vars(&self) -> Vec<VarId> {
        match self {
            Expression::Var(v) => vec![*v],
            Expression::Const(_) => Vec::new(),
            Expression::Call { args, .. } => {
                args.iter().flat_map(Expression::referenced_vars).collect()
            }
            Expression::Exists { patterns, .. } => {
                patterns.iter().flat_map(Pattern::referenced_vars).collect()
            }
        }
    }


    /// Returns Some(var) if filter references exactly one variable
    ///
    /// Used to determine if a filter can be attached inline to a pattern.
    pub fn single_var(&self) -> Option<VarId> {
        let vars = self.referenced_vars();
        let unique: std::collections::HashSet<_> = vars.into_iter().collect();
        if unique.len() == 1 {
            unique.into_iter().next()
        } else {
            None
        }
    }

    /// Returns true if this filter can be pushed down to index scans as range bounds.
    ///
    /// "Range-safe" filters can be converted to contiguous range constraints on the
    /// object position of index scans, enabling early filtering at the storage layer
    /// rather than post-scan filtering in the operator pipeline.
    ///
    /// # Accepted patterns
    ///
    /// - **Simple comparisons** (`<`, `<=`, `>`, `>=`, `=`) between a variable and constant
    /// - **Conjunctions** (`AND`) of range-safe expressions
    ///
    /// # Rejected patterns (NOT range-safe)
    ///
    /// - `!=` (not-equal) - cannot be represented as a contiguous range
    /// - `OR` - would require multiple disjoint ranges
    /// - `NOT` - negation cannot be efficiently bounded
    /// - Arithmetic expressions - require evaluation, not just bounds
    /// - Function calls - require runtime evaluation
    /// - `IN` clauses - multiple discrete values, not a range
    /// - Variable-to-variable comparisons - no constant bound available
    ///
    /// # Usage
    ///
    /// Filters that are range-safe are extracted during query planning and converted
    /// to `ObjectBounds` for index scans. Non-range-safe filters are applied as
    /// `FilterOperator` nodes after the scan completes.
    ///
    /// # Example
    ///
    /// ```text
    /// FILTER(?age > 18 AND ?age < 65)  -> range-safe (becomes scan bounds)
    /// FILTER(?age != 30)               -> NOT range-safe (post-scan filter)
    /// FILTER(?x > ?y)                  -> NOT range-safe (no constant bound)
    /// (< 10 ?x 20)                     -> range-safe (sandwich: const var const)
    /// (< ?x ?y 20)                     -> NOT range-safe (non-sandwich variadic)
    /// ```
    pub fn is_range_safe(&self) -> bool {
        match self {
            Expression::Call { func, args } => match func {
                // Comparison operators (except Ne) are range-safe if var vs const
                Function::Eq | Function::Lt | Function::Le | Function::Gt | Function::Ge => {
                    // 2-arg: var vs const (either order)
                    (args.len() == 2
                        && matches!(
                            (&args[0], &args[1]),
                            (Expression::Var(_), Expression::Const(_))
                                | (Expression::Const(_), Expression::Var(_))
                        ))
                    // 3-arg sandwich: const var const
                    || (args.len() == 3
                        && matches!(
                            (&args[0], &args[1], &args[2]),
                            (Expression::Const(_), Expression::Var(_), Expression::Const(_))
                        ))
                }
                // AND of range-safe expressions is range-safe
                Function::And => args.iter().all(Expression::is_range_safe),
                // Everything else is NOT range-safe
                _ => false,
            },
            // Var, Const, Exists are not range-safe on their own
            Expression::Var(_) | Expression::Const(_) | Expression::Exists { .. } => false,
        }
    }

    /// Check if this is a comparison expression
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Expression::Call {
                func: Function::Eq
                    | Function::Ne
                    | Function::Lt
                    | Function::Le
                    | Function::Gt
                    | Function::Ge,
                ..
            }
        )
    }

    /// Get the comparison function if this is a comparison expression
    pub fn as_comparison(&self) -> Option<(&Function, &[Expression])> {
        match self {
            Expression::Call { func, args }
                if matches!(
                    func,
                    Function::Eq
                        | Function::Ne
                        | Function::Lt
                        | Function::Le
                        | Function::Gt
                        | Function::Ge
                ) =>
            {
                Some((func, args))
            }
            _ => None,
        }
    }
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CompareOp {
    /// Return the operator symbol as a static string.
    pub fn symbol(self) -> &'static str {
        match self {
            CompareOp::Eq => "=",
            CompareOp::Ne => "!=",
            CompareOp::Lt => "<",
            CompareOp::Le => "<=",
            CompareOp::Gt => ">",
            CompareOp::Ge => ">=",
        }
    }
}

impl std::fmt::Display for CompareOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.symbol())
    }
}

/// Arithmetic operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl std::fmt::Display for ArithmeticOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithmeticOp::Add => write!(f, "+"),
            ArithmeticOp::Sub => write!(f, "-"),
            ArithmeticOp::Mul => write!(f, "*"),
            ArithmeticOp::Div => write!(f, "/"),
        }
    }
}

/// Constant value in filter expressions
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Long(i64),
    Double(f64),
    String(String),
    Bool(bool),
    /// Temporal or duration value (wraps any temporal/duration FlakeValue)
    Temporal(fluree_db_core::value::FlakeValue),
}

// =============================================================================
// From implementations for lowering unresolved AST types
// =============================================================================

impl From<CompareOp> for Function {
    fn from(op: CompareOp) -> Self {
        match op {
            CompareOp::Eq => Function::Eq,
            CompareOp::Ne => Function::Ne,
            CompareOp::Lt => Function::Lt,
            CompareOp::Le => Function::Le,
            CompareOp::Gt => Function::Gt,
            CompareOp::Ge => Function::Ge,
        }
    }
}

impl From<ArithmeticOp> for Function {
    fn from(op: ArithmeticOp) -> Self {
        match op {
            ArithmeticOp::Add => Function::Add,
            ArithmeticOp::Sub => Function::Sub,
            ArithmeticOp::Mul => Function::Mul,
            ArithmeticOp::Div => Function::Div,
        }
    }
}

impl From<&crate::parse::ast::UnresolvedFilterValue> for FilterValue {
    fn from(val: &crate::parse::ast::UnresolvedFilterValue) -> Self {
        use crate::parse::ast::UnresolvedFilterValue;
        match val {
            UnresolvedFilterValue::Long(l) => FilterValue::Long(*l),
            UnresolvedFilterValue::Double(d) => FilterValue::Double(*d),
            UnresolvedFilterValue::String(s) => FilterValue::String(s.to_string()),
            UnresolvedFilterValue::Bool(b) => FilterValue::Bool(*b),
        }
    }
}

/// Built-in functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Function {
    // =========================================================================
    // Comparison operators
    // =========================================================================
    /// Equality (=)
    Eq,
    /// Not equal (!=)
    Ne,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    Le,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Ge,

    // =========================================================================
    // Arithmetic operators
    // =========================================================================
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Unary negation (-)
    Negate,

    // =========================================================================
    // Logical operators
    // =========================================================================
    /// Logical AND
    And,
    /// Logical OR
    Or,
    /// Logical NOT
    Not,
    /// IN expression (?x IN (1, 2, 3))
    In,
    /// NOT IN expression (?x NOT IN (1, 2, 3))
    NotIn,

    // =========================================================================
    // String functions
    // =========================================================================
    Strlen,
    Substr,
    Ucase,
    Lcase,
    Contains,
    StrStarts,
    StrEnds,
    Regex,
    Concat,
    StrBefore,
    StrAfter,
    Replace,
    Str,
    StrDt,
    StrLang,
    EncodeForUri,

    // =========================================================================
    // Numeric functions
    // =========================================================================
    Abs,
    Round,
    Ceil,
    Floor,
    Rand,

    // =========================================================================
    // RDF term constructors
    // =========================================================================
    Iri,
    Bnode,

    // =========================================================================
    // DateTime functions
    // =========================================================================
    Now,
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
    Tz,
    Timezone,

    // =========================================================================
    // Type functions
    // =========================================================================
    IsIri,
    IsBlank,
    IsLiteral,
    IsNumeric,

    // =========================================================================
    // RDF term functions
    // =========================================================================
    Lang,
    Datatype,
    LangMatches,
    SameTerm,

    // =========================================================================
    // Fluree-specific functions
    // =========================================================================
    /// Transaction time of the matching flake (i64).
    T,
    /// Operation type of the matching flake in history queries — boolean
    /// (`true` = assert, `false` = retract). Mirrors `Flake.op` on disk;
    /// returns `None` for current-state scans.
    Op,

    // =========================================================================
    // Hash functions
    // =========================================================================
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,

    // =========================================================================
    // UUID functions
    // =========================================================================
    Uuid,
    StrUuid,

    // =========================================================================
    // Vector/embedding similarity functions
    // =========================================================================
    DotProduct,
    CosineSimilarity,
    EuclideanDistance,

    // =========================================================================
    // Geospatial functions
    // =========================================================================
    GeofDistance,

    // =========================================================================
    // Fulltext scoring functions
    // =========================================================================
    Fulltext,

    // =========================================================================
    // Conditional functions
    // =========================================================================
    Bound,
    If,
    Coalesce,

    // =========================================================================
    // XSD datatype constructor (cast) functions — W3C SPARQL 1.1 §17.5
    // SPARQL-only: JSON-LD queries do not produce these (casts are a SPARQL concept).
    // =========================================================================
    XsdBoolean,
    XsdInteger,
    XsdFloat,
    XsdDouble,
    XsdDecimal,
    XsdString,

    // =========================================================================
    // Custom/unknown function
    // =========================================================================
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_expr_single_var() {
        // Single var: ?x > 10
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert_eq!(expr.single_var(), Some(VarId(0)));

        // Two vars: ?x > ?y
        let expr2 = Expression::gt(Expression::Var(VarId(0)), Expression::Var(VarId(1)));
        assert_eq!(expr2.single_var(), None);
    }

    #[test]
    fn test_filter_expr_is_range_safe() {
        // Range-safe: ?x > 10
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // Range-safe: AND of range-safe
        let and_expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FilterValue::Long(65)),
            ),
        ]);
        assert!(and_expr.is_range_safe());

        // Not range-safe: OR
        let or_expr = Expression::or(vec![Expression::eq(
            Expression::Var(VarId(0)),
            Expression::Const(FilterValue::Long(1)),
        )]);
        assert!(!or_expr.is_range_safe());
    }
}
