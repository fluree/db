//! Filter / bind expressions: the AST that's evaluated against solutions
//! at FILTER, BIND, and HAVING positions, plus the comparison and
//! arithmetic operators and the built-in function catalog.

use super::pattern::Pattern;
use crate::var_registry::VarId;
use fluree_db_core::value::FlakeValue;

/// Which quantifier a [`Expression::ListPredicate`] applies over a list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPredicateKind {
    /// `all` — true iff the predicate holds for every element.
    All,
    /// `any` — true iff the predicate holds for at least one element.
    Any,
    /// `none` — true iff the predicate holds for no element.
    None,
    /// `single` — true iff the predicate holds for exactly one element.
    Single,
}

/// Filter expression AST
///
/// Represents expressions that can be evaluated against solution bindings.
/// All operations are represented as function calls for uniform dispatch.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Variable reference
    Var(VarId),
    /// Constant value
    Const(FlakeValue),
    /// Function call (includes operators like +, -, =, AND, OR, etc.)
    Call {
        func: Function,
        args: Vec<Expression>,
    },
    /// Map literal `{k: v, …}` — builds an ordered map value from its entries.
    /// Keys are static (resolved at lowering); values are sub-expressions
    /// evaluated per row. Insertion order is preserved for display; identity
    /// (equality / grouping) is key-order-insensitive. Duplicate keys resolve
    /// last-wins at construction. Produces a [`crate::binding::Binding::Map`].
    Map(Vec<(std::sync::Arc<str>, Expression)>),

    /// List comprehension `[var IN list WHERE filter | map]`. Iterates `list`,
    /// binding `var` to each element (a scoped local, excluded from
    /// `referenced_vars`); keeps elements passing `filter` (if any) and projects
    /// `map` (identity if absent). Produces a [`crate::binding::Binding::List`].
    ListComprehension {
        var: VarId,
        list: Box<Expression>,
        filter: Option<Box<Expression>>,
        map: Option<Box<Expression>>,
    },
    /// `reduce(acc = init, var IN list | body)`. Folds `list` left-to-right:
    /// `acc` starts at `init`, and each step re-binds `acc` and `var` (both
    /// scoped locals) and evaluates `body` to the next accumulator.
    Reduce {
        acc: VarId,
        init: Box<Expression>,
        var: VarId,
        list: Box<Expression>,
        body: Box<Expression>,
    },
    /// List predicate `all/any/none/single(var IN list WHERE pred)` — tests
    /// `pred` (with `var` a scoped local) across the elements of `list`,
    /// short-circuiting. Produces a boolean.
    ListPredicate {
        kind: ListPredicateKind,
        var: VarId,
        list: Box<Expression>,
        predicate: Box<Expression>,
    },
    /// Eval-time member access `target.key` — used when `target` can't be a
    /// graph-join variable (a loop-local from a comprehension/reduce). At eval:
    /// a [`crate::binding::Binding::Map`] target looks up `key`; a node (ref)
    /// target scans `(node, predicate_iri, ?)` for the data property; anything
    /// else is null. `predicate_iri` is resolved at lowering (it needs the
    /// Cypher vocab, absent from the engine). Outer query-variable property
    /// access still lowers to the efficient auxiliary-pattern join instead.
    Member {
        target: Box<Expression>,
        key: std::sync::Arc<str>,
        predicate_iri: std::sync::Arc<str>,
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
    /// Pattern comprehension `[(a)-[:T]->(b) WHERE pred | proj]`: a correlated
    /// subquery that projects `proj` over each match of `patterns` (the inner
    /// `WHERE` is folded into `patterns` as a `Filter`) and collects the results
    /// into a list. Like [`Expression::Exists`] it is resolved **asynchronously**
    /// per outer row (seeded with that row's bindings) and replaced with a
    /// [`Expression::Resolved`] list — it never reaches the synchronous evaluator.
    PatternComprehension {
        patterns: Vec<Pattern>,
        projection: Box<Expression>,
    },
    /// A value pre-computed by async resolution (the list result of a
    /// [`Expression::PatternComprehension`]). The synchronous evaluator returns
    /// it directly. Never produced by lowering — only substituted in at runtime.
    Resolved(Box<crate::binding::Binding>),
}

impl Expression {
    /// Rename every occurrence of variable `old` to `new` (recursively through
    /// call arguments and EXISTS sub-patterns). Used by the equijoin-filter fold.
    pub fn substitute_var(&mut self, old: VarId, new: VarId) {
        match self {
            Expression::Var(v) => {
                if *v == old {
                    *v = new;
                }
            }
            Expression::Const(_) => {}
            Expression::Call { args, .. } => {
                for arg in args {
                    arg.substitute_var(old, new);
                }
            }
            Expression::Map(entries) => {
                for (_, v) in entries {
                    v.substitute_var(old, new);
                }
            }
            // Scoped iteration: always rename in the list/init (outer scope), but
            // not inside the body when the bound (loop/acc) variable shadows.
            Expression::ListComprehension {
                var,
                list,
                filter,
                map,
            } => {
                list.substitute_var(old, new);
                if *var != old {
                    if let Some(f) = filter {
                        f.substitute_var(old, new);
                    }
                    if let Some(m) = map {
                        m.substitute_var(old, new);
                    }
                }
            }
            Expression::Reduce {
                acc,
                init,
                var,
                list,
                body,
            } => {
                init.substitute_var(old, new);
                list.substitute_var(old, new);
                if *acc != old && *var != old {
                    body.substitute_var(old, new);
                }
            }
            Expression::ListPredicate {
                var,
                list,
                predicate,
                ..
            } => {
                list.substitute_var(old, new);
                if *var != old {
                    predicate.substitute_var(old, new);
                }
            }
            Expression::Member { target, .. } => target.substitute_var(old, new),
            Expression::Exists { patterns, .. } => {
                for p in patterns {
                    p.substitute_var(old, new);
                }
            }
            Expression::PatternComprehension {
                patterns,
                projection,
            } => {
                for p in patterns {
                    p.substitute_var(old, new);
                }
                projection.substitute_var(old, new);
            }
            Expression::Resolved(_) => {}
        }
    }

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
            Expression::Map(entries) => entries.iter().any(|(_, v)| v.contains_function(target)),
            Expression::ListComprehension {
                list, filter, map, ..
            } => {
                list.contains_function(target)
                    || filter.as_ref().is_some_and(|f| f.contains_function(target))
                    || map.as_ref().is_some_and(|m| m.contains_function(target))
            }
            Expression::Reduce {
                init, list, body, ..
            } => {
                init.contains_function(target)
                    || list.contains_function(target)
                    || body.contains_function(target)
            }
            Expression::ListPredicate {
                list, predicate, ..
            } => list.contains_function(target) || predicate.contains_function(target),
            Expression::Member { target: t, .. } => t.contains_function(target),
            Expression::Exists { patterns, .. } => {
                patterns.iter().any(|p| p.contains_function(target))
            }
            Expression::PatternComprehension {
                patterns,
                projection,
            } => {
                patterns.iter().any(|p| p.contains_function(target))
                    || projection.contains_function(target)
            }
            Expression::Resolved(_) => false,
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
            (Expression::Map(a), Expression::Map(b)) => a == b,
            (
                Expression::ListComprehension {
                    var: v1,
                    list: l1,
                    filter: f1,
                    map: m1,
                },
                Expression::ListComprehension {
                    var: v2,
                    list: l2,
                    filter: f2,
                    map: m2,
                },
            ) => v1 == v2 && l1 == l2 && f1 == f2 && m1 == m2,
            (
                Expression::Reduce {
                    acc: a1,
                    init: i1,
                    var: v1,
                    list: l1,
                    body: b1,
                },
                Expression::Reduce {
                    acc: a2,
                    init: i2,
                    var: v2,
                    list: l2,
                    body: b2,
                },
            ) => a1 == a2 && i1 == i2 && v1 == v2 && l1 == l2 && b1 == b2,
            (
                Expression::ListPredicate {
                    kind: k1,
                    var: v1,
                    list: l1,
                    predicate: p1,
                },
                Expression::ListPredicate {
                    kind: k2,
                    var: v2,
                    list: l2,
                    predicate: p2,
                },
            ) => k1 == k2 && v1 == v2 && l1 == l2 && p1 == p2,
            (
                Expression::Member {
                    target: t1,
                    key: k1,
                    predicate_iri: p1,
                },
                Expression::Member {
                    target: t2,
                    key: k2,
                    predicate_iri: p2,
                },
            ) => t1 == t2 && k1 == k2 && p1 == p2,
            (Expression::Exists { .. }, Expression::Exists { .. }) => false,
            // Patterns aren't comparable (mirrors Exists); a resolved value is.
            (Expression::PatternComprehension { .. }, Expression::PatternComprehension { .. }) => {
                false
            }
            (Expression::Resolved(a), Expression::Resolved(b)) => a == b,
            _ => false,
        }
    }
}

impl Expression {
    // =========================================================================
    // Constructors for common expression types
    // =========================================================================

    /// Create a binary call expression: `func(left, right)`.
    pub fn binary(func: impl Into<Function>, left: Expression, right: Expression) -> Self {
        Self::call(func.into(), vec![left, right])
    }

    /// Create an equality comparison
    pub fn eq(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Eq, left, right)
    }

    /// Create a not-equal comparison
    pub fn ne(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Ne, left, right)
    }

    /// Create a less-than comparison
    pub fn lt(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Lt, left, right)
    }

    /// Create a less-than-or-equal comparison
    pub fn le(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Le, left, right)
    }

    /// Create a greater-than comparison
    pub fn gt(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Gt, left, right)
    }

    /// Create a greater-than-or-equal comparison
    pub fn ge(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Ge, left, right)
    }

    /// Create an addition expression
    #[allow(clippy::should_implement_trait)]
    pub fn add(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Add, left, right)
    }

    /// Create a subtraction expression
    #[allow(clippy::should_implement_trait)]
    pub fn sub(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Sub, left, right)
    }

    /// Create a multiplication expression
    #[allow(clippy::should_implement_trait)]
    pub fn mul(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Mul, left, right)
    }

    /// Create a division expression
    #[allow(clippy::should_implement_trait)]
    pub fn div(left: Expression, right: Expression) -> Self {
        Self::binary(Function::Div, left, right)
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
            Expression::Map(entries) => entries
                .iter()
                .flat_map(|(_, v)| v.referenced_vars())
                .collect(),
            // The loop/acc variables are bound internally — exclude them, but
            // keep the free vars referenced by the list and the scoped bodies.
            Expression::ListComprehension {
                var,
                list,
                filter,
                map,
            } => {
                let mut vars = list.referenced_vars();
                let mut inner = Vec::new();
                if let Some(f) = filter {
                    inner.extend(f.referenced_vars());
                }
                if let Some(m) = map {
                    inner.extend(m.referenced_vars());
                }
                inner.retain(|x| x != var);
                vars.extend(inner);
                vars
            }
            Expression::Reduce {
                acc,
                init,
                var,
                list,
                body,
            } => {
                let mut vars = init.referenced_vars();
                vars.extend(list.referenced_vars());
                let mut inner = body.referenced_vars();
                inner.retain(|x| x != acc && x != var);
                vars.extend(inner);
                vars
            }
            Expression::ListPredicate {
                var,
                list,
                predicate,
                ..
            } => {
                let mut vars = list.referenced_vars();
                let mut inner = predicate.referenced_vars();
                inner.retain(|x| x != var);
                vars.extend(inner);
                vars
            }
            Expression::Member { target, .. } => target.referenced_vars(),
            // EXISTS has no projection — only the pattern's correlation vars.
            Expression::Exists { patterns, .. } => {
                patterns.iter().flat_map(Pattern::referenced_vars).collect()
            }
            // A pattern comprehension's projection can capture OUTER variables
            // that never appear in the inner pattern (e.g. `[(a)-->(b) | c]`).
            // Those are real dependencies — include them so dependency trimming
            // can't drop them. Pattern-internal vars (`b`) are already covered.
            Expression::PatternComprehension {
                patterns,
                projection,
            } => {
                let mut vars: Vec<VarId> =
                    patterns.iter().flat_map(Pattern::referenced_vars).collect();
                for v in projection.referenced_vars() {
                    if !vars.contains(&v) {
                        vars.push(v);
                    }
                }
                vars
            }
            Expression::Resolved(_) => Vec::new(),
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
            Expression::Var(_)
            | Expression::Const(_)
            | Expression::Map(_)
            | Expression::ListComprehension { .. }
            | Expression::Reduce { .. }
            | Expression::ListPredicate { .. }
            | Expression::Member { .. }
            | Expression::Exists { .. }
            | Expression::PatternComprehension { .. }
            | Expression::Resolved(_) => false,
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
    Mod,
}

impl std::fmt::Display for ArithmeticOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithmeticOp::Add => write!(f, "+"),
            ArithmeticOp::Sub => write!(f, "-"),
            ArithmeticOp::Mul => write!(f, "*"),
            ArithmeticOp::Div => write!(f, "/"),
            ArithmeticOp::Mod => write!(f, "%"),
        }
    }
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
            ArithmeticOp::Mod => Function::Mod,
        }
    }
}

impl From<&crate::parse::ast::UnresolvedFilterValue> for FlakeValue {
    fn from(val: &crate::parse::ast::UnresolvedFilterValue) -> Self {
        use crate::parse::ast::UnresolvedFilterValue;
        match val {
            UnresolvedFilterValue::Long(l) => FlakeValue::Long(*l),
            UnresolvedFilterValue::Double(d) => FlakeValue::Double(*d),
            UnresolvedFilterValue::String(s) => FlakeValue::String(s.to_string()),
            UnresolvedFilterValue::Bool(b) => FlakeValue::Boolean(*b),
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
    /// Modulus (%)
    Mod,
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
    /// Logical XOR (Cypher `XOR`). Two-valued: `bool(a) ^ bool(b)`, matching the
    /// truthiness semantics of the `(a OR b) AND NOT(a AND b)` form it replaces.
    /// Cypher-only; never produced by SPARQL/JSON-LD lowering.
    Xor,
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
    /// Cypher `replace(s, search, replacement)` — LITERAL replace-all (vs the
    /// regex [`Replace`]).
    ReplaceAll,
    /// Cypher `split(s, delim)` — split a string into a list (list-valued).
    Split,
    /// Cypher `trim` / `ltrim` / `rtrim` — strip leading/trailing whitespace.
    Trim,
    LTrim,
    RTrim,
    /// Cypher `left(s, n)` / `right(s, n)` — first / last `n` characters.
    Left,
    Right,

    // =========================================================================
    // Numeric functions
    // =========================================================================
    Abs,
    Round,
    Ceil,
    Floor,
    Rand,
    /// Cypher `sqrt(x)`.
    Sqrt,
    /// Cypher `sign(x)` — -1 / 0 / 1.
    Sign,
    /// Cypher `log(x)` — natural logarithm.
    Ln,
    /// Exponentiation (`x ^ y`).
    Pow,

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
    // Path functions (Cypher shortestPath result values)
    // =========================================================================
    /// `length(p)` — hop count of a path value (`nodes - 1`).
    PathLength,

    // =========================================================================
    // List functions (Cypher list values)
    // =========================================================================
    /// `size(list|string)` — element count of a list or length of a string.
    Size,
    /// `head(list)` — first element (null if empty).
    Head,
    /// `last(list)` — last element (null if empty).
    Last,
    /// `tail(list)` — the list without its first element.
    Tail,
    /// `reverse(list|string)` — reversed list or string.
    Reverse,
    /// List constructor `[a, b, …]` — builds a list value from its arguments.
    MakeList,
    /// `nodes(path)` — the list of node refs along a path value.
    Nodes,
    /// `range(start, end[, step])` — inclusive integer list.
    Range,
    /// `pathPairs(path)` — consecutive node pairs `[[a,b],[b,c],…]` along a path
    /// value, each pair a two-element list. Drives per-edge aggregation (IC14).
    PathPairs,
    /// `list[index]` — element access (0-based; negative indexes from the end).
    /// Out-of-range / non-integer index / non-list → unbound (Cypher null).
    ListIndex,

    // =========================================================================
    // Cypher metadata functions
    // =========================================================================
    /// `labels(node)` — Cypher label strings from `rdf:type` assertions.
    Labels,
    /// `type(rel)` — relationship type string from `f:reifiesPredicate`.
    RelType,
    /// `startNode(rel)` / `endNode(rel)` — the relationship's start / end node
    /// ref, from `f:reifiesSubject` / `f:reifiesObject` on the reifier.
    StartNode,
    EndNode,
    /// `relationships(path)` — the list of relationship values along a path
    /// (one per hop), built from the path's nodes and per-hop predicates.
    Relationships,
    /// Construct a relationship value: `MakeRel(start, Const(Ref(predicate)), end)`
    /// → [`crate::binding::Binding::Rel`] (reifier = None). Internal; emitted by
    /// the var-length relationship-variable binding.
    MakeRel,
    /// Construct a path value: `MakePath(Const(Ref(predicate)), node0, …, nodeN)`
    /// → [`crate::binding::Binding::Path`] with every hop using `predicate`.
    /// Internal; emitted by the var-length path-variable binding.
    MakePath,
    /// `keys(node)` — the list of a node's data-property keys (local names),
    /// excluding `rdf:type`, the `f:reifies*` bundle, and relationship (ref)
    /// edges. Produces a [`crate::binding::Binding::List`] of strings.
    Keys,
    /// `properties(node)` — a map of a node's data properties (`{key: value}`),
    /// using the same exclusions as [`Function::Keys`]. Produces a
    /// [`crate::binding::Binding::Map`].
    Properties,

    // =========================================================================
    // Custom/unknown function
    // =========================================================================
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_expr_is_range_safe() {
        // Range-safe: ?x > 10
        let expr = Expression::gt(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(10)),
        );
        assert!(expr.is_range_safe());

        // Range-safe: AND of range-safe
        let and_expr = Expression::and(vec![
            Expression::ge(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(65)),
            ),
        ]);
        assert!(and_expr.is_range_safe());

        // Not range-safe: OR
        let or_expr = Expression::or(vec![Expression::eq(
            Expression::Var(VarId(0)),
            Expression::Const(FlakeValue::Long(1)),
        )]);
        assert!(!or_expr.is_range_safe());
    }
}
