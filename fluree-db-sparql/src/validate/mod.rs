//! SPARQL Validation.
//!
//! This module validates parsed SPARQL AST against Fluree's capability model
//! and SPARQL semantic rules.
//!
//! ## Responsibilities
//!
//! - Fluree restrictions (property path depth, USING NAMED, etc.)
//! - Ground-only validation for INSERT DATA / DELETE DATA
//! - Variable scoping rules (future)
//!
//! ## Design
//!
//! Validation produces diagnostics without transforming the AST.
//! This enables standalone validation for IDE/LSP integration.
//!
//! ## Usage
//!
//! ```
//! use fluree_db_sparql::{parse_sparql, validate, Capabilities};
//!
//! let output = parse_sparql("SELECT ?x WHERE { ?x <http://example.org/p> ?y }");
//! if let Some(ast) = &output.ast {
//!     let diagnostics = validate(ast, &Capabilities::default());
//!     for d in &diagnostics {
//!         println!("{}: {}", d.code, d.message);
//!     }
//! }
//! ```

use crate::ast::expr::Expression;
use crate::ast::path::PropertyPath;
use crate::ast::pattern::{GraphPattern, TriplePattern};
use crate::ast::query::{
    AskQuery, ConstructQuery, DescribeQuery, GroupCondition, QueryBody, SelectQuery,
    SelectVariable, SelectVariables, SparqlAst,
};
use crate::ast::term::{PredicateTerm, SubjectTerm, Term, Var};
use crate::ast::update::{
    DeleteData, DeleteWhere, InsertData, Modify, QuadData, QuadPattern, QuadPatternElement,
    UpdateOperation,
};
use crate::diag::{DiagCode, Diagnostic, Label};
use crate::span::SourceSpan;
use std::collections::HashSet;
use std::sync::Arc;

/// Fluree capability configuration.
///
/// Controls which SPARQL features are allowed during validation.
/// By default, all Fluree-supported features are enabled.
#[derive(Clone, Debug)]
pub struct Capabilities {
    /// Allow property path operators (+, *, ?, /, |, ^)
    pub property_paths: bool,
    /// Allow MINUS operator (with partial semantics warning)
    pub minus_operator: bool,
    /// Allow USING clause in updates
    pub using_clause: bool,
    /// Strict W3C SPARQL §18.5 aggregate-scope validation.
    ///
    /// When `true`, rejects queries that project an ungrouped non-aggregated
    /// variable in a grouped query — this matches W3C negative-syntax tests
    /// `agg08`–`agg12`. When `false` (the default), Fluree's extension that
    /// auto-collects ungrouped projected variables into per-group lists
    /// remains available to existing customer queries that depend on it.
    /// The W3C compliance harness in `testsuite-sparql` flips this to `true`.
    pub strict_aggregate_scope: bool,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            property_paths: true,
            minus_operator: true,
            using_clause: true,
            strict_aggregate_scope: false,
        }
    }
}

impl Capabilities {
    /// Strict W3C SPARQL conformance: every spec-defined validation rule
    /// is enforced. Enables `strict_aggregate_scope`; everything else
    /// matches the default. Use this in the W3C testsuite harness and any
    /// other context that demands rejection of spec-noncompliant queries.
    pub fn w3c_strict() -> Self {
        Self {
            strict_aggregate_scope: true,
            ..Self::default()
        }
    }
}

/// Validate a SPARQL AST against capabilities and semantic rules.
///
/// Returns a list of diagnostics (errors and warnings).
/// An empty list indicates the query is valid.
pub fn validate(ast: &SparqlAst, caps: &Capabilities) -> Vec<Diagnostic> {
    let mut validator = Validator::new(caps);
    validator.validate_ast(ast);
    validator.diagnostics
}

/// Internal validator state.
struct Validator<'a> {
    caps: &'a Capabilities,
    diagnostics: Vec<Diagnostic>,
}

impl<'a> Validator<'a> {
    fn new(caps: &'a Capabilities) -> Self {
        Self {
            caps,
            diagnostics: Vec::new(),
        }
    }

    fn validate_ast(&mut self, ast: &SparqlAst) {
        match &ast.body {
            QueryBody::Select(query) => self.validate_select(query),
            QueryBody::Construct(query) => self.validate_construct(query),
            QueryBody::Ask(query) => self.validate_ask(query),
            QueryBody::Describe(query) => self.validate_describe(query),
            QueryBody::Update(op) => self.validate_update(op),
        }
    }

    fn validate_select(&mut self, query: &SelectQuery) {
        self.validate_graph_pattern(&query.where_clause.pattern);
        if self.caps.strict_aggregate_scope {
            self.validate_aggregate_scope(query);
        }
    }

    /// W3C SPARQL §18.5 aggregate scope rules.
    ///
    /// When a query is "grouped" — has an explicit `GROUP BY` clause OR has
    /// any aggregate in the SELECT — every variable referenced in a
    /// non-aggregate SELECT projection (or its alias-bearing expression)
    /// must be either:
    ///
    /// 1. A grouped variable (named directly in GROUP BY, or the alias of a
    ///    `GROUP BY (expr AS ?alias)` clause, or the bare variable in
    ///    `GROUP BY (?x)`).
    /// 2. An aggregate alias from a `SELECT (AGG(...) AS ?alias)` element.
    ///
    /// Variables that appear only inside an aggregate function (e.g. the
    /// `?x` in `COUNT(?x)`) are aggregate inputs — allowed regardless.
    ///
    /// W3C negative-syntax tests `agg08`–`agg12` exercise this rule.
    fn validate_aggregate_scope(&mut self, query: &SelectQuery) {
        let has_group_by = query.modifiers.group_by.is_some();
        let has_aggregates = match &query.select.variables {
            SelectVariables::Star => false,
            SelectVariables::Explicit(vars) => vars.iter().any(|v| {
                matches!(v, SelectVariable::Expr { expr, .. }
                    if expr_contains_aggregate(expr))
            }),
        };
        if !has_group_by && !has_aggregates {
            return;
        }

        // Build the set of in-scope (grouped or aggregate-alias) variable names.
        let mut in_scope: HashSet<Arc<str>> = HashSet::new();

        if let Some(ref gb) = query.modifiers.group_by {
            for cond in &gb.conditions {
                match cond {
                    GroupCondition::Var(v) => {
                        in_scope.insert(v.name.clone());
                    }
                    GroupCondition::Expr {
                        expr,
                        alias: Some(a),
                        ..
                    } => {
                        // GROUP BY (expr AS ?alias) — alias is grouped.
                        in_scope.insert(a.name.clone());
                        // The vars *inside* expr are NOT grouped (only the alias is).
                        let _ = expr; // suppress unused-binding warning if any
                    }
                    GroupCondition::Expr {
                        expr, alias: None, ..
                    } => {
                        // GROUP BY (?x) without alias unwraps to a plain
                        // grouped variable. GROUP BY (?x + ?y) without alias
                        // contributes no user-visible grouped name (the
                        // synthesized alias is implementation-only and
                        // cannot be referenced from SELECT).
                        if let Expression::Var(v) = expr.unwrap_bracketed() {
                            in_scope.insert(v.name.clone());
                        }
                    }
                }
            }
        }

        if let SelectVariables::Explicit(vars) = &query.select.variables {
            for sv in vars {
                if let SelectVariable::Expr {
                    expr: Expression::Aggregate { .. },
                    alias,
                    ..
                } = sv
                {
                    in_scope.insert(alias.name.clone());
                }
            }
        }

        // Walk each non-aggregate SELECT element and validate its variable
        // references. Aggregate elements are skipped — their inputs are
        // always allowed regardless of grouping.
        if let SelectVariables::Explicit(vars) = &query.select.variables {
            for sv in vars {
                match sv {
                    SelectVariable::Var(v) => {
                        if !in_scope.contains(&v.name) {
                            self.report_ungrouped_var(v);
                        }
                    }
                    SelectVariable::Expr { expr, .. } => {
                        if matches!(expr, Expression::Aggregate { .. }) {
                            continue;
                        }
                        self.check_expr_vars_in_scope(expr, &in_scope);
                    }
                }
            }
        }
    }

    /// Walk an expression tree and report any ungrouped variable reference.
    /// Recursion stops at `Expression::Aggregate` boundaries — aggregate
    /// inputs are always allowed.
    fn check_expr_vars_in_scope(&mut self, expr: &Expression, in_scope: &HashSet<Arc<str>>) {
        match expr {
            Expression::Var(v) => {
                if !in_scope.contains(&v.name) {
                    self.report_ungrouped_var(v);
                }
            }
            Expression::Aggregate { .. } => {
                // Inside an aggregate: input vars are aggregate operands, allowed.
            }
            Expression::Binary { left, right, .. } => {
                self.check_expr_vars_in_scope(left, in_scope);
                self.check_expr_vars_in_scope(right, in_scope);
            }
            Expression::Unary { operand, .. } => {
                self.check_expr_vars_in_scope(operand, in_scope);
            }
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    self.check_expr_vars_in_scope(arg, in_scope);
                }
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                self.check_expr_vars_in_scope(condition, in_scope);
                self.check_expr_vars_in_scope(then_expr, in_scope);
                self.check_expr_vars_in_scope(else_expr, in_scope);
            }
            Expression::Coalesce { args, .. } => {
                for arg in args {
                    self.check_expr_vars_in_scope(arg, in_scope);
                }
            }
            Expression::In { expr, list, .. } => {
                self.check_expr_vars_in_scope(expr, in_scope);
                for arg in list {
                    self.check_expr_vars_in_scope(arg, in_scope);
                }
            }
            Expression::Bracketed { inner, .. } => {
                self.check_expr_vars_in_scope(inner, in_scope);
            }
            // EXISTS/NOT EXISTS introduce their own scope; their pattern's
            // bound variables are local. Don't propagate the outer scope check.
            Expression::Exists { .. } | Expression::NotExists { .. } => {}
            Expression::Literal(_) | Expression::Iri(_) => {}
        }
    }

    fn report_ungrouped_var(&mut self, v: &Var) {
        self.diagnostics.push(Diagnostic {
            code: DiagCode::UngroupedVariableInProjection,
            severity: DiagCode::UngroupedVariableInProjection.default_severity(),
            message: format!(
                "Variable '?{}' is referenced in a SELECT projection but is not in the GROUP BY scope and not an aggregate alias (W3C SPARQL §18.5)",
                v.name
            ),
            span: v.span,
            labels: vec![Label {
                span: v.span,
                message: format!(
                    "'?{}' must appear in GROUP BY, or be wrapped in an aggregate function",
                    v.name
                ),
            }],
            help: None,
            note: None,
        });
    }

    fn validate_construct(&mut self, query: &ConstructQuery) {
        self.validate_graph_pattern(&query.where_clause.pattern);
        // Template triples don't need ground validation (they use WHERE variables)
    }

    fn validate_ask(&mut self, query: &AskQuery) {
        self.validate_graph_pattern(&query.where_clause.pattern);
    }

    fn validate_describe(&mut self, query: &DescribeQuery) {
        if let Some(where_clause) = &query.where_clause {
            self.validate_graph_pattern(&where_clause.pattern);
        }
    }

    fn validate_update(&mut self, op: &UpdateOperation) {
        match op {
            UpdateOperation::InsertData(insert) => {
                self.validate_insert_data(insert);
            }
            UpdateOperation::DeleteData(delete) => {
                self.validate_delete_data(delete);
            }
            UpdateOperation::DeleteWhere(delete_where) => {
                self.validate_delete_where(delete_where);
            }
            UpdateOperation::Modify(modify) => {
                self.validate_modify(modify);
            }
        }
    }

    /// Validate INSERT DATA - triples must be ground (no variables).
    fn validate_insert_data(&mut self, insert: &InsertData) {
        self.validate_ground_quad_data(&insert.data, "INSERT DATA");
    }

    /// Validate DELETE DATA - triples must be ground (no variables).
    fn validate_delete_data(&mut self, delete: &DeleteData) {
        self.validate_ground_quad_data(&delete.data, "DELETE DATA");
    }

    /// Validate DELETE WHERE - patterns can have variables.
    fn validate_delete_where(&mut self, delete_where: &DeleteWhere) {
        // DELETE WHERE allows variables - no ground validation needed.
        //
        // Phase 1: GRAPH blocks in DELETE WHERE are not supported yet because the lowering
        // path in `fluree-db-transact` currently targets triple-only patterns.
        for el in &delete_where.pattern.patterns {
            if let QuadPatternElement::Graph { span, .. } = el {
                self.diagnostics.push(
                    Diagnostic::error(
                        DiagCode::UnsupportedGraphInUpdate,
                        "GRAPH blocks are not supported in DELETE WHERE yet",
                        *span,
                    )
                    .with_help("Rewrite using explicit triples in the default graph, or use DELETE/INSERT with WHERE once GRAPH template support is extended to DELETE WHERE."),
                );
            }
        }
    }

    /// Validate Modify (INSERT/DELETE with WHERE).
    fn validate_modify(&mut self, modify: &Modify) {
        // DELETE and INSERT templates can have variables (bound by WHERE)
        // No ground validation needed for templates
        if let Some(delete_clause) = &modify.delete_clause {
            self.validate_update_template_quad_pattern(delete_clause, "DELETE");
        }
        if let Some(insert_clause) = &modify.insert_clause {
            self.validate_update_template_quad_pattern(insert_clause, "INSERT");
        }

        // Validate WHERE graph pattern (same capabilities as query WHERE).
        self.validate_graph_pattern(&modify.where_clause);
    }

    fn validate_update_template_quad_pattern(&mut self, pattern: &QuadPattern, context: &str) {
        for el in &pattern.patterns {
            if let QuadPatternElement::Graph { name, span, .. } = el {
                match name {
                    crate::ast::pattern::GraphName::Iri(_iri) => {
                        // Allowed (Phase 1)
                    }
                    crate::ast::pattern::GraphName::Var(v) => {
                        self.diagnostics.push(
                            Diagnostic::error(
                                DiagCode::UnsupportedGraphInUpdate,
                                format!(
                                    "GRAPH ?{} is not supported in SPARQL Update {} templates",
                                    v.name, context
                                ),
                                *span,
                            )
                            .with_label(Label::new(v.span, "graph variables not supported here"))
                            .with_help(
                                "Use GRAPH <iri> { ... } with an explicit graph IRI, or rewrite to a fixed target graph.",
                            ),
                        );
                    }
                }
            }
        }
    }

    /// Validate that QuadData contains only ground triples (no variables).
    fn validate_ground_quad_data(&mut self, data: &QuadData, context: &str) {
        for triple in &data.triples {
            self.validate_ground_triple(triple, context);
        }
    }

    /// Validate that a triple pattern is ground (no variables).
    fn validate_ground_triple(&mut self, triple: &TriplePattern, context: &str) {
        // Check subject
        if let SubjectTerm::Var(var) = &triple.subject {
            self.diagnostics.push(
                Diagnostic::error(
                    DiagCode::VariableInGroundData,
                    format!("Variable ?{} not allowed in {}", var.name, context),
                    var.span,
                )
                .with_label(Label::new(var.span, "variable not allowed here"))
                .with_help(format!(
                    "{context} requires ground triples (IRIs, literals, blank nodes) with no variables."
                ))
                .with_note(
                    "Use DELETE WHERE or INSERT/DELETE with WHERE clause for patterns with variables.",
                ),
            );
        }

        // Check predicate
        if let PredicateTerm::Var(var) = &triple.predicate {
            self.diagnostics.push(
                Diagnostic::error(
                    DiagCode::VariableInGroundData,
                    format!("Variable ?{} not allowed in {}", var.name, context),
                    var.span,
                )
                .with_label(Label::new(var.span, "variable not allowed here"))
                .with_help(format!(
                    "{context} requires ground triples (IRIs, literals, blank nodes) with no variables."
                ))
                .with_note(
                    "Use DELETE WHERE or INSERT/DELETE with WHERE clause for patterns with variables.",
                ),
            );
        }

        // Check object
        if let Term::Var(var) = &triple.object {
            self.diagnostics.push(
                Diagnostic::error(
                    DiagCode::VariableInGroundData,
                    format!("Variable ?{} not allowed in {}", var.name, context),
                    var.span,
                )
                .with_label(Label::new(var.span, "variable not allowed here"))
                .with_help(format!(
                    "{context} requires ground triples (IRIs, literals, blank nodes) with no variables."
                ))
                .with_note(
                    "Use DELETE WHERE or INSERT/DELETE with WHERE clause for patterns with variables.",
                ),
            );
        }
    }

    /// Validate a graph pattern recursively.
    fn validate_graph_pattern(&mut self, pattern: &GraphPattern) {
        match pattern {
            GraphPattern::Bgp { patterns, .. } => {
                for triple in patterns {
                    self.validate_triple_pattern(triple);
                }
            }
            GraphPattern::Group { patterns, .. } => {
                for p in patterns {
                    self.validate_graph_pattern(p);
                }
            }
            GraphPattern::Optional { pattern, .. } => {
                self.validate_graph_pattern(pattern);
            }
            GraphPattern::Union { left, right, .. } => {
                self.validate_graph_pattern(left);
                self.validate_graph_pattern(right);
            }
            GraphPattern::Minus { left, right, span } => {
                self.validate_graph_pattern(left);
                self.validate_graph_pattern(right);
                // Emit warning about partial MINUS semantics
                if self.caps.minus_operator {
                    self.diagnostics.push(
                        Diagnostic::warning(
                            DiagCode::MinusSemanticsPartial,
                            "MINUS may have different semantics than SPARQL specification",
                            *span,
                        )
                        .with_note(
                            "Fluree's MINUS implementation may differ from standard SPARQL \
                             in edge cases involving unbound variables.",
                        ),
                    );
                }
            }
            GraphPattern::Filter { .. } => {
                // Expression validation could be added here
            }
            GraphPattern::Bind { .. } => {
                // Expression validation could be added here
            }
            GraphPattern::Values { .. } => {
                // Values are ground by construction
            }
            GraphPattern::Graph { pattern, .. } => {
                self.validate_graph_pattern(pattern);
            }
            GraphPattern::Service { pattern, .. } => {
                self.validate_graph_pattern(pattern);
            }
            GraphPattern::SubSelect { query, .. } => {
                self.validate_graph_pattern(&query.pattern);
            }
            GraphPattern::Path { path, span, .. } => {
                self.validate_property_path(path, *span);
            }
        }
    }

    /// Validate a triple pattern for property paths.
    fn validate_triple_pattern(&mut self, _triple: &TriplePattern) {
        // Triple patterns use PredicateTerm (Var or Iri), not PropertyPath
        // No property path validation needed for basic triples
    }

    /// Validate a property path for unsupported features.
    fn validate_property_path(&mut self, path: &PropertyPath, _pattern_span: SourceSpan) {
        match path {
            PropertyPath::NegatedSet { span, .. } => {
                self.diagnostics.push(
                    Diagnostic::error(
                        DiagCode::UnsupportedNegatedPropertySet,
                        "Negated property sets are not supported",
                        *span,
                    )
                    .with_label(Label::new(*span, "negated set not supported"))
                    .with_help(
                        "Rewrite using FILTER NOT EXISTS or explicit UNION of allowed predicates.",
                    )
                    .with_note("Fluree supports +, *, ?, /, |, ^ but not negated property sets."),
                );
            }
            PropertyPath::Iri(_) | PropertyPath::A { .. } => {
                // Simple paths are always valid
            }
            PropertyPath::Inverse { path, .. } => {
                self.validate_property_path(path, path.span());
            }
            PropertyPath::Sequence { left, right, .. } => {
                self.validate_property_path(left, left.span());
                self.validate_property_path(right, right.span());
            }
            PropertyPath::Alternative { left, right, .. } => {
                self.validate_property_path(left, left.span());
                self.validate_property_path(right, right.span());
            }
            PropertyPath::ZeroOrMore { path, .. } => {
                self.validate_property_path(path, path.span());
            }
            PropertyPath::OneOrMore { path, .. } => {
                self.validate_property_path(path, path.span());
            }
            PropertyPath::ZeroOrOne { path, .. } => {
                self.validate_property_path(path, path.span());
            }
            PropertyPath::Group { path, .. } => {
                self.validate_property_path(path, path.span());
            }
        }
    }
}

/// Recursively detect any `Expression::Aggregate` in an expression tree.
/// Used to flag a SELECT clause as "grouped" when it contains aggregates,
/// even when there is no explicit GROUP BY clause (W3C SPARQL §18.5
/// implicit single-group case).
fn expr_contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Aggregate { .. } => true,
        Expression::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expression::Unary { operand, .. } => expr_contains_aggregate(operand),
        Expression::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        Expression::If {
            condition,
            then_expr,
            else_expr,
            ..
        } => {
            expr_contains_aggregate(condition)
                || expr_contains_aggregate(then_expr)
                || expr_contains_aggregate(else_expr)
        }
        Expression::Coalesce { args, .. } => args.iter().any(expr_contains_aggregate),
        Expression::In { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expression::Bracketed { inner, .. } => expr_contains_aggregate(inner),
        Expression::Var(_)
        | Expression::Literal(_)
        | Expression::Iri(_)
        | Expression::Exists { .. }
        | Expression::NotExists { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_sparql;

    fn validate_query_strict(sparql: &str) -> Vec<Diagnostic> {
        let output = parse_sparql(sparql);
        assert!(
            output.ast.is_some(),
            "Parse failed: {:?}",
            output.diagnostics
        );
        validate(output.ast.as_ref().unwrap(), &Capabilities::w3c_strict())
    }

    fn validate_query(sparql: &str) -> Vec<Diagnostic> {
        let output = parse_sparql(sparql);
        assert!(
            output.ast.is_some(),
            "Parse failed: {:?}",
            output.diagnostics
        );
        validate(output.ast.as_ref().unwrap(), &Capabilities::default())
    }

    // =========================================================================
    // Ground-only validation tests (INSERT DATA / DELETE DATA)
    // =========================================================================

    #[test]
    fn test_insert_data_ground_valid() {
        let diags = validate_query(
            "INSERT DATA { <http://example.org/s> <http://example.org/p> \"value\" }",
        );
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "Expected no errors: {diags:?}"
        );
    }

    #[test]
    fn test_insert_data_variable_subject() {
        let diags = validate_query("INSERT DATA { ?s <http://example.org/p> \"value\" }");
        assert!(diags
            .iter()
            .any(|d| d.code == DiagCode::VariableInGroundData));
    }

    #[test]
    fn test_insert_data_variable_predicate() {
        let diags = validate_query("INSERT DATA { <http://example.org/s> ?p \"value\" }");
        assert!(diags
            .iter()
            .any(|d| d.code == DiagCode::VariableInGroundData));
    }

    #[test]
    fn test_insert_data_variable_object() {
        let diags =
            validate_query("INSERT DATA { <http://example.org/s> <http://example.org/p> ?o }");
        assert!(diags
            .iter()
            .any(|d| d.code == DiagCode::VariableInGroundData));
    }

    #[test]
    fn test_insert_data_all_variables() {
        let diags = validate_query("INSERT DATA { ?s ?p ?o }");
        // Should have 3 errors (one per variable position)
        let var_errors: Vec<_> = diags
            .iter()
            .filter(|d| d.code == DiagCode::VariableInGroundData)
            .collect();
        assert_eq!(var_errors.len(), 3);
    }

    #[test]
    fn test_delete_data_ground_valid() {
        let diags = validate_query(
            "DELETE DATA { <http://example.org/s> <http://example.org/p> \"value\" }",
        );
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "Expected no errors: {diags:?}"
        );
    }

    #[test]
    fn test_delete_data_variable() {
        let diags = validate_query("DELETE DATA { ?s <http://example.org/p> \"value\" }");
        assert!(diags
            .iter()
            .any(|d| d.code == DiagCode::VariableInGroundData));
    }

    // =========================================================================
    // DELETE WHERE and Modify tests (variables allowed)
    // =========================================================================

    #[test]
    fn test_delete_where_variables_allowed() {
        let diags = validate_query("DELETE WHERE { ?s ?p ?o }");
        // Variables are allowed in DELETE WHERE
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::VariableInGroundData),
            "Variables should be allowed in DELETE WHERE"
        );
    }

    #[test]
    fn test_modify_variables_allowed() {
        let diags = validate_query(
            "DELETE { ?s ex:old ?o } INSERT { ?s ex:new ?o } WHERE { ?s ex:old ?o }",
        );
        // Variables are allowed in DELETE/INSERT with WHERE
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::VariableInGroundData),
            "Variables should be allowed in Modify operations"
        );
    }

    // =========================================================================
    // Property path validation tests
    // =========================================================================

    #[test]
    fn test_property_path_simple_valid() {
        let diags = validate_query("SELECT * WHERE { ?s ex:name ?o }");
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "Expected no errors: {diags:?}"
        );
    }

    #[test]
    fn test_property_path_transitive_valid() {
        let diags = validate_query("SELECT * WHERE { ?s ex:parent+ ?o }");
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedNegatedPropertySet),
            "Transitive paths should be valid"
        );
    }

    #[test]
    fn test_property_path_negated_invalid() {
        let diags = validate_query("SELECT * WHERE { ?s !ex:hidden ?o }");
        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedNegatedPropertySet),
            "Negated property sets should be rejected"
        );
    }

    #[test]
    fn test_property_path_negated_set_invalid() {
        let diags = validate_query("SELECT * WHERE { ?s !(ex:a|ex:b) ?o }");
        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedNegatedPropertySet),
            "Negated property sets should be rejected"
        );
    }

    #[test]
    fn test_property_path_complex_valid() {
        let diags = validate_query("SELECT * WHERE { ?s ^ex:parent/ex:child+ ?o }");
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedNegatedPropertySet),
            "Complex but supported paths should be valid"
        );
    }

    // =========================================================================
    // USING clause validation tests
    // =========================================================================

    #[test]
    fn test_using_clause_valid() {
        let diags = validate_query(
            "DELETE { ?s ex:p ?o } WHERE { ?s ex:p ?o } USING <http://example.org/graph>",
        );
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedUsingNamed),
            "USING should be valid"
        );
    }

    // Note: USING NAMED parsing would need to be tested if we add parser support
    // Currently the parser may not parse USING NAMED syntax

    // =========================================================================
    // MINUS warning tests
    // =========================================================================

    #[test]
    fn test_minus_warning() {
        // MINUS must be inside the WHERE clause braces
        let diags = validate_query("SELECT * WHERE { ?s ?p ?o MINUS { ?s ex:hidden ?o } }");
        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagCode::MinusSemanticsPartial && d.is_warning()),
            "MINUS should emit a warning: {diags:?}"
        );
    }

    // =========================================================================
    // SELECT query tests (no special validation needed)
    // =========================================================================

    #[test]
    fn test_select_query_valid() {
        let diags = validate_query("SELECT ?x ?y WHERE { ?x ex:knows ?y }");
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "Expected no errors: {diags:?}"
        );
    }

    // =========================================================================
    // Diagnostic message quality tests
    // =========================================================================

    #[test]
    fn test_variable_error_has_help() {
        let diags = validate_query("INSERT DATA { ?s <http://example.org/p> \"value\" }");
        let var_error = diags
            .iter()
            .find(|d| d.code == DiagCode::VariableInGroundData)
            .expect("Expected variable error");
        assert!(var_error.help.is_some(), "Error should have help text");
        assert!(var_error.note.is_some(), "Error should have a note");
    }

    #[test]
    fn test_negated_path_error_has_help() {
        let diags = validate_query("SELECT * WHERE { ?s !ex:hidden ?o }");
        let path_error = diags
            .iter()
            .find(|d| d.code == DiagCode::UnsupportedNegatedPropertySet)
            .expect("Expected path error");
        assert!(path_error.help.is_some(), "Error should have help text");
        assert!(path_error.note.is_some(), "Error should have a note");
    }

    // =========================================================================
    // Aggregate scope rules — W3C SPARQL §18.5
    // (negative-syntax tests agg08–agg12)
    // =========================================================================

    fn has_ungrouped_var_error(diags: &[Diagnostic]) -> bool {
        diags
            .iter()
            .any(|d| d.code == DiagCode::UngroupedVariableInProjection)
    }

    #[test]
    fn test_ungrouped_var_with_aggregate_no_groupby_rejected() {
        // agg10: SELECT ?P (COUNT(?O) AS ?C) WHERE { ?S ?P ?O } — no GROUP BY,
        // ?P is ungrouped while an aggregate is present.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?P (COUNT(?O) AS ?C) WHERE { ?S ?P ?O }",
        );
        assert!(
            has_ungrouped_var_error(&diags),
            "expected ungrouped-var error; got: {diags:?}"
        );
    }

    #[test]
    fn test_ungrouped_var_with_explicit_groupby_rejected() {
        // agg09: GROUP BY ?S but SELECT projects ?P which isn't grouped.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?P (COUNT(?O) AS ?C) WHERE { ?S ?P ?O } GROUP BY ?S",
        );
        assert!(
            has_ungrouped_var_error(&diags),
            "expected ungrouped-var error; got: {diags:?}"
        );
    }

    #[test]
    fn test_groupby_expression_without_alias_does_not_expose_inner_vars() {
        // agg08: GROUP BY (?O1 + ?O2) without alias — ?O1 and ?O2 are not
        // grouped (only the synthesized expression alias is implementation-only).
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ((?O1 + ?O2) AS ?O12) (COUNT(?O1) AS ?C)
             WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?O1 + ?O2)
             ORDER BY ?O12",
        );
        assert!(
            has_ungrouped_var_error(&diags),
            "expected ungrouped-var error; got: {diags:?}"
        );
    }

    #[test]
    fn test_ungrouped_var_in_projection_expression_rejected() {
        // agg11: GROUP BY (?S) but projection expression uses ?O1 + ?O2.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ((?O1 + ?O2) AS ?O12) (COUNT(?O1) AS ?C)
             WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?S)",
        );
        assert!(
            has_ungrouped_var_error(&diags),
            "expected ungrouped-var error; got: {diags:?}"
        );
    }

    #[test]
    fn test_var_inside_groupby_expr_is_not_grouped() {
        // agg12: GROUP BY (?O1 + ?O2) but SELECT projects ?O1.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?O1 (COUNT(?O2) AS ?C)
             WHERE { ?S :p ?O1; :q ?O2 } GROUP BY (?O1 + ?O2)",
        );
        assert!(
            has_ungrouped_var_error(&diags),
            "expected ungrouped-var error; got: {diags:?}"
        );
    }

    #[test]
    fn test_grouped_var_in_projection_accepted() {
        // SELECT ?S projects a grouped variable — should pass.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?S (COUNT(?O) AS ?C) WHERE { ?S ?P ?O } GROUP BY ?S",
        );
        assert!(
            !has_ungrouped_var_error(&diags),
            "should not flag grouped variable; got: {diags:?}"
        );
    }

    #[test]
    fn test_aggregate_alias_in_post_aggregation_projection_accepted() {
        // SELECT ?S (COUNT(?O) AS ?C) (?C + 1 AS ?D) — ?C is an aggregate
        // alias and is allowed in subsequent projection expressions.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?S (COUNT(?O) AS ?C) ((?C + 1) AS ?D)
             WHERE { ?S ?P ?O } GROUP BY ?S",
        );
        assert!(
            !has_ungrouped_var_error(&diags),
            "should accept aggregate alias in projection; got: {diags:?}"
        );
    }

    #[test]
    fn test_groupby_expression_with_alias_grouped_var_accepted() {
        // GROUP BY (UCASE(?S) AS ?up) — ?up is grouped.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?up (COUNT(?O) AS ?C)
             WHERE { ?S ?P ?O } GROUP BY (UCASE(STR(?S)) AS ?up)",
        );
        assert!(
            !has_ungrouped_var_error(&diags),
            "should accept GROUP BY alias; got: {diags:?}"
        );
    }

    #[test]
    fn test_simple_select_without_aggregates_or_groupby_unaffected() {
        // No aggregates, no GROUP BY: scope check is skipped entirely.
        let diags = validate_query_strict(
            "PREFIX : <http://example.org/>
             SELECT ?S ?P ?O WHERE { ?S ?P ?O }",
        );
        assert!(
            !has_ungrouped_var_error(&diags),
            "non-aggregate query should not trigger scope check; got: {diags:?}"
        );
    }
}
