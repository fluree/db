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

use crate::ast::path::PropertyPath;
use crate::ast::pattern::{GraphPattern, TriplePattern};
use crate::ast::query::{
    AskQuery, ConstructQuery, DescribeQuery, QueryBody, SelectQuery, SparqlAst,
};
use crate::ast::term::{PredicateTerm, SubjectTerm, Term};
use crate::ast::update::{
    DeleteData, DeleteWhere, InsertData, Modify, QuadData, QuadPattern, QuadPatternElement,
    UpdateOperation,
};
use crate::diag::{DiagCode, Diagnostic, Label};
use crate::span::SourceSpan;

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
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            property_paths: true,
            minus_operator: true,
            using_clause: true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_sparql;

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
}
