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

mod bnode_scope;
mod projection;

use crate::ast::path::PropertyPath;
use crate::ast::pattern::{GraphPattern, TriplePattern};
use crate::ast::query::{
    AskQuery, ConstructQuery, DescribeQuery, QueryBody, SelectQuery, SparqlAst,
};
use crate::ast::term::BlankNodeValue;
use crate::ast::term::{PredicateTerm, SubjectTerm, Term};
use crate::ast::update::{
    DeleteData, DeleteWhere, InsertData, Modify, QuadData, QuadPattern, QuadPatternElement,
    UpdateOperation,
};
use crate::diag::{DiagCode, Diagnostic, Label};
use crate::span::SourceSpan;

/// Blank-node labels with this prefix are Fluree *stable ids*: they address
/// the existing stored node as a constant instead of acting as a fresh blank
/// node, so the SPARQL 1.1 Update §19.8 bnode-in-DELETE restriction does not
/// apply to them. Must stay in sync with
/// `fluree_db_core::ns_encoding::STABLE_BLANK_NODE_LABEL_PREFIX` (that crate
/// is a feature-gated dependency, so the value is duplicated here; a
/// lowering-feature test asserts equality).
const STABLE_BLANK_NODE_LABEL_PREFIX: &str = "fdb-";

/// Fluree capability configuration.
///
/// Controls which SPARQL features are allowed during validation.
/// By default, all Fluree-supported features are enabled — except
/// [`Capabilities::delete_where_extensions`], which defaults to the strict
/// W3C surface (see its field docs).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Capabilities {
    /// Allow property path operators (+, *, ?, /, |, ^)
    pub property_paths: bool,
    /// Allow MINUS operator (with partial semantics warning)
    pub minus_operator: bool,
    /// Allow USING clause in updates
    pub using_clause: bool,
    /// Allow Fluree's documented DELETE WHERE extensions: non-stable blank
    /// nodes as existential variables (the lowering's deliberate carve-out
    /// in fluree-db-transact) and anonymous annotation tails (`{| ... |}`),
    /// whose reifier is minted at lowering (docs/concepts/edge-annotations.md
    /// "SPARQL UPDATE rules by operation").
    ///
    /// `false` — the default — is the strict W3C surface, which rejects both
    /// (SPARQL 1.1 Update §19.8 grammar note 8, W3C `syntax-update-bad-10`;
    /// SPARQL 1.2 `syntax-update-anonreifier-01`). The W3C harness and every
    /// pre-existing caller rely on the strict default; the production
    /// transact seam (fluree-db-api) opts in via
    /// [`Capabilities::with_delete_where_extensions`] to keep the shipped
    /// update surface. Only the two DELETE WHERE rejections are waived —
    /// DELETE DATA, Modify DELETE templates, and graph-variable rules apply
    /// on every surface.
    pub delete_where_extensions: bool,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            property_paths: true,
            minus_operator: true,
            using_clause: true,
            delete_where_extensions: false,
        }
    }
}

impl Capabilities {
    /// The Fluree transact-surface configuration: strict W3C validation with
    /// the documented DELETE WHERE extensions admitted
    /// ([`Capabilities::delete_where_extensions`]).
    ///
    /// The struct is `#[non_exhaustive]`, so out-of-crate callers cannot use
    /// functional-update syntax — this constructor is the supported way to
    /// opt in.
    pub fn with_delete_where_extensions() -> Self {
        Self {
            delete_where_extensions: true,
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
            QueryBody::Update(request) => {
                for op in &request.operations {
                    self.validate_update(&op.operation);
                }
            }
        }
    }

    fn validate_select(&mut self, query: &SelectQuery) {
        self.validate_query_where(&query.where_clause.pattern);
        projection::check_projection_scope(
            &query.select.variables,
            query.modifiers.group_by.as_ref(),
            query.select.span,
            &mut self.diagnostics,
        );
        projection::check_select_aliases(
            &query.select.variables,
            &query.where_clause.pattern,
            &mut self.diagnostics,
        );
        projection::check_nested_aggregates(
            &query.select.variables,
            &query.modifiers,
            &mut self.diagnostics,
        );
        // The post-query VALUES clause shares the Values pattern arm
        // (duplicate-variable check).
        if let Some(values) = &query.values {
            self.validate_graph_pattern(values);
        }
    }

    fn validate_construct(&mut self, query: &ConstructQuery) {
        self.validate_query_where(&query.where_clause.pattern);
        // Template triples don't need ground validation (they use WHERE variables)
    }

    fn validate_ask(&mut self, query: &AskQuery) {
        self.validate_query_where(&query.where_clause.pattern);
    }

    fn validate_describe(&mut self, query: &DescribeQuery) {
        if let Some(where_clause) = &query.where_clause {
            self.validate_query_where(&where_clause.pattern);
        }
    }

    /// Shared validation for a query form's WHERE pattern: the capability
    /// walk plus the query-only semantic passes (blank-node label scope —
    /// update operations have their own blank-node rules and are exempt).
    fn validate_query_where(&mut self, pattern: &GraphPattern) {
        self.validate_graph_pattern(pattern);
        bnode_scope::check_blank_node_scopes(pattern, &mut self.diagnostics);
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
    ///
    /// Annotation tails minting anonymous reifiers are deliberately
    /// ALLOWED here: they are Fluree's committed SPARQL 1.2 transact
    /// surface for edge annotations (a fresh blank reifier is minted at
    /// lowering, like a bnode subject would be) — a reviewed divergence
    /// from the W3C negative-syntax reading. DELETE DATA differs; see
    /// [`Validator::validate_delete_data`].
    fn validate_insert_data(&mut self, insert: &InsertData) {
        // Blank nodes are allowed: they mint fresh nodes (CONSTRUCT-style).
        self.validate_ground_quad_data(&insert.data, "INSERT DATA", false);
    }

    /// Validate DELETE DATA - triples must be ground (no variables), and
    /// annotation tails must not mint anonymous reifiers (a `{| ... |}`
    /// block or bare `~` with no explicit reifier id): an anonymous
    /// reifier has no addressable identity to delete (SPARQL 1.1 §3.1.3
    /// blank-node rule extended to RDF 1.2 reifiers; W3C sparql12
    /// syntax-update-anonreifier-02). Mirrors the existing lowering-time
    /// rejection in fluree-db-transact so the query never produces an AST
    /// the API would act on.
    fn validate_delete_data(&mut self, delete: &DeleteData) {
        // Ground-data validation with blank nodes forbidden (SPARQL 1.1
        // Update §19.8 grammar note 8): a blank node denotes a fresh node,
        // which can never match stored data.
        self.validate_ground_quad_data(&delete.data, "DELETE DATA", true);
        for el in &delete.data.quads {
            match el {
                QuadPatternElement::Triple(triple) => {
                    self.check_delete_data_annotation(triple);
                }
                QuadPatternElement::Graph { triples, .. } => {
                    for triple in triples {
                        self.check_delete_data_annotation(triple);
                    }
                }
            }
        }
    }

    /// Reject annotation units without an explicit reifier id in DELETE
    /// forms. An anonymous annotation (`{| ... |}` with no `~ id`, or a
    /// bare `~`) mints a fresh blank node, and SPARQL 1.1 §3.1.3
    /// forbids blank nodes in DELETE templates (W3C negative test
    /// `syntax-update-anonreifier-01`). Blank-node *reifier ids*
    /// (`~ _:b`) stay under the broader bnodes-in-DELETE validation
    /// owned by burn-down PR-U1.
    fn reject_anonymous_annotations_in_delete(&mut self, pattern: &QuadPattern, context: &str) {
        let check_triples = |triples: &[TriplePattern], diags: &mut Vec<Diagnostic>| {
            for tp in triples {
                let Some(ann) = &tp.annotation else { continue };
                for unit in &ann.units {
                    if unit.reifier.is_none() {
                        diags.push(
                            Diagnostic::error(
                                DiagCode::AnonymousAnnotationInDelete,
                                format!(
                                    "anonymous annotation (`{{| ... |}}` without `~ <reifier>`) \
                                     is not allowed in {context}"
                                ),
                                unit.span,
                            )
                            .with_help(
                                "Name the reifier explicitly (`~ <iri>`) or bind it with a \
                                 variable reifier (`~ ?r`) in the WHERE clause.",
                            ),
                        );
                    }
                }
            }
        };
        for el in &pattern.patterns {
            match el {
                QuadPatternElement::Triple(tp) => {
                    check_triples(std::slice::from_ref(&**tp), &mut self.diagnostics);
                }
                QuadPatternElement::Graph { triples, .. } => {
                    check_triples(triples, &mut self.diagnostics);
                }
            }
        }
    }

    /// Reject an annotation tail that mints an anonymous reifier inside
    /// DELETE DATA (see [`Validator::validate_delete_data`]).
    fn check_delete_data_annotation(&mut self, triple: &TriplePattern) {
        if let Some(annotation) = &triple.annotation {
            for unit in annotation
                .units
                .iter()
                .filter(|unit| unit.reifier.is_none())
            {
                self.diagnostics.push(
                    Diagnostic::error(
                        DiagCode::AnonymousAnnotationInGroundData,
                        "anonymous annotation block ({| |}) in DELETE DATA — \
                         no addressable identity to delete",
                        unit.span,
                    )
                    .with_help(
                        "Name the reifier explicitly (s p o ~ <reifier> {| ... |}) \
                         so the annotation to delete is addressable, or use \
                         DELETE WHERE.",
                    ),
                );
            }
        }
    }

    /// Validate DELETE WHERE - patterns can have variables.
    fn validate_delete_where(&mut self, delete_where: &DeleteWhere) {
        // DELETE WHERE allows variables - no ground validation needed.
        //
        // The quad pattern doubles as the DELETE template (`DELETE WHERE { P }`
        // is shorthand for `DELETE { P } WHERE { P }`), so the same template
        // rules as Modify apply: `GRAPH <iri>` blocks are supported, graph
        // variables are not (Phase 1), and blank nodes are forbidden
        // (SPARQL 1.1 Update §19.8 grammar note 8).
        //
        // Under `Capabilities::delete_where_extensions` (the Fluree transact
        // surface) the two strict-surface rejections below are waived: blank
        // nodes keep their documented existential-variable semantics and
        // anonymous annotation tails mint their reifier at lowering. The
        // graph-variable rejection applies on every surface, and the other
        // update arms (DELETE DATA, Modify templates) stay strict — the
        // waiver is scoped to this method by design; see the field docs.
        let strict = !self.caps.delete_where_extensions;
        self.validate_update_template_quad_pattern(&delete_where.pattern, "DELETE WHERE", strict);
        // The DELETE WHERE pattern doubles as the delete template —
        // anonymous reifiers are blank nodes, which SPARQL forbids in
        // DELETE templates.
        if strict {
            self.reject_anonymous_annotations_in_delete(&delete_where.pattern, "DELETE WHERE");
        }
    }

    /// Validate Modify (INSERT/DELETE with WHERE).
    fn validate_modify(&mut self, modify: &Modify) {
        // DELETE and INSERT templates can have variables (bound by WHERE)
        // No ground validation needed for templates. Blank nodes are
        // forbidden in the DELETE template only (INSERT templates mint
        // per-solution fresh nodes, CONSTRUCT-style).
        if let Some(delete_clause) = &modify.delete_clause {
            self.validate_update_template_quad_pattern(delete_clause, "DELETE", true);
            self.reject_anonymous_annotations_in_delete(delete_clause, "DELETE template");
        }
        if let Some(insert_clause) = &modify.insert_clause {
            self.validate_update_template_quad_pattern(insert_clause, "INSERT", false);
        }

        // Validate WHERE graph pattern (same capabilities as query WHERE).
        self.validate_graph_pattern(&modify.where_clause);
    }

    fn validate_update_template_quad_pattern(
        &mut self,
        pattern: &QuadPattern,
        context: &str,
        reject_blank_nodes: bool,
    ) {
        for el in &pattern.patterns {
            match el {
                QuadPatternElement::Triple(triple) => {
                    if reject_blank_nodes {
                        self.validate_no_blank_nodes_in_delete_triple(triple, context);
                    }
                }
                QuadPatternElement::Graph {
                    name,
                    triples,
                    span,
                } => {
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
                    if reject_blank_nodes {
                        for triple in triples {
                            self.validate_no_blank_nodes_in_delete_triple(triple, context);
                        }
                    }
                }
            }
        }
    }

    /// Reject blank nodes in a DELETE-side triple (SPARQL 1.1 Update §19.8
    /// grammar note 8). Fluree stable ids (`_:fdb-...`) are exempt: they are
    /// constants addressing the existing stored node.
    fn validate_no_blank_nodes_in_delete_triple(&mut self, triple: &TriplePattern, context: &str) {
        if let SubjectTerm::BlankNode(bn) = &triple.subject {
            self.reject_blank_node_in_delete(&bn.value, bn.span, context);
        }
        if let Term::BlankNode(bn) = &triple.object {
            self.reject_blank_node_in_delete(&bn.value, bn.span, context);
        }
    }

    fn reject_blank_node_in_delete(
        &mut self,
        value: &BlankNodeValue,
        span: SourceSpan,
        context: &str,
    ) {
        let label = match value {
            BlankNodeValue::Labeled(l) => {
                if l.starts_with(STABLE_BLANK_NODE_LABEL_PREFIX) {
                    // Fluree stable id: denotes the stored node, not a fresh one.
                    return;
                }
                format!("_:{l}")
            }
            BlankNodeValue::Anon => "[]".to_string(),
        };
        self.diagnostics.push(
            Diagnostic::error(
                DiagCode::BlankNodeInDelete,
                format!("Blank node {label} is not allowed in {context}"),
                span,
            )
            .with_label(Label::new(span, "blank node not allowed here"))
            .with_help(
                "SPARQL 1.1 Update forbids blank nodes in DELETE operations: a blank node \
                 denotes a fresh node and can never match existing data. Use a variable \
                 bound by a WHERE clause, or a concrete IRI.",
            )
            .with_note(
                "Fluree stable blank-node ids (_:fdb-...) are allowed here: they address \
                 the existing stored node.",
            ),
        );
    }

    /// Validate that QuadData contains only ground triples (no variables),
    /// including inside `GRAPH <iri> { ... }` blocks. Variable graph names are
    /// rejected: DATA must be ground. When `reject_blank_nodes` is set
    /// (DELETE DATA), blank nodes are also rejected per SPARQL 1.1 Update
    /// §19.8 grammar note 8.
    fn validate_ground_quad_data(
        &mut self,
        data: &QuadData,
        context: &str,
        reject_blank_nodes: bool,
    ) {
        for el in &data.quads {
            match el {
                QuadPatternElement::Triple(triple) => {
                    self.validate_ground_triple(triple, context);
                    if reject_blank_nodes {
                        self.validate_no_blank_nodes_in_delete_triple(triple, context);
                    }
                }
                QuadPatternElement::Graph {
                    name,
                    triples,
                    span,
                } => {
                    if let crate::ast::pattern::GraphName::Var(v) = name {
                        self.diagnostics.push(
                            Diagnostic::error(
                                DiagCode::VariableInGroundData,
                                format!("Variable graph name ?{} not allowed in {context}", v.name),
                                *span,
                            )
                            .with_label(Label::new(v.span, "variable not allowed here"))
                            .with_help(format!(
                                "{context} requires a fixed graph IRI; use GRAPH <iri> {{ ... }}."
                            ))
                            .with_note(
                                "Use INSERT/DELETE with a WHERE clause for variable graph targets.",
                            ),
                        );
                    }
                    for triple in triples {
                        self.validate_ground_triple(triple, context);
                        if reject_blank_nodes {
                            self.validate_no_blank_nodes_in_delete_triple(triple, context);
                        }
                    }
                }
            }
        }
    }

    /// Validate that a triple pattern is ground (no variables),
    /// recursing into RDF 1.2 reified-triple terms (`<< s p o ~ r >>`)
    /// whose inner positions and reifier must be ground too.
    fn validate_ground_triple(&mut self, triple: &TriplePattern, context: &str) {
        self.validate_ground_subject(&triple.subject, context);
        self.validate_ground_predicate(&triple.predicate, context);
        self.validate_ground_object(&triple.object, context);
    }

    fn validate_ground_subject(&mut self, subject: &SubjectTerm, context: &str) {
        match subject {
            SubjectTerm::Var(var) => self.push_ground_violation(&var.name, var.span, context),
            SubjectTerm::QuotedTriple(qt) => self.validate_ground_quoted_triple(qt, context),
            SubjectTerm::Iri(_) | SubjectTerm::BlankNode(_) => {}
        }
    }

    fn validate_ground_predicate(&mut self, predicate: &PredicateTerm, context: &str) {
        if let PredicateTerm::Var(var) = predicate {
            self.push_ground_violation(&var.name, var.span, context);
        }
    }

    fn validate_ground_object(&mut self, object: &Term, context: &str) {
        match object {
            Term::Var(var) => self.push_ground_violation(&var.name, var.span, context),
            Term::QuotedTriple(qt) => self.validate_ground_quoted_triple(qt, context),
            Term::Iri(_) | Term::Literal(_) | Term::BlankNode(_) => {}
        }
    }

    fn validate_ground_quoted_triple(&mut self, qt: &crate::ast::QuotedTriple, context: &str) {
        self.validate_ground_subject(&qt.subject, context);
        self.validate_ground_predicate(&qt.predicate, context);
        self.validate_ground_object(&qt.object, context);
        if let Some(crate::ast::ReifierId::Var(var)) =
            qt.reifier.as_ref().and_then(|r| r.id.as_ref())
        {
            self.push_ground_violation(&var.name, var.span, context);
        }
    }

    fn push_ground_violation(&mut self, name: &str, span: SourceSpan, context: &str) {
        self.diagnostics.push(
            Diagnostic::error(
                DiagCode::VariableInGroundData,
                format!("Variable ?{name} not allowed in {context}"),
                span,
            )
            .with_label(Label::new(span, "variable not allowed here"))
            .with_help(format!(
                "{context} requires ground triples (IRIs, literals, blank nodes) with no variables."
            ))
            .with_note(
                "Use DELETE WHERE or INSERT/DELETE with WHERE clause for patterns with variables.",
            ),
        );
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
            GraphPattern::Values { vars, .. } => {
                // Values terms are ground by construction; the variable
                // list, however, must not repeat a variable (SPARQL 1.2
                // negative-syntax rule; also implied by SPARQL 1.1 §10.2's
                // "must all be distinct" data-block contract).
                let mut seen = std::collections::HashSet::new();
                for var in vars {
                    if !seen.insert(var.name.as_ref()) {
                        self.diagnostics.push(
                            Diagnostic::error(
                                DiagCode::DuplicateValuesVariable,
                                format!(
                                    "variable ?{} is listed more than once in VALUES",
                                    var.name
                                ),
                                var.span,
                            )
                            .with_help("Each variable in a VALUES clause must be distinct."),
                        );
                    }
                }
            }
            GraphPattern::Graph { pattern, .. } => {
                self.validate_graph_pattern(pattern);
            }
            GraphPattern::Service { pattern, .. } => {
                self.validate_graph_pattern(pattern);
            }
            GraphPattern::SubSelect { query, span } => {
                self.validate_graph_pattern(&query.pattern);
                projection::check_projection_scope(
                    &query.variables,
                    query.modifiers.group_by.as_ref(),
                    *span,
                    &mut self.diagnostics,
                );
                projection::check_select_aliases(
                    &query.variables,
                    &query.pattern,
                    &mut self.diagnostics,
                );
                projection::check_nested_aggregates(
                    &query.variables,
                    &query.modifiers,
                    &mut self.diagnostics,
                );
            }
            GraphPattern::Path { path, span, .. } => {
                self.validate_property_path(path, *span);
            }
            GraphPattern::AnnotationTarget { .. } => {
                // No additional validation here; deferred-shape rejection
                // happens at parse time and lowering enforces the rest.
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
            // Negated property sets lower to a fresh-predicate-var triple plus a
            // FILTER excluding the listed predicates (see lower/path.rs). Members
            // are leaf IRIs/`a` (optionally inverse), so no inner recursion.
            PropertyPath::NegatedSet { .. } => {}
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

    #[test]
    fn test_insert_data_graph_block_ground_valid() {
        // Issue #1288: GRAPH <iri> { ground triples } is valid in INSERT DATA.
        let diags = validate_query(
            "INSERT DATA { GRAPH <urn:g> { <http://example.org/s> <http://example.org/p> \"v\" } }",
        );
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "Expected no errors: {diags:?}"
        );
    }

    #[test]
    fn test_insert_data_graph_block_variable_rejected() {
        // A variable inside a GRAPH block in DATA is still non-ground.
        let diags =
            validate_query("INSERT DATA { GRAPH <urn:g> { ?s <http://example.org/p> \"v\" } }");
        assert!(diags
            .iter()
            .any(|d| d.code == DiagCode::VariableInGroundData));
    }

    #[test]
    fn test_insert_data_variable_graph_name_rejected() {
        // A variable graph name is not ground and must be rejected.
        let diags = validate_query(
            "INSERT DATA { GRAPH ?g { <http://example.org/s> <http://example.org/p> \"v\" } }",
        );
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
    fn test_delete_where_graph_iri_block_allowed() {
        // W3C syntax-update-1 test_36: GRAPH <iri> blocks are valid in
        // DELETE WHERE (the pattern doubles as a Modify-style template).
        let diags =
            validate_query("DELETE WHERE { GRAPH <urn:g> { <urn:s> <http://example.org/p> ?o } }");
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "GRAPH <iri> should be allowed in DELETE WHERE: {diags:?}"
        );
    }

    #[test]
    fn test_delete_where_graph_variable_rejected() {
        // Graph variables are unsupported in update templates (Phase 1), and
        // the DELETE WHERE pattern is also the delete template.
        let diags = validate_query("DELETE WHERE { GRAPH ?g { ?s ?p ?o } }");
        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedGraphInUpdate),
            "GRAPH ?var should be rejected in DELETE WHERE: {diags:?}"
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
    // Blank-node-in-DELETE validation tests (SPARQL 1.1 Update §19.8 note 8)
    // =========================================================================

    fn has_bnode_in_delete_error(diags: &[Diagnostic]) -> bool {
        diags
            .iter()
            .any(|d| d.code == DiagCode::BlankNodeInDelete && d.is_error())
    }

    #[test]
    fn test_delete_data_blank_node_rejected() {
        // W3C syntax-update-1 test_52 (syntax-update-bad-12.ru)
        let diags = validate_query("DELETE DATA { _:a <http://example.org/p> <urn:o> }");
        assert!(
            has_bnode_in_delete_error(&diags),
            "labeled blank node must be rejected in DELETE DATA: {diags:?}"
        );
    }

    #[test]
    fn test_delete_where_blank_node_rejected() {
        // W3C syntax-update-1 test_50 (syntax-update-bad-10.ru)
        let diags = validate_query("DELETE WHERE { _:a <http://example.org/p> <urn:o> }");
        assert!(
            has_bnode_in_delete_error(&diags),
            "labeled blank node must be rejected in DELETE WHERE: {diags:?}"
        );
    }

    #[test]
    fn test_delete_template_anonymous_blank_node_rejected() {
        // W3C syntax-update-1 test_51 (syntax-update-bad-11.ru)
        let diags = validate_query(
            "DELETE { <urn:s> <http://example.org/p> [] } WHERE { ?x <http://example.org/p> <urn:o> }",
        );
        assert!(
            has_bnode_in_delete_error(&diags),
            "anonymous blank node must be rejected in a DELETE template: {diags:?}"
        );
    }

    #[test]
    fn test_delete_template_graph_block_blank_node_rejected() {
        // The rejection also applies inside GRAPH <iri> blocks.
        let diags = validate_query(
            "DELETE { GRAPH <urn:g> { _:b <http://example.org/p> ?o } } WHERE { ?s <http://example.org/p> ?o }",
        );
        assert!(
            has_bnode_in_delete_error(&diags),
            "blank node inside a GRAPH block must be rejected in a DELETE template: {diags:?}"
        );
    }

    #[test]
    fn test_insert_forms_blank_nodes_still_allowed() {
        // INSERT DATA and INSERT templates mint fresh nodes (CONSTRUCT-style).
        let diags = validate_query("INSERT DATA { _:a <http://example.org/p> <urn:o> }");
        assert!(
            !has_bnode_in_delete_error(&diags),
            "blank nodes stay legal in INSERT DATA: {diags:?}"
        );
        let diags = validate_query(
            "INSERT { ?s <http://example.org/q> [] } WHERE { ?s <http://example.org/p> ?o }",
        );
        assert!(
            !has_bnode_in_delete_error(&diags),
            "blank nodes stay legal in INSERT templates: {diags:?}"
        );
    }

    #[test]
    fn test_delete_forms_stable_blank_node_ids_allowed() {
        // Fluree extension: stable `_:fdb-...` ids address the existing
        // stored node (a constant), so the SPARQL restriction doesn't apply.
        let diags = validate_query("DELETE DATA { _:fdb-abc123 <http://example.org/p> <urn:o> }");
        assert!(
            !has_bnode_in_delete_error(&diags),
            "stable blank-node ids stay legal in DELETE DATA: {diags:?}"
        );
        let diags = validate_query("DELETE WHERE { _:fdb-abc123 ?p ?o }");
        assert!(
            !has_bnode_in_delete_error(&diags),
            "stable blank-node ids stay legal in DELETE WHERE: {diags:?}"
        );
    }

    // =========================================================================
    // Capabilities::delete_where_extensions (the Fluree transact surface)
    // =========================================================================

    fn validate_with(caps: &Capabilities, sparql: &str) -> Vec<Diagnostic> {
        let output = parse_sparql(sparql);
        assert!(
            output.ast.is_some(),
            "Parse failed: {:?}",
            output.diagnostics
        );
        validate(output.ast.as_ref().unwrap(), caps)
    }

    #[test]
    fn test_delete_where_extensions_allow_existential_blank_nodes() {
        // The production transact seam keeps Fluree's documented
        // existential-variable semantics for DELETE WHERE blank nodes
        // (fluree-db-transact's lowering carve-out), on the triple-only and
        // GRAPH-bearing forms alike.
        let caps = Capabilities::with_delete_where_extensions();
        let diags = validate_with(&caps, "DELETE WHERE { _:a <http://example.org/p> <urn:o> }");
        assert!(
            !has_bnode_in_delete_error(&diags),
            "existential blank nodes stay legal in DELETE WHERE on the transact surface: {diags:?}"
        );
        let diags = validate_with(
            &caps,
            "DELETE WHERE { GRAPH <urn:g> { _:x <http://example.org/p> ?o } }",
        );
        assert!(
            !has_bnode_in_delete_error(&diags),
            "existential blank nodes stay legal inside GRAPH blocks too: {diags:?}"
        );
    }

    #[test]
    fn test_delete_where_extensions_allow_anonymous_annotations() {
        // Anonymous annotation tails in DELETE WHERE mint their reifier at
        // lowering (AnnotationExpansionMode::DeleteWhere) — the transact
        // surface has always accepted them (pinned end-to-end by
        // fluree-db-api's it_query_sparql_annotations.rs).
        let caps = Capabilities::with_delete_where_extensions();
        let diags = validate_with(
            &caps,
            "DELETE WHERE { <urn:s> <http://example.org/p> <urn:o> {| <http://example.org/q> ?v |} . }",
        );
        assert!(
            diags.iter().all(|d| !d.is_error()),
            "anonymous annotation tails stay legal in DELETE WHERE on the transact surface: {diags:?}"
        );
    }

    #[test]
    fn test_delete_where_extensions_keep_every_other_arm_strict() {
        // The capability waives ONLY the two DELETE WHERE rejections; the
        // DELETE DATA and Modify-DELETE-template arms and the graph-variable
        // rule must stay strict on every surface.
        let caps = Capabilities::with_delete_where_extensions();
        let diags = validate_with(&caps, "DELETE DATA { _:a <http://example.org/p> <urn:o> }");
        assert!(
            has_bnode_in_delete_error(&diags),
            "DELETE DATA blank nodes stay rejected: {diags:?}"
        );
        let diags = validate_with(
            &caps,
            "DELETE { _:b <http://example.org/p> ?o } WHERE { ?s <http://example.org/p> ?o }",
        );
        assert!(
            has_bnode_in_delete_error(&diags),
            "Modify DELETE-template blank nodes stay rejected: {diags:?}"
        );
        let diags = validate_with(&caps, "DELETE WHERE { GRAPH ?g { ?s ?p ?o } }");
        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedGraphInUpdate),
            "graph variables stay rejected in DELETE WHERE: {diags:?}"
        );
        let diags = validate_with(
            &caps,
            "DELETE DATA { <urn:s> <http://example.org/p> <urn:o> {| <http://example.org/q> 1 |} . }",
        );
        assert!(
            diags
                .iter()
                .any(|d| d.code == DiagCode::AnonymousAnnotationInGroundData && d.is_error()),
            "anonymous annotations stay rejected in DELETE DATA: {diags:?}"
        );
    }

    #[cfg(feature = "lowering")]
    #[test]
    fn test_stable_prefix_stays_in_sync_with_core() {
        // The validator can't depend on fluree-db-core unconditionally (the
        // dependency is feature-gated), so the prefix is duplicated. Keep it
        // honest whenever the lowering feature is compiled in.
        assert_eq!(
            STABLE_BLANK_NODE_LABEL_PREFIX,
            fluree_db_core::ns_encoding::STABLE_BLANK_NODE_LABEL_PREFIX
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
    fn test_property_path_negated_now_valid() {
        // Negated property sets are now supported (lowered to a fresh-predicate
        // triple + FILTER); the validator no longer rejects them.
        let diags = validate_query("SELECT * WHERE { ?s !ex:hidden ?o }");
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedNegatedPropertySet),
            "Negated property sets are supported and should validate"
        );
    }

    #[test]
    fn test_property_path_negated_set_now_valid() {
        let diags = validate_query("SELECT * WHERE { ?s !(ex:a|ex:b) ?o }");
        assert!(
            !diags
                .iter()
                .any(|d| d.code == DiagCode::UnsupportedNegatedPropertySet),
            "Negated property sets are supported and should validate"
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
        // Grammar: UsingClause* comes BEFORE the WHERE clause. The previous
        // input put USING after WHERE, where it was unparseable trailing
        // input — the recovered AST contained no USING clause at all, so the
        // assertion was vacuous (and once trailing input started suppressing
        // AST production, the vacuity surfaced as a parse failure here).
        let diags = validate_query(
            "DELETE { ?s ex:p ?o } USING <http://example.org/graph> WHERE { ?s ex:p ?o }",
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
    // V3 — blank-node label scope tests (SPARQL 1.1 §19.6)
    // =========================================================================

    fn has_code(diags: &[Diagnostic], code: DiagCode) -> bool {
        diags.iter().any(|d| d.code == code && d.is_error())
    }

    #[test]
    fn test_bnode_scope_same_bgp_valid() {
        // Same label twice in one basic graph pattern is fine.
        let diags = validate_query("SELECT * WHERE { _:a ex:p ?v . _:a ex:q 1 }");
        assert!(
            !has_code(&diags, DiagCode::BlankNodeLabelCrossScope),
            "{diags:?}"
        );
    }

    #[test]
    fn test_bnode_scope_across_filter_valid() {
        // FILTER does not end a basic graph pattern (§18.2.2.5).
        let diags = validate_query("SELECT * WHERE { _:a ex:p ?v FILTER(?v > 1) _:a ex:q 1 }");
        assert!(
            !has_code(&diags, DiagCode::BlankNodeLabelCrossScope),
            "{diags:?}"
        );
    }

    #[test]
    fn test_bnode_scope_across_optional_rejected() {
        let diags = validate_query("SELECT * WHERE { _:a ex:p ?v OPTIONAL { _:a ex:q 1 } }");
        assert!(has_code(&diags, DiagCode::BlankNodeLabelCrossScope));
    }

    #[test]
    fn test_bnode_scope_across_nested_group_rejected() {
        let diags = validate_query("SELECT * WHERE { _:a ?p ?v . { _:a ?q 1 } }");
        assert!(has_code(&diags, DiagCode::BlankNodeLabelCrossScope));
    }

    #[test]
    fn test_bnode_scope_across_union_rejected() {
        let diags = validate_query("SELECT * WHERE { { _:a ex:p ?v } UNION { _:a ex:q 1 } }");
        assert!(has_code(&diags, DiagCode::BlankNodeLabelCrossScope));
    }

    #[test]
    fn test_bnode_scope_boundary_breaks_bgp_rejected() {
        // Reuse in the SAME group but across a GRAPH boundary: the GRAPH
        // pattern ends the first BGP, so the second `_:a` is a new BGP.
        let diags = validate_query("SELECT * WHERE { _:a ?p ?v . GRAPH ?g { ?s ?p ?v } _:a ?q 1 }");
        assert!(has_code(&diags, DiagCode::BlankNodeLabelCrossScope));
    }

    #[test]
    fn test_bnode_scope_distinct_labels_valid() {
        let diags = validate_query("SELECT * WHERE { _:a ex:p ?v OPTIONAL { _:b ex:q 1 } }");
        assert!(
            !has_code(&diags, DiagCode::BlankNodeLabelCrossScope),
            "{diags:?}"
        );
    }

    #[test]
    fn test_bnode_scope_anon_bnodes_valid() {
        // `[]` anonymous blank nodes have no label and are never flagged.
        let diags = validate_query("SELECT * WHERE { [] ex:p ?v OPTIONAL { [] ex:q 1 } }");
        assert!(
            !has_code(&diags, DiagCode::BlankNodeLabelCrossScope),
            "{diags:?}"
        );
    }

    // =========================================================================
    // V4 — GROUP BY / aggregate projection-scope tests (SPARQL 1.1 §11)
    // =========================================================================

    #[test]
    fn test_projection_star_with_group_by_rejected() {
        let diags = validate_query("SELECT * { ?s ?p ?o } GROUP BY ?s");
        assert!(has_code(&diags, DiagCode::SelectStarWithGroupBy));
    }

    #[test]
    fn test_projection_ungrouped_var_rejected() {
        let diags = validate_query("SELECT ?o { ?s ?p ?o } GROUP BY ?s");
        assert!(has_code(&diags, DiagCode::UngroupedVariableInProjection));
    }

    #[test]
    fn test_projection_group_key_and_aggregate_valid() {
        let diags = validate_query("SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s");
        assert!(
            !has_code(&diags, DiagCode::UngroupedVariableInProjection),
            "{diags:?}"
        );
    }

    #[test]
    fn test_projection_implicit_group_ungrouped_var_rejected() {
        // agg10 shape: an aggregate in the projection groups implicitly;
        // ?p is then neither a key nor aggregated.
        let diags = validate_query("SELECT ?p (COUNT(?o) AS ?c) WHERE { ?s ?p ?o }");
        assert!(has_code(&diags, DiagCode::UngroupedVariableInProjection));
    }

    #[test]
    fn test_projection_expression_key_vars_not_licensed() {
        // agg08 shape: GROUP BY (?a + ?b) — the *expression* is the key,
        // not its variables.
        let diags = validate_query(
            "SELECT ((?a + ?b) AS ?ab) (COUNT(?a) AS ?c) WHERE { ?s ?p ?a, ?b } GROUP BY (?a + ?b)",
        );
        assert!(has_code(&diags, DiagCode::UngroupedVariableInProjection));
    }

    #[test]
    fn test_projection_bracketed_var_key_valid() {
        // GROUP BY (?s) — a bracketed bare variable counts as the key ?s.
        let diags = validate_query("SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY (?s)");
        assert!(
            !has_code(&diags, DiagCode::UngroupedVariableInProjection),
            "{diags:?}"
        );
    }

    #[test]
    fn test_projection_group_by_alias_valid() {
        let diags = validate_query(
            "SELECT ?key (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY (STR(?s) AS ?key)",
        );
        assert!(
            !has_code(&diags, DiagCode::UngroupedVariableInProjection),
            "{diags:?}"
        );
    }

    #[test]
    fn test_projection_earlier_alias_usable_in_later_expression() {
        let diags = validate_query(
            "SELECT (SUM(?x) AS ?sum) ((?sum + 1) AS ?sump) WHERE { ?s ?p ?x } GROUP BY ?s",
        );
        assert!(
            !has_code(&diags, DiagCode::UngroupedVariableInProjection),
            "{diags:?}"
        );
    }

    #[test]
    fn test_projection_ungrouped_query_unaffected() {
        let diags = validate_query("SELECT ?s ?o WHERE { ?s ?p ?o }");
        assert!(
            !has_code(&diags, DiagCode::UngroupedVariableInProjection)
                && !has_code(&diags, DiagCode::SelectStarWithGroupBy),
            "{diags:?}"
        );
    }

    #[test]
    fn test_projection_subselect_checked() {
        let diags =
            validate_query("SELECT ?s WHERE { { SELECT ?o { ?s ?p ?o } GROUP BY ?s } ?s ?p ?o2 }");
        assert!(has_code(&diags, DiagCode::UngroupedVariableInProjection));
    }

    // =========================================================================
    // V6 — SELECT alias tests (SPARQL 1.1 §19.8 note 13)
    // =========================================================================

    #[test]
    fn test_select_alias_duplicate_rejected() {
        // W3C syn-bad-03 (test_45).
        let diags = validate_query("SELECT (1 AS ?X) (1 AS ?X) {}");
        assert!(has_code(&diags, DiagCode::SelectAliasAlreadyBound));
    }

    #[test]
    fn test_select_alias_in_scope_via_subselect_rejected() {
        // W3C syntax-SELECTscope2 (test_65): the sub-SELECT projects ?X
        // into the outer WHERE pattern's scope.
        let diags = validate_query("SELECT (1 AS ?X) { SELECT (2 AS ?X) {} }");
        assert!(has_code(&diags, DiagCode::SelectAliasAlreadyBound));
    }

    #[test]
    fn test_select_alias_in_scope_via_pattern_rejected() {
        let diags = validate_query("SELECT ((?x + 1) AS ?y) WHERE { ?x ex:p ?y }");
        assert!(has_code(&diags, DiagCode::SelectAliasAlreadyBound));
    }

    #[test]
    fn test_select_alias_fresh_variable_valid() {
        let diags = validate_query("SELECT ((?x + 1) AS ?y) WHERE { ?x ex:p ?o }");
        assert!(
            !has_code(&diags, DiagCode::SelectAliasAlreadyBound),
            "{diags:?}"
        );
    }

    #[test]
    fn test_select_alias_parallel_subselects_valid() {
        // W3C syntax-SELECTscope1/3 (positive): each sub-SELECT clause is
        // checked against its own (empty) pattern; the outer SELECT * makes
        // no assignments.
        let diags =
            validate_query("SELECT * WHERE { { SELECT (1 AS ?X) {} } { SELECT (1 AS ?X) {} } }");
        assert!(
            !has_code(&diags, DiagCode::SelectAliasAlreadyBound),
            "{diags:?}"
        );
    }

    #[test]
    fn test_select_alias_chained_use_valid() {
        // Later expressions may USE an earlier alias — only re-ASSIGNMENT
        // is an error.
        let diags = validate_query("SELECT (1 AS ?x) ((?x + 1) AS ?y) WHERE {}");
        assert!(
            !has_code(&diags, DiagCode::SelectAliasAlreadyBound),
            "{diags:?}"
        );
    }

    // =========================================================================
    // SPARQL 1.2 — nested aggregates + duplicated VALUES variables
    // =========================================================================

    #[test]
    fn test_nested_aggregate_rejected() {
        // W3C sparql12 nested-aggregate-functions.
        let diags = validate_query("SELECT (COUNT(COUNT(*)) AS ?c) WHERE {}");
        assert!(has_code(&diags, DiagCode::NestedAggregate));
    }

    #[test]
    fn test_nested_aggregate_under_arithmetic_rejected() {
        let diags = validate_query("SELECT (SUM(1 + MAX(?x)) AS ?c) WHERE { ?s ?p ?x }");
        assert!(has_code(&diags, DiagCode::NestedAggregate));
    }

    #[test]
    fn test_flat_aggregates_valid() {
        let diags = validate_query(
            "SELECT (COUNT(?x) AS ?c) ((MIN(?x) + MAX(?x)) AS ?range) WHERE { ?s ?p ?x }",
        );
        assert!(!has_code(&diags, DiagCode::NestedAggregate), "{diags:?}");
    }

    #[test]
    fn test_nested_aggregate_in_having_rejected() {
        let diags =
            validate_query("SELECT ?s WHERE { ?s ?p ?x } GROUP BY ?s HAVING (SUM(AVG(?x)) > 1)");
        assert!(has_code(&diags, DiagCode::NestedAggregate));
    }

    #[test]
    fn test_duplicate_values_variable_rejected() {
        // W3C sparql12 duplicated-values-variable.
        let diags = validate_query("SELECT * WHERE { VALUES (?a ?a) { (1 1) } }");
        assert!(has_code(&diags, DiagCode::DuplicateValuesVariable));
    }

    #[test]
    fn test_duplicate_values_variable_postclause_rejected() {
        let diags = validate_query("SELECT * WHERE { ?s ?p ?o } VALUES (?a ?a) { (1 1) }");
        assert!(has_code(&diags, DiagCode::DuplicateValuesVariable));
    }

    #[test]
    fn test_distinct_values_variables_valid() {
        let diags = validate_query("SELECT * WHERE { VALUES (?a ?b) { (1 1) } }");
        assert!(
            !has_code(&diags, DiagCode::DuplicateValuesVariable),
            "{diags:?}"
        );
    }

    // =========================================================================
    // Anonymous annotation in DELETE DATA (SPARQL 1.2 negative update syntax)
    // =========================================================================

    #[test]
    fn test_delete_data_anonymous_annotation_rejected() {
        // W3C sparql12 syntax-update-anonreifier-02 (first operation).
        let diags = validate_query(
            "PREFIX : <http://example.com/ns#> DELETE DATA { :s :p :o1 {| :added 'Test' |} }",
        );
        assert!(has_code(&diags, DiagCode::AnonymousAnnotationInGroundData));
    }

    #[test]
    fn test_delete_data_named_reifier_annotation_valid() {
        // An explicit IRI reifier is addressable — allowed.
        let diags = validate_query(
            "PREFIX : <http://example.com/ns#> DELETE DATA { :s :p :o1 ~ :r {| :added 'Test' |} }",
        );
        assert!(
            !has_code(&diags, DiagCode::AnonymousAnnotationInGroundData),
            "{diags:?}"
        );
    }

    #[test]
    fn test_insert_data_anonymous_annotation_still_allowed() {
        // Fluree's committed SPARQL 1.2 transact surface (reviewed
        // divergence): anonymous annotation blocks mint a fresh reifier in
        // INSERT DATA.
        let diags = validate_query(
            "PREFIX : <http://example.com/ns#> INSERT DATA { :s :p :o2 {| :added 'Test' |} }",
        );
        assert!(
            !has_code(&diags, DiagCode::AnonymousAnnotationInGroundData),
            "{diags:?}"
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
}
