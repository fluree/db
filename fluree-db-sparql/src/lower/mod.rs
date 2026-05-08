//! SPARQL Lowering.
//!
//! This module lowers the SPARQL AST to the query algebra defined in
//! `fluree-db-query`. This involves:
//!
//! - Prefix expansion (resolving prefixed names to full IRIs)
//! - IRI encoding via `IriEncoder` trait
//! - Variable registration via `VarRegistry`
//! - Graph pattern lowering to `Pattern`
//! - Expression lowering to `Expression`
//!
//! ## Usage
//!
//! ```
//! use fluree_db_sparql::{parse_sparql, lower_sparql};
//! use fluree_db_query::parse::encode::MemoryEncoder;
//! use fluree_db_query::var_registry::VarRegistry;
//!
//! let output = parse_sparql("SELECT ?name WHERE { ?s <http://example.org/name> ?name }");
//! let ast = output.ast.unwrap();
//!
//! let mut encoder = MemoryEncoder::with_common_namespaces();
//! encoder.add_namespace("http://example.org/", 100);
//! let mut vars = VarRegistry::new();
//! let query = lower_sparql(&ast, &encoder, &mut vars).unwrap();
//! ```
//!
//! ## Integration
//!
//! After lowering, the query can be executed by the existing
//! `fluree-db-query` execution pipeline.
//!
//! ## Module Structure
//!
//! Lowering logic is split across focused submodules:
//!
//! - [`term`] — Variables, IRIs, literals, blank nodes
//! - [`expression`] — Filter expressions and function calls
//! - [`aggregate`] — Aggregate extraction and HAVING
//! - [`pattern`] — Graph pattern dispatch (BGP, OPTIONAL, UNION, etc.)
//! - [`path`] — Property path lowering
//! - [`rdf_star`] — RDF-star quoted triple expansion
//! - [`ask`] — ASK query lowering
//! - [`construct`] — CONSTRUCT query lowering
//! - [`select`] — SELECT clause, solution modifiers, subqueries

mod aggregate;
mod ask;
mod construct;
mod describe;
mod error;
mod expression;
mod path;
mod pattern;
mod rdf_star;
mod select;
mod term;

pub use error::{LowerError, Result};

use crate::ast::query::{QueryBody, SelectVariables, SparqlAst};

use fluree_db_query::ir::Pattern;
use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::{VarId, VarRegistry};

use fluree_graph_json_ld::{parse_context, ParsedContext};

use std::collections::HashMap;
use std::sync::Arc;

/// Lower a SPARQL AST to a Query.
///
/// This produces a `Query` that can be directly executed by the
/// fluree-db-query engine via `ExecutableQuery`.
///
/// # Arguments
///
/// * `ast` - The parsed SPARQL AST
/// * `encoder` - IRI encoder for converting IRIs to Sids
/// * `vars` - Variable registry (caller provides to enable sharing across subqueries)
///
/// # Returns
///
/// A `Query` ready for execution, or a `LowerError`.
pub fn lower_sparql<E: IriEncoder>(
    ast: &SparqlAst,
    encoder: &E,
    vars: &mut VarRegistry,
) -> Result<Query> {
    lower_sparql_with_source(ast, encoder, vars, None)
}

/// Lower a SPARQL AST to a Query, with optional source text for SERVICE body capture.
///
/// When `source_text` is provided, SERVICE patterns will capture the original SPARQL
/// text for their body, enabling remote execution without an IR-to-SPARQL serializer.
pub fn lower_sparql_with_source<E: IriEncoder>(
    ast: &SparqlAst,
    encoder: &E,
    vars: &mut VarRegistry,
    source_text: Option<&str>,
) -> Result<Query> {
    let span = tracing::debug_span!("sparql_lower");
    let _guard = span.enter();

    tracing::debug!("lowering SPARQL AST to query algebra");

    let mut ctx = LoweringContext::new(ast, encoder, vars, source_text);
    let result = ctx.lower();

    match &result {
        Ok(query) => {
            tracing::debug!(
                pattern_count = query.patterns.len(),
                var_count = vars.len(),
                "SPARQL lowering completed"
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "SPARQL lowering failed");
        }
    }

    result
}

/// Context for lowering operations.
///
/// Maintains prefix mappings and provides methods for lowering each AST type.
/// Methods are split across submodules but all operate on this shared context.
struct LoweringContext<'a, E> {
    ast: &'a SparqlAst,
    encoder: &'a E,
    vars: &'a mut VarRegistry,
    /// Prefix → IRI namespace mapping from prologue
    prefixes: HashMap<Arc<str>, Arc<str>>,
    /// Base IRI for relative IRI resolution
    base: Option<Arc<str>>,
    /// Aggregate expression → alias variable mapping (for HAVING)
    aggregate_aliases: Option<HashMap<String, VarId>>,
    /// Cache of lowered aggregate-input expressions that have been desugared to a
    /// pre-aggregation `BIND(expr AS ?__agg_expr_N)` variable.
    ///
    /// Key is a span-free structural string of the input expression.
    agg_expr_binds: HashMap<String, VarId>,
    /// Monotonic counter for generating aggregate-input bind variables (`?__agg_expr_0`, `?__agg_expr_1`, …).
    agg_counter: u32,
    /// Monotonic counter for generating intermediate property-path join variables (`?__pp0`, `?__pp1`, …).
    pp_counter: u32,
    /// Original SPARQL source text (for extracting SERVICE body text).
    source_text: Option<&'a str>,
}

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    fn new(
        ast: &'a SparqlAst,
        encoder: &'a E,
        vars: &'a mut VarRegistry,
        source_text: Option<&'a str>,
    ) -> Self {
        // Build prefix map from prologue
        let mut prefixes = HashMap::new();
        for decl in &ast.prologue.prefixes {
            prefixes.insert(decl.prefix.clone(), decl.iri.clone());
        }

        // Get base IRI
        let base = ast.prologue.base.as_ref().map(|b| b.iri.clone());

        Self {
            ast,
            encoder,
            vars,
            prefixes,
            base,
            aggregate_aliases: None,
            agg_expr_binds: HashMap::new(),
            agg_counter: 0,
            pp_counter: 0,
            source_text,
        }
    }

    /// Main entry point for lowering.
    fn lower(&mut self) -> Result<Query> {
        match &self.ast.body {
            QueryBody::Select(select_query) => {
                // Lower WHERE clause patterns
                let mut patterns = self.lower_graph_pattern(&select_query.where_clause.pattern)?;

                // Lower SELECT clause to get selected variables
                let select = self.lower_select_clause(&select_query.select)?;

                // Aggregate aliases referenced by SELECT expressions (for post-aggregation binds)
                let aggregate_aliases = self.collect_aggregate_alias_names(&select_query.select);

                // Lower SELECT expression bindings (e.g., SELECT (SHA512(?x) AS ?hash))
                let select_binds =
                    self.lower_select_expression_binds(&select_query.select, &aggregate_aliases)?;
                patterns.extend(select_binds.pre);

                // Lower post-query VALUES clause.  Stored in `post_values` (not
                // in `patterns`) so the WHERE-clause planner cannot reorder it
                // relative to OPTIONAL/UNION.  Applied after the WHERE tree.
                let post_values = if let Some(ref values_pattern) = select_query.values {
                    let mut values_ir = self.lower_graph_pattern(values_pattern)?;
                    // lower_graph_pattern returns a Vec; post-query VALUES is always exactly one Pattern::Values.
                    if values_ir.len() == 1 && matches!(values_ir[0], Pattern::Values { .. }) {
                        Some(values_ir.remove(0))
                    } else {
                        // Fallback: shouldn't happen, but keep patterns inline.
                        patterns.extend(values_ir);
                        None
                    }
                } else {
                    None
                };

                // Lower solution modifiers to QueryOptions.
                // Expression-based GROUP BY produces pre-group BINDs that must be
                // injected into the WHERE pattern list before query building.
                let lowered_modifiers =
                    self.lower_solution_modifiers(&select_query.modifiers, &select_query.select)?;
                patterns.extend(lowered_modifiers.pre_group_binds);
                let options = lowered_modifiers.options;
                let distinct = lowered_modifiers.distinct;

                // Assemble the grouping phase from the lowered components.
                // Aggregation only exists when there are aggregates; post-aggregation
                // binds (`select_binds.post`) live inside it.
                let aggregation = fluree_db_core::NonEmpty::try_from_vec(
                    lowered_modifiers.aggregates,
                )
                .map(|aggregates| fluree_db_query::ir::Aggregation {
                    aggregates,
                    binds: select_binds.post,
                });
                let grouping = if let Some(group_by) =
                    fluree_db_core::NonEmpty::try_from_vec(lowered_modifiers.group_by)
                {
                    Some(fluree_db_query::ir::Grouping::Explicit {
                        group_by,
                        aggregation,
                        having: lowered_modifiers.having,
                    })
                } else if let Some(aggregation) = aggregation {
                    Some(fluree_db_query::ir::Grouping::Implicit {
                        aggregation,
                        having: lowered_modifiers.having,
                    })
                } else {
                    None
                };

                // Build a JSON-LD-like context from SPARQL prologue prefixes so formatters can compact IRIs.
                let ctx = self.build_jsonld_context()?;

                // SELECT * should behave like "wildcard select" for JSON-LD-style outputs.
                // This lets formatters emit object rows keyed by variable name.
                //
                // SPARQL solution sequences are tabular by spec — every
                // `SELECT ?x` is a sequence of single-column rows, not a list
                // of bare values. Projection shape is `Tuple` (the default of
                // the `select`/`select_one` helpers).
                let output = match (&select_query.select.variables, distinct) {
                    (SelectVariables::Star, _) => QueryOutput::wildcard(),
                    (_, true) => QueryOutput::select_distinct(select),
                    (_, false) => QueryOutput::select_all(select),
                };

                Ok(Query {
                    context: ctx,
                    orig_context: None, // SPARQL doesn't originate from JSON context
                    output,
                    patterns,
                    grouping,
                    options,
                    post_values,
                })
            }
            QueryBody::Construct(construct_query) => self.lower_construct(construct_query),
            QueryBody::Ask(ask_query) => self.lower_ask(ask_query),
            QueryBody::Describe(describe_query) => self.lower_describe(describe_query),
            QueryBody::Update(_) => Err(LowerError::unsupported_form("UPDATE", self.ast.span)),
        }
    }

    /// Build a JSON-LD ParsedContext from the SPARQL prologue.
    ///
    /// This is used only for result formatting (IRI compaction), not for parsing.
    /// Delegates to `build_jsonld_context_value` for the raw JSON value, then
    /// parses it into a `ParsedContext`.
    fn build_jsonld_context(&self) -> Result<ParsedContext> {
        let value = self.build_jsonld_context_value();
        // Context parsing should not fail for simple prefix maps; if it does,
        // fall back to an empty context (formatters will emit full IRIs).
        match parse_context(&value) {
            Ok(ctx) => Ok(ctx),
            Err(_) => Ok(ParsedContext::default()),
        }
    }

    /// Build a JSON-LD context object from the SPARQL prologue.
    fn build_jsonld_context_value(&self) -> serde_json::Value {
        use serde_json::{Map, Value as JsonValue};

        let mut obj = Map::new();

        // BASE becomes @base and (for parity) @vocab when present.
        if let Some(base) = &self.base {
            obj.insert(
                "@base".to_string(),
                JsonValue::String(base.as_ref().to_string()),
            );
            obj.insert(
                "@vocab".to_string(),
                JsonValue::String(base.as_ref().to_string()),
            );
        }

        // PREFIX declarations map directly to JSON-LD prefix entries.
        for (prefix, iri) in &self.prefixes {
            obj.insert(
                prefix.as_ref().to_string(),
                JsonValue::String(iri.as_ref().to_string()),
            );
        }

        JsonValue::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_sparql;
    use fluree_db_query::ir::{AggregateFn, AggregateSpec};
    use fluree_db_query::ir::triple::{Ref, Term};
    use fluree_db_query::ir::{Expression, Grouping, PathModifier, Pattern};
    use fluree_db_query::parse::encode::MemoryEncoder;
    use fluree_db_query::sort::SortDirection;
    use fluree_db_query::var_registry::VarId;

    /// View aggregates of a lowered Query as a flat Vec of references.
    fn aggregates_of(query: &Query) -> Vec<&AggregateSpec> {
        query
            .grouping
            .iter()
            .flat_map(Grouping::aggregates)
            .collect()
    }

    /// View GROUP BY keys of a lowered Query.
    fn group_by_of(query: &Query) -> Vec<VarId> {
        match &query.grouping {
            Some(Grouping::Explicit { group_by, .. }) => group_by.iter().copied().collect(),
            _ => Vec::new(),
        }
    }

    /// View HAVING expression of a lowered Query.
    fn having_of(query: &Query) -> Option<&Expression> {
        query.grouping.as_ref().and_then(Grouping::having)
    }

    fn test_encoder() -> MemoryEncoder {
        let mut encoder = MemoryEncoder::with_common_namespaces();
        encoder.add_namespace("http://example.org/", 100);
        encoder.add_namespace("http://schema.org/", 101);
        encoder.add_namespace("http://xmlns.com/foaf/0.1/", 102);
        encoder
    }

    fn lower_query(sparql: &str) -> Result<Query> {
        lower_query_with_vars(sparql).map(|(q, _)| q)
    }

    fn lower_query_with_vars(sparql: &str) -> Result<(Query, VarRegistry)> {
        let output = parse_sparql(sparql);
        assert!(
            output.ast.is_some(),
            "Parse failed: {:?}",
            output.diagnostics
        );
        let ast = output.ast.unwrap();
        let encoder = test_encoder();
        let mut vars = VarRegistry::new();
        let query = lower_sparql(&ast, &encoder, &mut vars)?;
        Ok((query, vars))
    }

    // =========================================================================
    // Basic SELECT tests
    // =========================================================================

    #[test]
    fn test_simple_select() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE { ?s ex:name ?name }",
        )
        .unwrap();

        assert_eq!(query.output.projected_vars().unwrap().len(), 2);
        assert_eq!(query.patterns.len(), 1);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
    }

    #[test]
    fn test_select_star() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ?s ex:name ?name }",
        )
        .unwrap();

        // SELECT * should produce Wildcard projection
        assert!(query.output.is_wildcard());
    }

    #[test]
    fn test_multiple_patterns() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name ?age WHERE {
               ?s ex:name ?name .
               ?s ex:age ?age
             }",
        )
        .unwrap();

        assert_eq!(query.output.projected_vars().unwrap().len(), 3);
        assert_eq!(query.patterns.len(), 2);
    }

    // =========================================================================
    // Pattern tests
    // =========================================================================

    #[test]
    fn test_optional_pattern() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name ?age WHERE {
               ?s ex:name ?name .
               OPTIONAL { ?s ex:age ?age }
             }",
        )
        .unwrap();

        // Should have: Triple, Optional
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[1], Pattern::Optional(_)));
    }

    #[test]
    fn test_union_pattern() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE {
               { ?s ex:firstName ?name } UNION { ?s ex:lastName ?name }
             }",
        )
        .unwrap();

        // Should have a Union pattern
        assert!(!query.patterns.is_empty());
        let has_union = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Union(_)));
        assert!(has_union, "Expected Union pattern");
    }

    #[test]
    fn test_filter_pattern() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?age WHERE {
               ?s ex:age ?age .
               FILTER(?age > 18)
             }",
        )
        .unwrap();

        // Should have: Triple, Filter
        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter pattern");
    }

    #[test]
    fn test_bind_pattern() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name ?upper WHERE {
               ?s ex:name ?name .
               BIND(UCASE(?name) AS ?upper)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern");
    }

    #[test]
    fn test_bind_pattern_unbound_predicate() {
        // Regression: BIND must be preserved when triple has unbound predicate (?p).
        // Uses no space around + (i.e. ?o+10) to test that the lexer correctly
        // tokenizes + as a separate Plus operator rather than consuming it as
        // part of a signed integer literal.
        let query = lower_query(
            "PREFIX : <http://example.org/>
             SELECT ?z
             {
               ?s ?p ?o .
               BIND(?o+10 AS ?z)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(
            has_bind,
            "Expected Bind pattern in lowered query, got patterns: {:?}",
            query.patterns
        );
    }

    #[test]
    fn test_bind10_scoping() {
        // W3C bind10: BIND(4 AS ?z) { ?s :p ?v . FILTER(?v = ?z) }
        // The inner { } creates a scope boundary; ?z from outer BIND is
        // NOT visible inside. The inner group should lower to a Subquery
        // whose SELECT does NOT include ?z.
        let query = lower_query(
            "PREFIX : <http://example.org/>
             SELECT ?s ?v ?z
             {
               BIND(4 AS ?z)
               {
                 ?s :p ?v . FILTER(?v = ?z)
               }
             }",
        )
        .unwrap();

        // Should have a Bind and a Subquery
        assert!(
            query
                .patterns
                .iter()
                .any(|p| matches!(p, Pattern::Bind { .. })),
            "Expected Bind pattern"
        );
        let subquery = query
            .patterns
            .iter()
            .find(|p| matches!(p, Pattern::Subquery(_)));
        assert!(
            subquery.is_some(),
            "Expected Subquery pattern for nested group"
        );

        if let Some(Pattern::Subquery(sq)) = subquery {
            // ?z should NOT be in the subquery's select (it's only in FILTER, not bound)
            let z_var = query
                .patterns
                .iter()
                .find_map(|p| {
                    if let Pattern::Bind { var, .. } = p {
                        Some(*var)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert!(
                !sq.select.contains(&z_var),
                "Subquery SELECT should NOT contain ?z (VarId {:?}), but got select: {:?}",
                z_var,
                sq.select
            );
        }
    }

    #[test]
    fn test_values_pattern() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?x WHERE {
               ?s ex:value ?x .
               VALUES ?x { 1 2 3 }
             }",
        )
        .unwrap();

        let has_values = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Values { .. }));
        assert!(has_values, "Expected Values pattern");
    }

    // =========================================================================
    // Expression tests
    // =========================================================================

    #[test]
    fn test_comparison_operators() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?age WHERE {
               ?s ex:age ?age .
               FILTER(?age >= 18 && ?age <= 65)
             }",
        )
        .unwrap();

        // Should parse and lower the AND of comparisons
        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter);
    }

    #[test]
    fn test_function_calls() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE {
               ?s ex:name ?name .
               FILTER(STRLEN(?name) > 5)
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter);
    }

    // =========================================================================
    // Prefix resolution tests
    // =========================================================================

    #[test]
    fn test_prefix_resolution() {
        let query = lower_query(
            "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
             SELECT ?s ?name WHERE { ?s foaf:name ?name }",
        )
        .unwrap();

        // Should successfully resolve foaf prefix
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Triple(tp) = &query.patterns[0] {
            if let Ref::Sid(sid) = &tp.p {
                assert_eq!(sid.namespace_code, 102); // foaf namespace
                assert_eq!(sid.name.as_ref(), "name");
            } else {
                panic!("Expected Sid for predicate");
            }
        } else {
            panic!("Expected Triple pattern");
        }
    }

    #[test]
    fn test_undefined_prefix_error() {
        let result = lower_query("SELECT ?s ?name WHERE { ?s unknown:name ?name }");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LowerError::UndefinedPrefix { .. }
        ));
    }

    #[test]
    fn test_unknown_namespace_produces_fallback_sid() {
        // An unregistered namespace is benign: lowering keeps it as a raw IRI
        // so execution can still handle cross-ledger / non-local references.
        let query = lower_query(
            "PREFIX other: <http://other.example.org/>
             SELECT ?s ?name WHERE { ?s other:name ?name }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Triple(tp) = &query.patterns[0] {
            if let Ref::Iri(iri) = &tp.p {
                assert_eq!(iri.as_ref(), "http://other.example.org/name");
            } else {
                panic!("Expected raw IRI for predicate");
            }
        } else {
            panic!("Expected Triple pattern");
        }
    }

    #[test]
    fn test_misused_prefix_syntax_error() {
        // Common mistake: wrapping prefixed name in angle brackets <prefix:local>
        // This should give a helpful error message explaining the issue
        let result = lower_query(
            "PREFIX hsc: <http://example.org/schema/>
             SELECT ?name WHERE { <hsc:product/123> hsc:name ?name }",
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LowerError::MisusedPrefixSyntax { .. }),
            "Expected MisusedPrefixSyntax error, got: {err:?}"
        );

        // Verify the error message contains helpful information
        let msg = err.to_string();
        assert!(
            msg.contains("angle brackets"),
            "Error should mention angle brackets: {msg}"
        );
        assert!(
            msg.contains("http://example.org/schema/product/123"),
            "Error should show the expanded IRI: {msg}"
        );
    }

    #[test]
    fn test_prefixed_name_with_slash() {
        // Test that prefixed names with '/' in the local part work
        // e.g., hsc:product/123 should expand to http://example.org/schema/product/123
        let result = lower_query(
            "PREFIX hsc: <http://example.org/>
             SELECT ?name WHERE { hsc:product/123 hsc:name ?name }",
        );

        // If this fails at parse time, we need to update the lexer
        // If this fails at lowering time, we need to check prefix expansion
        match &result {
            Ok(query) => {
                // Success - verify the pattern was created
                assert_eq!(query.patterns.len(), 1);
                println!("Prefixed name with slash works!");
            }
            Err(e) => {
                println!("Prefixed name with slash failed: {e:?}");
                panic!("Expected prefixed name with '/' to work, got error: {e}");
            }
        }
    }

    // =========================================================================
    // MINUS Pattern Tests
    // =========================================================================

    #[test]
    fn test_minus_pattern() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:a ?o MINUS { ?s ex:b ?o } }",
        )
        .unwrap();

        // Should have Triple, Minus patterns
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Minus(_)));

        // Check the MINUS contains a triple
        if let Pattern::Minus(inner) = &query.patterns[1] {
            assert_eq!(inner.len(), 1);
            assert!(matches!(inner[0], Pattern::Triple(_)));
        } else {
            panic!("Expected Minus pattern");
        }
    }

    // =========================================================================
    // EXISTS/NOT EXISTS Tests
    // =========================================================================

    #[test]
    fn test_filter_exists() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:a ?o . FILTER EXISTS { ?s ex:b ?val } }",
        )
        .unwrap();

        // Should have Triple, Exists patterns
        let has_exists = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Exists(_)));
        assert!(has_exists, "Expected Exists pattern");
    }

    #[test]
    fn test_filter_not_exists() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:a ?o . FILTER NOT EXISTS { ?s ex:deleted true } }",
        )
        .unwrap();

        // Should have Triple, NotExists patterns
        let has_not_exists = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::NotExists(_)));
        assert!(has_not_exists, "Expected NotExists pattern");
    }

    #[test]
    fn test_filter_not_exists_unknown_predicate_does_not_error() {
        // Regression guard: unknown predicate IRIs must not fail lowering.
        // They should remain as raw IRIs and naturally produce no matches at runtime.
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:a ?o .
               FILTER NOT EXISTS { ?s <http://unknown.example/ns/does-not-exist> ?x }
             }",
        )
        .unwrap();

        let mut saw_not_exists = false;
        for p in &query.patterns {
            if let Pattern::NotExists(inner) = p {
                saw_not_exists = true;
                assert!(
                    inner.iter().any(|ip| matches!(ip, Pattern::Triple(_))),
                    "Expected NOT EXISTS inner pattern to contain a triple"
                );

                for ip in inner {
                    if let Pattern::Triple(tp) = ip {
                        if let Ref::Iri(iri) = &tp.p {
                            if iri.as_ref() == "http://unknown.example/ns/does-not-exist" {
                                return;
                            }
                        }
                    }
                }
            }
        }

        assert!(saw_not_exists, "Expected NotExists pattern");
        panic!("Expected NOT EXISTS predicate to remain as raw IRI");
    }

    // =========================================================================
    // New Function Tests
    // =========================================================================

    #[test]
    fn test_string_functions_regex() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE { ?s ex:name ?name . FILTER(REGEX(?name, \"^A\")) }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter pattern with REGEX");
    }

    #[test]
    fn test_string_functions_concat() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?full WHERE {
               ?s ex:first ?f .
               ?s ex:last ?l .
               BIND(CONCAT(?f, \" \", ?l) AS ?full)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with CONCAT");
    }

    #[test]
    fn test_string_functions_strbefore_strafter() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:name ?name . FILTER(STRBEFORE(?name, \"@\") != \"\") }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter);

        let query2 = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:name ?name . FILTER(STRAFTER(?name, \"@\") != \"\") }",
        )
        .unwrap();

        let has_filter2 = query2
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter2);
    }

    #[test]
    fn test_string_functions_replace() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?clean WHERE {
               ?s ex:text ?text .
               BIND(REPLACE(?text, \"\\\\s+\", \" \") AS ?clean)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with REPLACE");
    }

    #[test]
    fn test_datetime_functions() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:created ?dt .
               FILTER(YEAR(?dt) = 2024)
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter with YEAR function");
    }

    #[test]
    fn test_datetime_function_now() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:expires ?dt .
               FILTER(?dt > NOW())
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter with NOW function");
    }

    #[test]
    fn test_rdf_term_functions_lang() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE {
               ?s ex:name ?name .
               FILTER(LANG(?name) = \"en\")
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter with LANG function");
    }

    #[test]
    fn test_rdf_term_functions_datatype() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
             SELECT ?s ?val WHERE {
               ?s ex:value ?val .
               FILTER(DATATYPE(?val) = xsd:integer)
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter with DATATYPE function");
    }

    // =========================================================================
    // CONSTRUCT Query Tests
    // =========================================================================

    #[test]
    fn test_construct_basic() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             CONSTRUCT { ?s ex:newProp ?o } WHERE { ?s ex:oldProp ?o }",
        )
        .unwrap();

        // Verify output is Construct
        let template = query
            .output
            .construct_template()
            .expect("should be Construct");
        assert_eq!(template.patterns.len(), 1);

        // Verify WHERE patterns are lowered
        assert_eq!(query.patterns.len(), 1);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
    }

    #[test]
    fn test_construct_multiple_template_triples() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             CONSTRUCT {
               ?s ex:name ?name .
               ?s ex:type ex:Person
             }
             WHERE { ?s ex:oldName ?name }",
        )
        .unwrap();

        let template = query
            .output
            .construct_template()
            .expect("should be Construct");
        assert_eq!(template.patterns.len(), 2);
    }

    #[test]
    fn test_construct_where_shorthand() {
        // CONSTRUCT WHERE { ... } uses WHERE patterns as template
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             CONSTRUCT WHERE { ?s ex:name ?name }",
        )
        .unwrap();

        let template = query
            .output
            .construct_template()
            .expect("should be Construct");
        // Template should contain the WHERE patterns
        assert_eq!(template.patterns.len(), 1);
    }

    #[test]
    fn test_construct_with_limit_offset() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             CONSTRUCT { ?s ex:p ?o }
             WHERE { ?s ex:q ?o }
             LIMIT 10 OFFSET 5",
        )
        .unwrap();

        assert_eq!(query.options.limit, Some(10));
        assert_eq!(query.options.offset, Some(5));
    }

    #[test]
    fn test_construct_with_order_by() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             CONSTRUCT { ?s ex:p ?o }
             WHERE { ?s ex:q ?o }
             ORDER BY ?o",
        )
        .unwrap();

        assert_eq!(query.options.order_by.len(), 1);
    }

    #[test]
    fn test_construct_empty_select() {
        // CONSTRUCT queries don't project - select_vars() should be None
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             CONSTRUCT { ?s ex:p ?o } WHERE { ?s ex:q ?o }",
        )
        .unwrap();

        // CONSTRUCT doesn't project variables like SELECT does
        assert!(query.output.projected_vars().is_none());
    }

    // =========================================================================
    // ASK Query Tests
    // =========================================================================

    #[test]
    fn test_ask_basic() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             ASK { ?s ex:name \"Alice\" }",
        )
        .unwrap();

        assert!(matches!(query.output, QueryOutput::Ask));
        assert_eq!(query.options.limit, Some(1), "ASK should inject LIMIT 1");
        assert_eq!(query.patterns.len(), 1);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
    }

    #[test]
    fn test_ask_multiple_patterns() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             ASK { ?s ex:name ?name . ?s ex:age ?age }",
        )
        .unwrap();

        assert!(matches!(query.output, QueryOutput::Ask));
        assert_eq!(query.patterns.len(), 2);
    }

    #[test]
    fn test_ask_with_filter() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             ASK { ?s ex:age ?age FILTER(?age > 30) }",
        )
        .unwrap();

        assert!(matches!(query.output, QueryOutput::Ask));
        // Patterns: Triple + Filter
        assert!(query.patterns.len() >= 2);
    }

    // =========================================================================
    // Extended expression tests (Phase 9b)
    // =========================================================================

    #[test]
    fn test_arithmetic_expression() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?total WHERE {
               ?s ex:price ?price .
               ?s ex:qty ?qty .
               FILTER(?price * ?qty > 100)
             }",
        )
        .unwrap();

        // Should have Triple, Triple, Filter patterns
        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter pattern with arithmetic");
    }

    #[test]
    fn test_unary_negation() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?val WHERE {
               ?s ex:value ?val .
               FILTER(-?val < 0)
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter pattern with unary negation");
    }

    #[test]
    fn test_in_expression() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?status WHERE {
               ?s ex:status ?status .
               FILTER(?status IN (\"active\", \"pending\"))
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter pattern with IN expression");
    }

    #[test]
    fn test_not_in_expression() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?status WHERE {
               ?s ex:status ?status .
               FILTER(?status NOT IN (\"deleted\", \"archived\"))
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter pattern with NOT IN expression");
    }

    #[test]
    fn test_if_expression() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?label WHERE {
               ?s ex:value ?val .
               BIND(IF(?val > 0, \"positive\", \"non-positive\") AS ?label)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with IF expression");
    }

    #[test]
    fn test_complex_arithmetic() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:a ?a .
               ?s ex:b ?b .
               ?s ex:c ?c .
               FILTER((?a + ?b) * ?c > 100)
             }",
        )
        .unwrap();

        // Should parse complex arithmetic without error
        assert_eq!(query.output.projected_vars().unwrap().len(), 1);
        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter);
    }

    // =========================================================================
    // Solution Modifiers Tests
    // =========================================================================

    #[test]
    fn test_limit() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:p ?o } LIMIT 10",
        )
        .unwrap();

        assert_eq!(query.options.limit, Some(10));
        assert_eq!(query.options.offset, None);
    }

    #[test]
    fn test_offset() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:p ?o } OFFSET 5",
        )
        .unwrap();

        assert_eq!(query.options.offset, Some(5));
        assert_eq!(query.options.limit, None);
    }

    #[test]
    fn test_limit_offset() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE { ?s ex:p ?o } LIMIT 10 OFFSET 5",
        )
        .unwrap();

        assert_eq!(query.options.limit, Some(10));
        assert_eq!(query.options.offset, Some(5));
    }

    #[test]
    fn test_distinct() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT DISTINCT ?s WHERE { ?s ex:p ?o }",
        )
        .unwrap();

        assert!(query.output.is_distinct());
    }

    #[test]
    fn test_order_by_var() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?o WHERE { ?s ex:p ?o } ORDER BY ?o",
        )
        .unwrap();

        assert_eq!(query.options.order_by.len(), 1);
        assert_eq!(
            query.options.order_by[0].direction,
            SortDirection::Ascending
        );
    }

    #[test]
    fn test_order_by_desc() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?o WHERE { ?s ex:p ?o } ORDER BY DESC(?o)",
        )
        .unwrap();

        assert_eq!(query.options.order_by.len(), 1);
        assert_eq!(
            query.options.order_by[0].direction,
            SortDirection::Descending
        );
    }

    #[test]
    fn test_order_by_bracketed_var() {
        // ORDER BY ASC((?var)) with extra parentheses should work
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?o WHERE { ?s ex:p ?o } ORDER BY ASC((?o))",
        )
        .unwrap();

        assert_eq!(query.options.order_by.len(), 1);
        assert_eq!(
            query.options.order_by[0].direction,
            SortDirection::Ascending
        );
    }

    #[test]
    fn test_order_by_expr_unsupported() {
        let result = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?o WHERE { ?s ex:p ?o } ORDER BY (?o + 1)",
        );

        assert!(matches!(
            result,
            Err(LowerError::UnsupportedOrderByExpression { .. })
        ));
    }

    #[test]
    fn test_group_by_var() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type WHERE { ?s ex:type ?type } GROUP BY ?type",
        )
        .unwrap();

        assert_eq!(group_by_of(&query).len(), 1);
    }

    #[test]
    fn test_group_by_bracketed_var() {
        // GROUP BY (?var) with parentheses should work
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type WHERE { ?s ex:type ?type } GROUP BY (?type)",
        )
        .unwrap();

        assert_eq!(group_by_of(&query).len(), 1);
    }

    #[test]
    fn test_group_by_expression_desugars_to_bind() {
        // Expression GROUP BY: `GROUP BY (?x + 1 AS ?y)` desugars to
        // `BIND(?x + 1 AS ?y)` in patterns + `GROUP BY ?y` in options.
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ex:p ?x } GROUP BY (?x + 1 AS ?y)",
        )
        .unwrap();

        // GROUP BY should contain one variable (the alias ?y)
        assert_eq!(group_by_of(&query).len(), 1);
        let group_var = group_by_of(&query)[0];

        // Patterns should contain a Bind for the expression, targeting the same variable
        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { var, .. } if *var == group_var));
        assert!(has_bind, "expected a BIND pattern from expression GROUP BY");
    }

    #[test]
    fn test_group_by_expression_no_alias_generates_synthetic_var() {
        // GROUP BY (expr) without AS ?alias should generate a synthetic variable
        let (query, vars) = lower_query_with_vars(
            "PREFIX ex: <http://example.org/>
             SELECT (COUNT(?s) AS ?cnt) WHERE { ?s ex:p ?x } GROUP BY (?x + 1)",
        )
        .unwrap();

        assert_eq!(group_by_of(&query).len(), 1);
        let group_var = group_by_of(&query)[0];

        // The synthetic variable name starts with ?__group_expr_
        let var_name = vars.name(group_var);
        assert!(
            var_name.starts_with("?__group_expr_"),
            "expected synthetic variable name, got: {var_name}"
        );

        // Should have a BIND pattern targeting the synthetic variable
        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { var, .. } if *var == group_var));
        assert!(
            has_bind,
            "expected a BIND pattern for synthetic GROUP BY expression"
        );
    }

    #[test]
    fn test_group_by_bracketed_var_produces_no_bind() {
        // GROUP BY (?var) should NOT produce a BIND pattern — just unwrap the parens
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type WHERE { ?s ex:type ?type } GROUP BY (?type)",
        )
        .unwrap();

        assert_eq!(group_by_of(&query).len(), 1);
        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(
            !has_bind,
            "bracketed variable GROUP BY should not produce a BIND"
        );
    }

    #[test]
    fn test_having() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type WHERE { ?s ex:type ?type } GROUP BY ?type HAVING (?cnt > 5)",
        )
        .unwrap();

        assert!(having_of(&query).is_some());
    }

    #[test]
    fn test_all_modifiers() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT DISTINCT ?type WHERE { ?s ex:type ?type }
             GROUP BY ?type
             HAVING (?cnt > 0)
             ORDER BY ?type
             LIMIT 10
             OFFSET 5",
        )
        .unwrap();

        assert!(query.output.is_distinct());
        assert_eq!(group_by_of(&query).len(), 1);
        assert!(having_of(&query).is_some());
        assert_eq!(query.options.order_by.len(), 1);
        assert_eq!(query.options.limit, Some(10));
        assert_eq!(query.options.offset, Some(5));
    }

    // =========================================================================
    // Aggregate Extraction Tests (Step 6 & 7)
    // =========================================================================

    #[test]
    fn test_aggregate_count_with_alias() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (COUNT(?s) AS ?count) WHERE { ?s ex:p ?o }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        let agg = aggregates_of(&query)[0];
        assert!(matches!(agg.function, AggregateFn::Count));
    }

    #[test]
    fn test_aggregate_count_distinct() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s ex:p ?o }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        let agg = aggregates_of(&query)[0];
        assert!(matches!(agg.function, AggregateFn::CountDistinct));
    }

    #[test]
    fn test_aggregate_count_distinct_bracketed_var() {
        // COUNT(DISTINCT(?var)) with extra parentheses around the variable
        // is valid SPARQL and should be treated the same as COUNT(DISTINCT ?var)
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (COUNT(DISTINCT(?s)) AS ?count) WHERE { ?s ex:p ?o }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        let agg = aggregates_of(&query)[0];
        assert!(matches!(agg.function, AggregateFn::CountDistinct));
        assert!(agg.input_var.is_some()); // Should have resolved the variable
    }

    #[test]
    fn test_aggregate_count_distinct_bracketed_var_with_whitespace() {
        // COUNT(DISTINCT( ?var )) with whitespace inside parentheses
        // Whitespace is stripped by the lexer, so this should work identically
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ( COUNT( DISTINCT( ?s ) ) AS ?count ) WHERE { ?s ex:p ?o }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        let agg = aggregates_of(&query)[0];
        assert!(matches!(agg.function, AggregateFn::CountDistinct));
        assert!(agg.input_var.is_some());
    }

    #[test]
    fn test_distinct_flag_count_vs_sum() {
        // COUNT(DISTINCT) uses a dedicated AggregateFn::CountDistinct variant
        // with distinct=false (dedup is built into the variant's HashSet state).
        // SUM(DISTINCT) keeps AggregateFn::Sum with distinct=true (dedup happens
        // at execution time via HashSet filtering in apply_aggregate).
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (COUNT(DISTINCT ?s) AS ?c) (SUM(DISTINCT ?v) AS ?t)
             WHERE { ?s ex:val ?v }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 2);

        let count_agg = aggregates_of(&query)[0];
        assert!(matches!(count_agg.function, AggregateFn::CountDistinct));
        assert!(
            !count_agg.distinct,
            "CountDistinct variant should clear the distinct flag"
        );

        let sum_agg = aggregates_of(&query)[1];
        assert!(matches!(sum_agg.function, AggregateFn::Sum));
        assert!(sum_agg.distinct, "SUM(DISTINCT) should set distinct=true");
    }

    #[test]
    fn test_aggregate_sum_bracketed_var() {
        // SUM((?var)) with extra parentheses
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (SUM((?val)) AS ?total) WHERE { ?s ex:value ?val }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        assert!(matches!(
            aggregates_of(&query)[0].function,
            AggregateFn::Sum
        ));
        assert!(aggregates_of(&query)[0].input_var.is_some());
    }

    #[test]
    fn test_aggregate_sum() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (SUM(?val) AS ?total) WHERE { ?s ex:value ?val }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        assert!(matches!(
            aggregates_of(&query)[0].function,
            AggregateFn::Sum
        ));
    }

    #[test]
    fn test_aggregate_over_expression_desugars_to_bind() {
        let (query, vars) = lower_query_with_vars(
            "PREFIX ex: <http://example.org/>
             SELECT (SUM(YEAR(?dt)) AS ?sum) WHERE { ?s ex:created ?dt }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        let agg = aggregates_of(&query)[0];
        assert!(matches!(agg.function, AggregateFn::Sum));

        let input_var = agg.input_var.expect("expected aggregate input var");
        let input_name = vars.name(input_var);
        assert!(
            input_name.starts_with("?__agg_expr_"),
            "expected synthetic aggregate-input var, got: {input_name}"
        );

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { var, .. } if *var == input_var));
        assert!(
            has_bind,
            "expected a pre-aggregation BIND for the input expression"
        );
    }

    #[test]
    fn test_aggregate_avg() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (AVG(?val) AS ?average) WHERE { ?s ex:value ?val }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        assert!(matches!(
            aggregates_of(&query)[0].function,
            AggregateFn::Avg
        ));
    }

    #[test]
    fn test_aggregate_min_max() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (MIN(?val) AS ?minVal) (MAX(?val) AS ?maxVal) WHERE { ?s ex:value ?val }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 2);
        assert!(matches!(
            aggregates_of(&query)[0].function,
            AggregateFn::Min
        ));
        assert!(matches!(
            aggregates_of(&query)[1].function,
            AggregateFn::Max
        ));
    }

    #[test]
    fn test_aggregate_group_concat() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (GROUP_CONCAT(?name; SEPARATOR=\", \") AS ?names) WHERE { ?s ex:name ?name }",
        )
        .unwrap();

        let aggs = aggregates_of(&query);
        assert_eq!(aggs.len(), 1);
        match &aggs[0].function {
            AggregateFn::GroupConcat { separator } => {
                assert_eq!(separator, ", ");
            }
            _ => panic!("Expected GroupConcat"),
        }
    }

    #[test]
    fn test_aggregate_sample() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (SAMPLE(?name) AS ?sampleName) WHERE { ?s ex:name ?name }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        assert!(matches!(
            aggregates_of(&query)[0].function,
            AggregateFn::Sample
        ));
    }

    #[test]
    fn test_count_star() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT (COUNT(*) AS ?count) WHERE { ?s ex:p ?o }",
        )
        .unwrap();

        // Verify COUNT(*) aggregate was created
        assert_eq!(aggregates_of(&query).len(), 1);
        let agg = aggregates_of(&query)[0];
        assert!(matches!(agg.function, AggregateFn::CountAll));
        assert!(agg.input_var.is_none(), "COUNT(*) should have no input var");
    }

    #[test]
    fn test_auto_group_by_with_aggregate() {
        // When aggregates present but no explicit GROUP BY,
        // non-aggregate SELECT vars should be auto-added to GROUP BY
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s ex:type ?type }",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        // ?type should be auto-added to GROUP BY
        assert_eq!(group_by_of(&query).len(), 1);
    }

    #[test]
    fn test_explicit_group_by_not_modified() {
        // When explicit GROUP BY present, don't auto-populate
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s ex:type ?type } GROUP BY ?type",
        )
        .unwrap();

        assert_eq!(aggregates_of(&query).len(), 1);
        // Only the explicit GROUP BY var, not duplicated
        assert_eq!(group_by_of(&query).len(), 1);
    }

    #[test]
    fn test_aggregate_with_mixed_select() {
        // Mix of plain vars and aggregates
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?cat ?brand (COUNT(?product) AS ?cnt) (AVG(?price) AS ?avgPrice)
             WHERE { ?product ex:category ?cat ; ex:brand ?brand ; ex:price ?price }",
        )
        .unwrap();

        // 2 aggregates: COUNT and AVG
        assert_eq!(aggregates_of(&query).len(), 2);
        // 2 non-aggregate vars auto-added to GROUP BY: ?cat, ?brand
        assert_eq!(group_by_of(&query).len(), 2);
    }

    // =========================================================================
    // Property Path Tests
    // =========================================================================

    #[test]
    fn test_property_path_one_or_more() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?ancestor WHERE { ?s ex:parent+ ?ancestor }",
        )
        .unwrap();

        // Should have one PropertyPath pattern
        assert_eq!(query.patterns.len(), 1);
        assert!(matches!(query.patterns[0], Pattern::PropertyPath(_)));

        // Verify the modifier
        if let Pattern::PropertyPath(pp) = &query.patterns[0] {
            assert!(matches!(pp.modifier, PathModifier::OneOrMore));
        } else {
            panic!("Expected PropertyPath pattern");
        }
    }

    #[test]
    fn test_property_path_zero_or_more() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?member WHERE { ?s ex:hasMember* ?member }",
        )
        .unwrap();

        // Should have one PropertyPath pattern
        assert_eq!(query.patterns.len(), 1);
        assert!(matches!(query.patterns[0], Pattern::PropertyPath(_)));

        // Verify the modifier
        if let Pattern::PropertyPath(pp) = &query.patterns[0] {
            assert!(matches!(pp.modifier, PathModifier::ZeroOrMore));
        } else {
            panic!("Expected PropertyPath pattern");
        }
    }

    #[test]
    fn test_property_path_with_bound_subject() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?descendant WHERE { ex:alice ex:parent+ ?descendant }",
        )
        .unwrap();

        // Verify subject is bound
        if let Pattern::PropertyPath(pp) = &query.patterns[0] {
            assert!(pp.subject.is_bound());
            assert!(!pp.object.is_bound());
        } else {
            panic!("Expected PropertyPath pattern");
        }
    }

    #[test]
    fn test_property_path_with_bound_object() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?ancestor WHERE { ?ancestor ex:parent+ ex:bob }",
        )
        .unwrap();

        // Verify object is bound
        if let Pattern::PropertyPath(pp) = &query.patterns[0] {
            assert!(!pp.subject.is_bound());
            assert!(pp.object.is_bound());
        } else {
            panic!("Expected PropertyPath pattern");
        }
    }

    #[test]
    fn test_property_path_both_constants_error() {
        let result = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ex:alice ex:parent+ ex:bob }",
        );

        // Should error because both subject and object are bound
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, LowerError::InvalidPropertyPath { .. }));
    }

    #[test]
    fn test_property_path_inverse() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?child WHERE { ?child ^ex:parent ?parent }",
        )
        .unwrap();

        // Inverse compiles to a Triple with subject/object swapped
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Triple(tp) = &query.patterns[0] {
            // ^ex:parent swaps: triple is (?parent, ex:parent, ?child)
            assert!(tp.o.is_var()); // ?child in object position
            assert!(tp.s.is_var()); // ?parent in subject position
            assert!(tp.p.is_iri());
        } else {
            panic!(
                "Expected Triple pattern for inverse path, got {:?}",
                query.patterns[0]
            );
        }
    }

    #[test]
    fn test_property_path_inverse_one_or_more() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ex:b ^ex:knows+ ?x }",
        )
        .unwrap();

        // ^p+ → PropertyPathPattern with swapped subject/object
        // Original: s=ex:b, o=?x  →  pp.subject=?x, pp.object=ex:b
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::PropertyPath(pp) = &query.patterns[0] {
            assert!(matches!(pp.modifier, PathModifier::OneOrMore));
            assert!(!pp.subject.is_bound()); // ?x (original object)
            assert!(pp.object.is_bound()); // ex:b (original subject)
        } else {
            panic!(
                "Expected PropertyPath pattern for ^p+, got {:?}",
                query.patterns[0]
            );
        }
    }

    #[test]
    fn test_property_path_inverse_zero_or_more() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ex:b ^ex:knows* ?x }",
        )
        .unwrap();

        // ^p* → PropertyPathPattern with swapped subject/object
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::PropertyPath(pp) = &query.patterns[0] {
            assert!(matches!(pp.modifier, PathModifier::ZeroOrMore));
            assert!(!pp.subject.is_bound()); // ?x (original object)
            assert!(pp.object.is_bound()); // ex:b (original subject)
        } else {
            panic!(
                "Expected PropertyPath pattern for ^p*, got {:?}",
                query.patterns[0]
            );
        }
    }

    #[test]
    fn test_property_path_inverse_transitive_both_constants_error() {
        let result = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT * WHERE { ex:alice ^ex:knows+ ex:bob }",
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LowerError::InvalidPropertyPath { .. }
        ));
    }

    #[test]
    fn test_property_path_sequence_two_step() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?grandchild WHERE { ?s ex:parent/ex:parent ?grandchild }",
        )
        .unwrap();

        // Sequence compiles to 2 triple patterns joined by ?__pp0
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Triple(_)));

        // First triple: ?s → ?__pp0
        if let Pattern::Triple(tp) = &query.patterns[0] {
            assert!(tp.s.is_var()); // ?s
            assert!(tp.p.is_iri());
            assert!(tp.o.is_var()); // ?__pp0
        }
        // Second triple: ?__pp0 → ?grandchild
        if let Pattern::Triple(tp) = &query.patterns[1] {
            assert!(tp.s.is_var()); // ?__pp0
            assert!(tp.p.is_iri());
            assert!(tp.o.is_var()); // ?grandchild
        }
    }

    #[test]
    fn test_property_path_alternative() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?related WHERE { ?s ex:friend|ex:colleague ?related }",
        )
        .unwrap();

        // Alternative compiles to Union of triple branches
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);
            assert!(matches!(branches[0][0], Pattern::Triple(_)));
            assert!(matches!(branches[1][0], Pattern::Triple(_)));
        } else {
            panic!(
                "Expected Union pattern for alternative path, got {:?}",
                query.patterns[0]
            );
        }
    }

    #[test]
    fn test_property_path_alternative_with_inverse() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?related WHERE { ?s ex:friend|^ex:colleague ?related }",
        )
        .unwrap();

        // Alternative with inverse: Union of two branches
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);
            // First branch: forward ex:friend → (?s, ex:friend, ?related)
            if let Pattern::Triple(tp) = &branches[0][0] {
                assert!(tp.s.is_var());
                assert!(tp.o.is_var());
            } else {
                panic!("Expected Triple in first branch");
            }
            // Second branch: inverse ex:colleague → (?related, ex:colleague, ?s)
            if let Pattern::Triple(tp) = &branches[1][0] {
                assert!(tp.s.is_var()); // ?related
                assert!(tp.o.is_var()); // ?s
            } else {
                panic!("Expected Triple in second branch");
            }
        } else {
            panic!("Expected Union pattern, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_alternative_three_way() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?related WHERE { ?s ex:a|ex:b|ex:c ?related }",
        )
        .unwrap();

        // Three-way alternative flattens to Union with 3 branches
        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 3);
            for branch in branches {
                assert!(matches!(branch[0], Pattern::Triple(_)));
            }
        } else {
            panic!("Expected Union pattern, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_nested_alternative_under_transitive_errors() {
        let result = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s (ex:a|ex:b)+ ?x }",
        );

        // Transitive requires simple predicate, not complex expression
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, LowerError::InvalidPropertyPath { .. }));
    }

    #[test]
    fn test_property_path_rdf_type() {
        // Test a+ where a is the rdf:type shorthand
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?type WHERE { ex:thing a+ ?type }",
        )
        .unwrap();

        // Should lower to PropertyPath with rdf:type predicate
        assert_eq!(query.patterns.len(), 1);
        assert!(matches!(query.patterns[0], Pattern::PropertyPath(_)));
    }

    #[test]
    fn test_property_path_with_filter() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?ancestor WHERE {
               ?s ex:parent+ ?ancestor .
               FILTER(?ancestor != ex:root)
             }",
        )
        .unwrap();

        // Should have PropertyPath and Filter patterns
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::PropertyPath(_)));
        assert!(matches!(query.patterns[1], Pattern::Filter(_)));
    }

    #[test]
    fn test_property_path_in_optional() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?ancestor WHERE {
               ?s ex:name ?name .
               OPTIONAL { ?s ex:parent+ ?ancestor }
             }",
        )
        .unwrap();

        // Should have Triple and Optional patterns
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Optional(_)));

        // Check that Optional contains a PropertyPath
        if let Pattern::Optional(inner) = &query.patterns[1] {
            assert_eq!(inner.len(), 1);
            assert!(matches!(inner[0], Pattern::PropertyPath(_)));
        } else {
            panic!("Expected Optional pattern");
        }
    }

    // =========================================================================
    // Subquery Tests
    // =========================================================================

    #[test]
    fn test_subquery_basic() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name ?age WHERE {
               ?s ex:name ?name .
               { SELECT ?s ?age WHERE { ?s ex:age ?age } }
             }",
        )
        .unwrap();

        // Should have Triple and Subquery patterns
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Subquery(_)));

        // Check the subquery structure
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert_eq!(sq.select.len(), 2); // ?s and ?age
            assert_eq!(sq.patterns.len(), 1); // One triple pattern
            assert!(matches!(sq.patterns[0], Pattern::Triple(_)));
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_with_limit() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE {
               ?s ex:name ?name .
               { SELECT ?s WHERE { ?s ex:type ex:Person } LIMIT 10 }
             }",
        )
        .unwrap();

        // Check the subquery has limit
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert_eq!(sq.limit, Some(10));
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_with_limit_offset() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?score WHERE {
               ?s ex:name ?name .
               { SELECT ?s ?score WHERE { ?s ex:score ?score } LIMIT 5 OFFSET 10 }
             }",
        )
        .unwrap();

        // Check the subquery has limit and offset
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert_eq!(sq.limit, Some(5));
            assert_eq!(sq.offset, Some(10));
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_select_star_populates_select_list() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE {
               { SELECT * WHERE { ?s ex:name ?name } }
             }",
        )
        .unwrap();

        let sub = query.patterns.iter().find_map(|p| match p {
            Pattern::Subquery(sq) => Some(sq),
            _ => None,
        });
        let sub = sub.expect("Expected Subquery pattern");

        // `SELECT *` should select all variables referenced in the subquery patterns.
        // We don't assert exact order here; just that the important vars are present.
        assert!(
            sub.select.len() >= 2,
            "Expected SELECT * subquery to include ?s and ?name in select list"
        );
    }

    #[test]
    fn test_subquery_with_order_by() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:name ?name .
               { SELECT ?s WHERE { ?s ex:age ?age } ORDER BY ?age }
             }",
        )
        .unwrap();

        // Check the subquery has order_by
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert_eq!(sq.order_by.len(), 1, "Expected 1 ORDER BY spec");
            assert_eq!(
                sq.order_by[0].direction,
                SortDirection::Ascending,
                "Expected ascending order"
            );
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_with_order_by_desc() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:name ?name .
               { SELECT ?s WHERE { ?s ex:age ?age } ORDER BY DESC(?age) }
             }",
        )
        .unwrap();

        // Check the subquery has order_by with descending direction
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert_eq!(sq.order_by.len(), 1, "Expected 1 ORDER BY spec");
            assert_eq!(
                sq.order_by[0].direction,
                SortDirection::Descending,
                "Expected descending order"
            );
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_with_multiple_order_by() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:name ?name .
               { SELECT ?s ?age ?score WHERE { ?s ex:age ?age . ?s ex:score ?score } ORDER BY ?age DESC(?score) }
             }",
        )
        .unwrap();

        // Check the subquery has multiple order_by specs
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert_eq!(sq.order_by.len(), 2, "Expected 2 ORDER BY specs");
            assert_eq!(
                sq.order_by[0].direction,
                SortDirection::Ascending,
                "First should be ascending"
            );
            assert_eq!(
                sq.order_by[1].direction,
                SortDirection::Descending,
                "Second should be descending"
            );
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_with_distinct() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?type WHERE {
               ?s ex:name ?name .
               { SELECT DISTINCT ?s ?type WHERE { ?s ex:type ?type } }
             }",
        )
        .unwrap();

        // Check the subquery has distinct
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            assert!(sq.distinct);
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    #[test]
    fn test_subquery_nested_in_optional() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name ?score WHERE {
               ?s ex:name ?name .
               OPTIONAL {
                 { SELECT ?s ?score WHERE { ?s ex:score ?score } }
               }
             }",
        )
        .unwrap();

        // Should have Triple and Optional patterns
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Optional(_)));

        // Check that Optional contains a Subquery
        if let Pattern::Optional(inner) = &query.patterns[1] {
            assert_eq!(inner.len(), 1);
            assert!(matches!(inner[0], Pattern::Subquery(_)));
        } else {
            panic!("Expected Optional pattern");
        }
    }

    #[test]
    fn test_subquery_with_filter() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?topScore WHERE {
               ?s ex:name ?name .
               {
                 SELECT ?s ?topScore WHERE {
                   ?s ex:score ?topScore .
                   FILTER(?topScore > 90)
                 }
               }
             }",
        )
        .unwrap();

        // Check the subquery contains patterns
        if let Pattern::Subquery(sq) = &query.patterns[1] {
            // Should have Triple and Filter patterns
            assert_eq!(sq.patterns.len(), 2);
            assert!(matches!(sq.patterns[0], Pattern::Triple(_)));
            assert!(matches!(sq.patterns[1], Pattern::Filter(_)));
        } else {
            panic!("Expected Subquery pattern");
        }
    }

    // =========================================================================
    // Newly Implemented Function Tests
    // =========================================================================

    #[test]
    fn test_function_str() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?strVal WHERE {
               ?s ex:value ?val .
               BIND(STR(?val) AS ?strVal)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with STR function");
    }

    #[test]
    fn test_function_substr() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?part WHERE {
               ?s ex:name ?name .
               BIND(SUBSTR(?name, 2, 3) AS ?part)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with SUBSTR function");
    }

    #[test]
    fn test_function_encode_for_uri() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?encoded WHERE {
               ?s ex:label ?label .
               BIND(ENCODE_FOR_URI(?label) AS ?encoded)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(
            has_bind,
            "Expected Bind pattern with ENCODE_FOR_URI function"
        );
    }

    #[test]
    fn test_function_langmatches() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name WHERE {
               ?s ex:name ?name .
               FILTER(LANGMATCHES(LANG(?name), \"en\"))
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter with LANGMATCHES function");
    }

    #[test]
    fn test_function_sameterm() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s WHERE {
               ?s ex:a ?a .
               ?s ex:b ?b .
               FILTER(SAMETERM(?a, ?b))
             }",
        )
        .unwrap();

        let has_filter = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Filter(_)));
        assert!(has_filter, "Expected Filter with SAMETERM function");
    }

    #[test]
    fn test_function_md5() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?hash WHERE {
               ?s ex:data ?data .
               BIND(MD5(?data) AS ?hash)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with MD5 function");
    }

    #[test]
    fn test_function_sha1() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?hash WHERE {
               ?s ex:data ?data .
               BIND(SHA1(?data) AS ?hash)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with SHA1 function");
    }

    #[test]
    fn test_function_sha256() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?hash WHERE {
               ?s ex:data ?data .
               BIND(SHA256(?data) AS ?hash)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with SHA256 function");
    }

    #[test]
    fn test_function_sha384() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?hash WHERE {
               ?s ex:data ?data .
               BIND(SHA384(?data) AS ?hash)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with SHA384 function");
    }

    #[test]
    fn test_function_sha512() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?hash WHERE {
               ?s ex:data ?data .
               BIND(SHA512(?data) AS ?hash)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with SHA512 function");
    }

    #[test]
    fn test_function_uuid() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?id WHERE {
               ?s ex:name ?name .
               BIND(UUID() AS ?id)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with UUID function");
    }

    #[test]
    fn test_function_struuid() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?id WHERE {
               ?s ex:name ?name .
               BIND(STRUUID() AS ?id)
             }",
        )
        .unwrap();

        let has_bind = query
            .patterns
            .iter()
            .any(|p| matches!(p, Pattern::Bind { .. }));
        assert!(has_bind, "Expected Bind pattern with STRUUID function");
    }

    // =========================================================================
    // Property Path Sequence Tests
    // =========================================================================

    #[test]
    fn test_property_path_sequence_three_step() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?great WHERE { ?s ex:parent/ex:parent/ex:parent ?great }",
        )
        .unwrap();

        // 3-step sequence → 3 triple patterns, 2 join variables
        assert_eq!(query.patterns.len(), 3);
        for p in &query.patterns {
            assert!(matches!(p, Pattern::Triple(_)));
        }
    }

    #[test]
    fn test_property_path_sequence_with_inverse() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?name WHERE { ?s ^ex:friend/ex:name ?name }",
        )
        .unwrap();

        // 2-step: ^ex:friend then ex:name → 2 triples
        assert_eq!(query.patterns.len(), 2);

        // First triple: inverse means (?__pp0, ex:friend, ?s)
        if let Pattern::Triple(tp) = &query.patterns[0] {
            assert!(tp.s.is_var()); // ?__pp0
            assert!(tp.p.is_iri());
            assert!(tp.o.is_var()); // ?s
        }
        // Second triple: forward (?__pp0, ex:name, ?name)
        if let Pattern::Triple(tp) = &query.patterns[1] {
            assert!(tp.s.is_var()); // ?__pp0
            assert!(tp.p.is_iri());
            assert!(tp.o.is_var()); // ?name
        }
    }

    #[test]
    fn test_property_path_sequence_with_rdf_type() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
             SELECT ?type WHERE { ?s ex:knows/rdf:type ?type }",
        )
        .unwrap();

        // 2-step: ex:knows then rdf:type
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Triple(_)));
    }

    #[test]
    fn test_property_path_sequence_in_optional() {
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?s ?name ?gp WHERE {
               ?s ex:name ?name .
               OPTIONAL { ?s ex:parent/ex:parent ?gp }
             }",
        )
        .unwrap();

        // The sequence expands to 2 triple patterns inside the OPTIONAL.
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        if let Pattern::Optional(inner) = &query.patterns[1] {
            assert_eq!(inner.len(), 2);
            assert!(matches!(inner[0], Pattern::Triple(_)));
            assert!(matches!(inner[1], Pattern::Triple(_)));
        } else {
            panic!("Expected Optional pattern");
        }
    }

    #[test]
    fn test_property_path_sequence_transitive_step_allowed() {
        // Transitive modifier inside a sequence step should work
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ex:parent+/ex:name ?x }",
        )
        .unwrap();

        // Should lower to: PropertyPath(?s, ex:parent, +, ?__pp0), Triple(?__pp0, ex:name, ?x)
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::PropertyPath(_)));
        assert!(matches!(query.patterns[1], Pattern::Triple(_)));
    }

    #[test]
    fn test_property_path_sequence_with_alternative_step_distributes() {
        // (ex:a|ex:b)/ex:name → Union of two two-step chains
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s (ex:a|ex:b)/ex:name ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);

            // Each branch should have 2 triple patterns
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(
                    branch.len(),
                    2,
                    "Branch {} should have 2 triple patterns, got {}",
                    i,
                    branch.len()
                );
                assert!(
                    matches!(branch[0], Pattern::Triple(_)),
                    "Branch {i} pattern 0 should be Triple"
                );
                assert!(
                    matches!(branch[1], Pattern::Triple(_)),
                    "Branch {i} pattern 1 should be Triple"
                );
            }
        } else {
            panic!("Expected Pattern::Union, got {:?}", query.patterns[0]);
        }
    }

    // =========================================================================
    // Sequence-in-Alternative Property Path Tests
    // =========================================================================

    #[test]
    fn test_property_path_alternative_with_sequence_branches() {
        // ex:friend/ex:name | ex:colleague/ex:name
        // Each branch is a two-step sequence → Union of two triple-chains
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?name WHERE { ?s (ex:friend/ex:name)|(ex:colleague/ex:name) ?name }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);

            // Each branch should have 2 triple patterns (two-step chain)
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(
                    branch.len(),
                    2,
                    "Branch {} should have 2 triple patterns, got {}",
                    i,
                    branch.len()
                );
                assert!(
                    matches!(branch[0], Pattern::Triple(_)),
                    "Branch {i} pattern 0 should be Triple"
                );
                assert!(
                    matches!(branch[1], Pattern::Triple(_)),
                    "Branch {i} pattern 1 should be Triple"
                );
            }
        } else {
            panic!("Expected Union pattern, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_alternative_mixed_simple_and_sequence() {
        // ex:name | ex:friend/ex:name — one simple IRI, one sequence
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?name WHERE { ?s ex:name|(ex:friend/ex:name) ?name }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);
            // First branch: single triple (simple IRI)
            assert_eq!(branches[0].len(), 1);
            assert!(matches!(branches[0][0], Pattern::Triple(_)));
            // Second branch: two triples (sequence chain)
            assert_eq!(branches[1].len(), 2);
            assert!(matches!(branches[1][0], Pattern::Triple(_)));
            assert!(matches!(branches[1][1], Pattern::Triple(_)));
        } else {
            panic!("Expected Union pattern, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_alternative_three_way_with_sequence() {
        // ex:name | ex:friend/ex:name | ^ex:colleague
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?val WHERE { ?s ex:name|(ex:friend/ex:name)|^ex:colleague ?val }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 3);
            // Branch 0: simple IRI → 1 triple
            assert_eq!(branches[0].len(), 1);
            // Branch 1: sequence → 2 triples
            assert_eq!(branches[1].len(), 2);
            // Branch 2: inverse → 1 triple
            assert_eq!(branches[2].len(), 1);
        } else {
            panic!("Expected Union pattern, got {:?}", query.patterns[0]);
        }
    }

    // =========================================================================
    // Alternative-in-Sequence Distribution Tests
    // =========================================================================

    #[test]
    fn test_property_path_sequence_middle_alternative() {
        // ex:a/(ex:b|ex:c)/ex:d → Union of 2 three-step chains
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ex:a/(ex:b|ex:c)/ex:d ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);

            // Each branch should be a 3-step chain (3 triples)
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(
                    branch.len(),
                    3,
                    "Branch {} should have 3 triple patterns, got {}",
                    i,
                    branch.len()
                );
                for (j, pat) in branch.iter().enumerate() {
                    assert!(
                        matches!(pat, Pattern::Triple(_)),
                        "Branch {i} pattern {j} should be Triple"
                    );
                }
            }
        } else {
            panic!("Expected Pattern::Union, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_sequence_multiple_alternatives() {
        // (ex:a|ex:b)/(ex:c|ex:d) → Union of 4 two-step chains
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s (ex:a|ex:b)/(ex:c|ex:d) ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 4);

            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(
                    branch.len(),
                    2,
                    "Branch {} should have 2 triple patterns, got {}",
                    i,
                    branch.len()
                );
            }
        } else {
            panic!("Expected Pattern::Union, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_sequence_alternative_with_inverse() {
        // (ex:a|^ex:b)/ex:c → Union of 2 chains, one with inverse
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s (ex:a|^ex:b)/ex:c ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);

            // Both branches should have 2 triples
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(branch.len(), 2, "Branch {i} should have 2 triple patterns");
            }

            // Branch 0 first triple: forward — subject is a var (the ?s)
            if let Pattern::Triple(t0) = &branches[0][0] {
                assert!(
                    t0.s.is_var(),
                    "Branch 0 first triple subject should be a var"
                );
                // Predicate should be ex:a
                assert_eq!(t0.p.as_iri(), Some("http://example.org/a"));
            }

            // Branch 1 first triple: inverse — object is the ?s var (swapped)
            if let Pattern::Triple(t1) = &branches[1][0] {
                assert!(
                    matches!(&t1.o, Term::Var(_)),
                    "Branch 1 first triple object should be a var (inverse)"
                );
                // Predicate should be ex:b
                assert_eq!(t1.p.as_iri(), Some("http://example.org/b"));
            }
        } else {
            panic!("Expected Pattern::Union, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_sequence_expansion_limit() {
        // Build (a0|b0)/(a1|b1)/(a2|b2)/(a3|b3)/(a4|b4)/(a5|b5)/(a6|b6)
        // 2^7 = 128 > 64 limit
        let predicates: String = (0..7)
            .map(|i| format!("(ex:a{i}|ex:b{i})"))
            .collect::<Vec<_>>()
            .join("/");
        let sparql =
            format!("PREFIX ex: <http://example.org/>\nSELECT ?x WHERE {{ ?s {predicates} ?x }}");
        let result = lower_query(&sparql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, LowerError::InvalidPropertyPath { .. }));
        let msg = err.to_string();
        assert!(
            msg.contains("expands to 128") && msg.contains("limit 64"),
            "Unexpected error: {msg}",
        );
    }

    // ======================================================================
    // Inverse of complex paths
    // ======================================================================

    #[test]
    fn test_property_path_inverse_of_sequence() {
        // ^(ex:a/ex:b) → 2 triples with predicates reversed (b, a),
        // each with swapped s/o (inverse)
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ^(ex:a/ex:b) ?x }",
        )
        .unwrap();

        // Rewrite: Sequence([^b, ^a]) → 2 triple patterns
        assert_eq!(query.patterns.len(), 2);
        assert!(matches!(query.patterns[0], Pattern::Triple(_)));
        assert!(matches!(query.patterns[1], Pattern::Triple(_)));

        // Predicates in reversed order: b, a
        if let Pattern::Triple(tp0) = &query.patterns[0] {
            assert_eq!(
                tp0.p.as_iri(),
                Some("http://example.org/b"),
                "First predicate should be ex:b (reversed), got {:?}",
                tp0.p
            );
        }
        if let Pattern::Triple(tp1) = &query.patterns[1] {
            assert_eq!(
                tp1.p.as_iri(),
                Some("http://example.org/a"),
                "Second predicate should be ex:a (reversed), got {:?}",
                tp1.p
            );
        }
    }

    #[test]
    fn test_property_path_inverse_of_alternative() {
        // ^(ex:a|ex:b) → Union of 2 branches, each with swapped s/o
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ^(ex:a|ex:b) ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(branch.len(), 1, "Branch {i} should have 1 triple");
                assert!(
                    matches!(&branch[0], Pattern::Triple(_)),
                    "Branch {i} should be a Triple"
                );
            }
        } else {
            panic!("Expected Union, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_inverse_of_three_step_sequence() {
        // ^(ex:a/ex:b/ex:c) → 3 triples, predicates reversed (c, b, a)
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ^(ex:a/ex:b/ex:c) ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 3);
        let expected_preds = [
            "http://example.org/c",
            "http://example.org/b",
            "http://example.org/a",
        ];
        for (i, exp) in expected_preds.iter().enumerate() {
            if let Pattern::Triple(tp) = &query.patterns[i] {
                assert_eq!(
                    tp.p.as_iri(),
                    Some(*exp),
                    "Step {}: expected predicate {}, got {:?}",
                    i,
                    exp,
                    tp.p
                );
            } else {
                panic!("Step {}: expected Triple, got {:?}", i, query.patterns[i]);
            }
        }
    }

    #[test]
    fn test_property_path_inverse_of_sequence_with_alternative() {
        // ^(ex:a/(ex:b|ex:c)) →
        //   Rewrite: Sequence([Alternative([^b, ^c]), ^a])
        //   Distribution: Union of 2 two-step chains
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ^(ex:a/(ex:b|ex:c)) ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);
            for (i, branch) in branches.iter().enumerate() {
                assert_eq!(branch.len(), 2, "Branch {i} should have 2 triples");
            }
        } else {
            panic!("Expected Union, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_property_path_double_inverse_in_sequence() {
        // ^(^ex:a/ex:b) → Sequence([^b, a])
        // The ^ex:a step: Inverse(Inverse(a)) cancels to a (forward)
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ^(^ex:a/ex:b) ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 2);
        // Step 0: ^b (reversed, inverted) — inverse triple
        if let Pattern::Triple(tp0) = &query.patterns[0] {
            assert_eq!(
                tp0.p.as_iri(),
                Some("http://example.org/b"),
                "First predicate should be ex:b, got {:?}",
                tp0.p
            );
        }
        // Step 1: a (^(^a) cancelled to forward a)
        if let Pattern::Triple(tp1) = &query.patterns[1] {
            assert_eq!(
                tp1.p.as_iri(),
                Some("http://example.org/a"),
                "Second predicate should be ex:a, got {:?}",
                tp1.p
            );
        }
    }

    #[test]
    fn test_property_path_double_inverse_in_alternative() {
        // ^(ex:a|^ex:b) → Alternative([^a, b])
        // The ^ex:b branch: Inverse(Inverse(b)) cancels to b (forward)
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ?s ^(ex:a|^ex:b) ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        if let Pattern::Union(branches) = &query.patterns[0] {
            assert_eq!(branches.len(), 2);
            // Branch 0: ^a — inverse triple (?x, a, ?s)
            if let Pattern::Triple(tp0) = &branches[0][0] {
                assert_eq!(
                    tp0.p.as_iri(),
                    Some("http://example.org/a"),
                    "Branch 0 predicate should be ex:a, got {:?}",
                    tp0.p
                );
                // Inverse: subject should be ?x (the object var), object should be ?s
                // (swapped compared to forward)
            }
            // Branch 1: b — forward triple (?s, b, ?x)
            if let Pattern::Triple(tp1) = &branches[1][0] {
                assert_eq!(
                    tp1.p.as_iri(),
                    Some("http://example.org/b"),
                    "Branch 1 predicate should be ex:b, got {:?}",
                    tp1.p
                );
                // Forward: subject should be ?s, object should be ?x (not swapped)
            }
        } else {
            panic!("Expected Union, got {:?}", query.patterns[0]);
        }
    }

    #[test]
    fn test_existing_inverse_transitive_unchanged() {
        // ^ex:knows+ should still produce PropertyPathPattern (no regression)
        let query = lower_query(
            "PREFIX ex: <http://example.org/>
             SELECT ?x WHERE { ex:b ^ex:knows+ ?x }",
        )
        .unwrap();

        assert_eq!(query.patterns.len(), 1);
        assert!(
            matches!(query.patterns[0], Pattern::PropertyPath(_)),
            "Expected PropertyPath, got {:?}",
            query.patterns[0]
        );
    }
}
