//! Graph pattern lowering.
//!
//! Converts SPARQL graph patterns (BGP, OPTIONAL, UNION, FILTER, BIND,
//! VALUES, MINUS, GRAPH, etc.) to the query engine's `Pattern` representation.

use crate::ast::expr::Expression;
use crate::ast::pattern::GraphPattern as SparqlGraphPattern;
use crate::ast::term::{Term as SparqlTerm, Var};

use fluree_db_query::binding::Binding;
use fluree_db_query::ir::{
    GraphName as IrGraphName, Pattern, ServiceEndpoint as IrServiceEndpoint, ServicePattern,
    SubqueryPattern,
};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use super::{LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    pub(super) fn lower_graph_pattern(
        &mut self,
        pattern: &SparqlGraphPattern,
    ) -> Result<Vec<Pattern>> {
        match pattern {
            SparqlGraphPattern::Bgp { patterns, .. } => self.lower_bgp_with_rdf_star(patterns),

            SparqlGraphPattern::Group { patterns, .. } => {
                // Safety check: a Group containing another Group implies
                // explicitly nested `{ }` blocks from the source query.
                // If the parser ever wraps single patterns in synthetic
                // Group nodes (breaking the invariant below), this assertion
                // catches it in debug/test builds before silent mis-scoping.
                debug_assert!(
                    patterns.iter().filter(|p| matches!(p, SparqlGraphPattern::Group { .. })).all(|p| {
                        // A nested Group must contain ≥ 2 children — the parser
                        // returns a single child directly (without Group wrapping)
                        // when parse_group_graph_pattern encounters only one pattern.
                        matches!(p, SparqlGraphPattern::Group { patterns: inner, .. } if inner.len() >= 2)
                    }),
                    "Nested Group node with a single child detected — \
                     the parser should return single patterns directly, not \
                     wrapped in a Group.  This would cause incorrect scope \
                     boundary lowering.  See parse_group_graph_pattern()."
                );

                let mut result = Vec::new();
                for p in patterns {
                    if matches!(p, SparqlGraphPattern::Group { .. }) {
                        // An explicit nested { } block inside a group creates a
                        // SPARQL scope boundary.  Variables bound outside are NOT
                        // visible inside (see W3C bind10).  We lower it as an
                        // anonymous subquery (SELECT * WHERE { ... }) to preserve
                        // the scope.  Only variables BOUND inside the group
                        // (by triples, BIND, VALUES) appear in the subquery's
                        // SELECT — not variables merely referenced by FILTERs.
                        //
                        // Invariant: the parser only produces nested Group nodes for
                        // explicitly braced `{ }` blocks inside a WHERE clause, not
                        // for the outer WHERE group itself.  See parse_group_graph_pattern().
                        let inner = self.lower_graph_pattern(p)?;
                        let vars = collect_bound_variables(&inner);
                        result.push(Pattern::Subquery(SubqueryPattern::new(vars, inner)));
                    } else {
                        let lowered = self.lower_graph_pattern(p)?;
                        result.extend(lowered);
                    }
                }
                Ok(result)
            }

            SparqlGraphPattern::Optional { pattern, .. } => {
                let inner = self.lower_graph_pattern(pattern)?;
                // SPARQL semantics: OPTIONAL is a left-join of the ENTIRE inner group.
                // Do not split multi-triple OPTIONAL blocks; doing so changes results
                // (e.g., OPTIONAL { A . B } FILTER(!bound(?v)) patterns in BSBM Q3).
                Ok(vec![Pattern::Optional(inner)])
            }

            SparqlGraphPattern::Union { left, right, .. } => {
                let left_patterns = self.lower_graph_pattern(left)?;
                let right_patterns = self.lower_graph_pattern(right)?;
                Ok(vec![Pattern::Union(vec![left_patterns, right_patterns])])
            }

            SparqlGraphPattern::Filter { expr, .. } => self.lower_filter_pattern(expr),

            SparqlGraphPattern::Bind { expr, var, .. } => self.lower_bind_pattern(expr, var),

            SparqlGraphPattern::Values { vars, data, .. } => self.lower_values_pattern(vars, data),

            SparqlGraphPattern::Minus { left, right, .. } => {
                // Lower left patterns first (the base patterns to match)
                let mut result = self.lower_graph_pattern(left)?;
                // Lower right patterns and wrap in MINUS
                let right_patterns = self.lower_graph_pattern(right)?;
                result.push(Pattern::Minus(right_patterns));
                Ok(result)
            }

            SparqlGraphPattern::Graph { name, pattern, .. } => {
                self.lower_named_graph_pattern(name, pattern)
            }

            SparqlGraphPattern::Service {
                silent,
                endpoint,
                pattern,
                ..
            } => self.lower_service_pattern(*silent, endpoint, pattern),

            SparqlGraphPattern::SubSelect { query, span } => self.lower_subselect(query, *span),

            SparqlGraphPattern::Path {
                subject,
                path,
                object,
                span,
            } => self.lower_property_path(subject, path, object, *span),
        }
    }

    /// Lower FILTER pattern, handling EXISTS/NOT EXISTS specially
    fn lower_filter_pattern(&mut self, expr: &Expression) -> Result<Vec<Pattern>> {
        // Unwrap brackets before checking for standalone EXISTS/NOT EXISTS.
        // FILTER (NOT EXISTS { ... }) parses as Bracketed { inner: NotExists { ... } }.
        let unwrapped = expr.unwrap_bracketed();
        match unwrapped {
            Expression::Exists { pattern, .. } => {
                let inner = self.lower_graph_pattern(pattern)?;
                Ok(vec![Pattern::Exists(inner)])
            }
            Expression::NotExists { pattern, .. } => {
                let inner = self.lower_graph_pattern(pattern)?;
                Ok(vec![Pattern::NotExists(inner)])
            }
            _ => {
                let filter_expr = self.lower_expression(expr)?;
                Ok(vec![Pattern::Filter(filter_expr)])
            }
        }
    }

    /// Lower BIND pattern
    fn lower_bind_pattern(&mut self, expr: &Expression, var: &Var) -> Result<Vec<Pattern>> {
        let filter_expr = self.lower_expression(expr)?;
        let var_id = self.register_var(var);
        Ok(vec![Pattern::Bind {
            var: var_id,
            expr: filter_expr,
        }])
    }

    /// Lower VALUES pattern
    fn lower_values_pattern(
        &mut self,
        vars: &[Var],
        data: &[Vec<Option<SparqlTerm>>],
    ) -> Result<Vec<Pattern>> {
        let var_ids: Vec<VarId> = vars.iter().map(|v| self.register_var(v)).collect();

        let rows: Vec<Vec<Binding>> = data
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        Some(term) => self.term_to_binding(term),
                        None => Ok(Binding::Unbound),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(vec![Pattern::Values {
            vars: var_ids,
            rows,
        }])
    }

    /// Lower GRAPH { name pattern } pattern
    fn lower_named_graph_pattern(
        &mut self,
        name: &crate::ast::pattern::GraphName,
        pattern: &SparqlGraphPattern,
    ) -> Result<Vec<Pattern>> {
        // Lower the graph name (IRI or variable)
        let ir_name = match name {
            crate::ast::pattern::GraphName::Iri(iri) => {
                let expanded = self.expand_iri(iri)?;
                // Use string, not Sid - graph names are ledger identifiers, not encoded
                IrGraphName::Iri(std::sync::Arc::from(expanded))
            }
            crate::ast::pattern::GraphName::Var(v) => {
                let var_id = self.register_var(v);
                IrGraphName::Var(var_id)
            }
        };
        // Lower the inner pattern
        let inner_patterns = self.lower_graph_pattern(pattern)?;
        Ok(vec![Pattern::Graph {
            name: ir_name,
            patterns: inner_patterns,
        }])
    }

    /// Lower SERVICE pattern
    ///
    /// SERVICE <endpoint> { ... } or SERVICE SILENT <endpoint> { ... }
    ///
    /// For local Fluree ledgers, the endpoint should be `fluree:ledger:<alias>:<branch>`
    /// or just `fluree:ledger:<alias>` (defaults to :main branch).
    fn lower_service_pattern(
        &mut self,
        silent: bool,
        endpoint: &crate::ast::pattern::ServiceEndpoint,
        pattern: &SparqlGraphPattern,
    ) -> Result<Vec<Pattern>> {
        // Lower the endpoint (IRI or variable)
        let ir_endpoint = match endpoint {
            crate::ast::pattern::ServiceEndpoint::Iri(iri) => {
                let expanded = self.expand_iri(iri)?;
                IrServiceEndpoint::Iri(std::sync::Arc::from(expanded))
            }
            crate::ast::pattern::ServiceEndpoint::Var(v) => {
                let var_id = self.register_var(v);
                IrServiceEndpoint::Var(var_id)
            }
        };

        // Lower the inner pattern
        let inner_patterns = self.lower_graph_pattern(pattern)?;

        // Extract the original SPARQL text for the SERVICE body (for remote execution).
        // The pattern span covers the inner `{ ... }` content.
        let source_body = self.source_text.and_then(|src| {
            let span = pattern.span();
            src.get(span.start..span.end).map(std::sync::Arc::from)
        });

        let service = match source_body {
            Some(body) => {
                ServicePattern::with_source_body(silent, ir_endpoint, inner_patterns, body)
            }
            None => ServicePattern::new(silent, ir_endpoint, inner_patterns),
        };

        Ok(vec![Pattern::Service(service)])
    }
}

/// Collect variables that are **bound** (produced) by patterns.
///
/// This includes variables from triple patterns, BIND outputs, VALUES vars,
/// and subquery selects — but NOT variables merely referenced by FILTERs or
/// BIND expressions.  Used to build the SELECT list for anonymous subqueries
/// that enforce SPARQL scope boundaries.
fn collect_bound_variables(patterns: &[Pattern]) -> Vec<VarId> {
    use fluree_db_query::ir::triple::Ref;
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();

    fn collect(
        patterns: &[Pattern],
        seen: &mut std::collections::HashSet<VarId>,
        out: &mut Vec<VarId>,
    ) {
        use fluree_db_query::ir::triple::Term;
        fn add(v: VarId, seen: &mut std::collections::HashSet<VarId>, out: &mut Vec<VarId>) {
            if seen.insert(v) {
                out.push(v);
            }
        }
        for p in patterns {
            match p {
                Pattern::Triple(tp) => {
                    if let Ref::Var(v) = &tp.s {
                        add(*v, seen, out);
                    }
                    if let Ref::Var(v) = &tp.p {
                        add(*v, seen, out);
                    }
                    if let Term::Var(v) = &tp.o {
                        add(*v, seen, out);
                    }
                }
                Pattern::Bind { var, .. } if seen.insert(*var) => {
                    out.push(*var);
                }
                Pattern::Values { vars, .. } => {
                    for v in vars {
                        if seen.insert(*v) {
                            out.push(*v);
                        }
                    }
                }
                Pattern::Optional(inner) | Pattern::Minus(inner) => {
                    collect(inner, seen, out);
                }
                Pattern::Union(branches) => {
                    for branch in branches {
                        collect(branch, seen, out);
                    }
                }
                Pattern::Subquery(sq) => {
                    for v in &sq.select {
                        if seen.insert(*v) {
                            out.push(*v);
                        }
                    }
                }
                Pattern::Graph { patterns, .. } => {
                    collect(patterns, seen, out);
                }
                Pattern::Service(sp) => {
                    collect(&sp.patterns, seen, out);
                }
                Pattern::PropertyPath(pp) => {
                    for v in pp.produced_vars() {
                        add(v, seen, out);
                    }
                }
                // Filter, Exists, NotExists, etc. only reference vars
                _ => {}
            }
        }
    }

    collect(patterns, &mut seen, &mut result);
    result
}
