//! DESCRIBE query lowering.
//!
//! SPARQL DESCRIBE has implementation-defined semantics in the spec. For Fluree DB-R we
//! implement a fast, benchmark-friendly interpretation:
//! - Identify resources to describe (variables from WHERE results and/or explicit IRIs)
//! - For each resource, return all outgoing triples: `?res ?p ?o`
//!
//! This is lowered into the common query IR as a CONSTRUCT-like query with:
//! - A synthetic `?__describe` variable bound to resources
//! - A triple pattern that expands to outgoing triples for each bound resource

use crate::ast::query::{DescribeQuery, DescribeTarget, SolutionModifiers, VarOrIri};
use crate::SourceSpan;

use fluree_db_query::binding::Binding;
use fluree_db_query::ir::{Expression, Pattern, SubqueryPattern};
use fluree_db_query::options::QueryOptions;
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::parse::{
    ConstructTemplate as QueryConstructTemplate, ParsedQuery, QueryOutput,
};
use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_query::var_registry::VarId;

use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a DESCRIBE query to a ParsedQuery.
    pub(super) fn lower_describe(&mut self, describe: &DescribeQuery) -> Result<ParsedQuery> {
        let ctx = self.build_jsonld_context()?;
        let ctx_val = self.build_jsonld_context_value();

        // Synthetic variables used internally for DESCRIBE expansion.
        //
        // NOTE: These must be in the shared VarRegistry so the executor/formatters can
        // reference them consistently. They are not projected in output (CONSTRUCT graph).
        let describe_var = self.vars.get_or_insert("?__describe");
        let p_var = self.vars.get_or_insert("?__describe_p");
        let o_var = self.vars.get_or_insert("?__describe_o");

        // CONSTRUCT template: { ?__describe ?__describe_p ?__describe_o }
        let template = QueryConstructTemplate::new(vec![TriplePattern::new(
            Ref::Var(describe_var),
            Ref::Var(p_var),
            Term::Var(o_var),
        )]);

        // Build branches that bind ?__describe:
        // - Explicit IRIs become VALUES rows
        // - WHERE clause (if any) becomes a subquery producing target vars, then BIND → ?__describe
        let mut branches: Vec<Vec<Pattern>> = Vec::new();

        // 1) Explicit IRI targets (DESCRIBE <iri> ...)
        let mut explicit_sids = Vec::new();
        if let DescribeTarget::Resources(resources) = &describe.target {
            for r in resources {
                if let VarOrIri::Iri(iri) = r {
                    let full_iri = self.expand_iri(iri)?;
                    let sid = self
                        .encoder
                        .encode_iri(&full_iri)
                        .ok_or_else(|| LowerError::unknown_namespace(&full_iri, iri.span))?;
                    explicit_sids.push(sid);
                }
            }
        }
        if !explicit_sids.is_empty() {
            let rows = explicit_sids
                .into_iter()
                .map(|sid| vec![Binding::sid(sid)])
                .collect();
            branches.push(vec![Pattern::Values {
                vars: vec![describe_var],
                rows,
            }]);
        }

        // 2) WHERE-derived targets (DESCRIBE ?x WHERE { ... } / DESCRIBE * WHERE { ... })
        if let Some(where_clause) = &describe.where_clause {
            // Lower WHERE clause patterns first (so variable IDs are registered).
            let where_patterns = self.lower_graph_pattern(&where_clause.pattern)?;

            let target_vars = self.describe_target_vars(&describe.target, &where_patterns);
            if !target_vars.is_empty() {
                let mut subq = SubqueryPattern::new(target_vars.clone(), where_patterns);
                subq.distinct = true;
                self.lower_describe_modifiers(
                    &describe.modifiers,
                    describe.span,
                    &mut subq,
                    &target_vars,
                )?;

                // Map each selected target var to ?__describe.
                let bind_patterns = if target_vars.len() == 1 {
                    vec![Pattern::Bind {
                        var: describe_var,
                        expr: Expression::Var(target_vars[0]),
                    }]
                } else {
                    let mut inner = Vec::with_capacity(target_vars.len());
                    for v in &target_vars {
                        inner.push(vec![Pattern::Bind {
                            var: describe_var,
                            expr: Expression::Var(*v),
                        }]);
                    }
                    vec![Pattern::Union(inner)]
                };

                let mut branch = Vec::with_capacity(2 + bind_patterns.len());
                branch.push(Pattern::Subquery(subq));
                branch.extend(bind_patterns);
                branches.push(branch);
            }
        }

        // Safety: if nothing binds ?__describe, force empty results to avoid a full graph scan.
        if branches.is_empty() {
            branches.push(vec![Pattern::Values {
                vars: vec![describe_var],
                rows: Vec::new(),
            }]);
        }

        let mut patterns = if branches.len() == 1 {
            branches.pop().unwrap_or_default()
        } else {
            vec![Pattern::Union(branches)]
        };

        // Outgoing triple expansion: ?__describe ?p ?o
        patterns.push(Pattern::Triple(TriplePattern::new(
            Ref::Var(describe_var),
            Ref::Var(p_var),
            Term::Var(o_var),
        )));

        Ok(ParsedQuery {
            context: ctx,
            orig_context: Some(ctx_val),
            output: QueryOutput::Construct(template),
            patterns,
            options: QueryOptions::default(),
            graph_select: None, // SPARQL doesn't support graph crawl
            post_values: None,
        })
    }

    fn describe_target_vars(
        &mut self,
        target: &DescribeTarget,
        where_patterns: &[Pattern],
    ) -> Vec<VarId> {
        match target {
            DescribeTarget::Resources(resources) => resources
                .iter()
                .filter_map(|r| match r {
                    VarOrIri::Var(v) => Some(self.register_var(v)),
                    VarOrIri::Iri(_) => None,
                })
                .collect(),
            DescribeTarget::Star => {
                use std::collections::BTreeSet;
                let mut vars = BTreeSet::new();
                for p in where_patterns {
                    for v in p.variables() {
                        vars.insert(v);
                    }
                }
                vars.into_iter().collect()
            }
        }
    }

    fn lower_describe_modifiers(
        &mut self,
        modifiers: &SolutionModifiers,
        span: SourceSpan,
        subq: &mut SubqueryPattern,
        select_vars: &[VarId],
    ) -> Result<()> {
        // DESCRIBE supports LIMIT/OFFSET/ORDER BY (but not GROUP BY/HAVING in this lowering).
        if modifiers.group_by.is_some() || modifiers.having.is_some() {
            return Err(LowerError::unsupported_form(
                "DESCRIBE with GROUP BY/HAVING",
                span,
            ));
        }

        let mut opts = QueryOptions::default();
        self.lower_base_modifiers(modifiers, &mut opts)?;

        // For simplicity and predictable performance, require ORDER BY vars to be part of the subquery select list.
        for spec in &opts.order_by {
            if !select_vars.contains(&spec.var) {
                return Err(LowerError::unsupported_form(
                    "DESCRIBE ORDER BY on non-target variables",
                    span,
                ));
            }
        }

        subq.limit = opts.limit;
        subq.offset = opts.offset;
        subq.order_by = opts.order_by;
        Ok(())
    }
}
