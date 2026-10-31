//! DESCRIBE query lowering.
//!
//! SPARQL DESCRIBE has implementation-defined semantics in the spec. For Fluree DB-R we
//! implement a fast, benchmark-friendly interpretation:
//! - Identify resources to describe (variables from WHERE results and/or explicit IRIs)
//! - For each resource, return all outgoing triples: `?res ?p ?o`
//! - For each blank-node object, its own outgoing triples, and when it heads a
//!   list, those of every cell after it: a label the result alone names would
//!   otherwise describe nothing
//!
//! This is lowered into the common query IR as a CONSTRUCT-like query with:
//! - A synthetic `?__describe` variable bound to resources
//! - A triple pattern that expands to outgoing triples for each bound resource

use crate::ast::query::{DescribeQuery, DescribeTarget, SolutionModifiers, VarOrIri};
use crate::SourceSpan;

use fluree_db_query::binding::Binding;
use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_query::ir::{
    ConstructTemplate as QueryConstructTemplate, Expression, Function, PathModifier, Pattern,
    PropertyPathPattern, Query, QueryOutput, SubqueryPattern,
};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use super::select::BaseModifiers;
use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower a DESCRIBE query to a Query.
    pub(super) fn lower_describe(&mut self, describe: &DescribeQuery) -> Result<Query> {
        // DESCRIBE never groups, whatever its targets.
        if describe.modifiers.group_by.is_some() || describe.modifiers.having.is_some() {
            return Err(LowerError::unsupported_form(
                "DESCRIBE with GROUP BY/HAVING",
                describe.span,
            ));
        }
        let ctx = self.build_jsonld_context()?;
        let ctx_val = self.build_jsonld_context_value();

        // Synthetic variables used internally for DESCRIBE expansion.
        //
        // NOTE: These must be in the shared VarRegistry so the executor/formatters can
        // reference them consistently. They are not projected in output (CONSTRUCT graph).
        let describe_var = self.vars.get_or_insert("?__describe");
        let p_var = self.vars.get_or_insert("?__describe_p");
        let o_var = self.vars.get_or_insert("?__describe_o");
        let cell_var = self.vars.get_or_insert("?__describe_cell");
        let cell_p_var = self.vars.get_or_insert("?__describe_cell_p");
        let cell_o_var = self.vars.get_or_insert("?__describe_cell_o");

        // CONSTRUCT template: { ?__describe ?__describe_p ?__describe_o .
        //                       ?__describe_cell ?__describe_cell_p ?__describe_cell_o }
        let mut template = QueryConstructTemplate::new(vec![
            TriplePattern::new(Ref::Var(describe_var), Ref::Var(p_var), Term::Var(o_var)),
            TriplePattern::new(
                Ref::Var(cell_var),
                Ref::Var(cell_p_var),
                Term::Var(cell_o_var),
            ),
        ]);

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

        // 2) WHERE-derived targets (DESCRIBE ?x WHERE { ... } / DESCRIBE * WHERE { ... }).
        // A trailing VALUES clause joins the WHERE's solutions right after it,
        // as in a sub-SELECT (DESCRIBE never groups); without a WHERE it joins
        // the one empty solution (`DESCRIBE ?x VALUES ?x { <a> <b> }`).
        let where_patterns = match (&describe.where_clause, describe.values.as_deref()) {
            (None, None) => None,
            (where_clause, values) => {
                // Lower WHERE clause patterns first (so variable IDs are registered).
                let mut patterns = match where_clause {
                    Some(where_clause) => self.lower_graph_pattern(&where_clause.pattern)?,
                    None => Vec::new(),
                };
                if let Some(values) = values {
                    patterns.extend(self.lower_graph_pattern(values)?);
                }
                Some(patterns)
            }
        };
        if let Some(where_patterns) = where_patterns {
            let target_vars = self.describe_target_vars(&describe.target, &where_patterns);
            if !target_vars.is_empty() {
                let mut subq = SubqueryPattern::new(target_vars.clone(), where_patterns);
                subq.distinct = true;
                subq.uncorrelated = true;
                self.lower_describe_modifiers(
                    &describe.modifiers,
                    describe.span,
                    &mut subq,
                    &target_vars,
                )?;

                // Map each selected target var to ?__describe. A target a
                // solution leaves unbound describes nothing: the expansion scan
                // would read an unbound ?__describe as a free subject.
                let describe_target = |v: VarId| {
                    [
                        Pattern::Filter(Expression::Call {
                            func: Function::Bound,
                            args: vec![Expression::Var(v)],
                        }),
                        Pattern::Bind {
                            var: describe_var,
                            expr: Expression::Var(v),
                        },
                    ]
                };
                let bind_patterns = if target_vars.len() == 1 {
                    describe_target(target_vars[0]).to_vec()
                } else {
                    let inner = target_vars
                        .iter()
                        .map(|v| describe_target(*v).to_vec())
                        .collect();
                    vec![Pattern::Union(inner)]
                };

                let mut branch = Vec::with_capacity(1 + bind_patterns.len());
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
        // OPTIONAL { FILTER(isBlank(?o)) ?o rdf:rest* ?cell . ?cell ?cp ?co }
        if let Some(rest) = self.encoder.encode_iri(fluree_vocab::rdf::REST) {
            patterns.push(Pattern::Optional(vec![
                Pattern::Filter(Expression::Call {
                    func: Function::IsBlank,
                    args: vec![Expression::Var(o_var)],
                }),
                Pattern::PropertyPath(PropertyPathPattern::new(
                    Ref::Var(o_var),
                    rest,
                    PathModifier::ZeroOrMore,
                    Ref::Var(cell_var),
                )),
                Pattern::Triple(TriplePattern::new(
                    Ref::Var(cell_var),
                    Ref::Var(cell_p_var),
                    Term::Var(cell_o_var),
                )),
            ]));
        }
        template.mark_list_entries(&patterns, self.vars);

        Ok(Query {
            context: ctx,
            orig_context: Some(ctx_val),
            output: QueryOutput::Construct(template),
            patterns,
            reasoning: self.reasoning_config()?,
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: None,
            offset: None,
            post_values: None,
            include_system_facts: false,
            union_default_graph: None,
            cypher_vocab: None,
            unmatched_optional: Default::default(),
            rdf_list_view: false,
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
                    for v in p.produced_vars() {
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
        let BaseModifiers {
            limit,
            offset,
            ordering,
            order_binds,
            deferred_order_exprs,
        } = self.lower_base_modifiers(modifiers)?;

        // DESCRIBE restricts ORDER BY to target variables (see below); an
        // expression-based ORDER BY (aggregate-free or aggregate-bearing) would
        // sort on a synthetic, non-target var.
        if !order_binds.is_empty() || !deferred_order_exprs.is_empty() {
            return Err(LowerError::unsupported_form(
                "DESCRIBE ORDER BY by expression",
                span,
            ));
        }

        // For simplicity and predictable performance, require ORDER BY vars to be part of the subquery select list.
        for spec in &ordering {
            if !select_vars.contains(&spec.var) {
                return Err(LowerError::unsupported_form(
                    "DESCRIBE ORDER BY on non-target variables",
                    span,
                ));
            }
        }

        subq.limit = limit;
        subq.offset = offset;
        subq.ordering = ordering;
        Ok(())
    }
}
