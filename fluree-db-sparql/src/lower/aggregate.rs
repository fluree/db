//! Aggregate extraction and lowering.
//!
//! Handles extraction of aggregate specifications from SELECT clauses,
//! mapping SPARQL aggregate functions to engine functions, and collecting
//! aggregates referenced in HAVING conditions.

use crate::ast::expr::{AggregateFunction, Expression};
use crate::ast::query::{SelectClause, SelectVariable, SelectVariables};

use fluree_db_query::ir::Pattern;
use fluree_db_query::ir::{AggregateFn, AggregateSpec, InputSemantics};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    fn iri_key(iri: &crate::ast::term::Iri) -> String {
        use crate::ast::term::IriValue;
        match &iri.value {
            IriValue::Full(s) => format!("<{s}>"),
            IriValue::Prefixed { prefix, local } => format!("{prefix}:{local}"),
        }
    }

    /// Build a span-free structural key for an expression.
    ///
    /// This is used to de-duplicate aggregate-input BINDs and to build stable
    /// aggregate alias keys (HAVING → SELECT aggregate lookup).
    fn expr_key_no_span(expr: &Expression) -> String {
        use crate::ast::term::LiteralValue;

        match expr.unwrap_bracketed() {
            Expression::Var(v) => format!("?{}", v.name),
            Expression::Literal(lit) => match &lit.value {
                LiteralValue::Simple(s) => format!("\"{s}\""),
                LiteralValue::LangTagged { value, lang } => format!("\"{value}\"@{lang}"),
                LiteralValue::Typed { value, datatype } => {
                    format!("\"{}\"^^{}", value, Self::iri_key(datatype))
                }
                LiteralValue::Integer(i) => format!("{i}"),
                LiteralValue::Decimal(d) => format!("{d}"),
                LiteralValue::Double(d) => format!("{d}"),
                LiteralValue::Boolean(b) => format!("{b}"),
            },
            Expression::Iri(i) => Self::iri_key(i),
            Expression::Unary { op, operand, .. } => {
                format!("({}{})", op.as_str(), Self::expr_key_no_span(operand))
            }
            Expression::Binary {
                op, left, right, ..
            } => format!(
                "({}{}{})",
                Self::expr_key_no_span(left),
                op.as_str(),
                Self::expr_key_no_span(right)
            ),
            Expression::FunctionCall {
                name,
                args,
                distinct,
                ..
            } => {
                use crate::ast::expr::FunctionName;
                let name_key = match name {
                    FunctionName::Extension(iri) => format!("EXT{}", Self::iri_key(iri)),
                    other => format!("{other:?}"),
                };
                let args_key = args
                    .iter()
                    .map(Self::expr_key_no_span)
                    .collect::<Vec<_>>()
                    .join(",");
                format!("CALL[{name_key};distinct={distinct}]({args_key})")
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => format!(
                "IF({},{},{})",
                Self::expr_key_no_span(condition),
                Self::expr_key_no_span(then_expr),
                Self::expr_key_no_span(else_expr)
            ),
            Expression::Coalesce { args, .. } => format!(
                "COALESCE({})",
                args.iter()
                    .map(Self::expr_key_no_span)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Expression::In {
                expr,
                list,
                negated,
                ..
            } => format!(
                "{}IN[neg={}]({};{})",
                if *negated { "NOT_" } else { "" },
                negated,
                Self::expr_key_no_span(expr),
                list.iter()
                    .map(Self::expr_key_no_span)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Expression::Exists { .. } => "EXISTS{...}".to_string(),
            Expression::NotExists { .. } => "NOT_EXISTS{...}".to_string(),
            Expression::Aggregate { .. } => "AGG{...}".to_string(),
            Expression::Bracketed { inner, .. } => Self::expr_key_no_span(inner),
        }
    }

    fn lower_aggregate_input_var(
        &mut self,
        expr: &Option<Box<Expression>>,
        pre_binds: &mut Vec<Pattern>,
    ) -> Result<Option<VarId>> {
        match expr {
            None => Ok(None),
            Some(inner) => match inner.unwrap_bracketed() {
                Expression::Var(v) => Ok(Some(self.register_var(v))),
                other => {
                    let key = Self::expr_key_no_span(other);
                    if let Some(existing) = self.agg_expr_binds.get(&key) {
                        return Ok(Some(*existing));
                    }

                    let lowered = self.lower_expression(other)?;
                    let var_name = format!("?__agg_expr_{}", self.agg_counter);
                    self.agg_counter += 1;
                    let var_id = self.vars.get_or_insert(&var_name);
                    pre_binds.push(Pattern::Bind {
                        var: var_id,
                        expr: lowered,
                    });
                    self.agg_expr_binds.insert(key, var_id);
                    Ok(Some(var_id))
                }
            },
        }
    }

    pub(super) fn aggregate_key(&self, agg: &Expression) -> Result<String> {
        let Expression::Aggregate {
            function,
            expr,
            distinct,
            separator,
            ..
        } = agg
        else {
            return Err(LowerError::not_implemented(
                "aggregate_key called on non-Aggregate expression",
                agg.span(),
            ));
        };
        let input = match expr {
            Some(inner) => Self::expr_key_no_span(inner),
            None => "*".to_string(),
        };
        let sep = separator.as_deref().unwrap_or("");
        Ok(format!(
            "{}|{}|{}|{}",
            function.as_str(),
            input,
            *distinct,
            sep
        ))
    }

    pub(super) fn build_aggregate_aliases(
        &mut self,
        select: &SelectClause,
    ) -> Result<HashMap<String, VarId>> {
        let mut aliases = HashMap::new();

        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                if let SelectVariable::Expr {
                    expr: expr @ Expression::Aggregate { .. },
                    alias,
                    ..
                } = var
                {
                    let key = self.aggregate_key(expr)?;
                    let var_id = self.register_var(alias);
                    aliases.insert(key, var_id);
                }
            }
        }

        Ok(aliases)
    }

    pub(super) fn aggregate_spec_from_expr(
        &mut self,
        agg: &Expression,
        output_var: VarId,
        pre_binds: &mut Vec<Pattern>,
    ) -> Result<AggregateSpec> {
        let Expression::Aggregate {
            function,
            expr,
            distinct,
            separator,
            span,
        } = agg
        else {
            return Err(LowerError::not_implemented(
                "aggregate_spec_from_expr called on non-Aggregate expression",
                agg.span(),
            ));
        };

        let input_var = self.lower_aggregate_input_var(expr, pre_binds)?;
        let semantics = if *distinct {
            InputSemantics::Set
        } else {
            InputSemantics::List
        };
        let function = match (function, input_var) {
            // COUNT(*) — DISTINCT * is not meaningful for COUNT.
            (AggregateFunction::Count, None) => {
                if *distinct {
                    return Err(LowerError::not_implemented("COUNT(DISTINCT *)", *span));
                }
                AggregateFn::CountAll
            }
            // Every non-COUNT aggregate needs an input variable. The caller
            // is responsible for ensuring `expr` is `Some(_)`; reaching this
            // arm with `None` means a malformed AST.
            (_, None) => {
                return Err(LowerError::not_implemented(
                    "aggregate without input expression (only COUNT(*) supports that)",
                    *span,
                ));
            }
            (AggregateFunction::Count, Some(v)) => {
                if *distinct {
                    AggregateFn::CountDistinct(v)
                } else {
                    AggregateFn::Count(v)
                }
            }
            (AggregateFunction::Sum, Some(v)) => AggregateFn::Sum(v, semantics),
            (AggregateFunction::Avg, Some(v)) => AggregateFn::Avg(v, semantics),
            // DISTINCT is a semantic no-op for Min/Max/Sample; drop it at
            // the IR boundary so the variant invariant holds.
            (AggregateFunction::Min, Some(v)) => AggregateFn::Min(v),
            (AggregateFunction::Max, Some(v)) => AggregateFn::Max(v),
            (AggregateFunction::Sample, Some(v)) => AggregateFn::Sample(v),
            (AggregateFunction::GroupConcat, Some(v)) => AggregateFn::GroupConcat {
                input: v,
                semantics,
                separator: separator.as_deref().unwrap_or(" ").to_string(),
            },
        };

        Ok(AggregateSpec {
            function,
            output_var,
        })
    }

    pub(super) fn collect_having_aggregates(
        &mut self,
        expr: &Expression,
        aliases: &mut HashMap<String, VarId>,
        aggregates: &mut Vec<AggregateSpec>,
        pre_binds: &mut Vec<Pattern>,
    ) -> Result<()> {
        match expr.unwrap_bracketed() {
            agg @ Expression::Aggregate { .. } => {
                let key = self.aggregate_key(agg)?;
                if !aliases.contains_key(&key) {
                    let output_var = self
                        .vars
                        .get_or_insert(&format!("?__having_agg_{}", aliases.len()));
                    let spec = self.aggregate_spec_from_expr(agg, output_var, pre_binds)?;
                    aliases.insert(key, output_var);
                    aggregates.push(spec);
                }
                Ok(())
            }
            Expression::Binary { left, right, .. } => {
                self.collect_having_aggregates(left, aliases, aggregates, pre_binds)?;
                self.collect_having_aggregates(right, aliases, aggregates, pre_binds)?;
                Ok(())
            }
            Expression::Unary { operand, .. } => {
                self.collect_having_aggregates(operand, aliases, aggregates, pre_binds)
            }
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    self.collect_having_aggregates(arg, aliases, aggregates, pre_binds)?;
                }
                Ok(())
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                self.collect_having_aggregates(condition, aliases, aggregates, pre_binds)?;
                self.collect_having_aggregates(then_expr, aliases, aggregates, pre_binds)?;
                self.collect_having_aggregates(else_expr, aliases, aggregates, pre_binds)
            }
            Expression::Coalesce { args, .. } => {
                for arg in args {
                    self.collect_having_aggregates(arg, aliases, aggregates, pre_binds)?;
                }
                Ok(())
            }
            Expression::In { expr, list, .. } => {
                self.collect_having_aggregates(expr, aliases, aggregates, pre_binds)?;
                for arg in list {
                    self.collect_having_aggregates(arg, aliases, aggregates, pre_binds)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn expr_references_vars(&self, expr: &Expression, vars: &HashSet<Arc<str>>) -> bool {
        match expr.unwrap_bracketed() {
            Expression::Var(var) => vars.contains(&var.name),
            Expression::Literal(_) | Expression::Iri(_) => false,
            Expression::Unary { operand, .. } => self.expr_references_vars(operand, vars),
            Expression::Binary { left, right, .. } => {
                self.expr_references_vars(left, vars) || self.expr_references_vars(right, vars)
            }
            Expression::FunctionCall { args, .. } => {
                args.iter().any(|a| self.expr_references_vars(a, vars))
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                self.expr_references_vars(condition, vars)
                    || self.expr_references_vars(then_expr, vars)
                    || self.expr_references_vars(else_expr, vars)
            }
            Expression::Coalesce { args, .. } => {
                args.iter().any(|a| self.expr_references_vars(a, vars))
            }
            Expression::In { expr, list, .. } => {
                self.expr_references_vars(expr, vars)
                    || list.iter().any(|a| self.expr_references_vars(a, vars))
            }
            Expression::Exists { .. }
            | Expression::NotExists { .. }
            | Expression::Aggregate { .. } => false,
            Expression::Bracketed { inner, .. } => self.expr_references_vars(inner, vars),
        }
    }

    /// Extract aggregate specifications from SELECT clause.
    ///
    /// Walks the SELECT variables looking for aggregate expressions like:
    ///   SELECT (COUNT(?x) AS ?count) ...
    ///
    /// Returns AggregateSpecs for each aggregate found.
    pub(super) fn extract_aggregates(
        &mut self,
        select: &SelectClause,
    ) -> Result<(Vec<AggregateSpec>, Vec<Pattern>)> {
        let mut aggregates = Vec::new();
        let mut pre_binds: Vec<Pattern> = Vec::new();

        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                if let SelectVariable::Expr {
                    expr: expr @ Expression::Aggregate { .. },
                    alias,
                    ..
                } = var
                {
                    let output_var = self.register_var(alias);
                    let spec = self.aggregate_spec_from_expr(expr, output_var, &mut pre_binds)?;
                    aggregates.push(spec);
                }
            }
        }

        Ok((aggregates, pre_binds))
    }

    /// Collect non-aggregate SELECT variables for implicit GROUP BY.
    ///
    /// When a query has aggregates but no explicit GROUP BY, SPARQL requires
    /// all non-aggregated variables in SELECT to be grouped.
    pub(super) fn collect_non_aggregate_select_vars(
        &mut self,
        select: &SelectClause,
    ) -> Vec<VarId> {
        let mut group_vars = Vec::new();

        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                match var {
                    SelectVariable::Var(v) => {
                        // Plain variables are non-aggregate
                        let var_id = self.register_var(v);
                        group_vars.push(var_id);
                    }
                    SelectVariable::Expr { expr, .. } => {
                        // Skip aggregate expressions - they don't go in GROUP BY
                        if !matches!(expr, Expression::Aggregate { .. }) {
                            // For non-aggregate expressions with alias, we could
                            // add the alias var, but for MVP this is a complex case
                            // that requires BIND semantics - skip for now
                        }
                    }
                }
            }
        }

        group_vars
    }
}
