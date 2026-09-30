//! ASK query lowering.
//!
//! Converts SPARQL ASK queries to `Query` with `SelectMode::Ask`.
//! ASK tests whether a graph pattern has any solution — no variables are projected.

use crate::ast::query::AskQuery;

use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;

use super::{LowerError, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower an ASK query to a Query.
    pub(super) fn lower_ask(&mut self, ask: &AskQuery) -> Result<Query> {
        // GROUP BY / HAVING change the answer (`ASK { … } HAVING (false)` is
        // false), and this lowering has no grouping stage: refuse them rather
        // than answer as if they were absent.
        if ask.modifiers.group_by.is_some() || ask.modifiers.having.is_some() {
            return Err(LowerError::unsupported_form(
                "ASK with GROUP BY/HAVING",
                ask.span,
            ));
        }

        // Lower WHERE clause patterns
        let patterns = self.lower_graph_pattern(&ask.where_clause.pattern)?;

        // Per SPARQL spec, ORDER BY / LIMIT / OFFSET are meaningless for ASK
        // (the result is a single boolean), so we discard whatever the parser
        // accepted and set LIMIT 1 to short-circuit at the first solution. An
        // aggregate ORDER BY groups the level, which does change the answer.
        let base = self.lower_base_modifiers(&ask.modifiers)?;
        if !base.deferred_order_exprs.is_empty() {
            return Err(LowerError::unsupported_form(
                "aggregate ORDER BY in ASK",
                ask.span,
            ));
        }

        let ctx = self.build_jsonld_context()?;

        Ok(Query {
            context: ctx,
            orig_context: None,
            output: QueryOutput::Ask,
            patterns,
            reasoning: self.reasoning_config()?,
            grouping: None,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: Some(1),
            offset: None,
            post_values: None,
            include_system_facts: false,
            cypher_vocab: None,
            unmatched_optional: Default::default(),
        })
    }
}
