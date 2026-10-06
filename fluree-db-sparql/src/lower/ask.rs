//! ASK query lowering.
//!
//! Converts SPARQL ASK queries to `Query` with `SelectMode::Ask`.
//! ASK tests whether a graph pattern has any solution — no variables are projected.

use crate::ast::query::{AskQuery, SelectClause, SelectVariables};

use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;

use super::select::BaseModifiers;
use super::{post_values_then, LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower an ASK query to a Query.
    pub(super) fn lower_ask(&mut self, ask: &AskQuery) -> Result<Query> {
        // Lower WHERE clause patterns
        let mut patterns = self.lower_graph_pattern(&ask.where_clause.pattern)?;
        let values = self.lower_trailing_values(ask.values.as_deref(), &mut patterns)?;

        // GROUP BY, HAVING and an aggregate ORDER BY group the level
        // (§18.2.4.1), which changes the answer: ASK is then true when some
        // group passes HAVING (`ASK { … } HAVING (false)` is false). The level
        // lowers like a SELECT level that projects nothing.
        let select = SelectClause {
            modifier: None,
            variables: SelectVariables::Explicit(Vec::new()),
            span: ask.span,
        };
        let level =
            self.lower_select_level(&select, &ask.modifiers, &mut patterns, values.as_ref())?;
        let post_values = post_values_then(values, level.binds, &mut patterns);

        let ctx = self.build_jsonld_context()?;

        // ASK is true when a solution remains after the solution modifiers.
        // ORDER BY cannot change that and is dropped; OFFSET and LIMIT can
        // (`OFFSET 1` over one solution, `LIMIT 0`), so OFFSET applies and LIMIT
        // becomes at most 1, which stops at the first remaining solution (the
        // first surviving group, when the level groups).
        let BaseModifiers { limit, offset, .. } = level.base;
        Ok(Query {
            context: ctx,
            orig_context: None,
            output: QueryOutput::Ask,
            patterns,
            reasoning: self.reasoning_config()?,
            grouping: level.grouping,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: Some(limit.map_or(1, |limit| limit.min(1))),
            offset,
            post_values,
            include_system_facts: false,
            union_default_graph: None,
            cypher_vocab: None,
            unmatched_optional: Default::default(),
        })
    }
}
