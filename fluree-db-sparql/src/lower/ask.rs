//! ASK query lowering.
//!
//! Converts SPARQL ASK queries to `Query` with `SelectMode::Ask`.
//! ASK tests whether a graph pattern has any solution — no variables are projected.

use crate::ast::query::{AskQuery, SelectClause, SelectVariables};

use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;

use super::select::LoweredSelectLevel;
use super::{LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower an ASK query to a Query.
    pub(super) fn lower_ask(&mut self, ask: &AskQuery) -> Result<Query> {
        // Lower WHERE clause patterns
        let mut patterns = self.lower_graph_pattern(&ask.where_clause.pattern)?;

        // GROUP BY, HAVING and an aggregate ORDER BY group the level
        // (§18.2.4.1), which changes the answer: ASK is then true when some
        // group passes HAVING (`ASK { … } HAVING (false)` is false). The level
        // lowers like a SELECT level that projects nothing.
        let select = SelectClause {
            modifier: None,
            variables: SelectVariables::Explicit(Vec::new()),
            span: ask.span,
        };
        let level = self.lower_select_level(&select, &ask.modifiers, &mut patterns, None)?;
        patterns.extend(LoweredSelectLevel::bind_patterns(level.binds));

        let ctx = self.build_jsonld_context()?;

        // Per SPARQL spec, ORDER BY / LIMIT / OFFSET are meaningless for ASK
        // (the result is a single boolean), so we discard whatever the parser
        // accepted and set LIMIT 1 to short-circuit at the first solution (the
        // first surviving group, when the level groups).
        Ok(Query {
            context: ctx,
            orig_context: None,
            output: QueryOutput::Ask,
            patterns,
            reasoning: self.reasoning_config()?,
            grouping: level.grouping,
            ordering: Vec::new(),
            order_binds: Vec::new(),
            limit: Some(1),
            offset: None,
            post_values: None,
            include_system_facts: false,
            union_default_graph: None,
            cypher_vocab: None,
            unmatched_optional: Default::default(),
        })
    }
}
