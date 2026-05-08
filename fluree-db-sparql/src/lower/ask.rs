//! ASK query lowering.
//!
//! Converts SPARQL ASK queries to `Query` with `SelectMode::Ask`.
//! ASK tests whether a graph pattern has any solution — no variables are projected.

use crate::ast::query::AskQuery;

use fluree_db_query::ir::QueryOptions;
use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::parse::encode::IriEncoder;

use super::{LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower an ASK query to a Query.
    pub(super) fn lower_ask(&mut self, ask: &AskQuery) -> Result<Query> {
        // Lower WHERE clause patterns
        let patterns = self.lower_graph_pattern(&ask.where_clause.pattern)?;

        // Per SPARQL spec, ORDER BY / LIMIT / OFFSET are meaningless for ASK
        // (the result is a single boolean), so we discard whatever the parser
        // accepted and set LIMIT 1 to short-circuit at the first solution.
        let _ = self.lower_base_modifiers(&ask.modifiers)?;

        let ctx = self.build_jsonld_context()?;

        Ok(Query {
            context: ctx,
            orig_context: None,
            output: QueryOutput::Ask,
            patterns,
            options: QueryOptions::default(),
            grouping: None,
            ordering: Vec::new(),
            limit: Some(1),
            offset: None,
            post_values: None,
        })
    }
}
