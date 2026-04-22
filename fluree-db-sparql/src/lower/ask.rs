//! ASK query lowering.
//!
//! Converts SPARQL ASK queries to `ParsedQuery` with `SelectMode::Boolean`.
//! ASK tests whether a graph pattern has any solution — no variables are projected.

use crate::ast::query::AskQuery;

use fluree_db_query::options::QueryOptions;
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::parse::{ParsedQuery, QueryOutput};

use super::{LoweringContext, Result};

impl<E: IriEncoder> LoweringContext<'_, E> {
    /// Lower an ASK query to a ParsedQuery.
    pub(super) fn lower_ask(&mut self, ask: &AskQuery) -> Result<ParsedQuery> {
        // Lower WHERE clause patterns
        let patterns = self.lower_graph_pattern(&ask.where_clause.pattern)?;

        // Lower any solution modifiers the parser accepted (ORDER BY, LIMIT, OFFSET).
        // Per SPARQL spec, these are meaningless for ASK — we override LIMIT below.
        let mut options = QueryOptions::default();
        self.lower_base_modifiers(&ask.modifiers, &mut options)?;

        // Override to LIMIT 1 — ASK only needs to know if any solution exists
        options.limit = Some(1);

        let ctx = self.build_jsonld_context()?;

        Ok(ParsedQuery {
            context: ctx,
            orig_context: None,
            output: QueryOutput::Boolean,
            patterns,
            options,
            graph_select: None,
            post_values: None,
        })
    }
}
