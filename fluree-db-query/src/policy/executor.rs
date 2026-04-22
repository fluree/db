//! Policy query executor implementation
//!
//! Implements `PolicyQueryExecutor` using the query engine asynchronously.

use crate::context::ExecutionContext;
use crate::execute::build_where_operators_seeded;
use crate::var_registry::VarRegistry;
use fluree_db_core::{GraphId, LedgerSnapshot, OverlayProvider, Sid};
use fluree_db_policy::{
    PolicyQuery, PolicyQueryExecutor, PolicyQueryFut, Result as PolicyResult,
    UNBOUND_IDENTITY_PREFIX,
};
use std::collections::HashMap;

/// Policy query executor that runs queries against a database
///
/// This executor converts `PolicyQuery` to the query engine's IR and
/// executes with a root context (no policy filtering).
pub struct QueryPolicyExecutor<'a> {
    /// The database snapshot to query
    pub snapshot: &'a LedgerSnapshot,
    /// Optional overlay provider (for staged flakes)
    pub overlay: Option<&'a dyn OverlayProvider>,
    /// Target transaction time
    pub to_t: i64,
    /// Graph ID for range queries (default: 0 = default graph)
    pub g_id: GraphId,
}

impl<'a> QueryPolicyExecutor<'a> {
    /// Create a new query executor for the default graph
    pub fn new(snapshot: &'a LedgerSnapshot) -> Self {
        Self {
            snapshot,
            overlay: None,
            to_t: snapshot.t,
            g_id: 0,
        }
    }

    /// Create a query executor with overlay support for the default graph
    pub fn with_overlay(
        snapshot: &'a LedgerSnapshot,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
    ) -> Self {
        Self {
            snapshot,
            overlay: Some(overlay),
            to_t,
            g_id: 0,
        }
    }

    /// Set the graph ID for range queries.
    ///
    /// Policy queries will execute against this graph instead of the default graph.
    pub fn with_graph_id(mut self, g_id: GraphId) -> Self {
        self.g_id = g_id;
        self
    }
}

impl PolicyQueryExecutor for QueryPolicyExecutor<'_> {
    fn evaluate_policy_query<'b>(
        &'b self,
        query: &'b PolicyQuery,
        bindings: &'b HashMap<String, Sid>,
    ) -> PolicyQueryFut<'b> {
        Box::pin(self.evaluate_async(query, bindings))
    }
}

impl QueryPolicyExecutor<'_> {
    /// Async implementation of policy query evaluation
    async fn evaluate_async(
        &self,
        query: &PolicyQuery,
        bindings: &HashMap<String, Sid>,
    ) -> PolicyResult<bool> {
        // Parse and lower the policy's f:query using the main query parser/IR.
        //
        // We intentionally do NOT implement a bespoke parser here; this ensures full
        // feature parity (e.g., FILTER patterns) and avoids divergence.
        //
        // Policy queries behave like existence checks, with:
        // - select forced to ["?$this"]
        // - limit forced to 1
        // - VALUES injected into WHERE for special variables (?$this, ?$identity, etc.)
        let mut query_json: serde_json::Value = serde_json::from_str(&query.json).map_err(|e| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Invalid policy query JSON: {e}"),
            }
        })?;

        let obj = query_json.as_object_mut().ok_or_else(|| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: "Policy query must be a JSON object".to_string(),
            }
        })?;

        // Force select + limit for policy queries
        obj.insert(
            "select".to_string(),
            serde_json::Value::Array(vec![serde_json::Value::String("?$this".to_string())]),
        );
        obj.insert("limit".to_string(), serde_json::Value::from(1));

        // Build VALUES clause JSON for special variables.
        // Inject VALUES into WHERE clause BEFORE parsing.
        // This ensures even empty queries (no WHERE) work - the VALUES provides the pattern.
        //
        // Format: ["values", [["?$this", "?$identity", ...], [[iri1, iri2, ...]]]]
        let mut var_names: Vec<String> = bindings.keys().cloned().collect();
        var_names.sort();

        // Build VALUES row with IRIs for each variable
        // Special case: unbound identity uses null (UNDEF) to ensure it never matches
        let values_row: Vec<serde_json::Value> = var_names
            .iter()
            .map(|name| {
                let sid = bindings.get(name).expect("binding value exists");
                // Check if this is an unbound identity - use null (UNDEF) instead of IRI
                // This ensures patterns referencing ?$identity won't match anything
                if sid.name.starts_with(UNBOUND_IDENTITY_PREFIX) {
                    return serde_json::Value::Null;
                }
                // Decode SID to IRI for JSON representation
                let iri = self
                    .snapshot
                    .decode_sid(sid)
                    .unwrap_or_else(|| sid.name.to_string());
                serde_json::json!({"@id": iri})
            })
            .collect();

        let values_clause = serde_json::json!(["values", [var_names.clone(), [values_row]]]);

        // Inject VALUES into WHERE clause (or create WHERE if missing)
        let where_clause = obj.get_mut("where");
        match where_clause {
            Some(serde_json::Value::Array(arr)) => {
                // WHERE is an array - prepend VALUES
                arr.insert(0, values_clause);
            }
            Some(serde_json::Value::Object(_)) => {
                // WHERE is an object (single pattern) - wrap in array with VALUES
                let existing = obj.remove("where").unwrap();
                obj.insert(
                    "where".to_string(),
                    serde_json::json!([values_clause, existing]),
                );
            }
            Some(_) | None => {
                // No WHERE or invalid - create with just VALUES
                // This handles empty queries like {}
                obj.insert("where".to_string(), serde_json::json!([values_clause]));
            }
        }

        // Create a variable registry for this query execution
        let mut vars = VarRegistry::new();

        // Pre-register special variables so they are present even if not referenced.
        // This matches the "always ground" behavior.
        for var_name in &var_names {
            vars.get_or_insert(var_name);
        }

        let parsed = crate::parse::parse_query(&query_json, self.snapshot, &mut vars, None)
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Failed to parse policy query: {e}"),
            })?;

        let patterns = parsed.patterns;

        // Create the execution context WITHOUT policy (root context)
        // This is critical - policy queries must not be filtered by policy
        let ctx = if let Some(overlay) = self.overlay {
            ExecutionContext::with_time_and_overlay(self.snapshot, &vars, self.to_t, None, overlay)
                .with_graph_id(self.g_id)
        } else {
            ExecutionContext::with_time(self.snapshot, &vars, self.to_t, None)
                .with_graph_id(self.g_id)
        };

        // Build the where clause operators (VALUES is now part of parsed patterns)
        let mut operator =
            build_where_operators_seeded(None, &patterns, None, None).map_err(|e| {
                fluree_db_policy::PolicyError::QueryExecution {
                    message: e.to_string(),
                }
            })?;

        // Execute with limit 1 (we only need to know if there are any results)
        operator
            .open(&ctx)
            .await
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: e.to_string(),
            })?;

        // Check if there's at least one result
        let has_results = match operator.next_batch(&ctx).await {
            Ok(Some(batch)) => !batch.is_empty(),
            Ok(None) => false,
            Err(e) => {
                operator.close();
                return Err(fluree_db_policy::PolicyError::QueryExecution {
                    message: e.to_string(),
                });
            }
        };

        operator.close();

        Ok(has_results)
    }
}
