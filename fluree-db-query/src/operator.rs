//! Operator trait and lifecycle types for query execution.
//!
//! Operators form a tree that produces batches of results through the
//! `open/next_batch/close` lifecycle pattern.

pub mod inline;

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// Query execution operator
///
/// Operators follow a lifecycle pattern for resource control:
/// 1. `open()` - Initialize state, allocate buffers
/// 2. `next_batch()` - Pull batches until exhausted (returns None)
/// 3. `close()` - Release resources
///
/// # Schema Contract
///
/// - `schema()` returns the output variables, fixed at construction
/// - All batches from `next_batch()` have columns in schema order
/// - Schema contains no duplicate VarIds
///
/// Call `open`, then loop on `next_batch` until `None`, then `close`.
#[async_trait]
pub trait Operator: Send + Sync {
    /// Output schema - which variables this operator produces
    ///
    /// Fixed at construction time (does not change across batches).
    /// Batch columns are in this order.
    fn schema(&self) -> &[VarId];

    /// Initialize operator state
    ///
    /// Called once before `next_batch()`. Allocates buffers, opens
    /// child operators, etc.
    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()>;

    /// Pull next batch of results
    ///
    /// Returns `Ok(Some(batch))` with results, or `Ok(None)` when exhausted.
    /// Batch columns are ordered according to `schema()`.
    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>>;

    /// Release resources
    ///
    /// Called when operator is no longer needed. Closes child operators,
    /// releases buffers, etc.
    fn close(&mut self);

    /// Estimated cardinality (for planning/EXPLAIN)
    ///
    /// Returns estimated number of rows this operator will produce.
    /// Used by the planner for cost estimation and by EXPLAIN for display.
    fn estimated_rows(&self) -> Option<usize> {
        None
    }

    /// Consume all remaining output rows to exhaustion and return the total count.
    ///
    /// # Contract
    ///
    /// - **Exhaustion**: Must consume the operator fully. After returning `Some(n)`,
    ///   the operator is exhausted — `next_batch()` must return `None`.
    /// - **Semantic fidelity**: The count must equal the number of rows the normal
    ///   `next_batch()` loop would have produced. All operator semantics (filtering,
    ///   Unbound/Poisoned handling, SPARQL substitution) must be preserved.
    /// - **Fallback**: Return `Ok(None)` when count-only mode cannot guarantee the
    ///   above — the caller falls back to the normal `next_batch()` drain loop.
    ///
    /// Operators that implement this avoid materializing output batches, reducing
    /// allocation and cloning overhead when the downstream consumer only needs a count.
    async fn drain_count(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<u64>> {
        Ok(None)
    }

    // ------------------------------------------------------------------
    // EXPLAIN introspection (never called on the hot path)
    // ------------------------------------------------------------------
    //
    // `describe()` renders the *planned* physical plan from the built (but not
    // opened) operator tree. Composite operators override `plan_children()` to
    // expose their inputs so the tree connects; leaves can add `plan_details()`.
    // Decisions finalized at `open()` (multi-graph hash→nested-loop downgrade,
    // fast-path-vs-fallback, the actual index permutation) are NOT reflected
    // here — that is `EXPLAIN ANALYZE` territory. See `docs`/the plan note.

    /// Operator display name. Defaults to the bare Rust type name.
    fn op_name(&self) -> String {
        crate::plan_node::short_type_name(std::any::type_name_of_val(self)).to_string()
    }

    /// Operator-specific attributes for the plan node (join var, predicate,
    /// chosen index hint, …). Default: none.
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }

    /// Child operators this node will consume, tagged with their edge kind.
    /// Override on every operator that owns inputs, or the rendered plan tree
    /// truncates at this node. Default: none (leaf).
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        Vec::new()
    }

    /// Render this operator (and, recursively, its children) as a [`PlanNode`].
    /// Provided — operators customize via `op_name`/`plan_details`/`plan_children`.
    fn describe(&self) -> crate::plan_node::PlanNode {
        crate::plan_node::PlanNode {
            op: self.op_name(),
            est_rows: self.estimated_rows(),
            details: self.plan_details(),
            children: self
                .plan_children()
                .into_iter()
                .map(|c| crate::plan_node::PlanEdge {
                    rel: c.rel,
                    node: c.op.describe(),
                })
                .collect(),
        }
    }
}

/// Boxed operator for dynamic dispatch
pub type BoxedOperator = Box<dyn Operator + Send + Sync>;

/// Operator state for lifecycle tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorState {
    /// Not yet opened
    Created,
    /// Opened and ready to produce batches
    Open,
    /// Exhausted (next_batch returned None)
    Exhausted,
    /// Closed
    Closed,
}

impl OperatorState {
    /// Check if operator can be opened
    pub fn can_open(&self) -> bool {
        matches!(self, OperatorState::Created)
    }

    /// Check if operator can produce batches
    pub fn can_next(&self) -> bool {
        matches!(self, OperatorState::Open)
    }

    /// Check if operator is closed
    pub fn is_closed(&self) -> bool {
        matches!(self, OperatorState::Closed)
    }
}

// ============================================================================
// Projection trimming helpers
// ============================================================================
//
// These free functions implement the `with_out_schema` / `trim_output`
// pattern used by operators that support projection pushdown.  Each operator
// stores an `Option<Arc<[VarId]>>` computed at construction time and uses
// these helpers to trim its output schema and batches.

/// Intersect a full schema with downstream requirements, preserving order.
///
/// Returns `None` when `downstream` is `None` (no trimming requested).
pub fn compute_trimmed_vars(
    full_schema: &[VarId],
    downstream: Option<&[VarId]>,
) -> Option<Arc<[VarId]>> {
    downstream.map(|dv| {
        let trimmed: Vec<VarId> = full_schema
            .iter()
            .filter(|v| dv.contains(v))
            .copied()
            .collect();
        Arc::from(trimmed.into_boxed_slice())
    })
}

/// Return the trimmed schema if set, otherwise the full schema.
pub fn effective_schema<'a>(trimmed: &'a Option<Arc<[VarId]>>, full: &'a [VarId]) -> &'a [VarId] {
    trimmed.as_deref().unwrap_or(full)
}

/// Trim a batch to only the required variables, or pass through unchanged.
pub fn trim_batch(out_schema: &Option<Arc<[VarId]>>, batch: Batch) -> Option<Batch> {
    match out_schema {
        Some(schema) => Some(batch.retain(Arc::clone(schema))),
        None => Some(batch),
    }
}
