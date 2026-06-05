//! Physical-plan introspection nodes for `EXPLAIN`.
//!
//! [`Operator::describe`](crate::operator::Operator::describe) walks the real
//! operator tree built by [`build_operator_tree`](crate::build_operator_tree)
//! and produces a [`PlanNode`] tree — the planned physical plan, rendered
//! without executing (no `open()`/`next_batch()`).
//!
//! Edges carry a [`PlanEdgeRel`] so the renderer distinguishes a real input
//! ([`Child`](PlanEdgeRel::Child)) from a correctness fallback an operator
//! keeps but only runs conditionally ([`Fallback`](PlanEdgeRel::Fallback)).
//! A fallback must never read as a co-executing child.

use crate::operator::Operator;
use serde::Serialize;
use serde_json::{Map, Value};

/// How a child node relates to its parent in the physical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlanEdgeRel {
    /// A real input the parent consumes during execution.
    Child,
    /// A correctness fallback the parent runs *instead* of its fast path when
    /// the fast path bails at `open()` (overlay/history/policy/multi-graph).
    /// Only one of the fast path or the fallback executes.
    Fallback,
    /// A path taken only under a runtime condition decided at `open()`.
    Conditional,
}

/// An edge to a child [`PlanNode`], tagged with its [`PlanEdgeRel`].
#[derive(Debug, Clone, Serialize)]
pub struct PlanEdge {
    pub rel: PlanEdgeRel,
    pub node: PlanNode,
}

fn map_is_empty(m: &Map<String, Value>) -> bool {
    m.is_empty()
}

/// One node in the planned physical plan.
#[derive(Debug, Clone, Serialize)]
pub struct PlanNode {
    /// Operator name (e.g. `"HashJoinOperator"`, `"DatasetOperator"`).
    pub op: String,
    /// Build-time cardinality estimate, when the operator exposes one.
    #[serde(rename = "est-rows", skip_serializing_if = "Option::is_none")]
    pub est_rows: Option<usize>,
    /// Operator-specific attributes (join var, predicate, index hint, …).
    #[serde(skip_serializing_if = "map_is_empty")]
    pub details: Map<String, Value>,
    /// Child edges (inputs, fallbacks, conditionals).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<PlanEdge>,
}

impl PlanNode {
    /// A leaf node with just a name and optional estimate.
    pub fn leaf(op: impl Into<String>, est_rows: Option<usize>) -> Self {
        Self {
            op: op.into(),
            est_rows,
            details: Map::new(),
            children: Vec::new(),
        }
    }
}

/// A child operator reference for [`Operator::plan_children`], tagged with the
/// edge kind. Borrows the child so [`Operator::describe`] can recurse without
/// cloning the tree.
pub struct PlanChild<'a> {
    pub rel: PlanEdgeRel,
    pub op: &'a dyn Operator,
}

impl<'a> PlanChild<'a> {
    /// A real input edge.
    pub fn child(op: &'a dyn Operator) -> Self {
        Self {
            rel: PlanEdgeRel::Child,
            op,
        }
    }

    /// A correctness-fallback edge (see [`PlanEdgeRel::Fallback`]).
    pub fn fallback(op: &'a dyn Operator) -> Self {
        Self {
            rel: PlanEdgeRel::Fallback,
            op,
        }
    }

    /// A conditional edge decided at `open()` (see [`PlanEdgeRel::Conditional`]).
    /// Reserved for operators whose `open()`-time branch is a distinct
    /// conditional node; no current `describe()` impl needs it yet.
    pub fn conditional(op: &'a dyn Operator) -> Self {
        Self {
            rel: PlanEdgeRel::Conditional,
            op,
        }
    }
}

/// Strip a fully-qualified type name to its bare type, e.g.
/// `"fluree_db_query::project::ProjectOperator"` → `"ProjectOperator"`.
pub(crate) fn short_type_name(full: &str) -> &str {
    let base = full.split('<').next().unwrap_or(full);
    base.rsplit("::").next().unwrap_or(base)
}
