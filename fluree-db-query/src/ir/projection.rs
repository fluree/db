//! Selection specs that describe how query results are projected and
//! expanded into nested JSON-LD form. The canonical resolved-and-lowered
//! query type is [`crate::ir::Query`]; this module covers only
//! the projection-shape types that flow through it.

use crate::var_registry::VarId;
use fluree_db_core::Sid;

// ============================================================================
// Hydration types (resolved)
// ============================================================================

/// Root of a hydration (resolved)
///
/// Supports both variable and IRI constant roots:
/// - Variable root: from query results (e.g., `?person`)
/// - IRI constant root: direct subject fetch (e.g., `ex:alice`)
#[derive(Debug, Clone, PartialEq)]
pub enum Root {
    /// Variable root - value comes from query results
    Var(VarId),
    /// IRI constant root - direct subject lookup
    Sid(Sid),
}

/// Nested selection specification for sub-hydrations (resolved)
///
/// This type captures the full selection state for nested property expansion,
/// including both forward and reverse properties.
#[derive(Debug, Clone, PartialEq)]
pub struct NestedSelectSpec {
    /// Forward property selections
    pub forward: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested hydration
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Whether wildcard was specified at this level
    pub has_wildcard: bool,
}

impl NestedSelectSpec {
    /// Create a new nested select spec
    pub fn new(
        forward: Vec<SelectionSpec>,
        reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
        has_wildcard: bool,
    ) -> Self {
        Self {
            forward,
            reverse,
            has_wildcard,
        }
    }

    /// Check if this spec is empty (no selections)
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty() && self.reverse.is_empty()
    }
}

/// Selection specification for expansion (resolved)
///
/// Defines what properties to include at each level of expansion.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectionSpec {
    /// Explicit @id selection (include @id even when wildcard is not specified)
    Id,
    /// Wildcard - select all properties at this level
    Wildcard,
    /// Property selection with optional nested hydration
    Property {
        /// Predicate Sid
        predicate: Sid,
        /// Optional nested selection spec for expanding this property's values
        /// Uses Box to avoid infinite type recursion
        sub_spec: Option<Box<NestedSelectSpec>>,
    },
}

/// Hydrationion specification (resolved, with Sids)
///
/// This is the resolved form of `UnresolvedHydrationSpec`, with all IRIs
/// encoded as Sids. Used during result formatting for nested JSON-LD output.
#[derive(Debug, Clone, PartialEq)]
pub struct HydrationSpec {
    /// Root of the hydration - variable or IRI constant
    pub root: Root,
    /// Forward property selections
    pub selections: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested hydration
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Max depth for auto-expansion (0 = no auto-expand)
    pub depth: usize,
    /// Whether wildcard was specified (controls @id inclusion)
    pub has_wildcard: bool,
}

impl HydrationSpec {
    /// Create a new graph select spec
    pub fn new(root: Root, selections: Vec<SelectionSpec>) -> Self {
        let has_wildcard = selections
            .iter()
            .any(|s| matches!(s, SelectionSpec::Wildcard));
        Self {
            root,
            selections,
            reverse: std::collections::HashMap::new(),
            depth: 0,
            has_wildcard,
        }
    }

    /// Returns the root variable, if the root is a variable (not an IRI constant).
    pub fn root_var(&self) -> Option<VarId> {
        match &self.root {
            Root::Var(v) => Some(*v),
            Root::Sid(_) => None,
        }
    }
}

/// One column of a SELECT projection.
///
/// Columns are ordered: their position determines column order in tabular
/// output and JSON-LD array rendering. A single query may mix `Var` columns
/// (raw bindings) with `Hydration` columns (the formatter materializes the
/// root variable into a nested JSON-LD object).
#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    /// Project a single variable's binding.
    Var(VarId),
    /// Materialize a subject (variable or IRI constant) into a nested
    /// JSON-LD object. When the spec's root is a variable, that variable
    /// is the projected source; when it's a Sid, no variable is projected
    /// for this column — the formatter fetches the constant directly.
    Hydration(HydrationSpec),
}

impl Column {
    /// Variable bound for this column's row position, if any.
    ///
    /// `Var` returns its variable. `Hydration` returns its root variable
    /// (or `None` when the root is an IRI constant — that case projects
    /// no bound row column; the formatter fetches the constant directly).
    pub fn bound_var(&self) -> Option<VarId> {
        match self {
            Column::Var(v) => Some(*v),
            Column::Hydration(spec) => spec.root_var(),
        }
    }

    /// Returns the `HydrationSpec` if this is a hydration column.
    pub fn as_hydration(&self) -> Option<&HydrationSpec> {
        match self {
            Column::Hydration(spec) => Some(spec),
            Column::Var(_) => None,
        }
    }
}

/// The columns a SELECT query produces.
///
/// Carries column order for rendering; the SPARQL projection (the bound-var
/// set) is recoverable via [`Projection::bound_vars`].
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// SELECT * — all in-scope WHERE-bound variables, rendered raw.
    Wildcard,
    /// Array-form rows: each row is `[v1, v2, ...]` of any arity.
    Tuple(Vec<Column>),
    /// Bare-value rows from JSON-LD `select: "?x"` — exactly one column,
    /// each row is just the value (not wrapped in an array).
    Scalar(Column),
}

impl Projection {
    /// Columns in render order. Empty for `Wildcard`.
    pub fn columns(&self) -> &[Column] {
        match self {
            Projection::Wildcard => &[],
            Projection::Tuple(cs) => cs,
            Projection::Scalar(c) => std::slice::from_ref(c),
        }
    }

    /// Iterator over the bound variables of each column in render order.
    pub fn var_iter(&self) -> impl Iterator<Item = VarId> + '_ {
        self.columns().iter().filter_map(Column::bound_var)
    }

    /// SPARQL projection: variables this projection contributes to the
    /// row schema. `None` for `Wildcard` (means "all bound WHERE vars").
    pub fn bound_vars(&self) -> Option<Vec<VarId>> {
        match self {
            Projection::Wildcard => None,
            other => Some(other.var_iter().collect()),
        }
    }

    /// The hydration spec embedded in the projection (at most one;
    /// enforced by the parser).
    pub fn hydration(&self) -> Option<&HydrationSpec> {
        self.columns().iter().find_map(Column::as_hydration)
    }

    /// Returns `true` iff rows should be flattened from `[v]` to `v` at
    /// format time. Only fires for the bare-string `select: "?x"` form.
    pub fn is_scalar_var(&self) -> bool {
        matches!(self, Projection::Scalar(Column::Var(_)))
    }

    /// Returns `true` for `Wildcard`.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, Projection::Wildcard)
    }
}
