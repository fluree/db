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

/// One column of a SELECT/SELECT-ONE projection.
///
/// Selections are ordered: their position determines column order in tabular
/// output and JSON-LD array rendering. A single query may mix `Var` columns
/// (raw bindings) with `Hydrate` columns (the formatter materializes the
/// root variable into a nested JSON-LD object).
#[derive(Debug, Clone, PartialEq)]
pub enum Selection {
    /// Project a single variable's binding.
    Var(VarId),
    /// Hydrate a subject (variable or IRI constant) into a nested JSON-LD
    /// object. The root variable (when present) is the bound column the
    /// formatter materializes.
    Hydration(HydrationSpec),
}

impl Selection {
    /// Variable bound for this selection's row column, if any.
    ///
    /// `Var` returns its variable. `Hydrate` returns its root variable
    /// (or `None` when the root is an IRI constant — that case projects no
    /// bound row column; the formatter fetches the constant directly).
    pub fn bound_var(&self) -> Option<VarId> {
        match self {
            Selection::Var(v) => Some(*v),
            Selection::Hydration(spec) => spec.root_var(),
        }
    }

    /// Returns the `HydrationSpec` if this is a hydrate selection.
    pub fn as_hydration(&self) -> Option<&HydrationSpec> {
        match self {
            Selection::Hydration(spec) => Some(spec),
            Selection::Var(_) => None,
        }
    }
}
