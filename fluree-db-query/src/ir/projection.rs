//! Selection specs that describe how query results are projected and
//! crawled into nested JSON-LD form. The canonical resolved-and-lowered
//! query type is [`crate::ir::Query`]; this module covers only
//! the projection-shape types that flow through it.

use crate::var_registry::VarId;
use fluree_db_core::Sid;

// ============================================================================
// Graph crawl select types (resolved)
// ============================================================================

/// Root of a graph crawl select (resolved)
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

/// Nested selection specification for sub-crawls (resolved)
///
/// This type captures the full selection state for nested property expansion,
/// including both forward and reverse properties.
#[derive(Debug, Clone, PartialEq)]
pub struct NestedSelectSpec {
    /// Forward property selections
    pub forward: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested expansion
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

/// Selection specification for graph crawl (resolved)
///
/// Defines what properties to include at each level of expansion.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectionSpec {
    /// Explicit @id selection (include @id even when wildcard is not specified)
    Id,
    /// Wildcard - select all properties at this level
    Wildcard,
    /// Property selection with optional nested expansion
    Property {
        /// Predicate Sid
        predicate: Sid,
        /// Optional nested selection spec for expanding this property's values
        /// Uses Box to avoid infinite type recursion
        sub_spec: Option<Box<NestedSelectSpec>>,
    },
}

/// Graph crawl selection specification (resolved, with Sids)
///
/// This is the resolved form of `UnresolvedGraphSelectSpec`, with all IRIs
/// encoded as Sids. Used during result formatting for nested JSON-LD output.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphSelectSpec {
    /// Root of the crawl - variable or IRI constant
    pub root: Root,
    /// Forward property selections
    pub selections: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested expansion
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Max depth for auto-expansion (0 = no auto-expand)
    pub depth: usize,
    /// Whether wildcard was specified (controls @id inclusion)
    pub has_wildcard: bool,
}

impl GraphSelectSpec {
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
