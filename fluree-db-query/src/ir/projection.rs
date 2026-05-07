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

/// Selection at one level of a hydration (resolved).
///
/// Wildcard-vs-explicit is encoded structurally: the Wildcard variant
/// includes all forward properties at this level (and `@id` implicitly),
/// optionally refined per-property; the Explicit variant lists only the
/// chosen forward items (with `@id` membership tracked by the presence of
/// `ForwardItem::Id`).
type ReverseMap = std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>;

#[derive(Debug, Clone, PartialEq)]
pub enum NestedSelectSpec {
    /// `*` at this level — include all forward properties (and `@id`).
    /// `refinements` overrides the wildcard default of "include but don't
    /// recurse" for specific properties.
    Wildcard {
        refinements: std::collections::HashMap<Sid, Box<NestedSelectSpec>>,
        reverse: ReverseMap,
    },
    /// Explicit list of forward items. `@id` is included iff `forward`
    /// contains `ForwardItem::Id`.
    Explicit {
        forward: Vec<ForwardItem>,
        reverse: ReverseMap,
    },
}

impl NestedSelectSpec {
    /// Returns `true` if this level is a wildcard.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, NestedSelectSpec::Wildcard { .. })
    }

    /// Reverse property selections at this level.
    pub fn reverse(&self) -> &ReverseMap {
        match self {
            NestedSelectSpec::Wildcard { reverse, .. }
            | NestedSelectSpec::Explicit { reverse, .. } => reverse,
        }
    }

    /// Returns `true` if the level produces no output (empty Explicit
    /// selection with no reverse).
    pub fn is_empty(&self) -> bool {
        match self {
            NestedSelectSpec::Wildcard { .. } => false,
            NestedSelectSpec::Explicit { forward, reverse } => {
                forward.is_empty() && reverse.is_empty()
            }
        }
    }

    /// Whether this level explicitly includes `@id`.
    /// Wildcard always does; Explicit does iff `forward` contains `Id`.
    pub fn includes_id(&self) -> bool {
        match self {
            NestedSelectSpec::Wildcard { .. } => true,
            NestedSelectSpec::Explicit { forward, .. } => {
                forward.iter().any(|item| matches!(item, ForwardItem::Id))
            }
        }
    }

    /// Resolve a forward predicate against this level.
    ///
    /// Returns:
    /// - `Some(None)`: predicate is selected with no nested expansion.
    /// - `Some(Some(&nested))`: predicate is selected with explicit nested expansion.
    /// - `None`: predicate is not selected (only possible for `Explicit`).
    pub fn select_predicate(&self, pred: &Sid) -> Option<Option<&NestedSelectSpec>> {
        match self {
            NestedSelectSpec::Wildcard { refinements, .. } => {
                Some(refinements.get(pred).map(|b| &**b))
            }
            NestedSelectSpec::Explicit { forward, .. } => {
                forward.iter().find_map(|item| match item {
                    ForwardItem::Property {
                        predicate,
                        sub_spec,
                    } if predicate == pred => Some(sub_spec.as_deref()),
                    _ => None,
                })
            }
        }
    }
}

/// One forward item in an explicit (non-wildcard) selection level.
#[derive(Debug, Clone, PartialEq)]
pub enum ForwardItem {
    /// Explicit `@id` selection.
    Id,
    /// A specific property, with optional nested expansion of its values.
    Property {
        predicate: Sid,
        sub_spec: Option<Box<NestedSelectSpec>>,
    },
}

/// Top-level hydration spec (resolved, with Sids).
///
/// Resolves an [`UnresolvedHydrationSpec`] into IR form: IRIs encoded as
/// `Sid`s, the root chosen, and the level of selection captured in
/// `level`.
#[derive(Debug, Clone, PartialEq)]
pub struct HydrationSpec {
    /// Root of the hydration — variable or IRI constant.
    pub root: Root,
    /// Selection at the top level of the hydration.
    pub level: NestedSelectSpec,
    /// Max depth for auto-expansion (0 = no auto-expand).
    pub depth: usize,
}

impl HydrationSpec {
    /// Create a hydration with the given root and top-level selection,
    /// `depth: 0`.
    pub fn new(root: Root, level: NestedSelectSpec) -> Self {
        Self {
            root,
            level,
            depth: 0,
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

    /// Returns `true` if any column in the projection is a hydration column.
    pub fn has_hydration(&self) -> bool {
        self.columns()
            .iter()
            .any(|c| matches!(c, Column::Hydration(_)))
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
