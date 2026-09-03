//! Resource bounds on an incoming GraphQL document.
//!
//! A derived schema has cyclic types by construction — `Person.knows` is a
//! `[Person]` the moment one subject points at another — so nesting depth is
//! chosen by the caller, not by the schema. Without a ceiling a short document
//! can ask for arbitrarily deep recursion, and a document within the body-size
//! limit can name thousands of aliased root fields that resolve concurrently.

/// Nesting depth beyond which a document is refused.
///
/// Fifteen levels is far past any hand-written query and well short of what
/// threatens the recursive walks (`selection::walk`, lowering, reshaping).
pub const DEFAULT_MAX_DEPTH: usize = 15;

/// Field budget for one document, as async-graphql counts complexity.
///
/// Each field costs 1 by default, so this is effectively a cap on how many
/// fields — across every alias and fragment — one request may select.
pub const DEFAULT_MAX_COMPLEXITY: usize = 1000;

/// What a GraphQL document is allowed to ask for.
///
/// Enforced in two places, because they cover different windows. `max_depth`
/// is checked during `selection::extract`, which runs *before* async-graphql
/// validates — a schema-level limit alone would let a deeply nested document
/// recurse through the extraction walk first. Both are then handed to the
/// schema builder so validation applies them again to execution itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Limits {
    pub max_depth: usize,
    pub max_complexity: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_complexity: DEFAULT_MAX_COMPLEXITY,
        }
    }
}

impl Limits {
    /// Limits that refuse nothing.
    ///
    /// For an embedder running its own trusted documents. A server should not
    /// use this: it is the ceiling every other read surface has.
    pub fn unlimited() -> Self {
        Self {
            max_depth: usize::MAX,
            max_complexity: usize::MAX,
        }
    }
}
