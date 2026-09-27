//! Where a ledger's artifacts live in storage.
//!
//! Every artifact of a ledger sits under one folder, its [`StorageRoot`]:
//! each branch's commits, transactions, index and config under
//! `{root}/{branch}/`, and the dictionaries its branches share under
//! `{root}/@shared/`. A content store is scoped to one branch of one root, a
//! [`StorageNamespace`].
//!
//! The root comes from the ledger's nameservice record, so code that can reach
//! the record must take the namespace from it rather than derive one from the
//! ledger id: [`StorageNamespace::legacy`] is correct only for a ledger whose
//! root is its name.
//!
//! We avoid putting `:` in storage paths for cross-platform portability
//! (Windows/macOS filesystem restrictions).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ledger_id::{LedgerId, LedgerIdParseError, LedgerName};

/// Namespace for content shared across all branches of a ledger.
///
/// Uses `@` prefix, which cannot collide with any real branch name since
/// `@` is forbidden by [`validate_branch_name`](crate::validate_branch_name).
pub const SHARED_NAMESPACE: &str = "@shared";

/// Parse an id that a storage seam received as a string.
///
/// Debug builds panic on a non-canonical id so tests catch the path that
/// skipped edge normalization; release builds apply the default branch.
pub(crate) fn storage_ledger_id(
    ledger_id: &str,
    seam: &str,
) -> Result<LedgerId, LedgerIdParseError> {
    debug_assert!(
        LedgerId::expect_canonical(ledger_id, seam).is_ok(),
        "{}",
        LedgerId::expect_canonical(ledger_id, seam).unwrap_err()
    );
    LedgerId::parse(ledger_id)
}

/// The folder that holds every artifact of one ledger.
///
/// A ledger's root is its name (`mydb`). Everything that forms a path takes
/// the root as an opaque prefix, so the root can later name a folder other
/// than the ledger's name without any path-forming code changing.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StorageRoot(String);

impl StorageRoot {
    /// The root of a ledger whose artifacts live under its name.
    pub fn legacy(name: &LedgerName) -> Self {
        Self(name.as_str().to_string())
    }

    /// Parse a root read from a record.
    pub fn parse(root: &str) -> Result<Self, LedgerIdParseError> {
        Ok(Self::legacy(&LedgerName::parse(root).map_err(|e| {
            LedgerIdParseError::new(format!("Invalid storage root '{root}': {e}"))
        })?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The namespace of `branch` under this root.
    pub fn namespace(&self, branch: &str) -> StorageNamespace {
        StorageNamespace::new(self.clone(), branch)
    }

    /// `{root}/@shared`: the dictionaries every branch of the ledger reads.
    pub fn shared_prefix(&self) -> String {
        format!("{}/{SHARED_NAMESPACE}", self.0)
    }
}

impl TryFrom<String> for StorageRoot {
    type Error = LedgerIdParseError;
    fn try_from(root: String) -> Result<Self, Self::Error> {
        Self::parse(&root)
    }
}

impl From<StorageRoot> for String {
    fn from(root: StorageRoot) -> Self {
        root.0
    }
}

impl fmt::Display for StorageRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for StorageRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageRoot({})", self.0)
    }
}

/// One branch's place in storage: `{root}/{branch}` for its own artifacts,
/// and `{root}/@shared` for the dictionaries its ledger shares across
/// branches.
///
/// Graph sources use the same shape, keyed by their own name and branch.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StorageNamespace {
    root: StorageRoot,
    branch_prefix: String,
    shared_prefix: String,
}

impl StorageNamespace {
    pub fn new(root: StorageRoot, branch: &str) -> Self {
        Self {
            branch_prefix: format!("{}/{branch}", root.as_str()),
            shared_prefix: root.shared_prefix(),
            root,
        }
    }

    /// The namespace of a ledger branch whose root is the ledger's name.
    ///
    /// Only for code that has no nameservice record to take the namespace
    /// from.
    pub fn legacy(ledger_id: &LedgerId) -> Self {
        Self::new(
            StorageRoot::legacy(&ledger_id.ledger_name()),
            ledger_id.branch(),
        )
    }

    /// [`legacy`](Self::legacy) for an id given as a string.
    pub fn parse_legacy(ledger_id: &str) -> Result<Self, LedgerIdParseError> {
        Ok(Self::legacy(&LedgerId::parse(ledger_id)?))
    }

    /// The namespace of a graph source: always keyed by its own id.
    pub fn graph_source(graph_source_id: &LedgerId) -> Self {
        Self::legacy(graph_source_id)
    }

    /// [`graph_source`](Self::graph_source) for an id given as a string.
    pub fn parse_graph_source(graph_source_id: &str) -> Result<Self, LedgerIdParseError> {
        Ok(Self::graph_source(&LedgerId::parse(graph_source_id)?))
    }

    pub fn root(&self) -> &StorageRoot {
        &self.root
    }

    /// `{root}/{branch}`.
    pub fn branch_prefix(&self) -> &str {
        &self.branch_prefix
    }

    /// `{root}/@shared`.
    pub fn shared_prefix(&self) -> &str {
        &self.shared_prefix
    }
}

impl fmt::Display for StorageNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.branch_prefix)
    }
}

impl fmt::Debug for StorageNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageNamespace({})", self.branch_prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_namespace_is_the_name_and_branch() {
        let ns = StorageNamespace::legacy(&LedgerId::parse("acme/inventory:main").unwrap());
        assert_eq!(ns.branch_prefix(), "acme/inventory/main");
        assert_eq!(ns.shared_prefix(), "acme/inventory/@shared");
        assert_eq!(ns.root().as_str(), "acme/inventory");
    }

    #[test]
    fn root_round_trips_through_serde_and_rejects_invalid() {
        let root = StorageRoot::parse("acme/inventory").unwrap();
        let json = serde_json::to_string(&root).unwrap();
        assert_eq!(json, "\"acme/inventory\"");
        assert_eq!(serde_json::from_str::<StorageRoot>(&json).unwrap(), root);

        for bad in ["", "a:b", "a#b", "a/@x", "/a", "a/"] {
            assert!(
                serde_json::from_str::<StorageRoot>(&format!("\"{bad}\"")).is_err(),
                "{bad:?} must not parse as a storage root"
            );
        }
    }
}
