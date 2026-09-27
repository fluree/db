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

/// Separates a ledger name from an instance id in an instance root. `@` is
/// reserved in ledger and branch names, so no name reaches an instance folder.
const INSTANCE_SEPARATOR: &str = "/@";

/// Identifies one incarnation of a ledger: created, imported, or restored from
/// an archive. Unique across the whole registry and never reused, so nothing
/// keyed by it can be confused with a later ledger of the same name.
///
/// A ULID: 26 Crockford base32 characters, sorting by creation time.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InstanceId(String);

impl InstanceId {
    pub fn parse(id: &str) -> Result<Self, LedgerIdParseError> {
        let crockford =
            |c: char| c.is_ascii_digit() || (c.is_ascii_uppercase() && !"ILOU".contains(c));
        if id.len() != 26 || !id.chars().all(crockford) {
            return Err(LedgerIdParseError::new(format!(
                "Invalid instance id '{id}': expected a 26-character ULID"
            )));
        }
        Ok(Self(id.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InstanceId {
    type Error = LedgerIdParseError;
    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::parse(&id)
    }
}

impl From<InstanceId> for String {
    fn from(id: InstanceId) -> Self {
        id.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InstanceId({})", self.0)
    }
}

/// The folder that holds every artifact of one ledger.
///
/// A ledger created before instance roots keeps its name as its root
/// (`mydb`); every later incarnation gets a folder of its own under the name,
/// `mydb/@{instance}`, so a reused name never shares a folder. Everything that
/// forms a path takes the root as an opaque prefix.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StorageRoot(String);

impl StorageRoot {
    /// The root of a ledger whose artifacts live under its name.
    pub fn legacy(name: &LedgerName) -> Self {
        Self(name.as_str().to_string())
    }

    /// The root of one incarnation: `{name}/@{instance}`.
    pub fn for_instance(name: &LedgerName, instance: &InstanceId) -> Self {
        Self(format!("{name}{INSTANCE_SEPARATOR}{instance}"))
    }

    /// Parse a root read from a record: `name` or `name/@instance`.
    pub fn parse(root: &str) -> Result<Self, LedgerIdParseError> {
        let invalid = |e: LedgerIdParseError| {
            LedgerIdParseError::new(format!("Invalid storage root '{root}': {e}"))
        };
        match root.rsplit_once(INSTANCE_SEPARATOR) {
            Some((name, instance)) => Ok(Self::for_instance(
                &LedgerName::parse(name).map_err(invalid)?,
                &InstanceId::parse(instance).map_err(invalid)?,
            )),
            None => Ok(Self::legacy(&LedgerName::parse(root).map_err(invalid)?)),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The incarnation this root belongs to; `None` for a name root.
    pub fn instance(&self) -> Option<InstanceId> {
        let (_, instance) = self.0.rsplit_once(INSTANCE_SEPARATOR)?;
        InstanceId::parse(instance).ok()
    }

    /// The ledger name this root sits under.
    pub fn name(&self) -> &str {
        self.0
            .rsplit_once(INSTANCE_SEPARATOR)
            .map_or(self.0.as_str(), |(name, _)| name)
    }

    /// The instance root holding a stored file: `mydb/@01JB…` for
    /// `fluree:file://mydb/@01JB…/main/commit/x`. `None` for a file under a
    /// name root (`mydb/main/…`, `mydb/@shared/…`) or outside any ledger.
    ///
    /// A name holds no `@`, so the first segment starting with one ends the
    /// name, and it is an instance folder exactly when it names an instance.
    pub fn instance_root_of(address: &str) -> Option<Self> {
        let path = address.split_once("://").map_or(address, |(_, path)| path);
        let mut name_len: usize = 0;
        for segment in path.split('/') {
            if let Some(instance) = segment.strip_prefix('@') {
                let name = LedgerName::parse(path.get(..name_len.checked_sub(1)?)?).ok()?;
                return Some(Self::for_instance(
                    &name,
                    &InstanceId::parse(instance).ok()?,
                ));
            }
            name_len += segment.len() + 1;
        }
        None
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
    fn instance_root_sits_under_the_name() {
        let name = LedgerName::parse("acme/inventory").unwrap();
        let instance = InstanceId::parse("01JB8ZK4X5Y6Z7A8B9C0D1E2F3").unwrap();
        let root = StorageRoot::for_instance(&name, &instance);
        assert_eq!(root.as_str(), "acme/inventory/@01JB8ZK4X5Y6Z7A8B9C0D1E2F3");
        assert_eq!(root.instance(), Some(instance));
        assert_eq!(StorageRoot::parse(root.as_str()).unwrap(), root);
        assert_eq!(StorageRoot::legacy(&name).instance(), None);

        let ns = root.namespace("main");
        assert_eq!(
            ns.branch_prefix(),
            "acme/inventory/@01JB8ZK4X5Y6Z7A8B9C0D1E2F3/main"
        );
        assert_eq!(
            ns.shared_prefix(),
            "acme/inventory/@01JB8ZK4X5Y6Z7A8B9C0D1E2F3/@shared"
        );
    }

    #[test]
    fn a_stored_file_names_its_instance_root() {
        fn root(address: &str) -> Option<String> {
            StorageRoot::instance_root_of(address).map(|r| r.to_string())
        }
        let id = "01JB8ZK4X5Y6Z7A8B9C0D1E2F3";
        assert_eq!(
            root(&format!("fluree:file://acme/inventory/@{id}/main/commit/x")),
            Some(format!("acme/inventory/@{id}"))
        );
        assert_eq!(
            root(&format!("fluree:s3://mydb/@{id}/@shared/dicts/d")),
            Some(format!("mydb/@{id}"))
        );
        assert_eq!(
            StorageRoot::parse(&format!("acme/inventory/@{id}"))
                .unwrap()
                .name(),
            "acme/inventory"
        );
        assert_eq!(StorageRoot::parse("acme").unwrap().name(), "acme");

        for outside in [
            "fluree:file://mydb/main/commit/x".to_string(),
            "fluree:file://mydb/@shared/dicts/d".to_string(),
            format!("fluree:file://ns@v3/@dropped/{id}.json"),
            "fluree:file://ns@v3/mydb/@binding.json".to_string(),
            format!("fluree:file://@{id}/main/commit/x"),
            format!("fluree:file://mydb/@{id}x/main/commit/x"),
        ] {
            assert_eq!(root(&outside), None, "{outside}");
        }
    }

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

        for bad in [
            "",
            "a:b",
            "a#b",
            "a/@x",
            "a/@",
            "/a",
            "a/",
            "a/@01JB8ZK4X5Y6Z7A8B9C0D1E2F/b",
            "a/@01jb8zk4x5y6z7a8b9c0d1e2f3",
        ] {
            assert!(
                serde_json::from_str::<StorageRoot>(&format!("\"{bad}\"")).is_err(),
                "{bad:?} must not parse as a storage root"
            );
        }
    }
}
