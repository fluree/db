//! Group identity for multi-group hosting.
//!
//! A process can host several independent Raft groups — separate logs,
//! separate elections, separate snapshots. Each is named by a
//! [`GroupId`], which is used both as a filesystem path segment
//! (`<storage_root>/<group_id>/`) and as an HTTP route segment
//! (`/raft/<group_id>/...`). Both uses make an unvalidated string a
//! hazard, so the constructor is the only way to make one.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Maximum length of a group id, in bytes.
///
/// Well under any filesystem's per-component limit, and short enough
/// that a group id never dominates a log line or route path.
pub const MAX_GROUP_ID_LEN: usize = 64;

/// Names that would collide with the filesystem layout the storage
/// backend writes directly under a storage root.
///
/// The nameservice group keeps its historical unprefixed root
/// (`<root>/log`, `<root>/vote`, ...) for backward compatibility, so a
/// co-hosted group whose id matched one of those entries would write
/// its subtree straight into the nameservice's. Rejecting the names
/// outright is cheaper than teaching every backend to detect it.
const RESERVED_GROUP_IDS: &[&str] = &["log", "snapshots", "vote", "committed", "last_purged"];

/// Why a candidate group id was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GroupIdError {
    #[error("group id must not be empty")]
    Empty,
    #[error("group id is {len} bytes; maximum is {MAX_GROUP_ID_LEN}")]
    TooLong { len: usize },
    #[error(
        "group id {id:?} contains {ch:?}; only lowercase ASCII letters, digits, '-' and '_' are allowed"
    )]
    InvalidChar { id: String, ch: char },
    #[error("group id {id:?} must start with a lowercase ASCII letter or digit")]
    InvalidStart { id: String },
    #[error("group id {id:?} is reserved by the storage layout")]
    Reserved { id: String },
}

/// Validated identifier for one Raft group within a process.
///
/// Guaranteed to be a safe single path component and URL path segment:
/// non-empty, at most [`MAX_GROUP_ID_LEN`] bytes, drawn from
/// `[a-z0-9_-]`, starting with `[a-z0-9]`, and not one of the names the
/// storage layout reserves. The charset excludes `.` and `/`, so
/// traversal (`..`, `a/b`), absolute paths, Windows drive prefixes, and
/// URL-significant characters are all structurally impossible rather
/// than filtered case by case.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GroupId(String);

impl GroupId {
    /// Validate `id` and wrap it.
    pub fn new(id: impl Into<String>) -> Result<Self, GroupIdError> {
        let id = id.into();

        if id.is_empty() {
            return Err(GroupIdError::Empty);
        }
        if id.len() > MAX_GROUP_ID_LEN {
            return Err(GroupIdError::TooLong { len: id.len() });
        }
        if let Some(ch) = id
            .chars()
            .find(|ch| !matches!(ch, 'a'..='z' | '0'..='9' | '-' | '_'))
        {
            return Err(GroupIdError::InvalidChar { id, ch });
        }
        // A leading `-` reads as a flag to CLI tooling and a leading
        // `_` reads as private; require an alphanumeric first byte.
        if !id.starts_with(|ch: char| ch.is_ascii_lowercase() || ch.is_ascii_digit()) {
            return Err(GroupIdError::InvalidStart { id });
        }
        if RESERVED_GROUP_IDS.contains(&id.as_str()) {
            return Err(GroupIdError::Reserved { id });
        }

        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// This group's storage root under a shared `root`.
    ///
    /// The single place the `<root>/<group_id>/` convention is spelled
    /// out; the reserved-name list above exists to keep this from
    /// aliasing a legacy unprefixed root.
    pub fn storage_root(&self, root: impl AsRef<Path>) -> PathBuf {
        root.as_ref().join(&self.0)
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for GroupId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for GroupId {
    type Error = GroupIdError;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl TryFrom<&str> for GroupId {
    type Error = GroupIdError;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        Self::new(id)
    }
}

impl From<GroupId> for String {
    fn from(id: GroupId) -> String {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_intended_group_names() {
        for id in ["ns", "flow", "resolve", "group-1", "a", "0", "a_b-c9"] {
            assert!(GroupId::new(id).is_ok(), "{id} should be accepted");
        }
    }

    #[test]
    fn rejects_empty_and_oversize() {
        assert_eq!(GroupId::new(""), Err(GroupIdError::Empty));

        let long = "a".repeat(MAX_GROUP_ID_LEN + 1);
        assert_eq!(
            GroupId::new(&long),
            Err(GroupIdError::TooLong {
                len: MAX_GROUP_ID_LEN + 1
            })
        );
        // The boundary itself is fine.
        assert!(GroupId::new("a".repeat(MAX_GROUP_ID_LEN)).is_ok());
    }

    #[test]
    fn rejects_path_traversal_and_separators() {
        // The charset makes each of these unrepresentable rather than
        // specially detected; assert the outcome anyway, because these
        // are the cases that matter.
        for id in [
            "..", ".", "a/b", "../etc", "a\\b", "/abs", "c:", "a.b", "a b", "a\0b", "a\nb",
            "grouP", "GROUP", "café",
        ] {
            assert!(
                GroupId::new(id).is_err(),
                "{id:?} must be rejected as a path component",
            );
        }
    }

    #[test]
    fn rejects_leading_punctuation() {
        assert!(matches!(
            GroupId::new("-flow"),
            Err(GroupIdError::InvalidStart { .. })
        ));
        assert!(matches!(
            GroupId::new("_flow"),
            Err(GroupIdError::InvalidStart { .. })
        ));
    }

    #[test]
    fn rejects_storage_layout_names() {
        for id in RESERVED_GROUP_IDS {
            assert!(
                matches!(GroupId::new(*id), Err(GroupIdError::Reserved { .. })),
                "{id} must be reserved",
            );
        }
    }

    #[test]
    fn storage_root_is_a_single_child_component() {
        let id = GroupId::new("flow").unwrap();
        let root = id.storage_root("/var/lib/fluree/raft");
        assert_eq!(root, PathBuf::from("/var/lib/fluree/raft/flow"));
        assert_eq!(
            root.parent(),
            Some(Path::new("/var/lib/fluree/raft")),
            "a group root must stay directly under the shared root",
        );
    }

    #[test]
    fn deserialization_validates() {
        let ok: GroupId = serde_json::from_str("\"flow\"").unwrap();
        assert_eq!(ok.as_str(), "flow");

        assert!(
            serde_json::from_str::<GroupId>("\"../etc\"").is_err(),
            "serde must not be a way around validation",
        );
    }

    #[test]
    fn round_trips_through_serde() {
        let id = GroupId::new("resolve").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"resolve\"");
        assert_eq!(serde_json::from_str::<GroupId>(&json).unwrap(), id);
    }
}
