//! Moving a nameservice written before name bindings to the current format.
//!
//! The file and storage backends keep their records under an address
//! version. Binaries from before name bindings read and write only
//! `ns@v2/`; this format lives under `ns@v3/`, which they never see, so they
//! cannot rewrite a record and strip the fields they do not know. The
//! migration copies `ns@v2/` across once, binds its ledgers
//! ([`migrate_legacy`](crate::lifecycle::migrate_legacy)), retires `ns@v2/`,
//! and leaves a marker.
//!
//! Retiring keeps each `ns@v2/` file under [`LEGACY_BACKUP`], for a
//! rollback, and replaces it with [`RETIRED_FILE`]. That is not JSON, so a
//! binary from before name bindings still running against the store fails
//! on every read and write of an existing ledger, rather than committing to
//! `ns@v2/` where this format never sees it. Such a binary can still create
//! a ledger there, under a name `ns@v2/` did not hold; startup warns about
//! that.

use crate::{NameServiceError, Result};
use serde::{Deserialize, Serialize};

/// The nameservice format this binary reads and writes.
pub const FORMAT_VERSION: u32 = 3;

/// The address version of binaries from before name bindings.
pub(crate) const LEGACY_NS_VERSION: &str = "ns@v2";

/// Where the migration keeps the `ns@v2/` files it retires.
pub(crate) const LEGACY_BACKUP: &str = "ns@v2.bak";

/// What each `ns@v2/` file holds once retired.
pub(crate) const RETIRED_FILE: &[u8] =
    b"This nameservice was upgraded to name bindings and now lives \
under ns@v3/. Fluree 4.2 and earlier cannot use it. The file that was here is under ns@v2.bak/.\n";

/// The marker a finished migration leaves at the root of the current address
/// version. `@` keeps it out of every record listing.
pub(crate) const FORMAT_MARKER: &str = "@format.json";

/// The contents of [`FORMAT_MARKER`].
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FormatMarker {
    pub version: u32,
    /// Milliseconds since the Unix epoch.
    pub migrated_at: i64,
    /// The files under [`LEGACY_NS_VERSION`] when the migration copied them,
    /// on a backend that cannot tell when a file last changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy: Option<LegacyFiles>,
}

/// Which files an address version held: enough to tell that one was added
/// or removed since, though not that one was rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LegacyFiles {
    pub count: usize,
    /// SHA-256 of the sorted relative paths, one per line.
    pub digest: String,
}

impl LegacyFiles {
    pub fn of(relative_paths: &[String]) -> Self {
        let mut sorted: Vec<&str> = relative_paths.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        Self {
            count: sorted.len(),
            digest: fluree_db_core::sha256_hex(sorted.join("\n").as_bytes()),
        }
    }
}

impl FormatMarker {
    pub fn current(legacy: Option<LegacyFiles>) -> Self {
        Self {
            version: FORMAT_VERSION,
            migrated_at: fluree_db_core::clock::SystemTime::now()
                .duration_since(fluree_db_core::clock::SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
            legacy,
        }
    }

    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let marker: Self = serde_json::from_slice(bytes)?;
        check_format_version(marker.version)?;
        Ok(marker)
    }
}

/// Refuse a store a newer binary has moved to a format this one does not
/// understand.
pub fn check_format_version(version: u32) -> Result<()> {
    if version > FORMAT_VERSION {
        return Err(NameServiceError::storage(format!(
            "the nameservice is in format {version}, newer than this binary understands \
             ({FORMAT_VERSION}); upgrade it"
        )));
    }
    Ok(())
}
