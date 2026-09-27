//! Moving a nameservice written before name bindings to the current format.
//!
//! The file and storage backends keep their records under an address
//! version. Binaries from before name bindings read and write only
//! `ns@v2/`; this format lives under `ns@v3/`, which they never see, so they
//! cannot rewrite a record and strip the fields they do not know. The
//! migration copies `ns@v2/` across once, binds its ledgers
//! ([`migrate_legacy`](crate::lifecycle::migrate_legacy)), and leaves a
//! marker. `ns@v2/` is left as it was, for a rollback.

use crate::{NameServiceError, Result};
use serde::{Deserialize, Serialize};

/// The nameservice format this binary reads and writes.
pub const FORMAT_VERSION: u32 = 3;

/// The address version of binaries from before name bindings.
pub(crate) const LEGACY_NS_VERSION: &str = "ns@v2";

/// The marker a finished migration leaves at the root of the current address
/// version. `@` keeps it out of every record listing.
pub(crate) const FORMAT_MARKER: &str = "@format.json";

/// The contents of [`FORMAT_MARKER`].
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FormatMarker {
    pub version: u32,
    /// Milliseconds since the Unix epoch.
    pub migrated_at: i64,
}

impl FormatMarker {
    pub fn current() -> Self {
        Self {
            version: FORMAT_VERSION,
            migrated_at: fluree_db_core::clock::SystemTime::now()
                .duration_since(fluree_db_core::clock::SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
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
