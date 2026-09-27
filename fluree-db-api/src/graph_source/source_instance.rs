//! Which incarnation of its source ledger an index graph source was built
//! from.
//!
//! A ledger dropped and created again under its name is another ledger. A
//! graph source records the instance it indexed, and while its source
//! ledger's name leads to a different instance it is suspended: it syncs
//! nothing from that ledger until it is recreated over it. Restoring the
//! dropped ledger brings back the instance, and with it the graph source.

use crate::{ApiError, Result};
use fluree_db_core::{InstanceId, StorageRoot};
use fluree_db_nameservice::lifecycle::legacy_instance;
use fluree_db_nameservice::GraphSourceRecord;
use serde_json::Value as JsonValue;

/// The config key recording the instance a graph source indexed.
pub(crate) const SOURCE_INSTANCE: &str = "source_instance";

/// The instance of the ledger stored under `root`.
pub(crate) fn instance_of(root: &StorageRoot) -> InstanceId {
    // A name root belongs to a ledger from before instance folders, bound
    // under its legacy instance when the nameservice was migrated.
    root.instance()
        .unwrap_or_else(|| legacy_instance(root.name()))
}

/// Refuse to sync `record` from the ledger stored under `root` unless that is
/// the ledger it indexed.
pub(crate) fn check_source_instance(
    record: &GraphSourceRecord,
    source: &str,
    root: &StorageRoot,
) -> Result<()> {
    let config: JsonValue = serde_json::from_str(&record.config)?;
    let indexed = match config.get(SOURCE_INSTANCE).and_then(JsonValue::as_str) {
        Some(id) => InstanceId::parse(id)?,
        // Built before graph sources recorded it, when every ledger was
        // rooted at its name.
        None => legacy_instance(root.name()),
    };
    let current = instance_of(root);
    if indexed == current {
        return Ok(());
    }
    Err(ApiError::GraphSourceSuspended(format!(
        "{} indexed ledger '{source}' instance {indexed}, which was dropped; '{source}' \
         is now instance {current}. Drop and recreate the graph source to index it",
        record.graph_source_id
    )))
}
