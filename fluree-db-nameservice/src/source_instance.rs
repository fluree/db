//! Which incarnation of a ledger an index graph source was built over.
//!
//! A ledger dropped and created again under its name is another ledger. A
//! BM25 or vector index records the instance it indexed, and while its
//! source ledger's name leads to a different instance it is suspended.

use crate::lifecycle::legacy_instance;
use crate::{GraphSourceRecord, NameServiceError, Result};
use fluree_db_core::{InstanceId, StorageRoot};
use serde_json::Value as JsonValue;

/// The config key recording the instance a graph source indexed.
pub const SOURCE_INSTANCE: &str = "source_instance";

/// The instance of the ledger stored under `root`.
pub fn instance_of(root: &StorageRoot) -> InstanceId {
    // A name root belongs to a ledger from before instance folders, bound
    // under its legacy instance when the nameservice was migrated.
    root.instance()
        .unwrap_or_else(|| legacy_instance(root.name()))
}

/// The instance `record` indexed and the one stored under `root`, when they
/// differ; `None` when `root` holds the ledger `record` indexed.
pub fn source_instance_mismatch(
    record: &GraphSourceRecord,
    root: &StorageRoot,
) -> Result<Option<(InstanceId, InstanceId)>> {
    let config: JsonValue = serde_json::from_str(&record.config)?;
    let indexed = match config.get(SOURCE_INSTANCE).and_then(JsonValue::as_str) {
        Some(id) => {
            InstanceId::parse(id).map_err(|e| NameServiceError::invalid_id(e.to_string()))?
        }
        // Built before graph sources recorded it, when every ledger was
        // rooted at its name.
        None => legacy_instance(root.name()),
    };
    let current = instance_of(root);
    Ok((indexed != current).then_some((indexed, current)))
}

/// Why `record` is suspended, for an error message.
pub fn suspension_message(
    record: &GraphSourceRecord,
    source: &str,
    indexed: &InstanceId,
    current: &InstanceId,
) -> String {
    format!(
        "{} indexed ledger '{source}' instance {indexed}, which was dropped; '{source}' \
         is now instance {current}. Drop and recreate the graph source to index it",
        record.graph_source_id
    )
}
