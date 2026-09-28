//! Which incarnation of a ledger a graph source was built over.
//!
//! A ledger dropped and created again under its name is another ledger. An
//! index graph source records the instance it indexed, and while its source
//! ledger's name leads to a different instance it is suspended: it syncs
//! nothing from that ledger until it is recreated over it. A virtual source
//! records the instance of its model ledger, and is suspended the same way
//! rather than governed by another ledger's policies. Restoring the dropped
//! ledger brings back the instance, and with it the graph source.

use crate::{ApiError, Result};
use fluree_db_core::{InstanceId, StorageRoot};
use fluree_db_nameservice::lifecycle::legacy_instance;
use fluree_db_nameservice::{GraphSourceRecord, NsRecord};
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

/// Whether the BM25 or vector index `record` is suspended, given `source`,
/// the record of the ledger its first dependency names now. For listings: a
/// record whose config cannot be read counts as not suspended, and a sync of
/// it reports the error.
pub fn index_is_suspended(record: &GraphSourceRecord, source: &NsRecord) -> bool {
    (record.is_bm25() || record.is_vector())
        && matches!(
            check_source_instance(record, source.ledger_id.as_ref(), &source.storage_root()),
            Err(ApiError::GraphSourceSuspended(_))
        )
}

/// Whether `record` is suspended: the ledger stored under `root`, which
/// `source` names now, is not the ledger it indexed.
pub(crate) fn is_suspended(
    record: &GraphSourceRecord,
    source: &str,
    root: &StorageRoot,
) -> Result<bool> {
    match check_source_instance(record, source, root) {
        Ok(()) => Ok(false),
        Err(ApiError::GraphSourceSuspended(_)) => Ok(true),
        Err(e) => Err(e),
    }
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

/// Refuse to govern `record` by `current`, the ledger its `model` names now,
/// unless that is the ledger it was registered with, `recorded`.
#[cfg(feature = "iceberg")]
pub(crate) fn check_model_instance(
    record: &GraphSourceRecord,
    model: &str,
    recorded: Option<&str>,
    current: &InstanceId,
) -> Result<()> {
    let registered = match recorded {
        Some(id) => InstanceId::parse(id)?,
        // Registered before sources recorded it, when every ledger was
        // rooted at its name.
        None => legacy_instance(&fluree_db_core::LedgerId::parse(model)?.ledger_name()),
    };
    if registered == *current {
        return Ok(());
    }
    Err(ApiError::GraphSourceSuspended(format!(
        "{} is governed by model ledger '{model}' instance {registered}, which was dropped; \
         '{model}' is now instance {current}. Drop and recreate the graph source to govern it \
         by the new ledger",
        record.graph_source_id
    )))
}
