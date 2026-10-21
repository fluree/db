//! A branch's retention registration: whether the retention worker runs for
//! it, from which commit its retention input is honored, and where its
//! persisted schedule is.
//!
//! Kept in the branch's nameservice config record (`extra.retention`), which
//! every nameservice backend persists, Raft replicates (`PushConfig`), and a
//! ledger backup does not roll back. Written with compare-and-set under the
//! branch fence, so a stale owner cannot overwrite a newer checkpoint.

use fluree_db_nameservice::{ConfigCasResult, ConfigPayload, ConfigValue, NameServicePublisher};
use serde::{Deserialize, Serialize};

use crate::{ApiError, Result};

/// Key of the registration in the config record's `extra` map.
pub const EXTRA_KEY: &str = "retention";

/// How many times a registration update retries a lost compare-and-set.
const CAS_ATTEMPTS: usize = 8;

/// A branch's retention registration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    /// The activation boundary: the first commit whose retention input
    /// (TTL instructions, transaction defaults, retention config) is honored.
    pub active_since_t: i64,
    /// Commits with `t <= watermark_t` are in the persisted schedule.
    #[serde(default)]
    pub watermark_t: i64,
    /// Content id of the persisted schedule blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// When the earliest scheduled occurrence is due (epoch ms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_due_ms: Option<i64>,
    /// Scheduled occurrences not yet expired.
    #[serde(default)]
    pub pending: u64,
    /// When the worker last finished a pass (epoch ms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pass_ms: Option<i64>,
    /// Occurrences expired since activation.
    #[serde(default)]
    pub expired_total: u64,
    /// Expiration batches that had to be recomputed (head moved, conflict).
    #[serde(default)]
    pub retries: u64,
    /// What blocks or degrades processing, for an operator.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub problems: Vec<String>,
    /// The removal journal: what the branch's history no longer serves.
    #[serde(default, skip_serializing_if = "super::journal::Journal::is_empty")]
    pub journal: super::journal::Journal,
    /// The journal generation whose history rules the index has been rebuilt
    /// for (the backfill that drops existing history from the index).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub history_backfilled: u64,
    /// The CID of the ledger's genesis commit, which every copy of it
    /// (restored, cloned, branched) shares; keys the journal checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<String>,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// The branch's registration, if any, with the config value it was read from.
pub(crate) async fn read<N: fluree_db_nameservice::ConfigLookup + ?Sized>(
    ns: &N,
    ledger_id: &str,
) -> Result<(Option<ConfigValue>, Option<Registration>)> {
    let config = ns.get_config(ledger_id).await?;
    let registration = config
        .as_ref()
        .and_then(|c| c.payload.as_ref())
        .and_then(|p| p.extra.get(EXTRA_KEY))
        .map(|v| serde_json::from_value::<Registration>(v.clone()))
        .transpose()
        .map_err(|e| ApiError::internal(format!("retention registration: {e}")))?;
    Ok((config, registration))
}

/// Update the branch's registration with `update`, which sees the current
/// one and returns the new one (`None` leaves the record unchanged).
/// Retries a lost compare-and-set against the record as it then is.
pub(crate) async fn update(
    ns: &dyn NameServicePublisher,
    ledger_id: &str,
    update: impl FnMut(Option<&Registration>) -> Option<Registration>,
) -> Result<Option<Registration>> {
    let Some(record) = ns.lookup(ledger_id).await? else {
        return Err(ApiError::NotFound(ledger_id.to_string()));
    };
    update_fenced(ns, ledger_id, record.fence, update).await
}

/// [`update`] under `fence`, for a record not yet visible to lookups (a
/// ledger being restored).
pub(crate) async fn update_fenced(
    ns: &dyn NameServicePublisher,
    ledger_id: &str,
    fence: Option<fluree_db_nameservice::Fence>,
    mut update: impl FnMut(Option<&Registration>) -> Option<Registration>,
) -> Result<Option<Registration>> {
    for _ in 0..CAS_ATTEMPTS {
        let (config, current) = read(ns, ledger_id).await?;
        let Some(next) = update(current.as_ref()) else {
            return Ok(current);
        };
        let value = serde_json::to_value(&next)
            .map_err(|e| ApiError::internal(format!("retention registration: {e}")))?;
        let (v, mut payload) = match &config {
            Some(c) => (c.v, c.payload.clone().unwrap_or_default()),
            None => (0, ConfigPayload::default()),
        };
        payload.extra.insert(EXTRA_KEY.to_string(), value);
        let new = ConfigValue::new(v + 1, Some(payload));
        match ns
            .push_config_fenced(ledger_id, fence, config.as_ref(), &new)
            .await?
        {
            ConfigCasResult::Updated => return Ok(Some(next)),
            ConfigCasResult::Conflict { .. } => continue,
        }
    }
    Err(ApiError::internal(format!(
        "retention registration for {ledger_id}: compare-and-set kept losing"
    )))
}

/// Register the branch as active from `since_t`, keeping an earlier boundary
/// and everything else an existing registration holds.
pub(crate) async fn activate(
    ns: &dyn NameServicePublisher,
    ledger_id: &str,
    since_t: i64,
) -> Result<()> {
    update(ns, ledger_id, |current| match current {
        Some(r) if r.active_since_t <= since_t => None,
        Some(r) => Some(Registration {
            active_since_t: since_t,
            // An earlier boundary means commits the schedule never saw.
            watermark_t: r.watermark_t.min(since_t - 1),
            ..r.clone()
        }),
        None => Some(Registration {
            active_since_t: since_t,
            watermark_t: since_t - 1,
            ..Default::default()
        }),
    })
    .await
    .map(|_| ())
}

/// What a ledger archive carries of the source's retention: its activation
/// boundary and removal journal, so a restored copy serves no history the
/// source no longer served.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArchivedRetention {
    pub(crate) active_since_t: i64,
    #[serde(default)]
    pub(crate) journal: super::journal::Journal,
}

impl ArchivedRetention {
    pub(crate) fn parse(archived: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(archived.clone())
            .map_err(|e| ApiError::http(400, format!(".flpack retention section: {e}")))
    }
}

/// The archive's retention section for `ledger_id`, if it is registered.
pub(crate) async fn archived<N: fluree_db_nameservice::ConfigLookup + ?Sized>(
    ns: &N,
    ledger_id: &str,
) -> Result<Option<serde_json::Value>> {
    let Some(registration) = read(ns, ledger_id).await?.1 else {
        return Ok(None);
    };
    let archived = ArchivedRetention {
        active_since_t: registration.active_since_t,
        journal: registration.journal,
    };
    serde_json::to_value(archived)
        .map(Some)
        .map_err(|e| ApiError::internal(format!("retention registration: {e}")))
}

/// Register a restored ledger: its archive's retention section (if any),
/// with the journal the lineage's checkpoint holds merged in, so a copy
/// restored from an archive made before a removal still refuses it. Its
/// expiry schedule is rebuilt from its commits, and its index history
/// compacted again. Nothing is registered when neither has anything.
pub(crate) async fn restore(
    ns: &dyn NameServicePublisher,
    record: &fluree_db_nameservice::NsRecord,
    archived: Option<ArchivedRetention>,
    lineage: Option<&str>,
    checkpoint: Option<super::journal::Journal>,
) -> Result<Option<Registration>> {
    let mut journal = archived
        .as_ref()
        .map(|a| a.journal.clone())
        .unwrap_or_default();
    if let Some(checkpoint) = &checkpoint {
        journal.merge(checkpoint);
    }
    if archived.is_none() && journal.is_empty() {
        return Ok(None);
    }
    let active_since_t = archived.as_ref().map_or(1, |a| a.active_since_t);
    let registration = Registration {
        active_since_t,
        watermark_t: active_since_t - 1,
        journal,
        lineage: lineage.map(str::to_string),
        ..Default::default()
    };
    update_fenced(ns, record.ledger_id.as_str(), record.fence, |_| {
        Some(registration.clone())
    })
    .await?;
    Ok(Some(registration))
}
