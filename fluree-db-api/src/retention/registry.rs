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
}

/// The branch's registration, if any, with the config value it was read from.
pub(crate) async fn read(
    ns: &dyn NameServicePublisher,
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
    mut update: impl FnMut(Option<&Registration>) -> Option<Registration>,
) -> Result<Option<Registration>> {
    for _ in 0..CAS_ATTEMPTS {
        let Some(record) = ns.lookup(ledger_id).await? else {
            return Err(ApiError::NotFound(ledger_id.to_string()));
        };
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
            .push_config_fenced(ledger_id, record.fence, config.as_ref(), &new)
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
