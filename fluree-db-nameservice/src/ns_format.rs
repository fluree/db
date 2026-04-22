//! Shared ns@v2 format types used by both file-based and storage-backed nameservice
//! implementations.
//!
//! These types represent the JSON structures stored in ns@v2 record files. Both
//! `FileNameService` and `StorageNameService` serialize/deserialize these types,
//! so they are defined once here to ensure consistency.

use crate::{
    is_zero, parse_default_context_value, ConfigPayload, ConfigValue, StatusPayload, StatusValue,
};
use fluree_db_core::ContentId;
use serde::{Deserialize, Serialize};

/// ns@v2 format version path segment.
pub(crate) const NS_VERSION: &str = "ns@v2";

/// Create the standard ns@v2 context as JSON value.
/// Uses object format with the `"f"` prefix mapping to the Fluree DB namespace.
pub(crate) fn ns_context() -> serde_json::Value {
    serde_json::json!({"f": fluree_vocab::fluree::DB})
}

/// JSON structure for main ns@v2 record file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NsFileV2 {
    /// Context can be either a string or an object with prefix mappings
    #[serde(rename = "@context")]
    pub context: serde_json::Value,

    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@type")]
    pub record_type: Vec<String>,

    #[serde(rename = "f:ledger")]
    pub ledger: LedgerRef,

    #[serde(rename = "f:branch")]
    pub branch: String,

    /// Content identifier for the head commit (CID string, e.g. "bafy...").
    /// This is the authoritative identity for the commit head pointer.
    #[serde(
        rename = "f:commitCid",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub commit_cid: Option<String>,

    #[serde(rename = "f:t")]
    pub t: i64,

    #[serde(rename = "f:ledgerIndex", skip_serializing_if = "Option::is_none")]
    pub index: Option<IndexRef>,

    #[serde(rename = "f:status")]
    pub status: String,

    /// Content identifier for the default JSON-LD context (new CID format).
    #[serde(
        rename = "f:defaultContextCid",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub default_context_cid: Option<String>,

    // V2 extension fields (optional for backward compatibility)
    /// Status watermark (v2 extension) - defaults to 1 if missing
    #[serde(rename = "f:statusV", skip_serializing_if = "Option::is_none")]
    pub status_v: Option<i64>,

    /// Status metadata beyond the state field (v2 extension)
    #[serde(rename = "f:statusMeta", skip_serializing_if = "Option::is_none")]
    pub status_meta: Option<std::collections::HashMap<String, serde_json::Value>>,

    /// Config watermark (v2 extension) - defaults to 0 (unborn) if missing
    #[serde(rename = "f:configV", skip_serializing_if = "Option::is_none")]
    pub config_v: Option<i64>,

    /// Config metadata beyond default_context (v2 extension)
    #[serde(rename = "f:configMeta", skip_serializing_if = "Option::is_none")]
    pub config_meta: Option<std::collections::HashMap<String, serde_json::Value>>,

    /// Content identifier for the ledger config object (origin discovery)
    #[serde(
        rename = "f:configCid",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub config_cid: Option<String>,

    /// Source branch name recording where this branch was created from
    #[serde(
        rename = "f:sourceBranch",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub source_branch: Option<String>,

    /// Branch point metadata recording where this branch was created from.
    /// Kept for backward-compatible deserialization of older ns@v2 files.
    #[serde(
        rename = "f:branchPoint",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub branch_point: Option<BranchPointRef>,

    /// Number of child branches created from this branch
    #[serde(rename = "f:branches", default, skip_serializing_if = "is_zero")]
    pub branches: u32,
}

impl NsFileV2 {
    /// Extract the current `StatusValue` from this record's status fields.
    /// Defaults `status_v` to 1 if missing (backward compatibility with v1 records).
    pub fn to_status_value(&self) -> StatusValue {
        let extra = self.status_meta.clone().unwrap_or_default();
        let payload = StatusPayload {
            state: self.status.clone(),
            extra,
        };
        let v = self.status_v.unwrap_or(1);
        StatusValue { v, payload }
    }

    /// Extract the current `ConfigValue` from this record's config fields.
    /// Infers `config_v` from field presence when missing: 1 if any config
    /// data exists (legacy record), 0 otherwise (unborn).
    pub fn to_config_value(&self) -> ConfigValue {
        let has_default_ctx = self.default_context_cid.is_some();
        let v = self.config_v.unwrap_or_else(|| {
            i64::from(has_default_ctx || self.config_meta.is_some() || self.config_cid.is_some())
        });

        let resolved_ctx = self
            .default_context_cid
            .as_deref()
            .and_then(parse_default_context_value);

        let payload = if v == 0
            && resolved_ctx.is_none()
            && self.config_meta.is_none()
            && self.config_cid.is_none()
        {
            None
        } else {
            let extra = self.config_meta.clone().unwrap_or_default();
            Some(ConfigPayload {
                default_context: resolved_ctx,
                config_id: self
                    .config_cid
                    .as_deref()
                    .and_then(|s| s.parse::<ContentId>().ok()),
                extra,
            })
        };

        ConfigValue { v, payload }
    }

    /// Overwrite commit head and index head from a snapshot.
    /// Used by `reset_head` implementations to roll back after a failed operation.
    pub(crate) fn apply_snapshot(&mut self, snapshot: &crate::NsRecordSnapshot) {
        self.commit_cid = snapshot
            .commit_head_id
            .as_ref()
            .map(std::string::ToString::to_string);
        self.t = snapshot.commit_t;
        self.index = snapshot.index_head_id.as_ref().map(|id| IndexRef {
            cid: Some(id.to_string()),
            t: snapshot.index_t,
        });
    }
}

/// JSON structure for index-only ns@v2 file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NsIndexFileV2 {
    /// Context can be either a string or an object with prefix mappings
    #[serde(rename = "@context")]
    pub context: serde_json::Value,

    #[serde(rename = "f:ledgerIndex")]
    pub index: IndexRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LedgerRef {
    #[serde(rename = "@id")]
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IndexRef {
    /// Content identifier for this index root (CID string).
    #[serde(rename = "f:cid", skip_serializing_if = "Option::is_none", default)]
    pub cid: Option<String>,

    #[serde(rename = "f:t")]
    pub t: i64,
}

/// JSON-LD representation of a branch point in an ns@v2 file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BranchPointRef {
    #[serde(rename = "f:source")]
    pub source: String,

    #[serde(rename = "f:commitCid", skip_serializing_if = "Option::is_none")]
    pub commit_cid: Option<String>,

    #[serde(rename = "f:t")]
    pub t: i64,
}
