//! Shared ns@v2 format types used by both file-based and storage-backed nameservice
//! implementations.
//!
//! These types represent the JSON structures stored in ns@v2 record files. Both
//! `FileNameService` and `StorageNameService` serialize/deserialize these types,
//! so they are defined once here to ensure consistency.

use crate::{
    is_zero, parse_default_context_value, ConfigPayload, ConfigValue, Fence, LedgerHeads, NsRecord,
    RefValue, StatusPayload, StatusValue,
};
use fluree_db_core::{ContentId, LedgerId};
use serde::{Deserialize, Serialize};

/// ns@v2 format version path segment.
pub(crate) const NS_VERSION: &str = "ns@v2";

/// `f:status` of a branch a drop has frozen: it accepts no more writes.
pub(crate) const STATUS_FROZEN: &str = "frozen";
/// `f:status` of a deleted branch record. The file stays as a tombstone, so a
/// late conditional write finds the fence gone rather than no file at all.
pub(crate) const STATUS_DELETED: &str = "deleted";
/// `f:status` of a soft-dropped branch under the old lifecycle.
pub(crate) const STATUS_RETRACTED: &str = "retracted";

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

    /// The fence a writer must present; see [`NsRecord::fence`].
    #[serde(rename = "f:fence", default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<Fence>,
}

impl NsFileV2 {
    /// A fresh file for `record`: its heads, lineage and fence.
    pub(crate) fn for_record(record: &NsRecord) -> Self {
        Self {
            context: ns_context(),
            id: record.ledger_id.to_string(),
            record_type: vec!["f:LedgerSource".to_string()],
            ledger: LedgerRef {
                id: record.name.clone(),
            },
            branch: record.branch.clone(),
            commit_cid: record.commit_head_id.as_ref().map(ToString::to_string),
            t: record.commit_t,
            index: record.index_head_id.as_ref().map(|id| IndexRef {
                cid: Some(id.to_string()),
                t: record.index_t,
            }),
            status: if record.frozen {
                STATUS_FROZEN.to_string()
            } else if record.retracted {
                STATUS_RETRACTED.to_string()
            } else {
                "ready".to_string()
            },
            default_context_cid: record.default_context.as_ref().map(ToString::to_string),
            status_v: Some(1),
            status_meta: None,
            config_v: Some(0),
            config_meta: None,
            config_cid: record.config_id.as_ref().map(ToString::to_string),
            source_branch: record.source_branch.clone(),
            branch_point: None,
            branches: record.branches,
            fence: record.fence,
        }
    }

    /// Whether this file is a deleted record's tombstone.
    pub(crate) fn is_deleted(&self) -> bool {
        self.status == STATUS_DELETED
    }

    /// Whether a write presenting `fence` may change this record, which must
    /// not be a tombstone. See [`crate::fence_admits`].
    pub(crate) fn admits(&self, fence: Option<crate::Fence>) -> bool {
        !self.is_deleted() && crate::fence_admits(self.fence, self.status == STATUS_FROZEN, fence)
    }

    /// The record this file and its index-only file describe, or `None` for
    /// a tombstone. The index file counts only when it carries this file's
    /// fence: one left by an earlier incarnation of the key is stale.
    pub(crate) fn into_record(
        self,
        index_file: Option<NsIndexFileV2>,
    ) -> crate::Result<Option<NsRecord>> {
        if self.is_deleted() {
            return Ok(None);
        }
        let parse = |s: Option<&str>| s.and_then(|s| s.parse::<ContentId>().ok());
        let mut record = NsRecord {
            ledger_id: LedgerId::from_parts(&self.ledger.id, &self.branch)?,
            name: self.ledger.id.clone(),
            branch: self.branch,
            commit_head_id: parse(self.commit_cid.as_deref()),
            config_id: parse(self.config_cid.as_deref()),
            commit_t: self.t,
            index_head_id: parse(self.index.as_ref().and_then(|i| i.cid.as_deref())),
            index_t: self.index.as_ref().map(|i| i.t).unwrap_or(0),
            default_context: self
                .default_context_cid
                .as_deref()
                .and_then(parse_default_context_value),
            retracted: self.status == STATUS_RETRACTED,
            source_branch: self
                .source_branch
                .or_else(|| self.branch_point.map(|bp| bp.source)),
            branches: self.branches,
            storage_root: None,
            fence: self.fence,
            frozen: self.status == STATUS_FROZEN,
        };
        // READ-TIME merge rule: the index file wins at equal or higher t.
        if let Some(index_data) = index_file.filter(|f| f.fence == record.fence) {
            if index_data.index.t >= record.index_t {
                record.index_head_id = parse(index_data.index.cid.as_deref());
                record.index_t = index_data.index.t;
            }
        }
        Ok(Some(record))
    }

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

/// Head pointers from a main ns@v2 file plus its optional index-only file,
/// using the read-time merge rule shared by `load_record`: the separate
/// index file wins when its `t` is equal or higher.
pub(crate) fn merge_heads(main: &NsFileV2, index_file: Option<&NsIndexFileV2>) -> LedgerHeads {
    let parse = |s: Option<&str>| s.and_then(|s| s.parse::<ContentId>().ok());
    let mut index = RefValue {
        id: parse(main.index.as_ref().and_then(|i| i.cid.as_deref())),
        t: main.index.as_ref().map(|i| i.t).unwrap_or(0),
    };
    if let Some(f) = index_file.filter(|f| f.fence == main.fence) {
        if f.index.t >= index.t {
            index = RefValue {
                id: parse(f.index.cid.as_deref()),
                t: f.index.t,
            };
        }
    }
    LedgerHeads {
        commit: RefValue {
            id: parse(main.commit_cid.as_deref()),
            t: main.t,
        },
        index,
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

    /// The fence of the record this index file belongs to. A file whose fence
    /// differs from its record's was left by an earlier incarnation.
    #[serde(rename = "f:fence", default, skip_serializing_if = "Option::is_none")]
    pub fence: Option<Fence>,

    /// Set by a drop, alongside the record's frozen status.
    #[serde(
        rename = "f:frozen",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub frozen: bool,
}

impl NsIndexFileV2 {
    /// Whether a write presenting `fence` may change this index file. A
    /// fenced record's index file carries its fence from before the record
    /// exists, so a fenced write needs no read of the main file.
    pub(crate) fn admits(&self, fence: Option<crate::Fence>) -> bool {
        crate::fence_admits(self.fence, self.frozen, fence)
    }
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

/// Whether `bytes` hold a ledger's main ns record (as opposed to an index or
/// snapshots sidecar, or a graph-source record): only main records carry
/// `f:ledger`.
pub(crate) fn is_ledger_main_record(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes).is_ok_and(|v| v.get("f:ledger").is_some())
}

/// Keys that may be sidecars rather than main records.
pub(crate) fn has_sidecar_suffix(key: &str) -> bool {
    key.ends_with(".index.json") || key.ends_with(".snapshots.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn cid(label: &str) -> ContentId {
        ContentId::new(ContentKind::IndexRoot, label.as_bytes())
    }

    fn main_file(inline_index: Option<IndexRef>) -> NsFileV2 {
        NsFileV2 {
            context: ns_context(),
            id: "mydb:main".to_string(),
            record_type: vec!["f:LedgerSource".to_string()],
            ledger: LedgerRef {
                id: "mydb".to_string(),
            },
            branch: "main".to_string(),
            commit_cid: Some(ContentId::new(ContentKind::Commit, b"c").to_string()),
            t: 9,
            index: inline_index,
            status: "ready".to_string(),
            default_context_cid: None,
            status_v: None,
            status_meta: None,
            config_v: None,
            config_meta: None,
            config_cid: None,
            source_branch: None,
            branch_point: None,
            branches: 0,
            fence: None,
        }
    }

    fn index_file(c: &ContentId, t: i64) -> NsIndexFileV2 {
        NsIndexFileV2 {
            context: ns_context(),
            index: IndexRef {
                cid: Some(c.to_string()),
                t,
            },
            fence: None,
            frozen: false,
        }
    }

    /// The separate index file wins at EQUAL `t`, not only when strictly
    /// higher — `load_record` uses `>=` and `merge_heads` claims to match it
    /// byte for byte. Flipping the comparison to `>` passes every other test
    /// in this crate, so this is the only thing pinning the boundary.
    #[test]
    fn merge_heads_index_file_wins_at_equal_t() {
        let inline = IndexRef {
            cid: Some(cid("inline").to_string()),
            t: 5,
        };
        let separate = index_file(&cid("separate"), 5);

        let heads = merge_heads(&main_file(Some(inline)), Some(&separate));
        assert_eq!(heads.index.t, 5);
        assert_eq!(
            heads.index.id,
            Some(cid("separate")),
            "at equal t the separate index file must win, matching load_record"
        );
    }

    #[test]
    fn merge_heads_keeps_the_higher_t_either_way() {
        let inline = IndexRef {
            cid: Some(cid("inline").to_string()),
            t: 7,
        };
        // Stale separate file: the inline index is ahead and must survive.
        let heads = merge_heads(
            &main_file(Some(inline.clone())),
            Some(&index_file(&cid("separate"), 3)),
        );
        assert_eq!((heads.index.id, heads.index.t), (Some(cid("inline")), 7));

        // Separate file ahead: it wins.
        let heads = merge_heads(
            &main_file(Some(inline)),
            Some(&index_file(&cid("separate"), 11)),
        );
        assert_eq!((heads.index.id, heads.index.t), (Some(cid("separate")), 11));

        // No separate file at all, and no inline index at all.
        let heads = merge_heads(&main_file(None), None);
        assert_eq!(heads.index, RefValue { id: None, t: 0 });
        assert_eq!(heads.commit.t, 9);
    }
}
