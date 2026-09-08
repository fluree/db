//! Experimental validation for a narrow, unindexed linear commit chain.
//! Used by the limited `local_journal_ledger` adapter. Ordinary Fluree constructors
//! and HTTP transaction paths do not enable the experimental journal.
use fluree_db_core::local_journal::{AcceptanceValidator, AcceptanceView, Error, Result};
use fluree_db_core::{
    commit::codec::read_commit, content_path, ledger_id::split_ledger_id, ContentId, ContentKind,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// Validate exact v4 commit/raw bytes for one unindexed, unsigned linear ledger.
/// Policies, query execution, and application-state installation remain the staging
/// layer's responsibility. This is not remote-preparation validation across bases.
pub struct LinearCommitValidator;

// A deliberately closed subset of the ns@v2 shape. Deserialize typed fields first
// to reject duplicate/unknown fields before comparing the unmodified JSON values.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    #[serde(rename = "@context")]
    context: Value,
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "@type")]
    record_type: Vec<String>,
    #[serde(rename = "f:ledger")]
    ledger: Ledger,
    #[serde(rename = "f:branch")]
    branch: String,
    #[serde(rename = "f:commitCid")]
    commit: String,
    #[serde(rename = "f:t")]
    t: i64,
    #[serde(rename = "f:status")]
    status: String,
    #[serde(rename = "f:statusV")]
    status_v: Option<i64>,
    #[serde(rename = "f:configV")]
    config_v: Option<i64>,
    #[serde(rename = "f:statusMeta")]
    status_meta: Option<Value>,
    #[serde(rename = "f:configMeta")]
    config_meta: Option<Value>,
    #[serde(rename = "f:ledgerIndex")]
    index: Option<HeadIndex>,
    #[serde(rename = "f:defaultContextCid")]
    context_cid: Option<String>,
    #[serde(rename = "f:configCid")]
    config_cid: Option<String>,
    #[serde(rename = "f:sourceBranch")]
    source_branch: Option<String>,
    #[serde(rename = "f:branchPoint")]
    branch_point: Option<Value>,
    #[serde(rename = "f:branches", default)]
    branches: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    #[serde(rename = "@id")]
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadIndex {
    #[serde(rename = "f:cid")]
    cid: String,
    #[serde(rename = "f:t")]
    t: i64,
}

fn head(bytes: &[u8], ledger: &str, branch: &str, key: &str, indexed: bool) -> Result<Head> {
    let head: Head = serde_json::from_slice(bytes)
        .map_err(|_| Error::Invalid("unsupported journal head encoding"))?;
    if head.ledger.id != ledger
        || head.branch != branch
        || (head.id != key
            && head.id != format!("fluree:file://{key}")
            && head.id != format!("{ledger}:{branch}"))
        || head.record_type != ["f:LedgerSource"]
        || head.context != serde_json::json!({"f": fluree_vocab::fluree::DB})
        || head.status != "ready"
        || head.status_v.unwrap_or(1) != 1
        || head.config_v.unwrap_or(0) != i64::from(indexed && head.context_cid.is_some())
        || head.status_meta.is_some()
        || head.config_meta.is_some()
        || (!indexed && head.index.is_some())
        || (!indexed && head.context_cid.is_some())
        || head.config_cid.is_some()
        || head.source_branch.is_some()
        || head.branch_point.is_some()
        || head.branches != 0
    {
        return Err(Error::Invalid(
            "unsupported indexed/configured/branched journal head",
        ));
    }
    if let Some(index) = &head.index {
        let id: ContentId = index
            .cid
            .parse()
            .map_err(|_| Error::Invalid("invalid index CID"))?;
        if id.content_kind() != Some(ContentKind::IndexRoot) || index.t <= 0 || index.t > head.t {
            return Err(Error::Invalid("invalid fixed index time/kind"));
        }
    }
    Ok(head)
}

fn content<'a>(
    view: &'a AcceptanceView<'_>,
    id: &ContentId,
    kind: ContentKind,
    required: &mut BTreeSet<String>,
) -> Result<&'a [u8]> {
    if id.content_kind() != Some(kind) {
        return Err(Error::Invalid("required content kind mismatch"));
    }
    let key = content_path(kind, &view.transition.ledger, &id.digest_hex());
    let bytes = view.content(&key).ok_or(Error::Invalid(
        "required bytes absent from accepted journal/candidate",
    ))?;
    if !id.verify(bytes) {
        return Err(Error::Invalid("required content CID mismatch"));
    }
    required.insert(key);
    Ok(bytes)
}

impl AcceptanceValidator for LinearCommitValidator {
    fn validate(&self, view: &AcceptanceView<'_>) -> Result<()> {
        validate_linear(view, None)
    }
}

pub(crate) struct LinearBaseline {
    pub id: ContentId,
    pub t: i64,
}

/// Validate the same closed head shape, with an explicit fixed index exception.
pub(crate) fn indexed_head(bytes: &[u8], ledger_id: &str, key: &str) -> Result<()> {
    let (ledger, branch) =
        split_ledger_id(ledger_id).map_err(|_| Error::Invalid("invalid ledger"))?;
    if key != format!("ns@v2/{ledger}/{branch}.json") {
        return Err(Error::Invalid("noncanonical checkpoint head key"));
    }
    head(bytes, &ledger, &branch, key, true)?;
    Ok(())
}

pub(crate) fn validate_linear(
    view: &AcceptanceView<'_>,
    baseline: Option<&LinearBaseline>,
) -> Result<()> {
    validate_linear_from(view, baseline.is_some(), baseline)
}

/// A private embedding may stop at a separately validated accepted prefix while
/// retaining the original indexed/unindexed head restrictions.
pub(crate) fn validate_linear_from(
    view: &AcceptanceView<'_>,
    indexed: bool,
    baseline: Option<&LinearBaseline>,
) -> Result<()> {
    let t = view.transition;
    let (ledger, branch) =
        split_ledger_id(&t.ledger).map_err(|_| Error::Invalid("invalid journal ledger"))?;
    if t.head_key != format!("ns@v2/{ledger}/{branch}.json") {
        return Err(Error::Invalid("noncanonical journal head key"));
    }
    let new = head(&t.resulting_head, &ledger, &branch, &t.head_key, indexed)?;
    let new_id: ContentId = new
        .commit
        .parse()
        .map_err(|_| Error::Invalid("invalid head CID"))?;
    let old = t
        .expected_head
        .as_deref()
        .map(|bytes| head(bytes, &ledger, &branch, &t.head_key, indexed))
        .transpose()?;
    if let Some(old) = &old {
        if new.t
            != old
                .t
                .checked_add(1)
                .ok_or(Error::Invalid("transaction time overflow"))?
        {
            return Err(Error::Invalid(
                "journal commit must advance exactly one transaction",
            ));
        }
        let static_fields = |bytes: &[u8]| -> Result<Value> {
            let mut value: Value =
                serde_json::from_slice(bytes).map_err(|_| Error::Invalid("head JSON"))?;
            let map = value.as_object_mut().ok_or(Error::Invalid("head object"))?;
            map.remove("f:t");
            map.remove("f:commitCid");
            Ok(value)
        };
        if static_fields(t.expected_head.as_ref().unwrap())? != static_fields(&t.resulting_head)? {
            return Err(Error::Invalid(
                "journal commit changes lifecycle/configuration fields",
            ));
        }
    }
    let mut required = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut id = new_id;
    let mut expected_t = new.t;
    loop {
        if let Some(baseline) = baseline {
            if id == baseline.id {
                if expected_t != baseline.t || visited.is_empty() {
                    return Err(Error::Invalid("invalid checkpoint ancestry boundary"));
                }
                break;
            }
            if expected_t <= baseline.t {
                return Err(Error::Invalid(
                    "commit chain does not reach verified checkpoint",
                ));
            }
        }
        if !visited.insert(id.to_string()) {
            return Err(Error::Invalid("cyclic journal commit chain"));
        }
        let bytes = content(view, &id, ContentKind::Commit, &mut required)?;
        // The existing v4 CID verifies the full blob. Older commit formats
        // have different identity rules and are outside this initial scope.
        if bytes.get(4) != Some(&4) {
            return Err(Error::Invalid("journal validator requires v4 commit bytes"));
        }
        let commit =
            read_commit(bytes).map_err(|_| Error::Invalid("invalid journal commit bytes"))?;
        if commit.t != expected_t
            || commit.t < 0
            || commit.parents.len() > 1
            || commit.txn_signature.is_some()
            || !commit.commit_signatures.is_empty()
        {
            return Err(Error::Invalid("unsupported commit time/merge/signatures"));
        }
        if visited.len() == 1 {
            if let Some(old) = &old {
                let parent: ContentId = old
                    .commit
                    .parse()
                    .map_err(|_| Error::Invalid("invalid prior head CID"))?;
                if commit.parents != [parent] {
                    return Err(Error::Invalid("commit parent is not the accepted head"));
                }
            }
        }
        if let Some(raw) = commit.txn {
            content(view, &raw, ContentKind::Txn, &mut required)?;
        }
        match commit.parents.into_iter().next() {
            Some(parent) => {
                id = parent;
                expected_t = expected_t
                    .checked_sub(1)
                    .ok_or(Error::Invalid("invalid ancestor time"))?;
            }
            None if expected_t <= 1 && baseline.is_none() => break,
            None => return Err(Error::Invalid("missing genesis ancestry")),
        }
    }
    // No unrelated objects, config/index heads, CAS losers or side effects.
    if t.objects
        .iter()
        .any(|object| !required.contains(&object.key))
    {
        return Err(Error::Invalid(
            "object outside supported commit dependency closure",
        ));
    }
    Ok(())
}
