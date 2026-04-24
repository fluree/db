//! Commit detail builder for the [`Graph`] API.
//!
//! Fetches a single commit by CID, decodes the binary blob, resolves all
//! Sids to compact IRIs, and returns a [`CommitDetail`] with flat flake tuples.
//!
//! # Example
//!
//! ```ignore
//! let detail = fluree
//!     .graph("mydb:main")
//!     .commit(&commit_id)
//!     .execute()
//!     .await?;
//!
//! for flake in &detail.flakes {
//!     println!("{} {} {} [{}] {}",
//!         flake.s, flake.p, flake.o,
//!         flake.dt, if flake.op { "+" } else { "-" });
//! }
//! ```

use crate::dataset::QueryConnectionOptions;
use crate::format::iri::IriCompactor;
use crate::graph::Graph;
use crate::ledger_view::CommitRef;
use crate::{policy_builder, ApiError, Result};
use fluree_db_core::commit::codec::read_commit;
use fluree_db_core::{ContentId, ContentStore, FlakeValue, OverlayProvider, Tracker};
use fluree_db_novelty::Commit;
use fluree_db_query::QueryPolicyEnforcer;
use fluree_graph_json_ld::ParsedContext;
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Response types
// ============================================================================

/// A single flake with IRIs resolved to compact form.
///
/// Serializes as a JSON tuple: `[s, p, o, dt, op]` (plus optional meta fields).
#[derive(Clone, Debug)]
pub struct ResolvedFlake {
    /// Subject IRI (compact form, e.g. "ex:Person1")
    pub s: String,
    /// Predicate IRI (compact form, e.g. "schema:name")
    pub p: String,
    /// Object value as a displayable string.
    /// For refs: compact IRI. For literals: the lexical value.
    pub o: ResolvedValue,
    /// Datatype IRI (compact form, e.g. "xsd:string") or "@id" for refs
    pub dt: String,
    /// Operation: true = assert, false = retract
    pub op: bool,
    /// Language tag (if present)
    pub lang: Option<String>,
    /// List index (if present)
    pub i: Option<i32>,
    /// Named graph IRI (if not default graph)
    pub graph: Option<String>,
}

impl Serialize for ResolvedFlake {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        // Base: [s, p, o, dt, op]
        // With meta: [s, p, o, dt, op, {meta}]
        let has_meta = self.lang.is_some() || self.i.is_some() || self.graph.is_some();
        let len = if has_meta { 6 } else { 5 };
        let mut seq = serializer.serialize_seq(Some(len))?;
        seq.serialize_element(&self.s)?;
        seq.serialize_element(&self.p)?;
        seq.serialize_element(&self.o)?;
        seq.serialize_element(&self.dt)?;
        seq.serialize_element(&self.op)?;
        if has_meta {
            let meta = FlakeMeta {
                lang: self.lang.as_deref(),
                i: self.i,
                graph: self.graph.as_deref(),
            };
            seq.serialize_element(&meta)?;
        }
        seq.end()
    }
}

/// Object value that preserves type information for JSON serialization.
///
/// Refs serialize as strings (compact IRIs), numerics as numbers, etc.
#[derive(Clone, Debug)]
pub enum ResolvedValue {
    /// String or IRI value
    String(String),
    /// Boolean
    Boolean(bool),
    /// Integer
    Long(i64),
    /// Float
    Double(f64),
    /// Any other value rendered as its lexical form
    Lexical(String),
}

impl Serialize for ResolvedValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            ResolvedValue::String(s) => serializer.serialize_str(s),
            ResolvedValue::Boolean(b) => serializer.serialize_bool(*b),
            ResolvedValue::Long(n) => serializer.serialize_i64(*n),
            ResolvedValue::Double(d) => serializer.serialize_f64(*d),
            ResolvedValue::Lexical(s) => serializer.serialize_str(s),
        }
    }
}

#[derive(Serialize)]
struct FlakeMeta<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    lang: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    i: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph: Option<&'a str>,
}

/// Full decoded commit with resolved IRIs.
#[derive(Clone, Debug, Serialize)]
pub struct CommitDetail {
    /// Content identifier (CIDv1)
    pub id: String,
    /// Transaction time
    pub t: i64,
    /// ISO 8601 timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Blob size in bytes
    pub size: usize,
    /// Parent commit CIDs
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parents: Vec<String>,
    /// Transaction signer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer: Option<String>,
    /// Number of assertions
    pub asserts: usize,
    /// Number of retractions
    pub retracts: usize,
    /// Namespace prefix table (prefix → IRI)
    #[serde(rename = "@context")]
    pub context: HashMap<String, String>,
    /// Flakes in SPOT order, each as [s, p, o, dt, op] with resolved IRIs
    pub flakes: Vec<ResolvedFlake>,
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for fetching and decoding a single commit.
///
/// Created via [`Graph::commit()`] or [`Graph::commit_prefix()`].
pub struct CommitBuilder<'a, 'g> {
    graph: &'g Graph<'a>,
    commit_ref: CommitRef,
    user_context: Option<ParsedContext>,
    /// Authenticated identity IRI for policy filtering.
    identity: Option<String>,
    /// Default policy class for policy filtering.
    policy_class: Option<String>,
}

impl<'a, 'g> CommitBuilder<'a, 'g> {
    pub(crate) fn new(graph: &'g Graph<'a>, commit_id: ContentId) -> Self {
        Self {
            graph,
            commit_ref: CommitRef::Exact(commit_id),
            user_context: None,
            identity: None,
            policy_class: None,
        }
    }

    pub(crate) fn from_prefix(graph: &'g Graph<'a>, prefix: String) -> Self {
        Self {
            graph,
            commit_ref: CommitRef::Prefix(prefix),
            user_context: None,
            identity: None,
            policy_class: None,
        }
    }

    pub(crate) fn from_t(graph: &'g Graph<'a>, t: i64) -> Self {
        Self {
            graph,
            commit_ref: CommitRef::T(t),
            user_context: None,
            identity: None,
            policy_class: None,
        }
    }

    /// Supply a custom `@context` for IRI compaction.
    ///
    /// When provided, IRIs are compacted using this context's prefix mappings
    /// instead of (or in addition to) the auto-derived prefixes.
    pub fn context(mut self, ctx: ParsedContext) -> Self {
        self.user_context = Some(ctx);
        self
    }

    /// Set the authenticated identity for policy-based flake filtering.
    ///
    /// When set (along with an optional policy class), the returned flakes
    /// are filtered according to the ledger's policy rules — only flakes
    /// the identity is permitted to read are included.
    ///
    /// When neither identity nor policy class is set, all flakes are returned
    /// (equivalent to root/admin access).
    pub fn identity(mut self, identity: Option<&str>) -> Self {
        self.identity = identity.map(std::string::ToString::to_string);
        self
    }

    /// Set the default policy class for policy-based flake filtering.
    pub fn policy_class(mut self, policy_class: Option<&str>) -> Self {
        self.policy_class = policy_class.map(std::string::ToString::to_string);
        self
    }

    /// Execute: fetch the commit blob, decode it, resolve IRIs, return detail.
    pub async fn execute(self) -> Result<CommitDetail> {
        // 1. Load the ledger to get namespace_codes for IRI resolution
        let handle = self
            .graph
            .fluree
            .ledger_cached(&self.graph.ledger_id)
            .await?;
        let snapshot = handle.snapshot().await;
        let namespace_codes = snapshot.snapshot.namespaces();

        // 2. Resolve commit reference to a full CID
        let commit_id = snapshot.resolve_commit(self.commit_ref).await?;

        // 3. Build IRI compactor: user-supplied context > ledger default > none
        let default_ctx;
        let compactor = if let Some(ctx) = &self.user_context {
            IriCompactor::new(namespace_codes, ctx)
        } else if let Some(ctx_json) = &snapshot.default_context {
            default_ctx = ParsedContext::parse(None, ctx_json)
                .map_err(|e| ApiError::internal(format!("bad default @context: {e}")))?;
            IriCompactor::new(namespace_codes, &default_ctx)
        } else {
            IriCompactor::from_namespaces(namespace_codes)
        };

        // 4. Fetch commit blob from content-addressed storage
        let content_store = self.graph.fluree.content_store(&self.graph.ledger_id);
        let blob = content_store.get(&commit_id).await.map_err(|e| {
            if matches!(e, fluree_db_core::error::Error::NotFound(_)) {
                ApiError::NotFound(format!("Commit {commit_id} not found"))
            } else {
                ApiError::internal(format!(
                    "Failed to read commit {commit_id} from storage: {e}"
                ))
            }
        })?;
        let blob_size = blob.len();

        // 5. Decode the commit
        let mut commit = read_commit(&blob)
            .map_err(|e| ApiError::internal(format!("Failed to decode commit {commit_id}: {e}")))?;

        // 6. Apply policy filtering (if identity or policy_class is set).
        //    Calls policy_builder and enforcer directly to preserve ApiError
        //    variants (query vs internal) instead of losing them through
        //    BlockFetchError's stringified intermediary.
        if self.identity.is_some() || self.policy_class.is_some() {
            let opts = QueryConnectionOptions {
                identity: self.identity.clone(),
                policy_class: self.policy_class.as_deref().map(|c| vec![c.to_string()]),
                ..Default::default()
            };
            // Use the novelty overlay so policy rules in uncommitted
            // transactions are visible to the policy builder.
            let overlay: &dyn OverlayProvider = snapshot.novelty.as_ref();
            let policy_ctx = policy_builder::build_policy_context_from_opts(
                &snapshot.snapshot,
                overlay,
                Some(snapshot.novelty.as_ref()),
                commit.t,
                &opts,
                &[0],
            )
            .await?;

            if !policy_ctx.wrapper().is_root() {
                let enforcer = QueryPolicyEnforcer::new(Arc::new(policy_ctx));
                let tracker = Tracker::disabled();
                commit.flakes = enforcer
                    .filter_flakes_for_graph(
                        &snapshot.snapshot,
                        overlay,
                        commit.t,
                        &tracker,
                        commit.flakes,
                    )
                    .await?;
            }
        }

        // 7. Build the response
        build_commit_detail(&commit, &commit_id, blob_size, &compactor)
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Build a `CommitDetail` from a decoded `Commit` and an `IriCompactor`.
fn build_commit_detail(
    commit: &Commit,
    commit_id: &ContentId,
    blob_size: usize,
    compactor: &IriCompactor,
) -> Result<CommitDetail> {
    // Count asserts/retracts
    let asserts = commit.flakes.iter().filter(|f| f.op).count();
    let retracts = commit.flakes.len() - asserts;

    // Extract the effective prefix map from the same compactor used for flakes.
    // This ensures the @context and the compacted IRIs are always in sync.
    let context = compactor.effective_prefixes();

    // Resolve flakes
    let mut flakes = Vec::with_capacity(commit.flakes.len());
    for flake in &commit.flakes {
        let s = compactor
            .compact_sid_for_display(&flake.s)
            .map_err(|e| ApiError::internal(format!("Failed to resolve subject IRI: {e}")))?;
        let p = compactor
            .compact_sid_for_display(&flake.p)
            .map_err(|e| ApiError::internal(format!("Failed to resolve predicate IRI: {e}")))?;

        let (o, dt) = resolve_object_and_dt(compactor, &flake.o, &flake.dt)?;

        let lang = flake.m.as_ref().and_then(|m| m.lang.clone());
        let i = flake.m.as_ref().and_then(|m| m.i);
        let graph =
            match &flake.g {
                Some(g_sid) => Some(compactor.compact_sid_for_display(g_sid).map_err(|e| {
                    ApiError::internal(format!("Failed to resolve graph IRI: {e}"))
                })?),
                None => None,
            };

        flakes.push(ResolvedFlake {
            s,
            p,
            o,
            dt,
            op: flake.op,
            lang,
            i,
            graph,
        });
    }

    Ok(CommitDetail {
        id: commit_id.to_string(),
        t: commit.t,
        time: commit.time.clone(),
        size: blob_size,
        parents: commit.parents.iter().map(ToString::to_string).collect(),
        signer: commit.txn_signature.as_ref().map(|s| s.signer.clone()),
        asserts,
        retracts,
        context,
        flakes,
    })
}

/// Resolve a FlakeValue + datatype Sid into a displayable value and compact dt string.
fn resolve_object_and_dt(
    compactor: &IriCompactor,
    value: &FlakeValue,
    dt_sid: &fluree_db_core::Sid,
) -> Result<(ResolvedValue, String)> {
    match value {
        FlakeValue::Ref(ref_sid) => {
            let iri = compactor
                .compact_sid_for_display(ref_sid)
                .map_err(|e| ApiError::internal(format!("Failed to resolve ref IRI: {e}")))?;
            Ok((ResolvedValue::String(iri), "@id".to_string()))
        }
        FlakeValue::Boolean(b) => {
            let dt = compact_dt(compactor, dt_sid)?;
            Ok((ResolvedValue::Boolean(*b), dt))
        }
        FlakeValue::Long(n) => {
            let dt = compact_dt(compactor, dt_sid)?;
            Ok((ResolvedValue::Long(*n), dt))
        }
        FlakeValue::Double(d) => {
            let dt = compact_dt(compactor, dt_sid)?;
            Ok((ResolvedValue::Double(*d), dt))
        }
        FlakeValue::String(s) => {
            let dt = compact_dt(compactor, dt_sid)?;
            Ok((ResolvedValue::String(s.clone()), dt))
        }
        // All other types: render as lexical string
        other => {
            let dt = compact_dt(compactor, dt_sid)?;
            Ok((ResolvedValue::Lexical(format!("{other}")), dt))
        }
    }
}

/// Compact a datatype Sid to a display string.
fn compact_dt(compactor: &IriCompactor, dt_sid: &fluree_db_core::Sid) -> Result<String> {
    compactor
        .compact_sid_for_display(dt_sid)
        .map_err(|e| ApiError::internal(format!("Failed to resolve datatype IRI: {e}")))
}
