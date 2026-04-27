//! `MergePlan` — declarative input for the four-step merge flow
//! (preview → query → validate → commit).
//!
//! See `docs/design/merge-custom.md` for the full design. This module only defines the
//! wire-shaped data types and validation helpers; the merge engine that
//! consumes them lives in [`crate::merge`].
//!
//! Types here are wire-shaped: branch selectors carry user-friendly strings,
//! conflict resolution keys are IRIs, and patches are opaque JSON-LD
//! transaction bodies. The merge engine compiles these into SID-keyed
//! [`fluree_db_core::ConflictKey`] and [`fluree_db_core::Flake`] values when
//! it executes a plan.

use crate::error::{ApiError, Result};
use fluree_db_core::CommitId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

/// Top-level merge plan.
///
/// The plan operates on the ledger named in the request path; there is
/// intentionally no `ledger` field in this struct. Wire-format requests with a
/// top-level `ledger` field are rejected by the route handler with `400`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MergePlan {
    /// Which source branch to merge from, plus its expected commit head.
    pub source: BranchSelector,
    /// Which target branch to merge into, plus its expected commit head.
    pub target: BranchSelector,
    /// Fallback for conflicts not addressed by `resolutions`.
    pub base_strategy: BaseStrategy,
    /// Per-conflict resolutions (optional). Conflicts not listed fall back to
    /// `base_strategy`. Listing a non-conflicting key is rejected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolutions: Vec<MergeResolution>,
    /// Plan-level edits applied after resolutions (optional, no scope
    /// restrictions). Use cases: tagging a merge result, recording metadata,
    /// or workarounds where resolving conflict A requires editing key B.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_patch: Option<Patch>,
}

/// Branch selector — names a branch and pins its commit head.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BranchSelector {
    /// Branch name (e.g., `"main"`, `"feature-x"`).
    pub branch: String,
    /// Expected commit head — staleness guard. Required in v1.
    pub expected: CommitId,
    /// Reserved for future "merge from any t" support. Must be absent in v1;
    /// requests including this field are rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
}

/// Fallback strategy for conflicts not named in `resolutions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BaseStrategy {
    /// Replace the target's value at each conflict key with the source's.
    TakeSource,
    /// Keep the target's value at each conflict key (merge becomes a no-op
    /// for that key).
    TakeTarget,
    /// Union of source's and target's values at each conflict key.
    TakeBoth,
    /// Fail the operation; every conflict must be explicitly resolved.
    Abort,
}

impl BaseStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            BaseStrategy::TakeSource => "take-source",
            BaseStrategy::TakeTarget => "take-target",
            BaseStrategy::TakeBoth => "take-both",
            BaseStrategy::Abort => "abort",
        }
    }
}

/// Per-conflict resolution.
///
/// `key` identifies a `(subject, predicate, graph)` triple at IRI granularity.
/// The merge engine compiles it to a SID-keyed `ConflictKey` against the
/// target ledger's namespace registry; an IRI not present in the registry is
/// rejected (since unknown IRIs cannot have produced a real conflict).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MergeResolution {
    /// The `(s, p, g)` key being resolved. Must correspond to an actual
    /// detected conflict between source and target deltas.
    pub key: ResolutionKey,
    /// Action to take at this key.
    pub action: ResolutionAction,
    /// Required iff `action == Custom`. Must be absent otherwise.
    /// Compiled flakes must lie within the resolution's `key` scope; flakes
    /// outside that scope are rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_patch: Option<Patch>,
}

/// Action verb for [`MergeResolution`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResolutionAction {
    /// Replace target's value at this key with source's.
    TakeSource,
    /// Keep target's value at this key.
    TakeTarget,
    /// Union of source's and target's values at this key.
    TakeBoth,
    /// Apply [`MergeResolution::custom_patch`] instead.
    Custom,
}

impl ResolutionAction {
    pub fn as_str(self) -> &'static str {
        match self {
            ResolutionAction::TakeSource => "take-source",
            ResolutionAction::TakeTarget => "take-target",
            ResolutionAction::TakeBoth => "take-both",
            ResolutionAction::Custom => "custom",
        }
    }
}

/// IRI-level conflict key (wire form). The merge engine resolves these
/// against the target's namespace registry to produce a SID-keyed
/// [`fluree_db_core::ConflictKey`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionKey {
    /// Subject IRI.
    pub subject: String,
    /// Predicate IRI.
    pub predicate: String,
    /// Graph IRI. `None` means the default graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
}

/// JSON-LD transaction patch.
///
/// Same shape accepted by `/v1/fluree/insert`, `/v1/fluree/upsert`, and
/// `/v1/fluree/update`: an optional `@context` plus `insert` / `delete`
/// arrays of JSON-LD nodes. The merge engine compiles these to flakes via
/// the existing transact JSON-LD pipeline.
///
/// Unknown top-level fields are rejected (`deny_unknown_fields`) so a typo
/// like `inserts` or an out-of-place `where` doesn't silently produce a
/// no-op patch.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Patch {
    /// Optional JSON-LD context. If absent, the ledger's stored default
    /// context is used; if there is none, IRIs in the patch must be fully
    /// expanded.
    #[serde(rename = "@context", default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
    /// JSON-LD nodes to assert.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub insert: Vec<Value>,
    /// JSON-LD nodes to retract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delete: Vec<Value>,
}

impl Patch {
    pub fn is_empty(&self) -> bool {
        self.insert.is_empty() && self.delete.is_empty()
    }
}

impl MergePlan {
    /// Validate the plan's internal invariants. Does not check correctness
    /// against any ledger state — that's the merge engine's job.
    ///
    /// Returns `Err(ApiError::Http { status: 400, .. })` on violations.
    pub fn validate_shape(&self) -> Result<()> {
        if self.source.at.is_some() {
            return Err(bad_request(
                "source.at is reserved for future use and must be absent",
            ));
        }
        if self.target.at.is_some() {
            return Err(bad_request(
                "target.at is reserved for future use and must be absent",
            ));
        }
        if self.source.branch.is_empty() {
            return Err(bad_request("source.branch must be non-empty"));
        }
        if self.target.branch.is_empty() {
            return Err(bad_request("target.branch must be non-empty"));
        }
        if self.source.branch == self.target.branch {
            return Err(bad_request("source.branch and target.branch must differ"));
        }

        let mut seen: HashSet<&ResolutionKey> = HashSet::with_capacity(self.resolutions.len());
        for (idx, r) in self.resolutions.iter().enumerate() {
            if !seen.insert(&r.key) {
                return Err(bad_request(format!(
                    "resolutions[{idx}]: duplicate resolution for the same conflict key",
                )));
            }
            match r.action {
                ResolutionAction::Custom => {
                    if r.custom_patch.is_none() {
                        return Err(bad_request(format!(
                            "resolutions[{idx}]: action=custom requires customPatch",
                        )));
                    }
                }
                _ => {
                    if r.custom_patch.is_some() {
                        return Err(bad_request(format!(
                            "resolutions[{idx}]: customPatch is only allowed when action=custom",
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError::Http {
        status: 400,
        message: msg.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid() -> CommitId {
        // Any well-formed CID will do for type tests.
        use std::str::FromStr;
        CommitId::from_str("bafkreidvscm5ujghhdoyy7yofngzaytqnzs3hzigq4dawagvw6ouupnaem")
            .expect("test cid parses")
    }

    fn base_plan() -> MergePlan {
        MergePlan {
            source: BranchSelector {
                branch: "feature".into(),
                expected: cid(),
                at: None,
            },
            target: BranchSelector {
                branch: "main".into(),
                expected: cid(),
                at: None,
            },
            base_strategy: BaseStrategy::Abort,
            resolutions: vec![],
            additional_patch: None,
        }
    }

    #[test]
    fn validate_accepts_minimal_plan() {
        base_plan().validate_shape().expect("minimal plan is valid");
    }

    #[test]
    fn validate_rejects_source_at() {
        let mut p = base_plan();
        p.source.at = Some("t:5".into());
        let err = p.validate_shape().unwrap_err();
        assert!(matches!(err, ApiError::Http { status: 400, .. }));
    }

    #[test]
    fn validate_rejects_same_source_target_branch() {
        let mut p = base_plan();
        p.target.branch = "feature".into();
        assert!(p.validate_shape().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_resolution_keys() {
        let key = ResolutionKey {
            subject: "ex:s".into(),
            predicate: "ex:p".into(),
            graph: None,
        };
        let mut p = base_plan();
        p.resolutions = vec![
            MergeResolution {
                key: key.clone(),
                action: ResolutionAction::TakeSource,
                custom_patch: None,
            },
            MergeResolution {
                key,
                action: ResolutionAction::TakeTarget,
                custom_patch: None,
            },
        ];
        assert!(p.validate_shape().is_err());
    }

    #[test]
    fn validate_rejects_custom_without_patch() {
        let mut p = base_plan();
        p.resolutions = vec![MergeResolution {
            key: ResolutionKey {
                subject: "ex:s".into(),
                predicate: "ex:p".into(),
                graph: None,
            },
            action: ResolutionAction::Custom,
            custom_patch: None,
        }];
        assert!(p.validate_shape().is_err());
    }

    #[test]
    fn validate_rejects_non_custom_with_patch() {
        let mut p = base_plan();
        p.resolutions = vec![MergeResolution {
            key: ResolutionKey {
                subject: "ex:s".into(),
                predicate: "ex:p".into(),
                graph: None,
            },
            action: ResolutionAction::TakeSource,
            custom_patch: Some(Patch::default()),
        }];
        assert!(p.validate_shape().is_err());
    }

    #[test]
    fn json_roundtrip_preserves_kebab_case() {
        let plan = base_plan();
        let s = serde_json::to_string(&plan).unwrap();
        assert!(s.contains("\"baseStrategy\":\"abort\""));
        let back: MergePlan = serde_json::from_str(&s).unwrap();
        assert_eq!(back.source.branch, "feature");
    }

    #[test]
    fn patch_rejects_unknown_fields() {
        // A typo like "inserts" must fail to deserialize, not become a no-op
        // patch. Same for misplaced fields like "where".
        let bad_inserts = r#"{"inserts": [{"@id": "ex:s"}]}"#;
        assert!(
            serde_json::from_str::<Patch>(bad_inserts).is_err(),
            "Patch with typo 'inserts' must be rejected"
        );
        let bad_where = r#"{"insert": [], "where": {}}"#;
        assert!(
            serde_json::from_str::<Patch>(bad_where).is_err(),
            "Patch with stray 'where' must be rejected"
        );
        // Sanity: the canonical shape parses.
        let good = r#"{"@context": {"ex": "http://example.org/"}, "insert": [], "delete": []}"#;
        assert!(serde_json::from_str::<Patch>(good).is_ok());
    }

    #[test]
    fn deny_unknown_fields_at_top_level() {
        // A request that includes `ledger` at the top level must fail to
        // deserialize — the path is the source of truth.
        let bad = r#"{
            "ledger": "mydb",
            "source": {"branch": "feature", "expected": "bafkreidvscm5ujghhdoyy7yofngzaytqnzs3hzigq4dawagvw6ouupnaem"},
            "target": {"branch": "main",    "expected": "bafkreidvscm5ujghhdoyy7yofngzaytqnzs3hzigq4dawagvw6ouupnaem"},
            "baseStrategy": "abort"
        }"#;
        assert!(serde_json::from_str::<MergePlan>(bad).is_err());
    }
}
