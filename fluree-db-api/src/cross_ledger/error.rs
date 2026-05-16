//! Cross-ledger resolution errors.
//!
//! Every variant is fail-closed: the request fails. There is no
//! silent fallback to "no policy" / "no shapes" / "no schema" when a
//! model-ledger dependency cannot be resolved.
//!
//! See `docs/design/cross-ledger-model-enforcement.md` for the
//! semantics behind each variant.

use thiserror::Error;

/// A cross-ledger governance resolution failed.
///
/// Variants are distinguishable for audit and operator diagnostics —
/// callers MUST NOT collapse them into a single generic error. The
/// HTTP layer maps the wrapping `ApiError::CrossLedger` to status 502
/// uniformly, but preserves the variant in the response body so
/// clients can branch on the specific failure.
#[derive(Debug, Clone, Error)]
pub enum CrossLedgerError {
    /// `f:ledger` names a ledger that does not exist or has been
    /// dropped on this instance.
    #[error("model ledger '{ledger_id}' is not present on this instance")]
    ModelLedgerMissing {
        /// Canonical ledger id the request resolved to.
        ledger_id: String,
    },

    /// `f:ledger` resolves but the named graph IRI has no entry in the
    /// model ledger's graph registry at `resolved_t`.
    #[error(
        "graph '{graph_iri}' is not present in model ledger '{ledger_id}' at t={resolved_t}"
    )]
    GraphMissingAtT {
        /// Canonical model ledger id.
        ledger_id: String,
        /// Graph IRI that failed to resolve.
        graph_iri: String,
        /// Model ledger `t` at which lookup was attempted.
        resolved_t: i64,
    },

    /// `f:atT N` was requested but the model ledger no longer retains
    /// state at `N` (index pruning, history retention).
    #[error(
        "model ledger '{ledger_id}' no longer retains state at t={requested_t} \
         (oldest available is t={oldest_available_t})"
    )]
    TAtUnavailable {
        /// Canonical model ledger id.
        ledger_id: String,
        /// The `t` the configuration pinned.
        requested_t: i64,
        /// The oldest `t` the model ledger can still serve.
        oldest_available_t: i64,
    },

    /// The selector targets `#config` or `#txn-meta` on the model
    /// ledger.
    ///
    /// Rejected before any storage round-trip — `#txn-meta` in
    /// particular can leak commit metadata.
    #[error("selector '{graph_iri}' resolves to a reserved system graph; refusing")]
    ReservedGraphSelected {
        /// The graph IRI as configured.
        graph_iri: String,
    },

    /// The resolver successfully read the graph but could not translate
    /// it to term-neutral form (malformed rule, missing IRI on a Sid
    /// the model dictionary lost, etc.).
    #[error(
        "could not materialize graph '{graph_iri}' in model ledger '{ledger_id}': {detail}"
    )]
    TranslationFailed {
        /// Canonical model ledger id.
        ledger_id: String,
        /// Graph IRI within the model ledger.
        graph_iri: String,
        /// Free-form detail; produced by the per-artifact projector.
        detail: String,
    },

    /// `f:trustPolicy` failed verification, or `f:rollbackGuard` would
    /// be violated. Phase 4.
    #[error("trust check failed for model ledger '{ledger_id}': {detail}")]
    TrustCheckFailed {
        /// Canonical model ledger id.
        ledger_id: String,
        /// Which trust constraint was violated.
        detail: String,
    },

    /// `f:ledger` targets a ledger on a different instance.
    ///
    /// Same-instance is the v1 contract; cross-instance federation
    /// requires a different trust model and transport.
    #[error("model ledger '{ledger_id}' is on a different instance; cross-instance is not supported")]
    CrossInstanceUnsupported {
        /// The user-supplied ledger reference that triggered this.
        ledger_id: String,
    },

    /// Cycle detected through the `(ledger, graph, resolved_t)` chain.
    #[error("cycle detected in cross-ledger resolution: {}", chain_display(chain))]
    CycleDetected {
        /// The active resolution stack at the point the cycle was
        /// detected, ordered from outermost to innermost.
        chain: Vec<(String, String, i64)>,
    },
}

/// Render the resolution chain for diagnostic display.
fn chain_display(chain: &[(String, String, i64)]) -> String {
    chain
        .iter()
        .map(|(ledger, graph, t)| format!("{ledger}#{graph}@t={t}"))
        .collect::<Vec<_>>()
        .join(" → ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_display_renders_chain_in_order() {
        let err = CrossLedgerError::CycleDetected {
            chain: vec![
                ("a:main".into(), "http://ex.org/p".into(), 10),
                ("b:main".into(), "http://ex.org/q".into(), 20),
                ("a:main".into(), "http://ex.org/p".into(), 10),
            ],
        };
        let msg = err.to_string();
        assert!(msg.contains("a:main#http://ex.org/p@t=10"));
        assert!(msg.contains("b:main#http://ex.org/q@t=20"));
        assert!(msg.contains(" → "));
    }

    #[test]
    fn graph_missing_message_includes_all_coordinates() {
        let err = CrossLedgerError::GraphMissingAtT {
            ledger_id: "model:main".into(),
            graph_iri: "http://ex.org/policy".into(),
            resolved_t: 42,
        };
        let msg = err.to_string();
        assert!(msg.contains("model:main"));
        assert!(msg.contains("http://ex.org/policy"));
        assert!(msg.contains("t=42"));
    }

    #[test]
    fn all_variants_lift_into_api_error_via_from_and_map_to_502() {
        // Every CrossLedgerError variant must (a) convert into
        // ApiError::CrossLedger via From, and (b) surface as HTTP 502.
        // If a new variant is added without updating either path this
        // test will fail to compile or fail at runtime.
        let variants: Vec<CrossLedgerError> = vec![
            CrossLedgerError::ModelLedgerMissing {
                ledger_id: "m:main".into(),
            },
            CrossLedgerError::GraphMissingAtT {
                ledger_id: "m:main".into(),
                graph_iri: "http://ex.org/g".into(),
                resolved_t: 7,
            },
            CrossLedgerError::TAtUnavailable {
                ledger_id: "m:main".into(),
                requested_t: 1,
                oldest_available_t: 5,
            },
            CrossLedgerError::ReservedGraphSelected {
                graph_iri: "urn:fluree:m:main#config".into(),
            },
            CrossLedgerError::TranslationFailed {
                ledger_id: "m:main".into(),
                graph_iri: "http://ex.org/g".into(),
                detail: "malformed rule".into(),
            },
            CrossLedgerError::TrustCheckFailed {
                ledger_id: "m:main".into(),
                detail: "signer not in allowlist".into(),
            },
            CrossLedgerError::CrossInstanceUnsupported {
                ledger_id: "remote:m:main".into(),
            },
            CrossLedgerError::CycleDetected { chain: vec![] },
        ];

        for v in variants {
            let lifted: crate::error::ApiError = v.clone().into();
            assert_eq!(
                lifted.status_code(),
                502,
                "variant {v:?} must map to HTTP 502"
            );
            // Confirm we landed in the CrossLedger arm and preserved
            // the variant rather than collapsing to a string.
            match lifted {
                crate::error::ApiError::CrossLedger(_) => {}
                other => panic!("expected CrossLedger variant, got: {other:?}"),
            }
        }
    }

    #[test]
    fn cross_ledger_is_not_not_found() {
        // ModelLedgerMissing names a ledger that doesn't exist on this
        // instance — but the request itself isn't a "resource not
        // found" failure (the resource the caller asked for is on
        // *this* ledger; one of its governance dependencies is gone).
        // Surfacing as 404 would mislead clients into retrying as if
        // their target resource had been deleted. is_not_found() must
        // stay false for cross-ledger variants.
        let err: crate::error::ApiError = CrossLedgerError::ModelLedgerMissing {
            ledger_id: "m:main".into(),
        }
        .into();
        assert!(!err.is_not_found());
    }
}
