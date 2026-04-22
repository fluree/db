//! JWT Claims validation for SSE events and storage proxy authentication
//!
//! Provides validation of JWT/JWS claims for Bearer token authentication
//! on the `/fluree/events` SSE endpoint and `/fluree/storage/*` proxy endpoints.
//!
//! # Standard Claims
//! - `iss` - Issuer (must be valid did:key, match signing key)
//! - `sub` - Subject (optional)
//! - `aud` - Audience (optional, string or array)
//! - `exp` - Expiration time (required, Unix timestamp)
//! - `iat` - Issued at (optional)
//! - `nbf` - Not before (optional)
//!
//! # Fluree-specific Claims (Events)
//! - `fluree.events.all` - Grant access to all events
//! - `fluree.events.ledgers` - Grant access to specific ledgers
//! - `fluree.events.graph_sources` - Grant access to specific graph sources
//!
//! # Fluree-specific Claims (Storage Proxy)
//! - `fluree.storage.all` - Grant access to all ledgers via storage proxy
//! - `fluree.storage.ledgers` - Grant access to specific ledgers via storage proxy
//!
//! # Fluree-specific Claims (Data API)
//! - `fluree.ledger.read.all` - Grant read/query access to all ledgers
//! - `fluree.ledger.read.ledgers` - Grant read/query access to specific ledgers
//! - `fluree.ledger.write.all` - Grant transaction access to all ledgers
//! - `fluree.ledger.write.ledgers` - Grant transaction access to specific ledgers
//!
//! # Shared Claims
//! - `fluree.identity` - Identity for policy resolution

use crate::did::pubkey_from_did;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Clock skew allowance in seconds (±60s)
const CLOCK_SKEW_SECS: u64 = 60;

/// Combined payload for JWT standard claims + Fluree-specific claims.
/// Parsed once from verified JWS payload.
#[derive(Debug, Clone, Deserialize)]
pub struct EventsTokenPayload {
    // Standard JWT claims
    /// Issuer (must be valid did:key)
    pub iss: String,
    /// Subject
    pub sub: Option<String>,
    /// Audience (can be string or array in JWT)
    #[serde(default, deserialize_with = "deserialize_aud")]
    pub aud: Option<Vec<String>>,
    /// Expiration time (Unix timestamp, required)
    pub exp: u64,
    /// Issued at time (Unix timestamp)
    pub iat: Option<u64>,
    /// Not before time (Unix timestamp)
    pub nbf: Option<u64>,

    // Fluree-specific claims (Events)
    /// Grant access to all events
    #[serde(rename = "fluree.events.all")]
    pub events_all: Option<bool>,
    /// Grant access to specific ledgers
    #[serde(rename = "fluree.events.ledgers")]
    pub events_ledgers: Option<Vec<String>>,
    /// Grant access to specific graph sources
    #[serde(rename = "fluree.events.graph_sources")]
    pub events_graph_sources: Option<Vec<String>>,

    // Fluree-specific claims (Storage Proxy)
    /// Grant access to all ledgers via storage proxy
    #[serde(rename = "fluree.storage.all")]
    pub storage_all: Option<bool>,
    /// Grant access to specific ledgers via storage proxy
    #[serde(rename = "fluree.storage.ledgers")]
    pub storage_ledgers: Option<Vec<String>>,

    // Fluree-specific claims (Data API)
    /// Grant read/query access to all ledgers
    #[serde(rename = "fluree.ledger.read.all")]
    pub ledger_read_all: Option<bool>,
    /// Grant read/query access to specific ledgers
    #[serde(rename = "fluree.ledger.read.ledgers")]
    pub ledger_read_ledgers: Option<Vec<String>>,
    /// Grant transaction access to all ledgers
    #[serde(rename = "fluree.ledger.write.all")]
    pub ledger_write_all: Option<bool>,
    /// Grant transaction access to specific ledgers
    #[serde(rename = "fluree.ledger.write.ledgers")]
    pub ledger_write_ledgers: Option<Vec<String>>,

    // Shared claims
    /// Identity for policy resolution
    #[serde(rename = "fluree.identity")]
    pub fluree_identity: Option<String>,
}

/// Error type for JWT claims validation
#[derive(Debug, Error)]
pub enum ClaimsError {
    /// Token has expired (beyond clock skew allowance)
    #[error("token expired")]
    Expired,

    /// Token not yet valid (nbf in future beyond clock skew)
    #[error("token not yet valid (nbf)")]
    NotYetValid,

    /// Token issued in the future (iat beyond clock skew)
    #[error("token issued in the future (iat)")]
    IssuedInFuture,

    /// Audience claim doesn't match expected value
    #[error("audience mismatch: expected {expected}")]
    AudienceMismatch { expected: String },

    /// Issuer claim doesn't match signing key's did:key
    #[error("issuer mismatch: token iss={token_iss}, signing key={signing_did}")]
    IssuerMismatch {
        token_iss: String,
        signing_did: String,
    },

    /// Issuer is not a valid did:key format
    #[error("invalid issuer format: must be valid did:key")]
    InvalidIssuerFormat,

    /// Token grants no permissions (no events claims)
    #[error("token authorizes no resources")]
    NoPermissions,

    /// Identity required but not present
    #[error("identity required but not present (sub or fluree.identity)")]
    IdentityRequired,
}

impl EventsTokenPayload {
    /// Validate claims with clock skew allowance.
    ///
    /// # Arguments
    /// * `expected_aud` - Expected audience claim (if server requires one)
    /// * `signing_did` - The did:key derived from the JWS signing key
    /// * `require_identity` - Whether identity (sub or fluree.identity) is required
    ///
    /// # Errors
    /// Returns `ClaimsError` if any validation fails.
    pub fn validate(
        &self,
        expected_aud: Option<&str>,
        signing_did: &str,
        require_identity: bool,
    ) -> Result<(), ClaimsError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_secs();

        // iss must be valid did:key (parse via pubkey_from_did)
        if pubkey_from_did(&self.iss).is_err() {
            return Err(ClaimsError::InvalidIssuerFormat);
        }

        // exp must be in future (with skew)
        if self.exp + CLOCK_SKEW_SECS < now {
            return Err(ClaimsError::Expired);
        }

        // nbf must be in past (with skew)
        if let Some(nbf) = self.nbf {
            if nbf > now + CLOCK_SKEW_SECS {
                return Err(ClaimsError::NotYetValid);
            }
        }

        // iat must not be in future (with skew)
        if let Some(iat) = self.iat {
            if iat > now + CLOCK_SKEW_SECS {
                return Err(ClaimsError::IssuedInFuture);
            }
        }

        // aud must match if server expects one
        if let Some(expected) = expected_aud {
            match &self.aud {
                Some(audiences) if audiences.iter().any(|a| a == expected) => {}
                _ => {
                    return Err(ClaimsError::AudienceMismatch {
                        expected: expected.to_string(),
                    })
                }
            }
        }

        // iss must match signing key's did:key
        if self.iss != signing_did {
            return Err(ClaimsError::IssuerMismatch {
                token_iss: self.iss.clone(),
                signing_did: signing_did.to_string(),
            });
        }

        // identity required in Required mode
        if require_identity && self.resolve_identity().is_none() {
            return Err(ClaimsError::IdentityRequired);
        }

        Ok(())
    }

    /// Validate claims for OIDC/JWKS-verified tokens.
    ///
    /// Unlike [`validate()`], this does NOT require `iss` to be a `did:key` or match
    /// a signing key's DID. The `iss` claim is validated against the `expected_issuer`
    /// parameter, which should be the configured JWKS issuer URL.
    ///
    /// # Arguments
    /// * `expected_aud` - Expected audience claim. If `Some` and the token has no `aud`
    ///   claim, validation fails.
    /// * `expected_issuer` - The configured issuer URL. Token `iss` must exactly match.
    /// * `require_identity` - Whether identity (`sub` or `fluree.identity`) is required.
    ///
    /// # Errors
    /// Returns `ClaimsError` if any validation fails.
    pub fn validate_oidc(
        &self,
        expected_aud: Option<&str>,
        expected_issuer: &str,
        require_identity: bool,
    ) -> Result<(), ClaimsError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_secs();

        // exp must be in future (with skew)
        if self.exp + CLOCK_SKEW_SECS < now {
            return Err(ClaimsError::Expired);
        }

        // nbf must be in past (with skew)
        if let Some(nbf) = self.nbf {
            if nbf > now + CLOCK_SKEW_SECS {
                return Err(ClaimsError::NotYetValid);
            }
        }

        // iat must not be in future (with skew)
        if let Some(iat) = self.iat {
            if iat > now + CLOCK_SKEW_SECS {
                return Err(ClaimsError::IssuedInFuture);
            }
        }

        // aud must match if server expects one; fail if expected but absent
        if let Some(expected) = expected_aud {
            match &self.aud {
                Some(audiences) if audiences.iter().any(|a| a == expected) => {}
                _ => {
                    return Err(ClaimsError::AudienceMismatch {
                        expected: expected.to_string(),
                    })
                }
            }
        }

        // iss must exactly match the configured issuer
        if self.iss != expected_issuer {
            return Err(ClaimsError::IssuerMismatch {
                token_iss: self.iss.clone(),
                signing_did: expected_issuer.to_string(),
            });
        }

        // identity required in Required mode
        if require_identity && self.resolve_identity().is_none() {
            return Err(ClaimsError::IdentityRequired);
        }

        Ok(())
    }

    /// Check if token grants any events permissions.
    ///
    /// Note: If events_all is true, ledgers/graph_sources lists are irrelevant.
    pub fn has_permissions(&self) -> bool {
        self.events_all.unwrap_or(false)
            || self.events_ledgers.as_ref().is_some_and(|l| !l.is_empty())
            || self
                .events_graph_sources
                .as_ref()
                .is_some_and(|v| !v.is_empty())
    }

    /// Check if token grants storage proxy permissions.
    ///
    /// Note: If storage_all is true, ledgers list is irrelevant.
    pub fn has_storage_permissions(&self) -> bool {
        self.storage_all.unwrap_or(false)
            || self.storage_ledgers.as_ref().is_some_and(|l| !l.is_empty())
    }

    /// Check if token grants data API read permissions.
    ///
    /// Back-compat: `fluree.storage.*` claims imply read permissions if
    /// `fluree.ledger.read.*` claims are absent.
    pub fn has_ledger_read_permissions(&self) -> bool {
        self.ledger_read_all.unwrap_or(false)
            || self
                .ledger_read_ledgers
                .as_ref()
                .is_some_and(|l| !l.is_empty())
            || self.has_storage_permissions()
    }

    /// Check if token grants data API write permissions.
    pub fn has_ledger_write_permissions(&self) -> bool {
        self.ledger_write_all.unwrap_or(false)
            || self
                .ledger_write_ledgers
                .as_ref()
                .is_some_and(|l| !l.is_empty())
    }

    /// Check if token authorizes read/query access to a specific ledger ID.
    ///
    /// Back-compat: `fluree.storage.*` implies read access when ledger.read.* absent.
    pub fn is_ledger_read_authorized_for(&self, ledger_id: &str) -> bool {
        if self.ledger_read_all.unwrap_or(false) {
            return true;
        }
        if self
            .ledger_read_ledgers
            .as_ref()
            .is_some_and(|l| l.iter().any(|x| x == ledger_id))
        {
            return true;
        }
        // Back-compat: treat storage proxy scope as read scope
        self.storage_all.unwrap_or(false)
            || self
                .storage_ledgers
                .as_ref()
                .is_some_and(|l| l.iter().any(|x| x == ledger_id))
    }

    /// Check if token authorizes transaction access to a specific ledger ID.
    pub fn is_ledger_write_authorized_for(&self, ledger_id: &str) -> bool {
        self.ledger_write_all.unwrap_or(false)
            || self
                .ledger_write_ledgers
                .as_ref()
                .is_some_and(|l| l.iter().any(|x| x == ledger_id))
    }

    /// Resolve identity: fluree.identity takes precedence, then sub.
    pub fn resolve_identity(&self) -> Option<String> {
        self.fluree_identity.clone().or_else(|| self.sub.clone())
    }
}

/// Deserialize `aud` as either string or array (JWT spec allows both).
fn deserialize_aud<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrVec {
        String(String),
        Vec(Vec<String>),
    }

    Option::<StringOrVec>::deserialize(deserializer).map(|opt| {
        opt.map(|sov| match sov {
            StringOrVec::String(s) => vec![s],
            StringOrVec::Vec(v) => v,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did::did_from_pubkey;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn test_did() -> String {
        let pubkey = [0u8; 32];
        did_from_pubkey(&pubkey)
    }

    fn valid_payload(did: &str) -> EventsTokenPayload {
        EventsTokenPayload {
            iss: did.to_string(),
            sub: Some("user@example.com".to_string()),
            aud: None,
            exp: now_secs() + 3600, // 1 hour from now
            iat: Some(now_secs()),
            nbf: None,
            events_all: Some(true),
            events_ledgers: None,
            events_graph_sources: None,
            storage_all: None,
            storage_ledgers: None,
            ledger_read_all: None,
            ledger_read_ledgers: None,
            ledger_write_all: None,
            ledger_write_ledgers: None,
            fluree_identity: None,
        }
    }

    #[test]
    fn test_valid_claims() {
        let did = test_did();
        let payload = valid_payload(&did);
        assert!(payload.validate(None, &did, false).is_ok());
    }

    #[test]
    fn test_expired_token() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.exp = now_secs() - 120; // 2 minutes ago (beyond 60s skew)

        let result = payload.validate(None, &did, false);
        assert!(matches!(result, Err(ClaimsError::Expired)));
    }

    #[test]
    fn test_token_within_skew_not_expired() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.exp = now_secs() - 30; // 30 seconds ago (within 60s skew)

        assert!(payload.validate(None, &did, false).is_ok());
    }

    #[test]
    fn test_not_yet_valid() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.nbf = Some(now_secs() + 120); // 2 minutes from now (beyond 60s skew)

        let result = payload.validate(None, &did, false);
        assert!(matches!(result, Err(ClaimsError::NotYetValid)));
    }

    #[test]
    fn test_nbf_within_skew_valid() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.nbf = Some(now_secs() + 30); // 30 seconds from now (within 60s skew)

        assert!(payload.validate(None, &did, false).is_ok());
    }

    #[test]
    fn test_issued_in_future() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.iat = Some(now_secs() + 120); // 2 minutes from now (beyond 60s skew)

        let result = payload.validate(None, &did, false);
        assert!(matches!(result, Err(ClaimsError::IssuedInFuture)));
    }

    #[test]
    fn test_audience_mismatch() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.aud = Some(vec!["wrong-audience".to_string()]);

        let result = payload.validate(Some("expected-audience"), &did, false);
        assert!(matches!(result, Err(ClaimsError::AudienceMismatch { .. })));
    }

    #[test]
    fn test_audience_match_string() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.aud = Some(vec!["expected-audience".to_string()]);

        assert!(payload
            .validate(Some("expected-audience"), &did, false)
            .is_ok());
    }

    #[test]
    fn test_audience_match_array() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.aud = Some(vec![
            "other-audience".to_string(),
            "expected-audience".to_string(),
        ]);

        assert!(payload
            .validate(Some("expected-audience"), &did, false)
            .is_ok());
    }

    #[test]
    fn test_audience_not_required_when_server_doesnt_expect() {
        let did = test_did();
        let payload = valid_payload(&did);
        // No expected audience, token has no audience
        assert!(payload.validate(None, &did, false).is_ok());
    }

    #[test]
    fn test_issuer_mismatch() {
        let did = test_did();
        let payload = valid_payload(&did);
        let wrong_did = "did:key:z6MkwrongWrongWrongWrongWrongWrongWrongWrongWrongWro";

        let result = payload.validate(None, wrong_did, false);
        assert!(matches!(result, Err(ClaimsError::IssuerMismatch { .. })));
    }

    #[test]
    fn test_invalid_issuer_format() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.iss = "not-a-did-key".to_string();

        let result = payload.validate(None, &did, false);
        assert!(matches!(result, Err(ClaimsError::InvalidIssuerFormat)));
    }

    #[test]
    fn test_identity_required_missing() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.sub = None;
        payload.fluree_identity = None;

        let result = payload.validate(None, &did, true);
        assert!(matches!(result, Err(ClaimsError::IdentityRequired)));
    }

    #[test]
    fn test_identity_from_sub() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.sub = Some("user@example.com".to_string());
        payload.fluree_identity = None;

        assert!(payload.validate(None, &did, true).is_ok());
        assert_eq!(
            payload.resolve_identity(),
            Some("user@example.com".to_string())
        );
    }

    #[test]
    fn test_identity_from_fluree_identity() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.sub = Some("user@example.com".to_string());
        payload.fluree_identity = Some("did:fluree:user123".to_string());

        assert!(payload.validate(None, &did, true).is_ok());
        // fluree.identity takes precedence
        assert_eq!(
            payload.resolve_identity(),
            Some("did:fluree:user123".to_string())
        );
    }

    #[test]
    fn test_has_permissions_all() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.events_all = Some(true);
        payload.events_ledgers = None;
        payload.events_graph_sources = None;

        assert!(payload.has_permissions());
    }

    #[test]
    fn test_has_permissions_ledgers() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.events_all = None;
        payload.events_ledgers = Some(vec!["books:main".to_string()]);
        payload.events_graph_sources = None;

        assert!(payload.has_permissions());
    }

    #[test]
    fn test_has_permissions_graph_sources() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.events_all = None;
        payload.events_ledgers = None;
        payload.events_graph_sources = Some(vec!["search:main".to_string()]);

        assert!(payload.has_permissions());
    }

    #[test]
    fn test_has_permissions_none() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.events_all = None;
        payload.events_ledgers = None;
        payload.events_graph_sources = None;

        assert!(!payload.has_permissions());
    }

    #[test]
    fn test_has_permissions_empty_lists() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.events_all = Some(false);
        payload.events_ledgers = Some(vec![]);
        payload.events_graph_sources = Some(vec![]);

        assert!(!payload.has_permissions());
    }

    #[test]
    fn test_deserialize_aud_string() {
        let json = r#"{"iss":"did:key:z6MkexampleExampleExampleExampleExampleExam","exp":9999999999,"aud":"single-audience"}"#;
        let payload: EventsTokenPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.aud, Some(vec!["single-audience".to_string()]));
    }

    #[test]
    fn test_deserialize_aud_array() {
        let json = r#"{"iss":"did:key:z6MkexampleExampleExampleExampleExampleExam","exp":9999999999,"aud":["aud1","aud2"]}"#;
        let payload: EventsTokenPayload = serde_json::from_str(json).unwrap();
        assert_eq!(
            payload.aud,
            Some(vec!["aud1".to_string(), "aud2".to_string()])
        );
    }

    #[test]
    fn test_deserialize_aud_missing() {
        let json =
            r#"{"iss":"did:key:z6MkexampleExampleExampleExampleExampleExam","exp":9999999999}"#;
        let payload: EventsTokenPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.aud, None);
    }

    #[test]
    fn test_deserialize_fluree_claims() {
        let json = r#"{
            "iss": "did:key:z6MkexampleExampleExampleExampleExampleExam",
            "exp": 9999999999,
            "fluree.events.all": true,
            "fluree.events.ledgers": ["books:main", "users:main"],
            "fluree.events.graph_sources": ["search:main"],
            "fluree.identity": "did:fluree:user123"
        }"#;
        let payload: EventsTokenPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.events_all, Some(true));
        assert_eq!(
            payload.events_ledgers,
            Some(vec!["books:main".to_string(), "users:main".to_string()])
        );
        assert_eq!(
            payload.events_graph_sources,
            Some(vec!["search:main".to_string()])
        );
        assert_eq!(
            payload.fluree_identity,
            Some("did:fluree:user123".to_string())
        );
    }

    // Storage proxy permission tests
    #[test]
    fn test_has_storage_permissions_all() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.storage_all = Some(true);
        payload.storage_ledgers = None;

        assert!(payload.has_storage_permissions());
    }

    #[test]
    fn test_has_storage_permissions_ledgers() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.storage_all = None;
        payload.storage_ledgers = Some(vec!["books:main".to_string()]);

        assert!(payload.has_storage_permissions());
    }

    #[test]
    fn test_has_storage_permissions_none() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.storage_all = None;
        payload.storage_ledgers = None;

        assert!(!payload.has_storage_permissions());
    }

    #[test]
    fn test_has_storage_permissions_empty_list() {
        let did = test_did();
        let mut payload = valid_payload(&did);
        payload.storage_all = Some(false);
        payload.storage_ledgers = Some(vec![]);

        assert!(!payload.has_storage_permissions());
    }

    #[test]
    fn test_deserialize_storage_claims() {
        let json = r#"{
            "iss": "did:key:z6MkexampleExampleExampleExampleExampleExam",
            "exp": 9999999999,
            "fluree.storage.all": false,
            "fluree.storage.ledgers": ["books:main", "users:main"],
            "fluree.identity": "ex:PeerServiceAccount"
        }"#;
        let payload: EventsTokenPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.storage_all, Some(false));
        assert_eq!(
            payload.storage_ledgers,
            Some(vec!["books:main".to_string(), "users:main".to_string()])
        );
        assert_eq!(
            payload.fluree_identity,
            Some("ex:PeerServiceAccount".to_string())
        );
    }

    #[test]
    fn test_combined_events_and_storage_claims() {
        let json = r#"{
            "iss": "did:key:z6MkexampleExampleExampleExampleExampleExam",
            "exp": 9999999999,
            "fluree.events.all": true,
            "fluree.events.ledgers": ["books:main"],
            "fluree.storage.all": false,
            "fluree.storage.ledgers": ["users:main"],
            "fluree.identity": "ex:ServiceAccount"
        }"#;
        let payload: EventsTokenPayload = serde_json::from_str(json).unwrap();

        // Events permissions
        assert!(payload.has_permissions());
        assert_eq!(payload.events_all, Some(true));

        // Storage permissions
        assert!(payload.has_storage_permissions());
        assert_eq!(payload.storage_all, Some(false));
        assert_eq!(
            payload.storage_ledgers,
            Some(vec!["users:main".to_string()])
        );
    }

    // === validate_oidc tests ===

    fn oidc_payload(issuer: &str) -> EventsTokenPayload {
        EventsTokenPayload {
            iss: issuer.to_string(),
            sub: Some("user@example.com".to_string()),
            aud: None,
            exp: now_secs() + 3600,
            iat: Some(now_secs()),
            nbf: None,
            events_all: None,
            events_ledgers: None,
            events_graph_sources: None,
            storage_all: None,
            storage_ledgers: None,
            ledger_read_all: Some(true),
            ledger_read_ledgers: None,
            ledger_write_all: None,
            ledger_write_ledgers: None,
            fluree_identity: Some("did:key:z6MkTest".to_string()),
        }
    }

    #[test]
    fn test_validate_oidc_valid() {
        let payload = oidc_payload("https://solo.example.com");
        assert!(payload
            .validate_oidc(None, "https://solo.example.com", false)
            .is_ok());
    }

    #[test]
    fn test_validate_oidc_issuer_mismatch() {
        let payload = oidc_payload("https://evil.example.com");
        let result = payload.validate_oidc(None, "https://solo.example.com", false);
        assert!(matches!(result, Err(ClaimsError::IssuerMismatch { .. })));
    }

    #[test]
    fn test_validate_oidc_expired() {
        let mut payload = oidc_payload("https://solo.example.com");
        payload.exp = now_secs() - 120; // 2 minutes ago

        let result = payload.validate_oidc(None, "https://solo.example.com", false);
        assert!(matches!(result, Err(ClaimsError::Expired)));
    }

    #[test]
    fn test_validate_oidc_audience_match() {
        let mut payload = oidc_payload("https://solo.example.com");
        payload.aud = Some(vec!["fluree-server".to_string()]);

        assert!(payload
            .validate_oidc(Some("fluree-server"), "https://solo.example.com", false)
            .is_ok());
    }

    #[test]
    fn test_validate_oidc_audience_missing_when_expected() {
        let payload = oidc_payload("https://solo.example.com");
        // payload.aud is None but we expect "fluree-server"
        let result =
            payload.validate_oidc(Some("fluree-server"), "https://solo.example.com", false);
        assert!(matches!(result, Err(ClaimsError::AudienceMismatch { .. })));
    }

    #[test]
    fn test_validate_oidc_identity_required_missing() {
        let mut payload = oidc_payload("https://solo.example.com");
        payload.sub = None;
        payload.fluree_identity = None;

        let result = payload.validate_oidc(None, "https://solo.example.com", true);
        assert!(matches!(result, Err(ClaimsError::IdentityRequired)));
    }

    #[test]
    fn test_validate_oidc_url_issuer_accepted() {
        // validate_oidc should accept URL issuers (not just did:key)
        let payload = oidc_payload("https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123");
        assert!(payload
            .validate_oidc(
                None,
                "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123",
                false,
            )
            .is_ok());
    }
}
