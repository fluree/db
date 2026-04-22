//! Content-addressed ledger configuration for origin discovery.
//!
//! `LedgerConfig` is a content-addressed object (identified by its CID) that
//! describes where to fetch content for a ledger. It contains an ordered list
//! of origins with priority, transport URIs, and declarative auth requirements.
//!
//! ## Serialization
//!
//! Stored bytes are **canonical JSON with `f:`-prefixed compact keys**. This is
//! NOT JSON-LD — no `@context`/`@type` in stored bytes. JSON-LD framing is a
//! presentation concern for API responses only.
//!
//! ## Determinism
//!
//! `to_bytes()` produces deterministic output:
//! - NO `HashMap` in stored types — only `Vec` and fixed structs
//! - Origins are sorted by a total order over all serialized fields before serialization
//!   Primary key: `(priority, transport, auth.mode)`;
//!   tie-breakers: `(enabled, audience, scopes)`
//! - Auth scopes are sorted and deduplicated within each origin
//! - `Origin` has an explicit `Ord` impl that is a total order (stable across sort implementations)

use fluree_db_core::ContentId;
use fluree_db_core::ContentKind;
use serde::{Deserialize, Serialize};

/// Content-addressed ledger configuration for origin discovery.
///
/// Contains a priority-ordered list of content origins and replication defaults.
/// The canonical JSON bytes are the CID preimage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LedgerConfig {
    /// Priority-ordered list of content origins.
    #[serde(rename = "f:origins")]
    pub origins: Vec<Origin>,

    /// Replication defaults (pack preference, size limits).
    #[serde(rename = "f:replication", default)]
    pub replication: ReplicationDefaults,
}

/// A content origin — where to fetch CAS objects.
///
/// `Ord` is a **total order over all serialized fields** so that `Vec::sort()`
/// (which is not stable across Rust versions) produces a deterministic result
/// for CID computation. Primary key: `(priority, transport, auth.mode)`.
/// Tie-breakers: `(enabled, auth.audience, auth.scopes)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Origin {
    /// Lower number = higher priority.
    #[serde(rename = "f:priority")]
    pub priority: u32,

    /// Whether this origin is currently enabled.
    #[serde(rename = "f:enabled", default = "default_true")]
    pub enabled: bool,

    /// Transport URI (e.g., "https://api.example.com/v1/fluree",
    /// "s3://bucket/prefix", "ipfs://").
    #[serde(rename = "f:transport")]
    pub transport: String,

    /// Declarative auth requirement (no secrets stored in config).
    #[serde(rename = "f:auth", default)]
    pub auth: AuthRequirement,
}

impl Ord for Origin {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.transport.cmp(&other.transport))
            .then_with(|| self.auth.mode.cmp(&other.auth.mode))
            // Tie-breakers: total order over remaining serialized fields
            .then_with(|| self.enabled.cmp(&other.enabled))
            .then_with(|| self.auth.audience.cmp(&other.auth.audience))
            .then_with(|| self.auth.scopes.cmp(&other.auth.scopes))
    }
}

impl PartialOrd for Origin {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Declarative auth requirement for an origin.
///
/// Describes what authentication is needed, not the credentials themselves.
/// Clients match requirements against locally available credentials.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthRequirement {
    /// Auth mode: "none", "bearer", "oidc-device", "mtls", "signed-request"
    #[serde(rename = "f:mode", default = "default_none")]
    pub mode: String,

    /// Token audience (for bearer/OIDC).
    #[serde(
        rename = "f:audience",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub audience: Option<String>,

    /// Required scopes.
    #[serde(rename = "f:scopes", default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
}

impl Default for AuthRequirement {
    fn default() -> Self {
        Self {
            mode: "none".to_string(),
            audience: None,
            scopes: Vec::new(),
        }
    }
}

/// Replication defaults.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicationDefaults {
    /// Whether to prefer pack-based transfer over per-object fetch.
    #[serde(rename = "f:preferPack", default = "default_true")]
    pub prefer_pack: bool,

    /// Maximum pack size in MiB.
    #[serde(rename = "f:maxPackMiB", default = "default_64")]
    pub max_pack_mib: u32,
}

impl Default for ReplicationDefaults {
    fn default() -> Self {
        Self {
            prefer_pack: true,
            max_pack_mib: 64,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_none() -> String {
    "none".to_string()
}

fn default_64() -> u32 {
    64
}

impl LedgerConfig {
    /// Serialize to canonical JSON bytes (deterministic).
    ///
    /// Canonicalization steps:
    /// 1. Auth scopes sorted and deduplicated within each origin
    /// 2. Origins sorted by total order over all serialized fields
    ///
    /// This ensures logically equivalent configs produce the same bytes
    /// (and thus the same CID).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut canonical = self.clone();
        for origin in &mut canonical.origins {
            origin.auth.scopes.sort();
            origin.auth.scopes.dedup();
        }
        canonical.origins.sort();
        serde_json::to_vec(&canonical).expect("LedgerConfig serialization cannot fail")
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    /// Compute the content identifier (CID) for this config.
    ///
    /// Uses the canonical (sorted) JSON bytes as the hash preimage.
    pub fn content_id(&self) -> ContentId {
        ContentId::new(ContentKind::LedgerConfig, &self.to_bytes())
    }

    /// Get enabled origins sorted by ascending priority.
    pub fn active_origins(&self) -> Vec<&Origin> {
        let mut origins: Vec<&Origin> = self.origins.iter().filter(|o| o.enabled).collect();
        origins.sort_by_key(|o| o.priority);
        origins
    }
}

impl Origin {
    /// Create a new HTTP origin with no auth requirement.
    pub fn http(priority: u32, url: impl Into<String>) -> Self {
        Self {
            priority,
            enabled: true,
            transport: url.into(),
            auth: AuthRequirement::default(),
        }
    }

    /// Create a new HTTP origin with bearer auth requirement.
    pub fn http_bearer(priority: u32, url: impl Into<String>, audience: Option<String>) -> Self {
        Self {
            priority,
            enabled: true,
            transport: url.into(),
            auth: AuthRequirement {
                mode: "bearer".to_string(),
                audience,
                scopes: Vec::new(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> LedgerConfig {
        LedgerConfig {
            origins: vec![
                Origin::http_bearer(
                    10,
                    "https://api.example.com/v1/fluree",
                    Some("fluree-content".to_string()),
                ),
                Origin::http(20, "https://cdn.example.com/fluree/objects/"),
            ],
            replication: ReplicationDefaults::default(),
        }
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = sample_config();
        let bytes = config.to_bytes();
        let parsed = LedgerConfig::from_bytes(&bytes).unwrap();
        // Note: to_bytes() sorts origins, so parsed may differ in order from original.
        // But to_bytes() on both should be identical.
        assert_eq!(parsed.to_bytes(), bytes);
    }

    #[test]
    fn test_content_id_determinism() {
        let config = sample_config();
        let id1 = config.content_id();
        let id2 = config.content_id();
        assert_eq!(id1, id2);

        // Same config with origins in different order → same CID
        let mut config_reversed = config.clone();
        config_reversed.origins.reverse();
        let id3 = config_reversed.content_id();
        assert_eq!(id1, id3);
    }

    #[test]
    fn test_content_id_different_for_different_config() {
        let config1 = sample_config();
        let config2 = LedgerConfig {
            origins: vec![Origin::http(10, "https://other.example.com/fluree")],
            replication: ReplicationDefaults::default(),
        };
        assert_ne!(config1.content_id(), config2.content_id());
    }

    #[test]
    fn test_active_origins_filters_and_sorts() {
        let config = LedgerConfig {
            origins: vec![
                Origin {
                    priority: 30,
                    enabled: true,
                    transport: "https://c.example.com".to_string(),
                    auth: AuthRequirement::default(),
                },
                Origin {
                    priority: 10,
                    enabled: false, // disabled
                    transport: "https://a.example.com".to_string(),
                    auth: AuthRequirement::default(),
                },
                Origin {
                    priority: 20,
                    enabled: true,
                    transport: "https://b.example.com".to_string(),
                    auth: AuthRequirement::default(),
                },
            ],
            replication: ReplicationDefaults::default(),
        };

        let active = config.active_origins();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].priority, 20);
        assert_eq!(active[1].priority, 30);
    }

    #[test]
    fn test_auth_requirement_default() {
        let auth = AuthRequirement::default();
        assert_eq!(auth.mode, "none");
        assert!(auth.audience.is_none());
        assert!(auth.scopes.is_empty());
    }

    #[test]
    fn test_replication_defaults() {
        let repl = ReplicationDefaults::default();
        assert!(repl.prefer_pack);
        assert_eq!(repl.max_pack_mib, 64);
    }

    #[test]
    fn test_json_uses_f_prefix_keys() {
        let config = LedgerConfig {
            origins: vec![Origin::http(10, "https://example.com")],
            replication: ReplicationDefaults::default(),
        };
        let json = String::from_utf8(config.to_bytes()).unwrap();
        assert!(json.contains("\"f:origins\""));
        assert!(json.contains("\"f:priority\""));
        assert!(json.contains("\"f:transport\""));
        assert!(json.contains("\"f:replication\""));
        assert!(json.contains("\"f:preferPack\""));
    }

    #[test]
    fn test_content_id_has_ledger_config_codec() {
        let config = sample_config();
        let id = config.content_id();
        assert_eq!(id.content_kind(), Some(ContentKind::LedgerConfig));
    }

    #[test]
    fn test_origin_constructors() {
        let o1 = Origin::http(10, "https://example.com");
        assert_eq!(o1.priority, 10);
        assert!(o1.enabled);
        assert_eq!(o1.auth.mode, "none");

        let o2 = Origin::http_bearer(20, "https://api.example.com", Some("aud".to_string()));
        assert_eq!(o2.auth.mode, "bearer");
        assert_eq!(o2.auth.audience, Some("aud".to_string()));
    }

    #[test]
    fn test_scope_canonicalization() {
        // Scopes in different order + duplicates → same CID
        let config1 = LedgerConfig {
            origins: vec![Origin {
                priority: 10,
                enabled: true,
                transport: "https://example.com".to_string(),
                auth: AuthRequirement {
                    mode: "bearer".to_string(),
                    audience: None,
                    scopes: vec!["write".to_string(), "read".to_string(), "read".to_string()],
                },
            }],
            replication: ReplicationDefaults::default(),
        };
        let config2 = LedgerConfig {
            origins: vec![Origin {
                priority: 10,
                enabled: true,
                transport: "https://example.com".to_string(),
                auth: AuthRequirement {
                    mode: "bearer".to_string(),
                    audience: None,
                    scopes: vec!["read".to_string(), "write".to_string()],
                },
            }],
            replication: ReplicationDefaults::default(),
        };
        assert_eq!(config1.content_id(), config2.content_id());
    }

    #[test]
    fn test_origin_sort_key() {
        // Same priority, different transport → sorted by transport
        let a = Origin::http(10, "https://aaa.example.com");
        let b = Origin::http(10, "https://zzz.example.com");
        assert!(a < b);

        // Different priority → sorted by priority regardless of transport
        let low = Origin::http(5, "https://zzz.example.com");
        let high = Origin::http(20, "https://aaa.example.com");
        assert!(low < high);
    }
}
