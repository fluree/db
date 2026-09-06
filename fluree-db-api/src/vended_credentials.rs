//! Vended S3 credentials: mint STS credentials scoped to one ledger's
//! S3 prefix, so full-access peers read index content directly from S3
//! instead of proxying bytes through the origin server.
//!
//! The scope is **name-level**: a grant for `inventory:main` covers
//! `{root_prefix}/inventory/*`, which includes every branch and the
//! name-scoped `@shared/dicts` namespace — matching the all-or-nothing
//! access model of the raw block tier (see
//! `docs/design/remote-mounts.md`).
//!
//! Revocation caveat: unlike per-request block serving, a vended grant is
//! valid until it expires. Serving-posture changes (`f:serveBlocks`) and
//! token revocations take effect at grant expiry — keep TTLs short.

use crate::error::{ApiError, Result};
use serde::{Deserialize, Serialize};

/// The S3 location a server serves a ledger from, used to scope grants.
#[derive(Debug, Clone)]
pub struct S3VendScope {
    /// Bucket holding the ledger's CAS content.
    pub bucket: String,
    /// Root key prefix the storage backend prepends to every ledger path
    /// (`S3StorageConfig.prefix`), if any.
    pub root_prefix: Option<String>,
    /// Region the bucket lives in. `None` falls back to the ambient AWS
    /// SDK region at mint time (consumers need a region to build a client).
    pub region: Option<String>,
    /// Non-AWS endpoint override (LocalStack, MinIO), passed through to
    /// consumers.
    pub endpoint: Option<String>,
    /// Path-style addressing for the endpoint above, passed through to
    /// consumers. Travels with `endpoint` because it is meaningless
    /// without one, and an endpoint without it addresses virtual-hosted
    /// — which a plain MinIO rejects.
    pub force_path_style: Option<bool>,
}

impl S3VendScope {
    /// Extract a vend scope from a parsed connection config, when its index
    /// storage is S3.
    ///
    /// Returns `None` for non-S3 storage, and — for now — for split
    /// commit/index buckets (a grant would need to cover both tiers; refuse
    /// rather than mint a grant that can't read commits).
    pub fn from_connection_config(config: &fluree_db_connection::ConnectionConfig) -> Option<Self> {
        use fluree_db_connection::config::StorageType;

        let StorageType::S3(index) = &config.index_storage.storage_type else {
            return None;
        };

        if let Some(commit) = &config.commit_storage {
            if let StorageType::S3(commit_s3) = &commit.storage_type {
                if commit_s3.bucket != index.bucket || commit_s3.prefix != index.prefix {
                    tracing::warn!(
                        "vended credentials: split commit/index S3 storage is not supported; \
                         credential vending disabled"
                    );
                    return None;
                }
            }
        }

        Some(Self {
            bucket: index.bucket.to_string(),
            root_prefix: index.prefix.as_ref().map(std::string::ToString::to_string),
            region: None,
            endpoint: index
                .endpoint
                .as_ref()
                .map(std::string::ToString::to_string),
            force_path_style: index.force_path_style,
        })
    }
}

/// A minted, ledger-scoped S3 grant — the wire response of
/// `GET /storage/credentials`.
///
/// Consumers deserialize this shape in `fluree-db-nameservice-sync`; keep
/// additions backward-compatible (new fields must be optional).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VendedS3Grant {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    /// Grant expiry as Unix epoch seconds.
    pub expires_at_epoch_secs: i64,
    pub bucket: String,
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub endpoint: Option<String>,
    /// Path-style addressing for `endpoint`. Optional and defaulted, so a
    /// grant minted by an older server still deserializes.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub force_path_style: Option<bool>,
    /// Root key prefix to configure on the consumer's S3 storage
    /// (`S3StorageConfig.prefix`) so address→key mapping matches the origin.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key_prefix: Option<String>,
    /// The key prefix this grant is actually scoped to
    /// (`{key_prefix}/{ledger_name}`) — informational.
    pub scoped_prefix: String,
}

/// The key prefix a grant for `ledger_id` is scoped to: the ledger's
/// name-level prefix under the backend's root prefix.
pub fn ledger_scope_prefix(root_prefix: Option<&str>, ledger_id: &str) -> String {
    let name = ledger_id.split(':').next().unwrap_or(ledger_id);
    match root_prefix {
        Some(root) if !root.is_empty() => format!("{}/{name}", root.trim_end_matches('/')),
        _ => name.to_string(),
    }
}

/// Build the STS session policy restricting the grant to read access under
/// the ledger's prefix.
///
/// `s3:ListBucket` (prefix-conditioned) is included so missing keys surface
/// as 404 rather than 403 — the reader's not-found semantics (e.g. the
/// legacy dict-address fallback) depend on the distinction.
pub fn session_policy_json(bucket: &str, scoped_prefix: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GetLedgerObjects",
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": format!("arn:aws:s3:::{bucket}/{scoped_prefix}/*"),
            },
            {
                "Sid": "ListLedgerPrefix",
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": format!("arn:aws:s3:::{bucket}"),
                "Condition": { "StringLike": { "s3:prefix": format!("{scoped_prefix}/*") } },
            }
        ]
    })
    .to_string()
}

/// Sanitize an identity into a valid STS role-session name
/// (`[\w+=,.@-]`, 2–64 chars).
fn session_name(identity: Option<&str>) -> String {
    let raw = identity.unwrap_or("fluree-peer");
    let mut name: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '+' | '=' | ',' | '.' | '@' | '-' | '_') {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect();
    if name.len() < 2 {
        name = "fluree-peer".to_string();
    }
    name
}

/// Mint a ledger-scoped S3 grant via STS `AssumeRole`.
///
/// The assumed role's own permissions are the ceiling; the session policy
/// narrows them to the ledger's prefix. `ttl_secs` is clamped to the STS
/// minimum (900s); the role's `MaxSessionDuration` is the upper bound,
/// surfacing as an STS error if exceeded.
pub async fn mint_scoped_credentials(
    sts: &aws_sdk_sts::Client,
    role_arn: &str,
    scope: &S3VendScope,
    region: &str,
    ledger_id: &str,
    ttl_secs: i32,
    identity: Option<&str>,
) -> Result<VendedS3Grant> {
    let scoped_prefix = ledger_scope_prefix(scope.root_prefix.as_deref(), ledger_id);
    let policy = session_policy_json(&scope.bucket, &scoped_prefix);

    let assumed = sts
        .assume_role()
        .role_arn(role_arn)
        .role_session_name(session_name(identity))
        .policy(policy)
        .duration_seconds(ttl_secs.max(900))
        .send()
        .await
        .map_err(|e| ApiError::internal(format!("STS AssumeRole failed: {e}")))?;

    let creds = assumed
        .credentials()
        .ok_or_else(|| ApiError::internal("STS AssumeRole returned no credentials"))?;

    Ok(VendedS3Grant {
        access_key_id: creds.access_key_id().to_string(),
        secret_access_key: creds.secret_access_key().to_string(),
        session_token: creds.session_token().to_string(),
        expires_at_epoch_secs: creds.expiration().secs(),
        bucket: scope.bucket.clone(),
        region: region.to_string(),
        endpoint: scope.endpoint.clone(),
        force_path_style: scope.force_path_style,
        key_prefix: scope.root_prefix.clone(),
        scoped_prefix,
    })
}

/// Mint a grant using the process-global ambient AWS SDK configuration
/// (credentials chain + region), resolving the scope's region against it.
pub async fn mint_scoped_credentials_ambient(
    role_arn: &str,
    scope: &S3VendScope,
    ledger_id: &str,
    ttl_secs: i32,
    identity: Option<&str>,
) -> Result<VendedS3Grant> {
    let sdk = fluree_db_connection::aws::get_or_init_sdk_config()
        .await
        .map_err(|e| ApiError::internal(format!("AWS SDK config: {e}")))?;
    let region = scope
        .region
        .clone()
        .or_else(|| sdk.region().map(std::string::ToString::to_string))
        .ok_or_else(|| {
            ApiError::internal("no AWS region configured for credential vending (set AWS_REGION)")
        })?;
    let sts = aws_sdk_sts::Client::new(sdk);
    mint_scoped_credentials(
        &sts, role_arn, scope, &region, ledger_id, ttl_secs, identity,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_prefix_covers_name_level() {
        assert_eq!(
            ledger_scope_prefix(Some("ledgers"), "inventory:main"),
            "ledgers/inventory"
        );
        assert_eq!(ledger_scope_prefix(None, "inventory:main"), "inventory");
        assert_eq!(ledger_scope_prefix(Some(""), "inventory"), "inventory");
        assert_eq!(
            ledger_scope_prefix(Some("root/"), "inv:feature"),
            "root/inv"
        );
    }

    #[test]
    fn session_policy_scopes_get_and_list() {
        let policy = session_policy_json("my-bucket", "ledgers/inventory");
        let parsed: serde_json::Value = serde_json::from_str(&policy).unwrap();
        let statements = parsed["Statement"].as_array().unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(
            statements[0]["Resource"],
            "arn:aws:s3:::my-bucket/ledgers/inventory/*"
        );
        assert_eq!(statements[1]["Resource"], "arn:aws:s3:::my-bucket");
        assert_eq!(
            statements[1]["Condition"]["StringLike"]["s3:prefix"],
            "ledgers/inventory/*"
        );
    }

    #[test]
    fn session_name_sanitizes() {
        assert_eq!(session_name(Some("did:key:z6Mk/abc")), "did-key-z6Mk-abc");
        assert_eq!(session_name(Some("x")), "fluree-peer");
        assert_eq!(session_name(None), "fluree-peer");
        assert!(session_name(Some(&"a".repeat(100))).len() <= 64);
    }

    #[test]
    fn grant_round_trips_json() {
        let grant = VendedS3Grant {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expires_at_epoch_secs: 1_700_000_000,
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: None,
            force_path_style: None,
            key_prefix: Some("ledgers".into()),
            scoped_prefix: "ledgers/inventory".into(),
        };
        let json = serde_json::to_string(&grant).unwrap();
        let back: VendedS3Grant = serde_json::from_str(&json).unwrap();
        assert_eq!(back.bucket, "b");
        assert_eq!(back.key_prefix.as_deref(), Some("ledgers"));
        assert!(!json.contains("endpoint"), "None endpoint omitted: {json}");
        assert!(
            !json.contains("force_path_style"),
            "None force_path_style omitted: {json}"
        );
    }

    /// The MinIO knob rides the grant so a vended consumer addresses the
    /// endpoint the same way the origin does. It must serialize when set,
    /// and a grant minted before the field existed must still parse.
    #[test]
    fn grant_carries_path_style_and_tolerates_its_absence() {
        let grant = VendedS3Grant {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expires_at_epoch_secs: 1_700_000_000,
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://minio:9000".into()),
            force_path_style: Some(true),
            key_prefix: None,
            scoped_prefix: "inventory".into(),
        };
        let json = serde_json::to_string(&grant).unwrap();
        let back: VendedS3Grant = serde_json::from_str(&json).unwrap();
        assert_eq!(back.force_path_style, Some(true));

        let legacy = r#"{"access_key_id":"AKIA","secret_access_key":"secret",
            "session_token":"token","expires_at_epoch_secs":1700000000,
            "bucket":"b","region":"us-east-1","scoped_prefix":"inventory"}"#;
        let old: VendedS3Grant = serde_json::from_str(legacy).expect("pre-field grant parses");
        assert_eq!(old.force_path_style, None);
    }
}
