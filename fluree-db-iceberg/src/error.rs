//! Error types for Iceberg operations.

use thiserror::Error;

/// Errors from Iceberg operations.
///
/// `Clone` so a typed refusal can be recovered intact from an error chain
/// that only lends it out by reference (an object store's credential
/// provider, for one).
#[derive(Debug, Clone, Error)]
pub enum IcebergError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Catalog error (REST API issues)
    #[error("Catalog error: {0}")]
    Catalog(String),

    /// HTTP/network error
    #[error("HTTP error: {0}")]
    Http(String),

    /// Metadata parsing error
    #[error("Metadata error: {0}")]
    Metadata(String),

    /// Storage/IO error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Object storage denied a read (S3 or GCS 403, or a 401 that survived a
    /// renewed credential).
    ///
    /// Structured so callers can surface a 403 (rather than a generic 400/500)
    /// and name the exact object. AWS returns `AccessDenied` both for a genuine
    /// permission failure and — when the caller lacks `s3:ListBucket` — for a
    /// missing object (S3 cannot reveal 404 vs 403 without list permission), so
    /// this means the credentials lack access **or** the object was moved/removed.
    #[error(
        "Storage access denied reading {scheme}://{bucket}/{key}{region_suffix}: {message}. {hint}",
        region_suffix = .region.as_deref().map(|r| format!(" (region {r})")).unwrap_or_default(),
        hint = denial_hint(.scheme)
    )]
    StorageAccessDenied {
        /// The path's scheme as written (`s3`, `s3a`, `gs`, the schemes a
        /// store reads): what the message names the object by.
        scheme: &'static str,
        /// Bucket parsed from the `s3://`/`gs://` path.
        bucket: String,
        /// Object key parsed from the path.
        key: String,
        /// Configured/resolved region, if known.
        region: Option<String>,
        /// The underlying SDK error chain (status, request id, etc.).
        message: String,
    },

    /// A path whose scheme this source's storage identity does not serve: a
    /// `gs://` path on an AWS identity without a GCS interop endpoint, an
    /// `s3://` path on a Google identity, and so on. The identity is decided by
    /// the source's configuration, never by the path, so an unserved path is
    /// refused rather than read with whatever identity happens to accept its
    /// scheme (an `s3` client would read `gs://b/k` as `s3://b/k`, on AWS).
    #[error(
        "Refusing to read {path}: this source's storage identity ({identity}) serves only \
         {served} paths. Set io.storage_auth to an identity that serves {scheme}:// paths"
    )]
    SchemeNotServed {
        /// The refused path.
        path: String,
        /// Its scheme (`s3`, `gs`, `file`, …).
        scheme: String,
        /// The identity that refused it (`ambient AWS`, `Google`, …).
        identity: String,
        /// The schemes that identity serves, for the message.
        served: String,
    },

    /// An ambient-AWS read through an `s3_endpoint` override while the operator
    /// restricts ambient AWS to a bucket allowlist. An endpoint can name a
    /// bucket of its own (virtual-hosted or path-style addressing), so the
    /// bucket a path names is not the bucket read and the allowlist could not
    /// hold.
    #[error(
        "Graph sources on this instance may not read through an S3 endpoint override \
         ({endpoint}) with the host's ambient AWS identity: ambient reads are restricted to a \
         bucket allowlist ({env}), and an endpoint can address a bucket of its own",
        env = crate::host_identity::AMBIENT_BUCKETS_ENV
    )]
    AmbientEndpointRefused {
        /// The refused endpoint.
        endpoint: String,
    },

    /// A catalog placed a table on this host's local filesystem. Only a
    /// catalog-less (Direct) source may read local files: the operator
    /// registered that location, while a catalog's answer is not the
    /// operator's to vouch for.
    #[error(
        "Refusing to read {location}: the catalog placed the table on the local filesystem, \
         which only a catalog-less (Direct) source may read"
    )]
    LocalLocationFromCatalog {
        /// The location the catalog returned.
        location: String,
    },

    /// An ambient-AWS read of a bucket the operator's allowlist
    /// ([`crate::host_identity::AMBIENT_BUCKETS_ENV`]) does not name.
    #[error(
        "Graph sources on this instance may not read bucket '{bucket}' with the host's ambient \
         AWS identity: the bucket allowlist ({env}) does not name it",
        env = crate::host_identity::AMBIENT_BUCKETS_ENV
    )]
    AmbientBucketRefused {
        /// The refused bucket.
        bucket: String,
    },

    /// The source is configured to require catalog-vended storage credentials
    /// (`io.vended_credentials = true`) but the catalog's `loadTable` returned none.
    ///
    /// Carried as an `IcebergError` ONLY so the typed error survives the deferred
    /// `LazyS3Storage` builder's `Result` channel (which is `IcebergError`-typed);
    /// `storage_query_error` lifts it straight back to
    /// `QueryError::CatalogCredentialsNotVended` and its `err:catalog/CredentialsNotVended`
    /// wire code, so the lazy path reports identically to the eager one. Never
    /// construct it for a Direct-mode source — Direct never requests vending.
    #[error(
        "Catalog {catalog_uri} authorized the table but vended no storage credentials; \
         either fix the catalog's credential vending or set vended_credentials=false \
         on the source to explicitly use ambient AWS credentials"
    )]
    CatalogCredentialsNotVended {
        /// The REST catalog URI that authorized the table but vended nothing.
        catalog_uri: String,
    },

    /// The catalog refused this principal access to a table or listing (an AWS
    /// `AccessDeniedException` / 403), in the catalog's own words. Lifted to
    /// `QueryError::CatalogAccessDenied` (`err:catalog/AccessDenied`, HTTP 403) on
    /// the query path, so a missing IAM grant reads as one rather than as an
    /// internal error.
    #[error("Catalog denied access to {table}: {message}")]
    CatalogAccessDenied {
        /// The table (or catalog object) as the catalog names it.
        table: String,
        /// The catalog's refusal, with its full error chain.
        message: String,
    },

    /// A catalog on Google refused the request because no quota project was
    /// named (`USER_PROJECT_DENIED`, "requires a quota project", BigLake's
    /// "missing `x-goog-user-project` header"). BigLake bills every request,
    /// whatever the identity (user credentials, a service account, WIF), to a
    /// project the request names; the fix is the source's `quota_project`,
    /// not a broader grant.
    #[error(
        "Catalog request for {table} needs a quota project: Google bills the request to the \
         project named in x-goog-user-project (BigLake requires one for every identity); set \
         quota_project on the source's Google auth"
    )]
    CatalogQuotaProjectRequired {
        /// The table (or catalog route) the refused request was for.
        table: String,
    },

    /// A Google token exchange was refused or failed. Carries Google's own
    /// error code (`invalid_grant`, `PERMISSION_DENIED`) or one of Fluree's
    /// fixed codes, never a response body: a token endpoint's body can echo
    /// request material.
    #[error(
        "Google token exchange failed at the {stage} step ({outcome}){hint}",
        outcome = exchange_outcome(*.status, .reason.as_deref()),
        hint = if *.stage == GoogleTokenStage::Metadata {
            "; the metadata server is reachable only on GCE/GKE"
        } else {
            ""
        }
    )]
    GoogleTokenExchange {
        /// Which exchange failed.
        stage: GoogleTokenStage,
        /// Google's HTTP status, or `None` when no response arrived.
        status: Option<u16>,
        /// Google's error code, or a fixed Fluree code; never a body.
        reason: Option<String>,
    },

    /// A Google credential configuration was refused. The message names the
    /// offending field, never its value.
    #[error("Invalid Google credentials: {0}")]
    GoogleCredentialsInvalid(String),

    /// A host-derived identity was refused by this instance's host-identity
    /// policy (see [`crate::host_identity`]).
    #[error(
        "Graph sources on this instance may not use the host identity '{kind}': the host-identity \
         policy ({env}) does not allow it",
        env = crate::host_identity::HOST_IDENTITIES_ENV
    )]
    HostIdentityRefused {
        /// The refused kind.
        kind: crate::host_identity::HostIdentityKind,
    },

    /// A token minted from a host identity was about to go to a host that is
    /// not one of Google's APIs. A host identity is the server's own, so its
    /// token goes only to `https://*.googleapis.com` (or a host the operator
    /// names), never to wherever a source's configuration points.
    #[error(
        "A token from the host identity '{kind}' is only sent to Google APIs \
         (https://googleapis.com and its subdomains); refusing to send it to '{host}'. An \
         operator can name more hosts in {env}; a source's own service-account key or \
         Workload Identity Federation subject is not restricted this way",
        env = crate::auth::google::TOKEN_HOSTS_ENV
    )]
    GoogleTokenHostRefused {
        /// The host identity the token is from.
        kind: crate::host_identity::HostIdentityKind,
        /// The host the request was for (never the token).
        host: String,
    },

    /// Snapshot not found
    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Credential error
    #[error("Credential error: {0}")]
    Credential(String),

    /// Manifest parsing error
    #[error("Manifest error: {0}")]
    Manifest(String),

    /// Scan planning error
    #[error("Scan error: {0}")]
    Scan(String),

    /// Merge-on-read delete files are present but not yet applied. Raised by the
    /// fail-closed guard (see [`crate::mor_guard`]) rather than silently
    /// returning deleted rows / over-counting. Audit finding F-AUD-1.
    #[error("Merge-on-read deletes not applied: {0}")]
    MergeOnReadDeletes(String),

    /// Unsupported file format
    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),
}

impl IcebergError {
    /// Whether this is one of the graph-source identity refusals — a Google
    /// token exchange refused, invalid Google credentials, a host identity the
    /// policy refuses, a catalog that needs a quota project — that every
    /// surface (Iceberg, SQL, Delta) carries typed to its own wire code rather
    /// than flattening into a message.
    pub fn is_identity_refusal(&self) -> bool {
        matches!(
            self,
            IcebergError::GoogleTokenExchange { .. }
                | IcebergError::GoogleCredentialsInvalid(_)
                | IcebergError::HostIdentityRefused { .. }
                | IcebergError::GoogleTokenHostRefused { .. }
                | IcebergError::CatalogQuotaProjectRequired { .. }
        )
    }

    /// Whether this is a refusal the source's configuration and the
    /// instance's host-identity policy decide, which no retry or change on the
    /// remote side lifts: a host identity the policy refuses, an ambient read
    /// its bucket allowlist refuses (a bucket off it, or an endpoint override
    /// under it), a host identity's token bound for a host it may not reach, a
    /// table a catalog placed on this host's disk, or a path the source's
    /// identity does not serve. Registration refuses a source whose storage it
    /// can decide and gets one of these, rather than every later query.
    pub fn is_configuration_refusal(&self) -> bool {
        matches!(
            self,
            IcebergError::HostIdentityRefused { .. }
                | IcebergError::AmbientBucketRefused { .. }
                | IcebergError::AmbientEndpointRefused { .. }
                | IcebergError::GoogleTokenHostRefused { .. }
                | IcebergError::LocalLocationFromCatalog { .. }
                | IcebergError::SchemeNotServed { .. }
        )
    }

    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Auth(msg.into())
    }

    pub fn catalog(msg: impl Into<String>) -> Self {
        Self::Catalog(msg.into())
    }

    pub fn metadata(msg: impl Into<String>) -> Self {
        Self::Metadata(msg.into())
    }

    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    pub fn credential(msg: impl Into<String>) -> Self {
        Self::Credential(msg.into())
    }

    pub fn manifest(msg: impl Into<String>) -> Self {
        Self::Manifest(msg.into())
    }

    pub fn scan(msg: impl Into<String>) -> Self {
        Self::Scan(msg.into())
    }

    pub fn merge_on_read_deletes(msg: impl Into<String>) -> Self {
        Self::MergeOnReadDeletes(msg.into())
    }

    pub fn unsupported_format(msg: impl Into<String>) -> Self {
        Self::UnsupportedFormat(msg.into())
    }
}

/// The Google token exchange a [`IcebergError::GoogleTokenExchange`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoogleTokenStage {
    /// Workload Identity Federation's exchange at `sts.googleapis.com`.
    Sts,
    /// Service-account impersonation at `iamcredentials.googleapis.com`.
    Impersonation,
    /// A service-account JWT assertion at `oauth2.googleapis.com`.
    Jwt,
    /// A user refresh token at `oauth2.googleapis.com`.
    Refresh,
    /// The GCE/GKE metadata server.
    Metadata,
}

impl GoogleTokenStage {
    const ALL: [GoogleTokenStage; 5] = [
        GoogleTokenStage::Sts,
        GoogleTokenStage::Impersonation,
        GoogleTokenStage::Jwt,
        GoogleTokenStage::Refresh,
        GoogleTokenStage::Metadata,
    ];

    /// The stage [`as_str`](Self::as_str) names.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|stage| stage.as_str() == name)
    }

    /// The stage's stable name.
    pub fn as_str(self) -> &'static str {
        match self {
            GoogleTokenStage::Sts => "sts",
            GoogleTokenStage::Impersonation => "impersonation",
            GoogleTokenStage::Jwt => "jwt",
            GoogleTokenStage::Refresh => "refresh",
            GoogleTokenStage::Metadata => "metadata",
        }
    }
}

impl std::fmt::Display for GoogleTokenStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a denial most likely means, by store.
/// `err` and every error that caused it, each once: `err: cause: cause`.
/// Only `Display` text, never `Debug`. A level whose text the level above it
/// already showed is skipped, since a wrapper that renders its source's text
/// (as `object_store` does) would otherwise print it twice.
#[cfg(feature = "aws")]
pub(crate) fn concise_chain(err: &dyn std::error::Error) -> String {
    use std::fmt::Write;
    let mut shown = err.to_string();
    let mut previous = shown.clone();
    let mut source = err.source();
    while let Some(inner) = source {
        let text = inner.to_string();
        if !previous.contains(&text) {
            let _ = write!(shown, ": {text}");
        }
        previous = text;
        source = inner.source();
    }
    shown
}

fn denial_hint(scheme: &str) -> &'static str {
    match scheme {
        "gs" => {
            "The identity lacks storage.objects.get (or storage.objects.list) on this object, \
             or the object no longer exists. UserProjectAccessDenied means the identity lacks \
             serviceusage.services.use on the project its requests bill to \
             (x-goog-user-project)."
        }
        _ => {
            "The credentials lack permission for this object, or — without s3:ListBucket — the \
             object no longer exists (a missing object also surfaces as AccessDenied)."
        }
    }
}

fn exchange_outcome(status: Option<u16>, reason: Option<&str>) -> String {
    match (status, reason) {
        (Some(status), Some(reason)) => format!("HTTP {status}, {reason}"),
        (Some(status), None) => format!("HTTP {status}"),
        (None, Some(reason)) => reason.to_string(),
        (None, None) => "no response".to_string(),
    }
}

/// Result type for Iceberg operations.
pub type Result<T> = std::result::Result<T, IcebergError>;

// Integration with core errors
impl From<IcebergError> for fluree_db_core::error::Error {
    fn from(err: IcebergError) -> Self {
        match &err {
            IcebergError::TableNotFound(msg) => fluree_db_core::error::Error::not_found(msg),
            IcebergError::Storage(msg) => fluree_db_core::error::Error::storage(msg),
            // Access-denied is still a storage-read failure at the core layer
            // (core has no unauthorized variant); the full message is preserved.
            IcebergError::StorageAccessDenied { .. } => {
                fluree_db_core::error::Error::storage(err.to_string())
            }
            IcebergError::CatalogCredentialsNotVended { .. } => {
                fluree_db_core::error::Error::other(err.to_string())
            }
            // Auth and other errors map to generic "other" since core doesn't have unauthorized
            _ => fluree_db_core::error::Error::other(err.to_string()),
        }
    }
}

// Integration with tabular errors
impl From<fluree_db_tabular::TabularError> for IcebergError {
    fn from(err: fluree_db_tabular::TabularError) -> Self {
        match err {
            fluree_db_tabular::TabularError::Schema(msg) => IcebergError::Scan(msg),
        }
    }
}

// Integration with reqwest errors
impl From<reqwest::Error> for IcebergError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            IcebergError::Http(format!("Request timeout: {err}"))
        } else if err.is_connect() {
            IcebergError::Http(format!("Connection error: {err}"))
        } else {
            IcebergError::Http(format!("HTTP error: {err}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_identity::HostIdentityKind;

    /// The refusals registration fails on are the ones the configuration and
    /// policy decide; a denial, a token exchange or a network failure can
    /// change without the configuration changing, so it is not one.
    #[test]
    fn configuration_refusals_are_the_ones_no_retry_lifts() {
        let refusals = [
            IcebergError::HostIdentityRefused {
                kind: HostIdentityKind::Aws,
            },
            IcebergError::AmbientBucketRefused {
                bucket: "b".to_string(),
            },
            IcebergError::AmbientEndpointRefused {
                endpoint: "https://e.example".to_string(),
            },
            IcebergError::GoogleTokenHostRefused {
                kind: HostIdentityKind::GoogleMetadata,
                host: "h.example".to_string(),
            },
            IcebergError::LocalLocationFromCatalog {
                location: "file:///t".to_string(),
            },
            IcebergError::SchemeNotServed {
                path: "gs://b/k".to_string(),
                scheme: "gs".to_string(),
                identity: "ambient AWS".to_string(),
                served: "s3://".to_string(),
            },
        ];
        for refusal in &refusals {
            assert!(refusal.is_configuration_refusal(), "{refusal:?}");
        }
        let changeable = [
            IcebergError::StorageAccessDenied {
                scheme: "s3",
                bucket: "b".to_string(),
                key: "k".to_string(),
                region: None,
                message: "AccessDenied".to_string(),
            },
            IcebergError::CatalogAccessDenied {
                table: "t".to_string(),
                message: "403".to_string(),
            },
            IcebergError::GoogleTokenExchange {
                stage: GoogleTokenStage::Jwt,
                status: Some(503),
                reason: None,
            },
            IcebergError::Http("connection refused".to_string()),
        ];
        for error in &changeable {
            assert!(!error.is_configuration_refusal(), "{error:?}");
        }
    }
}
