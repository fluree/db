//! Operator policy for the identities a graph source may borrow from the host.
//!
//! Some graph-source identities are not carried by the source at all: they are
//! the identity of the process reading it. The ambient AWS credential chain,
//! the ambient Azure credentials (a Delta table's storage with no service
//! principal of its own), the GCE/GKE metadata server, the host's gcloud
//! application-default credentials, and a Workload Identity Federation
//! exchange signed with the host's own AWS identity all hand a source
//! whatever the host can reach. On a single-tenant install that is the point;
//! on a stack that serves several tenants it lets one tenant's source read
//! with an identity the operator never granted it (the WIF config is not
//! secret, so tenant B can paste tenant A's and become A on Google).
//!
//! So the operator decides which of these a graph source may use, modelled on
//! [`crate::local_guard`]: a deployment-time switch read once from the
//! environment ([`HostIdentityPolicy::from_env`]), defaulting to everything
//! allowed so a self-hosted install behaves as before, and settable per
//! instance through the API for an embedder that restricts it in code.

use std::sync::OnceLock;

use crate::error::{IcebergError, Result};

/// Which host-derived identities graph sources may use: a comma-separated list
/// of [`HostIdentityKind`] names (`aws`, `azure`, `google-metadata`,
/// `google-adc`, `google-wif`), or `all` (the default when unset) or `none`.
///
/// Fail closed: a value that does not parse (an unknown name, `all` mixed with
/// names) allows nothing, and set-but-empty allows nothing.
pub const HOST_IDENTITIES_ENV: &str = "FLUREE_GRAPH_SOURCE_HOST_IDENTITIES";

/// The buckets an ambient-AWS graph source may read: a comma-separated list of
/// bucket names. Unset allows any bucket; set-but-empty allows none.
pub const AMBIENT_BUCKETS_ENV: &str = "FLUREE_GRAPH_SOURCE_AMBIENT_BUCKETS";

/// A host-derived identity a graph source can ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HostIdentityKind {
    /// The process's ambient AWS credential chain, reading object storage.
    Aws,
    /// The process's ambient Azure credentials (environment, managed identity),
    /// reading Azure storage for a Delta source with no `io.azure` of its own.
    Azure,
    /// The GCE/GKE metadata server's attached service account.
    GoogleMetadata,
    /// The host's gcloud application-default credentials file.
    GoogleApplicationDefault,
    /// A Workload Identity Federation exchange whose AWS subject token is
    /// signed with the host's own AWS credential chain.
    GoogleHostSignedWif,
}

impl HostIdentityKind {
    /// Every kind, in the order [`HOST_IDENTITIES_ENV`] documents them.
    pub const ALL: [HostIdentityKind; 5] = [
        HostIdentityKind::Aws,
        HostIdentityKind::Azure,
        HostIdentityKind::GoogleMetadata,
        HostIdentityKind::GoogleApplicationDefault,
        HostIdentityKind::GoogleHostSignedWif,
    ];

    /// The name [`HOST_IDENTITIES_ENV`] lists this kind by.
    pub fn as_str(self) -> &'static str {
        match self {
            HostIdentityKind::Aws => "aws",
            HostIdentityKind::Azure => "azure",
            HostIdentityKind::GoogleMetadata => "google-metadata",
            HostIdentityKind::GoogleApplicationDefault => "google-adc",
            HostIdentityKind::GoogleHostSignedWif => "google-wif",
        }
    }

    /// The kind [`as_str`](Self::as_str) names.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == name)
    }
}

impl std::fmt::Display for HostIdentityKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which host-derived identities graph sources on this instance may use.
///
/// [`Default`] allows every kind and any bucket: the behavior of an install
/// that never set a policy. A multi-tenant embedder restricts it (and supplies
/// per-tenant AWS subject credentials for WIF, which this policy does not gate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostIdentityPolicy {
    /// The ambient AWS credential chain may read object storage.
    pub ambient_aws: bool,
    /// The ambient Azure credentials may read Azure storage.
    pub ambient_azure: bool,
    /// The buckets ambient AWS may read; `None` means any bucket. Under an
    /// allowlist an ambient read that names an endpoint override is refused
    /// (the endpoint, not the URI, would choose the bucket). It governs
    /// storage reads only: an AWS Glue or S3 Tables catalog call is gated by
    /// `aws` alone, and lists whatever the host role can see.
    pub ambient_aws_buckets: Option<Vec<String>>,
    /// The GCE/GKE metadata server may mint Google tokens.
    pub google_metadata: bool,
    /// The host's gcloud application-default credentials may be used.
    pub google_application_default: bool,
    /// A WIF exchange may be signed with the host's own AWS identity.
    pub google_host_signed_wif: bool,
}

impl Default for HostIdentityPolicy {
    fn default() -> Self {
        Self::allow_all()
    }
}

impl HostIdentityPolicy {
    /// Every host identity, any bucket.
    pub fn allow_all() -> Self {
        Self {
            ambient_aws: true,
            ambient_azure: true,
            ambient_aws_buckets: None,
            google_metadata: true,
            google_application_default: true,
            google_host_signed_wif: true,
        }
    }

    /// No host identity at all: every source must carry its own.
    pub fn deny_all() -> Self {
        Self {
            ambient_aws: false,
            ambient_azure: false,
            ambient_aws_buckets: None,
            google_metadata: false,
            google_application_default: false,
            google_host_signed_wif: false,
        }
    }

    /// The policy the operator set through [`HOST_IDENTITIES_ENV`] and
    /// [`AMBIENT_BUCKETS_ENV`], read once per process (it is a deployment
    /// decision, like [`crate::local_guard::local_roots`]).
    ///
    /// A value that does not parse yields [`deny_all`](Self::deny_all) and an
    /// error log naming the variable: a typo must narrow what sources can do,
    /// never widen it, and every refusal names the variable to fix.
    pub fn from_env() -> Self {
        static POLICY: OnceLock<HostIdentityPolicy> = OnceLock::new();
        POLICY
            .get_or_init(|| {
                let kinds = std::env::var(HOST_IDENTITIES_ENV).ok();
                let buckets = std::env::var(AMBIENT_BUCKETS_ENV).ok();
                Self::parse(kinds.as_deref(), buckets.as_deref()).unwrap_or_else(|e| {
                    tracing::error!(error = %e, "graph-source host-identity policy refused; no host identity is allowed");
                    Self::deny_all()
                })
            })
            .clone()
    }

    /// Parse the two variables' values (`None` = unset). Split out from
    /// [`from_env`](Self::from_env) so it is testable without the process
    /// environment.
    pub fn parse(kinds: Option<&str>, buckets: Option<&str>) -> Result<Self> {
        let mut policy = match kinds.map(str::trim) {
            None => Self::allow_all(),
            Some("all") => Self::allow_all(),
            Some("none" | "") => Self::deny_all(),
            Some(list) => {
                let mut policy = Self::deny_all();
                for name in list.split(',').map(str::trim) {
                    let kind = HostIdentityKind::from_name(name).ok_or_else(|| {
                        IcebergError::Config(format!(
                            "{HOST_IDENTITIES_ENV}: '{name}' is not a host identity (expected a \
                             comma-separated list of aws, azure, google-metadata, google-adc, \
                             google-wif, or exactly `all` or `none`)"
                        ))
                    })?;
                    policy.set(kind, true);
                }
                policy
            }
        };
        policy.ambient_aws_buckets = buckets.map(|list| {
            list.split(',')
                .map(str::trim)
                .filter(|b| !b.is_empty())
                .map(str::to_string)
                .collect()
        });
        Ok(policy)
    }

    /// Whether `kind` is allowed.
    pub fn allows(&self, kind: HostIdentityKind) -> bool {
        match kind {
            HostIdentityKind::Aws => self.ambient_aws,
            HostIdentityKind::Azure => self.ambient_azure,
            HostIdentityKind::GoogleMetadata => self.google_metadata,
            HostIdentityKind::GoogleApplicationDefault => self.google_application_default,
            HostIdentityKind::GoogleHostSignedWif => self.google_host_signed_wif,
        }
    }

    /// `Ok` when `kind` is allowed, else the typed refusal naming it.
    pub fn ensure(&self, kind: HostIdentityKind) -> Result<()> {
        if self.allows(kind) {
            Ok(())
        } else {
            Err(IcebergError::HostIdentityRefused { kind })
        }
    }

    fn set(&mut self, kind: HostIdentityKind, allowed: bool) {
        match kind {
            HostIdentityKind::Aws => self.ambient_aws = allowed,
            HostIdentityKind::Azure => self.ambient_azure = allowed,
            HostIdentityKind::GoogleMetadata => self.google_metadata = allowed,
            HostIdentityKind::GoogleApplicationDefault => {
                self.google_application_default = allowed;
            }
            HostIdentityKind::GoogleHostSignedWif => self.google_host_signed_wif = allowed,
        }
    }

    /// A stable, non-secret rendering of the policy, for keys of caches that
    /// hold objects built under it: a client built under a looser policy must
    /// never be handed to an instance with a stricter one.
    pub fn cache_scope(&self) -> String {
        let kinds: Vec<&str> = HostIdentityKind::ALL
            .into_iter()
            .filter(|kind| self.allows(*kind))
            .map(HostIdentityKind::as_str)
            .collect();
        match &self.ambient_aws_buckets {
            None => format!("[{}]", kinds.join(",")),
            Some(buckets) => format!("[{}]buckets=[{}]", kinds.join(","), buckets.join(",")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_allows_everything_and_any_bucket() {
        let policy = HostIdentityPolicy::parse(None, None).unwrap();
        assert_eq!(policy, HostIdentityPolicy::default());
        for kind in HostIdentityKind::ALL {
            assert!(policy.allows(kind), "{kind}");
        }
        assert_eq!(policy.ambient_aws_buckets, None);
    }

    #[test]
    fn a_list_allows_exactly_its_kinds() {
        let policy = HostIdentityPolicy::parse(Some(" aws , google-wif "), None).unwrap();
        assert!(policy.allows(HostIdentityKind::Aws));
        assert!(policy.allows(HostIdentityKind::GoogleHostSignedWif));
        assert!(!policy.allows(HostIdentityKind::GoogleMetadata));
        assert!(!policy.allows(HostIdentityKind::GoogleApplicationDefault));
    }

    #[test]
    fn azure_is_a_host_identity_of_its_own_and_part_of_all() {
        assert!(HostIdentityPolicy::parse(None, None)
            .unwrap()
            .allows(HostIdentityKind::Azure));
        assert!(HostIdentityPolicy::parse(Some("all"), None)
            .unwrap()
            .allows(HostIdentityKind::Azure));
        let azure_only = HostIdentityPolicy::parse(Some("azure"), None).unwrap();
        assert!(azure_only.allows(HostIdentityKind::Azure));
        assert!(!azure_only.allows(HostIdentityKind::Aws));
        let aws_only = HostIdentityPolicy::parse(Some("aws"), None).unwrap();
        assert!(!aws_only.allows(HostIdentityKind::Azure));
        assert!(HostIdentityPolicy::deny_all()
            .ensure(HostIdentityKind::Azure)
            .unwrap_err()
            .to_string()
            .contains("'azure'"));
    }

    #[test]
    fn none_and_empty_allow_nothing() {
        for value in ["none", "", "  "] {
            let policy = HostIdentityPolicy::parse(Some(value), None).unwrap();
            for kind in HostIdentityKind::ALL {
                assert!(!policy.allows(kind), "{value:?} allowed {kind}");
            }
        }
    }

    #[test]
    fn an_unknown_name_or_a_mixed_all_is_refused() {
        for value in ["aws,gogle-adc", "all,aws", "AWS"] {
            let err = HostIdentityPolicy::parse(Some(value), None)
                .unwrap_err()
                .to_string();
            assert!(err.contains(HOST_IDENTITIES_ENV), "{value:?}: {err}");
        }
    }

    #[test]
    fn buckets_parse_and_empty_means_none_allowed() {
        let policy = HostIdentityPolicy::parse(None, Some("a, b ,")).unwrap();
        assert_eq!(
            policy.ambient_aws_buckets,
            Some(vec!["a".to_string(), "b".to_string()])
        );
        let policy = HostIdentityPolicy::parse(None, Some("")).unwrap();
        assert_eq!(policy.ambient_aws_buckets, Some(Vec::new()));
    }

    #[test]
    fn the_refusal_names_the_kind_and_the_switch() {
        let err = HostIdentityPolicy::deny_all()
            .ensure(HostIdentityKind::GoogleApplicationDefault)
            .unwrap_err();
        assert!(matches!(
            err,
            IcebergError::HostIdentityRefused {
                kind: HostIdentityKind::GoogleApplicationDefault
            }
        ));
        let message = err.to_string();
        assert!(message.contains("google-adc"), "{message}");
        assert!(message.contains(HOST_IDENTITIES_ENV), "{message}");
    }

    #[test]
    fn cache_scope_distinguishes_policies() {
        let all = HostIdentityPolicy::allow_all().cache_scope();
        let none = HostIdentityPolicy::deny_all().cache_scope();
        let buckets = HostIdentityPolicy::parse(None, Some("a"))
            .unwrap()
            .cache_scope();
        assert_ne!(all, none);
        assert_ne!(all, buckets);
    }
}
