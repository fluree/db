//! Vended credentials that renew themselves inside the store.
//!
//! A catalog vends a table's storage credentials once, at `loadTable`, and they
//! expire (an hour, typically). A scan or materialize that outlives them would
//! fail mid-read, so the store holds a [`RenewingVendedCredentials`]: every
//! request asks it for the current credential, and inside a margin before
//! expiry it asks the catalog again (single-flight: one re-vend in flight,
//! concurrent readers wait for it). A still-valid credential is kept when a
//! re-vend fails; one the store has refused is not.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rand::Rng;

use crate::catalog::{SendCatalogClient, TableIdentifier};
use crate::credential::VendedCredentials;
use crate::error::{IcebergError, Result};

/// How long before expiry a held credential is renewed, before jitter: long
/// enough that a request which takes it has time to complete.
pub const DEFAULT_RENEWAL_MARGIN: std::time::Duration = std::time::Duration::from_secs(60);

/// Where a table's vended credentials are fetched again.
#[async_trait]
pub trait VendedCredentialSource: std::fmt::Debug + Send + Sync {
    /// The table's credentials, fresh from the catalog.
    async fn revend(&self) -> Result<VendedCredentials>;
}

/// The catalog that vended a table's credentials, asked again through its
/// per-table credentials endpoint (see [`SendCatalogClient::load_credentials`]).
#[derive(Debug)]
pub struct CatalogVendedSource {
    catalog: Arc<dyn SendCatalogClient>,
    table: TableIdentifier,
    metadata_location: String,
}

impl CatalogVendedSource {
    /// Re-vend `table` (whose metadata is at `metadata_location`, the location
    /// the vended prefix is matched against) from `catalog`.
    pub fn new(
        catalog: Arc<dyn SendCatalogClient>,
        table: TableIdentifier,
        metadata_location: impl Into<String>,
    ) -> Self {
        Self {
            catalog,
            table,
            metadata_location: metadata_location.into(),
        }
    }
}

#[async_trait]
impl VendedCredentialSource for CatalogVendedSource {
    async fn revend(&self) -> Result<VendedCredentials> {
        self.catalog
            .load_credentials(&self.table, &self.metadata_location)
            .await?
            .ok_or_else(|| {
                IcebergError::Credential(format!(
                    "the catalog vended no storage credentials for {}.{} on renewal",
                    self.table.namespace, self.table.table
                ))
            })
    }
}

/// A table's vended credential, renewed from its catalog before it expires.
pub struct RenewingVendedCredentials {
    held: tokio::sync::Mutex<VendedCredentials>,
    /// Set when the store refused the held credential: renew on next use, and
    /// never fall back to it.
    refused: AtomicBool,
    source: Option<Arc<dyn VendedCredentialSource>>,
    margin: std::time::Duration,
}

/// Redacting `Debug`: the held credential is never shown.
impl std::fmt::Debug for RenewingVendedCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenewingVendedCredentials")
            .field("renews", &self.source.is_some())
            .field("margin", &self.margin)
            .finish_non_exhaustive()
    }
}

impl RenewingVendedCredentials {
    /// Hold `initial`, renewing it from `source` (when there is one) inside
    /// [`DEFAULT_RENEWAL_MARGIN`] of its expiry. Without a source the
    /// credential is served until it expires and then refused.
    pub fn new(
        initial: VendedCredentials,
        source: Option<Arc<dyn VendedCredentialSource>>,
    ) -> Self {
        Self {
            held: tokio::sync::Mutex::new(initial),
            refused: AtomicBool::new(false),
            source,
            margin: DEFAULT_RENEWAL_MARGIN,
        }
    }

    /// Renew `margin` before expiry instead of the default.
    pub fn with_margin(mut self, margin: std::time::Duration) -> Self {
        self.margin = margin;
        self
    }

    /// The credential to sign the next request with: the held one while it is
    /// outside the renewal margin, else a fresh one from the catalog.
    pub async fn current(&self) -> Result<VendedCredentials> {
        let mut held = self.held.lock().await;
        let refused = self.refused.load(Ordering::Acquire);
        if !refused && !self.renewal_due(&held) {
            return Ok(held.clone());
        }
        let Some(source) = &self.source else {
            return if !refused && still_valid(&held) {
                Ok(held.clone())
            } else {
                Err(IcebergError::Credential(
                    "the table's vended storage credentials expired and this read has no way \
                     to renew them"
                        .to_string(),
                ))
            };
        };
        match source.revend().await {
            Ok(fresh) => {
                if std::mem::discriminant(&fresh) != std::mem::discriminant(&*held) {
                    return Err(IcebergError::Credential(format!(
                        "the catalog renewed the table's {} with {}; refusing a credential of a \
                         different kind than the store was built for",
                        held.kind(),
                        fresh.kind()
                    )));
                }
                tracing::debug!(
                    kind = fresh.kind(),
                    valid_for_s = fresh.seconds_until_expiry(),
                    "vended storage credentials renewed"
                );
                *held = fresh;
                self.refused.store(false, Ordering::Release);
                Ok(held.clone())
            }
            Err(e) if !refused && still_valid(&held) => {
                tracing::warn!(error = %e, "vended credential renewal failed; reading on with the held credential");
                Ok(held.clone())
            }
            Err(e) => Err(e),
        }
    }

    /// The store refused the held credential (a 401): renew before the next
    /// request, and do not fall back to it if that fails.
    pub fn invalidate(&self) {
        self.refused.store(true, Ordering::Release);
    }

    fn renewal_due(&self, held: &VendedCredentials) -> bool {
        let Some(expires_at) = held.expires_at() else {
            // A credential the catalog gave no expiry (only S3 keys may lack
            // one) is used as given.
            return false;
        };
        // Up to a tenth of the margin (at most 5 s) of jitter, so many stores
        // renewing one table's credentials do not all ask at once.
        let spread = (self.margin.as_millis() / 10).clamp(1, 5_000) as u64;
        let jitter = std::time::Duration::from_millis(rand::thread_rng().gen_range(0..spread));
        let margin = chrono::Duration::from_std(self.margin + jitter)
            .unwrap_or_else(|_| chrono::Duration::seconds(60));
        Utc::now() + margin >= expires_at
    }
}

/// Whether `held` has not actually expired yet.
fn still_valid(held: &VendedCredentials) -> bool {
    held.expires_at()
        .is_none_or(|expires_at| Utc::now() < expires_at)
}

#[cfg(feature = "aws")]
#[async_trait]
impl crate::credential::SendCredentialResolver for RenewingVendedCredentials {
    async fn resolve(
        &self,
        _key: &crate::credential::CredentialCacheKey,
    ) -> Result<Option<VendedCredentials>> {
        self.current().await.map(Some)
    }

    fn invalidate(&self, _key: &crate::credential::CredentialCacheKey) {
        RenewingVendedCredentials::invalidate(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::GcsCredentials;
    use std::sync::atomic::AtomicUsize;

    fn token(name: &str, expires_in: chrono::Duration) -> VendedCredentials {
        VendedCredentials::Gcs(GcsCredentials {
            token: name.to_string(),
            expires_at: Utc::now() + expires_in,
            user_project: None,
        })
    }

    fn bearer(creds: &VendedCredentials) -> &str {
        match creds {
            VendedCredentials::Gcs(gcs) => &gcs.token,
            VendedCredentials::S3(_) => panic!("expected a GCS token"),
        }
    }

    #[derive(Debug, Default)]
    struct Catalog {
        calls: AtomicUsize,
        fail: AtomicBool,
    }

    #[async_trait]
    impl VendedCredentialSource for Catalog {
        async fn revend(&self) -> Result<VendedCredentials> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            // Slow enough that concurrent callers overlap the exchange.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if self.fail.load(Ordering::SeqCst) {
                return Err(IcebergError::Credential("catalog down".to_string()));
            }
            Ok(token(&format!("t{n}"), chrono::Duration::hours(1)))
        }
    }

    #[tokio::test]
    async fn a_credential_outside_the_margin_is_used_as_held() {
        let catalog = Arc::new(Catalog::default());
        let renewing = RenewingVendedCredentials::new(
            token("t0", chrono::Duration::hours(1)),
            Some(catalog.clone()),
        );
        assert_eq!(bearer(&renewing.current().await.unwrap()), "t0");
        assert_eq!(catalog.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn inside_the_margin_concurrent_readers_share_one_revend() {
        let catalog = Arc::new(Catalog::default());
        let renewing = Arc::new(RenewingVendedCredentials::new(
            token("t0", chrono::Duration::seconds(30)),
            Some(catalog.clone()),
        ));
        let readers: Vec<_> = (0..8)
            .map(|_| {
                let renewing = Arc::clone(&renewing);
                tokio::spawn(async move { renewing.current().await.unwrap() })
            })
            .collect();
        for reader in readers {
            assert_eq!(bearer(&reader.await.unwrap()), "t1");
        }
        assert_eq!(catalog.calls.load(Ordering::SeqCst), 1, "single-flight");
    }

    #[tokio::test]
    async fn a_failed_renewal_keeps_a_still_valid_credential_but_not_a_refused_one() {
        let catalog = Arc::new(Catalog::default());
        catalog.fail.store(true, Ordering::SeqCst);
        let renewing = RenewingVendedCredentials::new(
            token("t0", chrono::Duration::seconds(30)),
            Some(catalog.clone()),
        );
        assert_eq!(bearer(&renewing.current().await.unwrap()), "t0");
        renewing.invalidate();
        assert!(
            renewing.current().await.is_err(),
            "refused, so never reused"
        );
        catalog.fail.store(false, Ordering::SeqCst);
        assert_ne!(bearer(&renewing.current().await.unwrap()), "t0");
    }

    #[tokio::test]
    async fn an_expired_credential_with_no_way_to_renew_is_refused() {
        let renewing =
            RenewingVendedCredentials::new(token("t0", chrono::Duration::seconds(-1)), None);
        assert!(renewing.current().await.is_err());
        let fresh = RenewingVendedCredentials::new(token("t0", chrono::Duration::hours(1)), None);
        assert_eq!(bearer(&fresh.current().await.unwrap()), "t0");
    }

    #[tokio::test]
    async fn a_renewal_of_another_kind_is_refused() {
        #[derive(Debug)]
        struct S3Catalog;
        #[async_trait]
        impl VendedCredentialSource for S3Catalog {
            async fn revend(&self) -> Result<VendedCredentials> {
                Ok(VendedCredentials::S3(crate::credential::S3Credentials {
                    access_key_id: "AKIA".to_string(),
                    secret_access_key: "s".to_string(),
                    session_token: None,
                    expires_at: None,
                    endpoint: None,
                    region: None,
                    path_style: false,
                }))
            }
        }
        let renewing = RenewingVendedCredentials::new(
            token("t0", chrono::Duration::seconds(5)),
            Some(Arc::new(S3Catalog)),
        );
        // Still valid, so the mismatched renewal is reported, not read on.
        assert!(renewing.current().await.is_err());
    }

    #[test]
    fn debug_never_shows_the_credential() {
        let renewing =
            RenewingVendedCredentials::new(token("ya29.secret", chrono::Duration::hours(1)), None);
        assert!(!format!("{renewing:?}").contains("ya29.secret"));
    }
}
