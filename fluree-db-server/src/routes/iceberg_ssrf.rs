//! SSRF guard applied at the Iceberg onboarding route boundary.
//!
//! A cheap up-front reject of request-supplied URLs that target internal hosts,
//! before any client work. It delegates the scheme/IP policy to the api
//! (`fluree_db_api::guard_iceberg_connection_urls`), where the AUTHORITATIVE
//! enforcement also lives: the catalog / OAuth2 HTTP clients are built with a
//! hardened, redirect-refusing, IP-denylisting resolver, so a redirect or
//! DNS-rebind to an internal address is blocked at connect time — which a
//! boundary string check alone cannot cover. `catalog_uri` / `oauth2_token_url`
//! get the full internal denylist; `s3_endpoint` gets the narrower metadata-only
//! block (MinIO / LocalStack legitimately use loopback/private hosts).

use crate::error::{Result, ServerError};

/// Guard the request-supplied outbound URLs on an Iceberg connection. Returns a
/// `400` (via [`ServerError::Api`]) when a URL targets a blocked host.
pub fn guard_connection_urls(
    catalog_uri: Option<&str>,
    oauth2_token_url: Option<&str>,
    s3_endpoint: Option<&str>,
) -> Result<()> {
    fluree_db_api::guard_iceberg_connection_urls(catalog_uri, oauth2_token_url, s3_endpoint)
        .map_err(ServerError::Api)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_metadata_and_private_catalog_and_token_uris() {
        assert!(guard_connection_urls(Some("http://169.254.169.254/"), None, None).is_err());
        assert!(guard_connection_urls(Some("http://10.0.0.1/"), None, None).is_err());
        assert!(guard_connection_urls(None, Some("http://127.0.0.1:9000/token"), None).is_err());
    }

    #[test]
    fn rejects_non_http_schemes() {
        assert!(guard_connection_urls(Some("file:///etc/passwd"), None, None).is_err());
    }

    #[test]
    fn allows_public_catalog_with_minio_s3_endpoint() {
        // A public catalog plus a MinIO-style loopback s3_endpoint (permitted).
        assert!(guard_connection_urls(
            Some("https://catalog.example.com"),
            None,
            Some("http://127.0.0.1:9000"),
        )
        .is_ok());
    }

    #[test]
    fn s3_endpoint_metadata_address_still_blocked() {
        assert!(guard_connection_urls(None, None, Some("http://169.254.169.254/")).is_err());
    }
}
