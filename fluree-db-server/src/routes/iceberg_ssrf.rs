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

/// Guard the outbound URLs a parsed Iceberg connection names: the REST
/// catalog's URI, its OAuth2 token URL, and the S3 endpoint override. It runs
/// on the parsed config rather than on request fields, so a structured
/// `catalog.uri` is guarded exactly as a flat `catalog_uri` is.
pub fn guard_connection(conn: &fluree_db_api::IcebergConnectionConfig) -> Result<()> {
    let (catalog_uri, auth) = match &conn.catalog_mode {
        fluree_db_api::CatalogMode::Rest(rest) => {
            (Some(rest.catalog_uri.as_str()), Some(&rest.auth))
        }
        _ => (None, None),
    };
    guard_connection_urls(
        catalog_uri,
        auth.and_then(token_url),
        conn.io.s3_endpoint.as_deref(),
    )
}

/// The URL an auth config sends its client credentials to, if it names one.
/// Google's token endpoints are fixed by the identity, never by the request.
pub fn token_url(auth: &fluree_db_api::IcebergAuthConfig) -> Option<&str> {
    match auth {
        fluree_db_api::IcebergAuthConfig::OAuth2ClientCredentials { token_url, .. } => {
            Some(token_url)
        }
        _ => None,
    }
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
    fn a_parsed_connection_is_guarded_whichever_form_it_came_in() {
        let rules = fluree_db_api::SecretRules::local(false);
        for body in [
            serde_json::json!({"catalog_uri": "http://169.254.169.254/"}),
            serde_json::json!({"catalog": {"type": "rest", "uri": "http://169.254.169.254/"}}),
            serde_json::json!({"catalog": {"type": "rest", "uri": "https://c.example.com",
                "auth": {"type": "oauth2_client_credentials", "token_url": "http://10.0.0.1/t",
                    "client_id": "", "client_secret": "s"}}}),
            serde_json::json!({"catalog": {"type": "direct", "table_location": "s3://b/w/n/t"},
                "io": {"s3_endpoint": "http://169.254.169.254/"}}),
        ] {
            let conn = fluree_db_api::iceberg_connection_from_json(
                body.as_object().unwrap().clone(),
                &rules,
            )
            .unwrap();
            assert!(guard_connection(&conn).is_err(), "{body}");
        }
    }

    #[test]
    fn s3_endpoint_metadata_address_still_blocked() {
        assert!(guard_connection_urls(None, None, Some("http://169.254.169.254/")).is_err());
    }
}
