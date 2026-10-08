//! Outbound HTTP hardening for the SQL endpoint.
//!
//! A SQL endpoint legitimately lives on loopback or a private network — a
//! `fluree-sql-bridge` or Trino sidecar next to the server is the primary
//! deployment shape — so the Iceberg catalog's "public addresses only" posture
//! would block the main use case. This mirrors the narrower S3 `endpoint`
//! policy instead: redirects are never followed, and the link-local /
//! cloud-metadata range (`169.254/16`, `fe80::/10`) is refused both up front
//! (literal IPs) and at connect time (names that resolve there).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use crate::error::{Result, SqlError};

fn ipv4_is_link_local_or_invalid(v4: Ipv4Addr) -> bool {
    v4.is_link_local() || v4.is_unspecified() || v4.is_broadcast()
}

fn ipv6_is_link_local(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

/// Whether an IP is in the range no SQL endpoint may ever be: link-local
/// (which contains the cloud-metadata address) or unspecified/broadcast.
pub fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_link_local_or_invalid(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ipv4_is_link_local_or_invalid(v4);
            }
            v6.is_unspecified() || ipv6_is_link_local(v6)
        }
    }
}

#[derive(Debug, Default)]
struct LinkLocalGuardResolver;

impl Resolve for LinkLocalGuardResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let resolved = match tokio::net::lookup_host((host.as_str(), 0)).await {
                Ok(it) => it,
                Err(e) => return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            };
            let allowed: Vec<SocketAddr> = resolved.filter(|sa| !ip_is_blocked(sa.ip())).collect();
            if allowed.is_empty() {
                return Err(format!(
                    "SSRF guard: host '{host}' resolves only to link-local/metadata addresses"
                )
                .into());
            }
            Ok(Box::new(allowed.into_iter()) as Addrs)
        })
    }
}

/// A client that follows no redirects and refuses link-local targets.
pub fn build_client(request_timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(LinkLocalGuardResolver))
        .connect_timeout(Duration::from_secs(30))
        .timeout(request_timeout)
        .build()
        .map_err(|e| SqlError::Http(format!("build HTTP client: {e}")))
}

/// Up-front validation of a configured endpoint: `http`/`https` only, a host
/// present, and not a literal link-local IP (the resolver never sees literals).
pub fn validate_endpoint(raw: &str) -> Result<()> {
    let url = reqwest::Url::parse(raw)
        .map_err(|e| SqlError::Config(format!("endpoint '{raw}' is not a valid URL: {e}")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(SqlError::Config(format!(
                "endpoint scheme '{other}' is not allowed (use https or http)"
            )))
        }
    }
    let host = url
        .host_str()
        .ok_or_else(|| SqlError::Config(format!("endpoint '{raw}' has no host")))?;
    let literal = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = literal.parse::<IpAddr>() {
        if ip_is_blocked(ip) {
            return Err(SqlError::Config(format!(
                "SSRF guard: endpoint host '{host}' is a blocked (link-local/metadata) address"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_and_private_are_allowed_but_metadata_is_not() {
        validate_endpoint("http://localhost:8080").unwrap();
        validate_endpoint("http://127.0.0.1:8080").unwrap();
        validate_endpoint("http://10.1.2.3:8080/").unwrap();
        validate_endpoint("https://trino.example.com").unwrap();
        assert!(validate_endpoint("http://169.254.169.254/latest").is_err());
        assert!(validate_endpoint("http://[fe80::1]:8080").is_err());
        assert!(validate_endpoint("http://0.0.0.0:8080").is_err());
        assert!(validate_endpoint("file:///etc/passwd").is_err());
        assert!(validate_endpoint("not a url").is_err());
    }
}
