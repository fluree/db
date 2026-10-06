//! SSRF hardening for outbound HTTP to catalog / OAuth2 / S3 endpoints.
//!
//! A request-supplied `catalog_uri` / `oauth2_token_url` / `s3_endpoint` reaches
//! an outbound HTTP client, and the onboarding routes are unauthenticated by
//! default — so an attacker could aim the server at an internal address
//! (`http://169.254.169.254/…` for cloud metadata, `http://10.0.0.1/…`, …).
//!
//! A URL-string allowlist alone is NOT enough: `reqwest` follows redirects by
//! default (a public URL can 3xx to an internal one) and re-resolves DNS at
//! connect time (a name can rebind public→private after a check). So the real
//! enforcement lives at the client/connect layer:
//!
//! - [`hardened_client_builder`] builds a `reqwest::Client` that follows **no**
//!   redirects and resolves through [`SsrfGuardResolver`], which denylists the
//!   resolved IPs and hands the connector only the checked addresses (blocking
//!   DNS-rebinding — there is no resolve-then-reconnect gap).
//! - [`validate_public_url`] is a cheap up-front check (scheme + literal IP) for
//!   the untrusted request boundary; it also covers decimal/octal/hex-encoded IP
//!   literals, which the URL parser normalizes and the resolver never sees.
//!
//! The S3 `endpoint` override legitimately targets loopback/private hosts
//! (MinIO / LocalStack), so it gets the narrower [`validate_s3_endpoint`], which
//! still refuses the cloud-metadata / link-local range.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use crate::error::{IcebergError, Result};

/// Whether an IP must be blocked as a non-public / internal target.
///
/// IPv4: loopback (127/8), private (RFC1918), link-local (169.254/16 — includes
/// the `169.254.169.254` cloud-metadata address), CGNAT shared space (100.64/10),
/// unspecified (0.0.0.0) and broadcast. IPv6: loopback (`::1`), unspecified
/// (`::`), unique-local (`fc00::/7`) and link-local (`fe80::/10`), plus
/// IPv4-mapped addresses unwrapped to their v4 form. (`Ipv6Addr::is_unique_local`
/// / `is_unicast_link_local` are still unstable, so those are checked by hand.)
pub fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_blocked(v4),
        IpAddr::V6(v6) => {
            // `::/96` IPv4-compatible (deprecated) embeds an IPv4 in the low 32
            // bits — treat it as that IPv4 so `::10.0.0.1` is blocked like
            // `10.0.0.1`. `::ffff:x` (mapped) is handled separately below.
            if is_ipv6_v4_compatible(v6) {
                let o = v6.octets();
                return ipv4_is_blocked(Ipv4Addr::new(o[12], o[13], o[14], o[15]));
            }
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ipv4_is_blocked(v4);
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || is_ipv6_unique_local(v6)
                || is_ipv6_link_local(v6)
        }
    }
}

fn ipv4_is_blocked(v4: Ipv4Addr) -> bool {
    v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || is_ipv4_shared(v4)
        || v4.octets()[0] == 0 // 0.0.0.0/8 "this network" (RFC 1122)
}

/// `::/96` IPv4-compatible IPv6 (deprecated) — the first 96 bits are zero and the
/// low 32 bits carry an IPv4 address (includes `::` and `::1`).
fn is_ipv6_v4_compatible(v6: Ipv6Addr) -> bool {
    v6.octets()[..12].iter().all(|&b| b == 0)
}

/// RFC 6598 shared address space (100.64.0.0/10, carrier-grade NAT).
fn is_ipv4_shared(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    o[0] == 100 && (o[1] & 0xc0) == 0x40
}

/// `fc00::/7` unique-local addresses.
fn is_ipv6_unique_local(v6: Ipv6Addr) -> bool {
    (v6.octets()[0] & 0xfe) == 0xfc
}

/// `fe80::/10` link-local unicast addresses.
fn is_ipv6_link_local(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

/// The cloud-metadata / link-local (+ unspecified/broadcast) range — the subset
/// blocked even for the S3 endpoint, which otherwise permits loopback/private.
fn ip_is_metadata_range(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local() || v4.is_unspecified() || v4.is_broadcast(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4.is_link_local() || v4.is_unspecified() || v4.is_broadcast();
            }
            v6.is_unspecified() || is_ipv6_link_local(v6)
        }
    }
}

/// A `reqwest` DNS resolver that denylists internal addresses at connect time.
///
/// It also pins: `reqwest` connects to exactly the addresses returned here, so
/// there is no resolve-then-reconnect (TOCTOU) window a rebinding DNS could use.
#[derive(Debug, Default)]
struct SsrfGuardResolver;

impl Resolve for SsrfGuardResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            // Port 0 — we only need the resolved IPs; the connector supplies the
            // real port.
            let resolved = match tokio::net::lookup_host((host.as_str(), 0)).await {
                Ok(it) => it,
                Err(e) => return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            };
            let allowed: Vec<SocketAddr> = resolved.filter(|sa| !ip_is_blocked(sa.ip())).collect();
            if allowed.is_empty() {
                let msg = format!(
                    "SSRF guard: host '{host}' resolves only to blocked internal addresses"
                );
                return Err(msg.into());
            }
            Ok(Box::new(allowed.into_iter()) as Addrs)
        })
    }
}

/// A `reqwest::ClientBuilder` hardened against SSRF: follows **no** redirects
/// (so it only ever connects to the caller-validated URL) and resolves through
/// [`SsrfGuardResolver`] (blocking names that map to internal addresses).
///
/// Callers add their own timeouts / TLS as before.
pub fn hardened_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(SsrfGuardResolver))
}

/// Up-front validation for a request-supplied outbound URL: an allowed scheme,
/// and a literal-IP host that is not internal. Hostnames pass here and are
/// enforced at connect time by [`hardened_client_builder`]'s resolver; literal
/// IPs (including decimal/octal/hex encodings the URL parser normalizes) are
/// blocked here because the resolver never sees them.
pub fn validate_public_url(raw: &str) -> Result<()> {
    let (scheme, host) = parse_scheme_host(raw)?;
    if scheme != "http" && scheme != "https" {
        return Err(IcebergError::Config(format!(
            "SSRF guard: URL scheme '{scheme}' is not allowed (use https or http)"
        )));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip_is_blocked(ip) {
            return Err(IcebergError::Config(format!(
                "SSRF guard: host '{host}' is a blocked \
                 (private/loopback/link-local/metadata) address"
            )));
        }
    }
    Ok(())
}

/// Validation for the S3 `endpoint` override. Narrower than [`validate_public_url`]
/// because MinIO / LocalStack legitimately run on loopback/private hosts — but the
/// cloud-metadata / link-local range (the credential-exfil target) is still refused.
///
/// RESIDUAL (tracked follow-up): the S3 sink uses the AWS SDK, not `reqwest`, so —
/// unlike the catalog / OAuth2 clients — there is no denylisting DNS resolver on
/// its connector. A DNS-name `s3_endpoint` that RESOLVES to a private address is
/// therefore only caught here when it is a literal metadata/link-local IP, not
/// when it is a hostname. This is lower-risk (s3_endpoint is data-plane and the
/// metadata IP is blocked). The full fix is to pin the AWS SDK's HTTP connector
/// to a checked IP — the equivalent of [`hardened_client_builder`]'s resolver —
/// which is deferred rather than done here.
pub fn validate_s3_endpoint(raw: &str) -> Result<()> {
    let (scheme, host) = parse_scheme_host(raw)?;
    if scheme != "http" && scheme != "https" {
        return Err(IcebergError::Config(format!(
            "SSRF guard: s3_endpoint scheme '{scheme}' is not allowed (use https or http)"
        )));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip_is_metadata_range(ip) {
            return Err(IcebergError::Config(format!(
                "SSRF guard: s3_endpoint host '{host}' is a blocked \
                 (link-local/metadata) address"
            )));
        }
    }
    Ok(())
}

/// Parse a URL into `(scheme, host)` (host without brackets for IPv6). Uses the
/// `reqwest`-re-exported `url` parser, which normalizes decimal/octal/hex IPv4
/// literals to dotted form so they compare as the IP they encode.
fn parse_scheme_host(raw: &str) -> Result<(String, String)> {
    let url = reqwest::Url::parse(raw)
        .map_err(|e| IcebergError::Config(format!("SSRF guard: invalid URL '{raw}': {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| IcebergError::Config(format!("SSRF guard: URL '{raw}' has no host")))?;
    // `host_str()` brackets IPv6 literals ("[::1]"); strip so it parses as an IP.
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
        .to_owned();
    Ok((url.scheme().to_owned(), host))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_internal_ipv4_including_encoded_and_cgnat() {
        for ip in [
            "169.254.169.254",
            "127.0.0.1",
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "100.64.0.1",
            "0.0.0.0",
            "255.255.255.255",
        ] {
            assert!(ip_is_blocked(ip.parse().unwrap()), "{ip} must be blocked");
        }
    }

    #[test]
    fn allows_public_ipv4() {
        for ip in ["8.8.8.8", "1.1.1.1", "93.184.216.34"] {
            assert!(!ip_is_blocked(ip.parse().unwrap()), "{ip} must be allowed");
        }
    }

    #[test]
    fn blocks_internal_ipv6_including_mapped_and_v4_compatible() {
        for ip in [
            "::1",
            "::",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            // ::/96 IPv4-compatible embeds an IPv4 in the low 32 bits.
            "::0.0.0.1",
            "::10.0.0.1",
            "::169.254.169.254",
        ] {
            assert!(ip_is_blocked(ip.parse().unwrap()), "{ip} must be blocked");
        }
        assert!(!ip_is_blocked("2606:4700:4700::1111".parse().unwrap()));
    }

    #[test]
    fn blocks_zero_network_ipv4() {
        // 0.0.0.0/8 "this network" — not just the unspecified 0.0.0.0.
        assert!(ip_is_blocked("0.0.0.0".parse().unwrap()));
        assert!(ip_is_blocked("0.1.2.3".parse().unwrap()));
        assert!(ip_is_blocked("0.255.255.255".parse().unwrap()));
    }

    #[tokio::test]
    async fn hardened_client_does_not_follow_redirects() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redirect"))
            .respond_with(ResponseTemplate::new(302).insert_header("Location", "/landed"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/landed"))
            .respond_with(ResponseTemplate::new(200).set_body_string("LANDED"))
            .mount(&server)
            .await;

        let client = hardened_client_builder().build().unwrap();
        let resp = client
            .get(format!("{}/redirect", server.uri()))
            .send()
            .await
            .unwrap();
        // Policy::none() → the 302 is returned as-is, never followed to /landed
        // (which is how a public catalog could bounce to an internal address).
        assert_eq!(resp.status().as_u16(), 302, "redirect must not be followed");
        assert_ne!(
            resp.text().await.unwrap(),
            "LANDED",
            "redirect target must not be reached"
        );
    }

    #[test]
    fn validate_public_url_blocks_literals_and_bad_schemes() {
        for url in [
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.1/",
            "https://192.168.0.5/catalog",
            "http://[::1]:8080/",
            "http://127.0.0.1:9000/",
            // decimal / hex / octal encodings normalize to 127.0.0.1
            "http://2130706433/",
            "http://0x7f000001/",
            "http://0177.0.0.1/",
            // non-http schemes
            "file:///etc/passwd",
            "ftp://8.8.8.8/",
            "not a url",
        ] {
            assert!(validate_public_url(url).is_err(), "{url} must be rejected");
        }
    }

    #[test]
    fn validate_public_url_allows_public_literal_and_domains() {
        // Public literal IP and a bare domain (enforced later by the resolver).
        assert!(validate_public_url("https://8.8.8.8/v1/config").is_ok());
        assert!(validate_public_url("https://catalog.example.com/v1").is_ok());
    }

    #[test]
    fn validate_s3_endpoint_allows_minio_but_blocks_metadata() {
        // MinIO / LocalStack dev endpoints are permitted.
        assert!(validate_s3_endpoint("http://127.0.0.1:9000").is_ok());
        assert!(validate_s3_endpoint("http://10.0.0.5:9000").is_ok());
        assert!(validate_s3_endpoint("http://minio.internal:9000").is_ok());
        // The cloud-metadata / link-local range is still refused.
        assert!(validate_s3_endpoint("http://169.254.169.254/").is_err());
        assert!(validate_s3_endpoint("http://0.0.0.0:9000").is_err());
        // Bad scheme rejected.
        assert!(validate_s3_endpoint("ftp://minio.internal:9000").is_err());
    }
}
