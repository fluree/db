//! HTTP request-forwarding helpers.

use http::uri::{PathAndQuery, Uri};

/// Hop-by-hop headers: the RFC 2616 §13.5.1 set — the spec that
/// coined the vocabulary and fixed this list; its "Trailers" entry
/// misspelled the `Trailer` field, hence both spellings here — plus
/// the legacy `proxy-connection`. These fields describe a single
/// connection rather than the end-to-end exchange, so a forwarder
/// strips them from relayed requests and responses. Successor specs
/// (RFC 7230 §6.1) replaced the fixed list with Connection
/// nomination — fields named in the `Connection` header are also
/// connection-specific, and this list does not cover those. `host`
/// is deliberately absent: it isn't hop-by-hop, it's a rewrite
/// concern for the forwarder's outbound client.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// Whether `name` is a hop-by-hop header. ASCII case-insensitive,
/// allocation-free.
pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|header| header.eq_ignore_ascii_case(name))
}

/// The URL a forwarder sends a request to on `base_url`: the path and
/// query the client sent. A nested router strips its prefix from the
/// request's `uri` (`/v1/fluree/create` becomes `/create`) and keeps the
/// full one as axum's `OriginalUri`, which `original` is when the request
/// carries it.
pub fn outbound_url(base_url: &str, original: Option<&Uri>, uri: &Uri) -> String {
    let path_and_query = original
        .and_then(Uri::path_and_query)
        .or_else(|| uri.path_and_query())
        .map_or("/", PathAndQuery::as_str);
    format!("{}{path_and_query}", base_url.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_url_keeps_the_path_the_client_sent() {
        let original: Uri = "/v1/fluree/insert?ledger=a".parse().unwrap();
        let stripped: Uri = "/insert?ledger=a".parse().unwrap();
        for base in ["http://tx:8090", "http://tx:8090/"] {
            assert_eq!(
                outbound_url(base, Some(&original), &stripped),
                "http://tx:8090/v1/fluree/insert?ledger=a",
                "{base}"
            );
            assert_eq!(
                outbound_url(base, None, &stripped),
                "http://tx:8090/insert?ledger=a",
                "{base}"
            );
        }
    }

    #[test]
    fn classifies_hop_by_hop_case_insensitively() {
        for header in HOP_BY_HOP_HEADERS {
            assert!(is_hop_by_hop(header), "{header} must classify");
            assert!(
                is_hop_by_hop(&header.to_ascii_uppercase()),
                "{header} must classify in upper case"
            );
        }
        assert!(is_hop_by_hop("Connection"));
        assert!(is_hop_by_hop("Proxy-Authorization"));
    }

    #[test]
    fn end_to_end_headers_pass_through() {
        for header in ["authorization", "content-type", "accept", "host"] {
            assert!(!is_hop_by_hop(header), "{header} must pass through");
        }
    }
}
