//! HTTP request-forwarding helpers.

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

#[cfg(test)]
mod tests {
    use super::*;

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
