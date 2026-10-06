//! Connection handshake: magic preamble + 4-slot version negotiation.
//!
//! The client sends 20 bytes: the magic `0x6060B017` followed by four 4-byte
//! version proposals in preference order. Each proposal is big-endian
//! `[reserved, range, minor, major]`, where `range` says "and the `range`
//! consecutive minors below `minor`". The server answers with the 4-byte
//! version it picked, or all zeros to reject (then closes).

/// Total handshake request size (magic + 4 proposals).
pub const HANDSHAKE_LEN: usize = 20;

pub const MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// A negotiated protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BoltVersion {
    pub major: u8,
    pub minor: u8,
}

impl BoltVersion {
    pub const V4_4: BoltVersion = BoltVersion { major: 4, minor: 4 };
    pub const V5_4: BoltVersion = BoltVersion { major: 5, minor: 4 };

    /// The minor-version window this server implements per major, or `None`
    /// for unsupported majors.
    ///
    /// 5.0–5.4 for current official drivers (element ids from 5.0, `LOGON`
    /// from 5.1 — both handled per negotiated version; the rest of the 5.x
    /// deltas are opt-in features we never advertise). 4.4 for the
    /// long-lived LTS drivers.
    fn supported_minors(major: u8) -> Option<(u8, u8)> {
        match major {
            5 => Some((0, 4)),
            4 => Some((4, 4)),
            _ => None,
        }
    }

    pub fn to_bytes(self) -> [u8; 4] {
        [0, 0, self.minor, self.major]
    }

    /// Whether HELLO carries auth (< 5.1) or a separate LOGON follows (>= 5.1).
    pub fn uses_logon(self) -> bool {
        self >= BoltVersion { major: 5, minor: 1 }
    }

    /// Whether graph structures carry `element_id` fields (5.0+).
    pub fn has_element_ids(self) -> bool {
        self.major >= 5
    }
}

impl std::fmt::Display for BoltVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Outcome of inspecting a 20-byte handshake request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeOutcome {
    /// Reply with `version.to_bytes()` and proceed at that version.
    Accept(BoltVersion),
    /// No overlap: reply `[0, 0, 0, 0]` and close.
    NoVersionOverlap,
    /// First four bytes were not the Bolt magic (e.g. an HTTP request hit
    /// the Bolt port): close without replying.
    BadMagic,
}

/// Negotiate against a client handshake. `request` must be exactly
/// [`HANDSHAKE_LEN`] bytes.
pub fn negotiate(request: &[u8; HANDSHAKE_LEN]) -> HandshakeOutcome {
    if request[0..4] != MAGIC {
        return HandshakeOutcome::BadMagic;
    }
    // Each proposal covers `[minor - range, minor]` of one major. Take the
    // first slot (client preference order) that intersects our window,
    // answering with the highest minor in the intersection.
    for slot in request[4..].chunks_exact(4) {
        let (range, minor, major) = (slot[1], slot[2], slot[3]);
        let Some((min_supported, max_supported)) = BoltVersion::supported_minors(major) else {
            continue;
        };
        let low = minor.saturating_sub(range).max(min_supported);
        let high = minor.min(max_supported);
        if low <= high {
            return HandshakeOutcome::Accept(BoltVersion { major, minor: high });
        }
    }
    HandshakeOutcome::NoVersionOverlap
}

/// The 4-byte rejection reply.
pub const REJECT: [u8; 4] = [0, 0, 0, 0];

#[cfg(test)]
mod tests {
    use super::*;

    fn request(proposals: [[u8; 4]; 4]) -> [u8; HANDSHAKE_LEN] {
        let mut req = [0u8; HANDSHAKE_LEN];
        req[0..4].copy_from_slice(&MAGIC);
        for (i, p) in proposals.iter().enumerate() {
            req[4 + i * 4..8 + i * 4].copy_from_slice(p);
        }
        req
    }

    #[test]
    fn negotiates_modern_driver_proposal() {
        // Shape of a neo4j 5.x python driver handshake: 5.4 back to 5.0,
        // then 5.0, 4.4 back to 4.1, 3.0.
        let req = request([[0, 4, 4, 5], [0, 0, 0, 5], [0, 3, 4, 4], [0, 0, 0, 3]]);
        assert_eq!(negotiate(&req), HandshakeOutcome::Accept(BoltVersion::V5_4));
    }

    #[test]
    fn negotiates_44_only_driver() {
        let req = request([[0, 3, 4, 4], [0, 0, 0, 4], [0, 0, 0, 3], [0, 0, 0, 0]]);
        assert_eq!(negotiate(&req), HandshakeOutcome::Accept(BoltVersion::V4_4));
    }

    #[test]
    fn range_expansion_matches_lower_minor() {
        // Client proposes 5.6 back to 5.2; we support 5.4 -> accept 5.4.
        let req = request([[0, 4, 6, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);
        assert_eq!(negotiate(&req), HandshakeOutcome::Accept(BoltVersion::V5_4));
    }

    #[test]
    fn no_overlap_rejects() {
        let req = request([[0, 0, 0, 3], [0, 0, 0, 2], [0, 0, 0, 1], [0, 0, 0, 0]]);
        assert_eq!(negotiate(&req), HandshakeOutcome::NoVersionOverlap);
    }

    #[test]
    fn bad_magic_detected() {
        let mut req = request([[0, 0, 0, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);
        req[0] = b'G'; // "GET ..." — an HTTP request on the Bolt port
        assert_eq!(negotiate(&req), HandshakeOutcome::BadMagic);
    }

    #[test]
    fn client_slot_preference_wins() {
        // Client prefers 4.4 (slot 0) over 5.4 (slot 1) — unusual but legal.
        let req = request([[0, 0, 4, 4], [0, 0, 4, 5], [0, 0, 0, 0], [0, 0, 0, 0]]);
        assert_eq!(negotiate(&req), HandshakeOutcome::Accept(BoltVersion::V4_4));
    }

    #[test]
    fn exact_older_five_minor_accepted() {
        // A 5.1-only client must get 5.1 back, not a rejection.
        let req = request([[0, 0, 1, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);
        assert_eq!(
            negotiate(&req),
            HandshakeOutcome::Accept(BoltVersion { major: 5, minor: 1 })
        );
    }

    #[test]
    fn future_five_minor_capped_at_supported() {
        // Client proposes 5.7 back to 5.0; we answer 5.4.
        let req = request([[0, 7, 7, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);
        assert_eq!(negotiate(&req), HandshakeOutcome::Accept(BoltVersion::V5_4));
    }
}
