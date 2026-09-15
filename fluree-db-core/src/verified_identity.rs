//! An identity the auth layer has verified.
//!
//! `f:overrideControl` with `f:IdentityRestricted` gates config overrides on
//! *who the caller provably is*, which is a different thing from the policy
//! identity a request may name in `opts.identity` or a `fluree-identity`
//! header. Those are policy evaluation context and may legitimately be
//! caller-supplied (a credential may let its holder select one); they must
//! never satisfy an allow-list. Keeping the verified identity in its own type makes
//! the difference visible at every boundary: a bare `String` cannot be passed
//! where a [`VerifiedIdentity`] is expected, so the only way one comes into
//! existence is an explicit [`VerifiedIdentity::new`], and those call sites
//! are the auth layer.

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// A canonical identity string (a DID, typically) established by an auth
/// layer: a verified JWS credential, a verified bearer token, an authenticated
/// MCP principal, or an embedding application that verifies identities
/// itself and is therefore the auth layer for its deployment.
///
/// Construct one only at such a boundary. Never build one from a request
/// body, a header, or query options: that is what [`VerifiedIdentity::new`]
/// being the sole constructor in code is meant to make conspicuous in review.
///
/// `Serialize` / `Deserialize` exist so the value can ride the consensus
/// request envelope from the accepting node to the commit worker. They are
/// not a client-facing decoder, and no request parser produces this type.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VerifiedIdentity(String);

impl VerifiedIdentity {
    /// Wrap an identity the caller has verified. See the type docs for what
    /// counts as verified; this is deliberately the only constructor in code
    /// (serde deserialization exists for the consensus envelope alone).
    pub fn new(identity: impl Into<String>) -> Self {
        Self(identity.into())
    }

    /// The identity string, for allow-list comparison and display.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unwrap into the plain string, for example to seed a policy identity
    /// from the verified one.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Deref for VerifiedIdentity {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for VerifiedIdentity {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for VerifiedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VerifiedIdentity({:?})", self.0)
    }
}

impl fmt::Display for VerifiedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::VerifiedIdentity;

    #[test]
    fn round_trips_through_serde_transparently() {
        let id = VerifiedIdentity::new("did:key:admin");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"did:key:admin\"");
        let back: VerifiedIdentity = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn derefs_to_the_identity_string() {
        let id = VerifiedIdentity::new("did:key:admin");
        assert_eq!(&*id, "did:key:admin");
        assert_eq!(id.as_str(), "did:key:admin");
        assert_eq!(id.to_string(), "did:key:admin");
        assert_eq!(Some(id).as_deref(), Some("did:key:admin"));
    }
}
