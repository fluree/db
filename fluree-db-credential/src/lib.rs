//! Credential verification for Fluree DB
//!
//! This crate provides verification of signed queries and transactions using:
//! - JWS (JSON Web Signature) compact format
//! - VerifiableCredential format (feature-gated, requires JSON-LD canonization)
//!
//! # JWS Format
//!
//! Compact serialization: `header.payload.signature`
//! - Header must contain `{"alg":"EdDSA","jwk":{...}}` with embedded Ed25519 public key
//! - Payload is base64url-encoded (JSON query/txn or raw SPARQL string)
//! - Signature is Ed25519 over `header.payload`
//!
//! # VerifiableCredential Format (requires "vc" feature)
//!
//! W3C VC with detached JWS proof. Requires JSON-LD URDNA2015 canonization
//! which is not yet implemented.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_credential::{verify, CredentialInput};
//!
//! // Verify a JWS string
//! let jws = "eyJhbGciOiJFZERTQSIsImp3ayI6ey4uLn19.eyJzZWxlY3QiOlsiP3MiXX0.c2ln...";
//! let result = verify(CredentialInput::Jws(jws))?;
//! println!("Identity: {}", result.did);
//! println!("Query: {}", result.subject);
//! ```

mod did;
mod ed25519;
pub mod error;
mod jws;
pub mod jwt_claims;

#[cfg(feature = "oidc")]
pub mod oidc_jwt;

pub use did::{did_from_pubkey, pubkey_from_did};
pub use ed25519::{sign_ed25519, SigningKey};
pub use error::{CredentialError, Result};
pub use jws::{verify_jws, JwsVerified};
pub use jwt_claims::{ClaimsError, EventsTokenPayload};

#[cfg(feature = "oidc")]
pub use oidc_jwt::{decode_unverified_issuer, peek_jwt_header, verify_jwt, JwtVerified};

use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

/// Domain separator for commit signature digests.
const COMMIT_DOMAIN_SEPARATOR: &[u8] = b"fluree/commit/v1";

/// Compute the domain-separated digest for commit signing/verification.
///
/// ```text
/// to_sign = SHA-256("fluree/commit/v1" || varint(ledger_id.len()) || ledger_id || commit_hash)
/// ```
///
/// The domain separator prevents cross-protocol replay. The ledger ID
/// prevents cross-ledger replay. The commit hash binds the signature to
/// the specific commit content.
fn compute_commit_digest(commit_hash: &[u8; 32], ledger_id: &str) -> [u8; 32] {
    let ledger_id_bytes = ledger_id.as_bytes();
    let mut hasher = Sha256::new();
    hasher.update(COMMIT_DOMAIN_SEPARATOR);
    // Length-prefix the ledger ID (varint-style: single byte for len < 128)
    let ledger_id_len = ledger_id_bytes.len();
    let mut len_buf = [0u8; 10];
    let len_bytes = encode_varint_to_buf(ledger_id_len as u64, &mut len_buf);
    hasher.update(len_bytes);
    hasher.update(ledger_id_bytes);
    hasher.update(commit_hash);
    hasher.finalize().into()
}

/// Encode a u64 as a varint into a fixed buffer, returning the used slice.
fn encode_varint_to_buf(mut value: u64, buf: &mut [u8; 10]) -> &[u8] {
    let mut i = 0;
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf[i] = byte;
            i += 1;
            break;
        }
        buf[i] = byte | 0x80;
        i += 1;
    }
    &buf[..i]
}

/// Sign a commit hash with domain separation and ledger binding.
///
/// Computes the domain-separated digest and signs it with Ed25519.
///
/// # Arguments
/// * `signing_key` - Ed25519 signing key
/// * `commit_hash` - 32-byte SHA-256 hash of the commit blob content
/// * `ledger_id` - Canonical ledger ID (must be immutable for verification)
///
/// # Returns
/// 64-byte Ed25519 signature
pub fn sign_commit_digest(
    signing_key: &SigningKey,
    commit_hash: &[u8; 32],
    ledger_id: &str,
) -> [u8; 64] {
    let digest = compute_commit_digest(commit_hash, ledger_id);
    sign_ed25519(signing_key, &digest)
}

/// Verify a commit signature against a commit hash and ledger ID.
///
/// Recomputes the domain-separated digest and verifies the Ed25519 signature.
///
/// # Arguments
/// * `signer_did` - Signer's did:key identifier (used to derive public key)
/// * `signature` - 64-byte Ed25519 signature
/// * `commit_hash` - 32-byte SHA-256 hash of the commit blob content
/// * `ledger_id` - Canonical ledger ID
///
/// # Errors
/// - `InvalidDid` if the DID format is invalid
/// - `InvalidPublicKey` if the public key can't be extracted
/// - `InvalidSignature` if the signature doesn't verify
pub fn verify_commit_digest(
    signer_did: &str,
    signature: &[u8; 64],
    commit_hash: &[u8; 32],
    ledger_id: &str,
) -> Result<()> {
    let pubkey = pubkey_from_did(signer_did)?;
    let digest = compute_commit_digest(commit_hash, ledger_id);
    ed25519::verify_ed25519(&pubkey, &digest, signature)
}

/// Input type for verify() - accepts either JWS string or JSON
#[derive(Debug, Clone)]
pub enum CredentialInput<'a> {
    /// Compact JWS string (header.payload.signature)
    Jws(&'a str),
    /// JSON object (VerifiableCredential or JSON-wrapped JWS)
    Json(&'a JsonValue),
}

/// Unified result for credential verification
#[derive(Debug, Clone)]
pub struct VerifiedCredential {
    /// Decoded subject (JSON-parsed payload for JWS, credentialSubject for VC)
    pub subject: JsonValue,
    /// Signing identity (did:key:z6Mk...)
    pub did: String,
    /// Envelope @context (only for VerifiableCredentials)
    pub parent_context: Option<JsonValue>,
}

/// Verify a credential and extract subject + identity
///
/// # Input Types
/// - `CredentialInput::Jws(str)`: Verifies JWS, JSON-parses payload into subject
/// - `CredentialInput::Json(value)`: If string value, treats as JWS; otherwise VC (requires "vc" feature)
///
/// # Returns
/// `VerifiedCredential` with subject JSON, DID identity, and optional parent context
///
/// # Errors
/// - Various `CredentialError` variants for format/signature/identity issues
/// - `VcNotEnabled` if JSON input is a VC object but "vc" feature is not enabled
pub fn verify(input: CredentialInput<'_>) -> Result<VerifiedCredential> {
    match input {
        CredentialInput::Jws(jws) => verify_jws_to_credential(jws),
        CredentialInput::Json(value) => {
            // Check if it's a JSON string (wrapped JWS)
            if let Some(jws) = value.as_str() {
                return verify_jws_to_credential(jws);
            }

            // Otherwise it's a VC object
            verify_vc_object(value)
        }
    }
}

/// Verify JWS and convert to VerifiedCredential (JSON-parsing payload)
fn verify_jws_to_credential(jws: &str) -> Result<VerifiedCredential> {
    let jws_result = verify_jws(jws)?;

    // JSON-parse the payload
    let subject: JsonValue = serde_json::from_str(&jws_result.payload)?;

    Ok(VerifiedCredential {
        subject,
        did: jws_result.did,
        parent_context: None,
    })
}

/// Verify a VerifiableCredential JSON object
#[allow(unused_variables)]
fn verify_vc_object(credential: &JsonValue) -> Result<VerifiedCredential> {
    #[cfg(feature = "vc")]
    {
        // VC verification requires JSON-LD URDNA2015 canonization
        // This is NOT implementable with the current Rust json-ld crate
        // which only provides RFC 8785 JSON canonicalization.
        //
        // For now, return an error indicating VC support needs additional work.
        Err(CredentialError::VcNotEnabled)
    }

    #[cfg(not(feature = "vc"))]
    {
        Err(CredentialError::VcNotEnabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::{Signer, SigningKey};

    fn create_test_jws(payload: &str, signing_key: &SigningKey) -> String {
        let pubkey = signing_key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

        let header = serde_json::json!({
            "alg": "EdDSA",
            "jwk": {
                "kty": "OKP",
                "crv": "Ed25519",
                "x": pubkey_b64
            }
        });

        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{header_b64}.{payload_b64}.{sig_b64}")
    }

    #[test]
    fn test_verify_jws_input() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"select":["?s"],"where":{"@id":"?s"}}"#;

        let jws = create_test_jws(payload, &signing_key);
        let result = verify(CredentialInput::Jws(&jws)).unwrap();

        assert!(result.did.starts_with("did:key:z"));
        assert_eq!(
            result.subject.get("select").unwrap(),
            &serde_json::json!(["?s"])
        );
        assert!(result.parent_context.is_none());
    }

    #[test]
    fn test_verify_json_string_input() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"from":"ledger:test","select":["?s"]}"#;

        let jws = create_test_jws(payload, &signing_key);
        let json_value = JsonValue::String(jws);

        let result = verify(CredentialInput::Json(&json_value)).unwrap();

        assert!(result.did.starts_with("did:key:z"));
        assert_eq!(result.subject.get("from").unwrap(), "ledger:test");
    }

    #[test]
    fn test_verify_json_object_without_vc_feature() {
        let vc = serde_json::json!({
            "@context": "https://www.w3.org/2018/credentials/v1",
            "type": ["VerifiableCredential"],
            "credentialSubject": {"select": ["?s"]},
            "proof": {"jws": "..."}
        });

        let result = verify(CredentialInput::Json(&vc));
        assert!(matches!(result, Err(CredentialError::VcNotEnabled)));
    }

    #[test]
    fn test_verify_jws_invalid_json_payload() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);

        // Create JWS with non-JSON payload
        let jws = create_test_jws("not json {{", &signing_key);

        let result = verify(CredentialInput::Jws(&jws));
        assert!(matches!(result, Err(CredentialError::JsonParse(_))));
    }

    #[test]
    fn test_sign_and_verify_commit_digest() {
        let key = SigningKey::from_bytes(&[99u8; 32]);
        let commit_hash = [0xABu8; 32];
        let alias = "books:main";

        let signature = sign_commit_digest(&key, &commit_hash, alias);
        let did = did_from_pubkey(&key.verifying_key().to_bytes());

        assert!(verify_commit_digest(&did, &signature, &commit_hash, alias).is_ok());
    }

    #[test]
    fn test_verify_commit_digest_wrong_alias() {
        let key = SigningKey::from_bytes(&[99u8; 32]);
        let commit_hash = [0xABu8; 32];

        let signature = sign_commit_digest(&key, &commit_hash, "books:main");
        let did = did_from_pubkey(&key.verifying_key().to_bytes());

        // Wrong alias should fail verification
        let result = verify_commit_digest(&did, &signature, &commit_hash, "users:main");
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_verify_commit_digest_wrong_hash() {
        let key = SigningKey::from_bytes(&[99u8; 32]);
        let commit_hash = [0xABu8; 32];

        let signature = sign_commit_digest(&key, &commit_hash, "books:main");
        let did = did_from_pubkey(&key.verifying_key().to_bytes());

        let wrong_hash = [0xCDu8; 32];
        let result = verify_commit_digest(&did, &signature, &wrong_hash, "books:main");
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_verify_commit_digest_wrong_signer() {
        let key = SigningKey::from_bytes(&[99u8; 32]);
        let other_key = SigningKey::from_bytes(&[88u8; 32]);
        let commit_hash = [0xABu8; 32];

        let signature = sign_commit_digest(&key, &commit_hash, "books:main");
        let wrong_did = did_from_pubkey(&other_key.verifying_key().to_bytes());

        let result = verify_commit_digest(&wrong_did, &signature, &commit_hash, "books:main");
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_did_derivation_matches_test_vector() {
        // Test vectors from fluree.crypto cross_platform_test.cljc
        let test_pubkey: [u8; 32] = [
            0xa8, 0xde, 0xf1, 0x2a, 0xd7, 0x36, 0xf8, 0x84, 0x0f, 0x83, 0x6a, 0x46, 0xc6, 0x6c,
            0x9f, 0x3e, 0x20, 0x15, 0xd1, 0xea, 0x2c, 0x69, 0xd5, 0x46, 0xc0, 0x50, 0xfe, 0xf7,
            0x46, 0xbd, 0x63, 0xb3,
        ];
        let expected_did = "did:key:z6MkqpTi7zUDy5nnSfpLf7SPGsepMNJAxRiH1jbCZbuaZoEz";

        let did = did_from_pubkey(&test_pubkey);
        assert_eq!(did, expected_did);
    }
}
