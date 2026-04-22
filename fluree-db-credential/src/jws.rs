//! JWS (JSON Web Signature) compact format verification
//!
//! Verifies Ed25519-signed JWS tokens in compact serialization format:
//! `header.payload.signature`

use crate::did::did_from_pubkey;
use crate::ed25519::verify_ed25519;
use crate::error::{CredentialError, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::Deserialize;

/// Result of JWS verification
#[derive(Debug, Clone)]
pub struct JwsVerified {
    /// Raw payload string (may be JSON or SPARQL)
    pub payload: String,
    /// Ed25519 public key used for verification
    pub pubkey: [u8; 32],
    /// Derived did:key:z6Mk... from pubkey
    pub did: String,
}

/// JWS header structure
#[derive(Deserialize, Debug)]
struct JwsHeader {
    /// Algorithm (must be "EdDSA")
    alg: String,
    /// Optional embedded JWK (OKP/Ed25519)
    jwk: Option<JwkOkp>,
    /// Optional key ID (base58 account ID).
    // Kept for: OIDC/JWKS verification path where kid identifies the signing key.
    // The embedded-JWK path (this module) does not use kid; the OIDC path
    // (oidc_jwt.rs) uses jsonwebtoken's own header decoder which has its own kid field.
    // This field exists for forward-compatibility if we ever unify the header parsing.
    #[expect(dead_code)]
    kid: Option<String>,
}

/// OKP (Octet Key Pair) JWK for Ed25519
#[derive(Deserialize, Debug)]
struct JwkOkp {
    /// Key type (must be "OKP")
    kty: String,
    /// Curve (must be "Ed25519")
    crv: String,
    /// Public key (base64url-encoded 32 bytes)
    x: String,
}

/// Verify a JWS compact token
///
/// # Format
/// `header.payload.signature` where:
/// - header: base64url-encoded JSON with `{"alg":"EdDSA","jwk":{...}}`
/// - payload: base64url-encoded string (JSON query/txn or raw SPARQL)
/// - signature: base64url-encoded Ed25519 signature (64 bytes)
///
/// # V1 Scope
/// Requires `header.jwk` (embedded public key in JWK format).
/// - Legacy `crypto/create-jws` with `{:include-pubkey true}` embeds pubkey
/// - `kid`-based verification deferred to V2
///
/// # Returns
/// `JwsVerified` with payload, pubkey, and derived DID
///
/// # Errors
/// - `InvalidJwsFormat` if not 3 dot-separated parts
/// - `InvalidJwsHeader` if header parsing fails
/// - `UnsupportedAlgorithm` if alg != "EdDSA"
/// - `MissingField` if jwk is missing
/// - `InvalidSignature` if signature verification fails
pub fn verify_jws(jws: &str) -> Result<JwsVerified> {
    // 1. Split by '.' into [header_b64, payload_b64, sig_b64]
    let parts: Vec<&str> = jws.split('.').collect();
    if parts.len() != 3 {
        return Err(CredentialError::InvalidJwsFormat(format!(
            "Expected 3 parts (header.payload.signature), got {}",
            parts.len()
        )));
    }

    let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    // 2. Decode header (base64url -> JSON)
    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|e| CredentialError::Base64Decode(format!("header: {e}")))?;

    let header: JwsHeader = serde_json::from_slice(&header_bytes)
        .map_err(|e| CredentialError::InvalidJwsHeader(e.to_string()))?;

    // 3. Validate alg == "EdDSA"
    if header.alg != "EdDSA" {
        return Err(CredentialError::UnsupportedAlgorithm(header.alg));
    }

    // 4. Extract pubkey from header.jwk (OKP/Ed25519 JWK format)
    let pubkey = extract_pubkey_from_jwk(&header)?;

    // 5. Decode signature (base64url -> 64 bytes)
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|e| CredentialError::Base64Decode(format!("signature: {e}")))?;

    // 6. signing_input = "{header_b64}.{payload_b64}"
    let signing_input = format!("{header_b64}.{payload_b64}");

    // 7. Verify Ed25519 signature over signing_input
    verify_ed25519(&pubkey, signing_input.as_bytes(), &signature_bytes)?;

    // 8. Decode payload (base64url -> string)
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| CredentialError::Base64Decode(format!("payload: {e}")))?;

    let payload = String::from_utf8(payload_bytes)
        .map_err(|e| CredentialError::InvalidJwsFormat(format!("payload not UTF-8: {e}")))?;

    // 9. Derive DID: did_from_pubkey(&pubkey)
    let did = did_from_pubkey(&pubkey);

    // 10. Return JwsVerified { payload, pubkey, did }
    Ok(JwsVerified {
        payload,
        pubkey,
        did,
    })
}

/// Extract Ed25519 public key from JWK in header
fn extract_pubkey_from_jwk(header: &JwsHeader) -> Result<[u8; 32]> {
    let jwk = header.jwk.as_ref().ok_or_else(|| {
        CredentialError::MissingField(
            "header.jwk (embedded public key required for V1)".to_string(),
        )
    })?;

    // Validate JWK type
    if jwk.kty != "OKP" {
        return Err(CredentialError::InvalidJwsHeader(format!(
            "JWK kty must be 'OKP', got '{}'",
            jwk.kty
        )));
    }

    if jwk.crv != "Ed25519" {
        return Err(CredentialError::InvalidJwsHeader(format!(
            "JWK crv must be 'Ed25519', got '{}'",
            jwk.crv
        )));
    }

    // Decode the public key from base64url
    let key_bytes = URL_SAFE_NO_PAD
        .decode(&jwk.x)
        .map_err(|e| CredentialError::Base64Decode(format!("jwk.x: {e}")))?;

    if key_bytes.len() != 32 {
        return Err(CredentialError::InvalidPublicKey(format!(
            "Ed25519 public key must be 32 bytes, got {}",
            key_bytes.len()
        )));
    }

    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(&key_bytes);
    Ok(pubkey)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// Create a test JWS with embedded JWK
    fn create_test_jws(payload: &str, signing_key: &SigningKey) -> String {
        let pubkey = signing_key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

        // Create header with embedded JWK
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

        // Sign header.payload
        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{header_b64}.{payload_b64}.{sig_b64}")
    }

    #[test]
    fn test_verify_jws_valid() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"select":["?s"],"where":{"@id":"?s"}}"#;

        let jws = create_test_jws(payload, &signing_key);
        let result = verify_jws(&jws).unwrap();

        assert_eq!(result.payload, payload);
        assert_eq!(result.pubkey, signing_key.verifying_key().to_bytes());
        assert!(result.did.starts_with("did:key:z"));
    }

    #[test]
    fn test_verify_jws_invalid_format() {
        let result = verify_jws("not.enough");
        assert!(matches!(result, Err(CredentialError::InvalidJwsFormat(_))));
    }

    #[test]
    fn test_verify_jws_wrong_algorithm() {
        let header = serde_json::json!({
            "alg": "RS256",
            "jwk": {"kty": "OKP", "crv": "Ed25519", "x": "AAAA"}
        });
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let jws = format!("{header_b64}.cGF5bG9hZA.c2ln");

        let result = verify_jws(&jws);
        assert!(matches!(
            result,
            Err(CredentialError::UnsupportedAlgorithm(_))
        ));
    }

    #[test]
    fn test_verify_jws_missing_jwk() {
        let header = serde_json::json!({"alg": "EdDSA"});
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let jws = format!("{header_b64}.cGF5bG9hZA.c2ln");

        let result = verify_jws(&jws);
        assert!(matches!(result, Err(CredentialError::MissingField(_))));
    }

    #[test]
    fn test_verify_jws_tampered_signature() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"query":"test"}"#;

        let mut jws = create_test_jws(payload, &signing_key);

        // Tamper with the last character of signature
        let last_char = jws.pop().unwrap();
        let new_char = if last_char == 'A' { 'B' } else { 'A' };
        jws.push(new_char);

        let result = verify_jws(&jws);
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_verify_jws_sparql_payload() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";

        let jws = create_test_jws(sparql, &signing_key);
        let result = verify_jws(&jws).unwrap();

        assert_eq!(result.payload, sparql);
    }
}
