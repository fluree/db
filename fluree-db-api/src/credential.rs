//! Credentialed API support - verifies signed queries/transactions
//!
//! This module provides verification of signed queries and transactions using:
//! - JWS (JSON Web Signature) compact format
//! - VerifiableCredential format (feature-gated, requires JSON-LD canonization)
//!
//! The verified identity (DID) is extracted and used for policy enforcement.

use crate::Result;
use fluree_db_credential::{verify, verify_jws, CredentialInput, JwsVerified, VerifiedCredential};

// Re-export for API consumers
pub use fluree_db_credential::{
    CredentialError, CredentialInput as Input, JwsVerified as JwsResult,
};

/// Verify credential (JWS string or JSON) and extract subject + identity + context
///
/// # Arguments
/// * `input` - Either a JWS string or JSON object (VC or wrapped JWS)
///
/// # Returns
/// `VerifiedCredential` with:
/// - `subject`: Decoded subject JSON (JSON-parsed payload for JWS, credentialSubject for VC)
/// - `did`: Signing identity as did:key:z6Mk...
/// - `parent_context`: Optional @context from envelope (VCs only)
///
/// # Errors
/// - `CredentialError::InvalidJwsFormat` if JWS format is invalid
/// - `CredentialError::UnsupportedAlgorithm` if alg is not EdDSA
/// - `CredentialError::InvalidSignature` if signature verification fails
/// - `CredentialError::VcNotEnabled` if JSON input is a VC object
pub fn verify_credential(input: CredentialInput<'_>) -> Result<VerifiedCredential> {
    Ok(verify(input)?)
}

/// Verify JWS for SPARQL format (don't JSON-parse payload)
///
/// For SPARQL queries, the JWS payload is the raw SPARQL string, not JSON.
/// This function returns the raw payload without attempting to JSON-parse it.
///
/// # Arguments
/// * `jws` - Compact JWS string (header.payload.signature)
///
/// # Returns
/// `JwsVerified` with:
/// - `payload`: Raw payload string (SPARQL query)
/// - `pubkey`: Ed25519 public key used for verification
/// - `did`: Derived did:key:z6Mk... from pubkey
///
/// # Errors
/// - `CredentialError::InvalidJwsFormat` if not 3 dot-separated parts
/// - `CredentialError::InvalidJwsHeader` if header parsing fails
/// - `CredentialError::UnsupportedAlgorithm` if alg != "EdDSA"
/// - `CredentialError::MissingField` if header.jwk is missing
/// - `CredentialError::InvalidSignature` if signature verification fails
pub fn verify_jws_sparql(jws: &str) -> Result<JwsVerified> {
    Ok(verify_jws(jws)?)
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
    fn test_verify_credential_jws() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let payload = r#"{"select":["?s"],"where":{"@id":"?s"}}"#;

        let jws = create_test_jws(payload, &signing_key);
        let result = verify_credential(CredentialInput::Jws(&jws)).unwrap();

        assert!(result.did.starts_with("did:key:z"));
        assert_eq!(
            result.subject.get("select").unwrap(),
            &serde_json::json!(["?s"])
        );
    }

    #[test]
    fn test_verify_jws_sparql() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";

        let jws = create_test_jws(sparql, &signing_key);
        let result = verify_jws_sparql(&jws).unwrap();

        assert!(result.did.starts_with("did:key:z"));
        assert_eq!(result.payload, sparql);
    }
}
