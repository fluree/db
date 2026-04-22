//! Standard JWT verification using pre-fetched JWK key material.
//!
//! This module verifies JWTs signed with RS256 (and potentially ES256/EdDSA in the future)
//! using keys provided as `jsonwebtoken::DecodingKey`. The caller (typically fluree-db-server)
//! is responsible for fetching and caching JWKS key sets.
//!
//! This module does NOT perform network operations.

use crate::error::{CredentialError, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::{decode, Algorithm, DecodingKey, TokenData, Validation};

/// Result of verifying a standard JWT (OIDC/JWKS path).
#[derive(Debug, Clone)]
pub struct JwtVerified {
    /// Raw payload as JSON string (for downstream parsing into EventsTokenPayload)
    pub payload_json: String,
    /// The `kid` from the JWT header (if present)
    pub kid: Option<String>,
    /// The algorithm declared in the JWT header
    pub algorithm: Algorithm,
    /// The `iss` claim value
    pub issuer: String,
}

/// Peek at a JWT header to determine which verification path to use.
///
/// Returns `(kid, algorithm, has_embedded_jwk)`:
/// - `kid`: The key ID from the header (used for JWKS lookup)
/// - `algorithm`: The declared algorithm
/// - `has_embedded_jwk`: Whether the header contains an embedded JWK (existing Ed25519 path)
///
/// # Errors
/// - `InvalidJwsHeader` if the header cannot be decoded
pub fn peek_jwt_header(token: &str) -> Result<(Option<String>, Option<Algorithm>, bool)> {
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| CredentialError::InvalidJwsHeader(format!("cannot decode header: {e}")))?;

    let has_embedded_jwk = header.jwk.is_some();
    let algorithm = Some(header.alg);
    let kid = header.kid;

    Ok((kid, algorithm, has_embedded_jwk))
}

/// Verify a standard JWT given a pre-fetched `DecodingKey`.
///
/// # Arguments
/// * `token` - The raw JWT string (header.payload.signature)
/// * `key` - Pre-fetched `DecodingKey` (from JWKS)
/// * `algorithms` - Allowed algorithms. The algorithm is constrained at the verification layer;
///   `alg=none` and mismatches are rejected regardless of what the header claims.
/// * `expected_issuer` - Required; the token's `iss` claim must exactly match this value.
///
/// # Design
/// This function disables `jsonwebtoken`'s built-in exp/nbf/aud validation.
/// Time and audience checks are deferred to `EventsTokenPayload::validate_oidc()`
/// for consistent 60-second clock skew behavior across both verification paths.
///
/// # Errors
/// - `InvalidJwsFormat` if the token structure is invalid
/// - `InvalidJwsHeader` if the header is malformed or issuer mismatches
/// - `InvalidSignature` if signature verification fails
/// - `UnsupportedAlgorithm` if the token's algorithm is not in the allowed set
pub fn verify_jwt(
    token: &str,
    key: &DecodingKey,
    algorithms: &[Algorithm],
    expected_issuer: &str,
) -> Result<JwtVerified> {
    if algorithms.is_empty() {
        return Err(CredentialError::InvalidJwsHeader(
            "no algorithms specified".to_string(),
        ));
    }

    // Build validation config
    let mut validation = Validation::new(algorithms[0]);
    validation.algorithms = algorithms.to_vec();

    // Disable jsonwebtoken's built-in time/audience checks.
    // We handle these in EventsTokenPayload::validate_oidc() for consistent clock skew.
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;

    // Validate issuer at the signature-verification layer
    validation.set_issuer(&[expected_issuer]);

    // Decode the header to extract kid/alg metadata
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| CredentialError::InvalidJwsHeader(e.to_string()))?;

    // Decode and verify signature
    let token_data: TokenData<serde_json::Value> =
        decode(token, key, &validation).map_err(|e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::InvalidSignature => {
                CredentialError::InvalidSignature("JWT signature verification failed".to_string())
            }
            jsonwebtoken::errors::ErrorKind::InvalidAlgorithm => {
                CredentialError::UnsupportedAlgorithm(format!("{:?}", header.alg))
            }
            jsonwebtoken::errors::ErrorKind::InvalidIssuer => CredentialError::InvalidJwsHeader(
                format!("issuer mismatch: expected {expected_issuer}, token has different iss"),
            ),
            _ => CredentialError::JwtError(e.to_string()),
        })?;

    // Extract issuer from verified claims
    let issuer = token_data
        .claims
        .get("iss")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let payload_json = serde_json::to_string(&token_data.claims)
        .map_err(|e| CredentialError::JwtError(format!("failed to serialize claims: {e}")))?;

    Ok(JwtVerified {
        payload_json,
        kid: header.kid,
        algorithm: header.alg,
        issuer,
    })
}

/// Decode the payload of a JWT without signature verification.
///
/// Used to extract the `iss` claim before key lookup (the `iss` determines which
/// JWKS endpoint to query). The caller MUST verify the signature afterward and
/// MUST reject the token if `iss` is not a configured issuer.
///
/// # Returns
/// The decoded payload as a `serde_json::Value`.
///
/// # Errors
/// - `InvalidJwsFormat` if the token is not valid JWT format (3 dot-separated parts)
/// - `JwtError` if the payload cannot be decoded
pub fn decode_unverified_issuer(token: &str) -> Result<Option<String>> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(CredentialError::InvalidJwsFormat(format!(
            "expected 3 parts, got {}",
            parts.len()
        )));
    }

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| CredentialError::JwtError(format!("payload base64 decode: {e}")))?;

    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| CredentialError::JwtError(format!("payload JSON parse: {e}")))?;

    Ok(claims.get("iss").and_then(|v| v.as_str()).map(String::from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    /// Generate an RSA key pair for testing.
    /// Uses a small key size (2048) which is fine for tests.
    fn test_rsa_keys() -> (EncodingKey, DecodingKey) {
        // Use a pre-generated test RSA key pair (PEM format)
        // This is a 2048-bit RSA key generated for testing only.
        let rsa_private = include_str!("../tests/fixtures/test_rsa_private.pem");
        let rsa_public = include_str!("../tests/fixtures/test_rsa_public.pem");
        let encoding = EncodingKey::from_rsa_pem(rsa_private.as_bytes()).unwrap();
        let decoding = DecodingKey::from_rsa_pem(rsa_public.as_bytes()).unwrap();
        (encoding, decoding)
    }

    fn create_test_jwt(claims: &serde_json::Value, key: &EncodingKey, kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        encode(&header, claims, key).unwrap()
    }

    #[test]
    fn test_verify_jwt_rs256_valid() {
        let (enc_key, dec_key) = test_rsa_keys();
        let claims = json!({
            "iss": "https://solo.example.com",
            "sub": "user@example.com",
            "exp": 9_999_999_999_u64,
            "iat": 1_700_000_000_u64,
            "fluree.identity": "did:key:z6MkTest",
            "fluree.ledger.read.all": true
        });

        let token = create_test_jwt(&claims, &enc_key, "test-kid-1");
        let result = verify_jwt(
            &token,
            &dec_key,
            &[Algorithm::RS256],
            "https://solo.example.com",
        )
        .unwrap();

        assert_eq!(result.issuer, "https://solo.example.com");
        assert_eq!(result.kid, Some("test-kid-1".to_string()));
        assert_eq!(result.algorithm, Algorithm::RS256);

        // Verify claims round-trip
        let parsed: serde_json::Value = serde_json::from_str(&result.payload_json).unwrap();
        assert_eq!(parsed["fluree.identity"], "did:key:z6MkTest");
        assert_eq!(parsed["fluree.ledger.read.all"], true);
    }

    #[test]
    fn test_verify_jwt_wrong_key_rejected() {
        let (enc_key, dec_key) = test_rsa_keys();
        let claims = json!({
            "iss": "https://solo.example.com",
            "exp": 9_999_999_999_u64,
        });

        let token = create_test_jwt(&claims, &enc_key, "kid-1");

        // Tamper with the signature to simulate wrong key
        let mut tampered = token.clone();
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });

        let result = verify_jwt(
            &tampered,
            &dec_key,
            &[Algorithm::RS256],
            "https://solo.example.com",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_jwt_issuer_mismatch_rejected() {
        let (enc_key, dec_key) = test_rsa_keys();
        let claims = json!({
            "iss": "https://evil.example.com",
            "exp": 9_999_999_999_u64,
        });

        let token = create_test_jwt(&claims, &enc_key, "kid-1");
        let result = verify_jwt(
            &token,
            &dec_key,
            &[Algorithm::RS256],
            "https://solo.example.com",
        );
        assert!(result.is_err(), "should reject token with wrong issuer");
        let err = result.unwrap_err();
        assert!(
            matches!(err, CredentialError::InvalidJwsHeader(_)),
            "expected InvalidJwsHeader for issuer mismatch, got {err:?}"
        );
    }

    #[test]
    fn test_verify_jwt_alg_none_rejected() {
        // alg=none should be rejected even if the header claims it
        let claims = json!({
            "iss": "https://solo.example.com",
            "exp": 9_999_999_999_u64,
        });

        // Manually construct an alg=none token
        let header_json = json!({"alg": "none", "typ": "JWT"});
        let header_b64 = URL_SAFE_NO_PAD.encode(header_json.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());
        let token = format!("{header_b64}..{payload_b64}");

        let (_enc_key, dec_key) = test_rsa_keys();
        let result = verify_jwt(
            &token,
            &dec_key,
            &[Algorithm::RS256],
            "https://solo.example.com",
        );
        assert!(result.is_err(), "alg=none must be rejected");
    }

    #[test]
    fn test_peek_header_with_kid() {
        let (enc_key, _) = test_rsa_keys();
        let claims = json!({"iss": "https://example.com", "exp": 9_999_999_999_u64});
        let token = create_test_jwt(&claims, &enc_key, "my-kid");

        let (kid, alg, has_jwk) = peek_jwt_header(&token).unwrap();
        assert_eq!(kid, Some("my-kid".to_string()));
        assert_eq!(alg, Some(Algorithm::RS256));
        assert!(!has_jwk, "RS256 token should not have embedded JWK");
    }

    #[test]
    fn test_peek_header_with_embedded_jwk() {
        // Construct a header with embedded JWK (the existing Ed25519 path)
        let header_json = json!({
            "alg": "EdDSA",
            "jwk": {"kty": "OKP", "crv": "Ed25519", "x": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
        });
        let header_b64 = URL_SAFE_NO_PAD.encode(header_json.to_string().as_bytes());
        let token = format!("{header_b64}.cGF5bG9hZA.c2ln");

        let (kid, alg, has_jwk) = peek_jwt_header(&token).unwrap();
        assert!(kid.is_none());
        assert_eq!(alg, Some(Algorithm::EdDSA));
        assert!(has_jwk, "should detect embedded JWK");
    }

    #[test]
    fn test_decode_unverified_issuer() {
        let (enc_key, _) = test_rsa_keys();
        let claims = json!({
            "iss": "https://solo.example.com",
            "exp": 9_999_999_999_u64,
            "sub": "user@example.com"
        });
        let token = create_test_jwt(&claims, &enc_key, "kid-1");

        let iss = decode_unverified_issuer(&token).unwrap();
        assert_eq!(iss, Some("https://solo.example.com".to_string()));
    }

    #[test]
    fn test_decode_unverified_issuer_missing_iss() {
        let (enc_key, _) = test_rsa_keys();
        let claims = json!({"exp": 9_999_999_999_u64});
        let token = create_test_jwt(&claims, &enc_key, "kid-1");

        let iss = decode_unverified_issuer(&token).unwrap();
        assert_eq!(iss, None);
    }

    #[test]
    fn test_decode_unverified_issuer_invalid_format() {
        let result = decode_unverified_issuer("not-a-jwt");
        assert!(result.is_err());
    }
}
