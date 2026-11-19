//! The HTTP shapes every Google token exchange shares: the token response, the
//! typed error, and Google's error code (never its body).

use crate::auth::token::CachedToken;
use crate::error::{GoogleTokenStage, IcebergError, Result};

/// The fixed reason when a 2xx response is not the token it should be.
pub(super) const UNREADABLE_RESPONSE: &str = "unreadable_response";

/// Lifetime assumed when a token response names none (Google's tokens live an
/// hour; every Google endpoint used here does send `expires_in`).
const DEFAULT_EXPIRES_IN_SECS: i64 = 3600;

/// An exchange that got no response at all.
pub(super) fn transport_error(stage: GoogleTokenStage, err: &reqwest::Error) -> IcebergError {
    let reason = if err.is_timeout() {
        Some("timeout".to_string())
    } else if err.is_connect() {
        Some("connect_failed".to_string())
    } else {
        None
    };
    IcebergError::GoogleTokenExchange {
        stage,
        status: None,
        reason,
    }
}

/// Read a token exchange's response: the token on success, else the typed
/// refusal carrying the status and Google's error code. The body is parsed for
/// the code only and is never logged or kept.
pub(super) async fn read_token(
    stage: GoogleTokenStage,
    response: reqwest::Response,
) -> Result<CachedToken> {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|e| transport_error(stage, &e))?;
    if !status.is_success() {
        return Err(refusal(stage, status.as_u16(), &body));
    }

    #[derive(serde::Deserialize)]
    struct TokenResponse {
        access_token: String,
        expires_in: Option<i64>,
    }
    let parsed: TokenResponse = serde_json::from_slice(&body).map_err(|_| unreadable(stage))?;
    if parsed.access_token.is_empty() {
        return Err(unreadable(stage));
    }
    let expires_in = parsed.expires_in.unwrap_or(DEFAULT_EXPIRES_IN_SECS);
    Ok(CachedToken {
        access_token: parsed.access_token,
        token_type: "Bearer".to_string(),
        expires_at: crate::auth::token::expires_at_in(expires_in),
    })
}

/// A non-2xx answer from a token endpoint.
pub(super) fn refusal(stage: GoogleTokenStage, status: u16, body: &[u8]) -> IcebergError {
    let reason = google_error_code(body);
    tracing::debug!(stage = %stage, status, reason = ?reason, "Google token exchange refused");
    IcebergError::GoogleTokenExchange {
        stage,
        status: Some(status),
        reason,
    }
}

/// A 2xx answer that is not a token.
pub(super) fn unreadable(stage: GoogleTokenStage) -> IcebergError {
    IcebergError::GoogleTokenExchange {
        stage,
        status: None,
        reason: Some(UNREADABLE_RESPONSE.to_string()),
    }
}

/// Google's error code from an error body: OAuth endpoints answer
/// `{"error": "invalid_grant", …}`, Google APIs `{"error": {"status":
/// "PERMISSION_DENIED", …}}`. Only a short code-shaped token is kept, so
/// nothing a body says can reach an error message.
pub(super) fn google_error_code(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let code = match value.get("error")? {
        serde_json::Value::String(code) => code.as_str(),
        serde_json::Value::Object(error) => error.get("status")?.as_str()?,
        _ => return None,
    };
    let code_shaped = !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
    code_shaped.then(|| code.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_error_codes_are_extracted_from_both_shapes() {
        assert_eq!(
            google_error_code(br#"{"error":"invalid_grant","error_description":"Invalid JWT"}"#)
                .as_deref(),
            Some("invalid_grant")
        );
        assert_eq!(
            google_error_code(
                br#"{"error":{"code":403,"message":"Permission 'iam.x' denied","status":"PERMISSION_DENIED"}}"#
            )
            .as_deref(),
            Some("PERMISSION_DENIED")
        );
    }

    #[test]
    fn nothing_but_a_code_shaped_token_is_kept() {
        for body in [
            &b"not json at all"[..],
            br#"{"error":"has spaces in it, so it is prose"}"#,
            br#"{"error":{"message":"no status"}}"#,
            br#"{"other":"x"}"#,
        ] {
            assert_eq!(
                google_error_code(body),
                None,
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }
}
