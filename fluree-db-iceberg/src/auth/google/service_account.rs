//! Service-account keys: an RS256 JWT assertion exchanged at
//! `oauth2.googleapis.com` (Google's "OAuth 2.0 for server to server
//! applications" flow).
//!
//! The key JSON is the secret; parsing it never echoes a value into an error
//! (`serde_json`'s messages can quote the offending input, so they are not
//! passed on).

use crate::error::{IcebergError, Result};

use super::{is_google_universe, is_service_account_email, OAUTH2_TOKEN_URL};

/// A validated service-account key.
#[cfg_attr(not(feature = "aws"), allow(dead_code))]
pub(super) struct ServiceAccountKey {
    pub(super) client_email: String,
    pub(super) private_key_id: String,
    pub(super) quota_project_id: Option<String>,
    #[cfg(feature = "aws")]
    signing_key: jsonwebtoken::EncodingKey,
    /// Digest of the private key PEM: binds a cached token to the key that
    /// minted it (see `cache::TokenKey`). In memory only.
    pub(super) binding: [u8; 32],
}

/// Redacting `Debug`: the signing key never prints.
impl std::fmt::Debug for ServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAccountKey")
            .field("client_email", &self.client_email)
            .field("private_key_id", &self.private_key_id)
            .finish_non_exhaustive()
    }
}

impl ServiceAccountKey {
    /// Parse and validate a key file's JSON text. `field` names it in errors
    /// (`service_account.key`, `application_default`).
    pub(super) fn parse(text: &str, field: &str) -> Result<Self> {
        let invalid = |what: String| IcebergError::GoogleCredentialsInvalid(what);
        let value: serde_json::Value = serde_json::from_str(text).map_err(|e| {
            invalid(format!(
                "{field} is not JSON (line {}, column {})",
                e.line(),
                e.column()
            ))
        })?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid(format!("{field} is not a JSON object")))?;
        let text_field = |name: &str| -> Result<Option<&str>> {
            match object.get(name) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(serde_json::Value::String(s)) => Ok(Some(s.as_str())),
                Some(_) => Err(invalid(format!("{field}.{name} must be a string"))),
            }
        };
        let required = |name: &str| -> Result<&str> {
            text_field(name)?
                .filter(|s| !s.is_empty())
                .ok_or_else(|| invalid(format!("{field}.{name} is required")))
        };

        if text_field("type")? != Some("service_account") {
            return Err(invalid(format!(
                "{field} is not a service-account key (type must be service_account)"
            )));
        }
        let client_email = required("client_email")?;
        if !is_service_account_email(client_email) {
            return Err(invalid(format!(
                "{field}.client_email is not a service-account email"
            )));
        }
        let private_key_id = required("private_key_id")?;
        let private_key = required("private_key")?;
        // The assertion's audience is the key's token_uri, so it must be
        // Google's: a key naming another endpoint would send a signed
        // assertion there.
        if let Some(token_uri) = text_field("token_uri")? {
            if token_uri != OAUTH2_TOKEN_URL {
                return Err(invalid(format!(
                    "{field}.token_uri must be {OAUTH2_TOKEN_URL}"
                )));
            }
        }
        if let Some(universe) = text_field("universe_domain")? {
            if !is_google_universe(universe) {
                return Err(invalid(format!(
                    "{field}.universe_domain must be googleapis.com"
                )));
            }
        }
        let quota_project_id = match text_field("quota_project_id")? {
            Some(project) if !super::is_project_id(project) => {
                return Err(invalid(format!(
                    "{field}.quota_project_id is not a Google Cloud project id"
                )))
            }
            project => project.map(str::to_string),
        };

        #[cfg(feature = "aws")]
        let signing_key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|_| invalid(format!("{field}.private_key is not a PEM RSA private key")))?;

        Ok(Self {
            client_email: client_email.to_string(),
            private_key_id: private_key_id.to_string(),
            quota_project_id,
            #[cfg(feature = "aws")]
            signing_key,
            binding: super::secret_binding(&[private_key]),
        })
    }
}

#[cfg(feature = "aws")]
mod exchange {
    use chrono::Utc;

    use super::ServiceAccountKey;
    use crate::auth::google::{wire, OAUTH2_TOKEN_URL};
    use crate::auth::token::CachedToken;
    use crate::error::{GoogleTokenStage, IcebergError, Result};

    /// Backdated so a host clock slightly ahead of Google's is not refused.
    const CLOCK_SKEW_SECS: i64 = 60;
    /// Google's maximum assertion lifetime.
    const ASSERTION_LIFETIME_SECS: i64 = 3600;
    const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    #[derive(serde::Serialize)]
    struct Claims<'a> {
        iss: &'a str,
        sub: &'a str,
        aud: &'a str,
        scope: String,
        iat: i64,
        exp: i64,
    }

    impl ServiceAccountKey {
        /// The signed assertion for `scopes`, valid from `now - 60s` for an hour.
        pub(in crate::auth::google) fn assertion(&self, scopes: &[String]) -> Result<String> {
            let iat = Utc::now().timestamp() - CLOCK_SKEW_SECS;
            let claims = Claims {
                iss: &self.client_email,
                sub: &self.client_email,
                aud: OAUTH2_TOKEN_URL,
                scope: scopes.join(" "),
                iat,
                exp: iat + ASSERTION_LIFETIME_SECS,
            };
            let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
            header.kid = Some(self.private_key_id.clone());
            jsonwebtoken::encode(&header, &claims, &self.signing_key).map_err(|_| {
                IcebergError::Credential("failed to sign the service-account assertion".to_string())
            })
        }

        /// Exchange a fresh assertion for an access token at `token_url`.
        pub(in crate::auth::google) async fn fetch(
            &self,
            http: &reqwest::Client,
            token_url: &str,
            scopes: &[String],
        ) -> Result<CachedToken> {
            let stage = GoogleTokenStage::Jwt;
            let assertion = self.assertion(scopes)?;
            let response = http
                .post(token_url)
                .form(&[("grant_type", JWT_BEARER_GRANT), ("assertion", &assertion)])
                .send()
                .await
                .map_err(|e| wire::transport_error(stage, &e))?;
            wire::read_token(stage, response).await
        }
    }
}
