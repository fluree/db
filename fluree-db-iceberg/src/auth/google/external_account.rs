//! Workload Identity Federation from AWS: the validated subset of Google's
//! external-account credential format (AIP-4117,
//! <https://google.aip.dev/auth/4117>).
//!
//! The configuration is not secret, and on a hosted stack it comes from
//! tenants, so it is validated rather than followed: Google's format can name
//! arbitrary token and impersonation URLs, local files, URLs and executables as
//! credential sources. Only an AWS source is accepted, every endpoint must be
//! Google's own, and the AWS metadata URLs gcloud always writes are checked
//! against Google's documented defaults and then ignored: the AWS credentials
//! come from the identity context's subject (the host's aws-config chain, or
//! an embedder's per-tenant credentials), never from a URL in the config.

use serde_json::{Map, Value};

use crate::error::{IcebergError, Result};

use super::{is_google_universe, is_project_id, is_service_account_email, STS_TOKEN_URL};

/// The subject token type for a SigV4-signed AWS GetCallerIdentity request.
pub(super) const AWS4_SUBJECT_TOKEN_TYPE: &str = "urn:ietf:params:aws:token-type:aws4_request";

/// Google's documented AWS credential-source URLs (AIP-4117), the only values
/// accepted for the fields gcloud writes.
const AWS_SECURITY_CREDENTIALS_URL: &str =
    "http://169.254.169.254/latest/meta-data/iam/security-credentials";
const AWS_REGION_URL: &str = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
const AWS_CRED_VERIFICATION_URL: &str =
    "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15";
const AWS_IMDSV2_SESSION_TOKEN_URL: &str = "http://169.254.169.254/latest/api/token";

const IMPERSONATION_PREFIX: &str =
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";
const IMPERSONATION_SUFFIX: &str = ":generateAccessToken";
const TOKEN_INFO_URL: &str = "https://sts.googleapis.com/v1/introspect";

/// Google's bounds on an impersonated token's lifetime.
const MIN_LIFETIME_SECS: u64 = 600;
const MAX_LIFETIME_SECS: u64 = 43_200;
/// Lifetime requested when the config names none (Google's default).
const DEFAULT_LIFETIME_SECS: u64 = 3600;

/// A validated AWS-sourced external-account configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "aws"), allow(dead_code))]
pub(super) struct ExternalAccountSpec {
    /// `//iam.googleapis.com/projects/<n>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`
    pub(super) audience: String,
    /// The service account the federated token is exchanged for, if any.
    pub(super) impersonate: Option<String>,
    /// Requested lifetime of the impersonated token.
    pub(super) lifetime_secs: u64,
    pub(super) quota_project_id: Option<String>,
}

fn invalid(what: impl Into<String>) -> IcebergError {
    IcebergError::GoogleCredentialsInvalid(format!("external_account.config{}", what.into()))
}

/// Validate `config`. Every refusal names the field, never its value.
pub(super) fn parse(config: &Map<String, Value>) -> Result<ExternalAccountSpec> {
    const ACCEPTED: [&str; 10] = [
        "type",
        "audience",
        "subject_token_type",
        "token_url",
        "service_account_impersonation_url",
        "service_account_impersonation",
        "credential_source",
        "universe_domain",
        "quota_project_id",
        "token_info_url",
    ];
    for key in config.keys() {
        match key.as_str() {
            "workforce_pool_user_project" => {
                return Err(invalid(
                    ".workforce_pool_user_project: workforce identity pools are not supported",
                ))
            }
            "client_id" | "client_secret" => {
                return Err(invalid(format!(
                    ".{key}: STS client authentication is not supported"
                )))
            }
            known if ACCEPTED.contains(&known) => {}
            other => return Err(invalid(format!(": unknown field `{other}`"))),
        }
    }

    let text = |name: &str| -> Result<Option<&str>> {
        match config.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.as_str())),
            Some(_) => Err(invalid(format!(".{name} must be a string"))),
        }
    };
    let expect = |name: &str, wanted: &str| -> Result<()> {
        match text(name)? {
            Some(value) if value == wanted => Ok(()),
            Some(_) => Err(invalid(format!(".{name} must be {wanted}"))),
            None => Err(invalid(format!(".{name} is required (must be {wanted})"))),
        }
    };

    expect("type", "external_account")?;
    expect("subject_token_type", AWS4_SUBJECT_TOKEN_TYPE)?;
    expect("token_url", STS_TOKEN_URL)?;
    if let Some(universe) = text("universe_domain")? {
        if !is_google_universe(universe) {
            return Err(invalid(".universe_domain must be googleapis.com"));
        }
    }
    if let Some(info) = text("token_info_url")? {
        if info != TOKEN_INFO_URL {
            return Err(invalid(format!(".token_info_url must be {TOKEN_INFO_URL}")));
        }
    }

    let audience = text("audience")?.ok_or_else(|| invalid(".audience is required"))?;
    if !is_workload_identity_audience(audience) {
        return Err(invalid(
            ".audience must be //iam.googleapis.com/projects/<number>/locations/global/\
             workloadIdentityPools/<pool>/providers/<provider>",
        ));
    }

    parse_credential_source(config.get("credential_source"))?;

    let impersonate = match text("service_account_impersonation_url")? {
        None => None,
        Some(url) => Some(
            url.strip_prefix(IMPERSONATION_PREFIX)
                .and_then(|rest| rest.strip_suffix(IMPERSONATION_SUFFIX))
                .filter(|email| is_service_account_email(email))
                .ok_or_else(|| {
                    invalid(format!(
                        ".service_account_impersonation_url must be \
                         {IMPERSONATION_PREFIX}<service-account email>{IMPERSONATION_SUFFIX}"
                    ))
                })?
                .to_string(),
        ),
    };

    let lifetime_secs = match config.get("service_account_impersonation") {
        None | Some(Value::Null) => DEFAULT_LIFETIME_SECS,
        Some(Value::Object(options)) => {
            if impersonate.is_none() {
                return Err(invalid(
                    ".service_account_impersonation needs service_account_impersonation_url",
                ));
            }
            if let Some(other) = options.keys().find(|k| *k != "token_lifetime_seconds") {
                return Err(invalid(format!(
                    ".service_account_impersonation: unknown field `{other}`"
                )));
            }
            match options.get("token_lifetime_seconds") {
                None => DEFAULT_LIFETIME_SECS,
                Some(value) => value
                    .as_u64()
                    .filter(|s| (MIN_LIFETIME_SECS..=MAX_LIFETIME_SECS).contains(s))
                    .ok_or_else(|| {
                        invalid(format!(
                            ".service_account_impersonation.token_lifetime_seconds must be an \
                             integer from {MIN_LIFETIME_SECS} to {MAX_LIFETIME_SECS}"
                        ))
                    })?,
            }
        }
        Some(_) => return Err(invalid(".service_account_impersonation must be an object")),
    };

    let quota_project_id = match text("quota_project_id")? {
        Some(project) if !is_project_id(project) => {
            return Err(invalid(
                ".quota_project_id is not a Google Cloud project id",
            ))
        }
        project => project.map(str::to_string),
    };

    Ok(ExternalAccountSpec {
        audience: audience.to_string(),
        impersonate,
        lifetime_secs,
        quota_project_id,
    })
}

/// Accept only `environment_id = aws1`, with each AWS URL gcloud writes equal
/// to Google's documented default. Anything else — `file`, `url` or
/// `executable` sources, extra fields — is refused.
fn parse_credential_source(source: Option<&Value>) -> Result<()> {
    let refused = || {
        invalid(
            ".credential_source: only an AWS credential source (environment_id aws1) is \
             supported; file, URL and executable sources are refused",
        )
    };
    let source = match source {
        Some(Value::Object(source)) => source,
        Some(_) | None => return Err(refused()),
    };
    if source.get("environment_id").and_then(Value::as_str) != Some("aws1") {
        return Err(refused());
    }
    for (key, value) in source {
        let wanted = match key.as_str() {
            "environment_id" => continue,
            "url" => AWS_SECURITY_CREDENTIALS_URL,
            "region_url" => AWS_REGION_URL,
            "regional_cred_verification_url" => AWS_CRED_VERIFICATION_URL,
            "imdsv2_session_token_url" => AWS_IMDSV2_SESSION_TOKEN_URL,
            _ => return Err(refused()),
        };
        if value.as_str() != Some(wanted) {
            return Err(invalid(format!(
                ".credential_source.{key} must be Google's documented AWS default ({wanted})"
            )));
        }
    }
    Ok(())
}

/// `//iam.googleapis.com/projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`
fn is_workload_identity_audience(audience: &str) -> bool {
    let Some(rest) = audience.strip_prefix("//iam.googleapis.com/projects/") else {
        return false;
    };
    let Some((number, rest)) = rest.split_once("/locations/global/workloadIdentityPools/") else {
        return false;
    };
    let Some((pool, provider)) = rest.split_once("/providers/") else {
        return false;
    };
    let id = |s: &str| {
        !s.is_empty()
            && s.len() <= 63
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    };
    !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()) && id(pool) && id(provider)
}

#[cfg(feature = "aws")]
mod exchange {
    use std::time::SystemTime;

    use chrono::{DateTime, Utc};

    use super::{ExternalAccountSpec, AWS4_SUBJECT_TOKEN_TYPE};
    use crate::auth::google::aws_subject::AwsSubjectCredentials;
    use crate::auth::google::{wire, GoogleEndpoints, CLOUD_PLATFORM_SCOPE};
    use crate::auth::token::CachedToken;
    use crate::error::{GoogleTokenStage, IcebergError, Result};

    const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
    const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";

    /// The STS subject token: a SigV4-signed `POST` to AWS STS
    /// `GetCallerIdentity`, serialized as URL-encoded JSON
    /// `{"url", "method", "headers": [{"key", "value"}]}` — the shape in
    /// AIP-4117 (<https://google.aip.dev/auth/4117>, "Determining the subject
    /// token in AWS") and Google's "Workload Identity Federation with other
    /// clouds" guide
    /// (<https://cloud.google.com/iam/docs/workload-identity-federation-with-other-clouds>).
    ///
    /// Google STS replays the request to AWS to learn the caller's identity.
    /// `x-goog-cloud-target-resource` (the audience) is inside the signature,
    /// as Google recommends and its Go library does, so the token cannot be
    /// replayed against another workload identity provider.
    fn subject_token(
        credentials: &aws_credential_types::Credentials,
        region: &str,
        audience: &str,
        now: SystemTime,
    ) -> Result<String> {
        use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
        use aws_sigv4::sign::v4;

        crate::config::validate_aws_region("the WIF subject token's AWS region", Some(region))?;
        let host = format!("sts.{region}.amazonaws.com");
        let url = super::AWS_CRED_VERIFICATION_URL.replace("{region}", region);
        let signing_failed = || {
            IcebergError::Credential(
                "failed to sign the AWS GetCallerIdentity request for the WIF subject token"
                    .to_string(),
            )
        };

        let identity = credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name("sts")
            .time(now)
            .settings(SigningSettings::default())
            .build()
            .map_err(|_| signing_failed())?
            .into();
        let signed_headers = [
            ("host", host.as_str()),
            ("x-goog-cloud-target-resource", audience),
        ];
        let request = SignableRequest::new(
            "POST",
            url.as_str(),
            signed_headers.iter().copied(),
            SignableBody::Bytes(&[]),
        )
        .map_err(|_| signing_failed())?;
        let (instructions, _signature) = sign(request, &params)
            .map_err(|_| signing_failed())?
            .into_parts();

        // The signer returns the headers it added (x-amz-date, the session
        // token when there is one, authorization); the two it signed over
        // come from the request.
        let added: Vec<(&str, &str)> = instructions.headers().collect();
        let header = |name: &str| {
            added
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| *v)
        };
        let mut headers = vec![
            ("x-goog-cloud-target-resource", audience),
            (
                "x-amz-date",
                header("x-amz-date").ok_or_else(signing_failed)?,
            ),
            (
                "Authorization",
                header("authorization").ok_or_else(signing_failed)?,
            ),
            ("host", host.as_str()),
        ];
        if let Some(token) = header("x-amz-security-token") {
            headers.push(("x-amz-security-token", token));
        }
        let serialized = serde_json::json!({
            "url": url,
            "method": "POST",
            "headers": headers
                .iter()
                .map(|(key, value)| serde_json::json!({"key": key, "value": value}))
                .collect::<Vec<_>>(),
        });
        Ok(urlencoding::encode(&serialized.to_string()).into_owned())
    }

    impl ExternalAccountSpec {
        /// Exchange the subject's AWS identity for a Google access token: STS,
        /// then impersonation when the config names a service account.
        pub(in crate::auth::google) async fn fetch(
            &self,
            subject: &dyn AwsSubjectCredentials,
            http: &reqwest::Client,
            endpoints: &GoogleEndpoints,
            scopes: &[String],
        ) -> Result<CachedToken> {
            let credentials = subject.credentials().await?;
            let region = subject
                .region()
                .or_else(|| std::env::var("AWS_REGION").ok())
                .filter(|r| !r.is_empty())
                .ok_or_else(|| {
                    IcebergError::Credential(
                        "no AWS region for the WIF subject token: the AWS subject names none and \
                         AWS_REGION is unset"
                            .to_string(),
                    )
                })?;
            let subject_token =
                subject_token(&credentials, &region, &self.audience, SystemTime::now())?;

            // With impersonation, STS mints a federated token able to call
            // iamcredentials, and the requested scopes go to the impersonated
            // token instead.
            let sts_scope = if self.impersonate.is_some() {
                CLOUD_PLATFORM_SCOPE.to_string()
            } else {
                scopes.join(" ")
            };
            let stage = GoogleTokenStage::Sts;
            let response = http
                .post(&endpoints.sts_token)
                .form(&[
                    ("grant_type", TOKEN_EXCHANGE_GRANT),
                    ("audience", self.audience.as_str()),
                    ("scope", sts_scope.as_str()),
                    ("requested_token_type", ACCESS_TOKEN_TYPE),
                    ("subject_token", subject_token.as_str()),
                    ("subject_token_type", AWS4_SUBJECT_TOKEN_TYPE),
                ])
                .send()
                .await
                .map_err(|e| wire::transport_error(stage, &e))?;
            let federated = wire::read_token(stage, response).await?;

            match &self.impersonate {
                None => Ok(federated),
                Some(email) => {
                    self.impersonate(http, endpoints, email, &federated, scopes)
                        .await
                }
            }
        }

        /// `generateAccessToken` for `email` with the federated token.
        async fn impersonate(
            &self,
            http: &reqwest::Client,
            endpoints: &GoogleEndpoints,
            email: &str,
            federated: &CachedToken,
            scopes: &[String],
        ) -> Result<CachedToken> {
            let stage = GoogleTokenStage::Impersonation;
            let url = format!(
                "{}/v1/projects/-/serviceAccounts/{email}:generateAccessToken",
                endpoints.iam_credentials
            );
            let response = http
                .post(&url)
                .bearer_auth(&federated.access_token)
                .json(&serde_json::json!({
                    "scope": scopes,
                    "lifetime": format!("{}s", self.lifetime_secs),
                }))
                .send()
                .await
                .map_err(|e| wire::transport_error(stage, &e))?;
            let status = response.status();
            let body = response
                .bytes()
                .await
                .map_err(|e| wire::transport_error(stage, &e))?;
            if !status.is_success() {
                return Err(wire::refusal(stage, status.as_u16(), &body));
            }

            // iamcredentials answers with an RFC 3339 `expireTime`, not
            // `expires_in`.
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Impersonated {
                access_token: String,
                expire_time: String,
            }
            let parsed: Impersonated =
                serde_json::from_slice(&body).map_err(|_| wire::unreadable(stage))?;
            // Believed up to the same ceiling as an `expires_in`.
            let expires_at = DateTime::parse_from_rfc3339(&parsed.expire_time)
                .map_err(|_| wire::unreadable(stage))?
                .with_timezone(&Utc)
                .min(crate::auth::token::expires_at_in(
                    crate::auth::token::MAX_TOKEN_LIFETIME_SECS,
                ));
            if parsed.access_token.is_empty() {
                return Err(wire::unreadable(stage));
            }
            Ok(CachedToken {
                access_token: parsed.access_token,
                token_type: "Bearer".to_string(),
                expires_at,
            })
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    /// What `gcloud iam workload-identity-pools create-cred-config … --aws
    /// --enable-imdsv2 --service-account=…` writes, per AIP-4117's example
    /// (plus `universe_domain`, which current gcloud adds).
    pub(crate) fn gcloud_aws_config() -> Map<String, Value> {
        serde_json::json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/aws-pool/providers/aws-provider",
            "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/reader@lake-project.iam.gserviceaccount.com:generateAccessToken",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": {
                "environment_id": "aws1",
                "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone",
                "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials",
                "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15",
                "imdsv2_session_token_url": "http://169.254.169.254/latest/api/token"
            },
            "universe_domain": "googleapis.com"
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn gclouds_emitted_aws_config_is_accepted() {
        let spec = parse(&gcloud_aws_config()).unwrap();
        assert_eq!(
            spec.audience,
            "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/aws-pool/providers/aws-provider"
        );
        assert_eq!(
            spec.impersonate.as_deref(),
            Some("reader@lake-project.iam.gserviceaccount.com")
        );
        assert_eq!(spec.lifetime_secs, DEFAULT_LIFETIME_SECS);
    }

    #[test]
    fn a_config_without_impersonation_or_imdsv2_is_accepted() {
        let mut config = gcloud_aws_config();
        config.remove("service_account_impersonation_url");
        config["credential_source"]
            .as_object_mut()
            .unwrap()
            .remove("imdsv2_session_token_url");
        let spec = parse(&config).unwrap();
        assert_eq!(spec.impersonate, None);
    }

    #[test]
    fn the_impersonated_token_lifetime_is_read_and_bounded() {
        let mut config = gcloud_aws_config();
        config.insert(
            "service_account_impersonation".into(),
            serde_json::json!({"token_lifetime_seconds": 1800}),
        );
        assert_eq!(parse(&config).unwrap().lifetime_secs, 1800);
        for bad in [
            serde_json::json!({"token_lifetime_seconds": 60}),
            serde_json::json!({"token_lifetime_seconds": 86400}),
            serde_json::json!({"token_lifetime_seconds": "3600"}),
            serde_json::json!({"other": 1}),
            serde_json::json!(5),
        ] {
            config.insert("service_account_impersonation".into(), bad.clone());
            assert!(parse(&config).is_err(), "{bad} must be refused");
        }
    }

    /// Mutate the gcloud config with `edit`, and expect a refusal naming
    /// `field` — never the value that was refused.
    fn refused(field: &str, edit: impl FnOnce(&mut Map<String, Value>)) {
        let mut config = gcloud_aws_config();
        edit(&mut config);
        let err = parse(&config).unwrap_err();
        assert!(
            matches!(err, IcebergError::GoogleCredentialsInvalid(_)),
            "{field}: {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains(field), "{field}: {message}");
        assert!(
            !message.contains("evil"),
            "{field} echoed its value: {message}"
        );
    }

    #[test]
    fn each_field_outside_the_subset_is_refused_by_name() {
        refused("type", |c| {
            c.insert("type".into(), "service_account".into());
        });
        refused("subject_token_type", |c| {
            c.insert(
                "subject_token_type".into(),
                "urn:ietf:params:oauth:token-type:jwt".into(),
            );
        });
        refused("token_url", |c| {
            c.insert(
                "token_url".into(),
                "https://sts.evil.example/v1/token".into(),
            );
        });
        refused("token_url", |c| {
            c.remove("token_url");
        });
        refused("universe_domain", |c| {
            c.insert("universe_domain".into(), "evil.example".into());
        });
        refused("audience", |c| {
            c.insert(
                "audience".into(),
                "//iam.googleapis.com/locations/global/workforcePools/evil/providers/p".into(),
            );
        });
        refused("audience", |c| {
            c.insert(
                "audience".into(),
                "//iam.googleapis.com/projects/12/locations/global/workloadIdentityPools/evil/providers/p/../x".into(),
            );
        });
        refused("service_account_impersonation_url", |c| {
            c.insert(
                "service_account_impersonation_url".into(),
                "https://iamcredentials.evil.example/v1/projects/-/serviceAccounts/a@b.iam.gserviceaccount.com:generateAccessToken".into(),
            );
        });
        refused("service_account_impersonation_url", |c| {
            c.insert(
                "service_account_impersonation_url".into(),
                "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/evil/x@b.com:generateAccessToken".into(),
            );
        });
        refused("workforce_pool_user_project", |c| {
            c.insert("workforce_pool_user_project".into(), "evil".into());
        });
        refused("client_secret", |c| {
            c.insert("client_secret".into(), "evil".into());
        });
        refused("unknown field `extra`", |c| {
            c.insert("extra".into(), "evil".into());
        });
        refused("token_info_url", |c| {
            c.insert(
                "token_info_url".into(),
                "https://evil.example/introspect".into(),
            );
        });
        refused("quota_project_id", |c| {
            c.insert("quota_project_id".into(), "evil project!".into());
        });
    }

    #[test]
    fn credential_sources_other_than_aws_are_refused() {
        for source in [
            serde_json::json!({"file": "/var/run/evil/token"}),
            serde_json::json!({"url": "http://evil.example/token", "headers": {"Metadata": "True"}}),
            serde_json::json!({"executable": {"command": "/bin/evil"}}),
            serde_json::json!({"environment_id": "aws2"}),
            serde_json::json!({"environment_id": "aws1", "file": "/var/run/evil"}),
            serde_json::json!("aws1"),
        ] {
            refused("credential_source", |c| {
                c.insert("credential_source".into(), source);
            });
        }
        refused("credential_source", |c| {
            c.remove("credential_source");
        });
    }

    #[test]
    fn each_aws_url_must_be_googles_documented_default() {
        for field in [
            "url",
            "region_url",
            "regional_cred_verification_url",
            "imdsv2_session_token_url",
        ] {
            refused(&format!("credential_source.{field}"), |c| {
                c["credential_source"]
                    .as_object_mut()
                    .unwrap()
                    .insert(field.into(), "http://evil.example/latest".into());
            });
        }
    }
}
