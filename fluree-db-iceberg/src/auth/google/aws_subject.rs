//! The AWS identity that signs a Workload Identity Federation subject token.
//!
//! By default it is the host's own aws-config chain (env, profile, web
//! identity, ECS container credentials, EC2 IMDS), the same identity every
//! other ambient AWS read in Fluree uses, so WIF works on Lambda, ECS, EC2 and
//! EKS alike. That is a host identity: every tenant on a stack shares it, so it
//! is governed by the host-identity policy. A multi-tenant embedder instead
//! supplies per-tenant credentials (e.g. an AssumeRole whose session name is
//! the tenant), letting the customer's WIF provider condition on the tenant;
//! those are not host-signed and the policy does not gate them.

use async_trait::async_trait;

use crate::error::{IcebergError, Result};

/// AWS credentials that sign a WIF subject token, supplied by an embedder
/// through the identity context.
#[async_trait]
pub trait AwsSubjectCredentials: std::fmt::Debug + Send + Sync {
    /// A stable, non-secret name for whose credentials these are (e.g. a role
    /// ARN and session name). Part of the Google token-cache key, so two
    /// subjects never share a token: it must differ whenever the identity AWS
    /// would report differs.
    fn principal_key(&self) -> String;

    /// Current credentials. Called once per Google token exchange.
    async fn credentials(&self) -> Result<aws_credential_types::Credentials>;

    /// The AWS region the subject token is signed for, if these credentials
    /// know one; otherwise `AWS_REGION` is used.
    fn region(&self) -> Option<String>;
}

/// The host chain's failure to provide credentials, with every cause it
/// gives: `CredentialsError` says only "an error occurred while loading
/// credentials", and the reason (an expired SSO session, a missing profile)
/// is in its sources. Their `Display` text is the providers' diagnostics,
/// which name profiles, sessions and endpoints; a failed load holds no key
/// material to show, and no `Debug` rendering is used.
fn no_subject_credentials(
    error: &aws_credential_types::provider::error::CredentialsError,
) -> IcebergError {
    IcebergError::Credential(format!(
        "the host's AWS credential chain provided no credentials for the WIF subject token: {}",
        crate::error::concise_chain(error)
    ))
}

/// The host's aws-config default chain, loaded on first use.
#[derive(Debug, Default)]
pub(crate) struct HostAwsSubject {
    config: tokio::sync::OnceCell<aws_config::SdkConfig>,
}

/// The [`AwsSubjectCredentials::principal_key`] of the host chain.
pub(crate) const HOST_PRINCIPAL_KEY: &str = "host";

#[async_trait]
impl AwsSubjectCredentials for HostAwsSubject {
    fn principal_key(&self) -> String {
        HOST_PRINCIPAL_KEY.to_string()
    }

    async fn credentials(&self) -> Result<aws_credential_types::Credentials> {
        use aws_credential_types::provider::ProvideCredentials;
        let config = self
            .config
            .get_or_init(|| aws_config::defaults(aws_config::BehaviorVersion::latest()).load())
            .await;
        let provider = config.credentials_provider().ok_or_else(|| {
            IcebergError::Credential(
                "the host has no AWS credential chain to sign the WIF subject token".to_string(),
            )
        })?;
        provider
            .provide_credentials()
            .await
            .map_err(|e| no_subject_credentials(&e))
    }

    fn region(&self) -> Option<String> {
        self.config
            .get()
            .and_then(aws_config::SdkConfig::region)
            .map(ToString::to_string)
    }
}

#[cfg(test)]
mod tests {
    use aws_credential_types::provider::error::CredentialsError;

    /// The cause of an error, with a cause of its own.
    #[derive(Debug)]
    struct Cause(&'static str, Option<Box<Cause>>);

    impl std::fmt::Display for Cause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Cause {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1
                .as_deref()
                .map(|cause| cause as &(dyn std::error::Error + 'static))
        }
    }

    /// An expired SSO session says so: the provider error's own text is a
    /// generic "an error occurred while loading credentials", and the reason
    /// is in its sources, each shown once.
    #[test]
    fn the_subject_credential_error_names_its_causes() {
        let expired = CredentialsError::provider_error(Cause(
            "failed to load SSO token",
            Some(Box::new(Cause(
                "Token has expired and refresh failed",
                None,
            ))),
        ));
        let shown = super::no_subject_credentials(&expired).to_string();
        assert!(
            shown.contains(
                "an error occurred while loading credentials: failed to load SSO token: Token \
                 has expired and refresh failed"
            ),
            "{shown}"
        );
        assert_eq!(shown.matches("Token has expired").count(), 1, "{shown}");
    }
}
