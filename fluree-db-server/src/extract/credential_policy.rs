//! Resolve a verified credential into one concrete policy selection per request.
use fluree_db_api::{ApiError, GovernanceOptions, PolicyAuthorization};
use serde_json::Value;

use crate::error::{Result, ServerError};

#[derive(Debug, Clone)]
pub enum CredentialPolicy {
    Fixed(PolicyAuthorization),
    Request,
    ScopeOnly,
}

impl CredentialPolicy {
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Fixed(_) => "fixed",
            Self::Request => "request-selected",
            Self::ScopeOnly => "scope-only",
        }
    }

    pub fn fixed_options(&self) -> Option<&GovernanceOptions> {
        match self {
            Self::Fixed(auth) => Some(auth.options()),
            _ => None,
        }
    }

    pub fn resolve_options(&self, requested: &GovernanceOptions) -> Result<GovernanceOptions> {
        match self {
            Self::Request => Ok(PolicyAuthorization::from_trusted_options(requested.clone())
                .options()
                .clone()),
            Self::Fixed(auth) => {
                validate_selection(requested, auth.options(), true)?;
                Ok(auth.constrain_options(requested))
            }
            Self::ScopeOnly => {
                validate_selection(requested, &GovernanceOptions::default(), true)?;
                Ok(GovernanceOptions {
                    default_allow: requested.default_allow,
                    ..Default::default()
                })
            }
        }
    }

    pub fn apply_to_jsonld(&self, query: &mut Value) -> Result<()> {
        let requested = GovernanceOptions::from_json(query)
            .map_err(|e| ServerError::bad_request(e.to_string()))?;
        let options = self.resolve_options(&requested)?;
        if !matches!(self, Self::Request) {
            // Check each spelling, including aliases shadowed by header defaults.
            let empty = GovernanceOptions::default();
            let bound = self.fixed_options().unwrap_or(&empty);
            if let Some(opts) = query.get("opts").and_then(Value::as_object) {
                for key in [
                    "identity",
                    "policy-class",
                    "policy_class",
                    "policyClass",
                    "policy",
                    "policy-values",
                    "policy_values",
                    "policyValues",
                    "default-allow",
                    "default_allow",
                    "defaultAllow",
                ] {
                    if let Some(value) = opts.get(key) {
                        let one = GovernanceOptions::from_json(
                            &serde_json::json!({"opts": {key: value}}),
                        )
                        .map_err(|e| ServerError::bad_request(e.to_string()))?;
                        validate_selection(&one, bound, true)?;
                    }
                }
            }
        }

        // One selection applies to all sources. Never silently discard a
        // source's conflicting selection, including a source-only narrowing.
        validate_sources(query, &options)?;
        if !matches!(self, Self::ScopeOnly) || options.has_any_policy_inputs() {
            PolicyAuthorization::from_trusted_options(options).apply_to_jsonld(query)?;
        }
        Ok(())
    }
}

fn conflict(field: &str) -> ServerError {
    ApiError::http(
        403,
        format!("Credential does not permit policy selection: conflicting {field}"),
    )
    .into()
}

fn validate_selection(
    requested: &GovernanceOptions,
    bound: &GovernanceOptions,
    narrow: bool,
) -> Result<()> {
    if requested.identity.is_some() && requested.identity != bound.identity {
        return Err(conflict("identity"));
    }
    if let Some(classes) = &requested.policy_class {
        // Class order and duplicates do not change the selected policy set.
        let equal = bound.policy_class.as_ref().is_some_and(|other| {
            classes.iter().all(|c| other.contains(c)) && other.iter().all(|c| classes.contains(c))
        });
        if !equal {
            return Err(conflict("policy-class"));
        }
    }
    if requested.policy.as_ref().is_some_and(|p| !p.is_null()) && requested.policy != bound.policy {
        return Err(conflict("policy"));
    }
    if requested.policy_values.is_some() && requested.policy_values != bound.policy_values {
        return Err(conflict("policy-values"));
    }
    if requested.default_allow.is_some()
        && !(narrow && requested.default_allow == Some(false))
        && requested.default_allow != bound.default_allow
    {
        return Err(conflict("default-allow"));
    }
    Ok(())
}

fn validate_sources(query: &Value, bound: &GovernanceOptions) -> Result<()> {
    for object in [Some(query), query.get("opts")].into_iter().flatten() {
        for key in ["from", "ledger", "to"] {
            if let Some(source) = object.get(key) {
                validate_source(source, bound)?;
            }
        }
        for key in ["fromNamed", "from-named"] {
            if let Some(source) = object.get(key) {
                if let Some(named) = source.as_object() {
                    for source in named.values() {
                        validate_source(source, bound)?;
                    }
                } else {
                    validate_source(source, bound)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_source(source: &Value, bound: &GovernanceOptions) -> Result<()> {
    if let Some(sources) = source.as_array() {
        for source in sources {
            validate_source(source, bound)?;
        }
    } else if let Some(policy) = source.get("policy") {
        let opts = GovernanceOptions::from_json(&serde_json::json!({"opts": policy}))
            .map_err(|e| ServerError::bad_request(e.to_string()))?;
        validate_selection(&opts, bound, false)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_selection_is_explicit_and_scope_only_preserves_defaults() {
        let empty = GovernanceOptions::default();
        let selected = CredentialPolicy::Request.resolve_options(&empty).unwrap();
        assert!(selected.has_any_policy_inputs());
        assert_eq!(selected.default_allow, Some(false));
        assert!(!CredentialPolicy::ScopeOnly
            .resolve_options(&empty)
            .unwrap()
            .has_any_policy_inputs());
        let allow = GovernanceOptions {
            default_allow: Some(true),
            ..empty
        };
        assert_eq!(
            CredentialPolicy::Request
                .resolve_options(&allow)
                .unwrap()
                .default_allow,
            Some(true)
        );
        assert!(CredentialPolicy::ScopeOnly.resolve_options(&allow).is_err());
    }

    #[test]
    fn fixed_context_accepts_echoes_and_narrowing_but_rejects_conflicts() {
        let auth = CredentialPolicy::Fixed(PolicyAuthorization::from_trusted_options(
            GovernanceOptions {
                identity: Some("https://example.org/user".into()),
                policy_class: Some(vec![
                    "https://example.org/A".into(),
                    "https://example.org/B".into(),
                ]),
                default_allow: Some(true),
                ..Default::default()
            },
        ));
        for opts in [
            json!({}),
            json!({"identity": "https://example.org/user"}),
            json!({"policyClass": ["https://example.org/B", "https://example.org/A"]}),
            json!({"default-allow": false}),
        ] {
            let mut query = json!({"opts": opts});
            auth.apply_to_jsonld(&mut query).unwrap();
            assert_eq!(query["opts"]["identity"], "https://example.org/user");
        }
        for mut query in [
            json!({"opts": {"identity": "https://example.org/other"}}),
            json!({"opts": {"policy-class": ["https://example.org/A", "https://example.org/B"], "policyClass": ["https://example.org/Admin"]}}),
            json!({"opts": {"policy": [{"f:allow": true}]}}),
            json!({"opts": {"policy-values": {"?$identity": "someone else"}}}),
            json!({"from": [{"@id": "db:main", "policy": {"default-allow": false}}]}),
        ] {
            let error = auth.apply_to_jsonld(&mut query).unwrap_err();
            assert!(matches!(
                error,
                ServerError::Api(ApiError::Http { status: 403, .. })
            ));
        }
    }
}
