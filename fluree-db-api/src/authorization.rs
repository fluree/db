//! Application-authored policy selection, separate from untrusted query options.
//!
//! Construct this only after authenticating a request and resolving its grants.
//! It deliberately does not implement `Deserialize`: receiving JSON that names
//! an authorization context does not make that context trusted. Network hosts
//! must verify their credential before constructing it.

use serde_json::{Map, Value};

use crate::{ApiError, GovernanceOptions, Result};

/// Immutable policy inputs selected by an authenticated host application.
///
/// This selects the policies to enforce; it does not itself grant ledger access
/// or bypass configured policy-override controls. The host must authorize every
/// ledger/source named by the request separately. A single instance applies the
/// same policy selection to all sources in the query.
#[derive(Debug, Clone)]
pub struct PolicyAuthorization {
    options: GovernanceOptions,
}

impl PolicyAuthorization {
    /// Select policies using trusted application inputs.
    ///
    /// Explicit policy classes select rules and identity binds `?$identity`,
    /// preserving the embedded application's grant-derived policy contract.
    /// Empty inputs mean default-deny, rather than the SDK's unrestricted
    /// no-policy shortcut. Use an explicit `default_allow: Some(true)` when the
    /// application has authorized unrestricted data access.
    pub fn from_trusted_options(mut options: GovernanceOptions) -> Self {
        if options.policy.as_ref().is_some_and(Value::is_null) {
            options.policy = None;
        }
        if !options.has_any_policy_inputs() {
            options.policy = Some(Value::Array(Vec::new()));
            options.default_allow = Some(false);
        }
        Self { options }
    }

    /// Policy inputs to pass to a view or transaction builder.
    pub fn options(&self) -> &GovernanceOptions {
        &self.options
    }

    /// Replace caller policy selection, retaining an explicit default-deny
    /// request as a narrowing of the application's default.
    pub fn constrain_options(&self, requested: &GovernanceOptions) -> GovernanceOptions {
        let mut options = self.options.clone();
        if requested.default_allow == Some(false) {
            options.default_allow = Some(false);
        }
        if !options.has_any_policy_inputs() {
            options.policy = Some(Value::Array(Vec::new()));
            options.default_allow = Some(false);
        }
        options
    }

    /// Install trusted policy inputs into a JSON-LD query or transaction.
    ///
    /// Non-policy options (tracking, formatting, etc.) are preserved. Source
    /// policy overrides are removed so they cannot replace the trusted global
    /// selection. Canonical nulls intentionally shadow policy defaults that an
    /// enclosing multi-query envelope or header merge might otherwise restore.
    pub fn apply_to_jsonld(&self, query: &mut Value) -> Result<()> {
        let requested =
            GovernanceOptions::from_json(query).map_err(|e| ApiError::query(e.to_string()))?;
        let options = self.constrain_options(&requested);
        let object = query
            .as_object_mut()
            .ok_or_else(|| ApiError::query("Authorized JSON-LD request must be an object"))?;

        remove_dataset_overrides(object);

        let value = object
            .entry("opts")
            .or_insert_with(|| Value::Object(Map::new()));
        // `from_json` permits null as an absent opts block.
        if value.is_null() {
            *value = Value::Object(Map::new());
        }
        let opts = value
            .as_object_mut()
            .ok_or_else(|| ApiError::query("Authorized request opts must be an object"))?;
        remove_dataset_overrides(opts);
        for alias in [
            "policy_class",
            "policyClass",
            "policy_values",
            "policyValues",
            "default_allow",
            "defaultAllow",
        ] {
            opts.remove(alias);
        }
        opts.insert("identity".into(), serde_json::to_value(&options.identity)?);
        opts.insert(
            "policy-class".into(),
            serde_json::to_value(&options.policy_class)?,
        );
        opts.insert("policy".into(), options.policy.unwrap_or(Value::Null));
        opts.insert(
            "policy-values".into(),
            serde_json::to_value(&options.policy_values)?,
        );
        opts.insert(
            "default-allow".into(),
            serde_json::to_value(options.default_allow)?,
        );
        Ok(())
    }
}

fn remove_dataset_overrides(object: &mut Map<String, Value>) {
    for field in ["from", "ledger", "to"] {
        if let Some(sources) = object.get_mut(field) {
            remove_source_overrides(sources);
        }
    }
    for field in ["fromNamed", "from-named"] {
        match object.get_mut(field) {
            Some(Value::Object(named)) => {
                for source in named.values_mut() {
                    remove_source_overrides(source);
                }
            }
            Some(sources) => remove_source_overrides(sources),
            None => {}
        }
    }
}

fn remove_source_overrides(sources: &mut Value) {
    match sources {
        Value::Object(source) => {
            source.remove("policy");
        }
        Value::Array(sources) => {
            for source in sources {
                remove_source_overrides(source);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn authorization_covers_source_forms_and_shadows_outer_defaults() {
        let auth = PolicyAuthorization::from_trusted_options(GovernanceOptions {
            identity: Some("http://example.org/employee".into()),
            ..Default::default()
        });
        let mut query = json!({
            "from": [{"@id": "db:main", "policy": {"default-allow": true}}],
            "fromNamed": {"policy": {"@id": "db:main", "policy": {"default-allow": true}}},
            "from-named": [{"@id": "db:main", "policy": {"default-allow": true}}],
            "opts": {"policyClass": ["ex:Manager"], "policy_values": {}, "defaultAllow": true, "meta": true,
                "from": {"@id": "db:main", "policy": {"default-allow": true}},
                "from-named": {"g": {"@id": "db:main", "policy": {"default-allow": true}}}
            },
            "where": {"@id": "?s", "policy": "?data"}
        });
        auth.apply_to_jsonld(&mut query).unwrap();
        let (spec, opts) = crate::query::helpers::parse_dataset_spec(&query).unwrap();
        assert!(spec
            .default_graphs
            .iter()
            .chain(spec.named_graphs.iter())
            .all(|s| s.policy_override.is_none()));
        assert_eq!(
            opts.identity.as_deref(),
            Some("http://example.org/employee")
        );
        assert!(opts.policy_class.is_none());
        assert!(opts.policy_values.is_none());
        assert_eq!(opts.default_allow, None);
        assert_eq!(query["opts"]["meta"], true);
        assert_eq!(query["where"]["policy"], "?data");
        let outer = json!({"policy-class": ["ex:Manager"], "policy": [{"f:allow": true}], "default-allow": true});
        let merged = crate::query::multi::merged_opts(Some(&outer), query.get("opts"));
        let opts = GovernanceOptions::from_json(&json!({"opts": merged})).unwrap();
        assert!(opts.policy_class.is_none() && opts.policy.is_none());
        assert_eq!(opts.default_allow, None);
    }

    #[test]
    fn empty_context_and_narrowed_unrestricted_context_engage_enforcement() {
        let deny = PolicyAuthorization::from_trusted_options(GovernanceOptions::default());
        assert!(deny.options().has_any_policy_inputs());
        assert!(!deny.options().effective_default_allow());
        let allow = PolicyAuthorization::from_trusted_options(GovernanceOptions {
            default_allow: Some(true),
            ..Default::default()
        });
        let narrowed = allow.constrain_options(&GovernanceOptions {
            default_allow: Some(false),
            ..Default::default()
        });
        assert!(narrowed.has_any_policy_inputs());
        assert!(!narrowed.effective_default_allow());
    }

    #[test]
    fn malformed_options_are_rejected_without_a_root_fallback() {
        let auth = PolicyAuthorization::from_trusted_options(GovernanceOptions::default());
        for mut query in [
            json!({"opts": true}),
            json!({"opts": {"policy-class": 1}}),
            json!([]),
        ] {
            assert!(auth.apply_to_jsonld(&mut query).is_err());
        }
    }
}
