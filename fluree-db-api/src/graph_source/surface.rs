//! The one codec from a surface's graph-source input to the persisted config.
//!
//! The HTTP routes, the CLI and the Python package all collect a graph
//! source's catalog, auth and storage identity, and every one of them reaches
//! the persisted serde types ([`CatalogConfig`], [`IoConfig`], [`AuthConfig`],
//! [`StorageAuth`], the Delta configs) through this module:
//!
//! - **Structured input is the stored JSON.** A request may give `catalog` and
//!   `io` (Iceberg), `auth` (SQL), or `unity` and `io` (Delta) exactly as a
//!   stored record spells them. [`strict`] parses each and refuses any key the
//!   persisted type does not know, by comparing the input with what the parsed
//!   value serializes back to, so the check can never drift from the type.
//! - **Legacy flat fields** (`catalog_uri`, `auth_bearer_env`, …) go through one
//!   converter per kind, and catalog auth through one converter for all kinds
//!   ([`CatalogAuthFields`]). A request uses one form, never both.
//! - **Every [`ConfigValue`] is checked** against the caller's [`SecretRules`]:
//!   which environment variables it may name, whether a secret reference can be
//!   resolved, and that a service-account key is never a literal.
//!
//! Nothing here reads the network, and the environment only through
//! [`SecretRules::server`].

use fluree_db_iceberg::auth::{AuthConfig, ConfigValueField, GoogleCredentials, ValueClass};
use fluree_db_iceberg::config::{CatalogConfig, IoConfig, StorageAuth};
use fluree_db_iceberg::ConfigValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::config::{
    CatalogModeArgs, CatalogModeError, IcebergConnectionConfig, IcebergCreateConfig,
};

/// The environment variable in which a server's operator lists the variables
/// (comma-separated) a request may name as a secret.
pub const SECRET_ENV_VARS_ENV: &str = "FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS";

// =============================================================================
// Errors
// =============================================================================

/// Why a surface's graph-source input was refused: HTTP 400,
/// `err:api/InvalidGraphSourceConfig`.
///
/// `field` is the input's own name or path (`auth_bearer_env`,
/// `catalog.auth.token`). [`Self::message`] renders it, and every other field
/// the error mentions, in a surface's own spelling (the CLI's flags).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSourceInputError {
    pub field: String,
    pub kind: InputErrorKind,
}

/// What is wrong with [`GraphSourceInputError::field`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputErrorKind {
    /// Not a field this input has.
    Unknown,
    /// A flat field in a request that gives the structured `structured`.
    Mixed { structured: String },
    /// Means nothing without one of `needs`.
    Incomplete { needs: Vec<String> },
    /// Says the same thing as `other`, or something it contradicts.
    Conflict { other: String },
    /// A second way to authenticate (or a second credential), beside `other`.
    TwoWays { other: String },
    /// Names environment variable `name`, which the operator does not list in
    /// [`SECRET_ENV_VARS_ENV`].
    EnvNotAllowed { name: String },
    /// A secret reference, and this instance has no resolver for one.
    SecretRefUnsupported,
    /// A Google service-account key given literally.
    LiteralKey,
    /// The Google metadata server's URL, which only the operator sets.
    MetadataUrl,
    /// An environment-variable reference with a `default_val` or
    /// `java_property`, or naming no variable.
    UnsupportedReference,
    /// A malformed value.
    Invalid { reason: String },
    /// Something the catalog mode refuses (see [`CatalogModeError`]).
    Mode(CatalogModeError),
}

impl GraphSourceInputError {
    pub fn new(field: impl Into<String>, kind: InputErrorKind) -> Self {
        Self {
            field: field.into(),
            kind,
        }
    }

    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new(
            field,
            InputErrorKind::Invalid {
                reason: reason.into(),
            },
        )
    }

    pub fn incomplete(field: impl Into<String>, needs: &[&str]) -> Self {
        Self::new(
            field,
            InputErrorKind::Incomplete {
                needs: needs.iter().map(|n| (*n).to_string()).collect(),
            },
        )
    }

    pub fn conflict(field: impl Into<String>, other: impl Into<String>) -> Self {
        Self::new(
            field,
            InputErrorKind::Conflict {
                other: other.into(),
            },
        )
    }

    fn two_ways(field: impl Into<String>, other: impl Into<String>) -> Self {
        Self::new(
            field,
            InputErrorKind::TwoWays {
                other: other.into(),
            },
        )
    }

    /// The message, with every field name rendered by `spell`.
    pub fn message(&self, spell: impl Fn(&str) -> String) -> String {
        let field = spell(&self.field);
        match &self.kind {
            InputErrorKind::Unknown => format!("{field} is not a field of this request"),
            InputErrorKind::Mixed { structured } => format!(
                "{field} is a flat connection field, and this request gives the structured \
                 {}: give the connection one way",
                spell(structured)
            ),
            InputErrorKind::Incomplete { needs } => format!(
                "{field} needs {}",
                needs
                    .iter()
                    .map(|n| spell(n))
                    .collect::<Vec<_>>()
                    .join(" or ")
            ),
            InputErrorKind::Conflict { other } => format!(
                "{field} and {} say the same thing two ways, or contradict each other: give one",
                spell(other)
            ),
            InputErrorKind::TwoWays { other } => format!(
                "{field} and {} are two ways to authenticate: give one",
                spell(other)
            ),
            InputErrorKind::EnvNotAllowed { name } => format!(
                "{field} names environment variable '{name}', which this server does not list \
                 in {SECRET_ENV_VARS_ENV}"
            ),
            InputErrorKind::SecretRefUnsupported => format!(
                "{field} is a secret reference (secret_ref), and this instance has no secret \
                 resolver to resolve it: name an environment variable instead"
            ),
            InputErrorKind::LiteralKey => format!(
                "{field}: a service-account key cannot be given literally, because it would be \
                 stored with the graph source; name an environment variable that holds it"
            ),
            InputErrorKind::MetadataUrl => format!(
                "{field}: the Google metadata server's URL is not set per source (an operator \
                 sets {} for tests)",
                fluree_db_iceberg::auth::google::METADATA_URL_ENV
            ),
            InputErrorKind::UnsupportedReference => format!(
                "{field}: an environment-variable reference names its variable (env_var) and \
                 nothing else; default_val and java_property are not accepted"
            ),
            InputErrorKind::Invalid { reason } => format!("{field}: {reason}"),
            InputErrorKind::Mode(e) => e.message(spell),
        }
    }
}

impl std::fmt::Display for GraphSourceInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message(str::to_string))
    }
}

impl std::error::Error for GraphSourceInputError {}

impl From<CatalogModeError> for GraphSourceInputError {
    fn from(e: CatalogModeError) -> Self {
        let field = match &e {
            CatalogModeError::UnknownMode(_) => "mode",
            CatalogModeError::MissingField { field, .. }
            | CatalogModeError::NotForMode { field, .. } => field,
        };
        Self::new(field, InputErrorKind::Mode(e))
    }
}

// =============================================================================
// ConfigValue rules
// =============================================================================

/// How a surface may give the values a config resolves when it is used.
#[derive(Debug, Clone)]
pub struct SecretRules {
    /// The environment variables a value may name; `None` accepts any.
    env: Option<Vec<String>>,
    /// Whether a `secret_ref` is accepted.
    secret_refs: bool,
}

impl SecretRules {
    /// A caller naming variables of its own process (the CLI, Python, an
    /// embedder): any variable. A secret reference only when `secret_refs`,
    /// i.e. the instance has a resolver.
    pub fn local(secret_refs: bool) -> Self {
        Self {
            env: None,
            secret_refs,
        }
    }

    /// A request naming variables of another process: only those in `list`
    /// (comma-separated).
    pub fn listed(list: &str, secret_refs: bool) -> Self {
        Self {
            env: Some(
                list.split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .collect(),
            ),
            secret_refs,
        }
    }

    /// A request to a server, which names variables of the server's process:
    /// only those its operator lists in [`SECRET_ENV_VARS_ENV`], since the
    /// request also chooses where the secret is sent. Secret references only
    /// when `fluree` has a resolver.
    pub fn server(fluree: &crate::Fluree) -> Self {
        Self::listed(
            &std::env::var(SECRET_ENV_VARS_ENV).unwrap_or_default(),
            fluree.has_secret_resolver(),
        )
    }

    fn check(&self, path: &str, field: &ConfigValueField<'_>) -> Result<(), GraphSourceInputError> {
        self.check_value(path, field.value, field.class)
    }

    /// Check one value at `path`: a flat field's own name, or a stored path.
    pub fn check_value(
        &self,
        path: &str,
        value: &ConfigValue,
        class: ValueClass,
    ) -> Result<(), GraphSourceInputError> {
        use InputErrorKind as K;
        let refuse = |kind| Err(GraphSourceInputError::new(path, kind));
        match value {
            ConfigValue::Literal(_) if class == ValueClass::ServiceAccountKey => {
                refuse(K::LiteralKey)
            }
            ConfigValue::Literal(_) => Ok(()),
            ConfigValue::SecretRef { .. } if !self.secret_refs => refuse(K::SecretRefUnsupported),
            ConfigValue::SecretRef { .. } => Ok(()),
            ConfigValue::Dynamic {
                env_var: Some(name),
                java_property: None,
                default_val: None,
            } if !name.trim().is_empty() => match &self.env {
                Some(listed) if !listed.iter().any(|l| l == name) => {
                    refuse(K::EnvNotAllowed { name: name.clone() })
                }
                _ => Ok(()),
            },
            ConfigValue::Dynamic { .. } => refuse(K::UnsupportedReference),
        }
    }

    fn check_all(
        &self,
        prefix: &str,
        fields: Vec<ConfigValueField<'_>>,
    ) -> Result<(), GraphSourceInputError> {
        fields
            .iter()
            .try_for_each(|f| self.check(&join(prefix, &f.path), f))
    }

    /// Check catalog or endpoint auth at `prefix` (`catalog.auth`, `auth`).
    pub fn check_auth(&self, prefix: &str, auth: &AuthConfig) -> Result<(), GraphSourceInputError> {
        if let AuthConfig::GoogleMetadata {
            metadata_url: Some(_),
            ..
        } = auth
        {
            return Err(GraphSourceInputError::new(
                join(prefix, "metadata_url"),
                InputErrorKind::MetadataUrl,
            ));
        }
        self.check_all(prefix, auth.config_values())
    }

    /// Check a storage identity at `prefix` (`io.storage_auth`).
    pub fn check_storage_auth(
        &self,
        prefix: &str,
        storage_auth: &StorageAuth,
    ) -> Result<(), GraphSourceInputError> {
        self.check_all(prefix, storage_auth.config_values())
    }

    /// Check everything an Iceberg connection resolves at use time.
    pub fn check_iceberg(
        &self,
        conn: &IcebergConnectionConfig,
    ) -> Result<(), GraphSourceInputError> {
        if let super::config::CatalogMode::Rest(rest) = &conn.catalog_mode {
            self.check_auth("catalog.auth", &rest.auth)?;
        }
        if let Some(storage_auth) = &conn.io.storage_auth {
            self.check_storage_auth("io.storage_auth", storage_auth)?;
        }
        Ok(())
    }
}

fn join(prefix: &str, path: &str) -> String {
    match (prefix.is_empty(), path.is_empty()) {
        (true, _) => path.to_string(),
        (_, true) => prefix.to_string(),
        _ => format!("{prefix}.{path}"),
    }
}

// =============================================================================
// Strict parsing
// =============================================================================

/// Parse `value` (at `field`) as `T`, refusing any key `T` does not know.
///
/// serde ignores unknown keys, and the persisted types must keep doing so to
/// read records older versions wrote, so strictness cannot be a
/// `deny_unknown_fields`. Instead the parsed value is serialized back, and a
/// key of the input that is not in the result is unknown (a null, `{}` or `[]`
/// carries nothing and is skipped, since serialization may omit a default).
pub(crate) fn strict<T: DeserializeOwned + Serialize>(
    field: &str,
    value: Value,
) -> Result<T, GraphSourceInputError> {
    let parsed: T = serde_json::from_value(value.clone())
        .map_err(|e| GraphSourceInputError::invalid(field, scrub(&e.to_string())))?;
    let canonical = serde_json::to_value(&parsed)
        .map_err(|e| GraphSourceInputError::invalid(field, scrub(&e.to_string())))?;
    match unknown_key(&value, &canonical) {
        Some(path) => Err(GraphSourceInputError::new(
            join(field, &path),
            InputErrorKind::Unknown,
        )),
        None => Ok(parsed),
    }
}

fn carries_nothing(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

/// The first key path of `input` that `canonical` lacks.
fn unknown_key(input: &Value, canonical: &Value) -> Option<String> {
    match (input, canonical) {
        (Value::Object(input), Value::Object(canonical)) => {
            input.iter().find_map(|(key, value)| {
                if carries_nothing(value) {
                    return None;
                }
                match canonical.get(key) {
                    None => Some(key.clone()),
                    Some(known) => unknown_key(value, known).map(|path| format!("{key}.{path}")),
                }
            })
        }
        (Value::Array(input), Value::Array(canonical)) => input
            .iter()
            .zip(canonical)
            .enumerate()
            .find_map(|(i, (value, known))| {
                unknown_key(value, known).map(|path| format!("{i}.{path}"))
            }),
        _ => None,
    }
}

/// A serde message with every quoted string replaced: serde quotes the value it
/// could not parse, and that value may be a secret in the wrong field.
fn scrub(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut quoted = false;
    for c in message.chars() {
        if c == '"' {
            if quoted {
                out.push_str("…\"");
            } else {
                out.push('"');
            }
            quoted = !quoted;
        } else if !quoted {
            out.push(c);
        }
    }
    out
}

/// The keys `T`'s default serializes to: every field of a flat form.
fn keys_of<T: Default + Serialize>() -> Vec<String> {
    match serde_json::to_value(T::default()) {
        Ok(Value::Object(map)) => map.into_iter().map(|(k, _)| k).collect(),
        _ => Vec::new(),
    }
}

/// Split a request into its structured part (the keys in `structured`) and
/// the rest, refusing a flat key beside a structured one and a key that is in
/// neither form. A key whose value is `null` is absent: clients that send
/// every field send the unused ones as `null`, and a null says nothing.
fn split_forms(
    mut input: Map<String, Value>,
    structured: &[&str],
    flat: &[String],
) -> Result<Form, GraphSourceInputError> {
    input.retain(|_, value| !value.is_null());
    let Some(given) = structured.iter().find(|k| input.contains_key(**k)).copied() else {
        return Ok(Form::Flat(input));
    };
    if let Some(key) = input.keys().find(|k| !structured.contains(&k.as_str())) {
        let kind = if flat.iter().any(|f| f == key) {
            InputErrorKind::Mixed {
                structured: given.to_string(),
            }
        } else {
            InputErrorKind::Unknown
        };
        return Err(GraphSourceInputError::new(key.clone(), kind));
    }
    let parts = structured
        .iter()
        .map(|k| ((*k).to_string(), input.remove(*k)))
        .collect();
    Ok(Form::Structured(parts))
}

enum Form {
    Structured(Vec<(String, Option<Value>)>),
    Flat(Map<String, Value>),
}

impl Form {
    fn take(parts: &mut [(String, Option<Value>)], key: &str) -> Option<Value> {
        parts
            .iter_mut()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.take())
    }
}

// =============================================================================
// Catalog auth, as the flat surfaces spell it
// =============================================================================

/// What a flat surface collects for catalog (or endpoint) auth: the legacy
/// HTTP fields and the CLI's flags. One converter, [`Self::into_auth`], decides
/// the mode for every kind, so the rules cannot differ between them.
///
/// A secret or Google credential carries the field it came from
/// (`auth_bearer` or `auth_bearer_env`), so a refusal names it.
#[derive(Debug, Clone, Default)]
pub struct CatalogAuthFields {
    pub bearer: Option<(&'static str, ConfigValue)>,
    pub oauth2_token_url: Option<String>,
    pub oauth2_client_id: Option<String>,
    pub oauth2_client_secret: Option<(&'static str, ConfigValue)>,
    pub oauth2_scope: Option<String>,
    pub oauth2_audience: Option<String>,
    /// Each Google credential source given (`auth_google_metadata`,
    /// `auth_google_adc`, `auth_google_service_account_env`,
    /// `auth_google_wif_config`).
    pub google: Vec<(&'static str, GoogleCredentials)>,
    pub google_scopes: Option<String>,
    pub google_quota_project: Option<String>,
}

/// Which catalog the auth is for: the modes and defaults that differ.
#[derive(Debug, Clone, Copy)]
pub enum AuthProfile<'a> {
    /// An Iceberg REST catalog or a SQL endpoint. OAuth2 needs its token URL
    /// and secret; the client id defaults to empty (Snowflake Horizon's
    /// `session:role:` exchange requires one).
    Catalog,
    /// A Unity Catalog workspace at `uri`. Authentication is required, OAuth2
    /// needs the client id and secret, the token URL defaults to the
    /// workspace's own and the scope to `all-apis`. No Google modes.
    Unity { uri: &'a str },
}

/// A secret given literally (`literal_field`) or as the name of an
/// environment variable (`env_field`), but not both.
pub fn secret_field(
    literal_field: &'static str,
    literal: Option<String>,
    env_field: &'static str,
    env: Option<String>,
) -> Result<Option<(&'static str, ConfigValue)>, GraphSourceInputError> {
    match (literal, env) {
        (Some(_), Some(_)) => Err(GraphSourceInputError::conflict(literal_field, env_field)),
        (Some(value), None) => Ok(Some((literal_field, ConfigValue::literal(value)))),
        (None, Some(name)) => Ok(Some((env_field, ConfigValue::from_env(name)))),
        (None, None) => Ok(None),
    }
}

const GOOGLE_SOURCES: [&str; 4] = [
    "auth_google_metadata",
    "auth_google_adc",
    "auth_google_service_account_env",
    "auth_google_wif_config",
];

impl CatalogAuthFields {
    /// Check each value under the field it came from (`auth_bearer_env`
    /// rather than `catalog.auth.token`), so a refusal names what the caller
    /// sent.
    pub fn check(&self, rules: &SecretRules) -> Result<(), GraphSourceInputError> {
        for (field, value) in [&self.bearer, &self.oauth2_client_secret]
            .into_iter()
            .flatten()
        {
            rules.check_value(field, value, ValueClass::Secret)?;
        }
        for (field, credentials) in &self.google {
            for value in credentials.config_values() {
                rules.check_value(field, value.value, value.class)?;
            }
        }
        Ok(())
    }

    /// Every field given, in a fixed order: for a mode that takes no catalog
    /// auth to refuse rather than ignore.
    pub fn given(&self) -> Vec<&'static str> {
        let mut given = Vec::new();
        if let Some((field, _)) = &self.bearer {
            given.push(*field);
        }
        let optional = [
            ("oauth2_token_url", self.oauth2_token_url.is_some()),
            ("oauth2_client_id", self.oauth2_client_id.is_some()),
        ];
        given.extend(optional.iter().filter(|(_, g)| *g).map(|(f, _)| *f));
        if let Some((field, _)) = &self.oauth2_client_secret {
            given.push(*field);
        }
        let optional = [
            ("oauth2_scope", self.oauth2_scope.is_some()),
            ("oauth2_audience", self.oauth2_audience.is_some()),
        ];
        given.extend(optional.iter().filter(|(_, g)| *g).map(|(f, _)| *f));
        given.extend(self.google.iter().map(|(field, _)| *field));
        let optional = [
            ("auth_google_scopes", self.google_scopes.is_some()),
            (
                "auth_google_quota_project",
                self.google_quota_project.is_some(),
            ),
        ];
        given.extend(optional.iter().filter(|(_, g)| *g).map(|(f, _)| *f));
        given
    }

    /// The one auth these fields describe, or a refusal naming the field:
    /// two modes at once, or a mode missing what it needs.
    pub fn into_auth(self, profile: AuthProfile<'_>) -> Result<AuthConfig, GraphSourceInputError> {
        let oauth2: Vec<&'static str> = self
            .given()
            .into_iter()
            .filter(|f| f.starts_with("oauth2_"))
            .collect();
        let google: Vec<&'static str> = self
            .given()
            .into_iter()
            .filter(|f| f.starts_with("auth_google_"))
            .collect();
        let modes: Vec<&'static str> = [
            self.bearer.as_ref().map(|(f, _)| *f),
            oauth2.first().copied(),
            google.first().copied(),
        ]
        .into_iter()
        .flatten()
        .collect();
        if let [first, second, ..] = modes.as_slice() {
            return Err(GraphSourceInputError::two_ways(*second, *first));
        }

        if let Some((_, token)) = self.bearer {
            return Ok(AuthConfig::Bearer { token });
        }
        if !oauth2.is_empty() {
            return self.oauth2(profile, &oauth2);
        }
        if !google.is_empty() {
            return self.google(profile, &google);
        }
        match profile {
            AuthProfile::Catalog => Ok(AuthConfig::None),
            AuthProfile::Unity { .. } => Err(GraphSourceInputError::incomplete(
                "unity_uri",
                &["auth_bearer", "oauth2_client_id"],
            )),
        }
    }

    fn oauth2(
        self,
        profile: AuthProfile<'_>,
        given: &[&'static str],
    ) -> Result<AuthConfig, GraphSourceInputError> {
        let secret_fields = ["oauth2_client_secret", "oauth2_client_secret_env"];
        let Some((_, client_secret)) = self.oauth2_client_secret else {
            return Err(GraphSourceInputError::incomplete(given[0], &secret_fields));
        };
        match profile {
            AuthProfile::Catalog => {
                let Some(token_url) = self.oauth2_token_url else {
                    return Err(GraphSourceInputError::incomplete(
                        given[0],
                        &["oauth2_token_url"],
                    ));
                };
                Ok(AuthConfig::OAuth2ClientCredentials {
                    token_url,
                    client_id: ConfigValue::literal(self.oauth2_client_id.unwrap_or_default()),
                    client_secret,
                    scope: self.oauth2_scope,
                    audience: self.oauth2_audience,
                })
            }
            AuthProfile::Unity { uri } => {
                let Some(client_id) = self.oauth2_client_id else {
                    return Err(GraphSourceInputError::incomplete(
                        given[0],
                        &["oauth2_client_id"],
                    ));
                };
                Ok(AuthConfig::OAuth2ClientCredentials {
                    token_url: self
                        .oauth2_token_url
                        .unwrap_or_else(|| format!("{}/oidc/v1/token", uri.trim_end_matches('/'))),
                    client_id: ConfigValue::literal(client_id),
                    client_secret,
                    scope: Some(self.oauth2_scope.unwrap_or_else(|| "all-apis".to_string())),
                    audience: self.oauth2_audience,
                })
            }
        }
    }

    fn google(
        mut self,
        profile: AuthProfile<'_>,
        given: &[&'static str],
    ) -> Result<AuthConfig, GraphSourceInputError> {
        if let AuthProfile::Unity { .. } = profile {
            return Err(GraphSourceInputError::invalid(
                given[0],
                "Unity Catalog takes a bearer token or OAuth2 client credentials, not a Google \
                 identity",
            ));
        }
        if self.google.len() > 1 {
            return Err(GraphSourceInputError::two_ways(
                self.google[1].0,
                self.google[0].0,
            ));
        }
        let Some((field, credentials)) = self.google.pop() else {
            return Err(GraphSourceInputError::incomplete(given[0], &GOOGLE_SOURCES));
        };
        // The legacy `auth_google_metadata` keeps writing the record it always
        // wrote; anything it could not say needs the `google` shape.
        if field == "auth_google_metadata" && self.google_quota_project.is_none() {
            return Ok(AuthConfig::GoogleMetadata {
                scopes: self.google_scopes,
                metadata_url: None,
            });
        }
        Ok(AuthConfig::Google {
            credentials,
            scopes: self.google_scopes,
            quota_project: self.google_quota_project,
        })
    }
}

// =============================================================================
// Storage identity, as the flat surfaces spell it
// =============================================================================

/// What a flat surface collects for a source's storage identity (the CLI's
/// `--storage-identity` and `--storage-google-*`).
#[derive(Debug, Clone, Default)]
pub struct StorageAuthFields {
    /// `vended`, `ambient`, `catalog` or `google`.
    pub identity: Option<String>,
    /// Each Google credential source given (`storage_google_metadata`, …).
    pub google: Vec<(&'static str, GoogleCredentials)>,
    pub google_quota_project: Option<String>,
}

impl StorageAuthFields {
    /// Whether any field is given.
    pub fn is_empty(&self) -> bool {
        self.identity.is_none() && self.google.is_empty() && self.google_quota_project.is_none()
    }

    /// The storage identity these fields describe (`None` when none is
    /// given). Google credentials alone mean `google`; with another identity
    /// they contradict it.
    pub fn into_storage_auth(mut self) -> Result<Option<StorageAuth>, GraphSourceInputError> {
        let google_given = !self.google.is_empty() || self.google_quota_project.is_some();
        let first_google = self
            .google
            .first()
            .map(|(f, _)| *f)
            .unwrap_or("storage_google_quota_project");
        let identity = match self.identity.as_deref() {
            None if !google_given => return Ok(None),
            None | Some("google") => "google",
            Some(other @ ("vended" | "ambient" | "catalog")) if google_given => {
                return Err(GraphSourceInputError::conflict(
                    first_google,
                    format!("storage_identity {other}"),
                ));
            }
            Some("vended") => return Ok(Some(StorageAuth::Vended)),
            Some("ambient") => return Ok(Some(StorageAuth::Ambient)),
            Some("catalog") => return Ok(Some(StorageAuth::Catalog)),
            Some(other) => {
                return Err(GraphSourceInputError::invalid(
                    "storage_identity",
                    format!("'{other}' is not vended, ambient, catalog or google"),
                ))
            }
        };
        debug_assert_eq!(identity, "google");
        if self.google.len() > 1 {
            return Err(GraphSourceInputError::two_ways(
                self.google[1].0,
                self.google[0].0,
            ));
        }
        let Some((_, credentials)) = self.google.pop() else {
            return Err(GraphSourceInputError::incomplete(
                "storage_identity",
                &[
                    "storage_google_metadata",
                    "storage_google_adc",
                    "storage_google_service_account_env",
                    "storage_google_wif_config",
                ],
            ));
        };
        Ok(Some(StorageAuth::Google {
            credentials,
            quota_project: self.google_quota_project,
        }))
    }
}

/// The storage identity a surface defaults to for a REST catalog's
/// `warehouse` when the caller names none: `catalog` for BigLake federation
/// (`bq://…`, which never vends; the catalog's Google identity reads the
/// bucket, as Google's own model has it), `vended` for a BigLake runtime
/// catalog (`bl://…`). `None` keeps what `vended_credentials` says. Only
/// surfaces call this; the engine never infers a storage identity.
pub fn default_storage_auth(warehouse: Option<&str>) -> Option<StorageAuth> {
    match warehouse? {
        w if w.starts_with("bq://") => Some(StorageAuth::Catalog),
        w if w.starts_with("bl://") => Some(StorageAuth::Vended),
        _ => None,
    }
}

/// The Google Cloud project a BigLake `warehouse` names
/// (`bq://projects/<P>/…` or `bl://projects/<P>/…`), which a surface defaults
/// a Google catalog identity's quota project to when the caller names none:
/// BigLake bills every request, whatever the identity, to the project named
/// in `x-goog-user-project`, and refuses one that names none. Only the CLI
/// and Python call this; the engine and the HTTP routes never infer a quota
/// project. It bills the catalog only, never storage reads.
pub fn default_google_quota_project(warehouse: Option<&str>) -> Option<String> {
    let warehouse = warehouse?;
    let rest = warehouse
        .strip_prefix("bq://")
        .or_else(|| warehouse.strip_prefix("bl://"))?;
    let project = rest.strip_prefix("projects/")?.split('/').next()?;
    (!project.is_empty()).then(|| project.to_string())
}

// =============================================================================
// Iceberg
// =============================================================================

/// The legacy flat connection fields of the Iceberg HTTP routes. Its default
/// serializes every key, which is how the flat form's key set is derived.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct FlatIcebergConnection {
    mode: Option<String>,
    catalog_uri: Option<String>,
    table_location: Option<String>,
    region: Option<String>,
    catalog_id: Option<String>,
    table_bucket_arn: Option<String>,
    auth_bearer: Option<String>,
    auth_bearer_env: Option<String>,
    oauth2_token_url: Option<String>,
    oauth2_client_id: Option<String>,
    oauth2_client_secret: Option<String>,
    oauth2_client_secret_env: Option<String>,
    oauth2_scope: Option<String>,
    oauth2_audience: Option<String>,
    auth_google_metadata: bool,
    auth_google_scopes: Option<String>,
    warehouse: Option<String>,
    no_vended_credentials: bool,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_path_style: bool,
}

/// The structured keys of an Iceberg connection.
const ICEBERG_STRUCTURED: [&str; 2] = ["catalog", "io"];

/// An Iceberg connection from a request's connection fields (the request
/// minus its route's own fields): the structured `catalog` / `io`, or the
/// legacy flat fields.
pub fn iceberg_connection_from_json(
    input: Map<String, Value>,
    rules: &SecretRules,
) -> Result<IcebergConnectionConfig, GraphSourceInputError> {
    let conn = match split_forms(
        input,
        &ICEBERG_STRUCTURED,
        &keys_of::<FlatIcebergConnection>(),
    )? {
        Form::Structured(mut parts) => {
            let catalog = Form::take(&mut parts, "catalog")
                .ok_or_else(|| GraphSourceInputError::incomplete("io", &["catalog"]))?;
            structured_iceberg(catalog, Form::take(&mut parts, "io"))?
        }
        Form::Flat(input) => flat_iceberg(strict("", Value::Object(input))?, rules)?,
    };
    rules.check_iceberg(&conn)?;
    Ok(conn)
}

/// An Iceberg graph source's create config from a `map` request's connection
/// fields; `table` is as for [`IcebergCreateConfig::from_mode`]. The request's
/// R2RML mapping, which every source has, names its tables.
pub fn iceberg_create_from_json(
    name: &str,
    table: Option<&str>,
    input: Map<String, Value>,
    rules: &SecretRules,
) -> Result<IcebergCreateConfig, GraphSourceInputError> {
    let connection = iceberg_connection_from_json(input, rules)?;
    Ok(IcebergCreateConfig::from_connection_for(
        name, connection, table,
    ))
}

fn structured_iceberg(
    catalog: Value,
    io: Option<Value>,
) -> Result<IcebergConnectionConfig, GraphSourceInputError> {
    let catalog: CatalogConfig = strict("catalog", catalog)?;
    // `vended_credentials` defaults to true, so only the input says whether
    // the caller asked for it.
    let asked_vended = io
        .as_ref()
        .and_then(|io| io.get("vended_credentials"))
        .and_then(Value::as_bool);
    let mut io: IoConfig = match io {
        Some(io) => strict("io", io)?,
        None => IoConfig::default(),
    };
    let storage_auth = io.storage_auth.take();

    let conn = IcebergConnectionConfig::from_catalog(catalog)?;
    let rest = conn.is_rest();
    if !rest && asked_vended == Some(true) {
        return Err(CatalogModeError::NotForMode {
            mode: conn.mode_name(),
            field: "io.vended_credentials",
        }
        .into());
    }
    let mut conn = conn;
    conn.io = IoConfig {
        vended_credentials: rest && io.vended_credentials,
        ..io
    };
    if let Some(storage_auth) = storage_auth {
        if asked_vended.is_some_and(|v| v != matches!(storage_auth, StorageAuth::Vended)) {
            return Err(GraphSourceInputError::conflict(
                "io.vended_credentials",
                "io.storage_auth",
            ));
        }
        conn = conn.with_storage_auth(storage_auth);
    }
    Ok(conn)
}

fn flat_iceberg(
    flat: FlatIcebergConnection,
    rules: &SecretRules,
) -> Result<IcebergConnectionConfig, GraphSourceInputError> {
    IcebergConnectionFields {
        mode: flat.mode.unwrap_or_default(),
        catalog_uri: flat.catalog_uri,
        table_location: flat.table_location,
        region: flat.region,
        catalog_id: flat.catalog_id,
        table_bucket_arn: flat.table_bucket_arn,
        auth: CatalogAuthFields {
            bearer: secret_field(
                "auth_bearer",
                flat.auth_bearer,
                "auth_bearer_env",
                flat.auth_bearer_env,
            )?,
            oauth2_token_url: flat.oauth2_token_url,
            oauth2_client_id: flat.oauth2_client_id,
            oauth2_client_secret: secret_field(
                "oauth2_client_secret",
                flat.oauth2_client_secret,
                "oauth2_client_secret_env",
                flat.oauth2_client_secret_env,
            )?,
            oauth2_scope: flat.oauth2_scope,
            oauth2_audience: flat.oauth2_audience,
            google: if flat.auth_google_metadata {
                vec![("auth_google_metadata", GoogleCredentials::Metadata)]
            } else {
                Vec::new()
            },
            google_scopes: flat.auth_google_scopes,
            google_quota_project: None,
        },
        warehouse: flat.warehouse,
        no_vended_credentials: flat.no_vended_credentials,
        s3_region: flat.s3_region,
        s3_endpoint: flat.s3_endpoint,
        s3_path_style: flat.s3_path_style,
        storage_auth: StorageAuthFields::default(),
        default_storage_auth: None,
    }
    .into_connection(rules)
}

/// An Iceberg connection's flat spelling: what the legacy HTTP fields and the
/// CLI's flags collect. [`Self::into_connection`] is the one conversion both
/// go through.
#[derive(Debug, Clone, Default)]
pub struct IcebergConnectionFields {
    /// `rest` (also when empty), `direct`, `glue` or `s3tables`.
    pub mode: String,
    pub catalog_uri: Option<String>,
    pub table_location: Option<String>,
    pub region: Option<String>,
    pub catalog_id: Option<String>,
    pub table_bucket_arn: Option<String>,
    pub auth: CatalogAuthFields,
    pub warehouse: Option<String>,
    pub no_vended_credentials: bool,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_path_style: bool,
    pub storage_auth: StorageAuthFields,
    /// The storage identity a REST catalog reads with when the caller names
    /// none and does not turn vending off: a surface's own default (see
    /// [`default_storage_auth`]). Never inferred here.
    pub default_storage_auth: Option<StorageAuth>,
}

impl IcebergConnectionFields {
    /// The connection these fields describe, every value checked against
    /// `rules`: the catalog mode refuses REST-only settings, the auth is one
    /// mode or none, and the storage identity is one or none.
    pub fn into_connection(
        self,
        rules: &SecretRules,
    ) -> Result<IcebergConnectionConfig, GraphSourceInputError> {
        let Self {
            mode,
            catalog_uri,
            table_location,
            region,
            catalog_id,
            table_bucket_arn,
            auth,
            warehouse,
            no_vended_credentials,
            s3_region,
            s3_endpoint,
            s3_path_style,
            storage_auth,
            default_storage_auth,
        } = self;
        let mut rest_only = auth.given();
        if warehouse.is_some() {
            rest_only.push("warehouse");
        }
        let mut conn = IcebergConnectionConfig::from_mode(CatalogModeArgs {
            mode: &mode,
            catalog_uri: catalog_uri.as_deref(),
            table_location: table_location.as_deref(),
            region: region.as_deref(),
            catalog_id: catalog_id.as_deref(),
            table_bucket_arn: table_bucket_arn.as_deref(),
            rest_only: &rest_only,
        })?;
        if conn.is_rest() {
            auth.check(rules)?;
            conn = conn.with_auth(auth.into_auth(AuthProfile::Catalog)?);
            if let Some(warehouse) = warehouse {
                conn = conn.with_warehouse(warehouse);
            }
        }
        if no_vended_credentials {
            conn = conn.with_vended_credentials(false);
        }
        if let Some(region) = s3_region {
            conn = conn.with_s3_region(region);
        }
        if let Some(endpoint) = s3_endpoint {
            conn = conn.with_s3_endpoint(endpoint);
        }
        if s3_path_style {
            conn = conn.with_s3_path_style(true);
        }
        for (field, credentials) in &storage_auth.google {
            for value in credentials.config_values() {
                rules.check_value(field, value.value, value.class)?;
            }
        }
        let given = !storage_auth.is_empty();
        match storage_auth.into_storage_auth()? {
            Some(_) if no_vended_credentials => {
                return Err(GraphSourceInputError::conflict(
                    "no_vended_credentials",
                    "storage_identity",
                ))
            }
            Some(storage_auth) => conn = conn.with_storage_auth(storage_auth),
            None if !given && !no_vended_credentials && conn.is_rest() => {
                if let Some(storage_auth) = default_storage_auth {
                    conn = conn.with_storage_auth(storage_auth);
                }
            }
            None => {}
        }
        Ok(conn)
    }
}

// =============================================================================
// SQL
// =============================================================================

/// The flat auth fields of `sql/map`.
#[cfg(feature = "sql")]
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct FlatSqlAuth {
    auth_bearer: Option<String>,
    auth_bearer_env: Option<String>,
    oauth2_token_url: Option<String>,
    oauth2_client_id: Option<String>,
    oauth2_client_secret: Option<String>,
    oauth2_client_secret_env: Option<String>,
    oauth2_scope: Option<String>,
    oauth2_audience: Option<String>,
}

/// A SQL endpoint's auth from a `sql/map` request's auth fields: the
/// structured `auth` (the stored shape), or the flat fields.
#[cfg(feature = "sql")]
pub fn sql_auth_from_json(
    input: Map<String, Value>,
    rules: &SecretRules,
) -> Result<AuthConfig, GraphSourceInputError> {
    let auth = match split_forms(input, &["auth"], &keys_of::<FlatSqlAuth>())? {
        Form::Structured(mut parts) => {
            let auth = Form::take(&mut parts, "auth").unwrap_or(Value::Null);
            strict::<AuthConfig>("auth", auth)?
        }
        Form::Flat(input) => {
            let flat: FlatSqlAuth = strict("", Value::Object(input))?;
            let auth = CatalogAuthFields {
                bearer: secret_field(
                    "auth_bearer",
                    flat.auth_bearer,
                    "auth_bearer_env",
                    flat.auth_bearer_env,
                )?,
                oauth2_token_url: flat.oauth2_token_url,
                oauth2_client_id: flat.oauth2_client_id,
                oauth2_client_secret: secret_field(
                    "oauth2_client_secret",
                    flat.oauth2_client_secret,
                    "oauth2_client_secret_env",
                    flat.oauth2_client_secret_env,
                )?,
                oauth2_scope: flat.oauth2_scope,
                oauth2_audience: flat.oauth2_audience,
                ..CatalogAuthFields::default()
            };
            auth.check(rules)?;
            auth.into_auth(AuthProfile::Catalog)?
        }
    };
    rules.check_auth("auth", &auth)?;
    Ok(auth)
}

// =============================================================================
// Delta
// =============================================================================

/// The flat Unity Catalog fields every Delta route takes.
#[cfg(feature = "delta")]
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct FlatUnity {
    unity_uri: Option<String>,
    unity_catalog: Option<String>,
    unity_schema: Option<String>,
    auth_bearer: Option<String>,
    auth_bearer_env: Option<String>,
    oauth2_client_id: Option<String>,
    oauth2_client_secret: Option<String>,
    oauth2_client_secret_env: Option<String>,
    oauth2_token_url: Option<String>,
    oauth2_scope: Option<String>,
    oauth2_audience: Option<String>,
}

/// The flat storage fields of the Delta routes that read tables.
#[cfg(feature = "delta")]
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct FlatDeltaIo {
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_path_style: bool,
    azure_tenant_id: Option<String>,
    azure_client_id: Option<String>,
    azure_client_secret: Option<String>,
    azure_client_secret_env: Option<String>,
}

#[cfg(feature = "delta")]
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct FlatDelta {
    #[serde(flatten)]
    unity: FlatUnity,
    #[serde(flatten)]
    io: FlatDeltaIo,
}

/// A Delta source's catalog and storage options.
#[cfg(feature = "delta")]
#[derive(Debug, Clone, Default)]
pub struct DeltaConnection {
    pub unity: Option<fluree_db_delta::UnityConfig>,
    pub io: fluree_db_delta::DeltaIoConfig,
}

/// A Delta connection from a request's connection fields: the structured
/// `unity` / `io` (the stored shape), or the flat fields. A route that reads
/// no storage (`reads_storage` false) takes no `io` and no storage field.
#[cfg(feature = "delta")]
pub fn delta_connection_from_json(
    input: Map<String, Value>,
    reads_storage: bool,
    rules: &SecretRules,
) -> Result<DeltaConnection, GraphSourceInputError> {
    let (structured, flat_keys): (&[&str], Vec<String>) = if reads_storage {
        (&["unity", "io"], keys_of::<FlatDelta>())
    } else {
        (&["unity"], keys_of::<FlatUnity>())
    };
    let conn = match split_forms(input, structured, &flat_keys)? {
        Form::Structured(mut parts) => DeltaConnection {
            unity: Form::take(&mut parts, "unity")
                .map(|unity| strict("unity", unity))
                .transpose()?,
            io: Form::take(&mut parts, "io")
                .map(|io| strict("io", io))
                .transpose()?
                .unwrap_or_default(),
        },
        Form::Flat(input) if reads_storage => {
            let flat: FlatDelta = strict("", Value::Object(input))?;
            DeltaConnection {
                unity: flat_unity(flat.unity, rules)?,
                io: flat_delta_io(flat.io, rules)?,
            }
        }
        Form::Flat(input) => DeltaConnection {
            unity: flat_unity(strict("", Value::Object(input))?, rules)?,
            io: Default::default(),
        },
    };
    if let Some(unity) = &conn.unity {
        rules.check_auth("unity.auth", &unity.auth)?;
    }
    if let Some(azure) = &conn.io.azure {
        rules.check_all("io.azure", azure.config_values())?;
    }
    Ok(conn)
}

#[cfg(feature = "delta")]
fn flat_unity(
    flat: FlatUnity,
    rules: &SecretRules,
) -> Result<Option<fluree_db_delta::UnityConfig>, GraphSourceInputError> {
    let auth = CatalogAuthFields {
        bearer: secret_field(
            "auth_bearer",
            flat.auth_bearer,
            "auth_bearer_env",
            flat.auth_bearer_env,
        )?,
        oauth2_token_url: flat.oauth2_token_url,
        oauth2_client_id: flat.oauth2_client_id,
        oauth2_client_secret: secret_field(
            "oauth2_client_secret",
            flat.oauth2_client_secret,
            "oauth2_client_secret_env",
            flat.oauth2_client_secret_env,
        )?,
        oauth2_scope: flat.oauth2_scope,
        oauth2_audience: flat.oauth2_audience,
        ..CatalogAuthFields::default()
    };
    auth.check(rules)?;
    unity_config(flat.unity_uri, flat.unity_catalog, flat.unity_schema, auth)
}

/// A Unity Catalog connection from its flat fields; `None` when none is
/// given. Catalog or auth fields without the workspace URL are refused.
#[cfg(feature = "delta")]
pub fn unity_config(
    uri: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    auth: CatalogAuthFields,
) -> Result<Option<fluree_db_delta::UnityConfig>, GraphSourceInputError> {
    let Some(uri) = uri else {
        let mut stray = Vec::new();
        if catalog.is_some() {
            stray.push("unity_catalog");
        }
        if schema.is_some() {
            stray.push("unity_schema");
        }
        stray.extend(auth.given());
        return match stray.first() {
            Some(field) => Err(GraphSourceInputError::incomplete(*field, &["unity_uri"])),
            None => Ok(None),
        };
    };
    let auth = auth.into_auth(AuthProfile::Unity { uri: &uri })?;
    Ok(Some(fluree_db_delta::UnityConfig {
        uri,
        auth,
        catalog,
        schema,
    }))
}

#[cfg(feature = "delta")]
fn flat_delta_io(
    flat: FlatDeltaIo,
    rules: &SecretRules,
) -> Result<fluree_db_delta::DeltaIoConfig, GraphSourceInputError> {
    let client_secret = secret_field(
        "azure_client_secret",
        flat.azure_client_secret,
        "azure_client_secret_env",
        flat.azure_client_secret_env,
    )?;
    if let Some((field, value)) = &client_secret {
        rules.check_value(field, value, ValueClass::Secret)?;
    }
    Ok(fluree_db_delta::DeltaIoConfig {
        s3_region: flat.s3_region,
        s3_endpoint: flat.s3_endpoint,
        s3_path_style: flat.s3_path_style,
        azure: azure_auth(flat.azure_tenant_id, flat.azure_client_id, client_secret)?,
    })
}

/// An Azure service principal from its flat fields; `None` when none is
/// given (ambient credentials). A partial set is refused: falling back to
/// ambient credentials would read with an identity the caller did not name.
#[cfg(feature = "delta")]
pub fn azure_auth(
    tenant_id: Option<String>,
    client_id: Option<String>,
    client_secret: Option<(&'static str, ConfigValue)>,
) -> Result<Option<fluree_db_delta::AzureAuth>, GraphSourceInputError> {
    let first = [
        tenant_id.as_ref().map(|_| "azure_tenant_id"),
        client_id.as_ref().map(|_| "azure_client_id"),
        client_secret.as_ref().map(|(f, _)| *f),
    ]
    .into_iter()
    .flatten()
    .next();
    let Some(first) = first else {
        return Ok(None);
    };
    match (tenant_id, client_id, client_secret) {
        (Some(tenant_id), Some(client_id), Some((_, client_secret))) => {
            Ok(Some(fluree_db_delta::AzureAuth::ClientSecret {
                tenant_id,
                client_id,
                client_secret,
            }))
        }
        (None, _, _) => Err(GraphSourceInputError::incomplete(
            first,
            &["azure_tenant_id"],
        )),
        (_, None, _) => Err(GraphSourceInputError::incomplete(
            first,
            &["azure_client_id"],
        )),
        (_, _, None) => Err(GraphSourceInputError::incomplete(
            first,
            &["azure_client_secret", "azure_client_secret_env"],
        )),
    }
}

// =============================================================================
// Read-out redaction
// =============================================================================

/// What [`redact_stored`] did with a stored config.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Redaction {
    /// Not a source type with auth this build has a type for.
    Untyped,
    /// A source type with auth whose config its type cannot read: nothing in
    /// it can be classified, so a read-out must not show any of it.
    Unreadable,
    /// Masked every secret the type holds and every key it does not know;
    /// `true` when anything changed.
    Masked(bool),
}

/// Mask, in a stored graph-source config, every [`ConfigValue`] the persisted
/// type's own walk classifies as secret (see
/// [`fluree_db_iceberg::auth::fields`]), and every key the type does not
/// know (which nothing could classify). Structural rather than by key name, so
/// a new auth variant cannot reach a read-out unclassified.
pub(crate) fn redact_stored(
    source_type: &fluree_db_nameservice::GraphSourceType,
    value: &mut Value,
) -> Redaction {
    use fluree_db_nameservice::GraphSourceType as T;
    match source_type {
        T::Iceberg | T::R2rml => {
            mask_typed::<fluree_db_iceberg::IcebergGsConfig>(value, |config| {
                let mut fields = Vec::new();
                if let CatalogConfig::Rest { auth, .. } = &config.catalog {
                    fields.extend(secret_paths("catalog.auth", auth.config_values()));
                }
                if let Some(storage_auth) = &config.io.storage_auth {
                    fields.extend(secret_paths(
                        "io.storage_auth",
                        storage_auth.config_values(),
                    ));
                }
                fields
            })
        }
        #[cfg(feature = "sql")]
        T::Sql => mask_typed::<fluree_db_sql::SqlGsConfig>(value, |config| {
            secret_paths("auth", config.auth.config_values())
        }),
        #[cfg(feature = "delta")]
        T::Delta => mask_typed::<fluree_db_delta::DeltaGsConfig>(value, |config| {
            let mut fields = Vec::new();
            if let Some(unity) = &config.unity {
                fields.extend(secret_paths("unity.auth", unity.auth.config_values()));
            }
            if let Some(azure) = &config.io.azure {
                fields.extend(secret_paths("io.azure", azure.config_values()));
            }
            fields
        }),
        _ => Redaction::Untyped,
    }
}

fn secret_paths(prefix: &str, fields: Vec<ConfigValueField<'_>>) -> Vec<Vec<String>> {
    fields
        .into_iter()
        .filter(|f| f.class.is_secret())
        .map(|f| {
            join(prefix, &f.path)
                .split('.')
                .map(str::to_string)
                .collect()
        })
        .collect()
}

fn mask_typed<T: DeserializeOwned + Serialize>(
    value: &mut Value,
    secrets: impl Fn(&T) -> Vec<Vec<String>>,
) -> Redaction {
    let Ok(parsed) = serde_json::from_value::<T>(value.clone()) else {
        return Redaction::Unreadable;
    };
    let Ok(canonical) = serde_json::to_value(&parsed) else {
        return Redaction::Unreadable;
    };
    let mut unknown = Vec::new();
    unknown_paths(value, &canonical, &mut Vec::new(), &mut unknown);
    let mut changed = false;
    for path in secrets(&parsed) {
        if let Some(target) = at_path(value, &path) {
            changed |= mask_config_value(target);
        }
    }
    for path in unknown {
        if let Some(target) = at_path(value, &path) {
            changed |= mask_all(target);
        }
    }
    Redaction::Masked(changed)
}

/// Every key path of `input` that `canonical` lacks (see [`unknown_key`]).
fn unknown_paths(
    input: &Value,
    canonical: &Value,
    path: &mut Vec<String>,
    out: &mut Vec<Vec<String>>,
) {
    match (input, canonical) {
        (Value::Object(input), Value::Object(canonical)) => {
            for (key, value) in input {
                path.push(key.clone());
                match canonical.get(key) {
                    None if !carries_nothing(value) => out.push(path.clone()),
                    None => {}
                    Some(known) => unknown_paths(value, known, path, out),
                }
                path.pop();
            }
        }
        (Value::Array(input), Value::Array(canonical)) => {
            for (i, (value, known)) in input.iter().zip(canonical).enumerate() {
                path.push(i.to_string());
                unknown_paths(value, known, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

fn at_path<'v>(value: &'v mut Value, path: &[String]) -> Option<&'v mut Value> {
    path.iter().try_fold(value, |v, key| match v {
        Value::Object(map) => map.get_mut(key),
        Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get_mut(i)),
        _ => None,
    })
}

const REDACTED: &str = "[redacted]";

/// Mask a secret [`ConfigValue`] as stored: a literal outright; a reference
/// keeps what names the secret's location (`secret_ref`, `env_var`) and loses
/// any inline `default_val`, the one place a reference can carry a value.
fn mask_config_value(value: &mut Value) -> bool {
    match value {
        Value::Null => false,
        Value::Object(map) => {
            let mut changed = false;
            for (key, field) in map.iter_mut() {
                let names_location =
                    matches!(key.as_str(), "secret_ref" | "env_var" | "java_property");
                if !(names_location && field.is_string()) {
                    changed |= mask_all(field);
                }
            }
            changed
        }
        _ => mask_all(value),
    }
}

/// Mask every non-null leaf.
fn mask_all(value: &mut Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(text) if text == REDACTED => false,
        Value::Object(map) => map.values_mut().fold(false, |c, v| mask_all(v) | c),
        Value::Array(items) => items.iter_mut().fold(false, |c, v| mask_all(v) | c),
        _ => {
            *value = Value::String(REDACTED.to_string());
            true
        }
    }
}

#[cfg(test)]
mod tests;
