use super::*;
use crate::graph_source::config::CatalogMode;
use fluree_db_iceberg::auth::ExternalAccountConfig;
use serde_json::json;

fn object(value: Value) -> Map<String, Value> {
    value.as_object().expect("a JSON object").clone()
}

/// Any variable, no secret references: the CLI and Python.
fn local() -> SecretRules {
    SecretRules::local(false)
}

fn connection(body: Value) -> Result<IcebergConnectionConfig, GraphSourceInputError> {
    iceberg_connection_from_json(object(body), &SecretRules::listed("TOKEN,SA_KEY", false))
}

/// The record a source over `conn` stores.
fn stored(conn: IcebergConnectionConfig) -> Value {
    serde_json::to_value(
        IcebergCreateConfig::from_connection("gs", conn, "ns.t").to_iceberg_gs_config(),
    )
    .unwrap()
}

fn refused(body: Value) -> GraphSourceInputError {
    connection(body).expect_err("refused")
}

const URI: &str = "https://catalog.example.com";

/// Every catalog mode and legacy auth mode, in both forms.
fn iceberg_pairs() -> Vec<(Value, Value)> {
    vec![
        (
            json!({"catalog_uri": URI, "auth_bearer_env": "TOKEN", "warehouse": "wh"}),
            json!({"catalog": {"type": "rest", "uri": URI, "warehouse": "wh",
                "auth": {"type": "bearer", "token": {"env_var": "TOKEN"}}}}),
        ),
        (
            json!({"mode": "rest", "catalog_uri": URI,
                "oauth2_token_url": "https://catalog.example.com/token",
                "oauth2_client_secret": "pat", "oauth2_scope": "session:role:R",
                "oauth2_audience": "polaris"}),
            json!({"catalog": {"type": "rest", "uri": URI, "auth": {
                "type": "oauth2_client_credentials",
                "token_url": "https://catalog.example.com/token", "client_id": "",
                "client_secret": "pat", "scope": "session:role:R", "audience": "polaris"}}}),
        ),
        (
            json!({"catalog_uri": URI, "auth_google_metadata": true,
                "auth_google_scopes": "https://www.googleapis.com/auth/bigquery"}),
            json!({"catalog": {"type": "rest", "uri": URI, "auth": {"type": "google_metadata",
                "scopes": "https://www.googleapis.com/auth/bigquery"}}}),
        ),
        (
            json!({"catalog_uri": URI, "no_vended_credentials": true}),
            json!({"catalog": {"type": "rest", "uri": URI}, "io": {"vended_credentials": false}}),
        ),
        (
            json!({"mode": "direct", "table_location": "s3://b/w/ns/t", "s3_region": "us-east-2",
                "s3_endpoint": "http://127.0.0.1:9000", "s3_path_style": true}),
            json!({"catalog": {"type": "direct", "table_location": "s3://b/w/ns/t"},
                "io": {"s3_region": "us-east-2", "s3_endpoint": "http://127.0.0.1:9000",
                    "s3_path_style": true}}),
        ),
        (
            json!({"mode": "glue", "region": "us-east-1", "catalog_id": "123456789012"}),
            json!({"catalog": {"type": "glue", "region": "us-east-1",
                "catalog_id": "123456789012"}}),
        ),
        (
            json!({"mode": "s3tables",
                "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/b"}),
            json!({"catalog": {"type": "s3tables",
                "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/b"}}),
        ),
    ]
}

#[test]
fn the_flat_and_structured_forms_store_the_same_config() {
    for (flat, structured) in iceberg_pairs() {
        let from_flat = stored(connection(flat.clone()).unwrap());
        let from_structured = stored(connection(structured.clone()).unwrap());
        assert_eq!(from_flat, from_structured, "{flat} vs {structured}");
    }
}

#[test]
fn a_stored_connection_given_back_stores_itself() {
    // A read-out's `catalog` and `io`, sent back as the structured form.
    for (flat, _) in iceberg_pairs() {
        let first = stored(connection(flat).unwrap());
        let again =
            stored(connection(json!({"catalog": first["catalog"], "io": first["io"]})).unwrap());
        assert_eq!(first, again);
    }
}

#[test]
fn google_identities_reach_the_connection_in_their_stored_shape() {
    let conn = connection(json!({
        "catalog": {"type": "rest", "uri": URI, "warehouse": "bq://projects/p/locations/us",
            "auth": {"type": "google", "quota_project": "lake",
                "credentials": {"source": "service_account", "key": {"env_var": "SA_KEY"}}}},
        "io": {"storage_auth": {"type": "catalog"}}
    }))
    .unwrap();
    conn.validate().unwrap();
    let stored = stored(conn);
    assert_eq!(stored["catalog"]["auth"]["type"], "google");
    assert_eq!(
        stored["catalog"]["auth"]["credentials"]["key"]["env_var"],
        "SA_KEY"
    );
    assert_eq!(stored["io"]["storage_auth"]["type"], "catalog");
    // `with_storage_auth` set the legacy flag to agree; the caller did not.
    assert_eq!(stored["io"]["vended_credentials"], false);
}

#[test]
fn legacy_google_metadata_stores_the_record_it_always_did() {
    let stored =
        stored(connection(json!({"catalog_uri": URI, "auth_google_metadata": true})).unwrap());
    assert_eq!(
        stored["catalog"]["auth"],
        json!({"type": "google_metadata", "scopes": null, "metadata_url": null})
    );
}

// ---- the 400 classes ---------------------------------------------------------

fn kind(e: &GraphSourceInputError) -> &InputErrorKind {
    &e.kind
}

#[test]
fn an_unknown_field_is_refused_with_its_path() {
    let e = refused(json!({"catalog_uri": URI, "auth_bearer_envv": "TOKEN"}));
    assert_eq!(
        (e.field.as_str(), kind(&e)),
        ("auth_bearer_envv", &InputErrorKind::Unknown)
    );

    for (body, path) in [
        (
            json!({"catalog": {"type": "rest", "uri": URI, "warehose": "wh"}}),
            "catalog.warehose",
        ),
        (
            json!({"catalog": {"type": "rest", "uri": URI,
                "auth": {"type": "oauth2_client_credentials", "token_url": "https://t",
                    "client_id": "", "client_secret": "s", "scop": "x"}}}),
            "catalog.auth.scop",
        ),
        (
            json!({"catalog": {"type": "rest", "uri": URI,
                "auth": {"type": "bearer", "token": {"env_var": "TOKEN", "bogus": 1}}}}),
            "catalog.auth.token.bogus",
        ),
        (
            json!({"catalog": {"type": "direct", "table_location": "s3://b/w/ns/t", "uri": URI}}),
            "catalog.uri",
        ),
        (
            json!({"catalog": {"type": "rest", "uri": URI}, "io": {"s3_regoin": "x"}}),
            "io.s3_regoin",
        ),
        // `auth` is not a top-level key of a stored Iceberg config.
        (
            json!({"catalog": {"type": "rest", "uri": URI}, "auth": {"type": "none"}}),
            "auth",
        ),
    ] {
        let e = refused(body);
        assert_eq!(e.field, path, "{e}");
    }
}

#[test]
fn an_explicit_null_is_an_absent_field() {
    // The canonical form omits these (`skip_serializing_if`), and a client
    // that sends every field sends the unused ones as null.
    let with_nulls = connection(json!({
        "catalog": {"type": "rest", "uri": URI, "warehouse": null,
            "auth": {"type": "google", "credentials": {"source": "metadata"},
                "scopes": null, "quota_project": null}},
        "io": {"storage_auth": null, "s3_endpoint": null, "s3_region": null},
        "catalog_uri": null, "auth_bearer": null, "mode": null,
    }))
    .unwrap();
    let without = connection(json!({
        "catalog": {"type": "rest", "uri": URI,
            "auth": {"type": "google", "credentials": {"source": "metadata"}}},
    }))
    .unwrap();
    assert_eq!(stored(with_nulls), stored(without));
    // Likewise in the flat form, and for the structured key itself.
    let flat = connection(json!({"catalog_uri": URI, "catalog": null, "io": null,
        "auth_bearer_env": "TOKEN", "oauth2_scope": null, "warehouse": null}))
    .unwrap();
    assert_eq!(stored(flat)["catalog"]["auth"]["token"]["env_var"], "TOKEN");
    // A null cannot hide an unknown key that carries a value.
    let e = refused(json!({"catalog_uri": URI, "auth_bearer": null, "warehose": "wh"}));
    assert_eq!(e.field, "warehose");
}

#[test]
fn the_legacy_untagged_catalog_a_stored_record_may_hold_is_accepted() {
    // `CatalogConfig` reads a catalog with no `type` as REST (records written
    // before the catalog modes); a read-out of one can be sent back.
    let legacy = connection(json!({"catalog": {"uri": URI, "warehouse": "wh",
        "auth": {"type": "bearer", "token": {"env_var": "TOKEN"}}}}))
    .unwrap();
    let tagged = connection(
        json!({"catalog": {"type": "rest", "uri": URI, "warehouse": "wh",
        "auth": {"type": "bearer", "token": {"env_var": "TOKEN"}}}}),
    )
    .unwrap();
    assert_eq!(stored(legacy), stored(tagged));
    // Its compat read path does not make it lenient.
    let e = refused(json!({"catalog": {"uri": URI, "warehose": "wh"}}));
    assert_eq!(e.field, "catalog.warehose");
    // A WIF configuration may be its JSON text, as Phase 1 documents.
    let text = connection(json!({"catalog": {"type": "rest", "uri": URI, "auth": {
        "type": "google", "credentials": {"source": "external_account",
            "config": "{\"type\": \"external_account\"}"}}}}));
    assert!(text.is_ok(), "{text:?}");
}

#[test]
fn flat_and_structured_fields_may_not_be_mixed() {
    let e = refused(json!({"catalog": {"type": "rest", "uri": URI}, "auth_bearer": "t"}));
    assert_eq!(e.field, "auth_bearer");
    assert!(matches!(e.kind, InputErrorKind::Mixed { .. }), "{e}");
    let e = refused(json!({"io": {"s3_region": "us-east-1"}, "catalog_uri": URI}));
    assert!(matches!(e.kind, InputErrorKind::Mixed { .. }), "{e}");
    let e = refused(json!({"io": {"s3_region": "us-east-1"}}));
    assert_eq!(e.field, "io", "{e}");
}

#[test]
fn partial_auth_is_refused_instead_of_dropped() {
    // Each of these used to store `{"type": "none"}`.
    for (body, field) in [
        (
            json!({"catalog_uri": URI, "oauth2_token_url": "https://t"}),
            "oauth2_token_url",
        ),
        (
            json!({"catalog_uri": URI, "oauth2_scope": "session:role:R"}),
            "oauth2_scope",
        ),
        (
            json!({"catalog_uri": URI, "oauth2_client_secret": "s"}),
            "oauth2_client_secret",
        ),
        (
            json!({"catalog_uri": URI, "auth_google_scopes": "x"}),
            "auth_google_scopes",
        ),
    ] {
        let e = refused(body);
        assert_eq!(e.field, field, "{e}");
        assert!(matches!(e.kind, InputErrorKind::Incomplete { .. }), "{e}");
    }
}

#[test]
fn two_auth_modes_at_once_are_refused() {
    for body in [
        json!({"catalog_uri": URI, "auth_bearer": "t", "oauth2_token_url": "https://t",
            "oauth2_client_secret": "s"}),
        // Google used to win over the bearer silently.
        json!({"catalog_uri": URI, "auth_bearer": "t", "auth_google_metadata": true}),
        json!({"catalog_uri": URI, "auth_bearer": "t", "auth_bearer_env": "TOKEN"}),
    ] {
        let e = refused(body.clone());
        assert!(
            matches!(
                e.kind,
                InputErrorKind::TwoWays { .. } | InputErrorKind::Conflict { .. }
            ),
            "{body}: {e}"
        );
    }
}

#[test]
fn a_variable_the_operator_did_not_list_is_refused_in_either_form() {
    for body in [
        json!({"catalog_uri": URI, "auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}),
        json!({"catalog": {"type": "rest", "uri": URI,
            "auth": {"type": "bearer", "token": {"env_var": "AWS_SECRET_ACCESS_KEY"}}}}),
        json!({"catalog": {"type": "rest", "uri": URI}, "io": {"storage_auth": {
            "type": "google", "credentials": {"source": "service_account",
                "key": {"env_var": "AWS_SECRET_ACCESS_KEY"}}}}}),
    ] {
        let e = refused(body);
        assert_eq!(
            e.kind,
            InputErrorKind::EnvNotAllowed {
                name: "AWS_SECRET_ACCESS_KEY".into()
            }
        );
        assert!(e.to_string().contains(SECRET_ENV_VARS_ENV), "{e}");
    }
    // The CLI and Python name their own process's variables.
    iceberg_connection_from_json(
        object(json!({"catalog_uri": URI, "auth_bearer_env": "ANYTHING"})),
        &local(),
    )
    .unwrap();
}

#[test]
fn a_flat_refusal_names_the_field_the_caller_sent() {
    let e = refused(json!({"catalog_uri": URI, "auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}));
    assert_eq!(e.field, "auth_bearer_env");
    let e = refused(json!({"catalog_uri": URI, "oauth2_token_url": "https://t",
        "oauth2_client_secret_env": "AWS_SECRET_ACCESS_KEY"}));
    assert_eq!(e.field, "oauth2_client_secret_env");
    #[cfg(feature = "delta")]
    {
        let e = delta(json!({"azure_tenant_id": "t", "azure_client_id": "c",
            "azure_client_secret_env": "AWS_SECRET_ACCESS_KEY"}))
        .unwrap_err();
        assert_eq!(e.field, "azure_client_secret_env");
    }
}

#[test]
fn a_secret_reference_needs_a_resolver() {
    let body = json!({"catalog": {"type": "rest", "uri": URI,
        "auth": {"type": "bearer", "token": {"secret_ref": "vault://t"}}}});
    let e = refused(body.clone());
    assert_eq!(
        (e.field.as_str(), &e.kind),
        ("catalog.auth.token", &InputErrorKind::SecretRefUnsupported)
    );
    iceberg_connection_from_json(object(body), &SecretRules::listed("", true)).unwrap();
}

#[test]
fn a_service_account_key_is_never_a_literal() {
    for body in [
        json!({"catalog": {"type": "rest", "uri": URI, "auth": {"type": "google",
            "credentials": {"source": "service_account", "key": "{\"private_key\":\"x\"}"}}}}),
        json!({"catalog": {"type": "direct", "table_location": "s3://b/w/ns/t"},
            "io": {"storage_auth": {"type": "google", "credentials": {
                "source": "service_account", "key": "{}"}}}}),
    ] {
        let e = iceberg_connection_from_json(object(body), &local()).unwrap_err();
        assert_eq!(e.kind, InputErrorKind::LiteralKey, "{e}");
        assert!(!e.to_string().contains("private_key"), "{e}");
    }
}

#[test]
fn the_metadata_url_is_not_set_per_source() {
    let e = refused(json!({"catalog": {"type": "rest", "uri": URI, "auth": {
        "type": "google_metadata",
        "metadata_url": "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
    }}}));
    assert_eq!(
        (e.field.as_str(), &e.kind),
        ("catalog.auth.metadata_url", &InputErrorKind::MetadataUrl)
    );
}

#[test]
fn a_reference_names_its_variable_and_nothing_else() {
    for token in [
        json!({"env_var": "TOKEN", "default_val": "fallback"}),
        json!({"env_var": "TOKEN", "java_property": "p"}),
        json!({"java_property": "p"}),
    ] {
        let e = refused(json!({"catalog": {"type": "rest", "uri": URI,
            "auth": {"type": "bearer", "token": token}}}));
        assert_eq!(e.kind, InputErrorKind::UnsupportedReference, "{e}");
    }
}

#[test]
fn a_structured_catalog_cannot_relabel_itself() {
    let e = refused(json!({"catalog": {"type": "rest", "uri": URI, "catalog_type": "unity"}}));
    assert_eq!(e.field, "catalog.catalog_type");
    connection(json!({"catalog": {"type": "rest", "uri": URI, "catalog_type": "polaris"}}))
        .unwrap();
}

#[test]
fn vending_is_refused_where_nothing_vends_and_where_storage_auth_says_otherwise() {
    let e = refused(json!({"catalog": {"type": "glue", "region": "us-east-1"},
        "io": {"vended_credentials": true}}));
    assert_eq!(e.field, "io.vended_credentials");
    assert!(matches!(e.kind, InputErrorKind::Mode(_)), "{e}");

    let e = refused(json!({"catalog": {"type": "rest", "uri": URI},
        "io": {"vended_credentials": true, "storage_auth": {"type": "ambient"}}}));
    assert_eq!(
        e,
        GraphSourceInputError::conflict("io.vended_credentials", "io.storage_auth")
    );
    // Agreeing, as a stored record does, is fine.
    connection(json!({"catalog": {"type": "rest", "uri": URI},
        "io": {"vended_credentials": false, "storage_auth": {"type": "ambient"}}}))
    .unwrap();
}

#[test]
fn rest_only_fields_are_refused_by_other_modes_and_named() {
    for field in [
        "auth_bearer",
        "oauth2_scope",
        "auth_google_metadata",
        "warehouse",
    ] {
        let mut body = json!({"mode": "direct", "table_location": "s3://b/w/ns/t"});
        body[field] = if field == "auth_google_metadata" {
            json!(true)
        } else {
            json!("x")
        };
        let e = refused(body);
        assert!(
            e.to_string()
                .contains(&format!("{field} applies to rest mode only")),
            "{field}: {e}"
        );
    }
}

#[test]
fn a_serde_message_never_echoes_a_value() {
    let e = refused(json!({"catalog_uri": URI, "s3_path_style": "sk-live-secret"}));
    assert!(!e.to_string().contains("sk-live-secret"), "{e}");
}

// ---- SQL ---------------------------------------------------------------------

#[cfg(feature = "sql")]
#[test]
fn sql_auth_is_the_same_in_both_forms_and_names_variables_like_iceberg() {
    let rules = SecretRules::listed("SQL_TOKEN", false);
    let sql = |body: Value| sql_auth_from_json(object(body), &rules);
    for (flat, structured) in [
        (
            json!({"auth_bearer_env": "SQL_TOKEN"}),
            json!({"auth": {"type": "bearer", "token": {"env_var": "SQL_TOKEN"}}}),
        ),
        (
            json!({"oauth2_token_url": "https://t", "oauth2_client_id": "app",
                "oauth2_client_secret_env": "SQL_TOKEN"}),
            json!({"auth": {"type": "oauth2_client_credentials", "token_url": "https://t",
                "client_id": "app", "client_secret": {"env_var": "SQL_TOKEN"}}}),
        ),
        (json!({}), json!({"auth": {"type": "none"}})),
    ] {
        assert_eq!(sql(flat).unwrap(), sql(structured).unwrap());
    }
    // Google, structured only, now reaches a SQL endpoint.
    let google = sql(json!({"auth": {"type": "google", "credentials": {"source": "metadata"}}}));
    assert!(matches!(google.unwrap(), AuthConfig::Google { .. }));
    // The old precedence (OAuth2 over a bearer) is a refusal.
    let e = sql(json!({"auth_bearer": "t", "oauth2_token_url": "https://t",
        "oauth2_client_secret": "s"}))
    .unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::TwoWays { .. }), "{e}");
    let e = sql(json!({"auth_bearer_env": "OTHER"})).unwrap_err();
    assert!(
        matches!(e.kind, InputErrorKind::EnvNotAllowed { .. }),
        "{e}"
    );
    let e = sql(json!({"auth": {"type": "none"}, "auth_bearer": "t"})).unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::Mixed { .. }), "{e}");
}

// ---- Delta -------------------------------------------------------------------

#[cfg(feature = "delta")]
fn delta(body: Value) -> Result<DeltaConnection, GraphSourceInputError> {
    delta_connection_from_json(object(body), true, &SecretRules::listed("SP_SECRET", false))
}

#[cfg(feature = "delta")]
fn delta_stored(conn: DeltaConnection) -> Value {
    json!({"unity": conn.unity, "io": conn.io})
}

#[cfg(feature = "delta")]
#[test]
fn delta_connections_are_the_same_in_both_forms() {
    for (flat, structured) in [
        (
            json!({"unity_uri": "https://ws.example.com", "unity_catalog": "main",
                "oauth2_client_id": "app", "oauth2_client_secret_env": "SP_SECRET"}),
            json!({"unity": {"uri": "https://ws.example.com", "catalog": "main", "auth": {
                "type": "oauth2_client_credentials",
                "token_url": "https://ws.example.com/oidc/v1/token", "client_id": "app",
                "client_secret": {"env_var": "SP_SECRET"}, "scope": "all-apis"}}}),
        ),
        (
            json!({"s3_region": "us-east-1", "azure_tenant_id": "t", "azure_client_id": "c",
                "azure_client_secret_env": "SP_SECRET"}),
            json!({"io": {"s3_region": "us-east-1", "azure": {"type": "client_secret",
                "tenant_id": "t", "client_id": "c",
                "client_secret": {"env_var": "SP_SECRET"}}}}),
        ),
    ] {
        assert_eq!(
            delta_stored(delta(flat).unwrap()),
            delta_stored(delta(structured).unwrap())
        );
    }
}

#[cfg(feature = "delta")]
#[test]
fn an_azure_secret_variable_goes_through_the_allowlist() {
    // `azure_client_secret_env` used to skip it.
    for body in [
        json!({"azure_tenant_id": "t", "azure_client_id": "c",
            "azure_client_secret_env": "AWS_SECRET_ACCESS_KEY"}),
        json!({"io": {"azure": {"type": "client_secret", "tenant_id": "t", "client_id": "c",
            "client_secret": {"env_var": "AWS_SECRET_ACCESS_KEY"}}}}),
    ] {
        let e = delta(body).unwrap_err();
        assert!(
            matches!(e.kind, InputErrorKind::EnvNotAllowed { .. }),
            "{e}"
        );
    }
    let e = delta(json!({"azure_tenant_id": "t"})).unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::Incomplete { .. }), "{e}");
}

#[cfg(feature = "delta")]
#[test]
fn a_catalog_only_delta_route_takes_no_storage() {
    let e = delta_connection_from_json(
        object(
            json!({"unity_uri": "https://ws.example.com", "auth_bearer": "t",
            "s3_region": "us-east-1"}),
        ),
        false,
        &local(),
    )
    .unwrap_err();
    assert_eq!(
        (e.field.as_str(), &e.kind),
        ("s3_region", &InputErrorKind::Unknown)
    );
    let e = delta(json!({"unity_catalog": "main"})).unwrap_err();
    assert_eq!(e.field, "unity_catalog");
    let e = delta(json!({"unity": {"uri": "https://ws.example.com",
        "auth": {"type": "bearer", "token": "t"}}, "unity_schema": "s"}))
    .unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::Mixed { .. }), "{e}");
}

// ---- the shared converters ---------------------------------------------------

#[test]
fn the_flat_google_sources_and_their_quota_project() {
    let fields = |google: Vec<(&'static str, GoogleCredentials)>, quota: Option<&str>| {
        CatalogAuthFields {
            google,
            google_quota_project: quota.map(str::to_string),
            ..Default::default()
        }
        .into_auth(AuthProfile::Catalog)
    };
    let adc = fields(
        vec![("auth_google_adc", GoogleCredentials::ApplicationDefault)],
        Some("lake"),
    )
    .unwrap();
    assert_eq!(
        adc,
        AuthConfig::Google {
            credentials: GoogleCredentials::ApplicationDefault,
            scopes: None,
            quota_project: Some("lake".into())
        }
    );
    // Metadata with a quota project needs the `google` shape.
    let metadata = fields(
        vec![("auth_google_metadata", GoogleCredentials::Metadata)],
        Some("lake"),
    )
    .unwrap();
    assert!(matches!(metadata, AuthConfig::Google { .. }));
    let e = fields(
        vec![
            ("auth_google_metadata", GoogleCredentials::Metadata),
            ("auth_google_adc", GoogleCredentials::ApplicationDefault),
        ],
        None,
    )
    .unwrap_err();
    assert_eq!(
        e,
        GraphSourceInputError::two_ways("auth_google_adc", "auth_google_metadata")
    );
    let e = fields(Vec::new(), Some("lake")).unwrap_err();
    assert_eq!(e.field, "auth_google_quota_project");
}

#[test]
fn unity_defaults_its_token_url_and_scope_and_takes_no_google() {
    let auth = CatalogAuthFields {
        oauth2_client_id: Some("app".into()),
        oauth2_client_secret: Some(("oauth2_client_secret", ConfigValue::literal("s"))),
        ..Default::default()
    }
    .into_auth(AuthProfile::Unity {
        uri: "https://ws.example.com/",
    })
    .unwrap();
    assert_eq!(
        auth,
        AuthConfig::OAuth2ClientCredentials {
            token_url: "https://ws.example.com/oidc/v1/token".into(),
            client_id: ConfigValue::literal("app"),
            client_secret: ConfigValue::literal("s"),
            scope: Some("all-apis".into()),
            audience: None
        }
    );
    let e = CatalogAuthFields {
        google: vec![("auth_google_adc", GoogleCredentials::ApplicationDefault)],
        ..Default::default()
    }
    .into_auth(AuthProfile::Unity { uri: "https://ws" })
    .unwrap_err();
    assert!(e.to_string().contains("Unity Catalog takes"), "{e}");
}

#[test]
fn a_storage_identity_from_flags() {
    let fields = |identity: Option<&str>, google: Vec<(&'static str, GoogleCredentials)>| {
        StorageAuthFields {
            identity: identity.map(str::to_string),
            google,
            google_quota_project: None,
        }
        .into_storage_auth()
    };
    assert_eq!(fields(None, Vec::new()).unwrap(), None);
    assert_eq!(
        fields(Some("catalog"), Vec::new()).unwrap(),
        Some(StorageAuth::Catalog)
    );
    let wif = GoogleCredentials::ExternalAccount {
        config: ExternalAccountConfig::Ref(ConfigValue::from_env("WIF")),
    };
    // Credentials alone mean `google`.
    let google = fields(None, vec![("storage_google_wif_config", wif.clone())]).unwrap();
    assert!(matches!(google, Some(StorageAuth::Google { .. })));
    let e = fields(Some("vended"), vec![("storage_google_wif_config", wif)]).unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::Conflict { .. }), "{e}");
    let e = fields(Some("google"), Vec::new()).unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::Incomplete { .. }), "{e}");
    let e = fields(Some("hmac"), Vec::new()).unwrap_err();
    assert!(matches!(e.kind, InputErrorKind::Invalid { .. }), "{e}");
}

#[test]
fn a_biglake_warehouse_picks_its_default_storage_identity() {
    assert_eq!(
        default_storage_auth(Some("bq://projects/p/locations/us/catalogs/c")),
        Some(StorageAuth::Catalog)
    );
    assert_eq!(
        default_storage_auth(Some("bl://projects/p/catalogs/c")),
        Some(StorageAuth::Vended)
    );
    assert_eq!(default_storage_auth(Some("wh")), None);
    assert_eq!(default_storage_auth(None), None);
}

#[test]
fn the_error_names_fields_in_the_callers_spelling() {
    let e = GraphSourceInputError::incomplete(
        "oauth2_token_url",
        &["oauth2_client_secret", "oauth2_client_secret_env"],
    );
    let flag = |f: &str| format!("--{}", f.replace('_', "-"));
    assert_eq!(
        e.message(flag),
        "--oauth2-token-url needs --oauth2-client-secret or --oauth2-client-secret-env"
    );
}

// ---- builders ----------------------------------------------------------------

#[test]
fn a_builder_setting_the_mode_cannot_honor_is_refused_at_validate() {
    let direct = || IcebergConnectionConfig::direct("s3://b/w/ns/t");
    for (conn, field) in [
        (direct().with_auth_bearer("t"), "auth"),
        (direct().with_auth_google_metadata(None), "auth"),
        (direct().with_warehouse("wh"), "warehouse"),
        (direct().with_oauth2_scope("s"), "oauth2_scope"),
        (direct().with_vended_credentials(true), "vended_credentials"),
        (
            IcebergConnectionConfig::glue(Some("us-east-1".into()), None).with_auth_bearer("t"),
            "auth",
        ),
    ] {
        let e = conn.validate().unwrap_err();
        let crate::ApiError::GraphSourceInput(input) = &e else {
            panic!("{field}: untyped {e}");
        };
        assert_eq!(input.field, field, "{e}");
        assert!(e.to_string().contains("applies to rest mode only"), "{e}");
    }
    // The message names the actual mode, not "Direct" for every one.
    let glue = IcebergConnectionConfig::glue(None, None).with_warehouse("wh");
    assert!(glue
        .validate()
        .unwrap_err()
        .to_string()
        .contains("glue mode"));
    // What every mode agrees with stays fine.
    direct().with_vended_credentials(false).validate().unwrap();
    direct().with_auth(AuthConfig::None).validate().unwrap();

    let e = IcebergConnectionConfig::rest(URI)
        .with_oauth2_audience("a")
        .validate()
        .unwrap_err();
    assert!(e.to_string().contains("oauth2_audience needs"), "{e}");
}

#[test]
fn with_storage_auth_keeps_the_legacy_flag_agreeing() {
    let conn = IcebergConnectionConfig::rest(URI)
        .with_auth(AuthConfig::Google {
            credentials: GoogleCredentials::Metadata,
            scopes: None,
            quota_project: None,
        })
        .with_storage_auth(StorageAuth::Catalog);
    assert!(!conn.io.vended_credentials);
    conn.validate().unwrap();
    let vended = IcebergConnectionConfig::rest(URI)
        .with_vended_credentials(false)
        .with_storage_auth(StorageAuth::Vended);
    assert!(vended.io.vended_credentials);
    vended.validate().unwrap();
}

#[test]
fn registration_refuses_a_storage_identity_the_catalog_cannot_serve() {
    // `catalog` needs a Google catalog identity; this one is a bearer.
    let create = crate::R2rmlCreateConfig::new(
        "gs",
        URI,
        "ns.t",
        "@prefix rr: <http://www.w3.org/ns/r2rml#> .",
    )
    .with_auth_bearer("t")
    .with_storage_auth(StorageAuth::Catalog);
    let e = create.validate().unwrap_err();
    assert_eq!(e.status_code(), 400);
    assert!(e.to_string().contains("io.storage_auth `catalog`"), "{e}");

    let iceberg =
        IcebergCreateConfig::new("gs", URI, "ns.t").with_storage_auth(StorageAuth::Catalog);
    assert!(iceberg.validate().is_err());

    // A Direct source refuses `vended`.
    let direct = IcebergCreateConfig::new_direct("gs", "s3://b/w/ns/t")
        .with_storage_auth(StorageAuth::Vended);
    assert!(direct
        .validate()
        .unwrap_err()
        .to_string()
        .contains("REST catalog"));
}

#[test]
fn registration_refuses_a_malformed_google_identity_before_storing_it() {
    let create = IcebergCreateConfig::new("gs", URI, "ns.t").with_auth(AuthConfig::Google {
        credentials: GoogleCredentials::Metadata,
        scopes: None,
        quota_project: Some("not a project".into()),
    });
    let e = create.validate().unwrap_err();
    assert_eq!(
        e.error_type(),
        fluree_vocab::errors::GOOGLE_CREDENTIALS_INVALID
    );
}

#[test]
fn registration_runs_the_engines_own_validation_in_every_mode() {
    // A Direct location must name its table when no mapping does; only the
    // stored config's validation knew that, and REST/Direct registration
    // never ran it.
    let e = IcebergCreateConfig::new_direct("gs", "s3://bucket/t")
        .validate()
        .unwrap_err();
    assert!(e.to_string().contains("table identifier"), "{e}");
}

#[test]
fn registration_refuses_what_every_query_would() {
    // A query validates the stored config before it reads anything, and a
    // Direct location must name a table (`ns/table`) even when a mapping names
    // them; registration used to accept one that no query could use.
    let e = crate::R2rmlCreateConfig::new_direct(
        "gs",
        "s3://bucket/dw",
        "@prefix rr: <http://www.w3.org/ns/r2rml#> .",
    )
    .validate()
    .unwrap_err();
    assert!(e.to_string().contains("table identifier"), "{e}");
}

#[test]
fn a_rest_connection_built_from_its_stored_catalog_round_trips() {
    let conn = IcebergConnectionConfig::from_catalog(CatalogConfig::Rest {
        catalog_type: "polaris".into(),
        uri: URI.into(),
        auth: AuthConfig::GoogleMetadata {
            scopes: None,
            metadata_url: None,
        },
        warehouse: Some("wh".into()),
    })
    .unwrap();
    let CatalogMode::Rest(rest) = &conn.catalog_mode else {
        panic!("rest");
    };
    assert_eq!(rest.warehouse.as_deref(), Some("wh"));
}

// ---- read-out redaction --------------------------------------------------------

/// Every way a `ConfigValue` can be stored, each carrying `secret` where a
/// value can sit.
fn every_value(secret: &str) -> Vec<ConfigValue> {
    let values = vec![
        ConfigValue::literal(secret),
        ConfigValue::from_env("SECRET_VAR"),
        ConfigValue::Dynamic {
            env_var: Some("SECRET_VAR".into()),
            java_property: None,
            default_val: Some(secret.into()),
        },
        ConfigValue::SecretRef {
            secret_ref: "vault://ref".into(),
        },
    ];
    // A new form fails to compile here until it is listed above.
    for value in &values {
        match value {
            ConfigValue::Literal(_)
            | ConfigValue::SecretRef { .. }
            | ConfigValue::Dynamic { .. } => {}
        }
    }
    values
}

fn every_google(secret: &str) -> Vec<GoogleCredentials> {
    let mut all = vec![
        GoogleCredentials::Metadata,
        GoogleCredentials::ApplicationDefault,
        GoogleCredentials::ExternalAccount {
            config: ExternalAccountConfig::Inline(
                json!({"type": "external_account", "audience": "PUBLIC-WIF"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        },
    ];
    for value in every_value(secret) {
        all.push(GoogleCredentials::ServiceAccount { key: value.clone() });
    }
    for value in every_value("PUBLIC-WIF") {
        all.push(GoogleCredentials::ExternalAccount {
            config: ExternalAccountConfig::Ref(value),
        });
    }
    for credentials in &all {
        match credentials {
            GoogleCredentials::Metadata
            | GoogleCredentials::ApplicationDefault
            | GoogleCredentials::ServiceAccount { .. }
            | GoogleCredentials::ExternalAccount { .. } => {}
        }
    }
    all
}

fn every_auth(secret: &str) -> Vec<AuthConfig> {
    let mut all = vec![
        AuthConfig::None,
        AuthConfig::GoogleMetadata {
            scopes: Some("PUBLIC-SCOPE".into()),
            metadata_url: None,
        },
    ];
    for value in every_value(secret) {
        all.push(AuthConfig::Bearer {
            token: value.clone(),
        });
        all.push(AuthConfig::OAuth2ClientCredentials {
            token_url: "https://PUBLIC-TOKEN-URL".into(),
            client_id: ConfigValue::literal("PUBLIC-CLIENT"),
            client_secret: value,
            scope: Some("PUBLIC-SCOPE".into()),
            audience: None,
        });
    }
    for credentials in every_google(secret) {
        all.push(AuthConfig::Google {
            credentials,
            scopes: None,
            quota_project: Some("PUBLIC-PROJECT".into()),
        });
    }
    for auth in &all {
        match auth {
            AuthConfig::None
            | AuthConfig::Bearer { .. }
            | AuthConfig::OAuth2ClientCredentials { .. }
            | AuthConfig::GoogleMetadata { .. }
            | AuthConfig::Google { .. } => {}
        }
    }
    all
}

fn every_storage_auth(secret: &str) -> Vec<StorageAuth> {
    let mut all = vec![
        StorageAuth::Vended,
        StorageAuth::Ambient,
        StorageAuth::Catalog,
    ];
    for credentials in every_google(secret) {
        all.push(StorageAuth::Google {
            credentials,
            quota_project: None,
        });
    }
    for storage_auth in &all {
        match storage_auth {
            StorageAuth::Vended
            | StorageAuth::Ambient
            | StorageAuth::Catalog
            | StorageAuth::Google { .. } => {}
        }
    }
    all
}

fn read_out(source_type: fluree_db_nameservice::GraphSourceType, config: &Value) -> String {
    crate::ledger_info::redact_graph_source_record_config(&source_type, &config.to_string())
}

/// What a read-out must and must not show.
fn assert_redacted(out: &str, stored: &Value) {
    assert!(!out.contains("SECRET-VALUE"), "secret leaked: {out}");
    for shown in [
        "SECRET_VAR",
        "vault://ref",
        "PUBLIC-CLIENT",
        "PUBLIC-TOKEN-URL",
    ] {
        if stored.to_string().contains(shown) {
            assert!(out.contains(shown), "{shown} hidden: {out}");
        }
    }
}

#[test]
fn every_secret_of_every_auth_variant_is_masked_in_a_read_out() {
    use fluree_db_nameservice::GraphSourceType;
    for auth in every_auth("SECRET-VALUE") {
        let config = IcebergCreateConfig::new("gs", URI, "ns.t").with_auth(auth.clone());
        let stored = serde_json::to_value(config.to_iceberg_gs_config()).unwrap();
        for source_type in [GraphSourceType::Iceberg, GraphSourceType::R2rml] {
            assert_redacted(&read_out(source_type, &stored), &stored);
        }
        #[cfg(feature = "sql")]
        {
            let sql = json!({"endpoint": URI, "auth": auth});
            assert_redacted(&read_out(GraphSourceType::Sql, &sql), &sql);
        }
        #[cfg(feature = "delta")]
        {
            let delta = json!({"unity": {"uri": URI, "auth": auth}});
            assert_redacted(&read_out(GraphSourceType::Delta, &delta), &delta);
        }
    }
    for storage_auth in every_storage_auth("SECRET-VALUE") {
        let config =
            IcebergCreateConfig::new_direct("gs", "s3://b/w/ns/t").with_storage_auth(storage_auth);
        let stored = serde_json::to_value(config.to_iceberg_gs_config()).unwrap();
        assert_redacted(&read_out(GraphSourceType::Iceberg, &stored), &stored);
    }
    #[cfg(feature = "delta")]
    for value in every_value("SECRET-VALUE") {
        let azure = fluree_db_delta::AzureAuth::ClientSecret {
            tenant_id: "PUBLIC-TENANT".into(),
            client_id: "PUBLIC-CLIENT".into(),
            client_secret: value,
        };
        match &azure {
            fluree_db_delta::AzureAuth::ClientSecret { .. } => {}
        }
        let delta = json!({"root": "s3://b/t", "io": {"azure": azure}});
        let out = read_out(GraphSourceType::Delta, &delta);
        assert_redacted(&out, &delta);
        assert!(out.contains("PUBLIC-TENANT"), "{out}");
    }
}

#[test]
fn a_read_out_masks_what_its_type_does_not_know_and_refuses_what_it_cannot_read() {
    use fluree_db_nameservice::GraphSourceType;
    let stored = json!({"catalog": {"type": "rest", "uri": URI,
        "auth": {"type": "none", "credentials_json": "SECRET-VALUE"}},
        "table": "ns.t", "private_key": {"pem": "SECRET-VALUE"}});
    let out = read_out(GraphSourceType::Iceberg, &stored);
    assert_redacted(&out, &stored);
    assert!(out.contains(URI), "{out}");

    let out = read_out(
        GraphSourceType::Iceberg,
        &json!({"not": "an iceberg config"}),
    );
    assert_eq!(out, "\"[redacted:unreadable]\"");

    // Nothing to mask: byte-identical.
    let plain = r#"{"catalog":{"type":"direct","table_location":"s3://b/w/ns/t"},"table":""}"#;
    assert_eq!(
        crate::ledger_info::redact_graph_source_record_config(&GraphSourceType::Iceberg, plain),
        plain
    );
}

// ---- the flat Iceberg converter's storage identity ----------------------------

fn rest_fields() -> IcebergConnectionFields {
    IcebergConnectionFields {
        catalog_uri: Some(URI.into()),
        ..Default::default()
    }
}

#[test]
fn a_surface_default_storage_identity_applies_only_when_none_is_named() {
    let with_default = |fields: IcebergConnectionFields| {
        IcebergConnectionFields {
            default_storage_auth: Some(StorageAuth::Vended),
            ..fields
        }
        .into_connection(&local())
        .unwrap()
        .io
        .storage_auth
    };
    assert_eq!(with_default(rest_fields()), Some(StorageAuth::Vended));
    // Turning vending off is a choice; so is naming an identity.
    assert_eq!(
        with_default(IcebergConnectionFields {
            no_vended_credentials: true,
            ..rest_fields()
        }),
        None
    );
    let named = with_default(IcebergConnectionFields {
        storage_auth: StorageAuthFields {
            identity: Some("ambient".into()),
            ..Default::default()
        },
        ..rest_fields()
    });
    assert_eq!(named, Some(StorageAuth::Ambient));
    // Never for a mode that has no catalog to vend.
    let direct = with_default(IcebergConnectionFields {
        mode: "direct".into(),
        table_location: Some("s3://b/w/ns/t".into()),
        ..Default::default()
    });
    assert_eq!(direct, None);
}

#[test]
fn a_flat_storage_identity_is_checked_like_any_other_value() {
    let e = IcebergConnectionFields {
        no_vended_credentials: true,
        storage_auth: StorageAuthFields {
            identity: Some("ambient".into()),
            ..Default::default()
        },
        ..rest_fields()
    }
    .into_connection(&local())
    .unwrap_err();
    assert_eq!(
        e,
        GraphSourceInputError::conflict("no_vended_credentials", "storage_identity")
    );
    let e = IcebergConnectionFields {
        storage_auth: StorageAuthFields {
            google: vec![(
                "storage_google_service_account_env",
                GoogleCredentials::ServiceAccount {
                    key: ConfigValue::from_env("AWS_SECRET_ACCESS_KEY"),
                },
            )],
            ..Default::default()
        },
        ..rest_fields()
    }
    .into_connection(&SecretRules::listed("SA_KEY", false))
    .unwrap_err();
    assert_eq!(e.field, "storage_google_service_account_env");
    assert!(
        matches!(e.kind, InputErrorKind::EnvNotAllowed { .. }),
        "{e}"
    );
}
