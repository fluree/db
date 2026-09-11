use super::*;

pub(super) async fn transport_state(mut config: ServerConfig) -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().unwrap();
    config.cors_enabled = false;
    config.indexing_enabled = false;
    config.storage_path = Some(tmp.path().to_path_buf());
    let telemetry = TelemetryConfig::with_server_config(&config);
    let state = Arc::new(AppState::new(config, telemetry).await.unwrap());
    (tmp, state)
}

#[tokio::test]
async fn storage_proxy_rejects_signed_delegation_without_downgrading_to_scopes() {
    let key = SigningKey::from_bytes(&[200; 32]);
    let issuer = did_from_pubkey(&key.verifying_key().to_bytes());
    let (_tmp, state) = transport_state(ServerConfig {
        storage_proxy_enabled: true,
        storage_proxy_trusted_issuers: vec![issuer.clone()],
        ..Default::default()
    })
    .await;
    let ledger = "storage-delegation:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let mut claims = serde_json::json!({
        "iss": issuer, "exp": now_secs() + 300,
        "fluree.storage.ledgers": [ledger]
    });
    for mode in ["ordinary", "delegated", "controller"] {
        let delegated = mode != "ordinary";
        claims.as_object_mut().unwrap().remove("fluree.policy");
        if mode == "delegated" {
            claims["fluree.policy"] = serde_json::json!({"default-allow": false});
        } else if mode == "controller" {
            claims["fluree.policy"] = "request".into();
        }
        let token = create_jws(&claims, &key);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/fluree/storage/ns/{ledger}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_body(resp).await;
        assert_eq!(
            status,
            if delegated {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::OK
            },
            "{body}"
        );
        if delegated {
            assert!(
                body.to_string()
                    .contains("not supported by storage proxy endpoints"),
                "{body}"
            );
            // Block fetch uses the same extractor, before any block lookup.
            let (status, body) = post_policy_request(
                &app,
                "/v1/fluree/storage/block",
                Some(&token),
                "application/json",
                "{}".into(),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        }
    }
}

#[tokio::test]
async fn push_preserves_signed_policy_and_rejected_commit_can_be_retried_with_a_grant() {
    use fluree_db_api::{Base64Bytes, PushCommitsRequest};
    use fluree_db_core::{Flake, FlakeValue, Sid};
    use fluree_db_novelty::Commit;
    use fluree_vocab::namespaces::{FLUREE_DB, XSD};

    let (_tmp, state) = policy_test_state().await;
    let app = build_router(state.clone());
    let ledger = "push-delegation:main";
    let (status, body) = post_policy_request(
        &app,
        "/v1/fluree/create",
        None,
        "application/json",
        serde_json::json!({"ledger": ledger}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let initial = state
        .fluree
        .ledger(ledger)
        .await
        .unwrap()
        .head_commit_id
        .clone();
    let commit = Commit::new(
        1,
        vec![Flake::new(
            Sid::new(FLUREE_DB, "alice"),
            Sid::new(FLUREE_DB, "name"),
            FlakeValue::String("Alice".into()),
            Sid::new(XSD, "string"),
            1,
            true,
            None,
        )],
    );
    let bytes = fluree_db_core::commit::codec::write_commit(&commit, true, None)
        .unwrap()
        .bytes;
    let request = serde_json::to_string(&PushCommitsRequest {
        commits: vec![Base64Bytes(bytes)],
        blobs: Default::default(),
        missing_blobs: vec![],
    })
    .unwrap();
    for allow in [false, true] {
        let token = delegated_token(
            "http://example.org/application-user",
            ledger,
            serde_json::json!({
                "policy": [{"f:action": ["f:view", "f:modify"], "f:allow": allow}],
                "default-allow": false
            }),
        );
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/push/{ledger}"))
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(request.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_body(resp).await;
        if allow {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["accepted"], 1, "{body}");
            assert_ne!(
                state.fluree.ledger(ledger).await.unwrap().head_commit_id,
                initial
            );
        } else {
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert!(body.to_string().to_lowercase().contains("policy"), "{body}");
            assert_eq!(
                state.fluree.ledger(ledger).await.unwrap().head_commit_id,
                initial
            );
        }
    }
}

#[cfg(feature = "oidc")]
#[tokio::test]
async fn oidc_delegation_requires_authority_and_constrains_reads_and_writes() {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock = MockServer::start().await;
    let issuer = "https://policy-issuer.example";
    let kid = "policy-rsa";
    let jwks = serde_json::json!({"keys": [{
        "kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
        "n": "qW0XZx4K2dAqsaNh4CdbaDyl79dtY2Cr7yKTD4lKunXuo1uE84VHRtLIdDw13GjG5fB1P7tjohAeQXYykJd2UaRZQzjiIExcYLnWQ6M1kC2DE4rsxOa2sPuHiKjdpd5XCgmKp-KmyroYn-Suyt3NjxtVeN1ko8bhJaVVR38kl_hmULEHcC8PvMZ5vVfuToxu95NMSU_QnxnHAQOSmoTqoNhqUCVLKxustsKBG-feS1ZvzJ3z0TklU8B_7oSKevuEq1hf8EbxnN2vAHL-uyko47twyc7LUFhufl4BETHmRZlJ4EsiPJ5Mye35d9sInq1VOZ3V2swXKF06kB8Lof2C2w",
        "e": "AQAB"
    }]});
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(jwks))
        .mount(&mock)
        .await;
    let key = EncodingKey::from_rsa_pem(include_bytes!(
        "../../fluree-db-credential/tests/fixtures/test_rsa_private.pem"
    ))
    .unwrap();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.into());
    for is_authority in [false, true] {
        let (_tmp, state) = transport_state(ServerConfig {
            data_auth_mode: DataAuthMode::Optional,
            data_auth_audience: Some("fluree-policy-test".into()),
            data_auth_policy_authorities: if is_authority {
                vec![issuer.into()]
            } else {
                vec![]
            },
            jwks_issuers: vec![format!("{issuer}={}/jwks", mock.uri())],
            storage_proxy_enabled: true,
            storage_proxy_trusted_issuers: vec![issuer.into()],
            ..Default::default()
        })
        .await;
        let ledger = "oidc-delegation:main";
        let app = setup_policy_ledger(build_router(state), ledger).await;
        add_modify_policies(&app, ledger).await;
        let mut claims = serde_json::json!({
            "iss": issuer, "aud": "fluree-policy-test", "exp": now_secs() + 300,
            "sub": "http://example.org/application-user",
            "fluree.ledger.read.ledgers": [ledger], "fluree.ledger.write.ledgers": [ledger],
            "fluree.storage.ledgers": [ledger],
            "fluree.policy": {"policy-class": ["http://example.org/EmployeeClass"]}
        });
        let mut controller = claims.clone();
        controller.as_object_mut().unwrap().remove("fluree.policy");
        controller["fluree.policy"] = "request".into();
        let controller = encode(&header, &controller, &key).unwrap();
        let query = serde_json::json!({
            "opts": {"policy-class": ["http://example.org/EmployeeClass"]},
            "select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}
        });
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/query/{ledger}"),
            Some(&controller),
            "application/json",
            query.to_string(),
        )
        .await;
        assert_eq!(
            status,
            if is_authority {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            },
            "{body}"
        );
        if is_authority {
            assert_eq!(body.as_array().unwrap().len(), 2, "{body}");
        }
        let token = encode(&header, &claims, &key).unwrap();
        let (status, body) = query_docs(app.clone(), ledger, Some(&token), false).await;
        if !is_authority {
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
            assert!(body.to_string().contains("policy authority"), "{body}");
            // Ordinary tokens from this same JWKS issuer still work.
            claims.as_object_mut().unwrap().remove("fluree.policy");
            claims["sub"] = serde_json::json!("http://example.org/employee-user");
            let token = encode(&header, &claims, &key).unwrap();
            let (status, body) = query_docs(app, ledger, Some(&token), false).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(names_from_results(&body).len(), 2, "{body}");
            continue;
        }
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(names_from_results(&body).len(), 2, "{body}");
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/update/{ledger}"),
            Some(&token),
            "application/json",
            modify_public_doc_content_body().to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body.to_string().contains("Employees may not modify"),
            "{body}"
        );
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/fluree/storage/ns/{ledger}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_body(resp).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        assert!(
            body.to_string()
                .contains("not supported by storage proxy endpoints"),
            "{body}"
        );

        let mut wrong_aud = claims.clone();
        wrong_aud["aud"] = serde_json::json!("another-server");
        let (status, body) = query_docs(
            app.clone(),
            ledger,
            Some(&encode(&header, &wrong_aud, &key).unwrap()),
            false,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        claims["fluree.policy"]["policy-class"] =
            serde_json::json!(["http://example.org/ManagerClass"]);
        let token = encode(&header, &claims, &key).unwrap();
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/update/{ledger}"),
            Some(&token),
            "application/json",
            modify_public_doc_content_body().to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        // Even a policy authority's grant cannot widen the signed ledger scopes.
        claims["fluree.ledger.read.ledgers"] = serde_json::json!(["different:main"]);
        claims
            .as_object_mut()
            .unwrap()
            .remove("fluree.storage.ledgers");
        let (status, body) = query_docs(
            app,
            ledger,
            Some(&encode(&header, &claims, &key).unwrap()),
            false,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }
}
