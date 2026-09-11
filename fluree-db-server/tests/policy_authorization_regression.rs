use super::*;

#[path = "policy_transport_regression.rs"]
mod policy_transport_regression;

async fn post_policy_request(
    app: &axum::Router,
    uri: &str,
    token: Option<&str>,
    content_type: &str,
    body: String,
) -> (StatusCode, JsonValue) {
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    json_body(
        app.clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap(),
    )
    .await
}

#[tokio::test]
async fn configured_model_policy_controls_delegated_reads_and_writes() {
    let (_tmp, state) = policy_test_state().await;
    let fluree = state.fluree.clone();
    let model = "authority-model:main";
    let app = setup_policy_ledger(build_router(state), model).await;
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/upsert/{model}"),
        None,
        "application/trig",
        r#"@prefix f: <https://ns.flur.ee/db#> .
           @prefix ex: <http://example.org/> .
           GRAPH <http://example.org/model-policies> {
               ex:protectContent a f:AccessPolicy, ex:ModelRules ;
                   f:required true ; f:onProperty ex:content ;
                   f:action f:view, f:modify ; f:allow false ;
                   f:exMessage "Model forbids content changes." .
               ex:manager a f:AccessPolicy, ex:ManagerClass ;
                   f:action f:view, f:modify ; f:allow true .
           }"#
        .into(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");

    // An explicit policy authority may select grants only where the ledger's
    // own override control permits it. Exercise both sides of that boundary.
    for (ledger, control, allowed) in [
        ("authority-model-locked:main", "OverrideNone", false),
        ("authority-model-open:main", "OverrideAll", true),
    ] {
        let app = setup_policy_ledger(app.clone(), ledger).await;
        let config = format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix ex: <http://example.org/> .
            GRAPH <urn:fluree:{ledger}#config> {{
                <urn:cfg:main> a f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true ; f:policyClass ex:ModelRules ;
                    f:overrideControl f:{control} ; f:policySource <urn:cfg:ref> .
                <urn:cfg:ref> a f:GraphRef ; f:graphSource <urn:cfg:source> .
                <urn:cfg:source> f:ledger <{model}> ;
                    f:graphSelector <http://example.org/model-policies> .
            }}"
        );
        let (status, result) = post_policy_request(
            &app,
            &format!("/v1/fluree/upsert/{ledger}"),
            None,
            "application/trig",
            config,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        let grant = serde_json::json!({
            "policy-class": ["http://example.org/ManagerClass"],
            "policy": [{"f:required": true, "f:action": ["f:view", "f:modify"], "f:allow": true}],
            "policy-values": {"?$identity": {"@id": "http://example.org/manager-user"}},
            "default-allow": true
        });
        let delegated =
            delegated_token("http://example.org/application-user", ledger, grant.clone());
        let key = SigningKey::from_bytes(&[205; 32]);
        let controller_key = SigningKey::from_bytes(&[200; 32]);
        if allowed {
            let controller = create_jws(&controller_claims(ledger), &controller_key);
            let empty = serde_json::json!({
                "opts": {"identity": "http://example.org/manager-user", "policy-class": []},
                "select": ["?value"],
                "where": {"@id": "http://example.org/doc1", "http://schema.org/name": "?value"}
            });
            let (status, body) = post_policy_request(
                &app,
                &format!("/v1/fluree/query/{ledger}"),
                Some(&controller),
                "application/json",
                empty.to_string(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(
                body,
                serde_json::json!([]),
                "empty groups must not inherit model defaults"
            );
        }

        let mut tokens = vec![
            delegated,
            create_jws(&controller_claims(ledger), &controller_key),
        ];
        if !allowed {
            tokens.push(identity_token_rw(
                &key,
                "http://example.org/employee-user",
                ledger,
            ));
            tokens.push(create_jws(
                &serde_json::json!({
                    "iss": did_from_pubkey(&key.verifying_key().to_bytes()),
                    "aud": "fluree-policy-test", "exp": now_secs() + 300,
                    "fluree.ledger.read.ledgers": [ledger],
                    "fluree.ledger.write.ledgers": [ledger]
                }),
                &key,
            ));
        }
        for (index, token) in tokens.into_iter().enumerate() {
            let request_opts = if index < 2 {
                grant.clone()
            } else {
                serde_json::json!({})
            };
            for (property, expected) in [
                ("http://schema.org/name", 1),
                ("http://example.org/content", usize::from(allowed)),
            ] {
                let query = serde_json::json!({
                    "opts": request_opts, "select": ["?value"],
                    "where": {"@id": "http://example.org/doc1", (property): "?value"}
                });
                let (status, result) = post_policy_request(
                    &app,
                    &format!("/v1/fluree/query/{ledger}"),
                    Some(&token),
                    "application/json",
                    query.to_string(),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{control}: {result}");
                assert_eq!(
                    result.as_array().unwrap().len(),
                    expected,
                    "{control}: {result}"
                );
            }
            // An unconditional write reaches modify enforcement even when
            // the read policy hides content from transaction WHERE matching.
            let mut body = serde_json::json!({
                "delete": {"@id": "http://example.org/doc1", "http://example.org/content": "visible to all"},
                "insert": {"@id": "http://example.org/doc1", "http://example.org/content": "rewritten"}
            });
            body["opts"] = request_opts;
            let (status, result) = post_policy_request(
                &app,
                &format!("/v1/fluree/update/{ledger}"),
                Some(&token),
                "application/json",
                body.to_string(),
            )
            .await;
            assert_eq!(
                status,
                if allowed {
                    StatusCode::OK
                } else {
                    StatusCode::BAD_REQUEST
                },
                "{control}: {result}"
            );
            if !allowed {
                assert!(
                    result
                        .to_string()
                        .contains("Model forbids content changes."),
                    "{result}"
                );
            }
        }
        // A trusted SDK read verifies persisted state independently of the
        // HTTP filter that intentionally hides content on the locked ledger.
        let view = fluree.db(ledger).await.unwrap();
        let query = serde_json::json!({"select": ["?value"], "where": {"@id": "http://example.org/doc1", "http://example.org/content": "?value"}});
        let result = fluree
            .query(&view, &query)
            .await
            .unwrap()
            .to_jsonld(&view.snapshot)
            .unwrap();
        assert_eq!(
            result,
            serde_json::json!([[if allowed {
                "rewritten"
            } else {
                "visible to all"
            }]])
        );
    }
}

#[cfg(feature = "credential")]
#[tokio::test]
async fn signed_request_identity_does_not_inherit_bearer_delegation() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "authority-signed-body:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let key = SigningKey::from_bytes(&[206; 32]);
    let identity = did_from_pubkey(&key.verifying_key().to_bytes());
    let assignment = serde_json::json!({"insert": {
        "@id": identity,
        "https://ns.flur.ee/db#policyClass": {"@id": "http://example.org/EmployeeClass"}
    }});
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/insert/{ledger}"),
        None,
        "application/json",
        assignment.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let bearer = delegated_token(
        "http://example.org/application-user",
        ledger,
        serde_json::json!({"policy-class": ["http://example.org/ManagerClass"]}),
    );
    let (status, result) = query_docs(app.clone(), ledger, Some(&bearer), false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(names_from_results(&result).len(), 3);
    let query = serde_json::json!({
        "opts": {"identity": "http://example.org/manager-user", "default-allow": true},
        "select": ["?name", "?class"],
        "where": {"@id": "?s", "http://schema.org/name": "?name", "http://example.org/classification": "?class"}
    });
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/query/{ledger}"),
        Some(&bearer),
        "application/jwt",
        create_jws(&query, &key),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{result}");
}

#[cfg(feature = "graphql")]
#[tokio::test]
async fn graphql_mutation_preserves_verified_authorization() {
    let (_tmp, state) = policy_test_state().await;
    let fluree = state.fluree.clone();
    let ledger = "authority-graphql:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    add_modify_policies(&app, ledger).await;
    let context = serde_json::json!({
        "ex": "http://example.org/", "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#", "f": "https://ns.flur.ee/db#",
        "graphql": "http://datashapes.org/graphql#"
    });
    fluree.set_default_context(ledger, &context).await.unwrap();
    let shapes = serde_json::json!({"@context": context, "insert": [
        {"@id": "ex:DocumentShape", "@type": "sh:NodeShape",
         "sh:targetClass": {"@id": "ex:Document"}, "sh:property": [
            {"sh:path": {"@id": "ex:content"}, "sh:datatype": {"@id": "xsd:string"}, "sh:maxCount": 1},
            {"sh:path": {"@id": "ex:classification"}, "sh:datatype": {"@id": "xsd:string"}, "sh:maxCount": 1}
         ]},
        {"@id": "ex:Api", "@type": "graphql:Schema",
         "graphql:publicShape": {"@id": "ex:DocumentShape"},
         "f:graphqlEnableMutations": true, "f:graphqlIriBase": "http://example.org/"}
    ]});
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/insert/{ledger}"),
        None,
        "application/json",
        shapes.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let mutation = serde_json::json!({"query": r#"mutation {
        create_Document(input: { id: "ex:doc4", content: "new", classification: "public" }) { id content }
    }"#});
    let denied = delegated_token(
        "http://example.org/application-user",
        ledger,
        serde_json::json!({"policy-class": ["http://example.org/EmployeeClass"]}),
    );
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/graphql/{ledger}"))
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {denied}"))
                .body(Body::from(mutation.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, result) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert!(
        result.get("errors").is_some(),
        "restricted mutation must fail: {result}"
    );
    assert!(
        result.to_string().contains("Policy enforcement"),
        "{result}"
    );
    let view = fluree.db(ledger).await.unwrap();
    let probe = serde_json::json!({"select": ["?v"], "where": {"@id": "http://example.org/doc4", "http://example.org/content": "?v"}});
    assert_eq!(
        fluree
            .query(&view, &probe)
            .await
            .unwrap()
            .to_jsonld(&view.snapshot)
            .unwrap(),
        serde_json::json!([])
    );

    let allowed = delegated_token(
        "http://example.org/application-user",
        ledger,
        serde_json::json!({"policy-class": ["http://example.org/ManagerClass"]}),
    );
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/graphql/{ledger}"),
        Some(&allowed),
        "application/json",
        mutation.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert!(result.get("errors").is_none(), "{result}");
    assert_eq!(
        result["data"]["create_Document"]["content"], "new",
        "{result}"
    );

    // Write authority need not imply read authority. A successful mutation
    // must not expose a newly created subject that fails the caller's view rule.
    let write_only = delegated_token(
        "http://example.org/application-user",
        ledger,
        serde_json::json!({"policy": [
            {"f:action": "f:view", "f:query": {"where": {
                "@id": "?$this", "http://example.org/classification": "public"
            }}},
            {"f:action": "f:modify", "f:allow": true}
        ], "default-allow": false}),
    );
    let mutation = serde_json::json!({"query": r#"mutation {
        create_Document(input: { id: "ex:doc5", content: "hidden", classification: "confidential" }) { id content }
    }"#});
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/graphql/{ledger}"),
        Some(&write_only),
        "application/json",
        mutation.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert!(result.get("errors").is_none(), "{result}");
    assert_eq!(
        result["data"]["create_Document"],
        JsonValue::Null,
        "{result}"
    );
    let view = fluree.db(ledger).await.unwrap();
    let probe = serde_json::json!({"select": ["?v"], "where": {"@id": "http://example.org/doc5", "http://example.org/content": "?v"}});
    assert_eq!(
        fluree
            .query(&view, &probe)
            .await
            .unwrap()
            .to_jsonld(&view.snapshot)
            .unwrap(),
        serde_json::json!([["hidden"]])
    );
}

#[tokio::test]
async fn restricted_bearer_cannot_replace_or_supplement_policy() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "audit-http:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let signing_key = SigningKey::from_bytes(&[83u8; 32]);
    let token = identity_token(&signing_key, "http://example.org/employee-user", ledger);
    let (status, baseline) = query_docs(app.clone(), ledger, Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(names_from_results(&baseline).len(), 2);
    for opts in [
        serde_json::json!({"policy-class": ["http://example.org/ManagerClass"], "default-allow": false}),
        serde_json::json!({"policy": [{"f:required": true, "f:action": "f:view", "f:allow": true}], "default-allow": false}),
        serde_json::json!({"policy-class": ["http://example.org/NonexistentClass"], "default-allow": true}),
        serde_json::json!({"policyClass": ["http://example.org/ManagerClass"], "defaultAllow": true}),
        serde_json::json!({"policy_class": ["http://example.org/ManagerClass"], "default_allow": true}),
        serde_json::json!({"policyValues": {"?$identity": {"@id": "http://example.org/manager-user"}}}),
    ] {
        let body = serde_json::json!({
            "opts": opts,
            "select": ["?name", "?class"],
            "where": [{"@id": "?doc", "@type": "http://example.org/Document", "http://schema.org/name": "?name", "http://example.org/classification": "?class"}]
        });
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/query/{ledger}"))
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, result) = json_body(resp).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{result}");
        assert!(result
            .to_string()
            .contains("Credential does not permit policy selection"));
    }
    let body = serde_json::json!({
        "from": {"@id": ledger, "policy": {"identity": "http://example.org/manager-user", "default-allow": false}},
        "select": ["?name", "?class"],
        "where": [{"@id": "?doc", "@type": "http://example.org/Document", "http://schema.org/name": "?name", "http://example.org/classification": "?class"}]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, result) = json_body(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{result}");
}

#[tokio::test]
async fn signed_policy_context_requires_authority_scope_and_valid_signature() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "authority-validation:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let authority = SigningKey::from_bytes(&[200; 32]);
    let ordinary = SigningKey::from_bytes(&[201; 32]);
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&authority.verifying_key().to_bytes()),
        "aud": "fluree-policy-test", "exp": now_secs() + 300,
        "fluree.identity": "http://example.org/application-user",
        "fluree.ledger.read.ledgers": [ledger],
        "fluree.policy": {"policy-class": ["http://example.org/EmployeeClass"]}
    });
    let valid = create_jws(&claims, &authority);
    let (status, result) = query_docs(app.clone(), ledger, Some(&valid), false).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(
        names_from_results(&result).len(),
        2,
        "grant-derived classes work without local identity assignments"
    );

    for (label, mut invalid, signer, expected) in [
        (
            "issuer",
            claims.clone(),
            &ordinary,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "audience",
            claims.clone(),
            &authority,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "expired",
            claims.clone(),
            &authority,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "future",
            claims.clone(),
            &authority,
            StatusCode::UNAUTHORIZED,
        ),
        ("ledger", claims.clone(), &authority, StatusCode::NOT_FOUND),
        ("action", claims.clone(), &authority, StatusCode::NOT_FOUND),
        (
            "unknown-field",
            claims.clone(),
            &authority,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "null-context",
            claims.clone(),
            &authority,
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        match label {
            "issuer" => {
                invalid["iss"] =
                    serde_json::json!(did_from_pubkey(&ordinary.verifying_key().to_bytes()));
            }
            "null-context" => invalid["fluree.policy"] = JsonValue::Null,
            "audience" => invalid["aud"] = serde_json::json!("another-server"),
            "expired" => invalid["exp"] = serde_json::json!(now_secs() - 120),
            "future" => invalid["nbf"] = serde_json::json!(now_secs() + 300),
            "ledger" => {
                invalid["fluree.ledger.read.ledgers"] = serde_json::json!(["elsewhere:main"]);
            }
            "action" => {
                invalid
                    .as_object_mut()
                    .unwrap()
                    .remove("fluree.ledger.read.ledgers");
                invalid["fluree.ledger.write.ledgers"] = serde_json::json!([ledger]);
            }
            _ => invalid["fluree.policy"]["trusted"] = serde_json::json!(true),
        }
        let token = create_jws(&invalid, signer);
        let (status, result) = query_docs(app.clone(), ledger, Some(&token), false).await;
        assert_eq!(status, expected, "{label}: {result}");
    }
    let unrestricted = delegated_token(
        "http://example.org/new-application-user",
        ledger,
        serde_json::json!({"default-allow": true}),
    );
    let (status, result) = query_docs(app.clone(), ledger, Some(&unrestricted), true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(names_from_results(&result).len(), 3);
    let (status, result) = query_docs(app.clone(), ledger, Some(&unrestricted), false).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        names_from_results(&result).is_empty(),
        "caller default-deny must narrow signed default-allow"
    );

    let pieces: Vec<_> = valid.split('.').collect();
    let mut tampered = claims.clone();
    tampered["fluree.policy"]["policy-class"] =
        serde_json::json!(["http://example.org/ManagerClass"]);
    let token = format!(
        "{}.{}.{}",
        pieces[0],
        URL_SAFE_NO_PAD.encode(tampered.to_string()),
        pieces[2]
    );
    let (status, _) = query_docs(app.clone(), ledger, Some(&token), false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A record with no policy assignments is not proof of delegation authority.
    register_root_identity(&app, ledger, "http://example.org/unassigned").await;
    let token = identity_token(&ordinary, "http://example.org/unassigned", ledger);
    let (status, result) =
        query_docs_as(app, ledger, &token, "http://example.org/manager-user").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{result}");
}

#[tokio::test]
async fn policy_headers_cannot_widen_sparql_reads() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "authority-text:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let token = identity_token(
        &SigningKey::from_bytes(&[202; 32]),
        "http://example.org/employee-user",
        ledger,
    );
    for (uri, content_type, body, _stream) in [
        (format!("/v1/fluree/query/{ledger}"), "application/sparql-query", "SELECT ?name WHERE { ?doc a <http://example.org/Document>; <http://schema.org/name> ?name }".to_string(), false),
        ("/v1/fluree/query".into(), "application/sparql-query", format!("SELECT ?name FROM <{ledger}> WHERE {{ ?doc a <http://example.org/Document>; <http://schema.org/name> ?name }}"), false),
        ("/v1/fluree/stream/query".into(), "application/sparql-query", format!("SELECT ?name FROM <{ledger}> WHERE {{ ?doc a <http://example.org/Document>; <http://schema.org/name> ?name }}"), true),
        (format!("/v1/fluree/stream/query/{ledger}"), "application/sparql-query", "SELECT ?name WHERE { ?doc a <http://example.org/Document>; <http://schema.org/name> ?name }".to_string(), true),
    ] {
        let resp = app.clone().oneshot(Request::builder().method("POST").uri(&uri)
            .header("content-type", content_type).header("authorization", format!("Bearer {token}"))
            .header("fluree-identity", "http://example.org/manager-user")
            .header("fluree-policy-class", "http://example.org/ManagerClass")
            .header("fluree-policy", r#"[{"f:required":true,"f:action":"f:view","f:allow":true}]"#)
            .header("fluree-policy-values", r#"{"?$identity":{"@id":"http://example.org/manager-user"}}"#)
            .header("fluree-default-allow", "true")
            .body(Body::from(body)).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{uri}");
    }
}

#[tokio::test]
async fn hostile_policy_selection_cannot_authorize_a_write() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "authority-write:main";
    let fluree = state.fluree.clone();
    let app = setup_policy_ledger(build_router(state), ledger).await;
    add_modify_policies(&app, ledger).await;
    fluree
        .set_default_context(
            ledger,
            &serde_json::json!({"@vocab": "http://example.org/"}),
        )
        .await
        .unwrap();
    let token = identity_token_rw(
        &SigningKey::from_bytes(&[203; 32]),
        "http://example.org/employee-user",
        ledger,
    );
    let hostile = serde_json::json!({
        "identity": "http://example.org/manager-user",
        "policyClass": ["http://example.org/ManagerClass"],
        "policy": [{"f:required": true, "f:action": "f:modify", "f:allow": true}],
        "defaultAllow": true
    });
    let mut body = modify_public_doc_content_body();
    body["opts"] = hostile;
    for (route, content_type, body) in [
        ("update", "application/json", body.to_string()),
        (
            "update",
            "application/sparql-update",
            r#"PREFIX ex: <http://example.org/>
            DELETE { ex:doc1 ex:content ?c } INSERT { ex:doc1 ex:content "rewritten" }
            WHERE { ex:doc1 ex:content ?c }"#
                .into(),
        ),
        (
            "update",
            "application/cypher",
            r#"MATCH (d:Document {classification: "public"}) SET d.content = "rewritten""#.into(),
        ),
        (
            "insert",
            "text/turtle",
            r#"<http://example.org/doc1> <http://example.org/content> "rewritten" ."#.into(),
        ),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/{route}/{ledger}"))
                    .header("content-type", content_type)
                    .header("authorization", format!("Bearer {token}"))
                    .header("fluree-identity", "http://example.org/manager-user")
                    .header("fluree-policy-class", "http://example.org/ManagerClass")
                    .header(
                        "fluree-policy",
                        r#"[{"f:required":true,"f:action":"f:modify","f:allow":true}]"#,
                    )
                    .header("fluree-default-allow", "true")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, error) = json_body(resp).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{content_type}: {error}");
        assert!(error
            .to_string()
            .contains("Credential does not permit policy selection"));
    }
    let query = serde_json::json!({"select": ["?content"], "where": {"@id": "http://example.org/doc1", "http://example.org/content": "?content"}});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/query/{ledger}"))
                .header("content-type", "application/json")
                .body(Body::from(query.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, result) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result, serde_json::json!([["visible to all"]]));
}

#[tokio::test]
async fn dataset_options_and_envelope_defaults_cannot_replace_authority() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "authority-dataset:main";
    let other = "authority-other:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let app = setup_policy_ledger(app, other).await;
    let token = identity_token(
        &SigningKey::from_bytes(&[204; 32]),
        "http://example.org/employee-user",
        ledger,
    );
    let query = serde_json::json!({
        "from": ledger,
        "opts": {"from": {"@id": ledger, "policy": {"identity": "http://example.org/manager-user"}}},
        "select": ["?name", "?class"],
        "where": {"@id": "?s", "http://schema.org/name": "?name", "http://example.org/classification": "?class"}
    });
    for uri in [
        "/v1/fluree/query".to_string(),
        format!("/v1/fluree/query/{ledger}"),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&uri)
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(query.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, rows) = json_body(resp).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {rows}");
        let mut redirected = query.clone();
        redirected["opts"]["from"]["@id"] = serde_json::json!(other);
        redirected["opts"]["from"]
            .as_object_mut()
            .unwrap()
            .remove("policy");
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&uri)
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(redirected.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    let envelope = serde_json::json!({
        "opts": {"policy-class": ["http://example.org/ManagerClass"], "policy": [{"f:required": true, "f:action": "f:view", "f:allow": true}], "default-allow": true},
        "queries": {
            "jsonld": {"language": "jsonld", "query": query, "opts": {"policyClass": ["http://example.org/ManagerClass"]}},
            "sparql": {"language": "sparql", "query": format!("SELECT ?name FROM <{ledger}> WHERE {{ ?s a <http://example.org/Document>; <http://schema.org/name> ?name }}")}
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/multi-query")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(envelope.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, result) = json_body(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{result}");
    assert!(!result.to_string().contains("Executive Salaries"));
    assert!(
        result
            .to_string()
            .contains("Credential does not permit policy selection"),
        "{result}"
    );
}

#[tokio::test]
async fn controller_selects_dynamic_policies_without_changing_credential() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "controller:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    add_modify_policies(&app, ledger).await;
    let key = SigningKey::from_bytes(&[200; 32]);
    let token = create_jws(&controller_claims(ledger), &key);
    for (class, count) in [
        ("PublicClass", 1),
        ("EmployeeClass", 2),
        ("ManagerClass", 3),
    ] {
        let query = serde_json::json!({
            "opts": {"identity": "http://example.org/manager-user", "policyClass": [format!("http://example.org/{class}")]},
            "from": [{"@id": ledger}],
            "select": ["?name"],
            "where": {"@id": "?s", "http://schema.org/name": "?name"}
        });
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/query/{ledger}"),
            Some(&token),
            "application/json",
            query.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body.as_array().unwrap().len(), count, "{body}");
    }
    // An empty set of application grants is not the same as omitting policy
    // selection. Both missing selection and empty grants fail closed.
    for (opts, expected) in [
        (serde_json::json!({}), 0),
        (serde_json::json!({"policy-class": []}), 0),
        (
            serde_json::json!({"policy-values": {"?unused": "value"}}),
            0,
        ),
        (
            serde_json::json!({"identity": "http://example.org/manager-user", "policy-class": []}),
            0,
        ),
        (serde_json::json!({"default-allow": false}), 0),
    ] {
        let query = serde_json::json!({"opts": opts, "select": ["?name"],
            "where": {"@id": "?s", "http://schema.org/name": "?name"}});
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/query/{ledger}"),
            Some(&token),
            "application/json",
            query.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body.as_array().unwrap().len(), expected, "{body}");
    }
    // Identity can be chosen dynamically without specifying a class as well.
    let query = serde_json::json!({"opts": {"identity": "http://example.org/employee-user"},
        "select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}});
    let (status, body) = post_policy_request(
        &app,
        &format!("/v1/fluree/query/{ledger}"),
        Some(&token),
        "application/json",
        query.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 2, "{body}");

    // Inline policy and variable bindings also belong to the app, not to the
    // credential subject. Reuse the same token to choose another user's data.
    assign_docs_to_identities(&app, ledger).await;
    let query = serde_json::json!({
        "opts": {
            "policy": [{"@type": "f:AccessPolicy", "f:action": [{"@id": "f:view"}],
                "f:query": {"@type": "@json", "@value": {
                    "where": {"@id": "?$this", "http://example.org/assignedTo": "?$identity"}
                }}
            }],
            "policy-values": {"?$identity": {"@id": "http://example.org/employee-user"}},
            "default-allow": false
        },
        "select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}
    });
    let (status, body) = post_policy_request(
        &app,
        &format!("/v1/fluree/query/{ledger}"),
        Some(&token),
        "application/json",
        query.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, serde_json::json!([["Internal Memo"]]));

    // SPARQL uses the same selection via existing headers.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/query/{ledger}"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/sparql-query")
                .header("fluree-policy-class", "http://example.org/EmployeeClass")
                .body(Body::from(
                    "SELECT ?name WHERE { ?s <http://schema.org/name> ?name }",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = json_body(response).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["results"]["bindings"].as_array().unwrap().len(),
        2,
        "{body}"
    );

    for (class, expected) in [
        ("EmployeeClass", StatusCode::BAD_REQUEST),
        ("ManagerClass", StatusCode::OK),
    ] {
        let mut update = modify_public_doc_content_body();
        update["opts"] = serde_json::json!({"policy-class": [format!("http://example.org/{class}")], "default-allow": true});
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/update/{ledger}"),
            Some(&token),
            "application/json",
            update.to_string(),
        )
        .await;
        assert_eq!(status, expected, "{body}");
    }
}

#[tokio::test]
async fn controller_capability_requires_explicit_claim_authority_and_scope() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "controller-gates:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let key = SigningKey::from_bytes(&[200; 32]);
    let query = serde_json::json!({"opts": {"policy-class": ["http://example.org/ManagerClass"], "default-allow": true},
        "select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}});
    let mut ordinary = controller_claims(ledger);
    ordinary.as_object_mut().unwrap().remove("fluree.policy");
    let ordinary = create_jws(&ordinary, &key);
    let (status, body) = post_policy_request(
        &app,
        &format!("/v1/fluree/query/{ledger}"),
        Some(&ordinary),
        "application/json",
        query.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    for case in [
        "untrusted-authority",
        "invalid-policy-mode",
        "wrong-audience",
        "expired",
        "wrong-scope",
        "null-claim",
    ] {
        let mut claims = controller_claims(ledger);
        let other = SigningKey::from_bytes(&[201; 32]);
        let signing_key = if case == "untrusted-authority" {
            &other
        } else {
            &key
        };
        claims["iss"] = did_from_pubkey(&signing_key.verifying_key().to_bytes()).into();
        match case {
            "invalid-policy-mode" => {
                claims["fluree.policy"] = serde_json::json!(["request", {"default-allow": false}]);
            }
            "wrong-audience" => claims["aud"] = "other-server".into(),
            "expired" => claims["exp"] = 1.into(),
            "wrong-scope" => {
                claims["fluree.ledger.read.ledgers"] = serde_json::json!(["other:main"]);
            }
            "null-claim" => claims["fluree.policy"] = JsonValue::Null,
            _ => {}
        }
        let token = create_jws(&claims, signing_key);
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/query/{ledger}"),
            Some(&token),
            "application/json",
            query.to_string(),
        )
        .await;
        assert_eq!(
            status,
            if case == "wrong-scope" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::UNAUTHORIZED
            },
            "{case}: {body}"
        );
    }
    let mut read_only = controller_claims(ledger);
    read_only
        .as_object_mut()
        .unwrap()
        .remove("fluree.ledger.write.ledgers");
    let token = create_jws(&read_only, &key);
    let (status, body) = post_policy_request(
        &app,
        &format!("/v1/fluree/update/{ledger}"),
        Some(&token),
        "application/json",
        modify_public_doc_content_body().to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn scope_only_preserves_delimited_output_and_ledger_write_defaults() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "scope-defaults:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    add_modify_policies(&app, ledger).await;
    let key = SigningKey::from_bytes(&[200; 32]);
    let mut claims = controller_claims(ledger);
    claims.as_object_mut().unwrap().remove("fluree.policy");
    claims.as_object_mut().unwrap().remove("sub");
    let token = create_jws(&claims, &key);
    let query = serde_json::json!({
        "select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}});
    for format in ["text/csv", "text/tab-separated-values"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/query/{ledger}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .header("accept", format)
                    .body(Body::from(query.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{format}");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(
            text.contains("Executive Salaries"),
            "scope-only request has no policy selection: {text}"
        );
    }
    let config = format!(
        r"@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
        GRAPH <urn:fluree:{ledger}#config> {{
          <urn:cfg:main> a f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> .
          <urn:cfg:policy> f:defaultAllow true ; f:policyClass ex:EmployeeClass .
        }}"
    );
    let (status, body) = post_policy_request(
        &app,
        &format!("/v1/fluree/upsert/{ledger}"),
        None,
        "application/trig",
        config,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Default governance must survive both the plain and dataset read paths,
    // including streaming and delimited output after the null-marker fix.
    for (route, content_type, accept, dataset) in [
        ("query", "application/json", "application/json", false),
        ("query", "application/json", "application/json", true),
        ("query", "application/json", "text/csv", false),
        (
            "query",
            "application/sparql-query",
            "application/json",
            false,
        ),
        (
            "stream/query",
            "application/json",
            "application/x-ndjson",
            false,
        ),
        (
            "stream/query",
            "application/sparql-query",
            "application/x-ndjson",
            false,
        ),
    ] {
        let mut query = serde_json::json!({"select": ["?name"],
            "where": {"@id": "?s", "http://schema.org/name": "?name"}});
        if dataset {
            query["from"] = serde_json::json!([ledger]);
        }
        let body = if content_type == "application/json" {
            query.to_string()
        } else {
            "SELECT ?name WHERE { ?s <http://schema.org/name> ?name }".into()
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/{route}/{ledger}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", content_type)
                    .header("accept", accept)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{route}: {content_type}");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("Public Post"), "{route}: {text}");
        assert!(!text.contains("Executive Salaries"), "{route}: {text}");
    }
    for bearer in [None, Some(token.as_str())] {
        let (status, body) = post_policy_request(
            &app,
            &format!("/v1/fluree/update/{ledger}"),
            bearer,
            "application/json",
            modify_public_doc_content_body().to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body.to_string().contains("Employees may not modify"),
            "{body}"
        );
    }
}

#[tokio::test]
async fn anonymous_transaction_policy_headers_apply_without_tracking() {
    for mode in [DataAuthMode::None, DataAuthMode::Optional] {
        let (_tmp, state) = policy_transport_regression::transport_state(ServerConfig {
            data_auth_mode: mode,
            ..Default::default()
        })
        .await;
        let ledger = "anonymous-header-defaults:main";
        let app = setup_policy_ledger(build_router(state), ledger).await;
        add_modify_policies(&app, ledger).await;

        // No tracking headers or opts: the employee header must still restrict
        // this write. A body selection takes precedence over that default.
        for (body_class, expected) in [
            (None, StatusCode::BAD_REQUEST),
            (Some("http://example.org/ManagerClass"), StatusCode::OK),
        ] {
            let mut body = modify_public_doc_content_body();
            if let Some(class) = body_class {
                body["opts"] = serde_json::json!({"policy-class": [class]});
            }
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/v1/fluree/update/{ledger}"))
                        .header("content-type", "application/json")
                        .header("fluree-policy-class", "http://example.org/EmployeeClass")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let (status, body) = json_body(response).await;
            assert_eq!(status, expected, "{mode:?}: {body}");
            if expected == StatusCode::BAD_REQUEST {
                assert!(
                    body.to_string().contains("Employees may not modify"),
                    "{body}"
                );
            }
        }
    }
}

#[tokio::test]
async fn show_preserves_narrowing_delegation_and_controller_headers() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "show-delegation:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let (status, body) = post_policy_request(&app, &format!("/v1/fluree/insert/{ledger}"), None, "application/json",
        serde_json::json!({"@context": {"ex": "http://example.org/", "schema": "http://schema.org/"}, "insert": [
            {"@id": "ex:new-public", "schema:name": "New public", "ex:classification": "public"},
            {"@id": "ex:new-secret", "schema:name": "New secret", "ex:classification": "confidential"}
        ]}).to_string()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let t = body["t"].as_i64().unwrap();
    let delegated = delegated_token(
        "http://example.org/manager-user",
        ledger,
        serde_json::json!({"policy-class": ["http://example.org/EmployeeClass"]}),
    );
    let key = SigningKey::from_bytes(&[200; 32]);
    let controller = create_jws(&controller_claims(ledger), &key);
    for (token, class, secret) in [
        (&delegated, "EmployeeClass", false),
        (&controller, "EmployeeClass", false),
        (&controller, "ManagerClass", true),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/fluree/show/{ledger}?commit=t:{t}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("fluree-policy-class", format!("http://example.org/{class}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, body) = json_body(response).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let flakes = body["flakes"].to_string();
        assert!(flakes.contains("New public"), "{body}");
        assert_eq!(flakes.contains("New secret"), secret, "{body}");
    }
    // Binding must reject a conflicting fixed header, and a request-selected
    // credential with no context must keep the commit flakes hidden.
    for (token, class, expected) in [
        (&delegated, Some("ManagerClass"), StatusCode::FORBIDDEN),
        (&controller, None, StatusCode::OK),
    ] {
        let mut request = Request::builder()
            .uri(format!("/v1/fluree/show/{ledger}?commit=t:{t}"))
            .header("authorization", format!("Bearer {token}"));
        if let Some(class) = class {
            request = request.header("fluree-policy-class", format!("http://example.org/{class}"));
        }
        let (status, body) = json_body(
            app.clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, expected, "{body}");
        if expected == StatusCode::OK {
            assert_eq!(body["flakes"], serde_json::json!([]), "{body}");
        }
    }
}

#[tokio::test]
async fn mcp_refuses_policy_credentials_instead_of_discarding_context() {
    use fluree_db_server::mcp::auth::validate_mcp_token;
    let key = SigningKey::from_bytes(&[200; 32]);
    let issuer = did_from_pubkey(&key.verifying_key().to_bytes());
    let (_tmp, state) = policy_transport_regression::transport_state(ServerConfig {
        mcp_auth_trusted_issuers: vec![issuer],
        ..Default::default()
    })
    .await;
    // Use the actual authentication middleware with a sentinel handler, so the
    // ordinary credential proves that the refusal isn't a missing route.
    let app = axum::Router::new()
        .route("/", axum::routing::get(|| async { StatusCode::OK }))
        .layer(axum::middleware::from_fn_with_state(
            state,
            validate_mcp_token,
        ));
    for mode in ["ordinary", "delegated", "controller"] {
        let mut claims = controller_claims("mcp:main");
        claims.as_object_mut().unwrap().remove("fluree.policy");
        match mode {
            "delegated" => {
                claims["fluree.policy"] =
                    serde_json::json!({"policy-class": ["http://example.org/EmployeeClass"]});
            }
            "controller" => claims["fluree.policy"] = "request".into(),
            _ => {}
        }
        let token = create_jws(&claims, &key);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if mode == "ordinary" {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            },
            "{mode}"
        );
    }
}

fn controller_claims(ledger: &str) -> JsonValue {
    let key = SigningKey::from_bytes(&[200; 32]);
    serde_json::json!({
        "iss": did_from_pubkey(&key.verifying_key().to_bytes()),
        "aud": "fluree-policy-test", "sub": "http://example.org/public-user",
        "exp": now_secs() + 300,
        "fluree.ledger.read.ledgers": [ledger],
        "fluree.ledger.write.ledgers": [ledger],
        "fluree.policy": "request"
    })
}

#[tokio::test]
async fn authority_alone_authenticates_and_missing_request_context_denies() {
    let key = SigningKey::from_bytes(&[200; 32]);
    let (_tmp, state) = policy_transport_regression::transport_state(ServerConfig {
        data_auth_mode: DataAuthMode::Optional,
        data_auth_audience: Some("fluree-policy-test".into()),
        data_auth_policy_authorities: vec![did_from_pubkey(&key.verifying_key().to_bytes())],
        ..Default::default()
    })
    .await;
    let ledger = "authority-alone:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let token = create_jws(&controller_claims(ledger), &key);
    for (opts, expected) in [
        (serde_json::json!({}), 0),
        (serde_json::json!({"identity": null}), 0),
        (serde_json::json!({"default-allow": true}), 3),
    ] {
        let (status, result) = post_policy_request(
            &app,
            &format!("/v1/fluree/query/{ledger}"),
            Some(&token),
            "application/json",
            serde_json::json!({"opts": opts,
                "select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}})
            .to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result.as_array().unwrap().len(), expected, "{result}");
    }
    // A headers-only write must resolve the same missing selection to deny.
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/insert/{ledger}"),
        Some(&token),
        "text/turtle",
        "<http://example.org/new> <http://schema.org/name> \"New\" .".into(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{result}");
    // Omitting auth still supports the private-server direct-option contract.
    let (status, result) = query_docs_tri_state(app, ledger, None, None, None).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(names_from_results(&result).len(), 3);
}

#[tokio::test]
async fn missing_controller_selection_with_locked_defaults_denies() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "empty-controller:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    add_modify_policies(&app, ledger).await;
    let config = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix ex: <http://example.org/> .
        GRAPH <urn:fluree:{ledger}#config> {{
            <urn:cfg:main> a f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> .
            <urn:cfg:policy> f:defaultAllow false ; f:policyClass ex:ManagerClass ;
                f:overrideControl f:OverrideNone .
        }}
    "
    );
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/upsert/{ledger}"),
        None,
        "application/trig",
        config,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let token = create_jws(
        &controller_claims(ledger),
        &SigningKey::from_bytes(&[200; 32]),
    );
    let query = serde_json::json!({
        "select": ["?value"],
        "where": {"@id": "http://example.org/doc3", "http://schema.org/name": "?value"}
    });
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/query/{ledger}"),
        Some(&token),
        "application/json",
        query.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(
        result,
        serde_json::json!([]),
        "missing request selection must deny"
    );

    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/insert/{ledger}"),
        Some(&token),
        "text/turtle",
        "<http://example.org/blocked> <http://schema.org/name> \"Blocked\" .".into(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{result}");

    // A fixed empty selection must not acquire the identity's or config's grants.
    let fixed = delegated_token(
        "http://example.org/manager-user",
        ledger,
        serde_json::json!({"policy-class": []}),
    );
    let (status, result) = post_policy_request(
        &app,
        &format!("/v1/fluree/query/{ledger}"),
        Some(&fixed),
        "application/json",
        query.to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result, serde_json::json!([]));
}

#[tokio::test]
async fn explain_uses_final_body_policy_selection_on_both_routes() {
    let (_tmp, state) = policy_test_state().await;
    let ledger = "explain-selection:main";
    let app = setup_policy_ledger(build_router(state), ledger).await;
    let token = create_jws(
        &controller_claims(ledger),
        &SigningKey::from_bytes(&[200; 32]),
    );
    for scoped_route in [true, false] {
        let uri = if scoped_route {
            format!("/v1/fluree/explain/{ledger}")
        } else {
            "/v1/fluree/explain".into()
        };
        for allow in [true, false] {
            let mut query = serde_json::json!({
                "opts": {"default-allow": allow},
                "select": ["?name"],
                "where": {"@id": "?s", "http://schema.org/name": "?name"}
            });
            if !scoped_route {
                query["from"] = ledger.into();
            }
            let (status, result) = post_policy_request(
                &app,
                &uri,
                Some(&token),
                "application/json",
                query.to_string(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{result}");
            assert_eq!(
                result["plan"]["reason"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("withheld by policy"),
                !allow,
                "{result}"
            );
        }
    }
}
