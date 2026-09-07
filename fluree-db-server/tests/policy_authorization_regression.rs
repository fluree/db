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
            r#"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix ex: <http://example.org/> .
            GRAPH <urn:fluree:{ledger}#config> {{
                <urn:cfg:main> a f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true ; f:policyClass ex:ModelRules ;
                    f:overrideControl f:{control} ; f:policySource <urn:cfg:ref> .
                <urn:cfg:ref> a f:GraphRef ; f:graphSource <urn:cfg:source> .
                <urn:cfg:source> f:ledger <{model}> ;
                    f:graphSelector <http://example.org/model-policies> .
            }}"#
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
        let mut tokens = vec![delegated];
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
        for token in tokens {
            for (property, expected) in [
                ("http://schema.org/name", 1),
                ("http://example.org/content", usize::from(allowed)),
            ] {
                let query = serde_json::json!({
                    "opts": grant, "select": ["?value"],
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
            body["opts"] = grant.clone();
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
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(names_from_results(&result).len(), 2, "{result}");
    assert!(!names_from_results(&result).contains(&"Executive Salaries"));
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
                .header("fluree-policy-class", "http://example.org/ManagerClass")
                .header("fluree-default-allow", "true")
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
        assert_eq!(status, StatusCode::OK, "{result}");
        let names = names_from_results(&result);
        println!("restricted bearer opts={opts}: {names:?}");
        assert_eq!(names.len(), 2);
        assert!(!names.contains(&"Executive Salaries"));
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
    assert_eq!(status, StatusCode::OK, "{result}");
    let names = names_from_results(&result);
    println!("restricted bearer per-source identity override: {names:?}");
    assert_eq!(names.len(), 2);
    assert!(!names.contains(&"Executive Salaries"));
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
                    serde_json::json!(did_from_pubkey(&ordinary.verifying_key().to_bytes()))
            }
            "null-context" => invalid["fluree.policy"] = JsonValue::Null,
            "audience" => invalid["aud"] = serde_json::json!("another-server"),
            "expired" => invalid["exp"] = serde_json::json!(now_secs() - 120),
            "future" => invalid["nbf"] = serde_json::json!(now_secs() + 300),
            "ledger" => {
                invalid["fluree.ledger.read.ledgers"] = serde_json::json!(["elsewhere:main"])
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
    assert_eq!(status, StatusCode::OK);
    assert!(names_from_results(&result).is_empty());
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
    for (uri, content_type, body, stream) in [
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
        assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("Public Post") && text.contains("Internal Memo"), "{uri}: {text}");
        assert!(!text.contains("Executive Salaries"), "{uri}: {text}");
        if !stream {
            assert_eq!(sparql_names(&serde_json::from_str(text).unwrap()).len(), 2);
        }
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
        assert_eq!(status, StatusCode::BAD_REQUEST, "{content_type}: {error}");
        assert!(error
            .to_string()
            .contains("Employees may not modify document content."));
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
        assert_eq!(status, StatusCode::OK, "{uri}: {rows}");
        assert_eq!(names_from_results(&rows).len(), 2);
        let mut redirected = query.clone();
        redirected["opts"]["from"]["@id"] = serde_json::json!(other);
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
    assert_eq!(status, StatusCode::OK, "{result}");
    assert!(
        result
            .get("errors")
            .is_none_or(|e| e.as_object().is_some_and(|o| o.is_empty())),
        "{result}"
    );
    assert_eq!(
        names_from_results(&result["results"]["jsonld"]).len(),
        2,
        "{result}"
    );
    assert_eq!(
        sparql_names(&result["results"]["sparql"]).len(),
        2,
        "{result}"
    );
    assert!(!result.to_string().contains("Executive Salaries"));
}
