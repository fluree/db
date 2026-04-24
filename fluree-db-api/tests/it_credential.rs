//! Credentialed query/transact integration tests
//!
//! Focus:
//! - `credential_transact` (signed update txn with policy enforcement)
//! - `credential_query_connection` (signed query with identity injected)
//! - `credential_query_sparql` (signed SPARQL query)

mod support;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_api::{credential, FlureeBuilder};
use serde_json::{json, Value as JsonValue};
use support::{assert_index_defaults, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

fn decode_hex_32(s: &str) -> [u8; 32] {
    assert_eq!(s.len(), 64, "expected 32-byte hex");
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex");
    }
    out
}

fn create_jws(payload: &str, signing_key: &SigningKey) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": { "kty":"OKP", "crv":"Ed25519", "x": pubkey_b64 }
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let sig = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(sig.to_bytes());
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

fn ctx_ct(ns_prefix: &str) -> JsonValue {
    json!({
        "f": "https://ns.flur.ee/db#",
        "ct": format!("ledger:{}/", ns_prefix)
    })
}

async fn seed_credential_ledger(
    fluree: &MemoryFluree,
    ledger_id: &str,
    ns_prefix: &str,
    did_root: &str,
    did_pleb: &str,
) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Seed an open record.
    let seeded = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx_ct(ns_prefix),
                "@id": "ct:open",
                "ct:foo": "bar"
            }),
        )
        .await
        .expect("seed open")
        .ledger;

    // Insert identities and a stored policy class. We intentionally store f:query as a JSON string
    // so it can be used by identity-based policy loading (for SPARQL credential tests).
    //
    // Policy rule: allow view/modify only when the accessed subject is the identity.
    //
    // We model this as a triple pattern over a self-link:
    //   ?$this ct:sameAs ?$identity
    // and we seed each user with ct:sameAs pointing to itself.
    //
    // This matches the proven policy-query shape used in `it_policy_tracking.rs`.
    let policy_query_str = serde_json::to_string(&json!({
        "@context": ctx_ct(ns_prefix),
        "where": [{"@id":"?$this","ct:sameAs":"?$identity"}]
    }))
    .expect("policy query json string");

    let tx = json!({
        "@context": ctx_ct(ns_prefix),
        "insert": [
            {
                "@id": did_root,
                "@type": "ct:User",
                "ct:name": "Daniel",
                "ct:favnums": [1, 2, 3],
                "ct:sameAs": {"@id": did_root},
                "f:policyClass": {"@id":"ct:DefaultUserPolicy"}
            },
            {
                "@id": did_pleb,
                "@type": "ct:User",
                "ct:name": "Plebian",
                "ct:sameAs": {"@id": did_pleb},
                "f:policyClass": {"@id":"ct:DefaultUserPolicy"}
            },
            {
                "@id": "ct:userPolicy",
                "@type": ["f:AccessPolicy", "ct:DefaultUserPolicy"],
                "f:required": true,
                "f:action": [{"@id":"f:view"}, {"@id":"f:modify"}],
                "f:exMessage": "Users can only manage their own data.",
                "f:query": policy_query_str
            }
        ]
    });

    fluree
        .update(seeded, &tx)
        .await
        .expect("seed identities + policy")
        .ledger
}

/// Test: Credentialed transact and query with f:query policy enforcement
///
/// credential-test (integration section)
#[tokio::test]
async fn credential_transact_then_credential_query_enforces_policy() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Use fixed private keys to keep DIDs stable.
    let root_sk = SigningKey::from_bytes(&decode_hex_32(
        "27ee972212ecf6f1810b11ece94bb85487b4694580bcc189f731d54f0a242429",
    ));
    let pleb_sk = SigningKey::from_bytes(&decode_hex_32(
        "fb9fb212adbd3f803081a207e84998058a34add87deefe220c254d6cadb77322",
    ));

    let did_root = fluree_db_credential::did_from_pubkey(&root_sk.verifying_key().to_bytes());
    let did_pleb = fluree_db_credential::did_from_pubkey(&pleb_sk.verifying_key().to_bytes());

    let ns_prefix = "credentialtest";
    let ledger_id = "it/credentialtest:main";

    let ledger1 = seed_credential_ledger(&fluree, ledger_id, ns_prefix, &did_root, &did_pleb).await;

    // Sanity: uncredentialed query should see the open record.
    let open_q = json!({
        "@context": ctx_ct(ns_prefix),
        "select": {"ct:open": ["*"]}
    });
    let open = support::query_jsonld(&fluree, &ledger1, &open_q)
        .await
        .expect("query open")
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(open, json!([{"@id":"ct:open","ct:foo":"bar"}]));

    // Signed credentialed transaction: update root user (remove name Daniel, remove favnums 1; add name D, add favnums 4/5/6).
    // Attach inline policy too (even though identity-based is stored) to make enforcement explicit for this test.
    let inline_policy = json!([{
        "@id": "ct:inlineSelfOnly",
        "f:required": true,
        "f:action": [{"@id":"f:modify"}],
        "f:exMessage": "Users can only manage their own data.",
        "f:query": serde_json::to_string(&json!({
            "@context": ctx_ct(ns_prefix),
            "where": [{"@id":"?$this","ct:sameAs":"?$identity"}]
        }))
        .unwrap()
    }]);

    let txn = json!({
        "@context": ctx_ct(ns_prefix),
        "where": {"@id": did_root, "ct:name": "Daniel"},
        "delete": {"@id": did_root, "ct:name": "Daniel", "ct:favnums": 1},
        "insert": {"@id": did_root, "ct:name": "D", "ct:favnums": [4, 5, 6]},
        "opts": { "policy": inline_policy }
    });
    let txn_jws = create_jws(&txn.to_string(), &root_sk);

    let ledger2 = fluree
        .credential_transact(ledger1, credential::Input::Jws(&txn_jws))
        .await
        .expect("credential_transact")
        .ledger;

    // Signed credentialed query selecting the root user (graph crawl).
    let query = json!({
        "@context": ctx_ct(ns_prefix),
        "from": ledger_id,
        "select": { did_root.clone(): ["*"] },
        "where": { "@id": did_root, "?p": "?o" },
        "opts": { "policy": [{
            "@id": "ct:inlineViewSelfOnly",
            "f:required": true,
            "f:action": [{"@id":"f:view"}],
            "f:query": serde_json::to_string(&json!({
                "@context": ctx_ct(ns_prefix),
                "where": [{"@id":"?$this","ct:sameAs":"?$identity"}]
            })).unwrap()
        }]}
    });

    // Root can see itself.
    let q_root_jws = create_jws(&query.to_string(), &root_sk);
    let root_res = fluree
        .credential_query_connection(credential::Input::Jws(&q_root_jws), None)
        .await
        .expect("credential_query_connection root");
    let root_json = root_res
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(
        normalize_rows(&root_json),
        normalize_rows(&json!([{
            "@id": did_root,
            "@type": "ct:User",
            "ct:name": "D",
            // We don't assert ordering; JSON-LD output may be set-like.
            "ct:favnums": [2, 3, 4, 5, 6],
            "ct:sameAs": {"@id": did_root},
            "f:policyClass": {"@id":"ct:DefaultUserPolicy"}
        }]))
    );

    // Pleb cannot see root.
    let q_pleb_jws = create_jws(&query.to_string(), &pleb_sk);
    let pleb_res = fluree
        .credential_query_connection(credential::Input::Jws(&q_pleb_jws), None)
        .await
        .expect("credential_query_connection pleb");
    let pleb_json = pleb_res
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(pleb_json, json!([]));
}

/// Test: Credentialed SPARQL query with identity-based f:query policy
///
/// credential-test (SPARQL with identity-based policy)
#[tokio::test]
async fn credential_query_sparql_uses_identity_based_policy() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let root_sk = SigningKey::from_bytes(&decode_hex_32(
        "27ee972212ecf6f1810b11ece94bb85487b4694580bcc189f731d54f0a242429",
    ));
    let pleb_sk = SigningKey::from_bytes(&decode_hex_32(
        "fb9fb212adbd3f803081a207e84998058a34add87deefe220c254d6cadb77322",
    ));

    let did_root = fluree_db_credential::did_from_pubkey(&root_sk.verifying_key().to_bytes());
    let did_pleb = fluree_db_credential::did_from_pubkey(&pleb_sk.verifying_key().to_bytes());

    let ns_prefix = "credentialtest-sparql";
    let ledger_id = "it/credentialtest-sparql:main";
    let ledger = seed_credential_ledger(&fluree, ledger_id, ns_prefix, &did_root, &did_pleb).await;

    // Make sure the value is "Daniel" before any credential_transact in this test.
    let sparql = format!(
        r"
PREFIX ct: <ledger:{ns_prefix}/>
SELECT ?name
FROM <{ledger_id}>
WHERE {{ <{did_root}> ct:name ?name }}
ORDER BY ?name
"
    );

    // Root can see itself.
    let jws_root = create_jws(&sparql, &root_sk);
    let result_root = fluree
        .credential_query_sparql(&jws_root, None)
        .await
        .expect("credential_query_sparql root");
    let jsonld_root = result_root.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld_root),
        normalize_rows(&json!([["Daniel"]]))
    );

    // Pleb cannot see root.
    let jws_pleb = create_jws(&sparql, &pleb_sk);
    let result_pleb = fluree
        .credential_query_sparql(&jws_pleb, None)
        .await
        .expect("credential_query_sparql pleb");
    let jsonld_pleb = result_pleb.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(jsonld_pleb, json!([]));
}
