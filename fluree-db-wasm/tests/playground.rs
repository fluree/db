//! Browser smoke test for the playground binding.
//!
//! Runs inside a dedicated Web Worker under `wasm-bindgen-test` — the same
//! hosting context the npm package uses — so the whole chain
//! (instantiate → `Playground::new` → create → insert → snapshot → query →
//! format → bytes across the binding) executes in a real browser engine.
//!
//! Positive ran-markers, never "didn't crash": each test asserts on the
//! *content* of what came back (row counts, bound values, receipt `t`).
//!
//! Run: `wasm-pack test --headless --chrome fluree-db-wasm`
//! (needs a `chromedriver` matching the installed Chrome on PATH or in
//! `$CHROMEDRIVER`).

#![cfg(target_arch = "wasm32")]

use fluree_db_wasm::Playground;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn parse(bytes: Vec<u8>) -> serde_json::Value {
    serde_json::from_slice(&bytes).expect("engine emitted valid JSON")
}

fn parse_str(s: String) -> serde_json::Value {
    serde_json::from_str(&s).expect("binding emitted valid JSON")
}

fn err_code(err: JsValue) -> String {
    js_sys::Reflect::get(&err, &JsValue::from_str("code"))
        .ok()
        .and_then(|v| v.as_string())
        .unwrap_or_else(|| "<no code>".to_string())
}

/// Snapshot the ledger head and return the handle.
async fn snap(pg: &Playground, ledger: &str) -> u32 {
    let info = parse_str(pg.snapshot(ledger.to_string()).await.unwrap());
    u32::try_from(info["handle"].as_u64().expect("numeric handle")).unwrap()
}

const PEOPLE: &str = r#"{
  "@context": {"ex": "http://example.org/ns/"},
  "@graph": [
    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 34,
     "ex:knows": {"@id": "ex:bob"}},
    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": 29}
  ]
}"#;

#[wasm_bindgen_test]
async fn create_insert_sparql_select_roundtrip() {
    let pg = Playground::new(None);

    let info = parse_str(pg.create_ledger("smoke".into()).await.unwrap());
    assert_eq!(info["id"], "smoke:main", "ledger id is branch-normalized");
    assert_eq!(info["t"], 0, "fresh ledger starts at t=0");

    let receipt = parse_str(pg.insert("smoke".into(), PEOPLE.into()).await.unwrap());
    assert_eq!(receipt["t"], 1, "first commit lands at t=1");
    assert!(
        receipt["flakes"].as_u64().unwrap() >= 6,
        "two typed subjects with three properties each: {receipt}"
    );
    assert!(
        receipt["commit"].as_str().unwrap().starts_with('b'),
        "receipt carries a CIDv1 commit id: {receipt}"
    );

    let s = snap(&pg, "smoke").await;
    let rows = parse(
        pg.query_sparql(
            s,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?name ?age WHERE { ?p a ex:Person ; ex:name ?name ; ex:age ?age } \
             ORDER BY ?name"
                .into(),
        )
        .await
        .unwrap(),
    );
    let bindings = rows["results"]["bindings"]
        .as_array()
        .expect("SPARQL Results JSON envelope");
    assert_eq!(bindings.len(), 2, "both people bound: {rows}");
    assert_eq!(bindings[0]["name"]["value"], "Alice");
    assert_eq!(bindings[0]["age"]["value"], "34");
    assert_eq!(bindings[1]["name"]["value"], "Bob");
    assert_eq!(rows["head"]["vars"], serde_json::json!(["name", "age"]));
    assert!(pg.release(s), "handle was live until released");
}

#[wasm_bindgen_test]
async fn jsonld_query_update_and_snapshot_isolation() {
    let pg = Playground::new(None);
    pg.create_ledger("jl".into()).await.unwrap();
    pg.insert("jl".into(), PEOPLE.into()).await.unwrap();

    let q = r#"{
      "@context": {"ex": "http://example.org/ns/"},
      "select": {"?p": ["*"]},
      "where": {"@id": "?p", "ex:knows": {"@id": "ex:bob"}}
    }"#;
    let before = snap(&pg, "jl").await;
    let out = parse(pg.query_jsonld(before, q.into()).await.unwrap());
    let rows = out.as_array().expect("JSON-LD select returns an array");
    assert_eq!(rows.len(), 1, "only alice knows bob: {out}");
    assert_eq!(rows[0]["ex:name"], "Alice");

    let update = r#"{
      "@context": {"ex": "http://example.org/ns/"},
      "where": {"@id": "ex:bob", "ex:age": "?old"},
      "delete": {"@id": "ex:bob", "ex:age": "?old"},
      "insert": {"@id": "ex:bob", "ex:age": 30}
    }"#;
    let receipt = parse_str(pg.update("jl".into(), update.into()).await.unwrap());
    assert_eq!(receipt["t"], 2);

    let ask_age = |s: u32| {
        pg.query_sparql(
            s,
            "PREFIX ex: <http://example.org/ns/> ASK { ex:bob ex:age 30 }".into(),
        )
    };

    // F6: the pre-update snapshot is frozen — it must NOT see the new value…
    let stale = parse(ask_age(before).await.unwrap());
    assert_eq!(stale["boolean"], false, "frozen snapshot moved: {stale}");
    // …while a fresh snapshot does.
    let after = snap(&pg, "jl").await;
    let fresh = parse(ask_age(after).await.unwrap());
    assert_eq!(fresh["boolean"], true, "new head not visible: {fresh}");

    let info = parse_str(pg.ledger_info("jl".into()).await.unwrap());
    assert_eq!(info["t"], 2);
}

#[wasm_bindgen_test]
async fn errors_carry_codes() {
    let pg = Playground::new(None);
    pg.create_ledger("errs".into()).await.unwrap();

    let dup = pg.create_ledger("errs".into()).await.unwrap_err();
    assert_eq!(err_code(dup), "conflict");

    let missing = pg.snapshot("nope".into()).await.unwrap_err();
    assert_eq!(err_code(missing), "not_found");

    let s = snap(&pg, "errs").await;
    let bad = pg
        .query_sparql(s, "SELECT WHERE {".into())
        .await
        .unwrap_err();
    assert_eq!(err_code(bad), "invalid_input");

    let not_json = pg
        .insert("errs".into(), "{not json".into())
        .await
        .unwrap_err();
    assert_eq!(err_code(not_json), "invalid_input");

    // A released handle is a typed not_found, and double-release is a no-op.
    assert!(pg.release(s));
    assert!(!pg.release(s));
    let gone = pg
        .query_sparql(s, "ASK { ?s ?p ?o }".into())
        .await
        .unwrap_err();
    assert_eq!(err_code(gone), "not_found");
}
