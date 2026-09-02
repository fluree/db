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

fn err_status(err: &JsValue) -> f64 {
    js_sys::Reflect::get(err, &JsValue::from_str("status"))
        .ok()
        .and_then(|v| v.as_f64())
        .unwrap_or(-1.0)
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
            None,
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
    // head.vars order is the engine's var-registry order, not SELECT-clause
    // order (same on the HTTP surface; the W3C JSON results format does not
    // mandate one) — assert the set, not the sequence.
    let mut vars: Vec<&str> = rows["head"]["vars"]
        .as_array()
        .expect("head.vars array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    vars.sort_unstable();
    assert_eq!(vars, ["age", "name"]);
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
    let out = parse(pg.query_jsonld(before, q.into(), None).await.unwrap());
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
            None,
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
    assert_eq!(err_status(&dup), 409.0, "conflict carries its HTTP status");
    assert_eq!(err_code(dup), "conflict");

    let missing = pg.snapshot("nope".into()).await.unwrap_err();
    assert_eq!(
        err_status(&missing),
        404.0,
        "not_found carries its HTTP status"
    );
    assert_eq!(err_code(missing), "not_found");

    let s = snap(&pg, "errs").await;
    let bad = pg
        .query_sparql(s, "SELECT WHERE {".into(), None)
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
        .query_sparql(s, "ASK { ?s ?p ?o }".into(), None)
        .await
        .unwrap_err();
    assert_eq!(err_code(gone), "not_found");
}

/// The F4 memory budget actually fires: a small budget + a query whose
/// retained working set (full cross join under ORDER BY) exceeds it must be
/// the typed `out_of_memory` (507), not a trap.
#[wasm_bindgen_test]
async fn memory_budget_rejects_oversized_query_typed() {
    // 64 KiB budget against grouped aggregation, the lane the engine
    // provably charges (GROUP_EST_BYTES = 128 per group): 64x64 cross-pairs
    // grouped on both vars = 4,096 groups = 512 KiB charged, 8x the budget,
    // from only 4,096 joined rows (fast even in a debug wasm build). Plain
    // cross joins and GROUP_CONCAT payload bytes are NOT charged today — at
    // any size that finishes inside the harness timeout they complete
    // without tripping — so this pins the typed-rejection contract, not any
    // particular operator's accounting completeness.
    let pg = Playground::new(Some(65536.0));
    pg.create_ledger("mem".into()).await.unwrap();

    let mut graph = Vec::new();
    for i in 0..64 {
        graph.push(serde_json::json!({
            "@id": format!("ex:n{i}"), "@type": "ex:Thing",
            "ex:label": format!("thing {i}"),
        }));
    }
    let doc = serde_json::json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": graph});
    pg.insert("mem".into(), doc.to_string()).await.unwrap();

    let s = snap(&pg, "mem").await;
    let res = pg
        .query_sparql(
            s,
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?a ?b (COUNT(?b) AS ?n) \
             WHERE { ?a a ex:Thing . ?b a ex:Thing } \
             GROUP BY ?a ?b"
                .into(),
            None,
        )
        .await;
    // A short panic on the wrong arm: dumping an Ok payload into the panic
    // message once blew chromedriver's 10 MB response cap.
    let over = match res {
        Err(e) => e,
        Ok(ok) => panic!("expected budget rejection, got Ok ({} bytes)", ok.len()),
    };
    assert_eq!(err_status(&over), 507.0, "budget failure carries 507");
    assert_eq!(err_code(over), "out_of_memory");

    // The engine survived (typed error, not a trap): a small query still runs.
    let ok = parse(
        pg.query_sparql(s, "ASK { ?s ?p ?o }".into(), None)
            .await
            .expect("engine alive after a budgeted rejection"),
    );
    assert_eq!(ok["boolean"], true);
}

/// The transact pre-gate: with a budget set, an input body over ¼ of it is
/// refused typed instead of gambling on an allocator trap.
#[wasm_bindgen_test]
async fn transact_pregate_rejects_oversized_body() {
    let pg = Playground::new(Some(65536.0));
    pg.create_ledger("gate".into()).await.unwrap();

    let big = format!(
        r#"{{"@context": {{"ex": "http://example.org/ns/"}}, "@id": "ex:big", "ex:blob": "{}"}}"#,
        "x".repeat(20000)
    );
    let refused = pg.insert("gate".into(), big).await.unwrap_err();
    assert_eq!(err_code(refused), "out_of_memory");

    // Small bodies pass the gate and commit.
    let receipt = parse_str(
        pg.insert(
            "gate".into(),
            r#"{"@context": {"ex": "http://example.org/ns/"}, "@id": "ex:ok", "ex:n": 1}"#.into(),
        )
        .await
        .unwrap(),
    );
    assert_eq!(receipt["t"], 1);
}

/// upsert and sparqlUpdate exercised at the binding level.
#[wasm_bindgen_test]
async fn upsert_and_sparql_update() {
    let pg = Playground::new(None);
    pg.create_ledger("mut".into()).await.unwrap();
    pg.insert("mut".into(), PEOPLE.into()).await.unwrap();

    let up = r#"{
      "@context": {"ex": "http://example.org/ns/"},
      "@id": "ex:bob", "ex:name": "Robert"
    }"#;
    let receipt = parse_str(pg.upsert("mut".into(), up.into()).await.unwrap());
    assert_eq!(receipt["t"], 2);

    let s1 = snap(&pg, "mut").await;
    let renamed = parse(
        pg.query_sparql(
            s1,
            "PREFIX ex: <http://example.org/ns/> ASK { ex:bob ex:name \"Robert\" }".into(),
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        renamed["boolean"], true,
        "upsert replaced the name: {renamed}"
    );

    let receipt = parse_str(
        pg.sparql_update(
            "mut".into(),
            "PREFIX ex: <http://example.org/ns/> INSERT DATA { ex:carol a ex:Person }".into(),
        )
        .await
        .unwrap(),
    );
    assert_eq!(receipt["t"], 3);

    let s2 = snap(&pg, "mut").await;
    let carol = parse(
        pg.query_sparql(
            s2,
            "PREFIX ex: <http://example.org/ns/> ASK { ex:carol a ex:Person }".into(),
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(carol["boolean"], true, "SPARQL update visible: {carol}");
}

/// Live subscriptions over the playground (A4): subscribe auto-primes (the
/// first outcome arrives with no commit), a relevant commit produces a
/// changed payload with the new rows, an unrelated ledger's commit produces
/// no cycle for this ledger, and unsubscribe is idempotent.
#[wasm_bindgen_test]
async fn live_subscription_primes_and_tracks_commits() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::JsCast;

    type Outcomes = Rc<RefCell<Vec<(serde_json::Value, Vec<String>)>>>;

    async fn yield_turn() {
        let _ =
            wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL)).await;
    }
    async fn wait_for(outcomes: &Outcomes, ledger: &str, n: usize) {
        for _ in 0..20_000 {
            let count = outcomes
                .borrow()
                .iter()
                .filter(|(meta, _)| meta["ledger"] == ledger)
                .count();
            if count >= n {
                return;
            }
            yield_turn().await;
        }
        panic!(
            "timed out waiting for {n} outcome(s) of {ledger}; saw {:?}",
            outcomes
                .borrow()
                .iter()
                .map(|(m, _)| m.clone())
                .collect::<Vec<_>>()
        );
    }
    fn nth(
        outcomes: &[(serde_json::Value, Vec<String>)],
        ledger: &str,
        n: usize,
    ) -> (serde_json::Value, Vec<String>) {
        outcomes
            .iter()
            .filter(|(meta, _)| meta["ledger"] == ledger)
            .nth(n)
            .expect("outcome present")
            .clone()
    }

    let pg = Playground::new(None);
    pg.create_ledger("live".into()).await.unwrap();

    let outcomes: Outcomes = Rc::new(RefCell::new(Vec::new()));
    {
        let outcomes = Rc::clone(&outcomes);
        let cb = Closure::wrap(Box::new(move |meta: String, payloads: js_sys::Array| {
            let meta: serde_json::Value =
                serde_json::from_str(&meta).expect("outcome meta is JSON");
            let decoded = payloads
                .iter()
                .map(|p| {
                    let bytes: js_sys::Uint8Array = p.dyn_into().expect("payload is Uint8Array");
                    String::from_utf8(bytes.to_vec()).expect("payload is UTF-8")
                })
                .collect();
            outcomes.borrow_mut().push((meta, decoded));
        }) as Box<dyn FnMut(String, js_sys::Array)>);
        pg.on_cycle_outcome(cb.as_ref().unchecked_ref::<js_sys::Function>().clone());
        cb.forget();
    }

    let sub = pg
        .subscribe_live(
            "live".into(),
            "sparql".into(),
            "PREFIX ex: <http://example.org/ns/> \
             SELECT ?name WHERE { ?s ex:name ?name } ORDER BY ?name"
                .into(),
        )
        .unwrap();

    // Auto-prime: one outcome with the (empty) current result, reported changed.
    wait_for(&outcomes, "live:main", 1).await;
    {
        let (meta, payloads) = nth(&outcomes.borrow(), "live:main", 0);
        assert_eq!(
            meta["changed"][0]["subId"].as_f64(),
            Some(sub),
            "prime reports the new sub as changed: {meta}"
        );
        let rows: serde_json::Value = serde_json::from_str(&payloads[0]).unwrap();
        assert_eq!(
            rows["results"]["bindings"].as_array().unwrap().len(),
            0,
            "primed against the empty ledger"
        );
    }

    // A relevant commit produces the new rows.
    pg.insert("live".into(), PEOPLE.into()).await.unwrap();
    wait_for(&outcomes, "live:main", 2).await;
    {
        let (meta, payloads) = nth(&outcomes.borrow(), "live:main", 1);
        assert_eq!(
            meta["t"].as_i64(),
            Some(1),
            "cycle at the committed head: {meta}"
        );
        let rows: serde_json::Value = serde_json::from_str(&payloads[0]).unwrap();
        let names: Vec<&str> = rows["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["name"]["value"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["Alice", "Bob"]);
    }

    // An unrelated ledger's commit never cycles this ledger.
    pg.create_ledger("other".into()).await.unwrap();
    pg.insert("other".into(), PEOPLE.into()).await.unwrap();
    wait_for(&outcomes, "other:main", 1).await;
    assert_eq!(
        outcomes
            .borrow()
            .iter()
            .filter(|(meta, _)| meta["ledger"] == "live:main")
            .count(),
        2,
        "no extra cycle for the subscribed ledger"
    );

    assert!(pg.unsubscribe_live(sub));
    assert!(!pg.unsubscribe_live(sub), "unsubscribe is idempotent");
}
