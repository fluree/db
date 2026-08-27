//! Live-query bridge (A4): `fluree-db-browser`'s [`LiveQuerySet`] behind the
//! worker protocol's subscribe/unsubscribe verbs and the batched
//! `cycleOutcome` event.
//!
//! One [`LiveQuerySet`] per engine, both modes: the peer drives `advance`
//! from SSE head changes; the playground drives it after each local commit,
//! so live subscriptions work in the no-server demo too. Delivery contract
//! (H §2): exactly ONE event per advance-cycle — `{ledger, t, changed,
//! unchanged, errored}` — with changed payloads as UTF-8 JSON bytes in the
//! subscription's language-matched format, handed to JS as `Uint8Array`s so
//! the worker can post their buffers in the transfer list. Unchanged
//! subscriptions ship zero payload (the driver's xxh3 gate); per-sub errors
//! repeat per cycle and never block other subscriptions.
//!
//! Subscribing auto-primes (detached): the first outcome for a new
//! subscription arrives as an ordinary cycle event at the current head —
//! always reported `changed` (no prior hash) — without waiting for a commit.
//!
//! Ledger ids are normalized at subscribe time (`"demo"` → `"demo:main"`) so
//! subscriptions match the normalized ids SSE head events and commit
//! receipts carry — the [`LiveQuerySet`] matches ledger strings exactly.

use fluree_db_browser::{LiveQuery, LiveQuerySet};
use wasm_bindgen::JsValue;

use crate::engine::parse_json;
use crate::error::{self, js_error};

pub(crate) struct LiveBridge {
    set: LiveQuerySet,
}

impl LiveBridge {
    pub(crate) fn new(set: LiveQuerySet) -> Self {
        Self { set }
    }

    /// Register a subscription and auto-prime it (detached). Returns the
    /// sub id as f64 (JS number; ids are sequential and stay far below
    /// 2^53). `kind` is the protocol's `QueryKind`.
    pub(crate) fn subscribe(&self, ledger: &str, kind: &str, text: &str) -> Result<f64, JsValue> {
        let query = match kind {
            "sparql" => LiveQuery::Sparql(text.to_string()),
            "jsonld" => LiveQuery::JsonLd(parse_json("query", text)?),
            other => {
                return Err(js_error(
                    error::code::INVALID_INPUT,
                    400,
                    &format!("unknown query kind \"{other}\""),
                ))
            }
        };
        let ledger = normalize(ledger);
        let sub_id = self.set.subscribe(ledger, query);
        #[cfg(target_arch = "wasm32")]
        {
            let set = self.set.clone();
            wasm_bindgen_futures::spawn_local(async move {
                // Emits through the outcome callbacks; the returned copy is
                // redundant here.
                let _ = set.prime(sub_id).await;
            });
        }
        #[allow(clippy::cast_precision_loss)]
        Ok(sub_id as f64)
    }

    /// Idempotent; a sub removed mid-cycle is dropped from that cycle too.
    pub(crate) fn unsubscribe(&self, sub_id: f64) -> bool {
        if !(sub_id.is_finite() && sub_id >= 0.0) {
            return false;
        }
        self.set.unsubscribe(sub_id as u64)
    }

    /// Bridge cycle outcomes to a JS callback `(metaJson: string,
    /// payloads: Uint8Array[])` — payloads aligned with `changed`'s order.
    /// The engine-side callback must be `Send + Sync`, so it forwards owned
    /// data through a channel; this drain task (worker event loop) is what
    /// touches JS.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn on_outcome_js(&self, callback: js_sys::Function) {
        use futures::StreamExt;
        let (tx, mut rx) = futures::channel::mpsc::unbounded::<(String, Vec<Vec<u8>>)>();
        self.set.on_outcome(move |outcome| {
            let meta = serde_json::json!({
                "ledger": outcome.ledger_id,
                "t": outcome.t,
                "changed": outcome
                    .changed
                    .iter()
                    .map(|c| serde_json::json!({ "subId": c.sub_id }))
                    .collect::<Vec<_>>(),
                "unchanged": outcome.unchanged,
                "errored": outcome
                    .errored
                    .iter()
                    .map(|(id, e)| serde_json::json!({ "subId": id, "error": e }))
                    .collect::<Vec<_>>(),
            })
            .to_string();
            let payloads = outcome.changed.iter().map(|c| c.payload.clone()).collect();
            let _ = tx.unbounded_send((meta, payloads));
        });
        wasm_bindgen_futures::spawn_local(async move {
            while let Some((meta, payloads)) = rx.next().await {
                let arr = js_sys::Array::new();
                for payload in &payloads {
                    arr.push(&js_sys::Uint8Array::from(payload.as_slice()));
                }
                let _ = callback.call2(&JsValue::NULL, &JsValue::from_str(&meta), &arr);
            }
        });
    }

    /// Run one advance-cycle for `ledger` on the worker event loop.
    /// Coalescing lives in the driver: concurrent calls fold into one
    /// follow-up cycle at the latest head.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn advance_detached(&self, ledger: &str) {
        let set = self.set.clone();
        let ledger = normalize(ledger);
        wasm_bindgen_futures::spawn_local(async move {
            set.advance(&ledger).await;
        });
    }
}

fn normalize(ledger: &str) -> String {
    fluree_db_core::ledger_id::normalize_ledger_id(ledger).unwrap_or_else(|_| ledger.to_string())
}
