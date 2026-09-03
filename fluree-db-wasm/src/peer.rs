//! Peer mode: a read-only engine over a remote Fluree server's ledgers.
//!
//! [`connect_peer`] assembles `fluree-db-browser`'s [`BrowserPeer`] — CID-
//! verified block fetches over the storage proxy, IndexedDB persistence, the
//! in-memory residency tier — and exposes the same snapshot/query surface as
//! the playground ([`crate::Playground`]), so `js/src/worker.ts` drives both
//! modes through one protocol. Differences from playground:
//!
//! - **Read-only.** No transact exports exist here; the JS worker answers
//!   transact ops in peer mode with a typed `unsupported` before Rust is
//!   reached. Commits are ordered by the origin server's write authority.
//! - **One memory ceiling.** `max_memory_bytes` feeds
//!   [`BrowserIoConfig::from_max_memory`] — residency tier, write-behind
//!   gauge, fetch width all derive from it — and the per-query budget is set
//!   to a quarter of the ceiling, inside the ~35% the governor's split
//!   reserves for engine/operator memory.
//! - **No raw token in `init`** (recycle replays init): the token arrives
//!   from the main thread over the event channel just before this
//!   constructor runs. Mid-session refresh is [`Peer::set_token`] →
//!   `BrowserPeer::set_token` (the shared `TokenCell`): every request or
//!   connect issued afterward stamps the new bearer, with no teardown and no
//!   loss of warm state. A 401 that lands before the app refreshed still
//!   surfaces typed, and the shell reconnects with a fresh `getToken` pull
//!   (worker recycle or explicit close+connect).
//! - **Head changes** fan out through [`Peer::on_head_change`]: the
//!   `Send + Sync` engine callback forwards into a channel, and a
//!   `spawn_local` drain task calls the registered JS function — JS handles
//!   never cross into the engine's callback registry.
//!
//! Whole module is wasm-only: `fluree_db_browser::connect` (the real driver)
//! exists only there, and the wasm32 CI job gates this crate `--all-targets`.

use std::cell::RefCell;

use fluree_db_browser::{BrowserIoConfig, BrowserPeer, HeadTracker, LiveQuerySet};
use futures::StreamExt;
use serde_json::json;
use wasm_bindgen::prelude::*;

use crate::engine::{make_exec_options, sanitize_bytes, EngineCore};
use crate::live::LiveBridge;

/// A connected read-only peer engine.
#[wasm_bindgen]
pub struct Peer {
    core: EngineCore,
    peer: BrowserPeer,
    tracker: RefCell<Option<HeadTracker>>,
    /// Live subscriptions (A4), advanced from SSE head changes.
    live: LiveBridge,
}

/// Connect to a remote Fluree server's storage proxy.
///
/// `api_base` is the versioned API base (`https://host/v1/fluree`); `token`
/// is a bearer token with `fluree.storage.*` scope (a full-read grant for
/// its ledgers). Cheap and synchronous: the driver starts, but no network
/// happens until the first ledger open.
#[wasm_bindgen(js_name = connectPeer)]
pub fn connect_peer(api_base: String, token: String, max_memory_bytes: Option<f64>) -> Peer {
    let max = sanitize_bytes(max_memory_bytes);
    let config = max
        .map(BrowserIoConfig::from_max_memory)
        .unwrap_or_default();
    let peer = fluree_db_browser::connect(api_base, token, config);
    let query_budget = max.map(|m| m / 4);
    let core = EngineCore::new(peer.fluree().clone(), query_budget);

    // Live-query driver: one cycle-level guard per cycle via the CAS layer,
    // the same per-query budget as the ad-hoc verbs.
    let live_set = LiveQuerySet::with_execution_options(
        peer.fluree().clone(),
        Some(peer.cas().clone()),
        make_exec_options(query_budget),
    );
    // SSE head change -> one advance per event. The engine callback must be
    // Send + Sync, so it only forwards the ledger id; the drain task spawns
    // each advance rather than awaiting it, so the driver's coalescer sees the
    // concurrency it was built for: bursts on one ledger fold into a single
    // follow-up cycle at the latest head (`begin` returns `None` for the
    // second and later, folding them), and different ledgers advance in
    // parallel rather than a slow cycle on one blocking the next. Awaiting
    // each advance to completion here — as this once did — serialized
    // everything and defeated the coalescer: a five-event burst ran five full
    // cycles (five re-queries, five IndexedDB round trips) instead of one.
    //
    // Two things the drain does before advancing:
    //
    //  - NORMALIZE. `LiveQuerySet` matches ledger strings exactly, and the
    //    subscribe path already normalizes; an un-normalized id here would
    //    match nothing and the subscription would silently stop updating.
    //    `normalize_ledger_id` is idempotent, so this is free insurance.
    //  - GATE on `has_ledger`. Head tracking defaults to every ledger the
    //    token can see, while an app typically subscribes to one. An
    //    advance for an unsubscribed ledger is not a no-op: the empty-cycle
    //    branch still opens the ledger to report the head it observed —
    //    on a peer that is a nameservice resolution, a root index-block
    //    fetch, CID verification and an IndexedDB write, for every commit
    //    anywhere on the server. The `headChange` event still fires for
    //    those ledgers; only the pointless cycle is skipped.
    let (advance_tx, mut advance_rx) = futures::channel::mpsc::unbounded::<String>();
    peer.on_head_change(move |change| {
        let _ = advance_tx.unbounded_send(change.ledger_id.clone());
    });
    {
        let set = live_set.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(ledger) = advance_rx.next().await {
                let ledger = crate::live::normalize(&ledger);
                if set.has_ledger(&ledger) {
                    // Spawn, don't await: overlapping advances are what let the
                    // coalescer fold a same-ledger burst and run different
                    // ledgers in parallel (see the note above).
                    let set = set.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        set.advance(&ledger).await;
                    });
                }
            }
        });
    }

    Peer {
        core,
        peer,
        tracker: RefCell::new(None),
        live: LiveBridge::new(live_set),
    }
}

#[wasm_bindgen]
impl Peer {
    /// Watermarks of a remote ledger as `{"id","t","indexT"}` (resolves the
    /// head through the remote nameservice and opens the ledger locally).
    /// Rejects with `not_found` for unknown/unauthorized ledgers (the server
    /// answers both identically — no existence oracle).
    #[wasm_bindgen(js_name = ledgerInfo)]
    pub async fn ledger_info(&self, ledger_id: String) -> Result<String, JsValue> {
        self.core.ledger_info(&ledger_id).await
    }

    /// Freeze the ledger's current head as a queryable snapshot —
    /// `{"handle","id","t"}`, same contract as the playground's.
    pub async fn snapshot(&self, ledger_id: String) -> Result<String, JsValue> {
        self.core.snapshot(&ledger_id).await
    }

    /// Drop a snapshot. Never errors; releasing twice is a no-op.
    pub fn release(&self, snapshot: u32) -> bool {
        self.core.release(snapshot)
    }

    /// SPARQL query against a snapshot handle → UTF-8 JSON bytes.
    #[wasm_bindgen(js_name = querySparql)]
    pub async fn query_sparql(
        &self,
        snapshot: u32,
        sparql: String,
        timeout_ms: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        self.core.query_sparql(snapshot, &sparql, timeout_ms).await
    }

    /// JSON-LD query against a snapshot handle → UTF-8 JSON bytes.
    #[wasm_bindgen(js_name = queryJsonld)]
    pub async fn query_jsonld(
        &self,
        snapshot: u32,
        query: String,
        timeout_ms: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        self.core.query_jsonld(snapshot, &query, timeout_ms).await
    }

    /// Replace the bearer token for every I/O surface — mid-session refresh
    /// for long-lived tabs whose connect-time token would otherwise expire
    /// (401s on block fetches, a fatal SSE reconnect). Delegates to
    /// `BrowserPeer::set_token`: requests already in flight keep the header
    /// they were stamped with; everything issued afterward carries the new
    /// bearer (fetch transports stamp per request, the SSE source resolves
    /// per connect). Refreshing proactively needs no restart.
    #[wasm_bindgen(js_name = setToken)]
    pub fn set_token(&self, token: String) {
        self.peer.set_token(token);
    }

    /// Register a JS callback for ledger head changes. Called with one JSON
    /// string argument: `{"ledger","t","indexT"}`. The engine-side callback
    /// must be `Send + Sync`, so it forwards through a channel and this
    /// drain task (on the worker's event loop) is what touches JS.
    #[wasm_bindgen(js_name = onHeadChange)]
    pub fn on_head_change(&self, callback: js_sys::Function) {
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        self.peer.on_head_change(move |change| {
            let _ = tx.unbounded_send(change.clone());
        });
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(change) = rx.next().await {
                let payload = json!({
                    "ledger": change.ledger_id,
                    "t": change.commit_t,
                    "indexT": change.index_t,
                })
                .to_string();
                let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(&payload));
            }
        });
    }

    /// Start SSE head tracking for `ledgers` (empty = everything the token
    /// may see). Replaces any previous tracking subscription. Head changes
    /// refresh the cached ledger (the next `snapshot()` sees the new head;
    /// frozen snapshots never move) and fan out to
    /// [`Self::on_head_change`] callbacks.
    #[wasm_bindgen(js_name = startHeadTracking)]
    pub fn start_head_tracking(&self, ledgers: Vec<String>) {
        let tracker = self.peer.start_head_tracking(&ledgers);
        if let Some(old) = self.tracker.borrow_mut().replace(tracker) {
            old.stop();
        }
    }

    /// Register a live subscription (auto-primed: its first result arrives
    /// as a `cycleOutcome` event at the current head). Returns the sub id.
    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe_live(
        &self,
        ledger: String,
        kind: String,
        text: String,
    ) -> Result<f64, JsValue> {
        self.live.subscribe(&ledger, &kind, &text)
    }

    /// Remove a live subscription. Idempotent.
    #[wasm_bindgen(js_name = unsubscribe)]
    pub fn unsubscribe_live(&self, sub_id: f64) -> bool {
        self.live.unsubscribe(sub_id)
    }

    /// Register the cycle-outcome fan-out callback
    /// `(metaJson: string, payloads: Uint8Array[])`.
    #[wasm_bindgen(js_name = onCycleOutcome)]
    pub fn on_cycle_outcome(&self, callback: js_sys::Function) {
        self.live.on_outcome_js(callback);
    }

    /// Stop head tracking and the I/O driver. In-flight jobs complete; new
    /// I/O fails typed. The engine remains usable for already-resident data.
    pub fn shutdown(&self) {
        if let Some(tracker) = self.tracker.borrow_mut().take() {
            tracker.stop();
        }
        self.peer.shutdown();
    }
}
