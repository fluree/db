//! IndexedDB block cache.
//!
//! Layout: one database with two object stores — `blocks` (key: CID
//! string → `Uint8Array` payload) and `meta` (key: CID string →
//! `{ s: size, a: lastAccess }`). Keeping metadata separate means the
//! open-time scan that rebuilds the [`CacheIndex`] never loads block
//! payloads.
//!
//! Semantics:
//! - **Verify-once-then-trust.** Only bytes that passed CID verification
//!   upstream are ever written, so a hit is returned as-is.
//! - **Write-behind.** A put runs in its own task and never delays the
//!   read that produced the bytes; failures are logged and the next put
//!   simply tries again.
//! - **Budgeted LRU.** The in-memory index plans evictions before each
//!   write; victims are deleted in the same transaction as the insert.
//! - **Batched access times.** Reads touch the index in memory and mark
//!   the key dirty; a periodic flush writes the timestamps to `meta`.
//!
//! The index is rebuilt from `meta` at open, so it reflects this session's
//! view; blocks written by another tab in the meantime are still found by
//! `get` (the store is consulted on every miss) and adopted into the index.

use crate::budget::CacheIndex;
use crate::config::CacheConfig;
use crate::driver::fetch::js_error_text;
use bytes::Bytes;
use fluree_db_core::ContentId;
use gloo_timers::future::TimeoutFuture;
use js_sys::{Array, Date, Object, Reflect, Uint8Array};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;
use web_sys::{
    Event, IdbCursorWithValue, IdbDatabase, IdbFactory, IdbObjectStore, IdbOpenDbRequest,
    IdbRequest, IdbTransaction, IdbTransactionMode, IdbVersionChangeEvent,
};

const BLOCKS: &str = "blocks";
const META: &str = "meta";
const DB_VERSION: u32 = 1;

/// The persistent block cache. Single-threaded (`Rc`); lives inside the
/// driver.
pub struct IdbCache {
    db: IdbDatabase,
    index: RefCell<CacheIndex>,
    dirty: RefCell<HashMap<String, f64>>,
    config: CacheConfig,
}

impl std::fmt::Debug for IdbCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let index = self.index.borrow();
        f.debug_struct("IdbCache")
            .field("db", &self.config.db_name)
            .field("entries", &index.len())
            .field("bytes", &index.total_bytes())
            .finish()
    }
}

fn js_text(value: JsValue) -> String {
    js_error_text(&value)
}

fn idb_factory() -> Result<IdbFactory, JsValue> {
    Reflect::get(&js_sys::global(), &JsValue::from_str("indexedDB"))?
        .dyn_into::<IdbFactory>()
        .map_err(|_| JsValue::from_str("indexedDB is not available in this context"))
}

/// Await an `IDBRequest` via its success/error events.
async fn request(req: IdbRequest) -> Result<JsValue, JsValue> {
    let (tx, rx) = oneshot::channel::<Result<JsValue, JsValue>>();
    let slot = Rc::new(RefCell::new(Some(tx)));

    let on_success = {
        let req = req.clone();
        let slot = Rc::clone(&slot);
        Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            if let Some(tx) = slot.borrow_mut().take() {
                let _ = tx.send(req.result());
            }
        })
    };
    let on_error = {
        let req = req.clone();
        let slot = Rc::clone(&slot);
        Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            if let Some(tx) = slot.borrow_mut().take() {
                let err = req
                    .error()
                    .ok()
                    .flatten()
                    .map(JsValue::from)
                    .unwrap_or_else(|| JsValue::from_str("IndexedDB request failed"));
                let _ = tx.send(Err(err));
            }
        })
    };
    req.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
    req.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    let out = rx
        .await
        .unwrap_or_else(|_| Err(JsValue::from_str("IndexedDB request was dropped")));
    req.set_onsuccess(None);
    req.set_onerror(None);
    out
}

/// Await an `IDBTransaction` reaching complete/error/abort.
async fn transaction_done(tx: &IdbTransaction) -> Result<(), JsValue> {
    let (done_tx, done_rx) = oneshot::channel::<Result<(), JsValue>>();
    let slot = Rc::new(RefCell::new(Some(done_tx)));

    let on_complete = {
        let slot = Rc::clone(&slot);
        Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            if let Some(s) = slot.borrow_mut().take() {
                let _ = s.send(Ok(()));
            }
        })
    };
    let on_fail = {
        let slot = Rc::clone(&slot);
        let tx = tx.clone();
        Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            if let Some(s) = slot.borrow_mut().take() {
                let err = tx
                    .error()
                    .map(JsValue::from)
                    .unwrap_or_else(|| JsValue::from_str("IndexedDB transaction aborted"));
                let _ = s.send(Err(err));
            }
        })
    };
    tx.set_oncomplete(Some(on_complete.as_ref().unchecked_ref()));
    tx.set_onerror(Some(on_fail.as_ref().unchecked_ref()));
    tx.set_onabort(Some(on_fail.as_ref().unchecked_ref()));
    let out = done_rx
        .await
        .unwrap_or_else(|_| Err(JsValue::from_str("IndexedDB transaction was dropped")));
    tx.set_oncomplete(None);
    tx.set_onerror(None);
    tx.set_onabort(None);
    out
}

async fn open_db(name: &str) -> Result<IdbDatabase, JsValue> {
    let factory = idb_factory()?;
    let open_req: IdbOpenDbRequest = factory.open_with_u32(name, DB_VERSION)?;
    let on_upgrade =
        Closure::<dyn FnMut(IdbVersionChangeEvent)>::new(move |event: IdbVersionChangeEvent| {
            let Some(target) = event.target() else {
                return;
            };
            let Ok(req) = target.dyn_into::<IdbOpenDbRequest>() else {
                return;
            };
            let Ok(db) = req.result().and_then(|r| {
                r.dyn_into::<IdbDatabase>()
                    .map_err(|_| JsValue::from_str("not a database"))
            }) else {
                return;
            };
            let names = db.object_store_names();
            if !names.contains(BLOCKS) {
                let _ = db.create_object_store(BLOCKS);
            }
            if !names.contains(META) {
                let _ = db.create_object_store(META);
            }
        });
    open_req.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
    // `blocked` is a third outcome alongside success and error: another
    // connection is holding the database open across a version change, so
    // this request waits — possibly forever, if that connection never
    // closes. It is not fatal (the open still completes if the other side
    // goes away), but it is invisible otherwise, and an open that never
    // returns used to strand the whole driver. The driver no longer waits
    // on this before serving jobs; log it so the condition is at least
    // diagnosable when persistence silently never arrives.
    let on_blocked = Closure::<dyn FnMut(Event)>::new(move |_: Event| {
        tracing::warn!(
            "IndexedDB open is blocked by another open connection; \
             the block cache will not attach until that connection closes"
        );
    });
    open_req.set_onblocked(Some(on_blocked.as_ref().unchecked_ref()));
    let base: &IdbRequest = &open_req;
    let result = request(base.clone()).await;
    open_req.set_onupgradeneeded(None);
    open_req.set_onblocked(None);
    drop(on_upgrade);
    drop(on_blocked);
    result?
        .dyn_into::<IdbDatabase>()
        .map_err(|_| JsValue::from_str("open did not yield a database"))
}

fn meta_record(size: u64, last_access: f64) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(
        &obj,
        &JsValue::from_str("s"),
        &JsValue::from_f64(size as f64),
    );
    let _ = Reflect::set(
        &obj,
        &JsValue::from_str("a"),
        &JsValue::from_f64(last_access),
    );
    obj.into()
}

fn meta_field(value: &JsValue, name: &str) -> f64 {
    Reflect::get(value, &JsValue::from_str(name))
        .ok()
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
}

/// Read every `meta` record: `(key, size, last_access)`.
async fn scan_meta(db: &IdbDatabase) -> Result<Vec<(String, u64, f64)>, JsValue> {
    let tx = db.transaction_with_str_and_mode(META, IdbTransactionMode::Readonly)?;
    let store = tx.object_store(META)?;
    let req = store.open_cursor()?;

    let records: Rc<RefCell<Vec<(String, u64, f64)>>> = Rc::new(RefCell::new(Vec::new()));
    let (done_tx, done_rx) = oneshot::channel::<Result<(), JsValue>>();
    let slot = Rc::new(RefCell::new(Some(done_tx)));

    let on_success = {
        let req = req.clone();
        let records = Rc::clone(&records);
        let slot = Rc::clone(&slot);
        Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            let finish = |r: Result<(), JsValue>| {
                if let Some(s) = slot.borrow_mut().take() {
                    let _ = s.send(r);
                }
            };
            let result = match req.result() {
                Ok(r) => r,
                Err(e) => {
                    finish(Err(e));
                    return;
                }
            };
            if result.is_null() || result.is_undefined() {
                finish(Ok(()));
                return;
            }
            let Ok(cursor) = result.dyn_into::<IdbCursorWithValue>() else {
                finish(Err(JsValue::from_str("meta cursor has unexpected shape")));
                return;
            };
            if let (Ok(key), Ok(value)) = (cursor.key(), cursor.value()) {
                if let Some(key) = key.as_string() {
                    let size = meta_field(&value, "s") as u64;
                    let last = meta_field(&value, "a");
                    records.borrow_mut().push((key, size, last));
                }
            }
            if let Err(e) = cursor.continue_() {
                finish(Err(e));
            }
        })
    };
    let on_error = {
        let req = req.clone();
        let slot = Rc::clone(&slot);
        Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            if let Some(s) = slot.borrow_mut().take() {
                let err = req
                    .error()
                    .ok()
                    .flatten()
                    .map(JsValue::from)
                    .unwrap_or_else(|| JsValue::from_str("meta scan failed"));
                let _ = s.send(Err(err));
            }
        })
    };
    req.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
    req.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    let out = done_rx
        .await
        .unwrap_or_else(|_| Err(JsValue::from_str("meta scan was dropped")));
    req.set_onsuccess(None);
    req.set_onerror(None);
    out?;
    Ok(records.take())
}

fn spawn_flusher(cache: Weak<IdbCache>, interval: Duration) {
    let ms = crate::config::timer_millis(interval).max(1_000);
    spawn_local(async move {
        loop {
            TimeoutFuture::new(ms).await;
            let Some(cache) = cache.upgrade() else {
                break;
            };
            cache.flush_access_times().await;
        }
    });
}

impl IdbCache {
    /// Open (creating if needed) the database named in `config` and rebuild
    /// the eviction index from its metadata.
    pub async fn open(config: &CacheConfig) -> Result<Rc<Self>, String> {
        let db = open_db(&config.db_name).await.map_err(js_text)?;
        let records = scan_meta(&db).await.map_err(js_text)?;
        let cache = Rc::new(Self {
            db,
            index: RefCell::new(CacheIndex::load(records)),
            dirty: RefCell::new(HashMap::new()),
            config: config.clone(),
        });
        spawn_flusher(Rc::downgrade(&cache), config.access_flush_interval);
        Ok(cache)
    }

    /// Number of indexed blocks.
    pub fn len(&self) -> usize {
        self.index.borrow().len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.borrow().is_empty()
    }

    /// Indexed bytes.
    pub fn total_bytes(&self) -> u64 {
        self.index.borrow().total_bytes()
    }

    fn store(&self, name: &str, mode: IdbTransactionMode) -> Result<IdbObjectStore, JsValue> {
        self.db
            .transaction_with_str_and_mode(name, mode)?
            .object_store(name)
    }

    /// Look a block up. A hit is one copy out of JavaScript memory.
    pub async fn get(&self, key: &ContentId) -> Option<Bytes> {
        let k = key.to_string();
        let value = match self.store(BLOCKS, IdbTransactionMode::Readonly) {
            Ok(store) => match store.get(&JsValue::from_str(&k)) {
                Ok(req) => request(req).await,
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        let value = match value {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(key = %k, error = %js_text(e), "IndexedDB get failed");
                return None;
            }
        };
        if value.is_undefined() || value.is_null() {
            // Store miss with a stale index entry (another tab's eviction,
            // a failed transaction): drop the entry, or put()'s
            // already-persisted skip would refuse to re-persist this block
            // forever.
            if self.index.borrow_mut().remove(&k).is_some() {
                self.dirty.borrow_mut().remove(&k);
            }
            return None;
        }
        let bytes = Bytes::from(Uint8Array::new(&value).to_vec());
        let now = Date::now();
        {
            let mut index = self.index.borrow_mut();
            if !index.touch(&k, now) {
                index.insert(k.clone(), bytes.len() as u64, now);
            }
        }
        self.dirty.borrow_mut().insert(k, now);
        Some(bytes)
    }

    /// Persist a verified block (write-behind). Evicts least-recently-used
    /// entries first when the budget would be exceeded.
    pub async fn put(&self, key: ContentId, bytes: Arc<[u8]>) {
        let k = key.to_string();
        let now = Date::now();
        let size = bytes.len() as u64;
        let victims = {
            let mut index = self.index.borrow_mut();
            if index.touch(&k, now) {
                // Already persisted; content is immutable.
                self.dirty.borrow_mut().insert(k, now);
                return;
            }
            index.plan_for_insert(
                size,
                self.config.budget_bytes,
                self.config.low_water_bytes(),
            )
        };
        // Apply the eviction to the in-memory index BEFORE awaiting the
        // transaction: the access-time flusher can run in that window, and
        // consulting the un-updated index would let it resurrect meta for a
        // victim the transaction just deleted (an orphan that inflates the
        // rebuilt index at next open). The NEW key enters the index only
        // after the transaction commits, so the flusher can never write
        // meta for a block that might not land.
        {
            let mut index = self.index.borrow_mut();
            index.apply_eviction(&victims);
        }
        {
            let mut dirty = self.dirty.borrow_mut();
            for victim in &victims {
                dirty.remove(victim);
            }
        }
        match self.write(&k, &bytes, size, now, &victims).await {
            Ok(()) => {
                self.index.borrow_mut().insert(k, size, now);
            }
            Err(e) => {
                // Victims are already gone from the index; if their blocks
                // survived the failed transaction, get() re-adopts them
                // (self-healing). The new block is in neither store nor
                // index — consistent.
                tracing::warn!(key = %k, error = %js_text(e), "IndexedDB put failed");
            }
        }
    }

    async fn write(
        &self,
        key: &str,
        bytes: &[u8],
        size: u64,
        now: f64,
        victims: &[String],
    ) -> Result<(), JsValue> {
        let names = Array::of2(&JsValue::from_str(BLOCKS), &JsValue::from_str(META));
        let tx = self
            .db
            .transaction_with_str_sequence_and_mode(&names, IdbTransactionMode::Readwrite)?;
        let blocks = tx.object_store(BLOCKS)?;
        let meta = tx.object_store(META)?;
        for victim in victims {
            let vk = JsValue::from_str(victim);
            blocks.delete(&vk)?;
            meta.delete(&vk)?;
        }
        let jk = JsValue::from_str(key);
        // Copy into JavaScript memory happens off the read path.
        let payload = Uint8Array::from(bytes);
        blocks.put_with_key(&payload, &jk)?;
        meta.put_with_key(&meta_record(size, now), &jk)?;
        transaction_done(&tx).await
    }

    /// Write batched last-access timestamps to `meta`.
    pub async fn flush_access_times(&self) {
        let dirty: Vec<(String, f64)> = self.dirty.borrow_mut().drain().collect();
        if dirty.is_empty() {
            return;
        }
        let result: Result<(), JsValue> = async {
            let tx = self
                .db
                .transaction_with_str_and_mode(META, IdbTransactionMode::Readwrite)?;
            let meta = tx.object_store(META)?;
            for (key, ts) in &dirty {
                let Some(entry) = self.index.borrow().get(key) else {
                    continue;
                };
                meta.put_with_key(&meta_record(entry.size, *ts), &JsValue::from_str(key))?;
            }
            transaction_done(&tx).await
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(error = %js_text(e), "IndexedDB access-time flush failed");
        }
    }

    /// Close the database handle.
    pub fn close(&self) {
        self.db.close();
    }

    /// Delete a cache database entirely (test and reset support).
    pub async fn delete_database(name: &str) -> Result<(), String> {
        let factory = idb_factory().map_err(js_text)?;
        let req: IdbOpenDbRequest = factory.delete_database(name).map_err(js_text)?;
        let base: &IdbRequest = &req;
        request(base.clone()).await.map_err(js_text)?;
        Ok(())
    }
}
