//! Fetch-streamed SSE for one `SseOpen` job.
//!
//! Opens `url` with `credentials: 'omit'` / CORS mode and forwards raw
//! `ReadableStream` chunks to the engine side. No timeout: SSE is
//! long-lived; cancellation is the chunk receiver dropping, which cancels
//! the reader and aborts the fetch.

use crate::driver::fetch::js_error_text;
use bytes::Bytes;
use fluree_db_nameservice_sync::SseConnectError;
use futures::future::{select, Either};
use gloo_timers::future::TimeoutFuture;
use js_sys::{Function, Promise, Reflect, Uint8Array};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    AbortController, Headers, ReadableStreamDefaultReader, Request, RequestCredentials,
    RequestInit, RequestMode, Response,
};

fn classify_connect(value: &JsValue) -> SseConnectError {
    // Network failures reject with a TypeError; treat everything at
    // connect time as retryable — credentials problems arrive as HTTP
    // statuses, classified below.
    SseConnectError::Retryable(js_error_text(value))
}

/// Run one SSE connection to completion.
pub async fn run(
    url: String,
    headers: Vec<(&'static str, String)>,
    connect_timeout: Duration,
    ready: oneshot::Sender<Result<(), SseConnectError>>,
    chunks: mpsc::Sender<Result<Bytes, String>>,
) {
    macro_rules! fail_ready {
        ($err:expr) => {{
            let _ = ready.send(Err($err));
            return;
        }};
    }

    let controller = match AbortController::new() {
        Ok(c) => c,
        Err(e) => fail_ready!(SseConnectError::Retryable(js_error_text(&e))),
    };
    let init = RequestInit::new();
    init.set_method("GET");
    let header_map = match Headers::new() {
        Ok(h) => h,
        Err(e) => fail_ready!(SseConnectError::Retryable(js_error_text(&e))),
    };
    for (name, value) in &headers {
        if let Err(e) = header_map.set(name, value) {
            fail_ready!(SseConnectError::Retryable(js_error_text(&e)));
        }
    }
    init.set_headers(&header_map);
    init.set_credentials(RequestCredentials::Omit);
    init.set_mode(RequestMode::Cors);
    init.set_signal(Some(&controller.signal()));

    let request = match Request::new_with_str_and_init(&url, &init) {
        Ok(r) => r,
        Err(e) => fail_ready!(SseConnectError::Retryable(js_error_text(&e))),
    };
    let global = js_sys::global();
    let fetch_fn: Function = match Reflect::get(&global, &JsValue::from_str("fetch"))
        .ok()
        .and_then(|f| f.dyn_into().ok())
    {
        Some(f) => f,
        None => fail_ready!(SseConnectError::Fatal(
            "global fetch is not available".to_string()
        )),
    };
    let promise: Promise = match fetch_fn
        .call1(&global, &request)
        .map_err(|e| classify_connect(&e))
        .and_then(|p| {
            p.dyn_into()
                .map_err(|_| SseConnectError::Retryable("fetch did not return a promise".into()))
        }) {
        Ok(p) => p,
        Err(e) => fail_ready!(e),
    };
    // Bound the CONNECT (time to first response headers), not the stream.
    // The module keeps no timeout on the long-lived stream deliberately, but
    // a server that accepts the TCP connection and never sends headers is not
    // long-lived — it is hung, and without this the head-tracking future
    // parks forever inside `until_stopped`, the reconnect backoff never runs,
    // no `Disconnected` is emitted, and the peer silently stops seeing
    // commits. On expiry, abort the fetch and surface a Retryable error so
    // the existing backoff path reconnects.
    let millis = crate::config::timer_millis(connect_timeout);
    let fetch_fut = JsFuture::from(promise);
    let timeout_fut = TimeoutFuture::new(millis);
    futures::pin_mut!(fetch_fut, timeout_fut);
    let response: Response = match select(fetch_fut, timeout_fut).await {
        Either::Left((Ok(r), _)) => match r.dyn_into() {
            Ok(r) => r,
            Err(_) => fail_ready!(SseConnectError::Retryable(
                "fetch resolved to a non-Response value".to_string()
            )),
        },
        Either::Left((Err(e), _)) => fail_ready!(classify_connect(&e)),
        Either::Right(((), _)) => {
            controller.abort();
            fail_ready!(SseConnectError::Retryable(format!(
                "SSE connect exceeded {millis}ms before response headers"
            )));
        }
    };

    let status = response.status();
    if !(200..300).contains(&status) {
        let reason = format!("HTTP status {status}");
        fail_ready!(if status == 401 || status == 403 {
            SseConnectError::Fatal(reason)
        } else {
            SseConnectError::Retryable(reason)
        });
    }
    let Some(body) = response.body() else {
        fail_ready!(SseConnectError::Retryable(
            "response carries no body stream".to_string()
        ));
    };
    let reader: ReadableStreamDefaultReader = match body.get_reader().dyn_into() {
        Ok(r) => r,
        Err(_) => fail_ready!(SseConnectError::Retryable(
            "response body reader has an unexpected shape".to_string()
        )),
    };

    if ready.send(Ok(())).is_err() {
        // The connect attempt was abandoned before headers arrived (pump
        // stopped): do not stream into the void.
        let _ = reader.cancel();
        controller.abort();
        return;
    }

    loop {
        // Race each read against the consumer going away: an IDLE stream
        // (no frames arriving) whose receiver was dropped must release its
        // connection immediately, not on the next chunk — otherwise a
        // stopped pump leaves a zombie request occupying one of the
        // browser's few per-host connection slots.
        let read = JsFuture::from(reader.read());
        let closed = chunks.closed();
        futures::pin_mut!(read);
        futures::pin_mut!(closed);
        let chunk = match futures::future::select(read, closed).await {
            futures::future::Either::Left((Ok(chunk), _)) => chunk,
            futures::future::Either::Left((Err(e), _)) => {
                let _ = chunks.send(Err(js_error_text(&e))).await;
                return;
            }
            futures::future::Either::Right(_) => {
                // Consumer stopped: cancel the stream and abort the fetch.
                let _ = reader.cancel();
                controller.abort();
                return;
            }
        };
        let done = Reflect::get(&chunk, &JsValue::from_str("done"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if done {
            // Clean end: dropping `chunks` ends the engine-side stream.
            return;
        }
        let Ok(value) = Reflect::get(&chunk, &JsValue::from_str("value")) else {
            continue;
        };
        let bytes = Bytes::from(Uint8Array::new(&value).to_vec());
        // Awaits when the channel is full: backpressure to the stream read,
        // not unbounded buffering. `Err` means the consumer dropped.
        if chunks.send(Ok(bytes)).await.is_err() {
            // Consumer stopped between reads.
            let _ = reader.cancel();
            controller.abort();
            return;
        }
    }
}
