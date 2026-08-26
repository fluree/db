//! `fetch()` execution for one [`TransportRequest`].
//!
//! Contract (see `fluree_db_nameservice_sync::transport`): `Ok` for any
//! HTTP status, full-body buffering, `credentials: 'omit'`, CORS mode,
//! transport-owned timeout via `AbortController`, error classes
//! `Timeout` / `Connect` / `Request` / `Body`, and exactly one copy of the
//! body out of JavaScript memory.

use bytes::Bytes;
use fluree_db_nameservice_sync::{
    TransportError, TransportMethod, TransportRequest, TransportResponse,
};
use futures::future::{select, Either};
use gloo_timers::future::TimeoutFuture;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use js_sys::{Array, Function, Promise, Reflect, Uint8Array};
use std::time::Duration;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    AbortController, DomException, Headers, Request, RequestCredentials, RequestInit, RequestMode,
    Response,
};

/// Human-readable text for a thrown JavaScript value.
pub(crate) fn js_error_text(value: &JsValue) -> String {
    if let Some(err) = value.dyn_ref::<js_sys::Error>() {
        return String::from(err.message());
    }
    if let Some(s) = value.as_string() {
        return s;
    }
    format!("{value:?}")
}

fn request_err(value: JsValue) -> TransportError {
    TransportError::Request(js_error_text(&value))
}

/// Classify a rejected `fetch()` promise.
fn classify_fetch_error(value: JsValue) -> TransportError {
    if let Some(dom) = value.dyn_ref::<DomException>() {
        if dom.name() == "AbortError" {
            return TransportError::Timeout(dom.message());
        }
        return TransportError::Request(format!("{}: {}", dom.name(), dom.message()));
    }
    if value.is_instance_of::<js_sys::TypeError>() {
        // Network-level failures (refused, DNS, CORS rejection) reject with
        // a bare TypeError.
        return TransportError::Connect(js_error_text(&value));
    }
    TransportError::Request(js_error_text(&value))
}

fn timeout_millis(timeout: Duration) -> u32 {
    u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX)
}

/// Execute one request against the global `fetch`.
pub async fn execute(
    req: TransportRequest,
    timeout: Duration,
) -> Result<TransportResponse, TransportError> {
    let timeout_ms = timeout_millis(timeout);
    let controller = AbortController::new().map_err(request_err)?;

    let init = RequestInit::new();
    init.set_method(match req.method {
        TransportMethod::Get => "GET",
        TransportMethod::Post => "POST",
    });
    let headers = Headers::new().map_err(request_err)?;
    for (name, value) in &req.headers {
        headers.set(name, value).map_err(request_err)?;
    }
    init.set_headers(&headers);
    if let Some(body) = &req.body {
        // Request bodies are the small JSON block requests; copying them
        // into JS memory is the only way to hand them to fetch.
        let array = Uint8Array::from(&body[..]);
        init.set_body(Some(&array));
    }
    init.set_credentials(RequestCredentials::Omit);
    init.set_mode(RequestMode::Cors);
    init.set_signal(Some(&controller.signal()));

    let request = Request::new_with_str_and_init(&req.url, &init).map_err(request_err)?;

    // `fetch` must be called with the global as `this` (window or worker
    // scope) or the engine throws "Illegal invocation".
    let global = js_sys::global();
    let fetch_fn: Function = Reflect::get(&global, &JsValue::from_str("fetch"))
        .map_err(request_err)?
        .dyn_into()
        .map_err(|_| TransportError::Request("global fetch is not a function".to_string()))?;
    let promise: Promise = fetch_fn
        .call1(&global, &request)
        .map_err(classify_fetch_error)?
        .dyn_into()
        .map_err(|_| TransportError::Request("fetch did not return a promise".to_string()))?;

    let response = match select(
        Box::pin(JsFuture::from(promise)),
        Box::pin(TimeoutFuture::new(timeout_ms)),
    )
    .await
    {
        Either::Left((result, _)) => result.map_err(classify_fetch_error)?,
        Either::Right(_) => {
            controller.abort();
            return Err(TransportError::Timeout(format!(
                "fetch timed out after {timeout_ms} ms"
            )));
        }
    };
    let response: Response = response
        .dyn_into()
        .map_err(|_| TransportError::Request("fetch resolved to a non-Response value".to_string()))?;

    let status = StatusCode::from_u16(response.status())
        .map_err(|e| TransportError::Request(format!("invalid HTTP status: {e}")))?;
    let headers = collect_headers(&response.headers());

    let body_promise = response
        .array_buffer()
        .map_err(|e| TransportError::Body(js_error_text(&e)))?;
    let buffer = match select(
        Box::pin(JsFuture::from(body_promise)),
        Box::pin(TimeoutFuture::new(timeout_ms)),
    )
    .await
    {
        Either::Left((result, _)) => result.map_err(|e| TransportError::Body(js_error_text(&e)))?,
        Either::Right(_) => {
            controller.abort();
            return Err(TransportError::Timeout(format!(
                "response body read timed out after {timeout_ms} ms"
            )));
        }
    };

    // The one copy out of JavaScript memory; `Bytes::from(Vec)` is free.
    let body = Bytes::from(Uint8Array::new(&buffer).to_vec());

    Ok(TransportResponse {
        status,
        headers,
        body,
    })
}

fn collect_headers(headers: &Headers) -> HeaderMap {
    let mut map = HeaderMap::new();
    let Ok(Some(iter)) = js_sys::try_iter(headers.as_ref()) else {
        return map;
    };
    for entry in iter.flatten() {
        let pair = Array::from(&entry);
        let (Some(name), Some(value)) = (pair.get(0).as_string(), pair.get(1).as_string()) else {
            continue;
        };
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            map.append(name, value);
        }
    }
    map
}
