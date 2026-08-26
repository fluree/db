//! WASM size/linkage probe: drives the real end-to-end path (memory ledger →
//! create → JSON-LD insert → SPARQL query) so the linker must keep the whole
//! engine. Polled manually so no JS async glue is needed for measurement.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use fluree_db_api::FlureeBuilder;

thread_local! {
    static FUT: RefCell<Option<Pin<Box<dyn Future<Output = Result<usize, String>>>>>> =
        const { RefCell::new(None) };
}

async fn run() -> Result<usize, String> {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("wasm/probe")
        .await
        .map_err(|e| e.to_string())?;
    let tx = serde_json::json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
        ]
    });
    let committed = fluree.insert(ledger, &tx).await.map_err(|e| e.to_string())?;
    let _ = committed;
    let view = fluree.db("wasm/probe").await.map_err(|e| e.to_string())?;
    let result = fluree
        .query(&view, "SELECT ?name WHERE { ?s <http://example.org/ns/name> ?name }")
        .await
        .map_err(|e| e.to_string())?;
    // Positive ran-marker: the two inserted names must round-trip. An empty
    // result exiting 0 would make the smoke vacuous.
    let rendered = format!("{result:?}");
    if !(rendered.contains("Alice") && rendered.contains("Bob")) {
        return Err(format!("query result missing inserted names: {rendered}"));
    }
    Ok(rendered.len())
}

fn noop_waker() -> Waker {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(|_| RAW, |_| {}, |_| {}, |_| {});
    const RAW: RawWaker = RawWaker::new(std::ptr::null(), &VTABLE);
    unsafe { Waker::from_raw(RAW) }
}

/// Start the probe future.
#[no_mangle]
pub extern "C" fn probe_start() {
    FUT.with(|f| *f.borrow_mut() = Some(Box::pin(run())));
}

/// Poll once: 0 = pending, 1 = done-ok, -1 = done-err, -2 = not started.
#[no_mangle]
pub extern "C" fn probe_poll() -> i32 {
    FUT.with(|f| {
        let mut slot = f.borrow_mut();
        let Some(fut) = slot.as_mut() else { return -2 };
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Pending => 0,
            Poll::Ready(Ok(_)) => {
                *slot = None;
                1
            }
            Poll::Ready(Err(_)) => {
                *slot = None;
                -1
            }
        }
    })
}
