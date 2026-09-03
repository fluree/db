# fluree-wasm-probe

Size/linkage/runtime probe for the `wasm32-unknown-unknown` build of the
query/api stack. `src/lib.rs` drives the real end-to-end path — memory ledger
→ `create_ledger` → JSON-LD `insert` → SPARQL `query` — through two raw
`extern "C"` exports (`probe_start`, `probe_poll`) so the linker must keep the
whole engine and no JS async glue is needed for measurement.

## Node smoke run (executable evidence, seed of the CI smoke test)

```
node run-node.mjs             # dev profile
node run-node.mjs --release   # release profile
```

The harness runs `wasm-pack build [--dev|--release] --target nodejs
--out-dir pkg-node-{dev,release}` (wasm-pack fetches the matching
wasm-bindgen CLI), appends a raw-exports handle to the generated glue (the
nodejs wrapper only re-exports `#[wasm_bindgen]` items), instantiates the
module under Node, calls `probe_start`, and polls `probe_poll` to completion.

**Exit 0 requires the positive ran-marker**: poll code `1`, the probe
future's own `Ok(_)`. Pending exhaustion (`0`), `Err` (`-1`), not-started
(`-2`), and any trap all exit non-zero — "didn't crash" is never a pass.

The release profile skips wasm-pack's bundled wasm-opt pass
(`package.metadata.wasm-pack.profile.release` in Cargo.toml): installed
wasm-opt binaries can reject bulk-memory output, and correctness is this
harness's job, not size.

## Size measurement (separate from the smoke run)

```
cargo build --release --target wasm32-unknown-unknown
wasm-opt -Oz --enable-bulk-memory --enable-sign-ext \
  --enable-nontrapping-float-to-int \
  target/wasm32-unknown-unknown/release/fluree_wasm_probe.wasm -o probe_opt.wasm
```

## Requirements

`rustup target add wasm32-unknown-unknown`; the repo's `.cargo/config.toml`
wasm toolchain wiring (wasm32-capable clang for zstd-sys, getrandom backend
rustflags); `wasm-pack` and Node for the smoke run.
