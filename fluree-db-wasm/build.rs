//! Link-time configuration for the wasm artifacts (cdylib AND the
//! wasm-bindgen-test executables — `cargo:rustc-link-arg` covers both).
//!
//! The wasm32 shadow stack defaults to 1 MiB. This workspace already
//! documents (the `RUST_MIN_STACK = "8388608"` note in `.cargo/config.toml`)
//! that its unoptimized async state machines overflow 2 MiB native stacks;
//! the same futures run on the shadow stack here, and a wasm stack overflow
//! is not a clean abort but a `memory access out of bounds` trap (observed in
//! the dev-profile browser tests). Match the native setting. rustc places the
//! stack first in linear memory, so overflow traps instead of corrupting
//! data, and untouched stack pages cost address space, not resident memory.

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.starts_with("wasm32") {
        println!("cargo:rustc-link-arg=-zstack-size=8388608");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
