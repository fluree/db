//! Target-compat shims for the wasm32 build.
//!
//! On native this module is the canonical import path for the re-exported
//! types below — not a spike artifact; removing it breaks native imports.
//!
//! On native these re-export the real implementations; on wasm32 they provide
//! compile-compatible stand-ins so the read path builds. Runtime behavior on
//! wasm: mmap-backed loads fail over to the Owned/CAS byte path, and the
//! spatial provider map stays empty (spatial search unsupported on wasm).

/// Drop-in stand-in for the `memmap2` crate: mapping always fails, pushing
/// callers onto the `SharedLeafBytes::Owned` / CAS-fetch path.
#[cfg(target_arch = "wasm32")]
pub mod memmap2 {
    pub struct Mmap(Vec<u8>);

    impl Mmap {
        /// # Safety
        /// Signature parity with `memmap2::Mmap::map`; never maps on wasm.
        pub unsafe fn map(_file: &std::fs::File) -> std::io::Result<Mmap> {
            Err(std::io::Error::other("mmap unavailable on wasm32"))
        }
    }

    impl std::ops::Deref for Mmap {
        type Target = [u8];
        fn deref(&self) -> &[u8] {
            &self.0
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use fluree_db_spatial::SpatialIndexProvider;

/// Object-safe stand-in: never implemented or invoked on wasm32; keeps
/// `Arc<dyn SpatialIndexProvider>` maps compiling with zero call sites.
#[cfg(target_arch = "wasm32")]
pub trait SpatialIndexProvider: Send + Sync {}
