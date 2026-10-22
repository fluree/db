//! What both guest programs share: the accelerated hash, phase markers, and
//! reading the input frame.

use risc0_zkvm::guest::env;
use risc0_zkvm::sha::{Impl, Sha256 as _};
use zk_redaction_core::{Probe, Sha256};

pub struct ZkSha;

impl Sha256 for ZkSha {
    fn sha256(data: &[u8]) -> [u8; 32] {
        Impl::hash_bytes(data).as_bytes().try_into().unwrap()
    }
}

pub struct Cycles(pub Vec<(&'static str, u64)>);

impl Cycles {
    pub fn start() -> Self {
        Cycles(vec![("start", env::cycle_count())])
    }

    /// Phase cycle counts, to stdout for the host's cost breakdown; not part
    /// of what the proof commits.
    pub fn report(self) {
        for (phase, cycles) in self.0 {
            println!("phase {phase} {cycles}");
        }
    }
}

impl Probe for Cycles {
    fn mark(&mut self, phase: &'static str) {
        self.0.push((phase, env::cycle_count()));
    }
}

/// The host's `write_frame`: a `u32` length, then the bytes. (The guest's
/// own `read_frame` is unstable API.)
pub fn read_frame() -> Vec<u8> {
    let mut len: u32 = 0;
    env::read_slice(core::slice::from_mut(&mut len));
    let mut bytes = vec![0u8; len as usize];
    env::read_slice(&mut bytes);
    bytes
}

/// The C allocator the zstd decoder links against, over the guest's own:
/// each block carries its size ahead of it, for `free`.
#[cfg(feature = "c-zstd")]
mod c_alloc {
    use core::ffi::c_void;
    use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};

    const HEADER: usize = 16;

    unsafe fn block(size: usize, zeroed: bool) -> *mut c_void {
        let Ok(layout) = Layout::from_size_align(size + HEADER, HEADER) else {
            return core::ptr::null_mut();
        };
        let base = if zeroed { alloc_zeroed(layout) } else { alloc(layout) };
        if base.is_null() {
            return core::ptr::null_mut();
        }
        (base as *mut usize).write(size);
        base.add(HEADER) as *mut c_void
    }

    #[no_mangle]
    pub unsafe extern "C" fn malloc(size: usize) -> *mut c_void {
        block(size, false)
    }

    #[no_mangle]
    pub unsafe extern "C" fn calloc(count: usize, size: usize) -> *mut c_void {
        match count.checked_mul(size) {
            Some(size) => block(size, true),
            None => core::ptr::null_mut(),
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn free(ptr: *mut c_void) {
        if ptr.is_null() {
            return;
        }
        let base = (ptr as *mut u8).sub(HEADER);
        let size = (base as *const usize).read();
        dealloc(base, Layout::from_size_align_unchecked(size + HEADER, HEADER));
    }
}
