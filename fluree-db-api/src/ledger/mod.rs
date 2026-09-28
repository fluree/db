mod loading;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use loading::claim_name;
pub(crate) use loading::while_claimed;
