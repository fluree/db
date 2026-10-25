use std::collections::HashMap;

use risc0_build::{DockerOptionsBuilder, GuestOptionsBuilder};

/// RISC Zero's guest toolchain image (Rust 1.97), by digest.
const BUILDER: &str =
    "r0.1.97.0@sha256:7ae0a27fa72b10fb5c1066005bd949ae43dc4ead8a284f2d6572b99c82ab4468";

fn main() {
    // A guest built elsewhere by this script, in the pinned container
    // (`--example guest` writes it): a platform whose runners have no
    // Docker builds its prover from it, and proves the same image.
    println!("cargo:rerun-if-env-changed=FLUREE_PROVER_GUEST");
    if let Some(guest) = std::env::var_os("FLUREE_PROVER_GUEST") {
        let path = std::fs::canonicalize(&guest).expect("FLUREE_PROVER_GUEST");
        println!("cargo:rerun-if-changed={}", path.display());
        let program = std::fs::read(&path).expect("FLUREE_PROVER_GUEST");
        let id = risc0_binfmt::compute_image_id(&program).expect("a guest program");
        let methods = format!(
            "pub const REDACT_ELF: &[u8] = include_bytes!({path:?});\n\
             pub const REDACT_PATH: &str = {path:?};\n\
             pub const REDACT_ID: [u32; 8] = {:?};\n",
            id.as_words()
        );
        let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("methods.rs");
        std::fs::write(out, methods).unwrap();
        return;
    }

    // RISC Zero's containerized build, from the repository root: the guest
    // compiles at the same paths with the same toolchain wherever it is
    // built, so its image id depends only on its sources and anyone can
    // rebuild the id Fluree pins.
    let docker = DockerOptionsBuilder::default()
        .root_dir("../..")
        .docker_container_tag(BUILDER)
        .build()
        .expect("docker options");
    let guest = GuestOptionsBuilder::default()
        .use_docker(docker)
        .build()
        .expect("guest options");
    risc0_build::embed_methods_with_options(HashMap::from([("redact", guest)]));
}
