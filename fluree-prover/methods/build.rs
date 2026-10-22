use std::collections::HashMap;

use risc0_build::{DockerOptionsBuilder, GuestOptionsBuilder};

/// RISC Zero's guest toolchain image (Rust 1.97), by digest.
const BUILDER: &str =
    "r0.1.97.0@sha256:7ae0a27fa72b10fb5c1066005bd949ae43dc4ead8a284f2d6572b99c82ab4468";

fn main() {
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
