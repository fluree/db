//! Writes the guest program to a file, for provers built where the pinned
//! container cannot run (`FLUREE_PROVER_GUEST`).

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: guest <path to write the program to>");
    std::fs::write(&path, fluree_prover_methods::REDACT_ELF).expect("writing the guest program");
}
