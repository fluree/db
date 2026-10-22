# fluree-prover

Proves redaction steps for permanent erasure (`DATA_ERASURE.md`, PR 5A). Fluree runs it once per
step, as a child process at low priority in its own scratch directory: the request frame on
stdin, the response frame on stdout (`fluree_db_redaction::protocol`). The prover returns the
replacement and a succinct RISC Zero receipt; Fluree verifies the receipt in-process
(`fluree-db-erasure`) against the guest image ids it pins, and never trusts the prover's word.

The prover is outside the main workspace, so RISC Zero's prover and toolchain never enter a
Fluree build. It proves in-process (`risc0-zkvm`'s `prove` feature), with no `r0vm` to install.

## Building

Linux x86-64, with the RISC Zero toolchain and Docker (with buildx):

```sh
curl -L https://risczero.com/install | bash && rzup install
cargo build --release              # target/release/fluree-prover
target/release/fluree-prover --image-id
```

The guest (`methods/guest`) compiles `fluree-db-redaction` from this repository with the C zstd
decoder and RISC Zero's accelerated curve25519 and big-integer arithmetic. It builds in RISC
Zero's guest-builder container, pinned by digest in `methods/build.rs`, with the repository root
as the build context.

## Image ids

Fluree accepts proofs only from the images in `fluree-db-erasure`'s `REDACT_IMAGE_IDS`. The
containerized build compiles the guest at the same paths with the same toolchain wherever it
runs, so a guest image id depends only on the guest's sources, its `Cargo.lock` and the pinned
builder image: building this directory at a release's commit gives the id that release pins.
Any change to `fluree-db-redaction` that reaches the guest changes the id: add the new id to
`REDACT_IMAGE_IDS`, and remove an old one only when no stored proof uses it.

## Running

Fluree's configuration names the binary, its scratch directory, thread count and priority. One
step at a time is the default; a step that runs past its timeout is killed with its process group
and retried later. The prover refuses to run with `RISC0_DEV_MODE` set, and Fluree clears it.
