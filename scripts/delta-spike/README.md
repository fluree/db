# Delta reader experiment

This standalone experiment generates synthetic Delta tables and compares readers
with explicit expected rows. It does not register a Fluree graph source or change
the server/CLI dependency graph. Python tools and the Rust probe have separate
lockfiles. Run commands below from the repository root.

## Prepare the storage compatibility patch

Before running Cargo, prepare the isolated dependency (Python 3 and `patch`
required):

```sh
python3 scripts/delta-spike/prepare_storage.py
```

This downloads the published `object_store` 0.13.2 source archive, checks its
SHA-256 against the published checksum pinned in the script, and applies
`patches/object_store-0.13.2-reqwest013.patch` under ignored `.patched/`.
For offline preparation, pass `--archive /path/to/object_store-0.13.2.crate`.
Repeated preparation verifies the generated source and refuses to overwrite
changes. The archive's Apache license and NOTICE are retained. Generated source
is not committed; the tracked script, patch, and experiment lockfile reproduce
it. The root workspace has no dependency patch.

## Generate and read locally

Requirements: the repository's Rust toolchain, `uv`, and Python 3.12 (uv can
provide it). Choose a new output directory; the generator refuses to overwrite
an existing one.

```sh
uv run --locked --python 3.12 scripts/delta-spike/fixture.py /tmp/delta-fixture-001
cargo run --locked --manifest-path scripts/delta-spike/Cargo.toml -- \
  /tmp/delta-fixture-001/manifest.json /tmp/delta-fixture-001
```

The generator writes:

- `dim_store`: one version with three synthetic stores.
- `fact_order`: five versions covering initial rows, append, update, delete,
  and an added nullable column. Data includes nullable integer amounts and
  string/null partition values. The latest version has a checkpoint.
- `manifest.json`: expected rows and columns for every version, plus file sizes
  and SHA-256 hashes. Expected rows are maintained separately from reader output.

All data and log files are retained. The generator verifies every version before
and after checkpoint creation. Data and expected results are repeatable; commit
timestamps, table IDs, and physical filenames can differ between runs.

The native probe checks current and historical rows, projected scans with an
`amount >= 300` predicate, empty results, and nonexistent-version rejection.
It uses Kernel's logical scan execution and applies an exact residual predicate
after pruning. Kernel's logical Arrow batches now pass through the real
`fluree-db-tabular::ColumnBatch` type before JSON oracle comparison. The small fixture
does not measure throughput or prove policy/count/top-k optimization parity.

## Read a copy on S3

Use a dedicated, unused prefix in a development bucket. Stop all writes before
copying the fixture; the generator validates that data-file references are
relative and contained in the table. Preserve removed files because historical
versions still reference them. Do not use this copy procedure for a live table.

For example, after confirming the destination prefix is unused:

```sh
aws s3 sync /tmp/delta-fixture-001/ s3://YOUR-DEV-BUCKET/delta-spike/RUN-ID/ \
  --profile YOUR-PROFILE --region us-east-1 --only-show-errors

python3 scripts/delta-spike/with_profile.py --profile YOUR-PROFILE -- \
  scripts/delta-spike/target/debug/fluree-delta-spike \
  /tmp/delta-fixture-001/manifest.json s3://YOUR-DEV-BUCKET/delta-spike/RUN-ID/

python3 scripts/delta-spike/with_profile.py --profile YOUR-PROFILE -- \
  uv run --locked --python 3.12 scripts/delta-spike/fixture.py \
  s3://YOUR-DEV-BUCKET/delta-spike/RUN-ID/ \
  --verify-manifest /tmp/delta-fixture-001/manifest.json
```

`with_profile.py` resolves credentials through AWS CLI and supplies them only to
the child process environment. It does not log or write them to files. This is
a short-lived development bridge without token renewal, not a production
credential-provider implementation. All probe table operations are reads;
the explicit `aws s3 sync` command above uploads the synthetic fixture.

## Initial results and dependency decisions

On 2026-09-14, Kernel 0.28.0 passed six snapshots and 18 scans on both local
storage and S3. Delta-rs Python 1.6.3 passed all six snapshots, including the
filtered/projection checks, locally and on S3. The test table files total about
35 KB. Both readers matched the hand-maintained expected data. These results
cover the basic fixture only. The independent Spark fixtures below extend that
evidence; customer-produced tables are still needed.

| Candidate | Observed dependency considerations |
| --- | --- |
| Kernel 0.28.0 + default engine | Probe selects Arrow/Parquet 58.4; the release also supports Arrow 59. The published `object_store` 0.13.2 brings reqwest 0.12 alongside the engine's reqwest 0.13. The compatibility patch below removes that duplicate in this experiment. Arrow features still enable multiple cloud backends. |
| Delta-rs Rust 0.32.4 (manifest inspection; Rust API not built here) | Arrow/Parquet 58, `object_store` 0.13.2, and the `buoyant_kernel` 0.22 family. DataFusion 53.1 is optional in the core crate; the Python wheel's contents do not establish production Rust binary size. |
| Fluree production workspace | Aligned to Arrow/Parquet 58.4 and unified reqwest 0.13. A tested experimental storage backport now exists; production packaging and maintenance remain to be resolved. |

Kernel's logical scan API remains a promising fit for Fluree's existing query
engine. Arrow/Parquet alignment is complete. The isolated compatibility backport
also unifies its HTTP dependencies without replacing Kernel's logical reader.
Measure the chosen production configuration after deciding how to maintain and
distribute the storage change.

## Storage/HTTP compatibility result

Rechecked on 2026-09-14: the latest released Kernel remains 0.28.0, and its
Arrow features select `object_store` 0.13. The inspected main revision
`03c39a6ecd7f0af24f6cd338c10a9d0aff16ca84` still does so. Parquet 58.4's object-store
integration also uses 0.13, so upgrading only Kernel to object_store 0.14 would
not align the types. Released object_store 0.14.1 uses reqwest 0.13 but includes
broader API changes.

The experiment instead backports the HTTP changes to object_store 0.13.2:

- Use reqwest 0.13's Rustls/platform verifier and `tls_certs_merge` for custom
  roots and proxy CA configuration. Platform verification replaces the previous
  native-root-loading path; this is a TLS behavior change that needs target testing.
- Preserve the optional `tls-webpki-roots` feature by merging bundled DER roots
  from `webpki-root-certs` into platform trust. Enable the probe's `bundled-roots`
  feature to exercise this configuration.
- Retain the 0.13 storage and credential-provider APIs, signing implementation,
  and Kernel/Parquet scan path. No custom Delta engine or new Azure SDK is added.

The experiment's lockfile now has **one reqwest version, 0.13.5**, and one
Arrow/Parquet stack, 58.4.0. Removing a duplicate dependency does not establish a
binary-size saving; no production Delta artifact has been measured yet. Kernel
still enables AWS, Azure, GCP, and HTTP backends, and signing still uses `ring`.

Validation on aarch64 macOS:

- Upstream object_store library suite with AWS/Azure/GCP/HTTP and bundled roots:
  **184 passed, four ignored**. These tests do not establish live Azure access.
- Local transport regressions check encoded object lengths/bytes, exact HTTP
  ranges even with reqwest gzip enabled, rejection of an untrusted CA, successful
  custom-CA trust, and rejection of a hostname mismatch.
- The preparation tests cover repeatability, changed-source preservation,
  checksum rejection, archive traversal, and symlink rejection.
- Both retained Delta fixture suites pass locally and on S3 through the patched
  reader: basic six snapshots/18 scans, advanced seven snapshots/22 scans plus
  the expected missing-data failure. Eight native tests pass in each root
  configuration, four preparation tests pass, and all-feature/all-target Clippy
  passes. Azure credentials/renewal, HTTPS proxy handshakes, other operating
  systems, and customer tables remain unverified.

```sh
python3 -m unittest discover -s scripts/delta-spike -p test_prepare_storage.py
cargo test --locked --manifest-path scripts/delta-spike/Cargo.toml
cargo test --locked --manifest-path scripts/delta-spike/Cargo.toml --features bundled-roots
cargo tree --locked --manifest-path scripts/delta-spike/Cargo.toml -i reqwest
CARGO_TARGET_DIR="$PWD/scripts/delta-spike/target" cargo test \
  --manifest-path scripts/delta-spike/.patched/object_store-0.13.2/Cargo.toml \
  --lib --features aws,azure,gcp,http,tls-webpki-roots
```

The last command tests the upstream package in its own workspace; its dev-only
lockfile may be regenerated and is excluded from preparation verification.
The spike itself builds with its tracked lockfile. Test-only certificate and
TLS-server libraries are confined to the standalone experiment.

Before production integration, select a maintained pinned fork/vendor package
or an upstream compatible release, preserve attribution, and rerun the provider
and table suites on supported targets. Avoid a generated, untracked source-path
requirement in the product build. The backport is a feasible integration path,
not an upstream release or a completed Azure implementation.

References: [upstream reqwest/crypto migration](https://github.com/apache/arrow-rs-object-store/commit/996e084600a4adc7e14cf34d332d3cb0972547b4),
[inspected Kernel main manifest](https://github.com/delta-io/delta-kernel-rs/blob/03c39a6ecd7f0af24f6cd338c10a9d0aff16ca84/kernel/Cargo.toml),
[Parquet 58.4 manifest](https://github.com/apache/arrow-rs/blob/58.4.0/parquet/Cargo.toml).

Still required: timestamp selection, expired-log/checkpoint failures, broader
type/schema evolution, graph-source
registration, R2RML/policy integration, and representative performance testing.
The basic update/delete fixture uses file rewrites; the advanced fixture below
tests deletion vectors. No Azure/OneLake access or Fluree policy enforcement is
tested here.

## Independent Spark feature fixtures

Requires Java 17 or 21 in addition to `uv` and Python 3.12. The separate script
lock pins Spark/Delta 4.0.0 and Python dependencies. Spark resolves the pinned
Delta JVM package and its transitive jars from Maven on first use; the Python
lock does not checksum those jars. These are development tools only.

```sh
uv run --locked --python 3.12 scripts/delta-spike/spark_fixture.py /tmp/delta-advanced-001
cargo run --locked --manifest-path scripts/delta-spike/Cargo.toml -- \
  /tmp/delta-advanced-001/manifest.json /tmp/delta-advanced-001
```

The generator checks Spark's reads against independently constructed expected
rows and inspects the physical files/log actions:

- `deletion_vectors`: 4,096 rows in two partitioned files, each with five Parquet
  row groups in the tested run. Deleting nine rows must reuse both original
  data files with nonempty deletion vectors. Historical and current results
  exercise logical deletion masks across row groups.
- `column_mapping`: four versions covering physical name mapping, rename,
  drop, and re-add of the original logical name. All versions reuse a single
  Parquet file. Re-added `amount` must be null, never the old column's values.
- `history_loss`: retains the current overwritten snapshot but deliberately
  removes one retired synthetic Parquet file created by this invocation. Its
  version-zero scan must fail with a missing-file error. No existing table is
  vacuumed or modified; the destination must be new. Spark emits an expected
  missing-file stack trace before the final successful JSON summary.

Manifest versions may specify a projected integer `>=` filter on a logical
column, including the renamed column. The native probe also checks empty
results, current reads, and nonexistent-version rejection. Missing-history
checks require successful log replay followed by a missing-data error naming
the expected file, rather than accepting any error as success.

On 2026-09-14, Kernel 0.28.0 passed **seven snapshots, 22 successful scans, and
one expected missing-data failure** both locally and on S3. Two complete Spark generations
produced equivalent logical version oracles. The amount predicate leaves all
DV fixture rows as scan candidates before residual filtering; these results
establish correctness, not row-group pruning effectiveness or relative speed.

To compare a specific table with delta-rs Python:

```sh
uv run --locked --python 3.12 scripts/delta-spike/fixture.py /tmp/delta-advanced-001 \
  --verify-manifest /tmp/delta-advanced-001/manifest.json --table history_loss
```

The Python 1.6.3 `to_pyarrow_table` / PyArrow Dataset path passed `history_loss`,
including missing-file rejection. Selecting `deletion_vectors` or
`column_mapping` instead exits unsuccessfully with `DeltaProtocolError`:
the former rejects the `deletionVectors` reader feature and the latter rejects
minimum reader version 2. This describes the **tested Python read path**, not
every delta-rs Rust API. Never bypass those guards with raw Parquet reads.

These results favor Kernel's logical scan API for the next integration spike.
Production storage packaging is still required. Correction to the initial timestamp
assessment: Kernel 0.28 provides `history_manager::latest_version_as_of`, in
addition to the builder's `at_version` and snapshot's `get_timestamp`. Use that
existing resolver when wiring timestamp selection; do not implement log-history
search ourselves. Its `CommitAt` documents in-commit timestamps when enabled,
otherwise file modification times, which makes copying non-ICT tables relevant
to time-based queries. `Recreatable` concerns log reconstruction; actual scans
must still reject missing data files. Timestamp boundaries, cleanup, and copy
semantics remain untested in this probe.

The completed fixture can use the same isolated S3 upload/read procedure above;
preserve its relative data paths and deletion-vector sidecars, and keep the
deliberately missing historical data file absent.

Feature references: [Spark/Delta compatibility](https://docs.delta.io/releases/),
[deletion vectors](https://docs.delta.io/delta-deletion-vectors/),
[column mapping](https://docs.delta.io/delta-column-mapping/).

## Typed Fluree batch boundary

`src/batch_bridge.rs` converts each logical Kernel batch directly to Fluree's
existing typed column vectors. There is no JSON conversion inside the bridge
and no intermediate per-cell value enum. This is an allocating conversion,
including owned strings/bytes, not zero-copy Arrow interchange. The surrounding
`batches` iterator converts one batch at a time; the oracle consumer alone
collects all rows. Async scheduling, cancellation and bounded engine prefetch
still need integration with the query provider's stream contract.

The bridge accepts booleans, signed integers (byte/short widened to i32),
float/double, strings, bytes, Date32, microsecond timestamps with/without a
timezone, and Decimal128 with its precision/scale intact. It supports Arrow
large/view string and byte representations as well. Unsupported projected
types, including nested collections, fail before scan execution, even for empty
results. Unsupported columns outside the projection are not converted.

Caller-supplied field IDs survive projection order changes. The probe uses
Delta column-mapping IDs where present and full-snapshot positions otherwise;
those fallback positions are **test-only**, not a durable identity scheme for
unmapped tables. Production registration must persist bindings and define
historical R2RML behavior across schema changes. A zero-column Arrow batch
preserves its row count, but this does not yet prove provider COUNT parity.

Six focused tests cover sliced/null scalar arrays, Unicode/binary values,
decimal precision, timestamp frames, large/view arrays, supplied IDs, invalid
schemas, and zero-column row counts. Both retained local fixture suites and the
advanced S3 suite pass through the bridge. These are reader/batch tests; no
Fluree graph-source registration or policy path is wired yet.

Dependency decision: retain the Arrow-free `fluree-db-tabular` contract. Linking
it into this standalone experiment adds only that crate and its existing
`thiserror` dependency; the experiment still has one Arrow version (58.4).
This lets us develop the batch adapter without changing Fluree's shared types.
The subsequent Iceberg upgrade aligns its Arrow/Parquet dependencies to the same
58.4 release, removing the earlier 54/58 version mismatch.

Production Iceberg now passes its scan and API regressions on Arrow/Parquet 58.4.
Representative performance checks remain outstanding. The storage backport
above resolves Kernel's reqwest duplication within the experiment. A storage wrapper alone does not remove dependencies
enabled by Kernel's Cargo features; a custom engine also owns JSON/Parquet and
expression behavior and needs the full fixture suite. The root workspace now
uses Arrow/Parquet 58.4, but Kernel and object_store remain confined to this
standalone experiment.

The upgrade explicitly selects Parquet's `flate2-rust_backened` feature (upstream
spelling) to retain the Rust GZIP backend. Its compatibility regression covers
GZIP/Snappy/Zstd files, delta-packed integers, and time-of-day values normalized
to Fluree's microsecond integer representation. Millisecond time statistics
are conservatively declined because their units differ; exact filtering runs
on normalized values. Local Iceberg history/materialization and R2RML/static
policy parity tests pass with the upgraded decoder.

Production upgrade checks:

```sh
cargo test --locked -p fluree-db-iceberg --all-features
cargo test --locked -p fluree-db-iceberg --no-default-features
cargo clippy --locked -p fluree-db-iceberg --all-features --all-targets -- -D warnings
cargo test --locked -p fluree-db-api --features iceberg,sql \
  --test it_graph_source_r2rml --test it_iceberg_local_fs \
  --test it_iceberg_policy --test it_iceberg_warehouse_root
cargo check --locked -p fluree-db-api --no-default-features --target wasm32-unknown-unknown
```

These pass with 321 feature-enabled Iceberg tests, 286 without default features,
and 87 API tests (overlapping reader tests are counted per configuration).
The standalone Iceberg crate's wasm32 build fails in Tokio/mio networking on
both the upgraded version and the unchanged Arrow 54 revision; the actual
browser API configuration above passes.

On aarch64-apple-darwin, the same `cargo build --locked --release
-p fluree-db-server --bin fluree-server --features otel` configuration (defaults
enabled, LTO and stripping) produced the following host measurements:

| Version | Stripped bytes | Gzip level 9 bytes (`mtime=0`) |
| --- | ---: | ---: |
| Arrow/Parquet 54, after reqwest upgrade | 61,022,384 | 27,582,422 |
| Arrow/Parquet 58.4 and compatibility changes | 63,023,408 | 28,238,897 |

The increase is 2,001,024 bytes (+3.28%), or 656,475 bytes compressed (+2.38%).
The resulting server passes `--help`. This measures one host release build,
not Linux/Lambda artifacts or scan performance; Azure and Kernel are not yet
linked into that server.

History reference: [Kernel 0.28 history manager source](https://github.com/delta-io/delta-kernel-rs/blob/v0.28.0/kernel/src/history_manager/mod.rs).

Validation commands:

```sh
cargo fmt --manifest-path scripts/delta-spike/Cargo.toml -- --check
cargo test --locked --manifest-path scripts/delta-spike/Cargo.toml
cargo clippy --locked --manifest-path scripts/delta-spike/Cargo.toml --all-targets -- -D warnings
```

References: [Kernel 0.28.0 dependencies](https://github.com/delta-io/delta-kernel-rs/blob/v0.28.0/kernel/Cargo.toml),
[default engine dependencies](https://github.com/delta-io/delta-kernel-rs/blob/v0.28.0/default-engine/Cargo.toml),
[delta-rs 0.32.4 dependencies](https://github.com/delta-io/delta-rs/blob/rust-v0.32.4/Cargo.toml),
[delta-rs core features](https://github.com/delta-io/delta-rs/blob/rust-v0.32.4/crates/core/Cargo.toml).
