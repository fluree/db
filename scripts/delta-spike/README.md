# Delta reader experiment

This standalone experiment generates synthetic Delta tables and compares readers
with explicit expected rows. It does not register a Fluree graph source or change
the server/CLI dependency graph. Python tools and the Rust probe have separate
lockfiles. Run commands below from the repository root.

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
after pruning. Arrow batches are serialized to JSON for oracle comparisons;
a production adapter would stream typed Fluree column batches. The small fixture
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
| Kernel 0.28.0 + default engine | Probe selects Arrow/Parquet 58.4; the release also supports Arrow 59. Its `object_store` 0.13.2 dependency brings reqwest 0.12, alongside the engine's reqwest 0.13. Arrow features also enable multiple cloud backends. |
| Delta-rs Rust 0.32.4 (manifest inspection; Rust API not built here) | Arrow/Parquet 58, `object_store` 0.13.2, and the `buoyant_kernel` 0.22 family. DataFusion 53.1 is optional in the core crate; the Python wheel's contents do not establish production Rust binary size. |
| Fluree production workspace | Arrow/Parquet 54 and unified reqwest 0.13. Neither candidate can be added as-is while keeping those dependency choices unchanged. |

Kernel's logical scan API remains a promising fit for Fluree's existing query
engine. Before integrating it, decide how to align Arrow and storage/HTTP
dependencies: an upstream-compatible upgrade or a custom engine needs evaluation.
Measure the chosen production configuration after that decision. This standalone
probe intentionally permits duplicate HTTP versions to expose the issue without
adding them to the product build.

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
Dependency alignment is still required. Timestamp-to-version resolution is
also adapter work: the inspected Kernel 0.28 snapshot builder exposes explicit
`at_version`, while snapshots expose `get_timestamp`; this probe has not tested
timestamp selection or history after log/checkpoint cleanup.

The completed fixture can use the same isolated S3 upload/read procedure above;
preserve its relative data paths and deletion-vector sidecars, and keep the
deliberately missing historical data file absent.

Feature references: [Spark/Delta compatibility](https://docs.delta.io/releases/),
[deletion vectors](https://docs.delta.io/delta-deletion-vectors/),
[column mapping](https://docs.delta.io/delta-column-mapping/).

Validation commands:

```sh
cargo fmt --manifest-path scripts/delta-spike/Cargo.toml -- --check
cargo clippy --locked --manifest-path scripts/delta-spike/Cargo.toml --all-targets -- -D warnings
```

References: [Kernel 0.28.0 dependencies](https://github.com/delta-io/delta-kernel-rs/blob/v0.28.0/kernel/Cargo.toml),
[default engine dependencies](https://github.com/delta-io/delta-kernel-rs/blob/v0.28.0/default-engine/Cargo.toml),
[delta-rs 0.32.4 dependencies](https://github.com/delta-io/delta-rs/blob/rust-v0.32.4/Cargo.toml),
[delta-rs core features](https://github.com/delta-io/delta-rs/blob/rust-v0.32.4/crates/core/Cargo.toml).
