# V1 — Adversarial verification: Iceberg merge-on-read DELETE-file gap

Verifier: audit-verifier. Code read-only at `<audit-checkout>`, tip `10e073fe93ac8d8a75a8795a1f89387b2b37c662`. AWS empirical via profile `serverless` (account <aws-account>, us-east-2), read-only. Every paragraph below is one line; labels: READ(file:line) = code read at the audit tip, WEB(url) = external source, AWS(measured) = live S3 read, INFERRED = my reasoning.

## VERDICT (one line)

CONFIRMED — state (a) silently ignored. Iceberg MoR delete files (position + equality) are parsed only far enough to be *recognized and discarded*, never read and never applied, on the entire virtual read path; there is NO loud refusal (state b) and NO application (state c). A virtual query over a MoR-maintained Iceberg table returns deleted rows as if live, and the manifest-derived COUNT over-counts by the delete cardinality. Exposure class = (ii) latent-but-certain (not live-today on the deployed smoke datasets, but certain the moment any MoR writer touches a consumed table). The refutation attempt FAILED to overturn the claim; it only narrowed it (CoW deletes ARE handled correctly — see below).

## TASK 1 — Refutation attempt (code): result CONFIRMED, state (a)

READ(fluree-db-iceberg/src/manifest/manifest_list.rs:23-28) — a `ManifestContent` enum exists: `Data = 0`, `Deletes = 1`, with `from_avro` mapping unknown values to `Data` (:35, :478 test). So the content discriminator (data-vs-delete manifest) IS parsed. This is the ceiling of delete awareness in the crate.

READ(fluree-db-iceberg/src/manifest/manifest_list.rs:154, :178-185) — the default `parse_manifest_list` calls `parse_manifest_list_with_deletes(data, false)`, and the loop DROPS any `entry.is_deletes()` manifest with a bare `continue`, emitting only `tracing::debug!("Skipping delete manifest (Phase 2 does not support delete files)")`. A debug-level log is invisible in production and never reaches the query result — this is silent discard, not refusal. Module doc (:9-12) states plainly "Format v1 tables (no content field) are NOT supported" and content=1 manifests "will SKIP these."

READ(fluree-db-iceberg/src/scan/planner.rs:210, :223-227) — the scan planner reads the manifest list via `parse_manifest_list` (already delete-filtered) and belt-and-suspenders re-skips `manifest_entry.is_deletes()` with `continue`. It then reads each DATA manifest (:230-231) and pushes every surviving data file as a scan task (:233-255). Nowhere in `plan()` is a delete file opened, a positional/equality delete parsed, a sequence number compared, or an error raised when deletes exist.

READ(fluree-db-iceberg/src/io/*) — grep of the entire `io/` (parquet.rs, arrow_reader.rs, batch.rs, chunk_reader.rs, storage.rs) finds NO positional-delete parquet reader, NO equality-delete predicate application, NO deletion-vector (Puffin/DV) handling. Delete application is absent from the read path end-to-end. INFERRED: the crate has no code that could subtract deleted rows even if a caller wanted it to.

READ(fluree-db-iceberg/src/manifest/data_file.rs:189-213) — REFUTATION NARROWING: `parse_manifest` = `parse_manifest_with_deleted(data, false)`, and it correctly DROPS per-entry `status = Deleted (2)` manifest entries (`is_active()` at :68-71, filter at :210-213). So COPY-ON-WRITE deletes (which rewrite data files and mark old entries DELETED) ARE handled correctly — scanned files are exactly ADDED+EXISTING. This confirms the sibling's claim is precisely scoped: the gap is MoR delete *files* (content=1 manifests), NOT entry-status. The claim survives; it is not a false alarm about CoW.

READ(fluree-db-iceberg/src/stats.rs:128-137, :19) — COUNT over-count CONFIRMED. `row_count` is summed from `df.record_count` over surviving data files; the comment (:135-136) explicitly says "consumers (the COUNT(*) manifest shortcut) re-derive the row count" this way. Delete files are never subtracted, so both this sum and any `total-records`-from-summary shortcut OVER-COUNT by (total-position-deletes + total-equality-deletes). No coverage gate protects this (the gate at :180-195 guards null/value stats, not the row total).

READ(fluree-db-iceberg/src/metadata/snapshot.rs:31, :48-85) — CHEAP-GUARD SURFACE EXISTS. `Snapshot.summary: HashMap<String,String>` retains the FULL snapshot summary. Accessors exist for `total_records()` (:49), `total_data_files()` (:56), `total_files_size()` (:63), `operation()` (:70), `added_records()` (:77), `deleted_records()` (:84) — but NONE for `total-delete-files` / `total-position-deletes` / `total-equality-deletes`. Those three keys are nonetheless present verbatim in the raw HashMap (proven empirically below), so a fail-closed guard can read them at zero extra I/O.

## TASK 2 — Exposure bounding

### (a) Snowflake-managed Iceberg — CoW today, MoR imminent

WEB(https://docs.snowflake.com/en/user-guide/tables-iceberg-manage) — `ICEBERG_MERGE_ON_READ_BEHAVIOR` defaults to `AUTO`: MoR for Snowflake-managed **v3** tables and for **ALL externally-managed** tables; **copy-on-write for Snowflake-managed v2 tables**. AUTO keeps v2 on CoW specifically so external readers lacking v2 positional-delete support keep working. So a Fluree virtual read over a *current* Snowflake-managed v2 table sees CoW (safe); over a v3 or externally-managed table it sees MoR (silent staleness).

WEB(https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_03/bcr-2279) — PENDING behavior change (bundle 2026_03): when enabled, Snowflake-managed Iceberg **v2** tables use **merge-on-read with positional delete files BY DEFAULT** for DELETE/UPDATE/MERGE (system default `ENABLE_ICEBERG_MERGE_ON_READ = TRUE`). This flips the exact table class Fluree targets today from CoW to MoR. INFERRED: Snowflake BCR bundles auto-enable on a schedule, so this is a dated, near-certain regression trigger, not a hypothetical.

WEB(https://docs.snowflake.com/en/blog/apache-iceberg-v3-support / release-notes/2026-03-02) — Snowflake-managed **v3** already uses MoR **deletion vectors** by default, and Snowflake can already query Delta/Iceberg deletion vectors. Deletion vectors are a v3 delete-file variant this crate also does not read. INFERRED: as customers move to v3, the gap is live immediately.

### (b) Externally-written BYO buckets (#1505 BYO-IAM segment) — MoR is common-to-default

WEB(https://iceberg.apache.org/docs/latest/spark-writes/) — Apache **Spark**: `write.delete.mode` / `write.update.mode` / `write.merge.mode` all DEFAULT to **copy-on-write** (v2 only). So vanilla Spark is safe by default, BUT streaming/large-table deployments routinely set these to `merge-on-read`; when they do, delete files appear.

WEB(https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-delete.html) — **Athena** DELETE writes Iceberg **position delete files** — "this is known as a merge-on-read delete" — BY DEFAULT. Any BYO bucket maintained via Athena DML produces MoR delete files with no opt-in.

WEB(https://iceberg.apache.org/docs/1.5.0/flink-writes/ + apache/iceberg#11535) — **Flink** upsert/CDC writes **equality delete** files by design (the natural fit for streaming CDC; Debezium/Kafka-Connect/Flink-CDC/RisingWave all emit them). Equality deletes are the harder MoR variant (predicate-based, cross-file) and are entirely unhandled here. INFERRED: any CDC-fed BYO Iceberg lake is MoR from row one.

INFERRED (b-summary): The BYO-IAM segment is the highest-exposure surface — Athena defaults to MoR, Flink/CDC is MoR by construction, and Spark is one table-property away. "External writer" ≈ "MoR likely."

### (c) Empirical — the deployed Fluree smoke bucket (AWS, measured)

AWS(measured) — `aws sts get-caller-identity --profile serverless` = `arn:aws:sts::<aws-account>:assumed-role/<sso-admin-role>`; SSO live. `s3://fl-svl-iceberg-smoke-use2/iceberg/dw/` holds 16 Snowflake-written tables (dim_* / fact_*), each in a `name.<8-char-suffix>/` dir (Snowflake's random table-dir suffix).

AWS(measured) — `fact_order_line.A2zi4Le7` current metadata `00001-...json`: **format-version 2**; single snapshot, `operation=append`; summary `total-delete-files: 0`, `total-position-deletes: 0`, `total-equality-deletes: 0`, `total-records: 120000000`; writer `Apache Iceberg 1.11.0-snowflake.7`; **partition-spec = `{spec-id:0, fields:[]}` (UNPARTITIONED)**. Manifest-list Avro (`snap-6626...avro`, parsed with fastavro): ONE entry, `content = 0` (data), `added_files_count = 123`, `existing = 0`, `deleted = 0` — ZERO delete manifests referenced.

AWS(measured) — `dim_customer.Zj9mxQPV` current metadata `00001-...json`: identical shape — format-version 2, `append`, `total-delete-files/position/equality = 0`, `total-records 1744133`, Iceberg 1.11.0-snowflake.7, partition-spec `fields:[]`.

INFERRED (empirical conclusion): the deployed smoke datasets are Snowflake-managed **v2, append-only, zero delete files** — consistent with Snowflake's current CoW default. The bug is therefore NOT triggered by today's smoke data. Separately, this CONFIRMS the sibling "partition pruning inert" finding at the data layer: Snowflake wrote EMPTY partition specs, so no partition transform exists to prune on regardless of code (see receipt-check c).

## TASK 3 — Severity verdict + guard

SEVERITY = (ii) latent-but-certain. Not (i) live-today: empirically no delete files exist in the deployed smoke tables (AWS-measured, both tables). Not (iii) refuted/mitigated: there is no loud refusal and no application — only a debug log. It is (ii) because MoR delete files arrive with CERTAINTY down at least four independent paths: Snowflake BCR 2026_03 (v2 → MoR by default, dated), Snowflake-managed v3 (MoR deletion vectors now), Snowflake externally-managed tables (MoR now), and BYO buckets via Athena (MoR default) or Flink/CDC (MoR by design). The failure mode is silent wrong answers (stale/deleted rows returned; COUNT inflated) with no error, log-at-info, or metric — the worst class for a database.

GUARD (recommended, fail-closed): refuse the scan when deletes are present, until application is implemented. Cheapest form: add `total_delete_files()` / `total_position_deletes()` / `total_equality_deletes()` accessors to READ(fluree-db-iceberg/src/metadata/snapshot.rs:84) (mirroring `deleted_records()`), and at the scan-plan entry READ(fluree-db-iceberg/src/scan/planner.rs:203-210), after the snapshot is resolved, return `IcebergError` if any of the three summary counters parse > 0. Zero extra I/O — `snapshot.summary` is already in memory. Ground-truth belt-and-suspenders (writers that omit summary counters): at planner.rs:210 call `parse_manifest_list_with_deletes(data, true)` and refuse if any `is_deletes()` entry exists (the manifest list is already read here). The SAME guard must gate the COUNT path READ(fluree-db-iceberg/src/stats.rs:128) — it sums `record_count` independently and would otherwise over-count silently. BEST placement (INFERRED): a single check at snapshot selection READ(fluree-db-iceberg/src/metadata/snapshot.rs:127 `select_snapshot`) or immediately after it in `plan()`, so scan and stats both inherit one fail-closed chokepoint. NOTE: manifest_list.rs:178 is where the signal first appears but is the WRONG guard site — silently `continue`-ing there is the bug; the guard belongs one layer up where an error can propagate to the caller.

## TASK 4 — Cheap receipt-checks (three sibling claims)

(a) Scan top-k gates on DESCENDING only, and DISTINCT kills it — CONFIRMED. READ(fluree-db-query/src/execute/operator_tree.rs:3377) `let can_topk = limit.is_some() && !distinct;` (DISTINCT ⇒ no top-k); READ(:3391-3396) `if can_topk { if primary.direction == SortDirection::Descending { operator.set_topk(primary.var, k) } }` (only a DESCENDING primary key offers scan-side top-k; the comment at :3386-3387 explains ASC declines because SPARQL orders unbound/null first).

(b) `set_row_budget` has a swallowing no-op default, honored by only a handful of operators — CONFIRMED. READ(fluree-db-query/src/operator.rs:101) trait default `fn set_row_budget(&mut self, _budget: usize) {}` with doc "Default = ABSORB (no-op)" (:89). Overrides exist in 9 files / 12 impls: READ(offset.rs, join.rs, limit.rs ×2, graph.rs, dataset_operator.rs ×2, union.rs ×2, bind.rs, project.rs, r2rml/operator.rs). Every other operator silently absorbs the budget. The sibling's "~8 files" is right in the ballpark (precisely 9 override files).

(c) `can_contain_partition` ignores its spec/summary args and has no planner caller — CONFIRMED. READ(fluree-db-iceberg/src/scan/pruning.rs:32-66) signature args are `_summaries`, `_schema`, `_partition_spec_fields` (all underscore = UNUSED); the body only recurses on And/Or/Not and returns `true` for every column predicate (`_ => true` at :64), so it can never prune on partition data. Callers: only the two recursive self-calls (:54, :60) and the re-export (mod.rs:18) — the planner uses `can_contain_file` (planner.rs:238), never `can_contain_partition`. Doubly inert: the function is a structural no-op AND the Snowflake tables carry empty partition specs (Task 2c), so there is nothing to prune even if it worked.
