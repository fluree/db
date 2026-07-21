# C4 — Solo serverless ingestion / materialization pipelines

Scope: READ-ONLY map of fluree/solo (checkout <solo-checkout>, tree consistent with PR #788). Labels: VERIFIED(file:line) = code read this session; PR(url) = git/gh receipt; UNVERIFIED = plausible but unconfirmed; DON'T-KNOW = out of scope / not traced. All file paths absolute under <solo-checkout>. Every paragraph is one line for clean copy/paste.

## Executive answers

Q (heavy-work executor): In BOTH the materialize and the import pipelines the actual writing/indexing is done by the **transact Lambda** (`fluree-lambda-transact`) consuming a per-ledger SQS FIFO queue via an event-source mapping; the materialize Lambda is a pure coordinator and the router only enqueues. Indexing is a third, separately-invoked Lambda (`fluree-lambda-indexing`).

Q (Iceberg → native materialization): **No such path exists in solo today, and the architecture actively prevents it.** Virtual (Iceberg/R2RML query-in-place) and materialized (native-commit) datasets are disjoint by construction and every native-write entrypoint rejects a virtual target. There is no solo-side code that reads a registered Iceberg/R2RML graph source and writes native commits. Headline receipts below.

Q (bplatz authorship): **Partly confirmed, with an important nuance.** bplatz authored the `.flpack` restore + CLI negotiated multipart-upload LAYER (PR #626), and did NOT author the original "native import" foundation (loose ttl/jsonld streaming + begin-import wire) — that is PR #498. Ceiling-tuning and virtual-dataset write-guards are yet other authors.

## Pipeline A — materialize (semantic-transform + bulk-API → native ledger)

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-materialize/src/main.rs:1-10): `fluree-lambda-materialize` is a COORDINATOR, not a transactor — it reads model + instance artifacts from S3, transacts the model synchronously via the target ledger's SQS FIFO, batches instances fire-and-forget onto the same FIFO, polls TransactionStatusTable, and self-reinvokes near the 900s deadline with a ResumePayload.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-materialize/src/handler.rs:117-598): three phases — Phase 1 transact model (poll to completion), Phase 2 stream instances NDJSON + queue batches, Phase 3 poll batch completion; timeout handling self-reinvokes rather than a watcher. There is NO task-watcher on this path (the parent-audit assumption was wrong); `routes/turn_completion.rs` is unrelated (async chat-turn completion).

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-materialize/Cargo.toml): deps are fluree-db-api, fluree-graph-turtle, fluree-graph-json-ld, orchestrator-boundary-types — NO iceberg / r2rml / graph_source dep. A repo-wide grep of the crate for iceberg|r2rml|graph_source|Sourcing|virtual returns nothing. The graph crates are only NDJSON/Turtle chunk splitters; it calls no "materialize" engine API in fluree-db.

VERIFIED(<solo-checkout>/template.yaml:2344-2357): MaterializeLambda 2 GB / 900 s / 10 GB /tmp, provided.al2023, comment "Coordinator pattern — reads model/instance artifacts from S3 and queues transactions to SQS FIFO. No fluree-db-api in-process transacting."

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/routes/materializations.rs:243-278,287,574-580): TRIGGER 1 = POST /v1/document-repos/{repoId}/executions/{executionId}/materialize; verifies the execution is COMPLETED and workflowType=="semantic-transform", resolves model (schema_turtle preferred, model_jsonld fallback) + consolidated_instances URLs from the workflow's output-consolidator.jsonld DCAT state, then async-invokes (InvocationType::Event) the materialize Lambda. Status row RECORD_TYPE_MATERIALIZATION, pk=MATERIALIZATION#<id>, gsi1=EXEC#<execId>.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/routes/bulk.rs:1-403): TRIGGER 2 = the bulk-API. POST /v1/datasets/{id}/bulk/upload-url mints a presigned S3 PUT into the Documents bucket at bulk-uploads/{dataset}/{uploadId}/{filename}; POST /v1/datasets/{id}/bulk/materialize head_objects the file, builds a MaterializationEvent with data_url/data_format/data_action set (NOT model_url/instances_url), and async-invokes the SAME materialize Lambda. Formats accepted: ndjson, jsonl, ttl/turtle, jsonld/json-ld, json (BulkFormat::from_filename, bulk.rs:110-115). Status row is RECORD_TYPE_MATERIALIZATION but gsi2=DATASET#<id>, source="bulk-api", s3_key, format (bulk.rs:277-320). Size cap MAX_BULK_FILE_SIZE=5 GB, ENFORCED at materialize via head_object (bulk.rs:239).

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-materialize/src/event.rs:58-64): the materialize Lambda's single-file (bulk) branch requires data_url to start with "s3://" — it reads a plain S3 object, NOT any Iceberg/graph source. So the bulk-API `source:"bulk-api"` tag is provenance only; there is no Iceberg reader here.

## Pipeline B — negotiated import / .flpack restore (files → native ledger)

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-transact/src/import.rs:243-584): the EXECUTOR is the transact Lambda's `process_begin_import`, consuming one begin-import SQS message off the per-ledger FIFO and dispatching on files[0].format — flpack (streaming restore), ttl/jsonld (streaming import_from_storage), zip (/tmp extract + local import). There is NO separate restore Lambda.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-transact/src/import.rs:354-440): flpack path streams the archive straight from S3 into fluree-db-api `FlureeClient::restore_ledger(&ledger_id, &mut reader)` — decode frames, verify integrity, write objects, finalize heads from the embedded nameservice manifest, roll back on failure. NO /tmp spool, NO reindex (the prebuilt index rides along); bounded only by the 900 s runtime. Result = {commits, txn_blobs, index_artifacts, commit_t, index_t}. THIS is db-side .flpack restore, and it CONSUMES a prebuilt pack (commits+index) rather than replaying transactions.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/routes/datasets.rs:14-19): the pack is "fluree-pack-v1: content-addressed commits + prebuilt index artifacts + nameservice manifest — no reindex." Producer side is db-side (the `fluree` CLI export) and was NOT traced — DON'T-KNOW whether solo can emit a pack (I found no solo-side producer).

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/routes/import.rs:51-83): size framing — MAX_IMPORT_FILE_SIZE=5 GB (S3 single-PUT hard limit); MAX_FLPACK_IMPORT_SIZE=40 GB (CLI multipart); IMPORT_MULTIPART_THRESHOLD_BYTES=5 GB; part size 256 MiB, auto-doubled until ceil(size/part)<=S3_MAX_PARTS=10 000.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/routes/import.rs:112,344,727,999): two front doors — UI POST /v1/datasets/{id}/import (handle_begin_import), and CLI negotiated POST {api_base}/import-upload (mint, single presigned PUT or multipart plan) → POST .../import-upload/{importId}/complete (stitches multipart via CompleteMultipartUpload, enqueues begin-import WITH importId set). Both enqueue one begin-import onto the per-ledger FIFO fluree-{stack}-txn-{queueId}.fifo.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-transact/src/import.rs:278-336): bound-safety / resume — no chunk-level resume; recovery is SQS redelivery guarded by a ledger-state probe (ledger exists & t>0 ⇒ retry ONLY the Dataset.status flip, never re-run the restore). Loose ttl/jsonld stream from S3 (never fully downloaded); zip uses 10 GB /tmp; memory-aware via ImportBuilder memory_budget_mb + parallelism passed from the request.

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-common/src/aws/status_table.rs:222-259): CLI job lifecycle row RECORD_TYPE_LEDGER_IMPORT, pk=LEDGER_IMPORT#<importId> (mint→awaiting-upload, complete→running, transactor→succeeded|failed with result summary). UI path uses _system Dataset.status ("importing"→cleared/"import_failed"); while importing the router rejects user transactions, so the FIFO + status flag jointly gate writes.

## Indexing pipeline

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-common/src/aws/dynamodb.rs:709-763): `fluree-lambda-indexing` is invoked async (lambda.invoke_async) by the transact Lambda after commits when indexing_needed, gated by a single-worker-per-ledger DynamoDB slot claim (attempt_claim_indexing_slot). Also driven by admin routes (POST /v1/admin/reindex/{ledgerId} async, POST /v1/fluree/reindex sync) and POST /v1/datasets/{id}/reindex (bulk.rs:543), all with reindex:true. Imported ledgers: flpack carries its own index (no reindex); ttl/jsonld/zip build the index INLINE inside ImportBuilder::execute() — the import path does not call trigger_indexing_if_needed. VERIFIED(<solo-checkout>/template.yaml:2309-2340): IndexingLambda MemorySize=!Ref TransactionLambdaMemorySize, 10 GB /tmp, 900 s, 8 GB disk cache, provided.al2.

## Datasets as native ledgers — Sourcing

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/system_db/types.rs:226-248,332,424-432): enum Sourcing has variant Iceberg { table_ref: Option<String> /* tableRef */ }. Dataset.kind=="virtual" marks a query-in-place Iceberg dataset; iceberg_mapping_id (bound fsys:R2RMLMapping, authoritative, holds tableNames[]) present ⇒ derive_sourcing returns Sourcing::Iceberg; iceberg_table_ref is a representative catalog.schema.table.

VERIFIED (grep, MEASURED absence): no last_sync / lastSync / freshness field exists on Dataset or Sourcing — virtual Iceberg datasets are live query-in-place, so there is no sync timestamp to surface.

## Authorship receipts

PR(https://github.com/fluree/solo/pull/626): "feat: dedicated servers + desktop substrate, .flpack multipart restore, query memory watchdog" — author bplatz, merged 2026-06-17. `gh pr view 626 --json commits` CONFIRMS it carries all four flpack commits: 837226c76 "feat(import): support .flpack native ledger archive import", 2d0906a3a "feat(import): CLI negotiated presigned-upload .flpack restore", 0a1115a36 "feat(import): multipart .flpack restore + registry delete for failed imports", 53533a57f "fix(router): address PR review must-fix items + bump fluree-db v4.0.7". So the .flpack restore + CLI negotiated multipart + 40 GB staging are bplatz via #626.

PR(https://github.com/fluree/solo/pull/626) — Lambda-bounds design commentary (substantiates the "thoughtful of Lambda upper bounds" read): body states "Multipart presigned upload... so archives over S3's 5 GiB single-PUT cap upload out-of-band"; "Paired fluree-db bumps for the restore side: frame-size cap fix (oversized dictionary frames) + parallel-write restore, letting a 21 GB / 561 M-triple archive restore within the transactor window"; plus the query Lambda memory watchdog that returns a structured 507 at 85% RSS instead of an OOM-kill. These are explicit upper-bound accommodations.

PR(https://github.com/fluree/solo/pull/498) NUANCE — refutes "bplatz authored the whole negotiated-import path": the ORIGINAL native import (begin-import wire + transact import.rs foundation, loose ttl/jsonld streaming) is Jacob Parsell, commit aca56ae5a "feat: Apps framework, identity resolver, ConfigTable consolidation, native import + supporting infrastructure (#498)", 2026-05-05 — a month before bplatz's flpack layer. bplatz built .flpack restore ON TOP of Parsell's native-import base.

VERIFIED (git log, other authors on these files): Jonathan Dorety 7d36a778e "cap import/bulk upload-url at 5 GB single-PUT ceiling", 9a2699743 "correct import upload ceiling to 3 GB, add CLI redirect CTA", b64463cb3 "normalize bare .json to .jsonld"; Andrew Johnson e961057ea "reject writes to virtual datasets + owner-check existing bind". So ceiling-tuning and the Iceberg write-guards are not bplatz.

## HEADLINE — is there an Iceberg/R2RML-read → native-commit-write path?

VERIFIED — NO, and it is structurally prevented. (1) Every native-write entrypoint rejects a virtual (Iceberg) target via reject_write_to_virtual_dataset: bulk upload-url (bulk.rs:99), bulk materialize (bulk.rs:192), bulk reindex (bulk.rs:564), import begin (import.rs:364), semantic-transform materialize (materializations.rs:356); the intent comment is at types.rs:544 ("bulk / materialize reject writes to a virtual Dataset").

VERIFIED(<solo-checkout>/src/rust/lambda/fluree-lambda-router/src/routes/virtual_graphs.rs:3464-3482): virtual and materialized are DISJOINT by construction — binding an Iceberg mapping requires graphType!="virtual" is absent/empty AND ledger commit_t==0 (no committed native data); a materialized dataset "cannot [be converted] to a virtual Iceberg dataset" and vice versa. The "materialized" vs "virtual" graphType (virtual_graphs.rs:6691) is a CLASSIFICATION, not a conversion.

VERIFIED (grep, MEASURED absence): no snapshot_to_ledger / freeze_virtual / materialize_virtual / iceberg-to-ledger / construct→insert-native path anywhere in src/rust; virtual_graphs.rs has no .transact(/.insert(/write-commit call reading a graph source. The materialize/bulk read side is a plain s3:// object only.

ASSESSMENT — could the materialize coordinator BECOME the Iceberg→native hook? The coordinator is the natural insertion point (it already fans instances to the transactor over the FIFO), but the READ side does not exist on the solo side: MaterializationEvent carries a data_url (s3:// file), not a graph-source ref, and nothing queries an R2RML/Iceberg source to emit triples/NDJSON. Making it a hook is net-new capability (a graph-source-reading producer that emits NDJSON, or a db-side CONSTRUCT-over-iceberg → commit), NOT a config flip — and it would additionally require lifting the virtual/materialized disjointness guard. DON'T-KNOW whether a db-side (fluree-db-api) call can read an Iceberg graph source and write native commits — out of solo scope; db-side is owned by another agent.
