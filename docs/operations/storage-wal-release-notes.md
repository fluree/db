# Storage WAL and segmented Raft log

File storage now defaults to a write-ahead log on supported Unix filesystems.
Publishing a commit flushes the preceding source writes together. Windows and
filesystems without advisory-lock support use per-write flushing instead.

This release also fixes a pre-existing power-loss recovery defect: index
artifacts are flushed before publishing the pointer that names them. Previously,
a power failure could preserve an index pointer while losing its files, leaving
the ledger unable to load that index.

Failed logged writes and deletes cancel their specific records before returning
an error. Recovery no longer applies those failed operations over subsequent
writes. Retirement flushes each touched key once per batch. Read-only API clients
recover an existing WAL without keeping the writer's ownership lock.

## Upgrades and downgrades

- New WAL segments use the `FRDOSEG2` format with an xxh64 checksum over the
  payload followed by the frame header. The reader still accepts `FRDOSEG1`
  SHA-256 segments from the preceding cancellation-based implementation.
  Intermediate builds that encoded `DeleteIf` as opcode 5 are not compatible
  with cancellation records; recover and cleanly stop those builds before
  upgrading.
- On a shared payload root, upgrade WAL readers and writers together. A binary
  that only understands `FRDOSEG1` must not run alongside a writer of
  `FRDOSEG2`, since nodes can recover each other's owner logs. Checkpoint and
  stop the older owners before starting the new binaries.
- Raft's committed watermark uses two slots on separate 4 KiB boundaries.
  Existing adjacent 64-byte slots are migrated using a flushed temporary file,
  atomic rename, and directory flush. This protects the previous slot from a
  sector-local tear of the next update, not arbitrary device corruption.
- Before downgrading file storage, recover with the current binary, stop it
  cleanly, and confirm its WAL segments have been retired. Older WAL readers
  cannot read the new checksum format. Skipping recovery can let a later WAL
  replay overwrite or delete changes made by the older binary.
- The Raft watermark migration is forward-only. A binary that only understands
  64-byte slots must not open the migrated Raft storage directory. Retiring the
  payload WAL does not reverse that separate Raft format migration.

If a filesystem refuses an owner lock, empty owner directories left by fallback
are skipped with a warning. Retained segments still require recovery with
working advisory locks; they may contain the only durable copy of acknowledged
payloads.
