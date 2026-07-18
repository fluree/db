# Cypher Sequential Writes (Multi-Clause MERGE Composition)

Status: implemented (v1)
Owner: query/transact
Related: `docs/query/cypher.md`, `docs/reference/cypher-support-matrix.md`

## Problem

A Cypher write statement is a *pipeline*: each clause observes the writes of
the clauses before it, within one atomic statement. The canonical idiom:

```cypher
UNWIND $rows AS row
MERGE (a:Person {id: row.src})
MERGE (b:Person {id: row.dst})
MERGE (a)-[:KNOWS]->(b)
```

The second MERGE's find-or-create check must see nodes the first MERGE just
created; the relationship MERGE must see both. A single Fluree `Txn` evaluates
its whole WHERE (including every `NOT EXISTS` MERGE guard) against **one
snapshot horizon** — the pre-transaction state — so it cannot express "later
clause observes earlier clause." That is the real architectural constraint
behind the v1 restriction ("MERGE must be the only write clause"), and no
rewriting into a single Txn can lift it.

## Approach: sequential staging within one commit

Fluree already solves "N ordered operations, each observing the previous, one
atomic commit" for SPARQL 1.1 `;`-separated updates (roadmap D-10,
`Fluree::stage_transaction_from_txns`): each operation stages against a
*virtual state* (base + prior operations' flakes applied as a commit-record-less
overlay via `LedgerState::apply_staged_flakes_for_sequential_staging`), flakes
fold last-wins per fact identity re-stamped to the single final `t+1`, and one
commit publishes.

The Cypher driver builds on the same machinery, factored into a reusable
`SequentialStager` (`fluree-db-api/src/tx.rs`). What Cypher adds over SPARQL
`;` is **row threading**: clauses pipe *rows* (variable bindings), not just
graph state. The driver is therefore an execute-time loop, not a static
decomposition:

1. **Row table init** — the read prefix (`MATCH` / `UNWIND` / `WITH` /
   desugared `InlineRows`) is evaluated as an ordinary read query against the
   base state, projecting every variable later clauses or the `RETURN` need.
   Entity bindings materialize as IRIs, literals as typed values. Read-path
   evaluation gives the full read surface (including `WITH` aggregation)
   for free.
2. **Per-clause staging loop** — each write clause becomes one or two sub-`Txn`s
   seeded with the current row table (a `VALUES` join), staged against the
   current virtual state via the full staging pipeline (policy, SHACL,
   uniqueness — per-clause validation, the same semantics choice D-10 made):
   - **CREATE**: one Txn; created-entity columns extend the row table as
     skolem IRIs (`_:fdb-{clause_skolem}-{row}-{label}` — solution order equals
     `VALUES` row order, pinned by test).
   - **MERGE**: (a) probe matched rows (`VALUES ⋈ pattern` query on the virtual
     state); (b) absent rows dedup by their engine-evaluated identity-value
     tuple, first row per key creates (create-branch Txn with `ON CREATE SET`);
     (c) flakes apply; (d) re-bind **all** rows (`VALUES ⋈ pattern` on the
     updated state) — every row now matches, created or pre-existing, with
     Cypher's multi-match row multiplication; (e) rows that did not create run
     `ON MATCH SET` (a seeded SET Txn). In-batch duplicate keys thus converge
     to sequential semantics: first occurrence creates + `ON CREATE SET`,
     later occurrences match + `ON MATCH SET`.
   - **SET / REMOVE**: one seeded Txn; row table unchanged.
   - **DELETE** in a multi-clause chain: rejected (clear error) in v1 — its
     relationship-existence / parallel-edge probes stay single-statement.
3. **Finish** — `SequentialStager::finish()` produces one `StageResult` over
   the original base: single commit at `t+1`, union namespace delta, folded
   flakes. Zero staged flakes → the ordinary no-op update path.
4. **RETURN** — answered by one final read query (`VALUES(final rows) RETURN
   items`) against the final virtual state: full expression surface
   (projections, `DISTINCT`, `ORDER BY`, `SKIP`/`LIMIT`) rather than the
   created-entity-only reconstruction single-Txn writes use.

## Row threading representation

Rows re-enter lowering through `ReadClause::InlineRows` whose cells now admit
an internal-only `Expr::SeededValue(UnresolvedValue, span)` variant — never
produced by the parser, carrying entity IRIs (including stable `_:fdb-` blank
node labels) and typed literals losslessly into both the read-side lowering
(probe/re-bind/RETURN queries) and the write-side lowering (seeded Txns).
`InlineRows` vars now register as bound in the write lowering (`bound_vars`),
so seeded rows satisfy `SET`/`REMOVE` target checks.

## Dispatch

`cypher_write_plan` classification order:

1. `detect_conditional` — the existing probe/branch shapes (standalone
   `MERGE … ON MATCH SET`, per-row relationship MERGE pair-staging, DELETE
   guards) keep their proven paths.
2. `detect_sequential` (new) — multi-write-clause statements containing a
   MERGE (MERGE chains, CREATE+MERGE mixes, interleaved SET/REMOVE), and the
   previously-rejected node MERGE under a plain leading MATCH (the driver's
   per-row partition makes it correct: one create per distinct absent key,
   every row binds).
3. Single-Txn lowering — everything else, including its existing clear errors.

`WritePlan` gains a `Sequential` variant. Call sites:

- **Autocommit** (`transact_cypher_statement`) and **Bolt explicit
  transactions** (`cypher_transaction_write`) call the driver directly; the
  Bolt path stages against the transaction's private state, so statement
  pipelining and read-your-writes compose.
- **Consensus** (`LocalCommitter`): the plan resolves under the write lock per
  retry attempt (same head-check/refresh contract as conditional MERGE).
  `TransactionReceipt` carries an optional Cypher RETURN table
  (`serde` default `None`); Raft mode rejects multi-clause RETURN in v1.

The `UNWIND $batch` desugar's last-wins row dedup
(`dedup_rows_by_merge_identity`) now applies **only to single-write-clause
statements** — it keys on the first node MERGE and would drop legitimate rows
in a chain; the driver owns duplicate-key semantics for multi-clause
statements.

## Non-impact guarantees

- JSON-LD / Turtle / SPARQL transaction paths are untouched; `stage()` in
  fluree-db-transact is not modified.
- `stage_transaction_from_txns` keeps its `len == 1` fast path; only its
  N ≥ 2 loop body moved into `SequentialStager`.
- Single-clause Cypher writes keep their existing plans byte-for-byte
  (`detect_conditional` runs first; `detect_sequential` fires only on shapes
  that previously errored).

## Costs

Per MERGE clause: two or three `VALUES`-seeded engine queries (match probe,
identity-tuple evaluation for dedup, re-bind) plus one or two stagings. All
row-bulk operations — no per-row query loops. Row tables are capped
(`MAX_SEQUENTIAL_ROWS`) with a clear error.
