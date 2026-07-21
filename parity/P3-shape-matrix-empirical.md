# P3 — Empirical query-shape matrix (virtual R2RML/Iceberg vs native twin)

**Role:** empirical tester (the only agent running live queries).
**Engine rev:** `733cb2c8e` (worktree `wt-parity`, release build, `fluree-db-api[iceberg]`).
**Live source:** `enterprise-sf01-v:main` (Snowflake-managed Iceberg, `ENTERPRISE_DEMO.DW_SF01`, 16-table star). Native twin: `enterprise-sf01:main` (35,238,778-triple materialized ledger, byte-identical source parquet — result hashes directly comparable).
**Surface:** JSON-LD/FQL bodies through `Fluree::query_from().jsonld()` (auto `with_r2rml()`) — the deployed DatasetOperator path. SPARQL bonus via `vbench exec-one`.
**Schema note:** vocab base is `http://ns.fluree.dev/edw#` (not the handoff's `example.org/enterprise-byo`); Customer subjects are `http://data.fluree.dev/edw/customer/{CUSTOMER_KEY}`; `edw:Customer` carries two FK RefObjectMaps (`edw:geography`, `edw:account`) — the Part-1 defect target. Safety: PAT in-memory only, single-flight, ~2s pacing, virtual-sf01 only.

---

## Result-SHAPE anchor (the core parity gap), Customer instance, both surfaces

Virtual (`format:jsonld`, and byte-identically `format:typed-json`):
```json
"http://ns.fluree.dev/edw#geography": "http://data.fluree.dev/edw/geography/21801",
"http://ns.fluree.dev/edw#account":   "http://data.fluree.dev/edw/account/1624",
"http://ns.fluree.dev/edw#isCurrent": "true"
```
Native (parity target):
```json
"http://ns.fluree.dev/edw#geography": { "@id": "http://data.fluree.dev/edw/geography/21801" },
"http://ns.fluree.dev/edw#account":   { "@id": "http://data.fluree.dev/edw/account/1624" },
"http://ns.fluree.dev/edw#isCurrent": true
```
FK refs come back as **bare strings** on virtual, `{"@id":…}` on native (Part-1a — CONFIRMED LIVE). Booleans are a **second, unreported** manifestation: `"true"` (string) virtual vs `true` (JSON boolean) native. Dates are `{"@value","@type"}` on both.

**`format:typed-json` is silently ignored (Part-1b — CONFIRMED byte-identically):** the same Shape-2 body run with `format:typed-json --normalize` and `format:jsonld --normalize` produced **byte-identical** output. The crawl reads only `normalize_arrays`; the format axis is dead. Real typed-json would have wrapped FK as `{"@id"}` and every literal as `{"@value","@type"}` (incl. the integer/boolean); it did not.

---

## The matrix

Telemetry key: `scan_n` = `r2rml.scan_table` count; `cm_n` = `r2rml.count_manifest` count. Walls are single-process (each run re-mints OAuth + reloads catalog, so cold ~2–4 s catalog floor is included; "warm" = second run in-process).

| # (handoff) | Shape (adapted) | Surface | status | rows | wall | telemetry | native twin | Verdict |
|---|---|---|---|---|---|---|---|---|
| 2 | class page `{select:{?s:[*]},where:{@id:?s,@type:Customer},limit:20}` | JSON-LD | ok | 20 | 3.3s cold / 276ms warm | scan_n=1, pruned=0 | ok, FK=`{"@id"}`, bool=`true` | **WORKS-BUT-WRONG-SHAPE** (FK bare string; bool string; typed-json ignored) |
| 3 | count `(count ?s)` Customer | JSON-LD | ok | 1 (=**390000**) | 2.8s cold | **cm_n=1, scan_n=0** | (=390000) | **WORKS** (manifest shortcut) |
| 4 | filtered page `where:[{segment:"Consumer"},{@type:Customer}],limit:20` | JSON-LD | ok | 20 | 4.3s | scan_n=1 | — | **WORKS** (page); filter correct (see below) |
| 4 | filtered **count** gender="F" (discriminator) | JSON-LD | ok | **129913** | **30.9s** | scan_n=2 | **129913** @ 0.3s | **WORKS (correct) but SLOW** — filter applies, but 100× native |
| 5 | subject detail `{select:{"<customer/1>":[*]}}` | JSON-LD | ok | 1 | **1ms** | scan_n=0 | **full node**, 107ms | **SILENT-EMPTY** → `[{"@id":"…/customer/1"}]` stub (Part-2b) |
| 5 | VALUES workaround (their ask) | JSON-LD | ok | **390000** | 6.7s / **415 MB** | scan_n=1 | — | **DENY** — returns the WHOLE table; VALUES-bound subject ignored |
| 5 | **where-clause** bound-subject `{select:[?p,?o],where:{@id:"<customer/1>",?p:?o}}` | JSON-LD | ok | **14** | 2.6s | pruned | — | **WORKS** — the real workaround |
| 6 | property-scoped `{select:{?s:[*]},where:{@id:?s,segment:?v},limit:10}` | JSON-LD | **dnf** | — | >90s (killed) | full crawl | — | **SLOW-UNPRUNED** — limit not pushed for property-var (cf. Shape 2 = 276ms) |
| 7 | `@type` lookup `{select:{"<customer/1>":["@type"]}}` | JSON-LD | ok | 1 | 5ms | scan_n=0 | `[{"@type":"…Customer"}]` | **SILENT-EMPTY** → `[{}]` (worse than degraded) |
| 8 | inbound, **variable predicate** `{selectDistinct:?property,where:[{@id:?s,?property:{@id:"<customer/1>"}}]}` | JSON-LD | **error** | — | 2ms | plan-time | (native ok) | **REFUSED (400)** — envelope below |
| 9 | inbound, **concrete predicate** `(count ?s) where ?s edw:customer {@id:"<customer/1>"}` | JSON-LD | **ok** | 1 (=**1**) | 6.0s | scan_n=5 | **count=1** | **WORKS** — CONTRADICTS handoff (not blocked by 2a) |
| 10 | inbound, concrete predicate `select ?s … limit 5` | JSON-LD | **ok** | 1 | 1.5s | scan_n=5 | — | **WORKS** — returns `…/event/842822`; CONTRADICTS handoff |
| 11 | txn-meta graph `from urn:fluree:enterprise-sf01-v#txn-meta` | JSON-LD | ok | 0 | 22ms | — | — | **CLEAN-EMPTY** `[]` |
| 11 | commit-type `where {@id:?c,@type:ledger#Commit}` | JSON-LD | **dnf** | — | >30s (killed) | **scan_n=16** | — | **SLOW-RUNAWAY** — unmapped @type scans ALL 16 tables (no short-circuit) |
| — | SPARQL bound-subject `<store/1> ?p ?o` (q002) | SPARQL | ok | **8** | 3.1s | scan_n=3 | **8** @ 0.2s | **WORKS** (prunes to subject's tables), matches native |

---

## The exact refusal envelope (Shape 8 / Part-2a) — machine-detectability answer

Display (`{e}`):
```
Query error: Invalid query: R2RML graph source 'enterprise-sf01-v:main' contains 1 pattern(s) that cannot be converted to R2RML scans. Patterns with bound subjects (e.g., <iri> ex:name ?o) or bound objects (e.g., ?s ex:name "value") are not yet supported in R2RML graph sources.
```
Debug (`{e:?}` — the typed variant):
```
Query(InvalidQuery("R2RML graph source 'enterprise-sf01-v:main' contains 1 pattern(s) that cannot be converted to R2RML scans. Patterns with bound subjects (e.g., <iri> ex:name ?o) or bound objects (e.g., ?s ex:name \"value\") are not yet supported in R2RML graph sources."))
```

**No machine-detectable code exists.** The variant is the generic `fluree_db_query::QueryError::InvalidQuery(String)` (wrapped as `ApiError::Query(_)`), which maps to HTTP **400** via the catch-all `ApiError::Query(_) => 400` arm (`error.rs`), identical to every other malformed query. `ApiError` exposes no `error_code()` (only `BuilderError` does), and `InvalidQuery` carries only prose — it is the same variant used for dozens of unrelated errors, so gating on the discriminant is too broad. Solo's options: (a) prose-match a stable substring (`"cannot be converted to R2RML scans"` or `"not yet supported in R2RML graph sources"`), or (b) request a dedicated typed variant / `err:` code from db. Source: `fluree-db-query/src/graph.rs:302-309`.

---

## Findings that CONTRADICT or refine the handoff

1. **Inbound edges are NOT uniformly blocked (Shapes 9 & 10 work).** The 400 fires on the **variable-predicate** discovery shape (Shape 8) only. Concrete-predicate inbound edges — `?s edw:customer {@id:<iri>}` — execute correctly: count matched native exactly (1 = 1), select returned the real referencing subject. Solo can keep Referenced-By **counts and previews for known predicates** (it has the schema); only the "list every property pointing here" affordance (Shape 8) must be gated. The handoff lumps 9/10 under Part-2a; empirically they run.

2. **The VALUES workaround (their explicit confirm/deny) is DENIED, and there is a different one that WORKS.** VALUES-bound subject returns the **entire 390k-row table** (415 MB, 6.7 s) — the binding is ignored, not pushed down. But a **where-clause bound-subject** (`{"select":["?p","?o"],"where":{"@id":"<iri>","?p":"?o"}}`) returns the subject's triples (14 rows, pruned, 2.6 s), and SPARQL `SELECT ?p ?o WHERE { <iri> ?p ?o }` works and matches native (8=8). So the Part-2b silent-empty is specific to the **select-map IRI-key crawl form** — bound subjects otherwise resolve. Solo should rewrite subject-detail into the where-clause (or DESCRIBE) form and pivot (p,o) client-side, not the VALUES form.

3. **Two silent-empty shapes, one root cause.** Both Shape 5 (`{"<iri>":["*"]}` → `[{"@id":…}]`) and Shape 7 (`{"<iri>":["@type"]}` → `[{}]`) fail via the same select-map-IRI-key path (both 1–5 ms, no scan). Native resolves both.

4. **Bound-literal-object filters DO push down correctly** (filtered count 129913 = native), so Shape 4 filtering is correct — but the un-limited filtered count is ~31 s (100× native); the limited page is ~4 s.

5. **Property-scoped (Shape 6) and commit-type (Shape 11b) are runaways.** Limit is pushed for a `@type` class scan (Shape 2, 276 ms) but NOT for a property-var scan (Shape 6, DNF >90 s). An unmapped `@type` (commit-type) scans all 16 tables instead of short-circuiting to empty — the real hazard behind the "#txn-meta → timeout" worry (it's a 16-table runaway, not a 500; only the literal txn-meta-graph form returns clean `[]`).

6. **A third literal-typing divergence beyond FK refs:** booleans render as `"true"` (string) on virtual vs `true` on native — same flat-select-formatter root cause as Part-1a/b, another reason the node-document serializer (not just the FK special-case) needs the typed/hydration path.
