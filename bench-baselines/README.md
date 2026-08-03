# bench-baselines

Committed performance reference points for the phase gate described in
[`BENCHMARKING.md`](../BENCHMARKING.md) ("Baselines: capture & compare")
and [`docs/audit/2026-06-architecture-audit.md`](../docs/audit/2026-06-architecture-audit.md)
(Phase 0.0).

Conventions:

- `phase-<N>-pre.json` — captured on the commit a refactor phase branches
  from, committed with the phase's first PR. Every PR in the phase runs
  `bench-baseline compare` against it.
- `phase-<N>-post.json` — captured at phase close on the same hardware
  class and env knobs (`FLUREE_BENCH_PROFILE`, `FLUREE_BENCH_SCALE`) as
  the pre-baseline. All tuples must be within budget vs pre;
  improvements are banked by tightening `regression-budget.json` in the
  closing PR.
- Ad-hoc local baselines (validation runs, experiments) should live
  outside the repo or be cleaned up before merge — only phase reference
  points belong here.
- Each baseline records a `host` block (`class`, name, os, arch, cpu model,
  cores). `host.class` names a *comparability set*, from
  `FLUREE_BENCH_HOST_CLASS` or the derived `{os}-{arch}` default. `compare`
  **refuses** (exit 2) when the baseline's class differs from the comparing
  host's, since neither wall-clock time nor a tracking allocator's peak survives
  a machine change; `--allow-host-mismatch` downgrades those to non-gating
  annotations. Per-phase **share** drift gates either way — it is a ratio within
  one run, so the machine cancels.
- A baseline that also carries a corpus block (sha256, byte length, element
  count, input/output syntax, thread count) makes `compare` refuse outright on a
  corpus change. There is no override: comparing different inputs is never
  meaningful, and the result would look exactly like a real regression.
- Baselines built with `capture --accumulate` over ≥ 5 runs carry a median + MAD
  per scenario, and the gate widens each budget to `3 × MAD / median` when that
  exceeds the declared budget. Prefer this for any baseline meant to gate on
  shared CI runners.

`guardrails-pre.json` is the PR-1 (guardrails net) reference — captured at the
merge-base engine, quick profile, tiny+small, schema 1. It predates the host
block, so its `runner_class: "local"` is read as `host.class = "local"`: an
honest "some developer machine, unspecified". That is why the per-PR compare
still runs with `--allow-host-mismatch`.

Capture (the `--label`/`--out` name the phase reference; capture at the
merge-base commit):

```bash
cargo run -p fluree-bench-support --bin bench-baseline -- \
    capture --label phase-1-pre --out bench-baselines/phase-1-pre.json
```
