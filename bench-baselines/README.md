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
- Each baseline records its `runner_class` (env `FLUREE_BENCH_RUNNER_CLASS`,
  default `local`) and `host`. `compare` enforces **memory** across any runner
  class but treats **wall-clock time** as advisory when the baseline's runner
  class differs from the comparing runner's (cross-machine time can't gate).
  Commit a CI-class baseline (`runner_class=ci-*`, via `bench.yml`'s
  `bench-capture` job) to make the per-PR time half enforce.

`guardrails-pre.json` is the PR-1 (guardrails net) reference — captured at the
merge-base engine, quick profile, tiny+small, `runner_class=local`.

Capture (the `--label`/`--out` name the phase reference; capture at the
merge-base commit):

```bash
cargo run -p fluree-bench-support --bin bench-baseline -- \
    capture --label phase-1-pre --out bench-baselines/phase-1-pre.json
```
