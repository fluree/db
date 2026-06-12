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

Capture:

```bash
cargo run -p fluree-bench-support --bin bench-baseline -- \
    capture --label phase-1-pre --out bench-baselines/phase-1-pre.json
```
