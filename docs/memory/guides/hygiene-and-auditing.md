# Memory hygiene and auditing

A committed `repo.ttl` is a shared asset: every contributor's agent loads from it, and every
session can write to it. Left alone, it drifts the same way any shared document drifts —
toward effort-specific narration that was true for one branch, one PR, or one week. This
guide defines what a healthy repo-scoped memory looks like, and gives a repeatable rubric
for auditing a store that has accumulated mess.

It complements [What is a memory?](../concepts/what-is-a-memory.md) (the data model) and
[Repo vs user memory](../concepts/repo-vs-user.md) (the scope split). Those describe the
mechanism; this describes the editorial standard.

## The five tests

A repo-scoped memory should pass **all five**. Each test has automatic-fail markers that
make auditing mechanical rather than a matter of taste.

### T1 — Durable: true at HEAD, not time-bound

The claim must hold for the current default branch, phrased so it stays true until the
world actually changes. Memories are not a changelog and not a status board.

Automatic fails:

- Progress framing: "SHIPPED", "landed as PR #N", "merged 2026-07-12", "wave 2 complete".
  Git history already records what shipped and when.
- Pending-state snapshots: "awaiting review", "pending", "next step is", "WIP",
  "deferred until".
- Claims about a branch that never merged, or refs that only resolve on an effort branch.

The durable *residue* of a shipped effort is welcome — the invariant that made the fix
necessary, the gotcha that cost a day — phrased as a fact about the code, not as news
about the effort.

### T2 — Shared: any contributor is the audience

Repo scope means the whole team, indefinitely. Personal context belongs in
[user scope](../concepts/repo-vs-user.md).

Automatic fails:

- Named-person state: schedules, approvals-in-flight, "X confirmed", "waiting on Y".
- Absolute paths into someone's home directory, or refs to another checkout.
- Session hand-off notes between agent runs ("resume from", "see the earlier analysis").

### T3 — Non-derivable: beats grep, git log, and the docs

A memory earns its context-window slot by saving real effort. If an agent could answer
the same question with one grep, one `git log`, or by opening a doc page, the memory is
noise that dilutes recall ranking for everything else.

Automatic fails:

- Diff narration: "added X to Y", "renamed A to B" — that is `git log -p`.
- Restating what a file plainly shows, or duplicating a `docs/` page.

What *does* earn the slot: invariants that code cannot express locally, cross-crate
conventions, traps that produced real debugging cost, and decision rationale — especially
**why the obvious alternative was rejected**.

### T4 — Actionable: a reader does something differently

After reading, a contributor should write different code, run a different command, or
avoid a specific mistake. Post-mortem narration with no lesson fails; so does trivia.

### T5 — Well-formed: mechanically healthy

- One insight per memory; content within the 750-character cap.
- Every `ref` resolves at HEAD (repo-relative, never absolute).
- Tags are lowercase recall keys drawn from stable vocabulary (crate/module names,
  topics) — not sentence fragments, effort codenames, or PR numbers.
- Correct kind: a rule is a `constraint` with severity; a choice is a `decision` with
  rationale; everything else is a `fact`.

## Audit procedure

Repeatable on any repo with a committed memory file.

1. **Extract.** Parse the TTL into structured records (id, kind, content, tags, refs,
   createdAt, branch).
2. **Mechanical pre-pass.** Flag automatic fails: refs that don't resolve against the
   default branch, content over the cap, progress/pending/person markers (regex),
   absolute paths, memories captured on never-merged branches.
3. **Judgment pass.** Read every memory against the five tests and assign a disposition:

   | Disposition | Meaning |
   |---|---|
   | **KEEP** | Passes all five; at most tag/ref touch-ups. |
   | **REWRITE** | A durable insight exists but is wrapped in effort narration — extract the invariant, keep the ID (`update`). |
   | **MERGE** | Several memories cover one insight — consolidate into the best one, `forget` the rest. |
   | **DELETE** | No durable core (`forget`): status snapshots, diff narration, superseded claims. |
   | **RESCOPE** | Valid but personal — belongs in user scope, so remove from repo scope. |

4. **Verify.** For KEEP/REWRITE memories making nontrivial claims, spot-check the claim
   against HEAD — refactors silently invalidate old truths. Anything wrong is fixed or
   deleted, not left "mostly right".
5. **Gap pass.** Mess is bidirectional: audit what's missing, not just what's stale.
   For each workspace crate / major subsystem, ask what non-obvious knowledge a new
   contributor's agent would need; sweep recently merged PRs for durable residue
   (T1's terms) that nobody captured; check that known traps (test-harness quirks,
   CI gotchas, feature-flag interactions) are represented. New memories must pass the
   same five tests — a gap pass that adds derivable summaries makes the store worse.
6. **Rebuild and commit.** Write the reconciled file with the normal tooling (so
   formatting stays canonical), run the store's own health checks, and commit with an
   audit summary so the next audit can diff against a known-good baseline.

## Tooling

`memory_audit` (MCP) and [`fluree memory audit`](../cli/audit.md) (CLI) automate steps 1 and 2 — the
extract and the mechanical pre-pass — and add two signals a manual pass tends to miss: refs whose
files have commits newer than the memory's last write or re-verification, and files the current
branch changed that no memory covers (step 5's gap pass, from the other direction).

The audit is read-only and deliberately stops there. Every flag is a signal, not a verdict, and the
judgment pass — the five tests, and the KEEP/REWRITE/MERGE/DELETE/RESCOPE call that follows — stays
with the reviewer or agent. Acting on the findings means `update` (which also stamps a re-verification
when it changes nothing), `forget`, and `add`.

## Keeping it healthy

- **Write memories as residue, not narration.** At the end of an effort, ask "what did
  we learn that the code and docs can't show?" — and store only that.
- **Update in place** when the world changes; **forget** what was wrong from the start
  ([supersession](../concepts/supersession.md)).
- **Review memory diffs in PRs** like code: a `repo.ttl` hunk that adds progress
  framing or personal state should not merge.
- **Re-audit periodically.** A store that gains more than a handful of memories per
  week is accumulating narration; run the audit before it compounds.
