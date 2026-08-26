# Working in this repo

Repo-wide guidance for Claude Code sessions. Personal notes belong in `CLAUDE.local.md`
(gitignored), not here — this file is shared, so keep it to things that are true for everyone.

## Linking issues from PRs

Reference direction must be explicit. Most issue↔PR references here are deferrals, not fixes, and
today the two are indistinguishable — an audit of the open backlog found eight issues that were
already fixed but still open for exactly this reason.

- **`Fixes #N` / `Closes #N` — in the PR body** when the PR resolves the issue. The body
  specifically: that is what creates the linked-PR relationship. (A commit-message keyword also
  closes on merge to the default branch, but leaves the issue closed with no visible cause.)
- **`Follow-up: #N`** when the PR *defers* work to an issue, including issues filed out of its own
  review. Not a closing keyword, deliberately.
- **`Partially addresses #N`** when the issue stays open; say what remains. Write it exactly that
  way — GitHub matches closing keywords anywhere in the body, so "partially fixes #N" closes it.

Closing keywords only fire when the PR's base is the default branch. Folding a child PR into its
parent silently discards any `Fixes #N` in the child's body — move it to the parent.

Full detail: `docs/contributing/issue-linking.md`.

## Gates before pushing

CI is the source of truth (`.github/workflows/ci.yml`). Two things catch people out:

- CI runs **two** fmt/clippy pairs — one over the workspace, and a second with
  `working-directory: testsuite-sparql`, which is *excluded* from the workspace. A root
  `cargo fmt --all` never reaches it. If you touched `testsuite-sparql/`, format and lint there
  separately.
- Run `cargo fmt --all` **after** your last edit. A post-clippy touch-up is the usual way a PR
  arrives with a red fmt gate.

Prefer narrow targets (`cargo test -p <crate>`, `cargo clippy -p <crate> --all-targets --no-deps`)
over workspace-wide runs.

## Issue triage labels

Open issues carry `P0`–`P3` (priority), one `area:*` (subsystem: `query`, `transact`, `storage`,
`iceberg`, `sparql`, `datalog`, `server`, `cli`), and `triage:*` where applicable
(`needs-decision`, `needs-info`, `blocked`). Keep them current when a PR changes an issue's scope.
PR labels drive release-note categories via `.github/release.yml` — an unlabeled PR lands under
"Other Changes".

## Verification habits that this codebase specifically rewards

- **A check that never ran is not a pass.** When a test, suite, or gate is skipped, say so
  explicitly rather than letting its absence read as success. This applies to CI too: a PR can
  end up with *no* workflow run at all, which `gh pr checks` reports as "no checks reported" —
  indistinguishable from pending. Confirm the expected workflows actually created runs.
- **Prove a new regression test is non-vacuous.** Revert the fix, watch the test fail, restore it.
  Several tests here have passed against the bug they were meant to pin.
- **`FLUREE_DISABLE_QUERY_FAST_PATHS` is the usual differential oracle, but it is not infallible** —
  see issue #1700, where the generic pipeline is the *wrong* lane for `COUNT(*)` over a
  multi-object join. Confirm which lane is correct before "fixing" toward the oracle.
- **Fast-path suites use routing stamps** (`MustFire` / `MustNotFire`) so a test cannot pass by
  silently taking the generic lane. If you add a fast path, stamp it.
- SPARQL and JSON-LD share an IR: a fix on one surface generally needs a twin test on the other.
