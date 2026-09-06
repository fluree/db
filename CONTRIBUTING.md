# Contributing

The full contributor guide — dev setup, code style, testing, benchmarks, the W3C SPARQL
compliance suite — lives in [`docs/contributing/`](docs/contributing/README.md).

This file covers the one convention GitHub surfaces at exactly the right moment: **how to link a
PR to an issue.** The full version, including stacked PRs and the reasoning, is at
[`docs/contributing/issue-linking.md`](docs/contributing/issue-linking.md).

## Linking issues from PRs

Reference direction has to be explicit, because most issue↔PR references in this repo are
deferrals rather than fixes:

- **`Fixes #N` / `Closes #N`, in the PR body** — this PR resolves the issue. The body, so the
  issue links back to the PR. (A commit-message keyword also closes on merge to the default
  branch, but creates no linked-PR relationship.) Closing keywords only fire when the PR's base
  is the default branch.
- **`Follow-up: #N`** — work this PR *defers* to an issue, including issues filed during its own
  review. Not a closing keyword, by design.
- **`Partially addresses #N`** — the issue stays open; say what remains. Write it this way:
  GitHub matches closing keywords anywhere in the body, so *"partially fixes #N"* would close it.

Landing a stack? A child folded into its parent takes its `Fixes #N` with it, silently — see
[the full page](docs/contributing/issue-linking.md#stacked-prs).

## Gates

CI is the source of truth; see `.github/workflows/ci.yml`. Worth knowing locally: CI runs **two**
fmt/clippy pairs — one over the workspace, and a second with `working-directory: testsuite-sparql`,
which is excluded from the workspace. A root `cargo fmt --all` does not reach it, so if you
touched `testsuite-sparql/`, format it there separately or CI reddens on a file you never opened.

## Triage labels

Open issues carry `P0`–`P3` (priority), `area:*` (subsystem), and `triage:*` (state:
`needs-decision`, `needs-info`, `blocked`). Keep them current when a PR changes an issue's scope
or priority. PR labels matter too — `.github/release.yml` derives release-note categories from
them, so an unlabeled PR lands under "Other Changes".
