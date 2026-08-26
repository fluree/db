# Contributing

The full contributor guide — dev setup, code style, testing, PR format — lives in
[`docs/contributing/README.md`](docs/contributing/README.md). This file covers one thing that
GitHub surfaces at exactly the right moment: how to link a PR to an issue.

## Linking issues from PRs

The issue tracker only stays truthful if PR↔issue references carry direction. Three forms, used deliberately:

- **`Fixes #N` / `Closes #N` — in the PR body** — when the PR resolves the issue. Put it in the body rather than only a comment: a keyword in the body creates the *linked pull request* relationship, so the issue shows what fixed it and the PR shows what it closes. (A closing keyword in a **commit message** also closes the issue when the commit reaches the default branch, but it does not create that linked-PR relationship — so prefer the body, and keep the commit trailer if you like both.) Either way, closing keywords only fire when the PR's base is the default branch.
- **`Follow-up: #N`** — when the PR *defers* work to an issue, including issues filed during the PR's own review. This marks the reference as a deferral so it can't be mistaken for a fix. Deliberately not a closing keyword.
- **`Partially addresses #N`** — leaves the issue open by design. Say in the PR body what remains, and rescope the issue after merge so the open remainder is what the issue actually describes. Write it exactly this way — *"partially fixes #N"* still contains a closing keyword and will close the issue on merge.

### Stacked PRs

Closing keywords fire only against the default branch, so a PR based on another PR won't close anything while it sits on that base. Two ways this repo lands stacks, with different consequences:

- **Retarget to `main` before merging** (what we usually do) — the keyword fires normally. Nothing to do.
- **Fold a child into its parent** — the child's PR body disappears with it, and any `Fixes #N` it carried is silently lost. Move those keywords into the surviving parent's body, or close the issues by hand with a citation.

Background: a 2026-08 audit of the full open backlog found eight issues that were already fixed but still open, and the cause was the same each time — the fixing PR referenced the issue without a closing keyword, or the reference direction (fix vs deferral) was impossible to tell without reading each mention by hand.

## Gates

CI is the source of truth; see `.github/workflows/ci.yml`. One thing worth knowing locally: `ci.yml` runs **two** fmt/clippy pairs — one over the workspace, and a second with `working-directory: testsuite-sparql`, which is excluded from the workspace. A root `cargo fmt --all` does not reach it, so if you touched `testsuite-sparql/`, format it separately or CI will redden on a file you never opened.

## Triage labels

Open issues carry `P0`–`P3` (priority), `area:*` (subsystem), and `triage:*` (state: `needs-decision`, `needs-info`, `blocked`). Keep them current when a PR changes an issue's scope or priority. PR labels matter too — `.github/release.yml` derives release-note categories from them, so an unlabeled PR lands under "Other Changes".
