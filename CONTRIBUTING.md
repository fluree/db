# Contributing

## Linking issues from PRs

The issue tracker only stays truthful if PR↔issue references carry direction. Three forms, used deliberately:

- **`Fixes #N` / `Closes #N` — in the PR body** — required when the PR resolves the issue. The body specifically, not a comment or a commit message: GitHub only auto-closes the issue on merge when the keyword is in the body (and only when the PR's base is the default branch).
- **`Follow-up: #N`** — required when the PR *defers* work to an issue, including issues filed during the PR's own review. This marks the reference as a deferral, so it can't be mistaken for a fix.
- **`Partially addresses #N`** — leaves the issue open by design. Say in the PR body what remains, and rescope the issue after merge so the open remainder is what the issue actually describes.

Background: a 2026-08 audit of the full open backlog found the dominant failure mode was exactly this ambiguity — merged PRs referencing issues without closing keywords left fixed issues open, and deferral references were indistinguishable from fix references without reading every mention's context.

## Gates

CI is the source of truth: `cargo fmt --all --check`, clippy, and the test suites under `.github/workflows/`. One local habit worth keeping: run `cargo fmt --all` *after* your final edit — a post-clippy touch-up can re-redden fmt.

## Triage labels

Open issues carry `P0`–`P3` (priority), `area:*` (subsystem), and `triage:*` (state: `needs-decision`, `needs-info`, `blocked`). Keep them current when a PR changes an issue's scope or priority.
