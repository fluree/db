# Linking issues from PRs

The issue tracker only stays truthful if PR↔issue references carry direction. Most references in
this repo are *deferrals* — an issue filed out of a PR's own review — and today those are
indistinguishable from references that mean "this PR fixed it." Three forms, used deliberately:

## `Fixes #N` / `Closes #N` — the PR resolves the issue

Put it in the **PR body**. A closing keyword in the body creates the *linked pull request*
relationship, so the issue shows what fixed it and the PR shows what it closes.

A closing keyword in a **commit message** also closes the issue once that commit reaches the
default branch — but it does not create the linked-PR relationship, so the issue ends up closed
with no visible cause. Prefer the body; keep a commit trailer as well if you like both.

Either way, closing keywords only fire when the PR's base is the default branch.

## `Follow-up: #N` — the PR defers work to an issue

Use this whenever a PR spins work out into an issue, including issues filed during the PR's own
review. It marks the reference as a deferral so it cannot be mistaken for a fix. Deliberately not
a closing keyword.

## `Partially addresses #N` — the issue stays open by design

Say in the PR body what remains, and rescope the issue after merge so the open remainder is what
the issue actually describes.

Write it exactly this way. GitHub matches closing keywords **anywhere in the body**, not just at
the start of a line, so *"partially fixes #N"* still contains `fixes #N` and will close the issue
on merge.

## Stacked PRs

Closing keywords fire only against the default branch, so a PR based on another PR closes nothing
while it sits on that base. Two ways this repo lands stacks, with different consequences:

- **Retarget to `main` before merging** — what we usually do, and the keyword then fires normally.
  Nothing to remember.
- **Fold a child into its parent** — the child's PR body disappears with it, and any `Fixes #N` it
  carried is silently lost. Move those keywords into the surviving parent's body, or close the
  issues by hand with a citation to the commit that did the work.

## Why

A 2026-08 audit of the full open backlog found eight issues that were already fixed but still
open. The cause was the same every time: the fixing PR referenced the issue without a closing
keyword, or the reference direction — fix versus deferral — was impossible to tell without
reading each mention by hand. Of fifteen PRs merged in one sample window, one used a closing
keyword.
