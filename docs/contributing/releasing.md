# Releasing

Fluree DB uses three tools working together:

| Tool          | Job                                                                                          |
|---------------|----------------------------------------------------------------------------------------------|
| `git-cliff`   | Generates `CHANGELOG.md` from conventional-commit subjects. Config: `cliff.toml`.            |
| `cargo-release` | Bumps the workspace version, runs git-cliff, creates the release commit. Config: `release.toml`. |
| `cargo-dist`    | Picks up the pushed `vX.Y.Z` tag, builds binaries, publishes the GitHub Release, Homebrew formula, and Docker images. Config: `dist-workspace.toml`, `.github/workflows/release.yml`. |

Releases are cut via a **two-phase pull-request flow**. Phase 1 prepares the release on a branch and opens a PR. Phase 2 — after the PR is reviewed and merged — tags the merge commit, which is what actually triggers cargo-dist.

This split exists deliberately: cargo-dist's release workflow triggers on **any** pushed `vX.Y.Z` tag, regardless of branch. By keeping the tag step manual and post-merge, we ensure no release ships without a reviewed PR.

## One-time setup

Install the two tools (cargo-dist runs in CI, not locally):

```bash
cargo install cargo-release git-cliff
```

## Phase 1 — Open the release PR

From a clean `main` branch:

```bash
# 1. Cut a release branch.
git checkout main && git pull
git checkout -b release/v4.0.2

# 2. Preview the release. cargo-release is dry-run by default.
cargo release patch

# 3. If the diff looks right, run for real with --execute.
cargo release patch --execute

# 4. Push the release branch and open a PR.
git push -u origin release/v4.0.2
gh pr create --title "release v4.0.2" --body "Bump workspace to 4.0.2 and regenerate CHANGELOG.md."
```

`patch` can be replaced with `minor`, `major`, or an explicit version like `4.0.2`.

What `cargo release patch --execute` does, on the release branch:

1. Confirms the working tree is clean and you're on a `release/*` branch (enforced by `release.toml`).
2. Runs the pre-release hook: `git cliff --tag vX.Y.Z --output CHANGELOG.md` (regenerates the changelog at the workspace root, including the new version).
3. Bumps `[workspace.package].version` in the root `Cargo.toml`. Every member crate inherits it via `version.workspace = true`.
4. Creates a single commit (`release vX.Y.Z`) with the version bump and the regenerated `CHANGELOG.md`.

It does **not** create a tag and does **not** push (both disabled in `release.toml`). That's intentional — the tag belongs on the merge commit on `main`, not on the release branch.

## Phase 2 — Tag and ship after merge

Once the release PR is approved and merged to `main`:

```bash
git checkout main && git pull
git tag v4.0.2
git push origin v4.0.2
```

Pushing the tag triggers `.github/workflows/release.yml` (cargo-dist), which:

- Builds all platform artifacts (Linux x64/arm64, macOS arm64, Windows x64).
- Creates the GitHub Release with the matching `CHANGELOG.md` section as the body.
- Publishes the Homebrew formula to `fluree/homebrew-tap`.
- Builds and pushes the multi-arch Docker image to `fluree/server`.

Watch the Actions tab. There's nothing else to do.

## Writing PR titles for clean changelogs

`git-cliff` reads commit subjects on the way to a tag and groups them by conventional-commit prefix. The branch-naming and PR-title convention from `CLAUDE.md` (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, plus `perf:`, `build:`, `ci:`) maps directly to changelog sections:

| Prefix       | Changelog section |
|--------------|-------------------|
| `feat:`      | Features          |
| `fix:`       | Bug fixes         |
| `perf:`      | Performance       |
| `refactor:`  | Refactoring       |
| `docs:`      | Documentation     |
| `test:`      | Tests             |
| `build:`     | Build             |
| `ci:`        | CI                |
| `chore:`     | Chore             |
| (other)      | Other             |
| `style:`, merge commits, `release vX`, `fmt` | skipped |

A `!` after the type marks a breaking change (`feat!: drop X`). Scopes render as bold prefixes (`fix(query): ...`).

If you squash-merge PRs, the PR title becomes the commit subject and ends up in the changelog automatically. If you merge-commit instead, the individual commit subjects are what get parsed.

## Bootstrapping the changelog

The committed `CHANGELOG.md` starts as a stub. To regenerate it from full git history (covering every existing tag back to the start of the repo), run once:

```bash
git cliff --output CHANGELOG.md
git add CHANGELOG.md
git commit -m "docs: bootstrap CHANGELOG.md from git history"
```

This only needs to be done once. Going forward, `cargo release` keeps the file current.

## Rolling back

**Before pushing the release branch (Phase 1):**

```bash
git reset --hard HEAD~1   # drop the release commit
git checkout main
git branch -D release/vX.Y.Z
```

**After the release PR is opened but before merge:**

Just close the PR and delete the branch on GitHub. Nothing has shipped.

**After Phase 2 — tag pushed but cargo-dist still running:**

```bash
git push origin :refs/tags/vX.Y.Z   # delete the remote tag
git tag -d vX.Y.Z                   # delete the local tag
```

Then cancel the in-progress `Release` workflow run from the Actions tab. cargo-dist won't have created the GitHub Release yet unless the workflow's `host` job has run.

**After cargo-dist created the GitHub Release:**

Delete the GitHub Release from the UI, then delete the tag (commands above). The merge commit on `main` stays in place — you can re-tag it once the underlying issue is fixed, or supersede it with another release PR.

## Configuration files

- `cliff.toml` — git-cliff parsing rules and output template.
- `release.toml` — cargo-release behavior: shared workspace version, hook, `tag = false`, `push = false`, `allow-branch = ["release/*"]`.
- `dist-workspace.toml` — cargo-dist's distribution targets and installers.
- `.github/workflows/release.yml` — autogenerated by cargo-dist; regenerated with `dist init`.

If you change `cliff.toml` or `release.toml`, validate with `cargo release patch` (dry-run is the default) on a throwaway `release/*` branch before relying on it.
