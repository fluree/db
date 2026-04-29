# Releasing

Fluree DB uses three pieces working together:

| Tool          | Job                                                                                          |
|---------------|----------------------------------------------------------------------------------------------|
| `git-cliff`   | Generates `CHANGELOG.md` from conventional-commit subjects. Config: `cliff.toml`.            |
| `cargo-release` | Bumps the workspace version, runs git-cliff, commits, tags, pushes. Config: `release.toml`. |
| `cargo-dist`    | Picks up the pushed `vX.Y.Z` tag, builds binaries for all targets, publishes the GitHub Release, Homebrew formula, and Docker images. Config: `dist-workspace.toml`, `.github/workflows/release.yml`. |

A normal release is one command. Everything below is supporting detail for first-time setup, edge cases, and rollback.

## One-time setup

Install the two tools (cargo-dist runs in CI, not locally):

```bash
cargo install cargo-release
cargo install git-cliff
```

## Cutting a release

From a clean `main` branch:

```bash
# 1. Always preview first. cargo-release is dry-run by default —
#    it prints what it would do without modifying anything.
cargo release patch

# 2. If the diff looks right, run for real with --execute.
cargo release patch --execute
```

`patch` can be replaced with `minor`, `major`, or an explicit version like `4.0.2`.

What happens, in order:

1. `cargo-release` confirms the working tree is clean and you're on `main`.
2. The pre-release hook runs `git cliff --tag vX.Y.Z --output CHANGELOG.md`, regenerating the changelog up to and including the new version.
3. `[workspace.package].version` is bumped in the root `Cargo.toml`. Every member crate inherits it via `version.workspace = true`.
4. A single commit (`release vX.Y.Z`) is created with the version bump and the regenerated `CHANGELOG.md`.
5. The commit is tagged `vX.Y.Z` and both are pushed to `origin`.
6. The pushed tag triggers `.github/workflows/release.yml` (cargo-dist), which builds all platform artifacts, creates the GitHub Release with the matching `CHANGELOG.md` section as the body, publishes the Homebrew formula, and builds + pushes the multi-arch Docker image.

You don't need to do anything between steps 5 and 6 — just watch the Actions tab.

## Writing PR titles for clean changelogs

`git-cliff` reads commit subjects and groups them by their conventional-commit prefix. The branch-naming and PR-title convention from `CLAUDE.md` (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, plus `perf:`, `build:`, `ci:`) maps directly to changelog sections:

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

A `!` after the type marks a breaking change (`feat!: drop X`). Scopes are supported and rendered as bold prefixes (`fix(query): ...`).

If you squash-merge PRs, the PR title becomes the commit subject and ends up in the changelog automatically. If you merge-commit instead, the individual commit subjects are what get parsed.

## Bootstrapping the changelog

The committed `CHANGELOG.md` starts as a stub. To regenerate it from full git history (covering every existing tag back to the start of the repo), run once:

```bash
git cliff --output CHANGELOG.md
git add CHANGELOG.md
git commit -m "docs: bootstrap CHANGELOG.md from git history"
```

This only needs to be done once. Going forward, `cargo release` keeps the file current.

## Dry-run output

`cargo release patch` (without `--execute`) prints exactly what would change without touching anything — dry-run is the default. Read it before running with `--execute` — in particular, confirm:

- The version bump is what you expect.
- `CHANGELOG.md` shows the right entries grouped under the right headings.
- The tag name is `v<new-version>`.
- It will push to `origin/main`.

## Rolling back

If something looks wrong **before** pushing (e.g., you ran without `--dry-run` and saw the result):

```bash
git reset --hard HEAD~1     # drop the release commit
git tag -d vX.Y.Z           # drop the local tag
```

If something looks wrong **after** the tag has pushed but before cargo-dist finishes:

```bash
git push origin :refs/tags/vX.Y.Z   # delete the remote tag
```

Then cancel the in-progress `Release` workflow run from the Actions tab. cargo-dist won't have created the GitHub Release yet unless the workflow's `host` job has run.

If the GitHub Release was already created, delete it from the GitHub UI, then delete the tag. The next `cargo release` will pick up where you left off.

## Configuration files

- `cliff.toml` — git-cliff parsing rules and output template.
- `release.toml` — cargo-release behavior for this workspace (shared version, tag format, hook).
- `dist-workspace.toml` — cargo-dist's distribution targets and installers.
- `.github/workflows/release.yml` — autogenerated by cargo-dist; regenerated with `dist init`.

If you change `cliff.toml` or `release.toml`, validate with `cargo release patch` (dry-run is the default) against a throwaway working copy before relying on it.
