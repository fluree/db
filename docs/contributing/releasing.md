# Releasing

Fluree DB releases go through a **two-phase pull-request flow**:

1. **Phase 1** — On a `release/*` branch, a small script (`scripts/prepare-release.sh`) bumps the workspace version and prepends a new section to `CHANGELOG.md`. You then review, commit, push, and open a PR.
2. **Phase 2** — After the PR is reviewed and merged to `main`, you tag the merge commit. The pushed tag triggers `.github/workflows/release.yml` (cargo-dist), which builds binaries, creates the GitHub Release, publishes Homebrew, and pushes Docker images.

The split is deliberate: cargo-dist's release workflow triggers on **any** pushed `vX.Y.Z` tag regardless of branch, so the tag step is held until after merge. The script does no committing or pushing — that's all on you, so there's a clear review gate.

| Tool          | Job                                                                                          |
|---------------|----------------------------------------------------------------------------------------------|
| `git-cliff`   | Generates `CHANGELOG.md` entries from conventional-commit subjects. Config: `cliff.toml`.    |
| `scripts/prepare-release.sh` | Bumps the workspace version, refreshes `Cargo.lock`, and runs git-cliff. Stops short of committing. |
| `cargo-dist`  | Picks up the pushed `vX.Y.Z` tag, builds and publishes everything. Config: `dist-workspace.toml`, `.github/workflows/release.yml`. |

## One-time setup

`git-cliff` is the only tool you install locally; cargo-dist runs in CI.

```bash
cargo install git-cliff
```

## Phase 1 — Open the release PR

```bash
# 1. Cut a release branch from clean main.
git checkout main && git pull
git checkout -b release/v4.0.2

# 2. Bump version + update CHANGELOG.md (no commit).
scripts/prepare-release.sh 4.0.2

# 3. Review the diff.
git diff

# 4. Commit when satisfied. Edit CHANGELOG.md by hand first if you want to
#    polish wording, regroup entries, drop noise, etc.
git commit -am "release v4.0.2"

# 5. Push and open a PR.
git push -u origin release/v4.0.2
gh pr create --title "release v4.0.2" \
    --body "Bump workspace to 4.0.2. See CHANGELOG.md for details."
```

What `prepare-release.sh` does, in order:

1. Verifies you're on a `release/*` branch with a clean working tree.
2. Bumps `[workspace.package].version` in the root `Cargo.toml`. Every member crate inherits via `version.workspace = true`.
3. Runs `cargo update --workspace` to refresh `Cargo.lock`.
4. Runs `git cliff --unreleased --tag v<version> --prepend CHANGELOG.md` — generates only the new section (commits since the previous tag) and prepends it above the existing changelog. Existing version sections are untouched.

It does **not** commit, tag, or push.

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

Hand-editing `CHANGELOG.md` after running the script is fine and expected — git-cliff gives you a starting point, not the final wording. The committed file is the source of truth for the GitHub Release notes.

## Bootstrapping the changelog

The committed `CHANGELOG.md` starts as a stub. To regenerate it from full git history (covering every existing tag back to the start of the repo), run once:

```bash
git cliff --output CHANGELOG.md
git add CHANGELOG.md
git commit -m "docs: bootstrap CHANGELOG.md from git history"
```

Subsequent releases use `--unreleased --prepend` (via `prepare-release.sh`) and only add the new section, so the bootstrap is one-time.

## Rolling back

**During Phase 1, before pushing the branch:**

```bash
git reset --hard HEAD            # if already committed
git checkout main
git branch -D release/vX.Y.Z
```

**After the release PR is opened but before merge:**

Close the PR and delete the branch on GitHub. Nothing has shipped.

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
- `scripts/prepare-release.sh` — release-prep entry point.
- `dist-workspace.toml` — cargo-dist's distribution targets and installers.
- `.github/workflows/release.yml` — autogenerated by cargo-dist; regenerated with `dist init`.
