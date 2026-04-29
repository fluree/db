#!/usr/bin/env bash
#
# prepare-release.sh — Stage a Fluree DB release on a release/* branch.
#
# Bumps the workspace version, refreshes Cargo.lock, and prepends a new
# section to CHANGELOG.md from commits since the previous tag. Does NOT
# commit — review the diff and commit yourself.
#
# Usage:
#   scripts/prepare-release.sh <new-version>
#
# Example:
#   git checkout main && git pull
#   git checkout -b release/v4.0.2
#   scripts/prepare-release.sh 4.0.2
#   git diff                              # review
#   git commit -am "release v4.0.2"       # commit when satisfied
#   git push -u origin release/v4.0.2
#   gh pr create --title "release v4.0.2"
#
# After the PR merges to main:
#   git checkout main && git pull
#   git tag v4.0.2 && git push origin v4.0.2   # triggers cargo-dist
#
# See docs/contributing/releasing.md for the full workflow.

set -euo pipefail

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    echo "usage: $0 <new-version>    e.g. $0 4.0.2" >&2
    exit 1
fi
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$ ]]; then
    echo "error: '$VERSION' doesn't look like a semver version" >&2
    exit 1
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

BRANCH="$(git branch --show-current)"
if [[ ! "$BRANCH" =~ ^release/ ]]; then
    echo "error: must be on a release/* branch (current: $BRANCH)" >&2
    echo "       e.g. git checkout -b release/v$VERSION" >&2
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "error: working tree must be clean before running" >&2
    exit 1
fi

if ! command -v git-cliff >/dev/null 2>&1; then
    echo "error: git-cliff is not installed (cargo install git-cliff)" >&2
    exit 1
fi

CURRENT="$(grep -E '^version = ' Cargo.toml | head -1 | sed -E 's/^version = "([^"]+)"/\1/')"
echo "→ Current workspace version: $CURRENT"
echo "→ New workspace version:     $VERSION"
echo

# 1. Bump [workspace.package].version. There is exactly one line in
#    Cargo.toml that starts with `version = ` (the workspace-package one),
#    so a simple sed substitution is unambiguous.
sed -i.bak -E "s/^version = \"[^\"]+\"/version = \"$VERSION\"/" Cargo.toml
rm Cargo.toml.bak

# 2. Refresh Cargo.lock so workspace member entries match.
echo "→ Refreshing Cargo.lock..."
cargo update --workspace --quiet

# 3. Prepend the new CHANGELOG.md section from commits since the last tag.
echo "→ Updating CHANGELOG.md..."
git cliff --unreleased --tag "v$VERSION" --prepend CHANGELOG.md

cat <<EOF

✓ Prepared release v$VERSION.

  Review:   git diff
  Commit:   git commit -am "release v$VERSION"
  Push:     git push -u origin $BRANCH
  Open PR:  gh pr create --title "release v$VERSION"

After the PR merges to main:
  git checkout main && git pull
  git tag v$VERSION && git push origin v$VERSION
EOF
