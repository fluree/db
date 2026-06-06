#!/usr/bin/env bash
# Updates the fluree-bin AUR package for a new upstream release.
#
# Usage: publish.sh <tag>           # e.g. publish.sh v4.0.2
#
# Expects, in the calling environment:
#   - SSH access to aur@aur.archlinux.org (key configured + known_hosts pinned).
#   - docker, available for a one-off makepkg run in an Arch container.
#   - git user.name / user.email configured.
set -euo pipefail

TAG="${1:?usage: $0 <tag>}"
VERSION="${TAG#v}"

REPO_ROOT="$(git -C "$(dirname "$0")/../.." rev-parse --show-toplevel)"
TEMPLATE="${REPO_ROOT}/contrib/aur/fluree-bin/PKGBUILD"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fetch_sha() {
  curl -fsSL \
    "https://github.com/fluree/db/releases/download/${TAG}/fluree-db-cli-${1}-unknown-linux-gnu.tar.xz.sha256" \
    | awk '{print $1}'
}

echo "==> Fetching release checksums for ${TAG}"
SHA_X86="$(fetch_sha x86_64)"
SHA_ARM="$(fetch_sha aarch64)"
[[ -n "$SHA_X86" && -n "$SHA_ARM" ]] || { echo "missing sha256 sums" >&2; exit 1; }

echo "==> Cloning AUR repo"
git clone "ssh://aur@aur.archlinux.org/fluree-bin.git" "${WORK}/aur"

echo "==> Templating PKGBUILD (pkgver=${VERSION})"
cp "$TEMPLATE" "${WORK}/aur/PKGBUILD"
sed -i \
  -e "s/^pkgver=.*/pkgver=${VERSION}/" \
  -e "s/^pkgrel=.*/pkgrel=1/" \
  -e "s/^sha256sums_x86_64=.*/sha256sums_x86_64=('${SHA_X86}')/" \
  -e "s/^sha256sums_aarch64=.*/sha256sums_aarch64=('${SHA_ARM}')/" \
  "${WORK}/aur/PKGBUILD"

# makepkg refuses to run as root, so we create an in-container `builder` user
# and chown the bind-mounted /work to it. We must chown back to the host
# runner's uid/gid before the container exits — otherwise the subsequent
# host-side `git add` can't write `.git/index.lock` and the trap can't clean up.
echo "==> Regenerating .SRCINFO via makepkg in archlinux:base-devel"
docker run --rm -v "${WORK}/aur:/work" -w /work \
  -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
  archlinux:base-devel bash -c '
  useradd -m builder && chown -R builder /work
  su builder -c "makepkg --printsrcinfo" > .SRCINFO
  chown -R "$HOST_UID:$HOST_GID" /work
'

cd "${WORK}/aur"
if git diff --quiet; then
  echo "==> No changes; AUR already at ${VERSION}"
  exit 0
fi

git add PKGBUILD .SRCINFO
git commit -m "fluree-bin ${VERSION}"
git push
echo "==> Pushed fluree-bin ${VERSION} to AUR"
