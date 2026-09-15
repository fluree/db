"""Prepare the isolated object_store compatibility backport; never overwrite edits.

Run before Cargo commands. Requires Python 3 and the standard `patch` executable.
Use --archive to supply a cached .crate file instead of downloading it.
"""

import argparse
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile
import urllib.request

NAME = "object_store-0.13.2"
SHA256 = "622acbc9100d3c10e2ee15804b0caa40e55c933d5aa53814cd520805b7958a49"
URL = f"https://static.crates.io/crates/object_store/{NAME}.crate"
ROOT = Path(__file__).resolve().parent


def inventory(root):
    # Cargo may regenerate the crate's own lockfile/build outputs when running
    # upstream tests. Neither is used by the spike's locked dependency build.
    if root.is_symlink():
        raise RuntimeError(f"unexpected source symlink: {root}")
    result = {}
    for directory, dirs, files in os.walk(root):
        if Path(directory) == root:
            dirs[:] = [name for name in dirs if name != "target"]
            files = [name for name in files if name != "Cargo.lock"]
        for name in dirs + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise RuntimeError(f"unexpected source symlink: {path}")
        for name in files:
            path = Path(directory) / name
            result[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def prepare(archive_path):
    parent = ROOT / ".patched"
    parent.mkdir(exist_ok=True)
    cached = parent / f"{NAME}.crate"
    if archive_path:
        data = archive_path.read_bytes()
    elif cached.exists():
        data = cached.read_bytes()
    else:
        with urllib.request.urlopen(URL, timeout=60) as response:
            data = response.read()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise RuntimeError("object_store archive checksum mismatch")
    if not cached.exists():
        cached.write_bytes(data)

    with tempfile.TemporaryDirectory(prefix="prepare-", dir=parent) as temporary:
        staging = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for member in archive.getmembers():
                name = PurePosixPath(member.name)
                if not name.parts or name.is_absolute() or ".." in name.parts or name.parts[0] != NAME:
                    raise RuntimeError(f"unexpected archive path: {member.name}")
                path = staging.joinpath(*name.parts)
                if member.isdir():
                    path.mkdir(parents=True, exist_ok=True)
                elif member.isfile():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with archive.extractfile(member) as source:
                        path.write_bytes(source.read())
                    path.chmod(member.mode & 0o777)
                else:
                    raise RuntimeError(f"unsupported archive entry: {member.name}")
        expected = staging / NAME
        patch = ROOT / "patches" / "object_store-0.13.2-reqwest013.patch"
        subprocess.run(["patch", "--batch", "--fuzz=0", "-p1", "-i", str(patch)], cwd=expected, check=True)
        destination = parent / NAME
        if destination.exists():
            if inventory(destination) != inventory(expected):
                raise RuntimeError(f"{destination} differs from the verified patch; refusing to overwrite it")
        else:
            expected.rename(destination)
    print(f"Verified {NAME} + reqwest 0.13 backport at {destination}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="local .crate archive with the published checksum")
    prepare(parser.parse_args().archive)
