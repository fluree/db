"""Offline preparation regressions with a tiny synthetic source archive."""

import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import prepare_storage as storage


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        root_patch = patch.object(storage, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        (self.root / "patches").mkdir()
        (self.root / "patches/object_store-0.13.2-reqwest013.patch").write_text(
            "--- a/source.txt\n+++ b/source.txt\n@@ -1 +1 @@\n-before\n+after\n"
        )

    def archive(self, name=None, symlink=False):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            member = tarfile.TarInfo(name or f"{storage.NAME}/source.txt")
            if symlink:
                member.type = tarfile.SYMTYPE
                member.linkname = "/tmp/outside"
                archive.addfile(member)
            else:
                member.size = len(b"before\n")
                archive.addfile(member, io.BytesIO(b"before\n"))
        path = self.root / "source.crate"
        path.write_bytes(buffer.getvalue())
        checksum_patch = patch.object(storage, "SHA256", hashlib.sha256(buffer.getvalue()).hexdigest())
        checksum_patch.start()
        self.addCleanup(checksum_patch.stop)
        return path

    def test_repeat_verification_and_refuse_modified_source(self):
        archive = self.archive()
        storage.prepare(archive)
        source = self.root / ".patched" / storage.NAME / "source.txt"
        self.assertEqual(source.read_text(), "after\n")
        storage.prepare(None)  # Verified cached archive; no network.
        source.write_text("local edits\n")
        with self.assertRaisesRegex(RuntimeError, "refusing to overwrite"):
            storage.prepare(archive)
        self.assertEqual(source.read_text(), "local edits\n")

    def test_corrupt_archive_is_rejected_before_extraction(self):
        archive = self.archive()
        archive.write_bytes(b"corrupt")
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            storage.prepare(archive)
        self.assertFalse((self.root / ".patched" / storage.NAME).exists())

    def test_traversal_and_archive_symlink_are_rejected(self):
        for name, symlink in [(f"{storage.NAME}/../outside", False), (None, True)]:
            with self.subTest(name=name, symlink=symlink):
                with self.assertRaisesRegex(RuntimeError, "unexpected archive path|unsupported archive entry"):
                    storage.prepare(self.archive(name, symlink))
        self.assertFalse((self.root / "outside").exists())

    def test_source_symlink_is_rejected(self):
        archive = self.archive()
        storage.prepare(archive)
        source = self.root / ".patched" / storage.NAME / "source.txt"
        outside = self.root / "same-content.txt"
        outside.write_bytes(source.read_bytes())
        source.unlink()
        source.symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "unexpected source symlink"):
            storage.prepare(archive)


if __name__ == "__main__":
    unittest.main()
