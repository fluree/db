"""Exit 0 when the image id on stdin (hex words, as `fluree-prover --image-id`
prints it) is one of fluree-db-erasure's REDACT_IMAGE_IDS."""

import pathlib
import re
import sys

verify = pathlib.Path(__file__).resolve().parent.parent / "fluree-db-erasure/src/verify.rs"
source = verify.read_text()
start = source.index("REDACT_IMAGE_IDS")
block = source[start : source.index("];", start)]
words = re.findall(r"0x[0-9a-f]{8}", block)
pinned = [words[i : i + 8] for i in range(0, len(words), 8)]
built = re.findall(r"0x[0-9a-f]{8}", sys.stdin.read())
if len(built) != 8:
    sys.exit(f"not an image id: {built}")
print("built:", " ".join(built))
if built not in pinned:
    sys.exit(
        "fluree-db-erasure's REDACT_IMAGE_IDS does not pin this guest; add its id "
        "(see fluree-prover/README.md, Image ids)"
    )
print("pinned in", verify.name)
