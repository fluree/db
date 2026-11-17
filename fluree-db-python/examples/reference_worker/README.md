# Reference worker

A custom stage that runs in Python next to a ledger: pinned inputs, an
explicit clock, and results published into a named graph the stage owns,
with a generation record beside them. The
[Custom stages in Python](https://fluree.github.io/db/guides/custom-stages-python.html)
guide explains the contract and the example program.

- `worker.py` — the worker: an `extract` stage that runs a model over
  documents and a versioned `gate` stage that decides each case.
- `manifest.json` — an example manifest (the format is experimental).
- `claims.ttl` — sample cases, documents and reference data.

```python
from pathlib import Path
import fluree

with fluree.connect("./data") as conn:
    conn.create("claims").insert(Path("claims.ttl"))
```

```sh
python worker.py manifest.json --storage ./data
```

`fluree-db-python/tests/test_reference_worker.py` runs the worker against
this data.
