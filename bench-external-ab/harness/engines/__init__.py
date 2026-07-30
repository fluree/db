"""Engine adapters for the external-engine A/B harness.

The harness core (run_pair.py, check_pair.py) is engine-agnostic. Each engine under
measurement is an external adapter in this package: `fluree.py` (the engine being
characterized) and `duckdb.py` (the first external yardstick engine, invoked as an
independently installed CLI — see the NOTICE in ../README.md / ../PROTOCOL.md). Adding
another external engine means adding another adapter module here; no core change.
"""
