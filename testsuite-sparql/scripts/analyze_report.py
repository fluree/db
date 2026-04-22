#!/usr/bin/env python3
"""Analyze W3C SPARQL test suite JSON reports.

Usage:
    python3 scripts/analyze_report.py summary   [REPORT_FILE]
    python3 scripts/analyze_report.py classify  [REPORT_FILE]
    python3 scripts/analyze_report.py failures  [REPORT_FILE] [--category CAT]

Default REPORT_FILE is report-eval.json.
"""

import json
import sys
from collections import defaultdict


def load_report(path):
    with open(path) as f:
        return json.load(f)


def extract_category(test_id):
    """Extract the W3C test category from a test ID URL."""
    parts = test_id.split("/")
    # SPARQL 1.1: .../data-sparql11/<category>/manifest#...
    for i, p in enumerate(parts):
        if p == "data-sparql11" and i + 1 < len(parts):
            return parts[i + 1]
    # SPARQL 1.0: .../data-r2/<category>/manifest#...
    for i, p in enumerate(parts):
        if p == "data-r2" and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"


def extract_test_name(test_id):
    """Extract the short test name from a full test ID URL."""
    return test_id.split("#")[-1] if "#" in test_id else test_id


def classify_error(test):
    """Classify a failing test into an error category."""
    err = test.get("error", "")
    if test.get("timeout"):
        return "TIMEOUT"
    if "panicked" in err:
        return "PANIC"
    if "parser accepted invalid" in err:
        return "NEGATIVE SYNTAX"
    if "parser rejected valid" in err:
        return "POSITIVE SYNTAX"
    if "not yet implemented" in err.lower():
        return "NOT IMPLEMENTED"
    if "lowering error" in err or "SPARQL error" in err or "unexpected character" in err:
        return "PARSE/LOWERING"
    if "Internal" in err:
        return "INTERNAL ERROR"
    if "not isomorphic" in err and "got 0" in err:
        return "EMPTY RESULTS"
    if "not isomorphic" in err:
        return "RESULT MISMATCH"
    return "OTHER"


def cmd_summary(report):
    """Print per-category pass/fail breakdown."""
    cats = defaultdict(lambda: {"pass": 0, "fail": 0, "total": 0})
    for t in report["tests"]:
        cat = extract_category(t["test_id"])
        cats[cat]["total"] += 1
        if t["status"] == "pass":
            cats[cat]["pass"] += 1
        else:
            cats[cat]["fail"] += 1

    print(f"{'Category':<24} {'Pass':>5} {'Fail':>5} {'Total':>5} {'Rate':>7}")
    print("-" * 52)
    for cat, c in sorted(cats.items(), key=lambda x: -(x[1]["pass"] / max(x[1]["total"], 1))):
        rate = f"{100 * c['pass'] / c['total']:.0f}%" if c["total"] > 0 else "-"
        print(f"{cat:<24} {c['pass']:>5} {c['fail']:>5} {c['total']:>5} {rate:>7}")
    print("-" * 52)
    print(f"{'TOTAL':<24} {report['passed']:>5} {report['failed']:>5} {report['total']:>5} {report['pass_rate']:>7}")


def cmd_classify(report):
    """Group failures by error type."""
    classes = defaultdict(list)
    for t in report["tests"]:
        if t["status"] != "fail":
            continue
        cls = classify_error(t)
        classes[cls].append(extract_test_name(t["test_id"]))

    for cls, tests in sorted(classes.items(), key=lambda x: -len(x[1])):
        print(f"{len(tests):>4}  {cls}")
        for t in tests[:3]:
            print(f"        - {t}")
        if len(tests) > 3:
            print(f"        ... and {len(tests) - 3} more")
        print()


def cmd_failures(report, category=None):
    """List all failures, optionally filtered to a category."""
    for t in report["tests"]:
        if t["status"] != "fail":
            continue
        cat = extract_category(t["test_id"])
        if category and cat != category:
            continue
        tid = extract_test_name(t["test_id"])
        cls = classify_error(t)
        first_line = t.get("error", "").strip().split("\n")[0][:100]
        print(f"{tid:<36} [{cls}]  {first_line}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print(__doc__.strip())
        sys.exit(0)

    command = sys.argv[1]
    report_file = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "report-eval.json"

    try:
        report = load_report(report_file)
    except FileNotFoundError:
        print(f"Error: {report_file} not found. Run 'make report-eval-json' first.", file=sys.stderr)
        sys.exit(1)

    if command == "summary":
        cmd_summary(report)
    elif command == "classify":
        cmd_classify(report)
    elif command == "failures":
        category = None
        for i, arg in enumerate(sys.argv):
            if arg == "--category" and i + 1 < len(sys.argv):
                category = sys.argv[i + 1]
        cmd_failures(report, category)
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print(__doc__.strip(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
