"""Engine-neutral helpers shared by the adapters: capped subprocess execution, the
/usr/bin/time invocation, and the resource/timer parsers. Portable across macOS (BSD time)
and Linux (GNU time). Nothing here is specific to any engine."""
import os
import platform
import re
import signal
import subprocess
import time

TIMER_RE = re.compile(r"real\s+([0-9.]+)")                          # duckdb ".timer" line
RSS_BYTES_RE = re.compile(r"(\d+)\s+maximum resident set size")     # BSD/macOS `time -l` (bytes)
RSS_KB_RE = re.compile(r"[Mm]aximum resident set size \(kbytes\):\s*(\d+)")  # GNU `time -v` (KB)


def time_argv():
    """The /usr/bin/time invocation for this OS. GNU time (Linux) uses -v and reports RSS in
    KB; BSD time (macOS) uses -l and reports RSS in bytes. parse_rss() normalizes both to bytes."""
    if platform.system() == "Linux":
        return ["/usr/bin/time", "-v"]
    return ["/usr/bin/time", "-l"]


def parse_reals(text):
    return [float(x) for x in TIMER_RE.findall(text)]


def parse_rss(text):
    """Peak RSS in BYTES, from either BSD `time -l` (bytes) or GNU `time -v` (KB)."""
    m = RSS_BYTES_RE.search(text)
    if m:
        return int(m.group(1))
    m = RSS_KB_RE.search(text)
    if m:
        return int(m.group(1)) * 1024
    return None


def run_capped(cmd, input_text, cwd, timeout_s, env=None):
    """Run cmd in its own process group with a hard wall cap. On timeout, SIGKILL the whole
    group (so a timed-out remote scan can't keep draining the network in the background) and
    report timed_out=True. Returns (stdout, stderr, rc, timed_out, proc_ms)."""
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, cwd=cwd,
                         start_new_session=True, env=env)
    timed_out = False
    try:
        out, err = p.communicate(input=input_text, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            out, err = p.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            out, err = "", ""
    proc_ms = (time.perf_counter() - t0) * 1000.0
    return out or "", err or "", p.returncode, timed_out, proc_ms
