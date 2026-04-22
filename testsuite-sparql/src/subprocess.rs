//! Subprocess-based test isolation for reliable timeout enforcement.
//!
//! Each W3C test runs in a child process. If the test exceeds its timeout,
//! `child.kill()` terminates the process and all its threads cleanly — no
//! zombie threads burning CPU.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// Describes a single W3C test to run in a subprocess.
#[derive(Debug, Serialize, Deserialize)]
pub enum TestDescriptor {
    /// Syntax test: parse the query and report whether errors were found.
    Syntax { query_url: String, test_id: String },
    /// Query evaluation test: load data, execute query, compare results.
    Eval {
        test_id: String,
        query_url: String,
        data_url: Option<String>,
        result_url: String,
        graph_data: Vec<(String, String)>,
    },
}

/// Result communicated back from the subprocess via stdout JSON.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubprocessResult {
    pub passed: bool,
    /// For syntax tests: whether the parser reported errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_errors: Option<bool>,
    /// Error message if the test failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Environment variable used to pass the test descriptor to the subprocess.
pub const ENV_TEST_DESCRIPTOR: &str = "W3C_TEST_DESCRIPTOR";

/// Run a W3C test in an isolated subprocess with a timeout.
///
/// On timeout, the child process is killed — all its threads die cleanly.
/// No zombie threads, no CPU leak.
pub fn run_in_subprocess(
    descriptor: &TestDescriptor,
    timeout: Duration,
) -> Result<SubprocessResult> {
    let binary = locate_binary()?;
    let descriptor_json =
        serde_json::to_string(descriptor).context("Serializing test descriptor")?;

    let mut child = Command::new(&binary)
        .env(ENV_TEST_DESCRIPTOR, &descriptor_json)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Spawning subprocess: {}", binary.display()))?;

    let start = Instant::now();

    // Poll for completion with 50ms intervals
    loop {
        match child.try_wait().context("Checking subprocess status")? {
            Some(_status) => {
                // Child exited — read its output
                let output = child
                    .wait_with_output()
                    .context("Reading subprocess output")?;
                return parse_subprocess_output(&output.stdout, &output.stderr);
            }
            None => {
                if start.elapsed() >= timeout {
                    // Timeout: kill the child process (and all its threads)
                    let _ = child.kill();
                    let _ = child.wait(); // reap the zombie
                    let test_id = match descriptor {
                        TestDescriptor::Syntax { test_id, .. }
                        | TestDescriptor::Eval { test_id, .. } => test_id,
                    };
                    bail!("Test timed out (>{timeout:?}) — subprocess killed.\nTest: {test_id}");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Locate the `run-w3c-test` binary.
///
/// When running under `cargo test`, the env var `CARGO_BIN_EXE_run-w3c-test`
/// points to the compiled binary. Fall back to searching near the test binary.
fn locate_binary() -> Result<std::path::PathBuf> {
    // Preferred: cargo sets this for integration tests
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_run-w3c-test") {
        let p = std::path::PathBuf::from(&path);
        if p.exists() {
            return Ok(p);
        }
    }

    // Fallback: look next to the current executable
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(dir) = current_exe.parent() {
            // Integration test binaries are in target/debug/deps/,
            // regular binaries are in target/debug/
            for candidate_dir in [dir, dir.parent().unwrap_or(dir)] {
                let candidate = candidate_dir.join("run-w3c-test");
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
        }
    }

    bail!(
        "Cannot find run-w3c-test binary. \
         Make sure to run via `cargo test` (which builds it automatically) \
         or build it first with `cargo build --bin run-w3c-test`."
    )
}

/// Parse the subprocess stdout as a `SubprocessResult`.
///
/// If stdout isn't valid JSON (e.g., the process crashed), synthesize an
/// error result from stderr.
fn parse_subprocess_output(stdout: &[u8], stderr: &[u8]) -> Result<SubprocessResult> {
    let stdout_str = String::from_utf8_lossy(stdout);

    // Try to parse the JSON result from stdout
    if let Ok(result) = serde_json::from_str::<SubprocessResult>(stdout_str.trim()) {
        return Ok(result);
    }

    // No valid JSON — the subprocess probably crashed or panicked
    let stderr_str = String::from_utf8_lossy(stderr);
    let error_msg = if stderr_str.is_empty() {
        format!("Subprocess produced no parseable output. stdout: {stdout_str}")
    } else {
        format!("Subprocess error: {stderr_str}")
    };

    Ok(SubprocessResult {
        passed: false,
        has_errors: None,
        error: Some(error_msg),
    })
}
