use std::path::Path;
use std::process::Command;

/// Detect the current git branch name.
///
/// Returns `None` if not in a git repository or if the command fails.
/// Uses the current working directory to find the git repo.
pub fn detect_git_branch() -> Option<String> {
    detect_git_branch_from(None)
}

/// Detect the current git branch, optionally from a specific directory.
///
/// When `dir` is provided, runs `git rev-parse` from that directory
/// (useful when the MCP server's working directory differs from the repo root).
/// Falls back to the process's current directory when `dir` is `None`.
pub fn detect_git_branch_from(dir: Option<&Path>) -> Option<String> {
    let mut cmd = Command::new("git");
    cmd.args(["rev-parse", "--abbrev-ref", "HEAD"]);
    if let Some(d) = dir {
        cmd.current_dir(d);
    }

    let output = cmd.output().ok()?;

    if !output.status.success() {
        return None;
    }

    let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if branch.is_empty() || branch == "HEAD" {
        None
    } else {
        Some(branch)
    }
}
