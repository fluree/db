//! Running the prover: a separate binary, one redaction step per process.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use fluree_db_redaction::protocol::{ProveRequest, ProveResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Why a step was not proved.
#[derive(Debug, thiserror::Error)]
pub enum ProverError {
    /// The prover could not be run, or did not finish in time.
    #[error("prover unavailable: {0}")]
    Unavailable(String),
    /// The prover refused the step (malformed input, invalid slots, a
    /// signature that does not verify): it will not prove on a retry.
    #[error("prover refused the step: {0}")]
    Refused(String),
    /// The prover ran and failed.
    #[error("prover failed: {0}")]
    Failed(String),
}

/// Proves one redaction step.
#[async_trait::async_trait]
pub trait Prover: Send + Sync + std::fmt::Debug {
    async fn prove(&self, request: &ProveRequest) -> Result<ProveResponse, ProverError>;

    /// Remove what steps of processes no longer running left in scratch:
    /// a crash mid-step leaves files that hold the commit being proved.
    /// Returns how many steps' it removed.
    fn clear_residue(&self) -> std::io::Result<usize> {
        Ok(0)
    }
}

/// How Fluree runs the prover binary.
#[derive(Clone, Debug)]
pub struct ProverConfig {
    /// The `fluree-prover` executable.
    pub command: PathBuf,
    /// Scratch directory for the prover's temporary files; each step gets
    /// its own subdirectory, removed when the step ends.
    pub scratch_dir: PathBuf,
    /// Proving threads; `None` uses every core.
    pub threads: Option<usize>,
    /// Scheduling priority (Unix nice value), so transactions come first.
    pub niceness: i32,
    /// A step still running after this long is killed and retried later.
    pub timeout: Duration,
}

/// Runs [`ProverConfig::command`] once per step: the request on stdin, the
/// response on stdout.
#[derive(Debug)]
pub struct SubprocessProver {
    config: ProverConfig,
}

impl SubprocessProver {
    pub fn new(config: ProverConfig) -> Self {
        SubprocessProver { config }
    }
}

#[async_trait::async_trait]
impl Prover for SubprocessProver {
    async fn prove(&self, request: &ProveRequest) -> Result<ProveResponse, ProverError> {
        let unavailable = |e: std::io::Error| ProverError::Unavailable(e.to_string());
        let scratch = self.config.scratch_dir.join(format!(
            "step-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        tokio::fs::create_dir_all(&scratch)
            .await
            .map_err(unavailable)?;
        let outcome = run(&self.config, &scratch, request).await;
        if let Err(e) = tokio::fs::remove_dir_all(&scratch).await {
            tracing::warn!(dir = %scratch.display(), error = %e, "prover scratch not removed");
        }
        outcome
    }

    fn clear_residue(&self) -> std::io::Result<usize> {
        fluree_db_core::process::clear_ended(&self.config.scratch_dir, |name| {
            name.strip_prefix("step-")?.split_once('-')?.0.parse().ok()
        })
    }
}

/// Kills a step's process group when the step ends, however it ends.
struct Group(Option<u32>);

impl Drop for Group {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.0 {
            // SAFETY: signals the group this step's child leads; ESRCH once
            // it has ended is harmless.
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

async fn run(
    config: &ProverConfig,
    scratch: &std::path::Path,
    request: &ProveRequest,
) -> Result<ProveResponse, ProverError> {
    let unavailable = |e: std::io::Error| ProverError::Unavailable(e.to_string());
    let mut command = tokio::process::Command::new(&config.command);
    // Nothing in Fluree's environment may switch the prover to fake
    // receipts or another backend.
    for var in [
        "RISC0_DEV_MODE",
        "RISC0_PROVER",
        "BONSAI_API_URL",
        "BONSAI_API_KEY",
    ] {
        command.env_remove(var);
    }
    command
        .env("TMPDIR", scratch)
        .current_dir(scratch)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(threads) = config.threads {
        command.env("RAYON_NUM_THREADS", threads.to_string());
    }
    #[cfg(unix)]
    {
        // Its own process group, so a step's children end with it.
        command.process_group(0);
        let niceness = config.niceness;
        // SAFETY: setpriority is async-signal-safe; it runs in the child
        // between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::setpriority(libc::PRIO_PROCESS, 0, niceness) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn().map_err(unavailable)?;
    let _group = Group(child.id());
    let mut stdin = child.stdin.take().expect("piped");
    let mut stdout = child.stdout.take().expect("piped");
    let mut stderr = child.stderr.take().expect("piped");
    let input = request.encode();
    let exchange = async {
        let write = async {
            stdin.write_all(&input).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        let mut out = Vec::new();
        let mut err = Vec::new();
        let (written, read, read_err) = tokio::join!(
            write,
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err)
        );
        written?;
        read?;
        read_err?;
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((status, out, err))
    };
    let (status, out, err) = tokio::time::timeout(config.timeout, exchange)
        .await
        .map_err(|_| ProverError::Unavailable(format!("no result in {:?}", config.timeout)))?
        .map_err(unavailable)?;
    if !status.success() {
        let message = String::from_utf8_lossy(&err);
        let last = message
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("no message")
            .to_string();
        // The prover's exit status 2 is a refusal (see `fluree-prover`).
        return Err(if status.code() == Some(2) {
            ProverError::Refused(last)
        } else {
            ProverError::Failed(format!("{status}: {last}"))
        });
    }
    ProveResponse::decode(&out).map_err(|e| ProverError::Failed(format!("its response: {e}")))
}

#[cfg(all(test, unix))]
mod residue_tests {
    use super::{Prover, ProverConfig, SubprocessProver};

    /// A step's scratch is residue once its process has ended; this
    /// process's steps stay.
    #[test]
    fn only_steps_of_ended_processes_are_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let ended = child.id();
        child.wait().unwrap();
        let dead = dir.path().join(format!("step-{ended}-1"));
        let ours = dir.path().join(format!("step-{}-2", std::process::id()));
        for d in [&dead, &ours] {
            std::fs::create_dir_all(d).unwrap();
        }
        let prover = SubprocessProver::new(ProverConfig {
            command: "fluree-prover".into(),
            scratch_dir: dir.path().to_path_buf(),
            threads: None,
            niceness: 0,
            timeout: std::time::Duration::from_secs(1),
        });

        assert_eq!(prover.clear_residue().unwrap(), 1);
        assert!(!dead.exists() && ours.exists());
    }
}
