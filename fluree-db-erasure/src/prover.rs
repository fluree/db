//! Running the prover: a separate binary, one redaction step per process.

use std::path::{Path, PathBuf};
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

/// The prover's executable name.
pub const PROVER_BINARY: &str = "fluree-prover";

/// Where the prover is, as an absolute path: `explicit` when given (a bare
/// command name looked up on the `PATH` first, as a shell would), else
/// `fluree-prover` beside the running executable, else the first on the
/// `PATH`.
pub fn find_prover(explicit: Option<&Path>) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect::<Vec<_>>())
        .unwrap_or_default();
    find_prover_on(explicit, path)
}

fn find_prover_on(explicit: Option<&Path>, path: Vec<PathBuf>) -> Option<PathBuf> {
    let found = match explicit {
        Some(explicit) if is_command_name(explicit) => {
            Some(find_named(path, explicit).unwrap_or_else(|| explicit.to_path_buf()))
        }
        Some(explicit) => Some(explicit.to_path_buf()),
        None => {
            let beside = std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(Path::to_path_buf));
            find_in(beside.into_iter().chain(path))
        }
    };
    // Steps run in their scratch directories, where a relative path names
    // nothing.
    found.map(|p| std::path::absolute(&p).unwrap_or(p))
}

/// Whether `path` is a bare name, with no directory part.
fn is_command_name(path: &Path) -> bool {
    let mut components = path.components();
    matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(_)), None)
    )
}

fn find_in(dirs: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    find_named(dirs, Path::new(PROVER_BINARY))
}

/// The first of `dirs` holding executable `name`, with the platform's
/// executable suffix when `name` has no extension.
fn find_named(dirs: impl IntoIterator<Item = PathBuf>, name: &Path) -> Option<PathBuf> {
    let name = match name.extension() {
        None => PathBuf::from(format!(
            "{}{}",
            name.display(),
            std::env::consts::EXE_SUFFIX
        )),
        Some(_) => name.to_path_buf(),
    };
    dirs.into_iter()
        .map(|dir| dir.join(&name))
        .find(|candidate| candidate.is_file())
}

/// Check that `prover` proves an image this build accepts
/// ([`crate::REDACT_IMAGE_IDS`]): a prover of another release makes proofs
/// every check here refuses. A prover that does not answer within ten
/// seconds is killed and refused.
pub fn check_prover(prover: &Path) -> Result<(), String> {
    check_within(prover, CHECK_TIMEOUT)
}

/// How long `--image-id`, which prints a constant, may take.
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

fn check_within(prover: &Path, timeout: Duration) -> Result<(), String> {
    let mut command = std::process::Command::new(prover);
    command
        .arg("--image-id")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    let child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", prover.display()))?;
    let _group = Group(Some(child.id()));
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    let output = rx
        .recv_timeout(timeout)
        .map_err(|_| {
            format!(
                "{} --image-id did not answer within {timeout:?}",
                prover.display()
            )
        })?
        .map_err(|e| format!("{}: {e}", prover.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} --image-id failed: {}",
            prover.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let printed = String::from_utf8_lossy(&output.stdout);
    let words: Option<Vec<u32>> = printed
        .split_whitespace()
        .map(|w| u32::from_str_radix(w.trim_start_matches("0x"), 16).ok())
        .collect();
    match words {
        Some(words) if crate::REDACT_IMAGE_IDS.iter().any(|id| id[..] == words[..]) => Ok(()),
        Some(_) => Err(format!(
            "{} proves image {}, which this release does not accept: install this release's \
             prover",
            prover.display(),
            printed.trim()
        )),
        None => Err(format!(
            "{} --image-id printed no image id: {}",
            prover.display(),
            printed.trim()
        )),
    }
}

/// Install `binary` at `dest` once it passes [`check_prover`]: staged
/// beside `dest`, then renamed into place, so `dest` is never a prover
/// that failed the check.
pub fn install_prover(binary: &[u8], dest: &Path) -> Result<(), String> {
    use std::io::Write;
    let dir = dest.parent().unwrap_or(Path::new("."));
    let mut staged = tempfile::Builder::new()
        .prefix(".fluree-prover-")
        .tempfile_in(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?;
    staged.write_all(binary).map_err(|e| e.to_string())?;
    // Closed before it runs: Linux refuses to run a file open for writing.
    let staged = staged.into_temp_path();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| e.to_string())?;
    }
    check_prover(&staged)?;
    staged
        .persist(dest)
        .map_err(|e| format!("{}: {}", dest.display(), e.error))
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

/// The command a step runs: in `scratch`, at low priority, in its own
/// process group, and the process the kernel prefers to kill when memory
/// runs out.
fn command(config: &ProverConfig, scratch: &Path) -> tokio::process::Command {
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
                // Proving can take gigabytes: when memory runs out, the
                // kernel picks the prover, whose step is retried, over the
                // server. A cgroup with memory.oom.group set is killed
                // whole; only a separate limit isolates the server.
                // Raising the score needs no privilege.
                #[cfg(target_os = "linux")]
                {
                    let fd = libc::open(c"/proc/self/oom_score_adj".as_ptr(), libc::O_WRONLY);
                    if fd >= 0 {
                        libc::write(fd, b"1000".as_ptr().cast(), 4);
                        libc::close(fd);
                    }
                }
                Ok(())
            });
        }
    }
    command
}

async fn run(
    config: &ProverConfig,
    scratch: &Path,
    request: &ProveRequest,
) -> Result<ProveResponse, ProverError> {
    let unavailable = |e: std::io::Error| ProverError::Unavailable(e.to_string());
    let mut child = command(config, scratch).spawn().map_err(unavailable)?;
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

#[cfg(all(test, unix))]
mod discovery_tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use super::{
        check_prover, check_within, find_in, find_prover, find_prover_on, install_prover,
        PROVER_BINARY,
    };

    /// A stand-in prover printing `id` for `--image-id`.
    fn fake(dir: &Path, id: &str) -> std::path::PathBuf {
        let path = dir.join(PROVER_BINARY);
        std::fs::write(&path, format!("#!/bin/sh\necho '{id}'\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn pinned() -> String {
        crate::REDACT_IMAGE_IDS[0]
            .iter()
            .map(|w| format!("{w:#010x}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The first directory holding the prover is where it is.
    #[test]
    fn the_prover_is_found_in_the_first_directory_holding_it() {
        let (empty, holding) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let prover = fake(holding.path(), &pinned());
        let dirs = [empty.path().to_path_buf(), holding.path().to_path_buf()];
        assert_eq!(find_in(dirs), Some(prover));
        assert_eq!(find_in([empty.path().to_path_buf()]), None);
    }

    /// A prover is accepted only when it proves an image this build pins.
    #[test]
    fn a_prover_of_another_image_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        check_prover(&fake(dir.path(), &pinned())).unwrap();
        let other = check_prover(&fake(dir.path(), "0x00000001 0x00000002")).unwrap_err();
        assert!(other.contains("does not accept"), "{other}");
        let garbled = check_prover(&fake(dir.path(), "not an id")).unwrap_err();
        assert!(garbled.contains("no image id"), "{garbled}");
        assert!(check_prover(&dir.path().join("missing")).is_err());
    }

    /// A relative prover path resolves where it was given, not in a step's
    /// scratch directory.
    #[test]
    fn the_prover_is_named_by_an_absolute_path() {
        let found = find_prover(Some(Path::new("bin/fluree-prover"))).unwrap();
        assert_eq!(
            found,
            std::env::current_dir().unwrap().join("bin/fluree-prover")
        );
    }

    /// A bare command name is the first on the `PATH` holding it, and
    /// resolves where it was given when none does.
    #[test]
    fn a_prover_named_by_command_is_found_on_the_path() {
        let (empty, holding) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let prover = fake(holding.path(), &pinned());
        let path = vec![empty.path().to_path_buf(), holding.path().to_path_buf()];
        let named = Path::new(PROVER_BINARY);
        assert_eq!(find_prover_on(Some(named), path), Some(prover));
        assert_eq!(
            find_prover_on(Some(named), vec![empty.path().to_path_buf()]),
            Some(std::env::current_dir().unwrap().join(PROVER_BINARY))
        );
    }

    /// A prover that does not answer is killed, with its process group,
    /// and refused.
    #[test]
    fn a_prover_that_does_not_answer_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("pid");
        let hung = fake(dir.path(), "");
        std::fs::write(
            &hung,
            format!(
                "#!/bin/sh\necho $$ > {}\nexec sleep 60\n",
                pid_file.display()
            ),
        )
        .unwrap();

        let started = std::time::Instant::now();
        let why = check_within(&hung, std::time::Duration::from_secs(2)).unwrap_err();
        assert!(why.contains("did not answer"), "{why}");
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        let pid: libc::pid_t = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let gone = (0..100).any(|_| {
            std::thread::sleep(std::time::Duration::from_millis(50));
            // SAFETY: signal 0 only checks that the process exists.
            unsafe { libc::kill(pid, 0) != 0 }
        });
        assert!(gone, "the prover {pid} still runs");
    }

    /// Only a prover that passes the check is installed, and a refused one
    /// leaves nothing behind.
    #[test]
    fn only_a_checked_prover_is_installed() {
        let script = |id: &str| format!("#!/bin/sh\necho '{id}'\n").into_bytes();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join(PROVER_BINARY);

        let why = install_prover(&script("0x00000001"), &dest).unwrap_err();
        assert!(why.contains("does not accept"), "{why}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        install_prover(&script(&pinned()), &dest).unwrap();
        check_prover(&dest).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod oom_tests {
    use super::{command, ProverConfig};

    /// A step's process is the first the kernel kills when memory runs out.
    #[tokio::test]
    async fn a_step_runs_first_in_line_for_the_oom_killer() {
        let dir = tempfile::tempdir().unwrap();
        let config = ProverConfig {
            command: "/bin/cat".into(),
            scratch_dir: dir.path().to_path_buf(),
            threads: None,
            niceness: 0,
            timeout: std::time::Duration::from_secs(5),
        };
        let output = command(&config, dir.path())
            .arg("/proc/self/oom_score_adj")
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "1000");
    }
}
