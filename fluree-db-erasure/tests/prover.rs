//! The prover runs as a child process: its request on stdin, its response on
//! stdout, in its own scratch directory, removed afterwards.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fluree_db_erasure::{
    ProveRequest, ProveResponse, Prover, ProverConfig, ProverError, Signatures, SubprocessProver,
};

fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn config(dir: &Path, command: PathBuf, timeout: Duration) -> ProverConfig {
    ProverConfig {
        command,
        scratch_dir: dir.join("scratch"),
        threads: Some(2),
        niceness: 10,
        timeout,
    }
}

fn request() -> ProveRequest {
    ProveRequest {
        input: b"commit bytes".to_vec(),
        removed_slots: vec![1],
        ledger_id: "acme/people:main".into(),
        signatures: Signatures::Verify,
    }
}

#[tokio::test]
async fn a_step_runs_in_its_own_scratch_directory_and_returns_its_response() {
    let dir = tempfile::tempdir().unwrap();
    let response = ProveResponse {
        replacement: b"replacement".to_vec(),
        receipt: b"receipt".to_vec(),
        user_cycles: 42,
        segments: 1,
    };
    std::fs::write(dir.path().join("response"), response.encode()).unwrap();
    std::fs::write(dir.path().join("expected"), request().encode()).unwrap();
    let d = dir.path().display();
    // Checks its request, environment and priority before answering.
    let prover = script(
        dir.path(),
        "prover",
        &format!(
            "cat > request\n\
             cmp -s request {d}/expected || exit 3\n\
             [ \"$(cd \"$TMPDIR\" && pwd -P)\" = \"$(pwd -P)\" ] || exit 4\n\
             [ \"$RAYON_NUM_THREADS\" = 2 ] || exit 5\n\
             [ -z \"$RISC0_DEV_MODE\" ] || exit 7\n\
             [ \"$(ps -o nice= -p $$ | tr -d ' ')\" -ge 10 ] || exit 6\n\
             pwd -P > {d}/ran-in\n\
             cat {d}/response"
        ),
    );
    let runner = SubprocessProver::new(config(dir.path(), prover, Duration::from_secs(30)));
    // Fluree's own dev mode never reaches the prover.
    std::env::set_var("RISC0_DEV_MODE", "1");
    let outcome = runner.prove(&request()).await;
    std::env::remove_var("RISC0_DEV_MODE");
    assert_eq!(outcome.unwrap(), response);
    let ran_in = std::fs::read_to_string(dir.path().join("ran-in")).unwrap();
    let scratch = dir.path().join("scratch").canonicalize().unwrap();
    assert!(Path::new(ran_in.trim()).starts_with(&scratch), "{ran_in}");
    assert!(!Path::new(ran_in.trim()).exists(), "scratch removed");
}

#[tokio::test]
async fn a_refusal_a_timeout_and_a_missing_binary_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let refuses = script(
        dir.path(),
        "refuses",
        "cat > /dev/null\necho 'redaction refused: invalid slots' >&2\nexit 2",
    );
    let runner = SubprocessProver::new(config(dir.path(), refuses, Duration::from_secs(30)));
    match runner.prove(&request()).await {
        Err(ProverError::Refused(m)) => assert!(m.contains("invalid slots"), "{m}"),
        other => panic!("{other:?}"),
    }
    let crashes = script(
        dir.path(),
        "crashes",
        "cat > /dev/null\necho 'proving: out of memory' >&2\nexit 1",
    );
    let runner = SubprocessProver::new(config(dir.path(), crashes, Duration::from_secs(30)));
    assert!(matches!(
        runner.prove(&request()).await,
        Err(ProverError::Failed(_))
    ));
    // It hangs with a child of its own; both are gone after the timeout.
    let d = dir.path().display();
    let hangs = script(
        dir.path(),
        "hangs",
        &format!("cat > /dev/null\nsleep 30 &\necho $! > {d}/grandchild\nwait"),
    );
    // Long enough for a freshly written script to start.
    let runner = SubprocessProver::new(config(dir.path(), hangs, Duration::from_secs(3)));
    let outcome = runner.prove(&request()).await;
    assert!(
        matches!(outcome, Err(ProverError::Unavailable(_))),
        "{outcome:?}"
    );
    let pid: i32 = std::fs::read_to_string(dir.path().join("grandchild"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let alive = |pid: i32| {
        std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .is_ok_and(|o| {
                let stat = String::from_utf8_lossy(&o.stdout);
                !stat.trim().is_empty() && !stat.trim().starts_with('Z')
            })
    };
    let mut waited = 0;
    while alive(pid) && waited < 40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        waited += 1;
    }
    assert!(!alive(pid), "the prover's child outlived the step");
    let garbage = script(
        dir.path(),
        "garbage",
        "cat > /dev/null\necho not-a-response",
    );
    let runner = SubprocessProver::new(config(dir.path(), garbage, Duration::from_secs(30)));
    assert!(matches!(
        runner.prove(&request()).await,
        Err(ProverError::Failed(_))
    ));
    let missing = dir.path().join("no-such-prover");
    let runner = SubprocessProver::new(config(dir.path(), missing, Duration::from_secs(30)));
    assert!(matches!(
        runner.prove(&request()).await,
        Err(ProverError::Unavailable(_))
    ));
    // Every step's scratch directory is gone.
    let left: Vec<_> = std::fs::read_dir(dir.path().join("scratch"))
        .unwrap()
        .collect();
    assert!(left.is_empty(), "{left:?}");
}
