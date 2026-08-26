use crate::context;
use crate::error::{CliError, CliResult, EXIT_VERIFY_CHAIN, EXIT_VERIFY_PROVENANCE};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{VerifyProblem, VerifySeverity};

pub async fn run(
    ledger: Option<&str>,
    limit: Option<usize>,
    json: bool,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let alias = context::resolve_ledger(ledger, dirs)?;
    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    let report = fluree.verify_ledger(&ledger_id, limit).await.map_err(|e| {
        if e.is_not_found() {
            CliError::NotFound(format!("ledger '{alias}' not found"))
        } else {
            CliError::Config(format!("verify failed: {e}"))
        }
    })?;

    let severity = report.severity();

    if json {
        // `severity` is derived from `problems`, so it is attached here
        // rather than stored on the report — one source of truth.
        let mut value = serde_json::to_value(&report)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert("severity".to_string(), serde_json::to_value(severity)?);
        }
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!("Ledger:   {}", report.ledger_id);
        println!(
            "Head:     t={} {}",
            report.head_t,
            report
                .head_commit_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "(none)".to_string())
        );
        println!(
            "Index:    t={} {}",
            report.index_t,
            report
                .index_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "(none)".to_string())
        );
        println!(
            "Checked:  {} commit(s), {} txn reference(s){}",
            report.commits_checked,
            report.txn_refs_checked,
            if report.truncated {
                " (truncated by --limit)"
            } else {
                ""
            }
        );
        if report.problems.is_empty() {
            println!("Result:   OK");
        } else {
            println!("Result:   {} problem(s)", report.problems.len());
            for p in &report.problems {
                println!("  - {}", describe(p));
            }
        }
    }

    // Typed exits so a caller can gate on what actually broke: a
    // provenance gap still clones (this is the case the tolerant read
    // paths were built for), a broken chain does not. Exit 1 stays
    // reserved for "verify could not run".
    match severity {
        VerifySeverity::Healthy => Ok(()),
        VerifySeverity::Provenance => {
            if !json {
                eprintln!(
                    "ledger '{alias}': provenance gaps only — state and replication are intact"
                );
            }
            Err(CliError::ExitCode(EXIT_VERIFY_PROVENANCE))
        }
        VerifySeverity::Chain => {
            if !json {
                eprintln!("ledger '{alias}': commit chain or index root is broken");
            }
            Err(CliError::ExitCode(EXIT_VERIFY_CHAIN))
        }
    }
}

fn describe(p: &VerifyProblem) -> String {
    match p {
        VerifyProblem::MissingCommit {
            commit_id,
            referenced_by,
            referenced_by_t,
        } => format!(
            "missing commit {commit_id} (parent of t={referenced_by_t} {referenced_by}); chain is broken below this point"
        ),
        VerifyProblem::MissingHead { commit_id } => format!(
            "missing head commit {commit_id}; the nameservice points at a commit that is not in storage"
        ),
        VerifyProblem::UnreadableCommit { commit_id, error } => {
            format!("unreadable commit {commit_id}: {error}")
        }
        VerifyProblem::MissingTxnBlob {
            t,
            commit_id,
            txn_id,
        } => format!(
            "missing txn blob {txn_id} referenced by commit t={t} {commit_id}; state is intact, provenance for this commit is lost"
        ),
        VerifyProblem::TGap {
            commit_id,
            t,
            parent_id,
            parent_t,
        } => format!("t gap: commit t={t} {commit_id} has primary parent t={parent_t} {parent_id}"),
        VerifyProblem::MissingIndexRoot { index_id, index_t } => {
            format!("missing index root {index_id} (index_t={index_t}); reindex to repair")
        }
    }
}
