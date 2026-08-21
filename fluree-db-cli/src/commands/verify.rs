use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::VerifyProblem;

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

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
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

    if report.is_healthy() {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "ledger '{alias}' has {} integrity problem(s)",
            report.problems.len()
        )))
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
