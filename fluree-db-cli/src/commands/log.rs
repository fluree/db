use crate::context;
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::{CommitSummary, ContentId};

/// How many commits `fluree log` shows when the invocation does not say.
///
/// Matches `LOG_DEFAULT_LIMIT` in `fluree-db-server/src/routes/log.rs`, which
/// the auto-routed and `--remote` lanes already applied. `--direct` did not, so
/// the same command answered differently depending on whether a local server
/// happened to be running, and only one of the two could exhaust memory.
///
/// What the limit does *not* buy: the chain is still walked in full.
/// `walk_commit_summaries` opens with `collect_dag_cids`, which loads a commit
/// *envelope* (a byte-range read) for every commit in the DAG before anything
/// can be ordered by `t`. The limit bounds the full-blob `load_commit_by_id`
/// calls and the summaries retained, not the walk. Bounding the walk means
/// switching to the first-parent lineage, which changes *which* commits appear
/// across a merge — a product decision, deliberately not taken here.
const DEFAULT_LOG_LIMIT: usize = 100;

/// How many characters of the hex digest identify a commit on screen.
///
/// 48 bits, the prefix width the rest of the repo already uses to address a
/// commit (`fluree-db-api/tests/it_graph_commit.rs`), and comfortably above
/// `COMMIT_PREFIX_MIN_LEN`, the floor both prefix resolvers enforce — which is
/// the binding constraint, since an id printed below it cannot be pasted back
/// in at all.
///
/// Fixed rather than derived from the ids being displayed. A screen-derived
/// width has to see every row before it can print the first, and it answers the
/// wrong question besides: an id unique among the rows on screen can still be
/// ambiguous ledger-wide, and both resolvers reject an ambiguous prefix.
const ABBREV_LEN: usize = 12;

/// The width has to clear the resolvers' floor, or this output cannot be pasted
/// back into `fluree show` / `history --to` / `@commit:` at all.
///
/// A compile error rather than a test, because it is the one property of the
/// width that nothing else can catch: a width that separates the ids on screen
/// and is still unusable looks correct from every other angle. Seven is git's
/// own comfort level for a content-addressed id and the floor is six, so the
/// second bound is the one that actually bites.
const _: () = assert!(
    ABBREV_LEN >= fluree_db_api::COMMIT_PREFIX_MIN_LEN && ABBREV_LEN >= 7,
    "fluree log's abbreviation must clear the commit resolvers' minimum prefix length"
);

pub async fn run(
    ledger: Option<&str>,
    oneline: bool,
    count: Option<usize>,
    all: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    // `--all` on a remote lane is still bounded by the server's hard cap; the
    // truncation line reports what was actually returned.
    let limit = if all {
        None
    } else {
        Some(count.unwrap_or(DEFAULT_LOG_LIMIT))
    };

    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let ledger_id = context::to_ledger_id(&alias)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = run_remote(&ledger_id, oneline, limit, &client).await;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let alias = context::resolve_ledger(ledger, dirs)?;
            let ledger_id = context::to_ledger_id(&alias)?;
            let result = run_remote(&ledger_id, oneline, limit, &client).await;
            context::persist_refreshed_tokens(&client, context::LOCAL_SERVER_REMOTE, dirs).await;
            return result;
        }
    }

    run_local(ledger, oneline, limit, dirs).await
}

async fn run_remote(
    alias: &str,
    oneline: bool,
    limit: Option<usize>,
    client: &RemoteLedgerClient,
) -> CliResult<()> {
    // `None` here would mean "server default", which is the opposite of what
    // `--all` asks for, so ask for everything and let the server's hard cap
    // decide. `truncated` then reports honestly what came back.
    let response = client
        .commit_log(alias, Some(limit.unwrap_or(usize::MAX)))
        .await
        .map_err(|e| CliError::Remote(format!("failed to fetch log for '{alias}': {e}")))?;

    let commits = response
        .get("commits")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::Remote("unexpected log response: missing commits array".into()))?;

    if commits.is_empty() {
        println!("No commits found for ledger '{alias}'");
        return Ok(());
    }

    for commit in commits {
        let t = commit
            .get("t")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let commit_id = commit
            .get("commit_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let time = commit.get("time").and_then(|v| v.as_str()).unwrap_or("");
        let asserts = commit
            .get("asserts")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let retracts = commit
            .get("retracts")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let flake_count = commit
            .get("flake_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(asserts + retracts);

        print_commit(t, &short_id(commit_id), time, flake_count as usize, oneline);
    }

    if let Some(true) = response
        .get("truncated")
        .and_then(serde_json::Value::as_bool)
    {
        let total = response
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as usize;
        print_truncation(commits.len(), total, limit);
    }

    Ok(())
}

async fn run_local(
    ledger: Option<&str>,
    oneline: bool,
    limit: Option<usize>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for tracked ledger — log requires local commit chain access
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let alias = context::resolve_ledger(ledger, dirs)?;
    if store.get_tracked(&context::to_ledger_id(&alias)?).is_some() {
        return Err(CliError::Usage(
            "commit log is not available for tracked ledgers (no local commit chain).\n  \
             Use `fluree track status` to check remote state instead, or pass `--remote <name>`."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias)?;

    // The same call the server's `GET /v1/fluree/log` makes, so `--direct` and
    // the auto-routed lane answer identically. It owns the nameservice lookup,
    // the branch-aware store that lets the walk cross fork points, and the
    // per-commit summary — including `total`, which the hand-rolled walk this
    // replaced had no way to report.
    let (summaries, total) = fluree
        .commit_log(&ledger_id, limit)
        .await
        .map_err(|e| match e {
            fluree_db_api::ApiError::NotFound(_) => {
                CliError::NotFound(format!("ledger '{alias}' not found"))
            }
            other => CliError::from(other),
        })?;

    if summaries.is_empty() {
        println!("No commits found for ledger '{alias}'");
        return Ok(());
    }

    for summary in &summaries {
        print_summary(summary, oneline);
    }

    if summaries.len() < total {
        print_truncation(summaries.len(), total, limit);
    }

    Ok(())
}

fn print_summary(summary: &CommitSummary, oneline: bool) {
    print_commit(
        summary.t,
        &short_id(&summary.commit_id.to_string()),
        summary.time.as_deref().unwrap_or(""),
        summary.flake_count,
        oneline,
    );
}

fn print_commit(t: i64, short: &str, time: &str, flake_count: usize, oneline: bool) {
    if oneline {
        // Note: commit messages are not currently persisted in the commit
        // format, so we show the timestamp instead.
        println!("t={t:<4}  {short}  {time}");
    } else {
        println!("commit {short}");
        if !time.is_empty() {
            println!("Date:    {time}");
        }
        println!("t:       {t}");
        println!("Flakes:  {flake_count}");
        println!();
    }
}

fn print_truncation(shown: usize, total: usize, limit: Option<usize>) {
    // `--all` sends no limit, and the server still applies its own hard cap.
    // Telling that caller to "pass --all" is advice to repeat what they just
    // did; the honest line is that the cap is the server's.
    match limit {
        Some(_) => {
            eprintln!("(showing {shown} of {total} commits — pass -n to widen, or --all)");
        }
        None => eprintln!(
            "(showing {shown} of {total} commits — the server caps a single response; \
             narrow the range or page with -n)"
        ),
    }
}

/// The on-screen id for a commit: the first [`ABBREV_LEN`] hex digest characters.
fn short_id(address: &str) -> String {
    let hex = commit_id_of(address);
    hex.chars().take(ABBREV_LEN).collect()
}

/// The commit's hex digest, from whichever spelling of its address we were given.
///
/// A commit is addressed two ways and they are not interchangeable. `ContentId`
/// *displays* as a base32 CIDv1 — that is the JSON API wire format and the
/// `db:address` flake value — but the indexed commit *subject* is
/// `Sid::new(FLUREE_COMMIT, cid.digest_hex())`, the on-disk blob path is hex,
/// and the `#txn-meta` IRI is `fluree:commit:sha256:<hex>`. Both prefix
/// resolvers therefore scan hex, so hex is the only spelling that can be pasted
/// from this output into `fluree show`, `--at`, or `@commit:`.
///
/// Base32 is also the wrong thing to abbreviate: the first twelve characters of
/// every commit CID are a constant header, so a thirteen-character abbreviation
/// carries four bits — less than one hex nibble — and seventeen are needed to
/// clear a floor hex clears at six.
///
/// Handles:
/// - a base32 CID, as the server returns in JSON — decoded to its hex digest
/// - `fluree:file://ledger/main/commit/<hex>.fcv2` — already hex
/// - `fluree:commit:sha256:<hex>` — already hex
/// - a plain hex string
fn commit_id_of(address: &str) -> String {
    // `parse_canonical`, not `parse`: multibase reads a leading `f` as base16,
    // so a bare hex digest can decode into a different CID entirely.
    if let Some(cid) = ContentId::parse_canonical(address) {
        return cid.digest_hex();
    }

    // Path-style addresses (e.g., .../commit/<hash>.fcv2)
    if let Some(pos) = address.rfind("/commit/") {
        let after = &address[pos + 8..];
        return after
            .strip_suffix(".fcv2")
            .or_else(|| after.strip_suffix(".json"))
            .unwrap_or(after)
            .to_string();
    }

    // `sha256:` prefix style
    if let Some(pos) = address.find("sha256:") {
        return address[pos + 7..].to_string();
    }

    // Fallback: the last path segment, which for an address with no `/` is
    // the whole string — `rsplit` always yields at least one item, so there is
    // no separate "or the address itself" case to handle.
    let last = address.rsplit('/').next().unwrap_or(address);
    last.strip_suffix(".fcv2")
        .or_else(|| last.strip_suffix(".json"))
        .unwrap_or(last)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::{commit_id_of, short_id, ABBREV_LEN};
    use fluree_db_core::{ContentId, ContentKind};

    fn real_commit_id(payload: &[u8]) -> ContentId {
        ContentId::new(ContentKind::Commit, payload)
    }

    /// One commit is enough to print a usable id.
    ///
    /// This is the case a width computed from the ids on screen cannot serve:
    /// with nothing to separate from, it returns its floor, and the floor of a
    /// base32 CID is inside the constant header.
    #[test]
    fn a_single_commit_still_gets_a_distinguishing_id() {
        let cid = real_commit_id(b"the only commit");
        let short = short_id(&cid.to_string());

        assert_eq!(short.len(), ABBREV_LEN);
        assert!(
            cid.digest_hex().starts_with(&short),
            "the printed id must be a prefix of the digest the resolvers scan: \
             short={short} digest={}",
            cid.digest_hex()
        );
        assert!(
            !short.starts_with("bagaybq"),
            "the printed id must not be the CID header: {short}"
        );
        assert!(
            short.chars().all(|c| c.is_ascii_hexdigit()),
            "the printed id must be hex, the only spelling the resolvers accept: {short}"
        );
    }

    /// Two commits print two different ids — and both are pasteable.
    #[test]
    fn two_commits_print_distinct_pasteable_ids() {
        let a = real_commit_id(b"commit a");
        let b = real_commit_id(b"commit b");

        let short_a = short_id(&a.to_string());
        let short_b = short_id(&b.to_string());

        assert_ne!(short_a, short_b, "distinct commits must print distinct ids");
        assert!(a.digest_hex().starts_with(&short_a));
        assert!(b.digest_hex().starts_with(&short_b));
        assert!(!short_a.starts_with("bagaybq"));
        assert!(!short_b.starts_with("bagaybq"));
    }

    /// Base32 abbreviation carries no digest, which is why this prints hex.
    ///
    /// A CIDv1 opens with 7 header bytes. Base32 encodes the payload five bits
    /// to a character after the single `b` multibase marker, so characters
    /// 2..=12 are 55 header bits, character 13 is the last header bit plus the
    /// digest's first four — sixteen possible values — and only character 14
    /// begins to carry the digest properly.
    ///
    /// Pins the constant `normalize_commit_ref` keys its "that is an
    /// abbreviated CID" diagnostic on, against ids the system actually mints.
    #[test]
    fn commit_cid_prefix_is_constant() {
        let ids: Vec<String> = (0..17u8)
            .map(|i| real_commit_id(&[i]).to_string())
            .collect();

        for id in &ids {
            assert!(
                id.starts_with("bagaybqabciq"),
                "the first twelve characters are header, identical for every commit: {id}"
            );
        }

        // Seventeen commits into sixteen possible values: a collision at
        // thirteen characters is arithmetic, not luck.
        let thirteen: std::collections::HashSet<&str> = ids.iter().map(|id| &id[..13]).collect();
        assert!(
            thirteen.len() < ids.len(),
            "thirteen base32 characters carry four bits of digest, so they \
             cannot separate seventeen commits: {thirteen:?}"
        );

        // The same seventeen at twelve hex characters, which is what we print.
        let hex: std::collections::HashSet<String> = (0..17u8)
            .map(|i| short_id(&real_commit_id(&[i]).to_string()))
            .collect();
        assert_eq!(hex.len(), ids.len(), "48 bits of digest separates them all");
    }

    /// Every address spelling reduces to the same hex digest.
    #[test]
    fn commit_id_of_normalizes_every_address_spelling() {
        let cid = real_commit_id(b"address spellings");
        let hex = cid.digest_hex();

        assert_eq!(commit_id_of(&cid.to_string()), hex, "bare base32 CID");
        assert_eq!(
            commit_id_of(&format!("fluree:file://claims/main/commit/{hex}.fcv2")),
            hex,
            "blob path, which content_address builds from digest_hex"
        );
        assert_eq!(
            commit_id_of(&format!("fluree:commit:sha256:{hex}")),
            hex,
            "the #txn-meta IRI form"
        );
        assert_eq!(commit_id_of(&hex), hex, "already hex");
    }
}
