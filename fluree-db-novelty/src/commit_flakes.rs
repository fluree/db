//! Commit metadata flakes generation
//!
//! This module generates commit metadata flakes.
//! These flakes are intended to be indexed alongside transaction flakes,
//! and must be reproducible during ledger load/replay (not only at commit time).
//!
//! # Flake Structure
//!
//! Each commit generates 7-10 flakes:
//!
//! **Commit subject flakes** (subject = commit CID digest hex):
//! - `db#address` - commit CID string (xsd:string)
//! - `db#alias` - ledger alias (xsd:string)
//! - `db#time` - timestamp in epoch ms (xsd:long)
//! - `db#t` - transaction number (xsd:long)
//! - `db#asserts` - number of assertions in this commit (xsd:long)
//! - `db#retracts` - number of retractions in this commit (xsd:long)
//! - `db#size` - data size in bytes of this commit's flakes (xsd:long)
//! - `db#previous` - reference to previous commit (@id, optional)
//! - `db#author` - transaction signer DID (xsd:string, optional)
//! - `db#txn` - transaction CID string (xsd:string, optional)

use chrono::DateTime;
use fluree_db_core::{Flake, FlakeValue, Sid};
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB, JSON_LD, XSD};
use fluree_vocab::{db, xsd_names};

use crate::Commit;

/// Stamp commit metadata flakes with a txn-meta graph SID.
///
/// Commit metadata flakes are identified by having no graph (`g: None`)
/// and a subject in the `FLUREE_COMMIT` namespace. This function mutates
/// them in-place to route them to the txn-meta graph instead of the
/// default graph.
pub fn stamp_graph_on_commit_flakes(flakes: &mut [Flake], graph_sid: &Sid) {
    for flake in flakes.iter_mut() {
        if flake.g.is_none() && flake.s.namespace_code == FLUREE_COMMIT {
            flake.g = Some(graph_sid.clone());
        }
    }
}

/// Parse ISO-8601 timestamp to epoch milliseconds
///
/// Falls back to 0 if parsing fails.
fn iso_to_epoch_ms(iso: &str) -> i64 {
    DateTime::parse_from_rfc3339(iso)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

/// Generate commit metadata flakes for a commit.
///
/// This function creates flakes that represent commit metadata in the index,
/// enabling efficient queries for commit information and CID-based time travel.
///
/// The commit subject SID uses the CID's SHA-256 digest hex as the local name
/// within the `FLUREE_COMMIT` namespace, preserving backward compatibility with
/// SHA-based time travel queries.
///
/// ## Important
/// This must be safe to call during ledger load/replay. If a commit is missing
/// an ID (not yet hashed), this function returns an empty vector.
pub fn generate_commit_flakes(commit: &Commit, ledger_id: &str, t: i64) -> Vec<Flake> {
    let Some(commit_id) = &commit.id else {
        return Vec::new();
    };
    let hex = commit_id.digest_hex();

    let mut flakes = Vec::with_capacity(10);

    // Build commit subject SID using CID digest hex
    let commit_sid = Sid::new(FLUREE_COMMIT, &hex);

    // Datatype SIDs
    let string_dt = Sid::new(XSD, xsd_names::STRING);
    let long_dt = Sid::new(XSD, xsd_names::LONG);
    let ref_dt = Sid::new(JSON_LD, "id"); // Reference datatype

    // === Commit subject flakes ===

    // 1. db#address (CID string — replaces legacy storage address)
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_DB, db::ADDRESS),
        FlakeValue::String(commit_id.to_string()),
        string_dt.clone(),
        t,
        true,
        None,
    ));

    // 2. db#alias
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_DB, db::ALIAS),
        FlakeValue::String(ledger_id.to_string()),
        string_dt.clone(),
        t,
        true,
        None,
    ));

    // 3. db#time (timestamp as epoch milliseconds)
    if let Some(time_str) = &commit.time {
        let epoch_ms = iso_to_epoch_ms(time_str);
        flakes.push(Flake::new(
            commit_sid.clone(),
            Sid::new(FLUREE_DB, db::TIME),
            FlakeValue::Long(epoch_ms),
            long_dt.clone(),
            t,
            true,
            None,
        ));
    }

    // 4. db#t (transaction number)
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_DB, db::T),
        FlakeValue::Long(commit.t),
        long_dt.clone(),
        t,
        true,
        None,
    ));

    // 5. db#asserts (count of asserted flakes)
    let asserts = commit.flakes.iter().filter(|f| f.op).count() as i64;
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_DB, db::ASSERTS),
        FlakeValue::Long(asserts),
        long_dt.clone(),
        t,
        true,
        None,
    ));

    // 6. db#retracts (count of retracted flakes)
    let retracts = commit.flakes.iter().filter(|f| !f.op).count() as i64;
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_DB, db::RETRACTS),
        FlakeValue::Long(retracts),
        long_dt.clone(),
        t,
        true,
        None,
    ));

    // 7. db#size (total byte size of commit flakes)
    let size: i64 = commit.flakes.iter().map(|f| f.size_bytes() as i64).sum();
    flakes.push(Flake::new(
        commit_sid.clone(),
        Sid::new(FLUREE_DB, db::SIZE),
        FlakeValue::Long(size),
        long_dt.clone(),
        t,
        true,
        None,
    ));

    // 8. db#previous (reference to parent commits — one per parent)
    for prev_id in commit.parent_ids() {
        let prev_hex = prev_id.digest_hex();
        let prev_sid = Sid::new(FLUREE_COMMIT, &prev_hex);
        flakes.push(Flake::new(
            commit_sid.clone(),
            Sid::new(FLUREE_DB, db::PREVIOUS),
            FlakeValue::Ref(prev_sid),
            ref_dt.clone(),
            t,
            true,
            None,
        ));
    }

    // 9. db#txn (optional: transaction CID string)
    if let Some(txn_id) = &commit.txn {
        flakes.push(Flake::new(
            commit_sid.clone(),
            Sid::new(FLUREE_DB, db::TXN),
            FlakeValue::String(txn_id.to_string()),
            string_dt.clone(),
            t,
            true,
            None,
        ));
    }

    // 11+. txn_meta entries (user-provided and system-generated metadata)
    for entry in &commit.txn_meta {
        let pred_sid = Sid::new(entry.predicate_ns, &entry.predicate_name);
        let (value, dt) = match &entry.value {
            crate::TxnMetaValue::String(s) => (FlakeValue::String(s.clone()), string_dt.clone()),
            crate::TxnMetaValue::TypedLiteral {
                value,
                dt_ns,
                dt_name,
            } => {
                let dt_sid = Sid::new(*dt_ns, dt_name);
                (FlakeValue::String(value.clone()), dt_sid)
            }
            crate::TxnMetaValue::LangString { value, lang: _ } => {
                (FlakeValue::String(value.clone()), string_dt.clone())
            }
            crate::TxnMetaValue::Ref { ns, name } => {
                (FlakeValue::Ref(Sid::new(*ns, name)), ref_dt.clone())
            }
            crate::TxnMetaValue::Long(n) => (FlakeValue::Long(*n), long_dt.clone()),
            crate::TxnMetaValue::Double(n) => {
                (FlakeValue::Double(*n), Sid::new(XSD, xsd_names::DOUBLE))
            }
            crate::TxnMetaValue::Boolean(b) => {
                (FlakeValue::Boolean(*b), Sid::new(XSD, xsd_names::BOOLEAN))
            }
        };
        flakes.push(Flake::new(
            commit_sid.clone(),
            pred_sid,
            value,
            dt,
            t,
            true,
            None,
        ));
    }

    flakes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommitRef;
    use fluree_db_core::{ContentId, ContentKind};

    fn make_test_content_id(kind: ContentKind, label: &str) -> ContentId {
        ContentId::new(kind, label.as_bytes())
    }

    fn make_test_commit(with_previous: bool) -> Commit {
        let commit_id = make_test_content_id(ContentKind::Commit, "test-commit-bytes");
        let mut commit = Commit::new(5, vec![]);
        commit.id = Some(commit_id);
        commit.time = Some("2025-01-20T12:00:00Z".to_string());

        if with_previous {
            let prev_id = make_test_content_id(ContentKind::Commit, "prev-commit-bytes");
            commit.previous_refs = vec![CommitRef::new(prev_id)];
        }

        commit
    }

    #[test]
    fn test_generate_commit_flakes_basic() {
        let commit = make_test_commit(false);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Should have 7 flakes (address, alias, time, t, asserts, retracts, size — no previous)
        assert_eq!(flakes.len(), 7);

        // Check commit subject uses correct namespace
        let commit_flake = &flakes[0];
        assert_eq!(commit_flake.s.namespace_code, FLUREE_COMMIT);
        // Name should be hex digest
        assert!(
            commit_flake.s.name.chars().all(|c| c.is_ascii_hexdigit()),
            "SID name should be hex digest, got: {}",
            commit_flake.s.name
        );
    }

    #[test]
    fn test_generate_commit_flakes_with_previous() {
        let commit = make_test_commit(true);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Should have 8 flakes (includes db#previous)
        assert_eq!(flakes.len(), 8);

        // Find the previous flake
        let prev_flake = flakes
            .iter()
            .find(|f| f.p.namespace_code == FLUREE_DB && f.p.name.as_ref() == db::PREVIOUS);
        assert!(prev_flake.is_some(), "Should have db#previous flake");

        let prev_flake = prev_flake.unwrap();
        // Verify it's a ref with correct datatype
        assert!(
            matches!(&prev_flake.o, FlakeValue::Ref(_)),
            "Previous should be a Ref"
        );
        assert_eq!(prev_flake.dt.namespace_code, JSON_LD);
        assert_eq!(prev_flake.dt.name.as_ref(), "id");
    }

    #[test]
    fn test_ref_flakes_have_correct_datatype() {
        let commit = make_test_commit(false);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        let ref_flakes: Vec<&Flake> = flakes
            .iter()
            .filter(|f| matches!(&f.o, FlakeValue::Ref(_)))
            .collect();
        assert!(
            ref_flakes.is_empty(),
            "no-previous commit should have no Ref metadata flakes"
        );
    }

    #[test]
    fn test_commit_sid_uses_digest_hex() {
        let commit_id = make_test_content_id(ContentKind::Commit, "test-data");
        let expected_hex = commit_id.digest_hex();

        let mut commit = Commit::new(1, vec![]);
        commit.id = Some(commit_id);

        let flakes = generate_commit_flakes(&commit, "test:main", 1);
        let commit_flake = &flakes[0];

        assert_eq!(commit_flake.s.name.as_ref(), expected_hex.as_str());
    }

    #[test]
    fn test_iso_to_epoch_ms() {
        let epoch_ms = iso_to_epoch_ms("2025-01-20T12:00:00Z");
        assert!(epoch_ms > 0);
        assert!(epoch_ms > 1_737_000_000_000);
        assert!(epoch_ms < 1_738_000_000_000);

        let invalid_ms = iso_to_epoch_ms("not-a-date");
        assert_eq!(invalid_ms, 0);
    }

    #[test]
    fn test_commit_subject_flakes_include_db_metadata() {
        let commit = make_test_commit(false);
        let flakes = generate_commit_flakes(&commit, "test:main", 5);

        // Find db#t flake
        let t_flake = flakes
            .iter()
            .find(|f| f.p.namespace_code == FLUREE_DB && f.p.name.as_ref() == db::T);
        assert!(t_flake.is_some());
        let t_flake = t_flake.unwrap();
        assert_eq!(t_flake.s.namespace_code, FLUREE_COMMIT);
        assert!(matches!(&t_flake.o, FlakeValue::Long(5)));
    }

    #[test]
    fn test_missing_id_is_safe() {
        let commit = Commit::new(1, vec![]);
        let flakes = generate_commit_flakes(&commit, "test:main", 1);
        assert!(
            flakes.is_empty(),
            "missing commit.id should yield no metadata flakes"
        );
    }
}
