//! Process-local proof, never supplied by a transaction caller or serialized.
use super::*;
use crate::local_journal_acceptance::LinearBaseline;
use fluree_db_core::local_journal::{AcceptanceFrontier, AcceptanceView};

pub(super) struct ValidatedPrefix {
    frontier: AcceptanceFrontier,
    linear: Option<LinearBaseline>,
}
impl ValidatedPrefix {
    // The only production callers are full recovery installation and the
    // semantically validated, flushed state-installation hook. Owner health and
    // the cache gate prevent reuse before installation completes or after error.
    pub fn after_validation(
        frontier: AcceptanceFrontier,
        ledger: &str,
    ) -> fluree_db_core::local_journal::Result<Self> {
        let record = ns_record(ledger, frontier.head())?;
        let linear = record.commit_head_id.map(|id| LinearBaseline {
            id,
            t: record.commit_t,
        });
        Ok(Self { frontier, linear })
    }
    pub fn matches(&self, frontier: &AcceptanceFrontier) -> bool {
        self.frontier == *frontier
    }
    pub fn boundary(
        &self,
        view: &AcceptanceView<'_>,
    ) -> fluree_db_core::local_journal::Result<Option<&LinearBaseline>> {
        if view.frontier() != Some(&self.frontier) {
            return Err(JournalError::Invalid(
                "stale or foreign validated journal prefix",
            ));
        }
        Ok(self.linear.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::commit::codec::{read_commit, write_commit};

    async fn ledger() -> (tempfile::TempDir, JournalLedger) {
        let d = tempfile::tempdir().unwrap();
        let l = JournalLedger::initialize(d.path().into(), "prefix:main".into(), "g1".into())
            .await
            .unwrap();
        (d, l)
    }
    async fn records() -> (tempfile::TempDir, JournalLedger, Vec<Record>) {
        let (d, l) = ledger().await;
        for i in 1..=3 {
            l.transact(
                TxnType::Insert,
                &json!({"@id":format!("urn:node:{i}"),"urn:value":i}),
            )
            .await
            .unwrap();
        }
        let mut records = Vec::new();
        l.0.owner
            .recover_with_frontier(|rs, cp, _| {
                AdapterValidator {
                    proof: None,
                    prefix: None,
                }
                .validate_recovered_from(rs, cp.as_deref())?;
                records = rs.to_vec();
                Ok(())
            })
            .unwrap();
        (d, l, records)
    }
    fn apply(l: &JournalLedger, t: &Transition) {
        l.0.owner
            .accept_with(
                t,
                &AdapterValidator {
                    proof: None,
                    prefix: None,
                },
                |_, _| Ok(()),
            )
            .unwrap();
    }
    fn altered(t: &Transition, change: impl FnOnce(&mut fluree_db_core::Commit)) -> Transition {
        let mut t = t.clone();
        let head: Value = serde_json::from_slice(&t.resulting_head).unwrap();
        let old: ContentId = head["f:commitCid"].as_str().unwrap().parse().unwrap();
        let key = content_path(ContentKind::Commit, &t.ledger, &old.digest_hex());
        let o = t.objects.iter_mut().find(|o| o.key == key).unwrap();
        let mut commit = read_commit(&o.bytes).unwrap();
        change(&mut commit);
        o.bytes = write_commit(&commit, true, None).unwrap().bytes;
        let id = ContentId::new(ContentKind::Commit, &o.bytes);
        o.key = content_path(ContentKind::Commit, &t.ledger, &id.digest_hex());
        let mut head = head;
        head["f:commitCid"] = json!(id);
        t.resulting_head = serde_json::to_vec(&head).unwrap();
        t
    }
    #[tokio::test]
    async fn first_candidate_after_indexed_checkpoint_is_still_fully_checked() {
        let source = tempfile::tempdir().unwrap();
        let input_dir = tempfile::tempdir().unwrap();
        let input = input_dir.path().join("source.ttl");
        std::fs::write(&input, "<urn:one> <urn:value> 1 .").unwrap();
        let engine = FlureeBuilder::file(source.path().to_string_lossy().into_owned())
            .without_indexing()
            .build()
            .unwrap();
        engine
            .create("prefix:main")
            .import(&input)
            .threads(1)
            .memory_budget_mb(256)
            .execute()
            .await
            .unwrap();
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let producer = JournalLedger::bootstrap(
            a.path().into(),
            source.path().into(),
            "prefix:main".into(),
            "g1".into(),
        )
        .await
        .unwrap();
        let target = JournalLedger::bootstrap(
            b.path().into(),
            source.path().into(),
            "prefix:main".into(),
            "g1".into(),
        )
        .await
        .unwrap();
        producer
            .transact(TxnType::Insert, &json!({"@id":"urn:two","urn:value":2}))
            .await
            .unwrap();
        let mut records = Vec::new();
        producer
            .0
            .owner
            .recover_with_checkpoint(|rs, _| {
                records = rs.to_vec();
                Ok(())
            })
            .unwrap();
        let cache = target.ready().await.unwrap();
        let validator = AdapterValidator {
            proof: cache.proof.as_deref(),
            prefix: cache.prefix.as_deref(),
        };
        let valid = &records[0].transition;
        let frontier = target.0.owner.accepted_frontier().unwrap();
        for bad in [
            altered(valid, |c| c.parents.clear()),
            altered(valid, |c| c.t += 1),
        ] {
            assert!(target
                .0
                .owner
                .accept_with(&bad, &validator, |_, _| panic!(
                    "bad first checkpoint candidate installed"
                ))
                .is_err());
            assert_eq!(target.0.owner.accepted_frontier().unwrap(), frontier);
        }
        let mut bad = valid.clone();
        let mut head: Value = serde_json::from_slice(&bad.resulting_head).unwrap();
        head["f:ledgerIndex"] =
            json!({"f:cid":ContentId::new(ContentKind::IndexRoot,b"wrong"),"f:t":1});
        bad.resulting_head = serde_json::to_vec(&head).unwrap();
        assert!(target
            .0
            .owner
            .accept_with(&bad, &validator, |_, _| panic!(
                "changed checkpoint installed"
            ))
            .is_err());
        target
            .0
            .owner
            .accept_with(valid, &validator, |_, _| Ok(()))
            .unwrap();
        drop(cache);
        target.recover().await.unwrap();
        assert_eq!(target.head().await.unwrap().unwrap().t, 2);
    }
    #[tokio::test]
    async fn ordered_recovery_revalidates_every_record_and_discards_failed_passes() {
        let (_dir, _ledger, original) = records().await;
        let validator = AdapterValidator {
            proof: None,
            prefix: None,
        };
        validator.validate_recovered(&original).unwrap();
        assert!(validator.validate_recovered(&original[1..]).is_err());
        let mut reordered = original.clone();
        reordered.swap(0, 1);
        assert!(validator.validate_recovered(&reordered).is_err());
        for position in 0..original.len() {
            let valid = &original[position].transition;
            let mut missing_raw = valid.clone();
            missing_raw.objects.retain(|o| !o.key.contains("/txn/"));
            let mut damaged = valid.clone();
            damaged.objects[0].bytes.push(0);
            let mut changed_config = valid.clone();
            let mut head: Value = serde_json::from_slice(&valid.resulting_head).unwrap();
            head["f:configV"] = json!(1);
            changed_config.resulting_head = serde_json::to_vec(&head).unwrap();
            for bad in [
                missing_raw,
                damaged,
                changed_config,
                altered(valid, |c| c.parents.clear()),
                altered(valid, |c| c.t += 1),
                altered(valid, |c| {
                    c.flakes[0].g = Some(fluree_db_core::Sid::new(0, "urn:graph"))
                }),
            ] {
                let mut records = original.clone();
                records[position].transition = bad;
                // Genesis already has no parent, so clearing it is not corruption.
                if position == 0 && records[0].transition == original[0].transition {
                    continue;
                }
                assert!(
                    validator.validate_recovered(&records).is_err(),
                    "record {position}"
                );
                // The same adapter must start over, never reuse a partial failed pass.
                validator.validate_recovered(&original).unwrap();
            }
        }
        let mut delayed = original.clone();
        let raw = delayed[0]
            .transition
            .objects
            .iter()
            .position(|o| o.key.contains("/txn/"))
            .unwrap();
        let object = delayed[0].transition.objects.remove(raw);
        delayed.last_mut().unwrap().transition.objects.push(object);
        assert!(validator.validate_recovered(&delayed).is_err());
    }

    #[tokio::test]
    async fn stale_foreign_and_recovery_prefixes_fail_closed_and_ready_self_refreshes() {
        let (_source, source, rs) = records().await;
        let (_d, l) = ledger().await;
        apply(&l, &rs[0].transition);
        let stale = l.ready().await.unwrap().prefix.clone().unwrap();
        apply(&l, &rs[1].transition);
        let foreign = source.ready().await.unwrap().prefix.clone().unwrap();
        // Matching ledger, generation and expected head cannot substitute for the
        // root-specific journal digest. Rebuild a foreign proof at identical head.
        let (_fd, foreign_l) = ledger().await;
        for r in &rs[..2] {
            apply(&foreign_l, &r.transition);
        }
        let same_head_foreign = foreign_l.ready().await.unwrap().prefix.clone().unwrap();
        for p in [stale, foreign, same_head_foreign] {
            assert!(matches!(
                l.0.owner.accept_with(
                    &rs[2].transition,
                    &AdapterValidator {
                        proof: None,
                        prefix: Some(&p)
                    },
                    |_, _| panic!("invalid proof installed")
                ),
                Err(JournalError::Invalid(
                    "stale or foreign validated journal prefix"
                ))
            ));
        }
        // ready compares full frontier, then fully restores before staging.
        let valid = l.ready().await.unwrap().prefix.clone().unwrap();
        assert!(AdapterValidator {
            proof: None,
            prefix: Some(&valid)
        }
        .validate_recovered(&rs[..2])
        .is_err());
        AdapterValidator {
            proof: None,
            prefix: None,
        }
        .validate_recovered(&rs)
        .unwrap();
        l.transact(TxnType::Insert, &json!({"@id":"urn:next","urn:value":4}))
            .await
            .unwrap();
        assert_eq!(l.head().await.unwrap().unwrap().t, 3);
    }
    #[tokio::test]
    async fn prefix_reuse_still_validates_candidate_and_older_raw_dependencies() {
        let (_source, _l, rs) = records().await;
        let (dir, l) = ledger().await;
        for r in &rs[..2] {
            apply(&l, &r.transition);
        }
        let prefix = l.ready().await.unwrap().prefix.clone().unwrap();
        let t = &rs[2].transition;
        let mut bad_head = t.clone();
        let mut h: Value = serde_json::from_slice(&t.resulting_head).unwrap();
        h["f:configV"] = json!(1);
        bad_head.resulting_head = serde_json::to_vec(&h).unwrap();
        let mut bad_index = t.clone();
        h = serde_json::from_slice(&t.resulting_head).unwrap();
        h["f:ledgerIndex"] =
            json!({"f:cid":ContentId::new(ContentKind::IndexRoot,b"unsupported"),"f:t":1});
        bad_index.resulting_head = serde_json::to_vec(&h).unwrap();
        let mut missing = altered(t, |c| {
            c.txn = Some(ContentId::new(ContentKind::Txn, b"missing"))
        });
        missing.objects.retain(|o| !o.key.contains("/txn/"));
        let bad_parent = altered(t, |c| c.parents.clear());
        let bad_time = altered(t, |c| c.t += 1);
        let bad_graph = altered(t, |c| {
            c.flakes[0].g = Some(fluree_db_core::Sid::new(0, "urn:graph"))
        });
        let mut bad_bytes = t.clone();
        bad_bytes.objects[0].bytes.push(0);
        let mut extra = t.clone();
        extra.objects.push(
            rs[0]
                .transition
                .objects
                .iter()
                .find(|o| read_commit(&o.bytes).is_ok())
                .unwrap()
                .clone(),
        );
        let path = dir.path().join(".fluree-wal/journal");
        let length = std::fs::metadata(&path).unwrap().len();
        let frontier = l.0.owner.accepted_frontier().unwrap();
        for bad in [
            bad_head, bad_index, missing, bad_parent, bad_time, bad_graph, bad_bytes, extra,
        ] {
            assert!(l
                .0
                .owner
                .accept_with(
                    &bad,
                    &AdapterValidator {
                        proof: None,
                        prefix: Some(&prefix)
                    },
                    |_, _| panic!("bad candidate installed")
                )
                .is_err());
            assert_eq!(l.0.owner.accepted_frontier().unwrap(), frontier);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
        }
        // A candidate may reference valid raw content from the accepted prefix.
        // Its CID still gets checked even though old commit bodies are skipped.
        let first = rs[0]
            .transition
            .objects
            .iter()
            .find_map(|o| read_commit(&o.bytes).ok())
            .unwrap();
        let old_raw = first.txn.unwrap();
        let mut reused = altered(t, |c| c.txn = Some(old_raw.clone()));
        let raw_key = content_path(ContentKind::Txn, &t.ledger, &old_raw.digest_hex());
        reused
            .objects
            .retain(|o| read_commit(&o.bytes).is_ok() || o.key == raw_key);
        l.0.owner
            .accept_with(
                &reused,
                &AdapterValidator {
                    proof: None,
                    prefix: Some(&prefix),
                },
                |_, _| Ok(()),
            )
            .unwrap();
        l.recover().await.unwrap();
        assert_eq!(l.head().await.unwrap().unwrap().t, 3);
    }
}
