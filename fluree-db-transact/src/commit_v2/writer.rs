//! Commit v2 writer: Commit -> binary blob.
//!
//! The writer implementation lives in `fluree_db_core::commit::codec::writer`.
//! This module re-exports the public API for backward compatibility.

pub use fluree_db_core::commit::codec::{write_commit, CommitWriteResult};

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::commit::codec::{read_commit, read_commit_envelope, MAGIC};
    use fluree_db_core::{ContentId, ContentKind, Flake, FlakeMeta, FlakeValue, Sid};
    use fluree_db_novelty::Commit;
    use std::collections::HashMap;

    fn make_test_commit(flakes: Vec<Flake>, t: i64) -> Commit {
        Commit::new(t, flakes)
    }

    fn assert_flake_eq(a: &Flake, b: &Flake) {
        assert_eq!(a.s.namespace_code, b.s.namespace_code, "s namespace_code");
        assert_eq!(a.s.name.as_ref(), b.s.name.as_ref(), "s name");
        assert_eq!(a.p.namespace_code, b.p.namespace_code, "p namespace_code");
        assert_eq!(a.p.name.as_ref(), b.p.name.as_ref(), "p name");
        assert_eq!(
            a.dt.namespace_code, b.dt.namespace_code,
            "dt namespace_code"
        );
        assert_eq!(a.dt.name.as_ref(), b.dt.name.as_ref(), "dt name");
        assert_eq!(a.o, b.o, "object value");
        assert_eq!(a.op, b.op, "op flag");
        assert_eq!(a.t, b.t, "t");
        match (&a.m, &b.m) {
            (None, None) => {}
            (Some(am), Some(bm)) => {
                assert_eq!(am.lang, bm.lang, "meta lang");
                assert_eq!(am.i, bm.i, "meta i");
            }
            _ => panic!("meta mismatch: {:?} vs {:?}", a.m, b.m),
        }
    }

    #[test]
    fn test_round_trip_basic() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice Smith".to_string()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "age"),
                FlakeValue::Long(30),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
        ];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false, None).unwrap();
        assert!(!result.bytes.is_empty());
        assert_eq!(&result.bytes[0..4], &MAGIC);

        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.t, 1);
        assert_eq!(decoded.flakes.len(), 2);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_round_trip_with_compression() {
        let flakes: Vec<Flake> = (0..100)
            .map(|i| {
                Flake::new(
                    Sid::new(101, format!("node{i}")),
                    Sid::new(101, "value"),
                    FlakeValue::Long(i),
                    Sid::new(2, "integer"),
                    5,
                    true,
                    None,
                )
            })
            .collect();

        let commit = make_test_commit(flakes, 5);
        let result_c = write_commit(&commit, true, None).unwrap();
        let result_u = write_commit(&commit, false, None).unwrap();

        assert!(
            result_c.bytes.len() < result_u.bytes.len(),
            "compressed {} should be < uncompressed {}",
            result_c.bytes.len(),
            result_u.bytes.len()
        );
        // Compressed and uncompressed blobs should differ
        assert_ne!(result_c.bytes, result_u.bytes);

        let dec_c = read_commit(&result_c.bytes).unwrap();
        let dec_u = read_commit(&result_u.bytes).unwrap();
        assert_eq!(dec_c.flakes.len(), 100);
        assert_eq!(dec_u.flakes.len(), 100);
        for (orig, dec) in commit.flakes.iter().zip(dec_c.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_round_trip_ref_values() {
        let flakes = vec![Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "knows"),
            FlakeValue::Ref(Sid::new(101, "Bob")),
            Sid::new(1, "id"),
            1,
            true,
            None,
        )];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false, None).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        match &decoded.flakes[0].o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, 101);
                assert_eq!(sid.name.as_ref(), "Bob");
            }
            other => panic!("expected Ref, got {other:?}"),
        }
    }

    #[test]
    fn test_round_trip_mixed_value_types() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "str"),
                FlakeValue::String("hello".into()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "num"),
                FlakeValue::Long(-42),
                Sid::new(2, "long"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "dbl"),
                FlakeValue::Double(3.13),
                Sid::new(2, "double"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "flag"),
                FlakeValue::Boolean(true),
                Sid::new(2, "boolean"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "empty"),
                FlakeValue::Null,
                Sid::new(2, "string"),
                1,
                false,
                None,
            ),
        ];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false, None).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.flakes.len(), 5);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_round_trip_with_metadata() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice".into()),
                Sid::new(3, "langString"),
                1,
                true,
                Some(FlakeMeta::with_lang("en")),
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "items"),
                FlakeValue::Long(42),
                Sid::new(2, "integer"),
                1,
                true,
                Some(FlakeMeta::with_index(0)),
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "items"),
                FlakeValue::Long(99),
                Sid::new(2, "integer"),
                1,
                true,
                Some(FlakeMeta {
                    lang: Some("de".into()),
                    i: Some(1),
                }),
            ),
        ];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false, None).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.flakes.len(), 3);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_envelope_only_read() {
        let mut commit = make_test_commit(
            vec![Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "v"),
                FlakeValue::Long(1),
                Sid::new(2, "integer"),
                5,
                true,
                None,
            )],
            5,
        );
        let prev_cid = ContentId::new(ContentKind::Commit, b"prev-commit-bytes");
        commit.parents = vec![prev_cid.clone()];
        commit.namespace_delta = HashMap::from([(200, "ex:".to_string())]);

        let result = write_commit(&commit, false, None).unwrap();
        let envelope = read_commit_envelope(&result.bytes).unwrap();

        assert_eq!(envelope.t, 5);
        assert_eq!(envelope.parents.first().unwrap(), &prev_cid);
        assert_eq!(envelope.namespace_delta.get(&200), Some(&"ex:".to_string()));
    }

    #[test]
    fn test_hash_integrity() {
        let flakes = vec![Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "v"),
            FlakeValue::Long(1),
            Sid::new(2, "integer"),
            1,
            true,
            None,
        )];

        let commit = make_test_commit(flakes, 1);
        let mut result = write_commit(&commit, false, None).unwrap();

        let mid = result.bytes.len() / 2;
        result.bytes[mid] ^= 0xFF;

        let read_result = read_commit(&result.bytes);
        assert!(
            read_result.is_err(),
            "corrupted blob should fail hash check"
        );
    }

    #[test]
    fn test_envelope_fields_round_trip() {
        let mut commit = make_test_commit(vec![], 10);
        commit.time = Some("2024-01-01T00:00:00Z".into());
        let txn_cid = ContentId::new(ContentKind::Txn, b"txn-abc123");
        commit.txn = Some(txn_cid.clone());

        commit.flakes.push(Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "v"),
            FlakeValue::Long(1),
            Sid::new(2, "integer"),
            10,
            true,
            None,
        ));

        let result = write_commit(&commit, false, None).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.t, 10);
        assert_eq!(decoded.time.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert_eq!(decoded.txn.as_ref(), Some(&txn_cid));
    }

    #[test]
    fn test_large_commit() {
        let flakes: Vec<Flake> = (0..1000)
            .map(|i| {
                let value = if i % 3 == 0 {
                    FlakeValue::Long(i)
                } else if i % 3 == 1 {
                    FlakeValue::String(format!("value_{i}"))
                } else {
                    FlakeValue::Ref(Sid::new(101, format!("ref_{i}")))
                };
                let dt = if i % 3 == 2 {
                    Sid::new(1, "id")
                } else if i % 3 == 0 {
                    Sid::new(2, "integer")
                } else {
                    Sid::new(2, "string")
                };
                Flake::new(
                    Sid::new(101, format!("s_{i}")),
                    Sid::new(101, format!("p_{}", i % 10)),
                    value,
                    dt,
                    42,
                    i % 5 != 0,
                    None,
                )
            })
            .collect();

        let commit = make_test_commit(flakes, 42);
        let result = write_commit(&commit, true, None).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.flakes.len(), 1000);
        assert_eq!(decoded.t, 42);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }
}
