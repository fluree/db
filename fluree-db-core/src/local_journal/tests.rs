use super::*;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone, Default)]
struct FaultIo(Rc<RefCell<State>>);
#[derive(Default)]
struct State {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    writes: Vec<(u64, Vec<u8>)>,
    barriers: Vec<Vec<u8>>,
    short: Option<usize>,
    interrupt: bool,
    write_budget: Option<usize>,
    fail_flush: Option<bool>, // false: lose writes; true: persist despite error
}

impl FaultIo {
    fn from_image(image: Vec<u8>) -> Self {
        Self(Rc::new(RefCell::new(State {
            volatile: image.clone(),
            durable: image,
            ..State::default()
        })))
    }
    fn crash(&self) -> Self {
        Self::from_image(self.0.borrow().durable.clone())
    }
}

impl JournalIo for FaultIo {
    fn len(&mut self) -> io::Result<u64> {
        Ok(self.0.borrow().volatile.len() as u64)
    }
    fn read_at(&mut self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let s = self.0.borrow();
        let start = (offset as usize).min(s.volatile.len());
        let n = out
            .len()
            .min(s.volatile.len() - start)
            .min(s.short.unwrap_or(usize::MAX));
        out[..n].copy_from_slice(&s.volatile[start..start + n]);
        Ok(n)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        let mut s = self.0.borrow_mut();
        if std::mem::take(&mut s.interrupt) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if s.write_budget == Some(0) {
            return Err(io::ErrorKind::StorageFull.into());
        }
        let n = bytes
            .len()
            .min(s.short.unwrap_or(usize::MAX))
            .min(s.write_budget.unwrap_or(usize::MAX));
        if let Some(budget) = &mut s.write_budget {
            *budget -= n;
        }
        let end = offset as usize + n;
        if end > s.volatile.len() {
            s.volatile.resize(end, 0);
        }
        s.volatile[offset as usize..end].copy_from_slice(&bytes[..n]);
        s.writes.push((offset, bytes[..n].to_vec()));
        Ok(n)
    }
    fn sync_all(&mut self) -> io::Result<()> {
        let mut s = self.0.borrow_mut();
        let fail = s.fail_flush.take();
        if fail != Some(false) {
            s.durable = s.volatile.clone();
            let image = s.durable.clone();
            s.barriers.push(image);
        }
        if fail.is_some() {
            Err(io::ErrorKind::Other.into())
        } else {
            Ok(())
        }
    }
}

fn transition(n: u8) -> Transition {
    Transition {
        ledger: "test:main".into(),
        generation: "generation-1".into(),
        head_key: "ns/test/main.json".into(),
        expected_head: if n == 1 { None } else { Some(vec![n - 1]) },
        resulting_head: vec![n],
        objects: vec![Object {
            key: format!("objects/{n}"),
            bytes: vec![n; 17],
        }],
    }
}

#[test]
fn short_interrupted_io_receipts_match_actual_flush_barriers() {
    let io = FaultIo::default();
    io.0.borrow_mut().short = Some(7);
    io.0.borrow_mut().interrupt = true;
    let mut journal = Journal::create(io.clone(), [3; 16]).unwrap();
    let receipts: Vec<_> = (1..=3)
        .map(|n| journal.append_and_sync(&transition(n)).unwrap())
        .collect();
    let s = io.0.borrow();
    assert_eq!(s.barriers.len(), 4); // initialization + one per accepted record
    for (i, receipt) in receipts.iter().enumerate() {
        assert_eq!(receipt.end as usize, s.barriers[i + 1].len());
        let (_, records) = Journal::open(FaultIo::from_image(s.barriers[i + 1].clone())).unwrap();
        assert_eq!(records.last().unwrap().receipt, *receipt);
        assert_eq!(records.len(), i + 1);
    }
    drop(s);
    let (_, records) = Journal::open(io.crash()).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|r| r.transition.clone())
            .collect::<Vec<_>>(),
        (1..=3).map(transition).collect::<Vec<_>>()
    );
    let (_, again) = Journal::open(io.crash()).unwrap();
    assert_eq!(records, again);
}

#[test]
fn uncertain_flush_poisoning_and_reconciliation_cover_both_outcomes() {
    for persists in [false, true] {
        let io = FaultIo::default();
        let mut journal = Journal::create(io.clone(), [1; 16]).unwrap();
        let ack = journal.append_and_sync(&transition(1)).unwrap();
        io.0.borrow_mut().fail_flush = Some(persists);
        assert!(matches!(
            journal.append_and_sync(&transition(2)),
            Err(Error::Io(_))
        ));
        let writes = io.0.borrow().writes.len();
        assert!(matches!(
            journal.append_and_sync(&transition(3)),
            Err(Error::Poisoned)
        ));
        assert_eq!(writes, io.0.borrow().writes.len());
        let (mut recovered, records) = Journal::open(io.crash()).unwrap();
        assert_eq!(records.len(), if persists { 2 } else { 1 });
        assert_eq!(records[0].receipt, ack);
        let next = if persists { 3 } else { 2 };
        assert_eq!(
            recovered
                .append_and_sync(&transition(next))
                .unwrap()
                .sequence,
            next as u64
        );
    }
}

#[test]
fn every_torn_append_cut_is_either_absent_complete_or_explicitly_rejected() {
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [2; 16]).unwrap();
    journal.append_and_sync(&transition(1)).unwrap();
    let prefix = io.0.borrow().durable.clone();
    io.0.borrow_mut().fail_flush = Some(false);
    assert!(journal.append_and_sync(&transition(2)).is_err());
    let full = io.0.borrow().volatile.clone();
    // Images derive from the actual write stream after its last successful sync.
    for cut in prefix.len()..=full.len() {
        let result = Journal::open(FaultIo::from_image(full[..cut].to_vec()));
        if cut == prefix.len() {
            assert_eq!(result.unwrap().1.len(), 1);
        } else if cut == full.len() {
            assert_eq!(result.unwrap().1.len(), 2);
        } else {
            assert!(result.is_err(), "silently accepted torn byte {cut}");
        }
    }
    // Out-of-order sectors with final length present, but a hole in the append.
    let mut reordered = full.clone();
    reordered[prefix.len()..prefix.len() + FRAME_HEADER].fill(0);
    assert!(Journal::open(FaultIo::from_image(reordered)).is_err());
}

#[test]
fn acknowledged_prefix_damage_and_shared_boundary_tears_fail_closed() {
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [4; 16]).unwrap();
    let first = journal.append_and_sync(&transition(1)).unwrap();
    journal.append_and_sync(&transition(2)).unwrap();
    let image = io.0.borrow().durable.clone();
    // Every byte in an acknowledged image is integrity-covered.
    for byte in 0..image.len() {
        let mut damaged = image.clone();
        damaged[byte] ^= 0x80;
        assert!(
            Journal::open(FaultIo::from_image(damaged)).is_err(),
            "byte {byte}"
        );
    }
    // Explicitly model an append rewriting the shared physical boundary and
    // damaging the preceding ACK. Detection is not automatic repair or proof of
    // any particular device's sector atomicity.
    let mut shared = image.clone();
    shared[first.end as usize - 16..first.end as usize + 16].fill(0);
    assert!(Journal::open(FaultIo::from_image(shared)).is_err());
    // Spliced records from a different journal cannot be promoted.
    let foreign = FaultIo::default();
    let mut other = Journal::create(foreign.clone(), [5; 16]).unwrap();
    other.append_and_sync(&transition(1)).unwrap();
    let mut splice = foreign.0.borrow().durable.clone();
    splice.extend_from_slice(&image[first.end as usize..]);
    assert!(Journal::open(FaultIo::from_image(splice)).is_err());
}

#[test]
fn disk_full_write_zero_and_capacity_cannot_issue_receipts() {
    for budget in [0, 1, FRAME_HEADER + 5] {
        let io = FaultIo::default();
        let mut journal = Journal::create(io.clone(), [6; 16]).unwrap();
        journal.append_and_sync(&transition(1)).unwrap();
        io.0.borrow_mut().write_budget = Some(budget);
        assert!(journal.append_and_sync(&transition(2)).is_err());
        assert!(matches!(
            journal.append_and_sync(&transition(2)),
            Err(Error::Poisoned)
        ));
        assert_eq!(Journal::open(io.crash()).unwrap().1.len(), 1);
    }
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [7; 16]).unwrap();
    io.0.borrow_mut().short = Some(0);
    assert!(
        matches!(journal.append_and_sync(&transition(1)), Err(Error::Io(e)) if e.kind() == io::ErrorKind::WriteZero)
    );
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [8; 16]).unwrap();
    journal.end = MAX_JOURNAL_BYTES - 1;
    let writes = io.0.borrow().writes.len();
    assert!(matches!(
        journal.append_and_sync(&transition(1)),
        Err(Error::Capacity)
    ));
    assert_eq!(writes, io.0.borrow().writes.len());
    assert!(!journal.poisoned);
}

#[test]
fn initialization_cuts_and_invalid_lengths_rejected_before_payload_allocation() {
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [9; 16]).unwrap();
    journal.append_and_sync(&transition(1)).unwrap();
    let image = io.0.borrow().durable.clone();
    for cut in 0..HEADER {
        assert!(Journal::open(FaultIo::from_image(image[..cut].to_vec())).is_err());
    }
    let mut oversize = image;
    oversize[HEADER + 4..HEADER + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        Journal::open(FaultIo::from_image(oversize)),
        Err(Error::Invalid(_))
    ));
    let bad = FaultIo::default();
    bad.0.borrow_mut().fail_flush = Some(false);
    assert!(Journal::create(bad.clone(), [9; 16]).is_err());
    assert!(Journal::open(bad.crash()).is_err());
}

#[test]
fn invalid_keys_and_duplicate_objects_rejected_without_io() {
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [10; 16]).unwrap();
    let writes = io.0.borrow().writes.len();
    for key in [
        "/absolute",
        "../escape",
        "a/../b",
        "a//b",
        "a\\b",
        "a\0b",
        "C:x",
        "",
    ] {
        let mut t = transition(1);
        t.objects[0].key = key.into();
        assert!(matches!(
            journal.append_and_sync(&t),
            Err(Error::Invalid(_))
        ));
    }
    let mut duplicate = transition(1);
    duplicate.objects.push(duplicate.objects[0].clone());
    assert!(journal.append_and_sync(&duplicate).is_err());
    assert_eq!(writes, io.0.borrow().writes.len());
}

#[test]
fn oversized_encoded_payload_is_rejected_without_poisoning_or_writes() {
    let io = FaultIo::default();
    let mut journal = Journal::create(io.clone(), [12; 16]).unwrap();
    let writes = io.0.borrow().writes.len();
    let mut t = transition(1);
    // JSON byte arrays expand: this must cap encoded bytes, not input byte count.
    t.objects[0].bytes = vec![255; MAX_PAYLOAD_BYTES / 4];
    assert!(matches!(journal.append_and_sync(&t), Err(Error::Capacity)));
    assert_eq!(writes, io.0.borrow().writes.len());
    assert_eq!(journal.append_and_sync(&transition(1)).unwrap().sequence, 1);
}

#[test]
fn native_file_roundtrip_exclusive_owner_and_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal");
    let mut journal = Journal::create(FileIo::create_new(&path).unwrap(), [11; 16]).unwrap();
    assert!(FileIo::open(&path).is_err());
    assert!(FileIo::create_new(&path).is_err());
    let receipt = journal.append_and_sync(&transition(1)).unwrap();
    drop(journal);
    let (journal, records) = Journal::open(FileIo::open(&path).unwrap()).unwrap();
    assert_eq!(records[0].receipt, receipt);
    assert_eq!(records[0].transition, transition(1));
    drop(journal);
    let mut image = std::fs::read(&path).unwrap();
    image[HEADER + FRAME_HEADER] ^= 1;
    std::fs::write(&path, image).unwrap();
    assert!(Journal::open(FileIo::open(&path).unwrap()).is_err());
}

struct CheckContent;
#[test]
fn coordinator_capacity_is_definite_rejection_but_partial_write_is_unresolved() {
    use super::acceptance::Coordinator;
    let io = FaultIo::default();
    let mut coordinator = Coordinator::restored(
        Journal::create(io.clone(), [24; 16]).unwrap(),
        &[],
        "test:main",
        "generation-1",
    )
    .unwrap();
    let mut target = Materialized::default();
    coordinator.journal.as_mut().unwrap().end = MAX_JOURNAL_BYTES - 1;
    assert!(matches!(
        coordinator.accept(&transition(1), &CheckContent, &mut target, |_, _| panic!(
            "capacity install"
        )),
        Err(Error::Capacity)
    ));
    assert!(!coordinator.poisoned);
    assert_eq!(io.0.borrow().barriers.len(), 1);
    coordinator.journal.as_mut().unwrap().end = HEADER as u64;
    io.0.borrow_mut().write_budget = Some(7);
    assert!(matches!(
        coordinator.accept(&transition(1), &CheckContent, &mut target, |_, _| panic!(
            "partial write install"
        )),
        Err(Error::AcceptanceUnresolved { durable: None, .. })
    ));
    assert!(coordinator.poisoned);
    assert!(target.head.is_none());
}
impl super::AcceptanceValidator for CheckContent {
    fn validate(&self, view: &super::AcceptanceView<'_>) -> Result<()> {
        let key = format!("objects/{}", view.transition.resulting_head[0]);
        if view.content(&key).is_none() {
            return Err(Error::Invalid("missing required content"));
        }
        Ok(())
    }
}

#[derive(Default)]
struct Materialized {
    head: Option<Vec<u8>>,
    objects: std::collections::BTreeMap<String, Vec<u8>>,
    remaining: Option<usize>,
}
impl Materialized {
    fn operation(&mut self) -> Result<()> {
        if let Some(remaining) = &mut self.remaining {
            if *remaining == 0 {
                return Err(Error::Io(io::ErrorKind::Other.into()));
            }
            *remaining -= 1;
        }
        Ok(())
    }
}
impl super::ReplayTarget for Materialized {
    fn read_head(&mut self, _: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.head.clone())
    }
    fn put_immutable(&mut self, object: &Object) -> Result<()> {
        self.operation()?;
        self.objects
            .insert(object.key.clone(), object.bytes.clone());
        Ok(())
    }
    fn publish_head(&mut self, _: &str, expected: Option<&[u8]>, bytes: &[u8]) -> Result<()> {
        self.operation()?;
        assert_eq!(self.head.as_deref(), expected);
        self.head = Some(bytes.to_vec());
        Ok(())
    }
}
impl super::acceptance::AcceptanceTarget for Materialized {
    fn preflight(&mut self, _: &Transition) -> Result<()> {
        Ok(())
    }
}

#[test]
fn coordinator_rejects_stale_and_incomplete_candidates_before_append() {
    use super::acceptance::Coordinator;
    let io = FaultIo::default();
    let mut coordinator = Coordinator::restored(
        Journal::create(io.clone(), [21; 16]).unwrap(),
        &[],
        "test:main",
        "generation-1",
    )
    .unwrap();
    let mut target = Materialized::default();
    let mut missing = transition(1);
    missing.objects.clear();
    assert!(coordinator
        .accept(&missing, &CheckContent, &mut target, |_, _| panic!(
            "must not install"
        ))
        .is_err());
    assert_eq!(io.0.borrow().barriers.len(), 1);
    let receipt = coordinator
        .accept(
            &transition(1),
            &CheckContent,
            &mut target,
            |view, receipt| {
                assert_eq!(
                    io.0.borrow().barriers.last().unwrap().len() as u64,
                    receipt.end
                );
                assert!(view.content("objects/1").is_some());
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(target.head, Some(vec![1]));
    let writes = io.0.borrow().writes.len();
    assert!(matches!(
        coordinator.accept(&transition(1), &CheckContent, &mut target, |_, _| panic!(
            "CAS loser"
        )),
        Err(Error::Conflict)
    ));
    assert_eq!(writes, io.0.borrow().writes.len());
    assert_eq!(coordinator.last, Some(receipt));
    assert!(!coordinator.poisoned);
}

#[test]
fn coordinator_uncertain_flush_never_materializes_or_installs() {
    use super::acceptance::Coordinator;
    for persists in [false, true] {
        let io = FaultIo::default();
        let mut coordinator = Coordinator::restored(
            Journal::create(io.clone(), [22; 16]).unwrap(),
            &[],
            "test:main",
            "generation-1",
        )
        .unwrap();
        let mut target = Materialized::default();
        coordinator
            .accept(&transition(1), &CheckContent, &mut target, |_, _| Ok(()))
            .unwrap();
        io.0.borrow_mut().fail_flush = Some(persists);
        assert!(coordinator
            .accept(&transition(2), &CheckContent, &mut target, |_, _| panic!(
                "uncertain flush cannot install"
            ))
            .is_err());
        assert_eq!(target.head, Some(vec![1]));
        assert!(!target.objects.contains_key("objects/2"));
        assert!(coordinator.poisoned);
        assert!(matches!(
            coordinator.accept(&transition(2), &CheckContent, &mut target, |_, _| Ok(())),
            Err(Error::Poisoned)
        ));
        let (_, records) = Journal::open(io.crash()).unwrap();
        assert_eq!(records.len(), if persists { 2 } else { 1 });
        super::replay_chain(&records, "test:main", "generation-1", &mut target).unwrap();
        assert_eq!(target.head, Some(vec![if persists { 2 } else { 1 }]));
    }
}

#[test]
fn coordinator_materialization_and_install_failures_leave_recoverable_unknown_outcomes() {
    use super::acceptance::Coordinator;
    for cut in 0..=2 {
        let io = FaultIo::default();
        let mut coordinator = Coordinator::restored(
            Journal::create(io.clone(), [23; 16]).unwrap(),
            &[],
            "test:main",
            "generation-1",
        )
        .unwrap();
        let mut target = Materialized {
            remaining: Some(cut),
            ..Materialized::default()
        };
        assert!(coordinator
            .accept(&transition(1), &CheckContent, &mut target, |_, _| {
                assert_eq!(cut, 2, "install ran before all materialization operations");
                Err(Error::Invalid("injected installation failure"))
            })
            .is_err());
        assert!(coordinator.poisoned);
        assert!(coordinator.last.is_none()); // no completed outcome receipt
        let (_, records) = Journal::open(io.crash()).unwrap();
        assert_eq!(records.len(), 1); // durable accepted bytes still recover
        target.remaining = None;
        super::replay_chain(&records, "test:main", "generation-1", &mut target).unwrap();
        assert_eq!(target.head, Some(vec![1]));
        assert_eq!(target.objects["objects/1"], vec![1; 17]);
    }
}
