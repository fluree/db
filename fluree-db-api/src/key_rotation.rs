//! Encryption key rotation.
//!
//! A rotation is a background sweep that re-envelopes every blob on a
//! retiring key under the current key. Addresses are hashes of plaintext,
//! so each rewrite is an in-place overwrite at the same address: no pointer
//! changes, and a crash between blobs leaves each one on exactly one key.
//!
//! The blobs are the truth; the progress record kept at [`RECORD_PATH`] is
//! a cache of where the sweep last stood. Resuming from a stale record only
//! re-reads headers the sweep already handled. Nothing here removes a key:
//! completion is a verification pass that finds zero blobs on the retiring
//! key and stamps the record, after which an operator drops the key from
//! configuration.

use crate::error::{ApiError, Result};
use crate::Fluree;
use fluree_db_core::address_path::shared_prefix_for_path;
use fluree_db_core::storage::GRAPH_SOURCES_PATH_SEGMENT;
use fluree_db_core::{
    ledger_id_prefix_for_path, EncryptionAdmin, Storage, StorageMethod, StorageRead, StorageWrite,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Storage-relative path of the progress record.
pub const RECORD_PATH: &str = "@maintenance/key-rotation.json";
/// A running record whose heartbeat is older than this is abandoned: its
/// holder may be taken over by a new sweep.
pub const STALE_AFTER: Duration = Duration::from_secs(10 * 60);
const CHECKPOINT_EVERY: u64 = 1000;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);
const FAILED_ADDRESSES_CAP: usize = 100;

/// Where a rotation stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyRotationState {
    /// The sweep is in progress, or was when the holder last checkpointed.
    Running,
    /// Stopped by an operator; `start` with the same retiring key resumes it.
    Paused,
    /// Stopped by an operator; the next `start` begins over.
    Cancelled,
    /// Stopped by a listing error; `start` resumes from the cursor.
    Failed,
    /// The sweep finished but verification found blobs still on the
    /// retiring key (see `failed_addresses` and `completion`).
    Swept,
    /// Verification found no blob on the retiring key. The key may be
    /// removed from configuration.
    Completed,
}

/// What the verification pass found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRotationCompletion {
    /// Unix seconds when verification finished.
    pub verified_at: u64,
    /// Blobs still on the retiring key. Zero stamps completion.
    pub remaining_on_retired: u64,
}

/// The progress record: checkpointed to storage by the sweep, returned by
/// status, and printed by the CLI. Counters are since the sweep started
/// and survive resumption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRotationProgress {
    pub state: KeyRotationState,
    pub retire_key_id: u32,
    pub current_key_id: u32,
    /// Identity of the process that last checkpointed.
    pub holder: String,
    pub dry_run: bool,
    /// Ledger name or id the sweep is limited to, if any.
    pub ledger_scope: Option<String>,
    pub started_at: u64,
    /// Heartbeat: unix seconds of the last checkpoint.
    pub updated_at: u64,
    /// Sweep units: one per ledger branch, one per ledger's shared
    /// dictionaries, one for graph sources.
    pub units_total: usize,
    pub units_done: usize,
    /// The unit in progress.
    pub unit: Option<String>,
    /// Last address fully processed in `unit`; resumption skips up to it.
    pub cursor: Option<String>,
    pub scanned: u64,
    pub rewritten: u64,
    pub already_current: u64,
    /// Blobs on a held key that is neither current nor retiring.
    pub on_other_keys: u64,
    /// Addresses whose bytes are not an envelope (nameservice records,
    /// lock files, plaintext left by an unencrypted run).
    pub not_enveloped: u64,
    /// Blobs found on the retiring key. In a dry run this is the work a real
    /// run would do; in a real run it equals `rewritten + failed`.
    pub on_retired: u64,
    pub failed: u64,
    /// Up to the first hundred addresses that failed, for the retry pass.
    pub failed_addresses: Vec<String>,
    pub bytes_rewritten: u64,
    pub last_error: Option<String>,
    pub completion: Option<KeyRotationCompletion>,
}

/// What `start` needs.
#[derive(Debug, Clone)]
pub struct KeyRotationOptions {
    /// The key being retired: every blob on it is rewritten.
    pub retire_key_id: u32,
    /// Count only; write nothing, checkpoint nothing.
    pub dry_run: bool,
    /// Limit the sweep to one ledger, by name or branch-qualified id.
    pub ledger: Option<String>,
    /// Throttle on rewritten plaintext bytes per second.
    pub max_bytes_per_sec: Option<u64>,
    /// Identity of this process, recorded as the holder.
    pub holder: String,
}

/// The status view: the record plus what only this process knows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRotationStatus {
    /// Ids of the keys the storage holds, current first.
    pub key_ids: Vec<u32>,
    pub current_key_id: u32,
    /// The progress record, from memory when the sweep runs here, else from
    /// storage. `None` when no rotation has ever been started.
    pub progress: Option<KeyRotationProgress>,
    /// The sweep task is alive in this process.
    pub active_here: bool,
    pub seconds_since_update: Option<u64>,
    /// `Running` with a heartbeat older than [`STALE_AFTER`]: nobody is
    /// advancing it. A fresh heartbeat with frozen counters is reported the
    /// same way by the CLI.
    pub stalled: bool,
    /// `Running` and released by its last holder (a leader that lost
    /// leadership): the next leader normally takes it over at once. One
    /// that stays released has not been picked up; `resume` takes it over.
    #[serde(default)]
    pub released: bool,
}

struct Control {
    pause: AtomicBool,
    cancel: AtomicBool,
    /// Stop without changing state: the record stays `Running` with its
    /// heartbeat cleared, so the next holder takes over at once.
    release: AtomicBool,
}

struct ActiveJob {
    control: Arc<Control>,
    progress: Arc<parking_lot::RwLock<KeyRotationProgress>>,
    /// `None` once the task has been awaited to completion.
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ActiveJob {
    fn is_running(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }
}

/// The sweep running in this process, if any.
#[derive(Default)]
pub struct KeyRotationSlot {
    active: parking_lot::Mutex<Option<ActiveJob>>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn record_address(method: &str) -> String {
    format!("fluree:{method}://{RECORD_PATH}")
}

/// One enumeration unit: a label for the record and the prefix to list.
#[derive(Debug, Clone)]
struct Unit {
    label: String,
    prefix: String,
}

async fn read_record(storage: &Arc<dyn Storage>) -> Result<Option<KeyRotationProgress>> {
    match storage
        .read_bytes(&record_address(storage.storage_method()))
        .await
    {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ApiError::internal(format!("key rotation record is unreadable: {e}"))),
        Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn write_record(storage: &Arc<dyn Storage>, progress: &KeyRotationProgress) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(progress)
        .map_err(|e| ApiError::internal(format!("key rotation record: {e}")))?;
    storage
        .write_bytes(&record_address(storage.storage_method()), &bytes)
        .await?;
    Ok(())
}

/// Throttle on bytes rewritten: sleeps until the average rate since the
/// sweep started is under the cap.
struct Throttle {
    cap: Option<u64>,
    started: Instant,
    bytes: u64,
}

impl Throttle {
    async fn account(&mut self, bytes: u64) {
        let Some(cap) = self.cap else { return };
        self.bytes += bytes;
        let expected = Duration::from_secs_f64(self.bytes as f64 / cap.max(1) as f64);
        let elapsed = self.started.elapsed();
        if expected > elapsed {
            tokio::time::sleep(expected - elapsed).await;
        }
    }
}

impl Fluree {
    /// The key ids this instance's storage holds, current first, and the id
    /// of the current key; `None` when the storage does not encrypt at rest.
    /// A permanent (IPFS) backend has no admin storage and is never
    /// encrypted: `build_ipfs` refuses a key.
    pub fn encryption_key_ids(&self) -> Option<(Vec<u32>, u32)> {
        let admin = self.backend.admin_storage_cloned()?.encryption_admin()?;
        Some((admin.key_ids(), admin.current_key_id()))
    }

    /// Ids of the keys the storage holds, current first, or an error when
    /// the storage is not encrypted.
    fn encryption_admin(&self) -> Result<(Arc<dyn Storage>, Arc<dyn EncryptionAdmin>)> {
        let storage = self
            .backend
            .admin_storage_cloned()
            .ok_or_else(|| ApiError::config("key rotation requires a managed storage backend"))?;
        let admin = storage
            .encryption_admin()
            .ok_or_else(|| ApiError::config("storage is not encrypted; nothing to rotate"))?;
        Ok((storage, admin))
    }

    /// The sweep units in a fixed order: each ledger branch's own prefix,
    /// each ledger's shared dictionaries, and the graph-source artifacts.
    async fn key_rotation_units(&self, method: &str, scope: Option<&str>) -> Result<Vec<Unit>> {
        let mut records = self.nameservice().all_records().await?;
        records.sort_by(|a, b| a.ledger_id.cmp(&b.ledger_id));
        let in_scope = |ledger_id: &str, name: &str| match scope {
            None => true,
            Some(s) if s.contains(':') => s == ledger_id,
            Some(s) => s == name,
        };
        let mut units = Vec::new();
        let mut shared = BTreeSet::new();
        for record in &records {
            if !in_scope(&record.ledger_id, &record.name) {
                continue;
            }
            units.push(Unit {
                label: record.ledger_id.clone(),
                prefix: format!(
                    "fluree:{method}://{}/",
                    ledger_id_prefix_for_path(&record.ledger_id)
                ),
            });
            shared.insert(record.name.clone());
        }
        for name in shared {
            units.push(Unit {
                label: format!("{name} (shared dictionaries)"),
                prefix: format!("fluree:{method}://{}/", shared_prefix_for_path(&name)),
            });
        }
        if scope.is_none() {
            units.push(Unit {
                label: "graph sources".to_string(),
                prefix: format!("fluree:{method}://{GRAPH_SOURCES_PATH_SEGMENT}/"),
            });
        }
        Ok(units)
    }

    /// Start a rotation, or resume the one the progress record describes.
    ///
    /// Preflight: the storage is encrypted, holds `retire_key_id`, and that
    /// key is not current. A record that is `Running` under another holder
    /// with a fresh heartbeat is refused; a stale one is taken over. A
    /// `Paused`, `Failed` or `Swept` record for the same key resumes from
    /// its cursor. A dry run counts only and touches no record.
    pub async fn start_key_rotation(
        &self,
        opts: KeyRotationOptions,
    ) -> Result<KeyRotationProgress> {
        let (storage, admin) = self.encryption_admin()?;
        let current = admin.current_key_id();
        if opts.retire_key_id == current {
            return Err(ApiError::config(format!(
                "key {} is the current key; make another key current before retiring it",
                opts.retire_key_id
            )));
        }
        if !admin.key_ids().contains(&opts.retire_key_id) {
            return Err(ApiError::config(format!(
                "key {} is not held; it must stay configured until every blob is off it",
                opts.retire_key_id
            )));
        }

        // Not held across the awaits below; re-checked before the task is
        // installed so two concurrent starts cannot both spawn.
        let already_running = || {
            self.key_rotation
                .active
                .lock()
                .as_ref()
                .is_some_and(ActiveJob::is_running)
        };
        if already_running() {
            return Err(ApiError::http(
                409,
                "a key rotation is already running in this process",
            ));
        }

        let existing = if opts.dry_run {
            None
        } else {
            read_record(&storage).await?
        };
        let now = now_secs();
        let progress = match existing {
            Some(rec)
                if rec.state == KeyRotationState::Running
                    && rec.holder != opts.holder
                    && now.saturating_sub(rec.updated_at) < STALE_AFTER.as_secs() =>
            {
                return Err(ApiError::http(
                    409,
                    format!(
                        "a key rotation is running on {} (last checkpoint {}s ago)",
                        rec.holder,
                        now.saturating_sub(rec.updated_at)
                    ),
                ));
            }
            // Same job definition (key and scope): continue it.
            Some(mut rec)
                if rec.retire_key_id == opts.retire_key_id
                    && rec.ledger_scope == opts.ledger
                    && matches!(
                        rec.state,
                        KeyRotationState::Running
                            | KeyRotationState::Paused
                            | KeyRotationState::Failed
                            | KeyRotationState::Swept
                    ) =>
            {
                // A swept record has finished its units; running it again is
                // a retry from the top, which only re-reads headers until it
                // reaches a blob still on the retiring key.
                if rec.state == KeyRotationState::Swept {
                    rec.units_done = 0;
                    rec.cursor = None;
                }
                rec.state = KeyRotationState::Running;
                rec.holder = opts.holder.clone();
                rec.current_key_id = current;
                rec.updated_at = now;
                rec.last_error = None;
                rec
            }
            _ => KeyRotationProgress {
                state: KeyRotationState::Running,
                retire_key_id: opts.retire_key_id,
                current_key_id: current,
                holder: opts.holder.clone(),
                dry_run: opts.dry_run,
                ledger_scope: opts.ledger.clone(),
                started_at: now,
                updated_at: now,
                units_total: 0,
                units_done: 0,
                unit: None,
                cursor: None,
                scanned: 0,
                rewritten: 0,
                already_current: 0,
                on_other_keys: 0,
                not_enveloped: 0,
                on_retired: 0,
                failed: 0,
                failed_addresses: Vec::new(),
                bytes_rewritten: 0,
                last_error: None,
                completion: None,
            },
        };

        let units = self
            .key_rotation_units(storage.storage_method(), progress.ledger_scope.as_deref())
            .await?;
        let mut progress = progress;
        progress.units_total = units.len();
        if !progress.dry_run {
            write_record(&storage, &progress).await?;
        }

        let control = Arc::new(Control {
            pause: AtomicBool::new(false),
            cancel: AtomicBool::new(false),
            release: AtomicBool::new(false),
        });
        let shared = Arc::new(parking_lot::RwLock::new(progress.clone()));
        let mut active = self.key_rotation.active.lock();
        if active.as_ref().is_some_and(ActiveJob::is_running) {
            return Err(ApiError::http(
                409,
                "a key rotation is already running in this process",
            ));
        }
        let handle = tokio::spawn(run_sweep(
            storage,
            admin,
            units,
            Arc::clone(&control),
            Arc::clone(&shared),
            opts.max_bytes_per_sec,
        ));
        *active = Some(ActiveJob {
            control,
            progress: shared,
            handle: Some(handle),
        });
        Ok(progress)
    }

    /// Resume a `Running` record after a restart or a leadership change,
    /// if one exists and no fresh holder has it. Returns the progress when
    /// a sweep was started here.
    pub async fn resume_pending_key_rotation(
        &self,
        holder: &str,
    ) -> Result<Option<KeyRotationProgress>> {
        let Ok((storage, admin)) = self.encryption_admin() else {
            return Ok(None);
        };
        let Some(rec) = read_record(&storage).await? else {
            return Ok(None);
        };
        if rec.state != KeyRotationState::Running {
            return Ok(None);
        }
        if rec.holder != holder && now_secs().saturating_sub(rec.updated_at) < STALE_AFTER.as_secs()
        {
            return Ok(None);
        }
        if !admin.key_ids().contains(&rec.retire_key_id)
            || rec.retire_key_id == admin.current_key_id()
        {
            tracing::warn!(
                retire_key_id = rec.retire_key_id,
                "a key rotation record is pending but this process cannot continue it: \
                 the retiring key must be held and not current"
            );
            return Ok(None);
        }
        self.start_key_rotation(KeyRotationOptions {
            retire_key_id: rec.retire_key_id,
            dry_run: false,
            ledger: rec.ledger_scope.clone(),
            max_bytes_per_sec: None,
            holder: holder.to_string(),
        })
        .await
        .map(Some)
    }

    /// The record plus what only this process knows.
    pub async fn key_rotation_status(&self) -> Result<KeyRotationStatus> {
        let (storage, admin) = self.encryption_admin()?;
        let (active_here, local) = {
            let active = self.key_rotation.active.lock();
            match active.as_ref() {
                Some(job) => (job.is_running(), Some(job.progress.read().clone())),
                None => (false, None),
            }
        };
        let progress = match local {
            Some(p) if active_here || p.dry_run => Some(p),
            _ => read_record(&storage).await?.or(local),
        };
        let now = now_secs();
        let seconds_since_update = progress.as_ref().map(|p| now.saturating_sub(p.updated_at));
        // A released record (`updated_at == 0`) is reported as released, not
        // stalled: during a handover it is about to be taken over.
        let released = !active_here
            && progress
                .as_ref()
                .is_some_and(|p| p.state == KeyRotationState::Running && p.updated_at == 0);
        let stalled = !released
            && matches!(
                (&progress, seconds_since_update),
                (Some(p), Some(age)) if p.state == KeyRotationState::Running
                    && age > STALE_AFTER.as_secs()
            );
        Ok(KeyRotationStatus {
            key_ids: admin.key_ids(),
            current_key_id: admin.current_key_id(),
            progress,
            active_here,
            seconds_since_update,
            stalled,
            released,
        })
    }

    /// Stop the sweep running here after its next blob and mark the record
    /// `Paused`; `start` with the same key resumes it.
    pub fn pause_key_rotation(&self) -> Result<()> {
        self.signal_key_rotation(|c| &c.pause)
    }

    /// Stop the sweep running here and mark the record `Cancelled`.
    pub fn cancel_key_rotation(&self) -> Result<()> {
        self.signal_key_rotation(|c| &c.cancel)
    }

    /// Hand the sweep off: stop the local task after its next blob and
    /// leave the record `Running` with a cleared heartbeat, so whichever
    /// process next calls [`resume_pending_key_rotation`] continues it
    /// immediately. Used on loss of leadership. Silent when nothing runs.
    ///
    /// [`resume_pending_key_rotation`]: Self::resume_pending_key_rotation
    pub fn release_key_rotation(&self) {
        let _ = self.signal_key_rotation(|c| &c.release);
    }

    fn signal_key_rotation(&self, flag: impl Fn(&Control) -> &AtomicBool) -> Result<()> {
        let active = self.key_rotation.active.lock();
        match active.as_ref() {
            Some(job) if job.is_running() => {
                flag(&job.control).store(true, Ordering::SeqCst);
                Ok(())
            }
            _ => Err(ApiError::http(
                409,
                "no key rotation is running in this process",
            )),
        }
    }

    /// Wait for the sweep running here to finish. Tests and the CLI's
    /// local mode use it; the server does not.
    pub async fn wait_for_key_rotation(&self) -> Result<Option<KeyRotationProgress>> {
        let job = self.key_rotation.active.lock().take();
        let Some(mut job) = job else { return Ok(None) };
        if let Some(handle) = job.handle.take() {
            handle
                .await
                .map_err(|e| ApiError::internal(format!("key rotation task: {e}")))?;
        }
        let progress = job.progress.read().clone();
        *self.key_rotation.active.lock() = Some(job);
        Ok(Some(progress))
    }

    /// Count the blobs still on `retire_key_id` across the whole store,
    /// reading headers only, and stamp the record: `Completed` when zero,
    /// `Swept` otherwise. Only this stamp licenses removing the key.
    pub async fn verify_key_rotation(&self, retire_key_id: u32) -> Result<KeyRotationProgress> {
        let (storage, admin) = self.encryption_admin()?;
        let mut progress = match read_record(&storage).await? {
            Some(rec) if rec.retire_key_id == retire_key_id => rec,
            _ => KeyRotationProgress {
                state: KeyRotationState::Swept,
                retire_key_id,
                current_key_id: admin.current_key_id(),
                holder: String::new(),
                dry_run: false,
                ledger_scope: None,
                started_at: now_secs(),
                updated_at: now_secs(),
                units_total: 0,
                units_done: 0,
                unit: None,
                cursor: None,
                scanned: 0,
                rewritten: 0,
                already_current: 0,
                on_other_keys: 0,
                not_enveloped: 0,
                on_retired: 0,
                failed: 0,
                failed_addresses: Vec::new(),
                bytes_rewritten: 0,
                last_error: None,
                completion: None,
            },
        };
        let remaining = count_on_key(&storage, &admin, retire_key_id).await?;
        progress.completion = Some(KeyRotationCompletion {
            verified_at: now_secs(),
            remaining_on_retired: remaining,
        });
        progress.state = if remaining == 0 {
            KeyRotationState::Completed
        } else {
            KeyRotationState::Swept
        };
        progress.updated_at = now_secs();
        write_record(&storage, &progress).await?;
        Ok(progress)
    }
}

/// Header-only count of blobs on `key_id` under the whole store.
async fn count_on_key(
    storage: &Arc<dyn Storage>,
    admin: &Arc<dyn EncryptionAdmin>,
    key_id: u32,
) -> Result<u64> {
    let root = format!("fluree:{}://", storage.storage_method());
    let addresses = storage.list_prefix(&root).await?;
    let mut remaining = 0;
    for address in addresses {
        if admin.key_id_at(&address).await? == Some(key_id) {
            remaining += 1;
        }
    }
    Ok(remaining)
}

async fn run_sweep(
    storage: Arc<dyn Storage>,
    admin: Arc<dyn EncryptionAdmin>,
    units: Vec<Unit>,
    control: Arc<Control>,
    shared: Arc<parking_lot::RwLock<KeyRotationProgress>>,
    max_bytes_per_sec: Option<u64>,
) {
    let mut progress = shared.read().clone();
    let dry_run = progress.dry_run;
    let retire = progress.retire_key_id;
    let current = progress.current_key_id;
    let mut throttle = Throttle {
        cap: max_bytes_per_sec,
        started: Instant::now(),
        bytes: 0,
    };
    let mut since_checkpoint = 0u64;
    let mut last_checkpoint = Instant::now();

    // Checkpoint: publish to the in-process view and, for a real run, to storage.
    async fn checkpoint(
        storage: &Arc<dyn Storage>,
        shared: &parking_lot::RwLock<KeyRotationProgress>,
        progress: &mut KeyRotationProgress,
    ) {
        progress.updated_at = now_secs();
        *shared.write() = progress.clone();
        if !progress.dry_run {
            if let Err(e) = write_record(storage, progress).await {
                tracing::warn!(%e, "key rotation: failed to checkpoint progress record");
            }
        }
    }

    let resume_from = progress.units_done;
    let resume_cursor = progress.cursor.clone();
    for (i, unit) in units.iter().enumerate().skip(resume_from) {
        progress.unit = Some(unit.label.clone());
        let addresses = match storage.list_prefix(&unit.prefix).await {
            Ok(mut a) => {
                a.sort();
                a
            }
            Err(e) => {
                progress.state = KeyRotationState::Failed;
                progress.last_error = Some(format!("cannot list {}: {e}", unit.prefix));
                checkpoint(&storage, &shared, &mut progress).await;
                return;
            }
        };
        let skip_through = if i == resume_from {
            resume_cursor.clone()
        } else {
            None
        };
        for address in addresses {
            if let Some(done) = &skip_through {
                if address.as_str() <= done.as_str() {
                    continue;
                }
            }
            if control.cancel.load(Ordering::SeqCst) {
                progress.state = KeyRotationState::Cancelled;
                checkpoint(&storage, &shared, &mut progress).await;
                return;
            }
            if control.pause.load(Ordering::SeqCst) {
                progress.state = KeyRotationState::Paused;
                checkpoint(&storage, &shared, &mut progress).await;
                return;
            }
            if control.release.load(Ordering::SeqCst) {
                checkpoint(&storage, &shared, &mut progress).await;
                // Clear the heartbeat after the checkpoint so the record is
                // stale to the next holder without losing the cursor.
                progress.updated_at = 0;
                *shared.write() = progress.clone();
                if !dry_run {
                    if let Err(e) = write_record(&storage, &progress).await {
                        tracing::warn!(%e, "key rotation: failed to release progress record");
                    }
                }
                return;
            }

            match admin.key_id_at(&address).await {
                Ok(None) => progress.not_enveloped += 1,
                Ok(Some(id)) if id == current => progress.already_current += 1,
                Ok(Some(id)) if id != retire => progress.on_other_keys += 1,
                Ok(Some(_)) => {
                    progress.on_retired += 1;
                    if !dry_run {
                        match admin.reencrypt(&address).await {
                            Ok(Some(bytes)) => {
                                progress.rewritten += 1;
                                progress.bytes_rewritten += bytes;
                                throttle.account(bytes).await;
                            }
                            // Rewritten by a concurrent writer between the
                            // header read and ours; nothing to do.
                            Ok(None) => progress.already_current += 1,
                            Err(e) => record_failure(&mut progress, &address, &e),
                        }
                    }
                }
                Err(e) => record_failure(&mut progress, &address, &e),
            }
            progress.scanned += 1;
            progress.cursor = Some(address);
            since_checkpoint += 1;
            if since_checkpoint >= CHECKPOINT_EVERY
                || last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL
            {
                checkpoint(&storage, &shared, &mut progress).await;
                since_checkpoint = 0;
                last_checkpoint = Instant::now();
            }
        }
        progress.units_done = i + 1;
        progress.cursor = None;
        checkpoint(&storage, &shared, &mut progress).await;
    }
    progress.unit = None;

    if dry_run {
        progress.state = KeyRotationState::Completed;
        checkpoint(&storage, &shared, &mut progress).await;
        return;
    }

    // Verify: the whole store, headers only. Zero remaining stamps completion.
    match count_on_key(&storage, &admin, retire).await {
        Ok(remaining) => {
            progress.completion = Some(KeyRotationCompletion {
                verified_at: now_secs(),
                remaining_on_retired: remaining,
            });
            progress.state = if remaining == 0 {
                KeyRotationState::Completed
            } else {
                KeyRotationState::Swept
            };
        }
        Err(e) => {
            progress.state = KeyRotationState::Swept;
            progress.last_error = Some(format!("verification listing failed: {e}"));
        }
    }
    checkpoint(&storage, &shared, &mut progress).await;
}

fn record_failure(
    progress: &mut KeyRotationProgress,
    address: &str,
    error: &dyn std::fmt::Display,
) {
    progress.failed += 1;
    if progress.failed_addresses.len() < FAILED_ADDRESSES_CAP {
        progress.failed_addresses.push(address.to_string());
    }
    progress.last_error = Some(format!("{address}: {error}"));
    tracing::warn!(address, %error, "key rotation: blob skipped");
}
