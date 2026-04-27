//! Transaction builders: context-first, compile-time-safe transaction construction.
//!
//! Two builder types:
//! - [`OwnedTransactBuilder`] — consumes a `LedgerState`, returns updated state
//! - [`RefTransactBuilder`] — borrows a `LedgerHandle`, updates in-place
//!
//! Plus [`Staged`] — a first-class staged (uncommitted) transaction that is
//! queryable and committable.
//!
//! # Design
//!
//! - **Infallible setters**: All setters return `Self`; errors are accumulated
//!   and reported at `.execute()` / `.stage()` / `.validate()`.
//! - **Composition**: Both builders share `TransactCore` for common fields.

use serde_json::Value as JsonValue;

use crate::error::{BuilderError, BuilderErrors};
use crate::ledger_manager::LedgerHandle;
use crate::tx::{IndexingMode, IndexingStatus, StageResult, TransactResult, TransactResultRef};
use crate::{
    ApiError, Fluree, PolicyContext, Result, TrackedErrorResponse, TrackedTransactionInput,
    Tracker, TrackingOptions,
};
use fluree_db_core::{ContentId, ContentKind};
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_transact::{
    parse_trig_phase1, CommitOpts, NamedGraphBlock, NamespaceRegistry, RawTrigMeta, Txn, TxnOpts,
    TxnType,
};

// ============================================================================
// TransactOperation (private)
// ============================================================================

/// The type of transaction operation to perform.
pub(crate) enum TransactOperation<'a> {
    InsertJson(&'a JsonValue),
    UpsertJson(&'a JsonValue),
    UpdateJson(&'a JsonValue),
    InsertTurtle(&'a str),
    UpsertTurtle(&'a str),
}

/// Result of parsing a transaction operation to JSON.
/// For Turtle inputs with TriG GRAPH blocks, also includes raw txn-meta and named graphs.
pub(crate) struct ParsedOperation {
    pub json: JsonValue,
    pub trig_meta: Option<RawTrigMeta>,
    pub named_graphs: Vec<NamedGraphBlock>,
}

impl TransactOperation<'_> {
    /// Get the `TxnType` for this operation.
    pub(crate) fn txn_type(&self) -> TxnType {
        match self {
            TransactOperation::InsertJson(_) => TxnType::Insert,
            TransactOperation::UpsertJson(_) => TxnType::Upsert,
            TransactOperation::UpdateJson(_) => TxnType::Update,
            TransactOperation::InsertTurtle(_) => TxnType::Insert,
            TransactOperation::UpsertTurtle(_) => TxnType::Upsert,
        }
    }

    /// Parse the operation to JSON, extracting TriG txn-meta and named graphs from Turtle inputs.
    ///
    /// For Turtle inputs with `GRAPH <...> { ... }` blocks, this extracts:
    /// - Metadata from txn-meta graph
    /// - Named graph blocks for user-defined graphs
    ///
    /// The metadata can be resolved to `TxnMetaEntry` using `resolve_trig_meta()` once a
    /// `NamespaceRegistry` is available. Named graphs are converted to
    /// `TripleTemplate`s with appropriate graph_id during staging.
    pub(crate) fn to_json_with_trig_meta(&self) -> Result<ParsedOperation> {
        match self {
            TransactOperation::InsertJson(j) => Ok(ParsedOperation {
                json: (*j).clone(),
                trig_meta: None,
                named_graphs: Vec::new(),
            }),
            TransactOperation::UpsertJson(j) => Ok(ParsedOperation {
                json: (*j).clone(),
                trig_meta: None,
                named_graphs: Vec::new(),
            }),
            TransactOperation::UpdateJson(j) => Ok(ParsedOperation {
                json: (*j).clone(),
                trig_meta: None,
                named_graphs: Vec::new(),
            }),
            TransactOperation::InsertTurtle(ttl) | TransactOperation::UpsertTurtle(ttl) => {
                // Phase 1: Extract TriG GRAPH block (if present)
                let phase1 = parse_trig_phase1(ttl)?;

                // Parse cleaned Turtle to JSON
                let json = fluree_graph_turtle::parse_to_json(&phase1.turtle)?;

                Ok(ParsedOperation {
                    json,
                    trig_meta: phase1.raw_meta,
                    named_graphs: phase1.named_graphs,
                })
            }
        }
    }
}

// ============================================================================
// TransactCore (shared, private)
// ============================================================================

/// Shared fields for both transaction builders.
pub(crate) struct TransactCore<'a> {
    pub(crate) operation: Option<TransactOperation<'a>>,
    /// Pre-built transaction IR (bypasses parsing, used for SPARQL UPDATE)
    pub(crate) pre_built_txn: Option<Txn>,
    pub(crate) txn_opts: TxnOpts,
    pub(crate) commit_opts: CommitOpts,
    pub(crate) index_config: Option<IndexConfig>,
    pub(crate) tracking: Option<TrackingOptions>,
    pub(crate) policy: Option<PolicyContext>,
    errors: Vec<BuilderError>,
}

impl<'a> TransactCore<'a> {
    pub(crate) fn new() -> Self {
        Self {
            operation: None,
            pre_built_txn: None,
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            index_config: None,
            tracking: None,
            policy: None,
            errors: Vec::new(),
        }
    }

    pub(crate) fn set_pre_built_txn(&mut self, txn: Txn) {
        if self.operation.is_some() || self.pre_built_txn.is_some() {
            self.errors.push(BuilderError::Conflict {
                field: "operation",
                message: "Transaction operation already set; cannot set pre-built txn".to_string(),
            });
        } else {
            self.pre_built_txn = Some(txn);
        }
    }

    pub(crate) fn set_operation(&mut self, op: TransactOperation<'a>) {
        if self.operation.is_some() {
            self.errors.push(BuilderError::Conflict {
                field: "operation",
                message: "Transaction operation already set; cannot set multiple operations"
                    .to_string(),
            });
        } else {
            self.operation = Some(op);
        }
    }

    pub(crate) fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let mut errors = self.errors.clone();
        // Either operation or pre_built_txn must be set
        if self.operation.is_none() && self.pre_built_txn.is_none() {
            errors.push(BuilderError::Missing {
                field: "operation",
                hint: "Call .insert(), .upsert(), .update(), .insert_turtle(), .upsert_turtle(), or .txn()",
            });
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errors))
        }
    }
}

// ============================================================================
// Staged
// ============================================================================

/// A staged (uncommitted) transaction. Queryable and committable.
///
/// Created by [`OwnedTransactBuilder::stage()`]. The staged state can be:
/// - **Queried** via [`GraphDb::from_staged()`](crate::GraphDb) to
///   preview changes before committing
/// - **Committed** via [`Fluree::commit_staged()`](crate::Fluree) to persist
///
/// # Example
///
/// ```ignore
/// let staged = fluree.stage_owned(ledger)
///     .insert(&data)
///     .stage().await?;
///
/// // Query staged state
/// let graph = GraphDb::from_staged(&staged);
/// let preview = graph.query(&fluree).jsonld(&q).execute().await?;
///
/// // Commit if satisfied
/// let result = fluree.commit_staged(staged, CommitOpts::default()).await?;
/// ```
pub struct Staged {
    /// The queryable staged view (base + overlay with staged flakes).
    pub view: LedgerView,
    /// Namespace registry needed for commit.
    pub ns_registry: NamespaceRegistry,
    /// Named graph IRI mappings introduced by this transaction (g_id → IRI).
    ///
    /// Carried here so that `GraphDb::from_staged()` can apply the full
    /// envelope delta (namespace codes + graph IRIs) to the snapshot clone,
    /// ensuring `decode_sid` works for SIDs referencing new namespaces or
    /// graphs introduced by the staged transaction.
    pub graph_delta: rustc_hash::FxHashMap<u16, String>,
}

impl std::fmt::Debug for Staged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Staged")
            .field("has_staged", &self.view.has_staged())
            .field("graph_delta_len", &self.graph_delta.len())
            .finish()
    }
}

// ============================================================================
// OwnedTransactBuilder
// ============================================================================

/// Builder for transactions that consume a `LedgerState`.
///
/// Created via [`Fluree::stage_owned()`]. Use this for CLI tools, scripts, or
/// tests where you manage your own ledger state. For server/application
/// contexts, prefer [`RefTransactBuilder`] via [`Fluree::stage()`].
///
/// The ledger state is consumed and returned in the result as an updated
/// `LedgerState`.
///
/// # Example
///
/// ```ignore
/// let result = fluree.stage_owned(ledger)
///     .insert(&data)
///     .execute().await?;
/// let ledger = result.ledger;
/// ```
pub struct OwnedTransactBuilder<'a> {
    fluree: &'a Fluree,
    ledger: LedgerState,
    core: TransactCore<'a>,
}

impl<'a> OwnedTransactBuilder<'a> {
    /// Create a new builder (called by `Fluree::stage_owned()`).
    pub(crate) fn new(fluree: &'a Fluree, ledger: LedgerState) -> Self {
        Self {
            fluree,
            ledger,
            core: TransactCore::new(),
        }
    }

    // -- Operation setters --

    /// Set the operation to insert JSON-LD data.
    pub fn insert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::InsertJson(data));
        self
    }

    /// Set the operation to upsert JSON-LD data.
    pub fn upsert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpsertJson(data));
        self
    }

    /// Set the operation to update with WHERE/DELETE/INSERT semantics.
    pub fn update(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpdateJson(data));
        self
    }

    /// Set the operation to insert Turtle data.
    pub fn insert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::InsertTurtle(turtle));
        self
    }

    /// Set the operation to upsert Turtle data.
    pub fn upsert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::UpsertTurtle(turtle));
        self
    }

    /// Set a pre-built transaction IR (bypasses JSON/Turtle parsing).
    ///
    /// This is used for SPARQL UPDATE where the transaction is already
    /// lowered to the IR representation.
    pub fn txn(mut self, txn: Txn) -> Self {
        self.core.set_pre_built_txn(txn);
        self
    }

    // -- Option setters --

    /// Set transaction options (author, context, etc.).
    pub fn txn_opts(mut self, opts: TxnOpts) -> Self {
        self.core.txn_opts = opts;
        self
    }

    /// Set commit options (message, author, etc.).
    pub fn commit_opts(mut self, opts: CommitOpts) -> Self {
        self.core.commit_opts = opts;
        self
    }

    /// Override the index configuration.
    pub fn index_config(mut self, config: IndexConfig) -> Self {
        self.core.index_config = Some(config);
        self
    }

    /// Enable tracking with custom options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.tracking = Some(opts);
        self
    }

    /// Set policy enforcement for the transaction.
    pub fn policy(mut self, ctx: PolicyContext) -> Self {
        self.core.policy = Some(ctx);
        self
    }

    // -- Terminal operations --

    /// Validate the builder configuration without executing.
    ///
    /// Returns all accumulated errors at once.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        self.core.validate()
    }

    /// Stage + commit the transaction, returning the updated ledger state.
    pub async fn execute(self) -> Result<TransactResult> {
        self.core.validate().map_err(ApiError::Builder)?;

        let index_config = self
            .core
            .index_config
            .clone()
            .unwrap_or_else(crate::server_defaults::default_index_config);

        // Pre-built Txn IR path (e.g., SPARQL UPDATE lowered to Txn)
        if let Some(txn) = self.core.pre_built_txn {
            let txn_type = txn.txn_type;
            let tracker = self
                .core
                .tracking
                .clone()
                .map(Tracker::new)
                .unwrap_or_default();
            let StageResult {
                view,
                ns_registry,
                txn_meta,
                graph_delta,
            } = self
                .fluree
                .stage_transaction_from_txn(
                    self.ledger,
                    txn,
                    Some(&index_config),
                    self.core.policy.as_ref(),
                    Some(&tracker),
                )
                .await?;

            // Add extracted transaction metadata and graph delta to commit opts
            let commit_opts = self
                .core
                .commit_opts
                .with_txn_meta(txn_meta)
                .with_graph_delta(graph_delta.into_iter().collect());

            // No-op updates: return success without committing.
            let (receipt, ledger) =
                if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                    let (base, flakes) = view.into_parts();
                    debug_assert!(
                        flakes.is_empty(),
                        "no-op transaction path requires zero staged flakes"
                    );
                    (
                        fluree_db_transact::CommitReceipt {
                            commit_id: ContentId::new(ContentKind::Commit, &[]),
                            t: base.t(),
                            flake_count: 0,
                        },
                        base,
                    )
                } else {
                    self.fluree
                        .commit_staged(view, ns_registry, &index_config, commit_opts)
                        .await?
                };

            // Compute indexing status AFTER publish_commit succeeds
            let indexing_enabled =
                self.fluree.indexing_mode.is_enabled() && self.fluree.defaults_indexing_enabled();
            let indexing_needed = ledger.should_reindex(&index_config);
            let indexing_status = IndexingStatus {
                enabled: indexing_enabled,
                needed: indexing_needed,
                novelty_size: ledger.novelty_size(),
                index_t: ledger.index_t(),
                commit_t: receipt.t,
            };

            // Trigger indexing AFTER publish_commit succeeds (fast operation)
            if let IndexingMode::Background(handle) = &self.fluree.indexing_mode {
                if indexing_enabled && indexing_needed {
                    handle.trigger(ledger.ledger_id(), receipt.t).await;
                }
            }

            return Ok(TransactResult {
                receipt,
                ledger,
                indexing: indexing_status,
            });
        }

        let op = self.core.operation.unwrap_or_else(|| {
            unreachable!("validate ensures operation exists when pre_built_txn is None")
        });

        // Direct flake path for InsertTurtle (bypass JSON-LD / IR)
        if let TransactOperation::InsertTurtle(turtle) = op {
            return self
                .fluree
                .insert_turtle_with_opts(
                    self.ledger,
                    turtle,
                    self.core.txn_opts,
                    self.core.commit_opts,
                    &index_config,
                )
                .await;
        }

        let txn_type = op.txn_type();
        // Parse transaction, extracting TriG metadata and named graphs for Turtle inputs
        let parsed = op.to_json_with_trig_meta()?;
        let txn_json = parsed.json;
        let trig_meta = parsed.trig_meta;
        let named_graphs = parsed.named_graphs;

        // Spawn raw transaction upload in parallel with the rest of the
        // pipeline when explicitly opted-in, or let downstream attach it if
        // a signed credential envelope has already been pre-set.
        let store_raw_txn = self.core.txn_opts.store_raw_txn.unwrap_or(false);
        let commit_opts = if self.core.commit_opts.raw_txn.is_none()
            && self.core.commit_opts.raw_txn_upload.is_none()
            && store_raw_txn
        {
            let content_store = self.fluree.content_store(self.ledger.ledger_id());
            self.core
                .commit_opts
                .with_raw_txn_spawned(content_store, txn_json.clone())
        } else {
            self.core.commit_opts
        };

        // If policy + tracking are set, use the tracked+policy path
        // TODO: Add named_graphs support to tracked+policy path
        if let Some(policy) = &self.core.policy {
            let input =
                TrackedTransactionInput::new(txn_type, &txn_json, self.core.txn_opts, policy);
            let (result, _tally) = self
                .fluree
                .transact_tracked_with_policy(self.ledger, input, commit_opts, &index_config)
                .await
                .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?;
            return Ok(result);
        }

        // Standard path: delegate to transact_with_named_graphs
        self.fluree
            .transact_with_named_graphs(
                self.ledger,
                txn_type,
                &txn_json,
                self.core.txn_opts,
                commit_opts,
                &index_config,
                trig_meta.as_ref(),
                &named_graphs,
            )
            .await
    }

    /// Stage the transaction without committing.
    ///
    /// Returns a [`Staged`] that can be queried and later committed.
    pub async fn stage(self) -> Result<Staged> {
        self.core.validate().map_err(ApiError::Builder)?;

        let index_config = self
            .core
            .index_config
            .clone()
            .unwrap_or_else(crate::server_defaults::default_index_config);

        // Pre-built Txn IR path
        if let Some(txn) = self.core.pre_built_txn {
            let tracker = self
                .core
                .tracking
                .clone()
                .map(Tracker::new)
                .unwrap_or_default();
            let stage_result = self
                .fluree
                .stage_transaction_from_txn(
                    self.ledger,
                    txn,
                    Some(&index_config),
                    self.core.policy.as_ref(),
                    Some(&tracker),
                )
                .await?;
            return Ok(Staged {
                view: stage_result.view,
                ns_registry: stage_result.ns_registry,
                graph_delta: stage_result.graph_delta,
            });
        }

        let op = self.core.operation.unwrap_or_else(|| {
            unreachable!("validate ensures operation exists when pre_built_txn is None")
        });

        // Direct flake path for InsertTurtle
        if let TransactOperation::InsertTurtle(turtle) = op {
            let stage_result = self
                .fluree
                .stage_turtle_insert(self.ledger, turtle, Some(&index_config))
                .await?;
            return Ok(Staged {
                view: stage_result.view,
                ns_registry: stage_result.ns_registry,
                graph_delta: stage_result.graph_delta,
            });
        }

        let txn_type = op.txn_type();
        // Parse transaction, extracting TriG metadata and named graphs for Turtle inputs
        let parsed = op.to_json_with_trig_meta()?;
        let txn_json = parsed.json;
        let trig_meta = parsed.trig_meta;
        let named_graphs = parsed.named_graphs;

        // If policy is set, use the tracked+policy staging path
        // TODO: Add named_graphs support to tracked+policy path
        if let Some(policy) = &self.core.policy {
            let tracker = Tracker::new(self.core.tracking.unwrap_or(TrackingOptions {
                track_time: true,
                track_fuel: true,
                track_policy: true,
                max_fuel: None,
            }));
            let input =
                TrackedTransactionInput::new(txn_type, &txn_json, self.core.txn_opts, policy);
            let stage_result = self
                .fluree
                .stage_transaction_tracked_with_policy(
                    self.ledger,
                    input,
                    Some(&index_config),
                    &tracker,
                )
                .await
                .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?;

            return Ok(Staged {
                view: stage_result.view,
                ns_registry: stage_result.ns_registry,
                graph_delta: stage_result.graph_delta,
            });
        }

        // Standard staging path with named graphs support
        let stage_result = self
            .fluree
            .stage_transaction_with_named_graphs(
                self.ledger,
                txn_type,
                &txn_json,
                self.core.txn_opts,
                Some(&index_config),
                trig_meta.as_ref(),
                &named_graphs,
            )
            .await?;

        Ok(Staged {
            view: stage_result.view,
            ns_registry: stage_result.ns_registry,
            graph_delta: stage_result.graph_delta,
        })
    }
}

// ============================================================================
// RefTransactBuilder
// ============================================================================

/// Builder for transactions using a cached [`LedgerHandle`].
///
/// Created via [`Fluree::stage()`]. This is the recommended way to transact
/// in server/application contexts. The handle is borrowed and updated
/// in-place on successful commit, ensuring concurrent readers see the update.
///
/// # Example
///
/// ```ignore
/// let handle = fluree.ledger_cached("mydb:main").await?;
/// let result = fluree.stage(&handle)
///     .insert(&data)
///     .execute().await?;
/// ```
pub struct RefTransactBuilder<'a> {
    fluree: &'a Fluree,
    handle: &'a LedgerHandle,
    core: TransactCore<'a>,
}

impl<'a> RefTransactBuilder<'a> {
    /// Create a new builder (called by `Fluree::stage()`).
    pub(crate) fn new(fluree: &'a Fluree, handle: &'a LedgerHandle) -> Self {
        Self {
            fluree,
            handle,
            core: TransactCore::new(),
        }
    }

    // -- Operation setters --

    /// Set the operation to insert JSON-LD data.
    pub fn insert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::InsertJson(data));
        self
    }

    /// Set the operation to upsert JSON-LD data.
    pub fn upsert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpsertJson(data));
        self
    }

    /// Set the operation to update with WHERE/DELETE/INSERT semantics.
    pub fn update(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpdateJson(data));
        self
    }

    /// Set the operation to insert Turtle data.
    pub fn insert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::InsertTurtle(turtle));
        self
    }

    /// Set the operation to upsert Turtle data.
    pub fn upsert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::UpsertTurtle(turtle));
        self
    }

    /// Set a pre-built transaction IR (bypasses JSON/Turtle parsing).
    ///
    /// This is used for SPARQL UPDATE where the transaction is already
    /// lowered to the IR representation.
    pub fn txn(mut self, txn: Txn) -> Self {
        self.core.set_pre_built_txn(txn);
        self
    }

    // -- Option setters --

    /// Set transaction options (author, context, etc.).
    pub fn txn_opts(mut self, opts: TxnOpts) -> Self {
        self.core.txn_opts = opts;
        self
    }

    /// Set commit options (message, author, etc.).
    pub fn commit_opts(mut self, opts: CommitOpts) -> Self {
        self.core.commit_opts = opts;
        self
    }

    /// Override the index configuration.
    pub fn index_config(mut self, config: IndexConfig) -> Self {
        self.core.index_config = Some(config);
        self
    }

    /// Enable tracking with custom options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.tracking = Some(opts);
        self
    }

    /// Set policy enforcement for the transaction.
    pub fn policy(mut self, ctx: PolicyContext) -> Self {
        self.core.policy = Some(ctx);
        self
    }

    // -- Terminal operations --

    /// Validate the builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        self.core.validate()
    }

    /// Stage + commit the transaction, updating the handle in-place.
    pub async fn execute(self) -> Result<TransactResultRef> {
        commit_with_handle(self.fluree, self.handle, self.core).await
    }
}

// ============================================================================
// Shared commit helper (used by RefTransactBuilder and GraphTransactBuilder)
// ============================================================================

/// Stage and commit a transaction against a cached ledger handle.
///
/// This is the shared logic for `RefTransactBuilder::execute()` and
/// `GraphTransactBuilder::commit()`.
pub(crate) async fn commit_with_handle(
    fluree: &Fluree,
    handle: &LedgerHandle,
    core: TransactCore<'_>,
) -> Result<TransactResultRef> {
    core.validate().map_err(ApiError::Builder)?;

    let index_config = core
        .index_config
        .clone()
        .unwrap_or_else(crate::server_defaults::default_index_config);
    let store_raw_txn = core.txn_opts.store_raw_txn.unwrap_or(false);

    // Create tracker from builder-level tracking options when present.
    // This tracker is passed into the staging pipeline so fuel is counted per flake.
    let tracker = core
        .tracking
        .map(Tracker::new)
        .unwrap_or_else(Tracker::disabled);

    // Fast path retains legacy behavior for complex cases that cannot be retried
    // without cloning inputs (e.g., pre-built Txn IR), or when using tracked+policy staging.
    if core.pre_built_txn.is_some() || core.policy.is_some() {
        // Acquire write lock
        let mut write_guard = handle.lock_for_write().await;
        let ledger_state = write_guard.clone_state();

        // Handle pre-built Txn (SPARQL UPDATE) vs operation-based transaction
        let (stage_result, txn_type, commit_opts) = if let Some(txn) = core.pre_built_txn {
            let txn_type = txn.txn_type;
            // For pre-built Txn, don't attach raw_txn (we don't have the original format)
            let stage_result = fluree
                .stage_transaction_from_txn(
                    ledger_state,
                    txn,
                    Some(&index_config),
                    core.policy.as_ref(),
                    Some(&tracker),
                )
                .await?;
            (stage_result, txn_type, core.commit_opts)
        } else {
            let op = core.operation.unwrap(); // safe: validate checks

            // Direct flake path for InsertTurtle (bypass JSON-LD / IR)
            if let TransactOperation::InsertTurtle(turtle) = op {
                let ledger_id = ledger_state.ledger_id().to_string();
                let stage_result = fluree
                    .stage_turtle_insert(ledger_state, turtle, Some(&index_config))
                    .await?;
                // Spawn raw Turtle upload when explicitly opted-in — overlaps
                // with the commit prelude (sequencing lookup, envelope apply).
                let commit_opts = if core.commit_opts.raw_txn.is_none()
                    && core.commit_opts.raw_txn_upload.is_none()
                    && store_raw_txn
                {
                    let content_store = fluree.content_store(&ledger_id);
                    core.commit_opts.with_raw_txn_spawned(
                        content_store,
                        serde_json::Value::String(turtle.to_string()),
                    )
                } else {
                    core.commit_opts
                };
                (stage_result, TxnType::Insert, commit_opts)
            } else {
                let txn_type = op.txn_type();
                // Parse transaction, extracting TriG metadata and named graphs for Turtle inputs
                let parsed = op.to_json_with_trig_meta()?;
                let txn_json = parsed.json;
                let trig_meta = parsed.trig_meta;
                let named_graphs = parsed.named_graphs;
                let ledger_id = ledger_state.ledger_id().to_string();

                // Spawn raw_txn upload when explicitly opted-in, or skip if a
                // signed credential envelope has already been pre-set.
                let commit_opts = if core.commit_opts.raw_txn.is_none()
                    && core.commit_opts.raw_txn_upload.is_none()
                    && store_raw_txn
                {
                    let content_store = fluree.content_store(&ledger_id);
                    core.commit_opts
                        .with_raw_txn_spawned(content_store, txn_json.clone())
                } else {
                    core.commit_opts
                };

                // Stage with external tracker when tracking is enabled
                let tracker_ref = if tracker.is_enabled() {
                    Some(&tracker)
                } else {
                    None
                };
                let stage_result = fluree
                    .stage_transaction_with_named_graphs_tracked(
                        ledger_state,
                        txn_type,
                        &txn_json,
                        core.txn_opts,
                        Some(&index_config),
                        trig_meta.as_ref(),
                        &named_graphs,
                        tracker_ref,
                        core.policy.as_ref(),
                    )
                    .await?;
                (stage_result, txn_type, commit_opts)
            }
        };

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = stage_result;

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // Handle no-op
        let (receipt, mut new_state) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, _) = view.into_parts();
                (
                    fluree_db_transact::CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                fluree
                    .commit_staged(view, ns_registry, &index_config, commit_opts)
                    .await?
            };

        // Compute indexing status
        let indexing_status = IndexingStatus {
            enabled: fluree.indexing_mode.is_enabled(),
            needed: new_state.should_reindex(&index_config),
            novelty_size: new_state.novelty_size(),
            index_t: new_state.index_t(),
            commit_t: receipt.t,
        };

        if crate::ns_helpers::binary_store_missing_snapshot_namespaces(&new_state) {
            let cache_dir = fluree.binary_store_cache_dir();
            // Result unused: load_and_attach mutates new_state in-place
            let _store = crate::ledger_manager::load_and_attach_binary_store(
                fluree.backend(),
                &mut new_state,
                &cache_dir,
                Some(std::sync::Arc::clone(fluree.leaflet_cache())),
            )
            .await?;
        }

        // Update cache — sync binary_store BEFORE replacing state so that
        // concurrent readers never see the new state with a stale binary_store.
        handle.sync_binary_store_from_state(&new_state).await;
        write_guard.replace(new_state);

        // Trigger background indexing if needed (outside cache update is fine here)
        if let IndexingMode::Background(h) = &fluree.indexing_mode {
            if indexing_status.needed {
                h.trigger(handle.ledger_id(), receipt.t).await;
            }
        }

        return Ok(TransactResultRef {
            receipt,
            indexing: indexing_status,
            tally: tracker.tally(),
        });
    }

    // Optimistic staging path: stage outside the write lock to allow parallel parsing/staging
    // in bulk import scenarios. Commit is still serialized by the write lock for safety.
    let op = core.operation.unwrap(); // safe: validate checks
    let txn_opts = core.txn_opts.clone();
    let commit_opts_base = core.commit_opts.clone();

    // Pre-parse JSON/TriG once when possible (independent of ledger state).
    enum OpPlan<'a> {
        InsertTurtle(&'a str),
        JsonLike {
            txn_type: TxnType,
            txn_json: JsonValue,
            trig_meta: Option<RawTrigMeta>,
            named_graphs: Vec<NamedGraphBlock>,
        },
    }

    let op_plan = match op {
        TransactOperation::InsertTurtle(turtle) => OpPlan::InsertTurtle(turtle),
        _ => {
            let txn_type = op.txn_type();
            let parsed = op.to_json_with_trig_meta()?;
            OpPlan::JsonLike {
                txn_type,
                txn_json: parsed.json,
                trig_meta: parsed.trig_meta,
                named_graphs: parsed.named_graphs,
            }
        }
    };

    let tracker_ref = if tracker.is_enabled() {
        Some(&tracker)
    } else {
        None
    };

    const MAX_RETRIES: usize = 16;
    for _attempt in 0..MAX_RETRIES {
        // Snapshot current cached state (brief lock), then stage without holding the write lock.
        let snap = handle.snapshot().await;
        let base_t = snap.t;
        let base_head_id = snap.head_commit_id.clone();
        let ledger_state = snap.to_ledger_state();

        let ledger_id = ledger_state.ledger_id().to_string();
        let (stage_result, txn_type, commit_opts) = match &op_plan {
            OpPlan::InsertTurtle(turtle) => {
                // Spawn raw Turtle upload in parallel with staging when opted in.
                // On retry, the prior attempt's pending upload was released via its
                // Drop guard; this iteration re-spawns a fresh upload.
                let commit_opts = if commit_opts_base.raw_txn.is_none() && store_raw_txn {
                    let content_store = fluree.content_store(&ledger_id);
                    commit_opts_base.clone().with_raw_txn_spawned(
                        content_store,
                        serde_json::Value::String((*turtle).to_string()),
                    )
                } else {
                    commit_opts_base.clone()
                };
                let stage_result = fluree
                    .stage_turtle_insert(ledger_state, turtle, Some(&index_config))
                    .await?;
                (stage_result, TxnType::Insert, commit_opts)
            }
            OpPlan::JsonLike {
                txn_type,
                txn_json,
                trig_meta,
                named_graphs,
            } => {
                // Spawn raw_txn upload in parallel with staging when opted in.
                let commit_opts = if commit_opts_base.raw_txn.is_none() && store_raw_txn {
                    let content_store = fluree.content_store(&ledger_id);
                    commit_opts_base
                        .clone()
                        .with_raw_txn_spawned(content_store, txn_json.clone())
                } else {
                    commit_opts_base.clone()
                };

                let stage_result = fluree
                    .stage_transaction_with_named_graphs_tracked(
                        ledger_state,
                        *txn_type,
                        txn_json,
                        txn_opts.clone(),
                        Some(&index_config),
                        trig_meta.as_ref(),
                        named_graphs,
                        tracker_ref,
                        None,
                    )
                    .await?;
                (stage_result, *txn_type, commit_opts)
            }
        };

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = stage_result;

        // Acquire write lock only for the commit + cache update section.
        let mut write_guard = handle.lock_for_write().await;
        let current_t = write_guard.state().t();
        let current_head_id = write_guard.state().head_commit_id.as_ref();

        // If state changed since snapshot, retry staging against the latest state.
        if current_t != base_t || current_head_id != base_head_id.as_ref() {
            continue;
        }

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // Handle no-op updates: return success without committing.
        if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
            let (base, _) = view.into_parts();
            return Ok(TransactResultRef {
                receipt: fluree_db_transact::CommitReceipt {
                    commit_id: ContentId::new(ContentKind::Commit, &[]),
                    t: base.t(),
                    flake_count: 0,
                },
                indexing: IndexingStatus {
                    enabled: fluree.indexing_mode.is_enabled(),
                    needed: false,
                    novelty_size: base.novelty_size(),
                    index_t: base.index_t(),
                    commit_t: base.t(),
                },
                tally: tracker.tally(),
            });
        }

        let (receipt, mut new_state) = fluree
            .commit_staged(view, ns_registry, &index_config, commit_opts)
            .await?;

        let indexing_status = IndexingStatus {
            enabled: fluree.indexing_mode.is_enabled(),
            needed: new_state.should_reindex(&index_config),
            novelty_size: new_state.novelty_size(),
            index_t: new_state.index_t(),
            commit_t: receipt.t,
        };

        if crate::ns_helpers::binary_store_missing_snapshot_namespaces(&new_state) {
            let cache_dir = fluree.binary_store_cache_dir();
            // Result unused: load_and_attach mutates new_state in-place
            let _store = crate::ledger_manager::load_and_attach_binary_store(
                fluree.backend(),
                &mut new_state,
                &cache_dir,
                Some(std::sync::Arc::clone(fluree.leaflet_cache())),
            )
            .await?;
        }

        // Update cache — sync binary_store BEFORE replacing state so that
        // concurrent readers never see the new state with a stale binary_store.
        handle.sync_binary_store_from_state(&new_state).await;
        write_guard.replace(new_state);
        drop(write_guard);

        // Trigger background indexing if needed (after cache update; no need to hold lock)
        if let IndexingMode::Background(h) = &fluree.indexing_mode {
            if indexing_status.needed {
                h.trigger(handle.ledger_id(), receipt.t).await;
            }
        }

        return Ok(TransactResultRef {
            receipt,
            indexing: indexing_status,
            tally: tracker.tally(),
        });
    }

    Err(ApiError::internal(format!(
        "transaction commit retry limit exceeded ({MAX_RETRIES} attempts)"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;
    use serde_json::json;

    // ========================================================================
    // Validation tests
    // ========================================================================

    #[test]
    fn test_transact_core_missing_operation() {
        let core = TransactCore::new();
        let result = core.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert_eq!(errs.0.len(), 1);
        assert!(matches!(
            &errs.0[0],
            BuilderError::Missing {
                field: "operation",
                ..
            }
        ));
    }

    #[test]
    fn test_transact_core_double_operation_conflict() {
        let json1 =
            json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:name": "Alice"});
        let json2 =
            json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:b", "ex:name": "Bob"});
        let mut core = TransactCore::new();
        core.set_operation(TransactOperation::InsertJson(&json1));
        core.set_operation(TransactOperation::UpsertJson(&json2));
        let result = core.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.0.iter().any(|e| matches!(
            e,
            BuilderError::Conflict {
                field: "operation",
                ..
            }
        )));
    }

    #[test]
    fn test_transact_core_valid_insert() {
        let json =
            json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:name": "Alice"});
        let mut core = TransactCore::new();
        core.set_operation(TransactOperation::InsertJson(&json));
        let result = core.validate();
        assert!(result.is_ok());
    }

    // ========================================================================
    // OwnedTransactBuilder validation tests
    // ========================================================================

    #[tokio::test]
    async fn test_owned_builder_missing_operation() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let result = fluree.stage_owned(ledger).execute().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), 400);
    }

    #[tokio::test]
    async fn test_owned_builder_double_operation() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data1 = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let data2 = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:b", "ex:name": "Bob"}]});

        let result = fluree
            .stage_owned(ledger)
            .insert(&data1)
            .upsert(&data2)
            .execute()
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), 400);
    }

    // ========================================================================
    // Integration tests
    // ========================================================================

    #[tokio::test]
    async fn test_owned_builder_insert() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree.stage_owned(ledger).insert(&data).execute().await;
        assert!(result.is_ok());
        let txn_result = result.unwrap();
        assert_eq!(txn_result.receipt.t, 1);
    }

    #[tokio::test]
    async fn test_owned_builder_upsert() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree.stage_owned(ledger).upsert(&data).execute().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_owned_builder_with_commit_opts() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree
            .stage_owned(ledger)
            .insert(&data)
            .commit_opts(CommitOpts::default())
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_owned_builder_equivalence_with_convenience() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Via convenience method
        let ledger1 = fluree.create_ledger("testdb1").await.unwrap();
        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result1 = fluree.insert(ledger1, &data).await.unwrap();

        // Via builder
        let ledger2 = fluree.create_ledger("testdb2").await.unwrap();
        let result2 = fluree
            .stage_owned(ledger2)
            .insert(&data)
            .execute()
            .await
            .unwrap();

        // Both should succeed at t=1
        assert_eq!(result1.receipt.t, 1);
        assert_eq!(result2.receipt.t, 1);
    }

    #[tokio::test]
    async fn test_owned_builder_stage_without_commit() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let staged = fluree.stage_owned(ledger).insert(&data).stage().await;
        assert!(staged.is_ok());
        let staged = staged.unwrap();
        assert!(staged.view.has_staged());
    }

    #[tokio::test]
    async fn test_owned_builder_execute_pre_built_txn_ir() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        // Seed data at t=1
        let seed = json!({
            "insert": [{
                "@id": "http://example.org/a",
                "http://example.org/seq": 1
            }]
        });
        let seeded = fluree
            .stage_owned(ledger)
            .insert(&seed)
            .execute()
            .await
            .unwrap();
        assert_eq!(seeded.receipt.t, 1);

        // Build a SPARQL UPDATE Txn IR (Modify) and execute via stage_owned().txn(txn).execute().
        // This must NOT panic (regression for OwnedTransactBuilder::execute unwrap bug).
        let sparql_update = r"
            INSERT { <http://example.org/counter> <http://example.org/next> ?next }
            WHERE  {
              <http://example.org/a> <http://example.org/seq> ?n .
              BIND((?n + 1) AS ?next)
            }
        ";
        let parsed = fluree_db_sparql::parse_sparql(sparql_update);
        assert!(
            !parsed.has_errors(),
            "SPARQL parse failed: {:?}",
            parsed.diagnostics
        );
        let ast = parsed.ast.expect("expected SPARQL AST");

        let mut ns = NamespaceRegistry::from_db(&seeded.ledger.snapshot);
        let txn = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
            .expect("lower SPARQL UPDATE AST to Txn IR");

        let result = fluree
            .stage_owned(seeded.ledger)
            .txn(txn)
            .execute()
            .await
            .unwrap();
        assert_eq!(result.receipt.t, 2);
    }

    #[tokio::test]
    async fn test_owned_builder_validate() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});

        // Valid builder
        let builder = fluree.stage_owned(ledger).insert(&data);
        assert!(builder.validate().is_ok());
    }

    #[tokio::test]
    async fn test_ref_builder_insert() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();
        let handle = fluree.ledger_cached("testdb:main").await.unwrap();

        let data = json!({"@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree.stage(&handle).insert(&data).execute().await;
        assert!(result.is_ok());
        let txn_result = result.unwrap();
        assert_eq!(txn_result.receipt.t, 1);

        // Handle should be updated
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.t, 1);
    }
}
