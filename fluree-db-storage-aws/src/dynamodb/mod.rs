//! DynamoDB nameservice implementation (composite-key layout v2)
//!
//! Stores ledger and graph-source metadata using a composite primary key
//! (`pk` + `sk`) with separate concern items and a GSI for listing by kind.
//!
//! For the schema specification, see:
//! - `docs/operations/dynamodb-guide.md` (operator-focused)
//! - `fluree-db-storage-aws/src/dynamodb/schema.rs` (authoritative attribute constants)

pub mod schema;

use async_trait::async_trait;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, KeysAndAttributes, Projection, ProjectionType, Put, ScalarAttributeType,
    TransactWriteItem, Update,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_types::timeout::TimeoutConfig;
use fluree_db_core::ledger_id::{
    format_ledger_id, normalize_ledger_id, split_ledger_id, DEFAULT_BRANCH,
};
use fluree_db_core::ContentId;
use fluree_db_nameservice::{
    AdminPublisher, CasResult, ConfigCasResult, ConfigLookup, ConfigPayload, ConfigPublisher,
    ConfigValue, GraphSourceLookup, GraphSourcePublisher, GraphSourceRecord, GraphSourceType,
    NameService, NameServiceError, NsLookupResult, NsRecord, Publisher, RefKind, RefLookup,
    RefPublisher, RefValue, StatusCasResult, StatusLookup, StatusPayload, StatusPublisher,
    StatusValue,
};
use schema::*;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type Item = HashMap<String, AttributeValue>;

/// DynamoDB nameservice configuration
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    /// DynamoDB table name
    pub table_name: String,
    /// AWS region (optional, uses SDK default if not specified)
    pub region: Option<String>,
    /// Optional endpoint override (e.g. LocalStack)
    pub endpoint: Option<String>,
    /// Timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

/// DynamoDB-based nameservice (composite-key layout v2)
///
/// Each ledger ID maps to multiple DynamoDB items (one per concern: meta, head,
/// index, config, status). Init operations materialize all concern items
/// atomically; subsequent writes are plain UpdateItem.
#[derive(Clone)]
pub struct DynamoDbNameService {
    client: Client,
    table_name: String,
}

impl std::fmt::Debug for DynamoDbNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoDbNameService")
            .field("table_name", &self.table_name)
            .finish()
    }
}

// ─── Constructors ───────────────────────────────────────────────────────────

impl DynamoDbNameService {
    /// Create a new DynamoDB nameservice from SDK config.
    pub async fn new(
        sdk_config: &aws_config::SdkConfig,
        config: DynamoDbConfig,
    ) -> crate::error::Result<Self> {
        let mut builder = aws_sdk_dynamodb::config::Builder::from(sdk_config);

        if let Some(region_str) = config.region {
            builder = builder.region(aws_sdk_dynamodb::config::Region::new(region_str));
        }
        if let Some(endpoint) = config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        if let Some(timeout_ms) = config.timeout_ms {
            let timeout_config = TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(timeout_ms))
                .build();
            builder = builder.timeout_config(timeout_config);
        }

        let client = Client::from_conf(builder.build());
        Ok(Self {
            client,
            table_name: config.table_name,
        })
    }

    /// Create from a pre-built client (for testing).
    pub fn from_client(client: Client, table_name: String) -> Self {
        Self { client, table_name }
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

impl DynamoDbNameService {
    /// Normalize ledger ID to canonical `name:branch` form.
    fn normalize(ledger_id: &str) -> String {
        normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string())
    }

    /// Current epoch time in milliseconds.
    fn now_epoch_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Query all concern items for a given pk (consistent read).
    async fn query_all_items(&self, pk: &str) -> std::result::Result<Vec<Item>, NameServiceError> {
        let response = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("#pk = :pk")
            .expression_attribute_names("#pk", ATTR_PK)
            .expression_attribute_values(":pk", AttributeValue::S(pk.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB Query failed: {e}")))?;

        Ok(response.items().to_vec())
    }

    /// Find an item with a specific sort key value.
    fn find_item_by_sk<'a>(items: &'a [Item], sk: &str) -> Option<&'a Item> {
        items
            .iter()
            .find(|item| item.get(ATTR_SK).and_then(|v| v.as_s().ok()) == Some(&sk.to_string()))
    }

    /// Assemble an NsRecord from concern items (requires kind=ledger).
    fn items_to_ns_record(pk: &str, items: &[Item]) -> Option<NsRecord> {
        let meta = Self::find_item_by_sk(items, SK_META)?;
        let kind = meta.get(ATTR_KIND)?.as_s().ok()?;
        if kind != KIND_LEDGER {
            return None;
        }

        let name = meta
            .get(ATTR_NAME)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let branch = meta
            .get(ATTR_BRANCH)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| DEFAULT_BRANCH.to_string());
        let retracted = meta
            .get(ATTR_RETRACTED)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);

        let head = Self::find_item_by_sk(items, SK_HEAD);
        let commit_head_id = head
            .and_then(|h| h.get(ATTR_COMMIT_ID))
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<ContentId>().ok());
        let commit_t: i64 = head
            .and_then(|h| h.get(ATTR_COMMIT_T))
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let index = Self::find_item_by_sk(items, SK_INDEX);
        let index_head_id = index
            .and_then(|i| i.get(ATTR_INDEX_ID))
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<ContentId>().ok());
        let index_t: i64 = index
            .and_then(|i| i.get(ATTR_INDEX_T))
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let config = Self::find_item_by_sk(items, SK_CONFIG);
        let default_context = config
            .and_then(|c| c.get(ATTR_DEFAULT_CONTEXT_ADDRESS))
            .and_then(|v| v.as_s().ok())
            .and_then(|s| fluree_db_nameservice::parse_default_context_value(s));

        let source_branch = meta
            .get(ATTR_BP_SOURCE)
            .and_then(|v| v.as_s().ok())
            .cloned();

        let branches = meta
            .get(ATTR_BRANCHES)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        Some(NsRecord {
            ledger_id: pk.to_string(),
            name,
            branch,
            commit_head_id,
            config_id: None,
            commit_t,
            index_head_id,
            index_t,
            default_context,
            retracted,
            source_branch,
            branches,
        })
    }

    /// Assemble a GraphSourceRecord from concern items (requires kind=graph_source).
    fn items_to_gs_record(pk: &str, items: &[Item]) -> Option<GraphSourceRecord> {
        let meta = Self::find_item_by_sk(items, SK_META)?;
        let kind = meta.get(ATTR_KIND)?.as_s().ok()?;
        if kind != KIND_GRAPH_SOURCE {
            return None;
        }

        Self::gs_record_from_meta(
            pk,
            meta,
            Self::find_item_by_sk(items, SK_CONFIG),
            Self::find_item_by_sk(items, SK_INDEX),
        )
    }

    /// Build a GraphSourceRecord from separate meta / config / index items.
    fn gs_record_from_meta(
        pk: &str,
        meta: &Item,
        config_item: Option<&Item>,
        index_item: Option<&Item>,
    ) -> Option<GraphSourceRecord> {
        let name = meta.get(ATTR_NAME).and_then(|v| v.as_s().ok()).cloned()?;
        let branch = meta.get(ATTR_BRANCH).and_then(|v| v.as_s().ok()).cloned()?;
        let retracted = meta
            .get(ATTR_RETRACTED)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);
        let source_type_str = meta
            .get(ATTR_SOURCE_TYPE)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let source_type = GraphSourceType::from_type_string(&source_type_str);
        let dependencies: Vec<String> = meta
            .get(ATTR_DEPENDENCIES)
            .and_then(|v| v.as_l().ok())
            .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
            .unwrap_or_default();

        let config = config_item
            .and_then(|c| c.get(ATTR_CONFIG_JSON))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| "{}".to_string());

        let index_id = index_item
            .and_then(|i| i.get(ATTR_INDEX_ID))
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<ContentId>().ok());
        let index_t: i64 = index_item
            .and_then(|i| i.get(ATTR_INDEX_T))
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        Some(GraphSourceRecord {
            graph_source_id: pk.to_string(),
            name,
            branch,
            source_type,
            config,
            dependencies,
            index_id,
            index_t,
            retracted,
        })
    }

    /// Check whether a meta item exists for the given pk.
    async fn meta_exists(&self, pk: &str) -> std::result::Result<bool, NameServiceError> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .projection_expression("#pk")
            .expression_attribute_names("#pk", ATTR_PK)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {e}")))?;

        Ok(response.item().is_some())
    }

    /// Read the `kind` field from the meta item for the given pk.
    ///
    /// Returns `None` if no meta item exists.
    async fn meta_kind(&self, pk: &str) -> std::result::Result<Option<String>, NameServiceError> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .projection_expression("#kind")
            .expression_attribute_names("#kind", ATTR_KIND)
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {e}")))?;

        Ok(response
            .item()
            .and_then(|item| item.get(ATTR_KIND))
            .and_then(|v| v.as_s().ok())
            .cloned())
    }

    /// Map RefKind → sort key value.
    fn ref_kind_sk(kind: RefKind) -> &'static str {
        match kind {
            RefKind::CommitHead => SK_HEAD,
            RefKind::IndexHead => SK_INDEX,
        }
    }

    /// Map RefKind → (id_attr, t_attr).
    fn ref_kind_attrs(kind: RefKind) -> (&'static str, &'static str) {
        match kind {
            RefKind::CommitHead => (ATTR_COMMIT_ID, ATTR_COMMIT_T),
            RefKind::IndexHead => (ATTR_INDEX_ID, ATTR_INDEX_T),
        }
    }

    /// Query GSI1 for all meta items of a given kind, with pagination.
    async fn query_gsi_by_kind(
        &self,
        kind: &str,
    ) -> std::result::Result<Vec<Item>, NameServiceError> {
        let mut items = Vec::new();
        let mut last_key = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .index_name(GSI1_NAME)
                .key_condition_expression("#kind = :kind")
                .expression_attribute_names("#kind", ATTR_KIND)
                .expression_attribute_values(":kind", AttributeValue::S(kind.to_string()));

            if let Some(key) = last_key.take() {
                query = query.set_exclusive_start_key(Some(key));
            }

            let response = query.send().await.map_err(|e| {
                NameServiceError::storage(format!("DynamoDB GSI query failed: {e}"))
            })?;

            items.extend(response.items().iter().cloned());

            match response.last_evaluated_key() {
                Some(key) if !key.is_empty() => last_key = Some(key.clone()),
                _ => break,
            }
        }

        Ok(items)
    }

    // ── DynamoDB error classification ───────────────────────────────────

    fn is_conditional_check_failed(
        err: &aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
        >,
    ) -> bool {
        use aws_sdk_dynamodb::error::SdkError;
        use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
        match err {
            SdkError::ServiceError(se) => {
                matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                )
            }
            _ => false,
        }
    }

    fn is_transaction_canceled(
        err: &aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError,
        >,
    ) -> bool {
        use aws_sdk_dynamodb::error::SdkError;
        use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
        match err {
            SdkError::ServiceError(se) => {
                matches!(
                    se.err(),
                    TransactWriteItemsError::TransactionCanceledException(_)
                )
            }
            _ => false,
        }
    }

    // ── JSON ↔ DynamoDB conversion helpers ──────────────────────────────

    fn dynamo_map_to_json_map(
        map: &HashMap<String, AttributeValue>,
    ) -> HashMap<String, serde_json::Value> {
        map.iter()
            .filter_map(|(k, v)| Self::dynamo_attr_to_json(v).map(|val| (k.clone(), val)))
            .collect()
    }

    fn dynamo_attr_to_json(attr: &AttributeValue) -> Option<serde_json::Value> {
        match attr {
            AttributeValue::S(s) => Some(serde_json::Value::String(s.clone())),
            AttributeValue::N(n) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(serde_json::Value::Number(i.into()))
                } else if let Ok(f) = n.parse::<f64>() {
                    serde_json::Number::from_f64(f).map(serde_json::Value::Number)
                } else {
                    None
                }
            }
            AttributeValue::Bool(b) => Some(serde_json::Value::Bool(*b)),
            AttributeValue::Null(_) => Some(serde_json::Value::Null),
            AttributeValue::L(list) => {
                let items: Vec<_> = list.iter().filter_map(Self::dynamo_attr_to_json).collect();
                Some(serde_json::Value::Array(items))
            }
            AttributeValue::M(map) => {
                let obj: serde_json::Map<_, _> = map
                    .iter()
                    .filter_map(|(k, v)| Self::dynamo_attr_to_json(v).map(|val| (k.clone(), val)))
                    .collect();
                Some(serde_json::Value::Object(obj))
            }
            _ => None,
        }
    }

    fn json_map_to_dynamo_map(map: &HashMap<String, serde_json::Value>) -> AttributeValue {
        let dynamo_map: HashMap<String, AttributeValue> = map
            .iter()
            .filter_map(|(k, v)| Self::json_to_dynamo_attr(v).map(|attr| (k.clone(), attr)))
            .collect();
        AttributeValue::M(dynamo_map)
    }

    fn json_to_dynamo_attr(val: &serde_json::Value) -> Option<AttributeValue> {
        match val {
            serde_json::Value::Null => Some(AttributeValue::Null(true)),
            serde_json::Value::Bool(b) => Some(AttributeValue::Bool(*b)),
            serde_json::Value::Number(n) => Some(AttributeValue::N(n.to_string())),
            serde_json::Value::String(s) => Some(AttributeValue::S(s.clone())),
            serde_json::Value::Array(arr) => {
                let items: Vec<_> = arr.iter().filter_map(Self::json_to_dynamo_attr).collect();
                Some(AttributeValue::L(items))
            }
            serde_json::Value::Object(obj) => {
                let map: HashMap<String, AttributeValue> = obj
                    .iter()
                    .filter_map(|(k, v)| Self::json_to_dynamo_attr(v).map(|a| (k.clone(), a)))
                    .collect();
                Some(AttributeValue::M(map))
            }
        }
    }
}

// ─── NameService ────────────────────────────────────────────────────────────

#[async_trait]
impl NameService for DynamoDbNameService {
    async fn lookup(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<NsRecord>, NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let items = self.query_all_items(&pk).await?;
        Ok(Self::items_to_ns_record(&pk, &items))
    }

    async fn all_records(&self) -> std::result::Result<Vec<NsRecord>, NameServiceError> {
        // 1. Query GSI1 for all ledger meta items
        let meta_items = self.query_gsi_by_kind(KIND_LEDGER).await?;

        // 2. Collect PKs
        let pks: Vec<String> = meta_items
            .iter()
            .filter_map(|item| item.get(ATTR_PK)?.as_s().ok().cloned())
            .collect();

        // 3. For each PK, query all concern items and assemble NsRecord
        let mut records = Vec::with_capacity(pks.len());
        for pk in &pks {
            let items = self.query_all_items(pk).await?;
            if let Some(record) = Self::items_to_ns_record(pk, &items) {
                records.push(record);
            }
        }

        Ok(records)
    }

    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(ContentId, i64)>,
    ) -> std::result::Result<(), NameServiceError> {
        // Look up the source branch to validate it exists (and to get commit
        // info when `at_commit` is None).
        let source_id = format_ledger_id(ledger_name, source_branch);
        let source_record = self.lookup(&source_id).await?.ok_or_else(|| {
            NameServiceError::not_found(format!(
                "Source branch {source_branch} not found for {ledger_name}"
            ))
        })?;

        let (commit_id, commit_t) = match at_commit {
            Some((id, t)) => (id, t),
            None => {
                let id = source_record.commit_head_id.ok_or_else(|| {
                    NameServiceError::storage(format!(
                        "Source branch {source_id} has no commit head"
                    ))
                })?;
                (id, source_record.commit_t)
            }
        };

        let pk = format_ledger_id(ledger_name, new_branch);
        let now = Self::now_epoch_ms().to_string();
        let sv = SCHEMA_VERSION.to_string();

        let base_item = |sk: &str| -> Item {
            HashMap::from([
                (ATTR_PK.to_string(), AttributeValue::S(pk.clone())),
                (ATTR_SK.to_string(), AttributeValue::S(sk.to_string())),
                (
                    ATTR_UPDATED_AT_MS.to_string(),
                    AttributeValue::N(now.clone()),
                ),
                (ATTR_SCHEMA.to_string(), AttributeValue::N(sv.clone())),
            ])
        };

        let cond = "attribute_not_exists(pk)";

        // 1. Meta — includes source branch attribute
        let mut meta = base_item(SK_META);
        meta.insert(
            ATTR_KIND.to_string(),
            AttributeValue::S(KIND_LEDGER.to_string()),
        );
        meta.insert(
            ATTR_NAME.to_string(),
            AttributeValue::S(ledger_name.to_string()),
        );
        meta.insert(
            ATTR_BRANCH.to_string(),
            AttributeValue::S(new_branch.to_string()),
        );
        meta.insert(ATTR_RETRACTED.to_string(), AttributeValue::Bool(false));
        meta.insert(
            ATTR_BP_SOURCE.to_string(),
            AttributeValue::S(source_branch.to_string()),
        );

        // 2. Head — starts at source commit
        let mut head = base_item(SK_HEAD);
        head.insert(
            ATTR_COMMIT_ID.to_string(),
            AttributeValue::S(commit_id.to_string()),
        );
        head.insert(
            ATTR_COMMIT_T.to_string(),
            AttributeValue::N(commit_t.to_string()),
        );

        // 3. Index (unborn)
        let mut index = base_item(SK_INDEX);
        index.insert(ATTR_INDEX_T.to_string(), AttributeValue::N("0".to_string()));

        // 4. Status (ready, v=1)
        let mut status = base_item(SK_STATUS);
        status.insert(
            ATTR_STATUS.to_string(),
            AttributeValue::S(STATUS_READY.to_string()),
        );
        status.insert(
            ATTR_STATUS_V.to_string(),
            AttributeValue::N("1".to_string()),
        );

        // 5. Config (unborn)
        let mut config = base_item(SK_CONFIG);
        config.insert(
            ATTR_CONFIG_V.to_string(),
            AttributeValue::N("0".to_string()),
        );

        let make_put = |item: Item| -> TransactWriteItem {
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&self.table_name)
                        .set_item(Some(item))
                        .condition_expression(cond)
                        .build()
                        .expect("valid Put"),
                )
                .build()
        };

        let result = self
            .client
            .transact_write_items()
            .transact_items(make_put(meta))
            .transact_items(make_put(head))
            .transact_items(make_put(index))
            .transact_items(make_put(status))
            .transact_items(make_put(config))
            .send()
            .await;

        match result {
            Ok(_) => {}
            Err(e) if Self::is_transaction_canceled(&e) => {
                return Err(NameServiceError::ledger_already_exists(&pk));
            }
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "DynamoDB TransactWriteItems failed: {e}"
                )));
            }
        }

        // Increment source branch's child count atomically
        let source_pk = format_ledger_id(ledger_name, source_branch);
        let _ = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(source_pk))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .update_expression("SET #b = if_not_exists(#b, :zero) + :one")
            .expression_attribute_names("#b", ATTR_BRANCHES)
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .send()
            .await
            .map_err(|e| {
                NameServiceError::storage(format!("DynamoDB increment branches failed: {e}"))
            })?;

        Ok(())
    }

    async fn drop_branch(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<u32>, NameServiceError> {
        let pk = Self::normalize(ledger_id);

        // Read all items for this branch to find the parent
        let items = self.query_all_items(&pk).await?;
        let meta = Self::find_item_by_sk(&items, SK_META)
            .ok_or_else(|| NameServiceError::not_found(ledger_id))?;

        let parent_source = meta
            .get(ATTR_BP_SOURCE)
            .and_then(|v| v.as_s().ok())
            .cloned();

        let ledger_name = meta
            .get(ATTR_NAME)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();

        // Atomic linearization point: conditional delete of meta, then sweep
        // every remaining row under this pk. If meta is already gone, another
        // caller already won — surface as NotFound here so we don't decrement
        // the parent's branch count twice.
        if !self.delete_all_rows_for_pk(&pk, &items).await? {
            return Err(NameServiceError::not_found(ledger_id));
        }

        // Decrement parent's child count if this branch had a parent
        match parent_source {
            Some(source) => {
                let parent_pk = format_ledger_id(&ledger_name, &source);
                let result = self
                    .client
                    .update_item()
                    .table_name(&self.table_name)
                    .key(ATTR_PK, AttributeValue::S(parent_pk))
                    .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
                    .update_expression("SET #b = if_not_exists(#b, :zero) - :one")
                    .expression_attribute_names("#b", ATTR_BRANCHES)
                    .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
                    .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
                    .return_values(aws_sdk_dynamodb::types::ReturnValue::UpdatedNew)
                    .send()
                    .await
                    .map_err(|e| {
                        NameServiceError::storage(format!(
                            "DynamoDB decrement branches failed: {e}"
                        ))
                    })?;

                let new_count = result
                    .attributes()
                    .and_then(|attrs| attrs.get(ATTR_BRANCHES))
                    .and_then(|v| v.as_n().ok())
                    .and_then(|s| s.parse().ok());

                Ok(new_count)
            }
            None => Ok(None),
        }
    }

    async fn reset_head(
        &self,
        ledger_id: &str,
        snapshot: fluree_db_nameservice::NsRecordSnapshot,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let now = Self::now_epoch_ms().to_string();

        // Build commit head update
        let mut head = Update::builder()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_HEAD.to_string()))
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":now", AttributeValue::N(now.clone()))
            .expression_attribute_names("#ct", ATTR_COMMIT_T)
            .expression_attribute_names("#ci", ATTR_COMMIT_ID)
            .expression_attribute_values(":t", AttributeValue::N(snapshot.commit_t.to_string()));
        if let Some(ref cid) = snapshot.commit_head_id {
            head = head
                .update_expression("SET #ci = :cid, #ct = :t, #ua = :now")
                .expression_attribute_values(":cid", AttributeValue::S(cid.to_string()));
        } else {
            head = head.update_expression("SET #ct = :t, #ua = :now REMOVE #ci");
        }

        // Build index head update
        let mut idx = Update::builder()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_INDEX.to_string()))
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":now", AttributeValue::N(now.clone()))
            .expression_attribute_names("#it", ATTR_INDEX_T)
            .expression_attribute_names("#ii", ATTR_INDEX_ID)
            .expression_attribute_values(":it", AttributeValue::N(snapshot.index_t.to_string()));
        if let Some(ref id) = snapshot.index_head_id {
            idx = idx
                .update_expression("SET #ii = :iid, #it = :it, #ua = :now")
                .expression_attribute_values(":iid", AttributeValue::S(id.to_string()));
        } else {
            idx = idx.update_expression("SET #it = :it, #ua = :now REMOVE #ii");
        }

        // Combine into a single atomic transaction
        let txn = self
            .client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .update(head.build().expect("valid Update"))
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .update(idx.build().expect("valid Update"))
                    .build(),
            );

        txn.send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB reset_head failed: {e}")))?;

        Ok(())
    }
}

// ─── Publisher ──────────────────────────────────────────────────────────────

#[async_trait]
impl Publisher for DynamoDbNameService {
    async fn publish_ledger_init(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let (ledger_name, branch) = split_ledger_id(ledger_id)
            .unwrap_or_else(|_| (ledger_id.to_string(), DEFAULT_BRANCH.to_string()));
        let now = Self::now_epoch_ms().to_string();
        let sv = SCHEMA_VERSION.to_string();

        // Build a function for creating the common item fields
        let base_item = |sk: &str| -> Item {
            HashMap::from([
                (ATTR_PK.to_string(), AttributeValue::S(pk.clone())),
                (ATTR_SK.to_string(), AttributeValue::S(sk.to_string())),
                (
                    ATTR_UPDATED_AT_MS.to_string(),
                    AttributeValue::N(now.clone()),
                ),
                (ATTR_SCHEMA.to_string(), AttributeValue::N(sv.clone())),
            ])
        };

        // Condition: item must not exist
        let cond = "attribute_not_exists(pk)";

        // 1. Meta
        let mut meta = base_item(SK_META);
        meta.insert(
            ATTR_KIND.to_string(),
            AttributeValue::S(KIND_LEDGER.to_string()),
        );
        meta.insert(ATTR_NAME.to_string(), AttributeValue::S(ledger_name));
        meta.insert(ATTR_BRANCH.to_string(), AttributeValue::S(branch));
        meta.insert(ATTR_RETRACTED.to_string(), AttributeValue::Bool(false));

        // 2. Head (unborn: commit_t=0, no address)
        let mut head = base_item(SK_HEAD);
        head.insert(
            ATTR_COMMIT_T.to_string(),
            AttributeValue::N("0".to_string()),
        );

        // 3. Index (unborn: index_t=0, no address)
        let mut index = base_item(SK_INDEX);
        index.insert(ATTR_INDEX_T.to_string(), AttributeValue::N("0".to_string()));

        // 4. Status (initial: ready, v=1)
        let mut status = base_item(SK_STATUS);
        status.insert(
            ATTR_STATUS.to_string(),
            AttributeValue::S(STATUS_READY.to_string()),
        );
        status.insert(
            ATTR_STATUS_V.to_string(),
            AttributeValue::N("1".to_string()),
        );

        // 5. Config (unborn: config_v=0)
        let mut config = base_item(SK_CONFIG);
        config.insert(
            ATTR_CONFIG_V.to_string(),
            AttributeValue::N("0".to_string()),
        );

        let make_put = |item: Item| -> TransactWriteItem {
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&self.table_name)
                        .set_item(Some(item))
                        .condition_expression(cond)
                        .build()
                        .expect("valid Put"),
                )
                .build()
        };

        let result = self
            .client
            .transact_write_items()
            .transact_items(make_put(meta))
            .transact_items(make_put(head))
            .transact_items(make_put(index))
            .transact_items(make_put(status))
            .transact_items(make_put(config))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_transaction_canceled(&e) => {
                Err(NameServiceError::ledger_already_exists(&pk))
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB TransactWriteItems failed: {e}"
            ))),
        }
    }

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let now = Self::now_epoch_ms().to_string();

        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_HEAD.to_string()))
            .update_expression("SET #ci = :cid, #ct = :t, #ua = :now")
            .condition_expression("attribute_exists(#pk) AND #ct < :t")
            .expression_attribute_names("#pk", ATTR_PK)
            .expression_attribute_names("#ci", ATTR_COMMIT_ID)
            .expression_attribute_names("#ct", ATTR_COMMIT_T)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":cid", AttributeValue::S(commit_id.to_string()))
            .expression_attribute_values(":t", AttributeValue::N(commit_t.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                // Distinguish stale (item exists, t >= new) from missing (not initialized).
                if !self.meta_exists(&pk).await? {
                    return Err(NameServiceError::not_found(format!(
                        "Ledger not initialized: {pk}"
                    )));
                }
                Ok(()) // Stale — silently ignored.
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {e}"
            ))),
        }
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> std::result::Result<(), NameServiceError> {
        self.update_index_item(ledger_id, index_t, index_id, "#it < :t")
            .await
    }

    async fn retract(&self, ledger_id: &str) -> std::result::Result<(), NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let now = Self::now_epoch_ms().to_string();

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .update_expression("SET #ret = :true_val, #ua = :now")
            .expression_attribute_names("#ret", ATTR_RETRACTED)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":true_val", AttributeValue::Bool(true))
            .expression_attribute_values(":now", AttributeValue::N(now))
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB UpdateItem failed: {e}")))?;

        Ok(())
    }

    async fn purge(&self, ledger_id: &str) -> std::result::Result<(), NameServiceError> {
        // Hard drop: delete every row under this pk (meta/head/index/status/
        // config, plus any other ledger-level rows). Idempotent — if the
        // record is already gone we return Ok so repeated drops are safe.
        let pk = Self::normalize(ledger_id);
        let items = self.query_all_items(&pk).await?;
        let _ = self.delete_all_rows_for_pk(&pk, &items).await?;
        Ok(())
    }

    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        Some(Self::normalize(ledger_id))
    }
}

impl DynamoDbNameService {
    /// Conditionally delete the meta row and sweep every remaining row under `pk`.
    ///
    /// Always attempts to sweep non-meta rows, even when the conditional meta
    /// delete fails (meta already gone). That handles the partial-failure
    /// replay case: a prior caller could have deleted meta and crashed before
    /// sweeping head/index/status/config; the next purge must finish the job.
    ///
    /// Returns `Ok(true)` if this caller won the conditional delete on meta
    /// (the linearization point — used by `drop_branch` to gate its single
    /// parent-count decrement). Returns `Ok(false)` if meta was already gone.
    /// In both cases the remaining rows are swept.
    async fn delete_all_rows_for_pk(
        &self,
        pk: &str,
        items: &[Item],
    ) -> std::result::Result<bool, NameServiceError> {
        let meta_was_present = match self
            .client
            .delete_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .condition_expression("attribute_exists(#pk)")
            .expression_attribute_names("#pk", ATTR_PK)
            .send()
            .await
        {
            Ok(_) => true,
            Err(aws_sdk_dynamodb::error::SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    aws_sdk_dynamodb::operation::delete_item::DeleteItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                false
            }
            Err(e) => {
                return Err(NameServiceError::storage(format!(
                    "DynamoDB conditional delete failed: {e}"
                )));
            }
        };

        // Sweep remaining (non-meta) items via BatchWriteItem. DynamoDB
        // BatchWriteItem accepts at most 25 items per call; chunk accordingly.
        // Runs unconditionally so a half-completed purge is finished on the
        // next call.
        let mut delete_requests: Vec<aws_sdk_dynamodb::types::WriteRequest> = items
            .iter()
            .filter_map(|item| {
                let sk_val = item.get(ATTR_SK)?.as_s().ok()?;
                if sk_val == SK_META {
                    return None;
                }
                Some(
                    aws_sdk_dynamodb::types::WriteRequest::builder()
                        .delete_request(
                            aws_sdk_dynamodb::types::DeleteRequest::builder()
                                .key(ATTR_PK, AttributeValue::S(pk.to_string()))
                                .key(ATTR_SK, AttributeValue::S(sk_val.to_string()))
                                .build()
                                .expect("delete request keys set"),
                        )
                        .build(),
                )
            })
            .collect();

        while !delete_requests.is_empty() {
            let chunk_size = delete_requests.len().min(25);
            let chunk: Vec<_> = delete_requests.drain(..chunk_size).collect();

            let mut remaining: std::collections::HashMap<String, Vec<_>> =
                std::collections::HashMap::new();
            remaining.insert(self.table_name.clone(), chunk);

            while !remaining.is_empty() {
                let mut builder = self.client.batch_write_item();
                for (table, batch) in &remaining {
                    builder = builder.request_items(table, batch.clone());
                }

                let result = builder.send().await.map_err(|e| {
                    NameServiceError::storage(format!("DynamoDB batch delete failed: {e}"))
                })?;

                remaining = result.unprocessed_items().cloned().unwrap_or_default();
                if !remaining.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        Ok(meta_was_present)
    }

    /// Shared helper for publish_index and publish_index_allow_equal.
    ///
    /// First attempts the update with only the monotonicity `condition`
    /// (e.g., `#it < :t`). If the item doesn't exist yet (legacy ledgers
    /// where `sk="index"` was never written), the condition check fails;
    /// in that case we retry with `attribute_not_exists(#it) OR {condition}`
    /// to create the missing item. This keeps the common (item-exists) path
    /// free of the extra `attribute_not_exists` evaluation.
    ///
    /// A `meta_exists` pre-check guards against publishing to a completely
    /// uninitialized alias — matching the `publish_commit` contract.
    async fn update_index_item(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
        condition: &str,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = Self::normalize(ledger_id);

        // Guard: the ledger must be initialized (meta item must exist).
        if !self.meta_exists(&pk).await? {
            return Err(NameServiceError::not_found(format!(
                "Ledger not initialized: {pk}"
            )));
        }

        let now = Self::now_epoch_ms().to_string();

        // Fast path: assume the index item already exists (common case).
        let result = self
            .send_index_update(&pk, index_id, index_t, &now, condition)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                // Either (a) the item exists and index_t >= incoming t
                // (stale/duplicate — safe to ignore), or (b) the item is
                // absent so the condition on `#it` failed because the
                // attribute doesn't exist. Retry with the fallback
                // condition to disambiguate.
                let fallback = format!("attribute_not_exists(#it) OR {condition}");
                let retry = self
                    .send_index_update(&pk, index_id, index_t, &now, &fallback)
                    .await;

                match retry {
                    Ok(_) => Ok(()),
                    Err(e) if Self::is_conditional_check_failed(&e) => {
                        // Item exists and index_t >= incoming t — stale publish.
                        Ok(())
                    }
                    Err(e) => Err(NameServiceError::storage(format!(
                        "DynamoDB UpdateItem failed: {e}"
                    ))),
                }
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {e}"
            ))),
        }
    }

    /// Send a single DynamoDB UpdateItem for the index record.
    async fn send_index_update(
        &self,
        pk: &str,
        index_id: &ContentId,
        index_t: i64,
        now: &str,
        condition_expression: &str,
    ) -> std::result::Result<
        aws_sdk_dynamodb::operation::update_item::UpdateItemOutput,
        aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
        >,
    > {
        self.client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(SK_INDEX.to_string()))
            .update_expression("SET #ii = :iid, #it = :t, #ua = :now")
            .condition_expression(condition_expression)
            .expression_attribute_names("#ii", ATTR_INDEX_ID)
            .expression_attribute_names("#it", ATTR_INDEX_T)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":iid", AttributeValue::S(index_id.to_string()))
            .expression_attribute_values(":t", AttributeValue::N(index_t.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .send()
            .await
    }

    /// Create a new ledger via TransactWriteItems with one ref pre-set.
    ///
    /// Used by `compare_and_set_ref(expected=None)` to match StorageNameService
    /// semantics where the "create" case bootstraps the full ledger record.
    async fn create_ledger_with_ref(
        &self,
        pk: &str,
        ledger_id: &str,
        kind: RefKind,
        new: &RefValue,
    ) -> std::result::Result<(), NameServiceError> {
        // Reject setting a watermark without a pointer — avoids a weird
        // "t is advanced but there's nothing to read" state.
        if new.id.is_none() && new.t > 0 {
            return Err(NameServiceError::invalid_id(format!(
                "Cannot create ref with t={} but no id for {pk}",
                new.t
            )));
        }

        let (ledger_name, branch) = split_ledger_id(ledger_id)
            .unwrap_or_else(|_| (ledger_id.to_string(), DEFAULT_BRANCH.to_string()));
        let now = Self::now_epoch_ms().to_string();
        let sv = SCHEMA_VERSION.to_string();

        let base_item = |sk: &str| -> Item {
            HashMap::from([
                (ATTR_PK.to_string(), AttributeValue::S(pk.to_string())),
                (ATTR_SK.to_string(), AttributeValue::S(sk.to_string())),
                (
                    ATTR_UPDATED_AT_MS.to_string(),
                    AttributeValue::N(now.clone()),
                ),
                (ATTR_SCHEMA.to_string(), AttributeValue::N(sv.clone())),
            ])
        };

        let cond = "attribute_not_exists(pk)";

        // Meta
        let mut meta = base_item(SK_META);
        meta.insert(
            ATTR_KIND.to_string(),
            AttributeValue::S(KIND_LEDGER.to_string()),
        );
        meta.insert(ATTR_NAME.to_string(), AttributeValue::S(ledger_name));
        meta.insert(ATTR_BRANCH.to_string(), AttributeValue::S(branch));
        meta.insert(ATTR_RETRACTED.to_string(), AttributeValue::Bool(false));

        // Head — pre-set if CommitHead, else unborn
        let mut head = base_item(SK_HEAD);
        match kind {
            RefKind::CommitHead => {
                if let Some(ref id) = new.id {
                    head.insert(
                        ATTR_COMMIT_ID.to_string(),
                        AttributeValue::S(id.to_string()),
                    );
                }
                head.insert(
                    ATTR_COMMIT_T.to_string(),
                    AttributeValue::N(new.t.to_string()),
                );
            }
            RefKind::IndexHead => {
                head.insert(
                    ATTR_COMMIT_T.to_string(),
                    AttributeValue::N("0".to_string()),
                );
            }
        }

        // Index — pre-set if IndexHead, else unborn
        let mut index = base_item(SK_INDEX);
        match kind {
            RefKind::IndexHead => {
                if let Some(ref id) = new.id {
                    index.insert(ATTR_INDEX_ID.to_string(), AttributeValue::S(id.to_string()));
                }
                index.insert(
                    ATTR_INDEX_T.to_string(),
                    AttributeValue::N(new.t.to_string()),
                );
            }
            RefKind::CommitHead => {
                index.insert(ATTR_INDEX_T.to_string(), AttributeValue::N("0".to_string()));
            }
        }

        // Status (initial: ready, v=1)
        let mut status = base_item(SK_STATUS);
        status.insert(
            ATTR_STATUS.to_string(),
            AttributeValue::S(STATUS_READY.to_string()),
        );
        status.insert(
            ATTR_STATUS_V.to_string(),
            AttributeValue::N("1".to_string()),
        );

        // Config (unborn: config_v=0)
        let mut config = base_item(SK_CONFIG);
        config.insert(
            ATTR_CONFIG_V.to_string(),
            AttributeValue::N("0".to_string()),
        );

        let make_put = |item: Item| -> TransactWriteItem {
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&self.table_name)
                        .set_item(Some(item))
                        .condition_expression(cond)
                        .build()
                        .expect("valid Put"),
                )
                .build()
        };

        let result = self
            .client
            .transact_write_items()
            .transact_items(make_put(meta))
            .transact_items(make_put(head))
            .transact_items(make_put(index))
            .transact_items(make_put(status))
            .transact_items(make_put(config))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_transaction_canceled(&e) => {
                // Race: someone else created the ledger between our get_ref and
                // this transaction. Return a generic error; the caller's CAS
                // retry loop will re-read and handle it.
                Err(NameServiceError::ledger_already_exists(pk))
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB TransactWriteItems failed: {e}"
            ))),
        }
    }
}

// ─── AdminPublisher ─────────────────────────────────────────────────────────

#[async_trait]
impl AdminPublisher for DynamoDbNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> std::result::Result<(), NameServiceError> {
        self.update_index_item(ledger_id, index_t, index_id, "#it <= :t")
            .await
    }
}

// ─── RefLookup ─────────────────────────────────────────────────────────────

#[async_trait]
impl RefLookup for DynamoDbNameService {
    async fn get_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
    ) -> std::result::Result<Option<RefValue>, NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let sk = Self::ref_kind_sk(kind);
        let (id_attr, t_attr) = Self::ref_kind_attrs(kind);

        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(sk.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {e}")))?;

        match response.item() {
            Some(item) => {
                let id = item
                    .get(id_attr)
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| s.parse::<ContentId>().ok());
                let t: i64 = item
                    .get(t_attr)
                    .and_then(|v| v.as_n().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                Ok(Some(RefValue { id, t }))
            }
            None => {
                // Item missing — check if meta exists (fallback)
                if self.meta_exists(&pk).await? {
                    Ok(Some(RefValue { id: None, t: 0 }))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

// ─── RefPublisher ───────────────────────────────────────────────────────────

#[async_trait]
impl RefPublisher for DynamoDbNameService {
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> std::result::Result<CasResult, NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let sk = Self::ref_kind_sk(kind);
        let (id_attr, t_attr) = Self::ref_kind_attrs(kind);

        // ── Case 1: expected = None ─────────────────────────────────────
        // Caller expects the ref doesn't exist. Matches StorageNameService
        // semantics: if ledger_id truly unknown, create the ledger with the ref set.
        let Some(exp) = expected else {
            let current = self.get_ref(ledger_id, kind).await?;
            if current.is_some() {
                return Ok(CasResult::Conflict { actual: current });
            }
            // Ledger ID is truly unknown — create via init with the ref pre-set.
            self.create_ledger_with_ref(&pk, ledger_id, kind, new)
                .await?;
            return Ok(CasResult::Updated);
        };

        // ── Client-side monotonic pre-check ─────────────────────────────
        match kind {
            RefKind::CommitHead => {
                if new.t <= exp.t {
                    let actual = self.get_ref(ledger_id, kind).await?;
                    return Ok(CasResult::Conflict { actual });
                }
            }
            RefKind::IndexHead => {
                if new.t < exp.t {
                    let actual = self.get_ref(ledger_id, kind).await?;
                    return Ok(CasResult::Conflict { actual });
                }
            }
        }

        // ── Case 2: expected unborn (id=None, t=0) ──────────────────────
        if exp.id.is_none() && exp.t == 0 {
            let mut request = self
                .client
                .update_item()
                .table_name(&self.table_name)
                .key(ATTR_PK, AttributeValue::S(pk.clone()))
                .key(ATTR_SK, AttributeValue::S(sk.to_string()))
                .condition_expression(
                    "(attribute_not_exists(#id) OR attribute_type(#id, :null_type)) AND #t = :zero",
                )
                .expression_attribute_names("#id", id_attr)
                .expression_attribute_names("#t", t_attr)
                .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
                .expression_attribute_values(":new_t", AttributeValue::N(new.t.to_string()))
                .expression_attribute_values(
                    ":now",
                    AttributeValue::N(Self::now_epoch_ms().to_string()),
                )
                .expression_attribute_values(":null_type", AttributeValue::S("NULL".to_string()))
                .expression_attribute_values(":zero", AttributeValue::N("0".to_string()));

            let update_expr = if let Some(ref id) = new.id {
                request = request
                    .expression_attribute_values(":new_id", AttributeValue::S(id.to_string()));
                "SET #id = :new_id, #t = :new_t, #ua = :now"
            } else {
                "SET #t = :new_t, #ua = :now"
            };

            let result = request.update_expression(update_expr).send().await;

            return match result {
                Ok(_) => Ok(CasResult::Updated),
                Err(e) if Self::is_conditional_check_failed(&e) => {
                    let actual = self.get_ref(ledger_id, kind).await?;
                    Ok(CasResult::Conflict { actual })
                }
                Err(e) => Err(NameServiceError::storage(format!(
                    "DynamoDB UpdateItem failed: {e}"
                ))),
            };
        }

        // ── Case 3: expected has id ─────────────────────────────────────
        let exp_id = exp.id.as_ref().expect("id must be Some in case 3");

        let mut request = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(sk.to_string()))
            .condition_expression("#id = :exp_id AND #t = :exp_t")
            .expression_attribute_names("#id", id_attr)
            .expression_attribute_names("#t", t_attr)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":new_t", AttributeValue::N(new.t.to_string()))
            .expression_attribute_values(
                ":now",
                AttributeValue::N(Self::now_epoch_ms().to_string()),
            )
            .expression_attribute_values(":exp_id", AttributeValue::S(exp_id.to_string()))
            .expression_attribute_values(":exp_t", AttributeValue::N(exp.t.to_string()));

        let update_expr = if let Some(ref id) = new.id {
            request =
                request.expression_attribute_values(":new_id", AttributeValue::S(id.to_string()));
            "SET #id = :new_id, #t = :new_t, #ua = :now"
        } else {
            "SET #t = :new_t, #ua = :now"
        };

        let result = request.update_expression(update_expr).send().await;

        match result {
            Ok(_) => Ok(CasResult::Updated),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                let actual = self.get_ref(ledger_id, kind).await?;
                Ok(CasResult::Conflict { actual })
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {e}"
            ))),
        }
    }
}

// ─── GraphSourcePublisher ───────────────────────────────────────────────────

#[async_trait]
impl GraphSourcePublisher for DynamoDbNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> std::result::Result<(), NameServiceError> {
        let pk = format_ledger_id(name, branch);
        let now = Self::now_epoch_ms().to_string();
        let sv = SCHEMA_VERSION.to_string();

        let deps_av = AttributeValue::L(
            dependencies
                .iter()
                .map(|d| AttributeValue::S(d.clone()))
                .collect(),
        );

        // 1. Meta (UpdateItem — preserves retracted via if_not_exists)
        let meta_update = Update::builder()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .update_expression(
                "SET #kind = :gs, #st = :src_type, #name = :name, #br = :branch, \
                 #deps = :deps, #ret = if_not_exists(#ret, :false_val), \
                 #ua = :now, #schema = :sv",
            )
            .expression_attribute_names("#kind", ATTR_KIND)
            .expression_attribute_names("#st", ATTR_SOURCE_TYPE)
            .expression_attribute_names("#name", ATTR_NAME)
            .expression_attribute_names("#br", ATTR_BRANCH)
            .expression_attribute_names("#deps", ATTR_DEPENDENCIES)
            .expression_attribute_names("#ret", ATTR_RETRACTED)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_names("#schema", ATTR_SCHEMA)
            .expression_attribute_values(":gs", AttributeValue::S(KIND_GRAPH_SOURCE.to_string()))
            .expression_attribute_values(
                ":src_type",
                AttributeValue::S(source_type.to_type_string()),
            )
            .expression_attribute_values(":name", AttributeValue::S(name.to_string()))
            .expression_attribute_values(":branch", AttributeValue::S(branch.to_string()))
            .expression_attribute_values(":deps", deps_av)
            .expression_attribute_values(":false_val", AttributeValue::Bool(false))
            .expression_attribute_values(":now", AttributeValue::N(now.clone()))
            .expression_attribute_values(":sv", AttributeValue::N(sv))
            .build()
            .expect("valid Update");

        // 2. Config (UpdateItem — bumps config_v monotonically)
        let config_update = Update::builder()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_CONFIG.to_string()))
            .update_expression("SET #cj = :cfg, #cv = if_not_exists(#cv, :zero) + :one, #ua = :now")
            .expression_attribute_names("#cj", ATTR_CONFIG_JSON)
            .expression_attribute_names("#cv", ATTR_CONFIG_V)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":cfg", AttributeValue::S(config.to_string()))
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.clone()))
            .build()
            .expect("valid Update");

        // 3. Index (UpdateItem — create-if-absent, preserve-if-exists)
        let index_update = Update::builder()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_INDEX.to_string()))
            .update_expression(
                "SET #it = if_not_exists(#it, :zero), #ua = if_not_exists(#ua, :now)",
            )
            .expression_attribute_names("#it", ATTR_INDEX_T)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":zero", AttributeValue::N("0".to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.clone()))
            .build()
            .expect("valid Update");

        // 4. Status (UpdateItem — create-if-absent, preserve-if-exists)
        let status_update = Update::builder()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_STATUS.to_string()))
            .update_expression(
                "SET #st = if_not_exists(#st, :ready), \
                 #sv = if_not_exists(#sv, :one), \
                 #ua = if_not_exists(#ua, :now)",
            )
            .expression_attribute_names("#st", ATTR_STATUS)
            .expression_attribute_names("#sv", ATTR_STATUS_V)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":ready", AttributeValue::S(STATUS_READY.to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now))
            .build()
            .expect("valid Update");

        self.client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().update(meta_update).build())
            .transact_items(TransactWriteItem::builder().update(config_update).build())
            .transact_items(TransactWriteItem::builder().update(index_update).build())
            .transact_items(TransactWriteItem::builder().update(status_update).build())
            .send()
            .await
            .map_err(|e| {
                NameServiceError::storage(format!("DynamoDB TransactWriteItems failed: {e}"))
            })?;

        Ok(())
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = format_ledger_id(name, branch);
        let now = Self::now_epoch_ms().to_string();

        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk))
            .key(ATTR_SK, AttributeValue::S(SK_INDEX.to_string()))
            .update_expression("SET #ii = :iid, #it = :t, #ua = :now")
            .condition_expression("#it < :t")
            .expression_attribute_names("#ii", ATTR_INDEX_ID)
            .expression_attribute_names("#it", ATTR_INDEX_T)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":iid", AttributeValue::S(index_id.to_string()))
            .expression_attribute_values(":t", AttributeValue::N(index_t.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_conditional_check_failed(&e) => Ok(()),
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {e}"
            ))),
        }
    }

    async fn retract_graph_source(
        &self,
        name: &str,
        branch: &str,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = format_ledger_id(name, branch);
        let now = Self::now_epoch_ms().to_string();

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .update_expression("SET #ret = :true_val, #ua = :now")
            .expression_attribute_names("#ret", ATTR_RETRACTED)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":true_val", AttributeValue::Bool(true))
            .expression_attribute_values(":now", AttributeValue::N(now))
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB UpdateItem failed: {e}")))?;

        Ok(())
    }
}

#[async_trait]
impl GraphSourceLookup for DynamoDbNameService {
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> std::result::Result<Option<GraphSourceRecord>, NameServiceError> {
        let pk = Self::normalize(graph_source_id);
        let items = self.query_all_items(&pk).await?;
        Ok(Self::items_to_gs_record(&pk, &items))
    }

    async fn lookup_any(
        &self,
        resource_id: &str,
    ) -> std::result::Result<NsLookupResult, NameServiceError> {
        let pk = Self::normalize(resource_id);
        let items = self.query_all_items(&pk).await?;

        // Discriminate by meta.kind
        let meta = Self::find_item_by_sk(&items, SK_META);
        let kind = meta
            .and_then(|m| m.get(ATTR_KIND))
            .and_then(|v| v.as_s().ok());

        match kind {
            Some(k) if k == KIND_LEDGER => {
                if let Some(record) = Self::items_to_ns_record(&pk, &items) {
                    return Ok(NsLookupResult::Ledger(record));
                }
            }
            Some(k) if k == KIND_GRAPH_SOURCE => {
                if let Some(record) = Self::items_to_gs_record(&pk, &items) {
                    return Ok(NsLookupResult::GraphSource(record));
                }
            }
            _ => {}
        }

        Ok(NsLookupResult::NotFound)
    }

    async fn all_graph_source_records(
        &self,
    ) -> std::result::Result<Vec<GraphSourceRecord>, NameServiceError> {
        // 1. Query GSI1 for all graph_source meta items
        let meta_items = self.query_gsi_by_kind(KIND_GRAPH_SOURCE).await?;
        if meta_items.is_empty() {
            return Ok(vec![]);
        }

        // 2. Collect PKs
        let pks: Vec<String> = meta_items
            .iter()
            .filter_map(|item| item.get(ATTR_PK)?.as_s().ok().cloned())
            .collect();

        // 3. BatchGetItem for config + index items (50 PKs × 2 = 100 keys per batch)
        let mut fetched: HashMap<String, Vec<Item>> = HashMap::new();
        for chunk in pks.chunks(50) {
            let keys: Vec<Item> = chunk
                .iter()
                .flat_map(|pk| {
                    vec![
                        HashMap::from([
                            (ATTR_PK.to_string(), AttributeValue::S(pk.clone())),
                            (
                                ATTR_SK.to_string(),
                                AttributeValue::S(SK_CONFIG.to_string()),
                            ),
                        ]),
                        HashMap::from([
                            (ATTR_PK.to_string(), AttributeValue::S(pk.clone())),
                            (ATTR_SK.to_string(), AttributeValue::S(SK_INDEX.to_string())),
                        ]),
                    ]
                })
                .collect();

            let ka = KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .map_err(|e| {
                    NameServiceError::storage(format!("KeysAndAttributes build failed: {e}"))
                })?;

            // BatchGetItem with retry for UnprocessedKeys (throttling).
            let mut pending = Some(ka);
            let max_retries = 5;
            for retry in 0..=max_retries {
                let request_ka = pending.take().expect("pending keys");
                let response = self
                    .client
                    .batch_get_item()
                    .request_items(&self.table_name, request_ka)
                    .send()
                    .await
                    .map_err(|e| {
                        NameServiceError::storage(format!("DynamoDB BatchGetItem failed: {e}"))
                    })?;

                if let Some(table_items) =
                    response.responses().and_then(|r| r.get(&self.table_name))
                {
                    for item in table_items {
                        if let Some(pk_val) = item.get(ATTR_PK).and_then(|v| v.as_s().ok()) {
                            fetched
                                .entry(pk_val.clone())
                                .or_default()
                                .push(item.clone());
                        }
                    }
                }

                // Retry unprocessed keys with exponential backoff.
                match response
                    .unprocessed_keys()
                    .and_then(|u| u.get(&self.table_name))
                {
                    Some(unprocessed) if !unprocessed.keys().is_empty() => {
                        if retry == max_retries {
                            return Err(NameServiceError::storage(
                                "BatchGetItem: max retries exhausted for UnprocessedKeys"
                                    .to_string(),
                            ));
                        }
                        let backoff_ms = 50 * (1 << retry.min(4)); // 50, 100, 200, 400, 800ms
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        pending = Some(unprocessed.clone());
                    }
                    _ => break,
                }
            }
        }

        // 4. Assemble GraphSourceRecords from GSI meta items + BatchGet config/index
        let mut records = Vec::with_capacity(meta_items.len());
        for meta in &meta_items {
            let Some(pk) = meta.get(ATTR_PK).and_then(|v| v.as_s().ok()) else {
                continue;
            };

            let extras = fetched.get(pk.as_str());
            let config_item = extras.and_then(|items| {
                items.iter().find(|i| {
                    i.get(ATTR_SK).and_then(|v| v.as_s().ok()) == Some(&SK_CONFIG.to_string())
                })
            });
            let index_item = extras.and_then(|items| {
                items.iter().find(|i| {
                    i.get(ATTR_SK).and_then(|v| v.as_s().ok()) == Some(&SK_INDEX.to_string())
                })
            });

            if let Some(record) = Self::gs_record_from_meta(pk, meta, config_item, index_item) {
                records.push(record);
            }
        }

        Ok(records)
    }
}

// ─── StatusLookup ──────────────────────────────────────────────────────────

#[async_trait]
impl StatusLookup for DynamoDbNameService {
    async fn get_status(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<StatusValue>, NameServiceError> {
        let pk = Self::normalize(ledger_id);

        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_STATUS.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {e}")))?;

        match response.item() {
            Some(item) => {
                let v: i64 = item
                    .get(ATTR_STATUS_V)
                    .and_then(|v| v.as_n().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);
                let state = item
                    .get(ATTR_STATUS)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_else(|| STATUS_READY.to_string());
                let extra = item
                    .get(ATTR_STATUS_META)
                    .and_then(|v| v.as_m().ok())
                    .map(Self::dynamo_map_to_json_map)
                    .unwrap_or_default();

                Ok(Some(StatusValue::new(
                    v,
                    StatusPayload::with_extra(state, extra),
                )))
            }
            None => {
                // Status item missing — check if meta exists (fallback)
                if self.meta_exists(&pk).await? {
                    Ok(Some(StatusValue::initial()))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

// ─── StatusPublisher ────────────────────────────────────────────────────────

#[async_trait]
impl StatusPublisher for DynamoDbNameService {
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> std::result::Result<StatusCasResult, NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let now = Self::now_epoch_ms().to_string();

        let Some(exp) = expected else {
            let current = self.get_status(ledger_id).await?;
            return Ok(StatusCasResult::Conflict { actual: current });
        };

        if new.v <= exp.v {
            let current = self.get_status(ledger_id).await?;
            return Ok(StatusCasResult::Conflict { actual: current });
        }

        // Build update expression
        let mut update_expr = "SET #st = :new_state, #sv = :new_v, #ua = :now".to_string();
        let mut request = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk))
            .key(ATTR_SK, AttributeValue::S(SK_STATUS.to_string()))
            .condition_expression("#sv = :expected_v AND #st = :expected_state")
            .expression_attribute_names("#st", ATTR_STATUS)
            .expression_attribute_names("#sv", ATTR_STATUS_V)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_values(":expected_v", AttributeValue::N(exp.v.to_string()))
            .expression_attribute_values(
                ":expected_state",
                AttributeValue::S(exp.payload.state.clone()),
            )
            .expression_attribute_values(":new_state", AttributeValue::S(new.payload.state.clone()))
            .expression_attribute_values(":new_v", AttributeValue::N(new.v.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now));

        if !new.payload.extra.is_empty() {
            update_expr.push_str(", #sm = :new_meta");
            request = request
                .expression_attribute_names("#sm", ATTR_STATUS_META)
                .expression_attribute_values(
                    ":new_meta",
                    Self::json_map_to_dynamo_map(&new.payload.extra),
                );
        } else {
            update_expr.push_str(" REMOVE #sm");
            request = request.expression_attribute_names("#sm", ATTR_STATUS_META);
        }

        let result = request.update_expression(update_expr).send().await;

        match result {
            Ok(_) => Ok(StatusCasResult::Updated),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                let current = self.get_status(ledger_id).await?;
                Ok(StatusCasResult::Conflict { actual: current })
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {e}"
            ))),
        }
    }
}

// ─── ConfigLookup (ledger configs only) ─────────────────────────────────────
//
// ConfigLookup/ConfigPublisher handle ledger configs (ConfigPayload with
// default_context + extra). Graph-source config lives under
// GraphSourcePublisher as raw config_json. Calling get_config/push_config on a
// graph-source ledger_id returns None / Conflict to prevent
// cross-contamination of the config_v watermark.

#[async_trait]
impl ConfigLookup for DynamoDbNameService {
    async fn get_config(
        &self,
        ledger_id: &str,
    ) -> std::result::Result<Option<ConfigValue>, NameServiceError> {
        let pk = Self::normalize(ledger_id);

        // Gate: only ledger configs — graph sources use GraphSourcePublisher.
        match self.meta_kind(&pk).await? {
            Some(ref k) if k == KIND_GRAPH_SOURCE => return Ok(None),
            None => return Ok(None),
            _ => {} // KIND_LEDGER — continue
        }

        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_CONFIG.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {e}")))?;

        match response.item() {
            Some(item) => {
                let default_context = item
                    .get(ATTR_DEFAULT_CONTEXT_ADDRESS)
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| fluree_db_nameservice::parse_default_context_value(s));
                let config_meta = item.get(ATTR_CONFIG_META).and_then(|v| v.as_m().ok());

                let v: i64 = item
                    .get(ATTR_CONFIG_V)
                    .and_then(|v| v.as_n().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let payload = if v == 0 && default_context.is_none() && config_meta.is_none() {
                    None
                } else {
                    let extra = config_meta
                        .map(Self::dynamo_map_to_json_map)
                        .unwrap_or_default();
                    Some(ConfigPayload {
                        default_context,
                        config_id: None,
                        extra,
                    })
                };

                Ok(Some(ConfigValue { v, payload }))
            }
            None => {
                // Config item missing but meta exists (already checked above)
                Ok(Some(ConfigValue::unborn()))
            }
        }
    }
}

// ─── ConfigPublisher ────────────────────────────────────────────────────────

#[async_trait]
impl ConfigPublisher for DynamoDbNameService {
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> std::result::Result<ConfigCasResult, NameServiceError> {
        let pk = Self::normalize(ledger_id);
        let now = Self::now_epoch_ms().to_string();

        // Gate: only ledger configs.
        match self.meta_kind(&pk).await? {
            Some(ref k) if k == KIND_GRAPH_SOURCE => {
                return Ok(ConfigCasResult::Conflict { actual: None })
            }
            None => return Ok(ConfigCasResult::Conflict { actual: None }),
            _ => {}
        }

        let Some(exp) = expected else {
            let current = self.get_config(ledger_id).await?;
            return Ok(ConfigCasResult::Conflict { actual: current });
        };

        if new.v <= exp.v {
            let current = self.get_config(ledger_id).await?;
            return Ok(ConfigCasResult::Conflict { actual: current });
        }

        // Condition: config_v must match expected
        let condition = if exp.v == 0 {
            "(attribute_not_exists(#cv) OR #cv = :zero)"
        } else {
            "#cv = :expected_v"
        };

        let mut update_parts = vec!["#cv = :new_v", "#ua = :now"];
        let mut request = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk))
            .key(ATTR_SK, AttributeValue::S(SK_CONFIG.to_string()))
            .expression_attribute_names("#cv", ATTR_CONFIG_V)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT_MS)
            .expression_attribute_names("#dc", ATTR_DEFAULT_CONTEXT_ADDRESS)
            .expression_attribute_values(":new_v", AttributeValue::N(new.v.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now));

        if exp.v == 0 {
            request =
                request.expression_attribute_values(":zero", AttributeValue::N("0".to_string()));
        } else {
            request = request
                .expression_attribute_values(":expected_v", AttributeValue::N(exp.v.to_string()));
        }

        let mut remove_parts: Vec<&str> = vec![];

        if let Some(ref payload) = new.payload {
            if let Some(ref ctx) = payload.default_context {
                update_parts.push("#dc = :new_dc");
                request = request
                    .expression_attribute_values(":new_dc", AttributeValue::S(ctx.to_string()));
            } else {
                remove_parts.push("#dc");
            }

            if !payload.extra.is_empty() {
                update_parts.push("#cm = :new_meta");
                request = request
                    .expression_attribute_names("#cm", ATTR_CONFIG_META)
                    .expression_attribute_values(
                        ":new_meta",
                        Self::json_map_to_dynamo_map(&payload.extra),
                    );
            } else {
                request = request.expression_attribute_names("#cm", ATTR_CONFIG_META);
                remove_parts.push("#cm");
            }
        } else {
            remove_parts.push("#dc");
            request = request.expression_attribute_names("#cm", ATTR_CONFIG_META);
            remove_parts.push("#cm");
        }

        let mut update_expr = format!("SET {}", update_parts.join(", "));
        if !remove_parts.is_empty() {
            update_expr.push_str(&format!(" REMOVE {}", remove_parts.join(", ")));
        }

        let result = request
            .condition_expression(condition)
            .update_expression(update_expr)
            .send()
            .await;

        match result {
            Ok(_) => Ok(ConfigCasResult::Updated),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                let current = self.get_config(ledger_id).await?;
                Ok(ConfigCasResult::Conflict { actual: current })
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {e}"
            ))),
        }
    }
}

// ─── Table provisioning ─────────────────────────────────────────────────────

impl DynamoDbNameService {
    /// Create the DynamoDB table with composite key + GSI1 if it does not exist.
    ///
    /// Waits for the table to become ACTIVE before returning.
    pub async fn ensure_table(&self) -> crate::error::Result<()> {
        let result = self
            .client
            .create_table()
            .table_name(&self.table_name)
            // Attribute definitions (only for key attributes)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(ATTR_PK)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("valid attr def"),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(ATTR_SK)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("valid attr def"),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(ATTR_KIND)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("valid attr def"),
            )
            // Table key schema
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(ATTR_PK)
                    .key_type(KeyType::Hash)
                    .build()
                    .expect("valid key schema"),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(ATTR_SK)
                    .key_type(KeyType::Range)
                    .build()
                    .expect("valid key schema"),
            )
            // GSI1: list by kind
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(GSI1_NAME)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name(ATTR_KIND)
                            .key_type(KeyType::Hash)
                            .build()
                            .expect("valid key schema"),
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name(ATTR_PK)
                            .key_type(KeyType::Range)
                            .build()
                            .expect("valid key schema"),
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::Include)
                            .non_key_attributes(ATTR_NAME)
                            .non_key_attributes(ATTR_BRANCH)
                            .non_key_attributes(ATTR_SOURCE_TYPE)
                            .non_key_attributes(ATTR_DEPENDENCIES)
                            .non_key_attributes(ATTR_RETRACTED)
                            .build(),
                    )
                    .build()
                    .expect("valid GSI"),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;

        match result {
            Ok(_) => {}
            Err(ref e) => {
                // Ignore ResourceInUseException (table already exists)
                let is_exists = matches!(
                    e,
                    aws_sdk_dynamodb::error::SdkError::ServiceError(se)
                    if matches!(
                        se.err(),
                        aws_sdk_dynamodb::operation::create_table::CreateTableError::ResourceInUseException(_)
                    )
                );
                if !is_exists {
                    return Err(crate::error::AwsStorageError::dynamodb(format!(
                        "CreateTable failed: {e}"
                    )));
                }
            }
        }

        // Wait for ACTIVE
        for _ in 0..60 {
            let desc = self
                .client
                .describe_table()
                .table_name(&self.table_name)
                .send()
                .await
                .map_err(|e| {
                    crate::error::AwsStorageError::dynamodb(format!("DescribeTable failed: {e}"))
                })?;

            if let Some(table) = desc.table() {
                if table.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active) {
                    return Ok(());
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(crate::error::AwsStorageError::dynamodb(
            "Table did not become ACTIVE within 30s",
        ))
    }
}

// ─── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    fn av_s(s: &str) -> AttributeValue {
        AttributeValue::S(s.to_string())
    }
    fn av_n(n: i64) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }
    fn av_bool(b: bool) -> AttributeValue {
        AttributeValue::Bool(b)
    }

    fn make_meta_ledger(pk: &str, name: &str, branch: &str) -> Item {
        HashMap::from([
            (ATTR_PK.to_string(), av_s(pk)),
            (ATTR_SK.to_string(), av_s(SK_META)),
            (ATTR_KIND.to_string(), av_s(KIND_LEDGER)),
            (ATTR_NAME.to_string(), av_s(name)),
            (ATTR_BRANCH.to_string(), av_s(branch)),
            (ATTR_RETRACTED.to_string(), av_bool(false)),
        ])
    }

    fn make_head(pk: &str, commit_id: Option<&str>, t: i64) -> Item {
        let mut item = HashMap::from([
            (ATTR_PK.to_string(), av_s(pk)),
            (ATTR_SK.to_string(), av_s(SK_HEAD)),
            (ATTR_COMMIT_T.to_string(), av_n(t)),
        ]);
        if let Some(id) = commit_id {
            item.insert(ATTR_COMMIT_ID.to_string(), av_s(id));
        }
        item
    }

    fn make_index(pk: &str, index_id: Option<&str>, t: i64) -> Item {
        let mut item = HashMap::from([
            (ATTR_PK.to_string(), av_s(pk)),
            (ATTR_SK.to_string(), av_s(SK_INDEX)),
            (ATTR_INDEX_T.to_string(), av_n(t)),
        ]);
        if let Some(id) = index_id {
            item.insert(ATTR_INDEX_ID.to_string(), av_s(id));
        }
        item
    }

    fn make_config_ledger(pk: &str, default_ctx: Option<&str>) -> Item {
        let mut item = HashMap::from([
            (ATTR_PK.to_string(), av_s(pk)),
            (ATTR_SK.to_string(), av_s(SK_CONFIG)),
            (ATTR_CONFIG_V.to_string(), av_n(0)),
        ]);
        if let Some(ctx) = default_ctx {
            item.insert(ATTR_DEFAULT_CONTEXT_ADDRESS.to_string(), av_s(ctx));
        }
        item
    }

    fn make_meta_gs(pk: &str, name: &str, branch: &str, source_type: &str) -> Item {
        HashMap::from([
            (ATTR_PK.to_string(), av_s(pk)),
            (ATTR_SK.to_string(), av_s(SK_META)),
            (ATTR_KIND.to_string(), av_s(KIND_GRAPH_SOURCE)),
            (ATTR_NAME.to_string(), av_s(name)),
            (ATTR_BRANCH.to_string(), av_s(branch)),
            (ATTR_SOURCE_TYPE.to_string(), av_s(source_type)),
            (ATTR_RETRACTED.to_string(), av_bool(false)),
            (
                ATTR_DEPENDENCIES.to_string(),
                AttributeValue::L(vec![av_s("source:main")]),
            ),
        ])
    }

    fn make_config_gs(pk: &str, config_json: &str) -> Item {
        HashMap::from([
            (ATTR_PK.to_string(), av_s(pk)),
            (ATTR_SK.to_string(), av_s(SK_CONFIG)),
            (ATTR_CONFIG_JSON.to_string(), av_s(config_json)),
            (ATTR_CONFIG_V.to_string(), av_n(1)),
        ])
    }

    // ── items_to_ns_record tests ────────────────────────────────────────

    #[test]
    fn test_items_to_ns_record_full() {
        let pk = "mydb:main";
        let commit_id = ContentId::new(ContentKind::Commit, b"commit-abc");
        let index_id = ContentId::new(ContentKind::IndexRoot, b"index-xyz");
        let ctx_id = ContentId::new(ContentKind::LedgerConfig, b"ctx-data");
        let items = vec![
            make_meta_ledger(pk, "mydb", "main"),
            make_head(pk, Some(&commit_id.to_string()), 10),
            make_index(pk, Some(&index_id.to_string()), 5),
            make_config_ledger(pk, Some(&ctx_id.to_string())),
        ];

        let record = DynamoDbNameService::items_to_ns_record(pk, &items).unwrap();
        assert_eq!(record.ledger_id, "mydb:main");
        assert_eq!(record.name, "mydb");
        assert_eq!(record.branch, "main");
        assert_eq!(record.commit_head_id, Some(commit_id));
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_head_id, Some(index_id));
        assert_eq!(record.index_t, 5);
        assert_eq!(record.default_context, Some(ctx_id));
        assert!(!record.retracted);
    }

    #[test]
    fn test_items_to_ns_record_unborn_head() {
        let pk = "mydb:main";
        let items = vec![
            make_meta_ledger(pk, "mydb", "main"),
            make_head(pk, None, 0),
            make_index(pk, None, 0),
            make_config_ledger(pk, None),
        ];

        let record = DynamoDbNameService::items_to_ns_record(pk, &items).unwrap();
        assert_eq!(record.commit_head_id, None);
        assert_eq!(record.commit_t, 0);
        assert_eq!(record.index_head_id, None);
        assert_eq!(record.index_t, 0);
        assert_eq!(record.default_context, None);
    }

    #[test]
    fn test_items_to_ns_record_wrong_kind() {
        let pk = "search:main";
        let items = vec![make_meta_gs(pk, "search", "main", "f:Bm25Index")];

        assert!(DynamoDbNameService::items_to_ns_record(pk, &items).is_none());
    }

    #[test]
    fn test_items_to_ns_record_no_meta() {
        let pk = "mydb:main";
        let items = vec![make_head(pk, Some("commit-1"), 5)];

        assert!(DynamoDbNameService::items_to_ns_record(pk, &items).is_none());
    }

    // ── items_to_gs_record tests ────────────────────────────────────────

    #[test]
    fn test_items_to_gs_record_full() {
        let pk = "search:main";
        let index_id = ContentId::new(ContentKind::IndexRoot, b"snap-001");
        let items = vec![
            make_meta_gs(pk, "search", "main", "f:Bm25Index"),
            make_config_gs(pk, r#"{"k1":1.2}"#),
            make_index(pk, Some(&index_id.to_string()), 42),
        ];

        let record = DynamoDbNameService::items_to_gs_record(pk, &items).unwrap();
        assert_eq!(record.graph_source_id, "search:main");
        assert_eq!(record.name, "search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.source_type, GraphSourceType::Bm25);
        assert_eq!(record.config, r#"{"k1":1.2}"#);
        assert_eq!(record.dependencies, vec!["source:main".to_string()]);
        assert_eq!(record.index_id, Some(index_id));
        assert_eq!(record.index_t, 42);
        assert!(!record.retracted);
    }

    #[test]
    fn test_items_to_gs_record_unborn_index() {
        let pk = "search:main";
        let items = vec![
            make_meta_gs(pk, "search", "main", "f:Bm25Index"),
            make_config_gs(pk, "{}"),
            make_index(pk, None, 0),
        ];

        let record = DynamoDbNameService::items_to_gs_record(pk, &items).unwrap();
        assert_eq!(record.index_id, None);
        assert_eq!(record.index_t, 0);
    }

    // ── now_epoch_ms test ───────────────────────────────────────────────

    #[test]
    fn test_now_epoch_ms() {
        let now = DynamoDbNameService::now_epoch_ms();
        // Must be after 2024-01-01 in milliseconds
        assert!(now > 1_704_067_200_000);
    }

    // ── normalize test ──────────────────────────────────────────────────

    #[test]
    fn test_normalize() {
        assert_eq!(DynamoDbNameService::normalize("mydb"), "mydb:main");
        assert_eq!(DynamoDbNameService::normalize("mydb:dev"), "mydb:dev");
        assert_eq!(DynamoDbNameService::normalize("mydb:main"), "mydb:main");
    }
}
