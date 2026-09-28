//! Name bindings, the dropped-ledger registry, and fenced branch-record
//! writes for the DynamoDB nameservice.
//!
//! A binding or registry entry is one item holding a version counter and
//! the value as JSON. Deleting it removes the value and keeps the item, so
//! versions never repeat for a key. Branch records carry their fence on
//! every item, and each write to an item is conditional on it.

use super::schema::*;
use super::{DynamoDbNameService, Item};
use async_trait::async_trait;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use fluree_db_core::InstanceId;
use fluree_db_nameservice::{
    BranchRecordStore, DroppedLedger, Fence, FenceOutcome, LedgerRegistry, NameBinding,
    NameServiceError, NsRecord, RegistryCas, Versioned,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;

type Result<T> = std::result::Result<T, NameServiceError>;

fn storage_err(what: &str, e: impl std::fmt::Display) -> NameServiceError {
    NameServiceError::storage(format!("DynamoDB {what} failed: {e}"))
}

fn fence_value(fence: Fence) -> AttributeValue {
    AttributeValue::S(fence.to_string())
}

/// Whether a write failed its condition expression.
fn condition_failed<E: aws_sdk_dynamodb::error::ProvideErrorMetadata>(
    err: &aws_sdk_dynamodb::error::SdkError<E>,
) -> bool {
    matches!(err, aws_sdk_dynamodb::error::SdkError::ServiceError(se)
        if se.err().code() == Some("ConditionalCheckFailedException"))
}

/// The clause a write presenting `fence` adds to its item's condition: the
/// item carries that fence and is not frozen. A write presenting none is
/// refused, as [`fluree_db_nameservice::fence_admits`] refuses it: its clause
/// can never hold.
struct FenceClause {
    expr: &'static str,
    names: &'static [(&'static str, &'static str)],
    values: Vec<(&'static str, AttributeValue)>,
}

impl FenceClause {
    fn new(fence: Option<Fence>) -> Self {
        match fence {
            Some(fence) => Self {
                expr: "#fence = :fence AND (attribute_not_exists(#frozen) OR #frozen <> :frozen)",
                names: &[("#fence", ATTR_FENCE), ("#frozen", ATTR_FROZEN)],
                values: vec![
                    (":fence", fence_value(fence)),
                    (":frozen", AttributeValue::Bool(true)),
                ],
            },
            None => Self {
                expr: "attribute_exists(#fence) AND attribute_not_exists(#fence)",
                names: &[("#fence", ATTR_FENCE)],
                values: Vec::new(),
            },
        }
    }

    /// `condition`, if any, joined with this clause.
    fn condition(&self, condition: Option<&str>) -> String {
        match condition {
            Some(condition) => format!("({condition}) AND {}", self.expr),
            None => self.expr.to_string(),
        }
    }
}

/// Condition an `UpdateItem` on `condition` and on its item admitting
/// `fence`.
pub(super) fn fenced_update_item(
    mut request: aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder,
    condition: Option<&str>,
    fence: Option<Fence>,
) -> aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder {
    let clause = FenceClause::new(fence);
    request = request.condition_expression(clause.condition(condition));
    for (name, attr) in clause.names {
        request = request.expression_attribute_names(*name, *attr);
    }
    for (name, value) in clause.values {
        request = request.expression_attribute_values(name, value);
    }
    request
}

/// [`fenced_update_item`] for an update inside a transaction.
pub(super) fn fenced_update(
    mut update: aws_sdk_dynamodb::types::builders::UpdateBuilder,
    condition: Option<&str>,
    fence: Option<Fence>,
) -> aws_sdk_dynamodb::types::builders::UpdateBuilder {
    let clause = FenceClause::new(fence);
    update = update.condition_expression(clause.condition(condition));
    for (name, attr) in clause.names {
        update = update.expression_attribute_names(*name, *attr);
    }
    for (name, value) in clause.values {
        update = update.expression_attribute_values(name, value);
    }
    update
}

impl DynamoDbNameService {
    /// Whether the item at `sk` refuses a write presenting `fence`: it does
    /// not admit it, or it is missing, since publication never creates a
    /// record. Read after a conditional write failed, to tell a refused fence
    /// from a lost race.
    pub(super) async fn fence_refuses(
        &self,
        pk: &str,
        sk: &str,
        fence: Option<Fence>,
    ) -> Result<bool> {
        let Some(item) = self.get_item(pk, sk).await? else {
            return Ok(true);
        };
        let stored = item
            .get(ATTR_FENCE)
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse().ok());
        let frozen = item.get(ATTR_FROZEN).and_then(|v| v.as_bool().ok()) == Some(&true);
        Ok(!fluree_db_nameservice::fence_admits(stored, frozen, fence))
    }

    async fn get_item(&self, pk: &str, sk: &str) -> Result<Option<Item>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(sk.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| storage_err("GetItem", e))?;
        Ok(response.item().cloned())
    }

    /// The live value of a versioned item: its version and the JSON in
    /// `attr`, or `None` for no item or a tombstone.
    fn live_value<T: DeserializeOwned>(item: &Item, attr: &str) -> Result<Option<Versioned<T>>> {
        let Some(json) = item.get(attr).and_then(|v| v.as_s().ok()) else {
            return Ok(None);
        };
        let version = item
            .get(ATTR_V)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        Ok(Some(Versioned {
            value: serde_json::from_str(json)?,
            version,
        }))
    }

    async fn read_versioned<T: DeserializeOwned>(
        &self,
        pk: &str,
        sk: &str,
        attr: &str,
    ) -> Result<Option<Versioned<T>>> {
        match self.get_item(pk, sk).await? {
            Some(item) => Self::live_value(&item, attr),
            None => Ok(None),
        }
    }

    /// Compare-and-swap a versioned item; see [`LedgerRegistry`].
    async fn cas_versioned<T: Serialize + DeserializeOwned>(
        &self,
        pk: &str,
        sk: &str,
        kind: &str,
        attr: &str,
        expected: Option<u64>,
        new: Option<&T>,
    ) -> Result<RegistryCas<T>> {
        let current = self.get_item(pk, sk).await?;
        let actual = match &current {
            Some(item) => Self::live_value::<T>(item, attr)?,
            None => None,
        };
        if actual.as_ref().map(|v| v.version) != expected {
            return Ok(RegistryCas::Conflict { actual });
        }
        let current_v = current.as_ref().and_then(|item| {
            item.get(ATTR_V)
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
        });
        let version = current_v.map_or(1, |v| v + 1);

        let mut item: Item = HashMap::from([
            (ATTR_PK.to_string(), AttributeValue::S(pk.to_string())),
            (ATTR_SK.to_string(), AttributeValue::S(sk.to_string())),
            (ATTR_KIND.to_string(), AttributeValue::S(kind.to_string())),
            (ATTR_V.to_string(), AttributeValue::N(version.to_string())),
            (
                ATTR_UPDATED_AT_MS.to_string(),
                AttributeValue::N(Self::now_epoch_ms().to_string()),
            ),
            (
                ATTR_SCHEMA.to_string(),
                AttributeValue::N(SCHEMA_VERSION.to_string()),
            ),
        ]);
        if let Some(value) = new {
            item.insert(
                attr.to_string(),
                AttributeValue::S(serde_json::to_string(value)?),
            );
        }

        let mut put = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item));
        put = match current_v {
            Some(v) => put
                .condition_expression("#v = :v")
                .expression_attribute_names("#v", ATTR_V)
                .expression_attribute_values(":v", AttributeValue::N(v.to_string())),
            None => put
                .condition_expression("attribute_not_exists(#pk)")
                .expression_attribute_names("#pk", ATTR_PK),
        };
        match put.send().await {
            Ok(_) => Ok(RegistryCas::Updated {
                version: new.map(|_| version),
            }),
            Err(e) if condition_failed(&e) => Ok(RegistryCas::Conflict {
                actual: self.read_versioned(pk, sk, attr).await?,
            }),
            Err(e) => Err(storage_err("PutItem", e)),
        }
    }

    /// The items of the record at `pk`, keyed by sort key, excluding the
    /// commit index.
    async fn record_items(&self, pk: &str) -> Result<Vec<Item>> {
        Ok(self
            .query_all_items_paginated(pk)
            .await?
            .into_iter()
            .filter(|item| {
                item.get(ATTR_SK)
                    .and_then(|v| v.as_s().ok())
                    .is_some_and(|sk| !sk.starts_with(SK_COMMIT_PREFIX))
            })
            .collect())
    }

    fn sk_of(item: &Item) -> Option<&str> {
        item.get(ATTR_SK)
            .and_then(|v| v.as_s().ok())
            .map(String::as_str)
    }

    /// Update one item of a record if it carries `fence`.
    async fn update_item_fenced(
        &self,
        pk: &str,
        sk: &str,
        fence: Fence,
        update: &str,
        values: &[(&str, AttributeValue)],
    ) -> Result<bool> {
        self.update_item_conditioned(pk, sk, fence, None, update, values)
            .await
    }

    /// [`update_item_fenced`](Self::update_item_fenced), also requiring
    /// `also` to hold.
    async fn update_item_conditioned(
        &self,
        pk: &str,
        sk: &str,
        fence: Fence,
        also: Option<&str>,
        update: &str,
        values: &[(&str, AttributeValue)],
    ) -> Result<bool> {
        let condition = match also {
            Some(also) => format!("#fence = :fence AND ({also})"),
            None => "#fence = :fence".to_string(),
        };
        let mut request = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(sk.to_string()))
            .update_expression(update)
            .condition_expression(&condition)
            .expression_attribute_names("#fence", ATTR_FENCE)
            .expression_attribute_values(":fence", fence_value(fence));
        for (name, value) in values {
            request = request.expression_attribute_values(*name, value.clone());
        }
        if update.contains("#frozen") || condition.contains("#frozen") {
            request = request.expression_attribute_names("#frozen", ATTR_FROZEN);
        }
        if update.contains("#branches") {
            request = request.expression_attribute_names("#branches", ATTR_BRANCHES);
        }
        match request.send().await {
            Ok(_) => Ok(true),
            Err(e) if condition_failed(&e) => Ok(false),
            Err(e) => Err(storage_err("UpdateItem", e)),
        }
    }

    /// Every item of a new record: its meta, heads, status and config, each
    /// carrying the fence.
    fn record_items_for(&self, pk: &str, record: &NsRecord) -> Vec<Item> {
        let now = Self::now_epoch_ms().to_string();
        let base = |sk: &str| -> Item {
            let mut item = HashMap::from([
                (ATTR_PK.to_string(), AttributeValue::S(pk.to_string())),
                (ATTR_SK.to_string(), AttributeValue::S(sk.to_string())),
                (
                    ATTR_UPDATED_AT_MS.to_string(),
                    AttributeValue::N(now.clone()),
                ),
                (
                    ATTR_SCHEMA.to_string(),
                    AttributeValue::N(SCHEMA_VERSION.to_string()),
                ),
            ]);
            if let Some(fence) = record.fence {
                item.insert(ATTR_FENCE.to_string(), fence_value(fence));
            }
            if record.frozen {
                item.insert(ATTR_FROZEN.to_string(), AttributeValue::Bool(true));
            }
            item
        };

        let mut meta = base(SK_META);
        meta.insert(
            ATTR_KIND.to_string(),
            AttributeValue::S(KIND_LEDGER.to_string()),
        );
        meta.insert(
            ATTR_NAME.to_string(),
            AttributeValue::S(record.name.clone()),
        );
        meta.insert(
            ATTR_BRANCH.to_string(),
            AttributeValue::S(record.branch.clone()),
        );
        meta.insert(
            ATTR_RETRACTED.to_string(),
            AttributeValue::Bool(record.retracted),
        );
        if let Some(source) = &record.source_branch {
            meta.insert(
                ATTR_BP_SOURCE.to_string(),
                AttributeValue::S(source.clone()),
            );
        }
        if record.branches > 0 {
            meta.insert(
                ATTR_BRANCHES.to_string(),
                AttributeValue::N(record.branches.to_string()),
            );
        }

        let mut head = base(SK_HEAD);
        head.insert(
            ATTR_COMMIT_T.to_string(),
            AttributeValue::N(record.commit_t.to_string()),
        );
        if let Some(id) = &record.commit_head_id {
            head.insert(
                ATTR_COMMIT_ID.to_string(),
                AttributeValue::S(id.to_string()),
            );
        }

        let mut index = base(SK_INDEX);
        index.insert(
            ATTR_INDEX_T.to_string(),
            AttributeValue::N(record.index_t.to_string()),
        );
        if let Some(id) = &record.index_head_id {
            index.insert(ATTR_INDEX_ID.to_string(), AttributeValue::S(id.to_string()));
        }

        let mut status = base(SK_STATUS);
        status.insert(
            ATTR_STATUS.to_string(),
            AttributeValue::S(STATUS_READY.to_string()),
        );
        status.insert(
            ATTR_STATUS_V.to_string(),
            AttributeValue::N("1".to_string()),
        );

        let mut config = base(SK_CONFIG);
        let has_context = record.default_context.is_some();
        config.insert(
            ATTR_CONFIG_V.to_string(),
            AttributeValue::N(if has_context { "1" } else { "0" }.to_string()),
        );
        if let Some(ctx) = &record.default_context {
            config.insert(
                ATTR_DEFAULT_CONTEXT_ADDRESS.to_string(),
                AttributeValue::S(ctx.to_string()),
            );
        }

        vec![meta, head, index, status, config]
    }

    /// Whether the record's meta item exists, and with which fence.
    async fn meta_fence(&self, pk: &str) -> Result<Option<Option<Fence>>> {
        Ok(self.get_item(pk, SK_META).await?.map(|meta| {
            meta.get(ATTR_FENCE)
                .and_then(|v| v.as_s().ok())
                .and_then(|s| s.parse().ok())
        }))
    }

    /// Bind the ledgers a binary from before name bindings left, once (see
    /// [`fluree_db_nameservice::lifecycle::migrate_legacy`]), and record the
    /// format so later starts skip it. Returns what it bound, or `None` when
    /// the table was already current. Every node must run this version
    /// first: an older binary writes without checking fences.
    pub async fn migrate(
        &self,
    ) -> Result<Option<fluree_db_nameservice::lifecycle::MigrationReport>> {
        use fluree_db_nameservice::migration::{check_format_version, FORMAT_VERSION};

        if let Some(item) = self.get_item(PK_FORMAT, SK_META).await? {
            let version = item
                .get(ATTR_SCHEMA)
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            check_format_version(version)?;
            if version == FORMAT_VERSION {
                return Ok(None);
            }
        }

        let report = fluree_db_nameservice::lifecycle::migrate_legacy(self).await?;
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item(ATTR_PK, AttributeValue::S(PK_FORMAT.to_string()))
            .item(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .item(ATTR_SCHEMA, AttributeValue::N(FORMAT_VERSION.to_string()))
            .item(
                ATTR_UPDATED_AT_MS,
                AttributeValue::N(Self::now_epoch_ms().to_string()),
            )
            .send()
            .await
            .map_err(|e| storage_err("PutItem", e))?;
        if !report.is_empty() {
            tracing::info!(
                bound = report.bound.len(),
                dropped = report.dropped.len(),
                "nameservice migrated to format {FORMAT_VERSION}"
            );
        }
        Ok(Some(report))
    }

    /// Write `item` unless its key is taken.
    async fn put_item_if_absent(&self, item: Item) -> Result<()> {
        match self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if condition_failed(&e) => Ok(()),
            Err(e) => Err(storage_err("PutItem", e)),
        }
    }

    /// Give the item at `sk` `fence`, if it carries none or already carries
    /// it. Returns whether it carries `fence` afterwards.
    async fn adopt_item(&self, pk: &str, sk: &str, fence: Fence) -> Result<bool> {
        match self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.to_string()))
            .key(ATTR_SK, AttributeValue::S(sk.to_string()))
            .update_expression("SET #fence = :fence")
            .condition_expression(
                "attribute_exists(pk) AND (attribute_not_exists(#fence) OR #fence = :fence)",
            )
            .expression_attribute_names("#fence", ATTR_FENCE)
            .expression_attribute_values(":fence", fence_value(fence))
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if condition_failed(&e) => Ok(false),
            Err(e) => Err(storage_err("UpdateItem", e)),
        }
    }
}

#[async_trait]
impl LedgerRegistry for DynamoDbNameService {
    async fn get_binding(&self, name: &str) -> Result<Option<Versioned<NameBinding>>> {
        self.read_versioned(name, SK_BINDING, ATTR_BINDING).await
    }

    async fn cas_binding(
        &self,
        name: &str,
        expected: Option<u64>,
        new: Option<&NameBinding>,
    ) -> Result<RegistryCas<NameBinding>> {
        self.cas_versioned(name, SK_BINDING, KIND_BINDING, ATTR_BINDING, expected, new)
            .await
    }

    /// Listed through the kind index, which is eventually consistent; each
    /// binding it names is then read consistently.
    async fn list_bindings(&self) -> Result<Vec<(String, Versioned<NameBinding>)>> {
        let mut found = Vec::new();
        for item in self.query_gsi_by_kind(KIND_BINDING).await? {
            let Some(name) = item.get(ATTR_PK).and_then(|v| v.as_s().ok()) else {
                continue;
            };
            if let Some(binding) = self.get_binding(name).await? {
                found.push((name.clone(), binding));
            }
        }
        Ok(found)
    }

    async fn get_dropped(&self, instance: &InstanceId) -> Result<Option<Versioned<DroppedLedger>>> {
        self.read_versioned(&format!("@{instance}"), SK_DROPPED, ATTR_ENTRY)
            .await
    }

    async fn cas_dropped(
        &self,
        instance: &InstanceId,
        expected: Option<u64>,
        new: Option<&DroppedLedger>,
    ) -> Result<RegistryCas<DroppedLedger>> {
        self.cas_versioned(
            &format!("@{instance}"),
            SK_DROPPED,
            KIND_DROPPED,
            ATTR_ENTRY,
            expected,
            new,
        )
        .await
    }

    async fn list_dropped(&self) -> Result<Vec<Versioned<DroppedLedger>>> {
        let mut found = Vec::new();
        for item in self.query_gsi_by_kind(KIND_DROPPED).await? {
            let Some(instance) = item
                .get(ATTR_PK)
                .and_then(|v| v.as_s().ok())
                .and_then(|pk| pk.strip_prefix('@'))
                .and_then(|id| InstanceId::parse(id).ok())
            else {
                continue;
            };
            if let Some(entry) = self.get_dropped(&instance).await? {
                found.push(entry);
            }
        }
        Ok(found)
    }
}

#[async_trait]
impl BranchRecordStore for DynamoDbNameService {
    async fn all_raw_records(&self) -> Result<Vec<NsRecord>> {
        self.list_raw_records().await
    }

    async fn raw_record(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let pk = Self::normalize(ledger_id)?;
        let items = self.query_metadata_items(&pk).await?;
        Ok(Self::items_to_ns_record(&pk, &items))
    }

    /// Inserts every item of the record in one transaction, each conditional
    /// on its key being absent. Items a partly deleted record left without
    /// its meta item are garbage and are cleared first.
    async fn insert_record(&self, record: &NsRecord) -> Result<Option<NsRecord>> {
        let pk = Self::normalize(&record.ledger_id)?;
        for _ in 0..3 {
            if let Some(existing) = self.raw_record(&pk).await? {
                return Ok(Some(existing));
            }
            for item in self.record_items(&pk).await? {
                if let Some(sk) = Self::sk_of(&item) {
                    self.client
                        .delete_item()
                        .table_name(&self.table_name)
                        .key(ATTR_PK, AttributeValue::S(pk.clone()))
                        .key(ATTR_SK, AttributeValue::S(sk.to_string()))
                        .send()
                        .await
                        .map_err(|e| storage_err("DeleteItem", e))?;
                }
            }

            let transaction = self.record_items_for(&pk, record).into_iter().fold(
                self.client.transact_write_items(),
                |tx, item| {
                    tx.transact_items(
                        TransactWriteItem::builder()
                            .put(
                                Put::builder()
                                    .table_name(&self.table_name)
                                    .set_item(Some(item))
                                    .condition_expression("attribute_not_exists(pk)")
                                    .build()
                                    .expect("valid Put"),
                            )
                            .build(),
                    )
                },
            );
            match transaction.send().await {
                Ok(_) => return Ok(None),
                Err(e) if Self::is_transaction_canceled(&e) => continue,
                Err(e) => return Err(storage_err("TransactWriteItems", e)),
            }
        }
        Err(NameServiceError::storage(format!(
            "could not insert the record for {pk}; retry"
        )))
    }

    /// Fences every item but the commit index, the meta item last, so a
    /// crash part way leaves the record unfenced for the next attempt.
    ///
    /// An item the record never had (an index never published) is written
    /// fenced, since a fenced publish never creates one.
    async fn adopt_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let pk = Self::normalize(ledger_id)?;
        match self.meta_fence(&pk).await? {
            None => return Ok(FenceOutcome::Missing),
            Some(Some(f)) if f == fence => return Ok(FenceOutcome::Applied),
            Some(Some(_)) => return Ok(FenceOutcome::Mismatch),
            Some(None) => {}
        }
        let items = self.record_items(&pk).await?;
        if let Some(mut record) = Self::items_to_ns_record(&pk, &items) {
            record.fence = Some(fence);
            for item in self.record_items_for(&pk, &record) {
                if Self::sk_of(&item)
                    .is_some_and(|sk| sk != SK_META && Self::find_item_by_sk(&items, sk).is_none())
                {
                    self.put_item_if_absent(item).await?;
                }
            }
        }
        for item in &items {
            match Self::sk_of(item) {
                Some(sk) if sk != SK_META => {
                    self.adopt_item(&pk, sk, fence).await?;
                }
                _ => {}
            }
        }
        if self.adopt_item(&pk, SK_META, fence).await? {
            return Ok(FenceOutcome::Applied);
        }
        Ok(match self.meta_fence(&pk).await? {
            None => FenceOutcome::Missing,
            Some(_) => FenceOutcome::Mismatch,
        })
    }

    async fn freeze_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let pk = Self::normalize(ledger_id)?;
        match self.meta_fence(&pk).await? {
            None => return Ok(FenceOutcome::Missing),
            Some(f) if f != Some(fence) => return Ok(FenceOutcome::Mismatch),
            Some(_) => {}
        }
        let frozen = [(":frozen", AttributeValue::Bool(true))];
        if !self
            .update_item_fenced(&pk, SK_META, fence, "SET #frozen = :frozen", &frozen)
            .await?
        {
            return Ok(FenceOutcome::Mismatch);
        }
        for item in self.record_items(&pk).await? {
            match Self::sk_of(&item) {
                Some(sk) if sk != SK_META => {
                    self.update_item_fenced(&pk, sk, fence, "SET #frozen = :frozen", &frozen)
                        .await?;
                }
                _ => {}
            }
        }
        Ok(FenceOutcome::Applied)
    }

    /// Deletes the record's items conditionally on the fence, meta last, so a
    /// crash part way leaves a record the drop can find and finish. The
    /// commit index carries no fence and goes once the record's fence is
    /// confirmed.
    async fn delete_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let pk = Self::normalize(ledger_id)?;
        match self.meta_fence(&pk).await? {
            None => return Ok(FenceOutcome::Missing),
            Some(f) if f != Some(fence) => return Ok(FenceOutcome::Mismatch),
            Some(_) => {}
        }
        for item in self.query_all_items_paginated(&pk).await? {
            let Some(sk) = Self::sk_of(&item) else {
                continue;
            };
            if sk == SK_META {
                continue;
            }
            let mut delete = self
                .client
                .delete_item()
                .table_name(&self.table_name)
                .key(ATTR_PK, AttributeValue::S(pk.clone()))
                .key(ATTR_SK, AttributeValue::S(sk.to_string()));
            if !sk.starts_with(SK_COMMIT_PREFIX) {
                delete = delete
                    .condition_expression("#fence = :fence")
                    .expression_attribute_names("#fence", ATTR_FENCE)
                    .expression_attribute_values(":fence", fence_value(fence));
            }
            match delete.send().await {
                Ok(_) => {}
                Err(e) if condition_failed(&e) => {}
                Err(e) => return Err(storage_err("DeleteItem", e)),
            }
        }
        match self
            .client
            .delete_item()
            .table_name(&self.table_name)
            .key(ATTR_PK, AttributeValue::S(pk.clone()))
            .key(ATTR_SK, AttributeValue::S(SK_META.to_string()))
            .condition_expression("#fence = :fence")
            .expression_attribute_names("#fence", ATTR_FENCE)
            .expression_attribute_values(":fence", fence_value(fence))
            .send()
            .await
        {
            Ok(_) => Ok(FenceOutcome::Applied),
            Err(e) if condition_failed(&e) => Ok(FenceOutcome::Mismatch),
            Err(e) => Err(storage_err("DeleteItem", e)),
        }
    }
}
