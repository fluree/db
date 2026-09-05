//! Column profiling over the stats kernel.
//!
//! Two faces, one output. [`Fluree::profile_ledger`] walks a property's
//! values at a pinned `t` through the same index range the SHACL
//! validator uses, novelty included; [`Fluree::profile_table`] streams a
//! lake table through the scan the virtual graph reads. Both fill
//! [`fluree_db_stats::ColumnProfile`]s, optionally grouped by another
//! property or column, and report [`ProfileReport`]s that serialise the
//! same way whichever side they came from.
//!
//! Profiles are computed on demand and never touch the index statistics
//! that ride on every reindex. The sketches themselves are a few
//! kilobytes, but the ledger face has no streaming predicate walk to
//! drive them from yet: each property's current flakes are ranged into
//! memory once and folded from there, so peak memory is proportional to
//! the largest property profiled. The table face streams batches and is
//! bounded by the scan.
//!
//! Neither face applies view policy. Both read the ledger or lake
//! directly and belong behind an administrative surface until they do.

use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_core::{
    range_with_overlay, FlakeValue, IndexType, LedgerSnapshot, RangeMatch, RangeOptions, RangeTest,
    Sid,
};
use fluree_db_stats::{
    ColumnProfile, ColumnSummary, GroupedProfile, GroupedSummary, ProfileConfig, ProfileValue,
};
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, Result};
use crate::ledger_info::GraphSelector;

const MILLIS_PER_DAY: i64 = 86_400_000;

/// What to profile.
#[derive(Debug, Clone)]
pub struct ProfileRequest {
    /// Property IRIs (ledger) or column names (table). Empty on a table
    /// means every column; a ledger request must name its properties.
    pub columns: Vec<String>,
    /// Ledger only: which named graph. Ignored for tables.
    pub graph: GraphSelector,
    /// Property IRIs / column names whose values form the group key.
    /// Empty means no grouping.
    pub group_by: Vec<String>,
    pub config: ProfileConfig,
    /// Most groups kept per column before pooling into overflow.
    pub max_groups: usize,
}

impl ProfileRequest {
    pub fn columns(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
            graph: GraphSelector::Default,
            group_by: Vec::new(),
            config: ProfileConfig::default(),
            max_groups: fluree_db_stats::grouped::DEFAULT_MAX_GROUPS,
        }
    }

    pub fn group_by(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.group_by = keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn graph(mut self, graph: GraphSelector) -> Self {
        self.graph = graph;
        self
    }

    pub fn config(mut self, config: ProfileConfig) -> Self {
        self.config = config;
        self
    }

    pub fn max_groups(mut self, n: usize) -> Self {
        self.max_groups = n;
        self
    }
}

/// One profiled column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfiledColumn {
    pub name: String,
    pub summary: ColumnSummary,
    /// Present when the request grouped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouped: Option<GroupedSummary>,
}

/// A requested column that could not be profiled, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkippedColumn {
    pub name: String,
    pub reason: String,
}

/// The result of a profiling run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileReport {
    /// Ledger id, or `graph_source/table`.
    pub source: String,
    /// The ledger `t` the profile was pinned to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t: Option<i64>,
    /// The Iceberg snapshot the profile was pinned to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    pub group_by: Vec<String>,
    pub columns: Vec<ProfiledColumn>,
    pub skipped: Vec<SkippedColumn>,
}

/// One column's accumulator: flat or grouped, decided by the request.
enum Accumulator {
    Flat(Box<ColumnProfile>),
    Grouped(Box<GroupedProfile>),
}

impl Accumulator {
    fn new(req: &ProfileRequest) -> Self {
        if req.group_by.is_empty() {
            Self::Flat(Box::new(ColumnProfile::new(req.config)))
        } else {
            Self::Grouped(Box::new(GroupedProfile::new(req.config, req.max_groups)))
        }
    }

    fn observe(&mut self, key: Option<&str>, value: ProfileValue<'_>) {
        match (self, key) {
            (Self::Flat(p), _) => p.observe(value),
            (Self::Grouped(g), Some(k)) => g.observe(k, value),
            (Self::Grouped(g), None) => g.observe_ungrouped(value),
        }
    }

    fn finish(self, name: String) -> ProfiledColumn {
        match self {
            Self::Flat(p) => ProfiledColumn {
                name,
                summary: p.summary(),
                grouped: None,
            },
            Self::Grouped(g) => ProfiledColumn {
                name,
                summary: g.total().summary(),
                grouped: Some(g.summary()),
            },
        }
    }
}

/// A flake's object as the profiler sees it. `scratch` receives any text
/// the value has to be rendered into (a decoded IRI, a big number), so
/// the returned value can borrow it.
fn profile_value<'a>(
    snapshot: &LedgerSnapshot,
    value: &'a FlakeValue,
    scratch: &'a mut String,
) -> ProfileValue<'a> {
    scratch.clear();
    match value {
        FlakeValue::Null => ProfileValue::Null,
        FlakeValue::Boolean(b) => ProfileValue::Bool(*b),
        FlakeValue::Long(i) => ProfileValue::Int(*i),
        FlakeValue::Double(f) => ProfileValue::Float(*f),
        FlakeValue::String(s) => ProfileValue::Str(s),
        FlakeValue::Ref(sid) => {
            match snapshot.decode_sid(sid) {
                Some(iri) => scratch.push_str(&iri),
                None => scratch.push_str(&format!("{sid:?}")),
            }
            ProfileValue::Ref(scratch)
        }
        FlakeValue::DateTime(dt) => ProfileValue::Temporal(dt.epoch_millis()),
        FlakeValue::Date(d) => {
            ProfileValue::Temporal(i64::from(d.days_since_epoch()) * MILLIS_PER_DAY)
        }
        FlakeValue::BigInt(_) | FlakeValue::Decimal(_) => {
            // Exact numbers read as floats for the moments and quantiles.
            // The float hash folds an integral value onto the integer's
            // hash, so `7.00` and `7` still count once (see
            // `fluree_db_stats::hash`).
            scratch.push_str(&value.to_string());
            match scratch.parse::<f64>() {
                Ok(f) => ProfileValue::Float(f),
                Err(_) => ProfileValue::Other(scratch),
            }
        }
        other => {
            scratch.push_str(&other.to_string());
            ProfileValue::Other(scratch)
        }
    }
}

/// Every current assertion of `p` in graph `g_id` at the view's `t`.
async fn property_flakes(
    view: &crate::ledger_view::LedgerView,
    g_id: fluree_db_core::GraphId,
    p: Sid,
) -> Result<Vec<fluree_db_core::Flake>> {
    let opts = RangeOptions::default().with_to_t(view.t);
    let flakes = range_with_overlay(
        &view.snapshot,
        g_id,
        view.novelty.as_ref(),
        IndexType::Psot,
        RangeTest::Eq,
        RangeMatch::predicate(p),
        opts,
    )
    .await?;
    Ok(flakes.into_iter().filter(|f| f.op).collect())
}

impl crate::Fluree {
    /// Profile properties of a ledger at its current `t`.
    ///
    /// Each named property is ranged once over the predicate-ordered
    /// index with novelty folded in, held in memory for the fold, and
    /// dropped before the next. With `group_by`, the grouping properties
    /// are ranged first to map each subject to its key; a subject with
    /// several values for one grouping property takes the first seen,
    /// and a subject with none is counted as ungrouped. Keys are interned
    /// so the map costs one pointer per subject.
    ///
    /// No view policy is applied.
    pub async fn profile_ledger(
        &self,
        ledger_id: &str,
        req: &ProfileRequest,
    ) -> Result<ProfileReport> {
        if req.columns.is_empty() {
            return Err(ApiError::config(
                "profile_ledger needs at least one property IRI",
            ));
        }
        let handle = self.ledger_cached(ledger_id).await?;
        let view = handle.snapshot().await;
        let g_id =
            crate::ledger_info::resolve_graph_selector(&req.graph, view.binary_store.as_deref())
                .map_err(|e| ApiError::NotFound(e.to_string()))?;
        let snapshot = &view.snapshot;

        // Subject → group key. Each grouping property is scanned into its
        // own map; a subject gets a key only when it has a value for every
        // one of them, several joined with " | ". A subject with several
        // values for one property takes the first seen.
        let mut interner: HashMap<String, Arc<str>> = HashMap::new();
        let mut intern = |text: String| -> Arc<str> {
            if let Some(k) = interner.get(&text) {
                return Arc::clone(k);
            }
            let k: Arc<str> = Arc::from(text.as_str());
            interner.insert(text, Arc::clone(&k));
            k
        };
        let mut maps: Vec<HashMap<Sid, Arc<str>>> = Vec::with_capacity(req.group_by.len());
        for iri in &req.group_by {
            let p = snapshot.encode_iri_strict(iri).ok_or_else(|| {
                ApiError::NotFound(format!(
                    "group-by property '{iri}' is unknown to ledger '{ledger_id}'"
                ))
            })?;
            let flakes = property_flakes(&view, g_id, p).await?;
            if flakes.is_empty() {
                return Err(ApiError::NotFound(format!(
                    "group-by property '{iri}' has no values in ledger '{ledger_id}'"
                )));
            }
            let mut map: HashMap<Sid, Arc<str>> = HashMap::new();
            let mut scratch = String::new();
            for f in flakes {
                if map.contains_key(&f.s) {
                    continue;
                }
                let text = profile_value(snapshot, &f.o, &mut scratch).to_text();
                map.insert(f.s.clone(), intern(text));
            }
            maps.push(map);
        }
        // Taken by value: the one-key case is the common one, and
        // cloning it would hold two whole subject maps at once.
        let mut maps = maps.into_iter();
        let keys: Option<HashMap<Sid, Arc<str>>> = match maps.next() {
            None => None,
            Some(first) => {
                let rest: Vec<HashMap<Sid, Arc<str>>> = maps.collect();
                if rest.is_empty() {
                    Some(first)
                } else {
                    Some(
                        first
                            .into_iter()
                            .filter_map(|(s, head)| {
                                let mut key = head.to_string();
                                for m in &rest {
                                    key.push_str(" | ");
                                    key.push_str(m.get(&s)?);
                                }
                                Some((s, intern(key)))
                            })
                            .collect(),
                    )
                }
            }
        };

        let mut columns = Vec::with_capacity(req.columns.len());
        let mut skipped = Vec::new();
        let mut scratch = String::new();
        for iri in &req.columns {
            let Some(p) = snapshot.encode_iri_strict(iri) else {
                skipped.push(SkippedColumn {
                    name: iri.clone(),
                    reason: "property is unknown to this ledger".into(),
                });
                continue;
            };
            let flakes = property_flakes(&view, g_id, p).await?;
            if flakes.is_empty() {
                skipped.push(SkippedColumn {
                    name: iri.clone(),
                    reason: "property has no values in this graph".into(),
                });
                continue;
            }
            let mut acc = Accumulator::new(req);
            for f in flakes {
                let key = keys.as_ref().and_then(|m| m.get(&f.s)).map(|k| &**k);
                let v = profile_value(snapshot, &f.o, &mut scratch);
                acc.observe(key, v);
            }
            columns.push(acc.finish(iri.clone()));
        }

        Ok(ProfileReport {
            source: ledger_id.to_string(),
            t: Some(view.t),
            snapshot_id: None,
            group_by: req.group_by.clone(),
            columns,
            skipped,
        })
    }

    /// Profile columns of a lake table behind a graph source, streaming
    /// the table through the same scan the virtual graph reads, at the
    /// table's current snapshot. Peak memory is one batch plus the
    /// sketches.
    ///
    /// No view policy is applied.
    #[cfg(feature = "iceberg")]
    pub async fn profile_table(
        &self,
        graph_source_id: &str,
        table_name: &str,
        req: &ProfileRequest,
    ) -> Result<ProfileReport> {
        use fluree_db_query::r2rml::R2rmlTableProvider;
        use fluree_db_stats::tabular;
        use futures::StreamExt;

        let provider = crate::graph_source::FlureeR2rmlProvider::new(self);
        let snapshot_id = provider
            .current_snapshot_id(graph_source_id, table_name)
            .await?;
        let table_columns = provider
            .table_column_names(graph_source_id, table_name)
            .await?;
        for k in &req.group_by {
            if !table_columns.contains(k) {
                return Err(ApiError::NotFound(format!(
                    "group-by column '{k}' is not in table '{table_name}'"
                )));
            }
        }

        // The schema decides the columns up front, so an empty table still
        // reports what was asked for and what was not there.
        let names: Vec<String> = if req.columns.is_empty() {
            table_columns.clone()
        } else {
            req.columns.clone()
        };
        let mut accs: Vec<Option<Accumulator>> = Vec::with_capacity(names.len());
        let mut skipped = Vec::new();
        for name in &names {
            if table_columns.contains(name) {
                accs.push(Some(Accumulator::new(req)));
            } else {
                accs.push(None);
                skipped.push(SkippedColumn {
                    name: name.clone(),
                    reason: "column is not in the table".into(),
                });
            }
        }

        // Nothing the request named is in the table, so there is no
        // accumulator to feed. An empty projection means "every
        // column" to the scan, which would read the whole table to
        // produce a report of nothing but skips.
        if accs.iter().all(Option::is_none) {
            return Ok(ProfileReport {
                source: format!("{graph_source_id}/{table_name}"),
                t: None,
                snapshot_id,
                group_by: req.group_by.clone(),
                columns: Vec::new(),
                skipped,
            });
        }

        // Project only what the profile needs; every column when the
        // request named none.
        let mut projection: Vec<String> = if req.columns.is_empty() {
            Vec::new()
        } else {
            names
                .iter()
                .filter(|n| table_columns.contains(n))
                .cloned()
                .collect()
        };
        if !projection.is_empty() {
            for k in &req.group_by {
                if !projection.contains(k) {
                    projection.push(k.clone());
                }
            }
        }
        let mut stream = provider
            .scan_table(graph_source_id, table_name, &projection, &[], None, None)
            .await?;

        let mut key = String::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let key_cols: Vec<&fluree_db_tabular::Column> = req
                .group_by
                .iter()
                .filter_map(|k| batch.column_by_name(k))
                .collect();
            for (name, acc) in names.iter().zip(accs.iter_mut()) {
                let (Some(acc), Some(col)) = (acc.as_mut(), batch.column_by_name(name)) else {
                    continue;
                };
                for row in 0..batch.num_rows {
                    let k = if key_cols.is_empty() {
                        None
                    } else {
                        key.clear();
                        for (i, kc) in key_cols.iter().enumerate() {
                            if i > 0 {
                                key.push_str(" | ");
                            }
                            key.push_str(&tabular::display_at(kc, row));
                        }
                        Some(key.as_str())
                    };
                    acc.observe(k, tabular::value_at(col, row));
                }
            }
        }

        let columns = names
            .into_iter()
            .zip(accs)
            .filter_map(|(name, acc)| acc.map(|a| a.finish(name)))
            .collect();
        Ok(ProfileReport {
            source: format!("{graph_source_id}/{table_name}"),
            t: None,
            snapshot_id,
            group_by: req.group_by.clone(),
            columns,
            skipped,
        })
    }
}
