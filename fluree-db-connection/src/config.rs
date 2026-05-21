//! Configuration types for Fluree connections

use crate::error::{ConnectionError, Result};
use crate::graph::ConfigGraph;
use crate::vocab;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// A JSON-LD ConfigurationValue node (envVar/javaProp/defaultVal)
///
/// This matches the pattern used by the legacy configuration system where
/// leaf values can be indirected through environment variables and defaults.
#[derive(Debug, Clone, Default)]
pub struct ConfigValue {
    pub env_var: Option<String>,
    pub java_prop: Option<String>,
    pub default_val: Option<String>,
}

impl ConfigValue {
    pub fn resolve_string(&self) -> Option<String> {
        // Prefer environment variables on non-wasm targets.
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(var) = &self.env_var {
                if let Ok(val) = std::env::var(var) {
                    if !val.is_empty() {
                        return Some(val);
                    }
                }
            }
            // Java system properties are not a Rust concept, but for parity we allow
            // mapping them to environment variables (best-effort).
            if let Some(prop) = &self.java_prop {
                if let Ok(val) = std::env::var(prop) {
                    if !val.is_empty() {
                        return Some(val);
                    }
                }
            }
        }

        self.default_val.clone()
    }
}

/// Parsed S3 configuration (currently not implemented as a backend in Rust).
#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub bucket: Arc<str>,
    pub prefix: Option<Arc<str>>,
    pub endpoint: Option<Arc<str>>,
    pub read_timeout_ms: Option<u64>,
    pub write_timeout_ms: Option<u64>,
    pub list_timeout_ms: Option<u64>,
    pub max_retries: Option<u64>,
    pub retry_base_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub max_concurrent_requests: Option<usize>,
    /// Optional address identifier
    pub address_identifier: Option<Arc<str>>,
}

/// Connection defaults parsed from JSON-LD (`defaults` field).
#[derive(Debug, Clone)]
pub struct DefaultsConfig {
    pub identity: Option<IdentityDefaults>,
    pub indexing: Option<IndexingDefaults>,
}

#[derive(Debug, Clone)]
pub struct IdentityDefaults {
    pub public_key: Option<Arc<str>>,
    pub private_key: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct IndexingDefaults {
    pub reindex_min_bytes: Option<u64>,
    pub reindex_max_bytes: Option<u64>,
    pub max_old_indexes: Option<u64>,
    pub indexing_enabled: Option<bool>,
    pub track_class_stats: Option<bool>,
    pub gc_min_time_mins: Option<u64>,
}

/// Publisher configuration (nameservice backend selection).
#[derive(Debug, Clone)]
pub enum PublisherType {
    /// Storage-backed nameservice (writes records into a configured storage backend)
    Storage { storage: StorageConfig },
    /// DynamoDB-backed nameservice
    DynamoDb {
        table: Arc<str>,
        region: Option<Arc<str>>,
        endpoint: Option<Arc<str>>,
        timeout_ms: Option<u64>,
    },
    /// Unsupported publisher
    Unsupported { type_iri: String, raw: JsonValue },
}

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    pub id: Option<Arc<str>>,
    pub publisher_type: PublisherType,
}

/// Storage backend type
#[derive(Debug, Clone)]
pub enum StorageType {
    /// In-memory storage
    Memory,
    /// File-based storage
    File,
    /// S3 storage (not yet supported in Rust implementation)
    S3(S3StorageConfig),
    /// Unsupported storage type (parsed but not instantiatable)
    Unsupported { type_iri: String, raw: JsonValue },
}

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Optional identifier for the storage
    pub id: Option<Arc<str>>,
    /// Storage backend type
    pub storage_type: StorageType,
    /// Base path for file storage
    pub path: Option<Arc<str>>,
    /// Optional AES-256 encryption key (works with any storage backend: file, S3, memory)
    pub aes256_key: Option<Arc<str>>,
    /// Optional address identifier
    pub address_identifier: Option<Arc<str>>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            id: None,
            storage_type: StorageType::Memory,
            path: None,
            aes256_key: None,
            address_identifier: None,
        }
    }
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache size in MB
    pub max_mb: usize,
}

impl CacheConfig {
    /// Create a cache config with the given max MB.
    pub fn with_max_mb(max_mb: usize) -> Self {
        CacheConfig { max_mb }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        // Use memory-based default: tiered fraction of system RAM (see cache::default_cache_max_mb).
        let max_mb = crate::cache::default_cache_max_mb();
        CacheConfig { max_mb }
    }
}

/// Main connection configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Optional connection identifier
    pub id: Option<Arc<str>>,
    /// Maximum parallelism for operations
    pub parallelism: usize,
    /// Cache configuration
    pub cache: CacheConfig,
    /// Index storage configuration
    pub index_storage: StorageConfig,
    /// Optional separate commit storage
    pub commit_storage: Option<StorageConfig>,
    /// Optional primary publisher (nameservice) configuration
    pub primary_publisher: Option<PublisherConfig>,
    /// Optional connection defaults (identity + indexing options)
    pub defaults: Option<DefaultsConfig>,
    /// Optional address identifier resolver map (identifier -> StorageConfig).
    ///
    /// Used for routing reads based on address identifiers. When an address like
    /// `fluree:<identifier>:<method>://path` is read, the identifier is looked up
    /// in this map to route to the correct storage.
    pub address_identifiers: Option<HashMap<Arc<str>, StorageConfig>>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            id: None,
            parallelism: 4,
            cache: CacheConfig::default(),
            index_storage: StorageConfig::default(),
            commit_storage: None,
            primary_publisher: None,
            defaults: None,
            address_identifiers: None,
        }
    }
}

impl ConnectionConfig {
    /// Create a memory-backed connection config
    pub fn memory() -> Self {
        ConnectionConfig::default()
    }

    /// Create a file-backed connection config
    pub fn file(base_path: impl Into<Arc<str>>) -> Self {
        ConnectionConfig {
            index_storage: StorageConfig {
                id: None,
                storage_type: StorageType::File,
                path: Some(base_path.into()),
                aes256_key: None,
                address_identifier: None,
            },
            ..Default::default()
        }
    }

    /// Parse configuration from JSON
    pub fn from_json(json: &JsonValue) -> Result<Self> {
        let obj = json
            .as_object()
            .ok_or_else(|| ConnectionError::invalid_config("Configuration must be an object"))?;

        let mut config = ConnectionConfig::default();

        for (key, value) in obj {
            match key.as_str() {
                "@id" | "id" => {
                    if let Some(s) = value.as_str() {
                        config.id = Some(Arc::from(s));
                    }
                }
                "@type" | "type" => {
                    // Validate it's a connection type, but we already know this
                }
                "parallelism" => {
                    if let Some(n) = value.as_u64() {
                        config.parallelism = n as usize;
                    }
                }
                "cache" => {
                    config.cache = CacheConfig::from_json(value)?;
                }
                "indexStorage" | "index_storage" => {
                    config.index_storage = StorageConfig::from_json(value)?;
                }
                "commitStorage" | "commit_storage" => {
                    config.commit_storage = Some(StorageConfig::from_json(value)?);
                }
                // We parse publisher config for parity with the legacy JSON-LD config,
                // but the query connection does not use it yet.
                "primaryPublisher" | "primary_publisher" => {
                    // Best-effort: store unsupported publisher in flat JSON mode.
                    config.primary_publisher = Some(PublisherConfig {
                        id: None,
                        publisher_type: PublisherType::Unsupported {
                            type_iri: "primaryPublisher".to_string(),
                            raw: value.clone(),
                        },
                    });
                }
                "@context" | "context" => {
                    // Ignore context at config level
                }
                _ => {
                    return Err(ConnectionError::invalid_config(format!(
                        "Unknown configuration field: '{key}'"
                    )));
                }
            }
        }

        Ok(config)
    }

    /// Parse from legacy JSON-LD config
    ///
    /// This handles the JSON-LD format used by the legacy connection config:
    /// - @context with @base and @vocab
    /// - @graph array with nodes
    /// - @id references between nodes
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = json!({
    ///     "@context": {
    ///         "@base": "https://ns.flur.ee/config/connection/",
    ///         "@vocab": "https://ns.flur.ee/system#"
    ///     },
    ///     "@graph": [
    ///         {"@id": "fileStorage", "@type": "Storage", "filePath": "/data"},
    ///         {"@id": "conn", "@type": "Connection", "indexStorage": {"@id": "fileStorage"}}
    ///     ]
    /// });
    /// let conn_config = ConnectionConfig::from_json_ld(&config)?;
    /// ```
    pub fn from_json_ld(json: &JsonValue) -> Result<Self> {
        // 1. Expand with json-ld library
        let expanded = fluree_graph_json_ld::expand(json)?;

        // 2. Build graph index (handles flattening)
        let graph = ConfigGraph::from_expanded(&expanded)?;

        // 3. Find Connection node by type (error if multiple found)
        let conn_nodes = graph.find_by_type(vocab::TYPE_CONNECTION);
        if conn_nodes.len() > 1 {
            return Err(ConnectionError::invalid_config(
                "Multiple Connection nodes found in JSON-LD config; only one is allowed",
            ));
        }
        let conn_node = conn_nodes.first().ok_or_else(|| {
            ConnectionError::invalid_config("No Connection node found in JSON-LD config")
        })?;

        // 4. Extract fields (handle array-wrapped values, with ConfigValue support)
        let parallelism = get_int_field(conn_node, vocab::FIELD_PARALLELISM).unwrap_or(4) as usize;

        // Cache max MB: supports ConfigurationValue (envVar/javaProp/defaultVal)
        let cache_max_mb = resolve_u64(&graph, conn_node, vocab::FIELD_CACHE_MAX_MB)
            .map(|v| v as usize)
            .unwrap_or_else(crate::cache::default_cache_max_mb);

        // 5. Resolve storage references using resolve_first (single-valued fields)
        let index_storage = conn_node
            .get(vocab::FIELD_INDEX_STORAGE)
            .and_then(|v| graph.resolve_first(v))
            .map(|n| parse_storage_node(&graph, n))
            .transpose()?;

        let commit_storage = conn_node
            .get(vocab::FIELD_COMMIT_STORAGE)
            .and_then(|v| graph.resolve_first(v))
            .map(|n| parse_storage_node(&graph, n))
            .transpose()?;

        // 6. Resolve publisher references (nameservice)
        let primary_publisher = conn_node
            .get(vocab::FIELD_PRIMARY_PUBLISHER)
            .and_then(|v| graph.resolve_first(v))
            .map(|n| parse_publisher_node(&graph, n))
            .transpose()?;

        // 7. Parse defaults (identity + indexing options)
        let defaults = conn_node
            .get(vocab::FIELD_DEFAULTS)
            .and_then(|v| graph.resolve_first(v))
            .map(|n| parse_defaults_node(&graph, n))
            .transpose()?;

        // 8. Parse addressIdentifiers map (identifier -> StorageConfig)
        let address_identifiers = parse_address_identifiers_map(&graph, conn_node)?;

        Ok(ConnectionConfig {
            id: get_id(conn_node),
            parallelism,
            cache: CacheConfig::with_max_mb(cache_max_mb),
            index_storage: index_storage.ok_or_else(|| {
                ConnectionError::invalid_config("indexStorage required in Connection config")
            })?,
            commit_storage,
            primary_publisher,
            defaults,
            address_identifiers,
        })
    }
}

/// Parse storage config from a resolved JSON-LD node
fn parse_storage_node(graph: &ConfigGraph, node: &JsonValue) -> Result<StorageConfig> {
    let address_identifier =
        resolve_string(graph, node, vocab::FIELD_ADDRESS_IDENTIFIER).map(Arc::from);

    // Determine storage type from properties (not hardcoded strings)
    if node.get(vocab::FIELD_S3_BUCKET).is_some() {
        let bucket = resolve_string(graph, node, vocab::FIELD_S3_BUCKET).ok_or_else(|| {
            ConnectionError::invalid_config("S3 storage requires s3Bucket to be specified")
        })?;
        let s3 = S3StorageConfig {
            bucket: Arc::from(bucket),
            prefix: resolve_string(graph, node, vocab::FIELD_S3_PREFIX).map(Arc::from),
            endpoint: resolve_string(graph, node, vocab::FIELD_S3_ENDPOINT).map(Arc::from),
            read_timeout_ms: resolve_u64(graph, node, vocab::FIELD_S3_READ_TIMEOUT_MS),
            write_timeout_ms: resolve_u64(graph, node, vocab::FIELD_S3_WRITE_TIMEOUT_MS),
            list_timeout_ms: resolve_u64(graph, node, vocab::FIELD_S3_LIST_TIMEOUT_MS),
            max_retries: resolve_u64(graph, node, vocab::FIELD_S3_MAX_RETRIES),
            retry_base_delay_ms: resolve_u64(graph, node, vocab::FIELD_S3_RETRY_BASE_DELAY_MS),
            retry_max_delay_ms: resolve_u64(graph, node, vocab::FIELD_S3_RETRY_MAX_DELAY_MS),
            max_concurrent_requests: resolve_u64(
                graph,
                node,
                vocab::FIELD_S3_MAX_CONCURRENT_REQUESTS,
            )
            .map(|n| n as usize),
            address_identifier: address_identifier.clone(),
        };

        return Ok(StorageConfig {
            id: get_id(node),
            storage_type: StorageType::S3(s3),
            path: None,
            aes256_key: None,
            address_identifier,
        });
    }

    if node.get(vocab::FIELD_FILE_PATH).is_some() {
        let path = resolve_string(graph, node, vocab::FIELD_FILE_PATH).map(Arc::from);
        let aes256_key = resolve_string(graph, node, vocab::FIELD_AES256_KEY).map(Arc::from);
        return Ok(StorageConfig {
            id: get_id(node),
            storage_type: StorageType::File,
            path,
            aes256_key,
            address_identifier,
        });
    }

    // Memory storage: Storage node with no additional configuration
    Ok(StorageConfig {
        id: get_id(node),
        storage_type: StorageType::Memory,
        path: None,
        aes256_key: None,
        address_identifier,
    })
}

/// Parse defaults config from a resolved JSON-LD node
fn parse_defaults_node(graph: &ConfigGraph, node: &JsonValue) -> Result<DefaultsConfig> {
    let identity = node
        .get(vocab::FIELD_IDENTITY)
        .and_then(|v| graph.resolve_first(v))
        .map(|n| IdentityDefaults {
            public_key: resolve_string(graph, n, vocab::FIELD_PUBLIC_KEY).map(Arc::from),
            private_key: resolve_string(graph, n, vocab::FIELD_PRIVATE_KEY).map(Arc::from),
        });

    let indexing = node
        .get(vocab::FIELD_INDEXING)
        .and_then(|v| graph.resolve_first(v))
        .map(|n| IndexingDefaults {
            reindex_min_bytes: resolve_u64(graph, n, vocab::FIELD_REINDEX_MIN_BYTES),
            reindex_max_bytes: resolve_u64(graph, n, vocab::FIELD_REINDEX_MAX_BYTES),
            max_old_indexes: resolve_u64(graph, n, vocab::FIELD_MAX_OLD_INDEXES),
            indexing_enabled: resolve_bool(graph, n, vocab::FIELD_INDEXING_ENABLED),
            track_class_stats: resolve_bool(graph, n, vocab::FIELD_TRACK_CLASS_STATS),
            gc_min_time_mins: resolve_u64(graph, n, vocab::FIELD_GC_MIN_TIME_MINS),
        });

    Ok(DefaultsConfig { identity, indexing })
}

/// Parse publisher config from a resolved JSON-LD node
fn parse_publisher_node(graph: &ConfigGraph, node: &JsonValue) -> Result<PublisherConfig> {
    // DynamoDB nameservice
    if node.get(vocab::FIELD_DYNAMODB_TABLE).is_some() {
        let table = resolve_string(graph, node, vocab::FIELD_DYNAMODB_TABLE).ok_or_else(|| {
            ConnectionError::invalid_config("DynamoDB publisher requires dynamodbTable")
        })?;
        return Ok(PublisherConfig {
            id: get_id(node),
            publisher_type: PublisherType::DynamoDb {
                table: Arc::from(table),
                region: resolve_string(graph, node, vocab::FIELD_DYNAMODB_REGION).map(Arc::from),
                endpoint: resolve_string(graph, node, vocab::FIELD_DYNAMODB_ENDPOINT)
                    .map(Arc::from),
                timeout_ms: resolve_u64(graph, node, vocab::FIELD_DYNAMODB_TIMEOUT_MS),
            },
        });
    }

    // Storage-backed nameservice: publisher has a "storage" reference
    if let Some(storage_node) = node
        .get(vocab::FIELD_STORAGE)
        .and_then(|v| graph.resolve_first(v))
    {
        let storage = parse_storage_node(graph, storage_node)?;
        return Ok(PublisherConfig {
            id: get_id(node),
            publisher_type: PublisherType::Storage { storage },
        });
    }

    // Otherwise: unsupported publisher (preserve raw)
    let type_iri = get_type_iri(node).unwrap_or_else(|| vocab::TYPE_PUBLISHER.to_string());
    Ok(PublisherConfig {
        id: get_id(node),
        publisher_type: PublisherType::Unsupported {
            type_iri,
            raw: node.clone(),
        },
    })
}

/// Get first @type IRI from node
fn get_type_iri(node: &JsonValue) -> Option<String> {
    node.get("@type")
        .and_then(|t| match t {
            JsonValue::Array(arr) => arr.first().and_then(|v| v.as_str()),
            JsonValue::String(s) => Some(s.as_str()),
            _ => None,
        })
        .map(String::from)
}

/// Get string field from expanded JSON-LD (handles @value wrapper and arrays)
fn get_string_field(node: &JsonValue, field: &str) -> Option<String> {
    node.get(field)
        .and_then(|v| match v {
            JsonValue::Array(arr) => arr.first(),
            other => Some(other),
        })
        .and_then(|v| {
            v.get("@value")
                .and_then(|x| x.as_str())
                .or_else(|| v.as_str())
        })
        .map(String::from)
}

/// Resolve a string field that may be a raw value object or a ConfigurationValue node.
fn resolve_string(graph: &ConfigGraph, node: &JsonValue, field: &str) -> Option<String> {
    let raw = node.get(field)?;
    let resolved = graph.resolve_first(raw).unwrap_or(raw);

    // 1) Literal value
    if let Some(s) = get_string_field(
        &JsonValue::Object(
            [(field.to_string(), resolved.clone())]
                .into_iter()
                .collect(),
        ),
        field,
    ) {
        return Some(s);
    }

    // 2) ConfigurationValue node (envVar/javaProp/defaultVal)
    if resolved.get(vocab::FIELD_ENV_VAR).is_some()
        || resolved.get(vocab::FIELD_DEFAULT_VAL).is_some()
        || resolved.get(vocab::FIELD_JAVA_PROP).is_some()
    {
        let spec = ConfigValue {
            env_var: get_string_field(resolved, vocab::FIELD_ENV_VAR),
            java_prop: get_string_field(resolved, vocab::FIELD_JAVA_PROP),
            default_val: get_string_field(resolved, vocab::FIELD_DEFAULT_VAL),
        };
        return spec.resolve_string();
    }

    None
}

/// Resolve a u64 field that may be a raw value object or a ConfigurationValue node.
fn resolve_u64(graph: &ConfigGraph, node: &JsonValue, field: &str) -> Option<u64> {
    let raw = node.get(field)?;
    let resolved = graph.resolve_first(raw).unwrap_or(raw);

    // 1) Literal number
    let n = resolved
        .get("@value")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| resolved.as_u64());
    if n.is_some() {
        return n;
    }

    // 2) ConfigurationValue node
    if resolved.get(vocab::FIELD_ENV_VAR).is_some()
        || resolved.get(vocab::FIELD_DEFAULT_VAL).is_some()
        || resolved.get(vocab::FIELD_JAVA_PROP).is_some()
    {
        let spec = ConfigValue {
            env_var: get_string_field(resolved, vocab::FIELD_ENV_VAR),
            java_prop: get_string_field(resolved, vocab::FIELD_JAVA_PROP),
            default_val: get_string_field(resolved, vocab::FIELD_DEFAULT_VAL),
        };
        return spec.resolve_string().and_then(|s| s.parse::<u64>().ok());
    }

    None
}

/// Resolve a boolean field that may be a raw literal or a ConfigurationValue node.
fn resolve_bool(graph: &ConfigGraph, node: &JsonValue, field: &str) -> Option<bool> {
    let raw = node.get(field)?;
    let resolved = graph.resolve_first(raw).unwrap_or(raw);

    // 1) Literal boolean
    let b = resolved
        .get("@value")
        .and_then(serde_json::Value::as_bool)
        .or_else(|| resolved.as_bool());
    if b.is_some() {
        return b;
    }

    // 2) ConfigurationValue node
    if resolved.get(vocab::FIELD_ENV_VAR).is_some()
        || resolved.get(vocab::FIELD_DEFAULT_VAL).is_some()
        || resolved.get(vocab::FIELD_JAVA_PROP).is_some()
    {
        let spec = ConfigValue {
            env_var: get_string_field(resolved, vocab::FIELD_ENV_VAR),
            java_prop: get_string_field(resolved, vocab::FIELD_JAVA_PROP),
            default_val: get_string_field(resolved, vocab::FIELD_DEFAULT_VAL),
        };
        return spec.resolve_string().and_then(|s| match s.as_str() {
            "true" | "TRUE" | "1" => Some(true),
            "false" | "FALSE" | "0" => Some(false),
            _ => None,
        });
    }

    None
}

/// Get int field from expanded JSON-LD
fn get_int_field(node: &JsonValue, field: &str) -> Option<i64> {
    node.get(field)
        .and_then(|v| match v {
            JsonValue::Array(arr) => arr.first(),
            other => Some(other),
        })
        .and_then(|v| {
            v.get("@value")
                .and_then(serde_json::Value::as_i64)
                .or_else(|| v.as_i64())
        })
}

/// Get @id from node
fn get_id(node: &JsonValue) -> Option<Arc<str>> {
    node.get("@id").and_then(|v| v.as_str()).map(Arc::from)
}

impl CacheConfig {
    /// Parse cache configuration from JSON
    ///
    /// Supports both direct values and ConfigurationValue pattern for env var indirection:
    /// ```json
    /// {
    ///   "maxMb": 2000,
    ///   // or with env var:
    ///   "maxMb": {
    ///     "https://ns.flur.ee/system#envVar": "FLUREE_CACHE_MAX_MB",
    ///     "https://ns.flur.ee/system#defaultVal": "1000"
    ///   }
    /// }
    /// ```
    pub fn from_json(json: &JsonValue) -> Result<Self> {
        let obj = json
            .as_object()
            .ok_or_else(|| ConnectionError::invalid_config("Cache config must be an object"))?;

        let mut max_mb: Option<usize> = None;

        for (key, value) in obj {
            match key.as_str() {
                "@id" | "id" => {
                    // Ignored for cache
                }
                "@type" | "type" => {
                    // Validated but not stored
                }
                "maxMb" | "max_mb" | "cacheMaxMb" => {
                    max_mb = Some(resolve_config_value_usize(value)?);
                }
                _ => {
                    return Err(ConnectionError::invalid_config(format!(
                        "Unknown cache configuration field: '{key}'"
                    )));
                }
            }
        }

        Ok(match max_mb {
            Some(mb) => CacheConfig::with_max_mb(mb),
            None => CacheConfig::default(),
        })
    }
}

/// Resolve a JSON value that may be a direct number or a ConfigurationValue node.
fn resolve_config_value_usize(value: &JsonValue) -> Result<usize> {
    // Direct number
    if let Some(n) = value.as_u64() {
        return Ok(n as usize);
    }

    // ConfigurationValue object
    if let Some(obj) = value.as_object() {
        let spec = ConfigValue {
            env_var: obj
                .get(vocab::FIELD_ENV_VAR)
                .or_else(|| obj.get("envVar"))
                .and_then(|v| v.as_str())
                .map(String::from),
            java_prop: obj
                .get(vocab::FIELD_JAVA_PROP)
                .or_else(|| obj.get("javaProp"))
                .and_then(|v| v.as_str())
                .map(String::from),
            default_val: obj
                .get(vocab::FIELD_DEFAULT_VAL)
                .or_else(|| obj.get("defaultVal"))
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        if let Some(resolved) = spec.resolve_string() {
            return resolved.parse::<usize>().map_err(|_| {
                ConnectionError::invalid_config(format!(
                    "Could not parse cache config value as number: '{resolved}'"
                ))
            });
        }
    }

    Err(ConnectionError::invalid_config(
        "Cache config value must be a number or ConfigurationValue object",
    ))
}

impl StorageConfig {
    /// Parse storage configuration from JSON
    pub fn from_json(json: &JsonValue) -> Result<Self> {
        let obj = json
            .as_object()
            .ok_or_else(|| ConnectionError::invalid_config("Storage config must be an object"))?;

        // First, determine the type
        let type_value = obj.get("@type").or_else(|| obj.get("type"));
        let storage_type = match type_value {
            Some(t) => parse_storage_type(t, json)?,
            None => StorageType::Memory, // Default to memory
        };

        let mut config = StorageConfig {
            id: None,
            storage_type,
            path: None,
            aes256_key: None,
            address_identifier: None,
        };

        for (key, value) in obj {
            match key.as_str() {
                "@id" | "id" => {
                    if let Some(s) = value.as_str() {
                        config.id = Some(Arc::from(s));
                    }
                }
                "@type" | "type" => {
                    // Already handled
                }
                // Simple flat config uses `path`, JSON-LD-style configs use `filePath`.
                "path" | "basePath" | "base_path" | "filePath" => {
                    if let Some(s) = value.as_str() {
                        config.path = Some(Arc::from(s));
                    }
                }
                "AES256Key" | "aes256Key" | "aes256_key" => {
                    if let Some(s) = value.as_str() {
                        config.aes256_key = Some(Arc::from(s));
                    }
                }
                "addressIdentifier" | "address_identifier" => {
                    if let Some(s) = value.as_str() {
                        config.address_identifier = Some(Arc::from(s));
                    }
                }
                _ => {
                    // For unsupported types, we allow unknown fields
                    if !matches!(config.storage_type, StorageType::Unsupported { .. }) {
                        return Err(ConnectionError::invalid_config(format!(
                            "Unknown storage configuration field: '{key}'"
                        )));
                    }
                }
            }
        }

        Ok(config)
    }
}

/// Parse storage type from JSON value
fn parse_storage_type(type_val: &JsonValue, raw: &JsonValue) -> Result<StorageType> {
    let type_str = type_val
        .as_str()
        .ok_or_else(|| ConnectionError::invalid_config("Storage @type must be a string"))?;

    // Normalize common type IRIs
    let storage_type = match type_str {
        "Memory" | "MemoryStorage" | "fluree:MemoryStorage" => StorageType::Memory,
        "File" | "FileStorage" | "fluree:FileStorage" => StorageType::File,
        "S3" | "S3Storage" | "fluree:S3Storage" => {
            // We'll parse concrete fields in JSON-LD mode; in flat mode, preserve raw.
            StorageType::Unsupported {
                type_iri: type_str.to_string(),
                raw: raw.clone(),
            }
        }
        other => StorageType::Unsupported {
            type_iri: other.to_string(),
            raw: raw.clone(),
        },
    };

    Ok(storage_type)
}

/// Parse addressIdentifiers map from JSON-LD.
///
/// The addressIdentifiers field is a map of identifier strings to storage references:
/// ```json
/// "addressIdentifiers": {
///   "commit-storage": {"@id": "commitS3"},
///   "index-storage": {"@id": "indexS3"}
/// }
/// ```
///
/// After JSON-LD expansion, this becomes an object in the expanded form.
/// We iterate over the keys (which are the identifiers), resolve the @id references,
/// and parse the Storage nodes to get the full StorageConfig.
fn parse_address_identifiers_map(
    graph: &ConfigGraph,
    conn_node: &JsonValue,
) -> Result<Option<HashMap<Arc<str>, StorageConfig>>> {
    let Some(identifiers_value) = conn_node.get(vocab::FIELD_ADDRESS_IDENTIFIERS) else {
        return Ok(None);
    };

    // In expanded JSON-LD, the value is wrapped in an array
    let identifiers_obj = match identifiers_value {
        JsonValue::Array(arr) => arr.first(),
        other => Some(other),
    };

    let Some(identifiers) = identifiers_obj else {
        return Ok(None);
    };

    let Some(map) = identifiers.as_object() else {
        return Ok(None);
    };

    let mut result = HashMap::new();

    for (key, value) in map {
        // Skip JSON-LD keywords
        if key.starts_with('@') {
            continue;
        }

        // Extract the local name from the key (strip vocab prefix if present)
        let identifier = if let Some(local) = key.strip_prefix(vocab::SYSTEM_VOCAB) {
            local
        } else {
            key.as_str()
        };

        // Skip empty identifiers
        if identifier.is_empty() {
            continue;
        }

        // Resolve the value to a storage node using ConfigGraph
        let storage_node = graph.resolve_first(value).ok_or_else(|| {
            ConnectionError::invalid_config(format!(
                "addressIdentifiers[{identifier}] must reference a Storage node"
            ))
        })?;

        // Parse the storage node to get the full StorageConfig
        let storage_config = parse_storage_node(graph, storage_node)?;
        result.insert(Arc::from(identifier), storage_config);
    }

    if result.is_empty() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_config() {
        let config = ConnectionConfig::default();
        assert!(config.id.is_none());
        assert_eq!(config.parallelism, 4);
        assert!(matches!(
            config.index_storage.storage_type,
            StorageType::Memory
        ));
    }

    #[test]
    fn test_file_config() {
        let config = ConnectionConfig::file("/data/fluree");
        assert!(matches!(
            config.index_storage.storage_type,
            StorageType::File
        ));
        assert_eq!(config.index_storage.path.as_deref(), Some("/data/fluree"));
    }

    #[test]
    fn test_parse_simple_config() {
        let json = json!({
            "@id": "my-connection",
            "parallelism": 8
        });

        let config = ConnectionConfig::from_json(&json).unwrap();
        assert_eq!(config.id.as_deref(), Some("my-connection"));
        assert_eq!(config.parallelism, 8);
    }

    #[test]
    fn test_parse_config_with_storage() {
        let json = json!({
            "indexStorage": {
                "@type": "FileStorage",
                "path": "/data/index"
            }
        });

        let config = ConnectionConfig::from_json(&json).unwrap();
        assert!(matches!(
            config.index_storage.storage_type,
            StorageType::File
        ));
        assert_eq!(config.index_storage.path.as_deref(), Some("/data/index"));
    }

    #[test]
    fn test_parse_config_with_cache() {
        let json = json!({
            "cache": {
                "maxMb": 256
            }
        });

        let config = ConnectionConfig::from_json(&json).unwrap();
        assert_eq!(config.cache.max_mb, 256);
    }

    #[test]
    fn test_unknown_field_errors() {
        let json = json!({
            "unknownField": "value"
        });

        let result = ConnectionConfig::from_json(&json);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown configuration field"));
    }

    #[test]
    fn test_unsupported_storage_type_parses() {
        let json = json!({
            "indexStorage": {
                "@type": "S3Storage",
                "bucket": "my-bucket",
                "region": "us-east-1"
            }
        });

        let config = ConnectionConfig::from_json(&json).unwrap();
        match &config.index_storage.storage_type {
            StorageType::Unsupported { type_iri, raw } => {
                assert_eq!(type_iri, "S3Storage");
                assert_eq!(raw["bucket"], "my-bucket");
            }
            _ => panic!("Expected Unsupported storage type"),
        }
    }

    #[test]
    fn test_jsonld_config_with_unsupported_components_parses() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "fileStorage", "@type": "Storage", "filePath": "/tmp/test"},
                {
                    "@id": "publisher",
                    "@type": "Publisher",
                    "storage": {"@id": "fileStorage"}
                },
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "indexStorage": {"@id": "fileStorage"},
                    "primaryPublisher": {"@id": "publisher"}
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse JSON-LD config");
        assert!(matches!(
            parsed.index_storage.storage_type,
            StorageType::File
        ));
        assert_eq!(parsed.index_storage.path.as_deref(), Some("/tmp/test"));
    }

    #[test]
    fn test_jsonld_config_index_storage_only() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "fileStorage", "@type": "Storage", "filePath": "/data/fluree"},
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "parallelism": 8,
                    "indexStorage": {"@id": "fileStorage"}
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse config");
        assert_eq!(parsed.parallelism, 8);
        assert!(parsed.commit_storage.is_none());
    }

    #[test]
    fn test_jsonld_memory_storage_config() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "memStorage", "@type": "Storage"},
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "indexStorage": {"@id": "memStorage"}
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse memory config");
        assert!(matches!(
            parsed.index_storage.storage_type,
            StorageType::Memory
        ));
    }

    #[test]
    fn test_jsonld_expansion_array_wrapped_values() {
        // After JSON-LD expansion, scalar values are wrapped in arrays like [{"@value": 4}].
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "fs", "@type": "Storage", "filePath": "/path/to/data"},
                {
                    "@id": "conn",
                    "@type": "Connection",
                    "parallelism": 16,
                    "cacheMaxMb": 2000,
                    "indexStorage": {"@id": "fs"}
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
        assert_eq!(parsed.parallelism, 16);
        assert_eq!(parsed.cache.max_mb, 2000);
        assert_eq!(parsed.index_storage.path.as_deref(), Some("/path/to/data"));
    }

    #[test]
    fn test_jsonld_error_no_connection_node() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "fileStorage", "@type": "Storage", "filePath": "/tmp/test"}
            ]
        });

        let result = ConnectionConfig::from_json_ld(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No Connection node"));
    }

    #[test]
    fn test_jsonld_error_no_index_storage() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "parallelism": 4
                }
            ]
        });

        let result = ConnectionConfig::from_json_ld(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("indexStorage required"));
    }

    #[test]
    fn test_jsonld_error_multiple_connection_nodes() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "fs", "@type": "Storage", "filePath": "/tmp/test"},
                {
                    "@id": "conn1",
                    "@type": "Connection",
                    "indexStorage": {"@id": "fs"}
                },
                {
                    "@id": "conn2",
                    "@type": "Connection",
                    "indexStorage": {"@id": "fs"}
                }
            ]
        });

        let result = ConnectionConfig::from_json_ld(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Multiple Connection nodes"));
    }

    #[test]
    fn test_jsonld_address_identifiers_parsing() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "memStorage", "@type": "Storage"},
                {"@id": "fileStorage", "@type": "Storage", "filePath": "/data/commits"},
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "indexStorage": {"@id": "memStorage"},
                    "addressIdentifiers": {
                        "commit-storage": {"@id": "fileStorage"},
                        "index-storage": {"@id": "memStorage"}
                    }
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
        let addr_ids = parsed
            .address_identifiers
            .as_ref()
            .expect("addressIdentifiers should be present");
        assert_eq!(addr_ids.len(), 2);

        let commit_storage = addr_ids
            .get("commit-storage")
            .expect("commit-storage should exist");
        assert!(matches!(commit_storage.storage_type, StorageType::File));
        assert_eq!(commit_storage.path.as_deref(), Some("/data/commits"));

        let index_storage = addr_ids
            .get("index-storage")
            .expect("index-storage should exist");
        assert!(matches!(index_storage.storage_type, StorageType::Memory));
    }

    #[test]
    fn test_jsonld_no_address_identifiers_backward_compat() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "storage", "@type": "Storage"},
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "indexStorage": {"@id": "storage"}
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
        assert!(parsed.address_identifiers.is_none());
    }

    #[test]
    fn test_jsonld_multiple_address_identifiers() {
        let config = json!({
            "@context": {
                "@base": "https://ns.flur.ee/config/connection/",
                "@vocab": "https://ns.flur.ee/system#"
            },
            "@graph": [
                {"@id": "storage1", "@type": "Storage", "filePath": "/data/store1"},
                {"@id": "storage2", "@type": "Storage", "filePath": "/data/store2"},
                {"@id": "storage3", "@type": "Storage", "filePath": "/data/store3"},
                {"@id": "memStorage", "@type": "Storage"},
                {
                    "@id": "connection",
                    "@type": "Connection",
                    "indexStorage": {"@id": "memStorage"},
                    "addressIdentifiers": {
                        "store-1": {"@id": "storage1"},
                        "store-2": {"@id": "storage2"},
                        "store-3": {"@id": "storage3"}
                    }
                }
            ]
        });

        let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
        let addr_ids = parsed
            .address_identifiers
            .as_ref()
            .expect("addressIdentifiers should be present");
        assert_eq!(addr_ids.len(), 3);

        assert_eq!(
            addr_ids.get("store-1").unwrap().path.as_deref(),
            Some("/data/store1")
        );
        assert_eq!(
            addr_ids.get("store-2").unwrap().path.as_deref(),
            Some("/data/store2")
        );
        assert_eq!(
            addr_ids.get("store-3").unwrap().path.as_deref(),
            Some("/data/store3")
        );
    }
}
