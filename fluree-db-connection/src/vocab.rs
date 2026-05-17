//! Fluree system vocabulary IRIs (post-expansion)
//!
//! These constants represent the fully expanded IRIs used in JSON-LD
//! configuration documents after expansion processing.

/// Base IRI for connection configuration identifiers
pub const CONFIG_BASE: &str = "https://ns.flur.ee/config/connection/";

/// Base IRI for Fluree system vocabulary
pub const SYSTEM_VOCAB: &str = "https://ns.flur.ee/system#";

// Types

/// ConfigurationValue type IRI
pub const TYPE_CONFIG_VALUE: &str = "https://ns.flur.ee/system#ConfigurationValue";

/// Storage type IRI
pub const TYPE_STORAGE: &str = "https://ns.flur.ee/system#Storage";

/// Connection type IRI
pub const TYPE_CONNECTION: &str = "https://ns.flur.ee/system#Connection";

/// Publisher type IRI
pub const TYPE_PUBLISHER: &str = "https://ns.flur.ee/system#Publisher";

// Storage fields

/// File path field for file-based storage
pub const FIELD_FILE_PATH: &str = "https://ns.flur.ee/system#filePath";

/// AES-256 key field for file-based storage (optional)
pub const FIELD_AES256_KEY: &str = "https://ns.flur.ee/system#AES256Key";

/// S3 bucket field for S3 storage
pub const FIELD_S3_BUCKET: &str = "https://ns.flur.ee/system#s3Bucket";

/// S3 prefix field for S3 storage
pub const FIELD_S3_PREFIX: &str = "https://ns.flur.ee/system#s3Prefix";

/// S3 endpoint field for S3 storage
pub const FIELD_S3_ENDPOINT: &str = "https://ns.flur.ee/system#s3Endpoint";

/// S3 read timeout (ms)
pub const FIELD_S3_READ_TIMEOUT_MS: &str = "https://ns.flur.ee/system#s3ReadTimeoutMs";

/// S3 write timeout (ms)
pub const FIELD_S3_WRITE_TIMEOUT_MS: &str = "https://ns.flur.ee/system#s3WriteTimeoutMs";

/// S3 list timeout (ms)
pub const FIELD_S3_LIST_TIMEOUT_MS: &str = "https://ns.flur.ee/system#s3ListTimeoutMs";

/// S3 max retries
pub const FIELD_S3_MAX_RETRIES: &str = "https://ns.flur.ee/system#s3MaxRetries";

/// S3 retry base delay (ms)
pub const FIELD_S3_RETRY_BASE_DELAY_MS: &str = "https://ns.flur.ee/system#s3RetryBaseDelayMs";

/// S3 retry max delay (ms)
pub const FIELD_S3_RETRY_MAX_DELAY_MS: &str = "https://ns.flur.ee/system#s3RetryMaxDelayMs";

/// Maximum concurrent S3 SDK requests per storage instance.
pub const FIELD_S3_MAX_CONCURRENT_REQUESTS: &str =
    "https://ns.flur.ee/system#s3MaxConcurrentRequests";

/// Optional address identifier for a storage backend
///
/// Used in legacy configs to embed a storage identifier into Fluree addresses, e.g.
/// `fluree:{addressIdentifier}:s3://...`.
pub const FIELD_ADDRESS_IDENTIFIER: &str = "https://ns.flur.ee/system#addressIdentifier";

// ConfigurationValue fields

/// Environment variable name for ConfigurationValue
pub const FIELD_ENV_VAR: &str = "https://ns.flur.ee/system#envVar";

/// Java system property name for ConfigurationValue
pub const FIELD_JAVA_PROP: &str = "https://ns.flur.ee/system#javaProp";

/// Default value for ConfigurationValue
pub const FIELD_DEFAULT_VAL: &str = "https://ns.flur.ee/system#defaultVal";

// Connection fields

/// Parallelism setting for query execution
pub const FIELD_PARALLELISM: &str = "https://ns.flur.ee/system#parallelism";

/// Maximum cache size in megabytes
pub const FIELD_CACHE_MAX_MB: &str = "https://ns.flur.ee/system#cacheMaxMb";

/// Index storage reference
pub const FIELD_INDEX_STORAGE: &str = "https://ns.flur.ee/system#indexStorage";

/// Commit storage reference
pub const FIELD_COMMIT_STORAGE: &str = "https://ns.flur.ee/system#commitStorage";

/// Address identifiers map for routing reads to specific storages
///
/// Maps identifier strings to storage node references:
/// ```json
/// "addressIdentifiers": {
///   "commit-storage": {"@id": "commitS3"},
///   "index-storage": {"@id": "indexS3"}
/// }
/// ```
pub const FIELD_ADDRESS_IDENTIFIERS: &str = "https://ns.flur.ee/system#addressIdentifiers";

/// Primary publisher reference
pub const FIELD_PRIMARY_PUBLISHER: &str = "https://ns.flur.ee/system#primaryPublisher";

/// Storage reference field on Publisher nodes
pub const FIELD_STORAGE: &str = "https://ns.flur.ee/system#storage";

// DynamoDB nameservice fields (publisher config)
pub const FIELD_DYNAMODB_TABLE: &str = "https://ns.flur.ee/system#dynamodbTable";
pub const FIELD_DYNAMODB_REGION: &str = "https://ns.flur.ee/system#dynamodbRegion";
pub const FIELD_DYNAMODB_ENDPOINT: &str = "https://ns.flur.ee/system#dynamodbEndpoint";
pub const FIELD_DYNAMODB_TIMEOUT_MS: &str = "https://ns.flur.ee/system#dynamodbTimeoutMs";

// Defaults (connection-level)
pub const FIELD_DEFAULTS: &str = "https://ns.flur.ee/system#defaults";
pub const FIELD_IDENTITY: &str = "https://ns.flur.ee/system#identity";
pub const FIELD_PUBLIC_KEY: &str = "https://ns.flur.ee/system#publicKey";
pub const FIELD_PRIVATE_KEY: &str = "https://ns.flur.ee/system#privateKey";
pub const FIELD_INDEXING: &str = "https://ns.flur.ee/system#indexing";
pub const FIELD_REINDEX_MIN_BYTES: &str = "https://ns.flur.ee/system#reindexMinBytes";
pub const FIELD_REINDEX_MAX_BYTES: &str = "https://ns.flur.ee/system#reindexMaxBytes";
pub const FIELD_MAX_OLD_INDEXES: &str = "https://ns.flur.ee/system#maxOldIndexes";
pub const FIELD_INDEXING_ENABLED: &str = "https://ns.flur.ee/system#indexingEnabled";
pub const FIELD_TRACK_CLASS_STATS: &str = "https://ns.flur.ee/system#trackClassStats";
pub const FIELD_GC_MIN_TIME_MINS: &str = "https://ns.flur.ee/system#gcMinTimeMins";
