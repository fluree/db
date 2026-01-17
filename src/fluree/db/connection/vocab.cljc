(ns fluree.db.connection.vocab
  (:refer-clojure :exclude [identity]))

#?(:clj (set! *warn-on-reflection* true))

(def system-ns
  "https://ns.flur.ee/system#")

(defn system-iri
  [s]
  (str system-ns s))

(def config-type
  (system-iri "Configuration"))

(def config-val-type
  (system-iri "ConfigurationValue"))

(def connection-type
  (system-iri "Connection"))

(def storage-type
  (system-iri "Storage"))

(def publisher-type
  (system-iri "Publisher"))

(def system-type
  (system-iri "System"))

(def env-var
  (system-iri "envVar"))

(def java-prop
  (system-iri "javaProp"))

(def default-val
  (system-iri "defaultVal"))

(def address-identifier
  (system-iri "addressIdentifier"))

(def address-identifiers
  (system-iri "addressIdentifiers"))

(def file-path
  (system-iri "filePath"))

(def aes256-key
  (system-iri "AES256Key"))

(def s3-bucket
  (system-iri "s3Bucket"))

(def s3-prefix
  (system-iri "s3Prefix"))

(def s3-endpoint
  (system-iri "s3Endpoint"))

(def s3-read-timeout-ms
  (system-iri "s3ReadTimeoutMs"))

(def s3-write-timeout-ms
  (system-iri "s3WriteTimeoutMs"))

(def s3-list-timeout-ms
  (system-iri "s3ListTimeoutMs"))

(def s3-max-retries
  (system-iri "s3MaxRetries"))

(def s3-retry-base-delay-ms
  (system-iri "s3RetryBaseDelayMs"))

(def s3-retry-max-delay-ms
  (system-iri "s3RetryMaxDelayMs"))

(def storage
  (system-iri "storage"))

(def ipfs-endpoint
  (system-iri "ipfsEndpoint"))

(def ipns-key
  (system-iri "ipnsKey"))

(def parallelism
  (system-iri "parallelism"))

(def cache-max-mb
  (system-iri "cacheMaxMb"))

(def commit-storage
  (system-iri "commitStorage"))

(def index-storage
  (system-iri "indexStorage"))

(def primary-publisher
  (system-iri "primaryPublisher"))

(def secondary-publishers
  (system-iri "secondaryPublishers"))

(def remote-systems
  (system-iri "remoteSystems"))

(def server-urls
  (system-iri "serverUrls"))

(def defaults
  (system-iri "defaults"))

(def identity
  (system-iri "identity"))

(def public-key
  (system-iri "publicKey"))

(def private-key
  (system-iri "privateKey"))

(def index-options
  (system-iri "indexing"))

(def reindex-min-bytes
  (system-iri "reindexMinBytes"))

(def reindex-max-bytes
  (system-iri "reindexMaxBytes"))

(def max-old-indexes
  (system-iri "maxOldIndexes"))

(def indexing-enabled
  (system-iri "indexingEnabled"))

(def track-class-stats
  (system-iri "trackClassStats"))

(def connection
  (system-iri "connection"))

(def connection-config
  (system-iri "connectionConfig"))

;; DynamoDB nameservice config
(def dynamodb-table
  (system-iri "dynamodbTable"))

(def dynamodb-region
  (system-iri "dynamodbRegion"))

(def dynamodb-endpoint
  (system-iri "dynamodbEndpoint"))

(def dynamodb-timeout-ms
  (system-iri "dynamodbTimeoutMs"))

;; -----------------------------------------------------------------------------
;; Iceberg / Virtual Graph configuration (nameservice-level)
;; -----------------------------------------------------------------------------

;; Iceberg configuration node types
(def iceberg-catalog-class
  (system-iri "IcebergCatalog"))

(def iceberg-auth-class
  (system-iri "IcebergAuth"))

(def iceberg-cache-class
  (system-iri "IcebergCache"))

;; Publisher/Connection policy toggles
(def virtual-graph-allow-publish
  "If false, disallow publishing new virtual graphs into the nameservice."
  (system-iri "virtualGraphAllowPublish"))

(def iceberg-allow-dynamic-virtual-graphs
  "If false, disallow dynamic creation of Iceberg virtual graphs."
  (system-iri "icebergAllowDynamicVirtualGraphs"))

(def iceberg-allow-dynamic-catalogs
  "If false, only configured catalogs may be used."
  (system-iri "icebergAllowDynamicCatalogs"))

(def iceberg-persist-dynamic-catalog-secrets
  "If true, dynamic catalog secrets may be persisted (recommended only with encryption-at-rest)."
  (system-iri "icebergPersistDynamicCatalogSecrets"))

(def iceberg-allowed-catalog-names
  "Optional allow-list of catalog names that may be used."
  (system-iri "icebergAllowedCatalogNames"))

;; Catalog configuration
(def iceberg-catalogs
  "List of IcebergCatalog nodes configured for this environment."
  (system-iri "icebergCatalogs"))

(def iceberg-catalog-name
  "A stable name/identifier for this catalog (unique within config)."
  (system-iri "icebergCatalogName"))

(def iceberg-catalog-type
  "Catalog type, e.g. \"rest\". Reserved for future expansion."
  (system-iri "icebergCatalogType"))

(def iceberg-rest-uri
  "REST catalog base URI."
  (system-iri "icebergRestUri"))

(def iceberg-default-headers
  "Optional additional headers to send to the REST catalog."
  (system-iri "icebergDefaultHeaders"))

(def iceberg-allow-vended-credentials
  "If true, allow requesting vended credentials from the catalog."
  (system-iri "icebergAllowVendedCredentials"))

;; Auth configuration
(def iceberg-auth
  "Reference to an IcebergAuth node."
  (system-iri "icebergAuth"))

(def iceberg-auth-type
  "Auth type for REST catalog, e.g. \"bearer\", \"apiKey\", or \"none\"."
  (system-iri "icebergAuthType"))

(def iceberg-bearer-token
  "Bearer token for REST catalog (use ConfigurationValue for env/java-prop)."
  (system-iri "icebergBearerToken"))

(def iceberg-api-key
  "API key for REST catalog (use ConfigurationValue for env/java-prop)."
  (system-iri "icebergApiKey"))

;; Cache configuration
(def iceberg-cache
  "Reference to an IcebergCache node."
  (system-iri "icebergCache"))

(def iceberg-cache-enabled
  "Enable/disable Iceberg caches."
  (system-iri "icebergCacheEnabled"))

(def iceberg-cache-dir
  "Cache directory path (Lambda default should be /tmp)."
  (system-iri "icebergCacheDir"))

(def iceberg-mem-cache-mb
  "In-memory cache budget in MB (e.g., for range-block caching)."
  (system-iri "icebergMemCacheMb"))

(def iceberg-disk-cache-mb
  "On-disk cache budget in MB."
  (system-iri "icebergDiskCacheMb"))

(def iceberg-block-size-mb
  "Byte-range block size in MB."
  (system-iri "icebergBlockSizeMb"))

(def iceberg-cache-ttl-seconds
  "Optional TTL in seconds for cache entries."
  (system-iri "icebergCacheTtlSeconds"))
