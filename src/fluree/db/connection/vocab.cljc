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
