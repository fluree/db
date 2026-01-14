(ns fluree.db.tabular.iceberg
  "Iceberg implementation of ITabularSource using Fluree's storage abstraction.

   This namespace provides:
   - FlureeIcebergSource: Production-ready source using Fluree's FileIO
   - Re-exports create-iceberg-source from hadoop namespace for convenience

   For local development/testing, you can use either:
   1. create-iceberg-source (Hadoop-based, just needs a path)
   2. create-rest-iceberg-source (REST catalog, cloud-agnostic)
   3. create-fluree-iceberg-source (Fluree storage, needs a store)

   Supports:
   - Predicate pushdown (eq, ne, gt, gte, lt, lte, in, between, is-null, not-null, and, or)
   - Column projection
   - Time-travel via snapshot-id or as-of-time
   - Schema introspection
   - Statistics from snapshot summary
   - Arrow vectorized reads for high performance

   Performance optimizations:
   - Caffeine-based metadata cache for immutable metadata files
   - Block-caching SeekableInputStream for range reads"
  (:require [clojure.string :as str]
            [fluree.db.tabular.file-io :as file-io]
            [fluree.db.tabular.iceberg.core :as core]
            [fluree.db.tabular.iceberg.hadoop :as hadoop]
            [fluree.db.tabular.iceberg.rest :as rest]
            [fluree.db.tabular.protocol :as proto]
            [fluree.db.util.log :as log])
  (:import [com.github.benmanes.caffeine.cache Cache Caffeine]
           [org.apache.iceberg BaseTable Table StaticTableOperations]
           [org.apache.iceberg.io FileIO]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Table Metadata Cache
;;; ---------------------------------------------------------------------------

;; Global cache for loaded Table objects, keyed by metadata-location.
;; Iceberg metadata files are immutable (each new version creates a new file),
;; so we can cache Table objects indefinitely. The only mutable file is
;; version-hint.text which should not be cached.
;; Bounded to 100 entries to limit memory usage.
(defonce ^:private table-cache
  (delay
    (-> (Caffeine/newBuilder)
        (.maximumSize 100)
        ^Cache (.build))))

(defn- get-table-cache
  "Get the global table cache instance."
  ^Cache []
  @table-cache)

;;; ---------------------------------------------------------------------------
;;; Re-export Hadoop factory for backward compatibility
;;; ---------------------------------------------------------------------------

(def create-iceberg-source
  "Create an IcebergSource for querying Iceberg tables via Hadoop.

   Config:
     :warehouse-path - Root path to Iceberg warehouse (required)

   Example:
     (create-iceberg-source {:warehouse-path \"/path/to/warehouse\"})

   See fluree.db.tabular.iceberg.hadoop for details."
  hadoop/create-iceberg-source)

(def create-rest-iceberg-source
  "Create an IcebergSource using a REST catalog for discovery and
   Fluree's storage protocols for data access.

   Config:
     :uri        - REST catalog endpoint (required)
     :store      - Fluree storage store (required) - S3Store, FileStore, etc.
     :auth-token - Optional bearer token for REST API auth

   This approach uses REST API only for catalog discovery (list namespaces,
   tables, get metadata locations) while all file reads go through Fluree's
   existing storage infrastructure.

   See fluree.db.tabular.iceberg.rest for details."
  rest/create-rest-iceberg-source)

;;; ---------------------------------------------------------------------------
;;; FlureeIcebergSource Implementation
;;; ---------------------------------------------------------------------------

(defn- load-table-from-metadata
  "Load an Iceberg Table from a metadata location using StaticTableOperations.
   This avoids needing a full catalog - just point to the metadata JSON.

   Tables are cached by metadata-location since Iceberg metadata files are immutable.
   Each new snapshot creates a new metadata file, so caching is safe."
  ^Table [^FileIO file-io ^String metadata-location ^String table-name]
  (let [^Cache cache (get-table-cache)]
    (or (.getIfPresent cache metadata-location)
        (let [ops   (StaticTableOperations. metadata-location file-io)
              table (BaseTable. ops table-name)]
          (log/debug "Caching table metadata for:" metadata-location)
          (.put cache metadata-location table)
          table))))

(defn- resolve-metadata-location
  "Resolve the metadata location for an Iceberg table.

   If metadata-location is provided directly, use it.
   Otherwise, read version-hint.text from the table directory to find latest metadata."
  [^FileIO file-io warehouse-path table-name metadata-location]
  (or metadata-location
      ;; Read version-hint.text to find current metadata
      (let [hint-path (str warehouse-path "/" table-name "/metadata/version-hint.text")]
        (try
          (with-open [stream (.newStream (.newInputFile file-io hint-path))]
            (let [version (-> (slurp stream) str/trim)]
              (str warehouse-path "/" table-name "/metadata/v" version ".metadata.json")))
          (catch Exception e
            ;; Fall back to scanning metadata directory for latest
            (log/warn "Could not read version-hint.text for" table-name ":" (.getMessage e))
            nil)))))

(defrecord FlureeIcebergSource [^FileIO file-io warehouse-path metadata-cache]
  proto/ITabularSource

  (scan-batches [_ table-name {:keys [columns predicates snapshot-id as-of-time batch-size limit metadata-location]
                               :or {batch-size 4096}}]
    (let [meta-loc (or metadata-location
                       (get @metadata-cache table-name)
                       (let [loc (resolve-metadata-location file-io warehouse-path table-name nil)]
                         (when loc (swap! metadata-cache assoc table-name loc))
                         loc))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :warehouse warehouse-path})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (log/debug "FlureeIcebergSource scan-batches (Arrow):" {:table table-name
                                                              :batch-size batch-size
                                                              :metadata meta-loc})
      (core/scan-with-arrow table {:columns columns
                                   :predicates predicates
                                   :snapshot-id snapshot-id
                                   :as-of-time as-of-time
                                   :batch-size batch-size
                                   :limit limit})))

  (scan-arrow-batches [_ table-name {:keys [columns predicates snapshot-id as-of-time batch-size copy-batches metadata-location]
                                     :or {batch-size 4096 copy-batches true}}]
    (let [meta-loc (or metadata-location
                       (get @metadata-cache table-name)
                       (let [loc (resolve-metadata-location file-io warehouse-path table-name nil)]
                         (when loc (swap! metadata-cache assoc table-name loc))
                         loc))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :warehouse warehouse-path})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (log/debug "FlureeIcebergSource scan-arrow-batches (filtered):" {:table table-name
                                                                       :batch-size batch-size
                                                                       :predicates (count predicates)
                                                                       :copy-batches copy-batches
                                                                       :metadata meta-loc})
      ;; Use filtered Arrow batches for correct row-level filtering
      ;; When copy-batches is true (default), data is copied to avoid buffer reuse issues
      ;; When false, batches share underlying buffers - use for streaming consumption
      (core/scan-filtered-arrow-batches table {:columns columns
                                               :predicates predicates
                                               :snapshot-id snapshot-id
                                               :as-of-time as-of-time
                                               :batch-size batch-size
                                               :copy-batches copy-batches})))

  (scan-rows [this table-name opts]
    ;; scan-batches now returns row maps directly
    (proto/scan-batches this table-name opts))

  (get-schema [_ table-name {:keys [snapshot-id as-of-time metadata-location]}]
    (let [meta-loc (or metadata-location (get @metadata-cache table-name))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (core/extract-schema table {:snapshot-id snapshot-id :as-of-time as-of-time})))

  (get-statistics [_ table-name {:keys [snapshot-id metadata-location]}]
    (let [meta-loc (or metadata-location (get @metadata-cache table-name))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)]
      (core/extract-statistics table {:snapshot-id snapshot-id})))

  (supported-predicates [_]
    core/supported-predicate-ops)

  proto/ICloseable
  (close [_]
    (.close file-io)))

;;; ---------------------------------------------------------------------------
;;; Factory Function
;;; ---------------------------------------------------------------------------

(defn create-fluree-iceberg-source
  "Create an IcebergSource backed by Fluree storage (no Hadoop dependencies at runtime).

   This uses StaticTableOperations to load tables from known metadata locations,
   with file I/O provided by Fluree's storage protocols.

   Config:
     :store          - Fluree storage store (required) - must implement ByteStore
     :warehouse-path - Root path prefix for tables (optional, for path resolution)
     :file-io-opts   - Optional FileIO options map:
                       - :cache-instance - Shared Caffeine cache for block caching
                       - :block-size - Block size in bytes for range reads

   Example:
     (create-fluree-iceberg-source {:store my-s3-store
                                    :warehouse-path \"s3://bucket/warehouse\"
                                    :file-io-opts {:cache-instance my-cache}})

   Tables are loaded by:
   1. Direct metadata-location in scan opts
   2. Cached metadata location from previous scan
   3. Reading version-hint.text from table directory"
  [{:keys [store warehouse-path file-io-opts]}]
  {:pre [(some? store)]}
  (let [file-io (file-io/create-fluree-file-io store (or file-io-opts {}))]
    (->FlureeIcebergSource file-io (or warehouse-path "") (atom {}))))
