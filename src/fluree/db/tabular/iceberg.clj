(ns fluree.db.tabular.iceberg
  "Iceberg implementation of ITabularSource.

   Two implementations provided:
   1. IcebergSource - Uses HadoopTables (requires Hadoop deps, simple for local dev)
   2. FlureeIcebergSource - Uses StaticTableOperations + FlureeFileIO (no Hadoop deps)

   Uses IcebergGenerics for row-oriented reads. For production workloads
   with large tables, consider upgrading to Arrow vectorized reads via
   iceberg-arrow module.

   Supports:
   - Predicate pushdown (eq, ne, gt, gte, lt, lte, in, between, is-null, not-null, and, or)
   - Column projection
   - Time-travel via snapshot-id or as-of-time
   - Schema introspection
   - Statistics from snapshot summary"
  (:require [clojure.string :as str]
            [fluree.db.tabular.file-io :as file-io]
            [fluree.db.tabular.protocol :as proto]
            [fluree.db.util.log :as log])
  (:import [java.time Instant]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.iceberg BaseTable Schema Snapshot StaticTableOperations Table]
           [org.apache.iceberg.catalog TableIdentifier]
           [org.apache.iceberg.data IcebergGenerics]
           [org.apache.iceberg.expressions Expressions Expression]
           [org.apache.iceberg.hadoop HadoopTables]
           [org.apache.iceberg.io CloseableIterable FileIO]
           [org.apache.iceberg.types Type Types$NestedField]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Predicate Translation
;;; ---------------------------------------------------------------------------

(defn- predicate->iceberg-expr
  "Convert internal predicate map to Iceberg Expression.

   Supported ops: :eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or"
  ^Expression [{:keys [column op value predicates]}]
  (case op
    :eq        (Expressions/equal ^String column value)
    :ne        (Expressions/notEqual ^String column value)
    :gt        (Expressions/greaterThan ^String column value)
    :gte       (Expressions/greaterThanOrEqual ^String column value)
    :lt        (Expressions/lessThan ^String column value)
    :lte       (Expressions/lessThanOrEqual ^String column value)
    :in        (Expressions/in ^String column ^objects (into-array Object value))
    :between   (Expressions/and
                (Expressions/greaterThanOrEqual ^String column (first value))
                (Expressions/lessThanOrEqual ^String column (second value)))
    :is-null   (Expressions/isNull ^String column)
    :not-null  (Expressions/notNull ^String column)
    :and       (reduce (fn [^Expression a ^Expression b] (Expressions/and a b))
                       (map predicate->iceberg-expr predicates))
    :or        (reduce (fn [^Expression a ^Expression b] (Expressions/or a b))
                       (map predicate->iceberg-expr predicates))
    ;; Unknown op - return always-true (no filtering)
    (Expressions/alwaysTrue)))

(defn- predicates->expression
  "Combine multiple predicates with AND."
  ^Expression [predicates]
  (if (seq predicates)
    (reduce (fn [^Expression a ^Expression b] (Expressions/and a b))
            (map predicate->iceberg-expr predicates))
    (Expressions/alwaysTrue)))

;;; ---------------------------------------------------------------------------
;;; Type Mapping
;;; ---------------------------------------------------------------------------

(defn- iceberg-type->keyword
  "Map Iceberg Type to keyword."
  [^Type t]
  (condp = (.typeId t)
    org.apache.iceberg.types.Type$TypeID/BOOLEAN   :boolean
    org.apache.iceberg.types.Type$TypeID/INTEGER   :int
    org.apache.iceberg.types.Type$TypeID/LONG      :long
    org.apache.iceberg.types.Type$TypeID/FLOAT     :float
    org.apache.iceberg.types.Type$TypeID/DOUBLE    :double
    org.apache.iceberg.types.Type$TypeID/STRING    :string
    org.apache.iceberg.types.Type$TypeID/DATE      :date
    org.apache.iceberg.types.Type$TypeID/TIME      :time
    org.apache.iceberg.types.Type$TypeID/TIMESTAMP :timestamp
    org.apache.iceberg.types.Type$TypeID/BINARY    :binary
    org.apache.iceberg.types.Type$TypeID/DECIMAL   :decimal
    org.apache.iceberg.types.Type$TypeID/UUID      :uuid
    org.apache.iceberg.types.Type$TypeID/FIXED     :fixed
    org.apache.iceberg.types.Type$TypeID/LIST      :list
    org.apache.iceberg.types.Type$TypeID/MAP       :map
    org.apache.iceberg.types.Type$TypeID/STRUCT    :struct
    :unknown))

;;; ---------------------------------------------------------------------------
;;; Record Conversion
;;; ---------------------------------------------------------------------------

(defn- generic-record->map
  "Convert IcebergGenerics Record to Clojure map."
  [record ^Schema schema]
  (let [fields (.columns schema)]
    (into {}
          (for [^Types$NestedField field fields
                :let [name (.name field)
                      value (.getField record name)]]
            [name value]))))

;;; ---------------------------------------------------------------------------
;;; Lazy Scan Iteration
;;; ---------------------------------------------------------------------------

(defn- closeable-lazy-seq
  "Create a lazy seq from a CloseableIterable that closes when exhausted or limit reached.

   This enables early termination: if the consumer stops iterating (e.g., via take/limit),
   the scan won't continue reading. The closeable is closed when:
   - The seq is fully consumed
   - A limit is reached
   - An exception occurs during iteration

   IMPORTANT - Resource Management:
   If iteration stops before exhaustion without hitting limit (e.g., consumer abandons
   the seq via (take n ...) where n < limit), the scan remains open until GC finalizes
   the iterator. This is a known limitation of lazy seqs with external resources.

   For strict resource management, callers should either:
   1. Fully consume the seq (via doall, reduce, count, etc.)
   2. Pass a limit that will be reached
   3. Use reducers/transducers for streaming with guaranteed cleanup

   Thread Safety: Assumes single-threaded consumption. Do not share across threads.

   Limit Semantics: The limit parameter is per-scan. In multi-table joins, do NOT pass
   per-scan limits as they may drop needed rows for the join. Keep global limit
   enforcement at the join layer and treat per-scan limits as hints only."
  [^CloseableIterable closeable ^Schema schema limit]
  (let [iter (.iterator closeable)
        remaining (atom (or limit Long/MAX_VALUE))
        closed? (atom false)
        close-scan! (fn []
                      (when (compare-and-set! closed? false true)
                        (try (.close closeable)
                             (catch Exception e
                               (log/debug "Error closing scan:" (.getMessage e))))))]
    (letfn [(lazy-iter []
              (lazy-seq
               (cond
                  ;; Limit reached - close and stop
                 (<= @remaining 0)
                 (do (close-scan!) nil)

                  ;; More rows available
                 (.hasNext iter)
                 (try
                   (let [record (.next iter)
                         row-map (generic-record->map record schema)]
                     (swap! remaining dec)
                     (cons row-map (lazy-iter)))
                   (catch Exception e
                     (close-scan!)
                     (throw e)))

                  ;; No more rows - close and stop
                 :else
                 (do (close-scan!) nil))))]
      (lazy-iter))))

;;; ---------------------------------------------------------------------------
;;; IcebergSource Implementation
;;; ---------------------------------------------------------------------------

(defrecord IcebergSource [^HadoopTables tables ^Configuration conf warehouse-path]
  proto/ITabularSource

  (scan-rows [_ table-name {:keys [columns predicates snapshot-id as-of-time limit]}]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)
          schema (.schema table)
          ;; Build scan with all pushdowns
          ^CloseableIterable scan (cond-> (IcebergGenerics/read table)
                                    ;; Column projection
                                    (seq columns)
                                    (.select ^java.util.Collection (vec columns))

                                    ;; Predicate pushdown
                                    (seq predicates)
                                    (.where (predicates->expression predicates))

                                    ;; Time travel
                                    snapshot-id
                                    (.useSnapshot ^long snapshot-id)

                                    as-of-time
                                    (.asOfTime (.toEpochMilli ^Instant as-of-time))

                                    ;; Build the scan
                                    true
                                    (.build))]
      ;; Return lazy seq - closes scan when exhausted or limit reached
      (closeable-lazy-seq scan schema limit)))

  (get-schema [_ table-name {:keys [snapshot-id as-of-time]}]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)
          ;; Get schema (optionally at snapshot).
          ;; Iceberg 1.4+ uses snapshot.schemaId() with table.schemas() map.
          ^Schema schema (cond
                           snapshot-id
                           (if-let [^Snapshot snapshot (.snapshot table ^long snapshot-id)]
                             (let [schema-id (.schemaId snapshot)]
                               (.get (.schemas table) (int schema-id)))
                             (.schema table))

                           as-of-time
                           (let [snap-id (.snapshotIdAsOfTime table (.toEpochMilli ^Instant as-of-time))]
                             (if (pos? snap-id)
                               (let [^Snapshot snapshot (.snapshot table snap-id)
                                     schema-id (.schemaId snapshot)]
                                 (.get (.schemas table) (int schema-id)))
                               (.schema table)))

                           :else
                           (.schema table))
          ;; Get partition columns
          partition-spec (.spec table)
          partition-fields (set (for [field (.fields partition-spec)]
                                  (let [source-id (.sourceId field)]
                                    (.name (.findField schema source-id)))))]
      {:columns (for [^Types$NestedField field (.columns schema)]
                  {:name (.name field)
                   :type (iceberg-type->keyword (.type field))
                   :nullable? (.isOptional field)
                   :is-partition-key? (contains? partition-fields (.name field))})
       :partition-spec {:fields (for [field (.fields partition-spec)]
                                  {:source-id (.sourceId field)
                                   :name (.name field)
                                   :transform (str (.transform field))})}}))

  (get-statistics [_ table-name {:keys [snapshot-id]}]
    (let [table-path (str warehouse-path "/" table-name)
          ^Table table (.load tables table-path)
          snapshot (if snapshot-id
                     (.snapshot table ^long snapshot-id)
                     (.currentSnapshot table))]
      (when snapshot
        (let [summary (.summary snapshot)]
          {:row-count (some-> (get summary "total-records") parse-long)
           :file-count (some-> (get summary "total-data-files") parse-long)
           :added-records (some-> (get summary "added-records") parse-long)
           :snapshot-id (.snapshotId snapshot)
           :timestamp-ms (.timestampMillis snapshot)}))))

  (supported-predicates [_]
    #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or})

  proto/ICloseable
  (close [_]
    ;; Clean up Hadoop FileSystem resources
    (FileSystem/closeAll)))

;;; ---------------------------------------------------------------------------
;;; Factory Functions
;;; ---------------------------------------------------------------------------

(defn create-iceberg-source
  "Create an IcebergSource for querying Iceberg tables.

   Config:
     :warehouse-path - Root path to Iceberg warehouse (required)

   Example:
     (create-iceberg-source {:warehouse-path \"/path/to/warehouse\"})

   The warehouse-path should contain table directories. Tables are loaded
   by path: warehouse-path + \"/\" + table-name

   Note: This uses HadoopTables which is simple but has no warehouse root
   concept. For production with many tables, consider using HadoopCatalog
   or REST/Glue catalogs instead."
  [{:keys [warehouse-path]}]
  {:pre [(string? warehouse-path)]}
  (let [conf (Configuration.)
        tables (HadoopTables. conf)]
    (->IcebergSource tables conf warehouse-path)))

;;; ---------------------------------------------------------------------------
;;; FlureeIcebergSource - No Hadoop Dependencies
;;; ---------------------------------------------------------------------------

(defn- load-table-from-metadata
  "Load an Iceberg Table from a metadata location using StaticTableOperations.
   This avoids needing a full catalog - just point to the metadata JSON."
  ^Table [^FileIO file-io ^String metadata-location ^String table-name]
  (let [ops (StaticTableOperations. metadata-location file-io)
        table-id (TableIdentifier/of "fluree" table-name)]
    (BaseTable. ops table-id)))

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

  (scan-rows [_ table-name {:keys [columns predicates snapshot-id as-of-time limit metadata-location]}]
    (let [meta-loc (or metadata-location
                       (get @metadata-cache table-name)
                       (let [loc (resolve-metadata-location file-io warehouse-path table-name nil)]
                         (when loc (swap! metadata-cache assoc table-name loc))
                         loc))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name :warehouse warehouse-path})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)
          schema (.schema table)
          ;; Build scan with all pushdowns
          ^CloseableIterable scan (cond-> (IcebergGenerics/read table)
                                    ;; Column projection
                                    (seq columns)
                                    (.select ^java.util.Collection (vec columns))

                                    ;; Predicate pushdown
                                    (seq predicates)
                                    (.where (predicates->expression predicates))

                                    ;; Time travel
                                    snapshot-id
                                    (.useSnapshot ^long snapshot-id)

                                    as-of-time
                                    (.asOfTime (.toEpochMilli ^Instant as-of-time))

                                    ;; Build the scan
                                    true
                                    (.build))]
      (log/debug "FlureeIcebergSource: Scanning" table-name "from" meta-loc)
      ;; Return lazy seq - closes scan when exhausted or limit reached
      (closeable-lazy-seq scan schema limit)))

  (get-schema [_ table-name {:keys [snapshot-id as-of-time metadata-location]}]
    (let [meta-loc (or metadata-location (get @metadata-cache table-name))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)
          ^Schema schema (cond
                           snapshot-id
                           (if-let [^Snapshot snapshot (.snapshot table ^long snapshot-id)]
                             (let [schema-id (.schemaId snapshot)]
                               (.get (.schemas table) (int schema-id)))
                             (.schema table))

                           as-of-time
                           (let [snap-id (.snapshotIdAsOfTime table (.toEpochMilli ^Instant as-of-time))]
                             (if (pos? snap-id)
                               (let [^Snapshot snapshot (.snapshot table snap-id)
                                     schema-id (.schemaId snapshot)]
                                 (.get (.schemas table) (int schema-id)))
                               (.schema table)))

                           :else
                           (.schema table))
          partition-spec (.spec table)
          partition-fields (set (for [field (.fields partition-spec)]
                                  (let [source-id (.sourceId field)]
                                    (.name (.findField schema source-id)))))]
      {:columns (for [^Types$NestedField field (.columns schema)]
                  {:name (.name field)
                   :type (iceberg-type->keyword (.type field))
                   :nullable? (.isOptional field)
                   :is-partition-key? (contains? partition-fields (.name field))})
       :partition-spec {:fields (for [field (.fields partition-spec)]
                                  {:source-id (.sourceId field)
                                   :name (.name field)
                                   :transform (str (.transform field))})}}))

  (get-statistics [_ table-name {:keys [snapshot-id metadata-location]}]
    (let [meta-loc (or metadata-location (get @metadata-cache table-name))
          _ (when-not meta-loc
              (throw (ex-info (str "Cannot resolve metadata for table: " table-name)
                              {:table table-name})))
          ^Table table (load-table-from-metadata file-io meta-loc table-name)
          snapshot (if snapshot-id
                     (.snapshot table ^long snapshot-id)
                     (.currentSnapshot table))]
      (when snapshot
        (let [summary (.summary snapshot)]
          {:row-count (some-> (get summary "total-records") parse-long)
           :file-count (some-> (get summary "total-data-files") parse-long)
           :added-records (some-> (get summary "added-records") parse-long)
           :snapshot-id (.snapshotId snapshot)
           :timestamp-ms (.timestampMillis snapshot)}))))

  (supported-predicates [_]
    #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or})

  proto/ICloseable
  (close [_]
    (.close file-io)))

(defn create-fluree-iceberg-source
  "Create an IcebergSource backed by Fluree storage (no Hadoop dependencies).

   This uses StaticTableOperations to load tables from known metadata locations,
   with file I/O provided by Fluree's storage protocols.

   Config:
     :store          - Fluree storage store (required) - must implement ByteStore
     :warehouse-path - Root path prefix for tables (optional, for path resolution)

   Example:
     (create-fluree-iceberg-source {:store my-s3-store
                                    :warehouse-path \"s3://bucket/warehouse\"})

   Tables are loaded by:
   1. Direct metadata-location in scan opts
   2. Cached metadata location from previous scan
   3. Reading version-hint.text from table directory"
  [{:keys [store warehouse-path]}]
  {:pre [(some? store)]}
  (let [file-io (file-io/create-fluree-file-io store)]
    (->FlureeIcebergSource file-io (or warehouse-path "") (atom {}))))
