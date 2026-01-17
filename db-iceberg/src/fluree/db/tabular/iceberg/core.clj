(ns fluree.db.tabular.iceberg.core
  "Shared Iceberg utilities for predicate translation, row-based reading, and schema handling.

   This namespace provides row-based functionality used by Iceberg sources.
   Arrow vectorized reading is provided by fluree.db.tabular.iceberg.arrow
   in the db-iceberg-arrow module.

   Key components:
   - Table identifiers: Canonical format and conversion utilities
   - Predicate translation: Convert internal predicates to Iceberg Expressions
   - Type mapping: Iceberg types to Clojure keywords
   - Table scanning: Build scans with projections and pushdown
   - Row-based reads: Using IcebergGenerics"
  (:require [clojure.string :as str]
            [fluree.db.util.log :as log])
  (:import [java.nio ByteBuffer]
           [java.time Instant]
           [org.apache.iceberg DataFile ManifestFile ManifestFiles PartitionField
            PartitionSpec Snapshot Table TableScan]
           [org.apache.iceberg.data IcebergGenerics Record]
           [org.apache.iceberg.expressions Expressions Expression]
           [org.apache.iceberg.io CloseableIterable]
           [org.apache.iceberg.types Conversions Type Types$NestedField]
           [org.apache.iceberg.util SnapshotUtil]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Table Identifier Utilities
;;; ---------------------------------------------------------------------------
;;
;; Canonical Format: "namespace.table" (e.g., "openflights.airlines")
;;
;; Different catalog types use different formats:
;; - REST catalogs:  "namespace.table" (canonical)
;; - Hadoop paths:   "namespace/table" (slash-separated)
;; - Multi-level:    "db.schema.table" -> ["db" "schema"] namespace + "table"
;;
;; These utilities ensure consistent handling across all catalog types.

(defn parse-table-identifier
  "Parse a table identifier into namespace and table components.

   Supports multiple formats:
   - Canonical (dot): 'namespace.table' or 'ns1.ns2.table'
   - Path (slash):    'namespace/table' or 'ns1/ns2/table'

   Returns: {:namespace 'ns1.ns2' :table 'table'}

   The namespace is always returned in dot-separated format (canonical)."
  [table-id]
  (cond
    ;; Slash-separated (path format)
    (str/includes? table-id "/")
    (let [parts (str/split table-id #"/")
          namespace (str/join "." (butlast parts))
          table (last parts)]
      {:namespace namespace :table table})

    ;; Dot-separated (canonical format)
    (str/includes? table-id ".")
    (let [last-dot (str/last-index-of table-id ".")
          namespace (subs table-id 0 last-dot)
          table (subs table-id (inc last-dot))]
      {:namespace namespace :table table})

    ;; No separator - just a table name, no namespace
    :else
    {:namespace nil :table table-id}))

(defn canonical-table-id
  "Convert a table identifier to canonical format (namespace.table).

   Handles:
   - Already canonical: 'ns.table' -> 'ns.table'
   - Path format:       'ns/table' -> 'ns.table'
   - Multi-level:       'db/schema/table' -> 'db.schema.table'"
  [table-id]
  (if (str/includes? table-id "/")
    (str/replace table-id "/" ".")
    table-id))

(defn table-id->path
  "Convert a table identifier to path format (namespace/table).

   Used for Hadoop-based catalogs that expect path-separated identifiers.

   Examples:
   - 'ns.table' -> 'ns/table'
   - 'db.schema.table' -> 'db/schema/table'"
  [table-id]
  (str/replace table-id "." "/"))

(defn table-id->rest-path
  "Convert a table identifier to REST API path format.

   REST catalogs use URL-encoded paths with unit separator (\\u001F) for
   multi-level namespaces.

   Returns: {:namespace-path 'encoded-ns' :table 'table'}

   Example:
   - 'openflights.airlines' -> {:namespace-path 'openflights' :table 'airlines'}
   - 'db.schema.table' -> {:namespace-path 'db%1Fschema' :table 'table'}"
  [table-id]
  (let [{:keys [namespace table]} (parse-table-identifier table-id)]
    (when namespace
      {:namespace-path (-> namespace
                           (str/replace "." "\u001F")
                           (java.net.URLEncoder/encode "UTF-8"))
       :table table})))

(defn namespace-levels
  "Split a namespace into its component levels.

   Examples:
   - 'openflights' -> ['openflights']
   - 'db.schema' -> ['db' 'schema']"
  [namespace-str]
  (when namespace-str
    (str/split namespace-str #"\.")))

(defn join-table-id
  "Join namespace and table into canonical table identifier.

   Examples:
   - 'openflights' 'airlines' -> 'openflights.airlines'
   - ['db' 'schema'] 'table' -> 'db.schema.table'"
  [namespace table]
  (let [ns-str (if (sequential? namespace)
                 (str/join "." namespace)
                 namespace)]
    (if ns-str
      (str ns-str "." table)
      table)))

;;; ---------------------------------------------------------------------------
;;; Predicate Translation
;;; ---------------------------------------------------------------------------

(defn predicate->iceberg-expr
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

(defn predicates->expression
  "Combine multiple predicates with AND."
  ^Expression [predicates]
  (if (seq predicates)
    (reduce (fn [^Expression a ^Expression b] (Expressions/and a b))
            (map predicate->iceberg-expr predicates))
    (Expressions/alwaysTrue)))

;;; ---------------------------------------------------------------------------
;;; Clojure-level Row Filtering
;;; ---------------------------------------------------------------------------

(declare predicate-matches?)

(defn predicate-matches?
  "Check if a row map matches a predicate."
  [row {:keys [column op value predicates]}]
  (case op
    :eq        (= (get row column) value)
    :ne        (not= (get row column) value)
    :gt        (when-let [v (get row column)] (> (compare v value) 0))
    :gte       (when-let [v (get row column)] (>= (compare v value) 0))
    :lt        (when-let [v (get row column)] (< (compare v value) 0))
    :lte       (when-let [v (get row column)] (<= (compare v value) 0))
    :in        (contains? (set value) (get row column))
    :between   (when-let [v (get row column)]
                 (and (>= (compare v (first value)) 0)
                      (<= (compare v (second value)) 0)))
    :is-null   (nil? (get row column))
    :not-null  (some? (get row column))
    :and       (every? #(predicate-matches? row %) predicates)
    :or        (some #(predicate-matches? row %) predicates)
    ;; Unknown op - pass through
    true))

(defn row-matches-predicates?
  "Check if a row matches all predicates (AND semantics)."
  [predicates row]
  (if (seq predicates)
    (every? #(predicate-matches? row %) predicates)
    true))

;;; ---------------------------------------------------------------------------
;;; Type Mapping
;;; ---------------------------------------------------------------------------

(defn iceberg-type->keyword
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
;;; Record Conversion (for IcebergGenerics)
;;; ---------------------------------------------------------------------------

(defn generic-record->map
  "Convert IcebergGenerics Record to Clojure map."
  [^Record record ^org.apache.iceberg.Schema schema]
  (let [fields (.columns schema)]
    (into {}
          (for [^Types$NestedField field fields
                :let [name (.name field)
                      value (.getField record name)]]
            [name value]))))

;;; ---------------------------------------------------------------------------
;;; Lazy Scan Iteration (for IcebergGenerics)
;;; ---------------------------------------------------------------------------

(defn closeable-lazy-seq
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
  [^CloseableIterable closeable ^org.apache.iceberg.Schema schema limit]
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
;;; Table Scan Building
;;; ---------------------------------------------------------------------------

(defn build-table-scan
  "Build Iceberg TableScan with projection and predicate pushdown."
  ^TableScan [^Table table {:keys [columns predicates snapshot-id as-of-time]}]
  (let [scan ^TableScan (.newScan table)
        scan (if snapshot-id
               (.useSnapshot scan ^long snapshot-id)
               scan)
        scan (if as-of-time
               (.asOfTime scan (.toEpochMilli ^Instant as-of-time))
               scan)
        scan (if (seq columns)
               (.select scan ^java.util.Collection (vec columns))
               scan)
        scan (if (seq predicates)
               (.filter ^TableScan scan ^Expression (predicates->expression predicates))
               scan)]
    scan))

;;; ---------------------------------------------------------------------------
;;; Schema Extraction
;;; ---------------------------------------------------------------------------

(defn extract-schema
  "Extract schema information from an Iceberg Table.

   Options:
     :snapshot-id - specific snapshot ID
     :as-of-time  - java.time.Instant for time travel

   Returns:
     {:columns [{:name :type :nullable? :is-partition-key?}]
      :partition-spec {:fields [...]}}"
  [^Table table {:keys [snapshot-id as-of-time]}]
  (let [^org.apache.iceberg.Schema schema (cond
                                            snapshot-id
                                            (if-let [^Snapshot snapshot (.snapshot table ^long snapshot-id)]
                                              (let [schema-id (.schemaId snapshot)]
                                                (.get (.schemas table) (int schema-id)))
                                              (.schema table))

                                            as-of-time
                                            (let [snap-id (SnapshotUtil/snapshotIdAsOfTime table (.toEpochMilli ^Instant as-of-time))]
                                              (if (pos? snap-id)
                                                (let [^Snapshot snapshot (.snapshot table snap-id)
                                                      schema-id (.schemaId snapshot)]
                                                  (.get (.schemas table) (int schema-id)))
                                                (.schema table)))

                                            :else
                                            (.schema table))
        ^PartitionSpec partition-spec (.spec table)
        partition-fields (set (for [^PartitionField field (.fields partition-spec)]
                                (let [source-id (.sourceId field)]
                                  (.name (.findField schema (int source-id))))))]
    {:columns (for [^Types$NestedField field (.columns schema)]
                {:name (.name field)
                 :type (iceberg-type->keyword (.type field))
                 :nullable? (.isOptional field)
                 :is-partition-key? (contains? partition-fields (.name field))})
     :partition-spec {:fields (for [^PartitionField field (.fields partition-spec)]
                                {:source-id (.sourceId field)
                                 :name (.name field)
                                 :transform (str (.transform field))})}}))

;;; ---------------------------------------------------------------------------
;;; Statistics Extraction
;;; ---------------------------------------------------------------------------

(defn- decode-bound-value
  "Decode a ByteBuffer bound value to a Clojure value using the field type."
  [^ByteBuffer buf ^Type field-type]
  (when buf
    (try
      (Conversions/fromByteBuffer field-type (.duplicate buf))
      (catch Exception _
        nil))))

(defn- aggregate-column-stats
  "Aggregate column statistics from all data files in a snapshot.

   Returns a map of column-name -> {:min :max :null-count :value-count}"
  [^Table table ^Snapshot snapshot]
  (let [^org.apache.iceberg.Schema schema (.schema table)
        file-io (.io table)
        ;; Build field-id -> field map for type lookups
        field-by-id (into {}
                          (for [^Types$NestedField field (.columns schema)]
                            [(.fieldId field) field]))
        ;; Accumulator: field-id -> {:min :max :null-count :value-count}
        stats-acc (atom {})]
    ;; Read all manifest files
    (doseq [^ManifestFile manifest (.dataManifests snapshot file-io)]
      (with-open [^CloseableIterable reader (ManifestFiles/read manifest file-io)]
        (doseq [^DataFile data-file reader]
          (let [lower-bounds (.lowerBounds data-file)
                upper-bounds (.upperBounds data-file)
                null-counts  (.nullValueCounts data-file)
                value-counts (.valueCounts data-file)]
            ;; Process each column's stats
            (doseq [[^Integer field-id ^Types$NestedField field] field-by-id
                    :let [field-type (.type field)
                          col-name (.name field)]]
              (let [existing (get @stats-acc field-id)
                    lower-buf (when lower-bounds (.get lower-bounds field-id))
                    upper-buf (when upper-bounds (.get upper-bounds field-id))
                    lower-val (decode-bound-value lower-buf field-type)
                    upper-val (decode-bound-value upper-buf field-type)
                    null-cnt  (when null-counts (or (.get null-counts field-id) 0))
                    val-cnt   (when value-counts (or (.get value-counts field-id) 0))]
                (swap! stats-acc assoc field-id
                       {:name col-name
                        :min (if (and lower-val (:min existing))
                               (if (neg? (compare lower-val (:min existing)))
                                 lower-val
                                 (:min existing))
                               (or lower-val (:min existing)))
                        :max (if (and upper-val (:max existing))
                               (if (pos? (compare upper-val (:max existing)))
                                 upper-val
                                 (:max existing))
                               (or upper-val (:max existing)))
                        :null-count (+ (or null-cnt 0) (or (:null-count existing) 0))
                        :value-count (+ (or val-cnt 0) (or (:value-count existing) 0))})))))))
    ;; Convert to column-name keyed map
    (into {}
          (for [[_ stats] @stats-acc]
            [(:name stats) (dissoc stats :name)]))))

(defn extract-statistics
  "Extract statistics from an Iceberg Table snapshot.

   Options:
     :snapshot-id - specific snapshot ID (nil = current)
     :as-of-time  - java.time.Instant for time travel (nil = current)
     :columns     - seq of column names to include (nil = all)
     :include-column-stats? - include per-column min/max/null-count (default false)

   Returns:
     {:row-count long
      :file-count long
      :added-records long
      :snapshot-id long
      :timestamp-ms long
      :column-stats {col-name {:min :max :null-count :value-count}}}  ; when include-column-stats? true"
  [^Table table {:keys [snapshot-id as-of-time columns include-column-stats?]}]
  (let [snapshot-id* (cond
                       snapshot-id
                       snapshot-id

                       as-of-time
                       (let [sid (SnapshotUtil/snapshotIdAsOfTime table (.toEpochMilli ^Instant as-of-time))]
                         (when (pos? sid) sid))

                       :else
                       nil)
        snapshot (if snapshot-id*
                   (.snapshot table ^long snapshot-id*)
                   (.currentSnapshot table))]
    (when snapshot
      (let [summary (.summary snapshot)
            base-stats {:row-count (some-> (get summary "total-records") parse-long)
                        :file-count (some-> (get summary "total-data-files") parse-long)
                        :added-records (some-> (get summary "added-records") parse-long)
                        :snapshot-id (.snapshotId snapshot)
                        :timestamp-ms (.timestampMillis snapshot)}]
        (if include-column-stats?
          (let [all-col-stats (aggregate-column-stats table snapshot)
                col-stats (if columns
                            (select-keys all-col-stats columns)
                            all-col-stats)]
            (assoc base-stats :column-stats col-stats))
          base-stats)))))

;;; ---------------------------------------------------------------------------
;;; IcebergGenerics Scan (row-based)
;;; ---------------------------------------------------------------------------

(defn scan-with-generics
  "Execute an Iceberg table scan using IcebergGenerics (row-at-a-time).

   This is the primary scan method for db-iceberg. Arrow vectorized reads
   require the db-iceberg-arrow module.

   Args:
     table      - Iceberg Table instance
     opts       - Scan options:
       :columns     - seq of column names to project
       :predicates  - seq of predicate maps for pushdown
       :limit       - max rows to return

   Returns: lazy seq of row maps

   Resource Safety:
     If an exception occurs during scan setup, resources are cleaned up before
     re-throwing. Once the lazy seq is returned, resource cleanup is handled
     by closeable-lazy-seq (closes on exhaustion, limit, or exception)."
  [^Table table {:keys [columns predicates limit]}]
  (let [^org.apache.iceberg.Schema schema (.schema table)
        builder (IcebergGenerics/read table)
        ;; Apply column projection
        builder (if (seq columns)
                  (.select builder ^"[Ljava.lang.String;" (into-array String columns))
                  builder)
        ;; Apply predicate filter
        builder (if (seq predicates)
                  (.where builder (predicates->expression predicates))
                  builder)
        ^CloseableIterable rows (.build builder)]
    (try
      (closeable-lazy-seq rows schema limit)
      (catch Exception e
        ;; Clean up if setup fails before lazy-seq takes ownership
        (try (.close rows) (catch Exception _ nil))
        (throw e)))))

;;; ---------------------------------------------------------------------------
;;; Supported Predicates
;;; ---------------------------------------------------------------------------

(def supported-predicate-ops
  "Set of predicate operations supported by Iceberg."
  #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or})
