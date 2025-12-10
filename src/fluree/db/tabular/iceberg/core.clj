(ns fluree.db.tabular.iceberg.core
  "Shared Iceberg utilities for predicate translation, Arrow reading, and schema handling.

   This namespace provides common functionality used by both:
   - IcebergSource (Hadoop-based, for local development)
   - FlureeIcebergSource (Fluree FileIO, for production)

   Key components:
   - Predicate translation: Convert internal predicates to Iceberg Expressions
   - Arrow reading: Vectorized batch reading with row-level filtering
   - Type mapping: Iceberg types to Clojure keywords
   - Table scanning: Build scans with projections and pushdown"
  (:require [fluree.db.util.log :as log])
  (:import [java.time Instant]
           [org.apache.iceberg PartitionField PartitionSpec Schema Snapshot Table TableScan]
           [org.apache.iceberg.data IcebergGenerics Record]
           [org.apache.iceberg.expressions Expressions Expression]
           [org.apache.iceberg.io CloseableIterable]
           [org.apache.iceberg.types Type Types$NestedField]
           ;; Arrow imports for vectorized reads
           [org.apache.iceberg.arrow.vectorized ArrowReader ColumnarBatch]
           [org.apache.arrow.vector VectorSchemaRoot FieldVector
            BigIntVector IntVector Float4Vector Float8Vector
            VarCharVector BitVector]))

(set! *warn-on-reflection* true)

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
;;; Clojure-level Row Filtering (for Arrow reads)
;;; ---------------------------------------------------------------------------
;;
;; Arrow vectorized reads don't apply row-level filtering - only file/row-group
;; pruning based on statistics. We need to apply row-level filtering in Clojure.

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
  [^Record record ^Schema schema]
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
;;; Arrow Vectorized Reading
;;; ---------------------------------------------------------------------------

(defn get-arrow-value
  "Extract typed value from Arrow FieldVector at given index.
   Returns nil for null values."
  [^FieldVector vector ^long idx]
  (when-not (.isNull vector (int idx))
    (condp instance? vector
      BigIntVector   (.get ^BigIntVector vector (int idx))
      IntVector      (.get ^IntVector vector (int idx))
      Float4Vector   (.get ^Float4Vector vector (int idx))
      Float8Vector   (.get ^Float8Vector vector (int idx))
      VarCharVector  (let [bytes (.get ^VarCharVector vector (int idx))]
                       (String. ^bytes bytes "UTF-8"))
      BitVector      (= 1 (.get ^BitVector vector (int idx)))
      ;; Fallback for other types - use getObject
      (.getObject vector (int idx)))))

(defn batch->row-maps
  "Convert Arrow VectorSchemaRoot batch to lazy seq of row maps.
   Each row map has column names as keys and typed values."
  [^VectorSchemaRoot batch]
  (let [row-count (.getRowCount batch)
        field-vectors (.getFieldVectors batch)
        column-names (mapv #(.getName (.getField ^FieldVector %)) field-vectors)]
    (for [i (range row-count)]
      (into {}
            (map (fn [^FieldVector v col-name]
                   [col-name (get-arrow-value v i)])
                 field-vectors
                 column-names)))))

(defn columnar-batch->row-maps
  "Convert Iceberg ColumnarBatch to seq of row maps.
   Uses the Arrow VectorSchemaRoot for extraction."
  [^ColumnarBatch batch]
  (let [^VectorSchemaRoot root (.createVectorSchemaRootFromVectors batch)]
    (batch->row-maps root)))

(defn arrow-batch-lazy-seq
  "Create lazy seq of row maps from ArrowReader's CloseableIterator.

   Row-level filtering is applied here since Arrow reads only do file/row-group
   pruning based on statistics. The predicates parameter enables Clojure-level
   filtering of individual rows.

   IMPORTANT: Resources are closed when:
   - The seq is fully consumed
   - An exception occurs
   - The limit is reached

   If iteration stops early without hitting limit, resources may leak.
   Callers should fully consume or use with-open pattern."
  [^java.util.Iterator iter ^java.io.Closeable closeable predicates limit]
  (let [remaining (atom (or limit Long/MAX_VALUE))
        closed? (atom false)
        row-filter (if (seq predicates)
                     (partial row-matches-predicates? predicates)
                     identity)
        close-all! (fn []
                     (when (compare-and-set! closed? false true)
                       (try
                         (.close closeable)
                         (catch Exception e
                           (log/debug "Error closing ArrowReader:" (.getMessage e))))))]
    (letfn [(batch-seq []
              (lazy-seq
               (cond
                 ;; Limit reached
                 (<= @remaining 0)
                 (do (close-all!) nil)

                 ;; Try to get next batch
                 (.hasNext iter)
                 (try
                   (let [^ColumnarBatch batch (.next iter)
                         all-rows (columnar-batch->row-maps batch)
                         ;; Apply row-level filtering (Arrow only does file/row-group pruning)
                         filtered-rows (if (seq predicates)
                                         (filter row-filter all-rows)
                                         all-rows)
                         limit-remaining @remaining
                         rows-to-take (take limit-remaining filtered-rows)
                         num-taken (count rows-to-take)]
                     (swap! remaining - num-taken)
                     ;; Return filtered rows within limit, then continue with next batch
                     (concat rows-to-take (batch-seq)))
                   (catch Exception e
                     (close-all!)
                     (throw e)))

                 ;; No more batches
                 :else
                 (do (close-all!) nil))))]
      (batch-seq))))

;;; ---------------------------------------------------------------------------
;;; Table Scan Building
;;; ---------------------------------------------------------------------------

(defn build-table-scan
  "Build Iceberg TableScan with projection and predicate pushdown."
  ^TableScan [^Table table {:keys [columns predicates snapshot-id as-of-time]}]
  (cond-> (.newScan table)
    ;; Time travel
    snapshot-id
    (.useSnapshot ^long snapshot-id)

    as-of-time
    (.asOfTime (.toEpochMilli ^Instant as-of-time))

    ;; Column projection
    (seq columns)
    (.select ^java.util.Collection (vec columns))

    ;; Predicate pushdown
    (seq predicates)
    (-> ^TableScan (.filter (predicates->expression predicates)))))

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
  (let [^Schema schema (cond
                         snapshot-id
                         (if-let [^Snapshot snapshot (.snapshot table ^long snapshot-id)]
                           (let [schema-id (.schemaId snapshot)]
                             (.get (.schemas table) (int schema-id)))
                           (.schema table))

                         as-of-time
                         (let [snap-id (long (.snapshotIdAsOfTime table (.toEpochMilli ^Instant as-of-time)))]
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

(defn extract-statistics
  "Extract statistics from an Iceberg Table snapshot.

   Options:
     :snapshot-id - specific snapshot ID (nil = current)

   Returns:
     {:row-count :file-count :added-records :snapshot-id :timestamp-ms}"
  [^Table table {:keys [snapshot-id]}]
  (let [snapshot (if snapshot-id
                   (.snapshot table ^long snapshot-id)
                   (.currentSnapshot table))]
    (when snapshot
      (let [summary (.summary snapshot)]
        {:row-count (some-> (get summary "total-records") parse-long)
         :file-count (some-> (get summary "total-data-files") parse-long)
         :added-records (some-> (get summary "added-records") parse-long)
         :snapshot-id (.snapshotId snapshot)
         :timestamp-ms (.timestampMillis snapshot)}))))

;;; ---------------------------------------------------------------------------
;;; Arrow Scan Execution
;;; ---------------------------------------------------------------------------

(defn scan-with-arrow
  "Execute an Iceberg table scan using Arrow vectorized reads.

   Args:
     table      - Iceberg Table instance
     opts       - Scan options:
       :columns     - seq of column names to project
       :predicates  - seq of predicate maps for pushdown
       :snapshot-id - specific snapshot for time travel
       :as-of-time  - Instant for time travel
       :batch-size  - rows per Arrow batch (default 4096)
       :limit       - max rows to return

   Returns: lazy seq of row maps"
  [^Table table {:keys [columns predicates snapshot-id as-of-time batch-size limit]
                 :or {batch-size 4096}}]
  (let [^TableScan scan (build-table-scan table {:columns columns
                                                 :predicates predicates
                                                 :snapshot-id snapshot-id
                                                 :as-of-time as-of-time})
        ^ArrowReader reader (ArrowReader. scan (int batch-size) false)
        scan-tasks (.planTasks scan)
        iter (.open reader scan-tasks)]
    (arrow-batch-lazy-seq iter reader predicates limit)))

;;; ---------------------------------------------------------------------------
;;; Supported Predicates
;;; ---------------------------------------------------------------------------

(def supported-predicate-ops
  "Set of predicate operations supported by Iceberg."
  #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or})
