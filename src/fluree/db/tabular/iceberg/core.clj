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
  (:import [java.nio ByteBuffer]
           [java.time Instant]
           [org.apache.arrow.vector VectorSchemaRoot FieldVector
            BigIntVector IntVector Float4Vector Float8Vector
            VarCharVector BitVector]
           [org.apache.iceberg DataFile ManifestFile ManifestFiles PartitionField
            PartitionSpec Schema Snapshot Table TableScan]
           ;; Arrow imports for vectorized reads
           [org.apache.iceberg.arrow.vectorized ArrowReader ColumnarBatch]
           [org.apache.iceberg.data IcebergGenerics Record]
           [org.apache.iceberg.expressions Expressions Expression]
           [org.apache.iceberg.io CloseableIterable]
           [org.apache.iceberg.types Conversions Type Types$NestedField]))

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

;;; ---------------------------------------------------------------------------
;;; Columnar Predicate Evaluation (avoid row-by-row conversion overhead)
;;; ---------------------------------------------------------------------------

(declare row-matches-predicate-columnar?)

(defn- vector-matches-predicate?
  "Check if value at index in vector matches predicate.
   Operates directly on Arrow vector without conversion to map.
   For compound predicates (:and/:or), delegates to row-matches-predicate-columnar?."
  [vectors ^long idx {:keys [column op value predicates]}]
  (case op
    ;; Compound predicates - recurse
    :and (every? #(row-matches-predicate-columnar? vectors idx %) predicates)
    :or  (some #(row-matches-predicate-columnar? vectors idx %) predicates)
    ;; Simple predicates - evaluate directly on vector
    (if-let [^FieldVector vector (get vectors column)]
      (let [v (get-arrow-value vector idx)]
        (case op
          :eq      (= v value)
          :ne      (not= v value)
          :gt      (when v (> (compare v value) 0))
          :gte     (when v (>= (compare v value) 0))
          :lt      (when v (< (compare v value) 0))
          :lte     (when v (<= (compare v value) 0))
          :in      (contains? (if (set? value) value (set value)) v)
          :between (when v
                     (and (>= (compare v (first value)) 0)
                          (<= (compare v (second value)) 0)))
          :is-null (nil? v)
          :not-null (some? v)
          ;; Unknown op - pass through
          true))
      ;; Unknown column - pass through
      true)))

(defn- row-matches-predicate-columnar?
  "Check if row at index matches a single predicate using columnar evaluation."
  [vectors ^long idx pred]
  (vector-matches-predicate? vectors idx pred))

(defn- find-matching-row-indices
  "Find row indices that match all predicates using columnar evaluation.
   Returns a vector of matching indices, avoiding conversion of non-matching rows.

   This is more efficient than converting all rows to maps then filtering because:
   1. Only extracts values from columns referenced in predicates
   2. Short-circuits on first failing predicate per row
   3. Only matching rows will be fully converted to maps later"
  [^VectorSchemaRoot root predicates]
  (if (empty? predicates)
    ;; No predicates - return nil to signal 'all rows match'
    nil
    ;; Build column name -> vector map for O(1) lookup
    (let [vectors (into {}
                        (for [^FieldVector v (.getFieldVectors root)]
                          [(.getName (.getField v)) v]))
          row-count (.getRowCount root)]
      ;; For each row, check all predicates (AND semantics, short-circuit on failure)
      (persistent!
       (reduce
        (fn [matches ^long i]
          (if (every? #(row-matches-predicate-columnar? vectors i %) predicates)
            (conj! matches i)
            matches))
        (transient [])
        (range row-count))))))

(defn- extract-row-at-index
  "Extract a single row map at given index from VectorSchemaRoot.
   Only called for rows that passed predicate filtering."
  [field-vectors column-names ^long idx]
  (into {}
        (map (fn [^FieldVector v col-name]
               [col-name (get-arrow-value v idx)])
             field-vectors
             column-names)))

(defn- columnar-batch->filtered-rows
  "Convert ColumnarBatch to row maps, filtering at columnar level first.

   When predicates are provided:
   1. Evaluate predicates directly on Arrow vectors (no map boxing)
   2. Build list of matching row indices
   3. Only convert matching rows to Clojure maps

   This avoids creating map objects for filtered-out rows."
  [^ColumnarBatch batch predicates]
  (let [^VectorSchemaRoot root (.createVectorSchemaRootFromVectors batch)
        field-vectors (.getFieldVectors root)
        column-names (mapv #(.getName (.getField ^FieldVector %)) field-vectors)
        matching-indices (find-matching-row-indices root predicates)]
    (if matching-indices
      ;; Predicates present - only convert matching rows
      (map #(extract-row-at-index field-vectors column-names %) matching-indices)
      ;; No predicates - convert all rows
      (batch->row-maps root))))

(defn arrow-batch-lazy-seq
  "Create lazy seq of row maps from ArrowReader's CloseableIterator.

   Row-level filtering is applied at the columnar level before converting to maps.
   This is more efficient because:
   1. Predicates are evaluated directly on Arrow vectors
   2. Only matching rows are converted to Clojure maps
   3. Non-matching rows never allocate map objects

   IMPORTANT: Resources are closed when:
   - The seq is fully consumed
   - An exception occurs
   - The limit is reached

   If iteration stops early without hitting limit, resources may leak.
   Callers should fully consume or use with-open pattern."
  [^java.util.Iterator iter ^java.io.Closeable closeable predicates limit]
  (let [remaining (atom (or limit Long/MAX_VALUE))
        closed? (atom false)
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
                         ;; Filter at columnar level - only converts matching rows to maps
                         filtered-rows (columnar-batch->filtered-rows batch predicates)
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
  (let [^Schema schema (.schema table)
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
     :columns     - seq of column names to include (nil = all)
     :include-column-stats? - include per-column min/max/null-count (default false)

   Returns:
     {:row-count long
      :file-count long
      :added-records long
      :snapshot-id long
      :timestamp-ms long
      :column-stats {col-name {:min :max :null-count :value-count}}}  ; when include-column-stats? true"
  [^Table table {:keys [snapshot-id columns include-column-stats?]}]
  (let [snapshot (if snapshot-id
                   (.snapshot table ^long snapshot-id)
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

   Returns: lazy seq of row maps

   Resource Safety:
     If an exception occurs during scan setup, resources are cleaned up before
     re-throwing. Once the lazy seq is returned, resource cleanup is handled
     by arrow-batch-lazy-seq (closes on exhaustion, limit, or exception)."
  [^Table table {:keys [columns predicates snapshot-id as-of-time batch-size limit]
                 :or {batch-size 4096}}]
  (let [^TableScan scan (build-table-scan table {:columns columns
                                                 :predicates predicates
                                                 :snapshot-id snapshot-id
                                                 :as-of-time as-of-time})
        ^ArrowReader reader (ArrowReader. scan (int batch-size) false)]
    (try
      (let [scan-tasks (.planTasks scan)
            iter (.open reader scan-tasks)]
        (arrow-batch-lazy-seq iter reader predicates limit))
      (catch Exception e
        ;; Clean up reader if setup fails before lazy-seq takes ownership
        (try (.close reader) (catch Exception _ nil))
        (throw e)))))

;;; ---------------------------------------------------------------------------
;;; IcebergGenerics Scan (non-vectorized, for comparison)
;;; ---------------------------------------------------------------------------

(defn scan-with-generics
  "Execute an Iceberg table scan using IcebergGenerics (row-at-a-time).

   This is slower than Arrow but useful for comparison/debugging.

   Args:
     table      - Iceberg Table instance
     opts       - Scan options (same as scan-with-arrow)

   Returns: lazy seq of row maps"
  [^Table table {:keys [columns predicates limit]}]
  (let [^Schema schema (.schema table)
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
    (closeable-lazy-seq rows schema limit)))

;;; ---------------------------------------------------------------------------
;;; Supported Predicates
;;; ---------------------------------------------------------------------------

(def supported-predicate-ops
  "Set of predicate operations supported by Iceberg."
  #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or})
