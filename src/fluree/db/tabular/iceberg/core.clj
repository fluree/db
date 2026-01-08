(ns fluree.db.tabular.iceberg.core
  "Shared Iceberg utilities for predicate translation, Arrow reading, and schema handling.

   This namespace provides common functionality used by both:
   - IcebergSource (Hadoop-based, for local development)
   - FlureeIcebergSource (Fluree FileIO, for production)

   Key components:
   - Table identifiers: Canonical format and conversion utilities
   - Predicate translation: Convert internal predicates to Iceberg Expressions
   - Arrow reading: Vectorized batch reading with row-level filtering
   - Type mapping: Iceberg types to Clojure keywords
   - Table scanning: Build scans with projections and pushdown"
  (:require [clojure.string :as str]
            [fluree.db.util.log :as log])
  (:import [java.nio ByteBuffer]
           [java.time Instant]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot FieldVector
            BigIntVector IntVector Float4Vector Float8Vector
            VarCharVector VarBinaryVector BitVector DateDayVector
            TimeStampMicroTZVector TimeStampMicroVector DecimalVector]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [org.apache.iceberg DataFile ManifestFile ManifestFiles PartitionField
            PartitionSpec Snapshot Table TableScan]
           ;; Arrow imports for vectorized reads
           [org.apache.iceberg.arrow.vectorized ArrowReader ColumnarBatch]
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
          :in      (contains? value v)  ;; value should be pre-normalized to set
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

(defn- normalize-predicate
  "Pre-normalize a predicate for efficient evaluation.
   - :in values become sets (O(1) lookup vs rebuilding per row)
   - :between values become vectors
   - :and/:or predicates recurse to normalize children"
  [pred]
  (let [{:keys [op value predicates]} pred]
    (case op
      :in (assoc pred :value (if (set? value) value (set value)))
      :between (assoc pred :value (vec value))
      :and (assoc pred :predicates (mapv normalize-predicate predicates))
      :or (assoc pred :predicates (mapv normalize-predicate predicates))
      ;; Other ops pass through unchanged
      pred)))

(defn- normalize-predicates
  "Normalize all predicates once before filtering.
   This avoids repeated allocations during per-row evaluation."
  [predicates]
  (mapv normalize-predicate predicates))

(defn- find-matching-row-indices
  "Find row indices that match all predicates using columnar evaluation.
   Returns a vector of matching indices, avoiding conversion of non-matching rows.

   This is more efficient than converting all rows to maps then filtering because:
   1. Only extracts values from columns referenced in predicates
   2. Short-circuits on first failing predicate per row
   3. Only matching rows will be fully converted to maps later

   Predicates are normalized once (e.g., :in values -> sets) before evaluation."
  [^VectorSchemaRoot root predicates]
  (if (empty? predicates)
    ;; No predicates - return nil to signal 'all rows match'
    nil
    ;; Build column name -> vector map for O(1) lookup
    (let [vectors (into {}
                        (for [^FieldVector v (.getFieldVectors root)]
                          [(.getName (.getField v)) v]))
          row-count (.getRowCount root)
          ;; Normalize predicates once (convert :in to sets, etc.)
          normalized-preds (normalize-predicates predicates)]
      ;; For each row, check all predicates (AND semantics, short-circuit on failure)
      (persistent!
       (reduce
        (fn [matches ^long i]
          (if (every? #(row-matches-predicate-columnar? vectors i %) normalized-preds)
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

;;; ---------------------------------------------------------------------------
;;; Filtered Arrow Batch Creation (for true columnar execution)
;;; ---------------------------------------------------------------------------
;;
;; These functions support Phase 3b columnar execution by creating new
;; VectorSchemaRoot batches containing only filtered rows. The data is
;; copied to avoid buffer reuse issues from the underlying ColumnarBatch.

(def ^:private ^BufferAllocator shared-allocator
  "Shared Arrow allocator for creating filtered batches.
   Uses a RootAllocator with default settings."
  (delay (RootAllocator.)))

(defn- copy-vector-value!
  "Copy a single value from source vector at src-idx to dest vector at dest-idx.
   Handles null values correctly. Uses setSafe for variable-length vectors
   to handle automatic buffer expansion."
  [^FieldVector src ^long src-idx ^FieldVector dest ^long dest-idx]
  (if (.isNull src (int src-idx))
    ;; Set null in destination
    (.setNull dest (int dest-idx))
    ;; Copy non-null value based on vector type
    (condp instance? src
      BigIntVector
      (.set ^BigIntVector dest (int dest-idx)
            (.get ^BigIntVector src (int src-idx)))

      IntVector
      (.set ^IntVector dest (int dest-idx)
            (.get ^IntVector src (int src-idx)))

      Float4Vector
      (.set ^Float4Vector dest (int dest-idx)
            (.get ^Float4Vector src (int src-idx)))

      Float8Vector
      (.set ^Float8Vector dest (int dest-idx)
            (.get ^Float8Vector src (int src-idx)))

      VarCharVector
      ;; Use setSafe for variable-length vectors to handle auto buffer expansion
      (let [bytes (.get ^VarCharVector src (int src-idx))]
        (.setSafe ^VarCharVector dest (int dest-idx) ^bytes bytes))

      BitVector
      (.set ^BitVector dest (int dest-idx)
            (.get ^BitVector src (int src-idx)))

      DateDayVector
      (.set ^DateDayVector dest (int dest-idx)
            (.get ^DateDayVector src (int src-idx)))

      TimeStampMicroTZVector
      (.set ^TimeStampMicroTZVector dest (int dest-idx)
            (.get ^TimeStampMicroTZVector src (int src-idx)))

      TimeStampMicroVector
      (.set ^TimeStampMicroVector dest (int dest-idx)
            (.get ^TimeStampMicroVector src (int src-idx)))

      DecimalVector
      (.setSafe ^DecimalVector dest (int dest-idx)
                (.getObject ^DecimalVector src (int src-idx)))

      VarBinaryVector
      (let [bytes (.get ^VarBinaryVector src (int src-idx))]
        (.setSafe ^VarBinaryVector dest (int dest-idx) ^bytes bytes))

      ;; Fallback: throw with informative message about unsupported type
      (throw (ex-info "Unsupported Arrow vector type for copy operation"
                      {:vector-type (type src)
                       :field-name (.getName (.getField ^FieldVector src))})))))

(defn- allocate-vector!
  "Allocate space in a destination vector. Uses type-specific allocation
   to properly handle fixed-width vs variable-width vectors."
  [^FieldVector dest num-rows]
  (condp instance? dest
    ;; Fixed-width vectors use allocateNew(valueCount)
    BigIntVector (.allocateNew ^BigIntVector dest (int num-rows))
    IntVector (.allocateNew ^IntVector dest (int num-rows))
    Float4Vector (.allocateNew ^Float4Vector dest (int num-rows))
    Float8Vector (.allocateNew ^Float8Vector dest (int num-rows))
    BitVector (.allocateNew ^BitVector dest (int num-rows))
    DateDayVector (.allocateNew ^DateDayVector dest (int num-rows))
    TimeStampMicroTZVector (.allocateNew ^TimeStampMicroTZVector dest (int num-rows))
    TimeStampMicroVector (.allocateNew ^TimeStampMicroVector dest (int num-rows))
    DecimalVector (.allocateNew ^DecimalVector dest (int num-rows))
    ;; Variable-width: estimate 32 bytes average per value, let setSafe grow if needed
    VarCharVector (.allocateNew ^VarCharVector dest (* 32 num-rows) (int num-rows))
    VarBinaryVector (.allocateNew ^VarBinaryVector dest (* 32 num-rows) (int num-rows))
    ;; Fallback: use setInitialCapacity and allocateNew (for any other vector types)
    (do
      (.setInitialCapacity dest (int num-rows))
      (.allocateNew dest))))

(defn- create-vector-copy
  "Create a new vector of the same type with values at specified indices copied.
   Uses the shared allocator for memory allocation."
  [^FieldVector src-vector indices ^BufferAllocator allocator]
  (let [^Field field (.getField src-vector)
        ^FieldVector dest-vector (.createVector field allocator)
        num-rows (count indices)]
    ;; Use type-specific allocation
    (allocate-vector! dest-vector num-rows)
    ;; Copy values at specified indices
    (doseq [[dest-idx src-idx] (map-indexed vector indices)]
      (copy-vector-value! src-vector src-idx dest-vector dest-idx))
    ;; Set the value count
    (.setValueCount dest-vector num-rows)
    dest-vector))

(defn create-filtered-arrow-batch
  "Create a new VectorSchemaRoot containing only rows at specified indices.

   This function copies data from the source batch to a new batch, avoiding
   buffer reuse issues. The returned batch owns its data and is safe to hold
   beyond the lifetime of the source batch.

   Args:
     source-batch - VectorSchemaRoot to filter
     indices      - Vector of row indices to include (nil = all rows)

   Returns:
     New VectorSchemaRoot with copied data for specified rows.
     Caller is responsible for closing this batch when done."
  [^VectorSchemaRoot source-batch indices]
  (if (nil? indices)
    ;; No filtering - copy all rows
    (let [allocator @shared-allocator
          field-vectors (.getFieldVectors source-batch)
          all-indices (vec (range (.getRowCount source-batch)))
          new-vectors (mapv #(create-vector-copy % all-indices allocator) field-vectors)
          ^VectorSchemaRoot root (VectorSchemaRoot. ^java.util.List new-vectors)]
      ;; Explicitly set row count to ensure it's correct
      (.setRowCount root (count all-indices))
      root)
    ;; Copy only specified indices
    (let [allocator @shared-allocator
          field-vectors (.getFieldVectors source-batch)
          new-vectors (mapv #(create-vector-copy % indices allocator) field-vectors)
          ^VectorSchemaRoot root (VectorSchemaRoot. ^java.util.List new-vectors)]
      ;; Explicitly set row count to ensure it's correct
      (.setRowCount root (count indices))
      root)))

(defn filter-arrow-batch
  "Apply predicates to an Arrow batch and return a filtered copy.

   Uses vectorized predicate evaluation to find matching rows, then
   copies only those rows to a new batch.

   Args:
     batch       - VectorSchemaRoot to filter
     predicates  - Seq of predicate maps for filtering
     copy-batch? - If true (default), copy data to new batch.
                   If false and no predicates, return original batch.
                   WARNING: When false, batch is only valid until iterator advances.

   Returns:
     VectorSchemaRoot with matching rows.
     Returns nil if no rows match.
     Caller is responsible for closing the returned batch (if copied)."
  ([^VectorSchemaRoot batch predicates]
   (filter-arrow-batch batch predicates true))
  ([^VectorSchemaRoot batch predicates copy-batch?]
   (let [matching-indices (find-matching-row-indices batch predicates)]
     (cond
       ;; No predicates - return all rows
       (nil? matching-indices)
       (if copy-batch?
         (create-filtered-arrow-batch batch nil)
         batch)  ;; Return original (caller must consume before next iteration)

       ;; No matching rows
       (empty? matching-indices)
       nil

       ;; Create filtered batch with matching rows (always copy when filtering)
       :else
       (create-filtered-arrow-batch batch matching-indices)))))

(defn arrow-filtered-batch-lazy-seq
  "Create lazy seq of filtered Arrow VectorSchemaRoot from ArrowReader iterator.

   Each batch has predicates applied via vectorized evaluation, and only
   matching rows are copied to a new batch. The returned batches own their
   data and are safe to hold.

   Args:
     iter        - CloseableIterator of ColumnarBatch
     closeable   - Resource to close when done
     predicates  - Predicates for filtering
     copy-batch? - If true, copy batches for safety. If false and no predicates,
                   return raw batches (only valid until next iteration).

   IMPORTANT: Resources are closed when:
   - The seq is fully consumed
   - An exception occurs

   Callers should fully consume or use doall to realize."
  ([^java.util.Iterator iter ^java.io.Closeable closeable predicates]
   (arrow-filtered-batch-lazy-seq iter closeable predicates true))
  ([^java.util.Iterator iter ^java.io.Closeable closeable predicates copy-batch?]
   (let [closed? (atom false)
         close-all! (fn []
                      (when (compare-and-set! closed? false true)
                        (try
                          (.close closeable)
                          (catch Exception e
                            (log/debug "Error closing ArrowReader:" (.getMessage e))))))]
     (letfn [(batch-seq []
               (lazy-seq
                (if (.hasNext iter)
                  (try
                    (let [^ColumnarBatch batch (.next iter)
                          ^VectorSchemaRoot root (.createVectorSchemaRootFromVectors batch)
                          filtered-batch (filter-arrow-batch root predicates copy-batch?)]
                      (if filtered-batch
                        (cons filtered-batch (batch-seq))
                        ;; No matching rows in this batch, continue to next
                        (batch-seq)))
                    (catch Exception e
                      (close-all!)
                      (throw e)))
                  (do (close-all!) nil))))]
       (batch-seq)))))

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

   Returns: lazy seq of row maps

   Resource Safety:
     If an exception occurs during scan setup, resources are cleaned up before
     re-throwing. Once the lazy seq is returned, resource cleanup is handled
     by closeable-lazy-seq (closes on exhaustion, limit, or exception)."
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
    (try
      (closeable-lazy-seq rows schema limit)
      (catch Exception e
        ;; Clean up if setup fails before lazy-seq takes ownership
        (try (.close rows) (catch Exception _ nil))
        (throw e)))))

;;; ---------------------------------------------------------------------------
;;; Safe Reducible Scan (guaranteed resource cleanup)
;;; ---------------------------------------------------------------------------

(defn reduce-arrow-scan
  "Execute an Iceberg table scan with guaranteed resource cleanup.

   Unlike scan-with-arrow which returns a lazy seq, this function uses
   reduce semantics for guaranteed cleanup. The ArrowReader is always
   closed, even if the reduction is short-circuited via `reduced`.

   Args:
     table      - Iceberg Table instance
     opts       - Scan options (same as scan-with-arrow)
     f          - Reducing function (fn [acc row-map] ...)
     init       - Initial accumulator value

   Returns: Final accumulated value

   Example:
     ;; Count rows
     (reduce-arrow-scan table {} (fn [n _] (inc n)) 0)

     ;; Collect first 10 rows
     (reduce-arrow-scan table {:limit 10}
       (fn [acc row] (conj acc row)) [])"
  [^Table table opts f init]
  (let [{:keys [columns predicates snapshot-id as-of-time batch-size limit]
         :or {batch-size 4096}} opts
        ^TableScan scan (build-table-scan table {:columns columns
                                                 :predicates predicates
                                                 :snapshot-id snapshot-id
                                                 :as-of-time as-of-time})
        ^ArrowReader reader (ArrowReader. scan (int batch-size) false)]
    (try
      (let [scan-tasks (.planTasks scan)
            iter (.open reader scan-tasks)
            remaining (atom (or limit Long/MAX_VALUE))]
        (loop [acc init]
          (cond
            ;; Limit reached
            (<= @remaining 0) acc

            ;; More batches available
            (.hasNext iter)
            (let [^ColumnarBatch batch (.next iter)
                  filtered-rows (columnar-batch->filtered-rows batch predicates)
                  limit-remaining @remaining
                  result (reduce
                          (fn [acc' row]
                            (if (<= @remaining 0)
                              (reduced acc')
                              (do
                                (swap! remaining dec)
                                (let [res (f acc' row)]
                                  (if (reduced? res)
                                    res
                                    res)))))
                          acc
                          (take limit-remaining filtered-rows))]
              (if (reduced? result)
                @result
                (recur result)))

            ;; No more batches
            :else acc)))
      (finally
        (.close reader)))))

;;; ---------------------------------------------------------------------------
;;; Supported Predicates
;;; ---------------------------------------------------------------------------

;;; ---------------------------------------------------------------------------
;;; Raw Arrow Batch Scanning (for columnar execution)
;;; ---------------------------------------------------------------------------

(defn arrow-raw-batch-lazy-seq
  "Create lazy seq of Arrow VectorSchemaRoot from ArrowReader's CloseableIterator.

   Each batch is immediately converted to VectorSchemaRoot to avoid issues with
   ColumnarBatch buffer reuse. The returned VectorSchemaRoot owns its data and
   is valid until the next batch is requested.

   IMPORTANT: Resources are closed when:
   - The seq is fully consumed
   - An exception occurs

   If iteration stops early, resources may leak.
   Callers should fully consume or use with-open pattern."
  [^java.util.Iterator iter ^java.io.Closeable closeable]
  (let [closed? (atom false)
        close-all! (fn []
                     (when (compare-and-set! closed? false true)
                       (try
                         (.close closeable)
                         (catch Exception e
                           (log/debug "Error closing ArrowReader:" (.getMessage e))))))]
    (letfn [(batch-seq []
              (lazy-seq
               (if (.hasNext iter)
                 (try
                   (let [^ColumnarBatch batch (.next iter)
                         ;; Convert immediately to VectorSchemaRoot to avoid buffer reuse issues
                         ^org.apache.arrow.vector.VectorSchemaRoot vsr
                         (.createVectorSchemaRootFromVectors batch)]
                     (cons vsr (batch-seq)))
                   (catch Exception e
                     (close-all!)
                     (throw e)))
                 (do (close-all!) nil))))]
      (batch-seq))))

(defn scan-raw-arrow-batches
  "Execute an Iceberg table scan returning Arrow VectorSchemaRoot batches.

   Unlike scan-with-arrow which converts to row maps, this returns
   VectorSchemaRoot batches for vectorized columnar processing.

   Args:
     table      - Iceberg Table instance
     opts       - Scan options:
       :columns     - seq of column names to project
       :predicates  - seq of predicate maps for pushdown
       :snapshot-id - specific snapshot for time travel
       :as-of-time  - Instant for time travel
       :batch-size  - rows per Arrow batch (default 4096)

   Returns: lazy seq of org.apache.arrow.vector.VectorSchemaRoot

   Resource Safety:
     If an exception occurs during scan setup, resources are cleaned up before
     re-throwing. Once the lazy seq is returned, resource cleanup is handled
     by arrow-raw-batch-lazy-seq (closes on exhaustion or exception).

   Note: Predicate pushdown is applied at the Iceberg layer (file/row-group pruning).
   Row-level filtering on returned batches must be done by the caller if needed."
  [^Table table {:keys [columns predicates snapshot-id as-of-time batch-size]
                 :or {batch-size 4096}}]
  (let [^TableScan scan (build-table-scan table {:columns columns
                                                 :predicates predicates
                                                 :snapshot-id snapshot-id
                                                 :as-of-time as-of-time})
        ^ArrowReader reader (ArrowReader. scan (int batch-size) false)]
    (try
      (let [scan-tasks (.planTasks scan)
            iter (.open reader scan-tasks)]
        (arrow-raw-batch-lazy-seq iter reader))
      (catch Exception e
        ;; Clean up reader if setup fails before lazy-seq takes ownership
        (try (.close reader) (catch Exception _ nil))
        (throw e)))))

(defn scan-filtered-arrow-batches
  "Execute an Iceberg table scan returning filtered Arrow VectorSchemaRoot batches.

   Unlike scan-raw-arrow-batches, this applies row-level filtering using
   vectorized predicate evaluation. The returned batches contain only matching
   rows, with data copied to avoid buffer reuse issues.

   This provides the best of both worlds:
   - Vectorized predicate evaluation (fast filtering)
   - Arrow batch output (no per-row map allocation)
   - Safe batch lifetime (copied data, no buffer reuse issues)

   Args:
     table      - Iceberg Table instance
     opts       - Scan options:
       :columns      - seq of column names to project
       :predicates   - seq of predicate maps for filtering
       :snapshot-id  - specific snapshot for time travel
       :as-of-time   - Instant for time travel
       :batch-size   - rows per Arrow batch (default 4096)
       :copy-batches - if true (default), copy batches for safety.
                       If false and no predicates, return raw batches
                       (only valid until next iteration - use for streaming).

   Returns: lazy seq of org.apache.arrow.vector.VectorSchemaRoot (filtered)

   Resource Safety:
     When copy-batches is true (default), returned batches own their data
     and are safe to hold beyond iteration.
     The ArrowReader is closed when the seq is exhausted or on exception."
  [^Table table {:keys [columns predicates snapshot-id as-of-time batch-size copy-batches]
                 :or {batch-size 4096 copy-batches true}}]
  (let [^TableScan scan (build-table-scan table {:columns columns
                                                 :predicates predicates
                                                 :snapshot-id snapshot-id
                                                 :as-of-time as-of-time})
        ^ArrowReader reader (ArrowReader. scan (int batch-size) false)]
    (try
      (let [scan-tasks (.planTasks scan)
            iter (.open reader scan-tasks)]
        (arrow-filtered-batch-lazy-seq iter reader predicates copy-batches))
      (catch Exception e
        ;; Clean up reader if setup fails before lazy-seq takes ownership
        (try (.close reader) (catch Exception _ nil))
        (throw e)))))

;;; ---------------------------------------------------------------------------
;;; Supported Predicates
;;; ---------------------------------------------------------------------------

(def supported-predicate-ops
  "Set of predicate operations supported by Iceberg."
  #{:eq :ne :gt :gte :lt :lte :in :between :is-null :not-null :and :or})
