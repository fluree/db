(ns fluree.db.tabular.iceberg.arrow
  "Arrow vectorized reading for Iceberg tables.

   This namespace provides Arrow-based vectorized read support.
   Requires the db-iceberg-arrow module with Arrow dependencies.

   Key components:
   - Arrow value extraction from FieldVectors
   - Batch-to-row-map conversion
   - Columnar predicate evaluation (no per-row map allocation)
   - Filtered batch creation (vectorized filtering)
   - Arrow batch lazy sequences"
  (:require [fluree.db.tabular.iceberg.core :as core]
            [fluree.db.util.log :as log])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot FieldVector
            BigIntVector IntVector Float4Vector Float8Vector
            VarCharVector VarBinaryVector BitVector DateDayVector
            TimeStampMicroTZVector TimeStampMicroVector DecimalVector]
           [org.apache.arrow.vector.types.pojo Field]
           [org.apache.iceberg Table TableScan]
           [org.apache.iceberg.arrow.vectorized ArrowReader ColumnarBatch]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Arrow Value Extraction
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

;;; ---------------------------------------------------------------------------
;;; Batch to Row Maps Conversion
;;; ---------------------------------------------------------------------------

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
;; These functions support columnar execution by creating new
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

;;; ---------------------------------------------------------------------------
;;; Arrow Batch Lazy Sequences
;;; ---------------------------------------------------------------------------

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
                         ^VectorSchemaRoot vsr
                         (.createVectorSchemaRootFromVectors batch)]
                     (cons vsr (batch-seq)))
                   (catch Exception e
                     (close-all!)
                     (throw e)))
                 (do (close-all!) nil))))]
      (batch-seq))))

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
  (let [^TableScan scan (core/build-table-scan table {:columns columns
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
  (let [^TableScan scan (core/build-table-scan table {:columns columns
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
  (let [^TableScan scan (core/build-table-scan table {:columns columns
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
        ^TableScan scan (core/build-table-scan table {:columns columns
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
