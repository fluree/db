(ns fluree.db.virtual-graph.iceberg.plan
  "Tabular plan execution for columnar Iceberg queries.

   This namespace provides the ITabularPlan protocol and physical operators
   for executing multi-table queries in a fully columnar fashion. Plans are
   compiled from SPARQL pattern groups and executed on Arrow batches, with
   solution maps materialized only at the boundary.

   Key Components:
   - ITabularPlan protocol: Lifecycle interface (open!/next-batch!/close!)
   - ScanOp: Leaf operator that reads from ITabularSource
   - HashJoinOp: Columnar hash join on Arrow batches
   - FilterOp: Vectorized predicate evaluation
   - ProjectOp: Column projection/renaming

   Execution Model:
   - Pull-based: Operators pull batches from children via next-batch!
   - Batched: Data flows as Arrow VectorSchemaRoot batches
   - Lazy: Batches are produced on-demand
   - Resource-managed: close! releases all resources

   Example:
     (let [plan (compile-plan sources pattern-groups join-graph stats)
           _    (open! plan)]
       (try
         (loop [solutions []]
           (if-let [batch (next-batch! plan)]
             (recur (into solutions (batch->solutions batch mapping)))
             solutions))
         (finally
           (close! plan))))"
  (:require [clojure.set :as set]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.join :as join])
  (:import [java.util HashMap ArrayList]
           [org.apache.arrow.memory RootAllocator BufferAllocator]
           [org.apache.arrow.vector VectorSchemaRoot FieldVector
            BigIntVector IntVector Float4Vector Float8Vector
            VarCharVector BitVector]
           [org.apache.arrow.vector.types FloatingPointPrecision]
           [org.apache.arrow.vector.types.pojo Field FieldType ArrowType ArrowType$Int
            ArrowType$FloatingPoint ArrowType$Utf8 ArrowType$Bool]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; ITabularPlan Protocol
;;; ---------------------------------------------------------------------------

(defprotocol ITabularPlan
  "Executable tabular plan node.

   Lifecycle:
   1. Create plan via factory function
   2. Call open! to initialize (returns self)
   3. Call next-batch! repeatedly until nil
   4. Call close! to release resources

   Thread safety: Not thread-safe. Use from a single thread."

  (open! [this]
    "Initialize the plan operator.
     - Opens child operators (if any)
     - Allocates resources (Arrow memory, hash tables, etc.)
     Returns self for chaining.")

  (next-batch! [this]
    "Produce the next batch of results.
     Returns an Arrow VectorSchemaRoot or nil when exhausted.

     IMPORTANT: The returned batch is owned by the operator and may be
     reused on the next call to next-batch!. Callers must consume the
     batch before calling next-batch! again.")

  (close! [this]
    "Release all resources held by this operator.
     - Closes child operators
     - Releases Arrow memory
     - Clears hash tables
     Safe to call multiple times.")

  (estimated-rows [this]
    "Return estimated output row count for planning.
     Used by the optimizer for join ordering and memory allocation."))

(defprotocol IColumnarHashJoin
  "Internal protocol for columnar hash join operations."
  (build-from-batch! [this batch]
    "Add a batch to the build side hash table.")
  (probe-batch [this batch]
    "Probe the hash table with a batch, returning joined batch."))

;;; ---------------------------------------------------------------------------
;;; Arrow Batch Utilities
;;; ---------------------------------------------------------------------------

(defn- get-vector-value
  "Extract a value from an Arrow vector at the given index.
   Returns nil for null values."
  [^org.apache.arrow.vector.FieldVector vector ^long idx]
  (when-not (.isNull vector (int idx))
    (let [type-id (.getMinorType vector)]
      (case (str type-id)
        "INT" (.get ^org.apache.arrow.vector.IntVector vector (int idx))
        "BIGINT" (.get ^org.apache.arrow.vector.BigIntVector vector (int idx))
        "FLOAT4" (.get ^org.apache.arrow.vector.Float4Vector vector (int idx))
        "FLOAT8" (.get ^org.apache.arrow.vector.Float8Vector vector (int idx))
        "VARCHAR" (str (.getObject ^org.apache.arrow.vector.VarCharVector vector (int idx)))
        "BIT" (= 1 (.get ^org.apache.arrow.vector.BitVector vector (int idx)))
        ;; Default: try getObject
        (.getObject vector (int idx))))))

(defn- arrow-batch?
  "Check if batch is an Arrow VectorSchemaRoot (vs a row map)."
  [batch]
  (instance? org.apache.arrow.vector.VectorSchemaRoot batch))

(defn- extract-key-from-arrow-batch
  "Extract join key values from an Arrow batch at the given row index."
  [^org.apache.arrow.vector.VectorSchemaRoot batch key-columns ^long row-idx]
  (let [vals (mapv (fn [col-name]
                     (when-let [vector (.getVector batch ^String col-name)]
                       (get-vector-value vector row-idx)))
                   key-columns)]
    (when-not (some nil? vals)
      (if (= 1 (count vals))
        (first vals)
        vals))))

(defn- extract-key-from-row-map
  "Extract join key values from a row map.
   Handles both string and keyword column names for flexibility."
  [row-map key-columns]
  (let [vals (mapv (fn [col]
                     ;; Try string key first, then keyword
                     (or (get row-map col)
                         (get row-map (keyword col))
                         (get row-map (name col))))
                   key-columns)]
    (when-not (some nil? vals)
      (if (= 1 (count vals))
        (first vals)
        vals))))

(defn- extract-key-from-batch
  "Extract join key values from a batch at the given row index.
   Handles both Arrow VectorSchemaRoot and row maps (dual-mode).
   For single-column keys, returns the value directly.
   For composite keys, returns a vector of values.
   Returns nil if any key column is null (null never matches)."
  ([batch key-columns]
   ;; For row maps, no row-idx needed
   (if (arrow-batch? batch)
     (extract-key-from-arrow-batch batch key-columns 0)
     (extract-key-from-row-map batch key-columns)))
  ([batch key-columns row-idx]
   (if (arrow-batch? batch)
     (extract-key-from-arrow-batch batch key-columns row-idx)
     ;; Row maps ignore row-idx (single row per "batch")
     (extract-key-from-row-map batch key-columns))))

(defn- batch-row-count
  "Get the number of rows in a batch.
   Handles both Arrow VectorSchemaRoot and row maps (dual-mode)."
  [batch]
  (if (arrow-batch? batch)
    (.getRowCount ^org.apache.arrow.vector.VectorSchemaRoot batch)
    ;; Row map is a single row
    1))

(defn- extract-row-from-arrow-batch
  "Extract a single row from an Arrow batch as a map."
  [^org.apache.arrow.vector.VectorSchemaRoot batch ^long row-idx]
  (into {}
        (for [^org.apache.arrow.vector.FieldVector fv (.getFieldVectors batch)
              :let [col-name (.getName (.getField fv))]]
          [col-name (get-vector-value fv row-idx)])))

(defn- extract-row-from-batch
  "Extract a row from a batch as a map.
   Handles both Arrow VectorSchemaRoot and row maps (dual-mode)."
  ([batch]
   (if (arrow-batch? batch)
     (extract-row-from-arrow-batch batch 0)
     ;; Row map is already a map
     batch))
  ([batch row-idx]
   (if (arrow-batch? batch)
     (extract-row-from-arrow-batch batch row-idx)
     ;; Row map ignores row-idx
     batch)))

;;; ---------------------------------------------------------------------------
;;; Arrow Batch Construction (for join output)
;;; ---------------------------------------------------------------------------

(def ^:private ^BufferAllocator join-allocator
  "Shared Arrow allocator for creating join output batches."
  (delay (RootAllocator.)))

(defn- value->arrow-type
  "Infer Arrow type from a Clojure value."
  [v]
  (cond
    (nil? v)        nil  ;; Can't infer from nil
    (string? v)     (ArrowType$Utf8.)
    (boolean? v)    (ArrowType$Bool.)
    (int? v)        (ArrowType$Int. 32 true)
    (integer? v)    (ArrowType$Int. 64 true)  ;; Long
    (float? v)      (ArrowType$FloatingPoint. FloatingPointPrecision/DOUBLE)
    (double? v)     (ArrowType$FloatingPoint. FloatingPointPrecision/DOUBLE)
    :else           (ArrowType$Utf8.)))  ;; Default to string

(defn- infer-column-type
  "Infer Arrow type for a column by sampling non-nil values."
  [rows col-name]
  (or (some (fn [row]
              (when-let [v (get row col-name)]
                (value->arrow-type v)))
            rows)
      ;; Default to string if all nil
      (ArrowType$Utf8.)))

(defn- create-vector-for-type
  "Create an Arrow vector for the given type."
  ^FieldVector [^BufferAllocator allocator ^String col-name ^ArrowType arrow-type num-rows]
  (let [field-type (FieldType/nullable arrow-type)
        field (Field. col-name field-type nil)
        ^FieldVector vector (.createVector field allocator)]
    ;; Allocate space based on type
    (condp instance? vector
      BigIntVector (.allocateNew ^BigIntVector vector (int num-rows))
      IntVector (.allocateNew ^IntVector vector (int num-rows))
      Float4Vector (.allocateNew ^Float4Vector vector (int num-rows))
      Float8Vector (.allocateNew ^Float8Vector vector (int num-rows))
      BitVector (.allocateNew ^BitVector vector (int num-rows))
      VarCharVector (.allocateNew ^VarCharVector vector (* 64 num-rows) (int num-rows))
      ;; Fallback
      (do (.setInitialCapacity vector (int num-rows))
          (.allocateNew vector)))
    vector))

(defn- set-vector-value!
  "Set a value in an Arrow vector at the given index."
  [^FieldVector vector ^long idx value]
  (if (nil? value)
    (.setNull vector (int idx))
    (condp instance? vector
      BigIntVector (.set ^BigIntVector vector (int idx) (long value))
      IntVector (.set ^IntVector vector (int idx) (int value))
      Float4Vector (.set ^Float4Vector vector (int idx) (float value))
      Float8Vector (.set ^Float8Vector vector (int idx) (double value))
      BitVector (.set ^BitVector vector (int idx) (if value 1 0))
      VarCharVector (.setSafe ^VarCharVector vector (int idx)
                              (.getBytes (str value) "UTF-8"))
      ;; No fallback - all supported types handled above
      (throw (ex-info "Unsupported Arrow vector type for value setting"
                      {:vector-type (type vector) :value value})))))

(defn- rows->arrow-batch
  "Convert a seq of row maps to an Arrow VectorSchemaRoot.

   Infers schema from the first row's column names and value types.
   Returns nil if rows is empty."
  ^VectorSchemaRoot [rows]
  (when (seq rows)
    (let [rows-vec (vec rows)
          num-rows (count rows-vec)
          ;; Get all column names from first row (preserves order)
          col-names (keys (first rows-vec))
          allocator @join-allocator
          ;; Create vectors for each column
          vectors (mapv (fn [col-name]
                          (let [arrow-type (infer-column-type rows-vec col-name)
                                ^FieldVector vector (create-vector-for-type
                                                     allocator (str col-name) arrow-type num-rows)]
                            ;; Populate vector
                            (dotimes [i num-rows]
                              (set-vector-value! vector i (get (nth rows-vec i) col-name)))
                            (.setValueCount vector num-rows)
                            vector))
                        col-names)
          ^VectorSchemaRoot root (VectorSchemaRoot. ^java.util.List vectors)]
      (.setRowCount root num-rows)
      root)))

;;; ---------------------------------------------------------------------------
;;; Vectorized Join Output (Phase 3: True columnar joins)
;;; ---------------------------------------------------------------------------

(defn- copy-arrow-value!
  "Copy a single value from source vector to destination vector.
   Handles null values correctly."
  [^FieldVector src-vector ^long src-idx ^FieldVector dest-vector ^long dest-idx]
  (if (.isNull src-vector (int src-idx))
    (.setNull dest-vector (int dest-idx))
    (condp instance? src-vector
      BigIntVector (.set ^BigIntVector dest-vector (int dest-idx)
                         (.get ^BigIntVector src-vector (int src-idx)))
      IntVector (.set ^IntVector dest-vector (int dest-idx)
                      (.get ^IntVector src-vector (int src-idx)))
      Float4Vector (.set ^Float4Vector dest-vector (int dest-idx)
                         (.get ^Float4Vector src-vector (int src-idx)))
      Float8Vector (.set ^Float8Vector dest-vector (int dest-idx)
                         (.get ^Float8Vector src-vector (int src-idx)))
      BitVector (.set ^BitVector dest-vector (int dest-idx)
                      (.get ^BitVector src-vector (int src-idx)))
      VarCharVector (let [bytes (.get ^VarCharVector src-vector (int src-idx))]
                      (.setSafe ^VarCharVector dest-vector (int dest-idx) ^bytes bytes))
      ;; Fallback: copy via object (slower but safe)
      (.set dest-vector (int dest-idx) (.getObject src-vector (int src-idx))))))

(defn- allocate-output-vector!
  "Allocate space in a destination vector for num-rows."
  [^FieldVector vector num-rows]
  (condp instance? vector
    BigIntVector (.allocateNew ^BigIntVector vector (int num-rows))
    IntVector (.allocateNew ^IntVector vector (int num-rows))
    Float4Vector (.allocateNew ^Float4Vector vector (int num-rows))
    Float8Vector (.allocateNew ^Float8Vector vector (int num-rows))
    BitVector (.allocateNew ^BitVector vector (int num-rows))
    ;; Variable-width: estimate 64 bytes per value, setSafe will grow if needed
    VarCharVector (.allocateNew ^VarCharVector vector (* 64 num-rows) (int num-rows))
    ;; Fallback
    (do (.setInitialCapacity vector (int num-rows))
        (.allocateNew vector))))

(defn- gather-join-output-batch
  "Create output Arrow batch by gathering from build and probe batches.

   This is the core of vectorized join output - instead of extracting rows
   to maps and merging, we directly copy values from source vectors to
   output vectors based on match indices.

   Args:
     build-batches - Vector of VectorSchemaRoot batches from build side
     probe-batch   - Current VectorSchemaRoot batch from probe side
     build-batch-idxs - int[] of build batch indices for each output row
                        For left outer join, -1 indicates no match (write nulls).
     build-row-idxs   - int[] of build row indices for each output row
     probe-row-idxs   - int[] of probe row indices for each output row
     output-columns   - Optional set of column names to include in output.
                        If nil, all columns from both sides are included.
                        Use for projection pushdown to avoid copying unneeded columns.
     left-outer?      - If true, build-batch-idx = -1 means write nulls for build
                        columns (for left outer join unmatched rows).

   NOTE: If build and probe have columns with the same name, both will be
   included in the output. This can cause ambiguity when accessing columns.
   Consider using :output-columns to select specific columns and avoid
   collision, or ensure your tables don't have conflicting column names.

   Returns:
     New VectorSchemaRoot with gathered output, or nil if no matches."
  [build-batches ^VectorSchemaRoot probe-batch
   ^ints build-batch-idxs ^ints build-row-idxs ^ints probe-row-idxs
   output-columns left-outer?]
  (let [num-rows (alength build-batch-idxs)]
    (when (pos? num-rows)
      (let [allocator @join-allocator
            ;; Get first build batch to determine schema (all should have same schema)
            ^VectorSchemaRoot first-build (first build-batches)
            ;; Helper to check if column should be included
            include-col? (if output-columns
                           #(contains? output-columns %)
                           (constantly true))
            ;; Collect fields and source vectors, filtering by output-columns
            ;; Returns vector of [Field source-vector-idx] pairs for included columns
            build-col-specs (when first-build
                              (let [field-vectors (.getFieldVectors first-build)]
                                (into []
                                      (keep-indexed
                                       (fn [idx ^FieldVector fv]
                                         (let [field (.getField fv)
                                               col-name (.getName field)]
                                           (when (include-col? col-name)
                                             [field idx]))))
                                      field-vectors)))
            probe-col-specs (let [field-vectors (.getFieldVectors probe-batch)]
                              (into []
                                    (keep-indexed
                                     (fn [idx ^FieldVector fv]
                                       (let [field (.getField fv)
                                             col-name (.getName field)]
                                         (when (include-col? col-name)
                                           [field idx]))))
                                    field-vectors))
            ;; Check for column name collisions (warn once per join)
            _ (let [build-names (into #{} (map #(.getName ^Field (first %)) build-col-specs))
                    probe-names (into #{} (map #(.getName ^Field (first %)) probe-col-specs))
                    collisions (set/intersection build-names probe-names)]
                (when (seq collisions)
                  (log/warn "Column name collision in join output:" collisions
                            "- both build and probe have columns with these names."
                            "Use :output-columns to select specific columns.")))
            ;; Create output vectors only for included columns
            build-vectors (mapv (fn [[^Field field _]]
                                  (let [^FieldVector vector (.createVector field allocator)]
                                    (allocate-output-vector! vector num-rows)
                                    vector))
                                build-col-specs)
            probe-vectors (mapv (fn [[^Field field _]]
                                  (let [^FieldVector vector (.createVector field allocator)]
                                    (allocate-output-vector! vector num-rows)
                                    vector))
                                probe-col-specs)
            ;; Precompute: cache source vectors for each build batch (vector of vectors)
            build-src-vectors-by-batch (mapv #(vec (.getFieldVectors ^VectorSchemaRoot %))
                                             build-batches)
            ;; Pre-fetch probe source vectors
            probe-src-vectors (vec (.getFieldVectors probe-batch))
            ;; Source column indices for included columns
            build-src-indices (mapv second build-col-specs)
            probe-src-indices (mapv second probe-col-specs)
            ;; Column counts
            num-build-cols (count build-vectors)
            num-probe-cols (count probe-vectors)]
        ;; Gather values from source batches into output vectors using primitive loop
        (dotimes [out-idx num-rows]
          (let [build-batch-idx (aget build-batch-idxs out-idx)
                build-row-idx (aget build-row-idxs out-idx)
                probe-row-idx (aget probe-row-idxs out-idx)]
            ;; Handle build columns - check for left outer join null case
            (if (and left-outer? (neg? build-batch-idx))
              ;; No match for this probe row - write nulls for all build columns
              (dotimes [i num-build-cols]
                (let [^FieldVector out-vec (nth build-vectors i)]
                  (.setNull out-vec out-idx)))
              ;; Normal case - copy from build batch
              (let [build-src-vectors (nth build-src-vectors-by-batch build-batch-idx)]
                (dotimes [i num-build-cols]
                  (let [src-idx (nth build-src-indices i)]
                    (copy-arrow-value! (nth build-src-vectors src-idx) build-row-idx
                                       (nth build-vectors i) out-idx)))))
            ;; Copy only included probe columns (always present)
            (dotimes [i num-probe-cols]
              (let [src-idx (nth probe-src-indices i)]
                (copy-arrow-value! (nth probe-src-vectors src-idx) probe-row-idx
                                   (nth probe-vectors i) out-idx)))))
        ;; Set value counts and create root
        (doseq [^FieldVector v build-vectors]
          (.setValueCount v num-rows))
        (doseq [^FieldVector v probe-vectors]
          (.setValueCount v num-rows))
        (let [all-vectors (into (vec build-vectors) probe-vectors)
              ^VectorSchemaRoot root (VectorSchemaRoot. ^java.util.List all-vectors)]
          (.setRowCount root num-rows)
          root)))))

;;; ---------------------------------------------------------------------------
;;; ScanOp - Leaf Operator
;;; ---------------------------------------------------------------------------

(defrecord ScanOp [source table-name columns predicates time-travel
                   batch-size use-arrow-batches? copy-batches? state]
  ;; state is an atom containing:
  ;; {:batch-iter nil :opened? false :row-count-estimate nil :mode :row-maps|:arrow}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "ScanOp opening:" {:table table-name
                                    :columns columns
                                    :use-arrow-batches? use-arrow-batches?})
      ;; Get statistics for row count estimate
      (let [stats (tabular/get-statistics source table-name
                                          (cond-> {}
                                            (:snapshot-id time-travel)
                                            (assoc :snapshot-id (:snapshot-id time-travel))
                                            (:as-of-time time-travel)
                                            (assoc :as-of-time (:as-of-time time-travel))))
            row-count-estimate (or (:row-count stats) 1000)
            ;; Choose scan method based on use-arrow-batches? flag
            scan-opts (cond-> {:columns columns
                               :predicates predicates
                               :batch-size (or batch-size 4096)}
                        (:snapshot-id time-travel)
                        (assoc :snapshot-id (:snapshot-id time-travel))
                        (:as-of-time time-travel)
                        (assoc :as-of-time (:as-of-time time-travel))
                        ;; Pass through copy-batches option (nil = default true)
                        (some? copy-batches?)
                        (assoc :copy-batches copy-batches?))
            batches (if use-arrow-batches?
                      ;; Use raw Arrow batches for columnar execution
                      ;; scan-arrow-batches returns VectorSchemaRoot directly
                      (tabular/scan-arrow-batches source table-name scan-opts)
                      ;; Use row maps (legacy behavior)
                      (tabular/scan-batches source table-name scan-opts))]
        ;; Determine if batches are actually copied:
        ;; - If not using Arrow batches, doesn't matter (row maps)
        ;; - If copy-batches? is true (or nil/default), always copied
        ;; - If copy-batches? is false AND no predicates, raw batches returned
        ;; - If copy-batches? is false BUT predicates exist, filtering copies data
        ;; CRITICAL: filter-arrow-batch always copies when predicates match rows,
        ;; even when copy-batch? is false. Only when no predicates is original returned.
        (reset! state {:batch-iter (seq batches)
                       :opened? true
                       :row-count-estimate row-count-estimate
                       :mode (if use-arrow-batches? :arrow :row-maps)
                       ;; Track whether batches are copied (affects close behavior)
                       :batches-copied? (or (not use-arrow-batches?)
                                            (not (false? copy-batches?))
                                            (seq predicates))})))
    this)

  (next-batch! [_this]
    (let [{:keys [opened? batch-iter]} @state]
      (when (and opened? batch-iter)
        (let [batch (first batch-iter)]
          (swap! state assoc :batch-iter (next batch-iter))
          ;; Batch is either a row map or VectorSchemaRoot depending on mode
          batch))))

  (close! [this]
    (when (:opened? @state)
      (log/debug "ScanOp closing:" {:table table-name})
      (reset! state {:batch-iter nil :opened? false :row-count-estimate nil :mode nil}))
    this)

  (estimated-rows [_this]
    (or (:row-count-estimate @state) 1000)))

(defn create-scan-op
  "Create a scan operator for reading from an Iceberg table.

   Args:
     source      - ITabularSource implementation
     table-name  - Fully qualified table name
     columns     - Seq of column names to project (nil = all)
     predicates  - Seq of pushdown predicate maps
     time-travel - Time travel spec {:snapshot-id or :as-of-time}

   Options:
     :batch-size        - Rows per batch (default 4096)
     :use-arrow-batches? - If true, return raw Arrow VectorSchemaRoot batches.
                           If false (default), return row maps for backward
                           compatibility. Set to true for columnar execution.
     :copy-batches?     - If true (default), copy Arrow batches for safe holding.
                          If false, batches share underlying buffers and are only
                          valid until the next batch is requested. Use false for
                          streaming consumption where batches are immediately
                          processed and discarded."
  ([source table-name columns predicates]
   (create-scan-op source table-name columns predicates nil {}))
  ([source table-name columns predicates time-travel]
   (create-scan-op source table-name columns predicates time-travel {}))
  ([source table-name columns predicates time-travel opts]
   (map->ScanOp {:source source
                 :table-name table-name
                 :columns (when columns (vec columns))
                 :predicates (when predicates (vec predicates))
                 :time-travel time-travel
                 :batch-size (get opts :batch-size 4096)
                 :use-arrow-batches? (get opts :use-arrow-batches? false)
                 :copy-batches? (get opts :copy-batches?)  ;; nil = use default (true)
                 :state (atom {:batch-iter nil
                               :opened? false
                               :row-count-estimate nil
                               :mode nil})})))

(defn batches-copied?
  "Check whether a plan's Arrow batches are safe to close.

   For ScanOp: returns the :batches-copied? state (true if copy-batches? was true)
   For other plans: returns true (default safe assumption)

   Use this to decide whether to call .close on Arrow batches from this plan.
   Non-copied batches share underlying buffers with the iterator and should
   NOT be closed by the caller - the iterator manages their lifecycle."
  [plan]
  (if (instance? ScanOp plan)
    (get @(:state plan) :batches-copied? true)
    ;; Default: assume batches are copied and safe to close
    true))

;;; ---------------------------------------------------------------------------
;;; HashJoinOp - Columnar Hash Join
;;; ---------------------------------------------------------------------------

(defrecord HashJoinOp [build-child probe-child build-keys probe-keys
                       output-arrow? vectorized? output-columns left-outer? state]
  ;; output-columns: Optional set of column names to include in output.
  ;;                 If nil, all columns from both sides are included.
  ;;                 Use for projection pushdown to skip copying unneeded columns.
  ;; left-outer?:    If true, this is a left outer join. All probe rows appear in
  ;;                 output; unmatched probe rows have nulls for build-side columns.
  ;;                 This implements SPARQL OPTIONAL semantics.
  ;; state is an atom containing:
  ;; Standard mode:
  ;;   {:hash-table HashMap (key -> [row-map...]), :build-complete? bool, :opened? bool,
  ;;    :build-row-count int, :estimated-output-rows int}
  ;; Vectorized mode (vectorized? = true):
  ;;   {:hash-table HashMap (key -> [[batch-idx row-idx]...]), :build-batches [VectorSchemaRoot...],
  ;;    :build-complete? bool, :opened? bool, :build-row-count int, :estimated-output-rows int}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "HashJoinOp opening:" {:build-keys build-keys
                                        :probe-keys probe-keys
                                        :vectorized? vectorized?})
      ;; Open children
      (open! build-child)
      (open! probe-child)
      ;; Estimate output rows using join cardinality estimation
      (let [build-rows (estimated-rows build-child)
            probe-rows (estimated-rows probe-child)
            est-output (min (* build-rows probe-rows) (max build-rows probe-rows))
            ;; Check if batches are copied (determines if we should close them)
            ;; Non-copied batches share buffers with iterator - don't close
            close-build-batches? (batches-copied? build-child)
            close-probe-batches? (batches-copied? probe-child)]
        (reset! state (cond-> {:hash-table (HashMap.)
                               :build-complete? false
                               :opened? true
                               :build-row-count 0
                               :estimated-output-rows est-output
                               :close-build-batches? close-build-batches?
                               :close-probe-batches? close-probe-batches?}
                        ;; Vectorized mode stores build batches
                        vectorized? (assoc :build-batches [])))))
    this)

  (next-batch! [this]
    (let [{:keys [opened? build-complete? close-build-batches? close-probe-batches?]} @state]
      (when opened?
        ;; Phase 1: Build hash table from build side (if not done)
        (when-not build-complete?
          (log/debug "HashJoinOp building hash table..." {:vectorized? vectorized?})
          (loop []
            (when-let [batch (next-batch! build-child)]
              (build-from-batch! this batch)
              ;; In vectorized mode, we store batches - don't close them
              ;; In standard mode, close if they were copied
              (when (and (not vectorized?)
                         close-build-batches?
                         (arrow-batch? batch))
                (.close ^org.apache.arrow.vector.VectorSchemaRoot batch))
              (recur)))
          (swap! state assoc :build-complete? true)
          (let [{:keys [hash-table build-row-count build-batches]} @state]
            (log/debug "HashJoinOp build complete:" {:build-rows build-row-count
                                                     :unique-keys (.size ^HashMap hash-table)
                                                     :stored-batches (when vectorized?
                                                                       (count build-batches))})))
        ;; Phase 2: Probe with batches from probe side
        (when-let [probe-b (next-batch! probe-child)]
          (let [result (probe-batch this probe-b)]
            ;; Only close probe batch if it was copied (owns its memory)
            ;; Non-copied batches share buffers with iterator - don't close
            (when (and close-probe-batches? (arrow-batch? probe-b))
              (.close ^org.apache.arrow.vector.VectorSchemaRoot probe-b))
            result)))))

  (close! [this]
    (when (:opened? @state)
      (log/debug "HashJoinOp closing" {:vectorized? vectorized?})
      (close! build-child)
      (close! probe-child)
      (when-let [^HashMap ht (:hash-table @state)]
        (.clear ht))
      ;; In vectorized mode, close stored build batches
      (when vectorized?
        (doseq [^VectorSchemaRoot batch (:build-batches @state)]
          (try
            (.close batch)
            (catch Exception e
              (log/debug "Error closing build batch:" (.getMessage e))))))
      (reset! state {:hash-table nil :build-complete? false :opened? false
                     :build-row-count 0 :estimated-output-rows nil
                     :build-batches nil}))
    this)

  (estimated-rows [_this]
    (or (:estimated-output-rows @state) 1000))

  IColumnarHashJoin
  (build-from-batch! [_this batch]
    (if vectorized?
      ;; Vectorized mode: store batch reference + index pairs in hash table
      ;; Hash table maps: key -> [[batch-idx row-idx] ...]
      (when (arrow-batch? batch)
        (let [^HashMap hash-table (:hash-table @state)
              batch-idx (count (:build-batches @state))
              row-count (batch-row-count batch)]
          ;; Store the batch (must be copied so it outlives iteration)
          ;; Build-side should use copy-batches? true
          (swap! state update :build-batches conj batch)
          ;; Index each row by its key
          (dotimes [i row-count]
            (when-let [key (extract-key-from-batch batch build-keys i)]
              (let [^ArrayList refs (or (.get hash-table key)
                                        (let [al (ArrayList.)]
                                          (.put hash-table key al)
                                          al))]
                (.add refs (int-array [batch-idx i]))
                (swap! state update :build-row-count inc))))))
      ;; Standard mode: extract row data to maps
      (let [^HashMap hash-table (:hash-table @state)
            row-count (batch-row-count batch)]
        ;; Capture build-side column names from first batch (for left outer join null creation)
        (when (and left-outer? (nil? (:build-columns @state)) (pos? row-count))
          (let [first-row (extract-row-from-batch batch 0)
                col-names (when (map? first-row) (keys first-row))]
            (swap! state assoc :build-columns col-names)))
        (dotimes [i row-count]
          (when-let [key (extract-key-from-batch batch build-keys i)]
            (let [^ArrayList rows (or (.get hash-table key)
                                      (let [al (ArrayList.)]
                                        (.put hash-table key al)
                                        al))
                  row-data (extract-row-from-batch batch i)]
              (.add rows row-data)
              (swap! state update :build-row-count inc)))))))

  (probe-batch [_this batch]
    (if vectorized?
      ;; Vectorized mode: gather output directly from source vectors
      (when (arrow-batch? batch)
        (let [^HashMap hash-table (:hash-table @state)
              build-batches (:build-batches @state)
              row-count (batch-row-count batch)
              ;; First pass: count matches and track which probe rows have matches
              match-count (atom 0)
              ;; For left outer: track which probe rows have matches
              probe-has-match (when left-outer? (boolean-array row-count))]
          ;; Count matches (and mark matched probe rows for left-outer)
          (dotimes [probe-row-idx row-count]
            (when-let [key (extract-key-from-batch batch probe-keys probe-row-idx)]
              (when-let [^ArrayList build-refs (.get hash-table key)]
                (let [match-size (.size build-refs)]
                  (when (pos? match-size)
                    (swap! match-count + match-size)
                    (when probe-has-match
                      (aset probe-has-match probe-row-idx true)))))))
          ;; For left outer: count unmatched probe rows
          (let [unmatched-count (if left-outer?
                                  (reduce (fn [cnt i]
                                            (if (aget probe-has-match i) cnt (inc cnt)))
                                          0 (range row-count))
                                  0)
                total-matches @match-count
                total-output (+ total-matches unmatched-count)]
            (when (pos? total-output)
              ;; Allocate primitive int arrays for match indices
              (let [build-batch-idxs (int-array total-output)
                    build-row-idxs (int-array total-output)
                    probe-row-idxs (int-array total-output)
                    write-idx (atom 0)]
                ;; Second pass: fill arrays with match data
                (dotimes [probe-row-idx row-count]
                  (let [key (extract-key-from-batch batch probe-keys probe-row-idx)
                        ^ArrayList build-refs (when key (.get hash-table key))]
                    (if (and build-refs (pos? (.size build-refs)))
                      ;; Has matches - emit all matched rows
                      (doseq [^ints ref build-refs]
                        (let [idx @write-idx]
                          (aset build-batch-idxs idx (aget ref 0))
                          (aset build-row-idxs idx (aget ref 1))
                          (aset probe-row-idxs idx (int probe-row-idx))
                          (swap! write-idx inc)))
                      ;; No matches - for left outer, emit with -1 sentinel
                      (when left-outer?
                        (let [idx @write-idx]
                          (aset build-batch-idxs idx (int -1))
                          (aset build-row-idxs idx (int -1))
                          (aset probe-row-idxs idx (int probe-row-idx))
                          (swap! write-idx inc))))))
                (log/debug "HashJoinOp vectorized probe:" {:probe-rows row-count
                                                           :matches total-matches
                                                           :unmatched unmatched-count
                                                           :left-outer? left-outer?
                                                           :output-columns (count output-columns)})
                ;; Gather output from source batches using primitive arrays
                ;; Pass output-columns for projection pushdown
                (gather-join-output-batch build-batches batch
                                          build-batch-idxs build-row-idxs probe-row-idxs
                                          output-columns left-outer?))))))
      ;; Standard mode: extract and merge row maps
      (let [^HashMap hash-table (:hash-table @state)
            row-count (batch-row-count batch)
            joined-rows (java.util.ArrayList.)
            ;; For left outer join, we need build-side column names to create null entries
            build-columns (:build-columns @state)]
        (dotimes [i row-count]
          (let [key (extract-key-from-batch batch probe-keys i)
                ^ArrayList build-rows (when key (.get hash-table key))
                probe-row (extract-row-from-batch batch i)]
            (if (and build-rows (pos? (.size build-rows)))
              ;; Has matches - merge probe with each build row
              (doseq [build-row build-rows]
                (.add joined-rows (merge build-row probe-row)))
              ;; No matches
              (when left-outer?
                ;; Left outer join: include probe row with nulls for build columns
                (let [null-build-row (when build-columns
                                       (zipmap build-columns (repeat nil)))]
                  (.add joined-rows (merge null-build-row probe-row)))))))
        (log/debug "HashJoinOp probe batch:" {:probe-rows row-count
                                              :joined-rows (.size joined-rows)
                                              :output-arrow? output-arrow?
                                              :left-outer? left-outer?})
        (if output-arrow?
          (rows->arrow-batch (vec joined-rows))
          (vec joined-rows))))))

(defn create-hash-join-op
  "Create a hash join operator for joining two tabular plans.

   The build child's output is loaded into a hash table, then the probe
   child's batches are streamed through to find matches.

   Args:
     build-child - ITabularPlan for build side (smaller table preferred)
     probe-child - ITabularPlan for probe side
     build-keys  - Vector of column names for build-side key
     probe-keys  - Vector of column names for probe-side key
     opts        - Optional map with:
                   :output-arrow? - If true, output Arrow VectorSchemaRoot batches
                                    instead of row maps. Use for columnar pipelines.
                                    Default: false (returns row maps).
                   :vectorized?   - If true, use true vectorized join (Phase 3).
                                    Build stores Arrow batches + index refs in hash table.
                                    Probe gathers output directly from source vectors.
                                    Requires Arrow batch inputs. ~2x faster than standard.
                                    Default: false.
                   :output-columns - Optional set of column names to include in output.
                                     If nil (default), all columns from both sides
                                     are included. Use for projection pushdown to
                                     avoid copying unneeded columns in vectorized mode.
                                     Only applies when :vectorized? is true.
                   :left-outer?   - If true, this is a left outer join. All probe
                                    rows appear in output; unmatched probe rows have
                                    nulls for build-side columns. This implements
                                    SPARQL OPTIONAL semantics. Default: false.

   IMPORTANT: When vectorized? is true:
   - Build-side MUST use copy-batches? true (batches must outlive iteration)
   - Probe-side can use copy-batches? false (streaming mode)
   - Both sides MUST provide Arrow batches (use-arrow-batches? true)

   The join produces output containing columns from both sides (or subset if
   :output-columns specified)."
  ([build-child probe-child build-keys probe-keys]
   (create-hash-join-op build-child probe-child build-keys probe-keys {}))
  ([build-child probe-child build-keys probe-keys opts]
   {:pre [(satisfies? ITabularPlan build-child)
          (satisfies? ITabularPlan probe-child)
          (vector? build-keys)
          (vector? probe-keys)
          (= (count build-keys) (count probe-keys))]}
   (map->HashJoinOp {:build-child build-child
                     :probe-child probe-child
                     :build-keys build-keys
                     :probe-keys probe-keys
                     :output-arrow? (get opts :output-arrow? false)
                     :vectorized? (get opts :vectorized? false)
                     :output-columns (get opts :output-columns)
                     :left-outer? (get opts :left-outer? false)
                     :state (atom {:hash-table nil
                                   :build-complete? false
                                   :opened? false
                                   :build-row-count 0
                                   :estimated-output-rows nil})})))

;;; ---------------------------------------------------------------------------
;;; FilterOp - Vectorized Filtering
;;; ---------------------------------------------------------------------------

(defrecord FilterOp [child predicates state]
  ;; state is an atom containing {:opened? bool}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "FilterOp opening:" {:predicates (count predicates)})
      (open! child)
      (reset! state {:opened? true}))
    this)

  (next-batch! [_this]
    (when (:opened? @state)
      ;; Get next batch from child and apply filters
      ;; For now, pass through (filtering done at scan level via pushdown)
      ;; A full implementation would apply vectorized predicates here
      (when-let [batch (next-batch! child)]
        ;; TODO: Apply predicates that couldn't be pushed down
        ;; For Iceberg, most predicates are pushed to scan level
        batch)))

  (close! [this]
    (when (:opened? @state)
      (log/debug "FilterOp closing")
      (close! child)
      (reset! state {:opened? false}))
    this)

  (estimated-rows [_this]
    ;; Apply selectivity estimate
    (let [child-rows (estimated-rows child)
          selectivity (join/estimate-selectivity {} predicates)]
      (long (* child-rows selectivity)))))

(defn create-filter-op
  "Create a filter operator for applying predicates.

   Note: For Iceberg sources, predicates are typically pushed down to
   the scan level. This operator handles residual predicates that
   couldn't be pushed down.

   Args:
     child      - Child ITabularPlan
     predicates - Seq of predicate maps {:column :op :value}"
  [child predicates]
  {:pre [(satisfies? ITabularPlan child)]}
  (map->FilterOp {:child child
                  :predicates (vec predicates)
                  :state (atom {:opened? false})}))

;;; ---------------------------------------------------------------------------
;;; ProjectOp - Column Projection
;;; ---------------------------------------------------------------------------

(defrecord ProjectOp [child columns column-aliases state]
  ;; state is an atom containing {:opened? bool}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "ProjectOp opening:" {:columns columns})
      (open! child)
      (reset! state {:opened? true}))
    this)

  (next-batch! [_this]
    (when (:opened? @state)
      (when-let [batch (next-batch! child)]
        ;; TODO: Create new batch with only projected columns
        ;; For now, return original batch
        ;; A full implementation would slice the batch to requested columns
        batch)))

  (close! [this]
    (when (:opened? @state)
      (log/debug "ProjectOp closing")
      (close! child)
      (reset! state {:opened? false}))
    this)

  (estimated-rows [_this]
    (estimated-rows child)))

(defn create-project-op
  "Create a projection operator for selecting/renaming columns.

   Note: For Iceberg sources, column projection is typically pushed
   down to the scan level. This operator handles post-join projections
   and column aliasing for SPARQL variables.

   Args:
     child          - Child ITabularPlan
     columns        - Seq of column names to keep
     column-aliases - Map of {old-name -> new-name} for renaming (optional)"
  ([child columns]
   (create-project-op child columns nil))
  ([child columns column-aliases]
   {:pre [(satisfies? ITabularPlan child)]}
   (map->ProjectOp {:child child
                    :columns (vec columns)
                    :column-aliases column-aliases
                    :state (atom {:opened? false})})))

;;; ---------------------------------------------------------------------------
;;; UnionOp - SPARQL UNION
;;; ---------------------------------------------------------------------------

(defrecord UnionOp [children state]
  ;; state is an atom containing {:opened? bool, :current-child-idx int}
  ;; children is a vector of ITabularPlan operators (one per UNION branch)
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "UnionOp opening:" {:branch-count (count children)})
      ;; Open all children upfront
      (doseq [child children]
        (open! child))
      (reset! state {:opened? true
                     :current-child-idx 0}))
    this)

  (next-batch! [_this]
    (when (:opened? @state)
      (let [idx (:current-child-idx @state)]
        (when (< idx (count children))
          ;; Try to get a batch from the current child
          (if-let [batch (next-batch! (nth children idx))]
            batch
            ;; Current child exhausted, try next child
            (do
              (swap! state update :current-child-idx inc)
              ;; Recursively try next child (will return nil if all exhausted)
              (let [new-idx (:current-child-idx @state)]
                (when (< new-idx (count children))
                  ;; Keep trying children until we get a batch or run out
                  (loop [child-idx new-idx]
                    (when (< child-idx (count children))
                      (if-let [batch (next-batch! (nth children child-idx))]
                        (do
                          (swap! state assoc :current-child-idx child-idx)
                          batch)
                        (do
                          (swap! state assoc :current-child-idx (inc child-idx))
                          (recur (inc child-idx))))))))))))))

  (close! [this]
    (when (:opened? @state)
      (log/debug "UnionOp closing")
      (doseq [child children]
        (close! child))
      (reset! state {:opened? false
                     :current-child-idx 0}))
    this)

  (estimated-rows [_this]
    ;; UNION output is sum of all branch estimates
    (reduce + 0 (map estimated-rows children))))

(defn create-union-op
  "Create a UNION operator that concatenates results from multiple branches.

   UNION returns all results from all branches sequentially. This is equivalent
   to SPARQL UNION semantics where results from all alternatives are combined.

   For streaming execution, branches are processed sequentially (not interleaved)
   to minimize memory usage.

   Args:
     children - Vector of ITabularPlan operators, one per UNION branch

   Example:
     (create-union-op [(create-scan-op src1 \"routes\" [...] [])
                       (create-scan-op src2 \"airlines\" [...] [])])"
  [children]
  {:pre [(vector? children)
         (every? #(satisfies? ITabularPlan %) children)
         (pos? (count children))]}
  (map->UnionOp {:children children
                 :state (atom {:opened? false
                               :current-child-idx 0})}))

;;; ---------------------------------------------------------------------------
;;; HashAggOp - SPARQL GROUP BY + Aggregations
;;; ---------------------------------------------------------------------------

;; Forward declaration for batch->row-maps used in HashAggOp
(declare batch->row-maps)

(defn- get-row-value
  "Get a value from a row map, handling both string and keyword keys.

   Arrow batches produce string keys, while mock sources may use keywords.
   This function tries both to ensure compatibility."
  [row col]
  (or (get row col)
      (get row (keyword col))
      (get row (name col))))

(defn- create-aggregator
  "Create an accumulator state for an aggregate function.

   Returns initial state map for the aggregator. State is updated incrementally
   as values are processed.

   Aggregate function types:
     :count       - COUNT(*) or COUNT(?var) - counts rows/non-null values
     :count-distinct - COUNT(DISTINCT ?var) - counts unique non-null values
     :sum         - SUM(?var) - sum of numeric values
     :avg         - AVG(?var) - average (tracks sum and count)
     :min         - MIN(?var) - minimum value
     :max         - MAX(?var) - maximum value"
  [agg-type]
  (case agg-type
    :count {:type :count :count 0}
    :count-distinct {:type :count-distinct :values #{}}
    :sum {:type :sum :sum 0}
    :avg {:type :avg :sum 0 :count 0}
    :min {:type :min :value nil}
    :max {:type :max :value nil}
    (throw (ex-info "Unsupported aggregate function" {:type agg-type}))))

(defn- update-aggregator
  "Update aggregator state with a new value.

   Args:
     state - Current aggregator state map
     value - Value to accumulate (may be nil)

   Returns updated state map."
  [state value]
  (case (:type state)
    :count
    ;; COUNT(*) counts all rows, COUNT(?var) counts non-null
    ;; The caller decides whether to pass nil or skip
    (if (some? value)
      (update state :count inc)
      state)

    :count-distinct
    (if (some? value)
      (update state :values conj value)
      state)

    :sum
    (if (and (some? value) (number? value))
      (update state :sum + value)
      state)

    :avg
    (if (and (some? value) (number? value))
      (-> state
          (update :sum + value)
          (update :count inc))
      state)

    :min
    (if (some? value)
      (let [current (:value state)]
        (if (or (nil? current)
                (and (number? value) (number? current) (< value current))
                (and (string? value) (string? current) (neg? (compare value current))))
          (assoc state :value value)
          state))
      state)

    :max
    (if (some? value)
      (let [current (:value state)]
        (if (or (nil? current)
                (and (number? value) (number? current) (> value current))
                (and (string? value) (string? current) (pos? (compare value current))))
          (assoc state :value value)
          state))
      state)

    state))

(defn- finalize-aggregator
  "Compute final aggregate value from accumulator state.

   Args:
     state - Final aggregator state map

   Returns the computed aggregate value."
  [state]
  (case (:type state)
    :count (:count state)
    :count-distinct (count (:values state))
    :sum (:sum state)
    :avg (let [{:keys [sum count]} state]
           (if (pos? count)
             (/ sum count)
             0))
    :min (:value state)
    :max (:value state)
    nil))

(defrecord HashAggOp [child group-keys aggregates state]
  ;; group-keys: Vector of column names to group by (may be empty for implicit grouping)
  ;; aggregates: Vector of {:fn :count/:sum/:avg/:min/:max/:count-distinct
  ;;                        :column column-name (nil for count(*))
  ;;                        :alias output-column-name}
  ;; state is an atom containing:
  ;; {:opened? bool
  ;;  :groups HashMap (group-key -> {:aggs [accumulator-states...]})
  ;;  :result-emitted? bool
  ;;  :result-batch [row-maps...]}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "HashAggOp opening:" {:group-keys group-keys
                                       :aggregates (count aggregates)})
      (open! child)
      (reset! state {:opened? true
                     :groups (HashMap.)
                     :result-emitted? false
                     :result-batch nil}))
    this)

  (next-batch! [_this]
    (let [{:keys [opened? result-emitted?]} @state]
      (when (and opened? (not result-emitted?))
        ;; Phase 1: Consume all child batches and build groups
        (let [^HashMap groups (:groups @state)]
          (loop []
            (when-let [batch (next-batch! child)]
              ;; Process each row in the batch
              (let [rows (cond
                           (arrow-batch? batch)
                           (let [row-maps (batch->row-maps batch)]
                             ;; Close Arrow batch to free memory
                             (.close ^org.apache.arrow.vector.VectorSchemaRoot batch)
                             row-maps)
                           (map? batch) [batch]
                           (vector? batch) batch
                           (sequential? batch) (vec batch)
                           :else [])]
                (doseq [row rows]
                  ;; Extract group key
                  (let [group-key (if (seq group-keys)
                                    (mapv #(get-row-value row %) group-keys)
                                    [::all-rows])  ;; Implicit grouping
                        ;; Get or create group state
                        group-state (or (.get groups group-key)
                                        (let [initial {:aggs (mapv #(create-aggregator (:fn %)) aggregates)
                                                       :group-values (when (seq group-keys)
                                                                       (zipmap group-keys group-key))}]
                                          (.put groups group-key initial)
                                          initial))
                        ;; Update each aggregator
                        updated-aggs
                        (mapv (fn [agg-state agg-spec]
                                (let [col (:column agg-spec)
                                      ;; For COUNT(*), always pass a non-nil value
                                      ;; For other aggs, get the column value
                                      value (if (nil? col)
                                              ::count-star  ;; Sentinel for COUNT(*)
                                              (get-row-value row col))]
                                  (update-aggregator agg-state value)))
                              (:aggs group-state)
                              aggregates)]
                    (.put groups group-key (assoc group-state :aggs updated-aggs)))))
              (recur)))

          ;; SPARQL semantics: implicit grouping (no GROUP BY) with 0 input rows
          ;; must still return 1 row with COUNT()=0, SUM()=0, AVG()=null, etc.
          (when (and (empty? group-keys) (.isEmpty groups))
            (let [implicit-key [::all-rows]
                  initial-aggs (mapv #(create-aggregator (:fn %)) aggregates)]
              (.put groups implicit-key {:aggs initial-aggs :group-values nil})))

          ;; Phase 2: Emit aggregated results as a single batch
          (let [result-rows
                (vec
                 (for [group-key (keys groups)
                       :let [group-state (.get groups group-key)]]
                   ;; Build output row with group keys + aggregate results
                   (let [group-vals (or (:group-values group-state) {})
                         agg-vals (into {}
                                        (map (fn [agg-state agg-spec]
                                               [(:alias agg-spec) (finalize-aggregator agg-state)])
                                             (:aggs group-state)
                                             aggregates))]
                     (merge group-vals agg-vals))))]
            (swap! state assoc :result-emitted? true :result-batch result-rows)
            (log/debug "HashAggOp emitting results:" {:groups (count result-rows)})
            ;; Return result rows (may be single row for implicit grouping with 0 input)
            result-rows)))))

  (close! [this]
    (when (:opened? @state)
      (log/debug "HashAggOp closing")
      (close! child)
      (when-let [^HashMap groups (:groups @state)]
        (.clear groups))
      (reset! state {:opened? false
                     :groups nil
                     :result-emitted? false
                     :result-batch nil}))
    this)

  (estimated-rows [_this]
    ;; Estimate output rows as min(child-rows, distinct-groups-estimate)
    ;; If no group keys, always 1 row (implicit grouping)
    (if (empty? group-keys)
      1
      ;; Estimate number of groups as sqrt of input (rough heuristic)
      (let [child-rows (estimated-rows child)]
        (max 1 (long (Math/sqrt child-rows)))))))

(defn create-hash-agg-op
  "Create a hash aggregation operator for GROUP BY with aggregate functions.

   The HashAggOp accumulates input rows into groups based on group-keys,
   computing aggregate functions over each group. After all input is consumed,
   it emits one row per group containing group key values and aggregate results.

   Args:
     child      - Child ITabularPlan to aggregate
     group-keys - Vector of column names to GROUP BY (empty for implicit grouping)
     aggregates - Vector of aggregate specifications, each a map:
                  {:fn     - Aggregate function keyword (:count, :count-distinct,
                             :sum, :avg, :min, :max)
                   :column - Column name to aggregate (nil for COUNT(*))
                   :alias  - Output column name for this aggregate}

   Aggregate Functions:
     :count         - COUNT(*) when :column is nil, else COUNT(?var) (non-nulls)
     :count-distinct - COUNT(DISTINCT ?var) - unique non-null values
     :sum           - SUM(?var) - sum of numeric values
     :avg           - AVG(?var) - average (sum / count)
     :min           - MIN(?var) - minimum value
     :max           - MAX(?var) - maximum value

   Examples:
     ;; SELECT (COUNT(*) AS ?total) WHERE { ?s ?p ?o }
     (create-hash-agg-op scan-op [] [{:fn :count :column nil :alias \"total\"}])

     ;; SELECT ?category (SUM(?amount) AS ?sum) WHERE {...} GROUP BY ?category
     (create-hash-agg-op scan-op [\"category\"]
                         [{:fn :sum :column \"amount\" :alias \"sum\"}])

     ;; SELECT ?dept (COUNT(?emp) AS ?cnt) (AVG(?salary) AS ?avg_sal)
     ;;        WHERE {...} GROUP BY ?dept
     (create-hash-agg-op scan-op [\"dept\"]
                         [{:fn :count :column \"emp\" :alias \"cnt\"}
                          {:fn :avg :column \"salary\" :alias \"avg_sal\"}])

   Notes:
     - All child rows must be consumed before results are emitted (blocking)
     - Memory usage is proportional to number of distinct groups
     - For very high cardinality GROUP BY, consider approximate aggregations
     - Empty input produces empty output (no rows, not row with nulls)"
  [child group-keys aggregates]
  {:pre [(satisfies? ITabularPlan child)
         (vector? group-keys)
         (vector? aggregates)
         (every? #(and (:fn %) (:alias %)) aggregates)]}
  (map->HashAggOp {:child child
                   :group-keys group-keys
                   :aggregates aggregates
                   :state (atom {:opened? false
                                 :groups nil
                                 :result-emitted? false
                                 :result-batch nil})}))

;;; ---------------------------------------------------------------------------
;;; Batch to Solution Conversion
;;; ---------------------------------------------------------------------------

(defn batch->row-maps
  "Convert an Arrow batch to a vector of row maps.

   IMPORTANT: This function is EAGER (not lazy) because Arrow buffers may be
   reused after the batch is consumed. The data must be extracted immediately
   while the batch is still valid.

   Args:
     batch - Arrow VectorSchemaRoot

   Returns vector of {column-name -> value} maps."
  [^org.apache.arrow.vector.VectorSchemaRoot batch]
  (let [row-count (.getRowCount batch)
        field-vectors (.getFieldVectors batch)
        col-names (mapv #(.getName (.getField ^org.apache.arrow.vector.FieldVector %)) field-vectors)]
    ;; Use mapv to eagerly realize all rows while the batch is still valid
    (mapv (fn [i]
            (into {}
                  (keep-indexed
                   (fn [col-idx ^org.apache.arrow.vector.FieldVector fv]
                     (let [value (get-vector-value fv i)]
                       (when (some? value)
                         [(nth col-names col-idx) value])))
                   field-vectors)))
          (range row-count))))

;;; ---------------------------------------------------------------------------
;;; Plan Execution Helper
;;; ---------------------------------------------------------------------------

(defn- batch->rows
  "Convert a batch to row maps and close Arrow batches to free off-heap memory.
   Handles three cases:
   1. Arrow VectorSchemaRoot -> extract as row maps, then close batch
   2. Single row map -> wrap in vector
   3. Vector of row maps (from join) -> pass through"
  [batch]
  (cond
    (arrow-batch? batch)
    (let [rows (batch->row-maps batch)]
      ;; Close Arrow batch to release off-heap memory
      (.close ^org.apache.arrow.vector.VectorSchemaRoot batch)
      rows)
    (map? batch) [batch]
    (vector? batch) batch
    (sequential? batch) (vec batch)
    :else [batch]))

(defn execute-plan
  "Execute a tabular plan, returning all row maps.

   Opens the plan, drains all batches, converts to row maps, and closes.
   Use for testing or when full materialization is acceptable.

   Args:
     plan - ITabularPlan to execute

   Returns vector of row maps."
  [plan]
  (open! plan)
  (try
    (loop [rows []]
      (if-let [batch (next-batch! plan)]
        (recur (into rows (batch->rows batch)))
        rows))
    (finally
      (close! plan))))

;;; ---------------------------------------------------------------------------
;;; Plan Compiler
;;; ---------------------------------------------------------------------------

(defn- collect-columns-for-table
  "Collect all columns needed for a table: predicates + join columns."
  [mapping predicates join-graph]
  (let [table-name (:table mapping)
        ;; Columns from predicates (for WHERE conditions)
        predicate-cols (keep :column predicates)
        ;; Columns from subject template
        subject-cols (when-let [template (:subject-template mapping)]
                       (re-seq #"\{([^}]+)\}" template))
        ;; Join columns from edges
        join-cols (when join-graph
                    (let [edges (join/edges-for-table join-graph table-name)]
                      (for [edge edges
                            col (if (= table-name (:child-table edge))
                                  (join/child-columns edge)
                                  (join/parent-columns edge))]
                        col)))]
    (-> (concat predicate-cols
                (map second subject-cols)
                join-cols)
        distinct
        vec)))

(defn- build-scan-op-for-group
  "Build a ScanOp for a single pattern group.

   Args:
     sources           - Map of {table-name -> ITabularSource}
     mapping           - R2RML mapping for this table
     predicates        - Pushdown predicates for this table
     join-graph        - Join graph (for join column inclusion)
     time-travel       - Time travel spec
     opts              - Options map:
                         :use-arrow-batches? - If true, use raw Arrow batches
                         :copy-batches?      - If false, don't copy batches (streaming)"
  [sources mapping predicates join-graph time-travel opts]
  (let [table-name (:table mapping)
        source (get sources table-name)
        columns (collect-columns-for-table mapping predicates join-graph)]
    (when-not source
      (throw (ex-info (str "No source for table: " table-name)
                      {:table table-name
                       :available (keys sources)})))
    (create-scan-op source table-name columns predicates time-travel
                    {:use-arrow-batches? (get opts :use-arrow-batches? false)
                     :copy-batches? (get opts :copy-batches?)})))

(defn- find-join-edge
  "Find the join edge connecting two tables, or nil if not connected."
  [join-graph table-a table-b]
  (first (join/edges-between join-graph table-a table-b)))

(defn- collect-downstream-join-keys
  "Collect all join key columns needed for joins after the current position.

   For a 3-table join A → B → C, when we're creating the A-B join, we need
   to include B's join keys for the B-C join in the output.

   Args:
     join-graph    - Join graph with edges between tables
     join-order    - Vector of tables in join order
     current-idx   - Current position in join order (0-indexed)

   Returns:
     Set of column names needed for downstream joins."
  [join-graph join-order current-idx]
  (let [remaining-tables (subvec join-order (inc current-idx))
        joined-so-far (set (take (inc current-idx) join-order))]
    (into #{}
          (for [table remaining-tables
                edge (join/edges-for-table join-graph table)
                :when (or (contains? joined-so-far (:parent-table edge))
                          (contains? joined-so-far (:child-table edge)))
                col (concat (join/parent-columns edge)
                            (join/child-columns edge))]
            col))))

(defn compile-plan
  "Compile a tabular plan from pattern groups.

   This is the main entry point for creating an executable plan from
   SPARQL pattern groups routed to Iceberg tables.

   The compiler:
   1. Creates a ScanOp for each table
   2. Uses greedy join ordering based on cardinality estimates
   3. Chains ScanOps together with HashJoinOps
   4. Returns the root operator

   Args:
     sources         - Map of {table-name -> ITabularSource}
     pattern-groups  - [{:mapping m :patterns [...] :predicates [...] :optional? bool}]
     join-graph      - Join graph from build-join-graph
     stats-by-table  - Map of {table-name -> statistics}
     time-travel     - Optional time travel spec
     opts            - Options map:
                       :use-arrow-batches? - If true, use raw Arrow batches
                                             for columnar execution (default false)
                       :copy-batches?      - If false, don't copy Arrow batches.
                                             Use for streaming where batches are
                                             immediately consumed. Default true.
                       :output-arrow?      - If true, hash joins output Arrow batches
                                             instead of row maps. Use with
                                             :use-arrow-batches? for full columnar
                                             pipeline. Default false.
                       :vectorized?        - If true, use true vectorized joins (Phase 3).
                                             Join builds store Arrow batches + index refs,
                                             probe gathers directly from source vectors.
                                             ~2x faster than standard mode.
                                             Requires :use-arrow-batches? true.
                                             NOTE: For optimal performance with vectorized?,
                                             set :copy-batches? true (batches must outlive
                                             iteration for build storage). Default false.
                      :output-columns     - Optional set of column names to include in
                                            join output. If specified, hash joins only
                                            copy these columns to output batches,
                                            reducing memory bandwidth. Best for two-table
                                            joins; for 3+ tables, ensure all downstream
                                            join keys are included. Default nil (all cols).

   Returns:
     ITabularPlan root operator, or nil if no pattern groups."
  ([sources pattern-groups join-graph stats-by-table time-travel]
   (compile-plan sources pattern-groups join-graph stats-by-table time-travel {}))
  ([sources pattern-groups join-graph stats-by-table time-travel opts]
   (when (seq pattern-groups)
     (let [use-arrow-batches? (get opts :use-arrow-batches? false)
           copy-batches? (get opts :copy-batches?)  ;; nil = default (true)
           output-arrow? (get opts :output-arrow? false)
           vectorized? (get opts :vectorized? false)
           output-columns (get opts :output-columns)  ;; nil = all columns
           scan-opts {:use-arrow-batches? use-arrow-batches?
                      :copy-batches? copy-batches?}
           ;; Build scan ops for each table, tracking optional status
           scans-by-table
           (into {}
                 (for [{:keys [mapping predicates optional?]} pattern-groups
                       :let [table-name (:table mapping)]]
                   [table-name
                    {:scan (build-scan-op-for-group sources mapping
                                                    (or predicates [])
                                                    join-graph time-travel
                                                    scan-opts)
                     :mapping mapping
                     :optional? (boolean optional?)}]))

           table-names (keys scans-by-table)

          ;; Use greedy join ordering if multiple tables
           join-order (if (> (count table-names) 1)
                        (join/greedy-join-order (set table-names)
                                                join-graph
                                                stats-by-table
                                                {})
                        (vec table-names))]

       (log/debug "Plan compiler:" {:tables table-names
                                    :join-order join-order})

       (if (= 1 (count join-order))
        ;; Single table - just return the scan
         (:scan (get scans-by-table (first join-order)))

        ;; Multiple tables - chain with hash joins
        ;; Use reduce with indexed join-order to track position for
        ;; downstream join key preservation
         (let [indexed-order (vec join-order)
               num-tables (count indexed-order)]
           ;; Extract final plan from [plan, table-idx] tuple returned by reduce
           (first
            (reduce
             (fn [[accumulated-plan table-idx] current-table]
               (if (nil? accumulated-plan)
                ;; First table - start with its scan (no join created yet)
                ;; Return 1 so next iteration can (take 1 indexed-order) to get first table
                 [(:scan (get scans-by-table current-table)) 1]
                ;; Subsequent tables - join to accumulated plan
                 (let [current-scan (:scan (get scans-by-table current-table))
                       ;; Check if current table's pattern group is optional
                       current-optional? (get-in scans-by-table [current-table :optional?])
                      ;; Find join edge between current and any accumulated table
                       edge (some (fn [t]
                                    (find-join-edge join-graph t current-table))
                                  (take table-idx indexed-order))]
                   (if edge
                    ;; Create hash join
                     (let [;; For OPTIONAL (left outer join), we must ensure:
                           ;; - probe side = accumulated (required) - gets preserved
                           ;; - build side = current (optional) - allows nulls
                           ;;
                           ;; For inner join, use FK-based heuristic for efficiency
                           current-is-child? (= current-table (:child-table edge))

                           ;; CRITICAL: For OPTIONAL, force correct orientation
                           ;; Left outer join preserves ALL probe rows, so probe must be required
                           [build-plan probe-plan build-keys probe-keys]
                           (if current-optional?
                             ;; OPTIONAL: accumulated is required (probe), current is optional (build)
                             ;; This ensures all required rows are preserved with nulls for optional
                             (if current-is-child?
                               ;; Current (optional) is child, accumulated (required) is parent
                               ;; probe=accumulated uses parent cols, build=current uses child cols
                               [current-scan accumulated-plan
                                (vec (join/child-columns edge))
                                (vec (join/parent-columns edge))]
                               ;; Current (optional) is parent, accumulated (required) is child
                               ;; probe=accumulated uses child cols, build=current uses parent cols
                               [current-scan accumulated-plan
                                (vec (join/parent-columns edge))
                                (vec (join/child-columns edge))])
                             ;; Inner join: use FK-based heuristic for efficiency
                             (if current-is-child?
                               ;; Accumulated is parent (dimension), current is child (fact)
                               [accumulated-plan current-scan
                                (vec (join/parent-columns edge))
                                (vec (join/child-columns edge))]
                               ;; Current is parent, accumulated is child
                               [current-scan accumulated-plan
                                (vec (join/parent-columns edge))
                                (vec (join/child-columns edge))]))

                           ;; For intermediate joins, merge downstream join keys with user's output-columns
                           ;; For final join, use user's output-columns directly
                           ;; For n tables, final join is when table-idx = n-1 (processing last table)
                           is-final-join? (= table-idx (dec num-tables))
                           effective-output-columns
                           (if (and output-columns (not is-final-join?))
                             ;; Intermediate join: include downstream join keys
                             (let [downstream-keys (collect-downstream-join-keys join-graph indexed-order table-idx)]
                               (into output-columns downstream-keys))
                             ;; Final join or no output-columns specified
                             output-columns)
                           ;; Use left outer join for optional pattern groups
                           left-outer? (boolean current-optional?)]
                       (log/debug "Creating hash join:" {:build-keys build-keys
                                                         :probe-keys probe-keys
                                                         :edge edge
                                                         :output-arrow? output-arrow?
                                                         :vectorized? vectorized?
                                                         :is-final-join? is-final-join?
                                                         :left-outer? left-outer?
                                                         :optional-orientation (when left-outer? "probe=required, build=optional")
                                                         :output-columns (count effective-output-columns)})
                       [(create-hash-join-op build-plan probe-plan build-keys probe-keys
                                             {:output-arrow? output-arrow?
                                              :vectorized? vectorized?
                                              :output-columns effective-output-columns
                                              :left-outer? left-outer?})
                        (inc table-idx)])
                    ;; No edge found - would be Cartesian product
                    ;; For now, just return accumulated (caller should handle)
                     (do
                       (log/warn "No join edge found, skipping table:" current-table)
                       [accumulated-plan (inc table-idx)])))))
             [nil 0]
             indexed-order))))))))

(defn compile-single-table-plan
  "Compile a plan for a single table query (no joins).

   Simpler entry point when only one table is involved.

   Args:
     source      - ITabularSource
     table-name  - Table name
     columns     - Columns to project
     predicates  - Pushdown predicates
     time-travel - Optional time travel spec
     opts        - Options map:
                   :use-arrow-batches? - If true, use raw Arrow batches
                                         for columnar execution (default false)
                   :copy-batches?      - If false, don't copy Arrow batches.
                                         Use for streaming. Default true.

   Returns:
     ScanOp for the table."
  ([source table-name columns predicates time-travel]
   (compile-single-table-plan source table-name columns predicates time-travel {}))
  ([source table-name columns predicates time-travel opts]
   (create-scan-op source table-name columns predicates time-travel opts)))
